//! Collect bounded GitHub activity through gh without adding an HTTP client.
mod github_activity;

use cordis_core::{BoxError, Context, Disposer, FiberHandle, Plugin, Runtime, ServiceKey};
use github_activity::{
    ITEM_LIMIT, Item, MAX_SNAPSHOT_BYTES, Snapshot, atomic_write, drain, invalid, read_limited,
    validate_output_parent, validate_repository,
};
use std::{
    convert::Infallible,
    path::PathBuf,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, sync::watch};

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) repository: String,
    pub(crate) output: PathBuf,
    pub(crate) interval: Option<Duration>,
    pub(crate) gh: PathBuf,
    pub(crate) timeout: Duration,
}

impl Config {
    pub(crate) fn new(repository: String, output: PathBuf, interval: Option<Duration>) -> Self {
        Self {
            repository,
            output,
            interval,
            gh: "gh".into(),
            timeout: Duration::from_secs(30),
        }
    }

    fn validate(&self) -> Result<Self, BoxError> {
        validate_repository(&self.repository)?;
        for duration in self.interval.into_iter().chain([self.timeout]) {
            if duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none() {
                return Err(invalid(
                    "interval and timeout must be positive, representable durations",
                )
                .into());
            }
        }
        Ok(self.clone())
    }
}

async fn query(config: &Config, kind: &str) -> Result<Vec<Item>, BoxError> {
    let mut child = Command::new(&config.gh)
        .args([
            kind,
            "list",
            "--repo",
            &config.repository,
            "--state",
            "all",
            "--limit",
            &ITEM_LIMIT.to_string(),
            "--search",
            "sort:updated-desc",
            "--json",
            "number,title,state,url,updatedAt",
        ])
        .env("GH_HOST", "github.com")
        .env("GH_PROMPT_DISABLED", "1")
        .env("GH_PAGER", "cat")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| invalid("missing gh stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| invalid("missing gh stderr"))?;
    let result = tokio::time::timeout(config.timeout, async {
        tokio::try_join!(
            child.wait(),
            read_limited(stdout, MAX_SNAPSHOT_BYTES),
            read_limited(stderr, 64 * 1024)
        )
    })
    .await;
    let (status, stdout, stderr) = match result {
        Ok(Ok(output)) => output,
        failure => {
            child.kill().await?;
            return Err(match failure {
                Ok(Err(error)) => error.into(),
                Err(error) => std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("gh {kind} query timed out: {error}"),
                )
                .into(),
                Ok(Ok(_)) => unreachable!("successful query handled above"),
            });
        }
    };
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "gh {kind} failed ({status}): {}",
            String::from_utf8_lossy(&stderr).trim()
        ))
        .into());
    }
    let items: Vec<Item> = serde_json::from_slice(&stdout)?;
    if items.len() > ITEM_LIMIT {
        return Err(invalid("gh returned more than 100 items").into());
    }
    Ok(items)
}

pub(crate) async fn collect_once(config: &Config) -> Result<(), BoxError> {
    config.validate()?;
    let issues = query(config, "issue").await?;
    let pull_requests = query(config, "pr").await?;
    let snapshot = Snapshot {
        repository: config.repository.clone(),
        captured_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        issues,
        pull_requests,
    };
    snapshot.validate()?;
    let mut bytes = serde_json::to_vec_pretty(&snapshot)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(invalid("combined snapshot exceeds 4 MiB").into());
    }
    atomic_write(&config.output, &bytes).await
}

pub(crate) struct Collector {
    pub(crate) configuration: FiberHandle<Config>,
    pub(crate) poller: FiberHandle<()>,
    pub(crate) completed: watch::Receiver<u64>,
}

pub(crate) fn start(root: &Context, config: Config) -> Result<Collector, BoxError> {
    let key = ServiceKey::<Config>::new("githubCollectionConfig");
    let configuration = root.plugin(
        &Plugin::validated("CollectionConfig", Config::validate, {
            let key = key.clone();
            move |ctx, config: Config| {
                let key = key.clone();
                async move {
                    validate_output_parent(&config.output).await?;
                    ctx.provide(&key, config)?;
                    Ok::<_, BoxError>(())
                }
            }
        }),
        config,
    )?;
    let (completed, observations) = watch::channel(0);
    let dependency = key.required();
    let poller = root.plugin(
        &Plugin::new("GitHubCollector", move |ctx, ()| {
            let key = key.clone();
            let completed = completed.clone();
            async move {
                let config = ctx
                    .get(&key)?
                    .ok_or_else(|| invalid("collection config unavailable"))?;
                collect_once(&config).await?;
                completed.send_modify(|count| *count += 1);
                ctx.logger(None).info("GitHub snapshot written")?;
                if let Some(interval) = config.interval {
                    let logger = ctx.logger(None);
                    ctx.effect(move || {
                        let (stop, mut stopped) = watch::channel(false);
                        let worker = tokio::task::spawn_local(async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    _ = stopped.changed() => return Ok::<_, BoxError>(()),
                                    _ = tokio::time::sleep(interval) => {}
                                }
                                match collect_once(&config).await {
                                    Ok(()) => {
                                        completed.send_modify(|count| *count += 1);
                                        logger.info("GitHub snapshot written")?;
                                    }
                                    Err(error) => {
                                        eprintln!("GitHub collection failed: {error}");
                                        logger
                                            .error(format!("GitHub collection failed: {error}"))?;
                                    }
                                };
                            }
                        });
                        Ok::<_, Infallible>(Disposer::new(move || async move {
                            stop.send_replace(true);
                            worker.await?
                        }))
                    })?;
                }
                Ok::<_, BoxError>(())
            }
        })
        .injecting([dependency]),
        (),
    )?;
    Ok(Collector {
        configuration,
        poller,
        completed: observations,
    })
}

pub(crate) async fn run(config: Config) -> Result<(), BoxError> {
    let periodic = config.interval.is_some();
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let result = async {
                let app = start(&root, config)?;
                tokio::select! {
                    result = async {
                        app.configuration.ready()?.await?;
                        app.poller.ready()?.await?;
                        println!("completed {} snapshot(s)", *app.completed.borrow());
                        if periodic {
                            std::future::pending::<()>().await;
                        }
                        Ok::<_, BoxError>(())
                    } => result,
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        Ok(())
                    }
                }
            }
            .await;
            let cleanup = drain(&root).await;
            result.and(cleanup)
        })
        .await?
}

#[cfg(not(test))]
fn main() -> Result<(), BoxError> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=3).contains(&args.len()) {
        return Err(
            invalid("usage: github_collect OWNER/REPO SNAPSHOT.json [INTERVAL_SECONDS]").into(),
        );
    }
    let interval = args
        .get(2)
        .map(|value| value.parse::<u64>().map(Duration::from_secs))
        .transpose()?;
    let config = Config::new(args[0].clone(), args[1].clone().into(), interval);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(config))
}
