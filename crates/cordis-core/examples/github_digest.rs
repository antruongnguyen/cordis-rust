//! Convert a bounded GitHub activity snapshot into a deterministic Markdown report.
mod github_activity;

use cordis_core::{BoxError, Context, FiberHandle, Plugin, Runtime, ServiceKey};
use github_activity::{
    MAX_SNAPSHOT_BYTES, Snapshot, State, atomic_write, drain, invalid, read_limited,
    validate_output_parent,
};
use std::{io, path::PathBuf};

fn markdown(text: &str) -> String {
    let mut escaped = String::new();
    for c in text.chars() {
        if "\\`*_{}[]<>#|!".contains(c) {
            escaped.push('\\');
        }
        escaped.push(if c.is_control() { ' ' } else { c });
    }
    escaped
}

pub(crate) fn render(snapshot: &Snapshot) -> Result<String, BoxError> {
    snapshot.validate()?;
    let mut report = format!(
        "# {} activity\n\nCaptured at Unix second {}. Up to 100 items per kind; not full history.\n",
        snapshot.repository, snapshot.captured_at
    );
    for (heading, items) in [
        ("Issues", &snapshot.issues),
        ("Pull requests", &snapshot.pull_requests),
    ] {
        report.push_str(&format!("\n## {heading}\n"));
        for (label, state) in [
            ("Open", State::Open),
            ("Closed", State::Closed),
            ("Merged", State::Merged),
        ] {
            let mut selected: Vec<_> = items.iter().filter(|item| item.state == state).collect();
            if selected.is_empty() {
                continue;
            }
            selected.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then(a.number.cmp(&b.number))
            });
            report.push_str(&format!("\n### {label}\n\n"));
            for item in selected {
                report.push_str(&format!(
                    "- [#{} {}]({}) - {}\n",
                    item.number,
                    markdown(&item.title),
                    item.url,
                    item.updated_at
                ));
            }
        }
        if items.is_empty() {
            report.push_str("\nNone.\n");
        }
    }
    Ok(report)
}

pub(crate) struct Digest {
    pub(crate) destination: FiberHandle<PathBuf>,
    pub(crate) processor: FiberHandle<PathBuf>,
}

pub(crate) fn start(root: &Context, input: PathBuf, output: PathBuf) -> Result<Digest, BoxError> {
    let key = ServiceKey::<PathBuf>::new("githubReportDestination");
    let destination = root.plugin(
        &Plugin::new("ReportDestination", {
            let key = key.clone();
            move |ctx, output: PathBuf| {
                let key = key.clone();
                async move {
                    validate_output_parent(&output).await?;
                    ctx.provide(&key, output)?;
                    Ok::<_, BoxError>(())
                }
            }
        }),
        output,
    )?;
    let processor = root.plugin(
        &Plugin::new("ActivityDigest", move |ctx, input: PathBuf| {
            let key = key.clone();
            async move {
                let output = ctx
                    .get(&key)?
                    .ok_or_else(|| invalid("report destination unavailable"))?;
                let input_identity = tokio::fs::canonicalize(&input).await?;
                match tokio::fs::canonicalize(output.as_ref()).await {
                    Ok(path) if path == input_identity => {
                        return Err(invalid("input and output must differ").into());
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                let bytes =
                    read_limited(tokio::fs::File::open(&input).await?, MAX_SNAPSHOT_BYTES).await?;
                let snapshot: Snapshot = serde_json::from_slice(&bytes)?;
                atomic_write(&output, render(&snapshot)?.as_bytes()).await?;
                ctx.logger(None).info("activity report written")?;
                Ok::<_, BoxError>(())
            }
        })
        .injecting([ServiceKey::<PathBuf>::new("githubReportDestination").required()]),
        input,
    )?;
    Ok(Digest {
        destination,
        processor,
    })
}

pub(crate) async fn run(input: PathBuf, output: PathBuf) -> Result<(), BoxError> {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let result = async {
                let digest = start(&root, input, output)?;
                digest.destination.ready()?.await?;
                digest.processor.ready()?.await?;
                Ok::<_, BoxError>(())
            }
            .await;
            let cleanup = drain(&root).await;
            result.and(cleanup)
        })
        .await?
}

#[cfg(not(test))]
fn main() -> Result<(), BoxError> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err(invalid("usage: github_digest SNAPSHOT.json REPORT.md").into());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(args[0].clone().into(), args[1].clone().into()))
}
