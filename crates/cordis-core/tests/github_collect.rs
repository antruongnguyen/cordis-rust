//! Real subprocess, snapshot publication and supervised polling acceptance.
#![cfg(unix)]

#[path = "../examples/github_collect.rs"]
mod collector;
#[path = "support/github_activity.rs"]
mod support;

use collector::Config;
use cordis_core::{FiberState, Listener, Runtime, hooks};
use futures_util::FutureExt;
use std::{
    cell::RefCell, convert::Infallible, ops::ControlFlow, os::unix::fs::PermissionsExt,
    time::Duration,
};
use support::Directory;

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn script(dir: &Directory, prefix: &str, pr: &str) -> std::path::PathBuf {
    let snapshot: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/github_snapshot.json")).unwrap();
    let issues = quoted(&snapshot["issues"].to_string());
    let pull_requests = quoted(&snapshot["pull_requests"].to_string());
    let file = dir.path("gh");
    std::fs::write(
        &file,
        format!(
            "#!/bin/sh\nset -eu\n\
         [ \"$#\" -eq 12 ] && [ \"$2\" = list ] && [ \"$3\" = --repo ] && \
         [ \"$4\" = antruongnguyen/cordis-rust ] && [ \"$5\" = --state ] && \
         [ \"$6\" = all ] && [ \"$7\" = --limit ] && [ \"$8\" = 100 ] && \
         [ \"$9\" = --search ] && [ \"${{10}}\" = sort:updated-desc ] && \
         [ \"${{11}}\" = --json ] && [ \"${{12}}\" = number,title,state,url,updatedAt ]\n\
         [ \"$GH_HOST\" = github.com ] && [ \"$GH_PROMPT_DISABLED\" = 1 ]\n\
         {prefix}\ncase \"$1\" in\nissue) printf '%s\\n' {issues};;\n\
         pr) {pr}; printf '%s\\n' {pull_requests};;\n*) exit 99;;\nesac\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700)).unwrap();
    file
}

fn config(dir: &Directory, prefix: &str, pr: &str) -> Config {
    let mut config = Config::new(
        "antruongnguyen/cordis-rust".into(),
        dir.path("snapshot.json"),
        None,
    );
    config.gh = script(dir, prefix, pr);
    config
}

#[tokio::test(flavor = "current_thread")]
async fn executable_collects_complete_snapshot_and_preserves_last_good_on_query_failure() {
    let dir = Directory::new("collector");
    let config = config(&dir, "", ":");
    collector::run(config.clone()).await.unwrap();
    let original = tokio::fs::read(&config.output).await.unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/github_snapshot.json")).unwrap();
    assert_eq!(actual["issues"], expected["issues"]);
    assert_eq!(actual["pull_requests"], expected["pull_requests"]);
    assert_eq!(actual["repository"], expected["repository"]);
    assert!(actual["captured_at"].as_u64().unwrap() > 0);
    script(&dir, "", "echo 'simulated query failure' >&2; exit 7");
    let error = collector::run(config.clone()).await.unwrap_err();
    assert!(error.to_string().contains("simulated query failure"));
    assert_eq!(tokio::fs::read(&config.output).await.unwrap(), original);
    script(
        &dir,
        "if [ \"$1\" = issue ]; then printf '{'; exit 0; fi",
        ":",
    );
    assert!(collector::collect_once(&config).await.is_err());
    assert_eq!(tokio::fs::read(&config.output).await.unwrap(), original);
    let mut invalid = config.clone();
    invalid.repository = "--invalid".into();
    assert!(collector::collect_once(&invalid).await.is_err());
    invalid = config.clone();
    invalid.interval = Some(Duration::ZERO);
    assert!(collector::collect_once(&invalid).await.is_err());
    let mut items = Vec::new();
    for number in 1000..1100 {
        let mut item = expected["issues"][0].clone();
        item["number"] = number.into();
        item["url"] =
            format!("https://github.com/antruongnguyen/cordis-rust/issues/{number}").into();
        items.push(item);
    }
    let prefix = |items: &Vec<serde_json::Value>| {
        format!(
            "if [ \"$1\" = issue ]; then printf '%s\\n' {}; exit 0; fi",
            quoted(&serde_json::to_string(items).unwrap())
        )
    };
    script(&dir, &prefix(&items), ":");
    collector::collect_once(&config).await.unwrap();
    let bounded = tokio::fs::read(&config.output).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bounded).unwrap()["issues"]
            .as_array()
            .unwrap()
            .len(),
        100
    );
    items.push(items[0].clone());
    script(&dir, &prefix(&items), ":");
    assert!(
        collector::collect_once(&config)
            .await
            .unwrap_err()
            .to_string()
            .contains("more than 100")
    );
    assert_eq!(tokio::fs::read(&config.output).await.unwrap(), bounded);
}

#[tokio::test(flavor = "current_thread")]
async fn timed_out_and_oversized_subprocesses_are_reaped_without_publishing() {
    let dir = Directory::new("collector-timeout");
    let blocked = dir.path("blocked");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&blocked)
            .status()
            .unwrap()
            .success()
    );
    let mut config = config(
        &dir,
        &format!("read -r answer < {}", quoted(blocked.to_str().unwrap())),
        ":",
    );
    config.timeout = Duration::from_secs(1);
    let error = collector::collect_once(&config).await.unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    let text = error.to_string();
    let pid: u32 = text
        .split_once(" pid ")
        .unwrap()
        .1
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let running = std::process::Command::new("/bin/kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!running.success(), "timed out child was not reaped");
    assert!(!config.output.exists());
    script(&dir, "exec head -c 4194305 /dev/zero", ":");
    config.timeout = Duration::from_secs(5);
    assert!(
        collector::collect_once(&config)
            .await
            .unwrap_err()
            .to_string()
            .contains("exceeds")
    );
    assert!(!config.output.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn periodic_work_finishes_before_reconfiguration_and_shutdown_and_failed_loads_recover_explicitly()
 {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = Directory::new("collector-lifecycle");
        let entered = dir.path("entered");
        let release = dir.path("release");
        let enabled = dir.path("gate-enabled");
        assert!(std::process::Command::new("mkfifo").args([&entered, &release]).status().unwrap().success());
        let prefix = format!(
            "if [ \"$1\" = issue ] && [ -f {} ]; then printf 'entered\\n' > {}; read -r answer < {}; fi",
            quoted(enabled.to_str().unwrap()), quoted(entered.to_str().unwrap()), quoted(release.to_str().unwrap())
        );
        let mut config = config(&dir, &prefix, ":");
        config.interval = Some(Duration::from_secs(1));
        let runtime = Runtime::new();
        runtime.run(async {
            let root = runtime.context();
            let baseline = root.get_effects().unwrap();
            let mut app = collector::start(&root, config.clone()).unwrap();
            app.configuration.ready().unwrap().await.unwrap();
            app.poller.ready().unwrap().await.unwrap();
            assert_eq!(*app.completed.borrow_and_update(), 1);
            tokio::fs::write(&enabled, "").await.unwrap();
            tokio::time::pause();
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::time::resume();
            assert_eq!(tokio::fs::read_to_string(&entered).await.unwrap(), "entered\n");
            let mut next = config.clone();
            next.output = dir.path("next.json");
            let (unloading, unloaded) = tokio::sync::oneshot::channel();
            let unloading = RefCell::new(Some(unloading));
            let poller_id = app.poller.id().clone();
            root.on(&hooks::status(), Listener::sync(move |_, status: hooks::Status| {
                if status.fiber.id == poller_id && status.fiber.state == FiberState::Unloading {
                    if let Some(sender) = unloading.borrow_mut().take() {
                        sender.send(()).unwrap();
                    }
                }
                Ok::<_, Infallible>(ControlFlow::Continue(()))
            })).unwrap();
            let mut updating = Box::pin(app.configuration.update(next.clone()).unwrap());
            unloaded.await.unwrap();
            assert_eq!(app.poller.state(), FiberState::Unloading);
            assert!(updating.as_mut().now_or_never().is_none());
            assert!(!next.output.exists());
            tokio::fs::remove_file(&enabled).await.unwrap();
            tokio::fs::write(&release, "continue\n").await.unwrap();
            updating.await.unwrap();
            app.poller.ready().unwrap().await.unwrap();
            assert_eq!(*app.completed.borrow_and_update(), 3);
            assert!(next.output.exists());
            script(&dir, "", "echo 'load failed' >&2; exit 9");
            app.configuration.update(next.clone()).unwrap().await.unwrap();
            assert!(app.poller.ready().unwrap().await.is_err());
            assert_eq!(app.poller.state(), FiberState::Failed);
            assert_eq!(runtime.take_failures().len(), 1);
            script(&dir, "", ":");
            app.configuration.update(next).unwrap().await.unwrap();
            assert_eq!(app.poller.state(), FiberState::Failed);
            app.poller.update(()).unwrap().await.unwrap();
            assert_eq!(*app.completed.borrow(), 4);
            script(&dir, &prefix, ":");
            tokio::fs::write(&enabled, "").await.unwrap();
            tokio::time::pause();
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::time::resume();
            tokio::fs::read_to_string(&entered).await.unwrap();
            let mut stopping = Box::pin(root.drain().unwrap());
            assert!(stopping.as_mut().now_or_never().is_none());
            tokio::fs::remove_file(&enabled).await.unwrap();
            tokio::fs::write(&release, "continue\n").await.unwrap();
            let report = stopping.await.unwrap();
            assert_eq!(*app.completed.borrow(), 5);
            assert!(report.failures().is_empty());
            assert!(root.registry().is_empty().unwrap());
            assert_eq!(root.get_effects().unwrap(), baseline);
            assert!(runtime.take_failures().is_empty());
        }).await.unwrap();
    }).await.expect("collector lifecycle stalled");
}
