//! Application-level snapshot/report acceptance, using the executable itself.
#[path = "../examples/github_digest.rs"]
mod digest;

use cordis_core::{FiberState, Runtime};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static ID: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "cordis-digest-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

const INPUT: &str = include_str!("fixtures/github_snapshot.json");
const OUTPUT: &str = include_str!("fixtures/github_digest.md");

#[tokio::test(flavor = "current_thread")]
async fn executable_writes_exact_markdown_and_preserves_output_on_invalid_input() {
    let dir = Directory::new();
    let input = dir.path("snapshot.json");
    let output = dir.path("report.md");
    tokio::fs::write(&input, INPUT).await.unwrap();
    digest::run(input.clone(), output.clone()).await.unwrap();
    assert_eq!(tokio::fs::read_to_string(&output).await.unwrap(), OUTPUT);
    for invalid in [
        "{".to_owned(),
        INPUT.replace("2026-10-22T12:00:00Z", "2026-02-30T12:00:00Z"),
        INPUT.replace("/issues/1", "/issues/999"),
        INPUT.replace("\"number\": 1", "\"number\": 3"),
        INPUT.replace("\"OPEN\"", "\"UNKNOWN\""),
    ] {
        tokio::fs::write(&input, invalid).await.unwrap();
        assert!(digest::run(input.clone(), output.clone()).await.is_err());
        assert_eq!(tokio::fs::read_to_string(&output).await.unwrap(), OUTPUT);
    }
    tokio::fs::write(&input, INPUT).await.unwrap();
    assert!(digest::run(input.clone(), input.clone()).await.is_err());
    assert_eq!(tokio::fs::read_to_string(&input).await.unwrap(), INPUT);
    assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn destination_reconfiguration_and_explicit_recovery_use_existing_lifecycle() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let dir = Directory::new();
        let input = dir.path("snapshot.json");
        let first = dir.path("first.md");
        let second = dir.path("second.md");
        tokio::fs::write(&input, "{").await.unwrap();
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let baseline = root.get_effects().unwrap();
                let app = digest::start(&root, input.clone(), first.clone()).unwrap();
                app.destination.ready().unwrap().await.unwrap();
                assert!(app.processor.ready().unwrap().await.is_err());
                assert_eq!(app.processor.state(), FiberState::Failed);
                assert_eq!(runtime.take_failures().len(), 1);
                tokio::fs::write(&input, INPUT).await.unwrap();
                app.destination
                    .update(second.clone())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(app.processor.state(), FiberState::Failed);
                app.processor.update(input).unwrap().await.unwrap();
                assert_eq!(tokio::fs::read_to_string(&second).await.unwrap(), OUTPUT);
                assert!(!first.exists());
                app.destination
                    .update(first.clone())
                    .unwrap()
                    .await
                    .unwrap();
                app.processor.ready().unwrap().await.unwrap();
                assert_eq!(tokio::fs::read_to_string(first).await.unwrap(), OUTPUT);
                let report = root.drain().unwrap().await.unwrap();
                assert!(report.failures().is_empty());
                assert!(root.registry().is_empty().unwrap());
                assert_eq!(root.get_effects().unwrap(), baseline);
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
    })
    .await
    .expect("digest lifecycle stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_input_and_failed_rename_do_not_publish_partial_reports() {
    let dir = Directory::new();
    let input = dir.path("snapshot.json");
    let output = dir.path("report.md");
    tokio::fs::write(&input, vec![b' '; 4 * 1024 * 1024 + 1])
        .await
        .unwrap();
    assert!(digest::run(input.clone(), output.clone()).await.is_err());
    assert!(!output.exists());
    tokio::fs::write(&input, INPUT).await.unwrap();
    tokio::fs::create_dir(&output).await.unwrap();
    assert!(digest::run(input, output.clone()).await.is_err());
    assert!(output.is_dir());
    assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 2);
}
