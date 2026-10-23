//! Executable counterparts of tests/parity/lifecycle.spec.ts at the pinned TS core.
use cordis_core::{
    BoxError, CleanupFailure, FiberState, Level, LifecycleError, Plugin, Runtime, ServiceKey,
};
use futures_util::FutureExt;
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    error::Error,
    fmt,
    rc::Rc,
};
use tokio::sync::oneshot;

#[derive(Debug)]
struct LoadFailure(Rc<std::io::Error>);

impl fmt::Display for LoadFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Error for LoadFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.0)
    }
}

fn assert_trace(name: &str, expected: &str, trace: &[String]) {
    let actual = trace.join("\n") + "\n";
    assert_eq!(actual, expected, "{name} lifecycle trace");
    if let Some(directory) = std::env::var_os("CORDIS_PARITY_OUTPUT") {
        std::fs::write(
            std::path::Path::new(&directory).join(format!("{name}.txt")),
            actual,
        )
        .unwrap();
    }
}

fn assert_source(failure: &CleanupFailure, expected: &Rc<std::io::Error>) {
    let source = failure
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert!(std::ptr::eq(source, expected.as_ref()));
}

fn assert_failure(error: LifecycleError, expected: &Rc<std::io::Error>) {
    let LifecycleError::Callback(failure) = error else {
        panic!("expected the original callback failure");
    };
    assert_source(&failure, expected);
}

fn assert_diagnostic(runtime: &Runtime, expected: &Rc<std::io::Error>) {
    let failures = runtime.take_failures();
    assert_eq!(failures.len(), 1);
    assert_source(&failures[0], expected);
    let records = runtime.context().log_buffer().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, Level::Error);
    assert_source(records[0].failure.as_ref().unwrap(), expected);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Combined,
    Coalesced,
    Removed,
}

impl Scenario {
    fn fixture(self) -> (&'static str, &'static str) {
        match self {
            Self::Combined => (
                "combined",
                include_str!("../../../tests/parity/combined.txt"),
            ),
            Self::Coalesced => (
                "coalesced",
                include_str!("../../../tests/parity/coalesced.txt"),
            ),
            Self::Removed => ("removed", include_str!("../../../tests/parity/removed.txt")),
        }
    }
}

struct Provider {
    version: u32,
    open: Rc<Cell<bool>>,
}

async fn scenario(mode: Scenario) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let baseline = root.get_effects().unwrap();
                assert!(root.registry().is_empty().unwrap());
                let key = ServiceKey::<Provider>::new("parityProvider");
                let trace = Rc::new(RefCell::new(Vec::new()));
                let provider = root
                    .plugin(
                        &Plugin::new("provider", {
                            let key = key.clone();
                            let trace = trace.clone();
                            move |ctx, version| {
                                let key = key.clone();
                                let trace = trace.clone();
                                async move {
                                    let open = Rc::new(Cell::new(true));
                                    trace.borrow_mut().push(format!("provider-open {version}"));
                                    ctx.provide(
                                        &key,
                                        Provider {
                                            version,
                                            open: open.clone(),
                                        },
                                    )?;
                                    ctx.on_dispose_sync(move || {
                                        assert!(open.replace(false));
                                        trace
                                            .borrow_mut()
                                            .push(format!("provider-close {version}"));
                                        Ok::<_, Infallible>(())
                                    })?;
                                    Ok::<_, BoxError>(())
                                }
                            }
                        }),
                        1,
                    )
                    .unwrap();
                provider.ready().unwrap().await.unwrap();
                let old_provider = root.get_direct(&key).unwrap().unwrap();
                let (entered, cleanup_started) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let (latest_entered, latest_started) = oneshot::channel();
                let (latest_release, latest_gate) = oneshot::channel();
                let latest = RefCell::new(Some((latest_entered, latest_gate)));
                let gates = RefCell::new(if mode == Scenario::Combined {
                    None
                } else {
                    Some((entered, gate))
                });
                let consumer_plugin = Plugin::new("consumer", {
                    let key = key.clone();
                    let trace = trace.clone();
                    move |ctx, config: &'static str| {
                        let key = key.clone();
                        let trace = trace.clone();
                        let pause = gates.borrow_mut().take();
                        let latest = if mode == Scenario::Coalesced && config == "new" {
                            latest.borrow_mut().take()
                        } else {
                            None
                        };
                        async move {
                            let version = ctx.get(&key)?.unwrap().version;
                            trace.borrow_mut().push(format!("apply {version} {config}"));
                            let cleanup_trace = trace.clone();
                            ctx.clone().on_dispose(move || async move {
                                if let Some((entered, gate)) = pause {
                                    cleanup_trace
                                        .borrow_mut()
                                        .push(format!("cleanup-gated {version} {config}"));
                                    entered.send(()).unwrap();
                                    gate.await.unwrap();
                                }
                                let old = ctx.get(&key)?.unwrap();
                                assert_eq!(old.version, version);
                                assert!(old.open.get(), "provider closed before final read");
                                cleanup_trace
                                    .borrow_mut()
                                    .push(format!("read {version} {config}"));
                                Ok::<_, BoxError>(())
                            })?;
                            if let Some((entered, gate)) = latest {
                                entered.send(()).unwrap();
                                gate.await.unwrap();
                            }
                            Ok::<_, BoxError>(())
                        }
                    }
                })
                .injecting([key.required()]);
                let consumer = root.plugin(&consumer_plugin, "old").unwrap();
                consumer.ready().unwrap().await.unwrap();
                let mut provider_tasks = vec![Box::pin(provider.update(2).unwrap())];
                let mut consumer_tasks = vec![Box::pin(
                    consumer
                        .update(if mode == Scenario::Combined {
                            "new"
                        } else {
                            "middle"
                        })
                        .unwrap(),
                )];
                if mode != Scenario::Combined {
                    cleanup_started.await.unwrap();
                    provider_tasks.push(Box::pin(provider.update(3).unwrap()));
                    consumer_tasks.push(Box::pin(consumer.update("new").unwrap()));
                    if mode == Scenario::Removed {
                        let removed = root.registry().remove(&consumer_plugin).unwrap();
                        assert_eq!(removed.len(), 1);
                        consumer_tasks.extend(removed.into_iter().map(Box::pin));
                        assert!(!root.registry().contains(&consumer_plugin).unwrap());
                        trace.borrow_mut().push("registry consumer=removed".into());
                    }
                    assert_eq!(provider.state(), FiberState::Unloading);
                    assert_eq!(consumer.state(), FiberState::Unloading);
                    assert!(old_provider.open.get());
                    assert!(
                        provider_tasks
                            .iter_mut()
                            .chain(consumer_tasks.iter_mut())
                            .all(|task| task.as_mut().now_or_never().is_none())
                    );
                    trace
                        .borrow_mut()
                        .push("blocked provider=open waiters=pending".into());
                    release.send(()).unwrap();
                }
                if mode == Scenario::Coalesced {
                    latest_started.await.unwrap();
                }
                let consumer_state = if mode == Scenario::Removed {
                    FiberState::Disposed
                } else {
                    FiberState::Active
                };
                for task in provider_tasks {
                    assert_eq!(task.await.unwrap(), FiberState::Active);
                }
                for task in consumer_tasks {
                    assert_eq!(
                        task.await.unwrap(),
                        if mode == Scenario::Removed {
                            FiberState::Disposed
                        } else {
                            FiberState::Pending
                        }
                    );
                }
                let mut ready = Box::pin(consumer.ready().unwrap());
                if mode == Scenario::Coalesced {
                    assert_eq!(consumer.state(), FiberState::Loading);
                    assert!(ready.as_mut().now_or_never().is_none());
                    trace
                        .borrow_mut()
                        .push("latest-load consumer=Loading ready=pending".into());
                    latest_release.send(()).unwrap();
                }
                assert_eq!(ready.await.unwrap(), consumer_state);
                trace.borrow_mut().push(format!(
                    "update-waiters provider=Active consumer={}",
                    if mode == Scenario::Removed {
                        "Disposed"
                    } else {
                        "Pending"
                    }
                ));
                assert_eq!(provider.state(), FiberState::Active);
                assert_eq!(consumer.state(), consumer_state);
                trace.borrow_mut().push(format!(
                    "settled provider=Active consumer={consumer_state:?}"
                ));
                let report = root.drain().unwrap().await.unwrap();
                assert!(report.failures().is_empty());
                assert!(root.registry().is_empty().unwrap());
                assert!(root.get_unchecked(&key).unwrap().is_none());
                assert_eq!(root.get_effects().unwrap(), baseline);
                assert!(runtime.take_failures().is_empty());
                trace.borrow_mut().push("baseline restored".into());
                let (name, expected) = mode.fixture();
                assert_trace(name, expected, &trace.borrow());
            })
            .await
            .unwrap();
    })
    .await
    .expect("parity scenario stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn combined_provider_and_consumer_update() {
    scenario(Scenario::Combined).await;
}

#[tokio::test(flavor = "current_thread")]
async fn latest_target_during_gated_cleanup() {
    scenario(Scenario::Coalesced).await;
}

#[tokio::test(flavor = "current_thread")]
async fn registry_removed_consumer_retains_provider_and_cannot_reload() {
    scenario(Scenario::Removed).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FailureMode {
    Superseded,
    Latest,
}

async fn failure_scenario(mode: FailureMode) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let baseline = root.get_effects().unwrap();
                let trace = Rc::new(RefCell::new(Vec::new()));
                let failure = Rc::new(std::io::Error::other(if mode == FailureMode::Latest {
                    "latest load failed"
                } else {
                    "superseded load"
                }));
                let (load_started, load_entered) = oneshot::channel();
                let (load_release, load_gate) = oneshot::channel();
                let load = RefCell::new(Some((load_started, load_gate)));
                let (rollback_started, rollback_entered) = oneshot::channel();
                let (rollback_release, rollback_gate) = oneshot::channel();
                let rollback = RefCell::new(Some((rollback_started, rollback_gate)));
                let failing_config = if mode == FailureMode::Latest { 2 } else { 1 };
                let fiber = root
                    .plugin(
                        &Plugin::new("worker", {
                            let trace = trace.clone();
                            let failure = failure.clone();
                            move |ctx, config: u32| {
                                let load = if config == 1 {
                                    load.borrow_mut().take()
                                } else {
                                    None
                                };
                                let rollback = if config == failing_config {
                                    rollback.borrow_mut().take()
                                } else {
                                    None
                                };
                                let trace = trace.clone();
                                let failure = failure.clone();
                                async move {
                                    trace.borrow_mut().push(format!("apply {config}"));
                                    let cleanup_trace = trace.clone();
                                    ctx.on_dispose(move || async move {
                                        if let Some((started, gate)) = rollback {
                                            cleanup_trace
                                                .borrow_mut()
                                                .push(format!("rollback-gated {config}"));
                                            started.send(()).unwrap();
                                            gate.await.unwrap();
                                        }
                                        cleanup_trace
                                            .borrow_mut()
                                            .push(format!("dispose {config}"));
                                        Ok::<_, Infallible>(())
                                    })?;
                                    if let Some((started, gate)) = load {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                    }
                                    if config == failing_config {
                                        trace.borrow_mut().push(format!("fail {config}"));
                                        return Err::<(), BoxError>(LoadFailure(failure).into());
                                    }
                                    Ok(())
                                }
                            }
                        }),
                        if mode == FailureMode::Latest { 0 } else { 1 },
                    )
                    .unwrap();
                let first = if mode == FailureMode::Latest {
                    fiber.ready().unwrap().await.unwrap();
                    fiber.update(1).unwrap()
                } else {
                    fiber.ready().unwrap()
                };
                load_entered.await.unwrap();
                let mut tasks = [Box::pin(first), Box::pin(fiber.update(2).unwrap())];
                if mode == FailureMode::Latest {
                    drop(fiber.update(2).unwrap());
                    trace.borrow_mut().push("update-waiter abandoned".into());
                }
                load_release.send(()).unwrap();
                rollback_entered.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Unloading);
                assert!(
                    tasks
                        .iter_mut()
                        .all(|task| task.as_mut().now_or_never().is_none())
                );
                trace
                    .borrow_mut()
                    .push("blocked rollback waiters=pending".into());
                rollback_release.send(()).unwrap();
                for task in tasks {
                    let result = task.await;
                    if mode == FailureMode::Latest {
                        assert_failure(result.unwrap_err(), &failure);
                    } else {
                        assert_eq!(result.unwrap(), FiberState::Active);
                    }
                }
                let final_state = if mode == FailureMode::Latest {
                    FiberState::Failed
                } else {
                    FiberState::Active
                };
                assert_eq!(fiber.state(), final_state);
                assert_eq!(*fiber.config(), 2);
                assert_eq!(
                    fiber.context().get_effects().unwrap().len(),
                    usize::from(mode == FailureMode::Superseded)
                );
                trace.borrow_mut().push(format!("waiters={final_state:?}"));
                assert_diagnostic(&runtime, &failure);
                trace.borrow_mut().push("diagnostics=1".into());
                assert!(root.drain().unwrap().await.unwrap().failures().is_empty());
                assert!(root.registry().is_empty().unwrap());
                assert_eq!(root.get_effects().unwrap(), baseline);
                assert!(runtime.take_failures().is_empty());
                assert_eq!(root.log_buffer().unwrap().len(), 1);
                trace.borrow_mut().push("baseline restored".into());
                let (name, expected) = match mode {
                    FailureMode::Superseded => (
                        "superseded-failure",
                        include_str!("../../../tests/parity/superseded-failure.txt"),
                    ),
                    FailureMode::Latest => (
                        "latest-failure",
                        include_str!("../../../tests/parity/latest-failure.txt"),
                    ),
                };
                assert_trace(name, expected, &trace.borrow());
            })
            .await
            .unwrap();
    })
    .await
    .expect("failure parity scenario stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn superseded_failure_rolls_back_without_poisoning_latest_target() {
    failure_scenario(FailureMode::Superseded).await;
}

#[tokio::test(flavor = "current_thread")]
async fn latest_failure_reaches_waiters_after_rollback_despite_abandoned_update() {
    failure_scenario(FailureMode::Latest).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_consumer_ignores_provider_refresh_and_update_awaits_recovery() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let baseline = root.get_effects().unwrap();
                let trace = Rc::new(RefCell::new(Vec::new()));
                let key = ServiceKey::<u32>::new("parityRecovery");
                let old = root.provide(&key, 1).unwrap();
                trace.borrow_mut().push("provider 1".into());
                let failure = Rc::new(std::io::Error::other("initial failure"));
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let recovery = RefCell::new(Some((started, gate)));
                let attempts = Rc::new(Cell::new(0));
                let fiber = root
                    .plugin(
                        &Plugin::new("consumer", {
                            let key = key.clone();
                            let trace = trace.clone();
                            let attempts = attempts.clone();
                            let failure = failure.clone();
                            move |ctx, config: u32| {
                                let key = key.clone();
                                let trace = trace.clone();
                                let attempts = attempts.clone();
                                let failure = failure.clone();
                                let recovery = if config == 1 {
                                    recovery.borrow_mut().take()
                                } else {
                                    None
                                };
                                async move {
                                    attempts.set(attempts.get() + 1);
                                    let version = *ctx.get(&key)?.unwrap();
                                    trace.borrow_mut().push(format!("apply {version} {config}"));
                                    let cleanup_trace = trace.clone();
                                    ctx.clone().on_dispose(move || async move {
                                        assert_eq!(*ctx.get(&key)?.unwrap(), version);
                                        cleanup_trace
                                            .borrow_mut()
                                            .push(format!("dispose {version} {config}"));
                                        Ok::<_, BoxError>(())
                                    })?;
                                    if let Some((started, gate)) = recovery {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                    }
                                    if config == 0 {
                                        trace.borrow_mut().push("fail initial".into());
                                        return Err::<(), BoxError>(LoadFailure(failure).into());
                                    }
                                    Ok(())
                                }
                            }
                        })
                        .injecting([key.required()]),
                        0,
                    )
                    .unwrap();
                assert_failure(fiber.ready().unwrap().await.unwrap_err(), &failure);
                assert_eq!(fiber.state(), FiberState::Failed);
                assert!(fiber.context().get_effects().unwrap().is_empty());
                trace.borrow_mut().push("failed".into());
                old.withdraw().unwrap().await.unwrap();
                root.provide(&key, 2).unwrap();
                trace.borrow_mut().push("provider 2".into());
                assert_failure(fiber.ready().unwrap().await.unwrap_err(), &failure);
                assert_eq!(fiber.state(), FiberState::Failed);
                assert_eq!(attempts.get(), 1);
                trace
                    .borrow_mut()
                    .push("refresh consumer=Failed attempts=1".into());
                let mut updating = Box::pin(fiber.update(1).unwrap());
                entered.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Loading);
                assert!(updating.as_mut().now_or_never().is_none());
                trace
                    .borrow_mut()
                    .push("recovery consumer=Loading waiter=pending".into());
                release.send(()).unwrap();
                assert_eq!(updating.await.unwrap(), FiberState::Active);
                assert_eq!(fiber.state(), FiberState::Active);
                assert_eq!(attempts.get(), 2);
                assert_eq!(*fiber.config(), 1);
                trace.borrow_mut().push("waiter=Active".into());
                assert_diagnostic(&runtime, &failure);
                trace.borrow_mut().push("diagnostics=1".into());
                assert!(root.drain().unwrap().await.unwrap().failures().is_empty());
                assert!(root.registry().is_empty().unwrap());
                assert!(root.get_unchecked(&key).unwrap().is_none());
                assert_eq!(root.get_effects().unwrap(), baseline);
                assert!(runtime.take_failures().is_empty());
                assert_eq!(root.log_buffer().unwrap().len(), 1);
                trace.borrow_mut().push("baseline restored".into());
                assert_trace(
                    "explicit-recovery",
                    include_str!("../../../tests/parity/explicit-recovery.txt"),
                    &trace.borrow(),
                );
            })
            .await
            .unwrap();
    })
    .await
    .expect("recovery parity scenario stalled");
}
