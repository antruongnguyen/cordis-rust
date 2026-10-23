//! Executable counterparts of tests/parity/lifecycle.spec.ts at the pinned TS core.
use cordis_core::{BoxError, FiberState, Plugin, Runtime, ServiceKey};
use futures_util::FutureExt;
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    rc::Rc,
};
use tokio::sync::oneshot;

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
                let actual = trace.borrow().join("\n") + "\n";
                let (name, expected) = mode.fixture();
                assert_eq!(actual, expected, "{name} lifecycle trace");
                if let Some(directory) = std::env::var_os("CORDIS_PARITY_OUTPUT") {
                    std::fs::write(
                        std::path::Path::new(&directory).join(format!("{name}.txt")),
                        actual,
                    )
                    .unwrap();
                }
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
