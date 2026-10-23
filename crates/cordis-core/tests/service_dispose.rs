//! Exact-binding and cleanup partial-order evidence against service-dispose.spec.ts.
use cordis_core::{
    BoxError, Context, DispatchOptions, Disposer, Event, FiberHandle, FiberState, LifecycleError,
    Listener, Plugin, Runtime, ServiceKey,
};
use futures_util::FutureExt;
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("service cleanup stalled");
}

#[derive(Clone)]
struct Probe {
    open: Rc<Cell<bool>>,
    trace: Rc<RefCell<Vec<&'static str>>>,
}
impl Probe {
    fn new() -> Self {
        Self {
            open: Rc::new(Cell::new(false)),
            trace: Rc::new(RefCell::new(Vec::new())),
        }
    }
    fn read(&self) {
        assert!(self.open.get(), "provider closed before dependent cleanup");
        self.trace.borrow_mut().push("read");
    }
    fn close(&self) {
        self.open.set(false);
        self.trace.borrow_mut().push("closed");
    }
}

async fn provider(root: &Context, key: &ServiceKey<Probe>, probe: &Probe) -> FiberHandle<()> {
    let plugin = Plugin::new("provider", {
        let key = key.clone();
        let probe = probe.clone();
        move |ctx, ()| {
            let key = key.clone();
            let probe = probe.clone();
            async move {
                probe.open.set(true);
                ctx.provide(&key, probe.clone())?;
                ctx.on_dispose_sync(move || {
                    probe.close();
                    Ok::<_, Infallible>(())
                })?;
                Ok::<_, BoxError>(())
            }
        }
    });
    let fiber = root.plugin(&plugin, ()).unwrap();
    fiber.ready().unwrap().await.unwrap();
    fiber
}

fn gated_consumer(
    root: &Context,
    key: &ServiceKey<Probe>,
) -> (FiberHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (started, entered) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    let gates = RefCell::new(Some((started, gate)));
    let consumer = root
        .inject([key.required()], {
            let key = key.clone();
            move |ctx| {
                let key = key.clone();
                let gate = gates.borrow_mut().take();
                async move {
                    ctx.clone().on_dispose(move || async move {
                        if let Some((started, gate)) = gate {
                            started.send(()).unwrap();
                            gate.await.unwrap();
                        }
                        ctx.get(&key)?.unwrap().read();
                        Ok::<_, BoxError>(())
                    })?;
                    Ok::<_, BoxError>(())
                }
            }
        })
        .unwrap();
    (consumer, entered, release)
}

#[tokio::test(flavor = "current_thread")]
async fn withdrawal_waits_for_a_consumer_already_removed_from_the_registry() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Probe>::new("foo");
                let probe = Probe::new();
                probe.open.set(true);
                let publication = root.provide(&key, probe.clone()).unwrap();
                let (consumer, entered, release) = gated_consumer(&root, &key);
                consumer.ready().unwrap().await.unwrap();
                let disposing = consumer.dispose().unwrap();
                assert!(root.registry().is_empty().unwrap());
                entered.await.unwrap();
                let mut withdrawal = Box::pin(publication.withdraw().unwrap());
                assert!(root.get_direct(&key).unwrap().is_none());
                assert!(withdrawal.as_mut().now_or_never().is_none());
                release.send(()).unwrap();
                disposing.await.unwrap();
                withdrawal.await.unwrap();
                assert_eq!(*probe.trace.borrow(), ["read"]);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn one_withdrawal_does_not_wait_for_another_publication_from_the_same_provider() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let foo = ServiceKey::<Probe>::new("foo");
                let bar = ServiceKey::<Probe>::new("bar");
                let probe = Probe::new();
                let provider = provider(&root, &foo, &probe).await;
                let withdrawal = provider.context().provide(&bar, probe.clone()).unwrap();
                let (consumer, entered, release) = gated_consumer(&root, &foo);
                consumer.ready().unwrap().await.unwrap();
                let disposal = consumer.dispose().unwrap();
                entered.await.unwrap();
                withdrawal.withdraw().unwrap().await.unwrap();
                assert!(probe.open.get());
                release.send(()).unwrap();
                disposal.await.unwrap();
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_resources_survive_all_disposal_orders_and_restart() {
    for action in [
        "provider",
        "root",
        "provider-first",
        "consumer-first",
        "restart",
    ] {
        bounded(async {
            let runtime = Runtime::new();
            let root = runtime.context();
            runtime
                .run(async {
                    let key = ServiceKey::<Probe>::new("foo");
                    let probe = Probe::new();
                    let provider = provider(&root, &key, &probe).await;
                    let (consumer, entered, release) = gated_consumer(&root, &key);
                    consumer.ready().unwrap().await.unwrap();
                    let task: cordis_core::LocalFuture<()> = match action {
                        "root" => {
                            let task = root.drain().unwrap();
                            Box::pin(async move {
                                task.await.unwrap();
                            })
                        }
                        "restart" => {
                            let task = provider.restart().unwrap();
                            Box::pin(async move {
                                task.await.unwrap();
                            })
                        }
                        "provider-first" => {
                            let first = provider.dispose().unwrap();
                            let second = consumer.dispose().unwrap();
                            Box::pin(async move {
                                first.await.unwrap();
                                second.await.unwrap();
                            })
                        }
                        "consumer-first" => {
                            let first = consumer.dispose().unwrap();
                            let second = provider.dispose().unwrap();
                            Box::pin(async move {
                                first.await.unwrap();
                                second.await.unwrap();
                            })
                        }
                        _ => {
                            let task = provider.dispose().unwrap();
                            Box::pin(async move {
                                task.await.unwrap();
                            })
                        }
                    };
                    entered.await.unwrap();
                    assert!(probe.open.get(), "{action}");
                    assert!(root.get_direct(&key).unwrap().is_none(), "{action}");
                    assert!(root.get_unchecked(&key).unwrap().is_some(), "{action}");
                    release.send(()).unwrap();
                    task.await;
                    assert_eq!(*probe.trace.borrow(), ["read", "closed"], "{action}");
                    if action == "restart" {
                        assert_eq!(consumer.ready().unwrap().await.unwrap(), FiberState::Active);
                        assert!(probe.open.get());
                    }
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn transitive_cleanup_retains_providers_while_unrelated_owners_and_siblings_progress() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let foo = ServiceKey::<Probe>::new("foo");
                let bar = ServiceKey::<Probe>::new("bar");
                let probe = Probe::new();
                provider(&root, &foo, &probe).await;
                let middle = root
                    .inject([foo.required()], {
                        let foo = foo.clone();
                        let bar = bar.clone();
                        let probe = probe.clone();
                        move |ctx| {
                            let foo = foo.clone();
                            let bar = bar.clone();
                            let probe = probe.clone();
                            async move {
                                ctx.provide(&bar, probe)?;
                                ctx.clone().on_dispose_sync(move || {
                                    ctx.get(&foo)?.unwrap().read();
                                    Ok::<_, BoxError>(())
                                })?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .unwrap();
                middle.ready().unwrap().await.unwrap();
                let (leaf, entered, release) = gated_consumer(&root, &bar);
                leaf.ready().unwrap().await.unwrap();
                let (sibling_done, sibling) = oneshot::channel();
                leaf.context()
                    .on_dispose_sync(move || {
                        sibling_done.send(()).unwrap();
                        Ok::<_, Infallible>(())
                    })
                    .unwrap();
                let (independent_done, independent) = oneshot::channel();
                let gate = RefCell::new(Some(independent_done));
                let independent_key = ServiceKey::<()>::new("unrelated");
                root.plugin(
                    &Plugin::new("unrelated", move |ctx, ()| {
                        let gate = gate.borrow_mut().take().unwrap();
                        let key = independent_key.clone();
                        async move {
                            ctx.provide(&key, ())?;
                            ctx.on_dispose_sync(move || {
                                gate.send(()).unwrap();
                                Ok::<_, Infallible>(())
                            })?;
                            Ok::<_, BoxError>(())
                        }
                    }),
                    (),
                )
                .unwrap()
                .ready()
                .unwrap()
                .await
                .unwrap();
                let disposal = root.drain().unwrap();
                entered.await.unwrap();
                sibling.await.unwrap();
                independent.await.unwrap();
                assert!(probe.open.get());
                assert!(probe.trace.borrow().is_empty());
                release.send(()).unwrap();
                disposal.await.unwrap();
                assert_eq!(*probe.trace.borrow(), ["read", "read", "closed"]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn snapshots_survive_in_flight_apply_and_partial_failure_rollback() {
    for fail in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            let root = runtime.context();
            runtime
                .run(async {
                    let key = ServiceKey::<Probe>::new("foo");
                    let probe = Probe::new();
                    let provider = provider(&root, &key, &probe).await;
                    let (started, entered) = oneshot::channel();
                    let (release, gate) = oneshot::channel();
                    let gates = RefCell::new(Some((started, gate)));
                    let consumer = root
                        .inject([key.required()], {
                            let key = key.clone();
                            move |ctx| {
                                let key = key.clone();
                                let (started, gate) = gates.borrow_mut().take().unwrap();
                                async move {
                                    let cleanup_ctx = ctx.clone();
                                    let cleanup_key = key.clone();
                                    ctx.on_dispose_sync(move || {
                                        cleanup_ctx.get(&cleanup_key)?.unwrap().read();
                                        Ok::<_, BoxError>(())
                                    })?;
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                    ctx.get(&key)?.unwrap().read();
                                    if fail {
                                        return Err(
                                            std::io::Error::other("obsolete setup failed").into()
                                        );
                                    }
                                    Ok::<_, BoxError>(())
                                }
                            }
                        })
                        .unwrap();
                    entered.await.unwrap();
                    let disposal = provider.dispose().unwrap();
                    assert!(probe.open.get());
                    release.send(()).unwrap();
                    disposal.await.unwrap();
                    assert_eq!(
                        consumer.ready().unwrap().await.unwrap(),
                        FiberState::Pending
                    );
                    assert_eq!(*probe.trace.borrow(), ["read", "read", "closed"]);
                    assert_eq!(runtime.take_failures().len(), usize::from(fail));
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_errors_release_dependencies_and_keep_final_provider_listeners_alive() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Probe>::new("foo");
                let probe = Probe::new();
                let provider = provider(&root, &key, &probe).await;
                let event = Event::<()>::symbol("final event");
                let received = Rc::new(Cell::new(0));
                provider
                    .context()
                    .on(
                        &event,
                        Listener::sync({
                            let received = received.clone();
                            move |_, ()| {
                                received.set(received.get() + 1);
                                Ok::<_, Infallible>(ControlFlow::Continue(()))
                            }
                        }),
                    )
                    .unwrap();
                root.inject([key.required()], {
                    let key = key.clone();
                    let event = event.clone();
                    move |ctx| {
                        let key = key.clone();
                        let event = event.clone();
                        async move {
                            ctx.clone().on_dispose(move || async move {
                                ctx.get(&key)?.unwrap().read();
                                ctx.emit(&event, (), DispatchOptions::default())?.await?;
                                Err::<(), BoxError>(std::io::Error::other("cleanup failed").into())
                            })?;
                            Ok::<_, BoxError>(())
                        }
                    }
                })
                .unwrap()
                .ready()
                .unwrap()
                .await
                .unwrap();
                root.drain().unwrap().await.unwrap();
                assert_eq!(received.get(), 1);
                assert_eq!(*probe.trace.borrow(), ["read", "closed"]);
                assert_eq!(runtime.take_failures().len(), 1);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn old_withdrawal_does_not_wait_for_a_replacement_load() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<u32>::new("version");
                let old = root.provide(&key, 1).unwrap();
                let (old_started, old_cleanup) = oneshot::channel();
                let (release_old, old_gate) = oneshot::channel();
                let (new_started, new_setup) = oneshot::channel();
                let (release_new, new_gate) = oneshot::channel();
                let old_gates = RefCell::new(Some((old_started, old_gate)));
                let new_gates = RefCell::new(Some((new_started, new_gate)));
                let versions = Rc::new(RefCell::new(Vec::new()));
                let consumer = root
                    .inject([key.required()], {
                        let key = key.clone();
                        let versions = versions.clone();
                        move |ctx| {
                            let key = key.clone();
                            let versions = versions.clone();
                            let old = old_gates.borrow_mut().take();
                            let new = if old.is_none() {
                                new_gates.borrow_mut().take()
                            } else {
                                None
                            };
                            async move {
                                let version = *ctx.get(&key)?.unwrap();
                                versions.borrow_mut().push(version);
                                if let Some((started, gate)) = new {
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                }
                                ctx.clone().on_dispose(move || async move {
                                    if let Some((started, gate)) = old {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                    }
                                    versions.borrow_mut().push(*ctx.get(&key)?.unwrap());
                                    Ok::<_, BoxError>(())
                                })?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .unwrap();
                consumer.ready().unwrap().await.unwrap();
                let withdrawal = old.withdraw().unwrap();
                root.provide(&key, 2).unwrap();
                old_cleanup.await.unwrap();
                release_old.send(()).unwrap();
                new_setup.await.unwrap();
                withdrawal.await.unwrap();
                assert_eq!(consumer.state(), FiberState::Loading);
                assert_eq!(*versions.borrow(), [1, 1, 2]);
                release_new.send(()).unwrap();
                consumer.ready().unwrap().await.unwrap();
                root.drain().unwrap().await.unwrap();
                assert_eq!(*versions.borrow(), [1, 1, 2, 2]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn equal_value_replacement_during_initialization_rolls_back_the_exact_old_snapshot() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<u32>::new("version");
                let shared = Rc::new(3);
                let key = ServiceKey::<Rc<u32>>::new(key.name());
                let old = root.provide(&key, shared.clone()).unwrap();
                let old_id = old.id().clone();
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let timeline = Rc::new(RefCell::new(Vec::new()));
                let loads = Rc::new(Cell::new(0));
                let consumer = root
                    .inject([key.required()], {
                        let key = key.clone();
                        let timeline = timeline.clone();
                        let loads = loads.clone();
                        move |ctx| {
                            loads.set(loads.get() + 1);
                            let load = loads.get();
                            let key = key.clone();
                            let timeline = timeline.clone();
                            let gate = gates.borrow_mut().take();
                            async move {
                                timeline.borrow_mut().push(load);
                                let cleanup = ctx.clone();
                                let cleanup_key = key.clone();
                                let cleanup_timeline = timeline.clone();
                                ctx.on_dispose_sync(move || {
                                    assert_eq!(**cleanup.get(&cleanup_key)?.unwrap(), 3);
                                    cleanup_timeline.borrow_mut().push(-load);
                                    Ok::<_, BoxError>(())
                                })?;
                                if let Some((started, gate)) = gate {
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                    assert_eq!(**ctx.get(&key)?.unwrap(), 3);
                                }
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .unwrap();
                entered.await.unwrap();
                let withdrawal = old.withdraw().unwrap();
                let next = root.provide(&key, shared).unwrap();
                assert_ne!(next.id(), &old_id);
                release.send(()).unwrap();
                consumer.ready().unwrap().await.unwrap();
                withdrawal.await.unwrap();
                consumer.dispose().unwrap().await.unwrap();
                assert_eq!(*timeline.borrow(), [1, -1, 2, -2]);
                assert!(
                    root.get_direct(&key).unwrap().is_some(),
                    "old withdrawal must not erase replacement"
                );
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_shutdown_waits_for_current_and_already_withdrawn_publications() {
    for withdrawn_first in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            let root = runtime.context();
            runtime
                .run(async {
                    let foo = ServiceKey::<Probe>::new("foo");
                    let bar = ServiceKey::<Probe>::new("bar");
                    let probe = Probe::new();
                    let provider = provider(&root, &foo, &probe).await;
                    let old = provider.context().provide(&bar, probe.clone()).unwrap();
                    let (current, current_entered, current_release) = gated_consumer(&root, &foo);
                    let (retired, retired_entered, retired_release) = gated_consumer(&root, &bar);
                    current.ready().unwrap().await.unwrap();
                    retired.ready().unwrap().await.unwrap();
                    let withdrawal = old.withdraw().unwrap();
                    provider.context().provide(&bar, probe.clone()).unwrap();
                    let disposal = provider.dispose().unwrap();
                    current_entered.await.unwrap();
                    retired_entered.await.unwrap();
                    assert!(probe.open.get());
                    if withdrawn_first {
                        retired_release.send(()).unwrap();
                        withdrawal.await.unwrap();
                        assert!(probe.open.get());
                        current_release.send(()).unwrap();
                    } else {
                        current_release.send(()).unwrap();
                        current.ready().unwrap().await.unwrap();
                        assert!(probe.open.get());
                        retired_release.send(()).unwrap();
                        withdrawal.await.unwrap();
                    }
                    disposal.await.unwrap();
                    assert_eq!(*probe.trace.borrow(), ["read", "read", "closed"]);
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ancestor_injection_is_retained_until_a_child_finishes_inherited_cleanup() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Probe>::new("foo");
                let probe = Probe::new();
                provider(&root, &key, &probe).await;
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let child = Rc::new(RefCell::new(None));
                root.inject([key.required()], {
                    let child = child.clone();
                    let key = key.clone();
                    move |ctx| {
                        let key = key.clone();
                        let (started, gate) = gates.borrow_mut().take().unwrap();
                        let gates = RefCell::new(Some((started, gate)));
                        let fiber = ctx
                            .plugin(
                                &Plugin::new("child", move |ctx, ()| {
                                    let key = key.clone();
                                    let (started, gate) = gates.borrow_mut().take().unwrap();
                                    async move {
                                        ctx.clone().on_dispose(move || async move {
                                            started.send(()).unwrap();
                                            gate.await.unwrap();
                                            ctx.get(&key)?.unwrap().read();
                                            Ok::<_, BoxError>(())
                                        })?;
                                        Ok::<_, BoxError>(())
                                    }
                                }),
                                (),
                            )
                            .unwrap();
                        *child.borrow_mut() = Some(fiber);
                        async { Ok::<_, Infallible>(()) }
                    }
                })
                .unwrap()
                .ready()
                .unwrap()
                .await
                .unwrap();
                let child = child.borrow_mut().take().unwrap();
                child.ready().unwrap().await.unwrap();
                let disposal = root.drain().unwrap();
                entered.await.unwrap();
                assert!(probe.open.get());
                release.send(()).unwrap();
                disposal.await.unwrap();
                assert_eq!(*probe.trace.borrow(), ["read", "closed"]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_child_explicitly_injecting_its_parent_service_does_not_deadlock_parent_unload() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Probe>::new("foo");
                let probe = Probe::new();
                let provider = provider(&root, &key, &probe).await;
                let (child, entered, release) = gated_consumer(&provider.context(), &key);
                child.ready().unwrap().await.unwrap();
                let disposal = provider.dispose().unwrap();
                entered.await.unwrap();
                assert!(probe.open.get());
                release.send(()).unwrap();
                disposal.await.unwrap();
                assert_eq!(child.state(), FiberState::Disposed);
                assert_eq!(*probe.trace.borrow(), ["read", "closed"]);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn composed_publication_cleanup_retains_resources_on_manual_and_owner_disposal() {
    for owner_shutdown in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            let root = runtime.context();
            runtime
                .run(async {
                    let key = ServiceKey::<Probe>::new("foo");
                    let probe = Probe::new();
                    probe.open.set(true);
                    let group = Rc::new(RefCell::new(None));
                    let provider = root
                        .plugin(
                            &Plugin::new("composed", {
                                let key = key.clone();
                                let probe = probe.clone();
                                let group = group.clone();
                                move |ctx, ()| {
                                    let key = key.clone();
                                    let probe = probe.clone();
                                    let group = group.clone();
                                    async move {
                                        let effect = ctx.effect(|| {
                                            let resource = ctx.on_dispose_sync({
                                                let probe = probe.clone();
                                                move || {
                                                    probe.close();
                                                    Ok::<_, Infallible>(())
                                                }
                                            })?;
                                            let publication = ctx.provide(&key, probe)?;
                                            Ok::<_, BoxError>(Disposer::compose(vec![
                                                Disposer::from_effect(resource)?,
                                                Disposer::from_effect(publication.into_effect())?,
                                            ]))
                                        })?;
                                        *group.borrow_mut() = Some(effect);
                                        Ok::<_, BoxError>(())
                                    }
                                }
                            }),
                            (),
                        )
                        .unwrap();
                    provider.ready().unwrap().await.unwrap();
                    let (consumer, entered, release) = gated_consumer(&root, &key);
                    consumer.ready().unwrap().await.unwrap();
                    let disposal: cordis_core::LocalFuture<()> = if owner_shutdown {
                        let task = provider.dispose().unwrap();
                        Box::pin(async move {
                            task.await.unwrap();
                        })
                    } else {
                        let task = group.borrow_mut().take().unwrap().dispose().unwrap();
                        Box::pin(async move {
                            task.await.unwrap();
                        })
                    };
                    entered.await.unwrap();
                    assert!(probe.open.get());
                    release.send(()).unwrap();
                    disposal.await;
                    assert_eq!(*probe.trace.borrow(), ["read", "closed"]);
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inherited_own_provision_survives_a_composed_child_disposal() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<u32>::new("foo");
                let reads = Rc::new(Cell::new(0));
                let child_slot = Rc::new(RefCell::new(None));
                let owner = root
                    .plugin(
                        &Plugin::new("owner", {
                            let key = key.clone();
                            let reads = reads.clone();
                            let child_slot = child_slot.clone();
                            move |ctx, ()| {
                                let key = key.clone();
                                let reads = reads.clone();
                                let child_slot = child_slot.clone();
                                async move {
                                    ctx.provide(&key, 3)?;
                                    let child = ctx.plugin(
                                        &Plugin::new("child", move |ctx, ()| {
                                            let key = key.clone();
                                            let reads = reads.clone();
                                            async move {
                                                ctx.clone().on_dispose_sync(move || {
                                                    assert_eq!(*ctx.get(&key)?.unwrap(), 3);
                                                    reads.set(reads.get() + 1);
                                                    Ok::<_, BoxError>(())
                                                })?;
                                                Ok::<_, BoxError>(())
                                            }
                                        }),
                                        (),
                                    )?;
                                    let child_task = child.clone();
                                    let effect = ctx.on_dispose(move || {
                                        let task = child_task.dispose();
                                        async move {
                                            task?.await?;
                                            Ok::<_, LifecycleError>(())
                                        }
                                    })?;
                                    ctx.effect(|| {
                                        Ok::<_, BoxError>(Disposer::from_effect(effect)?)
                                    })?;
                                    *child_slot.borrow_mut() = Some(child);
                                    Ok::<_, BoxError>(())
                                }
                            }
                        }),
                        (),
                    )
                    .unwrap();
                owner.ready().unwrap().await.unwrap();
                let child = child_slot.borrow_mut().take().unwrap();
                child.ready().unwrap().await.unwrap();
                owner.dispose().unwrap().await.unwrap();
                assert_eq!(reads.get(), 1);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn host_destruction_cancels_a_provider_waiting_for_removed_consumer_cleanup() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let key = ServiceKey::<Probe>::new("foo");
    let probe = Probe::new();
    let (consumer_task, provider_task, release) = runtime
        .run(async {
            let provider = provider(&root, &key, &probe).await;
            let (consumer, entered, release) = gated_consumer(&root, &key);
            consumer.ready().unwrap().await.unwrap();
            let consumer_task = consumer.dispose().unwrap();
            let provider_task = provider.dispose().unwrap();
            entered.await.unwrap();
            (consumer_task, provider_task, release)
        })
        .await
        .unwrap();
    drop(runtime);
    assert!(matches!(
        consumer_task.await,
        Err(LifecycleError::HostStopped)
    ));
    assert!(matches!(
        provider_task.await,
        Err(LifecycleError::HostStopped)
    ));
    drop(release);
    assert!(
        probe.open.get(),
        "cancellation is not successful async cleanup"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn root_owned_resources_wait_for_consumers_and_root_remains_reusable() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Probe>::new("foo");
                let probe = Probe::new();
                probe.open.set(true);
                root.provide(&key, probe.clone()).unwrap();
                root.on_dispose_sync({
                    let probe = probe.clone();
                    move || {
                        probe.close();
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
                let (consumer, entered, release) = gated_consumer(&root, &key);
                consumer.ready().unwrap().await.unwrap();
                let task = root.drain().unwrap();
                entered.await.unwrap();
                assert!(probe.open.get());
                assert!(root.get_direct(&key).unwrap().is_none());
                release.send(()).unwrap();
                task.await.unwrap();
                assert_eq!(*probe.trace.borrow(), ["read", "closed"]);
                assert_eq!(consumer.state(), FiberState::Disposed);
                root.provide(&key, probe).unwrap();
                assert!(root.get_direct(&key).unwrap().is_some());
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_intent_coalesces_during_unload_without_loading_the_middle_publication() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<u32>::new("version");
                let first = root.provide(&key, 1).unwrap();
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let versions = Rc::new(RefCell::new(Vec::new()));
                let consumer = root
                    .inject([key.required()], {
                        let key = key.clone();
                        let versions = versions.clone();
                        move |ctx| {
                            let key = key.clone();
                            let versions = versions.clone();
                            let gate = gates.borrow_mut().take();
                            async move {
                                versions.borrow_mut().push(*ctx.get(&key)?.unwrap());
                                ctx.clone().on_dispose(move || async move {
                                    if let Some((started, gate)) = gate {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                    }
                                    assert_ne!(*ctx.get(&key)?.unwrap(), 2);
                                    Ok::<_, BoxError>(())
                                })?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .unwrap();
                consumer.ready().unwrap().await.unwrap();
                let first_withdrawal = first.withdraw().unwrap();
                entered.await.unwrap();
                let second = root.provide(&key, 2).unwrap();
                let waiter = consumer.ready().unwrap();
                second.withdraw().unwrap().await.unwrap();
                root.provide(&key, 3).unwrap();
                assert_eq!(consumer.state(), FiberState::Unloading);
                release.send(()).unwrap();
                assert_eq!(waiter.await.unwrap(), FiberState::Active);
                first_withdrawal.await.unwrap();
                assert_eq!(*versions.borrow(), [1, 3]);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn pending_effect_acquisition_and_its_final_cleanup_keep_the_committed_injection() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Probe>::new("foo");
                let probe = Probe::new();
                let provider = provider(&root, &key, &probe).await;
                let (setup_started, setup_entered) = oneshot::channel();
                let (release_setup, setup_gate) = oneshot::channel();
                let (cleanup_started, cleanup_entered) = oneshot::channel();
                let (release_cleanup, cleanup_gate) = oneshot::channel();
                let gates = RefCell::new(Some((
                    setup_started,
                    setup_gate,
                    cleanup_started,
                    cleanup_gate,
                )));
                let consumer = root
                    .inject([key.required()], {
                        let key = key.clone();
                        move |ctx| {
                            let key = key.clone();
                            let (setup_started, setup_gate, cleanup_started, cleanup_gate) =
                                gates.borrow_mut().take().unwrap();
                            async move {
                                let setup_context = ctx.clone();
                                ctx.effect_async(move || async move {
                                    setup_started.send(()).unwrap();
                                    setup_gate.await.unwrap();
                                    setup_context.get(&key)?.unwrap().read();
                                    Ok::<_, BoxError>(Disposer::new(move || async move {
                                        cleanup_started.send(()).unwrap();
                                        cleanup_gate.await.unwrap();
                                        setup_context.get(&key)?.unwrap().read();
                                        Ok::<_, BoxError>(())
                                    }))
                                })?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .unwrap();
                consumer.ready().unwrap().await.unwrap();
                setup_entered.await.unwrap();
                let task = provider.dispose().unwrap();
                assert!(probe.open.get());
                release_setup.send(()).unwrap();
                cleanup_entered.await.unwrap();
                assert!(probe.open.get());
                release_cleanup.send(()).unwrap();
                task.await.unwrap();
                assert_eq!(*probe.trace.borrow(), ["read", "read", "closed"]);
                assert_eq!(
                    consumer.ready().unwrap().await.unwrap(),
                    FiberState::Pending
                );
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}
