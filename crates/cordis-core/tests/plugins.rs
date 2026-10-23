//! M3 Registry/lifecycle evidence against pinned plugin.spec.ts and fiber.spec.ts.
use cordis_core::{
    AdmissionError, BoxError, Context, DispatchOptions, Event, FiberHandle, FiberState,
    LifecycleError, Listener, Plugin, Runtime,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    error::Error,
    fmt,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("plugin lifecycle test stalled");
}

fn empty(name: &str) -> Plugin<()> {
    Plugin::new(name, |_, ()| async { Ok::<_, Infallible>(()) })
}

#[tokio::test(flavor = "current_thread")]
async fn cloned_declarations_group_distinct_instances_and_empty_groups_are_pruned() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let applied = Rc::new(RefCell::new(Vec::new()));
    let plugin = Plugin::new("worker", {
        let applied = applied.clone();
        move |ctx, config: u32| {
            let applied = applied.clone();
            async move {
                applied.borrow_mut().push((ctx.fiber_id().clone(), config));
                Ok::<_, Infallible>(())
            }
        }
    });
    runtime
        .run(async {
            let first = root.plugin(&plugin, 1).unwrap();
            let second = root.plugin(&plugin.clone(), 2).unwrap();
            let independent = root.plugin(&empty("worker"), ()).unwrap();
            assert_eq!(first.state(), FiberState::Loading);
            assert_eq!(first.context().name(), "worker");
            assert_eq!(format!("{:?}", first.context()), "Context <worker>");
            assert_ne!(first.id(), second.id());
            assert_ne!(first.id(), root.fiber_id());
            assert_eq!(root.registry().len().unwrap(), 2);
            assert_eq!(root.registry().instances(&plugin).unwrap().len(), 2);
            assert_eq!(first.ready().unwrap().await.unwrap(), FiberState::Active);
            second.ready().unwrap().await.unwrap();
            independent.ready().unwrap().await.unwrap();
            assert_eq!(first.config().as_ref(), &1);
            assert_eq!(second.config().as_ref(), &2);
            assert_eq!(applied.borrow().len(), 2);
            assert_eq!(
                first.dispose().unwrap().await.unwrap(),
                FiberState::Disposed
            );
            assert!(root.registry().contains(&plugin).unwrap());
            assert_eq!(
                root.registry().instances(&plugin).unwrap(),
                [second.id().clone()]
            );
            second.dispose().unwrap().await.unwrap();
            assert!(!root.registry().contains(&plugin).unwrap());
            independent.dispose().unwrap().await.unwrap();
            assert!(root.registry().is_empty().unwrap());
            assert!(root.get_effects().unwrap().is_empty());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_handles_is_inert_and_explicit_disposal_unlinks_parent_ownership() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let probe = Rc::new(());
    let weak = Rc::downgrade(&probe);
    let calls = Rc::new(Cell::new(0));
    let plugin = Plugin::new("resident", {
        let calls = calls.clone();
        move |ctx: Context, probe: Rc<()>| {
            let calls = calls.clone();
            async move {
                ctx.on_dispose_sync(move || {
                    drop(probe);
                    calls.set(calls.get() + 1);
                    Ok::<_, Infallible>(())
                })?;
                Ok::<_, AdmissionError>(())
            }
        }
    });
    runtime
        .run(async {
            let fiber = root.plugin(&plugin, probe).unwrap();
            fiber.ready().unwrap().await.unwrap();
            assert_eq!(root.get_effects().unwrap()[0].label, "ctx.plugin()");
            drop(fiber);
            assert!(weak.upgrade().is_some());
            assert_eq!(calls.get(), 0);
            assert_eq!(root.registry().instances(&plugin).unwrap().len(), 1);
            for task in root.registry().remove(&plugin).unwrap() {
                task.await.unwrap();
            }
            assert_eq!(calls.get(), 1);
            assert!(weak.upgrade().is_none());
            assert!(root.get_effects().unwrap().is_empty());
            assert!(root.registry().is_empty().unwrap());
        })
        .await
        .unwrap();
}

#[derive(Debug)]
struct ConfigIssue {
    path: &'static str,
}
impl fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {}", self.path)
    }
}
impl Error for ConfigIssue {}

#[tokio::test(flavor = "current_thread")]
async fn validation_runs_per_load_and_source_remains_distinct_from_resolved_input() {
    let runtime = Runtime::new();
    let root = runtime.context().extend(String::from("inherited"));
    let validations = Rc::new(Cell::new(0));
    let inputs = Rc::new(RefCell::new(Vec::new()));
    let plugin = Plugin::validated(
        "validated",
        {
            let validations = validations.clone();
            move |config: &i32| {
                validations.set(validations.get() + 1);
                u32::try_from(*config).map_err(|_| ConfigIssue { path: "count" })
            }
        },
        {
            let inputs = inputs.clone();
            move |ctx, config: u32| {
                let inputs = inputs.clone();
                async move {
                    assert_eq!(&*ctx.metadata::<String>().unwrap(), "inherited");
                    assert_ne!(ctx.fiber_id(), ctx.root().fiber_id());
                    assert!(ctx.root().metadata::<String>().is_none());
                    inputs.borrow_mut().push(config);
                    Ok::<_, Infallible>(())
                }
            }
        },
    );
    runtime
        .run(async {
            let good = root.plugin(&plugin, 3).unwrap();
            let bad = root.plugin(&plugin, -1).unwrap();
            assert_eq!(
                validations.get(),
                0,
                "validation is inside the scheduled load"
            );
            good.ready().unwrap().await.unwrap();
            let failure = bad.ready().unwrap().await.unwrap_err();
            let LifecycleError::Callback(failure) = failure else {
                panic!("wrong failure family")
            };
            assert_eq!(
                failure
                    .source()
                    .unwrap()
                    .downcast_ref::<ConfigIssue>()
                    .unwrap()
                    .path,
                "count"
            );
            assert_eq!(bad.state(), FiberState::Failed);
            assert_eq!(*good.config(), 3_i32);
            assert_eq!(*inputs.borrow(), [3_u32]);
            let id = good.id().clone();
            good.restart().unwrap().await.unwrap();
            assert_eq!(good.id(), &id);
            assert_eq!(*inputs.borrow(), [3_u32, 3]);
            assert_eq!(validations.get(), 3);
            assert!(bad.restart().unwrap().await.is_err());
            assert_eq!(
                validations.get(),
                3,
                "failed restart must not silently retry"
            );
            assert_eq!(runtime.take_failures().len(), 1);
            let diagnostic = root
                .log_buffer()
                .unwrap()
                .into_iter()
                .find(|record| record.failure.is_some())
                .unwrap();
            assert_eq!(&diagnostic.fiber, bad.id());
            root.root().drain().unwrap().await.unwrap();
            assert_eq!(bad.state(), FiberState::Disposed);
            assert_eq!(good.state(), FiberState::Disposed);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn initial_failure_waits_for_partial_cleanup_and_failed_fiber_stays_resident() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((entered_tx, release_rx)));
        let event = Event::<()>::symbol("failed listener");
        let calls = Rc::new(Cell::new(0));
        let plugin = Plugin::new("fails", {
            let event = event.clone();
            let calls = calls.clone();
            move |ctx: Context, ()| {
                let event = event.clone();
                let calls = calls.clone();
                let (entered, release) = gates.borrow_mut().take().unwrap();
                async move {
                    ctx.on(
                        &event,
                        Listener::sync(move |_, ()| {
                            calls.set(calls.get() + 1);
                            Ok::<_, Infallible>(ControlFlow::Continue(()))
                        }),
                    )?;
                    ctx.on_dispose(move || async move {
                        entered.send(()).unwrap();
                        release.await.unwrap();
                        Ok::<_, Infallible>(())
                    })?;
                    Err::<(), BoxError>(std::io::Error::other("apply failed").into())
                }
            }
        });
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, ()).unwrap();
                entered_rx.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Unloading);
                assert!(root.registry().contains(&plugin).unwrap());
                let mut ready = Box::pin(fiber.ready().unwrap());
                assert!(
                    std::future::Future::poll(
                        ready.as_mut(),
                        &mut std::task::Context::from_waker(std::task::Waker::noop())
                    )
                    .is_pending()
                );
                release_tx.send(()).unwrap();
                assert!(ready.await.is_err());
                assert_eq!(fiber.state(), FiberState::Failed);
                assert!(fiber.context().get_effects().unwrap().is_empty());
                root.emit(&event, (), DispatchOptions::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(calls.get(), 0);
                assert_eq!(runtime.take_failures().len(), 1);
                fiber.dispose().unwrap().await.unwrap();
                assert_eq!(fiber.state(), FiberState::Disposed);
                assert!(root.registry().is_empty().unwrap());
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn parent_cleanup_cascades_to_nested_plugins_and_preserves_root_listeners() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let event = Event::<()>::symbol("nested");
    let calls = Rc::new(Cell::new(0));
    let leaf = Plugin::new("", {
        let event = event.clone();
        let calls = calls.clone();
        move |ctx: Context, ()| {
            let event = event.clone();
            let calls = calls.clone();
            async move {
                assert_eq!(
                    ctx.name(),
                    "child",
                    "unnamed plugins inherit the nearest named parent"
                );
                ctx.on(
                    &event,
                    Listener::sync(move |_, ()| {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }),
                )?;
                Ok::<_, BoxError>(())
            }
        }
    });
    let child = Plugin::new("child", {
        let event = event.clone();
        let calls = calls.clone();
        move |ctx: Context, ()| {
            let leaf = leaf.clone();
            let event = event.clone();
            let calls = calls.clone();
            async move {
                ctx.on(
                    &event,
                    Listener::sync(move |_, ()| {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }),
                )?;
                let leaf = ctx.plugin(&leaf, ())?;
                leaf.ready()?.await?;
                Ok::<_, BoxError>(())
            }
        }
    });
    let children = Rc::new(RefCell::new(Vec::new()));
    let parent = Plugin::new("parent", {
        let child = child.clone();
        let children = children.clone();
        let event = event.clone();
        let calls = calls.clone();
        move |ctx: Context, ()| {
            let child = child.clone();
            let children = children.clone();
            let event = event.clone();
            let calls = calls.clone();
            async move {
                ctx.on(
                    &event,
                    Listener::sync(move |_, ()| {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }),
                )?;
                let fiber = ctx.plugin(&child, ())?;
                fiber.ready()?.await?;
                children.borrow_mut().push(fiber);
                Ok::<_, BoxError>(())
            }
        }
    });
    runtime
        .run(async {
            let _root = root
                .on(
                    &event,
                    Listener::sync({
                        let calls = calls.clone();
                        move |_, ()| {
                            calls.set(calls.get() + 1);
                            Ok::<_, Infallible>(ControlFlow::Continue(()))
                        }
                    }),
                )
                .unwrap();
            let fiber = root.plugin(&parent, ()).unwrap();
            fiber.ready().unwrap().await.unwrap();
            assert_eq!(root.registry().len().unwrap(), 3);
            assert_eq!(fiber.context().name(), "parent");
            assert_eq!(children.borrow()[0].name(), "child");
            root.emit(&event, (), DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 4);
            fiber.dispose().unwrap().await.unwrap();
            assert_eq!(children.borrow()[0].state(), FiberState::Disposed);
            assert!(root.registry().is_empty().unwrap());
            assert_eq!(root.get_effects().unwrap().len(), 1);
            root.emit(&event, (), DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 5);
            fiber.dispose().unwrap().await.unwrap();
            root.drain().unwrap().await.unwrap();
            let fresh = root.plugin(&empty("after root drain"), ()).unwrap();
            fresh.ready().unwrap().await.unwrap();
            root.drain().unwrap().await.unwrap();
            assert_eq!(fresh.state(), FiberState::Disposed);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn disposing_child_rejects_new_work_but_final_cleanup_can_dispatch() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let event = Event::<()>::symbol("final");
    let received = Rc::new(Cell::new(false));
    let plugin = Plugin::new("owner", {
        let event = event.clone();
        move |ctx: Context, ()| {
            let event = event.clone();
            async move {
                let captured = ctx.clone();
                ctx.on_dispose_sync(move || {
                    assert_eq!(
                        captured.on_dispose_sync(|| Ok::<_, Infallible>(())).err(),
                        Some(AdmissionError::InactiveContext)
                    );
                    assert!(matches!(
                        captured.plugin(&empty("late child"), ()),
                        Err(LifecycleError::Admission(AdmissionError::InactiveContext))
                    ));
                    assert!(
                        captured
                            .on(
                                &event,
                                Listener::sync(|_, ()| Ok::<_, Infallible>(ControlFlow::Continue(
                                    ()
                                )))
                            )
                            .is_err()
                    );
                    drop(
                        captured
                            .emit(&event, (), DispatchOptions::default())
                            .unwrap(),
                    );
                    Ok::<_, Infallible>(())
                })?;
                Ok::<_, AdmissionError>(())
            }
        }
    });
    runtime
        .run(async {
            let _listener = root
                .on(
                    &event,
                    Listener::sync({
                        let received = received.clone();
                        move |_, ()| {
                            received.set(true);
                            Ok::<_, Infallible>(ControlFlow::Continue(()))
                        }
                    }),
                )
                .unwrap();
            let fiber = root.plugin(&plugin, ()).unwrap();
            fiber.ready().unwrap().await.unwrap();
            let stale = fiber.context();
            fiber.dispose().unwrap().await.unwrap();
            assert!(received.get());
            assert_eq!(
                stale.on_dispose_sync(|| Ok::<_, Infallible>(())).err(),
                Some(AdmissionError::InactiveContext)
            );
            assert!(runtime.take_failures().is_empty());
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn retired_registry_group_cannot_remove_a_new_group_of_the_same_plugin() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((entered_tx, release_rx)));
        let plugin = Plugin::new("replace", move |ctx: Context, value: u32| {
            let gates = if value == 1 {
                gates.borrow_mut().take()
            } else {
                None
            };
            async move {
                if let Some((entered, release)) = gates {
                    ctx.on_dispose(move || async move {
                        entered.send(()).unwrap();
                        release.await.unwrap();
                        Ok::<_, Infallible>(())
                    })?;
                }
                Ok::<_, AdmissionError>(())
            }
        });
        runtime
            .run(async {
                let old = root.plugin(&plugin, 1).unwrap();
                old.ready().unwrap().await.unwrap();
                let mut retiring = root.registry().remove(&plugin).unwrap();
                assert!(!root.registry().contains(&plugin).unwrap());
                let replacement = root.plugin(&plugin, 2).unwrap();
                entered_rx.await.unwrap();
                replacement.ready().unwrap().await.unwrap();
                assert_eq!(old.state(), FiberState::Unloading);
                assert_eq!(
                    root.registry().instances(&plugin).unwrap(),
                    [replacement.id().clone()]
                );
                release_tx.send(()).unwrap();
                retiring.pop().unwrap().await.unwrap();
                assert_eq!(old.state(), FiberState::Disposed);
                assert!(root.registry().contains(&plugin).unwrap());
                assert_eq!(
                    root.registry().instances(&plugin).unwrap(),
                    [replacement.id().clone()]
                );
                assert_eq!(root.get_effects().unwrap().len(), 1);
                root.drain().unwrap().await.unwrap();
                assert!(root.registry().is_empty().unwrap());
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn restart_preserves_identity_and_waits_for_cleanup_while_requests_coalesce() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let applied = Rc::new(Cell::new(0));
        let cleaned = Rc::new(RefCell::new(Vec::new()));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((entered_tx, release_rx)));
        let plugin = Plugin::new("restart", {
            let applied = applied.clone();
            let cleaned = cleaned.clone();
            move |ctx: Context, value: u32| {
                assert_eq!(value, 7);
                applied.set(applied.get() + 1);
                let attempt = applied.get();
                let gate = gates.borrow_mut().take();
                let cleaned = cleaned.clone();
                async move {
                    ctx.on_dispose(move || async move {
                        if let Some((entered, release)) = gate {
                            entered.send(()).unwrap();
                            release.await.unwrap();
                        }
                        cleaned.borrow_mut().push(attempt);
                        Ok::<_, Infallible>(())
                    })?;
                    Ok::<_, AdmissionError>(())
                }
            }
        });
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, 7).unwrap();
                fiber.ready().unwrap().await.unwrap();
                let id = fiber.id().clone();
                let first = fiber.restart().unwrap();
                entered_rx.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Unloading);
                let second = fiber.restart().unwrap();
                let third = fiber.restart().unwrap();
                assert_eq!(applied.get(), 1);
                release_tx.send(()).unwrap();
                assert_eq!(first.await.unwrap(), FiberState::Active);
                second.await.unwrap();
                third.await.unwrap();
                assert_eq!(applied.get(), 2);
                assert_eq!(*cleaned.borrow(), [1]);
                assert_eq!(fiber.id(), &id);
                assert_eq!(*fiber.config(), 7);
                assert_eq!(fiber.context().get_effects().unwrap().len(), 1);
                fiber.dispose().unwrap().await.unwrap();
                assert_eq!(*cleaned.borrow(), [1, 2]);
                assert!(root.get_effects().unwrap().is_empty());
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_disposal_during_apply_wins_over_a_queued_restart_and_dropped_waiters() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let applied = Rc::new(Cell::new(0));
        let cleaned = Rc::new(Cell::new(0));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((started_tx, release_rx)));
        let plugin = Plugin::new("loading", {
            let applied = applied.clone();
            let cleaned = cleaned.clone();
            move |ctx: Context, ()| {
                applied.set(applied.get() + 1);
                let (started, release) =
                    gates.borrow_mut().take().expect("obsolete restart applied");
                let cleaned = cleaned.clone();
                async move {
                    ctx.on_dispose_sync(move || {
                        cleaned.set(cleaned.get() + 1);
                        Ok::<_, Infallible>(())
                    })?;
                    started.send(()).unwrap();
                    release.await.unwrap();
                    assert_eq!(
                        ctx.on_dispose_sync(|| Ok::<_, Infallible>(())).err(),
                        Some(AdmissionError::InactiveContext)
                    );
                    Ok::<_, AdmissionError>(())
                }
            }
        });
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, ()).unwrap();
                started_rx.await.unwrap();
                let restarted = fiber.restart().unwrap();
                let disposed = fiber.dispose().unwrap();
                drop(disposed);
                assert!(!root.registry().contains(&plugin).unwrap());
                release_tx.send(()).unwrap();
                assert_eq!(restarted.await.unwrap(), FiberState::Disposed);
                assert_eq!(fiber.ready().unwrap().await.unwrap(), FiberState::Disposed);
                assert_eq!(applied.get(), 1);
                assert_eq!(cleaned.get(), 1);
                assert_eq!(
                    fiber.dispose().unwrap().await.unwrap(),
                    FiberState::Disposed
                );
                assert!(root.get_effects().unwrap().is_empty());
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn superseded_load_failure_rolls_back_without_failing_a_later_restart() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let attempt = Rc::new(Cell::new(0));
        let cleaned = Rc::new(RefCell::new(Vec::new()));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((started_tx, release_rx)));
        let plugin = Plugin::new("superseded", {
            let attempt = attempt.clone();
            let cleaned = cleaned.clone();
            move |ctx: Context, ()| {
                attempt.set(attempt.get() + 1);
                let current = attempt.get();
                let gate = gates.borrow_mut().take();
                let cleaned = cleaned.clone();
                async move {
                    ctx.on_dispose_sync(move || {
                        cleaned.borrow_mut().push(current);
                        Ok::<_, Infallible>(())
                    })?;
                    if let Some((started, release)) = gate {
                        started.send(()).unwrap();
                        release.await.unwrap();
                        return Err::<(), BoxError>(std::io::Error::other("obsolete apply").into());
                    }
                    Ok(())
                }
            }
        });
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, ()).unwrap();
                started_rx.await.unwrap();
                let first_waiter = fiber.ready().unwrap();
                let restart = fiber.restart().unwrap();
                release_tx.send(()).unwrap();
                assert_eq!(first_waiter.await.unwrap(), FiberState::Active);
                restart.await.unwrap();
                assert_eq!(attempt.get(), 2);
                assert_eq!(*cleaned.borrow(), [1]);
                assert_eq!(runtime.take_failures().len(), 1);
                root.drain().unwrap().await.unwrap();
                assert_eq!(*cleaned.borrow(), [1, 2]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn immediate_disposal_skips_queued_validation_and_apply() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let plugin = Plugin::validated(
        "never load",
        |_: &()| -> Result<(), Infallible> { panic!("queued validation ran") },
        |_, ()| async {
            panic!("queued apply ran");
            #[allow(unreachable_code)]
            Ok::<_, Infallible>(())
        },
    );
    runtime
        .run(async {
            let fiber = root.plugin(&plugin, ()).unwrap();
            assert_eq!(
                fiber.dispose().unwrap().await.unwrap(),
                FiberState::Disposed
            );
            assert_eq!(fiber.ready().unwrap().await.unwrap(), FiberState::Disposed);
            assert!(runtime.take_failures().is_empty());
            assert!(root.registry().is_empty().unwrap());
            assert!(root.get_effects().unwrap().is_empty());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_self_and_ancestor_waits_are_refused_without_detaching_registry_groups() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let own_handle = Rc::new(RefCell::new(None::<FiberHandle<()>>));
        let own_plugin = Rc::new(RefCell::new(None::<Plugin<()>>));
        let child = Plugin::new("child", {
            let own_handle = own_handle.clone();
            move |ctx: Context, ()| {
                let parent = own_handle.borrow().as_ref().unwrap().clone();
                async move {
                    assert!(matches!(
                        parent.ready(),
                        Err(LifecycleError::Admission(AdmissionError::ReentrantDrain))
                    ));
                    assert_eq!(
                        ctx.root().drain().err(),
                        Some(AdmissionError::ReentrantDrain)
                    );
                    Ok::<_, Infallible>(())
                }
            }
        });
        let parent = Plugin::new("parent", {
            let own_handle = own_handle.clone();
            let own_plugin = own_plugin.clone();
            move |ctx: Context, ()| {
                let own = own_handle.borrow().as_ref().unwrap().clone();
                let plugin = own_plugin.borrow().as_ref().unwrap().clone();
                let child = child.clone();
                async move {
                    assert!(matches!(
                        own.ready(),
                        Err(LifecycleError::Admission(AdmissionError::ReentrantDrain))
                    ));
                    assert!(matches!(
                        own.restart(),
                        Err(LifecycleError::Admission(AdmissionError::ReentrantDrain))
                    ));
                    assert!(matches!(
                        own.dispose(),
                        Err(LifecycleError::Admission(AdmissionError::ReentrantDrain))
                    ));
                    assert!(matches!(
                        ctx.registry().remove(&plugin),
                        Err(LifecycleError::Admission(AdmissionError::ReentrantDrain))
                    ));
                    assert_eq!(ctx.drain().err(), Some(AdmissionError::ReentrantDrain));
                    assert_eq!(
                        ctx.root().drain().err(),
                        Some(AdmissionError::ReentrantDrain)
                    );
                    let child = ctx.plugin(&child, ())?;
                    child.ready()?.await?;
                    Ok::<_, BoxError>(())
                }
            }
        });
        *own_plugin.borrow_mut() = Some(parent.clone());
        runtime
            .run(async {
                let fiber = root.plugin(&parent, ()).unwrap();
                *own_handle.borrow_mut() = Some(fiber.clone());
                fiber.ready().unwrap().await.unwrap();
                assert!(root.registry().contains(&parent).unwrap());
                root.drain().unwrap().await.unwrap();
                assert!(root.registry().is_empty().unwrap());
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
        own_handle.borrow_mut().take();
        own_plugin.borrow_mut().take();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn root_drain_waits_for_children_and_remains_usable_after_an_abandoned_waiter() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let root_id = root.fiber_id().clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((entered_tx, release_rx)));
        let plugin = Plugin::new("gated child", move |ctx: Context, ()| {
            let (entered, release) = gates.borrow_mut().take().unwrap();
            async move {
                ctx.on_dispose(move || async move {
                    entered.send(()).unwrap();
                    release.await.unwrap();
                    Ok::<_, Infallible>(())
                })?;
                Ok::<_, AdmissionError>(())
            }
        });
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, ()).unwrap();
                fiber.ready().unwrap().await.unwrap();
                drop(root.drain().unwrap());
                entered_rx.await.unwrap();
                assert!(!root.registry().contains(&plugin).unwrap());
                assert_eq!(fiber.state(), FiberState::Unloading);
                let barrier = root.drain().unwrap();
                release_tx.send(()).unwrap();
                barrier.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Disposed);
                assert_eq!(root.fiber_id(), &root_id);
                let fresh = root.plugin(&empty("reuse"), ()).unwrap();
                fresh.ready().unwrap().await.unwrap();
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn child_effect_streams_and_exporters_follow_child_ownership_not_the_root() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let cleaned = Rc::new(Cell::new(false));
        let exports = Rc::new(Cell::new(0));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = RefCell::new(Some((entered_tx, release_rx)));
        let plugin = Plugin::new("scoped", {
            let cleaned = cleaned.clone();
            let exports = exports.clone();
            move |ctx: Context, ()| {
                let (entered, release) = gates.borrow_mut().take().unwrap();
                let cleaned = cleaned.clone();
                let exports = exports.clone();
                async move {
                    ctx.add_exporter(cordis_core::ExporterOptions::default(), move |_| {
                        exports.set(exports.get() + 1);
                        Ok::<_, Infallible>(())
                    })?;
                    let _setup = ctx.effect_async(move || async move {
                        entered.send(()).unwrap();
                        release.await.unwrap();
                        Ok::<_, Infallible>(cordis_core::Disposer::sync(move || {
                            cleaned.set(true);
                            Ok::<_, Infallible>(())
                        }))
                    })?;
                    Ok::<_, BoxError>(())
                }
            }
        });
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, ()).unwrap();
                fiber.ready().unwrap().await.unwrap();
                entered_rx.await.unwrap();
                assert_eq!(root.get_effects().unwrap().len(), 1);
                assert_eq!(fiber.context().get_effects().unwrap().len(), 2);
                let record = fiber.context().logger(None).info("child").unwrap();
                assert_eq!(&record.fiber, fiber.id());
                assert_eq!(record.name, "scoped");
                assert_eq!(exports.get(), 1);
                let done = fiber.dispose().unwrap();
                assert!(!cleaned.get());
                release_tx.send(()).unwrap();
                done.await.unwrap();
                assert!(cleaned.get());
                root.logger(None).info("after child").unwrap();
                assert_eq!(exports.get(), 1);
                assert!(root.get_effects().unwrap().is_empty());
                assert!(fiber.context().get_effects().unwrap().is_empty());
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_failures_are_attempted_and_reported_without_blocking_disposal() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let cleaned = Rc::new(Cell::new(0));
    let plugin = Plugin::new("cleanup errors", {
        let cleaned = cleaned.clone();
        move |ctx: Context, ()| {
            let cleaned = cleaned.clone();
            async move {
                ctx.on_dispose_sync(move || {
                    cleaned.set(cleaned.get() + 1);
                    Ok::<_, Infallible>(())
                })?;
                ctx.on_dispose_sync(|| Err::<(), _>(std::io::Error::other("cleanup error")))?;
                ctx.on_dispose_sync(|| -> Result<(), Infallible> { panic!("cleanup panic") })?;
                Ok::<_, AdmissionError>(())
            }
        }
    });
    runtime
        .run(async {
            let fiber = root.plugin(&plugin, ()).unwrap();
            fiber.ready().unwrap().await.unwrap();
            fiber.dispose().unwrap().await.unwrap();
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(cleaned.get(), 1);
            let failures = runtime.take_failures();
            assert_eq!(failures.len(), 2);
            assert!(failures[0].is_panic());
            assert!(!failures[1].is_panic());
            fiber.dispose().unwrap().await.unwrap();
            assert!(runtime.take_failures().is_empty());
            assert!(root.registry().is_empty().unwrap());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn validation_and_apply_factory_panics_fail_only_their_instances() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let validate = Plugin::validated(
        "validator panic",
        |_: &()| -> Result<(), Infallible> { panic!("validation panic") },
        |_, ()| async { Ok::<_, Infallible>(()) },
    );
    let factory = Plugin::new(
        "factory panic",
        |_: Context, ()| -> std::future::Ready<Result<(), Infallible>> {
            panic!("apply factory panic")
        },
    );
    runtime
        .run(async {
            let first = root.plugin(&validate, ()).unwrap();
            let second = root.plugin(&factory, ()).unwrap();
            let healthy = root.plugin(&empty("healthy"), ()).unwrap();
            for failed in [&first, &second] {
                let LifecycleError::Callback(failure) = failed.ready().unwrap().await.unwrap_err()
                else {
                    panic!("callback panic became a framework failure")
                };
                assert!(failure.is_panic());
                assert_eq!(failed.state(), FiberState::Failed);
            }
            healthy.ready().unwrap().await.unwrap();
            assert_eq!(runtime.take_failures().len(), 2);
            root.drain().unwrap().await.unwrap();
            assert!(root.registry().is_empty().unwrap());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn idle_disposed_handles_do_not_retain_cleanup_captures_or_parent_journal_entries() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let plugin = Plugin::new("", |ctx: Context, ()| async move {
        assert_eq!(ctx.fiber_id(), ctx.extend(true).fiber_id());
        assert_eq!(ctx.root().fiber_id(), ctx.root().extend(1_u32).fiber_id());
        Ok::<_, Infallible>(())
    });
    runtime
        .run(async {
            let mut handles = Vec::new();
            for _ in 0..12 {
                let fiber = root.plugin(&plugin, ()).unwrap();
                fiber.ready().unwrap().await.unwrap();
                assert_eq!(fiber.name(), "root");
                let probe = Rc::new(());
                let weak = Rc::downgrade(&probe);
                let captured = fiber.context();
                let _resource = captured
                    .on_dispose_sync(move || {
                        drop(probe);
                        Ok::<_, Infallible>(())
                    })
                    .unwrap();
                fiber.dispose().unwrap().await.unwrap();
                assert!(weak.upgrade().is_none());
                assert!(root.registry().is_empty().unwrap());
                assert!(root.get_effects().unwrap().is_empty());
                handles.push(fiber);
            }
            assert!(
                handles
                    .iter()
                    .all(|handle| handle.state() == FiberState::Disposed)
            );
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn host_destruction_cancels_loading_and_contexts_do_not_keep_runtime_alive() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let root_id = root.fiber_id().clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let gate = RefCell::new(Some((entered_tx, release_rx)));
        let plugin = Plugin::new("cancelled", move |ctx: Context, ()| {
            let (entered, release) = gate.borrow_mut().take().unwrap();
            async move {
                ctx.on_dispose_sync(|| Ok::<_, Infallible>(()))?;
                entered.send(()).unwrap();
                release.await.unwrap();
                Ok::<_, AdmissionError>(())
            }
        });
        let mut handle = None;
        let mut ready = None;
        runtime
            .run(async {
                let fiber = root.plugin(&plugin, ()).unwrap();
                ready = Some(fiber.ready().unwrap());
                entered_rx.await.unwrap();
                handle = Some(fiber);
            })
            .await
            .unwrap();
        let fiber = handle.take().unwrap();
        let stale = fiber.context().extend(7_u32);
        drop(runtime);
        assert!(matches!(
            ready.take().unwrap().await,
            Err(LifecycleError::HostStopped)
        ));
        assert!(release_tx.send(()).is_err());
        assert_eq!(stale.root().fiber_id(), &root_id);
        assert_eq!(stale.name(), "cancelled");
        assert_eq!(stale.root().name(), "root");
        assert_eq!(*stale.metadata::<u32>().unwrap(), 7);
        assert_eq!(
            stale.on_dispose_sync(|| Ok::<_, Infallible>(())).err(),
            Some(AdmissionError::RuntimeDropped)
        );
        assert!(matches!(
            fiber.dispose(),
            Err(LifecycleError::Admission(AdmissionError::RuntimeDropped))
        ));
        assert_eq!(
            stale.registry().len().err(),
            Some(AdmissionError::RuntimeDropped)
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn registration_tokens_remain_inert_after_the_disposed_owner_is_released() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let listener = Rc::new(RefCell::new(None));
    let exporter = Rc::new(RefCell::new(None));
    let plugin = Plugin::new("tokens", {
        let listener = listener.clone();
        let exporter = exporter.clone();
        move |ctx: Context, ()| {
            let listener = listener.clone();
            let exporter = exporter.clone();
            async move {
                *listener.borrow_mut() = Some(ctx.on(
                    &Event::<()>::named("temporary"),
                    Listener::sync(|_, ()| Ok::<_, Infallible>(ControlFlow::Continue(()))),
                )?);
                *exporter.borrow_mut() = Some(
                    ctx.add_exporter(cordis_core::ExporterOptions::default(), |_| {
                        Ok::<_, Infallible>(())
                    })?,
                );
                Ok::<_, BoxError>(())
            }
        }
    });
    runtime
        .run(async {
            let fiber = root.plugin(&plugin, ()).unwrap();
            fiber.ready().unwrap().await.unwrap();
            let stale = fiber.context();
            fiber.dispose().unwrap().await.unwrap();
            drop(fiber);
            assert_eq!(
                stale.get_effects().err(),
                Some(AdmissionError::InactiveContext)
            );
            assert!(!listener.borrow_mut().take().unwrap().remove().unwrap());
            assert!(!exporter.borrow_mut().take().unwrap().remove().unwrap());
            assert!(root.registry().is_empty().unwrap());
            assert!(root.get_effects().unwrap().is_empty());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn typed_object_method_adapter_keeps_config_and_cleanup_with_its_instance() {
    struct Behavior {
        applied: Rc<RefCell<Vec<String>>>,
        cleaned: Rc<RefCell<Vec<String>>>,
    }
    impl Behavior {
        fn apply(&self, ctx: Context, config: String) -> Result<(), AdmissionError> {
            self.applied.borrow_mut().push(config.clone());
            let cleaned = self.cleaned.clone();
            ctx.on_dispose_sync(move || {
                cleaned.borrow_mut().push(config);
                Ok::<_, Infallible>(())
            })?;
            Ok(())
        }
    }
    let runtime = Runtime::new();
    let root = runtime.context();
    let applied = Rc::new(RefCell::new(Vec::new()));
    let cleaned = Rc::new(RefCell::new(Vec::new()));
    let object = Behavior {
        applied: applied.clone(),
        cleaned: cleaned.clone(),
    };
    let plugin = Plugin::new("method adapter", move |ctx, config: String| {
        std::future::ready(object.apply(ctx, config))
    });
    runtime
        .run(async {
            let first = root.plugin(&plugin, String::from("first")).unwrap();
            let second = root.plugin(&plugin, String::from("second")).unwrap();
            first.ready().unwrap().await.unwrap();
            second.ready().unwrap().await.unwrap();
            let mut seen = applied.borrow().clone();
            seen.sort();
            assert_eq!(seen, ["first", "second"]);
            first.dispose().unwrap().await.unwrap();
            assert_eq!(*cleaned.borrow(), ["first"]);
            assert_eq!(second.state(), FiberState::Active);
            assert_eq!(&*second.config(), "second");
            second.dispose().unwrap().await.unwrap();
            assert_eq!(*cleaned.borrow(), ["first", "second"]);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_registry_removal_and_recreation_restore_listener_and_effect_baselines() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let event = Event::<()>::named("reload baseline");
    let calls = Rc::new(Cell::new(0));
    let plugin = Plugin::new("reload", {
        let event = event.clone();
        let calls = calls.clone();
        move |ctx: Context, ()| {
            let event = event.clone();
            let calls = calls.clone();
            async move {
                ctx.on(
                    &event,
                    Listener::sync(move |_, ()| {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }),
                )?;
                Ok::<_, BoxError>(())
            }
        }
    });
    runtime
        .run(async {
            let _root = root
                .on(
                    &event,
                    Listener::sync(|_, ()| Ok::<_, Infallible>(ControlFlow::Continue(()))),
                )
                .unwrap();
            let before = root.get_effects().unwrap();
            let mut loaded_snapshot = None;
            for cycle in 0..4 {
                let fiber = root.plugin(&plugin, ()).unwrap();
                fiber.ready().unwrap().await.unwrap();
                let loaded = root.get_effects().unwrap();
                if let Some(expected) = &loaded_snapshot {
                    assert_eq!(&loaded, expected);
                } else {
                    loaded_snapshot = Some(loaded);
                }
                root.emit(&event, (), DispatchOptions::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(calls.get(), cycle + 1);
                for task in root.registry().remove(&plugin).unwrap() {
                    task.await.unwrap();
                }
                assert_eq!(fiber.state(), FiberState::Disposed);
                assert_eq!(root.get_effects().unwrap(), before);
                assert!(root.registry().is_empty().unwrap());
                root.emit(&event, (), DispatchOptions::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(calls.get(), cycle + 1);
            }
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}
