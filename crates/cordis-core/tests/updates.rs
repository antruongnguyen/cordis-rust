//! Gated convergence/validation evidence against update.spec.ts and fiber.spec.ts.
use cordis_core::{
    BoxError, FiberState, LifecycleError, Listener, ListenerOptions, Plugin, Runtime, ServiceKey,
    UpdateOptions, hooks,
};
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
        .expect("update stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn all_concurrent_waiters_follow_latest_config_with_and_without_dependencies() {
    for inject in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            runtime
                .run(async {
                    let root = runtime.context();
                    let key = ServiceKey::<()>::new("foo");
                    root.provide(&key, ()).unwrap();
                    let (started, entered) = oneshot::channel();
                    let (release, gate) = oneshot::channel();
                    let (latest_started, latest_entered) = oneshot::channel();
                    let (latest_release, latest_gate) = oneshot::channel();
                    let gates = RefCell::new(Some((started, gate)));
                    let latest = RefCell::new(Some((latest_started, latest_gate)));
                    let applied = Rc::new(RefCell::new(Vec::new()));
                    let disposed = Rc::new(RefCell::new(Vec::new()));
                    let plugin = Plugin::new("worker", {
                        let applied = applied.clone();
                        let disposed = disposed.clone();
                        move |ctx, config: u32| {
                            let pause = match config {
                                1 => gates.borrow_mut().take(),
                                3 => latest.borrow_mut().take(),
                                _ => None,
                            };
                            let applied = applied.clone();
                            let disposed = disposed.clone();
                            async move {
                                applied.borrow_mut().push(config);
                                ctx.on_dispose_sync(move || {
                                    disposed.borrow_mut().push(config);
                                    Ok::<_, Infallible>(())
                                })?;
                                if let Some((started, gate)) = pause {
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                }
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .injecting(if inject {
                        vec![key.required()]
                    } else {
                        vec![]
                    });
                    let fiber = root.plugin(&plugin, 0).unwrap();
                    fiber.ready().unwrap().await.unwrap();
                    let first = fiber.update(1).unwrap();
                    entered.await.unwrap();
                    let second = fiber.update(2).unwrap();
                    let third = fiber.update(3).unwrap();
                    release.send(()).unwrap();
                    latest_entered.await.unwrap();
                    assert_eq!(*applied.borrow(), [0, 1, 3]);
                    assert_eq!(*disposed.borrow(), [0, 1]);
                    assert_eq!(fiber.state(), FiberState::Loading);
                    assert_eq!(*fiber.config(), 3);
                    latest_release.send(()).unwrap();
                    for result in [first.await, second.await, third.await] {
                        assert_eq!(result.unwrap(), FiberState::Active);
                    }
                    root.drain().unwrap().await.unwrap();
                    assert_eq!(*disposed.borrow(), [0, 1, 3]);
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn updates_during_unloading_never_interrupt_cleanup_or_load_the_middle_source() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let applied = Rc::new(RefCell::new(Vec::new()));
                let plugin = Plugin::new("worker", {
                    let applied = applied.clone();
                    move |ctx, config: u32| {
                        let pause = if config == 0 {
                            gates.borrow_mut().take()
                        } else {
                            None
                        };
                        let applied = applied.clone();
                        async move {
                            applied.borrow_mut().push(config);
                            ctx.on_dispose(move || async move {
                                if let Some((started, gate)) = pause {
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                }
                                Ok::<_, Infallible>(())
                            })?;
                            Ok::<_, BoxError>(())
                        }
                    }
                });
                let fiber = root.plugin(&plugin, 0).unwrap();
                fiber.ready().unwrap().await.unwrap();
                let first = fiber.update(1).unwrap();
                entered.await.unwrap();
                let second = fiber.update(2).unwrap();
                assert_eq!(fiber.state(), FiberState::Unloading);
                assert_eq!(*applied.borrow(), [0]);
                release.send(()).unwrap();
                first.await.unwrap();
                second.await.unwrap();
                assert_eq!(*applied.borrow(), [0, 2]);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn queued_initial_sources_are_skipped_and_pending_loads_validate_only_the_latest_source() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<()>::new("foo");
            let applied = Rc::new(RefCell::new(Vec::new()));
            let validations = Rc::new(Cell::new(0));
            let plugin = Plugin::validated(
                "worker",
                {
                    let validations = validations.clone();
                    move |source: &i32| {
                        validations.set(validations.get() + 1);
                        u32::try_from(*source)
                    }
                },
                {
                    let applied = applied.clone();
                    move |_, value| {
                        applied.borrow_mut().push(value);
                        async { Ok::<_, Infallible>(()) }
                    }
                },
            );
            let first = root.plugin(&plugin, 1).unwrap();
            let a = first.update(2).unwrap();
            let b = first.update(3).unwrap();
            a.await.unwrap();
            b.await.unwrap();
            assert_eq!(*applied.borrow(), [3]);
            let pending = root
                .plugin(&plugin.injecting([key.required()]), -1)
                .unwrap();
            pending.update(-2).unwrap().await.unwrap();
            pending.update(4).unwrap().await.unwrap();
            assert_eq!(pending.state(), FiberState::Pending);
            assert_eq!(
                validations.get(),
                3,
                "loaded updates validate before commit and again on load"
            );
            root.provide(&key, ()).unwrap();
            pending.ready().unwrap().await.unwrap();
            assert_eq!(*applied.borrow(), [3, 4]);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_loaded_update_cannot_commit_or_reach_resolved_middleware_and_no_save_only_skips_commit()
 {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let commits = Rc::new(Cell::new(0));
            root.on(
                &hooks::commit(),
                Listener::sync({
                    let commits = commits.clone();
                    move |_, _| {
                        commits.set(commits.get() + 1);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }
                }),
            )
            .unwrap();
            let applied = Rc::new(RefCell::new(Vec::new()));
            let received = Rc::new(RefCell::new(Vec::new()));
            let plugin = Plugin::validated(
                "worker",
                |source: &i32| u32::try_from(*source).map(|v| format!("resolved {v}")),
                {
                    let applied = applied.clone();
                    let received = received.clone();
                    move |ctx, value: String| {
                        let applied = applied.clone();
                        let received = received.clone();
                        async move {
                            applied.borrow_mut().push(value);
                            ctx.on_update_sync(move |value: Rc<String>| {
                                received.borrow_mut().push((*value).clone());
                                Ok::<_, Infallible>(false)
                            })?;
                            Ok::<_, BoxError>(())
                        }
                    }
                },
            );
            let fiber = root.plugin(&plugin, 1).unwrap();
            fiber.ready().unwrap().await.unwrap();
            assert!(matches!(fiber.update(-1), Err(LifecycleError::Callback(_))));
            assert_eq!(*fiber.config(), 1);
            assert_eq!(commits.get(), 0);
            assert!(received.borrow().is_empty());
            fiber.update(2).unwrap().await.unwrap();
            assert_eq!(*received.borrow(), ["resolved 2"]);
            assert_eq!(*applied.borrow(), ["resolved 1"]);
            assert_eq!(*fiber.config(), 2);
            assert_eq!(commits.get(), 1);
            fiber
                .update_with(3, UpdateOptions { no_save: true })
                .unwrap()
                .await
                .unwrap();
            assert_eq!(commits.get(), 1);
            assert_eq!(*fiber.config(), 3);
            fiber.restart().unwrap().await.unwrap();
            assert_eq!(*applied.borrow(), ["resolved 1", "resolved 3"]);
            assert!(runtime.take_failures().is_empty());
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_loads_recover_only_on_update_and_latest_failure_reaches_every_waiter_once() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let plugin = Plugin::new("worker", move |_, source: i32| {
                    let pause = if source == 1 {
                        gates.borrow_mut().take()
                    } else {
                        None
                    };
                    async move {
                        if let Some((started, gate)) = pause {
                            started.send(()).unwrap();
                            gate.await.unwrap();
                        }
                        if source < 0 {
                            Err(std::io::Error::other("bad source"))
                        } else {
                            Ok(())
                        }
                    }
                });
                let fiber = root.plugin(&plugin, -1).unwrap();
                assert!(fiber.ready().unwrap().await.is_err());
                assert_eq!(runtime.take_failures().len(), 1);
                fiber.update(0).unwrap().await.unwrap();
                assert_eq!(fiber.state(), FiberState::Active);
                let first = fiber.update(1).unwrap();
                entered.await.unwrap();
                let latest = fiber.update(-2).unwrap();
                release.send(()).unwrap();
                assert!(matches!(first.await, Err(LifecycleError::Callback(_))));
                assert!(matches!(latest.await, Err(LifecycleError::Callback(_))));
                assert_eq!(
                    runtime.take_failures().len(),
                    1,
                    "load failure must not be re-reported through update middleware"
                );
                fiber.update(3).unwrap().await.unwrap();
                assert_eq!(fiber.state(), FiberState::Active);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn disposal_beats_queued_updates_and_dropped_waiters_do_not_cancel_cleanup() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let applied = Rc::new(RefCell::new(Vec::new()));
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let disposed = Rc::new(Cell::new(0));
                let fiber = root
                    .plugin(
                        &Plugin::new("worker", {
                            let applied = applied.clone();
                            let disposed = disposed.clone();
                            move |ctx, value: u32| {
                                let (started, gate) = gates.borrow_mut().take().unwrap();
                                let applied = applied.clone();
                                let disposed = disposed.clone();
                                async move {
                                    applied.borrow_mut().push(value);
                                    ctx.on_dispose_sync(move || {
                                        disposed.set(disposed.get() + 1);
                                        Ok::<_, Infallible>(())
                                    })?;
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                    Ok::<_, BoxError>(())
                                }
                            }
                        }),
                        1,
                    )
                    .unwrap();
                entered.await.unwrap();
                let updating = fiber.update(2).unwrap();
                drop(fiber.dispose().unwrap());
                release.send(()).unwrap();
                assert_eq!(updating.await.unwrap(), FiberState::Disposed);
                assert_eq!(*applied.borrow(), [1]);
                assert_eq!(disposed.get(), 1);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_update_during_loading_keeps_the_current_apply_and_new_source() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let applied = Rc::new(Cell::new(0));
                let received = Rc::new(Cell::new(0));
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let fiber = root
                    .plugin(
                        &Plugin::new("worker", {
                            let applied = applied.clone();
                            let received = received.clone();
                            move |ctx, _: u32| {
                                let (started, gate) = gates.borrow_mut().take().unwrap();
                                let applied = applied.clone();
                                let received = received.clone();
                                async move {
                                    applied.set(applied.get() + 1);
                                    ctx.on_update_sync(move |value: Rc<u32>| {
                                        received.set(*value);
                                        Ok::<_, Infallible>(false)
                                    })?;
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                    Ok::<_, BoxError>(())
                                }
                            }
                        }),
                        1,
                    )
                    .unwrap();
                entered.await.unwrap();
                fiber.update(2).unwrap().await.unwrap();
                assert_eq!(fiber.state(), FiberState::Loading);
                assert_eq!(*fiber.config(), 2);
                assert_eq!(received.get(), 2);
                release.send(()).unwrap();
                fiber.ready().unwrap().await.unwrap();
                assert_eq!(applied.get(), 1);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn local_update_hooks_do_not_accumulate_and_global_hooks_survive_child_reload() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let local = Rc::new(Cell::new(0));
            let global = Rc::new(Cell::new(0));
            root.on_with(
                &hooks::update(),
                Listener::around_async({
                    let global = global.clone();
                    move |_, _, next| {
                        global.set(global.get() + 1);
                        async move { next.call().await }
                    }
                }),
                ListenerOptions {
                    global: true,
                    ..Default::default()
                },
            )
            .unwrap();
            let plugin = Plugin::new("worker", {
                let local = local.clone();
                move |ctx, _: u32| {
                    let local = local.clone();
                    async move {
                        ctx.on_update_sync(move |_: Rc<u32>| {
                            local.set(local.get() + 1);
                            Ok::<_, Infallible>(true)
                        })?;
                        Ok::<_, BoxError>(())
                    }
                }
            });
            let first = root.plugin(&plugin, 0).unwrap();
            let other = root.plugin(&plugin, 0).unwrap();
            first.ready().unwrap().await.unwrap();
            other.ready().unwrap().await.unwrap();
            for value in 1..=5 {
                first.update(value).unwrap().await.unwrap();
            }
            assert_eq!(
                local.get(),
                5,
                "the other instance and retired local hooks must not receive updates"
            );
            assert_eq!(global.get(), 5);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn source_transformation_hooks_run_before_validation_and_schema_contract_mismatches_are_errors()
 {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let token = root
                .on(
                    &hooks::config(),
                    Listener::around(
                        |_, request: hooks::Config, next: cordis_core::Next<hooks::Value>| {
                            assert_eq!(*request.source.get::<i32>()?, 1);
                            let value = next.call()?.get::<i32>()?;
                            Ok::<_, BoxError>(hooks::Value::new(*value + 1))
                        },
                    ),
                )
                .unwrap();
            let seen = Rc::new(Cell::new(0));
            let plugin = Plugin::new("worker", {
                let seen = seen.clone();
                move |_, value: i32| {
                    seen.set(value);
                    async { Ok::<_, Infallible>(()) }
                }
            });
            root.plugin(&plugin, 1)
                .unwrap()
                .ready()
                .unwrap()
                .await
                .unwrap();
            assert_eq!(seen.get(), 2);
            token.remove().unwrap();
            root.on(
                &hooks::config(),
                Listener::around(|_, _, _| Ok::<_, Infallible>(hooks::Value::new("wrong type"))),
            )
            .unwrap();
            let failed = root.plugin(&plugin, 1).unwrap();
            let error = failed.ready().unwrap().await.unwrap_err();
            let LifecycleError::Callback(error) = error else {
                panic!("wrong error kind");
            };
            let source = std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<cordis_core::ServiceError>()
                .unwrap();
            assert!(matches!(
                source,
                cordis_core::ServiceError::TypeMismatch {
                    expected: "i32",
                    actual: "&str",
                    ..
                }
            ));
            assert_eq!(failed.state(), FiberState::Failed);
            assert_eq!(runtime.take_failures().len(), 1);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn config_aba_and_explicit_restart_force_a_new_load_even_with_equal_final_source() {
    for restart in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            runtime
                .run(async {
                    let root = runtime.context();
                    let applied = Rc::new(RefCell::new(Vec::new()));
                    let (started, entered) = oneshot::channel();
                    let (release, gate) = oneshot::channel();
                    let gates = RefCell::new(Some((started, gate)));
                    let fiber = root
                        .plugin(
                            &Plugin::new("worker", {
                                let applied = applied.clone();
                                move |_, value: u32| {
                                    let pause = gates.borrow_mut().take();
                                    let applied = applied.clone();
                                    async move {
                                        applied.borrow_mut().push(value);
                                        if let Some((started, gate)) = pause {
                                            started.send(()).unwrap();
                                            gate.await.unwrap();
                                        }
                                        Ok::<_, Infallible>(())
                                    }
                                }
                            }),
                            1,
                        )
                        .unwrap();
                    entered.await.unwrap();
                    let waiters = if restart {
                        vec![fiber.restart().unwrap()]
                    } else {
                        vec![fiber.update(2).unwrap(), fiber.update(1).unwrap()]
                    };
                    release.send(()).unwrap();
                    for task in waiters {
                        task.await.unwrap();
                    }
                    assert_eq!(*applied.borrow(), [1, 1]);
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_obsolete_apply_failure_does_not_poison_the_new_config() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let applied = Rc::new(RefCell::new(Vec::new()));
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let fiber = root
                    .plugin(
                        &Plugin::new("worker", {
                            let applied = applied.clone();
                            move |_, value: u32| {
                                let pause = gates.borrow_mut().take();
                                let applied = applied.clone();
                                async move {
                                    applied.borrow_mut().push(value);
                                    if let Some((started, gate)) = pause {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                        return Err(std::io::Error::other("obsolete load"));
                                    }
                                    Ok(())
                                }
                            }
                        }),
                        1,
                    )
                    .unwrap();
                entered.await.unwrap();
                let updating = fiber.update(2).unwrap();
                release.send(()).unwrap();
                updating.await.unwrap();
                assert_eq!(*applied.borrow(), [1, 2]);
                assert_eq!(fiber.state(), FiberState::Active);
                assert_eq!(runtime.take_failures().len(), 1);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_update_and_consumer_update_converge_without_an_old_provider_new_config_load() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<u32>::new("provider");
            let provider = root
                .plugin(
                    &Plugin::new("provider", {
                        let key = key.clone();
                        move |ctx, value| {
                            let key = key.clone();
                            async move {
                                ctx.provide(&key, value)?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    }),
                    1,
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            let applied = Rc::new(RefCell::new(Vec::new()));
            let consumer = root
                .plugin(
                    &Plugin::new("consumer", {
                        let key = key.clone();
                        let applied = applied.clone();
                        move |ctx, value: &'static str| {
                            let key = key.clone();
                            let applied = applied.clone();
                            async move {
                                applied.borrow_mut().push((*ctx.get(&key)?.unwrap(), value));
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .injecting([key.required()]),
                    "old",
                )
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            let a = provider.update(2).unwrap();
            let b = consumer.update("new").unwrap();
            a.await.unwrap();
            b.await.unwrap();
            consumer.ready().unwrap().await.unwrap();
            assert_eq!(*applied.borrow(), [(1, "old"), (2, "new")]);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn outdated_effect_stream_stops_after_its_in_flight_final_yield() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let trace = Rc::new(RefCell::new(Vec::new()));
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let fiber = root
                    .plugin(
                        &Plugin::new("worker", {
                            let trace = trace.clone();
                            move |ctx, value: u32| {
                                let pause = gates.borrow_mut().take();
                                let trace = trace.clone();
                                async move {
                                    trace.borrow_mut().push(value);
                                    if let Some((started, gate)) = pause {
                                        let effect = ctx.effect_stream(move || {
                                            futures_util::stream::unfold(
                                                (Some((started, gate)), trace),
                                                |(pause, trace)| async move {
                                                    let (started, gate) =
                                                        pause.expect("outdated continuation ran");
                                                    started.send(()).unwrap();
                                                    gate.await.unwrap();
                                                    let cleanup_trace = trace.clone();
                                                    Some((
                                                        Ok::<_, Infallible>(
                                                            cordis_core::Disposer::sync(
                                                                move || {
                                                                    cleanup_trace
                                                                        .borrow_mut()
                                                                        .push(10);
                                                                    Ok::<_, Infallible>(())
                                                                },
                                                            ),
                                                        ),
                                                        (None, trace),
                                                    ))
                                                },
                                            )
                                        })?;
                                        effect.ready().await?;
                                    }
                                    Ok::<_, BoxError>(())
                                }
                            }
                        }),
                        1,
                    )
                    .unwrap();
                entered.await.unwrap();
                let task = fiber.update(2).unwrap();
                release.send(()).unwrap();
                task.await.unwrap();
                assert_eq!(*trace.borrow(), [1, 10, 2]);
                assert!(runtime.take_failures().is_empty());
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[test]
fn validation_issues_retain_structured_paths_and_format_like_the_pinned_contract() {
    use cordis_core::{PathSegment, ValidationError, ValidationIssue};
    let error = ValidationError::new(vec![
        ValidationIssue {
            message: "not a number".into(),
            path: vec![PathSegment::Key("items".into()), PathSegment::Index(0)],
        },
        ValidationIssue {
            message: "required".into(),
            path: vec![],
        },
    ]);
    assert_eq!(
        error.to_string(),
        "invalid config:\n  - not a number (at items.0)\n  - required"
    );
    assert_eq!(error.issues[0].path[1], PathSegment::Index(0));
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_update_hook_removal_and_local_before_global_order_match_the_router() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let local = Rc::new(Cell::new(0));
            let global = Rc::new(Cell::new(0));
            root.on_with(
                &hooks::update(),
                Listener::around_async({
                    let global = global.clone();
                    move |_, _, next| {
                        global.set(global.get() + 1);
                        async move { next.call().await }
                    }
                }),
                ListenerOptions {
                    global: true,
                    ..Default::default()
                },
            )
            .unwrap();
            let token = Rc::new(RefCell::new(None));
            let fiber = root
                .plugin(
                    &Plugin::new("worker", {
                        let local = local.clone();
                        let token = token.clone();
                        move |ctx, _: u32| {
                            let local = local.clone();
                            let token = token.clone();
                            async move {
                                *token.borrow_mut() =
                                    Some(ctx.on_update_sync(move |_: Rc<u32>| {
                                        local.set(local.get() + 1);
                                        Ok::<_, Infallible>(false)
                                    })?);
                                Ok::<_, BoxError>(())
                            }
                        }
                    }),
                    0,
                )
                .unwrap();
            fiber.ready().unwrap().await.unwrap();
            fiber.update(1).unwrap().await.unwrap();
            assert_eq!(local.get(), 1);
            assert_eq!(global.get(), 0);
            token.borrow_mut().take().unwrap().remove().unwrap();
            fiber.update(2).unwrap().await.unwrap();
            assert_eq!(local.get(), 1);
            assert_eq!(global.get(), 1);
            fiber.update(3).unwrap().await.unwrap();
            assert_eq!(local.get(), 2);
            assert_eq!(global.get(), 1);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_recovery_is_awaited_and_dropped_failed_recovery_is_still_diagnosed() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let (started, entered) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let gates = RefCell::new(Some((started, gate)));
                let fiber = root
                    .plugin(
                        &Plugin::new("recovering", move |_, value: i32| {
                            let pause = if value == 1 {
                                gates.borrow_mut().take()
                            } else {
                                None
                            };
                            async move {
                                if let Some((started, gate)) = pause {
                                    started.send(()).unwrap();
                                    gate.await.unwrap();
                                }
                                if value < 0 {
                                    Err(std::io::Error::other("failed recovery"))
                                } else {
                                    Ok(())
                                }
                            }
                        }),
                        -1,
                    )
                    .unwrap();
                assert!(fiber.ready().unwrap().await.is_err());
                assert_eq!(runtime.take_failures().len(), 1);
                let recovery = fiber.update(1).unwrap();
                entered.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Loading);
                release.send(()).unwrap();
                recovery.await.unwrap();
                assert_eq!(fiber.state(), FiberState::Active);
                assert!(fiber.update(-2).unwrap().await.is_err());
                assert_eq!(runtime.take_failures().len(), 1);
                drop(fiber.update(-3).unwrap());
                assert!(fiber.ready().unwrap().await.is_err());
                assert_eq!(runtime.take_failures().len(), 1);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}
