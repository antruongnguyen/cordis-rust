//! Exact M2 sync timing and effect-tree equivalents of pinned dispose.spec.ts.
use cordis_core::{
    AdmissionError, DispatchOptions, Disposer, EffectMeta, Event, Listener, Runtime, SetupError,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

fn meta(label: &str, children: Vec<EffectMeta>) -> EffectMeta {
    EffectMeta {
        label: label.into(),
        children,
    }
}
fn sync_resource(trace: Rc<RefCell<Vec<u32>>>, value: u32) -> Disposer {
    Disposer::sync(move || {
        trace.borrow_mut().push(value);
        Ok::<_, Infallible>(())
    })
}
async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("effect metadata test stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_manual_disposal_and_value_only_snapshots_do_not_retain_resources() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let trace = Rc::new(RefCell::new(Vec::new()));
    let probe = Rc::new(());
    let weak = Rc::downgrade(&probe);
    let snapshot = runtime
        .run(async {
            let effect = ctx
                .on_dispose_sync({
                    let trace = trace.clone();
                    move || {
                        drop(probe);
                        trace.borrow_mut().push(1);
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap()
                .with_label("resource");
            let snapshot = ctx.get_effects().unwrap();
            assert_eq!(snapshot, [meta("resource", vec![])]);
            let effect = effect.with_label("renamed");
            assert_eq!(ctx.get_effects().unwrap(), [meta("renamed", vec![])]);
            let done = effect.dispose().unwrap();
            assert_eq!(
                *trace.borrow(),
                [1],
                "sync cleanup must run before awaiting completion"
            );
            assert!(weak.upgrade().is_none());
            assert!(ctx.get_effects().unwrap().is_empty());
            let mut done = Box::pin(done);
            assert!(
                std::future::Future::poll(
                    done.as_mut(),
                    &mut std::task::Context::from_waker(std::task::Waker::noop())
                )
                .is_ready()
            );
            snapshot
        })
        .await
        .unwrap();
    drop(runtime);
    assert_eq!(snapshot, [meta("resource", vec![])]);
    assert_eq!(
        ctx.get_effects().err(),
        Some(AdmissionError::RuntimeDropped)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn nested_labels_track_only_yielded_effects_and_sync_children_run_before_return() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let trace = Rc::new(RefCell::new(Vec::new()));
    runtime
        .run(async {
            let event = Event::<()>::named("custom-event");
            let calls = Rc::new(Cell::new(0));
            let outer = ctx
                .effect_iter(|| {
                    let listener = ctx
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
                    let nested = ctx
                        .effect_iter(|| {
                            let listener = ctx
                                .on(
                                    &event,
                                    Listener::sync(|_, ()| {
                                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                                    }),
                                )
                                .unwrap();
                            vec![
                                Ok::<_, Infallible>(
                                    Disposer::from_effect(listener.into_effect()).unwrap(),
                                ),
                                Ok(sync_resource(trace.clone(), 3)),
                            ]
                        })
                        .unwrap()
                        .with_label("nested");
                    vec![
                        Ok::<_, Infallible>(sync_resource(trace.clone(), 1)),
                        Ok(Disposer::from_effect(listener.into_effect()).unwrap()),
                        Ok(sync_resource(trace.clone(), 2)),
                        Ok(Disposer::from_effect(nested).unwrap()),
                    ]
                })
                .unwrap()
                .with_label("outer");
            let _sibling = ctx
                .on(
                    &event,
                    Listener::sync(|_, ()| Ok::<_, Infallible>(ControlFlow::Continue(()))),
                )
                .unwrap();
            let label = "ctx.on(\"custom-event\")";
            assert_eq!(
                ctx.get_effects().unwrap(),
                [
                    meta(
                        "outer",
                        vec![
                            meta(label, vec![]),
                            meta("nested", vec![meta(label, vec![])])
                        ]
                    ),
                    meta(label, vec![])
                ]
            );
            let completion = outer.dispose().unwrap();
            assert_eq!(*trace.borrow(), [3, 2, 1]);
            assert_eq!(ctx.get_effects().unwrap(), [meta(label, vec![])]);
            ctx.emit(&event, (), DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(
                calls.get(),
                0,
                "composed listener removal must be synchronous"
            );
            completion.await.unwrap();
            ctx.drain().unwrap().await.unwrap();
            assert!(ctx.get_effects().unwrap().is_empty());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_iterator_error_and_panic_finish_partial_rollback_before_return() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let trace = Rc::new(RefCell::new(Vec::new()));
    runtime
        .run(async {
            let error = ctx.effect_iter(|| {
                vec![
                    Ok(sync_resource(trace.clone(), 1)),
                    Err(std::io::Error::other("setup")),
                ]
            });
            assert!(matches!(error, Err(SetupError::Callback(_))));
            assert_eq!(*trace.borrow(), [1]);
            assert!(ctx.get_effects().unwrap().is_empty());
            let mut step = 0;
            let result = ctx.effect_iter(|| {
                std::iter::from_fn({
                    let trace = trace.clone();
                    move || {
                        step += 1;
                        if step == 1 {
                            Some(Ok::<_, Infallible>(sync_resource(trace.clone(), 2)))
                        } else {
                            panic!("iterator step panicked")
                        }
                    }
                })
            });
            assert!(matches!(result, Err(SetupError::Callback(_))));
            assert_eq!(*trace.borrow(), [1, 2]);
            assert!(ctx.get_effects().unwrap().is_empty());
            assert_eq!(runtime.take_failures().len(), 2);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_composition_runs_sync_prefix_now_and_never_advances_past_pending_child() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        let trace = Rc::new(RefCell::new(Vec::new()));
        runtime
            .run(async {
                let (entered_tx, entered_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                let outer = ctx
                    .effect({
                        let trace = trace.clone();
                        move || {
                            Ok::<_, Infallible>(Disposer::compose(vec![
                                sync_resource(trace.clone(), 1),
                                Disposer::new({
                                    let trace = trace.clone();
                                    move || {
                                        trace.borrow_mut().push(2);
                                        async move {
                                            entered_tx.send(()).unwrap();
                                            release_rx.await.unwrap();
                                            trace.borrow_mut().push(3);
                                            Ok::<_, Infallible>(())
                                        }
                                    }
                                }),
                                sync_resource(trace.clone(), 4),
                            ]))
                        }
                    })
                    .unwrap();
                let done = outer.dispose().unwrap();
                assert_eq!(
                    *trace.borrow(),
                    [4, 2],
                    "only sync prefix runs before first await"
                );
                entered_rx.await.unwrap();
                assert_eq!(*trace.borrow(), [4, 2]);
                release_tx.send(()).unwrap();
                done.await.unwrap();
                assert_eq!(*trace.borrow(), [4, 2, 3, 1]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn async_acquisition_updates_child_metadata_and_ready_cleanup_remains_an_async_barrier() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let effect = ctx
                .effect_stream({
                    let ctx = ctx.clone();
                    move || {
                        futures_util::stream::unfold((0, ctx), |(step, ctx)| async move {
                            if step == 2 {
                                return None;
                            }
                            let child = ctx
                                .on_dispose_sync(|| Ok::<_, Infallible>(()))
                                .unwrap()
                                .with_label(format!("child {step}"));
                            Some((
                                Ok::<_, Infallible>(Disposer::from_effect(child).unwrap()),
                                (step + 1, ctx),
                            ))
                        })
                    }
                })
                .unwrap()
                .with_label("stream");
            assert_eq!(ctx.get_effects().unwrap(), [meta("stream", vec![])]);
            effect.ready().await.unwrap();
            assert_eq!(
                ctx.get_effects().unwrap(),
                [meta(
                    "stream",
                    vec![meta("child 0", vec![]), meta("child 1", vec![])]
                )]
            );
            let done = effect.dispose().unwrap();
            assert!(ctx.get_effects().unwrap().is_empty());
            done.await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_composed_cleanup_stops_its_chain_but_independent_root_effects_are_attempted() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let calls = Rc::new(RefCell::new(Vec::new()));
    runtime
        .run(async {
            let group = ctx
                .effect({
                    let calls = calls.clone();
                    move || {
                        Ok::<_, Infallible>(Disposer::compose(vec![
                            sync_resource(calls.clone(), 1),
                            Disposer::sync(|| Err::<(), _>(std::io::Error::other("child failed"))),
                            sync_resource(calls, 3),
                        ]))
                    }
                })
                .unwrap();
            let done = group.dispose().unwrap();
            assert_eq!(*calls.borrow(), [3]);
            let report = done.await.unwrap();
            assert_eq!(report.failures().len(), 1);
            let _panic = ctx
                .on_dispose_sync(|| -> Result<(), Infallible> { panic!("sync cleanup") })
                .unwrap();
            let _sibling = ctx
                .on_dispose_sync({
                    let calls = calls.clone();
                    move || {
                        calls.borrow_mut().push(4);
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
            let report = ctx.drain().unwrap().await.unwrap();
            assert_eq!(report.attempted(), 2);
            assert_eq!(report.failures().len(), 1);
            assert_eq!(*calls.borrow(), [3, 4]);
            assert!(ctx.get_effects().unwrap().is_empty());
            assert_eq!(runtime.take_failures().len(), 2);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn sync_cleanup_can_reenter_metadata_and_register_the_next_root_snapshot() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let captured = ctx.clone();
            let effect = ctx
                .on_dispose_sync(move || {
                    assert!(captured.get_effects().unwrap().is_empty());
                    assert_eq!(captured.drain().err(), Some(AdmissionError::ReentrantDrain));
                    let _next = captured
                        .on_dispose_sync(|| Ok::<_, Infallible>(()))
                        .unwrap()
                        .with_label("later");
                    Ok::<_, Infallible>(())
                })
                .unwrap()
                .with_label("initial");
            let done = effect.dispose().unwrap();
            assert_eq!(ctx.get_effects().unwrap(), [meta("later", vec![])]);
            done.await.unwrap();
            ctx.drain().unwrap().await.unwrap();
            assert!(ctx.get_effects().unwrap().is_empty());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn labels_do_not_create_cross_runtime_ownership_and_wrong_host_transfers_leave_roots_unchanged()
 {
    let first = Runtime::new();
    let second = Runtime::new();
    let ctx = first.context();
    let mut token = None;
    first
        .run(async {
            token = Some(
                ctx.on_dispose_sync(|| Ok::<_, Infallible>(()))
                    .unwrap()
                    .with_label("first"),
            );
        })
        .await
        .unwrap();
    second
        .run(async {
            assert!(matches!(
                Disposer::from_effect(token.take().unwrap()),
                Err(SetupError::Admission(AdmissionError::WrongHost))
            ));
        })
        .await
        .unwrap();
    assert_eq!(ctx.get_effects().unwrap(), [meta("first", vec![])]);
    assert!(second.context().get_effects().unwrap().is_empty());
    first
        .run(async {
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
    assert!(ctx.get_effects().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn active_setup_cannot_transfer_itself_or_an_ancestor_into_its_own_tree() {
    bounded(async {
        for ancestor in [false, true] {
            let runtime = Runtime::new();
            let ctx = runtime.context();
            let token = Rc::new(RefCell::new(None::<cordis_core::Effect>));
            runtime
                .run(async {
                    let (checked_tx, checked_rx) = oneshot::channel();
                    let effect = ctx
                        .effect_stream({
                            let token = token.clone();
                            move || {
                                futures_util::stream::once(async move {
                                    assert!(matches!(
                                        Disposer::from_effect(token.borrow_mut().take().unwrap()),
                                        Err(SetupError::RecursiveComposition)
                                    ));
                                    checked_tx.send(()).unwrap();
                                    Ok::<_, Infallible>(Disposer::sync(|| Ok::<_, Infallible>(())))
                                })
                            }
                        })
                        .unwrap()
                        .with_label("child");
                    let effect = if ancestor {
                        ctx.effect(|| Ok::<_, Infallible>(Disposer::from_effect(effect).unwrap()))
                            .unwrap()
                            .with_label("parent")
                    } else {
                        effect
                    };
                    *token.borrow_mut() = Some(effect);
                    checked_rx.await.unwrap();
                    let expected = if ancestor {
                        vec![meta("parent", vec![meta("child", vec![])])]
                    } else {
                        vec![meta("child", vec![])]
                    };
                    assert_eq!(
                        ctx.get_effects().unwrap(),
                        expected,
                        "refusal must leave the existing root claim intact"
                    );
                    let report = ctx.drain().unwrap().await.unwrap();
                    assert!(report.failures().is_empty());
                    assert!(ctx.get_effects().unwrap().is_empty());
                })
                .await
                .unwrap();
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_stream_reports_cleanup_panic_and_setup_error_after_rollback() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let registration = ctx
                .effect_stream(|| {
                    futures_util::stream::iter(vec![
                        Ok(Disposer::sync(|| -> Result<(), Infallible> {
                            panic!("rollback panic")
                        })),
                        Err(std::io::Error::other("setup failed")),
                    ])
                })
                .unwrap();
            assert!(matches!(
                registration.ready().await,
                Err(SetupError::Callback(_))
            ));
            let failures = runtime.take_failures();
            assert_eq!(failures.len(), 2);
            assert!(failures[0].is_panic());
            assert!(!failures[1].is_panic());
            assert!(ctx.get_effects().unwrap().is_empty());
            assert_eq!(
                registration.dispose().unwrap().await.unwrap().attempted(),
                0
            );
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn transferred_exporter_is_labeled_and_removed_before_sync_group_disposal_returns() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let calls = Rc::new(Cell::new(0));
    runtime
        .run(async {
            let exporter = ctx
                .add_exporter(cordis_core::ExporterOptions::default(), {
                    let calls = calls.clone();
                    move |_| {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
            assert_eq!(
                ctx.get_effects().unwrap(),
                [meta("ctx.logger.exporter()", vec![])]
            );
            ctx.logger(None).info("before").unwrap();
            assert_eq!(calls.get(), 1);
            let group = ctx
                .effect(|| {
                    Ok::<_, Infallible>(Disposer::from_effect(exporter.into_effect()).unwrap())
                })
                .unwrap()
                .with_label("exports");
            assert_eq!(
                ctx.get_effects().unwrap(),
                [meta("exports", vec![meta("ctx.logger.exporter()", vec![])])]
            );
            let completion = group.dispose().unwrap();
            ctx.logger(None).info("after").unwrap();
            assert_eq!(calls.get(), 1);
            assert!(ctx.get_effects().unwrap().is_empty());
            completion.await.unwrap();
            assert_eq!(
                ctx.log_buffer().unwrap().len(),
                2,
                "built-in buffer is not a disposable effect"
            );
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn nested_setup_keeps_the_outer_acquisition_guard_until_its_poll_finishes() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        let token = Rc::new(RefCell::new(None::<cordis_core::Effect>));
        runtime
            .run(async {
                let (checked_tx, checked_rx) = oneshot::channel();
                let effect = ctx
                    .effect_stream({
                        let token = token.clone();
                        let ctx = ctx.clone();
                        move || {
                            futures_util::stream::once(async move {
                                let inner = ctx
                                    .effect(|| {
                                        assert!(matches!(
                                            Disposer::from_effect(
                                                token.borrow_mut().take().unwrap()
                                            ),
                                            Err(SetupError::RecursiveComposition)
                                        ));
                                        Ok::<_, Infallible>(Disposer::sync(|| {
                                            Ok::<_, Infallible>(())
                                        }))
                                    })
                                    .unwrap()
                                    .with_label("nested setup");
                                checked_tx.send(()).unwrap();
                                Ok::<_, Infallible>(Disposer::from_effect(inner).unwrap())
                            })
                        }
                    })
                    .unwrap()
                    .with_label("outer setup");
                *token.borrow_mut() = Some(effect);
                checked_rx.await.unwrap();
                assert_eq!(
                    ctx.get_effects().unwrap(),
                    [meta("outer setup", vec![meta("nested setup", vec![])])]
                );
                let report = ctx.drain().unwrap().await.unwrap();
                assert!(report.failures().is_empty());
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
    })
    .await;
}
