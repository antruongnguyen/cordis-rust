//! Resource setup/rollback/composition equivalents of pinned dispose.spec.ts.
use cordis_core::{DispatchOptions, Disposer, Event, Listener, Runtime, SetupError};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

fn resource(trace: Rc<RefCell<Vec<u32>>>, number: u32) -> Disposer {
    Disposer::new(move || async move {
        trace.borrow_mut().push(number);
        Ok::<_, Infallible>(())
    })
}
fn error(text: &str) -> std::io::Error {
    std::io::Error::other(text)
}
async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("effect test stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn sync_setup_is_immediate_and_nested_effects_and_listeners_transfer_ownership() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let trace = Rc::new(RefCell::new(Vec::new()));
            let event = Event::<()>::symbol("owned listener");
            let calls = Rc::new(Cell::new(0));
            let outer = ctx
                .effect_iter(|| {
                    trace.borrow_mut().push(0);
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
                        .effect(|| {
                            Ok::<_, Infallible>(Disposer::compose(vec![
                                resource(trace.clone(), 2),
                                resource(trace.clone(), 3),
                            ]))
                        })
                        .unwrap();
                    vec![
                        Ok::<_, Infallible>(resource(trace.clone(), 1)),
                        Ok(Disposer::from_effect(listener.into_effect()).unwrap()),
                        Ok(Disposer::from_effect(nested).unwrap()),
                    ]
                })
                .unwrap();
            assert_eq!(*trace.borrow(), [0]);
            ctx.emit(&event, (), DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 1);
            assert_eq!(outer.dispose().unwrap().await.unwrap().attempted(), 1);
            assert_eq!(*trace.borrow(), [0, 3, 2, 1]);
            ctx.emit(&event, (), DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 1);
            assert_eq!(ctx.drain().unwrap().await.unwrap().attempted(), 0);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn async_return_is_not_cancelled_by_immediate_disposal_and_dropped_waiter() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        let trace = Rc::new(RefCell::new(Vec::new()));
        runtime
            .run(async {
                let prefix = Rc::new(Cell::new(false));
                let (entered_tx, entered_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                let (done_tx, done_rx) = oneshot::channel();
                let registration = ctx
                    .effect_async({
                        let trace = trace.clone();
                        let prefix = prefix.clone();
                        move || {
                            prefix.set(true);
                            async move {
                                entered_tx.send(()).unwrap();
                                release_rx.await.unwrap();
                                trace.borrow_mut().push(1);
                                Ok::<_, Infallible>(Disposer::new(move || async move {
                                    trace.borrow_mut().push(2);
                                    done_tx.send(()).unwrap();
                                    Ok::<_, Infallible>(())
                                }))
                            }
                        }
                    })
                    .unwrap();
                assert!(prefix.get());
                drop(registration.dispose().unwrap());
                entered_rx.await.unwrap();
                release_tx.send(()).unwrap();
                done_rx.await.unwrap();
                assert_eq!(*trace.borrow(), [1, 2]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn async_stream_stops_before_next_step_but_collects_in_flight_final_yield() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        runtime
            .run(async {
                let trace = Rc::new(RefCell::new(Vec::new()));
                let (second_tx, second_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                let registration = ctx
                    .effect_stream({
                        let trace = trace.clone();
                        move || {
                            futures_util::stream::unfold(
                                (0, Some(second_tx), Some(release_rx), trace),
                                |(step, mut entered, mut release, trace)| async move {
                                    if step == 2 {
                                        panic!("outdated stream was advanced");
                                    }
                                    if step == 1 {
                                        entered.take().unwrap().send(()).unwrap();
                                        release.take().unwrap().await.unwrap();
                                    }
                                    trace.borrow_mut().push(if step == 0 { 1 } else { 3 });
                                    let disposer =
                                        resource(trace.clone(), if step == 0 { 2 } else { 4 });
                                    Some((
                                        Ok::<_, Infallible>(disposer),
                                        (step + 1, entered, release, trace),
                                    ))
                                },
                            )
                        }
                    })
                    .unwrap();
                second_rx.await.unwrap();
                let group = ctx
                    .effect(|| Ok::<_, Infallible>(Disposer::from_effect(registration).unwrap()))
                    .unwrap();
                let done = group.dispose().unwrap();
                release_tx.send(()).unwrap();
                done.await.unwrap();
                assert_eq!(*trace.borrow(), [1, 3, 4, 2]);
                assert!(
                    runtime.take_failures().is_empty(),
                    "transferred stream advanced an obsolete step"
                );
                ctx.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn stream_failure_rolls_back_before_ready_rejects_and_preserves_source() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let trace = Rc::new(RefCell::new(Vec::new()));
            let registration = ctx
                .effect_stream({
                    let trace = trace.clone();
                    let context = ctx.clone();
                    move || {
                        futures_util::stream::iter(vec![
                            Ok(Disposer::new(move || async move {
                                assert_eq!(
                                    context.drain().err(),
                                    Some(cordis_core::AdmissionError::ReentrantDrain)
                                );
                                trace.borrow_mut().push(1);
                                Ok::<_, Infallible>(())
                            })),
                            Err(error("setup failed")),
                        ])
                    }
                })
                .unwrap();
            let error = registration.ready().await.unwrap_err();
            assert!(matches!(error, SetupError::Callback(_)));
            assert_eq!(*trace.borrow(), [1]);
            assert_eq!(runtime.take_failures().len(), 1);
            assert_eq!(
                registration.dispose().unwrap().await.unwrap().attempted(),
                0
            );
            assert_eq!(ctx.drain().unwrap().await.unwrap().attempted(), 0);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_iterator_failure_starts_partial_rollback_and_setup_panics_are_explicit() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        runtime
            .run(async {
                let (done_tx, done_rx) = oneshot::channel();
                let result = ctx.effect_iter(|| {
                    vec![
                        Ok(Disposer::new(move || async move {
                            done_tx.send(()).unwrap();
                            Ok::<_, Infallible>(())
                        })),
                        Err(error("iterator failure")),
                    ]
                });
                assert!(matches!(result, Err(SetupError::Callback(_))));
                done_rx.await.unwrap();
                let result =
                    ctx.effect(|| -> Result<Disposer, Infallible> { panic!("setup panic") });
                assert!(matches!(result, Err(SetupError::Callback(_))));
                ctx.drain().unwrap().await.unwrap();
                assert_eq!(runtime.take_failures().len(), 2);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn composed_async_cleanup_waits_in_reverse_order_and_root_siblings_progress() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        runtime
            .run(async {
                let trace = Rc::new(RefCell::new(Vec::new()));
                let (entered_tx, entered_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                let (sibling_tx, sibling_rx) = oneshot::channel();
                let _group = ctx
                    .effect({
                        let trace = trace.clone();
                        move || {
                            Ok::<_, Infallible>(Disposer::compose(vec![
                                resource(trace.clone(), 1),
                                Disposer::new(move || async move {
                                    trace.borrow_mut().push(2);
                                    entered_tx.send(()).unwrap();
                                    release_rx.await.unwrap();
                                    trace.borrow_mut().push(3);
                                    Ok::<_, Infallible>(())
                                }),
                            ]))
                        }
                    })
                    .unwrap();
                let _sibling = ctx
                    .on_dispose(move || async move {
                        sibling_tx.send(()).unwrap();
                        Ok::<_, Infallible>(())
                    })
                    .unwrap();
                let drain = ctx.drain().unwrap();
                entered_rx.await.unwrap();
                sibling_rx.await.unwrap();
                assert_eq!(*trace.borrow(), [2]);
                release_tx.send(()).unwrap();
                drain.await.unwrap();
                assert_eq!(*trace.borrow(), [2, 3, 1]);
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn effect_readiness_does_not_dispose_successful_acquisitions() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let trace = Rc::new(RefCell::new(Vec::new()));
            let registration = ctx
                .effect_async({
                    let trace = trace.clone();
                    let context = ctx.clone();
                    move || {
                        assert_eq!(
                            context.drain().err(),
                            Some(cordis_core::AdmissionError::ReentrantDrain)
                        );
                        async move {
                            assert_eq!(
                                context.drain().err(),
                                Some(cordis_core::AdmissionError::ReentrantDrain)
                            );
                            trace.borrow_mut().push(1);
                            Ok::<_, Infallible>(resource(trace, 2))
                        }
                    }
                })
                .unwrap();
            registration.ready().await.unwrap();
            assert_eq!(*trace.borrow(), [1]);
            registration.dispose().unwrap().await.unwrap();
            assert_eq!(*trace.borrow(), [1, 2]);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn finite_stream_readiness_preserves_all_resources_and_async_failure_is_explicit() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let trace = Rc::new(RefCell::new(Vec::new()));
            let registration = ctx
                .effect_stream({
                    let trace = trace.clone();
                    move || {
                        futures_util::stream::iter(vec![
                            Ok::<_, Infallible>(resource(trace.clone(), 1)),
                            Ok(resource(trace.clone(), 2)),
                            Ok(resource(trace, 3)),
                        ])
                    }
                })
                .unwrap();
            registration.ready().await.unwrap();
            assert!(trace.borrow().is_empty());
            registration.dispose().unwrap().await.unwrap();
            assert_eq!(*trace.borrow(), [3, 2, 1]);
            let failed = ctx
                .effect_async(|| async { Err::<Disposer, _>(error("async return failed")) })
                .unwrap();
            assert!(matches!(failed.ready().await, Err(SetupError::Callback(_))));
            assert_eq!(runtime.take_failures().len(), 1);
            assert_eq!(failed.dispose().unwrap().await.unwrap().attempted(), 0);
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}
