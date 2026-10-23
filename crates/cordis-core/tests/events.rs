//! Integrated equivalents of the public dispatch cases in pinned events.spec.ts.
use cordis_core::{
    BoxError, DispatchOptions as Options, Event, EventError, ExporterOptions, Listener,
    ListenerOptions, Runtime,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

fn continued<R>() -> Result<ControlFlow<R>, Infallible> {
    Ok(ControlFlow::Continue(()))
}
fn error(text: &str) -> std::io::Error {
    std::io::Error::other(text)
}
async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("event test stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn symbols_named_contracts_prototype_names_and_empty_bucket_removal() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            for name in ["__proto__", "toString", "constructor"] {
                let event = Event::<u32, u32>::named(name);
                ctx.emit(&event, 0, Options::default())
                    .unwrap()
                    .await
                    .unwrap();
                let calls = Rc::new(RefCell::new(Vec::new()));
                let registration = ctx
                    .on(
                        &event,
                        Listener::sync({
                            let calls = calls.clone();
                            move |_, value| {
                                calls.borrow_mut().push(value);
                                Ok::<_, Infallible>(ControlFlow::Break(value))
                            }
                        }),
                    )
                    .unwrap();
                ctx.emit(&event, 1, Options::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(
                    ctx.bail(&event, 0, Options::default()).unwrap(),
                    ControlFlow::Break(0)
                );
                assert_eq!(
                    ctx.serial(&event, 2, Options::default())
                        .unwrap()
                        .await
                        .unwrap(),
                    ControlFlow::Break(2)
                );
                ctx.parallel(&event, 3, Options::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(*calls.borrow(), [1, 0, 2, 3]);
                assert!(matches!(
                    ctx.on(
                        &Event::<String>::named(name),
                        Listener::sync(|_, _| continued())
                    ),
                    Err(EventError::ContractMismatch(_))
                ));
                assert!(registration.remove().unwrap());
                ctx.emit(&event, 4, Options::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(*calls.borrow(), [1, 0, 2, 3]);
                let new = ctx
                    .on(
                        &Event::<String>::named(name),
                        Listener::sync(|_, _| continued()),
                    )
                    .unwrap();
                assert!(
                    new.remove().unwrap(),
                    "last removal must release the live contract bucket"
                );
            }
            let symbol = Event::<u32, u32>::symbol("same");
            let other = Event::<u32, u32>::symbol("same");
            let calls = Rc::new(Cell::new(0));
            let _reg = ctx
                .on(
                    &symbol,
                    Listener::sync({
                        let calls = calls.clone();
                        move |_, v| {
                            calls.set(calls.get() + 1);
                            Ok::<_, Infallible>(ControlFlow::Break(v))
                        }
                    }),
                )
                .unwrap();
            ctx.emit(&other, 1, Options::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 0);
            assert_eq!(
                ctx.bail(&symbol.clone(), 2, Options::default()).unwrap(),
                ControlFlow::Break(2)
            );
            ctx.emit(&symbol, 3, Options::default())
                .unwrap()
                .await
                .unwrap();
            ctx.parallel(&symbol, 4, Options::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(
                ctx.serial(&symbol, 5, Options::default())
                    .unwrap()
                    .await
                    .unwrap(),
                ControlFlow::Break(5)
            );
            assert_eq!(calls.get(), 4);
            ctx.drain().unwrap().await.unwrap();
            ctx.emit(&symbol, 3, Options::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 4);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn once_is_removed_before_recursive_invocation_and_manual_remove_is_exact() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let event = Event::<()>::symbol("once");
            let calls = Rc::new(Cell::new(0));
            let registration = ctx
                .once(
                    &event,
                    Listener::sync({
                        let event = event.clone();
                        let calls = calls.clone();
                        move |ctx, ()| {
                            calls.set(calls.get() + 1);
                            drop(ctx.emit(&event, (), Options::default()).unwrap());
                            continued()
                        }
                    }),
                )
                .unwrap();
            ctx.emit(&event, (), Options::default())
                .unwrap()
                .await
                .unwrap();
            ctx.emit(&event, (), Options::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 1);
            assert!(!registration.remove().unwrap());
            assert_eq!(ctx.drain().unwrap().await.unwrap().attempted(), 0);
        })
        .await
        .unwrap();
}

#[derive(Clone)]
struct Visible(bool);
#[tokio::test(flavor = "current_thread")]
async fn prepend_and_global_filters_apply_to_each_dispatch_shape() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            for mode in ["emit", "parallel", "serial", "bail"] {
                let event = Event::<()>::symbol(mode);
                let trace = Rc::new(RefCell::new(Vec::new()));
                let visible = ctx.extend(Visible(true));
                let hidden = ctx.extend(Visible(false));
                let _a = visible
                    .on(
                        &event,
                        Listener::sync({
                            let trace = trace.clone();
                            move |_, ()| {
                                trace.borrow_mut().push("visible");
                                continued()
                            }
                        }),
                    )
                    .unwrap();
                let _b = hidden
                    .on(
                        &event,
                        Listener::sync({
                            let trace = trace.clone();
                            move |_, ()| {
                                trace.borrow_mut().push("hidden");
                                continued()
                            }
                        }),
                    )
                    .unwrap();
                let _c = hidden
                    .on_with(
                        &event,
                        Listener::sync({
                            let trace = trace.clone();
                            move |_, ()| {
                                trace.borrow_mut().push("global");
                                continued()
                            }
                        }),
                        ListenerOptions {
                            prepend: true,
                            global: true,
                        },
                    )
                    .unwrap();
                let options = Options::filtered(|ctx| ctx.metadata::<Visible>().unwrap().0);
                match mode {
                    "emit" => ctx.emit(&event, (), options).unwrap().await.unwrap(),
                    "parallel" => ctx.parallel(&event, (), options).unwrap().await.unwrap(),
                    "serial" => {
                        let _ = ctx.serial(&event, (), options).unwrap().await.unwrap();
                    }
                    _ => {
                        let _ = ctx.bail(&event, (), options).unwrap();
                    }
                }
                assert_eq!(*trace.borrow(), ["global", "visible"]);
                trace.borrow_mut().clear();
                ctx.emit(&event, (), Options::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(*trace.borrow(), ["global", "visible", "hidden"]);
                ctx.drain().unwrap().await.unwrap();
            }
            let event = Event::<(), u32>::symbol("waterfall filter");
            let trace = Rc::new(RefCell::new(Vec::new()));
            let _a = ctx
                .extend(Visible(false))
                .on(&event, Listener::around(|_, (), _| Ok::<_, Infallible>(99)))
                .unwrap();
            let _b = ctx
                .extend(Visible(true))
                .on(
                    &event,
                    Listener::around({
                        let trace = trace.clone();
                        move |_, (), next| {
                            trace.borrow_mut().push("around");
                            next.call()
                        }
                    }),
                )
                .unwrap();
            let result = ctx
                .waterfall(
                    &event,
                    (),
                    Options::filtered(|ctx| ctx.metadata::<Visible>().unwrap().0),
                    || Ok::<_, Infallible>(7),
                )
                .unwrap();
            assert_eq!(result, 7);
            assert_eq!(*trace.borrow(), ["around"]);
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn removal_after_selection_does_not_cancel_in_flight_snapshots() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let event = Event::<()>::symbol("snapshot");
            let trace = Rc::new(RefCell::new(Vec::new()));
            let victim = Rc::new(RefCell::new(None::<cordis_core::ListenerRegistration>));
            let _first = ctx
                .on(
                    &event,
                    Listener::sync({
                        let victim = victim.clone();
                        let trace = trace.clone();
                        move |_, ()| {
                            trace.borrow_mut().push("first");
                            if let Some(victim) = victim.borrow_mut().take() {
                                victim.remove().unwrap();
                            }
                            continued()
                        }
                    }),
                )
                .unwrap();
            *victim.borrow_mut() = Some(
                ctx.on(
                    &event,
                    Listener::sync({
                        let trace = trace.clone();
                        move |_, ()| {
                            trace.borrow_mut().push("selected");
                            continued()
                        }
                    }),
                )
                .unwrap(),
            );
            ctx.emit(&event, (), Options::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(*trace.borrow(), ["first", "selected"]);
            trace.borrow_mut().clear();
            ctx.emit(&event, (), Options::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(*trace.borrow(), ["first"]);
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_invokes_prefixes_immediately_and_aggregates_all_failures() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        runtime
            .run(async {
                let event = Event::<(), u32>::symbol("parallel");
                let prefix = Rc::new(Cell::new(false));
                let _sync = ctx
                    .on(
                        &event,
                        Listener::sync(|_, ()| Err::<ControlFlow<u32>, _>(error("sync"))),
                    )
                    .unwrap();
                let (release_tx, release_rx) = oneshot::channel();
                let gate = Rc::new(RefCell::new(Some(release_rx)));
                let _async = ctx
                    .on(
                        &event,
                        Listener::asynchronous({
                            let prefix = prefix.clone();
                            move |_, ()| {
                                prefix.set(true);
                                let gate = gate.borrow_mut().take().unwrap();
                                async move {
                                    gate.await.unwrap();
                                    Err::<ControlFlow<u32>, _>(error("async"))
                                }
                            }
                        }),
                    )
                    .unwrap();
                let _panic = ctx
                    .on(
                        &event,
                        Listener::sync(|_, ()| -> Result<ControlFlow<u32>, Infallible> {
                            panic!("listener panic")
                        }),
                    )
                    .unwrap();
                let mut dispatch = Box::pin(ctx.parallel(&event, (), Options::default()).unwrap());
                assert!(prefix.get());
                assert!(
                    dispatch
                        .as_mut()
                        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                        .is_pending()
                );
                release_tx.send(()).unwrap();
                let EventError::Callbacks { event, failures } = dispatch.await.unwrap_err() else {
                    panic!("wrong error shape")
                };
                assert_eq!(event, "Symbol(parallel)");
                assert_eq!(failures.len(), 3);
                assert!(failures[2].is_panic());
                assert!(
                    runtime.take_failures().is_empty(),
                    "awaited errors belong to the caller"
                );
                ctx.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn serial_has_an_immediate_first_prefix_and_stops_on_zero_or_empty_answers() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let event = Event::<(), u32>::named("query");
            let prefix = Rc::new(Cell::new(false));
            let _first = ctx
                .on(
                    &event,
                    Listener::asynchronous({
                        let prefix = prefix.clone();
                        move |_, ()| {
                            prefix.set(true);
                            async { continued() }
                        }
                    }),
                )
                .unwrap();
            let _answer = ctx
                .on(
                    &event,
                    Listener::sync(|_, ()| Ok::<_, Infallible>(ControlFlow::Break(0))),
                )
                .unwrap();
            let _last = ctx
                .on(
                    &event,
                    Listener::sync(|_, ()| -> Result<ControlFlow<u32>, Infallible> {
                        panic!("after answer")
                    }),
                )
                .unwrap();
            let task = ctx.serial(&event, (), Options::default()).unwrap();
            assert!(prefix.get());
            assert_eq!(task.await.unwrap(), ControlFlow::Break(0));
            assert!(matches!(
                ctx.bail(&event, (), Options::default()),
                Err(EventError::UnsupportedMode { mode: "bail", .. })
            ));
            let string = Event::<(), String>::symbol("empty");
            let _registration = ctx
                .on(
                    &string,
                    Listener::sync(|_, ()| Ok::<_, Infallible>(ControlFlow::Break(String::new()))),
                )
                .unwrap();
            assert_eq!(
                ctx.bail(&string, (), Options::default()).unwrap(),
                ControlFlow::Break(String::new())
            );
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn emitted_async_failure_is_diagnosed_when_waiter_is_dropped() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        runtime
            .run(async {
                let (logged_tx, logged_rx) = oneshot::channel();
                let logged = RefCell::new(Some(logged_tx));
                let _export = ctx
                    .add_exporter(ExporterOptions::default(), move |record| {
                        if record.failure.is_some() {
                            if let Some(sender) = logged.borrow_mut().take() {
                                sender.send(()).unwrap();
                            }
                        }
                        Ok::<_, Infallible>(())
                    })
                    .unwrap();
                let event = Event::<()>::symbol("emit failure");
                let prefix = Rc::new(Cell::new(false));
                let _reg = ctx
                    .on(
                        &event,
                        Listener::asynchronous({
                            let prefix = prefix.clone();
                            move |_, ()| {
                                prefix.set(true);
                                async { Err::<ControlFlow<()>, _>(error("detached")) }
                            }
                        }),
                    )
                    .unwrap();
                drop(ctx.emit(&event, (), Options::default()).unwrap());
                assert!(prefix.get());
                logged_rx.await.unwrap();
                assert_eq!(runtime.take_failures().len(), 1);
                let records = ctx.log_buffer().unwrap();
                assert!(records.last().unwrap().failure.is_some());
                ctx.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_emit_failure_stops_later_callbacks() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let event = Event::<()>::symbol("sync failure");
            let _first = ctx
                .on(
                    &event,
                    Listener::sync(|_, ()| Err::<ControlFlow<()>, _>(error("stop"))),
                )
                .unwrap();
            let _last = ctx
                .on(
                    &event,
                    Listener::sync(|_, ()| -> Result<ControlFlow<()>, Infallible> {
                        panic!("after failure")
                    }),
                )
                .unwrap();
            assert!(matches!(
                ctx.emit(&event, (), Options::default()),
                Err(EventError::Callbacks { .. })
            ));
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn waterfall_is_around_middleware_with_veto_and_nested_async_calls() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            let event = Event::<u32, u32>::symbol("waterfall");
            let trace = Rc::new(RefCell::new(Vec::new()));
            let _outer = ctx
                .on(
                    &event,
                    Listener::around({
                        let trace = trace.clone();
                        move |_, value, next| {
                            trace.borrow_mut().push("before");
                            let result = next.call()?;
                            trace.borrow_mut().push("after");
                            Ok::<_, BoxError>(value + result)
                        }
                    }),
                )
                .unwrap();
            let inner = ctx
                .on(
                    &event,
                    Listener::around(|_, value, next| Ok::<_, BoxError>(value + next.call()?)),
                )
                .unwrap();
            assert_eq!(
                ctx.waterfall(&event, 1, Options::default(), || Ok::<_, Infallible>(2))
                    .unwrap(),
                4
            );
            assert_eq!(*trace.borrow(), ["before", "after"]);
            inner.remove().unwrap();
            let _veto = ctx
                .on(&event, Listener::around(|_, _, _| Ok::<_, Infallible>(9)))
                .unwrap();
            assert_eq!(
                ctx.waterfall(
                    &event,
                    1,
                    Options::default(),
                    || -> Result<u32, Infallible> { panic!("vetoed") }
                )
                .unwrap(),
                10
            );
            let async_event = Event::<u32, u32>::symbol("async waterfall");
            let prefix = Rc::new(Cell::new(false));
            let _async = ctx
                .on(
                    &async_event,
                    Listener::around_async({
                        let prefix = prefix.clone();
                        let event = async_event.clone();
                        move |ctx, value, next| {
                            prefix.set(true);
                            let event = event.clone();
                            async move {
                                let result = next.call().await?;
                                if value == 1 {
                                    Ok::<_, BoxError>(
                                        result
                                            + ctx
                                                .waterfall_async(
                                                    &event,
                                                    2,
                                                    Options::default(),
                                                    || async { Ok::<_, Infallible>(2) },
                                                )?
                                                .await?,
                                    )
                                } else {
                                    Ok(result)
                                }
                            }
                        }
                    }),
                )
                .unwrap();
            let task = ctx
                .waterfall_async(&async_event, 1, Options::default(), || async {
                    Ok::<_, Infallible>(2)
                })
                .unwrap();
            assert!(prefix.get());
            assert_eq!(task.await.unwrap(), 4);
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn root_removal_does_not_cancel_an_already_started_listener() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        runtime
            .run(async {
                let event = Event::<()>::symbol("retained dispatch");
                let (release_tx, release_rx) = oneshot::channel();
                let gate = RefCell::new(Some(release_rx));
                let _reg = ctx
                    .on(
                        &event,
                        Listener::asynchronous(move |_, ()| {
                            let gate = gate.borrow_mut().take().unwrap();
                            async move {
                                gate.await.unwrap();
                                continued()
                            }
                        }),
                    )
                    .unwrap();
                let dispatch = ctx.emit(&event, (), Options::default()).unwrap();
                ctx.drain().unwrap().await.unwrap();
                release_tx.send(()).unwrap();
                dispatch.await.unwrap();
                ctx.emit(&event, (), Options::default())
                    .unwrap()
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn snapshots_survive_removal_in_serial_parallel_bail_and_waterfall() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    runtime
        .run(async {
            for mode in ["serial", "parallel", "bail", "waterfall", "waterfall_async"] {
                let event = Event::<(), u32>::symbol(mode);
                let calls = Rc::new(Cell::new(0));
                let victim = Rc::new(RefCell::new(None::<cordis_core::ListenerRegistration>));
                match mode {
                    "waterfall" => {
                        let _outer = ctx
                            .on(
                                &event,
                                Listener::around({
                                    let victim = victim.clone();
                                    move |_, (), next| {
                                        victim.borrow_mut().take().unwrap().remove().unwrap();
                                        next.call()
                                    }
                                }),
                            )
                            .unwrap();
                        *victim.borrow_mut() = Some(
                            ctx.on(
                                &event,
                                Listener::around({
                                    let calls = calls.clone();
                                    move |_, (), next| {
                                        calls.set(calls.get() + 1);
                                        next.call()
                                    }
                                }),
                            )
                            .unwrap(),
                        );
                        assert_eq!(
                            ctx.waterfall(&event, (), Options::default(), || Ok::<_, Infallible>(
                                7
                            ))
                            .unwrap(),
                            7
                        );
                    }
                    "waterfall_async" => {
                        let _outer = ctx
                            .on(
                                &event,
                                Listener::around_async({
                                    let victim = victim.clone();
                                    move |_, (), next| {
                                        victim.borrow_mut().take().unwrap().remove().unwrap();
                                        next.call()
                                    }
                                }),
                            )
                            .unwrap();
                        *victim.borrow_mut() = Some(
                            ctx.on(
                                &event,
                                Listener::around_async({
                                    let calls = calls.clone();
                                    move |_, (), next| {
                                        calls.set(calls.get() + 1);
                                        next.call()
                                    }
                                }),
                            )
                            .unwrap(),
                        );
                        assert_eq!(
                            ctx.waterfall_async(
                                &event,
                                (),
                                Options::filtered(|_| true),
                                || async { Ok::<_, Infallible>(7) }
                            )
                            .unwrap()
                            .await
                            .unwrap(),
                            7
                        );
                    }
                    _ => {
                        let _first = ctx
                            .on(
                                &event,
                                Listener::sync({
                                    let victim = victim.clone();
                                    move |_, ()| {
                                        victim.borrow_mut().take().unwrap().remove().unwrap();
                                        continued()
                                    }
                                }),
                            )
                            .unwrap();
                        *victim.borrow_mut() = Some(
                            ctx.on(
                                &event,
                                Listener::sync({
                                    let calls = calls.clone();
                                    move |_, ()| {
                                        calls.set(calls.get() + 1);
                                        continued()
                                    }
                                }),
                            )
                            .unwrap(),
                        );
                        match mode {
                            "serial" => {
                                let _ = ctx
                                    .serial(&event, (), Options::default())
                                    .unwrap()
                                    .await
                                    .unwrap();
                            }
                            "parallel" => ctx
                                .parallel(&event, (), Options::default())
                                .unwrap()
                                .await
                                .unwrap(),
                            _ => {
                                let _ = ctx.bail(&event, (), Options::default()).unwrap();
                            }
                        }
                    }
                }
                assert_eq!(calls.get(), 1, "selected callback was lost in {mode}");
                ctx.drain().unwrap().await.unwrap();
            }
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn host_destruction_cancels_dispatch_and_setup_without_context_reference_cycles() {
    bounded(async {
        let runtime = Runtime::new();
        let ctx = runtime.context();
        let probe = Rc::new(());
        let weak = Rc::downgrade(&probe);
        let derived = ctx.extend(probe);
        let (event_entered_tx, event_entered_rx) = oneshot::channel();
        let (event_release_tx, event_release_rx) = oneshot::channel::<()>();
        let event_gate = RefCell::new(Some(event_release_rx));
        let event_entered = RefCell::new(Some(event_entered_tx));
        let (setup_entered_tx, setup_entered_rx) = oneshot::channel();
        let (setup_release_tx, setup_release_rx) = oneshot::channel::<()>();
        let mut dispatch = None;
        let mut setup = None;
        runtime
            .run(async {
                let event = Event::<()>::symbol("host cancellation");
                let captured = derived.clone();
                let _reg = derived
                    .on(
                        &event,
                        Listener::asynchronous(move |_, ()| {
                            assert!(captured.metadata::<Rc<()>>().is_some());
                            let gate = event_gate.borrow_mut().take().unwrap();
                            let entered = event_entered.borrow_mut().take().unwrap();
                            async move {
                                entered.send(()).unwrap();
                                gate.await.unwrap();
                                continued()
                            }
                        }),
                    )
                    .unwrap();
                dispatch = Some(ctx.emit(&event, (), Options::default()).unwrap());
                setup = Some(
                    ctx.effect_async(move || async move {
                        setup_entered_tx.send(()).unwrap();
                        setup_release_rx.await.unwrap();
                        Ok::<_, Infallible>(cordis_core::Disposer::new(|| async {
                            Ok::<_, Infallible>(())
                        }))
                    })
                    .unwrap(),
                );
                event_entered_rx.await.unwrap();
                setup_entered_rx.await.unwrap();
            })
            .await
            .unwrap();
        drop(derived);
        assert!(weak.upgrade().is_some());
        drop(runtime);
        assert!(matches!(
            dispatch.take().unwrap().await,
            Err(EventError::HostStopped)
        ));
        assert!(matches!(
            setup.take().unwrap().ready().await,
            Err(cordis_core::SetupError::HostStopped)
        ));
        assert!(weak.upgrade().is_none());
        assert!(event_release_tx.send(()).is_err());
        assert!(setup_release_tx.send(()).is_err());
    })
    .await;
}
