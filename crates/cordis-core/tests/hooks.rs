//! Typed internal protocol coverage without JavaScript reflection or callback coercion.
use cordis_core::{
    BoxError, DispatchOptions, Event, FiberState, Listener, ListenerOptions, Plugin, Runtime,
    ServiceKey, hooks,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};

#[tokio::test(flavor = "current_thread")]
async fn status_and_plugin_snapshots_do_not_retain_fibers_and_terminal_sequences_are_stable() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let transitions = Rc::new(RefCell::new(Vec::new()));
            let plugins = Rc::new(RefCell::new(Vec::new()));
            root.on(
                &hooks::status(),
                Listener::sync({
                    let transitions = transitions.clone();
                    move |_, change: hooks::Status| {
                        transitions.borrow_mut().push(change);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }
                }),
            )
            .unwrap();
            root.on(
                &hooks::plugin(),
                Listener::sync({
                    let plugins = plugins.clone();
                    move |_, info| {
                        plugins.borrow_mut().push(info);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }
                }),
            )
            .unwrap();
            let pending = root
                .inject([ServiceKey::<()>::new("missing").required()], |_| async {
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            pending.dispose().unwrap().await.unwrap();
            let observed: Vec<_> = transitions
                .borrow()
                .iter()
                .filter(|s| s.fiber.id == *pending.id())
                .map(|s| (s.previous, s.fiber.state))
                .collect();
            assert_eq!(observed, [(FiberState::Pending, FiberState::Disposed)]);
            let failed = root
                .plugin(
                    &Plugin::new("failed", |_, ()| async {
                        Err::<(), _>(std::io::Error::other("failed"))
                    }),
                    (),
                )
                .unwrap();
            assert!(failed.ready().unwrap().await.is_err());
            transitions.borrow_mut().clear();
            failed.dispose().unwrap().await.unwrap();
            assert_eq!(
                transitions
                    .borrow()
                    .iter()
                    .map(|s| (s.previous, s.fiber.state))
                    .collect::<Vec<_>>(),
                [(FiberState::Failed, FiberState::Disposed)]
            );
            let active = root
                .plugin(
                    &Plugin::new("active", |_, ()| async { Ok::<_, Infallible>(()) }),
                    (),
                )
                .unwrap();
            active.ready().unwrap().await.unwrap();
            transitions.borrow_mut().clear();
            active.dispose().unwrap().await.unwrap();
            assert_eq!(
                transitions
                    .borrow()
                    .iter()
                    .map(|s| (s.previous, s.fiber.state))
                    .collect::<Vec<_>>(),
                [
                    (FiberState::Active, FiberState::Unloading),
                    (FiberState::Unloading, FiberState::Disposed)
                ]
            );
            assert_eq!(plugins.borrow().len(), 6);
            assert!(plugins.borrow().iter().step_by(2).all(|p| p.live));
            assert!(plugins.borrow().iter().skip(1).step_by(2).all(|p| !p.live));
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn listener_interception_replaces_the_exact_typed_callback_without_recursion() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let calls = Rc::new(Cell::new(0));
            root.on(
                &hooks::listener(),
                Listener::sync({
                    let calls = calls.clone();
                    move |_, request: hooks::ListenerRequest| {
                        if !request.event().is::<Event<u32>>() {
                            return Ok::<_, BoxError>(ControlFlow::Continue(()));
                        }
                        let calls = calls.clone();
                        let replacement = request.replace(Listener::sync(move |_, arg: u32| {
                            calls.set(calls.get() + arg);
                            Ok::<_, Infallible>(ControlFlow::<()>::Continue(()))
                        }))?;
                        Ok(ControlFlow::Break(replacement))
                    }
                }),
            )
            .unwrap();
            let event = Event::<u32>::symbol("replaced");
            let token = root
                .on(
                    &event,
                    Listener::sync(|_, _| -> Result<_, Infallible> {
                        panic!("original listener ran")
                    }),
                )
                .unwrap();
            root.emit(&event, 3, DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 3);
            token.remove().unwrap();
            root.emit(&event, 4, DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(calls.get(), 3);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_observation_has_typed_args_and_omits_all_internal_protocol_events() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let modes = Rc::new(RefCell::new(Vec::new()));
            root.on(
                &hooks::dispatch(),
                Listener::sync({
                    let modes = modes.clone();
                    move |_, dispatch: hooks::Dispatch| {
                        assert_eq!(*dispatch.arguments.get::<u32>()?, 7);
                        modes.borrow_mut().push(dispatch.mode);
                        Ok::<_, BoxError>(ControlFlow::Continue(()))
                    }
                }),
            )
            .unwrap();
            let event = Event::<u32>::symbol("event");
            root.on(
                &event,
                Listener::sync(|_, _| Ok::<_, Infallible>(ControlFlow::Continue(()))),
            )
            .unwrap();
            root.emit(&event, 7, DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(
                root.bail(&event, 7, DispatchOptions::default()).unwrap(),
                ControlFlow::Continue(())
            );
            assert_eq!(
                root.serial(&event, 7, DispatchOptions::default())
                    .unwrap()
                    .await
                    .unwrap(),
                ControlFlow::Continue(())
            );
            root.parallel(&event, 7, DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            root.waterfall(
                &Event::<u32, u32>::symbol("sync around"),
                7,
                DispatchOptions::default(),
                || Ok::<_, Infallible>(0),
            )
            .unwrap();
            root.waterfall_async(
                &Event::<u32, u32>::symbol("async around"),
                7,
                DispatchOptions::default(),
                || async { Ok::<_, Infallible>(0) },
            )
            .unwrap()
            .await
            .unwrap();
            assert_eq!(
                *modes.borrow(),
                [
                    "emit",
                    "bail",
                    "serial",
                    "parallel",
                    "waterfall",
                    "waterfall"
                ]
            );
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn get_set_and_realm_service_hooks_preserve_contracts_and_set_does_not_notify() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<u32>::new("foo");
            let isolated = root.isolate(&key);
            let changes = Rc::new(RefCell::new(Vec::new()));
            let global = Rc::new(Cell::new(0));
            isolated
                .on(
                    &hooks::service(),
                    Listener::sync({
                        let changes = changes.clone();
                        move |_, change: hooks::ServiceChange| {
                            changes.borrow_mut().push(change);
                            Ok::<_, Infallible>(ControlFlow::Continue(()))
                        }
                    }),
                )
                .unwrap();
            root.on_with(
                &hooks::service(),
                Listener::sync({
                    let global = global.clone();
                    move |_, _| {
                        global.set(global.get() + 1);
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }
                }),
                ListenerOptions {
                    global: true,
                    ..Default::default()
                },
            )
            .unwrap();
            let default = root.provide(&key, 0).unwrap();
            let publication = isolated.provide(&key, 1).unwrap();
            assert_eq!(changes.borrow().len(), 1);
            assert_eq!(global.get(), 2);
            isolated.set(&key, 2).unwrap();
            assert_eq!(global.get(), 2);
            let provider = root
                .plugin(
                    &Plugin::new("", |_, ()| async { Ok::<_, Infallible>(()) }),
                    (),
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            let other = ServiceKey::new("other");
            provider.context().provide(&other, 5_u32).unwrap();
            let consumer = root
                .inject([other.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            root.on(
                &hooks::get(),
                Listener::around(
                    |_, request: hooks::Get, next: cordis_core::Next<Option<hooks::Value>>| {
                        let value = next.call()?.unwrap().get::<u32>()?;
                        assert_eq!(request.name, "other");
                        Ok::<_, BoxError>(Some(hooks::Value::new(*value + 1)))
                    },
                ),
            )
            .unwrap();
            assert_eq!(*consumer.context().get(&other).unwrap().unwrap(), 6);
            root.on(
                &hooks::set(),
                Listener::around(|_, _, _| Ok::<_, Infallible>(())),
            )
            .unwrap();
            provider.context().set(&other, 20).unwrap();
            assert_eq!(*root.get_direct(&other).unwrap().unwrap(), 5);
            publication.withdraw().unwrap().await.unwrap();
            assert!(changes.borrow().last().unwrap().value.is_none());
            default.withdraw().unwrap().await.unwrap();
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}
