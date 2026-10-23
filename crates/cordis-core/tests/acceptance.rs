//! M6 end-to-end and explicit representation acceptance against remaining pinned cases.
use cordis_core::{
    Accessor, AccessorKey, BoxError, Context, DispatchOptions, Disposer, Event, FiberState,
    Listener, Plugin, Runtime, ServiceError, ServiceKey, hooks,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

#[path = "../examples/lifecycle.rs"]
mod lifecycle;

#[tokio::test(flavor = "current_thread")]
async fn service_filters_retain_only_identity_and_use_each_listener_runtime_after_origin_drop() {
    let origin = Runtime::new();
    let key = ServiceKey::<()>::new("shared");
    let (options, label) = origin
        .run(async {
            let ctx = origin.context();
            (
                ctx.service_filter(&key).unwrap(),
                ctx.isolation(&key).unwrap(),
            )
        })
        .await
        .unwrap();
    drop(origin);
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let event = Event::<()>::symbol("filtered");
            let counts = Rc::new(RefCell::new([0_u32; 3]));
            for (index, ctx, global) in [
                (0, root.clone(), false),
                (1, root.isolate_with(&key, &label), false),
                (2, root.clone(), true),
            ] {
                let counts = counts.clone();
                ctx.on_with(
                    &event,
                    Listener::sync(move |_, ()| {
                        counts.borrow_mut()[index] += 1;
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }),
                    cordis_core::ListenerOptions {
                        global,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            root.emit(&event, (), options).unwrap().await.unwrap();
            assert_eq!(*counts.borrow(), [0, 1, 1]);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

struct Caller;
impl cordis_core::ServiceInvoke<()> for Caller {
    type Output = cordis_core::FiberId;
    fn invoke(&self, ctx: Context, (): ()) -> Result<Self::Output, BoxError> {
        Ok(ctx.caller_site().fiber_id().clone())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn callable_adapters_expose_the_accessing_definition_not_the_outermost_consumer() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let callable = ServiceKey::<Caller>::new("callable");
            let outer = ServiceKey::<()>::new("outer");
            root.plugin(
                &Plugin::new("callable", {
                    let callable = callable.clone();
                    move |ctx, ()| {
                        let callable = callable.clone();
                        async move {
                            ctx.provide(&callable, Caller)?;
                            Ok::<_, BoxError>(())
                        }
                    }
                }),
                (),
            )
            .unwrap()
            .ready()
            .unwrap()
            .await
            .unwrap();
            let provider = root
                .plugin(
                    &Plugin::new("outer", {
                        let outer = outer.clone();
                        move |ctx, ()| {
                            let outer = outer.clone();
                            async move {
                                ctx.provide(&outer, ())?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .injecting([callable.required()]),
                    (),
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            let consumer = root
                .inject([outer.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            for access in [root.clone(), consumer.context()] {
                let caller = access
                    .bind(&outer)
                    .unwrap()
                    .call(|_, ctx| ctx.bind(&callable).unwrap().invoke(()).unwrap());
                assert_eq!(&caller, provider.id());
                assert_ne!(&caller, consumer.id());
            }
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn optional_publication_presence_and_shared_updated_handles_are_explicit_typed_contracts() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let key = ServiceKey::<Option<u32>>::new("optional");
    runtime
        .run(async {
            let publication = root.provide(&key, None).unwrap();
            let calls = Rc::new(Cell::new(0));
            let consumer = root
                .inject([key.required()], {
                    let calls = calls.clone();
                    let key = key.clone();
                    move |ctx| {
                        let calls = calls.clone();
                        let key = key.clone();
                        async move {
                            assert!(ctx.get(&key)?.unwrap().is_none());
                            calls.set(calls.get() + 1);
                            Ok::<_, BoxError>(())
                        }
                    }
                })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            root.set(&key, Some(0)).unwrap();
            assert_eq!(*consumer.context().get(&key).unwrap().unwrap(), Some(0));
            assert_eq!(calls.get(), 1);
            publication.withdraw().unwrap().await.unwrap();
            let fiber = root
                .plugin(
                    &Plugin::new("shared", |_, _: u32| async { Ok::<_, Infallible>(()) }),
                    0,
                )
                .unwrap();
            let alias = fiber.clone();
            fiber.ready().unwrap().await.unwrap();
            alias.update(5).unwrap().await.unwrap();
            assert_eq!(*fiber.config(), 5);
            assert_eq!(*alias.config(), 5);
            assert_eq!(fiber.id(), alias.id());
            let wrong_contract = fiber
                .context()
                .on_update_sync(|_: Rc<String>| Ok::<_, Infallible>(true))
                .unwrap();
            assert!(fiber.update(6).unwrap().await.is_err());
            assert_eq!(
                *fiber.config(),
                6,
                "middleware failure follows source commit"
            );
            assert_eq!(fiber.state(), FiberState::Active);
            assert_eq!(runtime.take_failures().len(), 1);
            wrong_contract.remove().unwrap();
            alias.update(7).unwrap().await.unwrap();
            assert_eq!(*fiber.config(), 7);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
    drop(runtime);
    assert!(matches!(
        root.bind(&key),
        Err(ServiceError::Admission(
            cordis_core::AdmissionError::RuntimeDropped
        ))
    ));
}

async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("acceptance scenario stalled");
}

#[test]
fn executable_example_proves_isolated_reconfigure_and_final_read_before_provider_close() {
    lifecycle::run().unwrap();
}

struct Method {
    calls: Cell<u32>,
    stops: Cell<u32>,
}
impl Method {
    async fn run(self: Rc<Self>, ctx: Context) -> Result<(), BoxError> {
        self.calls.set(self.calls.get() + 1);
        ctx.clone().on_dispose_sync(move || {
            assert!(ctx.get(&ServiceKey::<u32>::new("foo"))?.is_some());
            self.stops.set(self.stops.get() + 1);
            Ok::<_, BoxError>(())
        })?;
        Ok(())
    }
}
#[tokio::test(flavor = "current_thread")]
async fn method_injection_owns_cleanup_and_reloads_without_macros_or_constructor_reflection() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let foo = ServiceKey::<u32>::new("foo");
                let bar = ServiceKey::<Rc<Method>>::new("bar");
                let target = Rc::new(Method {
                    calls: Cell::new(0),
                    stops: Cell::new(0),
                });
                let method_child = Rc::new(RefCell::new(None));
                let owner = root
                    .plugin(
                        &Plugin::initialized("Bar", {
                            let target = target.clone();
                            let foo = foo.clone();
                            let bar = bar.clone();
                            let method_child = method_child.clone();
                            move |ctx, ()| {
                                let target = target.clone();
                                let foo = foo.clone();
                                let bar = bar.clone();
                                let method_child = method_child.clone();
                                async move {
                                    ctx.provide(&bar, target.clone())?;
                                    *method_child.borrow_mut() = Some(ctx.inject_method(
                                        [foo.required()],
                                        target,
                                        Method::run,
                                    )?);
                                    Ok::<_, BoxError>(Disposer::sync(|| Ok::<_, Infallible>(())))
                                }
                            }
                        }),
                        (),
                    )
                    .unwrap();
                owner.ready().unwrap().await.unwrap();
                let child = method_child.borrow_mut().take().unwrap();
                assert_eq!(target.calls.get(), 0);
                let first = root.provide(&foo, 1).unwrap();
                child.ready().unwrap().await.unwrap();
                assert_eq!(target.calls.get(), 1);
                first.withdraw().unwrap().await.unwrap();
                assert_eq!(target.stops.get(), 1);
                let second = root.provide(&foo, 2).unwrap();
                child.ready().unwrap().await.unwrap();
                assert_eq!(target.calls.get(), 2);
                second.withdraw().unwrap().await.unwrap();
                owner.dispose().unwrap().await.unwrap();
                assert_eq!(target.calls.get(), target.stops.get());
                assert_eq!(child.state(), FiberState::Disposed);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_initialization_is_an_availability_barrier_and_failure_is_diagnosed_once() {
    for fail in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            runtime
                .run(async {
                    let root = runtime.context();
                    let key = ServiceKey::<u32>::new("foo");
                    let (started, entered) = oneshot::channel();
                    let (release, gate) = oneshot::channel();
                    let gates = RefCell::new(Some((started, gate)));
                    let closed = Rc::new(Cell::new(false));
                    let consumer = root
                        .inject([key.required()], |_| async { Ok::<_, Infallible>(()) })
                        .unwrap();
                    let provider = root
                        .plugin(
                            &Plugin::initialized("FooInit", {
                                let key = key.clone();
                                let closed = closed.clone();
                                move |ctx, ()| {
                                    let key = key.clone();
                                    let closed = closed.clone();
                                    let (started, gate) = gates.borrow_mut().take().unwrap();
                                    async move {
                                        ctx.provide(&key, 7)?;
                                        ctx.on_dispose_sync({
                                            let closed = closed.clone();
                                            move || {
                                                closed.set(true);
                                                Ok::<_, Infallible>(())
                                            }
                                        })?;
                                        ctx.logger(None).info("init")?;
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                        if fail {
                                            return Err(std::io::Error::other(
                                                "initialization failed",
                                            )
                                            .into());
                                        }
                                        Ok::<_, BoxError>(Disposer::sync(
                                            || Ok::<_, Infallible>(()),
                                        ))
                                    }
                                }
                            }),
                            (),
                        )
                        .unwrap();
                    entered.await.unwrap();
                    assert_eq!(consumer.state(), FiberState::Pending);
                    assert!(root.get_direct(&key).unwrap().is_none());
                    release.send(()).unwrap();
                    let result = provider.ready().unwrap().await;
                    if fail {
                        assert!(result.is_err());
                        assert!(closed.get());
                        assert_eq!(consumer.state(), FiberState::Pending);
                        assert_eq!(runtime.take_failures().len(), 1);
                    } else {
                        result.unwrap();
                        consumer.ready().unwrap().await.unwrap();
                        assert_eq!(consumer.state(), FiberState::Active);
                        provider.dispose().unwrap().await.unwrap();
                        assert!(closed.get());
                    }
                    assert!(
                        root.log_buffer()
                            .unwrap()
                            .iter()
                            .any(|r| r.name == "foo-init" && r.message == "init")
                    );
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn injection_inertia_preserves_loading_and_unloading_until_gated_work_finishes() {
    for mode in 0..3 {
        bounded(async {
            let runtime = Runtime::new();
            runtime
                .run(async {
                    let root = runtime.context();
                    let key = ServiceKey::<u32>::new("foo");
                    let provider = root
                        .plugin(
                            &Plugin::new("provider", |_, ()| async { Ok::<_, Infallible>(()) }),
                            (),
                        )
                        .unwrap();
                    provider.ready().unwrap().await.unwrap();
                    let old = provider.context().provide(&key, 1).unwrap();
                    let (initial_started, initial_entered) = oneshot::channel();
                    let (initial_release, initial_gate) = oneshot::channel();
                    let (cleanup_started, cleanup_entered) = oneshot::channel();
                    let (cleanup_release, cleanup_gate) = oneshot::channel();
                    let (latest_started, latest_entered) = oneshot::channel();
                    let (latest_release, latest_gate) = oneshot::channel();
                    let initial = RefCell::new(Some((initial_started, initial_gate)));
                    let cleanup = RefCell::new(Some((cleanup_started, cleanup_gate)));
                    let latest = RefCell::new(Some((latest_started, latest_gate)));
                    let consumer = root
                        .inject([key.required()], {
                            let key = key.clone();
                            move |ctx| {
                                let first = initial.borrow_mut().take();
                                let first_load = first.is_some();
                                let setup = if first_load {
                                    first
                                } else {
                                    latest.borrow_mut().take()
                                };
                                let cleanup = if first_load {
                                    cleanup.borrow_mut().take()
                                } else {
                                    None
                                };
                                let key = key.clone();
                                async move {
                                    let version = *ctx.get(&key)?.unwrap();
                                    let final_ctx = ctx.clone();
                                    let final_key = key.clone();
                                    ctx.on_dispose(move || async move {
                                        if let Some((started, gate)) = cleanup {
                                            started.send(()).unwrap();
                                            gate.await.unwrap();
                                        }
                                        assert_eq!(*final_ctx.get(&final_key)?.unwrap(), version);
                                        Ok::<_, BoxError>(())
                                    })?;
                                    if let Some((started, gate)) = setup {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                    }
                                    assert_eq!(*ctx.get(&key)?.unwrap(), version);
                                    Ok::<_, BoxError>(())
                                }
                            }
                        })
                        .unwrap();
                    initial_entered.await.unwrap();
                    assert_eq!(consumer.state(), FiberState::Loading);
                    let withdrawing: cordis_core::LocalFuture<()> = if mode == 2 {
                        initial_release.send(()).unwrap();
                        consumer.ready().unwrap().await.unwrap();
                        assert_eq!(consumer.state(), FiberState::Active);
                        let task = provider.dispose().unwrap();
                        Box::pin(async move {
                            task.await.unwrap();
                        })
                    } else {
                        let task = old.withdraw().unwrap();
                        assert_eq!(consumer.state(), FiberState::Loading);
                        if mode == 1 {
                            provider.context().provide(&key, 2).unwrap();
                        }
                        initial_release.send(()).unwrap();
                        Box::pin(async move {
                            task.await.unwrap();
                        })
                    };
                    cleanup_entered.await.unwrap();
                    assert_eq!(consumer.state(), FiberState::Unloading);
                    if mode == 0 {
                        provider.context().provide(&key, 2).unwrap();
                    }
                    cleanup_release.send(()).unwrap();
                    if mode != 2 {
                        latest_entered.await.unwrap();
                        assert_eq!(consumer.state(), FiberState::Loading);
                        withdrawing.await;
                        latest_release.send(()).unwrap();
                        consumer.ready().unwrap().await.unwrap();
                        assert_eq!(consumer.state(), FiberState::Active);
                        assert_eq!(*consumer.context().get(&key).unwrap().unwrap(), 2);
                    } else {
                        withdrawing.await;
                        assert_eq!(
                            consumer.ready().unwrap().await.unwrap(),
                            FiberState::Pending
                        );
                    }
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}

struct Base {
    qux: Cell<u32>,
}
struct Session {
    secret: Cell<u32>,
}
#[tokio::test(flavor = "current_thread")]
async fn associated_service_and_accessor_adapters_use_original_authority_and_receiver_identity() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let foo = ServiceKey::<Base>::new("foo");
            let bar = ServiceKey::<()>::new("bar");
            let answer = ServiceKey::<u32>::new("foo.answer");
            let extension = ServiceKey::<u32>::new("session.extension");
            let property_service = ServiceKey::<u32>::new("foo.property");
            root.provide(&foo, Base { qux: Cell::new(1) }).unwrap();
            root.provide(&property_service, 3).unwrap();
            let provider = root
                .plugin(
                    &Plugin::new("extensions", |_, ()| async { Ok::<_, Infallible>(()) }),
                    (),
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            provider.context().provide(&bar, ()).unwrap();
            provider.context().provide(&answer, 42).unwrap();
            provider.context().provide(&extension, 9).unwrap();
            let consumer = root
                .inject([foo.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            let base = consumer.context().bind(&foo).unwrap();
            base.call(|value, _| value.qux.set(2));
            assert_eq!(base.call(|value, _| value.qux.get()), 2);
            assert!(matches!(
                consumer.context().get(&ServiceKey::<u32>::new("foo.qux")),
                Err(ServiceError::Undeclared { .. })
            ));
            assert!(base.associated::<u32>("answer").is_err());
            assert_eq!(
                base.associated::<u32>("property").unwrap().call(|v, _| *v),
                3
            );
            root.set(&property_service, 4).unwrap();
            assert_eq!(
                base.associated::<u32>("property").unwrap().call(|v, _| *v),
                4
            );
            let getter = AccessorKey::<Base, Rc<Base>>::new("foo.self");
            root.accessor(
                &getter,
                Accessor::new(|_, receiver: Rc<Base>| Ok::<_, Infallible>(receiver)),
            )
            .unwrap();
            let same = base.read_accessor(&getter).unwrap();
            assert!(Rc::ptr_eq(&same, &root.get(&foo).unwrap().unwrap()));
            assert!(
                root.get(&ServiceKey::<Rc<Base>>::new("foo.self"))
                    .unwrap()
                    .is_none()
            );
            let property = AccessorKey::<Session, u32>::new("session.answer");
            root.accessor(
                &property,
                Accessor::from_service(bar.clone(), |_, _, receiver: Rc<Session>| {
                    Ok::<_, Infallible>(receiver.secret.get())
                })
                .with_setter({
                    let bar = bar.clone();
                    move |ctx, receiver, value| {
                        ctx.bind(&bar)?;
                        receiver.secret.set(value + 1);
                        Ok::<_, BoxError>(())
                    }
                }),
            )
            .unwrap();
            let session = base.map(|_| Session {
                secret: Cell::new(0),
            });
            assert!(session.read_accessor(&property).is_err());
            assert!(session.associated_key(&extension).is_err());
            let child = consumer
                .context()
                .inject(
                    [bar.required(), answer.required(), extension.required()],
                    |_| async { Ok::<_, Infallible>(()) },
                )
                .unwrap();
            child.ready().unwrap().await.unwrap();
            let associated = session.rebind(&child.context()).unwrap();
            associated.write_accessor(&property, 100).unwrap();
            assert_eq!(associated.read_accessor(&property).unwrap(), 101);
            assert_eq!(
                associated
                    .associated::<u32>("answer")
                    .unwrap()
                    .call(|value, _| *value),
                42
            );
            assert!(
                base.write_accessor(&getter, same).is_err(),
                "readonly accessors reject writes"
            );
            assert_eq!(
                associated
                    .associated_key(&extension)
                    .unwrap()
                    .call(|value, _| *value),
                9
            );
            provider.dispose().unwrap().await.unwrap();
            assert!(root.get_direct(&answer).unwrap().is_none());
            assert!(root.get_direct(&foo).unwrap().is_some());
            assert!(
                root.log_buffer()
                    .unwrap()
                    .iter()
                    .all(|record| record.level != cordis_core::Level::Warn
                        && record.level != cordis_core::Level::Error)
            );
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_bound_effects_keep_root_provisions_accessible_with_and_without_injection() {
    for injected in [false, true] {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let counter = ServiceKey::<Cell<u32>>::new("counter");
                let foo = ServiceKey::<()>::new("foo");
                root.provide(&counter, Cell::new(0)).unwrap();
                let plugin = Plugin::initialized("Foo", {
                    let foo = foo.clone();
                    move |ctx, ()| {
                        let foo = foo.clone();
                        async move {
                            ctx.provide(&foo, ())?;
                            Ok::<_, BoxError>(Disposer::sync(|| Ok::<_, Infallible>(())))
                        }
                    }
                })
                .injecting(if injected {
                    vec![counter.required()]
                } else {
                    vec![]
                });
                root.plugin(&plugin, ())
                    .unwrap()
                    .ready()
                    .unwrap()
                    .await
                    .unwrap();
                let increase = || {
                    root.bind(&foo).unwrap().call(|_, ctx| {
                        let value = ctx.get(&counter).unwrap().unwrap();
                        value.set(value.get() + 1);
                    })
                };
                increase();
                let consumer = root
                    .inject([foo.required()], |_| async { Ok::<_, Infallible>(()) })
                    .unwrap();
                consumer.ready().unwrap().await.unwrap();
                increase();
                consumer.dispose().unwrap().await.unwrap();
                increase();
                assert_eq!(root.get(&counter).unwrap().unwrap().get(), 3);
                assert!(runtime.take_failures().is_empty());
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn typed_rooted_values_dynamic_events_and_context_type_guards_need_no_proxy_reflection() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<()>::new("inner");
            let provider = root
                .plugin(
                    &Plugin::new("provider", {
                        let key = key.clone();
                        move |ctx, ()| {
                            let key = key.clone();
                            async move {
                                ctx.provide(&key, ())?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    }),
                    (),
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            let rooted = ServiceKey::<Context>::new("rooted");
            root.provide(&rooted, root.clone()).unwrap();
            root.bind(&rooted)
                .unwrap()
                .call(|definition, _| assert!(definition.get(&key).unwrap().is_some()));
            let erased: Box<dyn std::any::Any> = Box::new(root.clone());
            assert!(erased.is::<Context>());
            assert!(!erased.is::<String>());
            #[derive(Debug)]
            struct X;
            assert_eq!(
                root.bind(&rooted).unwrap().call(|_, _| format!("{:?}", X)),
                "X"
            );
            let event = Event::<Vec<hooks::Value>>::symbol("dynamic");
            let count = Rc::new(Cell::new(0));
            root.on(
                &event,
                Listener::sync({
                    let count = count.clone();
                    move |_, values: Vec<hooks::Value>| {
                        assert_eq!(&*values[0].get::<String>()?, "dynamic");
                        count.set(*values[1].get::<u32>()?);
                        Ok::<_, BoxError>(ControlFlow::Continue(()))
                    }
                }),
            )
            .unwrap();
            root.emit(
                &event,
                vec![
                    hooks::Value::new("dynamic".to_string()),
                    hooks::Value::new(1_u32),
                ],
                DispatchOptions::default(),
            )
            .unwrap()
            .await
            .unwrap();
            assert_eq!(count.get(), 1);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_initialized_services_restore_registry_effect_and_listener_baselines() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<()>::new("foo");
            let event = Event::<()>::symbol("owned");
            let calls = Rc::new(Cell::new(0));
            let plugin = Plugin::initialized("Foo", {
                let key = key.clone();
                let event = event.clone();
                let calls = calls.clone();
                move |ctx, ()| {
                    let key = key.clone();
                    let event = event.clone();
                    let calls = calls.clone();
                    async move {
                        ctx.provide(&key, ())?;
                        ctx.on(
                            &event,
                            Listener::sync(move |_, ()| {
                                calls.set(calls.get() + 1);
                                Ok::<_, Infallible>(ControlFlow::Continue(()))
                            }),
                        )?;
                        ctx.inject([key.required()], |_| async { Ok::<_, Infallible>(()) })?;
                        Ok::<_, BoxError>(Disposer::sync(|| Ok::<_, Infallible>(())))
                    }
                }
            });
            for iteration in 1..=10 {
                let fiber = root.plugin(&plugin, ()).unwrap();
                fiber.ready().unwrap().await.unwrap();
                root.emit(&event, (), DispatchOptions::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(calls.get(), iteration);
                root.registry()
                    .remove(&plugin)
                    .unwrap()
                    .pop()
                    .unwrap()
                    .await
                    .unwrap();
                assert!(root.registry().is_empty().unwrap());
                assert!(root.get_effects().unwrap().is_empty());
                assert!(root.get_direct(&key).unwrap().is_none());
                root.emit(&event, (), DispatchOptions::default())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(calls.get(), iteration);
            }
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_status_observers_are_diagnosed_without_stranding_owned_cleanup() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let closed = Rc::new(Cell::new(false));
            root.on(
                &hooks::status(),
                Listener::sync(|_, _| {
                    Err::<ControlFlow<()>, _>(std::io::Error::other("observer failed"))
                }),
            )
            .unwrap();
            let fiber = root
                .plugin(
                    &Plugin::new("observed", {
                        let closed = closed.clone();
                        move |ctx, ()| {
                            let closed = closed.clone();
                            async move {
                                ctx.on_dispose_sync(move || {
                                    closed.set(true);
                                    Ok::<_, Infallible>(())
                                })?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    }),
                    (),
                )
                .unwrap();
            fiber.ready().unwrap().await.unwrap();
            fiber.dispose().unwrap().await.unwrap();
            assert!(closed.get());
            assert_eq!(runtime.take_failures().len(), 4);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}
