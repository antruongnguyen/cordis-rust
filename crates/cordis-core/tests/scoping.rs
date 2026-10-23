//! Explicit Rust equivalents of isolate/intercept/shadow/associate/invoke specs.
use cordis_core::{
    Accessor, AccessorKey, BoxError, Context, DispatchOptions, Event, FiberState, IsolationId,
    Listener, ListenerOptions, Plugin, ReflectError, Runtime, ServiceError, ServiceInvoke,
    ServiceKey,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    ops::ControlFlow,
    rc::Rc,
};
use tokio::sync::oneshot;

fn empty() -> Plugin<()> {
    Plugin::new("", |_, ()| async { Ok::<_, Infallible>(()) })
}
async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("scoping stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_and_same_label_boundaries_stop_inherited_authority_without_parent_fallback() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::new("foo");
            root.provide(&key, 1_u32).unwrap();
            let label = root.isolation(&key).unwrap();
            let consumer = root
                .inject([key.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            let ctx = consumer.context();
            let child = ctx.plugin(&empty(), ()).unwrap();
            child.ready().unwrap().await.unwrap();
            assert_eq!(*child.context().get(&key).unwrap().unwrap(), 1);
            for isolated in [ctx.isolate(&key), ctx.isolate_with(&key, &label)] {
                assert!(matches!(
                    isolated.get(&key),
                    Err(ServiceError::Undeclared { .. })
                ));
                let child = isolated.plugin(&empty(), ()).unwrap();
                child.ready().unwrap().await.unwrap();
                assert!(matches!(
                    child.context().get(&key),
                    Err(ServiceError::Undeclared { .. })
                ));
            }
            let isolated = root.isolate(&key);
            assert!(isolated.get_direct(&key).unwrap().is_none());
            isolated.provide(&key, 2).unwrap();
            assert_eq!(*root.get(&key).unwrap().unwrap(), 1);
            assert_eq!(*isolated.get(&key).unwrap().unwrap(), 2);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn shared_labels_and_cross_name_alias_contracts_use_the_slot_not_the_value_type() {
    bounded(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let foo = ServiceKey::<u32>::new("foo");
                let alias = ServiceKey::<u32>::new("alias");
                let wrong = ServiceKey::<String>::new("wrong");
                let slot = IsolationId::new();
                let first = root.isolate_with(&foo, &slot);
                let second = root.isolate_with(&alias, &slot);
                let consumer = second
                    .inject([alias.required()], |_| async { Ok::<_, Infallible>(()) })
                    .unwrap();
                let publication = first.provide(&foo, 3).unwrap();
                consumer.ready().unwrap().await.unwrap();
                assert_eq!(*consumer.context().get(&alias).unwrap().unwrap(), 3);
                assert_eq!(*second.get(&alias).unwrap().unwrap(), 3);
                assert!(matches!(
                    root.isolate_with(&wrong, &slot).get(&wrong),
                    Err(ServiceError::TypeMismatch { .. })
                ));
                assert!(matches!(
                    second.provide(&alias, 4),
                    Err(ServiceError::DuplicateProvider { .. })
                ));
                publication.withdraw().unwrap().await.unwrap();
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

#[tokio::test(flavor = "current_thread")]
async fn bound_nested_calls_keep_definition_authority_but_use_caller_realm_metadata_and_effects() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let foo = ServiceKey::<u32>::new("foo");
            let relay = ServiceKey::<()>::new("relay");
            let first = IsolationId::new();
            let second = IsolationId::new();
            let provider = root.plugin(&empty(), ()).unwrap();
            provider.ready().unwrap().await.unwrap();
            provider
                .context()
                .isolate_with(&foo, &first)
                .provide(&foo, 1)
                .unwrap();
            provider
                .context()
                .isolate_with(&foo, &second)
                .provide(&foo, 2)
                .unwrap();
            let relay_fiber = root
                .isolate_with(&foo, &first)
                .inject([foo.required().with_config(vec!["definition"])], {
                    let relay = relay.clone();
                    move |ctx| {
                        let relay = relay.clone();
                        async move {
                            ctx.provide(&relay, ())?;
                            Ok::<_, BoxError>(())
                        }
                    }
                })
                .unwrap();
            relay_fiber.ready().unwrap().await.unwrap();
            let usage = root
                .isolate_with(&foo, &second)
                .extend("caller")
                .intercept(&foo, vec!["usage"]);
            let bound = usage.bind(&relay).unwrap();
            let origin = bound.context().definition_site().fiber_id().clone();
            assert_eq!(origin, *relay_fiber.id());
            let (value, selected) = bound.call(|_, ctx| {
                assert_eq!(&*ctx.metadata::<&str>().unwrap(), &"caller");
                assert_eq!(ctx.fiber_id(), root.fiber_id());
                let inner = ctx.bind(&foo).unwrap();
                assert_eq!(inner.caller().fiber_id(), relay_fiber.id());
                assert_eq!(inner.context().caller_site().fiber_id(), relay_fiber.id());
                let configs = inner.intercepts::<Vec<&str>>().unwrap();
                assert_eq!(
                    configs
                        .iter()
                        .flat_map(|c| c.iter().copied())
                        .collect::<Vec<_>>(),
                    ["definition", "usage"]
                );
                inner.call_unshadowed(|_, ctx, caller| {
                    assert_eq!(caller.fiber_id(), relay_fiber.id());
                    assert_eq!(ctx.definition_site().fiber_id(), root.fiber_id());
                });
                inner.call(|_, nested| {
                    (
                        *nested.get(&foo).unwrap().unwrap(),
                        nested.isolation(&foo).unwrap(),
                    )
                })
            });
            assert_eq!(value, 1);
            assert_eq!(selected, second);
            let retraced = bound.map(|_| ()).rebind(&usage.extend(42_u32)).unwrap();
            assert_eq!(
                retraced.context().definition_site().fiber_id(),
                relay_fiber.id()
            );
            assert_eq!(*retraced.context().metadata::<u32>().unwrap(), 42);
            let spawned = bound.call(|_, ctx| ctx.plugin(&empty(), ()).unwrap());
            spawned.ready().unwrap().await.unwrap();
            assert_eq!(spawned.context().definition_site().fiber_id(), spawned.id());
            assert!(matches!(
                spawned.context().get(&foo),
                Err(ServiceError::Undeclared { .. })
            ));
            let cleaned = Rc::new(Cell::new(false));
            bound.call(|_, ctx| {
                let cleaned = cleaned.clone();
                ctx.on_dispose_sync(move || {
                    cleaned.set(true);
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            });
            relay_fiber.dispose().unwrap().await.unwrap();
            assert!(!cleaned.get(), "bound effects belong to the root use site");
            root.drain().unwrap().await.unwrap();
            assert!(cleaned.get());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn intercepts_merge_nearest_injection_then_outer_to_inner_and_own_provisions_do_not_borrow_it()
 {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<()>::new("foo");
            root.provide(&key, ()).unwrap();
            let consumer = root
                .intercept(&key, vec!["outer"])
                .inject(
                    [
                        key.required().with_config(vec!["overridden"]),
                        key.required().with_config(vec!["inject"]),
                    ],
                    |_| async { Ok::<_, Infallible>(()) },
                )
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            let ctx = consumer.context().intercept(&key, vec!["inner"]);
            let merge = |configs: Vec<Rc<Vec<&'static str>>>| {
                configs
                    .iter()
                    .flat_map(|c| c.iter().copied())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                ctx.bind(&key).unwrap().resolve_intercept(merge).unwrap(),
                ["inject", "outer", "inner"]
            );
            let inherited = ctx.plugin(&empty(), ()).unwrap();
            inherited.ready().unwrap().await.unwrap();
            assert_eq!(
                inherited
                    .context()
                    .bind(&key)
                    .unwrap()
                    .resolve_intercept(merge)
                    .unwrap(),
                ["inject", "outer", "inner"]
            );
            let nearest = ctx
                .inject([key.required().with_config(vec!["nearest"])], |_| async {
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            nearest.ready().unwrap().await.unwrap();
            assert_eq!(
                nearest
                    .context()
                    .bind(&key)
                    .unwrap()
                    .resolve_intercept(merge)
                    .unwrap(),
                ["nearest", "outer", "inner"]
            );
            let own = ctx.isolate(&key);
            own.provide(&key, ()).unwrap();
            assert_eq!(
                own.bind(&key).unwrap().resolve_intercept(merge).unwrap(),
                ["outer", "inner"]
            );
            assert!(own.bind(&key).unwrap().intercepts::<u32>().is_err());
            let direct = ctx
                .isolate_with(&key, &root.isolation(&key).unwrap())
                .bind_direct(&key)
                .unwrap();
            assert_eq!(direct.resolve_intercept(merge).unwrap(), ["outer", "inner"]);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn realm_filtered_events_still_admit_global_listeners() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let key = ServiceKey::<()>::new("foo");
            let isolated = root.isolate(&key);
            isolated.provide(&key, ()).unwrap();
            let event = Event::<()>::symbol("isolated");
            let counts = Rc::new(RefCell::new([0; 3]));
            for (index, ctx, global) in [
                (0, root.clone(), false),
                (1, isolated.clone(), false),
                (2, root.clone(), true),
            ] {
                let counts = counts.clone();
                ctx.on_with(
                    &event,
                    Listener::sync(move |_, ()| {
                        counts.borrow_mut()[index] += 1;
                        Ok::<_, Infallible>(ControlFlow::Continue(()))
                    }),
                    ListenerOptions {
                        global,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            let bound = isolated.bind(&key).unwrap();
            root.emit(&event, (), bound.dispatch_options().unwrap())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(*counts.borrow(), [0, 1, 1]);
            root.emit(&event, (), DispatchOptions::default())
                .unwrap()
                .await
                .unwrap();
            assert_eq!(*counts.borrow(), [1, 2, 2]);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

struct Callable;
impl ServiceInvoke<u32> for Callable {
    type Output = u32;
    fn invoke(&self, ctx: Context, add: u32) -> Result<u32, BoxError> {
        Ok(*ctx.get(&ServiceKey::<u32>::new("dependency"))?.unwrap() + add)
    }
}
#[tokio::test(flavor = "current_thread")]
async fn explicit_accessors_mixins_associations_and_callable_extensions_preserve_authority() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let dep = ServiceKey::<u32>::new("dependency");
            let callable = ServiceKey::<Callable>::new("callable");
            let provider = root.plugin(&empty(), ()).unwrap();
            provider.ready().unwrap().await.unwrap();
            provider.context().provide(&dep, 7).unwrap();
            root.inject([dep.required()], {
                let callable = callable.clone();
                move |ctx| {
                    let callable = callable.clone();
                    async move {
                        ctx.provide(&callable, Callable)?;
                        Ok::<_, BoxError>(())
                    }
                }
            })
            .unwrap()
            .ready()
            .unwrap()
            .await
            .unwrap();
            let bound = root.bind(&callable).unwrap();
            assert_eq!(bound.invoke(1).unwrap(), 8);
            assert_eq!(bound.map(|_| Callable).invoke(2).unwrap(), 9);
            let accessor = AccessorKey::<Cell<u32>, u32>::new("session.value");
            let token = root
                .accessor(
                    &accessor,
                    Accessor::new(|_: Context, receiver: Rc<Cell<u32>>| {
                        Ok::<_, Infallible>(receiver.get())
                    })
                    .with_setter(|_, receiver, value| {
                        receiver.set(value + 1);
                        Ok::<_, Infallible>(())
                    }),
                )
                .unwrap();
            let receiver = Rc::new(Cell::new(0));
            root.write_accessor(&accessor, receiver.clone(), 10)
                .unwrap();
            assert_eq!(root.read_accessor(&accessor, receiver).unwrap(), 11);
            assert!(matches!(
                root.read_accessor(&AccessorKey::<(), u32>::new("session.value"), Rc::new(())),
                Err(ReflectError::Contract(_))
            ));
            token.dispose().unwrap().await.unwrap();
            assert!(matches!(
                root.read_accessor(&accessor, Rc::new(Cell::new(0))),
                Err(ReflectError::Missing(_))
            ));
            let mixin = AccessorKey::<(), u32>::new("session.answer");
            root.accessor(
                &mixin,
                Accessor::from_service(dep.clone(), |value: &u32, _, _: Rc<()>| {
                    Ok::<_, Infallible>(*value)
                }),
            )
            .unwrap();
            let stranger = root.plugin(&empty(), ()).unwrap();
            stranger.ready().unwrap().await.unwrap();
            assert!(
                stranger
                    .context()
                    .read_accessor(&mixin, Rc::new(()))
                    .is_err()
            );
            let base = ServiceKey::<()>::new("base");
            let associated = ServiceKey::<u32>::new("base.answer");
            root.provide(&base, ()).unwrap();
            provider.context().provide(&associated, 42).unwrap();
            let consumer = root
                .inject([base.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            assert!(matches!(
                consumer
                    .context()
                    .bind(&base)
                    .unwrap()
                    .associated::<u32>("answer"),
                Err(ServiceError::Undeclared { .. })
            ));
            assert_eq!(
                root.bind(&base)
                    .unwrap()
                    .associated::<u32>("answer")
                    .unwrap()
                    .call(|n, _| *n),
                42
            );
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn an_injected_publication_is_released_even_after_providing_the_same_name_in_another_realm() {
    for action in ["consumer", "provider", "root"] {
        bounded(async {
            let runtime = Runtime::new();
            runtime
                .run(async {
                    let root = runtime.context();
                    let key = ServiceKey::<u32>::new("foo");
                    let provider = root.plugin(&empty(), ()).unwrap();
                    provider.ready().unwrap().await.unwrap();
                    provider.context().provide(&key, 1).unwrap();
                    let consumer = root
                        .inject([key.required()], {
                            let key = key.clone();
                            move |ctx| {
                                let key = key.clone();
                                async move {
                                    ctx.isolate(&key).provide(&key, 2)?;
                                    Ok::<_, BoxError>(())
                                }
                            }
                        })
                        .unwrap();
                    consumer.ready().unwrap().await.unwrap();
                    match action {
                        "consumer" => {
                            consumer.dispose().unwrap().await.unwrap();
                            provider.dispose().unwrap().await.unwrap();
                        }
                        "provider" => {
                            provider.dispose().unwrap().await.unwrap();
                        }
                        _ => {
                            root.drain().unwrap().await.unwrap();
                        }
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
async fn same_name_publications_in_two_realms_keep_resources_until_the_exact_consumer_finishes() {
    for root_shutdown in [false, true] {
        bounded(async {
            let runtime = Runtime::new();
            runtime
                .run(async {
                    let root = runtime.context();
                    let key = ServiceKey::<u32>::new("foo");
                    let first = root.isolate(&key);
                    let second = root.isolate(&key);
                    let owner = root.plugin(&empty(), ()).unwrap();
                    owner.ready().unwrap().await.unwrap();
                    owner
                        .context()
                        .isolate_with(&key, &first.isolation(&key).unwrap())
                        .provide(&key, 1)
                        .unwrap();
                    owner
                        .context()
                        .isolate_with(&key, &second.isolation(&key).unwrap())
                        .provide(&key, 2)
                        .unwrap();
                    let closed = Rc::new(Cell::new(false));
                    owner
                        .context()
                        .on_dispose_sync({
                            let closed = closed.clone();
                            move || {
                                closed.set(true);
                                Ok::<_, Infallible>(())
                            }
                        })
                        .unwrap();
                    let (started, entered) = oneshot::channel();
                    let (release, gate) = oneshot::channel();
                    let gates = RefCell::new(Some((started, gate)));
                    let consumer = first
                        .inject([key.required()], {
                            let key = key.clone();
                            let closed = closed.clone();
                            move |ctx| {
                                let (started, gate) = gates.borrow_mut().take().unwrap();
                                let key = key.clone();
                                let closed = closed.clone();
                                async move {
                                    ctx.clone().on_dispose(move || async move {
                                        started.send(()).unwrap();
                                        gate.await.unwrap();
                                        assert!(!closed.get());
                                        assert_eq!(*ctx.get(&key)?.unwrap(), 1);
                                        Ok::<_, BoxError>(())
                                    })?;
                                    Ok::<_, BoxError>(())
                                }
                            }
                        })
                        .unwrap();
                    consumer.ready().unwrap().await.unwrap();
                    let task: cordis_core::LocalFuture<()> = if root_shutdown {
                        let task = root.drain().unwrap();
                        Box::pin(async move {
                            task.await.unwrap();
                        })
                    } else {
                        let task = owner.dispose().unwrap();
                        Box::pin(async move {
                            task.await.unwrap();
                        })
                    };
                    entered.await.unwrap();
                    assert!(!closed.get());
                    release.send(()).unwrap();
                    task.await;
                    assert!(closed.get());
                    root.drain().unwrap().await.unwrap();
                })
                .await
                .unwrap();
        })
        .await;
    }
}
