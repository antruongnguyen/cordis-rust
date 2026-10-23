//! M4 access/publication evidence against reflect.spec.ts, service.spec.ts, and fiber.spec.ts.
use cordis_core::{
    AdmissionError, BoxError, Context, FiberState, Plugin, Runtime, ServiceError, ServiceKey,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    rc::Rc,
};
use tokio::sync::oneshot;

async fn bounded(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("service test stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn publication_ownership_type_contracts_and_set_are_not_replacement() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<u32>::new("counter");
                let wrong = ServiceKey::<String>::new("counter");
                let provider = root
                    .plugin(
                        &Plugin::new("provider", |_, ()| async { Ok::<_, Infallible>(()) }),
                        (),
                    )
                    .unwrap();
                provider.ready().unwrap().await.unwrap();
                let publication = provider.context().provide(&key, 1).unwrap();
                let id = publication.id().clone();
                assert!(matches!(
                    root.provide(&key, 2),
                    Err(ServiceError::DuplicateProvider { .. })
                ));
                assert!(matches!(
                    root.set(&key, 2),
                    Err(ServiceError::NotOwner { .. })
                ));
                assert!(matches!(
                    root.get(&wrong),
                    Err(ServiceError::TypeMismatch { .. })
                ));
                assert!(matches!(
                    provider.context().set(&wrong, "bad".into()),
                    Err(ServiceError::TypeMismatch { .. })
                ));
                let loads = Rc::new(Cell::new(0));
                let consumer = root
                    .inject([key.required()], {
                        let key = key.clone();
                        let loads = loads.clone();
                        move |ctx| {
                            let key = key.clone();
                            let loads = loads.clone();
                            async move {
                                assert!(ctx.get(&key)?.is_some());
                                loads.set(loads.get() + 1);
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .unwrap();
                consumer.ready().unwrap().await.unwrap();
                provider.context().set(&key, 2).unwrap();
                assert_eq!(publication.id(), &id);
                assert_eq!(*consumer.context().get(&key).unwrap().unwrap(), 2);
                consumer.ready().unwrap().await.unwrap();
                assert_eq!(loads.get(), 1, "set must not trigger a dependency reload");
                publication.withdraw().unwrap().await.unwrap();
                assert_eq!(
                    consumer.ready().unwrap().await.unwrap(),
                    FiberState::Pending
                );
                let next = provider.context().provide(&key, 2).unwrap();
                assert_ne!(next.id(), &id);
                consumer.dispose().unwrap().await.unwrap();
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn checked_access_distinguishes_undeclared_inactive_and_direct_paths() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let key = ServiceKey::<u32>::new("foo");
            let missing = ServiceKey::<u32>::new("missing");
            assert!(root.get(&missing).unwrap().is_none());
            assert!(matches!(
                root.set(&missing, 1),
                Err(ServiceError::NotProvided { .. })
            ));
            let provider = root
                .plugin(
                    &Plugin::new("provider", {
                        let key = key.clone();
                        move |ctx, ()| {
                            let key = key.clone();
                            async move {
                                ctx.provide(&key, 1)?;
                                assert_eq!(*ctx.get(&key)?.unwrap(), 1);
                                Ok::<_, BoxError>(())
                            }
                        }
                    }),
                    (),
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            let stranger = root
                .plugin(
                    &Plugin::new("stranger", |_, ()| async { Ok::<_, Infallible>(()) }),
                    (),
                )
                .unwrap();
            stranger.ready().unwrap().await.unwrap();
            assert!(matches!(
                stranger.context().get(&key),
                Err(ServiceError::Undeclared { .. })
            ));
            assert_eq!(*stranger.context().get_direct(&key).unwrap().unwrap(), 1);
            let consumer = root
                .inject([key.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            consumer.ready().unwrap().await.unwrap();
            let context = consumer.context();
            consumer.dispose().unwrap().await.unwrap();
            assert!(matches!(
                context.get(&key),
                Err(ServiceError::Inactive { .. })
            ));
            assert_eq!(*context.get_direct(&key).unwrap().unwrap(), 1);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn loading_provider_blocks_consumers_and_pending_defers_validation() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        let key = ServiceKey::<u32>::new("foo");
        let other = ServiceKey::<String>::new("bar");
        let validations = Rc::new(Cell::new(0));
        let (published, publication_ready) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let gates = RefCell::new(Some((published, gate)));
        runtime
            .run(async {
                let consumer = root
                    .plugin(
                        &Plugin::validated(
                            "consumer",
                            {
                                let validations = validations.clone();
                                move |_: &()| {
                                    validations.set(validations.get() + 1);
                                    Ok::<_, Infallible>(())
                                }
                            },
                            |_, ()| async { Ok::<_, Infallible>(()) },
                        )
                        .injecting([key.required(), other.required()]),
                        (),
                    )
                    .unwrap();
                assert_eq!(consumer.state(), FiberState::Pending);
                assert_eq!(
                    consumer.ready().unwrap().await.unwrap(),
                    FiberState::Pending
                );
                consumer.restart().unwrap().await.unwrap();
                assert_eq!(validations.get(), 0);
                root.provide(&other, "ready".into()).unwrap();
                let provider = root
                    .plugin(
                        &Plugin::new("provider", {
                            let key = key.clone();
                            move |ctx, ()| {
                                let key = key.clone();
                                let (published, gate) = gates.borrow_mut().take().unwrap();
                                async move {
                                    ctx.provide(&key, 7)?;
                                    published.send(()).unwrap();
                                    gate.await.unwrap();
                                    Ok::<_, BoxError>(())
                                }
                            }
                        }),
                        (),
                    )
                    .unwrap();
                publication_ready.await.unwrap();
                assert!(root.get_direct(&key).unwrap().is_none());
                assert_eq!(*root.get_unchecked(&key).unwrap().unwrap(), 7);
                assert_eq!(consumer.state(), FiberState::Pending);
                release.send(()).unwrap();
                provider.ready().unwrap().await.unwrap();
                assert_eq!(consumer.ready().unwrap().await.unwrap(), FiberState::Active);
                assert_eq!(validations.get(), 1);
                provider.dispose().unwrap().await.unwrap();
                assert_eq!(
                    consumer.ready().unwrap().await.unwrap(),
                    FiberState::Pending
                );
                assert!(matches!(
                    consumer.context().get(&key),
                    Err(ServiceError::Inactive { .. })
                ));
                consumer.dispose().unwrap().await.unwrap();
                assert_eq!(consumer.state(), FiberState::Disposed);
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn injection_lists_are_per_fiber_but_plugin_identity_is_shared() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let first = ServiceKey::<u32>::new("first");
            let second = ServiceKey::<u32>::new("second");
            let plugin = Plugin::new("consumer", |_, ()| async { Ok::<_, Infallible>(()) });
            let a = root
                .plugin(&plugin.clone().injecting([first.required()]), ())
                .unwrap();
            let b = root
                .plugin(&plugin.injecting([second.required()]), ())
                .unwrap();
            assert_eq!(root.registry().len().unwrap(), 1);
            root.provide(&first, 1).unwrap();
            a.ready().unwrap().await.unwrap();
            assert_eq!(a.state(), FiberState::Active);
            assert_eq!(b.state(), FiberState::Pending);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_consumers_ignore_dependency_refresh_and_mismatches_fail_explicitly() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let key = ServiceKey::<u32>::new("foo");
            let old = root.provide(&key, 1).unwrap();
            let loads = Rc::new(Cell::new(0));
            let failed = root
                .inject([key.required()], {
                    let loads = loads.clone();
                    move |_| {
                        loads.set(loads.get() + 1);
                        async { Err::<(), _>(std::io::Error::other("load failed")) }
                    }
                })
                .unwrap();
            assert!(failed.ready().unwrap().await.is_err());
            old.withdraw().unwrap().await.unwrap();
            root.provide(&key, 1).unwrap();
            assert_eq!(failed.state(), FiberState::Failed);
            assert_eq!(loads.get(), 1);
            let mismatch = root
                .inject([ServiceKey::<String>::new("foo").required()], |_| async {
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            let error = mismatch.ready().unwrap().await.unwrap_err();
            let cordis_core::LifecycleError::Callback(error) = error else {
                panic!("wrong error")
            };
            assert!(matches!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<ServiceError>(),
                Some(ServiceError::TypeMismatch { .. })
            ));
            assert_eq!(runtime.take_failures().len(), 2);
            assert_eq!(
                failed.dispose().unwrap().await.unwrap(),
                FiberState::Disposed
            );
            assert_eq!(
                mismatch.dispose().unwrap().await.unwrap(),
                FiberState::Disposed
            );
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_checks_block_and_report_without_running_under_store_borrows() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let key = ServiceKey::<u32>::new("checked");
            let checked = root
                .provide_checked(&key, 0, {
                    let root = root.clone();
                    let key = key.clone();
                    move |value| {
                        assert!(root.get_unchecked(&key)?.is_some());
                        Ok::<_, ServiceError>(*value > 0)
                    }
                })
                .unwrap();
            let consumer = root
                .inject([key.required()], |_| async { Ok::<_, Infallible>(()) })
                .unwrap();
            assert_eq!(consumer.state(), FiberState::Pending);
            root.set(&key, 1).unwrap();
            assert_eq!(consumer.state(), FiberState::Pending, "set does not notify");
            checked.withdraw().unwrap().await.unwrap();
            let bad = root
                .provide_checked(&key, 1, |_| {
                    Err::<bool, _>(std::io::Error::other("check failed"))
                })
                .unwrap();
            assert_eq!(consumer.state(), FiberState::Pending);
            assert_eq!(runtime.take_failures().len(), 1);
            bad.withdraw().unwrap().await.unwrap();
            let panic = root
                .provide_checked(&key, 1, |_| -> Result<bool, Infallible> {
                    panic!("check panic")
                })
                .unwrap();
            assert_eq!(runtime.take_failures().len(), 1);
            panic.withdraw().unwrap().await.unwrap();
            root.provide_checked(&key, 1, |_| Ok::<_, Infallible>(true))
                .unwrap();
            assert_eq!(consumer.ready().unwrap().await.unwrap(), FiberState::Active);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[test]
fn publication_requires_host_admission_and_values_do_not_keep_runtime_alive() {
    let key = ServiceKey::<Context>::new("context");
    let runtime = Runtime::new();
    let context = runtime.context();
    assert!(matches!(
        context.provide(&key, context.clone()),
        Err(ServiceError::Admission(AdmissionError::HostNotRunning))
    ));
    drop(runtime);
    assert!(matches!(
        context.get(&key),
        Err(ServiceError::Admission(AdmissionError::RuntimeDropped))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn restart_and_other_service_notifications_do_not_recheck_an_unaffected_readiness_check() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let foo = ServiceKey::<()>::new("foo");
            let bar = ServiceKey::<()>::new("bar");
            let calls = Rc::new(Cell::new(0));
            let ready = Rc::new(Cell::new(false));
            let first = root
                .provide_checked(&foo, (), {
                    let calls = calls.clone();
                    let ready = ready.clone();
                    move |_| {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(ready.get())
                    }
                })
                .unwrap();
            let consumer = root
                .inject([foo.required(), bar.required()], |_| async {
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            assert_eq!(calls.get(), 1);
            ready.set(true);
            root.provide(&bar, ()).unwrap();
            assert_eq!(calls.get(), 1);
            consumer.restart().unwrap().await.unwrap();
            assert_eq!(consumer.state(), FiberState::Pending);
            assert_eq!(calls.get(), 1);
            first.withdraw().unwrap().await.unwrap();
            root.provide(&foo, ()).unwrap();
            assert_eq!(consumer.ready().unwrap().await.unwrap(), FiberState::Active);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_republication_restores_baselines_and_disposed_handles_release_snapshot_values() {
    bounded(async {
        let runtime = Runtime::new();
        let root = runtime.context();
        runtime
            .run(async {
                let key = ServiceKey::<Rc<()>>::new("__proto__");
                let loads = Rc::new(Cell::new(0));
                let consumer = root
                    .inject([key.required()], {
                        let loads = loads.clone();
                        move |_| {
                            loads.set(loads.get() + 1);
                            async { Ok::<_, Infallible>(()) }
                        }
                    })
                    .unwrap();
                let effect_baseline = root.get_effects().unwrap().len();
                for iteration in 1..=20 {
                    let value = Rc::new(());
                    let weak = Rc::downgrade(&value);
                    let publication = root.provide(&key, value).unwrap();
                    consumer.ready().unwrap().await.unwrap();
                    assert_eq!(loads.get(), iteration);
                    publication.withdraw().unwrap().await.unwrap();
                    consumer.ready().unwrap().await.unwrap();
                    assert!(weak.upgrade().is_none());
                    assert!(root.get_unchecked(&key).unwrap().is_none());
                    assert_eq!(root.get_effects().unwrap().len(), effect_baseline);
                }
                let value = Rc::new(());
                let weak = Rc::downgrade(&value);
                let publication = root.provide(&key, value).unwrap();
                consumer.ready().unwrap().await.unwrap();
                consumer.dispose().unwrap().await.unwrap();
                publication.withdraw().unwrap().await.unwrap();
                assert!(
                    weak.upgrade().is_none(),
                    "retained disposed handle kept its snapshot"
                );
                assert!(root.registry().is_empty().unwrap());
                assert!(root.get_effects().unwrap().is_empty());
                assert_eq!(consumer.state(), FiberState::Disposed);
                let captured = root.clone();
                let context_key = ServiceKey::new("context");
                root.provide(&context_key, captured).unwrap();
                root.drain().unwrap().await.unwrap();
                assert!(root.get_unchecked(&context_key).unwrap().is_none());
                root.provide(&key, Rc::new(())).unwrap();
                root.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn root_invocation_does_not_grant_a_captured_plugin_context_root_authority() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let key = ServiceKey::<u32>::new("foo");
            let provider = root
                .plugin(
                    &Plugin::new("provider", {
                        let key = key.clone();
                        move |ctx, ()| {
                            let key = key.clone();
                            async move {
                                ctx.provide(&key, 7)?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    }),
                    (),
                )
                .unwrap();
            provider.ready().unwrap().await.unwrap();
            let reader = Plugin::new("reader", |ctx, key: ServiceKey<Context>| async move {
                ctx.provide(&key, ctx.clone())?;
                Ok::<_, BoxError>(())
            });
            let unchecked = ServiceKey::new("unchecked-reader");
            let checked = ServiceKey::new("checked-reader");
            root.plugin(&reader, unchecked.clone())
                .unwrap()
                .ready()
                .unwrap()
                .await
                .unwrap();
            root.plugin(&reader.injecting([key.required()]), checked.clone())
                .unwrap()
                .ready()
                .unwrap()
                .await
                .unwrap();
            assert_eq!(*root.get(&key).unwrap().unwrap(), 7);
            let captured = root.get(&unchecked).unwrap().unwrap();
            assert!(matches!(
                captured.get(&key),
                Err(ServiceError::Undeclared { .. })
            ));
            assert_eq!(
                *root
                    .get(&checked)
                    .unwrap()
                    .unwrap()
                    .get(&key)
                    .unwrap()
                    .unwrap(),
                7
            );
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_resident_fibers_release_dependency_values_after_rollback() {
    let runtime = Runtime::new();
    let root = runtime.context();
    runtime
        .run(async {
            let key = ServiceKey::<Rc<()>>::new("foo");
            let value = Rc::new(());
            let weak = Rc::downgrade(&value);
            let publication = root.provide(&key, value).unwrap();
            let failed = root
                .inject([key.required()], |_| async {
                    Err::<(), _>(std::io::Error::other("failed load"))
                })
                .unwrap();
            assert!(failed.ready().unwrap().await.is_err());
            publication.withdraw().unwrap().await.unwrap();
            assert!(weak.upgrade().is_none());
            assert_eq!(failed.state(), FiberState::Failed);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}
