//! End-to-end isolated service initialization, config convergence, and exact teardown.
use cordis_core::{BoxError, Disposer, Plugin, Runtime, ServiceKey};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    rc::Rc,
};

struct Store {
    version: u32,
    open: Rc<Cell<bool>>,
}

pub(crate) fn run() -> Result<(), BoxError> {
    let executor = tokio::runtime::Builder::new_current_thread().build()?;
    executor.block_on(async {
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let key = ServiceKey::<Store>::new("store");
                let realm = root.isolate(&key);
                let timeline = Rc::new(RefCell::new(Vec::new()));
                let provider = realm.plugin(
                    &Plugin::initialized("StoreProvider", {
                        let key = key.clone();
                        let timeline = timeline.clone();
                        move |ctx, version: u32| {
                            let key = key.clone();
                            let timeline = timeline.clone();
                            async move {
                                let open = Rc::new(Cell::new(true));
                                ctx.provide(
                                    &key,
                                    Store {
                                        version,
                                        open: open.clone(),
                                    },
                                )?;
                                ctx.logger(None)
                                    .info(format!("publishing store {version}"))?;
                                Ok::<_, BoxError>(Disposer::sync(move || {
                                    open.set(false);
                                    timeline.borrow_mut().push((version, "close"));
                                    Ok::<_, Infallible>(())
                                }))
                            }
                        }
                    }),
                    1,
                )?;
                provider.ready()?.await?;
                assert!(
                    root.get_direct(&key)?.is_none(),
                    "realm must not fall back to root"
                );
                let applied = Rc::new(RefCell::new(Vec::new()));
                let consumer = realm.plugin(
                    &Plugin::new("StoreConsumer", {
                        let key = key.clone();
                        let timeline = timeline.clone();
                        let applied = applied.clone();
                        move |ctx, mode: &'static str| {
                            let key = key.clone();
                            let timeline = timeline.clone();
                            let applied = applied.clone();
                            async move {
                                let store = ctx.get(&key)?.unwrap();
                                applied.borrow_mut().push((store.version, mode));
                                ctx.clone().on_dispose_sync(move || {
                                    let old = ctx.get(&key)?.unwrap();
                                    assert!(
                                        old.open.get(),
                                        "exact provider must survive final use"
                                    );
                                    timeline.borrow_mut().push((old.version, "read"));
                                    Ok::<_, BoxError>(())
                                })?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .injecting([key.required()]),
                    "old",
                )?;
                consumer.ready()?.await?;
                let updating_provider = provider.update(2)?;
                let updating_consumer = consumer.update("new")?;
                updating_provider.await?;
                updating_consumer.await?;
                consumer.ready()?.await?;
                assert_eq!(*applied.borrow(), [(1, "old"), (2, "new")]);
                root.drain()?.await?;
                assert_eq!(
                    *timeline.borrow(),
                    [(1, "read"), (1, "close"), (2, "read"), (2, "close")]
                );
                assert!(root.registry().is_empty()?);
                assert!(realm.get_unchecked(&key)?.is_none());
                assert!(
                    root.log_buffer()?
                        .iter()
                        .any(|record| record.name == "store-provider")
                );
                Ok::<_, BoxError>(())
            })
            .await??;
        Ok::<_, BoxError>(())
    })
}

#[cfg(not(test))]
fn main() -> Result<(), BoxError> {
    run()
}
