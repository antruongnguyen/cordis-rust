use cordis_m0_spike::LocalFuture;
use futures::{FutureExt, future::join_all};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::rc::{Rc, Weak};
use tokio::sync::{Notify, oneshot};
use tokio::task::{JoinHandle, LocalSet};

type Cleanup = Box<dyn FnOnce() -> LocalFuture<Result<(), &'static str>>>;
type Setup = Rc<dyn Fn(Rc<Fiber>) -> Cleanup>;

tokio::task_local! {
    static CLEANING: Rc<Fiber>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Pending,
    Active,
    Unloading,
    Disposed,
}

struct Binding {
    value: u32,
    closed: Cell<bool>,
    dependents: RefCell<BTreeMap<u64, Weak<Fiber>>>,
}

impl Binding {
    fn read(&self) -> u32 {
        assert!(
            !self.closed.get(),
            "provider closed before its last consumer"
        );
        self.value
    }
}

struct Fiber {
    id: u64,
    root: bool,
    terminal: Cell<bool>,
    busy: Cell<bool>,
    settled: Notify,
    state: Cell<State>,
    // ponytail: one dependency per Fiber; replace with a dependency map in M4.
    required: Option<&'static str>,
    committed: RefCell<Option<Rc<Binding>>>,
    provisions: RefCell<Vec<(&'static str, Rc<Binding>)>>,
    // ponytail: disposed children stay until parent drain; unlink promptly in core.
    children: RefCell<Vec<Rc<Fiber>>>,
    cleanups: RefCell<Vec<Cleanup>>,
    setup: Option<Setup>,
}

struct Runtime {
    next: Cell<u64>,
    residents: RefCell<BTreeMap<u64, Rc<Fiber>>>,
    services: RefCell<BTreeMap<&'static str, Rc<Binding>>>,
    errors: RefCell<Vec<String>>,
}

impl Runtime {
    fn new() -> Rc<Self> {
        let runtime = Rc::new(Self {
            next: Cell::new(0),
            residents: RefCell::new(BTreeMap::new()),
            services: RefCell::new(BTreeMap::new()),
            errors: RefCell::new(Vec::new()),
        });
        runtime.create(None, None, None);
        runtime
    }

    fn root(&self) -> Rc<Fiber> {
        self.residents.borrow()[&0].clone()
    }

    fn create(
        &self,
        parent: Option<&Rc<Fiber>>,
        required: Option<&'static str>,
        setup: Option<Setup>,
    ) -> Rc<Fiber> {
        assert!(parent.is_none_or(|parent| !parent.terminal.get()));
        let id = self.next.get();
        self.next.set(id + 1);
        let fiber = Rc::new(Fiber {
            id,
            root: parent.is_none(),
            terminal: Cell::new(false),
            busy: Cell::new(false),
            settled: Notify::new(),
            state: Cell::new(State::Pending),
            required,
            committed: RefCell::new(None),
            provisions: RefCell::new(Vec::new()),
            children: RefCell::new(Vec::new()),
            cleanups: RefCell::new(Vec::new()),
            setup,
        });
        self.residents.borrow_mut().insert(id, fiber.clone());
        if let Some(parent) = parent {
            parent.children.borrow_mut().push(fiber.clone());
        }
        self.activate(&fiber);
        fiber
    }

    fn activate(&self, fiber: &Rc<Fiber>) {
        if fiber.terminal.get() || fiber.state.get() != State::Pending {
            return;
        }
        if let Some(name) = fiber.required {
            let binding = self.services.borrow().get(name).cloned();
            let Some(binding) = binding else { return };
            binding
                .dependents
                .borrow_mut()
                .insert(fiber.id, Rc::downgrade(fiber));
            *fiber.committed.borrow_mut() = Some(binding);
        }
        fiber.state.set(State::Active);
        if let Some(setup) = &fiber.setup {
            let cleanup = setup(fiber.clone());
            fiber.cleanups.borrow_mut().push(cleanup);
        }
    }

    fn publish(
        &self,
        owner: &Rc<Fiber>,
        name: &'static str,
        value: u32,
    ) -> Result<Rc<Binding>, &'static str> {
        if owner.terminal.get() {
            return Err("inactive owner");
        }
        if self.services.borrow().contains_key(name) {
            return Err("duplicate publication");
        }
        let binding = Rc::new(Binding {
            value,
            closed: Cell::new(false),
            dependents: RefCell::new(BTreeMap::new()),
        });
        self.services.borrow_mut().insert(name, binding.clone());
        owner.provisions.borrow_mut().push((name, binding.clone()));
        let candidates: Vec<_> = self.residents.borrow().values().cloned().collect();
        for candidate in candidates {
            if candidate.required == Some(name) {
                self.activate(&candidate);
            }
        }
        Ok(binding)
    }

    fn unpublish(&self, name: &'static str, binding: &Rc<Binding>) {
        let mut services = self.services.borrow_mut();
        if services
            .get(name)
            .is_some_and(|current| Rc::ptr_eq(current, binding))
        {
            services.remove(name);
        }
    }

    fn withdraw(
        self: &Rc<Self>,
        owner: Rc<Fiber>,
        name: &'static str,
        binding: Rc<Binding>,
    ) -> JoinHandle<()> {
        self.unpublish(name, &binding);
        let runtime = self.clone();
        tokio::task::spawn_local(async move {
            let dependents: Vec<_> = binding
                .dependents
                .borrow()
                .values()
                .filter_map(Weak::upgrade)
                .collect();
            join_all(
                dependents
                    .into_iter()
                    .map(|dependent| runtime.clone().drain(dependent)),
            )
            .await;
            assert!(binding.dependents.borrow().is_empty());
            binding.closed.set(true);
            owner
                .provisions
                .borrow_mut()
                .retain(|(_, current)| !Rc::ptr_eq(current, &binding));
        })
    }

    fn terminalize(&self, fiber: &Fiber) {
        if !fiber.root {
            fiber.terminal.set(true);
            self.residents.borrow_mut().remove(&fiber.id);
        }
    }

    // The request commits before returning; the JoinHandle only waits for completion.
    fn dispose(self: &Rc<Self>, fiber: Rc<Fiber>) -> JoinHandle<()> {
        self.try_dispose(fiber)
            .expect("test requests an admitted operation")
    }

    fn try_dispose(self: &Rc<Self>, fiber: Rc<Fiber>) -> Result<JoinHandle<()>, &'static str> {
        if CLEANING
            .try_with(|owner| Rc::ptr_eq(owner, &fiber))
            .unwrap_or(false)
        {
            return Err("same-fiber lifecycle request during cleanup");
        }
        self.terminalize(&fiber);
        let runtime = self.clone();
        Ok(tokio::task::spawn_local(async move {
            runtime.drain(fiber).await
        }))
    }

    fn drain(self: Rc<Self>, fiber: Rc<Fiber>) -> LocalFuture<()> {
        Box::pin(async move {
            while fiber.busy.get() {
                fiber.settled.notified().await;
            }
            if fiber.state.get() == State::Disposed {
                return;
            }
            fiber.busy.set(true);
            fiber.state.set(State::Unloading);
            let provisions = fiber.provisions.borrow().clone();
            for (name, binding) in &provisions {
                self.unpublish(name, binding);
            }
            let dependents: Vec<_> = provisions
                .iter()
                .flat_map(|(_, binding)| {
                    binding
                        .dependents
                        .borrow()
                        .values()
                        .filter_map(Weak::upgrade)
                        .collect::<Vec<_>>()
                })
                .collect();
            join_all(
                dependents
                    .into_iter()
                    .map(|dependent| self.clone().drain(dependent)),
            )
            .await;

            let children = std::mem::take(&mut *fiber.children.borrow_mut());
            for child in &children {
                self.terminalize(child);
            }
            join_all(children.into_iter().map(|child| self.clone().drain(child))).await;

            let cleanups = std::mem::take(&mut *fiber.cleanups.borrow_mut());
            let outcomes = join_all(cleanups.into_iter().rev().map(|cleanup| async {
                AssertUnwindSafe(CLEANING.scope(fiber.clone(), async { cleanup().await }))
                    .catch_unwind()
                    .await
            }))
            .await;
            for outcome in outcomes {
                match outcome {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => self.errors.borrow_mut().push(error.into()),
                    Err(_) => self
                        .errors
                        .borrow_mut()
                        .push("user cleanup panicked".into()),
                }
            }
            for (_, binding) in provisions {
                assert!(binding.dependents.borrow().is_empty());
                binding.closed.set(true);
            }
            fiber.provisions.borrow_mut().clear();
            if let Some(binding) = fiber.committed.borrow_mut().take() {
                binding.dependents.borrow_mut().remove(&fiber.id);
            }
            fiber.state.set(if fiber.terminal.get() {
                State::Disposed
            } else {
                State::Pending
            });
            fiber.busy.set(false);
            fiber.settled.notify_waiters();
            if !fiber.terminal.get() {
                self.activate(&fiber);
            }
        })
    }
}

fn cleanup(run: impl Future<Output = Result<(), &'static str>> + 'static) -> Cleanup {
    Box::new(move || Box::pin(run))
}

async fn local(test: impl Future<Output = ()>) {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        LocalSet::new().run_until(test),
    )
    .await
    .expect("lifecycle spike stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn pending_consumer_keeps_exact_binding_through_removed_consumer_cleanup() {
    local(async {
        let rt = Runtime::new();
        let provider = rt.create(Some(&rt.root()), None, None);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gates = Rc::new(RefCell::new(Some((entered_tx, release_rx))));
        let reads = Rc::new(RefCell::new(Vec::new()));
        let setup: Setup = Rc::new({
            let reads = reads.clone();
            move |fiber| {
                let fiber = Rc::downgrade(&fiber);
                let gates = gates.borrow_mut().take();
                let reads = reads.clone();
                cleanup(async move {
                    if let Some((entered, release)) = gates {
                        entered.send(()).unwrap();
                        release.await.unwrap();
                    }
                    let fiber = fiber.upgrade().expect("cleanup owner retained by driver");
                    reads
                        .borrow_mut()
                        .push(fiber.committed.borrow().as_ref().unwrap().read());
                    Ok(())
                })
            }
        });
        let consumer = rt.create(Some(&rt.root()), Some("foo"), Some(setup));
        assert_eq!(consumer.state.get(), State::Pending);
        let old = rt.publish(&provider, "foo", 7).unwrap();
        assert_eq!(consumer.state.get(), State::Active);
        let consumer_done = rt.dispose(consumer.clone());
        entered_rx.await.unwrap();
        assert!(!rt.residents.borrow().contains_key(&consumer.id));
        let provider_done = rt.dispose(provider);
        tokio::task::yield_now().await;
        assert!(!provider_done.is_finished());
        assert!(!old.closed.get());
        assert!(!rt.services.borrow().contains_key("foo"));
        let replacement = rt.create(Some(&rt.root()), None, None);
        let new = rt.publish(&replacement, "foo", 7).unwrap();
        assert!(!Rc::ptr_eq(&old, &new));
        let late = rt.create(Some(&rt.root()), Some("foo"), None);
        assert!(Rc::ptr_eq(late.committed.borrow().as_ref().unwrap(), &new));
        release_tx.send(()).unwrap();
        consumer_done.await.unwrap();
        provider_done.await.unwrap();
        assert_eq!(*reads.borrow(), [7]);
        assert!(old.closed.get());
        assert!(!new.closed.get());
        rt.dispose(rt.root()).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn withdrawal_returns_consumer_to_pending_then_rebinds() {
    local(async {
        let rt = Runtime::new();
        let provider = rt.create(Some(&rt.root()), None, None);
        let consumer = rt.create(Some(&rt.root()), Some("foo"), None);
        let old = rt.publish(&provider, "foo", 1).unwrap();
        rt.dispose(provider).await.unwrap();
        assert_eq!(consumer.state.get(), State::Pending);
        assert!(consumer.committed.borrow().is_none());
        let new = rt.publish(&rt.root(), "foo", 1).unwrap();
        assert!(!Rc::ptr_eq(&old, &new));
        assert!(Rc::ptr_eq(
            consumer.committed.borrow().as_ref().unwrap(),
            &new
        ));
        rt.dispose(rt.root()).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn parent_cascades_root_reuses_and_dropped_waiter_does_not_cancel() {
    local(async {
        let rt = Runtime::new();
        let weak_runtime = Rc::downgrade(&rt);
        let parent = rt.create(Some(&rt.root()), None, None);
        let child = rt.create(Some(&parent), None, None);
        let weak_child = Rc::downgrade(&child);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (finished_tx, finished_rx) = oneshot::channel();
        child.cleanups.borrow_mut().push(cleanup(async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            finished_tx.send(()).unwrap();
            Ok(())
        }));
        let waiter = rt.dispose(parent.clone());
        assert!(!rt.residents.borrow().contains_key(&parent.id));
        drop(waiter);
        drop(child);
        entered_rx.await.unwrap();
        assert!(weak_child.upgrade().is_some());
        release_tx.send(()).unwrap();
        finished_rx.await.unwrap();
        rt.dispose(rt.root()).await.unwrap();
        assert_eq!(parent.state.get(), State::Disposed);
        assert!(weak_child.upgrade().is_none());
        assert_eq!(rt.root().state.get(), State::Active);
        let fresh = rt.create(Some(&rt.root()), None, None);
        assert_eq!(fresh.state.get(), State::Active);
        rt.dispose(rt.root()).await.unwrap();
        rt.dispose(rt.root()).await.unwrap();
        assert_eq!(rt.residents.borrow().len(), 1);
        drop(parent);
        drop(fresh);
        drop(rt);
        assert!(weak_runtime.upgrade().is_none());
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn top_level_cleanup_is_concurrent_attempt_all_and_exactly_once() {
    local(async {
        let rt = Runtime::new();
        let fiber = rt.create(Some(&rt.root()), None, None);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (sibling_tx, sibling_rx) = oneshot::channel();
        let calls = Rc::new(Cell::new(0));
        fiber.cleanups.borrow_mut().push(cleanup(async move {
            started_tx.send(()).unwrap();
            release_rx.await.unwrap();
            Ok(())
        }));
        fiber.cleanups.borrow_mut().push(cleanup({
            let calls = calls.clone();
            async move {
                calls.set(calls.get() + 1);
                sibling_tx.send(()).unwrap();
                Err("cleanup failed")
            }
        }));
        fiber
            .cleanups
            .borrow_mut()
            .push(Box::new(|| panic!("user cleanup")));
        let first = rt.dispose(fiber.clone());
        let second = rt.dispose(fiber.clone());
        started_rx.await.unwrap();
        sibling_rx.await.unwrap();
        assert!(!first.is_finished());
        release_tx.send(()).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(
            *rt.errors.borrow(),
            ["user cleanup panicked", "cleanup failed"]
        );
        assert_eq!(rt.publish(&fiber, "late", 0).err(), Some("inactive owner"));
        rt.dispose(rt.root()).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn composed_cleanup_sequences_async_children_in_reverse_order() {
    let (release_tx, release_rx) = oneshot::channel();
    let trace = Rc::new(RefCell::new(Vec::new()));
    let mut children = vec![
        cleanup({
            let trace = trace.clone();
            async move {
                trace.borrow_mut().push("first");
                Ok(())
            }
        }),
        cleanup({
            let trace = trace.clone();
            async move {
                trace.borrow_mut().push("second start");
                release_rx.await.unwrap();
                trace.borrow_mut().push("second end");
                Ok(())
            }
        }),
    ];
    let composed = async {
        while let Some(child) = children.pop() {
            child().await?;
        }
        Ok::<_, &'static str>(())
    };
    tokio::pin!(composed);
    assert!(futures::poll!(&mut composed).is_pending());
    assert_eq!(*trace.borrow(), ["second start"]);
    release_tx.send(()).unwrap();
    composed.await.unwrap();
    assert_eq!(*trace.borrow(), ["second start", "second end", "first"]);
}

#[tokio::test(flavor = "current_thread")]
async fn individual_withdrawal_does_not_wait_for_another_binding() {
    local(async {
        let rt = Runtime::new();
        let provider = rt.create(Some(&rt.root()), None, None);
        let foo = rt.publish(&provider, "foo", 1).unwrap();
        let bar = rt.publish(&provider, "bar", 2).unwrap();
        let first = rt.create(Some(&rt.root()), Some("foo"), None);
        let second = rt.create(Some(&rt.root()), Some("bar"), None);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        second.cleanups.borrow_mut().push(cleanup(async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            Ok(())
        }));
        let blocked = rt.dispose(second);
        entered_rx.await.unwrap();
        rt.withdraw(provider, "foo", foo.clone()).await.unwrap();
        assert!(foo.closed.get());
        assert!(!bar.closed.get());
        assert_eq!(first.state.get(), State::Pending);
        assert!(!blocked.is_finished());
        release_tx.send(()).unwrap();
        blocked.await.unwrap();
        rt.dispose(rt.root()).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn transitive_provider_chain_drains_from_leaf_to_provider() {
    local(async {
        let rt = Runtime::new();
        let provider = rt.create(Some(&rt.root()), None, None);
        let foo = rt.publish(&provider, "foo", 1).unwrap();
        let trace = Rc::new(RefCell::new(Vec::new()));
        let setup: Setup = Rc::new({
            let runtime = Rc::downgrade(&rt);
            let trace = trace.clone();
            move |relay| {
                runtime
                    .upgrade()
                    .unwrap()
                    .publish(&relay, "bar", 2)
                    .unwrap();
                let relay = Rc::downgrade(&relay);
                let trace = trace.clone();
                cleanup(async move {
                    let relay = relay.upgrade().unwrap();
                    assert_eq!(relay.committed.borrow().as_ref().unwrap().read(), 1);
                    trace.borrow_mut().push("relay read foo");
                    Ok(())
                })
            }
        });
        let relay = rt.create(Some(&rt.root()), Some("foo"), Some(setup));
        let leaf = rt.create(Some(&rt.root()), Some("bar"), None);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        leaf.cleanups.borrow_mut().push(cleanup({
            let leaf = Rc::downgrade(&leaf);
            let trace = trace.clone();
            async move {
                entered_tx.send(()).unwrap();
                release_rx.await.unwrap();
                assert_eq!(
                    leaf.upgrade()
                        .unwrap()
                        .committed
                        .borrow()
                        .as_ref()
                        .unwrap()
                        .read(),
                    2
                );
                trace.borrow_mut().push("leaf read bar");
                Ok(())
            }
        }));
        let done = rt.dispose(rt.root());
        entered_rx.await.unwrap();
        assert!(!foo.closed.get());
        assert!(relay.committed.borrow().is_some());
        release_tx.send(()).unwrap();
        done.await.unwrap();
        assert_eq!(*trace.borrow(), ["leaf read bar", "relay read foo"]);
        assert!(foo.closed.get());
        assert!(rt.errors.borrow().is_empty());
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executor_shutdown_is_not_a_cleanup_completion_guarantee() {
    let local = LocalSet::new();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel();
    let runtime = Runtime::new();
    let fiber = runtime.create(Some(&runtime.root()), None, None);
    fiber.cleanups.borrow_mut().push(cleanup(async move {
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
        done_tx.send(()).unwrap();
        Ok(())
    }));
    let operation = {
        let _entered = local.enter();
        runtime.dispose(fiber)
    };
    local.run_until(entered_rx).await.unwrap();
    drop(local);
    assert!(operation.await.unwrap_err().is_cancelled());
    assert!(done_rx.await.is_err());
    assert!(release_tx.send(()).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn same_fiber_cleanup_request_is_refused_before_admission() {
    local(async {
        let runtime = Runtime::new();
        let fiber = runtime.create(Some(&runtime.root()), None, None);
        let other_runtime = Runtime::new();
        let other_fiber = other_runtime.create(Some(&other_runtime.root()), None, None);
        assert_eq!(fiber.id, other_fiber.id);
        let other_host = Rc::downgrade(&other_runtime);
        let owner = Rc::downgrade(&fiber);
        let host = Rc::downgrade(&runtime);
        fiber.cleanups.borrow_mut().push(cleanup(async move {
            let result = host
                .upgrade()
                .unwrap()
                .try_dispose(owner.upgrade().unwrap());
            assert_eq!(
                result.err(),
                Some("same-fiber lifecycle request during cleanup")
            );
            other_host
                .upgrade()
                .unwrap()
                .try_dispose(other_fiber)
                .unwrap()
                .await
                .unwrap();
            Ok(())
        }));
        runtime.dispose(fiber.clone()).await.unwrap();
        assert_eq!(fiber.state.get(), State::Disposed);
        assert!(runtime.errors.borrow().is_empty());
        runtime.dispose(runtime.root()).await.unwrap();
        other_runtime.dispose(other_runtime.root()).await.unwrap();
    })
    .await;
}
