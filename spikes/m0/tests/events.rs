use cordis_m0_spike::{LocalFuture, Next, SyncNext};
use futures::future::join_all;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::rc::Rc;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, LocalSet};

type Reply<R> = Result<ControlFlow<R>, &'static str>;
type Handler<A, R> = Rc<dyn Fn(&Event<A, R>, A) -> Invocation<R>>;

enum Invocation<R> {
    Immediate(Reply<R>),
    Deferred(LocalFuture<Reply<R>>),
}

struct Hook<A, R> {
    id: u64,
    context: u32,
    global: bool,
    once: bool,
    asynchronous: bool,
    handler: Handler<A, R>,
}

impl<A, R> Clone for Hook<A, R> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            ..*self
        }
    }
}

struct Store<A, R> {
    next: Cell<u64>,
    hooks: RefCell<Vec<Hook<A, R>>>,
    errors: Rc<RefCell<Vec<&'static str>>>,
    tasks: RefCell<Vec<JoinHandle<()>>>,
}

struct Event<A, R>(Rc<Store<A, R>>);

impl<A, R> Clone for Event<A, R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: Clone + 'static, R: 'static> Event<A, R> {
    fn new() -> Self {
        Self(Rc::new(Store {
            next: Cell::new(0),
            hooks: RefCell::new(Vec::new()),
            errors: Rc::new(RefCell::new(Vec::new())),
            tasks: RefCell::new(Vec::new()),
        }))
    }

    fn on(
        &self,
        handler: Handler<A, R>,
        asynchronous: bool,
        prepend: bool,
        once: bool,
        context: u32,
        global: bool,
    ) -> u64 {
        let id = self.0.next.get();
        self.0.next.set(id + 1);
        let hook = Hook {
            id,
            handler,
            context,
            global,
            once,
            asynchronous,
        };
        if prepend {
            self.0.hooks.borrow_mut().insert(0, hook)
        } else {
            self.0.hooks.borrow_mut().push(hook)
        }
        id
    }

    fn remove(&self, id: u64) {
        let removed = {
            let mut hooks = self.0.hooks.borrow_mut();
            hooks
                .iter()
                .position(|hook| hook.id == id)
                .map(|index| hooks.remove(index))
        };
        drop(removed);
    }

    fn snapshot(&self, filter: Option<&dyn Fn(u32) -> bool>) -> Vec<Hook<A, R>> {
        self.0
            .hooks
            .borrow()
            .iter()
            .filter(|hook| hook.global || filter.is_none_or(|filter| filter(hook.context)))
            .cloned()
            .collect()
    }

    fn invoke(&self, hook: &Hook<A, R>, args: A) -> Invocation<R> {
        if hook.once {
            self.remove(hook.id)
        }
        (hook.handler)(self, args)
    }

    fn emit(&self, args: A, filter: Option<&dyn Fn(u32) -> bool>) -> Result<(), &'static str> {
        for hook in self.snapshot(filter) {
            match self.invoke(&hook, args.clone()) {
                Invocation::Immediate(result) => {
                    let _ = result?;
                }
                Invocation::Deferred(future) => {
                    let errors = self.0.errors.clone();
                    self.0
                        .tasks
                        .borrow_mut()
                        .push(tokio::task::spawn_local(async move {
                            if let Err(error) = future.await {
                                errors.borrow_mut().push(error)
                            }
                        }));
                }
            }
        }
        Ok(())
    }

    async fn settle(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.tasks.borrow_mut());
            if tasks.is_empty() {
                return;
            }
            for outcome in join_all(tasks).await {
                outcome.expect("event task panicked");
            }
        }
    }

    fn bail(&self, args: A) -> Reply<R> {
        let hooks = self.snapshot(None);
        if hooks.iter().any(|hook| hook.asynchronous) {
            return Err("async listener requires serial");
        }
        for hook in hooks {
            match self.invoke(&hook, args.clone()) {
                Invocation::Immediate(result) => {
                    if let ControlFlow::Break(value) = result? {
                        return Ok(ControlFlow::Break(value));
                    }
                }
                Invocation::Deferred(_) => panic!("sync hook returned async invocation"),
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn serial(&self, args: A) -> Reply<R> {
        for hook in self.snapshot(None) {
            let result = match self.invoke(&hook, args.clone()) {
                Invocation::Immediate(result) => result,
                Invocation::Deferred(future) => future.await,
            };
            if let ControlFlow::Break(value) = result? {
                return Ok(ControlFlow::Break(value));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn parallel(&self, args: A) -> Result<(), Vec<&'static str>> {
        let invocations: Vec<_> = self
            .snapshot(None)
            .iter()
            .map(|hook| self.invoke(hook, args.clone()))
            .collect();
        let outcomes = join_all(invocations.into_iter().map(|invocation| async {
            match invocation {
                Invocation::Immediate(result) => result,
                Invocation::Deferred(future) => future.await,
            }
        }))
        .await;
        let errors: Vec<_> = outcomes.into_iter().filter_map(Result::err).collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[derive(Default)]
struct Contracts(RefCell<BTreeMap<&'static str, Rc<dyn Any>>>);

impl Contracts {
    fn named<A: Clone + 'static, R: 'static>(
        &self,
        name: &'static str,
    ) -> Result<Event<A, R>, &'static str> {
        let mut contracts = self.0.borrow_mut();
        let stored = contracts
            .entry(name)
            .or_insert_with(|| Rc::new(Event::<A, R>::new()));
        stored
            .downcast_ref::<Event<A, R>>()
            .cloned()
            .ok_or("event contract mismatch")
    }
}

type Around =
    Rc<dyn Fn(u32, Next<Result<u32, &'static str>>) -> LocalFuture<Result<u32, &'static str>>>;
type SyncAround = Rc<dyn Fn(u32, SyncNext<u32>) -> u32>;

fn waterfall(
    hooks: Vec<Around>,
    value: u32,
    tail: Next<Result<u32, &'static str>>,
) -> LocalFuture<Result<u32, &'static str>> {
    hooks
        .into_iter()
        .rev()
        .fold(tail, |next, hook| Next::new(move || hook(value, next)))
        .call()
}

fn sync_waterfall(hooks: Vec<SyncAround>, value: u32, tail: SyncNext<u32>) -> u32 {
    hooks
        .into_iter()
        .rev()
        .fold(tail, |next, hook| SyncNext::new(move || hook(value, next)))
        .call()
}

#[test]
fn emit_is_immediate_snapshot_based_filtered_and_once_reentrant() {
    let event = Event::<(), ()>::new();
    let trace = Rc::new(RefCell::new(Vec::new()));
    let victim = Rc::new(Cell::new(0));
    event.on(
        Rc::new({
            let trace = trace.clone();
            let victim = victim.clone();
            move |event, ()| {
                trace.borrow_mut().push("first");
                event.remove(victim.get());
                Invocation::Immediate(Ok(ControlFlow::Continue(())))
            }
        }),
        false,
        false,
        false,
        1,
        false,
    );
    victim.set(event.on(
        Rc::new({
            let trace = trace.clone();
            move |_, ()| {
                trace.borrow_mut().push("selected");
                Invocation::Immediate(Ok(ControlFlow::Continue(())))
            }
        }),
        false,
        false,
        false,
        1,
        false,
    ));
    event.emit((), None).unwrap();
    assert_eq!(*trace.borrow(), ["first", "selected"]);
    trace.borrow_mut().clear();
    event.emit((), None).unwrap();
    assert_eq!(*trace.borrow(), ["first"]);
    event.on(
        Rc::new({
            let trace = trace.clone();
            move |_, ()| {
                trace.borrow_mut().push("global");
                Invocation::Immediate(Ok(ControlFlow::Continue(())))
            }
        }),
        false,
        true,
        false,
        2,
        true,
    );
    trace.borrow_mut().clear();
    event.emit((), Some(&|_| false)).unwrap();
    assert_eq!(*trace.borrow(), ["global"]);
    trace.borrow_mut().clear();
    event.emit((), None).unwrap();
    assert_eq!(*trace.borrow(), ["global", "first"]);

    let recursive = Event::<(), ()>::new();
    let calls = Rc::new(Cell::new(0));
    recursive.on(
        Rc::new({
            let calls = calls.clone();
            move |event, ()| {
                calls.set(calls.get() + 1);
                event.emit((), None).unwrap();
                Invocation::Immediate(Ok(ControlFlow::Continue(())))
            }
        }),
        false,
        false,
        true,
        0,
        false,
    );
    recursive.emit((), None).unwrap();
    assert_eq!(calls.get(), 1);

    struct ReentrantDrop(Event<(), ()>);
    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            self.0.remove(u64::MAX);
        }
    }
    let removable = Event::<(), ()>::new();
    let guard = ReentrantDrop(removable.clone());
    let id = removable.on(
        Rc::new(move |_, ()| {
            let _keep_guard = &guard;
            Invocation::Immediate(Ok(ControlFlow::Continue(())))
        }),
        false,
        false,
        false,
        0,
        false,
    );
    removable.remove(id);
    assert!(removable.0.hooks.borrow().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn split_async_emit_starts_prefix_now_and_reports_detached_failure() {
    LocalSet::new()
        .run_until(async {
            let event = Event::<(), ()>::new();
            let prefix = Rc::new(Cell::new(false));
            event.on(
                Rc::new({
                    let prefix = prefix.clone();
                    move |_, ()| {
                        prefix.set(true);
                        Invocation::Deferred(Box::pin(async { Err("async failure") }))
                    }
                }),
                true,
                false,
                false,
                0,
                false,
            );
            event.emit((), None).unwrap();
            assert!(prefix.get());
            event.settle().await;
            assert_eq!(*event.0.errors.borrow(), ["async failure"]);
            assert_eq!(event.bail(()), Err("async listener requires serial"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_waits_for_all_failures_and_serial_bails_on_zero() {
    let event = Event::<(), u32>::new();
    event.on(
        Rc::new(|_, ()| Invocation::Immediate(Err("sync failure"))),
        false,
        false,
        false,
        0,
        false,
    );
    let (release_tx, release_rx) = oneshot::channel();
    let gate = Rc::new(RefCell::new(Some(release_rx)));
    event.on(
        Rc::new(move |_, ()| {
            let release = gate.borrow_mut().take().unwrap();
            Invocation::Deferred(Box::pin(async {
                release.await.unwrap();
                Err("async failure")
            }))
        }),
        true,
        false,
        false,
        0,
        false,
    );
    let parallel = event.parallel(());
    tokio::pin!(parallel);
    assert!(futures::poll!(&mut parallel).is_pending());
    release_tx.send(()).unwrap();
    assert_eq!(parallel.await, Err(vec!["sync failure", "async failure"]));

    let serial = Event::<(), u32>::new();
    serial.on(
        Rc::new(|_, ()| Invocation::Deferred(Box::pin(async { Ok(ControlFlow::Continue(())) }))),
        true,
        false,
        false,
        0,
        false,
    );
    serial.on(
        Rc::new(|_, ()| Invocation::Immediate(Ok(ControlFlow::Break(0)))),
        false,
        false,
        false,
        0,
        false,
    );
    serial.on(
        Rc::new(|_, ()| panic!("called after bail")),
        false,
        false,
        false,
        0,
        false,
    );
    assert_eq!(serial.serial(()).await, Ok(ControlFlow::Break(0)));
    let bail = Event::<(), String>::new();
    bail.on(
        Rc::new(|_, ()| Invocation::Immediate(Ok(ControlFlow::Break(String::new())))),
        false,
        false,
        false,
        0,
        false,
    );
    assert_eq!(bail.bail(()), Ok(ControlFlow::Break(String::new())));
}

#[test]
fn synchronous_error_stops_emit_and_named_contracts_are_checked() {
    let event = Event::<(), ()>::new();
    event.on(
        Rc::new(|_, ()| Invocation::Immediate(Err("stop"))),
        false,
        false,
        false,
        0,
        false,
    );
    event.on(
        Rc::new(|_, ()| panic!("called after error")),
        false,
        false,
        false,
        0,
        false,
    );
    assert_eq!(event.emit((), None), Err("stop"));
    let contracts = Contracts::default();
    let a = contracts.named::<u32, ()>("ping").unwrap();
    let b = contracts.named::<u32, ()>("ping").unwrap();
    assert!(Rc::ptr_eq(&a.0, &b.0));
    assert!(matches!(
        contracts.named::<String, ()>("ping"),
        Err("event contract mismatch")
    ));
    let first_symbol = Event::<u32, ()>::new();
    let second_symbol = Event::<u32, ()>::new();
    assert!(!Rc::ptr_eq(&first_symbol.0, &second_symbol.0));
}

#[tokio::test(flavor = "current_thread")]
async fn waterfall_is_around_middleware_can_veto_and_supports_nested_async_calls() {
    let trace = Rc::new(RefCell::new(Vec::new()));
    let outer: Around = Rc::new({
        let trace = trace.clone();
        move |value, next| {
            let trace = trace.clone();
            Box::pin(async move {
                trace.borrow_mut().push("before");
                let result = next.call().await?;
                trace.borrow_mut().push("after");
                Ok(value + result)
            })
        }
    });
    let inner: Around = Rc::new(|value, next| {
        Box::pin(async move {
            Ok(value
                + next.call().await?
                + waterfall(Vec::new(), 0, Next::new(|| async { Ok(2) })).await?)
        })
    });
    assert_eq!(
        waterfall(vec![outer, inner], 1, Next::new(|| async { Ok(2) })).await,
        Ok(6)
    );
    assert_eq!(*trace.borrow(), ["before", "after"]);
    let veto: Around = Rc::new(|_, _next| Box::pin(async { Ok(9) }));
    assert_eq!(
        waterfall(vec![veto], 1, Next::new(|| async { panic!("vetoed tail") })).await,
        Ok(9)
    );
    let sync: SyncAround = Rc::new(|value, next| value + next.call());
    assert_eq!(
        sync_waterfall(vec![sync.clone(), sync], 1, SyncNext::new(|| 2)),
        4
    );
}
