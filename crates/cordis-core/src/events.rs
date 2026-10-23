use crate::{
    AdmissionError, BoxError, CleanupFailure, Context, Effect, LocalFuture, effect, runtime::State,
};
use futures_util::future::join_all;
use std::{
    any::Any,
    cell::RefCell,
    error::Error,
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::ControlFlow,
    pin::Pin,
    rc::Rc,
    task::{Context as TaskContext, Poll},
};
use tokio::sync::oneshot;

#[derive(Clone)]
pub(crate) enum Key {
    Named(String),
    Symbol(Rc<()>, String),
}
impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Named(a), Self::Named(b)) => a == b,
            (Self::Symbol(a, _), Self::Symbol(b, _)) => Rc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Eq for Key {}
impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Named(name) => name.hash(state),
            Self::Symbol(id, _) => std::ptr::hash(Rc::as_ptr(id), state),
        }
    }
}
impl Key {
    fn label(&self) -> String {
        match self {
            Self::Named(name) => format!("{name:?}"),
            Self::Symbol(_, name) => format!("Symbol({name})"),
        }
    }
}

/// A typed event identity, independent from any runtime. Named keys share a
/// checked argument/result contract; symbol keys share only when cloned.
/// ```compile_fail
/// use cordis_core::{Runtime, Event, DispatchOptions};
/// let runtime = Runtime::new();
/// let event = Event::<u32, String>::symbol("ready");
/// let _ = runtime.context().bail(&event, "wrong argument", DispatchOptions::default());
/// ```
///
/// ```
/// use cordis_core::{DispatchOptions, Event, Listener, Runtime};
/// use std::{convert::Infallible, ops::ControlFlow};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let executor = tokio::runtime::Builder::new_current_thread().build()?;
/// executor.block_on(async {
///     let runtime = Runtime::new();
///     runtime.run(async {
///         let ctx = runtime.context();
///         let ready = Event::<u32, String>::symbol("ready");
///         ctx.on(&ready, Listener::sync(|_, count: u32| {
///             Ok::<_, Infallible>(ControlFlow::Break(count.to_string()))
///         }))?;
///         if let ControlFlow::Break(answer) = ctx.bail(&ready, 7, DispatchOptions::default())? {
///             assert_eq!(answer.to_uppercase(), "7");
///         } else { panic!("missing typed answer"); }
///         ctx.drain()?.await?;
///         Ok::<_, Box<dyn std::error::Error>>(())
///     }).await??;
///     Ok::<_, Box<dyn std::error::Error>>(())
/// })
/// # }
/// ```
pub struct Event<A, R = ()> {
    key: Key,
    marker: PhantomData<fn(A) -> R>,
}
impl<A, R> Clone for Event<A, R> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            marker: PhantomData,
        }
    }
}
impl<A, R> Event<A, R> {
    /// Select a string-keyed event. All names, including prototype-like names, are ordinary strings.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            key: Key::Named(name.into()),
            marker: PhantomData,
        }
    }
    /// Allocate a new symbol-like key. Equal descriptions do not imply equality.
    pub fn symbol(description: impl Into<String>) -> Self {
        Self {
            key: Key::Symbol(Rc::new(()), description.into()),
            marker: PhantomData,
        }
    }
}

/// Listener placement. Global listeners bypass dispatch filters.
#[derive(Clone, Copy, Debug, Default)]
pub struct ListenerOptions {
    /// Insert before existing listeners.
    pub prepend: bool,
    /// Receive dispatch even when the supplied filter rejects this context.
    pub global: bool,
}

type Filter = Rc<dyn Fn(&Context) -> bool>;
type Snapshot<A, R> = (Rc<State>, Vec<Hook<A, R>>);
/// Snapshot selection for one dispatch. No automatic ancestry routing is implied.
#[derive(Clone, Default)]
pub struct DispatchOptions {
    filter: Option<Filter>,
    fiber: Option<crate::FiberId>,
}
impl DispatchOptions {
    pub(crate) fn for_fiber(id: crate::FiberId) -> Self {
        Self {
            filter: None,
            fiber: Some(id),
        }
    }
    /// Filter against each listener's registration context; global hooks bypass it.
    pub fn filtered(filter: impl Fn(&Context) -> bool + 'static) -> Self {
        Self {
            filter: Some(Rc::new(filter)),
            fiber: None,
        }
    }
}

type Reply<R> = Result<ControlFlow<R>, CleanupFailure>;
type Sync<A, R> = dyn Fn(Context, A) -> Reply<R>;
type Async<A, R> = dyn Fn(Context, A) -> LocalFuture<Reply<R>>;
type Around<A, R> = dyn Fn(Context, A, Next<R>) -> Result<R, CleanupFailure>;
type AsyncAround<A, R> = dyn Fn(Context, A, AsyncNext<R>) -> LocalFuture<Result<R, CleanupFailure>>;
enum Kind<A, R> {
    Sync(Rc<Sync<A, R>>),
    Async(Rc<Async<A, R>>),
    Around(Rc<Around<A, R>>),
    AsyncAround(Rc<AsyncAround<A, R>>),
}

/// Explicit callback shape. Sync and async responders support emit/parallel/
/// serial; bail requires sync responders; waterfall requires around callbacks.
///
/// ```compile_fail
/// use cordis_core::{Event, Listener, Runtime};
/// use std::{convert::Infallible, ops::ControlFlow};
/// let event = Event::<u32, String>::symbol("ready");
/// let _ = Runtime::new().context().on(&event, Listener::sync(|_, count: String| {
///     Ok::<_, Infallible>(ControlFlow::Break(count))
/// }));
/// ```
///
/// ```compile_fail
/// use cordis_core::{Event, Listener, Runtime};
/// use std::{convert::Infallible, ops::ControlFlow};
/// let event = Event::<u32, String>::symbol("ready");
/// let _ = Runtime::new().context().on(&event, Listener::sync(|_, _: u32| {
///     Ok::<_, Infallible>(ControlFlow::Break(42_u32))
/// }));
/// ```
pub struct Listener<A, R>(Rc<Kind<A, R>>);
impl<A, R> Clone for Listener<A, R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<A: 'static, R: 'static> Listener<A, R> {
    /// Build a synchronous notification/query callback. Continue means no answer.
    pub fn sync<F, E>(callback: F) -> Self
    where
        F: Fn(Context, A) -> Result<ControlFlow<R>, E> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self(Rc::new(Kind::Sync(Rc::new(move |ctx, args| {
            effect::invoke(|| callback(ctx, args).map_err(Into::into))
        }))))
    }
    /// Build an async responder with an explicit eager prefix: the factory runs
    /// during invocation, its returned future is awaited/scheduled separately.
    pub fn asynchronous<F, Fut, E>(callback: F) -> Self
    where
        F: Fn(Context, A) -> Fut + 'static,
        Fut: Future<Output = Result<ControlFlow<R>, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self(Rc::new(Kind::Async(Rc::new(move |ctx, args| {
            let future = effect::invoke(|| Ok(callback(ctx, args)));
            Box::pin(async move {
                match future {
                    Ok(future) => {
                        effect::invoke_async(async { future.await.map_err(Into::into) }).await
                    }
                    Err(failure) => Err(failure),
                }
            })
        }))))
    }
    /// Build synchronous around-middleware. Omitting next vetoes the remainder.
    pub fn around<F, E>(callback: F) -> Self
    where
        F: Fn(Context, A, Next<R>) -> Result<R, E> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self(Rc::new(Kind::Around(Rc::new(move |ctx, args, next| {
            effect::invoke(|| callback(ctx, args, next).map_err(Into::into))
        }))))
    }
    /// Build asynchronous around-middleware with an owned, one-use next.
    pub fn around_async<F, Fut, E>(callback: F) -> Self
    where
        F: Fn(Context, A, AsyncNext<R>) -> Fut + 'static,
        Fut: Future<Output = Result<R, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self(Rc::new(Kind::AsyncAround(Rc::new(
            move |ctx, args, next| {
                let future = effect::invoke(|| Ok(callback(ctx, args, next)));
                Box::pin(async move {
                    match future {
                        Ok(future) => {
                            effect::invoke_async(async { future.await.map_err(Into::into) }).await
                        }
                        Err(failure) => Err(failure),
                    }
                })
            },
        ))))
    }
}

/// A single-use synchronous continuation.
/// ```compile_fail
/// use cordis_core::Next;
/// fn twice(next: Next<u32>) { let _ = next.call(); let _ = next.call(); }
/// ```
pub struct Next<R>(Box<dyn FnOnce() -> Result<R, EventError>>);
impl<R> Next<R> {
    /// Consume this continuation exactly once.
    pub fn call(self) -> Result<R, EventError> {
        (self.0)()
    }
}
/// A single-use continuation for async-around listeners using owned local futures.
/// Sync and async waterfall chains have explicit, separate callback shapes.
/// ```compile_fail
/// use cordis_core::AsyncNext;
/// fn twice(next: AsyncNext<u32>) { let _ = next.call(); let _ = next.call(); }
/// ```
pub struct AsyncNext<R>(Box<dyn FnOnce() -> LocalFuture<Result<R, EventError>>>);
impl<R> AsyncNext<R> {
    /// Consume this continuation, producing the next frame's future.
    pub fn call(self) -> LocalFuture<Result<R, EventError>> {
        (self.0)()
    }
}

/// An event registration, removed synchronously by remove or automatically by
/// its owner's cleanup. Drop is inert, matching other resource registrations.
pub struct ListenerRegistration {
    effect: Effect,
    key: Key,
    id: u64,
}
impl ListenerRegistration {
    /// Remove this exact listener now. Already-selected dispatch callbacks remain selected.
    pub fn remove(self) -> Result<bool, AdmissionError> {
        let state = self
            .effect
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        let owner = self.effect.owner.upgrade();
        let removed = remove(&state, &self.key, self.id);
        let cleanup = owner.and_then(|owner| owner.cleanups.borrow_mut().remove(&self.effect.id));
        drop(cleanup);
        Ok(removed)
    }
    /// Transfer this Fiber's cleanup into a composed effect with Disposer::from_effect.
    pub fn into_effect(self) -> Effect {
        self.effect
    }
}

struct Hook<A, R> {
    id: u64,
    cleanup_id: u64,
    context: Context,
    options: ListenerOptions,
    once: bool,
    listener: Rc<Kind<A, R>>,
}
impl<A, R> Clone for Hook<A, R> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            cleanup_id: self.cleanup_id,
            context: self.context.clone(),
            options: self.options,
            once: self.once,
            listener: self.listener.clone(),
        }
    }
}
trait ErasedBucket: Any {
    fn as_any(&self) -> &dyn Any;
    fn remove(&self, id: u64) -> bool;
    fn is_empty(&self) -> bool;
}
struct Bucket<A, R>(RefCell<Vec<Hook<A, R>>>);
impl<A: 'static, R: 'static> ErasedBucket for Bucket<A, R> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn remove(&self, id: u64) -> bool {
        let removed = {
            let mut hooks = self.0.borrow_mut();
            hooks
                .iter()
                .position(|h| h.id == id)
                .map(|i| hooks.remove(i))
        };
        let found = removed.is_some();
        drop(removed);
        found
    }
    fn is_empty(&self) -> bool {
        self.0.borrow().is_empty()
    }
}
#[derive(Default)]
pub(crate) struct EventStore(RefCell<std::collections::HashMap<Key, Rc<dyn ErasedBucket>>>);

fn remove(state: &State, key: &Key, id: u64) -> bool {
    let bucket = state.events.0.borrow().get(key).cloned();
    let Some(bucket) = bucket else { return false };
    let removed = bucket.remove(id);
    if bucket.is_empty() {
        let stale = {
            let mut buckets = state.events.0.borrow_mut();
            if buckets
                .get(key)
                .is_some_and(|current| Rc::ptr_eq(current, &bucket))
            {
                buckets.remove(key)
            } else {
                None
            }
        };
        drop(stale);
    }
    removed
}

impl crate::hooks::ListenerRequest {
    /// Substitute a listener of the original event's exact contract. The new
    /// registration still belongs to the original registering Fiber.
    pub fn replace<A: 'static, R: 'static>(
        &self,
        listener: Listener<A, R>,
    ) -> Result<ListenerRegistration, EventError> {
        self.replace_with(listener, self.options)
    }
    /// Substitute a listener and its prepend/global options.
    pub fn replace_with<A: 'static, R: 'static>(
        &self,
        listener: Listener<A, R>,
        options: ListenerOptions,
    ) -> Result<ListenerRegistration, EventError> {
        let event = self
            .event
            .get::<Event<A, R>>()
            .map_err(|error| EventError::ContractMismatch(error.to_string()))?;
        self.context
            .register_event_raw(&event, listener, options, self.once)
    }
}

/// Event admission, type/mode mismatch, callback failure, or interrupted worker.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum EventError {
    /// No event work was admitted.
    Admission(AdmissionError),
    /// An existing name has a different argument/result contract.
    ContractMismatch(String),
    /// The snapshot contains callbacks incompatible with the selected mode.
    UnsupportedMode {
        /// Diagnostic event label.
        event: String,
        /// Dispatch mode refusing the listener shape.
        mode: &'static str,
    },
    /// One or more callbacks failed. Parallel retains every failure in snapshot order.
    Callbacks {
        /// Diagnostic event label.
        event: String,
        /// Original callback sources, including caught panics.
        failures: Vec<CleanupFailure>,
    },
    /// The host was destroyed before dispatch completion.
    HostStopped,
    /// Framework dispatch work panicked outside callback containment.
    DriverPanicked,
}
impl From<AdmissionError> for EventError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
    }
}
impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(e) => e.fmt(f),
            Self::ContractMismatch(name) => write!(f, "event {name} contract mismatch"),
            Self::UnsupportedMode { event, mode } => {
                write!(f, "event {event} has a listener incompatible with {mode}")
            }
            Self::Callbacks { event, failures } => {
                write!(f, "{} listener(s) of event {event} failed", failures.len())
            }
            Self::HostStopped => f.write_str("host stopped before event completion"),
            Self::DriverPanicked => f.write_str("event driver panicked"),
        }
    }
}
impl Error for EventError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(e) => Some(e),
            Self::Callbacks { failures, .. } => failures.first().map(|f| f as &dyn Error),
            _ => None,
        }
    }
}
fn failed(key: &Key, failure: CleanupFailure) -> EventError {
    EventError::Callbacks {
        event: key.label(),
        failures: vec![failure],
    }
}
fn report(state: &State, error: &EventError) {
    if let EventError::Callbacks { failures, .. } = error {
        for failure in failures {
            state.report(failure.clone());
        }
    }
}

/// A wait-only handle to dispatch started at the method call. Dropping this
/// handle never cancels work; abandoned callback failures are diagnosed.
#[must_use = "dispatch has started; await completion or explicitly drop the waiter"]
pub struct EventTask<T>(LocalFuture<Result<T, EventError>>);
impl<T> Future for EventTask<T> {
    type Output = Result<T, EventError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
fn start<T: 'static>(
    state: &Rc<State>,
    future: impl Future<Output = Result<T, EventError>> + 'static,
    always_report: bool,
) -> EventTask<T> {
    let host = state
        .admit()
        .expect("dispatch was admitted before callback invocation");
    let task = state.spawn(&host, future);
    let (sender, receiver) = oneshot::channel();
    let owner = state.clone();
    state.spawn(&host, async move {
        let outcome = match task.await {
            Ok(result) => result,
            Err(error) => {
                eprintln!("cordis: event driver did not complete: {error}");
                Err(if error.is_panic() {
                    EventError::DriverPanicked
                } else {
                    EventError::HostStopped
                })
            }
        };
        if always_report {
            if let Err(error) = &outcome {
                report(&owner, error);
            }
        }
        if let Err(Err(error)) = sender.send(outcome) {
            if !always_report {
                report(&owner, &error);
            }
        }
    });
    EventTask(Box::pin(async move {
        receiver.await.map_err(|_| EventError::HostStopped)?
    }))
}

impl Context {
    /// Register a responder or around listener with default options.
    pub fn on<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        listener: Listener<A, R>,
    ) -> Result<ListenerRegistration, EventError> {
        self.register_event(event, listener, ListenerOptions::default(), false)
    }
    /// Register with explicit prepend/global options.
    pub fn on_with<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        listener: Listener<A, R>,
        options: ListenerOptions,
    ) -> Result<ListenerRegistration, EventError> {
        self.register_event(event, listener, options, false)
    }
    /// Remove before invocation, including recursive dispatch. Existing snapshots
    /// still retain their selected callback, matching the TS resolver.
    pub fn once<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        listener: Listener<A, R>,
    ) -> Result<ListenerRegistration, EventError> {
        self.register_event(event, listener, ListenerOptions::default(), true)
    }
    fn register_event<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        listener: Listener<A, R>,
        options: ListenerOptions,
        once: bool,
    ) -> Result<ListenerRegistration, EventError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.owner()?.admit()?;
        if !matches!(&event.key, Key::Named(name) if name == "internal/listener") {
            let request = crate::hooks::ListenerRequest {
                context: self.clone(),
                event: crate::hooks::Value::new(event.clone()),
                listener: crate::hooks::Value::new(listener.clone()),
                options,
                once,
            };
            if let ControlFlow::Break(replacement) = self.bail(
                &crate::hooks::listener(),
                request,
                DispatchOptions::default(),
            )? {
                return Ok(replacement);
            }
        }
        self.register_event_raw(event, listener, options, once)
    }
    fn register_event_raw<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        listener: Listener<A, R>,
        options: ListenerOptions,
        once: bool,
    ) -> Result<ListenerRegistration, EventError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.owner()?.admit()?;
        let existing = state.events.0.borrow().get(&event.key).cloned();
        let bucket: Rc<dyn ErasedBucket> =
            existing.unwrap_or_else(|| Rc::new(Bucket::<A, R>(RefCell::new(Vec::new()))));
        let typed = bucket
            .as_any()
            .downcast_ref::<Bucket<A, R>>()
            .ok_or_else(|| EventError::ContractMismatch(event.key.label()))?;
        let id = state
            .next
            .get()
            .checked_add(1)
            .ok_or(AdmissionError::IdentityExhausted)?;
        state.next.set(id);
        let effect = self
            .on_dispose_sync({
                let state = Rc::downgrade(&state);
                let key = event.key.clone();
                move || {
                    if let Some(state) = state.upgrade() {
                        remove(&state, &key, id);
                    }
                    Ok::<_, std::convert::Infallible>(())
                }
            })?
            .with_label(format!("ctx.on({})", event.key.label()));
        let hook = Hook {
            id,
            cleanup_id: effect.id,
            context: self.clone(),
            options,
            once,
            listener: listener.0,
        };
        if options.prepend {
            typed.0.borrow_mut().insert(0, hook);
        } else {
            typed.0.borrow_mut().push(hook);
        }
        state
            .events
            .0
            .borrow_mut()
            .insert(event.key.clone(), bucket);
        Ok(ListenerRegistration {
            effect,
            key: event.key.clone(),
            id,
        })
    }
    fn event_snapshot<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        options: DispatchOptions,
    ) -> Result<Snapshot<A, R>, EventError> {
        self.select_hooks(event, options, true)
    }
    fn select_hooks<A: 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        options: DispatchOptions,
        hosted: bool,
    ) -> Result<Snapshot<A, R>, EventError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        if hosted {
            state.admit()?;
        }
        let bucket = state.events.0.borrow().get(&event.key).cloned();
        let hooks = match bucket {
            None => Vec::new(),
            Some(bucket) => bucket
                .as_any()
                .downcast_ref::<Bucket<A, R>>()
                .ok_or_else(|| EventError::ContractMismatch(event.key.label()))?
                .0
                .borrow()
                .clone(),
        };
        let mut selected = Vec::new();
        for hook in hooks {
            if options
                .fiber
                .as_ref()
                .is_some_and(|fiber| !hook.options.global && hook.context.fiber_id() != fiber)
            {
                continue;
            }
            let admitted = effect::invoke(|| {
                Ok(hook.options.global
                    || options
                        .filter
                        .as_ref()
                        .is_none_or(|filter| filter(&hook.context)))
            })
            .map_err(|failure| failed(&event.key, failure))?;
            if admitted {
                selected.push(hook);
            }
        }
        if options.fiber.is_some() {
            selected.sort_by_key(|hook| {
                if hook.options.global && hook.options.prepend {
                    0
                } else if !hook.options.global {
                    1
                } else {
                    2
                }
            });
        }
        Ok((state, selected))
    }

    fn dispatch_hook<A: Clone + 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        args: &A,
        mode: &'static str,
    ) -> Result<(), EventError> {
        if matches!(&event.key, Key::Named(name) if name.starts_with("internal/")) {
            return Ok(());
        }
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        crate::hooks::observe(
            self,
            &crate::hooks::dispatch(),
            crate::hooks::Dispatch {
                mode,
                event: crate::hooks::Value::new(event.clone()),
                arguments: crate::hooks::Value::new(args.clone()),
                context: self.use_site(),
            },
            DispatchOptions::default(),
        );
        Ok(())
    }

    /// Invoke sync callbacks and async factories immediately. Deferred callbacks
    /// run independently; their errors are logged even if the waiter is dropped.
    pub fn emit<A: Clone + 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
    ) -> Result<EventTask<()>, EventError> {
        self.dispatch_hook(event, &args, "emit")?;
        let (state, hooks) = self.event_snapshot(event, options)?;
        validate(&event.key, &hooks, "emit", |kind| {
            matches!(kind, Kind::Sync(_) | Kind::Async(_))
        })?;
        let mut tasks = Vec::new();
        for hook in hooks {
            claim_once(&state, &event.key, &hook);
            match &*hook.listener {
                Kind::Sync(callback) => {
                    let _ = callback(hook.context, args.clone())
                        .map_err(|failure| failed(&event.key, failure))?;
                }
                Kind::Async(callback) => {
                    let future = callback(hook.context, args.clone());
                    let key = event.key.clone();
                    tasks.push(start(
                        &state,
                        async move { future.await.map(|_| ()).map_err(|f| failed(&key, f)) },
                        true,
                    ));
                }
                _ => unreachable!("validated listener mode"),
            }
        }
        let key = event.key.clone();
        Ok(EventTask(Box::pin(async move {
            let mut failures = Vec::new();
            for outcome in join_all(tasks).await {
                match outcome {
                    Ok(()) => {}
                    Err(EventError::Callbacks {
                        failures: errors, ..
                    }) => failures.extend(errors),
                    Err(error) => return Err(error),
                }
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(EventError::Callbacks {
                    event: key.label(),
                    failures,
                })
            }
        })))
    }

    /// Synchronous ordered first-answer dispatch. Mixed async snapshots are refused before invocation.
    pub fn bail<A: Clone + 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
    ) -> Result<ControlFlow<R>, EventError> {
        self.dispatch_hook(event, &args, "bail")?;
        let (state, hooks) = self.event_snapshot(event, options)?;
        validate(&event.key, &hooks, "bail", |kind| {
            matches!(kind, Kind::Sync(_))
        })?;
        for hook in hooks {
            claim_once(&state, &event.key, &hook);
            let Kind::Sync(callback) = &*hook.listener else {
                unreachable!("validated sync listeners")
            };
            if let ControlFlow::Break(value) = callback(hook.context, args.clone())
                .map_err(|failure| failed(&event.key, failure))?
            {
                return Ok(ControlFlow::Break(value));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Start ordered async first-answer dispatch. Zero/empty values can be Break answers.
    pub fn serial<A: Clone + 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
    ) -> Result<EventTask<ControlFlow<R>>, EventError> {
        self.dispatch_hook(event, &args, "serial")?;
        let (state, hooks) = self.event_snapshot(event, options)?;
        validate(&event.key, &hooks, "serial", |kind| {
            matches!(kind, Kind::Sync(_) | Kind::Async(_))
        })?;
        let key = event.key.clone();
        let owner = state.clone();
        let mut hooks = hooks.into_iter();
        let first = hooks
            .next()
            .map(|hook| invoke_response(&state, &key, hook, args.clone()));
        Ok(start(
            &state,
            async move {
                if let Some(first) = first {
                    let result = first.await?;
                    if result.is_break() {
                        return Ok(result);
                    }
                }
                for hook in hooks {
                    let result = invoke_response(&owner, &key, hook, args.clone()).await?;
                    if result.is_break() {
                        return Ok(result);
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
            false,
        ))
    }

    /// Invoke every responder/factory now and await all outcomes, retaining all failures.
    pub fn parallel<A: Clone + 'static, R: 'static>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
    ) -> Result<EventTask<()>, EventError> {
        self.dispatch_hook(event, &args, "parallel")?;
        let (state, hooks) = self.event_snapshot(event, options)?;
        validate(&event.key, &hooks, "parallel", |kind| {
            matches!(kind, Kind::Sync(_) | Kind::Async(_))
        })?;
        let futures: Vec<_> = hooks
            .into_iter()
            .map(|hook| invoke_response(&state, &event.key, hook, args.clone()))
            .collect();
        let key = event.key.clone();
        Ok(start(
            &state,
            async move {
                let mut failures = Vec::new();
                for result in join_all(futures).await {
                    match result {
                        Ok(_) => {}
                        Err(EventError::Callbacks {
                            failures: errors, ..
                        }) => failures.extend(errors),
                        Err(error) => return Err(error),
                    }
                }
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(EventError::Callbacks {
                        event: key.label(),
                        failures,
                    })
                }
            },
            false,
        ))
    }

    /// Run a synchronous around chain with an owned terminal callback.
    pub fn waterfall<A: Clone + 'static, R: 'static, F, E>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
        tail: F,
    ) -> Result<R, EventError>
    where
        F: FnOnce() -> Result<R, E> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.dispatch_hook(event, &args, "waterfall")?;
        self.waterfall_read(event, args, options, tail, true)
    }

    pub(crate) fn waterfall_read<A: Clone + 'static, R: 'static, F, E>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
        tail: F,
        hosted: bool,
    ) -> Result<R, EventError>
    where
        F: FnOnce() -> Result<R, E> + 'static,
        E: Into<BoxError> + 'static,
    {
        let (state, hooks) = self.select_hooks(event, options, hosted)?;
        validate(&event.key, &hooks, "waterfall", |kind| {
            matches!(kind, Kind::Around(_))
        })?;
        let key = event.key.clone();
        let mut next = Next(Box::new(move || {
            effect::invoke(|| tail().map_err(Into::into)).map_err(|f| failed(&key, f))
        }));
        for hook in hooks.into_iter().rev() {
            let key = event.key.clone();
            let state = state.clone();
            let args = args.clone();
            next = Next(Box::new(move || {
                claim_once(&state, &key, &hook);
                let Kind::Around(callback) = &*hook.listener else {
                    unreachable!("validated middleware")
                };
                callback(hook.context, args, next).map_err(|f| failed(&key, f))
            }));
        }
        next.call()
    }

    /// Start async around-middleware. Async mode requires async-around listeners;
    /// use the explicit synchronous waterfall for synchronous chains.
    pub fn waterfall_async<A: Clone + 'static, R: 'static, F, Fut, E>(
        &self,
        event: &Event<A, R>,
        args: A,
        options: DispatchOptions,
        tail: F,
    ) -> Result<EventTask<R>, EventError>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = Result<R, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.dispatch_hook(event, &args, "waterfall")?;
        let (state, hooks) = self.event_snapshot(event, options)?;
        validate(&event.key, &hooks, "waterfall_async", |kind| {
            matches!(kind, Kind::AsyncAround(_))
        })?;
        let key = event.key.clone();
        let mut next = AsyncNext(Box::new(move || {
            Box::pin(async move {
                effect::invoke_async(async { tail().await.map_err(Into::into) })
                    .await
                    .map_err(|f| failed(&key, f))
            })
        }));
        for hook in hooks.into_iter().rev() {
            let key = event.key.clone();
            let state = state.clone();
            let args = args.clone();
            next = AsyncNext(Box::new(move || {
                claim_once(&state, &key, &hook);
                let Kind::AsyncAround(callback) = &*hook.listener else {
                    unreachable!("validated async middleware")
                };
                let future = callback(hook.context, args, next);
                Box::pin(async move { future.await.map_err(|f| failed(&key, f)) })
            }));
        }
        Ok(start(&state, next.call(), false))
    }
}

fn validate<A, R>(
    key: &Key,
    hooks: &[Hook<A, R>],
    mode: &'static str,
    accept: impl Fn(&Kind<A, R>) -> bool,
) -> Result<(), EventError> {
    if hooks.iter().all(|hook| accept(&hook.listener)) {
        Ok(())
    } else {
        Err(EventError::UnsupportedMode {
            event: key.label(),
            mode,
        })
    }
}
fn claim_once<A, R>(state: &State, key: &Key, hook: &Hook<A, R>) {
    if hook.once {
        remove(state, key, hook.id);
        if let Ok(owner) = hook.context.owner() {
            let callback = owner.cleanups.borrow_mut().remove(&hook.cleanup_id);
            drop(callback);
        }
    }
}
fn invoke_response<A: 'static, R: 'static>(
    state: &Rc<State>,
    key: &Key,
    hook: Hook<A, R>,
    args: A,
) -> LocalFuture<Result<ControlFlow<R>, EventError>> {
    claim_once(state, key, &hook);
    let key = key.clone();
    match &*hook.listener {
        Kind::Sync(callback) => {
            let result = callback(hook.context, args).map_err(|f| failed(&key, f));
            Box::pin(async move { result })
        }
        Kind::Async(callback) => {
            let future = callback(hook.context, args);
            Box::pin(async move { future.await.map_err(|f| failed(&key, f)) })
        }
        _ => unreachable!("validated responder mode"),
    }
}
