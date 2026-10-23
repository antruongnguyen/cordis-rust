use crate::{
    AdmissionError, BoxError, CleanupFailure, Context, Effect, FiberId, LocalFuture, effect,
    owner::Owner, runtime::State,
};
use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    error::Error,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context as TaskContext, Poll},
};
use tokio::sync::watch;

/// A Fiber's observable lifecycle state. Pending dependency resolution is M4.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FiberState {
    /// Waiting to begin, or (once services exist) waiting for dependencies.
    Pending,
    /// Validating configuration or applying the plugin.
    Loading,
    /// Apply completed successfully.
    Active,
    /// Apply/validation failed and its partial effects have been drained.
    Failed,
    /// Owned effects and children are being drained.
    Unloading,
    /// Terminal disposal completed.
    Disposed,
}

type Validate = dyn Fn(&dyn Any) -> Result<Box<dyn Any>, CleanupFailure>;
type Apply = dyn Fn(Context, Box<dyn Any>) -> LocalFuture<Result<(), CleanupFailure>>;
struct Definition {
    name: String,
    validate: Box<Validate>,
    apply: Box<Apply>,
}

/// A reusable typed plugin declaration. Clones share Registry identity; two new
/// declarations remain distinct even if they use the same Rust callback type.
/// Apply registers effects explicitly through its Context and returns Result.
///
/// ```
/// use cordis_core::{AdmissionError, FiberState, Plugin, Runtime};
/// use std::convert::Infallible;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let executor = tokio::runtime::Builder::new_current_thread().build()?;
/// executor.block_on(async {
///     let runtime = Runtime::new();
///     runtime.run(async {
///         let root = runtime.context();
///         let plugin = Plugin::new("worker", |ctx, count: u32| async move {
///             assert_eq!(count, 3);
///             let _resource = ctx.on_dispose_sync(|| Ok::<_, Infallible>(()))?;
///             Ok::<_, AdmissionError>(())
///         });
///         let fiber = root.plugin(&plugin, 3)?;
///         assert_eq!(fiber.ready()?.await?, FiberState::Active);
///         fiber.restart()?.await?;
///         assert_eq!(fiber.dispose()?.await?, FiberState::Disposed);
///         assert!(root.registry().is_empty()?);
///         Ok::<_, Box<dyn std::error::Error>>(())
///     }).await??;
///     Ok::<_, Box<dyn std::error::Error>>(())
/// })
/// # }
/// ```
///
/// ```compile_fail
/// use cordis_core::{Context, Plugin, Runtime};
/// use std::convert::Infallible;
/// let plugin = Plugin::new("typed", |_: Context, _: u32| async { Ok::<_, Infallible>(()) });
/// let _ = Runtime::new().context().plugin(&plugin, String::from("wrong config"));
/// ```
///
/// ```compile_fail
/// use cordis_core::Runtime;
/// let _ = Runtime::new().context().plugin(&"not a plugin declaration", ());
/// ```
pub struct Plugin<C, R = C> {
    definition: Rc<Definition>,
    marker: PhantomData<fn(C) -> R>,
}
impl<C, R> Clone for Plugin<C, R> {
    fn clone(&self) -> Self {
        Self {
            definition: self.definition.clone(),
            marker: PhantomData,
        }
    }
}
impl<C: Clone + 'static> Plugin<C, C> {
    /// Define a plugin with identity configuration validation.
    pub fn new<F, Fut, E>(name: impl Into<String>, apply: F) -> Self
    where
        F: Fn(Context, C) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self::validated(
            name,
            |config: &C| Ok::<_, Infallible>(config.clone()),
            apply,
        )
    }
}
impl<C: 'static, R: 'static> Plugin<C, R> {
    /// Define synchronous source-to-resolved validation and asynchronous apply.
    /// Validation runs on every load, not before lifecycle admission. Typed
    /// validation errors retain their original structured source.
    pub fn validated<V, VE, F, Fut, E>(name: impl Into<String>, validate: V, apply: F) -> Self
    where
        V: Fn(&C) -> Result<R, VE> + 'static,
        VE: Into<BoxError> + 'static,
        F: Fn(Context, R) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        let definition = Definition {
            name: name.into(),
            validate: Box::new(move |source| {
                let config = source
                    .downcast_ref::<C>()
                    .expect("source matches plugin contract");
                effect::invoke(|| {
                    validate(config)
                        .map(|resolved| Box::new(resolved) as Box<dyn Any>)
                        .map_err(Into::into)
                })
            }),
            apply: Box::new(move |ctx, resolved| {
                let resolved = *resolved
                    .downcast::<R>()
                    .expect("resolved config matches plugin contract");
                let future = effect::invoke(|| Ok(apply(ctx, resolved)));
                Box::pin(async move {
                    match future {
                        Ok(future) => {
                            effect::invoke_async(async { future.await.map_err(Into::into) }).await
                        }
                        Err(failure) => Err(failure),
                    }
                })
            }),
        };
        Self {
            definition: Rc::new(definition),
            marker: PhantomData,
        }
    }
    fn key(&self) -> usize {
        Rc::as_ptr(&self.definition) as usize
    }
}

/// Admission, user callback, recursion, or worker failure from a Fiber operation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum LifecycleError {
    /// No operation was admitted.
    Admission(AdmissionError),
    /// The latest load failed after partial effect cleanup.
    Callback(CleanupFailure),
    /// The host was destroyed before the requested state settled.
    HostStopped,
    /// A lifecycle driver failed outside user callback containment.
    DriverPanicked,
}
impl From<AdmissionError> for LifecycleError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
    }
}
impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(e) => e.fmt(f),
            Self::Callback(e) => write!(f, "plugin load: {e}"),
            Self::HostStopped => f.write_str("host stopped before Fiber settlement"),
            Self::DriverPanicked => f.write_str("Fiber lifecycle driver panicked"),
        }
    }
}
impl Error for LifecycleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(e) => Some(e),
            Self::Callback(e) => Some(e),
            _ => None,
        }
    }
}

type Outcome = Result<FiberState, LifecycleError>;
type Receiver = watch::Receiver<Option<Outcome>>;

/// Wait-only control for a started lifecycle pass. Dropping it never disposes
/// a Fiber or cancels its already-owned cleanup.
#[must_use = "lifecycle work has started; await settlement or explicitly drop the waiter"]
pub struct LifecycleTask(LocalFuture<Outcome>);
impl LifecycleTask {
    fn waiting(mut receiver: Receiver) -> Self {
        Self(Box::pin(async move {
            loop {
                if let Some(result) = receiver.borrow().clone() {
                    return result;
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| LifecycleError::HostStopped)?;
            }
        }))
    }
    fn resolved(result: Outcome) -> Self {
        Self(Box::pin(async move { result }))
    }
}
impl Future for LifecycleTask {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Outcome> {
        self.0.as_mut().poll(cx)
    }
}

struct Group {
    definition: Rc<Definition>,
    fibers: RefCell<BTreeMap<u64, Rc<Fiber>>>,
}
#[derive(Default)]
pub(crate) struct RegistryStore(RefCell<HashMap<usize, Rc<Group>>>);

struct Fiber {
    serial: u64,
    owner: Rc<Owner>,
    context: Context,
    group: Weak<Group>,
    definition: Rc<Definition>,
    source: Rc<dyn Any>,
    parent_effect: RefCell<Option<Effect>>,
    state: Cell<FiberState>,
    revision: Cell<u64>,
    settled_revision: Cell<u64>,
    loaded: Cell<bool>,
    failure: RefCell<Option<LifecycleError>>,
    driving: Cell<bool>,
    completion: RefCell<Option<Receiver>>,
}
impl Fiber {
    fn outcome(&self) -> Outcome {
        if self.state.get() == FiberState::Disposed {
            Ok(FiberState::Disposed)
        } else if let Some(error) = self.failure.borrow().clone() {
            Err(error)
        } else {
            Ok(self.state.get())
        }
    }
    fn ready(&self) -> LifecycleTask {
        match self.completion.borrow().as_ref() {
            Some(receiver) => LifecycleTask::waiting(receiver.clone()),
            None => LifecycleTask::resolved(self.outcome()),
        }
    }
    fn kick(self: &Rc<Self>, state: &Rc<State>) -> Result<LifecycleTask, LifecycleError> {
        let host = state.admit()?;
        if self.driving.replace(true) {
            return Ok(self.ready());
        }
        let (sender, receiver) = watch::channel(None);
        *self.completion.borrow_mut() = Some(receiver.clone());
        if self.loaded.get() || !self.owner.live.get() {
            self.state.set(FiberState::Unloading);
        } else {
            self.state.set(FiberState::Loading);
        }
        let fiber = self.clone();
        let runtime = state.clone();
        let worker = state.spawn(&host, {
            let fiber = fiber.clone();
            let runtime = runtime.clone();
            async move { fiber.drive(runtime).await }
        });
        state.spawn(&host, async move {
            let mut worker = worker;
            let outcome = loop {
                match worker.await {
                    Ok(result) => {
                        let drift = (!fiber.owner.live.get()
                            && fiber.state.get() != FiberState::Disposed)
                            || (fiber.owner.live.get()
                                && fiber.failure.borrow().is_none()
                                && fiber.revision.get() != fiber.settled_revision.get());
                        if drift {
                            let host = match runtime.admit() {
                                Ok(host) => host,
                                Err(error) => break Err(error.into()),
                            };
                            worker = runtime.spawn(&host, {
                                let fiber = fiber.clone();
                                let runtime = runtime.clone();
                                async move { fiber.drive(runtime).await }
                            });
                            continue;
                        }
                        break result;
                    }
                    Err(error) => {
                        eprintln!("cordis: Fiber driver did not complete: {error}");
                        let error = if error.is_panic() {
                            LifecycleError::DriverPanicked
                        } else {
                            LifecycleError::HostStopped
                        };
                        *fiber.failure.borrow_mut() = Some(error.clone());
                        break Err(error);
                    }
                }
            };
            fiber.driving.set(false);
            sender.send_replace(Some(outcome));
        });
        Ok(LifecycleTask::waiting(receiver))
    }

    fn dispose(self: &Rc<Self>, state: &Rc<State>) -> Result<LifecycleTask, LifecycleError> {
        state.admit()?;
        self.owner.assert_not_cleaning()?;
        if self.state.get() == FiberState::Disposed {
            return Ok(LifecycleTask::resolved(Ok(FiberState::Disposed)));
        }
        self.owner.live.set(false);
        let parent = self.parent_effect.borrow_mut().take();
        if let Some(parent) = parent {
            if let Some(owner) = parent.owner.upgrade() {
                let cleanup = owner.cleanups.borrow_mut().remove(&parent.id);
                drop(cleanup);
            }
        }
        if let Some(group) = self.group.upgrade() {
            let removed = group.fibers.borrow_mut().remove(&self.serial);
            drop(removed);
            if group.fibers.borrow().is_empty() {
                let key = Rc::as_ptr(&group.definition) as usize;
                let stale = {
                    let mut registry = state.registry.0.borrow_mut();
                    if registry
                        .get(&key)
                        .is_some_and(|current| Rc::ptr_eq(current, &group))
                    {
                        registry.remove(&key)
                    } else {
                        None
                    }
                };
                drop(stale);
            }
        }
        self.kick(state)
    }

    async fn drain(&self, state: &Rc<State>) -> Result<(), LifecycleError> {
        let host = state.admit()?;
        let existing = self.owner.draining.borrow().as_ref().cloned();
        let completion = match existing {
            Some(receiver) => crate::Completion::waiting(receiver),
            None => {
                let callbacks = std::mem::take(&mut *self.owner.cleanups.borrow_mut())
                    .into_values()
                    .rev()
                    .collect();
                state.start(&host, self.owner.clone(), callbacks, true)
            }
        };
        completion.await.map_err(|error| match error {
            crate::CompletionError::HostStopped => LifecycleError::HostStopped,
            _ => LifecycleError::DriverPanicked,
        })?;
        Ok(())
    }

    async fn drive(self: Rc<Self>, state: Rc<State>) -> Outcome {
        loop {
            if self.loaded.get() || !self.owner.live.get() {
                self.state.set(FiberState::Unloading);
                self.drain(&state).await?;
                self.loaded.set(false);
            }
            if !self.owner.live.get() {
                self.state.set(FiberState::Disposed);
                self.settled_revision.set(self.revision.get());
                return Ok(FiberState::Disposed);
            }
            let target = self.revision.get();
            self.state.set(FiberState::Loading);
            // A queued restart/dispose can supersede this load before user code begins.
            tokio::task::yield_now().await;
            if !self.owner.live.get() || target != self.revision.get() {
                continue;
            }
            let resolved = self
                .owner
                .setup_scope(|| (self.definition.validate)(&*self.source));
            let outcome = match resolved {
                Ok(resolved) => {
                    self.owner
                        .cleanup_scope(async {
                            (self.definition.apply)(self.context.clone(), resolved).await
                        })
                        .await
                }
                Err(failure) => Err(failure),
            };
            self.loaded.set(true);
            if let Err(failure) = outcome {
                state.report_for(&self.owner, failure.clone());
                self.state.set(FiberState::Unloading);
                self.drain(&state).await?;
                self.loaded.set(false);
                if self.owner.live.get() && target == self.revision.get() {
                    let error = LifecycleError::Callback(failure);
                    *self.failure.borrow_mut() = Some(error.clone());
                    self.state.set(FiberState::Failed);
                    self.settled_revision.set(target);
                    return Err(error);
                }
                continue;
            }
            if self.owner.live.get() && target == self.revision.get() {
                self.state.set(FiberState::Active);
                self.settled_revision.set(target);
                return Ok(FiberState::Active);
            }
        }
    }
}

/// Cloneable control of one non-root Fiber. Dropping all handles is inert;
/// residency and parent ownership keep the Fiber alive until explicit disposal.
pub struct FiberHandle<C: 'static> {
    fiber: Rc<Fiber>,
    runtime: Weak<State>,
    marker: PhantomData<fn() -> C>,
}
impl<C: 'static> Clone for FiberHandle<C> {
    fn clone(&self) -> Self {
        Self {
            fiber: self.fiber.clone(),
            runtime: self.runtime.clone(),
            marker: PhantomData,
        }
    }
}
impl<C: 'static> FiberHandle<C> {
    /// Stable identity, preserved by restart and disposal.
    pub fn id(&self) -> &FiberId {
        &self.fiber.owner.id
    }
    /// Diagnostic name captured at creation, with the nearest named parent fallback.
    pub fn name(&self) -> &str {
        &self.fiber.owner.name
    }
    /// Current observable lifecycle state, not a settlement barrier.
    pub fn state(&self) -> FiberState {
        self.fiber.state.get()
    }
    /// The plugin's source config, distinct from validated apply input.
    pub fn config(&self) -> Rc<C> {
        self.fiber
            .source
            .clone()
            .downcast::<C>()
            .expect("typed Fiber source matches its plugin")
    }
    /// Non-owning view into this Fiber and its inherited metadata.
    pub fn context(&self) -> Context {
        self.fiber.context.clone()
    }
    /// Await the current lifecycle pass without creating work. Self-waits are
    /// explicitly refused instead of stalling apply/cleanup.
    pub fn ready(&self) -> Result<LifecycleTask, LifecycleError> {
        self.fiber.owner.assert_not_cleaning()?;
        Ok(self.fiber.ready())
    }
    /// Force unload/reapply of the same source and identity. Failed Fibers retain
    /// their failure; configuration update/recovery is a later milestone.
    pub fn restart(&self) -> Result<LifecycleTask, LifecycleError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.fiber.owner.admit()?;
        self.fiber.owner.assert_not_cleaning()?;
        if let Some(error) = self.fiber.failure.borrow().clone() {
            return Ok(LifecycleTask::resolved(Err(error)));
        }
        self.fiber.revision.set(
            self.fiber
                .revision
                .get()
                .checked_add(1)
                .ok_or(AdmissionError::IdentityExhausted)?,
        );
        self.fiber.kick(&state)
    }
    /// Mark terminal and unlink residency/parent registration now, then wait for
    /// in-flight apply, owned children, and resources to finish teardown.
    pub fn dispose(&self) -> Result<LifecycleTask, LifecycleError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        self.fiber.dispose(&state)
    }
}

/// A non-owning Registry view. Grouping follows reusable Plugin allocation, not
/// a Rust callback type. Empty current groups are pruned by exact allocation.
pub struct Registry {
    context: Context,
}
impl Registry {
    /// Number of live plugin groups, not individual Fiber instances.
    pub fn len(&self) -> Result<usize, AdmissionError> {
        let state = self
            .context
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let len = state.registry.0.borrow().len();
        Ok(len)
    }
    /// Whether no plugin groups remain.
    pub fn is_empty(&self) -> Result<bool, AdmissionError> {
        self.len().map(|len| len == 0)
    }
    /// Does this reusable declaration have any resident Fibers?
    pub fn contains<C: 'static, R: 'static>(
        &self,
        plugin: &Plugin<C, R>,
    ) -> Result<bool, AdmissionError> {
        let state = self
            .context
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let contains = state.registry.0.borrow().contains_key(&plugin.key());
        Ok(contains)
    }
    /// Snapshot identities belonging to this current plugin group.
    pub fn instances<C: 'static, R: 'static>(
        &self,
        plugin: &Plugin<C, R>,
    ) -> Result<Vec<FiberId>, AdmissionError> {
        let state = self
            .context
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let group = state.registry.0.borrow().get(&plugin.key()).cloned();
        Ok(group
            .map(|group| {
                group
                    .fibers
                    .borrow()
                    .values()
                    .map(|fiber| fiber.owner.id.clone())
                    .collect()
            })
            .unwrap_or_default())
    }
    /// Detach one current group and start each member's disposal immediately.
    /// Returns wait-only barriers; dropping them never cancels cleanup. New spawns
    /// of the same Plugin form a fresh group unaffected by the retired members.
    pub fn remove<C: 'static, R: 'static>(
        &self,
        plugin: &Plugin<C, R>,
    ) -> Result<Vec<LifecycleTask>, LifecycleError> {
        let state = self
            .context
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.context.owner()?.admit()?;
        let group = state.registry.0.borrow().get(&plugin.key()).cloned();
        let Some(group) = group else {
            return Ok(Vec::new());
        };
        let fibers: Vec<_> = group.fibers.borrow().values().cloned().collect();
        for fiber in &fibers {
            fiber.owner.assert_not_cleaning()?;
        }
        let detached = state.registry.0.borrow_mut().remove(&plugin.key());
        drop(detached);
        fibers
            .into_iter()
            .map(|fiber| fiber.dispose(&state))
            .collect()
    }
}

impl Context {
    /// Obtain a read/control view of the runtime's plugin Registry.
    pub fn registry(&self) -> Registry {
        Registry {
            context: self.clone(),
        }
    }
    /// Admit one plugin instance, commit parent cleanup ownership, and schedule
    /// initial apply. Configuration validation occurs inside the lifecycle pass.
    pub fn plugin<C: 'static, R: 'static>(
        &self,
        plugin: &Plugin<C, R>,
        config: C,
    ) -> Result<FiberHandle<C>, LifecycleError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        let parent = self.owner()?;
        parent.admit()?;
        let serial = state
            .next
            .get()
            .checked_add(1)
            .ok_or(AdmissionError::IdentityExhausted)?;
        state.next.set(serial);
        let existing = state.registry.0.borrow().get(&plugin.key()).cloned();
        let group = existing.unwrap_or_else(|| {
            Rc::new(Group {
                definition: plugin.definition.clone(),
                fibers: RefCell::new(BTreeMap::new()),
            })
        });
        let name = if group.definition.name.is_empty() {
            parent.name.clone()
        } else {
            group.definition.name.clone()
        };
        let owner = Owner::child(name, &parent);
        let context = self.with_owner(&owner);
        let fiber = Rc::new(Fiber {
            serial,
            owner,
            context,
            group: Rc::downgrade(&group),
            definition: group.definition.clone(),
            source: Rc::new(config),
            parent_effect: RefCell::new(None),
            state: Cell::new(FiberState::Pending),
            revision: Cell::new(0),
            settled_revision: Cell::new(0),
            loaded: Cell::new(false),
            failure: RefCell::new(None),
            driving: Cell::new(false),
            completion: RefCell::new(None),
        });
        let parent_effect = self
            .on_dispose({
                let fiber = fiber.clone();
                let state = Rc::downgrade(&state);
                move || {
                    let operation = state
                        .upgrade()
                        .ok_or(AdmissionError::RuntimeDropped)
                        .map_err(LifecycleError::from)
                        .and_then(|state| fiber.dispose(&state));
                    async move {
                        operation?.await?;
                        Ok::<_, LifecycleError>(())
                    }
                }
            })?
            .with_label("ctx.plugin()");
        *fiber.parent_effect.borrow_mut() = Some(parent_effect);
        group.fibers.borrow_mut().insert(serial, fiber.clone());
        state.registry.0.borrow_mut().insert(plugin.key(), group);
        drop(fiber.kick(&state)?);
        Ok(FiberHandle {
            fiber,
            runtime: self.runtime.clone(),
            marker: PhantomData,
        })
    }
}
