use crate::{
    AdmissionError, BoxError, CleanupFailure, Context, Dependency, Effect, FiberId, IsolationId,
    LocalFuture, ServiceError, effect, hooks, owner::Owner, runtime::State, service,
};
use std::{
    any::{Any, TypeId},
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
use tokio::{sync::watch, task::LocalSet};

/// A Fiber's observable lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FiberState {
    /// Waiting for required service publications to become available.
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
    source_type: &'static str,
    source_contract: TypeId,
    resolved_type: &'static str,
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
    dependencies: Vec<Dependency>,
    marker: PhantomData<fn(C) -> R>,
}
impl<C, R> Clone for Plugin<C, R> {
    fn clone(&self) -> Self {
        Self {
            definition: self.definition.clone(),
            dependencies: self.dependencies.clone(),
            marker: PhantomData,
        }
    }
}
impl<C: Clone + 'static> Plugin<C, C> {
    /// Explicit Service.init equivalent. Await initialization, own its returned
    /// disposer, and publish Active only after that acquisition settles.
    pub fn initialized<F, Fut, E>(name: impl Into<String>, initialize: F) -> Self
    where
        F: Fn(Context, C) -> Fut + 'static,
        Fut: Future<Output = Result<crate::Disposer, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        let initialize = Rc::new(initialize);
        Self::new(name, move |ctx, config| {
            let initialize = initialize.clone();
            async move {
                let definition = ctx.clone();
                let effect = ctx
                    .effect_async(move || initialize(definition, config))
                    .map_err(initialization_failure)?;
                effect.ready().await.map_err(initialization_failure)
            }
        })
    }
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
    ///
    /// ```compile_fail
    /// use cordis_core::Plugin;
    /// use std::convert::Infallible;
    /// let _ = Plugin::validated("async validator",
    ///     |_: &u32| async { Ok::<_, Infallible>(1_u32) },
    ///     |_, _: u32| async { Ok::<_, Infallible>(()) });
    /// ```
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
            source_type: std::any::type_name::<C>(),
            source_contract: TypeId::of::<C>(),
            resolved_type: std::any::type_name::<R>(),
            validate: Box::new(move |source| {
                let config = source.downcast_ref::<C>().ok_or_else(|| {
                    CleanupFailure::returned(Box::new(ServiceError::TypeMismatch {
                        name: "plugin source config".into(),
                        expected: std::any::type_name::<C>(),
                        actual: "hook-provided value",
                    }))
                })?;
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
            dependencies: Vec::new(),
            marker: PhantomData,
        }
    }
    fn key(&self) -> usize {
        Rc::as_ptr(&self.definition) as usize
    }
    /// Declare required services for instances loaded through this declaration
    /// view. Clones keep their grouping identity; each Fiber captures its own list.
    /// Required dependencies may carry typed injection configuration.
    /// Repeated names keep their first position and last contract/config,
    /// matching upstream injection dictionary normalization.
    #[must_use]
    pub fn injecting(mut self, dependencies: impl IntoIterator<Item = Dependency>) -> Self {
        let mut normalized = Vec::new();
        let mut indices = HashMap::new();
        for dependency in dependencies {
            if let Some(index) = indices.get(&dependency.name) {
                normalized[*index] = dependency;
            } else {
                indices.insert(dependency.name.clone(), normalized.len());
                normalized.push(dependency);
            }
        }
        self.dependencies = normalized;
        self
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
    /// A typed config/update hook failed.
    Hook(crate::EventError),
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
            Self::Hook(e) => e.fmt(f),
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
            Self::Hook(e) => Some(e),
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

impl RegistryStore {
    pub(crate) fn notify(
        &self,
        state: &Rc<State>,
        host: &LocalSet,
        changes: &[(String, IsolationId)],
    ) {
        let fibers: Vec<_> = self
            .0
            .borrow()
            .values()
            .flat_map(|group| group.fibers.borrow().values().cloned().collect::<Vec<_>>())
            .collect();
        for fiber in fibers {
            let names: Vec<_> = changes
                .iter()
                .flat_map(|(_, slot)| {
                    fiber
                        .owner
                        .dependencies
                        .iter()
                        .filter(|dependency| {
                            state.services.slot(&fiber.context, &dependency.name) == *slot
                        })
                        .map(|dependency| dependency.name.clone())
                })
                .collect();
            if !names.is_empty() {
                fiber.check_services(state, host, &names);
            }
        }
    }
}

struct Target {
    revision: u64,
    bindings: Result<Option<service::Bindings>, ServiceError>,
}
impl Target {
    fn same(&self, other: &Self) -> bool {
        match (&self.bindings, &other.bindings) {
            (Ok(None), Ok(None)) => true,
            (Ok(Some(left)), Ok(Some(right))) => {
                self.revision == other.revision
                    && left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left.publication.id == right.publication.id)
            }
            _ => false,
        }
    }
}

pub(crate) struct Fiber {
    serial: u64,
    owner: Rc<Owner>,
    context: Context,
    group: Weak<Group>,
    definition: Rc<Definition>,
    source: RefCell<Rc<dyn Any>>,
    parent_effect: RefCell<Option<Effect>>,
    state: Cell<FiberState>,
    revision: Cell<u64>,
    candidates: RefCell<Vec<service::Candidate>>,
    target: RefCell<Rc<Target>>,
    settled: RefCell<Option<Rc<Target>>>,
    loaded: Cell<bool>,
    failure: RefCell<Option<LifecycleError>>,
    driving: Cell<bool>,
    completion: RefCell<Option<Receiver>>,
}
impl Fiber {
    pub(crate) fn info(&self) -> hooks::FiberInfo {
        hooks::FiberInfo {
            id: self.owner.id.clone(),
            name: self.owner.name.clone(),
            state: self.state.get(),
            live: self.owner.live.get(),
        }
    }
    fn resolve_config(&self, source: Rc<dyn Any>) -> Result<Box<dyn Any>, CleanupFailure> {
        let source = hooks::Value {
            inner: source,
            actual: self.definition.source_type,
        };
        let tail = source.clone();
        let request = hooks::Config {
            fiber: self.info(),
            source,
        };
        let transformed = self
            .context
            .waterfall(
                &hooks::config(),
                request,
                crate::DispatchOptions::default(),
                move || Ok::<_, BoxError>(tail),
            )
            .map_err(|error| CleanupFailure::returned(Box::new(error)))?;
        if transformed.inner.as_ref().type_id() != self.definition.source_contract {
            return Err(CleanupFailure::returned(Box::new(
                ServiceError::TypeMismatch {
                    name: "plugin source config".into(),
                    expected: self.definition.source_type,
                    actual: transformed.actual,
                },
            )));
        }
        (self.definition.validate)(&*transformed.inner)
    }
    fn request_restart(
        self: &Rc<Self>,
        state: &Rc<State>,
    ) -> Result<LifecycleTask, LifecycleError> {
        if !self.owner.live.get() {
            return Ok(self.ready());
        }
        let host = state.admit()?;
        self.revision.set(
            self.revision
                .get()
                .checked_add(1)
                .ok_or(AdmissionError::IdentityExhausted)?,
        );
        self.refresh(state, &host);
        Ok(self.ready())
    }
    pub(crate) fn state(&self) -> FiberState {
        self.state.get()
    }
    fn set_state(&self, state: &Rc<State>, host: &LocalSet, next: FiberState) {
        let previous = self.state.replace(next);
        if previous != next {
            hooks::observe(
                &self.context,
                &hooks::status(),
                hooks::Status {
                    fiber: self.info(),
                    previous,
                },
                crate::DispatchOptions::default(),
            );
        }
        if previous != next && (previous == FiberState::Active || next == FiberState::Active) {
            service::notify(state, host, &self.owner);
        }
    }
    fn transition(&self, state: &Rc<State>, next: FiberState) -> Result<(), LifecycleError> {
        let host = state.admit()?;
        self.set_state(state, &host, next);
        Ok(())
    }
    fn refresh(self: &Rc<Self>, state: &Rc<State>, host: &LocalSet) {
        if !self.owner.live.get() || self.failure.borrow().is_some() {
            return;
        }
        let target = Rc::new(Target {
            revision: self.revision.get(),
            bindings: (|| {
                let mut bindings = Vec::new();
                for (candidate, dependency) in self
                    .candidates
                    .borrow()
                    .iter()
                    .zip(&self.owner.dependencies)
                {
                    let Some(publication) = candidate.clone()? else {
                        return Ok(None);
                    };
                    bindings.push(service::Binding {
                        name: dependency.name.clone(),
                        publication,
                    });
                }
                Ok(Some(bindings))
            })(),
        });
        if self.target.borrow().same(&target) {
            return;
        }
        if self.loaded.get() {
            self.owner.stop_acquisitions();
        }
        let old = self.target.replace(target);
        drop(old);
        drop(self.kick(state, host));
    }
    fn check_services(self: &Rc<Self>, state: &Rc<State>, host: &LocalSet, names: &[String]) {
        if !self.owner.live.get() || self.failure.borrow().is_some() {
            return;
        }
        for (index, dependency) in self.owner.dependencies.iter().enumerate() {
            if names.contains(&dependency.name) {
                let candidate = service::resolve(state, &self.context, dependency);
                if !self.owner.live.get() {
                    return;
                }
                let old = std::mem::replace(&mut self.candidates.borrow_mut()[index], candidate);
                drop(old);
            }
        }
        self.refresh(state, host);
    }
    fn forget_targets(&self) {
        let old_target = self.target.replace(Rc::new(Target {
            revision: self.revision.get(),
            bindings: Ok(None),
        }));
        let settled = self.settled.borrow_mut().take();
        let candidates = self
            .candidates
            .replace(vec![Ok(None); self.owner.dependencies.len()]);
        drop((old_target, settled, candidates));
    }
    fn drifted(&self) -> bool {
        if !self.owner.live.get() {
            return self.state.get() != FiberState::Disposed;
        }
        self.failure.borrow().is_none()
            && self
                .settled
                .borrow()
                .as_ref()
                .is_none_or(|settled| !Rc::ptr_eq(settled, &self.target.borrow()))
    }
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
    fn kick(self: &Rc<Self>, state: &Rc<State>, host: &LocalSet) -> LifecycleTask {
        if self.driving.replace(true) {
            return self.ready();
        }
        let (sender, receiver) = watch::channel(None);
        *self.completion.borrow_mut() = Some(receiver.clone());
        if self.loaded.get() {
            self.set_state(state, host, FiberState::Unloading);
        } else if self.owner.live.get() {
            self.set_state(state, host, FiberState::Loading);
        }
        let fiber = self.clone();
        let runtime = state.clone();
        let worker = state.spawn(host, {
            let fiber = fiber.clone();
            let runtime = runtime.clone();
            async move { fiber.drive(runtime).await }
        });
        state.spawn(host, async move {
            let mut worker = worker;
            let outcome = loop {
                match worker.await {
                    Ok(result) => {
                        if fiber.drifted() {
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
        LifecycleTask::waiting(receiver)
    }

    fn dispose(self: &Rc<Self>, state: &Rc<State>) -> Result<LifecycleTask, LifecycleError> {
        let host = state.admit()?;
        self.owner.assert_not_cleaning()?;
        if self.state.get() == FiberState::Disposed {
            return Ok(LifecycleTask::resolved(Ok(FiberState::Disposed)));
        }
        self.owner.live.set(false);
        self.owner.stop_acquisitions();
        hooks::observe(
            &self.context,
            &hooks::plugin(),
            self.info(),
            crate::DispatchOptions::default(),
        );
        self.forget_targets();
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
        Ok(self.kick(state, &host))
    }

    async fn drain(&self, state: &Rc<State>) -> Result<(), LifecycleError> {
        loop {
            let host = state.admit()?;
            let existing = self.owner.draining.borrow().as_ref().cloned();
            let joined = existing.is_some();
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
            drop(host);
            completion.await.map_err(|error| match error {
                crate::CompletionError::HostStopped => LifecycleError::HostStopped,
                _ => LifecycleError::DriverPanicked,
            })?;
            // A prior journal snapshot does not own effects admitted after it.
            if !joined {
                break;
            }
        }
        self.owner.release_dependencies();
        Ok(())
    }

    async fn drive(self: Rc<Self>, state: Rc<State>) -> Outcome {
        loop {
            if self.loaded.get() || !self.owner.live.get() {
                if self.loaded.get() {
                    self.transition(&state, FiberState::Unloading)?;
                }
                self.drain(&state).await?;
                self.loaded.set(false);
            }
            if !self.owner.live.get() {
                self.transition(&state, FiberState::Disposed)?;
                return Ok(FiberState::Disposed);
            }
            let target = self.target.borrow().clone();
            let bindings = match &target.bindings {
                Ok(Some(bindings)) => bindings.clone(),
                Ok(None) => {
                    self.transition(&state, FiberState::Pending)?;
                    *self.settled.borrow_mut() = Some(target);
                    return Ok(FiberState::Pending);
                }
                Err(error) => {
                    let failure = CleanupFailure::returned(Box::new(error.clone()));
                    state.report_for(&self.owner, failure.clone());
                    let error = LifecycleError::Callback(failure);
                    *self.failure.borrow_mut() = Some(error.clone());
                    self.forget_targets();
                    self.transition(&state, FiberState::Failed)?;
                    return Err(error);
                }
            };
            self.owner.commit(bindings);
            self.loaded.set(true);
            self.transition(&state, FiberState::Loading)?;
            // A queued restart/dispose can supersede this load before user code begins.
            tokio::task::yield_now().await;
            if !self.owner.live.get() || !Rc::ptr_eq(&target, &self.target.borrow()) {
                continue;
            }
            let source = self.source.borrow().clone();
            let resolved = self.owner.setup_scope(|| self.resolve_config(source));
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
            if let Err(failure) = outcome {
                state.report_for(&self.owner, failure.clone());
                self.transition(&state, FiberState::Unloading)?;
                self.drain(&state).await?;
                self.loaded.set(false);
                if self.owner.live.get() && Rc::ptr_eq(&target, &self.target.borrow()) {
                    let error = LifecycleError::Callback(failure);
                    *self.failure.borrow_mut() = Some(error.clone());
                    self.forget_targets();
                    self.transition(&state, FiberState::Failed)?;
                    return Err(error);
                }
                continue;
            }
            if self.owner.live.get() && Rc::ptr_eq(&target, &self.target.borrow()) {
                self.transition(&state, FiberState::Active)?;
                if Rc::ptr_eq(&target, &self.target.borrow()) {
                    *self.settled.borrow_mut() = Some(target);
                    return Ok(FiberState::Active);
                }
            }
        }
    }
}

/// Configuration update behavior.
#[derive(Clone, Copy, Debug, Default)]
pub struct UpdateOptions {
    /// Suppress source commit notification only, not source replacement or update.
    pub no_save: bool,
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
            .borrow()
            .clone()
            .downcast::<C>()
            .expect("typed Fiber source matches its plugin")
    }
    /// Non-owning view into this Fiber and its inherited metadata.
    pub fn context(&self) -> Context {
        self.fiber.context.clone()
    }
    /// Await the current lifecycle pass without creating work. Self-waits are
    /// explicitly refused instead of stalling apply/cleanup. A missing dependency
    /// can settle this pass to Pending; later service arrival starts another pass.
    pub fn ready(&self) -> Result<LifecycleTask, LifecycleError> {
        self.fiber.owner.assert_not_cleaning()?;
        Ok(self.fiber.ready())
    }
    /// Commit a typed source config and converge toward its latest load.
    /// Invalid loaded candidates fail synchronously without changing source or
    /// invoking update middleware. Pending instances defer validation.
    ///
    /// ```compile_fail
    /// use cordis_core::{Plugin, Runtime};
    /// use std::convert::Infallible;
    /// let runtime = Runtime::new();
    /// let plugin = Plugin::new("typed", |_, _: u32| async { Ok::<_, Infallible>(()) });
    /// let fiber = runtime.context().plugin(&plugin, 1).unwrap();
    /// let _ = fiber.update(String::from("wrong source"));
    /// ```
    pub fn update(&self, config: C) -> Result<LifecycleTask, LifecycleError> {
        self.update_with(config, UpdateOptions::default())
    }

    /// Update with explicit commit-notification behavior. Started updates survive
    /// dropped waiters; update middleware may accept the source without restarting.
    pub fn update_with(
        &self,
        config: C,
        options: UpdateOptions,
    ) -> Result<LifecycleTask, LifecycleError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        self.fiber.owner.admit()?;
        self.fiber.owner.assert_not_cleaning()?;
        let source: Rc<dyn Any> = Rc::new(config);
        let loaded = matches!(&self.fiber.target.borrow().bindings, Ok(Some(_)));
        let resolved = if loaded {
            Some(
                self.fiber
                    .owner
                    .setup_scope(|| self.fiber.resolve_config(source.clone()))
                    .map_err(LifecycleError::Callback)?,
            )
        } else {
            None
        };
        if !options.no_save {
            let task = self
                .fiber
                .context
                .emit(
                    &hooks::commit(),
                    hooks::Commit {
                        fiber: self.fiber.info(),
                        source: hooks::Value {
                            inner: source.clone(),
                            actual: self.fiber.definition.source_type,
                        },
                    },
                    crate::DispatchOptions::default(),
                )
                .map_err(LifecycleError::Hook)?;
            drop(task);
        }
        let previous = self.fiber.source.replace(source);
        let failure = self.fiber.failure.borrow_mut().take();
        let recovering = failure.is_some();
        drop((previous, failure));
        if recovering {
            self.fiber.completion.borrow_mut().take();
            self.fiber.transition(&state, FiberState::Pending)?;
            let names: Vec<_> = self
                .fiber
                .owner
                .dependencies
                .iter()
                .map(|dependency| dependency.name.clone())
                .collect();
            self.fiber.check_services(&state, &host, &names);
        }
        let Some(resolved) = resolved else {
            return self.fiber.request_restart(&state);
        };
        let request = hooks::Update {
            fiber: self.fiber.info(),
            resolved: hooks::Value {
                inner: Rc::from(resolved),
                actual: self.fiber.definition.resolved_type,
            },
        };
        let middleware = self
            .fiber
            .context
            .waterfall_async(
                &hooks::update(),
                request,
                crate::DispatchOptions::for_fiber(self.fiber.owner.id.clone()),
                {
                    let fiber = self.fiber.clone();
                    let state = state.clone();
                    move || {
                        let task = fiber.request_restart(&state);
                        async move {
                            task?.await?;
                            Ok::<_, BoxError>(true)
                        }
                    }
                },
            )
            .map_err(LifecycleError::Hook)?;
        let (sender, receiver) = watch::channel(None);
        let fiber = self.fiber.clone();
        let runtime = state.clone();
        let worker = state.spawn(&host, async move {
            match middleware.await {
                Ok(true) => fiber.ready().await,
                Ok(false) => fiber.outcome(),
                Err(error) => {
                    if let Some(failure) = restart_failure(&error) {
                        let diagnostic = match &failure {
                            LifecycleError::Callback(callback) => callback.clone(),
                            _ => CleanupFailure::returned(Box::new(failure.clone())),
                        };
                        runtime.report_for(&fiber.owner, diagnostic);
                        return Err(failure);
                    }
                    runtime.report_for(
                        &fiber.owner,
                        CleanupFailure::returned(Box::new(error.clone())),
                    );
                    Err(LifecycleError::Hook(error))
                }
            }
        });
        state.spawn(&host, async move {
            let result = match worker.await {
                Ok(result) => result,
                Err(error) => {
                    eprintln!("cordis: update driver did not complete: {error}");
                    Err(if error.is_panic() {
                        LifecycleError::DriverPanicked
                    } else {
                        LifecycleError::HostStopped
                    })
                }
            };
            sender.send_replace(Some(result));
        });
        Ok(LifecycleTask::waiting(receiver))
    }
    /// Force unload/reapply of the same source and identity. Failed Fibers retain
    /// their failure; use update for explicit recovery.
    pub fn restart(&self) -> Result<LifecycleTask, LifecycleError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        self.fiber.owner.admit()?;
        self.fiber.owner.assert_not_cleaning()?;
        if let Some(error) = self.fiber.failure.borrow().clone() {
            return Ok(LifecycleTask::resolved(Err(error)));
        }
        drop(host);
        self.fiber.request_restart(&state)
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

fn initialization_failure(error: crate::SetupError) -> CleanupFailure {
    match error {
        crate::SetupError::Callback(failure) => failure,
        error => CleanupFailure::returned(Box::new(error)),
    }
}

fn restart_failure(error: &crate::EventError) -> Option<LifecycleError> {
    let crate::EventError::Callbacks { failures, .. } = error else {
        return None;
    };
    if failures.len() != 1 {
        return None;
    }
    let source = &failures[0].source;
    if let Some(error) = source.downcast_ref::<LifecycleError>() {
        return Some(error.clone());
    }
    source
        .downcast_ref::<crate::EventError>()
        .and_then(restart_failure)
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
    /// Explicit method-injection/decorator equivalent. The child owns method
    /// effects and unloads/reloads with dependencies; target capture is explicit.
    pub fn inject_method<T: 'static, F, Fut, E>(
        &self,
        dependencies: impl IntoIterator<Item = Dependency>,
        target: Rc<T>,
        method: F,
    ) -> Result<FiberHandle<()>, LifecycleError>
    where
        F: Fn(Rc<T>, Context) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.inject(dependencies, move |context| method(target.clone(), context))
    }
    /// Register resolved-config update middleware on this Fiber. Omitting next
    /// accepts the new source without restarting. Global middleware can use
    /// hooks::update with ordinary global listener options.
    pub fn on_update<R: 'static, F, Fut, E>(
        &self,
        callback: F,
    ) -> Result<crate::ListenerRegistration, crate::EventError>
    where
        F: Fn(Context, Rc<R>, crate::AsyncNext<bool>) -> Fut + 'static,
        Fut: Future<Output = Result<bool, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.on(
            &hooks::update(),
            crate::Listener::around_async(move |ctx, request: hooks::Update, next| {
                let future = request
                    .resolved
                    .get::<R>()
                    .map(|resolved| callback(ctx, resolved, next));
                async move {
                    match future {
                        Ok(future) => future.await.map_err(Into::into),
                        Err(error) => Err(Box::new(error) as BoxError),
                    }
                }
            }),
        )
    }
    /// Synchronous-prefix update adapter. True continues the middleware chain;
    /// false accepts the config without invoking the restart continuation.
    pub fn on_update_sync<R: 'static, F, E>(
        &self,
        callback: F,
    ) -> Result<crate::ListenerRegistration, crate::EventError>
    where
        F: Fn(Rc<R>) -> Result<bool, E> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.on_update(move |_, resolved, next| {
            let decision = callback(resolved).map_err(Into::into);
            let continuation = match decision {
                Ok(true) => Some(next.call()),
                _ => None,
            };
            async move {
                decision?;
                match continuation {
                    Some(future) => future.await.map_err(|error| Box::new(error) as BoxError),
                    None => Ok(false),
                }
            }
        })
    }
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
        let host = state.admit()?;
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
        let owner = Owner::child(
            name,
            &parent,
            plugin.dependencies.clone(),
            self.scope.clone(),
        );
        let context = self.with_owner(&owner);
        let fiber = Rc::new(Fiber {
            serial,
            owner,
            context,
            group: Rc::downgrade(&group),
            definition: group.definition.clone(),
            source: RefCell::new(Rc::new(config)),
            parent_effect: RefCell::new(None),
            state: Cell::new(FiberState::Pending),
            revision: Cell::new(0),
            candidates: RefCell::new(vec![Ok(None); plugin.dependencies.len()]),
            target: RefCell::new(Rc::new(Target {
                revision: 0,
                bindings: Ok(None),
            })),
            settled: RefCell::new(None),
            loaded: Cell::new(false),
            failure: RefCell::new(None),
            driving: Cell::new(false),
            completion: RefCell::new(None),
        });
        *fiber.owner.fiber.borrow_mut() = Rc::downgrade(&fiber);
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
        hooks::observe(
            &fiber.context,
            &hooks::plugin(),
            fiber.info(),
            crate::DispatchOptions::default(),
        );
        let names: Vec<_> = plugin
            .dependencies
            .iter()
            .map(|dependency| dependency.name.clone())
            .collect();
        fiber.check_services(&state, &host, &names);
        Ok(FiberHandle {
            fiber,
            runtime: self.runtime.clone(),
            marker: PhantomData,
        })
    }

    /// Create a parent-owned, no-config consumer with required typed services.
    pub fn inject<F, Fut, E>(
        &self,
        dependencies: impl IntoIterator<Item = Dependency>,
        apply: F,
    ) -> Result<FiberHandle<()>, LifecycleError>
    where
        F: Fn(Context) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.plugin(
            &Plugin::new("", move |ctx, ()| apply(ctx)).injecting(dependencies),
            (),
        )
    }
}
