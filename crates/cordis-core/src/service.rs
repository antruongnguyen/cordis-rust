use crate::{
    AdmissionError, BoxError, Completion, Context, Effect, FiberId, FiberState, IsolationId,
    PublicationId,
    effect::{self, Cleanup},
    owner::Owner,
    runtime::State,
};
use std::{
    any::{Any, TypeId, type_name},
    cell::RefCell,
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    marker::PhantomData,
    rc::{Rc, Weak},
};
use tokio::{sync::watch, task::LocalSet};

/// A typed contract for a named service slot. Equal names select the same slot;
/// incompatible value types are reported, not treated as separate services.
///
/// ```compile_fail
/// use cordis_core::{Runtime, ServiceKey};
/// let key = ServiceKey::<u32>::new("count");
/// let _ = Runtime::new().context().provide(&key, "not a u32");
/// ```
///
/// ```
/// use cordis_core::{BoxError, FiberState, Runtime, ServiceKey};
/// # fn main() -> Result<(), BoxError> {
/// let executor = tokio::runtime::Builder::new_current_thread().build()?;
/// executor.block_on(async {
///     let runtime = Runtime::new();
///     runtime.run(async {
///         let root = runtime.context();
///         let key = ServiceKey::<u32>::new("count");
///         let consumer = root.inject([key.required()], {
///             let key = key.clone();
///             move |ctx| {
///                 let key = key.clone();
///                 async move {
///                     assert_eq!(*ctx.get(&key)?.unwrap(), 7);
///                     Ok::<_, BoxError>(())
///                 }
///             }
///         })?;
///         assert_eq!(consumer.state(), FiberState::Pending);
///         let publication = root.provide(&key, 7)?;
///         assert_eq!(consumer.ready()?.await?, FiberState::Active);
///         publication.withdraw()?.await?;
///         assert_eq!(consumer.ready()?.await?, FiberState::Pending);
///         root.drain()?.await?;
///         Ok::<_, BoxError>(())
///     }).await??;
///     Ok::<_, BoxError>(())
/// })
/// # }
/// ```
pub struct ServiceKey<T> {
    dependency: Dependency,
    marker: PhantomData<fn() -> T>,
}
impl<T> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        Self {
            dependency: self.dependency.clone(),
            marker: PhantomData,
        }
    }
}
impl<T: 'static> ServiceKey<T> {
    /// Select a named service with value type T.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            dependency: Dependency {
                name: name.into(),
                contract: TypeId::of::<T>(),
                type_name: type_name::<T>(),
                config: None,
            },
            marker: PhantomData,
        }
    }
    /// The service's name.
    pub fn name(&self) -> &str {
        &self.dependency.name
    }
    /// Declare this service as a required injection.
    pub fn required(&self) -> Dependency {
        self.dependency.clone()
    }
}

/// One required typed injection. Different value types can share one declaration list.
#[derive(Clone, Debug)]
pub struct Dependency {
    pub(crate) name: String,
    contract: TypeId,
    type_name: &'static str,
    pub(crate) config: Option<(Rc<dyn Any>, &'static str)>,
}
impl Dependency {
    /// Supply typed injection configuration beneath use-site intercepts.
    pub fn with_config<C: 'static>(mut self, config: C) -> Self {
        self.config = Some((Rc::new(config), type_name::<C>()));
        self
    }
}

/// Publication, access, or ownership failure. Missing required services make a
/// plugin Pending; undeclared/inactive checked access is an explicit error.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ServiceError {
    /// No work was admitted.
    Admission(AdmissionError),
    /// Checked reflection middleware failed.
    Hook(crate::EventError),
    /// Another live publication occupies the name.
    DuplicateProvider {
        /// Occupied service name.
        name: String,
        /// Diagnostic name of the existing provider.
        provider: String,
    },
    /// A name was used with an incompatible Rust value contract.
    TypeMismatch {
        /// Service name.
        name: String,
        /// Requested value type.
        expected: &'static str,
        /// Published value type.
        actual: &'static str,
    },
    /// The definition's Fiber ancestry does not admit this service.
    Undeclared {
        /// Service name.
        name: String,
    },
    /// A declaring Fiber has no committed dependency snapshot.
    Inactive {
        /// Service name.
        name: String,
    },
    /// Set requires an existing publication.
    NotProvided {
        /// Service name.
        name: String,
    },
    /// Only the exact providing Fiber can set a service.
    NotOwner {
        /// Service name.
        name: String,
    },
}
impl From<AdmissionError> for ServiceError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
    }
}
impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(f),
            Self::Hook(error) => error.fmt(f),
            Self::DuplicateProvider { name, provider } => {
                write!(f, "service {name:?} has been registered at <{provider}>")
            }
            Self::TypeMismatch {
                name,
                expected,
                actual,
            } => write!(
                f,
                "service {name:?} requires {expected}, but provides {actual}"
            ),
            Self::Undeclared { name } => write!(f, "cannot get service {name:?} without inject"),
            Self::Inactive { name } => {
                write!(
                    f,
                    "cannot get required service {name:?} in inactive context"
                )
            }
            Self::NotProvided { name } => write!(f, "cannot set service {name:?} without provide"),
            Self::NotOwner { name } => write!(f, "cannot set service {name:?} in multiple fibers"),
        }
    }
}
impl Error for ServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Hook(error) => Some(error),
            _ => None,
        }
    }
}

type Check = dyn Fn(&dyn Any) -> Result<bool, BoxError>;

pub(crate) struct Publication {
    pub(crate) id: PublicationId,
    pub(crate) name: String,
    pub(crate) slot: IsolationId,
    definition: Context,
    contract: TypeId,
    type_name: &'static str,
    value: RefCell<Rc<dyn Any>>,
    provider: Weak<Owner>,
    check: Option<Box<Check>>,
    dependents: RefCell<HashSet<FiberId>>,
    changed: watch::Sender<()>,
}
impl Publication {
    fn matches(&self, dependency: &Dependency) -> Result<(), ServiceError> {
        if self.contract == dependency.contract {
            Ok(())
        } else {
            Err(ServiceError::TypeMismatch {
                name: dependency.name.clone(),
                expected: dependency.type_name,
                actual: self.type_name,
            })
        }
    }
    fn read<T: 'static>(&self, key: &ServiceKey<T>) -> Result<Rc<T>, ServiceError> {
        self.matches(&key.dependency)?;
        self.value
            .borrow()
            .clone()
            .downcast()
            .map_err(|_| ServiceError::TypeMismatch {
                name: self.name.clone(),
                expected: type_name::<T>(),
                actual: self.type_name,
            })
    }
    fn available(&self) -> bool {
        self.provider.upgrade().is_some_and(|owner| {
            !owner.services_paused.get()
                && (owner.is_root()
                    || owner
                        .fiber
                        .borrow()
                        .upgrade()
                        .is_some_and(|fiber| fiber.state() == FiberState::Active))
        })
    }
    pub(crate) fn retain(&self, owner: &Owner) {
        if self
            .provider
            .upgrade()
            .is_some_and(|provider| provider.id == owner.id)
        {
            return;
        }
        self.dependents.borrow_mut().insert(owner.id.clone());
    }
    pub(crate) fn release(&self, owner: &Owner) {
        if self.dependents.borrow_mut().remove(&owner.id) {
            self.changed.send_replace(());
        }
    }
    pub(crate) async fn wait_unused(&self) {
        let mut changed = self.changed.subscribe();
        while !self.dependents.borrow().is_empty() {
            changed
                .changed()
                .await
                .expect("publication owns its change sender");
        }
    }
}

#[derive(Default)]
pub(crate) struct ServiceStore {
    current: RefCell<HashMap<IsolationId, Rc<Publication>>>,
    defaults: RefCell<HashMap<String, IsolationId>>,
    declared: RefCell<HashSet<String>>,
}
impl ServiceStore {
    pub(crate) fn declared(&self, name: &str) -> bool {
        self.declared.borrow().contains(name)
    }
    pub(crate) fn slot(&self, ctx: &Context, name: &str) -> IsolationId {
        let mut scope = ctx.scope.as_ref();
        while let Some(layer) = scope {
            if layer.name == name {
                return layer.slot.clone();
            }
            scope = layer.parent.as_ref();
        }
        self.defaults
            .borrow_mut()
            .entry(name.into())
            .or_default()
            .clone()
    }
}

/// The exact owned publication registration. Drop is inert. Withdrawal starts
/// now, bars new consumers, and waits only for this publication's old dependents.
pub struct ServiceRegistration<T> {
    effect: Effect,
    id: PublicationId,
    marker: PhantomData<fn() -> T>,
}
impl<T> ServiceRegistration<T> {
    /// Exact identity, unchanged by set and distinct after republication.
    pub fn id(&self) -> &PublicationId {
        &self.id
    }
    /// Withdraw the publication and join its exact dependent cleanup.
    pub fn withdraw(self) -> Result<Completion, AdmissionError> {
        self.effect.dispose()
    }
    /// Transfer publication cleanup into an existing effect composition.
    pub fn into_effect(self) -> Effect {
        self.effect
    }
}

#[derive(Clone)]
pub(crate) struct Binding {
    pub(crate) name: String,
    pub(crate) publication: Rc<Publication>,
}
pub(crate) type Bindings = Vec<Binding>;
pub(crate) type Candidate = Result<Option<Rc<Publication>>, ServiceError>;

pub(crate) fn resolve(state: &State, ctx: &Context, dependency: &Dependency) -> Candidate {
    loop {
        let slot = state.services.slot(ctx, &dependency.name);
        let publication = state.services.current.borrow().get(&slot).cloned();
        let Some(publication) = publication.filter(|publication| publication.available()) else {
            return Ok(None);
        };
        publication.matches(dependency)?;
        if let Some(check) = &publication.check {
            let value = publication.value.borrow().clone();
            match effect::invoke(|| check(&*value)) {
                Ok(true) => {}
                Ok(false) => return Ok(None),
                Err(failure) => {
                    if let Some(provider) = publication.provider.upgrade() {
                        state.report_for(&provider, failure);
                    }
                    return Ok(None);
                }
            }
        }
        let current = state.services.current.borrow().get(&slot).cloned();
        if current.is_some_and(|current| Rc::ptr_eq(&current, &publication))
            && publication.available()
        {
            return Ok(Some(publication));
        }
    }
}

pub(crate) fn notify(state: &Rc<State>, host: &LocalSet, owner: &Owner) {
    let names: Vec<_> = owner
        .provisions
        .borrow()
        .iter()
        .map(|publication| (publication.name.clone(), publication.slot.clone()))
        .collect();
    notify_slots(state, host, &names);
}

fn notify_slots(state: &Rc<State>, host: &LocalSet, changes: &[(String, IsolationId)]) {
    state.registry.notify(state, host, changes);
    for (name, slot) in changes {
        let publication = state.services.current.borrow().get(slot).cloned();
        let value = publication.map(|publication| crate::hooks::Value {
            inner: publication.value.borrow().clone(),
            actual: publication.type_name,
        });
        let key = ServiceKey::<()>::new(name);
        let ctx = Context::new(state).isolate_with(&key, slot);
        let selected = slot.clone();
        let name_filter = name.clone();
        let runtime = Rc::downgrade(state);
        let options = crate::DispatchOptions::filtered(move |target| {
            let state = runtime.upgrade().expect("dispatch retains runtime");
            state.services.slot(target, &name_filter) == selected
        });
        crate::hooks::observe(
            &ctx,
            &crate::hooks::service(),
            crate::hooks::ServiceChange {
                name: name.clone(),
                slot: slot.clone(),
                value,
            },
            options,
        );
    }
}
pub(crate) async fn wait_dependents(owner: &Owner) {
    let publications = owner.provisions.borrow().clone();
    futures_util::future::join_all(
        publications
            .iter()
            .map(|publication| publication.wait_unused()),
    )
    .await;
}

impl Context {
    /// Publish a typed value owned by this Fiber. A second live publication of
    /// the same name is an error even if the value/type/provider is the same.
    pub fn provide<T: 'static>(
        &self,
        key: &ServiceKey<T>,
        value: T,
    ) -> Result<ServiceRegistration<T>, ServiceError> {
        self.publish(key, value, None)
    }

    /// Publish with a synchronous readiness check. False keeps consumers Pending;
    /// check errors/panics are diagnosed against the provider and also block loads.
    /// Checks run at dependency resolution, not on ordinary reads or set.
    pub fn provide_checked<T: 'static, F, E>(
        &self,
        key: &ServiceKey<T>,
        value: T,
        check: F,
    ) -> Result<ServiceRegistration<T>, ServiceError>
    where
        F: Fn(&T) -> Result<bool, E> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.publish(
            key,
            value,
            Some(Box::new(move |value| {
                let value = value
                    .downcast_ref::<T>()
                    .expect("checked publication contract");
                check(value).map_err(Into::into)
            })),
        )
    }

    fn publish<T: 'static>(
        &self,
        key: &ServiceKey<T>,
        value: T,
        check: Option<Box<Check>>,
    ) -> Result<ServiceRegistration<T>, ServiceError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        let owner = self.owner()?;
        owner.admit()?;
        if state.reflect.contains(key.name()) {
            return Err(ServiceError::DuplicateProvider {
                name: key.name().into(),
                provider: "accessor".into(),
            });
        }
        let slot = state.services.slot(self, key.name());
        let existing = state.services.current.borrow().get(&slot).cloned();
        if let Some(existing) = existing {
            return Err(ServiceError::DuplicateProvider {
                name: key.name().into(),
                provider: existing
                    .provider
                    .upgrade()
                    .map(|owner| owner.name.clone())
                    .unwrap_or_else(|| "released".into()),
            });
        }
        let publication = Rc::new(Publication {
            id: PublicationId::new(),
            name: key.name().into(),
            slot: slot.clone(),
            definition: self.use_site(),
            contract: key.dependency.contract,
            type_name: key.dependency.type_name,
            value: RefCell::new(Rc::new(value)),
            provider: Rc::downgrade(&owner),
            check,
            dependents: RefCell::new(HashSet::new()),
            changed: watch::channel(()).0,
        });
        let cleanup = Cleanup::asynchronous({
            let publication = publication.clone();
            let runtime = self.runtime.clone();
            let provider = Rc::downgrade(&owner);
            move || {
                let state = runtime.upgrade();
                let provider = provider.upgrade();
                let preparation = match &state {
                    Some(state) => state.admit().map(|host| {
                        let removed = {
                            let mut services = state.services.current.borrow_mut();
                            if services
                                .get(&publication.slot)
                                .is_some_and(|current| Rc::ptr_eq(current, &publication))
                            {
                                services.remove(&publication.slot)
                            } else {
                                None
                            }
                        };
                        drop(removed);
                        notify_slots(
                            state,
                            &host,
                            &[(publication.name.clone(), publication.slot.clone())],
                        );
                    }),
                    None => Err(AdmissionError::RuntimeDropped),
                };
                Box::pin(async move {
                    preparation?;
                    publication.wait_unused().await;
                    // Let already-started children perform inherited own-service cleanup.
                    tokio::task::yield_now().await;
                    if let Some(provider) = provider {
                        let removed = {
                            let mut provisions = provider.provisions.borrow_mut();
                            provisions
                                .iter()
                                .position(|current| Rc::ptr_eq(current, &publication))
                                .map(|index| provisions.remove(index))
                        };
                        drop(removed);
                    }
                    Ok(())
                })
            }
        });
        let effect = self
            .register_cleanup(cleanup)?
            .with_label(format!("ctx.provide({:?})", key.name()));
        owner.provisions.borrow_mut().push(publication.clone());
        state
            .services
            .declared
            .borrow_mut()
            .insert(key.name().into());
        state
            .services
            .current
            .borrow_mut()
            .insert(slot, publication.clone());
        if publication.available() {
            notify_slots(
                &state,
                &host,
                &[(publication.name.clone(), publication.slot.clone())],
            );
        }
        Ok(ServiceRegistration {
            effect,
            id: publication.id.clone(),
            marker: PhantomData,
        })
    }

    /// Set the current publication's value without replacing its identity or
    /// notifying consumers. Only its exact provider Fiber may do this.
    pub fn set<T: 'static>(&self, key: &ServiceKey<T>, value: T) -> Result<(), ServiceError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        let request = crate::hooks::Set {
            context: self.clone(),
            name: key.name().into(),
            value: crate::hooks::Value::new(value),
        };
        let value = request.value.get::<T>()?;
        let context = self.clone();
        let key = key.clone();
        self.waterfall(
            &crate::hooks::set(),
            request,
            crate::DispatchOptions::default(),
            move || context.set_value(&key, value),
        )
        .map_err(hook_error)
    }

    fn set_value<T: 'static>(&self, key: &ServiceKey<T>, value: Rc<T>) -> Result<(), ServiceError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        let owner = self.owner()?;
        let publication = state
            .services
            .current
            .borrow()
            .get(&state.services.slot(self, key.name()))
            .cloned()
            .ok_or_else(|| ServiceError::NotProvided {
                name: key.name().into(),
            })?;
        if !publication
            .provider
            .upgrade()
            .is_some_and(|provider| provider.id == owner.id)
        {
            return Err(ServiceError::NotOwner {
                name: key.name().into(),
            });
        }
        publication.matches(&key.dependency)?;
        let old = publication.value.replace(value);
        drop(old);
        Ok(())
    }

    /// Checked definition-site access: own provisions, then the nearest declaring
    /// ancestor's committed snapshot. Root keeps its unchecked direct-access bypass.
    /// An injected snapshot remains readable during setup, rollback, and cleanup.
    pub fn get<T: 'static>(&self, key: &ServiceKey<T>) -> Result<Option<Rc<T>>, ServiceError> {
        let _state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        if self.definition_site().owner()?.is_root() {
            return self.definition_site().get_unchecked(key);
        }
        let context = self.clone();
        let selected = key.clone();
        self.waterfall_read(
            &crate::hooks::get(),
            crate::hooks::Get {
                context: self.clone(),
                name: key.name().into(),
            },
            crate::DispatchOptions::default(),
            move || {
                let value = context
                    .locate(selected.name())?
                    .checked(selected.name())?
                    .0
                    .read(&selected)?;
                Ok::<_, ServiceError>(Some(crate::hooks::Value::from_rc(value)))
            },
            false,
        )
        .map_err(hook_error)?
        .map(|value| value.get::<T>())
        .transpose()
    }

    fn locate(&self, name: &str) -> Result<Located, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let definition = self.definition_site();
        let mut owner = definition.owner()?;
        let slot = state.services.slot(&definition, name);
        let mut scope = definition.scope.clone();
        loop {
            let own = owner
                .provisions
                .borrow()
                .iter()
                .rev()
                .find(|publication| publication.name == name && publication.slot == slot)
                .cloned();
            if let Some(publication) = own {
                return Ok(Located::Own(publication));
            }
            while !same_scope(&scope, &owner.scope) {
                let Some(layer) = scope else {
                    return Ok(Located::Undeclared);
                };
                if layer.name == name {
                    return Ok(Located::Undeclared);
                }
                scope = layer.parent.clone();
            }
            let declaration = owner
                .dependencies
                .iter()
                .find(|dependency| dependency.name == name)
                .cloned();
            if let Some(dependency) = declaration {
                let publication = owner.committed.borrow().as_ref().and_then(|bindings| {
                    bindings
                        .iter()
                        .find(|publication| publication.name == name)
                        .cloned()
                });
                return Ok(Located::Injected(
                    dependency,
                    publication.map(|binding| binding.publication),
                ));
            }
            let Some(parent) = owner.parent.upgrade() else {
                break;
            };
            owner = parent;
        }
        Ok(Located::Undeclared)
    }

    /// Bind checked service access to separate definition and use contexts.
    pub fn bind<T: 'static>(&self, key: &ServiceKey<T>) -> Result<BoundService<T>, ServiceError> {
        let (publication, config) = if self.definition_site().owner()?.is_root() {
            return self.bind_direct_inner(key, false);
        } else {
            self.locate(key.name())?.checked(key.name())?
        };
        let mut bound = self.bind_publication(key, publication, config)?;
        bound.value = self.get(key)?.ok_or_else(|| ServiceError::Inactive {
            name: key.name().into(),
        })?;
        Ok(bound)
    }

    /// Bind direct access, bypassing injection authority but requiring Active.
    pub fn bind_direct<T: 'static>(
        &self,
        key: &ServiceKey<T>,
    ) -> Result<BoundService<T>, ServiceError> {
        self.bind_direct_inner(key, true)
    }

    fn bind_direct_inner<T: 'static>(
        &self,
        key: &ServiceKey<T>,
        strict: bool,
    ) -> Result<BoundService<T>, ServiceError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let publication = state
            .services
            .current
            .borrow()
            .get(&state.services.slot(self, key.name()))
            .cloned()
            .filter(|publication| !strict || publication.available())
            .ok_or_else(|| ServiceError::Inactive {
                name: key.name().into(),
            })?;
        let config = match self.locate(key.name())? {
            Located::Injected(dependency, _) => dependency.config,
            _ => None,
        };
        self.bind_publication(key, publication, config)
    }

    fn bind_publication<T: 'static>(
        &self,
        key: &ServiceKey<T>,
        publication: Rc<Publication>,
        config: Option<(Rc<dyn Any>, &'static str)>,
    ) -> Result<BoundService<T>, ServiceError> {
        Ok(BoundService {
            value: publication.read(key)?,
            context: self.shadow(&publication.definition),
            caller: self.definition_site(),
            access: self.clone(),
            name: key.name().into(),
            config,
        })
    }

    /// Explicit direct lookup, bypassing injection authority. Only an Active
    /// provider is visible; missing/inactive services return None.
    pub fn get_direct<T: 'static>(
        &self,
        key: &ServiceKey<T>,
    ) -> Result<Option<Rc<T>>, ServiceError> {
        self.direct(key, true)
    }
}

fn hook_error(error: crate::EventError) -> ServiceError {
    if let crate::EventError::Callbacks { failures, .. } = &error {
        if failures.len() == 1 {
            if let Some(error) = failures[0].source.downcast_ref::<ServiceError>() {
                return error.clone();
            }
        }
    }
    ServiceError::Hook(error)
}

fn same_scope(
    left: &Option<Rc<crate::context::IsolationLayer>>,
    right: &Option<Rc<crate::context::IsolationLayer>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Rc::ptr_eq(left, right),
        _ => false,
    }
}

type ConfigValue = Option<(Rc<dyn Any>, &'static str)>;
enum Located {
    Own(Rc<Publication>),
    Injected(Dependency, Option<Rc<Publication>>),
    Undeclared,
}
impl Located {
    fn checked(self, name: &str) -> Result<(Rc<Publication>, ConfigValue), ServiceError> {
        match self {
            Self::Own(publication) => Ok((publication, None)),
            Self::Injected(dependency, Some(publication)) => Ok((publication, dependency.config)),
            Self::Injected(_, None) => Err(ServiceError::Inactive { name: name.into() }),
            Self::Undeclared => Err(ServiceError::Undeclared { name: name.into() }),
        }
    }
}

/// Explicit traced service value. Calls receive a Context whose dependency
/// authority is the provider's definition and whose effects/configuration belong
/// to the use site. Like an Rc read, this is not an independent lifecycle lease.
pub struct BoundService<T> {
    value: Rc<T>,
    context: Context,
    caller: Context,
    access: Context,
    name: String,
    config: ConfigValue,
}
impl<T> Clone for BoundService<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            context: self.context.clone(),
            caller: self.caller.clone(),
            access: self.access.clone(),
            name: self.name.clone(),
            config: self.config.clone(),
        }
    }
}
impl<T: 'static> BoundService<T> {
    /// Execute an explicit method/accessor adapter with the traced Context.
    pub fn call<R>(&self, callback: impl FnOnce(&T, Context) -> R) -> R {
        callback(&self.value, self.context.clone())
    }
    /// The traced Context, safe to capture for later nested calls.
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    /// Filter service-originated events by the use site's selected realm.
    pub fn dispatch_options(&self) -> Result<crate::DispatchOptions, AdmissionError> {
        self.context
            .service_filter(&ServiceKey::<T>::new(&self.name))
    }
    /// The definition Context of the code that accessed this service.
    pub fn caller(&self) -> Context {
        self.caller.clone()
    }
    /// Explicit noShadow adapter: expose the caller but pass an unshadowed use view.
    pub fn call_unshadowed<R>(&self, callback: impl FnOnce(&T, Context, Context) -> R) -> R {
        callback(&self.value, self.context.use_site(), self.caller.clone())
    }
    /// Retrace a captured holder/extension at a new use site, never nesting its
    /// definition shadow or borrowing the new caller's dependency authority.
    pub fn rebind(&self, usage: &Context) -> Result<Self, ServiceError> {
        if usage.runtime_id() != self.context.runtime_id() {
            return Err(AdmissionError::WrongHost.into());
        }
        let config = match usage.locate(&self.name)? {
            Located::Injected(dependency, _) => dependency.config,
            _ => None,
        };
        Ok(Self {
            value: self.value.clone(),
            context: usage.shadow(&self.context.definition_site()),
            caller: usage.definition_site(),
            access: usage.clone(),
            name: self.name.clone(),
            config,
        })
    }
    /// Read a typed associated accessor using the original access authority.
    pub fn read_accessor<U: 'static>(
        &self,
        key: &crate::AccessorKey<T, U>,
    ) -> Result<U, crate::ReflectError> {
        self.access.read_accessor(key, self.value.clone())
    }
    /// Set a typed associated accessor using the original access authority.
    pub fn write_accessor<U: 'static>(
        &self,
        key: &crate::AccessorKey<T, U>,
        value: U,
    ) -> Result<(), crate::ReflectError> {
        self.access.write_accessor(key, self.value.clone(), value)
    }
    /// Access a dotted associated service with the original access authority,
    /// rather than accidentally borrowing this service's own dependency rights.
    pub fn associated<U: 'static>(&self, property: &str) -> Result<BoundService<U>, ServiceError> {
        self.access
            .bind(&ServiceKey::new(format!("{}.{}", self.name, property)))
    }
    /// Extend a service/holder while keeping its exact definition/use views.
    pub fn map<U: 'static>(&self, extend: impl FnOnce(&T) -> U) -> BoundService<U> {
        BoundService {
            value: Rc::new(extend(&self.value)),
            context: self.context.clone(),
            caller: self.caller.clone(),
            access: self.access.clone(),
            name: self.name.clone(),
            config: self.config.clone(),
        }
    }
    /// Injection config first (nearest admitting declaration only), followed by
    /// use-site intercept layers from outer to inner.
    pub fn intercepts<C: 'static>(&self) -> Result<Vec<Rc<C>>, ServiceError> {
        let mut values = Vec::new();
        if let Some((config, actual)) = &self.config {
            values.push(config.clone().downcast::<C>().map_err(|_| {
                ServiceError::TypeMismatch {
                    name: format!("{} injection config", self.name),
                    expected: type_name::<C>(),
                    actual,
                }
            })?);
        }
        values.extend(self.context.intercept_values::<C>(&self.name)?);
        Ok(values)
    }
    /// Resolve service-specific configuration using an explicit merge operation.
    pub fn resolve_intercept<C: 'static, R>(
        &self,
        merge: impl FnOnce(Vec<Rc<C>>) -> R,
    ) -> Result<R, ServiceError> {
        Ok(merge(self.intercepts()?))
    }
    /// Invoke a callable service through its typed Rust adapter.
    pub fn invoke<A>(&self, args: A) -> Result<T::Output, BoxError>
    where
        T: ServiceInvoke<A>,
    {
        self.value.invoke(self.context.clone(), args)
    }
}

/// Stable Rust equivalent of a callable service object.
pub trait ServiceInvoke<A> {
    /// Typed invocation result.
    type Output;
    /// Invoke using explicit traced definition/use context.
    fn invoke(&self, context: Context, args: A) -> Result<Self::Output, BoxError>;
}

impl Context {
    /// Select listeners in this service's use-site realm. Global listeners
    /// still bypass the filter in every dispatch mode.
    pub fn service_filter<T: 'static>(
        &self,
        key: &ServiceKey<T>,
    ) -> Result<crate::DispatchOptions, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let slot = state.services.slot(self, key.name());
        let runtime = Rc::downgrade(&state);
        let name = key.name().to_owned();
        Ok(crate::DispatchOptions::filtered(move |target| {
            let state = runtime
                .upgrade()
                .expect("event dispatch retains its runtime");
            state.services.slot(target, &name) == slot
        }))
    }
    /// Explicit unchecked direct lookup, including Loading/Unloading providers.
    /// This does not retain a provider's lifecycle resources through cleanup.
    pub fn get_unchecked<T: 'static>(
        &self,
        key: &ServiceKey<T>,
    ) -> Result<Option<Rc<T>>, ServiceError> {
        self.direct(key, false)
    }

    fn direct<T: 'static>(
        &self,
        key: &ServiceKey<T>,
        strict: bool,
    ) -> Result<Option<Rc<T>>, ServiceError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let publication = state
            .services
            .current
            .borrow()
            .get(&state.services.slot(self, key.name()))
            .cloned();
        publication
            .filter(|publication| !strict || publication.available())
            .map(|publication| publication.read(key))
            .transpose()
    }
}
