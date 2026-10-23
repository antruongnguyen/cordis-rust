use crate::{
    AdmissionError, BoxError, Completion, Context, Effect, FiberId, FiberState, PublicationId,
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
}

/// Publication, access, or ownership failure. Missing required services make a
/// plugin Pending; undeclared/inactive checked access is an explicit error.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ServiceError {
    /// No work was admitted.
    Admission(AdmissionError),
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
            _ => None,
        }
    }
}

type Check = dyn Fn(&dyn Any) -> Result<bool, BoxError>;

pub(crate) struct Publication {
    pub(crate) id: PublicationId,
    pub(crate) name: String,
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
pub(crate) struct ServiceStore(RefCell<HashMap<String, Rc<Publication>>>);

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

pub(crate) type Bindings = Vec<Rc<Publication>>;
pub(crate) type Candidate = Result<Option<Rc<Publication>>, ServiceError>;

pub(crate) fn resolve(state: &State, dependency: &Dependency) -> Candidate {
    loop {
        let publication = state.services.0.borrow().get(&dependency.name).cloned();
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
        let current = state.services.0.borrow().get(&dependency.name).cloned();
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
        .map(|publication| publication.name.clone())
        .collect();
    state.registry.notify(state, host, &names);
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
        let existing = state.services.0.borrow().get(key.name()).cloned();
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
                            let mut services = state.services.0.borrow_mut();
                            if services
                                .get(&publication.name)
                                .is_some_and(|current| Rc::ptr_eq(current, &publication))
                            {
                                services.remove(&publication.name)
                            } else {
                                None
                            }
                        };
                        drop(removed);
                        state.registry.notify(
                            state,
                            &host,
                            std::slice::from_ref(&publication.name),
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
            .0
            .borrow_mut()
            .insert(publication.name.clone(), publication.clone());
        if publication.available() {
            state
                .registry
                .notify(&state, &host, std::slice::from_ref(&publication.name));
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
        let owner = self.owner()?;
        let publication = state
            .services
            .0
            .borrow()
            .get(key.name())
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
        let old = publication.value.replace(Rc::new(value));
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
        let mut owner = self.owner()?;
        if owner.is_root() {
            return self.get_unchecked(key);
        }
        loop {
            let own = owner
                .provisions
                .borrow()
                .iter()
                .rev()
                .find(|publication| publication.name == key.name())
                .cloned();
            if let Some(publication) = own {
                return publication.read(key).map(Some);
            }
            if owner
                .dependencies
                .iter()
                .any(|dependency| dependency.name == key.name())
            {
                let publication = owner.committed.borrow().as_ref().and_then(|bindings| {
                    bindings
                        .iter()
                        .find(|publication| publication.name == key.name())
                        .cloned()
                });
                return publication
                    .ok_or_else(|| ServiceError::Inactive {
                        name: key.name().into(),
                    })?
                    .read(key)
                    .map(Some);
            }
            let Some(parent) = owner.parent.upgrade() else {
                break;
            };
            owner = parent;
        }
        Err(ServiceError::Undeclared {
            name: key.name().into(),
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
        let publication = state.services.0.borrow().get(key.name()).cloned();
        publication
            .filter(|publication| !strict || publication.available())
            .map(|publication| publication.read(key))
            .transpose()
    }
}
