use crate::{AdmissionError, BoxError, CleanupFailure, Context, Effect, ServiceKey, effect};
use std::{
    any::{Any, type_name},
    cell::RefCell,
    collections::HashMap,
    error::Error,
    fmt,
    marker::PhantomData,
    rc::Rc,
};

/// Typed accessor identity, including the associated receiver type.
pub struct AccessorKey<R, T> {
    name: String,
    marker: PhantomData<fn(R) -> T>,
}
impl<R, T> Clone for AccessorKey<R, T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            marker: PhantomData,
        }
    }
}
impl<R, T> AccessorKey<R, T> {
    /// Select an accessor by its global reflection name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            marker: PhantomData,
        }
    }
    /// Name of this accessor.
    pub fn name(&self) -> &str {
        &self.name
    }
}
type Get<R, T> = dyn Fn(Context, Rc<R>) -> Result<T, BoxError>;
type Set<R, T> = dyn Fn(Context, Rc<R>, T) -> Result<(), BoxError>;

/// Explicit typed replacement for a dynamic accessor/mixin.
pub struct Accessor<R, T> {
    get: Box<Get<R, T>>,
    set: Option<Box<Set<R, T>>>,
}
impl<R: 'static, T: 'static> Accessor<R, T> {
    /// Define a read-only accessor receiving the traced Context and receiver.
    pub fn new<F, E>(get: F) -> Self
    where
        F: Fn(Context, Rc<R>) -> Result<T, E> + 'static,
        E: Into<BoxError>,
    {
        Self {
            get: Box::new(move |ctx, receiver| get(ctx, receiver).map_err(Into::into)),
            set: None,
        }
    }
    /// Add a setter; receiver mutation can use normal Rust interior mutability.
    pub fn with_setter<F, E>(mut self, set: F) -> Self
    where
        F: Fn(Context, Rc<R>, T) -> Result<(), E> + 'static,
        E: Into<BoxError>,
    {
        self.set = Some(Box::new(move |ctx, receiver, value| {
            set(ctx, receiver, value).map_err(Into::into)
        }));
        self
    }
    /// Explicit service mixin: resolve the source through the accessing code's
    /// authority, and pass its definition/use shadow to the adapted method.
    pub fn from_service<S: 'static, F, E>(key: ServiceKey<S>, get: F) -> Self
    where
        F: Fn(&S, Context, Rc<R>) -> Result<T, E> + 'static,
        E: Into<BoxError>,
    {
        Self::new(move |ctx: Context, receiver| {
            ctx.bind(&key)?
                .call(|service, context| get(service, context, receiver).map_err(Into::into))
        })
    }
}

/// Accessor admission, reflection-contract, readonly, or callback failure.
#[derive(Clone, Debug)]
pub enum ReflectError {
    /// Host/owner admission failed.
    Admission(AdmissionError),
    /// A property already has a declaration.
    Duplicate(String),
    /// No live accessor has this name.
    Missing(String),
    /// Receiver/value contracts do not match.
    Contract(String),
    /// No setter was declared.
    ReadOnly(String),
    /// User code failed or panicked.
    Callback(CleanupFailure),
}
impl From<AdmissionError> for ReflectError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
    }
}
impl fmt::Display for ReflectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(f),
            Self::Duplicate(name) => write!(f, "property {name:?} is already declared"),
            Self::Missing(name) => write!(f, "accessor {name:?} is not declared"),
            Self::Contract(name) => write!(
                f,
                "accessor {name:?} has an incompatible receiver/value contract"
            ),
            Self::ReadOnly(name) => write!(f, "accessor {name:?} is readonly"),
            Self::Callback(error) => error.fmt(f),
        }
    }
}
impl Error for ReflectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Callback(error) => Some(error),
            _ => None,
        }
    }
}
#[derive(Default)]
pub(crate) struct ReflectStore {
    entries: RefCell<HashMap<String, Rc<dyn Any>>>,
}
impl ReflectStore {
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.entries.borrow().contains_key(name)
    }
}
impl Context {
    /// Register an accessor globally, with lifetime owned by this Fiber.
    pub fn accessor<R: 'static, T: 'static>(
        &self,
        key: &AccessorKey<R, T>,
        accessor: Accessor<R, T>,
    ) -> Result<Effect, ReflectError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.owner()?.admit()?;
        if state.reflect.contains(&key.name) || state.services.declared(&key.name) {
            return Err(ReflectError::Duplicate(key.name.clone()));
        }
        let accessor = Rc::new(accessor);
        let erased: Rc<dyn Any> = accessor;
        let cleanup = self
            .on_dispose_sync({
                let runtime = self.runtime.clone();
                let name = key.name.clone();
                let exact = erased.clone();
                move || {
                    if let Some(state) = runtime.upgrade() {
                        let removed = {
                            let mut entries = state.reflect.entries.borrow_mut();
                            if entries
                                .get(&name)
                                .is_some_and(|current| Rc::ptr_eq(current, &exact))
                            {
                                entries.remove(&name)
                            } else {
                                None
                            }
                        };
                        drop(removed);
                    }
                    Ok::<_, std::convert::Infallible>(())
                }
            })?
            .with_label(format!("ctx.accessor({:?})", key.name));
        state
            .reflect
            .entries
            .borrow_mut()
            .insert(key.name.clone(), erased);
        Ok(cleanup)
    }
    fn accessor_entry<R: 'static, T: 'static>(
        &self,
        key: &AccessorKey<R, T>,
    ) -> Result<Rc<Accessor<R, T>>, ReflectError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let entry = state
            .reflect
            .entries
            .borrow()
            .get(&key.name)
            .cloned()
            .ok_or_else(|| ReflectError::Missing(key.name.clone()))?;
        entry.downcast().map_err(|_| {
            ReflectError::Contract(format!("{} ({})", key.name, type_name::<(R, T)>()))
        })
    }
    /// Read an accessor with the original access authority and explicit receiver.
    pub fn read_accessor<R: 'static, T: 'static>(
        &self,
        key: &AccessorKey<R, T>,
        receiver: Rc<R>,
    ) -> Result<T, ReflectError> {
        let accessor = self.accessor_entry(key)?;
        effect::invoke(|| (accessor.get)(self.clone(), receiver)).map_err(ReflectError::Callback)
    }
    /// Invoke an accessor setter; readonly access is an explicit error.
    pub fn write_accessor<R: 'static, T: 'static>(
        &self,
        key: &AccessorKey<R, T>,
        receiver: Rc<R>,
        value: T,
    ) -> Result<(), ReflectError> {
        let accessor = self.accessor_entry(key)?;
        let set = accessor
            .set
            .as_ref()
            .ok_or_else(|| ReflectError::ReadOnly(key.name.clone()))?;
        effect::invoke(|| set(self.clone(), receiver, value)).map_err(ReflectError::Callback)
    }
}
