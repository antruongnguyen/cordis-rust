//! Typed equivalents of Cordis's internal hook protocols.
use crate::{BoxError, Context, Event, FiberId, FiberState, IsolationId, ServiceError};
use std::{
    any::{Any, type_name},
    fmt,
    rc::Rc,
};

/// Checked heterogeneous hook payload. No unchecked downcasts are exposed.
#[derive(Clone)]
pub struct Value {
    pub(crate) inner: Rc<dyn Any>,
    pub(crate) actual: &'static str,
}
impl Value {
    /// Test a payload contract without treating a nonmatching event as a failure.
    pub fn is<T: 'static>(&self) -> bool {
        self.inner.is::<T>()
    }
    /// Own a typed payload.
    pub fn new<T: 'static>(value: T) -> Self {
        Self::from_rc(Rc::new(value))
    }
    /// Share an existing typed payload.
    pub fn from_rc<T: 'static>(value: Rc<T>) -> Self {
        Self {
            inner: value,
            actual: type_name::<T>(),
        }
    }
    /// Read a payload with a checked Rust contract.
    pub fn get<T: 'static>(&self) -> Result<Rc<T>, ServiceError> {
        self.inner
            .clone()
            .downcast()
            .map_err(|_| ServiceError::TypeMismatch {
                name: "hook payload".into(),
                expected: type_name::<T>(),
                actual: self.actual,
            })
    }
}
impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Value").field(&self.actual).finish()
    }
}

/// Value-only lifecycle snapshot; retaining it cannot retain a Fiber.
#[derive(Clone, Debug)]
pub struct FiberInfo {
    /// Stable instance identity.
    pub id: FiberId,
    /// Captured diagnostic name.
    pub name: String,
    /// Current state.
    pub state: FiberState,
    /// Whether the instance is still admitted, independent of cleanup completion.
    pub live: bool,
}
/// One observable lifecycle transition.
#[derive(Clone, Debug)]
pub struct Status {
    /// New instance snapshot.
    pub fiber: FiberInfo,
    /// Previous state.
    pub previous: FiberState,
}
/// Source config commit notification. noSave suppresses only this hook.
#[derive(Clone, Debug)]
pub struct Commit {
    /// Updated instance.
    pub fiber: FiberInfo,
    /// Original source config, not the validated apply input.
    pub source: Value,
}
/// Source transformation middleware input, before synchronous validation.
#[derive(Clone, Debug)]
pub struct Config {
    /// Instance being resolved.
    pub fiber: FiberInfo,
    /// Candidate source config.
    pub source: Value,
}
/// Per-Fiber resolved-config update middleware.
#[derive(Clone, Debug)]
pub struct Update {
    /// Target instance.
    pub fiber: FiberInfo,
    /// Validated apply input, distinct from retained source.
    pub resolved: Value,
}
/// Service availability change, filtered by its exact realm.
#[derive(Clone, Debug)]
pub struct ServiceChange {
    /// Service name whose availability changed.
    pub name: String,
    /// Exact isolation slot.
    pub slot: IsolationId,
    /// Current value, even if its provider has just become inactive.
    pub value: Option<Value>,
}
/// Checked service access middleware input.
#[derive(Clone, Debug)]
pub struct Get {
    /// Accessing traced Context.
    pub context: Context,
    /// Requested service name.
    pub name: String,
}
/// Owned service mutation middleware input.
#[derive(Clone, Debug)]
pub struct Set {
    /// Mutating use-site Context.
    pub context: Context,
    /// Requested service name.
    pub name: String,
    /// Candidate value.
    pub value: Value,
}
/// Listener-registration interception. Use replace to substitute a typed listener
/// without recursively reentering the interception protocol.
#[derive(Clone)]
pub struct ListenerRequest {
    pub(crate) context: Context,
    pub(crate) event: Value,
    pub(crate) listener: Value,
    pub(crate) options: crate::ListenerOptions,
    pub(crate) once: bool,
}
impl ListenerRequest {
    /// Registration Context.
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    /// Original typed Event, recoverable through Value::get.
    pub fn event(&self) -> &Value {
        &self.event
    }
    /// Original typed Listener, recoverable through Value::get.
    pub fn listener(&self) -> &Value {
        &self.listener
    }
    /// Registration options.
    pub fn options(&self) -> crate::ListenerOptions {
        self.options
    }
}
/// Observation of a non-internal event dispatch, before callback selection.
#[derive(Clone, Debug)]
pub struct Dispatch {
    /// One of emit, parallel, serial, bail, waterfall.
    pub mode: &'static str,
    /// Exact typed Event, including symbol identity.
    pub event: Value,
    /// Typed argument value.
    pub arguments: Value,
    /// Dispatch use-site Context.
    pub context: Context,
}

/// Plugin admission/removal observations.
pub fn plugin() -> Event<FiberInfo> {
    Event::named("internal/plugin")
}
/// State transition observations.
pub fn status() -> Event<Status> {
    Event::named("internal/status")
}
/// Source commit observations.
pub fn commit() -> Event<Commit> {
    Event::named("internal/commit")
}
/// Source transformation around-middleware.
pub fn config() -> Event<Config, Value> {
    Event::named("internal/config")
}
/// Resolved update around-middleware. Calling next runs restart; false accepts/vetoes it.
pub fn update() -> Event<Update, bool> {
    Event::named("internal/update")
}
/// Realm-filtered service availability observations.
pub fn service() -> Event<ServiceChange> {
    Event::named("internal/service")
}
/// Checked get around-middleware.
pub fn get() -> Event<Get, Option<Value>> {
    Event::named("internal/get")
}
/// Owned set around-middleware.
pub fn set() -> Event<Set, ()> {
    Event::named("internal/set")
}
/// Listener registration interception; Break supplies the replacement registration.
pub fn listener() -> Event<ListenerRequest, crate::ListenerRegistration> {
    Event::named("internal/listener")
}
/// Non-internal event dispatch observations.
pub fn dispatch() -> Event<Dispatch> {
    Event::named("internal/dispatch")
}

pub(crate) fn observe<A: Clone + 'static>(
    ctx: &Context,
    event: &Event<A>,
    args: A,
    options: crate::DispatchOptions,
) {
    match ctx.emit(event, args, options) {
        Ok(task) => drop(task),
        Err(error) => {
            if let (Some(state), Ok(owner)) = (ctx.runtime.upgrade(), ctx.owner()) {
                state.report_for(
                    &owner,
                    crate::CleanupFailure::returned(Box::new(error) as BoxError),
                );
            }
        }
    }
}
