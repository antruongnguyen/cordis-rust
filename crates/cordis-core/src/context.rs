use crate::{
    AdmissionError, Completion, Effect, FiberId, RuntimeId, effect::Cleanup, runtime::State,
};
use std::{
    any::{Any, TypeId},
    fmt,
    future::Future,
    rc::{Rc, Weak},
};

struct Metadata {
    parent: Option<Rc<Metadata>>,
    contract: TypeId,
    value: Rc<dyn Any>,
}

/// A cheap, immutable view with typed, parent-layered metadata.
///
/// Context does not own the runtime or its cleanup journal. Capturing a Context
/// in a registered callback therefore creates no internal ownership cycle.
/// After the host is dropped, metadata and identities remain readable, but
/// admitting work returns [`AdmissionError::RuntimeDropped`].
///
/// ```compile_fail
/// use cordis_core::Runtime;
/// let runtime = Runtime::new();
/// let context = runtime.context();
/// std::thread::spawn(move || drop(context)); // Context is intentionally !Send
/// ```
#[derive(Clone)]
pub struct Context {
    pub(crate) runtime: Weak<State>,
    pub(crate) owner: Weak<crate::owner::Owner>,
    runtime_id: RuntimeId,
    fiber_id: FiberId,
    root_id: FiberId,
    name: Rc<str>,
    metadata: Option<Rc<Metadata>>,
}

impl Context {
    pub(crate) fn owner(&self) -> Result<Rc<crate::owner::Owner>, AdmissionError> {
        self.owner.upgrade().ok_or(AdmissionError::InactiveContext)
    }
    pub(crate) fn with_owner(&self, owner: &Rc<crate::owner::Owner>) -> Self {
        Self {
            owner: Rc::downgrade(owner),
            fiber_id: owner.id.clone(),
            name: Rc::from(owner.name.as_str()),
            ..self.clone()
        }
    }
    pub(crate) fn new(runtime: &Rc<State>) -> Self {
        Self {
            runtime: Rc::downgrade(runtime),
            owner: Rc::downgrade(&runtime.root),
            runtime_id: runtime.id.clone(),
            fiber_id: runtime.root.id.clone(),
            root_id: runtime.root.id.clone(),
            name: Rc::from(runtime.root.name.as_str()),
            metadata: None,
        }
    }

    /// Identity of the runtime shared by all derived views.
    pub fn runtime_id(&self) -> &RuntimeId {
        &self.runtime_id
    }

    /// Identity of the selected Fiber owning registrations through this view.
    pub fn fiber_id(&self) -> &FiberId {
        &self.fiber_id
    }

    /// Diagnostic name of the selected Fiber, inherited from the nearest named
    /// plugin at creation. Remains readable after the runtime is dropped.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Derive a view shadowing metadata of type T without mutating its parent.
    #[must_use]
    pub fn extend<T: 'static>(&self, value: T) -> Self {
        Self {
            metadata: Some(Rc::new(Metadata {
                parent: self.metadata.clone(),
                contract: TypeId::of::<T>(),
                value: Rc::new(value),
            })),
            ..self.clone()
        }
    }

    /// Read the nearest metadata layer of type T, or None if it is absent.
    pub fn metadata<T: 'static>(&self) -> Option<Rc<T>> {
        let mut cursor = self.metadata.as_ref();
        while let Some(layer) = cursor {
            if layer.contract == TypeId::of::<T>() {
                return Some(
                    layer
                        .value
                        .clone()
                        .downcast::<T>()
                        .expect("metadata type matches its contract"),
                );
            }
            cursor = layer.parent.as_ref();
        }
        None
    }

    /// Return a root view in the same runtime, discarding derived metadata.
    #[must_use]
    pub fn root(&self) -> Self {
        let mut root = Self {
            metadata: None,
            fiber_id: self.root_id.clone(),
            name: Rc::from("root"),
            ..self.clone()
        };
        if let Some(state) = self.runtime.upgrade() {
            root.owner = Rc::downgrade(&state.root);
            root.fiber_id = state.root.id.clone();
        }
        root
    }

    /// Register a one-shot async cleanup on the selected Fiber.
    ///
    /// The resource must already be acquired. Use effect/effect_stream when
    /// setup and partial acquisition need ownership. Dropping this token does not
    /// unregister it. Callbacks may capture this Context without retaining the
    /// runtime. Admission requires the owning [`crate::Runtime::run`] scope.
    pub fn on_dispose<F, Fut, E>(&self, cleanup: F) -> Result<Effect, AdmissionError>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: Into<crate::BoxError> + 'static,
    {
        let callback = Cleanup::asynchronous(move || {
            let future = cleanup();
            Box::pin(async move { future.await.map_err(Into::into) })
        });
        self.register_cleanup(callback)
    }

    /// Register an all-synchronous cleanup. Manual disposal runs it before
    /// returning; errors/panics are reported and retained in its completion.
    pub fn on_dispose_sync<F, E>(&self, cleanup: F) -> Result<Effect, AdmissionError>
    where
        F: FnOnce() -> Result<(), E> + 'static,
        E: Into<crate::BoxError> + 'static,
    {
        self.register_cleanup(Cleanup::sync(move || cleanup().map_err(Into::into)))
    }

    pub(crate) fn register_cleanup(&self, cleanup: Cleanup) -> Result<Effect, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.owner()?.admit()?;
        let id = state
            .next
            .get()
            .checked_add(1)
            .ok_or(AdmissionError::IdentityExhausted)?;
        state.next.set(id);
        Ok(self.insert_cleanup(id, cleanup))
    }

    fn insert_cleanup(&self, id: u64, mut cleanup: Cleanup) -> Effect {
        let node =
            crate::effect::EffectNode::new("anonymous".into(), std::mem::take(&mut cleanup.nodes));
        cleanup.nodes.push(node.clone());
        self.owner
            .upgrade()
            .expect("registration owner was admitted")
            .cleanups
            .borrow_mut()
            .insert(id, cleanup);
        Effect::new(self.runtime.clone(), self.owner.clone(), id, node)
    }

    /// Snapshot this Fiber's live effects in registration order, including transferred
    /// nested effects in acquisition order. Does not retain any resource.
    pub fn get_effects(&self) -> Result<Vec<crate::EffectMeta>, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let owner = self.owner()?;
        drop(state);
        Ok(owner
            .cleanups
            .borrow()
            .values()
            .flat_map(|cleanup| cleanup.nodes.iter().map(|node| node.snapshot()))
            .collect())
    }

    /// Start a reusable resource-journal drain now; the handle only waits.
    ///
    /// All currently registered cleanups are claimed in reverse registration
    /// order and polled concurrently. Registrations made after this snapshot
    /// belong to the next drain. Concurrent drain requests share the current
    /// operation. This does not dispose a plugin or change its state; use its
    /// FiberHandle for lifecycle control. Owned services stop admitting consumers
    /// during the drain; their exact dependents finish before resources are cleaned.
    /// A self/ancestor drain from setup or
    /// cleanup is refused before admission. Root drain also owns child disposal.
    pub fn drain(&self) -> Result<Completion, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        let owner = self.owner()?;
        owner.assert_not_cleaning()?;
        if let Some(receiver) = owner.draining.borrow().as_ref() {
            return Ok(Completion::waiting(receiver.clone()));
        }
        let callbacks = std::mem::take(&mut *owner.cleanups.borrow_mut())
            .into_values()
            .rev()
            .collect();
        Ok(state.start(&host, owner, callbacks, true))
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Context <{}>", self.name)
    }
}
