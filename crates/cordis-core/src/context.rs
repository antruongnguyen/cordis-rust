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
    runtime_id: RuntimeId,
    fiber_id: FiberId,
    metadata: Option<Rc<Metadata>>,
}

impl Context {
    pub(crate) fn new(runtime: &Rc<State>) -> Self {
        Self {
            runtime: Rc::downgrade(runtime),
            runtime_id: runtime.id.clone(),
            fiber_id: runtime.root_id.clone(),
            metadata: None,
        }
    }

    /// Identity of the runtime shared by all derived views.
    pub fn runtime_id(&self) -> &RuntimeId {
        &self.runtime_id
    }

    /// Identity of the root Fiber owning registrations through this view.
    pub fn fiber_id(&self) -> &FiberId {
        &self.fiber_id
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
        Self {
            metadata: None,
            ..self.clone()
        }
    }

    /// Register a one-shot async cleanup on the root Fiber.
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
        let id = state
            .next
            .get()
            .checked_add(1)
            .ok_or(AdmissionError::IdentityExhausted)?;
        state.next.set(id);
        Ok(self.insert_cleanup(&state, id, cleanup))
    }

    fn insert_cleanup(&self, state: &Rc<State>, id: u64, mut cleanup: Cleanup) -> Effect {
        let node =
            crate::effect::EffectNode::new("anonymous".into(), std::mem::take(&mut cleanup.nodes));
        cleanup.nodes.push(node.clone());
        state.cleanups.borrow_mut().insert(id, cleanup);
        Effect::new(self.runtime.clone(), id, node)
    }

    /// Snapshot live root effects in registration order, including transferred
    /// nested effects in acquisition order. Does not retain any resource.
    pub fn get_effects(&self) -> Result<Vec<crate::EffectMeta>, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        Ok(state
            .cleanups
            .borrow()
            .values()
            .flat_map(|cleanup| cleanup.nodes.iter().map(|node| node.snapshot()))
            .collect())
    }

    /// Start a reusable root drain now; the returned handle only waits.
    ///
    /// All currently registered cleanups are claimed in reverse registration
    /// order and polled concurrently. Registrations made after this snapshot
    /// belong to the next drain. Concurrent drain requests share the current
    /// operation. A call from this root's own cleanup is refused before admission.
    pub fn drain(&self) -> Result<Completion, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        state.assert_not_cleaning()?;
        if let Some(receiver) = state.draining.borrow().as_ref() {
            return Ok(Completion::waiting(receiver.clone()));
        }
        let callbacks = std::mem::take(&mut *state.cleanups.borrow_mut())
            .into_values()
            .rev()
            .collect();
        Ok(state.start(&host, callbacks, true))
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("runtime", &self.runtime_id)
            .field("fiber", &self.fiber_id)
            .finish_non_exhaustive()
    }
}
