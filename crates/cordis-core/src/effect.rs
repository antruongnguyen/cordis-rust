use crate::{AdmissionError, CompletionError, runtime::State};
use futures_util::future::LocalBoxFuture;
use std::{
    cell::{Cell, RefCell},
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
};
use tokio::sync::watch;

/// A value-only snapshot of an effect's label and transferred child effects.
/// This contains no callbacks, Context values, or resource ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectMeta {
    /// Diagnostic label; anonymous unless explicitly named.
    pub label: String,
    /// Nested effect registrations in acquisition order (not cleanup order).
    pub children: Vec<EffectMeta>,
}

pub(crate) struct EffectNode {
    label: RefCell<String>,
    children: RefCell<Vec<Rc<EffectNode>>>,
}
impl EffectNode {
    pub(crate) fn new(label: String, children: Vec<Rc<Self>>) -> Rc<Self> {
        Rc::new(Self {
            label: RefCell::new(label),
            children: RefCell::new(children),
        })
    }
    pub(crate) fn collect(&self, cleanup: &Cleanup) {
        self.children
            .borrow_mut()
            .extend(cleanup.nodes.iter().cloned());
    }
    pub(crate) fn contains(&self, target: &Rc<Self>) -> bool {
        std::ptr::eq(self, &**target)
            || self
                .children
                .borrow()
                .iter()
                .any(|child| child.contains(target))
    }

    pub(crate) fn snapshot(&self) -> EffectMeta {
        EffectMeta {
            label: self.label.borrow().clone(),
            children: self
                .children
                .borrow()
                .iter()
                .map(|node| node.snapshot())
                .collect(),
        }
    }
}

pub(crate) enum CleanupStep {
    Ready(Result<(), BoxError>),
    Pending(LocalFuture<Result<(), BoxError>>),
}
impl CleanupStep {
    pub(crate) async fn finish(self) -> Result<(), BoxError> {
        match self {
            Self::Ready(result) => result,
            Self::Pending(future) => future.await,
        }
    }
}

pub(crate) struct Cleanup {
    callback: Box<dyn FnOnce() -> CleanupStep>,
    pub(crate) nodes: Vec<Rc<EffectNode>>,
}
impl Cleanup {
    pub(crate) fn sync(callback: impl FnOnce() -> Result<(), BoxError> + 'static) -> Self {
        Self {
            callback: Box::new(move || CleanupStep::Ready(callback())),
            nodes: Vec::new(),
        }
    }
    pub(crate) fn asynchronous(
        callback: impl FnOnce() -> LocalFuture<Result<(), BoxError>> + 'static,
    ) -> Self {
        Self {
            callback: Box::new(move || CleanupStep::Pending(callback())),
            nodes: Vec::new(),
        }
    }
    pub(crate) fn sequence(callbacks: Vec<Self>) -> Self {
        let nodes = callbacks
            .iter()
            .flat_map(|callback| callback.nodes.iter().cloned())
            .collect();
        Self {
            callback: Box::new(move || run_sequence(callbacks)),
            nodes,
        }
    }
    pub(crate) fn collected(callbacks: Rc<RefCell<Vec<Self>>>) -> Self {
        Self {
            callback: Box::new(move || {
                let callbacks = std::mem::take(&mut *callbacks.borrow_mut());
                run_sequence(callbacks)
            }),
            nodes: Vec::new(),
        }
    }
    pub(crate) fn run(self) -> CleanupStep {
        (self.callback)()
    }
}

fn run_sequence(mut callbacks: Vec<Cleanup>) -> CleanupStep {
    while let Some(callback) = callbacks.pop() {
        match callback.run() {
            CleanupStep::Ready(Ok(())) => {}
            CleanupStep::Ready(Err(error)) => return CleanupStep::Ready(Err(error)),
            CleanupStep::Pending(future) => {
                return CleanupStep::Pending(Box::pin(async move {
                    future.await?;
                    run_sequence(callbacks).finish().await
                }));
            }
        }
    }
    CleanupStep::Ready(Ok(()))
}

pub(crate) enum StartedCleanup {
    Ready(Result<(), CleanupFailure>),
    Pending(LocalFuture<Result<(), CleanupFailure>>),
}
impl StartedCleanup {
    pub(crate) async fn finish(self) -> Result<(), CleanupFailure> {
        match self {
            Self::Ready(result) => result,
            Self::Pending(future) => future.await,
        }
    }
}
pub(crate) fn begin(callback: Cleanup) -> StartedCleanup {
    match invoke(|| Ok(callback.run())) {
        Err(failure) => StartedCleanup::Ready(Err(failure)),
        Ok(CleanupStep::Ready(result)) => {
            StartedCleanup::Ready(result.map_err(CleanupFailure::returned))
        }
        Ok(CleanupStep::Pending(future)) => StartedCleanup::Pending(Box::pin(invoke_async(future))),
    }
}
pub(crate) type Outcome = Result<CleanupReport, CompletionError>;

/// An owned local callback future.
pub type LocalFuture<T> = LocalBoxFuture<'static, T>;
/// A retained local callback error without a Send/Sync requirement.
pub type BoxError = Box<dyn Error>;

impl CleanupFailure {
    pub(crate) fn claim_diagnostic(&self) -> bool {
        !self.reported.replace(true)
    }
    pub(crate) fn returned(error: BoxError) -> Self {
        match error.downcast::<Self>() {
            Ok(failure) => *failure,
            Err(error) => Self::from_source(Rc::from(error)),
        }
    }
    pub(crate) fn from_source(source: Rc<dyn Error>) -> Self {
        Self {
            source,
            panicked: false,
            reported: Rc::new(Cell::new(false)),
        }
    }
    pub(crate) fn panic(payload: Box<dyn std::any::Any + Send>) -> Self {
        let message = if let Some(text) = payload.downcast_ref::<String>() {
            text.clone()
        } else if let Some(text) = payload.downcast_ref::<&str>() {
            (*text).to_owned()
        } else {
            "non-string panic payload".to_owned()
        };
        Self {
            source: Rc::new(PanicMessage(message)),
            panicked: true,
            reported: Rc::new(Cell::new(false)),
        }
    }
}

#[derive(Debug)]
struct PanicMessage(String);
impl fmt::Display for PanicMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for PanicMessage {}

pub(crate) fn invoke<T>(
    callback: impl FnOnce() -> Result<T, BoxError>,
) -> Result<T, CleanupFailure> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        Ok(result) => result.map_err(CleanupFailure::returned),
        Err(payload) => Err(CleanupFailure::panic(payload)),
    }
}

pub(crate) async fn invoke_async<T>(
    future: impl Future<Output = Result<T, BoxError>>,
) -> Result<T, CleanupFailure> {
    use futures_util::FutureExt;
    match std::panic::AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => result.map_err(CleanupFailure::returned),
        Err(payload) => Err(CleanupFailure::panic(payload)),
    }
}
pub(crate) type Receiver = watch::Receiver<Option<Outcome>>;

/// One returned error or panic from a user callback, retaining its error source.
#[derive(Clone, Debug)]
pub struct CleanupFailure {
    pub(crate) source: Rc<dyn Error>,
    pub(crate) panicked: bool,
    reported: Rc<Cell<bool>>,
}

impl CleanupFailure {
    /// Whether the failure came from an unwind rather than a returned error.
    pub fn is_panic(&self) -> bool {
        self.panicked
    }
}

impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.panicked {
            write!(f, "callback panicked: {}", self.source)
        } else {
            write!(f, "callback failed: {}", self.source)
        }
    }
}
impl Error for CleanupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.source)
    }
}

/// A completed cleanup barrier, including every individual user failure.
///
/// Completion does not imply that callbacks were error-free. Each failure is
/// reported to stderr, retained by the runtime, and included here.
#[derive(Clone, Debug, Default)]
pub struct CleanupReport {
    pub(crate) attempted: usize,
    pub(crate) failures: Vec<CleanupFailure>,
}

impl CleanupReport {
    /// Number of callbacks claimed by this operation.
    pub fn attempted(&self) -> usize {
        self.attempted
    }
    /// Returned errors and caught user panics in reverse registration order.
    pub fn failures(&self) -> &[CleanupFailure] {
        &self.failures
    }
}

/// A wait-only handle to already-started cleanup. Dropping it does not cancel.
#[must_use = "cleanup has started; await its barrier or explicitly drop this waiter"]
pub struct Completion(LocalBoxFuture<'static, Outcome>);

impl Completion {
    pub(crate) fn waiting(mut receiver: Receiver) -> Self {
        Self(Box::pin(async move {
            loop {
                if let Some(outcome) = receiver.borrow().clone() {
                    return outcome;
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| CompletionError::HostStopped)?;
            }
        }))
    }

    pub(crate) fn empty() -> Self {
        Self(Box::pin(async { Ok(CleanupReport::default()) }))
    }
}

impl Future for Completion {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// Exact manual control of one Fiber-owned cleanup registration; Drop is inert.
pub struct Effect {
    pub(crate) runtime: Weak<State>,
    pub(crate) owner: Weak<crate::owner::Owner>,
    pub(crate) id: u64,
    pub(crate) setup: Option<crate::setup::SetupState>,
    pub(crate) node: Rc<EffectNode>,
}

impl Effect {
    pub(crate) fn new(
        runtime: Weak<State>,
        owner: Weak<crate::owner::Owner>,
        id: u64,
        node: Rc<EffectNode>,
    ) -> Self {
        Self {
            runtime,
            owner,
            id,
            setup: None,
            node,
        }
    }

    /// Wait for asynchronous setup; does not start or dispose the effect.
    /// Setup failure is reported only after partial-acquisition rollback.
    pub async fn ready(&self) -> Result<(), crate::SetupError> {
        match &self.setup {
            Some(setup) => setup.wait().await,
            None => Ok(()),
        }
    }

    /// Name this effect for introspection. The label follows transfers into
    /// composition without altering ownership.
    #[must_use]
    pub fn with_label(self, label: impl Into<String>) -> Self {
        let old = self.node.label.replace(label.into());
        drop(old);
        self
    }
    /// Claim this callback and start cleanup now. An all-synchronous chain
    /// finishes before this method returns; an async factory's synchronous
    /// prefix also starts now. If an automatic drain already claimed this
    /// registration, report zero attempts instead. That result does not
    /// wait for a different operation's work. Admission errors do not unregister
    /// the callback: it remains owned by its Fiber for a later drain.
    pub fn dispose(self) -> Result<Completion, AdmissionError> {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or(AdmissionError::InactiveContext)?;
        let callback = owner.cleanups.borrow_mut().remove(&self.id);
        if callback.is_some() {
            if let Some(setup) = &self.setup {
                setup.stop();
            }
        }
        Ok(match callback {
            Some(callback) => state.start(&host, owner, vec![callback], false),
            None => Completion::empty(),
        })
    }
}
