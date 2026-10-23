use crate::{
    AdmissionError, CleanupFailure, Context, Effect,
    effect::{self, BoxError, Cleanup},
};
use futures_util::{Stream, StreamExt};
use std::{
    cell::{Cell, RefCell},
    error::Error,
    fmt,
    future::Future,
    rc::Rc,
};
use tokio::sync::watch;

tokio::task_local! {
    static ACQUIRING: Rc<Acquisition>;
}

struct Acquisition {
    node: Rc<crate::effect::EffectNode>,
    parent: Option<Rc<Self>>,
}
impl Acquisition {
    fn enter(node: Rc<crate::effect::EffectNode>) -> Rc<Self> {
        Rc::new(Self {
            node,
            parent: ACQUIRING.try_with(Rc::clone).ok(),
        })
    }
    fn blocks(&self, candidate: &crate::effect::EffectNode) -> bool {
        candidate.contains(&self.node)
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.blocks(candidate))
    }
}
/// One acquired resource's owned cleanup. Drop is inert; yield it to an effect.
#[must_use = "yield/register this disposer; dropping it does not perform cleanup"]
pub struct Disposer(Cleanup);

impl Disposer {
    /// Wrap a one-shot synchronous-prefix/async cleanup callback.
    pub fn new<F, Fut, E>(callback: F) -> Self
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self(Cleanup::asynchronous(move || {
            let future = callback();
            Box::pin(async move { future.await.map_err(Into::into) })
        }))
    }

    /// Wrap synchronous cleanup with no artificial future boundary.
    pub fn sync<F, E>(callback: F) -> Self
    where
        F: FnOnce() -> Result<(), E> + 'static,
        E: Into<BoxError> + 'static,
    {
        Self(Cleanup::sync(move || callback().map_err(Into::into)))
    }

    /// Transfer an existing registration out of its Fiber journal into composition.
    /// The registration must belong to the current host and be unclaimed. Active
    /// setup cannot transfer itself or an ancestor containing it.
    pub fn from_effect(effect: Effect) -> Result<Self, SetupError> {
        let state = effect
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        if ACQUIRING
            .try_with(|owner| owner.blocks(&effect.node))
            .unwrap_or(false)
        {
            return Err(SetupError::RecursiveComposition);
        }
        let owner = effect
            .owner
            .upgrade()
            .ok_or(AdmissionError::InactiveContext)?;
        let callback = owner
            .cleanups
            .borrow_mut()
            .remove(&effect.id)
            .ok_or(SetupError::AlreadyClaimed)?;
        Ok(Self(callback))
    }

    /// Compose cleanup in reverse order, awaiting each child before the next.
    /// Like TS composed effects, a child failure stops the remaining chain.
    pub fn compose(children: Vec<Self>) -> Self {
        Self(Cleanup::sequence(
            children.into_iter().map(|child| child.0).collect(),
        ))
    }
}

/// Setup admission, callback, or hosting failure. Callback sources are preserved.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SetupError {
    /// No setup code was run because admission failed.
    Admission(AdmissionError),
    /// Setup failed; partial acquisitions were rolled back.
    Callback(CleanupFailure),
    /// An attempted composition transfer already lost its exact claim.
    AlreadyClaimed,
    /// An active setup tried to acquire itself or an ancestor composition.
    RecursiveComposition,
    /// The host stopped before setup/rollback reached its barrier.
    HostStopped,
    /// The setup worker unexpectedly panicked outside callback containment.
    DriverPanicked,
}
impl From<AdmissionError> for SetupError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
    }
}
impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(e) => e.fmt(f),
            Self::Callback(e) => write!(f, "effect setup: {e}"),
            Self::AlreadyClaimed => f.write_str("effect has already been claimed"),
            Self::RecursiveComposition => {
                f.write_str("cannot transfer an active effect or its ancestor into itself")
            }
            Self::HostStopped => f.write_str("host stopped before effect setup completed"),
            Self::DriverPanicked => f.write_str("effect setup worker panicked"),
        }
    }
}
impl Error for SetupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(e) => Some(e),
            Self::Callback(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SetupState {
    stop: Rc<Cell<bool>>,
    result: watch::Receiver<Option<Result<(), SetupError>>>,
}
impl SetupState {
    pub(crate) fn stop(&self) {
        self.stop.set(true);
    }
    pub(crate) async fn wait(&self) -> Result<(), SetupError> {
        let mut result = self.result.clone();
        loop {
            if let Some(result) = result.borrow().clone() {
                return result;
            }
            result
                .changed()
                .await
                .map_err(|_| SetupError::HostStopped)?;
        }
    }
}

impl Context {
    /// Run synchronous setup now, returning one disposer or a composed disposer.
    /// Setup failure is reported and its cleanup registration is retired by rollback.
    pub fn effect<F, E>(&self, setup: F) -> Result<Effect, SetupError>
    where
        F: FnOnce() -> Result<Disposer, E>,
        E: Into<BoxError> + 'static,
    {
        self.effect_iter(|| std::iter::once(setup()))
    }

    /// Collect a synchronous iterator of acquired resources immediately.
    /// On failure, all-sync rollback completes before returning. A pending async
    /// child schedules the remainder on the host. Use effect_stream plus
    /// Effect::ready when a partial async rollback barrier must be observed.
    pub fn effect_iter<F, I, E>(&self, setup: F) -> Result<Effect, SetupError>
    where
        F: FnOnce() -> I,
        I: IntoIterator<Item = Result<Disposer, E>>,
        E: Into<BoxError> + 'static,
    {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        self.owner()?.admit()?;
        let resources = Rc::new(RefCell::new(Vec::new()));
        let owner = self.owner()?;
        let registration = self.register_cleanup(Cleanup::collected(resources.clone()))?;
        let outcome: Result<(), CleanupFailure> = (|| {
            let mut iterator = effect::invoke(|| {
                owner.setup_scope(|| {
                    ACQUIRING.sync_scope(Acquisition::enter(registration.node.clone()), || {
                        Ok(setup().into_iter())
                    })
                })
            })?;
            loop {
                let item = effect::invoke(|| {
                    owner.setup_scope(|| {
                        ACQUIRING.sync_scope(Acquisition::enter(registration.node.clone()), || {
                            iterator.next().transpose().map_err(Into::into)
                        })
                    })
                })?;
                let Some(disposer) = item else { break };
                registration.node.collect(&disposer.0);
                resources.borrow_mut().push(disposer.0);
            }
            Ok(())
        })();
        match outcome {
            Ok(()) => Ok(registration),
            Err(failure) => {
                state.report_for(&owner, failure.clone());
                let rollback = registration.dispose()?;
                drop(rollback);
                Err(SetupError::Callback(failure))
            }
        }
    }

    /// Start async setup which returns a single acquired disposer.
    /// The factory runs now; its future body runs on the local host.
    pub fn effect_async<F, Fut, E>(&self, setup: F) -> Result<Effect, SetupError>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = Result<Disposer, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        self.effect_stream(move || {
            let future = setup();
            futures_util::stream::once(future)
        })
    }

    /// Start a stream of acquired disposers, collecting each yield exactly once.
    /// Disposal stops advancing before the next poll, but retains an in-flight
    /// final acquisition and waits for its cleanup. Partial failures roll back
    /// before ready resolves. Root drain can await setup without deadlocking it.
    pub fn effect_stream<F, S, E>(&self, setup: F) -> Result<Effect, SetupError>
    where
        F: FnOnce() -> S + 'static,
        S: Stream<Item = Result<Disposer, E>> + 'static,
        E: Into<BoxError> + 'static,
    {
        let state = self
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        let host = state.admit()?;
        let owner = self.owner()?;
        owner.admit()?;
        let resources = Rc::new(RefCell::new(Vec::new()));
        let (sender, receiver) = watch::channel(None);
        let setup_state = SetupState {
            stop: Rc::new(Cell::new(false)),
            result: receiver,
        };
        {
            let mut acquisitions = owner.acquisitions.borrow_mut();
            acquisitions.retain(|flag| flag.strong_count() > 0);
            acquisitions.push(Rc::downgrade(&setup_state.stop));
        }
        let mut registration = self.on_dispose({
            let setup = setup_state.clone();
            let resources = resources.clone();
            move || {
                setup.stop();
                async move {
                    match setup.wait().await {
                        Ok(()) | Err(SetupError::Callback(_)) => {}
                        Err(error) => return Err(Box::new(error) as BoxError),
                    }
                    let callbacks = std::mem::take(&mut *resources.borrow_mut());
                    Cleanup::sequence(callbacks).run().finish().await
                }
            }
        })?;
        registration.setup = Some(setup_state.clone());
        let stream = match effect::invoke(|| {
            owner.setup_scope(|| {
                ACQUIRING.sync_scope(Acquisition::enter(registration.node.clone()), || {
                    Ok(Box::pin(setup()))
                })
            })
        }) {
            Ok(stream) => stream,
            Err(failure) => {
                state.report_for(&owner, failure.clone());
                sender.send_replace(Some(Err(SetupError::Callback(failure.clone()))));
                drop(registration.dispose()?);
                return Err(SetupError::Callback(failure));
            }
        };
        let id = registration.id;
        let worker_state = state.clone();
        let worker_owner = owner.clone();
        let node = registration.node.clone();
        let worker = state.spawn(&host, async move {
            let mut stream = stream;
            let outcome: Result<(), CleanupFailure> = async {
                let mut first = true;
                while first || !setup_state.stop.get() {
                    first = false;
                    let item = effect::invoke_async(worker_owner.cleanup_scope(
                        ACQUIRING.scope(Acquisition::enter(node.clone()), async {
                            stream.next().await.transpose().map_err(Into::into)
                        }),
                    ))
                    .await?;
                    let Some(disposer) = item else {
                        break;
                    };
                    node.collect(&disposer.0);
                    resources.borrow_mut().push(disposer.0);
                    tokio::task::yield_now().await;
                }
                Ok(())
            }
            .await;
            if let Err(failure) = &outcome {
                let callbacks = std::mem::take(&mut *resources.borrow_mut());
                if let Err(cleanup) = effect::invoke_async(
                    worker_owner
                        .cleanup_scope(async { Cleanup::sequence(callbacks).run().finish().await }),
                )
                .await
                {
                    worker_state.report_for(&worker_owner, cleanup);
                }
                worker_state.report_for(&worker_owner, failure.clone());
                let callback = worker_owner.cleanups.borrow_mut().remove(&id);
                drop(callback);
            }
            outcome.map_err(SetupError::Callback)
        });
        state.spawn(&host, async move {
            let outcome = match worker.await {
                Ok(outcome) => outcome,
                Err(error) => {
                    state_driver_error(&error);
                    Err(if error.is_panic() {
                        SetupError::DriverPanicked
                    } else {
                        SetupError::HostStopped
                    })
                }
            };
            sender.send_replace(Some(outcome));
        });
        Ok(registration)
    }
}

fn state_driver_error(error: &tokio::task::JoinError) {
    eprintln!("cordis: effect setup driver did not complete: {error}");
}
