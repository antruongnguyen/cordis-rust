use crate::{
    AdmissionError, CleanupFailure, CleanupReport, Completion, CompletionError, Context, HostError,
    RuntimeId,
    effect::{Cleanup, StartedCleanup, begin},
    owner::Owner,
};
use futures_util::future::join_all;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    panic::AssertUnwindSafe,
    rc::{Rc, Weak},
};
use tokio::{sync::watch, task::LocalSet};

tokio::task_local! {
    static HOST: RuntimeId;
}

pub(crate) struct State {
    pub(crate) id: RuntimeId,
    pub(crate) root: Rc<Owner>,
    pub(crate) next: Cell<u64>,
    host: Weak<LocalSet>,
    failures: RefCell<Vec<CleanupFailure>>,
    pub(crate) events: crate::events::EventStore,
    pub(crate) logger: crate::logger::LogStore,
    pub(crate) registry: crate::plugin::RegistryStore,
    pub(crate) services: crate::service::ServiceStore,
    pub(crate) reflect: crate::reflect::ReflectStore,
}

impl State {
    pub(crate) fn spawn<F>(&self, host: &LocalSet, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        host.spawn_local(HOST.scope(self.id.clone(), future))
    }

    pub(crate) fn report(&self, failure: CleanupFailure) {
        self.report_for(&self.root, failure);
    }

    pub(crate) fn report_for(&self, owner: &Owner, failure: CleanupFailure) {
        if !failure.claim_diagnostic() {
            return;
        }
        let diagnostic = std::panic::catch_unwind(AssertUnwindSafe(|| failure.to_string()));
        let text = match diagnostic {
            Ok(text) => text,
            Err(_) => {
                "callback failed; its diagnostic formatter panicked (source retained)".to_owned()
            }
        };
        eprintln!("cordis: {text}");
        self.failures.borrow_mut().push(failure.clone());
        let result = self.logger.write(
            if owner.is_root() {
                "cordis".into()
            } else {
                crate::logger::normalize_name(&owner.name)
            },
            crate::Level::Error,
            text,
            crate::logger::WeakFiber {
                id: owner.id.clone(),
                owner: owner.weak.clone(),
            },
            Some(failure),
            crate::Level::Info,
        );
        match result {
            Ok(_) => {}
            Err(crate::LogError::Exporters(failures)) => {
                eprintln!(
                    "cordis: {} diagnostic exporters failed (sources retained)",
                    failures.len()
                );
                self.failures.borrow_mut().extend(failures);
            }
            Err(error) => eprintln!("cordis: diagnostic logging failed: {error}"),
        }
    }

    pub(crate) fn admit(&self) -> Result<Rc<LocalSet>, AdmissionError> {
        match HOST.try_with(|id| id == &self.id) {
            Ok(true) => self.host.upgrade().ok_or(AdmissionError::RuntimeDropped),
            Ok(false) => Err(AdmissionError::WrongHost),
            Err(_) => Err(AdmissionError::HostNotRunning),
        }
    }

    pub(crate) fn start(
        self: &Rc<Self>,
        host: &LocalSet,
        journal: Rc<Owner>,
        callbacks: Vec<Cleanup>,
        root_drain: bool,
    ) -> Completion {
        let (sender, receiver) = watch::channel(None);
        if root_drain {
            *journal.draining.borrow_mut() = Some(receiver.clone());
            let available = journal.services_active();
            journal.services_paused.set(true);
            if available {
                crate::service::notify(self, host, &journal);
            }
        }
        let attempted = callbacks.len();
        let (callbacks, started) = if root_drain {
            (callbacks, None)
        } else {
            let started: Vec<_> = callbacks
                .into_iter()
                .map(|callback| journal.setup_scope(|| begin(callback)))
                .collect();
            (Vec::new(), Some(started))
        };
        if started.as_ref().is_some_and(|callbacks| {
            callbacks
                .iter()
                .all(|callback| matches!(callback, StartedCleanup::Ready(_)))
        }) {
            let failures = started
                .expect("manual cleanup was prepared")
                .into_iter()
                .filter_map(|callback| {
                    let StartedCleanup::Ready(result) = callback else {
                        unreachable!("only ready callbacks remain")
                    };
                    result.err()
                })
                .collect();
            let report = CleanupReport {
                attempted,
                failures,
            };
            for failure in report.failures() {
                self.report_for(&journal, failure.clone());
            }
            sender.send_replace(Some(Ok(report)));
            return Completion::waiting(receiver);
        }
        let worker_owner = journal.clone();
        let task = self.spawn(host, async move {
            if root_drain {
                crate::service::wait_dependents(&worker_owner).await;
            }
            let started = started.unwrap_or_else(|| {
                callbacks
                    .into_iter()
                    .map(|callback| worker_owner.setup_scope(|| begin(callback)))
                    .collect()
            });
            let failures = join_all(
                started
                    .into_iter()
                    .map(|callback| worker_owner.cleanup_scope(callback.finish())),
            )
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();
            CleanupReport {
                attempted,
                failures,
            }
        });
        let state = self.clone();
        self.spawn(host, async move {
            let outcome = match task.await {
                Ok(report) => {
                    for failure in report.failures() {
                        state.report_for(&journal, failure.clone());
                    }
                    Ok(report)
                }
                Err(error) => {
                    eprintln!("cordis: cleanup driver did not complete: {error}");
                    Err(if error.is_panic() {
                        CompletionError::DriverPanicked
                    } else {
                        CompletionError::HostStopped
                    })
                }
            };
            if root_drain {
                journal.draining.borrow_mut().take();
                journal.services_paused.set(false);
                if let Ok(host) = state.admit() {
                    if journal.services_active() {
                        crate::service::notify(&state, &host, &journal);
                    }
                } else {
                    eprintln!("cordis: cleanup supervisor lost its host admission");
                    sender.send_replace(Some(Err(CompletionError::DriverPanicked)));
                    return;
                }
            }
            sender.send_replace(Some(outcome));
        });
        Completion::waiting(receiver)
    }
}

/// The explicit owner and local task host for one Cordis runtime.
///
/// No executor is created implicitly. Drive this host from a Tokio runtime's
/// block_on/main/test entry point with [`Runtime::run`], not a Tokio-spawned
/// task. Await drains before dropping this host; Drop cannot run async cleanup.
/// Runtime is deliberately not Clone, Send, or Sync.
pub struct Runtime {
    state: Rc<State>,
    host: Rc<LocalSet>,
    running: Cell<bool>,
}

struct Running<'a>(&'a Cell<bool>);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl Runtime {
    /// Construct an independent runtime and its permanent root Fiber.
    /// Construction does not need Tokio; admitting work requires run.
    pub fn new() -> Self {
        let host = Rc::new(LocalSet::new());
        let state = Rc::new(State {
            id: RuntimeId::new(),
            root: Owner::new("root".into()),
            next: Cell::new(0),
            host: Rc::downgrade(&host),
            failures: RefCell::new(Vec::new()),
            events: crate::events::EventStore::default(),
            logger: crate::logger::LogStore::default(),
            registry: crate::plugin::RegistryStore::default(),
            services: crate::service::ServiceStore::default(),
            reflect: crate::reflect::ReflectStore::default(),
        });
        Self {
            state,
            host,
            running: Cell::new(false),
        }
    }

    /// Create a non-owning root Context. Pure derivations need no active host.
    pub fn context(&self) -> Context {
        Context::new(&self.state)
    }

    /// Drive local work until the supplied future finishes.
    ///
    /// Returning or dropping this future pauses pending tasks; a later run can
    /// resume them. This is not a shutdown barrier. Nested Cordis run scopes are
    /// rejected. User-spawned Tokio tasks do not inherit admission authority.
    pub async fn run<F: Future>(&self, future: F) -> Result<F::Output, HostError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(HostError::MissingExecutor);
        }
        if HOST.try_with(|_| ()).is_ok() {
            return Err(HostError::NestedHost);
        }
        if tokio::task::try_id().is_some() {
            return Err(HostError::SpawnedTask);
        }
        if self.running.replace(true) {
            return Err(HostError::AlreadyRunning);
        }
        let _running = Running(&self.running);
        Ok(self
            .host
            .run_until(HOST.scope(self.state.id.clone(), future))
            .await)
    }

    /// Take reported callback failures, including those whose waiter was dropped.
    /// Error sources are preserved. This does not clear a completion's report.
    pub fn take_failures(&self) -> Vec<CleanupFailure> {
        std::mem::take(&mut *self.state.failures.borrow_mut())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
