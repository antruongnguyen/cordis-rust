use std::{error::Error, fmt};

/// Why a local host could not be driven. No work is admitted by this failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HostError {
    /// Runtime::run was polled without a Tokio executor.
    MissingExecutor,
    /// A Cordis host was already being driven on this task.
    NestedHost,
    /// Another future is already driving this host.
    AlreadyRunning,
    /// LocalSet must be driven from an executor entry point, not a spawned task.
    SpawnedTask,
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MissingExecutor => "a Tokio executor is required to drive the Cordis host",
            Self::NestedHost => "Cordis hosts cannot be nested in the same task",
            Self::AlreadyRunning => "this Cordis host is already being driven",
            Self::SpawnedTask => {
                "drive the Cordis host from Tokio block_on/main/test, not a spawned task"
            }
        })
    }
}
impl Error for HostError {}

/// Why a registration or cleanup request was refused before committing work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdmissionError {
    /// The owning runtime was dropped; Context is a non-owning view.
    RuntimeDropped,
    /// The request was not made inside its runtime's run scope.
    HostNotRunning,
    /// The request was made inside another runtime's run scope.
    WrongHost,
    /// A root drain was requested from that root's effect setup/cleanup callback.
    ReentrantDrain,
    /// The registration identity space is exhausted.
    IdentityExhausted,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RuntimeDropped => "the owning Cordis runtime has been dropped",
            Self::HostNotRunning => {
                "enter the owning runtime with Runtime::run before admitting work"
            }
            Self::WrongHost => "the request belongs to a different Cordis host",
            Self::ReentrantDrain => {
                "cannot drain the root from its own effect setup/cleanup callback"
            }
            Self::IdentityExhausted => "cleanup registration identity space exhausted",
        })
    }
}
impl Error for AdmissionError {}

/// A started operation could not reach its cleanup barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompletionError {
    /// The host was destroyed before the operation could complete.
    HostStopped,
    /// Framework work panicked; its task failure was reported, not repaired.
    DriverPanicked,
}

impl fmt::Display for CompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::HostStopped => "the host stopped before cleanup completed",
            Self::DriverPanicked => "the Cordis cleanup driver panicked",
        })
    }
}
impl Error for CompletionError {}
