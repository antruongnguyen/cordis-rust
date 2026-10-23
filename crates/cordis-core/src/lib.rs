//! Cordis M4: a local plugin runtime with typed services and injection convergence.
//!
//! Typed plugin grouping, validation, initial load, restart, and parent-owned
//! disposal, named service publication, checked/direct lookup, and retained
//! dependency snapshots are implemented. Isolation/intercepts/tracing,
//! configuration updates, and full logger behavior are later milestones.
//!
//! ```
//! use cordis_core::Runtime;
//! use std::{cell::Cell, convert::Infallible, rc::Rc};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let executor = tokio::runtime::Builder::new_current_thread().build()?;
//! executor.block_on(async {
//!     let runtime = Runtime::new();
//!     let context = runtime.context();
//!     let closed = Rc::new(Cell::new(false));
//!     runtime.run(async {
//!         let resource = closed.clone();
//!         let _effect = context.on_dispose(move || async move {
//!             resource.set(true);
//!             Ok::<_, Infallible>(())
//!         })?;
//!         let report = context.drain()?.await?;
//!         assert_eq!(report.attempted(), 1);
//!         assert!(report.failures().is_empty());
//!         Ok::<_, Box<dyn std::error::Error>>(())
//!     }).await??;
//!     assert!(closed.get());
//!     Ok::<_, Box<dyn std::error::Error>>(())
//! })
//! # }
//! ```
//!
//! Contexts, registrations, and completion handles are local, not Send or Sync.
//! The host must remain alive and driven until cleanup finishes. See
//! `docs/m1.md`, `docs/m2.md`, `docs/m3.md`, and `docs/m4.md` in the repository
//! for ownership, dispatch, composition, lifecycle, and cancellation contracts.

mod context;
mod effect;
mod error;
mod events;
mod identity;
mod logger;
mod owner;
mod plugin;
mod runtime;
mod service;
mod setup;

pub use context::Context;
pub use effect::{
    BoxError, CleanupFailure, CleanupReport, Completion, Effect, EffectMeta, LocalFuture,
};
pub use error::{AdmissionError, CompletionError, HostError};
pub use events::{
    AsyncNext, DispatchOptions, Event, EventError, EventTask, Listener, ListenerOptions,
    ListenerRegistration, Next,
};
pub use identity::{FiberId, PublicationId, RuntimeId};
pub use logger::{ExporterOptions, ExporterRegistration, Level, LogError, LogRecord, Logger};
pub use plugin::{FiberHandle, FiberState, LifecycleError, LifecycleTask, Plugin, Registry};
pub use runtime::Runtime;
pub use service::{Dependency, ServiceError, ServiceKey, ServiceRegistration};
pub use setup::{Disposer, SetupError};
