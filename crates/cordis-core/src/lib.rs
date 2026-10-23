//! Cordis M2: a locally hosted runtime, scoped effects, typed events, and diagnostics.
//!
//! This is not yet the full Cordis framework. Plugins, service lookup,
//! and the non-root Fiber state machine are not implemented. Full traced logger
//! naming and JavaScript-specific formatting are later milestones.
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
//! `docs/m1.md` and `docs/m2.md` in the repository for ownership, dispatch,
//! composition, and cancellation contracts.

mod context;
mod effect;
mod error;
mod events;
mod identity;
mod runtime;
mod setup;

mod logger;
pub use context::Context;
pub use effect::{
    BoxError, CleanupFailure, CleanupReport, Completion, Effect, EffectMeta, LocalFuture,
};
pub use error::{AdmissionError, CompletionError, HostError};
pub use events::{
    AsyncNext, DispatchOptions, Event, EventError, EventTask, Listener, ListenerOptions,
    ListenerRegistration, Next,
};
pub use identity::{FiberId, RuntimeId};
pub use logger::{ExporterOptions, ExporterRegistration, Level, LogError, LogRecord, Logger};
pub use runtime::Runtime;
pub use setup::{Disposer, SetupError};
