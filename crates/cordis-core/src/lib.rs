//! Cordis: local plugin lifecycle, explicitly scoped services, and config updates.
//!
//! Typed plugin grouping, validation, initial load, restart, and parent-owned
//! disposal, named service publication, checked/direct lookup, and retained
//! dependency snapshots are implemented, including isolated slots, explicit
//! bound calls/intercepts, typed reflection/hooks and config update/recovery.
//! Logging, explicit initialization/method injection, public type checks, and
//! core acceptance are implemented. This is not transparent JS Proxy reflection.
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
//! `docs/m1.md` through `docs/m6.md` in the repository
//! for ownership, dispatch, composition, lifecycle, and cancellation contracts.

mod context;
mod effect;
mod error;
mod events;
pub mod hooks;
mod identity;
mod logger;
mod owner;
mod plugin;
mod reflect;
mod runtime;
mod service;
mod setup;

pub use context::Context;
pub use effect::{
    BoxError, CleanupFailure, CleanupReport, Completion, Effect, EffectMeta, LocalFuture,
};
pub use error::{
    AdmissionError, CompletionError, HostError, PathSegment, ValidationError, ValidationIssue,
};
pub use events::{
    AsyncNext, DispatchOptions, Event, EventError, EventTask, Listener, ListenerOptions,
    ListenerRegistration, Next,
};
pub use identity::{FiberId, IsolationId, PublicationId, RuntimeId};
pub use logger::{
    ColorDepth, ErrorValue, ExporterOptions, ExporterRegistration, Level, LogError, LogRecord,
    Logger, LoggerConfig, WeakFiber,
};
pub use plugin::{
    FiberHandle, FiberState, LifecycleError, LifecycleTask, Plugin, Registry, UpdateOptions,
};
pub use reflect::{Accessor, AccessorKey, ReflectError};
pub use runtime::Runtime;
pub use service::{
    BoundService, Dependency, ServiceError, ServiceInvoke, ServiceKey, ServiceRegistration,
};
pub use setup::{Disposer, SetupError};
