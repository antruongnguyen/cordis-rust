use crate::{
    AdmissionError, BoxError, CleanupFailure, Context, Effect, FiberId, effect, runtime::State,
};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    rc::{Rc, Weak},
    time::SystemTime,
};

/// Core log severity. Exporters select their own maximum verbosity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// A failed operation.
    Error,
    /// An unusual but recoverable condition.
    Warn,
    /// Ordinary application information.
    Info,
    /// Verbose diagnostic information.
    Debug,
}

/// One immutable structured log record. Fiber identity does not own a lifecycle.
#[derive(Clone, Debug)]
pub struct LogRecord {
    /// Monotonically increasing within this runtime, starting at one.
    pub sequence: u64,
    /// Wall-clock creation time; no monotonic timing guarantee.
    pub timestamp: SystemTime,
    /// Explicit channel name or the selected Fiber's diagnostic name.
    pub name: String,
    /// Severity of the message.
    pub level: Level,
    /// Rust-formatted message; JS printf syntax is not interpreted.
    pub message: String,
    /// Selected Fiber attribution without retaining the runtime or lifecycle.
    pub fiber: FiberId,
    /// Original source for an automatic callback diagnostic.
    pub failure: Option<CleanupFailure>,
}

/// Per-exporter routing overrides. A name-specific level wins over the default.
#[derive(Clone, Debug, Default)]
pub struct ExporterOptions {
    /// Maximum verbosity for all channels without a name-specific override.
    pub default_level: Option<Level>,
    /// Maximum verbosity per exact channel name.
    pub levels: BTreeMap<String, Level>,
}
type Export = dyn Fn(&LogRecord) -> Result<(), BoxError>;
struct Exporter {
    options: ExporterOptions,
    callback: Rc<Export>,
}

#[derive(Default)]
pub(crate) struct LogStore {
    sequence: Cell<u64>,
    buffer: RefCell<VecDeque<LogRecord>>,
    exporters: RefCell<BTreeMap<u64, Rc<Exporter>>>,
}
impl LogStore {
    fn remove(&self, id: u64) -> bool {
        let removed = self.exporters.borrow_mut().remove(&id);
        let found = removed.is_some();
        drop(removed);
        found
    }
    pub(crate) fn write(
        &self,
        name: String,
        level: Level,
        message: String,
        fiber: FiberId,
        failure: Option<CleanupFailure>,
        channel_level: Level,
    ) -> Result<LogRecord, LogError> {
        let sequence = self
            .sequence
            .get()
            .checked_add(1)
            .ok_or(LogError::SequenceExhausted)?;
        self.sequence.set(sequence);
        let record = LogRecord {
            sequence,
            timestamp: SystemTime::now(),
            name,
            level,
            message,
            fiber,
            failure,
        };
        if level <= channel_level {
            let evicted = {
                let mut buffer = self.buffer.borrow_mut();
                let evicted = if buffer.len() == 1000 {
                    buffer.pop_front()
                } else {
                    None
                };
                buffer.push_back(record.clone());
                evicted
            };
            drop(evicted);
        }
        let exporters: Vec<_> = self.exporters.borrow().values().cloned().collect();
        let mut failures = Vec::new();
        for exporter in exporters {
            let limit = exporter
                .options
                .levels
                .get(&record.name)
                .copied()
                .or(exporter.options.default_level)
                .unwrap_or(channel_level);
            if level > limit {
                continue;
            }
            if let Err(failure) = effect::invoke(|| (exporter.callback)(&record)) {
                failures.push(failure);
            }
        }
        if failures.is_empty() {
            Ok(record)
        } else {
            Err(LogError::Exporters(failures))
        }
    }
}

/// Log admission, formatting, sequence, or exporter failure.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum LogError {
    /// The logger's runtime no longer exists.
    RuntimeDropped,
    /// No record was emitted because formatting panicked.
    Formatting(CleanupFailure),
    /// Every selected exporter was attempted; these callbacks failed.
    Exporters(Vec<CleanupFailure>),
    /// The sequence space is exhausted; no record was emitted.
    SequenceExhausted,
}
impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeDropped => f.write_str("logger runtime has been dropped"),
            Self::Formatting(_) => f.write_str("log formatting failed"),
            Self::Exporters(errors) => write!(f, "{} log exporters failed", errors.len()),
            Self::SequenceExhausted => f.write_str("log sequence space exhausted"),
        }
    }
}
impl Error for LogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Formatting(e) => Some(e),
            Self::Exporters(errors) => errors.first().map(|e| e as &dyn Error),
            _ => None,
        }
    }
}

/// A non-owning named log channel. Logging is synchronous and does not require
/// a run scope; exporter registration and removal do require host admission.
#[derive(Clone)]
pub struct Logger {
    state: Weak<State>,
    fiber: FiberId,
    name: String,
    level: Level,
}
impl Logger {
    /// Return a channel with an explicit maximum verbosity.
    #[must_use]
    pub fn with_level(&self, level: Level) -> Self {
        Self {
            level,
            ..self.clone()
        }
    }
    /// Format a Rust Display value, buffer it, and attempt all selected exporters.
    pub fn log(&self, level: Level, message: impl fmt::Display) -> Result<LogRecord, LogError> {
        let state = self.state.upgrade().ok_or(LogError::RuntimeDropped)?;
        let message = effect::invoke(|| Ok(message.to_string())).map_err(LogError::Formatting)?;
        state.logger.write(
            self.name.clone(),
            level,
            message,
            self.fiber.clone(),
            None,
            self.level,
        )
    }
    /// Record an error.
    pub fn error(&self, message: impl fmt::Display) -> Result<LogRecord, LogError> {
        self.log(Level::Error, message)
    }
    /// Record a warning.
    pub fn warn(&self, message: impl fmt::Display) -> Result<LogRecord, LogError> {
        self.log(Level::Warn, message)
    }
    /// Record ordinary information.
    pub fn info(&self, message: impl fmt::Display) -> Result<LogRecord, LogError> {
        self.log(Level::Info, message)
    }
    /// Record verbose diagnostics (filtered by default).
    pub fn debug(&self, message: impl fmt::Display) -> Result<LogRecord, LogError> {
        self.log(Level::Debug, message)
    }
}

/// A Fiber-owned exporter with exact synchronous manual removal and inert Drop.
pub struct ExporterRegistration {
    effect: Effect,
    id: u64,
}
impl ExporterRegistration {
    /// Remove only this exporter; other registrations of the same callback remain.
    pub fn remove(self) -> Result<bool, AdmissionError> {
        let state = self
            .effect
            .runtime
            .upgrade()
            .ok_or(AdmissionError::RuntimeDropped)?;
        state.admit()?;
        let owner = self.effect.owner.upgrade();
        let found = state.logger.remove(self.id);
        let cleanup = owner.and_then(|owner| owner.cleanups.borrow_mut().remove(&self.effect.id));
        drop(cleanup);
        Ok(found)
    }
    /// Transfer cleanup to a composed effect using Disposer::from_effect.
    pub fn into_effect(self) -> Effect {
        self.effect
    }
}

impl Context {
    /// Acquire an explicitly named logger. None selects the current Fiber's
    /// diagnostic name; service tracing, name normalization and intercepts are
    /// M5/M6 work.
    pub fn logger(&self, name: Option<&str>) -> Logger {
        Logger {
            state: self.runtime.clone(),
            fiber: self.fiber_id().clone(),
            name: name.unwrap_or(self.name()).to_owned(),
            level: Level::Info,
        }
    }
    /// Snapshot the default buffer (last 1000 records, chronological).
    pub fn log_buffer(&self) -> Result<Vec<LogRecord>, LogError> {
        let state = self.runtime.upgrade().ok_or(LogError::RuntimeDropped)?;
        let records = state.logger.buffer.borrow().iter().cloned().collect();
        Ok(records)
    }
    /// Register a scoped exporter. Callback failures are explicit and never stop
    /// later exporters; no user code runs while exporter storage is borrowed.
    pub fn add_exporter<F, E>(
        &self,
        options: ExporterOptions,
        callback: F,
    ) -> Result<ExporterRegistration, AdmissionError>
    where
        F: Fn(&LogRecord) -> Result<(), E> + 'static,
        E: Into<BoxError> + 'static,
    {
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
        let effect = self
            .on_dispose_sync({
                let state = Rc::downgrade(&state);
                move || {
                    if let Some(state) = state.upgrade() {
                        state.logger.remove(id);
                    }
                    Ok::<_, std::convert::Infallible>(())
                }
            })?
            .with_label("ctx.logger.exporter()");
        let exporter = Exporter {
            options,
            callback: Rc::new(move |record| callback(record).map_err(Into::into)),
        };
        state
            .logger
            .exporters
            .borrow_mut()
            .insert(id, Rc::new(exporter));
        Ok(ExporterRegistration { effect, id })
    }
}
