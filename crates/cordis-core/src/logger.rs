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
    /// Non-owning access to current lifecycle metadata; can expire independently.
    pub fiber_ref: WeakFiber,
    /// Original source for an automatic diagnostic or explicit error record.
    pub failure: Option<CleanupFailure>,
}

/// Weak lifecycle metadata, equivalent to the upstream log record's WeakRef.
#[derive(Clone)]
pub struct WeakFiber {
    pub(crate) id: FiberId,
    pub(crate) owner: Weak<crate::owner::Owner>,
}
impl WeakFiber {
    /// Read the current value-only lifecycle snapshot, or None after release.
    pub fn snapshot(&self) -> Option<crate::hooks::FiberInfo> {
        let owner = self.owner.upgrade()?;
        if owner.is_root() {
            Some(crate::hooks::FiberInfo {
                id: owner.id.clone(),
                name: owner.name.clone(),
                state: crate::FiberState::Active,
                live: owner.live.get(),
            })
        } else {
            let fiber = owner.fiber.borrow().upgrade()?;
            Some(fiber.info())
        }
    }
}
impl fmt::Debug for WeakFiber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WeakFiber").field(&self.id).finish()
    }
}

/// Use-site logger overrides. Missing fields inherit the outer layer.
#[derive(Clone, Debug, Default)]
pub struct LoggerConfig {
    /// Override the default channel name (an explicit acquisition name still wins).
    pub name: Option<String>,
    /// Override default verbosity.
    pub level: Option<Level>,
}

/// Explicit Rust representation of an error with cause/aggregate metadata.
/// Owned trees preserve exact error identities without JS property reflection.
#[derive(Clone, Debug)]
pub struct ErrorValue {
    source: Rc<dyn Error>,
    cause: Option<Box<Self>>,
    members: Vec<Self>,
}
impl ErrorValue {
    /// Wrap an owned error.
    pub fn new<E: Error + 'static>(error: E) -> Self {
        Self::from_rc(Rc::new(error))
    }
    /// Preserve an existing error's exact identity.
    pub fn from_rc(source: Rc<dyn Error>) -> Self {
        Self {
            source,
            cause: None,
            members: Vec::new(),
        }
    }
    /// Log this cause before the outer error; cause takes precedence over members.
    #[must_use]
    pub fn with_cause(mut self, cause: Self) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }
    /// Nonempty members flatten recursively instead of logging this aggregate.
    /// An empty aggregate still logs its own error.
    #[must_use]
    pub fn with_members(mut self, members: Vec<Self>) -> Self {
        self.members = members;
        self
    }
}

/// Supported ANSI palette depth; styling remains an exporter choice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColorDepth {
    /// Plain text.
    #[default]
    None,
    /// Cordis's six standard foreground colors.
    Ansi16,
    /// Cordis's 256-color palette.
    Ansi256,
}

impl LogRecord {
    /// Format the already Rust-formatted message, limiting each line in UTF-16
    /// units as upstream does, but never splitting a Unicode scalar/surrogate.
    /// CRLF normalizes to LF; zero is a real zero-character limit.
    pub fn format(&self, max_length: usize) -> String {
        let mut lines = self.message.split('\n').peekable();
        let mut output = Vec::new();
        while let Some(line) = lines.next() {
            let line = if lines.peek().is_some() {
                line.strip_suffix('\r').unwrap_or(line)
            } else {
                line
            };
            let mut budget = 0;
            let mut rendered = String::new();
            for character in line.chars() {
                budget += character.len_utf16();
                if budget > max_length {
                    rendered.push_str("...");
                    break;
                }
                rendered.push(character);
            }
            output.push(rendered);
        }
        output.join("\n")
    }
    /// Color this channel's text deterministically, without adding an implicit prefix.
    pub fn color(&self, value: &str, depth: ColorDepth) -> String {
        let colors: &[u8] = match depth {
            ColorDepth::None => return value.into(),
            ColorDepth::Ansi16 => &[6, 2, 3, 4, 5, 1],
            ColorDepth::Ansi256 => &[
                20, 21, 26, 27, 32, 33, 38, 39, 40, 41, 42, 43, 44, 45, 56, 57, 62, 63, 68, 69, 74,
                75, 76, 77, 78, 79, 80, 81, 92, 93, 98, 99, 112, 113, 129, 134, 135, 148, 149, 160,
                161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 178, 179, 184,
                185, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 214,
                215, 220, 221,
            ],
        };
        let hash = self.name.encode_utf16().fold(0_i32, |hash, code| {
            hash.wrapping_mul(7).wrapping_add(i32::from(code) + 13)
        });
        let code = colors[hash.unsigned_abs() as usize % colors.len()];
        if code < 8 {
            format!("\u{1b}[3{code}m{value}\u{1b}[0m")
        } else {
            format!("\u{1b}[38;5;{code}m{value}\u{1b}[0m")
        }
    }
}

pub(crate) fn normalize_name(name: &str) -> String {
    let mut output = String::new();
    let mut letters = name.chars().peekable();
    let mut word = false;
    let mut upper = false;
    while let Some(character) = letters.next() {
        if character.is_ascii_uppercase() {
            if word && (!upper || letters.peek().is_some_and(char::is_ascii_lowercase)) {
                output.push('-');
            }
            output.push(character.to_ascii_lowercase());
            word = true;
            upper = true;
        } else if character.is_ascii_lowercase() {
            output.push(character);
            word = true;
            upper = false;
        } else if character == '-' || character == '_' {
            if word {
                output.push('-');
            }
            word = false;
            upper = false;
        } else {
            output.push(character);
        }
    }
    output
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
        fiber: WeakFiber,
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
            fiber: fiber.id.clone(),
            fiber_ref: fiber,
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
    /// Logger intercept configuration has an incompatible Rust contract.
    Configuration(crate::ServiceError),
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
            Self::Configuration(error) => error.fmt(f),
            Self::Exporters(errors) => write!(f, "{} log exporters failed", errors.len()),
            Self::SequenceExhausted => f.write_str("log sequence space exhausted"),
        }
    }
}
impl Error for LogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Formatting(e) => Some(e),
            Self::Configuration(e) => Some(e),
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
    fiber: WeakFiber,
    settings: Result<Settings, crate::ServiceError>,
}
#[derive(Clone)]
struct Settings {
    name: String,
    level: Level,
}
impl Logger {
    /// Return a channel with an explicit maximum verbosity.
    #[must_use]
    pub fn with_level(&self, level: Level) -> Self {
        Self {
            settings: self.settings.clone().map(|mut settings| {
                settings.level = level;
                settings
            }),
            ..self.clone()
        }
    }
    /// Format a Rust Display value, buffer it, and attempt all selected exporters.
    pub fn log(&self, level: Level, message: impl fmt::Display) -> Result<LogRecord, LogError> {
        let state = self.state.upgrade().ok_or(LogError::RuntimeDropped)?;
        let settings = self
            .settings
            .as_ref()
            .map_err(|error| LogError::Configuration(error.clone()))?;
        let message = effect::invoke(|| Ok(message.to_string())).map_err(LogError::Formatting)?;
        state.logger.write(
            settings.name.clone(),
            level,
            message,
            self.fiber.clone(),
            None,
            settings.level,
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

    /// Log explicit cause/aggregate errors with exact source identities. Causes
    /// precede their outer error; nonempty aggregates flatten, empty ones remain.
    pub fn log_error(&self, level: Level, value: ErrorValue) -> Result<Vec<LogRecord>, LogError> {
        let mut records = Vec::new();
        self.error_records(level, value, &mut records)?;
        Ok(records)
    }

    fn error_records(
        &self,
        level: Level,
        value: ErrorValue,
        records: &mut Vec<LogRecord>,
    ) -> Result<(), LogError> {
        if let Some(cause) = value.cause {
            self.error_records(level, *cause, records)?;
        } else if !value.members.is_empty() {
            for member in value.members {
                self.error_records(level, member, records)?;
            }
            return Ok(());
        }
        let state = self.state.upgrade().ok_or(LogError::RuntimeDropped)?;
        let settings = self
            .settings
            .as_ref()
            .map_err(|error| LogError::Configuration(error.clone()))?;
        let message =
            effect::invoke(|| Ok(value.source.to_string())).map_err(LogError::Formatting)?;
        records.push(state.logger.write(
            settings.name.clone(),
            level,
            message,
            self.fiber.clone(),
            Some(CleanupFailure::from_source(value.source)),
            settings.level,
        )?);
        Ok(())
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
    /// Cache a channel name: explicit argument, use-site logger intercept, then
    /// normalized definition Fiber name. A malformed intercept is surfaced by
    /// the first log operation, not silently ignored.
    pub fn logger(&self, name: Option<&str>) -> Logger {
        let definition = self.definition_site();
        let settings = self
            .intercept_values::<LoggerConfig>("logger")
            .map(|configs| {
                let mut config = LoggerConfig::default();
                for layer in configs {
                    if layer.name.is_some() {
                        config.name = layer.name.clone();
                    }
                    if layer.level.is_some() {
                        config.level = layer.level;
                    }
                }
                Settings {
                    name: name
                        .map(str::to_owned)
                        .or(config.name)
                        .unwrap_or_else(|| normalize_name(definition.name())),
                    level: config.level.unwrap_or(Level::Info),
                }
            });
        Logger {
            state: self.runtime.clone(),
            fiber: WeakFiber {
                id: definition.fiber_id().clone(),
                owner: definition.owner.clone(),
            },
            settings,
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
