//! Disposable M0 experiments, not the Cordis public API.

use std::future::Future;
use std::pin::Pin;

/// An owned, local continuation future. No Send guarantee is made by this spike.
pub type LocalFuture<T> = Pin<Box<dyn Future<Output = T>>>;

/// A continuation whose ownership makes a second invocation a compile error.
///
/// ```compile_fail
/// use cordis_m0_spike::Next;
/// let next = Next::new(|| async { 1 });
/// let first = next.call();
/// let second = next.call(); // use of moved value
/// ```
pub struct Next<T>(Box<dyn FnOnce() -> LocalFuture<T>>);

impl<T> Next<T> {
    /// Build a continuation without starting its body.
    pub fn new<F, Fut>(run: F) -> Self
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        Self(Box::new(move || Box::pin(run())))
    }

    /// Consume this continuation, producing the next frame's future.
    pub fn call(self) -> LocalFuture<T> {
        (self.0)()
    }
}

/// The synchronous counterpart of [`Next`].
pub struct SyncNext<T>(Box<dyn FnOnce() -> T>);

impl<T> SyncNext<T> {
    /// Build a synchronous continuation.
    pub fn new(run: impl FnOnce() -> T + 'static) -> Self {
        Self(Box::new(run))
    }

    /// Consume the continuation exactly once.
    pub fn call(self) -> T {
        (self.0)()
    }
}
