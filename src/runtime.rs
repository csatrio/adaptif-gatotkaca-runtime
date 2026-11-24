//! Runtime traits: the stable interface over the executor running underneath gatotkaca.
//!
//! The traits are generic rather than object-safe. The executor is known at compile time, so calls go straight to it with no boxing
//! or dynamic dispatch. For a process-wide singleton, see [`declare_runtime!`](crate::declare_runtime).

use std::future::Future;
use std::time::Duration;

use crate::cancellation::GatotkacaCancellationToken;
use crate::signals::GatotkacaSignals;
use crate::timers::GatotkacaTimer;

/// A runtime implementation that can be owned and created.
pub trait OwnedRuntime: Sized + Send + Sync + 'static {
    /// The handle type this runtime hands out.
    type Handle: GatotkacaRuntimeHandle;

    /// Creates a runtime with the implementation's defaults, ready to run work.
    ///
    /// Implementations that need configuration offer their own constructors; anything built that way installs the same.
    fn create() -> Self;

    /// Drives `future` to completion on the calling thread, blocking until it resolves.
    ///
    /// Call it from outside the runtime, such as `main` or a foreign host thread. Calling it from a runtime worker would block
    /// the worker on itself, so implementations should panic there instead of deadlocking. Inside a task, use
    /// [`GatotkacaRuntimeHandle::block_in_place`] for blocking work.
    fn block_on<F: Future>(&self, future: F) -> F::Output;

    /// A handle to this runtime. Handles are cheap to clone and can move into spawned work.
    fn handle(&self) -> Self::Handle;

    /// Shuts the runtime down gracefully, then forcibly.
    ///
    /// In order: stop accepting new work (later spawns are cancelled), cancel the
    /// [shutdown token](GatotkacaRuntimeHandle::shutdown_token) so tasks can flush, wait up to `timeout` for spawned tasks to
    /// finish (`None` waits forever), then abort whatever is left. Blocking work that has started cannot be aborted and may
    /// outlive the call.
    ///
    /// Call it from outside the runtime, like [`block_on`](Self::block_on). Later calls return
    /// [`ShutdownResult::AlreadyShutDown`].
    fn shutdown(&self, timeout: Option<Duration>) -> ShutdownResult;
}

/// The outcome of [`OwnedRuntime::shutdown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShutdownResult {
    /// Every spawned task finished within the timeout.
    Clean,
    /// The timeout passed first; the remaining tasks were aborted.
    TimedOut,
    /// The runtime was already shut down.
    AlreadyShutDown,
}

/// The Gatotkaca runtime, wrapping a concrete executor.
pub struct GatotkacaRuntime<R: OwnedRuntime> {
    /// The executor that runs the work.
    pub runtime: R,
}

impl<R: OwnedRuntime> GatotkacaRuntime<R> {
    /// Wraps `runtime`.
    pub fn new(runtime: R) -> Self {
        Self { runtime }
    }

    /// Blocks the calling thread until `future` resolves, then returns its output. See [`OwnedRuntime::block_on`].
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// Returns a handle to the underlying runtime.
    pub fn handle(&self) -> R::Handle {
        self.runtime.handle()
    }

    /// Shuts the runtime down. See [`OwnedRuntime::shutdown`].
    pub fn shutdown(&self, timeout: Option<Duration>) -> ShutdownResult {
        self.runtime.shutdown(timeout)
    }
}

/// A handle to the Gatotkaca runtime, handed out by [`OwnedRuntime::handle`].
pub trait GatotkacaRuntimeHandle: Clone + Send + Sync + 'static {
    /// The error a join handle yields when its task panics or is cancelled.
    type JoinError: std::error::Error + Send + Sync + 'static;

    /// Resolves to the output of spawned work.
    type JoinHandle<T: Send + 'static>: GatotkacaJoinHandle + Future<Output = Result<T, Self::JoinError>>;

    /// The timer this runtime hands out.
    type Timer: GatotkacaTimer;

    /// The signal source this runtime hands out.
    type Signals: GatotkacaSignals;

    /// The cancellation token this runtime uses.
    type CancellationToken: GatotkacaCancellationToken;

    /// Spawns `future` on the underlying runtime.
    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Runs `work` on the underlying runtime's blocking pool.
    fn spawn_blocking<F, T>(&self, work: F) -> Self::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;

    /// Runs blocking `work` inside a task without stalling the other tasks on this worker.
    ///
    /// Use it in place of [`OwnedRuntime::block_on`] when already inside the runtime. How the worker is freed is up to the
    /// implementation; one that cannot free it may run `work` in place.
    fn block_in_place<F, T>(&self, work: F) -> T
    where
        F: FnOnce() -> T;

    /// A timer bound to this runtime.
    fn timer(&self) -> Self::Timer;

    /// The signal source for this runtime.
    fn signals(&self) -> Self::Signals;

    /// A token cancelled when [`OwnedRuntime::shutdown`] begins, so tasks can flush and exit. Each call returns a child of the
    /// runtime's token, so cancelling it does not affect the runtime.
    fn shutdown_token(&self) -> Self::CancellationToken;
}

/// Control over a spawned task, from [`GatotkacaRuntimeHandle::spawn`] or [`GatotkacaRuntimeHandle::spawn_blocking`].
///
/// Dropping it detaches the task; it does not cancel it.
pub trait GatotkacaJoinHandle: Send + 'static {
    /// Cancels the task. Its join handle then resolves to a cancellation error. Blocking work that has started cannot be
    /// aborted and runs to completion.
    fn abort(&self);

    /// Whether the task has ended, by finishing, being aborted or panicking.
    fn is_finished(&self) -> bool;
}
