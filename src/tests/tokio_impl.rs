//! A tokio implementation of every trait, to prove the interface maps onto a real executor.

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::runtime::{Handle, Runtime};
use tokio::task::futures::TaskLocalFuture;
use tokio::task::{JoinError, JoinHandle, LocalKey};
use tokio::time::{Interval, MissedTickBehavior, Sleep, Timeout, error::Elapsed};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;

use crate::cancellation::GatotkacaCancellationToken;
use crate::runtime::{GatotkacaJoinHandle, GatotkacaRuntimeHandle, OwnedRuntime, ShutdownResult};
use crate::signals::{GatotkacaSignalListener, GatotkacaSignals, Signal};
use crate::task_local::GatotkacaTaskLocal;
use crate::timers::{GatotkacaInterval, GatotkacaTimer, GatotkacaTimerTask};

/// State every handle shares, so shutdown can reach all spawned work.
struct Shared {
    closed: AtomicBool,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

/// tokio shuts down by consuming its runtime, so the runtime sits in a lock that `shutdown` can take it out of.
pub struct TokioRuntime {
    runtime: RwLock<Option<Runtime>>,
    handle: TokioHandle,
}

impl OwnedRuntime for TokioRuntime {
    type Handle = TokioHandle;

    fn create() -> Self {
        let runtime = Runtime::new().expect("failed to build tokio runtime");
        let shared = Shared { closed: AtomicBool::new(false), shutdown: CancellationToken::new(), tracker: TaskTracker::new() };
        let handle = TokioHandle { inner: runtime.handle().clone(), shared: Arc::new(shared) };
        Self { runtime: RwLock::new(Some(runtime)), handle }
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        let runtime = self.runtime.read().unwrap();
        // tokio itself panics if this is a worker thread.
        runtime.as_ref().expect("Gatotkaca runtime is shut down").block_on(future)
    }

    fn handle(&self) -> TokioHandle {
        self.handle.clone()
    }

    fn shutdown(&self, timeout: Option<Duration>) -> ShutdownResult {
        let Some(runtime) = self.runtime.write().unwrap().take() else {
            return ShutdownResult::AlreadyShutDown;
        };
        let shared = &self.handle.shared;
        shared.closed.store(true, Ordering::SeqCst);
        shared.shutdown.cancel();
        shared.tracker.close();

        let tracker = shared.tracker.clone();
        let finished = runtime.block_on(async move {
            match timeout {
                Some(limit) => tokio::time::timeout(limit, tracker.wait()).await.is_ok(),
                None => {
                    tracker.wait().await;
                    true
                }
            }
        });
        // Aborts every task still running.
        runtime.shutdown_background();
        if finished { ShutdownResult::Clean } else { ShutdownResult::TimedOut }
    }
}

/// A tokio handle plus the shared shutdown state.
#[derive(Clone)]
pub struct TokioHandle {
    inner: Handle,
    shared: Arc<Shared>,
}

impl GatotkacaRuntimeHandle for TokioHandle {
    type JoinError = JoinError;
    type JoinHandle<T: Send + 'static> = JoinHandle<T>;
    type Timer = TokioHandle;
    type Signals = TokioSignals;
    type CancellationToken = CancellationToken;

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if self.shared.closed.load(Ordering::SeqCst) {
            return cancelled_task(&self.inner);
        }
        self.shared.tracker.spawn_on(future, &self.inner)
    }

    fn spawn_blocking<F, T>(&self, work: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if self.shared.closed.load(Ordering::SeqCst) {
            return cancelled_task(&self.inner);
        }
        self.shared.tracker.spawn_blocking_on(work, &self.inner)
    }

    fn block_in_place<F, T>(&self, work: F) -> T
    where
        F: FnOnce() -> T,
    {
        tokio::task::block_in_place(work)
    }

    fn timer(&self) -> TokioHandle {
        self.clone()
    }

    fn signals(&self) -> TokioSignals {
        TokioSignals(self.inner.clone())
    }

    fn shutdown_token(&self) -> CancellationToken {
        self.shared.shutdown.child_token()
    }
}

/// A join handle for work refused after shutdown. The work itself is dropped unrun.
fn cancelled_task<T: Send + 'static>(handle: &Handle) -> JoinHandle<T> {
    let task = handle.spawn(std::future::pending::<T>());
    task.abort();
    task
}

impl<T: Send + 'static> GatotkacaJoinHandle for JoinHandle<T> {
    fn abort(&self) {
        JoinHandle::abort(self);
    }

    fn is_finished(&self) -> bool {
        JoinHandle::is_finished(self)
    }
}

// tokio timers find the runtime through thread-local context, so each constructor enters the handle first.
// That lets the timer work from any thread, not only from inside a task.
impl GatotkacaTimer for TokioHandle {
    type Sleep = Sleep;
    type Timeout<F: Future> = Timeout<F>;
    type Elapsed = Elapsed;
    type Interval = Interval;
    type TimerTask = TokioTimerTask;

    fn sleep(&self, duration: Duration) -> Sleep {
        let _ctx = self.inner.enter();
        tokio::time::sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep {
        let _ctx = self.inner.enter();
        tokio::time::sleep_until(deadline.into())
    }

    fn yield_now(&self) -> impl Future<Output = ()> + Send + 'static {
        tokio::task::yield_now()
    }

    fn timeout<F: Future>(&self, duration: Duration, future: F) -> Timeout<F> {
        let _ctx = self.inner.enter();
        tokio::time::timeout(duration, future)
    }

    fn interval(&self, period: Duration) -> Interval {
        let _ctx = self.inner.enter();
        let mut ticks = tokio::time::interval(period);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticks
    }

    fn run_after<F>(&self, delay: Duration, callback: F) -> TokioTimerTask
    where
        F: FnOnce() + Send + 'static,
    {
        let sleep = GatotkacaTimer::sleep(self, delay);
        TokioTimerTask(Some(GatotkacaRuntimeHandle::spawn(self, async move {
            sleep.await;
            callback();
        })))
    }

    fn run_every<F>(&self, period: Duration, mut callback: F) -> TokioTimerTask
    where
        F: FnMut() + Send + 'static,
    {
        let mut ticks = GatotkacaTimer::interval(self, period);
        TokioTimerTask(Some(GatotkacaRuntimeHandle::spawn(self, async move {
            // The first tick is immediate; skip it so the first call is one period away.
            ticks.tick().await;
            loop {
                ticks.tick().await;
                callback();
            }
        })))
    }
}

impl GatotkacaInterval for Interval {
    async fn tick(&mut self) -> Instant {
        Interval::tick(self).await.into_std()
    }

    fn reset(&mut self) {
        Interval::reset(self);
    }

    fn period(&self) -> Duration {
        Interval::period(self)
    }
}

/// Aborts its task on drop. `None` only after `detach`.
pub struct TokioTimerTask(Option<JoinHandle<()>>);

impl GatotkacaTimerTask for TokioTimerTask {
    fn cancel(&self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }

    fn is_finished(&self) -> bool {
        self.0.as_ref().is_none_or(JoinHandle::is_finished)
    }

    fn detach(mut self) {
        // Dropping a tokio JoinHandle detaches its task.
        self.0.take();
    }
}

impl Drop for TokioTimerTask {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl GatotkacaCancellationToken for CancellationToken {
    type Cancelled = WaitForCancellationFutureOwned;

    fn new() -> Self {
        CancellationToken::new()
    }

    fn cancel(&self) {
        CancellationToken::cancel(self);
    }

    fn is_cancelled(&self) -> bool {
        CancellationToken::is_cancelled(self)
    }

    fn cancelled(&self) -> WaitForCancellationFutureOwned {
        self.clone().cancelled_owned()
    }

    fn child_token(&self) -> Self {
        CancellationToken::child_token(self)
    }
}

/// Signals through tokio. Entering the handle lets `listen` work from any thread.
#[derive(Clone)]
pub struct TokioSignals(Handle);

impl GatotkacaSignals for TokioSignals {
    type Listener = TokioSignalListener;

    fn listen(&self, signal: Signal) -> io::Result<TokioSignalListener> {
        let _ctx = self.0.enter();
        #[cfg(unix)]
        {
            use tokio::signal::unix::SignalKind;
            let kind = match signal {
                Signal::Interrupt => SignalKind::interrupt(),
                Signal::Terminate => SignalKind::terminate(),
                Signal::Hangup => SignalKind::hangup(),
            };
            tokio::signal::unix::signal(kind).map(TokioSignalListener)
        }
        // The tests run on Unix; elsewhere only `ctrl_c` is wired up.
        #[cfg(not(unix))]
        {
            let _ = signal;
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    fn ctrl_c(&self) -> impl Future<Output = io::Result<()>> + Send + 'static {
        tokio::signal::ctrl_c()
    }
}

/// One signal's deliveries.
pub struct TokioSignalListener(#[cfg(unix)] tokio::signal::unix::Signal, #[cfg(not(unix))] std::convert::Infallible);

impl GatotkacaSignalListener for TokioSignalListener {
    async fn recv(&mut self) -> Option<()> {
        #[cfg(unix)]
        {
            self.0.recv().await
        }
        #[cfg(not(unix))]
        {
            match self.0 {}
        }
    }
}

impl<T: 'static> GatotkacaTaskLocal for LocalKey<T> {
    type Value = T;
    type Scope<F: Future> = TaskLocalFuture<T, F>;

    fn scope<F: Future>(&'static self, value: T, future: F) -> TaskLocalFuture<T, F> {
        LocalKey::scope(self, value, future)
    }

    fn sync_scope<F, R>(&'static self, value: T, work: F) -> R
    where
        F: FnOnce() -> R,
    {
        LocalKey::sync_scope(self, value, work)
    }

    fn with<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        LocalKey::with(self, f)
    }

    fn try_with<F, R>(&'static self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        LocalKey::try_with(self, f).ok()
    }
}
