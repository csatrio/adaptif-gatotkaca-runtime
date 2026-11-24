//! Tests for every trait and the singleton macro, run against the tokio implementation in `tokio_impl`.

mod tokio_impl;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::cancellation::GatotkacaCancellationToken;
use crate::runtime::{GatotkacaJoinHandle, GatotkacaRuntime, GatotkacaRuntimeHandle, OwnedRuntime, ShutdownResult};
use crate::signals::{GatotkacaSignalListener, GatotkacaSignals, Signal};
use crate::task_local::GatotkacaTaskLocal;
use crate::timers::{GatotkacaInterval, GatotkacaTimer, GatotkacaTimerTask};
use tokio_impl::TokioRuntime;

fn runtime() -> GatotkacaRuntime<TokioRuntime> {
    GatotkacaRuntime::new(TokioRuntime::create())
}

#[test]
fn block_on_returns_output() {
    assert_eq!(runtime().block_on(async { 21 * 2 }), 42);
}

#[test]
fn block_on_waits_for_pending_future() {
    let rt = runtime();
    let value = rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        "done"
    });
    assert_eq!(value, "done");
}

#[test]
fn handle_spawns_future() {
    let rt = runtime();
    let handle = rt.handle();
    let task = GatotkacaRuntimeHandle::spawn(&handle, async { 42 });
    assert_eq!(rt.block_on(task).unwrap(), 42);
}

#[test]
fn handle_spawns_blocking_work() {
    let rt = runtime();
    let handle = rt.handle();
    let task = GatotkacaRuntimeHandle::spawn_blocking(&handle, || "ran");
    assert_eq!(rt.block_on(task).unwrap(), "ran");
}

#[test]
fn join_handle_reports_panic() {
    let rt = runtime();
    let handle = rt.handle();
    let task = GatotkacaRuntimeHandle::spawn(&handle, async { panic!("boom") });
    assert!(rt.block_on(task).unwrap_err().is_panic());
}

#[test]
fn sleep_waits_for_duration() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let start = Instant::now();
    rt.block_on(timer.sleep(Duration::from_millis(20)));
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[test]
fn sleep_until_waits_for_deadline() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let deadline = Instant::now() + Duration::from_millis(20);
    rt.block_on(timer.sleep_until(deadline));
    assert!(Instant::now() >= deadline);
}

#[test]
fn yield_now_lets_other_task_run() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let ran = rt.block_on(async {
        // A local set pins both tasks to this thread, so the other task can only run if we yield.
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let flag = Arc::new(AtomicUsize::new(0));
                let seen = flag.clone();
                tokio::task::spawn_local(async move { seen.store(1, Ordering::SeqCst) });
                timer.yield_now().await;
                flag.load(Ordering::SeqCst)
            })
            .await
    });
    assert_eq!(ran, 1);
}

#[test]
fn timeout_returns_output_when_in_time() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let result = rt.block_on(timer.timeout(Duration::from_secs(5), async { 3 }));
    assert_eq!(result.unwrap(), 3);
}

#[test]
fn timeout_elapses_when_too_slow() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let slow = timer.sleep(Duration::from_secs(5));
    assert!(rt.block_on(timer.timeout(Duration::from_millis(10), slow)).is_err());
}

#[test]
fn interval_ticks_at_period() {
    let rt = runtime();
    let mut ticks = rt.handle().timer().interval(Duration::from_millis(10));
    assert_eq!(ticks.period(), Duration::from_millis(10));
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..3 {
            GatotkacaInterval::tick(&mut ticks).await;
        }
    });
    // Three ticks, the first immediate.
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[test]
fn run_after_calls_once() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let task = timer.run_after(Duration::from_millis(10), move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    rt.block_on(timer.sleep(Duration::from_millis(50)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(GatotkacaTimerTask::is_finished(&task));
}

#[test]
fn run_after_cancelled_never_calls() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let task = timer.run_after(Duration::from_millis(20), move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    task.cancel();
    rt.block_on(timer.sleep(Duration::from_millis(50)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn run_every_repeats_until_cancelled() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let task = timer.run_every(Duration::from_millis(10), move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    rt.block_on(timer.sleep(Duration::from_millis(55)));
    task.cancel();
    rt.block_on(timer.sleep(Duration::from_millis(20)));
    let after_cancel = calls.load(Ordering::SeqCst);
    assert!(after_cancel >= 2, "expected repeated calls, got {after_cancel}");
    assert!(GatotkacaTimerTask::is_finished(&task));

    rt.block_on(timer.sleep(Duration::from_millis(30)));
    assert_eq!(calls.load(Ordering::SeqCst), after_cancel);
}

#[test]
fn timer_task_cancels_on_drop() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    drop(timer.run_every(Duration::from_millis(10), move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    rt.block_on(timer.sleep(Duration::from_millis(40)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn timer_task_detach_keeps_running() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    timer
        .run_after(Duration::from_millis(10), move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .detach();
    rt.block_on(timer.sleep(Duration::from_millis(40)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn interval_skips_missed_ticks() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let mut ticks = timer.interval(Duration::from_millis(50));
    let fast_ticks = rt.block_on(async {
        GatotkacaInterval::tick(&mut ticks).await;
        // Stall for several periods, then count ticks that are already due. A zero timeout still polls the tick once.
        std::thread::sleep(Duration::from_millis(260));
        let mut ready = 0;
        while timer.timeout(Duration::ZERO, GatotkacaInterval::tick(&mut ticks)).await.is_ok() {
            ready += 1;
        }
        ready
    });
    // Burst would fire ~5 catch-up ticks at once. Skip fires one; the period is long enough that a slow, loaded machine sees
    // at most one more on schedule.
    assert!(fast_ticks <= 2, "expected missed ticks to be skipped, got {fast_ticks} ready");
}

#[test]
fn join_handle_abort_cancels_task() {
    let rt = runtime();
    let task = rt.handle().spawn(std::future::pending::<()>());
    GatotkacaJoinHandle::abort(&task);
    assert!(rt.block_on(task).unwrap_err().is_cancelled());
}

#[test]
fn join_handle_reports_finished() {
    let rt = runtime();
    let handle = rt.handle();
    let task = handle.spawn(async { 1 });
    rt.block_on(handle.timer().sleep(Duration::from_millis(20)));
    assert!(GatotkacaJoinHandle::is_finished(&task));
}

#[test]
fn block_in_place_runs_inside_task() {
    let rt = runtime();
    let handle = rt.handle();
    let inner = handle.clone();
    let task = handle.spawn(async move { inner.block_in_place(|| 5) });
    assert_eq!(rt.block_on(task).unwrap(), 5);
}

#[test]
fn shutdown_is_clean_when_tasks_flush() {
    let rt = runtime();
    let handle = rt.handle();
    let flushed = Arc::new(AtomicBool::new(false));
    let seen = flushed.clone();
    let token = handle.shutdown_token();
    handle.spawn(async move {
        token.cancelled().await;
        seen.store(true, Ordering::SeqCst);
    });
    assert_eq!(rt.shutdown(Some(Duration::from_secs(5))), ShutdownResult::Clean);
    assert!(flushed.load(Ordering::SeqCst));
    assert_eq!(rt.shutdown(None), ShutdownResult::AlreadyShutDown);
}

#[test]
fn shutdown_times_out_and_aborts_stuck_tasks() {
    let rt = runtime();
    rt.handle().spawn(std::future::pending::<()>());
    assert_eq!(rt.shutdown(Some(Duration::from_millis(20))), ShutdownResult::TimedOut);
}

#[test]
fn spawn_after_shutdown_is_cancelled() {
    let rt = runtime();
    let handle = rt.handle();
    rt.shutdown(None);
    let task = handle.spawn(async { 1 });
    // The Gatotkaca runtime is gone, so a throwaway one polls the join handle.
    let other = tokio::runtime::Runtime::new().unwrap();
    assert!(other.block_on(task).unwrap_err().is_cancelled());
}

#[test]
fn shutdown_token_child_does_not_cancel_runtime() {
    let rt = runtime();
    let handle = rt.handle();
    handle.shutdown_token().cancel();
    assert!(!handle.shutdown_token().is_cancelled());
}

fn exercise_token<C: GatotkacaCancellationToken>(rt: &GatotkacaRuntime<TokioRuntime>) {
    let parent = C::new();
    let child = parent.child_token();
    child.cancel();
    assert!(child.is_cancelled());
    assert!(!parent.is_cancelled());

    let other_child = parent.child_token();
    let waiting = other_child.cancelled();
    parent.cancel();
    assert!(other_child.is_cancelled());
    rt.block_on(waiting);
}

#[test]
fn cancellation_token_propagates_to_children() {
    exercise_token::<tokio_util::sync::CancellationToken>(&runtime());
}

#[cfg(unix)]
#[test]
fn signal_listener_receives_hangup() {
    let rt = runtime();
    let handle = rt.handle();
    let mut listener = handle.signals().listen(Signal::Hangup).unwrap();
    let status = std::process::Command::new("kill").args(["-HUP", &std::process::id().to_string()]).status().unwrap();
    assert!(status.success());
    let got = rt.block_on(handle.timer().timeout(Duration::from_secs(5), listener.recv()));
    assert_eq!(got.unwrap(), Some(()));
}

tokio::task_local! {
    static REQUEST_ID: u32;
}

fn read_local<K: GatotkacaTaskLocal<Value = u32>>(key: &'static K) -> Option<u32> {
    key.try_with(|id| *id)
}

fn scoped<K: GatotkacaTaskLocal<Value = u32>>(key: &'static K, value: u32) -> K::Scope<impl Future<Output = Option<u32>>> {
    key.scope(value, async move { read_local(key) })
}

#[test]
fn task_local_visible_only_in_scope() {
    let rt = runtime();
    assert_eq!(rt.block_on(scoped(&REQUEST_ID, 7)), Some(7));
    assert_eq!(read_local(&REQUEST_ID), None);
    assert_eq!(GatotkacaTaskLocal::sync_scope(&REQUEST_ID, 9, || GatotkacaTaskLocal::with(&REQUEST_ID, |id| *id)), 9);
}

#[test]
fn run_every_stops_after_callback_panics() {
    let rt = runtime();
    let timer = rt.handle().timer();
    let task = timer.run_every(Duration::from_millis(5), || panic!("callback failed"));
    rt.block_on(timer.sleep(Duration::from_millis(40)));
    assert!(GatotkacaTimerTask::is_finished(&task));
    // The runtime still runs work.
    assert_eq!(rt.block_on(rt.handle().spawn(async { 2 })).unwrap(), 2);
}

// Each singleton test gets its own module, and so its own static, because tests run in parallel.
mod installed {
    use super::tokio_impl::TokioRuntime;
    use crate::runtime::{GatotkacaRuntimeHandle, OwnedRuntime};

    crate::declare_runtime!(TokioRuntime);

    #[test]
    fn install_then_get_returns_same_runtime() {
        let first = install(TokioRuntime::create());
        let second = install(TokioRuntime::create());
        assert!(std::ptr::eq(first, second));
        assert!(std::ptr::eq(first, get()));
        assert!(try_get().is_some());

        let task = GatotkacaRuntimeHandle::spawn(&get().handle(), async { 7 });
        assert_eq!(get().block_on(task).unwrap(), 7);
    }
}

mod missing {
    use super::tokio_impl::TokioRuntime;

    crate::declare_runtime!(TokioRuntime);

    #[test]
    fn try_get_is_none_before_install() {
        assert!(try_get().is_none());
    }

    #[test]
    #[should_panic(expected = "not installed")]
    fn get_panics_before_install() {
        get();
    }
}
