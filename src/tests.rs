//! Tests for every trait and the singleton macro, run against the tokio implementation in `tokio_impl`.

mod tokio_impl;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::cancellation::GatotkacaCancellationToken;
use crate::runtime::{GatotkacaJoinHandle, GatotkacaRuntime, GatotkacaRuntimeHandle, OwnedRuntime, ShutdownResult};
#[cfg(unix)]
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

/// Sets its flag when dropped, to see that a local future was dropped rather than leaked.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn spawn_local_runs_non_send_future() {
    let rt = runtime();
    let handle = rt.handle();
    let timer = handle.timer();
    let task = handle.spawn_local(move || async move {
        // `Rc` is not `Send`; holding it across an await is what spawn_local allows.
        let shared = std::rc::Rc::new(std::cell::Cell::new(20));
        timer.sleep(Duration::from_millis(10)).await;
        shared.set(shared.get() + 1);
        shared.get() * 2
    });
    assert_eq!(rt.block_on(task).unwrap(), 42);
}

#[test]
fn spawn_local_stays_on_one_thread() {
    let rt = runtime();
    let handle = rt.handle();
    let timer = handle.timer();
    let task = handle.spawn_local(move || {
        let built_on = std::thread::current().id();
        async move {
            let first_poll = std::thread::current().id();
            for _ in 0..3 {
                timer.sleep(Duration::from_millis(5)).await;
                assert_eq!(std::thread::current().id(), first_poll);
            }
            (built_on, first_poll)
        }
    });
    let (built_on, polled_on) = rt.block_on(task).unwrap();
    assert_eq!(built_on, polled_on);
}

#[test]
fn spawn_local_from_inside_a_task() {
    let rt = runtime();
    let handle = rt.handle();
    let inner = handle.clone();
    let task = handle.spawn(async move {
        let local = inner.spawn_local(|| {
            let value = std::rc::Rc::new(7);
            async move { *value }
        });
        local.await
    });
    assert_eq!(rt.block_on(task).unwrap().unwrap(), 7);
}

#[test]
fn spawn_local_reports_panic() {
    let rt = runtime();
    let task = rt.handle().spawn_local(|| async { panic!("boom") });
    assert!(rt.block_on(task).unwrap_err().is_panic());
}

#[test]
fn spawn_local_abort_cancels_and_drops_future() {
    let rt = runtime();
    let handle = rt.handle();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    let task = handle.spawn_local(move || async move {
        let _guard = DropFlag(flag);
        std::future::pending::<()>().await
    });
    rt.block_on(handle.timer().sleep(Duration::from_millis(20)));
    GatotkacaJoinHandle::abort(&task);
    assert!(rt.block_on(task).unwrap_err().is_cancelled());
    // The pinned future is dropped on its own thread, a little after the abort.
    rt.block_on(handle.timer().sleep(Duration::from_millis(100)));
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn spawn_local_after_shutdown_is_cancelled() {
    let rt = runtime();
    let handle = rt.handle();
    rt.shutdown(None);
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let task = handle.spawn_local(move || {
        flag.store(true, Ordering::SeqCst);
        async { 1 }
    });
    let other = tokio::runtime::Runtime::new().unwrap();
    assert!(other.block_on(task).unwrap_err().is_cancelled());
    assert!(!ran.load(Ordering::SeqCst));
}

#[test]
fn shutdown_waits_for_local_tasks() {
    let rt = runtime();
    let handle = rt.handle();
    let flushed = Arc::new(AtomicBool::new(false));
    let seen = flushed.clone();
    let token = handle.shutdown_token();
    let timer = handle.timer();
    handle.spawn_local(move || async move {
        token.cancelled().await;
        // Still flushing after the token fires; shutdown must wait for this.
        timer.sleep(Duration::from_millis(50)).await;
        seen.store(true, Ordering::SeqCst);
    });
    assert_eq!(rt.shutdown(Some(Duration::from_secs(5))), ShutdownResult::Clean);
    assert!(flushed.load(Ordering::SeqCst));
}

#[test]
fn shutdown_aborts_stuck_local_tasks() {
    let rt = runtime();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    rt.handle().spawn_local(move || async move {
        let _guard = DropFlag(flag);
        std::future::pending::<()>().await
    });
    assert_eq!(rt.shutdown(Some(Duration::from_millis(50))), ShutdownResult::TimedOut);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(dropped.load(Ordering::SeqCst), "stuck local task was not aborted");
}

#[test]
fn block_in_place_runs_inside_local_task() {
    let rt = runtime();
    let handle = rt.handle();
    let inner = handle.clone();
    let task = handle.spawn_local(move || async move { inner.block_in_place(|| 5) });
    assert_eq!(rt.block_on(task).unwrap(), 5);
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

// Readiness on unix descriptors: pipes and unix socket pairs, registered from outside any task.
#[cfg(unix)]
mod readiness {
    use std::io::{self, Read, Write};
    use std::os::fd::{AsRawFd, RawFd};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    use super::runtime;
    use super::tokio_impl::{TokioRegistration, TokioRuntime};
    use crate::readiness::{GatotkacaReadiness, GatotkacaRegistration};
    use crate::runtime::{GatotkacaRuntime, GatotkacaRuntimeHandle};

    /// Long enough that a wait which should stay pending would have resolved by then.
    const QUIET: Duration = Duration::from_millis(50);
    /// Generous bound for a wait that should resolve.
    const SOON: Duration = Duration::from_secs(5);

    /// Registers `fd`. Every caller keeps the descriptor open until the registration is dropped.
    fn register(rt: &GatotkacaRuntime<TokioRuntime>, fd: &impl AsRawFd) -> TokioRegistration {
        // SAFETY: each test drops the registration before the descriptor it borrows.
        unsafe { rt.handle().readiness().register(fd.as_raw_fd()) }.expect("register")
    }

    fn set_nonblocking(fd: RawFd) {
        // SAFETY: `fd` is an open descriptor owned by the calling test; F_GETFL / F_SETFL only change its status flags.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            assert!(flags >= 0, "F_GETFL: {}", io::Error::last_os_error());
            assert!(libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) >= 0, "F_SETFL: {}", io::Error::last_os_error());
        }
    }

    /// A pipe with both ends non-blocking.
    fn pipe() -> (io::PipeReader, io::PipeWriter) {
        let (reader, writer) = io::pipe().unwrap();
        set_nonblocking(reader.as_raw_fd());
        set_nonblocking(writer.as_raw_fd());
        (reader, writer)
    }

    fn resolves(rt: &GatotkacaRuntime<TokioRuntime>, wait: impl Future<Output = io::Result<()>>, within: Duration) -> bool {
        match rt.block_on(async { tokio::time::timeout(within, wait).await }) {
            Ok(result) => {
                result.expect("readiness error");
                true
            }
            Err(_elapsed) => false,
        }
    }

    /// Checks that a cleared direction stays pending for `QUIET`. The contract allows spurious wake-ups (a poller can deliver
    /// an event queued before the clear), so a wait that resolves anyway is accepted only if `attempt` (the caller's
    /// non-blocking operation) still returns `WouldBlock`; the test then clears and waits again, as a real caller would.
    fn stays_pending(
        rt: &GatotkacaRuntime<TokioRuntime>,
        registration: &TokioRegistration,
        read: bool,
        mut attempt: impl FnMut() -> io::Result<usize>,
    ) -> bool {
        for _ in 0..5 {
            let woke = if read { resolves(rt, registration.readable(), QUIET) } else { resolves(rt, registration.writable(), QUIET) };
            if !woke {
                return true;
            }
            match attempt() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                other => panic!("woke with the object ready: {other:?}"),
            }
            if read { registration.clear_readable() } else { registration.clear_writable() }
        }
        false
    }

    /// Reads until `WouldBlock`; returns how many bytes came out.
    fn drain(mut reader: impl Read) -> usize {
        let mut buf = [0u8; 4096];
        let mut total = 0;
        loop {
            match reader.read(&mut buf) {
                Ok(0) => return total,
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return total,
                Err(e) => panic!("read: {e}"),
            }
        }
    }

    #[test]
    fn readable_resolves_after_a_write() {
        let rt = runtime();
        let (reader, mut writer) = pipe();
        let registration = register(&rt, &reader);
        assert!(!resolves(&rt, registration.readable(), QUIET), "an empty pipe is not readable");

        writer.write_all(b"x").unwrap();
        assert!(resolves(&rt, registration.readable(), SOON));
        drop(registration);
    }

    #[test]
    fn readable_stays_set_until_cleared() {
        let rt = runtime();
        let (reader, mut writer) = pipe();
        let registration = register(&rt, &reader);
        writer.write_all(b"x").unwrap();
        assert!(resolves(&rt, registration.readable(), SOON));

        // Consuming the data does not clear readiness: waiting resolves at once until `clear_readable`.
        assert_eq!(drain(&reader), 1);
        for _ in 0..3 {
            assert!(resolves(&rt, registration.readable(), QUIET));
        }

        // After `WouldBlock`, clearing makes the next wait pend until more data arrives.
        registration.clear_readable();
        assert!(stays_pending(&rt, &registration, true, || (&reader).read(&mut [0u8; 1])));
        writer.write_all(b"y").unwrap();
        assert!(resolves(&rt, registration.readable(), SOON));
        drop(registration);
    }

    #[test]
    fn clearing_keeps_readiness_while_data_remains() {
        let rt = runtime();
        let (reader, mut writer) = pipe();
        let registration = register(&rt, &reader);
        writer.write_all(b"x").unwrap();
        assert!(resolves(&rt, registration.readable(), SOON));

        // Clearing while the pipe still holds data must not lose the wake-up (the edge has already been consumed).
        registration.clear_readable();
        assert!(resolves(&rt, registration.readable(), QUIET));
        drop(registration);
    }

    #[test]
    fn a_source_ready_at_registration_wakes_the_first_wait() {
        let rt = runtime();
        let (reader, mut writer) = pipe();
        writer.write_all(b"x").unwrap();
        let registration = register(&rt, &reader);
        assert!(resolves(&rt, registration.readable(), SOON));
        drop(registration);
    }

    #[test]
    fn writable_waits_on_a_full_pipe() {
        let rt = runtime();
        let (reader, mut writer) = pipe();
        let registration = register(&rt, &writer);
        assert!(resolves(&rt, registration.writable(), SOON), "an empty pipe is writable");

        // Fill the pipe until the kernel refuses even one byte: a refused large write can leave room the OS still reports
        // as writable (macOS reports a pipe writable with `PIPE_BUF` bytes free).
        let mut written = 0;
        for chunk in [&[0u8; 4096][..], &[0u8; 1][..]] {
            loop {
                match writer.write(chunk) {
                    Ok(n) => written += n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("write: {e}"),
                }
            }
        }
        assert!(written > 0);
        registration.clear_writable();
        assert!(stays_pending(&rt, &registration, false, || (&writer).write(&[0u8; 1])), "a full pipe is not writable");

        assert_eq!(drain(&reader), written);
        assert!(resolves(&rt, registration.writable(), SOON));
        drop(registration);
    }

    #[test]
    fn hang_up_counts_as_readable() {
        let rt = runtime();
        let (ours, peer) = UnixStream::pair().unwrap();
        ours.set_nonblocking(true).unwrap();
        let registration = register(&rt, &ours);
        assert!(!resolves(&rt, registration.readable(), QUIET));

        drop(peer);
        assert!(resolves(&rt, registration.readable(), SOON));
        assert_eq!(drain(&ours), 0, "end of file");
        drop(registration);
    }

    #[test]
    fn drop_deregisters_and_the_source_can_close() {
        let rt = runtime();
        let (ours, mut peer) = UnixStream::pair().unwrap();
        ours.set_nonblocking(true).unwrap();

        // On epoll a second registration of a still-registered descriptor fails with `AlreadyExists`, so registering again
        // after the drop proves the first one was removed.
        drop(register(&rt, &ours));
        let registration = register(&rt, &ours);
        peer.write_all(b"x").unwrap();
        assert!(resolves(&rt, registration.readable(), SOON));
        drop(registration);

        // The descriptor closes normally after its registration is gone, and its number can be reused.
        let number = ours.as_raw_fd();
        drop(ours);
        let (again, mut other) = UnixStream::pair().unwrap();
        again.set_nonblocking(true).unwrap();
        let registration = register(&rt, &again);
        other.write_all(b"y").unwrap();
        assert!(resolves(&rt, registration.readable(), SOON), "new source (old number {number}) is watched");
        drop(registration);
    }

    #[test]
    fn many_registrations_on_one_runtime() {
        const PAIRS: usize = 64;
        let rt = runtime();
        let handle = rt.handle();
        let mut peers = Vec::with_capacity(PAIRS);
        let mut tasks = Vec::with_capacity(PAIRS);
        for _ in 0..PAIRS {
            let (ours, peer) = UnixStream::pair().unwrap();
            ours.set_nonblocking(true).unwrap();
            let registration = register(&rt, &ours);
            peers.push(peer);
            // The task owns both, dropping the registration before the stream.
            tasks.push(handle.spawn(async move {
                let mut got = Vec::new();
                while got.is_empty() {
                    registration.readable().await.unwrap();
                    let mut buf = [0u8; 16];
                    match (&ours).read(&mut buf) {
                        Ok(n) => got.extend_from_slice(&buf[..n]),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => registration.clear_readable(),
                        Err(e) => panic!("read: {e}"),
                    }
                }
                drop(registration);
                drop(ours);
                got
            }));
        }
        for (i, peer) in peers.iter_mut().enumerate() {
            peer.write_all(&[i as u8]).unwrap();
        }
        let results = rt.block_on(async { tokio::time::timeout(SOON, async { futures_join(tasks).await }).await }).expect("all wake");
        for (i, got) in results.into_iter().enumerate() {
            assert_eq!(got, [i as u8]);
        }
    }

    async fn futures_join<T>(tasks: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
        let mut out = Vec::with_capacity(tasks.len());
        for task in tasks {
            out.push(task.await.unwrap());
        }
        out
    }

    #[test]
    fn register_fails_after_shutdown() {
        let rt = runtime();
        let readiness = rt.handle().readiness();
        rt.shutdown(Some(Duration::from_secs(1)));
        let (ours, _peer) = UnixStream::pair().unwrap();
        // SAFETY: registration fails, so nothing outlives `ours`.
        let result = unsafe { readiness.register(ours.as_raw_fd()) };
        assert!(result.is_err());
    }
}
