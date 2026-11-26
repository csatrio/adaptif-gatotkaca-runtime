//! Tests for every trait and the singleton macro, run against the tokio implementation in `tokio_impl`.

mod fs_impl;
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

/// The `fs` traits, run against `std::fs` both on the tokio blocking pool and inline.
mod filesystem {
    use std::fs::{FileTimes, TryLockError};
    use std::future::Future;
    use std::io::{ErrorKind, SeekFrom};
    use std::path::PathBuf;
    use std::time::SystemTime;

    use super::fs_impl::{Inline, InlineFs, TokioFs};
    use super::*;
    use crate::fs::{GatotkacaDirEntry, GatotkacaFile, GatotkacaFs, GatotkacaReadDir, OpenOptions};

    /// A fresh directory under the system temp dir, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let unique = NEXT.fetch_add(1, Ordering::SeqCst);
            let dir = std::env::temp_dir().join(format!("gatotkaca-fs-{}-{name}-{unique}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self, relative: &str) -> PathBuf {
            self.0.join(relative)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Runs each check against both reference filesystems, as `tokio_pool::<check>` and `inline::<check>`.
    macro_rules! on_every_fs {
        ($($check:ident),* $(,)?) => {
            mod tokio_pool {
                $(
                    #[test]
                    fn $check() {
                        let rt = super::runtime();
                        let fs = super::TokioFs::new(rt.handle());
                        rt.block_on(super::$check(fs, super::Scratch::new(stringify!($check))));
                    }
                )*
            }
            mod inline {
                $(
                    #[test]
                    fn $check() {
                        let rt = super::runtime();
                        let fs = super::InlineFs::new(super::Inline);
                        rt.block_on(super::$check(fs, super::Scratch::new(stringify!($check))));
                    }
                )*
            }
        };
    }

    on_every_fs!(
        whole_files,
        copy_rename_remove_and_links,
        directories,
        positional_io,
        cursor_io,
        open_options,
        permissions_and_times,
        advisory_locks,
    );

    async fn whole_files<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let path = dir.path("a.txt");
        assert!(!fs.exists(&path).await.unwrap());
        fs.write(&path, "hello").await.unwrap();
        assert!(fs.exists(&path).await.unwrap());
        assert_eq!(fs.read(&path).await.unwrap(), b"hello");
        assert_eq!(fs.read_to_string(&path).await.unwrap(), "hello");
        assert_eq!(fs.metadata(&path).await.unwrap().len(), 5);

        fs.write(&path, vec![0xff, 0xfe]).await.unwrap();
        assert_eq!(fs.read_to_string(&path).await.unwrap_err().kind(), ErrorKind::InvalidData);
        assert_eq!(fs.read(dir.path("missing")).await.unwrap_err().kind(), ErrorKind::NotFound);
    }

    async fn copy_rename_remove_and_links<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let original = dir.path("a");
        fs.write(&original, "abc").await.unwrap();
        assert_eq!(fs.copy(&original, dir.path("b")).await.unwrap(), 3);
        fs.rename(dir.path("b"), dir.path("c")).await.unwrap();
        assert!(!fs.exists(dir.path("b")).await.unwrap());
        assert_eq!(fs.read(dir.path("c")).await.unwrap(), b"abc");

        fs.hard_link(&original, dir.path("h")).await.unwrap();
        fs.remove_file(&original).await.unwrap();
        assert!(!fs.exists(&original).await.unwrap());
        assert_eq!(fs.read(dir.path("h")).await.unwrap(), b"abc");
        assert_eq!(fs.remove_file(&original).await.unwrap_err().kind(), ErrorKind::NotFound);

        assert_eq!(fs.canonicalize(dir.path("c")).await.unwrap(), std::fs::canonicalize(dir.path("c")).unwrap());

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path("c"), dir.path("s")).unwrap();
            assert_eq!(fs.read_link(dir.path("s")).await.unwrap(), dir.path("c"));
            assert!(fs.symlink_metadata(dir.path("s")).await.unwrap().is_symlink());
            assert!(fs.metadata(dir.path("s")).await.unwrap().is_file());
            fs.remove_file(dir.path("c")).await.unwrap();
            // A broken link does not exist.
            assert!(!fs.exists(dir.path("s")).await.unwrap());
        }
    }

    async fn directories<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let nested = dir.path("x/y/z");
        assert_eq!(fs.create_dir(&nested).await.unwrap_err().kind(), ErrorKind::NotFound);
        fs.create_dir_all(&nested).await.unwrap();
        fs.write(nested.join("f"), "1").await.unwrap();
        fs.create_dir(dir.path("x/empty")).await.unwrap();
        fs.write(dir.path("x/file"), "2").await.unwrap();

        let mut listing = fs.read_dir(dir.path("x")).await.unwrap();
        let mut seen = Vec::new();
        while let Some(entry) = listing.next_entry().await.unwrap() {
            assert_eq!(entry.path(), dir.path("x").join(entry.file_name()));
            let is_dir = entry.file_type().await.unwrap().is_dir();
            assert_eq!(entry.metadata().await.unwrap().is_dir(), is_dir);
            seen.push((entry.file_name().into_string().unwrap(), is_dir));
        }
        seen.sort();
        assert_eq!(seen, [("empty".into(), true), ("file".into(), false), ("y".into(), true)]);
        // Exhausted stays exhausted.
        assert!(listing.next_entry().await.unwrap().is_none());

        assert_eq!(fs.remove_dir(dir.path("x/y")).await.unwrap_err().kind(), ErrorKind::DirectoryNotEmpty);
        fs.remove_dir(dir.path("x/empty")).await.unwrap();
        fs.remove_dir_all(dir.path("x")).await.unwrap();
        assert!(!fs.exists(dir.path("x")).await.unwrap());
    }

    async fn positional_io<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let file = fs.create_new(dir.path("p")).await.unwrap();
        let (result, buf) = file.write_all_at(b"0123456789".to_vec(), 0).await;
        result.unwrap();
        assert_eq!(buf, b"0123456789", "the buffer comes back");
        let (written, _) = file.write_at(b"ab".to_vec(), 4).await;
        assert_eq!(written.unwrap(), 2);
        let contents = b"0123ab6789";

        // Reads go after the existing contents, into spare capacity only.
        let mut buf = Vec::with_capacity(5);
        buf.push(b'>');
        let (read, buf) = file.read_at(buf, 2).await;
        let read = read.unwrap();
        assert!(read > 0);
        assert_eq!(buf[0], b'>');
        assert_eq!(&buf[1..], &contents[2..2 + read]);

        let buf = Vec::with_capacity(4);
        let want = buf.capacity();
        let (result, buf) = file.read_exact_at(buf, 3).await;
        result.unwrap();
        assert_eq!(buf, &contents[3..3 + want]);

        // No spare capacity reads nothing.
        let (read, buf) = file.read_at(Vec::new(), 0).await;
        assert_eq!(read.unwrap(), 0);
        assert!(buf.is_empty());

        let (read, _) = file.read_at(Vec::with_capacity(4), 10).await;
        assert_eq!(read.unwrap(), 0, "end of file");
        let (result, buf) = file.read_exact_at(Vec::with_capacity(4), 8).await;
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnexpectedEof);
        assert!(buf.capacity() >= 4, "the buffer comes back on error too");

        // Positional calls take `&self`, so several can be in flight on one file.
        let (first, second) = tokio::join!(file.read_exact_at(Vec::with_capacity(2), 0), file.read_exact_at(Vec::with_capacity(2), 4));
        first.0.unwrap();
        second.0.unwrap();
        assert_eq!((first.1, second.1), (b"01".to_vec(), b"ab".to_vec()));
    }

    async fn cursor_io<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let path = dir.path("c");
        let mut file = fs.open_with(&path, OpenOptions::new().read(true).write(true).create(true)).await.unwrap();
        let (result, _) = file.write_all(b"hello world".to_vec()).await;
        result.unwrap();
        assert_eq!(file.stream_position().await.unwrap(), 11);
        file.flush().await.unwrap();

        assert_eq!(file.seek(SeekFrom::Start(6)).await.unwrap(), 6);
        let (result, buf) = file.read_exact(Vec::with_capacity(5)).await;
        result.unwrap();
        assert_eq!(buf, b"world");

        file.rewind().await.unwrap();
        let (read, buf) = file.read_to_end(b"<".to_vec()).await;
        assert_eq!(read.unwrap(), 11);
        assert_eq!(buf, b"<hello world");
        let (read, _) = file.read(Vec::with_capacity(4)).await;
        assert_eq!(read.unwrap(), 0, "end of file");
        let (result, _) = file.read_exact(Vec::with_capacity(4)).await;
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnexpectedEof);

        file.seek(SeekFrom::Start(0)).await.unwrap();
        let (written, _) = file.write(b"J".to_vec()).await;
        assert_eq!(written.unwrap(), 1);
        file.set_len(5).await.unwrap();
        assert_eq!(file.metadata().await.unwrap().len(), 5);
        assert_eq!(file.stream_position().await.unwrap(), 1, "set_len leaves the cursor alone");
        file.sync_all().await.unwrap();
        file.sync_data().await.unwrap();
        assert_eq!(fs.read(&path).await.unwrap(), b"Jello");

        // Append writes at the end, wherever the cursor is.
        let mut log = fs.open_with(&path, OpenOptions::new().append(true)).await.unwrap();
        log.seek(SeekFrom::Start(0)).await.unwrap();
        let (result, _) = log.write_all(b"!".to_vec()).await;
        result.unwrap();
        assert_eq!(fs.read(&path).await.unwrap(), b"Jello!");

        // A clone shares the cursor.
        let mut clone = file.try_clone().await.unwrap();
        file.seek(SeekFrom::Start(2)).await.unwrap();
        assert_eq!(clone.stream_position().await.unwrap(), 2);
    }

    async fn open_options<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let path = dir.path("o");
        assert_eq!(fs.open(&path).await.err().unwrap().kind(), ErrorKind::NotFound);
        fs.create_new(&path).await.unwrap();
        assert_eq!(fs.create_new(&path).await.err().unwrap().kind(), ErrorKind::AlreadyExists);

        fs.write(&path, "long contents").await.unwrap();
        fs.create(&path).await.unwrap();
        assert_eq!(fs.metadata(&path).await.unwrap().len(), 0, "create truncates");

        let mut read_only = fs.open(&path).await.unwrap();
        let (written, _) = read_only.write(b"x".to_vec()).await;
        assert!(written.is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let private = dir.path("private");
            fs.open_with(&private, OpenOptions::new().write(true).create(true).mode(0o600)).await.unwrap();
            assert_eq!(fs.metadata(&private).await.unwrap().permissions().mode() & 0o777, 0o600);
        }
    }

    async fn permissions_and_times<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let path = dir.path("t");
        fs.write(&path, "x").await.unwrap();
        let original = fs.metadata(&path).await.unwrap().permissions();
        let mut read_only = original.clone();
        read_only.set_readonly(true);
        fs.set_permissions(&path, read_only).await.unwrap();
        assert!(fs.metadata(&path).await.unwrap().permissions().readonly());
        fs.set_permissions(&path, original.clone()).await.unwrap();
        assert!(!fs.metadata(&path).await.unwrap().permissions().readonly());

        let file = fs.open_with(&path, OpenOptions::new().write(true)).await.unwrap();
        file.set_permissions(original).await.unwrap();
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        file.set_modified(modified).await.unwrap();
        assert_eq!(file.metadata().await.unwrap().modified().unwrap(), modified);
        let later = modified + Duration::from_secs(60);
        file.set_times(FileTimes::new().set_modified(later)).await.unwrap();
        assert_eq!(fs.metadata(&path).await.unwrap().modified().unwrap(), later);
    }

    async fn advisory_locks<F: GatotkacaFs>(fs: F, dir: Scratch) {
        let path = dir.path("l");
        // Two separate opens, so two separate locks.
        let first = fs.create(&path).await.unwrap();
        let second = fs.open(&path).await.unwrap();

        first.lock().await.unwrap();
        assert!(matches!(second.try_lock_shared().await, Err(TryLockError::WouldBlock)));
        first.unlock().await.unwrap();

        second.lock_shared().await.unwrap();
        first.try_lock_shared().await.unwrap();
        assert!(matches!(second.try_lock().await, Err(TryLockError::WouldBlock)));
        first.unlock().await.unwrap();
        second.unlock().await.unwrap();
        first.try_lock().await.unwrap();
    }

    #[test]
    fn lock_wait_does_not_block_other_tasks() {
        let rt = runtime();
        let handle = rt.handle();
        let fs = TokioFs::new(handle.clone());
        let dir = Scratch::new("lock_wait");
        let path = dir.path("l");
        rt.block_on(async {
            let holder = fs.create(&path).await.unwrap();
            holder.lock().await.unwrap();
            let waiter = fs.open(&path).await.unwrap();
            let waiting = handle.spawn(async move { waiter.lock().await });
            // The waiter is parked on the blocking pool; timers and other tasks still run.
            handle.timer().sleep(Duration::from_millis(50)).await;
            assert!(!GatotkacaJoinHandle::is_finished(&waiting));
            holder.unlock().await.unwrap();
            handle.timer().timeout(Duration::from_secs(5), waiting).await.unwrap().unwrap().unwrap();
        });
    }

    #[test]
    fn read_dir_is_cancel_safe() {
        let rt = runtime();
        let fs = TokioFs::new(rt.handle());
        let dir = Scratch::new("cancel");
        for name in ["a", "b", "c", "d"] {
            std::fs::write(dir.path(name), name).unwrap();
        }
        let count = rt.block_on(async {
            let mut listing = fs.read_dir(&dir.0).await.unwrap();
            // Poll once, which starts the fetch on the blocking pool, then drop the future mid-flight.
            let first = {
                let mut next = std::pin::pin!(listing.next_entry());
                std::future::poll_fn(|cx| std::task::Poll::Ready(next.as_mut().poll(cx))).await
            };
            assert!(first.is_pending(), "the fetch should still be in flight");
            let mut count = 0;
            tokio::time::sleep(Duration::from_millis(50)).await;
            while listing.next_entry().await.unwrap().is_some() {
                count += 1;
            }
            count
        });
        assert_eq!(count, 4, "no entry lost to the cancelled call");
    }

    #[test]
    fn calls_fail_after_shutdown_and_return_no_buffer() {
        let rt = runtime();
        let fs = TokioFs::new(rt.handle());
        let dir = Scratch::new("shutdown");
        let path = dir.path("s");
        std::fs::write(&path, "data").unwrap();
        let file = rt.block_on(fs.open(&path)).unwrap();
        rt.shutdown(Some(Duration::from_secs(1)));

        let other = tokio::runtime::Runtime::new().unwrap();
        assert!(other.block_on(fs.read(&path)).is_err());
        let (result, buf) = other.block_on(file.read_at(Vec::with_capacity(4), 0));
        assert!(result.is_err());
        assert!(buf.is_empty());
    }
}
