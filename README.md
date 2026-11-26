# adaptif-gatotkaca-runtime

[![crates.io](https://img.shields.io/crates/v/adaptif-gatotkaca-runtime.svg)](https://crates.io/crates/adaptif-gatotkaca-runtime)
[![docs.rs](https://docs.rs/adaptif-gatotkaca-runtime/badge.svg)](https://docs.rs/adaptif-gatotkaca-runtime)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-blue.svg)](https://blog.rust-lang.org/)

The runtime interface of the adaptif-gatotkaca engine, developed by [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).

## Why this crate exists

Every part of gatotkaca runs async work: the Python and Node.js bindings, the SQLite layer, servers, telemetry. Different
deployments need different executors. tokio is the common choice, a plain thread pool fits small or embedded hosts, and a
thread-per-core runtime gives the most throughput.

This crate makes the **whole gatotkaca runtime model uniform**. Whichever executor runs underneath, code that uses gatotkaca
has the same:

- **shape:** one set of traits for running, spawning, timing, cancelling, signalling, waiting for I/O readiness, filesystem
  access and task-local state;
- **flow:** install once, enter with `block_on`, fan out through a handle, shut down gracefully;
- **execution path:** the same rules for panics, cancellation, missed timer ticks, shutdown order and signal handling.

You write a binding or a service once, against the interface. Switching executors means switching the implementation crate,
with no change to your code. You can also build everything against a stand-in implementation (tokio, say) before your
production runtime is ready.

The crate is **interface only**: traits, a few types and one macro, with no executor dependency. Every trait is generic rather
than object-safe, so the executor is known at compile time and calls go straight to it, with no boxing and no vtables.

## Install

```toml
[dependencies]
adaptif-gatotkaca-runtime = "0.2"
```

You also need an implementation crate that provides a concrete runtime.

## At a glance

Declare the process-wide runtime once, in the crate that picks the executor:

```rust,ignore
pub mod rt {
    adaptif_gatotkaca_runtime::declare_runtime!(pub my_runtime::MyRuntime);
}
```

Then use it the same way everywhere, whatever `MyRuntime` is:

```rust,ignore
use adaptif_gatotkaca_runtime::runtime::{GatotkacaRuntimeHandle, OwnedRuntime};
use adaptif_gatotkaca_runtime::timers::{GatotkacaTimer, GatotkacaTimerTask};
use adaptif_gatotkaca_runtime::cancellation::GatotkacaCancellationToken;

rt::install(MyRuntime::create());

rt::get().block_on(async {
    let handle = rt::get().handle();

    // Spawn work and await its result.
    let answer = handle.spawn(async { 42 }).await?;

    // Periodic work that stops when shutdown begins.
    let heartbeat = handle.timer().run_every(Duration::from_secs(5), || send_heartbeat());
    handle.shutdown_token().cancelled().await;
    heartbeat.cancel();
});

rt::get().shutdown(Some(Duration::from_secs(10)));
```

## The execution model

```text
install ──▶ block_on (host thread) ──▶ handle ──┬─ spawn / spawn_local / spawn_blocking / block_in_place
                                                ├─ timer: sleep, timeout, interval, run_after, run_every
                                                ├─ signals: listen, ctrl_c
                                                ├─ readiness: register a raw fd / socket, then readable, writable, clear_*
                                                └─ shutdown_token
shutdown: refuse new work ─▶ cancel shutdown token ─▶ wait up to timeout ─▶ abort the rest
```

| Module | Provides |
|---|---|
| `runtime` | `OwnedRuntime`, `GatotkacaRuntime`, `GatotkacaRuntimeHandle`, `GatotkacaJoinHandle`, `ShutdownResult` |
| `timers` | `GatotkacaTimer`, `GatotkacaInterval`, `GatotkacaTimerTask` |
| `cancellation` | `GatotkacaCancellationToken` |
| `signals` | `Signal`, `GatotkacaSignals`, `GatotkacaSignalListener` |
| `readiness` | `RawSource`, `GatotkacaReadiness`, `GatotkacaRegistration` |
| `task_local` | `GatotkacaTaskLocal` |
| `fs` | `GatotkacaFs`, `GatotkacaFile`, `GatotkacaReadDir`, `GatotkacaDirEntry`, `OpenOptions`: `std::fs`, asynchronous, with owned buffers; standalone from the handle |
| crate root | `declare_runtime!` |

Guarantees every implementation provides:

- A panicking task becomes a `JoinError`; the runtime keeps running.
- Dropping a join handle detaches its task. Dropping a timer task cancels it.
- Intervals skip missed ticks instead of firing them in a burst.
- Signal handlers are installed only when something listens, so Python and Node keep control of Ctrl-C.
- After shutdown begins, new work is refused and its join handle resolves to a cancellation error.
- Readiness is level-triggered, whatever the executor polls with: a wait resolves at once until the caller, having seen
  `WouldBlock`, clears it, and clearing never loses a wake-up.

## Waiting for readiness

The runtime's poller can watch an OS object your code owns: a file descriptor on unix, a socket on Windows. You keep doing
the non-blocking I/O yourself and come back to wait only after it returns `WouldBlock`:

```rust,ignore
use adaptif_gatotkaca_runtime::readiness::{GatotkacaReadiness, GatotkacaRegistration};

// SAFETY: `receiver` stays open until `registration` is dropped.
let registration = unsafe { handle.readiness().register(receiver.as_raw_fd()) }?;
loop {
    registration.readable().await?;
    loop {
        match receiver.try_recv() {
            Ok(message) => process(message),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }
    registration.clear_readable();
}
```

`register` is `unsafe` because the registration borrows the raw object: it must stay open, as the same object, until the
registration is dropped. Dropping the registration deregisters the object and closes nothing. The full contract is in the
`readiness` module docs. On Windows an implementation needs its own poller (mio cannot register foreign sockets), for example
one `WSAPoll` thread over the registered sockets.

## Implementing the interface

An implementation crate provides a runtime, a handle and the associated types each trait asks for. The tests here contain a
complete reference implementation on tokio (`src/tests/tokio_impl.rs`), and [AGENTS.md](AGENTS.md) maps every trait and the
behaviour it must provide.

### Thread-per-core runtimes

The traits require `Send + Sync` on handles, timers, tokens and their futures. A thread-per-core executor, where tasks never
leave the thread that spawned them, may meet those bounds with `unsafe impl Send` / `unsafe impl Sync`, but only if it keeps
these promises. Breaking them is undefined behaviour.

- **Handles forward, never touch.** A handle used from another thread must not reach into thread-local executor state. It sends
  the request (spawn, timer registration, signal subscription) to the owning thread, for example over a channel, and that
  thread does the work. `spawn_local` takes a `Send` factory for this reason: the handle sends it to a core, which builds the
  `!Send` future and keeps it.
- **Poll where valid.** A future, timer, interval or listener tied to one thread's reactor must only be polled on that thread,
  or must be internally thread-safe. If it can end up polled elsewhere, it must not be marked `Send`.
- **Wakers are thread-safe.** A `Waker` handed out must be safe to wake from any thread, as `std::task::Waker` requires.
- **Drop is safe anywhere.** Dropping a marked type on a foreign thread must not touch thread-local state. Forward the drop to
  the owner if needed.
- **`block_on` from host threads.** Foreign threads (Python, Node, plain `std::thread`) call `block_on`. An executor with one
  runtime per core either runs the future on a core and blocks the caller until it finishes, or builds a temporary local
  runtime for the call.

## Status

Early development (0.x). The interface may still change between minor versions; see [CHANGELOG.md](CHANGELOG.md).

## Development

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo clippy --target x86_64-pc-windows-msvc --all-targets -- -D warnings
```

tokio, tokio-util and (on unix) libc are dev-dependencies only. The tests implement every trait on tokio to prove the
interface fits a real executor; readiness is implemented there on unix only.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate, as defined
in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
