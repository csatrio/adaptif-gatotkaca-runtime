# adaptif-gatotkaca-runtime: guide for agents

This crate is a **uniform facade over execution models** for the adaptif-gatotkaca engine. It holds only traits, a few types,
and one macro. Crates built on it (Python, Node.js and SQLite bindings, servers, telemetry) call the facade and never name an
executor. An **implementation** crate plugs a real executor in behind it: tokio, a plain thread pool, a thread-per-core runtime.

This guide is for agents working on this crate or writing an implementation of it. The doc comments on each trait are the
source of truth; this file is the map.

## Ground rules

- **Interface only.** No executor in `[dependencies]`, and no implementation ships in this crate, not even behind a feature.
  tokio and tokio-util are dev-dependencies, used to prove the traits fit a real executor.
- **No `dyn`.** Every trait is generic, with associated types and `impl Future` returns. Calls go straight to the executor. Do
  not add object-safe variants, boxing, or `Box<dyn Future>`.
- **`std` types only** in public signatures (`Future`, `Duration`, `Instant`, `io::Result`). No executor types.
- **Every public item has a doc comment** (`missing_docs` warns). Docs say what an implementation must guarantee.
- MSRV 1.89, edition 2024, 140-column rustfmt.

## Layout

| Path | Contents |
|---|---|
| `src/lib.rs` | Crate docs and module list. Keep it thin. |
| `src/runtime.rs` | `OwnedRuntime`, `GatotkacaRuntime<R>`, `ShutdownResult`, `GatotkacaRuntimeHandle`, `GatotkacaJoinHandle` |
| `src/timers.rs` | `GatotkacaTimer`, `GatotkacaInterval`, `GatotkacaTimerTask` |
| `src/cancellation.rs` | `GatotkacaCancellationToken` |
| `src/signals.rs` | `Signal`, `GatotkacaSignals`, `GatotkacaSignalListener` |
| `src/task_local.rs` | `GatotkacaTaskLocal` |
| `src/macros.rs` | `declare_runtime!`, the typed process-wide singleton |
| `src/tests/tokio_impl.rs` | **Reference implementation** of every trait on tokio. Read it first when implementing. |
| `src/tests.rs` | Behaviour tests against the reference implementation |

## How the pieces connect

```text
OwnedRuntime ──handle()──▶ GatotkacaRuntimeHandle ──spawn()──────────▶ JoinHandle<T>: GatotkacaJoinHandle
   │                          ├─timer()──────▶ GatotkacaTimer ──run_after / run_every──▶ TimerTask
   │                          │                                ──interval()───────────▶ Interval
   │                          ├─signals()────▶ GatotkacaSignals ──listen()─────────────▶ Listener
   │                          └─shutdown_token()──▶ GatotkacaCancellationToken
   └─ wrapped by GatotkacaRuntime<R>, made global by declare_runtime!(R)

GatotkacaTaskLocal stands alone: keys are statics the implementation declares.
```

Execution flow, the same on every executor:

1. `rt::install(MyRuntime::create())` at startup. `create()` returns a running runtime with no configuration; implementations
   offer their own constructors for settings.
2. Host threads (`main`, Python, Node, plain threads) enter through `rt::get().block_on(..)`.
3. Inside, work fans out through the handle: `spawn`, `spawn_blocking`, timers, signals. Blocking work inside a task uses
   `block_in_place`.
4. `rt::get().shutdown(timeout)`: graceful, then forced.

## Implementing the interface

Define these types, usually in your own crate:

| You write | Implements | tokio reference uses |
|---|---|---|
| runtime | `OwnedRuntime` | `TokioRuntime` (runtime in an `RwLock<Option<_>>`, so `shutdown(&self)` can take it) |
| handle | `GatotkacaRuntimeHandle` and usually `GatotkacaTimer` | `TokioHandle` (handle plus shared shutdown state) |
| join handle | `GatotkacaJoinHandle` + `Future<Output = Result<T, JoinError>>` | `tokio::task::JoinHandle<T>` |
| join error | `std::error::Error + Send + Sync` | `tokio::task::JoinError` |
| sleep, timeout, elapsed | futures and error | `tokio::time::{Sleep, Timeout, error::Elapsed}` |
| interval | `GatotkacaInterval` | `tokio::time::Interval` |
| timer task | `GatotkacaTimerTask` | `TokioTimerTask` (aborts on drop) |
| cancellation token | `GatotkacaCancellationToken` | `tokio_util::sync::CancellationToken` |
| signals, listener | `GatotkacaSignals`, `GatotkacaSignalListener` | `TokioSignals`, `TokioSignalListener` |
| task-local key | `GatotkacaTaskLocal` | `tokio::task::LocalKey<T>` |

### Behaviour every implementation must provide

Consumers rely on these, so every executor has to behave the same way. The tests in `src/tests.rs` check each one.

**Runtime**
- `block_on` is for callers outside the runtime. On a runtime worker it should panic instead of deadlocking.
- `handle()` is a cheap clone and works from any thread.
- `shutdown(timeout)` runs in this order: refuse new work; cancel the root shutdown token; wait up to `timeout` (`None` means
  forever) for all spawned work, including timer tasks; abort the rest. It returns `Clean`, `TimedOut`, or `AlreadyShutDown`
  on later calls. Started blocking work cannot be aborted.
- After shutdown begins, `spawn` and `spawn_blocking` return join handles that resolve to a cancellation error. The work never
  runs.

**Handle and join handles**
- A panic in spawned work becomes a `JoinError`. The runtime keeps running.
- Dropping a join handle **detaches** the task. `abort()` cancels it, and the handle then resolves to a cancellation error.
- `block_in_place` frees the worker if it can, or runs the work in place.
- `shutdown_token()` returns a **child** of the root token, so cancelling it never shuts the runtime down.

**Timers**
- `timer()`, `sleep`, `timeout` and `interval` work when called from any thread, not only inside a task. tokio does this by
  entering the handle's context in each constructor.
- An interval's first tick is immediate. Missed ticks are **skipped**, never burst.
- `run_after` and `run_every` spawn through the handle, so shutdown tracks them. `run_every` first fires after one period. A
  panicking callback ends that timer only.
- Dropping a timer task **cancels** it. `detach()` lets it run on.

**Cancellation**
- Clones share state. Cancelling a parent cancels its children, but not the reverse. `cancelled()` owns its token (`'static`)
  and completes at once if the token is already cancelled.

**Signals**
- Install a handler only on first `listen` or `ctrl_c`, so Python and Node keep their own Ctrl-C handling.
- An unsupported signal returns `io::ErrorKind::Unsupported`. `Hangup` is Unix only.
- Deliveries between `recv` calls may merge into one. `recv` returns `None` once no more can arrive.

**Task-locals**
- Interface only: the implementation chooses how keys are declared and stored. `with` panics outside a scope, and `try_with`
  returns `None` there.

### Thread-per-core executors

The traits require `Send + Sync`. A thread-per-core executor may satisfy them with `unsafe impl Send`/`Sync` only under the
contract in README.md, "Implementing for a thread-per-core runtime". In short: handles forward work to the thread that owns
it; thread-bound futures are polled only on that thread; wakers are thread-safe; drop is safe anywhere; and `block_on`
serves foreign threads.

## Changing this crate

- Add behaviour by extending a trait, with associated types and no boxing. Then update `src/tests/tokio_impl.rs` and add a test
  to `src/tests.rs` that pins down the behaviour.
- Timing tests run against real time. Leave wide margins (periods of 50 ms or more) so they pass on a loaded CI machine.
- Record public API changes under `## [Unreleased]` in `CHANGELOG.md` (Keep a Changelog format).
- Before finishing:

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```
