# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-11-26

Breaking for implementors: `GatotkacaRuntimeHandle` has a new required method, `spawn_local`. Code that only calls the facade
is unaffected. The `fs` module is new and standalone, so it breaks nothing.

### Added

- `fs` module: the operations of `std::fs`, made asynchronous, so the filesystem can be std on a blocking pool, tokio,
  monoio / io_uring or inline std without changing callers. Standalone: chosen separately from the runtime handle.
  - `GatotkacaFs`: every `std::fs` free function (`read`, `read_to_string`, `write`, `copy`, `rename`, `hard_link`,
    `remove_file`, `create_dir`, `create_dir_all`, `remove_dir`, `remove_dir_all`, `read_dir`, `metadata`,
    `symlink_metadata`, `set_permissions`, `canonicalize`, `read_link`, `exists`), plus `open_with`, `open`, `create` and
    `create_new`.
  - `GatotkacaFile`: positional I/O (`read_at`, `read_exact_at`, `write_at`, `write_all_at`, on `&self`) and cursor I/O
    (`read`, `read_exact`, `read_to_end`, `write`, `write_all`, `flush`, `seek`, `stream_position`, `rewind`), plus
    `sync_all`, `sync_data`, `set_len`, `metadata`, `set_permissions`, `set_times`, `set_modified`, `try_clone` and advisory
    locks (`lock`, `lock_shared`, `try_lock`, `try_lock_shared`, `unlock`).
  - `GatotkacaReadDir` (cancel-safe `next_entry`) and `GatotkacaDirEntry`.
  - `OpenOptions`: a plain `Copy` value mirroring `std::fs::OpenOptions` (`mode` and `custom_flags` on unix), with `to_std`.
  - Buffers are owned: reads and writes take a `Vec<u8>` and return it with the result, so completion-based implementations
    are sound. Reads fill spare capacity; writes write `buf[..]`. Results use std's `Metadata`, `Permissions`, `FileType`,
    `FileTimes`, `SeekFrom` and `TryLockError`.
- Reference `fs` implementation on `std::fs` through an offload strategy: the tokio reference's blocking pool, and inline on
  the polling thread. Both run the same tests.

- `GatotkacaRuntimeHandle::spawn_local(make)`: runs a future that is not `Send`, pinned to one thread. `make` is a `Send`
  factory that the handle forwards to a thread the implementation chooses; it builds the future there, and the future is polled
  and dropped only on that thread. Callable from any thread. Returns the usual `JoinHandle`. Panics, abort, detach-on-drop and
  shutdown (refused after it begins, waited on, then aborted) behave as for `spawn`.
- Reference implementation on tokio-util `LocalPoolHandle::spawn_pinned`, wrapped in a task tracked by the runtime, with
  tests.

### Changed

- `block_in_place` must not panic. An implementation that cannot free the worker, such as a local worker, runs the work in
  place. The tokio reference now does so on non-multi-thread runtimes.

## [0.2.0] - 2026-11-26

Breaking for implementors: `GatotkacaRuntimeHandle` has a new associated type and method. Code that only calls the facade is
unaffected.

### Added

- `readiness` module: wait until a raw OS object may be read or written, through the runtime's own poller.
  - `RawSource`: `RawFd` on unix, `RawSocket` on Windows (uninhabited elsewhere).
  - `GatotkacaReadiness`: `unsafe fn register(&self, RawSource)`. The registration borrows the object, which must stay open
    until the registration is dropped; dropping it deregisters and closes nothing. Fails once shutdown has begun.
  - `GatotkacaRegistration`: `readable`, `writable`, `clear_readable` and `clear_writable`, with level semantics: a wait
    resolves at once until the caller, having seen `WouldBlock`, clears it, and clearing never loses a wake-up (an
    implementation over an edge-triggered poller checks the level when clearing). An object ready at registration wakes the
    first wait; end of file, hang-up and errors count as ready.
- `GatotkacaRuntimeHandle::Readiness` and `GatotkacaRuntimeHandle::readiness()`.
- Reference implementation on tokio `AsyncFd` (unix) in the tests, with tests for pipes and socket pairs. Windows
  implementations need their own poller (for example a `WSAPoll` thread); the reference returns `Unsupported` there.

### Changed

- Lints deny `unsafe_op_in_unsafe_fn`, undocumented `unsafe` blocks and `unsafe fn` without a `# Safety` section.

## [0.1.0] - 2026-11-24

### Added

- Interface-only facade over execution models. All traits are generic, with no boxing or dynamic dispatch, and the crate has
  no executor dependency.
- `runtime` module:
  - `OwnedRuntime`: `create`, `block_on`, `handle`, and a graceful-then-forced `shutdown` returning `ShutdownResult`.
  - `GatotkacaRuntime<R>`: a thin wrapper around a concrete runtime.
  - `GatotkacaRuntimeHandle`: `spawn`, `spawn_blocking`, `block_in_place`, `timer`, `signals` and `shutdown_token`.
  - `GatotkacaJoinHandle`: `abort` and `is_finished`. Dropping a join handle detaches its task.
- `declare_runtime!` macro: a typed process-wide singleton with `install`, `get` and `try_get`.
- `timers` module:
  - `GatotkacaTimer`: `sleep`, `sleep_until`, `yield_now`, `timeout`, `interval`, `run_after` and `run_every`.
  - `GatotkacaInterval`: `tick`, `reset` and `period`. Missed ticks are skipped.
  - `GatotkacaTimerTask`: `cancel`, `is_finished` and `detach`. Dropping a timer task cancels it, and a panicking callback
    stops only its own timer.
- `cancellation` module: `GatotkacaCancellationToken`, with child tokens.
- `signals` module: `Signal` (`Interrupt`, `Terminate`, `Hangup`), `GatotkacaSignals` and `GatotkacaSignalListener`. Handlers
  are installed only on first use.
- `task_local` module: `GatotkacaTaskLocal`, an interface only; implementations choose how keys are stored.
- A documented safety contract for thread-per-core runtimes that meet the `Send + Sync` bounds with `unsafe impl`.

[0.3.0]: https://github.com/csatrio/adaptif-gatotkaca-runtime/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/csatrio/adaptif-gatotkaca-runtime/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/csatrio/adaptif-gatotkaca-runtime/releases/tag/v0.1.0
