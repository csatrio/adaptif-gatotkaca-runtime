# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/csatrio/adaptif-gatotkaca-runtime/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/csatrio/adaptif-gatotkaca-runtime/releases/tag/v0.1.0
