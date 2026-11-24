//! Timer traits, modelled on the common parts of `tokio::time`.
//!
//! A timer comes from [`GatotkacaRuntimeHandle::timer`](crate::runtime::GatotkacaRuntimeHandle::timer). It is a cheap clone, so it
//! can move into spawned work. Like the runtime traits, everything is generic so calls go straight to the executor.

use std::future::Future;
use std::time::{Duration, Instant};

/// Sleeps, timeouts, intervals and timed callbacks on the underlying runtime.
pub trait GatotkacaTimer: Clone + Send + Sync + 'static {
    /// The future returned by [`sleep`](Self::sleep) and [`sleep_until`](Self::sleep_until).
    type Sleep: Future<Output = ()> + Send + 'static;

    /// The future returned by [`timeout`](Self::timeout). It is `Send` when `F` is, for a concrete timer.
    type Timeout<F: Future>: Future<Output = Result<F::Output, Self::Elapsed>>;

    /// The error a [`timeout`](Self::timeout) yields when the deadline passes first.
    type Elapsed: std::error::Error + Send + Sync + 'static;

    /// The ticker returned by [`interval`](Self::interval).
    type Interval: GatotkacaInterval;

    /// The handle returned by [`run_after`](Self::run_after) and [`run_every`](Self::run_every).
    type TimerTask: GatotkacaTimerTask;

    /// Completes after `duration` has passed.
    fn sleep(&self, duration: Duration) -> Self::Sleep;

    /// Completes once `deadline` is reached. Completes at once if it is already past.
    fn sleep_until(&self, deadline: Instant) -> Self::Sleep;

    /// Yields once to the scheduler, letting other tasks run before this one continues.
    fn yield_now(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Runs `future`, giving up if it has not finished within `duration`. On timeout the future is dropped.
    fn timeout<F: Future>(&self, duration: Duration, future: F) -> Self::Timeout<F>;

    /// A ticker that fires every `period`. The first tick completes at once. Ticks missed during a stall are skipped, not
    /// fired in a burst; the next tick lands on the original schedule.
    ///
    /// # Panics
    ///
    /// Panics if `period` is zero.
    fn interval(&self, period: Duration) -> Self::Interval;

    /// Calls `callback` once, after `delay`, on the runtime.
    fn run_after<F>(&self, delay: Duration, callback: F) -> Self::TimerTask
    where
        F: FnOnce() + Send + 'static;

    /// Calls `callback` every `period` on the runtime until cancelled. The first call is after one `period`. Missed calls are
    /// skipped, as with [`interval`](Self::interval). If `callback` panics, the timer stops; the runtime carries on.
    ///
    /// # Panics
    ///
    /// Panics if `period` is zero.
    fn run_every<F>(&self, period: Duration, callback: F) -> Self::TimerTask
    where
        F: FnMut() + Send + 'static;
}

/// A ticker that fires at a fixed period, from [`GatotkacaTimer::interval`].
pub trait GatotkacaInterval: Send + 'static {
    /// Completes at the next tick and returns the instant it was scheduled for.
    fn tick(&mut self) -> impl Future<Output = Instant> + Send + '_;

    /// Restarts the period from now, so the next tick is one period away.
    fn reset(&mut self);

    /// The period between ticks.
    fn period(&self) -> Duration;
}

/// A running timed callback, from [`GatotkacaTimer::run_after`] or [`GatotkacaTimer::run_every`].
///
/// Dropping it cancels the callback, so a forgotten `run_every` cannot run forever. Call [`detach`](Self::detach) to let it run
/// on unowned.
pub trait GatotkacaTimerTask: Send + Sync + 'static {
    /// Stops the callback. A call already in progress runs to completion.
    fn cancel(&self);

    /// Whether the task has ended, by finishing, being cancelled or panicking.
    fn is_finished(&self) -> bool;

    /// Lets the callback keep running without this handle. It can no longer be cancelled.
    fn detach(self);
}
