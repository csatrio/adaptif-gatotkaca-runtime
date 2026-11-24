//! Runtime abstraction for the adaptif-gatotkaca engine.
//!
//! A uniform facade over execution models. Crates written against these traits run unchanged on tokio, a thread pool, a
//! thread-per-core executor or anything else an implementation plugs in. Only the interface lives here; implementations live
//! in their own crates. See the README for the contract thread-per-core implementations must keep.

pub mod cancellation;
mod macros;
pub mod runtime;
pub mod signals;
pub mod task_local;
pub mod timers;

#[cfg(test)]
mod tests;
