//! Runtime abstraction for the adaptif-gatotkaca engine.
//!
//! The crate holds the traits that stand between gatotkaca and whatever runs its work, so callers see one interface whether the
//! engine runs on tokio or on a plain thread pool.

pub mod runtime;
