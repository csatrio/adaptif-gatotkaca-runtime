//! Task-local storage: a value visible to one task and everything it awaits.
//!
//! Only the interface lives here. How keys are declared and stored is up to each implementation.

use std::future::Future;

/// A key to a task-local value. Keys are `'static`, usually a `static` declared by the implementation's own macro.
pub trait GatotkacaTaskLocal: Sync + 'static {
    /// The type of value the key holds.
    type Value: 'static;

    /// The future returned by [`scope`](Self::scope).
    type Scope<F: Future>: Future<Output = F::Output>;

    /// Runs `future` with the key set to `value`, for every poll of it.
    fn scope<F: Future>(&'static self, value: Self::Value, future: F) -> Self::Scope<F>;

    /// Runs `work` with the key set to `value`, on the calling thread.
    fn sync_scope<F, R>(&'static self, value: Self::Value, work: F) -> R
    where
        F: FnOnce() -> R;

    /// Calls `f` with the current value.
    ///
    /// # Panics
    ///
    /// Panics if called outside a [`scope`](Self::scope) or [`sync_scope`](Self::sync_scope) of this key.
    fn with<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&Self::Value) -> R;

    /// Calls `f` with the current value, or returns `None` outside a scope of this key.
    fn try_with<F, R>(&'static self, f: F) -> Option<R>
    where
        F: FnOnce(&Self::Value) -> R;
}
