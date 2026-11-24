//! Cooperative cancellation, shared across many tasks.

use std::future::Future;

/// A token that tasks watch to know when to stop.
///
/// Clones share state: cancelling one cancels all of them. A [child](Self::child_token) is cancelled with its parent, but
/// cancelling the child leaves the parent alone.
pub trait GatotkacaCancellationToken: Clone + Send + Sync + 'static {
    /// The future returned by [`cancelled`](Self::cancelled). It owns a clone of the token, so it can move into a task.
    type Cancelled: Future<Output = ()> + Send + 'static;

    /// Creates a token that is not cancelled.
    fn new() -> Self;

    /// Cancels this token, its clones and its children. Cancelling twice has no further effect.
    fn cancel(&self);

    /// Whether the token has been cancelled.
    fn is_cancelled(&self) -> bool;

    /// Completes once the token is cancelled. Completes at once if it already is.
    fn cancelled(&self) -> Self::Cancelled;

    /// A new token cancelled whenever this one is.
    fn child_token(&self) -> Self;
}
