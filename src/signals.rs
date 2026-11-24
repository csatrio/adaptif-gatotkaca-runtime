//! Process signals, in a portable subset.
//!
//! Handlers are installed only when something listens. FFI hosts such as Python or Node keep their own Ctrl-C handling as long
//! as nothing in the process calls these methods.

use std::future::Future;
use std::io;

/// A process signal that can be listened for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Signal {
    /// Ctrl-C: `SIGINT` on Unix, the console Ctrl-C event on Windows.
    Interrupt,
    /// A request to stop: `SIGTERM` on Unix, the console close event on Windows.
    Terminate,
    /// The controlling terminal closed: `SIGHUP`. Unix only.
    Hangup,
}

/// The signal source for a runtime, from [`GatotkacaRuntimeHandle::signals`](crate::runtime::GatotkacaRuntimeHandle::signals).
pub trait GatotkacaSignals: Clone + Send + Sync + 'static {
    /// The listener returned by [`listen`](Self::listen).
    type Listener: GatotkacaSignalListener;

    /// Starts listening for `signal`, installing its handler on first use.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] if the platform lacks `signal`, or with the OS error if the handler cannot be
    /// installed.
    fn listen(&self, signal: Signal) -> io::Result<Self::Listener>;

    /// Completes at the next Ctrl-C. A shortcut for listening to [`Signal::Interrupt`] once.
    fn ctrl_c(&self) -> impl Future<Output = io::Result<()>> + Send + 'static;
}

/// Receives deliveries of one signal, from [`GatotkacaSignals::listen`].
///
/// Deliveries that arrive between calls to [`recv`](Self::recv) may merge into one.
pub trait GatotkacaSignalListener: Send + 'static {
    /// Completes at the next delivery. Returns `None` once no more deliveries can arrive.
    fn recv(&mut self) -> impl Future<Output = Option<()>> + Send + '_;
}
