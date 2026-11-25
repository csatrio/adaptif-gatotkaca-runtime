//! Readiness traits: wait until an OS object (a file descriptor on unix, a socket on Windows) may be read or written.
//!
//! A readiness source comes from [`GatotkacaRuntimeHandle::readiness`](crate::runtime::GatotkacaRuntimeHandle::readiness). It
//! registers a raw OS object with the runtime's poller and hands back a [`GatotkacaRegistration`], whose futures resolve when
//! the object may be ready. The I/O itself stays with the caller: it uses its own non-blocking calls on the object and comes
//! back here only after they return [`WouldBlock`](std::io::ErrorKind::WouldBlock).
//!
//! # The waiting protocol
//!
//! ```text
//! // SAFETY: `raw` stays open until `registration` is dropped.
//! let registration = unsafe { handle.readiness().register(raw) }?;
//! loop {
//!     registration.readable().await?;
//!     loop {
//!         match nonblocking_read() {
//!             Ok(data) => handle(data),
//!             Err(WouldBlock) => break,
//!             Err(e) => return Err(e),
//!         }
//!         // A caller that stops early (a budget) simply loops: readiness is still set, so `readable` resolves at once.
//!     }
//!     registration.clear_readable();
//! }
//! ```
//!
//! # Level semantics
//!
//! Readiness behaves like a level-triggered poller (`poll`, `WSAPoll`, epoll without `EPOLLET`), whatever the executor uses
//! underneath:
//!
//! - **Ready until cleared.** Once [`readable`](GatotkacaRegistration::readable) has resolved, it keeps resolving at once
//!   until the caller calls [`clear_readable`](GatotkacaRegistration::clear_readable). Waiting never consumes readiness, so a
//!   caller may stop early (a budget) and wait again without losing anything. The same holds for `writable` and
//!   `clear_writable`.
//! - **Clear only after `WouldBlock`.** A caller clears a direction only after a non-blocking call on the object returned
//!   `WouldBlock`. The poller sees only what the OS reports; an object with user-space state (a queue whose doorbell rings
//!   only after its reader saw `WouldBlock`) can hold data the OS does not report, and clearing early would wait on it.
//! - **Clearing never loses readiness.** If the object is ready when `clear_*` runs, or becomes ready afterwards, the next
//!   wait still resolves. An implementation over an edge-triggered poller (epoll with `EPOLLET`, kqueue with `EV_CLEAR`, which
//!   is what tokio and mio use) must therefore check the object's level when clearing, or otherwise close the window between
//!   the caller's `WouldBlock` and the clear. The caller never retries before waiting.
//! - **Starts with the current state.** An object that is already ready when it is registered makes the first wait resolve.
//! - **Errors and hang-ups count as ready.** End of file, a closed peer or a socket error make the object readable (and, where
//!   the OS reports it, writable); the caller's next operation reports what happened.
//! - **Spurious wake-ups are allowed.** A wait may resolve when the object is not ready; the caller's operation then returns
//!   `WouldBlock` and the caller clears and waits again.

#[cfg(unix)]
mod raw {
    /// A pollable OS object: a file descriptor on unix.
    ///
    /// A registration borrows it: the caller keeps it open until the registration is dropped (see
    /// [`GatotkacaReadiness::register`](super::GatotkacaReadiness::register)).
    pub type RawSource = std::os::fd::RawFd;
}

#[cfg(windows)]
mod raw {
    /// A pollable OS object: a socket on Windows.
    ///
    /// A registration borrows it: the caller keeps it open until the registration is dropped (see
    /// [`GatotkacaReadiness::register`](super::GatotkacaReadiness::register)).
    pub type RawSource = std::os::windows::io::RawSocket;
}

#[cfg(not(any(unix, windows)))]
mod raw {
    /// A pollable OS object. This target has none the interface knows about, so the type is uninhabited and
    /// [`GatotkacaReadiness::register`](super::GatotkacaReadiness::register) cannot be called.
    pub type RawSource = std::convert::Infallible;
}

pub use raw::RawSource;

/// Registers OS objects with the runtime's poller. From
/// [`GatotkacaRuntimeHandle::readiness`](crate::runtime::GatotkacaRuntimeHandle::readiness); a cheap clone that works from any
/// thread.
pub trait GatotkacaReadiness: Clone + Send + Sync + 'static {
    /// The registration [`register`](Self::register) hands back.
    type Registration: GatotkacaRegistration;

    /// Registers `source` for read and write readiness.
    ///
    /// The source is **borrowed, not owned**: dropping the registration deregisters it and leaves it open, so it can be closed
    /// after that. It should be in non-blocking mode, since the caller's own calls on it must return `WouldBlock` rather than
    /// block.
    ///
    /// Register an object at most once at a time per runtime: a second live registration of the same source may fail (epoll
    /// reports `AlreadyExists`) or interfere with the first. Many different sources may be registered at once.
    ///
    /// Works from any thread, not only inside a task. Returns an error if the poller refuses the source (for example a regular
    /// file on epoll, or a non-socket handle on Windows), if this target has no poller the implementation supports
    /// ([`Unsupported`](std::io::ErrorKind::Unsupported)), or once [`OwnedRuntime::shutdown`](crate::runtime::OwnedRuntime::shutdown)
    /// has begun.
    ///
    /// # Safety
    ///
    /// `source` must be an open file descriptor (unix) or socket (Windows), and it must stay open and refer to the same open
    /// file description (socket) until the returned registration is dropped: the caller does not close it, or let its number
    /// be reused for another object, while the registration lives. The implementation may poll and deregister it at any time
    /// until then, including in the registration's `Drop`. This is the I/O-safety contract of `BorrowedFd::borrow_raw` and of
    /// tokio's `AsyncFd::register`, extended to the registration's lifetime.
    unsafe fn register(&self, source: RawSource) -> std::io::Result<Self::Registration>;
}

/// One registered OS object. Dropping it deregisters the object; the object itself stays open.
///
/// The futures borrow the registration and may be polled from any thread. One task at a time should wait for each direction
/// (one `readable` and one `writable` may be pending together); with more, an implementation may wake only one of them.
/// Dropping a pending future is safe and does not change readiness. See the [module docs](self) for the full contract.
pub trait GatotkacaRegistration: Send + Sync + 'static {
    /// Resolves once the object may be readable.
    ///
    /// Level semantics: it resolves again at once until the caller, having seen `WouldBlock`, calls
    /// [`clear_readable`](Self::clear_readable). End of file, a closed peer and errors count as readable. An `Err` means
    /// readiness can no longer be reported (for example the runtime has shut down); waiting again will not help.
    fn readable(&self) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Resolves once the object may be writable. Level semantics, as for [`readable`](Self::readable), cleared by
    /// [`clear_writable`](Self::clear_writable).
    fn writable(&self) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Clears read readiness, after a non-blocking read returned `WouldBlock`.
    ///
    /// Never loses a wake-up: if the object is still readable, or becomes readable afterwards, the next
    /// [`readable`](Self::readable) resolves.
    fn clear_readable(&self);

    /// Clears write readiness, after a non-blocking write returned `WouldBlock`. Never loses a wake-up, as for
    /// [`clear_readable`](Self::clear_readable).
    fn clear_writable(&self);
}
