//! Filesystem traits: the operations of [`std::fs`], made asynchronous, over whichever implementation runs underneath.
//!
//! The filesystem is **standalone**: a [`GatotkacaFs`] is chosen separately from the runtime handle, so a crate can pair any
//! runtime with any filesystem. Typical implementations:
//!
//! - **std on a blocking pool** (what tokio's own `tokio::fs` does): each call runs the `std::fs` operation on a blocking
//!   thread.
//! - **Completion-based** (monoio, tokio-uring, io_uring, IOCP): each call is submitted to the kernel and completes later.
//! - **Inline std**: each call runs the `std::fs` operation on the polling thread. Only for executors where blocking a worker is
//!   acceptable.
//!
//! The functions mirror `std::fs` one for one, and the return types are std's own: [`Metadata`], [`Permissions`],
//! [`FileType`], [`FileTimes`], [`SeekFrom`] and [`TryLockError`].
//!
//! # Buffers are owned
//!
//! File reads and writes take a `Vec<u8>` by value and hand it back with the result, as `(io::Result<_>, Vec<u8>)`. On a
//! completion-based implementation the kernel writes into the buffer while the operation is in flight, possibly after the
//! future was dropped, so a borrowed slice cannot be used soundly. Owning the buffer makes every implementation safe and lets
//! the caller reuse the allocation.
//!
//! - **Reads fill spare capacity.** A read writes at most `buf.capacity() - buf.len()` bytes after the existing contents and
//!   grows the length by the count read. The contents already there are kept. Reserve capacity first (`Vec::with_capacity`,
//!   `reserve`): a read into a buffer with no spare capacity reads nothing and returns `Ok(0)`, which looks like end of file.
//! - **Writes write `buf[..]`**, starting at its first byte. A short write returns the count; the `*_all` variants loop.
//! - **The buffer comes back** on success and on error. The one exception is an implementation that could not run the
//!   operation at all (for example its runtime is shut down): it returns the error with an empty `Vec`.
//!
//! # Futures
//!
//! - Futures are `Send`, and nothing happens until one is first polled.
//! - **Dropping a future does not undo the operation.** Once started, it may still complete: a write may land, the cursor may
//!   move, a file may be created. Its buffer is dropped when the operation ends. Cursor I/O after a cancelled cursor operation
//!   starts from an unspecified position; [`seek`](GatotkacaFile::seek) first.
//! - Errors are the operating system's, as `std::fs` reports them, so [`io::ErrorKind`] matches across implementations
//!   (`NotFound`, `AlreadyExists`, `PermissionDenied`, `UnexpectedEof` for a short `*_exact` read, `WriteZero`).
//!
//! # Cursor and positional I/O
//!
//! A [`GatotkacaFile`] offers both:
//!
//! - **Positional** (`read_at`, `write_at`, `read_exact_at`, `write_all_at`) takes `&self` and an explicit offset, so several
//!   can run at once on one file. They do not use the cursor. Whether they move it is platform-defined, as in std (on Windows
//!   they do), so [`seek`](GatotkacaFile::seek) before going back to cursor I/O.
//! - **Cursor** (`read`, `write`, `read_exact`, `write_all`, `read_to_end`, `seek`, `flush`) takes `&mut self` and works like
//!   [`std::io::Read`], [`Write`](std::io::Write) and [`Seek`](std::io::Seek) on a `File`. A file opened with
//!   [`append`](OpenOptions::append) writes at the end whatever the cursor says; on Linux positional writes append too.
//!
//! An implementation with no OS cursor, such as one over io_uring, keeps the cursor itself.
//!
//! # Example
//!
//! ```
//! use std::io;
//! use std::path::Path;
//!
//! use adaptif_gatotkaca_runtime::fs::{GatotkacaFile, GatotkacaFs};
//!
//! async fn header<F: GatotkacaFs>(fs: &F, path: &Path) -> io::Result<Vec<u8>> {
//!     let file = fs.open(path).await?;
//!     let (result, header) = file.read_exact_at(Vec::with_capacity(16), 0).await;
//!     result?;
//!     Ok(header)
//! }
//! ```

use std::ffi::OsString;
use std::fs::{FileTimes, FileType, Metadata, Permissions, TryLockError};
use std::future::Future;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Asynchronous filesystem operations: the free functions of [`std::fs`], plus opening files.
///
/// Cheap to clone; clones share whatever state the implementation keeps. Every method works from any thread.
pub trait GatotkacaFs: Clone + Send + Sync + 'static {
    /// An open file.
    type File: GatotkacaFile;

    /// An iterator over the entries of a directory, from [`read_dir`](Self::read_dir).
    type ReadDir: GatotkacaReadDir<DirEntry = Self::DirEntry>;

    /// One entry of a directory.
    type DirEntry: GatotkacaDirEntry;

    /// Opens a file with `options`. See [`std::fs::OpenOptions::open`].
    fn open_with(&self, path: impl AsRef<Path>, options: OpenOptions) -> impl Future<Output = io::Result<Self::File>> + Send;

    /// Opens a file for reading. See [`std::fs::File::open`].
    fn open(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>> + Send {
        self.open_with(path, OpenOptions::new().read(true))
    }

    /// Opens a file for writing, creating it or truncating it. See [`std::fs::File::create`].
    fn create(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>> + Send {
        self.open_with(path, OpenOptions::new().write(true).create(true).truncate(true))
    }

    /// Creates a new file for reading and writing, failing with `AlreadyExists` if it exists. See
    /// [`std::fs::File::create_new`].
    fn create_new(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>> + Send {
        self.open_with(path, OpenOptions::new().read(true).write(true).create_new(true))
    }

    /// Reads a whole file. See [`std::fs::read`].
    fn read(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Vec<u8>>> + Send;

    /// Reads a whole file as UTF-8. Invalid UTF-8 is an `InvalidData` error. See [`std::fs::read_to_string`].
    fn read_to_string(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<String>> + Send;

    /// Writes `contents` as the whole file, creating or truncating it. See [`std::fs::write`].
    ///
    /// `contents` is owned so a completion-based implementation can hand it to the kernel.
    fn write(&self, path: impl AsRef<Path>, contents: impl AsRef<[u8]> + Send + 'static) -> impl Future<Output = io::Result<()>> + Send;

    /// Copies a file's contents and permissions, returning the number of bytes copied. See [`std::fs::copy`].
    fn copy(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> impl Future<Output = io::Result<u64>> + Send;

    /// Renames a file or directory, replacing `to` if it exists. See [`std::fs::rename`].
    fn rename(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Creates a hard link `link` pointing at `original`. See [`std::fs::hard_link`].
    fn hard_link(&self, original: impl AsRef<Path>, link: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Removes a file. See [`std::fs::remove_file`].
    fn remove_file(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Creates a directory. Its parent must exist. See [`std::fs::create_dir`].
    fn create_dir(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Creates a directory and any missing parents. See [`std::fs::create_dir_all`].
    fn create_dir_all(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Removes an empty directory. See [`std::fs::remove_dir`].
    fn remove_dir(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Removes a directory and everything in it. See [`std::fs::remove_dir_all`].
    fn remove_dir_all(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send;

    /// Lists a directory. See [`std::fs::read_dir`].
    fn read_dir(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::ReadDir>> + Send;

    /// Metadata, following symbolic links. See [`std::fs::metadata`].
    fn metadata(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Metadata>> + Send;

    /// Metadata of the path itself, without following a symbolic link. See [`std::fs::symlink_metadata`].
    fn symlink_metadata(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Metadata>> + Send;

    /// Sets the permissions of a file or directory. See [`std::fs::set_permissions`].
    fn set_permissions(&self, path: impl AsRef<Path>, permissions: Permissions) -> impl Future<Output = io::Result<()>> + Send;

    /// The absolute path with every symbolic link resolved. See [`std::fs::canonicalize`].
    fn canonicalize(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<PathBuf>> + Send;

    /// The target of a symbolic link. See [`std::fs::read_link`].
    fn read_link(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<PathBuf>> + Send;

    /// Whether the path exists, following symbolic links. A broken link is `Ok(false)`; an error such as `PermissionDenied`
    /// is returned rather than guessed. See [`std::fs::exists`].
    fn exists(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<bool>> + Send;
}

/// An open file, from [`GatotkacaFs`]. See the [module docs](self) for buffers, cancellation and cursor versus positional I/O.
///
/// Dropping it closes the file. An implementation may close in the background, so errors on close are not reported; call
/// [`sync_all`](Self::sync_all) first when they matter.
pub trait GatotkacaFile: Send + Sync + 'static + Sized {
    /// Reads into `buf`'s spare capacity at `offset`, returning the count read; `0` at end of file. See
    /// [`std::os::unix::fs::FileExt::read_at`].
    fn read_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send;

    /// Fills `buf`'s spare capacity from `offset`. End of file first is `UnexpectedEof`, and the buffer's contents past its
    /// original length are then unspecified. See [`std::os::unix::fs::FileExt::read_exact_at`].
    fn read_exact_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send;

    /// Writes `buf[..]` at `offset`, returning the count written. See [`std::os::unix::fs::FileExt::write_at`].
    fn write_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send;

    /// Writes all of `buf` at `offset`. See [`std::os::unix::fs::FileExt::write_all_at`].
    fn write_all_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send;

    /// Reads into `buf`'s spare capacity at the cursor and advances it, returning the count read; `0` at end of file. See
    /// [`std::io::Read::read`].
    fn read(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send;

    /// Fills `buf`'s spare capacity from the cursor. End of file first is `UnexpectedEof`. See
    /// [`std::io::Read::read_exact`].
    fn read_exact(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send;

    /// Appends everything from the cursor to end of file to `buf`, growing it as needed, and returns the count read. See
    /// [`std::io::Read::read_to_end`].
    fn read_to_end(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send;

    /// Writes `buf[..]` at the cursor and advances it, returning the count written. See [`std::io::Write::write`].
    fn write(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send;

    /// Writes all of `buf` at the cursor. See [`std::io::Write::write_all`].
    fn write_all(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send;

    /// Pushes any writes the implementation buffers down to the OS. Does not sync to disk; see [`sync_all`](Self::sync_all).
    /// See [`std::io::Write::flush`].
    fn flush(&mut self) -> impl Future<Output = io::Result<()>> + Send;

    /// Moves the cursor, returning the new position from the start. See [`std::io::Seek::seek`].
    fn seek(&mut self, position: SeekFrom) -> impl Future<Output = io::Result<u64>> + Send;

    /// The cursor's position from the start. See [`std::io::Seek::stream_position`].
    fn stream_position(&mut self) -> impl Future<Output = io::Result<u64>> + Send {
        self.seek(SeekFrom::Current(0))
    }

    /// Moves the cursor to the start. See [`std::io::Seek::rewind`].
    fn rewind(&mut self) -> impl Future<Output = io::Result<()>> + Send {
        let seek = self.seek(SeekFrom::Start(0));
        async move { seek.await.map(drop) }
    }

    /// Flushes data and metadata to disk. See [`std::fs::File::sync_all`].
    fn sync_all(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Flushes data to disk, and metadata only where needed to read it back. See [`std::fs::File::sync_data`].
    fn sync_data(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Truncates or extends the file to `size` bytes; the cursor does not move. See [`std::fs::File::set_len`].
    fn set_len(&self, size: u64) -> impl Future<Output = io::Result<()>> + Send;

    /// The file's metadata. See [`std::fs::File::metadata`].
    fn metadata(&self) -> impl Future<Output = io::Result<Metadata>> + Send;

    /// Sets the file's permissions. See [`std::fs::File::set_permissions`].
    fn set_permissions(&self, permissions: Permissions) -> impl Future<Output = io::Result<()>> + Send;

    /// Sets the file's timestamps. See [`std::fs::File::set_times`].
    fn set_times(&self, times: FileTimes) -> impl Future<Output = io::Result<()>> + Send;

    /// Sets the file's modification time. See [`std::fs::File::set_modified`].
    fn set_modified(&self, time: SystemTime) -> impl Future<Output = io::Result<()>> + Send {
        self.set_times(FileTimes::new().set_modified(time))
    }

    /// A second handle to the same open file. As with [`std::fs::File::try_clone`], both share one cursor and one lock.
    fn try_clone(&self) -> impl Future<Output = io::Result<Self>> + Send;

    /// Takes an exclusive advisory lock, waiting until it is free. See [`std::fs::File::lock`].
    ///
    /// The wait does not hold up other tasks. Locks belong to the open file: [`try_clone`](Self::try_clone) shares one, a
    /// second [`open`](GatotkacaFs::open) of the same path does not.
    fn lock(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Takes a shared advisory lock, waiting until no exclusive lock is held. See [`std::fs::File::lock_shared`].
    fn lock_shared(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Takes an exclusive advisory lock if it is free, or returns [`TryLockError::WouldBlock`]. See
    /// [`std::fs::File::try_lock`].
    fn try_lock(&self) -> impl Future<Output = Result<(), TryLockError>> + Send;

    /// Takes a shared advisory lock if no exclusive lock is held, or returns [`TryLockError::WouldBlock`]. See
    /// [`std::fs::File::try_lock_shared`].
    fn try_lock_shared(&self) -> impl Future<Output = Result<(), TryLockError>> + Send;

    /// Releases this file's lock. Closing the file releases it too. See [`std::fs::File::unlock`].
    fn unlock(&self) -> impl Future<Output = io::Result<()>> + Send;
}

/// The entries of a directory, from [`GatotkacaFs::read_dir`]. See [`std::fs::ReadDir`].
///
/// Entries come in no particular order and never include `.` or `..`. Entries added or removed while listing may or may not
/// appear.
pub trait GatotkacaReadDir: Send + 'static {
    /// One entry.
    type DirEntry: GatotkacaDirEntry;

    /// The next entry, or `None` once the listing is done.
    ///
    /// Cancel-safe: dropping the future before it resolves loses no entry.
    fn next_entry(&mut self) -> impl Future<Output = io::Result<Option<Self::DirEntry>>> + Send;
}

/// One entry of a directory. See [`std::fs::DirEntry`].
pub trait GatotkacaDirEntry: Send + Sync + 'static {
    /// The full path: the listed directory joined with [`file_name`](Self::file_name).
    fn path(&self) -> PathBuf;

    /// The entry's name, without any leading path.
    fn file_name(&self) -> OsString;

    /// The entry's metadata, not following a symbolic link. See [`std::fs::DirEntry::metadata`].
    fn metadata(&self) -> impl Future<Output = io::Result<Metadata>> + Send;

    /// The entry's type, not following a symbolic link. Often free, from the listing itself. See
    /// [`std::fs::DirEntry::file_type`].
    fn file_type(&self) -> impl Future<Output = io::Result<FileType>> + Send;
}

/// How to open a file, for [`GatotkacaFs::open_with`]. Mirrors [`std::fs::OpenOptions`].
///
/// A plain value: the builder methods take and return it, and implementations read the fields. Everything starts off.
///
/// ```
/// use adaptif_gatotkaca_runtime::fs::OpenOptions;
///
/// let log = OpenOptions::new().append(true).create(true);
/// assert!(log.append && log.create && !log.truncate);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenOptions {
    /// Open for reading.
    pub read: bool,
    /// Open for writing.
    pub write: bool,
    /// Write at the end of the file, whatever the cursor. Implies `write`.
    pub append: bool,
    /// Truncate an existing file to zero length. Needs `write`.
    pub truncate: bool,
    /// Create the file if it does not exist. Needs `write` or `append`.
    pub create: bool,
    /// Create the file, failing with `AlreadyExists` if it exists. Overrides `create` and `truncate`. Needs `write` or
    /// `append`.
    pub create_new: bool,
    /// Permission bits for a created file, before the umask. `None` means `0o666`.
    #[cfg(unix)]
    pub mode: Option<u32>,
    /// Extra `open(2)` flags, or'ed in. `0` means none.
    #[cfg(unix)]
    pub custom_flags: i32,
}

impl OpenOptions {
    /// Every option off.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets [`read`](Self::read).
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Sets [`write`](Self::write).
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Sets [`append`](Self::append).
    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Sets [`truncate`](Self::truncate).
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Sets [`create`](Self::create).
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Sets [`create_new`](Self::create_new).
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Sets [`mode`](Self::mode).
    #[cfg(unix)]
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Sets [`custom_flags`](Self::custom_flags).
    #[cfg(unix)]
    pub fn custom_flags(mut self, flags: i32) -> Self {
        self.custom_flags = flags;
        self
    }

    /// The equivalent [`std::fs::OpenOptions`], for implementations built on std.
    pub fn to_std(&self) -> std::fs::OpenOptions {
        let mut options = std::fs::OpenOptions::new();
        options
            .read(self.read)
            .write(self.write)
            .append(self.append)
            .truncate(self.truncate)
            .create(self.create)
            .create_new(self.create_new);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            if let Some(mode) = self.mode {
                options.mode(mode);
            }
            options.custom_flags(self.custom_flags);
        }
        options
    }
}
