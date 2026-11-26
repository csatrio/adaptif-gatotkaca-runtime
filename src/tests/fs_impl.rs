//! A filesystem implementation on `std::fs`, run through an [`Offload`] strategy, to prove the `fs` traits fit both models:
//!
//! - [`TokioFs`]: every call goes to the tokio reference runtime's blocking pool, the way `tokio::fs` works. Shutdown tracks
//!   the calls, and the polling worker is never blocked.
//! - [`InlineFs`]: every call runs on the polling thread when first polled. The shape a plain std implementation takes.
//!
//! A completion-based implementation (monoio, io_uring) would instead submit each owned buffer to the kernel.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::{File, FileTimes, FileType, Metadata, Permissions, TryLockError};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::tokio_impl::TokioHandle;
use crate::fs::{GatotkacaDirEntry, GatotkacaFile, GatotkacaFs, GatotkacaReadDir, OpenOptions};
use crate::runtime::GatotkacaRuntimeHandle;

/// Where a blocking std call runs.
pub trait Offload: Clone + Send + Sync + 'static {
    /// Runs `op` when the returned future is first polled. The error is the strategy's own failure (its runtime is gone),
    /// never `op`'s.
    fn run<T, F>(&self, op: F) -> impl Future<Output = io::Result<T>> + Send + 'static
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;
}

/// Runs each call on the polling thread.
#[derive(Clone, Copy)]
pub struct Inline;

impl Offload for Inline {
    // `async fn` would borrow `self`, and the trait promises a `'static` future.
    #[allow(clippy::manual_async_fn)]
    fn run<T, F>(&self, op: F) -> impl Future<Output = io::Result<T>> + Send + 'static
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        async move { Ok(op()) }
    }
}

impl Offload for TokioHandle {
    fn run<T, F>(&self, op: F) -> impl Future<Output = io::Result<T>> + Send + 'static
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let handle = self.clone();
        async move {
            match GatotkacaRuntimeHandle::spawn_blocking(&handle, op).await {
                Ok(output) => Ok(output),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(error) => Err(io::Error::other(error)),
            }
        }
    }
}

/// `std::fs` through an [`Offload`] strategy.
#[derive(Clone)]
pub struct StdFs<X>(X);

/// `std::fs` on the tokio reference runtime's blocking pool.
pub type TokioFs = StdFs<TokioHandle>;

/// `std::fs` on the polling thread.
pub type InlineFs = StdFs<Inline>;

impl<X: Offload> StdFs<X> {
    /// A filesystem running its calls through `offload`.
    pub fn new(offload: X) -> Self {
        Self(offload)
    }

    fn call<T: Send + 'static>(
        &self,
        op: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> impl Future<Output = io::Result<T>> + Send + 'static {
        let run = self.0.run(op);
        async move { run.await? }
    }
}

fn owned(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_owned()
}

impl<X: Offload> GatotkacaFs for StdFs<X> {
    type File = StdFile<X>;
    type ReadDir = StdReadDir<X>;
    type DirEntry = StdDirEntry<X>;

    fn open_with(&self, path: impl AsRef<Path>, options: OpenOptions) -> impl Future<Output = io::Result<StdFile<X>>> + Send {
        let path = owned(path);
        let offload = self.0.clone();
        let open = self.call(move || options.to_std().open(path));
        async move { Ok(StdFile { file: Arc::new(open.await?), offload }) }
    }

    fn read(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Vec<u8>>> + Send {
        let path = owned(path);
        self.call(move || std::fs::read(path))
    }

    fn read_to_string(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<String>> + Send {
        let path = owned(path);
        self.call(move || std::fs::read_to_string(path))
    }

    fn write(&self, path: impl AsRef<Path>, contents: impl AsRef<[u8]> + Send + 'static) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::write(path, contents))
    }

    fn copy(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> impl Future<Output = io::Result<u64>> + Send {
        let (from, to) = (owned(from), owned(to));
        self.call(move || std::fs::copy(from, to))
    }

    fn rename(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let (from, to) = (owned(from), owned(to));
        self.call(move || std::fs::rename(from, to))
    }

    fn hard_link(&self, original: impl AsRef<Path>, link: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let (original, link) = (owned(original), owned(link));
        self.call(move || std::fs::hard_link(original, link))
    }

    fn remove_file(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::remove_file(path))
    }

    fn create_dir(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::create_dir(path))
    }

    fn create_dir_all(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::create_dir_all(path))
    }

    fn remove_dir(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::remove_dir(path))
    }

    fn remove_dir_all(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::remove_dir_all(path))
    }

    fn read_dir(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<StdReadDir<X>>> + Send {
        let path = owned(path);
        let offload = self.0.clone();
        let open = self.call(move || std::fs::read_dir(path));
        async move {
            let state = ListState { entries: open.await?, ready: VecDeque::new() };
            Ok(StdReadDir { state: Arc::new(Mutex::new(state)), offload })
        }
    }

    fn metadata(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Metadata>> + Send {
        let path = owned(path);
        self.call(move || std::fs::metadata(path))
    }

    fn symlink_metadata(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Metadata>> + Send {
        let path = owned(path);
        self.call(move || std::fs::symlink_metadata(path))
    }

    fn set_permissions(&self, path: impl AsRef<Path>, permissions: Permissions) -> impl Future<Output = io::Result<()>> + Send {
        let path = owned(path);
        self.call(move || std::fs::set_permissions(path, permissions))
    }

    fn canonicalize(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<PathBuf>> + Send {
        let path = owned(path);
        self.call(move || std::fs::canonicalize(path))
    }

    fn read_link(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<PathBuf>> + Send {
        let path = owned(path);
        self.call(move || std::fs::read_link(path))
    }

    fn exists(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<bool>> + Send {
        let path = owned(path);
        self.call(move || std::fs::exists(path))
    }
}

/// A std file shared with the calls in flight, so a cancelled call can finish on its own.
pub struct StdFile<X> {
    file: Arc<File>,
    offload: X,
}

impl<X: Offload> StdFile<X> {
    /// Runs `op` on the file.
    fn call<T: Send + 'static>(
        &self,
        op: impl FnOnce(&File) -> io::Result<T> + Send + 'static,
    ) -> impl Future<Output = io::Result<T>> + Send + 'static {
        let file = self.file.clone();
        let run = self.offload.run(move || op(&file));
        async move { run.await? }
    }

    /// Runs `op` on the file and an owned buffer, handing the buffer back. If the call cannot run, the buffer is lost.
    fn with_buf<T: Send + 'static>(
        &self,
        mut buf: Vec<u8>,
        op: impl FnOnce(&File, &mut Vec<u8>) -> io::Result<T> + Send + 'static,
    ) -> impl Future<Output = (io::Result<T>, Vec<u8>)> + Send + 'static {
        let file = self.file.clone();
        let run = self.offload.run(move || (op(&file, &mut buf), buf));
        async move { run.await.unwrap_or_else(|error| (Err(error), Vec::new())) }
    }

    fn try_lock_with(
        &self,
        op: impl FnOnce(&File) -> Result<(), TryLockError> + Send + 'static,
    ) -> impl Future<Output = Result<(), TryLockError>> + Send + 'static {
        let file = self.file.clone();
        let run = self.offload.run(move || op(&file));
        async move { run.await.map_err(TryLockError::Error)? }
    }
}

/// Reads into `buf`'s spare capacity, after its contents. Zeroes the spare first: safe, and fine for a reference.
fn read_spare(buf: &mut Vec<u8>, read: impl FnOnce(&mut [u8]) -> io::Result<usize>) -> io::Result<usize> {
    let start = buf.len();
    buf.resize(buf.capacity(), 0);
    let result = read(&mut buf[start..]);
    buf.truncate(start + result.as_ref().copied().unwrap_or(0));
    result
}

/// Fills `buf`'s spare capacity. On error the length goes back to where it was.
fn fill_spare(buf: &mut Vec<u8>, fill: impl FnOnce(&mut [u8]) -> io::Result<()>) -> io::Result<()> {
    let start = buf.len();
    buf.resize(buf.capacity(), 0);
    let result = fill(&mut buf[start..]);
    if result.is_err() {
        buf.truncate(start);
    }
    result
}

#[cfg(unix)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(unix)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

#[cfg(windows)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

fn pread_exact(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match pread(file, buf, offset) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")),
            Ok(read) => {
                buf = &mut std::mem::take(&mut buf)[read..];
                offset += read as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn pwrite_all(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match pwrite(file, buf, offset) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer")),
            Ok(written) => {
                buf = &buf[written..];
                offset += written as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

// Cursor calls use std's `Read`, `Write` and `Seek` for `&File`, so the OS keeps the cursor.
impl<X: Offload> GatotkacaFile for StdFile<X> {
    fn read_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send {
        self.with_buf(buf, move |file, buf| read_spare(buf, |spare| pread(file, spare, offset)))
    }

    fn read_exact_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send {
        self.with_buf(buf, move |file, buf| fill_spare(buf, |spare| pread_exact(file, spare, offset)))
    }

    fn write_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send {
        self.with_buf(buf, move |file, buf| pwrite(file, buf, offset))
    }

    fn write_all_at(&self, buf: Vec<u8>, offset: u64) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send {
        self.with_buf(buf, move |file, buf| pwrite_all(file, buf, offset))
    }

    fn read(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send {
        self.with_buf(buf, |mut file, buf| read_spare(buf, |spare| file.read(spare)))
    }

    fn read_exact(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send {
        self.with_buf(buf, |mut file, buf| fill_spare(buf, |spare| file.read_exact(spare)))
    }

    fn read_to_end(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send {
        self.with_buf(buf, |mut file, buf| file.read_to_end(buf))
    }

    fn write(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send {
        self.with_buf(buf, |mut file, buf| file.write(buf))
    }

    fn write_all(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<()>, Vec<u8>)> + Send {
        self.with_buf(buf, |mut file, buf| file.write_all(buf))
    }

    fn flush(&mut self) -> impl Future<Output = io::Result<()>> + Send {
        self.call(|mut file| file.flush())
    }

    fn seek(&mut self, position: SeekFrom) -> impl Future<Output = io::Result<u64>> + Send {
        self.call(move |mut file| file.seek(position))
    }

    fn sync_all(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.call(File::sync_all)
    }

    fn sync_data(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.call(File::sync_data)
    }

    fn set_len(&self, size: u64) -> impl Future<Output = io::Result<()>> + Send {
        self.call(move |file| file.set_len(size))
    }

    fn metadata(&self) -> impl Future<Output = io::Result<Metadata>> + Send {
        self.call(File::metadata)
    }

    fn set_permissions(&self, permissions: Permissions) -> impl Future<Output = io::Result<()>> + Send {
        self.call(move |file| file.set_permissions(permissions))
    }

    fn set_times(&self, times: FileTimes) -> impl Future<Output = io::Result<()>> + Send {
        self.call(move |file| file.set_times(times))
    }

    fn try_clone(&self) -> impl Future<Output = io::Result<Self>> + Send {
        let offload = self.offload.clone();
        let clone = self.call(File::try_clone);
        async move { Ok(StdFile { file: Arc::new(clone.await?), offload }) }
    }

    fn lock(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.call(File::lock)
    }

    fn lock_shared(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.call(File::lock_shared)
    }

    fn try_lock(&self) -> impl Future<Output = Result<(), TryLockError>> + Send {
        self.try_lock_with(File::try_lock)
    }

    fn try_lock_shared(&self) -> impl Future<Output = Result<(), TryLockError>> + Send {
        self.try_lock_with(File::try_lock_shared)
    }

    fn unlock(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.call(File::unlock)
    }
}

struct ListState {
    entries: std::fs::ReadDir,
    /// Entries already read but not yet handed out. A call parks its entry here, so a cancelled `next_entry` loses nothing.
    ready: VecDeque<io::Result<std::fs::DirEntry>>,
}

/// A std directory listing.
pub struct StdReadDir<X> {
    state: Arc<Mutex<ListState>>,
    offload: X,
}

impl<X: Offload> GatotkacaReadDir for StdReadDir<X> {
    type DirEntry = StdDirEntry<X>;

    fn next_entry(&mut self) -> impl Future<Output = io::Result<Option<StdDirEntry<X>>>> + Send {
        let state = self.state.clone();
        let offload = self.offload.clone();
        let fetch = {
            let state = self.state.clone();
            self.offload.run(move || {
                let mut state = state.lock().unwrap();
                if state.ready.is_empty()
                    && let Some(entry) = state.entries.next()
                {
                    state.ready.push_back(entry);
                }
            })
        };
        async move {
            fetch.await?;
            // Empty after a fetch means the listing is done.
            let next = state.lock().unwrap().ready.pop_front();
            next.transpose().map(|entry| entry.map(|entry| StdDirEntry { entry: Arc::new(entry), offload }))
        }
    }
}

/// A std directory entry.
pub struct StdDirEntry<X> {
    entry: Arc<std::fs::DirEntry>,
    offload: X,
}

impl<X: Offload> StdDirEntry<X> {
    fn call<T: Send + 'static>(
        &self,
        op: impl FnOnce(&std::fs::DirEntry) -> io::Result<T> + Send + 'static,
    ) -> impl Future<Output = io::Result<T>> + Send + 'static {
        let entry = self.entry.clone();
        let run = self.offload.run(move || op(&entry));
        async move { run.await? }
    }
}

impl<X: Offload> GatotkacaDirEntry for StdDirEntry<X> {
    fn path(&self) -> PathBuf {
        self.entry.path()
    }

    fn file_name(&self) -> OsString {
        self.entry.file_name()
    }

    fn metadata(&self) -> impl Future<Output = io::Result<Metadata>> + Send {
        self.call(std::fs::DirEntry::metadata)
    }

    fn file_type(&self) -> impl Future<Output = io::Result<FileType>> + Send {
        self.call(std::fs::DirEntry::file_type)
    }
}
