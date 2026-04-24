//! I/O subsystem interfaces.
//!
//! This module defines the abstract interfaces presented by the I/O subsystem:
//! - [`IOLoop`] advances a concrete backend and retires completed work
//! - [`IO`] submits new operations and allocates backend objects
//! - [`IOFile`] and [`IOSocket`] expose file and socket capabilities
//! - [`Completion`] provides the caller-owned record for one in-flight operation
//!
//! The subsystem is completion-based. A caller prepares and owns a
//! [`Completion`] object, passes it to an operation such as [`IO::mkdir`] or
//! [`IOSocket::recv`], and later observes a terminal [`CompletionResult`] in
//! that same object. The backend is responsible for translating abstract
//! operations into concrete kernel work, advancing them to completion, and
//! recording the semantic result.
//!
//! Separating [`IOLoop`] from [`IO`] preserves a clear distinction between
//! driving the backend and issuing work into it. A single implementation may
//! provide both interfaces, but the contracts remain conceptually distinct:
//! one interface progresses the subsystem, the other requests service from it.

#![feature(allocator_api)]
#![feature(coroutine_trait)]
#![feature(coroutines)]
#![feature(stmt_expr_attributes)]

use std::{alloc::Allocator, io as stdio, net::SocketAddr, path::Path, rc::Rc};

pub mod completion;
pub mod io;
pub mod op;
pub mod slab;
pub mod task;

pub use completion::{Completion, CompletionCallback, CompletionContext, CompletionResult};

pub(crate) use completion::{
    AcceptOp, FsyncOp, MkdirOp, Operation, PReadOp, PWriteOp, RecvOp, SendOp, SizeOp,
};

/// Drives a concrete I/O backend forward.
///
/// An implementation is expected to:
/// - submit queued work to the kernel as needed
/// - harvest completed operations
/// - complete the caller-owned [`Completion`] objects associated with them
///
/// The return value indicates whether this tick observed or produced useful
/// work. Callers may use that signal for blocking, idling, or simulation logic.
pub trait IOLoop: IO {
    /// Advances the backend by one iteration.
    ///
    /// Returns `Ok(true)` when the backend submitted or completed at least one
    /// unit of work during this step, and `Ok(false)` when nothing progressed.
    fn step(&self) -> stdio::Result<bool>;
}

/// Submits file-system and socket operations to the backend.
///
/// This interface creates backend objects and arms caller-owned
/// [`Completion`] slots. The backend owns syscall translation, retry policy,
/// and kernel-specific details; callers own completion lifetimes and interpret
/// semantic [`CompletionResult`] values.
pub trait IO {
    /// Opens a file with the requested [`OpenOptions`].
    fn open(&self, path: &Path, options: OpenOptions) -> stdio::Result<Box<dyn IOFile>>;

    /// Creates a new socket object owned by the caller.
    fn socket(&self) -> stdio::Result<Box<dyn IOSocket>>;

    /// Submits a single-directory creation operation.
    fn mkdir(&self, c: &mut Completion, path: &Path, mode: u32) -> stdio::Result<()>;

    /// Returns a short backend name for logging and diagnostics.
    fn backend_name(&self) -> &'static str;
}

/// Open flags for [`IO::open`].
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    /// Open the file for reading.
    pub read: bool,
    /// Open the file for writing.
    pub write: bool,
    /// Create the file if it does not already exist.
    pub create: bool,
    /// Truncate the file to zero length on open.
    pub truncate: bool,
}

/// Backend-agnostic file operations.
///
/// A file object is a handle that submits work into caller-owned
/// [`Completion`] slots. The backend later completes those same slots with
/// semantic results such as [`CompletionResult::PRead`] or
/// [`CompletionResult::Fsync`].
pub trait IOFile {
    /// Reads up to `len` bytes starting at `offset`.
    fn pread(&self, c: &mut Completion, len: usize, offset: u64) -> stdio::Result<()>;

    /// Writes `buf` starting at `offset`.
    fn pwrite(&self, c: &mut Completion, buf: Vec<u8>, offset: u64) -> stdio::Result<()>;

    /// Flushes file data to stable storage.
    fn fsync(&self, c: &mut Completion) -> stdio::Result<()>;

    /// Reads the current file size.
    fn size(&self, c: &mut Completion) -> stdio::Result<()>;
}

/// Backend-agnostic socket operations.
///
/// A socket object follows the same caller-owned completion model as files.
/// Distinct completion slots may be used concurrently for independent socket
/// operations, subject to the rules imposed by the concrete backend.
pub trait IOSocket {
    /// Binds the socket to `addr`.
    fn bind(&self, addr: SocketAddr) -> stdio::Result<()>;

    /// Accepts one inbound connection on a listening socket.
    fn accept(&self, c: &mut Completion) -> stdio::Result<()>;

    /// Receives up to `len` bytes from a connected socket.
    fn recv(&self, c: &mut Completion, len: usize) -> stdio::Result<()>;

    /// Sends the contents of `buf` on a connected socket.
    fn send(&self, c: &mut Completion, buf: Vec<u8>) -> stdio::Result<()>;

    /// Enables or disables the `TCP_NODELAY` socket option.
    ///
    /// Must be called on a connected stream socket.
    fn set_nodelay(&self, on: bool) -> stdio::Result<()>;

    /// Closes the socket and releases any backend resources it owns.
    fn close(&self);
}

/// Shared handle that drives a concrete I/O backend.
#[derive(Clone)]
pub struct IOLoopHandle<A> {
    inner: Rc<dyn IOLoop>,
    _allocator: A,
}

impl<A> IOLoopHandle<A> {
    pub fn new(inner: Rc<dyn IOLoop>, allocator: A) -> Self {
        Self {
            inner,
            _allocator: allocator,
        }
    }

    pub fn io(&self) -> IOHandle {
        IOHandle {
            io_loop: self.inner.clone(),
        }
    }
}

impl<A> From<(Rc<dyn IOLoop>, A)> for IOLoopHandle<A> {
    fn from((inner, allocator): (Rc<dyn IOLoop>, A)) -> Self {
        Self::new(inner, allocator)
    }
}

impl<A> IO for IOLoopHandle<A> {
    fn open(&self, path: &Path, options: OpenOptions) -> stdio::Result<Box<dyn IOFile>> {
        self.inner.open(path, options)
    }

    fn socket(&self) -> stdio::Result<Box<dyn IOSocket>> {
        self.inner.socket()
    }

    fn mkdir(&self, c: &mut Completion, path: &Path, mode: u32) -> stdio::Result<()> {
        self.inner.mkdir(c, path, mode)
    }

    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }
}

impl<A> IOLoop for IOLoopHandle<A> {
    fn step(&self) -> stdio::Result<bool> {
        self.inner.step()
    }
}

/// Creates the native backend for the current target OS as a shared loop-capable handle.
pub fn io_loop<A: Allocator + Clone>(allocator: A) -> stdio::Result<IOLoopHandle<A>> {
    #[cfg(target_os = "linux")]
    {
        let inner: Rc<dyn IOLoop> = Rc::new(io::linux::IoUringIO::new()?);
        return Ok(IOLoopHandle::new(inner, allocator));
    }
    #[cfg(target_os = "macos")]
    {
        let inner: Rc<dyn IOLoop> = Rc::new(io::darwin::DarwinIO::new()?);
        return Ok(IOLoopHandle::new(inner, allocator));
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = allocator;
        Err(stdio::Error::new(
            stdio::ErrorKind::Unsupported,
            format!(
                "betelgeuse is supported on Linux and macOS only (got {})",
                std::env::consts::OS
            ),
        ))
    }
}

impl<A> From<IOLoopHandle<A>> for IOHandle {
    fn from(io_loop: IOLoopHandle<A>) -> Self {
        Self {
            io_loop: io_loop.inner,
        }
    }
}

#[derive(Clone)]
pub struct IOHandle {
    io_loop: Rc<dyn IOLoop>,
}

impl IOHandle {
    pub fn io_loop(&self) -> Rc<dyn IOLoop> {
        self.io_loop.clone()
    }
}

impl IO for IOHandle {
    fn open(&self, path: &Path, options: OpenOptions) -> stdio::Result<Box<dyn IOFile>> {
        self.io_loop.open(path, options)
    }

    fn socket(&self) -> stdio::Result<Box<dyn IOSocket>> {
        self.io_loop.socket()
    }

    fn mkdir(&self, c: &mut Completion, path: &Path, mode: u32) -> stdio::Result<()> {
        self.io_loop.mkdir(c, path, mode)
    }

    fn backend_name(&self) -> &'static str {
        self.io_loop.backend_name()
    }
}
