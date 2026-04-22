//! Completion-based IO primitives shared by all backends.
//!
//! A [`Completion`] is a caller-owned slot for one in-flight IO operation.
//! The caller passes `&mut Completion` to an IO method, the backend records an
//! [`Operation`] in that slot, submits the work, and later finishes the same
//! object with a semantic [`CompletionResult`].
//!
//! This gives the IO layer a simple shape:
//! - callers own lifecycle and reuse of completion slots
//! - backends own syscall submission, retry policy, and result decoding
//! - higher layers work with semantic operation results
//!
//! Long-lived state machines, such as listeners and connections, typically keep
//! persistent completion slots for their accept/recv/send operations. Synchronous
//! helper code, such as WAL bootstrap, can also allocate a completion on the
//! stack, submit one operation, and wait until it completes.

use std::{any::Any, ffi::CString, io::Result, os::fd::RawFd};

use super::IOSocket;

/// A caller-owned in-flight IO operation slot.
///
/// A `Completion` starts idle, is prepared with an [`Operation`], transitions
/// through submission, and eventually stores a semantic [`CompletionResult`].
/// The same object is then reset back to idle when the result is taken.
pub struct Completion {
    state: CompletionState,
    pub(crate) op: Operation,
    result: Option<Result<CompletionResult>>,
    callback: Option<CompletionCallback>,
}

impl Default for Completion {
    fn default() -> Self {
        Self::new()
    }
}

impl Completion {
    /// Creates a new idle completion slot.
    pub fn new() -> Self {
        Self {
            state: CompletionState::Idle,
            op: Operation::Nop,
            result: None,
            callback: None,
        }
    }

    /// Returns the current lifecycle state of the completion.
    pub fn state(&self) -> CompletionState {
        self.state
    }

    /// Returns `true` when the completion is available for a new operation.
    pub fn is_idle(&self) -> bool {
        self.state == CompletionState::Idle
    }

    /// Returns `true` when the completion holds a terminal result.
    pub fn has_result(&self) -> bool {
        self.result.is_some()
    }

    /// Takes the completed result and resets the slot back to idle.
    pub fn take_result(&mut self) -> Option<Result<CompletionResult>> {
        let result = self.result.take();
        if result.is_some() {
            self.state = CompletionState::Idle;
            self.op = Operation::Nop;
            self.callback = None;
        }
        result
    }

    /// Arms the completion with a new operation.
    pub fn prepare(&mut self, op: Operation) {
        assert!(self.is_idle(), "completion is already in flight");
        self.state = CompletionState::Queued;
        self.op = op;
        self.result = None;
    }

    /// Registers a callback to run when the completion reaches a terminal state.
    pub fn set_callback(&mut self, callback: CompletionCallback) {
        assert!(
            matches!(self.state, CompletionState::Idle | CompletionState::Queued),
            "completion callback must be set before completion finishes",
        );
        self.callback = Some(callback);
    }

    /// Registers a typed callback to run when the completion reaches a terminal
    /// state.
    pub fn on_complete<T: CompletionContext>(&mut self, ctx: T) {
        self.set_callback(CompletionCallback::new(ctx));
    }

    /// Returns the currently armed operation.
    pub fn operation(&self) -> &Operation {
        &self.op
    }

    /// Returns the currently armed operation mutably.
    pub fn operation_mut(&mut self) -> &mut Operation {
        &mut self.op
    }

    /// Marks the completion as submitted to the backend.
    pub fn mark_submitted(&mut self) {
        assert!(
            self.state == CompletionState::Queued,
            "completion must be queued before submission",
        );
        self.state = CompletionState::Submitted;
    }

    /// Moves a submitted completion back to the queued state for retry.
    pub fn mark_queued(&mut self) {
        assert!(
            self.state == CompletionState::Submitted,
            "completion must be submitted before it can be re-queued",
        );
        self.state = CompletionState::Queued;
    }

    /// Stores the terminal result and transitions the completion to completed.
    pub fn complete(&mut self, result: Result<CompletionResult>) {
        assert!(
            matches!(
                self.state,
                CompletionState::Queued | CompletionState::Submitted
            ),
            "completion must be in flight before it can complete",
        );
        self.state = CompletionState::Completed;
        self.result = Some(result);
        if let Some(mut callback) = self.callback.take() {
            callback.call(self);
        }
    }
}

/// Lifecycle states for a [`Completion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionState {
    /// The slot is free and may be prepared for a new operation.
    #[default]
    Idle,
    /// The slot has been prepared but not yet submitted.
    Queued,
    /// The operation has been submitted to the backend.
    Submitted,
    /// The backend has produced a terminal result.
    Completed,
}

/// Optional callback invoked when a completion reaches a terminal state.
///
/// The current codebase mainly consumes completions via `take_result()`, but the
/// callback slot allows the completion object to support more callback-driven
/// state machines without changing the core model.
pub struct CompletionCallback {
    ctx: Box<dyn Any>,
    func: fn(&mut dyn Any, &mut Completion),
}

impl CompletionCallback {
    /// Constructs a callback with typed context.
    pub fn new<T: CompletionContext>(ctx: T) -> Self {
        Self {
            ctx: Box::new(ctx),
            func: callback_trampoline::<T>,
        }
    }

    fn call(&mut self, c: &mut Completion) {
        (self.func)(self.ctx.as_mut(), c);
    }
}

/// Typed completion callback context.
pub trait CompletionContext: Any {
    /// Called when the associated completion reaches a terminal state.
    fn complete(&mut self, c: &mut Completion);
}

fn callback_trampoline<T: CompletionContext>(ctx: &mut dyn Any, c: &mut Completion) {
    let ctx = ctx
        .downcast_mut::<T>()
        .expect("completion callback context type mismatch");
    ctx.complete(c);
}

/// Semantic results produced by completed IO operations.
pub enum CompletionResult {
    /// A newly accepted connected socket.
    Accept(Box<dyn IOSocket>),
    /// Bytes read from a socket.
    Recv(Vec<u8>),
    /// Number of bytes written to a socket.
    Send(usize),
    /// Bytes read from a file using positional IO.
    PRead(Vec<u8>),
    /// Number of bytes written to a file using positional IO.
    PWrite(usize),
    /// Successful file data synchronization.
    Fsync,
    /// File size in bytes.
    Size(u64),
    /// Successful single-directory creation.
    Mkdir,
}

/// The operation currently armed in a [`Completion`].
///
/// Backends translate this enum into concrete syscalls or `io_uring`
/// submissions and later decode the kernel result back into a
/// [`CompletionResult`].
pub enum Operation {
    /// No operation is currently armed.
    Nop,
    /// Accept one connection from a listening socket.
    Accept(AcceptOp),
    /// Receive bytes from a connected socket.
    Recv(RecvOp),
    /// Send bytes to a connected socket.
    Send(SendOp),
    /// Read bytes from a file at a fixed offset.
    PRead(PReadOp),
    /// Write bytes to a file at a fixed offset.
    PWrite(PWriteOp),
    /// Flush file data to stable storage.
    Fsync(FsyncOp),
    /// Read file size metadata.
    Size(SizeOp),
    /// Create one directory.
    Mkdir(MkdirOp),
}

/// Payload for an `accept(2)` operation.
pub struct AcceptOp {
    pub fd: RawFd,
}

/// Payload for a `recv(2)`-style operation.
pub struct RecvOp {
    pub fd: RawFd,
    pub buf: Vec<u8>,
    pub flags: i32,
}

/// Payload for a `send(2)`-style operation.
pub struct SendOp {
    pub fd: RawFd,
    pub buf: Vec<u8>,
    pub flags: i32,
}

/// Payload for a positional file read operation.
pub struct PReadOp {
    pub fd: RawFd,
    pub buf: Vec<u8>,
    pub offset: u64,
}

/// Payload for a positional file write operation.
pub struct PWriteOp {
    pub fd: RawFd,
    pub buf: Vec<u8>,
    pub offset: u64,
}

/// Payload for an `fsync(2)` operation.
pub struct FsyncOp {
    pub fd: RawFd,
}

/// Payload for a file size query.
pub struct SizeOp {
    pub fd: RawFd,
}

/// Payload for a `mkdir(2)` operation.
pub struct MkdirOp {
    pub path: CString,
    pub mode: u32,
}

#[cfg(test)]
mod tests {
    use super::{Completion, CompletionState, Operation};

    #[test]
    fn submitted_completion_can_be_requeued_for_retry() {
        let mut completion = Completion::new();
        completion.prepare(Operation::Nop);
        assert_eq!(completion.state(), CompletionState::Queued);

        completion.mark_submitted();
        assert_eq!(completion.state(), CompletionState::Submitted);

        completion.mark_queued();
        assert_eq!(completion.state(), CompletionState::Queued);
    }
}
