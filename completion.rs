//! Completion-based IO primitives shared by all backends.
//!
//! There is one concrete completion type per operation kind:
//! [`AcceptCompletion`], [`RecvCompletion`], [`SendCompletion`],
//! [`PReadCompletion`], [`PWriteCompletion`], [`FsyncCompletion`],
//! [`SizeCompletion`], [`MkdirCompletion`]. Each is a thin wrapper around
//! [`CompletionInner`] (the shared state machine and operation slot) and
//! carries its own typed result.
//!
//! The caller passes `&mut RecvCompletion` to [`IOSocket::recv`] (and so on),
//! the backend records an [`Operation`] in the underlying slot, submits the
//! work, and later finishes the same object with a typed result.
//!
//! ## Backend dispatch
//!
//! Every typed completion is `#[repr(C)]` with `inner: CompletionInner` as
//! the first field, so a `*mut CompletionInner` can be cast back to the
//! matching typed pointer. Backends use this to dispatch completion: they
//! match on the [`Operation`] variant they armed (e.g. `Operation::Recv`),
//! cast the inner pointer to the corresponding typed completion (e.g.
//! `*mut RecvCompletion`), and write the typed result directly. The
//! Operation variant and the typed completion are kept consistent because
//! the IO method that armed the completion is the only place that calls
//! [`CompletionInner::prepare`].
//!
//! [`IOSocket::recv`]: super::IOSocket::recv

use std::{ffi::CString, io, os::fd::RawFd};

use super::IOSocket;

/// Defines a completion type backed by [`CompletionInner`] that stores a result.
///
/// This macro generates a `#[repr(C)]` struct with a fixed layout and a standard
/// set of methods for managing completion state and retrieving results.
///
/// # Syntax
///
/// ```text
/// define_completion!(
///     $(#[$meta])*
///     $vis struct Name => ResultType
/// );
/// ```
macro_rules! define_completion {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident => $result_ty:ty
    ) => {
        $(#[$meta])*
        #[repr(C)]
        $vis struct $name {
            inner: CompletionInner,
            result: Option<$result_ty>,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    inner: CompletionInner::new(),
                    result: None,
                }
            }

            pub fn state(&self) -> CompletionState {
                self.inner.state()
            }

            pub fn is_idle(&self) -> bool {
                self.inner.is_idle()
            }

            pub fn has_result(&self) -> bool {
                self.result.is_some()
            }

            pub fn take_result(&mut self) -> Option<$result_ty> {
                let result = self.result.take()?;
                self.inner.reset();
                Some(result)
            }

            pub fn inner_mut(&mut self) -> &mut CompletionInner {
                &mut self.inner
            }

            /// Reconstitutes the typed completion from a pointer to its inner slot.
            ///
            /// # Safety
            ///
            /// `inner` must be the first field of a live instance of this typed
            /// completion.
            pub unsafe fn from_inner_mut(inner: &mut CompletionInner) -> &mut Self {
                unsafe { &mut *(inner as *mut CompletionInner as *mut Self) }
            }

            pub fn complete(&mut self, result: $result_ty) {
                self.inner.mark_completed();
                self.result = Some(result);
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

define_completion!(
    /// Completion slot for an `accept(2)` operation. Yields the accepted socket.
    pub struct AcceptCompletion => io::Result<Box<dyn IOSocket>>
);

define_completion!(
    /// Completion slot for a `recv(2)`-style operation. Yields the bytes read.
    pub struct RecvCompletion => io::Result<Vec<u8>>
);

define_completion!(
    /// Completion slot for a `send(2)`-style operation. Yields the byte count sent.
    pub struct SendCompletion => io::Result<usize>
);

define_completion!(
    /// Completion slot for a positional file read. Yields the bytes read.
    pub struct PReadCompletion => io::Result<Vec<u8>>
);

define_completion!(
    /// Completion slot for a positional file write. Yields the byte count written.
    pub struct PWriteCompletion => io::Result<usize>
);

define_completion!(
    /// Completion slot for an `fsync(2)` operation.
    pub struct FsyncCompletion => io::Result<()>
);

define_completion!(
    /// Completion slot for a file size query. Yields the size in bytes.
    pub struct SizeCompletion => io::Result<u64>
);

define_completion!(
    /// Completion slot for a `mkdir(2)` operation.
    pub struct MkdirCompletion => io::Result<()>
);

/// Type-erased completion slot embedded in every typed completion.
///
/// Holds the lifecycle state and the currently-armed [`Operation`]. The
/// typed result lives in the wrapping struct, written by the backend after
/// it casts `*mut CompletionInner` to the matching typed pointer.
pub struct CompletionInner {
    state: CompletionState,
    pub(crate) op: Operation,
}

impl CompletionInner {
    fn new() -> Self {
        Self {
            state: CompletionState::Idle,
            op: Operation::Nop,
        }
    }

    pub fn state(&self) -> CompletionState {
        self.state
    }

    pub fn is_idle(&self) -> bool {
        self.state == CompletionState::Idle
    }

    /// Arms the slot with a new operation.
    pub fn prepare(&mut self, op: Operation) {
        assert!(self.is_idle(), "completion is already in flight");
        self.state = CompletionState::Queued;
        self.op = op;
    }

    /// Returns the currently armed operation.
    pub fn operation(&self) -> &Operation {
        &self.op
    }

    /// Returns the currently armed operation mutably.
    pub fn operation_mut(&mut self) -> &mut Operation {
        &mut self.op
    }

    /// Marks the slot as submitted to the backend.
    pub fn mark_submitted(&mut self) {
        assert!(
            self.state == CompletionState::Queued,
            "completion must be queued before submission",
        );
        self.state = CompletionState::Submitted;
    }

    /// Moves a submitted slot back to the queued state for retry.
    pub fn mark_queued(&mut self) {
        assert!(
            self.state == CompletionState::Submitted,
            "completion must be submitted before it can be re-queued",
        );
        self.state = CompletionState::Queued;
    }

    /// Marks the slot as completed. Called by typed wrappers after they
    /// store the result. Clears the armed operation so the slot can be
    /// reused after `take_result`.
    pub fn mark_completed(&mut self) {
        assert!(
            matches!(
                self.state,
                CompletionState::Queued | CompletionState::Submitted
            ),
            "completion must be in flight before it can complete",
        );
        self.state = CompletionState::Completed;
    }

    /// Resets a completed slot back to idle. Called by typed wrappers'
    /// `take_result` after extracting the typed value.
    fn reset(&mut self) {
        self.state = CompletionState::Idle;
        self.op = Operation::Nop;
    }
}

/// Lifecycle states for a completion slot.
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

/// The operation currently armed in a completion slot.
///
/// Backends translate this enum into concrete syscalls or `io_uring`
/// submissions and later use it to dispatch completion to the matching
/// typed wrapper.
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
    use super::{CompletionInner, CompletionState, Operation};

    #[test]
    fn submitted_completion_can_be_requeued_for_retry() {
        let mut completion = CompletionInner::new();
        completion.prepare(Operation::Nop);
        assert_eq!(completion.state(), CompletionState::Queued);

        completion.mark_submitted();
        assert_eq!(completion.state(), CompletionState::Submitted);

        completion.mark_queued();
        assert_eq!(completion.state(), CompletionState::Queued);
    }
}
