use std::io;

use betelgeuse::{IOSocket, RecvCompletion, SendCompletion, slab::SlabEntry};

pub const READ_CHUNK: usize = 8192;

/// Outcome of advancing a connection's state machine for one tick.
pub enum ConnectionStep {
    /// Slot is still alive — either nothing was ready or it advanced.
    Continue,
    /// The peer hung up or sent zero; the owner should release the slot.
    Close,
}

enum ConnectionState {
    Open {
        socket: Box<dyn IOSocket>,
        phase: Phase,
    },
    Free {
        next: Option<usize>,
    },
}

/// Which I/O direction the connection is currently waiting on.
enum Phase {
    Receiving,
    Sending { buf: Vec<u8>, offset: usize },
}

pub struct Connection {
    state: ConnectionState,
    recv_completion: RecvCompletion,
    send_completion: SendCompletion,
}

impl Connection {
    pub fn open(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ConnectionState::Open {
            socket,
            phase: Phase::Receiving,
        };
        self.recv_completion = RecvCompletion::new();
        self.send_completion = SendCompletion::new();
        self.arm_recv()
    }

    pub fn step(&mut self) -> io::Result<ConnectionStep> {
        match &self.state {
            ConnectionState::Open {
                phase: Phase::Receiving,
                ..
            } if self.recv_completion.has_result() => self.complete_recv(),
            ConnectionState::Open {
                phase: Phase::Sending { .. },
                ..
            } if self.send_completion.has_result() => self.complete_send(),
            _ => Ok(ConnectionStep::Continue),
        }
    }

    fn complete_recv(&mut self) -> io::Result<ConnectionStep> {
        let buf = self
            .recv_completion
            .take_result()
            .expect("step() guarantees a recv result is ready")?;
        if buf.is_empty() {
            return Ok(ConnectionStep::Close);
        }
        let ConnectionState::Open { phase, .. } = &mut self.state else {
            unreachable!("step() matched Open before dispatching");
        };
        *phase = Phase::Sending { buf, offset: 0 };
        self.arm_send()?;
        Ok(ConnectionStep::Continue)
    }

    fn complete_send(&mut self) -> io::Result<ConnectionStep> {
        let written = self
            .send_completion
            .take_result()
            .expect("step() guarantees a send result is ready")?;
        if written == 0 {
            return Ok(ConnectionStep::Close);
        }
        let ConnectionState::Open { phase, .. } = &mut self.state else {
            unreachable!("step() matched Open before dispatching");
        };
        let Phase::Sending { buf, offset } = phase else {
            unreachable!("step() dispatches complete_send only in Sending phase");
        };
        *offset += written;
        if *offset >= buf.len() {
            *phase = Phase::Receiving;
            self.arm_recv()
        } else {
            self.arm_send()
        }?;
        Ok(ConnectionStep::Continue)
    }

    fn arm_recv(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket, .. } = &self.state else {
            panic!("connection requires an open socket");
        };
        socket.recv(&mut self.recv_completion, READ_CHUNK)
    }

    fn arm_send(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket, phase } = &self.state else {
            panic!("connection requires an open socket");
        };
        let Phase::Sending { buf, offset } = phase else {
            panic!("arm_send requires the Sending phase");
        };
        socket.send(&mut self.send_completion, buf[*offset..].to_vec())
    }
}

impl SlabEntry for Connection {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ConnectionState::Free { next },
            recv_completion: RecvCompletion::new(),
            send_completion: SendCompletion::new(),
        }
    }

    fn is_free(&self) -> bool {
        matches!(self.state, ConnectionState::Free { .. })
    }

    fn next_free(&self) -> Option<usize> {
        match self.state {
            ConnectionState::Free { next } => next,
            ConnectionState::Open { .. } => None,
        }
    }

    fn release(&mut self, next: Option<usize>) {
        if let ConnectionState::Open { socket, .. } =
            std::mem::replace(&mut self.state, ConnectionState::Free { next })
        {
            socket.close();
        }
        self.recv_completion = RecvCompletion::new();
        self.send_completion = SendCompletion::new();
    }
}
