use std::io;

use betelgeuse::{IOSocket, RecvCompletion, SendCompletion, slab::SlabEntry};

pub const READ_CHUNK: usize = 8192;

enum ConnectionState {
    Free { next: Option<usize> },
    Open { socket: Box<dyn IOSocket> },
}

pub struct Connection {
    state: ConnectionState,
    pub write_buf: Vec<u8>,
    pub write_offset: usize,
    recv_completion: RecvCompletion,
    send_completion: SendCompletion,
}

impl Connection {
    pub fn open(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ConnectionState::Open { socket };
        self.write_buf.clear();
        self.write_offset = 0;
        self.recv_completion = RecvCompletion::new();
        self.send_completion = SendCompletion::new();
        self.arm_recv()
    }

    pub fn recv_result_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Open { .. }) && self.recv_completion.has_result()
    }

    pub fn send_result_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Open { .. }) && self.send_completion.has_result()
    }

    pub fn take_recv_result(&mut self) -> Option<io::Result<Vec<u8>>> {
        self.recv_completion.take_result()
    }

    pub fn take_send_result(&mut self) -> Option<io::Result<usize>> {
        self.send_completion.take_result()
    }

    pub fn arm_recv(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket } = &self.state else {
            panic!("connection slot requires open socket");
        };
        socket.recv(&mut self.recv_completion, READ_CHUNK)
    }

    pub fn arm_send(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket } = &self.state else {
            panic!("connection slot requires open socket");
        };
        socket.send(
            &mut self.send_completion,
            self.write_buf[self.write_offset..].to_vec(),
        )
    }
}

impl SlabEntry for Connection {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ConnectionState::Free { next },
            write_buf: Vec::with_capacity(READ_CHUNK),
            write_offset: 0,
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
        if let ConnectionState::Open { socket } =
            std::mem::replace(&mut self.state, ConnectionState::Free { next })
        {
            socket.close();
        }
        self.write_buf.clear();
        self.write_offset = 0;
        self.recv_completion = RecvCompletion::new();
        self.send_completion = SendCompletion::new();
    }
}
