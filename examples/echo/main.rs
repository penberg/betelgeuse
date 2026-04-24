//! TCP echo server built on Betelgeuse.
//!
//! The example shows the canonical "step-driven" server shape:
//!
//! - one fixed-capacity `Slab` of listener slots,
//! - one fixed-capacity `Slab` of connection slots,
//! - a `Server::step` that drains ready completions and advances each slot,
//! - the outer loop alternates `io.step()` (drive the backend) with
//!   `server.step()` (react to completions).
//!
//! Each slot type implements `SlabEntry`, so the slab owns its free list and
//! the server never allocates on the hot path.
//!
//! Run one of:
//!     cargo run --example echo
//!     BETELGEUSE_BACKEND=syscall cargo run --example echo
//!
//! Then in another terminal:
//!     nc 127.0.0.1 5555

#![feature(allocator_api)]

use std::{alloc::{Allocator, Global}, io, net::SocketAddr};

use betelgeuse::{
    Completion, CompletionResult, IO, IOBackend, IOHandle, IOLoop, IOSocket, io_loop,
    slab::{Slab, SlabEntry},
};

const ADDR: &str = "127.0.0.1:5555";
const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;
const READ_CHUNK: usize = 8192;

fn main() -> io::Result<()> {
    let backend = match std::env::var("BETELGEUSE_BACKEND").as_deref() {
        Ok("syscall") => IOBackend::Syscall,
        _ => IOBackend::IoUring,
    };
    let allocator = Global;
    let io_loop = io_loop(allocator, backend)?;
    let addr: SocketAddr = ADDR.parse().expect("valid address");

    let mut server = Server::new(allocator, io_loop.io());
    server.listen(addr)?;
    println!(
        "echo listening on {addr} (backend: {})",
        io_loop.backend_name()
    );

    loop {
        server.step()?;
        io_loop.step()?;
    }
}

struct Server<A: Allocator + Clone> {
    io: IOHandle,
    listeners: Slab<Listener, A>,
    connections: Slab<Connection, A>,
}

impl<A: Allocator + Clone> Server<A> {
    fn new(allocator: A, io: IOHandle) -> Self {
        Self {
            io,
            listeners: Slab::new(allocator.clone(), MAX_LISTENERS),
            connections: Slab::new(allocator, MAX_CONNECTIONS),
        }
    }

    fn listen(&mut self, addr: SocketAddr) -> io::Result<()> {
        let id = self
            .listeners
            .acquire()
            .ok_or_else(|| io::Error::other("listener pool exhausted"))?;
        let socket = self.io.socket()?;
        if let Err(err) = socket.bind(addr) {
            self.listeners.release(id);
            return Err(err);
        }
        self.listeners
            .entry_mut(id)
            .expect("just-acquired slot must exist")
            .activate(socket)
    }

    fn step(&mut self) -> io::Result<bool> {
        let mut progressed = false;

        for idx in 0..self.listeners.capacity() {
            if self
                .listeners
                .entry_mut(idx)
                .is_some_and(|s| s.accept_result_ready())
            {
                progressed = true;
                self.handle_accept(idx)?;
            }
        }

        for idx in 0..self.connections.capacity() {
            if self
                .connections
                .entry_mut(idx)
                .is_some_and(|s| s.recv_result_ready())
            {
                progressed = true;
                self.handle_recv(idx)?;
            }
            if self
                .connections
                .entry_mut(idx)
                .is_some_and(|s| s.send_result_ready())
            {
                progressed = true;
                self.handle_send(idx)?;
            }
        }

        Ok(progressed)
    }

    fn handle_accept(&mut self, idx: usize) -> io::Result<()> {
        let listener = self.listeners.entry_mut(idx).expect("valid slot");
        let result = listener
            .take_accept_result()
            .expect("accept step requires completion result");
        let socket = match result? {
            CompletionResult::Accept(socket) => socket,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "listener accept completion had wrong type",
                ));
            }
        };
        listener.arm_accept()?;
        self.insert_connection(socket)
    }

    fn insert_connection(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        let Some(id) = self.connections.acquire() else {
            eprintln!("connection pool exhausted, dropping accepted socket");
            socket.close();
            return Ok(());
        };
        if let Err(err) = self
            .connections
            .entry_mut(id)
            .expect("acquired slot")
            .open(socket)
        {
            self.connections.release(id);
            return Err(err);
        }
        Ok(())
    }

    fn handle_recv(&mut self, idx: usize) -> io::Result<()> {
        let conn = self.connections.entry_mut(idx).expect("valid slot");
        let result = conn
            .take_recv_result()
            .expect("recv step requires completion result");
        let buf = match result? {
            CompletionResult::Recv(buf) => buf,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "connection recv completion had wrong type",
                ));
            }
        };
        if buf.is_empty() {
            self.connections.release(idx);
            return Ok(());
        }
        conn.write_buf = buf;
        conn.write_offset = 0;
        conn.arm_send()
    }

    fn handle_send(&mut self, idx: usize) -> io::Result<()> {
        let conn = self.connections.entry_mut(idx).expect("valid slot");
        let result = conn
            .take_send_result()
            .expect("send step requires completion result");
        let written = match result? {
            CompletionResult::Send(n) => n,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "connection send completion had wrong type",
                ));
            }
        };
        if written == 0 {
            self.connections.release(idx);
            return Ok(());
        }
        conn.write_offset += written;
        if conn.write_offset < conn.write_buf.len() {
            conn.arm_send()
        } else {
            conn.write_buf.clear();
            conn.write_offset = 0;
            conn.arm_recv()
        }
    }
}

enum ListenerState {
    Free { next: Option<usize> },
    Active { socket: Box<dyn IOSocket> },
}

struct Listener {
    state: ListenerState,
    accept_completion: Completion,
}

impl Listener {
    fn activate(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ListenerState::Active { socket };
        self.accept_completion = Completion::new();
        self.arm_accept()
    }

    fn accept_result_ready(&self) -> bool {
        !matches!(self.state, ListenerState::Free { .. })
            && self.accept_completion.has_result()
    }

    fn take_accept_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.accept_completion.take_result()
    }

    fn arm_accept(&mut self) -> io::Result<()> {
        let ListenerState::Active { socket } = &self.state else {
            panic!("listener slot must be active before accept");
        };
        socket.accept(&mut self.accept_completion)
    }
}

impl SlabEntry for Listener {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ListenerState::Free { next },
            accept_completion: Completion::new(),
        }
    }

    fn is_free(&self) -> bool {
        matches!(self.state, ListenerState::Free { .. })
    }

    fn next_free(&self) -> Option<usize> {
        match self.state {
            ListenerState::Free { next } => next,
            ListenerState::Active { .. } => None,
        }
    }

    fn release(&mut self, next: Option<usize>) {
        if let ListenerState::Active { socket } =
            std::mem::replace(&mut self.state, ListenerState::Free { next })
        {
            socket.close();
        }
        self.accept_completion = Completion::new();
    }
}

enum ConnectionState {
    Free { next: Option<usize> },
    Open { socket: Box<dyn IOSocket> },
}

struct Connection {
    state: ConnectionState,
    write_buf: Vec<u8>,
    write_offset: usize,
    recv_completion: Completion,
    send_completion: Completion,
}

impl Connection {
    fn open(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ConnectionState::Open { socket };
        self.write_buf.clear();
        self.write_offset = 0;
        self.recv_completion = Completion::new();
        self.send_completion = Completion::new();
        self.arm_recv()
    }

    fn recv_result_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Open { .. })
            && self.recv_completion.has_result()
    }

    fn send_result_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Open { .. })
            && self.send_completion.has_result()
    }

    fn take_recv_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.recv_completion.take_result()
    }

    fn take_send_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.send_completion.take_result()
    }

    fn arm_recv(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket } = &self.state else {
            panic!("connection slot requires open socket");
        };
        socket.recv(&mut self.recv_completion, READ_CHUNK)
    }

    fn arm_send(&mut self) -> io::Result<()> {
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
            recv_completion: Completion::new(),
            send_completion: Completion::new(),
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
        self.recv_completion = Completion::new();
        self.send_completion = Completion::new();
    }
}
