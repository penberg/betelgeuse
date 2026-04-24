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

mod connection;
mod listener;

use std::{alloc::{Allocator, Global}, io, net::SocketAddr};

use betelgeuse::{
    CompletionResult, IO, IOBackend, IOHandle, IOLoop, IOSocket, io_loop,
    slab::Slab,
};

use connection::Connection;
use listener::Listener;

const ADDR: &str = "127.0.0.1:5555";
const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;

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
