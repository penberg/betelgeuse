//! TCP echo server built on Betelgeuse.
//!
//! The example shows the canonical "step-driven" server shape:
//!
//! - one fixed-capacity `Slab` of listener slots,
//! - one fixed-capacity `Slab` of connection slots,
//! - a `Server::step` that walks every slot and lets each one advance its own
//!   state machine via `Listener::step` / `Connection::step`,
//! - the outer loop alternates `io.step()` (drive the backend) with
//!   `server.step()` (react to completions).
//!
//! Each slot type implements `SlabEntry`, so the slab owns its free list and
//! the server never allocates on the hot path.
//!
//! Run:
//!     cargo run --example echo
//!
//! Then in another terminal:
//!     nc 127.0.0.1 5555

#![feature(allocator_api)]

mod connection;
mod listener;

use std::{
    alloc::{Allocator, Global},
    io,
    net::SocketAddr,
};

use betelgeuse::{IO, IOHandle, IOLoop, IOSocket, io_loop, slab::Slab};

use connection::{Connection, ConnectionStep};
use listener::{Listener, ListenerStep};

const ADDR: &str = "127.0.0.1:5555";
const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;

fn main() -> io::Result<()> {
    let allocator = Global;
    let io_loop = io_loop(allocator)?;
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
            let Some(listener) = self.listeners.entry_mut(idx) else {
                continue;
            };
            match listener.step()? {
                ListenerStep::Idle => {}
                ListenerStep::Accepted(socket) => {
                    progressed = true;
                    self.insert_connection(socket)?;
                }
            }
        }

        for idx in 0..self.connections.capacity() {
            let Some(conn) = self.connections.entry_mut(idx) else {
                continue;
            };
            match conn.step()? {
                ConnectionStep::Idle => {}
                ConnectionStep::Progressed => progressed = true,
                ConnectionStep::Close => {
                    progressed = true;
                    self.connections.release(idx);
                }
            }
        }

        Ok(progressed)
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
}
