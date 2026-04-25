//! In-memory memcached-style server built on Betelgeuse.
//!
//! Uses a fixed-capacity `Slab` of listener slots and a second `Slab` of
//! connection slots; each slot implements `SlabEntry`, so the server never
//! allocates on the hot path. Protocol state lives in a separate
//! `ProtocolState` struct so that the parser can be unit-tested without
//! constructing a real socket.
//!
//! Run:
//!     cargo run --example memcached
//!
//! Then in another terminal:
//!     nc 127.0.0.1 11211
//!
//! Example session:
//!     set greeting 0 0 5
//!     hello
//!     get greeting

#![feature(allocator_api)]

mod connection;
mod listener;

use std::{
    alloc::{Allocator, Global},
    collections::HashMap,
    io,
    net::SocketAddr,
};

use betelgeuse::{IO, IOHandle, IOLoop, IOSocket, io_loop, slab::Slab};

use connection::{Connection, ConnectionStep, Item};
use listener::{Listener, ListenerStep};

const ADDR: &str = "127.0.0.1:11211";
const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;

fn main() -> io::Result<()> {
    let allocator = Global;
    let io_loop = io_loop(allocator)?;
    let addr: SocketAddr = ADDR.parse().expect("valid address");

    let mut server = Server::new(allocator, io_loop.io());
    server.listen(addr)?;
    println!(
        "memcached listening on {addr} (backend: {})",
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
    store: HashMap<Vec<u8>, Item>,
}

impl<A: Allocator + Clone> Server<A> {
    fn new(allocator: A, io: IOHandle) -> Self {
        Self {
            io,
            listeners: Slab::new(allocator.clone(), MAX_LISTENERS),
            connections: Slab::new(allocator, MAX_CONNECTIONS),
            store: HashMap::new(),
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
            match conn.step(&mut self.store)? {
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
        // Memcached is a small-request/small-response protocol; disabling
        // Nagle's algorithm matches the real server's behavior and avoids
        // per-request latency spikes.
        if let Err(err) = socket.set_nodelay(true) {
            eprintln!("set_nodelay failed: {err}, closing accepted socket");
            socket.close();
            self.connections.release(id);
            return Ok(());
        }
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
