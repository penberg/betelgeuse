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

use connection::{Connection, Item, is_peer_disconnect};
use listener::Listener;

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
        let socket = listener
            .take_accept_result()
            .expect("accept step requires completion result")?;
        listener.arm_accept()?;
        self.insert_connection(socket)
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

    fn handle_recv(&mut self, idx: usize) -> io::Result<()> {
        let conn = self.connections.entry_mut(idx).expect("valid slot");
        let buf = match conn
            .take_recv_result()
            .expect("recv step requires completion result")
        {
            Ok(buf) => buf,
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if buf.is_empty() {
            self.connections.release(idx);
            return Ok(());
        }

        conn.proto.read_buf.extend_from_slice(&buf);
        conn.proto.process_input(&mut self.store);

        let next = if conn.proto.has_pending_output() {
            conn.arm_send()
        } else if conn.proto.close_after_write {
            self.connections.release(idx);
            return Ok(());
        } else {
            conn.arm_recv()
        };

        match next {
            Ok(()) => Ok(()),
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn handle_send(&mut self, idx: usize) -> io::Result<()> {
        let conn = self.connections.entry_mut(idx).expect("valid slot");
        let written = match conn
            .take_send_result()
            .expect("send step requires completion result")
        {
            Ok(n) => n,
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if written == 0 {
            self.connections.release(idx);
            return Ok(());
        }

        conn.proto.write_offset += written;
        let next = if conn.proto.write_offset < conn.proto.write_buf.len() {
            conn.arm_send()
        } else {
            conn.proto.finish_write();
            if conn.proto.close_after_write {
                self.connections.release(idx);
                return Ok(());
            } else if conn.proto.has_pending_output() {
                conn.arm_send()
            } else {
                conn.arm_recv()
            }
        };

        match next {
            Ok(()) => Ok(()),
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}
