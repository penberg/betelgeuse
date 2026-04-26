use std::{alloc::Allocator, collections::HashMap, io, net::SocketAddr};

use betelgeuse::{IOHandle, IOSocket, slab::Slab};

use crate::connection::{Connection, ConnectionStep, Item};
use crate::listener::{Listener, ListenerStep};

const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;

pub struct Server<A: Allocator + Clone> {
    io: IOHandle,
    listeners: Slab<A, Listener>,
    connections: Slab<A, Connection>,
    store: HashMap<Vec<u8>, Item>,
}

impl<A: Allocator + Clone> Server<A> {
    pub fn new(allocator: A, io: IOHandle) -> Self {
        Self {
            io,
            listeners: Slab::new(allocator.clone(), MAX_LISTENERS),
            connections: Slab::new(allocator, MAX_CONNECTIONS),
            store: HashMap::new(),
        }
    }

    pub fn listen(&mut self, addr: SocketAddr) -> io::Result<()> {
        let mut listener = self
            .listeners
            .acquire_mut()
            .ok_or_else(|| io::Error::other("listener pool exhausted"))?;
        listener.listen(&self.io, addr)
    }

    pub fn step(&mut self) -> io::Result<()> {
        for idx in 0..self.listeners.capacity() {
            let Some(listener) = self.listeners.entry_mut(idx) else {
                continue;
            };
            if let ListenerStep::Accepted(socket) = listener.step()? {
                self.register_connection(socket)?;
            }
        }

        for idx in 0..self.connections.capacity() {
            let Some(conn) = self.connections.entry_mut(idx) else {
                continue;
            };
            if let ConnectionStep::Close = conn.step(&mut self.store)? {
                self.connections.release(idx);
            }
        }

        Ok(())
    }

    fn register_connection(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        let Some(mut conn) = self.connections.acquire_mut() else {
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
            return Ok(());
        }
        if let Err(err) = conn.open(socket) {
            conn.release();
            return Err(err);
        }
        Ok(())
    }
}
