use std::{alloc::Allocator, io, net::SocketAddr};

use betelgeuse::{IO, IOHandle, IOSocket, slab::Slab};

use crate::connection::{Connection, ConnectionStep};
use crate::listener::{Listener, ListenerStep};

const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;

pub struct Server<A: Allocator + Clone> {
    io: IOHandle,
    listeners: Slab<A, Listener>,
    connections: Slab<A, Connection>,
}

impl<A: Allocator + Clone> Server<A> {
    pub fn new(allocator: A, io: IOHandle) -> Self {
        Self {
            io,
            listeners: Slab::new(allocator.clone(), MAX_LISTENERS),
            connections: Slab::new(allocator, MAX_CONNECTIONS),
        }
    }

    pub fn listen(&mut self, addr: SocketAddr) -> io::Result<()> {
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

    pub fn step(&mut self) -> io::Result<()> {
        for idx in 0..self.listeners.capacity() {
            let Some(listener) = self.listeners.entry_mut(idx) else {
                continue;
            };
            if let ListenerStep::Accepted(socket) = listener.step()? {
                self.insert_connection(socket)?;
            }
        }

        for idx in 0..self.connections.capacity() {
            let Some(conn) = self.connections.entry_mut(idx) else {
                continue;
            };
            if let ConnectionStep::Close = conn.step()? {
                self.connections.release(idx);
            }
        }

        Ok(())
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
