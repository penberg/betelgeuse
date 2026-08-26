use std::{alloc::Allocator, io, net::SocketAddr};

use betelgeuse::{IOHandle, IOSocket, slab::Slab};

use crate::connection::{Connection, ConnectionStep};
use crate::listener::{Listener, ListenerStep};

const MAX_CONNECTIONS: usize = 1024;

pub struct Server<A: Allocator + Clone> {
    listener: Listener,
    connections: Slab<A, Connection>,
}

impl<A: Allocator + Clone> Server<A> {
    pub fn start(allocator: A, io: IOHandle, addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            listener: Listener::start(&io, addr)?,
            connections: Slab::new(allocator, MAX_CONNECTIONS),
        })
    }

    pub fn step(&mut self) -> io::Result<()> {
        let connections = &mut self.connections;
        if let ListenerStep::Accepted(socket) = self.listener.step()? {
            Self::register_connection(connections, socket)?;
        }

        for mut conn in self.connections.entries_mut() {
            if let ConnectionStep::Close = conn.step()? {
                conn.release();
            }
        }

        Ok(())
    }

    fn register_connection(
        connections: &mut Slab<A, Connection>,
        socket: Box<dyn IOSocket>,
    ) -> io::Result<()> {
        let Some(mut conn) = connections.acquire_mut() else {
            eprintln!("connection pool exhausted, dropping accepted socket");
            socket.close();
            return Ok(());
        };
        if let Err(err) = conn.open(socket) {
            conn.release();
            return Err(err);
        }
        Ok(())
    }
}
