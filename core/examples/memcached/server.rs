use std::{alloc::Allocator, collections::HashMap, io, net::SocketAddr};

use betelgeuse::{IOHandle, IOSocket, slab::Slab};

use crate::connection::{Connection, ConnectionStep, Item};
use crate::listener::{Listener, ListenerStep};

const MAX_CONNECTIONS: usize = 1024;

pub struct Server<A: Allocator + Clone> {
    listener: Listener,
    connections: Slab<A, Connection>,
    store: HashMap<Vec<u8>, Item>,
}

impl<A: Allocator + Clone> Server<A> {
    pub fn start(allocator: A, io: IOHandle, addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            listener: Listener::start(&io, addr)?,
            connections: Slab::new(allocator, MAX_CONNECTIONS),
            store: HashMap::new(),
        })
    }

    pub fn step(&mut self) -> io::Result<()> {
        let connections = &mut self.connections;
        if let ListenerStep::Accepted(socket) = self.listener.step()? {
            Self::register_connection(connections, socket)?;
        }

        let store = &mut self.store;
        for mut conn in self.connections.entries_mut() {
            if let ConnectionStep::Close = conn.step(store)? {
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
