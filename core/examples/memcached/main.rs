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
mod server;

use std::{alloc::Global, io, net::SocketAddr};

use betelgeuse::{IO, IOLoop, io_loop};

use server::Server;

const ADDR: &str = "127.0.0.1:11211";

fn main() -> io::Result<()> {
    let allocator = Global;
    let io_loop = io_loop(allocator)?;
    let addr: SocketAddr = ADDR.parse().expect("valid address");

    let mut server = Server::start(allocator, io_loop.io(), addr)?;
    println!(
        "memcached listening on {addr} (backend: {})",
        io_loop.backend_name()
    );

    loop {
        server.step()?;
        io_loop.step()?;
    }
}
