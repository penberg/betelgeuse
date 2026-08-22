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
mod server;

use std::{alloc::Global, io, net::SocketAddr};

use betelgeuse::{IO, IOLoop, io_loop};

use server::Server;

const ADDR: &str = "127.0.0.1:5555";

fn main() -> io::Result<()> {
    let allocator = Global;
    let io_loop = io_loop(allocator)?;
    let addr: SocketAddr = ADDR.parse().expect("valid address");

    let mut server = Server::start(allocator, io_loop.io(), addr)?;
    println!(
        "echo listening on {addr} (backend: {})",
        io_loop.backend_name()
    );

    loop {
        server.step()?;
        io_loop.step()?;
    }
}
