//! Fetches one URL path over plain HTTP and prints the response.
//!
//! Usage: `cargo run -p betelgeuse-http --example fetch -- <addr> [path]`
//!
//! ```text
//! cargo run -p betelgeuse-http --example fetch -- 127.0.0.1:8080 /
//! ```

#![feature(allocator_api)]

use std::{alloc::Global, io, net::SocketAddr, process::ExitCode};

use betelgeuse::{IOLoop, io_loop};
use betelgeuse_http::{Fetch, Request, Text};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(addr) = args.next() else {
        eprintln!("usage: fetch <host:port> [path]");
        return ExitCode::FAILURE;
    };
    let path = args.next().unwrap_or_else(|| "/".to_string());

    match fetch(&addr, &path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("fetch failed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn fetch(addr: &str, path: &str) -> io::Result<()> {
    let addr: SocketAddr = addr
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "expected an ip:port address"))?;

    let io_loop = io_loop(Global)?;
    let mut fetch = Fetch::new(Global, &io_loop.io(), addr, Request::get(Global, path))?;

    let response = loop {
        if let Some(response) = fetch.step()? {
            break response;
        }
        io_loop.step()?;
    };

    println!("{} {}", response.status(), response.reason());
    for (name, value) in response.headers().iter() {
        println!("{name}: {value}");
    }
    println!();
    println!("{}", Text::from_utf8_lossy_in(response.body(), Global));
    Ok(())
}
