//! HTTP/1.1 client for Betelgeuse.
//!
//! The crate provides one thing: [`Fetch`], a caller-driven state machine that
//! performs a single HTTP request over a Betelgeuse socket. It follows the same
//! model as the rest of the project — nothing advances unless the caller asks:
//!
//! ```no_run
//! # #![feature(allocator_api)]
//! # use std::alloc::Global;
//! # use betelgeuse::{IOLoop, io_loop};
//! # use betelgeuse_http::{Fetch, Request};
//! # fn main() -> std::io::Result<()> {
//! let io_loop = io_loop(Global)?;
//! let addr = "127.0.0.1:8080".parse().unwrap();
//! let mut fetch = Fetch::new(Global, &io_loop.io(), addr, Request::get(Global, "/"))?;
//!
//! let response = loop {
//!     if let Some(response) = fetch.step()? {
//!         break response;
//!     }
//!     io_loop.step()?;
//! };
//! println!("{} {}", response.status(), response.body().len());
//! # Ok(())
//! # }
//! ```
//!
//! [`Fetch`] also implements [`betelgeuse::op::StepOp`], so the same request
//! composes with `spawn!`/`io_await!` when a coroutine is more convenient than
//! an explicit state machine.
//!
//! # Scope
//!
//! Plain HTTP/1.1 over TCP, one request per connection, response buffered in
//! memory. No TLS, no redirects, no content decoding, no connection reuse, no
//! name resolution: the caller passes an already-resolved [`SocketAddr`](std::net::SocketAddr).
//!
//! # Memory
//!
//! Nothing in this crate allocates implicitly. Every type is generic over an
//! [`Allocator`](std::alloc::Allocator) and every buffer it owns is created
//! with an explicit `*_in` constructor, so a caller that runs on an arena, a
//! per-connection pool, or a failure-injecting allocator gets the whole
//! request and response out of it. That extends to text: [`Text`] stands in
//! for [`String`](std::string::String), which is hard-wired to the global
//! allocator.
//!
//! Two allocations remain global, and both belong to somebody else's API: the
//! transfer buffer [`betelgeuse::IOSocket`] moves in and out of the backend on
//! every `send` and `recv`, and whatever [`std::io::Error`] does internally
//! with the message of a protocol failure.

#![feature(allocator_api)]

mod body;
mod fetch;
mod request;
mod response;
mod text;

pub use fetch::Fetch;
pub use request::Request;
pub use response::{Head, Headers, Response};
pub use text::Text;
