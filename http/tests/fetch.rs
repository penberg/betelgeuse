//! Integration tests for [`Fetch`] against a real socket.
//!
//! Each test stands up a one-shot server on a background thread that accepts a
//! single connection, reads the request, and writes canned response bytes. The
//! client side runs the same loop a real program would: step the `Fetch`, then
//! step the I/O loop.

#![feature(allocator_api)]
#![feature(coroutines)]

use std::{
    alloc::Global,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use betelgeuse::{IOLoop, IOLoopHandle, io_loop, spawn, task::Task};
use betelgeuse_http::{Fetch, Request, Response};

const DEADLINE: Duration = Duration::from_secs(10);

struct TestServer {
    addr: SocketAddr,
    handle: JoinHandle<Vec<u8>>,
}

impl TestServer {
    fn request(self) -> Vec<u8> {
        self.handle.join().expect("server thread panicked")
    }
}

fn serve(response: &[u8]) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let response = response.to_vec();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let request = read_request(&mut stream);
        let _ = stream.write_all(&response);
        let _ = stream.flush();
        request
    });
    TestServer { addr, handle }
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        if let Some(at) = find(&buf, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..at]).to_lowercase();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buf.len() >= at + 4 + length {
                break;
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    buf
}

/// Copies test bytes into an explicitly allocated request body.
fn body(bytes: &[u8]) -> Vec<u8, Global> {
    let mut body = Vec::new_in(Global);
    body.extend_from_slice(bytes);
    body
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn make_loop() -> IOLoopHandle<Global> {
    io_loop(Global).expect("io_loop construction failed")
}

fn run(io_loop: &IOLoopHandle<Global>, fetch: &mut Fetch<Global>) -> io::Result<Response<Global>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(response) = fetch.step()? {
            return Ok(response);
        }
        io_loop.step()?;
        assert!(
            Instant::now() < deadline,
            "request did not complete in time"
        );
    }
}

fn exchange(response: &[u8], request: Request<Global>) -> (io::Result<Response<Global>>, Vec<u8>) {
    let server = serve(response);
    let io_loop = make_loop();
    let mut fetch = Fetch::new(Global, &io_loop.io(), server.addr, request).expect("fetch");
    let result = run(&io_loop, &mut fetch);
    (result, server.request())
}

#[test]
fn get_returns_the_status_headers_and_body() {
    let (response, request) = exchange(
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Trace: abc\r\n\r\nhello",
        Request::get(Global, "/index.html"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.status(), 200);
    assert_eq!(response.reason(), "OK");
    assert!(response.is_success());
    assert_eq!(response.body(), b"hello");
    // Header lookup is case-insensitive.
    assert_eq!(response.header("x-trace"), Some("abc"));
    assert_eq!(response.header("Content-Length"), Some("5"));

    let request = String::from_utf8(request).expect("utf-8 request");
    assert!(request.starts_with("GET /index.html HTTP/1.1\r\n"));
    assert!(request.contains("Host: 127.0.0.1:"));
    assert!(request.contains("Connection: close\r\n"));
    assert!(request.ends_with("\r\n\r\n"));
}

#[test]
fn chunked_response_is_reassembled() {
    let (response, _) = exchange(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
          5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
        Request::get(Global, "/"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"hello world");
}

#[test]
fn body_without_framing_ends_at_close() {
    let (response, _) = exchange(
        b"HTTP/1.1 200 OK\r\n\r\nno framing here",
        Request::get(Global, "/"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.body(), b"no framing here");
}

#[test]
fn no_content_response_has_an_empty_body() {
    let (response, _) = exchange(
        b"HTTP/1.1 204 No Content\r\n\r\n",
        Request::get(Global, "/"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.status(), 204);
    assert!(response.body().is_empty());
}

#[test]
fn head_response_ignores_its_content_length() {
    let (response, request) = exchange(
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
        Request::head(Global, "/asset"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.header("content-length"), Some("5"));
    assert!(response.body().is_empty());
    assert!(String::from_utf8_lossy(&request).starts_with("HEAD /asset HTTP/1.1\r\n"));
}

#[test]
fn post_sends_its_body_and_length() {
    let (response, request) = exchange(
        b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok",
        Request::post(Global, "/items", body(b"{\"a\":1}"))
            .with_header("Content-Type", "application/json"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.status(), 201);
    assert_eq!(response.body(), b"ok");

    let request = String::from_utf8(request).expect("utf-8 request");
    assert!(request.starts_with("POST /items HTTP/1.1\r\n"));
    assert!(request.contains("Content-Type: application/json\r\n"));
    assert!(request.contains("Content-Length: 7\r\n"));
    assert!(request.ends_with("\r\n\r\n{\"a\":1}"));
}

#[test]
fn interim_responses_are_skipped() {
    let (response, _) = exchange(
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi",
        Request::get(Global, "/"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"hi");
}

#[test]
fn error_status_is_a_response_not_an_error() {
    let (response, _) = exchange(
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nnot found",
        Request::get(Global, "/missing"),
    );

    let response = response.expect("request failed");
    assert_eq!(response.status(), 404);
    assert!(!response.is_success());
    assert_eq!(response.body(), b"not found");
}

#[test]
fn malformed_response_head_fails() {
    let (response, _) = exchange(b"NOT-HTTP 200 OK\r\n\r\n", Request::get(Global, "/"));

    let err = response.expect_err("expected a protocol error");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn truncated_body_fails() {
    let (response, _) = exchange(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc",
        Request::get(Global, "/"),
    );

    let err = response.expect_err("expected a truncation error");
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn closing_before_the_head_fails() {
    let (response, _) = exchange(b"", Request::get(Global, "/"));

    let err = response.expect_err("expected a truncation error");
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn oversized_response_fails() {
    let server = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    let io_loop = make_loop();
    let mut fetch = Fetch::new(
        Global,
        &io_loop.io(),
        server.addr,
        Request::get(Global, "/"),
    )
    .expect("fetch")
    .with_max_response_bytes(8);

    let err = run(&io_loop, &mut fetch).expect_err("expected a size-limit error");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn connection_refused_fails() {
    // Bind and drop, so the port is almost certainly unused.
    let addr = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("local_addr");

    let io_loop = make_loop();
    let mut fetch =
        Fetch::new(Global, &io_loop.io(), addr, Request::get(Global, "/")).expect("fetch");
    let err = run(&io_loop, &mut fetch).expect_err("expected a connect error");
    assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
}

#[test]
fn a_finished_fetch_stays_finished() {
    let server = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
    let io_loop = make_loop();
    let mut fetch = Fetch::new(
        Global,
        &io_loop.io(),
        server.addr,
        Request::get(Global, "/"),
    )
    .expect("fetch");

    assert!(!fetch.is_done());
    assert_eq!(
        run(&io_loop, &mut fetch).expect("request failed").body(),
        b"hi"
    );
    assert!(fetch.is_done());
    assert!(fetch.step().expect("step after completion").is_none());
}

#[test]
fn a_malformed_request_is_rejected_before_connecting() {
    let io_loop = make_loop();
    let addr: SocketAddr = "127.0.0.1:9".parse().expect("addr");
    let err = Fetch::new(
        Global,
        &io_loop.io(),
        addr,
        Request::get(Global, "no-leading-slash"),
    )
    .err()
    .expect("expected a request validation error");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn fetch_composes_as_a_task() {
    let server = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
    let addr = server.addr;
    let io_loop = make_loop();

    // Fetch implements StepOp, so a request is awaitable inside a coroutine
    // exactly like a core operation is.
    let mut task: Task<Response<Global>, Global> = spawn!(Global, |io| {
        let response = io_await!(
            io,
            Fetch::new(Global, &io, addr, Request::get(Global, "/"))?
        )?;
        Ok(response)
    });

    let io = io_loop.io();
    let deadline = Instant::now() + DEADLINE;
    let response = loop {
        if let Some(response) = task.step(&io).expect("task failed") {
            break response;
        }
        io_loop.step().expect("io loop failed");
        assert!(Instant::now() < deadline, "task did not complete in time");
    };

    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"hello");
    assert!(task.is_done());
}

#[test]
fn a_one_byte_recv_chunk_still_assembles_the_response() {
    let server = serve(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
    );
    let io_loop = make_loop();
    let mut fetch = Fetch::new(
        Global,
        &io_loop.io(),
        server.addr,
        Request::get(Global, "/"),
    )
    .expect("fetch")
    .with_recv_chunk(1);

    let response = run(&io_loop, &mut fetch).expect("request failed");
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"hello world");
}
