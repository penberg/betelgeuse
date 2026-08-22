//! Benchmarks for the HTTP client.
//!
//! Two kinds of measurement live here. `request_encode` is pure CPU work and
//! measures what it says. Everything under `fetch` performs a real request over
//! a loopback connection to an in-process server, so the numbers include the
//! kernel and the busy-polled I/O loop; they are useful for comparing framing,
//! body sizes, and receive chunk sizes against each other, not as an absolute
//! cost of the client.
//!
//! Every iteration of a `fetch` benchmark opens and closes a TCP connection, so
//! outliers are normal: the run competes with the kernel's connection teardown
//! for ephemeral ports, and the client busy-polls its I/O loop while the server
//! thread wants the same CPU. Compare medians across a run, not across
//! machines.

#![feature(allocator_api)]

use std::{
    alloc::Global,
    hint::black_box,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::Duration,
};

use betelgeuse::{IOLoop, IOLoopHandle, io_loop};
use betelgeuse_http::{Fetch, Request, Response};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Matches the client's default, so a benchmark that is not varying the
/// receive chunk measures the out-of-the-box configuration.
const DEFAULT_RECV_CHUNK: usize = 8 * 1024;

fn serve(response: Vec<u8>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            drain_request(&mut stream);
            let _ = stream.write_all(&response);
        }
    });
    addr
}

fn drain_request(stream: &mut TcpStream) {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        if buf.windows(4).any(|window| window == b"\r\n\r\n") {
            return;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

fn content_length_response(size: usize) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\n\r\n").into_bytes();
    response.resize(response.len() + size, b'x');
    response
}

fn chunked_response(size: usize, chunk: usize) -> Vec<u8> {
    let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    let mut written = 0;
    while written < size {
        let n = chunk.min(size - written);
        response.extend_from_slice(format!("{n:x}\r\n").as_bytes());
        response.resize(response.len() + n, b'x');
        response.extend_from_slice(b"\r\n");
        written += n;
    }
    response.extend_from_slice(b"0\r\n\r\n");
    response
}

fn make_loop() -> IOLoopHandle<Global> {
    io_loop(Global).expect("io_loop construction failed")
}

/// Performs one request, the way an application drives it.
fn fetch_once(
    io_loop: &IOLoopHandle<Global>,
    addr: SocketAddr,
    recv_chunk: usize,
) -> Response<Global> {
    let mut fetch = Fetch::new(Global, &io_loop.io(), addr, Request::get(Global, "/"))
        .expect("fetch")
        .with_recv_chunk(recv_chunk)
        .with_max_response_bytes(16 * 1024 * 1024);
    loop {
        if let Some(response) = fetch.step().expect("step") {
            return response;
        }
        io_loop.step().expect("io loop step");
    }
}

fn request_encode(c: &mut Criterion) {
    let get = Request::get(Global, "/index.html");
    let mut body = Vec::new_in(Global);
    body.resize(256, b'x');
    let post = Request::post(Global, "/api/items", body)
        .with_header("Content-Type", "application/json")
        .with_header("Accept", "*/*")
        .with_header("User-Agent", "betelgeuse-http/0.1");

    let mut group = c.benchmark_group("request_encode");
    group.bench_function("get", |b| {
        b.iter(|| black_box(&get).encode("example.com:8080").expect("encode"))
    });
    group.bench_function("post_with_headers", |b| {
        b.iter(|| black_box(&post).encode("example.com:8080").expect("encode"))
    });
    group.finish();
}

/// Per-request overhead: connect, send, parse a head, close.
fn fetch_overhead(c: &mut Criterion) {
    let addr = serve(content_length_response(0));
    let io_loop = make_loop();

    let mut group = c.benchmark_group("fetch");
    group.throughput(Throughput::Elements(1));
    group.bench_function("empty_response", |b| {
        b.iter(|| fetch_once(&io_loop, addr, DEFAULT_RECV_CHUNK))
    });
    group.finish();
}

/// How the client scales with the size of the body it buffers.
fn fetch_body_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch_body_size");
    for size in [1024, 64 * 1024, 1024 * 1024] {
        let addr = serve(content_length_response(size));
        let io_loop = make_loop();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| fetch_once(&io_loop, addr, DEFAULT_RECV_CHUNK))
        });
    }
    group.finish();
}

/// What chunked framing costs over a plain `Content-Length` body.
fn fetch_framing(c: &mut Criterion) {
    const SIZE: usize = 64 * 1024;
    let responses = [
        ("content_length", content_length_response(SIZE)),
        ("chunked_4k", chunked_response(SIZE, 4 * 1024)),
        ("chunked_256b", chunked_response(SIZE, 256)),
    ];

    let mut group = c.benchmark_group("fetch_framing");
    group.throughput(Throughput::Bytes(SIZE as u64));
    for (name, response) in responses {
        let addr = serve(response);
        let io_loop = make_loop();
        group.bench_function(name, |b| {
            b.iter(|| fetch_once(&io_loop, addr, DEFAULT_RECV_CHUNK))
        });
    }
    group.finish();
}

/// The receive chunk trades syscalls against the cost of compacting the
/// receive buffer after every read.
fn fetch_recv_chunk(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let addr = serve(content_length_response(SIZE));
    let io_loop = make_loop();

    let mut group = c.benchmark_group("fetch_recv_chunk");
    group.throughput(Throughput::Bytes(SIZE as u64));
    for chunk in [8 * 1024, 64 * 1024, 256 * 1024] {
        group.bench_with_input(BenchmarkId::from_parameter(chunk), &chunk, |b, &chunk| {
            b.iter(|| fetch_once(&io_loop, addr, chunk))
        });
    }
    group.finish();
}

criterion_group!(cpu, request_encode);

// Every iteration here opens a TCP connection, so the sampling is kept short:
// a long run would work through the ephemeral port range faster than closed
// connections leave TIME_WAIT, and start measuring the kernel's port table.
criterion_group! {
    name = network;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(20);
    targets = fetch_overhead, fetch_body_size, fetch_framing, fetch_recv_chunk
}

criterion_main!(cpu, network);
