//! Integration tests for the Betelgeuse I/O interface.
//!
//! Each test is parameterized over both backends (`syscall`, `io_uring`) via
//! the `io_test!` macro, so every contract is exercised against each concrete
//! implementation. Tests share a small runtime harness: `make_loop` constructs
//! an `IOLoopHandle`, `wait` pumps the loop until a given completion carries a
//! terminal result, and `free_port` hands out an ephemeral TCP port.

#![feature(allocator_api)]

use std::{
    alloc::Global,
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
};

use betelgeuse::{
    Completion, CompletionResult, IO, IOBackend, IOLoop, IOLoopHandle, OpenOptions, io_loop,
};
use tempfile::TempDir;

fn make_loop(backend: IOBackend) -> IOLoopHandle<Global> {
    io_loop(Global, backend).expect("io_loop construction failed")
}

fn wait(
    io_loop: &IOLoopHandle<Global>,
    completion: &mut Completion,
) -> io::Result<CompletionResult> {
    while !completion.has_result() {
        io_loop.step()?;
    }
    completion.take_result().expect("completion has no result")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind port 0")
        .local_addr()
        .expect("local_addr")
        .port()
}

fn rw_create_truncate() -> OpenOptions {
    OpenOptions {
        read: true,
        write: true,
        create: true,
        truncate: true,
    }
}

macro_rules! io_test {
    (fn $name:ident($io_loop:ident) $body:block) => {
        mod $name {
            use super::*;

            fn run($io_loop: &IOLoopHandle<Global>) $body

            #[test]
            fn syscall() {
                run(&make_loop(IOBackend::Syscall));
            }

            #[test]
            fn io_uring() {
                run(&make_loop(IOBackend::IoUring));
            }
        }
    };
}

io_test! {
    fn backend_name_survives_io_handle(io_loop) {
        let name = io_loop.backend_name();
        assert!(name == "syscall" || name == "io_uring");
        assert_eq!(io_loop.io().backend_name(), name);
    }
}

io_test! {
    fn open_creates_file(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new.bin");
        let _file = io_loop.io().open(&path, rw_create_truncate()).unwrap();
        assert!(path.exists());
    }
}

io_test! {
    fn open_missing_without_create_fails(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("missing.bin");
        let opts = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };
        match io_loop.io().open(&path, opts) {
            Ok(_) => panic!("open should fail when file does not exist"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::NotFound),
        }
    }
}

io_test! {
    fn pwrite_then_pread_roundtrip(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.bin");
        let file = io_loop.io().open(&path, rw_create_truncate()).unwrap();

        let mut c = Completion::new();
        file.pwrite(&mut c, b"hello, world".to_vec(), 0).unwrap();
        match wait(io_loop, &mut c).unwrap() {
            CompletionResult::PWrite(n) => assert_eq!(n, 12),
            _ => panic!("expected PWrite"),
        }

        let mut c = Completion::new();
        file.pread(&mut c, 12, 0).unwrap();
        match wait(io_loop, &mut c).unwrap() {
            CompletionResult::PRead(buf) => assert_eq!(&buf, b"hello, world"),
            _ => panic!("expected PRead"),
        }
    }
}

io_test! {
    fn pread_past_eof_returns_short_read(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("short.bin");
        let file = io_loop.io().open(&path, rw_create_truncate()).unwrap();

        let mut c = Completion::new();
        file.pwrite(&mut c, b"abc".to_vec(), 0).unwrap();
        wait(io_loop, &mut c).unwrap();

        let mut c = Completion::new();
        file.pread(&mut c, 128, 0).unwrap();
        match wait(io_loop, &mut c).unwrap() {
            CompletionResult::PRead(buf) => assert_eq!(&buf, b"abc"),
            _ => panic!("expected PRead"),
        }
    }
}

io_test! {
    fn pwrite_at_offset_grows_file(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("offset.bin");
        let file = io_loop.io().open(&path, rw_create_truncate()).unwrap();

        let mut c = Completion::new();
        file.pwrite(&mut c, b"xyz".to_vec(), 5).unwrap();
        wait(io_loop, &mut c).unwrap();

        let mut c = Completion::new();
        file.size(&mut c).unwrap();
        match wait(io_loop, &mut c).unwrap() {
            CompletionResult::Size(n) => assert_eq!(n, 8),
            _ => panic!("expected Size"),
        }

        let mut c = Completion::new();
        file.pread(&mut c, 3, 5).unwrap();
        match wait(io_loop, &mut c).unwrap() {
            CompletionResult::PRead(buf) => assert_eq!(&buf, b"xyz"),
            _ => panic!("expected PRead"),
        }
    }
}

io_test! {
    fn pwrite_then_size_preserves_fifo_order(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("ordered-size.bin");
        let file = io_loop.io().open(&path, rw_create_truncate()).unwrap();

        let mut write_c = Completion::new();
        let mut size_c = Completion::new();
        file.pwrite(&mut write_c, b"xyz".to_vec(), 0).unwrap();
        file.size(&mut size_c).unwrap();

        while !write_c.has_result() || !size_c.has_result() {
            io_loop.step().unwrap();
        }

        match write_c.take_result().unwrap().unwrap() {
            CompletionResult::PWrite(n) => assert_eq!(n, 3),
            _ => panic!("expected PWrite"),
        }
        match size_c.take_result().unwrap().unwrap() {
            CompletionResult::Size(n) => assert_eq!(n, 3),
            _ => panic!("expected Size"),
        }
    }
}

io_test! {
    fn fsync_succeeds_after_write(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sync.bin");
        let file = io_loop.io().open(&path, rw_create_truncate()).unwrap();

        let mut c = Completion::new();
        file.pwrite(&mut c, b"sync me".to_vec(), 0).unwrap();
        wait(io_loop, &mut c).unwrap();

        let mut c = Completion::new();
        file.fsync(&mut c).unwrap();
        assert!(matches!(
            wait(io_loop, &mut c).unwrap(),
            CompletionResult::Fsync
        ));
    }
}

io_test! {
    fn truncate_resets_existing_file(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("truncate.bin");
        std::fs::write(&path, b"preexisting contents").unwrap();

        let file = io_loop
            .io()
            .open(
                &path,
                OpenOptions {
                    read: true,
                    write: true,
                    create: false,
                    truncate: true,
                },
            )
            .unwrap();

        let mut c = Completion::new();
        file.size(&mut c).unwrap();
        match wait(io_loop, &mut c).unwrap() {
            CompletionResult::Size(n) => assert_eq!(n, 0),
            _ => panic!("expected Size"),
        }
    }
}

io_test! {
    fn mkdir_creates_directory(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_dir");

        let mut c = Completion::new();
        io_loop.io().mkdir(&mut c, &path, 0o755).unwrap();
        assert!(matches!(
            wait(io_loop, &mut c).unwrap(),
            CompletionResult::Mkdir
        ));
        assert!(path.is_dir());
    }
}

io_test! {
    fn mkdir_on_existing_directory_returns_already_exists(io_loop) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("exists");
        std::fs::create_dir(&path).unwrap();

        let mut c = Completion::new();
        io_loop.io().mkdir(&mut c, &path, 0o755).unwrap();
        match wait(io_loop, &mut c) {
            Ok(_) => panic!("mkdir on existing dir should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::AlreadyExists),
        }
    }
}

io_test! {
    fn step_returns_false_while_accept_is_still_pending(io_loop) {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let listener = io_loop.io().socket().unwrap();
        listener.bind(addr).unwrap();

        let mut accept_c = Completion::new();
        listener.accept(&mut accept_c).unwrap();

        if io_loop.step().unwrap() {
            assert!(!accept_c.has_result());
        }
        assert!(!io_loop.step().unwrap());
        assert!(!accept_c.has_result());
    }
}

io_test! {
    fn accept_send_recv_echo(io_loop) {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let listener = io_loop.io().socket().unwrap();
        listener.bind(addr).unwrap();

        let mut accept_c = Completion::new();
        listener.accept(&mut accept_c).unwrap();

        let mut client = TcpStream::connect(addr).unwrap();

        let accepted = match wait(io_loop, &mut accept_c).unwrap() {
            CompletionResult::Accept(s) => s,
            _ => panic!("expected Accept"),
        };

        client.write_all(b"ping").unwrap();

        let mut recv_c = Completion::new();
        accepted.recv(&mut recv_c, 32).unwrap();
        let received = match wait(io_loop, &mut recv_c).unwrap() {
            CompletionResult::Recv(b) => b,
            _ => panic!("expected Recv"),
        };
        assert_eq!(&received, b"ping");

        let mut send_c = Completion::new();
        accepted.send(&mut send_c, b"pong".to_vec()).unwrap();
        match wait(io_loop, &mut send_c).unwrap() {
            CompletionResult::Send(n) => assert_eq!(n, 4),
            _ => panic!("expected Send"),
        }

        let mut reply = [0u8; 4];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"pong");
    }
}

io_test! {
    fn recv_returns_empty_on_peer_close(io_loop) {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let listener = io_loop.io().socket().unwrap();
        listener.bind(addr).unwrap();

        let mut accept_c = Completion::new();
        listener.accept(&mut accept_c).unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let accepted = match wait(io_loop, &mut accept_c).unwrap() {
            CompletionResult::Accept(s) => s,
            _ => panic!("expected Accept"),
        };

        drop(client);

        let mut recv_c = Completion::new();
        accepted.recv(&mut recv_c, 32).unwrap();
        match wait(io_loop, &mut recv_c).unwrap() {
            CompletionResult::Recv(b) => assert!(b.is_empty(), "expected empty recv on EOF"),
            _ => panic!("expected Recv"),
        }
    }
}
