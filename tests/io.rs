//! Integration tests for the Betelgeuse I/O interface.
//!
//! Each test is parameterized over both native backends (`linux`, `darwin`) via
//! the `io_test!` macro, so every contract is exercised against each concrete
//! implementation. Tests share two small helpers: `make_loop` constructs an
//! `IOLoopHandle`, and `free_port` hands out an ephemeral TCP port. Each test
//! pumps the loop with `while !c.has_result() { io_loop.step()?; }` directly.

#![feature(allocator_api)]

use std::{
    alloc::Global,
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
};

use betelgeuse::{
    AcceptCompletion, FsyncCompletion, IO, IOLoop, IOLoopHandle, MkdirCompletion, OpenOptions,
    PReadCompletion, PWriteCompletion, RecvCompletion, SendCompletion, SizeCompletion, io_loop,
};
use tempfile::TempDir;

fn make_loop() -> IOLoopHandle<Global> {
    io_loop(Global).expect("io_loop construction failed")
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
    (fn $name:ident($io_loop:ident) -> io::Result<()> $body:block) => {
        mod $name {
            use super::*;

            fn run($io_loop: &IOLoopHandle<Global>) -> io::Result<()> $body

            #[test]
            fn native() {
                run(&make_loop()).unwrap();
            }
        }
    };
}

io_test! {
    fn backend_name_survives_io_handle(io_loop) -> io::Result<()> {
        let name = io_loop.backend_name();
        assert!(name == "linux" || name == "darwin");
        assert_eq!(io_loop.io().backend_name(), name);
        Ok(())
    }
}

io_test! {
    fn open_creates_file(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new.bin");
        let _file = io_loop.io().open(&path, rw_create_truncate())?;
        assert!(path.exists());
        Ok(())
    }
}

io_test! {
    fn open_missing_without_create_fails(io_loop) -> io::Result<()> {
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
        Ok(())
    }
}

io_test! {
    fn pwrite_then_pread_roundtrip(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.bin");
        let file = io_loop.io().open(&path, rw_create_truncate())?;

        let mut c = PWriteCompletion::new();
        file.pwrite(&mut c, b"hello, world".to_vec(), 0)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(c.take_result().unwrap()?, 12);

        let mut c = PReadCompletion::new();
        file.pread(&mut c, 12, 0)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(&c.take_result().unwrap()?, b"hello, world");
        Ok(())
    }
}

io_test! {
    fn pread_past_eof_returns_short_read(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("short.bin");
        let file = io_loop.io().open(&path, rw_create_truncate())?;

        let mut c = PWriteCompletion::new();
        file.pwrite(&mut c, b"abc".to_vec(), 0)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        c.take_result().unwrap()?;

        let mut c = PReadCompletion::new();
        file.pread(&mut c, 128, 0)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(&c.take_result().unwrap()?, b"abc");
        Ok(())
    }
}

io_test! {
    fn pwrite_at_offset_grows_file(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("offset.bin");
        let file = io_loop.io().open(&path, rw_create_truncate())?;

        let mut c = PWriteCompletion::new();
        file.pwrite(&mut c, b"xyz".to_vec(), 5)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        c.take_result().unwrap()?;

        let mut c = SizeCompletion::new();
        file.size(&mut c)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(c.take_result().unwrap()?, 8);

        let mut c = PReadCompletion::new();
        file.pread(&mut c, 3, 5)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(&c.take_result().unwrap()?, b"xyz");
        Ok(())
    }
}

io_test! {
    fn pwrite_then_size_preserves_fifo_order(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("ordered-size.bin");
        let file = io_loop.io().open(&path, rw_create_truncate())?;

        let mut write_c = PWriteCompletion::new();
        let mut size_c = SizeCompletion::new();
        file.pwrite(&mut write_c, b"xyz".to_vec(), 0)?;
        file.size(&mut size_c)?;

        while !write_c.has_result() || !size_c.has_result() {
            io_loop.step()?;
        }

        assert_eq!(write_c.take_result().unwrap()?, 3);
        assert_eq!(size_c.take_result().unwrap()?, 3);
        Ok(())
    }
}

io_test! {
    fn fsync_succeeds_after_write(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sync.bin");
        let file = io_loop.io().open(&path, rw_create_truncate())?;

        let mut c = PWriteCompletion::new();
        file.pwrite(&mut c, b"sync me".to_vec(), 0)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        c.take_result().unwrap()?;

        let mut c = FsyncCompletion::new();
        file.fsync(&mut c)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        c.take_result().unwrap()?;
        Ok(())
    }
}

io_test! {
    fn truncate_resets_existing_file(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("truncate.bin");
        std::fs::write(&path, b"preexisting contents").unwrap();

        let file = io_loop.io().open(
            &path,
            OpenOptions {
                read: true,
                write: true,
                create: false,
                truncate: true,
            },
        )?;

        let mut c = SizeCompletion::new();
        file.size(&mut c)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(c.take_result().unwrap()?, 0);
        Ok(())
    }
}

io_test! {
    fn mkdir_creates_directory(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_dir");

        let mut c = MkdirCompletion::new();
        io_loop.io().mkdir(&mut c, &path, 0o755)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        c.take_result().unwrap()?;
        assert!(path.is_dir());
        Ok(())
    }
}

io_test! {
    fn mkdir_on_existing_directory_returns_already_exists(io_loop) -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("exists");
        std::fs::create_dir(&path).unwrap();

        let mut c = MkdirCompletion::new();
        io_loop.io().mkdir(&mut c, &path, 0o755)?;
        while !c.has_result() {
            io_loop.step()?;
        }
        match c.take_result().unwrap() {
            Ok(_) => panic!("mkdir on existing dir should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::AlreadyExists),
        }
        Ok(())
    }
}

io_test! {
    fn step_returns_false_while_accept_is_still_pending(io_loop) -> io::Result<()> {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let listener = io_loop.io().socket()?;
        listener.bind(addr)?;

        let mut accept_c = AcceptCompletion::new();
        listener.accept(&mut accept_c)?;

        if io_loop.step()? {
            assert!(!accept_c.has_result());
        }
        assert!(!io_loop.step()?);
        assert!(!accept_c.has_result());
        Ok(())
    }
}

io_test! {
    fn accept_send_recv_echo(io_loop) -> io::Result<()> {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let listener = io_loop.io().socket()?;
        listener.bind(addr)?;

        let mut accept_c = AcceptCompletion::new();
        listener.accept(&mut accept_c)?;

        let mut client = TcpStream::connect(addr).unwrap();

        while !accept_c.has_result() {
            io_loop.step()?;
        }
        let accepted = accept_c.take_result().unwrap()?;

        client.write_all(b"ping").unwrap();

        let mut recv_c = RecvCompletion::new();
        accepted.recv(&mut recv_c, 32)?;
        while !recv_c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(&recv_c.take_result().unwrap()?, b"ping");

        let mut send_c = SendCompletion::new();
        accepted.send(&mut send_c, b"pong".to_vec())?;
        while !send_c.has_result() {
            io_loop.step()?;
        }
        assert_eq!(send_c.take_result().unwrap()?, 4);

        let mut reply = [0u8; 4];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"pong");
        Ok(())
    }
}

io_test! {
    fn recv_returns_empty_on_peer_close(io_loop) -> io::Result<()> {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let listener = io_loop.io().socket()?;
        listener.bind(addr)?;

        let mut accept_c = AcceptCompletion::new();
        listener.accept(&mut accept_c)?;

        let client = TcpStream::connect(addr).unwrap();
        while !accept_c.has_result() {
            io_loop.step()?;
        }
        let accepted = accept_c.take_result().unwrap()?;

        drop(client);

        let mut recv_c = RecvCompletion::new();
        accepted.recv(&mut recv_c, 32)?;
        while !recv_c.has_result() {
            io_loop.step()?;
        }
        let received = recv_c.take_result().unwrap()?;
        assert!(received.is_empty(), "expected empty recv on EOF");
        Ok(())
    }
}
