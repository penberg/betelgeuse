//! In-memory memcached-style server built on Betelgeuse.
//!
//! Uses a fixed-capacity `Slab` of listener slots and a second `Slab` of
//! connection slots; each slot implements `SlabEntry`, so the server never
//! allocates on the hot path. Protocol state lives in a separate
//! `ProtocolState` struct so that the parser can be unit-tested without
//! constructing a real socket.
//!
//! Run one of:
//!     cargo run --example memcached
//!     BETELGEUSE_BACKEND=syscall cargo run --example memcached
//!
//! Then in another terminal:
//!     nc 127.0.0.1 11211
//!
//! Example session:
//!     set greeting 0 0 5
//!     hello
//!     get greeting

#![feature(allocator_api)]

use std::{alloc::Global, collections::HashMap, io, net::SocketAddr};

use betelgeuse::{
    Completion, CompletionResult, IO, IOBackend, IOHandle, IOLoop, IOSocket, io_loop,
    slab::{Slab, SlabEntry},
};

const ADDR: &str = "127.0.0.1:11211";
const MAX_LISTENERS: usize = 4;
const MAX_CONNECTIONS: usize = 1024;
const READ_CHUNK: usize = 8192;
const VERSION: &str = "betelgeuse-memcached 0.1";
const CRLF: &[u8] = b"\r\n";

fn main() -> io::Result<()> {
    let backend = match std::env::var("BETELGEUSE_BACKEND").as_deref() {
        Ok("syscall") => IOBackend::Syscall,
        _ => IOBackend::IoUring,
    };
    let io_loop = io_loop(Global, backend)?;
    let addr: SocketAddr = ADDR.parse().expect("valid address");

    let mut server = Server::new(io_loop.io());
    server.listen(addr)?;
    println!(
        "memcached listening on {addr} (backend: {})",
        io_loop.backend_name()
    );

    loop {
        server.step()?;
        io_loop.step()?;
    }
}

struct Server {
    io: IOHandle,
    listeners: Slab<Listener, Global>,
    connections: Slab<Connection, Global>,
    store: HashMap<Vec<u8>, Item>,
}

impl Server {
    fn new(io: IOHandle) -> Self {
        Self {
            io,
            listeners: Slab::new(Global, MAX_LISTENERS),
            connections: Slab::new(Global, MAX_CONNECTIONS),
            store: HashMap::new(),
        }
    }

    fn listen(&mut self, addr: SocketAddr) -> io::Result<()> {
        let id = self
            .listeners
            .acquire()
            .ok_or_else(|| io::Error::other("listener pool exhausted"))?;
        let socket = self.io.socket()?;
        if let Err(err) = socket.bind(addr) {
            self.listeners.release(id);
            return Err(err);
        }
        self.listeners
            .entry_mut(id)
            .expect("just-acquired slot must exist")
            .activate(socket)
    }

    fn step(&mut self) -> io::Result<bool> {
        let mut progressed = false;

        for idx in 0..self.listeners.capacity() {
            if self
                .listeners
                .entry_mut(idx)
                .is_some_and(|s| s.accept_result_ready())
            {
                progressed = true;
                self.handle_accept(idx)?;
            }
        }

        for idx in 0..self.connections.capacity() {
            if self
                .connections
                .entry_mut(idx)
                .is_some_and(|s| s.recv_result_ready())
            {
                progressed = true;
                self.handle_recv(idx)?;
            }
            if self
                .connections
                .entry_mut(idx)
                .is_some_and(|s| s.send_result_ready())
            {
                progressed = true;
                self.handle_send(idx)?;
            }
        }

        Ok(progressed)
    }

    fn handle_accept(&mut self, idx: usize) -> io::Result<()> {
        let listener = self.listeners.entry_mut(idx).expect("valid slot");
        let result = listener
            .take_accept_result()
            .expect("accept step requires completion result");
        let socket = match result? {
            CompletionResult::Accept(socket) => socket,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "listener accept completion had wrong type",
                ));
            }
        };
        listener.arm_accept()?;
        self.insert_connection(socket)
    }

    fn insert_connection(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        let Some(id) = self.connections.acquire() else {
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
            self.connections.release(id);
            return Ok(());
        }
        if let Err(err) = self
            .connections
            .entry_mut(id)
            .expect("acquired slot")
            .open(socket)
        {
            self.connections.release(id);
            return Err(err);
        }
        Ok(())
    }

    fn handle_recv(&mut self, idx: usize) -> io::Result<()> {
        let conn = self.connections.entry_mut(idx).expect("valid slot");
        let result = conn
            .take_recv_result()
            .expect("recv step requires completion result");
        let buf = match result {
            Ok(CompletionResult::Recv(buf)) => buf,
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                return Ok(());
            }
            Err(err) => return Err(err),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "connection recv completion had wrong type",
                ));
            }
        };
        if buf.is_empty() {
            self.connections.release(idx);
            return Ok(());
        }

        conn.proto.read_buf.extend_from_slice(&buf);
        conn.proto.process_input(&mut self.store);

        let next = if conn.proto.has_pending_output() {
            conn.arm_send()
        } else if conn.proto.close_after_write {
            self.connections.release(idx);
            return Ok(());
        } else {
            conn.arm_recv()
        };

        match next {
            Ok(()) => Ok(()),
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn handle_send(&mut self, idx: usize) -> io::Result<()> {
        let conn = self.connections.entry_mut(idx).expect("valid slot");
        let result = conn
            .take_send_result()
            .expect("send step requires completion result");
        let written = match result {
            Ok(CompletionResult::Send(n)) => n,
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                return Ok(());
            }
            Err(err) => return Err(err),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "connection send completion had wrong type",
                ));
            }
        };
        if written == 0 {
            self.connections.release(idx);
            return Ok(());
        }

        conn.proto.write_offset += written;
        let next = if conn.proto.write_offset < conn.proto.write_buf.len() {
            conn.arm_send()
        } else {
            conn.proto.finish_write();
            if conn.proto.close_after_write {
                self.connections.release(idx);
                return Ok(());
            } else if conn.proto.has_pending_output() {
                conn.arm_send()
            } else {
                conn.arm_recv()
            }
        };

        match next {
            Ok(()) => Ok(()),
            Err(err) if is_peer_disconnect(&err) => {
                self.connections.release(idx);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

enum ListenerState {
    Free { next: Option<usize> },
    Active { socket: Box<dyn IOSocket> },
}

struct Listener {
    state: ListenerState,
    accept_completion: Completion,
}

impl Listener {
    fn activate(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ListenerState::Active { socket };
        self.accept_completion = Completion::new();
        self.arm_accept()
    }

    fn accept_result_ready(&self) -> bool {
        matches!(self.state, ListenerState::Active { .. })
            && self.accept_completion.has_result()
    }

    fn take_accept_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.accept_completion.take_result()
    }

    fn arm_accept(&mut self) -> io::Result<()> {
        let ListenerState::Active { socket } = &self.state else {
            panic!("listener slot must be active before accept");
        };
        socket.accept(&mut self.accept_completion)
    }
}

impl SlabEntry for Listener {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ListenerState::Free { next },
            accept_completion: Completion::new(),
        }
    }

    fn is_free(&self) -> bool {
        matches!(self.state, ListenerState::Free { .. })
    }

    fn next_free(&self) -> Option<usize> {
        match self.state {
            ListenerState::Free { next } => next,
            ListenerState::Active { .. } => None,
        }
    }

    fn release(&mut self, next: Option<usize>) {
        if let ListenerState::Active { socket } =
            std::mem::replace(&mut self.state, ListenerState::Free { next })
        {
            socket.close();
        }
        self.accept_completion = Completion::new();
    }
}

enum ConnectionState {
    Free { next: Option<usize> },
    Open { socket: Box<dyn IOSocket> },
}

struct Connection {
    state: ConnectionState,
    proto: ProtocolState,
    recv_completion: Completion,
    send_completion: Completion,
}

impl Connection {
    fn open(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ConnectionState::Open { socket };
        self.proto = ProtocolState::default();
        self.recv_completion = Completion::new();
        self.send_completion = Completion::new();
        self.arm_recv()
    }

    fn recv_result_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Open { .. })
            && self.recv_completion.has_result()
    }

    fn send_result_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Open { .. })
            && self.send_completion.has_result()
    }

    fn take_recv_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.recv_completion.take_result()
    }

    fn take_send_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.send_completion.take_result()
    }

    fn arm_recv(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket } = &self.state else {
            panic!("connection slot requires open socket");
        };
        socket.recv(&mut self.recv_completion, READ_CHUNK)
    }

    fn arm_send(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket } = &self.state else {
            panic!("connection slot requires open socket");
        };
        socket.send(
            &mut self.send_completion,
            self.proto.write_buf[self.proto.write_offset..].to_vec(),
        )
    }
}

impl SlabEntry for Connection {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ConnectionState::Free { next },
            proto: ProtocolState::default(),
            recv_completion: Completion::new(),
            send_completion: Completion::new(),
        }
    }

    fn is_free(&self) -> bool {
        matches!(self.state, ConnectionState::Free { .. })
    }

    fn next_free(&self) -> Option<usize> {
        match self.state {
            ConnectionState::Free { next } => next,
            ConnectionState::Open { .. } => None,
        }
    }

    fn release(&mut self, next: Option<usize>) {
        if let ConnectionState::Open { socket } =
            std::mem::replace(&mut self.state, ConnectionState::Free { next })
        {
            socket.close();
        }
        self.proto = ProtocolState::default();
        self.recv_completion = Completion::new();
        self.send_completion = Completion::new();
    }
}

#[derive(Default)]
struct ProtocolState {
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    write_offset: usize,
    pending_storage: Option<StorageRequest>,
    close_after_write: bool,
}

impl ProtocolState {
    fn has_pending_output(&self) -> bool {
        self.write_offset < self.write_buf.len()
    }

    fn finish_write(&mut self) {
        self.write_buf.clear();
        self.write_offset = 0;
    }

    fn queue_response(&mut self, data: &[u8]) {
        self.write_buf.extend_from_slice(data);
    }

    fn process_input(&mut self, store: &mut HashMap<Vec<u8>, Item>) {
        loop {
            if let Some(request) = self.pending_storage.take() {
                if self.read_buf.len() < request.bytes + CRLF.len() {
                    self.pending_storage = Some(request);
                    break;
                }

                if &self.read_buf[request.bytes..request.bytes + CRLF.len()] != CRLF {
                    self.queue_response(b"CLIENT_ERROR bad data chunk\r\n");
                    self.close_after_write = true;
                    break;
                }

                let value = self.read_buf.drain(..request.bytes).collect::<Vec<_>>();
                self.read_buf.drain(..CRLF.len());
                self.apply_storage(store, request, value);
                continue;
            }

            let Some(line) = take_line(&mut self.read_buf) else {
                break;
            };
            self.handle_command_line(store, &line);

            if self.close_after_write {
                break;
            }
        }
    }

    fn handle_command_line(&mut self, store: &mut HashMap<Vec<u8>, Item>, line: &[u8]) {
        let mut parts = line
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|part| !part.is_empty());
        let Some(command) = parts.next() else {
            self.queue_response(b"ERROR\r\n");
            return;
        };

        match command {
            b"get" => {
                let keys = parts.collect::<Vec<_>>();
                if keys.is_empty() {
                    self.queue_response(b"CLIENT_ERROR missing key\r\n");
                    return;
                }
                self.handle_get(store, &keys);
            }
            b"set" | b"add" | b"replace" => {
                let Some(key) = parts.next() else {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                };
                let Some(flags) = parse_u32(parts.next()) else {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                };
                let Some(_exptime) = parse_u32(parts.next()) else {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                };
                let Some(bytes) = parse_usize(parts.next()) else {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                };
                let noreply = match parts.next() {
                    None => false,
                    Some(b"noreply") => true,
                    Some(_) => {
                        self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                        return;
                    }
                };
                if parts.next().is_some() {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                }

                let mode = match command {
                    b"set" => StorageMode::Set,
                    b"add" => StorageMode::Add,
                    b"replace" => StorageMode::Replace,
                    _ => unreachable!(),
                };
                self.pending_storage = Some(StorageRequest {
                    mode,
                    key: key.to_vec(),
                    flags,
                    bytes,
                    noreply,
                });
            }
            b"delete" => {
                let Some(key) = parts.next() else {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                };
                let noreply = match parts.next() {
                    None => false,
                    Some(b"noreply") => true,
                    Some(_) => {
                        self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                        return;
                    }
                };
                if parts.next().is_some() {
                    self.queue_response(b"CLIENT_ERROR bad command line format\r\n");
                    return;
                }

                let removed = store.remove(key).is_some();
                if !noreply {
                    if removed {
                        self.queue_response(b"DELETED\r\n");
                    } else {
                        self.queue_response(b"NOT_FOUND\r\n");
                    }
                }
            }
            b"version" => {
                self.queue_response(format!("VERSION {VERSION}\r\n").as_bytes());
            }
            b"quit" => {
                self.close_after_write = true;
            }
            _ => self.queue_response(b"ERROR\r\n"),
        }
    }

    fn handle_get(&mut self, store: &HashMap<Vec<u8>, Item>, keys: &[&[u8]]) {
        for key in keys {
            if let Some(item) = store.get(*key) {
                self.queue_response(b"VALUE ");
                self.queue_response(key);
                self.queue_response(
                    format!(" {} {}\r\n", item.flags, item.value.len()).as_bytes(),
                );
                self.queue_response(&item.value);
                self.queue_response(CRLF);
            }
        }
        self.queue_response(b"END\r\n");
    }

    fn apply_storage(
        &mut self,
        store: &mut HashMap<Vec<u8>, Item>,
        request: StorageRequest,
        value: Vec<u8>,
    ) {
        let should_store = match request.mode {
            StorageMode::Set => true,
            StorageMode::Add => !store.contains_key(&request.key),
            StorageMode::Replace => store.contains_key(&request.key),
        };

        if should_store {
            store.insert(
                request.key,
                Item {
                    flags: request.flags,
                    value,
                },
            );
            if !request.noreply {
                self.queue_response(b"STORED\r\n");
            }
        } else if !request.noreply {
            self.queue_response(b"NOT_STORED\r\n");
        }
    }
}

struct Item {
    flags: u32,
    value: Vec<u8>,
}

struct StorageRequest {
    mode: StorageMode,
    key: Vec<u8>,
    flags: u32,
    bytes: usize,
    noreply: bool,
}

enum StorageMode {
    Set,
    Add,
    Replace,
}

fn is_peer_disconnect(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

fn take_line(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    let pos = buf.windows(CRLF.len()).position(|window| window == CRLF)?;
    let line = buf.drain(..pos).collect::<Vec<_>>();
    buf.drain(..CRLF.len());
    Some(line)
}

fn parse_u32(part: Option<&[u8]>) -> Option<u32> {
    std::str::from_utf8(part?).ok()?.parse().ok()
}

fn parse_usize(part: Option<&[u8]>) -> Option<usize> {
    std::str::from_utf8(part?).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_non_utf8_keys() {
        let mut proto = ProtocolState::default();
        let mut store = HashMap::new();

        proto
            .read_buf
            .extend_from_slice(b"set key\xff 7 0 3\r\nabc\r\nget key\xff\r\n");
        proto.process_input(&mut store);

        assert_eq!(store.get(&b"key\xff"[..]).unwrap().flags, 7);
        assert_eq!(store.get(&b"key\xff"[..]).unwrap().value, b"abc");
        assert_eq!(
            proto.write_buf,
            b"STORED\r\nVALUE key\xff 7 3\r\nabc\r\nEND\r\n"
        );
    }

    #[test]
    fn handles_binary_values() {
        let mut proto = ProtocolState::default();
        let mut store = HashMap::new();

        proto
            .read_buf
            .extend_from_slice(b"set bin 0 0 4\r\na\x00\r\n\r\nget bin\r\n");
        proto.process_input(&mut store);

        assert_eq!(store.get(&b"bin"[..]).unwrap().value, b"a\x00\r\n");
        assert_eq!(
            proto.write_buf,
            b"STORED\r\nVALUE bin 0 4\r\na\x00\r\n\r\nEND\r\n"
        );
    }
}
