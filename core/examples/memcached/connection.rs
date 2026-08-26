use std::{collections::HashMap, io};

use betelgeuse::{IOSocket, RecvCompletion, SendCompletion, slab::SlabEntry};

pub const READ_CHUNK: usize = 8192;
pub const VERSION: &str = "betelgeuse-memcached 0.1";
pub const CRLF: &[u8] = b"\r\n";

/// Outcome of advancing a connection's state machine for one tick.
pub enum ConnectionStep {
    /// Slot is still alive — either nothing was ready or it advanced.
    Continue,
    /// The peer hung up, sent zero, or the protocol asked to close.
    Close,
}

enum ConnectionState {
    Open {
        socket: Box<dyn IOSocket>,
        phase: Phase,
    },
    Free {
        next: Option<usize>,
    },
}

/// Which I/O direction the connection is currently waiting on.
enum Phase {
    Receiving,
    Sending,
}

pub struct Connection {
    state: ConnectionState,
    proto: ProtocolState,
    recv_completion: RecvCompletion,
    send_completion: SendCompletion,
}

impl Connection {
    pub fn open(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ConnectionState::Open {
            socket,
            phase: Phase::Receiving,
        };
        self.proto = ProtocolState::default();
        self.recv_completion = RecvCompletion::new();
        self.send_completion = SendCompletion::new();
        self.arm_recv()
    }

    pub fn step(&mut self, store: &mut HashMap<Vec<u8>, Item>) -> io::Result<ConnectionStep> {
        match &self.state {
            ConnectionState::Open {
                phase: Phase::Receiving,
                ..
            } if self.recv_completion.has_result() => self.complete_recv(store),
            ConnectionState::Open {
                phase: Phase::Sending,
                ..
            } if self.send_completion.has_result() => self.complete_send(),
            _ => Ok(ConnectionStep::Continue),
        }
    }

    fn complete_recv(&mut self, store: &mut HashMap<Vec<u8>, Item>) -> io::Result<ConnectionStep> {
        let buf = match self
            .recv_completion
            .take_result()
            .expect("step() guarantees a recv result is ready")
        {
            Ok(buf) => buf,
            Err(err) if is_peer_disconnect(&err) => return Ok(ConnectionStep::Close),
            Err(err) => return Err(err),
        };
        if buf.is_empty() {
            return Ok(ConnectionStep::Close);
        }

        self.proto.read_buf.extend_from_slice(&buf);
        self.proto.process_input(store);

        if self.proto.has_pending_output() {
            classify(self.transition_to_sending())
        } else if self.proto.close_after_write {
            Ok(ConnectionStep::Close)
        } else {
            classify(self.arm_recv())
        }
    }

    fn complete_send(&mut self) -> io::Result<ConnectionStep> {
        let written = match self
            .send_completion
            .take_result()
            .expect("step() guarantees a send result is ready")
        {
            Ok(n) => n,
            Err(err) if is_peer_disconnect(&err) => return Ok(ConnectionStep::Close),
            Err(err) => return Err(err),
        };
        if written == 0 {
            return Ok(ConnectionStep::Close);
        }

        self.proto.write_offset += written;
        if self.proto.write_offset < self.proto.write_buf.len() {
            return classify(self.arm_send());
        }

        self.proto.finish_write();
        if self.proto.close_after_write {
            Ok(ConnectionStep::Close)
        } else if self.proto.has_pending_output() {
            classify(self.arm_send())
        } else {
            classify(self.transition_to_receiving())
        }
    }

    fn transition_to_sending(&mut self) -> io::Result<()> {
        let ConnectionState::Open { phase, .. } = &mut self.state else {
            unreachable!("transitions only run while Open");
        };
        *phase = Phase::Sending;
        self.arm_send()
    }

    fn transition_to_receiving(&mut self) -> io::Result<()> {
        let ConnectionState::Open { phase, .. } = &mut self.state else {
            unreachable!("transitions only run while Open");
        };
        *phase = Phase::Receiving;
        self.arm_recv()
    }

    fn arm_recv(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket, .. } = &self.state else {
            panic!("connection requires an open socket");
        };
        socket.recv(&mut self.recv_completion, READ_CHUNK)
    }

    fn arm_send(&mut self) -> io::Result<()> {
        let ConnectionState::Open { socket, .. } = &self.state else {
            panic!("connection requires an open socket");
        };
        socket.send(
            &mut self.send_completion,
            self.proto.write_buf[self.proto.write_offset..].to_vec(),
        )
    }
}

fn classify(result: io::Result<()>) -> io::Result<ConnectionStep> {
    match result {
        Ok(()) => Ok(ConnectionStep::Continue),
        Err(err) if is_peer_disconnect(&err) => Ok(ConnectionStep::Close),
        Err(err) => Err(err),
    }
}

impl SlabEntry for Connection {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ConnectionState::Free { next },
            proto: ProtocolState::default(),
            recv_completion: RecvCompletion::new(),
            send_completion: SendCompletion::new(),
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
        if let ConnectionState::Open { socket, .. } =
            std::mem::replace(&mut self.state, ConnectionState::Free { next })
        {
            socket.close();
        }
        self.proto = ProtocolState::default();
        self.recv_completion = RecvCompletion::new();
        self.send_completion = SendCompletion::new();
    }
}

#[derive(Default)]
pub struct ProtocolState {
    pub read_buf: Vec<u8>,
    pub write_buf: Vec<u8>,
    pub write_offset: usize,
    pending_storage: Option<StorageRequest>,
    pub close_after_write: bool,
}

impl ProtocolState {
    pub fn has_pending_output(&self) -> bool {
        self.write_offset < self.write_buf.len()
    }

    pub fn finish_write(&mut self) {
        self.write_buf.clear();
        self.write_offset = 0;
    }

    fn queue_response(&mut self, data: &[u8]) {
        self.write_buf.extend_from_slice(data);
    }

    pub fn process_input(&mut self, store: &mut HashMap<Vec<u8>, Item>) {
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
                self.queue_response(format!(" {} {}\r\n", item.flags, item.value.len()).as_bytes());
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

pub struct Item {
    pub flags: u32,
    pub value: Vec<u8>,
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

pub fn is_peer_disconnect(err: &io::Error) -> bool {
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
