//! The client state machine.

use std::{alloc::Allocator, io, io::Write as _, net::SocketAddr};

use betelgeuse::{
    ConnectCompletion, IO, IOHandle, IOSocket, RecvCompletion, SendCompletion, op::StepOp,
};

use crate::{
    body::{BodyDecoder, BodyStatus},
    request::Request,
    response::{DEFAULT_MAX_HEAD_BYTES, Head, Response, parse_head},
};

/// Default number of bytes requested per `recv`.
const DEFAULT_RECV_CHUNK: usize = 8 * 1024;

/// Default ceiling on the decoded body plus the bytes still being framed.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// HTTP request processing state.
enum State {
    /// Nothing submitted yet. The connect is armed on the first `step`, once
    /// the completion slots have stopped moving.
    Init,
    /// Waiting for the connection to be established.
    Connecting,
    /// Writing the encoded request.
    Sending,
    /// Reading until the response head is complete.
    ReceivingHead,
    /// Reading until the body framing says the response is complete.
    ReceivingBody,
    /// The response has been returned, or the request has failed.
    Done,
}

/// A single HTTP request, driven by the caller.
///
/// Each `step` consumes at most one completion and arms at most one new
/// operation, returning the response once the body is complete. The caller
/// interleaves it with the I/O loop:
///
/// ```text
/// loop {
///     if let Some(response) = fetch.step()? { break response; }
///     io_loop.step()?;
/// }
/// ```
///
/// # Memory
///
/// `A` owns everything the request buffers: the encoded request, the receive
/// buffer, the decoded body, and the response handed back. The one exception
/// is the transfer buffer each `send` and `recv` uses, which belongs to the
/// I/O backend: [`IOSocket`] moves a plain `Vec<u8>` across that boundary, so
/// the backend allocates it in the global allocator.
///
/// # Lifetime rules
///
/// A `Fetch` owns the completion slots the backend writes into, so it must not
/// move once it has been stepped, and must not be dropped while an operation is
/// in flight. Both rules are satisfied by the loop above: no operation is in
/// flight when `step` returns a response or an error, which is where a caller
/// would move or drop it.
pub struct Fetch<A: Allocator + Clone> {
    allocator: A,
    socket: Box<dyn IOSocket>,
    addr: SocketAddr,
    state: State,
    connect: ConnectCompletion,
    send: SendCompletion,
    recv: RecvCompletion,
    tx: Vec<u8, A>,
    tx_offset: usize,
    rx: Vec<u8, A>,
    head: Option<Head<A>>,
    decoder: BodyDecoder,
    body: Vec<u8, A>,
    head_request: bool,
    recv_chunk: usize,
    max_head_bytes: usize,
    max_response_bytes: usize,
    nodelay: bool,
}

impl<A: Allocator + Clone> Fetch<A> {
    /// Prepares a request to `addr`, buffering everything it owns in
    /// `allocator`.
    ///
    /// The request is encoded here, so a malformed method, target, or header
    /// field fails before any connection is made. Nothing is submitted until
    /// the first [`Fetch::step`].
    pub fn new(
        allocator: A,
        io: &IOHandle,
        addr: SocketAddr,
        request: Request<A>,
    ) -> io::Result<Self> {
        let tx = request.encode(&host_of(addr, allocator.clone()))?;
        Ok(Self {
            socket: io.socket()?,
            addr,
            state: State::Init,
            connect: ConnectCompletion::new(),
            send: SendCompletion::new(),
            recv: RecvCompletion::new(),
            tx,
            tx_offset: 0,
            rx: Vec::new_in(allocator.clone()),
            head: None,
            decoder: BodyDecoder::Empty,
            body: Vec::new_in(allocator.clone()),
            head_request: request.is_head(),
            recv_chunk: DEFAULT_RECV_CHUNK,
            max_head_bytes: DEFAULT_MAX_HEAD_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            nodelay: true,
            allocator,
        })
    }

    /// Sets how many bytes each `recv` asks for. Defaults to 8 KiB.
    pub fn with_recv_chunk(mut self, bytes: usize) -> Self {
        assert!(bytes > 0, "recv chunk must be non-zero");
        self.recv_chunk = bytes;
        self
    }

    /// Sets the ceiling on the size of the response head before the request
    /// fails. Defaults to 64 KiB.
    pub fn with_max_head_bytes(mut self, bytes: usize) -> Self {
        self.max_head_bytes = bytes;
        self
    }

    /// Sets the ceiling on how much response data is buffered before the
    /// request fails. Defaults to 8 MiB.
    pub fn with_max_response_bytes(mut self, bytes: usize) -> Self {
        self.max_response_bytes = bytes;
        self
    }

    /// Sets `TCP_NODELAY` on the connection. Defaults to true.
    pub fn with_nodelay(mut self, on: bool) -> Self {
        self.nodelay = on;
        self
    }

    /// Returns the peer address this request is being sent to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the allocator this request buffers in.
    pub fn allocator(&self) -> &A {
        &self.allocator
    }

    /// True once the response has been returned or the request has failed.
    pub fn is_done(&self) -> bool {
        matches!(self.state, State::Done)
    }

    /// Advances the request by one step.
    ///
    /// Returns `Ok(None)` while the request is still in flight, `Ok(Some(_))`
    /// once the response is complete, and `Err` on a transport or protocol
    /// failure. After either terminal outcome the socket is closed and further
    /// steps return `Ok(None)`.
    pub fn step(&mut self) -> io::Result<Option<Response<A>>> {
        let result = self.advance();
        match &result {
            Ok(Some(_)) | Err(_) => self.finish(),
            Ok(None) => {}
        }
        result
    }

    fn advance(&mut self) -> io::Result<Option<Response<A>>> {
        match self.state {
            State::Init => {
                self.socket.connect(&mut self.connect, self.addr)?;
                self.state = State::Connecting;
                Ok(None)
            }
            State::Connecting => self.step_connecting(),
            State::Sending => self.step_sending(),
            State::ReceivingHead | State::ReceivingBody => self.step_receiving(),
            State::Done => Ok(None),
        }
    }

    fn step_connecting(&mut self) -> io::Result<Option<Response<A>>> {
        let Some(result) = self.connect.take_result() else {
            return Ok(None);
        };
        result?;

        if self.nodelay {
            self.socket.set_nodelay(true)?;
        }
        self.state = State::Sending;
        self.arm_send()?;
        Ok(None)
    }

    fn step_sending(&mut self) -> io::Result<Option<Response<A>>> {
        let Some(result) = self.send.take_result() else {
            return Ok(None);
        };
        let sent = result?;
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "peer accepted no request bytes",
            ));
        }

        self.tx_offset += sent;
        if self.tx_offset < self.tx.len() {
            return self.arm_send().map(|()| None);
        }

        self.state = State::ReceivingHead;
        self.arm_recv()?;
        Ok(None)
    }

    fn step_receiving(&mut self) -> io::Result<Option<Response<A>>> {
        let Some(result) = self.recv.take_result() else {
            return Ok(None);
        };
        let bytes = result?;

        if bytes.is_empty() {
            return self.at_eof().map(Some);
        }
        self.rx.extend_from_slice(&bytes);
        self.check_budget()?;

        if let Some(response) = self.consume_rx()? {
            return Ok(Some(response));
        }
        self.arm_recv()?;
        Ok(None)
    }

    /// Parses as much of the buffered bytes as possible, returning the response
    /// once the body is complete.
    fn consume_rx(&mut self) -> io::Result<Option<Response<A>>> {
        if matches!(self.state, State::ReceivingHead) {
            let Some(head) = self.take_head()? else {
                return Ok(None);
            };
            self.decoder = BodyDecoder::for_response(&head, self.head_request)?;
            self.head = Some(head);
            self.state = State::ReceivingBody;
        }

        let (consumed, status) = self.decoder.decode(&self.rx, &mut self.body)?;
        self.rx.drain(..consumed);
        self.check_budget()?;
        match status {
            BodyStatus::NeedMore => Ok(None),
            BodyStatus::Complete => Ok(Some(self.take_response())),
        }
    }

    /// Parses the response head, skipping any interim 1xx responses that
    /// precede the real one.
    fn take_head(&mut self) -> io::Result<Option<Head<A>>> {
        loop {
            let Some((head, consumed)) =
                parse_head(&self.rx, self.max_head_bytes, self.allocator.clone())?
            else {
                return Ok(None);
            };
            self.rx.drain(..consumed);
            if head.status >= 200 {
                return Ok(Some(head));
            }
        }
    }

    /// Handles a clean close from the peer, which completes a close-delimited
    /// body and truncates anything else.
    fn at_eof(&mut self) -> io::Result<Response<A>> {
        match self.state {
            State::ReceivingHead => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before the response head arrived",
            )),
            _ => {
                self.decoder.at_eof()?;
                Ok(self.take_response())
            }
        }
    }

    fn take_response(&mut self) -> Response<A> {
        let head = self.head.take().expect("head is parsed before the body");
        let body = std::mem::replace(&mut self.body, Vec::new_in(self.allocator.clone()));
        Response::new(head.status, head.reason, head.headers, body)
    }

    /// Fails the request rather than buffering an unbounded response.
    fn check_budget(&self) -> io::Result<()> {
        if self.body.len() + self.rx.len() > self.max_response_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response exceeds the configured size limit",
            ));
        }
        Ok(())
    }

    /// Hands the unsent tail of the request to the backend.
    ///
    /// The copy is the I/O boundary, not a stray allocation: [`IOSocket::send`]
    /// takes ownership of a globally allocated `Vec<u8>` for the duration of
    /// the operation, and returns it through the completion.
    fn arm_send(&mut self) -> io::Result<()> {
        let pending = self.tx[self.tx_offset..].to_vec();
        self.socket.send(&mut self.send, pending)
    }

    fn arm_recv(&mut self) -> io::Result<()> {
        self.socket.recv(&mut self.recv, self.recv_chunk)
    }

    /// Releases the connection. No operation is in flight at this point: every
    /// terminal transition happens while consuming a completion result.
    fn finish(&mut self) {
        self.state = State::Done;
        self.socket.close();
    }
}

impl<A: Allocator + Clone> StepOp for Fetch<A> {
    type Output = Response<A>;

    fn step(&mut self) -> io::Result<Option<Self::Output>> {
        Fetch::step(self)
    }
}

/// Formats `addr` as the default `Host` field value.
///
/// `SocketAddr::to_string` would allocate a global `String`, so the text is
/// rendered into the caller's allocator instead.
fn host_of<A: Allocator>(addr: SocketAddr, allocator: A) -> HostText<A> {
    let mut bytes = Vec::new_in(allocator);
    // Writing to a `Vec` never fails.
    let _ = write!(bytes, "{addr}");
    HostText { bytes }
}

/// A rendered socket address, borrowed as a `&str` for encoding.
struct HostText<A: Allocator> {
    bytes: Vec<u8, A>,
}

impl<A: Allocator> std::ops::Deref for HostText<A> {
    type Target = str;

    fn deref(&self) -> &str {
        std::str::from_utf8(&self.bytes).expect("a socket address renders as ascii")
    }
}
