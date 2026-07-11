//! Windows IOCP backend.
//!
//! Every file and socket handle is associated with a single I/O completion
//! port at creation time. Submitting an operation leases a heap-allocated
//! [`OverlappedPacket`] (the `OVERLAPPED` plus a pointer back to the
//! caller-owned completion slot) to the kernel; `step` reclaims the packet
//! when `GetQueuedCompletionStatus` hands the `OVERLAPPED` pointer back and
//! stores the typed result into the completion.
//!
//! Synchronous operations (`fsync`, `stat`, `mkdir`) never touch the port;
//! they are executed inline when `step` drains the submit queue.

use std::{
    cell::RefCell,
    collections::VecDeque,
    io, mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::windows::ffi::OsStrExt,
    path::Path,
    ptr::{self, NonNull},
    rc::Rc,
};

use log::trace;

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_HANDLE_EOF, ERROR_IO_PENDING, FALSE, GENERIC_READ, GENERIC_WRITE,
    GetLastError, HANDLE, INVALID_HANDLE_VALUE, TRUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
    ADDRESS_FAMILY, AF_INET, AF_INET6, AcceptEx, INVALID_SOCKET, IPPROTO_IPV6, IPPROTO_TCP,
    IPV6_V6ONLY, LPFN_CONNECTEX, SIO_GET_EXTENSION_FUNCTION_POINTER, SO_REUSEADDR,
    SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCK_STREAM, SOCKADDR_IN, SOCKADDR_IN6,
    SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, TCP_NODELAY, WSA_FLAG_OVERLAPPED,
    WSABUF, WSACleanup, WSADATA, WSAGetLastError, WSAID_CONNECTEX, WSAIoctl, WSARecv, WSASend,
    WSASocketW, WSAStartup, bind as winsock_bind, closesocket, getsockname, listen, setsockopt,
};
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_ALWAYS, CreateDirectoryW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OVERLAPPED,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FlushFileBuffers, GetFileSizeEx,
    OPEN_ALWAYS, OPEN_EXISTING, ReadFile, TRUNCATE_EXISTING, WriteFile,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
};
use windows_sys::core::GUID;

use crate::{
    AcceptCompletion, AcceptOp, CompletionInner, ConnectCompletion, ConnectOp, FsyncCompletion,
    FsyncOp, IO, IOFile, IOLoop, IOSocket, MkdirCompletion, MkdirOp, OpenOptions, Operation,
    PReadCompletion, PReadOp, PWriteCompletion, PWriteOp, RecvCompletion, RecvOp, SendCompletion,
    SendOp, StatCompletion, StatOp,
};

/// Address buffer slot size required by `AcceptEx` for each of the local and
/// remote addresses.
const ACCEPT_ADDR_LEN: usize = mem::size_of::<SOCKADDR_STORAGE>() + 16;
const ACCEPT_ADDR_BUF_LEN: usize = 2 * ACCEPT_ADDR_LEN;

enum SocketKind {
    Listener,
    Stream,
}

struct OwnedHandle {
    handle: HANDLE,
}

impl OwnedHandle {
    fn new(handle: HANDLE) -> Self {
        Self { handle }
    }

    fn raw(&self) -> HANDLE {
        self.handle
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        trace!("close handle={:?}", self.handle);
        unsafe {
            CloseHandle(self.handle);
        }
    }
}

struct OwnedSocket {
    sock: SOCKET,
}

impl OwnedSocket {
    fn new(sock: SOCKET) -> Self {
        Self { sock }
    }

    fn raw(&self) -> SOCKET {
        self.sock
    }
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        trace!("close socket={}", self.sock);
        unsafe {
            closesocket(self.sock);
        }
    }
}

struct WindowsState {
    iocp: HANDLE,
    queued: VecDeque<NonNull<CompletionInner>>,
    inflight: usize,
}

impl Drop for WindowsState {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.iocp);
            WSACleanup();
        }
    }
}

/// Per-operation state leased to the kernel alongside the `OVERLAPPED`.
///
/// `Box::into_raw` transfers the packet to the kernel at submission and
/// `step` reclaims it with `Box::from_raw` when the completion port hands
/// the `OVERLAPPED` pointer back.
#[repr(C)]
struct OverlappedPacket {
    /// Must remain the first field so the `*mut OVERLAPPED` returned by the
    /// completion port can be cast back to `*mut OverlappedPacket`.
    overlapped: OVERLAPPED,
    completion: NonNull<CompletionInner>,
    payload: PacketPayload,
}

enum PacketPayload {
    None,
    /// Scatter/gather descriptor for `WSARecv`/`WSASend`; the descriptor
    /// array must stay valid for the duration of the operation.
    Buf(WSABUF),
    Accept(AcceptPayload),
}

struct AcceptPayload {
    /// The pre-created socket `AcceptEx` hands the inbound connection to.
    socket: SOCKET,
    /// Listener socket, needed for `SO_UPDATE_ACCEPT_CONTEXT` on completion.
    listener: SOCKET,
    /// Receives the local and remote addresses written by `AcceptEx`.
    addr_buf: Box<[u8; ACCEPT_ADDR_BUF_LEN]>,
}

fn new_packet(
    completion: NonNull<CompletionInner>,
    offset: u64,
    payload: PacketPayload,
) -> Box<OverlappedPacket> {
    let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
    overlapped.Anonymous.Anonymous.Offset = offset as u32;
    overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    Box::new(OverlappedPacket {
        overlapped,
        completion,
        payload,
    })
}

struct WindowsFile {
    state: Rc<RefCell<WindowsState>>,
    handle: Rc<OwnedHandle>,
}

struct WindowsSocket {
    state: Rc<RefCell<WindowsState>>,
    sock: Rc<RefCell<Option<Rc<OwnedSocket>>>>,
    kind: Rc<RefCell<Option<SocketKind>>>,
}

pub struct WindowsIO {
    state: Rc<RefCell<WindowsState>>,
}

/// Outcome of issuing one queued operation.
#[derive(PartialEq)]
enum Issued {
    /// The packet is with the kernel; a completion will arrive via the port.
    Pending,
    /// The operation finished inline and its completion has been stored.
    Done,
}

impl WindowsIO {
    pub fn new() -> io::Result<Self> {
        let mut wsa_data: WSADATA = unsafe { mem::zeroed() };
        let rc = unsafe { WSAStartup(0x0202, &mut wsa_data) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        let iocp = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1) };
        if iocp.is_null() {
            let err = io::Error::last_os_error();
            unsafe {
                WSACleanup();
            }
            return Err(err);
        }
        trace!("create iocp handle={iocp:?}");
        Ok(Self {
            state: Rc::new(RefCell::new(WindowsState {
                iocp,
                queued: VecDeque::new(),
                inflight: 0,
            })),
        })
    }

    fn open_handle(
        state: &Rc<RefCell<WindowsState>>,
        path: &Path,
        options: OpenOptions,
    ) -> io::Result<Rc<OwnedHandle>> {
        let mut access = 0;
        if options.read {
            access |= GENERIC_READ;
        }
        if options.write {
            access |= GENERIC_WRITE;
        }
        if access == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "open requires read and/or write access",
            ));
        }
        let disposition = match (options.create, options.truncate) {
            (true, true) => CREATE_ALWAYS,
            (true, false) => OPEN_ALWAYS,
            (false, true) => TRUNCATE_EXISTING,
            (false, false) => OPEN_EXISTING,
        };
        let wide = wide_path(path)?;
        trace!(
            "open path={} access=0x{access:x} disposition={disposition}",
            path.display()
        );
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                ptr::null(),
                disposition,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
                ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        trace!("open ok handle={handle:?}");
        let owned = Rc::new(OwnedHandle::new(handle));
        associate(state, handle)?;
        Ok(owned)
    }

    fn socket_fd(
        state: &Rc<RefCell<WindowsState>>,
        addr: SocketAddr,
    ) -> io::Result<Rc<OwnedSocket>> {
        let family = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        trace!("socket family={family} type=SOCK_STREAM overlapped");
        let sock = unsafe {
            WSASocketW(
                family as i32,
                SOCK_STREAM,
                IPPROTO_TCP,
                ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED,
            )
        };
        if sock == INVALID_SOCKET {
            return Err(wsa_error());
        }
        let owned = Rc::new(OwnedSocket::new(sock));
        if matches!(addr, SocketAddr::V6(_)) {
            set_sockopt_i32(sock, IPPROTO_IPV6, IPV6_V6ONLY, 0)?;
        }
        associate(state, sock as HANDLE)?;
        Ok(owned)
    }
}

impl IOFile for WindowsFile {
    fn pread(&self, c: &mut PReadCompletion, len: usize, offset: u64) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::PRead(PReadOp {
            fd: self.handle.raw() as usize,
            buf: vec![0_u8; len],
            offset,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn pwrite(&self, c: &mut PWriteCompletion, buf: Vec<u8>, offset: u64) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::PWrite(PWriteOp {
            fd: self.handle.raw() as usize,
            buf,
            offset,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn fsync(&self, c: &mut FsyncCompletion) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::Fsync(FsyncOp {
            fd: self.handle.raw() as usize,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn stat(&self, c: &mut StatCompletion) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::Stat(StatOp {
            fd: self.handle.raw() as usize,
        }));
        queue(&self.state, inner);
        Ok(())
    }
}

fn queue(state: &Rc<RefCell<WindowsState>>, c: &mut CompletionInner) {
    c.mark_submitted();
    state.borrow_mut().queued.push_back(NonNull::from(c));
}

impl IOSocket for WindowsSocket {
    fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        let sock = WindowsIO::socket_fd(&self.state, addr)?;
        trace!("bind setup socket={} addr={addr}", sock.raw());

        set_sockopt_i32(sock.raw(), SOL_SOCKET, SO_REUSEADDR, 1)?;

        let (storage, len) = socket_addr_to_raw(addr);
        trace!("bind socket={} addr={addr} len={len}", sock.raw());
        let rc = unsafe {
            winsock_bind(
                sock.raw(),
                (&storage as *const SOCKADDR_STORAGE).cast(),
                len,
            )
        };
        if rc == SOCKET_ERROR {
            return Err(wsa_error());
        }

        let rc = unsafe { listen(sock.raw(), 128) };
        if rc == SOCKET_ERROR {
            return Err(wsa_error());
        }

        *self.sock.borrow_mut() = Some(sock);
        *self.kind.borrow_mut() = Some(SocketKind::Listener);
        Ok(())
    }

    fn connect(&self, c: &mut ConnectCompletion, addr: SocketAddr) -> io::Result<()> {
        match &*self.kind.borrow() {
            Some(SocketKind::Listener) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "connect called on listener socket",
                ));
            }
            Some(SocketKind::Stream) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "connect called on already-initialized stream socket",
                ));
            }
            None => {}
        }

        let sock = WindowsIO::socket_fd(&self.state, addr)?;
        // ConnectEx requires an explicitly bound socket.
        let wildcard: SocketAddr = match addr {
            SocketAddr::V4(_) => SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
            SocketAddr::V6(_) => SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        };
        let (storage, len) = socket_addr_to_raw(wildcard);
        let rc = unsafe {
            winsock_bind(
                sock.raw(),
                (&storage as *const SOCKADDR_STORAGE).cast(),
                len,
            )
        };
        if rc == SOCKET_ERROR {
            return Err(wsa_error());
        }

        let raw = sock.raw();
        *self.sock.borrow_mut() = Some(sock);
        *self.kind.borrow_mut() = Some(SocketKind::Stream);

        let inner = c.inner_mut();
        inner.prepare(Operation::Connect(ConnectOp {
            fd: raw,
            addr,
            started: false,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn accept(&self, c: &mut AcceptCompletion) -> io::Result<()> {
        let sock = match &*self.kind.borrow() {
            Some(SocketKind::Listener) => self
                .sock
                .borrow()
                .as_ref()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "listener is closed"))?
                .raw(),
            Some(SocketKind::Stream) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "accept called on stream socket",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "accept called on closed socket",
                ));
            }
        };
        let inner = c.inner_mut();
        inner.prepare(Operation::Accept(AcceptOp { fd: sock }));
        queue(&self.state, inner);
        Ok(())
    }

    fn recv(&self, c: &mut RecvCompletion, len: usize) -> io::Result<()> {
        let sock = self
            .sock
            .borrow()
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "recv called on closed socket")
            })?
            .raw();
        match &*self.kind.borrow() {
            Some(SocketKind::Stream) => {}
            Some(SocketKind::Listener) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "recv called on listener socket",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "recv called on closed socket",
                ));
            }
        }

        let inner = c.inner_mut();
        inner.prepare(Operation::Recv(RecvOp {
            fd: sock,
            buf: vec![0_u8; len],
            flags: 0,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn send(&self, c: &mut SendCompletion, buf: Vec<u8>) -> io::Result<()> {
        let sock = self
            .sock
            .borrow()
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "send called on closed socket")
            })?
            .raw();
        match &*self.kind.borrow() {
            Some(SocketKind::Stream) => {}
            Some(SocketKind::Listener) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "send called on listener socket",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "send called on closed socket",
                ));
            }
        }

        let inner = c.inner_mut();
        inner.prepare(Operation::Send(SendOp {
            fd: sock,
            buf,
            flags: 0,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn set_nodelay(&self, on: bool) -> io::Result<()> {
        let sock = self
            .sock
            .borrow()
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "set_nodelay on closed socket")
            })?
            .raw();
        match &*self.kind.borrow() {
            Some(SocketKind::Stream) => {}
            Some(SocketKind::Listener) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "set_nodelay called on listener socket",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "set_nodelay on closed socket",
                ));
            }
        }
        set_sockopt_i32(sock, IPPROTO_TCP, TCP_NODELAY, on as i32)
    }

    fn close(&self) {
        self.sock.borrow_mut().take();
        self.kind.borrow_mut().take();
    }
}

impl IO for WindowsIO {
    fn open(&self, path: &Path, options: OpenOptions) -> io::Result<Box<dyn IOFile>> {
        Ok(Box::new(WindowsFile {
            state: self.state.clone(),
            handle: Self::open_handle(&self.state, path, options)?,
        }))
    }

    fn socket(&self) -> io::Result<Box<dyn IOSocket>> {
        Ok(Box::new(WindowsSocket {
            state: self.state.clone(),
            sock: Rc::new(RefCell::new(None)),
            kind: Rc::new(RefCell::new(None)),
        }))
    }

    fn mkdir(&self, c: &mut MkdirCompletion, path: &Path, mode: u32) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::Mkdir(MkdirOp {
            path: wide_path(path)?,
            mode,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "windows"
    }
}

impl IOLoop for WindowsIO {
    fn step(&self) -> io::Result<bool> {
        let mut progressed = false;

        let queued_len = self.state.borrow().queued.len();
        for _ in 0..queued_len {
            let completion_ptr = self
                .state
                .borrow_mut()
                .queued
                .pop_front()
                .expect("pending length checked above");

            if issue_completion(&self.state, completion_ptr) == Issued::Pending {
                self.state.borrow_mut().inflight += 1;
            }
            progressed = true;
        }

        loop {
            let iocp = self.state.borrow().iocp;
            let mut bytes = 0_u32;
            let mut key = 0_usize;
            let mut overlapped: *mut OVERLAPPED = ptr::null_mut();
            let ok = unsafe {
                GetQueuedCompletionStatus(iocp, &mut bytes, &mut key, &mut overlapped, 0)
            };
            let error = if ok == FALSE {
                unsafe { GetLastError() }
            } else {
                0
            };

            let Some(overlapped) = NonNull::new(overlapped) else {
                if ok == FALSE && error != WAIT_TIMEOUT {
                    return Err(io::Error::from_raw_os_error(error as i32));
                }
                break;
            };

            let packet =
                unsafe { Box::from_raw(overlapped.as_ptr().cast::<OverlappedPacket>()) };
            {
                let mut state = self.state.borrow_mut();
                state.inflight = state
                    .inflight
                    .checked_sub(1)
                    .expect("completion port retired more packets than submitted");
            }
            let result = if ok == TRUE {
                Ok(bytes as usize)
            } else {
                Err(io::Error::from_raw_os_error(error as i32))
            };
            dispatch_complete(&self.state, *packet, result);
            progressed = true;
        }

        Ok(progressed)
    }
}

/// Issues the operation armed in the completion `completion_ptr` points at.
///
/// Asynchronous operations lease an [`OverlappedPacket`] to the kernel and
/// return [`Issued::Pending`]; synchronous operations and submission
/// failures store the typed result immediately and return [`Issued::Done`].
///
/// SAFETY in each completing arm: the pointed-at inner is the inner of the
/// typed completion that armed the matching `Operation` variant. The IO
/// methods that arm a slot take `&mut <kind>Completion` and only set the
/// matching `Operation`, so the cast back is sound.
fn issue_completion(
    state: &Rc<RefCell<WindowsState>>,
    mut completion_ptr: NonNull<CompletionInner>,
) -> Issued {
    let completion = unsafe { completion_ptr.as_mut() };
    match completion.operation_mut() {
        Operation::PRead(op) => {
            let handle = op.fd as HANDLE;
            let buf_ptr = op.buf.as_mut_ptr();
            let buf_len = op.buf.len() as u32;
            let packet = new_packet(completion_ptr, op.offset, PacketPayload::None);
            let overlapped = Box::into_raw(packet).cast::<OVERLAPPED>();
            let ok =
                unsafe { ReadFile(handle, buf_ptr.cast(), buf_len, ptr::null_mut(), overlapped) };
            finish_issue(state, ok == TRUE, overlapped)
        }
        Operation::PWrite(op) => {
            let handle = op.fd as HANDLE;
            let buf_ptr = op.buf.as_ptr();
            let buf_len = op.buf.len() as u32;
            let packet = new_packet(completion_ptr, op.offset, PacketPayload::None);
            let overlapped = Box::into_raw(packet).cast::<OVERLAPPED>();
            let ok =
                unsafe { WriteFile(handle, buf_ptr.cast(), buf_len, ptr::null_mut(), overlapped) };
            finish_issue(state, ok == TRUE, overlapped)
        }
        Operation::Recv(op) => {
            let sock = op.fd as SOCKET;
            let wsabuf = WSABUF {
                len: op.buf.len() as u32,
                buf: op.buf.as_mut_ptr(),
            };
            let mut packet = new_packet(completion_ptr, 0, PacketPayload::Buf(wsabuf));
            let buf_ptr = match &mut packet.payload {
                PacketPayload::Buf(buf) => buf as *mut WSABUF,
                _ => unreachable!(),
            };
            let overlapped = Box::into_raw(packet).cast::<OVERLAPPED>();
            let mut flags = 0_u32;
            let rc = unsafe {
                WSARecv(
                    sock,
                    buf_ptr,
                    1,
                    ptr::null_mut(),
                    &mut flags,
                    overlapped,
                    None,
                )
            };
            finish_issue(state, rc == 0, overlapped)
        }
        Operation::Send(op) => {
            let sock = op.fd as SOCKET;
            let wsabuf = WSABUF {
                len: op.buf.len() as u32,
                buf: op.buf.as_ptr().cast_mut(),
            };
            let mut packet = new_packet(completion_ptr, 0, PacketPayload::Buf(wsabuf));
            let buf_ptr = match &mut packet.payload {
                PacketPayload::Buf(buf) => buf as *mut WSABUF,
                _ => unreachable!(),
            };
            let overlapped = Box::into_raw(packet).cast::<OVERLAPPED>();
            let rc = unsafe { WSASend(sock, buf_ptr, 1, ptr::null_mut(), 0, overlapped, None) };
            finish_issue(state, rc == 0, overlapped)
        }
        Operation::Accept(op) => {
            let listener = op.fd as SOCKET;
            let family = match socket_family(listener) {
                Ok(family) => family,
                Err(err) => {
                    unsafe { AcceptCompletion::from_inner_mut(completion) }.complete(Err(err));
                    return Issued::Done;
                }
            };
            let accept_sock = unsafe {
                WSASocketW(
                    family as i32,
                    SOCK_STREAM,
                    IPPROTO_TCP,
                    ptr::null(),
                    0,
                    WSA_FLAG_OVERLAPPED,
                )
            };
            if accept_sock == INVALID_SOCKET {
                unsafe { AcceptCompletion::from_inner_mut(completion) }
                    .complete(Err(wsa_error()));
                return Issued::Done;
            }
            let mut packet = new_packet(
                completion_ptr,
                0,
                PacketPayload::Accept(AcceptPayload {
                    socket: accept_sock,
                    listener,
                    addr_buf: Box::new([0_u8; ACCEPT_ADDR_BUF_LEN]),
                }),
            );
            let addr_buf_ptr = match &mut packet.payload {
                PacketPayload::Accept(payload) => payload.addr_buf.as_mut_ptr(),
                _ => unreachable!(),
            };
            let overlapped = Box::into_raw(packet).cast::<OVERLAPPED>();
            let mut received = 0_u32;
            let ok = unsafe {
                AcceptEx(
                    listener,
                    accept_sock,
                    addr_buf_ptr.cast(),
                    0,
                    ACCEPT_ADDR_LEN as u32,
                    ACCEPT_ADDR_LEN as u32,
                    &mut received,
                    overlapped,
                )
            };
            // A failed submission is dispatched with the packet, so the
            // error path below closes the pre-created accept socket.
            finish_issue(state, ok == TRUE, overlapped)
        }
        Operation::Connect(op) => {
            let sock = op.fd as SOCKET;
            let connect_ex = match load_connect_ex(sock) {
                Ok(connect_ex) => connect_ex,
                Err(err) => {
                    unsafe { ConnectCompletion::from_inner_mut(completion) }.complete(Err(err));
                    return Issued::Done;
                }
            };
            let (storage, len) = socket_addr_to_raw(op.addr);
            let packet = new_packet(completion_ptr, 0, PacketPayload::None);
            let overlapped = Box::into_raw(packet).cast::<OVERLAPPED>();
            let mut sent = 0_u32;
            let ok = unsafe {
                connect_ex(
                    sock,
                    (&storage as *const SOCKADDR_STORAGE).cast(),
                    len,
                    ptr::null(),
                    0,
                    &mut sent,
                    overlapped,
                )
            };
            finish_issue(state, ok == TRUE, overlapped)
        }
        Operation::Fsync(op) => {
            let result = if unsafe { FlushFileBuffers(op.fd as HANDLE) } == FALSE {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            };
            unsafe { FsyncCompletion::from_inner_mut(completion) }.complete(result);
            Issued::Done
        }
        Operation::Stat(op) => {
            let mut size = 0_i64;
            let result = if unsafe { GetFileSizeEx(op.fd as HANDLE, &mut size) } == FALSE {
                Err(io::Error::last_os_error())
            } else {
                Ok(size as u64)
            };
            unsafe { StatCompletion::from_inner_mut(completion) }.complete(result);
            Issued::Done
        }
        Operation::Mkdir(op) => {
            // Windows has no direct mode equivalent; directories inherit
            // the parent's security descriptor.
            let _ = op.mode;
            let result = if unsafe { CreateDirectoryW(op.path.as_ptr(), ptr::null()) } == FALSE {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            };
            unsafe { MkdirCompletion::from_inner_mut(completion) }.complete(result);
            Issued::Done
        }
        Operation::Nop => Issued::Done,
    }
}

/// Shared post-submission step for overlapped calls. A synchronous success
/// still posts a packet to the completion port, so both success and
/// `ERROR_IO_PENDING` leave the packet with the kernel. Any other error
/// reclaims the packet and fails the completion.
fn finish_issue(
    state: &Rc<RefCell<WindowsState>>,
    submitted: bool,
    overlapped: *mut OVERLAPPED,
) -> Issued {
    if submitted {
        return Issued::Pending;
    }
    let error = unsafe { GetLastError() };
    if error == ERROR_IO_PENDING {
        return Issued::Pending;
    }
    let packet = unsafe { Box::from_raw(overlapped.cast::<OverlappedPacket>()) };
    dispatch_complete(state, *packet, Err(io::Error::from_raw_os_error(error as i32)));
    Issued::Done
}

/// Stores the typed result for the operation the reclaimed `packet` belongs
/// to directly into the wrapping typed completion.
///
/// SAFETY in each arm: the packet's completion pointer is the inner of the
/// typed completion that armed the matching `Operation` variant. The IO
/// methods that arm a slot take `&mut <kind>Completion` and only set the
/// matching `Operation`, so the cast back is sound.
fn dispatch_complete(
    state: &Rc<RefCell<WindowsState>>,
    packet: OverlappedPacket,
    result: io::Result<usize>,
) {
    let mut completion_ptr = packet.completion;
    let c = unsafe { completion_ptr.as_mut() };
    match c.operation() {
        Operation::Accept(_) => {
            let PacketPayload::Accept(payload) = packet.payload else {
                unreachable!("accept packet must carry an accept payload")
            };
            let r = match result.and_then(|_| finish_accept(state, &payload)) {
                Ok(sock) => Ok(Box::new(WindowsSocket {
                    state: state.clone(),
                    sock: Rc::new(RefCell::new(Some(Rc::new(sock)))),
                    kind: Rc::new(RefCell::new(Some(SocketKind::Stream))),
                }) as Box<dyn IOSocket>),
                Err(err) => {
                    unsafe {
                        closesocket(payload.socket);
                    }
                    Err(err)
                }
            };
            unsafe { AcceptCompletion::from_inner_mut(c) }.complete(r);
        }
        Operation::Connect(op) => {
            let sock = op.fd as SOCKET;
            let r = result.and_then(|_| {
                // Transition the socket into the fully connected state so
                // getpeername/shutdown work as they would after connect().
                let rc = unsafe {
                    setsockopt(sock, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, ptr::null(), 0)
                };
                if rc == SOCKET_ERROR {
                    Err(wsa_error())
                } else {
                    Ok(())
                }
            });
            unsafe { ConnectCompletion::from_inner_mut(c) }.complete(r);
        }
        Operation::Recv(_) => {
            let r = match result {
                Ok(n) => {
                    let Operation::Recv(op) = c.operation_mut() else {
                        unreachable!()
                    };
                    op.buf.truncate(n);
                    Ok(mem::take(&mut op.buf))
                }
                Err(err) => Err(err),
            };
            unsafe { RecvCompletion::from_inner_mut(c) }.complete(r);
        }
        Operation::Send(_) => {
            let r = result;
            unsafe { SendCompletion::from_inner_mut(c) }.complete(r);
        }
        Operation::PRead(_) => {
            // Reading at or past end-of-file fails with ERROR_HANDLE_EOF;
            // surface it as a short (empty) read to match pread(2)
            // semantics on the Unix backends.
            let n = match result {
                Ok(n) => Ok(n),
                Err(err) if err.raw_os_error() == Some(ERROR_HANDLE_EOF as i32) => Ok(0),
                Err(err) => Err(err),
            };
            let r = match n {
                Ok(n) => {
                    let Operation::PRead(op) = c.operation_mut() else {
                        unreachable!()
                    };
                    op.buf.truncate(n);
                    Ok(mem::take(&mut op.buf))
                }
                Err(err) => Err(err),
            };
            unsafe { PReadCompletion::from_inner_mut(c) }.complete(r);
        }
        Operation::PWrite(_) => {
            let r = result;
            unsafe { PWriteCompletion::from_inner_mut(c) }.complete(r);
        }
        Operation::Fsync(_) | Operation::Stat(_) | Operation::Mkdir(_) | Operation::Nop => {
            unreachable!("synchronous operations never reach the completion port")
        }
    }
}

/// Finishes an accepted connection: applies the listener context and binds
/// the new socket to the completion port. Ownership of the raw socket is
/// only taken on success; the caller closes it on error.
fn finish_accept(
    state: &Rc<RefCell<WindowsState>>,
    payload: &AcceptPayload,
) -> io::Result<OwnedSocket> {
    let rc = unsafe {
        setsockopt(
            payload.socket,
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            (&payload.listener as *const SOCKET).cast(),
            mem::size_of::<SOCKET>() as i32,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    associate(state, payload.socket as HANDLE)?;
    Ok(OwnedSocket::new(payload.socket))
}

/// Binds `handle` to the backend's completion port.
fn associate(state: &Rc<RefCell<WindowsState>>, handle: HANDLE) -> io::Result<()> {
    let iocp = state.borrow().iocp;
    let port = unsafe { CreateIoCompletionPort(handle, iocp, 0, 0) };
    if port.is_null() {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn load_connect_ex(
    sock: SOCKET,
) -> io::Result<
    unsafe extern "system" fn(
        SOCKET,
        *const windows_sys::Win32::Networking::WinSock::SOCKADDR,
        i32,
        *const core::ffi::c_void,
        u32,
        *mut u32,
        *mut OVERLAPPED,
    ) -> windows_sys::core::BOOL,
> {
    let guid = WSAID_CONNECTEX;
    let mut connect_ex: LPFN_CONNECTEX = None;
    let mut bytes = 0_u32;
    let rc = unsafe {
        WSAIoctl(
            sock,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            (&guid as *const GUID).cast(),
            mem::size_of::<GUID>() as u32,
            (&mut connect_ex as *mut LPFN_CONNECTEX).cast(),
            mem::size_of::<LPFN_CONNECTEX>() as u32,
            &mut bytes,
            ptr::null_mut(),
            None,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    connect_ex
        .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "ConnectEx is unavailable"))
}

fn socket_family(sock: SOCKET) -> io::Result<ADDRESS_FAMILY> {
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<SOCKADDR_STORAGE>() as i32;
    let rc =
        unsafe { getsockname(sock, (&mut storage as *mut SOCKADDR_STORAGE).cast(), &mut len) };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok(storage.ss_family)
}

fn set_sockopt_i32(sock: SOCKET, level: i32, optname: i32, value: i32) -> io::Result<()> {
    let rc = unsafe {
        setsockopt(
            sock,
            level,
            optname,
            (&value as *const i32).cast(),
            mem::size_of::<i32>() as i32,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok(())
}

fn wsa_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        ));
    }
    wide.push(0);
    Ok(wide)
}

fn socket_addr_to_raw(addr: SocketAddr) -> (SOCKADDR_STORAGE, i32) {
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    match addr {
        SocketAddr::V4(addr) => {
            let mut sockaddr: SOCKADDR_IN = unsafe { mem::zeroed() };
            sockaddr.sin_family = AF_INET;
            sockaddr.sin_port = addr.port().to_be();
            sockaddr.sin_addr.S_un.S_addr = u32::from_ne_bytes(addr.ip().octets());
            unsafe {
                ptr::write((&mut storage as *mut SOCKADDR_STORAGE).cast(), sockaddr);
            }
            (storage, mem::size_of::<SOCKADDR_IN>() as i32)
        }
        SocketAddr::V6(addr) => {
            let mut sockaddr: SOCKADDR_IN6 = unsafe { mem::zeroed() };
            sockaddr.sin6_family = AF_INET6;
            sockaddr.sin6_port = addr.port().to_be();
            sockaddr.sin6_flowinfo = addr.flowinfo();
            sockaddr.sin6_addr.u.Byte = addr.ip().octets();
            sockaddr.Anonymous.sin6_scope_id = addr.scope_id();
            unsafe {
                ptr::write((&mut storage as *mut SOCKADDR_STORAGE).cast(), sockaddr);
            }
            (storage, mem::size_of::<SOCKADDR_IN6>() as i32)
        }
    }
}
