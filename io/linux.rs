use std::{
    cell::RefCell,
    collections::VecDeque,
    ffi::CString,
    io,
    mem::{self, MaybeUninit},
    net::SocketAddr,
    os::{fd::RawFd, unix::ffi::OsStrExt},
    path::Path,
    ptr::NonNull,
    rc::Rc,
};

use io_uring::{IoUring, opcode, squeue, types};
use log::trace;

use crate::{
    AcceptCompletion, AcceptOp, CompletionInner, ConnectCompletion, ConnectOp, FsyncCompletion,
    FsyncOp, IO, IOFile, IOLoop, IOSocket, MkdirCompletion, MkdirOp, OpenOptions, Operation,
    PReadCompletion, PReadOp, PWriteCompletion, PWriteOp, RecvCompletion, RecvOp, SendCompletion,
    SendOp, SizeCompletion, SizeOp,
};

enum SocketKind {
    Listener,
    Stream,
}

struct OwnedFd {
    fd: RawFd,
}

impl OwnedFd {
    fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    fn raw(&self) -> RawFd {
        self.fd
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        trace!("close fd={}", self.fd);
        unsafe {
            libc::close(self.fd);
        }
    }
}

struct IoUringState {
    ring: IoUring,
    queued: VecDeque<NonNull<CompletionInner>>,
    inflight: usize,
}

struct IoUringFile {
    state: Rc<RefCell<IoUringState>>,
    fd: Rc<OwnedFd>,
}

struct IoUringSocket {
    state: Rc<RefCell<IoUringState>>,
    fd: Rc<RefCell<Option<Rc<OwnedFd>>>>,
    kind: Rc<RefCell<Option<SocketKind>>>,
}

pub struct IoUringIO {
    state: Rc<RefCell<IoUringState>>,
}

impl IoUringIO {
    const RING_ENTRIES: u32 = 256;
    const MIN_RING_ENTRIES: u32 = 8;

    pub fn new() -> io::Result<Self> {
        let mut entries = Self::RING_ENTRIES;

        loop {
            trace!("create io_uring ring entries={entries}");
            match IoUring::new(entries) {
                Ok(ring) => {
                    return Ok(Self {
                        state: Rc::new(RefCell::new(IoUringState {
                            ring,
                            queued: VecDeque::new(),
                            inflight: 0,
                        })),
                    });
                }
                Err(err)
                    if err.kind() == io::ErrorKind::OutOfMemory
                        && entries > Self::MIN_RING_ENTRIES =>
                {
                    trace!("io_uring creation failed with {err}; retrying with fewer entries");
                    entries /= 2;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn should_retry(completion: &CompletionInner, result: i32) -> bool {
        let errno = -result;
        if result >= 0 {
            return false;
        }
        match completion.operation() {
            Operation::Accept(_) | Operation::Recv(_) | Operation::Send(_) => {
                errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::EINTR
            }
            Operation::Connect(_) => errno == libc::EINTR,
            Operation::PRead(_)
            | Operation::PWrite(_)
            | Operation::Fsync(_)
            | Operation::Mkdir(_) => errno == libc::EINTR,
            Operation::Size(_) | Operation::Nop => false,
        }
    }

    fn prepare_entry(c: &mut CompletionInner) -> squeue::Entry {
        let entry = match c.operation_mut() {
            Operation::Accept(op) => {
                opcode::Accept::new(types::Fd(op.fd), std::ptr::null_mut(), std::ptr::null_mut())
                    .build()
            }
            Operation::Connect(op) => {
                let (storage, len) = socket_addr_to_raw(op.addr);
                // Store address bytes in the op to keep them alive for io_uring.
                op.addr_storage = storage;
                op.addr_len = len;
                opcode::Connect::new(
                    types::Fd(op.fd),
                    (&op.addr_storage as *const libc::sockaddr_storage).cast(),
                    op.addr_len,
                )
                .build()
            }
            Operation::Recv(op) => {
                opcode::Recv::new(types::Fd(op.fd), op.buf.as_mut_ptr(), op.buf.len() as u32)
                    .flags(op.flags)
                    .build()
            }
            Operation::Send(op) => {
                opcode::Send::new(types::Fd(op.fd), op.buf.as_ptr(), op.buf.len() as u32)
                    .flags(op.flags)
                    .build()
            }
            Operation::PRead(op) => {
                opcode::Read::new(types::Fd(op.fd), op.buf.as_mut_ptr(), op.buf.len() as u32)
                    .offset(op.offset)
                    .build()
            }
            Operation::PWrite(op) => {
                opcode::Write::new(types::Fd(op.fd), op.buf.as_ptr(), op.buf.len() as u32)
                    .offset(op.offset)
                    .build()
            }
            Operation::Fsync(op) => opcode::Fsync::new(types::Fd(op.fd)).build(),
            Operation::Mkdir(op) => {
                opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), op.path.as_ptr())
                    .mode(op.mode)
                    .build()
            }
            Operation::Size(_) => {
                unreachable!("Size is handled inline; should never reach prepare_entry")
            }
            Operation::Nop => unreachable!("Nop cannot be queued"),
        };
        entry.user_data(c as *mut CompletionInner as u64)
    }

    /// Stores the typed result for the operation armed in `c` directly into
    /// the wrapping typed completion.
    ///
    /// SAFETY in each arm: `c` is the inner of the typed completion that
    /// armed the matching `Operation` variant. The IO methods that arm a
    /// slot take `&mut <kind>Completion` and only set the matching
    /// `Operation`, so the cast back is sound.
    fn dispatch_complete(state: &Rc<RefCell<IoUringState>>, c: &mut CompletionInner, result: i32) {
        match c.operation() {
            Operation::Accept(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    Ok(Box::new(IoUringSocket {
                        state: state.clone(),
                        fd: Rc::new(RefCell::new(Some(Rc::new(OwnedFd::new(result as RawFd))))),
                        kind: Rc::new(RefCell::new(Some(SocketKind::Stream))),
                    }) as Box<dyn IOSocket>)
                };
                unsafe { AcceptCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Connect(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    Ok(())
                };
                unsafe { ConnectCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Recv(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    let Operation::Recv(op) = c.operation_mut() else {
                        unreachable!()
                    };
                    op.buf.truncate(result as usize);
                    Ok(mem::take(&mut op.buf))
                };
                unsafe { RecvCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Send(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    Ok(result as usize)
                };
                unsafe { SendCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::PRead(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    let Operation::PRead(op) = c.operation_mut() else {
                        unreachable!()
                    };
                    op.buf.truncate(result as usize);
                    Ok(mem::take(&mut op.buf))
                };
                unsafe { PReadCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::PWrite(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    Ok(result as usize)
                };
                unsafe { PWriteCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Fsync(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    Ok(())
                };
                unsafe { FsyncCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Size(_) => {
                let r = {
                    let Operation::Size(op) = c.operation_mut() else {
                        unreachable!()
                    };
                    let mut stat = MaybeUninit::<libc::stat>::uninit();
                    if unsafe { libc::fstat(op.fd, stat.as_mut_ptr()) } < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        let stat = unsafe { stat.assume_init() };
                        Ok(stat.st_size as u64)
                    }
                };
                unsafe { SizeCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Mkdir(_) => {
                let r = if result < 0 {
                    Err(io_uring_err(result))
                } else {
                    Ok(())
                };
                unsafe { MkdirCompletion::from_inner_mut(c) }.complete(r);
            }
            Operation::Nop => {}
        }
    }

    fn open_fd(path: &Path, options: OpenOptions) -> io::Result<Rc<OwnedFd>> {
        let path = c_string(path)?;
        let mut flags = libc::O_CLOEXEC;
        match (options.read, options.write) {
            (true, true) => flags |= libc::O_RDWR,
            (true, false) => flags |= libc::O_RDONLY,
            (false, true) => flags |= libc::O_WRONLY,
            (false, false) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "open requires read and/or write access",
                ));
            }
        }
        if options.create {
            flags |= libc::O_CREAT;
        }
        if options.truncate {
            flags |= libc::O_TRUNC;
        }
        trace!("open path={} flags=0x{flags:x}", path.to_string_lossy());
        let fd = unsafe { libc::open(path.as_ptr(), flags, 0o644) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        trace!("open ok fd={fd}");
        Ok(Rc::new(OwnedFd::new(fd)))
    }

    fn socket_fd(addr: SocketAddr) -> io::Result<Rc<OwnedFd>> {
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        trace!("socket domain={domain} type=SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC");
        let fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        trace!("socket ok fd={fd}");
        if matches!(addr, SocketAddr::V6(_)) {
            let off: libc::c_int = 0;
            let rc = unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_V6ONLY,
                    (&off as *const libc::c_int).cast(),
                    mem::size_of_val(&off) as libc::socklen_t,
                )
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                unsafe {
                    libc::close(fd);
                }
                return Err(err);
            }
        }
        Ok(Rc::new(OwnedFd::new(fd)))
    }
}

impl IOFile for IoUringFile {
    fn pread(&self, c: &mut PReadCompletion, len: usize, offset: u64) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::PRead(PReadOp {
            fd: self.fd.raw(),
            buf: vec![0_u8; len],
            offset,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn pwrite(&self, c: &mut PWriteCompletion, buf: Vec<u8>, offset: u64) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::PWrite(PWriteOp {
            fd: self.fd.raw(),
            buf,
            offset,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn fsync(&self, c: &mut FsyncCompletion) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::Fsync(FsyncOp { fd: self.fd.raw() }));
        queue(&self.state, inner);
        Ok(())
    }

    fn size(&self, c: &mut SizeCompletion) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::Size(SizeOp { fd: self.fd.raw() }));
        queue(&self.state, inner);
        Ok(())
    }
}

fn queue(state: &Rc<RefCell<IoUringState>>, c: &mut CompletionInner) {
    state.borrow_mut().queued.push_back(NonNull::from(c));
}

impl IOSocket for IoUringSocket {
    fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        let fd = IoUringIO::socket_fd(addr)?;
        trace!("bind setup fd={} addr={addr}", fd.raw());
        let on: libc::c_int = 1;
        let rc = unsafe {
            libc::setsockopt(
                fd.raw(),
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                (&on as *const libc::c_int).cast(),
                mem::size_of_val(&on) as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        let (storage, len) = socket_addr_to_raw(addr);
        trace!("bind fd={} addr={} len={len}", fd.raw(), addr);
        let rc = unsafe {
            libc::bind(
                fd.raw(),
                (&storage as *const libc::sockaddr_storage).cast(),
                len,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        let rc = unsafe { libc::listen(fd.raw(), 128) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        *self.fd.borrow_mut() = Some(fd);
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

        let fd = IoUringIO::socket_fd(addr)?;
        let raw_fd = fd.raw();
        *self.fd.borrow_mut() = Some(fd);
        *self.kind.borrow_mut() = Some(SocketKind::Stream);

        let inner = c.inner_mut();
        inner.prepare(Operation::Connect(ConnectOp {
            fd: raw_fd,
            addr,
            started: false,
            addr_storage: unsafe { mem::zeroed::<libc::sockaddr_storage>() },
            addr_len: 0,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn accept(&self, c: &mut AcceptCompletion) -> io::Result<()> {
        let fd = match &*self.kind.borrow() {
            Some(SocketKind::Listener) => self
                .fd
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
        inner.prepare(Operation::Accept(AcceptOp { fd }));
        queue(&self.state, inner);
        Ok(())
    }

    fn recv(&self, c: &mut RecvCompletion, len: usize) -> io::Result<()> {
        let fd = self
            .fd
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
            fd,
            buf: vec![0_u8; len],
            flags: libc::MSG_DONTWAIT,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn send(&self, c: &mut SendCompletion, buf: Vec<u8>) -> io::Result<()> {
        let fd = self
            .fd
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
            fd,
            buf,
            flags: libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn set_nodelay(&self, on: bool) -> io::Result<()> {
        let fd = self
            .fd
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
        let value: libc::c_int = if on { 1 } else { 0 };
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&value as *const libc::c_int).cast(),
                mem::size_of_val(&value) as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn close(&self) {
        self.fd.borrow_mut().take();
        self.kind.borrow_mut().take();
    }
}

impl IO for IoUringIO {
    fn open(&self, path: &Path, options: OpenOptions) -> io::Result<Box<dyn IOFile>> {
        Ok(Box::new(IoUringFile {
            state: self.state.clone(),
            fd: Self::open_fd(path, options)?,
        }))
    }

    fn socket(&self) -> io::Result<Box<dyn IOSocket>> {
        Ok(Box::new(IoUringSocket {
            state: self.state.clone(),
            fd: Rc::new(RefCell::new(None)),
            kind: Rc::new(RefCell::new(None)),
        }))
    }

    fn mkdir(&self, c: &mut MkdirCompletion, path: &Path, mode: u32) -> io::Result<()> {
        let inner = c.inner_mut();
        inner.prepare(Operation::Mkdir(MkdirOp {
            path: c_string(path)?,
            mode,
        }));
        queue(&self.state, inner);
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "linux"
    }
}

impl IOLoop for IoUringIO {
    fn step(&self) -> io::Result<bool> {
        let mut progressed = false;
        let mut size_ops = Vec::new();

        {
            let mut state = self.state.borrow_mut();
            let queued_len = state.queued.len();
            let mut submitted = 0;
            for _ in 0..queued_len {
                let completion_ptr = match state.queued.pop_front() {
                    Some(completion) => completion,
                    None => break,
                };
                let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };

                if matches!(completion.operation(), Operation::Size(_)) {
                    if state.inflight > 0 {
                        state.queued.push_front(completion_ptr);
                        break;
                    }
                    size_ops.push(completion_ptr);
                    progressed = true;
                    continue;
                }

                let entry = Self::prepare_entry(completion);
                let mut submission = state.ring.submission();
                let pushed = unsafe { submission.push(&entry).is_ok() };
                drop(submission);
                if !pushed {
                    state.queued.push_front(completion_ptr);
                    break;
                }
                completion.mark_submitted();
                state.inflight += 1;
                submitted += 1;
                progressed = true;
            }

            if submitted > 0 {
                state.ring.submit()?;
            }
        }

        for completion_ptr in size_ops {
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
            Self::dispatch_complete(&self.state, completion, 0);
        }

        let mut completed = Vec::new();
        {
            let mut state = self.state.borrow_mut();
            let len = state.ring.completion().len();
            for _ in 0..len {
                let cqe = state
                    .ring
                    .completion()
                    .next()
                    .expect("completion length checked above");
                let completion_ptr = NonNull::new(cqe.user_data() as *mut CompletionInner)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "completion pointer missing")
                    })?;
                let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
                state.inflight = state
                    .inflight
                    .checked_sub(1)
                    .expect("completion queue retired more requests than submitted");
                if Self::should_retry(completion, cqe.result()) {
                    completion.mark_queued();
                    state.queued.push_back(completion_ptr);
                    progressed = true;
                    continue;
                }
                completed.push((completion_ptr, cqe.result()));
                progressed = true;
            }
        }

        for (completion_ptr, result) in completed {
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
            Self::dispatch_complete(&self.state, completion, result);
        }

        Ok(progressed)
    }
}

pub(crate) fn c_string(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        )
    })
}

fn io_uring_err(result: i32) -> io::Error {
    io::Error::from_raw_os_error(-result)
}

fn socket_addr_to_raw(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    match addr {
        SocketAddr::V4(addr) => {
            let sockaddr = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: addr.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(addr.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            let mut storage = unsafe { mem::zeroed::<libc::sockaddr_storage>() };
            unsafe {
                std::ptr::write(
                    (&mut storage as *mut libc::sockaddr_storage).cast(),
                    sockaddr,
                );
            }
            (
                storage,
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(addr) => {
            let sockaddr = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: addr.port().to_be(),
                sin6_flowinfo: addr.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: addr.ip().octets(),
                },
                sin6_scope_id: addr.scope_id(),
            };
            let mut storage = unsafe { mem::zeroed::<libc::sockaddr_storage>() };
            unsafe {
                std::ptr::write(
                    (&mut storage as *mut libc::sockaddr_storage).cast(),
                    sockaddr,
                );
            }
            (
                storage,
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}
