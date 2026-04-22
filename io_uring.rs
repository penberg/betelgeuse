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

use super::{
    AcceptOp, Completion, CompletionResult, FsyncOp, IO, IOFile, IOLoop, IOSocket, MkdirOp,
    OpenOptions, Operation, PReadOp, PWriteOp, RecvOp, SendOp, SizeOp,
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
    queued: VecDeque<NonNull<Completion>>,
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

    fn result_to_io(result: i32) -> io::Result<usize> {
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        }
    }

    fn should_retry(completion: &Completion, result: i32) -> bool {
        let errno = -result;
        if result >= 0 {
            return false;
        }
        match completion.operation() {
            Operation::Accept(_) | Operation::Recv(_) | Operation::Send(_) => {
                errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::EINTR
            }
            Operation::PRead(_)
            | Operation::PWrite(_)
            | Operation::Fsync(_)
            | Operation::Mkdir(_) => errno == libc::EINTR,
            Operation::Size(_) | Operation::Nop => false,
        }
    }

    fn prepare_entry(c: &mut Completion) -> io::Result<squeue::Entry> {
        let entry = match c.operation_mut() {
            Operation::Accept(op) => {
                opcode::Accept::new(types::Fd(op.fd), std::ptr::null_mut(), std::ptr::null_mut())
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
            Operation::Size(_) | Operation::Nop => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "operation is not submitted via io_uring",
                ));
            }
        };
        Ok(entry.user_data(c as *mut Completion as u64))
    }

    fn decode_result(
        state: &Rc<RefCell<IoUringState>>,
        c: &mut Completion,
        result: i32,
    ) -> io::Result<CompletionResult> {
        match c.operation_mut() {
            Operation::Accept(_) => {
                let accepted = Self::result_to_io(result)? as RawFd;
                Ok(CompletionResult::Accept(Box::new(IoUringSocket {
                    state: state.clone(),
                    fd: Rc::new(RefCell::new(Some(Rc::new(OwnedFd::new(accepted))))),
                    kind: Rc::new(RefCell::new(Some(SocketKind::Stream))),
                })))
            }
            Operation::Recv(op) => {
                let n = Self::result_to_io(result)?;
                op.buf.truncate(n);
                Ok(CompletionResult::Recv(mem::take(&mut op.buf)))
            }
            Operation::Send(_) => Self::result_to_io(result).map(CompletionResult::Send),
            Operation::PRead(op) => {
                let n = Self::result_to_io(result)?;
                op.buf.truncate(n);
                Ok(CompletionResult::PRead(mem::take(&mut op.buf)))
            }
            Operation::PWrite(_) => Self::result_to_io(result).map(CompletionResult::PWrite),
            Operation::Fsync(_) => {
                Self::result_to_io(result)?;
                Ok(CompletionResult::Fsync)
            }
            Operation::Size(op) => {
                let mut stat = MaybeUninit::<libc::stat>::uninit();
                let rc = unsafe { libc::fstat(op.fd, stat.as_mut_ptr()) };
                if rc < 0 {
                    return Err(io::Error::last_os_error());
                }
                let stat = unsafe { stat.assume_init() };
                Ok(CompletionResult::Size(stat.st_size as u64))
            }
            Operation::Mkdir(_) => {
                Self::result_to_io(result)?;
                Ok(CompletionResult::Mkdir)
            }
            Operation::Nop => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "completion has no operation",
            )),
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
    fn pread(&self, c: &mut Completion, len: usize, offset: u64) -> io::Result<()> {
        c.prepare(Operation::PRead(PReadOp {
            fd: self.fd.raw(),
            buf: vec![0_u8; len],
            offset,
        }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn pwrite(&self, c: &mut Completion, buf: Vec<u8>, offset: u64) -> io::Result<()> {
        c.prepare(Operation::PWrite(PWriteOp {
            fd: self.fd.raw(),
            buf,
            offset,
        }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn fsync(&self, c: &mut Completion) -> io::Result<()> {
        c.prepare(Operation::Fsync(FsyncOp { fd: self.fd.raw() }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn size(&self, c: &mut Completion) -> io::Result<()> {
        c.prepare(Operation::Size(SizeOp { fd: self.fd.raw() }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }
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

    fn accept(&self, c: &mut Completion) -> io::Result<()> {
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
        c.prepare(Operation::Accept(AcceptOp { fd }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn recv(&self, c: &mut Completion, len: usize) -> io::Result<()> {
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
        c.prepare(Operation::Recv(RecvOp {
            fd,
            buf: vec![0_u8; len],
            flags: libc::MSG_DONTWAIT,
        }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn send(&self, c: &mut Completion, buf: Vec<u8>) -> io::Result<()> {
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
        c.prepare(Operation::Send(SendOp {
            fd,
            buf,
            flags: libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
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

    fn mkdir(&self, c: &mut Completion, path: &Path, mode: u32) -> io::Result<()> {
        c.prepare(Operation::Mkdir(MkdirOp {
            path: c_string(path)?,
            mode,
        }));
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "io_uring"
    }
}

impl IOLoop for IoUringIO {
    fn step(&self) -> io::Result<bool> {
        let mut ready = Vec::new();
        let mut progressed = false;

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
                    let result = Self::decode_result(&self.state, completion, 0);
                    ready.push((completion_ptr, result));
                    progressed = true;
                    continue;
                }

                let entry = match Self::prepare_entry(completion) {
                    Ok(entry) => entry,
                    Err(err) => {
                        ready.push((completion_ptr, Err(err)));
                        progressed = true;
                        continue;
                    }
                };
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

        {
            let mut state = self.state.borrow_mut();
            let len = state.ring.completion().len();
            for _ in 0..len {
                let cqe = state
                    .ring
                    .completion()
                    .next()
                    .expect("completion length checked above");
                let completion_ptr = NonNull::new(cqe.user_data() as *mut Completion).ok_or_else(
                    || io::Error::new(io::ErrorKind::InvalidData, "completion pointer missing"),
                )?;
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
                let result = Self::decode_result(&self.state, completion, cqe.result());
                ready.push((completion_ptr, result));
                progressed = true;
            }
        }

        for (completion_ptr, result) in ready {
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
            completion.complete(result);
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
