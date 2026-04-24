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

use log::trace;

use crate::{
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

struct DarwinState {
    kq: RawFd,
    queued: VecDeque<NonNull<Completion>>,
}

impl Drop for DarwinState {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.kq);
        }
    }
}

struct DarwinFile {
    state: Rc<RefCell<DarwinState>>,
    fd: Rc<OwnedFd>,
}

struct DarwinSocket {
    state: Rc<RefCell<DarwinState>>,
    fd: Rc<RefCell<Option<Rc<OwnedFd>>>>,
    kind: Rc<RefCell<Option<SocketKind>>>,
}

pub struct DarwinIO {
    state: Rc<RefCell<DarwinState>>,
}

enum PollResult {
    Wait,
    Retry,
    Ready(io::Result<CompletionResult>),
}

impl DarwinIO {
    pub fn new() -> io::Result<Self> {
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            state: Rc::new(RefCell::new(DarwinState {
                kq,
                queued: VecDeque::new(),
            })),
        })
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
        trace!("socket domain={domain} type=SOCK_STREAM");
        let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        if let Err(err) = set_nonblocking_and_cloexec(fd).and_then(|_| set_no_sigpipe(fd)) {
            unsafe {
                libc::close(fd);
            }
            return Err(err);
        }

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

    fn queue_completion(&self, c: &mut Completion) {
        c.mark_submitted();
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
    }

    fn watch_fd(kq: RawFd, c: NonNull<Completion>, op: &Operation) -> io::Result<()> {
        let (fd, filter) = match op {
            Operation::Accept(op) => (op.fd, libc::EVFILT_READ),
            Operation::Recv(op) => (op.fd, libc::EVFILT_READ),
            Operation::Send(op) => (op.fd, libc::EVFILT_WRITE),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "operation cannot be watched by kqueue",
                ));
            }
        };

        let mut change = libc::kevent {
            ident: fd as libc::uintptr_t,
            filter,
            flags: (libc::EV_ADD | libc::EV_ONESHOT) as u16,
            fflags: 0,
            data: 0,
            udata: c.as_ptr().cast(),
        };

        let rc = unsafe {
            libc::kevent(
                kq,
                &mut change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl IOFile for DarwinFile {
    fn pread(&self, c: &mut Completion, len: usize, offset: u64) -> io::Result<()> {
        c.prepare(Operation::PRead(PReadOp {
            fd: self.fd.raw(),
            buf: vec![0_u8; len],
            offset,
        }));
        c.mark_submitted();
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn pwrite(&self, c: &mut Completion, buf: Vec<u8>, offset: u64) -> io::Result<()> {
        c.prepare(Operation::PWrite(PWriteOp {
            fd: self.fd.raw(),
            buf,
            offset,
        }));
        c.mark_submitted();
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn fsync(&self, c: &mut Completion) -> io::Result<()> {
        c.prepare(Operation::Fsync(FsyncOp { fd: self.fd.raw() }));
        c.mark_submitted();
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }

    fn size(&self, c: &mut Completion) -> io::Result<()> {
        c.prepare(Operation::Size(SizeOp { fd: self.fd.raw() }));
        c.mark_submitted();
        self.state.borrow_mut().queued.push_back(NonNull::from(c));
        Ok(())
    }
}

impl IOSocket for DarwinSocket {
    fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        let fd = DarwinIO::socket_fd(addr)?;
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
        c.mark_submitted();
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
        c.mark_submitted();
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
            flags: libc::MSG_DONTWAIT,
        }));
        c.mark_submitted();
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

impl IO for DarwinIO {
    fn open(&self, path: &Path, options: OpenOptions) -> io::Result<Box<dyn IOFile>> {
        Ok(Box::new(DarwinFile {
            state: self.state.clone(),
            fd: Self::open_fd(path, options)?,
        }))
    }

    fn socket(&self) -> io::Result<Box<dyn IOSocket>> {
        Ok(Box::new(DarwinSocket {
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
        self.queue_completion(c);
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "darwin"
    }
}

impl IOLoop for DarwinIO {
    fn step(&self) -> io::Result<bool> {
        let mut ready = Vec::new();
        let mut progressed = false;

        let queued_len = self.state.borrow().queued.len();
        for _ in 0..queued_len {
            let completion_ptr = self
                .state
                .borrow_mut()
                .queued
                .pop_front()
                .expect("pending length checked above");
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };

            match execute_completion(&self.state, completion) {
                PollResult::Wait => {
                    let kq = self.state.borrow().kq;
                    Self::watch_fd(kq, completion_ptr, completion.operation())?;
                    progressed = true;
                }
                PollResult::Retry => {
                    self.state.borrow_mut().queued.push_back(completion_ptr);
                    progressed = true;
                }
                PollResult::Ready(result) => {
                    ready.push((completion_ptr, result));
                    progressed = true;
                }
            }
        }

        {
            let kq = self.state.borrow().kq;
            let mut events: [libc::kevent; 64] = unsafe { MaybeUninit::zeroed().assume_init() };
            let timeout = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            let n = unsafe {
                libc::kevent(
                    kq,
                    std::ptr::null(),
                    0,
                    events.as_mut_ptr(),
                    events.len() as i32,
                    &timeout,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            if n > 0 {
                progressed = true;
            }

            for ev in events.iter().take(n as usize) {
                let Some(completion_ptr) = NonNull::new(ev.udata.cast::<Completion>()) else {
                    continue;
                };

                if (ev.flags & libc::EV_ERROR as u16) != 0 && ev.data != 0 {
                    ready.push((
                        completion_ptr,
                        Err(io::Error::from_raw_os_error(ev.data as i32)),
                    ));
                    continue;
                }

                self.state.borrow_mut().queued.push_back(completion_ptr);
            }
        }

        for (completion_ptr, result) in ready {
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
            completion.complete(result);
        }

        Ok(progressed)
    }
}

fn execute_completion(state: &Rc<RefCell<DarwinState>>, c: &mut Completion) -> PollResult {
    match c.operation_mut() {
        Operation::Accept(op) => {
            let accepted =
                unsafe { libc::accept(op.fd, std::ptr::null_mut(), std::ptr::null_mut()) };
            if accepted < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return PollResult::Wait;
                }
                if err.raw_os_error() == Some(libc::EINTR) {
                    return PollResult::Retry;
                }
                return PollResult::Ready(Err(err));
            }

            if let Err(err) =
                set_nonblocking_and_cloexec(accepted).and_then(|_| set_no_sigpipe(accepted))
            {
                unsafe {
                    libc::close(accepted);
                }
                return PollResult::Ready(Err(err));
            }

            PollResult::Ready(Ok(CompletionResult::Accept(Box::new(DarwinSocket {
                state: state.clone(),
                fd: Rc::new(RefCell::new(Some(Rc::new(OwnedFd::new(accepted))))),
                kind: Rc::new(RefCell::new(Some(SocketKind::Stream))),
            }))))
        }
        Operation::Recv(op) => {
            let rc =
                unsafe { libc::recv(op.fd, op.buf.as_mut_ptr().cast(), op.buf.len(), op.flags) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return PollResult::Wait;
                }
                if err.raw_os_error() == Some(libc::EINTR) {
                    return PollResult::Retry;
                }
                return PollResult::Ready(Err(err));
            }
            op.buf.truncate(rc as usize);
            PollResult::Ready(Ok(CompletionResult::Recv(mem::take(&mut op.buf))))
        }
        Operation::Send(op) => {
            let rc = unsafe { libc::send(op.fd, op.buf.as_ptr().cast(), op.buf.len(), op.flags) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return PollResult::Wait;
                }
                if err.raw_os_error() == Some(libc::EINTR) {
                    return PollResult::Retry;
                }
                return PollResult::Ready(Err(err));
            }
            PollResult::Ready(Ok(CompletionResult::Send(rc as usize)))
        }
        Operation::PRead(op) => {
            let rc = unsafe {
                libc::pread(
                    op.fd,
                    op.buf.as_mut_ptr().cast(),
                    op.buf.len(),
                    op.offset as libc::off_t,
                )
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    return PollResult::Retry;
                }
                return PollResult::Ready(Err(err));
            }
            op.buf.truncate(rc as usize);
            PollResult::Ready(Ok(CompletionResult::PRead(mem::take(&mut op.buf))))
        }
        Operation::PWrite(op) => {
            let rc = unsafe {
                libc::pwrite(
                    op.fd,
                    op.buf.as_ptr().cast(),
                    op.buf.len(),
                    op.offset as libc::off_t,
                )
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    return PollResult::Retry;
                }
                return PollResult::Ready(Err(err));
            }
            PollResult::Ready(Ok(CompletionResult::PWrite(rc as usize)))
        }
        Operation::Fsync(op) => {
            let rc = unsafe { libc::fsync(op.fd) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    return PollResult::Retry;
                }
                return PollResult::Ready(Err(err));
            }
            PollResult::Ready(Ok(CompletionResult::Fsync))
        }
        Operation::Size(op) => {
            let mut stat = MaybeUninit::<libc::stat>::uninit();
            match unsafe { libc::fstat(op.fd, stat.as_mut_ptr()) } {
                0 => {
                    let stat = unsafe { stat.assume_init() };
                    PollResult::Ready(Ok(CompletionResult::Size(stat.st_size as u64)))
                }
                _ => PollResult::Ready(Err(io::Error::last_os_error())),
            }
        }
        Operation::Mkdir(op) => {
            match unsafe { libc::mkdir(op.path.as_ptr(), op.mode as libc::mode_t) } {
                0 => PollResult::Ready(Ok(CompletionResult::Mkdir)),
                _ => {
                    let err = io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::EINTR) {
                        PollResult::Retry
                    } else {
                        PollResult::Ready(Err(err))
                    }
                }
            }
        }
        Operation::Nop => PollResult::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "completion has no operation",
        ))),
    }
}

fn c_string(path: &Path) -> io::Result<CString> {
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
                sin_len: mem::size_of::<libc::sockaddr_in>() as u8,
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
                sin6_len: mem::size_of::<libc::sockaddr_in6>() as u8,
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

fn set_nonblocking_and_cloexec(fd: RawFd) -> io::Result<()> {
    let status = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if status < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, status | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }

    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

fn set_no_sigpipe(fd: RawFd) -> io::Result<()> {
    let on: libc::c_int = 1;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            (&on as *const libc::c_int).cast(),
            mem::size_of_val(&on) as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
