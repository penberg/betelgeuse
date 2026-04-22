use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::{File, OpenOptions as StdOpenOptions},
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    os::fd::{AsRawFd, FromRawFd},
    path::Path,
    ptr::NonNull,
    rc::Rc,
};

use super::{
    AcceptOp, Completion, CompletionResult, FsyncOp, IO, IOFile, IOLoop, IOSocket, MkdirOp,
    OpenOptions, Operation, PReadOp, PWriteOp, RecvOp, SendOp, SizeOp,
};

enum Socket {
    Listener(TcpListener),
    Stream(TcpStream),
}

enum PollResult {
    Pending,
    Ready(io::Result<CompletionResult>),
}

struct SyscallState {
    pending: VecDeque<NonNull<Completion>>,
}

struct SyscallFile {
    state: Rc<RefCell<SyscallState>>,
    file: Rc<File>,
}

struct SyscallSocket {
    state: Rc<RefCell<SyscallState>>,
    socket: Rc<RefCell<Option<Socket>>>,
}

pub struct SyscallIO {
    state: Rc<RefCell<SyscallState>>,
}

impl SyscallIO {
    pub fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(SyscallState {
                pending: VecDeque::new(),
            })),
        }
    }
}

impl SyscallFile {
    fn submit(&self, c: &mut Completion) {
        c.mark_submitted();
        self.state.borrow_mut().pending.push_back(NonNull::from(c));
    }
}

impl IOFile for SyscallFile {
    fn pread(&self, c: &mut Completion, len: usize, offset: u64) -> io::Result<()> {
        c.prepare(Operation::PRead(PReadOp {
            fd: self.file.as_raw_fd(),
            buf: vec![0_u8; len],
            offset,
        }));
        self.submit(c);
        Ok(())
    }

    fn pwrite(&self, c: &mut Completion, buf: Vec<u8>, offset: u64) -> io::Result<()> {
        c.prepare(Operation::PWrite(PWriteOp {
            fd: self.file.as_raw_fd(),
            buf,
            offset,
        }));
        self.submit(c);
        Ok(())
    }

    fn fsync(&self, c: &mut Completion) -> io::Result<()> {
        c.prepare(Operation::Fsync(FsyncOp {
            fd: self.file.as_raw_fd(),
        }));
        self.submit(c);
        Ok(())
    }

    fn size(&self, c: &mut Completion) -> io::Result<()> {
        c.prepare(Operation::Size(SizeOp {
            fd: self.file.as_raw_fd(),
        }));
        self.submit(c);
        Ok(())
    }
}

impl IOSocket for SyscallSocket {
    fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        *self.socket.borrow_mut() = Some(Socket::Listener(listener));
        Ok(())
    }

    fn accept(&self, c: &mut Completion) -> io::Result<()> {
        let fd = match self.socket.borrow().as_ref() {
            Some(Socket::Listener(listener)) => listener.as_raw_fd(),
            Some(Socket::Stream(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "accept called on stream socket",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "accept called on unbound socket",
                ));
            }
        };
        c.prepare(Operation::Accept(AcceptOp { fd }));
        c.mark_submitted();
        self.state.borrow_mut().pending.push_back(NonNull::from(c));
        Ok(())
    }

    fn recv(&self, c: &mut Completion, len: usize) -> io::Result<()> {
        let fd = match self.socket.borrow().as_ref() {
            Some(Socket::Stream(stream)) => stream.as_raw_fd(),
            Some(Socket::Listener(_)) => {
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
        };
        c.prepare(Operation::Recv(RecvOp {
            fd,
            buf: vec![0_u8; len],
            flags: libc::MSG_DONTWAIT,
        }));
        c.mark_submitted();
        self.state.borrow_mut().pending.push_back(NonNull::from(c));
        Ok(())
    }

    fn send(&self, c: &mut Completion, buf: Vec<u8>) -> io::Result<()> {
        let fd = match self.socket.borrow().as_ref() {
            Some(Socket::Stream(stream)) => stream.as_raw_fd(),
            Some(Socket::Listener(_)) => {
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
        };
        c.prepare(Operation::Send(SendOp {
            fd,
            buf,
            flags: libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        }));
        c.mark_submitted();
        self.state.borrow_mut().pending.push_back(NonNull::from(c));
        Ok(())
    }

    fn set_nodelay(&self, on: bool) -> io::Result<()> {
        match self.socket.borrow().as_ref() {
            Some(Socket::Stream(stream)) => stream.set_nodelay(on),
            Some(Socket::Listener(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "set_nodelay called on listener socket",
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "set_nodelay on closed socket",
            )),
        }
    }

    fn close(&self) {
        self.socket.borrow_mut().take();
    }
}

impl IO for SyscallIO {
    fn open(&self, path: &Path, options: OpenOptions) -> io::Result<Box<dyn IOFile>> {
        let file = StdOpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .truncate(options.truncate)
            .open(path)?;
        Ok(Box::new(SyscallFile {
            state: self.state.clone(),
            file: Rc::new(file),
        }))
    }

    fn socket(&self) -> io::Result<Box<dyn IOSocket>> {
        Ok(Box::new(SyscallSocket {
            state: self.state.clone(),
            socket: Rc::new(RefCell::new(None)),
        }))
    }

    fn mkdir(&self, c: &mut Completion, path: &Path, mode: u32) -> io::Result<()> {
        c.prepare(Operation::Mkdir(MkdirOp {
            path: super::io_uring::c_string(path)?,
            mode,
        }));
        c.mark_submitted();
        self.state.borrow_mut().pending.push_back(NonNull::from(c));
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "syscall"
    }
}

impl IOLoop for SyscallIO {
    fn step(&self) -> io::Result<bool> {
        let pending_len = self.state.borrow().pending.len();
        let mut ready = Vec::new();
        for _ in 0..pending_len {
            let completion_ptr = self
                .state
                .borrow_mut()
                .pending
                .pop_front()
                .expect("pending length checked above");
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
            match execute_completion(&self.state, completion) {
                PollResult::Pending => self.state.borrow_mut().pending.push_back(completion_ptr),
                PollResult::Ready(result) => ready.push((completion_ptr, result)),
            }
        }

        let progressed = !ready.is_empty();
        for (completion_ptr, result) in ready {
            let completion = unsafe { completion_ptr.as_ptr().as_mut().expect("non-null") };
            completion.complete(result);
        }

        Ok(progressed)
    }
}

fn execute_completion(state: &Rc<RefCell<SyscallState>>, c: &mut Completion) -> PollResult {
    match c.operation_mut() {
        Operation::Accept(op) => {
            let accepted = unsafe {
                libc::accept4(
                    op.fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            if accepted < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return PollResult::Pending;
                }
                return PollResult::Ready(Err(err));
            }
            let stream = unsafe { TcpStream::from_raw_fd(accepted) };
            PollResult::Ready(Ok(CompletionResult::Accept(Box::new(SyscallSocket {
                state: state.clone(),
                socket: Rc::new(RefCell::new(Some(Socket::Stream(stream)))),
            }))))
        }
        Operation::Recv(op) => {
            let rc =
                unsafe { libc::recv(op.fd, op.buf.as_mut_ptr().cast(), op.buf.len(), op.flags) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return PollResult::Pending;
                }
                return PollResult::Ready(Err(err));
            }
            op.buf.truncate(rc as usize);
            PollResult::Ready(Ok(CompletionResult::Recv(std::mem::take(&mut op.buf))))
        }
        Operation::Send(op) => {
            let rc = unsafe { libc::send(op.fd, op.buf.as_ptr().cast(), op.buf.len(), op.flags) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return PollResult::Pending;
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
                return PollResult::Ready(Err(io::Error::last_os_error()));
            }
            op.buf.truncate(rc as usize);
            PollResult::Ready(Ok(CompletionResult::PRead(std::mem::take(&mut op.buf))))
        }
        Operation::PWrite(op) => match unsafe {
            libc::pwrite(
                op.fd,
                op.buf.as_ptr().cast(),
                op.buf.len(),
                op.offset as libc::off_t,
            )
        } {
            n if n >= 0 => PollResult::Ready(Ok(CompletionResult::PWrite(n as usize))),
            _ => PollResult::Ready(Err(io::Error::last_os_error())),
        },
        Operation::Fsync(op) => match unsafe { libc::fsync(op.fd) } {
            0 => PollResult::Ready(Ok(CompletionResult::Fsync)),
            _ => PollResult::Ready(Err(io::Error::last_os_error())),
        },
        Operation::Size(op) => {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
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
                _ => PollResult::Ready(Err(io::Error::last_os_error())),
            }
        }
        Operation::Nop => PollResult::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "completion has no operation",
        ))),
    }
}
