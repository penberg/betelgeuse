use std::io;
use std::net::SocketAddr;

use betelgeuse::{AcceptCompletion, IO, IOSocket};

pub enum ListenerStep {
    Accepted(Box<dyn IOSocket>),
    Idle,
}

pub struct Listener {
    socket: Box<dyn IOSocket>,
    accept_completion: AcceptCompletion,
}

impl Listener {
    pub fn start(io: &impl IO, addr: SocketAddr) -> io::Result<Self> {
        let socket = io.socket()?;
        socket.bind(addr)?;
        let mut listener = Self {
            socket,
            accept_completion: AcceptCompletion::new(),
        };
        listener.arm_accept()?;
        Ok(listener)
    }

    pub fn step(&mut self) -> io::Result<ListenerStep> {
        if !self.accept_completion.has_result() {
            return Ok(ListenerStep::Idle);
        }
        let socket = self
            .accept_completion
            .take_result()
            .expect("step() guarantees an accept result is ready")?;
        self.arm_accept()?;
        Ok(ListenerStep::Accepted(socket))
    }

    fn arm_accept(&mut self) -> io::Result<()> {
        self.socket.accept(&mut self.accept_completion)
    }
}
