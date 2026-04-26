use std::io;

use std::net::SocketAddr;

use betelgeuse::{AcceptCompletion, IO, IOSocket, slab::SlabEntry};

/// Outcome of advancing a listener's state machine for one tick.
pub enum ListenerStep {
    /// No accept has completed yet.
    Idle,
    /// A new socket is ready for the owner to install in the connection slab.
    Accepted(Box<dyn IOSocket>),
}

enum ListenerState {
    Free { next: Option<usize> },
    Active { socket: Box<dyn IOSocket> },
}

pub struct Listener {
    state: ListenerState,
    accept_completion: AcceptCompletion,
}

impl Listener {
    pub fn listen(&mut self, io: &impl IO, addr: SocketAddr) -> io::Result<()> {
        let socket = io.socket()?;
        socket.bind(addr)?;
        self.activate(socket)
    }

    fn activate(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ListenerState::Active { socket };
        self.accept_completion = AcceptCompletion::new();
        self.arm_accept()
    }

    pub fn step(&mut self) -> io::Result<ListenerStep> {
        if !matches!(self.state, ListenerState::Active { .. })
            || !self.accept_completion.has_result()
        {
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
        let ListenerState::Active { socket } = &self.state else {
            panic!("listener must be active before accept");
        };
        socket.accept(&mut self.accept_completion)
    }
}

impl SlabEntry for Listener {
    fn new_free(next: Option<usize>) -> Self {
        Self {
            state: ListenerState::Free { next },
            accept_completion: AcceptCompletion::new(),
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
        self.accept_completion = AcceptCompletion::new();
    }
}
