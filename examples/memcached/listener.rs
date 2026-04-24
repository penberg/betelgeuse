use std::io;

use betelgeuse::{Completion, CompletionResult, IOSocket, slab::SlabEntry};

enum ListenerState {
    Free { next: Option<usize> },
    Active { socket: Box<dyn IOSocket> },
}

pub struct Listener {
    state: ListenerState,
    accept_completion: Completion,
}

impl Listener {
    pub fn activate(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
        self.state = ListenerState::Active { socket };
        self.accept_completion = Completion::new();
        self.arm_accept()
    }

    pub fn accept_result_ready(&self) -> bool {
        matches!(self.state, ListenerState::Active { .. }) && self.accept_completion.has_result()
    }

    pub fn take_accept_result(&mut self) -> Option<io::Result<CompletionResult>> {
        self.accept_completion.take_result()
    }

    pub fn arm_accept(&mut self) -> io::Result<()> {
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
