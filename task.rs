use std::{
    alloc::Allocator,
    io,
    ops::{Coroutine, CoroutineState},
    pin::Pin,
};

use crate::IOHandle;

pub enum TaskYield {
    Pending,
}

pub struct Task<T = (), A: Allocator = std::alloc::Global> {
    coroutine:
        Option<Pin<Box<dyn Coroutine<IOHandle, Yield = TaskYield, Return = io::Result<T>>, A>>>,
}

impl<T, A: Allocator + 'static> Task<T, A> {
    pub fn done() -> Self {
        Self { coroutine: None }
    }

    pub fn new_in<C>(allocator: A, coroutine: C) -> Self
    where
        C: Coroutine<IOHandle, Yield = TaskYield, Return = io::Result<T>> + 'static,
    {
        Self {
            coroutine: Some(Box::into_pin(Box::new_in(coroutine, allocator))),
        }
    }

    pub fn step(&mut self, io: &IOHandle) -> io::Result<Option<T>> {
        let Some(coroutine) = self.coroutine.as_mut() else {
            return Ok(None);
        };

        match coroutine.as_mut().resume(io.clone()) {
            CoroutineState::Yielded(TaskYield::Pending) => Ok(None),
            CoroutineState::Complete(result) => {
                self.coroutine = None;
                result.map(Some)
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.coroutine.is_none()
    }
}

#[macro_export]
macro_rules! spawn {
    ($allocator:expr, |$io:ident| $body:block) => {{
        $crate::task::Task::new_in(
            $allocator,
            #[coroutine]
            static move |mut $io: $crate::IOHandle| {
                macro_rules! io_await {
                    ($io_next:ident, $op:expr) => {{
                        let mut __op = $op;
                        loop {
                            if let Some(__result) = $crate::op::StepOp::step(&mut __op)? {
                                break ::std::io::Result::Ok(__result);
                            }
                            $io_next = yield $crate::task::TaskYield::Pending;
                        }
                    }};
                }

                $body
            },
        )
    }};
}
