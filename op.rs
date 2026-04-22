use std::{
    io,
    path::{Path, PathBuf},
};

use super::{Completion, CompletionResult, IO, IOHandle};

pub trait StepOp {
    type Output;

    fn step(&mut self) -> io::Result<Option<Self::Output>>;
}

struct Op<T> {
    completion: Completion,
    submitted: bool,
    decode: fn(io::Result<CompletionResult>) -> io::Result<T>,
}

impl<T> Op<T> {
    fn new(decode: fn(io::Result<CompletionResult>) -> io::Result<T>) -> Self {
        Self {
            completion: Completion::new(),
            submitted: false,
            decode,
        }
    }

    fn is_submitted(&self) -> bool {
        self.submitted
    }

    fn submit(&mut self, submit: impl FnOnce(&mut Completion) -> io::Result<()>) -> io::Result<()> {
        assert!(
            !self.submitted,
            "operation must not be submitted twice before completion",
        );
        submit(&mut self.completion)?;
        self.submitted = true;
        Ok(())
    }

    fn poll(&mut self) -> Option<io::Result<T>> {
        let result = self.completion.take_result()?;
        self.submitted = false;
        Some((self.decode)(result))
    }
}

impl Op<()> {
    fn mkdir() -> Self {
        Self::new(decode_mkdir)
    }
}

pub struct Mkdir {
    io: IOHandle,
    path: PathBuf,
    mode: u32,
    op: Op<()>,
}

impl Mkdir {
    fn new(io: IOHandle, path: PathBuf, mode: u32) -> Self {
        Self {
            io,
            path,
            mode,
            op: Op::mkdir(),
        }
    }
}

impl StepOp for Mkdir {
    type Output = ();

    fn step(&mut self) -> io::Result<Option<Self::Output>> {
        if !self.op.is_submitted() {
            self.op
                .submit(|c| self.io.mkdir(c, &self.path, self.mode))?;
        }
        self.op.poll().transpose()
    }
}

pub fn mkdir(io: &IOHandle, path: &Path, mode: u32) -> Mkdir {
    Mkdir::new(io.clone(), path.to_path_buf(), mode)
}

fn decode_mkdir(result: io::Result<CompletionResult>) -> io::Result<()> {
    match result {
        Ok(CompletionResult::Mkdir) => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected mkdir completion",
        )),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err),
    }
}
