use std::{
    io,
    path::{Path, PathBuf},
};

use super::{IO, IOHandle, MkdirCompletion};

pub trait StepOp {
    type Output;

    fn step(&mut self) -> io::Result<Option<Self::Output>>;
}

pub struct Mkdir {
    io: IOHandle,
    path: PathBuf,
    mode: u32,
    completion: MkdirCompletion,
    submitted: bool,
}

impl Mkdir {
    fn new(io: IOHandle, path: PathBuf, mode: u32) -> Self {
        Self {
            io,
            path,
            mode,
            completion: MkdirCompletion::new(),
            submitted: false,
        }
    }
}

impl StepOp for Mkdir {
    type Output = ();

    fn step(&mut self) -> io::Result<Option<Self::Output>> {
        if !self.submitted {
            self.io.mkdir(&mut self.completion, &self.path, self.mode)?;
            self.submitted = true;
        }
        match self.completion.take_result() {
            None => Ok(None),
            Some(Ok(())) => {
                self.submitted = false;
                Ok(Some(()))
            }
            Some(Err(err)) if err.kind() == io::ErrorKind::AlreadyExists => {
                self.submitted = false;
                Ok(Some(()))
            }
            Some(Err(err)) => {
                self.submitted = false;
                Err(err)
            }
        }
    }
}

pub fn mkdir(io: &IOHandle, path: &Path, mode: u32) -> Mkdir {
    Mkdir::new(io.clone(), path.to_path_buf(), mode)
}
