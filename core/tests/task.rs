//! Integration tests for the `Task` runtime and the `spawn!` macro.
//!
//! These tests drive coroutine-backed tasks to completion using the same
//! loop structure as `main()` in a real server: step the task, and when it
//! yields `Pending`, step the I/O loop to make progress.

#![feature(allocator_api)]
#![feature(coroutines)]
#![feature(stmt_expr_attributes)]

use std::{
    alloc::{Allocator, Global},
    io,
    path::PathBuf,
};

use betelgeuse::{IOLoop, IOLoopHandle, io_loop, op, spawn, task::Task};
use tempfile::TempDir;

fn make_loop() -> IOLoopHandle<Global> {
    io_loop(Global).expect("io_loop construction failed")
}

fn run_task<T, A: Allocator + Clone + 'static>(
    io_loop: &IOLoopHandle<A>,
    task: &mut Task<T, A>,
) -> io::Result<T> {
    let io = io_loop.io();
    loop {
        if let Some(result) = task.step(&io)? {
            return Ok(result);
        }
        io_loop.step()?;
    }
}

macro_rules! task_test {
    (fn $name:ident($io_loop:ident) $body:block) => {
        mod $name {
            use super::*;

            fn run($io_loop: &IOLoopHandle<Global>) $body

            #[test]
            fn native() {
                run(&make_loop());
            }
        }
    };
}

task_test! {
    fn done_task_reports_done(_io_loop) {
        let task: Task<(), Global> = Task::done();
        assert!(task.is_done());
    }
}

task_test! {
    fn done_task_step_returns_none(io_loop) {
        let mut task: Task<i32, Global> = Task::done();
        let io = io_loop.io();
        assert!(task.step(&io).unwrap().is_none());
    }
}

task_test! {
    fn spawn_single_mkdir(io_loop) {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("child");
        let task_target = target.clone();

        let mut task = spawn!(Global, |io| {
            io_await!(io, op::mkdir(&io, &task_target, 0o755))?;
            Ok(())
        });
        assert!(!task.is_done());

        run_task(io_loop, &mut task).unwrap();
        assert!(target.is_dir());
        assert!(task.is_done());
    }
}

task_test! {
    fn spawn_sequential_mkdirs(io_loop) {
        let dir = TempDir::new().unwrap();
        let a = dir.path().join("a");
        let b = a.join("b");
        let c = b.join("c");
        let task_a = a.clone();
        let task_b = b.clone();
        let task_c = c.clone();

        let mut task = spawn!(Global, |io| {
            io_await!(io, op::mkdir(&io, &task_a, 0o755))?;
            io_await!(io, op::mkdir(&io, &task_b, 0o755))?;
            io_await!(io, op::mkdir(&io, &task_c, 0o755))?;
            Ok(())
        });

        run_task(io_loop, &mut task).unwrap();
        assert!(a.is_dir() && b.is_dir() && c.is_dir());
    }
}

task_test! {
    fn spawn_returns_typed_value(io_loop) {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("typed");
        let task_target = target.clone();

        let mut task: Task<u32, Global> = spawn!(Global, |io| {
            io_await!(io, op::mkdir(&io, &task_target, 0o755))?;
            Ok(42u32)
        });

        assert_eq!(run_task(io_loop, &mut task).unwrap(), 42);
        assert!(task.is_done());
    }
}

task_test! {
    fn spawn_step_returns_none_after_completion(io_loop) {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("once");
        let task_target = target.clone();

        let mut task = spawn!(Global, |io| {
            io_await!(io, op::mkdir(&io, &task_target, 0o755))?;
            Ok(())
        });

        run_task(io_loop, &mut task).unwrap();
        let io = io_loop.io();
        assert!(task.step(&io).unwrap().is_none());
    }
}

task_test! {
    fn spawn_mkdir_already_exists_is_ok(io_loop) {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("existing");
        std::fs::create_dir(&target).unwrap();
        let task_target = target.clone();

        let mut task = spawn!(Global, |io| {
            io_await!(io, op::mkdir(&io, &task_target, 0o755))?;
            Ok(())
        });

        run_task(io_loop, &mut task).expect("op::mkdir should swallow AlreadyExists");
    }
}

task_test! {
    fn spawn_propagates_error(io_loop) {
        let dir = TempDir::new().unwrap();
        let bogus: PathBuf = dir.path().join("does/not/exist/nested");

        let mut task = spawn!(Global, |io| {
            io_await!(io, op::mkdir(&io, &bogus, 0o755))?;
            Ok(())
        });

        match run_task(io_loop, &mut task) {
            Ok(_) => panic!("mkdir on missing parent should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::NotFound),
        }
    }
}
