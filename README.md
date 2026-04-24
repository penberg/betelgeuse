<h1 align="center">Betelgeuse</h1>

<p align="center">
  Completion-based I/O for Rust. No runtime, no hidden tasks.
</p>

## Motivation

Asynchronous I/O in Rust is supported with with futures using the `async` and
`await` keywords, which are easy to use and backed by a large ecosystem,
including [Tokio](https://tokio.rs/). However, the model has problems that
matter under load [[1]](#references):

- A CPU-heavy section stalls the cooperative executor.
- `tokio::spawn()` accepts any amount of work until queues grow past OOM.
- Work-stealing migrates state machines between cores and pays the cache-miss tax.
- The programmer ends up as the human in the loop scheduler.

Betelgeuse takes a different direction for asynchronous I/O. A single thread
loops forever, calling `step()` on two objects: the server and the I/O loop.
Completions are owned by the caller, state transitions happen in one place, and
nothing advances unless something explicitly asks it to. No waker, no executor,
no hidden tasks.

Deterministic simulation testing falls out for free. Once nothing runs on its
own, a simulation backend can drive the same server binary under controlled
time, I/O ordering, and fault injection — without changing a line of
application code.

## Scope

Betelgeuse is deliberately small. It is not a runtime, not a framework, and not an async ecosystem.

In scope:

- A completion-based trait for files (`pread`, `pwrite`, `fsync`, `size`) and sockets (`bind`, `accept`, `recv`, `send`).
- Caller-owned completion slots with a clear lifecycle.
- Pluggable backends: `syscall` and `io_uring` for production, and a planned `simulation` backend that controls time, I/O ordering, and faults for deterministic simulation testing.

Out of scope: wire protocols, framing, buffer pools, state machines, WAL, consensus, client sessions. Those belong in the systems built on top of Betelgeuse.

## Design

- **Completion-based, not `async`/`await`.** Callers prepare an operation, hand a `&mut Completion` to the backend, and later observe a semantic result in the same slot. No futures, no executors, no hidden tasks.
- **Caller owns the slot.** Long-lived state machines keep persistent completion slots; synchronous bootstrap code can allocate one on the stack. The backend never allocates on the hot path.
- **One interface, many backends.** `IOLoop` drives the backend forward; `IO` submits work. A simulation backend can implement both without any kernel calls, giving DST full control over time, ordering, and faults.

## Examples

- [`examples/echo`](examples/echo/README.md) - minimal TCP echo server showing the basic `server.step(); io_loop.step();` shape.
- [`examples/memcached`](examples/memcached/README.md) - in-memory memcached-style server using the same completion-driven model on a less trivial protocol.

## References

[1] Peter Mbanugo (2026). [The Tokio/Rayon Trap and Why Async/Await Fails Concurrency](https://pmbanugo.me/blog/why-async-await-complect-concurrency).
