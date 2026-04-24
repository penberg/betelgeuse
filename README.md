<h1 align="center">Betelgeuse</h1>

<p align="center">
  Completion-based I/O for Rust. No runtime, no hidden tasks.
</p>

## 💡 Why

Asynchronous I/O is a must for modern servers: blocking a thread per operation does not scale because it cannot leverage the parallelism of storage and network devices. In Rust, that usually means using futures with `async` and `await`, backed by a large ecosystem including [Tokio](https://tokio.rs/). However, the futures model has problems that matter for high-performance servers under load [[2]](#references):

- The model is designed for multi-threading, which can itself hinder high performance due to synchronization.
- You can spawn any amount of work by default until queues grow beyond memory limits.
- Work-stealing moves computation between cores, resulting in CPU cache misses.

Betelgeuse takes a different direction for asynchronous I/O, inspired by [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle). A single thread loops forever, calling `step()` on two objects: the server and the I/O loop. The caller owns completions; state transitions happen in one place, and nothing advances unless explicitly asked to—no waker, no executor, no hidden tasks. Modern I/O devices are fast enough that CPU-side abstractions and kernel overhead can become the bottleneck, so Betelgeuse aims to keep that path direct and eliminate as much overhead as possible [[1]](#references).

The I/O model also works well with deterministic simulation testing. Once nothing runs on its own, a simulation backend can drive the same server binary under controlled time, I/O ordering, and fault injection — without changing a line of application code.

## 🎯 Scope

Betelgeuse is deliberately small. It is not a runtime, not a framework, and not an async ecosystem.

In scope:

- A completion-based trait for files (`pread`, `pwrite`, `fsync`, `size`) and sockets (`bind`, `accept`, `recv`, `send`).
- Caller-owned completion slots with a clear lifecycle.
- Pluggable native backends: `linux` (`io_uring`) and `darwin` (`kqueue`), plus a planned `simulation` backend that controls time, I/O ordering, and faults for deterministic simulation testing.

Out of scope: wire protocols, framing, buffer pools, state machines, WAL, consensus, client sessions. Those belong in the systems built on top of Betelgeuse.

## ⚙️ Design

- **Completion-based, not `async`/`await`.** Callers prepare an operation, hand a `&mut Completion` to the backend, and later observe a semantic result in the same slot. No futures, no executors, no hidden tasks.
- **Caller owns the slot.** Long-lived state machines keep persistent completion slots; synchronous bootstrap code can allocate one on the stack. The backend never allocates on the hot path.
- **One interface, many backends.** `IOLoop` drives the backend forward; `IO` submits work. A simulation backend can implement both without any kernel calls, giving DST full control over time, ordering, and faults.

## 🧪 Examples

- [`examples/echo`](examples/echo/README.md) - minimal TCP echo server showing the basic `server.step(); io_loop.step();` shape.
- [`examples/memcached`](examples/memcached/README.md) - in-memory memcached-style server using the same completion-driven model on a less trivial protocol.

## 🔗 References

[1] Pekka Enberg, Ashwin Rao, and Sasu Tarkoma (2019). [I/O Is Faster Than the CPU - Let's Partition Resources and Eliminate (Most) OS Abstractions](https://penberg.org/parakernel-hotos19.pdf).

[2] Peter Mbanugo (2026). [The Tokio/Rayon Trap and Why Async/Await Fails Concurrency](https://pmbanugo.me/blog/why-async-await-complect-concurrency).
