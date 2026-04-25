# Betelgeuse

This document describes the programming model Betelgeuse enables: servers
that run on a single thread, drive asynchronous I/O through completions, and
express long-lived work as coroutines.

The model rests on four ideas:

- **Explicit `step()`.** Progress happens only when an owner drives it.
- **Completion-based I/O.** Callers own in-flight operation slots; the
  backend fills them.
- **Owned state machines.** Application state transitions happen in one
  place.
- **Coroutine tasks.** Stepped state machines authored as linear code,
  without a hidden executor.

There is no background executor, no waker-driven scheduling, and no
thread-per-connection. A single thread loops forever, calling `step()` on
two objects.

## The Step Loop

Two long-lived objects cooperate:

- an **I/O loop** that drives a concrete backend (for example Linux
  `io_uring`, Darwin `kqueue`, or a deterministic simulator)
- a **server** that owns the application's state machines

```rust
loop {
    server.step()?;
    io_loop.step()?;
}
```

The order reflects ownership, not blocking semantics:

- the server decides what work should exist and submits it
- the I/O loop retires work already submitted and fills completion slots

Neither side blocks the other. The top-level loop stays obvious: advance the
application, then advance the kernel interface. Idle waiting, if any, lives
inside `io_loop.step()` — never in `main`.

## What `step()` Guarantees

Each `step()` call is a bounded unit of synchronous work.

- `Server::step()` drains ready completions, advances state machines, and
  may submit new operations.
- `IOLoop::step()` submits pending operations to the backend, reaps any
  that finished, and writes results back into the caller-owned completion
  slots.

Nothing advances between `step()` calls. There is no background thread, no
waker callback, no implicit scheduling. This is what makes the program
friendly to deterministic replay: given the same inputs and the same prior
state, two `step()` calls produce the same result.

## Completion-Based I/O

The I/O layer is split in two:

- `IO` submits operations.
- `IOLoop` drives the backend and retires them.

```rust
pub trait IO {
    fn open(&self, path: &Path, options: OpenOptions) -> io::Result<Box<dyn IOFile>>;
    fn socket(&self) -> io::Result<Box<dyn IOSocket>>;
    fn mkdir(&self, c: &mut Completion, path: &Path, mode: u32) -> io::Result<()>;
}
```

The important detail is `&mut Completion`. Callers own and reuse completion
slots. A completion is not an ID. It is not a token for looking up state
elsewhere. The slot itself carries:

- the armed operation
- its lifecycle state
- its terminal result
- an optional callback

The per-operation flow:

1. the caller prepares or reuses a `Completion`
2. the caller submits work through a file or socket handle
3. the backend translates the operation into the platform backend
   submission
4. the backend later completes the same `Completion`
5. the caller observes a semantic `CompletionResult` and reuses the slot

Long-lived state machines naturally own long-lived completions:

- a listener owns one accept completion
- a connection owns recv and send completions
- synchronous helper code may own a stack-local completion and drive it by
  polling the backend directly

Higher layers stay backend-agnostic. They reason about `CompletionResult`,
not about backend-specific mechanics (`kqueue`, `io_uring`, or errno retries).
Transient conditions such
as `EAGAIN`, `EWOULDBLOCK`, and `EINTR` stay inside the I/O layer.

## Owned State Machines

Each slot owns its own state machine. `Listener::step()` and
`Connection::step()` know what phase they are in, what completion they are
waiting on, and how to advance themselves. The server's job in
`Server::step()` is just to walk the slots and react to what each one
reports — *not* to encode the transition logic itself.

```rust
pub fn step(&mut self) -> Result<()> {
    for idx in 0..self.listeners.capacity() {
        let Some(listener) = self.listeners.entry_mut(idx) else {
            continue;
        };
        if let ListenerStep::Accepted(socket) = listener.step()? {
            self.insert_connection(socket)?;
        }
    }

    for idx in 0..self.connections.capacity() {
        let Some(conn) = self.connections.entry_mut(idx) else {
            continue;
        };
        match conn.step()? {
            ConnectionStep::Idle | ConnectionStep::Progressed => {}
            ConnectionStep::Close => self.connections.release(idx),
        }
    }

    Ok(())
}
```

Completion callbacks, if any, are small — they publish readiness, not
transitions. The real transition runs when the slot's `step()` notices the
ready completion. The consequence: if server state changed, there is a
small, known set of places to look, and the loop in `Server::step()` stays
free of `recv_result_ready` / `send_result_ready` branches.

## Fixed-Capacity Tables

Tables of long-lived state — listeners, connections — are preallocated. A
worker does not allocate new listener or connection objects on every event.

- a fixed-capacity slab of listeners
- a fixed-capacity slab of connections

When a listener accepts a new socket, it acquires a connection entry from
the preallocated slab. If none is free, the socket is closed and the event
is logged. That policy is explicit, not hidden behind an allocator that
might succeed or fail unpredictably.

The benefits stack up:

- stable ownership for long-lived completions — a slot's address does not
  move
- predictable memory use
- simple simulator behavior
- fewer allocation paths on the hot path

### The `Slab` type

Betelgeuse ships one concrete implementation of this pattern: `Slab<A, T>`.
It owns a `Box<[T], A>` of user-defined entries and an intrusive free list
over the unused ones. The backing array is allocated once at construction
and is never resized.

```rust
use betelgeuse::slab::{Slab, SlabEntry};

let mut connections: Slab<Global, Connection> = Slab::new(Global, 1024);
```

The free list is intrusive: free entries carry the `Option<usize>` link to
the next free entry *inside themselves*. There is no side allocation for
"the list of free indices" — the slab stores `free_head: Option<usize>` and
walks the entries' own links from there.

Entries opt into this by implementing `SlabEntry`:

```rust
pub trait SlabEntry {
    fn new_free(next: Option<usize>) -> Self;
    fn is_free(&self) -> bool;
    fn next_free(&self) -> Option<usize>;
    fn release(&mut self, next: Option<usize>);
}
```

The idiomatic way to satisfy `SlabEntry` is to make the entry's state a
two-variant enum where `Free` carries the free-list link and `Open` (or
`Active`, or whatever the occupied state is called) carries the
domain-specific payload:

```rust
enum ConnectionState {
    Free { next: Option<usize> },
    Open { socket: Box<dyn IOSocket> },
}
```

`new_free` constructs a `Free` entry with the given link. `is_free` and
`next_free` read the variant. `release` swaps back to `Free { next }` and
drops the occupied payload — closing sockets, clearing buffers, resetting
completion slots.

### Acquiring and releasing slots

The hot path has two operations:

- **`acquire() -> Option<usize>`** — unlinks the head of the free list and
  hands back its index. Returns `None` when the slab is full.
- **`release(id)`** — returns a slot to the free list by calling
  `SlabEntry::release` on it and then pushing its id onto `free_head`.

Both are O(1) and allocate nothing. A typical accept path looks like this:

```rust
fn insert_connection(&mut self, socket: Box<dyn IOSocket>) -> io::Result<()> {
    let Some(id) = self.connections.acquire() else {
        socket.close();
        return Ok(());
    };
    if let Err(err) = self
        .connections
        .entry_mut(id)
        .expect("acquired slot")
        .open(socket)
    {
        self.connections.release(id);
        return Err(err);
    }
    Ok(())
}
```

### Iterating and stepping

`tick`-shaped servers walk all slots each step. The public surface is
`capacity() + entry_mut(id)`:

```rust
for idx in 0..self.connections.capacity() {
    let Some(conn) = self.connections.entry_mut(idx) else {
        continue;
    };
    match conn.step()? {
        ConnectionStep::Idle => {}
        ConnectionStep::Progressed => progressed = true,
        ConnectionStep::Close => {
            progressed = true;
            self.connections.release(idx);
        }
    }
}
```

Each entry owns its own state machine — the outer loop just walks them and
reacts to whatever `step()` reports. The state-machine logic for "what's
the next I/O direction?" lives inside `Connection`, not the server.

Because the backing array never resizes, a slot's address is stable for the
lifetime of the slab. That matters: completion slots live inside entries,
and the I/O backend holds raw pointers to them across `step()` calls.

### Invariants

The slab's correctness rests on a few rules:

- Only occupied entries carry domain state (sockets, buffers, armed
  completions).
- Only free entries are in the free list.
- Free and occupied entries partition the array — no entry is in both, none
  is in neither.

`Slab::acquire` and `Slab::release` maintain these. The library's
`proptest`-backed unit tests in `slab.rs` verify the partition under
arbitrary interleavings of acquire/release operations.

## Tasks

Hand-rolled stepped state machines get tedious fast. Even a trivial "create
each component of a path" becomes an enum of phases, a completion slot,
shared cells for the callback to write results into, and an open-coded
`step()` that advances the phase on each call.

A `Task<T>` collapses that into a coroutine resumed explicitly by the
owner:

```rust
pub struct Task<T = ()> {
    coroutine: Option<
        Pin<Box<dyn Coroutine<IOHandle, Yield = TaskYield, Return = io::Result<T>>>>,
    >,
}
```

Key properties:

- it is not a `Future`; there is no `Waker`
- it is not scheduled by the I/O subsystem; nothing wakes it automatically
- the owner advances it with `task.step(&io)` — exactly like everything
  else in the program

When the task cannot make progress because an operation is still in flight,
it yields `TaskYield::Pending`. The owner resumes it again on a later
`step()`. Ownership stays where it was:

- the application owns the task
- the application decides when to resume it
- the I/O layer only completes operations

## `spawn!` and `io_await!`

Tasks are authored with `spawn!`:

```rust
spawn!(|io| {
    // coroutine body
    Ok(())
})
```

Inside the body, `io_await!` hides the repeated pattern of:

- construct or own an operation wrapper
- submit it once
- yield while it is still pending
- decode its completed result

The resulting code reads like a loop but compiles to the same stepped state
machine a human would have written:

```rust
pub fn create_tree(path: &Path) -> Task<()> {
    let target = path.to_path_buf();

    spawn!(|io| {
        let mut current = PathBuf::new();

        for component in target.components() {
            current.push(component);
            if current.as_os_str().is_empty() {
                continue;
            }
            io_await!(io, op::mkdir(&io, &current, 0o755))?;
        }

        Ok(())
    })
}
```

The state machine is still there. The compiler generates it at the
coroutine's resume points rather than asking the author to maintain it by
hand.

## Completion-Backed Operations

`io_await!` does not bypass the completion model — it sits on top of it.

Operation wrappers adapt raw completions into something a task can step.
Each wrapper:

- owns exactly one `Completion`
- submits it once, then polls until the backend fills the result
- decodes the `CompletionResult` into a typed output

A task with N operations in flight owns N long-lived completions. No
per-submission allocation, no completion IDs, no pending-op hash maps.
Tasks improve ergonomics; they do not change the ownership model.

## Summary

The whole model is a small number of rules applied consistently:

1. A single thread loops: `server.step()`, then `io_loop.step()`.
2. Every in-flight I/O operation is a caller-owned `Completion`. The
   backend fills it.
3. Application state transitions happen in `Server::step()`. Callbacks
   publish readiness, not transitions.
4. Listeners and connections live in preallocated slabs.
5. Long-running flows are `Task`s — coroutines the application resumes
   explicitly.

Nothing runs on its own. If state changed, it is because something
explicitly called `step()`.
