# echo example

A minimal TCP echo server built on Betelgeuse. It shows the canonical
single-threaded loop shape: drive application state with `server.step()`,
drive I/O with `io_loop.step()`, and keep completions owned by the caller.

## Run

```sh
cargo run --example echo
```

The server listens on `127.0.0.1:5555`.

## Smoke test

```sh
nc 127.0.0.1 5555
```

Anything you type is echoed back.
