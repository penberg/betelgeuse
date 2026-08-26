# memcached example

An in-memory memcached-style server built on Betelgeuse. Speaks the ASCII
protocol subset `get`, `set`, `add`, `replace`, `delete`, `version`, and
`quit`. Fixed-capacity slabs for listeners and connections with no allocation
on the hot path.

## Run

```sh
cargo run --example memcached
```

The server listens on `127.0.0.1:11211`.

## Smoke test

```sh
nc 127.0.0.1 11211
set greeting 0 0 5
hello
get greeting
```

## Load testing with memaslap

[memaslap](https://docs.libmemcached.org/bin/memaslap.html) is the
libmemcached load generator. It ships with `libmemcached-tools` on
Debian/Ubuntu and `libmemcached` on Fedora.

```sh
memaslap -s 127.0.0.1:11211 -t 30s -c 64 -T 4
```

Useful flags:

- `-s` server list (`host:port`)
- `-t` test duration
- `-c` concurrent clients
- `-T` worker threads
- `-x` fixed number of operations (alternative to `-t`)
- `-S` stats interval, e.g. `-S 5s`
