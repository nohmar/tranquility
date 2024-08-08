# Tranquility

Fly.io released a series of distributed system challenges, specifically for
learning how these systems are built, tested, and optimized.

You can find the challenge at [https://fly.io/dist-sys/](https://fly.io/dist-sys/).

I've added my solution(s) to this repo which can be compiled and run using the following command:

```
cargo build -r
```

Then, in the Maelstrom directory:

```
./maelstrom test -w broadcast --bin $PATH_TO_TRANQUILTY/target/release/tranquility ...
```

# Challenge TODO list
- [x] Echo server
- [x] Unique ID generation
- [x] Broadcast
    - [x] Single node broadcast
    - [x] Multi-node broadcast
    - [x] Fault tolerant broadcast
    - [x] Efficient broadcast I
    - [x] Efficient broadcast II
- [ ] Grow only counter
- [ ] Kafka style log
- [ ] Totally Available

# Resources
- [Maelstrom Repo](https://github.com/jepsen-io/maelstrom)
- [Another solution as a Rust crate](https://github.com/sitano/maelstrom-rust-node)

