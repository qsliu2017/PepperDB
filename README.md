# PepperDB

A proof-of-concept PostgreSQL implementation in Rust.

## Motivation

What if PostgreSQL were written in Rust? Rust's ownership model, memory safety guarantees, and first-class concurrency support make it an appealing choice for building a database system. PepperDB explores this idea by reimplementing PostgreSQL's core components in Rust.

## Goals

- **PostgreSQL-compatible SQL grammar** — same SQL dialect and behavior
- **PostgreSQL-compatible storage format** — heap files, page layout matching PostgreSQL on disk
- **PostgreSQL wire protocol** — connect with standard PostgreSQL clients (`psql`, drivers)
- **Same module separation** — parser, catalog, executor, storage, buffer pool, WAL, etc.
- **Leverage the Rust ecosystem** — use existing libraries for parsing and protocol handling where practical

## Non-Goals

- Full feature parity with PostgreSQL (this is a PoC)
- 1:1 source file mapping with PostgreSQL

## Building

```bash
cargo build
cargo test
```

## Status

Early stage. Currently implemented:

- **Buffer Pool** — LRU-K replacement policy

## License

See [LICENSE](LICENSE).
