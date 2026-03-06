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

Actively developing. Currently implemented:

- **Query Engine** -- Apache DataFusion handles SELECT parsing, planning, optimization, and execution (GROUP BY, JOINs, subqueries, set operations, window functions, etc.)
- **DDL/DML** -- CREATE TABLE, INSERT, UPDATE, DELETE, DROP TABLE with our own parser and executor
- **Storage** -- PostgreSQL-style heap pages (8KB), null bitmap, dead tuple marking (UPDATE = mark-dead + insert-new)
- **Catalog** -- in-memory table metadata
- **Types** -- bool, int2, int4, int8, float4, float8, text
- **Network** -- PostgreSQL wire protocol via pgwire (connect with `psql`)
- **Buffer Pool** -- LRU-K replacement policy
- **Regression Tests** -- 22 PostgreSQL-style regression tests covering: basics, select, orderby, boolean, int2, int4, int8, float, text, null, update, delete, drop_table, case expressions, general expressions, strings, limit, distinct, aliases, group by, joins, subqueries

## License

See [LICENSE](LICENSE).
