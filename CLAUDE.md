# CLAUDE.md

## Project Overview

PepperDB is a PostgreSQL implementation in Rust, targeting production readiness. It leverages Rust's memory safety and concurrency model to build a correct, durable, and performant database.

**Design principles:**
- Match PostgreSQL behavior in: SQL grammar, storage file format, network protocol, module separation
- Use existing Rust libraries where practical (parser, network protocol)
- Match PostgreSQL's module and file organization (e.g., `access/heap/`, `storage/buffer/`, `catalog/`)
- Production-ready: correctness, durability, and performance for real workloads

## Build Commands

```bash
cargo build                    # Build the project
cargo test                     # Run all tests
cargo test <name>              # Run a specific test (e.g., cargo test lru_replacer_test)
cargo fmt                      # Format code
cargo fmt -- --check           # Check formatting (CI)
cargo clippy -- -D warnings    # Lint, deny all warnings (CI)
```

## Architecture

Rust library crate (`pepper_db`). `src/lib.rs` as crate root, submodules in directories with `mod.rs`.

**Current modules:**
- `access/heap` -- heap tuple operations (insert, read, build, MVCC visibility, VACUUM)
- `access/heap/visibilitymap` -- visibility map (1 bit per heap page)
- `access/nbtree` -- B-tree index access method
- `access/transam` -- transaction manager, snapshots, XID constants
- `access/transam/clog` -- commit log (2 bits per XID)
- `access/transam/pg_control` -- control file (system state, checkpoint LSN)
- `access/transam/xlog` -- WAL writer (16MB segments, page headers)
- `access/transam/xlogrecord` -- WAL record serialization (CRC-32C)
- `access/transam/xlogrecovery` -- WAL replay for crash recovery
- `storage/bufpage` -- page-level ops (8KB pages, checksums, ItemId, LSN)
- `storage/smgr` -- storage manager / disk I/O (read/write pages with checksums)
- `storage/freespace` -- free space map (1 byte per heap page)
- `buffer_pool` -- LRU-K page replacement policy (`LRUKReplacer`)
- `catalog` -- in-memory table/column metadata (simplified pg_class + pg_attribute)
- `parser` -- converts sqlparser-rs AST to PepperDB AST for DDL/DML statements
- `executor` -- routes SELECT to DataFusion, handles DDL/DML directly; includes `HeapTableProvider` bridging heap storage to DataFusion's `TableProvider`
- `server` -- pgwire protocol handler
- `types` -- core type definitions (Bool, Int2, Int4, Int8, Float4, Float8, Text)

**Key dependencies:**
- Apache DataFusion (v44) -- query engine for all SELECT execution
- sqlparser-rs (v0.53) -- SQL parsing for DDL/DML (also used internally by DataFusion)
- pgwire (v0.25) -- PostgreSQL wire protocol

**Reference implementation:**
- `postgres/` -- PostgreSQL 18 source (git submodule, `REL_18_STABLE` branch). Use as the authoritative reference for behavior, storage format, and protocol details. Read the C source when implementing or debugging PepperDB features.

**Planned modules:**
- WAL, Buffer Pool integration with storage

## Conventions

### Code style
- ASCII only in all source files, SQL tests, and expected output -- no emojis, no Unicode dashes/quotes (use `-`, `--`, `'`, `"`)
- Write clean, minimal code; fewer lines is better
- Prioritize simplicity for effective and maintainable software
- Rust 2021 edition
- Tests are inline (`#[cfg(test)] mod test`) within the source files they test
- Prefer methods over free functions when a struct is the natural receiver
- Prefer iterator chains (`.map()`, `.filter()`, `.collect()`) over `let mut vec` + `for` loop + push
- Prefer `const fn` for pure computation functions (no I/O, no allocation, no mutable slice ops)
- Use `//!` for module-level doc comments (file headers)

### Documentation
- Do NOT write docs everywhere -- avoid docs rot
- Preferred locations: regression tests, source file header comments, `.claude/skills/`, `README.md`, `CLAUDE.md`
- Header comments explain purpose and usage of each source file; maintain after each edit
- Only include comments essential to understanding functionality or conveying non-obvious information

### Workflow
- Before committing, always run `cargo fmt` and `cargo clippy -- -D warnings` to match CI checks
- When modifying multiple files, run file modifications in parallel whenever possible
- When installing any dependency (Cargo crate, apt-get package, etc.), also update `.devcontainer/` so the dev container stays in sync
