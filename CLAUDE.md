# CLAUDE.md

## Project Overview

PepperDB is a proof-of-concept PostgreSQL implementation in Rust, exploring what PostgreSQL would look like with Rust's memory safety and concurrency model.

**Design principles:**
- Match PostgreSQL behavior in: SQL grammar, storage file format, network protocol, module separation
- Use existing Rust libraries where practical (parser, network protocol) for rapid prototyping
- Same logical module boundaries as PostgreSQL, but not a 1:1 source file mapping
- PoC scope -- not all PostgreSQL features will be covered

## Build Commands

```bash
cargo build          # Build the project
cargo test           # Run all tests
cargo test <name>    # Run a specific test (e.g., cargo test lru_replacer_test)
cargo clippy         # Lint
cargo fmt            # Format code
```

## Architecture

Rust library crate (`pepper_db`). `src/lib.rs` as crate root, submodules in directories with `mod.rs`.

**Current modules:**
- `buffer_pool` -- LRU-K page replacement policy (`LRUKReplacer`)
- `storage` -- heap page layout (8KB pages, null bitmap, dead tuple marking) and disk manager
- `catalog` -- in-memory table/column metadata (simplified pg_class + pg_attribute)
- `parser` -- converts sqlparser-rs AST to PepperDB AST for DDL/DML statements
- `executor` -- routes SELECT to DataFusion, handles DDL/DML directly; includes `HeapTableProvider` bridging heap storage to DataFusion's `TableProvider`
- `server` -- pgwire protocol handler
- `types` -- core type definitions (Bool, Int2, Int4, Int8, Float4, Float8, Text)

**Key dependencies:**
- Apache DataFusion (v44) -- query engine for all SELECT execution
- sqlparser-rs (v0.53) -- SQL parsing for DDL/DML (also used internally by DataFusion)
- pgwire (v0.25) -- PostgreSQL wire protocol

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
- Use `//!` for module-level doc comments (file headers)

### Documentation
- Do NOT write docs everywhere -- avoid docs rot
- Preferred locations: regression tests, source file header comments, `.claude/skills/`, `README.md`, `CLAUDE.md`
- Header comments explain purpose and usage of each source file; maintain after each edit
- Only include comments essential to understanding functionality or conveying non-obvious information

### Workflow
- When modifying multiple files, run file modifications in parallel whenever possible
- When installing any dependency (Cargo crate, apt-get package, etc.), also update `.devcontainer/` so the dev container stays in sync
