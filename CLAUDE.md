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

**Planned modules** (following PostgreSQL's architecture):
- Parser, Catalog, Executor, Storage, WAL, Network

## Conventions

### Code style
- ASCII only in all source files, SQL tests, and expected output -- no emojis, no Unicode dashes/quotes (use `-`, `--`, `'`, `"`)
- Write clean, minimal code; fewer lines is better
- Prioritize simplicity for effective and maintainable software
- Rust 2021 edition
- Tests are inline (`#[cfg(test)] mod test`) within the source files they test

### Documentation
- Do NOT write docs everywhere -- avoid docs rot
- Preferred locations: regression tests, source file header comments, `.claude/skills/`, `README.md`, `CLAUDE.md`
- Header comments explain purpose and usage of each source file; maintain after each edit
- Only include comments essential to understanding functionality or conveying non-obvious information

### Workflow
- When modifying multiple files, run file modifications in parallel whenever possible
- When installing any dependency (Cargo crate, apt-get package, etc.), also update `.devcontainer/` so the dev container stays in sync
