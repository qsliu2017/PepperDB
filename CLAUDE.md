# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PepperDB is a proof-of-concept PostgreSQL implementation in Rust. The goal is to explore what PostgreSQL would look like rewritten in Rust, leveraging Rust's memory safety and concurrency model.

**Design principles:**
- Match PostgreSQL behavior in: SQL grammar, storage file format, network protocol, module separation
- Use existing Rust libraries where practical (parser, network protocol) for rapid prototyping
- Not a 1:1 source file mapping with PostgreSQL, but same logical module boundaries
- PoC scope — not all PostgreSQL features will be covered

## Build Commands

```bash
cargo build          # Build the project
cargo test           # Run all tests
cargo test <name>    # Run a specific test (e.g., cargo test lru_replacer_test)
cargo clippy         # Lint
cargo fmt            # Format code
```

## Architecture

The project is structured as a Rust library crate (`pepper_db`).

**Current modules:**
- `buffer_pool` — Buffer pool management, starting with the LRU-K page replacement policy (`LRUKReplacer`)

**Planned modules** (following PostgreSQL's architecture):
- Parser (SQL parsing — will use a library)
- Catalog (system catalog / metadata)
- Executor (query execution)
- Storage (heap files, page layout — PostgreSQL-compatible format)
- WAL (write-ahead logging)
- Network (PostgreSQL wire protocol — will use a library)

## Conventions

- Rust 2021 edition
- Standard Rust project layout: `src/lib.rs` as crate root, submodules in directories with `mod.rs`
- Tests are inline (`#[cfg(test)] mod test`) within the source files they test
