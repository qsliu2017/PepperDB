//! PepperDB - a 1:1 Rust translation of the PostgreSQL backend.
//!
//! TRANSLATION CONVENTIONS (read before adding a module)
//! =====================================================
//! PostgreSQL source under `postgres/src` is translated file-for-file into Rust.
//! Because Rust has no separate header files, each translated unit merges the C
//! header and its implementation into one `.rs` file:
//!
//!   postgres/src/include/<subdir>/<name>.h  +  postgres/src/backend/<subdir>/<name>.c
//!     ==>  src/<subdir>/<name>.rs
//!
//! (Shared code lives under postgres/src/common and postgres/src/port; it maps
//! to src/common/<name>.rs and src/port/<name>.rs respectively.)
//!
//! Fidelity rules:
//!   * Keep PostgreSQL identifiers verbatim (`Oid`, `int32`, `pairingheap_node`,
//!     `appendStringInfo`). The crate globally allows non-snake/camel-case names
//!     so the translation reads like the C source.
//!   * C `struct`/`union` become `#[repr(C)]` Rust structs/unions. Intrusive,
//!     pointer-linked data structures are translated literally with raw pointers
//!     and `unsafe`; this is the faithful 1:1 form even though it is not idiomatic
//!     safe Rust.
//!   * `palloc`/`pfree`/`Assert`/`elog`/`ereport` come from `crate::prelude`.
//!   * Where a C construct has no direct Rust equivalent yet (varargs printf,
//!     `setjmp`/`longjmp` error unwinding, shared memory), the call site is kept
//!     but backed by a clearly-marked bootstrap shim: search for `TODO(pg-port)`.
//!
//! Every translated `.c` file begins with `use crate::prelude::*;`, mirroring the
//! `#include "postgres.h"` that begins every PostgreSQL backend file.

#![allow(non_snake_case)] // PostgreSQL uses CamelCase functions: DatumGetBool, appendStringInfo
#![allow(non_camel_case_types)] // PostgreSQL uses lower_snake types: int32, pairingheap_node
#![allow(non_upper_case_globals)] // PostgreSQL uses mixed-case consts: InvalidOid, MaxAllocSize
#![allow(dead_code)] // a partial port has many not-yet-used items
#![allow(unused_macros)]
// Every translated unit begins with `use crate::prelude::*;` (the stand-in for
// `#include "postgres.h"`), even when a small file uses none of it directly.
#![allow(unused_imports)]
#![allow(unused_variables)] // TODO(pg-port) stubs and faithful translations leave some params unused
#![allow(unused_parens)] // faithful 1:1 translations preserve C's parenthesization
#![allow(unused_assignments)] // C's declare-then-reassign patterns translate literally
// Several modules bind the same libc variadic (e.g. snprintf) with different fixed
// argument lists; the C ABI resolves them to the one real function.
#![allow(clashing_extern_declarations)]
#![allow(clippy::all)]
// We mirror PostgreSQL's `lib` subsystem at src/lib/mod.rs (no src/lib.rs exists,
// so resolution is unambiguous and no library target is created).
#![allow(special_module_name)]
// PostgreSQL has process-global mutable state (e.g. CurrentMemoryContext); the
// bootstrap shims model it with `static mut`. Reads/writes stay in unsafe fns.
#![allow(static_mut_refs)]
// extern "C" fn pointers pass PG structs (MemoryContextData etc.) by reference; this
// is the deliberate C ABI, not an accident.
#![allow(improper_ctypes)]
#![allow(improper_ctypes_definitions)]
// Faithful 1:1 translations preserve C's exhaustive switch + default arm, and C's
// `{0}` struct initialization (fields are set before use).
#![allow(unreachable_patterns)]
#![allow(unreachable_code)] // diverging loops/switches end with a safety unreachable!()
#![allow(unused_unsafe)] // faithful ports wrap already-unsafe contexts in extra unsafe
#![allow(invalid_value)]
#![allow(unused_labels)] // C goto-as-labeled-block leaves some labels unreferenced
#![allow(function_casts_as_integer)] // C casts signal handlers / callbacks fn-items directly to integers
// C #ifdef knobs (USE_ASSERT_CHECKING, USE_VALGRIND) are not cargo features here.
#![allow(unexpected_cfgs)]

// ---- pg_config.h / pg_config_manual.h: build-time configuration constants ----
pub mod bootstrap;
pub mod pg_config;

// ---- Precompiled-header aggregators (no runtime symbols) ----
pub mod pch;

// ---- Fundamental definitions: postgres_ext.h, c.h, postgres.h ----
pub mod postgres_ext;
pub use postgres_ext::Oid;
#[macro_use]
pub mod c;
pub mod postgres;
pub mod varatt;

// ---- Standalone top-level headers (src/include/*.h) ----
pub mod pg_config_manual;
pub mod miscadmin;
pub mod pg_getopt;
pub mod pg_trace;
pub mod pgtar;
pub mod pgtime;
pub mod postgres_fe;
pub mod windowapi;

// ---- Backend support subsystems ----
pub mod access;
pub mod backup;
pub mod catalog;
pub mod commands;
pub mod common;
pub mod archive;
pub mod executor;
pub mod fe_utils;
pub mod foreign;
pub mod jit;
pub mod libpq;
pub mod mb;
pub mod nodes;
pub mod optimizer;
pub mod parser;
pub mod partitioning;
pub mod port;
pub mod portability;
pub mod postmaster;
pub mod regex;
pub mod replication;
pub mod rewrite;
pub mod snowball;
pub mod statistics;
pub mod storage;
pub mod tcop;
pub mod tsearch;
pub mod utils;
pub mod lib;

// ---- The common prelude pulled in by every translated unit ----
pub mod prelude;

fn main() {
    // TODO(pg-port): translate postgres/src/backend/main/main.c (PostgresMain entry).
    eprintln!("PepperDB: PostgreSQL backend port (work in progress).");
}
