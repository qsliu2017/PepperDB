#![doc = "PepperDB: a single-process, async Rust port of PostgreSQL."]
// The connect-to-database backend future is deeply nested (executor -> access
// methods -> smgr -> md); auto-trait (Send/Sync) solving over it exceeds the
// default depth of 128. Raise the limit so `tokio::spawn(backend_main(..))`
// resolves the (genuine) Send bound instead of overflowing.
#![recursion_limit = "256"]
#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
#![allow(dead_code, unused_imports, unused_variables)]
#![allow(unsafe_op_in_unsafe_fn)]

// === scaffold: child modules (Phase 0) ===
pub mod access;
pub mod archive;
pub mod backend;
pub mod backup;
pub mod bootstrap;
pub mod c;
pub mod catalog;
pub mod commands;
pub mod common;
pub mod datatype;
pub mod executor;
pub mod fe_utils;
pub mod fmgr;
pub mod foreign;
pub mod funcapi;
pub mod getopt_long;
pub mod jit;
#[path = "lib/mod.rs"] // stem clashes with crate root src/lib.rs
pub mod lib;
pub mod libpq;
pub mod mb;
pub mod miscadmin;
pub mod nodes;
pub mod optimizer;
pub mod parser;
pub mod partitioning;
pub mod pch;
pub mod pg_config;
pub mod pg_config_ext;
pub mod pg_config_manual;
pub mod pg_config_os;
pub mod pg_getopt;
pub mod pg_trace;
pub mod pgstat;
pub mod pgtar;
pub mod pgtime;
pub mod port;
pub mod portability;
pub mod postgres;
pub mod postgres_ext;
pub mod postgres_fe;
pub mod postmaster;
pub mod regex;
pub mod replication;
pub mod rewrite;
pub mod session;
pub mod shared_state;
pub mod snowball;
pub mod statistics;
pub mod storage;
pub mod tcop;
pub mod tsearch;
pub mod utils;
pub mod varatt;
pub mod windowapi;
// === end scaffold ===

/// PG's `Assert` (error.md s3.2): an internal-invariant check that runs in debug
/// builds only and, on failure, takes the PANIC path -- an UNCATCHABLE process
/// abort (via `ereport!(PANIC, ...)`), NOT a catchable `std::panic!`. Compiled out
/// in release (like `debug_assert!`). Use `crate::assert!`; do NOT use
/// `std::assert!`/`std::debug_assert!` for invariants -- a std assert is a
/// catchable unwind that `catch_unwind` could swallow, hiding a corruption signal.
///
/// Call as `crate::assert!(cond)` or `crate::assert!(cond, "fmt {x}")`. Unqualified
/// `assert!` is unaffected and still resolves to `std::assert!`.
#[macro_export]
macro_rules! assert {
    ($cond:expr $(,)?) => {{
        if ::core::cfg!(debug_assertions) && !$cond {
            $crate::ereport!($crate::utils::elog::PANIC, |__e: &mut $crate::utils::elog::ErrorData| {
                __e.errmsg_internal(::core::concat!("assertion failed: ", ::core::stringify!($cond)));
            });
        }
    }};
    ($cond:expr, $($arg:tt)+) => {{
        if ::core::cfg!(debug_assertions) && !$cond {
            $crate::ereport!($crate::utils::elog::PANIC, |__e: &mut $crate::utils::elog::ErrorData| {
                __e.errmsg_internal(::std::format!($($arg)+));
            });
        }
    }};
}
