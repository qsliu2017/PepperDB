#![doc = "PepperDB: a single-process, async Rust port of PostgreSQL."]
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
