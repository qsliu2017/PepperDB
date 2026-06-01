//! The common prelude every translated PostgreSQL unit imports.
//!
//! `use crate::prelude::*;` is the Rust stand-in for the `#include "postgres.h"`
//! that begins each backend `.c` file: it brings the fundamental types, the
//! Datum helpers, the allocator, and the assertion/error macros into scope.

// The raw C scalar ffi types, under their canonical names, so translated units
// can use `c_char`/`c_int`/`c_void`/... directly after `use crate::prelude::*;`
// (matching how the C source uses `char`/`int`/`void`).
pub use core::ffi::{c_char, c_int, c_long, c_uchar, c_uint, c_ulong, c_void};

// NULL pointer constructors (C `NULL` is pervasive in the source).
pub use core::ptr::{null, null_mut};

// Fundamental types and helpers from c.h.
pub use crate::c::*;

// Datum and conversions from postgres.h.
pub use crate::postgres::*;

// Oid and friends from postgres_ext.h.
pub use crate::postgres_ext::*;

// Allocator interface (palloc.h).
pub use crate::utils::palloc::{
    palloc, palloc0, palloc_extended, pfree, pnstrdup, pstrdup, repalloc, repalloc0,
    CurrentMemoryContext, GetMemoryChunkContext, MemoryContext, MemoryContextAlloc,
    MemoryContextAllocExtended, MemoryContextAllocZero, MemoryContextIsValid,
    MemoryContextSetIdentifier, MemoryContextSwitchTo, TopMemoryContext,
};

// Error reporting levels (elog.h).
pub use crate::utils::elog::{
    errcode, DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5, ERROR, FATAL, INFO, LOG, NOTICE, PANIC,
    WARNING,
};

// Allocation limits + memory-context lifecycle (memutils.h).
pub use crate::utils::memutils::{
    MemoryContextDelete, MemoryContextReset, MaxAllocSize, ALLOCSET_DEFAULT_SIZES,
    ALLOCSET_SMALL_SIZES, ALLOCSET_START_SMALL_SIZES,
};

// Function-like and assertion macros (declared with #[macro_export], so they live
// at the crate root and are re-exported here for `prelude::*` ergonomics).
pub use crate::{elog, ereport, errmsg, lengthof, Assert, AssertMacro};

// Memory-context creation macro (memutils.h). #[macro_export] puts it at the crate
// root; re-export for prelude ergonomics.
pub use crate::AllocSetContextCreate;
