//! snowball/header.h - Replacement header file for Snowball stemmer modules.
//!
//! The Snowball stemmer modules `#include "header.h"` and think they are
//! including `snowball/libstemmer/header.h`. PostgreSQL adjusts CPPFLAGS so this
//! replacement is found instead, ensuring `postgres.h` is included before any
//! system headers (largefile portability), and redefining the Snowball memory
//! allocation interface to PostgreSQL's palloc/pfree family.
//!
//! This C header is almost entirely preprocessor: it `#include`s postgres.h and
//! the original `snowball/libstemmer/header.h`, undefs MAXINT/MININT, and
//! redefines malloc/calloc/realloc/free. There are no typedefs, structs, enums,
//! or function prototypes of its own. The only Rust-meaningful content is the
//! allocation macro remapping, translated below as inline functions.

use crate::c::Size;
use std::ffi::c_void;

// The original Snowball `snowball/libstemmer/header.h` is part of the Snowball
// libstemmer sources (machine-generated, not part of the PostgreSQL include
// tree). When those modules are ported, their header.rs equivalent should be
// `use`d here. Until then we do not re-export anything from it.
// TODO: dedup with snowball/libstemmer/header once ported.

// PostgreSQL memory-management prototypes used by the remapped allocator
// macros. These are defined in utils/mmgr (mcxt). We declare local stubs as
// extern prototypes; the real symbols live in crate::utils::mmgr once wired.
// TODO: dedup - replace these with the actual palloc/palloc0/repalloc/pfree
// from crate::utils::mmgr when available.
unsafe extern "C" {
    fn palloc(size: Size) -> *mut c_void;
    fn palloc0(size: Size) -> *mut c_void;
    fn repalloc(pointer: *mut c_void, size: Size) -> *mut c_void;
    fn pfree(pointer: *mut c_void);
}

/// C: `#define malloc(a) palloc(a)`
#[inline]
pub unsafe fn malloc(a: Size) -> *mut c_void {
    palloc(a)
}

/// C: `#define calloc(a,b) palloc0((a) * (b))`
#[inline]
pub unsafe fn calloc(a: Size, b: Size) -> *mut c_void {
    palloc0(a * b)
}

/// C: `#define realloc(a,b) repalloc(a,b)`
#[inline]
pub unsafe fn realloc(a: *mut c_void, b: Size) -> *mut c_void {
    repalloc(a, b)
}

/// C: `#define free(a) pfree(a)`
#[inline]
pub unsafe fn free(a: *mut c_void) {
    pfree(a)
}
