//! Detect whether src/common functions came from frontend or backend.
//!
//! Translated from:
//!   - src/include/common/link-canary.h
//!   - src/common/link-canary.c
//!
//! Portions Copyright (c) 2018-2025, PostgreSQL Global Development Group

use crate::prelude::*;

/*
 * This function just reports whether this file was compiled for frontend
 * or backend environment.  We need this because in some systems, mainly
 * ELF-based platforms, it is possible for a shlib (such as libpq) loaded
 * into the backend to call a backend function named XYZ in preference to
 * the shlib's own function XYZ.  That's bad if the two functions don't
 * act identically.  This exact situation comes up for many functions in
 * src/common and src/port, where the same function names exist in both
 * libpq and the backend but they don't act quite identically.  To verify
 * that appropriate measures have been taken to prevent incorrect symbol
 * resolution, libpq should test that this function returns true.
 */
#[no_mangle]
pub extern "C" fn pg_link_canary_is_frontend() -> bool {
    // This unit is part of the BACKEND build of libpgcommon.
    false
    // TODO(pg-port): the FRONTEND build would `return true;` here.
}
