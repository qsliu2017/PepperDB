//! Translated from PostgreSQL `src/port/strnlen.c`
//! (declaration in `src/include/port.h`).
//!
//! Fallback implementation of strnlen().
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

/*
 * Implementation of posix' strnlen for systems where it's not available.
 *
 * Returns the number of characters before a null-byte in the string pointed
 * to by str, unless there's no null-byte before maxlen. In the latter case
 * maxlen is returned.
 */
#[no_mangle]
pub unsafe extern "C" fn strnlen(str: *const c_char, mut maxlen: Size) -> Size {
    let mut p = str;

    // while (maxlen-- > 0 && *p)
    //   In C, maxlen-- > 0 evaluates the old value then decrements; the loop
    //   continues while the old value was > 0 (so it inspects up to maxlen
    //   bytes) and the current byte is non-NUL.
    loop {
        let old = maxlen;
        maxlen = maxlen.wrapping_sub(1);
        if !(old > 0 && *p != 0) {
            break;
        }
        p = p.add(1);
    }
    // return p - str;
    (p as Size).wrapping_sub(str as Size)
}
