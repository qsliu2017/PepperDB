//! Translated from PostgreSQL 18.3 `src/port/strsep.c`
//! (declaration in `src/include/port.h`).
//!
//! $OpenBSD: strsep.c,v 1.8 2015/08/31 02:53:57 guenther Exp $
//!
//! Copyright (c) 1990, 1993
//!	The Regents of the University of California.  All rights reserved.
//!
//! Redistribution and use in source and binary forms, with or without
//! modification, are permitted provided that the following conditions
//! are met:
//! 1. Redistributions of source code must retain the above copyright
//!    notice, this list of conditions and the following disclaimer.
//! 2. Redistributions in binary form must reproduce the above copyright
//!    notice, this list of conditions and the following disclaimer in the
//!    documentation and/or other materials provided with the distribution.
//! 3. Neither the name of the University nor the names of its contributors
//!    may be used to endorse or promote products derived from this software
//!    without specific prior written permission.
//!
//! THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
//! ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
//! IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
//! ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
//! FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
//! DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
//! OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
//! HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
//! LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
//! OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
//! SUCH DAMAGE.

use crate::prelude::*;

/*
 * Get next token from string *stringp, where tokens are possibly-empty
 * strings separated by characters from delim.
 *
 * Writes NULs into the string at *stringp to end tokens.
 * delim need not remain constant from call to call.
 * On return, *stringp points past the last NUL written (if there might
 * be further tokens), or is NULL (if there are definitely no more tokens).
 *
 * If *stringp is NULL, strsep returns NULL.
 */
#[no_mangle]
pub unsafe extern "C" fn strsep(
    stringp: *mut *mut c_char,
    delim: *const c_char,
) -> *mut c_char {
    let mut s: *mut c_char;
    let mut spanp: *const c_char;
    let mut c: c_int;
    let mut sc: c_int;
    let tok: *mut c_char;

    s = *stringp;
    if s.is_null() {
        return core::ptr::null_mut();
    }
    tok = s;
    loop {
        // c = *s++;
        c = *s as c_int;
        s = s.add(1);
        spanp = delim;
        loop {
            // sc = *spanp++;
            sc = *spanp as c_int;
            spanp = spanp.add(1);
            if sc == c {
                if c == 0 {
                    s = core::ptr::null_mut();
                } else {
                    // s[-1] = 0;
                    *s.offset(-1) = 0;
                }
                *stringp = s;
                return tok;
            }
            if sc == 0 {
                break;
            }
        }
    }
    /* NOTREACHED */
}
