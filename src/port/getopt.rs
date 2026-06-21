//! Translated from PostgreSQL 18.3 `src/port/getopt.c`
//! (declarations in `src/include/pg_getopt.h`).
//!
//! BSD getopt(3): parse an argc/argv argument vector against an option string.
//!
//! Copyright (c) 1987, 1993, 1994
//!	The Regents of the University of California.  All rights reserved.
//!
//! Redistribution and use in source and binary forms, with or without
//! modification, are permitted provided that the following conditions
//! are met:
//! 1. Redistributions of source code must retain the above copyright
//!	  notice, this list of conditions and the following disclaimer.
//! 2. Redistributions in binary form must reproduce the above copyright
//!	  notice, this list of conditions and the following disclaimer in the
//!	  documentation and/or other materials provided with the distribution.
//! 3. Neither the name of the University nor the names of its contributors
//!	  may be used to endorse or promote products derived from this software
//!	  without specific prior written permission.
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
//!
//! Port notes:
//!   - On this platform configure does not find opterr/optind/optopt/optarg in
//!     libc's getopt module (the `#ifndef HAVE_INT_OPTERR` branch is taken), so
//!     we define them here as `pub static mut`.
//!   - The function-local `static char *place` in C becomes a private module
//!     `static mut PLACE`, which is sound under getopt's single-threaded use.
//!   - `fprintf(stderr, ...)` for the two diagnostics is rendered with
//!     `eprintln!`, matching the established convention in this port (see
//!     `path.rs`).

#![allow(static_mut_refs)]

use crate::prelude::*;

/*
 * On OpenBSD and some versions of Solaris, opterr and friends are defined in
 * core libc rather than in a separate getopt module.  Define these variables
 * only if configure found they aren't there by default; otherwise, this
 * module and its callers will just use libc's variables.  (We assume that
 * testing opterr is sufficient for all of these.)
 */
// #ifndef HAVE_INT_OPTERR

// #[no_mangle]: this getopt() overrides libc; its globals must export the C symbols too so
// extern-"C" readers (postmaster etc.) see what getopt() sets (else they read libc's unset ones).
#[no_mangle] pub static mut opterr: c_int = 1; /* if error message should be printed */
#[no_mangle] pub static mut optind: c_int = 1; /* index into parent argv vector */
#[no_mangle] pub static mut optopt: c_int = 0; /* character checked for validity */
#[no_mangle] pub static mut optarg: *mut c_char = null_mut(); /* argument associated with option */

// #endif

const BADCH: c_int = '?' as c_int;
const BADARG: c_int = ':' as c_int;
// #define EMSG ""
// C's EMSG is the empty string ""; we represent the "no place" sentinel with a
// pointer to a single NUL byte so that the `*PLACE` dereference reads '\0'.
static EMSG: c_char = 0;

/* option letter processing */
static mut PLACE: *const c_char = &EMSG; /* = EMSG */

/*
 * strchr - locate first occurrence of c in the NUL-terminated string s.
 * Returns a pointer to the located byte, or NULL if c does not occur.  The
 * terminating NUL is considered part of the string, matching libc strchr.
 */
#[inline]
unsafe fn strchr(mut s: *const c_char, c: c_int) -> *const c_char {
    let ch = c as c_char;
    loop {
        if *s == ch {
            return s;
        }
        if *s == 0 {
            return null();
        }
        s = s.add(1);
    }
}

/*
 * getopt
 *	Parse argc/argv argument vector.
 *
 * This implementation does not use optreset.  Instead, we guarantee that
 * it can be restarted on a new argv array after a previous call returned -1,
 * if the caller resets optind to 1 before the first call of the new series.
 * (Internally, this means we must be sure to reset "place" to EMSG before
 * returning -1.)
 */
#[no_mangle]
pub unsafe extern "C" fn getopt(
    nargc: c_int,
    nargv: *const *mut c_char,
    ostr: *const c_char,
) -> c_int {
    let oli: *const c_char; /* option letter list index */

    if *PLACE == 0 {
        /* update scanning pointer */
        // if (optind >= nargc || *(place = nargv[optind]) != '-')
        PLACE = *nargv.offset(optind as isize);
        if optind >= nargc || *PLACE != '-' as c_char {
            PLACE = &EMSG;
            return -1;
        }
        // if (place[1] && *++place == '-' && place[1] == '\0')
        if *PLACE.offset(1) != 0 && {
            PLACE = PLACE.add(1);
            *PLACE == '-' as c_char
        } && *PLACE.offset(1) == 0
        {
            /* found "--" */
            optind += 1;
            PLACE = &EMSG;
            return -1;
        }
    }
    /* option letter okay? */
    // if ((optopt = (int) *place++) == (int) ':' || !(oli = strchr(ostr, optopt)))
    optopt = *PLACE as c_int;
    PLACE = PLACE.add(1);
    oli = strchr(ostr, optopt);
    if optopt == ':' as c_int || oli.is_null() {
        /*
         * if the user didn't specify '-' as an option, assume it means -1.
         */
        if optopt == '-' as c_int {
            PLACE = &EMSG;
            return -1;
        }
        if *PLACE == 0 {
            optind += 1;
        }
        if opterr != 0 && *ostr != ':' as c_char {
            // (void) fprintf(stderr, "illegal option -- %c\n", optopt);
            eprintln!("illegal option -- {}", optopt as u8 as char);
        }
        return BADCH;
    }
    // if (*++oli != ':')
    if *oli.add(1) != ':' as c_char {
        /* don't need argument */
        optarg = null_mut();
        if *PLACE == 0 {
            optind += 1;
        }
    } else {
        /* need an argument */
        if *PLACE != 0 {
            /* no white space */
            optarg = PLACE as *mut c_char;
        } else {
            // else if (nargc <= ++optind)
            optind += 1;
            if nargc <= optind {
                /* no arg */
                PLACE = &EMSG;
                if *ostr == ':' as c_char {
                    return BADARG;
                }
                if opterr != 0 {
                    // (void) fprintf(stderr,
                    //                "option requires an argument -- %c\n",
                    //                optopt);
                    eprintln!("option requires an argument -- {}", optopt as u8 as char);
                }
                return BADCH;
            } else {
                /* white space */
                optarg = *nargv.offset(optind as isize);
            }
        }
        PLACE = &EMSG;
        optind += 1;
    }
    optopt /* dump back option letter */
}
