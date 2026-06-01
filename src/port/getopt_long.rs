//! Translated from PostgreSQL 18.3 `src/port/getopt_long.c`
//! (declarations in `src/include/getopt_long.h`, which in turn includes
//! `src/include/pg_getopt.h` for the `optarg`/`optind`/`opterr`/`optopt`
//! globals).
//!
//! getopt_long() -- long options parser
//!
//! Portions Copyright (c) 1987, 1993, 1994
//! The Regents of the University of California.  All rights reserved.
//!
//! Portions Copyright (c) 2003
//! PostgreSQL Global Development Group
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
//! src/port/getopt_long.c

#![allow(static_mut_refs)]

use crate::prelude::*;

// ----------------------------------------------------------------
//   getopt_long.h declarations
// ----------------------------------------------------------------

/* struct option -- a single long-option description (getopt_long.h). */
#[repr(C)]
pub struct option {
    pub name: *const c_char,
    pub has_arg: c_int,
    pub flag: *mut c_int,
    pub val: c_int,
}

pub const no_argument: c_int = 0;
pub const required_argument: c_int = 1;
pub const optional_argument: c_int = 2;

// ----------------------------------------------------------------
//   getopt(3) global state (pg_getopt.h)
// ----------------------------------------------------------------
//
// TODO(pg-port): in the C build these are the *same* objects that
// src/port/getopt.c (and the platform getopt) reference -- a single set of
// process-global `optarg`/`optind`/`opterr`/`optopt` shared across both
// translation units.  Here each ported file is kept self-contained with its
// own `pub static mut`, so getopt.rs (when ported) would declare separate
// Rust items; callers must use one or the other consistently within a series.
pub static mut optarg: *mut c_char = null_mut();
pub static mut optind: c_int = 1;
pub static mut opterr: c_int = 1;
pub static mut optopt: c_int = 0;

// ----------------------------------------------------------------
//   libc / <string.h> bindings
// ----------------------------------------------------------------
//
// The C source uses fprintf(stderr, ...), strcspn(), strlen(), strncmp() and
// strchr().  We bind them directly so behavior matches the C 1:1.
extern "C" {
    /// fprintf(3) onto a FILE*; we only target stderr.
    fn fprintf(stream: *mut c_void, format: *const c_char, ...) -> c_int;

    fn strcspn(s: *const c_char, reject: *const c_char) -> usize;
    fn strlen(s: *const c_char) -> usize;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;

    // stderr is a `FILE *` global.  glibc names it `stderr`; the BSD/macOS C
    // library exposes the underlying array as `__stderrp`.
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios", target_vendor = "apple"),
        link_name = "__stderrp"
    )]
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")),
        link_name = "stderr"
    )]
    static mut pg_stderr: *mut c_void;
}

const BADCH: c_int = '?' as c_int;
const BADARG: c_int = ':' as c_int;
// #define EMSG "" -- the empty C string literal "\0".
const EMSG: *mut c_char = c"".as_ptr() as *mut c_char;

/*
 * getopt_long
 *	Parse argc/argv argument vector, with long options.
 *
 * This implementation does not use optreset.  Instead, we guarantee that
 * it can be restarted on a new argv array after a previous call returned -1,
 * if the caller resets optind to 1 before the first call of the new series.
 * (Internally, this means we must be sure to reset "place" to EMSG,
 * "nonopt_start" to -1, and "force_nonopt" to false before returning -1.)
 *
 * Note that this routine reorders the pointers in argv (despite the const
 * qualifier) so that all non-options will be at the end when -1 is returned.
 */
//
// # Safety
// `argv` must point to `argc` valid C-string pointers, `optstring` must be a
// valid NUL-terminated string, `longopts` must be a NULL-`name`-terminated
// array of `option`, and `longindex` is either NULL or points to a writable
// `c_int`.
pub unsafe fn getopt_long(
    argc: c_int,
    argv: *const *mut c_char,
    optstring: *const c_char,
    longopts: *const option,
    longindex: *mut c_int,
) -> c_int {
    static mut place: *mut c_char = EMSG; /* option letter processing */
    let oli: *const c_char; /* option letter list index */
    static mut nonopt_start: c_int = -1;
    static mut force_nonopt: bool = false;

    if *place == 0 {
        /* update scanning pointer */
        let args: *mut *mut c_char = argv as *mut *mut c_char;

        'retry: loop {
            /*
             * If we are out of arguments or only non-options remain, return -1.
             */
            if optind >= argc || optind == nonopt_start {
                place = EMSG;
                nonopt_start = -1;
                force_nonopt = false;
                return -1;
            }

            place = *argv.offset(optind as isize);

            /*
             * An argument is a non-option if it meets any of the following
             * criteria: it follows an argument that is equivalent to the string
             * "--", it does not start with '-', or it is equivalent to the string
             * "-".  When we encounter a non-option, we move it to the end of argv
             * (after shifting all remaining arguments over to make room), and
             * then we try again with the next argument.
             */
            if force_nonopt || *place.offset(0) != b'-' as c_char || *place.offset(1) == b'\0' as c_char {
                let mut i = optind;
                while i < argc - 1 {
                    *args.offset(i as isize) = *args.offset((i + 1) as isize);
                    i += 1;
                }
                *args.offset((argc - 1) as isize) = place;

                if nonopt_start == -1 {
                    nonopt_start = argc - 1;
                } else {
                    nonopt_start -= 1;
                }

                continue 'retry;
            }

            place = place.add(1);

            if *place.offset(0) == b'-' as c_char && *place.offset(1) == b'\0' as c_char {
                /* found "--", treat it as end of options */
                optind += 1;
                force_nonopt = true;
                continue 'retry;
            }

            if *place.offset(0) == b'-' as c_char && *place.offset(1) != 0 {
                /* long option */
                let namelen: usize;
                let mut i: c_int;

                place = place.add(1);

                namelen = strcspn(place, c"=".as_ptr());
                i = 0;
                while !(*longopts.offset(i as isize)).name.is_null() {
                    if strlen((*longopts.offset(i as isize)).name) == namelen
                        && strncmp(place, (*longopts.offset(i as isize)).name, namelen) == 0
                    {
                        let has_arg: c_int = (*longopts.offset(i as isize)).has_arg;

                        if has_arg != no_argument {
                            if *place.offset(namelen as isize) == b'=' as c_char {
                                optarg = place.offset(namelen as isize + 1);
                            } else if optind < argc - 1 && has_arg == required_argument {
                                optind += 1;
                                optarg = *argv.offset(optind as isize);
                            } else {
                                if *optstring.offset(0) == b':' as c_char {
                                    return BADARG;
                                }

                                if opterr != 0 && has_arg == required_argument {
                                    fprintf(
                                        pg_stderr,
                                        c"%s: option requires an argument -- %s\n".as_ptr(),
                                        *argv.offset(0),
                                        place,
                                    );
                                }

                                place = EMSG;
                                optind += 1;

                                if has_arg == required_argument {
                                    return BADCH;
                                }
                                optarg = null_mut();
                            }
                        } else {
                            optarg = null_mut();
                            if *place.offset(namelen as isize) != 0 {
                                /* XXX error? */
                            }
                        }

                        optind += 1;

                        if !longindex.is_null() {
                            *longindex = i;
                        }

                        place = EMSG;

                        if (*longopts.offset(i as isize)).flag.is_null() {
                            return (*longopts.offset(i as isize)).val;
                        } else {
                            *(*longopts.offset(i as isize)).flag = (*longopts.offset(i as isize)).val;
                            return 0;
                        }
                    }
                    i += 1;
                }

                if opterr != 0 && *optstring.offset(0) != b':' as c_char {
                    fprintf(
                        pg_stderr,
                        c"%s: illegal option -- %s\n".as_ptr(),
                        *argv.offset(0),
                        place,
                    );
                }
                place = EMSG;
                optind += 1;
                return BADCH;
            }

            // The C falls through out of the `if (!*place)` block to the short
            // option handling below; break out of the retry loop to do so.
            break 'retry;
        }
    }

    /* short option */
    // optopt = (int) *place++;
    optopt = *place as c_int;
    place = place.add(1);

    oli = strchr(optstring, optopt);
    if oli.is_null() {
        if *place == 0 {
            optind += 1;
        }
        if opterr != 0 && *optstring != b':' as c_char {
            fprintf(
                pg_stderr,
                c"%s: illegal option -- %c\n".as_ptr(),
                *argv.offset(0),
                optopt,
            );
        }
        return BADCH;
    }

    if *oli.offset(1) != b':' as c_char {
        /* don't need argument */
        optarg = null_mut();
        if *place == 0 {
            optind += 1;
        }
    } else {
        /* need an argument */
        if *place != 0 {
            /* no white space */
            optarg = place;
        } else if argc <= {
            optind += 1;
            optind
        } {
            /* no arg */
            place = EMSG;
            if *optstring == b':' as c_char {
                return BADARG;
            }
            if opterr != 0 {
                fprintf(
                    pg_stderr,
                    c"%s: option requires an argument -- %c\n".as_ptr(),
                    *argv.offset(0),
                    optopt,
                );
            }
            return BADCH;
        } else {
            /* white space */
            optarg = *argv.offset(optind as isize);
        }
        place = EMSG;
        optind += 1;
    }
    optopt
}
