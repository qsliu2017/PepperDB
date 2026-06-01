//! regfree - free an RE
//!
//! Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
//!
//! Development of this software was funded, in part, by Cray Research Inc.,
//! UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
//! Corporation, none of whom are responsible for the results.  The author
//! thanks all of them.
//!
//! Translated 1:1 from postgres/src/backend/regex/regfree.c
//!
//! You might think that this could be incorporated into regcomp.c, and
//! that would be a reasonable idea... except that this is a generic
//! function (with a generic name), applicable to all compiled REs
//! regardless of the size of their characters, whereas the stuff in
//! regcomp.c gets compiled once per character size.

use crate::regex::regex::regex_t;
use crate::regex::regguts::fns;

/*
 * pg_regfree - free an RE (generic function, punts to RE-specific function)
 *
 * Ignoring invocation with NULL is a convenience.
 */
pub unsafe fn pg_regfree(re: *mut regex_t) {
    if re.is_null() {
        return;
    }
    // regguts' fns table types its regex_t argument as c_void; cast at the boundary.
    ((*((*re).re_fns as *mut fns)).free.unwrap())(re as *mut core::ffi::c_void);
}
