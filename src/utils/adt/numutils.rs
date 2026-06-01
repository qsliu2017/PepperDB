//! Translation of postgres/src/backend/utils/adt/numutils.c
//!
//! Utility functions for I/O of built-in numeric types.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! These are plain C functions (NOT fmgr-callable): the integer string parsers
//! pg_strtoint16/32/64 (used by int.c/int8.c in/out), uint32in_subr/uint64in_subr
//! (used by oid.c et al.), and the itoa family pg_itoa/pg_ltoa/pg_lltoa/pg_ultoa_n/
//! pg_ulltoa_n/pg_ultostr[_zeropad].
//!
//! `#include`s mapped: common/int.h -> crate::common::int (pg_neg_u*_overflow),
//! port/pg_bitutils.h -> crate::port::pg_bitutils (pg_leftmost_one_pos*),
//! <ctype.h> isspace/isdigit/isxdigit bound via extern "C" (scansup.rs convention),
//! strtoul/strtou64 via libc.  The `_safe` variants take a `*mut Node escontext`
//! ErrorSaveContext; soft errors are NOT yet supported, so every `ereturn` here
//! reports a hard ERROR (TODO(pg-port): soft-error path once ErrorSaveContext lands).

use crate::prelude::*;
use crate::c::{int16, int32, int64, uint16, uint32, uint64};
use crate::c::{PG_INT16_MAX, PG_INT16_MIN, PG_INT32_MAX, PG_INT32_MIN, PG_INT64_MAX, PG_INT64_MIN};
use crate::common::int::{pg_neg_u16_overflow, pg_neg_u32_overflow, pg_neg_u64_overflow};
use crate::port::pg_bitutils::{pg_leftmost_one_pos32, pg_leftmost_one_pos64};
use crate::nodes::nodes::Node;
use core::ffi::{c_char, c_int, c_ulong, c_ulonglong};

// <ctype.h>, used on the slow paths exactly as the C does via `is*((unsigned char) *p)`.
extern "C" {
    fn isspace(ch: c_int) -> c_int;
    fn isdigit(ch: c_int) -> c_int;
    fn isxdigit(ch: c_int) -> c_int;
    // uint32in_subr/uint64in_subr defer to the C library's strtoul/strtoull (base 0).
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn strtoull(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulonglong;
}

// errno access for the strtoul-based subroutines (platform errno location).
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}
const ERANGE: c_int = 34; // <errno.h>, 34 on Linux and macOS

/* errcodes.h classification (the errcode() shim ignores the value). */
// TODO(pg-port): real codes from utils/errcodes.h.
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;

/*
 * A table of all two-digit numbers. This is used to speed up decimal digit
 * generation by copying pairs of digits into the final output.
 */
const DIGIT_TABLE: [u8; 200] = {
    let mut t = [0u8; 200];
    let mut i = 0usize;
    while i < 100 {
        t[i * 2] = b'0' + (i / 10) as u8;
        t[i * 2 + 1] = b'0' + (i % 10) as u8;
        i += 1;
    }
    t
};

/*
 * Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
 */
#[inline]
fn decimalLength32(v: uint32) -> c_int {
    const POWERS_OF_TEN: [uint32; 10] = [
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
    ];
    /*
     * Compute base-10 logarithm by dividing the base-2 logarithm by a
     * good-enough approximation of the base-2 logarithm of 10
     */
    let t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
    t + (v >= POWERS_OF_TEN[t as usize]) as c_int
}

#[inline]
fn decimalLength64(v: uint64) -> c_int {
    const POWERS_OF_TEN: [uint64; 20] = [
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
        10000000000, 100000000000, 1000000000000, 10000000000000, 100000000000000,
        1000000000000000, 10000000000000000, 100000000000000000, 1000000000000000000,
        10000000000000000000,
    ];
    let t = (pg_leftmost_one_pos64(v) + 1) * 1233 / 4096;
    t + (v >= POWERS_OF_TEN[t as usize]) as c_int
}

const HEXLOOKUP: [i8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
];

// --- error reporters (the `ereturn(escontext, 0, ...)` targets) ---
// Soft errors are not yet supported, so these always raise a hard ERROR.  Each
// returns () (ereport! diverges at runtime under the elog shim); callers add the
// `(Datum) 0`-equivalent return value to satisfy the type, mirroring ereturn's 0.
unsafe fn report_out_of_range(s: *const c_char, typname: &str) {
    let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    ereport!(
        ERROR,
        errmsg!("value \"{}\" is out of range for type {}", cstr(s), typname)
    );
}
unsafe fn report_invalid_syntax(s: *const c_char, typname: &str) {
    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereport!(
        ERROR,
        errmsg!("invalid input syntax for type {}: \"{}\"", typname, cstr(s))
    );
}

/*
 * Convert input string to a signed 16 bit integer.  See the C source for the
 * accepted syntax (base-10/hex/octal/binary, optional sign, underscores).
 *
 * pg_strtoint16() throws on bad input/overflow; pg_strtoint16_safe() reports via
 * *escontext if it's an ErrorSaveContext (TODO: soft errors; currently throws).
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
pub unsafe fn pg_strtoint16(s: *const c_char) -> int16 {
    pg_strtoint16_safe(s, null_mut())
}

pub unsafe fn pg_strtoint16_safe(s: *const c_char, escontext: *mut Node) -> int16 {
    let mut ptr = s;
    let firstdigit: *const c_char;
    let mut tmp: uint16 = 0;
    let mut neg = false;
    let mut digit: u8;
    let mut result: int16 = 0;
    let _ = escontext; // TODO(pg-port): ErrorSaveContext soft errors

    'slow: {
        // ---- fast path: base-10, no underscores ----
        if *ptr as u8 == b'-' {
            ptr = ptr.add(1);
            neg = true;
        }
        digit = ((*ptr as i32) - (b'0' as i32)) as u8;
        if digit < 10 {
            ptr = ptr.add(1);
            tmp = digit as uint16;
        } else {
            break 'slow; // need at least one digit -> slow
        }
        loop {
            digit = ((*ptr as i32) - (b'0' as i32)) as u8;
            if digit >= 10 {
                break;
            }
            ptr = ptr.add(1);
            if tmp > (-(PG_INT16_MIN as i32 / 10)) as uint16 {
                report_out_of_range(s, "smallint");
                return 0;
            }
            tmp = tmp * 10 + digit as uint16;
        }
        if *ptr as u8 != b'\0' {
            break 'slow; // doesn't end in a digit -> slow
        }
        if neg {
            if pg_neg_u16_overflow(tmp, &mut result) {
                report_out_of_range(s, "smallint");
                return 0;
            }
            return result;
        }
        if tmp > PG_INT16_MAX as uint16 {
            report_out_of_range(s, "smallint");
            return 0;
        }
        return tmp as int16;
    }

    // ---- slow path ----
    tmp = 0;
    ptr = s;
    /* no need to reset neg */

    while isspace(*ptr as u8 as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr as u8 == b'-' {
        ptr = ptr.add(1);
        neg = true;
    } else if *ptr as u8 == b'+' {
        ptr = ptr.add(1);
    }

    if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'x' || *ptr.add(1) as u8 == b'X') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if isxdigit(*ptr as u8 as c_int) != 0 {
                if tmp > (-(PG_INT16_MIN as i32 / 16)) as uint16 {
                    report_out_of_range(s, "smallint");
                    return 0;
                }
                tmp = tmp * 16 + HEXLOOKUP[*ptr as u8 as usize] as uint16;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || isxdigit(*ptr as u8 as c_int) == 0 {
                    report_invalid_syntax(s, "smallint");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'o' || *ptr.add(1) as u8 == b'O') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'7' {
                if tmp > (-(PG_INT16_MIN as i32 / 8)) as uint16 {
                    report_out_of_range(s, "smallint");
                    return 0;
                }
                tmp = tmp * 8 + (*ptr as u8 - b'0') as uint16;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || (*ptr as u8) < b'0' || (*ptr as u8) > b'7' {
                    report_invalid_syntax(s, "smallint");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'b' || *ptr.add(1) as u8 == b'B') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'1' {
                if tmp > (-(PG_INT16_MIN as i32 / 2)) as uint16 {
                    report_out_of_range(s, "smallint");
                    return 0;
                }
                tmp = tmp * 2 + (*ptr as u8 - b'0') as uint16;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || (*ptr as u8) < b'0' || (*ptr as u8) > b'1' {
                    report_invalid_syntax(s, "smallint");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else {
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'9' {
                if tmp > (-(PG_INT16_MIN as i32 / 10)) as uint16 {
                    report_out_of_range(s, "smallint");
                    return 0;
                }
                tmp = tmp * 10 + (*ptr as u8 - b'0') as uint16;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                if ptr == firstdigit {
                    report_invalid_syntax(s, "smallint");
                    return 0;
                }
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || isdigit(*ptr as u8 as c_int) == 0 {
                    report_invalid_syntax(s, "smallint");
                    return 0;
                }
            } else {
                break;
            }
        }
    }

    if ptr == firstdigit {
        report_invalid_syntax(s, "smallint");
        return 0;
    }
    while isspace(*ptr as u8 as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr as u8 != b'\0' {
        report_invalid_syntax(s, "smallint");
        return 0;
    }
    if neg {
        if pg_neg_u16_overflow(tmp, &mut result) {
            report_out_of_range(s, "smallint");
            return 0;
        }
        return result;
    }
    if tmp > PG_INT16_MAX as uint16 {
        report_out_of_range(s, "smallint");
        return 0;
    }
    tmp as int16
}

/*
 * Convert input string to a signed 32 bit integer.  (Structure mirrors the
 * 16-bit version; see its comments.)
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
pub unsafe fn pg_strtoint32(s: *const c_char) -> int32 {
    pg_strtoint32_safe(s, null_mut())
}

pub unsafe fn pg_strtoint32_safe(s: *const c_char, escontext: *mut Node) -> int32 {
    let mut ptr = s;
    let firstdigit: *const c_char;
    let mut tmp: uint32 = 0;
    let mut neg = false;
    let mut digit: u8;
    let mut result: int32 = 0;
    let _ = escontext;

    'slow: {
        if *ptr as u8 == b'-' {
            ptr = ptr.add(1);
            neg = true;
        }
        digit = ((*ptr as i32) - (b'0' as i32)) as u8;
        if digit < 10 {
            ptr = ptr.add(1);
            tmp = digit as uint32;
        } else {
            break 'slow;
        }
        loop {
            digit = ((*ptr as i32) - (b'0' as i32)) as u8;
            if digit >= 10 {
                break;
            }
            ptr = ptr.add(1);
            if tmp > (-(PG_INT32_MIN as i64 / 10)) as uint32 {
                report_out_of_range(s, "integer");
                return 0;
            }
            tmp = tmp * 10 + digit as uint32;
        }
        if *ptr as u8 != b'\0' {
            break 'slow;
        }
        if neg {
            if pg_neg_u32_overflow(tmp, &mut result) {
                report_out_of_range(s, "integer");
                return 0;
            }
            return result;
        }
        if tmp > PG_INT32_MAX as uint32 {
            report_out_of_range(s, "integer");
            return 0;
        }
        return tmp as int32;
    }

    tmp = 0;
    ptr = s;
    while isspace(*ptr as u8 as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr as u8 == b'-' {
        ptr = ptr.add(1);
        neg = true;
    } else if *ptr as u8 == b'+' {
        ptr = ptr.add(1);
    }

    if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'x' || *ptr.add(1) as u8 == b'X') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if isxdigit(*ptr as u8 as c_int) != 0 {
                if tmp > (-(PG_INT32_MIN as i64 / 16)) as uint32 {
                    report_out_of_range(s, "integer");
                    return 0;
                }
                tmp = tmp * 16 + HEXLOOKUP[*ptr as u8 as usize] as uint32;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || isxdigit(*ptr as u8 as c_int) == 0 {
                    report_invalid_syntax(s, "integer");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'o' || *ptr.add(1) as u8 == b'O') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'7' {
                if tmp > (-(PG_INT32_MIN as i64 / 8)) as uint32 {
                    report_out_of_range(s, "integer");
                    return 0;
                }
                tmp = tmp * 8 + (*ptr as u8 - b'0') as uint32;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || (*ptr as u8) < b'0' || (*ptr as u8) > b'7' {
                    report_invalid_syntax(s, "integer");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'b' || *ptr.add(1) as u8 == b'B') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'1' {
                if tmp > (-(PG_INT32_MIN as i64 / 2)) as uint32 {
                    report_out_of_range(s, "integer");
                    return 0;
                }
                tmp = tmp * 2 + (*ptr as u8 - b'0') as uint32;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || (*ptr as u8) < b'0' || (*ptr as u8) > b'1' {
                    report_invalid_syntax(s, "integer");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else {
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'9' {
                if tmp > (-(PG_INT32_MIN as i64 / 10)) as uint32 {
                    report_out_of_range(s, "integer");
                    return 0;
                }
                tmp = tmp * 10 + (*ptr as u8 - b'0') as uint32;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                if ptr == firstdigit {
                    report_invalid_syntax(s, "integer");
                    return 0;
                }
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || isdigit(*ptr as u8 as c_int) == 0 {
                    report_invalid_syntax(s, "integer");
                    return 0;
                }
            } else {
                break;
            }
        }
    }

    if ptr == firstdigit {
        report_invalid_syntax(s, "integer");
        return 0;
    }
    while isspace(*ptr as u8 as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr as u8 != b'\0' {
        report_invalid_syntax(s, "integer");
        return 0;
    }
    if neg {
        if pg_neg_u32_overflow(tmp, &mut result) {
            report_out_of_range(s, "integer");
            return 0;
        }
        return result;
    }
    if tmp > PG_INT32_MAX as uint32 {
        report_out_of_range(s, "integer");
        return 0;
    }
    tmp as int32
}

/*
 * Convert input string to a signed 64 bit integer.  (Structure mirrors the
 * 16-/32-bit versions.)
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
pub unsafe fn pg_strtoint64(s: *const c_char) -> int64 {
    pg_strtoint64_safe(s, null_mut())
}

pub unsafe fn pg_strtoint64_safe(s: *const c_char, escontext: *mut Node) -> int64 {
    let mut ptr = s;
    let firstdigit: *const c_char;
    let mut tmp: uint64 = 0;
    let mut neg = false;
    let mut digit: u8;
    let mut result: int64 = 0;
    let _ = escontext;

    'slow: {
        if *ptr as u8 == b'-' {
            ptr = ptr.add(1);
            neg = true;
        }
        digit = ((*ptr as i32) - (b'0' as i32)) as u8;
        if digit < 10 {
            ptr = ptr.add(1);
            tmp = digit as uint64;
        } else {
            break 'slow;
        }
        loop {
            digit = ((*ptr as i32) - (b'0' as i32)) as u8;
            if digit >= 10 {
                break;
            }
            ptr = ptr.add(1);
            if tmp > (-(PG_INT64_MIN / 10)) as uint64 {
                report_out_of_range(s, "bigint");
                return 0;
            }
            tmp = tmp * 10 + digit as uint64;
        }
        if *ptr as u8 != b'\0' {
            break 'slow;
        }
        if neg {
            if pg_neg_u64_overflow(tmp, &mut result) {
                report_out_of_range(s, "bigint");
                return 0;
            }
            return result;
        }
        if tmp > PG_INT64_MAX as uint64 {
            report_out_of_range(s, "bigint");
            return 0;
        }
        return tmp as int64;
    }

    tmp = 0;
    ptr = s;
    while isspace(*ptr as u8 as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr as u8 == b'-' {
        ptr = ptr.add(1);
        neg = true;
    } else if *ptr as u8 == b'+' {
        ptr = ptr.add(1);
    }

    if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'x' || *ptr.add(1) as u8 == b'X') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if isxdigit(*ptr as u8 as c_int) != 0 {
                if tmp > (-(PG_INT64_MIN / 16)) as uint64 {
                    report_out_of_range(s, "bigint");
                    return 0;
                }
                tmp = tmp * 16 + HEXLOOKUP[*ptr as u8 as usize] as uint64;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || isxdigit(*ptr as u8 as c_int) == 0 {
                    report_invalid_syntax(s, "bigint");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'o' || *ptr.add(1) as u8 == b'O') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'7' {
                if tmp > (-(PG_INT64_MIN / 8)) as uint64 {
                    report_out_of_range(s, "bigint");
                    return 0;
                }
                tmp = tmp * 8 + (*ptr as u8 - b'0') as uint64;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || (*ptr as u8) < b'0' || (*ptr as u8) > b'7' {
                    report_invalid_syntax(s, "bigint");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else if *ptr as u8 == b'0' && (*ptr.add(1) as u8 == b'b' || *ptr.add(1) as u8 == b'B') {
        ptr = ptr.add(2);
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'1' {
                if tmp > (-(PG_INT64_MIN / 2)) as uint64 {
                    report_out_of_range(s, "bigint");
                    return 0;
                }
                tmp = tmp * 2 + (*ptr as u8 - b'0') as uint64;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || (*ptr as u8) < b'0' || (*ptr as u8) > b'1' {
                    report_invalid_syntax(s, "bigint");
                    return 0;
                }
            } else {
                break;
            }
        }
    } else {
        firstdigit = ptr;
        loop {
            if (*ptr as u8) >= b'0' && (*ptr as u8) <= b'9' {
                if tmp > (-(PG_INT64_MIN / 10)) as uint64 {
                    report_out_of_range(s, "bigint");
                    return 0;
                }
                tmp = tmp * 10 + (*ptr as u8 - b'0') as uint64;
                ptr = ptr.add(1);
            } else if *ptr as u8 == b'_' {
                if ptr == firstdigit {
                    report_invalid_syntax(s, "bigint");
                    return 0;
                }
                ptr = ptr.add(1);
                if *ptr as u8 == b'\0' || isdigit(*ptr as u8 as c_int) == 0 {
                    report_invalid_syntax(s, "bigint");
                    return 0;
                }
            } else {
                break;
            }
        }
    }

    if ptr == firstdigit {
        report_invalid_syntax(s, "bigint");
        return 0;
    }
    while isspace(*ptr as u8 as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr as u8 != b'\0' {
        report_invalid_syntax(s, "bigint");
        return 0;
    }
    if neg {
        if pg_neg_u64_overflow(tmp, &mut result) {
            report_out_of_range(s, "bigint");
            return 0;
        }
        return result;
    }
    if tmp > PG_INT64_MAX as uint64 {
        report_out_of_range(s, "bigint");
        return 0;
    }
    tmp as int64
}

/*
 * Convert input string to an unsigned 32 bit integer.  Defers to the C library's
 * strtoul(base 0) so 0x/0/decimal prefixes are honored.
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string; `endloc`, if non-null, receives the
 * pointer past the parsed number.
 */
pub unsafe fn uint32in_subr(
    s: *const c_char,
    endloc: *mut *mut c_char,
    typname: *const c_char,
    escontext: *mut Node,
) -> uint32 {
    let result: uint32;
    let cvt: c_ulong;
    let mut endptr: *mut c_char = null_mut();
    let _ = escontext;

    *errno_location() = 0;
    cvt = strtoul(s, &mut endptr, 0);

    if (*errno_location() != 0 && *errno_location() != ERANGE) || endptr == s as *mut c_char {
        report_invalid_syntax(s, &cstr(typname));
        return 0;
    }
    if *errno_location() == ERANGE {
        report_out_of_range(s, &cstr(typname));
        return 0;
    }

    if !endloc.is_null() {
        *endloc = endptr;
    } else {
        let mut e = endptr;
        while *e != 0 && isspace(*e as u8 as c_int) != 0 {
            e = e.add(1);
        }
        if *e != 0 {
            report_invalid_syntax(s, &cstr(typname));
            return 0;
        }
    }

    result = cvt as uint32;

    /*
     * Cope with possibility that unsigned long is wider than uint32 (true on
     * LP64).  Accept inputs that match after signed or unsigned extension.
     */
    if cvt != result as c_ulong && cvt != (result as c_int) as c_ulong {
        report_out_of_range(s, &cstr(typname));
        return 0;
    }

    result
}

/*
 * Convert input string to an unsigned 64 bit integer.
 *
 * # Safety
 * As uint32in_subr.
 */
pub unsafe fn uint64in_subr(
    s: *const c_char,
    endloc: *mut *mut c_char,
    typname: *const c_char,
    escontext: *mut Node,
) -> uint64 {
    let result: uint64;
    let mut endptr: *mut c_char = null_mut();
    let _ = escontext;

    *errno_location() = 0;
    result = strtoull(s, &mut endptr, 0) as uint64;

    if (*errno_location() != 0 && *errno_location() != ERANGE) || endptr == s as *mut c_char {
        report_invalid_syntax(s, &cstr(typname));
        return 0;
    }
    if *errno_location() == ERANGE {
        report_out_of_range(s, &cstr(typname));
        return 0;
    }

    if !endloc.is_null() {
        *endloc = endptr;
    } else {
        let mut e = endptr;
        while *e != 0 && isspace(*e as u8 as c_int) != 0 {
            e = e.add(1);
        }
        if *e != 0 {
            report_invalid_syntax(s, &cstr(typname));
            return 0;
        }
    }

    result
}

/*
 * pg_itoa: converts a signed 16-bit integer to its string representation and
 * returns strlen(a).  Caller must ensure 'a' has at least 7 bytes.
 *
 * # Safety
 * `a` must point to a writable buffer of at least 7 bytes.
 */
pub unsafe fn pg_itoa(i: int16, a: *mut c_char) -> c_int {
    pg_ltoa(i as int32, a)
}

// memcpy(dst, DIGIT_TABLE + off, 2)
#[inline]
unsafe fn put2(dst: *mut c_char, off: uint32) {
    core::ptr::copy_nonoverlapping(DIGIT_TABLE.as_ptr().add(off as usize), dst as *mut u8, 2);
}

/*
 * pg_ultoa_n: converts an unsigned 32-bit integer to its (non-NUL-terminated)
 * string representation, returning the length.  Caller must ensure >= 10 bytes.
 *
 * # Safety
 * `a` must point to a writable buffer of at least 10 bytes.
 */
pub unsafe fn pg_ultoa_n(mut value: uint32, a: *mut c_char) -> c_int {
    let olength: c_int;
    let mut i: c_int = 0;

    if value == 0 {
        *a = b'0' as c_char;
        return 1;
    }

    olength = decimalLength32(value);

    while value >= 10000 {
        let c = value - 10000 * (value / 10000);
        let c0 = (c % 100) << 1;
        let c1 = (c / 100) << 1;
        let pos = a.offset((olength - i) as isize);
        value /= 10000;
        put2(pos.sub(2), c0);
        put2(pos.sub(4), c1);
        i += 4;
    }
    if value >= 100 {
        let c = (value % 100) << 1;
        let pos = a.offset((olength - i) as isize);
        value /= 100;
        put2(pos.sub(2), c);
        i += 2;
    }
    if value >= 10 {
        let c = value << 1;
        let pos = a.offset((olength - i) as isize);
        put2(pos.sub(2), c);
    } else {
        *a = (b'0' as u32 + value) as u8 as c_char;
    }

    olength
}

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation and
 * returns strlen(a).  Caller must ensure 'a' has at least 12 bytes.
 *
 * # Safety
 * `a` must point to a writable buffer of at least 12 bytes.
 */
pub unsafe fn pg_ltoa(value: int32, a: *mut c_char) -> c_int {
    let mut uvalue = value as uint32;
    let mut len: c_int = 0;

    if value < 0 {
        uvalue = (0u32).wrapping_sub(uvalue);
        *a.offset(len as isize) = b'-' as c_char;
        len += 1;
    }
    len += pg_ultoa_n(uvalue, a.offset(len as isize));
    *a.offset(len as isize) = b'\0' as c_char;
    len
}

/*
 * pg_ulltoa_n: decimal representation of a uint64, non-NUL-terminated, returns
 * the length.  Caller must ensure 'a' has at least MAXINT8LEN (20) bytes.
 *
 * # Safety
 * `a` must point to a writable buffer of at least 20 bytes.
 */
pub unsafe fn pg_ulltoa_n(mut value: uint64, a: *mut c_char) -> c_int {
    let olength: c_int;
    let mut i: c_int = 0;
    let mut value2: uint32;

    if value == 0 {
        *a = b'0' as c_char;
        return 1;
    }

    olength = decimalLength64(value);

    while value >= 100000000 {
        let q = value / 100000000;
        let value3 = (value - 100000000 * q) as uint32;

        let c = value3 % 10000;
        let d = value3 / 10000;
        let c0 = (c % 100) << 1;
        let c1 = (c / 100) << 1;
        let d0 = (d % 100) << 1;
        let d1 = (d / 100) << 1;

        let pos = a.offset((olength - i) as isize);
        value = q;
        put2(pos.sub(2), c0);
        put2(pos.sub(4), c1);
        put2(pos.sub(6), d0);
        put2(pos.sub(8), d1);
        i += 8;
    }

    value2 = value as uint32;

    if value2 >= 10000 {
        let c = value2 - 10000 * (value2 / 10000);
        let c0 = (c % 100) << 1;
        let c1 = (c / 100) << 1;
        let pos = a.offset((olength - i) as isize);
        value2 /= 10000;
        put2(pos.sub(2), c0);
        put2(pos.sub(4), c1);
        i += 4;
    }
    if value2 >= 100 {
        let c = (value2 % 100) << 1;
        let pos = a.offset((olength - i) as isize);
        value2 /= 100;
        put2(pos.sub(2), c);
        i += 2;
    }
    if value2 >= 10 {
        let c = value2 << 1;
        let pos = a.offset((olength - i) as isize);
        put2(pos.sub(2), c);
    } else {
        *a = (b'0' as u32 + value2) as u8 as c_char;
    }

    olength
}

/*
 * pg_lltoa: converts a signed 64-bit integer to its string representation and
 * returns strlen(a).  Caller must ensure 'a' has at least MAXINT8LEN + 1 bytes.
 *
 * # Safety
 * `a` must point to a writable buffer of at least 21 bytes.
 */
pub unsafe fn pg_lltoa(value: int64, a: *mut c_char) -> c_int {
    let mut uvalue = value as uint64;
    let mut len: c_int = 0;

    if value < 0 {
        uvalue = (0u64).wrapping_sub(uvalue);
        *a.offset(len as isize) = b'-' as c_char;
        len += 1;
    }
    len += pg_ulltoa_n(uvalue, a.offset(len as isize));
    *a.offset(len as isize) = b'\0' as c_char;
    len
}

/*
 * pg_ultostr_zeropad: decimal of 'value' at 'str', zero-padded to 'minwidth'.
 * Returns the address past the last char written (no NUL written).
 *
 * # Safety
 * `str` must point to enough memory to hold the result.
 */
pub unsafe fn pg_ultostr_zeropad(str: *mut c_char, value: uint32, minwidth: int32) -> *mut c_char {
    let len: c_int;

    Assert!(minwidth > 0);

    if value < 100 && minwidth == 2 {
        put2(str, value * 2);
        return str.offset(2);
    }

    len = pg_ultoa_n(value, str);
    if len >= minwidth {
        return str.offset(len as isize);
    }

    // memmove(str + minwidth - len, str, len)
    core::ptr::copy(str, str.offset((minwidth - len) as isize), len as usize);
    // memset(str, '0', minwidth - len)
    core::ptr::write_bytes(str, b'0', (minwidth - len) as usize);
    str.offset(minwidth as isize)
}

/*
 * pg_ultostr: decimal of 'value' at 'str'.  Returns the address past the last
 * char written (no NUL written).
 *
 * # Safety
 * `str` must point to enough memory to hold the result.
 */
pub unsafe fn pg_ultostr(str: *mut c_char, value: uint32) -> *mut c_char {
    let len = pg_ultoa_n(value, str);
    str.offset(len as isize)
}

/*
 * Format a C string for an error message via Rust `{}` (lossy), matching the C
 * `%s` conversion.
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strtoint_parsers() {
        unsafe {
            // base-10, fast path
            assert_eq!(pg_strtoint16(c"123".as_ptr()), 123);
            assert_eq!(pg_strtoint16(c"-32768".as_ptr()), -32768); // INT16_MIN edge
            assert_eq!(pg_strtoint32(c"-2147483648".as_ptr()), i32::MIN);
            assert_eq!(pg_strtoint32(c"2147483647".as_ptr()), i32::MAX);
            assert_eq!(pg_strtoint64(c"9223372036854775807".as_ptr()), i64::MAX);
            assert_eq!(pg_strtoint64(c"-9223372036854775808".as_ptr()), i64::MIN);
            // slow path: leading/trailing spaces, '+', underscores, alt bases
            assert_eq!(pg_strtoint32(c"  +1_000  ".as_ptr()), 1000);
            assert_eq!(pg_strtoint32(c"0xFF".as_ptr()), 255);
            assert_eq!(pg_strtoint32(c"0o17".as_ptr()), 15);
            assert_eq!(pg_strtoint32(c"0b1010".as_ptr()), 10);
            assert_eq!(pg_strtoint16(c"-0x10".as_ptr()), -16);
            // uint subrs (strtoul base 0)
            assert_eq!(uint32in_subr(c"4294967295".as_ptr(), null_mut(), c"oid".as_ptr(), null_mut()), u32::MAX);
            assert_eq!(uint64in_subr(c"0x10".as_ptr(), null_mut(), c"xid8".as_ptr(), null_mut()), 16);
        }
    }

    #[test]
    fn itoa_family() {
        unsafe {
            let mut buf = [0i8; 32];
            let to_str = |b: &[i8], n: i32| {
                let bytes: std::vec::Vec<u8> = b[..n as usize].iter().map(|&c| c as u8).collect();
                std::string::String::from_utf8(bytes).unwrap()
            };

            let n = pg_ltoa(0, buf.as_mut_ptr());
            assert_eq!(to_str(&buf, n), "0");
            let n = pg_ltoa(-2147483648, buf.as_mut_ptr());
            assert_eq!(to_str(&buf, n), "-2147483648");
            let n = pg_ltoa(1234567, buf.as_mut_ptr());
            assert_eq!(to_str(&buf, n), "1234567");
            let n = pg_itoa(-32768, buf.as_mut_ptr());
            assert_eq!(to_str(&buf, n), "-32768");
            let n = pg_lltoa(9223372036854775807, buf.as_mut_ptr());
            assert_eq!(to_str(&buf, n), "9223372036854775807");
            let n = pg_lltoa(-9223372036854775808, buf.as_mut_ptr());
            assert_eq!(to_str(&buf, n), "-9223372036854775808");

            // zeropad
            let end = pg_ultostr_zeropad(buf.as_mut_ptr(), 7, 3);
            let len = end.offset_from(buf.as_mut_ptr()) as i32;
            assert_eq!(to_str(&buf, len), "007");
        }
    }
}
