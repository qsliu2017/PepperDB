//! regex/regcustom.h - application-dependent overrides for the regex library
//!
//! Copyright (c) 1998, 1999 Henry Spencer.  See PostgreSQL source for the full
//! license text. This file supplies the Postgres-specific definitions that the
//! regex library (regguts.h) consumes.

use crate::mb::wchar::pg_wchar;
use std::ffi::{c_int, c_uint};

// The C header pulls in postgres.h, <ctype.h>, <limits.h>, <wctype.h>,
// mb/pg_wchar.h, miscadmin.h, and finally regex.h. In the Rust port those are
// provided by the respective modules and wired by the consuming code.

// ---------------------------------------------------------------------------
// overrides for regguts.h definitions
// ---------------------------------------------------------------------------
//
// C:
//   #define FUNCPTR(name, args) (*name) args
//   #define MALLOC(n)     palloc_extended((n), MCXT_ALLOC_NO_OOM)
//   #define FREE(p)       pfree(VS(p))
//   #define REALLOC(p,n)  repalloc_extended(VS(p),(n), MCXT_ALLOC_NO_OOM)
//   #define INTERRUPT(re) CHECK_FOR_INTERRUPTS()
//   #define assert(x)     Assert(x)
//
// FUNCPTR is a syntactic macro for declaring function pointers; it has no
// standalone Rust equivalent. MALLOC/FREE/REALLOC/INTERRUPT/assert are
// expressed at their use sites in the translated regex sources via
// palloc_extended / pfree / repalloc_extended / CHECK_FOR_INTERRUPTS! / Assert!.

// ---------------------------------------------------------------------------
// internal character type and related
// ---------------------------------------------------------------------------

/// the type itself: typedef pg_wchar chr;
pub type chr = pg_wchar;

/// unsigned type that will hold a chr: typedef unsigned uchr;
pub type uchr = c_uint;

/// turn char literal into chr literal: #define CHR(c) ((unsigned char) (c))
#[inline]
pub fn CHR(c: c_int) -> chr {
    (c as u8) as chr
}

/// turn chr digit into its value: #define DIGITVAL(c) ((c)-'0')
#[inline]
pub fn DIGITVAL(c: chr) -> chr {
    c.wrapping_sub(b'0' as chr)
}

/// bits in a chr; must not use sizeof
pub const CHRBITS: c_int = 32;

/// smallest chr value
pub const CHR_MIN: chr = 0x00000000;

/// largest chr value; CHR_MAX-CHR_MIN+1 must fit in an int, and CHR_MAX+1 must
/// fit in a chr variable
pub const CHR_MAX: chr = 0x7ffffffe;

/// Check if a chr value is in range.
/// #define CHR_IS_IN_RANGE(c) ((c) <= CHR_MAX)
#[inline]
pub fn CHR_IS_IN_RANGE(c: chr) -> bool {
    c <= CHR_MAX
}

/// MAX_SIMPLE_CHR is the cutoff between "simple" and "complicated" processing
/// in the color map logic.  Suitable value for Unicode.
pub const MAX_SIMPLE_CHR: chr = 0x7FF;

// ---------------------------------------------------------------------------
// functions operating on chr
// ---------------------------------------------------------------------------
//
// C:
//   #define iscalnum(x) pg_wc_isalnum(x)
//   #define iscalpha(x) pg_wc_isalpha(x)
//   #define iscdigit(x) pg_wc_isdigit(x)
//   #define iscspace(x) pg_wc_isspace(x)

use crate::regex::regc_pg_locale::{pg_wc_isalnum, pg_wc_isalpha, pg_wc_isdigit, pg_wc_isspace};

/// #define iscalnum(x) pg_wc_isalnum(x)
#[inline]
pub unsafe fn iscalnum(x: chr) -> bool {
    pg_wc_isalnum(x) != 0
}

/// #define iscalpha(x) pg_wc_isalpha(x)
#[inline]
pub unsafe fn iscalpha(x: chr) -> bool {
    pg_wc_isalpha(x) != 0
}

/// #define iscdigit(x) pg_wc_isdigit(x)
#[inline]
pub unsafe fn iscdigit(x: chr) -> bool {
    pg_wc_isdigit(x) != 0
}

/// #define iscspace(x) pg_wc_isspace(x)
#[inline]
pub unsafe fn iscspace(x: chr) -> bool {
    pg_wc_isspace(x) != 0
}
