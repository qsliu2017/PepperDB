//! tsearch/ts_locale.rs - locale compatibility layer for tsearch.
//!
//! Source: postgres/src/backend/tsearch/ts_locale.c
//! Merged header: postgres/src/include/tsearch/ts_locale.h
//!
//! #include mapping:
//!   - "postgres.h"          -> crate::prelude::* (Datum, c-types, palloc, elog!, ...)
//!   - "common/string.h"     -> not needed here (pg_str_endswith etc. unused)
//!   - "storage/fd.h"        -> STUB: the VFD/FILE* layer (AllocateFile / FreeFile /
//!                              pg_get_line_buf) is not ported yet.
//!   - "tsearch/ts_locale.h" -> merged below (TOUCHAR/t_iseq macros as fns,
//!                              tsearch_readline_state struct, t_is* decls).
//!   - "lib/stringinfo.h"    -> StringInfoData (used only by the stubbed readline
//!                              state); referenced via crate::lib::stringinfo if/when
//!                              the readline layer is ported.
//!   - "mb/pg_wchar.h"       -> crate::mb::mbutils (pg_mblen_with_len / pg_mblen_cstr /
//!                              pg_mblen_unbounded).
//!   - "utils/pg_locale.h"   -> STUB: pg_locale_t / char2wchar / database_ctype_is_c are
//!                              not ported.  See REAL-vs-STUB note below.
//!   - <ctype.h>/<wctype.h>  -> libc isalpha/isalnum/isdigit/isspace/isprint +
//!                              iswalpha/... bound via `extern "C"`.
//!
//! REAL vs STUB:
//!   The upstream C uses char2wchar() + the isw*() wide-char family for the
//!   multibyte path, gated on `database_ctype_is_c`.  Neither char2wchar nor
//!   database_ctype_is_c is ported (utils/pg_locale.h is absent), so the
//!   single-byte / database-C path is done REAL here via libc is*(), and the
//!   multibyte (clen > 1) branch is STUBBED with a TODO that falls back to the
//!   libc isw*() family applied to the first byte.  This matches PG behaviour
//!   exactly for ASCII and single-byte encodings, and for the database-C ctype.
//!
//!   The file-reading helpers (tsearch_readline_begin / tsearch_readline /
//!   tsearch_readline_end and the tsearch_readline_state struct) need the
//!   fd/VFD layer (AllocateFile/FreeFile/pg_get_line_buf) and the error-context
//!   stack; both are unported, so those are STUBBED (unimplemented!() + TODO).
//!
//!   lowerstr / lowerstr_with_len are NOT present in this PG 18 ts_locale.c
//!   (they were removed upstream long ago; lowercasing now goes through
//!   str_tolower in formatting.c).  The task asked for them as a fallback, so
//!   they are provided REAL here as an ASCII tolower (TODO: route through
//!   str_tolower once pg_locale is ported), since downstream tsearch code
//!   historically depended on a `lowerstr` in this module.

use crate::prelude::*;

use crate::mb::mbutils::{pg_mblen_cstr, pg_mblen_unbounded, pg_mblen_with_len};
use crate::lib::stringinfo::{initStringInfo, StringInfoData};
use crate::common::pg_get_line::{pg_get_line_buf, FILE};
use crate::utils::mb::mbutils::pg_any_to_server;
use crate::mb::pg_wchar::PG_UTF8;
use crate::utils::error::elog_impl::{error_context_stack, ErrorContextCallback};

/// errcontext() shim (elog.h).  Real expansion pushes a message onto the active
/// error report; mirrors the placeholder used elsewhere in the port (params.rs).
/// TODO(pg-port): route through the real elog errcontext machinery.
macro_rules! errcontext {
    ($fmt:expr $(, $arg:expr)* $(,)?) => {{ let _ = ($fmt $(, $arg)*); }};
}

// AllocateFile/FreeFile (storage/fd.h) have no canonical home yet; the port
// keeps per-file stubs (see access/transam/timeline.rs).  Mirror that here.
/// TODO(pg-port): real AllocateFile from storage/fd.c (VFD-tracked fopen).
unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut FILE {
    core::ptr::null_mut()
}
/// TODO(pg-port): real FreeFile from storage/fd.c.
unsafe fn FreeFile(_file: *mut FILE) -> c_int {
    0
}

// ----------------------------------------------------------------------------
// libc ctype / wctype bindings (<ctype.h>, <wctype.h>).
// ----------------------------------------------------------------------------

/// C `wint_t`.  On Linux and macOS this is `int`-compatible; we bind it as
/// c_int (matching how the libc isw*() prototypes take it after default
/// promotion).  TODO(pg-port): centralize in crate::c if other units need it.
#[allow(non_camel_case_types)]
pub type wint_t = c_int;

extern "C" {
    fn isalnum(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn isprint(c: c_int) -> c_int;

    fn iswalnum(wc: wint_t) -> c_int;
    fn iswalpha(wc: wint_t) -> c_int;
    fn iswdigit(wc: wint_t) -> c_int;
    fn iswspace(wc: wint_t) -> c_int;
    fn iswprint(wc: wint_t) -> c_int;

    fn tolower(c: c_int) -> c_int;
}

// ----------------------------------------------------------------------------
// Merged ts_locale.h helpers.
// ----------------------------------------------------------------------------

/// `#define TOUCHAR(x) (*((const unsigned char *) (x)))`
///
/// Dereference `ptr` as an unsigned char and widen to c_int (the argument type
/// the libc is*() functions expect).
#[inline]
pub unsafe fn TOUCHAR(ptr: *const c_char) -> c_int {
    *(ptr as *const c_uchar) as c_int
}

/// `#define t_iseq(x,c) (TOUCHAR(x) == (unsigned char) (c))`
///
/// `c` must be a plain ASCII character.
#[inline]
pub unsafe fn t_iseq(x: *const c_char, c: c_char) -> bool {
    TOUCHAR(x) == (c as c_uchar) as c_int
}

/// `ts_copychar_with_len`: copy a multibyte char of known byte length, return length.
#[inline]
pub unsafe fn ts_copychar_with_len(dest: *mut c_void, src: *const c_void, length: c_int) -> c_int {
    core::ptr::copy_nonoverlapping(src as *const u8, dest as *mut u8, length as usize);
    length
}

/// `ts_copychar_cstr`: copy a multibyte char from a NUL-terminated string, return length.
#[inline]
pub unsafe fn ts_copychar_cstr(dest: *mut c_void, src: *const c_void) -> c_int {
    ts_copychar_with_len(dest, src, pg_mblen_cstr(src as *const c_char))
}

/// Historical macro `#define COPYCHAR ts_copychar_cstr`.
#[inline]
pub unsafe fn COPYCHAR(dest: *mut c_void, src: *const c_void) -> c_int {
    ts_copychar_cstr(dest, src)
}

// ----------------------------------------------------------------------------
// t_is<class> family.
//
// Upstream macro GENERATE_T_ISCLASS_DEF expands to four functions per class:
//   t_is<class>_with_len(ptr, mblen)  -- core; mblen is the length of the buffer
//   t_is<class>_cstr(ptr)             -- NUL-terminated string
//   t_is<class>_unbounded(ptr)        -- pre-validated encoding
//   t_is<class>(ptr)                  -- historical alias for _unbounded
//
// We reproduce that expansion with a Rust macro_rules!.
// ----------------------------------------------------------------------------

// All four function names per class are passed explicitly (stable Rust has no
// identifier concatenation in declaration position without a proc-macro, and we
// avoid adding the `paste` dependency per project rules).
macro_rules! generate_t_isclass {
    (
        $with_len:ident, $cstr:ident, $unbounded:ident, $alias:ident,
        $libc_byte:ident, $libc_wide:ident
    ) => {
        /// Classify the first character of `ptr`; `mblen` bounds the buffer.
        pub unsafe fn $with_len(ptr: *const c_char, mblen: c_int) -> c_int {
            let clen = pg_mblen_with_len(ptr, mblen);
            // REAL: single-byte / ASCII path.  `database_ctype_is_c` is unported,
            // so we take only the `clen == 1` branch as the fast/real path.
            if clen == 1 {
                return (($libc_byte)(TOUCHAR(ptr)) != 0) as c_int;
            }
            // STUB(pg-port): multibyte path.  Upstream calls
            //   char2wchar(character, WC_BUF_LEN, ptr, clen, mylocale)
            // and then isw<class>((wint_t) character[0]).  char2wchar and the
            // pg_locale_t machinery (utils/pg_locale.h) are not ported, so we
            // approximate by passing the leading byte to the libc isw<class>()
            // function.  This is only correct for code points <= 0xFF; full
            // multibyte support needs char2wchar.
            // TODO(pg-port): replace with char2wchar + real pg_locale_t.
            (($libc_wide)(TOUCHAR(ptr) as wint_t) != 0) as c_int
        }

        /// Classify the first character of a NUL-terminated string.
        pub unsafe fn $cstr(ptr: *const c_char) -> c_int {
            $with_len(ptr, pg_mblen_cstr(ptr))
        }

        /// Classify the first character of a pre-validated (encoding-checked) string.
        pub unsafe fn $unbounded(ptr: *const c_char) -> c_int {
            $with_len(ptr, pg_mblen_unbounded(ptr))
        }

        /// Historical alias for the `_unbounded` variant.
        pub unsafe fn $alias(ptr: *const c_char) -> c_int {
            $unbounded(ptr)
        }
    };
}

// Upstream defines only alnum and alpha via the macro.  The task also asks for
// digit/space/print as REAL helpers; PG exposes those through the same shape,
// so we generate all five from the one macro for a faithful, uniform surface.
generate_t_isclass!(
    t_isalnum_with_len, t_isalnum_cstr, t_isalnum_unbounded, t_isalnum,
    isalnum, iswalnum
);
generate_t_isclass!(
    t_isalpha_with_len, t_isalpha_cstr, t_isalpha_unbounded, t_isalpha,
    isalpha, iswalpha
);
generate_t_isclass!(
    t_isdigit_with_len, t_isdigit_cstr, t_isdigit_unbounded, t_isdigit,
    isdigit, iswdigit
);
generate_t_isclass!(
    t_isspace_with_len, t_isspace_cstr, t_isspace_unbounded, t_isspace,
    isspace, iswspace
);
generate_t_isclass!(
    t_isprint_with_len, t_isprint_cstr, t_isprint_unbounded, t_isprint,
    isprint, iswprint
);

// ----------------------------------------------------------------------------
// lowerstr / lowerstr_with_len.
//
// Not present in PG 18's ts_locale.c, but requested by the port task as an
// ASCII-tolower fallback for downstream tsearch code.  REAL ASCII lowercasing;
// returns a freshly palloc'd, NUL-terminated string.
// TODO(pg-port): route through str_tolower (utils/adt/formatting.c) once the
// pg_locale collation machinery is ported, for correct locale-aware folding.
// ----------------------------------------------------------------------------

/// Lowercase the first `len` bytes of `str`, ASCII-only, returning a palloc'd
/// NUL-terminated copy.  Mirrors the historical lowerstr_with_len signature.
pub unsafe fn lowerstr_with_len(str: *const c_char, len: c_int) -> *mut c_char {
    if len == 0 {
        // palloc(1) and write the terminator, matching "return pstrdup("")".
        let out = palloc(1) as *mut c_char;
        *out = 0;
        return out;
    }
    let n = len as usize;
    let out = palloc(n + 1) as *mut c_char;
    for i in 0..n {
        let b = *str.add(i) as c_uchar as c_int;
        *out.add(i) = tolower(b) as c_char;
    }
    *out.add(n) = 0;
    out
}

/// Lowercase a NUL-terminated string, ASCII-only; palloc'd result.
pub unsafe fn lowerstr(str: *const c_char) -> *mut c_char {
    let mut len: c_int = 0;
    while *str.add(len as usize) != 0 {
        len += 1;
    }
    lowerstr_with_len(str, len)
}

// ----------------------------------------------------------------------------
// tsearch_readline_state and the file-reading helpers.
//
// Faithful 1:1 translation of ts_locale.c.  AllocateFile/FreeFile are local
// stubs (storage/fd.h is unported); everything else routes through the real
// ported helpers (pg_get_line_buf, pg_any_to_server, initStringInfo,
// error_context_stack).
// ----------------------------------------------------------------------------

/// Working state for `tsearch_readline()` (merged from ts_locale.h).
#[repr(C)]
pub struct tsearch_readline_state {
    pub fp: *mut FILE,
    pub filename: *const c_char,
    pub lineno: c_int,
    pub buf: StringInfoData,
    pub curline: *mut c_char,
    pub cb: ErrorContextCallback,
}

/*
 * Set up to read a file using tsearch_readline().  This facility is
 * better than just reading the file directly because it provides error
 * context pointing to the specific line where a problem is detected.
 *
 * Note that the caller supplies the ereport() for file open failure;
 * this is so that a custom message can be provided.  The filename string
 * passed to tsearch_readline_begin() must remain valid through
 * tsearch_readline_end().
 */
pub unsafe fn tsearch_readline_begin(
    stp: *mut tsearch_readline_state,
    filename: *const c_char,
) -> bool {
    (*stp).fp = AllocateFile(filename, c"r".as_ptr());
    if (*stp).fp.is_null() {
        return false;
    }
    (*stp).filename = filename;
    (*stp).lineno = 0;
    initStringInfo(&raw mut (*stp).buf);
    (*stp).curline = core::ptr::null_mut();
    /* Setup error traceback support for ereport() */
    (*stp).cb.callback = tsearch_readline_callback;
    (*stp).cb.arg = stp as *mut c_void;
    (*stp).cb.previous = error_context_stack;
    error_context_stack = &raw mut (*stp).cb;
    true
}

/*
 * Read the next line from a tsearch data file (expected to be in UTF-8), and
 * convert it to database encoding if needed. The returned string is palloc'd.
 * NULL return means EOF.
 */
pub unsafe fn tsearch_readline(stp: *mut tsearch_readline_state) -> *mut c_char {
    let recoded: *mut c_char;

    /* Advance line number to use in error reports */
    (*stp).lineno += 1;

    /* Clear curline, it's no longer relevant */
    if !(*stp).curline.is_null() {
        if (*stp).curline != (*stp).buf.data {
            pfree((*stp).curline as *mut c_void);
        }
        (*stp).curline = core::ptr::null_mut();
    }

    /* Collect next line, if there is one */
    if !pg_get_line_buf((*stp).fp, &raw mut (*stp).buf) {
        return core::ptr::null_mut();
    }

    /* Validate the input as UTF-8, then convert to DB encoding if needed */
    recoded = pg_any_to_server((*stp).buf.data, (*stp).buf.len, PG_UTF8);

    /* Save the correctly-encoded string for possible error reports */
    (*stp).curline = recoded; /* might be equal to buf.data */

    /*
     * We always return a freshly pstrdup'd string.  This is clearly necessary
     * if pg_any_to_server() returned buf.data, and we need a second copy even
     * if encoding conversion did occur.  The caller is entitled to pfree the
     * returned string at any time, which would leave curline pointing to
     * recycled storage, causing problems if an error occurs after that point.
     * (It's preferable to return the result of pstrdup instead of the output
     * of pg_any_to_server, because the conversion result tends to be
     * over-allocated.  Since callers might save the result string directly
     * into a long-lived dictionary structure, we don't want it to be a larger
     * palloc chunk than necessary.  We'll reclaim the conversion result on
     * the next call.)
     */
    pstrdup(recoded)
}

/*
 * Close down after reading a file with tsearch_readline()
 */
pub unsafe fn tsearch_readline_end(stp: *mut tsearch_readline_state) {
    /* Suppress use of curline in any error reported below */
    if !(*stp).curline.is_null() {
        if (*stp).curline != (*stp).buf.data {
            pfree((*stp).curline as *mut c_void);
        }
        (*stp).curline = core::ptr::null_mut();
    }

    /* Release other resources */
    pfree((*stp).buf.data as *mut c_void);
    FreeFile((*stp).fp);

    /* Pop the error context stack */
    error_context_stack = (*stp).cb.previous;
}

/*
 * Error context callback for errors occurring while reading a tsearch
 * configuration file.
 */
unsafe extern "C" fn tsearch_readline_callback(arg: *mut c_void) {
    let stp = arg as *mut tsearch_readline_state;

    /*
     * We can't include the text of the config line for errors that occur
     * during tsearch_readline() itself.  The major cause of such errors is
     * encoding violations, and we daren't try to print error messages
     * containing badly-encoded data.
     */
    if !(*stp).curline.is_null() {
        errcontext!(
            "line {} of configuration file \"{}\": \"{}\"",
            (*stp).lineno,
            std::ffi::CStr::from_ptr((*stp).filename).to_string_lossy(),
            std::ffi::CStr::from_ptr((*stp).curline).to_string_lossy()
        );
    } else {
        errcontext!(
            "line {} of configuration file \"{}\"",
            (*stp).lineno,
            std::ffi::CStr::from_ptr((*stp).filename).to_string_lossy()
        );
    }
}

// ----------------------------------------------------------------------------
// Tests for the REAL classification + lowercasing logic.
// ----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    // A helper to get a NUL-terminated C string pointer from a Rust &str.
    fn cstr(s: &str) -> std::ffi::CString {
        std::ffi::CString::new(s).unwrap()
    }

    #[test]
    fn t_isspace_space_is_true() {
        let s = cstr(" ");
        unsafe {
            assert_eq!(t_isspace(s.as_ptr()), 1);
            assert_eq!(t_isspace_cstr(s.as_ptr()), 1);
        }
    }

    #[test]
    fn t_isdigit_five_is_true() {
        let s = cstr("5");
        unsafe {
            assert_eq!(t_isdigit(s.as_ptr()), 1);
        }
    }

    #[test]
    fn t_isalpha_and_alnum() {
        let a = cstr("a");
        let one = cstr("1");
        let bang = cstr("!");
        unsafe {
            assert_eq!(t_isalpha(a.as_ptr()), 1);
            assert_eq!(t_isalpha(one.as_ptr()), 0);
            assert_eq!(t_isalnum(a.as_ptr()), 1);
            assert_eq!(t_isalnum(one.as_ptr()), 1);
            assert_eq!(t_isalnum(bang.as_ptr()), 0);
        }
    }

    #[test]
    fn t_isprint_basic() {
        let a = cstr("A");
        let tab = cstr("\t");
        unsafe {
            assert_eq!(t_isprint(a.as_ptr()), 1);
            assert_eq!(t_isprint(tab.as_ptr()), 0);
        }
    }

    #[test]
    fn lowerstr_lowercases_ascii() {
        let s = cstr("ABC");
        unsafe {
            let out = lowerstr(s.as_ptr());
            let got = std::ffi::CStr::from_ptr(out).to_str().unwrap();
            assert_eq!(got, "abc");
        }
    }

    #[test]
    fn lowerstr_with_len_respects_len() {
        let s = cstr("ABCDEF");
        unsafe {
            let out = lowerstr_with_len(s.as_ptr(), 3);
            let got = std::ffi::CStr::from_ptr(out).to_str().unwrap();
            assert_eq!(got, "abc");
        }
    }
}
