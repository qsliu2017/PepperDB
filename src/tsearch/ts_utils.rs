//! tsearch/ts_utils.rs - various tsearch support functions.
//!
//! Source: postgres/src/backend/tsearch/ts_utils.c
//! Merged decls: the `StopList` struct + readstoplist/searchstoplist prototypes
//!               from postgres/src/include/tsearch/ts_public.h.
//!
//! #include mapping:
//!   - "postgres.h"                  -> crate::prelude::* (Datum, c-types, palloc,
//!                                      pfree, repalloc, ereport!, elog!, ...)
//!   - <ctype.h>                     -> libc isspace bound via `extern "C"`
//!                                      (matches the raw `isspace((unsigned char)*pbuf)`
//!                                      in readstoplist's trailing-space trim).
//!   - "catalog/pg_collation_d.h"    -> DEFAULT_COLLATION_OID from
//!                                      crate::catalog::pg_known_oids.
//!   - "miscadmin.h"                 -> my_exec_path (a process-global path string).
//!                                      STUBBED here (see get_share_path note).
//!   - "tsearch/ts_locale.h"         -> crate::tsearch::ts_locale (tsearch_readline*
//!                                      family, pg_mblen_cstr re-exported via mbutils).
//!   - "tsearch/ts_public.h"         -> the StopList struct, merged below.
//!
//! REAL vs STUB:
//!   * get_tsearch_config_filename: the basename whitelist check and the
//!     snprintf path assembly ("$SHAREDIR/tsearch_data/<base>.<ext>") are REAL.
//!     get_share_path() (path.c) and my_exec_path (main.c) are NOT ported, so
//!     get_share_path is STUBBED to write a fixed placeholder share dir into the
//!     caller's buffer; the real assembly around it is preserved exactly.
//!   * readstoplist: the line-trim / empty-skip / dynamic-array grow / wordop
//!     application / final sort logic is REAL. It depends on the tsearch_readline
//!     facility (ts_locale), which is itself STUBBED on the unported fd/VFD layer;
//!     so readstoplist will panic via tsearch_readline_begin until that lands.
//!     The sort uses pg_qsort_strcmp semantics (libc strcmp on the char* keys)
//!     via slice::sort_by, matching qsort(..., pg_qsort_strcmp).
//!   * searchstoplist: REAL. A binary search over the sorted s.stop matching the
//!     C bsearch(&key, s->stop, s->len, sizeof(char*), pg_qsort_strcmp).
//!
//! The sort (readstoplist tail) + bsearch (searchstoplist) + path assembly are
//! the self-contained, testable core; tests cover searchstoplist against a
//! hand-built sorted StopList and the path-assembly + whitelist of
//! get_tsearch_config_filename.

use crate::prelude::*;

use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::tsearch::ts_locale::{
    tsearch_readline, tsearch_readline_begin, tsearch_readline_end, tsearch_readline_state,
};

use crate::mb::mbutils::pg_mblen_cstr;

extern "C" {
    fn isspace(c: c_int) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn snprintf(buf: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
// TODO(pg-port): pull real values from utils/errcodes.h.
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_CONFIG_FILE_ERROR: c_int = 0;

// ----------------------------------------------------------------------------
// Merged from tsearch/ts_public.h:
//
//   typedef struct { int len; char **stop; } StopList;
//
// "Often useful stopword list management".
// ----------------------------------------------------------------------------

/// Sorted stop-word list. `stop` is a palloc'd array of `len` palloc'd C strings,
/// kept sorted by `strcmp` so searchstoplist can binary-search it.
#[repr(C)]
pub struct StopList {
    pub len: c_int,
    pub stop: *mut *mut c_char,
}

// ----------------------------------------------------------------------------
// Path constants / stubs.
// ----------------------------------------------------------------------------

/// `#define MAXPGPATH 1024` (pg_config_manual.h). Buffer size for path assembly.
pub const MAXPGPATH: usize = 1024;

/// STUB(pg-port): placeholder for the unported `my_exec_path` global (main.c),
/// the full path of the running postgres executable. get_share_path() derives
/// the share dir from it; since neither is ported, we keep an empty sentinel and
/// let the stubbed get_share_path() ignore it.
/// TODO(pg-port): wire to the real my_exec_path once main.c bootstrap lands.
static mut MY_EXEC_PATH: [c_char; MAXPGPATH] = [0; MAXPGPATH];

/// STUB(pg-port): real impl is get_share_path() in src/port/path.c, which
/// resolves "<prefix>/share" relative to the executable in `my_exec_path`.
/// That whole path-resolution layer is unported, so we write a fixed placeholder
/// share directory into `ret` (NUL-terminated, bounded by MAXPGPATH). The REAL
/// snprintf assembly in get_tsearch_config_filename runs unchanged on top of it.
/// TODO(pg-port): replace with the real get_share_path (src/port/path.c).
unsafe fn get_share_path(_my_exec_path: *const c_char, ret: *mut c_char) {
    // Placeholder share dir. Real value is e.g. "/usr/local/pgsql/share".
    const PLACEHOLDER: &[u8] = b"/SHAREDIR\0";
    core::ptr::copy_nonoverlapping(PLACEHOLDER.as_ptr() as *const c_char, ret, PLACEHOLDER.len());
}

/// STUB(pg-port): pg_qsort_strcmp (port/qsort.c) compares two `const void *`
/// each pointing at a `char *` element, dispatching to strcmp. Provided locally
/// so the sort/bsearch comparators read identically to the C; we don't actually
/// pass it to a C qsort (we use Rust slice methods), but keep it for fidelity.
unsafe fn pg_qsort_strcmp(a: *const c_void, b: *const c_void) -> c_int {
    let pa = *(a as *const *const c_char);
    let pb = *(b as *const *const c_char);
    strcmp(pa, pb)
}

// ----------------------------------------------------------------------------
// get_tsearch_config_filename
// ----------------------------------------------------------------------------

/// Given the base name and extension of a tsearch config file, return its full
/// path name. The base name is user-supplied and is checked to prevent pathname
/// attacks; the extension is assumed safe. Result is a palloc'd string.
pub unsafe fn get_tsearch_config_filename(
    basename: *const c_char,
    extension: *const c_char,
) -> *mut c_char {
    // We limit the basename to a-z, 0-9, and underscores. This may be overly
    // restrictive, but we don't want to allow access to anything outside the
    // tsearch_data directory, so for instance '/' *must* be rejected, and on
    // some platforms '\' and ':' are risky as well. Uppercase could cause
    // case-sensitive vs case-insensitive filesystem incompatibilities, and
    // non-ASCII chars create other risks, so a tight policy seems best.
    const ALLOWED: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_\0";
    if strspn(basename, ALLOWED.as_ptr() as *const c_char) != strlen(basename) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!(
                "invalid text search configuration file name \"{}\"",
                cstr_to_string(basename)
            )
        );
        unreachable!();
    }

    let mut sharepath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    get_share_path(MY_EXEC_PATH.as_ptr(), sharepath.as_mut_ptr());

    let result = palloc(MAXPGPATH) as *mut c_char;
    const FMT: &[u8] = b"%s/tsearch_data/%s.%s\0";
    snprintf(
        result,
        MAXPGPATH,
        FMT.as_ptr() as *const c_char,
        sharepath.as_ptr(),
        basename,
        extension,
    );

    result
}

// ----------------------------------------------------------------------------
// readstoplist
// ----------------------------------------------------------------------------

/// `wordop` callback type: `char *(*)(const char *, size_t, Oid)`.
pub type WordOpFn = unsafe fn(*const c_char, usize, Oid) -> *mut c_char;

/// Reads a stop-word file. Each word is run through `wordop`, if given. `wordop`
/// may either modify the input in-place or palloc a new version.
pub unsafe fn readstoplist(fname: *const c_char, s: *mut StopList, wordop: Option<WordOpFn>) {
    let mut stop: *mut *mut c_char = null_mut();

    (*s).len = 0;
    if !fname.is_null() && *fname != 0 {
        let extension = b"stop\0";
        let filename = get_tsearch_config_filename(fname, extension.as_ptr() as *const c_char);
        let mut trst: tsearch_readline_state = core::mem::zeroed();
        let mut reallen: c_int = 0;

        if !tsearch_readline_begin(&mut trst, filename) {
            // C appends ": %m" (errno text); the %m machinery is not ported, so
            // the message stops at the filename. TODO(pg-port): append %m text.
            let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
            ereport!(
                ERROR,
                errmsg!(
                    "could not open stop-word file \"{}\"",
                    cstr_to_string(filename)
                )
            );
            unreachable!();
        }

        loop {
            let line = tsearch_readline(&mut trst);
            if line.is_null() {
                break;
            }

            // Trim trailing space.
            let mut pbuf = line;
            while *pbuf != 0 && isspace(*(pbuf as *const c_uchar) as c_int) == 0 {
                pbuf = pbuf.add(pg_mblen_cstr(pbuf) as usize);
            }
            *pbuf = 0;

            // Skip empty lines.
            if *line == 0 {
                pfree(line as *mut c_void);
                continue;
            }

            if (*s).len >= reallen {
                if reallen == 0 {
                    reallen = 64;
                    stop = palloc((core::mem::size_of::<*mut c_char>() as c_int * reallen) as Size)
                        as *mut *mut c_char;
                } else {
                    reallen *= 2;
                    stop = repalloc(
                        stop as *mut c_void,
                        (core::mem::size_of::<*mut c_char>() as c_int * reallen) as Size,
                    ) as *mut *mut c_char;
                }
            }

            if let Some(op) = wordop {
                let w = op(line, strlen(line), DEFAULT_COLLATION_OID);
                *stop.add((*s).len as usize) = w;
                if w != line {
                    pfree(line as *mut c_void);
                }
            } else {
                *stop.add((*s).len as usize) = line;
            }

            (*s).len += 1;
        }

        tsearch_readline_end(&mut trst);
        pfree(filename as *mut c_void);
    }

    (*s).stop = stop;

    // Sort to allow binary searching.
    if !(*s).stop.is_null() && (*s).len > 0 {
        // qsort(s->stop, s->len, sizeof(char *), pg_qsort_strcmp)
        let slice = core::slice::from_raw_parts_mut((*s).stop, (*s).len as usize);
        slice.sort_by(|a, b| {
            let c = strcmp(*a, *b);
            c.cmp(&0)
        });
    }
}

// ----------------------------------------------------------------------------
// searchstoplist
// ----------------------------------------------------------------------------

/// Binary-search the sorted stop list for `key`. Mirrors the C:
///   bsearch(&key, s->stop, s->len, sizeof(char *), pg_qsort_strcmp)
/// returning whether a matching element was found.
pub unsafe fn searchstoplist(s: *mut StopList, key: *mut c_char) -> bool {
    if (*s).stop.is_null() || (*s).len <= 0 {
        return false;
    }
    let slice = core::slice::from_raw_parts((*s).stop, (*s).len as usize);
    // Manual bsearch matching pg_qsort_strcmp(&key, &elem) == strcmp(key, elem).
    let mut lo: isize = 0;
    let mut hi: isize = slice.len() as isize - 1;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let cmp = strcmp(key, slice[mid as usize]);
        if cmp == 0 {
            return true;
        } else if cmp < 0 {
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }
    false
}

// ----------------------------------------------------------------------------
// Helpers.
// ----------------------------------------------------------------------------

/// Render a C string as an owned Rust String for ereport!/elog! interpolation.
unsafe fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    let len = strlen(p);
    let bytes = core::slice::from_raw_parts(p as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

// Silence unused warnings for the fidelity-only comparator (kept to mirror C).
#[allow(dead_code)]
const _USES: unsafe fn(*const c_void, *const c_void) -> c_int = pg_qsort_strcmp;

// ----------------------------------------------------------------------------
// Tests for the REAL, self-contained core: searchstoplist bsearch over a
// hand-built sorted StopList, plus get_tsearch_config_filename path assembly
// and the basename whitelist boundary.
// ----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    // palloc-backed StopList from a pre-sorted list of words. Caller must keep
    // the returned Vec<CString>-equivalent storage alive: we leak palloc'd
    // copies via the crate allocator, so they stay valid for the test.
    unsafe fn build_stoplist(words: &[&str]) -> StopList {
        let n = words.len();
        let arr = palloc((core::mem::size_of::<*mut c_char>() * n) as Size) as *mut *mut c_char;
        for (i, w) in words.iter().enumerate() {
            let bytes = w.as_bytes();
            let p = palloc((bytes.len() + 1) as Size) as *mut c_char;
            core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
            *p.add(bytes.len()) = 0;
            *arr.add(i) = p;
        }
        StopList {
            len: n as c_int,
            stop: arr,
        }
    }

    unsafe fn cstr(s: &str) -> *mut c_char {
        let bytes = s.as_bytes();
        let p = palloc((bytes.len() + 1) as Size) as *mut c_char;
        core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
        *p.add(bytes.len()) = 0;
        p
    }

    #[test]
    fn searchstoplist_finds_present_keys() {
        unsafe {
            // Must be sorted by strcmp (ASCII order).
            let mut s = build_stoplist(&["a", "and", "is", "of", "the", "to"]);
            for w in ["a", "and", "is", "of", "the", "to"] {
                let k = cstr(w);
                assert!(searchstoplist(&mut s, k), "should find {}", w);
            }
        }
    }

    #[test]
    fn searchstoplist_rejects_absent_keys() {
        unsafe {
            let mut s = build_stoplist(&["a", "and", "is", "of", "the", "to"]);
            for w in ["", "b", "an", "zzz", "thee", "x"] {
                let k = cstr(w);
                assert!(!searchstoplist(&mut s, k), "should NOT find {:?}", w);
            }
        }
    }

    #[test]
    fn searchstoplist_empty_list_returns_false() {
        unsafe {
            let mut empty = StopList {
                len: 0,
                stop: null_mut(),
            };
            let k = cstr("a");
            assert!(!searchstoplist(&mut empty, k));

            // Non-null storage but len 0 still returns false.
            let mut zero = build_stoplist(&["a"]);
            zero.len = 0;
            assert!(!searchstoplist(&mut zero, k));
        }
    }

    #[test]
    fn searchstoplist_single_element() {
        unsafe {
            let mut s = build_stoplist(&["solo"]);
            assert!(searchstoplist(&mut s, cstr("solo")));
            assert!(!searchstoplist(&mut s, cstr("solx")));
            assert!(!searchstoplist(&mut s, cstr("sol")));
        }
    }

    #[test]
    fn config_filename_assembles_path() {
        unsafe {
            let base = cstr("english");
            let ext = cstr("stop");
            let got = get_tsearch_config_filename(base, ext);
            let s = CStr::from_ptr(got).to_str().unwrap();
            // get_share_path stub writes "/SHAREDIR".
            assert_eq!(s, "/SHAREDIR/tsearch_data/english.stop");
        }
    }

    #[test]
    fn config_filename_allows_digits_and_underscore() {
        unsafe {
            let base = cstr("my_dict_2");
            let ext = cstr("dict");
            let got = get_tsearch_config_filename(base, ext);
            let s = CStr::from_ptr(got).to_str().unwrap();
            assert_eq!(s, "/SHAREDIR/tsearch_data/my_dict_2.dict");
        }
    }

    #[test]
    #[should_panic]
    fn config_filename_rejects_slash() {
        unsafe {
            let base = cstr("../etc/passwd");
            let ext = cstr("stop");
            let _ = get_tsearch_config_filename(base, ext);
        }
    }

    #[test]
    #[should_panic]
    fn config_filename_rejects_uppercase() {
        unsafe {
            let base = cstr("English");
            let ext = cstr("stop");
            let _ = get_tsearch_config_filename(base, ext);
        }
    }
}
