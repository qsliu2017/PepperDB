//! tsearch/dict_synonym.rs - the "synonym" text-search dictionary.
//!
//! Source: postgres/src/backend/tsearch/dict_synonym.c
//!
//! A synonym dictionary replaces a word by its configured synonym.  At init time
//! `dsynonym_init` reads a synonym config file (one "in out" pair per line),
//! builds a `Syn[]` array, case-folds both words (unless `casesensitive`), and
//! sorts the array by the `in` word with `compareSyn` (strcmp).  At lookup time
//! `dsynonym_lexize` case-folds the incoming token and binary-searches the sorted
//! array; on a hit it returns a 1-element-plus-NULL `TSLexeme` array carrying the
//! `out` word (with any TSL_PREFIX flag parsed from a trailing `*`).
//!
//! #include mapping:
//!   - "postgres.h"               -> crate::prelude::* (Datum, c-types, palloc0/
//!                                    palloc/repalloc/pfree, pstrdup/pnstrdup,
//!                                    ereport!/errmsg!, null_mut).
//!   - "catalog/pg_collation_d.h" -> DEFAULT_COLLATION_OID
//!                                    (crate::catalog::pg_known_oids).
//!   - "commands/defrem.h"        -> defGetString / defGetBoolean
//!                                    (crate::commands::define); DefElem
//!                                    (crate::nodes::parsenodes).
//!   - "tsearch/ts_locale.h"      -> t_iseq / pg_mblen_cstr (the multibyte
//!                                    word-scan helpers) + tsearch_readline_state
//!                                    and the readline facility.
//!   - "tsearch/ts_public.h"      -> TSLexeme / TSL_PREFIX, reused from
//!                                    crate::tsearch::dict_simple.
//!   - "utils/fmgrprotos.h"       -> the fmgr V1 call interface
//!                                    (crate::utils::fmgr + PG_GETARG_*!/PG_RETURN_*!).
//!   - "utils/formatting.h"       -> str_tolower(): NOT ported (formatting.c is
//!                                    absent); folded REAL via the ASCII
//!                                    lowerstr_with_len from ts_locale.  See STUB.
//!
//! REAL vs STUB:
//!   - REAL: the `Syn` struct, `findwrd` word-scan (incl. the trailing-`*`
//!     TSL_PREFIX rule), `compareSyn`, the option parsing, the array build +
//!     case-fold, the pg_qsort sort, and the bsearch lexize lookup.  The line
//!     parsing/array build is factored into `dsynonym_build_lines` so it is
//!     exercised by the in-memory tests without touching a file.
//!   - STUB: `str_tolower` (utils/formatting.c unported) -> ASCII
//!     lowerstr_with_len, collation ignored.  The file-open path in
//!     `dsynonym_init` uses ts_locale's `tsearch_readline*`, which are
//!     `unimplemented!()` (the fd/VFD layer is unported) -- so init reaches that
//!     stub once a real file is required, matching "not ported" rather than a
//!     wrong result.  `bsearch` is provided locally via libc.

use crate::prelude::*; // Datum, c-types, palloc0/palloc/repalloc/pfree, pstrdup/pnstrdup, ereport!/errmsg!, null_mut

use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::commands::define::{defGetBoolean, defGetString};
use crate::mb::mbutils::pg_mblen_cstr;
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lfirst, List};
use crate::port::qsort::pg_qsort;
use crate::tsearch::dict_simple::{TSLexeme, TSL_PREFIX};
use crate::tsearch::ts_locale::{
    lowerstr_with_len, t_iseq, tsearch_readline, tsearch_readline_begin, tsearch_readline_end,
    tsearch_readline_state,
};
use crate::tsearch::ts_utils::get_tsearch_config_filename;
use crate::utils::fmgr::FunctionCallInfo;
// PG_GETARG_*!/PG_RETURN_*! are #[macro_export] macro_rules! at the crate root; a
// glob import does NOT bring them in, so name them here.
use crate::{current_cell, foreach, PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_POINTER};

use core::ffi::CStr;

// Local libc bindings (no libc crate per project rules).
extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *mut c_void;
}

// ----------------------------------------------------------------------------
// Synonym entry + dictionary state.
// ----------------------------------------------------------------------------

/// One synonym pair: case-folded `in` word -> `out` word, with the `out` length
/// and any TSL_* `flags` (TSL_PREFIX when the out word ended in `*`).
#[repr(C)]
pub struct Syn {
    /// The (case-folded) input word; the bsearch/sort key.
    pub r#in: *mut c_char,
    /// The (case-folded) output word emitted as the lexeme.
    pub out: *mut c_char,
    /// Byte length of `out` (precomputed for pnstrdup at lexize time).
    pub outlen: c_int,
    /// TSL_* flags carried into the result lexeme (e.g. TSL_PREFIX).
    pub flags: u16,
}

/// Configured synonym dictionary: a sorted `syn` array of `len` entries.
#[repr(C)]
pub struct DictSyn {
    /// Number of valid entries in `syn`.
    pub len: c_int,
    /// The sorted (by `in`) synonym array.
    pub syn: *mut Syn,
    /// Whether matching is case-sensitive (skips str_tolower).
    pub case_sensitive: bool,
}

/// Locale-aware lowercasing (formatting.c `str_tolower`).
///
/// STUB: utils/formatting.c is not ported, so this delegates to the ASCII
/// lowerstr_with_len from ts_locale and ignores the collation.  Signature mirrors
/// str_tolower(buff, nbytes, collid) returning a palloc'd NUL-terminated string.
///
/// # Safety
/// `buff` must point to at least `nbytes` readable bytes.
unsafe fn str_tolower(buff: *const c_char, nbytes: c_int, _collid: Oid) -> *mut c_char {
    // TODO(pg-port): route through utils/formatting.c str_tolower for full
    // locale/collation-correct lowercasing once it is translated.
    lowerstr_with_len(buff, nbytes)
}

// ----------------------------------------------------------------------------
// findwrd + compareSyn.
// ----------------------------------------------------------------------------

/// Finds the next whitespace-delimited word within the `in` string.  Returns a
/// pointer to the first character of the word, and sets `*end` to the next byte
/// after the last character.  A trailing `*` is not treated as a word character
/// when `flags` is non-null; in that case `*flags` is set to TSL_PREFIX.
///
/// Returns NULL (and sets `*end = NULL`) on an empty line.
///
/// # Safety
/// `in_` is a NUL-terminated, valid-encoding string; `end`/`flags` are writable
/// (or `flags` may be null to skip the prefix handling).
unsafe fn findwrd(mut in_: *mut c_char, end: *mut *mut c_char, flags: *mut u16) -> *mut c_char {
    // Skip leading spaces.
    while *in_ != 0 && isspace(*(in_ as *const c_uchar) as c_int) != 0 {
        in_ = in_.add(pg_mblen_cstr(in_) as usize);
    }

    // Return NULL on empty lines.
    if *in_ == 0 {
        *end = null_mut();
        return null_mut();
    }

    let start = in_;
    let mut lastchar = in_;

    // Find end of word.
    while *in_ != 0 && isspace(*(in_ as *const c_uchar) as c_int) == 0 {
        lastchar = in_;
        in_ = in_.add(pg_mblen_cstr(in_) as usize);
    }

    if (in_ as isize - lastchar as isize) == 1 && t_iseq(lastchar, b'*' as c_char) && !flags.is_null()
    {
        *flags = TSL_PREFIX;
        *end = lastchar;
    } else {
        if !flags.is_null() {
            *flags = 0;
        }
        *end = in_;
    }

    start
}

/// qsort/bsearch comparator: order `Syn` entries by their `in` word (strcmp).
///
/// # Safety
/// `a`/`b` point to `Syn` records with non-null `in` C strings.
unsafe fn compareSyn(a: *const c_void, b: *const c_void) -> c_int {
    strcmp((*(a as *const Syn)).r#in, (*(b as *const Syn)).r#in)
}

/// `extern "C"` shim over `compareSyn` for libc `bsearch` (which wants a C ABI
/// function pointer; `pg_qsort` instead takes the plain-Rust `compareSyn`).
///
/// # Safety
/// As `compareSyn`.
unsafe extern "C" fn compareSyn_c(a: *const c_void, b: *const c_void) -> c_int {
    compareSyn(a, b)
}

// ----------------------------------------------------------------------------
// Line-parsing core (factored out so tests can drive it in memory).
// ----------------------------------------------------------------------------

/// Build a sorted `DictSyn` from an iterator of already-read config lines, the
/// exact body of `dsynonym_init`'s read loop + sort.  Each `line` is a palloc'd,
/// NUL-terminated, mutable C string that this function consumes (frees) -- just
/// like the upstream loop pfree's each `tsearch_readline()` result.
///
/// Lines that are empty, hold only one word, or one word plus whitespace are
/// silently skipped, matching upstream.
///
/// # Safety
/// Every pointer in `lines` must be a valid, writable, NUL-terminated, palloc'd C
/// string.  Returns a palloc'd `DictSyn`.
pub unsafe fn dsynonym_build_lines(
    lines: impl IntoIterator<Item = *mut c_char>,
    case_sensitive: bool,
) -> *mut DictSyn {
    let d = palloc0(core::mem::size_of::<DictSyn>()) as *mut DictSyn;
    let mut cur: c_int = 0;
    let mut flags: u16 = 0;

    for line in lines {
        // The upstream `goto skipline` always pfree's `line`; we mirror that by
        // doing the parse inside a labelled block (break == goto skipline) and
        // pfree-ing `line` unconditionally after it.
        'parse: {
            // First word.
            let mut end1: *mut c_char = null_mut();
            let starti = findwrd(line, &mut end1, null_mut());
            if starti.is_null() {
                break 'parse; // empty line
            }
            if *end1 == 0 {
                break 'parse; // only one word
            }
            *end1 = 0;

            // Second word (with prefix-flag handling).
            let mut end2: *mut c_char = null_mut();
            let starto = findwrd(end1.add(1), &mut end2, &mut flags);
            if starto.is_null() {
                break 'parse; // only one word (+whitespace)
            }
            *end2 = 0;

            // Grow the array if needed.
            if cur >= (*d).len {
                if (*d).len == 0 {
                    (*d).len = 64;
                    (*d).syn = palloc(core::mem::size_of::<Syn>() * (*d).len as usize) as *mut Syn;
                } else {
                    (*d).len *= 2;
                    (*d).syn = repalloc(
                        (*d).syn as *mut c_void,
                        core::mem::size_of::<Syn>() * (*d).len as usize,
                    ) as *mut Syn;
                }
            }

            let slot = (*d).syn.add(cur as usize);
            if case_sensitive {
                (*slot).r#in = pstrdup(starti);
                (*slot).out = pstrdup(starto);
            } else {
                (*slot).r#in = str_tolower(starti, strlen(starti) as c_int, DEFAULT_COLLATION_OID);
                (*slot).out = str_tolower(starto, strlen(starto) as c_int, DEFAULT_COLLATION_OID);
            }
            (*slot).outlen = strlen(starto) as c_int;
            (*slot).flags = flags;

            cur += 1;
        }

        // skipline: pfree(line)
        pfree(line as *mut c_void);
    }

    (*d).len = cur;
    if (*d).len > 0 {
        pg_qsort(
            (*d).syn as *mut c_void,
            (*d).len as usize,
            core::mem::size_of::<Syn>(),
            compareSyn,
        );
    }

    (*d).case_sensitive = case_sensitive;
    d
}

// ----------------------------------------------------------------------------
// fmgr V1 entry points.
// ----------------------------------------------------------------------------

/// `dsynonym_init(dictoptions) -> internal` - build a DictSyn from its options.
///
/// Reads the "synonyms" (defGetString) and "casesensitive" (defGetBoolean)
/// options off the dictoptions List, requires "synonyms", resolves the config
/// path via get_tsearch_config_filename, then reads + parses the file.
///
/// # Safety
/// fcinfo arg 0 must be a `*mut List` of DefElem options (fmgr dict-init
/// convention).
pub unsafe fn dsynonym_init(fcinfo: FunctionCallInfo) -> Datum {
    let dictoptions = PG_GETARG_POINTER!(fcinfo, 0) as *mut List;
    let mut filename: *mut c_char = null_mut();
    let mut case_sensitive = false;

    foreach!(l, dictoptions, {
        let defel = lfirst(current_cell!(l)) as *mut DefElem;
        let name = CStr::from_ptr((*defel).defname).to_str().unwrap_or("");

        if name == "synonyms" {
            filename = defGetString(defel);
        } else if name == "casesensitive" {
            case_sensitive = defGetBoolean(defel);
        } else {
            ereport!(
                ERROR,
                errmsg!("unrecognized synonym parameter: \"{}\"", name)
            );
        }
    });

    if filename.is_null() {
        ereport!(ERROR, errmsg!("missing Synonyms parameter"));
    }

    let ext = b"syn\0";
    let filename = get_tsearch_config_filename(filename, ext.as_ptr() as *const c_char);

    let mut trst: tsearch_readline_state = core::mem::zeroed();
    if !tsearch_readline_begin(&mut trst, filename) {
        // %m -> errno detail omitted (no strerror plumbing here).
        ereport!(
            ERROR,
            errmsg!(
                "could not open synonym file \"{}\"",
                CStr::from_ptr(filename).to_str().unwrap_or("")
            )
        );
    }

    // Drain the file into our line-build core.  `tsearch_readline` is currently
    // STUBBED (unimplemented!()), so control reaches here only once the fd/VFD
    // layer is ported; the parsing/build itself is REAL and shared with tests.
    let mut lines: Vec<*mut c_char> = Vec::new();
    loop {
        let line = tsearch_readline(&mut trst);
        if line.is_null() {
            break;
        }
        lines.push(line);
    }
    tsearch_readline_end(&mut trst);

    let d = dsynonym_build_lines(lines, case_sensitive);

    PG_RETURN_POINTER!(d);
}

/// `dsynonym_lexize(dict, in, len) -> internal` - case-fold + bsearch lookup.
///
/// On a hit returns a palloc'd 1-element-plus-NULL `TSLexeme` array carrying the
/// matched `out` word (and its flags); on a miss / empty input returns NULL so a
/// following dictionary in the chain gets the token.
///
/// # Safety
/// fcinfo arg 0 is a `*mut DictSyn`, arg 1 a `*const c_char` of `len` bytes, arg 2
/// the int32 length (fmgr dict-lexize convention).
pub unsafe fn dsynonym_lexize(fcinfo: FunctionCallInfo) -> Datum {
    let d = PG_GETARG_POINTER!(fcinfo, 0) as *mut DictSyn;
    let r#in = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_char;
    let len = PG_GETARG_INT32!(fcinfo, 2);

    // note: d->len test protects against Solaris bsearch-of-no-items bug.
    if len <= 0 || (*d).len <= 0 {
        PG_RETURN_POINTER!(null_mut::<TSLexeme>());
    }

    let mut key = Syn {
        r#in: null_mut(),
        out: null_mut(),
        outlen: 0,
        flags: 0,
    };
    if (*d).case_sensitive {
        key.r#in = pnstrdup(r#in, len as usize);
    } else {
        key.r#in = str_tolower(r#in, len, DEFAULT_COLLATION_OID);
    }

    let found = bsearch(
        &key as *const Syn as *const c_void,
        (*d).syn as *const c_void,
        (*d).len as usize,
        core::mem::size_of::<Syn>(),
        compareSyn_c,
    ) as *mut Syn;
    pfree(key.r#in as *mut c_void);

    if found.is_null() {
        PG_RETURN_POINTER!(null_mut::<TSLexeme>());
    }

    let res = palloc0(core::mem::size_of::<TSLexeme>() * 2) as *mut TSLexeme;
    (*res).lexeme = pnstrdup((*found).out, (*found).outlen as usize);
    (*res).flags = (*found).flags;

    PG_RETURN_POINTER!(res);
}

// ----------------------------------------------------------------------------
// Tests (in-memory: build a Syn[] from "a b" lines, no file reading).
// ----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetPointer, Int32GetDatum, NullableDatum, PointerGetDatum};
    use crate::LOCAL_FCINFO;

    // palloc a NUL-terminated mutable copy of `s`, as tsearch_readline would.
    unsafe fn dup_line(s: &str) -> *mut c_char {
        let bytes = s.as_bytes();
        let p = palloc(bytes.len() + 1) as *mut c_char;
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), p as *mut u8, bytes.len());
        *p.add(bytes.len()) = 0;
        p
    }

    // Run dsynonym_lexize for `word` against dict `d`; returns the matched out
    // lexeme as a String, or None on a miss.
    unsafe fn lookup(d: *mut DictSyn, word: &str) -> Option<String> {
        let token = word.as_bytes();
        LOCAL_FCINFO!(fcinfo, 3);
        (*fcinfo).args.as_mut_ptr().add(0).write(NullableDatum {
            value: PointerGetDatum(d as *const c_void),
            isnull: false,
        });
        (*fcinfo).args.as_mut_ptr().add(1).write(NullableDatum {
            value: PointerGetDatum(token.as_ptr() as *const c_void),
            isnull: false,
        });
        (*fcinfo).args.as_mut_ptr().add(2).write(NullableDatum {
            value: Int32GetDatum(token.len() as i32),
            isnull: false,
        });
        let res = DatumGetPointer(dsynonym_lexize(fcinfo)) as *mut TSLexeme;
        if res.is_null() {
            return None;
        }
        assert!(!(*res).lexeme.is_null());
        // The terminator element must be NULL.
        assert!((*res.add(1)).lexeme.is_null());
        Some(CStr::from_ptr((*res).lexeme).to_str().unwrap().to_string())
    }

    // Snapshot the sorted (in -> out) pairs of a built dict for equality checks.
    unsafe fn pairs(d: *mut DictSyn) -> Vec<(String, String)> {
        (0..(*d).len)
            .map(|i| {
                let s = (*d).syn.add(i as usize);
                (
                    CStr::from_ptr((*s).r#in).to_str().unwrap().to_string(),
                    CStr::from_ptr((*s).out).to_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn sample_lines() -> &'static [&'static str] {
        // Deliberately unsorted; includes mixed case and skip-worthy lines.
        &[
            "Postgres pgsql",
            "a b",
            "indices index",
            "",            // empty -> skipped
            "loneword",    // single word -> skipped
            "GoOgLe search",
        ]
    }

    #[test]
    fn lexize_finds_synonym_and_is_case_insensitive() {
        unsafe {
            let lines: Vec<*mut c_char> = sample_lines().iter().map(|s| dup_line(s)).collect();
            let d = dsynonym_build_lines(lines, false);

            // "a" -> "b"
            assert_eq!(lookup(d, "a").as_deref(), Some("b"));
            // case-insensitive on the input side: "POSTGRES" matches folded "postgres"
            assert_eq!(lookup(d, "POSTGRES").as_deref(), Some("pgsql"));
            assert_eq!(lookup(d, "google").as_deref(), Some("search"));
            // absent word -> None
            assert_eq!(lookup(d, "absent"), None);
            // skipped lines did not create entries
            assert_eq!(lookup(d, "loneword"), None);
        }
    }

    #[test]
    fn case_sensitive_does_not_fold() {
        unsafe {
            let lines: Vec<*mut c_char> = sample_lines().iter().map(|s| dup_line(s)).collect();
            let d = dsynonym_build_lines(lines, true);

            // Stored as "Postgres"; exact-case hit, lowercase miss.
            assert_eq!(lookup(d, "Postgres").as_deref(), Some("pgsql"));
            assert_eq!(lookup(d, "postgres"), None);
        }
    }

    #[test]
    fn prefix_flag_parsed_from_trailing_star() {
        unsafe {
            // out word ends in '*': TSL_PREFIX, and the '*' is stripped from out.
            let lines = vec![dup_line("crab crustacean*")];
            let d = dsynonym_build_lines(lines, false);
            assert_eq!((*d).len, 1);
            let s = (*d).syn;
            assert_eq!(CStr::from_ptr((*s).out).to_str().unwrap(), "crustacean");
            assert_eq!((*s).flags, TSL_PREFIX);

            // And the result lexeme carries the flag.
            let token = b"crab";
            LOCAL_FCINFO!(fcinfo, 3);
            (*fcinfo).args.as_mut_ptr().add(0).write(NullableDatum {
                value: PointerGetDatum(d as *const c_void),
                isnull: false,
            });
            (*fcinfo).args.as_mut_ptr().add(1).write(NullableDatum {
                value: PointerGetDatum(token.as_ptr() as *const c_void),
                isnull: false,
            });
            (*fcinfo).args.as_mut_ptr().add(2).write(NullableDatum {
                value: Int32GetDatum(token.len() as i32),
                isnull: false,
            });
            let res = DatumGetPointer(dsynonym_lexize(fcinfo)) as *mut TSLexeme;
            assert!(!res.is_null());
            assert_eq!((*res).flags, TSL_PREFIX);
            assert_eq!(CStr::from_ptr((*res).lexeme).to_str().unwrap(), "crustacean");
        }
    }

    #[test]
    fn building_same_lines_twice_is_idempotent() {
        unsafe {
            let l1: Vec<*mut c_char> = sample_lines().iter().map(|s| dup_line(s)).collect();
            let l2: Vec<*mut c_char> = sample_lines().iter().map(|s| dup_line(s)).collect();
            let d1 = dsynonym_build_lines(l1, false);
            let d2 = dsynonym_build_lines(l2, false);
            assert_eq!((*d1).len, (*d2).len);
            assert_eq!(pairs(d1), pairs(d2));
            // And the array is actually sorted by `in`.
            let p = pairs(d1);
            let mut sorted = p.clone();
            sorted.sort();
            assert_eq!(p, sorted);
        }
    }
}
