//! tsearch/dict_simple.rs - the "simple" text-search dictionary.
//!
//! Source: postgres/src/backend/tsearch/dict_simple.c
//! Merged decls: the TSLexeme / TSL_* flags / DictSubState ("DictSubmitData")
//! and the StopList type from postgres/src/include/tsearch/ts_public.h that
//! dict_simple.c needs.  (The header also declares LexDescr/HeadlineParsedText
//! and DictInitData lives elsewhere -- see the DictInitData note below.)
//!
//! The simple dictionary does two things: lowercase the incoming token and,
//! optionally, drop it if it is a stopword.  Its `accept` flag (default true)
//! controls whether a non-stopword is emitted as a recognized lexeme or
//! reported as unrecognized (so a following dictionary in the chain gets it).
//!
//! #include mapping:
//!   - "postgres.h"                 -> crate::prelude::* (Datum, c-types, palloc0,
//!                                     pfree, ereport!/errmsg!, null/null_mut).
//!   - "catalog/pg_collation_d.h"   -> DEFAULT_COLLATION_OID from
//!                                     crate::catalog::pg_known_oids.
//!   - "commands/defrem.h"          -> defGetBoolean / defGetString from
//!                                     crate::commands::define; DefElem from
//!                                     crate::nodes::parsenodes.
//!   - "tsearch/ts_public.h"        -> TSLexeme / TSL_* / DictSubState / StopList
//!                                     merged below.
//!   - "utils/fmgrprotos.h"         -> the fmgr V1 call interface from
//!                                     crate::utils::fmgr (PG_GETARG_*!/PG_RETURN_*!).
//!   - "utils/formatting.h"         -> str_tolower(): NOT ported (formatting.c is
//!                                     absent), so lowercasing is done REAL via the
//!                                     ASCII lowerstr_with_len from
//!                                     crate::tsearch::ts_locale.  See STUB note.
//!
//! REAL vs STUB:
//!   - The option parsing (stopwords/accept via defGetString/defGetBoolean over
//!     the dictoptions List) is REAL.
//!   - The lexize accept/stopword decision is REAL.
//!   - StopList file loading (readstoplist) needs tsearch/stopwords infra + the
//!     fd/VFD layer; both are unported, so StopList is an EMPTY struct and
//!     readstoplist / searchstoplist are STUBBED: readstoplist is unimplemented!()
//!     and searchstoplist always returns false (no word is ever a stopword).
//!     dsimple_init therefore cannot actually honor a "stopwords" option yet; it
//!     hits the readstoplist stub, matching "not ported" rather than wrong-result.
//!   - str_tolower (locale-aware, collation-driven) is STUBBED by the ASCII
//!     lowerstr_with_len from ts_locale; collation is ignored for now. TODO: route
//!     through str_tolower once utils/formatting.c is ported.
//!
//! DictInitData note:
//!   PG 18's ts_public.h does NOT define a `DictInitData` struct; the task brief
//!   referred to it, but upstream dict_simple.c's dsimple_init takes the raw
//!   `List *dictoptions` as arg 0 (PG_GETARG_POINTER(0)), exactly as translated
//!   here.  "DictSubmitData" likewise corresponds to the header's `DictSubState`,
//!   which is included below for completeness even though dict_simple doesn't use
//!   it (it has no 4th-arg lexize entry point).

use crate::prelude::*; // Datum, c_char/c_int, palloc0/pfree, ereport!/errmsg!, null/null_mut, Oid

use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::commands::define::{defGetBoolean, defGetString};
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lfirst, List};
use crate::tsearch::ts_locale::lowerstr_with_len;
use crate::utils::fmgr::FunctionCallInfo;
// PG_GETARG_*!/PG_RETURN_*! are #[macro_export] macro_rules! living at the crate
// root; a glob import of utils::fmgr does NOT bring them in, so name them here.
use crate::{
    current_cell, foreach, PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_POINTER,
};

use core::ffi::CStr;

// ----------------------------------------------------------------------------
// ts_public.h decls needed here.
// ----------------------------------------------------------------------------

/// Return struct for any dictionary lexize function (ts_public.h `TSLexeme`).
///
/// `nvariant` tags which split-variant a lexeme belongs to (changes between
/// adjacent entries delimit variants); `flags` carries the TSL_* bits; `lexeme`
/// is a C string (NULL terminates a lexize result array).
#[repr(C)]
pub struct TSLexeme {
    /// Number of the current variant of a split word (see ts_public.h).
    pub nvariant: u16,
    /// See the TSL_* flag bits below.
    pub flags: u16,
    /// The lexeme as a C string; a NULL `lexeme` marks end-of-array.
    pub lexeme: *mut c_char,
}

// Flag bits that can appear in TSLexeme.flags.
/// Lexeme should advance the position counter.
pub const TSL_ADDPOS: u16 = 0x01;
/// Lexeme is a prefix to match.
pub const TSL_PREFIX: u16 = 0x02;
/// Lexeme came from a thesaurus-style filter.
pub const TSL_FILTER: u16 = 0x04;

/// Struct for supporting complex dictionaries like thesaurus; the 4th argument
/// for a dictlexize method is a pointer to this (ts_public.h `DictSubState`,
/// referred to as "DictSubmitData" in the task brief).  Unused by dict_simple,
/// included for header fidelity.
#[repr(C)]
pub struct DictSubState {
    /// in: marks for lexize_info that text end is reached.
    pub isend: bool,
    /// out: dict wants next lexeme.
    pub getnext: bool,
    /// internal dict state between calls with getnext == true.
    pub private_state: *mut c_void,
}

/// Often-useful stopword list (ts_public.h `StopList`).
///
/// STUB: the real StopList holds `len`/`stop` (a sorted `char **`) loaded from a
/// config file via the fd layer + tsearch/stopwords infra, neither of which is
/// ported.  We keep it an empty marker so DictSimple can embed it; the loader
/// (readstoplist) and lookup (searchstoplist) are stubbed below.
#[repr(C)]
#[derive(Default)]
pub struct StopList {
    // TODO(pg-port): add `len: c_int` + `stop: *mut *mut c_char` once
    // tsearch/stopwords.c + the fd/VFD readline layer are ported.
}

// ----------------------------------------------------------------------------
// Stopword infrastructure stubs (tsearch/stopwords.c, unported).
// ----------------------------------------------------------------------------

/// Load a stopword file into `s`, applying `wordop` to each word.
///
/// STUB: needs tsearch/stopwords.c + the fd/VFD readline layer (AllocateFile /
/// tsearch_readline) plus get_tsearch_config_filename, none of which are ported.
///
/// # Safety
/// Mirrors the C prototype; unconditionally unimplemented for now.
pub unsafe fn readstoplist(
    _fname: *const c_char,
    _s: *mut StopList,
    _wordop: unsafe fn(*const c_char, c_int, Oid) -> *mut c_char,
) {
    // TODO(pg-port): implement once tsearch/stopwords.c is translated.
    unimplemented!("readstoplist: tsearch/stopwords.c + fd readline layer not ported");
}

/// Test whether `key` is in stoplist `s`.
///
/// STUB: with the StopList loader unported, no word is ever a stopword, so this
/// always returns false.  This keeps dsimple_lexize's decision logic REAL.
///
/// # Safety
/// Pointers are read-only and may be the empty StopList; always safe here.
pub unsafe fn searchstoplist(_s: *const StopList, _key: *const c_char) -> bool {
    // TODO(pg-port): bsearch the loaded `stop` array once StopList is populated.
    false
}

/// Locale-aware lowercasing (formatting.c `str_tolower`).
///
/// STUB: utils/formatting.c is not ported, so this delegates to the ASCII
/// lowerstr_with_len from ts_locale and ignores the collation.  Signature matches
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
// DictSimple state.
// ----------------------------------------------------------------------------

/// Internal state of a configured simple dictionary.
#[repr(C)]
pub struct DictSimple {
    /// Stopwords to drop (currently always empty -- see StopList stub).
    pub stoplist: StopList,
    /// Whether non-stopwords are emitted as recognized lexemes.
    pub accept: bool,
}

// ----------------------------------------------------------------------------
// fmgr V1 entry points.
// ----------------------------------------------------------------------------

/// `dsimple_init(dictoptions) -> internal` - build a DictSimple from its options.
///
/// Reads the "stopwords" (defGetString) and "accept" (defGetBoolean) options off
/// the dictoptions List; rejects duplicates and unknown options.  `accept`
/// defaults to true.
///
/// # Safety
/// fcinfo arg 0 must be a `*mut List` of DefElem options (as fmgr guarantees for
/// the dict-init calling convention).
pub unsafe fn dsimple_init(fcinfo: FunctionCallInfo) -> Datum {
    let dictoptions = PG_GETARG_POINTER!(fcinfo, 0) as *mut List;
    let d = palloc0(core::mem::size_of::<DictSimple>()) as *mut DictSimple;
    let mut stoploaded = false;
    let mut acceptloaded = false;

    (*d).accept = true; // default

    foreach!(l, dictoptions, {
        let defel = lfirst(current_cell!(l)) as *mut DefElem;

        let defname = (*defel).defname;
        let name = CStr::from_ptr(defname).to_str().unwrap_or("");

        if name == "stopwords" {
            if stoploaded {
                ereport!(
                    ERROR,
                    errmsg!("multiple StopWords parameters")
                );
            }
            readstoplist(defGetString(defel), &mut (*d).stoplist, str_tolower);
            stoploaded = true;
        } else if name == "accept" {
            if acceptloaded {
                ereport!(
                    ERROR,
                    errmsg!("multiple Accept parameters")
                );
            }
            (*d).accept = defGetBoolean(defel);
            acceptloaded = true;
        } else {
            ereport!(
                ERROR,
                errmsg!("unrecognized simple dictionary parameter: \"{}\"", name)
            );
        }
    });

    PG_RETURN_POINTER!(d);
}

/// `dsimple_lexize(dict, in, len) -> internal` - lowercase + stopword-filter.
///
/// Returns a palloc'd TSLexeme array: a 1-element-plus-NULL array carrying the
/// lowercased lexeme on accept, a single NULL element (empty) for a rejected
/// stopword / empty token, or NULL to report the token as unrecognized when
/// `accept` is false.
///
/// # Safety
/// fcinfo arg 0 is a `*mut DictSimple`, arg 1 a `*const c_char` of `len` bytes,
/// arg 2 the int32 length -- as fmgr guarantees for the dict-lexize convention.
pub unsafe fn dsimple_lexize(fcinfo: FunctionCallInfo) -> Datum {
    let d = PG_GETARG_POINTER!(fcinfo, 0) as *mut DictSimple;
    let r#in = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_char;
    let len = PG_GETARG_INT32!(fcinfo, 2);

    let txt = str_tolower(r#in, len, DEFAULT_COLLATION_OID);

    if *txt == 0 || searchstoplist(&(*d).stoplist, txt) {
        // reject as stopword
        pfree(txt as *mut c_void);
        let res = palloc0(core::mem::size_of::<TSLexeme>() * 2) as *mut TSLexeme;
        PG_RETURN_POINTER!(res);
    } else if (*d).accept {
        // accept
        let res = palloc0(core::mem::size_of::<TSLexeme>() * 2) as *mut TSLexeme;
        (*res).lexeme = txt;
        PG_RETURN_POINTER!(res);
    } else {
        // report as unrecognized
        pfree(txt as *mut c_void);
        PG_RETURN_POINTER!(null_mut::<TSLexeme>());
    }
}

// ----------------------------------------------------------------------------
// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{Int32GetDatum, NullableDatum};
    use crate::LOCAL_FCINFO;

    // dsimple_lexize on a mixed-case token returns a 1-element (plus NULL
    // terminator) TSLexeme array carrying the lowercased lexeme, given an
    // empty stoplist and accept = true.
    #[test]
    fn dsimple_lexize_lowercases_and_accepts() {
        unsafe {
            let mut d = DictSimple {
                stoplist: StopList::default(),
                accept: true,
            };

            let token = b"HeLLo";
            LOCAL_FCINFO!(fcinfo, 3);
            (*fcinfo).args.as_mut_ptr().add(0).write(
                NullableDatum {
                    value: PointerGetDatum(&mut d as *mut DictSimple as *const c_void),
                    isnull: false,
                },
            );
            (*fcinfo).args.as_mut_ptr().add(1).write(
                NullableDatum {
                    value: PointerGetDatum(token.as_ptr() as *const c_void),
                    isnull: false,
                },
            );
            (*fcinfo).args.as_mut_ptr().add(2).write(
                NullableDatum {
                    value: Int32GetDatum(token.len() as i32),
                    isnull: false,
                },
            );

            let res = DatumGetPointer(dsimple_lexize(fcinfo)) as *mut TSLexeme;
            assert!(!res.is_null());
            // first element holds the lowercased lexeme
            assert!(!(*res).lexeme.is_null());
            let got = CStr::from_ptr((*res).lexeme).to_str().unwrap();
            assert_eq!(got, "hello");
            // second element is the NULL terminator
            assert!((*res.add(1)).lexeme.is_null());
        }
    }

    // When accept = false and the (lowercased) token is non-empty and not a
    // stopword, dsimple_lexize reports it as unrecognized by returning NULL.
    #[test]
    fn dsimple_lexize_unrecognized_when_not_accept() {
        unsafe {
            let mut d = DictSimple {
                stoplist: StopList::default(),
                accept: false,
            };

            let token = b"World";
            LOCAL_FCINFO!(fcinfo, 3);
            (*fcinfo).args.as_mut_ptr().add(0).write(
                NullableDatum {
                    value: PointerGetDatum(&mut d as *mut DictSimple as *const c_void),
                    isnull: false,
                },
            );
            (*fcinfo).args.as_mut_ptr().add(1).write(
                NullableDatum {
                    value: PointerGetDatum(token.as_ptr() as *const c_void),
                    isnull: false,
                },
            );
            (*fcinfo).args.as_mut_ptr().add(2).write(
                NullableDatum {
                    value: Int32GetDatum(token.len() as i32),
                    isnull: false,
                },
            );

            let res = DatumGetPointer(dsimple_lexize(fcinfo)) as *mut TSLexeme;
            assert!(res.is_null());
        }
    }
}
