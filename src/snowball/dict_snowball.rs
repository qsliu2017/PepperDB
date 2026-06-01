//! snowball/dict_snowball.rs - the Snowball stemmer text-search dictionary.
//!
//! Source: postgres/src/backend/snowball/dict_snowball.c
//!
//! Registers the Snowball stemmers as a TS dictionary: `dsnowball_init` parses
//! the Language / StopWords options and selects a stemmer module; `dsnowball_lexize`
//! lowercases the token, optionally recodes it to UTF-8, runs the stemmer, and
//! returns the stemmed lexeme (a 2-element TSLexeme array, NULL-terminated).
//! Follows the sibling crate::tsearch::dict_simple TS-dict idioms.
//!
//! #include mapping:
//!   - "postgres.h"                 -> crate::prelude::* (Datum, c-types, palloc0,
//!                                     pfree, repalloc, ereport!/errmsg!, null_mut,
//!                                     MemoryContext / CurrentMemoryContext /
//!                                     MemoryContextSwitchTo -- all REAL via prelude).
//!   - "catalog/pg_collation_d.h"   -> DEFAULT_COLLATION_OID from
//!                                     crate::catalog::pg_known_oids.
//!   - "commands/defrem.h"          -> defGetString from crate::commands::define;
//!                                     DefElem from crate::nodes::parsenodes.
//!   - "mb/pg_wchar.h"              -> PG_UTF8 / PG_SQL_ASCII (pg_enc variants) from
//!                                     crate::mb::wchar; GetDatabaseEncoding /
//!                                     GetDatabaseEncodingName / pg_server_to_any /
//!                                     pg_any_to_server from crate::mb::mbutils.
//!   - "tsearch/ts_public.h"        -> TSLexeme (reused from crate::tsearch::dict_simple);
//!                                     StopList / readstoplist / searchstoplist from
//!                                     crate::tsearch::ts_utils.
//!   - "snowball/libstemmer/header.h" + the per-language stem headers -> SN_env /
//!                                     symbol / SN_set_current from crate::snowball::api
//!                                     and the per-language *_UTF_8_* stemmers.
//!   - "utils/formatting.h"         -> str_tolower(): NOT ported (formatting.c is
//!                                     absent), STUBBED via the ASCII lowerstr_with_len
//!                                     from crate::tsearch::ts_locale (collation ignored).
//!   - pg_strcasecmp                -> crate::port::pgstrcasecmp.
//!
//! DEVIATIONS / STUBS:
//!   - SUBSET STEMMER TABLE: upstream lists ~50 stemmer modules (ISO-8859-1/2,
//!     KOI8-R, and UTF-8 variants).  Only the 13 PORTED UTF-8 stemmers are wired
//!     here (armenian, danish, dutch, english, finnish, german, hindi, hungarian,
//!     indonesian, irish, nepali, norwegian, swedish).  See the
//!     "TODO: add remaining stemmers once ported" marker on the table.
//!   - str_tolower: STUBBED to ASCII lowerstr_with_len (no collation), matching
//!     dict_simple's stub.  TODO: route through utils/formatting.c once ported.
//!   - pg_any_to_server (back-recode path) is REAL (mbutils is ported).
//!   - GETSTRUCT / ts_cache bits: dsnowball_init takes the raw `List *dictoptions`
//!     as arg 0 directly (PG_GETARG_POINTER(0)), as upstream does; no ts_cache infra
//!     is touched.

use crate::prelude::*; // Datum, c_char/c_int/c_void, palloc0/pfree/repalloc, null_mut,
                       // ereport!/errmsg!, MemoryContext/CurrentMemoryContext/MemoryContextSwitchTo

use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::commands::define::defGetString;
use crate::mb::mbutils::{
    pg_any_to_server, pg_server_to_any, GetDatabaseEncoding, GetDatabaseEncodingName,
};
use crate::mb::wchar::{PG_LATIN1, PG_SQL_ASCII, PG_UTF8};
use crate::snowball::stem_ISO_8859_1_danish::{
    danish_ISO_8859_1_close_env, danish_ISO_8859_1_create_env, danish_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_dutch::{
    dutch_ISO_8859_1_close_env, dutch_ISO_8859_1_create_env, dutch_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_finnish::{
    finnish_ISO_8859_1_close_env, finnish_ISO_8859_1_create_env, finnish_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_german::{
    german_ISO_8859_1_close_env, german_ISO_8859_1_create_env, german_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_indonesian::{
    indonesian_ISO_8859_1_close_env, indonesian_ISO_8859_1_create_env, indonesian_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_irish::{
    irish_ISO_8859_1_close_env, irish_ISO_8859_1_create_env, irish_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_norwegian::{
    norwegian_ISO_8859_1_close_env, norwegian_ISO_8859_1_create_env, norwegian_ISO_8859_1_stem,
};
use crate::snowball::stem_ISO_8859_1_swedish::{
    swedish_ISO_8859_1_close_env, swedish_ISO_8859_1_create_env, swedish_ISO_8859_1_stem,
};
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lfirst, List};
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::snowball::api::{symbol, SN_env, SN_set_current};
use crate::tsearch::dict_simple::TSLexeme;
use crate::tsearch::ts_locale::lowerstr_with_len;
use crate::tsearch::ts_utils::{readstoplist, searchstoplist, StopList, WordOpFn};
use crate::utils::fmgr::FunctionCallInfo;
// PG_GETARG_*!/PG_RETURN_*! are #[macro_export] macro_rules! at the crate root; a
// glob import of utils::fmgr does NOT bring them in, so name them here.
use crate::{
    current_cell, foreach, PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_POINTER,
};

// The 13 ported UTF-8 stemmers.
use crate::snowball::stem_UTF_8_armenian::{
    armenian_UTF_8_close_env, armenian_UTF_8_create_env, armenian_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_danish::{
    danish_UTF_8_close_env, danish_UTF_8_create_env, danish_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_dutch::{
    dutch_UTF_8_close_env, dutch_UTF_8_create_env, dutch_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_english::{
    english_UTF_8_close_env, english_UTF_8_create_env, english_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_finnish::{
    finnish_UTF_8_close_env, finnish_UTF_8_create_env, finnish_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_german::{
    german_UTF_8_close_env, german_UTF_8_create_env, german_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_hindi::{
    hindi_UTF_8_close_env, hindi_UTF_8_create_env, hindi_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_hungarian::{
    hungarian_UTF_8_close_env, hungarian_UTF_8_create_env, hungarian_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_indonesian::{
    indonesian_UTF_8_close_env, indonesian_UTF_8_create_env, indonesian_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_irish::{
    irish_UTF_8_close_env, irish_UTF_8_create_env, irish_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_nepali::{
    nepali_UTF_8_close_env, nepali_UTF_8_create_env, nepali_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_norwegian::{
    norwegian_UTF_8_close_env, norwegian_UTF_8_create_env, norwegian_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_portuguese::{
    portuguese_UTF_8_close_env, portuguese_UTF_8_create_env, portuguese_UTF_8_stem,
};
use crate::snowball::stem_UTF_8_swedish::{
    swedish_UTF_8_close_env, swedish_UTF_8_create_env, swedish_UTF_8_stem,
};

use core::ffi::CStr;

// Local libc binding (no libc crate per project rules).
extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// ----------------------------------------------------------------------------
// str_tolower stub (utils/formatting.c unported).
// ----------------------------------------------------------------------------

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

/// WordOpFn wrapper for readstoplist (its `wordop` arg expects
/// `unsafe fn(*const c_char, usize, Oid) -> *mut c_char`).  Bridges to the
/// str_tolower stub, narrowing the `usize` length to `c_int` as the C call does.
unsafe fn str_tolower_wordop(buff: *const c_char, nbytes: usize, collid: Oid) -> *mut c_char {
    str_tolower(buff, nbytes as c_int, collid)
}

// ----------------------------------------------------------------------------
// Stemmer module table (ts_public.h `stemmer_module`).
// ----------------------------------------------------------------------------

/// Snowball stemmer create-env function pointer: `struct SN_env *(*create)(void)`.
type CreateFn = unsafe extern "C" fn() -> *mut SN_env;
/// Snowball stemmer close-env function pointer: `void (*close)(struct SN_env *)`.
type CloseFn = unsafe extern "C" fn(*mut SN_env);
/// Snowball stemmer step function pointer: `int (*stem)(struct SN_env *)`.
type StemFn = unsafe extern "C" fn(*mut SN_env) -> c_int;

/// One entry in the supported-stemmers table.  `close` is carried for fidelity
/// with the C struct (a configured DictSnowball never frees its env, mirroring
/// upstream which also keeps `close` only in the table).
struct stemmer_module {
    /// Stemmer/language name (matched case-insensitively against the option).
    /// A NUL-terminated `&CStr` so its `as_ptr()` is a valid C string for
    /// `pg_strcasecmp` (which scans to the NUL).
    name: &'static CStr,
    /// Server encoding the entry is valid for (a `pg_enc` discriminant as c_int).
    enc: c_int,
    /// Allocates a fresh stemmer environment.
    create: CreateFn,
    /// Frees a stemmer environment (unused once configured -- see struct note).
    #[allow(dead_code)]
    close: CloseFn,
    /// Runs one stemming step over the env's current word.
    stem: StemFn,
}

/// List of supported stemmer modules.
///
/// SUBSET: upstream wires ~50 modules across ISO-8859-1/2, KOI8-R and UTF-8.
/// Only the 13 PORTED UTF-8 stemmers appear here.
/// TODO: add remaining stemmers once ported (the ISO-8859-1/2 and KOI8-R
/// variants, plus the other UTF-8 languages: arabic, basque, catalan, estonian,
/// french, greek, italian, lithuanian, porter, portuguese, romanian, russian,
/// serbian, spanish, tamil, turkish, yiddish, and the PG_SQL_ASCII english entry).
static STEMMER_MODULES: &[stemmer_module] = &[
    stemmer_module {
        name: c"armenian",
        enc: PG_UTF8 as c_int,
        create: armenian_UTF_8_create_env,
        close: armenian_UTF_8_close_env,
        stem: armenian_UTF_8_stem,
    },
    stemmer_module {
        name: c"danish",
        enc: PG_UTF8 as c_int,
        create: danish_UTF_8_create_env,
        close: danish_UTF_8_close_env,
        stem: danish_UTF_8_stem,
    },
    stemmer_module {
        name: c"dutch",
        enc: PG_UTF8 as c_int,
        create: dutch_UTF_8_create_env,
        close: dutch_UTF_8_close_env,
        stem: dutch_UTF_8_stem,
    },
    stemmer_module {
        name: c"english",
        enc: PG_UTF8 as c_int,
        create: english_UTF_8_create_env,
        close: english_UTF_8_close_env,
        stem: english_UTF_8_stem,
    },
    stemmer_module {
        name: c"finnish",
        enc: PG_UTF8 as c_int,
        create: finnish_UTF_8_create_env,
        close: finnish_UTF_8_close_env,
        stem: finnish_UTF_8_stem,
    },
    stemmer_module {
        name: c"german",
        enc: PG_UTF8 as c_int,
        create: german_UTF_8_create_env,
        close: german_UTF_8_close_env,
        stem: german_UTF_8_stem,
    },
    stemmer_module {
        name: c"hindi",
        enc: PG_UTF8 as c_int,
        create: hindi_UTF_8_create_env,
        close: hindi_UTF_8_close_env,
        stem: hindi_UTF_8_stem,
    },
    stemmer_module {
        name: c"hungarian",
        enc: PG_UTF8 as c_int,
        create: hungarian_UTF_8_create_env,
        close: hungarian_UTF_8_close_env,
        stem: hungarian_UTF_8_stem,
    },
    stemmer_module {
        name: c"indonesian",
        enc: PG_UTF8 as c_int,
        create: indonesian_UTF_8_create_env,
        close: indonesian_UTF_8_close_env,
        stem: indonesian_UTF_8_stem,
    },
    stemmer_module {
        name: c"irish",
        enc: PG_UTF8 as c_int,
        create: irish_UTF_8_create_env,
        close: irish_UTF_8_close_env,
        stem: irish_UTF_8_stem,
    },
    stemmer_module {
        name: c"nepali",
        enc: PG_UTF8 as c_int,
        create: nepali_UTF_8_create_env,
        close: nepali_UTF_8_close_env,
        stem: nepali_UTF_8_stem,
    },
    stemmer_module {
        name: c"danish",
        enc: PG_LATIN1 as c_int,
        create: danish_ISO_8859_1_create_env,
        close: danish_ISO_8859_1_close_env,
        stem: danish_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"dutch",
        enc: PG_LATIN1 as c_int,
        create: dutch_ISO_8859_1_create_env,
        close: dutch_ISO_8859_1_close_env,
        stem: dutch_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"finnish",
        enc: PG_LATIN1 as c_int,
        create: finnish_ISO_8859_1_create_env,
        close: finnish_ISO_8859_1_close_env,
        stem: finnish_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"german",
        enc: PG_LATIN1 as c_int,
        create: german_ISO_8859_1_create_env,
        close: german_ISO_8859_1_close_env,
        stem: german_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"indonesian",
        enc: PG_LATIN1 as c_int,
        create: indonesian_ISO_8859_1_create_env,
        close: indonesian_ISO_8859_1_close_env,
        stem: indonesian_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"irish",
        enc: PG_LATIN1 as c_int,
        create: irish_ISO_8859_1_create_env,
        close: irish_ISO_8859_1_close_env,
        stem: irish_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"norwegian",
        enc: PG_LATIN1 as c_int,
        create: norwegian_ISO_8859_1_create_env,
        close: norwegian_ISO_8859_1_close_env,
        stem: norwegian_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"swedish",
        enc: PG_LATIN1 as c_int,
        create: swedish_ISO_8859_1_create_env,
        close: swedish_ISO_8859_1_close_env,
        stem: swedish_ISO_8859_1_stem,
    },
    stemmer_module {
        name: c"norwegian",
        enc: PG_UTF8 as c_int,
        create: norwegian_UTF_8_create_env,
        close: norwegian_UTF_8_close_env,
        stem: norwegian_UTF_8_stem,
    },
    stemmer_module {
        name: c"portuguese",
        enc: PG_UTF8 as c_int,
        create: portuguese_UTF_8_create_env,
        close: portuguese_UTF_8_close_env,
        stem: portuguese_UTF_8_stem,
    },
    stemmer_module {
        name: c"swedish",
        enc: PG_UTF8 as c_int,
        create: swedish_UTF_8_create_env,
        close: swedish_UTF_8_close_env,
        stem: swedish_UTF_8_stem,
    },
    // TODO: add remaining stemmers once ported.
];

// ----------------------------------------------------------------------------
// DictSnowball state.
// ----------------------------------------------------------------------------

/// Internal state of a configured Snowball dictionary (ts_public.h `DictSnowball`).
#[repr(C)]
pub struct DictSnowball {
    /// The stemmer environment allocated by the selected module's create fn.
    pub z: *mut SN_env,
    /// Stopwords to drop before stemming.
    pub stoplist: StopList,
    /// Whether the token must be recoded to UTF-8 before/after the stem call
    /// (true when the chosen stemmer is UTF-8 but the server encoding is not).
    pub needrecode: bool,
    /// The selected stemmer step function (NULL until a Language is located).
    pub stem: Option<StemFn>,
    /// Snowball keeps alloced memory between calls, so the stem runs in this
    /// private (init-time / long-lived) context.  We remember CurrentMemoryContext.
    pub dictCtx: MemoryContext,
}

// ----------------------------------------------------------------------------
// Stemmer selection.
// ----------------------------------------------------------------------------

/// Find and instantiate the stemmer module for `lang` under the current server
/// encoding, populating `d->stem`, `d->z`, `d->needrecode`.  Mirrors C
/// `locate_stem_module`; ereport(ERROR) -- and thus panics -- when none matches.
///
/// # Safety
/// `d` must be a valid, writable DictSnowball; `lang` a valid NUL-terminated C string.
unsafe fn locate_stem_module(d: *mut DictSnowball, lang: *const c_char) {
    // First, try an exact encoding match.  A PG_SQL_ASCII stemmer works for any
    // server encoding.  (Only UTF-8 entries are currently in the table.)
    for m in STEMMER_MODULES {
        if (m.enc == PG_SQL_ASCII as c_int || m.enc == GetDatabaseEncoding())
            && pg_strcasecmp(m.name.as_ptr(), lang) == 0
        {
            (*d).stem = Some(m.stem);
            (*d).z = (m.create)();
            (*d).needrecode = false;
            return;
        }
    }

    // Second, try a UTF-8 stemmer for the language (recoding at lexize time).
    for m in STEMMER_MODULES {
        if m.enc == PG_UTF8 as c_int && pg_strcasecmp(m.name.as_ptr(), lang) == 0 {
            (*d).stem = Some(m.stem);
            (*d).z = (m.create)();
            (*d).needrecode = true;
            return;
        }
    }

    ereport!(
        ERROR,
        errmsg!(
            "no Snowball stemmer available for language \"{}\" and encoding \"{}\"",
            CStr::from_ptr(lang).to_str().unwrap_or(""),
            CStr::from_ptr(GetDatabaseEncodingName()).to_str().unwrap_or("")
        )
    );
    // ereport!(ERROR, ..) is typed () and panics; C control flow never returns
    // past it here.
    unreachable!()
}

// ----------------------------------------------------------------------------
// fmgr V1 entry points.
// ----------------------------------------------------------------------------

/// `dsnowball_init(dictoptions) -> internal` - build a DictSnowball from options.
///
/// Reads the "stopwords" (defGetString -> readstoplist) and "language"
/// (defGetString -> locate_stem_module) options off the dictoptions List;
/// rejects duplicates, unknown options, and a missing Language.
///
/// # Safety
/// fcinfo arg 0 must be a `*mut List` of DefElem options (as fmgr guarantees for
/// the dict-init calling convention).
pub unsafe fn dsnowball_init(fcinfo: FunctionCallInfo) -> Datum {
    let dictoptions = PG_GETARG_POINTER!(fcinfo, 0) as *mut List;
    let d = palloc0(core::mem::size_of::<DictSnowball>()) as *mut DictSnowball;
    let mut stoploaded = false;

    foreach!(l, dictoptions, {
        let defel = lfirst(current_cell!(l)) as *mut DefElem;

        let defname = (*defel).defname;
        let name = CStr::from_ptr(defname).to_str().unwrap_or("");

        if name == "stopwords" {
            if stoploaded {
                ereport!(ERROR, errmsg!("multiple StopWords parameters"));
            }
            readstoplist(
                defGetString(defel),
                &mut (*d).stoplist,
                Some(str_tolower_wordop as WordOpFn),
            );
            stoploaded = true;
        } else if name == "language" {
            if (*d).stem.is_some() {
                ereport!(ERROR, errmsg!("multiple Language parameters"));
            }
            locate_stem_module(d, defGetString(defel));
        } else {
            ereport!(
                ERROR,
                errmsg!("unrecognized Snowball parameter: \"{}\"", name)
            );
        }
    });

    if (*d).stem.is_none() {
        ereport!(ERROR, errmsg!("missing Language parameter"));
    }

    (*d).dictCtx = CurrentMemoryContext;

    PG_RETURN_POINTER!(d);
}

/// `dsnowball_lexize(dict, in, len) -> internal` - lowercase, recode, stem.
///
/// Returns a palloc'd 2-element TSLexeme array (NULL-terminated): the stemmed
/// lexeme on success, a passthrough lowercased copy for overlong (>1000 byte)
/// input, or an empty array (single NULL element) for an empty token / stopword.
///
/// # Safety
/// fcinfo arg 0 is a `*mut DictSnowball`, arg 1 a `*const c_char` of `len` bytes,
/// arg 2 the int32 length -- as fmgr guarantees for the dict-lexize convention.
pub unsafe fn dsnowball_lexize(fcinfo: FunctionCallInfo) -> Datum {
    let d = PG_GETARG_POINTER!(fcinfo, 0) as *mut DictSnowball;
    let r#in = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_char;
    let len = PG_GETARG_INT32!(fcinfo, 2);
    let mut txt = str_tolower(r#in, len, DEFAULT_COLLATION_OID);
    let res = palloc0(core::mem::size_of::<TSLexeme>() * 2) as *mut TSLexeme;

    // Do not pass strings exceeding 1000 bytes to the stemmer -- they are surely
    // not human-language words.  Return the lexeme lowercased but unmodified
    // (Snowball dicts must recognize all strings, so we can't reject it).
    if len > 1000 {
        (*res).lexeme = txt;
    } else if *txt == 0 || searchstoplist(&mut (*d).stoplist, txt) {
        // empty or stopword, so report as stopword
        pfree(txt as *mut c_void);
    } else {
        // Recode to utf8 if the stemmer is utf8 and doesn't match server encoding.
        if (*d).needrecode {
            let recoded = pg_server_to_any(txt, strlen(txt) as c_int, PG_UTF8 as c_int);
            if recoded != txt {
                pfree(txt as *mut c_void);
                txt = recoded;
            }
        }

        // See comment about d->dictCtx: snowball keeps memory between calls.
        let saveCtx = MemoryContextSwitchTo((*d).dictCtx);
        SN_set_current((*d).z, strlen(txt) as c_int, txt as *mut symbol);
        ((*d).stem.unwrap())((*d).z);
        MemoryContextSwitchTo(saveCtx);

        if !(*(*d).z).p.is_null() && (*(*d).z).l != 0 {
            let zl = (*(*d).z).l;
            txt = repalloc(txt as *mut c_void, (zl + 1) as Size) as *mut c_char;
            core::ptr::copy_nonoverlapping((*(*d).z).p as *const c_char, txt, zl as usize);
            *txt.add(zl as usize) = 0;
        }

        // Back-recode if needed.
        if (*d).needrecode {
            let recoded = pg_any_to_server(txt, strlen(txt) as c_int, PG_UTF8 as c_int);
            if recoded != txt {
                pfree(txt as *mut c_void);
                txt = recoded;
            }
        }

        (*res).lexeme = txt;
    }

    PG_RETURN_POINTER!(res);
}

// ----------------------------------------------------------------------------
// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // locate_stem_module with lang = "english" under the default server encoding
    // (PG_UTF8) selects the english UTF-8 entry: z becomes non-null and the
    // chosen stem fn pointer equals english_UTF_8_stem.
    #[test]
    fn locate_english_utf8_selects_english() {
        unsafe {
            let mut d: DictSnowball = core::mem::zeroed();
            let lang = b"english\0";
            locate_stem_module(&mut d, lang.as_ptr() as *const c_char);

            assert!(!d.z.is_null());
            assert!(d.stem.is_some());
            // The stem fn pointer should be english_UTF_8_stem.
            let got = d.stem.unwrap() as usize;
            let want = english_UTF_8_stem as StemFn as usize;
            assert_eq!(got, want);
            // Default server encoding is UTF-8, so needrecode is false (exact match).
            assert!(!d.needrecode);
        }
    }

    // A bogus language matches no module and triggers the ereport(ERROR) panic.
    #[test]
    #[should_panic]
    fn locate_bogus_language_errors() {
        unsafe {
            let mut d: DictSnowball = core::mem::zeroed();
            let lang = b"klingon\0";
            locate_stem_module(&mut d, lang.as_ptr() as *const c_char);
        }
    }
}
