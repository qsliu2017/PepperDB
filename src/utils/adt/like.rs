//! Translation of postgres/src/backend/utils/adt/like.c
//!                (+ postgres/src/backend/utils/adt/like_match.c merged in)
//!
//! SQL LIKE / NOT LIKE pattern matching.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: mb/pg_wchar.h -> crate::mb::mbutils / crate::mb::wchar
//! (pg_mblen_with_len / pg_database_encoding_max_length / GetDatabaseEncoding /
//! PG_UTF8), utils/fmgrprotos.h -> crate::utils::fmgr, varatt.h -> crate::varatt.
//! The VAR* macros come from crate::varatt; libc memcpy bound via extern "C".
//!
//! MATCHER MERGE NOTE
//! ------------------
//! The C source `#include`s like_match.c FOUR times, instantiating
//! SB_MatchText (single-byte), MB_MatchText (general multibyte), UTF8_MatchText
//! (UTF8 fast NextChar) and SB_IMatchText (single-byte case-insensitive).  We
//! collapse all four into ONE generic state machine:
//!
//!   unsafe fn GenericMatchText(s, slen, p, plen, locale, ci) -> c_int
//!
//! It implements the SQL LIKE state machine (`%` and `_` wildcards, `\` escape)
//! returning LIKE_TRUE(1)/LIKE_FALSE(0)/LIKE_ABORT(-1).  The CHAR step (`NextChar`)
//! advances by one multibyte character via pg_mblen_with_len, which is correct for
//! single-byte, UTF8 and general multibyte encodings alike (and for plain ASCII it
//! returns 1, matching SB/byte-wise behavior).  Byte-wise comparison between text
//! and pattern is preserved exactly as in the C (the matcher is only char-synced at
//! wildcard boundaries).  The `ci` flag selects ASCII-only case folding (see
//! GETCHAR below); the `locale` parameter is carried for signature fidelity but the
//! real pg_locale path (nondeterministic collations, ICU folding) is not ported.
//!
//! TRANSLATED:
//!  - GenericMatchText (merged SB/MB/UTF8/SB_I MatchText state machine)
//!  - wchareq (the multibyte char-equality helper)
//!  - GenericMatchTextEntry (the GenericMatchText dispatcher of like.c)
//!  - namelike/namenlike, textlike/textnlike, bytealike/byteanlike (byte-wise)
//!  - like_escape / like_escape_bytea  (self-contained ESCAPE-clause transform)
//!  - texticlike/texticnlike/nameiclike/nameicnlike  (ASCII-fold ILIKE, see TODO)
//!
//! STUBBED (deps not yet ported):
//!  - like_support: nodes/supportnodes.h (SupportRequestIndexCondition / Simplify),
//!    optimizer/optimizer.h (estimate_expression_value), the Pattern_Prefix_* /
//!    match_pattern_prefix planner machinery (selfuncs.c) - none translated.
//!  - patternsel / likesel / nlikesel / iclikesel / icnlikesel / regexeqsel ...:
//!    these live in utils/adt/selfuncs.c, not in like.c, and require the planner
//!    statistics machinery; not applicable here.
//!  - Generic_Text_IC_like proper path: the C lowers via lower()/pg_strncoll using
//!    pg_locale (utils/pg_locale.h, lower() in oracle_compat/formatting).  Neither
//!    lower() nor pg_locale is ported, so the IC entry points instead use the
//!    matcher's ASCII-only `ci` fold (correct for ASCII; see TODO at the IC fns).
//!  - nondeterministic-collation substring matching (pg_strncoll path inside the C
//!    MatchText): omitted; deterministic ASCII/byte semantics only.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
// GLOB-AMBIGUITY: both crate::varatt and crate::utils::fmgr export
// pg_detoast_datum_packed; the explicit import here wins over the two globs.
use crate::varatt::pg_detoast_datum_packed;
use crate::{
    PG_GETARG_BYTEA_PP, PG_GETARG_DATUM, PG_GETARG_NAME, PG_GETARG_TEXT_PP, PG_GET_COLLATION,
    PG_RETURN_BOOL,
};
use crate::c::{bytea, text};
use crate::mb::mbutils::{pg_database_encoding_max_length, pg_mblen_with_len};
use crate::postgres::PointerGetDatum;
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INDETERMINATE_COLLATION: c_int = 0;
const ERRCODE_INVALID_ESCAPE_SEQUENCE: c_int = 0;

pub const LIKE_TRUE: c_int = 1;
pub const LIKE_FALSE: c_int = 0;
pub const LIKE_ABORT: c_int = -1;

/*
 * pg_locale_t stand-in.  The real type (utils/pg_locale.h) carries the collation
 * provider, ctype info and the `deterministic` flag.  None of that is ported, so
 * we thread a unit-typed placeholder through the matcher for signature fidelity.
 * The C `locale == NULL` (used for bytea and IC-multibyte) maps to `None`.
 *
 * TODO(pg-port): real pg_locale_t (collation provider / nondeterministic / ICU).
 */
pub type pg_locale_t = Option<()>;

/*
 * pg_ascii_tolower: ASCII-only downcasing (mb/pg_wchar.h pg_ascii_tolower).
 * This is the only case folding we can perform without pg_locale support.
 */
#[inline]
fn pg_ascii_tolower(c: u8) -> u8 {
    if c >= b'A' && c <= b'Z' {
        c + (b'a' - b'A')
    } else {
        c
    }
}

/*
 * SB_lower_char: case-fold a single byte for single-byte case-insensitive
 * matching (like.c SB_lower_char).  The real C selects between three folders
 * based on the collation's ctype info:
 *   locale->ctype_is_c     -> pg_ascii_tolower(c)   (C/POSIX ctype)
 *   locale->is_default     -> pg_tolower(c)         (default DB collation)
 *   else                   -> tolower_l(c, lt)      (libc per-locale)
 *
 * Our pg_locale_t is the unit-typed placeholder (see above): it carries no
 * ctype/provider info, so we cannot reproduce the branch faithfully.  We fold
 * ASCII only, which is what the merged matcher (getchar_fold) also does.
 *
 * TODO(pg-port): real pg_locale ctype dispatch (ctype_is_c / is_default /
 * tolower_l(c, locale->info.lt)).  pg_tolower lives in
 * crate::port::pgstrcasecmp::pg_tolower once locale->is_default is available.
 */
#[inline]
fn SB_lower_char(c: u8, _locale: pg_locale_t) -> c_char {
    pg_ascii_tolower(c) as c_char
}

/*--------------------
 * Support routine for the matcher (like.c wchareq).  Compares given multibyte
 * streams as wide characters; if they match returns 1 otherwise 0.
 *
 * # Safety
 * `p1`/`p2` are readable for at least `p1len`/`p2len` bytes respectively.
 *--------------------
 */
#[inline]
unsafe fn wchareq(p1: *const c_char, p1len: c_int, p2: *const c_char, p2len: c_int) -> c_int {
    /* Optimization:  quickly compare the first byte. */
    if *p1 != *p2 {
        return 0;
    }

    let mut p1clen: c_int = pg_mblen_with_len(p1, p1len);
    if pg_mblen_with_len(p2, p2len) != p1clen {
        return 0;
    }

    /* They are the same length */
    let mut a = p1;
    let mut b = p2;
    while p1clen != 0 {
        p1clen -= 1;
        if *a != *b {
            return 0;
        }
        a = a.add(1);
        b = b.add(1);
    }
    1
}

/*
 * NextByte(p, plen): (p)++, (plen)--.  As a Rust helper that mutates the locals.
 */
#[inline]
unsafe fn next_byte(p: &mut *const c_char, plen: &mut c_int) {
    *p = (*p).add(1);
    *plen -= 1;
}

/*
 * NextChar(t, tlen): advance by one CHAR.  In the C this is one of NextByte
 * (single byte), the pg_mblen_with_len step (general MB), or the UTF8 fast step;
 * the merged matcher always uses the proper multibyte step, which is correct for
 * every encoding (and degenerates to one byte for ASCII / single-byte data).
 */
#[inline]
unsafe fn next_char(t: &mut *const c_char, tlen: &mut c_int) {
    let l: c_int = pg_mblen_with_len(*t, *tlen);
    *t = (*t).add(l as usize);
    *tlen -= l;
}

/*
 * GETCHAR(t): the comparison value for one byte.  When `ci` is set we fold to
 * lower case (ASCII only); otherwise the byte is used as-is.  This subsumes the
 * C MATCH_LOWER / SB_lower_char branch used only by the case-insensitive
 * instantiation.
 *
 * TODO(pg-port): true ILIKE folding needs pg_locale (SB_lower_char / pg_tolower /
 * tolower_l); here we fold ASCII A-Z only.
 */
#[inline]
fn getchar_fold(c: c_char, ci: bool) -> c_char {
    if ci {
        pg_ascii_tolower(c as u8) as c_char
    } else {
        c
    }
}

/*--------------------
 *	Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
 *
 *	LIKE_TRUE: they match
 *	LIKE_FALSE: they don't match
 *	LIKE_ABORT: not only don't they match, but the text is too short.
 *
 * If LIKE_ABORT is returned, then no suffix of the text can match the
 * pattern either, so an upper-level % scan can stop scanning now.
 *
 * This merges like_match.c's MatchText for all four instantiations.  `ci`
 * selects ASCII case folding (the MATCH_LOWER path); `locale` is carried for
 * fidelity but the nondeterministic-collation substring path is not ported.
 *
 * # Safety
 * `t`/`p` are readable for `tlen`/`plen` bytes.  Recurses on `%`.
 *--------------------
 */
pub unsafe fn GenericMatchText(
    mut t: *const c_char,
    mut tlen: c_int,
    mut p: *const c_char,
    mut plen: c_int,
    locale: pg_locale_t,
    ci: bool,
) -> c_int {
    /* Fast path for match-everything pattern */
    if plen == 1 && *p == b'%' as c_char {
        return LIKE_TRUE;
    }

    /* Since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /*
     * In this loop, we advance by char when matching wildcards (and thus on
     * recursive entry to this function we are properly char-synced). On other
     * occasions it is safe to advance by byte, as the text and pattern will be
     * in lockstep.  This allows us to perform all comparisons between the text
     * and pattern on a byte by byte basis, even for multi-byte encodings.
     */
    while tlen > 0 && plen > 0 {
        if *p == b'\\' as c_char {
            /* Next pattern byte must match literally, whatever it is */
            next_byte(&mut p, &mut plen);
            /* ... and there had better be one, per SQL standard */
            if plen <= 0 {
                let _ = errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE);
                ereport!(
                    ERROR,
                    errmsg!("LIKE pattern must not end with escape character")
                );
            }
            if getchar_fold(*p, ci) != getchar_fold(*t, ci) {
                return LIKE_FALSE;
            }
        } else if *p == b'%' as c_char {
            let firstpat: c_char;

            /*
             * % processing is essentially a search for a text position at which
             * the remainder of the text matches the remainder of the pattern,
             * using a recursive call to check each potential match.
             *
             * If there are wildcards immediately following the %, we can skip
             * over them first, using the idea that any sequence of N _'s and one
             * or more %'s is equivalent to N _'s and one % (ie, it will match any
             * sequence of at least N text characters).  In this way we will
             * always run the recursive search loop using a pattern fragment that
             * begins with a literal character-to-match, thereby not recursing
             * more than we have to.
             */
            next_byte(&mut p, &mut plen);

            while plen > 0 {
                if *p == b'%' as c_char {
                    next_byte(&mut p, &mut plen);
                } else if *p == b'_' as c_char {
                    /* If not enough text left to match the pattern, ABORT */
                    if tlen <= 0 {
                        return LIKE_ABORT;
                    }
                    next_char(&mut t, &mut tlen);
                    next_byte(&mut p, &mut plen);
                } else {
                    break; /* Reached a non-wildcard pattern char */
                }
            }

            /*
             * If we're at end of pattern, match: we have a trailing % which
             * matches any remaining text string.
             */
            if plen <= 0 {
                return LIKE_TRUE;
            }

            /*
             * Otherwise, scan for a text position at which we can match the rest
             * of the pattern.  The first remaining pattern char is known to be a
             * regular or escaped literal character, so we can compare the first
             * pattern byte to each text byte to avoid recursing more than we have
             * to.  This fact also guarantees that we don't have to consider a
             * match to the zero-length substring at the end of the text.
             */
            if *p == b'\\' as c_char {
                if plen < 2 {
                    let _ = errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE);
                    ereport!(
                        ERROR,
                        errmsg!("LIKE pattern must not end with escape character")
                    );
                }
                firstpat = getchar_fold(*p.add(1), ci);
            } else {
                firstpat = getchar_fold(*p, ci);
            }

            while tlen > 0 {
                if getchar_fold(*t, ci) == firstpat {
                    let matched = GenericMatchText(t, tlen, p, plen, locale, ci);

                    if matched != LIKE_FALSE {
                        return matched; /* TRUE or ABORT */
                    }
                }

                next_char(&mut t, &mut tlen);
            }

            /*
             * End of text with no match, so no point in trying later places to
             * start matching this pattern.
             */
            return LIKE_ABORT;
        } else if *p == b'_' as c_char {
            /* _ matches any single character, and we know there is one */
            next_char(&mut t, &mut tlen);
            next_byte(&mut p, &mut plen);
            continue;
        } else if getchar_fold(*p, ci) != getchar_fold(*t, ci) {
            /* non-wildcard pattern char fails to match text char */
            return LIKE_FALSE;
        }

        /*
         * Pattern and text match, so advance.
         *
         * It is safe to use NextByte instead of NextChar here, even for
         * multi-byte character sets, because we are not following immediately
         * after a wildcard character. If we are in the middle of a multibyte
         * character, we must already have matched at least one byte of the
         * character from both text and pattern; so we cannot get out-of-sync on
         * character boundaries.  And we know that no backend-legal encoding
         * allows ASCII characters such as '%' to appear as non-first bytes of
         * characters, so we won't mistakenly detect a new wildcard.
         */
        next_byte(&mut t, &mut tlen);
        next_byte(&mut p, &mut plen);
    }

    if tlen > 0 {
        return LIKE_FALSE; /* end of pattern, but not of text */
    }

    /*
     * End of text, but perhaps not of pattern.  Match iff the remaining pattern
     * can match a zero-length string, ie, it's zero or more %'s.
     */
    while plen > 0 && *p == b'%' as c_char {
        next_byte(&mut p, &mut plen);
    }
    if plen <= 0 {
        return LIKE_TRUE;
    }

    /*
     * End of text with no match, so no point in trying later places to start
     * matching this pattern.
     */
    LIKE_ABORT
}

/*
 * check_stack_depth (miscadmin.h): the recursion guard.  The real implementation
 * lives in tcop/postgres.c; here it is a no-op (matches other ported units).
 */
#[inline]
fn check_stack_depth() {}

/*
 * do_like_escape (merged from like_match.c) --- given a pattern and an ESCAPE
 * string, convert the pattern to use Postgres' standard backslash escape
 * convention.  Self-contained string transform.
 *
 * This single function serves both the single-byte (SB_do_like_escape) and
 * multibyte (MB_do_like_escape) instantiations: CHAREQ is rendered as a one-CHAR
 * wchareq, NextChar / CopyAdvChar advance by one multibyte character, both of
 * which degrade to single-byte behavior for SB/ASCII data.
 *
 * # Safety
 * `pat`/`esc` are valid (already-detoasted) text/bytea varlenas.
 */
unsafe fn do_like_escape(pat: *mut text, esc: *mut text) -> *mut text {
    let result: *mut text;
    let mut p: *const c_char;
    let mut e: *const c_char;
    let mut r: *mut c_char;
    let mut plen: c_int;
    let mut elen: c_int;
    let mut afterescape: bool;

    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;
    e = VARDATA_ANY(esc as *const c_char);
    elen = VARSIZE_ANY_EXHDR(esc as *const c_char) as c_int;

    /*
     * Worst-case pattern growth is 2x --- unlikely, but it's hardly worth trying
     * to calculate the size more accurately than that.
     */
    result = palloc((plen * 2 + VARHDRSZ) as Size) as *mut text;
    r = VARDATA(result as *const c_char);

    if elen == 0 {
        /*
         * No escape character is wanted.  Double any backslashes in the pattern
         * to make them act like ordinary characters.
         */
        while plen > 0 {
            if *p == b'\\' as c_char {
                *r = b'\\' as c_char;
                r = r.add(1);
            }
            /* CopyAdvChar(r, p, plen) */
            let mut l: c_int = pg_mblen_with_len(p, plen);
            plen -= l;
            while l > 0 {
                *r = *p;
                r = r.add(1);
                p = p.add(1);
                l -= 1;
            }
        }
    } else {
        /*
         * The specified escape must be only a single character.
         */
        /* NextChar(e, elen) */
        {
            let l: c_int = pg_mblen_with_len(e, elen);
            e = e.add(l as usize);
            elen -= l;
        }
        if elen != 0 {
            let _ = errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE);
            // C also: errhint("Escape string must be empty or one character.")
            ereport!(ERROR, errmsg!("invalid escape string"));
        }

        e = VARDATA_ANY(esc as *const c_char);
        elen = VARSIZE_ANY_EXHDR(esc as *const c_char) as c_int;

        /*
         * If specified escape is '\', just copy the pattern as-is.
         */
        if *e == b'\\' as c_char {
            memcpy(
                result as *mut c_void,
                pat as *const c_void,
                VARSIZE_ANY(pat as *const c_char) as usize,
            );
            return result;
        }

        /*
         * Otherwise, convert occurrences of the specified escape character to
         * '\', and double occurrences of '\' --- unless they immediately follow
         * an escape character!
         */
        afterescape = false;
        while plen > 0 {
            if wchareq(p, plen, e, elen) != 0 && !afterescape {
                *r = b'\\' as c_char;
                r = r.add(1);
                /* NextChar(p, plen) */
                let l: c_int = pg_mblen_with_len(p, plen);
                p = p.add(l as usize);
                plen -= l;
                afterescape = true;
            } else if *p == b'\\' as c_char {
                *r = b'\\' as c_char;
                r = r.add(1);
                if !afterescape {
                    *r = b'\\' as c_char;
                    r = r.add(1);
                }
                /* NextChar(p, plen) */
                let l: c_int = pg_mblen_with_len(p, plen);
                p = p.add(l as usize);
                plen -= l;
                afterescape = false;
            } else {
                /* CopyAdvChar(r, p, plen) */
                let mut l: c_int = pg_mblen_with_len(p, plen);
                plen -= l;
                while l > 0 {
                    *r = *p;
                    r = r.add(1);
                    p = p.add(1);
                    l -= 1;
                }
                afterescape = false;
            }
        }
    }

    SET_VARSIZE(
        result as *mut c_char,
        (r as isize - result as isize) as int32,
    );

    result
}

/*
 * Generic dispatcher for all cases not requiring inline case-folding (like.c's
 * GenericMatchText).  The C selects SB_/UTF8_/MB_MatchText by encoding; the
 * merged matcher handles all encodings via the multibyte CHAR step, so we call
 * it once.  `collation` validity is checked exactly as in the C.
 *
 * # Safety
 * `s`/`p` are readable for `slen`/`plen` bytes.
 */
unsafe fn GenericMatchTextEntry(
    s: *const c_char,
    slen: c_int,
    p: *const c_char,
    plen: c_int,
    collation: Oid,
) -> c_int {
    if !OidIsValid(collation) {
        /*
         * This typically means that the parser could not resolve a conflict of
         * implicit collations, so report it that way.
         */
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        // C also: errhint("Use the COLLATE clause to set the collation explicitly.")
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for LIKE")
        );
    }

    // C: locale = pg_newlocale_from_collation(collation);
    //    if (pg_database_encoding_max_length() == 1) SB_MatchText(...);
    //    else if (GetDatabaseEncoding() == PG_UTF8) UTF8_MatchText(...);
    //    else MB_MatchText(...);
    // TODO(pg-port): pg_newlocale_from_collation (utils/pg_locale.h) not ported;
    // pass a None placeholder.  The merged matcher already covers SB/UTF8/MB.
    let _ = pg_database_encoding_max_length();
    GenericMatchText(s, slen, p, plen, None, false)
}

/*
 * Generic_Text_IC_like: case-insensitive variant.
 *
 * The C lowers both pattern and text via lower()/pg_strncoll (pg_locale), or in
 * the single-byte deterministic case folds on the fly with SB_IMatchText.  Since
 * neither lower() nor pg_locale is ported, we always use the matcher's ASCII-only
 * fold (the SB_IMatchText path) which is correct for ASCII input.
 *
 * TODO(pg-port): real ILIKE folding (lower() in formatting.c + pg_locale collation
 * lowering / pg_strncoll) needed for full Unicode/locale correctness.
 *
 * # Safety
 * `str`/`pat` are valid (already-detoasted) text varlenas.
 */
unsafe fn Generic_Text_IC_like(str: *mut text, pat: *mut text, collation: Oid) -> c_int {
    if !OidIsValid(collation) {
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        // C also: errhint("Use the COLLATE clause to set the collation explicitly.")
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for ILIKE")
        );
    }

    let p: *const c_char = VARDATA_ANY(pat as *const c_char);
    let plen: c_int = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;
    let s: *const c_char = VARDATA_ANY(str as *const c_char);
    let slen: c_int = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;

    GenericMatchText(s, slen, p, plen, None, true)
}

/*
 *	interface routines called by the function manager
 */

pub unsafe fn namelike(fcinfo: FunctionCallInfo) -> Datum {
    let str: Name = PG_GETARG_NAME!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;
    let s: *const c_char;
    let p: *const c_char;
    let slen: c_int;
    let plen: c_int;

    s = NameStr(&*str);
    slen = strlen(s) as c_int;
    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;

    result = GenericMatchTextEntry(s, slen, p, plen, PG_GET_COLLATION!(fcinfo)) == LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn namenlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: Name = PG_GETARG_NAME!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;
    let s: *const c_char;
    let p: *const c_char;
    let slen: c_int;
    let plen: c_int;

    s = NameStr(&*str);
    slen = strlen(s) as c_int;
    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;

    result = GenericMatchTextEntry(s, slen, p, plen, PG_GET_COLLATION!(fcinfo)) != LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn textlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;
    let s: *const c_char;
    let p: *const c_char;
    let slen: c_int;
    let plen: c_int;

    s = VARDATA_ANY(str as *const c_char);
    slen = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;

    result = GenericMatchTextEntry(s, slen, p, plen, PG_GET_COLLATION!(fcinfo)) == LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn textnlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;
    let s: *const c_char;
    let p: *const c_char;
    let slen: c_int;
    let plen: c_int;

    s = VARDATA_ANY(str as *const c_char);
    slen = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;

    result = GenericMatchTextEntry(s, slen, p, plen, PG_GET_COLLATION!(fcinfo)) != LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bytealike(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let pat: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let result: bool;
    let s: *const c_char;
    let p: *const c_char;
    let slen: c_int;
    let plen: c_int;

    s = VARDATA_ANY(str as *const c_char);
    slen = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;

    /* bytea is byte-wise: locale = None (0 in C), ci = false. */
    result = GenericMatchText(s, slen, p, plen, None, false) == LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn byteanlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let pat: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let result: bool;
    let s: *const c_char;
    let p: *const c_char;
    let slen: c_int;
    let plen: c_int;

    s = VARDATA_ANY(str as *const c_char);
    slen = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
    p = VARDATA_ANY(pat as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat as *const c_char) as c_int;

    result = GenericMatchText(s, slen, p, plen, None, false) != LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

/*
 * Case-insensitive versions
 */

pub unsafe fn nameiclike(fcinfo: FunctionCallInfo) -> Datum {
    let str: Name = PG_GETARG_NAME!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;
    let strtext: *mut text;

    // C: strtext = DatumGetTextPP(DirectFunctionCall1(name_text, NameGetDatum(str)));
    // name_text (varchar.c) is ported, but to avoid a cross-file dependency we
    // build the text directly from the NUL-padded Name (identical result).
    strtext = name_to_text(str);
    result = Generic_Text_IC_like(strtext, pat, PG_GET_COLLATION!(fcinfo)) == LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn nameicnlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: Name = PG_GETARG_NAME!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;
    let strtext: *mut text;

    strtext = name_to_text(str);
    result = Generic_Text_IC_like(strtext, pat, PG_GET_COLLATION!(fcinfo)) != LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn texticlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;

    result = Generic_Text_IC_like(str, pat, PG_GET_COLLATION!(fcinfo)) == LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

pub unsafe fn texticnlike(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;

    result = Generic_Text_IC_like(str, pat, PG_GET_COLLATION!(fcinfo)) != LIKE_TRUE;

    PG_RETURN_BOOL!(result);
}

/*
 * name_to_text: build a freshly-palloc'd text from a NUL-padded Name.  This is
 * the local stand-in for DirectFunctionCall1(name_text, ...) used by the IC name
 * entry points (the C name_text strips the NAMEDATALEN padding via strlen).
 *
 * # Safety
 * `str` points to a live NameData.
 */
unsafe fn name_to_text(str: Name) -> *mut text {
    let s: *const c_char = NameStr(&*str);
    let len: c_int = strlen(s) as c_int;
    let result: *mut text = palloc((len + VARHDRSZ) as Size) as *mut text;
    SET_VARSIZE(result as *mut c_char, len + VARHDRSZ);
    core::ptr::copy_nonoverlapping(s, VARDATA(result as *const c_char), len as usize);
    result
}

/*
 * like_escape() --- given a pattern and an ESCAPE string, convert the pattern to
 * use Postgres' standard backslash escape convention.
 */
pub unsafe fn like_escape(fcinfo: FunctionCallInfo) -> Datum {
    let pat: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let esc: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: *mut text;

    // C: if (pg_database_encoding_max_length() == 1) SB_do_like_escape(...);
    //    else MB_do_like_escape(...);
    // The merged do_like_escape handles both (CHAR step degrades to a byte for SB).
    let _ = pg_database_encoding_max_length();
    result = do_like_escape(pat, esc);

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * like_escape_bytea() --- given a pattern and an ESCAPE string, convert the
 * pattern to use Postgres' standard backslash escape convention.
 */
pub unsafe fn like_escape_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let pat: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let esc: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    /* C uses SB_do_like_escape((text *) pat, (text *) esc) - byte-wise. */
    let result: *mut bytea = do_like_escape(pat as *mut text, esc as *mut text) as *mut bytea;

    return PointerGetDatum(result as *const c_void); // PG_RETURN_BYTEA_P
}

/*
 * like_support()
 *
 * Planner support function for the LIKE family.  In the C this handles
 * SupportRequestIndexCondition (turning `x LIKE 'foo%'` into a btree range scan
 * via match_pattern_prefix) and SupportRequestSimplify.
 *
 * TODO(pg-port): nodes/supportnodes.h + the Pattern_Prefix_* / match_pattern_prefix
 * planner machinery (utils/adt/like_support.c, selfuncs.c) are not translated.
 */
pub unsafe fn like_support(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("like_support: planner support (supportnodes.h / like_support.c) not yet translated")
}

#[cfg(test)]
mod tests {
    use super::*;
    // PointerGetDatum is already in scope via `use super::*` (the outer module
    // imports it from crate::postgres); only DatumGetBool is new here.
    use crate::postgres::DatumGetBool;
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::{cstring_to_text_with_len, TextDatumGetCString};
    // DirectFunctionCall1Coll / DirectFunctionCall2Coll are in scope via
    // `use super::*` (outer module does `use crate::utils::fmgr::*`).

    // The default collation used by C LIKE is DEFAULT_COLLATION_OID; the matcher's
    // ASCII path doesn't actually consult pg_locale, but GenericMatchTextEntry
    // rejects InvalidOid, so use a fixed valid Oid for the LIKE entry points.
    const C_COLLATION_OID: Oid = 950; // pg_collation.h C_COLLATION_OID

    unsafe fn mk(s: &str) -> Datum {
        let p = cstring_to_text_with_len(s.as_ptr() as *const c_char, s.len() as c_int);
        PointerGetDatum(p as *const c_void)
    }
    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn textlike_basic() {
        unsafe {
            // exact match
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("hello"), mk("hello")
            )));
            // % matches everything
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("hello"), mk("%")
            )));
            // prefix + suffix %
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("hello world"), mk("hello%")
            )));
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("hello world"), mk("%world")
            )));
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("hello world"), mk("%lo wo%")
            )));
            // _ matches exactly one char
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("abc"), mk("a_c")
            )));
            // _ does NOT match zero chars
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("ac"), mk("a_c")
            )));
            // non-match
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("hello"), mk("world")
            )));
            // backslash escapes a wildcard: literal '%'
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("50%"), mk("50\\%")
            )));
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("5012"), mk("50\\%")
            )));
        }
    }

    #[test]
    fn textnlike_is_negation() {
        unsafe {
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                textnlike, C_COLLATION_OID, mk("abc"), mk("a%")
            )));
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textnlike, C_COLLATION_OID, mk("abc"), mk("x%")
            )));
        }
    }

    #[test]
    fn texticlike_ascii_fold() {
        unsafe {
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                texticlike, C_COLLATION_OID, mk("Hello"), mk("hello")
            )));
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                texticlike, C_COLLATION_OID, mk("HELLO WORLD"), mk("%world")
            )));
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                texticlike, C_COLLATION_OID, mk("Hello"), mk("xyz")
            )));
        }
    }

    #[test]
    fn bytealike_bytewise() {
        unsafe {
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                bytealike, InvalidOid, mk("abc"), mk("a%c")
            )));
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                bytealike, InvalidOid, mk("abc"), mk("A%C")
            )));
            // byteanlike negates
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                byteanlike, InvalidOid, mk("abc"), mk("A%C")
            )));
        }
    }

    #[test]
    fn like_escape_transform() {
        unsafe {
            // ESCAPE '#': '#' becomes '\', so "50#%" -> "50\%" (literal percent).
            let out = DirectFunctionCall2Coll(like_escape, InvalidOid, mk("50#%"), mk("#"));
            let s = TextDatumGetCString(out);
            assert!(cstr_eq(s, "50\\%"));

            // The transformed pattern matched against text via textlike: 50% literal.
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                textlike, C_COLLATION_OID, mk("50%"), out
            )));

            // Empty escape: backslashes get doubled.
            let out2 = DirectFunctionCall2Coll(like_escape, InvalidOid, mk("a\\b"), mk(""));
            let s2 = TextDatumGetCString(out2);
            assert!(cstr_eq(s2, "a\\\\b"));

            // ESCAPE '\': pattern copied as-is.
            let out3 = DirectFunctionCall2Coll(like_escape, InvalidOid, mk("a\\%b"), mk("\\"));
            let s3 = TextDatumGetCString(out3);
            assert!(cstr_eq(s3, "a\\%b"));
        }
    }

    #[test]
    #[should_panic]
    fn pattern_ending_in_escape_errors() {
        unsafe {
            // Trailing backslash with text remaining must ereport(ERROR).
            DirectFunctionCall2Coll(textlike, C_COLLATION_OID, mk("ab"), mk("a\\"));
        }
    }

    #[test]
    #[should_panic]
    fn like_escape_multichar_escape_errors() {
        unsafe {
            DirectFunctionCall2Coll(like_escape, InvalidOid, mk("abc"), mk("xy"));
        }
    }
}
