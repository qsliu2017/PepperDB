//! Translation of postgres/src/backend/utils/adt/tsvector_parser.c
//!
//! Parser for tsvector.  This is the shared input lexer for both `tsvector`
//! (tsvectorin) and `tsquery` (tsquery.c); the boolean flags select the
//! per-caller behavior.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/tsvector_parser.c
//!
//! ---------------------------------------------------------------------------
//! Includes mapped:
//!   #include "postgres.h"            -> use crate::prelude::*;
//!   #include "tsearch/ts_locale.h"   -> t_iseq / ts_copychar_cstr modeled inline
//!                                       (see below); pg_mblen_cstr via crate::mb::mbutils.
//!   #include "tsearch/ts_utils.h"    -> P_TSV_* flag consts + ISOPERATOR + the
//!                                       TSVectorParseState typedef are MERGED here.
//!
//! MERGED from headers (so this file is self-contained and consumable by the
//! not-yet-ported tsvector.c / tsquery.c):
//!   - ts_utils.h: P_TSV_OPR_IS_DELIM / P_TSV_IS_TSQUERY / P_TSV_IS_WEB flag
//!     bits, the ISOPERATOR predicate, and the opaque TSVectorParseStateData /
//!     TSVectorParseState typedef (whose definition genuinely lives in this .c).
//!   - ts_type.h: WordEntryPos (a `uint16` with weight:2/pos:14 bitfields), plus
//!     its WEP_* accessor macros and the LIMITPOS/MAXENTRYPOS limits, modeled as
//!     accessor functions over a plain u16.  The TSVector / WordEntry varlena
//!     definitions also live in ts_type.h but are NOT needed by the lexer, so
//!     they are described in comments only (the consuming tsvector.c will own
//!     them once it is ported).
//!
//! ts_locale.h helpers:
//!   - t_iseq(x, c): compares *x (as unsigned char) to a plain-ASCII byte.  The C
//!     macro is `(*(const unsigned char *)(x) == (unsigned char)(c))`; modeled as
//!     `t_iseq()` below.
//!   - ts_copychar_cstr(dest, src): memcpy of one multibyte char (length =
//!     pg_mblen_cstr(src)), returning the byte length.  Modeled below using
//!     crate::mb::mbutils::pg_mblen_cstr + ptr::copy_nonoverlapping.

use crate::prelude::*;
use crate::mb::mbutils::{pg_database_encoding_max_length, pg_mblen_cstr};
use crate::nodes::nodes::Node;
use core::ffi::{c_char, c_int};

// <ctype.h>: isspace / isdigit, used exactly as the C does, e.g.
// `isspace((unsigned char) *state->prsbuf)`.  Bound via extern "C" (the
// scansup.rs / numutils.rs convention).
extern "C" {
    fn isspace(ch: c_int) -> c_int;
    fn isdigit(ch: c_int) -> c_int;
    // <stdlib.h> atoi, used for parsing the position number (atoi(state->prsbuf)).
    fn atoi(s: *const c_char) -> c_int;
}

// TODO(pg-port): ERRCODE_SYNTAX_ERROR lives in the generated utils/errcodes.h,
// not yet translated.  errcode() is a no-op shim (it ignores the value), so the
// concrete number is immaterial; 0 is the placeholder used by the other adt files.
const ERRCODE_SYNTAX_ERROR: c_int = 0;

// ---------------------------------------------------------------------------
// MERGED from tsearch/ts_utils.h: parser flag bits.
// ---------------------------------------------------------------------------

/* flag bits that can be passed to init_tsvector_parser: */
pub const P_TSV_OPR_IS_DELIM: c_int = 1 << 0;
pub const P_TSV_IS_TSQUERY: c_int = 1 << 1;
pub const P_TSV_IS_WEB: c_int = 1 << 2;

/// MERGED from tsearch/ts_utils.h: the phrase operator begins with '<'.
///
/// C macro:
/// ```c
/// #define ISOPERATOR(x) (*(x)=='!'||*(x)=='&'||*(x)=='|'||*(x)=='('||*(x)==')'||*(x)=='<')
/// ```
///
/// # Safety
/// `x` must point to at least one readable byte.
#[inline]
pub unsafe fn ISOPERATOR(x: *const c_char) -> bool {
    let c = *x as u8;
    c == b'!' || c == b'&' || c == b'|' || c == b'(' || c == b')' || c == b'<'
}

// ---------------------------------------------------------------------------
// MERGED from tsearch/ts_type.h: WordEntryPos and its accessors.
//
// In C:
//   /* Equivalent to a uint16 with weight:2, pos:14 bitfields */
//   typedef uint16 WordEntryPos;
//   #define WEP_GETWEIGHT(x) ( (x) >> 14 )
//   #define WEP_GETPOS(x)    ( (x) & 0x3fff )
//   #define WEP_SETWEIGHT(x,v) ( (x) = ((v) << 14) | ((x) & 0x3fff) )
//   #define WEP_SETPOS(x,v)    ( (x) = ((x) & 0xc000) | ((v) & 0x3fff) )
//   #define MAXENTRYPOS (1<<14)
//   #define LIMITPOS(x) ( ((x) >= MAXENTRYPOS) ? (MAXENTRYPOS-1) : (x) )
// ---------------------------------------------------------------------------

/// `typedef uint16 WordEntryPos;` - a packed weight(2)/pos(14) value.
pub type WordEntryPos = uint16;

pub const MAXENTRYPOS: c_int = 1 << 14;
pub const MAXNUMPOS: c_int = 256;

/// `WEP_GETWEIGHT(x)` -> high 2 bits.
#[inline]
pub fn WEP_GETWEIGHT(x: WordEntryPos) -> uint16 {
    x >> 14
}

/// `WEP_GETPOS(x)` -> low 14 bits.
#[inline]
pub fn WEP_GETPOS(x: WordEntryPos) -> uint16 {
    x & 0x3fff
}

/// `WEP_SETWEIGHT(x, v)` -> set the high 2 bits, preserving the low 14.
#[inline]
pub fn WEP_SETWEIGHT(x: &mut WordEntryPos, v: uint16) {
    *x = (v << 14) | (*x & 0x3fff);
}

/// `WEP_SETPOS(x, v)` -> set the low 14 bits, preserving the high 2.
#[inline]
pub fn WEP_SETPOS(x: &mut WordEntryPos, v: uint16) {
    *x = (*x & 0xc000) | (v & 0x3fff);
}

/// `LIMITPOS(x)` - clamp a position into the 14-bit field.
#[inline]
pub fn LIMITPOS(x: c_int) -> c_int {
    if x >= MAXENTRYPOS {
        MAXENTRYPOS - 1
    } else {
        x
    }
}

// ---------------------------------------------------------------------------
// ts_locale.h helpers needed by the lexer.
// ---------------------------------------------------------------------------

/// `t_iseq(x, c)` - the second argument must be a plain-ASCII character.
///
/// C macro: `(*(const unsigned char *)(x) == (unsigned char)(c))`.
///
/// # Safety
/// `x` must point to at least one readable byte.
#[inline]
unsafe fn t_iseq(x: *const c_char, c: u8) -> bool {
    (*x as u8) == c
}

/// `ts_copychar_cstr(dest, src)` - copy one multibyte character (of byte length
/// `pg_mblen_cstr(src)`) and return the number of bytes copied.
///
/// # Safety
/// `src` is a valid NUL-terminated string; `dest` must have room for one
/// multibyte character (the RESIZEPRSBUF logic in the caller guarantees this).
#[inline]
unsafe fn ts_copychar_cstr(dest: *mut c_char, src: *const c_char) -> c_int {
    let length = pg_mblen_cstr(src);
    core::ptr::copy_nonoverlapping(src, dest, length as usize);
    length
}

// ---------------------------------------------------------------------------
// Private state of tsvector parser.
//
// Note that tsquery also uses this code to parse its input, hence the boolean
// flags.  The oprisdelim and is_tsquery flags are both true or both false in
// current usage, but we keep them separate for clarity.
//
// If oprisdelim is set, the following characters are treated as delimiters
// (in addition to whitespace): ! | & ( )
//
// is_tsquery affects *only* the content of error messages.
//
// is_web can be true to further modify tsquery parsing.
//
// If escontext is an ErrorSaveContext node, then soft errors can be captured
// there rather than being thrown.
// ---------------------------------------------------------------------------
pub struct TSVectorParseStateData {
    pub prsbuf: *mut c_char,   /* next input character */
    pub bufstart: *mut c_char, /* whole string (used only for errors) */
    pub word: *mut c_char,     /* buffer to hold the current word */
    pub len: c_int,            /* size in bytes allocated for 'word' */
    pub eml: c_int,            /* max bytes per character */
    pub oprisdelim: bool,      /* treat ! | * ( ) as delimiters? */
    pub is_tsquery: bool,      /* say "tsquery" not "tsvector" in errors? */
    pub is_web: bool,          /* we're in websearch_to_tsquery() */
    pub escontext: *mut Node,  /* for soft error reporting */
}

/// `typedef struct TSVectorParseStateData *TSVectorParseState;` (ts_utils.h).
pub type TSVectorParseState = *mut TSVectorParseStateData;

/*
 * Initializes a parser state object for the given input string.
 * A bitmask of flags (see ts_utils.h) and an error context object
 * can be provided as well.
 *
 * # Safety
 * `input` is a valid NUL-terminated string that outlives the returned state.
 * `escontext` is NULL or a valid Node pointer.
 */
pub unsafe fn init_tsvector_parser(
    input: *mut c_char,
    flags: c_int,
    escontext: *mut Node,
) -> TSVectorParseState {
    let state: TSVectorParseState =
        palloc(core::mem::size_of::<TSVectorParseStateData>() as Size) as TSVectorParseState;
    (*state).prsbuf = input;
    (*state).bufstart = input;
    (*state).len = 32;
    (*state).word = palloc((*state).len as Size) as *mut c_char;
    (*state).eml = pg_database_encoding_max_length();
    (*state).oprisdelim = (flags & P_TSV_OPR_IS_DELIM) != 0;
    (*state).is_tsquery = (flags & P_TSV_IS_TSQUERY) != 0;
    (*state).is_web = (flags & P_TSV_IS_WEB) != 0;
    (*state).escontext = escontext;

    state
}

/*
 * Reinitializes parser to parse 'input', instead of previous input.
 *
 * Note that bufstart (the string reported in errors) is not changed.
 *
 * # Safety
 * `state` is a valid parser state; `input` is a valid NUL-terminated string.
 */
pub unsafe fn reset_tsvector_parser(state: TSVectorParseState, input: *mut c_char) {
    (*state).prsbuf = input;
}

/*
 * Shuts down a tsvector parser.
 *
 * # Safety
 * `state` is a valid parser state created by init_tsvector_parser().
 */
pub unsafe fn close_tsvector_parser(state: TSVectorParseState) {
    pfree((*state).word as *mut c_void);
    pfree(state as *mut c_void);
}

/* State codes used in gettoken_tsvector */
const WAITWORD: c_int = 1;
const WAITENDWORD: c_int = 2;
const WAITNEXTCHAR: c_int = 3;
const WAITENDCMPLX: c_int = 4;
const WAITPOSINFO: c_int = 5;
const INPOSINFO: c_int = 6;
const WAITPOSDELIM: c_int = 7;
const WAITCHARCMPLX: c_int = 8;

/*
 * #define PRSSYNTAXERROR return prssyntaxerror(state)
 *
 * In C, prssyntaxerror() reports a soft error via errsave(escontext, ...) and
 * returns false.  Soft-error contexts (ErrorSaveContext) are not yet ported, so
 * the ereport! shim raises a hard ERROR (which diverges by panicking).  The
 * `false` return mirrors the C convenience value the caller path expects.
 *
 * # Safety
 * `state` is a valid parser state.
 */
unsafe fn prssyntaxerror(state: TSVectorParseState) -> bool {
    let _ = errcode(ERRCODE_SYNTAX_ERROR);
    if (*state).is_tsquery {
        ereport!(
            ERROR,
            errmsg!("syntax error in tsquery: \"{}\"", cstr((*state).bufstart))
        );
    } else {
        ereport!(
            ERROR,
            errmsg!("syntax error in tsvector: \"{}\"", cstr((*state).bufstart))
        );
    }
    /* In soft error situation, return false as convenience for caller */
    false
}

/*
 * Get next token from string being parsed. Returns true if successful,
 * false if end of input string is reached or soft error.
 *
 * On success, these output parameters are filled in:
 *
 * *strval		pointer to token
 * *lenval		length of *strval
 * *pos_ptr		pointer to a palloc'd array of positions and weights
 *				associated with the token. If the caller is not interested
 *				in the information, NULL can be supplied. Otherwise
 *				the caller is responsible for pfreeing the array.
 * *poslen		number of elements in *pos_ptr
 * *endptr		scan resumption point
 *
 * Pass NULL for any unwanted output parameters.
 *
 * If state->escontext is an ErrorSaveContext, then caller must check
 * SOFT_ERROR_OCCURRED() to determine whether a "false" result means
 * error or normal end-of-string.  (Soft errors are not yet ported here, so a
 * syntax error currently raises a hard ERROR rather than returning false.)
 *
 * # Safety
 * `state` is a valid parser state; the output pointers, where non-NULL, are
 * writable.
 */
pub unsafe fn gettoken_tsvector(
    state: TSVectorParseState,
    strval: *mut *mut c_char,
    lenval: *mut c_int,
    pos_ptr: *mut *mut WordEntryPos,
    poslen: *mut c_int,
    endptr: *mut *mut c_char,
) -> bool {
    let mut oldstate: c_int = 0;
    let mut curpos: *mut c_char = (*state).word;
    let mut statecode: c_int = WAITWORD;

    /*
     * pos is for collecting the comma delimited list of positions followed by
     * the actual token.
     */
    let mut pos: *mut WordEntryPos = null_mut();
    let mut npos: c_int = 0; /* elements of pos used */
    let mut posalen: c_int = 0; /* allocated size of pos */

    // RESIZEPRSBUF: increase the size of 'word' if needed to hold one more
    // character.  As a C `do { ... } while (0)` macro it mutates `curpos`; here
    // it is an inline closure-free block re-implemented at each use site is
    // verbose, so we use a small macro that captures `state` and `curpos`.
    macro_rules! RESIZEPRSBUF {
        () => {{
            let clen = curpos as usize - (*state).word as usize;
            if clen as c_int + (*state).eml >= (*state).len {
                (*state).len *= 2;
                (*state).word = repalloc((*state).word as *mut c_void, (*state).len as Size)
                    as *mut c_char;
                curpos = (*state).word.add(clen);
            }
        }};
    }

    // RETURN_TOKEN: fills gettoken_tsvector's output parameters and returns true.
    macro_rules! RETURN_TOKEN {
        () => {{
            if !pos_ptr.is_null() {
                *pos_ptr = pos;
                *poslen = npos;
            } else if !pos.is_null() {
                pfree(pos as *mut c_void);
            }

            if !strval.is_null() {
                *strval = (*state).word;
            }
            if !lenval.is_null() {
                *lenval = (curpos as usize - (*state).word as usize) as c_int;
            }
            if !endptr.is_null() {
                *endptr = (*state).prsbuf;
            }
            return true;
        }};
    }

    // PRSSYNTAXERROR: report a (soft) syntax error and return false.
    macro_rules! PRSSYNTAXERROR {
        () => {
            return prssyntaxerror(state)
        };
    }

    loop {
        if statecode == WAITWORD {
            if *(*state).prsbuf == 0 {
                return false;
            } else if !(*state).is_web && t_iseq((*state).prsbuf, b'\'') {
                statecode = WAITENDCMPLX;
            } else if !(*state).is_web && t_iseq((*state).prsbuf, b'\\') {
                statecode = WAITNEXTCHAR;
                oldstate = WAITENDWORD;
            } else if ((*state).oprisdelim && ISOPERATOR((*state).prsbuf))
                || ((*state).is_web && t_iseq((*state).prsbuf, b'"'))
            {
                PRSSYNTAXERROR!();
            } else if isspace(*(*state).prsbuf as u8 as c_int) == 0 {
                curpos = curpos.add(ts_copychar_cstr(curpos, (*state).prsbuf) as usize);
                statecode = WAITENDWORD;
            }
        } else if statecode == WAITNEXTCHAR {
            if *(*state).prsbuf == 0 {
                // ereturn(state->escontext, false, ...): soft-error context not yet
                // ported, so this raises a hard ERROR (ereport! diverges).
                let _ = errcode(ERRCODE_SYNTAX_ERROR);
                ereport!(
                    ERROR,
                    errmsg!(
                        "there is no escaped character: \"{}\"",
                        cstr((*state).bufstart)
                    )
                );
                #[allow(unreachable_code)]
                {
                    return false;
                }
            } else {
                RESIZEPRSBUF!();
                curpos = curpos.add(ts_copychar_cstr(curpos, (*state).prsbuf) as usize);
                Assert!(oldstate != 0);
                statecode = oldstate;
            }
        } else if statecode == WAITENDWORD {
            if !(*state).is_web && t_iseq((*state).prsbuf, b'\\') {
                statecode = WAITNEXTCHAR;
                oldstate = WAITENDWORD;
            } else if isspace(*(*state).prsbuf as u8 as c_int) != 0
                || *(*state).prsbuf == 0
                || ((*state).oprisdelim && ISOPERATOR((*state).prsbuf))
                || ((*state).is_web && t_iseq((*state).prsbuf, b'"'))
            {
                RESIZEPRSBUF!();
                if curpos == (*state).word {
                    PRSSYNTAXERROR!();
                }
                *curpos = 0;
                RETURN_TOKEN!();
            } else if t_iseq((*state).prsbuf, b':') {
                if curpos == (*state).word {
                    PRSSYNTAXERROR!();
                }
                *curpos = 0;
                if (*state).oprisdelim {
                    RETURN_TOKEN!();
                } else {
                    statecode = INPOSINFO;
                }
            } else {
                RESIZEPRSBUF!();
                curpos = curpos.add(ts_copychar_cstr(curpos, (*state).prsbuf) as usize);
            }
        } else if statecode == WAITENDCMPLX {
            if !(*state).is_web && t_iseq((*state).prsbuf, b'\'') {
                statecode = WAITCHARCMPLX;
            } else if !(*state).is_web && t_iseq((*state).prsbuf, b'\\') {
                statecode = WAITNEXTCHAR;
                oldstate = WAITENDCMPLX;
            } else if *(*state).prsbuf == 0 {
                PRSSYNTAXERROR!();
            } else {
                RESIZEPRSBUF!();
                curpos = curpos.add(ts_copychar_cstr(curpos, (*state).prsbuf) as usize);
            }
        } else if statecode == WAITCHARCMPLX {
            if !(*state).is_web && t_iseq((*state).prsbuf, b'\'') {
                RESIZEPRSBUF!();
                curpos = curpos.add(ts_copychar_cstr(curpos, (*state).prsbuf) as usize);
                statecode = WAITENDCMPLX;
            } else {
                RESIZEPRSBUF!();
                *curpos = 0;
                if curpos == (*state).word {
                    PRSSYNTAXERROR!();
                }
                if (*state).oprisdelim {
                    /* state->prsbuf+=pg_mblen_cstr(state->prsbuf); */
                    RETURN_TOKEN!();
                } else {
                    statecode = WAITPOSINFO;
                }
                continue; /* recheck current character */
            }
        } else if statecode == WAITPOSINFO {
            if t_iseq((*state).prsbuf, b':') {
                statecode = INPOSINFO;
            } else {
                RETURN_TOKEN!();
            }
        } else if statecode == INPOSINFO {
            if isdigit(*(*state).prsbuf as u8 as c_int) != 0 {
                if posalen == 0 {
                    posalen = 4;
                    pos = palloc(
                        core::mem::size_of::<WordEntryPos>() * posalen as usize,
                    ) as *mut WordEntryPos;
                    npos = 0;
                } else if npos + 1 >= posalen {
                    posalen *= 2;
                    pos = repalloc(
                        pos as *mut c_void,
                        core::mem::size_of::<WordEntryPos>() * posalen as usize,
                    ) as *mut WordEntryPos;
                }
                npos += 1;
                WEP_SETPOS(
                    &mut *pos.add((npos - 1) as usize),
                    LIMITPOS(atoi((*state).prsbuf)) as uint16,
                );
                /* we cannot get here in tsquery, so no need for 2 errmsgs */
                if WEP_GETPOS(*pos.add((npos - 1) as usize)) == 0 {
                    // ereturn(state->escontext, false, ...): hard ERROR (soft not ported).
                    let _ = errcode(ERRCODE_SYNTAX_ERROR);
                    ereport!(
                        ERROR,
                        errmsg!(
                            "wrong position info in tsvector: \"{}\"",
                            cstr((*state).bufstart)
                        )
                    );
                    #[allow(unreachable_code)]
                    {
                        return false;
                    }
                }
                WEP_SETWEIGHT(&mut *pos.add((npos - 1) as usize), 0);
                statecode = WAITPOSDELIM;
            } else {
                PRSSYNTAXERROR!();
            }
        } else if statecode == WAITPOSDELIM {
            if t_iseq((*state).prsbuf, b',') {
                statecode = INPOSINFO;
            } else if t_iseq((*state).prsbuf, b'a')
                || t_iseq((*state).prsbuf, b'A')
                || t_iseq((*state).prsbuf, b'*')
            {
                if WEP_GETWEIGHT(*pos.add((npos - 1) as usize)) != 0 {
                    PRSSYNTAXERROR!();
                }
                WEP_SETWEIGHT(&mut *pos.add((npos - 1) as usize), 3);
            } else if t_iseq((*state).prsbuf, b'b') || t_iseq((*state).prsbuf, b'B') {
                if WEP_GETWEIGHT(*pos.add((npos - 1) as usize)) != 0 {
                    PRSSYNTAXERROR!();
                }
                WEP_SETWEIGHT(&mut *pos.add((npos - 1) as usize), 2);
            } else if t_iseq((*state).prsbuf, b'c') || t_iseq((*state).prsbuf, b'C') {
                if WEP_GETWEIGHT(*pos.add((npos - 1) as usize)) != 0 {
                    PRSSYNTAXERROR!();
                }
                WEP_SETWEIGHT(&mut *pos.add((npos - 1) as usize), 1);
            } else if t_iseq((*state).prsbuf, b'd') || t_iseq((*state).prsbuf, b'D') {
                if WEP_GETWEIGHT(*pos.add((npos - 1) as usize)) != 0 {
                    PRSSYNTAXERROR!();
                }
                WEP_SETWEIGHT(&mut *pos.add((npos - 1) as usize), 0);
            } else if isspace(*(*state).prsbuf as u8 as c_int) != 0 || *(*state).prsbuf == 0 {
                RETURN_TOKEN!();
            } else if isdigit(*(*state).prsbuf as u8 as c_int) == 0 {
                PRSSYNTAXERROR!();
            }
        } else {
            /* internal error */
            elog!(
                ERROR,
                "unrecognized state in gettoken_tsvector: {}",
                statecode
            );
        }

        /* get next char */
        (*state).prsbuf = (*state).prsbuf.add(pg_mblen_cstr((*state).prsbuf) as usize);
    }
}

/// Helper: render a NUL-terminated C string into a Rust String for errmsg!
/// (mirrors the `%s` substitution of the C error messages).  Lossy UTF-8 so
/// arbitrary DB-encoding bytes never panic the formatter.
///
/// # Safety
/// `s` is a valid NUL-terminated C string.
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

    // Drive the lexer over an owned, NUL-terminated byte buffer and collect the
    // (lexeme, [(pos, weight)...]) tokens.  This exercises the real
    // init/reset/gettoken/close path (the type round-trip lives in tsvector.c,
    // which is split into a separate unit).
    unsafe fn lex(input: &str, flags: c_int) -> Vec<(std::string::String, Vec<(u16, u16)>)> {
        let mut buf: Vec<u8> = input.as_bytes().to_vec();
        buf.push(0);
        let state = init_tsvector_parser(buf.as_mut_ptr() as *mut c_char, flags, null_mut());

        let mut out = Vec::new();
        loop {
            let mut strval: *mut c_char = null_mut();
            let mut lenval: c_int = 0;
            let mut pos_ptr: *mut WordEntryPos = null_mut();
            let mut poslen: c_int = 0;
            let ok = gettoken_tsvector(
                state,
                &mut strval,
                &mut lenval,
                &mut pos_ptr,
                &mut poslen,
                null_mut(),
            );
            if !ok {
                break;
            }
            let lexeme = {
                let bytes = core::slice::from_raw_parts(strval as *const u8, lenval as usize);
                std::string::String::from_utf8_lossy(bytes).into_owned()
            };
            let mut positions = Vec::new();
            for i in 0..poslen as usize {
                let p = *pos_ptr.add(i);
                positions.push((WEP_GETPOS(p), WEP_GETWEIGHT(p)));
            }
            if !pos_ptr.is_null() {
                pfree(pos_ptr as *mut c_void);
            }
            out.push((lexeme, positions));
        }
        close_tsvector_parser(state);
        out
    }

    #[test]
    fn weight_pos_accessors_roundtrip() {
        let mut x: WordEntryPos = 0;
        WEP_SETPOS(&mut x, 12345);
        WEP_SETWEIGHT(&mut x, 3);
        assert_eq!(WEP_GETPOS(x), 12345);
        assert_eq!(WEP_GETWEIGHT(x), 3);
        // pos field is 14 bits, weight occupies the top 2 untouched by SETPOS.
        WEP_SETPOS(&mut x, 1);
        assert_eq!(WEP_GETWEIGHT(x), 3);
        assert_eq!(WEP_GETPOS(x), 1);
        assert_eq!(LIMITPOS(MAXENTRYPOS + 5), MAXENTRYPOS - 1);
        assert_eq!(LIMITPOS(7), 7);
    }

    #[test]
    fn simple_lexemes() {
        unsafe {
            let toks = lex("a cat sat", 0);
            let words: Vec<_> = toks.iter().map(|(w, _)| w.as_str()).collect();
            assert_eq!(words, vec!["a", "cat", "sat"]);
            assert!(toks.iter().all(|(_, p)| p.is_empty()));
        }
    }

    #[test]
    fn lexeme_with_positions_and_weights() {
        unsafe {
            // "cat:1A,2 dog:3B"
            let toks = lex("cat:1A,2 dog:3B", 0);
            assert_eq!(toks.len(), 2);
            assert_eq!(toks[0].0, "cat");
            // weight A == 3, weight default == 0
            assert_eq!(toks[0].1, vec![(1u16, 3u16), (2u16, 0u16)]);
            assert_eq!(toks[1].0, "dog");
            assert_eq!(toks[1].1, vec![(3u16, 2u16)]);
        }
    }

    #[test]
    fn quoted_lexeme_with_escapes() {
        unsafe {
            // '\'' quoting allows embedded spaces; backslash escapes a quote.
            let toks = lex("'hello world'", 0);
            assert_eq!(toks.len(), 1);
            assert_eq!(toks[0].0, "hello world");

            // backslash-escaped space in an unquoted word
            let toks2 = lex("a\\ b", 0);
            assert_eq!(toks2.len(), 1);
            assert_eq!(toks2[0].0, "a b");
        }
    }
}
