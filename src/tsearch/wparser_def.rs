//! wparser_def.c
//!		Default text search parser
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/tsearch/wparser_def.c
//!
//! #include mapping:
//!   - "postgres.h"            -> crate::prelude::* (Datum, c-types, palloc/pfree,
//!                                 elog!/ereport!/errmsg!, Assert)
//!   - <limits.h>, <wctype.h>  -> libc isw*/is* via extern "C" (mirrors ts_locale.rs)
//!   - "commands/defrem.h"     -> defGetString from crate::commands::defrem
//!   - "mb/pg_wchar.h"         -> char2wchar/pg_mb2wchar_with_len/pg_mblen_range/
//!                                 pg_dsplen/pg_database_encoding_max_length/
//!                                 GetDatabaseEncoding/PG_UTF8/pg_wchar from crate::mb
//!   - "miscadmin.h"           -> check_stack_depth/CHECK_FOR_INTERRUPTS
//!   - "tsearch/ts_public.h"   -> LexDescr/HeadlineParsedText/HeadlineWordEntry
//!   - "tsearch/ts_type.h"     -> QueryOperand/TSQuery/GETQUERY (utils::adt::ts_type)
//!   - "tsearch/ts_utils.h"    -> TS_execute/TS_execute_locations/ExecPhraseData/
//!                                 TSTernaryValue/TS_EXEC_EMPTY (utils::adt::tsvector_op)
//!   - "utils/builtins.h"      -> pg_strtoint32 (utils::adt::numutils),
//!                                 pg_strncasecmp/pg_strcasecmp (port::pgstrcasecmp)
//!   - "utils/pg_locale.h"     -> pg_locale_t/database_ctype_is_c (STUBBED below)

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_uchar, c_uint, c_void};
use core::ffi::CStr;

use crate::{foreach, current_cell, Assert};

use crate::mb::pg_wchar::pg_wchar;
use crate::mb::mbutils::{
    pg_database_encoding_max_length, pg_mblen_range, pg_dsplen, GetDatabaseEncoding,
};
use crate::mb::pg_wchar::{pg_mb2wchar_with_len, PG_UTF8};

use crate::nodes::pg_list::{lfirst, List, ListCell, NIL};
use crate::nodes::parsenodes::DefElem;

use crate::commands::defrem::defGetString;
use crate::port::pgstrcasecmp::{pg_strcasecmp, pg_strncasecmp};
use crate::utils::adt::numutils::pg_strtoint32;

use crate::tsearch::ts_public::{HeadlineParsedText, HeadlineWordEntry, LexDescr};
use crate::utils::adt::ts_type::TSQuery;
use crate::utils::adt::tsquery_util::{GETQUERY, QueryOperand};
use crate::utils::adt::tsvector::WordEntryPos;
use crate::utils::adt::tsvector_op::{
    ExecPhraseData, TSTernaryValue, TS_execute, TS_execute_locations, TS_EXEC_EMPTY, TS_NO, TS_YES,
};

use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_INT32, PG_RETURN_POINTER, PG_RETURN_VOID,
};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;

    fn isalnum(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn islower(c: c_int) -> c_int;
    fn isprint(c: c_int) -> c_int;
    fn ispunct(c: c_int) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn isupper(c: c_int) -> c_int;
    fn isxdigit(c: c_int) -> c_int;

    fn iswalnum(wc: c_int) -> c_int;
    fn iswalpha(wc: c_int) -> c_int;
    fn iswdigit(wc: c_int) -> c_int;
    fn iswlower(wc: c_int) -> c_int;
    fn iswprint(wc: c_int) -> c_int;
    fn iswpunct(wc: c_int) -> c_int;
    fn iswspace(wc: c_int) -> c_int;
    fn iswupper(wc: c_int) -> c_int;
    fn iswxdigit(wc: c_int) -> c_int;
}

// ----------------------------------------------------------------------------
// Merged from utils/pg_locale.h / utils/adt/pg_locale.c -- not yet ported here.
// char2wchar lives in utils/adt/pg_locale_libc.rs but pg_locale_t/
// database_ctype_is_c are not yet centralized; STUB to match the C-locale path.
// ----------------------------------------------------------------------------

#[allow(non_camel_case_types)]
type wchar_t = i32;

#[allow(non_camel_case_types)]
type pg_locale_t = *mut c_void;

// TODO(pg-port): utils/adt/pg_locale.c -- whether the DB ctype is the C locale.
static mut database_ctype_is_c: bool = false;

// TODO(pg-port): utils/adt/pg_locale_libc.c char2wchar (multibyte -> wide for
// non-C locale).  Only reachable when database_ctype_is_c is false.
unsafe fn char2wchar(
    _to: *mut wchar_t,
    _tolen: usize,
    _from: *const c_char,
    fromlen: usize,
    _locale: pg_locale_t,
) -> usize {
    fromlen
}

// miscadmin.h
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{}};
}

// TODO(pg-port): miscadmin.c -- recursion guard.  No-op until tcop is wired.
unsafe fn check_stack_depth() {}


/* Define me to enable tracing of parser behavior */
/* #define WPARSER_TRACE */


/* Output token categories */

const ASCIIWORD: c_int = 1;
const WORD_T: c_int = 2;
const NUMWORD: c_int = 3;
const EMAIL: c_int = 4;
const URL_T: c_int = 5;
const HOST: c_int = 6;
const SCIENTIFIC: c_int = 7;
const VERSIONNUMBER: c_int = 8;
const NUMPARTHWORD: c_int = 9;
const PARTHWORD: c_int = 10;
const ASCIIPARTHWORD: c_int = 11;
const SPACE: c_int = 12;
const TAG_T: c_int = 13;
const PROTOCOL: c_int = 14;
const NUMHWORD: c_int = 15;
const ASCIIHWORD: c_int = 16;
const HWORD: c_int = 17;
const URLPATH: c_int = 18;
const FILEPATH: c_int = 19;
const DECIMAL_T: c_int = 20;
const SIGNEDINT: c_int = 21;
const UNSIGNEDINT: c_int = 22;
const XMLENTITY: c_int = 23;

const LASTNUM: c_int = 23;

static tok_alias: [*const c_char; 24] = [
    c"".as_ptr(),
    c"asciiword".as_ptr(),
    c"word".as_ptr(),
    c"numword".as_ptr(),
    c"email".as_ptr(),
    c"url".as_ptr(),
    c"host".as_ptr(),
    c"sfloat".as_ptr(),
    c"version".as_ptr(),
    c"hword_numpart".as_ptr(),
    c"hword_part".as_ptr(),
    c"hword_asciipart".as_ptr(),
    c"blank".as_ptr(),
    c"tag".as_ptr(),
    c"protocol".as_ptr(),
    c"numhword".as_ptr(),
    c"asciihword".as_ptr(),
    c"hword".as_ptr(),
    c"url_path".as_ptr(),
    c"file".as_ptr(),
    c"float".as_ptr(),
    c"int".as_ptr(),
    c"uint".as_ptr(),
    c"entity".as_ptr(),
];

static lex_descr: [*const c_char; 24] = [
    c"".as_ptr(),
    c"Word, all ASCII".as_ptr(),
    c"Word, all letters".as_ptr(),
    c"Word, letters and digits".as_ptr(),
    c"Email address".as_ptr(),
    c"URL".as_ptr(),
    c"Host".as_ptr(),
    c"Scientific notation".as_ptr(),
    c"Version number".as_ptr(),
    c"Hyphenated word part, letters and digits".as_ptr(),
    c"Hyphenated word part, all letters".as_ptr(),
    c"Hyphenated word part, all ASCII".as_ptr(),
    c"Space symbols".as_ptr(),
    c"XML tag".as_ptr(),
    c"Protocol head".as_ptr(),
    c"Hyphenated word, letters and digits".as_ptr(),
    c"Hyphenated word, all ASCII".as_ptr(),
    c"Hyphenated word, all letters".as_ptr(),
    c"URL path".as_ptr(),
    c"File or path name".as_ptr(),
    c"Decimal notation".as_ptr(),
    c"Signed integer".as_ptr(),
    c"Unsigned integer".as_ptr(),
    c"XML entity".as_ptr(),
];


/* Parser states */

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
enum TParserState {
    TPS_Base = 0,
    TPS_InNumWord,
    TPS_InAsciiWord,
    TPS_InWord,
    TPS_InUnsignedInt,
    TPS_InSignedIntFirst,
    TPS_InSignedInt,
    TPS_InSpace,
    TPS_InUDecimalFirst,
    TPS_InUDecimal,
    TPS_InDecimalFirst,
    TPS_InDecimal,
    TPS_InVerVersion,
    TPS_InSVerVersion,
    TPS_InVersionFirst,
    TPS_InVersion,
    TPS_InMantissaFirst,
    TPS_InMantissaSign,
    TPS_InMantissa,
    TPS_InXMLEntityFirst,
    TPS_InXMLEntity,
    TPS_InXMLEntityNumFirst,
    TPS_InXMLEntityNum,
    TPS_InXMLEntityHexNumFirst,
    TPS_InXMLEntityHexNum,
    TPS_InXMLEntityEnd,
    TPS_InTagFirst,
    TPS_InXMLBegin,
    TPS_InTagCloseFirst,
    TPS_InTagName,
    TPS_InTagBeginEnd,
    TPS_InTag,
    TPS_InTagEscapeK,
    TPS_InTagEscapeKK,
    TPS_InTagBackSleshed,
    TPS_InTagEnd,
    TPS_InCommentFirst,
    TPS_InCommentLast,
    TPS_InComment,
    TPS_InCloseCommentFirst,
    TPS_InCloseCommentLast,
    TPS_InCommentEnd,
    TPS_InHostFirstDomain,
    TPS_InHostDomainSecond,
    TPS_InHostDomain,
    TPS_InPortFirst,
    TPS_InPort,
    TPS_InHostFirstAN,
    TPS_InHost,
    TPS_InEmail,
    TPS_InFileFirst,
    TPS_InFileTwiddle,
    TPS_InPathFirst,
    TPS_InPathFirstFirst,
    TPS_InPathSecond,
    TPS_InFile,
    TPS_InFileNext,
    TPS_InURLPathFirst,
    TPS_InURLPathStart,
    TPS_InURLPath,
    TPS_InFURL,
    TPS_InProtocolFirst,
    TPS_InProtocolSecond,
    TPS_InProtocolEnd,
    TPS_InHyphenAsciiWordFirst,
    TPS_InHyphenAsciiWord,
    TPS_InHyphenWordFirst,
    TPS_InHyphenWord,
    TPS_InHyphenNumWordFirst,
    TPS_InHyphenNumWord,
    TPS_InHyphenDigitLookahead,
    TPS_InParseHyphen,
    TPS_InParseHyphenHyphen,
    TPS_InHyphenWordPart,
    TPS_InHyphenAsciiWordPart,
    TPS_InHyphenNumWordPart,
    TPS_InHyphenUnsignedInt,
    TPS_Null, /* last state (fake value) */
}
use TParserState::*;

type TParserCharTest = unsafe fn(*mut TParser) -> c_int; /* any p_is* functions
                                                          * except p_iseq */
type TParserSpecial = unsafe fn(*mut TParser); /* special handler for
                                                * special cases... */

#[repr(C)]
struct TParserStateActionItem {
    isclass: Option<TParserCharTest>,
    c: c_char,
    flags: u16,
    tostate: TParserState,
    r#type: c_int,
    special: Option<TParserSpecial>,
}

/* Flag bits in TParserStateActionItem.flags */
const A_NEXT: u16 = 0x0000;
const A_BINGO: u16 = 0x0001;
const A_POP: u16 = 0x0002;
const A_PUSH: u16 = 0x0004;
const A_RERUN: u16 = 0x0008;
const A_CLEAR: u16 = 0x0010;
const A_MERGE: u16 = 0x0020;
const A_CLRALL: u16 = 0x0040;

#[repr(C)]
struct TParserPosition {
    posbyte: c_int,      /* position of parser in bytes */
    poschar: c_int,      /* position of parser in characters */
    charlen: c_int,      /* length of current char */
    lenbytetoken: c_int, /* length of token-so-far in bytes */
    lenchartoken: c_int, /* and in chars */
    state: TParserState,
    prev: *mut TParserPosition,
    pushedAtAction: *const TParserStateActionItem,
}

#[repr(C)]
struct TParser {
    /* string and position information */
    str: *mut c_char, /* multibyte string */
    lenstr: c_int,    /* length of mbstring */
    wstr: *mut wchar_t, /* wide character string */
    pgwstr: *mut pg_wchar, /* wide character string for C-locale */
    usewide: bool,

    /* State of parse */
    charmaxlen: c_int,
    state: *mut TParserPosition,
    ignore: bool,
    wanthost: bool,

    /* silly char */
    c: c_char,

    /* out */
    token: *mut c_char,
    lenbytetoken: c_int,
    lenchartoken: c_int,
    r#type: c_int,
}


/* forward decls here */
/* static bool TParserGet(TParser *prs); -- defined below */


unsafe fn newTParserPosition(prev: *mut TParserPosition) -> *mut TParserPosition {
    let res = palloc(core::mem::size_of::<TParserPosition>()) as *mut TParserPosition;

    if !prev.is_null() {
        memcpy(
            res as *mut c_void,
            prev as *const c_void,
            core::mem::size_of::<TParserPosition>(),
        );
    } else {
        memset(res as *mut c_void, 0, core::mem::size_of::<TParserPosition>());
    }

    (*res).prev = prev;

    (*res).pushedAtAction = core::ptr::null();

    res
}

unsafe fn TParserInit(str: *mut c_char, len: c_int) -> *mut TParser {
    let prs = palloc0(core::mem::size_of::<TParser>()) as *mut TParser;

    (*prs).charmaxlen = pg_database_encoding_max_length();
    (*prs).str = str;
    (*prs).lenstr = len;

    /*
     * Use wide char code only when max encoding length > 1.
     */
    if (*prs).charmaxlen > 1 {
        let mylocale: pg_locale_t = core::ptr::null_mut(); /* TODO */

        (*prs).usewide = true;
        if database_ctype_is_c {
            /*
             * char2wchar doesn't work for C-locale and sizeof(pg_wchar) could
             * be different from sizeof(wchar_t)
             */
            (*prs).pgwstr = palloc(
                core::mem::size_of::<pg_wchar>() * ((*prs).lenstr as usize + 1),
            ) as *mut pg_wchar;
            pg_mb2wchar_with_len((*prs).str, (*prs).pgwstr, (*prs).lenstr);
        } else {
            (*prs).wstr = palloc(
                core::mem::size_of::<wchar_t>() * ((*prs).lenstr as usize + 1),
            ) as *mut wchar_t;
            char2wchar(
                (*prs).wstr,
                (*prs).lenstr as usize + 1,
                (*prs).str,
                (*prs).lenstr as usize,
                mylocale,
            );
        }
    } else {
        (*prs).usewide = false;
    }

    (*prs).state = newTParserPosition(core::ptr::null_mut());
    (*(*prs).state).state = TPS_Base;

    prs
}

/*
 * As an alternative to a full TParserInit one can create a
 * TParserCopy which basically is a regular TParser without a private
 * copy of the string - instead it uses the one from another TParser.
 * This is useful because at some places TParsers are created
 * recursively and the repeated copying around of the strings can
 * cause major inefficiency if the source string is long.
 * The new parser starts parsing at the original's current position.
 *
 * Obviously one must not close the original TParser before the copy.
 */
unsafe fn TParserCopyInit(orig: *const TParser) -> *mut TParser {
    let prs = palloc0(core::mem::size_of::<TParser>()) as *mut TParser;

    (*prs).charmaxlen = (*orig).charmaxlen;
    (*prs).str = (*orig).str.add((*(*orig).state).posbyte as usize);
    (*prs).lenstr = (*orig).lenstr - (*(*orig).state).posbyte;
    (*prs).usewide = (*orig).usewide;

    if !(*orig).pgwstr.is_null() {
        (*prs).pgwstr = (*orig).pgwstr.add((*(*orig).state).poschar as usize);
    }
    if !(*orig).wstr.is_null() {
        (*prs).wstr = (*orig).wstr.add((*(*orig).state).poschar as usize);
    }

    (*prs).state = newTParserPosition(core::ptr::null_mut());
    (*(*prs).state).state = TPS_Base;

    prs
}


unsafe fn TParserClose(prs: *mut TParser) {
    while !(*prs).state.is_null() {
        let ptr = (*(*prs).state).prev;

        pfree((*prs).state as *mut c_void);
        (*prs).state = ptr;
    }

    if !(*prs).wstr.is_null() {
        pfree((*prs).wstr as *mut c_void);
    }
    if !(*prs).pgwstr.is_null() {
        pfree((*prs).pgwstr as *mut c_void);
    }

    pfree(prs as *mut c_void);
}

/*
 * Close a parser created with TParserCopyInit
 */
unsafe fn TParserCopyClose(prs: *mut TParser) {
    while !(*prs).state.is_null() {
        let ptr = (*(*prs).state).prev;

        pfree((*prs).state as *mut c_void);
        (*prs).state = ptr;
    }

    pfree(prs as *mut c_void);
}


/*
 * Character-type support functions, equivalent to is* macros, but
 * working with any possible encodings and locales. Notes:
 *	- with multibyte encoding and C-locale isw* function may fail
 *	  or give wrong result.
 *	- multibyte encoding and C-locale often are used for
 *	  Asian languages.
 *	- if locale is C then we use pgwstr instead of wstr.
 */

/*
 * In C locale with a multibyte encoding, any non-ASCII symbol is considered
 * an alpha character, but not a member of other char classes.
 *
 * The C `p_iswhat(type, nonascii)` macro generates a p_is<type>() returning
 * is<type>()/isw<type>() and a p_isnot<type>() returning its negation.
 * Expanded explicitly below for each char class.
 */
macro_rules! p_iswhat {
    ($pis:ident, $pisnot:ident, $isfn:ident, $iswfn:ident, $nonascii:expr) => {
        unsafe fn $pis(prs: *mut TParser) -> c_int {
            Assert!(!(*prs).state.is_null());
            if (*prs).usewide {
                if !(*prs).pgwstr.is_null() {
                    let c: c_uint = *(*prs).pgwstr.add((*(*prs).state).poschar as usize);
                    if c > 0x7f {
                        return $nonascii;
                    }
                    return $isfn(c as c_int);
                }
                return $iswfn(*(*prs).wstr.add((*(*prs).state).poschar as usize) as c_int);
            }
            $isfn(*((*prs).str.add((*(*prs).state).posbyte as usize) as *const c_uchar) as c_int)
        }

        unsafe fn $pisnot(prs: *mut TParser) -> c_int {
            (!($pis(prs) != 0)) as c_int
        }
    };
}

p_iswhat!(p_isalnum, p_isnotalnum, isalnum, iswalnum, 1);
p_iswhat!(p_isalpha, p_isnotalpha, isalpha, iswalpha, 1);
p_iswhat!(p_isdigit, p_isnotdigit, isdigit, iswdigit, 0);
p_iswhat!(p_islower, p_isnotlower, islower, iswlower, 0);
p_iswhat!(p_isprint, p_isnotprint, isprint, iswprint, 0);
p_iswhat!(p_ispunct, p_isnotpunct, ispunct, iswpunct, 0);
p_iswhat!(p_isspace, p_isnotspace, isspace, iswspace, 0);
p_iswhat!(p_isupper, p_isnotupper, isupper, iswupper, 0);
p_iswhat!(p_isxdigit, p_isnotxdigit, isxdigit, iswxdigit, 0);

/* p_iseq should be used only for ascii symbols */

unsafe fn p_iseq(prs: *mut TParser, c: c_char) -> c_int {
    Assert!(!(*prs).state.is_null());
    if (*(*prs).state).charlen == 1 && *(*prs).str.add((*(*prs).state).posbyte as usize) == c {
        1
    } else {
        0
    }
}

unsafe fn p_isEOF(prs: *mut TParser) -> c_int {
    Assert!(!(*prs).state.is_null());
    if (*(*prs).state).posbyte == (*prs).lenstr || (*(*prs).state).charlen == 0 {
        1
    } else {
        0
    }
}

unsafe fn p_iseqC(prs: *mut TParser) -> c_int {
    p_iseq(prs, (*prs).c)
}

unsafe fn p_isneC(prs: *mut TParser) -> c_int {
    (!(p_iseq(prs, (*prs).c) != 0)) as c_int
}

unsafe fn p_isascii(prs: *mut TParser) -> c_int {
    if (*(*prs).state).charlen == 1
        && isascii(*((*prs).str.add((*(*prs).state).posbyte as usize) as *const c_uchar) as c_int)
            != 0
    {
        1
    } else {
        0
    }
}

unsafe fn p_isasclet(prs: *mut TParser) -> c_int {
    if p_isascii(prs) != 0 && p_isalpha(prs) != 0 {
        1
    } else {
        0
    }
}

unsafe fn p_isurlchar(prs: *mut TParser) -> c_int {
    /* no non-ASCII need apply */
    if (*(*prs).state).charlen != 1 {
        return 0;
    }
    let ch: c_char = *(*prs).str.add((*(*prs).state).posbyte as usize);
    /* no spaces or control characters */
    if ch <= 0x20 || ch >= 0x7F {
        return 0;
    }
    /* reject characters disallowed by RFC 3986 */
    match ch as u8 as char {
        '"' | '<' | '>' | '\\' | '^' | '`' | '{' | '|' | '}' => return 0,
        _ => {}
    }
    1
}

/* isascii(c): true iff c is a 7-bit value (ctype.h macro). */
#[inline]
unsafe fn isascii(c: c_int) -> c_int {
    ((c & !0x7f) == 0) as c_int
}


/* deliberately suppress unused-function complaints for the above */
#[allow(dead_code)]
pub unsafe fn _make_compiler_happy() {
    let np: *mut TParser = core::ptr::null_mut();
    p_isalnum(np);
    p_isnotalnum(np);
    p_isalpha(np);
    p_isnotalpha(np);
    p_isdigit(np);
    p_isnotdigit(np);
    p_islower(np);
    p_isnotlower(np);
    p_isprint(np);
    p_isnotprint(np);
    p_ispunct(np);
    p_isnotpunct(np);
    p_isspace(np);
    p_isnotspace(np);
    p_isupper(np);
    p_isnotupper(np);
    p_isxdigit(np);
    p_isnotxdigit(np);
    p_isEOF(np);
    p_iseqC(np);
    p_isneC(np);
}


unsafe fn SpecialTags(prs: *mut TParser) {
    match (*(*prs).state).lenchartoken {
        8 => {
            /* </script */
            if pg_strncasecmp((*prs).token, c"</script".as_ptr(), 8) == 0 {
                (*prs).ignore = false;
            }
        }
        7 => {
            /* <script || </style */
            if pg_strncasecmp((*prs).token, c"</style".as_ptr(), 7) == 0 {
                (*prs).ignore = false;
            } else if pg_strncasecmp((*prs).token, c"<script".as_ptr(), 7) == 0 {
                (*prs).ignore = true;
            }
        }
        6 => {
            /* <style */
            if pg_strncasecmp((*prs).token, c"<style".as_ptr(), 6) == 0 {
                (*prs).ignore = true;
            }
        }
        _ => {}
    }
}

unsafe fn SpecialFURL(prs: *mut TParser) {
    (*prs).wanthost = true;
    (*(*prs).state).posbyte -= (*(*prs).state).lenbytetoken;
    (*(*prs).state).poschar -= (*(*prs).state).lenchartoken;
}

unsafe fn SpecialHyphen(prs: *mut TParser) {
    (*(*prs).state).posbyte -= (*(*prs).state).lenbytetoken;
    (*(*prs).state).poschar -= (*(*prs).state).lenchartoken;
}

unsafe fn SpecialVerVersion(prs: *mut TParser) {
    (*(*prs).state).posbyte -= (*(*prs).state).lenbytetoken;
    (*(*prs).state).poschar -= (*(*prs).state).lenchartoken;
    (*(*prs).state).lenbytetoken = 0;
    (*(*prs).state).lenchartoken = 0;
}

unsafe fn p_isstophost(prs: *mut TParser) -> c_int {
    if (*prs).wanthost {
        (*prs).wanthost = false;
        return 1;
    }
    0
}

unsafe fn p_isignore(prs: *mut TParser) -> c_int {
    if (*prs).ignore {
        1
    } else {
        0
    }
}

unsafe fn p_ishost(prs: *mut TParser) -> c_int {
    let tmpprs: *mut TParser = TParserCopyInit(prs);
    let mut res: c_int = 0;

    (*tmpprs).wanthost = true;

    /*
     * Check stack depth before recursing.  (Since TParserGet() doesn't
     * normally recurse, we put the cost of checking here not there.)
     */
    check_stack_depth();

    if TParserGet(tmpprs) && (*tmpprs).r#type == HOST {
        (*(*prs).state).posbyte += (*tmpprs).lenbytetoken;
        (*(*prs).state).poschar += (*tmpprs).lenchartoken;
        (*(*prs).state).lenbytetoken += (*tmpprs).lenbytetoken;
        (*(*prs).state).lenchartoken += (*tmpprs).lenchartoken;
        (*(*prs).state).charlen = (*(*tmpprs).state).charlen;
        res = 1;
    }
    TParserCopyClose(tmpprs);

    res
}

unsafe fn p_isURLPath(prs: *mut TParser) -> c_int {
    let tmpprs: *mut TParser = TParserCopyInit(prs);
    let mut res: c_int = 0;

    (*tmpprs).state = newTParserPosition((*tmpprs).state);
    (*(*tmpprs).state).state = TPS_InURLPathFirst;

    /*
     * Check stack depth before recursing.  (Since TParserGet() doesn't
     * normally recurse, we put the cost of checking here not there.)
     */
    check_stack_depth();

    if TParserGet(tmpprs) && (*tmpprs).r#type == URLPATH {
        (*(*prs).state).posbyte += (*tmpprs).lenbytetoken;
        (*(*prs).state).poschar += (*tmpprs).lenchartoken;
        (*(*prs).state).lenbytetoken += (*tmpprs).lenbytetoken;
        (*(*prs).state).lenchartoken += (*tmpprs).lenchartoken;
        (*(*prs).state).charlen = (*(*tmpprs).state).charlen;
        res = 1;
    }
    TParserCopyClose(tmpprs);

    res
}

/*
 * returns true if current character has zero display length or
 * it's a special sign in several languages. Such characters
 * aren't a word-breaker although they aren't an isalpha.
 * In beginning of word they aren't a part of it.
 */
unsafe fn p_isspecial(prs: *mut TParser) -> c_int {
    /*
     * pg_dsplen could return -1 which means error or control character
     */
    if pg_dsplen((*prs).str.add((*(*prs).state).posbyte as usize)) == 0 {
        return 1;
    }

    /*
     * Unicode Characters in the 'Mark, Spacing Combining' Category That
     * characters are not alpha although they are not breakers of word too.
     * Check that only in utf encoding, because other encodings aren't
     * supported by postgres or even exists.
     */
    if GetDatabaseEncoding() == PG_UTF8 as c_int && (*prs).usewide {
        static strange_letter: [pg_wchar; 333] = [
            /*
             * use binary search, so elements should be ordered
             */
            0x0903, /* DEVANAGARI SIGN VISARGA */
            0x093E, /* DEVANAGARI VOWEL SIGN AA */
            0x093F, /* DEVANAGARI VOWEL SIGN I */
            0x0940, /* DEVANAGARI VOWEL SIGN II */
            0x0949, /* DEVANAGARI VOWEL SIGN CANDRA O */
            0x094A, /* DEVANAGARI VOWEL SIGN SHORT O */
            0x094B, /* DEVANAGARI VOWEL SIGN O */
            0x094C, /* DEVANAGARI VOWEL SIGN AU */
            0x0982, /* BENGALI SIGN ANUSVARA */
            0x0983, /* BENGALI SIGN VISARGA */
            0x09BE, /* BENGALI VOWEL SIGN AA */
            0x09BF, /* BENGALI VOWEL SIGN I */
            0x09C0, /* BENGALI VOWEL SIGN II */
            0x09C7, /* BENGALI VOWEL SIGN E */
            0x09C8, /* BENGALI VOWEL SIGN AI */
            0x09CB, /* BENGALI VOWEL SIGN O */
            0x09CC, /* BENGALI VOWEL SIGN AU */
            0x09D7, /* BENGALI AU LENGTH MARK */
            0x0A03, /* GURMUKHI SIGN VISARGA */
            0x0A3E, /* GURMUKHI VOWEL SIGN AA */
            0x0A3F, /* GURMUKHI VOWEL SIGN I */
            0x0A40, /* GURMUKHI VOWEL SIGN II */
            0x0A83, /* GUJARATI SIGN VISARGA */
            0x0ABE, /* GUJARATI VOWEL SIGN AA */
            0x0ABF, /* GUJARATI VOWEL SIGN I */
            0x0AC0, /* GUJARATI VOWEL SIGN II */
            0x0AC9, /* GUJARATI VOWEL SIGN CANDRA O */
            0x0ACB, /* GUJARATI VOWEL SIGN O */
            0x0ACC, /* GUJARATI VOWEL SIGN AU */
            0x0B02, /* ORIYA SIGN ANUSVARA */
            0x0B03, /* ORIYA SIGN VISARGA */
            0x0B3E, /* ORIYA VOWEL SIGN AA */
            0x0B40, /* ORIYA VOWEL SIGN II */
            0x0B47, /* ORIYA VOWEL SIGN E */
            0x0B48, /* ORIYA VOWEL SIGN AI */
            0x0B4B, /* ORIYA VOWEL SIGN O */
            0x0B4C, /* ORIYA VOWEL SIGN AU */
            0x0B57, /* ORIYA AU LENGTH MARK */
            0x0BBE, /* TAMIL VOWEL SIGN AA */
            0x0BBF, /* TAMIL VOWEL SIGN I */
            0x0BC1, /* TAMIL VOWEL SIGN U */
            0x0BC2, /* TAMIL VOWEL SIGN UU */
            0x0BC6, /* TAMIL VOWEL SIGN E */
            0x0BC7, /* TAMIL VOWEL SIGN EE */
            0x0BC8, /* TAMIL VOWEL SIGN AI */
            0x0BCA, /* TAMIL VOWEL SIGN O */
            0x0BCB, /* TAMIL VOWEL SIGN OO */
            0x0BCC, /* TAMIL VOWEL SIGN AU */
            0x0BD7, /* TAMIL AU LENGTH MARK */
            0x0C01, /* TELUGU SIGN CANDRABINDU */
            0x0C02, /* TELUGU SIGN ANUSVARA */
            0x0C03, /* TELUGU SIGN VISARGA */
            0x0C41, /* TELUGU VOWEL SIGN U */
            0x0C42, /* TELUGU VOWEL SIGN UU */
            0x0C43, /* TELUGU VOWEL SIGN VOCALIC R */
            0x0C44, /* TELUGU VOWEL SIGN VOCALIC RR */
            0x0C82, /* KANNADA SIGN ANUSVARA */
            0x0C83, /* KANNADA SIGN VISARGA */
            0x0CBE, /* KANNADA VOWEL SIGN AA */
            0x0CC0, /* KANNADA VOWEL SIGN II */
            0x0CC1, /* KANNADA VOWEL SIGN U */
            0x0CC2, /* KANNADA VOWEL SIGN UU */
            0x0CC3, /* KANNADA VOWEL SIGN VOCALIC R */
            0x0CC4, /* KANNADA VOWEL SIGN VOCALIC RR */
            0x0CC7, /* KANNADA VOWEL SIGN EE */
            0x0CC8, /* KANNADA VOWEL SIGN AI */
            0x0CCA, /* KANNADA VOWEL SIGN O */
            0x0CCB, /* KANNADA VOWEL SIGN OO */
            0x0CD5, /* KANNADA LENGTH MARK */
            0x0CD6, /* KANNADA AI LENGTH MARK */
            0x0D02, /* MALAYALAM SIGN ANUSVARA */
            0x0D03, /* MALAYALAM SIGN VISARGA */
            0x0D3E, /* MALAYALAM VOWEL SIGN AA */
            0x0D3F, /* MALAYALAM VOWEL SIGN I */
            0x0D40, /* MALAYALAM VOWEL SIGN II */
            0x0D46, /* MALAYALAM VOWEL SIGN E */
            0x0D47, /* MALAYALAM VOWEL SIGN EE */
            0x0D48, /* MALAYALAM VOWEL SIGN AI */
            0x0D4A, /* MALAYALAM VOWEL SIGN O */
            0x0D4B, /* MALAYALAM VOWEL SIGN OO */
            0x0D4C, /* MALAYALAM VOWEL SIGN AU */
            0x0D57, /* MALAYALAM AU LENGTH MARK */
            0x0D82, /* SINHALA SIGN ANUSVARAYA */
            0x0D83, /* SINHALA SIGN VISARGAYA */
            0x0DCF, /* SINHALA VOWEL SIGN AELA-PILLA */
            0x0DD0, /* SINHALA VOWEL SIGN KETTI AEDA-PILLA */
            0x0DD1, /* SINHALA VOWEL SIGN DIGA AEDA-PILLA */
            0x0DD8, /* SINHALA VOWEL SIGN GAETTA-PILLA */
            0x0DD9, /* SINHALA VOWEL SIGN KOMBUVA */
            0x0DDA, /* SINHALA VOWEL SIGN DIGA KOMBUVA */
            0x0DDB, /* SINHALA VOWEL SIGN KOMBU DEKA */
            0x0DDC, /* SINHALA VOWEL SIGN KOMBUVA HAA AELA-PILLA */
            0x0DDD, /* SINHALA VOWEL SIGN KOMBUVA HAA DIGA
                     * AELA-PILLA */
            0x0DDE, /* SINHALA VOWEL SIGN KOMBUVA HAA GAYANUKITTA */
            0x0DDF, /* SINHALA VOWEL SIGN GAYANUKITTA */
            0x0DF2, /* SINHALA VOWEL SIGN DIGA GAETTA-PILLA */
            0x0DF3, /* SINHALA VOWEL SIGN DIGA GAYANUKITTA */
            0x0F3E, /* TIBETAN SIGN YAR TSHES */
            0x0F3F, /* TIBETAN SIGN MAR TSHES */
            0x0F7F, /* TIBETAN SIGN RNAM BCAD */
            0x102B, /* MYANMAR VOWEL SIGN TALL AA */
            0x102C, /* MYANMAR VOWEL SIGN AA */
            0x1031, /* MYANMAR VOWEL SIGN E */
            0x1038, /* MYANMAR SIGN VISARGA */
            0x103B, /* MYANMAR CONSONANT SIGN MEDIAL YA */
            0x103C, /* MYANMAR CONSONANT SIGN MEDIAL RA */
            0x1056, /* MYANMAR VOWEL SIGN VOCALIC R */
            0x1057, /* MYANMAR VOWEL SIGN VOCALIC RR */
            0x1062, /* MYANMAR VOWEL SIGN SGAW KAREN EU */
            0x1063, /* MYANMAR TONE MARK SGAW KAREN HATHI */
            0x1064, /* MYANMAR TONE MARK SGAW KAREN KE PHO */
            0x1067, /* MYANMAR VOWEL SIGN WESTERN PWO KAREN EU */
            0x1068, /* MYANMAR VOWEL SIGN WESTERN PWO KAREN UE */
            0x1069, /* MYANMAR SIGN WESTERN PWO KAREN TONE-1 */
            0x106A, /* MYANMAR SIGN WESTERN PWO KAREN TONE-2 */
            0x106B, /* MYANMAR SIGN WESTERN PWO KAREN TONE-3 */
            0x106C, /* MYANMAR SIGN WESTERN PWO KAREN TONE-4 */
            0x106D, /* MYANMAR SIGN WESTERN PWO KAREN TONE-5 */
            0x1083, /* MYANMAR VOWEL SIGN SHAN AA */
            0x1084, /* MYANMAR VOWEL SIGN SHAN E */
            0x1087, /* MYANMAR SIGN SHAN TONE-2 */
            0x1088, /* MYANMAR SIGN SHAN TONE-3 */
            0x1089, /* MYANMAR SIGN SHAN TONE-5 */
            0x108A, /* MYANMAR SIGN SHAN TONE-6 */
            0x108B, /* MYANMAR SIGN SHAN COUNCIL TONE-2 */
            0x108C, /* MYANMAR SIGN SHAN COUNCIL TONE-3 */
            0x108F, /* MYANMAR SIGN RUMAI PALAUNG TONE-5 */
            0x17B6, /* KHMER VOWEL SIGN AA */
            0x17BE, /* KHMER VOWEL SIGN OE */
            0x17BF, /* KHMER VOWEL SIGN YA */
            0x17C0, /* KHMER VOWEL SIGN IE */
            0x17C1, /* KHMER VOWEL SIGN E */
            0x17C2, /* KHMER VOWEL SIGN AE */
            0x17C3, /* KHMER VOWEL SIGN AI */
            0x17C4, /* KHMER VOWEL SIGN OO */
            0x17C5, /* KHMER VOWEL SIGN AU */
            0x17C7, /* KHMER SIGN REAHMUK */
            0x17C8, /* KHMER SIGN YUUKALEAPINTU */
            0x1923, /* LIMBU VOWEL SIGN EE */
            0x1924, /* LIMBU VOWEL SIGN AI */
            0x1925, /* LIMBU VOWEL SIGN OO */
            0x1926, /* LIMBU VOWEL SIGN AU */
            0x1929, /* LIMBU SUBJOINED LETTER YA */
            0x192A, /* LIMBU SUBJOINED LETTER RA */
            0x192B, /* LIMBU SUBJOINED LETTER WA */
            0x1930, /* LIMBU SMALL LETTER KA */
            0x1931, /* LIMBU SMALL LETTER NGA */
            0x1933, /* LIMBU SMALL LETTER TA */
            0x1934, /* LIMBU SMALL LETTER NA */
            0x1935, /* LIMBU SMALL LETTER PA */
            0x1936, /* LIMBU SMALL LETTER MA */
            0x1937, /* LIMBU SMALL LETTER RA */
            0x1938, /* LIMBU SMALL LETTER LA */
            0x19B0, /* NEW TAI LUE VOWEL SIGN VOWEL SHORTENER */
            0x19B1, /* NEW TAI LUE VOWEL SIGN AA */
            0x19B2, /* NEW TAI LUE VOWEL SIGN II */
            0x19B3, /* NEW TAI LUE VOWEL SIGN U */
            0x19B4, /* NEW TAI LUE VOWEL SIGN UU */
            0x19B5, /* NEW TAI LUE VOWEL SIGN E */
            0x19B6, /* NEW TAI LUE VOWEL SIGN AE */
            0x19B7, /* NEW TAI LUE VOWEL SIGN O */
            0x19B8, /* NEW TAI LUE VOWEL SIGN OA */
            0x19B9, /* NEW TAI LUE VOWEL SIGN UE */
            0x19BA, /* NEW TAI LUE VOWEL SIGN AY */
            0x19BB, /* NEW TAI LUE VOWEL SIGN AAY */
            0x19BC, /* NEW TAI LUE VOWEL SIGN UY */
            0x19BD, /* NEW TAI LUE VOWEL SIGN OY */
            0x19BE, /* NEW TAI LUE VOWEL SIGN OAY */
            0x19BF, /* NEW TAI LUE VOWEL SIGN UEY */
            0x19C0, /* NEW TAI LUE VOWEL SIGN IY */
            0x19C8, /* NEW TAI LUE TONE MARK-1 */
            0x19C9, /* NEW TAI LUE TONE MARK-2 */
            0x1A19, /* BUGINESE VOWEL SIGN E */
            0x1A1A, /* BUGINESE VOWEL SIGN O */
            0x1A1B, /* BUGINESE VOWEL SIGN AE */
            0x1B04, /* BALINESE SIGN BISAH */
            0x1B35, /* BALINESE VOWEL SIGN TEDUNG */
            0x1B3B, /* BALINESE VOWEL SIGN RA REPA TEDUNG */
            0x1B3D, /* BALINESE VOWEL SIGN LA LENGA TEDUNG */
            0x1B3E, /* BALINESE VOWEL SIGN TALING */
            0x1B3F, /* BALINESE VOWEL SIGN TALING REPA */
            0x1B40, /* BALINESE VOWEL SIGN TALING TEDUNG */
            0x1B41, /* BALINESE VOWEL SIGN TALING REPA TEDUNG */
            0x1B43, /* BALINESE VOWEL SIGN PEPET TEDUNG */
            0x1B44, /* BALINESE ADEG ADEG */
            0x1B82, /* SUNDANESE SIGN PANGWISAD */
            0x1BA1, /* SUNDANESE CONSONANT SIGN PAMINGKAL */
            0x1BA6, /* SUNDANESE VOWEL SIGN PANAELAENG */
            0x1BA7, /* SUNDANESE VOWEL SIGN PANOLONG */
            0x1BAA, /* SUNDANESE SIGN PAMAAEH */
            0x1C24, /* LEPCHA SUBJOINED LETTER YA */
            0x1C25, /* LEPCHA SUBJOINED LETTER RA */
            0x1C26, /* LEPCHA VOWEL SIGN AA */
            0x1C27, /* LEPCHA VOWEL SIGN I */
            0x1C28, /* LEPCHA VOWEL SIGN O */
            0x1C29, /* LEPCHA VOWEL SIGN OO */
            0x1C2A, /* LEPCHA VOWEL SIGN U */
            0x1C2B, /* LEPCHA VOWEL SIGN UU */
            0x1C34, /* LEPCHA CONSONANT SIGN NYIN-DO */
            0x1C35, /* LEPCHA CONSONANT SIGN KANG */
            0xA823, /* SYLOTI NAGRI VOWEL SIGN A */
            0xA824, /* SYLOTI NAGRI VOWEL SIGN I */
            0xA827, /* SYLOTI NAGRI VOWEL SIGN OO */
            0xA880, /* SAURASHTRA SIGN ANUSVARA */
            0xA881, /* SAURASHTRA SIGN VISARGA */
            0xA8B4, /* SAURASHTRA CONSONANT SIGN HAARU */
            0xA8B5, /* SAURASHTRA VOWEL SIGN AA */
            0xA8B6, /* SAURASHTRA VOWEL SIGN I */
            0xA8B7, /* SAURASHTRA VOWEL SIGN II */
            0xA8B8, /* SAURASHTRA VOWEL SIGN U */
            0xA8B9, /* SAURASHTRA VOWEL SIGN UU */
            0xA8BA, /* SAURASHTRA VOWEL SIGN VOCALIC R */
            0xA8BB, /* SAURASHTRA VOWEL SIGN VOCALIC RR */
            0xA8BC, /* SAURASHTRA VOWEL SIGN VOCALIC L */
            0xA8BD, /* SAURASHTRA VOWEL SIGN VOCALIC LL */
            0xA8BE, /* SAURASHTRA VOWEL SIGN E */
            0xA8BF, /* SAURASHTRA VOWEL SIGN EE */
            0xA8C0, /* SAURASHTRA VOWEL SIGN AI */
            0xA8C1, /* SAURASHTRA VOWEL SIGN O */
            0xA8C2, /* SAURASHTRA VOWEL SIGN OO */
            0xA8C3, /* SAURASHTRA VOWEL SIGN AU */
            0xA952, /* REJANG CONSONANT SIGN H */
            0xA953, /* REJANG VIRAMA */
            0xAA2F, /* CHAM VOWEL SIGN O */
            0xAA30, /* CHAM VOWEL SIGN AI */
            0xAA33, /* CHAM CONSONANT SIGN YA */
            0xAA34, /* CHAM CONSONANT SIGN RA */
            0xAA4D, /* CHAM CONSONANT SIGN FINAL H */
        ];
        let mut StopLow: *const pg_wchar = strange_letter.as_ptr();
        let mut StopHigh: *const pg_wchar = strange_letter.as_ptr().add(strange_letter.len());
        let mut StopMiddle: *const pg_wchar;
        let c: pg_wchar;

        if !(*prs).pgwstr.is_null() {
            c = *(*prs).pgwstr.add((*(*prs).state).poschar as usize);
        } else {
            c = *(*prs).wstr.add((*(*prs).state).poschar as usize) as pg_wchar;
        }

        while StopLow < StopHigh {
            StopMiddle = StopLow.add((StopHigh.offset_from(StopLow) as usize) >> 1);
            if *StopMiddle == c {
                return 1;
            } else if *StopMiddle < c {
                StopLow = StopMiddle.add(1);
            } else {
                StopHigh = StopMiddle;
            }
        }
    }

    0
}


/*
 * Table of state/action of parser
 *
 * Each row is { isclass, c, flags, tostate, type, special }.  A NULL isclass
 * in C becomes None here (the catch-all final row).
 */

macro_rules! AI {
    ($isclass:expr, $c:expr, $flags:expr, $tostate:expr, $type:expr, $special:expr) => {
        TParserStateActionItem {
            isclass: $isclass,
            c: $c as c_char,
            flags: $flags,
            tostate: $tostate,
            r#type: $type,
            special: $special,
        }
    };
}

static actionTPS_Base: [TParserStateActionItem; 13] = [
    AI!(Some(p_isEOF), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'<', A_PUSH, TPS_InTagFirst, 0, None),
    AI!(Some(p_isignore), 0, A_NEXT, TPS_InSpace, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InAsciiWord, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InWord, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InUnsignedInt, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InSignedIntFirst, 0, None),
    AI!(Some(p_iseqC), b'+', A_PUSH, TPS_InSignedIntFirst, 0, None),
    AI!(Some(p_iseqC), b'&', A_PUSH, TPS_InXMLEntityFirst, 0, None),
    AI!(Some(p_iseqC), b'~', A_PUSH, TPS_InFileTwiddle, 0, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFileFirst, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InPathFirstFirst, 0, None),
    AI!(None, 0, A_NEXT, TPS_InSpace, 0, None),
];


static actionTPS_InNumWord: [TParserStateActionItem; 8] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, NUMWORD, None),
    AI!(Some(p_isalnum), 0, A_NEXT, TPS_InNumWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InNumWord, 0, None),
    AI!(Some(p_iseqC), b'@', A_PUSH, TPS_InEmail, 0, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFileFirst, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InFileNext, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHyphenNumWordFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, NUMWORD, None),
];

static actionTPS_InAsciiWord: [TParserStateActionItem; 15] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, ASCIIWORD, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InHostFirstDomain, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InFileNext, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHyphenAsciiWordFirst, 0, None),
    AI!(Some(p_iseqC), b'_', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'@', A_PUSH, TPS_InEmail, 0, None),
    AI!(Some(p_iseqC), b':', A_PUSH, TPS_InProtocolFirst, 0, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFileFirst, 0, None),
    AI!(Some(p_isdigit), 0, A_PUSH, TPS_InHost, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InNumWord, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InWord, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, ASCIIWORD, None),
];

static actionTPS_InWord: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, WORD_T, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InNumWord, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHyphenWordFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, WORD_T, None),
];

static actionTPS_InUnsignedInt: [TParserStateActionItem; 14] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, UNSIGNEDINT, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InHostFirstDomain, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InUDecimalFirst, 0, None),
    AI!(Some(p_iseqC), b'e', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(Some(p_iseqC), b'E', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'_', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'@', A_PUSH, TPS_InEmail, 0, None),
    AI!(Some(p_isasclet), 0, A_PUSH, TPS_InHost, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InNumWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InNumWord, 0, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFileFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, UNSIGNEDINT, None),
];

static actionTPS_InSignedIntFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT | A_CLEAR, TPS_InSignedInt, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InSignedInt: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, SIGNEDINT, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InDecimalFirst, 0, None),
    AI!(Some(p_iseqC), b'e', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(Some(p_iseqC), b'E', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, SIGNEDINT, None),
];

static actionTPS_InSpace: [TParserStateActionItem; 9] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, SPACE, None),
    AI!(Some(p_iseqC), b'<', A_BINGO, TPS_Base, SPACE, None),
    AI!(Some(p_isignore), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_BINGO, TPS_Base, SPACE, None),
    AI!(Some(p_iseqC), b'+', A_BINGO, TPS_Base, SPACE, None),
    AI!(Some(p_iseqC), b'&', A_BINGO, TPS_Base, SPACE, None),
    AI!(Some(p_iseqC), b'/', A_BINGO, TPS_Base, SPACE, None),
    AI!(Some(p_isnotalnum), 0, A_NEXT, TPS_InSpace, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, SPACE, None),
];

static actionTPS_InUDecimalFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_CLEAR, TPS_InUDecimal, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InUDecimal: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, DECIMAL_T, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InUDecimal, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InVersionFirst, 0, None),
    AI!(Some(p_iseqC), b'e', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(Some(p_iseqC), b'E', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, DECIMAL_T, None),
];

static actionTPS_InDecimalFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_CLEAR, TPS_InDecimal, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InDecimal: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, DECIMAL_T, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InDecimal, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InVerVersion, 0, None),
    AI!(Some(p_iseqC), b'e', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(Some(p_iseqC), b'E', A_PUSH, TPS_InMantissaFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, DECIMAL_T, None),
];

static actionTPS_InVerVersion: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_RERUN, TPS_InSVerVersion, 0, Some(SpecialVerVersion)),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InSVerVersion: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_BINGO | A_CLRALL, TPS_InUnsignedInt, SPACE, None),
    AI!(None, 0, A_NEXT, TPS_Null, 0, None),
];


static actionTPS_InVersionFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_CLEAR, TPS_InVersion, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InVersion: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, VERSIONNUMBER, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InVersion, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InVersionFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, VERSIONNUMBER, None),
];

static actionTPS_InMantissaFirst: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_CLEAR, TPS_InMantissa, 0, None),
    AI!(Some(p_iseqC), b'+', A_NEXT, TPS_InMantissaSign, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InMantissaSign, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InMantissaSign: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_CLEAR, TPS_InMantissa, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InMantissa: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, SCIENTIFIC, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InMantissa, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, SCIENTIFIC, None),
];

static actionTPS_InXMLEntityFirst: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'#', A_NEXT, TPS_InXMLEntityNumFirst, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b':', A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLEntity: [TParserStateActionItem; 8] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isalnum), 0, A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b':', A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b'.', A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InXMLEntity, 0, None),
    AI!(Some(p_iseqC), b';', A_NEXT, TPS_InXMLEntityEnd, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLEntityNumFirst: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'x', A_NEXT, TPS_InXMLEntityHexNumFirst, 0, None),
    AI!(Some(p_iseqC), b'X', A_NEXT, TPS_InXMLEntityHexNumFirst, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InXMLEntityNum, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLEntityHexNumFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isxdigit), 0, A_NEXT, TPS_InXMLEntityHexNum, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLEntityNum: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InXMLEntityNum, 0, None),
    AI!(Some(p_iseqC), b';', A_NEXT, TPS_InXMLEntityEnd, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLEntityHexNum: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isxdigit), 0, A_NEXT, TPS_InXMLEntityHexNum, 0, None),
    AI!(Some(p_iseqC), b';', A_NEXT, TPS_InXMLEntityEnd, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLEntityEnd: [TParserStateActionItem; 1] = [
    AI!(None, 0, A_BINGO | A_CLEAR, TPS_Base, XMLENTITY, None),
];

static actionTPS_InTagFirst: [TParserStateActionItem; 8] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InTagCloseFirst, 0, None),
    AI!(Some(p_iseqC), b'!', A_PUSH, TPS_InCommentFirst, 0, None),
    AI!(Some(p_iseqC), b'?', A_PUSH, TPS_InXMLBegin, 0, None),
    AI!(Some(p_isasclet), 0, A_PUSH, TPS_InTagName, 0, None),
    AI!(Some(p_iseqC), b':', A_PUSH, TPS_InTagName, 0, None),
    AI!(Some(p_iseqC), b'_', A_PUSH, TPS_InTagName, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InXMLBegin: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    /* <?xml ... */
    /* XXX do we wants states for the m and l ?  Right now this accepts <?xZ */
    AI!(Some(p_iseqC), b'x', A_NEXT, TPS_InTag, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InTagCloseFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InTagName, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InTagName: [TParserStateActionItem; 10] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    /* <br/> case */
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_InTagBeginEnd, 0, None),
    AI!(Some(p_iseqC), b'>', A_NEXT, TPS_InTagEnd, 0, Some(SpecialTags)),
    AI!(Some(p_isspace), 0, A_NEXT, TPS_InTag, 0, Some(SpecialTags)),
    AI!(Some(p_isalnum), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b':', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'.', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_Null, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InTagBeginEnd: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'>', A_NEXT, TPS_InTagEnd, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InTag: [TParserStateActionItem; 19] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'>', A_NEXT, TPS_InTagEnd, 0, Some(SpecialTags)),
    AI!(Some(p_iseqC), b'\'', A_NEXT, TPS_InTagEscapeK, 0, None),
    AI!(Some(p_iseqC), b'"', A_NEXT, TPS_InTagEscapeKK, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'=', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'#', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b':', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'.', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'&', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'?', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'%', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'~', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_isspace), 0, A_NEXT, TPS_Null, 0, Some(SpecialTags)),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InTagEscapeK: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'\\', A_PUSH, TPS_InTagBackSleshed, 0, None),
    AI!(Some(p_iseqC), b'\'', A_NEXT, TPS_InTag, 0, None),
    AI!(None, 0, A_NEXT, TPS_InTagEscapeK, 0, None),
];

static actionTPS_InTagEscapeKK: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'\\', A_PUSH, TPS_InTagBackSleshed, 0, None),
    AI!(Some(p_iseqC), b'"', A_NEXT, TPS_InTag, 0, None),
    AI!(None, 0, A_NEXT, TPS_InTagEscapeKK, 0, None),
];

static actionTPS_InTagBackSleshed: [TParserStateActionItem; 2] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(None, 0, A_MERGE, TPS_Null, 0, None),
];

static actionTPS_InTagEnd: [TParserStateActionItem; 1] = [
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_Base, TAG_T, None),
];

static actionTPS_InCommentFirst: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InCommentLast, 0, None),
    /* <!DOCTYPE ...> */
    AI!(Some(p_iseqC), b'D', A_NEXT, TPS_InTag, 0, None),
    AI!(Some(p_iseqC), b'd', A_NEXT, TPS_InTag, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InCommentLast: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InComment, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InComment: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InCloseCommentFirst, 0, None),
    AI!(None, 0, A_NEXT, TPS_Null, 0, None),
];

static actionTPS_InCloseCommentFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InCloseCommentLast, 0, None),
    AI!(None, 0, A_NEXT, TPS_InComment, 0, None),
];

static actionTPS_InCloseCommentLast: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'>', A_NEXT, TPS_InCommentEnd, 0, None),
    AI!(None, 0, A_NEXT, TPS_InComment, 0, None),
];

static actionTPS_InCommentEnd: [TParserStateActionItem; 1] = [
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_Base, TAG_T, None),
];

static actionTPS_InHostFirstDomain: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHostDomainSecond, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHost, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHostDomainSecond: [TParserStateActionItem; 8] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHostDomain, 0, None),
    AI!(Some(p_isdigit), 0, A_PUSH, TPS_InHost, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'_', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InHostFirstDomain, 0, None),
    AI!(Some(p_iseqC), b'@', A_PUSH, TPS_InEmail, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHostDomain: [TParserStateActionItem; 12] = [
    AI!(Some(p_isEOF), 0, A_BINGO | A_CLRALL, TPS_Base, HOST, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHostDomain, 0, None),
    AI!(Some(p_isdigit), 0, A_PUSH, TPS_InHost, 0, None),
    AI!(Some(p_iseqC), b':', A_PUSH, TPS_InPortFirst, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'_', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InHostFirstDomain, 0, None),
    AI!(Some(p_iseqC), b'@', A_PUSH, TPS_InEmail, 0, None),
    AI!(Some(p_isdigit), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isstophost), 0, A_BINGO | A_CLRALL, TPS_InURLPathStart, HOST, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFURL, 0, None),
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_Base, HOST, None),
];

static actionTPS_InPortFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InPort, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InPort: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_BINGO | A_CLRALL, TPS_Base, HOST, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InPort, 0, None),
    AI!(Some(p_isstophost), 0, A_BINGO | A_CLRALL, TPS_InURLPathStart, HOST, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFURL, 0, None),
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_Base, HOST, None),
];

static actionTPS_InHostFirstAN: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHost, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHost, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHost: [TParserStateActionItem; 8] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHost, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHost, 0, None),
    AI!(Some(p_iseqC), b'@', A_PUSH, TPS_InEmail, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InHostFirstDomain, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(Some(p_iseqC), b'_', A_PUSH, TPS_InHostFirstAN, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InEmail: [TParserStateActionItem; 3] = [
    AI!(Some(p_isstophost), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_ishost), 0, A_BINGO | A_CLRALL, TPS_Base, EMAIL, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InFileFirst: [TParserStateActionItem; 7] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'.', A_NEXT, TPS_InPathFirst, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'~', A_PUSH, TPS_InFileTwiddle, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InFileTwiddle: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_InFileFirst, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InPathFirst: [TParserStateActionItem; 7] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'.', A_NEXT, TPS_InPathSecond, 0, None),
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_InFileFirst, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InPathFirstFirst: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'.', A_NEXT, TPS_InPathSecond, 0, None),
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_InFileFirst, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InPathSecond: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_BINGO | A_CLEAR, TPS_Base, FILEPATH, None),
    AI!(Some(p_iseqC), b'/', A_NEXT | A_PUSH, TPS_InFileFirst, 0, None),
    AI!(Some(p_iseqC), b'/', A_BINGO | A_CLEAR, TPS_Base, FILEPATH, None),
    AI!(Some(p_isspace), 0, A_BINGO | A_CLEAR, TPS_Base, FILEPATH, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InFile: [TParserStateActionItem; 8] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, FILEPATH, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'.', A_PUSH, TPS_InFileNext, 0, None),
    AI!(Some(p_iseqC), b'_', A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'-', A_NEXT, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'/', A_PUSH, TPS_InFileFirst, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, FILEPATH, None),
];

static actionTPS_InFileNext: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_CLEAR, TPS_InFile, 0, None),
    AI!(Some(p_isdigit), 0, A_CLEAR, TPS_InFile, 0, None),
    AI!(Some(p_iseqC), b'_', A_CLEAR, TPS_InFile, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InURLPathFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isurlchar), 0, A_NEXT, TPS_InURLPath, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InURLPathStart: [TParserStateActionItem; 1] = [
    AI!(None, 0, A_NEXT, TPS_InURLPath, 0, None),
];

static actionTPS_InURLPath: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, URLPATH, None),
    AI!(Some(p_isurlchar), 0, A_NEXT, TPS_InURLPath, 0, None),
    AI!(None, 0, A_BINGO, TPS_Base, URLPATH, None),
];

static actionTPS_InFURL: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isURLPath), 0, A_BINGO | A_CLRALL, TPS_Base, URL_T, Some(SpecialFURL)),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InProtocolFirst: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_InProtocolSecond, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InProtocolSecond: [TParserStateActionItem; 3] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_iseqC), b'/', A_NEXT, TPS_InProtocolEnd, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InProtocolEnd: [TParserStateActionItem; 1] = [
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_Base, PROTOCOL, None),
];

static actionTPS_InHyphenAsciiWordFirst: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHyphenAsciiWord, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWord, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenDigitLookahead, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHyphenAsciiWord: [TParserStateActionItem; 7] = [
    AI!(Some(p_isEOF), 0, A_BINGO | A_CLRALL, TPS_InParseHyphen, ASCIIHWORD, Some(SpecialHyphen)),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHyphenAsciiWord, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenWord, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHyphenAsciiWordFirst, 0, None),
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_InParseHyphen, ASCIIHWORD, Some(SpecialHyphen)),
];

static actionTPS_InHyphenWordFirst: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWord, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenDigitLookahead, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHyphenWord: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_BINGO | A_CLRALL, TPS_InParseHyphen, HWORD, Some(SpecialHyphen)),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenWord, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHyphenWordFirst, 0, None),
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_InParseHyphen, HWORD, Some(SpecialHyphen)),
];

static actionTPS_InHyphenNumWordFirst: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenDigitLookahead, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHyphenNumWord: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_BINGO | A_CLRALL, TPS_InParseHyphen, NUMHWORD, Some(SpecialHyphen)),
    AI!(Some(p_isalnum), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InHyphenNumWordFirst, 0, None),
    AI!(None, 0, A_BINGO | A_CLRALL, TPS_InParseHyphen, NUMHWORD, Some(SpecialHyphen)),
];

static actionTPS_InHyphenDigitLookahead: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenDigitLookahead, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenNumWord, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InParseHyphen: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_RERUN, TPS_Base, 0, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHyphenAsciiWordPart, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWordPart, 0, None),
    AI!(Some(p_isdigit), 0, A_PUSH, TPS_InHyphenUnsignedInt, 0, None),
    AI!(Some(p_iseqC), b'-', A_PUSH, TPS_InParseHyphenHyphen, 0, None),
    AI!(None, 0, A_RERUN, TPS_Base, 0, None),
];

static actionTPS_InParseHyphenHyphen: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isalnum), 0, A_BINGO | A_CLEAR, TPS_InParseHyphen, SPACE, None),
    AI!(Some(p_isspecial), 0, A_BINGO | A_CLEAR, TPS_InParseHyphen, SPACE, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];

static actionTPS_InHyphenWordPart: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, PARTHWORD, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWordPart, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenWordPart, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenNumWordPart, 0, None),
    AI!(None, 0, A_BINGO, TPS_InParseHyphen, PARTHWORD, None),
];

static actionTPS_InHyphenAsciiWordPart: [TParserStateActionItem; 6] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, ASCIIPARTHWORD, None),
    AI!(Some(p_isasclet), 0, A_NEXT, TPS_InHyphenAsciiWordPart, 0, None),
    AI!(Some(p_isalpha), 0, A_NEXT, TPS_InHyphenWordPart, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenWordPart, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_InHyphenNumWordPart, 0, None),
    AI!(None, 0, A_BINGO, TPS_InParseHyphen, ASCIIPARTHWORD, None),
];

static actionTPS_InHyphenNumWordPart: [TParserStateActionItem; 4] = [
    AI!(Some(p_isEOF), 0, A_BINGO, TPS_Base, NUMPARTHWORD, None),
    AI!(Some(p_isalnum), 0, A_NEXT, TPS_InHyphenNumWordPart, 0, None),
    AI!(Some(p_isspecial), 0, A_NEXT, TPS_InHyphenNumWordPart, 0, None),
    AI!(None, 0, A_BINGO, TPS_InParseHyphen, NUMPARTHWORD, None),
];

static actionTPS_InHyphenUnsignedInt: [TParserStateActionItem; 5] = [
    AI!(Some(p_isEOF), 0, A_POP, TPS_Null, 0, None),
    AI!(Some(p_isdigit), 0, A_NEXT, TPS_Null, 0, None),
    AI!(Some(p_isalpha), 0, A_CLEAR, TPS_InHyphenNumWordPart, 0, None),
    AI!(Some(p_isspecial), 0, A_CLEAR, TPS_InHyphenNumWordPart, 0, None),
    AI!(None, 0, A_POP, TPS_Null, 0, None),
];


/*
 * main table of per-state parser actions
 */
#[repr(C)]
struct TParserStateAction {
    action: *const TParserStateActionItem, /* the actual state info */
    state: TParserState,                   /* only for Assert crosscheck */
    // #ifdef WPARSER_TRACE
    //   const char *state_name; /* only for debug printout */
    // #endif
}

/*
 * Static array of raw pointers; mark Sync so it can be a `static`.  The
 * pointees are `static` arrays with 'static lifetime, so the raw pointers are
 * valid for the life of the program.
 */
unsafe impl Sync for TParserStateAction {}

macro_rules! TPARSERSTATEACTION {
    ($state:ident, $action:ident) => {
        TParserStateAction {
            action: $action.as_ptr(),
            state: $state,
        }
    };
}

/*
 * order must be the same as in typedef enum {} TParserState!!
 */

static Actions: [TParserStateAction; 75] = [
    TPARSERSTATEACTION!(TPS_Base, actionTPS_Base),
    TPARSERSTATEACTION!(TPS_InNumWord, actionTPS_InNumWord),
    TPARSERSTATEACTION!(TPS_InAsciiWord, actionTPS_InAsciiWord),
    TPARSERSTATEACTION!(TPS_InWord, actionTPS_InWord),
    TPARSERSTATEACTION!(TPS_InUnsignedInt, actionTPS_InUnsignedInt),
    TPARSERSTATEACTION!(TPS_InSignedIntFirst, actionTPS_InSignedIntFirst),
    TPARSERSTATEACTION!(TPS_InSignedInt, actionTPS_InSignedInt),
    TPARSERSTATEACTION!(TPS_InSpace, actionTPS_InSpace),
    TPARSERSTATEACTION!(TPS_InUDecimalFirst, actionTPS_InUDecimalFirst),
    TPARSERSTATEACTION!(TPS_InUDecimal, actionTPS_InUDecimal),
    TPARSERSTATEACTION!(TPS_InDecimalFirst, actionTPS_InDecimalFirst),
    TPARSERSTATEACTION!(TPS_InDecimal, actionTPS_InDecimal),
    TPARSERSTATEACTION!(TPS_InVerVersion, actionTPS_InVerVersion),
    TPARSERSTATEACTION!(TPS_InSVerVersion, actionTPS_InSVerVersion),
    TPARSERSTATEACTION!(TPS_InVersionFirst, actionTPS_InVersionFirst),
    TPARSERSTATEACTION!(TPS_InVersion, actionTPS_InVersion),
    TPARSERSTATEACTION!(TPS_InMantissaFirst, actionTPS_InMantissaFirst),
    TPARSERSTATEACTION!(TPS_InMantissaSign, actionTPS_InMantissaSign),
    TPARSERSTATEACTION!(TPS_InMantissa, actionTPS_InMantissa),
    TPARSERSTATEACTION!(TPS_InXMLEntityFirst, actionTPS_InXMLEntityFirst),
    TPARSERSTATEACTION!(TPS_InXMLEntity, actionTPS_InXMLEntity),
    TPARSERSTATEACTION!(TPS_InXMLEntityNumFirst, actionTPS_InXMLEntityNumFirst),
    TPARSERSTATEACTION!(TPS_InXMLEntityNum, actionTPS_InXMLEntityNum),
    TPARSERSTATEACTION!(TPS_InXMLEntityHexNumFirst, actionTPS_InXMLEntityHexNumFirst),
    TPARSERSTATEACTION!(TPS_InXMLEntityHexNum, actionTPS_InXMLEntityHexNum),
    TPARSERSTATEACTION!(TPS_InXMLEntityEnd, actionTPS_InXMLEntityEnd),
    TPARSERSTATEACTION!(TPS_InTagFirst, actionTPS_InTagFirst),
    TPARSERSTATEACTION!(TPS_InXMLBegin, actionTPS_InXMLBegin),
    TPARSERSTATEACTION!(TPS_InTagCloseFirst, actionTPS_InTagCloseFirst),
    TPARSERSTATEACTION!(TPS_InTagName, actionTPS_InTagName),
    TPARSERSTATEACTION!(TPS_InTagBeginEnd, actionTPS_InTagBeginEnd),
    TPARSERSTATEACTION!(TPS_InTag, actionTPS_InTag),
    TPARSERSTATEACTION!(TPS_InTagEscapeK, actionTPS_InTagEscapeK),
    TPARSERSTATEACTION!(TPS_InTagEscapeKK, actionTPS_InTagEscapeKK),
    TPARSERSTATEACTION!(TPS_InTagBackSleshed, actionTPS_InTagBackSleshed),
    TPARSERSTATEACTION!(TPS_InTagEnd, actionTPS_InTagEnd),
    TPARSERSTATEACTION!(TPS_InCommentFirst, actionTPS_InCommentFirst),
    TPARSERSTATEACTION!(TPS_InCommentLast, actionTPS_InCommentLast),
    TPARSERSTATEACTION!(TPS_InComment, actionTPS_InComment),
    TPARSERSTATEACTION!(TPS_InCloseCommentFirst, actionTPS_InCloseCommentFirst),
    TPARSERSTATEACTION!(TPS_InCloseCommentLast, actionTPS_InCloseCommentLast),
    TPARSERSTATEACTION!(TPS_InCommentEnd, actionTPS_InCommentEnd),
    TPARSERSTATEACTION!(TPS_InHostFirstDomain, actionTPS_InHostFirstDomain),
    TPARSERSTATEACTION!(TPS_InHostDomainSecond, actionTPS_InHostDomainSecond),
    TPARSERSTATEACTION!(TPS_InHostDomain, actionTPS_InHostDomain),
    TPARSERSTATEACTION!(TPS_InPortFirst, actionTPS_InPortFirst),
    TPARSERSTATEACTION!(TPS_InPort, actionTPS_InPort),
    TPARSERSTATEACTION!(TPS_InHostFirstAN, actionTPS_InHostFirstAN),
    TPARSERSTATEACTION!(TPS_InHost, actionTPS_InHost),
    TPARSERSTATEACTION!(TPS_InEmail, actionTPS_InEmail),
    TPARSERSTATEACTION!(TPS_InFileFirst, actionTPS_InFileFirst),
    TPARSERSTATEACTION!(TPS_InFileTwiddle, actionTPS_InFileTwiddle),
    TPARSERSTATEACTION!(TPS_InPathFirst, actionTPS_InPathFirst),
    TPARSERSTATEACTION!(TPS_InPathFirstFirst, actionTPS_InPathFirstFirst),
    TPARSERSTATEACTION!(TPS_InPathSecond, actionTPS_InPathSecond),
    TPARSERSTATEACTION!(TPS_InFile, actionTPS_InFile),
    TPARSERSTATEACTION!(TPS_InFileNext, actionTPS_InFileNext),
    TPARSERSTATEACTION!(TPS_InURLPathFirst, actionTPS_InURLPathFirst),
    TPARSERSTATEACTION!(TPS_InURLPathStart, actionTPS_InURLPathStart),
    TPARSERSTATEACTION!(TPS_InURLPath, actionTPS_InURLPath),
    TPARSERSTATEACTION!(TPS_InFURL, actionTPS_InFURL),
    TPARSERSTATEACTION!(TPS_InProtocolFirst, actionTPS_InProtocolFirst),
    TPARSERSTATEACTION!(TPS_InProtocolSecond, actionTPS_InProtocolSecond),
    TPARSERSTATEACTION!(TPS_InProtocolEnd, actionTPS_InProtocolEnd),
    TPARSERSTATEACTION!(TPS_InHyphenAsciiWordFirst, actionTPS_InHyphenAsciiWordFirst),
    TPARSERSTATEACTION!(TPS_InHyphenAsciiWord, actionTPS_InHyphenAsciiWord),
    TPARSERSTATEACTION!(TPS_InHyphenWordFirst, actionTPS_InHyphenWordFirst),
    TPARSERSTATEACTION!(TPS_InHyphenWord, actionTPS_InHyphenWord),
    TPARSERSTATEACTION!(TPS_InHyphenNumWordFirst, actionTPS_InHyphenNumWordFirst),
    TPARSERSTATEACTION!(TPS_InHyphenNumWord, actionTPS_InHyphenNumWord),
    TPARSERSTATEACTION!(TPS_InHyphenDigitLookahead, actionTPS_InHyphenDigitLookahead),
    TPARSERSTATEACTION!(TPS_InParseHyphen, actionTPS_InParseHyphen),
    TPARSERSTATEACTION!(TPS_InParseHyphenHyphen, actionTPS_InParseHyphenHyphen),
    TPARSERSTATEACTION!(TPS_InHyphenWordPart, actionTPS_InHyphenWordPart),
    TPARSERSTATEACTION!(TPS_InHyphenAsciiWordPart, actionTPS_InHyphenAsciiWordPart),
    TPARSERSTATEACTION!(TPS_InHyphenNumWordPart, actionTPS_InHyphenNumWordPart),
    TPARSERSTATEACTION!(TPS_InHyphenUnsignedInt, actionTPS_InHyphenUnsignedInt),
];


unsafe fn TParserGet(prs: *mut TParser) -> bool {
    let mut item: *const TParserStateActionItem = core::ptr::null();

    CHECK_FOR_INTERRUPTS!();

    Assert!(!(*prs).state.is_null());

    if (*(*prs).state).posbyte >= (*prs).lenstr {
        return false;
    }

    (*prs).token = (*prs).str.add((*(*prs).state).posbyte as usize);
    (*(*prs).state).pushedAtAction = core::ptr::null();

    /* look at string */
    while (*(*prs).state).posbyte <= (*prs).lenstr {
        if (*(*prs).state).posbyte == (*prs).lenstr {
            (*(*prs).state).charlen = 0;
        } else {
            (*(*prs).state).charlen = if (*prs).charmaxlen == 1 {
                (*prs).charmaxlen
            } else {
                pg_mblen_range(
                    (*prs).str.add((*(*prs).state).posbyte as usize),
                    (*prs).str.add((*prs).lenstr as usize),
                ) as c_int
            };
        }

        Assert!((*(*prs).state).posbyte + (*(*prs).state).charlen <= (*prs).lenstr);
        Assert!((*(*prs).state).state >= TPS_Base && (*(*prs).state).state < TPS_Null);
        Assert!(Actions[(*(*prs).state).state as usize].state == (*(*prs).state).state);

        if !(*(*prs).state).pushedAtAction.is_null() {
            /* After a POP, pick up at the next test */
            item = (*(*prs).state).pushedAtAction.add(1);
            (*(*prs).state).pushedAtAction = core::ptr::null();
        } else {
            item = Actions[(*(*prs).state).state as usize].action;
            Assert!(!item.is_null());
        }

        /* find action by character class */
        while let Some(isclass) = (*item).isclass {
            (*prs).c = (*item).c;
            if isclass(prs) != 0 {
                break;
            }
            item = item.add(1);
        }

        // #ifdef WPARSER_TRACE -- debug printout omitted

        /* call special handler if exists */
        if let Some(special) = (*item).special {
            special(prs);
        }

        /* BINGO, token is found */
        if (*item).flags & A_BINGO != 0 {
            Assert!((*item).r#type > 0);
            (*prs).lenbytetoken = (*(*prs).state).lenbytetoken;
            (*prs).lenchartoken = (*(*prs).state).lenchartoken;
            (*(*prs).state).lenbytetoken = 0;
            (*(*prs).state).lenchartoken = 0;
            (*prs).r#type = (*item).r#type;
        }

        /* do various actions by flags */
        if (*item).flags & A_POP != 0 {
            /* pop stored state in stack */
            let ptr = (*(*prs).state).prev;

            pfree((*prs).state as *mut c_void);
            (*prs).state = ptr;
            Assert!(!(*prs).state.is_null());
        } else if (*item).flags & A_PUSH != 0 {
            /* push (store) state in stack */
            (*(*prs).state).pushedAtAction = item; /* remember where we push */
            (*prs).state = newTParserPosition((*prs).state);
        } else if (*item).flags & A_CLEAR != 0 {
            /* clear previous pushed state */
            Assert!(!(*(*prs).state).prev.is_null());
            let ptr = (*(*(*prs).state).prev).prev;
            pfree((*(*prs).state).prev as *mut c_void);
            (*(*prs).state).prev = ptr;
        } else if (*item).flags & A_CLRALL != 0 {
            /* clear all previous pushed state */
            while !(*(*prs).state).prev.is_null() {
                let ptr = (*(*(*prs).state).prev).prev;
                pfree((*(*prs).state).prev as *mut c_void);
                (*(*prs).state).prev = ptr;
            }
        } else if (*item).flags & A_MERGE != 0 {
            /* merge posinfo with current and pushed state */
            let ptr = (*prs).state;

            Assert!(!(*(*prs).state).prev.is_null());
            (*prs).state = (*(*prs).state).prev;

            (*(*prs).state).posbyte = (*ptr).posbyte;
            (*(*prs).state).poschar = (*ptr).poschar;
            (*(*prs).state).charlen = (*ptr).charlen;
            (*(*prs).state).lenbytetoken = (*ptr).lenbytetoken;
            (*(*prs).state).lenchartoken = (*ptr).lenchartoken;
            pfree(ptr as *mut c_void);
        }

        /* set new state if pointed */
        if (*item).tostate != TPS_Null {
            (*(*prs).state).state = (*item).tostate;
        }

        /* check for go away */
        if (*item).flags & A_BINGO != 0
            || ((*(*prs).state).posbyte >= (*prs).lenstr && (*item).flags & A_RERUN == 0)
        {
            break;
        }

        /* go to beginning of loop if we should rerun or we just restore state */
        if (*item).flags & (A_RERUN | A_POP) != 0 {
            continue;
        }

        /* move forward */
        if (*(*prs).state).charlen != 0 {
            (*(*prs).state).posbyte += (*(*prs).state).charlen;
            (*(*prs).state).lenbytetoken += (*(*prs).state).charlen;
            (*(*prs).state).poschar += 1;
            (*(*prs).state).lenchartoken += 1;
        }
    }

    !item.is_null() && (*item).flags & A_BINGO != 0
}

pub unsafe fn prsd_lextype(_fcinfo: FunctionCallInfo) -> Datum {
    let descr = palloc(core::mem::size_of::<LexDescr>() * (LASTNUM as usize + 1)) as *mut LexDescr;
    let mut i: c_int;

    i = 1;
    while i <= LASTNUM {
        (*descr.add((i - 1) as usize)).lexid = i;
        (*descr.add((i - 1) as usize)).alias = pstrdup(tok_alias[i as usize]);
        (*descr.add((i - 1) as usize)).descr = pstrdup(lex_descr[i as usize]);
        i += 1;
    }

    (*descr.add(LASTNUM as usize)).lexid = 0;

    PG_RETURN_POINTER!(descr);
}

pub unsafe fn prsd_start(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_POINTER!(TParserInit(
        PG_GETARG_POINTER!(fcinfo, 0) as *mut c_char,
        PG_GETARG_INT32!(fcinfo, 1)
    ));
}

pub unsafe fn prsd_nexttoken(fcinfo: FunctionCallInfo) -> Datum {
    let p = PG_GETARG_POINTER!(fcinfo, 0) as *mut TParser;
    let t = PG_GETARG_POINTER!(fcinfo, 1) as *mut *mut c_char;
    let tlen = PG_GETARG_POINTER!(fcinfo, 2) as *mut c_int;

    if !TParserGet(p) {
        PG_RETURN_INT32!(0);
    }

    *t = (*p).token;
    *tlen = (*p).lenbytetoken;

    PG_RETURN_INT32!((*p).r#type);
}

pub unsafe fn prsd_end(fcinfo: FunctionCallInfo) -> Datum {
    let p = PG_GETARG_POINTER!(fcinfo, 0) as *mut TParser;

    TParserClose(p);
    PG_RETURN_VOID!();
}


/*
 * ts_headline support begins here
 */

/* token type classification macros */
#[inline]
fn TS_IDIGNORE(x: c_int) -> bool {
    x == TAG_T || x == PROTOCOL || x == SPACE || x == XMLENTITY
}
#[inline]
fn HLIDREPLACE(x: c_int) -> bool {
    x == TAG_T
}
#[inline]
fn HLIDSKIP(x: c_int) -> bool {
    x == URL_T || x == NUMHWORD || x == ASCIIHWORD || x == HWORD
}
#[inline]
fn XMLHLIDSKIP(x: c_int) -> bool {
    x == URL_T || x == NUMHWORD || x == ASCIIHWORD || x == HWORD
}
#[inline]
fn NONWORDTOKEN(x: c_int) -> bool {
    x == SPACE || HLIDREPLACE(x) || HLIDSKIP(x)
}
#[inline]
fn NOENDTOKEN(x: c_int) -> bool {
    NONWORDTOKEN(x)
        || x == SCIENTIFIC
        || x == VERSIONNUMBER
        || x == DECIMAL_T
        || x == SIGNEDINT
        || x == UNSIGNEDINT
        || TS_IDIGNORE(x)
}

/*
 * Macros useful in headline selection.  These rely on availability of
 * "HeadlineParsedText *prs" describing some text, and "int shortword"
 * describing the "short word" length parameter.
 */

/* Interesting words are non-repeated search terms */
#[inline]
unsafe fn INTERESTINGWORD(prs: *mut HeadlineParsedText, j: c_int) -> bool {
    !(*(*prs).words.add(j as usize)).item.is_null()
        && (*(*prs).words.add(j as usize)).repeated() == 0
}

/* Don't want to end at a non-word or a short word, unless interesting */
#[inline]
unsafe fn BADENDPOINT(prs: *mut HeadlineParsedText, shortword: c_int, j: c_int) -> bool {
    (NOENDTOKEN((*(*prs).words.add(j as usize)).r#type() as c_int)
        || (*(*prs).words.add(j as usize)).len() as c_int <= shortword)
        && !INTERESTINGWORD(prs, j)
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CoverPos {
    /* one cover (well, really one fragment) for mark_hl_fragments */
    startpos: int32, /* fragment's starting word index */
    endpos: int32,   /* ending word index (inclusive) */
    poslen: int32,   /* number of interesting words */
    curlen: int32,   /* total number of words */
    chosen: bool,    /* chosen? */
    excluded: bool,  /* excluded? */
}

#[repr(C)]
struct hlCheck {
    /* callback data for checkcondition_HL */
    words: *mut HeadlineWordEntry,
    len: c_int,
}


/*
 * TS_execute callback for matching a tsquery operand to headline words
 *
 * Note: it's tempting to report words[] indexes as pos values to save
 * searching in hlCover; but that would screw up phrase matching, which
 * expects to measure distances in lexemes not tokens.
 */
unsafe fn checkcondition_HL(
    opaque: *mut c_void,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let checkval = opaque as *mut hlCheck;
    let mut i: c_int;

    /* scan words array for matching items */
    i = 0;
    while i < (*checkval).len {
        if (*(*checkval).words.add(i as usize)).item == val {
            /* if data == NULL, don't need to report positions */
            if data.is_null() {
                return TS_YES;
            }

            if (*data).pos.is_null() {
                (*data).pos = palloc(
                    core::mem::size_of::<WordEntryPos>() * (*checkval).len as usize,
                ) as *mut WordEntryPos;
                (*data).allocated = true;
                (*data).npos = 1;
                *(*data).pos.add(0) = (*(*checkval).words.add(i as usize)).pos;
            } else if *(*data).pos.add(((*data).npos - 1) as usize)
                < (*(*checkval).words.add(i as usize)).pos
            {
                *(*data).pos.add((*data).npos as usize) =
                    (*(*checkval).words.add(i as usize)).pos;
                (*data).npos += 1;
            }
        }
        i += 1;
    }

    if !data.is_null() && (*data).npos > 0 {
        return TS_YES;
    }

    TS_NO
}


/* limits.h INT_MAX */
const INT_MAX: c_int = c_int::MAX;

macro_rules! Max {
    ($a:expr, $b:expr) => {{
        let a = $a;
        let b = $b;
        if a > b {
            a
        } else {
            b
        }
    }};
}

/*
 * hlCover: try to find a substring of prs' word list that satisfies query
 *
 * locations is the result of TS_execute_locations() for the query.
 * We use this to identify plausible subranges of the query.
 *
 * *nextpos is the lexeme position (NOT word index) to start the search
 * at.  Caller should initialize this to zero.  If successful, we'll
 * advance it to the next place to search at.
 *
 * On success, sets *p to first word index and *q to last word index of the
 * cover substring, and returns true.
 *
 * The result is a minimal cover, in the sense that both *p and *q will be
 * words used in the query.
 */
unsafe fn hlCover(
    prs: *mut HeadlineParsedText,
    query: TSQuery,
    locations: *mut List,
    nextpos: *mut c_int,
    p: *mut c_int,
    q: *mut c_int,
) -> bool {
    let mut pos: c_int = *nextpos;

    /* This loop repeats when our selected word-range fails the query */
    loop {
        let posb: c_int;
        let mut pose: c_int;
        /* lc: ListCell* is declared by the foreach! macro below */

        /*
         * For each AND'ed query term or phrase, find its first occurrence at
         * or after pos; set pose to the maximum of those positions.
         *
         * We need not consider ORs or NOTs here; see the comments for
         * TS_execute_locations().  Rechecking the match with TS_execute(),
         * below, will deal with any ensuing imprecision.
         */
        pose = -1;
        foreach!(lc, locations, {
            let pdata = lfirst(current_cell!(lc)) as *mut ExecPhraseData;
            let mut first: c_int = -1;

            let mut i: c_int = 0;
            while i < (*pdata).npos {
                /* For phrase matches, use the ending lexeme */
                let endp = *(*pdata).pos.add(i as usize) as c_int;

                if endp >= pos {
                    first = endp;
                    break;
                }
                i += 1;
            }
            if first < 0 {
                return false; /* no more matches for this term */
            }
            if first > pose {
                pose = first;
            }
        });

        if pose < 0 {
            return false; /* we only get here if empty list */
        }

        /*
         * Now, for each AND'ed query term or phrase, find its last occurrence
         * at or before pose; set posb to the minimum of those positions.
         *
         * We start posb at INT_MAX - 1 to guarantee no overflow if we compute
         * posb + 1 below.
         */
        let mut posb_v: c_int = INT_MAX - 1;
        foreach!(lc, locations, {
            let pdata = lfirst(current_cell!(lc)) as *mut ExecPhraseData;
            let mut last: c_int = -1;

            let mut i: c_int = (*pdata).npos - 1;
            while i >= 0 {
                /* For phrase matches, use the starting lexeme */
                let startp = *(*pdata).pos.add(i as usize) as c_int - (*pdata).width;

                if startp <= pose {
                    last = startp;
                    break;
                }
                i -= 1;
            }
            if last < posb_v {
                posb_v = last;
            }
        });

        /*
         * We could end up with posb to the left of pos, in case some phrase
         * match crosses pos.  Try the match starting at pos anyway, since the
         * result of TS_execute_locations is imprecise for phrase matches OR'd
         * with plain matches; that is, if the query is "(A <-> B) | C" then C
         * could match at pos even though the phrase match would have to
         * extend to the left of pos.
         */
        posb = Max!(posb_v, pos);

        /* This test probably always succeeds, but be paranoid */
        if posb <= pose {
            /*
             * posb .. pose is now the shortest, earliest-after-pos range of
             * lexeme positions containing all the query terms.  It will
             * contain all phrase matches, too, except in the corner case
             * described just above.
             *
             * Now convert these lexeme positions to indexes in prs->words[].
             */
            let mut idxb: c_int = -1;
            let mut idxe: c_int = -1;

            let mut i: c_int = 0;
            while i < (*prs).curwords {
                if (*(*prs).words.add(i as usize)).item.is_null() {
                    i += 1;
                    continue;
                }
                if idxb < 0 && (*(*prs).words.add(i as usize)).pos as c_int >= posb {
                    idxb = i;
                }
                if (*(*prs).words.add(i as usize)).pos as c_int <= pose {
                    idxe = i;
                } else {
                    break;
                }
                i += 1;
            }

            /* This test probably always succeeds, but be paranoid */
            if idxb >= 0 && idxe >= idxb {
                /*
                 * Finally, check that the selected range satisfies the query.
                 * This should succeed in all simple cases; but odd cases
                 * involving non-top-level NOT conditions or phrase matches
                 * OR'd with other things could fail, since the result of
                 * TS_execute_locations doesn't fully represent such things.
                 */
                let mut ch: hlCheck = hlCheck {
                    words: &mut *(*prs).words.add(idxb as usize),
                    len: idxe - idxb + 1,
                };
                if TS_execute(
                    GETQUERY(query),
                    &mut ch as *mut hlCheck as *mut c_void,
                    TS_EXEC_EMPTY,
                    checkcondition_HL,
                ) {
                    /* Match!  Advance *nextpos and return the word range. */
                    *nextpos = posb + 1;
                    *p = idxb;
                    *q = idxe;
                    return true;
                }
            }
        }

        /*
         * Advance pos and try again.  Any later workable match must start
         * beyond posb.
         */
        pos = posb + 1;
    }
    /* Can't get here, but stupider compilers complain if we leave it off */
    #[allow(unreachable_code)]
    false
}

/*
 * Apply suitable highlight marking to words selected by headline selector
 *
 * The words from startpos to endpos inclusive are marked per highlightall
 */
unsafe fn mark_fragment(
    prs: *mut HeadlineParsedText,
    highlightall: bool,
    startpos: c_int,
    endpos: c_int,
) {
    let mut i: c_int;

    i = startpos;
    while i <= endpos {
        if !(*(*prs).words.add(i as usize)).item.is_null() {
            (*(*prs).words.add(i as usize)).set_selected(1);
        }
        if !highlightall {
            if HLIDREPLACE((*(*prs).words.add(i as usize)).r#type() as c_int) {
                (*(*prs).words.add(i as usize)).set_replace(1);
            } else if HLIDSKIP((*(*prs).words.add(i as usize)).r#type() as c_int) {
                (*(*prs).words.add(i as usize)).set_skip(1);
            }
        } else if XMLHLIDSKIP((*(*prs).words.add(i as usize)).r#type() as c_int) {
            (*(*prs).words.add(i as usize)).set_skip(1);
        }

        (*(*prs).words.add(i as usize))
            .set_in(if (*(*prs).words.add(i as usize)).repeated() != 0 { 0 } else { 1 });
        i += 1;
    }
}

/*
 * split a cover substring into fragments not longer than max_words
 *
 * At entry, *startpos and *endpos are the (remaining) bounds of the cover
 * substring.  They are updated to hold the bounds of the next fragment.
 *
 * *curlen and *poslen are set to the fragment's length, in words and
 * interesting words respectively.
 */
unsafe fn get_next_fragment(
    prs: *mut HeadlineParsedText,
    startpos: *mut c_int,
    endpos: *mut c_int,
    curlen: *mut c_int,
    poslen: *mut c_int,
    max_words: c_int,
) {
    let mut i: c_int;

    /*
     * Objective: select a fragment of words between startpos and endpos such
     * that it has at most max_words and both ends have query words. If the
     * startpos and endpos are the endpoints of the cover and the cover has
     * fewer words than max_words, then this function should just return the
     * cover
     */
    /* first move startpos to an item */
    i = *startpos;
    while i <= *endpos {
        *startpos = i;
        if INTERESTINGWORD(prs, i) {
            break;
        }
        i += 1;
    }
    /* cut endpos to have only max_words */
    *curlen = 0;
    *poslen = 0;
    i = *startpos;
    while i <= *endpos && *curlen < max_words {
        if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
            *curlen += 1;
        }
        if INTERESTINGWORD(prs, i) {
            *poslen += 1;
        }
        i += 1;
    }
    /* if the cover was cut then move back endpos to a query item */
    if *endpos > i {
        *endpos = i;
        i = *endpos;
        while i >= *startpos {
            *endpos = i;
            if INTERESTINGWORD(prs, i) {
                break;
            }
            if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                *curlen -= 1;
            }
            i -= 1;
        }
    }
}

/*
 * Headline selector used when MaxFragments > 0
 *
 * Note: in this mode, highlightall is disregarded for phrase selection;
 * it only controls presentation details.
 */
unsafe fn mark_hl_fragments(
    prs: *mut HeadlineParsedText,
    query: TSQuery,
    locations: *mut List,
    highlightall: bool,
    shortword: c_int,
    min_words: c_int,
    max_words: c_int,
    max_fragments: c_int,
) {
    let mut poslen: int32;
    let mut curlen: int32;
    let mut i: int32;
    let mut f: int32;
    let mut num_f: int32 = 0;
    let mut stretch: int32;
    let mut maxstretch: int32;
    let mut posmarker: int32;

    let mut startpos: int32 = 0;
    let mut endpos: int32 = 0;
    let mut nextpos: int32 = 0;
    let mut p: int32 = 0;
    let mut q: int32 = 0;

    let mut numcovers: int32 = 0;
    let mut maxcovers: int32 = 32;

    let mut minI: int32;
    let mut minwords: int32;
    let mut maxitems: int32;
    let mut covers: *mut CoverPos;

    covers = palloc(maxcovers as usize * core::mem::size_of::<CoverPos>()) as *mut CoverPos;

    /* get all covers */
    while hlCover(prs, query, locations, &mut nextpos, &mut p, &mut q) {
        startpos = p;
        endpos = q;

        /*
         * Break the cover into smaller fragments such that each fragment has
         * at most max_words. Also ensure that each end of each fragment is a
         * query word. This will allow us to stretch the fragment in either
         * direction
         */

        while startpos <= endpos {
            get_next_fragment(prs, &mut startpos, &mut endpos, &mut curlen, &mut poslen, max_words);
            if numcovers >= maxcovers {
                maxcovers *= 2;
                covers = repalloc(
                    covers as *mut c_void,
                    core::mem::size_of::<CoverPos>() * maxcovers as usize,
                ) as *mut CoverPos;
            }
            (*covers.add(numcovers as usize)).startpos = startpos;
            (*covers.add(numcovers as usize)).endpos = endpos;
            (*covers.add(numcovers as usize)).curlen = curlen;
            (*covers.add(numcovers as usize)).poslen = poslen;
            (*covers.add(numcovers as usize)).chosen = false;
            (*covers.add(numcovers as usize)).excluded = false;
            numcovers += 1;
            startpos = endpos + 1;
            endpos = q;
        }
    }

    /* choose best covers */
    f = 0;
    while f < max_fragments {
        maxitems = 0;
        minwords = PG_INT32_MAX;
        minI = -1;

        /*
         * Choose the cover that contains max items. In case of tie choose the
         * one with smaller number of words.
         */
        i = 0;
        while i < numcovers {
            if !(*covers.add(i as usize)).chosen
                && !(*covers.add(i as usize)).excluded
                && (maxitems < (*covers.add(i as usize)).poslen
                    || (maxitems == (*covers.add(i as usize)).poslen
                        && minwords > (*covers.add(i as usize)).curlen))
            {
                maxitems = (*covers.add(i as usize)).poslen;
                minwords = (*covers.add(i as usize)).curlen;
                minI = i;
            }
            i += 1;
        }
        /* if a cover was found mark it */
        if minI >= 0 {
            (*covers.add(minI as usize)).chosen = true;
            /* adjust the size of cover */
            startpos = (*covers.add(minI as usize)).startpos;
            endpos = (*covers.add(minI as usize)).endpos;
            curlen = (*covers.add(minI as usize)).curlen;
            /* stretch the cover if cover size is lower than max_words */
            if curlen < max_words {
                /* divide the stretch on both sides of cover */
                maxstretch = (max_words - curlen) / 2;

                /*
                 * first stretch the startpos stop stretching if 1. we hit the
                 * beginning of document 2. exceed maxstretch 3. we hit an
                 * already marked fragment
                 */
                stretch = 0;
                posmarker = startpos;
                i = startpos - 1;
                while i >= 0 && stretch < maxstretch && (*(*prs).words.add(i as usize)).r#in() == 0 {
                    if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                        curlen += 1;
                        stretch += 1;
                    }
                    posmarker = i;
                    i -= 1;
                }
                /* cut back startpos till we find a good endpoint */
                i = posmarker;
                while i < startpos && BADENDPOINT(prs, shortword, i) {
                    if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                        curlen -= 1;
                    }
                    i += 1;
                }
                startpos = i;
                /* now stretch the endpos as much as possible */
                posmarker = endpos;
                i = endpos + 1;
                while i < (*prs).curwords
                    && curlen < max_words
                    && (*(*prs).words.add(i as usize)).r#in() == 0
                {
                    if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                        curlen += 1;
                    }
                    posmarker = i;
                    i += 1;
                }
                /* cut back endpos till we find a good endpoint */
                i = posmarker;
                while i > endpos && BADENDPOINT(prs, shortword, i) {
                    if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                        curlen -= 1;
                    }
                    i -= 1;
                }
                endpos = i;
            }
            (*covers.add(minI as usize)).startpos = startpos;
            (*covers.add(minI as usize)).endpos = endpos;
            (*covers.add(minI as usize)).curlen = curlen;
            /* Mark the chosen fragments (covers) */
            mark_fragment(prs, highlightall, startpos, endpos);
            num_f += 1;
            /* Exclude covers overlapping this one from future consideration */
            i = 0;
            while i < numcovers {
                if i != minI
                    && (((*covers.add(i as usize)).startpos >= startpos
                        && (*covers.add(i as usize)).startpos <= endpos)
                        || ((*covers.add(i as usize)).endpos >= startpos
                            && (*covers.add(i as usize)).endpos <= endpos)
                        || ((*covers.add(i as usize)).startpos < startpos
                            && (*covers.add(i as usize)).endpos > endpos))
                {
                    (*covers.add(i as usize)).excluded = true;
                }
                i += 1;
            }
        } else {
            break; /* no selectable covers remain */
        }
        f += 1;
    }

    /* show the first min_words words if we have not marked anything */
    if num_f <= 0 {
        startpos = 0;
        curlen = 0;
        endpos = -1;
        i = 0;
        while i < (*prs).curwords && curlen < min_words {
            if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                curlen += 1;
            }
            endpos = i;
            i += 1;
        }
        mark_fragment(prs, highlightall, startpos, endpos);
    }

    pfree(covers as *mut c_void);
}

/*
 * Headline selector used when MaxFragments == 0
 */
unsafe fn mark_hl_words(
    prs: *mut HeadlineParsedText,
    query: TSQuery,
    locations: *mut List,
    highlightall: bool,
    shortword: c_int,
    min_words: c_int,
    max_words: c_int,
) {
    let mut nextpos: c_int = 0;
    let mut p: c_int = 0;
    let mut q: c_int = 0;
    let mut bestb: c_int = -1;
    let mut beste: c_int = -1;
    let mut bestlen: c_int = -1;
    let mut bestcover: bool = false;
    let mut pose: c_int;
    let mut posb: c_int;
    let mut poslen: c_int;
    let mut curlen: c_int;
    let mut poscover: bool;
    let mut i: c_int;

    if !highlightall {
        /* examine all covers, select a headline using the best one */
        while hlCover(prs, query, locations, &mut nextpos, &mut p, &mut q) {
            /*
             * Count words (curlen) and interesting words (poslen) within
             * cover, but stop once we reach max_words.  This step doesn't
             * consider whether that's a good stopping point.  posb and pose
             * are set to the start and end indexes of the possible headline.
             */
            curlen = 0;
            poslen = 0;
            posb = p;
            pose = p;
            i = p;
            while i <= q && curlen < max_words {
                if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                    curlen += 1;
                }
                if INTERESTINGWORD(prs, i) {
                    poslen += 1;
                }
                pose = i;
                i += 1;
            }

            if curlen < max_words {
                /*
                 * We have room to lengthen the headline, so search forward
                 * until it's full or we find a good stopping point.  We'll
                 * reconsider the word at "q", then move forward.
                 */
                i = i - 1;
                while i < (*prs).curwords && curlen < max_words {
                    if i > q {
                        if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                            curlen += 1;
                        }
                        if INTERESTINGWORD(prs, i) {
                            poslen += 1;
                        }
                    }
                    pose = i;
                    if BADENDPOINT(prs, shortword, i) {
                        i += 1;
                        continue;
                    }
                    if curlen >= min_words {
                        break;
                    }
                    i += 1;
                }
                if curlen < min_words {
                    /*
                     * Reached end of text and our headline is still shorter
                     * than min_words, so try to extend it to the left.
                     */
                    i = p - 1;
                    while i >= 0 {
                        if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                            curlen += 1;
                        }
                        if INTERESTINGWORD(prs, i) {
                            poslen += 1;
                        }
                        if curlen >= max_words {
                            break;
                        }
                        if BADENDPOINT(prs, shortword, i) {
                            i -= 1;
                            continue;
                        }
                        if curlen >= min_words {
                            break;
                        }
                        i -= 1;
                    }
                    posb = if i >= 0 { i } else { 0 };
                }
            } else {
                /*
                 * Can't make headline longer, so consider making it shorter
                 * if needed to avoid a bad endpoint.
                 */
                if i > q {
                    i = q;
                }
                while curlen > min_words {
                    if !BADENDPOINT(prs, shortword, i) {
                        break;
                    }
                    if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                        curlen -= 1;
                    }
                    if INTERESTINGWORD(prs, i) {
                        poslen -= 1;
                    }
                    pose = i - 1;
                    i -= 1;
                }
            }

            /*
             * Check whether the proposed headline includes the original
             * cover; it might not if we trimmed it due to max_words.
             */
            poscover = posb <= p && pose >= q;

            /*
             * Adopt this headline if it's better than the last one, giving
             * highest priority to headlines including the cover, then to
             * headlines with more interesting words, then to headlines with
             * good stopping points.  (Since bestlen is initially -1, we will
             * certainly adopt the first headline.)
             */
            if poscover as c_int > bestcover as c_int
                || (poscover == bestcover && poslen > bestlen)
                || (poscover == bestcover
                    && poslen == bestlen
                    && !BADENDPOINT(prs, shortword, pose)
                    && BADENDPOINT(prs, shortword, beste))
            {
                bestb = posb;
                beste = pose;
                bestlen = poslen;
                bestcover = poscover;
            }
        }

        /*
         * If we found nothing acceptable, select min_words words starting at
         * the beginning.
         */
        if bestlen < 0 {
            curlen = 0;
            pose = -1;
            i = 0;
            while i < (*prs).curwords && curlen < min_words {
                if !NONWORDTOKEN((*(*prs).words.add(i as usize)).r#type() as c_int) {
                    curlen += 1;
                }
                pose = i;
                i += 1;
            }
            bestb = 0;
            beste = pose;
        }
    } else {
        /* highlightall mode: headline is whole document */
        bestb = 0;
        beste = (*prs).curwords - 1;
    }

    mark_fragment(prs, highlightall, bestb, beste);
}

// PG_GETARG_TSQUERY(n): de-toast the arg datum to a TSQuery.
// TODO(pg-port): real PG_GETARG_TSQUERY macro lives in utils/adt/ts_type.h
unsafe fn PG_GETARG_TSQUERY(_fcinfo: FunctionCallInfo, _n: c_int) -> TSQuery {
    unimplemented!() // TODO(pg-port): utils/adt/ts_type.h
}

/*
 * Default parser's prsheadline function
 */
pub unsafe fn prsd_headline(fcinfo: FunctionCallInfo) -> Datum {
    let prs = PG_GETARG_POINTER!(fcinfo, 0) as *mut HeadlineParsedText;
    let prsoptions = PG_GETARG_POINTER!(fcinfo, 1) as *mut List;
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let locations: *mut List;

    /* default option values: */
    let mut min_words: c_int = 15;
    let mut max_words: c_int = 35;
    let mut shortword: c_int = 3;
    let mut max_fragments: c_int = 0;
    let mut highlightall: bool = false;
    /* l: ListCell* is declared by the foreach! macro below */

    /* Extract configuration option values */
    (*prs).startsel = core::ptr::null_mut();
    (*prs).stopsel = core::ptr::null_mut();
    (*prs).fragdelim = core::ptr::null_mut();
    foreach!(l, prsoptions, {
        let defel = lfirst(current_cell!(l)) as *mut DefElem;
        let val = defGetString(defel);

        if pg_strcasecmp((*defel).defname, c"MaxWords".as_ptr()) == 0 {
            max_words = pg_strtoint32(val);
        } else if pg_strcasecmp((*defel).defname, c"MinWords".as_ptr()) == 0 {
            min_words = pg_strtoint32(val);
        } else if pg_strcasecmp((*defel).defname, c"ShortWord".as_ptr()) == 0 {
            shortword = pg_strtoint32(val);
        } else if pg_strcasecmp((*defel).defname, c"MaxFragments".as_ptr()) == 0 {
            max_fragments = pg_strtoint32(val);
        } else if pg_strcasecmp((*defel).defname, c"StartSel".as_ptr()) == 0 {
            (*prs).startsel = pstrdup(val);
        } else if pg_strcasecmp((*defel).defname, c"StopSel".as_ptr()) == 0 {
            (*prs).stopsel = pstrdup(val);
        } else if pg_strcasecmp((*defel).defname, c"FragmentDelimiter".as_ptr()) == 0 {
            (*prs).fragdelim = pstrdup(val);
        } else if pg_strcasecmp((*defel).defname, c"HighlightAll".as_ptr()) == 0 {
            highlightall = pg_strcasecmp(val, c"1".as_ptr()) == 0
                || pg_strcasecmp(val, c"on".as_ptr()) == 0
                || pg_strcasecmp(val, c"true".as_ptr()) == 0
                || pg_strcasecmp(val, c"t".as_ptr()) == 0
                || pg_strcasecmp(val, c"y".as_ptr()) == 0
                || pg_strcasecmp(val, c"yes".as_ptr()) == 0;
        } else {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized headline parameter: \"{}\"",
                    CStr::from_ptr((*defel).defname).to_string_lossy()
                )
            );
        }
    });

    /* in HighlightAll mode these parameters are ignored */
    if !highlightall {
        if min_words >= max_words {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("{} must be less than {}", "MinWords", "MaxWords"));
        }
        if min_words <= 0 {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("{} must be positive", "MinWords"));
        }
        if shortword < 0 {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("{} must be >= 0", "ShortWord"));
        }
        if max_fragments < 0 {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("{} must be >= 0", "MaxFragments"));
        }
    }

    /* Locate words and phrases matching the query */
    if (*query).size > 0 {
        let mut ch: hlCheck = hlCheck {
            words: (*prs).words,
            len: (*prs).curwords,
        };
        locations = TS_execute_locations(
            GETQUERY(query),
            &mut ch as *mut hlCheck as *mut c_void,
            TS_EXEC_EMPTY,
            checkcondition_HL,
        );
    } else {
        locations = NIL; /* empty query matches nothing */
    }

    /* Apply appropriate headline selector */
    if max_fragments == 0 {
        mark_hl_words(prs, query, locations, highlightall, shortword, min_words, max_words);
    } else {
        mark_hl_fragments(
            prs,
            query,
            locations,
            highlightall,
            shortword,
            min_words,
            max_words,
            max_fragments,
        );
    }

    /* Fill in default values for string options */
    if (*prs).startsel.is_null() {
        (*prs).startsel = pstrdup(c"<b>".as_ptr());
    }
    if (*prs).stopsel.is_null() {
        (*prs).stopsel = pstrdup(c"</b>".as_ptr());
    }
    if (*prs).fragdelim.is_null() {
        (*prs).fragdelim = pstrdup(c" ... ".as_ptr());
    }

    /* Caller will need these lengths, too */
    (*prs).startsellen = strlen((*prs).startsel) as int16;
    (*prs).stopsellen = strlen((*prs).stopsel) as int16;
    (*prs).fragdelimlen = strlen((*prs).fragdelim) as int16;

    PG_RETURN_POINTER!(prs);
}
