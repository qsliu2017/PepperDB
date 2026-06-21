//! src/backend/parser/parser.c
//!
//! parser.c
//!		Main entry point/driver for PostgreSQL grammar
//!
//! Note that the grammar is not allowed to perform any table access
//! (since we need to be able to do basic parsing even while inside an
//! aborted transaction).  Therefore, the data structures returned by
//! the grammar are "raw" parsetrees that still need to be analyzed by
//! analyze.c and related files.
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/parser/parser.c
//!
//! Companion header: src/include/parser/parser.h

use crate::prelude::*;
use crate::nodes::pg_list::{List, NIL};

use std::ffi::{c_char, c_int};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn isxdigit(c: c_int) -> c_int;
}

// ----------------------------------------------------------------
// parser.h definitions
// ----------------------------------------------------------------

/*
 * RawParseMode determines the form of the string that raw_parser() accepts:
 *
 * RAW_PARSE_DEFAULT: parse a semicolon-separated list of SQL commands,
 * and return a List of RawStmt nodes.
 *
 * RAW_PARSE_TYPE_NAME: parse a type name, and return a one-element List
 * containing a TypeName node.
 *
 * RAW_PARSE_PLPGSQL_EXPR: parse a PL/pgSQL expression, and return
 * a one-element List containing a RawStmt node.
 *
 * RAW_PARSE_PLPGSQL_ASSIGNn: parse a PL/pgSQL assignment statement,
 * and return a one-element List containing a RawStmt node.  "n"
 * gives the number of dotted names comprising the target ColumnRef.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RawParseMode {
    RAW_PARSE_DEFAULT = 0,
    RAW_PARSE_TYPE_NAME,
    RAW_PARSE_PLPGSQL_EXPR,
    RAW_PARSE_PLPGSQL_ASSIGN1,
    RAW_PARSE_PLPGSQL_ASSIGN2,
    RAW_PARSE_PLPGSQL_ASSIGN3,
}
pub use RawParseMode::*;

/* Values for the backslash_quote GUC */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BackslashQuoteType {
    BACKSLASH_QUOTE_OFF,
    BACKSLASH_QUOTE_ON,
    BACKSLASH_QUOTE_SAFE_ENCODING,
}

/* GUC variables in scan.l (every one of these is a bad idea :-() */
pub static mut backslash_quote: c_int = 0;
pub static mut escape_string_warning: bool = false;
pub static mut standard_conforming_strings: bool = false;

// ----------------------------------------------------------------
// Types and externals pulled from gramparse.h / scan.l / mb / scansup
// ----------------------------------------------------------------

pub type core_yyscan_t = *mut c_void;
pub type pg_wchar = u32;

// YYSTYPE / YYLTYPE / core_YYSTYPE are produced by bison/flex; stub them.
pub type YYLTYPE = c_int;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct core_YYSTYPE {
    pub str: *mut c_char,
    // (union of many fields in C; we only use .str here)
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct YYSTYPE {
    pub core_yystype: core_YYSTYPE,
}

// core_yy_extra_type and base_yy_extra_type from gramparse.h
#[repr(C)]
pub struct core_yy_extra_type {
    pub scanbuf: *mut c_char,
    // ... other fields
}

#[repr(C)]
pub struct base_yy_extra_type {
    /*
     * If this is true, the value of lookahead_token has been determined and
     * the next call to base_yylex should return lookahead_token.
     */
    pub have_lookahead: bool,
    /* Type of the lookahead token, if have_lookahead is true */
    pub lookahead_token: c_int,
    /* Semantic value (lval) for the lookahead token */
    pub lookahead_yylval: core_YYSTYPE,
    /* Location (lloc) for the lookahead token */
    pub lookahead_yylloc: YYLTYPE,
    /* Saved character previously held in lookahead_end position */
    pub lookahead_hold_char: c_char,
    /* Where lookahead token ended (NULL if no lookahead) */
    pub lookahead_end: *mut c_char,

    /* state variables for base_yylex() */
    pub core_yy_extra: core_yy_extra_type,

    /* parse workspace */
    pub parsetree: *mut List, /* final parse result is delivered here */
}

#[repr(C)]
pub struct ScannerCallbackState {
    // opaque; defined in scansup.h
    _opaque: [u8; 0],
}

// Token codes from gram.h (bison). Stub as constants.
const FORMAT: c_int = 1;
const NOT: c_int = 2;
const NULLS_P: c_int = 3;
const WITH: c_int = 4;
const UIDENT: c_int = 5;
const USCONST: c_int = 6;
const WITHOUT: c_int = 7;
const JSON: c_int = 8;
const FORMAT_LA: c_int = 9;
const BETWEEN: c_int = 10;
const IN_P: c_int = 11;
const LIKE: c_int = 12;
const ILIKE: c_int = 13;
const SIMILAR: c_int = 14;
const NOT_LA: c_int = 15;
const FIRST_P: c_int = 16;
const LAST_P: c_int = 17;
const NULLS_LA: c_int = 18;
const TIME: c_int = 19;
const ORDINALITY: c_int = 20;
const WITH_LA: c_int = 21;
const WITHOUT_LA: c_int = 22;
const UESCAPE: c_int = 23;
const SCONST: c_int = 24;
const IDENT: c_int = 25;

// Parse-mode pseudo-tokens (from gram.y)
const MODE_TYPE_NAME: c_int = 100;
const MODE_PLPGSQL_EXPR: c_int = 101;
const MODE_PLPGSQL_ASSIGN1: c_int = 102;
const MODE_PLPGSQL_ASSIGN2: c_int = 103;
const MODE_PLPGSQL_ASSIGN3: c_int = 104;

// From mb/pg_wchar.h
const MAX_UNICODE_EQUIVALENT_STRING: usize = 16;

// errcode constants
const ERRCODE_SYNTAX_ERROR: c_int = 0;

// ScanKeywords / ScanKeywordTokens are defined elsewhere (kwlist).
extern "C" {
    static ScanKeywords: c_void;
    static ScanKeywordTokens: c_void;
}

// ----------------------------------------------------------------
// Local stubs for unported helpers
// ----------------------------------------------------------------

unsafe fn scanner_init(
    _str: *const c_char,
    _yyext: *mut core_yy_extra_type,
    _keywords: *const c_void,
    _keyword_tokens: *const c_void,
) -> core_yyscan_t {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn scanner_finish(_yyscanner: core_yyscan_t) {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn core_yylex(
    _lvalp: *mut core_YYSTYPE,
    _llocp: *mut YYLTYPE,
    _yyscanner: core_yyscan_t,
) -> c_int {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn scanner_yyerror(_message: *const c_char, _yyscanner: core_yyscan_t) {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn scanner_isspace(_ch: c_char) -> bool {
    crate::parser::scansup::scanner_isspace(_ch)
}

unsafe fn scanner_errposition(_location: c_int, _yyscanner: core_yyscan_t) -> c_int {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn setup_scanner_errposition_callback(
    _scbstate: *mut ScannerCallbackState,
    _yyscanner: core_yyscan_t,
    _location: c_int,
) {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn cancel_scanner_errposition_callback(_scbstate: *mut ScannerCallbackState) {
    unimplemented!() // TODO: backend/parser/scan.l
}

unsafe fn truncate_identifier(_ident: *mut c_char, _len: c_int, _warn: bool) {
    crate::parser::scansup::truncate_identifier(_ident as _, _len, _warn)
}

unsafe fn parser_init(_yyext: *mut base_yy_extra_type) {
    unimplemented!() // TODO: backend/parser/gram.y
}

unsafe fn base_yyparse(_yyscanner: core_yyscan_t) -> c_int {
    unimplemented!() // TODO: backend/parser/gram.y
}

unsafe fn pg_yyget_extra(_yyscanner: core_yyscan_t) -> *mut base_yy_extra_type {
    unimplemented!() // TODO: backend/parser/scan.l (flex generated)
}

unsafe fn is_valid_unicode_codepoint(_c: pg_wchar) -> bool {
    crate::mb::wchar::is_valid_unicode_codepoint(_c as _)
}

unsafe fn is_utf16_surrogate_first(_c: pg_wchar) -> bool {
    crate::mb::wchar::is_utf16_surrogate_first(_c as _)
}

unsafe fn is_utf16_surrogate_second(_c: pg_wchar) -> bool {
    crate::mb::wchar::is_utf16_surrogate_second(_c as _)
}

unsafe fn surrogate_pair_to_codepoint(_first: pg_wchar, _second: pg_wchar) -> pg_wchar {
    crate::mb::wchar::surrogate_pair_to_codepoint(_first as _, _second as _) as _
}

unsafe fn pg_unicode_to_server(_c: pg_wchar, _s: *mut u8) {
    crate::utils::mb::mbutils::pg_unicode_to_server(_c as _, _s as _)
}

// ----------------------------------------------------------------
// parser.c body
// ----------------------------------------------------------------

/*
 * raw_parser
 *		Given a query in string form, do lexical and grammatical analysis.
 *
 * Returns a list of raw (un-analyzed) parse trees.  The contents of the
 * list have the form required by the specified RawParseMode.
 */
pub unsafe fn raw_parser(str: *const c_char, mode: RawParseMode) -> *mut List {
    // The flex/bison-generated scanner+grammar (scan.c/gram.c/parser.c) are linked
    // from C via build.rs; forward to the real C entry point.
    extern "C" {
        #[link_name = "raw_parser"]
        fn c_raw_parser(str: *const c_char, mode: c_int) -> *mut List;
    }
    c_raw_parser(str, mode as c_int)
}

/*
 * Intermediate filter between parser and core lexer (core_yylex in scan.l).
 *
 * This filter is needed because in some cases the standard SQL grammar
 * requires more than one token lookahead.  We reduce these cases to one-token
 * lookahead by replacing tokens here, in order to keep the grammar LALR(1).
 *
 * Using a filter is simpler than trying to recognize multiword tokens
 * directly in scan.l, because we'd have to allow for comments between the
 * words.  Furthermore it's not clear how to do that without re-introducing
 * scanner backtrack, which would cost more performance than this filter
 * layer does.
 *
 * We also use this filter to convert UIDENT and USCONST sequences into
 * plain IDENT and SCONST tokens.  While that could be handled by additional
 * productions in the main grammar, it's more efficient to do it like this.
 *
 * The filter also provides a convenient place to translate between
 * the core_YYSTYPE and YYSTYPE representations (which are really the
 * same thing anyway, but notationally they're different).
 */
pub unsafe fn base_yylex(
    lvalp: *mut YYSTYPE,
    llocp: *mut YYLTYPE,
    yyscanner: core_yyscan_t,
) -> c_int {
    let yyextra: *mut base_yy_extra_type = pg_yyget_extra(yyscanner);
    let mut cur_token: c_int;
    let mut next_token: c_int;
    let cur_token_length: c_int;
    let mut cur_yylloc: YYLTYPE;

    /* Get next token --- we might already have it */
    if (*yyextra).have_lookahead {
        cur_token = (*yyextra).lookahead_token;
        (*lvalp).core_yystype = (*yyextra).lookahead_yylval;
        *llocp = (*yyextra).lookahead_yylloc;
        if !(*yyextra).lookahead_end.is_null() {
            *(*yyextra).lookahead_end = (*yyextra).lookahead_hold_char;
        }
        (*yyextra).have_lookahead = false;
    } else {
        cur_token = core_yylex(&mut (*lvalp).core_yystype, llocp, yyscanner);
    }

    /*
     * If this token isn't one that requires lookahead, just return it.  If it
     * does, determine the token length.  (We could get that via strlen(), but
     * since we have such a small set of possibilities, hardwiring seems
     * feasible and more efficient --- at least for the fixed-length cases.)
     */
    if cur_token == FORMAT {
        cur_token_length = 6;
    } else if cur_token == NOT {
        cur_token_length = 3;
    } else if cur_token == NULLS_P {
        cur_token_length = 5;
    } else if cur_token == WITH {
        cur_token_length = 4;
    } else if cur_token == UIDENT || cur_token == USCONST {
        cur_token_length =
            strlen((*yyextra).core_yy_extra.scanbuf.offset(*llocp as isize)) as c_int;
    } else if cur_token == WITHOUT {
        cur_token_length = 7;
    } else {
        return cur_token;
    }

    /*
     * Identify end+1 of current token.  core_yylex() has temporarily stored a
     * '\0' here, and will undo that when we call it again.  We need to redo
     * it to fully revert the lookahead call for error reporting purposes.
     */
    (*yyextra).lookahead_end = (*yyextra)
        .core_yy_extra
        .scanbuf
        .offset((*llocp + cur_token_length) as isize);
    Assert!(*(*yyextra).lookahead_end == 0);

    /*
     * Save and restore *llocp around the call.  It might look like we could
     * avoid this by just passing &lookahead_yylloc to core_yylex(), but that
     * does not work because flex actually holds onto the last-passed pointer
     * internally, and will use that for error reporting.  We need any error
     * reports to point to the current token, not the next one.
     */
    cur_yylloc = *llocp;

    /* Get next token, saving outputs into lookahead variables */
    next_token = core_yylex(&mut (*yyextra).lookahead_yylval, llocp, yyscanner);
    (*yyextra).lookahead_token = next_token;
    (*yyextra).lookahead_yylloc = *llocp;

    *llocp = cur_yylloc;

    /* Now revert the un-truncation of the current token */
    (*yyextra).lookahead_hold_char = *(*yyextra).lookahead_end;
    *(*yyextra).lookahead_end = 0;

    (*yyextra).have_lookahead = true;

    /* Replace cur_token if needed, based on lookahead */
    if cur_token == FORMAT {
        /* Replace FORMAT by FORMAT_LA if it's followed by JSON */
        if next_token == JSON {
            cur_token = FORMAT_LA;
        }
    } else if cur_token == NOT {
        /* Replace NOT by NOT_LA if it's followed by BETWEEN, IN, etc */
        match next_token {
            x if x == BETWEEN || x == IN_P || x == LIKE || x == ILIKE || x == SIMILAR => {
                cur_token = NOT_LA;
            }
            _ => {}
        }
    } else if cur_token == NULLS_P {
        /* Replace NULLS_P by NULLS_LA if it's followed by FIRST or LAST */
        if next_token == FIRST_P || next_token == LAST_P {
            cur_token = NULLS_LA;
        }
    } else if cur_token == WITH {
        /* Replace WITH by WITH_LA if it's followed by TIME or ORDINALITY */
        if next_token == TIME || next_token == ORDINALITY {
            cur_token = WITH_LA;
        }
    } else if cur_token == WITHOUT {
        /* Replace WITHOUT by WITHOUT_LA if it's followed by TIME */
        if next_token == TIME {
            cur_token = WITHOUT_LA;
        }
    } else if cur_token == UIDENT || cur_token == USCONST {
        /* Look ahead for UESCAPE */
        if next_token == UESCAPE {
            /* Yup, so get third token, which had better be SCONST */
            let escstr: *const c_char;

            /* Again save and restore *llocp */
            cur_yylloc = *llocp;

            /* Un-truncate current token so errors point to third token */
            *(*yyextra).lookahead_end = (*yyextra).lookahead_hold_char;

            /* Get third token */
            next_token = core_yylex(&mut (*yyextra).lookahead_yylval, llocp, yyscanner);

            /* If we throw error here, it will point to third token */
            if next_token != SCONST {
                scanner_yyerror(
                    c"UESCAPE must be followed by a simple string literal".as_ptr(),
                    yyscanner,
                );
            }

            escstr = (*yyextra).lookahead_yylval.str;
            if strlen(escstr) != 1 || !check_uescapechar(*escstr as u8) {
                scanner_yyerror(c"invalid Unicode escape character".as_ptr(), yyscanner);
            }

            /* Now restore *llocp; errors will point to first token */
            *llocp = cur_yylloc;

            /* Apply Unicode conversion */
            (*lvalp).core_yystype.str =
                str_udeescape((*lvalp).core_yystype.str, *escstr, *llocp, yyscanner);

            /*
             * We don't need to revert the un-truncation of UESCAPE.  What
             * we do want to do is clear have_lookahead, thereby consuming
             * all three tokens.
             */
            (*yyextra).have_lookahead = false;
        } else {
            /* No UESCAPE, so convert using default escape character */
            (*lvalp).core_yystype.str =
                str_udeescape((*lvalp).core_yystype.str, b'\\' as c_char, *llocp, yyscanner);
        }

        if cur_token == UIDENT {
            /* It's an identifier, so truncate as appropriate */
            truncate_identifier(
                (*lvalp).core_yystype.str,
                strlen((*lvalp).core_yystype.str) as c_int,
                true,
            );
            cur_token = IDENT;
        } else if cur_token == USCONST {
            cur_token = SCONST;
        }
    }

    cur_token
}

/* convert hex digit (caller should have verified that) to value */
unsafe fn hexval(c: u8) -> u32 {
    if c >= b'0' && c <= b'9' {
        return (c - b'0') as u32;
    }
    if c >= b'a' && c <= b'f' {
        return (c - b'a') as u32 + 0xA;
    }
    if c >= b'A' && c <= b'F' {
        return (c - b'A') as u32 + 0xA;
    }
    elog!(ERROR, "invalid hexadecimal digit");
    0 /* not reached */
}

/* is Unicode code point acceptable? */
unsafe fn check_unicode_value(c: pg_wchar) {
    if !is_valid_unicode_codepoint(c) {
        ereport!(ERROR, "invalid Unicode escape value");
        unreachable!()
    }
}

/* is 'escape' acceptable as Unicode escape character (UESCAPE syntax) ? */
unsafe fn check_uescapechar(escape: u8) -> bool {
    if isxdigit(escape as c_int) != 0
        || escape == b'+'
        || escape == b'\''
        || escape == b'"'
        || scanner_isspace(escape as c_char)
    {
        false
    } else {
        true
    }
}

/*
 * Process Unicode escapes in "str", producing a palloc'd plain string
 *
 * escape: the escape character to use
 * position: start position of U&'' or U&"" string token
 * yyscanner: context information needed for error reports
 */
unsafe fn str_udeescape(
    str: *const c_char,
    escape: c_char,
    position: c_int,
    yyscanner: core_yyscan_t,
) -> *mut c_char {
    let mut in_: *const c_char;
    let mut new: *mut c_char;
    let mut out: *mut c_char;
    let mut new_len: usize;
    let mut pair_first: pg_wchar = 0;
    let mut scbstate: ScannerCallbackState = std::mem::zeroed();

    /*
     * Guesstimate that result will be no longer than input, but allow enough
     * padding for Unicode conversion.
     */
    new_len = strlen(str) + MAX_UNICODE_EQUIVALENT_STRING + 1;
    new = palloc(new_len) as *mut c_char;

    in_ = str;
    out = new;

    let escape_u = escape as u8;

    'outer: loop {
        if *in_ == 0 {
            break;
        }

        /* Enlarge string if needed */
        let out_dist = out.offset_from(new) as usize;

        if out_dist > new_len - (MAX_UNICODE_EQUIVALENT_STRING + 1) {
            new_len *= 2;
            new = repalloc(new as *mut c_void, new_len) as *mut c_char;
            out = new.add(out_dist);
        }

        if *in_.offset(0) as u8 == escape_u {
            /*
             * Any errors reported while processing this escape sequence will
             * have an error cursor pointing at the escape.
             */
            setup_scanner_errposition_callback(
                &mut scbstate,
                yyscanner,
                (in_.offset_from(str) as c_int) + position + 3, /* 3 for U&" */
            );
            if *in_.offset(1) as u8 == escape_u {
                if pair_first != 0 {
                    /* goto invalid_pair */
                    break 'outer;
                }
                *out = escape;
                out = out.add(1);
                in_ = in_.add(2);
            } else if isxdigit(*in_.offset(1) as u8 as c_int) != 0
                && isxdigit(*in_.offset(2) as u8 as c_int) != 0
                && isxdigit(*in_.offset(3) as u8 as c_int) != 0
                && isxdigit(*in_.offset(4) as u8 as c_int) != 0
            {
                let mut unicode: pg_wchar;

                unicode = (hexval(*in_.offset(1) as u8) << 12)
                    + (hexval(*in_.offset(2) as u8) << 8)
                    + (hexval(*in_.offset(3) as u8) << 4)
                    + hexval(*in_.offset(4) as u8);
                check_unicode_value(unicode);
                if pair_first != 0 {
                    if is_utf16_surrogate_second(unicode) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        /* goto invalid_pair */
                        break 'outer;
                    }
                } else if is_utf16_surrogate_second(unicode) {
                    /* goto invalid_pair */
                    break 'outer;
                }

                if is_utf16_surrogate_first(unicode) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, out as *mut u8);
                    out = out.add(strlen(out));
                }
                in_ = in_.add(5);
            } else if *in_.offset(1) as u8 == b'+'
                && isxdigit(*in_.offset(2) as u8 as c_int) != 0
                && isxdigit(*in_.offset(3) as u8 as c_int) != 0
                && isxdigit(*in_.offset(4) as u8 as c_int) != 0
                && isxdigit(*in_.offset(5) as u8 as c_int) != 0
                && isxdigit(*in_.offset(6) as u8 as c_int) != 0
                && isxdigit(*in_.offset(7) as u8 as c_int) != 0
            {
                let mut unicode: pg_wchar;

                unicode = (hexval(*in_.offset(2) as u8) << 20)
                    + (hexval(*in_.offset(3) as u8) << 16)
                    + (hexval(*in_.offset(4) as u8) << 12)
                    + (hexval(*in_.offset(5) as u8) << 8)
                    + (hexval(*in_.offset(6) as u8) << 4)
                    + hexval(*in_.offset(7) as u8);
                check_unicode_value(unicode);
                if pair_first != 0 {
                    if is_utf16_surrogate_second(unicode) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        /* goto invalid_pair */
                        break 'outer;
                    }
                } else if is_utf16_surrogate_second(unicode) {
                    /* goto invalid_pair */
                    break 'outer;
                }

                if is_utf16_surrogate_first(unicode) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, out as *mut u8);
                    out = out.add(strlen(out));
                }
                in_ = in_.add(8);
            } else {
                ereport!(ERROR, "invalid Unicode escape");
                unreachable!()
            }

            cancel_scanner_errposition_callback(&mut scbstate);
        } else {
            if pair_first != 0 {
                /* goto invalid_pair */
                break 'outer;
            }

            *out = *in_;
            out = out.add(1);
            in_ = in_.add(1);
        }
    }

    /*
     * We arrive here either by falling out of the loop normally (in_ reached
     * '\0') or via a `break 'outer` which corresponds to `goto invalid_pair`.
     * Distinguish the two by re-checking the conditions that caused a jump.
     */
    if *in_ != 0 || pair_first != 0 {
        /* invalid_pair: */
        /*
         * We might get here with the error callback active, or not.  Call
         * scanner_errposition to make sure an error cursor appears; if the
         * callback is active, this is duplicative but harmless.
         */
        scanner_errposition(
            (in_.offset_from(str) as c_int) + position + 3, /* 3 for U&" */
            yyscanner,
        );
        ereport!(ERROR, "invalid Unicode surrogate pair");
        unreachable!()
    }

    /* unfinished surrogate pair? (already handled above, but kept for clarity) */

    *out = 0;
    new
}
