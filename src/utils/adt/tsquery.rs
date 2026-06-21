//! tsquery.rs
//!   I/O functions for tsquery
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/tsquery.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;

use crate::{
    current_cell, foreach, IsA, PG_FREE_IF_COPY, PG_GETARG_CSTRING, PG_GETARG_POINTER,
    PG_GETARG_TSQUERY, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_POINTER, PG_RETURN_TEXT_P,
    PG_RETURN_TSQUERY, SOFT_ERROR_OCCURRED,
};

use std::ffi::CStr;

use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_getmsgstring, pq_sendint16, pq_sendint32,
    pq_sendint8, pq_sendstring,
};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::mb::mbutils::{pg_database_encoding_max_length, pg_mblen_cstr};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{lcons, lfirst, list_length, List, ListCell, NIL};
use crate::port::pgstrcasecmp::pg_strncasecmp;
use crate::tsearch::ts_locale::{t_isalnum_cstr, t_iseq, ts_copychar_cstr};
use crate::utils::adt::tsquery_cleanup::{clean_NOT, cleanup_tsquery_stopwords};
use crate::utils::adt::tsvector_parser::{
    close_tsvector_parser, gettoken_tsvector, init_tsvector_parser, reset_tsvector_parser,
    ISOPERATOR, TSVectorParseState, P_TSV_IS_TSQUERY, P_TSV_IS_WEB, P_TSV_OPR_IS_DELIM,
};
use crate::utils::adt::ts_type::{
    QueryItem, QueryOperand, QueryOperator, COMPUTESIZE, GETOPERAND, GETQUERY, HDRSIZETQ,
    MAXENTRYPOS, MAXSTRLEN, MAXSTRPOS, OP_AND, OP_COUNT, OP_NOT, OP_OR, OP_PHRASE, OP_PRIORITY,
    QI_OPR, QI_VAL, QI_VALSTOP, QO_PRIORITY, TSQuery, TSQueryData, TSQUERY_TOO_BIG,
};
use crate::utils::adt::varlena::cstring_to_text;
use crate::utils::builtins::cstring_to_text_with_len;
// `text` is the varlena alias from c.h, in scope via the prelude (crate::c::*).
use crate::utils::hash::pg_crc::{
    pg_crc32, COMP_LEGACY_CRC32, FIN_LEGACY_CRC32, INIT_LEGACY_CRC32,
};
use crate::utils::memutils::MaxAllocSize;
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::varatt::SET_VARSIZE;

// TODO(pg-port): ERRCODE_* live in the generated utils/errcodes.h, not yet
// translated.  errcode() is a no-op shim (it ignores the value), so these
// placeholder constants keep the call sites textually faithful.
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

/*
 * ereturn(escontext, dummy, ...): soft-error shim -> ereport!(ERROR, ...).
 * The elog shim always raises at ERROR (errcode/errdetail dropped per porting
 * convention), so control never returns; the dummy mirrors the C convenience
 * return value.  Defined textually before first use (macro_rules! is not
 * hoisted).
 */
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{
        let __ctx = $escontext as *mut Node;
        if SOFT_ERROR_FLAG(__ctx) {
            return $dummy;
        }
        ereport!(ERROR, $($arg)*);
        #[allow(unreachable_code)]
        return $dummy;
    }};
}

/*
 * errsave(escontext, ...): record a soft error into a real ErrorSaveContext and
 * continue; otherwise raise a hard ERROR.
 */
macro_rules! errsave {
    ($escontext:expr, $($arg:tt)*) => {{
        let __ctx = $escontext as *mut Node;
        if !SOFT_ERROR_FLAG(__ctx) {
            ereport!(ERROR, $($arg)*);
        }
    }};
}

/*
 * SOFT_ERROR_FLAG: if `escontext` is a real ErrorSaveContext, record that a soft
 * error occurred and return true; otherwise return false.
 */
#[inline]
unsafe fn SOFT_ERROR_FLAG(escontext: *mut Node) -> bool {
    const T_ErrorSaveContext: c_int = 447;
    if !escontext.is_null() && *(escontext as *const c_int) == T_ErrorSaveContext {
        (*(escontext as *mut crate::nodes::miscnodes::ErrorSaveContext)).error_occurred = true;
        return true;
    }
    false
}

/*
 * SOFT_ERROR_OCCURRED(escontext): with the elog shim always raising at ERROR,
 * control never returns from a soft-error site, so this is effectively always
 * false here.  Mirror crate::nodes::miscnodes semantics.
 */
#[inline]
unsafe fn SOFT_ERROR_OCCURRED(escontext: *mut Node) -> bool {
    crate::SOFT_ERROR_OCCURRED!(escontext)
}

/*
 * cstr(p): borrow a *const c_char as a Rust string for use in error messages.
 */
#[inline]
unsafe fn cstr(p: *const c_char) -> std::borrow::Cow<'static, str> {
    CStr::from_ptr(p).to_string_lossy()
}

/* FTS operator priorities, see ts_type.h */
#[no_mangle]
pub static tsearch_op_priority: [c_int; OP_COUNT] = [
    4, /* OP_NOT */
    2, /* OP_AND */
    1, /* OP_OR */
    3, /* OP_PHRASE */
];

/*
 * parser's states
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ts_parserstate {
    WAITOPERAND = 1,
    WAITOPERATOR = 2,
    WAITFIRSTOPERAND = 3,
}
use ts_parserstate::*;

/*
 * token types for parsing
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ts_tokentype {
    PT_END = 0,
    PT_ERR = 1,
    PT_VAL = 2,
    PT_OPR = 3,
    PT_OPEN = 4,
    PT_CLOSE = 5,
}
use ts_tokentype::*;

/*
 * get token from query string
 *
 * All arguments except "state" are output arguments.
 *
 * If return value is PT_OPR, then *operator is filled with an OP_* code
 * and *weight will contain a distance value in case of phrase operator.
 *
 * If return value is PT_VAL, then *lenval, *strval, *weight, and *prefix
 * are filled.
 *
 * If PT_ERR is returned then a soft error has occurred.  If state->escontext
 * isn't already filled then this should be reported as a generic parse error.
 */
pub type ts_tokenizer = unsafe fn(
    state: TSQueryParserState,
    operator: *mut int8,
    lenval: *mut c_int,
    strval: *mut *mut c_char,
    weight: *mut int16,
    prefix: *mut bool,
) -> ts_tokentype;

/*
 * PushFunction - callback invoked by parse_tsquery for each operand value.
 * (ts_utils.h: typedef void (*PushFunction)(Datum opaque, TSQueryParserState
 * state, char *token, int tokenlen, int16 tokenweights, bool prefix);)
 */
pub type PushFunction = unsafe fn(
    opaque: Datum,
    state: TSQueryParserState,
    token: *mut c_char,
    tokenlen: c_int,
    tokenweights: int16,
    prefix: bool,
);

/* P_TSQ_* flags (ts_utils.h) */
pub const P_TSQ_PLAIN: c_int = 0x0001;
pub const P_TSQ_WEB: c_int = 0x0002;

#[repr(C)]
pub struct TSQueryParserStateData {
    /* Tokenizer used for parsing tsquery */
    pub gettoken: ts_tokenizer,

    /* State of tokenizer function */
    pub buffer: *mut c_char, /* entire string we are scanning */
    pub buf: *mut c_char,    /* current scan point */
    pub count: c_int,        /* nesting count, incremented by (,
                              * decremented by ) */
    pub state: ts_parserstate,

    /* polish (prefix) notation in list, filled in by push* functions */
    pub polstr: *mut List,

    /*
     * Strings from operands are collected in op. curop is a pointer to the
     * end of used space of op.
     */
    pub op: *mut c_char,
    pub curop: *mut c_char,
    pub lenop: c_int,  /* allocated size of op */
    pub sumlen: c_int, /* used size of op */

    /* state for value's parser */
    pub valstate: TSVectorParseState,

    /* context object for soft errors - must match valstate's escontext */
    pub escontext: *mut Node,
}

pub type TSQueryParserState = *mut TSQueryParserStateData;

/*
 * subroutine to parse the modifiers (weight and prefix flag currently)
 * part, like ':AB*' of a query.
 */
unsafe fn get_modifiers(mut buf: *mut c_char, weight: *mut int16, prefix: *mut bool) -> *mut c_char {
    *weight = 0;
    *prefix = false;

    if !t_iseq(buf, b':' as c_char) {
        return buf;
    }

    buf = buf.add(1);
    while *buf != 0 && pg_mblen_cstr(buf) == 1 {
        match *buf as u8 {
            b'a' | b'A' => {
                *weight |= 1 << 3;
            }
            b'b' | b'B' => {
                *weight |= 1 << 2;
            }
            b'c' | b'C' => {
                *weight |= 1 << 1;
            }
            b'd' | b'D' => {
                *weight |= 1;
            }
            b'*' => {
                *prefix = true;
            }
            _ => {
                return buf;
            }
        }
        buf = buf.add(1);
    }

    buf
}

/*
 * Parse phrase operator. The operator
 * may take the following forms:
 *
 *		a <N> b (distance is exactly N lexemes)
 *		a <-> b (default distance = 1)
 *
 * The buffer should begin with '<' char
 */
unsafe fn parse_phrase_operator(pstate: TSQueryParserState, distance: *mut int16) -> bool {
    #[derive(PartialEq, Eq)]
    enum PhraseState {
        PHRASE_OPEN,
        PHRASE_DIST,
        PHRASE_CLOSE,
        PHRASE_FINISH,
    }
    use PhraseState::*;

    let mut state = PHRASE_OPEN;
    let mut ptr: *mut c_char = (*pstate).buf;
    let mut endptr: *mut c_char = null_mut();
    let mut l: c_long = 1; /* default distance */

    while *ptr != 0 {
        match state {
            PHRASE_OPEN => {
                if t_iseq(ptr, b'<' as c_char) {
                    state = PHRASE_DIST;
                    ptr = ptr.add(1);
                } else {
                    return false;
                }
            }

            PHRASE_DIST => {
                if t_iseq(ptr, b'-' as c_char) {
                    state = PHRASE_CLOSE;
                    ptr = ptr.add(1);
                    continue;
                }

                if (*ptr as u8 as char).is_ascii_digit() == false {
                    return false;
                }

                set_errno(0);
                l = strtol(ptr, &raw mut endptr, 10);
                if ptr == endptr {
                    return false;
                } else if get_errno() == ERANGE || l < 0 || l > MAXENTRYPOS as c_long {
                    ereturn!(
                        (*pstate).escontext,
                        false,
                        errmsg!(
                            "distance in phrase operator must be an integer value between zero and {} inclusive",
                            MAXENTRYPOS
                        )
                    );
                } else {
                    state = PHRASE_CLOSE;
                    ptr = endptr;
                }
            }

            PHRASE_CLOSE => {
                if t_iseq(ptr, b'>' as c_char) {
                    state = PHRASE_FINISH;
                    ptr = ptr.add(1);
                } else {
                    return false;
                }
            }

            PHRASE_FINISH => {
                *distance = l as int16;
                (*pstate).buf = ptr;
                return true;
            }
        }
    }

    false
}

/*
 * Parse OR operator used in websearch_to_tsquery(), returns true if we
 * believe that "OR" literal could be an operator OR
 */
unsafe fn parse_or_operator(pstate: TSQueryParserState) -> bool {
    let mut ptr: *mut c_char = (*pstate).buf;

    /* it should begin with "OR" literal */
    if pg_strncasecmp(ptr, c"or".as_ptr(), 2) != 0 {
        return false;
    }

    ptr = ptr.add(2);

    /*
     * it shouldn't be a part of any word but somewhere later it should be
     * some operand
     */
    if *ptr == 0 {
        /* no operand */
        return false;
    }

    /* it shouldn't be a part of any word */
    if t_iseq(ptr, b'-' as c_char) || t_iseq(ptr, b'_' as c_char) || t_isalnum_cstr(ptr) != 0 {
        return false;
    }

    loop {
        ptr = ptr.add(pg_mblen_cstr(ptr) as usize);

        if *ptr == 0 {
            /* got end of string without operand */
            return false;
        }

        /*
         * Suppose, we found an operand, but could be a not correct operand.
         * So we still treat OR literal as operation with possibly incorrect
         * operand and will not search it as lexeme
         */
        if !(*ptr as u8 as char).is_whitespace() {
            break;
        }
    }

    (*pstate).buf = (*pstate).buf.add(2);
    true
}

unsafe fn gettoken_query_standard(
    state: TSQueryParserState,
    operator: *mut int8,
    lenval: *mut c_int,
    strval: *mut *mut c_char,
    weight: *mut int16,
    prefix: *mut bool,
) -> ts_tokentype {
    *weight = 0;
    *prefix = false;

    loop {
        match (*state).state {
            WAITFIRSTOPERAND | WAITOPERAND => {
                if t_iseq((*state).buf, b'!' as c_char) {
                    (*state).buf = (*state).buf.add(1);
                    (*state).state = WAITOPERAND;
                    *operator = OP_NOT;
                    return PT_OPR;
                } else if t_iseq((*state).buf, b'(' as c_char) {
                    (*state).buf = (*state).buf.add(1);
                    (*state).state = WAITOPERAND;
                    (*state).count += 1;
                    return PT_OPEN;
                } else if t_iseq((*state).buf, b':' as c_char) {
                    /* generic syntax error message is fine */
                    return PT_ERR;
                } else if !(*(*state).buf as u8 as char).is_whitespace() {
                    /*
                     * We rely on the tsvector parser to parse the value for
                     * us
                     */
                    reset_tsvector_parser((*state).valstate, (*state).buf);
                    if gettoken_tsvector(
                        (*state).valstate,
                        strval,
                        lenval,
                        null_mut(),
                        null_mut(),
                        &raw mut (*state).buf,
                    ) {
                        (*state).buf = get_modifiers((*state).buf, weight, prefix);
                        (*state).state = WAITOPERATOR;
                        return PT_VAL;
                    } else if SOFT_ERROR_OCCURRED((*state).escontext) {
                        /* gettoken_tsvector reported a soft error */
                        return PT_ERR;
                    } else if (*state).state == WAITFIRSTOPERAND {
                        return PT_END;
                    } else {
                        ereturn!(
                            (*state).escontext,
                            PT_ERR,
                            errmsg!(
                                "no operand in tsquery: \"{}\"",
                                cstr((*state).buffer)
                            )
                        );
                    }
                }
            }

            WAITOPERATOR => {
                if t_iseq((*state).buf, b'&' as c_char) {
                    (*state).buf = (*state).buf.add(1);
                    (*state).state = WAITOPERAND;
                    *operator = OP_AND;
                    return PT_OPR;
                } else if t_iseq((*state).buf, b'|' as c_char) {
                    (*state).buf = (*state).buf.add(1);
                    (*state).state = WAITOPERAND;
                    *operator = OP_OR;
                    return PT_OPR;
                } else if parse_phrase_operator(state, weight) {
                    /* weight var is used as storage for distance */
                    (*state).state = WAITOPERAND;
                    *operator = OP_PHRASE;
                    return PT_OPR;
                } else if SOFT_ERROR_OCCURRED((*state).escontext) {
                    /* parse_phrase_operator reported a soft error */
                    return PT_ERR;
                } else if t_iseq((*state).buf, b')' as c_char) {
                    (*state).buf = (*state).buf.add(1);
                    (*state).count -= 1;
                    return if (*state).count < 0 { PT_ERR } else { PT_CLOSE };
                } else if *(*state).buf == 0 {
                    return if (*state).count != 0 { PT_ERR } else { PT_END };
                } else if !(*(*state).buf as u8 as char).is_whitespace() {
                    return PT_ERR;
                }
            }
        }

        (*state).buf = (*state).buf.add(pg_mblen_cstr((*state).buf) as usize);
    }
}

unsafe fn gettoken_query_websearch(
    state: TSQueryParserState,
    operator: *mut int8,
    lenval: *mut c_int,
    strval: *mut *mut c_char,
    weight: *mut int16,
    prefix: *mut bool,
) -> ts_tokentype {
    *weight = 0;
    *prefix = false;

    loop {
        match (*state).state {
            WAITFIRSTOPERAND | WAITOPERAND => {
                if t_iseq((*state).buf, b'-' as c_char) {
                    (*state).buf = (*state).buf.add(1);
                    (*state).state = WAITOPERAND;

                    *operator = OP_NOT;
                    return PT_OPR;
                } else if t_iseq((*state).buf, b'"' as c_char) {
                    /* Everything in quotes is processed as a single token */

                    /* skip opening quote */
                    (*state).buf = (*state).buf.add(1);
                    *strval = (*state).buf;

                    /* iterate to the closing quote or end of the string */
                    while *(*state).buf != 0 && !t_iseq((*state).buf, b'"' as c_char) {
                        (*state).buf = (*state).buf.add(1);
                    }
                    *lenval = (*state).buf.offset_from(*strval) as c_int;

                    /* skip closing quote if not end of the string */
                    if *(*state).buf != 0 {
                        (*state).buf = (*state).buf.add(1);
                    }

                    (*state).state = WAITOPERATOR;
                    (*state).count += 1;
                    return PT_VAL;
                } else if ISOPERATOR((*state).buf) {
                    /* ignore, else gettoken_tsvector() will raise an error */
                    (*state).buf = (*state).buf.add(1);
                    (*state).state = WAITOPERAND;
                    continue;
                } else if !(*(*state).buf as u8 as char).is_whitespace() {
                    /*
                     * We rely on the tsvector parser to parse the value for
                     * us
                     */
                    reset_tsvector_parser((*state).valstate, (*state).buf);
                    if gettoken_tsvector(
                        (*state).valstate,
                        strval,
                        lenval,
                        null_mut(),
                        null_mut(),
                        &raw mut (*state).buf,
                    ) {
                        (*state).state = WAITOPERATOR;
                        return PT_VAL;
                    } else if SOFT_ERROR_OCCURRED((*state).escontext) {
                        /* gettoken_tsvector reported a soft error */
                        return PT_ERR;
                    } else if (*state).state == WAITFIRSTOPERAND {
                        return PT_END;
                    } else {
                        /* finally, we have to provide an operand */
                        pushStop(state);
                        return PT_END;
                    }
                }
            }

            WAITOPERATOR => {
                if *(*state).buf == 0 {
                    return PT_END;
                } else if parse_or_operator(state) {
                    (*state).state = WAITOPERAND;
                    *operator = OP_OR;
                    return PT_OPR;
                } else if ISOPERATOR((*state).buf) {
                    /* ignore other operators in this state too */
                    (*state).buf = (*state).buf.add(1);
                    continue;
                } else if !(*(*state).buf as u8 as char).is_whitespace() {
                    /* insert implicit AND between operands */
                    (*state).state = WAITOPERAND;
                    *operator = OP_AND;
                    return PT_OPR;
                }
            }
        }

        (*state).buf = (*state).buf.add(pg_mblen_cstr((*state).buf) as usize);
    }
}

unsafe fn gettoken_query_plain(
    state: TSQueryParserState,
    operator: *mut int8,
    lenval: *mut c_int,
    strval: *mut *mut c_char,
    weight: *mut int16,
    prefix: *mut bool,
) -> ts_tokentype {
    *weight = 0;
    *prefix = false;

    if *(*state).buf == 0 {
        return PT_END;
    }

    *strval = (*state).buf;
    *lenval = strlen((*state).buf) as c_int;
    (*state).buf = (*state).buf.add(*lenval as usize);
    (*state).count += 1;
    PT_VAL
}

/*
 * Push an operator to state->polstr
 */
pub unsafe fn pushOperator(state: TSQueryParserState, oper: int8, distance: int16) {
    let tmp: *mut QueryOperator;

    Assert!(oper == OP_NOT || oper == OP_AND || oper == OP_OR || oper == OP_PHRASE);

    tmp = palloc0(core::mem::size_of::<QueryOperator>() as Size) as *mut QueryOperator;
    (*tmp).r#type = QI_OPR;
    (*tmp).oper = oper;
    (*tmp).distance = if oper == OP_PHRASE { distance } else { 0 };
    /* left is filled in later with findoprnd */

    (*state).polstr = lcons(tmp as *mut c_void, (*state).polstr);
}

unsafe fn pushValue_internal(
    state: TSQueryParserState,
    valcrc: pg_crc32,
    distance: c_int,
    lenval: c_int,
    weight: c_int,
    prefix: bool,
) {
    let tmp: *mut QueryOperand;

    if distance >= MAXSTRPOS as c_int {
        ereturn!(
            (*state).escontext,
            (),
            errmsg!(
                "value is too big in tsquery: \"{}\"",
                cstr((*state).buffer)
            )
        );
    }
    if lenval >= MAXSTRLEN as c_int {
        ereturn!(
            (*state).escontext,
            (),
            errmsg!(
                "operand is too long in tsquery: \"{}\"",
                cstr((*state).buffer)
            )
        );
    }

    tmp = palloc0(core::mem::size_of::<QueryOperand>() as Size) as *mut QueryOperand;
    (*tmp).r#type = QI_VAL;
    (*tmp).weight = weight as uint8;
    (*tmp).prefix = prefix;
    (*tmp).valcrc = valcrc as int32;
    (*tmp).set_length(lenval as uint32);
    (*tmp).set_distance(distance as uint32);

    (*state).polstr = lcons(tmp as *mut c_void, (*state).polstr);
}

/*
 * Push an operand to state->polstr.
 *
 * strval must point to a string equal to state->curop. lenval is the length
 * of the string.
 */
pub unsafe fn pushValue(
    state: TSQueryParserState,
    strval: *mut c_char,
    lenval: c_int,
    weight: int16,
    prefix: bool,
) {
    let mut valcrc: pg_crc32;

    if lenval >= MAXSTRLEN as c_int {
        ereturn!(
            (*state).escontext,
            (),
            errmsg!(
                "word is too long in tsquery: \"{}\"",
                cstr((*state).buffer)
            )
        );
    }

    valcrc = INIT_LEGACY_CRC32();
    valcrc = COMP_LEGACY_CRC32(valcrc, strval as *const c_void, lenval as uint32);
    valcrc = FIN_LEGACY_CRC32(valcrc);
    pushValue_internal(
        state,
        valcrc,
        (*state).curop.offset_from((*state).op) as c_int,
        lenval,
        weight as c_int,
        prefix,
    );

    /* append the value string to state.op, enlarging buffer if needed first */
    while (*state).curop.offset_from((*state).op) as c_int + lenval + 1 >= (*state).lenop {
        let used: c_int = (*state).curop.offset_from((*state).op) as c_int;

        (*state).lenop *= 2;
        (*state).op = repalloc((*state).op as *mut c_void, (*state).lenop as Size) as *mut c_char;
        (*state).curop = (*state).op.add(used as usize);
    }
    memcpy(
        (*state).curop as *mut c_void,
        strval as *const c_void,
        lenval as usize,
    );
    (*state).curop = (*state).curop.add(lenval as usize);
    *(*state).curop = b'\0' as c_char;
    (*state).curop = (*state).curop.add(1);
    (*state).sumlen += lenval + 1 /* \0 */;
}

/*
 * Push a stopword placeholder to state->polstr
 */
pub unsafe fn pushStop(state: TSQueryParserState) {
    let tmp: *mut QueryOperand;

    tmp = palloc0(core::mem::size_of::<QueryOperand>() as Size) as *mut QueryOperand;
    (*tmp).r#type = QI_VALSTOP;

    (*state).polstr = lcons(tmp as *mut c_void, (*state).polstr);
}

const STACKDEPTH: c_int = 32;

#[repr(C)]
#[derive(Clone, Copy)]
struct OperatorElement {
    op: int8,
    distance: int16,
}

unsafe fn pushOpStack(
    stack: *mut OperatorElement,
    lenstack: *mut c_int,
    op: int8,
    distance: int16,
) {
    if *lenstack == STACKDEPTH {
        /* internal error */
        elog!(ERROR, "tsquery stack too small");
    }

    (*stack.add(*lenstack as usize)).op = op;
    (*stack.add(*lenstack as usize)).distance = distance;

    *lenstack += 1;
}

unsafe fn cleanOpStack(
    state: TSQueryParserState,
    stack: *mut OperatorElement,
    lenstack: *mut c_int,
    op: int8,
) {
    let opPriority: c_int = OP_PRIORITY(op);

    while *lenstack != 0 {
        /* NOT is right associative unlike to others */
        if (op != OP_NOT && opPriority > OP_PRIORITY((*stack.add((*lenstack - 1) as usize)).op))
            || (op == OP_NOT
                && opPriority >= OP_PRIORITY((*stack.add((*lenstack - 1) as usize)).op))
        {
            break;
        }

        *lenstack -= 1;
        pushOperator(
            state,
            (*stack.add(*lenstack as usize)).op,
            (*stack.add(*lenstack as usize)).distance,
        );
    }
}

/*
 * Make polish (prefix) notation of query.
 *
 * See parse_tsquery for explanation of pushval.
 */
unsafe fn makepol(state: TSQueryParserState, pushval: PushFunction, opaque: Datum) {
    let mut operator: int8 = 0;
    let mut r#type: ts_tokentype;
    let mut lenval: c_int = 0;
    let mut strval: *mut c_char = null_mut();
    let mut opstack: [OperatorElement; STACKDEPTH as usize] =
        [OperatorElement { op: 0, distance: 0 }; STACKDEPTH as usize];
    let mut lenstack: c_int = 0;
    let mut weight: int16 = 0;
    let mut prefix: bool = false;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    loop {
        r#type = ((*state).gettoken)(
            state,
            &raw mut operator,
            &raw mut lenval,
            &raw mut strval,
            &raw mut weight,
            &raw mut prefix,
        );
        if r#type == PT_END {
            break;
        }

        match r#type {
            PT_VAL => {
                pushval(opaque, state, strval, lenval, weight, prefix);
            }
            PT_OPR => {
                cleanOpStack(state, opstack.as_mut_ptr(), &raw mut lenstack, operator);
                pushOpStack(opstack.as_mut_ptr(), &raw mut lenstack, operator, weight);
            }
            PT_OPEN => {
                makepol(state, pushval, opaque);
            }
            PT_CLOSE => {
                cleanOpStack(
                    state,
                    opstack.as_mut_ptr(),
                    &raw mut lenstack,
                    OP_OR, /* lowest */
                );
                return;
            }
            PT_ERR | _ => {
                /* don't overwrite a soft error saved by gettoken function */
                if !SOFT_ERROR_OCCURRED((*state).escontext) {
                    errsave!(
                        (*state).escontext,
                        errmsg!(
                            "syntax error in tsquery: \"{}\"",
                            cstr((*state).buffer)
                        )
                    );
                }
                return;
            }
        }
        /* detect soft error in pushval or recursion */
        if SOFT_ERROR_OCCURRED((*state).escontext) {
            return;
        }
    }

    cleanOpStack(
        state,
        opstack.as_mut_ptr(),
        &raw mut lenstack,
        OP_OR, /* lowest */
    );
}

unsafe fn findoprnd_recurse(
    ptr: *mut QueryItem,
    pos: *mut uint32,
    nnodes: c_int,
    needcleanup: *mut bool,
) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if *pos >= nnodes as uint32 {
        elog!(ERROR, "malformed tsquery: operand not found");
    }

    if (*ptr.add(*pos as usize)).r#type == QI_VAL {
        *pos += 1;
    } else if (*ptr.add(*pos as usize)).r#type == QI_VALSTOP {
        *needcleanup = true; /* we'll have to remove stop words */
        *pos += 1;
    } else {
        Assert!((*ptr.add(*pos as usize)).r#type == QI_OPR);

        if (*ptr.add(*pos as usize)).qoperator.oper == OP_NOT {
            (*ptr.add(*pos as usize)).qoperator.left = 1; /* fixed offset */
            *pos += 1;

            /* process the only argument */
            findoprnd_recurse(ptr, pos, nnodes, needcleanup);
        } else {
            let curitem: *mut QueryOperator = &raw mut (*ptr.add(*pos as usize)).qoperator;
            let tmp: c_int = *pos as c_int; /* save current position */

            Assert!(
                (*curitem).oper == OP_AND
                    || (*curitem).oper == OP_OR
                    || (*curitem).oper == OP_PHRASE
            );

            *pos += 1;

            /* process RIGHT argument */
            findoprnd_recurse(ptr, pos, nnodes, needcleanup);

            (*curitem).left = *pos - tmp as uint32; /* set LEFT arg's offset */

            /* process LEFT argument */
            findoprnd_recurse(ptr, pos, nnodes, needcleanup);
        }
    }
}

/*
 * Fill in the left-fields previously left unfilled.
 * The input QueryItems must be in polish (prefix) notation.
 * Also, set *needcleanup to true if there are any QI_VALSTOP nodes.
 */
unsafe fn findoprnd(ptr: *mut QueryItem, size: c_int, needcleanup: *mut bool) {
    let mut pos: uint32;

    *needcleanup = false;
    pos = 0;
    findoprnd_recurse(ptr, &raw mut pos, size, needcleanup);

    if pos != size as uint32 {
        elog!(ERROR, "malformed tsquery: extra nodes");
    }
}

/*
 * Parse the tsquery stored in "buf".
 *
 * Each value (operand) in the query is passed to pushval. pushval can
 * transform the simple value to an arbitrarily complex expression using
 * pushValue and pushOperator. It must push a single value with pushValue,
 * a complete expression with all operands, or a stopword placeholder
 * with pushStop, otherwise the prefix notation representation will be broken,
 * having an operator with no operand.
 *
 * opaque is passed on to pushval as is, pushval can use it to store its
 * private state.
 *
 * The pushval function can record soft errors via escontext.
 * Callers must check SOFT_ERROR_OCCURRED to detect that.
 *
 * A bitmask of flags (see ts_utils.h) and an error context object
 * can be provided as well.  If a soft error occurs, NULL is returned.
 */
#[allow(invalid_value)]
pub unsafe fn parse_tsquery(
    buf: *mut c_char,
    pushval: PushFunction,
    opaque: Datum,
    flags: c_int,
    escontext: *mut Node,
) -> TSQuery {
    let mut state: TSQueryParserStateData = core::mem::zeroed();
    let mut i: c_int;
    let mut query: TSQuery;
    let commonlen: c_int;
    let ptr: *mut QueryItem;
    let noisy: bool;
    let mut needcleanup: bool = false;
    let mut tsv_flags: c_int = P_TSV_OPR_IS_DELIM | P_TSV_IS_TSQUERY;

    /* plain should not be used with web */
    Assert!((flags & (P_TSQ_PLAIN | P_TSQ_WEB)) != (P_TSQ_PLAIN | P_TSQ_WEB));

    /* select suitable tokenizer */
    if flags & P_TSQ_PLAIN != 0 {
        state.gettoken = gettoken_query_plain;
    } else if flags & P_TSQ_WEB != 0 {
        state.gettoken = gettoken_query_websearch;
        tsv_flags |= P_TSV_IS_WEB;
    } else {
        state.gettoken = gettoken_query_standard;
    }

    /* emit nuisance NOTICEs only if not doing soft errors */
    noisy = !(!escontext.is_null() && crate::IsA!(escontext, T_ErrorSaveContext));

    /* init state */
    state.buffer = buf;
    state.buf = buf;
    state.count = 0;
    state.state = WAITFIRSTOPERAND;
    state.polstr = NIL;
    state.escontext = escontext;

    /* init value parser's state */
    state.valstate = init_tsvector_parser(state.buffer, tsv_flags, escontext);

    /* init list of operand */
    state.sumlen = 0;
    state.lenop = 64;
    state.op = palloc(state.lenop as Size) as *mut c_char;
    state.curop = state.op;
    *(state.curop) = b'\0' as c_char;

    /* parse query & make polish notation (postfix, but in reverse order) */
    makepol(&raw mut state, pushval, opaque);

    close_tsvector_parser(state.valstate);

    if SOFT_ERROR_OCCURRED(escontext) {
        return null_mut();
    }

    if state.polstr == NIL {
        if noisy {
            ereport!(
                NOTICE,
                errmsg!(
                    "text-search query doesn't contain lexemes: \"{}\"",
                    cstr(state.buffer)
                )
            );
        }
        query = palloc(HDRSIZETQ()) as TSQuery;
        SET_VARSIZE(query as *mut c_char, HDRSIZETQ() as int32);
        (*query).size = 0;
        return query;
    }

    if TSQUERY_TOO_BIG(list_length(state.polstr), state.sumlen) {
        ereturn!(
            escontext,
            null_mut(),
            errmsg!("tsquery is too large")
        );
    }
    commonlen = COMPUTESIZE(list_length(state.polstr), state.sumlen) as c_int;

    /* Pack the QueryItems in the final TSQuery struct to return to caller */
    query = palloc0(commonlen as Size) as TSQuery;
    SET_VARSIZE(query as *mut c_char, commonlen);
    (*query).size = list_length(state.polstr);
    ptr = GETQUERY(query);

    /* Copy QueryItems to TSQuery */
    i = 0;
    foreach!(cell, state.polstr, {
        let item: *mut QueryItem = lfirst(current_cell!(cell)) as *mut QueryItem;

        match (*item).r#type {
            QI_VAL => {
                memcpy(
                    ptr.add(i as usize) as *mut c_void,
                    item as *const c_void,
                    core::mem::size_of::<QueryOperand>(),
                );
            }
            QI_VALSTOP => {
                (*ptr.add(i as usize)).r#type = QI_VALSTOP;
            }
            QI_OPR => {
                memcpy(
                    ptr.add(i as usize) as *mut c_void,
                    item as *const c_void,
                    core::mem::size_of::<QueryOperator>(),
                );
            }
            _ => {
                elog!(ERROR, "unrecognized QueryItem type: {}", (*item).r#type);
            }
        }
        i += 1;
    });

    /* Copy all the operand strings to TSQuery */
    memcpy(
        GETOPERAND(query) as *mut c_void,
        state.op as *const c_void,
        state.sumlen as usize,
    );
    pfree(state.op as *mut c_void);

    /*
     * Set left operand pointers for every operator.  While we're at it,
     * detect whether there are any QI_VALSTOP nodes.
     */
    findoprnd(ptr, (*query).size, &raw mut needcleanup);

    /*
     * If there are QI_VALSTOP nodes, delete them and simplify the tree.
     */
    if needcleanup {
        query = cleanup_tsquery_stopwords(query, noisy);
    }

    query
}

unsafe fn pushval_asis(
    opaque: Datum,
    state: TSQueryParserState,
    strval: *mut c_char,
    lenval: c_int,
    weight: int16,
    prefix: bool,
) {
    pushValue(state, strval, lenval, weight, prefix);
}

/*
 * in without morphology
 */
pub unsafe fn tsqueryin(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context as *mut Node;

    PG_RETURN_TSQUERY!(parse_tsquery(
        in_,
        pushval_asis,
        PointerGetDatum(null()),
        0,
        escontext
    ))
}

/*
 * out function
 */
#[repr(C)]
struct INFIX {
    curpol: *mut QueryItem,
    buf: *mut c_char,
    cur: *mut c_char,
    op: *mut c_char,
    buflen: c_int,
}

/* Makes sure inf->buf is large enough for adding 'addsize' bytes */
/* #define RESIZEBUF(inf, addsize) ... */
macro_rules! RESIZEBUF {
    ($inf:expr, $addsize:expr) => {{
        while ($inf.cur.offset_from($inf.buf) as c_int) + ($addsize) + 1 >= $inf.buflen {
            let len = $inf.cur.offset_from($inf.buf) as c_int;
            $inf.buflen *= 2;
            $inf.buf = repalloc($inf.buf as *mut c_void, $inf.buflen as Size) as *mut c_char;
            $inf.cur = $inf.buf.add(len as usize);
        }
    }};
}

/*
 * recursively traverse the tree and
 * print it in infix (human-readable) form
 */
unsafe fn infix(in_: *mut INFIX, parentPriority: c_int, rightPhraseOp: bool) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*in_).curpol).r#type == QI_VAL {
        let curpol: *mut QueryOperand = &raw mut (*(*in_).curpol).qoperand;
        let mut op: *mut c_char = (*in_).op.add((*curpol).distance() as usize);
        let mut clen: c_int;

        RESIZEBUF!(
            (*in_),
            (*curpol).length() as c_int * (pg_database_encoding_max_length() + 1) + 2 + 6
        );
        *((*in_).cur) = b'\'' as c_char;
        (*in_).cur = (*in_).cur.add(1);
        while *op != 0 {
            if t_iseq(op, b'\'' as c_char) {
                *((*in_).cur) = b'\'' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            } else if t_iseq(op, b'\\' as c_char) {
                *((*in_).cur) = b'\\' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            }

            clen = ts_copychar_cstr((*in_).cur as *mut c_void, op as *const c_void);
            op = op.add(clen as usize);
            (*in_).cur = (*in_).cur.add(clen as usize);
        }
        *((*in_).cur) = b'\'' as c_char;
        (*in_).cur = (*in_).cur.add(1);
        if (*curpol).weight != 0 || (*curpol).prefix {
            *((*in_).cur) = b':' as c_char;
            (*in_).cur = (*in_).cur.add(1);
            if (*curpol).prefix {
                *((*in_).cur) = b'*' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            }
            if (*curpol).weight & (1 << 3) != 0 {
                *((*in_).cur) = b'A' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            }
            if (*curpol).weight & (1 << 2) != 0 {
                *((*in_).cur) = b'B' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            }
            if (*curpol).weight & (1 << 1) != 0 {
                *((*in_).cur) = b'C' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            }
            if (*curpol).weight & 1 != 0 {
                *((*in_).cur) = b'D' as c_char;
                (*in_).cur = (*in_).cur.add(1);
            }
        }
        *((*in_).cur) = b'\0' as c_char;
        (*in_).curpol = (*in_).curpol.add(1);
    } else if (*(*in_).curpol).qoperator.oper == OP_NOT {
        let priority: c_int = QO_PRIORITY(&raw const (*(*in_).curpol).qoperator);

        if priority < parentPriority {
            RESIZEBUF!((*in_), 2);
            sprintf_str((*in_).cur, "( ");
            (*in_).cur = strchr_nul((*in_).cur);
        }
        RESIZEBUF!((*in_), 1);
        *((*in_).cur) = b'!' as c_char;
        (*in_).cur = (*in_).cur.add(1);
        *((*in_).cur) = b'\0' as c_char;
        (*in_).curpol = (*in_).curpol.add(1);

        infix(in_, priority, false);
        if priority < parentPriority {
            RESIZEBUF!((*in_), 2);
            sprintf_str((*in_).cur, " )");
            (*in_).cur = strchr_nul((*in_).cur);
        }
    } else {
        let op: int8 = (*(*in_).curpol).qoperator.oper;
        let priority: c_int = QO_PRIORITY(&raw const (*(*in_).curpol).qoperator);
        let distance: int16 = (*(*in_).curpol).qoperator.distance;
        let mut nrm: INFIX = core::mem::zeroed();
        let mut needParenthesis: bool = false;

        (*in_).curpol = (*in_).curpol.add(1);
        if priority < parentPriority ||
            /* phrase operator depends on order */
            (op == OP_PHRASE && rightPhraseOp)
        {
            needParenthesis = true;
            RESIZEBUF!((*in_), 2);
            sprintf_str((*in_).cur, "( ");
            (*in_).cur = strchr_nul((*in_).cur);
        }

        nrm.curpol = (*in_).curpol;
        nrm.op = (*in_).op;
        nrm.buflen = 16;
        nrm.buf = palloc((core::mem::size_of::<c_char>() * nrm.buflen as usize) as Size)
            as *mut c_char;
        nrm.cur = nrm.buf;

        /* get right operand */
        infix(&raw mut nrm, priority, op == OP_PHRASE);

        /* get & print left operand */
        (*in_).curpol = nrm.curpol;
        infix(in_, priority, false);

        /* print operator & right operand */
        RESIZEBUF!(
            (*in_),
            3 + (2 + 10 /* distance */) + (nrm.cur.offset_from(nrm.buf) as c_int)
        );
        match op {
            OP_OR => {
                sprintf_str((*in_).cur, &format!(" | {}", cstr(nrm.buf)));
            }
            OP_AND => {
                sprintf_str((*in_).cur, &format!(" & {}", cstr(nrm.buf)));
            }
            OP_PHRASE => {
                if distance != 1 {
                    sprintf_str((*in_).cur, &format!(" <{}> {}", distance, cstr(nrm.buf)));
                } else {
                    sprintf_str((*in_).cur, &format!(" <-> {}", cstr(nrm.buf)));
                }
            }
            _ => {
                /* OP_NOT is handled in above if-branch */
                elog!(ERROR, "unrecognized operator type: {}", op);
            }
        }
        (*in_).cur = strchr_nul((*in_).cur);
        pfree(nrm.buf as *mut c_void);

        if needParenthesis {
            RESIZEBUF!((*in_), 2);
            sprintf_str((*in_).cur, " )");
            (*in_).cur = strchr_nul((*in_).cur);
        }
    }
}

pub unsafe fn tsqueryout(fcinfo: FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 0);
    let mut nrm: INFIX = core::mem::zeroed();

    if (*query).size == 0 {
        let b: *mut c_char = palloc(1) as *mut c_char;

        *b = b'\0' as c_char;
        PG_RETURN_POINTER!(b);
    }
    nrm.curpol = GETQUERY(query);
    nrm.buflen = 32;
    nrm.buf = palloc((core::mem::size_of::<c_char>() * nrm.buflen as usize) as Size) as *mut c_char;
    nrm.cur = nrm.buf;
    *(nrm.cur) = b'\0' as c_char;
    nrm.op = GETOPERAND(query);
    infix(&raw mut nrm, -1 /* lowest priority */, false);

    PG_FREE_IF_COPY!(fcinfo, query, 0);
    PG_RETURN_CSTRING!(nrm.buf)
}

/*
 * Binary Input / Output functions. The binary format is as follows:
 *
 * uint32	 number of operators/operands in the query
 *
 * Followed by the operators and operands, in prefix notation. For each
 * operand:
 *
 * uint8	type, QI_VAL
 * uint8	weight
 * uint8	prefix
 *			operand text in client encoding, null-terminated
 *
 * For each operator:
 *
 * uint8	type, QI_OPR
 * uint8	operator, one of OP_AND, OP_PHRASE OP_OR, OP_NOT.
 * uint16	distance (only for OP_PHRASE)
 */
pub unsafe fn tsquerysend(fcinfo: FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut i: c_int;
    let mut item: *mut QueryItem = GETQUERY(query);

    pq_begintypsend(&raw mut buf);

    pq_sendint32(&raw mut buf, (*query).size as uint32);
    i = 0;
    while i < (*query).size {
        pq_sendint8(&raw mut buf, (*item).r#type as uint8);

        match (*item).r#type {
            QI_VAL => {
                pq_sendint8(&raw mut buf, (*item).qoperand.weight);
                pq_sendint8(&raw mut buf, (*item).qoperand.prefix as uint8);
                pq_sendstring(
                    &raw mut buf,
                    GETOPERAND(query).add((*item).qoperand.distance() as usize),
                );
            }
            QI_OPR => {
                pq_sendint8(&raw mut buf, (*item).qoperator.oper as uint8);
                if (*item).qoperator.oper == OP_PHRASE {
                    pq_sendint16(&raw mut buf, (*item).qoperator.distance as uint16);
                }
            }
            _ => {
                elog!(ERROR, "unrecognized tsquery node type: {}", (*item).r#type);
            }
        }
        item = item.add(1);
        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, query, 0);

    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf))
}

pub unsafe fn tsqueryrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let mut query: TSQuery;
    let mut i: c_int;
    let len: c_int;
    let mut item: *mut QueryItem;
    let mut datalen: c_int;
    let mut ptr: *mut c_char;
    let size: uint32;
    let operands: *mut *const c_char;
    let mut needcleanup: bool = false;

    size = pq_getmsgint(buf, core::mem::size_of::<uint32>() as c_int) as uint32;
    if size as Size > (MaxAllocSize / core::mem::size_of::<QueryItem>() as Size) {
        elog!(ERROR, "invalid size of tsquery");
    }

    /* Allocate space to temporarily hold operand strings */
    operands =
        palloc(size as Size * core::mem::size_of::<*const c_char>() as Size) as *mut *const c_char;

    /* Allocate space for all the QueryItems. */
    len = HDRSIZETQ() as c_int + core::mem::size_of::<QueryItem>() as c_int * size as c_int;
    query = palloc0(len as Size) as TSQuery;
    (*query).size = size as int32;
    item = GETQUERY(query);

    datalen = 0;
    i = 0;
    while (i as uint32) < size {
        (*item).r#type = pq_getmsgint(buf, core::mem::size_of::<int8>() as c_int) as int8;

        if (*item).r#type == QI_VAL {
            let val_len: usize; /* length after recoding to server
                                 * encoding */
            let weight: uint8;
            let prefix: uint8;
            let val: *const c_char;
            let mut valcrc: pg_crc32;

            weight = pq_getmsgint(buf, core::mem::size_of::<uint8>() as c_int) as uint8;
            prefix = pq_getmsgint(buf, core::mem::size_of::<uint8>() as c_int) as uint8;
            val = pq_getmsgstring(buf);
            val_len = strlen(val);

            /* Sanity checks */

            if weight > 0xF {
                elog!(ERROR, "invalid tsquery: invalid weight bitmap");
            }

            if val_len > MAXSTRLEN as usize {
                elog!(ERROR, "invalid tsquery: operand too long");
            }

            if datalen > MAXSTRPOS as c_int {
                elog!(ERROR, "invalid tsquery: total operand length exceeded");
            }

            /* Looks valid. */

            valcrc = INIT_LEGACY_CRC32();
            valcrc = COMP_LEGACY_CRC32(valcrc, val as *const c_void, val_len as uint32);
            valcrc = FIN_LEGACY_CRC32(valcrc);

            (*item).qoperand.weight = weight;
            (*item).qoperand.prefix = if prefix != 0 { true } else { false };
            (*item).qoperand.valcrc = valcrc as int32;
            (*item).qoperand.set_length(val_len as uint32);
            (*item).qoperand.set_distance(datalen as uint32);

            /*
             * Operand strings are copied to the final struct after this loop;
             * here we just collect them to an array
             */
            *operands.add(i as usize) = val;

            datalen += val_len as c_int + 1; /* + 1 for the '\0' terminator */
        } else if (*item).r#type == QI_OPR {
            let oper: int8;

            oper = pq_getmsgint(buf, core::mem::size_of::<int8>() as c_int) as int8;
            if oper != OP_NOT && oper != OP_OR && oper != OP_AND && oper != OP_PHRASE {
                elog!(
                    ERROR,
                    "invalid tsquery: unrecognized operator type {}",
                    oper as c_int
                );
            }
            if i == size as c_int - 1 {
                elog!(ERROR, "invalid pointer to right operand");
            }

            (*item).qoperator.oper = oper;
            if oper == OP_PHRASE {
                (*item).qoperator.distance =
                    pq_getmsgint(buf, core::mem::size_of::<int16>() as c_int) as int16;
            }
        } else {
            elog!(ERROR, "unrecognized tsquery node type: {}", (*item).r#type);
        }

        item = item.add(1);
        i += 1;
    }

    /* Enlarge buffer to make room for the operand values. */
    query = repalloc(query as *mut c_void, (len + datalen) as Size) as TSQuery;
    item = GETQUERY(query);
    ptr = GETOPERAND(query);

    /*
     * Fill in the left-pointers. Checks that the tree is well-formed as a
     * side-effect.
     */
    findoprnd(item, size as c_int, &raw mut needcleanup);

    /* Can't have found any QI_VALSTOP nodes */
    Assert!(!needcleanup);

    /* Copy operands to output struct */
    i = 0;
    while (i as uint32) < size {
        if (*item).r#type == QI_VAL {
            memcpy(
                ptr as *mut c_void,
                *operands.add(i as usize) as *const c_void,
                (*item).qoperand.length() as usize + 1,
            );
            ptr = ptr.add((*item).qoperand.length() as usize + 1);
        }
        item = item.add(1);
        i += 1;
    }

    pfree(operands as *mut c_void);

    Assert!(ptr.offset_from(GETOPERAND(query)) as c_int == datalen);

    SET_VARSIZE(query as *mut c_char, len + datalen);

    PG_RETURN_TSQUERY!(query)
}

/*
 * debug function, used only for view query
 * which will be executed in non-leaf pages in index
 */
pub unsafe fn tsquerytree(fcinfo: FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 0);
    let mut nrm: INFIX = core::mem::zeroed();
    let res: *mut text;
    let q: *mut QueryItem;
    let mut len: c_int = 0;

    if (*query).size == 0 {
        res = palloc(VARHDRSZ as Size) as *mut text;
        SET_VARSIZE(res as *mut c_char, VARHDRSZ);
        PG_RETURN_POINTER!(res);
    }

    q = clean_NOT(GETQUERY(query) as *mut crate::utils::adt::tsquery_util::QueryItem, &raw mut len) as *mut QueryItem;

    if q.is_null() {
        res = cstring_to_text(c"T".as_ptr());
    } else {
        nrm.curpol = q;
        nrm.buflen = 32;
        nrm.buf =
            palloc((core::mem::size_of::<c_char>() * nrm.buflen as usize) as Size) as *mut c_char;
        nrm.cur = nrm.buf;
        *(nrm.cur) = b'\0' as c_char;
        nrm.op = GETOPERAND(query);
        infix(&raw mut nrm, -1, false);
        res = cstring_to_text_with_len(nrm.buf, nrm.cur.offset_from(nrm.buf) as c_int);
        pfree(q as *mut c_void);
    }

    PG_FREE_IF_COPY!(fcinfo, query, 0);

    PG_RETURN_TEXT_P!(res)
}

// ============================================================================
//   libc / helper shims used above
// ============================================================================

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strtol(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
}

const ERANGE: c_int = 34;

// errno get/set shims. C uses `errno = 0;` and tests `errno == ERANGE`.
unsafe fn set_errno(v: c_int) {
    *libc_errno_location() = v;
}
unsafe fn get_errno() -> c_int {
    *libc_errno_location()
}
extern "C" {
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios"),
        link_name = "__error"
    )]
    #[cfg_attr(target_os = "linux", link_name = "__errno_location")]
    fn libc_errno_location() -> *mut c_int;
}

// strchr(p, '\0'): pointer to the NUL terminator of p (C `strchr(cur, '\0')`).
unsafe fn strchr_nul(p: *mut c_char) -> *mut c_char {
    strchr(p, 0)
}

// sprintf(cur, "%s", s) style: copy a plain Rust &str (no format directives)
// into the c-string buffer at `cur`, NUL-terminating.  Used for the constant
// and pre-formatted strings the C code sprintf()s; the buffer is sized via
// RESIZEBUF beforehand exactly as in the C source.
unsafe fn sprintf_str(cur: *mut c_char, s: &str) {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        *cur.add(i) = bytes[i] as c_char;
        i += 1;
    }
    *cur.add(bytes.len()) = b'\0' as c_char;
}
