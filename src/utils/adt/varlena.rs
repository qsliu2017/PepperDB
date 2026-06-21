//! Translation of postgres/src/backend/utils/adt/varlena.c (in progress)
//!
//! Functions for the variable-length built-in types, plus the widely-used
//! cstring<->text conversion helpers.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! varatt.h VAR* helpers -> crate::varatt.  This is a LARGE source file; only the
//! cstring_to_text family and the core `text` I/O are translated so far.  The rest
//! (byteain/byteaout escape parsing [needs the standard_conforming_strings GUC],
//! text comparison/btree/collation, substring/position/overlay, the string-agg
//! aggregates, encode/decode, split/regexp, etc.) is STUBBED with TODO(pg-port).
//! text binary recv/send need mb/mbutils + pq_endtypsend (varatt/bytea), also TODO.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{
    pg_detoast_datum_packed, SET_VARSIZE, VARATT_IS_COMPRESSED, VARATT_IS_EXTERNAL, VARDATA,
    VARDATA_ANY, VARSIZE_ANY_EXHDR, VARSIZE, VARSIZE_ANY,
};
use crate::c::Name;
use crate::lengthof;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_POINTER, PG_GET_COLLATION, PG_RETURN_CSTRING,
    PG_ARGISNULL, PG_FREE_IF_COPY, PG_GETARG_NAME, PG_GETARG_TEXT_PP, PG_RETURN_BOOL,
    PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_TEXT_P, PG_RETURN_VOID,
    PG_GETARG_BYTEA_PP, PG_GETARG_BYTEA_P_COPY, PG_RETURN_BYTEA_P, PG_RETURN_INT64,
    PG_GETARG_INT64, PG_RETURN_NAME, DatumGetByteaPSlice, DatumGetByteaPP, PG_RETURN_INT16,
    PG_NARGS, PG_RETURN_DATUM,
};
use crate::postgres::DatumGetBool;
use crate::c::bits8;
use crate::port::pg_bitutils::pg_popcount;
use crate::catalog::pg_type_d::BYTEAOID;
use crate::pg_config_manual::BITS_PER_BYTE;
use crate::utils::adt::int::{int2send, int4send};
use crate::utils::adt::int8::int8send;
use crate::lib::stringinfo::{
    initStringInfo, appendStringInfoChar, appendStringInfoString,
};
use crate::appendStringInfo;
use crate::utils::array::{
    ArrayType, ARR_NDIM, ARR_DIMS, ARR_ELEMTYPE, ARR_DATA_PTR, ARR_NULLBITMAP,
};
use crate::utils::adt::arrayfuncs::{
    ArrayBuildState, accumArrayResult, makeArrayResult, construct_empty_array,
};
use crate::utils::adt::arrayutils::ArrayGetNItems;
use crate::nodes::execnodes::{ReturnSetInfo, Tuplestorestate};
use crate::utils::cache::lsyscache::{
    get_type_io_data, IOFunc_output, getTypeOutputInfo, get_base_element_type,
};
use crate::utils::mb::mbutils::pg_mbcharcliplen;
use crate::access::tupmacs::{fetch_att, att_addlength_pointer, att_align_nominal};
use crate::access::common::tupdesc::TupleDesc;
use crate::utils::mmgr::mcxt::{CurrentMemoryContext, MemoryContextAlloc};
use crate::utils::adt::regexp::RE_compile_and_cache;
use crate::regex::regex::{
    regex_t, regmatch_t, pg_regerror, pg_regexec, REG_NOSUB, REG_NOMATCH, REG_OKAY,
};
use crate::utils::mb::mbutils::pg_mb2wchar_with_len;
use crate::mb::pg_wchar::pg_wchar;
use crate::catalog::pg_type_d::TEXTOID;
use crate::utils::adt::array_expanded::ArrayMetaState;
use crate::nodes::pg_list::*;
use crate::nodes::list::{lappend, list_free, list_free_deep};
use crate::nodes::value::makeString;
use crate::parser::scansup::{scanner_isspace, downcase_truncate_identifier, truncate_identifier};
use crate::port::port_api::canonicalize_path;
use crate::pg_config_manual::MAXPGPATH;
use crate::miscadmin::{TimestampTz, ssize_t};
use crate::varatt::VARHDRSZ_SHORT;
use crate::common::unicode_version::PG_UNICODE_VERSION;
use crate::c::{bytea, text, Max, Min};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use core::ffi::{c_char, c_int, c_void};

use crate::access::common::detoast::{
    pg_detoast_datum_copy, pg_detoast_datum_slice, toast_raw_datum_size,
};
use crate::common::int::{pg_add_s32_overflow, pg_mul_s32_overflow};
use crate::executor::nodeAgg::AggCheckCallContext;
use crate::lib::stringinfo::{appendBinaryStringInfo, makeStringInfo};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_copymsgbytes, pq_endtypsend, pq_getmsgtext, pq_sendtext,
};
use crate::mb::pg_wchar::PG_UTF8;
use crate::nodes::nodes::Node;
use crate::utils::adt::encode::{hex_decode_safe, hex_encode};
use crate::utils::adt::pg_locale::{pg_locale_t, pg_newlocale_from_collation, pg_strncoll};
use crate::utils::bytea::{bytea_output, BYTEA_OUTPUT_ESCAPE, BYTEA_OUTPUT_HEX};
use crate::utils::mb::mbutils::{
    pg_database_encoding_max_length, pg_mbcliplen, pg_mblen_range, pg_mblen_unbounded,
    pg_mblen_with_len, pg_mbstrlen_with_len, GetDatabaseEncoding,
};
use crate::c::OidIsValid;
use crate::DirectFunctionCall1;
use crate::utils::cache::lsyscache::{get_typlen, get_typlenbyvalalign};
use crate::access::common::detoast::{toast_datum_size, varatt_external};
use crate::access::common::toast_compression::{
    toast_get_compression_id, ToastCompressionId, TOAST_INVALID_COMPRESSION_ID,
    TOAST_PGLZ_COMPRESSION_ID, TOAST_LZ4_COMPRESSION_ID,
};
use crate::libpq::pqformat::{
    pq_sendint, pq_sendbytes, pq_getmsgint, pq_getmsgbytes, pq_getmsgend,
};
use crate::lib::stringinfo::{initReadOnlyStringInfo, appendStringInfoSpaces};
use crate::utils::mb::mbutils::{pg_mbstrlen, pg_unicode_to_server};
use crate::utils::builtins::{quote_literal_cstr, pg_strtoint32};
use crate::catalog::objectaddress_impl::quote_identifier;
use crate::postgres::{DatumGetInt16, DatumGetInt32};
use crate::utils::adt::arrayfuncs::deconstruct_array;
use crate::catalog::pg_type_d::{INT2OID, INT4OID};
use crate::common::unicode_norm::{
    unicode_normalize, unicode_is_normalized_quickcheck, UnicodeNormalizationForm,
    UnicodeNormalizationQC, UNICODE_NFC, UNICODE_NFD, UNICODE_NFKC, UNICODE_NFKD,
};
use crate::common::unicode_normprops_table::{UNICODE_NORM_QC_YES, UNICODE_NORM_QC_NO};
use crate::common::unicode_category::unicode_category;
use crate::common::unicode_category_table::PG_U_UNASSIGNED;
use crate::mb::pg_wchar::{
    utf8_to_unicode, unicode_to_utf8, MAX_UNICODE_EQUIVALENT_STRING,
    is_valid_unicode_codepoint, is_utf16_surrogate_first, is_utf16_surrogate_second,
    surrogate_pair_to_codepoint,
};
use crate::mb::wchar::pg_utf_mblen;
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::foreign::foreign::ClosestMatchState;
use crate::{PG_RETURN_POINTER, PG_RETURN_OID, appendStringInfoCharMacro};
use crate::{foreach, current_cell};

unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) { crate::utils::sort::tuplestore::tuplestore_putvalues(_state as _, _tdesc as _, _values as _, _isnull as _) }

/*
 * varstr_levenshtein_less_equal and MAX_LEVENSHTEIN_STRLEN come from
 * levenshtein.c (#include'd twice into varlena.c).  The port folds the
 * Levenshtein variants into levenshtein.rs::levenshtein_internal and does not
 * export a standalone varstr_levenshtein_less_equal yet; stub locally.
 */
const MAX_LEVENSHTEIN_STRLEN: c_int = 255;
unsafe fn varstr_levenshtein_less_equal(
    _source: *const c_char,
    _slen: c_int,
    _target: *const c_char,
    _tlen: c_int,
    _ins_c: c_int,
    _del_c: c_int,
    _sub_c: c_int,
    _max_d: c_int,
    _trusted: bool,
) -> c_int {
    // TODO(pg-port): wire to levenshtein_internal (levenshtein.c LEVENSHTEIN_LESS_EQUAL)
    _max_d + 1
}

extern "C" {
    #[link_name = "strchr"]
    fn varlena_strchr2(s: *const c_char, c: c_int) -> *mut c_char;
}

/* CHECK_FOR_INTERRUPTS is a fn in miscadmin; this file uses macro-call syntax. */
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {
        crate::miscadmin::CHECK_FOR_INTERRUPTS()
    };
}

/* funcapi.c not yet wired into the module tree; faithful local copies. */
const MAT_SRF_USE_EXPECTED_DESC: u32 = 0x01;
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: u32) {
    // TODO(pg-port): port InitMaterializedSRF from utils/fmgr/funcapi.c
}

extern "C" {
    #[link_name = "memchr"]
    fn varlena_memchr(s: *const c_void, c: c_int, n: usize) -> *mut c_void;
    #[link_name = "strchr"]
    fn varlena_strchr(s: *const c_char, c: c_int) -> *mut c_char;
    #[link_name = "memmove"]
    fn varlena_memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    #[link_name = "strncpy"]
    fn varlena_strncpy(dst: *mut c_char, src: *const c_char, n: usize) -> *mut c_char;
}
#[inline]
unsafe fn memchr(s: *const c_void, c: c_int, n: usize) -> *mut c_void {
    varlena_memchr(s, c, n)
}

/* array.h macros are local per-file in this port; faithful local copies. */
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        crate::utils::adt::xml::DatumGetArrayTypeP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_RETURN_ARRAYTYPE_P {
    ($x:expr) => {
        return $crate::postgres::PointerGetDatum($x as *const core::ffi::c_void)
    };
}

const ERRCODE_INVALID_REGULAR_EXPRESSION: c_int = 0;
const ERRCODE_INVALID_NAME: c_int = 0;
const ERRCODE_INDETERMINATE_COLLATION: c_int = 0;
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_SUBSTRING_ERROR: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;
const ERRCODE_ARRAY_SUBSCRIPT_ERROR: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_NULL_VALUE_NOT_ALLOWED: c_int = 0;
const ERRCODE_SYNTAX_ERROR: c_int = 0;

/*
 * check_collation_set
 *
 * Bail out if we don't have a collation for the comparison.
 */
unsafe fn check_collation_set(collid: Oid) {
    if !OidIsValid(collid) {
        /*
         * This typically means that the parser could not resolve a conflict
         * of implicit collations, so report it that way.
         */
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        // C also: errhint("Use the COLLATE clause to set the collation explicitly.")
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for string comparison")
        );
    }
}

/*
 * State for text_position_* functions.
 */
#[repr(C)]
pub struct TextPositionState {
    pub locale: pg_locale_t,           /* collation used for substring matching */
    pub is_multibyte_char_in_char: bool, /* need to check char boundaries? */
    pub greedy: bool,                  /* find longest possible substring? */

    pub str1: *mut c_char,             /* haystack string */
    pub str2: *mut c_char,             /* needle string */
    pub len1: c_int,                   /* string lengths in bytes */
    pub len2: c_int,

    /* Skip table for Boyer-Moore-Horspool search algorithm: */
    pub skiptablemask: c_int,          /* mask for ANDing with skiptable subscripts */
    pub skiptable: [c_int; 256],       /* skip distance for given mismatched char */

    pub last_match: *mut c_char,       /* pointer to last match in 'str1' */
    pub last_match_len: c_int,         /* length of last match */
    pub last_match_len_tmp: c_int,     /* same but for internal use */

    pub refpoint: *mut c_char,         /* pointer within original haystack string */
    pub refpos: c_int,                 /* 0-based character offset of the same point */
}

/*
 * makeStringAggState
 *	Initialize the aggregate state into the aggregate's memory context.
 */
unsafe fn makeStringAggState(fcinfo: FunctionCallInfo) -> StringInfo {
    let state: StringInfo;
    let mut aggcontext: MemoryContext = core::ptr::null_mut();
    let oldcontext: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut aggcontext) == 0 {
        /* cannot be called directly because of internal-type argument */
        elog!(ERROR, "string_agg_transfn called in non-aggregate context");
    }

    /*
     * Create state in aggregate context.  It'll stay there across subsequent
     * calls.
     */
    oldcontext = MemoryContextSwitchTo(aggcontext);
    state = makeStringInfo();
    MemoryContextSwitchTo(oldcontext);

    state
}

// libc strlen (string.h, via postgres.h).
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * cstring_to_text
 *
 * Create a text value from a null-terminated C string.  Freshly palloc'd with a
 * full-size (4-byte) VARHDR.
 *
 * # Safety
 * `s` is a valid NUL-terminated C string.
 */
#[no_mangle]
pub unsafe fn cstring_to_text(s: *const c_char) -> *mut text {
    cstring_to_text_with_len(s, strlen(s) as c_int)
}

/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length; the
 * string need not be null-terminated.
 *
 * # Safety
 * `s` is readable for `len` bytes.
 */
pub unsafe fn cstring_to_text_with_len(s: *const c_char, len: c_int) -> *mut text {
    let result: *mut text = palloc((len + VARHDRSZ) as Size) as *mut text;

    SET_VARSIZE(result as *mut c_char, len + VARHDRSZ);
    core::ptr::copy_nonoverlapping(s, VARDATA(result as *const c_char), len as usize);

    result
}

/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.  Supports a
 * compressed or toasted text value (via pg_detoast_datum_packed).
 *
 * # Safety
 * `t` points to a valid text datum.
 */
#[no_mangle]
pub unsafe fn text_to_cstring(t: *const text) -> *mut c_char {
    /* must cast away the const, unfortunately */
    let tunpacked: *mut text = pg_detoast_datum_packed(t as *mut c_void) as *mut text;
    let len = VARSIZE_ANY_EXHDR(tunpacked as *const c_char) as usize;
    let result: *mut c_char;

    result = palloc(len + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(VARDATA_ANY(tunpacked as *const c_char), result, len);
    *result.add(len) = 0;

    if tunpacked != t as *mut text {
        pfree(tunpacked as *mut c_void);
    }

    result
}

/// `TextDatumGetCString(d)` (a builtins.h macro) - text_to_cstring of a text Datum.
///
/// # Safety
/// `d` is a Datum holding a text pointer.
#[inline]
pub unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    text_to_cstring(DatumGetPointer(d) as *const text)
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 *		byteain			- converts from printable representation of byte array
 *
 *		Non-printable characters must be passed as '\nnn' (octal) and are
 *		converted to internal form.  '\' must be passed as '\\'.
 *		ereport(ERROR, ...) if bad form.
 *
 *		BUGS:
 *				The input is scanned twice.
 *				The error checking of input is minimal.
 */
pub unsafe fn byteain(fcinfo: FunctionCallInfo) -> Datum {
    let inputText: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)
    let escontext: *mut Node = (*fcinfo).context as *mut Node;
    let mut tp: *mut c_char;
    let mut rp: *mut c_char;
    let mut bc: c_int;
    let result: *mut bytea;

    /* Recognize hex input */
    if *inputText == b'\\' as c_char && *inputText.add(1) == b'x' as c_char {
        let len = strlen(inputText);

        bc = ((len - 2) / 2) as c_int + VARHDRSZ; /* maximum possible length */
        result = palloc(bc as Size) as *mut bytea;
        bc = hex_decode_safe(
            inputText.add(2),
            len - 2,
            VARDATA(result as *const c_char),
            escontext,
        ) as c_int;
        SET_VARSIZE(result as *mut c_char, bc + VARHDRSZ); /* actual length */

        return PointerGetDatum(result as *const c_void); // PG_RETURN_BYTEA_P
    }

    /* Else, it's the traditional escaped style */
    bc = 0;
    tp = inputText;
    while *tp != 0 {
        if *tp != b'\\' as c_char {
            tp = tp.add(1);
        } else if *tp == b'\\' as c_char
            && (*tp.add(1) >= b'0' as c_char && *tp.add(1) <= b'3' as c_char)
            && (*tp.add(2) >= b'0' as c_char && *tp.add(2) <= b'7' as c_char)
            && (*tp.add(3) >= b'0' as c_char && *tp.add(3) <= b'7' as c_char)
        {
            tp = tp.add(4);
        } else if *tp == b'\\' as c_char && *tp.add(1) == b'\\' as c_char {
            tp = tp.add(2);
        } else {
            /*
             * one backslash, not followed by another or ### valid octal
             */
            let _ = escontext;
            // C: ereturn(escontext, (Datum) 0, ...)
            let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
            ereport!(
                ERROR,
                errmsg!("invalid input syntax for type {}", "bytea")
            );
        }
        bc += 1;
    }

    bc += VARHDRSZ;

    let result = palloc(bc as Size) as *mut bytea;
    SET_VARSIZE(result as *mut c_char, bc);

    tp = inputText;
    rp = VARDATA(result as *const c_char);
    while *tp != 0 {
        if *tp != b'\\' as c_char {
            *rp = *tp;
            rp = rp.add(1);
            tp = tp.add(1);
        } else if *tp == b'\\' as c_char
            && (*tp.add(1) >= b'0' as c_char && *tp.add(1) <= b'3' as c_char)
            && (*tp.add(2) >= b'0' as c_char && *tp.add(2) <= b'7' as c_char)
            && (*tp.add(3) >= b'0' as c_char && *tp.add(3) <= b'7' as c_char)
        {
            bc = (*tp.add(1) as c_int) - ('0' as c_int); // VAL(tp[1])
            bc <<= 3;
            bc += (*tp.add(2) as c_int) - ('0' as c_int); // VAL(tp[2])
            bc <<= 3;
            *rp = (bc + ((*tp.add(3) as c_int) - ('0' as c_int))) as c_char; // VAL(tp[3])
            rp = rp.add(1);

            tp = tp.add(4);
        } else if *tp == b'\\' as c_char && *tp.add(1) == b'\\' as c_char {
            *rp = b'\\' as c_char;
            rp = rp.add(1);
            tp = tp.add(2);
        } else {
            /*
             * We should never get here. The first pass should not allow it.
             */
            let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
            ereport!(
                ERROR,
                errmsg!("invalid input syntax for type {}", "bytea")
            );
        }
    }

    PointerGetDatum(result as *const c_void) // PG_RETURN_BYTEA_P
}

/*
 *		byteaout		- converts to printable representation of byte array
 *
 *		In the traditional escaped format, non-printable characters are
 *		printed as '\nnn' (octal) and '\' as '\\'.
 */
pub unsafe fn byteaout(fcinfo: FunctionCallInfo) -> Datum {
    let vlena: *mut bytea = pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 0) as *mut c_void)
        as *mut bytea; // PG_GETARG_BYTEA_PP(0)
    let result: *mut c_char;
    let mut rp: *mut c_char;

    if bytea_output == BYTEA_OUTPUT_HEX {
        /* Print hex format */
        result =
            palloc((VARSIZE_ANY_EXHDR(vlena as *const c_char) as Size) * 2 + 2 + 1) as *mut c_char;
        rp = result;
        *rp = b'\\' as c_char;
        rp = rp.add(1);
        *rp = b'x' as c_char;
        rp = rp.add(1);
        rp = rp.add(hex_encode(
            VARDATA_ANY(vlena as *const c_char),
            VARSIZE_ANY_EXHDR(vlena as *const c_char) as usize,
            rp,
        ) as usize);
    } else if bytea_output == BYTEA_OUTPUT_ESCAPE {
        /* Print traditional escaped format */
        let mut vp: *mut c_char;
        let mut len: u64;
        let mut i: c_int;

        len = 1; /* empty string has 1 char */
        vp = VARDATA_ANY(vlena as *const c_char);
        i = VARSIZE_ANY_EXHDR(vlena as *const c_char) as i32;
        while i != 0 {
            if *vp == b'\\' as c_char {
                len += 2;
            } else if (*vp as u8) < 0x20 || (*vp as u8) > 0x7e {
                len += 4;
            } else {
                len += 1;
            }
            i -= 1;
            vp = vp.add(1);
        }

        /*
         * In principle len can't overflow uint32 if the input fit in 1GB, but
         * for safety let's check rather than relying on palloc's internal
         * check.
         */
        if len > MaxAllocSize as u64 {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!("result of bytea output conversion is too large")
            );
        }
        result = palloc(len as Size) as *mut c_char;
        rp = result;

        vp = VARDATA_ANY(vlena as *const c_char);
        i = VARSIZE_ANY_EXHDR(vlena as *const c_char) as i32;
        while i != 0 {
            if *vp == b'\\' as c_char {
                *rp = b'\\' as c_char;
                rp = rp.add(1);
                *rp = b'\\' as c_char;
                rp = rp.add(1);
            } else if (*vp as u8) < 0x20 || (*vp as u8) > 0x7e {
                let mut val: c_int; /* holds unprintable chars */

                val = *vp as c_int;
                *rp.add(0) = b'\\' as c_char;
                *rp.add(3) = ((val & 0o7) + ('0' as c_int)) as c_char; // DIG
                val >>= 3;
                *rp.add(2) = ((val & 0o7) + ('0' as c_int)) as c_char;
                val >>= 3;
                *rp.add(1) = ((val & 0o3) + ('0' as c_int)) as c_char;
                rp = rp.add(4);
            } else {
                *rp = *vp;
                rp = rp.add(1);
            }
            i -= 1;
            vp = vp.add(1);
        }
    } else {
        elog!(
            ERROR,
            "unrecognized \"bytea_output\" setting: {}",
            bytea_output
        );
        result = core::ptr::null_mut(); /* keep compiler quiet */
        rp = result; /* keep compiler quiet */
    }
    *rp = b'\0' as c_char;
    PG_RETURN_CSTRING!(result);
}

/*
 *		textin			- converts cstring to internal representation
 */
pub unsafe fn textin(fcinfo: FunctionCallInfo) -> Datum {
    let input_text: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING

    return PointerGetDatum(cstring_to_text(input_text) as *const c_void); // PG_RETURN_TEXT_P
}

/*
 *		textout			- converts internal representation to cstring
 */
pub unsafe fn textout(fcinfo: FunctionCallInfo) -> Datum {
    let txt: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    PG_RETURN_CSTRING!(TextDatumGetCString(txt));
}

/*
 *		textrecv			- converts external binary format to text
 */
pub unsafe fn textrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let result: *mut text;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);

    result = cstring_to_text_with_len(str, nbytes);
    pfree(str as *mut c_void);
    PointerGetDatum(result as *const c_void) // PG_RETURN_TEXT_P
}

/*
 *		textsend			- converts text to binary format
 */
pub unsafe fn textsend(fcinfo: FunctionCallInfo) -> Datum {
    let t: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 0) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(0)
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendtext(
        &mut buf,
        VARDATA_ANY(t as *const c_char),
        VARSIZE_ANY_EXHDR(t as *const c_char) as i32,
    );
    PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void) // PG_RETURN_BYTEA_P
}

/*
 * Returns version of Unicode used by Postgres in "major.minor" format (the
 * version of Unicode appropriate to the server encoding, as a text value).
 */
pub unsafe fn unicode_version(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TEXT_P!(cstring_to_text_with_len(
        PG_UNICODE_VERSION.as_ptr() as *const c_char,
        PG_UNICODE_VERSION.len() as c_int,
    ))
}

/*
 * Returns version of Unicode used by ICU, if enabled; otherwise NULL.
 */
pub unsafe fn icu_unicode_version(fcinfo: FunctionCallInfo) -> Datum {
    /* #ifdef USE_ICU: PG_RETURN_TEXT_P(cstring_to_text(U_UNICODE_VERSION)) */
    /* #else (this build has no ICU): */
    PG_RETURN_NULL!(fcinfo)
}

/*
 * Check that the first n characters of instr are all hex digits.
 */
unsafe fn isxdigits_n(instr: *const c_char, n: usize) -> bool {
    for i in 0..n {
        if !(*instr.add(i) as u8 as char).is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

/*
 * Convert a single hexadecimal digit to its integer value (0-15).
 */
unsafe fn hexval(c: u8) -> c_int {
    if c >= b'0' && c <= b'9' {
        return (c - b'0') as c_int;
    }
    if c >= b'a' && c <= b'f' {
        return (c - b'a' + 0xA) as c_int;
    }
    if c >= b'A' && c <= b'F' {
        return (c - b'A' + 0xA) as c_int;
    }
    elog!(ERROR, "invalid hexadecimal digit");
    0 /* not reached */
}

/*
 * Parse the first n characters of instr as a hexadecimal number.
 */
unsafe fn hexval_n(instr: *const c_char, n: usize) -> u32 {
    let mut result: u32 = 0;

    for i in 0..n {
        result += (hexval(*instr.add(i) as u8) as u32) << (4 * (n - i - 1));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetCString};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn text_io_and_cstring_roundtrip() {
        unsafe {
            // cstring_to_text -> text_to_cstring round trip
            let t = cstring_to_text(c"hello, world".as_ptr());
            let back = text_to_cstring(t);
            assert!(cstr_eq(back, "hello, world"));

            // with_len (non-NUL-terminated slice of 5)
            let t2 = cstring_to_text_with_len(c"abcdefgh".as_ptr(), 5);
            let back2 = text_to_cstring(t2);
            assert!(cstr_eq(back2, "abcde"));

            // textin -> textout through the fmgr dispatch
            let d = DirectFunctionCall1Coll(textin, InvalidOid, CStringGetDatum(c"PepperDB".as_ptr()));
            let s = DatumGetCString(DirectFunctionCall1Coll(textout, InvalidOid, d));
            assert!(cstr_eq(s, "PepperDB"));

            // empty string
            let e = text_to_cstring(cstring_to_text(c"".as_ptr()));
            assert!(cstr_eq(e, ""));
        }
    }
}

/*
 * text_to_cstring_buffer
 *
 * Copy a text value into a caller-supplied buffer of size dst_len.
 *
 * The text value is truncated (if necessary) to fit, and is encoding-safely
 * NUL-terminated.
 *
 * # Safety
 * `src` points to a valid text datum; `dst` is writable for `dst_len` bytes.
 */
pub unsafe fn text_to_cstring_buffer(src: *const text, dst: *mut c_char, dst_len: usize) {
    /* must cast away the const, unfortunately */
    let srcunpacked: *mut text = pg_detoast_datum_packed(src as *mut c_void) as *mut text;
    let src_len = VARSIZE_ANY_EXHDR(srcunpacked as *const c_char) as usize;

    if dst_len > 0 {
        let mut dst_len = dst_len - 1;
        if dst_len >= src_len {
            dst_len = src_len;
        } else {
            /* ensure truncation is encoding-safe */
            dst_len = pg_mbcliplen(
                VARDATA_ANY(srcunpacked as *const c_char),
                src_len as c_int,
                dst_len as c_int,
            ) as usize;
        }
        core::ptr::copy_nonoverlapping(
            VARDATA_ANY(srcunpacked as *const c_char),
            dst,
            dst_len,
        );
        *dst.add(dst_len) = b'\0' as c_char;
    }

    if srcunpacked != src as *mut text {
        pfree(srcunpacked as *mut c_void);
    }
}

/*
 *		bytearecv			- converts external binary format to bytea
 */
pub unsafe fn bytearecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let result: *mut bytea;
    let nbytes: c_int;

    nbytes = (*buf).len - (*buf).cursor;
    result = palloc((nbytes + VARHDRSZ) as Size) as *mut bytea;
    SET_VARSIZE(result as *mut c_char, nbytes + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result as *const c_char) as *mut c_void, nbytes);
    PointerGetDatum(result as *const c_void) // PG_RETURN_BYTEA_P
}

/*
 *		byteasend			- converts bytea to binary format
 *
 * This is a special case: just copy the input...
 */
pub unsafe fn byteasend(fcinfo: FunctionCallInfo) -> Datum {
    let vlena: *mut bytea =
        pg_detoast_datum_copy(PG_GETARG_DATUM!(fcinfo, 0) as *mut crate::c::varlena) as *mut bytea; // PG_GETARG_BYTEA_P_COPY(0)

    PointerGetDatum(vlena as *const c_void) // PG_RETURN_BYTEA_P
}

pub unsafe fn bytea_string_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: StringInfo;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        core::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as StringInfo
    };

    /* Append the value unless null, preceding it with the delimiter. */
    if !PG_ARGISNULL!(fcinfo, 1) {
        let value: *mut bytea =
            pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 1) as *mut c_void) as *mut bytea; // PG_GETARG_BYTEA_PP(1)
        let mut isfirst = false;

        /*
         * You might think we can just throw away the first delimiter, however
         * we must keep it as we may be a parallel worker doing partial
         * aggregation building a state to send to the main process.  We need
         * to keep the delimiter of every aggregation so that the combine
         * function can properly join up the strings of two separately
         * partially aggregated results.  The first delimiter is only stripped
         * off in the final function.  To know how much to strip off the front
         * of the string, we store the length of the first delimiter in the
         * StringInfo's cursor field, which we don't otherwise need here.
         */
        if state.is_null() {
            state = makeStringAggState(fcinfo);
            isfirst = true;
        }

        if !PG_ARGISNULL!(fcinfo, 2) {
            let delim: *mut bytea =
                pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 2) as *mut c_void) as *mut bytea; // PG_GETARG_BYTEA_PP(2)

            appendBinaryStringInfo(
                state,
                VARDATA_ANY(delim as *const c_char) as *const c_void,
                VARSIZE_ANY_EXHDR(delim as *const c_char) as i32,
            );
            if isfirst {
                (*state).cursor = VARSIZE_ANY_EXHDR(delim as *const c_char) as c_int;
            }
        }

        appendBinaryStringInfo(
            state,
            VARDATA_ANY(value as *const c_char) as *const c_void,
            VARSIZE_ANY_EXHDR(value as *const c_char) as i32,
        );
    }

    /*
     * The transition type for string_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    if !state.is_null() {
        return PointerGetDatum(state as *const c_void); // PG_RETURN_POINTER
    }
    PG_RETURN_NULL!(fcinfo);
}

pub unsafe fn bytea_string_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let state: StringInfo;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, core::ptr::null_mut()) != 0);

    state = if PG_ARGISNULL!(fcinfo, 0) {
        core::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as StringInfo
    };

    if !state.is_null() {
        /* As per comment in transfn, strip data before the cursor position */
        let result: *mut bytea;
        let strippedlen: c_int = (*state).len - (*state).cursor;

        result = palloc((strippedlen + VARHDRSZ) as Size) as *mut bytea;
        SET_VARSIZE(result as *mut c_char, strippedlen + VARHDRSZ);
        core::ptr::copy_nonoverlapping(
            (*state).data.add((*state).cursor as usize),
            VARDATA(result as *const c_char),
            strippedlen as usize,
        );
        PointerGetDatum(result as *const c_void) // PG_RETURN_BYTEA_P
    } else {
        PG_RETURN_NULL!(fcinfo);
    }
}

/*
 *		unknownin			- converts cstring to internal representation
 */
pub unsafe fn unknownin(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)

    /* representation is same as cstring */
    PG_RETURN_CSTRING!(pstrdup(str));
}

/*
 *		unknownout			- converts internal representation to cstring
 */
pub unsafe fn unknownout(fcinfo: FunctionCallInfo) -> Datum {
    /* representation is same as cstring */
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)

    PG_RETURN_CSTRING!(pstrdup(str));
}

/*
 *		unknownrecv			- converts external binary format to unknown
 */
pub unsafe fn unknownrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);
    /* representation is same as cstring */
    PG_RETURN_CSTRING!(str);
}

/*
 *		unknownsend			- converts unknown to binary format
 */
pub unsafe fn unknownsend(fcinfo: FunctionCallInfo) -> Datum {
    /* representation is same as cstring */
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendtext(&mut buf, str, strlen(str) as c_int);
    PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void) // PG_RETURN_BYTEA_P
}

/* ========== PUBLIC ROUTINES ========== */

/*
 * textlen -
 *	  returns the logical length of a text*
 *	   (which is less than the VARSIZE of the text*)
 */
pub unsafe fn textlen(fcinfo: FunctionCallInfo) -> Datum {
    let str: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    /* try to avoid decompressing argument */
    PG_RETURN_INT32!(text_length(str));
}

/*
 * text_length -
 *	Does the real work for textlen()
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.  Note that the argument is passed as a Datum, to indicate that
 *	it may still be in compressed form.  We can avoid decompressing it at all
 *	in some cases.
 */
unsafe fn text_length(str: Datum) -> int32 {
    /* fastpath when max encoding length is one */
    if pg_database_encoding_max_length() == 1 {
        (toast_raw_datum_size(str) as int32) - VARHDRSZ // PG_RETURN_INT32
    } else {
        let t: *mut text = pg_detoast_datum_packed(DatumGetPointer(str) as *mut c_void) as *mut text; // DatumGetTextPP

        pg_mbstrlen_with_len(
            VARDATA_ANY(t as *const c_char),
            VARSIZE_ANY_EXHDR(t as *const c_char) as i32,
        ) // PG_RETURN_INT32
    }
}

/*
 * textoctetlen -
 *	  returns the physical length of a text*
 *	   (which is less than the VARSIZE of the text*)
 */
pub unsafe fn textoctetlen(fcinfo: FunctionCallInfo) -> Datum {
    let str: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    /* We need not detoast the input at all */
    PG_RETURN_INT32!((toast_raw_datum_size(str) as int32) - VARHDRSZ);
}

/*
 * textcat -
 *	  takes two text* and returns a text* that is the concatenation of
 *	  the two.
 */
pub unsafe fn textcat(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 0) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(0)
    let t2: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 1) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(1)

    PointerGetDatum(text_catenate(t1, t2) as *const c_void) // PG_RETURN_TEXT_P
}

/*
 * text_catenate
 *	Guts of textcat(), broken out so it can be used by other functions
 *
 * Arguments can be in short-header form, but not compressed or out-of-line
 */
unsafe fn text_catenate(t1: *mut text, t2: *mut text) -> *mut text {
    let result: *mut text;
    let mut len1: c_int;
    let mut len2: c_int;
    let len: c_int;
    let ptr: *mut c_char;

    len1 = VARSIZE_ANY_EXHDR(t1 as *const c_char) as i32;
    len2 = VARSIZE_ANY_EXHDR(t2 as *const c_char) as i32;

    /* paranoia ... probably should throw error instead? */
    if len1 < 0 {
        len1 = 0;
    }
    if len2 < 0 {
        len2 = 0;
    }

    len = len1 + len2 + VARHDRSZ;
    result = palloc(len as Size) as *mut text;

    /* Set size of result string... */
    SET_VARSIZE(result as *mut c_char, len);

    /* Fill data field of result string... */
    ptr = VARDATA(result as *const c_char);
    if len1 > 0 {
        core::ptr::copy_nonoverlapping(VARDATA_ANY(t1 as *const c_char), ptr, len1 as usize);
    }
    if len2 > 0 {
        core::ptr::copy_nonoverlapping(
            VARDATA_ANY(t2 as *const c_char),
            ptr.add(len1 as usize),
            len2 as usize,
        );
    }

    result
}

/*
 * charlen_to_bytelen()
 *	Compute the number of bytes occupied by n characters starting at *p
 */
unsafe fn charlen_to_bytelen(p: *const c_char, mut n: c_int) -> c_int {
    if pg_database_encoding_max_length() == 1 {
        /* Optimization for single-byte encodings */
        n
    } else {
        let mut s: *const c_char;

        s = p;
        while n > 0 {
            s = s.add(pg_mblen_unbounded(s) as usize); /* caller verified encoding */
            n -= 1;
        }

        (s as isize - p as isize) as c_int
    }
}

/*
 * text_substr()
 * Return a substring starting at the specified position.
 */
pub unsafe fn text_substr(fcinfo: FunctionCallInfo) -> Datum {
    PointerGetDatum(text_substring(
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        PG_GETARG_INT32!(fcinfo, 2),
        false,
    ) as *const c_void) // PG_RETURN_TEXT_P
}

/*
 * text_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
pub unsafe fn text_substr_no_len(fcinfo: FunctionCallInfo) -> Datum {
    PointerGetDatum(text_substring(
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        -1,
        true,
    ) as *const c_void) // PG_RETURN_TEXT_P
}

/*
 * text_substring -
 *	Does the real work for text_substr() and text_substr_no_len()
 *
 *	The result is always a freshly palloc'd datum.
 */
unsafe fn text_substring(
    str: Datum,
    start: int32,
    length: int32,
    length_not_specified: bool,
) -> *mut text {
    let eml: int32 = pg_database_encoding_max_length();
    let s: int32 = start; /* start position */
    let s1: int32; /* adjusted start position */
    let mut l1: int32; /* adjusted substring length */
    let mut e: int32 = 0; /* end position, exclusive */

    /*
     * SQL99 says S can be zero or negative (which we don't document), but we
     * still must fetch from the start of the string.
     */
    s1 = Max(s, 1);

    /* life is easy if the encoding max length is 1 */
    if eml == 1 {
        if length_not_specified {
            /* special case - get length to end of string */
            l1 = -1;
        } else if length < 0 {
            /* SQL99 says to throw an error for E < S, i.e., negative length */
            let _ = errcode(ERRCODE_SUBSTRING_ERROR);
            ereport!(ERROR, errmsg!("negative substring length not allowed"));
            #[allow(unreachable_code)]
            {
                l1 = -1; /* silence stupider compilers */
            }
        } else if pg_add_s32_overflow(s, length, &mut e) {
            /*
             * L could be large enough for S + L to overflow, in which case
             * the substring must run to end of string.
             */
            l1 = -1;
        } else {
            /*
             * A zero or negative value for the end position can happen if the
             * start was negative or one. SQL99 says to return a zero-length
             * string.
             */
            if e < 1 {
                return cstring_to_text(c"".as_ptr());
            }

            l1 = e - s1;
        }

        /*
         * If the start position is past the end of the string, SQL99 says to
         * return a zero-length string -- DatumGetTextPSlice() will do that
         * for us.  We need only convert S1 to zero-based starting position.
         */
        return pg_detoast_datum_slice(DatumGetPointer(str) as *mut crate::c::varlena, s1 - 1, l1)
            as *mut text; // DatumGetTextPSlice(str, S1 - 1, L1)
    } else if eml > 1 {
        /*
         * When encoding max length is > 1, we can't get LC without
         * detoasting, so we'll grab a conservatively large slice now and go
         * back later to do the right thing
         */
        let slice_start: int32;
        let mut slice_size: int32 = 0;
        let slice_strlen: int32;
        let slice_len: int32;
        let slice: *mut text;
        let e1: int32;
        let mut i: int32;
        let mut p: *mut c_char;
        let s_ptr: *mut c_char;
        let ret: *mut text;

        /*
         * We need to start at position zero because there is no way to know
         * in advance which byte offset corresponds to the supplied start
         * position.
         */
        slice_start = 0;

        if length_not_specified {
            /* special case - get length to end of string */
            e = -1;
            slice_size = -1;
            l1 = -1;
        } else if length < 0 {
            /* SQL99 says to throw an error for E < S, i.e., negative length */
            let _ = errcode(ERRCODE_SUBSTRING_ERROR);
            ereport!(ERROR, errmsg!("negative substring length not allowed"));
            #[allow(unreachable_code)]
            {
                e = -1;
                slice_size = -1;
                l1 = -1; /* silence stupider compilers */
            }
        } else if pg_add_s32_overflow(s, length, &mut e) {
            /*
             * L could be large enough for S + L to overflow, in which case
             * the substring must run to end of string.
             */
            slice_size = -1;
            l1 = -1;
        } else {
            /*
             * Ending at position 1, exclusive, obviously yields an empty
             * string.  A zero or negative value can happen if the start was
             * negative or one. SQL99 says to return a zero-length string.
             */
            if e <= 1 {
                return cstring_to_text(c"".as_ptr());
            }

            /*
             * if E is past the end of the string, the tuple toaster will
             * truncate the length for us
             */
            l1 = e - s1;

            /*
             * Total slice size in bytes can't be any longer than the
             * inclusive end position times the encoding max length.  If that
             * overflows, we can just use -1.
             */
            if pg_mul_s32_overflow(e - 1, eml, &mut slice_size) {
                slice_size = -1;
            }
        }

        /*
         * If we're working with an untoasted source, no need to do an extra
         * copying step.
         */
        if VARATT_IS_COMPRESSED(DatumGetPointer(str) as *const c_char)
            || VARATT_IS_EXTERNAL(DatumGetPointer(str) as *const c_char)
        {
            slice = pg_detoast_datum_slice(
                DatumGetPointer(str) as *mut crate::c::varlena,
                slice_start,
                slice_size,
            ) as *mut text; // DatumGetTextPSlice
        } else {
            slice = DatumGetPointer(str) as *mut text;
        }

        /* see if we got back an empty string */
        slice_len = VARSIZE_ANY_EXHDR(slice as *const c_char) as i32;
        if slice_len == 0 {
            if slice != DatumGetPointer(str) as *mut text {
                pfree(slice as *mut c_void);
            }
            return cstring_to_text(c"".as_ptr());
        }

        /*
         * Now we can get the actual length of the slice in MB characters,
         * stopping at the end of the substring.
         */
        slice_strlen = if slice_size == -1 {
            pg_mbstrlen_with_len(VARDATA_ANY(slice as *const c_char), slice_len)
        } else {
            pg_mbcharcliplen_chars(VARDATA_ANY(slice as *const c_char), slice_len, e - 1)
        };

        /*
         * Check that the start position wasn't > slice_strlen. If so, SQL99
         * says to return a zero-length string.
         */
        if s1 > slice_strlen {
            if slice != DatumGetPointer(str) as *mut text {
                pfree(slice as *mut c_void);
            }
            return cstring_to_text(c"".as_ptr());
        }

        /*
         * Adjust L1 and E1 now that we know the slice string length. Again
         * remember that S1 is one based, and slice_start is zero based.
         */
        if l1 > -1 {
            e1 = Min(s1 + l1, slice_start + 1 + slice_strlen);
        } else {
            e1 = slice_start + 1 + slice_strlen;
        }

        /*
         * Find the start position in the slice; remember S1 is not zero based
         */
        p = VARDATA_ANY(slice as *const c_char);
        i = 0;
        while i < s1 - 1 {
            p = p.add(pg_mblen_unbounded(p) as usize);
            i += 1;
        }

        /* hang onto a pointer to our start position */
        s_ptr = p;

        /*
         * Count the actual bytes used by the substring of the requested
         * length.
         */
        i = s1;
        while i < e1 {
            p = p.add(pg_mblen_unbounded(p) as usize);
            i += 1;
        }

        let bytelen = (p as isize - s_ptr as isize) as usize;
        ret = palloc(VARHDRSZ as usize + bytelen) as *mut text;
        SET_VARSIZE(ret as *mut c_char, VARHDRSZ + bytelen as int32);
        core::ptr::copy_nonoverlapping(s_ptr, VARDATA(ret as *const c_char), bytelen);

        if slice != DatumGetPointer(str) as *mut text {
            pfree(slice as *mut c_void);
        }

        ret
    } else {
        elog!(ERROR, "invalid backend encoding: encoding max length < 1");
        #[allow(unreachable_code)]
        {
            /* not reached: suppress compiler warning */
            core::ptr::null_mut()
        }
    }
}

/*
 * pg_mbcharcliplen_chars -
 *	Mirror pg_mbcharcliplen(), except return value unit is chars, not bytes.
 */
unsafe fn pg_mbcharcliplen_chars(mut mbstr: *const c_char, mut len: c_int, limit: c_int) -> c_int {
    let mut nch: c_int = 0;
    let mut l: c_int;

    Assert!(len > 0);
    Assert!(limit > 0);
    Assert!(pg_database_encoding_max_length() > 1);

    while len > 0 && *mbstr != 0 {
        l = pg_mblen_with_len(mbstr, len);
        nch += 1;
        if nch == limit {
            break;
        }
        len -= l;
        mbstr = mbstr.add(l as usize);
    }
    nch
}

/*
 * textoverlay
 *	Replace specified substring of first string with second
 */
pub unsafe fn textoverlay(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 0) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(0)
    let t2: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 1) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(1)
    let sp: c_int = PG_GETARG_INT32!(fcinfo, 2); /* substring start position */
    let sl: c_int = PG_GETARG_INT32!(fcinfo, 3); /* substring length */

    PointerGetDatum(text_overlay(t1, t2, sp, sl) as *const c_void) // PG_RETURN_TEXT_P
}

pub unsafe fn textoverlay_no_len(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 0) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(0)
    let t2: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 1) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(1)
    let sp: c_int = PG_GETARG_INT32!(fcinfo, 2); /* substring start position */
    let sl: c_int;

    sl = text_length(PointerGetDatum(t2 as *const c_void)); /* defaults to length(t2) */
    PointerGetDatum(text_overlay(t1, t2, sp, sl) as *const c_void) // PG_RETURN_TEXT_P
}

unsafe fn text_overlay(t1: *mut text, t2: *mut text, sp: c_int, sl: c_int) -> *mut text {
    let mut result: *mut text;
    let s1: *mut text;
    let s2: *mut text;
    let mut sp_pl_sl: c_int = 0;

    /*
     * Check for possible integer-overflow cases.  For negative sp, throw a
     * "substring length" error because that's what should be expected
     * according to the spec's definition of OVERLAY().
     */
    if sp <= 0 {
        let _ = errcode(ERRCODE_SUBSTRING_ERROR);
        ereport!(ERROR, errmsg!("negative substring length not allowed"));
    }
    if pg_add_s32_overflow(sp, sl, &mut sp_pl_sl) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    s1 = text_substring(PointerGetDatum(t1 as *const c_void), 1, sp - 1, false);
    s2 = text_substring(PointerGetDatum(t1 as *const c_void), sp_pl_sl, -1, true);
    result = text_catenate(s1, t2);
    result = text_catenate(result, s2);

    result
}

/*
 * textpos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL POSITION() function.
 */
pub unsafe fn textpos(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 0) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(0)
    let search_str: *mut text =
        pg_detoast_datum_packed(PG_GETARG_DATUM!(fcinfo, 1) as *mut c_void) as *mut text; // PG_GETARG_TEXT_PP(1)

    PG_RETURN_INT32!(text_position(str, search_str, PG_GET_COLLATION!(fcinfo)) as int32);
}

/*
 * text_position -
 *	Does the real work for textpos()
 *
 * Result:
 *		Character index of the first matched char, starting from 1,
 *		or 0 if no match.
 */
unsafe fn text_position(t1: *mut text, t2: *mut text, collid: Oid) -> c_int {
    let mut state: TextPositionState = core::mem::zeroed();
    let result: c_int;

    check_collation_set(collid);

    /* Empty needle always matches at position 1 */
    if VARSIZE_ANY_EXHDR(t2 as *const c_char) < 1 {
        return 1;
    }

    /* Otherwise, can't match if haystack is shorter than needle */
    if VARSIZE_ANY_EXHDR(t1 as *const c_char) < VARSIZE_ANY_EXHDR(t2 as *const c_char)
        && (*pg_newlocale_from_collation(collid)).deterministic
    {
        return 0;
    }

    text_position_setup(t1, t2, collid, &mut state);
    /* don't need greedy mode here */
    state.greedy = false;

    if !text_position_next(&mut state) {
        result = 0;
    } else {
        result = text_position_get_match_pos(&mut state);
    }
    text_position_cleanup(&mut state);
    result
}

/*
 * text_position_setup, text_position_next, text_position_cleanup -
 *	Component steps of text_position()
 */
unsafe fn text_position_setup(
    t1: *mut text,
    t2: *mut text,
    collid: Oid,
    state: *mut TextPositionState,
) {
    let len1: c_int = VARSIZE_ANY_EXHDR(t1 as *const c_char) as i32;
    let len2: c_int = VARSIZE_ANY_EXHDR(t2 as *const c_char) as i32;

    check_collation_set(collid);

    (*state).locale = pg_newlocale_from_collation(collid);

    /*
     * Most callers need greedy mode, but some might want to unset this to
     * optimize.
     */
    (*state).greedy = true;

    Assert!(len2 > 0);

    /*
     * Even with a multi-byte encoding, we perform the search using the raw
     * byte sequence, ignoring multibyte issues.
     */
    if pg_database_encoding_max_length() == 1 {
        (*state).is_multibyte_char_in_char = false;
    } else if GetDatabaseEncoding() == PG_UTF8 {
        (*state).is_multibyte_char_in_char = false;
    } else {
        (*state).is_multibyte_char_in_char = true;
    }

    (*state).str1 = VARDATA_ANY(t1 as *const c_char);
    (*state).str2 = VARDATA_ANY(t2 as *const c_char);
    (*state).len1 = len1;
    (*state).len2 = len2;
    (*state).last_match = core::ptr::null_mut();
    (*state).refpoint = (*state).str1;
    (*state).refpos = 0;

    /*
     * Prepare the skip table for Boyer-Moore-Horspool searching.
     */
    if len1 >= len2 && len2 > 1 && (*(*state).locale).deterministic {
        let searchlength: c_int = len1 - len2;
        let skiptablemask: c_int;
        let last: c_int;
        let mut i: c_int;
        let str2: *const c_char = (*state).str2;

        /*
         * First we must determine how much of the skip table to use.
         */
        if searchlength < 16 {
            skiptablemask = 3;
        } else if searchlength < 64 {
            skiptablemask = 7;
        } else if searchlength < 128 {
            skiptablemask = 15;
        } else if searchlength < 512 {
            skiptablemask = 31;
        } else if searchlength < 2048 {
            skiptablemask = 63;
        } else if searchlength < 4096 {
            skiptablemask = 127;
        } else {
            skiptablemask = 255;
        }
        (*state).skiptablemask = skiptablemask;

        /*
         * Initialize the skip table.  We set all elements to the needle
         * length, since this is the correct skip distance for any character
         * not found in the needle.
         */
        i = 0;
        while i <= skiptablemask {
            (*state).skiptable[i as usize] = len2;
            i += 1;
        }

        /*
         * Now examine the needle.  For each character except the last one,
         * set the corresponding table element to the appropriate skip
         * distance.
         */
        last = len2 - 1;

        i = 0;
        while i < last {
            (*state).skiptable[((*str2.add(i as usize) as u8) as c_int & skiptablemask) as usize] =
                last - i;
            i += 1;
        }
    }
}

/*
 * Advance to the next match, starting from the end of the previous match
 * (or the beginning of the string, on first call).  Returns true if a match
 * is found.
 */
unsafe fn text_position_next(state: *mut TextPositionState) -> bool {
    let needle_len: c_int = (*state).len2;
    let mut start_ptr: *mut c_char;
    let mut matchptr: *mut c_char;

    if needle_len <= 0 {
        return false; /* result for empty pattern */
    }

    /* Start from the point right after the previous match. */
    if !(*state).last_match.is_null() {
        start_ptr = (*state).last_match.add((*state).last_match_len as usize);
    } else {
        start_ptr = (*state).str1;
    }

    loop {
        // retry:
        matchptr = text_position_next_internal(start_ptr, state);

        if matchptr.is_null() {
            return false;
        }

        /*
         * Found a match for the byte sequence.  If this is a multibyte encoding,
         * where one character's byte sequence can appear inside a longer
         * multi-byte character, we need to verify that the match was at a
         * character boundary, not in the middle of a multi-byte character.
         */
        if (*state).is_multibyte_char_in_char && (*(*state).locale).deterministic {
            let haystack_end: *const c_char = (*state).str1.add((*state).len1 as usize);
            let mut retry = false;

            /* Walk one character at a time, until we reach the match. */

            /* the search should never move backwards. */
            Assert!((*state).refpoint as *const c_char <= matchptr as *const c_char);

            while ((*state).refpoint as *const c_char) < (matchptr as *const c_char) {
                /* step to next character. */
                (*state).refpoint = (*state)
                    .refpoint
                    .add(pg_mblen_range((*state).refpoint, haystack_end) as usize);
                (*state).refpos += 1;

                /*
                 * If we stepped over the match's start position, then it was a
                 * false positive, where the byte sequence appeared in the middle
                 * of a multi-byte character.  Skip it, and continue the search at
                 * the next character boundary.
                 */
                if (*state).refpoint as *const c_char > matchptr as *const c_char {
                    start_ptr = (*state).refpoint;
                    retry = true;
                    break;
                }
            }
            if retry {
                continue; // goto retry
            }
        }

        (*state).last_match = matchptr;
        (*state).last_match_len = (*state).last_match_len_tmp;
        return true;
    }
}

/*
 * Subroutine of text_position_next().  This searches for the raw byte
 * sequence, ignoring any multi-byte encoding issues.  Returns the first
 * match starting at 'start_ptr', or NULL if no match is found.
 */
unsafe fn text_position_next_internal(
    start_ptr: *mut c_char,
    state: *mut TextPositionState,
) -> *mut c_char {
    let haystack_len: c_int = (*state).len1;
    let needle_len: c_int = (*state).len2;
    let skiptablemask: c_int = (*state).skiptablemask;
    let haystack: *const c_char = (*state).str1;
    let needle: *const c_char = (*state).str2;
    let haystack_end: *const c_char = haystack.add(haystack_len as usize);
    let mut hptr: *const c_char;

    Assert!(start_ptr as *const c_char >= haystack && start_ptr as *const c_char <= haystack_end);
    Assert!(needle_len > 0);

    (*state).last_match_len_tmp = needle_len;

    if !(*(*state).locale).deterministic {
        /*
         * With a nondeterministic collation, we have to use an unoptimized
         * route.
         */
        let mut result_hptr: *const c_char = core::ptr::null();

        hptr = start_ptr;
        while hptr < haystack_end {
            let mut test_end: *const c_char;

            /*
             * First check the common case that there is a match in the
             * haystack of exactly the length of the needle.
             */
            if !(*state).greedy
                && (haystack_end as isize - hptr as isize) >= needle_len as isize
                && pg_strncoll(
                    hptr,
                    needle_len as isize,
                    needle,
                    needle_len as isize,
                    (*state).locale,
                ) == 0
            {
                return hptr as *mut c_char;
            }

            /*
             * Else check if any of the non-empty substrings starting at hptr
             * compare equal to the needle.
             */
            test_end = hptr;
            loop {
                test_end = test_end.add(pg_mblen_range(test_end, haystack_end) as usize);
                if pg_strncoll(
                    hptr,
                    (test_end as isize - hptr as isize) as isize,
                    needle,
                    needle_len as isize,
                    (*state).locale,
                ) == 0
                {
                    (*state).last_match_len_tmp = (test_end as isize - hptr as isize) as c_int;
                    result_hptr = hptr;
                    if !(*state).greedy {
                        break;
                    }
                }
                if !(test_end < haystack_end) {
                    break;
                }
            }

            if !result_hptr.is_null() {
                break;
            }

            hptr = hptr.add(pg_mblen_range(hptr, haystack_end) as usize);
        }

        result_hptr as *mut c_char
    } else if needle_len == 1 {
        /* No point in using B-M-H for a one-character needle */
        let nchar: c_char = *needle;

        hptr = start_ptr;
        while hptr < haystack_end {
            if *hptr == nchar {
                return hptr as *mut c_char;
            }
            hptr = hptr.add(1);
        }
        core::ptr::null_mut() /* not found */
    } else {
        let needle_last: *const c_char = needle.add((needle_len - 1) as usize);

        /* Start at startpos plus the length of the needle */
        hptr = start_ptr.add((needle_len - 1) as usize);
        while hptr < haystack_end {
            /* Match the needle scanning *backward* */
            let mut nptr: *const c_char;
            let mut p: *const c_char;

            nptr = needle_last;
            p = hptr;
            while *nptr == *p {
                /* Matched it all?	If so, return 1-based position */
                if nptr == needle {
                    return p as *mut c_char;
                }
                nptr = nptr.sub(1);
                p = p.sub(1);
            }

            /*
             * No match, so use the haystack char at hptr to decide how far to
             * advance.
             */
            hptr = hptr.add(
                (*state).skiptable[((*hptr as u8) as c_int & skiptablemask) as usize] as usize,
            );
        }
        core::ptr::null_mut() /* not found */
    }
}

// TODO(pg-port): the following two helpers belong to source-order indices >= 36
// (another agent's range).  Provided here as genuine translations so this range
// compiles; a later dedup pass keeps a single definition.

/*
 * Return the offset of the current match.
 *
 * The offset is in characters, 1-based.
 */
unsafe fn text_position_get_match_pos(state: *mut TextPositionState) -> c_int {
    /* Convert the byte position to char position. */
    (*state).refpos += pg_mbstrlen_with_len(
        (*state).refpoint,
        ((*state).last_match as isize - (*state).refpoint as isize) as c_int,
    );
    (*state).refpoint = (*state).last_match;
    (*state).refpos + 1
}

unsafe fn text_position_cleanup(_state: *mut TextPositionState) {
    /* no cleanup needed */
}

// ===========================================================================
// varlena.c functions [36,72): comparison/collation/sortsupport batch.
// (text_position_get_match_pos, text_position_cleanup, check_collation_set
//  already translated above; not repeated here.)
// ===========================================================================

use crate::catalog::pg_type_d::{BPCHAROID, NAMEOID};
use crate::utils::sort::sortsupport::{SortSupport, SortSupportData};
use crate::utils::sort::tuplesort::ssup_datum_unsigned_cmp;
use crate::utils::adt::pg_locale::{
    pg_strcoll, pg_strxfrm, pg_strxfrm_enabled, pg_strxfrm_prefix, pg_strxfrm_prefix_enabled,
};
use crate::utils::builtins::bpchartruelen;
use crate::common::hashfn::{hash_any, hash_uint32};
use crate::port::pg_bswap::DatumBigEndianToNative;
use crate::lib::hyperloglog::{
    addHyperLogLog, estimateHyperLogLog, hyperLogLogState, initHyperLogLog,
};
use crate::pg_config::{NAMEDATALEN, PG_CACHE_LINE_SIZE, SIZEOF_DATUM};
use crate::postgres::{DatumGetName, DatumGetUInt32, Int32GetDatum, UInt32GetDatum};
use crate::c::{NameStr, Pointer, VARHDRSZ};

extern "C" {
    fn memcmp_v(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn memcpy_v(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset_v(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strncmp_v(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn strcmp_v(a: *const c_char, b: *const c_char) -> c_int;
}
extern "C" {
    #[link_name = "memcmp"]
    fn varlena_memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    #[link_name = "memcpy"]
    fn varlena_memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    #[link_name = "memset"]
    fn varlena_memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    #[link_name = "strncmp"]
    fn varlena_strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    #[link_name = "strcmp"]
    fn varlena_strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

const C_COLLATION_OID: Oid = 950;

/* varatt.h: VarString and BpChar share text's representation. */
pub type VarString = text;
pub type BpChar = text;

/*
 * DatumGetVarStringPP / DatumGetBpCharPP: detoast packed, like DatumGetTextPP.
 */
#[inline]
unsafe fn DatumGetVarStringPP(x: Datum) -> *mut VarString {
    pg_detoast_datum_packed(DatumGetPointer(x) as *mut c_void) as *mut VarString
}
#[inline]
unsafe fn DatumGetBpCharPP(x: Datum) -> *mut BpChar {
    pg_detoast_datum_packed(DatumGetPointer(x) as *mut c_void) as *mut BpChar
}
#[inline]
unsafe fn DatumGetTextPP(x: Datum) -> *mut text {
    pg_detoast_datum_packed(DatumGetPointer(x) as *mut c_void) as *mut text
}

/* TODO(pg-port): trace_sort GUC lives in utils/misc/guc_tables. */
static mut trace_sort: bool = false;

const TEXTBUFLEN: c_int = 1024;

/*
 * VarStringSortSupport - private state for varstr_sortsupport.
 */
#[repr(C)]
struct VarStringSortSupport {
    buf1: *mut c_char,      /* Buffer for left arg */
    buf2: *mut c_char,      /* Buffer for right arg */
    buflen1: c_int,         /* Allocated length of buf1 */
    buflen2: c_int,         /* Allocated length of buf2 */
    last_len1: c_int,       /* Length of last buf1 string/strxfrm() blob */
    last_len2: c_int,       /* Length of last buf2 string/strxfrm() blob */
    last_returned: c_int,   /* Last comparison result (cache) */
    locale: pg_locale_t,    /* Locale used for comparisons (NULL for C) */
    collate_c: bool,
    typid: Oid,             /* Actual datatype (text/bpchar/bytea/name) */
    cache_blob: bool,       /* Does buf2 contain strxfrm() blob, etc? */
    prop_card: f64,         /* Required cardinality proportion */
    abbr_card: hyperLogLogState, /* Abbreviated key cardinality state */
    full_card: hyperLogLogState, /* Full key cardinality state */
}

/*
 * varstr_cmp()
 *
 * Comparison function for text strings with given lengths, using the
 * appropriate locale. Returns an integer less than, equal to, or greater than
 * zero, indicating whether arg1 is less than, equal to, or greater than arg2.
 */
#[no_mangle]
pub unsafe fn varstr_cmp(
    arg1: *const c_char,
    len1: c_int,
    arg2: *const c_char,
    len2: c_int,
    collid: Oid,
) -> c_int {
    let mut result: c_int;
    let mylocale: pg_locale_t;

    check_collation_set(collid);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).collate_is_c {
        result = varlena_memcmp(arg1 as *const c_void, arg2 as *const c_void, Min(len1, len2) as usize);
        if (result == 0) && (len1 != len2) {
            result = if len1 < len2 { -1 } else { 1 };
        }
    } else {
        /*
         * memcmp() can't tell us which of two unequal strings sorts first,
         * but it's a cheap way to tell if they're equal.  Testing shows that
         * memcmp() followed by strcoll() is only trivially slower than
         * strcoll() by itself, so we don't lose much if this doesn't work out
         * very often, and if it does - for example, because there are many
         * equal strings in the input - then we win big by avoiding expensive
         * collation-aware comparisons.
         */
        if len1 == len2
            && varlena_memcmp(arg1 as *const c_void, arg2 as *const c_void, len1 as usize) == 0
        {
            return 0;
        }

        result = pg_strncoll(arg1, len1 as ssize_t, arg2, len2 as ssize_t, mylocale);

        /* Break tie if necessary. */
        if result == 0 && (*mylocale).deterministic {
            result = varlena_memcmp(arg1 as *const c_void, arg2 as *const c_void, Min(len1, len2) as usize);
            if (result == 0) && (len1 != len2) {
                result = if len1 < len2 { -1 } else { 1 };
            }
        }
    }

    result
}

/* text_cmp()
 * Internal comparison function for text strings.
 * Returns -1, 0 or 1
 */
unsafe fn text_cmp(arg1: *mut text, arg2: *mut text, collid: Oid) -> c_int {
    let a1p: *mut c_char;
    let a2p: *mut c_char;
    let len1: c_int;
    let len2: c_int;

    a1p = VARDATA_ANY(arg1 as *const c_char) as *mut c_char;
    a2p = VARDATA_ANY(arg2 as *const c_char) as *mut c_char;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    varstr_cmp(a1p, len1, a2p, len2, collid)
}

/*
 * Comparison functions for text strings.
 */
pub unsafe fn texteq(fcinfo: FunctionCallInfo) -> Datum {
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let mylocale: pg_locale_t;
    let result: bool;

    check_collation_set(collid);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).deterministic {
        let arg1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
        let arg2: Datum = PG_GETARG_DATUM!(fcinfo, 1);
        let len1: Size;
        let len2: Size;

        /*
         * Since we only care about equality or not-equality, we can avoid all
         * the expense of strcoll() here, and just do bitwise comparison.
         */
        len1 = toast_raw_datum_size(arg1);
        len2 = toast_raw_datum_size(arg2);
        if len1 != len2 {
            result = false;
        } else {
            let targ1: *mut text = DatumGetTextPP(arg1);
            let targ2: *mut text = DatumGetTextPP(arg2);

            result = varlena_memcmp(
                VARDATA_ANY(targ1 as *const c_char) as *const c_void,
                VARDATA_ANY(targ2 as *const c_char) as *const c_void,
                len1 - VARHDRSZ as Size,
            ) == 0;

            PG_FREE_IF_COPY!(fcinfo, targ1, 0);
            PG_FREE_IF_COPY!(fcinfo, targ2, 1);
        }
    } else {
        let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

        result = text_cmp(arg1, arg2, collid) == 0;

        PG_FREE_IF_COPY!(fcinfo, arg1, 0);
        PG_FREE_IF_COPY!(fcinfo, arg2, 1);
    }

    PG_RETURN_BOOL!(result);
}

pub unsafe fn textne(fcinfo: FunctionCallInfo) -> Datum {
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let mylocale: pg_locale_t;
    let result: bool;

    check_collation_set(collid);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).deterministic {
        let arg1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
        let arg2: Datum = PG_GETARG_DATUM!(fcinfo, 1);
        let len1: Size;
        let len2: Size;

        /* See comment in texteq() */
        len1 = toast_raw_datum_size(arg1);
        len2 = toast_raw_datum_size(arg2);
        if len1 != len2 {
            result = true;
        } else {
            let targ1: *mut text = DatumGetTextPP(arg1);
            let targ2: *mut text = DatumGetTextPP(arg2);

            result = varlena_memcmp(
                VARDATA_ANY(targ1 as *const c_char) as *const c_void,
                VARDATA_ANY(targ2 as *const c_char) as *const c_void,
                len1 - VARHDRSZ as Size,
            ) != 0;

            PG_FREE_IF_COPY!(fcinfo, targ1, 0);
            PG_FREE_IF_COPY!(fcinfo, targ2, 1);
        }
    } else {
        let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

        result = text_cmp(arg1, arg2, collid) != 0;

        PG_FREE_IF_COPY!(fcinfo, arg1, 0);
        PG_FREE_IF_COPY!(fcinfo, arg2, 1);
    }

    PG_RETURN_BOOL!(result);
}

pub unsafe fn text_lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;

    result = text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) < 0;

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn text_le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;

    result = text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) <= 0;

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn text_gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;

    result = text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) > 0;

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn text_ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: bool;

    result = text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) >= 0;

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn text_starts_with(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let arg2: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let mylocale: pg_locale_t;
    let result: bool;
    let len1: Size;
    let len2: Size;

    check_collation_set(collid);

    mylocale = pg_newlocale_from_collation(collid);

    if !(*mylocale).deterministic {
        ereport!(
            ERROR,
            errmsg!("nondeterministic collations are not supported for substring searches")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if len2 > len1 {
        result = false;
    } else {
        let targ1: *mut text = text_substring(arg1, 1, len2 as int32, false);
        let targ2: *mut text = DatumGetTextPP(arg2);

        result = varlena_memcmp(
            VARDATA_ANY(targ1 as *const c_char) as *const c_void,
            VARDATA_ANY(targ2 as *const c_char) as *const c_void,
            VARSIZE_ANY_EXHDR(targ2 as *const c_char) as usize,
        ) == 0;

        PG_FREE_IF_COPY!(fcinfo, targ1, 0);
        PG_FREE_IF_COPY!(fcinfo, targ2, 1);
    }

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bttextcmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: int32;

    result = text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo));

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_INT32!(result);
}

pub unsafe fn bttextsortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;
    let collid: Oid = (*ssup).ssup_collation;
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

    /* Use generic string SortSupport */
    varstr_sortsupport(ssup, TEXTOID, collid);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID!();
}

/*
 * Generic sortsupport interface for character type's operator classes.
 */
pub unsafe fn varstr_sortsupport(ssup: SortSupport, typid: Oid, collid: Oid) {
    let mut abbreviate: bool = (*ssup).abbreviate;
    let mut collate_c: bool = false;
    let sss: *mut VarStringSortSupport;
    let locale: pg_locale_t;

    check_collation_set(collid);

    locale = pg_newlocale_from_collation(collid);

    /*
     * If possible, set ssup->comparator to a function which can be used to
     * directly compare two datums.
     */
    if (*locale).collate_is_c {
        if typid == BPCHAROID {
            (*ssup).comparator = Some(bpcharfastcmp_c);
        } else if typid == NAMEOID {
            (*ssup).comparator = Some(namefastcmp_c);
            /* Not supporting abbreviation with type NAME, for now */
            abbreviate = false;
        } else {
            (*ssup).comparator = Some(varstrfastcmp_c);
        }

        collate_c = true;
    } else {
        /*
         * We use varlenafastcmp_locale except for type NAME.
         */
        if typid == NAMEOID {
            (*ssup).comparator = Some(namefastcmp_locale);
            /* Not supporting abbreviation with type NAME, for now */
            abbreviate = false;
        } else {
            (*ssup).comparator = Some(varlenafastcmp_locale);
        }

        /*
         * Unfortunately, it seems that abbreviation for non-C collations is
         * broken on many common platforms; see pg_strxfrm_enabled().
         */
        if !pg_strxfrm_enabled(locale) {
            abbreviate = false;
        }
    }

    /*
     * If we're using abbreviated keys, or if we're using a locale-aware
     * comparison, we need to initialize a VarStringSortSupport object.
     */
    if abbreviate || !collate_c {
        sss = palloc(core::mem::size_of::<VarStringSortSupport>()) as *mut VarStringSortSupport;
        (*sss).buf1 = palloc(TEXTBUFLEN as usize) as *mut c_char;
        (*sss).buflen1 = TEXTBUFLEN;
        (*sss).buf2 = palloc(TEXTBUFLEN as usize) as *mut c_char;
        (*sss).buflen2 = TEXTBUFLEN;
        /* Start with invalid values */
        (*sss).last_len1 = -1;
        (*sss).last_len2 = -1;
        /* Initialize */
        (*sss).last_returned = 0;
        if collate_c {
            (*sss).locale = core::ptr::null_mut();
        } else {
            (*sss).locale = locale;
        }

        /*
         * To avoid somehow confusing a strxfrm() blob and an original string,
         * constantly keep track of the variety of data that buf1 and buf2
         * currently contain.
         *
         * Arbitrarily initialize cache_blob to true.
         */
        (*sss).cache_blob = true;
        (*sss).collate_c = collate_c;
        (*sss).typid = typid;
        (*ssup).ssup_extra = sss as *mut c_void;

        /*
         * If possible, plan to use the abbreviated keys optimization.
         */
        if abbreviate {
            (*sss).prop_card = 0.20;
            initHyperLogLog(&mut (*sss).abbr_card, 10);
            initHyperLogLog(&mut (*sss).full_card, 10);
            (*ssup).abbrev_full_comparator = (*ssup).comparator;
            (*ssup).comparator = Some(ssup_datum_unsigned_cmp);
            (*ssup).abbrev_converter = Some(varstr_abbrev_convert);
            (*ssup).abbrev_abort = Some(varstr_abbrev_abort);
        }
    }
}

/*
 * sortsupport comparison func (for C locale case)
 */
unsafe fn varstrfastcmp_c(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let arg1: *mut VarString = DatumGetVarStringPP(x);
    let arg2: *mut VarString = DatumGetVarStringPP(y);
    let a1p: *mut c_char;
    let a2p: *mut c_char;
    let len1: c_int;
    let len2: c_int;
    let mut result: c_int;

    a1p = VARDATA_ANY(arg1 as *const c_char) as *mut c_char;
    a2p = VARDATA_ANY(arg2 as *const c_char) as *mut c_char;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    result = varlena_memcmp(a1p as *const c_void, a2p as *const c_void, Min(len1, len2) as usize);
    if (result == 0) && (len1 != len2) {
        result = if len1 < len2 { -1 } else { 1 };
    }

    /* We can't afford to leak memory here. */
    if PointerGetDatum(arg1 as *const c_void) != x {
        pfree(arg1 as *mut c_void);
    }
    if PointerGetDatum(arg2 as *const c_void) != y {
        pfree(arg2 as *mut c_void);
    }

    result
}

/*
 * sortsupport comparison func (for BpChar C locale case)
 */
unsafe fn bpcharfastcmp_c(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let arg1: *mut BpChar = DatumGetBpCharPP(x);
    let arg2: *mut BpChar = DatumGetBpCharPP(y);
    let a1p: *mut c_char;
    let a2p: *mut c_char;
    let len1: c_int;
    let len2: c_int;
    let mut result: c_int;

    a1p = VARDATA_ANY(arg1 as *const c_char) as *mut c_char;
    a2p = VARDATA_ANY(arg2 as *const c_char) as *mut c_char;

    len1 = bpchartruelen(a1p, VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int);
    len2 = bpchartruelen(a2p, VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int);

    result = varlena_memcmp(a1p as *const c_void, a2p as *const c_void, Min(len1, len2) as usize);
    if (result == 0) && (len1 != len2) {
        result = if len1 < len2 { -1 } else { 1 };
    }

    /* We can't afford to leak memory here. */
    if PointerGetDatum(arg1 as *const c_void) != x {
        pfree(arg1 as *mut c_void);
    }
    if PointerGetDatum(arg2 as *const c_void) != y {
        pfree(arg2 as *mut c_void);
    }

    result
}

/*
 * sortsupport comparison func (for NAME C locale case)
 */
unsafe fn namefastcmp_c(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let arg1: Name = DatumGetName(x);
    let arg2: Name = DatumGetName(y);

    varlena_strncmp(NameStr(&*arg1), NameStr(&*arg2), NAMEDATALEN)
}

/*
 * sortsupport comparison func (for locale case with all varlena types)
 */
unsafe fn varlenafastcmp_locale(x: Datum, y: Datum, ssup: SortSupport) -> c_int {
    let arg1: *mut VarString = DatumGetVarStringPP(x);
    let arg2: *mut VarString = DatumGetVarStringPP(y);
    let a1p: *mut c_char;
    let a2p: *mut c_char;
    let len1: c_int;
    let len2: c_int;
    let result: c_int;

    a1p = VARDATA_ANY(arg1 as *const c_char) as *mut c_char;
    a2p = VARDATA_ANY(arg2 as *const c_char) as *mut c_char;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    result = varstrfastcmp_locale(a1p, len1, a2p, len2, ssup);

    /* We can't afford to leak memory here. */
    if PointerGetDatum(arg1 as *const c_void) != x {
        pfree(arg1 as *mut c_void);
    }
    if PointerGetDatum(arg2 as *const c_void) != y {
        pfree(arg2 as *mut c_void);
    }

    result
}

/*
 * sortsupport comparison func (for locale case with NAME type)
 */
unsafe fn namefastcmp_locale(x: Datum, y: Datum, ssup: SortSupport) -> c_int {
    let arg1: Name = DatumGetName(x);
    let arg2: Name = DatumGetName(y);

    varstrfastcmp_locale(
        NameStr(&*arg1) as *mut c_char,
        strlen(NameStr(&*arg1)) as c_int,
        NameStr(&*arg2) as *mut c_char,
        strlen(NameStr(&*arg2)) as c_int,
        ssup,
    )
}

/*
 * sortsupport comparison func for locale cases
 */
unsafe fn varstrfastcmp_locale(
    a1p: *mut c_char,
    mut len1: c_int,
    a2p: *mut c_char,
    mut len2: c_int,
    ssup: SortSupport,
) -> c_int {
    let sss: *mut VarStringSortSupport = (*ssup).ssup_extra as *mut VarStringSortSupport;
    let mut result: c_int;
    let mut arg1_match: bool;

    /* Fast pre-check for equality, as discussed in varstr_cmp() */
    if len1 == len2 && varlena_memcmp(a1p as *const c_void, a2p as *const c_void, len1 as usize) == 0 {
        /*
         * No change in buf1 or buf2 contents, so avoid changing last_len1 or
         * last_len2.  Existing contents of buffers might still be used by
         * next call.
         */
        return 0;
    }

    if (*sss).typid == BPCHAROID {
        /* Get true number of bytes, ignoring trailing spaces */
        len1 = bpchartruelen(a1p, len1);
        len2 = bpchartruelen(a2p, len2);
    }

    if len1 >= (*sss).buflen1 {
        (*sss).buflen1 = Max(len1 + 1, Min((*sss).buflen1 * 2, MaxAllocSize as c_int));
        (*sss).buf1 = repalloc((*sss).buf1 as *mut c_void, (*sss).buflen1 as usize) as *mut c_char;
    }
    if len2 >= (*sss).buflen2 {
        (*sss).buflen2 = Max(len2 + 1, Min((*sss).buflen2 * 2, MaxAllocSize as c_int));
        (*sss).buf2 = repalloc((*sss).buf2 as *mut c_void, (*sss).buflen2 as usize) as *mut c_char;
    }

    /*
     * We're likely to be asked to compare the same strings repeatedly, and
     * memcmp() is so much cheaper than strcoll() that it pays to try to cache
     * comparisons.
     */
    arg1_match = true;
    if len1 != (*sss).last_len1
        || varlena_memcmp((*sss).buf1 as *const c_void, a1p as *const c_void, len1 as usize) != 0
    {
        arg1_match = false;
        varlena_memcpy((*sss).buf1 as *mut c_void, a1p as *const c_void, len1 as usize);
        *(*sss).buf1.add(len1 as usize) = b'\0' as c_char;
        (*sss).last_len1 = len1;
    }

    /*
     * If we're comparing the same two strings as last time, we can return the
     * same answer without calling strcoll() again.
     */
    if len2 != (*sss).last_len2
        || varlena_memcmp((*sss).buf2 as *const c_void, a2p as *const c_void, len2 as usize) != 0
    {
        varlena_memcpy((*sss).buf2 as *mut c_void, a2p as *const c_void, len2 as usize);
        *(*sss).buf2.add(len2 as usize) = b'\0' as c_char;
        (*sss).last_len2 = len2;
    } else if arg1_match && !(*sss).cache_blob {
        /* Use result cached following last actual strcoll() call */
        return (*sss).last_returned;
    }

    result = pg_strcoll((*sss).buf1, (*sss).buf2, (*sss).locale);

    /* Break tie if necessary. */
    if result == 0 && (*(*sss).locale).deterministic {
        result = varlena_strcmp((*sss).buf1, (*sss).buf2);
    }

    /* Cache result, perhaps saving an expensive strcoll() call next time */
    (*sss).cache_blob = false;
    (*sss).last_returned = result;
    result
}

/*
 * Conversion routine for sortsupport.  Converts original to abbreviated key
 * representation.
 */
unsafe fn varstr_abbrev_convert(original: Datum, ssup: SortSupport) -> Datum {
    let max_prefix_bytes: usize = core::mem::size_of::<Datum>();
    let sss: *mut VarStringSortSupport = (*ssup).ssup_extra as *mut VarStringSortSupport;
    let authoritative: *mut VarString = DatumGetVarStringPP(original);
    let authoritative_data: *mut c_char = VARDATA_ANY(authoritative as *const c_char) as *mut c_char;

    /* working state */
    let mut res: Datum = 0;
    let pres: *mut c_char;
    let mut len: c_int;
    let mut hash: uint32;

    pres = &mut res as *mut Datum as *mut c_char;
    /* memset(), so any non-overwritten bytes are NUL */
    varlena_memset(pres as *mut c_void, 0, max_prefix_bytes);
    len = VARSIZE_ANY_EXHDR(authoritative as *const c_char) as c_int;

    /* Get number of bytes, ignoring trailing spaces */
    if (*sss).typid == BPCHAROID {
        len = bpchartruelen(authoritative_data, len);
    }

    /*
     * If we're using the C collation, use memcpy(), rather than strxfrm(), to
     * abbreviate keys.
     */
    let mut bsize_done: bool = false;
    if (*sss).collate_c {
        varlena_memcpy(
            pres as *mut c_void,
            authoritative_data as *const c_void,
            Min(len as usize, max_prefix_bytes),
        );
    } else {
        let mut bsize: Size = 0;

        /*
         * We're not using the C collation, so fall back on strxfrm or ICU
         * analogs.
         */

        /* By convention, we use buffer 1 to store and NUL-terminate */
        if len >= (*sss).buflen1 {
            (*sss).buflen1 = Max(len + 1, Min((*sss).buflen1 * 2, MaxAllocSize as c_int));
            (*sss).buf1 = repalloc((*sss).buf1 as *mut c_void, (*sss).buflen1 as usize) as *mut c_char;
        }

        /* Might be able to reuse strxfrm() blob from last call */
        if (*sss).last_len1 == len
            && (*sss).cache_blob
            && varlena_memcmp((*sss).buf1 as *const c_void, authoritative_data as *const c_void, len as usize) == 0
        {
            varlena_memcpy(
                pres as *mut c_void,
                (*sss).buf2 as *const c_void,
                Min(max_prefix_bytes, (*sss).last_len2 as usize),
            );
            /* No change affecting cardinality, so no hashing required */
            bsize_done = true;
        }

        if !bsize_done {
            varlena_memcpy((*sss).buf1 as *mut c_void, authoritative_data as *const c_void, len as usize);

            /*
             * pg_strxfrm() and pg_strxfrm_prefix expect NUL-terminated strings.
             */
            *(*sss).buf1.add(len as usize) = b'\0' as c_char;
            (*sss).last_len1 = len;

            if pg_strxfrm_prefix_enabled((*sss).locale) {
                if ((*sss).buflen2 as usize) < max_prefix_bytes {
                    (*sss).buflen2 = Max(max_prefix_bytes as c_int, Min((*sss).buflen2 * 2, MaxAllocSize as c_int));
                    (*sss).buf2 = repalloc((*sss).buf2 as *mut c_void, (*sss).buflen2 as usize) as *mut c_char;
                }

                bsize = pg_strxfrm_prefix((*sss).buf2, (*sss).buf1, max_prefix_bytes, (*sss).locale);
                (*sss).last_len2 = bsize as c_int;
            } else {
                /*
                 * Loop: Call pg_strxfrm(), possibly enlarge buffer, and try
                 * again.
                 */
                loop {
                    bsize = pg_strxfrm((*sss).buf2, (*sss).buf1, (*sss).buflen2 as usize, (*sss).locale);

                    (*sss).last_len2 = bsize as c_int;
                    if bsize < (*sss).buflen2 as usize {
                        break;
                    }

                    /*
                     * Grow buffer and retry.
                     */
                    (*sss).buflen2 = Max(bsize as c_int + 1, Min((*sss).buflen2 * 2, MaxAllocSize as c_int));
                    (*sss).buf2 = repalloc((*sss).buf2 as *mut c_void, (*sss).buflen2 as usize) as *mut c_char;
                }
            }

            /*
             * Every Datum byte is always compared.  This is safe because the
             * strxfrm() blob is itself NUL terminated.
             */
            varlena_memcpy(pres as *mut c_void, (*sss).buf2 as *const c_void, Min(max_prefix_bytes, bsize));
        }
    }

    if !bsize_done {
        /*
         * Maintain approximate cardinality of both abbreviated keys and
         * original, authoritative keys using HyperLogLog.
         */
        hash = DatumGetUInt32(hash_any(
            authoritative_data as *const c_uchar,
            Min(len, PG_CACHE_LINE_SIZE as c_int),
        ));

        if len > PG_CACHE_LINE_SIZE as c_int {
            hash ^= DatumGetUInt32(hash_uint32(len as uint32));
        }

        addHyperLogLog(&mut (*sss).full_card, hash);

        /* Hash abbreviated key */
        if SIZEOF_DATUM == 8 {
            let lohalf: uint32 = res as uint32;
            let hihalf: uint32 = (res >> 32) as uint32;
            hash = DatumGetUInt32(hash_uint32(lohalf ^ hihalf));
        } else {
            hash = DatumGetUInt32(hash_uint32(res as uint32));
        }

        addHyperLogLog(&mut (*sss).abbr_card, hash);

        /* Cache result, perhaps saving an expensive strxfrm() call next time */
        (*sss).cache_blob = true;
    }
    /* done: */

    /*
     * Byteswap on little-endian machines.
     */
    res = DatumBigEndianToNative(res);

    /* Don't leak memory here */
    if PointerGetDatum(authoritative as *const c_void) != original {
        pfree(authoritative as *mut c_void);
    }

    res
}

/*
 * Callback for estimating effectiveness of abbreviated key optimization.
 */
unsafe fn varstr_abbrev_abort(memtupcount: c_int, ssup: SortSupport) -> bool {
    let sss: *mut VarStringSortSupport = (*ssup).ssup_extra as *mut VarStringSortSupport;
    let mut abbrev_distinct: f64;
    let mut key_distinct: f64;

    Assert!((*ssup).abbreviate);

    /* Have a little patience */
    if memtupcount < 100 {
        return false;
    }

    abbrev_distinct = estimateHyperLogLog(&mut (*sss).abbr_card);
    key_distinct = estimateHyperLogLog(&mut (*sss).full_card);

    /*
     * Clamp cardinality estimates to at least one distinct value.
     */
    if abbrev_distinct <= 1.0 {
        abbrev_distinct = 1.0;
    }

    if key_distinct <= 1.0 {
        key_distinct = 1.0;
    }

    /*
     * In the worst case all abbreviated keys are identical, while at the same
     * time there are differences within full key strings not captured in
     * abbreviations.
     */
    if trace_sort {
        let norm_abbrev_card: f64 = abbrev_distinct / (memtupcount as f64);

        elog!(
            LOG,
            "varstr_abbrev: abbrev_distinct after {}: {} (key_distinct: {}, norm_abbrev_card: {}, prop_card: {})",
            memtupcount,
            abbrev_distinct,
            key_distinct,
            norm_abbrev_card,
            (*sss).prop_card
        );
    }

    /*
     * If the number of distinct abbreviated keys approximately matches the
     * number of distinct authoritative original keys, that's reason enough to
     * proceed.
     */
    if abbrev_distinct > key_distinct * (*sss).prop_card {
        /*
         * When we have exceeded 10,000 tuples, decay required cardinality
         * aggressively for next call.
         */
        if memtupcount > 10000 {
            (*sss).prop_card *= 0.65;
        }

        return false;
    }

    /*
     * Abort abbreviation strategy.
     */
    if trace_sort {
        elog!(
            LOG,
            "varstr_abbrev: aborted abbreviation at {} (abbrev_distinct: {}, key_distinct: {}, prop_card: {})",
            memtupcount,
            abbrev_distinct,
            key_distinct,
            (*sss).prop_card
        );
    }

    true
}

/*
 * Generic equalimage support function for character type's operator classes.
 */
pub unsafe fn btvarstrequalimage(fcinfo: FunctionCallInfo) -> Datum {
    /* Oid		opcintype = PG_GETARG_OID(0); */
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let locale: pg_locale_t;

    check_collation_set(collid);

    locale = pg_newlocale_from_collation(collid);

    PG_RETURN_BOOL!((*locale).deterministic);
}

pub unsafe fn text_larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: *mut text;

    result = if text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) > 0 { arg1 } else { arg2 };

    PG_RETURN_TEXT_P!(result);
}

pub unsafe fn text_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: *mut text;

    result = if text_cmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) < 0 { arg1 } else { arg2 };

    PG_RETURN_TEXT_P!(result);
}

/*
 * Cross-type comparison functions for types text and name.
 */
pub unsafe fn nameeqtext(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let len1: usize = strlen(NameStr(&*arg1));
    let len2: usize = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as usize;
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    check_collation_set(collid);

    if collid == C_COLLATION_OID {
        result = len1 == len2
            && varlena_memcmp(
                NameStr(&*arg1) as *const c_void,
                VARDATA_ANY(arg2 as *const c_char) as *const c_void,
                len1,
            ) == 0;
    } else {
        result = varstr_cmp(
            NameStr(&*arg1),
            len1 as c_int,
            VARDATA_ANY(arg2 as *const c_char) as *const c_char,
            len2 as c_int,
            collid,
        ) == 0;
    }

    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn texteqname(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);
    let len1: usize = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as usize;
    let len2: usize = strlen(NameStr(&*arg2));
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    check_collation_set(collid);

    if collid == C_COLLATION_OID {
        result = len1 == len2
            && varlena_memcmp(
                VARDATA_ANY(arg1 as *const c_char) as *const c_void,
                NameStr(&*arg2) as *const c_void,
                len1,
            ) == 0;
    } else {
        result = varstr_cmp(
            VARDATA_ANY(arg1 as *const c_char) as *const c_char,
            len1 as c_int,
            NameStr(&*arg2),
            len2 as c_int,
            collid,
        ) == 0;
    }

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn namenetext(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let len1: usize = strlen(NameStr(&*arg1));
    let len2: usize = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as usize;
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    check_collation_set(collid);

    if collid == C_COLLATION_OID {
        result = !(len1 == len2
            && varlena_memcmp(
                NameStr(&*arg1) as *const c_void,
                VARDATA_ANY(arg2 as *const c_char) as *const c_void,
                len1,
            ) == 0);
    } else {
        result = !(varstr_cmp(
            NameStr(&*arg1),
            len1 as c_int,
            VARDATA_ANY(arg2 as *const c_char) as *const c_char,
            len2 as c_int,
            collid,
        ) == 0);
    }

    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn textnename(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);
    let len1: usize = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as usize;
    let len2: usize = strlen(NameStr(&*arg2));
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    check_collation_set(collid);

    if collid == C_COLLATION_OID {
        result = !(len1 == len2
            && varlena_memcmp(
                VARDATA_ANY(arg1 as *const c_char) as *const c_void,
                NameStr(&*arg2) as *const c_void,
                len1,
            ) == 0);
    } else {
        result = !(varstr_cmp(
            VARDATA_ANY(arg1 as *const c_char) as *const c_char,
            len1 as c_int,
            NameStr(&*arg2),
            len2 as c_int,
            collid,
        ) == 0);
    }

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn btnametextcmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: int32;

    result = varstr_cmp(
        NameStr(&*arg1),
        strlen(NameStr(&*arg1)) as c_int,
        VARDATA_ANY(arg2 as *const c_char) as *const c_char,
        VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int,
        PG_GET_COLLATION!(fcinfo),
    );

    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_INT32!(result);
}

pub unsafe fn bttextnamecmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);
    let result: int32;

    result = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char) as *const c_char,
        VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int,
        NameStr(&*arg2),
        strlen(NameStr(&*arg2)) as c_int,
        PG_GET_COLLATION!(fcinfo),
    );

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);

    PG_RETURN_INT32!(result);
}

/*
 * #define CmpCall(cmpfunc) \
 *	DatumGetInt32(DirectFunctionCall2Coll(cmpfunc, PG_GET_COLLATION(),
 *										  PG_GETARG_DATUM(0), PG_GETARG_DATUM(1)))
 */
macro_rules! CmpCall {
    ($fcinfo:expr, $cmpfunc:expr) => {
        DatumGetInt32(DirectFunctionCall2Coll(
            $cmpfunc,
            PG_GET_COLLATION!($fcinfo),
            PG_GETARG_DATUM!($fcinfo, 0),
            PG_GETARG_DATUM!($fcinfo, 1),
        ))
    };
}

pub unsafe fn namelttext(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, btnametextcmp) < 0);
}

pub unsafe fn nameletext(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, btnametextcmp) <= 0);
}

/* ===================================================================
 * bytea family (translated from varlena.c)
 * =================================================================== */

pub unsafe fn byteaoctetlen(fcinfo: FunctionCallInfo) -> Datum {
    let str: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    /* We need not detoast the input at all */
    PG_RETURN_INT32!(toast_raw_datum_size(str) as i32 - VARHDRSZ);
}

/*
 * byteacat -
 *	  takes two bytea* and returns a bytea* that is the concatenation of
 *	  the two.
 *
 * Cloned from textcat and modified as required.
 */
pub unsafe fn byteacat(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let t2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);

    PG_RETURN_BYTEA_P!(bytea_catenate(t1, t2));
}

/*
 * bytea_catenate
 *	Guts of byteacat(), broken out so it can be used by other functions
 *
 * Arguments can be in short-header form, but not compressed or out-of-line
 */
unsafe fn bytea_catenate(t1: *mut bytea, t2: *mut bytea) -> *mut bytea {
    let result: *mut bytea;
    let mut len1: c_int;
    let mut len2: c_int;
    let len: c_int;
    let ptr: *mut c_char;

    len1 = VARSIZE_ANY_EXHDR(t1 as *const c_char) as i32;
    len2 = VARSIZE_ANY_EXHDR(t2 as *const c_char) as i32;

    /* paranoia ... probably should throw error instead? */
    if len1 < 0 {
        len1 = 0;
    }
    if len2 < 0 {
        len2 = 0;
    }

    len = len1 + len2 + VARHDRSZ;
    result = palloc(len as Size) as *mut bytea;

    /* Set size of result string... */
    SET_VARSIZE(result as *mut c_char, len);

    /* Fill data field of result string... */
    ptr = VARDATA(result as *const c_char);
    if len1 > 0 {
        memcpy_v(ptr as *mut c_void, VARDATA_ANY(t1 as *const c_char) as *const c_void, len1 as usize);
    }
    if len2 > 0 {
        memcpy_v(
            ptr.add(len1 as usize) as *mut c_void,
            VARDATA_ANY(t2 as *const c_char) as *const c_void,
            len2 as usize,
        );
    }

    result
}

/*
 * #define PG_STR_GET_BYTEA(str_) \
 *	DatumGetByteaPP(DirectFunctionCall1(byteain, CStringGetDatum(str_)))
 */
macro_rules! PG_STR_GET_BYTEA {
    ($str:expr) => {
        DatumGetByteaPP!(DirectFunctionCall1!(byteain, CStringGetDatum($str))) as *mut bytea
    };
}

/*
 * bytea_substr()
 * Return a substring starting at the specified position.
 * Cloned from text_substr and modified as required.
 */
pub unsafe fn bytea_substr(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BYTEA_P!(bytea_substring(
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        PG_GETARG_INT32!(fcinfo, 2),
        false
    ));
}

/*
 * bytea_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
pub unsafe fn bytea_substr_no_len(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BYTEA_P!(bytea_substring(
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        -1,
        true
    ));
}

unsafe fn bytea_substring(
    str: Datum,
    s: c_int,
    l: c_int,
    length_not_specified: bool,
) -> *mut bytea {
    let s1: int32; /* adjusted start position */
    let l1: int32; /* adjusted substring length */
    let mut e: int32 = 0; /* end position */

    /*
     * The logic here should generally match text_substring().
     */
    s1 = Max(s, 1);

    if length_not_specified {
        /*
         * Not passed a length - DatumGetByteaPSlice() grabs everything to the
         * end of the string if we pass it a negative value for length.
         */
        l1 = -1;
    } else if l < 0 {
        /* SQL99 says to throw an error for E < S, i.e., negative length */
        let _ = errcode(ERRCODE_SUBSTRING_ERROR);
        ereport!(ERROR, errmsg!("negative substring length not allowed"));
        l1 = -1; /* silence stupider compilers */
    } else if pg_add_s32_overflow(s, l, &mut e) {
        /*
         * L could be large enough for S + L to overflow, in which case the
         * substring must run to end of string.
         */
        l1 = -1;
    } else {
        /*
         * A zero or negative value for the end position can happen if the
         * start was negative or one. SQL99 says to return a zero-length
         * string.
         */
        if e < 1 {
            return PG_STR_GET_BYTEA!(c"".as_ptr() as *const c_char);
        }

        l1 = e - s1;
    }

    /*
     * If the start position is past the end of the string, SQL99 says to
     * return a zero-length string -- DatumGetByteaPSlice() will do that for
     * us.  We need only convert S1 to zero-based starting position.
     */
    DatumGetByteaPSlice!(str, s1 - 1, l1)
}

/*
 * byteaoverlay
 *	Replace specified substring of first string with second
 */
pub unsafe fn byteaoverlay(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let t2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let sp: c_int = PG_GETARG_INT32!(fcinfo, 2); /* substring start position */
    let sl: c_int = PG_GETARG_INT32!(fcinfo, 3); /* substring length */

    PG_RETURN_BYTEA_P!(bytea_overlay(t1, t2, sp, sl));
}

pub unsafe fn byteaoverlay_no_len(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let t2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let sp: c_int = PG_GETARG_INT32!(fcinfo, 2); /* substring start position */
    let sl: c_int;

    sl = VARSIZE_ANY_EXHDR(t2 as *const c_char) as i32; /* defaults to length(t2) */
    PG_RETURN_BYTEA_P!(bytea_overlay(t1, t2, sp, sl));
}

unsafe fn bytea_overlay(t1: *mut bytea, t2: *mut bytea, sp: c_int, sl: c_int) -> *mut bytea {
    let mut result: *mut bytea;
    let s1: *mut bytea;
    let s2: *mut bytea;
    let mut sp_pl_sl: c_int = 0;

    /*
     * Check for possible integer-overflow cases.  For negative sp, throw a
     * "substring length" error because that's what should be expected
     * according to the spec's definition of OVERLAY().
     */
    if sp <= 0 {
        let _ = errcode(ERRCODE_SUBSTRING_ERROR);
        ereport!(ERROR, errmsg!("negative substring length not allowed"));
    }
    if pg_add_s32_overflow(sp, sl, &mut sp_pl_sl) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    s1 = bytea_substring(PointerGetDatum(t1 as *const c_void), 1, sp - 1, false);
    s2 = bytea_substring(PointerGetDatum(t1 as *const c_void), sp_pl_sl, -1, true);
    result = bytea_catenate(s1, t2);
    result = bytea_catenate(result, s2);

    result
}

/*
 * bit_count
 */
pub unsafe fn bytea_bit_count(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);

    PG_RETURN_INT64!(pg_popcount(
        VARDATA_ANY(t1 as *const c_char),
        VARSIZE_ANY_EXHDR(t1 as *const c_char) as c_int
    ) as i64);
}

/*
 * byteapos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL POSITION() function.
 * Cloned from textpos and modified as required.
 */
pub unsafe fn byteapos(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let t2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let mut pos: c_int;
    let px: c_int;
    let mut p: c_int;
    let len1: c_int;
    let len2: c_int;
    let mut p1: *mut c_char;
    let p2: *mut c_char;

    len1 = VARSIZE_ANY_EXHDR(t1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(t2 as *const c_char) as c_int;

    if len2 <= 0 {
        PG_RETURN_INT32!(1); /* result for empty pattern */
    }

    p1 = VARDATA_ANY(t1 as *const c_char);
    p2 = VARDATA_ANY(t2 as *const c_char);

    pos = 0;
    px = len1 - len2;
    p = 0;
    while p <= px {
        if (*p2 == *p1) && (memcmp_v(p1 as *const c_void, p2 as *const c_void, len2 as usize) == 0) {
            pos = p + 1;
            break;
        }
        p1 = p1.add(1);
        p += 1;
    }

    PG_RETURN_INT32!(pos);
}

/*-------------------------------------------------------------
 * byteaGetByte
 *
 * this routine treats "bytea" as an array of bytes.
 * It returns the Nth byte (a number between 0 and 255).
 *-------------------------------------------------------------
 */
pub unsafe fn byteaGetByte(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let n: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let len: c_int;
    let byte: c_int;

    len = VARSIZE_ANY_EXHDR(v as *const c_char) as c_int;

    if n < 0 || n >= len {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(
            ERROR,
            errmsg!("index {} out of valid range, 0..{}", n, len - 1)
        );
    }

    byte = *((VARDATA_ANY(v as *const c_char) as *const u8).add(n as usize)) as c_int;

    PG_RETURN_INT32!(byte);
}

/*-------------------------------------------------------------
 * byteaGetBit
 *
 * This routine treats a "bytea" type like an array of bits.
 * It returns the value of the Nth bit (0 or 1).
 *-------------------------------------------------------------
 */
pub unsafe fn byteaGetBit(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let n: i64 = PG_GETARG_INT64!(fcinfo, 1);
    let byte_no: c_int;
    let bit_no: c_int;
    let len: c_int;
    let byte: c_int;

    len = VARSIZE_ANY_EXHDR(v as *const c_char) as c_int;

    if n < 0 || n >= len as i64 * 8 {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(
            ERROR,
            errmsg!("index {} out of valid range, 0..{}", n, len as i64 * 8 - 1)
        );
    }

    /* n/8 is now known < len, so safe to cast to int */
    byte_no = (n / 8) as c_int;
    bit_no = (n % 8) as c_int;

    byte = *((VARDATA_ANY(v as *const c_char) as *const u8).add(byte_no as usize)) as c_int;

    if byte & (1 << bit_no) != 0 {
        PG_RETURN_INT32!(1);
    } else {
        PG_RETURN_INT32!(0);
    }
}

/*-------------------------------------------------------------
 * byteaSetByte
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth byte set to the given value.
 *-------------------------------------------------------------
 */
pub unsafe fn byteaSetByte(fcinfo: FunctionCallInfo) -> Datum {
    let res: *mut bytea = PG_GETARG_BYTEA_P_COPY!(fcinfo, 0);
    let n: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let new_byte: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let len: c_int;

    len = VARSIZE(res as *const c_char) as c_int - VARHDRSZ;

    if n < 0 || n >= len {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(
            ERROR,
            errmsg!("index {} out of valid range, 0..{}", n, len - 1)
        );
    }

    /*
     * Now set the byte.
     */
    *((VARDATA(res as *const c_char) as *mut u8).add(n as usize)) = new_byte as u8;

    PG_RETURN_BYTEA_P!(res);
}

/*-------------------------------------------------------------
 * byteaSetBit
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth bit set to the given value.
 *-------------------------------------------------------------
 */
pub unsafe fn byteaSetBit(fcinfo: FunctionCallInfo) -> Datum {
    let res: *mut bytea = PG_GETARG_BYTEA_P_COPY!(fcinfo, 0);
    let n: i64 = PG_GETARG_INT64!(fcinfo, 1);
    let new_bit: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let len: c_int;
    let old_byte: c_int;
    let new_byte: c_int;
    let byte_no: c_int;
    let bit_no: c_int;

    len = VARSIZE(res as *const c_char) as c_int - VARHDRSZ;

    if n < 0 || n >= len as i64 * 8 {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(
            ERROR,
            errmsg!("index {} out of valid range, 0..{}", n, len as i64 * 8 - 1)
        );
    }

    /* n/8 is now known < len, so safe to cast to int */
    byte_no = (n / 8) as c_int;
    bit_no = (n % 8) as c_int;

    /*
     * sanity check!
     */
    if new_bit != 0 && new_bit != 1 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("new bit must be 0 or 1"));
    }

    /*
     * Update the byte.
     */
    old_byte = *((VARDATA(res as *const c_char) as *const u8).add(byte_no as usize)) as c_int;

    if new_bit == 0 {
        new_byte = old_byte & (!(1 << bit_no));
    } else {
        new_byte = old_byte | (1 << bit_no);
    }

    *((VARDATA(res as *const c_char) as *mut u8).add(byte_no as usize)) = new_byte as u8;

    PG_RETURN_BYTEA_P!(res);
}

/*
 * Return reversed bytea
 */
pub unsafe fn bytea_reverse(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let mut p: *const c_char = VARDATA_ANY(v as *const c_char);
    let len: c_int = VARSIZE_ANY_EXHDR(v as *const c_char) as c_int;
    let endp: *const c_char = p.add(len as usize);
    let result: *mut bytea = palloc((len + VARHDRSZ) as Size) as *mut bytea;
    let mut dst: *mut c_char = (VARDATA(result as *const c_char)).add(len as usize);

    SET_VARSIZE(result as *mut c_char, len + VARHDRSZ);

    while p < endp {
        dst = dst.sub(1);
        *dst = *p;
        p = p.add(1);
    }

    PG_RETURN_BYTEA_P!(result);
}

/* text_name()
 * Converts a text type to a Name type.
 */
pub unsafe fn text_name(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let result: Name;
    let mut len: c_int;

    len = VARSIZE_ANY_EXHDR(s as *const c_char) as c_int;

    /* Truncate oversize input */
    if len >= NAMEDATALEN as c_int {
        len = pg_mbcliplen(VARDATA_ANY(s as *const c_char), len, NAMEDATALEN as c_int - 1);
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = palloc0(NAMEDATALEN as Size) as Name;
    memcpy_v(
        NameStr(&*result) as *mut c_void,
        VARDATA_ANY(s as *const c_char) as *const c_void,
        len as usize,
    );

    PG_RETURN_NAME!(result);
}

/* name_text()
 * Converts a Name type to a text type.
 */
pub unsafe fn name_text(fcinfo: FunctionCallInfo) -> Datum {
    let s: Name = PG_GETARG_NAME!(fcinfo, 0);

    PG_RETURN_TEXT_P!(cstring_to_text(NameStr(&*s)));
}

/* ===================================================================
 * bytea comparison / btree / casts (translated from varlena.c)
 * =================================================================== */

pub unsafe fn byteaeq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let arg2: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let result: bool;
    let len1: Size;
    let len2: Size;

    /*
     * We can use a fast path for unequal lengths, which might save us from
     * having to detoast one or both values.
     */
    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if len1 != len2 {
        result = false;
    } else {
        let barg1: *mut bytea = DatumGetByteaPP!(arg1) as *mut bytea;
        let barg2: *mut bytea = DatumGetByteaPP!(arg2) as *mut bytea;

        result = memcmp_v(
            VARDATA_ANY(barg1 as *const c_char) as *const c_void,
            VARDATA_ANY(barg2 as *const c_char) as *const c_void,
            len1 - VARHDRSZ as usize,
        ) == 0;

        PG_FREE_IF_COPY!(fcinfo, barg1, 0);
        PG_FREE_IF_COPY!(fcinfo, barg2, 1);
    }

    PG_RETURN_BOOL!(result);
}

pub unsafe fn byteane(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let arg2: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let result: bool;
    let len1: Size;
    let len2: Size;

    /*
     * We can use a fast path for unequal lengths, which might save us from
     * having to detoast one or both values.
     */
    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if len1 != len2 {
        result = true;
    } else {
        let barg1: *mut bytea = DatumGetByteaPP!(arg1) as *mut bytea;
        let barg2: *mut bytea = DatumGetByteaPP!(arg2) as *mut bytea;

        result = memcmp_v(
            VARDATA_ANY(barg1 as *const c_char) as *const c_void,
            VARDATA_ANY(barg2 as *const c_char) as *const c_void,
            len1 - VARHDRSZ as usize,
        ) != 0;

        PG_FREE_IF_COPY!(fcinfo, barg1, 0);
        PG_FREE_IF_COPY!(fcinfo, barg2, 1);
    }

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bytealt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

pub unsafe fn byteale(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

pub unsafe fn byteagt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

pub unsafe fn byteage(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

pub unsafe fn byteacmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let len1: c_int;
    let len2: c_int;
    let mut cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );
    if (cmp == 0) && (len1 != len2) {
        cmp = if len1 < len2 { -1 } else { 1 };
    }

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_INT32!(cmp);
}

pub unsafe fn bytea_larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let result: *mut bytea;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );
    result = if (cmp > 0) || ((cmp == 0) && (len1 > len2)) { arg1 } else { arg2 };

    PG_RETURN_BYTEA_P!(result);
}

pub unsafe fn bytea_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let arg2: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let result: *mut bytea;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    cmp = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );
    result = if (cmp < 0) || ((cmp == 0) && (len1 < len2)) { arg1 } else { arg2 };

    PG_RETURN_BYTEA_P!(result);
}

pub unsafe fn bytea_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

    /* Use generic string SortSupport, forcing "C" collation */
    varstr_sortsupport(ssup, BYTEAOID, C_COLLATION_OID);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID!();
}

/* Cast bytea -> int2 */
pub unsafe fn bytea_int2(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let len: c_int = VARSIZE_ANY_EXHDR(v as *const c_char) as c_int;
    let mut result: u16;

    /* Check that the byte array is not too long */
    if len as usize > core::mem::size_of::<u16>() {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("smallint out of range"));
    }

    /* Convert it to an integer; most significant bytes come first */
    result = 0;
    let mut i: c_int = 0;
    while i < len {
        result <<= BITS_PER_BYTE;
        result |= *((VARDATA_ANY(v as *const c_char) as *const u8).add(i as usize)) as u16;
        i += 1;
    }

    PG_RETURN_INT16!(result as i16);
}

/* Cast bytea -> int4 */
pub unsafe fn bytea_int4(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let len: c_int = VARSIZE_ANY_EXHDR(v as *const c_char) as c_int;
    let mut result: u32;

    /* Check that the byte array is not too long */
    if len as usize > core::mem::size_of::<u32>() {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    /* Convert it to an integer; most significant bytes come first */
    result = 0;
    let mut i: c_int = 0;
    while i < len {
        result <<= BITS_PER_BYTE;
        result |= *((VARDATA_ANY(v as *const c_char) as *const u8).add(i as usize)) as u32;
        i += 1;
    }

    PG_RETURN_INT32!(result as i32);
}

/* Cast bytea -> int8 */
pub unsafe fn bytea_int8(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let len: c_int = VARSIZE_ANY_EXHDR(v as *const c_char) as c_int;
    let mut result: u64;

    /* Check that the byte array is not too long */
    if len as usize > core::mem::size_of::<u64>() {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("bigint out of range"));
    }

    /* Convert it to an integer; most significant bytes come first */
    result = 0;
    let mut i: c_int = 0;
    while i < len {
        result <<= BITS_PER_BYTE;
        result |= *((VARDATA_ANY(v as *const c_char) as *const u8).add(i as usize)) as u64;
        i += 1;
    }

    PG_RETURN_INT64!(result as i64);
}

/* Cast int2 -> bytea; can just use int2send() */
pub unsafe fn int2_bytea(fcinfo: FunctionCallInfo) -> Datum {
    int2send(fcinfo)
}

/* Cast int4 -> bytea; can just use int4send() */
pub unsafe fn int4_bytea(fcinfo: FunctionCallInfo) -> Datum {
    int4send(fcinfo)
}

/* Cast int8 -> bytea; can just use int8send() */
pub unsafe fn int8_bytea(fcinfo: FunctionCallInfo) -> Datum {
    int8send(fcinfo)
}

/*
 * appendStringInfoText
 *
 * Append a text to str.
 * Like appendStringInfoString(str, text_to_cstring(t)) but faster.
 */
unsafe fn appendStringInfoText(str: StringInfo, t: *const text) {
    appendBinaryStringInfo(
        str,
        VARDATA_ANY(t as *const c_char) as *const c_void,
        VARSIZE_ANY_EXHDR(t as *const c_char) as c_int,
    );
}

/* ===================================================================
 * text_position helpers, replace_text, split, array_to_text, to_base,
 * (translated from varlena.c)
 * =================================================================== */

/*
 * Output data for split_text(): we output either to an array or a table.
 * tupstore and tupdesc must be set up in advance to output to a table.
 */
#[repr(C)]
struct SplitTextOutputData {
    astate: *mut ArrayBuildState,
    tupstore: *mut Tuplestorestate,
    tupdesc: TupleDesc,
}

unsafe fn text_position_get_match_ptr(state: *mut TextPositionState) -> *mut c_char {
    (*state).last_match
}

/*
 * Reset search state to the initial state installed by text_position_setup.
 *
 * The next call to text_position_next will search from the beginning of the
 * string.
 */
unsafe fn text_position_reset(state: *mut TextPositionState) {
    (*state).last_match = core::ptr::null_mut();
    (*state).refpoint = (*state).str1;
    (*state).refpos = 0;
}

/*
 * replace_text
 * replace all occurrences of 'old_sub_str' in 'orig_str'
 * with 'new_sub_str' to form 'new_str'
 *
 * returns 'orig_str' if 'old_sub_str' == '' or 'orig_str' == ''
 * otherwise returns 'new_str'
 */
pub unsafe fn replace_text(fcinfo: FunctionCallInfo) -> Datum {
    let src_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let from_sub_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let to_sub_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 2);
    let src_text_len: c_int;
    let from_sub_text_len: c_int;
    let mut state: TextPositionState = core::mem::zeroed();
    let ret_text: *mut text;
    let mut chunk_len: c_int;
    let mut curr_ptr: *mut c_char;
    let mut start_ptr: *mut c_char;
    let mut str: StringInfoData = core::mem::zeroed();
    let mut found: bool;

    src_text_len = VARSIZE_ANY_EXHDR(src_text as *const c_char) as c_int;
    from_sub_text_len = VARSIZE_ANY_EXHDR(from_sub_text as *const c_char) as c_int;

    /* Return unmodified source string if empty source or pattern */
    if src_text_len < 1 || from_sub_text_len < 1 {
        PG_RETURN_TEXT_P!(src_text);
    }

    text_position_setup(src_text, from_sub_text, PG_GET_COLLATION!(fcinfo), &mut state);

    found = text_position_next(&mut state);

    /* When the from_sub_text is not found, there is nothing to do. */
    if !found {
        text_position_cleanup(&mut state);
        PG_RETURN_TEXT_P!(src_text);
    }
    curr_ptr = text_position_get_match_ptr(&mut state);
    start_ptr = VARDATA_ANY(src_text as *const c_char);

    initStringInfo(&mut str);

    loop {
        CHECK_FOR_INTERRUPTS!();

        /* copy the data skipped over by last text_position_next() */
        chunk_len = curr_ptr.offset_from(start_ptr) as c_int;
        appendBinaryStringInfo(&mut str, start_ptr as *const c_void, chunk_len);

        appendStringInfoText(&mut str, to_sub_text);

        start_ptr = curr_ptr.add(state.last_match_len as usize);

        found = text_position_next(&mut state);
        if found {
            curr_ptr = text_position_get_match_ptr(&mut state);
        }

        if !found {
            break;
        }
    }

    /* copy trailing data */
    chunk_len = ((src_text as *mut c_char).add(VARSIZE_ANY(src_text as *const c_char) as usize))
        .offset_from(start_ptr) as c_int;
    appendBinaryStringInfo(&mut str, start_ptr as *const c_void, chunk_len);

    text_position_cleanup(&mut state);

    ret_text = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data as *mut c_void);

    PG_RETURN_TEXT_P!(ret_text);
}

/*
 * check_replace_text_has_escape
 *
 * Returns 0 if text contains no backslashes that need processing.
 * Returns 1 if text contains backslashes, but not regexp submatch specifiers.
 * Returns 2 if text contains regexp submatch specifiers (\1 .. \9).
 */
unsafe fn check_replace_text_has_escape(replace_text: *const text) -> c_int {
    let mut result: c_int = 0;
    let mut p: *const c_char = VARDATA_ANY(replace_text as *const c_char);
    let p_end: *const c_char = p.add(VARSIZE_ANY_EXHDR(replace_text as *const c_char) as usize);

    while p < p_end {
        /* Find next escape char, if any. */
        let found = memchr(p as *const c_void, b'\\' as c_int, p_end.offset_from(p) as usize);
        if found.is_null() {
            break;
        }
        p = (found as *const c_char).add(1);
        /* Note: a backslash at the end doesn't require extra processing. */
        if p < p_end {
            if *p >= b'1' as c_char && *p <= b'9' as c_char {
                return 2; /* Found a submatch specifier, so done */
            }
            result = 1; /* Found some other sequence, keep looking */
            p = p.add(1);
        }
    }
    result
}

/*
 * appendStringInfoRegexpSubstr
 *
 * Append replace_text to str, substituting regexp back references for
 * \n escapes.  start_ptr is the start of the match in the source string,
 * at logical character position data_pos.
 */
unsafe fn appendStringInfoRegexpSubstr(
    str: StringInfo,
    replace_text: *mut text,
    pmatch: *mut regmatch_t,
    start_ptr: *mut c_char,
    data_pos: c_int,
) {
    let mut p: *const c_char = VARDATA_ANY(replace_text as *const c_char);
    let p_end: *const c_char = p.add(VARSIZE_ANY_EXHDR(replace_text as *const c_char) as usize);

    while p < p_end {
        let chunk_start: *const c_char = p;
        let so: c_int;
        let eo: c_int;

        /* Find next escape char, if any. */
        let found = memchr(p as *const c_void, b'\\' as c_int, p_end.offset_from(p) as usize);
        if found.is_null() {
            p = p_end;
        } else {
            p = found as *const c_char;
        }

        /* Copy the text we just scanned over, if any. */
        if p > chunk_start {
            appendBinaryStringInfo(str, chunk_start as *const c_void, p.offset_from(chunk_start) as c_int);
        }

        /* Done if at end of string, else advance over escape char. */
        if p >= p_end {
            break;
        }
        p = p.add(1);

        if p >= p_end {
            /* Escape at very end of input.  Treat same as unexpected char */
            appendStringInfoChar(str, b'\\' as c_char);
            break;
        }

        if *p >= b'1' as c_char && *p <= b'9' as c_char {
            /* Use the back reference of regexp. */
            let idx = (*p - b'0' as c_char) as isize;

            so = (*pmatch.offset(idx)).rm_so as c_int;
            eo = (*pmatch.offset(idx)).rm_eo as c_int;
            p = p.add(1);
        } else if *p == b'&' as c_char {
            /* Use the entire matched string. */
            so = (*pmatch).rm_so as c_int;
            eo = (*pmatch).rm_eo as c_int;
            p = p.add(1);
        } else if *p == b'\\' as c_char {
            /* \\ means transfer one \ to output. */
            appendStringInfoChar(str, b'\\' as c_char);
            p = p.add(1);
            continue;
        } else {
            /*
             * If escape char is not followed by any expected char, just treat
             * it as ordinary data to copy.  (XXX would it be better to throw
             * an error?)
             */
            appendStringInfoChar(str, b'\\' as c_char);
            continue;
        }

        if so >= 0 && eo >= 0 {
            /*
             * Copy the text that is back reference of regexp.  Note so and eo
             * are counted in characters not bytes.
             */
            let mut chunk_start2: *mut c_char;
            let chunk_len: c_int;

            assert!(so >= data_pos);
            chunk_start2 = start_ptr;
            chunk_start2 = chunk_start2.add(charlen_to_bytelen(chunk_start2, so - data_pos) as usize);
            chunk_len = charlen_to_bytelen(chunk_start2, eo - so);
            appendBinaryStringInfo(str, chunk_start2 as *const c_void, chunk_len);
        }
    }
}

/*
 * replace_text_regexp
 *
 * replace substring(s) in src_text that match pattern with replace_text.
 */
pub unsafe fn replace_text_regexp(
    src_text: *mut text,
    pattern_text: *mut text,
    replace_text: *mut text,
    cflags: c_int,
    collation: Oid,
    search_start_in: c_int,
    n: c_int,
) -> *mut text {
    let mut cflags = cflags;
    let mut search_start = search_start_in as Size;
    let ret_text: *mut text;
    let re: *mut regex_t;
    let src_text_len: c_int = VARSIZE_ANY_EXHDR(src_text as *const c_char) as c_int;
    let mut nmatches: c_int = 0;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut pmatch: [regmatch_t; 10] = core::mem::zeroed(); /* main match, plus \1 to \9 */
    let mut nmatch: c_int = lengthof!(pmatch) as c_int;
    let data: *mut pg_wchar;
    let data_len: Size;
    let mut data_pos: c_int;
    let mut start_ptr: *mut c_char;
    let escape_status: c_int;

    initStringInfo(&mut buf);

    /* Convert data string to wide characters. */
    data = palloc(((src_text_len + 1) as usize * core::mem::size_of::<pg_wchar>()) as Size)
        as *mut pg_wchar;
    data_len = pg_mb2wchar_with_len(VARDATA_ANY(src_text as *const c_char), data, src_text_len)
        as Size;

    /* Check whether replace_text has escapes, especially regexp submatches. */
    escape_status = check_replace_text_has_escape(replace_text);

    /* If no regexp submatches, we can use REG_NOSUB. */
    if escape_status < 2 {
        cflags |= REG_NOSUB;
        /* Also tell pg_regexec we only want the whole-match location. */
        nmatch = 1;
    }

    /* Prepare the regexp. */
    re = RE_compile_and_cache(pattern_text, cflags, collation);

    /* start_ptr points to the data_pos'th character of src_text */
    start_ptr = VARDATA_ANY(src_text as *const c_char);
    data_pos = 0;

    while search_start <= data_len {
        let regexec_result: c_int;

        CHECK_FOR_INTERRUPTS!();

        regexec_result = pg_regexec(
            re,
            data,
            data_len,
            search_start,
            core::ptr::null_mut(), /* no details */
            nmatch as Size,
            pmatch.as_mut_ptr(),
            0,
        );

        if regexec_result == REG_NOMATCH {
            break;
        }

        if regexec_result != REG_OKAY {
            let mut err_msg: [c_char; 100] = [0; 100];

            pg_regerror(regexec_result, re, err_msg.as_mut_ptr(), err_msg.len());
            let _ = errcode(ERRCODE_INVALID_REGULAR_EXPRESSION);
            ereport!(
                ERROR,
                errmsg!(
                    "regular expression failed: {}",
                    std::ffi::CStr::from_ptr(err_msg.as_ptr()).to_string_lossy()
                )
            );
        }

        /*
         * Count matches, and decide whether to replace this match.
         */
        nmatches += 1;
        if n > 0 && nmatches != n {
            /*
             * No, so advance search_start, but not start_ptr/data_pos.
             */
            search_start = pmatch[0].rm_eo as Size;
            if pmatch[0].rm_so == pmatch[0].rm_eo {
                search_start += 1;
            }
            continue;
        }

        /*
         * Copy the text to the left of the match position.  Note we are given
         * character not byte indexes.
         */
        if pmatch[0].rm_so as c_int - data_pos > 0 {
            let chunk_len: c_int;

            chunk_len = charlen_to_bytelen(start_ptr, pmatch[0].rm_so as c_int - data_pos);
            appendBinaryStringInfo(&mut buf, start_ptr as *const c_void, chunk_len);

            /*
             * Advance start_ptr over that text.
             */
            start_ptr = start_ptr.add(chunk_len as usize);
            data_pos = pmatch[0].rm_so as c_int;
        }

        /*
         * Copy the replace_text, processing escapes if any are present.
         */
        if escape_status > 0 {
            appendStringInfoRegexpSubstr(&mut buf, replace_text, pmatch.as_mut_ptr(), start_ptr, data_pos);
        } else {
            appendStringInfoText(&mut buf, replace_text);
        }

        /* Advance start_ptr and data_pos over the matched text. */
        start_ptr = start_ptr.add(charlen_to_bytelen(start_ptr, pmatch[0].rm_eo as c_int - data_pos) as usize);
        data_pos = pmatch[0].rm_eo as c_int;

        /*
         * If we only want to replace one occurrence, we're done.
         */
        if n > 0 {
            break;
        }

        /*
         * Advance search position.
         */
        search_start = data_pos as Size;
        if pmatch[0].rm_so == pmatch[0].rm_eo {
            search_start += 1;
        }
    }

    /*
     * Copy the text to the right of the last match.
     */
    if (data_pos as Size) < data_len {
        let chunk_len: c_int;

        chunk_len = ((src_text as *mut c_char).add(VARSIZE_ANY(src_text as *const c_char) as usize))
            .offset_from(start_ptr) as c_int;
        appendBinaryStringInfo(&mut buf, start_ptr as *const c_void, chunk_len);
    }

    ret_text = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data as *mut c_void);
    pfree(data as *mut c_void);

    ret_text
}

/*
 * split_part
 * parse input string based on provided field separator
 * return N'th item (1 based, negative counts from end)
 */
pub unsafe fn split_part(fcinfo: FunctionCallInfo) -> Datum {
    let inputstring: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let fldsep: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut fldnum: c_int = PG_GETARG_INT32!(fcinfo, 2);
    let inputstring_len: c_int;
    let fldsep_len: c_int;
    let mut state: TextPositionState = core::mem::zeroed();
    let mut start_ptr: *mut c_char;
    let mut end_ptr: *mut c_char;
    let result_text: *mut text;
    let mut found: bool;

    /* field number is 1 based */
    if fldnum == 0 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("field position must not be zero"));
    }

    inputstring_len = VARSIZE_ANY_EXHDR(inputstring as *const c_char) as c_int;
    fldsep_len = VARSIZE_ANY_EXHDR(fldsep as *const c_char) as c_int;

    /* return empty string for empty input string */
    if inputstring_len < 1 {
        PG_RETURN_TEXT_P!(cstring_to_text(c"".as_ptr() as *const c_char));
    }

    /* handle empty field separator */
    if fldsep_len < 1 {
        /* if first or last field, return input string, else empty string */
        if fldnum == 1 || fldnum == -1 {
            PG_RETURN_TEXT_P!(inputstring);
        } else {
            PG_RETURN_TEXT_P!(cstring_to_text(c"".as_ptr() as *const c_char));
        }
    }

    /* find the first field separator */
    text_position_setup(inputstring, fldsep, PG_GET_COLLATION!(fcinfo), &mut state);

    found = text_position_next(&mut state);

    /* special case if fldsep not found at all */
    if !found {
        text_position_cleanup(&mut state);
        /* if first or last field, return input string, else empty string */
        if fldnum == 1 || fldnum == -1 {
            PG_RETURN_TEXT_P!(inputstring);
        } else {
            PG_RETURN_TEXT_P!(cstring_to_text(c"".as_ptr() as *const c_char));
        }
    }

    /*
     * take care of a negative field number (i.e. count from the right) by
     * converting to a positive field number; we need total number of fields
     */
    if fldnum < 0 {
        /* we found a fldsep, so there are at least two fields */
        let mut numfields: c_int = 2;

        while text_position_next(&mut state) {
            numfields += 1;
        }

        /* special case of last field does not require an extra pass */
        if fldnum == -1 {
            start_ptr = text_position_get_match_ptr(&mut state).add(state.last_match_len as usize);
            end_ptr = VARDATA_ANY(inputstring as *const c_char).add(inputstring_len as usize);
            text_position_cleanup(&mut state);
            PG_RETURN_TEXT_P!(cstring_to_text_with_len(
                start_ptr,
                end_ptr.offset_from(start_ptr) as c_int
            ));
        }

        /* else, convert fldnum to positive notation */
        fldnum += numfields + 1;

        /* if nonexistent field, return empty string */
        if fldnum <= 0 {
            text_position_cleanup(&mut state);
            PG_RETURN_TEXT_P!(cstring_to_text(c"".as_ptr() as *const c_char));
        }

        /* reset to pointing at first match, but now with positive fldnum */
        text_position_reset(&mut state);
        found = text_position_next(&mut state);
        assert!(found);
    }

    /* identify bounds of first field */
    start_ptr = VARDATA_ANY(inputstring as *const c_char);
    end_ptr = text_position_get_match_ptr(&mut state);

    fldnum -= 1;
    while found && fldnum > 0 {
        /* identify bounds of next field */
        start_ptr = end_ptr.add(state.last_match_len as usize);
        found = text_position_next(&mut state);
        if found {
            end_ptr = text_position_get_match_ptr(&mut state);
        }
        fldnum -= 1;
    }

    text_position_cleanup(&mut state);

    if fldnum > 0 {
        /* N'th field separator not found */
        /* if last field requested, return it, else empty string */
        if fldnum == 1 {
            let last_len: c_int = start_ptr.offset_from(VARDATA_ANY(inputstring as *const c_char)) as c_int;

            result_text = cstring_to_text_with_len(start_ptr, inputstring_len - last_len);
        } else {
            result_text = cstring_to_text(c"".as_ptr() as *const c_char);
        }
    } else {
        /* non-last field requested */
        result_text = cstring_to_text_with_len(start_ptr, end_ptr.offset_from(start_ptr) as c_int);
    }

    PG_RETURN_TEXT_P!(result_text);
}

/*
 * Convenience function to return true when two text params are equal.
 */
unsafe fn text_isequal(txt1: *mut text, txt2: *mut text, collid: Oid) -> bool {
    DatumGetBool(DirectFunctionCall2Coll(
        texteq,
        collid,
        PointerGetDatum(txt1 as *const c_void),
        PointerGetDatum(txt2 as *const c_void),
    ))
}

/*
 * text_to_array
 * parse input string and return text array of elements,
 * based on provided field separator
 */
pub unsafe fn text_to_array(fcinfo: FunctionCallInfo) -> Datum {
    let mut tstate: SplitTextOutputData = core::mem::zeroed();

    /* For array output, tstate should start as all zeroes */
    memset_v(
        &mut tstate as *mut SplitTextOutputData as *mut c_void,
        0,
        core::mem::size_of::<SplitTextOutputData>(),
    );

    if !split_text(fcinfo, &mut tstate) {
        PG_RETURN_NULL!(fcinfo);
    }

    if tstate.astate.is_null() {
        PG_RETURN_ARRAYTYPE_P!(construct_empty_array(TEXTOID));
    }

    PG_RETURN_DATUM!(makeArrayResult(tstate.astate, CurrentMemoryContext));
}

/*
 * text_to_array_null
 */
pub unsafe fn text_to_array_null(fcinfo: FunctionCallInfo) -> Datum {
    text_to_array(fcinfo)
}

/*
 * text_to_table
 * parse input string and return table of elements,
 * based on provided field separator
 */
pub unsafe fn text_to_table(fcinfo: FunctionCallInfo) -> Datum {
    let rsi: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut tstate: SplitTextOutputData = core::mem::zeroed();

    tstate.astate = core::ptr::null_mut();
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    tstate.tupstore = (*rsi).setResult;
    tstate.tupdesc = (*rsi).setDesc;

    let _ = split_text(fcinfo, &mut tstate);

    0 as Datum
}

/*
 * text_to_table_null
 */
pub unsafe fn text_to_table_null(fcinfo: FunctionCallInfo) -> Datum {
    text_to_table(fcinfo)
}

/*
 * Common code for text_to_array, text_to_array_null, text_to_table
 * and text_to_table_null functions.
 */
unsafe fn split_text(fcinfo: FunctionCallInfo, tstate: *mut SplitTextOutputData) -> bool {
    let inputstring: *mut text;
    let fldsep: *mut text;
    let null_string: *mut text;
    let collation: Oid = PG_GET_COLLATION!(fcinfo);
    let mut inputstring_len: c_int;
    let fldsep_len: c_int;
    let mut start_ptr: *mut c_char;
    let mut result_text: *mut text;

    /* when input string is NULL, then result is NULL too */
    if PG_ARGISNULL!(fcinfo, 0) {
        return false;
    }

    inputstring = PG_GETARG_TEXT_PP!(fcinfo, 0);

    /* fldsep can be NULL */
    if !PG_ARGISNULL!(fcinfo, 1) {
        fldsep = PG_GETARG_TEXT_PP!(fcinfo, 1);
    } else {
        fldsep = core::ptr::null_mut();
    }

    /* null_string can be NULL or omitted */
    if PG_NARGS!(fcinfo) > 2 && !PG_ARGISNULL!(fcinfo, 2) {
        null_string = PG_GETARG_TEXT_PP!(fcinfo, 2);
    } else {
        null_string = core::ptr::null_mut();
    }

    if !fldsep.is_null() {
        /*
         * Normal case with non-null fldsep.
         */
        let mut state: TextPositionState = core::mem::zeroed();

        inputstring_len = VARSIZE_ANY_EXHDR(inputstring as *const c_char) as c_int;
        fldsep_len = VARSIZE_ANY_EXHDR(fldsep as *const c_char) as c_int;

        /* return empty set for empty input string */
        if inputstring_len < 1 {
            return true;
        }

        /* empty field separator: return input string as a one-element set */
        if fldsep_len < 1 {
            split_text_accum_result(tstate, inputstring, null_string, collation);
            return true;
        }

        text_position_setup(inputstring, fldsep, collation, &mut state);

        start_ptr = VARDATA_ANY(inputstring as *const c_char);

        loop {
            let found: bool;
            let mut end_ptr: *mut c_char = core::ptr::null_mut();
            let chunk_len: c_int;

            CHECK_FOR_INTERRUPTS!();

            found = text_position_next(&mut state);
            if !found {
                /* fetch last field */
                chunk_len = ((inputstring as *mut c_char)
                    .add(VARSIZE_ANY(inputstring as *const c_char) as usize))
                .offset_from(start_ptr) as c_int;
            } else {
                /* fetch non-last field */
                end_ptr = text_position_get_match_ptr(&mut state);
                chunk_len = end_ptr.offset_from(start_ptr) as c_int;
            }

            /* build a temp text datum to pass to split_text_accum_result */
            result_text = cstring_to_text_with_len(start_ptr, chunk_len);

            /* stash away this field */
            split_text_accum_result(tstate, result_text, null_string, collation);

            pfree(result_text as *mut c_void);

            if !found {
                break;
            }

            start_ptr = end_ptr.add(state.last_match_len as usize);
        }

        text_position_cleanup(&mut state);
    } else {
        let end_ptr: *const c_char;

        /*
         * When fldsep is NULL, each character in the input string becomes a
         * separate element in the result set.
         */
        inputstring_len = VARSIZE_ANY_EXHDR(inputstring as *const c_char) as c_int;

        start_ptr = VARDATA_ANY(inputstring as *const c_char);
        end_ptr = start_ptr.add(inputstring_len as usize);

        while inputstring_len > 0 {
            let chunk_len: c_int = pg_mblen_range(start_ptr, end_ptr);

            CHECK_FOR_INTERRUPTS!();

            /* build a temp text datum to pass to split_text_accum_result */
            let result_text2 = cstring_to_text_with_len(start_ptr, chunk_len);

            /* stash away this field */
            split_text_accum_result(tstate, result_text2, null_string, collation);

            pfree(result_text2 as *mut c_void);

            start_ptr = start_ptr.add(chunk_len as usize);
            inputstring_len -= chunk_len;
        }
    }

    true
}

/*
 * Add text item to result set (table or array).
 */
unsafe fn split_text_accum_result(
    tstate: *mut SplitTextOutputData,
    field_value: *mut text,
    null_string: *mut text,
    collation: Oid,
) {
    let mut is_null: bool = false;

    if !null_string.is_null() && text_isequal(field_value, null_string, collation) {
        is_null = true;
    }

    if !(*tstate).tupstore.is_null() {
        let mut values: [Datum; 1] = [0; 1];
        let mut nulls: [bool; 1] = [false; 1];

        values[0] = PointerGetDatum(field_value as *const c_void);
        nulls[0] = is_null;

        tuplestore_putvalues(
            (*tstate).tupstore,
            (*tstate).tupdesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    } else {
        (*tstate).astate = accumArrayResult(
            (*tstate).astate,
            PointerGetDatum(field_value as *const c_void),
            is_null,
            TEXTOID,
            CurrentMemoryContext,
        );
    }
}

/*
 * array_to_text
 */
pub unsafe fn array_to_text(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let fldsep: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1));

    PG_RETURN_TEXT_P!(array_to_text_internal(fcinfo, v, fldsep, core::ptr::null()));
}

/*
 * array_to_text_null
 */
pub unsafe fn array_to_text_null(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut ArrayType;
    let fldsep: *mut c_char;
    let null_string: *const c_char;

    /* returns NULL when first or second parameter is NULL */
    if PG_ARGISNULL!(fcinfo, 0) || PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_NULL!(fcinfo);
    }

    v = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    fldsep = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1));

    /* NULL null string is passed through as a null pointer */
    if !PG_ARGISNULL!(fcinfo, 2) {
        null_string = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 2));
    } else {
        null_string = core::ptr::null();
    }

    PG_RETURN_TEXT_P!(array_to_text_internal(fcinfo, v, fldsep, null_string));
}

/*
 * common code for array_to_text and array_to_text_null functions
 */
unsafe fn array_to_text_internal(
    fcinfo: FunctionCallInfo,
    v: *mut ArrayType,
    fldsep: *const c_char,
    null_string: *const c_char,
) -> *mut text {
    let result: *mut text;
    let nitems: c_int;
    let dims: *mut c_int;
    let ndims: c_int;
    let element_type: Oid;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut printed: bool = false;
    let mut p: *mut c_char;
    let mut bitmap: *mut bits8;
    let mut bitmask: c_int;
    let mut i: c_int;
    let my_extra: *mut ArrayMetaState;

    ndims = ARR_NDIM(v);
    dims = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndims, dims);

    /* if there are no elements, return an empty string */
    if nitems == 0 {
        return cstring_to_text_with_len(c"".as_ptr() as *const c_char, 0);
    }

    element_type = ARR_ELEMTYPE(v);
    initStringInfo(&mut buf);

    /*
     * We arrange to look up info about element type only once per series of
     * calls, assuming the element type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>() as Size,
        );
        let me = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*me).element_type = !element_type;
    }
    let my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;

    if (*my_extra).element_type != element_type {
        /*
         * Get info about element type, including its output conversion proc
         */
        get_type_io_data(
            element_type,
            IOFunc_output,
            &mut (*my_extra).typlen,
            &mut (*my_extra).typbyval,
            &mut (*my_extra).typalign,
            &mut (*my_extra).typdelim,
            &mut (*my_extra).typioparam,
            &mut (*my_extra).typiofunc,
        );
        fmgr_info_cxt((*my_extra).typiofunc, &mut (*my_extra).proc, (*(*fcinfo).flinfo).fn_mcxt);
        (*my_extra).element_type = element_type;
    }
    typlen = (*my_extra).typlen as c_int;
    typbyval = (*my_extra).typbyval;
    typalign = (*my_extra).typalign;

    p = ARR_DATA_PTR(v);
    bitmap = ARR_NULLBITMAP(v);
    bitmask = 1;

    i = 0;
    while i < nitems {
        let itemvalue: Datum;
        let value: *mut c_char;

        /* Get source element, checking for NULL */
        if !bitmap.is_null() && (*bitmap as c_int & bitmask) == 0 {
            /* if null_string is NULL, we just ignore null elements */
            if !null_string.is_null() {
                if printed {
                    appendStringInfo!(&mut buf, "{}{}",
                        std::ffi::CStr::from_ptr(fldsep).to_string_lossy(),
                        std::ffi::CStr::from_ptr(null_string).to_string_lossy());
                } else {
                    appendStringInfoString(&mut buf, null_string);
                }
                printed = true;
            }
        } else {
            itemvalue = fetch_att(p as *const c_void, typbyval, typlen);

            value = OutputFunctionCall(&mut (*my_extra).proc, itemvalue);

            if printed {
                appendStringInfo!(&mut buf, "{}{}",
                    std::ffi::CStr::from_ptr(fldsep).to_string_lossy(),
                    std::ffi::CStr::from_ptr(value).to_string_lossy());
            } else {
                appendStringInfoString(&mut buf, value);
            }
            printed = true;

            p = p.add(att_addlength_pointer(0, typlen, p));
            p = att_align_nominal(p as usize, typalign) as *mut c_char;
        }

        /* advance bitmap pointer if any */
        if !bitmap.is_null() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmap = bitmap.add(1);
                bitmask = 1;
            }
        }

        i += 1;
    }

    result = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data as *mut c_void);

    result
}

/*
 * Workhorse for to_bin, to_oct, and to_hex.  Note that base must be > 1 and <=
 * 16.
 */
#[inline]
unsafe fn convert_to_base(value: u64, base: c_int) -> *mut text {
    let digits: *const c_char = c"0123456789abcdef".as_ptr() as *const c_char;
    let mut value = value;

    /* We size the buffer for to_bin's longest possible return value. */
    let mut buf: [c_char; core::mem::size_of::<u64>() * BITS_PER_BYTE] =
        [0; core::mem::size_of::<u64>() * BITS_PER_BYTE];
    let end: *mut c_char = buf.as_mut_ptr().add(buf.len());
    let mut ptr: *mut c_char = end;

    assert!(base > 1);
    assert!(base <= 16);

    loop {
        ptr = ptr.sub(1);
        *ptr = *digits.add((value % base as u64) as usize);
        value /= base as u64;

        if !(ptr > buf.as_mut_ptr() && value != 0) {
            break;
        }
    }

    cstring_to_text_with_len(ptr, end.offset_from(ptr) as c_int)
}

pub unsafe fn to_bin32(fcinfo: FunctionCallInfo) -> Datum {
    let value: u64 = PG_GETARG_INT32!(fcinfo, 0) as u32 as u64;

    PG_RETURN_TEXT_P!(convert_to_base(value, 2));
}
pub unsafe fn to_bin64(fcinfo: FunctionCallInfo) -> Datum {
    let value: u64 = PG_GETARG_INT64!(fcinfo, 0) as u64;

    PG_RETURN_TEXT_P!(convert_to_base(value, 2));
}

pub unsafe fn to_oct32(fcinfo: FunctionCallInfo) -> Datum {
    let value: u64 = PG_GETARG_INT32!(fcinfo, 0) as u32 as u64;

    PG_RETURN_TEXT_P!(convert_to_base(value, 8));
}
pub unsafe fn to_oct64(fcinfo: FunctionCallInfo) -> Datum {
    let value: u64 = PG_GETARG_INT64!(fcinfo, 0) as u64;

    PG_RETURN_TEXT_P!(convert_to_base(value, 8));
}

pub unsafe fn to_hex32(fcinfo: FunctionCallInfo) -> Datum {
    let value: u64 = PG_GETARG_INT32!(fcinfo, 0) as u32 as u64;

    PG_RETURN_TEXT_P!(convert_to_base(value, 16));
}
pub unsafe fn to_hex64(fcinfo: FunctionCallInfo) -> Datum {
    let value: u64 = PG_GETARG_INT64!(fcinfo, 0) as u64;

    PG_RETURN_TEXT_P!(convert_to_base(value, 16));
}

pub unsafe fn namegttext(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, btnametextcmp) > 0);
}

pub unsafe fn namegetext(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, btnametextcmp) >= 0);
}

pub unsafe fn textltname(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, bttextnamecmp) < 0);
}

pub unsafe fn textlename(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, bttextnamecmp) <= 0);
}

pub unsafe fn textgtname(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, bttextnamecmp) > 0);
}

pub unsafe fn textgename(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(CmpCall!(fcinfo, bttextnamecmp) >= 0);
}

/*
 * The following operators support character-by-character comparison
 * of text datums, to allow building indexes suitable for LIKE clauses.
 */
unsafe fn internal_text_pattern_compare(arg1: *mut text, arg2: *mut text) -> c_int {
    let result: c_int;
    let len1: c_int;
    let len2: c_int;

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    result = memcmp_v(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );
    if result != 0 {
        result
    } else if len1 < len2 {
        -1
    } else if len1 > len2 {
        1
    } else {
        0
    }
}

pub unsafe fn text_pattern_lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: c_int;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result < 0);
}

pub unsafe fn text_pattern_le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: c_int;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result <= 0);
}

pub unsafe fn text_pattern_ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: c_int;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result >= 0);
}

pub unsafe fn text_pattern_gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: c_int;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_BOOL!(result > 0);
}

pub unsafe fn bttext_pattern_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: c_int;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY!(fcinfo, arg1, 0);
    PG_FREE_IF_COPY!(fcinfo, arg2, 1);

    PG_RETURN_INT32!(result);
}

pub unsafe fn bttext_pattern_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

    /* Use generic string SortSupport, forcing "C" collation */
    varstr_sortsupport(ssup, TEXTOID, C_COLLATION_OID);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID!();
}

/* ===================================================================
 * concat() / text_left / text_right / text_reverse (translated from varlena.c)
 * =================================================================== */

unsafe fn build_concat_foutcache(fcinfo: FunctionCallInfo, argidx: c_int) -> *mut FmgrInfo {
    let foutcache: *mut FmgrInfo;
    let mut i: c_int;

    /* We keep the info in fn_mcxt so it survives across calls */
    foutcache = MemoryContextAlloc(
        (*(*fcinfo).flinfo).fn_mcxt,
        (PG_NARGS!(fcinfo) as usize * core::mem::size_of::<FmgrInfo>()) as Size,
    ) as *mut FmgrInfo;

    i = argidx;
    while i < PG_NARGS!(fcinfo) as c_int {
        let valtype: Oid;
        let mut typ_output: Oid = 0;
        let mut typ_is_varlena: bool = false;

        valtype = get_fn_expr_argtype((*fcinfo).flinfo, i);
        if !OidIsValid(valtype) {
            elog!(ERROR, "could not determine data type of concat() input");
        }

        getTypeOutputInfo(valtype, &mut typ_output, &mut typ_is_varlena);
        fmgr_info_cxt(typ_output, foutcache.offset(i as isize), (*(*fcinfo).flinfo).fn_mcxt);

        i += 1;
    }

    (*(*fcinfo).flinfo).fn_extra = foutcache as *mut c_void;

    foutcache
}

/*
 * Implementation of both concat() and concat_ws().
 */
unsafe fn concat_internal(
    sepstr: *const c_char,
    argidx: c_int,
    fcinfo: FunctionCallInfo,
) -> *mut text {
    let result: *mut text;
    let mut str: StringInfoData = core::mem::zeroed();
    let mut foutcache: *mut FmgrInfo;
    let mut first_arg: bool = true;
    let mut i: c_int;

    /*
     * concat(VARIADIC some-array) is essentially equivalent to array_to_text().
     */
    if get_fn_expr_variadic((*fcinfo).flinfo) {
        let arr: *mut ArrayType;

        /* Should have just the one argument */
        assert!(argidx == PG_NARGS!(fcinfo) as i32 - 1);

        /* concat(VARIADIC NULL) is defined as NULL */
        if PG_ARGISNULL!(fcinfo, argidx) {
            return core::ptr::null_mut();
        }

        assert!(OidIsValid(get_base_element_type(get_fn_expr_argtype(
            (*fcinfo).flinfo,
            argidx
        ))));

        /* OK, safe to fetch the array value */
        arr = PG_GETARG_ARRAYTYPE_P!(fcinfo, argidx);

        /*
         * And serialize the array.
         */
        return array_to_text_internal(fcinfo, arr, sepstr, core::ptr::null());
    }

    /* Normal case without explicit VARIADIC marker */
    initStringInfo(&mut str);

    /* Get output function info, building it if first time through */
    foutcache = (*(*fcinfo).flinfo).fn_extra as *mut FmgrInfo;
    if foutcache.is_null() {
        foutcache = build_concat_foutcache(fcinfo, argidx);
    }

    i = argidx;
    while i < PG_NARGS!(fcinfo) as c_int {
        if !PG_ARGISNULL!(fcinfo, i) {
            let value: Datum = PG_GETARG_DATUM!(fcinfo, i);

            /* add separator if appropriate */
            if first_arg {
                first_arg = false;
            } else {
                appendStringInfoString(&mut str, sepstr);
            }

            /* call the appropriate type output function, append the result */
            appendStringInfoString(
                &mut str,
                OutputFunctionCall(foutcache.offset(i as isize), value),
            );
        }
        i += 1;
    }

    result = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data as *mut c_void);

    result
}

/*
 * Concatenate all arguments. NULL arguments are ignored.
 */
pub unsafe fn text_concat(fcinfo: FunctionCallInfo) -> Datum {
    let result: *mut text;

    result = concat_internal(c"".as_ptr() as *const c_char, 0, fcinfo);
    if result.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_TEXT_P!(result);
}

/*
 * Concatenate all but first argument value with separators.
 */
pub unsafe fn text_concat_ws(fcinfo: FunctionCallInfo) -> Datum {
    let sep: *mut c_char;
    let result: *mut text;

    /* return NULL when separator is NULL */
    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }
    sep = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));

    result = concat_internal(sep, 1, fcinfo);
    if result.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_TEXT_P!(result);
}

/*
 * Return first n characters in the string.
 */
pub unsafe fn text_left(fcinfo: FunctionCallInfo) -> Datum {
    let mut n: c_int = PG_GETARG_INT32!(fcinfo, 1);

    if n < 0 {
        let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let p: *const c_char = VARDATA_ANY(str as *const c_char);
        let len: c_int = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
        let rlen: c_int;

        n = pg_mbstrlen_with_len(p, len) + n;
        rlen = pg_mbcharcliplen(p, len, n);
        PG_RETURN_TEXT_P!(cstring_to_text_with_len(p, rlen));
    } else {
        PG_RETURN_TEXT_P!(text_substring(PG_GETARG_DATUM!(fcinfo, 0), 1, n, false));
    }
}

/*
 * Return last n characters in the string.
 */
pub unsafe fn text_right(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *const c_char = VARDATA_ANY(str as *const c_char);
    let len: c_int = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
    let mut n: c_int = PG_GETARG_INT32!(fcinfo, 1);
    let off: c_int;

    if n < 0 {
        n = -n;
    } else {
        n = pg_mbstrlen_with_len(p, len) - n;
    }
    off = pg_mbcharcliplen(p, len, n);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len(p.add(off as usize), len - off));
}

/*
 * Return reversed string
 */
pub unsafe fn text_reverse(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let mut p: *const c_char = VARDATA_ANY(str as *const c_char);
    let len: c_int = VARSIZE_ANY_EXHDR(str as *const c_char) as c_int;
    let endp: *const c_char = p.add(len as usize);
    let result: *mut text;
    let mut dst: *mut c_char;

    result = palloc((len + VARHDRSZ) as Size) as *mut text;
    dst = (VARDATA(result as *const c_char)).add(len as usize);
    SET_VARSIZE(result as *mut c_char, len + VARHDRSZ);

    if pg_database_encoding_max_length() > 1 {
        /* multibyte version */
        while p < endp {
            let sz: c_int;

            sz = pg_mblen_range(p, endp);
            dst = dst.sub(sz as usize);
            memcpy_v(dst as *mut c_void, p as *const c_void, sz as usize);
            p = p.add(sz as usize);
        }
    } else {
        /* single byte version */
        while p < endp {
            dst = dst.sub(1);
            *dst = *p;
            p = p.add(1);
        }
    }

    PG_RETURN_TEXT_P!(result);
}

/* ===================================================================
 * textToQualifiedNameList / Split* (translated from varlena.c)
 * =================================================================== */

#[no_mangle]
pub unsafe fn textToQualifiedNameList(textval: *mut text) -> *mut List {
    let rawname: *mut c_char;
    let mut result: *mut List = NIL;
    let mut namelist: *mut List = NIL;

    /* Convert to C string (handles possible detoasting). */
    /* Note we rely on being able to modify rawname below. */
    rawname = text_to_cstring(textval);

    if !SplitIdentifierString(rawname, b'.' as c_char, &mut namelist) {
        let _ = errcode(ERRCODE_INVALID_NAME);
        ereport!(ERROR, errmsg!("invalid name syntax"));
    }

    if namelist == NIL {
        let _ = errcode(ERRCODE_INVALID_NAME);
        ereport!(ERROR, errmsg!("invalid name syntax"));
    }

    foreach!(l, namelist, {
        let curname: *mut c_char = lfirst(current_cell!(l)) as *mut c_char;

        result = lappend(result, makeString(pstrdup(curname)) as *mut c_void);
    });

    pfree(rawname as *mut c_void);
    list_free(namelist);

    result
}

/*
 * SplitIdentifierString --- parse a string containing identifiers
 */
pub unsafe fn SplitIdentifierString(
    rawstring: *mut c_char,
    separator: c_char,
    namelist: *mut *mut List,
) -> bool {
    let mut nextp: *mut c_char = rawstring;
    let mut done: bool = false;

    *namelist = NIL;

    while scanner_isspace(*nextp) {
        nextp = nextp.add(1); /* skip leading whitespace */
    }

    if *nextp == b'\0' as c_char {
        return true; /* allow empty string */
    }

    /* At the top of the loop, we are at start of a new identifier. */
    loop {
        let curname: *mut c_char;
        let mut endp: *mut c_char;

        if *nextp == b'"' as c_char {
            /* Quoted name --- collapse quote-quote pairs, no downcasing */
            curname = nextp.add(1);
            loop {
                endp = varlena_strchr(nextp.add(1), b'"' as c_int);
                if endp.is_null() {
                    return false; /* mismatched quotes */
                }
                if *endp.add(1) != b'"' as c_char {
                    break; /* found end of quoted name */
                }
                /* Collapse adjacent quotes into one quote, and look again */
                varlena_memmove(endp as *mut c_void, endp.add(1) as *const c_void, strlen(endp));
                nextp = endp;
            }
            /* endp now points at the terminating quote */
            nextp = endp.add(1);
        } else {
            /* Unquoted name --- extends to separator or whitespace */
            let downname: *mut c_char;
            let len: c_int;

            curname = nextp;
            while *nextp != 0 && *nextp != separator && !scanner_isspace(*nextp) {
                nextp = nextp.add(1);
            }
            endp = nextp;
            if curname == nextp {
                return false; /* empty unquoted name not allowed */
            }

            /*
             * Downcase the identifier, using same code as main lexer does.
             */
            len = endp.offset_from(curname) as c_int;
            downname = downcase_truncate_identifier(curname, len, false);
            assert!(strlen(downname) <= len as usize);
            varlena_strncpy(curname, downname, len as usize); /* strncpy is required here */
            pfree(downname as *mut c_void);
        }

        while scanner_isspace(*nextp) {
            nextp = nextp.add(1); /* skip trailing whitespace */
        }

        if *nextp == separator {
            nextp = nextp.add(1);
            while scanner_isspace(*nextp) {
                nextp = nextp.add(1); /* skip leading whitespace for next */
            }
            /* we expect another name, so done remains false */
        } else if *nextp == b'\0' as c_char {
            done = true;
        } else {
            return false; /* invalid syntax */
        }

        /* Now safe to overwrite separator with a null */
        *endp = b'\0' as c_char;

        /* Truncate name if it's overlength */
        truncate_identifier(curname, strlen(curname) as c_int, false);

        /*
         * Finished isolating current name --- add it to list
         */
        *namelist = lappend(*namelist, curname as *mut c_void);

        if done {
            break;
        }
    }

    true
}

/*
 * SplitDirectoriesString --- parse a string containing file/directory names
 */
pub unsafe fn SplitDirectoriesString(
    rawstring: *mut c_char,
    separator: c_char,
    namelist: *mut *mut List,
) -> bool {
    let mut nextp: *mut c_char = rawstring;
    let mut done: bool = false;

    *namelist = NIL;

    while scanner_isspace(*nextp) {
        nextp = nextp.add(1); /* skip leading whitespace */
    }

    if *nextp == b'\0' as c_char {
        return true; /* allow empty string */
    }

    /* At the top of the loop, we are at start of a new directory. */
    loop {
        let mut curname: *mut c_char;
        let mut endp: *mut c_char;

        if *nextp == b'"' as c_char {
            /* Quoted name --- collapse quote-quote pairs */
            curname = nextp.add(1);
            loop {
                endp = varlena_strchr(nextp.add(1), b'"' as c_int);
                if endp.is_null() {
                    return false; /* mismatched quotes */
                }
                if *endp.add(1) != b'"' as c_char {
                    break; /* found end of quoted name */
                }
                /* Collapse adjacent quotes into one quote, and look again */
                varlena_memmove(endp as *mut c_void, endp.add(1) as *const c_void, strlen(endp));
                nextp = endp;
            }
            /* endp now points at the terminating quote */
            nextp = endp.add(1);
        } else {
            /* Unquoted name --- extends to separator or end of string */
            curname = nextp;
            endp = nextp;
            while *nextp != 0 && *nextp != separator {
                /* trailing whitespace should not be included in name */
                if !scanner_isspace(*nextp) {
                    endp = nextp.add(1);
                }
                nextp = nextp.add(1);
            }
            if curname == endp {
                return false; /* empty unquoted name not allowed */
            }
        }

        while scanner_isspace(*nextp) {
            nextp = nextp.add(1); /* skip trailing whitespace */
        }

        if *nextp == separator {
            nextp = nextp.add(1);
            while scanner_isspace(*nextp) {
                nextp = nextp.add(1); /* skip leading whitespace for next */
            }
            /* we expect another name, so done remains false */
        } else if *nextp == b'\0' as c_char {
            done = true;
        } else {
            return false; /* invalid syntax */
        }

        /* Now safe to overwrite separator with a null */
        *endp = b'\0' as c_char;

        /* Truncate path if it's overlength */
        if strlen(curname) >= MAXPGPATH {
            *curname.add(MAXPGPATH - 1) = b'\0' as c_char;
        }

        /*
         * Finished isolating current name --- add it to list
         */
        curname = pstrdup(curname);
        canonicalize_path(curname);
        *namelist = lappend(*namelist, curname as *mut c_void);

        if done {
            break;
        }
    }

    true
}

/*
 * SplitGUCList --- parse a string containing identifiers or file names
 */
pub unsafe fn SplitGUCList(
    rawstring: *mut c_char,
    separator: c_char,
    namelist: *mut *mut List,
) -> bool {
    let mut nextp: *mut c_char = rawstring;
    let mut done: bool = false;

    *namelist = NIL;

    while scanner_isspace(*nextp) {
        nextp = nextp.add(1); /* skip leading whitespace */
    }

    if *nextp == b'\0' as c_char {
        return true; /* allow empty string */
    }

    /* At the top of the loop, we are at start of a new identifier. */
    loop {
        let curname: *mut c_char;
        let mut endp: *mut c_char;

        if *nextp == b'"' as c_char {
            /* Quoted name --- collapse quote-quote pairs */
            curname = nextp.add(1);
            loop {
                endp = varlena_strchr(nextp.add(1), b'"' as c_int);
                if endp.is_null() {
                    return false; /* mismatched quotes */
                }
                if *endp.add(1) != b'"' as c_char {
                    break; /* found end of quoted name */
                }
                /* Collapse adjacent quotes into one quote, and look again */
                varlena_memmove(endp as *mut c_void, endp.add(1) as *const c_void, strlen(endp));
                nextp = endp;
            }
            /* endp now points at the terminating quote */
            nextp = endp.add(1);
        } else {
            /* Unquoted name --- extends to separator or whitespace */
            curname = nextp;
            while *nextp != 0 && *nextp != separator && !scanner_isspace(*nextp) {
                nextp = nextp.add(1);
            }
            endp = nextp;
            if curname == nextp {
                return false; /* empty unquoted name not allowed */
            }
        }

        while scanner_isspace(*nextp) {
            nextp = nextp.add(1); /* skip trailing whitespace */
        }

        if *nextp == separator {
            nextp = nextp.add(1);
            while scanner_isspace(*nextp) {
                nextp = nextp.add(1); /* skip leading whitespace for next */
            }
            /* we expect another name, so done remains false */
        } else if *nextp == b'\0' as c_char {
            done = true;
        } else {
            return false; /* invalid syntax */
        }

        /* Now safe to overwrite separator with a null */
        *endp = b'\0' as c_char;

        /*
         * Finished isolating current name --- add it to list
         */
        *namelist = lappend(*namelist, curname as *mut c_void);

        if done {
            break;
        }
    }

    true
}

/* detoast.h macros not pub-exported; faithful local copies. */
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const c_char) -> bool {
    crate::varatt::VARATT_IS_EXTERNAL(ptr) && {
        /* VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK; tag byte is at offset 1 */
        let tag = *(ptr.add(1) as *const u8);
        tag == 18 /* VARTAG_ONDISK */
    }
}
#[inline]
unsafe fn VARATT_EXTERNAL_GET_POINTER(toast_pointer: *mut varatt_external, attr: *const c_char) {
    /* memcpy of the on-disk pointer payload (skips 2-byte external header) */
    core::ptr::copy_nonoverlapping(
        attr.add(2) as *const u8,
        toast_pointer as *mut u8,
        core::mem::size_of::<varatt_external>(),
    );
}

/*
 * Return the size of a datum, possibly compressed
 *
 * Works on any data type
 */
pub unsafe fn pg_column_size(fcinfo: FunctionCallInfo) -> Datum {
    let value: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let result: int32;
    let typlen: c_int;

    /* On first call, get the input type's typlen, and save at *fn_extra */
    if (*(*fcinfo).flinfo).fn_extra.is_null() {
        /* Lookup the datatype of the supplied argument */
        let argtypeid: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 0);

        typlen = get_typlen(argtypeid) as c_int;
        if typlen == 0 {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for type {}", argtypeid);
        }

        (*(*fcinfo).flinfo).fn_extra =
            MemoryContextAlloc((*(*fcinfo).flinfo).fn_mcxt, core::mem::size_of::<c_int>() as Size);
        *((*(*fcinfo).flinfo).fn_extra as *mut c_int) = typlen;
    } else {
        typlen = *((*(*fcinfo).flinfo).fn_extra as *mut c_int);
    }

    if typlen == -1 {
        /* varlena type, possibly toasted */
        result = toast_datum_size(value) as int32;
    } else if typlen == -2 {
        /* cstring */
        result = (strlen(DatumGetCString(value)) + 1) as int32;
    } else {
        /* ordinary fixed-width type */
        result = typlen;
    }

    PG_RETURN_INT32!(result);
}

/*
 * Return the compression method stored in the compressed attribute.  Return
 * NULL for non varlena type or uncompressed data.
 */
pub unsafe fn pg_column_compression(fcinfo: FunctionCallInfo) -> Datum {
    let typlen: c_int;
    let result: *const c_char;
    let cmid: ToastCompressionId;

    /* On first call, get the input type's typlen, and save at *fn_extra */
    if (*(*fcinfo).flinfo).fn_extra.is_null() {
        /* Lookup the datatype of the supplied argument */
        let argtypeid: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 0);

        typlen = get_typlen(argtypeid) as c_int;
        if typlen == 0 {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for type {}", argtypeid);
        }

        (*(*fcinfo).flinfo).fn_extra =
            MemoryContextAlloc((*(*fcinfo).flinfo).fn_mcxt, core::mem::size_of::<c_int>() as Size);
        *((*(*fcinfo).flinfo).fn_extra as *mut c_int) = typlen;
    } else {
        typlen = *((*(*fcinfo).flinfo).fn_extra as *mut c_int);
    }

    if typlen != -1 {
        PG_RETURN_NULL!(fcinfo);
    }

    /* get the compression method id stored in the compressed varlena */
    cmid = toast_get_compression_id(
        DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut crate::c::varlena,
    );
    if cmid == TOAST_INVALID_COMPRESSION_ID {
        PG_RETURN_NULL!(fcinfo);
    }

    /* convert compression method id to compression method name */
    match cmid {
        TOAST_PGLZ_COMPRESSION_ID => {
            result = c"pglz".as_ptr();
        }
        TOAST_LZ4_COMPRESSION_ID => {
            result = c"lz4".as_ptr();
        }
        _ => {
            elog!(ERROR, "invalid compression method id {}", cmid);
            unreachable!();
        }
    }

    PG_RETURN_TEXT_P!(cstring_to_text(result));
}

/*
 * Return the chunk_id of the on-disk TOASTed value.  Return NULL if the value
 * is un-TOASTed or not on-disk.
 */
pub unsafe fn pg_column_toast_chunk_id(fcinfo: FunctionCallInfo) -> Datum {
    let typlen: c_int;
    let attr: *mut crate::c::varlena;
    let mut toast_pointer: varatt_external = core::mem::zeroed();

    /* On first call, get the input type's typlen, and save at *fn_extra */
    if (*(*fcinfo).flinfo).fn_extra.is_null() {
        /* Lookup the datatype of the supplied argument */
        let argtypeid: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 0);

        typlen = get_typlen(argtypeid) as c_int;
        if typlen == 0 {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for type {}", argtypeid);
        }

        (*(*fcinfo).flinfo).fn_extra =
            MemoryContextAlloc((*(*fcinfo).flinfo).fn_mcxt, core::mem::size_of::<c_int>() as Size);
        *((*(*fcinfo).flinfo).fn_extra as *mut c_int) = typlen;
    } else {
        typlen = *((*(*fcinfo).flinfo).fn_extra as *mut c_int);
    }

    if typlen != -1 {
        PG_RETURN_NULL!(fcinfo);
    }

    attr = DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut crate::c::varlena;

    if !VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        PG_RETURN_NULL!(fcinfo);
    }

    VARATT_EXTERNAL_GET_POINTER(&mut toast_pointer, attr as *const c_char);

    PG_RETURN_OID!(toast_pointer.va_valueid);
}

/*
 * string_agg_transfn
 */
pub unsafe fn string_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: StringInfo = if PG_ARGISNULL!(fcinfo, 0) {
        core::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as StringInfo
    };

    /* Append the value unless null, preceding it with the delimiter. */
    if !PG_ARGISNULL!(fcinfo, 1) {
        let value: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
        let mut isfirst = false;

        /*
         * You might think we can just throw away the first delimiter, however
         * we must keep it as we may be a parallel worker doing partial
         * aggregation building a state to send to the main process.
         */
        if state.is_null() {
            state = makeStringAggState(fcinfo);
            isfirst = true;
        }

        if !PG_ARGISNULL!(fcinfo, 2) {
            let delim: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 2);

            appendStringInfoText(state, delim);
            if isfirst {
                (*state).cursor = VARSIZE_ANY_EXHDR(delim as *const c_char) as c_int;
            }
        }

        appendStringInfoText(state, value);
    }

    /*
     * The transition type for string_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    if !state.is_null() {
        PG_RETURN_POINTER!(state as *mut c_void);
    }
    PG_RETURN_NULL!(fcinfo);
}

/*
 * string_agg_combine
 *		Aggregate combine function for string_agg(text) and string_agg(bytea)
 */
pub unsafe fn string_agg_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: StringInfo;
    let state2: StringInfo;
    let mut agg_context: MemoryContext = core::ptr::null_mut();

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        core::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as StringInfo
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        core::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as StringInfo
    };

    if state2.is_null() {
        /*
         * NULL state2 is easy, just return state1, which we know is already
         * in the agg_context
         */
        if state1.is_null() {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if state1.is_null() {
        /* We must copy state2's data into the agg_context */
        let old_context: MemoryContext = MemoryContextSwitchTo(agg_context);
        state1 = makeStringAggState(fcinfo);
        appendBinaryStringInfo(state1, (*state2).data as *const c_void, (*state2).len);
        (*state1).cursor = (*state2).cursor;
        MemoryContextSwitchTo(old_context);
    } else if (*state2).len > 0 {
        /* Combine ... state1->cursor does not change in this case */
        appendBinaryStringInfo(state1, (*state2).data as *const c_void, (*state2).len);
    }

    PG_RETURN_POINTER!(state1 as *mut c_void);
}

/*
 * string_agg_serialize
 *		Aggregate serialize function for string_agg(text) and string_agg(bytea)
 *
 * This is strict, so we need not handle NULL input
 */
pub unsafe fn string_agg_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: StringInfo;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut bytea;

    /* cannot be called directly because of internal-type argument */
    debug_assert!(AggCheckCallContext(fcinfo, core::ptr::null_mut()) != 0);

    state = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    pq_begintypsend(&mut buf);

    /* cursor */
    pq_sendint(&mut buf, (*state).cursor as u32, 4);

    /* data */
    pq_sendbytes(&mut buf, (*state).data as *const c_void, (*state).len);

    result = pq_endtypsend(&mut buf) as *mut bytea;

    PG_RETURN_BYTEA_P!(result);
}

/*
 * string_agg_deserialize
 *		Aggregate deserial function for string_agg(text) and string_agg(bytea)
 *
 * This is strict, so we need not handle NULL input
 */
pub unsafe fn string_agg_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut bytea;
    let result: StringInfo;
    let mut buf: StringInfoData = core::mem::zeroed();
    let data: *mut c_char;
    let datalen: c_int;

    /* cannot be called directly because of internal-type argument */
    debug_assert!(AggCheckCallContext(fcinfo, core::ptr::null_mut()) != 0);

    sstate = PG_GETARG_BYTEA_PP!(fcinfo, 0);

    /*
     * Initialize a StringInfo so that we can "receive" it using the standard
     * recv-function infrastructure.
     */
    initReadOnlyStringInfo(
        &mut buf,
        VARDATA_ANY(sstate as *const c_char),
        VARSIZE_ANY_EXHDR(sstate as *const c_char) as c_int,
    );

    result = makeStringAggState(fcinfo);

    /* cursor */
    (*result).cursor = pq_getmsgint(&mut buf, 4) as c_int;

    /* data */
    datalen = VARSIZE_ANY_EXHDR(sstate as *const c_char) as c_int - 4;
    data = pq_getmsgbytes(&mut buf, datalen) as *mut c_char;
    appendBinaryStringInfo(result, data as *const c_void, datalen);

    pq_getmsgend(&mut buf);

    PG_RETURN_POINTER!(result as *mut c_void);
}

/*
 * string_agg_finalfn
 */
pub unsafe fn string_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let state: StringInfo;

    /* cannot be called directly because of internal-type argument */
    debug_assert!(AggCheckCallContext(fcinfo, core::ptr::null_mut()) != 0);

    state = if PG_ARGISNULL!(fcinfo, 0) {
        core::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as StringInfo
    };

    if !state.is_null() {
        /* As per comment in transfn, strip data before the cursor position */
        PG_RETURN_TEXT_P!(cstring_to_text_with_len(
            (*state).data.add((*state).cursor as usize),
            (*state).len - (*state).cursor
        ));
    } else {
        PG_RETURN_NULL!(fcinfo);
    }
}

/*
 * Support macros for text_format()
 */
const TEXT_FORMAT_FLAG_MINUS: c_int = 0x0001; /* is minus flag present? */

/*
 * ADVANCE_PARSE_POINTER(ptr, end_ptr): ++ptr, error if at/over end_ptr.
 */
macro_rules! ADVANCE_PARSE_POINTER {
    ($ptr:expr, $end_ptr:expr) => {{
        $ptr = $ptr.add(1);
        if $ptr >= $end_ptr {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            // C also: errhint("For a single \"%%\" use \"%%%%\".")
            ereport!(ERROR, errmsg!("unterminated format() type specifier"));
        }
    }};
}

/*
 * Returns a formatted string
 */
pub unsafe fn text_format(fcinfo: FunctionCallInfo) -> Datum {
    let fmt: *mut text;
    let mut str: StringInfoData = core::mem::zeroed();
    let mut cp: *const c_char;
    let start_ptr: *const c_char;
    let end_ptr: *const c_char;
    let result: *mut text;
    let mut arg: c_int;
    let funcvariadic: bool;
    let nargs: c_int;
    let mut elements: *mut Datum = core::ptr::null_mut();
    let mut nulls: *mut bool = core::ptr::null_mut();
    let mut element_type: Oid = crate::postgres_ext::InvalidOid;
    let mut prev_type: Oid = crate::postgres_ext::InvalidOid;
    let mut prev_width_type: Oid = crate::postgres_ext::InvalidOid;
    let mut typoutputfinfo: FmgrInfo = core::mem::zeroed();
    let mut typoutputinfo_width: FmgrInfo = core::mem::zeroed();

    /* When format string is null, immediately return null */
    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* If argument is marked VARIADIC, expand array into elements */
    if get_fn_expr_variadic((*fcinfo).flinfo) {
        let arr: *mut ArrayType;
        let mut elmlen: i16 = 0;
        let mut elmbyval: bool = false;
        let mut elmalign: c_char = 0;
        let mut nitems: c_int;

        /* Should have just the one argument */
        debug_assert!(PG_NARGS!(fcinfo) as c_int == 2);

        /* If argument is NULL, we treat it as zero-length array */
        if PG_ARGISNULL!(fcinfo, 1) {
            nitems = 0;
        } else {
            /* OK, safe to fetch the array value */
            arr = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);

            /* Get info about array element type */
            element_type = ARR_ELEMTYPE(arr);
            get_typlenbyvalalign(element_type, &mut elmlen, &mut elmbyval, &mut elmalign);

            /* Extract all array elements */
            nitems = 0;
            deconstruct_array(
                arr, element_type, elmlen as c_int, elmbyval, elmalign,
                &mut elements, &mut nulls, &mut nitems,
            );
        }

        nargs = nitems + 1;
        funcvariadic = true;
    } else {
        /* Non-variadic case, we'll process the arguments individually */
        nargs = PG_NARGS!(fcinfo) as c_int;
        funcvariadic = false;
    }

    /* Setup for main loop. */
    fmt = PG_GETARG_TEXT_PP!(fcinfo, 0);
    start_ptr = VARDATA_ANY(fmt as *const c_char);
    end_ptr = start_ptr.add(VARSIZE_ANY_EXHDR(fmt as *const c_char) as usize);
    initStringInfo(&mut str);
    arg = 1; /* next argument position to print */

    /* Scan format string, looking for conversion specifiers. */
    cp = start_ptr;
    while cp < end_ptr {
        let mut argpos: c_int = 0;
        let mut widthpos: c_int = 0;
        let mut flags: c_int = 0;
        let mut width: c_int = 0;
        let mut value: Datum;
        let mut isNull: bool;
        let mut typid: Oid;

        /*
         * If it's not the start of a conversion specifier, just copy it to
         * the output buffer.
         */
        if *cp != b'%' as c_char {
            appendStringInfoCharMacro!(&mut str as StringInfo, *cp);
            cp = cp.add(1);
            continue;
        }

        ADVANCE_PARSE_POINTER!(cp, end_ptr);

        /* Easy case: %% outputs a single % */
        if *cp == b'%' as c_char {
            appendStringInfoCharMacro!(&mut str as StringInfo, *cp);
            cp = cp.add(1);
            continue;
        }

        /* Parse the optional portions of the format specifier */
        cp = text_format_parse_format(
            cp, end_ptr, &mut argpos, &mut widthpos, &mut flags, &mut width,
        );

        /*
         * Next we should see the main conversion specifier.
         */
        if varlena_strchr2(c"sIL".as_ptr(), *cp as c_int).is_null() {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            // C also: errhint("For a single \"%%\" use \"%%%%\".")
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized format() type specifier \"{}\"",
                    {
                        let n = pg_mblen_range(cp, end_ptr) as usize;
                        core::str::from_utf8_unchecked(core::slice::from_raw_parts(cp as *const u8, n))
                    }
                )
            );
        }

        /* If indirect width was specified, get its value */
        if widthpos >= 0 {
            /* Collect the specified or next argument position */
            if widthpos > 0 {
                arg = widthpos;
            }
            if arg >= nargs {
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(ERROR, errmsg!("too few arguments for format()"));
            }

            /* Get the value and type of the selected argument */
            if !funcvariadic {
                value = PG_GETARG_DATUM!(fcinfo, arg);
                isNull = PG_ARGISNULL!(fcinfo, arg);
                typid = get_fn_expr_argtype((*fcinfo).flinfo, arg);
            } else {
                value = *elements.add((arg - 1) as usize);
                isNull = *nulls.add((arg - 1) as usize);
                typid = element_type;
            }
            if !OidIsValid(typid) {
                elog!(ERROR, "could not determine data type of format() input");
            }

            arg += 1;

            /* We can treat NULL width the same as zero */
            if isNull {
                width = 0;
            } else if typid == INT4OID {
                width = DatumGetInt32(value);
            } else if typid == INT2OID {
                width = DatumGetInt16(value) as c_int;
            } else {
                /* For less-usual datatypes, convert to text then to int */
                let strv: *mut c_char;

                if typid != prev_width_type {
                    let mut typoutputfunc: Oid = 0;
                    let mut typIsVarlena: bool = false;

                    getTypeOutputInfo(typid, &mut typoutputfunc, &mut typIsVarlena);
                    fmgr_info(typoutputfunc, &mut typoutputinfo_width);
                    prev_width_type = typid;
                }

                strv = OutputFunctionCall(&mut typoutputinfo_width, value);

                /* pg_strtoint32 will complain about bad data or overflow */
                width = pg_strtoint32(strv);

                pfree(strv as *mut c_void);
            }
        }

        /* Collect the specified or next argument position */
        if argpos > 0 {
            arg = argpos;
        }
        if arg >= nargs {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("too few arguments for format()"));
        }

        /* Get the value and type of the selected argument */
        if !funcvariadic {
            value = PG_GETARG_DATUM!(fcinfo, arg);
            isNull = PG_ARGISNULL!(fcinfo, arg);
            typid = get_fn_expr_argtype((*fcinfo).flinfo, arg);
        } else {
            value = *elements.add((arg - 1) as usize);
            isNull = *nulls.add((arg - 1) as usize);
            typid = element_type;
        }
        if !OidIsValid(typid) {
            elog!(ERROR, "could not determine data type of format() input");
        }

        arg += 1;

        /*
         * Get the appropriate typOutput function, reusing previous one if
         * same type as previous argument.
         */
        if typid != prev_type {
            let mut typoutputfunc: Oid = 0;
            let mut typIsVarlena: bool = false;

            getTypeOutputInfo(typid, &mut typoutputfunc, &mut typIsVarlena);
            fmgr_info(typoutputfunc, &mut typoutputfinfo);
            prev_type = typid;
        }

        /*
         * And now we can format the value.
         */
        match *cp as u8 as char {
            's' | 'I' | 'L' => {
                text_format_string_conversion(
                    &mut str, *cp, &mut typoutputfinfo, value, isNull, flags, width,
                );
            }
            _ => {
                /* should not get here, because of previous check */
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(
                    ERROR,
                    errmsg!(
                        "unrecognized format() type specifier \"{}\"",
                        {
                            let n = pg_mblen_range(cp, end_ptr) as usize;
                            core::str::from_utf8_unchecked(core::slice::from_raw_parts(cp as *const u8, n))
                        }
                    )
                );
            }
        }
        cp = cp.add(1);
    }

    /* Don't need deconstruct_array results anymore. */
    if !elements.is_null() {
        pfree(elements as *mut c_void);
    }
    if !nulls.is_null() {
        pfree(nulls as *mut c_void);
    }

    /* Generate results. */
    result = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data as *mut c_void);

    PG_RETURN_TEXT_P!(result);
}

/*
 * Parse contiguous digits as a decimal number.
 */
unsafe fn text_format_parse_digits(
    ptr: *mut *const c_char,
    end_ptr: *const c_char,
    value: *mut c_int,
) -> bool {
    let mut found = false;
    let mut cp: *const c_char = *ptr;
    let mut val: c_int = 0;

    while *cp >= b'0' as c_char && *cp <= b'9' as c_char {
        let digit: i8 = (*cp - b'0' as c_char) as i8;

        if pg_mul_s32_overflow(val, 10, &mut val) || pg_add_s32_overflow(val, digit as c_int, &mut val) {
            let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            ereport!(ERROR, errmsg!("number is out of range"));
        }
        ADVANCE_PARSE_POINTER!(cp, end_ptr);
        found = true;
    }

    *ptr = cp;
    *value = val;

    found
}

/*
 * Parse a format specifier (generally following the SUS printf spec).
 */
unsafe fn text_format_parse_format(
    start_ptr: *const c_char,
    end_ptr: *const c_char,
    argpos: *mut c_int,
    widthpos: *mut c_int,
    flags: *mut c_int,
    width: *mut c_int,
) -> *const c_char {
    let mut cp: *const c_char = start_ptr;
    let mut n: c_int = 0;

    /* set defaults for output parameters */
    *argpos = -1;
    *widthpos = -1;
    *flags = 0;
    *width = 0;

    /* try to identify first number */
    if text_format_parse_digits(&mut cp, end_ptr, &mut n) {
        if *cp != b'$' as c_char {
            /* Must be just a width and a type, so we're done */
            *width = n;
            return cp;
        }
        /* The number was argument position */
        *argpos = n;
        /* Explicit 0 for argument index is immediately refused */
        if n == 0 {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(
                ERROR,
                errmsg!("format specifies argument 0, but arguments are numbered from 1")
            );
        }
        ADVANCE_PARSE_POINTER!(cp, end_ptr);
    }

    /* Handle flags (only minus is supported now) */
    while *cp == b'-' as c_char {
        *flags |= TEXT_FORMAT_FLAG_MINUS;
        ADVANCE_PARSE_POINTER!(cp, end_ptr);
    }

    if *cp == b'*' as c_char {
        /* Handle indirect width */
        ADVANCE_PARSE_POINTER!(cp, end_ptr);
        if text_format_parse_digits(&mut cp, end_ptr, &mut n) {
            /* number in this position must be closed by $ */
            if *cp != b'$' as c_char {
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(ERROR, errmsg!("width argument position must be ended by \"$\""));
            }
            /* The number was width argument position */
            *widthpos = n;
            /* Explicit 0 for argument index is immediately refused */
            if n == 0 {
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(
                    ERROR,
                    errmsg!("format specifies argument 0, but arguments are numbered from 1")
                );
            }
            ADVANCE_PARSE_POINTER!(cp, end_ptr);
        } else {
            *widthpos = 0; /* width's argument position is unspecified */
        }
    } else {
        /* Check for direct width specification */
        if text_format_parse_digits(&mut cp, end_ptr, &mut n) {
            *width = n;
        }
    }

    /* cp should now be pointing at type character */
    cp
}

/*
 * Format a %s, %I, or %L conversion
 */
unsafe fn text_format_string_conversion(
    buf: StringInfo,
    conversion: c_char,
    typOutputInfo: *mut FmgrInfo,
    value: Datum,
    isNull: bool,
    flags: c_int,
    width: c_int,
) {
    let str: *mut c_char;

    /* Handle NULL arguments before trying to stringify the value. */
    if isNull {
        if conversion == b's' as c_char {
            text_format_append_string(buf, c"".as_ptr(), flags, width);
        } else if conversion == b'L' as c_char {
            text_format_append_string(buf, c"NULL".as_ptr(), flags, width);
        } else if conversion == b'I' as c_char {
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(
                ERROR,
                errmsg!("null values cannot be formatted as an SQL identifier")
            );
        }
        return;
    }

    /* Stringify. */
    str = OutputFunctionCall(typOutputInfo, value);

    /* Escape. */
    if conversion == b'I' as c_char {
        /* quote_identifier may or may not allocate a new string. */
        text_format_append_string(buf, quote_identifier(str), flags, width);
    } else if conversion == b'L' as c_char {
        let qstr: *mut c_char = quote_literal_cstr(str);

        text_format_append_string(buf, qstr, flags, width);
        /* quote_literal_cstr() always allocates a new string */
        pfree(qstr as *mut c_void);
    } else {
        text_format_append_string(buf, str, flags, width);
    }

    /* Cleanup. */
    pfree(str as *mut c_void);
}

/*
 * Append str to buf, padding as directed by flags/width
 */
unsafe fn text_format_append_string(
    buf: StringInfo,
    str: *const c_char,
    flags: c_int,
    mut width: c_int,
) {
    let mut align_to_left = false;
    let len: c_int;

    /* fast path for typical easy case */
    if width == 0 {
        appendStringInfoString(buf, str);
        return;
    }

    if width < 0 {
        /* Negative width: implicit '-' flag, then take absolute value */
        align_to_left = true;
        /* -INT_MIN is undefined */
        if width <= i32::MIN {
            let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            ereport!(ERROR, errmsg!("number is out of range"));
        }
        width = -width;
    } else if (flags & TEXT_FORMAT_FLAG_MINUS) != 0 {
        align_to_left = true;
    }

    len = pg_mbstrlen(str);
    if align_to_left {
        /* left justify */
        appendStringInfoString(buf, str);
        if len < width {
            appendStringInfoSpaces(buf, width - len);
        }
    } else {
        /* right justify */
        if len < width {
            appendStringInfoSpaces(buf, width - len);
        }
        appendStringInfoString(buf, str);
    }
}

/*
 * text_format_nv - nonvariadic wrapper for text_format function.
 */
pub unsafe fn text_format_nv(fcinfo: FunctionCallInfo) -> Datum {
    text_format(fcinfo)
}

/*
 * The following *ClosestMatch() functions can be used to determine whether a
 * user-provided string resembles any known valid values.
 */

/*
 * Initialize the given state with the source string and maximum Levenshtein
 * distance to consider.
 */
pub unsafe fn initClosestMatch(state: *mut ClosestMatchState, source: *const c_char, max_d: c_int) {
    debug_assert!(!state.is_null());
    debug_assert!(max_d >= 0);

    (*state).source = source;
    (*state).min_d = -1;
    (*state).max_d = max_d;
    (*state).match_ = core::ptr::null();
}

/*
 * If the candidate string is a closer match than the current one saved (or
 * there is no match saved), save it as the closest match.
 */
pub unsafe fn updateClosestMatch(state: *mut ClosestMatchState, candidate: *const c_char) {
    let dist: c_int;

    debug_assert!(!state.is_null());

    if (*state).source.is_null()
        || *(*state).source == b'\0' as c_char
        || candidate.is_null()
        || *candidate == b'\0' as c_char
    {
        return;
    }

    /*
     * To avoid ERROR-ing, we check the lengths here instead of setting
     * 'trusted' to false in the call to varstr_levenshtein_less_equal().
     */
    if strlen((*state).source) as c_int > MAX_LEVENSHTEIN_STRLEN
        || strlen(candidate) as c_int > MAX_LEVENSHTEIN_STRLEN
    {
        return;
    }

    dist = varstr_levenshtein_less_equal(
        (*state).source,
        strlen((*state).source) as c_int,
        candidate,
        strlen(candidate) as c_int,
        1,
        1,
        1,
        (*state).max_d,
        true,
    );
    if dist <= (*state).max_d
        && dist <= strlen((*state).source) as c_int / 2
        && ((*state).min_d == -1 || dist < (*state).min_d)
    {
        (*state).min_d = dist;
        (*state).match_ = candidate;
    }
}

/*
 * Return the closest match.  If no suitable candidates were provided via
 * updateClosestMatch(), return NULL.
 */
pub unsafe fn getClosestMatch(state: *mut ClosestMatchState) -> *const c_char {
    debug_assert!(!state.is_null());

    (*state).match_
}

/*
 * Unicode support
 */

unsafe fn unicode_norm_form_from_string(formstr: *const c_char) -> UnicodeNormalizationForm {
    let form: UnicodeNormalizationForm;

    /*
     * Might as well check this while we're here.
     */
    if GetDatabaseEncoding() != PG_UTF8 as c_int {
        let _ = errcode(ERRCODE_SYNTAX_ERROR);
        ereport!(
            ERROR,
            errmsg!("Unicode normalization can only be performed if server encoding is UTF8")
        );
    }

    if pg_strcasecmp(formstr, c"NFC".as_ptr()) == 0 {
        form = UNICODE_NFC;
    } else if pg_strcasecmp(formstr, c"NFD".as_ptr()) == 0 {
        form = UNICODE_NFD;
    } else if pg_strcasecmp(formstr, c"NFKC".as_ptr()) == 0 {
        form = UNICODE_NFKC;
    } else if pg_strcasecmp(formstr, c"NFKD".as_ptr()) == 0 {
        form = UNICODE_NFKD;
    } else {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!(
                "invalid normalization form: {}",
                std::ffi::CStr::from_ptr(formstr).to_string_lossy()
            )
        );
        unreachable!();
    }

    form
}

/*
 * Check whether the string contains only assigned Unicode code points.
 */
pub unsafe fn unicode_assigned(fcinfo: FunctionCallInfo) -> Datum {
    let input: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let mut p: *mut u8;
    let size: c_int;

    if GetDatabaseEncoding() != PG_UTF8 as c_int {
        ereport!(
            ERROR,
            errmsg!("Unicode categorization can only be performed if server encoding is UTF8")
        );
    }

    /* convert to pg_wchar */
    size = pg_mbstrlen_with_len(VARDATA_ANY(input as *const c_char), VARSIZE_ANY_EXHDR(input as *const c_char) as c_int);
    p = VARDATA_ANY(input as *const c_char) as *mut u8;
    for _i in 0..size {
        let uchar: pg_wchar = utf8_to_unicode(p);
        let category = unicode_category(uchar);

        if category == PG_U_UNASSIGNED {
            PG_RETURN_BOOL!(false);
        }

        p = p.add(pg_utf_mblen(p) as usize);
    }

    PG_RETURN_BOOL!(true);
}

pub unsafe fn unicode_normalize_func(fcinfo: FunctionCallInfo) -> Datum {
    let input: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let formstr: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1));
    let form: UnicodeNormalizationForm;
    let mut size: c_int;
    let input_chars: *mut pg_wchar;
    let output_chars: *mut pg_wchar;
    let mut p: *mut u8;
    let result: *mut text;
    let mut i: c_int;

    form = unicode_norm_form_from_string(formstr);

    /* convert to pg_wchar */
    size = pg_mbstrlen_with_len(VARDATA_ANY(input as *const c_char), VARSIZE_ANY_EXHDR(input as *const c_char) as c_int);
    input_chars = palloc(((size + 1) as Size) * core::mem::size_of::<pg_wchar>() as Size) as *mut pg_wchar;
    p = VARDATA_ANY(input as *const c_char) as *mut u8;
    i = 0;
    while i < size {
        *input_chars.add(i as usize) = utf8_to_unicode(p);
        p = p.add(pg_utf_mblen(p) as usize);
        i += 1;
    }
    *input_chars.add(i as usize) = b'\0' as pg_wchar;

    /* action */
    output_chars = unicode_normalize(form, input_chars);

    /* convert back to UTF-8 string */
    size = 0;
    let mut wp: *mut pg_wchar = output_chars;
    while *wp != 0 {
        let mut buf: [u8; 4] = [0; 4];
        unicode_to_utf8(*wp, buf.as_mut_ptr());
        size += pg_utf_mblen(buf.as_ptr());
        wp = wp.add(1);
    }

    result = palloc((size + VARHDRSZ) as Size) as *mut text;
    SET_VARSIZE(result as *mut c_char, size + VARHDRSZ);

    p = VARDATA_ANY(result as *const c_char) as *mut u8;
    wp = output_chars;
    while *wp != 0 {
        unicode_to_utf8(*wp, p);
        p = p.add(pg_utf_mblen(p) as usize);
        wp = wp.add(1);
    }

    PG_RETURN_TEXT_P!(result);
}

/*
 * Check whether the string is in the specified Unicode normalization form.
 */
pub unsafe fn unicode_is_normalized(fcinfo: FunctionCallInfo) -> Datum {
    let input: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let formstr: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1));
    let form: UnicodeNormalizationForm;
    let size: c_int;
    let input_chars: *mut pg_wchar;
    let output_chars: *mut pg_wchar;
    let mut p: *mut u8;
    let mut i: c_int;
    let quickcheck: UnicodeNormalizationQC;
    let mut output_size: c_int;
    let result: bool;

    form = unicode_norm_form_from_string(formstr);

    /* convert to pg_wchar */
    size = pg_mbstrlen_with_len(VARDATA_ANY(input as *const c_char), VARSIZE_ANY_EXHDR(input as *const c_char) as c_int);
    input_chars = palloc(((size + 1) as Size) * core::mem::size_of::<pg_wchar>() as Size) as *mut pg_wchar;
    p = VARDATA_ANY(input as *const c_char) as *mut u8;
    i = 0;
    while i < size {
        *input_chars.add(i as usize) = utf8_to_unicode(p);
        p = p.add(pg_utf_mblen(p) as usize);
        i += 1;
    }
    *input_chars.add(i as usize) = b'\0' as pg_wchar;

    /* quick check (see UAX #15) */
    quickcheck = unicode_is_normalized_quickcheck(form, input_chars);
    if quickcheck == UNICODE_NORM_QC_YES {
        PG_RETURN_BOOL!(true);
    } else if quickcheck == UNICODE_NORM_QC_NO {
        PG_RETURN_BOOL!(false);
    }

    /* normalize and compare with original */
    output_chars = unicode_normalize(form, input_chars);

    output_size = 0;
    let mut wp: *mut pg_wchar = output_chars;
    while *wp != 0 {
        output_size += 1;
        wp = wp.add(1);
    }

    result = (size == output_size)
        && (varlena_memcmp(
            input_chars as *const c_void,
            output_chars as *const c_void,
            (size as usize) * core::mem::size_of::<pg_wchar>(),
        ) == 0);

    PG_RETURN_BOOL!(result);
}

/*
 * Replaces Unicode escape sequences by Unicode characters
 */
pub unsafe fn unistr(fcinfo: FunctionCallInfo) -> Datum {
    let input_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let mut instr: *mut c_char;
    let mut len: c_int;
    let mut str: StringInfoData = core::mem::zeroed();
    let result: *mut text;
    let mut pair_first: pg_wchar = 0;
    let mut cbuf: [c_char; (MAX_UNICODE_EQUIVALENT_STRING + 1) as usize] =
        [0; (MAX_UNICODE_EQUIVALENT_STRING + 1) as usize];

    instr = VARDATA_ANY(input_text as *const c_char) as *mut c_char;
    len = VARSIZE_ANY_EXHDR(input_text as *const c_char) as c_int;

    initStringInfo(&mut str);

    /* C uses `goto invalid_pair` to raise the error from anywhere below. */
    macro_rules! invalid_pair {
        () => {{
            let _ = errcode(ERRCODE_SYNTAX_ERROR);
            ereport!(ERROR, errmsg!("invalid Unicode surrogate pair"));
            #[allow(unreachable_code)]
            { unreachable!() }
        }};
    }

    loop {
        if len <= 0 {
            break;
        }
        if *instr == b'\\' as c_char {
            if len >= 2 && *instr.add(1) == b'\\' as c_char {
                if pair_first != 0 {
                    invalid_pair!();
                }
                appendStringInfoChar(&mut str, b'\\' as c_char);
                instr = instr.add(2);
                len -= 2;
            } else if (len >= 5 && isxdigits_n(instr.add(1), 4))
                || (len >= 6 && *instr.add(1) == b'u' as c_char && isxdigits_n(instr.add(2), 4))
            {
                let mut unicode: pg_wchar;
                let offset: c_int = if *instr.add(1) == b'u' as c_char { 2 } else { 1 };

                unicode = hexval_n(instr.add(offset as usize), 4) as pg_wchar;

                if !is_valid_unicode_codepoint(unicode) {
                    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                    ereport!(ERROR, errmsg!("invalid Unicode code point: {:04X}", unicode));
                }

                if pair_first != 0 {
                    if is_utf16_surrogate_second(unicode) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        invalid_pair!();
                    }
                } else if is_utf16_surrogate_second(unicode) {
                    invalid_pair!();
                }

                if is_utf16_surrogate_first(unicode) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, cbuf.as_mut_ptr() as *mut u8);
                    appendStringInfoString(&mut str, cbuf.as_ptr());
                }

                instr = instr.add((4 + offset) as usize);
                len -= 4 + offset;
            } else if len >= 8 && *instr.add(1) == b'+' as c_char && isxdigits_n(instr.add(2), 6) {
                let mut unicode: pg_wchar;

                unicode = hexval_n(instr.add(2), 6) as pg_wchar;

                if !is_valid_unicode_codepoint(unicode) {
                    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                    ereport!(ERROR, errmsg!("invalid Unicode code point: {:04X}", unicode));
                }

                if pair_first != 0 {
                    if is_utf16_surrogate_second(unicode) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        invalid_pair!();
                    }
                } else if is_utf16_surrogate_second(unicode) {
                    invalid_pair!();
                }

                if is_utf16_surrogate_first(unicode) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, cbuf.as_mut_ptr() as *mut u8);
                    appendStringInfoString(&mut str, cbuf.as_ptr());
                }

                instr = instr.add(8);
                len -= 8;
            } else if len >= 10 && *instr.add(1) == b'U' as c_char && isxdigits_n(instr.add(2), 8) {
                let mut unicode: pg_wchar;

                unicode = hexval_n(instr.add(2), 8) as pg_wchar;

                if !is_valid_unicode_codepoint(unicode) {
                    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                    ereport!(ERROR, errmsg!("invalid Unicode code point: {:04X}", unicode));
                }

                if pair_first != 0 {
                    if is_utf16_surrogate_second(unicode) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        invalid_pair!();
                    }
                } else if is_utf16_surrogate_second(unicode) {
                    invalid_pair!();
                }

                if is_utf16_surrogate_first(unicode) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, cbuf.as_mut_ptr() as *mut u8);
                    appendStringInfoString(&mut str, cbuf.as_ptr());
                }

                instr = instr.add(10);
                len -= 10;
            } else {
                let _ = errcode(ERRCODE_SYNTAX_ERROR);
                // C also: errhint("Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.")
                ereport!(ERROR, errmsg!("invalid Unicode escape"));
            }
        } else {
            if pair_first != 0 {
                invalid_pair!();
            }

            appendStringInfoChar(&mut str, *instr);
            instr = instr.add(1);
            len -= 1;
        }
    }

    /* unfinished surrogate pair? */
    if pair_first != 0 {
        let _ = errcode(ERRCODE_SYNTAX_ERROR);
        ereport!(ERROR, errmsg!("invalid Unicode surrogate pair"));
    }

    result = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data as *mut c_void);

    PG_RETURN_TEXT_P!(result);
}
