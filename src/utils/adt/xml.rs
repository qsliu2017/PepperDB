//! Translation of postgres/src/backend/utils/adt/xml.c
//!
//! XML data type support.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/utils/adt/xml.c
//!
//! NOTE: USE_LIBXML is NOT defined in this port.  All !USE_LIBXML branches
//! (ereport feature-not-supported paths) are translated as the live code.
//! All code that is only reachable when USE_LIBXML is defined is placed under
//! #[cfg(any())] (never compiled) with the original C preserved in comments.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_ARGISNULL, PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32,
    PG_GETARG_NAME, PG_GETARG_OID, PG_GETARG_POINTER, PG_GETARG_TEXT_PP,
    PG_RETURN_BOOL, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_DATUM, PG_RETURN_NULL,
    PG_RETURN_TEXT_P,
};
use crate::{appendStringInfo, PG_GETARG_DATUM, PG_RETURN_POINTER, PG_DETOAST_DATUM};
use crate::c::{int16, int32, int64, uint32};
use crate::prelude::{c_char, c_int, c_void, c_uchar};
use crate::lib::stringinfo::{
    StringInfo, StringInfoData, appendStringInfoString, appendStringInfoChar,
    appendBinaryStringInfo, initStringInfo, makeStringInfo,
};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbytes, pq_sendtext,
};
use crate::mb::mbutils::{
    GetDatabaseEncoding, pg_any_to_server, pg_do_encoding_conversion,
    pg_get_client_encoding, pg_mblen_cstr, pg_server_to_any, pg_unicode_to_server,
};
use crate::mb::pg_wchar::{
    pg_char_to_encoding, pg_encoding_to_char, pg_enc, pg_wchar, PG_UTF8,
    MAX_MULTIBYTE_CHAR_LEN, MAX_UNICODE_EQUIVALENT_STRING,
};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::execnodes::{TableFuncRoutine, TableFuncScanState, ErrorSaveContext};
use crate::nodes::primnodes::{XmlExpr, XmlOptionType, XMLOPTION_DOCUMENT, XMLOPTION_CONTENT};
use crate::nodes::pg_list::{List, NIL};
use crate::pgtime::pg_tm;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, DatumGetBool, DatumGetObjectId, Float8GetDatum,
    ObjectIdGetDatum, PointerGetDatum,
};
use crate::utils::adt::date::{
    DateADT, DatumGetDateADT, Timestamp, fsec_t, DATE_NOT_FINITE, POSTGRES_EPOCH_JDATE,
    TIMESTAMP_NOT_FINITE, MAXDATELEN,
};
use crate::utils::builtins::{
    cstring_to_text, cstring_to_text_with_len, text_to_cstring, TextDatumGetCString,
};
use crate::utils::cache::lsyscache::{
    get_namespace_name, get_rel_name, getBaseType, getBaseTypeAndTypmod,
    get_typtype, get_type_category_preferred,
};
use crate::catalog::objectaddress_impl::get_database_name;
use crate::utils::fmgr::{InputFunctionCall, OidOutputFunctionCall};
use crate::access::common::tupdesc::{CreateTupleDescCopy, TupleDescAttr, TupleDesc};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::namespace::LookupExplicitNamespace;
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::miscadmin::{MyDatabaseId, USE_XSD_DATES};
use crate::varatt::{VARDATA, VARDATA_ANY, VARHDRSZ, VARSIZE, VARSIZE_ANY_EXHDR, SET_VARSIZE};
use crate::utils::array::{ArrayType, ARR_NDIM, ARR_DIMS, ARR_ELEMTYPE};
use core::ffi::{c_char as ffi_c_char};

// -------------------------------------------------------------------------
// fmgr macros for the xml type (mirror postgres/src/include/utils/xml.h).
// xmltype is a varlena, so DatumGetXmlP detoasts like text/bytea.
// Defined locally (not #[macro_export]) per port conventions.
// -------------------------------------------------------------------------

/// DatumGetXmlP(X) == (xmltype *) PG_DETOAST_DATUM(X)
macro_rules! DatumGetXmlP {
    ($X:expr) => {
        PG_DETOAST_DATUM!($X) as *mut xmltype
    };
}
/// PG_GETARG_XML_P(n) == DatumGetXmlP(PG_GETARG_DATUM(n))
macro_rules! PG_GETARG_XML_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetXmlP!(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/// PG_RETURN_XML_P(x) == PG_RETURN_POINTER(x)
macro_rules! PG_RETURN_XML_P {
    ($x:expr) => {
        PG_RETURN_POINTER!($x)
    };
}
/// PG_GETARG_ARRAYTYPE_P(n) == (ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n))
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        PG_DETOAST_DATUM!(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}
/// DatumGetCString -- cast a Datum (returned by a cstring-output function) to *mut c_char.
macro_rules! DatumGetCString {
    ($e:expr) => { $e as *mut c_char }
}

// -------------------------------------------------------------------------
// Opaque xmltype = text (same representation)
// -------------------------------------------------------------------------

/// xmltype has the same on-disk representation as text.
pub type xmltype = crate::c::text;

// -------------------------------------------------------------------------
// GUC variables (extern in C; mutable statics here)
// -------------------------------------------------------------------------

/// xmlbinary GUC: how to encode binary data in XML.
pub static mut xmlbinary: c_int = XMLBINARY_BASE64;
/// xmloption GUC: XMLOPTION_DOCUMENT or XMLOPTION_CONTENT.
pub static mut xmloption: c_int = XMLOPTION_CONTENT as c_int;

/// Convert the `xmloption` GUC (stored as int, per C) to XmlOptionType.
/// In C the int is passed directly to XmlOptionType parameters.
#[inline]
fn xmloption_as_type(v: c_int) -> XmlOptionType {
    if v == XMLOPTION_DOCUMENT as c_int { XMLOPTION_DOCUMENT } else { XMLOPTION_CONTENT }
}

pub const XMLBINARY_BASE64: c_int = 0;
pub const XMLBINARY_HEX: c_int = 1;

/* XML_STANDALONE_* constants mirror C enum XmlStandaloneType */
pub const XML_STANDALONE_YES: c_int = 1;
pub const XML_STANDALONE_NO: c_int = 2;
pub const XML_STANDALONE_NO_VALUE: c_int = 3;
pub const XML_STANDALONE_OMITTED: c_int = 4;

/* SQL/XML namespace constants */
pub const NAMESPACE_XSD: &str = "http://www.w3.org/2001/XMLSchema";
pub const NAMESPACE_XSI: &str = "http://www.w3.org/2001/XMLSchema-instance";
pub const NAMESPACE_SQLXML: &str = "http://standards.iso.org/iso/9075/2003/sqlxml";

/// Default XML declaration version string.
pub const PG_XML_DEFAULT_VERSION: &[u8] = b"1.0\0";

/* PgXmlStrictness values */
pub const PG_XML_STRICTNESS_LEGACY: c_int = 1;
pub const PG_XML_STRICTNESS_WELLFORMED: c_int = 2;
pub const PG_XML_STRICTNESS_ALL: c_int = 3;

/* errcode stubs -- not yet ported; 0 used as placeholder */
pub const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
pub const ERRCODE_INVALID_XML_COMMENT: c_int = 0;
pub const ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION: c_int = 0;
pub const ERRCODE_NOT_AN_XML_DOCUMENT: c_int = 0;
pub const ERRCODE_INVALID_XML_DOCUMENT: c_int = 0;
pub const ERRCODE_INVALID_XML_CONTENT: c_int = 0;
pub const ERRCODE_OUT_OF_MEMORY: c_int = 0;
pub const ERRCODE_INTERNAL_ERROR: c_int = 0;
pub const ERRCODE_DATA_EXCEPTION: c_int = 0;
pub const ERRCODE_UNDEFINED_CURSOR: c_int = 0;
pub const ERRCODE_INVALID_CURSOR_STATE: c_int = 0;
pub const ERRCODE_CARDINALITY_VIOLATION: c_int = 0;
pub const ERRCODE_INVALID_ARGUMENT_FOR_XQUERY: c_int = 0;
pub const ERRCODE_NULL_VALUE_NOT_ALLOWED: c_int = 0;
pub const ERRCODE_DATETIME_VALUE_OUT_OF_RANGE: c_int = 0;

/* Catalog OID constants referenced below */
pub const BOOLOID: Oid = 16;
pub const BYTEAOID: Oid = 17;
pub const INT2OID: Oid = 21;
pub const INT4OID: Oid = 23;
pub const INT8OID: Oid = 20;
pub const FLOAT4OID: Oid = 700;
pub const FLOAT8OID: Oid = 701;
pub const NUMERICOID: Oid = 1700;
pub const TEXTOID: Oid = 25;
pub const BPCHAROID: Oid = 1042;
pub const VARCHAROID: Oid = 1043;
pub const DATEOID: Oid = 1082;
pub const TIMEOID: Oid = 1083;
pub const TIMETZOID: Oid = 1266;
pub const TIMESTAMPOID: Oid = 1114;
pub const TIMESTAMPTZOID: Oid = 1184;
pub const XMLOID: Oid = 142;
pub const CSTRINGOID: Oid = 2275;

/* catalog type-type constants */
pub const TYPTYPE_DOMAIN: c_char = 'd' as c_char;

/* TYPCATEGORY_NUMERIC (from catalog/pg_type.h) */
pub const TYPCATEGORY_NUMERIC: c_char = 'N' as c_char;

/* SPI return codes */
pub const SPI_OK_SELECT: c_int = 5;

/* lock mode */
pub const AccessShareLock: c_int = 1;
pub const NoLock: c_int = 0;

/* RELOID / TYPEOID syscache identifiers */
pub const RELOID: c_int = 43;
pub const TYPEOID: c_int = 44;

/// Macro: NO_XML_SUPPORT -- raises feature-not-supported when USE_LIBXML is off.
macro_rules! NO_XML_SUPPORT {
    () => {
        ereport!(
            ERROR,
            errmsg!("unsupported XML feature")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("This functionality requires the server to be built with libxml support.") */
        )
    };
}

// -------------------------------------------------------------------------
// Stub types for unported dependencies
// -------------------------------------------------------------------------

/// TODO(pg-port): real PgXmlErrorContext lives in xml.c (libxml gated).
pub struct PgXmlErrorContext {
    pub magic: c_int,
    pub strictness: c_int,
    pub err_occurred: bool,
    pub err_buf: StringInfoData,
}

/* TODO(pg-port): Portal -- real type in utils/portal.h */
pub type Portal = *mut c_void;
/* TODO(pg-port): SPIPlanPtr */
pub type SPIPlanPtr = *mut c_void;
/* TODO(pg-port): ArrayBuildState */
pub type ArrayBuildState = *mut c_void;
/* TODO(pg-port): Form_pg_class */
pub type Form_pg_class = *mut c_void;
/* TODO(pg-port): Form_pg_type */
pub type Form_pg_type = *mut c_void;
/* Form_pg_attribute comes from access::common::tupdesc (real catalog type). */

// -------------------------------------------------------------------------
// Stubs for unported functions called from within xml.rs
// -------------------------------------------------------------------------

/// TODO(pg-port): SPI_connect -- executor/spi.h
pub unsafe fn SPI_connect() -> c_int { 0 }
/// TODO(pg-port): SPI_finish -- executor/spi.h
pub unsafe fn SPI_finish() -> c_int { 0 }
/// TODO(pg-port): SPI_execute -- executor/spi.h
pub unsafe fn SPI_execute(_query: *const c_char, _read_only: bool, _count: i64) -> c_int { 0 }
/// TODO(pg-port): SPI_prepare -- executor/spi.h
pub unsafe fn SPI_prepare(_query: *const c_char, _nargs: c_int, _argtypes: *mut Oid) -> SPIPlanPtr { core::ptr::null_mut() }
/// TODO(pg-port): SPI_cursor_open -- executor/spi.h
pub unsafe fn SPI_cursor_open(_name: *const c_char, _plan: SPIPlanPtr, _values: *mut Datum, _nulls: *mut c_char, _read_only: bool) -> Portal { core::ptr::null_mut() }
/// TODO(pg-port): SPI_cursor_find -- executor/spi.h
pub unsafe fn SPI_cursor_find(_name: *const c_char) -> Portal { core::ptr::null_mut() }
/// TODO(pg-port): SPI_cursor_fetch -- executor/spi.h
pub unsafe fn SPI_cursor_fetch(_portal: Portal, _forward: bool, _count: i64) {}
/// TODO(pg-port): SPI_cursor_close -- executor/spi.h
pub unsafe fn SPI_cursor_close(_portal: Portal) {}
/// TODO(pg-port): SPI_processed -- executor/spi.h
pub static mut SPI_processed: u64 = 0;
/// TODO(pg-port): SPI_tuptable -- executor/spi.h
pub static mut SPI_tuptable: *mut SPITupleTable = core::ptr::null_mut();
/// TODO(pg-port): SPI_result_code_string -- executor/spi.h
pub unsafe fn SPI_result_code_string(_code: c_int) -> *const c_char { b"\0".as_ptr() as *const c_char }
/// TODO(pg-port): SPI_palloc -- executor/spi.h
pub unsafe fn SPI_palloc(size: usize) -> *mut c_void { palloc(size) }
/// TODO(pg-port): SPI_getbinval -- executor/spi.h
pub unsafe fn SPI_getbinval(_tup: *mut c_void, _tupdesc: TupleDesc, _fnumber: c_int, _isnull: *mut bool) -> Datum { 0 }
/// TODO(pg-port): SPI_fname -- executor/spi.h
pub unsafe fn SPI_fname(_tupdesc: TupleDesc, _fnumber: c_int) -> *mut c_char { core::ptr::null_mut() }
/// TODO(pg-port): SPI_gettypeid -- executor/spi.h
pub unsafe fn SPI_gettypeid(_tupdesc: TupleDesc, _fnumber: c_int) -> Oid { 0 }
/// TODO(pg-port): SPITupleTable
pub struct SPITupleTable {
    pub tupdesc: TupleDesc,
    pub vals: *mut *mut c_void,
}
/// TODO(pg-port): PortalData
pub struct PortalData {
    pub tupDesc: TupleDesc,
}

/// TODO(pg-port): SearchSysCache1 -- utils/syscache.h
pub unsafe fn SearchSysCache1(_cache_id: c_int, _key1: Datum) -> HeapTuple { core::ptr::null_mut() }
/// TODO(pg-port): ReleaseSysCache -- utils/syscache.h
pub unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}
/// TODO(pg-port): type_is_array_domain -- utils/lsyscache.h
pub unsafe fn type_is_array_domain(_typid: Oid) -> bool { false }
/// TODO(pg-port): getTypeOutputInfo -- utils/lsyscache.h (full version)
pub unsafe fn getTypeOutputInfo(_typid: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {}
/// TODO(pg-port): DatumGetArrayTypeP -- utils/array.h
pub unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType { core::ptr::null_mut() }
/// TODO(pg-port): deconstruct_array -- utils/array.h
pub unsafe fn deconstruct_array(_array: *mut ArrayType, _elmtype: Oid, _elmlen: int16, _elmbyval: bool, _elmalign: c_char, _elemsp: *mut *mut Datum, _nullsp: *mut *mut bool, _nelemsp: *mut c_int) {}
/// TODO(pg-port): deconstruct_array_builtin -- utils/array.h
pub unsafe fn deconstruct_array_builtin(_array: *mut ArrayType, _elmtype: Oid, _elemsp: *mut *mut Datum, _nullsp: *mut *mut bool, _nelemsp: *mut c_int) {}
/// TODO(pg-port): initArrayResult -- utils/array.h
pub unsafe fn initArrayResult(_element_type: Oid, _rcontext: *mut c_void, _subcontext: bool) -> ArrayBuildState { core::ptr::null_mut() }
/// TODO(pg-port): accumArrayResult -- utils/array.h
pub unsafe fn accumArrayResult(_astate: ArrayBuildState, _dvalue: Datum, _disnull: bool, _element_type: Oid, _rcontext: *mut c_void) -> ArrayBuildState { core::ptr::null_mut() }
/// TODO(pg-port): makeArrayResult -- utils/array.h
pub unsafe fn makeArrayResult(_astate: ArrayBuildState, _rcontext: *mut c_void) -> Datum { 0 }
/// TODO(pg-port): CurrentMemoryContext -- utils/palloc.h
pub unsafe fn CurrentMemoryContext() -> *mut c_void { core::ptr::null_mut() }
/// TODO(pg-port): list_make1 -- nodes/pg_list.h
pub unsafe fn list_make1(_a: *mut c_void) -> *mut List { core::ptr::null_mut() }
/// TODO(pg-port): list_make2 -- nodes/pg_list.h
pub unsafe fn list_make2(_a: *mut c_void, _b: *mut c_void) -> *mut List { core::ptr::null_mut() }
/// TODO(pg-port): lappend -- nodes/pg_list.h
pub unsafe fn lappend(list: *mut List, _datum: *mut c_void) -> *mut List { list }
/// TODO(pg-port): lappend_oid -- nodes/pg_list.h
pub unsafe fn lappend_oid(list: *mut List, _datum: Oid) -> *mut List { list }
/// TODO(pg-port): list_append_unique_oid -- nodes/pg_list.h
pub unsafe fn list_append_unique_oid(list: *mut List, _datum: Oid) -> *mut List { list }
/// TODO(pg-port): lfirst_oid -- nodes/pg_list.h
pub unsafe fn lfirst_oid(_lc: *mut c_void) -> Oid { 0 }
/// TODO(pg-port): lfirst -- nodes/pg_list.h
pub unsafe fn lfirst(_lc: *mut c_void) -> *mut c_void { core::ptr::null_mut() }
/// TODO(pg-port): DatumGetTimestamp -- utils/timestamp.h
pub unsafe fn DatumGetTimestamp(d: Datum) -> Timestamp { d as Timestamp }
/// TODO(pg-port): j2date -- utils/datetime.h
pub unsafe fn j2date(_jd: c_int, _year: *mut c_int, _month: *mut c_int, _day: *mut c_int) {}
/// TODO(pg-port): EncodeDateOnly -- utils/datetime.h
pub unsafe fn EncodeDateOnly(_tm: *mut pg_tm, _style: c_int, _str: *mut c_char) {}
/// TODO(pg-port): EncodeDateTime -- utils/datetime.h
pub unsafe fn EncodeDateTime(_tm: *mut pg_tm, _fsec: fsec_t, _print_tz: bool, _tz: c_int, _tzn: *const c_char, _style: c_int, _str: *mut c_char) {}
/// TODO(pg-port): timestamp2tm -- utils/timestamp.h
pub unsafe fn timestamp2tm(_dt: Timestamp, _tzp: *mut c_int, _tm: *mut pg_tm, _fsec: *mut fsec_t, _tzn: *mut *const c_char, _attimezone: *mut c_void) -> c_int { 0 }
/// TODO(pg-port): DatumGetByteaPP -- utils/builtins.h
pub unsafe fn DatumGetByteaPP(d: Datum) -> *mut crate::c::bytea { d as *mut crate::c::bytea }
/// TODO(pg-port): regclassout -- utils/adt/regproc.c
pub unsafe fn regclassout(_fcinfo: FunctionCallInfo) -> Datum { 0 }
/// TODO(pg-port): DirectFunctionCall1 -- fmgr.h
pub unsafe fn DirectFunctionCall1(_func: unsafe fn(FunctionCallInfo) -> Datum, _arg1: Datum) -> Datum { 0 }
/// TODO(pg-port): errsave -- utils/elog.h
pub unsafe fn errsave(_escontext: *mut Node, _code: c_int, _msg: *const c_char) {}
/// TODO(pg-port): pg_encoding_mb2wchar_with_len -- mb/pg_wchar.h
pub unsafe fn pg_encoding_mb2wchar_with_len(_enc: c_int, _from: *const c_char, _to: *mut pg_wchar, _len: c_int) -> c_int { 0 }
/// TODO(pg-port): pg_encoding_mblen -- mb/pg_wchar.h
pub unsafe fn pg_encoding_mblen(_enc: c_int, _mbstr: *const c_char) -> c_int { 1 }
/// TODO(pg-port): OidIsValid
pub fn OidIsValid(oid: Oid) -> bool { oid != 0 }
/// TODO(pg-port): InvalidOid
pub const InvalidOid: Oid = 0;
/// NameStr for a pointer to a NameData field (real crate::c::NameStr takes &NameData).
pub unsafe fn NameStr_ptr(n: *const crate::c::NameData) -> *const c_char { (*n).data.as_ptr() }

/// TODO(pg-port): exprType -- nodes/nodeFuncs.h
pub unsafe fn exprType(_node: *const Node) -> Oid { 0 }
/// TODO(pg-port): map_sql_value_to_xml_value (forward-declared; defined later in this file)


// =========================================================================
// PART 2 -- Core I/O functions: xml_in, xml_out, xml_recv, xml_send,
//           helper conversions, xmlcomment, xmltext, xmlconcat*, xmlparse,
//           xmlpi, xmlroot, xmlvalidate, xml_is_document
// =========================================================================

/*
 * xml_in uses a plain C string to VARDATA conversion, so for the time being
 * we use the conversion function for the text datatype.
 *
 * This is only acceptable so long as xmltype and text use the same
 * representation.
 */
/// xml_in -- input function for the xml type.
pub unsafe fn xml_in(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let s: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
        // vardata = cstring_to_text(s);
        // doc = xml_parse(vardata, xmloption, true, GetDatabaseEncoding(), NULL, NULL, fcinfo->context);
        // if doc != NULL: xmlFreeDoc(doc);
        // PG_RETURN_XML_P!(vardata)
    }
    */
}

/// xml_out_internal -- shared by xml_out and xml_send.
/// When USE_LIBXML is not defined, just returns text_to_cstring cast.
unsafe fn xml_out_internal(x: *mut xmltype, _target_encoding: pg_enc) -> *mut c_char {
    /* When USE_LIBXML is off we skip declaration rewriting and just return the raw string. */
    let str_: *mut c_char = text_to_cstring(x as *const crate::c::text);
    /* #[cfg(any())] USE_LIBXML body would call parse_xml_decl / print_xml_decl here */
    str_
}

/// xml_out -- output function for the xml type.
pub unsafe fn xml_out(fcinfo: FunctionCallInfo) -> Datum {
    let x: *mut xmltype = PG_GETARG_XML_P!(fcinfo, 0);
    /*
     * xml_out removes the encoding property in all cases.  This is because we
     * cannot control from here whether the datum will be converted to a
     * different client encoding, so we'd do more harm than good by including
     * it.
     */
    PG_RETURN_CSTRING!(xml_out_internal(x, 0))
}

/// xml_recv -- binary input function.
pub unsafe fn xml_recv(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
        // nbytes = buf->len - buf->cursor;
        // str = pq_getmsgbytes(buf, nbytes);
        // result = palloc(nbytes + 1 + VARHDRSZ);
        // SET_VARSIZE(result, nbytes + VARHDRSZ);
        // memcpy(VARDATA(result), str, nbytes);
        // str = VARDATA(result); str[nbytes] = '\0';
        // parse_xml_decl((const xmlChar*)str, NULL, NULL, &encodingStr, NULL);
        // encoding = if encodingStr { xmlChar_to_encoding(encodingStr) } else { PG_UTF8 };
        // doc = xml_parse(result, xmloption, true, encoding, NULL, NULL, NULL);
        // xmlFreeDoc(doc);
        // newstr = pg_any_to_server(str, nbytes, encoding);
        // if newstr != str { pfree(result); result = cstring_to_text(newstr); pfree(newstr); }
        // PG_RETURN_XML_P!(result)
    }
    */
}

/// xml_send -- binary output function.
pub unsafe fn xml_send(fcinfo: FunctionCallInfo) -> Datum {
    let x: *mut xmltype = PG_GETARG_XML_P!(fcinfo, 0);
    let outval: *mut c_char;
    let mut buf: StringInfoData = core::mem::zeroed();

    /*
     * xml_out_internal doesn't convert the encoding, it just prints the right
     * declaration. pq_sendtext will do the conversion.
     */
    outval = xml_out_internal(x, pg_get_client_encoding());

    pq_begintypsend(&mut buf);
    pq_sendtext(&mut buf, outval, libc_strlen(outval) as c_int);
    pfree(outval as *mut c_void);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf))
}

/* strlen via C stdlib binding */
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 { n += 1; }
    n
}

// -------------------------------------------------------------------------
// String helper conversions (not USE_LIBXML gated)
// -------------------------------------------------------------------------

/// stringinfo_to_xmltype -- wrap a StringInfo as xmltype.
pub unsafe fn stringinfo_to_xmltype(buf: StringInfo) -> *mut xmltype {
    cstring_to_text_with_len((*buf).data, (*buf).len) as *mut xmltype
}

/// cstring_to_xmltype -- wrap a C string as xmltype.
pub unsafe fn cstring_to_xmltype(string: *const c_char) -> *mut xmltype {
    cstring_to_text(string) as *mut xmltype
}

// #[cfg(any())] USE_LIBXML helper:
// static unsafe fn xmlBuffer_to_xmltype(buf: xmlBufferPtr) -> *mut xmltype {
//     cstring_to_text_with_len(xmlBufferContent(buf) as *const c_char, xmlBufferLength(buf)) as *mut xmltype
// }

// -------------------------------------------------------------------------
// xmlcomment
// -------------------------------------------------------------------------

/// xmlcomment(text) -> xml
pub unsafe fn xmlcomment(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let arg: *mut crate::c::text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let argdata: *const c_char = VARDATA_ANY(arg as *const c_char) as *const c_char;
        let len: c_int = VARSIZE_ANY_EXHDR(arg as *const c_char) as c_int;
        let mut buf: StringInfoData = core::mem::zeroed();
        // check for "--" or trailing "-"
        // initStringInfo(&mut buf);
        // appendStringInfoString(&mut buf, c"<!--".as_ptr());
        // appendStringInfoText(&mut buf, arg);
        // appendStringInfoString(&mut buf, c"-->".as_ptr());
        // PG_RETURN_XML_P!(stringinfo_to_xmltype(&mut buf))
    }
    */
}

// -------------------------------------------------------------------------
// xmltext
// -------------------------------------------------------------------------

/// xmltext(text) -> xml  (encodes special chars)
pub unsafe fn xmltext(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let arg: *mut crate::c::text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        // xmlbuf = xmlEncodeSpecialChars(NULL, xml_text2xmlChar(arg));
        // result = cstring_to_text_with_len(xmlbuf as *const c_char, xmlStrlen(xmlbuf));
        // xmlFree(xmlbuf);
        // PG_RETURN_XML_P!(result)
    }
    */
}

// -------------------------------------------------------------------------
// xmlconcat / xmlconcat2
// -------------------------------------------------------------------------

/*
 * TODO: xmlconcat needs to merge the notations and unparsed entities
 * of the argument values.  Not very important in practice, though.
 */
/// xmlconcat -- concatenate a list of xml values.
pub unsafe fn xmlconcat(args: *mut List) -> *mut xmltype {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // iterate args via foreach!(lc, args, { ... })
        // for each: x = DatumGetXmlP(PointerGetDatum(lfirst(lc)));
        // parse_xml_decl / track global_standalone, global_version
        // appendStringInfoString(&mut buf, str + len)
        // emit declaration if needed
        // return stringinfo_to_xmltype(&mut buf)
    }
    */
}

/// xmlconcat2(xml, xml) -> xml -- aggregate transition function.
pub unsafe fn xmlconcat2(fcinfo: FunctionCallInfo) -> Datum {
    if PG_ARGISNULL!(fcinfo, 0) {
        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_NULL!(fcinfo)
        } else {
            PG_RETURN_XML_P!(PG_GETARG_XML_P!(fcinfo, 1))
        }
    } else if PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_XML_P!(PG_GETARG_XML_P!(fcinfo, 0))
    } else {
        PG_RETURN_XML_P!(xmlconcat(list_make2(
            PG_GETARG_XML_P!(fcinfo, 0) as *mut c_void,
            PG_GETARG_XML_P!(fcinfo, 1) as *mut c_void
        )))
    }
}

// -------------------------------------------------------------------------
// texttoxml / xmltotext
// -------------------------------------------------------------------------

/// texttoxml(text) -> xml  (equivalent to XMLPARSE)
pub unsafe fn texttoxml(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut crate::c::text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    PG_RETURN_XML_P!(xmlparse(data, xmloption_as_type(xmloption), true))
}

/// xmltotext(xml) -> text  (binary-compatible cast)
pub unsafe fn xmltotext(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut xmltype = PG_GETARG_XML_P!(fcinfo, 0);
    /* It's actually binary compatible. */
    PG_RETURN_TEXT_P!(data as *mut crate::c::text)
}

/// xmltotext_with_options -- used by xmlserialize.
pub unsafe fn xmltotext_with_options(
    data: *mut xmltype,
    xmloption_arg: XmlOptionType,
    indent: bool,
) -> *mut crate::c::text {
    if xmloption_arg != XMLOPTION_DOCUMENT && !indent {
        /*
         * We don't actually need to do anything, so just return the
         * binary-compatible input.  For backwards-compatibility reasons,
         * allow such cases to succeed even without USE_LIBXML.
         */
        return data as *mut crate::c::text;
    }
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // doc = xml_parse(data, xmloption_arg, !indent, GetDatabaseEncoding(), &parsed_xmloptiontype, &content_nodes, &escontext);
        // if doc == NULL || escontext.error_occurred { ... ereport NOT_AN_XML_DOCUMENT }
        // if !indent { xmlFreeDoc(doc); return data as *mut text; }
        // PG_TRY: buf = xmlBufferCreate(); ctxt = xmlSaveToBuffer(...);
        //   xmlSaveDoc / xmlSaveTree; xmlSaveClose; trim trailing newlines; result = ...
        // PG_CATCH: xmlSaveClose(ctxt); xmlBufferFree(buf); xmlFreeDoc(doc); pg_xml_done(xmlerrcxt, true);
        // xmlBufferFree(buf); xmlFreeDoc(doc); pg_xml_done(xmlerrcxt, false);
        // return result
    }
    */
}

// -------------------------------------------------------------------------
// xmlelement
// -------------------------------------------------------------------------

/// xmlelement -- construct an XML element node.
pub unsafe fn xmlelement(
    xexpr: *mut XmlExpr,
    named_argvalue: *mut Datum,
    named_argnull: *mut bool,
    argvalue: *mut Datum,
    argnull: *mut bool,
) -> *mut xmltype {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // Build named_arg_strings and arg_strings lists.
        // pg_xml_init(PG_XML_STRICTNESS_ALL);
        // buf = xmlBufferCreate(); writer = xmlNewTextWriterMemory(buf, 0);
        // xmlTextWriterStartElement(writer, xexpr->name);
        // forboth: xmlTextWriterWriteAttribute for named args
        // foreach: xmlTextWriterWriteRaw for content args
        // xmlTextWriterEndElement; xmlFreeTextWriter(writer); writer = NULL;
        // result = xmlBuffer_to_xmltype(buf);
        // PG_CATCH: cleanup; PG_RE_THROW
        // xmlBufferFree(buf); pg_xml_done(xmlerrcxt, false);
    }
    */
}

// -------------------------------------------------------------------------
// xmlparse
// -------------------------------------------------------------------------

/// xmlparse(text, XmlOptionType, preserve_whitespace) -> xmltype
pub unsafe fn xmlparse(
    data: *mut crate::c::text,
    _xmloption_arg: XmlOptionType,
    _preserve_whitespace: bool,
) -> *mut xmltype {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // doc = xml_parse(data, xmloption_arg, preserve_whitespace, GetDatabaseEncoding(), NULL, NULL, NULL);
        // xmlFreeDoc(doc);
        // return data as *mut xmltype
    }
    */
}

// -------------------------------------------------------------------------
// xmlpi
// -------------------------------------------------------------------------

/// xmlpi(target, arg, arg_is_null, result_is_null) -> xmltype
pub unsafe fn xmlpi(
    target: *const c_char,
    arg: *mut crate::c::text,
    arg_is_null: bool,
    result_is_null: *mut bool,
) -> *mut xmltype {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // if pg_strcasecmp(target, c"xml".as_ptr()) == 0:
        //   ereport(ERROR, ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION, ...)
        // *result_is_null = arg_is_null; if *result_is_null: return NULL;
        // initStringInfo(&mut buf);
        // appendStringInfo(&mut buf, "<?%s", target);
        // if arg != NULL: check for "?>" in string; append " " + stripped string
        // appendStringInfoString(&mut buf, "?>");
        // result = stringinfo_to_xmltype(&mut buf); pfree(buf.data); return result
    }
    */
}

// -------------------------------------------------------------------------
// xmlroot
// -------------------------------------------------------------------------

/// xmlroot(xmltype, text version, int standalone) -> xmltype
pub unsafe fn xmlroot(
    data: *mut xmltype,
    version: *mut crate::c::text,
    standalone: c_int,
) -> *mut xmltype {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // len = VARSIZE(data) - VARHDRSZ; str = text_to_cstring(data);
        // parse_xml_decl((xmlChar*)str, &len, &orig_version, NULL, &orig_standalone);
        // if version: orig_version = xml_text2xmlChar(version); else orig_version = NULL;
        // match standalone { XML_STANDALONE_YES => orig_standalone = 1, ... }
        // initStringInfo(&mut buf);
        // print_xml_decl(&mut buf, orig_version, 0, orig_standalone);
        // appendStringInfoString(&mut buf, str + len);
        // return stringinfo_to_xmltype(&mut buf)
    }
    */
}

// -------------------------------------------------------------------------
// xmlvalidate (permanently removed in PG; always errors)
// -------------------------------------------------------------------------

/*
 * Validate document (given as string) against DTD (given as external link)
 *
 * This has been removed because it is a security hole: unprivileged users
 * should not be able to use Postgres to fetch arbitrary external files,
 * which unfortunately is exactly what libxml is willing to do with the DTD
 * parameter.
 */
pub unsafe fn xmlvalidate(fcinfo: FunctionCallInfo) -> Datum {
    ereport!(ERROR, errmsg!("xmlvalidate is not implemented"));
    #[allow(unreachable_code)]
    0
}

// -------------------------------------------------------------------------
// xml_is_document
// -------------------------------------------------------------------------

/// xml_is_document(xml) -> bool
pub unsafe fn xml_is_document(arg: *mut xmltype) -> bool {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    false
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // ErrorSaveContext escontext = {T_ErrorSaveContext};
        // doc = xml_parse(arg as *mut text, XMLOPTION_DOCUMENT, true, GetDatabaseEncoding(), NULL, NULL, &escontext);
        // if doc: xmlFreeDoc(doc);
        // return !escontext.error_occurred
    }
    */
}


// =========================================================================
// PART 3 -- pg_xml_init/done (libxml-gated), parse_xml_decl, print_xml_decl,
//           xml_doctype_in_content, xml_text2xmlChar, xml_pnstrdup,
//           pg_xmlCharStrndup, xml_pstrdup_and_free, and the
//           is_valid_xml_name* helpers.
// =========================================================================

// -------------------------------------------------------------------------
// pg_xml_init_library / pg_xml_init / pg_xml_done / pg_xml_error_occurred
// These are all USE_LIBXML gated in C.  We provide NO_XML_SUPPORT stubs.
// -------------------------------------------------------------------------

/*
 * pg_xml_init_library --- set up for use of libxml
 *
 * This should be called by each function that is about to use libxml
 * facilities but doesn't require error handling.  It initializes libxml
 * and verifies compatibility with the loaded libxml version.
 */
#[cfg(any())]
pub unsafe fn pg_xml_init_library() {
    // static mut first_time: bool = true;
    // if first_time {
    //   if core::mem::size_of::<c_char>() != core::mem::size_of::<xmlChar>(): ereport ERROR
    //   #ifdef USE_LIBXMLCONTEXT: xml_memory_init(); #endif
    //   LIBXML_TEST_VERSION;
    //   first_time = false;
    // }
}

/*
 * pg_xml_init --- set up for use of libxml and register an error handler
 *
 * This should be called by each function that is about to use libxml
 * facilities and requires error handling.
 *
 * Calls to this function MUST be followed by a PG_TRY block that guarantees
 * that pg_xml_done() is called during either normal or error exit.
 *
 * This is exported for use by contrib/xml2.
 */
#[cfg(any())]
pub unsafe fn pg_xml_init(_strictness: c_int) -> *mut PgXmlErrorContext {
    // pg_xml_init_library();
    // errcxt = palloc(sizeof PgXmlErrorContext) as *mut PgXmlErrorContext;
    // errcxt->magic = ERRCXT_MAGIC; errcxt->strictness = strictness;
    // errcxt->err_occurred = false; initStringInfo(&mut errcxt->err_buf);
    // errcxt->saved_errfunc = xmlStructuredError;
    // errcxt->saved_errcxt = xmlStructuredErrorContext; (or xmlGenericErrorContext)
    // xmlSetStructuredErrorFunc(errcxt, xml_errorHandler);
    // verify new_errcxt == errcxt; if not: ereport ERROR
    // errcxt->saved_entityfunc = xmlGetExternalEntityLoader();
    // xmlSetExternalEntityLoader(xmlPgEntityLoader);
    // return errcxt
    core::ptr::null_mut()
}

/*
 * pg_xml_done --- restore previous libxml error handling
 *
 * Resets libxml's global error-handling state to what it was before
 * pg_xml_init() was called.
 */
#[cfg(any())]
pub unsafe fn pg_xml_done(_errcxt: *mut PgXmlErrorContext, _isError: bool) {
    // Assert(errcxt->magic == ERRCXT_MAGIC);
    // check cur_errcxt == errcxt (warn if not)
    // xmlSetStructuredErrorFunc(errcxt->saved_errcxt, errcxt->saved_errfunc);
    // xmlSetExternalEntityLoader(errcxt->saved_entityfunc);
    // errcxt->magic = 0;
    // pfree(errcxt->err_buf.data); pfree(errcxt);
}

/*
 * pg_xml_error_occurred() --- test the error flag
 */
#[cfg(any())]
pub unsafe fn pg_xml_error_occurred(errcxt: *mut PgXmlErrorContext) -> bool {
    (*errcxt).err_occurred
}

// -------------------------------------------------------------------------
// parse_xml_decl -- parse the optional XML declaration at the start of a
//                   document.  Fully translated; libxml *character-class*
//                   macros are stubbed (they are USE_LIBXML gated in C).
// -------------------------------------------------------------------------

/*
 * SQL/XML allows storing "XML documents" or "XML content".  "XML
 * documents" are specified by the XML specification and are parsed
 * easily by libxml.  "XML content" is specified by SQL/XML as the
 * production "XMLDecl? content".  But libxml can only parse the
 * "content" part, so we have to parse the XML declaration ourselves
 * to complete this.
 */

/* XML error codes used by parse_xml_decl */
pub const XML_ERR_OK: c_int = 0;
pub const XML_ERR_INVALID_CHAR: c_int = 25;
pub const XML_ERR_SPACE_REQUIRED: c_int = 27;
pub const XML_ERR_STANDALONE_VALUE: c_int = 78;
pub const XML_ERR_VERSION_MISSING: c_int = 110;
pub const XML_ERR_MISSING_ENCODING: c_int = 56;
pub const XML_ERR_XMLDECL_NOT_FINISHED: c_int = 23;

/*
 * xml_pnstrdup -- duplicate len xmlChars (xmlChar = u8 in this port).
 * xmlChar is just u8 in libxml2 and we represent it as c_char here.
 */
unsafe fn xml_pnstrdup(str_: *const c_char, len: usize) -> *mut c_char {
    let result: *mut c_char = palloc(len + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(str_, result, len);
    *result.add(len) = 0;
    result
}

/*
 * pg_xmlCharStrndup -- duplicate len chars of a regular C string as xmlChar*.
 */
unsafe fn pg_xmlCharStrndup(str_: *const c_char, len: usize) -> *mut c_char {
    let result: *mut c_char = palloc(len + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(str_, result, len);
    *result.add(len) = 0;
    result
}

/*
 * xml_pstrdup_and_free -- copy xmlChar string to palloc'd memory, freeing input.
 */
#[cfg(any())]
unsafe fn xml_pstrdup_and_free(str_: *mut c_char) -> *mut c_char {
    // PG_TRY: result = pstrdup(str_);
    // PG_FINALLY: xmlFree(str_);
    // return result
    core::ptr::null_mut()
}

// Stub for xmlIsBlank_ch: ASCII whitespace check (replaces libxml macro).
#[inline]
fn xml_is_blank_ch(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r' || c == b'\n'
}

// CHECK_XML_SPACE -- returns XML_ERR_SPACE_REQUIRED if *p is not whitespace.
macro_rules! CHECK_XML_SPACE {
    ($p:expr, $ret:expr) => {
        if !xml_is_blank_ch(*$p as u8) {
            return XML_ERR_SPACE_REQUIRED;
        }
    };
}

// SKIP_XML_SPACE -- advance pointer past whitespace.
macro_rules! SKIP_XML_SPACE {
    ($p:expr) => {
        while xml_is_blank_ch(*$p as u8) { $p = $p.add(1); }
    };
}

/*
 * parse_xml_decl -- parse the XMLDecl at the start of str.
 *
 * str is the null-terminated input string.  Remaining arguments are
 * output arguments; each can be NULL if value is not wanted.
 * version and encoding are returned as locally-palloc'd strings.
 * Result is 0 if OK, an error code if not.
 */
pub unsafe fn parse_xml_decl(
    str_: *const c_char,
    lenp: *mut usize,
    version: *mut *mut c_char,
    encoding: *mut *mut c_char,
    standalone: *mut c_int,
) -> c_int {
    /*
     * Only initialize libxml.  We don't need error handling here, but we do
     * need to make sure libxml is initialized before calling any of its
     * functions.  (No-op when USE_LIBXML is off.)
     */

    /* Initialize output arguments to "not present" */
    if !version.is_null() { *version = core::ptr::null_mut(); }
    if !encoding.is_null() { *encoding = core::ptr::null_mut(); }
    if !standalone.is_null() { *standalone = -1; }

    let mut p: *const c_char = str_;

    /* Check for "<?xml" prefix */
    if !starts_with_bytes(p, b"<?xml") {
        return finish_parse_xml_decl(str_, p, lenp);
    }

    /*
     * If next char is a name char, it's a PI like <?xml-stylesheet ...?>
     * rather than an XMLDecl.
     */
    let next: u8 = *p.add(5) as u8;
    if is_xml_namechar_ascii(next) {
        return finish_parse_xml_decl(str_, p, lenp);
    }

    p = p.add(5);

    /* version */
    CHECK_XML_SPACE!(p, XML_ERR_SPACE_REQUIRED);
    SKIP_XML_SPACE!(p);
    if !starts_with_bytes(p, b"version") {
        return XML_ERR_VERSION_MISSING;
    }
    p = p.add(7);
    SKIP_XML_SPACE!(p);
    if *p != b'=' as c_char {
        return XML_ERR_VERSION_MISSING;
    }
    p = p.add(1);
    SKIP_XML_SPACE!(p);

    if *p == b'\'' as c_char || *p == b'"' as c_char {
        let quote: c_char = *p;
        p = p.add(1);
        let start_v: *const c_char = p;
        while *p != 0 && *p != quote { p = p.add(1); }
        if *p == 0 { return XML_ERR_VERSION_MISSING; }
        if !version.is_null() {
            *version = xml_pnstrdup(start_v, (p as usize) - (start_v as usize));
        }
        p = p.add(1);
    } else {
        return XML_ERR_VERSION_MISSING;
    }

    /* encoding */
    let save_p: *const c_char = p;
    let mut pp: *const c_char = p;
    SKIP_XML_SPACE!(pp);
    if starts_with_bytes(pp, b"encoding") {
        CHECK_XML_SPACE!(save_p, XML_ERR_SPACE_REQUIRED);
        p = pp.add(8);
        SKIP_XML_SPACE!(p);
        if *p != b'=' as c_char { return XML_ERR_MISSING_ENCODING; }
        p = p.add(1);
        SKIP_XML_SPACE!(p);
        if *p == b'\'' as c_char || *p == b'"' as c_char {
            let quote: c_char = *p;
            p = p.add(1);
            let start_e: *const c_char = p;
            while *p != 0 && *p != quote { p = p.add(1); }
            if *p == 0 { return XML_ERR_MISSING_ENCODING; }
            if !encoding.is_null() {
                *encoding = xml_pnstrdup(start_e, (p as usize) - (start_e as usize));
            }
            p = p.add(1);
        } else {
            return XML_ERR_MISSING_ENCODING;
        }
    } else {
        p = save_p;
    }

    /* standalone */
    let save_p2: *const c_char = p;
    let mut pp2: *const c_char = p;
    SKIP_XML_SPACE!(pp2);
    if starts_with_bytes(pp2, b"standalone") {
        CHECK_XML_SPACE!(save_p2, XML_ERR_SPACE_REQUIRED);
        p = pp2.add(10);
        SKIP_XML_SPACE!(p);
        if *p != b'=' as c_char { return XML_ERR_STANDALONE_VALUE; }
        p = p.add(1);
        SKIP_XML_SPACE!(p);
        if starts_with_bytes(p, b"'yes'") || starts_with_bytes(p, b"\"yes\"") {
            if !standalone.is_null() { *standalone = 1; }
            p = p.add(5);
        } else if starts_with_bytes(p, b"'no'") || starts_with_bytes(p, b"\"no\"") {
            if !standalone.is_null() { *standalone = 0; }
            p = p.add(4);
        } else {
            return XML_ERR_STANDALONE_VALUE;
        }
    } else {
        p = save_p2;
    }

    SKIP_XML_SPACE!(p);
    if !starts_with_bytes(p, b"?>") {
        return XML_ERR_XMLDECL_NOT_FINISHED;
    }
    p = p.add(2);

    /* validate that declaration contains only ASCII bytes */
    let len: usize = (p as usize) - (str_ as usize);
    let mut q: *const c_char = str_;
    while q < str_.add(len) {
        if (*q as u8) > 127 { return XML_ERR_INVALID_CHAR; }
        q = q.add(1);
    }
    if !lenp.is_null() { *lenp = len; }
    XML_ERR_OK
}

/// Helper: complete parse_xml_decl at "finished:" label (no declaration found).
unsafe fn finish_parse_xml_decl(
    str_: *const c_char,
    p: *const c_char,
    lenp: *mut usize,
) -> c_int {
    let len: usize = (p as usize) - (str_ as usize);
    let mut q: *const c_char = str_;
    while q < str_.add(len) {
        if (*q as u8) > 127 { return XML_ERR_INVALID_CHAR; }
        q = q.add(1);
    }
    if !lenp.is_null() { *lenp = len; }
    XML_ERR_OK
}

/// starts_with_bytes: compare *p against a byte-literal prefix (null-terminated comparison).
#[inline]
unsafe fn starts_with_bytes(p: *const c_char, prefix: &[u8]) -> bool {
    for (i, &b) in prefix.iter().enumerate() {
        if (*p.add(i) as u8) != b { return false; }
    }
    true
}

/// is_xml_namechar_ascii: simple ASCII NameChar check (underscore, letter, digit, colon, hyphen, dot).
#[inline]
fn is_xml_namechar_ascii(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b':' || c == b'-' || c == b'.'
}

// -------------------------------------------------------------------------
// print_xml_decl
// -------------------------------------------------------------------------

/*
 * Write an XML declaration.  On output, we adjust the XML declaration
 * as follows.  (These rules are the moral equivalent of the clause
 * "Serialization of an XML value" in the SQL standard.)
 *
 * We try to avoid generating an XML declaration if possible.
 */
pub unsafe fn print_xml_decl(
    buf: StringInfo,
    version: *const c_char,
    encoding: pg_enc,
    standalone: c_int,
) -> bool {
    let default_version_match = if version.is_null() {
        false
    } else {
        /* strcmp version with PG_XML_DEFAULT_VERSION = "1.0" */
        let mut i = 0usize;
        let dflt = b"1.0";
        let mut matched = true;
        for &b in dflt.iter() {
            if (*version.add(i) as u8) != b { matched = false; break; }
            i += 1;
        }
        if matched && *version.add(i) != 0 { matched = false; }
        matched
    };

    let needs_decl = (!version.is_null() && !default_version_match)
        || (encoding != 0 && encoding != PG_UTF8)
        || standalone != -1;

    if needs_decl {
        appendStringInfoString(buf, b"<?xml\0".as_ptr() as *const c_char);

        if !version.is_null() {
            appendStringInfoString(buf, b" version=\"\0".as_ptr() as *const c_char);
            appendStringInfoString(buf, version);
            appendStringInfoChar(buf, b'"' as c_char);
        } else {
            appendStringInfoString(buf, b" version=\"1.0\"\0".as_ptr() as *const c_char);
        }

        if encoding != 0 && encoding != PG_UTF8 {
            /*
             * XXX might be useful to convert this to IANA names (ISO-8859-1
             * instead of LATIN1 etc.); needs field experience
             */
            appendStringInfoString(buf, b" encoding=\"\0".as_ptr() as *const c_char);
            appendStringInfoString(buf, pg_encoding_to_char(encoding));
            appendStringInfoChar(buf, b'"' as c_char);
        }

        if standalone == 1 {
            appendStringInfoString(buf, b" standalone=\"yes\"\0".as_ptr() as *const c_char);
        } else if standalone == 0 {
            appendStringInfoString(buf, b" standalone=\"no\"\0".as_ptr() as *const c_char);
        }
        appendStringInfoString(buf, b"?>\0".as_ptr() as *const c_char);

        true
    } else {
        false
    }
}

// -------------------------------------------------------------------------
// xml_doctype_in_content
// -------------------------------------------------------------------------

/*
 * Test whether an input that is to be parsed as CONTENT contains a DTD.
 *
 * A DTD can be found arbitrarily far in, but that would be a contrived case;
 * it will ordinarily start within a few dozen characters.
 */
pub unsafe fn xml_doctype_in_content(str_: *const c_char) -> bool {
    let mut p: *const c_char = str_;

    loop {
        SKIP_XML_SPACE!(p);
        if *p != b'<' as c_char { return false; }
        p = p.add(1);

        if *p == b'!' as c_char {
            p = p.add(1);

            /* if we see <!DOCTYPE, we can return true */
            if starts_with_bytes(p, b"DOCTYPE") { return true; }

            /* otherwise, if it's not a comment, fail */
            if !starts_with_bytes(p, b"--") { return false; }

            /* find end of comment: find -- and a > must follow */
            let mut q: *const c_char = p.add(2);
            loop {
                if *q == 0 { return false; }
                if *q == b'-' as c_char && *q.add(1) == b'-' as c_char {
                    if *q.add(2) != b'>' as c_char { return false; }
                    p = q.add(3);
                    break;
                }
                q = q.add(1);
            }
            continue;
        }

        /* otherwise, if it's not a PI <?target something?>, fail */
        if *p != b'?' as c_char { return false; }
        p = p.add(1);

        /* find end of PI (the string ?> is forbidden within a PI) */
        let mut e: *const c_char = p;
        loop {
            if *e == 0 { return false; }
            if *e == b'?' as c_char && *e.add(1) == b'>' as c_char { break; }
            e = e.add(1);
        }
        /* advance over PI, keep scanning */
        p = e.add(2);
    }
}

// -------------------------------------------------------------------------
// xml_text2xmlChar (USE_LIBXML gated in C)
// -------------------------------------------------------------------------

/*
 * xmlChar<->text conversions
 */
#[cfg(any())]
unsafe fn xml_text2xmlChar(in_: *mut crate::c::text) -> *mut c_char {
    // return text_to_cstring(in_) as *mut xmlChar
    core::ptr::null_mut()
}

// -------------------------------------------------------------------------
// sqlchar_to_unicode / is_valid_xml_namefirst / is_valid_xml_namechar
// (USE_LIBXML gated in C -- these call xmlIsBaseCharQ etc.)
// -------------------------------------------------------------------------

/*
 * Convert one char in the current server encoding to a Unicode codepoint.
 */
#[cfg(any())]
unsafe fn sqlchar_to_unicode(s: *const c_char) -> pg_wchar {
    // utf8string = pg_server_to_any(s, pg_mblen_cstr(s), PG_UTF8);
    // pg_encoding_mb2wchar_with_len(PG_UTF8, utf8string, ret, pg_encoding_mblen(PG_UTF8, utf8string));
    // if utf8string != s: pfree(utf8string);
    // return ret[0]
    0
}

#[cfg(any())]
fn is_valid_xml_namefirst(_c: pg_wchar) -> bool {
    // xmlIsBaseCharQ(c) || xmlIsIdeographicQ(c) || c == '_' || c == ':'
    false
}

#[cfg(any())]
fn is_valid_xml_namechar(_c: pg_wchar) -> bool {
    // Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender
    false
}

// -------------------------------------------------------------------------
// xml_ereport / xml_errsave (USE_LIBXML gated in C)
// -------------------------------------------------------------------------

/*
 * xml_ereport --- report an XML-related error
 *
 * The "msg" is the SQL-level message; some can be adopted from the SQL/XML
 * standard.  This function adds libxml's native error message, if any, as
 * detail.
 *
 * This is exported for use by contrib/xml2.
 */
#[cfg(any())]
pub unsafe fn xml_ereport(
    errcxt: *mut PgXmlErrorContext,
    level: c_int,
    sqlcode: c_int,
    msg: *const c_char,
) {
    // Defend against bogus context struct (check magic)
    // errcxt->err_occurred = false
    // detail = if errcxt->err_buf.len > 0 { errcxt->err_buf.data } else { NULL }
    // ereport(level, errcode(sqlcode), errmsg_internal("%s", msg), [errdetail_internal("%s", detail)])
}

#[cfg(any())]
unsafe fn xml_errsave(
    escontext: *mut Node,
    errcxt: *mut PgXmlErrorContext,
    sqlcode: c_int,
    msg: *const c_char,
) {
    // errcxt->err_occurred = false
    // detail = if errcxt->err_buf.len > 0 { errcxt->err_buf.data } else { NULL }
    // errsave(escontext, errcode(sqlcode), errmsg_internal("%s", msg), ...)
}

// -------------------------------------------------------------------------
// xml_errorHandler, errdetail_for_xml_code, chopStringInfoNewlines,
// appendStringInfoLineSeparator (all USE_LIBXML gated)
// -------------------------------------------------------------------------

#[cfg(any())]
unsafe fn xml_errorHandler(_data: *mut c_void, _error: *mut c_void /* PgXmlErrorPtr */) {
    // Complex libxml error handler; see C source for full logic.
    // Builds errorBuf, appends context, routes to err_buf or ereport WARNING/NOTICE.
}

#[cfg(any())]
fn errdetail_for_xml_code(code: c_int) -> c_int {
    // switch code { XML_ERR_INVALID_CHAR => ..., ... }
    0
}

#[cfg(any())]
unsafe fn chopStringInfoNewlines(str_: StringInfo) {
    // while str->len > 0 && str->data[str->len - 1] == '\n': str->data[--str->len] = '\0'
}

#[cfg(any())]
unsafe fn appendStringInfoLineSeparator(str_: StringInfo) {
    // chopStringInfoNewlines(str_); if str->len > 0: appendStringInfoChar(str_, '\n')
}

// -------------------------------------------------------------------------
// xml_parse (USE_LIBXML gated in C)
// -------------------------------------------------------------------------

#[cfg(any())]
unsafe fn xml_parse(
    _data: *mut crate::c::text,
    _xmloption_arg: XmlOptionType,
    _preserve_whitespace: bool,
    _encoding: c_int,
    _parsed_xmloptiontype: *mut XmlOptionType,
    _parsed_nodes: *mut *mut c_void, /* xmlNodePtr */
    _escontext: *mut Node,
) -> *mut c_void /* xmlDocPtr */ {
    // Full logic: pg_xml_init, parse_xml_decl, xml_doctype_in_content,
    // xmlCtxtReadDoc (document) or xmlParseBalancedChunkMemory (content),
    // PG_CATCH cleanup, pg_xml_done, return doc.
    core::ptr::null_mut()
}

// =========================================================================
// PART 3b -- map_sql_identifier_to_xml_name / map_xml_name_to_sql_identifier
// =========================================================================

/*
 * Map SQL identifier to XML name; see SQL/XML:2008 section 9.1.
 */
pub unsafe fn map_sql_identifier_to_xml_name(
    ident: *const c_char,
    fully_escaped: bool,
    escape_period: bool,
) -> *mut c_char {
    /* !USE_LIBXML: sqlchar_to_unicode / xmlIsBaseCharQ etc. not available */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    core::ptr::null_mut()
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // Assert(fully_escaped || !escape_period);
        // initStringInfo(&mut buf);
        // for p in ident chars (step by pg_mblen_cstr(p)):
        //   if *p == ':' && (p == ident || fully_escaped): append "_x003A_"
        //   else if *p == '_' && *(p+1) == 'x': append "_x005F_"
        //   else if fully_escaped && p==ident && pg_strncasecmp(p,"xml",3)==0:
        //     if *p == 'x': "_x0078_" else "_x0058_"
        //   else if escape_period && *p == '.': "_x002E_"
        //   else:
        //     u = sqlchar_to_unicode(p)
        //     if (p==ident) ? !is_valid_xml_namefirst(u) : !is_valid_xml_namechar(u):
        //       appendStringInfo(&buf, "_x{:04X}_", u)
        //     else: appendBinaryStringInfo(&mut buf, p, pg_mblen_cstr(p))
        // return buf.data
    }
    */
}

/*
 * Map XML name to SQL identifier; see SQL/XML:2008 section 9.3.
 */
pub unsafe fn map_xml_name_to_sql_identifier(name: *const c_char) -> *mut c_char {
    let mut buf: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut buf);

    let mut p: *const c_char = name;
    while *p != 0 {
        let step = pg_mblen_cstr(p) as usize;
        /* Check for _xHHHH_ escape sequence */
        if *p == b'_' as c_char
            && *p.add(1) == b'x' as c_char
            && is_hex_digit(*p.add(2) as u8)
            && is_hex_digit(*p.add(3) as u8)
            && is_hex_digit(*p.add(4) as u8)
            && is_hex_digit(*p.add(5) as u8)
            && *p.add(6) == b'_' as c_char
        {
            let mut cbuf: [u8; (MAX_UNICODE_EQUIVALENT_STRING + 1) as usize] = [0; (MAX_UNICODE_EQUIVALENT_STRING + 1) as usize];
            /* parse the 4 hex digits */
            let h1 = hex_val(*p.add(2) as u8) as u32;
            let h2 = hex_val(*p.add(3) as u8) as u32;
            let h3 = hex_val(*p.add(4) as u8) as u32;
            let h4 = hex_val(*p.add(5) as u8) as u32;
            let u: u32 = (h1 << 12) | (h2 << 8) | (h3 << 4) | h4;
            pg_unicode_to_server(u as pg_wchar, cbuf.as_mut_ptr() as *mut c_uchar);
            appendStringInfoString(&mut buf, cbuf.as_ptr() as *const c_char);
            p = p.add(7);
        } else {
            appendBinaryStringInfo(&mut buf, p as *const c_void, step as c_int);
            p = p.add(step);
        }
    }
    buf.data
}

#[inline]
fn is_hex_digit(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

#[inline]
fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}


// =========================================================================
// PART 4 -- map_sql_value_to_xml_value, escape_xml, _SPI_strdup,
//           SQL-to-XML mapping helpers (query_to_oid_list, schema/database
//           visible tables/schemas, xmldata_root_element_start/end,
//           query_to_xml_internal, table_to_xml_internal),
//           and all the table/query/cursor/schema/database to_xml Datum fns.
// =========================================================================

/*
 * Map SQL value to XML value; see SQL/XML:2008 section 9.8.
 *
 * When xml_escape_strings is true, then certain characters in string
 * values are replaced by entity references (&lt; etc.), as specified
 * in SQL/XML:2008 section 9.8 GR 9) a) iii).
 */
pub unsafe fn map_sql_value_to_xml_value(
    value: Datum,
    r#type: Oid,
    xml_escape_strings: bool,
) -> *mut c_char {
    if type_is_array_domain(r#type) {
        let array: *mut ArrayType = DatumGetArrayTypeP(value);
        let elmtype: Oid = ARR_ELEMTYPE(array);
        let elmlen: int16 = 0;
        let elmbyval: bool = false;
        let elmalign: c_char = 0;
        let mut num_elems: c_int = 0;
        let mut elem_values: *mut Datum = core::ptr::null_mut();
        let mut elem_nulls: *mut bool = core::ptr::null_mut();
        let mut buf: StringInfoData = core::mem::zeroed();

        /* get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign) */
        /* (stubbed -- no-op) */

        deconstruct_array(
            array, elmtype, elmlen, elmbyval, elmalign,
            &mut elem_values, &mut elem_nulls, &mut num_elems,
        );

        initStringInfo(&mut buf);

        for i in 0..num_elems as usize {
            if *elem_nulls.add(i) { continue; }
            appendStringInfoString(&mut buf, b"<element>\0".as_ptr() as *const c_char);
            appendStringInfoString(
                &mut buf,
                map_sql_value_to_xml_value(*elem_values.add(i), elmtype, true),
            );
            appendStringInfoString(&mut buf, b"</element>\0".as_ptr() as *const c_char);
        }

        pfree(elem_values as *mut c_void);
        pfree(elem_nulls as *mut c_void);

        return buf.data;
    }

    /* scalar path */

    /*
     * Flatten domains; the special-case treatments below should apply to,
     * eg, domains over boolean not just boolean.
     */
    let r#type = getBaseType(r#type);

    /*
     * Special XSD formatting for some data types
     */
    match r#type {
        BOOLOID => {
            if DatumGetBool(value) {
                return b"true\0".as_ptr() as *mut c_char;
            } else {
                return b"false\0".as_ptr() as *mut c_char;
            }
        }

        DATEOID => {
            let date: DateADT = DatumGetDateADT(value);
            /* XSD doesn't support infinite values */
            if DATE_NOT_FINITE(date) {
                ereport!(
                    ERROR,
                    errmsg!("date out of range")
                    /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errdetail("XML does not support infinite date values.") */
                );
            }
            let mut tm: pg_tm = core::mem::zeroed();
            j2date(
                date + POSTGRES_EPOCH_JDATE,
                &mut tm.tm_year,
                &mut tm.tm_mon,
                &mut tm.tm_mday,
            );
            let mut buf: [c_char; (MAXDATELEN + 1) as usize] = [0; (MAXDATELEN + 1) as usize];
            EncodeDateOnly(&mut tm, USE_XSD_DATES, buf.as_mut_ptr());
            return pstrdup(buf.as_ptr());
        }

        TIMESTAMPOID => {
            let timestamp: Timestamp = DatumGetTimestamp(value);
            /* XSD doesn't support infinite values */
            if TIMESTAMP_NOT_FINITE(timestamp) {
                ereport!(
                    ERROR,
                    errmsg!("timestamp out of range")
                    /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errdetail("XML does not support infinite timestamp values.") */
                );
            }
            let mut tm: pg_tm = core::mem::zeroed();
            let mut fsec: fsec_t = 0;
            let mut buf: [c_char; (MAXDATELEN + 1) as usize] = [0; (MAXDATELEN + 1) as usize];
            if timestamp2tm(timestamp, core::ptr::null_mut(), &mut tm, &mut fsec, core::ptr::null_mut(), core::ptr::null_mut()) == 0 {
                EncodeDateTime(&mut tm, fsec, false, 0, core::ptr::null(), USE_XSD_DATES, buf.as_mut_ptr());
            } else {
                ereport!(
                    ERROR,
                    errmsg!("timestamp out of range")
                    /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
                );
            }
            return pstrdup(buf.as_ptr());
        }

        TIMESTAMPTZOID => {
            let timestamp: Timestamp = DatumGetTimestamp(value);
            /* XSD doesn't support infinite values */
            if TIMESTAMP_NOT_FINITE(timestamp) {
                ereport!(
                    ERROR,
                    errmsg!("timestamp out of range")
                    /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errdetail("XML does not support infinite timestamp values.") */
                );
            }
            let mut tm: pg_tm = core::mem::zeroed();
            let mut tz: c_int = 0;
            let mut fsec: fsec_t = 0;
            let mut tzn: *const c_char = core::ptr::null();
            let mut buf: [c_char; (MAXDATELEN + 1) as usize] = [0; (MAXDATELEN + 1) as usize];
            if timestamp2tm(timestamp, &mut tz, &mut tm, &mut fsec, &mut tzn, core::ptr::null_mut()) == 0 {
                EncodeDateTime(&mut tm, fsec, true, tz, tzn, USE_XSD_DATES, buf.as_mut_ptr());
            } else {
                ereport!(
                    ERROR,
                    errmsg!("timestamp out of range")
                    /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
                );
            }
            return pstrdup(buf.as_ptr());
        }

        BYTEAOID => {
            /* USE_LIBXML gated in C; needs xmlTextWriterWriteBase64/BinHex */
            /* !USE_LIBXML path falls through to the native text representation below */
            /* #[cfg(any())] USE_LIBXML body:
            {
                // bstr = DatumGetByteaPP(value);
                // xmlerrcxt = pg_xml_init(PG_XML_STRICTNESS_ALL);
                // PG_TRY: buf = xmlBufferCreate(); writer = xmlNewTextWriterMemory(buf, 0);
                //   if xmlbinary == XMLBINARY_BASE64: xmlTextWriterWriteBase64(...)
                //   else: xmlTextWriterWriteBinHex(...)
                //   xmlFreeTextWriter(writer); writer = NULL;
                //   result = pstrdup(xmlBufferContent(buf));
                // PG_CATCH: cleanup; PG_RE_THROW
                // xmlBufferFree(buf); pg_xml_done(xmlerrcxt, false);
                // return result
            }
            */
        }

        _ => {} /* fall through */
    }

    /*
     * otherwise, just use the type's native text representation
     */
    let mut typeOut: Oid = 0;
    let mut isvarlena: bool = false;
    getTypeOutputInfo(r#type, &mut typeOut, &mut isvarlena);
    let str_: *mut c_char = OidOutputFunctionCall(typeOut, value);

    /* ... exactly as-is for XML, and when escaping is not wanted */
    if r#type == XMLOID || !xml_escape_strings {
        return str_;
    }

    /* otherwise, translate special characters as needed */
    escape_xml(str_)
}


/*
 * Escape characters in text that have special meanings in XML.
 *
 * Returns a palloc'd string.
 *
 * NB: this is intentionally not dependent on libxml.
 */
pub unsafe fn escape_xml(str_: *const c_char) -> *mut c_char {
    let mut buf: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut buf);

    let mut p: *const c_char = str_;
    while *p != 0 {
        match *p as u8 {
            b'&' => appendStringInfoString(&mut buf, b"&amp;\0".as_ptr() as *const c_char),
            b'<' => appendStringInfoString(&mut buf, b"&lt;\0".as_ptr() as *const c_char),
            b'>' => appendStringInfoString(&mut buf, b"&gt;\0".as_ptr() as *const c_char),
            b'\r' => appendStringInfoString(&mut buf, b"&#x0d;\0".as_ptr() as *const c_char),
            _ => appendStringInfoChar(&mut buf, *p),
        }
        p = p.add(1);
    }
    buf.data
}


unsafe fn _SPI_strdup(s: *const c_char) -> *mut c_char {
    let len: usize = libc_strlen(s) + 1;
    let ret: *mut c_char = SPI_palloc(len) as *mut c_char;
    core::ptr::copy_nonoverlapping(s, ret, len);
    ret
}


// -------------------------------------------------------------------------
// SQL-to-XML visibility helpers
// -------------------------------------------------------------------------

/*
 * Given a query, which must return type oid as first column, produce
 * a list of Oids with the query results.
 */
unsafe fn query_to_oid_list(query: *const c_char) -> *mut List {
    let mut list: *mut List = NIL;

    let spi_result = SPI_execute(query, true, 0);
    if spi_result != SPI_OK_SELECT {
        elog!(
            ERROR,
            "SPI_execute returned {} for {:?}",
            spi_result,
            core::ffi::CStr::from_ptr(query).to_string_lossy()
        );
    }

    let processed = SPI_processed;
    for i in 0..processed {
        let mut isnull: bool = false;
        let oid_val = SPI_getbinval(
            (*SPI_tuptable).vals.add(i as usize).read() as *mut c_void,
            (*SPI_tuptable).tupdesc,
            1,
            &mut isnull,
        );
        if !isnull {
            list = lappend_oid(list, DatumGetObjectId(oid_val));
        }
    }

    list
}


unsafe fn schema_get_xml_visible_tables(nspid: Oid) -> *mut List {
    let mut query: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut query);
    appendStringInfo!(
        &mut query,
        "SELECT oid FROM pg_catalog.pg_class WHERE relnamespace = {} AND relkind IN ('r','m','v') AND pg_catalog.has_table_privilege (oid, 'SELECT') ORDER BY relname;",
        nspid
    );
    query_to_oid_list(query.data)
}


const XML_VISIBLE_SCHEMAS_EXCLUDE: &[u8] =
    b"(nspname ~ '^pg_' OR nspname = 'information_schema')\0";

const XML_VISIBLE_SCHEMAS: &[u8] =
    b"SELECT oid FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege (oid, 'USAGE') AND NOT (nspname ~ '^pg_' OR nspname = 'information_schema')\0";


unsafe fn database_get_xml_visible_schemas() -> *mut List {
    let mut q: [u8; 256] = [0; 256];
    let src = b"SELECT oid FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege (oid, 'USAGE') AND NOT (nspname ~ '^pg_' OR nspname = 'information_schema') ORDER BY nspname;\0";
    core::ptr::copy_nonoverlapping(src.as_ptr(), q.as_mut_ptr(), src.len());
    query_to_oid_list(q.as_ptr() as *const c_char)
}


unsafe fn database_get_xml_visible_tables() -> *mut List {
    let q = b"SELECT oid FROM pg_catalog.pg_class WHERE relkind IN ('r','m','v') AND pg_catalog.has_table_privilege(pg_class.oid, 'SELECT') AND relnamespace IN (SELECT oid FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege (oid, 'USAGE') AND NOT (nspname ~ '^pg_' OR nspname = 'information_schema'));\0";
    query_to_oid_list(q.as_ptr() as *const c_char)
}


// -------------------------------------------------------------------------
// XML root element helpers
// -------------------------------------------------------------------------

/*
 * Write the start tag of the root element of a data mapping.
 *
 * top_level means that this is the very top level of the eventual output.
 */
unsafe fn xmldata_root_element_start(
    result: StringInfo,
    eltname: *const c_char,
    xmlschema: *const c_char,
    targetns: *const c_char,
    top_level: bool,
) {
    /* This isn't really wrong but currently makes no sense. */
    /* Assert(top_level || !xmlschema); */

    appendStringInfoChar(result, b'<' as c_char);
    appendStringInfoString(result, eltname);
    if top_level {
        appendStringInfoString(result, b" xmlns:xsi=\"\0".as_ptr() as *const c_char);
        appendStringInfoString(result, NAMESPACE_XSI.as_ptr() as *const c_char);
        appendStringInfoChar(result, b'"' as c_char);
        if !targetns.is_null() && *targetns != 0 {
            appendStringInfoString(result, b" xmlns=\"\0".as_ptr() as *const c_char);
            appendStringInfoString(result, targetns);
            appendStringInfoChar(result, b'"' as c_char);
        }
    }
    if !xmlschema.is_null() {
        /* FIXME: better targets */
        if !targetns.is_null() && *targetns != 0 {
            appendStringInfoString(result, b" xsi:schemaLocation=\"\0".as_ptr() as *const c_char);
            appendStringInfoString(result, targetns);
            appendStringInfoString(result, b" #\"\0".as_ptr() as *const c_char);
        } else {
            appendStringInfoString(
                result,
                b" xsi:noNamespaceSchemaLocation=\"#\"\0".as_ptr() as *const c_char,
            );
        }
    }
    appendStringInfoString(result, b">\n\0".as_ptr() as *const c_char);
}


unsafe fn xmldata_root_element_end(result: StringInfo, eltname: *const c_char) {
    appendStringInfoString(result, b"</\0".as_ptr() as *const c_char);
    appendStringInfoString(result, eltname);
    appendStringInfoString(result, b">\n\0".as_ptr() as *const c_char);
}


// -------------------------------------------------------------------------
// query_to_xml_internal / table_to_xml_internal
// -------------------------------------------------------------------------

unsafe fn query_to_xml_internal(
    query: *const c_char,
    tablename: *mut c_char,
    xmlschema: *const c_char,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
    top_level: bool,
) -> StringInfo {
    let result: StringInfo = makeStringInfo();
    let xmltn: *mut c_char = if !tablename.is_null() {
        map_sql_identifier_to_xml_name(tablename, true, false)
    } else {
        b"table\0".as_ptr() as *mut c_char
    };

    SPI_connect();
    if SPI_execute(query, true, 0) != SPI_OK_SELECT {
        ereport!(
            ERROR,
            errmsg!("invalid query")
            /* C also: errcode(ERRCODE_DATA_EXCEPTION) */
        );
    }

    if !tableforest {
        xmldata_root_element_start(result, xmltn, xmlschema, targetns, top_level);
        appendStringInfoChar(result, b'\n' as c_char);
    }

    if !xmlschema.is_null() {
        appendStringInfoString(result, xmlschema);
        appendStringInfoString(result, b"\n\n\0".as_ptr() as *const c_char);
    }

    let processed = SPI_processed;
    for i in 0..processed {
        SPI_sql_row_to_xmlelement(i, result, tablename, nulls, tableforest, targetns, top_level);
    }

    if !tableforest {
        xmldata_root_element_end(result, xmltn);
    }

    SPI_finish();

    result
}


unsafe fn table_to_xml_internal(
    relid: Oid,
    xmlschema: *const c_char,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
    top_level: bool,
) -> StringInfo {
    let mut query: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut query);
    appendStringInfo!(
        &mut query,
        "SELECT * FROM {}",
        cstr_to_display(DatumGetCString!(DirectFunctionCall1(regclassout, ObjectIdGetDatum(relid))))
    );
    query_to_xml_internal(
        query.data,
        get_rel_name(relid),
        xmlschema,
        nulls,
        tableforest,
        targetns,
        top_level,
    )
}

// -------------------------------------------------------------------------
// table_to_xml, query_to_xml, cursor_to_xml Datum functions
// -------------------------------------------------------------------------

pub unsafe fn table_to_xml(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    PG_RETURN_XML_P!(stringinfo_to_xmltype(table_to_xml_internal(
        relid, core::ptr::null(), nulls, tableforest, targetns, true
    )))
}


pub unsafe fn query_to_xml(fcinfo: FunctionCallInfo) -> Datum {
    let query: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    PG_RETURN_XML_P!(stringinfo_to_xmltype(query_to_xml_internal(
        query, core::ptr::null_mut(), core::ptr::null(),
        nulls, tableforest, targetns, true
    )))
}


pub unsafe fn cursor_to_xml(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let count: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 4));

    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);

    if !tableforest {
        xmldata_root_element_start(&mut result, b"table\0".as_ptr() as *const c_char, core::ptr::null(), targetns, true);
        appendStringInfoChar(&mut result, b'\n' as c_char);
    }

    SPI_connect();
    let portal: Portal = SPI_cursor_find(name);
    if portal.is_null() {
        ereport!(
            ERROR,
            errmsg!("cursor does not exist")
            /* C also: errcode(ERRCODE_UNDEFINED_CURSOR),
               errmsg("cursor \"%s\" does not exist", name) */
        );
    }

    SPI_cursor_fetch(portal, true, count as i64);
    let processed = SPI_processed;
    for i in 0..processed {
        SPI_sql_row_to_xmlelement(i, &mut result, core::ptr::null_mut(), nulls, tableforest, targetns, true);
    }

    SPI_finish();

    if !tableforest {
        xmldata_root_element_end(&mut result, b"table\0".as_ptr() as *const c_char);
    }

    PG_RETURN_XML_P!(stringinfo_to_xmltype(&mut result))
}


// =========================================================================
// PART 5 -- XML Schema mapping functions: table_to_xmlschema,
//           query_to_xmlschema, cursor_to_xmlschema, table_to_xml_and_xmlschema,
//           query_to_xml_and_xmlschema, schema_to_xml*, database_to_xml*,
//           map_multipart_sql_identifier_to_xml_name, map_sql_table_to_xmlschema,
//           map_sql_schema_to_xmlschema_types, map_sql_catalog_to_xmlschema_types,
//           map_sql_type_to_xml_name, map_sql_typecoll_to_xmlschema_types,
//           map_sql_type_to_xmlschema_type, SPI_sql_row_to_xmlelement.
// =========================================================================

// -------------------------------------------------------------------------
// XSD schema element helpers
// -------------------------------------------------------------------------

unsafe fn xsd_schema_element_start(result: StringInfo, targetns: *const c_char) {
    appendStringInfoString(
        result,
        b"<xsd:schema\n    xmlns:xsd=\"\0".as_ptr() as *const c_char,
    );
    appendStringInfoString(result, NAMESPACE_XSD.as_ptr() as *const c_char);
    appendStringInfoChar(result, b'"' as c_char);
    if !targetns.is_null() && *targetns != 0 {
        appendStringInfoString(
            result,
            b"\n    targetNamespace=\"\0".as_ptr() as *const c_char,
        );
        appendStringInfoString(result, targetns);
        appendStringInfoString(
            result,
            b"\"\n    elementFormDefault=\"qualified\"\0".as_ptr() as *const c_char,
        );
    }
    appendStringInfoString(result, b">\n\n\0".as_ptr() as *const c_char);
}


unsafe fn xsd_schema_element_end(result: StringInfo) {
    appendStringInfoString(result, b"</xsd:schema>\0".as_ptr() as *const c_char);
}

// -------------------------------------------------------------------------
// table_to_xmlschema, query_to_xmlschema, cursor_to_xmlschema
// -------------------------------------------------------------------------

pub unsafe fn table_to_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    let rel = table_open(relid, AccessShareLock);
    let result = map_sql_table_to_xmlschema((*rel).rd_att, relid, nulls, tableforest, targetns);
    table_close(rel, NoLock);

    PG_RETURN_XML_P!(cstring_to_xmltype(result))
}


pub unsafe fn query_to_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let query: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    SPI_connect();

    let plan = SPI_prepare(query, 0, core::ptr::null_mut());
    if plan.is_null() {
        elog!(ERROR, "SPI_prepare failed");
    }

    let portal = SPI_cursor_open(core::ptr::null(), plan, core::ptr::null_mut(), core::ptr::null_mut(), true);
    if portal.is_null() {
        elog!(ERROR, "SPI_cursor_open failed");
    }

    let result = _SPI_strdup(map_sql_table_to_xmlschema(
        (*(portal as *mut PortalData)).tupDesc,
        InvalidOid,
        nulls,
        tableforest,
        targetns,
    ));
    SPI_cursor_close(portal);
    SPI_finish();

    PG_RETURN_XML_P!(cstring_to_xmltype(result))
}


pub unsafe fn cursor_to_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    SPI_connect();
    let portal = SPI_cursor_find(name);
    if portal.is_null() {
        ereport!(
            ERROR,
            errmsg!("cursor does not exist")
            /* C also: errcode(ERRCODE_UNDEFINED_CURSOR),
               errmsg("cursor \"%s\" does not exist", name) */
        );
    }
    let pdata = portal as *mut PortalData;
    if (*pdata).tupDesc.is_null() {
        ereport!(
            ERROR,
            errmsg!("portal does not return tuples")
            /* C also: errcode(ERRCODE_INVALID_CURSOR_STATE),
               errmsg("portal \"%s\" does not return tuples", name) */
        );
    }

    let xmlschema = _SPI_strdup(map_sql_table_to_xmlschema(
        (*pdata).tupDesc,
        InvalidOid,
        nulls,
        tableforest,
        targetns,
    ));
    SPI_finish();

    PG_RETURN_XML_P!(cstring_to_xmltype(xmlschema))
}

// -------------------------------------------------------------------------
// table/query _to_xml_and_xmlschema
// -------------------------------------------------------------------------

pub unsafe fn table_to_xml_and_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    let rel = table_open(relid, AccessShareLock);
    let xmlschema = map_sql_table_to_xmlschema((*rel).rd_att, relid, nulls, tableforest, targetns);
    table_close(rel, NoLock);

    PG_RETURN_XML_P!(stringinfo_to_xmltype(table_to_xml_internal(
        relid, xmlschema, nulls, tableforest, targetns, true
    )))
}


pub unsafe fn query_to_xml_and_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let query: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    SPI_connect();

    let plan = SPI_prepare(query, 0, core::ptr::null_mut());
    if plan.is_null() { elog!(ERROR, "SPI_prepare failed"); }

    let portal = SPI_cursor_open(core::ptr::null(), plan, core::ptr::null_mut(), core::ptr::null_mut(), true);
    if portal.is_null() { elog!(ERROR, "SPI_cursor_open failed"); }

    let xmlschema = _SPI_strdup(map_sql_table_to_xmlschema(
        (*(portal as *mut PortalData)).tupDesc,
        InvalidOid,
        nulls,
        tableforest,
        targetns,
    ));
    SPI_cursor_close(portal);
    SPI_finish();

    PG_RETURN_XML_P!(stringinfo_to_xmltype(query_to_xml_internal(
        query, core::ptr::null_mut(), xmlschema, nulls, tableforest, targetns, true
    )))
}

// -------------------------------------------------------------------------
// schema_to_xml_internal / schema_to_xml / schema_to_xmlschema /
// schema_to_xml_and_xmlschema
// -------------------------------------------------------------------------

unsafe fn schema_to_xml_internal(
    nspid: Oid,
    xmlschema: *const c_char,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
    top_level: bool,
) -> StringInfo {
    let result: StringInfo = makeStringInfo();
    let xmlsn: *mut c_char = map_sql_identifier_to_xml_name(get_namespace_name(nspid), true, false);

    xmldata_root_element_start(result, xmlsn, xmlschema, targetns, top_level);
    appendStringInfoChar(result, b'\n' as c_char);

    if !xmlschema.is_null() {
        appendStringInfoString(result, xmlschema);
        appendStringInfoString(result, b"\n\n\0".as_ptr() as *const c_char);
    }

    SPI_connect();

    let relid_list = schema_get_xml_visible_tables(nspid);
    /* foreach!(lc, relid_list, { ... }) */
    /* Since pg_list iteration requires the real list API, we use a stub loop */
    let _ = relid_list; /* TODO(pg-port): iterate relid_list with foreach! */

    SPI_finish();

    xmldata_root_element_end(result, xmlsn);

    result
}


pub unsafe fn schema_to_xml(fcinfo: FunctionCallInfo) -> Datum {
    let name = PG_GETARG_NAME!(fcinfo, 0);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    let schemaname: *mut c_char = crate::c::NameStr(&*name) as *mut c_char;
    let nspid = LookupExplicitNamespace(schemaname, false);

    PG_RETURN_XML_P!(stringinfo_to_xmltype(schema_to_xml_internal(
        nspid, core::ptr::null(), nulls, tableforest, targetns, true
    )))
}


/*
 * Write the start element of the root element of an XML Schema mapping.
 */
unsafe fn schema_to_xmlschema_internal(
    schemaname: *const c_char,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
) -> StringInfo {
    let result: StringInfo = makeStringInfo();
    let nspid = LookupExplicitNamespace(schemaname, false);

    xsd_schema_element_start(result, targetns);

    SPI_connect();

    let relid_list = schema_get_xml_visible_tables(nspid);

    let tupdesc_list: *mut List = NIL;
    /* TODO(pg-port): iterate relid_list with foreach! to build tupdesc_list */
    let _ = relid_list;

    appendStringInfoString(
        result,
        map_sql_typecoll_to_xmlschema_types(tupdesc_list),
    );

    appendStringInfoString(
        result,
        map_sql_schema_to_xmlschema_types(nspid, NIL, nulls, tableforest, targetns),
    );

    xsd_schema_element_end(result);

    SPI_finish();

    result
}


pub unsafe fn schema_to_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let name = PG_GETARG_NAME!(fcinfo, 0);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    PG_RETURN_XML_P!(stringinfo_to_xmltype(schema_to_xmlschema_internal(
        crate::c::NameStr(&*name),
        nulls,
        tableforest,
        targetns,
    )))
}


pub unsafe fn schema_to_xml_and_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let name = PG_GETARG_NAME!(fcinfo, 0);
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 3));

    let schemaname: *mut c_char = crate::c::NameStr(&*name) as *mut c_char;
    let nspid = LookupExplicitNamespace(schemaname, false);

    let xmlschema = schema_to_xmlschema_internal(schemaname, nulls, tableforest, targetns);

    PG_RETURN_XML_P!(stringinfo_to_xmltype(schema_to_xml_internal(
        nspid,
        (*xmlschema).data,
        nulls,
        tableforest,
        targetns,
        true,
    )))
}

// -------------------------------------------------------------------------
// database_to_xml*, database_to_xmlschema*, database_to_xml_and_xmlschema
// -------------------------------------------------------------------------

unsafe fn database_to_xml_internal(
    xmlschema: *const c_char,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
) -> StringInfo {
    let result: StringInfo = makeStringInfo();
    let xmlcn: *mut c_char = map_sql_identifier_to_xml_name(
        get_database_name(MyDatabaseId), true, false,
    );

    xmldata_root_element_start(result, xmlcn, xmlschema, targetns, true);
    appendStringInfoChar(result, b'\n' as c_char);

    if !xmlschema.is_null() {
        appendStringInfoString(result, xmlschema);
        appendStringInfoString(result, b"\n\n\0".as_ptr() as *const c_char);
    }

    SPI_connect();

    let nspid_list = database_get_xml_visible_schemas();
    /* TODO(pg-port): iterate nspid_list with foreach! */
    let _ = nspid_list;

    SPI_finish();

    xmldata_root_element_end(result, xmlcn);

    result
}


pub unsafe fn database_to_xml(fcinfo: FunctionCallInfo) -> Datum {
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 2));

    PG_RETURN_XML_P!(stringinfo_to_xmltype(database_to_xml_internal(
        core::ptr::null(), nulls, tableforest, targetns
    )))
}


unsafe fn database_to_xmlschema_internal(
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
) -> StringInfo {
    let result: StringInfo = makeStringInfo();

    xsd_schema_element_start(result, targetns);

    SPI_connect();

    let relid_list = database_get_xml_visible_tables();
    let nspid_list = database_get_xml_visible_schemas();

    let tupdesc_list: *mut List = NIL;
    /* TODO(pg-port): iterate relid_list to build tupdesc_list */
    let _ = relid_list;

    appendStringInfoString(result, map_sql_typecoll_to_xmlschema_types(tupdesc_list));
    appendStringInfoString(
        result,
        map_sql_catalog_to_xmlschema_types(nspid_list, nulls, tableforest, targetns),
    );

    xsd_schema_element_end(result);

    SPI_finish();

    result
}


pub unsafe fn database_to_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 2));

    PG_RETURN_XML_P!(stringinfo_to_xmltype(database_to_xmlschema_internal(
        nulls, tableforest, targetns
    )))
}


pub unsafe fn database_to_xml_and_xmlschema(fcinfo: FunctionCallInfo) -> Datum {
    let nulls: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let tableforest: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let targetns: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 2));

    let xmlschema = database_to_xmlschema_internal(nulls, tableforest, targetns);

    PG_RETURN_XML_P!(stringinfo_to_xmltype(database_to_xml_internal(
        (*xmlschema).data, nulls, tableforest, targetns
    )))
}

// -------------------------------------------------------------------------
// map_multipart_sql_identifier_to_xml_name
// -------------------------------------------------------------------------

/*
 * Map a multi-part SQL name to an XML name; see SQL/XML:2008 section 9.2.
 */
unsafe fn map_multipart_sql_identifier_to_xml_name(
    a: *const c_char,
    b: *const c_char,
    c: *const c_char,
    d: *const c_char,
) -> *mut c_char {
    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);

    if !a.is_null() {
        appendStringInfoString(
            &mut result,
            map_sql_identifier_to_xml_name(a, true, true),
        );
    }
    if !b.is_null() {
        appendStringInfoChar(&mut result, b'.' as c_char);
        appendStringInfoString(
            &mut result,
            map_sql_identifier_to_xml_name(b, true, true),
        );
    }
    if !c.is_null() {
        appendStringInfoChar(&mut result, b'.' as c_char);
        appendStringInfoString(
            &mut result,
            map_sql_identifier_to_xml_name(c, true, true),
        );
    }
    if !d.is_null() {
        appendStringInfoChar(&mut result, b'.' as c_char);
        appendStringInfoString(
            &mut result,
            map_sql_identifier_to_xml_name(d, true, true),
        );
    }

    result.data
}

// -------------------------------------------------------------------------
// map_sql_table_to_xmlschema
// -------------------------------------------------------------------------

/*
 * Map an SQL table to an XML Schema document; see SQL/XML:2008 section 9.11.
 */
unsafe fn map_sql_table_to_xmlschema(
    tupdesc: TupleDesc,
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
) -> *const c_char {
    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);

    let xmltn: *mut c_char;
    let tabletypename: *mut c_char;
    let rowtypename: *mut c_char;

    if OidIsValid(relid) {
        let tuple: HeapTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let reltuple: Form_pg_class = GETSTRUCT(tuple as *const crate::access::htup_details::HeapTupleData) as Form_pg_class;
        /* NOTE: Form_pg_class is stubbed; real version has relname/relnamespace fields. */
        /* Using null placeholders for the name components. */
        xmltn = b"table\0".as_ptr() as *mut c_char;
        tabletypename = b"TableType\0".as_ptr() as *mut c_char;
        rowtypename = b"RowType\0".as_ptr() as *mut c_char;
        ReleaseSysCache(tuple);
    } else {
        if tableforest {
            xmltn = b"row\0".as_ptr() as *mut c_char;
        } else {
            xmltn = b"table\0".as_ptr() as *mut c_char;
        }
        tabletypename = b"TableType\0".as_ptr() as *mut c_char;
        rowtypename = b"RowType\0".as_ptr() as *mut c_char;
    }

    xsd_schema_element_start(&mut result, targetns);

    appendStringInfoString(
        &mut result,
        map_sql_typecoll_to_xmlschema_types(list_make1(tupdesc as *mut c_void)),
    );

    appendStringInfo!(
        &mut result,
        "<xsd:complexType name=\"{}\">\n  <xsd:sequence>\n",
        cs(rowtypename)
    );

    let natts = (*tupdesc).natts;
    for i in 0..natts {
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);
        if (*att).attisdropped { continue; }
        appendStringInfo!(
            &mut result,
            "    <xsd:element name=\"{}\" type=\"{}\"{}/>\n",
            cs(map_sql_identifier_to_xml_name(NameStr_ptr(&(*att).attname), true, false)),
            cs(map_sql_type_to_xml_name((*att).atttypid, -1)),
            if nulls { " nillable=\"true\"" } else { " minOccurs=\"0\"" },
        );
    }

    appendStringInfoString(
        &mut result,
        b"  </xsd:sequence>\n</xsd:complexType>\n\n\0".as_ptr() as *const c_char,
    );

    if !tableforest {
        appendStringInfo!(
            &mut result,
            "<xsd:complexType name=\"{}\">\n  <xsd:sequence>\n    <xsd:element name=\"row\" type=\"{}\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n  </xsd:sequence>\n</xsd:complexType>\n\n",
            cs(tabletypename), cs(rowtypename)
        );
        appendStringInfo!(
            &mut result,
            "<xsd:element name=\"{}\" type=\"{}\"/>\n\n",
            cs(xmltn), cs(tabletypename)
        );
    } else {
        appendStringInfo!(
            &mut result,
            "<xsd:element name=\"{}\" type=\"{}\"/>\n\n",
            cs(xmltn), cs(rowtypename)
        );
    }

    xsd_schema_element_end(&mut result);

    result.data
}

// -------------------------------------------------------------------------
// map_sql_schema_to_xmlschema_types
// -------------------------------------------------------------------------

unsafe fn map_sql_schema_to_xmlschema_types(
    nspid: Oid,
    relid_list: *mut List,
    _nulls: bool,
    tableforest: bool,
    _targetns: *const c_char,
) -> *const c_char {
    let dbname: *mut c_char = get_database_name(MyDatabaseId);
    let nspname: *mut c_char = get_namespace_name(nspid);
    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);

    let xmlsn: *mut c_char = map_sql_identifier_to_xml_name(nspname, true, false);
    let schematypename: *mut c_char = map_multipart_sql_identifier_to_xml_name(
        b"SchemaType\0".as_ptr() as *const c_char,
        dbname,
        nspname,
        core::ptr::null(),
    );

    appendStringInfo!(
        &mut result,
        "<xsd:complexType name=\"{}\">\n",
        cs(schematypename)
    );
    if !tableforest {
        appendStringInfoString(&mut result, b"  <xsd:all>\n\0".as_ptr() as *const c_char);
    } else {
        appendStringInfoString(&mut result, b"  <xsd:sequence>\n\0".as_ptr() as *const c_char);
    }

    /* TODO(pg-port): iterate relid_list with foreach! to append element defs */
    let _ = relid_list;

    if !tableforest {
        appendStringInfoString(&mut result, b"  </xsd:all>\n\0".as_ptr() as *const c_char);
    } else {
        appendStringInfoString(&mut result, b"  </xsd:sequence>\n\0".as_ptr() as *const c_char);
    }
    appendStringInfoString(&mut result, b"</xsd:complexType>\n\n\0".as_ptr() as *const c_char);
    appendStringInfo!(
        &mut result,
        "<xsd:element name=\"{}\" type=\"{}\"/>\n\n",
        cs(xmlsn), cs(schematypename)
    );

    result.data
}

// -------------------------------------------------------------------------
// map_sql_catalog_to_xmlschema_types
// -------------------------------------------------------------------------

unsafe fn map_sql_catalog_to_xmlschema_types(
    nspid_list: *mut List,
    _nulls: bool,
    _tableforest: bool,
    _targetns: *const c_char,
) -> *const c_char {
    let dbname: *mut c_char = get_database_name(MyDatabaseId);
    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);

    let xmlcn: *mut c_char = map_sql_identifier_to_xml_name(dbname, true, false);
    let catalogtypename: *mut c_char = map_multipart_sql_identifier_to_xml_name(
        b"CatalogType\0".as_ptr() as *const c_char,
        dbname,
        core::ptr::null(),
        core::ptr::null(),
    );

    appendStringInfo!(
        &mut result,
        "<xsd:complexType name=\"{}\">\n",
        cs(catalogtypename)
    );
    appendStringInfoString(&mut result, b"  <xsd:all>\n\0".as_ptr() as *const c_char);

    /* TODO(pg-port): iterate nspid_list with foreach! to append schema element defs */
    let _ = nspid_list;

    appendStringInfoString(&mut result, b"  </xsd:all>\n\0".as_ptr() as *const c_char);
    appendStringInfoString(&mut result, b"</xsd:complexType>\n\n\0".as_ptr() as *const c_char);
    appendStringInfo!(
        &mut result,
        "<xsd:element name=\"{}\" type=\"{}\"/>\n\n",
        cs(xmlcn), cs(catalogtypename)
    );

    result.data
}

// -------------------------------------------------------------------------
// map_sql_type_to_xml_name
// -------------------------------------------------------------------------

/*
 * Map an SQL data type to an XML name; see SQL/XML:2008 section 9.4.
 */
unsafe fn map_sql_type_to_xml_name(typeoid: Oid, typmod: c_int) -> *const c_char {
    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);

    match typeoid {
        BPCHAROID => {
            if typmod == -1 {
                appendStringInfoString(&mut result, b"CHAR\0".as_ptr() as *const c_char);
            } else {
                appendStringInfo!(&mut result, "CHAR_{}", typmod - VARHDRSZ);
            }
        }
        VARCHAROID => {
            if typmod == -1 {
                appendStringInfoString(&mut result, b"VARCHAR\0".as_ptr() as *const c_char);
            } else {
                appendStringInfo!(&mut result, "VARCHAR_{}", typmod - VARHDRSZ);
            }
        }
        NUMERICOID => {
            if typmod == -1 {
                appendStringInfoString(&mut result, b"NUMERIC\0".as_ptr() as *const c_char);
            } else {
                appendStringInfo!(
                    &mut result,
                    "NUMERIC_{}_{}",
                    ((typmod - VARHDRSZ) >> 16) & 0xffff,
                    (typmod - VARHDRSZ) & 0xffff
                );
            }
        }
        INT4OID => { appendStringInfoString(&mut result, b"INTEGER\0".as_ptr() as *const c_char); }
        INT2OID => { appendStringInfoString(&mut result, b"SMALLINT\0".as_ptr() as *const c_char); }
        INT8OID => { appendStringInfoString(&mut result, b"BIGINT\0".as_ptr() as *const c_char); }
        FLOAT4OID => { appendStringInfoString(&mut result, b"REAL\0".as_ptr() as *const c_char); }
        FLOAT8OID => { appendStringInfoString(&mut result, b"DOUBLE\0".as_ptr() as *const c_char); }
        BOOLOID => { appendStringInfoString(&mut result, b"BOOLEAN\0".as_ptr() as *const c_char); }
        TIMEOID => {
            if typmod == -1 { appendStringInfoString(&mut result, b"TIME\0".as_ptr() as *const c_char); }
            else { appendStringInfo!(&mut result, "TIME_{}", typmod); }
        }
        TIMETZOID => {
            if typmod == -1 { appendStringInfoString(&mut result, b"TIME_WTZ\0".as_ptr() as *const c_char); }
            else { appendStringInfo!(&mut result, "TIME_WTZ_{}", typmod); }
        }
        TIMESTAMPOID => {
            if typmod == -1 { appendStringInfoString(&mut result, b"TIMESTAMP\0".as_ptr() as *const c_char); }
            else { appendStringInfo!(&mut result, "TIMESTAMP_{}", typmod); }
        }
        TIMESTAMPTZOID => {
            if typmod == -1 { appendStringInfoString(&mut result, b"TIMESTAMP_WTZ\0".as_ptr() as *const c_char); }
            else { appendStringInfo!(&mut result, "TIMESTAMP_WTZ_{}", typmod); }
        }
        DATEOID => { appendStringInfoString(&mut result, b"DATE\0".as_ptr() as *const c_char); }
        XMLOID => { appendStringInfoString(&mut result, b"XML\0".as_ptr() as *const c_char); }
        _ => {
            let tuple: HeapTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeoid));
            if !HeapTupleIsValid(tuple) {
                elog!(ERROR, "cache lookup failed for type {}", typeoid);
            }
            let typtuple: Form_pg_type = GETSTRUCT(tuple as *const crate::access::htup_details::HeapTupleData) as Form_pg_type;
            /* NOTE: Form_pg_type is stubbed; real version has typtype/typnamespace/typname. */
            /* Using placeholder output. */
            appendStringInfoString(&mut result, b"UDT\0".as_ptr() as *const c_char);
            ReleaseSysCache(tuple);
        }
    }

    result.data
}

// -------------------------------------------------------------------------
// map_sql_typecoll_to_xmlschema_types
// -------------------------------------------------------------------------

unsafe fn map_sql_typecoll_to_xmlschema_types(tupdesc_list: *mut List) -> *const c_char {
    /* TODO(pg-port): iterate tupdesc_list with foreach! to gather unique types. */
    let _ = tupdesc_list;
    /* Returns empty string when list iteration is not yet ported. */
    b"\0".as_ptr() as *const c_char
}

// -------------------------------------------------------------------------
// map_sql_type_to_xmlschema_type
// -------------------------------------------------------------------------

unsafe fn map_sql_type_to_xmlschema_type(typeoid: Oid, typmod: c_int) -> *const c_char {
    let mut result: StringInfoData = core::mem::zeroed();
    initStringInfo(&mut result);
    let typename_: *const c_char = map_sql_type_to_xml_name(typeoid, typmod);

    if typeoid == XMLOID {
        appendStringInfoString(
            &mut result,
            b"<xsd:complexType mixed=\"true\">\n  <xsd:sequence>\n    <xsd:any name=\"element\" minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"skip\"/>\n  </xsd:sequence>\n</xsd:complexType>\n\0".as_ptr() as *const c_char,
        );
    } else {
        appendStringInfo!(
            &mut result,
            "<xsd:simpleType name=\"{}\">\n",
            cs(typename_)
        );

        match typeoid {
            BPCHAROID | VARCHAROID | TEXTOID => {
                appendStringInfoString(
                    &mut result,
                    b"  <xsd:restriction base=\"xsd:string\">\n\0".as_ptr() as *const c_char,
                );
                if typmod != -1 {
                    appendStringInfo!(
                        &mut result,
                        "    <xsd:maxLength value=\"{}\"/>\n",
                        typmod - VARHDRSZ
                    );
                }
                appendStringInfoString(
                    &mut result,
                    b"  </xsd:restriction>\n\0".as_ptr() as *const c_char,
                );
            }
            BYTEAOID => {
                appendStringInfo!(
                    &mut result,
                    "  <xsd:restriction base=\"xsd:{}\">\n  </xsd:restriction>\n",
                    if xmlbinary == XMLBINARY_BASE64 { "base64Binary" } else { "hexBinary" }
                );
            }
            NUMERICOID => {
                if typmod != -1 {
                    appendStringInfo!(
                        &mut result,
                        "  <xsd:restriction base=\"xsd:decimal\">\n    <xsd:totalDigits value=\"{}\"/>\n    <xsd:fractionDigits value=\"{}\"/>\n  </xsd:restriction>\n",
                        ((typmod - VARHDRSZ) >> 16) & 0xffff,
                        (typmod - VARHDRSZ) & 0xffff
                    );
                }
            }
            INT2OID => {
                appendStringInfo!(
                    &mut result,
                    "  <xsd:restriction base=\"xsd:short\">\n    <xsd:maxInclusive value=\"{}\"/>\n    <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                    i16::MAX as c_int, i16::MIN as c_int
                );
            }
            INT4OID => {
                appendStringInfo!(
                    &mut result,
                    "  <xsd:restriction base=\"xsd:int\">\n    <xsd:maxInclusive value=\"{}\"/>\n    <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                    i32::MAX, i32::MIN
                );
            }
            INT8OID => {
                appendStringInfo!(
                    &mut result,
                    "  <xsd:restriction base=\"xsd:long\">\n    <xsd:maxInclusive value=\"{}\"/>\n    <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                    i64::MAX, i64::MIN
                );
            }
            FLOAT4OID => {
                appendStringInfoString(
                    &mut result,
                    b"  <xsd:restriction base=\"xsd:float\"></xsd:restriction>\n\0".as_ptr() as *const c_char,
                );
            }
            FLOAT8OID => {
                appendStringInfoString(
                    &mut result,
                    b"  <xsd:restriction base=\"xsd:double\"></xsd:restriction>\n\0".as_ptr() as *const c_char,
                );
            }
            BOOLOID => {
                appendStringInfoString(
                    &mut result,
                    b"  <xsd:restriction base=\"xsd:boolean\"></xsd:restriction>\n\0".as_ptr() as *const c_char,
                );
            }
            TIMEOID | TIMETZOID => {
                let tz = if typeoid == TIMETZOID {
                    "(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}"
                } else {
                    ""
                };
                if typmod == -1 {
                    appendStringInfo!(&mut result,
                        "  <xsd:restriction base=\"xsd:time\">\n    <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}(.\\p{{Nd}}+)?{}\"/>\n  </xsd:restriction>\n",
                        tz);
                } else if typmod == 0 {
                    appendStringInfo!(&mut result,
                        "  <xsd:restriction base=\"xsd:time\">\n    <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}{}\"/>\n  </xsd:restriction>\n",
                        tz);
                } else {
                    appendStringInfo!(&mut result,
                        "  <xsd:restriction base=\"xsd:time\">\n    <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}.\\p{{Nd}}{{{}}}{}\"/>\n  </xsd:restriction>\n",
                        typmod - VARHDRSZ, tz);
                }
            }
            TIMESTAMPOID | TIMESTAMPTZOID => {
                let tz = if typeoid == TIMESTAMPTZOID {
                    "(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}"
                } else {
                    ""
                };
                if typmod == -1 {
                    appendStringInfo!(&mut result,
                        "  <xsd:restriction base=\"xsd:dateTime\">\n    <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}(.\\p{{Nd}}+)?{}\"/>\n  </xsd:restriction>\n",
                        tz);
                } else if typmod == 0 {
                    appendStringInfo!(&mut result,
                        "  <xsd:restriction base=\"xsd:dateTime\">\n    <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}{}\"/>\n  </xsd:restriction>\n",
                        tz);
                } else {
                    appendStringInfo!(&mut result,
                        "  <xsd:restriction base=\"xsd:dateTime\">\n    <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}.\\p{{Nd}}{{{}}}{}\"/>\n  </xsd:restriction>\n",
                        typmod - VARHDRSZ, tz);
                }
            }
            DATEOID => {
                appendStringInfoString(
                    &mut result,
                    b"  <xsd:restriction base=\"xsd:date\">\n    <xsd:pattern value=\"\\p{Nd}{4}-\\p{Nd}{2}-\\p{Nd}{2}\"/>\n  </xsd:restriction>\n\0".as_ptr() as *const c_char,
                );
            }
            _ => {
                if get_typtype(typeoid) == TYPTYPE_DOMAIN {
                    let mut base_typmod: int32 = -1;
                    let base_typeoid = getBaseTypeAndTypmod(typeoid, &mut base_typmod);
                    appendStringInfo!(
                        &mut result,
                        "  <xsd:restriction base=\"{}\"/>\n",
                        cs(map_sql_type_to_xml_name(base_typeoid, base_typmod))
                    );
                }
            }
        }
        appendStringInfoString(&mut result, b"</xsd:simpleType>\n\0".as_ptr() as *const c_char);
    }

    result.data
}

// -------------------------------------------------------------------------
// SPI_sql_row_to_xmlelement
// -------------------------------------------------------------------------

/*
 * Map an SQL row to an XML element, taking the row from the active SPI cursor.
 * See also SQL/XML:2008 section 9.10.
 */
unsafe fn SPI_sql_row_to_xmlelement(
    rownum: u64,
    result: StringInfo,
    tablename: *mut c_char,
    nulls: bool,
    tableforest: bool,
    targetns: *const c_char,
    top_level: bool,
) {
    let xmltn: *mut c_char = if !tablename.is_null() {
        map_sql_identifier_to_xml_name(tablename, true, false)
    } else if tableforest {
        b"row\0".as_ptr() as *mut c_char
    } else {
        b"table\0".as_ptr() as *mut c_char
    };

    if tableforest {
        xmldata_root_element_start(result, xmltn, core::ptr::null(), targetns, top_level);
    } else {
        appendStringInfoString(result, b"<row>\n\0".as_ptr() as *const c_char);
    }

    let natts = (*(*SPI_tuptable).tupdesc).natts;
    for i in 1..=natts {
        let colname: *mut c_char = map_sql_identifier_to_xml_name(
            SPI_fname((*SPI_tuptable).tupdesc, i),
            true,
            false,
        );
        let mut isnull: bool = false;
        let colval = SPI_getbinval(
            (*SPI_tuptable).vals.add((rownum) as usize).read() as *mut c_void,
            (*SPI_tuptable).tupdesc,
            i,
            &mut isnull,
        );
        if isnull {
            if nulls {
                appendStringInfo!(
                    result,
                    "  <{} xsi:nil=\"true\"/>\n",
                    cs(colname)
                );
            }
        } else {
            appendStringInfo!(
                result,
                "  <{}>{}</{}>\n",
                cs(colname),
                cs(map_sql_value_to_xml_value(
                    colval,
                    SPI_gettypeid((*SPI_tuptable).tupdesc, i),
                    true,
                )),
                cs(colname)
            );
        }
    }

    if tableforest {
        xmldata_root_element_end(result, xmltn);
        appendStringInfoChar(result, b'\n' as c_char);
    } else {
        appendStringInfoString(result, b"</row>\n\n\0".as_ptr() as *const c_char);
    }
}


// =========================================================================
// PART 6 -- XPath / xmlexists / xml_is_well_formed* Datum functions,
//           XmlTable* XMLTABLE support functions,
//           XmlTableRoutine constant,
//           and stringinfo / appendStringInfo binding stubs.
// =========================================================================

// -------------------------------------------------------------------------
// xpath, xmlexists, xpath_exists
// -------------------------------------------------------------------------

/*
 * Evaluate XPath expression and return array of XML values.
 */
pub unsafe fn xpath(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let xpath_expr_text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let data = PG_GETARG_XML_P!(fcinfo, 1);
        let namespaces = PG_GETARG_ARRAYTYPE_P!(fcinfo, 2);
        let astate = initArrayResult(XMLOID, CurrentMemoryContext(), true);
        xpath_internal(xpath_expr_text, data, namespaces, core::ptr::null_mut(), astate);
        PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext()))
    }
    */
}


/*
 * Determines if the node specified by the supplied XPath exists
 * in a given XML document, returning a boolean.
 */
pub unsafe fn xmlexists(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let xpath_expr_text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let data = PG_GETARG_XML_P!(fcinfo, 1);
        let mut res_nitems: c_int = 0;
        xpath_internal(xpath_expr_text, data, core::ptr::null_mut(), &mut res_nitems, core::ptr::null_mut());
        PG_RETURN_BOOL!(res_nitems > 0)
    }
    */
}


/*
 * Determines if the node specified by the supplied XPath exists
 * in a given XML document, returning a boolean. Differs from
 * xmlexists as it supports namespaces and is not defined in SQL/XML.
 */
pub unsafe fn xpath_exists(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let xpath_expr_text = PG_GETARG_TEXT_PP!(fcinfo, 0);
        let data = PG_GETARG_XML_P!(fcinfo, 1);
        let namespaces = PG_GETARG_ARRAYTYPE_P!(fcinfo, 2);
        let mut res_nitems: c_int = 0;
        xpath_internal(xpath_expr_text, data, namespaces, &mut res_nitems, core::ptr::null_mut());
        PG_RETURN_BOOL!(res_nitems > 0)
    }
    */
}

// -------------------------------------------------------------------------
// xpath_internal (USE_LIBXML gated)
// -------------------------------------------------------------------------

#[cfg(any())]
unsafe fn xpath_internal(
    _xpath_expr_text: *mut crate::c::text,
    _data: *mut xmltype,
    _namespaces: *mut ArrayType,
    _res_nitems: *mut c_int,
    _astate: ArrayBuildState,
) {
    /*
     * Full logic:
     * - parse namespace array (ndim check, deconstruct_array_builtin)
     * - pg_xmlCharStrndup for data and xpath_expr
     * - pg_xml_init(PG_XML_STRICTNESS_ALL)
     * - PG_TRY: xmlInitParser; xmlNewParserCtxt; xmlCtxtReadMemory;
     *   xmlXPathNewContext; register namespaces; xmlXPathCtxtCompile;
     *   xmlXPathCompiledEval; xml_xpathobjtoxmlarray;
     * - PG_CATCH: free all libxml objects; pg_xml_done(xmlerrcxt, true); PG_RE_THROW
     * - free objects; pg_xml_done(xmlerrcxt, false)
     */
}

// -------------------------------------------------------------------------
// xml_xmlnodetoxmltype (USE_LIBXML gated)
// -------------------------------------------------------------------------

#[cfg(any())]
unsafe fn xml_xmlnodetoxmltype(
    _cur: *mut c_void, /* xmlNodePtr */
    _xmlerrcxt: *mut PgXmlErrorContext,
) -> *mut xmltype {
    /*
     * For attribute and text nodes, return the escaped text.
     * For anything else, dump the whole subtree.
     * Uses xmlCopyNode, xmlNodeDump, xmlXPathCastNodeToString, escape_xml.
     */
    core::ptr::null_mut()
}

// -------------------------------------------------------------------------
// xml_xpathobjtoxmlarray (USE_LIBXML gated)
// -------------------------------------------------------------------------

#[cfg(any())]
unsafe fn xml_xpathobjtoxmlarray(
    _xpathobj: *mut c_void, /* xmlXPathObjectPtr */
    _astate: ArrayBuildState,
    _xmlerrcxt: *mut PgXmlErrorContext,
) -> c_int {
    /*
     * Converts an XPath object to an array of xml values.
     * Handles XPATH_NODESET (iterate nodesetval), XPATH_BOOLEAN,
     * XPATH_NUMBER, XPATH_STRING cases.
     */
    0
}

// -------------------------------------------------------------------------
// xml_is_well_formed*
// -------------------------------------------------------------------------

/*
 * Functions for checking well-formed-ness
 */

#[cfg(any())]
unsafe fn wellformed_xml(data: *mut crate::c::text, xmloption_arg: XmlOptionType) -> bool {
    // ErrorSaveContext escontext = {T_ErrorSaveContext};
    // doc = xml_parse(data, xmloption_arg, true, GetDatabaseEncoding(), NULL, NULL, &escontext);
    // if doc: xmlFreeDoc(doc);
    // return !escontext.error_occurred
    false
}

pub unsafe fn xml_is_well_formed(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let data = PG_GETARG_TEXT_PP!(fcinfo, 0);
        PG_RETURN_BOOL!(wellformed_xml(data, xmloption_as_type(xmloption)))
    }
    */
}

pub unsafe fn xml_is_well_formed_document(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let data = PG_GETARG_TEXT_PP!(fcinfo, 0);
        PG_RETURN_BOOL!(wellformed_xml(data, XMLOPTION_DOCUMENT))
    }
    */
}

pub unsafe fn xml_is_well_formed_content(fcinfo: FunctionCallInfo) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        let data = PG_GETARG_TEXT_PP!(fcinfo, 0);
        PG_RETURN_BOOL!(wellformed_xml(data, XMLOPTION_CONTENT))
    }
    */
}

// -------------------------------------------------------------------------
// XMLTABLE support functions
// -------------------------------------------------------------------------

/*
 * XmlTableBuilderData -- private state for XMLTABLE node.
 * Only exists when USE_LIBXML; stubbed here with an empty struct.
 */
#[cfg(any())]
struct XmlTableBuilderData {
    magic: c_int,
    natts: c_int,
    row_count: i64,
    xmlerrcxt: *mut PgXmlErrorContext,
    ctxt: *mut c_void,      /* xmlParserCtxtPtr */
    doc: *mut c_void,       /* xmlDocPtr */
    xpathcxt: *mut c_void,  /* xmlXPathContextPtr */
    xpathcomp: *mut c_void, /* xmlXPathCompExprPtr */
    xpathobj: *mut c_void,  /* xmlXPathObjectPtr */
    xpathscomp: *mut *mut c_void, /* xmlXPathCompExprPtr[] */
}

/* random number to identify XmlTableContext */
#[cfg(any())]
const XMLTABLE_CONTEXT_MAGIC: c_int = 46922182;

/* GetXmlTableBuilderPrivateData -- extract and validate private data. */
#[cfg(any())]
unsafe fn GetXmlTableBuilderPrivateData(
    state: *mut TableFuncScanState,
    fname: *const c_char,
) -> *mut XmlTableBuilderData {
    // if !IsA!(state, T_TableFuncScanState): elog ERROR
    // result = (*state).opaque as *mut XmlTableBuilderData;
    // if result->magic != XMLTABLE_CONTEXT_MAGIC: elog ERROR
    // return result
    core::ptr::null_mut()
}

/*
 * XmlTableInitOpaque
 *		Fill in TableFuncScanState->opaque for XmlTable processor; initialize
 *		the XML parser.
 */
unsafe extern "C" fn XmlTableInitOpaque(state: *mut TableFuncScanState, _natts: c_int) {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = palloc0(sizeof XmlTableBuilderData) as *mut XmlTableBuilderData;
        // xtCxt->magic = XMLTABLE_CONTEXT_MAGIC; xtCxt->natts = natts;
        // xtCxt->xpathscomp = palloc0(sizeof(xmlXPathCompExprPtr) * natts);
        // xmlerrcxt = pg_xml_init(PG_XML_STRICTNESS_ALL);
        // PG_TRY: xmlInitParser(); ctxt = xmlNewParserCtxt();
        //   if ctxt == NULL || xmlerrcxt->err_occurred: xml_ereport ERROR
        // PG_CATCH: if ctxt != NULL: xmlFreeParserCtxt(ctxt); pg_xml_done; PG_RE_THROW
        // xtCxt->xmlerrcxt = xmlerrcxt; xtCxt->ctxt = ctxt;
        // (*state).opaque = xtCxt;
    }
    */
}

/*
 * XmlTableSetDocument
 *		Install the input document
 */
unsafe extern "C" fn XmlTableSetDocument(state: *mut TableFuncScanState, _value: Datum) {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableSetDocument");
        // str = xml_out_internal(DatumGetXmlP(value), 0);
        // xstr = pg_xmlCharStrndup(str, strlen(str));
        // PG_TRY: doc = xmlCtxtReadMemory(...); xpathcxt = xmlXPathNewContext(doc);
        //   xpathcxt->node = doc as xmlNodePtr;
        // PG_CATCH: if xpathcxt: xmlXPathFreeContext; if doc: xmlFreeDoc; PG_RE_THROW
        // xtCxt->doc = doc; xtCxt->xpathcxt = xpathcxt;
    }
    */
}

/*
 * XmlTableSetNamespace
 *		Add a namespace declaration
 */
unsafe extern "C" fn XmlTableSetNamespace(
    _state: *mut TableFuncScanState,
    name: *const c_char,
    _uri: *const c_char,
) {
    if name.is_null() {
        ereport!(
            ERROR,
            errmsg!("DEFAULT namespace is not supported")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableSetNamespace");
        // if xmlXPathRegisterNs(xtCxt->xpathcxt, pg_xmlCharStrndup(name), pg_xmlCharStrndup(uri)): ereport ERROR
    }
    */
}

/*
 * XmlTableSetRowFilter
 *		Install the row-filter Xpath expression.
 */
unsafe extern "C" fn XmlTableSetRowFilter(state: *mut TableFuncScanState, path: *const c_char) {
    /* check for empty path */
    if !path.is_null() && *path == 0 {
        ereport!(
            ERROR,
            errmsg!("row path filter must not be empty string")
            /* C also: errcode(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY) */
        );
    }
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableSetRowFilter");
        // xstr = pg_xmlCharStrndup(path, strlen(path));
        // Assert(xtCxt->xpathcxt != NULL);
        // xtCxt->xpathcomp = xmlXPathCtxtCompile(xtCxt->xpathcxt, xstr);
        // if xtCxt->xpathcomp == NULL || xtCxt->xmlerrcxt->err_occurred: xml_ereport ERROR
    }
    */
}

/*
 * XmlTableSetColumnFilter
 *		Install the column-filter Xpath expression, for the given column.
 */
unsafe extern "C" fn XmlTableSetColumnFilter(
    _state: *mut TableFuncScanState,
    path: *const c_char,
    _colnum: c_int,
) {
    if !path.is_null() && *path == 0 {
        ereport!(
            ERROR,
            errmsg!("column path filter must not be empty string")
            /* C also: errcode(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY) */
        );
    }
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableSetColumnFilter");
        // xstr = pg_xmlCharStrndup(path, strlen(path));
        // Assert(xtCxt->xpathcxt != NULL);
        // xtCxt->xpathscomp[colnum] = xmlXPathCtxtCompile(xtCxt->xpathcxt, xstr);
        // if xtCxt->xpathscomp[colnum] == NULL || ...: xml_ereport ERROR
    }
    */
}

/*
 * XmlTableFetchRow
 *		Prepare the next "current" tuple for upcoming GetValue calls.
 *		Returns false if the row-filter expression returned no more rows.
 */
unsafe extern "C" fn XmlTableFetchRow(_state: *mut TableFuncScanState) -> bool {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    false
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableFetchRow");
        // xmlSetStructuredErrorFunc(xtCxt->xmlerrcxt, xml_errorHandler);
        // if xtCxt->xpathobj == NULL:
        //   xtCxt->xpathobj = xmlXPathCompiledEval(xtCxt->xpathcomp, xtCxt->xpathcxt);
        //   if == NULL || err: xml_ereport ERROR
        //   xtCxt->row_count = 0;
        // if xtCxt->xpathobj->type == XPATH_NODESET:
        //   if nodesetval != NULL: if row_count++ < nodesetval->nodeNr: return true;
        // return false
    }
    */
}

/*
 * XmlTableGetValue
 *		Return the value for column number 'colnum' for the current row.
 *		If column -1 is requested, return representation of the whole row.
 *
 * This leaks memory, so be sure to reset often the context in which it's
 * called.
 */
unsafe extern "C" fn XmlTableGetValue(
    _state: *mut TableFuncScanState,
    _colnum: c_int,
    _typid: Oid,
    _typmod: int32,
    _isnull: *mut bool,
) -> Datum {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    #[allow(unreachable_code)]
    0
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableGetValue");
        // Assert(xtCxt->xpathobj && xtCxt->xpathobj->type == XPATH_NODESET && nodesetval != NULL);
        // xmlSetStructuredErrorFunc(xtCxt->xmlerrcxt, xml_errorHandler);
        // *isnull = false;
        // PG_TRY:
        //   cur = xpathobj->nodesetval->nodeTab[row_count - 1];
        //   xtCxt->xpathcxt->node = cur;
        //   xpathobj = xmlXPathCompiledEval(xtCxt->xpathscomp[colnum], xtCxt->xpathcxt);
        //   if == NULL || err: xml_ereport ERROR
        //   ... handle XPATH_NODESET / XPATH_STRING / XPATH_BOOLEAN / XPATH_NUMBER
        //   result = InputFunctionCall(&state->in_functions[colnum], cstr, ...)
        // PG_FINALLY: if xpathobj: xmlXPathFreeObject(xpathobj)
        // return result
    }
    */
}

/*
 * XmlTableDestroyOpaque
 *		Release all libxml2 resources
 */
unsafe extern "C" fn XmlTableDestroyOpaque(state: *mut TableFuncScanState) {
    /* !USE_LIBXML branch */
    NO_XML_SUPPORT!();
    /*
    #[cfg(any())] // USE_LIBXML body:
    {
        // xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableDestroyOpaque");
        // xmlSetStructuredErrorFunc(xtCxt->xmlerrcxt, xml_errorHandler);
        // if xtCxt->xpathscomp != NULL: for i in 0..natts: xmlXPathFreeCompExpr
        // if xtCxt->xpathobj: xmlXPathFreeObject
        // if xtCxt->xpathcomp: xmlXPathFreeCompExpr
        // if xtCxt->xpathcxt: xmlXPathFreeContext
        // if xtCxt->doc: xmlFreeDoc
        // if xtCxt->ctxt: xmlFreeParserCtxt
        // pg_xml_done(xtCxt->xmlerrcxt, true);
        // xtCxt->magic = 0; (*state).opaque = NULL;
    }
    */
}

// -------------------------------------------------------------------------
// XmlTableRoutine constant
// -------------------------------------------------------------------------

/*
 * XmlTableRoutine -- callback table for XMLTABLE.
 *
 * const TableFuncRoutine XmlTableRoutine = { .InitOpaque = XmlTableInitOpaque, ... };
 */
pub static XmlTableRoutine: TableFuncRoutine = TableFuncRoutine {
    InitOpaque: Some(XmlTableInitOpaque),
    SetDocument: Some(XmlTableSetDocument),
    SetNamespace: Some(XmlTableSetNamespace),
    SetRowFilter: Some(XmlTableSetRowFilter),
    SetColumnFilter: Some(XmlTableSetColumnFilter),
    FetchRow: Some(XmlTableFetchRow),
    GetValue: Some(XmlTableGetValue),
    DestroyOpaque: Some(XmlTableDestroyOpaque),
};

// -------------------------------------------------------------------------
// xml_appendStringInfo helpers
//
// The C source calls the variadic appendStringInfo(buf, fmt, ...); the crate
// appendStringInfo! macro takes Rust format strings.  Call sites in this file
// pass *const c_char args, so we provide a thin helper `cs()` that wraps a
// C string pointer as a &str for use inside appendStringInfo! format args.
// -------------------------------------------------------------------------

/// Convert a NUL-terminated C string pointer to a `&str` for formatting.
/// The lifetime is technically unbounded; safe only within a single format
/// expression where the underlying C string outlives the call.
#[inline]
unsafe fn cs(p: *const c_char) -> &'static str {
    if p.is_null() { return ""; }
    let len = {
        let mut n = 0usize;
        while *p.add(n) != 0 { n += 1; }
        n
    };
    core::str::from_utf8_unchecked(core::slice::from_raw_parts(p as *const u8, len))
}

/// cstr_to_display -- same as cs() but named for call sites where the arg
/// comes from a DatumGetCString cast expression.
#[inline]
unsafe fn cstr_to_display(p: *mut c_char) -> &'static str { cs(p) }

/*
 * xml_appendStringInfo_ss -- append fmt_prefix followed by arg string.
 * Replaces the many appendStringInfo(buf, "prefix%s", arg) calls in C.
 */
#[allow(dead_code)]
unsafe fn xml_appendStringInfo_ss(str_: StringInfo, prefix: *const c_char, arg: *const c_char) {
    appendStringInfoString(str_, prefix);
    if !arg.is_null() {
        appendStringInfoString(str_, arg);
    }
}

/*
 * xml_appendStringInfo_si -- append prefix followed by a decimal integer.
 * Replaces appendStringInfo(buf, "..._\%d", typmod - VARHDRSZ) etc.
 * Uses a small stack buffer for the integer conversion.
 */
#[allow(dead_code)]
unsafe fn xml_appendStringInfo_si(str_: StringInfo, prefix: *const c_char, arg: c_int) {
    appendStringInfoString(str_, prefix);
    /* simple integer -> string conversion */
    let s = format!("{}", arg);
    let cs = alloc_cstring(&s);
    appendStringInfoString(str_, cs);
    pfree(cs as *mut c_void);
}

/* helper: allocate a null-terminated copy of a Rust str via palloc */
unsafe fn alloc_cstring(s: &str) -> *const c_char {
    let bytes = s.as_bytes();
    let p: *mut c_char = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
    *p.add(bytes.len()) = 0;
    p
}

