//! utils/builtins.h - Declarations for operations on built-in types.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{bits16, int16, int2vector, oidvector, text, uint32, uint64, Name, Size};
use crate::nodes::nodes::Node;
use crate::postgres::{Datum, DatumGetPointer, PointerGetDatum};
use crate::postgres_ext::Oid;
use crate::utils::fmgr::{fmStringInfo, FunctionCallInfo};
use crate::utils::mmgr::memnodes::MemoryContext;

// int32/int64 used in numutils prototypes.
use crate::c::{int32, int64};

/* Sign + the most decimal digits an 8-byte number could have */
pub const MAXINT8LEN: c_int = 20;

/* bool.c */
pub unsafe fn parse_bool(value: *const c_char, result: *mut bool) -> bool {
    unimplemented!()
}
pub unsafe fn parse_bool_with_len(value: *const c_char, len: Size, result: *mut bool) -> bool {
    unimplemented!()
}

/* domains.c */
pub unsafe fn domain_check(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mcxt: MemoryContext,
) {
    unimplemented!()
}
pub unsafe fn domain_check_safe(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mcxt: MemoryContext,
    escontext: *mut Node,
) -> bool {
    unimplemented!()
}
pub unsafe fn errdatatype(datatypeOid: Oid) -> c_int {
    unimplemented!()
}
pub unsafe fn errdomainconstraint(datatypeOid: Oid, conname: *const c_char) -> c_int {
    unimplemented!()
}

/* encode.c */
pub unsafe fn hex_encode(src: *const c_char, len: Size, dst: *mut c_char) -> uint64 {
    unimplemented!()
}
pub unsafe fn hex_decode(src: *const c_char, len: Size, dst: *mut c_char) -> uint64 {
    unimplemented!()
}
pub unsafe fn hex_decode_safe(
    src: *const c_char,
    len: Size,
    dst: *mut c_char,
    escontext: *mut Node,
) -> uint64 {
    unimplemented!()
}

/* int.c */
pub unsafe fn buildint2vector(int2s: *const int16, n: c_int) -> *mut int2vector {
    unimplemented!()
}

/* name.c */
pub unsafe fn namestrcpy(name: Name, str: *const c_char) {
    unimplemented!()
}
pub unsafe fn namestrcmp(name: Name, str: *const c_char) -> c_int {
    unimplemented!()
}

/* numutils.c */
pub unsafe fn pg_strtoint16(s: *const c_char) -> int16 {
    unimplemented!()
}
pub unsafe fn pg_strtoint16_safe(s: *const c_char, escontext: *mut Node) -> int16 {
    unimplemented!()
}
pub unsafe fn pg_strtoint32(s: *const c_char) -> int32 {
    unimplemented!()
}
pub unsafe fn pg_strtoint32_safe(s: *const c_char, escontext: *mut Node) -> int32 {
    unimplemented!()
}
pub unsafe fn pg_strtoint64(s: *const c_char) -> int64 {
    unimplemented!()
}
pub unsafe fn pg_strtoint64_safe(s: *const c_char, escontext: *mut Node) -> int64 {
    unimplemented!()
}
pub unsafe fn uint32in_subr(
    s: *const c_char,
    endloc: *mut *mut c_char,
    typname: *const c_char,
    escontext: *mut Node,
) -> uint32 {
    unimplemented!()
}
pub unsafe fn uint64in_subr(
    s: *const c_char,
    endloc: *mut *mut c_char,
    typname: *const c_char,
    escontext: *mut Node,
) -> uint64 {
    unimplemented!()
}
pub unsafe fn pg_itoa(i: int16, a: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_ultoa_n(value: uint32, a: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_ulltoa_n(value: uint64, a: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_ltoa(value: int32, a: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_lltoa(value: int64, a: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_ultostr_zeropad(str: *mut c_char, value: uint32, minwidth: int32) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn pg_ultostr(str: *mut c_char, value: uint32) -> *mut c_char {
    unimplemented!()
}

/* oid.c */
pub unsafe fn buildoidvector(oids: *const Oid, n: c_int) -> *mut oidvector {
    unimplemented!()
}
pub unsafe fn check_valid_oidvector(oidArray: *const oidvector) {
    unimplemented!()
}
pub unsafe fn oidparse(node: *mut Node) -> Oid {
    unimplemented!()
}
pub unsafe fn oid_cmp(p1: *const c_void, p2: *const c_void) -> c_int {
    unimplemented!()
}

/* regexp.c */
pub unsafe fn regexp_fixed_prefix(
    text_re: *mut text,
    case_insensitive: bool,
    collation: Oid,
    exact: *mut bool,
) -> *mut c_char {
    unimplemented!()
}

/* ruleutils.c */
// extern PGDLLIMPORT bool quote_all_identifiers;
#[no_mangle]
pub static mut quote_all_identifiers: bool = false;
pub unsafe fn quote_identifier(ident: *const c_char) -> *const c_char {
    unimplemented!()
}
pub unsafe fn quote_qualified_identifier(
    qualifier: *const c_char,
    ident: *const c_char,
) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn generate_operator_clause(
    buf: fmStringInfo,
    leftop: *const c_char,
    leftoptype: Oid,
    opoid: Oid,
    rightop: *const c_char,
    rightoptype: Oid,
) {
    unimplemented!()
}

/* varchar.c */
pub unsafe fn bpchartruelen(s: *mut c_char, len: c_int) -> c_int {
    unimplemented!()
}

/* popular functions from varlena.c */
pub unsafe fn cstring_to_text(s: *const c_char) -> *mut text {
    unimplemented!()
}
pub unsafe fn cstring_to_text_with_len(s: *const c_char, len: c_int) -> *mut text {
    unimplemented!()
}
pub unsafe fn text_to_cstring(t: *const text) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn text_to_cstring_buffer(src: *const text, dst: *mut c_char, dst_len: Size) {
    unimplemented!()
}

// #define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#[inline]
pub unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    PointerGetDatum(cstring_to_text(s) as *const c_void)
}
// #define TextDatumGetCString(d) text_to_cstring((text *) DatumGetPointer(d))
#[inline]
pub unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    text_to_cstring(DatumGetPointer(d) as *const text)
}

/* xid.c */
pub unsafe fn xidComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    unimplemented!()
}
pub unsafe fn xidLogicalComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    unimplemented!()
}

/* inet_cidr_ntop.c */
pub unsafe fn pg_inet_cidr_ntop(
    af: c_int,
    src: *const c_void,
    bits: c_int,
    dst: *mut c_char,
    size: Size,
) -> *mut c_char {
    unimplemented!()
}

/* inet_net_pton.c */
pub unsafe fn pg_inet_net_pton(af: c_int, src: *const c_char, dst: *mut c_void, size: Size) -> c_int {
    unimplemented!()
}

/* network.c */
pub unsafe fn convert_network_to_scalar(value: Datum, typid: Oid, failure: *mut bool) -> f64 {
    unimplemented!()
}
pub unsafe fn network_scan_first(input: Datum) -> Datum {
    unimplemented!()
}
pub unsafe fn network_scan_last(input: Datum) -> Datum {
    unimplemented!()
}
pub unsafe fn clean_ipv6_addr(addr_family: c_int, addr: *mut c_char) {
    unimplemented!()
}

/* numeric.c */
pub unsafe fn numeric_float8_no_overflow(fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!()
}

/* format_type.c */

/* Control flags for format_type_extended */
pub const FORMAT_TYPE_TYPEMOD_GIVEN: c_int = 0x01; /* typemod defined by caller */
pub const FORMAT_TYPE_ALLOW_INVALID: c_int = 0x02; /* allow invalid types */
pub const FORMAT_TYPE_FORCE_QUALIFY: c_int = 0x04; /* force qualification of type */
pub const FORMAT_TYPE_INVALID_AS_NULL: c_int = 0x08; /* NULL if undefined */
pub unsafe fn format_type_extended(type_oid: Oid, typemod: int32, flags: bits16) -> *mut c_char {
    unimplemented!()
}

pub unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn format_type_be_qualified(type_oid: Oid) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn format_type_with_typemod(type_oid: Oid, typemod: int32) -> *mut c_char {
    unimplemented!()
}

pub unsafe fn type_maximum_size(type_oid: Oid, typemod: int32) -> int32 {
    unimplemented!()
}

/* quote.c */
pub unsafe fn quote_literal_cstr(rawstr: *const c_char) -> *mut c_char {
    unimplemented!()
}
