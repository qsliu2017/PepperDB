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
    crate::utils::adt::bool::parse_bool(value as _, result as _) as _
}
pub unsafe fn parse_bool_with_len(value: *const c_char, len: Size, result: *mut bool) -> bool {
    crate::utils::adt::bool::parse_bool_with_len(value as _, len as _, result as _) as _
}

/* domains.c */
pub unsafe fn domain_check(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mcxt: MemoryContext,
) {
    crate::utils::adt::domains::domain_check(value as _, isnull as _, domainType as _, extra as _, mcxt as _)
}
pub unsafe fn domain_check_safe(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mcxt: MemoryContext,
    escontext: *mut Node,
) -> bool {
    crate::utils::adt::domains::domain_check_safe(value as _, isnull as _, domainType as _, extra as _, mcxt as _, escontext as _) as _
}
pub unsafe fn errdatatype(datatypeOid: Oid) -> c_int {
    crate::utils::adt::domains::errdatatype(datatypeOid as _) as _
}
pub unsafe fn errdomainconstraint(datatypeOid: Oid, conname: *const c_char) -> c_int {
    crate::utils::adt::domains::errdomainconstraint(datatypeOid as _, conname as _) as _
}

/* encode.c */
pub unsafe fn hex_encode(src: *const c_char, len: Size, dst: *mut c_char) -> uint64 {
    crate::utils::adt::encode::hex_encode(src as _, len as _, dst as _) as _
}
pub unsafe fn hex_decode(src: *const c_char, len: Size, dst: *mut c_char) -> uint64 {
    crate::utils::adt::encode::hex_decode(src as _, len as _, dst as _) as _
}
pub unsafe fn hex_decode_safe(
    src: *const c_char,
    len: Size,
    dst: *mut c_char,
    escontext: *mut Node,
) -> uint64 {
    crate::utils::adt::encode::hex_decode_safe(src as _, len as _, dst as _, escontext as _) as _
}

/* int.c */
pub unsafe fn buildint2vector(int2s: *const int16, n: c_int) -> *mut int2vector {
    crate::utils::adt::int::buildint2vector(int2s as _, n as _) as _
}

/* name.c */
pub unsafe fn namestrcpy(name: Name, str: *const c_char) {
    crate::utils::adt::name::namestrcpy(name as _, str as _)
}
pub unsafe fn namestrcmp(name: Name, str: *const c_char) -> c_int {
    crate::utils::adt::name::namestrcmp(name as _, str)
}

/* numutils.c */
pub unsafe fn pg_strtoint16(s: *const c_char) -> int16 {
    crate::utils::adt::numutils::pg_strtoint16(s as _) as _
}
pub unsafe fn pg_strtoint16_safe(s: *const c_char, escontext: *mut Node) -> int16 {
    crate::utils::adt::numutils::pg_strtoint16_safe(s as _, escontext as _) as _
}
pub unsafe fn pg_strtoint32(s: *const c_char) -> int32 {
    crate::utils::adt::numutils::pg_strtoint32(s)
}
pub unsafe fn pg_strtoint32_safe(s: *const c_char, escontext: *mut Node) -> int32 {
    crate::utils::adt::numutils::pg_strtoint32_safe(s, escontext as _)
}
pub unsafe fn pg_strtoint64(s: *const c_char) -> int64 {
    crate::utils::adt::numutils::pg_strtoint64(s as _) as _
}
pub unsafe fn pg_strtoint64_safe(s: *const c_char, escontext: *mut Node) -> int64 {
    crate::utils::adt::numutils::pg_strtoint64_safe(s as _, escontext as _) as _
}
pub unsafe fn uint32in_subr(
    s: *const c_char,
    endloc: *mut *mut c_char,
    typname: *const c_char,
    escontext: *mut Node,
) -> uint32 {
    crate::utils::adt::numutils::uint32in_subr(s as _, endloc as _, typname as _, escontext as _) as _
}
pub unsafe fn uint64in_subr(
    s: *const c_char,
    endloc: *mut *mut c_char,
    typname: *const c_char,
    escontext: *mut Node,
) -> uint64 {
    crate::utils::adt::numutils::uint64in_subr(s as _, endloc as _, typname as _, escontext as _) as _
}
pub unsafe fn pg_itoa(i: int16, a: *mut c_char) -> c_int {
    crate::utils::adt::numutils::pg_itoa(i as _, a as _) as _
}
pub unsafe fn pg_ultoa_n(value: uint32, a: *mut c_char) -> c_int {
    crate::utils::adt::numutils::pg_ultoa_n(value as _, a as _) as _
}
pub unsafe fn pg_ulltoa_n(value: uint64, a: *mut c_char) -> c_int {
    crate::utils::adt::numutils::pg_ulltoa_n(value as _, a as _) as _
}
pub unsafe fn pg_ltoa(value: int32, a: *mut c_char) -> c_int {
    crate::utils::adt::numutils::pg_ltoa(value as _, a as _) as _
}
pub unsafe fn pg_lltoa(value: int64, a: *mut c_char) -> c_int {
    crate::utils::adt::numutils::pg_lltoa(value as _, a as _) as _
}
pub unsafe fn pg_ultostr_zeropad(str: *mut c_char, value: uint32, minwidth: int32) -> *mut c_char {
    crate::utils::adt::numutils::pg_ultostr_zeropad(str as _, value as _, minwidth as _) as _
}
pub unsafe fn pg_ultostr(str: *mut c_char, value: uint32) -> *mut c_char {
    crate::utils::adt::numutils::pg_ultostr(str as _, value as _) as _
}

/* oid.c */
pub unsafe fn buildoidvector(oids: *const Oid, n: c_int) -> *mut oidvector {
    crate::utils::adt::oid::buildoidvector(oids as _, n as _) as _
}
pub unsafe fn check_valid_oidvector(oidArray: *const oidvector) {
    crate::utils::adt::oid::check_valid_oidvector(oidArray as _)
}
pub unsafe fn oidparse(node: *mut Node) -> Oid {
    crate::utils::adt::oid::oidparse(node as _) as _
}
pub unsafe fn oid_cmp(p1: *const c_void, p2: *const c_void) -> c_int {
    crate::utils::adt::oid::oid_cmp(p1 as _, p2 as _) as _
}

/* regexp.c */
pub unsafe fn regexp_fixed_prefix(
    text_re: *mut text,
    case_insensitive: bool,
    collation: Oid,
    exact: *mut bool,
) -> *mut c_char {
    crate::utils::adt::regexp::regexp_fixed_prefix(text_re as _, case_insensitive as _, collation as _, exact as _) as _
}

/* ruleutils.c */
// extern PGDLLIMPORT bool quote_all_identifiers;
#[no_mangle]
pub static mut quote_all_identifiers: bool = false;
#[no_mangle]
pub unsafe fn quote_identifier(ident: *const c_char) -> *const c_char {
    use crate::common::keywords::{ScanKeywordCategories, ScanKeywords, UNRESERVED_KEYWORD};
    use crate::common::kwlookup::ScanKeywordLookup;
    use crate::utils::mmgr::mcxt::palloc;

    let mut nquotes: i32 = 0;
    let mut ptr: *const u8 = ident as _;
    let c0 = *ptr;
    let mut safe = (c0 >= b'a' && c0 <= b'z') || c0 == b'_';
    while *ptr != 0 {
        let ch = *ptr;
        if !((ch >= b'a' && ch <= b'z') || (ch >= b'0' && ch <= b'9') || ch == b'_') {
            safe = false;
            if ch == b'"' { nquotes += 1; }
        }
        ptr = ptr.add(1);
    }
    if quote_all_identifiers { safe = false; }
    if safe {
        let kwnum = ScanKeywordLookup(ident, &ScanKeywords);
        if kwnum >= 0 && ScanKeywordCategories[kwnum as usize] != UNRESERVED_KEYWORD as u8 {
            safe = false;
        }
    }
    if safe {
        return ident;
    }
    let identlen = libc::strlen(ident);
    let result = palloc(identlen + nquotes as usize + 2 + 1) as *mut u8;
    let mut optr = result;
    *optr = b'"'; optr = optr.add(1);
    let mut ptr: *const u8 = ident as _;
    while *ptr != 0 {
        let ch = *ptr;
        if ch == b'"' { *optr = b'"'; optr = optr.add(1); }
        *optr = ch; optr = optr.add(1);
        ptr = ptr.add(1);
    }
    *optr = b'"'; optr = optr.add(1);
    *optr = 0;
    result as *const c_char
}
pub unsafe fn quote_qualified_identifier(
    qualifier: *const c_char,
    ident: *const c_char,
) -> *mut c_char {
    use crate::utils::mmgr::mcxt::palloc;
    let qident = quote_identifier(ident);
    if qualifier.is_null() {
        let len = libc::strlen(qident);
        let result = palloc(len + 1) as *mut c_char;
        libc::memcpy(result as _, qident as _, len + 1);
        return result;
    }
    let qqual = quote_identifier(qualifier);
    let ql = libc::strlen(qqual);
    let il = libc::strlen(qident);
    let result = palloc(ql + 1 + il + 1) as *mut c_char;
    libc::memcpy(result as _, qqual as _, ql);
    *result.add(ql) = b'.' as c_char;
    libc::memcpy(result.add(ql + 1) as _, qident as _, il + 1);
    result
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
    crate::utils::adt::varchar::bpchartruelen(s, len)
}

/* popular functions from varlena.c */
pub unsafe fn cstring_to_text(s: *const c_char) -> *mut text {
    crate::utils::adt::varlena::cstring_to_text(s) as *mut text
}
pub unsafe fn cstring_to_text_with_len(s: *const c_char, len: c_int) -> *mut text {
    crate::utils::adt::varlena::cstring_to_text_with_len(s, len) as *mut text
}
pub unsafe fn text_to_cstring(t: *const text) -> *mut c_char {
    crate::utils::adt::varlena::text_to_cstring(t as *const _)
}
pub unsafe fn text_to_cstring_buffer(src: *const text, dst: *mut c_char, dst_len: Size) {
    crate::utils::adt::varlena::text_to_cstring_buffer(src as *const _, dst, dst_len as usize)
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
    crate::utils::adt::xid::xidComparator(arg1 as _, arg2 as _) as _
}
pub unsafe fn xidLogicalComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    crate::utils::adt::xid::xidLogicalComparator(arg1 as _, arg2 as _) as _
}

/* inet_cidr_ntop.c */
pub unsafe fn pg_inet_cidr_ntop(
    af: c_int,
    src: *const c_void,
    bits: c_int,
    dst: *mut c_char,
    size: Size,
) -> *mut c_char {
    crate::utils::adt::inet_cidr_ntop::pg_inet_cidr_ntop(af as _, src as _, bits as _, dst as _, size as _) as _
}

/* inet_net_pton.c */
pub unsafe fn pg_inet_net_pton(af: c_int, src: *const c_char, dst: *mut c_void, size: Size) -> c_int {
    crate::utils::adt::inet_net_pton::pg_inet_net_pton(af as _, src as _, dst as _, size as _) as _
}

/* network.c */
pub unsafe fn convert_network_to_scalar(value: Datum, typid: Oid, failure: *mut bool) -> f64 {
    crate::utils::adt::network::convert_network_to_scalar(value as _, typid as _, failure as _) as _
}
pub unsafe fn network_scan_first(input: Datum) -> Datum {
    crate::utils::adt::network::network_scan_first(input as _) as _
}
pub unsafe fn network_scan_last(input: Datum) -> Datum {
    crate::utils::adt::network::network_scan_last(input as _) as _
}
pub unsafe fn clean_ipv6_addr(addr_family: c_int, addr: *mut c_char) {
    crate::utils::adt::network::clean_ipv6_addr(addr_family as _, addr as _)
}

/* numeric.c */
pub unsafe fn numeric_float8_no_overflow(fcinfo: FunctionCallInfo) -> Datum {
    crate::utils::adt::numeric::numeric_float8_no_overflow(fcinfo as _) as _
}

/* format_type.c */

/* Control flags for format_type_extended */
pub const FORMAT_TYPE_TYPEMOD_GIVEN: c_int = 0x01; /* typemod defined by caller */
pub const FORMAT_TYPE_ALLOW_INVALID: c_int = 0x02; /* allow invalid types */
pub const FORMAT_TYPE_FORCE_QUALIFY: c_int = 0x04; /* force qualification of type */
pub const FORMAT_TYPE_INVALID_AS_NULL: c_int = 0x08; /* NULL if undefined */
pub unsafe fn format_type_extended(type_oid: Oid, typemod: int32, flags: bits16) -> *mut c_char {
    crate::utils::adt::format_type::format_type_extended(type_oid as _, typemod as _, flags as _) as _
}

pub unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(type_oid as _) as _
}
pub unsafe fn format_type_be_qualified(type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be_qualified(type_oid as _) as _
}
pub unsafe fn format_type_with_typemod(type_oid: Oid, typemod: int32) -> *mut c_char {
    crate::utils::adt::format_type::format_type_with_typemod(type_oid as _, typemod as _) as _
}

pub unsafe fn type_maximum_size(type_oid: Oid, typemod: int32) -> int32 {
    crate::utils::adt::format_type::type_maximum_size(type_oid as _, typemod as _) as _
}

/* quote.c */
pub unsafe fn quote_literal_cstr(rawstr: *const c_char) -> *mut c_char {
    crate::utils::adt::quote::quote_literal_cstr(rawstr as _) as _
}
