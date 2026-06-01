//! Translation of postgres/src/backend/utils/adt/network.c (+ inet.h)
//!
//! PostgreSQL type definitions for the INET and CIDR types.
//!
//!	Jon Postel RIP 16 Oct 1998
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: common/hashfn.h -> crate::common::hashfn (hash_any/hash_any_extended),
//! libpq/pqformat.h -> crate::libpq::pqformat (pq_getmsgbyte/pq_sendbyte(=pq_sendint8)/
//! pq_begintypsend/pq_endtypsend), utils/inet.h merged in below, the inet text<->binary
//! codec from crate::utils::adt::inet_net_pton (pg_inet_net_pton) and
//! crate::port::inet_net_ntop (pg_inet_net_ntop), utils/builtins.h's cstring_to_text ->
//! crate::utils::adt::varlena, catalog/pg_type.h INETOID/CIDROID/MACADDROID/MACADDR8OID ->
//! crate::catalog::pg_type_d.  The VAR* macros come from crate::varatt.  libc
//! memcpy/memcmp/strchr/strlen/snprintf bound via extern "C".
//!
//! TRANSLATED FULLY: network_in (inet_in/cidr_in), network_out (inet_out/cidr_out),
//! network_recv (inet_recv/cidr_recv), network_send (inet_send/cidr_send), inet_to_cidr,
//! inet_set_masklen, cidr_set_masklen, cidr_set_masklen_internal, network_cmp_internal,
//! network_cmp, network_lt/le/eq/ge/gt/ne, network_smaller/network_larger, hashinet,
//! hashinetextended, network_sub/subeq/sup/supeq/overlap, network_host, network_show,
//! inet_abbrev, network_masklen, network_family, network_broadcast, network_network,
//! network_netmask, network_hostmask, inet_same_family, inet_merge,
//! convert_network_to_scalar (inet/cidr + macaddr/macaddr8 branches), bitncmp, bitncommon, addressOK,
//! network_scan_first, network_scan_last, inetnot, inetand, inetor, internal_inetpl,
//! inetpl, inetmi_int8, inetmi, clean_ipv6_addr.
//!
//! STUBBED (deps not yet ported):
//!  - network_sortsupport / network_fast_cmp / network_abbrev_abort /
//!    network_abbrev_convert: utils/sortsupport.h (SortSupport) + lib/hyperloglog.
//!  - network_subset_support / match_network_function / match_network_subset:
//!    nodes/supportnodes.h + nodes/nodeFuncs.h + nodes/makefuncs.h + utils/lsyscache.h
//!    + utils/fmgroids.h.
//!  - cidr_abbrev: pg_inet_cidr_ntop (port/inet_cidr_ntop.c) not yet translated.
//!  - inet_client_addr / inet_client_port / inet_server_addr / inet_server_port:
//!    backend MyProcPort (libpq/libpq-be.h, miscadmin.h) + common/ip.h pg_getnameinfo_all.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
// GLOB-AMBIGUITY: both crate::varatt and crate::utils::fmgr historically exported
// pg_detoast_datum_packed; the explicit import here wins over the globs.
use crate::varatt::pg_detoast_datum_packed;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_INT32, PG_RETURN_INT64,
};
use crate::c::{int32, int64, uint64};
use crate::catalog::pg_type_d::{CIDROID, INETOID, MACADDR8OID, MACADDROID};
use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_sendint8};
use crate::nodes::nodes::Node;
use crate::port::inet_net_ntop::pg_inet_net_ntop;
use crate::postgres::{DatumGetPointer, Int32GetDatum, PointerGetDatum};
use crate::postgres_ext::InvalidOid;
use crate::utils::adt::inet_net_pton::pg_inet_net_pton;
use crate::utils::adt::mac8::{DatumGetMacaddr8P, DatumGetMacaddrP};
use crate::utils::adt::varlena::cstring_to_text;
use core::ffi::{c_char, c_int, c_uchar, c_void};

// pstrdup is a backend palloc routine and comes from crate::utils::palloc via the
// prelude (not a libc symbol); the rest are <string.h> / <stdio.h> libc functions.
extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_INVALID_BINARY_REPRESENTATION: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;

// `pq_sendbyte(buf, byt)` is a static inline in pqformat.h that just calls
// pq_sendint8(buf, byt); reproduce it here (the Rust pqformat exports pq_sendint8).
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: u8) {
    pq_sendint8(buf, byt);
}

// ----------------------------------------------------------------
//   utils/inet.h merged in
// ----------------------------------------------------------------

/*
 *	This is the internal storage format for IP addresses
 *	(both INET and CIDR datatypes):
 */
#[repr(C)]
pub struct inet_struct {
    pub family: u8,       /* PGSQL_AF_INET or PGSQL_AF_INET6 */
    pub bits: u8,         /* number of bits in netmask */
    pub ipaddr: [u8; 16], /* up to 128 bits of address */
}

/*
 * We use these values for the "family" field.
 *
 * PGSQL_AF_INET = AF_INET + 0; PGSQL_AF_INET6 = AF_INET + 1.  AF_INET is 2 on
 * both Linux and macOS, matching inet_net_pton.rs / inet_net_ntop.rs.
 */
const PGSQL_AF_INET: u8 = 2 + 0;
const PGSQL_AF_INET6: u8 = 2 + 1;

/*
 * Both INET and CIDR addresses are represented within Postgres as varlena
 * objects, ie, there is a varlena header in front of the struct type
 * depicted above.
 */
#[repr(C)]
pub struct inet {
    pub vl_len_: [c_char; 4], /* Do not touch this field directly! */
    pub inet_data: inet_struct,
}

/*
 *	Access macros.  We use VARDATA_ANY so that we can process short-header
 *	varlena values without detoasting them.
 */

/* ip_family(inetptr) = ((inet_struct *) VARDATA_ANY(inetptr))->family */
#[inline]
unsafe fn ip_family(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).family
}
#[inline]
unsafe fn set_ip_family(inetptr: *mut inet, v: u8) {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct)).family = v;
}

/* ip_bits(inetptr) = ((inet_struct *) VARDATA_ANY(inetptr))->bits */
#[inline]
unsafe fn ip_bits(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).bits
}
#[inline]
unsafe fn set_ip_bits(inetptr: *mut inet, v: u8) {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct)).bits = v;
}

/* ip_addr(inetptr) = ((inet_struct *) VARDATA_ANY(inetptr))->ipaddr (an array) */
#[inline]
unsafe fn ip_addr(inetptr: *const inet) -> *mut u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct))
        .ipaddr
        .as_mut_ptr()
}

/* ip_addrsize(inetptr) = (family == PGSQL_AF_INET ? 4 : 16) */
#[inline]
unsafe fn ip_addrsize(inetptr: *const inet) -> c_int {
    if ip_family(inetptr) == PGSQL_AF_INET {
        4
    } else {
        16
    }
}

/* ip_maxbits(inetptr) = (family == PGSQL_AF_INET ? 32 : 128) */
#[inline]
unsafe fn ip_maxbits(inetptr: *const inet) -> c_int {
    if ip_family(inetptr) == PGSQL_AF_INET {
        32
    } else {
        128
    }
}

/*
 * SET_INET_VARSIZE(dst) =
 *   SET_VARSIZE(dst, VARHDRSZ + offsetof(inet_struct, ipaddr) + ip_addrsize(dst))
 */
#[inline]
unsafe fn SET_INET_VARSIZE(dst: *mut inet) {
    SET_VARSIZE(
        dst as *mut c_char,
        VARHDRSZ + core::mem::offset_of!(inet_struct, ipaddr) as int32 + ip_addrsize(dst),
    );
}

/*
 * fmgr interface helpers (inet.h).
 *
 * # Safety
 * `X` is a Datum holding a (possibly short-header) inet/cidr pointer.
 */
#[inline]
unsafe fn DatumGetInetPP(X: Datum) -> *mut inet {
    pg_detoast_datum_packed(DatumGetPointer(X) as *mut c_void) as *mut inet
}
#[inline]
unsafe fn InetPGetDatum(X: *const inet) -> Datum {
    PointerGetDatum(X as *const c_void)
}
// PG_GETARG_INET_PP(n) == DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, n))
// PG_RETURN_INET_P(x)  == return InetPGetDatum(x)

// ----------------------------------------------------------------
//   network.c
// ----------------------------------------------------------------

/*
 * An IPv4 netmask size is a value in the range of 0 - 32, which is
 * represented with 6 bits in inet/cidr abbreviated keys where possible.
 *
 * An IPv4 inet/cidr abbreviated key can use up to 25 bits for subnet
 * component.
 */
#[allow(dead_code)]
const ABBREV_BITS_INET4_NETMASK_SIZE: c_int = 6;
#[allow(dead_code)]
const ABBREV_BITS_INET4_SUBNET: c_int = 25;

/*
 * Common INET/CIDR input routine
 *
 * # Safety
 * `src` is a NUL-terminated C string; `escontext` is an optional error context node.
 */
unsafe fn network_in(src: *mut c_char, is_cidr: bool, _escontext: *mut Node) -> *mut inet {
    let bits: c_int;
    let dst: *mut inet;

    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    /*
     * First, check to see if this is an IPv6 or IPv4 address.  IPv6 addresses
     * will have a : somewhere in them (several, in fact) so if there is one
     * present, assume it's V6, otherwise assume it's V4.
     */

    if !strchr(src, ':' as c_int).is_null() {
        set_ip_family(dst, PGSQL_AF_INET6);
    } else {
        set_ip_family(dst, PGSQL_AF_INET);
    }

    bits = pg_inet_net_pton(
        ip_family(dst) as c_int,
        src,
        ip_addr(dst) as *mut c_void,
        if is_cidr {
            ip_addrsize(dst) as usize
        } else {
            usize::MAX /* (size_t) -1 */
        },
    );
    if bits < 0 || bits > ip_maxbits(dst) {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        /* translator: first %s is inet or cidr */
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                if is_cidr { "cidr" } else { "inet" },
                cstr(src)
            )
        );
    }

    /*
     * Error check: CIDR values must not have any bits set beyond the masklen.
     */
    if is_cidr && !addressOK(ip_addr(dst), bits, ip_family(dst) as c_int) {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        /* errdetail("Value has bits set to right of mask.") */
        ereport!(ERROR, errmsg!("invalid cidr value: \"{}\"", cstr(src)));
    }

    set_ip_bits(dst, bits as u8);
    SET_INET_VARSIZE(dst);

    dst
}

pub unsafe fn inet_in(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING

    return InetPGetDatum(network_in(src, false, (*fcinfo).context)); // PG_RETURN_INET_P
}

pub unsafe fn cidr_in(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING

    return InetPGetDatum(network_in(src, true, (*fcinfo).context)); // PG_RETURN_INET_P
}

/*
 * Common INET/CIDR output routine
 *
 * # Safety
 * `src` is a valid inet/cidr datum.
 */
unsafe fn network_out(src: *mut inet, is_cidr: bool) -> *mut c_char {
    /* char tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")]; */
    let mut tmp = [0 as c_char; 50];
    let dst: *mut c_char;
    let len: c_int;

    dst = pg_inet_net_ntop(
        ip_family(src) as c_int,
        ip_addr(src) as *const c_void,
        ip_bits(src) as c_int,
        tmp.as_mut_ptr(),
        tmp.len(),
    );
    if dst.is_null() {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        ereport!(ERROR, errmsg!("could not format inet value: %m"));
    }

    /* For CIDR, add /n if not present */
    if is_cidr && strchr(tmp.as_ptr(), '/' as c_int).is_null() {
        len = strlen(tmp.as_ptr()) as c_int;
        snprintf(
            tmp.as_mut_ptr().add(len as usize),
            tmp.len() - len as usize,
            c"/%u".as_ptr(),
            ip_bits(src) as c_uint,
        );
    }

    pstrdup(tmp.as_ptr())
}

pub unsafe fn inet_out(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP

    return CStringGetDatum(network_out(src, false)); // PG_RETURN_CSTRING
}

pub unsafe fn cidr_out(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP

    return CStringGetDatum(network_out(src, true)); // PG_RETURN_CSTRING
}

/*
 *		network_recv		- converts external binary format to inet
 *
 * The external representation is (one byte apiece for)
 * family, bits, is_cidr, address length, address in network byte order.
 *
 * # Safety
 * `buf` is a valid StringInfo.
 */
unsafe fn network_recv(buf: StringInfo, is_cidr: bool) -> *mut inet {
    let addr: *mut inet;
    let addrptr: *mut c_char;
    let bits: c_int;
    let nb: c_int;
    let mut i: c_int;

    /* make sure any unused bits in a CIDR value are zeroed */
    addr = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    set_ip_family(addr, pq_getmsgbyte(buf) as u8);
    if ip_family(addr) != PGSQL_AF_INET && ip_family(addr) != PGSQL_AF_INET6 {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        /* translator: %s is inet or cidr */
        ereport!(
            ERROR,
            errmsg!(
                "invalid address family in external \"{}\" value",
                if is_cidr { "cidr" } else { "inet" }
            )
        );
    }
    bits = pq_getmsgbyte(buf);
    if bits < 0 || bits > ip_maxbits(addr) {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        /* translator: %s is inet or cidr */
        ereport!(
            ERROR,
            errmsg!(
                "invalid bits in external \"{}\" value",
                if is_cidr { "cidr" } else { "inet" }
            )
        );
    }
    set_ip_bits(addr, bits as u8);
    i = pq_getmsgbyte(buf); /* ignore is_cidr */
    let _ = i;
    nb = pq_getmsgbyte(buf);
    if nb != ip_addrsize(addr) {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        /* translator: %s is inet or cidr */
        ereport!(
            ERROR,
            errmsg!(
                "invalid length in external \"{}\" value",
                if is_cidr { "cidr" } else { "inet" }
            )
        );
    }

    addrptr = ip_addr(addr) as *mut c_char;
    i = 0;
    while i < nb {
        *addrptr.add(i as usize) = pq_getmsgbyte(buf) as c_char;
        i += 1;
    }

    /*
     * Error check: CIDR values must not have any bits set beyond the masklen.
     */
    if is_cidr && !addressOK(ip_addr(addr), bits, ip_family(addr) as c_int) {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        /* errdetail("Value has bits set to right of mask.") */
        ereport!(ERROR, errmsg!("invalid external \"cidr\" value"));
    }

    SET_INET_VARSIZE(addr);

    addr
}

pub unsafe fn inet_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    return InetPGetDatum(network_recv(buf, false)); // PG_RETURN_INET_P
}

pub unsafe fn cidr_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    return InetPGetDatum(network_recv(buf, true)); // PG_RETURN_INET_P
}

/*
 *		network_send		- converts inet to binary format
 *
 * # Safety
 * `addr` is a valid inet/cidr datum.
 */
unsafe fn network_send(addr: *mut inet, is_cidr: bool) -> *mut crate::c::bytea {
    let mut buf: StringInfoData = core::mem::zeroed();
    let addrptr: *mut c_char;
    let nb: c_int;
    let mut i: c_int;

    pq_begintypsend(&mut buf);
    pq_sendbyte(&mut buf, ip_family(addr));
    pq_sendbyte(&mut buf, ip_bits(addr));
    pq_sendbyte(&mut buf, is_cidr as u8);
    nb = ip_addrsize(addr);
    pq_sendbyte(&mut buf, nb as u8);
    addrptr = ip_addr(addr) as *mut c_char;
    i = 0;
    while i < nb {
        pq_sendbyte(&mut buf, *addrptr.add(i as usize) as u8);
        i += 1;
    }
    pq_endtypsend(&mut buf)
}

pub unsafe fn inet_send(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP

    return PointerGetDatum(network_send(addr, false) as *const c_void); // PG_RETURN_BYTEA_P
}

pub unsafe fn cidr_send(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP

    return PointerGetDatum(network_send(addr, true) as *const c_void); // PG_RETURN_BYTEA_P
}

pub unsafe fn inet_to_cidr(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let bits: c_int;

    bits = ip_bits(src) as c_int;

    /* safety check */
    if bits < 0 || bits > ip_maxbits(src) {
        elog!(ERROR, "invalid inet bit length: {}", bits);
    }

    return InetPGetDatum(cidr_set_masklen_internal(src, bits)); // PG_RETURN_INET_P
}

pub unsafe fn inet_set_masklen(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let mut bits: c_int = PG_GETARG_INT32!(fcinfo, 1);
    let dst: *mut inet;

    if bits == -1 {
        bits = ip_maxbits(src);
    }

    if bits < 0 || bits > ip_maxbits(src) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("invalid mask length: {}", bits));
    }

    /* clone the original data */
    dst = palloc(VARSIZE_ANY(src as *const c_char) as Size) as *mut inet;
    memcpy(
        dst as *mut c_void,
        src as *const c_void,
        VARSIZE_ANY(src as *const c_char) as usize,
    );

    set_ip_bits(dst, bits as u8);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

pub unsafe fn cidr_set_masklen(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let mut bits: c_int = PG_GETARG_INT32!(fcinfo, 1);

    if bits == -1 {
        bits = ip_maxbits(src);
    }

    if bits < 0 || bits > ip_maxbits(src) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("invalid mask length: {}", bits));
    }

    return InetPGetDatum(cidr_set_masklen_internal(src, bits)); // PG_RETURN_INET_P
}

/*
 * Copy src and set mask length to 'bits' (which must be valid for the family)
 *
 * # Safety
 * `src` is a valid inet/cidr datum; `bits` valid for the family.
 */
pub unsafe fn cidr_set_masklen_internal(src: *const inet, bits: c_int) -> *mut inet {
    let dst: *mut inet = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    set_ip_family(dst, ip_family(src));
    set_ip_bits(dst, bits as u8);

    if bits > 0 {
        Assert!(bits <= ip_maxbits(dst));

        /* Clone appropriate bytes of the address, leaving the rest 0 */
        memcpy(
            ip_addr(dst) as *mut c_void,
            ip_addr(src) as *const c_void,
            ((bits + 7) / 8) as usize,
        );

        /* Clear any unwanted bits in the last partial byte */
        if bits % 8 != 0 {
            *ip_addr(dst).add((bits / 8) as usize) &= !(0xFFu8 >> (bits % 8));
        }
    }

    /* Set varlena header correctly */
    SET_INET_VARSIZE(dst);

    dst
}

/*
 *	Basic comparison function for sorting and inet/cidr comparisons.
 *
 * Comparison is first on the common bits of the network part, then on
 * the length of the network part, and then on the whole unmasked address.
 *
 * # Safety
 * `a1`/`a2` are valid inet/cidr datums.
 */
unsafe fn network_cmp_internal(a1: *mut inet, a2: *mut inet) -> int32 {
    if ip_family(a1) == ip_family(a2) {
        let mut order: c_int;

        order = bitncmp(
            ip_addr(a1),
            ip_addr(a2),
            Min(ip_bits(a1) as c_int, ip_bits(a2) as c_int),
        );
        if order != 0 {
            return order;
        }
        order = (ip_bits(a1) as c_int) - (ip_bits(a2) as c_int);
        if order != 0 {
            return order;
        }
        return bitncmp(ip_addr(a1), ip_addr(a2), ip_maxbits(a1));
    }

    ip_family(a1) as int32 - ip_family(a2) as int32
}

pub unsafe fn network_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_INT32!(network_cmp_internal(a1, a2));
}

/*
 * SortSupport strategy routine
 */
pub unsafe fn network_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    // C body sets ssup->comparator = network_fast_cmp and (when abbreviating)
    // installs network_abbrev_convert/abort + a hyperLogLog estimator.
    // TODO(pg-port): utils/sortsupport.h (SortSupport) + lib/hyperloglog not yet translated.
    let _ = fcinfo;
    unimplemented!("network_sortsupport: utils/sortsupport.h + lib/hyperloglog not yet translated")
}

/*
 * SortSupport comparison func
 */
#[allow(dead_code)]
unsafe fn network_fast_cmp(x: Datum, y: Datum, _ssup: *mut c_void) -> c_int {
    let arg1: *mut inet = DatumGetInetPP(x);
    let arg2: *mut inet = DatumGetInetPP(y);

    network_cmp_internal(arg1, arg2)
}

/*
 * Callback for estimating effectiveness of abbreviated key optimization.
 */
#[allow(dead_code)]
unsafe fn network_abbrev_abort(memtupcount: c_int, ssup: *mut c_void) -> bool {
    // TODO(pg-port): utils/sortsupport.h + lib/hyperloglog (network_sortsupport_state) not ported.
    let _ = (memtupcount, ssup);
    unimplemented!("network_abbrev_abort: utils/sortsupport.h + lib/hyperloglog not yet translated")
}

/*
 * SortSupport conversion routine.  Converts original inet/cidr representation
 * to an abbreviated key for 3-way unsigned int comparison.
 */
#[allow(dead_code)]
unsafe fn network_abbrev_convert(original: Datum, ssup: *mut c_void) -> Datum {
    // TODO(pg-port): utils/sortsupport.h + lib/hyperloglog + pg_bswap/DatumBigEndianToNative.
    let _ = (original, ssup);
    unimplemented!("network_abbrev_convert: utils/sortsupport.h + lib/hyperloglog not yet translated")
}

/*
 *	Boolean ordering tests.
 */
pub unsafe fn network_lt(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(network_cmp_internal(a1, a2) < 0);
}

pub unsafe fn network_le(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(network_cmp_internal(a1, a2) <= 0);
}

pub unsafe fn network_eq(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(network_cmp_internal(a1, a2) == 0);
}

pub unsafe fn network_ge(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(network_cmp_internal(a1, a2) >= 0);
}

pub unsafe fn network_gt(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(network_cmp_internal(a1, a2) > 0);
}

pub unsafe fn network_ne(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(network_cmp_internal(a1, a2) != 0);
}

/*
 * MIN/MAX support functions.
 */
pub unsafe fn network_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if network_cmp_internal(a1, a2) < 0 {
        return InetPGetDatum(a1); // PG_RETURN_INET_P
    } else {
        return InetPGetDatum(a2); // PG_RETURN_INET_P
    }
}

pub unsafe fn network_larger(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if network_cmp_internal(a1, a2) > 0 {
        return InetPGetDatum(a1); // PG_RETURN_INET_P
    } else {
        return InetPGetDatum(a2); // PG_RETURN_INET_P
    }
}

/*
 * Support function for hash indexes on inet/cidr.
 */
pub unsafe fn hashinet(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let addrsize: c_int = ip_addrsize(addr);

    /* XXX this assumes there are no pad bytes in the data structure */
    return hash_any(
        VARDATA_ANY(addr as *const c_char) as *const c_uchar,
        addrsize + 2,
    );
}

pub unsafe fn hashinetextended(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let addrsize: c_int = ip_addrsize(addr);

    return hash_any_extended(
        VARDATA_ANY(addr as *const c_char) as *const c_uchar,
        addrsize + 2,
        PG_GETARG_INT64!(fcinfo, 1) as uint64,
    );
}

/*
 *	Boolean network-inclusion tests.
 */
pub unsafe fn network_sub(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if ip_family(a1) == ip_family(a2) {
        PG_RETURN_BOOL!(
            ip_bits(a1) > ip_bits(a2)
                && bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a2) as c_int) == 0
        );
    }

    PG_RETURN_BOOL!(false);
}

pub unsafe fn network_subeq(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if ip_family(a1) == ip_family(a2) {
        PG_RETURN_BOOL!(
            ip_bits(a1) >= ip_bits(a2)
                && bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a2) as c_int) == 0
        );
    }

    PG_RETURN_BOOL!(false);
}

pub unsafe fn network_sup(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if ip_family(a1) == ip_family(a2) {
        PG_RETURN_BOOL!(
            ip_bits(a1) < ip_bits(a2)
                && bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1) as c_int) == 0
        );
    }

    PG_RETURN_BOOL!(false);
}

pub unsafe fn network_supeq(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if ip_family(a1) == ip_family(a2) {
        PG_RETURN_BOOL!(
            ip_bits(a1) <= ip_bits(a2)
                && bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1) as c_int) == 0
        );
    }

    PG_RETURN_BOOL!(false);
}

pub unsafe fn network_overlap(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    if ip_family(a1) == ip_family(a2) {
        PG_RETURN_BOOL!(
            bitncmp(
                ip_addr(a1),
                ip_addr(a2),
                Min(ip_bits(a1) as c_int, ip_bits(a2) as c_int)
            ) == 0
        );
    }

    PG_RETURN_BOOL!(false);
}

/*
 * Planner support function for network subset/superset operators
 */
pub unsafe fn network_subset_support(fcinfo: FunctionCallInfo) -> Datum {
    // C body inspects SupportRequestIndexCondition and dispatches to
    // match_network_function.
    // TODO(pg-port): nodes/supportnodes.h + nodes/nodeFuncs.h (is_opclause/is_funcclause)
    // not yet translated.
    let _ = fcinfo;
    unimplemented!("network_subset_support: nodes/supportnodes.h not yet translated")
}

/*
 * match_network_function
 *	  Try to generate an indexqual for a network subset/superset function.
 *
 * TODO(pg-port): utils/fmgroids.h (F_NETWORK_*) + match_network_subset deps.
 */
#[allow(dead_code)]
unsafe fn match_network_function(
    leftop: *mut Node,
    rightop: *mut Node,
    indexarg: c_int,
    funcid: Oid,
    opfamily: Oid,
) -> *mut List {
    let _ = (leftop, rightop, indexarg, funcid, opfamily);
    unimplemented!("match_network_function: utils/fmgroids.h (F_NETWORK_*) not yet translated")
}

/*
 * match_network_subset
 *	  Try to generate an indexqual for a network subset function.
 *
 * TODO(pg-port): nodes/makefuncs.h (make_opclause/makeConst) + nodes/pg_list.h +
 * utils/lsyscache.h (get_opfamily_member_for_cmptype).
 */
#[allow(dead_code)]
unsafe fn match_network_subset(
    leftop: *mut Node,
    rightop: *mut Node,
    is_eq: bool,
    opfamily: Oid,
) -> *mut List {
    let _ = (leftop, rightop, is_eq, opfamily);
    unimplemented!("match_network_subset: nodes/makefuncs.h + utils/lsyscache.h not yet translated")
}

/* opaque List for the (stubbed) planner-support routines above. */
pub enum List {}

/*
 * Extract data from a network datatype.
 */
pub unsafe fn network_host(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let ptr: *mut c_char;
    /* char tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")]; */
    let mut tmp = [0 as c_char; 50];

    /* force display of max bits, regardless of masklen... */
    if pg_inet_net_ntop(
        ip_family(ip) as c_int,
        ip_addr(ip) as *const c_void,
        ip_maxbits(ip),
        tmp.as_mut_ptr(),
        tmp.len(),
    )
    .is_null()
    {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        ereport!(ERROR, errmsg!("could not format inet value: %m"));
    }

    /* Suppress /n if present (shouldn't happen now) */
    ptr = strchr(tmp.as_ptr(), '/' as c_int);
    if !ptr.is_null() {
        *ptr = b'\0' as c_char;
    }

    return PointerGetDatum(cstring_to_text(tmp.as_ptr()) as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * network_show implements the inet and cidr casts to text.
 */
pub unsafe fn network_show(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let len: c_int;
    /* char tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")]; */
    let mut tmp = [0 as c_char; 50];

    if pg_inet_net_ntop(
        ip_family(ip) as c_int,
        ip_addr(ip) as *const c_void,
        ip_maxbits(ip),
        tmp.as_mut_ptr(),
        tmp.len(),
    )
    .is_null()
    {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        ereport!(ERROR, errmsg!("could not format inet value: %m"));
    }

    /* Add /n if not present (which it won't be) */
    if strchr(tmp.as_ptr(), '/' as c_int).is_null() {
        len = strlen(tmp.as_ptr()) as c_int;
        snprintf(
            tmp.as_mut_ptr().add(len as usize),
            tmp.len() - len as usize,
            c"/%u".as_ptr(),
            ip_bits(ip) as c_uint,
        );
    }

    return PointerGetDatum(cstring_to_text(tmp.as_ptr()) as *const c_void); // PG_RETURN_TEXT_P
}

pub unsafe fn inet_abbrev(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let dst: *mut c_char;
    /* char tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")]; */
    let mut tmp = [0 as c_char; 50];

    dst = pg_inet_net_ntop(
        ip_family(ip) as c_int,
        ip_addr(ip) as *const c_void,
        ip_bits(ip) as c_int,
        tmp.as_mut_ptr(),
        tmp.len(),
    );

    if dst.is_null() {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        ereport!(ERROR, errmsg!("could not format inet value: %m"));
    }

    return PointerGetDatum(cstring_to_text(tmp.as_ptr()) as *const c_void); // PG_RETURN_TEXT_P
}

pub unsafe fn cidr_abbrev(fcinfo: FunctionCallInfo) -> Datum {
    // C body formats via pg_inet_cidr_ntop(ip_family, ip_addr, ip_bits, tmp, sizeof(tmp))
    // then returns cstring_to_text(tmp).
    // TODO(pg-port): pg_inet_cidr_ntop (port/inet_cidr_ntop.c) not yet translated.
    let _ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    unimplemented!("cidr_abbrev: pg_inet_cidr_ntop (port/inet_cidr_ntop.c) not yet translated")
}

pub unsafe fn network_masklen(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP

    PG_RETURN_INT32!(ip_bits(ip) as int32);
}

pub unsafe fn network_family(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP

    match ip_family(ip) {
        PGSQL_AF_INET => {
            PG_RETURN_INT32!(4);
        }
        PGSQL_AF_INET6 => {
            PG_RETURN_INT32!(6);
        }
        _ => {
            PG_RETURN_INT32!(0);
        }
    }
}

pub unsafe fn network_broadcast(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let dst: *mut inet;
    let mut byte: c_int;
    let mut bits: c_int;
    let maxbytes: c_int;
    let mut mask: u8;
    let a: *mut u8;
    let b: *mut u8;

    /* make sure any unused bits are zeroed */
    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    maxbytes = ip_addrsize(ip);
    bits = ip_bits(ip) as c_int;
    a = ip_addr(ip);
    b = ip_addr(dst);

    byte = 0;
    while byte < maxbytes {
        if bits >= 8 {
            mask = 0x00;
            bits -= 8;
        } else if bits == 0 {
            mask = 0xff;
        } else {
            mask = 0xff >> bits;
            bits = 0;
        }

        *b.add(byte as usize) = *a.add(byte as usize) | mask;
        byte += 1;
    }

    set_ip_family(dst, ip_family(ip));
    set_ip_bits(dst, ip_bits(ip));
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

pub unsafe fn network_network(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let dst: *mut inet;
    let mut byte: c_int;
    let mut bits: c_int;
    let mut mask: u8;
    let a: *mut u8;
    let b: *mut u8;

    /* make sure any unused bits are zeroed */
    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    bits = ip_bits(ip) as c_int;
    a = ip_addr(ip);
    b = ip_addr(dst);

    byte = 0;

    while bits != 0 {
        if bits >= 8 {
            mask = 0xff;
            bits -= 8;
        } else {
            mask = 0xff << (8 - bits);
            bits = 0;
        }

        *b.add(byte as usize) = *a.add(byte as usize) & mask;
        byte += 1;
    }

    set_ip_family(dst, ip_family(ip));
    set_ip_bits(dst, ip_bits(ip));
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

pub unsafe fn network_netmask(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let dst: *mut inet;
    let mut byte: c_int;
    let mut bits: c_int;
    let mut mask: u8;
    let b: *mut u8;

    /* make sure any unused bits are zeroed */
    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    bits = ip_bits(ip) as c_int;
    b = ip_addr(dst);

    byte = 0;

    while bits != 0 {
        if bits >= 8 {
            mask = 0xff;
            bits -= 8;
        } else {
            mask = 0xff << (8 - bits);
            bits = 0;
        }

        *b.add(byte as usize) = mask;
        byte += 1;
    }

    set_ip_family(dst, ip_family(ip));
    set_ip_bits(dst, ip_maxbits(ip) as u8);
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

pub unsafe fn network_hostmask(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let dst: *mut inet;
    let mut byte: c_int;
    let mut bits: c_int;
    let maxbytes: c_int;
    let mut mask: u8;
    let b: *mut u8;

    /* make sure any unused bits are zeroed */
    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    maxbytes = ip_addrsize(ip);
    bits = ip_maxbits(ip) - ip_bits(ip) as c_int;
    b = ip_addr(dst);

    byte = maxbytes - 1;

    while bits != 0 {
        if bits >= 8 {
            mask = 0xff;
            bits -= 8;
        } else {
            mask = 0xff >> (8 - bits);
            bits = 0;
        }

        *b.add(byte as usize) = mask;
        byte -= 1;
    }

    set_ip_family(dst, ip_family(ip));
    set_ip_bits(dst, ip_maxbits(ip) as u8);
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

/*
 * Returns true if the addresses are from the same family, or false.
 */
pub unsafe fn inet_same_family(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP

    PG_RETURN_BOOL!(ip_family(a1) == ip_family(a2));
}

/*
 * Returns the smallest CIDR which contains both of the inputs.
 */
pub unsafe fn inet_merge(fcinfo: FunctionCallInfo) -> Datum {
    let a1: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let a2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP
    let commonbits: c_int;

    if ip_family(a1) != ip_family(a2) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("cannot merge addresses from different families")
        );
    }

    commonbits = bitncommon(
        ip_addr(a1),
        ip_addr(a2),
        Min(ip_bits(a1) as c_int, ip_bits(a2) as c_int),
    );

    return InetPGetDatum(cidr_set_masklen_internal(a1, commonbits)); // PG_RETURN_INET_P
}

/*
 * Convert a value of a network datatype to an approximate scalar value.
 * This is used for estimating selectivities of inequality operators
 * involving network types.
 *
 * # Safety
 * `value` is a Datum of type `typid`.
 */
pub unsafe fn convert_network_to_scalar(value: Datum, typid: Oid, failure: *mut bool) -> f64 {
    match typid {
        INETOID | CIDROID => {
            let ip: *mut inet = DatumGetInetPP(value);
            let len: c_int;
            let mut res: f64;
            let mut i: c_int;

            /*
             * Note that we don't use the full address for IPv6.
             */
            if ip_family(ip) == PGSQL_AF_INET {
                len = 4;
            } else {
                len = 5;
            }

            res = ip_family(ip) as f64;
            i = 0;
            while i < len {
                res *= 256.0;
                res += *ip_addr(ip).add(i as usize) as f64;
                i += 1;
            }
            return res;
        }
        MACADDROID => {
            let mac = DatumGetMacaddrP(value);
            let mut res: f64;

            res = (((*mac).a as c_int) << 16 | ((*mac).b as c_int) << 8 | ((*mac).c as c_int)) as f64;
            res *= (256 * 256 * 256) as f64;
            res += (((*mac).d as c_int) << 16 | ((*mac).e as c_int) << 8 | ((*mac).f as c_int)) as f64;
            return res;
        }
        MACADDR8OID => {
            let mac = DatumGetMacaddr8P(value);
            let mut res: f64;

            res = (((*mac).a as c_int) << 24
                | ((*mac).b as c_int) << 16
                | ((*mac).c as c_int) << 8
                | ((*mac).d as c_int)) as f64;
            res *= 256.0 * 256.0 * 256.0 * 256.0;
            res += (((*mac).e as c_int) << 24
                | ((*mac).f as c_int) << 16
                | ((*mac).g as c_int) << 8
                | ((*mac).h as c_int)) as f64;
            return res;
        }
        _ => {
            *failure = true;
            0.0
        }
    }
}

/*
 * int
 * bitncmp(l, r, n)
 *		compare bit masks l and r, for n bits.
 * return:
 *		<0, >0, or 0 in the libc tradition.
 * note:
 *		network byte order assumed.  this means 192.5.5.240/28 has
 *		0x11110000 in its fourth octet.
 * author:
 *		Paul Vixie (ISC), June 1996
 *
 * # Safety
 * `l`/`r` each readable for at least `(n + 7) / 8` bytes.
 */
pub unsafe fn bitncmp(l: *const c_uchar, r: *const c_uchar, n: c_int) -> c_int {
    let mut lb: c_uint;
    let mut rb: c_uint;
    let x: c_int;
    let mut b: c_int;

    b = n / 8;
    x = memcmp(l as *const c_void, r as *const c_void, b as usize);
    if x != 0 || (n % 8) == 0 {
        return x;
    }

    lb = *l.add(b as usize) as c_uint;
    rb = *r.add(b as usize) as c_uint;
    b = n % 8;
    while b > 0 {
        if IS_HIGHBIT_SET(lb as u8) != IS_HIGHBIT_SET(rb as u8) {
            if IS_HIGHBIT_SET(lb as u8) {
                return 1;
            }
            return -1;
        }
        lb <<= 1;
        rb <<= 1;
        b -= 1;
    }
    0
}

/*
 * bitncommon: compare bit masks l and r, for up to n bits.
 *
 * Returns the number of leading bits that match (0 to n).
 *
 * # Safety
 * `l`/`r` each readable for at least `(n + 7) / 8` bytes.
 */
pub unsafe fn bitncommon(l: *const c_uchar, r: *const c_uchar, n: c_int) -> c_int {
    let mut byte: c_int;
    let mut nbits: c_int;

    /* number of bits to examine in last byte */
    nbits = n % 8;

    /* check whole bytes */
    byte = 0;
    while byte < n / 8 {
        if *l.add(byte as usize) != *r.add(byte as usize) {
            /* at least one bit in the last byte is not common */
            nbits = 7;
            break;
        }
        byte += 1;
    }

    /* check bits in last partial byte */
    if nbits != 0 {
        /* calculate diff of first non-matching bytes */
        let diff: c_uint = (*l.add(byte as usize) ^ *r.add(byte as usize)) as c_uint;

        /* compare the bits from the most to the least */
        while (diff >> (8 - nbits)) != 0 {
            nbits -= 1;
        }
    }

    (8 * byte) + nbits
}

/*
 * Verify a CIDR address is OK (doesn't have bits set past the masklen)
 *
 * # Safety
 * `a` readable for the address size implied by `family`.
 */
unsafe fn addressOK(a: *mut c_uchar, bits: c_int, family: c_int) -> bool {
    let mut byte: c_int;
    let nbits: c_int;
    let maxbits: c_int;
    let maxbytes: c_int;
    let mut mask: u8;

    if family == PGSQL_AF_INET as c_int {
        maxbits = 32;
        maxbytes = 4;
    } else {
        maxbits = 128;
        maxbytes = 16;
    }
    Assert!(bits <= maxbits);

    if bits == maxbits {
        return true;
    }

    byte = bits / 8;

    nbits = bits % 8;
    mask = 0xff;
    if bits != 0 {
        mask >>= nbits;
    }

    while byte < maxbytes {
        if (*a.add(byte as usize) & mask) != 0 {
            return false;
        }
        mask = 0xff;
        byte += 1;
    }

    true
}

/*
 * These functions are used by planner to generate indexscan limits
 * for clauses a << b and a <<= b
 */

/* return the minimal value for an IP on a given network */
pub unsafe fn network_scan_first(in_: Datum) -> Datum {
    DirectFunctionCall1Coll(network_network, InvalidOid, in_)
}

/*
 * return "last" IP on a given network. It's the broadcast address,
 * however, masklen has to be set to its max bits, since
 * 192.168.0.255/24 is considered less than 192.168.0.255/32
 */
pub unsafe fn network_scan_last(in_: Datum) -> Datum {
    DirectFunctionCall2Coll(
        inet_set_masklen,
        InvalidOid,
        DirectFunctionCall1Coll(network_broadcast, InvalidOid, in_),
        Int32GetDatum(-1),
    )
}

/*
 * IP address that the client is connecting from (NULL if Unix socket)
 */
pub unsafe fn inet_client_addr(fcinfo: FunctionCallInfo) -> Datum {
    // C body reads MyProcPort->raddr, runs pg_getnameinfo_all, then network_in.
    // TODO(pg-port): backend MyProcPort (libpq/libpq-be.h, miscadmin.h) + common/ip.h
    // (pg_getnameinfo_all) not yet translated.
    let _ = fcinfo;
    unimplemented!("inet_client_addr: MyProcPort + pg_getnameinfo_all not yet translated")
}

/*
 * port that the client is connecting from (NULL if Unix socket)
 */
pub unsafe fn inet_client_port(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): MyProcPort + pg_getnameinfo_all not yet translated.
    let _ = fcinfo;
    unimplemented!("inet_client_port: MyProcPort + pg_getnameinfo_all not yet translated")
}

/*
 * IP address that the server accepted the connection on (NULL if Unix socket)
 */
pub unsafe fn inet_server_addr(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): MyProcPort + pg_getnameinfo_all not yet translated.
    let _ = fcinfo;
    unimplemented!("inet_server_addr: MyProcPort + pg_getnameinfo_all not yet translated")
}

/*
 * port that the server accepted the connection on (NULL if Unix socket)
 */
pub unsafe fn inet_server_port(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): MyProcPort + pg_getnameinfo_all not yet translated.
    let _ = fcinfo;
    unimplemented!("inet_server_port: MyProcPort + pg_getnameinfo_all not yet translated")
}

pub unsafe fn inetnot(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let dst: *mut inet;

    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    {
        let mut nb: c_int = ip_addrsize(ip);
        let pip: *mut u8 = ip_addr(ip);
        let pdst: *mut u8 = ip_addr(dst);

        nb -= 1;
        while nb >= 0 {
            *pdst.add(nb as usize) = !*pip.add(nb as usize);
            nb -= 1;
        }
    }
    set_ip_bits(dst, ip_bits(ip));

    set_ip_family(dst, ip_family(ip));
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

pub unsafe fn inetand(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let ip2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP
    let dst: *mut inet;

    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    if ip_family(ip) != ip_family(ip2) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("cannot AND inet values of different sizes"));
    } else {
        let mut nb: c_int = ip_addrsize(ip);
        let pip: *mut u8 = ip_addr(ip);
        let pip2: *mut u8 = ip_addr(ip2);
        let pdst: *mut u8 = ip_addr(dst);

        nb -= 1;
        while nb >= 0 {
            *pdst.add(nb as usize) = *pip.add(nb as usize) & *pip2.add(nb as usize);
            nb -= 1;
        }
    }
    set_ip_bits(dst, Max(ip_bits(ip), ip_bits(ip2)));

    set_ip_family(dst, ip_family(ip));
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

pub unsafe fn inetor(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let ip2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP
    let dst: *mut inet;

    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    if ip_family(ip) != ip_family(ip2) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("cannot OR inet values of different sizes"));
    } else {
        let mut nb: c_int = ip_addrsize(ip);
        let pip: *mut u8 = ip_addr(ip);
        let pip2: *mut u8 = ip_addr(ip2);
        let pdst: *mut u8 = ip_addr(dst);

        nb -= 1;
        while nb >= 0 {
            *pdst.add(nb as usize) = *pip.add(nb as usize) | *pip2.add(nb as usize);
            nb -= 1;
        }
    }
    set_ip_bits(dst, Max(ip_bits(ip), ip_bits(ip2)));

    set_ip_family(dst, ip_family(ip));
    SET_INET_VARSIZE(dst);

    return InetPGetDatum(dst); // PG_RETURN_INET_P
}

/*
 * # Safety
 * `ip` is a valid inet/cidr datum.
 */
unsafe fn internal_inetpl(ip: *mut inet, mut addend: int64) -> *mut inet {
    let dst: *mut inet;

    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    {
        let mut nb: c_int = ip_addrsize(ip);
        let pip: *mut u8 = ip_addr(ip);
        let pdst: *mut u8 = ip_addr(dst);
        let mut carry: c_int = 0;

        nb -= 1;
        while nb >= 0 {
            carry = *pip.add(nb as usize) as c_int + (addend & 0xFF) as c_int + carry;
            *pdst.add(nb as usize) = (carry & 0xFF) as u8;
            carry >>= 8;

            /*
             * We have to be careful about right-shifting addend because
             * right-shift isn't portable for negative values, while simply
             * dividing by 256 doesn't work.  So, explicitly clear the
             * low-order byte to remove any doubt about the correct result of
             * the division, and then divide rather than shift.
             */
            addend &= !(0xFF as int64);
            addend /= 0x100;
            nb -= 1;
        }

        /*
         * At this point we should have addend and carry both zero if original
         * addend was >= 0, or addend -1 and carry 1 if original addend was <
         * 0.  Anything else means overflow.
         */
        if !((addend == 0 && carry == 0) || (addend == -1 && carry == 1)) {
            let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            ereport!(ERROR, errmsg!("result is out of range"));
        }
    }

    set_ip_bits(dst, ip_bits(ip));
    set_ip_family(dst, ip_family(ip));
    SET_INET_VARSIZE(dst);

    dst
}

pub unsafe fn inetpl(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let addend: int64 = PG_GETARG_INT64!(fcinfo, 1);

    return InetPGetDatum(internal_inetpl(ip, addend)); // PG_RETURN_INET_P
}

pub unsafe fn inetmi_int8(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let addend: int64 = PG_GETARG_INT64!(fcinfo, 1);

    return InetPGetDatum(internal_inetpl(ip, -addend)); // PG_RETURN_INET_P
}

pub unsafe fn inetmi(fcinfo: FunctionCallInfo) -> Datum {
    let ip: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_INET_PP
    let ip2: *mut inet = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP
    let mut res: int64 = 0;

    if ip_family(ip) != ip_family(ip2) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("cannot subtract inet values of different sizes")
        );
    } else {
        /*
         * We form the difference using the traditional complement, increment,
         * and add rule, with the increment part being handled by starting the
         * carry off at 1.
         */
        let mut nb: c_int = ip_addrsize(ip);
        let mut byte: c_int = 0;
        let pip: *mut u8 = ip_addr(ip);
        let pip2: *mut u8 = ip_addr(ip2);
        let mut carry: c_int = 1;

        nb -= 1;
        while nb >= 0 {
            let lobyte: c_int;

            carry = *pip.add(nb as usize) as c_int + (!*pip2.add(nb as usize) & 0xFF) as c_int + carry;
            lobyte = carry & 0xFF;
            if (byte as usize) < core::mem::size_of::<int64>() {
                res |= (lobyte as int64) << (byte * 8);
            } else {
                /*
                 * Input wider than int64: check for overflow.  All bytes to
                 * the left of what will fit should be 0 or 0xFF, depending on
                 * sign of the now-complete result.
                 */
                if if res < 0 { lobyte != 0xFF } else { lobyte != 0 } {
                    let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                    ereport!(ERROR, errmsg!("result is out of range"));
                }
            }
            carry >>= 8;
            byte += 1;
            nb -= 1;
        }

        /*
         * If input is narrower than int64, overflow is not possible, but we
         * have to do proper sign extension.
         */
        if carry == 0 && (byte as usize) < core::mem::size_of::<int64>() {
            res |= ((-1i64) as uint64).wrapping_shl((byte * 8) as u32) as int64;
        }
    }

    PG_RETURN_INT64!(res);
}

/*
 * clean_ipv6_addr --- remove any '%zone' part from an IPv6 address string
 *
 * # Safety
 * `addr` is a writable NUL-terminated C string.
 */
pub unsafe fn clean_ipv6_addr(addr_family: c_int, addr: *mut c_char) {
    // AF_INET6: 30 on macOS, 10 on Linux.
    #[cfg(target_os = "macos")]
    const AF_INET6: c_int = 30;
    #[cfg(not(target_os = "macos"))]
    const AF_INET6: c_int = 10;

    if addr_family == AF_INET6 {
        let pct: *mut c_char = strchr(addr, '%' as c_int);

        if !pct.is_null() {
            *pct = b'\0' as c_char;
        }
    }
}

/*
 * Format a C string for an error message via Rust `{}` (lossy).
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
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
    use crate::postgres::{CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn inet_io_roundtrip() {
        unsafe {
            // network_in (inet path) "192.168.1.5/24" -> network_out same.
            let d = DirectFunctionCall1Coll(
                inet_in,
                InvalidOid,
                CStringGetDatum(c"192.168.1.5/24".as_ptr()),
            );
            let s = DatumGetCString(DirectFunctionCall1Coll(inet_out, InvalidOid, d));
            assert!(cstr_eq(s, "192.168.1.5/24"));

            // family/masklen accessors via the SQL-callable functions.
            assert_eq!(
                DatumGetInt32(DirectFunctionCall1Coll(network_family, InvalidOid, d)),
                4
            );
            assert_eq!(
                DatumGetInt32(DirectFunctionCall1Coll(network_masklen, InvalidOid, d)),
                24
            );

            // inspect the merged accessors directly.
            let ip = DatumGetInetPP(d);
            assert_eq!(ip_family(ip), PGSQL_AF_INET);
            assert_eq!(ip_bits(ip), 24);
            assert_eq!(*ip_addr(ip).add(0), 192);
            assert_eq!(*ip_addr(ip).add(3), 5);
        }
    }

    #[test]
    fn cidr_io_roundtrip() {
        unsafe {
            // cidr_in "10.0.0.0/8" -> cidr_out same.
            let d = DirectFunctionCall1Coll(
                cidr_in,
                InvalidOid,
                CStringGetDatum(c"10.0.0.0/8".as_ptr()),
            );
            let s = DatumGetCString(DirectFunctionCall1Coll(cidr_out, InvalidOid, d));
            assert!(cstr_eq(s, "10.0.0.0/8"));
        }
    }

    #[test]
    fn ipv6_roundtrip() {
        unsafe {
            // "::1" round trip through inet_in/inet_out.
            let d = DirectFunctionCall1Coll(inet_in, InvalidOid, CStringGetDatum(c"::1".as_ptr()));
            let s = DatumGetCString(DirectFunctionCall1Coll(inet_out, InvalidOid, d));
            // inet output omits the netmask when it equals the address's max bits
            // (PostgreSQL: `inet '::1'` displays as `::1`, not `::1/128`).
            assert!(cstr_eq(s, "::1"));
            let ip = DatumGetInetPP(d);
            assert_eq!(ip_family(ip), PGSQL_AF_INET6);
            assert_eq!(ip_bits(ip), 128);
        }
    }

    #[test]
    fn ordering_and_cmp() {
        unsafe {
            // 10.0.0.0/8 vs 192.168.1.0/24: same family, network part orders first.
            let a = DirectFunctionCall1Coll(
                cidr_in,
                InvalidOid,
                CStringGetDatum(c"10.0.0.0/8".as_ptr()),
            );
            let b = DirectFunctionCall1Coll(
                cidr_in,
                InvalidOid,
                CStringGetDatum(c"192.168.1.0/24".as_ptr()),
            );
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                network_lt, InvalidOid, a, b
            )));
            assert!(DatumGetInt32(DirectFunctionCall2Coll(network_cmp, InvalidOid, a, b)) < 0);
            assert!(DatumGetBool(DirectFunctionCall2Coll(
                network_gt, InvalidOid, b, a
            )));
            assert!(!DatumGetBool(DirectFunctionCall2Coll(
                network_eq, InvalidOid, a, b
            )));
        }
    }

    #[test]
    #[should_panic]
    fn inet_in_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(
                inet_in,
                InvalidOid,
                CStringGetDatum(c"not-an-ip-address".as_ptr()),
            );
        }
    }
}
