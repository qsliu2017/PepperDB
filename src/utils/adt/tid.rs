//! Translation of postgres/src/backend/utils/adt/tid.c
//!
//! Functions for the built-in type `tid` (ItemPointer / a tuple's ctid).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: storage/{itemptr,block,off}.h -> crate::storage::*,
//! common/hashfn.h -> crate::common::hashfn, libpq/pqformat -> crate::libpq::pqformat.
//! libc strtoul/snprintf + errno bound via extern "C".
//!
//! STUBBED: the currtid_* family (currtid_byrelname etc.) needs the heap/relation/
//! SPI layer (access/heapam, utils/rel, executor/spi) - not yet translated.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_CSTRING,
    PG_RETURN_INT32,
};
use crate::c::{int32, uint64};
use crate::storage::block::{BlockIdData, BlockNumber};
use crate::storage::off::OffsetNumber;
use crate::storage::itemptr::{
    DatumGetItemPointer, ItemPointer, ItemPointerCompare, ItemPointerData,
    ItemPointerGetBlockNumberNoCheck, ItemPointerGetDatum, ItemPointerGetOffsetNumberNoCheck,
    ItemPointerSet,
};
use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_sendint16, pq_sendint32,
};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::postgres::PointerGetDatum;
use crate::nodes::nodes::Node;
use core::ffi::{c_char, c_int, c_ulong, c_void};

const LDELIM: u8 = b'(';
const RDELIM: u8 = b')';
const DELIM: u8 = b',';
const NTIDARGS: usize = 2;
const USHRT_MAX: c_ulong = 65535;

extern "C" {
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}

const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;

unsafe fn tidin_syntax_error(str: *const c_char, _escontext: *mut Node) {
    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereport!(
        ERROR,
        errmsg!("invalid input syntax for type {}: \"{}\"", "tid", cstr(str))
    );
}

/* ----------------
 *		tidin
 * ---------------- */
pub unsafe fn tidin(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING
    let escontext: *mut Node = (*fcinfo).context;
    let mut coord: [*mut c_char; NTIDARGS] = [null_mut(); NTIDARGS];
    let mut i: usize = 0;
    let result: ItemPointer;
    let block_number: BlockNumber;
    let offset_number: OffsetNumber;
    let mut badp: *mut c_char = null_mut();
    let mut cvt: c_ulong;

    let mut p = str;
    while *p != 0 && i < NTIDARGS && *p as u8 != RDELIM {
        if *p as u8 == DELIM || (*p as u8 == LDELIM && i == 0) {
            coord[i] = p.add(1);
            i += 1;
        }
        p = p.add(1);
    }

    if i < NTIDARGS {
        tidin_syntax_error(str, escontext);
        return 0 as Datum;
    }

    *errno_location() = 0;
    cvt = strtoul(coord[0], &mut badp, 10);
    if *errno_location() != 0 || *badp as u8 != DELIM {
        tidin_syntax_error(str, escontext);
        return 0 as Datum;
    }
    block_number = cvt as BlockNumber;

    /* Cope with unsigned long being wider than BlockNumber (LP64). */
    if cvt != block_number as c_ulong && cvt != (block_number as i32) as c_ulong {
        tidin_syntax_error(str, escontext);
        return 0 as Datum;
    }

    cvt = strtoul(coord[1], &mut badp, 10);
    if *errno_location() != 0 || *badp as u8 != RDELIM || cvt > USHRT_MAX {
        tidin_syntax_error(str, escontext);
        return 0 as Datum;
    }
    offset_number = cvt as OffsetNumber;

    result = palloc(core::mem::size_of::<ItemPointerData>()) as ItemPointer;
    ItemPointerSet(result, block_number, offset_number);

    return ItemPointerGetDatum(result); // PG_RETURN_ITEMPOINTER
}

/* ----------------
 *		tidout
 * ---------------- */
pub unsafe fn tidout(fcinfo: FunctionCallInfo) -> Datum {
    let item_ptr: ItemPointer = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let block_number: BlockNumber = ItemPointerGetBlockNumberNoCheck(item_ptr);
    let offset_number: OffsetNumber = ItemPointerGetOffsetNumberNoCheck(item_ptr);
    let mut buf = [0i8; 32];

    snprintf(
        buf.as_mut_ptr(),
        32,
        c"(%u,%u)".as_ptr(),
        block_number as core::ffi::c_uint,
        offset_number as core::ffi::c_uint,
    );

    PG_RETURN_CSTRING!(pstrdup(buf.as_ptr()));
}

/*
 *		tidrecv			- converts external binary format to tid
 */
pub unsafe fn tidrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let result: ItemPointer;

    let block_number: BlockNumber =
        pq_getmsgint(buf, core::mem::size_of::<BlockNumber>() as c_int) as BlockNumber;
    let offset_number: OffsetNumber =
        pq_getmsgint(buf, core::mem::size_of::<OffsetNumber>() as c_int) as OffsetNumber;

    result = palloc(core::mem::size_of::<ItemPointerData>()) as ItemPointer;
    ItemPointerSet(result, block_number, offset_number);

    return ItemPointerGetDatum(result);
}

/*
 *		tidsend			- converts tid to binary format
 */
pub unsafe fn tidsend(fcinfo: FunctionCallInfo) -> Datum {
    let item_ptr: ItemPointer = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint32(&mut buf, ItemPointerGetBlockNumberNoCheck(item_ptr));
    pq_sendint16(&mut buf, ItemPointerGetOffsetNumberNoCheck(item_ptr));
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

pub unsafe fn tideq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(ItemPointerCompare(arg1, arg2) == 0);
}
pub unsafe fn tidne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(ItemPointerCompare(arg1, arg2) != 0);
}
pub unsafe fn tidlt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(ItemPointerCompare(arg1, arg2) < 0);
}
pub unsafe fn tidle(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(ItemPointerCompare(arg1, arg2) <= 0);
}
pub unsafe fn tidgt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(ItemPointerCompare(arg1, arg2) > 0);
}
pub unsafe fn tidge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(ItemPointerCompare(arg1, arg2) >= 0);
}

pub unsafe fn bttidcmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_INT32!(ItemPointerCompare(arg1, arg2));
}

pub unsafe fn tidlarger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    return ItemPointerGetDatum(if ItemPointerCompare(arg1, arg2) >= 0 { arg1 } else { arg2 });
}
pub unsafe fn tidsmaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 1));
    return ItemPointerGetDatum(if ItemPointerCompare(arg1, arg2) <= 0 { arg1 } else { arg2 });
}

pub unsafe fn hashtid(fcinfo: FunctionCallInfo) -> Datum {
    let key: ItemPointer = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    /* rely on the component field sizes, not sizeof(ItemPointerData) (no pad) */
    hash_any(
        key as *const core::ffi::c_uchar,
        (core::mem::size_of::<BlockIdData>() + core::mem::size_of::<OffsetNumber>()) as c_int,
    )
}

pub unsafe fn hashtidextended(fcinfo: FunctionCallInfo) -> Datum {
    let key: ItemPointer = DatumGetItemPointer(PG_GETARG_DATUM!(fcinfo, 0));
    let seed: uint64 = PG_GETARG_INT64!(fcinfo, 1) as uint64;
    hash_any_extended(
        key as *const core::ffi::c_uchar,
        (core::mem::size_of::<BlockIdData>() + core::mem::size_of::<OffsetNumber>()) as c_int,
        seed,
    )
}

/*
 *	currtid_byrelname - get the latest tid of a tuple in a named relation.  [STUBBED]
 */
pub unsafe fn currtid_byrelname(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): needs the heap/relation/snapshot layer (access/heapam, utils/rel,
    // table_tuple_get_latest_tid) - not yet translated.
    let _ = fcinfo;
    unimplemented!("currtid_byrelname: heap/relation layer not yet translated")
}

/*
 * Format a C string for an error message via Rust `{}` (lossy).
 *
 * # Safety
 * `s` is a valid NUL-terminated C string.
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    std::string::String::from_utf8_lossy(core::slice::from_raw_parts(s as *const u8, n)).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn tid_io_compare_hash() {
        unsafe {
            // tidin "(42,7)" -> tidout round trip
            let a = DirectFunctionCall1Coll(tidin, InvalidOid, CStringGetDatum(c"(42,7)".as_ptr()));
            let s = DatumGetCString(DirectFunctionCall1Coll(tidout, InvalidOid, a));
            assert!(cstr_eq(s, "(42,7)"));

            // ordering: (42,7) < (42,9) < (43,0)
            let b = DirectFunctionCall1Coll(tidin, InvalidOid, CStringGetDatum(c"(42,9)".as_ptr()));
            let c = DirectFunctionCall1Coll(tidin, InvalidOid, CStringGetDatum(c"(43,0)".as_ptr()));
            assert!(DatumGetBool(DirectFunctionCall2Coll(tidlt, InvalidOid, a, b)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(tidlt, InvalidOid, b, c)));
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(bttidcmp, InvalidOid, a, b)), -1);
            assert!(DatumGetBool(DirectFunctionCall2Coll(tideq, InvalidOid, a, a)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(tidne, InvalidOid, a, b)));

            // equal-value tids hash equally
            let a2 = DirectFunctionCall1Coll(tidin, InvalidOid, CStringGetDatum(c"(42,7)".as_ptr()));
            assert_eq!(
                DatumGetInt32(hashtid_dfc(a)),
                DatumGetInt32(hashtid_dfc(a2))
            );
        }
    }

    // helper: hashtid returns a Datum (hash), call directly
    unsafe fn hashtid_dfc(d: Datum) -> Datum {
        DirectFunctionCall1Coll(hashtid, InvalidOid, d)
    }

    #[test]
    #[should_panic]
    fn tidin_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(tidin, InvalidOid, CStringGetDatum(c"not a tid".as_ptr()));
        }
    }
}
