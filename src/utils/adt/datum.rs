//! Translation of postgres/src/backend/utils/adt/datum.c
//!
//! Generic, type-agnostic Datum manipulation: datumGetSize / datumCopy /
//! datumTransfer / datumIsEqual / datum_image_eq / datum_image_hash, the
//! btequalimage support function, and the parallel-query (de)serialization
//! helpers datumEstimateSpace / datumSerialize / datumRestore.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! These work on a Datum plus its type's (typByVal, typLen) - they do NOT consult
//! the catalog.  `#include`s mapped: common/hashfn.h -> crate::common::hashfn
//! (hash_bytes), varatt.h -> crate::varatt.
//!
//! EXPANDED-OBJECT handling (utils/expandeddatum.h: DatumGetEOHP / EOH_get_flat_size
//! / EOH_flatten_into / TransferExpandedObject) is NOT translated; those branches
//! are guarded by VARATT_IS_EXTERNAL_EXPANDED (always false for a plain in-memory
//! datum), so they are unreachable on the translated paths and call unimplemented!().
//! toast_raw_datum_size (access/common/detoast.c) is implemented here for the plain/
//! short in-line case only (compressed/external -> TODO).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{PG_RETURN_BOOL};
use crate::c::{uint32, varlena, Pointer};
use crate::common::hashfn::hash_bytes;
use crate::postgres::{DatumGetCString, DatumGetPointer, PointerGetDatum};
use crate::varatt::{
    pg_detoast_datum_packed, VARATT_IS_EXTERNAL_EXPANDED, VARATT_IS_EXTERNAL_EXPANDED_RW,
    VARDATA_ANY, VARSIZE_ANY, VARSIZE_ANY_EXHDR,
};
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_DATA_EXCEPTION: c_int = 0;

// --- expandeddatum.h operations (not yet translated; only reached for expanded objects) ---
unsafe fn EOH_get_flat_size(_eoh: *mut c_void) -> Size { crate::utils::adt::expandeddatum::EOH_get_flat_size(_eoh as _) as _ }
unsafe fn EOH_flatten_into(_eoh: *mut c_void, _result: *mut c_void, _size: Size) {
    unimplemented!("EOH_flatten_into: expanded objects (utils/expandeddatum.c) not yet translated")
}
unsafe fn DatumGetEOHP(_value: Datum) -> *mut c_void { crate::utils::adt::expandeddatum::DatumGetEOHP(_value as _) as _ }
unsafe fn TransferExpandedObject(_value: Datum, _context: *mut c_void) -> Datum { crate::utils::adt::expandeddatum::TransferExpandedObject(_value as _, _context as _) as _ }

/*
 * toast_raw_datum_size (access/common/detoast.c): logical size (incl. VARHDRSZ) of
 * a possibly-toasted varlena.  Implemented for the plain/short in-line forms only.
 *
 * # Safety
 * `value` is a varlena Datum.
 */
unsafe fn toast_raw_datum_size(value: Datum) -> Size {
    let ptr = DatumGetPointer(value);
    if crate::varatt::VARATT_IS_EXTERNAL(ptr) || crate::varatt::VARATT_IS_COMPRESSED(ptr) {
        // TODO(pg-port): external/compressed raw size needs detoast.c + toast_internals.
        unimplemented!("toast_raw_datum_size: external/compressed (detoast.c) not yet translated")
    }
    /* plain (4B) or short (1B) in-line: raw size == VARHDRSZ + payload length */
    VARHDRSZ as Size + VARSIZE_ANY_EXHDR(ptr) as Size
}

/*
 * datumGetSize - find the "real" size of a datum, given its type info.
 *
 * # Safety
 * For pass-by-ref types, `value` must hold a valid pointer of the indicated kind.
 */
pub unsafe fn datumGetSize(value: Datum, typByVal: bool, typLen: c_int) -> Size {
    let size: Size;

    if typByVal {
        /* Pass-by-value types are always fixed-length */
        Assert!(typLen > 0 && typLen as usize <= core::mem::size_of::<Datum>());
        size = typLen as Size;
    } else if typLen > 0 {
        /* Fixed-length pass-by-ref type */
        size = typLen as Size;
    } else if typLen == -1 {
        /* It is a varlena datatype */
        let s = DatumGetPointer(value) as *const varlena;
        if !PointerIsValid(s) {
            let _ = errcode(ERRCODE_DATA_EXCEPTION);
            ereport!(ERROR, errmsg!("invalid Datum pointer"));
        }
        size = VARSIZE_ANY(s as *const c_char) as Size;
    } else if typLen == -2 {
        /* It is a cstring datatype */
        let s = DatumGetPointer(value) as *const c_char;
        if !PointerIsValid(s) {
            let _ = errcode(ERRCODE_DATA_EXCEPTION);
            ereport!(ERROR, errmsg!("invalid Datum pointer"));
        }
        size = strlen(s) + 1;
    } else {
        elog!(ERROR, "invalid typLen: {}", typLen);
        size = 0; /* keep compiler quiet */
    }

    size
}

/*
 * datumCopy - make a copy of a non-NULL datum.
 *
 * # Safety
 * As datumGetSize.
 */
#[no_mangle]
pub unsafe fn datumCopy(value: Datum, typByVal: bool, typLen: c_int) -> Datum {
    let res: Datum;

    if typByVal {
        res = value;
    } else if typLen == -1 {
        /* It is a varlena datatype */
        let vl = DatumGetPointer(value) as *const varlena;

        if VARATT_IS_EXTERNAL_EXPANDED(vl as *const c_char) {
            /* Flatten into the caller's memory context */
            let eoh = DatumGetEOHP(value);
            let resultsize = EOH_get_flat_size(eoh);
            let resultptr = palloc(resultsize) as *mut c_char;
            EOH_flatten_into(eoh, resultptr as *mut c_void, resultsize);
            res = PointerGetDatum(resultptr as *const c_void);
        } else {
            /* Otherwise, just copy the varlena datum verbatim */
            let real_size = VARSIZE_ANY(vl as *const c_char) as Size;
            let resultptr = palloc(real_size) as *mut c_char;
            core::ptr::copy_nonoverlapping(vl as *const c_char, resultptr, real_size);
            res = PointerGetDatum(resultptr as *const c_void);
        }
    } else {
        /* Pass by reference, but not varlena, so not toasted */
        let real_size = datumGetSize(value, typByVal, typLen);
        let resultptr = palloc(real_size) as *mut c_char;
        core::ptr::copy_nonoverlapping(DatumGetPointer(value) as *const c_char, resultptr, real_size);
        res = PointerGetDatum(resultptr as *const c_void);
    }
    res
}

/*
 * datumTransfer - transfer a non-NULL datum into the current memory context.
 *
 * # Safety
 * As datumCopy.
 */
#[no_mangle]
pub unsafe fn datumTransfer(mut value: Datum, typByVal: bool, typLen: c_int) -> Datum {
    if !typByVal
        && typLen == -1
        && VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(value) as *const c_char)
    {
        value = TransferExpandedObject(value, CurrentMemoryContext as *mut c_void);
    } else {
        value = datumCopy(value, typByVal, typLen);
    }
    value
}

/*
 * datumIsEqual - byte-by-byte equality of two datums (NOT toast-aware).
 *
 * # Safety
 * As datumGetSize.
 */
pub unsafe fn datumIsEqual(value1: Datum, value2: Datum, typByVal: bool, typLen: c_int) -> bool {
    let res: bool;

    if typByVal {
        res = value1 == value2;
    } else {
        let size1 = datumGetSize(value1, typByVal, typLen);
        let size2 = datumGetSize(value2, typByVal, typLen);
        if size1 != size2 {
            return false;
        }
        let s1 = DatumGetPointer(value1) as *const c_void;
        let s2 = DatumGetPointer(value2) as *const c_void;
        res = memcmp(s1, s2, size1) == 0;
    }
    res
}

/*
 * datum_image_eq - byte-image equality (toast-aware for varlena).
 *
 * # Safety
 * As datumGetSize.
 */
#[no_mangle]
pub unsafe fn datum_image_eq(value1: Datum, value2: Datum, typByVal: bool, typLen: c_int) -> bool {
    let len1: Size;
    let len2: Size;
    let mut result = true;

    if typByVal {
        result = value1 == value2;
    } else if typLen > 0 {
        result = memcmp(
            DatumGetPointer(value1) as *const c_void,
            DatumGetPointer(value2) as *const c_void,
            typLen as usize,
        ) == 0;
    } else if typLen == -1 {
        len1 = toast_raw_datum_size(value1);
        len2 = toast_raw_datum_size(value2);
        /* No need to de-toast if lengths don't match. */
        if len1 != len2 {
            result = false;
        } else {
            let arg1val = pg_detoast_datum_packed(DatumGetPointer(value1) as *mut c_void) as *mut varlena;
            let arg2val = pg_detoast_datum_packed(DatumGetPointer(value2) as *mut c_void) as *mut varlena;

            result = memcmp(
                VARDATA_ANY(arg1val as *const c_char) as *const c_void,
                VARDATA_ANY(arg2val as *const c_char) as *const c_void,
                (len1 - VARHDRSZ as Size) as usize,
            ) == 0;

            /* Only free memory if it's a copy made here. */
            if arg1val as Pointer != DatumGetPointer(value1) {
                pfree(arg1val as *mut c_void);
            }
            if arg2val as Pointer != DatumGetPointer(value2) {
                pfree(arg2val as *mut c_void);
            }
        }
    } else if typLen == -2 {
        let s1 = DatumGetCString(value1);
        let s2 = DatumGetCString(value2);
        len1 = strlen(s1) + 1;
        len2 = strlen(s2) + 1;
        if len1 != len2 {
            return false;
        }
        result = memcmp(s1 as *const c_void, s2 as *const c_void, len1) == 0;
    } else {
        elog!(ERROR, "unexpected typLen: {}", typLen);
    }

    result
}

/*
 * datum_image_hash - hash of the binary representation of 'value'.
 *
 * # Safety
 * As datumGetSize.
 */
pub unsafe fn datum_image_hash(value: Datum, typByVal: bool, typLen: c_int) -> uint32 {
    let len: Size;
    let result: uint32;

    if typByVal {
        result = hash_bytes(&value as *const Datum as *const u8, core::mem::size_of::<Datum>() as c_int);
    } else if typLen > 0 {
        result = hash_bytes(DatumGetPointer(value) as *const u8, typLen);
    } else if typLen == -1 {
        len = toast_raw_datum_size(value);
        let val = pg_detoast_datum_packed(DatumGetPointer(value) as *mut c_void) as *mut varlena;
        result = hash_bytes(
            VARDATA_ANY(val as *const c_char) as *const u8,
            (len - VARHDRSZ as Size) as c_int,
        );
        if val as Pointer != DatumGetPointer(value) {
            pfree(val as *mut c_void);
        }
    } else if typLen == -2 {
        let s = DatumGetCString(value);
        len = strlen(s) + 1;
        result = hash_bytes(s as *const u8, len as c_int);
    } else {
        elog!(ERROR, "unexpected typLen: {}", typLen);
        result = 0;
    }

    result
}

/*
 * btequalimage - generic "equalimage" B-Tree support function (always true).
 */
pub unsafe fn btequalimage(fcinfo: FunctionCallInfo) -> Datum {
    /* Oid opcintype = PG_GETARG_OID(0); */
    let _ = fcinfo;
    PG_RETURN_BOOL!(true);
}

/*
 * datumEstimateSpace - bytes datumSerialize will need for a Datum.
 *
 * # Safety
 * As datumGetSize.
 */
#[no_mangle]
pub unsafe fn datumEstimateSpace(value: Datum, isnull: bool, typByVal: bool, typLen: c_int) -> Size {
    let mut sz: Size = core::mem::size_of::<c_int>();

    if !isnull {
        if typByVal {
            sz += core::mem::size_of::<Datum>();
        } else if typLen == -1 && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value) as *const c_char) {
            sz += EOH_get_flat_size(DatumGetEOHP(value));
        } else {
            sz += datumGetSize(value, typByVal, typLen);
        }
    }

    sz
}

/*
 * datumSerialize - serialize a possibly-NULL datum into caller storage.
 *
 * # Safety
 * `*start_address` must have room for datumEstimateSpace(value) bytes.
 */
#[no_mangle]
pub unsafe fn datumSerialize(
    value: Datum,
    isnull: bool,
    typByVal: bool,
    typLen: c_int,
    start_address: *mut *mut c_char,
) {
    let mut eoh: *mut c_void = null_mut();
    let header: c_int;

    /* Write header word. */
    if isnull {
        header = -2;
    } else if typByVal {
        header = -1;
    } else if typLen == -1 && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value) as *const c_char) {
        eoh = DatumGetEOHP(value);
        header = EOH_get_flat_size(eoh) as c_int;
    } else {
        header = datumGetSize(value, typByVal, typLen) as c_int;
    }
    core::ptr::copy_nonoverlapping(
        &header as *const c_int as *const c_char,
        *start_address,
        core::mem::size_of::<c_int>(),
    );
    *start_address = (*start_address).add(core::mem::size_of::<c_int>());

    /* If not null, write payload bytes. */
    if !isnull {
        if typByVal {
            core::ptr::copy_nonoverlapping(
                &value as *const Datum as *const c_char,
                *start_address,
                core::mem::size_of::<Datum>(),
            );
            *start_address = (*start_address).add(core::mem::size_of::<Datum>());
        } else if !eoh.is_null() {
            /* EOH_flatten_into needs a maxaligned target, so go via a temp. */
            let tmp = palloc(header as Size) as *mut c_char;
            EOH_flatten_into(eoh, tmp as *mut c_void, header as Size);
            core::ptr::copy_nonoverlapping(tmp, *start_address, header as usize);
            *start_address = (*start_address).add(header as usize);
            pfree(tmp as *mut c_void);
        } else {
            core::ptr::copy_nonoverlapping(
                DatumGetPointer(value) as *const c_char,
                *start_address,
                header as usize,
            );
            *start_address = (*start_address).add(header as usize);
        }
    }
}

/*
 * datumRestore - restore a datum previously serialized by datumSerialize.
 *
 * # Safety
 * `*start_address` points at a buffer produced by datumSerialize.
 */
#[no_mangle]
pub unsafe fn datumRestore(start_address: *mut *mut c_char, isnull: *mut bool) -> Datum {
    let mut header: c_int = 0;

    /* Read header word. */
    core::ptr::copy_nonoverlapping(
        *start_address as *const c_char,
        &mut header as *mut c_int as *mut c_char,
        core::mem::size_of::<c_int>(),
    );
    *start_address = (*start_address).add(core::mem::size_of::<c_int>());

    /* If this datum is NULL, we can stop here. */
    if header == -2 {
        *isnull = true;
        return 0 as Datum;
    }

    *isnull = false;

    /* If this datum is pass-by-value, sizeof(Datum) bytes follow. */
    if header == -1 {
        let mut val: Datum = 0;
        core::ptr::copy_nonoverlapping(
            *start_address as *const c_char,
            &mut val as *mut Datum as *mut c_char,
            core::mem::size_of::<Datum>(),
        );
        *start_address = (*start_address).add(core::mem::size_of::<Datum>());
        return val;
    }

    /* Pass-by-reference case; copy indicated number of bytes. */
    Assert!(header > 0);
    let d = palloc(header as Size) as *mut c_char;
    core::ptr::copy_nonoverlapping(*start_address as *const c_char, d, header as usize);
    *start_address = (*start_address).add(header as usize);
    PointerGetDatum(d as *const c_void)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{Int32GetDatum, DatumGetInt32};
    use crate::utils::adt::varlena::{cstring_to_text, text_to_cstring};

    #[test]
    fn datum_copy_size_equal_serialize() {
        unsafe {
            // by-value int4
            let v = Int32GetDatum(0x12345678);
            assert_eq!(datumGetSize(v, true, 4), 4);
            assert_eq!(DatumGetInt32(datumCopy(v, true, 4)), 0x12345678);
            assert!(datumIsEqual(v, Int32GetDatum(0x12345678), true, 4));
            assert!(!datumIsEqual(v, Int32GetDatum(9), true, 4));
            assert!(datum_image_eq(v, Int32GetDatum(0x12345678), true, 4));

            // varlena text (typLen == -1)
            let t = cstring_to_text(c"hello varlena".as_ptr()) as Datum;
            // datumGetSize == VARSIZE (hdr + 13)
            assert_eq!(datumGetSize(t, false, -1), (VARHDRSZ as Size) + 13);
            // datumCopy makes a distinct but byte-equal copy
            let tc = datumCopy(t, false, -1);
            assert_ne!(tc, t);
            assert!(datumIsEqual(t, tc, false, -1));
            assert!(datum_image_eq(t, tc, false, -1));
            // the copy round-trips back to the same string
            let s = text_to_cstring(DatumGetPointer(tc) as *const crate::c::text);
            assert_eq!(core::slice::from_raw_parts(s as *const u8, 13), b"hello varlena");
            // image hash is stable across copies
            assert_eq!(datum_image_hash(t, false, -1), datum_image_hash(tc, false, -1));

            // serialize -> restore round trip (by-value + by-ref)
            let space = datumEstimateSpace(t, false, false, -1);
            let buf = palloc(space) as *mut c_char;
            let mut p = buf;
            datumSerialize(t, false, false, -1, &mut p);
            let mut rp = buf;
            let mut isnull = true;
            let restored = datumRestore(&mut rp, &mut isnull);
            assert!(!isnull);
            assert!(datumIsEqual(t, restored, false, -1));
        }
    }
}
