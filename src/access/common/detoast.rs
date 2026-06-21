//! Translation of postgres/src/backend/access/common/detoast.c
//!
//! Retrieve compressed or external variable size attributes (the TOAST
//! fetch/decompress machinery).
//!
//! Copyright (c) 2000-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped:
//!   - access/detoast.h        -> VARATT_EXTERNAL_GET_POINTER (defined locally here)
//!   - access/table.h / tableam.h -> table_open/table_close + table_relation_fetch_toast_slice
//!       (heap/relation layer -- STUBBED, not yet ported)
//!   - access/toast_internals.h / toast_compression.h -> toast_compress_header layout +
//!       TOAST_COMPRESS_METHOD/EXTSIZE, ToastCompressionId (defined locally here)
//!   - common/int.h            -> crate::common::int::pg_add_s32_overflow
//!   - common/pg_lzcompress.h  -> crate::common::pg_lzcompress::{pglz_decompress,
//!       pglz_maximum_compressed_size}
//!   - utils/expandeddatum.h   -> ExpandedObjectHeader / EOH_* (expanded objects -- STUBBED)
//!   - utils/rel.h             -> Relation (STUBBED)
//!
//! IMPLEMENTED FULLY:
//!   - the COMPRESSED path: toast_decompress_datum / toast_decompress_datum_slice,
//!     dispatching to pglz_decompress (TOAST_PGLZ_COMPRESSION_ID).  The pglz_*_datum
//!     wrappers (which live in toast_compression.c, not yet ported) are inlined here.
//!   - detoast_attr / detoast_attr_slice / detoast_external_attr for the in-line
//!     (compressed / short / plain) cases, plus the indirect-pointer deref/copy logic.
//!   - toast_raw_datum_size / toast_datum_size for the in-line cases.
//!   - pg_detoast_datum / _copy / _packed / _slice (the PG_DETOAST_* entry points;
//!     these live in fmgr.h in C but their bodies are simple wrappers around the
//!     above and are most naturally hosted alongside detoast_attr).
//!
//! STUBBED (heap/relation layer + expanded objects not yet ported):
//!   - toast_fetch_datum / toast_fetch_datum_slice (need table_open/close +
//!     table_relation_fetch_toast_slice from access/tableam).
//!   - the EXTERNAL_INDIRECT pointer deref reads varatt_indirect from the datum (real),
//!     but the LZ4 decompress path and the EXTERNAL_EXPANDED flatten path are stubbed.
//!
//! INTEGRATOR NOTE: src/varatt.rs currently defines a stub
//!   `pub unsafe fn pg_detoast_datum_packed(datum: *mut c_void) -> *mut c_void`
//! (identity-for-plain, unimplemented! for TOAST).  This file defines the *real*
//! `pg_detoast_datum_packed` (and friends) with the proper `*mut varlena` signature.
//! The integrator should repoint callers at `crate::access::common::detoast::*` and
//! remove the varatt.rs stub to avoid the clash.

use crate::prelude::*;
use crate::varatt::*;

use crate::common::int::pg_add_s32_overflow;
use crate::common::pg_lzcompress::{pglz_decompress, pglz_maximum_compressed_size};

use crate::c::varlena;
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_void};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------
//   Symbols not (yet) provided by crate::varatt -- defined locally.
//
//   These mirror varatt.h / toast_internals.h / toast_compression.h and should
//   migrate into varatt.rs / a toast_compression.rs when those land.
// ----------------------------------------------------------------

/* ToastCompressionId enum (toast_compression.h). */
type ToastCompressionId = u32;

/* toast_compression.h: built-in compression method IDs (2 bits in va_tcinfo). */
const TOAST_PGLZ_COMPRESSION_ID: ToastCompressionId = 0;
const TOAST_LZ4_COMPRESSION_ID: ToastCompressionId = 1;
#[allow(dead_code)]
const TOAST_INVALID_COMPRESSION_ID: ToastCompressionId = 2;

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_DATA_CORRUPTED: c_int = 0;

/* varatt.h: "saved size" portion of va_extinfo / va_tcinfo. */
const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: uint32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/*
 * varatt.h: struct varatt_external is a traditional "TOAST pointer", that is,
 * the information needed to fetch a Datum stored out-of-line in a TOAST table.
 * Stored UNALIGNED inside tuples, so always memcpy into a local before reading.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct varatt_external {
    pub va_rawsize: int32,   /* Original data size (includes header) */
    pub va_extinfo: uint32,  /* External saved size (w/o header) + compression method */
    pub va_valueid: Oid,     /* Unique ID of value within TOAST table */
    pub va_toastrelid: Oid,  /* RelID of TOAST table containing it */
}

/*
 * varatt.h: struct varatt_indirect is a "TOAST pointer" referencing an
 * out-of-line Datum stored in memory (not in a TOAST relation).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct varatt_indirect {
    pub pointer: *mut varlena, /* Pointer to in-memory varlena */
}

/*
 * toast_internals.h: header at the start of compressed toast data.
 *	int32  vl_len_;  varlena header (do not touch directly!)
 *	uint32 tcinfo;   2 bits compression method + 30 bits external (raw) size.
 */
#[repr(C)]
struct toast_compress_header {
    vl_len_: int32,
    tcinfo: uint32,
}

/* varatt.h: VARHDRSZ_COMPRESSED == offsetof(varattrib_4b, va_compressed.va_data). */
const VARHDRSZ_COMPRESSED: usize = core::mem::size_of::<toast_compress_header>();

/* toast_internals.h: TOAST_COMPRESS_EXTSIZE / TOAST_COMPRESS_METHOD. */
#[inline]
unsafe fn TOAST_COMPRESS_EXTSIZE(ptr: *const c_char) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo & VARLENA_EXTSIZE_MASK
}
#[inline]
unsafe fn TOAST_COMPRESS_METHOD(ptr: *const c_char) -> ToastCompressionId {
    (*(ptr as *const toast_compress_header)).tcinfo >> VARLENA_EXTSIZE_BITS
}

/* varatt.h: decompressed size of a compressed-in-line Datum (va_compressed.va_tcinfo). */
#[inline]
unsafe fn VARDATA_COMPRESSED_GET_EXTSIZE(ptr: *const c_char) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo & VARLENA_EXTSIZE_MASK
}

/* varatt.h: VARATT_EXTERNAL_GET_EXTSIZE / VARATT_EXTERNAL_GET_COMPRESS_METHOD. */
#[inline]
fn VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer: varatt_external) -> int32 {
    (toast_pointer.va_extinfo & VARLENA_EXTSIZE_MASK) as int32
}
#[inline]
fn VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer: varatt_external) -> ToastCompressionId {
    toast_pointer.va_extinfo >> VARLENA_EXTSIZE_BITS
}

/*
 * varatt.h: an externally-stored value is compressed iff the actual external
 * length is less than the original raw size (which includes VARHDRSZ).
 */
#[inline]
fn VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer: varatt_external) -> bool {
    VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < (toast_pointer.va_rawsize - VARHDRSZ)
}

/*
 * detoast.h: VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr).
 *
 * Copies the possibly-unaligned TOAST-pointer payload of an EXTERNAL datum into
 * a local struct.  Generic over the pointer struct type (varatt_external or
 * varatt_indirect), matching the C macro's behaviour.
 *
 * # Safety
 * `attr` is a valid EXTERNAL varlena whose payload is at least `size_of::<T>()`.
 */
#[inline]
unsafe fn VARATT_EXTERNAL_GET_POINTER<T: Copy>(attr: *const varlena) -> T {
    Assert!(VARATT_IS_EXTERNAL(attr as *const c_char));
    let mut out: core::mem::MaybeUninit<T> = core::mem::MaybeUninit::uninit();
    memcpy(
        out.as_mut_ptr() as *mut c_void,
        VARDATA_1B_E(attr as *const c_char) as *const c_void, /* VARDATA_EXTERNAL */
        core::mem::size_of::<T>(),
    );
    out.assume_init()
}

/*
 * varatt.h externally-visible macros MISSING from crate::varatt -- defined
 * locally as thin wrappers (per the "use it / TODO if missing" convention).
 * These should migrate into varatt.rs when it grows the external-TOAST layer.
 */

/* VARSIZE_SHORT(PTR) == VARSIZE_1B(PTR). */
#[inline]
unsafe fn VARSIZE_SHORT(ptr: *const c_char) -> uint32 {
    VARSIZE_1B(ptr)
}
/* VARDATA_SHORT(PTR) == VARDATA_1B(PTR). */
#[inline]
unsafe fn VARDATA_SHORT(ptr: *const c_char) -> *mut c_char {
    VARDATA_1B(ptr)
}
/* SET_VARSIZE_COMPRESSED(PTR, len) == SET_VARSIZE_4B_C(PTR, len). */
#[inline]
unsafe fn SET_VARSIZE_COMPRESSED(ptr: *mut c_char, len: int32) {
    SET_VARSIZE_4B_C(ptr, len);
}
/* VARTAG_EXTERNAL(PTR) == VARTAG_1B_E(PTR). */
#[inline]
unsafe fn VARTAG_EXTERNAL(ptr: *const c_char) -> uint8 {
    VARTAG_1B_E(ptr)
}
/* VARATT_IS_EXTERNAL_ONDISK(PTR). */
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == VARTAG_ONDISK
}
/* VARATT_IS_EXTERNAL_INDIRECT(PTR). */
#[inline]
unsafe fn VARATT_IS_EXTERNAL_INDIRECT(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == VARTAG_INDIRECT
}

/* utils/expandeddatum.h: opaque expanded-object header (not yet ported). */
#[allow(non_camel_case_types)]
pub enum ExpandedObjectHeader {}

/* utils/rel.h: Relation (not yet ported). */
#[allow(non_camel_case_types)]
type Relation = *mut c_void;
/* storage/lockdefs.h */
#[allow(dead_code)]
const AccessShareLock: c_int = 1;

// ----------------------------------------------------------------
//   detoast.c proper
// ----------------------------------------------------------------

/* ----------
 * detoast_external_attr -
 *
 *	Public entry point to get back a toasted value from
 *	external source (possibly still in compressed format).
 *
 * This will return a datum that contains all the data internally, ie, not
 * relying on external storage or memory, but it can still be compressed or
 * have a short header.  Note some callers assume that if the input is an
 * EXTERNAL datum, the result will be a pfree'able chunk.
 * ----------
 *
 * # Safety
 * `attr` points to a valid varlena.
 */
#[no_mangle]
pub unsafe fn detoast_external_attr(mut attr: *mut varlena) -> *mut varlena {
    let result: *mut varlena;

    if VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        /*
         * This is an external stored plain value
         */
        result = toast_fetch_datum(attr);
    } else if VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char) {
        /*
         * This is an indirect pointer --- dereference it
         */
        let redirect: varatt_indirect = VARATT_EXTERNAL_GET_POINTER(attr);
        attr = redirect.pointer;

        /* nested indirect Datums aren't allowed */
        Assert!(!VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char));

        /* recurse if value is still external in some other way */
        if VARATT_IS_EXTERNAL(attr as *const c_char) {
            return detoast_external_attr(attr);
        }

        /*
         * Copy into the caller's memory context, in case caller tries to
         * pfree the result.
         */
        result = palloc(VARSIZE_ANY(attr as *const c_char) as usize) as *mut varlena;
        memcpy(
            result as *mut c_void,
            attr as *const c_void,
            VARSIZE_ANY(attr as *const c_char) as usize,
        );
    } else if VARATT_IS_EXTERNAL_EXPANDED(attr as *const c_char) {
        /*
         * This is an expanded-object pointer --- get flat format
         */
        // TODO(pg-port): utils/expandeddatum.h (DatumGetEOHP / EOH_get_flat_size /
        // EOH_flatten_into) not yet translated.
        //
        // ExpandedObjectHeader *eoh;
        // Size resultsize;
        // eoh = DatumGetEOHP(PointerGetDatum(attr));
        // resultsize = EOH_get_flat_size(eoh);
        // result = (struct varlena *) palloc(resultsize);
        // EOH_flatten_into(eoh, result, resultsize);
        unimplemented!("detoast_external_attr: expanded-object flatten (expandeddatum.h) not yet translated")
    } else {
        /*
         * This is a plain value inside of the main tuple - why am I called?
         */
        result = attr;
    }

    result
}

/* ----------
 * detoast_attr -
 *
 *	Public entry point to get back a toasted value from compression
 *	or external storage.  The result is always non-extended varlena form.
 *
 * Note some callers assume that if the input is an EXTERNAL or COMPRESSED
 * datum, the result will be a pfree'able chunk.
 * ----------
 *
 * # Safety
 * `attr` points to a valid varlena.
 */
pub unsafe fn detoast_attr(mut attr: *mut varlena) -> *mut varlena {
    if VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        /*
         * This is an externally stored datum --- fetch it back from there
         */
        attr = toast_fetch_datum(attr);
        /* If it's compressed, decompress it */
        if VARATT_IS_COMPRESSED(attr as *const c_char) {
            let tmp: *mut varlena = attr;

            attr = toast_decompress_datum(tmp);
            pfree(tmp as *mut c_void);
        }
    } else if VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char) {
        /*
         * This is an indirect pointer --- dereference it
         */
        let redirect: varatt_indirect = VARATT_EXTERNAL_GET_POINTER(attr);
        attr = redirect.pointer;

        /* nested indirect Datums aren't allowed */
        Assert!(!VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char));

        /* recurse in case value is still extended in some other way */
        attr = detoast_attr(attr);

        /* if it isn't, we'd better copy it */
        if attr == redirect.pointer {
            let result: *mut varlena;

            result = palloc(VARSIZE_ANY(attr as *const c_char) as usize) as *mut varlena;
            memcpy(
                result as *mut c_void,
                attr as *const c_void,
                VARSIZE_ANY(attr as *const c_char) as usize,
            );
            attr = result;
        }
    } else if VARATT_IS_EXTERNAL_EXPANDED(attr as *const c_char) {
        /*
         * This is an expanded-object pointer --- get flat format
         */
        attr = detoast_external_attr(attr);
        /* flatteners are not allowed to produce compressed/short output */
        Assert!(!VARATT_IS_EXTENDED(attr as *const c_char));
    } else if VARATT_IS_COMPRESSED(attr as *const c_char) {
        /*
         * This is a compressed value inside of the main tuple
         */
        attr = toast_decompress_datum(attr);
    } else if VARATT_IS_SHORT(attr as *const c_char) {
        /*
         * This is a short-header varlena --- convert to 4-byte header format
         */
        let data_size: Size =
            VARSIZE_SHORT(attr as *const c_char) as Size - VARHDRSZ_SHORT as Size;
        let new_size: Size = data_size + VARHDRSZ as Size;
        let new_attr: *mut varlena;

        new_attr = palloc(new_size) as *mut varlena;
        SET_VARSIZE(new_attr as *mut c_char, new_size as int32);
        memcpy(
            VARDATA(new_attr as *const c_char) as *mut c_void,
            VARDATA_SHORT(attr as *const c_char) as *const c_void,
            data_size,
        );
        attr = new_attr;
    }

    attr
}

/* ----------
 * detoast_attr_slice -
 *
 *		Public entry point to get back part of a toasted value
 *		from compression or external storage.
 *
 * sliceoffset is where to start (zero or more)
 * If slicelength < 0, return everything beyond sliceoffset
 * ----------
 *
 * # Safety
 * `attr` points to a valid varlena.
 */
pub unsafe fn detoast_attr_slice(
    attr: *mut varlena,
    mut sliceoffset: int32,
    mut slicelength: int32,
) -> *mut varlena {
    let mut preslice: *mut varlena;
    let result: *mut varlena;
    let attrdata: *mut c_char;
    let mut slicelimit: int32 = 0;
    let attrsize: int32;

    if sliceoffset < 0 {
        elog!(ERROR, "invalid sliceoffset: {}", sliceoffset);
    }

    /*
     * Compute slicelimit = offset + length, or -1 if we must fetch all of the
     * value.  In case of integer overflow, we must fetch all.
     */
    if slicelength < 0 {
        slicelimit = -1;
    } else if pg_add_s32_overflow(sliceoffset, slicelength, &mut slicelimit) {
        slicelength = -1;
        slicelimit = -1;
    }

    if VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        let toast_pointer: varatt_external = VARATT_EXTERNAL_GET_POINTER(attr);

        /* fast path for non-compressed external datums */
        if !VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) {
            return toast_fetch_datum_slice(attr, sliceoffset, slicelength);
        }

        /*
         * For compressed values, we need to fetch enough slices to decompress
         * at least the requested part (when a prefix is requested).
         * Otherwise, just fetch all slices.
         */
        if slicelimit >= 0 {
            let mut max_size: int32 = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

            /*
             * Determine maximum amount of compressed data needed for a prefix
             * of a given length (after decompression).
             *
             * At least for now, if it's LZ4 data, we'll have to fetch the
             * whole thing, because there doesn't seem to be an API call to
             * determine how much compressed data we need to be sure of being
             * able to decompress the required slice.
             */
            if VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer) == TOAST_PGLZ_COMPRESSION_ID {
                max_size = pglz_maximum_compressed_size(slicelimit, max_size);
            }

            /*
             * Fetch enough compressed slices (compressed marker will get set
             * automatically).
             */
            preslice = toast_fetch_datum_slice(attr, 0, max_size);
        } else {
            preslice = toast_fetch_datum(attr);
        }
    } else if VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char) {
        let redirect: varatt_indirect = VARATT_EXTERNAL_GET_POINTER(attr);

        /* nested indirect Datums aren't allowed */
        Assert!(!VARATT_IS_EXTERNAL_INDIRECT(redirect.pointer as *const c_char));

        return detoast_attr_slice(redirect.pointer, sliceoffset, slicelength);
    } else if VARATT_IS_EXTERNAL_EXPANDED(attr as *const c_char) {
        /* pass it off to detoast_external_attr to flatten */
        preslice = detoast_external_attr(attr);
    } else {
        preslice = attr;
    }

    Assert!(!VARATT_IS_EXTERNAL(preslice as *const c_char));

    if VARATT_IS_COMPRESSED(preslice as *const c_char) {
        let tmp: *mut varlena = preslice;

        /* Decompress enough to encompass the slice and the offset */
        if slicelimit >= 0 {
            preslice = toast_decompress_datum_slice(tmp, slicelimit);
        } else {
            preslice = toast_decompress_datum(tmp);
        }

        if tmp != attr {
            pfree(tmp as *mut c_void);
        }
    }

    if VARATT_IS_SHORT(preslice as *const c_char) {
        attrdata = VARDATA_SHORT(preslice as *const c_char);
        attrsize = VARSIZE_SHORT(preslice as *const c_char) as int32 - VARHDRSZ_SHORT;
    } else {
        attrdata = VARDATA(preslice as *const c_char);
        attrsize = VARSIZE(preslice as *const c_char) as int32 - VARHDRSZ;
    }

    /* slicing of datum for compressed cases and plain value */

    if sliceoffset >= attrsize {
        sliceoffset = 0;
        slicelength = 0;
    } else if slicelength < 0 || slicelimit > attrsize {
        slicelength = attrsize - sliceoffset;
    }

    result = palloc((slicelength + VARHDRSZ) as usize) as *mut varlena;
    SET_VARSIZE(result as *mut c_char, slicelength + VARHDRSZ);

    memcpy(
        VARDATA(result as *const c_char) as *mut c_void,
        attrdata.offset(sliceoffset as isize) as *const c_void,
        slicelength as usize,
    );

    if preslice != attr {
        pfree(preslice as *mut c_void);
    }

    result
}

/* ----------
 * toast_fetch_datum -
 *
 *	Reconstruct an in memory Datum from the chunks saved
 *	in the toast relation
 * ----------
 *
 * # Safety
 * `attr` points to a valid EXTERNAL_ONDISK varlena.
 */
unsafe fn toast_fetch_datum(attr: *mut varlena) -> *mut varlena {
    let _toastrel: Relation;
    let result: *mut varlena;
    let toast_pointer: varatt_external;
    let attrsize: int32;

    if !VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        elog!(ERROR, "toast_fetch_datum shouldn't be called for non-ondisk datums");
    }

    /* Must copy to access aligned fields */
    toast_pointer = VARATT_EXTERNAL_GET_POINTER(attr);

    attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

    result = palloc((attrsize + VARHDRSZ) as usize) as *mut varlena;

    if VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) {
        SET_VARSIZE_COMPRESSED(result as *mut c_char, attrsize + VARHDRSZ);
    } else {
        SET_VARSIZE(result as *mut c_char, attrsize + VARHDRSZ);
    }

    if attrsize == 0 {
        return result; /* Probably shouldn't happen, but just in case. */
    }

    // TODO(pg-port): access/table.h + access/tableam.h (table_open / table_close /
    // table_relation_fetch_toast_slice) and the heap/relation layer are not yet
    // translated.  The header + sizing above are real; the actual chunk fetch is stubbed.
    //
    // toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);
    // table_relation_fetch_toast_slice(toastrel, toast_pointer.va_valueid,
    //                                  attrsize, 0, attrsize, result);
    // table_close(toastrel, AccessShareLock);
    let _ = (toast_pointer.va_toastrelid, toast_pointer.va_valueid);
    unimplemented!("toast_fetch_datum: heap/relation TOAST chunk fetch (tableam) not yet translated")
}

/* ----------
 * toast_fetch_datum_slice -
 *
 *	Reconstruct a segment of a Datum from the chunks saved
 *	in the toast relation
 *
 *	Note that this function supports non-compressed external datums
 *	and compressed external datums (in which case the requested slice
 *	has to be a prefix, i.e. sliceoffset has to be 0).
 * ----------
 *
 * # Safety
 * `attr` points to a valid EXTERNAL_ONDISK varlena.
 */
unsafe fn toast_fetch_datum_slice(
    attr: *mut varlena,
    mut sliceoffset: int32,
    mut slicelength: int32,
) -> *mut varlena {
    let _toastrel: Relation;
    let result: *mut varlena;
    let toast_pointer: varatt_external;
    let attrsize: int32;

    if !VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        elog!(ERROR, "toast_fetch_datum_slice shouldn't be called for non-ondisk datums");
    }

    /* Must copy to access aligned fields */
    toast_pointer = VARATT_EXTERNAL_GET_POINTER(attr);

    /*
     * It's nonsense to fetch slices of a compressed datum unless when it's a
     * prefix -- this isn't lo_* we can't return a compressed datum which is
     * meaningful to toast later.
     */
    Assert!(!VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) || 0 == sliceoffset);

    attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

    if sliceoffset >= attrsize {
        sliceoffset = 0;
        slicelength = 0;
    }

    /*
     * When fetching a prefix of a compressed external datum, account for the
     * space required by va_tcinfo, which is stored at the beginning as an
     * int32 value.
     */
    if VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) && slicelength > 0 {
        slicelength = slicelength + core::mem::size_of::<int32>() as int32;
    }

    /*
     * Adjust length request if needed.  (Note: our sole caller,
     * detoast_attr_slice, protects us against sliceoffset + slicelength
     * overflowing.)
     */
    if ((sliceoffset + slicelength) > attrsize) || slicelength < 0 {
        slicelength = attrsize - sliceoffset;
    }

    result = palloc((slicelength + VARHDRSZ) as usize) as *mut varlena;

    if VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) {
        SET_VARSIZE_COMPRESSED(result as *mut c_char, slicelength + VARHDRSZ);
    } else {
        SET_VARSIZE(result as *mut c_char, slicelength + VARHDRSZ);
    }

    if slicelength == 0 {
        return result; /* Can save a lot of work at this point! */
    }

    // TODO(pg-port): access/table.h + access/tableam.h not yet translated (see
    // toast_fetch_datum).  Header + sizing above are real; chunk fetch is stubbed.
    //
    // toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);
    // table_relation_fetch_toast_slice(toastrel, toast_pointer.va_valueid,
    //                                  attrsize, sliceoffset, slicelength, result);
    // table_close(toastrel, AccessShareLock);
    let _ = (toast_pointer.va_toastrelid, toast_pointer.va_valueid);
    unimplemented!("toast_fetch_datum_slice: heap/relation TOAST chunk fetch (tableam) not yet translated")
}

/* ----------
 * toast_decompress_datum -
 *
 * Decompress a compressed version of a varlena datum
 *
 * # Safety
 * `attr` points to a valid COMPRESSED varlena.
 */
unsafe fn toast_decompress_datum(attr: *mut varlena) -> *mut varlena {
    let cmid: ToastCompressionId;

    Assert!(VARATT_IS_COMPRESSED(attr as *const c_char));

    /*
     * Fetch the compression method id stored in the compression header and
     * decompress the data using the appropriate decompression routine.
     */
    cmid = TOAST_COMPRESS_METHOD(attr as *const c_char);
    if cmid == TOAST_PGLZ_COMPRESSION_ID {
        pglz_decompress_datum(attr)
    } else if cmid == TOAST_LZ4_COMPRESSION_ID {
        lz4_decompress_datum(attr)
    } else {
        elog!(ERROR, "invalid compression method id {}", cmid);
        #[allow(unreachable_code)]
        {
            null_mut() /* keep compiler quiet */
        }
    }
}

/* ----------
 * toast_decompress_datum_slice -
 *
 * Decompress the front of a compressed version of a varlena datum.
 * offset handling happens in detoast_attr_slice.
 * Here we just decompress a slice from the front.
 *
 * # Safety
 * `attr` points to a valid COMPRESSED varlena.
 */
unsafe fn toast_decompress_datum_slice(attr: *mut varlena, slicelength: int32) -> *mut varlena {
    let cmid: ToastCompressionId;

    Assert!(VARATT_IS_COMPRESSED(attr as *const c_char));

    /*
     * Some callers may pass a slicelength that's more than the actual
     * decompressed size.  If so, just decompress normally.  This avoids
     * possibly allocating a larger-than-necessary result object, and may be
     * faster and/or more robust as well.  Notably, some versions of liblz4
     * have been seen to give wrong results if passed an output size that is
     * more than the data's true decompressed size.
     */
    if (slicelength as uint32) >= TOAST_COMPRESS_EXTSIZE(attr as *const c_char) {
        return toast_decompress_datum(attr);
    }

    /*
     * Fetch the compression method id stored in the compression header and
     * decompress the data slice using the appropriate decompression routine.
     */
    cmid = TOAST_COMPRESS_METHOD(attr as *const c_char);
    if cmid == TOAST_PGLZ_COMPRESSION_ID {
        pglz_decompress_datum_slice(attr, slicelength)
    } else if cmid == TOAST_LZ4_COMPRESSION_ID {
        lz4_decompress_datum_slice(attr, slicelength)
    } else {
        elog!(ERROR, "invalid compression method id {}", cmid);
        #[allow(unreachable_code)]
        {
            null_mut() /* keep compiler quiet */
        }
    }
}

// ----------------------------------------------------------------
//   pglz / lz4 *_datum wrappers.
//
//   In PostgreSQL these live in access/common/toast_compression.c (not yet
//   ported); inlined here so the COMPRESSED path is fully real for pglz.
// ----------------------------------------------------------------

/*
 * toast_compression.c: pglz_decompress_datum - decompress a pglz-compressed
 * varlena Datum (full value).
 *
 * # Safety
 * `value` is a valid COMPRESSED varlena holding pglz data.
 */
unsafe fn pglz_decompress_datum(value: *const varlena) -> *mut varlena {
    let result: *mut varlena;
    let rawsize: int32;

    /* allocate memory for the uncompressed data */
    result = palloc(
        VARDATA_COMPRESSED_GET_EXTSIZE(value as *const c_char) as usize + VARHDRSZ as usize,
    ) as *mut varlena;

    /* decompress the data */
    rawsize = pglz_decompress(
        (value as *const c_char).add(VARHDRSZ_COMPRESSED),
        VARSIZE(value as *const c_char) as int32 - VARHDRSZ_COMPRESSED as int32,
        VARDATA(result as *const c_char),
        VARDATA_COMPRESSED_GET_EXTSIZE(value as *const c_char) as int32,
        true,
    );
    if rawsize < 0 {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(ERROR, errmsg!("compressed pglz data is corrupt"));
    }

    SET_VARSIZE(result as *mut c_char, rawsize + VARHDRSZ);

    result
}

/*
 * toast_compression.c: pglz_decompress_datum_slice - decompress the front
 * `slicelength` bytes of a pglz-compressed varlena Datum.
 *
 * # Safety
 * `value` is a valid COMPRESSED varlena holding pglz data.
 */
unsafe fn pglz_decompress_datum_slice(value: *const varlena, slicelength: int32) -> *mut varlena {
    let result: *mut varlena;
    let rawsize: int32;

    /* allocate memory for the uncompressed data */
    result = palloc((slicelength + VARHDRSZ) as usize) as *mut varlena;

    /* decompress the data */
    rawsize = pglz_decompress(
        (value as *const c_char).add(VARHDRSZ_COMPRESSED),
        VARSIZE(value as *const c_char) as int32 - VARHDRSZ_COMPRESSED as int32,
        VARDATA(result as *const c_char),
        slicelength,
        false,
    );
    if rawsize < 0 {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(ERROR, errmsg!("compressed pglz data is corrupt"));
    }

    SET_VARSIZE(result as *mut c_char, rawsize + VARHDRSZ);

    result
}

/*
 * toast_compression.c: lz4_decompress_datum / _slice.  STUBBED: the LZ4 path
 * requires liblz4 (LZ4_decompress_safe), not yet bound.
 *
 * # Safety
 * `value` is a valid COMPRESSED varlena.
 */
unsafe fn lz4_decompress_datum(value: *const varlena) -> *mut varlena {
    // TODO(pg-port): liblz4 (LZ4_decompress_safe) not yet bound.
    let _ = value;
    unimplemented!("lz4_decompress_datum: liblz4 not yet bound")
}
unsafe fn lz4_decompress_datum_slice(value: *const varlena, slicelength: int32) -> *mut varlena {
    // TODO(pg-port): liblz4 (LZ4_decompress_safe_partial) not yet bound.
    let _ = (value, slicelength);
    unimplemented!("lz4_decompress_datum_slice: liblz4 not yet bound")
}

/* ----------
 * toast_raw_datum_size -
 *
 *	Return the raw (detoasted) size of a varlena datum
 *	(including the VARHDRSZ header)
 * ----------
 *
 * # Safety
 * `value` is a Datum holding a pointer to a valid varlena.
 */
pub unsafe fn toast_raw_datum_size(value: Datum) -> Size {
    let attr: *mut varlena = DatumGetPointer(value) as *mut varlena;
    let result: Size;

    if VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        /* va_rawsize is the size of the original datum -- including header */
        let toast_pointer: varatt_external = VARATT_EXTERNAL_GET_POINTER(attr);
        result = toast_pointer.va_rawsize as Size;
    } else if VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char) {
        let toast_pointer: varatt_indirect = VARATT_EXTERNAL_GET_POINTER(attr);

        /* nested indirect Datums aren't allowed */
        Assert!(!VARATT_IS_EXTERNAL_INDIRECT(toast_pointer.pointer as *const c_char));

        return toast_raw_datum_size(PointerGetDatum(toast_pointer.pointer as *const c_void));
    } else if VARATT_IS_EXTERNAL_EXPANDED(attr as *const c_char) {
        // TODO(pg-port): utils/expandeddatum.h (DatumGetEOHP / EOH_get_flat_size) not
        // yet translated.
        //
        // result = EOH_get_flat_size(DatumGetEOHP(value));
        let _ = value;
        unimplemented!("toast_raw_datum_size: expanded-object EOH_get_flat_size not yet translated")
    } else if VARATT_IS_COMPRESSED(attr as *const c_char) {
        /* here, va_rawsize is just the payload size */
        result =
            VARDATA_COMPRESSED_GET_EXTSIZE(attr as *const c_char) as Size + VARHDRSZ as Size;
    } else if VARATT_IS_SHORT(attr as *const c_char) {
        /*
         * we have to normalize the header length to VARHDRSZ or else the
         * callers of this function will be confused.
         */
        result = VARSIZE_SHORT(attr as *const c_char) as Size - VARHDRSZ_SHORT as Size
            + VARHDRSZ as Size;
    } else {
        /* plain untoasted datum */
        result = VARSIZE(attr as *const c_char) as Size;
    }
    result
}

/* ----------
 * toast_datum_size
 *
 *	Return the physical storage size (possibly compressed) of a varlena datum
 * ----------
 *
 * # Safety
 * `value` is a Datum holding a pointer to a valid varlena.
 */
pub unsafe fn toast_datum_size(value: Datum) -> Size {
    let attr: *mut varlena = DatumGetPointer(value) as *mut varlena;
    let result: Size;

    if VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        /*
         * Attribute is stored externally - return the extsize whether
         * compressed or not.  We do not count the size of the toast pointer
         * ... should we?
         */
        let toast_pointer: varatt_external = VARATT_EXTERNAL_GET_POINTER(attr);
        result = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) as Size;
    } else if VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char) {
        let toast_pointer: varatt_indirect = VARATT_EXTERNAL_GET_POINTER(attr);

        /* nested indirect Datums aren't allowed */
        Assert!(!VARATT_IS_EXTERNAL_INDIRECT(attr as *const c_char));

        return toast_datum_size(PointerGetDatum(toast_pointer.pointer as *const c_void));
    } else if VARATT_IS_EXTERNAL_EXPANDED(attr as *const c_char) {
        // TODO(pg-port): utils/expandeddatum.h (DatumGetEOHP / EOH_get_flat_size) not
        // yet translated.
        //
        // result = EOH_get_flat_size(DatumGetEOHP(value));
        let _ = value;
        unimplemented!("toast_datum_size: expanded-object EOH_get_flat_size not yet translated")
    } else if VARATT_IS_SHORT(attr as *const c_char) {
        result = VARSIZE_SHORT(attr as *const c_char) as Size;
    } else {
        /*
         * Attribute is stored inline either compressed or not, just calculate
         * the size of the datum in either case.
         */
        result = VARSIZE(attr as *const c_char) as Size;
    }
    result
}

// ----------------------------------------------------------------
//   PG_DETOAST_* entry points (fmgr.h).
//
//   In C these are macros in fmgr.h:
//     #define PG_DETOAST_DATUM(datum) pg_detoast_datum((struct varlena *) ...)
//   The functions themselves are exported by fmgr.c, but their bodies are thin
//   wrappers over detoast_attr and live most naturally next to it.  Defined here
//   as the *real* implementations (see INTEGRATOR NOTE at top of file).
// ----------------------------------------------------------------

/*
 * fmgr.c: pg_detoast_datum - fully detoast `datum` if it is extended.
 *
 * # Safety
 * `datum` points to a valid varlena.
 */
#[no_mangle]
pub unsafe fn pg_detoast_datum(datum: *mut varlena) -> *mut varlena {
    if VARATT_IS_EXTENDED(datum as *const c_char) {
        detoast_attr(datum)
    } else {
        datum
    }
}

/*
 * fmgr.c: pg_detoast_datum_copy - like pg_detoast_datum, but always returns a
 * freshly palloc'd copy even when the input is already plain.
 *
 * # Safety
 * `datum` points to a valid varlena.
 */
#[no_mangle]
pub unsafe fn pg_detoast_datum_copy(datum: *mut varlena) -> *mut varlena {
    if VARATT_IS_EXTENDED(datum as *const c_char) {
        detoast_attr(datum)
    } else {
        /* Make a modifiable copy of the varlena object */
        let len: Size = VARSIZE(datum as *const c_char) as Size;
        let result: *mut varlena = palloc(len) as *mut varlena;

        memcpy(result as *mut c_void, datum as *const c_void, len);
        result
    }
}

/*
 * fmgr.c: pg_detoast_datum_packed - for the common in-memory case of a plain
 * (4B-uncompressed or short-1B) datum this is the identity; otherwise detoast.
 *
 * # Safety
 * `datum` points to a valid varlena.
 */
#[no_mangle]
pub unsafe fn pg_detoast_datum_packed(datum: *mut varlena) -> *mut varlena {
    if VARATT_IS_COMPRESSED(datum as *const c_char) || VARATT_IS_EXTERNAL(datum as *const c_char) {
        detoast_attr(datum)
    } else {
        datum
    }
}

/*
 * fmgr.c: pg_detoast_datum_slice - fetch only [first, first+count) of `datum`.
 *
 * # Safety
 * `datum` points to a valid varlena.
 */
pub unsafe fn pg_detoast_datum_slice(
    datum: *mut varlena,
    first: int32,
    count: int32,
) -> *mut varlena {
    /* Only get the specified portion from the toast rel */
    detoast_attr_slice(datum, first, count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_lzcompress::{pglz_compress, PGLZ_strategy_always, PGLZ_MAX_OUTPUT};

    /*
     * Build a COMPRESSED (4B_C) varlena from `raw` using pglz, then run it
     * through detoast_attr / toast_decompress_datum and assert we recover the
     * original bytes.  Exercises the fully-real pglz COMPRESSED path.
     */
    unsafe fn make_compressed(raw: &[u8]) -> *mut varlena {
        let rawlen = raw.len() as int32;

        /* compress into a scratch buffer */
        let cap = PGLZ_MAX_OUTPUT(rawlen) as usize;
        let scratch = palloc(cap) as *mut c_char;
        let clen = pglz_compress(
            raw.as_ptr() as *const c_char,
            rawlen,
            scratch,
            PGLZ_strategy_always,
        );
        assert!(clen > 0, "pglz_compress failed to compress test input");

        /* wrap with the compressed header: [int32 vl_len_][uint32 tcinfo][cdata] */
        let total = VARHDRSZ_COMPRESSED + clen as usize;
        let comp = palloc(total) as *mut varlena;
        SET_VARSIZE_COMPRESSED(comp as *mut c_char, total as int32);
        /* tcinfo = rawsize(payload, excludes header) | (PGLZ_ID << 30) */
        let hdr = comp as *mut toast_compress_header;
        (*hdr).tcinfo =
            (rawlen as uint32 & VARLENA_EXTSIZE_MASK) | (TOAST_PGLZ_COMPRESSION_ID << VARLENA_EXTSIZE_BITS);
        memcpy(
            (comp as *mut c_char).add(VARHDRSZ_COMPRESSED) as *mut c_void,
            scratch as *const c_void,
            clen as usize,
        );

        pfree(scratch as *mut c_void);
        comp
    }

    #[test]
    fn pglz_compressed_roundtrip_via_detoast() {
        unsafe {
            // A repetitive, very compressible payload (>= min_input_size for "always").
            let raw: Vec<u8> = b"PostgreSQL TOAST detoast roundtrip "
                .iter()
                .cycle()
                .take(400)
                .copied()
                .collect();

            let comp = make_compressed(&raw);
            assert!(VARATT_IS_COMPRESSED(comp as *const c_char));
            assert!(VARATT_IS_EXTENDED(comp as *const c_char));
            // payload size recorded in the header matches the raw length
            assert_eq!(
                VARDATA_COMPRESSED_GET_EXTSIZE(comp as *const c_char) as usize,
                raw.len()
            );
            assert_eq!(TOAST_COMPRESS_METHOD(comp as *const c_char), TOAST_PGLZ_COMPRESSION_ID);

            // toast_decompress_datum: direct decompress
            let out = toast_decompress_datum(comp);
            assert!(!VARATT_IS_EXTENDED(out as *const c_char));
            assert_eq!(VARSIZE(out as *const c_char) as usize, raw.len() + VARHDRSZ as usize);
            let got = core::slice::from_raw_parts(
                VARDATA(out as *const c_char) as *const u8,
                raw.len(),
            );
            assert_eq!(got, &raw[..]);
            pfree(out as *mut c_void);

            // detoast_attr: same input, full entry point
            let out2 = detoast_attr(comp);
            let got2 = core::slice::from_raw_parts(
                VARDATA(out2 as *const c_char) as *const u8,
                raw.len(),
            );
            assert_eq!(got2, &raw[..]);
            pfree(out2 as *mut c_void);

            pfree(comp as *mut c_void);
        }
    }

    #[test]
    fn detoast_attr_slice_prefix_of_compressed() {
        unsafe {
            let raw: Vec<u8> = (0..300u32).map(|i| (b'A' + (i % 16) as u8)).collect();
            let comp = make_compressed(&raw);

            // request a 50-byte prefix starting at offset 0
            let sl = detoast_attr_slice(comp, 0, 50);
            assert_eq!(VARSIZE(sl as *const c_char) as usize, 50 + VARHDRSZ as usize);
            let got = core::slice::from_raw_parts(VARDATA(sl as *const c_char) as *const u8, 50);
            assert_eq!(got, &raw[0..50]);
            pfree(sl as *mut c_void);

            // request a mid-slice [100, 100+40)
            let sl2 = detoast_attr_slice(comp, 100, 40);
            let got2 = core::slice::from_raw_parts(VARDATA(sl2 as *const c_char) as *const u8, 40);
            assert_eq!(got2, &raw[100..140]);
            pfree(sl2 as *mut c_void);

            pfree(comp as *mut c_void);
        }
    }

    #[test]
    fn detoast_attr_short_to_4byte() {
        unsafe {
            // Build a SHORT (1B) header datum with 3 payload bytes.
            let total = VARHDRSZ_SHORT as usize + 3;
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE_SHORT(p, total as int32);
            let d = VARDATA_SHORT(p);
            *d.add(0) = b'x' as c_char;
            *d.add(1) = b'y' as c_char;
            *d.add(2) = b'z' as c_char;
            assert!(VARATT_IS_SHORT(p));

            let out = detoast_attr(p as *mut varlena);
            assert!(VARATT_IS_4B(out as *const c_char));
            assert_eq!(VARSIZE(out as *const c_char) as usize, 3 + VARHDRSZ as usize);
            let got = core::slice::from_raw_parts(VARDATA(out as *const c_char) as *const u8, 3);
            assert_eq!(got, b"xyz");

            pfree(out as *mut c_void);
            pfree(p as *mut c_void);
        }
    }

    #[test]
    fn pg_detoast_datum_packed_identity_on_plain() {
        unsafe {
            let total = VARHDRSZ as usize + 4;
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE(p, total as int32);
            // plain 4B-U datum: packed is identity
            assert_eq!(pg_detoast_datum_packed(p as *mut varlena), p as *mut varlena);
            // pg_detoast_datum: identity on non-extended too
            assert_eq!(pg_detoast_datum(p as *mut varlena), p as *mut varlena);
            // copy makes a fresh allocation with identical contents
            let c = pg_detoast_datum_copy(p as *mut varlena);
            assert_ne!(c, p as *mut varlena);
            assert_eq!(VARSIZE(c as *const c_char), VARSIZE(p as *const c_char));
            pfree(c as *mut c_void);
            pfree(p as *mut c_void);
        }
    }
}
