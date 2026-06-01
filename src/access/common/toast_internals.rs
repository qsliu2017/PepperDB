//! Translation of postgres/src/backend/access/common/toast_internals.c
//!
//! Functions for internal use by the TOAST system.
//!
//! Copyright (c) 2000-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/access/common/toast_internals.c
//!
//! `#include`s mapped:
//!   common/pg_lzcompress.h -> crate::common::pg_lzcompress (pglz_compress + PGLZ_MAX_OUTPUT)
//!   varatt.h               -> crate::varatt (VAR* macro layer; varattrib_4b va_compressed)
//!   access/toast_compression.h -> compression-method ids/chars defined module-locally below
//!     (pglz_compress_datum is inlined here from toast_compression.c; lz4_compress_datum is a STUB)
//!
//! TRANSLATED (self-contained, real): the compression path used by tuptoaster -
//!   toast_compress_datum, plus the inlined pglz_compress_datum helper and the
//!   TOAST_COMPRESS_* / toast_compress_header accessors.
//!
//! STUBBED (need heap / relation / catalog / snapshot machinery not yet ported):
//!   toast_save_datum, toast_delete_datum, toastrel_valueid_exists,
//!   toastid_valueid_exists, toast_get_valid_index, toast_open_indexes,
//!   toast_close_indexes, get_toast_snapshot.  These keep their C bodies as
//!   comments and `unimplemented!()`.  Their signatures use opaque local type
//!   aliases (Relation/LOCKMODE/Snapshot) so this file compiles standalone
//!   without the heap/relcache/snapmgr modules being wired into the crate.
//!
//! lz4 is not ported (USE_LZ4 off in this build): lz4_compress_datum ereports like
//! the upstream NO_LZ4_SUPPORT() path.
//!
//! libc memcpy is bound via extern "C" (used in the stubbed on-disk insert body
//! comments only; not actually reached from the compiled path).

use crate::prelude::*;
use crate::varatt::*;

use crate::common::pg_lzcompress::{pglz_compress, PGLZ_MAX_OUTPUT};

use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   access/toast_compression.h (the bits this file needs)
// ----------------------------------------------------------------------------

/* ToastCompressionId - see toast_compression.h */
pub const TOAST_PGLZ_COMPRESSION_ID: u32 = 0;
pub const TOAST_LZ4_COMPRESSION_ID: u32 = 1;
pub const TOAST_INVALID_COMPRESSION_ID: u32 = 2;

/* Char codes stored in pg_attribute.attcompression / chosen at compress time. */
pub const TOAST_PGLZ_COMPRESSION: c_char = b'p' as c_char;
pub const TOAST_LZ4_COMPRESSION: c_char = b'l' as c_char;
pub const InvalidCompressionMethod: c_char = b'\0' as c_char;

#[inline]
pub fn CompressionMethodIsValid(cm: c_char) -> bool {
    cm != InvalidCompressionMethod
}

/*
 * default_toast_compression is an integer for purposes of the GUC machinery,
 * but the value is one of the char codes above (default: pglz).
 * Defined in toast_compression.c; mirrored here as the compiled default.
 */
pub static mut default_toast_compression: c_char = TOAST_PGLZ_COMPRESSION;

// ----------------------------------------------------------------------------
//   access/toast_internals.h - compressed toast header accessors
// ----------------------------------------------------------------------------

/*
 *	The information at the start of the compressed toast data.
 */
#[repr(C)]
pub struct toast_compress_header {
    /// varlena header (do not touch directly!)
    pub vl_len_: int32,
    /// 2 bits for compression method and 30 bits external size; see va_extinfo
    pub tcinfo: uint32,
}

/*
 * Utilities for manipulation of header information for compressed toast
 * entries.  These mirror the VARDATA_COMPRESSED_GET_* macros in varatt.rs but
 * operate via the toast_compress_header overlay (the C macros in
 * toast_internals.h).  VARLENA_EXTSIZE_BITS=30 / VARLENA_EXTSIZE_MASK are not
 * yet exported from varatt.rs, so define them locally (TODO(pg-port): move to
 * varatt.rs alongside the va_compressed accessors).
 */
const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/* TOAST_COMPRESS_EXTSIZE(ptr) */
#[inline]
pub unsafe fn TOAST_COMPRESS_EXTSIZE(ptr: *const c_void) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo & VARLENA_EXTSIZE_MASK
}

/* TOAST_COMPRESS_METHOD(ptr) */
#[inline]
pub unsafe fn TOAST_COMPRESS_METHOD(ptr: *const c_void) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo >> VARLENA_EXTSIZE_BITS
}

/* TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(ptr, len, cm_method) */
#[inline]
pub unsafe fn TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(
    ptr: *mut c_void,
    len: int32,
    cm_method: uint32,
) {
    Assert!(len > 0 && (len as u32) <= VARLENA_EXTSIZE_MASK);
    Assert!(cm_method == TOAST_PGLZ_COMPRESSION_ID || cm_method == TOAST_LZ4_COMPRESSION_ID);
    (*(ptr as *mut toast_compress_header)).tcinfo =
        (len as uint32) | (cm_method << VARLENA_EXTSIZE_BITS);
}

/*
 * VARHDRSZ_COMPRESSED = offsetof(varattrib_4b, va_compressed.va_data).
 * va_compressed is { uint32 va_header; uint32 va_tcinfo; char va_data[] },
 * i.e. two 4-byte words before the data == 8.  This equals
 * size_of::<toast_compress_header>().
 */
const VARHDRSZ_COMPRESSED: int32 = core::mem::size_of::<toast_compress_header>() as int32;

// ----------------------------------------------------------------------------
//   Opaque type aliases for the still-stubbed heap/relcache/snapshot path.
//   TODO(pg-port): replace with crate::nodes::execnodes::{Relation,Snapshot}
//   and the real LOCKMODE once those modules are wired into the crate and the
//   on-disk TOAST insert/delete is translated.
// ----------------------------------------------------------------------------
#[allow(non_camel_case_types)]
pub type Relation = *mut c_void;
#[allow(non_camel_case_types)]
pub type LOCKMODE = c_int;
#[allow(non_camel_case_types)]
pub type Snapshot = *mut c_void;

// ----------------------------------------------------------------------------
//   pglz_compress_datum (inlined from access/common/toast_compression.c)
// ----------------------------------------------------------------------------

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 *
 * # Safety
 * `value` points to a valid in-line (non-external, non-compressed) varlena.
 */
unsafe fn pglz_compress_datum(value: *const crate::c::varlena) -> *mut crate::c::varlena {
    let valsize: int32;
    let len: int32;
    let tmp: *mut crate::c::varlena;

    valsize = VARSIZE_ANY_EXHDR(value as *const c_char) as int32;

    /*
     * No point in wasting a palloc cycle if value size is outside the allowed
     * range for compression.
     *
     * The C code reads PGLZ_strategy_default->min_input_size/max_input_size.
     * pglz_compress (below) re-validates against the strategy and returns -1
     * if out of range, so we let it decide rather than duplicating the bounds
     * here (the strategy struct is not re-exported).
     */

    /*
     * Figure out the maximum possible size of the pglz output, add the bytes
     * that will be needed for varlena overhead, and allocate that amount.
     */
    tmp = palloc((PGLZ_MAX_OUTPUT(valsize) + VARHDRSZ_COMPRESSED) as Size)
        as *mut crate::c::varlena;

    len = pglz_compress(
        VARDATA_ANY(value as *const c_char),
        valsize,
        (tmp as *mut c_char).add(VARHDRSZ_COMPRESSED as usize),
        core::ptr::null(),
    );
    if len < 0 {
        pfree(tmp as *mut c_void);
        return core::ptr::null_mut();
    }

    SET_VARSIZE_COMPRESSED(tmp as *mut c_char, len + VARHDRSZ_COMPRESSED);

    tmp
}

/*
 * Compress a varlena using LZ4.  Not built with USE_LZ4 in this port.
 *
 * # Safety
 * `value` points to a valid varlena.
 */
unsafe fn lz4_compress_datum(_value: *const crate::c::varlena) -> *mut crate::c::varlena {
    // TODO(pg-port): lz4 not ported (USE_LZ4 off). Upstream NO_LZ4_SUPPORT().
    ereport!(
        ERROR,
        errmsg!("compression method lz4 not supported")
    );
    #[allow(unreachable_code)]
    core::ptr::null_mut()
}

// ----------------------------------------------------------------------------
//   toast_compress_datum
// ----------------------------------------------------------------------------

/* ----------
 * toast_compress_datum -
 *
 *	Create a compressed version of a varlena datum
 *
 *	If we fail (ie, compressed result is actually bigger than original)
 *	then return NULL.  We must not use compressed data if it'd expand
 *	the tuple!
 *
 *	We use VAR{SIZE,DATA}_ANY so we can handle short varlenas here without
 *	copying them.  But we can't handle external or compressed datums.
 * ----------
 *
 * # Safety
 * `value` is a Datum referencing a valid in-line varlena (not external, not
 * compressed).
 */
pub unsafe fn toast_compress_datum(value: Datum, mut cmethod: c_char) -> Datum {
    let mut tmp: *mut crate::c::varlena = core::ptr::null_mut();
    let valsize: int32;
    let mut cmid: u32 = TOAST_INVALID_COMPRESSION_ID;

    Assert!(!VARATT_IS_EXTERNAL(DatumGetPointer(value) as *const c_char));
    Assert!(!VARATT_IS_COMPRESSED(DatumGetPointer(value) as *const c_char));

    valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value) as *const c_char) as int32;

    /* If the compression method is not valid, use the current default */
    if !CompressionMethodIsValid(cmethod) {
        cmethod = default_toast_compression;
    }

    /*
     * Call appropriate compression routine for the compression method.
     */
    if cmethod == TOAST_PGLZ_COMPRESSION {
        tmp = pglz_compress_datum(value as *const crate::c::varlena);
        cmid = TOAST_PGLZ_COMPRESSION_ID;
    } else if cmethod == TOAST_LZ4_COMPRESSION {
        tmp = lz4_compress_datum(value as *const crate::c::varlena);
        cmid = TOAST_LZ4_COMPRESSION_ID;
    } else {
        elog!(ERROR, "invalid compression method {}", cmethod as u8 as char);
    }

    if tmp.is_null() {
        return PointerGetDatum(core::ptr::null());
    }

    /*
     * We recheck the actual size even if compression reports success, because
     * it might be satisfied with having saved as little as one byte in the
     * compressed data --- which could turn into a net loss once you consider
     * header and alignment padding.  Worst case, the compressed format might
     * require three padding bytes (plus header, which is included in
     * VARSIZE(tmp)), whereas the uncompressed format would take only one
     * header byte and no padding if the value is short enough.  So we insist
     * on a savings of more than 2 bytes to ensure we have a gain.
     */
    if (VARSIZE(tmp as *const c_char) as int32) < valsize - 2 {
        /* successful compression */
        Assert!(cmid != TOAST_INVALID_COMPRESSION_ID);
        TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(tmp as *mut c_void, valsize, cmid);
        PointerGetDatum(tmp as *const c_void)
    } else {
        /* incompressible data */
        pfree(tmp as *mut c_void);
        PointerGetDatum(core::ptr::null())
    }
}

// ----------------------------------------------------------------------------
//   Stubbed heap / relcache / snapshot path
// ----------------------------------------------------------------------------

/* ----------
 * toast_save_datum -
 *
 *	Save one single datum into the secondary relation and return
 *	a Datum reference for it.
 *
 * rel: the main relation we're working with (not the toast rel!)
 * value: datum to be pushed to toast storage
 * oldexternal: if not NULL, toast pointer previously representing the datum
 * options: options to be passed to heap_insert() for toast rows
 * ----------
 *
 * # Safety
 * Stub: depends on heap/relcache/catalog machinery not yet ported.
 */
pub unsafe fn toast_save_datum(
    _rel: Relation,
    _value: Datum,
    _oldexternal: *mut crate::c::varlena,
    _options: c_int,
) -> Datum {
    // TODO(pg-port): needs access/table (table_open/close), access/heapam
    // (heap_form_tuple/heap_insert/heap_freetuple), access/genam (index_insert),
    // catalog GetNewOidWithIndex, utils/rel RelationGetRelid, the toast_open/
    // close_indexes below, and varatt_external (VARATT_EXTERNAL_*, SET_VARTAG_
    // EXTERNAL, VARDATA_EXTERNAL, TOAST_POINTER_SIZE) which are not in varatt.rs.
    //
    // C body (preserved):
    //   toastrel = table_open(rel->rd_rel->reltoastrelid, RowExclusiveLock);
    //   validIndex = toast_open_indexes(...);
    //   ... compute va_rawsize / va_extinfo from VARATT_IS_SHORT/COMPRESSED ...
    //   ... choose va_valueid via GetNewOidWithIndex (rewrite re-use path) ...
    //   while (data_todo > 0) {
    //       chunk_size = Min(TOAST_MAX_CHUNK_SIZE, data_todo);
    //       SET_VARSIZE(&chunk_data, chunk_size + VARHDRSZ);
    //       memcpy(VARDATA(&chunk_data), data_p, chunk_size);
    //       toasttup = heap_form_tuple(...); heap_insert(...); index_insert(...);
    //       heap_freetuple(toasttup);
    //   }
    //   result = palloc(TOAST_POINTER_SIZE);
    //   SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
    //   memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer));
    //   return PointerGetDatum(result);
    let _ = memcpy; // keep the libc binding referenced from the (stubbed) body
    unimplemented!("toast_save_datum: heap/relcache/catalog path not yet translated")
}

/* ----------
 * toast_delete_datum -
 *
 *	Delete a single external stored value.
 * ----------
 *
 * # Safety
 * Stub: depends on heap/relcache/snapshot machinery not yet ported.
 */
pub unsafe fn toast_delete_datum(_rel: Relation, _value: Datum, _is_speculative: bool) {
    // TODO(pg-port): needs VARATT_IS_EXTERNAL_ONDISK / VARATT_EXTERNAL_GET_POINTER
    // (varatt_external), table_open/close, toast_open/close_indexes, access/genam
    // systable_beginscan_ordered/getnext_ordered/endscan_ordered, ScanKeyInit,
    // heap_abort_speculative / simple_heap_delete, get_toast_snapshot.
    //
    // C body (preserved):
    //   if (!VARATT_IS_EXTERNAL_ONDISK(attr)) return;
    //   VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
    //   toastrel = table_open(toast_pointer.va_toastrelid, RowExclusiveLock);
    //   ... scan chunks matching va_valueid, delete each ...
    unimplemented!("toast_delete_datum: heap/relcache/snapshot path not yet translated")
}

/* ----------
 * toastrel_valueid_exists -
 *
 *	Test whether a toast value with the given ID exists in the toast relation.
 *	For safety, we consider a value to exist if there are either live or dead
 *	toast rows with that ID; see notes for GetNewOidWithIndex().
 * ----------
 *
 * # Safety
 * Stub: depends on systable scan machinery not yet ported.
 */
#[allow(dead_code)]
unsafe fn toastrel_valueid_exists(_toastrel: Relation, _valueid: Oid) -> bool {
    // TODO(pg-port): toast_open/close_indexes + access/genam systable_beginscan/
    // getnext/endscan with SnapshotAny.
    unimplemented!("toastrel_valueid_exists: systable scan not yet translated")
}

/* ----------
 * toastid_valueid_exists -
 *
 *	As above, but work from toast rel's OID not an open relation
 * ----------
 *
 * # Safety
 * Stub: depends on table_open/close not yet ported.
 */
#[allow(dead_code)]
unsafe fn toastid_valueid_exists(_toastrelid: Oid, _valueid: Oid) -> bool {
    // TODO(pg-port): table_open(toastrelid, AccessShareLock) +
    // toastrel_valueid_exists + table_close.
    unimplemented!("toastid_valueid_exists: table_open path not yet translated")
}

/* ----------
 * toast_get_valid_index
 *
 *	Get OID of valid index associated to given toast relation. A toast
 *	relation can have only one valid index at the same time.
 *
 * # Safety
 * Stub: depends on relcache/table_open machinery not yet ported.
 */
pub unsafe fn toast_get_valid_index(_toastoid: Oid, _lock: LOCKMODE) -> Oid {
    // TODO(pg-port): table_open + toast_open_indexes + RelationGetRelid +
    // toast_close_indexes + table_close.
    unimplemented!("toast_get_valid_index: relcache path not yet translated")
}

/* ----------
 * toast_open_indexes
 *
 *	Get an array of the indexes associated to the given toast relation
 *	and return as well the position of the valid index used by the toast
 *	relation in this array. It is the responsibility of the caller of this
 *	function to close the indexes as well as free them.
 *
 * # Safety
 * Stub: depends on relcache index-list machinery not yet ported.
 */
pub unsafe fn toast_open_indexes(
    _toastrel: Relation,
    _lock: LOCKMODE,
    _toastidxs: *mut *mut Relation,
    _num_indexes: *mut c_int,
) -> c_int {
    // TODO(pg-port): RelationGetIndexList, list_length/foreach, index_open,
    // rd_index->indisvalid scan, list_free.
    unimplemented!("toast_open_indexes: relcache index-list path not yet translated")
}

/* ----------
 * toast_close_indexes
 *
 *	Close an array of indexes for a toast relation and free it. This should
 *	be called for a set of indexes opened previously with toast_open_indexes.
 *
 * # Safety
 * Stub: depends on index_close not yet ported.
 */
pub unsafe fn toast_close_indexes(
    _toastidxs: *mut Relation,
    _num_indexes: c_int,
    _lock: LOCKMODE,
) {
    // TODO(pg-port): for i in 0..num_indexes { index_close(toastidxs[i], lock); }
    //                pfree(toastidxs);
    unimplemented!("toast_close_indexes: index_close path not yet translated")
}

/* ----------
 * get_toast_snapshot
 *
 *	Return the TOAST snapshot. Detoasting *must* happen in the same
 *	transaction that originally fetched the toast pointer.
 *
 * # Safety
 * Stub: depends on snapmgr (HaveRegisteredOrActiveSnapshot / SnapshotToastData).
 */
pub unsafe fn get_toast_snapshot() -> Snapshot {
    // TODO(pg-port): utils/snapmgr HaveRegisteredOrActiveSnapshot() guard and
    // &SnapshotToastData.
    //   if (!HaveRegisteredOrActiveSnapshot())
    //       elog(ERROR, "cannot fetch toast data without an active snapshot");
    //   return &SnapshotToastData;
    unimplemented!("get_toast_snapshot: snapmgr not yet translated")
}

// ----------------------------------------------------------------------------
//   Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_lzcompress::pglz_decompress;

    /// Round-trip a highly compressible varlena through toast_compress_datum and
    /// then pglz_decompress, verifying the compressed header (size + method) and
    /// that the payload reconstructs.
    #[test]
    fn pglz_compress_datum_roundtrip() {
        unsafe {
            // Build a 4-byte-header datum with very compressible content.
            let raw: Vec<u8> = std::iter::repeat(b'A').take(2000).collect();
            let total = VARHDRSZ as usize + raw.len();
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE(p, total as int32);
            let data = VARDATA(p);
            for (i, b) in raw.iter().enumerate() {
                *data.add(i) = *b as c_char;
            }

            let valsize = VARSIZE_ANY_EXHDR(p) as int32;

            let d = toast_compress_datum(PointerGetDatum(p as *const c_void), TOAST_PGLZ_COMPRESSION);
            let cptr = DatumGetPointer(d);
            assert!(!cptr.is_null(), "highly compressible data must compress");

            // It must be flagged as a compressed-in-line varlena.
            assert!(VARATT_IS_COMPRESSED(cptr as *const c_char));

            // Header: extsize == original payload size; method == PGLZ.
            assert_eq!(
                TOAST_COMPRESS_EXTSIZE(cptr as *const c_void) as int32,
                valsize
            );
            assert_eq!(
                TOAST_COMPRESS_METHOD(cptr as *const c_void),
                TOAST_PGLZ_COMPRESSION_ID
            );

            // Decompress the payload (starts after VARHDRSZ_COMPRESSED) and compare.
            let mut out = vec![0i8; raw.len()];
            let rawsize = pglz_decompress(
                (cptr as *const c_char).add(VARHDRSZ_COMPRESSED as usize),
                VARSIZE(cptr as *const c_char) as int32 - VARHDRSZ_COMPRESSED,
                out.as_mut_ptr() as *mut c_char,
                valsize,
                true,
            );
            assert_eq!(rawsize, raw.len() as int32);
            let got: Vec<u8> = out.iter().map(|&b| b as u8).collect();
            assert_eq!(got, raw);

            pfree(cptr as *mut c_void);
            pfree(p as *mut c_void);
        }
    }

    /// Incompressible (random-ish, short) data should return a NULL datum.
    #[test]
    fn incompressible_returns_null() {
        unsafe {
            // A short, non-repetitive payload won't beat the 2-byte gain threshold.
            let raw: [u8; 16] = [
                0x3f, 0xa1, 0x09, 0xce, 0x7b, 0x12, 0x44, 0x90, 0xde, 0x05, 0xbb, 0x6e, 0x21,
                0x88, 0xf3, 0x4c,
            ];
            let total = VARHDRSZ as usize + raw.len();
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE(p, total as int32);
            let data = VARDATA(p);
            for (i, b) in raw.iter().enumerate() {
                *data.add(i) = *b as c_char;
            }

            let d = toast_compress_datum(PointerGetDatum(p as *const c_void), TOAST_PGLZ_COMPRESSION);
            assert!(DatumGetPointer(d).is_null());

            pfree(p as *mut c_void);
        }
    }
}
