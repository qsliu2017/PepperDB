//! Translation of postgres/src/backend/access/common/toast_compression.c
//!
//! Functions for toast compression (the dispatch layer that wraps pglz / lz4).
//!
//! Copyright (c) 2021-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/access/common/toast_compression.c
//!
//! `#include`s mapped:
//!   access/detoast.h           -> VARATT_EXTERNAL_GET_POINTER (defined locally here)
//!   access/toast_compression.h -> ToastCompressionId ids + TOAST_*_COMPRESSION char codes
//!       (merged below; see RECONCILE note)
//!   common/pg_lzcompress.h     -> crate::common::pg_lzcompress::{pglz_compress, pglz_decompress,
//!       PGLZ_MAX_OUTPUT, PGLZ_strategy_default}
//!   varatt.h                   -> crate::varatt (VAR* macro layer) + locally-defined
//!       compressed/external accessors (VARHDRSZ_COMPRESSED, varatt_external,
//!       VARDATA_COMPRESSED_GET_*, VARATT_EXTERNAL_*, SET_VARSIZE_COMPRESSED).
//!   <lz4.h>                    -> NOT bound (USE_LZ4 off); lz4_* are NO_LZ4_SUPPORT stubs.
//!
//! TRANSLATED (real): pglz_compress_datum, pglz_decompress_datum,
//!   pglz_decompress_datum_slice, toast_get_compression_id, CompressionNameToMethod,
//!   GetCompressionMethodName.  These exercise the ported pglz codec end to end.
//!
//! STUBBED (USE_LZ4 off, liblz4 not bound): lz4_compress_datum,
//!   lz4_decompress_datum, lz4_decompress_datum_slice -> the upstream NO_LZ4_SUPPORT()
//!   path (ereport ERROR "compression method lz4 not supported").  CompressionNameToMethod
//!   also takes that path for "lz4" (matching #ifndef USE_LZ4).
//!
//! RECONCILE: src/access/common/toast_internals.rs ALREADY defines the same
//!   compression consts (TOAST_PGLZ_COMPRESSION_ID/_LZ4_/_INVALID_, the 'p'/'l'/'\0'
//!   char codes, CompressionMethodIsValid, default_toast_compression) and
//!   src/access/common/detoast.rs defines the varatt_external / VARATT_EXTERNAL_* /
//!   VARDATA_COMPRESSED_* accessors -- all module-PRIVATE there.  This file is kept
//!   as a SEPARATE module (toast_compression2) per instructions to avoid a name
//!   clash with toast_internals.rs's already-inlined pglz_compress_datum.  When the
//!   tree is reconciled, these consts + the varatt external/compressed accessors
//!   should move to ONE home (crate::access::common::toast_compression /
//!   crate::varatt) and the duplicates in toast_internals.rs / detoast.rs / here
//!   removed.

use crate::prelude::*;
use crate::varatt::*;

use crate::c::varlena;
use crate::common::pg_lzcompress::{
    pglz_compress, pglz_decompress, PGLZ_MAX_OUTPUT, PGLZ_strategy_default,
};
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_void};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ----------------------------------------------------------------------------
//   access/toast_compression.h (merged)
// ----------------------------------------------------------------------------

/*
 * Built-in compression method ID.  Stored in the first 2 bits of the raw
 * length in the toast compression header.  Only 4 values are ever possible.
 *
 * RECONCILE: duplicated from toast_internals.rs / detoast.rs (see file header).
 */
#[allow(non_camel_case_types)]
pub type ToastCompressionId = u32;
pub const TOAST_PGLZ_COMPRESSION_ID: ToastCompressionId = 0;
pub const TOAST_LZ4_COMPRESSION_ID: ToastCompressionId = 1;
pub const TOAST_INVALID_COMPRESSION_ID: ToastCompressionId = 2;

/*
 * Built-in compression methods, as stored in pg_attribute.attcompression.
 * InvalidCompressionMethod denotes the default behavior.
 */
pub const TOAST_PGLZ_COMPRESSION: c_char = b'p' as c_char;
pub const TOAST_LZ4_COMPRESSION: c_char = b'l' as c_char;
pub const InvalidCompressionMethod: c_char = b'\0' as c_char;

#[inline]
pub fn CompressionMethodIsValid(cm: c_char) -> bool {
    cm != InvalidCompressionMethod
}

/* errcodes.h classifications (errcode() shim ignores the value). */
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_DATA_CORRUPTED: c_int = 0;

/*
 * GUC.  default_toast_compression is an integer for the GUC machinery, but the
 * value is one of the char codes above (default: pglz).
 *
 * RECONCILE: also defined in toast_internals.rs.
 */
pub static mut default_toast_compression: c_char = TOAST_PGLZ_COMPRESSION;

// ----------------------------------------------------------------------------
//   varatt.h accessors not exported by crate::varatt -- defined locally.
//
//   RECONCILE: these mirror the private copies in detoast.rs; they should move
//   into crate::varatt once it grows the external/compressed-TOAST layer.
// ----------------------------------------------------------------------------

/* "saved size" portion of va_extinfo / tcinfo (30 bits) + 2-bit method. */
const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: uint32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/*
 * varatt.h: struct varatt_external -- a traditional out-of-line "TOAST pointer".
 * Stored UNALIGNED inside tuples, so always memcpy into a local before reading.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct varatt_external {
    pub va_rawsize: int32,  /* Original data size (includes header) */
    pub va_extinfo: uint32, /* External saved size (w/o header) + compression method */
    pub va_valueid: Oid,    /* Unique ID of value within TOAST table */
    pub va_toastrelid: Oid, /* RelID of TOAST table containing it */
}

/*
 * toast_internals.h: header at the start of compressed toast data.
 *  int32  vl_len_;  varlena header (do not touch directly!)
 *  uint32 tcinfo;   2 bits compression method + 30 bits external (raw) size.
 */
#[repr(C)]
struct toast_compress_header {
    vl_len_: int32,
    tcinfo: uint32,
}

/* varatt.h: VARHDRSZ_COMPRESSED == offsetof(varattrib_4b, va_compressed.va_data). */
const VARHDRSZ_COMPRESSED: usize = core::mem::size_of::<toast_compress_header>();

/*
 * varatt.h: VARDATA_COMPRESSED_GET_EXTSIZE / VARDATA_COMPRESSED_GET_COMPRESS_METHOD
 * on a compressed-in-line Datum (the tcinfo word right after the varlena header).
 */
#[inline]
unsafe fn VARDATA_COMPRESSED_GET_EXTSIZE(ptr: *const c_char) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo & VARLENA_EXTSIZE_MASK
}
#[inline]
unsafe fn VARDATA_COMPRESSED_GET_COMPRESS_METHOD(ptr: *const c_char) -> ToastCompressionId {
    (*(ptr as *const toast_compress_header)).tcinfo >> VARLENA_EXTSIZE_BITS
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
 * Copies the possibly-unaligned TOAST-pointer payload of an EXTERNAL datum.
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

/* VARTAG_EXTERNAL / VARATT_IS_EXTERNAL_ONDISK (not exported by crate::varatt). */
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_1B_E(ptr) == VARTAG_ONDISK
}

/*
 * NO_LZ4_SUPPORT() -- mirror the C macro: USE_LZ4 is off in this build, so this
 * always raises the "not supported" error (ereport ERROR panics, typed ()).
 */
macro_rules! NO_LZ4_SUPPORT {
    () => {{
        // C: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        //     errmsg("compression method lz4 not supported"),
        //     errdetail("This functionality requires the server to be built with lz4 support.")))
        // The ereport! shim takes (level, msg); errcode() is ignored and errdetail
        // is folded into the message text.
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("compression method lz4 not supported")
        )
    }};
}

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
pub unsafe fn pglz_compress_datum(value: *const varlena) -> *mut varlena {
    let valsize: int32;
    let len: int32;
    let tmp: *mut varlena;

    valsize = VARSIZE_ANY_EXHDR(value as *const c_char) as int32;

    /*
     * No point in wasting a palloc cycle if value size is outside the allowed
     * range for compression.
     */
    if valsize < PGLZ_strategy_default.min_input_size
        || valsize > PGLZ_strategy_default.max_input_size
    {
        return null_mut();
    }

    /*
     * Figure out the maximum possible size of the pglz output, add the bytes
     * that will be needed for varlena overhead, and allocate that amount.
     */
    tmp = palloc(PGLZ_MAX_OUTPUT(valsize) as usize + VARHDRSZ_COMPRESSED) as *mut varlena;

    len = pglz_compress(
        VARDATA_ANY(value as *const c_char),
        valsize,
        (tmp as *mut c_char).add(VARHDRSZ_COMPRESSED),
        null(),
    );
    if len < 0 {
        pfree(tmp as *mut c_void);
        return null_mut();
    }

    SET_VARSIZE_COMPRESSED(tmp as *mut c_char, len + VARHDRSZ_COMPRESSED as int32);

    tmp
}

/*
 * Decompress a varlena that was compressed using PGLZ.
 */
pub unsafe fn pglz_decompress_datum(value: *const varlena) -> *mut varlena {
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
 * Decompress part of a varlena that was compressed using PGLZ.
 */
pub unsafe fn pglz_decompress_datum_slice(
    value: *const varlena,
    slicelength: int32,
) -> *mut varlena {
    let result: *mut varlena;
    let rawsize: int32;

    /* allocate memory for the uncompressed data */
    result = palloc(slicelength as usize + VARHDRSZ as usize) as *mut varlena;

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
 * Compress a varlena using LZ4.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 *
 * STUB: USE_LZ4 is off (liblz4 not bound) -- take the NO_LZ4_SUPPORT() path.
 */
pub unsafe fn lz4_compress_datum(_value: *const varlena) -> *mut varlena {
    // #ifndef USE_LZ4
    NO_LZ4_SUPPORT!();
    #[allow(unreachable_code)]
    null_mut() /* keep compiler quiet */
    // TODO(pg-port): #else branch (LZ4_compressBound / LZ4_compress_default) needs
    // liblz4 bound via extern "C". Original C body:
    //   max_size = LZ4_compressBound(valsize);
    //   tmp = palloc(max_size + VARHDRSZ_COMPRESSED);
    //   len = LZ4_compress_default(VARDATA_ANY(value),
    //                              (char *) tmp + VARHDRSZ_COMPRESSED, valsize, max_size);
    //   if (len <= 0) elog(ERROR, "lz4 compression failed");
    //   if (len > valsize) { pfree(tmp); return NULL; }
    //   SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);
    //   return tmp;
}

/*
 * Decompress a varlena that was compressed using LZ4.
 *
 * STUB: USE_LZ4 is off -- NO_LZ4_SUPPORT().
 */
pub unsafe fn lz4_decompress_datum(_value: *const varlena) -> *mut varlena {
    // #ifndef USE_LZ4
    NO_LZ4_SUPPORT!();
    #[allow(unreachable_code)]
    null_mut() /* keep compiler quiet */
    // TODO(pg-port): #else branch needs LZ4_decompress_safe. Original C body:
    //   result = palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);
    //   rawsize = LZ4_decompress_safe((char *) value + VARHDRSZ_COMPRESSED,
    //                                 VARDATA(result),
    //                                 VARSIZE(value) - VARHDRSZ_COMPRESSED,
    //                                 VARDATA_COMPRESSED_GET_EXTSIZE(value));
    //   if (rawsize < 0) ereport(ERROR, ... "compressed lz4 data is corrupt");
    //   SET_VARSIZE(result, rawsize + VARHDRSZ);
    //   return result;
}

/*
 * Decompress part of a varlena that was compressed using LZ4.
 *
 * STUB: USE_LZ4 is off -- NO_LZ4_SUPPORT().
 */
pub unsafe fn lz4_decompress_datum_slice(
    _value: *const varlena,
    _slicelength: int32,
) -> *mut varlena {
    // #ifndef USE_LZ4
    NO_LZ4_SUPPORT!();
    #[allow(unreachable_code)]
    null_mut() /* keep compiler quiet */
    // TODO(pg-port): #else branch needs LZ4_versionNumber / LZ4_decompress_safe_partial.
    // Original C body:
    //   if (LZ4_versionNumber() < 10803) return lz4_decompress_datum(value);
    //   result = palloc(slicelength + VARHDRSZ);
    //   rawsize = LZ4_decompress_safe_partial((char *) value + VARHDRSZ_COMPRESSED,
    //                                         VARDATA(result),
    //                                         VARSIZE(value) - VARHDRSZ_COMPRESSED,
    //                                         slicelength, slicelength);
    //   if (rawsize < 0) ereport(ERROR, ... "compressed lz4 data is corrupt");
    //   SET_VARSIZE(result, rawsize + VARHDRSZ);
    //   return result;
}

/*
 * Extract compression ID from a varlena.
 *
 * Returns TOAST_INVALID_COMPRESSION_ID if the varlena is not compressed.
 */
pub unsafe fn toast_get_compression_id(attr: *mut varlena) -> ToastCompressionId {
    let mut cmid: ToastCompressionId = TOAST_INVALID_COMPRESSION_ID;

    /*
     * If it is stored externally then fetch the compression method id from
     * the external toast pointer.  If compressed inline, fetch it from the
     * toast compression header.
     */
    if VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        let toast_pointer: varatt_external = VARATT_EXTERNAL_GET_POINTER(attr as *const varlena);

        if VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) {
            cmid = VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer);
        }
    } else if VARATT_IS_COMPRESSED(attr as *const c_char) {
        cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(attr as *const c_char);
    }

    cmid
}

/*
 * CompressionNameToMethod - Get compression method from compression name
 *
 * Search in the available built-in methods.  If the compression is not found
 * in the built-in methods then return InvalidCompressionMethod.
 */
pub unsafe fn CompressionNameToMethod(compression: *const c_char) -> c_char {
    if strcmp(compression, c"pglz".as_ptr()) == 0 {
        return TOAST_PGLZ_COMPRESSION;
    } else if strcmp(compression, c"lz4".as_ptr()) == 0 {
        // #ifndef USE_LZ4
        NO_LZ4_SUPPORT!();
        #[allow(unreachable_code)]
        return TOAST_LZ4_COMPRESSION;
    }

    InvalidCompressionMethod
}

/*
 * GetCompressionMethodName - Get compression method name
 */
pub fn GetCompressionMethodName(method: c_char) -> *const c_char {
    match method {
        TOAST_PGLZ_COMPRESSION => c"pglz".as_ptr(),
        TOAST_LZ4_COMPRESSION => c"lz4".as_ptr(),
        _ => {
            elog!(ERROR, "invalid compression method {}", method as u8 as char);
            #[allow(unreachable_code)]
            null() /* keep compiler quiet */
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a plain 4B-uncompressed bytea/varlena holding `payload`.
    unsafe fn make_varlena(payload: &[u8]) -> *mut varlena {
        let total = VARHDRSZ as usize + payload.len();
        let p = palloc(total) as *mut c_char;
        SET_VARSIZE(p, total as int32);
        let data = VARDATA(p);
        for (i, b) in payload.iter().enumerate() {
            *data.add(i) = *b as c_char;
        }
        p as *mut varlena
    }

    #[test]
    fn pglz_compress_then_decompress_roundtrips() {
        unsafe {
            // Highly compressible input, well above min_input_size (32).
            let mut payload = Vec::new();
            for _ in 0..40 {
                payload.extend_from_slice(b"abcdefgh");
            }
            let orig = make_varlena(&payload);

            let comp = pglz_compress_datum(orig as *const varlena);
            assert!(!comp.is_null(), "compressible input should compress");
            // pglz_compress_datum only sets the varlena size (faithful to C); the
            // tcinfo word (raw size + method) is set by the caller toast_compress_datum.
            // Simulate that here so the round-trip can run.
            (*(comp as *mut toast_compress_header)).tcinfo =
                ((TOAST_PGLZ_COMPRESSION_ID as u32) << VARLENA_EXTSIZE_BITS) | payload.len() as u32;
            // It must actually be a compressed-in-line datum, and recorded the
            // raw extsize == payload length.
            assert!(VARATT_IS_COMPRESSED(comp as *const c_char));
            assert_eq!(
                VARDATA_COMPRESSED_GET_EXTSIZE(comp as *const c_char) as usize,
                payload.len()
            );
            // And the recorded method id is pglz.
            assert_eq!(
                VARDATA_COMPRESSED_GET_COMPRESS_METHOD(comp as *const c_char),
                TOAST_PGLZ_COMPRESSION_ID
            );

            let back = pglz_decompress_datum(comp as *const varlena);
            assert_eq!(
                VARSIZE_ANY_EXHDR(back as *const c_char) as usize,
                payload.len()
            );
            let got = core::slice::from_raw_parts(
                VARDATA(back as *const c_char) as *const u8,
                payload.len(),
            );
            assert_eq!(got, &payload[..]);

            pfree(orig as *mut c_void);
            pfree(comp as *mut c_void);
            pfree(back as *mut c_void);
        }
    }

    #[test]
    fn pglz_decompress_slice_prefix() {
        unsafe {
            let mut payload = Vec::new();
            for _ in 0..50 {
                payload.extend_from_slice(b"0123456789");
            }
            let orig = make_varlena(&payload);
            let comp = pglz_compress_datum(orig as *const varlena);
            assert!(!comp.is_null());

            let slicelen = 25;
            let back = pglz_decompress_datum_slice(comp as *const varlena, slicelen);
            assert_eq!(
                VARSIZE_ANY_EXHDR(back as *const c_char) as usize,
                slicelen as usize
            );
            let got = core::slice::from_raw_parts(
                VARDATA(back as *const c_char) as *const u8,
                slicelen as usize,
            );
            assert_eq!(got, &payload[..slicelen as usize]);

            pfree(orig as *mut c_void);
            pfree(comp as *mut c_void);
            pfree(back as *mut c_void);
        }
    }

    #[test]
    fn incompressible_input_returns_null() {
        unsafe {
            // Too small (< min_input_size 32) -> NULL without palloc.
            let small = make_varlena(b"hello");
            assert!(pglz_compress_datum(small as *const varlena).is_null());
            pfree(small as *mut c_void);
        }
    }

    #[test]
    fn name_to_method_and_back() {
        unsafe {
            assert_eq!(
                CompressionNameToMethod(c"pglz".as_ptr()),
                TOAST_PGLZ_COMPRESSION
            );
            // Unknown name -> InvalidCompressionMethod.
            assert_eq!(
                CompressionNameToMethod(c"zstd".as_ptr()),
                InvalidCompressionMethod
            );

            assert_eq!(strcmp(GetCompressionMethodName(TOAST_PGLZ_COMPRESSION), c"pglz".as_ptr()), 0);
            assert_eq!(strcmp(GetCompressionMethodName(TOAST_LZ4_COMPRESSION), c"lz4".as_ptr()), 0);
        }
    }

    #[test]
    #[should_panic]
    fn lz4_name_not_supported() {
        unsafe {
            // "lz4" hits NO_LZ4_SUPPORT() -> ereport ERROR (panics).
            let _ = CompressionNameToMethod(c"lz4".as_ptr());
        }
    }

    #[test]
    fn get_compression_id_for_inline_pglz() {
        unsafe {
            let mut payload = Vec::new();
            for _ in 0..40 {
                payload.extend_from_slice(b"xxxxyyyy");
            }
            let orig = make_varlena(&payload);
            let comp = pglz_compress_datum(orig as *const varlena);
            assert!(!comp.is_null());
            assert_eq!(
                toast_get_compression_id(comp as *mut varlena),
                TOAST_PGLZ_COMPRESSION_ID
            );
            // A plain (uncompressed) datum reports INVALID.
            assert_eq!(
                toast_get_compression_id(orig as *mut varlena),
                TOAST_INVALID_COMPRESSION_ID
            );
            pfree(orig as *mut c_void);
            pfree(comp as *mut c_void);
        }
    }
}
