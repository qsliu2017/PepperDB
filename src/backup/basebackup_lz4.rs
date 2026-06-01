//! basebackup_lz4.c - Basebackup sink implementing lz4 compression.
//!
//! Source: postgres/src/backend/backup/basebackup_lz4.c
//!
//! #include mapping:
//!   "postgres.h"                -> use crate::prelude::*
//!   <lz4frame.h>                -> the LZ4F frame API (STUBBED locally; external
//!                                  lz4 library is not part of the Rust port yet)
//!   "backup/basebackup_sink.h"  -> crate::backup::basebackup_sink (PORTED)
//!
//! The C file gates almost everything behind `#ifdef USE_LZ4`. This port assumes
//! a USE_LZ4 build (the binary is configured with lz4 support), so the real
//! compression-sink logic is translated 1:1. The LZ4F_* frame functions
//! themselves are stubbed pending an lz4 binding.

use crate::prelude::*;

use crate::backup::basebackup_sink::{
    bbsink, bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_end_archive, bbsink_forward_end_backup,
    bbsink_forward_end_manifest, bbsink_manifest_contents, bbsink_ops,
};
use crate::common::compression::pg_compress_specification;
use crate::pg_config::BLCKSZ;

// ---------------------------------------------------------------------------
// LZ4F frame API stubs (<lz4frame.h>).
//
// The external lz4 library is not yet part of the port. These mirror the C
// signatures used by this file so the algorithm translates 1:1; the bodies
// are placeholders. Compression contexts are opaque pointers in C.
// TODO: bind to the real liblz4 frame API (lz4frame.h).
// ---------------------------------------------------------------------------

/// LZ4F_compressionContext_t: opaque pointer to a frame compression context.
type LZ4F_compressionContext_t = *mut c_void;

/// LZ4F_errorCode_t: a size_t that is an error sentinel when LZ4F_isError().
type LZ4F_errorCode_t = Size;

/// LZ4F frame version constant (lz4frame.h: LZ4F_VERSION == 100).
const LZ4F_VERSION: c_uint = 100;

/// LZ4F_blockSizeID_t value LZ4F_max256KB (== 6 in lz4frame.h).
const LZ4F_max256KB: c_int = 6;

/// LZ4F_frameInfo_t (subset used here): only blockSizeID is set.
#[repr(C)]
#[derive(Clone, Copy)]
struct LZ4F_frameInfo_t {
    blockSizeID: c_int,
    blockMode: c_int,
    contentChecksumFlag: c_int,
    frameType: c_int,
    contentSize: u64,
    dictID: c_uint,
    blockChecksumFlag: c_int,
}

/// LZ4F_preferences_t (subset used here): frameInfo + compressionLevel.
#[repr(C)]
#[derive(Clone, Copy)]
struct LZ4F_preferences_t {
    frameInfo: LZ4F_frameInfo_t,
    compressionLevel: c_int,
    autoFlush: c_uint,
    favorDecSpeed: c_uint,
    reserved: [c_uint; 3],
}

// TODO: liblz4 LZ4F_isError.
unsafe fn LZ4F_isError(code: Size) -> c_uint {
    // In liblz4 an error code is a size_t whose value, when negated as a signed
    // value, is small. The canonical check is (ssize_t)code < 0 essentially.
    (code > (Size::MAX - 1024)) as c_uint
}

// TODO: liblz4 LZ4F_getErrorName.
unsafe fn LZ4F_getErrorName(_code: LZ4F_errorCode_t) -> *const c_char {
    c"unknown lz4 error".as_ptr()
}

// TODO: liblz4 LZ4F_compressBound.
unsafe fn LZ4F_compressBound(srcSize: Size, _prefs: *const LZ4F_preferences_t) -> Size {
    // A conservative upper bound mirroring liblz4's worst case.
    srcSize + (srcSize / 255) + 16
}

// TODO: liblz4 LZ4F_createCompressionContext.
unsafe fn LZ4F_createCompressionContext(
    cctxPtr: *mut LZ4F_compressionContext_t,
    _version: c_uint,
) -> LZ4F_errorCode_t {
    *cctxPtr = core::ptr::NonNull::<c_void>::dangling().as_ptr();
    0
}

// TODO: liblz4 LZ4F_freeCompressionContext.
unsafe fn LZ4F_freeCompressionContext(_cctx: LZ4F_compressionContext_t) -> LZ4F_errorCode_t {
    0
}

// TODO: liblz4 LZ4F_compressBegin.
unsafe fn LZ4F_compressBegin(
    _cctx: LZ4F_compressionContext_t,
    _dstBuffer: *mut c_void,
    _dstCapacity: Size,
    _prefs: *const LZ4F_preferences_t,
) -> Size {
    0
}

// TODO: liblz4 LZ4F_compressUpdate.
unsafe fn LZ4F_compressUpdate(
    _cctx: LZ4F_compressionContext_t,
    _dstBuffer: *mut c_void,
    _dstCapacity: Size,
    _srcBuffer: *const c_void,
    _srcSize: Size,
    _cOptPtr: *const c_void,
) -> Size {
    0
}

// TODO: liblz4 LZ4F_compressEnd.
unsafe fn LZ4F_compressEnd(
    _cctx: LZ4F_compressionContext_t,
    _dstBuffer: *mut c_void,
    _dstCapacity: Size,
    _cOptPtr: *const c_void,
) -> Size {
    0
}

// ---------------------------------------------------------------------------
// psprintf stub: the only use here is `psprintf("%s.lz4", archive_name)`.
// A canonical psprintf is not centrally ported; build the ".lz4"-suffixed name
// with palloc + manual copy. TODO: port utils/mb/.. / psprintf proper.
// ---------------------------------------------------------------------------
unsafe fn psprintf_lz4(archive_name: *const c_char) -> *mut c_char {
    let mut len: Size = 0;
    while *archive_name.add(len) != 0 {
        len += 1;
    }
    const SUFFIX: &[u8] = b".lz4";
    let out = palloc(len + SUFFIX.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(archive_name, out, len);
    for (i, &b) in SUFFIX.iter().enumerate() {
        *out.add(len + i) = b as c_char;
    }
    *out.add(len + SUFFIX.len()) = 0;
    out
}

// ---------------------------------------------------------------------------
// bbsink_lz4: an lz4-compressing sink decorator. `base: bbsink` is first so
// *mut bbsink_lz4 and *mut bbsink are interconvertible by pointer cast.
// ---------------------------------------------------------------------------
#[repr(C)]
struct bbsink_lz4 {
    /// Common information for all types of sink.
    base: bbsink,

    /// Compression level.
    compresslevel: c_int,

    ctx: LZ4F_compressionContext_t,
    prefs: LZ4F_preferences_t,

    /// Number of bytes staged in output buffer.
    bytes_written: Size,
}

static bbsink_lz4_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_lz4_begin_backup),
    begin_archive: Some(bbsink_lz4_begin_archive),
    archive_contents: Some(bbsink_lz4_archive_contents),
    end_archive: Some(bbsink_lz4_end_archive),
    begin_manifest: Some(bbsink_forward_begin_manifest),
    manifest_contents: Some(bbsink_lz4_manifest_contents),
    end_manifest: Some(bbsink_forward_end_manifest),
    end_backup: Some(bbsink_forward_end_backup),
    cleanup: Some(bbsink_lz4_cleanup),
};

/// Create a new basebackup sink that performs lz4 compression.
pub unsafe fn bbsink_lz4_new(
    next: *mut bbsink,
    compress: *mut pg_compress_specification,
) -> *mut bbsink {
    Assert!(!next.is_null());

    let compresslevel = (*compress).level;
    Assert!(compresslevel >= 0 && compresslevel <= 12);

    let sink = palloc0(core::mem::size_of::<bbsink_lz4>()) as *mut bbsink_lz4;
    (*sink).base.bbs_ops = &bbsink_lz4_ops;
    (*sink).base.bbs_next = next;
    (*sink).compresslevel = compresslevel;

    &mut (*sink).base
}

/// Begin backup.
unsafe fn bbsink_lz4_begin_backup(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_lz4;
    let prefs: *mut LZ4F_preferences_t = &mut (*mysink).prefs;

    // Initialize compressor object.
    core::ptr::write_bytes(prefs, 0, 1);
    (*prefs).frameInfo.blockSizeID = LZ4F_max256KB;
    (*prefs).compressionLevel = (*mysink).compresslevel;

    // We need our own buffer, because we're going to pass different data to the
    // next sink than what gets passed to us.
    (*mysink).base.bbs_buffer = palloc((*mysink).base.bbs_buffer_length) as *mut c_char;

    // Since LZ4F_compressUpdate() requires the output buffer of size equal or
    // greater than that of LZ4F_compressBound(), make sure we have the next
    // sink's bbs_buffer of length that can accommodate the compressed input
    // buffer.
    let mut output_buffer_bound =
        LZ4F_compressBound((*mysink).base.bbs_buffer_length, &(*mysink).prefs);

    // The buffer length is expected to be a multiple of BLCKSZ, so round up.
    output_buffer_bound = output_buffer_bound + BLCKSZ - (output_buffer_bound % BLCKSZ);

    bbsink_begin_backup(
        (*sink).bbs_next,
        (*sink).bbs_state,
        output_buffer_bound as c_int,
    );
}

/// Prepare to compress the next archive.
unsafe fn bbsink_lz4_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    let mysink = sink as *mut bbsink_lz4;

    let ctxError = LZ4F_createCompressionContext(&mut (*mysink).ctx, LZ4F_VERSION);
    if LZ4F_isError(ctxError) != 0 {
        elog!(
            ERROR,
            "could not create lz4 compression context: {:?}",
            LZ4F_getErrorName(ctxError)
        );
    }

    // First of all write the frame header to destination buffer.
    let headerSize = LZ4F_compressBegin(
        (*mysink).ctx,
        (*(*mysink).base.bbs_next).bbs_buffer as *mut c_void,
        (*(*mysink).base.bbs_next).bbs_buffer_length,
        &(*mysink).prefs,
    );

    if LZ4F_isError(headerSize) != 0 {
        elog!(
            ERROR,
            "could not write lz4 header: {:?}",
            LZ4F_getErrorName(headerSize)
        );
    }

    // We need to write the compressed data after the header in the output
    // buffer. So, make sure to update the notion of bytes written to output
    // buffer.
    (*mysink).bytes_written += headerSize;

    // Add ".lz4" to the archive name.
    let lz4_archive_name = psprintf_lz4(archive_name);
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_begin_archive((*sink).bbs_next, lz4_archive_name);
    pfree(lz4_archive_name as *mut c_void);
}

/// Compress the input data to the output buffer until we run out of input
/// data. Each time the output buffer falls below the compression bound for
/// the input buffer, invoke the archive_contents() method for then next sink.
///
/// Note that since we're compressing the input, it may very commonly happen
/// that we consume all the input data without filling the output buffer. In
/// that case, the compressed representation of the current input data won't
/// actually be sent to the next bbsink until a later call to this function,
/// or perhaps even not until bbsink_lz4_end_archive() is invoked.
unsafe fn bbsink_lz4_archive_contents(sink: *mut bbsink, avail_in: Size) {
    let mysink = sink as *mut bbsink_lz4;

    let avail_in_bound = LZ4F_compressBound(avail_in, &(*mysink).prefs);

    // If the number of available bytes has fallen below the value computed by
    // LZ4F_compressBound(), ask the next sink to process the data so that we
    // can empty the buffer.
    if ((*(*mysink).base.bbs_next).bbs_buffer_length - (*mysink).bytes_written) < avail_in_bound {
        bbsink_archive_contents((*sink).bbs_next, (*mysink).bytes_written);
        (*mysink).bytes_written = 0;
    }

    // Compress the input buffer and write it into the output buffer.
    let compressedSize = LZ4F_compressUpdate(
        (*mysink).ctx,
        ((*(*mysink).base.bbs_next).bbs_buffer).add((*mysink).bytes_written) as *mut c_void,
        (*(*mysink).base.bbs_next).bbs_buffer_length - (*mysink).bytes_written,
        (*mysink).base.bbs_buffer as *const c_void,
        avail_in,
        null(),
    );

    if LZ4F_isError(compressedSize) != 0 {
        elog!(
            ERROR,
            "could not compress data: {:?}",
            LZ4F_getErrorName(compressedSize)
        );
    }

    // Update our notion of how many bytes we've written into output buffer.
    (*mysink).bytes_written += compressedSize;
}

/// There might be some data inside lz4's internal buffers; we need to get
/// that flushed out and also finalize the lz4 frame and then get that forwarded
/// to the successor sink as archive content.
///
/// Then we can end processing for this archive.
unsafe fn bbsink_lz4_end_archive(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_lz4;

    let lz4_footer_bound = LZ4F_compressBound(0, &(*mysink).prefs);

    Assert!((*(*mysink).base.bbs_next).bbs_buffer_length >= lz4_footer_bound);

    if ((*(*mysink).base.bbs_next).bbs_buffer_length - (*mysink).bytes_written) < lz4_footer_bound {
        bbsink_archive_contents((*sink).bbs_next, (*mysink).bytes_written);
        (*mysink).bytes_written = 0;
    }

    let compressedSize = LZ4F_compressEnd(
        (*mysink).ctx,
        ((*(*mysink).base.bbs_next).bbs_buffer).add((*mysink).bytes_written) as *mut c_void,
        (*(*mysink).base.bbs_next).bbs_buffer_length - (*mysink).bytes_written,
        null(),
    );

    if LZ4F_isError(compressedSize) != 0 {
        elog!(
            ERROR,
            "could not end lz4 compression: {:?}",
            LZ4F_getErrorName(compressedSize)
        );
    }

    // Update our notion of how many bytes we've written.
    (*mysink).bytes_written += compressedSize;

    // Send whatever accumulated output bytes we have.
    bbsink_archive_contents((*sink).bbs_next, (*mysink).bytes_written);
    (*mysink).bytes_written = 0;

    // Release the resources.
    LZ4F_freeCompressionContext((*mysink).ctx);
    (*mysink).ctx = null_mut();

    // Pass on the information that this archive has ended.
    bbsink_forward_end_archive(sink);
}

/// Manifest contents are not compressed, but we do need to copy them into
/// the successor sink's buffer, because we have our own.
unsafe fn bbsink_lz4_manifest_contents(sink: *mut bbsink, len: Size) {
    core::ptr::copy_nonoverlapping(
        (*sink).bbs_buffer,
        (*(*sink).bbs_next).bbs_buffer,
        len,
    );
    bbsink_manifest_contents((*sink).bbs_next, len);
}

/// In case the backup fails, make sure we free the compression context by
/// calling LZ4F_freeCompressionContext() if needed to avoid memory leak.
unsafe fn bbsink_lz4_cleanup(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_lz4;

    if !(*mysink).ctx.is_null() {
        LZ4F_freeCompressionContext((*mysink).ctx);
        (*mysink).ctx = null_mut();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // bbsink_lz4_new wires the ops table, the successor, and copies the
    // compression level out of the spec. The constructor never dereferences
    // bbs_next, so a dummy non-null pointer suffices.
    #[test]
    fn new_sets_level_and_next() {
        unsafe {
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            let mut spec = pg_compress_specification::default();
            spec.level = 7;

            let base = bbsink_lz4_new(next, &mut spec);
            let sink = base as *mut bbsink_lz4;

            assert_eq!((*sink).compresslevel, 7);
            assert_eq!((*sink).base.bbs_next, next);
            assert_eq!((*sink).bytes_written, 0);
            assert!((*sink).ctx.is_null());

            pfree(sink as *mut c_void);
            pfree(next as *mut c_void);
        }
    }

    // psprintf_lz4 must append ".lz4" and NUL-terminate.
    #[test]
    fn psprintf_lz4_appends_suffix() {
        unsafe {
            let name = c"base.tar";
            let out = psprintf_lz4(name.as_ptr());
            let s = core::ffi::CStr::from_ptr(out);
            assert_eq!(s.to_bytes(), b"base.tar.lz4");
            pfree(out as *mut c_void);
        }
    }
}
