//! basebackup_zstd.c - Basebackup sink implementing zstd compression.
//!
//! Source: postgres/src/backend/backup/basebackup_zstd.c
//!
//! #include mapping:
//!   "postgres.h"                -> use crate::prelude::*
//!   <zstd.h>                    -> libzstd FFI (STUBBED locally; see below)
//!   "backup/basebackup_sink.h"  -> crate::backup::basebackup_sink (PORTED)
//!   common/compression.h        -> crate::common::compression (PORTED;
//!                                  pg_compress_specification + option flags)
//!
//! The C file compiles two ways. Without USE_ZSTD, bbsink_zstd_new() just
//! ereports "zstd compression is not supported by this build". With USE_ZSTD,
//! the full streaming-compression sink is built. We translate the USE_ZSTD
//! path (the real logic) and stub the libzstd C API locally, since the zstd
//! library bindings are not yet wired into the crate.

use crate::prelude::*;

use crate::common::compression::{
    pg_compress_specification, PG_COMPRESSION_OPTION_LONG_DISTANCE, PG_COMPRESSION_OPTION_WORKERS,
};
use crate::backup::basebackup_sink::{
    bbsink, bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_end_archive, bbsink_forward_end_backup,
    bbsink_forward_end_manifest, bbsink_manifest_contents, bbsink_ops, TimeLineID, XLogRecPtr,
};

// BLCKSZ from pg_config.h.
use crate::pg_config::BLCKSZ;

// ---------------------------------------------------------------------------
// libzstd FFI (<zstd.h>). STUBBED: the zstd C library is not yet linked into
// the crate. These mirror the real zstd API signatures so the translated
// streaming logic is faithful; the bodies are placeholders.
// TODO: replace with real bindings to libzstd (zstd-sys or extern "C").
// ---------------------------------------------------------------------------

// Opaque compression context (ZSTD_CCtx).
type ZSTD_CCtx = c_void;

// ZSTD_outBuffer / ZSTD_inBuffer: streaming buffer descriptors.
#[repr(C)]
struct ZSTD_outBuffer {
    dst: *mut c_void,
    size: Size,
    pos: Size,
}

#[repr(C)]
struct ZSTD_inBuffer {
    src: *const c_void,
    size: Size,
    pos: Size,
}

// ZSTD_cParameter enum members used here.
type ZSTD_cParameter = c_int;
const ZSTD_c_compressionLevel: ZSTD_cParameter = 100;
const ZSTD_c_nbWorkers: ZSTD_cParameter = 400;
const ZSTD_c_enableLongDistanceMatching: ZSTD_cParameter = 160;

// ZSTD_ResetDirective enum member used here.
type ZSTD_ResetDirective = c_int;
const ZSTD_reset_session_only: ZSTD_ResetDirective = 1;

// ZSTD_EndDirective enum members used here.
type ZSTD_EndDirective = c_int;
const ZSTD_e_continue: ZSTD_EndDirective = 0;
const ZSTD_e_end: ZSTD_EndDirective = 2;

// TODO: link real libzstd. These stubs let the translated control flow compile.
unsafe fn ZSTD_createCCtx() -> *mut ZSTD_CCtx {
    unimplemented!("ZSTD_createCCtx: libzstd not yet linked")
}

unsafe fn ZSTD_freeCCtx(_cctx: *mut ZSTD_CCtx) -> Size {
    unimplemented!("ZSTD_freeCCtx: libzstd not yet linked")
}

unsafe fn ZSTD_CCtx_setParameter(
    _cctx: *mut ZSTD_CCtx,
    _param: ZSTD_cParameter,
    _value: c_int,
) -> Size {
    unimplemented!("ZSTD_CCtx_setParameter: libzstd not yet linked")
}

unsafe fn ZSTD_CCtx_reset(_cctx: *mut ZSTD_CCtx, _reset: ZSTD_ResetDirective) -> Size {
    unimplemented!("ZSTD_CCtx_reset: libzstd not yet linked")
}

unsafe fn ZSTD_compressStream2(
    _cctx: *mut ZSTD_CCtx,
    _output: *mut ZSTD_outBuffer,
    _input: *mut ZSTD_inBuffer,
    _end_op: ZSTD_EndDirective,
) -> Size {
    unimplemented!("ZSTD_compressStream2: libzstd not yet linked")
}

unsafe fn ZSTD_compressBound(_src_size: Size) -> Size {
    unimplemented!("ZSTD_compressBound: libzstd not yet linked")
}

unsafe fn ZSTD_isError(_code: Size) -> c_uint {
    unimplemented!("ZSTD_isError: libzstd not yet linked")
}

unsafe fn ZSTD_getErrorName(_code: Size) -> *const c_char {
    unimplemented!("ZSTD_getErrorName: libzstd not yet linked")
}

// ---------------------------------------------------------------------------
// utils/errcodes.h code used by the ereport()s below. STUB: placeholder
// constant (mirrors the convention used by other ported units).
// TODO: port utils/errcodes.h.
// ---------------------------------------------------------------------------
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// utils/mmgr/mcxt.c psprintf: format into a palloc'd buffer. Not yet ported.
// Here we only ever need `psprintf("%s.zst", archive_name)`, so this helper
// reproduces exactly that: it appends the literal ".zst" suffix.
// TODO: port the general psprintf (utils/mmgr/mcxt.c).
unsafe fn psprintf_zst_suffix(archive_name: *const c_char) -> *mut c_char {
    let name_len = libc_strlen(archive_name);
    let suffix = b".zst\0";
    let total = name_len + (suffix.len() - 1); // not counting NUL of suffix here
    let buf = palloc(total + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(archive_name as *const u8, buf as *mut u8, name_len);
    core::ptr::copy_nonoverlapping(
        suffix.as_ptr(),
        (buf as *mut u8).add(name_len),
        suffix.len(),
    );
    buf
}

// strlen for a NUL-terminated C string (no libc dependency assumed in prelude).
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// ---------------------------------------------------------------------------
// bbsink_zstd: a zstd-compressing sink. Embeds `base: bbsink` as its first
// field so a *mut bbsink_zstd and *mut bbsink are interconvertible by cast.
// ---------------------------------------------------------------------------
#[repr(C)]
struct bbsink_zstd {
    /// Common information for all types of sink.
    base: bbsink,

    /// Compression options.
    compress: *mut pg_compress_specification,

    cctx: *mut ZSTD_CCtx,
    zstd_outBuf: ZSTD_outBuffer,
}

// ---------------------------------------------------------------------------
// Ops table.
// ---------------------------------------------------------------------------
static bbsink_zstd_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_zstd_begin_backup),
    begin_archive: Some(bbsink_zstd_begin_archive),
    archive_contents: Some(bbsink_zstd_archive_contents),
    end_archive: Some(bbsink_zstd_end_archive),
    begin_manifest: Some(bbsink_forward_begin_manifest),
    manifest_contents: Some(bbsink_zstd_manifest_contents),
    end_manifest: Some(bbsink_forward_end_manifest),
    end_backup: Some(bbsink_zstd_end_backup),
    cleanup: Some(bbsink_zstd_cleanup),
};

/// Create a new basebackup sink that performs zstd compression.
pub unsafe fn bbsink_zstd_new(
    next: *mut bbsink,
    compress: *mut pg_compress_specification,
) -> *mut bbsink {
    Assert!(!next.is_null());

    let sink = palloc0(core::mem::size_of::<bbsink_zstd>()) as *mut bbsink_zstd;
    (*sink).base.bbs_ops = &bbsink_zstd_ops;
    (*sink).base.bbs_next = next;
    (*sink).compress = compress;

    &mut (*sink).base
}

/// Begin backup.
unsafe fn bbsink_zstd_begin_backup(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_zstd;
    let compress = (*mysink).compress;

    (*mysink).cctx = ZSTD_createCCtx();
    if (*mysink).cctx.is_null() {
        elog!(ERROR, "could not create zstd compression context");
    }

    let mut ret = ZSTD_CCtx_setParameter(
        (*mysink).cctx,
        ZSTD_c_compressionLevel,
        (*compress).level,
    );
    if ZSTD_isError(ret) != 0 {
        elog!(
            ERROR,
            "could not set zstd compression level to {}: {}",
            (*compress).level,
            cstr_to_string(ZSTD_getErrorName(ret))
        );
    }

    if ((*compress).options & PG_COMPRESSION_OPTION_WORKERS) != 0 {
        // On older versions of libzstd, this option does not exist, and trying
        // to set it will fail. Similarly for newer versions if they are
        // compiled without threading support.
        ret = ZSTD_CCtx_setParameter((*mysink).cctx, ZSTD_c_nbWorkers, (*compress).workers);
        if ZSTD_isError(ret) != 0 {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            elog!(
                ERROR,
                "could not set compression worker count to {}: {}",
                (*compress).workers,
                cstr_to_string(ZSTD_getErrorName(ret))
            );
        }
    }

    if ((*compress).options & PG_COMPRESSION_OPTION_LONG_DISTANCE) != 0 {
        ret = ZSTD_CCtx_setParameter(
            (*mysink).cctx,
            ZSTD_c_enableLongDistanceMatching,
            (*compress).long_distance as c_int,
        );
        if ZSTD_isError(ret) != 0 {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            elog!(
                ERROR,
                "could not enable long-distance mode: {}",
                cstr_to_string(ZSTD_getErrorName(ret))
            );
        }
    }

    // We need our own buffer, because we're going to pass different data to the
    // next sink than what gets passed to us.
    (*mysink).base.bbs_buffer = palloc((*mysink).base.bbs_buffer_length) as *mut c_char;

    // Make sure that the next sink's bbs_buffer is big enough to accommodate
    // the compressed input buffer.
    let mut output_buffer_bound = ZSTD_compressBound((*mysink).base.bbs_buffer_length);

    // The buffer length is expected to be a multiple of BLCKSZ, so round up.
    output_buffer_bound = output_buffer_bound + BLCKSZ - (output_buffer_bound % BLCKSZ);

    bbsink_begin_backup(
        (*sink).bbs_next,
        (*sink).bbs_state,
        output_buffer_bound as c_int,
    );
}

/// Prepare to compress the next archive.
unsafe fn bbsink_zstd_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    let mysink = sink as *mut bbsink_zstd;

    // At the start of each archive we reset the state to start a new
    // compression operation. The parameters are sticky and they will stick
    // around as we are resetting with option ZSTD_reset_session_only.
    ZSTD_CCtx_reset((*mysink).cctx, ZSTD_reset_session_only);

    (*mysink).zstd_outBuf.dst = (*(*mysink).base.bbs_next).bbs_buffer as *mut c_void;
    (*mysink).zstd_outBuf.size = (*(*mysink).base.bbs_next).bbs_buffer_length;
    (*mysink).zstd_outBuf.pos = 0;

    // Add ".zst" to the archive name.
    let zstd_archive_name = psprintf_zst_suffix(archive_name);
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_begin_archive((*sink).bbs_next, zstd_archive_name);
    pfree(zstd_archive_name as *mut c_void);
}

/// Compress the input data to the output buffer until we run out of input data.
/// Each time the output buffer falls below the compression bound for the input
/// buffer, invoke the archive_contents() method for the next sink.
///
/// Note that since we're compressing the input, it may very commonly happen
/// that we consume all the input data without filling the output buffer. In
/// that case, the compressed representation of the current input data won't
/// actually be sent to the next bbsink until a later call to this function, or
/// perhaps even not until bbsink_zstd_end_archive() is invoked.
unsafe fn bbsink_zstd_archive_contents(sink: *mut bbsink, len: Size) {
    let mysink = sink as *mut bbsink_zstd;
    let mut inBuf = ZSTD_inBuffer {
        src: (*mysink).base.bbs_buffer as *const c_void,
        size: len,
        pos: 0,
    };

    while inBuf.pos < inBuf.size {
        let max_needed = ZSTD_compressBound(inBuf.size - inBuf.pos);

        // If the out buffer is not left with enough space, send the output
        // buffer to the next sink, and reset it.
        if (*mysink).zstd_outBuf.size - (*mysink).zstd_outBuf.pos < max_needed {
            bbsink_archive_contents((*mysink).base.bbs_next, (*mysink).zstd_outBuf.pos);
            (*mysink).zstd_outBuf.dst = (*(*mysink).base.bbs_next).bbs_buffer as *mut c_void;
            (*mysink).zstd_outBuf.size = (*(*mysink).base.bbs_next).bbs_buffer_length;
            (*mysink).zstd_outBuf.pos = 0;
        }

        let yet_to_flush = ZSTD_compressStream2(
            (*mysink).cctx,
            &mut (*mysink).zstd_outBuf,
            &mut inBuf,
            ZSTD_e_continue,
        );

        if ZSTD_isError(yet_to_flush) != 0 {
            elog!(
                ERROR,
                "could not compress data: {}",
                cstr_to_string(ZSTD_getErrorName(yet_to_flush))
            );
        }
    }
}

/// There might be some data inside zstd's internal buffers; we need to get that
/// flushed out, also end the zstd frame and then get that forwarded to the
/// successor sink as archive content.
///
/// Then we can end processing for this archive.
unsafe fn bbsink_zstd_end_archive(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_zstd;
    let mut yet_to_flush: Size;

    loop {
        let mut in_ = ZSTD_inBuffer {
            src: null(),
            size: 0,
            pos: 0,
        };
        let max_needed = ZSTD_compressBound(0);

        // If the out buffer is not left with enough space, send the output
        // buffer to the next sink, and reset it.
        if (*mysink).zstd_outBuf.size - (*mysink).zstd_outBuf.pos < max_needed {
            bbsink_archive_contents((*mysink).base.bbs_next, (*mysink).zstd_outBuf.pos);
            (*mysink).zstd_outBuf.dst = (*(*mysink).base.bbs_next).bbs_buffer as *mut c_void;
            (*mysink).zstd_outBuf.size = (*(*mysink).base.bbs_next).bbs_buffer_length;
            (*mysink).zstd_outBuf.pos = 0;
        }

        yet_to_flush = ZSTD_compressStream2(
            (*mysink).cctx,
            &mut (*mysink).zstd_outBuf,
            &mut in_,
            ZSTD_e_end,
        );

        if ZSTD_isError(yet_to_flush) != 0 {
            elog!(
                ERROR,
                "could not compress data: {}",
                cstr_to_string(ZSTD_getErrorName(yet_to_flush))
            );
        }

        if yet_to_flush == 0 {
            break;
        }
    }

    // Make sure to pass any remaining bytes to the next sink.
    if (*mysink).zstd_outBuf.pos > 0 {
        bbsink_archive_contents((*mysink).base.bbs_next, (*mysink).zstd_outBuf.pos);
    }

    // Pass on the information that this archive has ended.
    bbsink_forward_end_archive(sink);
}

/// Free the resources and context.
unsafe fn bbsink_zstd_end_backup(sink: *mut bbsink, endptr: XLogRecPtr, endtli: TimeLineID) {
    let mysink = sink as *mut bbsink_zstd;

    // Release the context.
    if !(*mysink).cctx.is_null() {
        ZSTD_freeCCtx((*mysink).cctx);
        (*mysink).cctx = null_mut();
    }

    bbsink_forward_end_backup(sink, endptr, endtli);
}

/// Manifest contents are not compressed, but we do need to copy them into the
/// successor sink's buffer, because we have our own.
unsafe fn bbsink_zstd_manifest_contents(sink: *mut bbsink, len: Size) {
    core::ptr::copy_nonoverlapping(
        (*sink).bbs_buffer as *const u8,
        (*(*sink).bbs_next).bbs_buffer as *mut u8,
        len,
    );
    bbsink_manifest_contents((*sink).bbs_next, len);
}

/// In case the backup fails, make sure we free any compression context that got
/// allocated, so that we don't leak memory.
unsafe fn bbsink_zstd_cleanup(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_zstd;

    // Release the context if not already released.
    if !(*mysink).cctx.is_null() {
        ZSTD_freeCCtx((*mysink).cctx);
        (*mysink).cctx = null_mut();
    }
}

// Best-effort rendering of a C error-name string for elog formatting. Used only
// to feed the {} placeholders in the error messages above.
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    let len = libc_strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    // bbsink_zstd_new wires the ops table, the successor, and the compress
    // spec; it does not touch libzstd (so no stub is exercised). The successor
    // pointer is never dereferenced by the constructor.
    #[test]
    fn new_wires_fields() {
        unsafe {
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            let mut spec = pg_compress_specification::default();
            let base = bbsink_zstd_new(next, &mut spec);
            let sink = base as *mut bbsink_zstd;

            assert_eq!((*sink).base.bbs_next, next);
            assert_eq!((*sink).compress, &mut spec as *mut _);
            assert!((*sink).cctx.is_null());

            pfree(sink as *mut c_void);
            pfree(next as *mut c_void);
        }
    }

    // psprintf_zst_suffix("base.tar") must yield "base.tar.zst" (NUL-terminated).
    #[test]
    fn zst_suffix_appended() {
        unsafe {
            let name = b"base.tar\0";
            let out = psprintf_zst_suffix(name.as_ptr() as *const c_char);
            let s = cstr_to_string(out);
            assert_eq!(s, "base.tar.zst");
            pfree(out as *mut c_void);
        }
    }
}
