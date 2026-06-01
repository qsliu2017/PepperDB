//! basebackup_gzip.c - Basebackup sink implementing gzip compression.
//!
//! Source: postgres/src/backend/backup/basebackup_gzip.c
//!
//! #include mapping:
//!   "postgres.h"                -> use crate::prelude::*
//!   <zlib.h>                    -> zlib z_stream / deflate* API (STUBBED locally below;
//!                                  PepperDB has no zlib binding yet). The real C file
//!                                  guards everything behind HAVE_LIBZ; we translate the
//!                                  HAVE_LIBZ path (the real logic) and stub the zlib FFI.
//!   "backup/basebackup_sink.h"  -> crate::backup::basebackup_sink (PORTED)

use crate::prelude::*;

use crate::backup::basebackup_sink::{
    bbsink, bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_manifest_contents, bbsink_ops,
    pg_compress_specification,
};

// ---------------------------------------------------------------------------
// Stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

// utils/mmgr/mcxt.c: psprintf("%s.gz", archive_name). PepperDB has no central
// varargs psprintf yet; for this single use we append ".gz" into a palloc'd
// NUL-terminated buffer, matching the only call site.
// TODO: import the real psprintf once ported.
unsafe fn psprintf_gz(archive_name: *const c_char) -> *mut c_char {
    let name_len = libc_strlen(archive_name);
    let suffix = b".gz";
    let total = name_len + suffix.len() + 1; // +1 for NUL
    let out = palloc(total) as *mut c_char;
    core::ptr::copy_nonoverlapping(archive_name as *const u8, out as *mut u8, name_len);
    core::ptr::copy_nonoverlapping(
        suffix.as_ptr(),
        (out as *mut u8).add(name_len),
        suffix.len(),
    );
    *out.add(name_len + suffix.len()) = 0;
    out
}

// strlen on a C string (length not counting the NUL).
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// ---------------------------------------------------------------------------
// <zlib.h> minimal binding. STUBBED: PepperDB does not yet link zlib. We mirror
// the z_stream layout and the deflate* entry points so the algorithm below is a
// faithful 1:1 translation; the actual compression is left unimplemented.
// TODO: replace with a real zlib FFI binding (extern "C" to libz) or a pure-Rust
// deflate implementation.
// ---------------------------------------------------------------------------

#[allow(non_camel_case_types)]
type alloc_func = unsafe fn(opaque: *mut c_void, items: c_uint, size: c_uint) -> *mut c_void;
#[allow(non_camel_case_types)]
type free_func = unsafe fn(opaque: *mut c_void, address: *mut c_void);

// zlib's z_stream. Field set/order matches what the algorithm touches; the
// remaining bookkeeping fields are present so size_of is plausible for palloc.
#[allow(non_camel_case_types)]
#[repr(C)]
struct z_stream {
    next_in: *mut uint8,
    avail_in: c_uint,
    total_in: c_ulong,
    next_out: *mut uint8,
    avail_out: c_uint,
    total_out: c_ulong,
    msg: *mut c_char,
    state: *mut c_void,
    zalloc: Option<alloc_func>,
    zfree: Option<free_func>,
    opaque: *mut c_void,
    data_type: c_int,
    adler: c_ulong,
    reserved: c_ulong,
}

// zlib return codes.
const Z_OK: c_int = 0;
const Z_STREAM_ERROR: c_int = -2;

// deflate() flush values.
const Z_NO_FLUSH: c_int = 0;
const Z_FINISH: c_int = 4;

// deflate method / strategy constants.
const Z_DEFLATED: c_int = 8;
const Z_DEFAULT_STRATEGY: c_int = 0;
const Z_DEFAULT_COMPRESSION: c_int = -1;

// deflateInit2: initialize the compressor. STUB: TODO real zlib.
unsafe fn deflateInit2(
    _strm: *mut z_stream,
    _level: c_int,
    _method: c_int,
    _windowBits: c_int,
    _memLevel: c_int,
    _strategy: c_int,
) -> c_int {
    unimplemented!("zlib deflateInit2 not yet available in PepperDB")
}

// deflate: compress (or flush) one chunk. STUB: TODO real zlib.
unsafe fn deflate(_strm: *mut z_stream, _flush: c_int) -> c_int {
    unimplemented!("zlib deflate not yet available in PepperDB")
}

// ---------------------------------------------------------------------------
// bbsink_gzip: gzip-compressing sink decorator. Embeds `base: bbsink` first so
// that *mut bbsink_gzip and *mut bbsink are interconvertible by pointer cast.
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct bbsink_gzip {
    /// Common information for all types of sink.
    pub base: bbsink,

    /// Compression level.
    pub compresslevel: c_int,

    /// Compressed data stream.
    zstream: z_stream,

    /// Number of bytes staged in output buffer.
    pub bytes_written: Size,
}

// ---------------------------------------------------------------------------
// Ops table: begin_backup/begin_archive/archive_contents/manifest_contents/
// end_archive are overridden; the rest forward to the successor sink.
// ---------------------------------------------------------------------------
static bbsink_gzip_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_gzip_begin_backup),
    begin_archive: Some(bbsink_gzip_begin_archive),
    archive_contents: Some(bbsink_gzip_archive_contents),
    end_archive: Some(bbsink_gzip_end_archive),
    begin_manifest: Some(bbsink_forward_begin_manifest),
    manifest_contents: Some(bbsink_gzip_manifest_contents),
    end_manifest: Some(bbsink_forward_end_manifest),
    end_backup: Some(bbsink_forward_end_backup),
    cleanup: Some(bbsink_forward_cleanup),
};

/// Create a new basebackup sink that performs gzip compression.
pub unsafe fn bbsink_gzip_new(
    next: *mut bbsink,
    compress: *mut pg_compress_specification,
) -> *mut bbsink {
    Assert!(!next.is_null());

    let compresslevel = compress_level(compress);
    Assert!((compresslevel >= 1 && compresslevel <= 9) || compresslevel == Z_DEFAULT_COMPRESSION);

    let sink = palloc0(core::mem::size_of::<bbsink_gzip>()) as *mut bbsink_gzip;
    (*sink).base.bbs_ops = &bbsink_gzip_ops;
    (*sink).base.bbs_next = next;
    (*sink).compresslevel = compresslevel;

    &mut (*sink).base
}

// Accessor for compress->level. pg_compress_specification is currently an opaque
// (fieldless) placeholder in basebackup_sink.rs, so we cannot read .level. STUB:
// returns the zlib default compression level. TODO: read compress->level once
// pg_compress_specification carries its fields.
unsafe fn compress_level(_compress: *mut pg_compress_specification) -> c_int {
    Z_DEFAULT_COMPRESSION
}

/// Begin backup.
unsafe fn bbsink_gzip_begin_backup(sink: *mut bbsink) {
    // We need our own buffer, because we're going to pass different data to the
    // next sink than what gets passed to us.
    (*sink).bbs_buffer = palloc((*sink).bbs_buffer_length) as *mut c_char;

    // Since deflate() doesn't require the output buffer to be of any particular
    // size, we can just make it the same size as the input buffer.
    bbsink_begin_backup(
        (*sink).bbs_next,
        (*sink).bbs_state,
        (*sink).bbs_buffer_length as c_int,
    );
}

/// Prepare to compress the next archive.
unsafe fn bbsink_gzip_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    let mysink = sink as *mut bbsink_gzip;
    let zs: *mut z_stream = &mut (*mysink).zstream;

    // Initialize compressor object.
    core::ptr::write_bytes(zs, 0, 1);
    (*zs).zalloc = Some(gzip_palloc);
    (*zs).zfree = Some(gzip_pfree);
    (*zs).next_out = (*(*sink).bbs_next).bbs_buffer as *mut uint8;
    (*zs).avail_out = (*(*sink).bbs_next).bbs_buffer_length as c_uint;

    // We need to use deflateInit2() rather than deflateInit() here so that we
    // can request a gzip header rather than a zlib header. Otherwise, we want to
    // supply the same values that would have been used by default if we had just
    // called deflateInit().
    //
    // Per the documentation for deflateInit2, the third argument must be
    // Z_DEFLATED; the fourth argument is the number of "window bits", by default
    // 15, but adding 16 gets you a gzip header rather than a zlib header; the
    // fifth argument controls memory usage, and 8 is the default; and likewise
    // Z_DEFAULT_STRATEGY is the default for the sixth argument.
    if deflateInit2(
        zs,
        (*mysink).compresslevel,
        Z_DEFLATED,
        15 + 16,
        8,
        Z_DEFAULT_STRATEGY,
    ) != Z_OK
    {
        ereport!(ERROR, "could not initialize compression library");
    }

    // Add ".gz" to the archive name. Note that the pg_basebackup -z produces
    // archives named ".tar.gz" rather than ".tgz", so we match that here.
    let gz_archive_name = psprintf_gz(archive_name);
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_begin_archive((*sink).bbs_next, gz_archive_name);
    pfree(gz_archive_name as *mut c_void);
}

/// Compress the input data to the output buffer until we run out of input data.
/// Each time the output buffer fills up, invoke the archive_contents() method
/// for the next sink.
///
/// Note that since we're compressing the input, it may very commonly happen that
/// we consume all the input data without filling the output buffer. In that
/// case, the compressed representation of the current input data won't actually
/// be sent to the next bbsink until a later call to this function, or perhaps
/// even not until bbsink_gzip_end_archive() is invoked.
unsafe fn bbsink_gzip_archive_contents(sink: *mut bbsink, len: Size) {
    let mysink = sink as *mut bbsink_gzip;
    let zs: *mut z_stream = &mut (*mysink).zstream;

    // Compress data from input buffer.
    (*zs).next_in = (*mysink).base.bbs_buffer as *mut uint8;
    (*zs).avail_in = len as c_uint;

    while (*zs).avail_in > 0 {
        // Write output data into unused portion of output buffer.
        Assert!((*mysink).bytes_written < (*(*mysink).base.bbs_next).bbs_buffer_length);
        (*zs).next_out =
            ((*(*mysink).base.bbs_next).bbs_buffer as *mut uint8).add((*mysink).bytes_written);
        (*zs).avail_out =
            ((*(*mysink).base.bbs_next).bbs_buffer_length - (*mysink).bytes_written) as c_uint;

        // Try to compress. Note that this will update zs->next_in and
        // zs->avail_in according to how much input data was consumed, and
        // zs->next_out and zs->avail_out according to how many output bytes were
        // produced.
        //
        // According to the zlib documentation, Z_STREAM_ERROR should only occur
        // if we've made a programming error, or if say there's been a memory
        // clobber; we use elog() rather than Assert() here out of an abundance
        // of caution.
        let res = deflate(zs, Z_NO_FLUSH);
        if res == Z_STREAM_ERROR {
            elog!(ERROR, "could not compress data: {:?}", (*zs).msg);
        }

        // Update our notion of how many bytes we've written.
        (*mysink).bytes_written =
            (*(*mysink).base.bbs_next).bbs_buffer_length - (*zs).avail_out as Size;

        // If the output buffer is full, it's time for the next sink to process
        // the contents.
        if (*mysink).bytes_written >= (*(*mysink).base.bbs_next).bbs_buffer_length {
            bbsink_archive_contents((*sink).bbs_next, (*mysink).bytes_written);
            (*mysink).bytes_written = 0;
        }
    }
}

/// There might be some data inside zlib's internal buffers; we need to get that
/// flushed out and forwarded to the successor sink as archive content.
///
/// Then we can end processing for this archive.
unsafe fn bbsink_gzip_end_archive(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_gzip;
    let zs: *mut z_stream = &mut (*mysink).zstream;

    // There is no more data available.
    (*zs).next_in = (*mysink).base.bbs_buffer as *mut uint8;
    (*zs).avail_in = 0;

    loop {
        // Write output data into unused portion of output buffer.
        Assert!((*mysink).bytes_written < (*(*mysink).base.bbs_next).bbs_buffer_length);
        (*zs).next_out =
            ((*(*mysink).base.bbs_next).bbs_buffer as *mut uint8).add((*mysink).bytes_written);
        (*zs).avail_out =
            ((*(*mysink).base.bbs_next).bbs_buffer_length - (*mysink).bytes_written) as c_uint;

        // As bbsink_gzip_archive_contents, but pass Z_FINISH since there is no
        // more input.
        let res = deflate(zs, Z_FINISH);
        if res == Z_STREAM_ERROR {
            elog!(ERROR, "could not compress data: {:?}", (*zs).msg);
        }

        // Update our notion of how many bytes we've written.
        (*mysink).bytes_written =
            (*(*mysink).base.bbs_next).bbs_buffer_length - (*zs).avail_out as Size;

        // Apparently we had no data in the output buffer and deflate() was not
        // able to add any. We must be done.
        if (*mysink).bytes_written == 0 {
            break;
        }

        // Send whatever accumulated output bytes we have.
        bbsink_archive_contents((*sink).bbs_next, (*mysink).bytes_written);
        (*mysink).bytes_written = 0;
    }

    // Must also pass on the information that this archive has ended.
    bbsink_forward_end_archive(sink);
}

/// Manifest contents are not compressed, but we do need to copy them into the
/// successor sink's buffer, because we have our own.
unsafe fn bbsink_gzip_manifest_contents(sink: *mut bbsink, len: Size) {
    core::ptr::copy_nonoverlapping(
        (*sink).bbs_buffer as *const u8,
        (*(*sink).bbs_next).bbs_buffer as *mut u8,
        len,
    );
    bbsink_manifest_contents((*sink).bbs_next, len);
}

/// Wrapper function to adjust the signature of palloc to match what libz
/// expects.
unsafe fn gzip_palloc(_opaque: *mut c_void, items: c_uint, size: c_uint) -> *mut c_void {
    palloc((items as Size) * (size as Size))
}

/// Wrapper function to adjust the signature of pfree to match what libz expects.
unsafe fn gzip_pfree(_opaque: *mut c_void, address: *mut c_void) {
    pfree(address);
}
