//! Default implementations for bbsink (basebackup sink) callbacks.
//!
//! Source: postgres/src/backend/backup/basebackup_sink.c
//! Merged header: postgres/src/include/backup/basebackup_sink.h
//!
//! #include mapping:
//!   "postgres.h"                  -> use crate::prelude::*
//!   "backup/basebackup_sink.h"    -> merged into this file (struct/ops/inline fns)
//!     header's #include "access/xlogdefs.h"     -> XLogRecPtr/TimeLineID (local aliases, STUB)
//!     header's #include "common/compression.h"  -> pg_compress_specification (STUB, only used by
//!                                                   the *_new constructors which are not defined here)
//!     header's #include "nodes/pg_list.h"        -> crate::nodes::pg_list::{List, list_length}

use crate::prelude::*;

use crate::nodes::pg_list::{list_length, List};
use crate::pg_config::BLCKSZ;

// ---------------------------------------------------------------------------
// Types pulled in from access/xlogdefs.h.
//
// There is no canonical xlogdefs.rs yet; the rest of the crate declares these
// as local aliases (see nodes/replnodes.rs). Mirror that convention here.
// ---------------------------------------------------------------------------
pub type XLogRecPtr = uint64;
pub type TimeLineID = uint32;

// pg_compress_specification comes from common/compression.h. It is only
// referenced by the bbsink_*_new constructors declared in the header, none of
// which are implemented in basebackup_sink.c. STUB: an opaque placeholder so
// the constructor signatures could later be expressed; not used in this file.
// TODO: translate common/compression.h -> pg_compress_specification.
pub enum pg_compress_specification {}

// ---------------------------------------------------------------------------
// bbsink_state: overall backup state shared by all bbsink objects.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct bbsink_state {
    pub tablespaces: *mut List,
    pub tablespace_num: c_int,
    pub bytes_done: uint64,
    pub bytes_total: uint64,
    pub bytes_total_is_valid: bool,
    pub startptr: XLogRecPtr,
    pub starttli: TimeLineID,
}

// ---------------------------------------------------------------------------
// bbsink: common data for any type of basebackup sink.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct bbsink {
    pub bbs_ops: *const bbsink_ops,
    pub bbs_buffer: *mut c_char,
    pub bbs_buffer_length: Size,
    pub bbs_next: *mut bbsink,
    pub bbs_state: *mut bbsink_state,
}

// ---------------------------------------------------------------------------
// bbsink_ops: callback vtable for a base backup sink.
//
// All callbacks are required (the inline dispatch wrappers Assert and call
// through unconditionally). Option<unsafe fn(...)> matches the crate's vtable
// convention (pure-Rust, no FFI).
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct bbsink_ops {
    pub begin_backup: Option<unsafe fn(sink: *mut bbsink)>,
    pub begin_archive: Option<unsafe fn(sink: *mut bbsink, archive_name: *const c_char)>,
    pub archive_contents: Option<unsafe fn(sink: *mut bbsink, len: Size)>,
    pub end_archive: Option<unsafe fn(sink: *mut bbsink)>,
    pub begin_manifest: Option<unsafe fn(sink: *mut bbsink)>,
    pub manifest_contents: Option<unsafe fn(sink: *mut bbsink, len: Size)>,
    pub end_manifest: Option<unsafe fn(sink: *mut bbsink)>,
    pub end_backup: Option<unsafe fn(sink: *mut bbsink, endptr: XLogRecPtr, endtli: TimeLineID)>,
    pub cleanup: Option<unsafe fn(sink: *mut bbsink)>,
}

// ---------------------------------------------------------------------------
// Inline dispatch wrappers (from the header).
//
// Callers should always invoke the callbacks via these rather than reaching
// into the vtable directly. Each Asserts the op is present and calls it.
// ---------------------------------------------------------------------------

/// Begin a backup.
#[inline]
pub unsafe fn bbsink_begin_backup(sink: *mut bbsink, state: *mut bbsink_state, buffer_length: c_int) {
    Assert!(!sink.is_null());

    Assert!(buffer_length > 0);

    (*sink).bbs_state = state;
    (*sink).bbs_buffer_length = buffer_length as Size;
    ((*(*sink).bbs_ops).begin_backup.unwrap())(sink);

    Assert!(!(*sink).bbs_buffer.is_null());
    Assert!(((*sink).bbs_buffer_length % BLCKSZ) == 0);
}

/// Begin an archive.
#[inline]
pub unsafe fn bbsink_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    Assert!(!sink.is_null());

    ((*(*sink).bbs_ops).begin_archive.unwrap())(sink, archive_name);
}

/// Process some of the contents of an archive.
#[inline]
pub unsafe fn bbsink_archive_contents(sink: *mut bbsink, len: Size) {
    Assert!(!sink.is_null());

    // The caller should make a reasonable attempt to fill the buffer before
    // calling this function, so it shouldn't be completely empty. Nor should
    // it be filled beyond capacity.
    Assert!(len > 0 && len <= (*sink).bbs_buffer_length);

    ((*(*sink).bbs_ops).archive_contents.unwrap())(sink, len);
}

/// Finish an archive.
#[inline]
pub unsafe fn bbsink_end_archive(sink: *mut bbsink) {
    Assert!(!sink.is_null());

    ((*(*sink).bbs_ops).end_archive.unwrap())(sink);
}

/// Begin the backup manifest.
#[inline]
pub unsafe fn bbsink_begin_manifest(sink: *mut bbsink) {
    Assert!(!sink.is_null());

    ((*(*sink).bbs_ops).begin_manifest.unwrap())(sink);
}

/// Process some of the manifest contents.
#[inline]
pub unsafe fn bbsink_manifest_contents(sink: *mut bbsink, len: Size) {
    Assert!(!sink.is_null());

    // See comments in bbsink_archive_contents.
    Assert!(len > 0 && len <= (*sink).bbs_buffer_length);

    ((*(*sink).bbs_ops).manifest_contents.unwrap())(sink, len);
}

/// Finish the backup manifest.
#[inline]
pub unsafe fn bbsink_end_manifest(sink: *mut bbsink) {
    Assert!(!sink.is_null());

    ((*(*sink).bbs_ops).end_manifest.unwrap())(sink);
}

/// Finish a backup.
#[inline]
pub unsafe fn bbsink_end_backup(sink: *mut bbsink, endptr: XLogRecPtr, endtli: TimeLineID) {
    Assert!(!sink.is_null());
    Assert!((*(*sink).bbs_state).tablespace_num == list_length((*(*sink).bbs_state).tablespaces));

    ((*(*sink).bbs_ops).end_backup.unwrap())(sink, endptr, endtli);
}

/// Release resources before destruction.
#[inline]
pub unsafe fn bbsink_cleanup(sink: *mut bbsink) {
    Assert!(!sink.is_null());

    ((*(*sink).bbs_ops).cleanup.unwrap())(sink);
}

// ---------------------------------------------------------------------------
// Forwarding callbacks (the default pass-through implementations).
//
// Use these to pass operations through to the next sink in the chain.
// ---------------------------------------------------------------------------

/// Forward begin_backup callback.
///
/// Only use this implementation if you want the bbsink you're implementing to
/// share a buffer with the successor bbsink.
pub unsafe fn bbsink_forward_begin_backup(sink: *mut bbsink) {
    Assert!(!(*sink).bbs_next.is_null());
    Assert!(!(*sink).bbs_state.is_null());
    bbsink_begin_backup(
        (*sink).bbs_next,
        (*sink).bbs_state,
        (*sink).bbs_buffer_length as c_int,
    );
    (*sink).bbs_buffer = (*(*sink).bbs_next).bbs_buffer;
}

/// Forward begin_archive callback.
pub unsafe fn bbsink_forward_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_begin_archive((*sink).bbs_next, archive_name);
}

/// Forward archive_contents callback.
///
/// Code that wants to use this should initialize its own bbs_buffer and
/// bbs_buffer_length fields to the values from the successor sink. In cases
/// where the buffer isn't shared, the data needs to be copied before forwarding
/// the callback. We don't try to do that here, because there's really no reason
/// to have separately allocated buffers containing the same identical data.
pub unsafe fn bbsink_forward_archive_contents(sink: *mut bbsink, len: Size) {
    Assert!(!(*sink).bbs_next.is_null());
    Assert!((*sink).bbs_buffer == (*(*sink).bbs_next).bbs_buffer);
    Assert!((*sink).bbs_buffer_length == (*(*sink).bbs_next).bbs_buffer_length);
    bbsink_archive_contents((*sink).bbs_next, len);
}

/// Forward end_archive callback.
pub unsafe fn bbsink_forward_end_archive(sink: *mut bbsink) {
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_end_archive((*sink).bbs_next);
}

/// Forward begin_manifest callback.
pub unsafe fn bbsink_forward_begin_manifest(sink: *mut bbsink) {
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_begin_manifest((*sink).bbs_next);
}

/// Forward manifest_contents callback.
///
/// As with the archive_contents callback, it's expected that the buffer is
/// shared.
pub unsafe fn bbsink_forward_manifest_contents(sink: *mut bbsink, len: Size) {
    Assert!(!(*sink).bbs_next.is_null());
    Assert!((*sink).bbs_buffer == (*(*sink).bbs_next).bbs_buffer);
    Assert!((*sink).bbs_buffer_length == (*(*sink).bbs_next).bbs_buffer_length);
    bbsink_manifest_contents((*sink).bbs_next, len);
}

/// Forward end_manifest callback.
pub unsafe fn bbsink_forward_end_manifest(sink: *mut bbsink) {
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_end_manifest((*sink).bbs_next);
}

/// Forward end_backup callback.
pub unsafe fn bbsink_forward_end_backup(sink: *mut bbsink, endptr: XLogRecPtr, endtli: TimeLineID) {
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_end_backup((*sink).bbs_next, endptr, endtli);
}

/// Forward cleanup callback.
pub unsafe fn bbsink_forward_cleanup(sink: *mut bbsink) {
    Assert!(!(*sink).bbs_next.is_null());
    bbsink_cleanup((*sink).bbs_next);
}

#[cfg(test)]
mod tests {
    use super::*;

    // A recording counter for the terminal sink's archive_contents op, so a
    // test can prove that bbsink_forward_archive_contents delegates to bbs_next.
    static mut RECORDED_LEN: Size = 0;
    static mut CALL_COUNT: c_int = 0;

    unsafe fn terminal_archive_contents(_sink: *mut bbsink, len: Size) {
        RECORDED_LEN = len;
        CALL_COUNT += 1;
    }

    static TERMINAL_OPS: bbsink_ops = bbsink_ops {
        begin_backup: None,
        begin_archive: None,
        archive_contents: Some(terminal_archive_contents),
        end_archive: None,
        begin_manifest: None,
        manifest_contents: None,
        end_manifest: None,
        end_backup: None,
        cleanup: None,
    };

    static FORWARD_OPS: bbsink_ops = bbsink_ops {
        begin_backup: Some(bbsink_forward_begin_backup),
        begin_archive: Some(bbsink_forward_begin_archive),
        archive_contents: Some(bbsink_forward_archive_contents),
        end_archive: Some(bbsink_forward_end_archive),
        begin_manifest: Some(bbsink_forward_begin_manifest),
        manifest_contents: Some(bbsink_forward_manifest_contents),
        end_manifest: Some(bbsink_forward_end_manifest),
        end_backup: Some(bbsink_forward_end_backup),
        cleanup: Some(bbsink_forward_cleanup),
    };

    // Build a 2-element sink chain by hand (palloc0 + assign individual fields,
    // since bbsink/bbsink_ops have no private fields but we mirror the required
    // construction style) and verify that the forwarding op delegates to
    // bbs_next, hitting the recording terminal op with the right length.
    #[test]
    fn forward_archive_contents_delegates_to_next() {
        unsafe {
            CALL_COUNT = 0;
            RECORDED_LEN = 0;

            // Shared buffer (must be a multiple of BLCKSZ per the contract).
            let buffer_length: Size = BLCKSZ;
            let buffer = palloc0(buffer_length) as *mut c_char;

            // Terminal (successor) sink.
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            (*next).bbs_ops = &TERMINAL_OPS;
            (*next).bbs_buffer = buffer;
            (*next).bbs_buffer_length = buffer_length;
            (*next).bbs_next = null_mut();
            (*next).bbs_state = null_mut();

            // Forwarding (front) sink, sharing the same buffer.
            let front = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            (*front).bbs_ops = &FORWARD_OPS;
            (*front).bbs_buffer = buffer;
            (*front).bbs_buffer_length = buffer_length;
            (*front).bbs_next = next;
            (*front).bbs_state = null_mut();

            // Dispatch through the front sink; it should forward to the terminal.
            let len: Size = 4096;
            bbsink_archive_contents(front, len);

            assert_eq!(CALL_COUNT, 1);
            assert_eq!(RECORDED_LEN, len);

            pfree(front as *mut c_void);
            pfree(next as *mut c_void);
            pfree(buffer as *mut c_void);
        }
    }
}
