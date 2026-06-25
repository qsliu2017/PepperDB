//! Translated from PostgreSQL src/include/access/xloginsert.h
//!
//! WAL-record construction API. The REGBUF_* flags ([`RegBuf`]) live here; the
//! function bodies live in `crate::backend::access::transam::xloginsert` and are
//! re-exported below so header call sites resolve. The construction functions
//! that touch shared WAL state ([`XLogInsert`], `log_newpage*`) are `async` and
//! take the [`XLogCtl`](crate::backend::access::transam::xlog::XLogCtl) handle.

use bitflags::bitflags;

/// The minimum size of the WAL construction working area; call
/// `XLogEnsureRecordSpace` to grow beyond these.
pub const XLR_NORMAL_MAX_BLOCK_ID: i32 = 4;
pub const XLR_NORMAL_RDATAS: i32 = 20;

bitflags! {
    /// Flags for XLogRegisterBuffer (PARTIAL: composite `WILL_INIT = 0x06`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RegBuf: u8 {
        const FORCE_IMAGE = 0x01;          // force a full-page image
        const NO_IMAGE    = 0x02;          // don't take a full-page image
        // page will be re-initialized at replay (implies NO_IMAGE)
        const WILL_INIT   = 0x04 | 0x02;
        const STANDARD    = 0x08;          // page follows "standard" layout
        const KEEP_DATA   = 0x10;          // include data even with a full-page image
        const NO_CHANGE   = 0x20;          // intentionally register clean buffer
    }
}

// The construction API is implemented in the backend module; re-export under the
// header-facing names. The begin/register/set/reset functions operate on the
// per-task staging (see `with_insertion`); `XLogInsert` and `log_newpage*` are
// async and take the `XLogCtl` handle.
pub use crate::backend::access::transam::xloginsert::{
    begin_insert as XLogBeginInsert, check_page_needs_backup as XLogCheckBufferNeedsBackup,
    log_newpage, log_newpage_range, log_newpages, register_block as XLogRegisterBlock,
    register_buf_data as XLogRegisterBufData, register_data as XLogRegisterData,
    reset_insertion as XLogResetInsertion, set_record_flags as XLogSetRecordFlags,
    with_insertion, xlog_insert as XLogInsert,
};
