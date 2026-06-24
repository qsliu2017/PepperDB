//! Translated from PostgreSQL src/include/access/generic_xlog.h

use bitflags::bitflags;

use crate::access::xloginsert::XLR_NORMAL_MAX_BLOCK_ID;
use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::PageMut;
use crate::utils::rel::Relation;

pub const MAX_GENERIC_XLOG_PAGES: i32 = XLR_NORMAL_MAX_BLOCK_ID;

bitflags! {
    /// Flag bits for GenericXLogRegisterBuffer.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GenericXLogFlags: i32 {
        /// write full-page image
        const FULL_IMAGE = 0x0001;
    }
}

/// state of generic xlog record construction (opaque; defined in generic_xlog.c)
pub struct GenericXLogState {
    _private: [u8; 0],
}

/* API for construction of generic xlog records */
pub fn generic_xlog_start(_relation: Relation) -> *mut GenericXLogState {
    unimplemented!()
}
pub fn generic_xlog_register_buffer<'a>(
    _state: &mut GenericXLogState,
    _buffer: Buffer,
    _flags: GenericXLogFlags,
) -> PageMut<'a> {
    unimplemented!()
}
pub fn generic_xlog_finish(_state: &mut GenericXLogState) -> XLogRecPtr {
    unimplemented!()
}
pub fn generic_xlog_abort(_state: &mut GenericXLogState) {
    unimplemented!()
}

/* functions defined for rmgr */
pub fn generic_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn generic_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn generic_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn generic_mask(_page: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
