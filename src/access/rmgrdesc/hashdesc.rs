//! Translation of postgres/src/backend/access/rmgrdesc/hashdesc.c
//!                + the xl_hash_* record structs and XLOG_HASH_* opcodes it
//!                  reads from postgres/src/include/access/hash_xlog.h
//!
//! rmgr descriptor routines for the hash AM (used by pg_waldump). hash_desc
//! casts the WAL record payload to the appropriate xl_hash_* struct (selected
//! by the record's info byte) and appends a human-readable summary of its
//! fields; hash_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   storage/off.h            -> OffsetNumber (crate::prelude / storage::off)
//!   c.h types                -> uint8/uint16/uint32, bool, RegProcedure,
//!                               TransactionId, BlockNumber
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the SizeOf* offsets, the XLOG_HASH_* opcode values, and
//! the hash_identify name table are REAL (faithful to hash_xlog.h / hashdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from c.h / storage/off.h / storage/block.h)
// ---------------------------------------------------------------------------

pub type OffsetNumber = uint16;
pub type BlockNumber = uint32;

// ---------------------------------------------------------------------------
// XLOG records for hash operations (hash_xlog.h)
// ---------------------------------------------------------------------------

/// Number of buffers required for XLOG_HASH_SQUEEZE_PAGE operation.
pub const HASH_XLOG_FREE_OVFL_BUFS: c_int = 6;

pub const XLOG_HASH_INIT_META_PAGE: uint8 = 0x00; // initialize the meta page
pub const XLOG_HASH_INIT_BITMAP_PAGE: uint8 = 0x10; // initialize the bitmap page
pub const XLOG_HASH_INSERT: uint8 = 0x20; // add index tuple without split
pub const XLOG_HASH_ADD_OVFL_PAGE: uint8 = 0x30; // add overflow page
pub const XLOG_HASH_SPLIT_ALLOCATE_PAGE: uint8 = 0x40; // allocate new page for split
pub const XLOG_HASH_SPLIT_PAGE: uint8 = 0x50; // split page
pub const XLOG_HASH_SPLIT_COMPLETE: uint8 = 0x60; // completion of split operation
pub const XLOG_HASH_MOVE_PAGE_CONTENTS: uint8 = 0x70; // remove tuples from one page and add to another
pub const XLOG_HASH_SQUEEZE_PAGE: uint8 = 0x80; // add tuples to a previous page in chain and free the ovfl page
pub const XLOG_HASH_DELETE: uint8 = 0x90; // delete index tuples from a page
pub const XLOG_HASH_SPLIT_CLEANUP: uint8 = 0xA0; // clear split-cleanup flag in primary bucket page
pub const XLOG_HASH_UPDATE_META_PAGE: uint8 = 0xB0; // update meta page after vacuum
pub const XLOG_HASH_VACUUM_ONE_PAGE: uint8 = 0xC0; // remove dead tuples from index page

/// xl_hash_split_allocate_page flag values (8 bits available).
pub const XLH_SPLIT_META_UPDATE_MASKS: uint8 = 1 << 0;
pub const XLH_SPLIT_META_UPDATE_SPLITPOINT: uint8 = 1 << 1;

/// XLOG_HASH_INSERT: simple (without split) insert.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_insert {
    pub offnum: OffsetNumber,
}

/// XLOG_HASH_ADD_OVFL_PAGE: addition of an overflow page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_add_ovfl_page {
    pub bmsize: uint16,
    pub bmpage_found: bool,
}

/// XLOG_HASH_SPLIT_ALLOCATE_PAGE: allocating a page for split.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_split_allocate_page {
    pub new_bucket: uint32,
    pub old_bucket_flag: uint16,
    pub new_bucket_flag: uint16,
    pub flags: uint8,
}

/// XLOG_HASH_SPLIT_COMPLETE: completion of the split operation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_split_complete {
    pub old_bucket_flag: uint16,
    pub new_bucket_flag: uint16,
}

/// XLOG_HASH_MOVE_PAGE_CONTENTS: move page contents during squeeze.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_move_page_contents {
    pub ntups: uint16,
    /// true if the page to which tuples are moved is the primary bucket page.
    pub is_prim_bucket_same_wrt: bool,
}

/// XLOG_HASH_SQUEEZE_PAGE: squeeze page operation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_squeeze_page {
    pub prevblkno: BlockNumber,
    pub nextblkno: BlockNumber,
    pub ntups: uint16,
    /// true if the page to which tuples are moved is the primary bucket page.
    pub is_prim_bucket_same_wrt: bool,
    /// true if the page to which tuples are moved is the page previous to the
    /// freed overflow page.
    pub is_prev_bucket_same_wrt: bool,
}

/// XLOG_HASH_DELETE: deletion of index tuples from a page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_delete {
    /// true if this operation clears the LH_PAGE_HAS_DEAD_TUPLES flag.
    pub clear_dead_marking: bool,
    /// true if the operation is for the primary bucket page.
    pub is_primary_bucket_page: bool,
}

/// XLOG_HASH_UPDATE_META_PAGE: metapage update operation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_update_meta_page {
    pub ntuples: f64,
}

/// XLOG_HASH_INIT_META_PAGE: initialize metapage.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_init_meta_page {
    pub num_tuples: f64,
    pub procid: RegProcedure,
    pub ffactor: uint16,
}

/// XLOG_HASH_INIT_BITMAP_PAGE: initialize bitmap page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_init_bitmap_page {
    pub bmsize: uint16,
}

/// XLOG_HASH_VACUUM_ONE_PAGE: index tuple deletion + meta page update.
///
/// In C this has a trailing `OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER]`; the
/// flexible array is omitted here (callers index past the fixed header).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_hash_vacuum_one_page {
    pub snapshotConflictHorizon: TransactionId,
    pub ntuples: uint16,
    /// to handle recovery conflict during logical decoding on standby.
    pub isCatalogRel: bool,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] -- omitted (flexible array).
}

// ---------------------------------------------------------------------------
// SizeOf* macros (hash_xlog.h). Each is `offsetof(struct, last) + sizeof(last)`,
// i.e. the size up to and including the last fixed member.
// ---------------------------------------------------------------------------

/// offsetof(xl_hash_insert, offnum) + sizeof(OffsetNumber)
pub const SizeOfHashInsert: usize =
    core::mem::offset_of!(xl_hash_insert, offnum) + core::mem::size_of::<OffsetNumber>();

/// offsetof(xl_hash_add_ovfl_page, bmpage_found) + sizeof(bool)
pub const SizeOfHashAddOvflPage: usize =
    core::mem::offset_of!(xl_hash_add_ovfl_page, bmpage_found) + core::mem::size_of::<bool>();

/// offsetof(xl_hash_split_allocate_page, flags) + sizeof(uint8)
pub const SizeOfHashSplitAllocPage: usize =
    core::mem::offset_of!(xl_hash_split_allocate_page, flags) + core::mem::size_of::<uint8>();

/// offsetof(xl_hash_split_complete, new_bucket_flag) + sizeof(uint16)
pub const SizeOfHashSplitComplete: usize =
    core::mem::offset_of!(xl_hash_split_complete, new_bucket_flag) + core::mem::size_of::<uint16>();

/// offsetof(xl_hash_move_page_contents, is_prim_bucket_same_wrt) + sizeof(bool)
pub const SizeOfHashMovePageContents: usize =
    core::mem::offset_of!(xl_hash_move_page_contents, is_prim_bucket_same_wrt)
        + core::mem::size_of::<bool>();

/// offsetof(xl_hash_squeeze_page, is_prev_bucket_same_wrt) + sizeof(bool)
pub const SizeOfHashSqueezePage: usize =
    core::mem::offset_of!(xl_hash_squeeze_page, is_prev_bucket_same_wrt)
        + core::mem::size_of::<bool>();

/// offsetof(xl_hash_delete, is_primary_bucket_page) + sizeof(bool)
pub const SizeOfHashDelete: usize =
    core::mem::offset_of!(xl_hash_delete, is_primary_bucket_page) + core::mem::size_of::<bool>();

/// offsetof(xl_hash_update_meta_page, ntuples) + sizeof(double)
pub const SizeOfHashUpdateMetaPage: usize =
    core::mem::offset_of!(xl_hash_update_meta_page, ntuples) + core::mem::size_of::<f64>();

/// offsetof(xl_hash_init_meta_page, ffactor) + sizeof(uint16)
pub const SizeOfHashInitMetaPage: usize =
    core::mem::offset_of!(xl_hash_init_meta_page, ffactor) + core::mem::size_of::<uint16>();

/// offsetof(xl_hash_init_bitmap_page, bmsize) + sizeof(uint16)
pub const SizeOfHashInitBitmapPage: usize =
    core::mem::offset_of!(xl_hash_init_bitmap_page, bmsize) + core::mem::size_of::<uint16>();

/// offsetof(xl_hash_vacuum_one_page, offsets) -- the fixed header size (the
/// flexible `offsets` array begins here).
pub const SizeOfHashVacuumOnePage: usize = core::mem::size_of::<xl_hash_vacuum_one_page>();

// ---------------------------------------------------------------------------
// Descriptor routines (hashdesc.c)
// ---------------------------------------------------------------------------

/// Render a single space-delimited 'T'/'F' from a bool, matching the C
/// `(cond) ? 'T' : 'F'` idiom.
#[inline]
fn tf(b: bool) -> char {
    if b {
        'T'
    } else {
        'F'
    }
}

/// hash_desc: describe a hash-AM WAL record into `buf`.
///
/// # Safety
/// `record` is an opaque XLogReaderState pointer; the data pointer it yields
/// (via the stubbed XLogRecGetData) is cast to the per-opcode struct.
pub unsafe fn hash_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec: *mut c_char = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_HASH_INIT_META_PAGE => {
            let xlrec = rec as *const xl_hash_init_meta_page;
            appendStringInfo!(
                buf,
                "num_tuples {}, fillfactor {}",
                (*xlrec).num_tuples,
                (*xlrec).ffactor
            );
        }
        XLOG_HASH_INIT_BITMAP_PAGE => {
            let xlrec = rec as *const xl_hash_init_bitmap_page;
            appendStringInfo!(buf, "bmsize {}", (*xlrec).bmsize);
        }
        XLOG_HASH_INSERT => {
            let xlrec = rec as *const xl_hash_insert;
            appendStringInfo!(buf, "off {}", (*xlrec).offnum);
        }
        XLOG_HASH_ADD_OVFL_PAGE => {
            let xlrec = rec as *const xl_hash_add_ovfl_page;
            appendStringInfo!(
                buf,
                "bmsize {}, bmpage_found {}",
                (*xlrec).bmsize,
                tf((*xlrec).bmpage_found)
            );
        }
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => {
            let xlrec = rec as *const xl_hash_split_allocate_page;
            appendStringInfo!(
                buf,
                "new_bucket {}, meta_page_masks_updated {}, issplitpoint_changed {}",
                (*xlrec).new_bucket,
                tf((*xlrec).flags & XLH_SPLIT_META_UPDATE_MASKS != 0),
                tf((*xlrec).flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0)
            );
        }
        XLOG_HASH_SPLIT_COMPLETE => {
            let xlrec = rec as *const xl_hash_split_complete;
            appendStringInfo!(
                buf,
                "old_bucket_flag {}, new_bucket_flag {}",
                (*xlrec).old_bucket_flag,
                (*xlrec).new_bucket_flag
            );
        }
        XLOG_HASH_MOVE_PAGE_CONTENTS => {
            let xlrec = rec as *const xl_hash_move_page_contents;
            appendStringInfo!(
                buf,
                "ntups {}, is_primary {}",
                (*xlrec).ntups,
                tf((*xlrec).is_prim_bucket_same_wrt)
            );
        }
        XLOG_HASH_SQUEEZE_PAGE => {
            let xlrec = rec as *const xl_hash_squeeze_page;
            appendStringInfo!(
                buf,
                "prevblkno {}, nextblkno {}, ntups {}, is_primary {}",
                (*xlrec).prevblkno,
                (*xlrec).nextblkno,
                (*xlrec).ntups,
                tf((*xlrec).is_prim_bucket_same_wrt)
            );
        }
        XLOG_HASH_DELETE => {
            let xlrec = rec as *const xl_hash_delete;
            appendStringInfo!(
                buf,
                "clear_dead_marking {}, is_primary {}",
                tf((*xlrec).clear_dead_marking),
                tf((*xlrec).is_primary_bucket_page)
            );
        }
        XLOG_HASH_UPDATE_META_PAGE => {
            let xlrec = rec as *const xl_hash_update_meta_page;
            appendStringInfo!(buf, "ntuples {}", (*xlrec).ntuples);
        }
        XLOG_HASH_VACUUM_ONE_PAGE => {
            let xlrec = rec as *const xl_hash_vacuum_one_page;
            appendStringInfo!(
                buf,
                "ntuples {}, snapshotConflictHorizon {}, isCatalogRel {}",
                (*xlrec).ntuples,
                (*xlrec).snapshotConflictHorizon,
                tf((*xlrec).isCatalogRel)
            );
        }
        _ => {}
    }
}

/// hash_identify: return the name of the hash-AM opcode encoded in `info`.
/// Returns a NUL-terminated C string, or null for an unknown opcode (matching
/// the C `const char *id = NULL`).
pub fn hash_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        XLOG_HASH_INIT_META_PAGE => b"INIT_META_PAGE\0",
        XLOG_HASH_INIT_BITMAP_PAGE => b"INIT_BITMAP_PAGE\0",
        XLOG_HASH_INSERT => b"INSERT\0",
        XLOG_HASH_ADD_OVFL_PAGE => b"ADD_OVFL_PAGE\0",
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => b"SPLIT_ALLOCATE_PAGE\0",
        XLOG_HASH_SPLIT_PAGE => b"SPLIT_PAGE\0",
        XLOG_HASH_SPLIT_COMPLETE => b"SPLIT_COMPLETE\0",
        XLOG_HASH_MOVE_PAGE_CONTENTS => b"MOVE_PAGE_CONTENTS\0",
        XLOG_HASH_SQUEEZE_PAGE => b"SQUEEZE_PAGE\0",
        XLOG_HASH_DELETE => b"DELETE\0",
        XLOG_HASH_SPLIT_CLEANUP => b"SPLIT_CLEANUP\0",
        XLOG_HASH_UPDATE_META_PAGE => b"UPDATE_META_PAGE\0",
        XLOG_HASH_VACUUM_ONE_PAGE => b"VACUUM_ONE_PAGE\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    #[test]
    fn identify_insert_returns_insert() {
        let p = hash_identify(XLOG_HASH_INSERT);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "INSERT");
    }

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (XLOG_HASH_INIT_META_PAGE, "INIT_META_PAGE"),
            (XLOG_HASH_INIT_BITMAP_PAGE, "INIT_BITMAP_PAGE"),
            (XLOG_HASH_INSERT, "INSERT"),
            (XLOG_HASH_ADD_OVFL_PAGE, "ADD_OVFL_PAGE"),
            (XLOG_HASH_SPLIT_ALLOCATE_PAGE, "SPLIT_ALLOCATE_PAGE"),
            (XLOG_HASH_SPLIT_PAGE, "SPLIT_PAGE"),
            (XLOG_HASH_SPLIT_COMPLETE, "SPLIT_COMPLETE"),
            (XLOG_HASH_MOVE_PAGE_CONTENTS, "MOVE_PAGE_CONTENTS"),
            (XLOG_HASH_SQUEEZE_PAGE, "SQUEEZE_PAGE"),
            (XLOG_HASH_DELETE, "DELETE"),
            (XLOG_HASH_SPLIT_CLEANUP, "SPLIT_CLEANUP"),
            (XLOG_HASH_UPDATE_META_PAGE, "UPDATE_META_PAGE"),
            (XLOG_HASH_VACUUM_ONE_PAGE, "VACUUM_ONE_PAGE"),
        ];
        for &(op, name) in cases {
            let p = hash_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should have a name", op);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name, "opcode {:#x}", op);
        }
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0xF0 has no rmgr-low-bit opcode after masking off XLR_INFO_MASK.
        assert!(hash_identify(0xF0).is_null());
    }

    #[test]
    fn insert_struct_size() {
        // xl_hash_insert is a single OffsetNumber (uint16).
        assert_eq!(core::mem::size_of::<xl_hash_insert>(), 2);
        assert_eq!(SizeOfHashInsert, 2);
    }

    #[test]
    fn sizeof_macros_match_field_layout() {
        assert_eq!(SizeOfHashSplitComplete, 4); // two uint16
        assert_eq!(SizeOfHashInitBitmapPage, 2); // one uint16
        assert_eq!(SizeOfHashUpdateMetaPage, 8); // one f64
        // split_allocate_page: up to and including flags (uint8).
        assert_eq!(
            SizeOfHashSplitAllocPage,
            core::mem::offset_of!(xl_hash_split_allocate_page, flags) + 1
        );
    }

    #[test]
    fn split_flag_bits() {
        assert_eq!(XLH_SPLIT_META_UPDATE_MASKS, 1);
        assert_eq!(XLH_SPLIT_META_UPDATE_SPLITPOINT, 2);
    }
}
