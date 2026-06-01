//! Translation of postgres/src/backend/access/rmgrdesc/nbtdesc.c
//!                + the xl_btree_* record structs and XLOG_BTREE_* opcodes it
//!                  reads from postgres/src/include/access/nbtxlog.h
//!
//! rmgr descriptor routines for the nbtree (B-tree) AM (used by pg_waldump).
//! btree_desc casts the WAL record payload to the appropriate xl_btree_* struct
//! (selected by the record's info byte) and appends a human-readable summary of
//! its fields; btree_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   access/transam.h         -> FullTransactionId, Epoch/XidFromFullTransactionId
//!                               (crate::access::transam)
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   storage/off.h            -> OffsetNumber
//!   storage/block.h          -> BlockNumber
//!   c.h types                -> uint8/uint16/uint32, bool, TransactionId
//!   common/relpath.h         -> RelFileNumber
//!   storage/relfilelocator.h -> RelFileLocator (spcOid/dbOid/relNumber)
//!   access/rmgrdesc_utils.h  -> array_desc, offset_elem_desc
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!   - XLogRecHasBlockData / XLogRecGetBlockData: stubbed (no block data).
//!
//! The struct layouts, the SizeOf* offsets, the XLOG_BTREE_* opcode values, and
//! the btree_identify name table are REAL (faithful to nbtxlog.h / nbtdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::rmgrdesc::rmgrdesc_utils::{array_desc, offset_elem_desc};
use crate::access::transam::{
    EpochFromFullTransactionId, FullTransactionId, XidFromFullTransactionId,
};
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetBlockData, XLogRecGetData, XLogRecGetInfo, XLogRecHasBlockData,
    XLR_INFO_MASK,
};
use crate::lib::stringinfo::{appendStringInfoChar, appendStringInfoString, StringInfo};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from c.h / storage/off.h / storage/block.h / common/relpath.h)
// ---------------------------------------------------------------------------

pub type OffsetNumber = uint16;
pub type BlockNumber = uint32;
pub type RelFileNumber = Oid;

/// RelFileLocator (storage/relfilelocator.h): physical locator of a relation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

// ---------------------------------------------------------------------------
// XLOG records for btree operations (nbtxlog.h)
//
// XLOG allows storing info in the high 4 bits of the record's xl_info field.
// ---------------------------------------------------------------------------

pub const XLOG_BTREE_INSERT_LEAF: uint8 = 0x00; // add index tuple without split
pub const XLOG_BTREE_INSERT_UPPER: uint8 = 0x10; // same, on a non-leaf page
pub const XLOG_BTREE_INSERT_META: uint8 = 0x20; // same, plus update metapage
pub const XLOG_BTREE_SPLIT_L: uint8 = 0x30; // add index tuple with split
pub const XLOG_BTREE_SPLIT_R: uint8 = 0x40; // as above, new item on right
pub const XLOG_BTREE_INSERT_POST: uint8 = 0x50; // add index tuple with posting split
pub const XLOG_BTREE_DEDUP: uint8 = 0x60; // deduplicate tuples for a page
pub const XLOG_BTREE_DELETE: uint8 = 0x70; // delete leaf index tuples for a page
pub const XLOG_BTREE_UNLINK_PAGE: uint8 = 0x80; // delete a half-dead page
pub const XLOG_BTREE_UNLINK_PAGE_META: uint8 = 0x90; // same, and update metapage
pub const XLOG_BTREE_NEWROOT: uint8 = 0xA0; // new root page
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: uint8 = 0xB0; // mark a leaf as half-dead
pub const XLOG_BTREE_VACUUM: uint8 = 0xC0; // delete entries on a page during vacuum
pub const XLOG_BTREE_REUSE_PAGE: uint8 = 0xD0; // old page is about to be reused from FSM
pub const XLOG_BTREE_META_CLEANUP: uint8 = 0xE0; // update cleanup-related data in the metapage

/// All that we need to regenerate the meta-data page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_metadata {
    pub version: uint32,
    pub root: BlockNumber,
    pub level: uint32,
    pub fastroot: BlockNumber,
    pub fastlevel: uint32,
    pub last_cleanup_num_delpages: uint32,
    pub allequalimage: bool,
}

/// Simple (without split) insert. Used for INSERT_LEAF, INSERT_UPPER,
/// INSERT_META, and INSERT_POST. The new tuple (and posting split offset for
/// INSERT_POST) follow at the end in WAL.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_insert {
    pub offnum: OffsetNumber,
    // POSTING SPLIT OFFSET FOLLOWS (INSERT_POST case)
    // NEW TUPLE ALWAYS FOLLOWS AT THE END
}

/// Insert with split. XLOG_BTREE_SPLIT_L and _R share this record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_split {
    pub level: uint32,             // tree level of page being split
    pub firstrightoff: OffsetNumber, // first origpage item on rightpage
    pub newitemoff: OffsetNumber,  // new item's offset
    pub postingoff: uint16,        // offset inside orig posting tuple
}

/// Deduplication pass for a leaf page. An array of BTDedupInterval structs
/// follows in WAL.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_dedup {
    pub nintervals: uint16,
    // DEDUPLICATION INTERVALS FOLLOW
}

/// Page reuse within btree. Only exists to generate a conflict point for Hot
/// Standby; includes a RelFileLocator since the buffer is not registered.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_reuse_page {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    /// to handle recovery conflict during logical decoding on standby.
    pub isCatalogRel: bool,
}

/// Deletion of index tuples on a leaf page by VACUUM. Offsets follow in blk 0.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_vacuum {
    pub ndeleted: uint16,
    pub nupdated: uint16,
    // In payload of blk 0: deleted offsets, updated offsets, xl_btree_update items.
}

/// Deletion of index tuples on a leaf page by ad-hoc btinsert() deletions.
/// Like xl_btree_vacuum but with snapshotConflictHorizon/isCatalogRel for
/// recovery conflicts.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_delete {
    pub snapshotConflictHorizon: TransactionId,
    pub ndeleted: uint16,
    pub nupdated: uint16,
    /// to handle recovery conflict during logical decoding on standby.
    pub isCatalogRel: bool,
    // In payload of blk 0: deleted offsets, updated offsets, xl_btree_update items.
}

/// Metadata for an "updated" posting list tuple: offsets here are 0-based
/// offsets into the original posting list, not page offset numbers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_update {
    pub ndeletedtids: uint16,
    // POSTING LIST uint16 OFFSETS TO A DELETED TID FOLLOW
}

/// Marking an empty subtree for deletion (half-dead leaf).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_mark_page_halfdead {
    pub poffset: OffsetNumber, // deleted tuple id in parent page
    // information needed to recreate the leaf page:
    pub leafblk: BlockNumber,    // leaf block ultimately being deleted
    pub leftblk: BlockNumber,    // leaf block's left sibling, if any
    pub rightblk: BlockNumber,   // leaf block's right sibling
    pub topparent: BlockNumber,  // topmost internal page in the subtree
}

/// Deletion (unlink) of a btree page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_unlink_page {
    pub leftsib: BlockNumber,        // target block's left sibling, if any
    pub rightsib: BlockNumber,       // target block's right sibling
    pub level: uint32,               // target block's level
    pub safexid: FullTransactionId,  // target block's BTPageSetDeleted() XID
    // Half-dead leaf recreation info (used when target is an internal page):
    pub leafleftsib: BlockNumber,
    pub leafrightsib: BlockNumber,
    pub leaftopparent: BlockNumber,  // next child down in the subtree
    // xl_btree_metadata FOLLOWS IF XLOG_BTREE_UNLINK_PAGE_META
}

/// New root log record (zero tuples for an empty root, two if splitting an old
/// root).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_btree_newroot {
    pub rootblk: BlockNumber, // location of new root (redundant with blk 0)
    pub level: uint32,        // its tree level
}

// ---------------------------------------------------------------------------
// SizeOf* macros (nbtxlog.h). Each is `offsetof(struct, last) + sizeof(last)`,
// i.e. the size up to and including the last fixed member.
// ---------------------------------------------------------------------------

/// offsetof(xl_btree_insert, offnum) + sizeof(OffsetNumber)
pub const SizeOfBtreeInsert: usize =
    core::mem::offset_of!(xl_btree_insert, offnum) + core::mem::size_of::<OffsetNumber>();

/// offsetof(xl_btree_split, postingoff) + sizeof(uint16)
pub const SizeOfBtreeSplit: usize =
    core::mem::offset_of!(xl_btree_split, postingoff) + core::mem::size_of::<uint16>();

/// offsetof(xl_btree_dedup, nintervals) + sizeof(uint16)
pub const SizeOfBtreeDedup: usize =
    core::mem::offset_of!(xl_btree_dedup, nintervals) + core::mem::size_of::<uint16>();

/// offsetof(xl_btree_reuse_page, isCatalogRel) + sizeof(bool)
pub const SizeOfBtreeReusePage: usize =
    core::mem::offset_of!(xl_btree_reuse_page, isCatalogRel) + core::mem::size_of::<bool>();

/// offsetof(xl_btree_vacuum, nupdated) + sizeof(uint16)
pub const SizeOfBtreeVacuum: usize =
    core::mem::offset_of!(xl_btree_vacuum, nupdated) + core::mem::size_of::<uint16>();

/// offsetof(xl_btree_delete, isCatalogRel) + sizeof(bool)
pub const SizeOfBtreeDelete: usize =
    core::mem::offset_of!(xl_btree_delete, isCatalogRel) + core::mem::size_of::<bool>();

/// offsetof(xl_btree_update, ndeletedtids) + sizeof(uint16)
pub const SizeOfBtreeUpdate: usize =
    core::mem::offset_of!(xl_btree_update, ndeletedtids) + core::mem::size_of::<uint16>();

/// offsetof(xl_btree_mark_page_halfdead, topparent) + sizeof(BlockNumber)
pub const SizeOfBtreeMarkPageHalfDead: usize =
    core::mem::offset_of!(xl_btree_mark_page_halfdead, topparent)
        + core::mem::size_of::<BlockNumber>();

/// offsetof(xl_btree_unlink_page, leaftopparent) + sizeof(BlockNumber)
pub const SizeOfBtreeUnlinkPage: usize =
    core::mem::offset_of!(xl_btree_unlink_page, leaftopparent) + core::mem::size_of::<BlockNumber>();

/// offsetof(xl_btree_newroot, level) + sizeof(uint32)
pub const SizeOfBtreeNewroot: usize =
    core::mem::offset_of!(xl_btree_newroot, level) + core::mem::size_of::<uint32>();

// ---------------------------------------------------------------------------
// Descriptor routines (nbtdesc.c)
// ---------------------------------------------------------------------------

/// delvacuum_desc: describe the deleted/updated offset arrays carried in block
/// 0 of an xl_btree_vacuum / xl_btree_delete record.
///
/// `block_data` layout: `ndeleted` deleted OffsetNumbers, then `nupdated`
/// updated OffsetNumbers, then `nupdated` variable-length xl_btree_update items
/// (each followed by `ndeletedtids` uint16 ptids).
///
/// # Safety
/// `block_data` must point to a buffer matching the layout above.
unsafe fn delvacuum_desc(
    buf: StringInfo,
    block_data: *mut c_char,
    ndeleted: uint16,
    nupdated: uint16,
) {
    // Output deleted page offset number array.
    appendStringInfoString(buf, c", deleted:".as_ptr());
    let deletedoffsets = block_data as *mut OffsetNumber;
    array_desc(
        buf,
        deletedoffsets as *mut c_void,
        core::mem::size_of::<OffsetNumber>(),
        ndeleted as c_int,
        offset_elem_desc,
        null_mut(),
    );

    // Output updates as an array of "update objects", each carrying a page
    // offset number from the updated array plus its posting-list ptids.
    appendStringInfoString(buf, c", updated: [".as_ptr());
    let updatedoffsets =
        block_data.add(ndeleted as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;
    let mut updates = (updatedoffsets as *mut c_char)
        .add(nupdated as usize * core::mem::size_of::<OffsetNumber>())
        as *mut xl_btree_update;

    for i in 0..nupdated as isize {
        let off = *updatedoffsets.offset(i);

        // "ptid" is the symbol name used when building each xl_btree_update's
        // array of offsets into a posting list tuple's ItemPointerData array.
        appendStringInfo!(
            buf,
            "{{ off: {}, nptids: {}, ptids: [",
            off,
            (*updates).ndeletedtids
        );
        let nptids = (*updates).ndeletedtids;
        for p in 0..nptids as isize {
            let ptid = ((updates as *mut c_char).add(SizeOfBtreeUpdate) as *mut uint16).offset(p);
            appendStringInfo!(buf, "{}", *ptid);

            if p < nptids as isize - 1 {
                appendStringInfoString(buf, c", ".as_ptr());
            }
        }
        appendStringInfoString(buf, c"] }".as_ptr());
        if i < nupdated as isize - 1 {
            appendStringInfoString(buf, c", ".as_ptr());
        }

        updates = (updates as *mut c_char)
            .add(SizeOfBtreeUpdate + nptids as usize * core::mem::size_of::<uint16>())
            as *mut xl_btree_update;
    }
    appendStringInfoChar(buf, b']' as c_char);
}

/// Render a 'T'/'F' from a bool, matching the C `(cond) ? 'T' : 'F'` idiom.
#[inline]
fn tf(b: bool) -> char {
    if b {
        'T'
    } else {
        'F'
    }
}

/// btree_desc: describe an nbtree-AM WAL record into `buf`.
///
/// # Safety
/// `record` is an opaque XLogReaderState pointer; the data pointer it yields
/// (via the stubbed XLogRecGetData) is cast to the per-opcode struct.
pub unsafe fn btree_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec: *mut c_char = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_BTREE_INSERT_LEAF
        | XLOG_BTREE_INSERT_UPPER
        | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_INSERT_POST => {
            let xlrec = rec as *const xl_btree_insert;
            appendStringInfo!(buf, "off: {}", (*xlrec).offnum);
        }
        XLOG_BTREE_SPLIT_L | XLOG_BTREE_SPLIT_R => {
            let xlrec = rec as *const xl_btree_split;
            appendStringInfo!(
                buf,
                "level: {}, firstrightoff: {}, newitemoff: {}, postingoff: {}",
                (*xlrec).level,
                (*xlrec).firstrightoff,
                (*xlrec).newitemoff,
                (*xlrec).postingoff
            );
        }
        XLOG_BTREE_DEDUP => {
            let xlrec = rec as *const xl_btree_dedup;
            appendStringInfo!(buf, "nintervals: {}", (*xlrec).nintervals);
        }
        XLOG_BTREE_VACUUM => {
            let xlrec = rec as *const xl_btree_vacuum;
            appendStringInfo!(
                buf,
                "ndeleted: {}, nupdated: {}",
                (*xlrec).ndeleted,
                (*xlrec).nupdated
            );

            if XLogRecHasBlockData(record, 0) {
                delvacuum_desc(
                    buf,
                    XLogRecGetBlockData(record, 0, null_mut()),
                    (*xlrec).ndeleted,
                    (*xlrec).nupdated,
                );
            }
        }
        XLOG_BTREE_DELETE => {
            let xlrec = rec as *const xl_btree_delete;
            appendStringInfo!(
                buf,
                "snapshotConflictHorizon: {}, ndeleted: {}, nupdated: {}, isCatalogRel: {}",
                (*xlrec).snapshotConflictHorizon,
                (*xlrec).ndeleted,
                (*xlrec).nupdated,
                tf((*xlrec).isCatalogRel)
            );

            if XLogRecHasBlockData(record, 0) {
                delvacuum_desc(
                    buf,
                    XLogRecGetBlockData(record, 0, null_mut()),
                    (*xlrec).ndeleted,
                    (*xlrec).nupdated,
                );
            }
        }
        XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            let xlrec = rec as *const xl_btree_mark_page_halfdead;
            appendStringInfo!(
                buf,
                "topparent: {}, leaf: {}, left: {}, right: {}",
                (*xlrec).topparent,
                (*xlrec).leafblk,
                (*xlrec).leftblk,
                (*xlrec).rightblk
            );
        }
        XLOG_BTREE_UNLINK_PAGE_META | XLOG_BTREE_UNLINK_PAGE => {
            let xlrec = rec as *const xl_btree_unlink_page;
            appendStringInfo!(
                buf,
                "left: {}, right: {}, level: {}, safexid: {}:{}, ",
                (*xlrec).leftsib,
                (*xlrec).rightsib,
                (*xlrec).level,
                EpochFromFullTransactionId((*xlrec).safexid),
                XidFromFullTransactionId((*xlrec).safexid)
            );
            appendStringInfo!(
                buf,
                "leafleft: {}, leafright: {}, leaftopparent: {}",
                (*xlrec).leafleftsib,
                (*xlrec).leafrightsib,
                (*xlrec).leaftopparent
            );
        }
        XLOG_BTREE_NEWROOT => {
            let xlrec = rec as *const xl_btree_newroot;
            appendStringInfo!(buf, "level: {}", (*xlrec).level);
        }
        XLOG_BTREE_REUSE_PAGE => {
            let xlrec = rec as *const xl_btree_reuse_page;
            appendStringInfo!(
                buf,
                "rel: {}/{}/{}, snapshotConflictHorizon: {}:{}, isCatalogRel: {}",
                (*xlrec).locator.spcOid,
                (*xlrec).locator.dbOid,
                (*xlrec).locator.relNumber,
                EpochFromFullTransactionId((*xlrec).snapshotConflictHorizon),
                XidFromFullTransactionId((*xlrec).snapshotConflictHorizon),
                tf((*xlrec).isCatalogRel)
            );
        }
        XLOG_BTREE_META_CLEANUP => {
            let xlrec = XLogRecGetBlockData(record, 0, null_mut()) as *const xl_btree_metadata;
            appendStringInfo!(
                buf,
                "last_cleanup_num_delpages: {}",
                (*xlrec).last_cleanup_num_delpages
            );
        }
        _ => {}
    }
}

/// btree_identify: return the name of the nbtree-AM opcode encoded in `info`.
/// Returns a NUL-terminated C string, or null for an unknown opcode (matching
/// the C `const char *id = NULL`).
pub fn btree_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        XLOG_BTREE_INSERT_LEAF => b"INSERT_LEAF\0",
        XLOG_BTREE_INSERT_UPPER => b"INSERT_UPPER\0",
        XLOG_BTREE_INSERT_META => b"INSERT_META\0",
        XLOG_BTREE_SPLIT_L => b"SPLIT_L\0",
        XLOG_BTREE_SPLIT_R => b"SPLIT_R\0",
        XLOG_BTREE_INSERT_POST => b"INSERT_POST\0",
        XLOG_BTREE_DEDUP => b"DEDUP\0",
        XLOG_BTREE_VACUUM => b"VACUUM\0",
        XLOG_BTREE_DELETE => b"DELETE\0",
        XLOG_BTREE_MARK_PAGE_HALFDEAD => b"MARK_PAGE_HALFDEAD\0",
        XLOG_BTREE_UNLINK_PAGE => b"UNLINK_PAGE\0",
        XLOG_BTREE_UNLINK_PAGE_META => b"UNLINK_PAGE_META\0",
        XLOG_BTREE_NEWROOT => b"NEWROOT\0",
        XLOG_BTREE_REUSE_PAGE => b"REUSE_PAGE\0",
        XLOG_BTREE_META_CLEANUP => b"META_CLEANUP\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    #[test]
    fn identify_insert_leaf_returns_name() {
        let p = btree_identify(XLOG_BTREE_INSERT_LEAF);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "INSERT_LEAF");
    }

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (XLOG_BTREE_INSERT_LEAF, "INSERT_LEAF"),
            (XLOG_BTREE_INSERT_UPPER, "INSERT_UPPER"),
            (XLOG_BTREE_INSERT_META, "INSERT_META"),
            (XLOG_BTREE_SPLIT_L, "SPLIT_L"),
            (XLOG_BTREE_SPLIT_R, "SPLIT_R"),
            (XLOG_BTREE_INSERT_POST, "INSERT_POST"),
            (XLOG_BTREE_DEDUP, "DEDUP"),
            (XLOG_BTREE_VACUUM, "VACUUM"),
            (XLOG_BTREE_DELETE, "DELETE"),
            (XLOG_BTREE_MARK_PAGE_HALFDEAD, "MARK_PAGE_HALFDEAD"),
            (XLOG_BTREE_UNLINK_PAGE, "UNLINK_PAGE"),
            (XLOG_BTREE_UNLINK_PAGE_META, "UNLINK_PAGE_META"),
            (XLOG_BTREE_NEWROOT, "NEWROOT"),
            (XLOG_BTREE_REUSE_PAGE, "REUSE_PAGE"),
            (XLOG_BTREE_META_CLEANUP, "META_CLEANUP"),
        ];
        for &(op, name) in cases {
            let p = btree_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should have a name", op);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name, "opcode {:#x}", op);
        }
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0xF0 has no nbtree opcode after masking off XLR_INFO_MASK.
        assert!(btree_identify(0xF0).is_null());
    }

    #[test]
    fn identify_masks_info_bits() {
        // The XLR_INFO_MASK low bits are masked off before matching.
        let p = btree_identify(XLOG_BTREE_NEWROOT | 0x0F);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "NEWROOT");
    }

    #[test]
    fn insert_struct_size() {
        // xl_btree_insert is a single OffsetNumber (uint16).
        assert_eq!(core::mem::size_of::<xl_btree_insert>(), 2);
        assert_eq!(SizeOfBtreeInsert, 2);
    }

    #[test]
    fn sizeof_macros_match_field_layout() {
        // split: level(u32) + firstrightoff(u16) + newitemoff(u16) + postingoff(u16) = 10.
        assert_eq!(SizeOfBtreeSplit, 10);
        assert_eq!(SizeOfBtreeDedup, 2); // one uint16
        assert_eq!(SizeOfBtreeUpdate, 2); // one uint16
        assert_eq!(SizeOfBtreeNewroot, 8); // BlockNumber(u32) + level(u32)
        // mark_page_halfdead: poffset(u16) pad to u32, then 4 BlockNumbers.
        assert_eq!(
            SizeOfBtreeMarkPageHalfDead,
            core::mem::offset_of!(xl_btree_mark_page_halfdead, topparent) + 4
        );
        // unlink_page ends at leaftopparent (BlockNumber).
        assert_eq!(
            SizeOfBtreeUnlinkPage,
            core::mem::offset_of!(xl_btree_unlink_page, leaftopparent) + 4
        );
    }

    #[test]
    fn reuse_page_horizon_is_full_xid() {
        // snapshotConflictHorizon is a FullTransactionId (8 bytes).
        assert_eq!(
            core::mem::size_of::<FullTransactionId>(),
            8,
            "FullTransactionId must be 64-bit"
        );
        let _ = SizeOfBtreeReusePage;
    }
}
