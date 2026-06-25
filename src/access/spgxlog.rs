//! Translated from PostgreSQL src/include/access/spgxlog.h

use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;

// XLOG record types for SP-GiST (info nibble): raw consts. 0x00 not used.
pub const XLOG_SPGIST_ADD_LEAF: u8 = 0x10;
pub const XLOG_SPGIST_MOVE_LEAFS: u8 = 0x20;
pub const XLOG_SPGIST_ADD_NODE: u8 = 0x30;
pub const XLOG_SPGIST_SPLIT_TUPLE: u8 = 0x40;
pub const XLOG_SPGIST_PICKSPLIT: u8 = 0x50;
pub const XLOG_SPGIST_VACUUM_LEAF: u8 = 0x60;
pub const XLOG_SPGIST_VACUUM_ROOT: u8 = 0x70;
pub const XLOG_SPGIST_VACUUM_REDIRECT: u8 = 0x80;

/// Minimal SpGistState carried in xlog records (see fillFakeState).
#[repr(C)]
pub struct spgxlogState {
    pub redirectXid: TransactionId,
    pub isBuild: bool,
}

/// Backup blk 0: dest leaf page; blk 1: parent page. New leaf tuple follows.
#[repr(C)]
pub struct spgxlogAddLeaf {
    pub newPage: bool,                 // init dest page?
    pub storesNulls: bool,             // page is in the nulls tree?
    pub offnumLeaf: OffsetNumber,      // where leaf tuple gets placed
    pub offnumHeadLeaf: OffsetNumber,  // head tuple in chain, if any
    pub offnumParent: OffsetNumber,    // where the parent downlink is, if any
    pub nodeI: u16,
}

/// FAM `offsets`: deleted/inserted tuple numbers then leaf tuples (unaligned).
#[repr(C)]
pub struct spgxlogMoveLeafs {
    pub nMoves: u16,                // tuples moved from source page
    pub newPage: bool,             // init dest page?
    pub replaceDead: bool,         // replacing a DEAD source tuple?
    pub storesNulls: bool,         // pages are in the nulls tree?
    pub offnumParent: OffsetNumber, // where the parent downlink is
    pub nodeI: u16,
    pub stateSrc: spgxlogState,
    // FAM: offsets: [OffsetNumber]
}
pub const SizeOfSpgxlogMoveLeafs: usize = core::mem::size_of::<spgxlogMoveLeafs>();

/// Add node. Updated inner tuple follows (unaligned).
#[repr(C)]
pub struct spgxlogAddNode {
    pub offnum: OffsetNumber,    // original inner tuple offset (blk 0)
    pub offnumNew: OffsetNumber, // new tuple offset (blk 1), invalid if overwrote
    pub newPage: bool,           // init new page?
    /// parentBlk: 0=orig page, 1=new page, 2=blk ref 2, -1=parent not updated
    pub parentBlk: i8,
    pub offnumParent: OffsetNumber, // offset within the parent page
    pub nodeI: u16,
    pub stateSrc: spgxlogState,
}

/// Split tuple. New prefix then postfix inner tuples follow (unaligned).
#[repr(C)]
pub struct spgxlogSplitTuple {
    pub offnumPrefix: OffsetNumber,  // where the prefix tuple goes
    pub offnumPostfix: OffsetNumber, // where the postfix tuple goes
    pub newPage: bool,               // need to init that page?
    pub postfixBlkSame: bool,        // postfix put on same page as prefix?
}

/// Pick split. FAM `offsets`: deleted/inserted numbers, selector bytes, tuples.
#[repr(C)]
pub struct spgxlogPickSplit {
    pub isRootSplit: bool,
    pub nDelete: u16,             // n to delete from Src
    pub nInsert: u16,             // n to insert on Src and/or Dest
    pub initSrc: bool,            // re-init the Src page?
    pub initDest: bool,           // re-init the Dest page?
    pub offnumInner: OffsetNumber, // where to put new inner tuple
    pub initInner: bool,          // re-init the Inner page?
    pub storesNulls: bool,        // pages are in the nulls tree?
    pub innerIsParent: bool,      // is parent the same as inner page?
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
    pub stateSrc: spgxlogState,
    // FAM: offsets: [OffsetNumber]
}
pub const SizeOfSpgxlogPickSplit: usize = core::mem::size_of::<spgxlogPickSplit>();

/// Vacuum leaf. FAM `offsets` lists tuple numbers per the data layout.
#[repr(C)]
pub struct spgxlogVacuumLeaf {
    pub nDead: u16,        // tuples to become DEAD
    pub nPlaceholder: u16, // tuples to become PLACEHOLDER
    pub nMove: u16,        // tuples to move
    pub nChain: u16,       // tuples to re-chain
    pub stateSrc: spgxlogState,
    // FAM: offsets: [OffsetNumber]
}
pub const SizeOfSpgxlogVacuumLeaf: usize = core::mem::size_of::<spgxlogVacuumLeaf>();

/// Vacuum a root page that is also a leaf. FAM `offsets`: tuples to delete.
#[repr(C)]
pub struct spgxlogVacuumRoot {
    pub nDelete: u16, // tuples to delete
    pub stateSrc: spgxlogState,
    // FAM: offsets: [OffsetNumber]
}
pub const SizeOfSpgxlogVacuumRoot: usize = core::mem::size_of::<spgxlogVacuumRoot>();

/// Vacuum redirect. FAM `offsets`: redirect tuples to make placeholders.
#[repr(C)]
pub struct spgxlogVacuumRedirect {
    pub nToPlaceholder: u16,         // redirects to make placeholders
    pub firstPlaceholder: OffsetNumber, // first placeholder tuple to remove
    pub snapshotConflictHorizon: TransactionId, // newest XID of removed redirects
    pub isCatalogRel: bool,          // recovery conflict during logical decoding
    // FAM: offsets: [OffsetNumber]
}
pub const SizeOfSpgxlogVacuumRedirect: usize = core::mem::size_of::<spgxlogVacuumRedirect>();

pub fn spg_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn spg_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn spg_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn spg_xlog_startup() {
    unimplemented!()
}
pub fn spg_xlog_cleanup() {
    unimplemented!()
}
pub fn spg_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
