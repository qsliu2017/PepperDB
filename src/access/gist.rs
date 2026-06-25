//! Translated from PostgreSQL src/include/access/gist.h

use bitflags::bitflags;

use crate::access::cmptype::CompareType;
use crate::access::stratnum::StrategyNumber;
use crate::access::transam::{
    full_transaction_id_from_epoch_and_xid, FullTransactionId, FIRST_NORMAL_TRANSACTION_ID,
};
use crate::access::xlogdefs::XLogRecPtr;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, PageXLogRecPtr};
use crate::storage::off::OffsetNumber;
use crate::utils::relcache::Relation;

// amproc indexes for GiST indexes.
pub const GIST_CONSISTENT_PROC: i32 = 1;
pub const GIST_UNION_PROC: i32 = 2;
pub const GIST_COMPRESS_PROC: i32 = 3;
pub const GIST_DECOMPRESS_PROC: i32 = 4;
pub const GIST_PENALTY_PROC: i32 = 5;
pub const GIST_PICKSPLIT_PROC: i32 = 6;
pub const GIST_EQUAL_PROC: i32 = 7;
pub const GIST_DISTANCE_PROC: i32 = 8;
pub const GIST_FETCH_PROC: i32 = 9;
pub const GIST_OPTIONS_PROC: i32 = 10;
pub const GIST_SORTSUPPORT_PROC: i32 = 11;
pub const GIST_TRANSLATE_CMPTYPE_PROC: i32 = 12;
pub const GISTNProcs: i32 = 12;

bitflags! {
    /// Page opaque flags in a GiST index page (single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GistPageFlags: u16 {
        const F_LEAF           = 1 << 0; // leaf page
        const F_DELETED        = 1 << 1; // the page has been deleted
        const F_TUPLES_DELETED = 1 << 2; // some tuples on the page were deleted
        const F_FOLLOW_RIGHT   = 1 << 3; // page to the right has no downlink
        const F_HAS_GARBAGE    = 1 << 4; // some tuples are dead, not deleted yet
    }
}

pub const F_LEAF: u16 = 1 << 0;
pub const F_DELETED: u16 = 1 << 1;
pub const F_TUPLES_DELETED: u16 = 1 << 2;
pub const F_FOLLOW_RIGHT: u16 = 1 << 3;
pub const F_HAS_GARBAGE: u16 = 1 << 4;

/// NSN (node sequence number): a special-purpose LSN stored on each index page,
/// updated only during page splits.
pub type GistNSN = XLogRecPtr;

/// Fake LSN/NSN used during index builds; smaller than any real/fake LSN after.
pub const GistBuildLSN: XLogRecPtr = XLogRecPtr(1);

/// On-disk NSN: two 32-bit fields, same as LSNs (pre-9.3 compatibility).
pub type PageGistNSN = PageXLogRecPtr;

#[repr(C)]
pub struct GISTPageOpaqueData {
    pub nsn: PageGistNSN,         // this value must change on page split
    pub rightlink: BlockNumber,   // next page if any
    pub flags: u16,               // see GistPageFlags
    pub gist_page_id: u16,        // for identification of GiST indexes
}

pub type GISTPageOpaque = *mut GISTPageOpaqueData; // TODO(ptr)

/// Page ID for pg_filedump etc.; should be the last 2 bytes on the page.
pub const GIST_PAGE_ID: u16 = 0xFF81;

/// Split Vector returned by the PickSplit method (in-memory method state).
pub struct GIST_SPLITVEC {
    pub left: Vec<OffsetNumber>,   // entries that go left
    pub ldatum: Datum,             // union of keys in left
    pub ldatum_exists: bool,       // true if ldatum already exists
    pub right: Vec<OffsetNumber>,  // entries that go right
    pub rdatum: Datum,             // union of keys in right
    pub rdatum_exists: bool,       // true if rdatum already exists
}

/// An entry on a GiST node: key plus its own location (rel,page,offset).
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: Relation,
    pub page: *mut u8, // Page; TODO(ptr)
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

/// On a deleted page, stored right after the standard page header.
#[repr(C)]
pub struct GISTDeletedPageContents {
    /// last xid which could see the page in a scan
    pub deleteXid: FullTransactionId,
}

pub fn GistPageSetDeleted(_page: Page, _deletexid: FullTransactionId) {
    unimplemented!()
}

pub fn GistPageGetDeleteXid(_page: Page) -> FullTransactionId {
    // Fallback when the deleteXid field isn't present.
    let _ = full_transaction_id_from_epoch_and_xid(0, FIRST_NORMAL_TRANSACTION_ID);
    unimplemented!()
}

/// Vector of GISTENTRY structs passed to user-defined union/picksplit methods.
pub struct GistEntryVector {
    pub vector: Vec<GISTENTRY>, // n folds into Vec::len
}

/// Initialize a GISTENTRY.
pub fn gistentryinit(
    e: &mut GISTENTRY,
    k: Datum,
    r: Relation,
    pg: *mut u8,
    o: OffsetNumber,
    l: bool,
) {
    e.key = k;
    e.rel = r;
    e.page = pg;
    e.offset = o;
    e.leafkey = l;
}

pub fn gisttranslatecmptype(_cmptype: CompareType, _opfamily: Oid) -> StrategyNumber {
    unimplemented!()
}
