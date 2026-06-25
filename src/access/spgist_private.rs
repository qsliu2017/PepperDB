//! Translated from PostgreSQL src/include/access/spgist_private.h
//! Private declarations for SP-GiST access method.

use crate::access::genam::IndexOrderByDistance;
use crate::access::itup::{IndexTupleData, MaxIndexTuplesPerPage};
use crate::access::skey::ScanKey;
use crate::access::spgist::spgConfigOut;
use crate::access::tupdesc::TupleDesc;
use crate::c::{MAXALIGN, MAXALIGN_DOWN, TransactionId};
use crate::fmgr::FmgrInfo;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::pg_config::BLCKSZ;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::geo_decls::BOX;
use crate::utils::memutils::MemoryContext;
use crate::utils::rel::Relation;

/// reloption struct for SP-GiST (in-memory).
pub struct SpGistOptions {
    /// varlena header (do not touch directly!)
    pub varlena_header_: i32,
    /// page fill factor in percent (0..100)
    pub fillfactor: i32,
}

/* SPGiST leaf tuples have one key column, optionally have included columns */
pub const spgKeyColumn: i32 = 0;
pub const spgFirstIncludeColumn: i32 = 1;

/* Page numbers of fixed-location pages */
pub const SPGIST_METAPAGE_BLKNO: BlockNumber = 0; // metapage
pub const SPGIST_ROOT_BLKNO: BlockNumber = 1; // root for normal entries
pub const SPGIST_NULL_BLKNO: BlockNumber = 2; // root for null-value entries
pub const SPGIST_LAST_FIXED_BLKNO: BlockNumber = SPGIST_NULL_BLKNO;

/// C: `SpGistBlockIsRoot`.
pub const fn SpGistBlockIsRoot(blkno: BlockNumber) -> bool {
    blkno == SPGIST_ROOT_BLKNO || blkno == SPGIST_NULL_BLKNO
}

/// C: `SpGistBlockIsFixed`.
pub const fn SpGistBlockIsFixed(blkno: BlockNumber) -> bool {
    blkno <= SPGIST_LAST_FIXED_BLKNO
}

/// Contents of page special space on SPGiST index pages (on-disk).
#[repr(C)]
pub struct SpGistPageOpaqueData {
    /// see SPGIST_* page flags
    pub flags: u16,
    /// number of redirection tuples on page
    pub nRedirection: u16,
    /// number of placeholder tuples on page
    pub nPlaceholder: u16,
    /// for identification of SP-GiST indexes
    pub spgist_page_id: u16,
}

const _: () = assert!(core::mem::size_of::<SpGistPageOpaqueData>() == 8);
const _: () = assert!(core::mem::offset_of!(SpGistPageOpaqueData, spgist_page_id) == 6);

pub type SpGistPageOpaque = *mut SpGistPageOpaqueData; // TODO(ptr)

/// Flag bits in page special space. Clean single-bit set (bitflags appendix A).
pub mod spgist_page_flags {
    use bitflags::bitflags;
    bitflags! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct SpGistPageFlags: u16 {
            const META = 1 << 0;
            /// never set, but keep for backwards compatibility
            const DELETED = 1 << 1;
            const LEAF = 1 << 2;
            const NULLS = 1 << 3;
        }
    }
}
pub use spgist_page_flags::SpGistPageFlags;

// Bare const aliases (the C #defines) for use in the raw `flags` u16 word.
pub const SPGIST_META: u16 = 1 << 0;
pub const SPGIST_DELETED: u16 = 1 << 1;
pub const SPGIST_LEAF: u16 = 1 << 2;
pub const SPGIST_NULLS: u16 = 1 << 3;

impl SpGistPageOpaqueData {
    /// C: `SpGistPageIsMeta`.
    #[inline]
    pub const fn is_meta(&self) -> bool {
        self.flags & SPGIST_META != 0
    }
    /// C: `SpGistPageIsDeleted`.
    #[inline]
    pub const fn is_deleted(&self) -> bool {
        self.flags & SPGIST_DELETED != 0
    }
    /// C: `SpGistPageIsLeaf`.
    #[inline]
    pub const fn is_leaf(&self) -> bool {
        self.flags & SPGIST_LEAF != 0
    }
    /// C: `SpGistPageStoresNulls`.
    #[inline]
    pub const fn stores_nulls(&self) -> bool {
        self.flags & SPGIST_NULLS != 0
    }
}

/// Last 2 bytes on every SP-GiST page, for pg_filedump identification.
pub const SPGIST_PAGE_ID: u16 = 0xFF82;

/// Per-page last-used info, cached in index->rd_amcache (in-memory).
pub struct SpGistLastUsedPage {
    /// block number, or InvalidBlockNumber
    pub blkno: BlockNumber,
    /// page's free space (could be obsolete!)
    pub freeSpace: i32,
}

/// Note: indexes in cachedPage[] match flag assignments for SpGistGetBuffer.
pub const SPGIST_CACHED_PAGES: usize = 8;

pub struct SpGistLUPCache {
    pub cachedPage: [SpGistLastUsedPage; SPGIST_CACHED_PAGES],
}

/// metapage (on-disk: magic number + last-used-page cache).
#[repr(C)]
pub struct SpGistMetaPageData {
    /// for identity cross-check
    pub magicNumber: u32,
    /// shared storage of last-used info
    pub lastUsedPages: SpGistLUPCache,
}

pub const SPGIST_MAGIC_NUMBER: u32 = 0xBA0BABEE;

/// Per-datatype info needed in SpGistState (in-memory).
pub struct SpGistTypeDesc {
    pub r#type: Oid,
    pub attlen: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
}

/// Private state of the index AM, common to insert and search code (in-memory).
pub struct SpGistState {
    /// index we're working with
    pub index: Relation,
    /// filled in by opclass config method
    pub config: spgConfigOut,
    /// type of values to be indexed/restored
    pub attType: SpGistTypeDesc,
    /// type of leaf-tuple values
    pub attLeafType: SpGistTypeDesc,
    /// type of inner-tuple prefix values
    pub attPrefixType: SpGistTypeDesc,
    /// type of node label values
    pub attLabelType: SpGistTypeDesc,
    /// descriptor for leaf-level tuples
    pub leafTupDesc: TupleDesc,
    /// workspace for spgFormDeadTuple
    pub deadTupleStorage: *mut u8, // TODO(ptr)
    /// XID to use when creating a redirect tuple
    pub redirectXid: TransactionId,
    /// true if doing index build
    pub isBuild: bool,
}

/// Item to be re-examined later during a search (in-memory).
/// C: `distances[FLEXIBLE_ARRAY_MEMBER]` becomes a `Vec` (in-memory FAM).
pub struct SpGistSearchItem {
    /// value reconstructed from parent, or leafValue if isLeaf
    pub value: Datum,
    /// whole leaf tuple, if needed
    pub leafTuple: *mut SpGistLeafTupleData, // TODO(ptr)
    /// opclass-specific traverse value
    pub traversalValue: *mut core::ffi::c_void, // TODO(ptr): opclass-defined
    /// level of items on this page
    pub level: i32,
    /// heap info, if heap tuple
    pub heapPtr: ItemPointerData,
    /// SearchItem is NULL item
    pub isNull: bool,
    /// SearchItem is heap item
    pub isLeaf: bool,
    /// qual recheck is needed
    pub recheck: bool,
    /// distance recheck is needed
    pub recheckDistances: bool,
    /// per-orderby distances (C FAM tail)
    pub distances: Vec<f64>,
}

/// Private state of an index scan (in-memory).
pub struct SpGistScanOpaqueData {
    pub state: SpGistState,
    /// queue of to-be-visited items (C: pairingheap *). Opaque owned pointer;
    /// the comparator type isn't expressible here (matches gist_private.rs).
    pub scanQueue: *mut core::ffi::c_void, // TODO(ptr): crate::lib::pairingheap::PairingHeap
    /// short-lived memory context
    pub tempCxt: MemoryContext,
    /// single scan lifetime memory context
    pub traversalCxt: MemoryContext,
    /// scan matches (all) null entries
    pub searchNulls: bool,
    /// scan matches (some) non-null entries
    pub searchNonNulls: bool,
    /// number of index qualifier conditions
    pub numberOfKeys: i32,
    /// array of index qualifier descriptors
    pub keyData: *mut crate::access::skey::ScanKeyData, // TODO(ptr)
    /// number of ordering operators
    pub numberOfOrderBys: i32,
    /// number of ordering operators with non-NULL arguments
    pub numberOfNonNullOrderBys: i32,
    /// array of ordering op descriptors
    pub orderByData: *mut crate::access::skey::ScanKeyData, // TODO(ptr)
    /// array of ordering op return types
    pub orderByTypes: *mut Oid, // TODO(ptr)
    /// array of offsets of non-NULL ordering keys in the original array
    pub nonNullOrderByOffsets: *mut i32, // TODO(ptr)
    /// collation of index column
    pub indexCollation: Oid,
    pub innerConsistentFn: FmgrInfo,
    pub leafConsistentFn: FmgrInfo,
    pub zeroDistances: *mut f64, // TODO(ptr)
    pub infDistances: *mut f64,  // TODO(ptr)
    /// bitmap being filled (amgetbitmap scans only)
    pub tbm: *mut TIDBitmap, // TODO(ptr)
    /// number of TIDs passed to bitmap
    pub ntids: i64,
    /// are we reconstructing tuples? (amgettuple scans only)
    pub want_itup: bool,
    /// if so, descriptor for reconstructed tuples
    pub reconTupDesc: TupleDesc,
    /// number of TIDs found on current page
    pub nPtrs: i32,
    /// index for scanning through same
    pub iPtr: i32,
    /// TIDs from cur page
    pub heapPtrs: [ItemPointerData; MaxIndexTuplesPerPage],
    /// their recheck flags
    pub recheck: [bool; MaxIndexTuplesPerPage],
    /// distance recheck flags
    pub recheckDistances: [bool; MaxIndexTuplesPerPage],
    /// reconstructed tuples
    pub reconTups: [crate::access::htup::HeapTuple; MaxIndexTuplesPerPage],
    /// distances (for recheck)
    pub distances: [*mut IndexOrderByDistance; MaxIndexTuplesPerPage], // TODO(ptr)
}

pub type SpGistScanOpaque = *mut SpGistScanOpaqueData; // TODO(ptr)

/// What we actually keep in index->rd_amcache (in-memory).
pub struct SpGistCache {
    pub config: spgConfigOut,
    pub attType: SpGistTypeDesc,
    pub attLeafType: SpGistTypeDesc,
    pub attPrefixType: SpGistTypeDesc,
    pub attLabelType: SpGistTypeDesc,
    pub lastUsedPages: SpGistLUPCache,
}

/* values of tupstate (see README for more info) */
pub const SPGIST_LIVE: u32 = 0; // normal live tuple (either inner or leaf)
pub const SPGIST_REDIRECT: u32 = 1; // temporary redirection placeholder
pub const SPGIST_DEAD: u32 = 2; // dead, cannot be removed because of links
pub const SPGIST_PLACEHOLDER: u32 = 3; // placeholder, used to preserve offsets

/// SPGiST inner tuple header (on-disk). The first C word bit-packs
/// tupstate:2 | allTheSame:1 | nNodes:13 | prefixSize:16; stored as a raw u32
/// with accessor methods (bitfield word; see translation-rules.md).
#[repr(C)]
pub struct SpGistInnerTupleData {
    /// tupstate:2 | allTheSame:1 | nNodes:13 | prefixSize:16
    pub t_bits: u32,
    /// total size of inner tuple
    pub size: u16,
    /* a couple of wasted bytes here on most machines; prefix datum then nodes follow */
}

const _: () = assert!(core::mem::offset_of!(SpGistInnerTupleData, size) == 4);

impl SpGistInnerTupleData {
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.t_bits & 0x3
    }
    #[inline]
    pub const fn set_tupstate(&mut self, v: u32) {
        self.t_bits = (self.t_bits & !0x3) | (v & 0x3);
    }
    #[inline]
    pub const fn all_the_same(&self) -> bool {
        (self.t_bits >> 2) & 0x1 != 0
    }
    #[inline]
    pub const fn set_all_the_same(&mut self, v: bool) {
        self.t_bits = (self.t_bits & !(0x1 << 2)) | ((v as u32) << 2);
    }
    #[inline]
    pub const fn n_nodes(&self) -> u32 {
        (self.t_bits >> 3) & 0x1FFF
    }
    #[inline]
    pub const fn set_n_nodes(&mut self, v: u32) {
        self.t_bits = (self.t_bits & !(0x1FFF << 3)) | ((v & 0x1FFF) << 3);
    }
    #[inline]
    pub const fn prefix_size(&self) -> u32 {
        (self.t_bits >> 16) & 0xFFFF
    }
    #[inline]
    pub const fn set_prefix_size(&mut self, v: u32) {
        self.t_bits = (self.t_bits & !(0xFFFF << 16)) | ((v & 0xFFFF) << 16);
    }
}

pub type SpGistInnerTuple = *mut SpGistInnerTupleData; // TODO(ptr)

/* these must match largest values that fit in bit fields declared above */
pub const SGITMAXNNODES: u32 = 0x1FFF;
pub const SGITMAXPREFIXSIZE: u32 = 0xFFFF;
pub const SGITMAXSIZE: u32 = 0xFFFF;

/// C: `SGITHDRSZ = MAXALIGN(sizeof(SpGistInnerTupleData))`.
pub const SGITHDRSZ: usize = MAXALIGN(core::mem::size_of::<SpGistInnerTupleData>());

/// C: `_SGITDATA(x) = (char*)x + SGITHDRSZ`.
pub fn _SGITDATA(_x: SpGistInnerTuple) -> *mut u8 {
    unimplemented!()
}

/// C: `SGITDATAPTR(x)` - prefix data ptr, or NULL if no prefix.
pub fn SGITDATAPTR(_x: &SpGistInnerTupleData) -> *mut u8 {
    unimplemented!()
}

/// C: `SGITDATUM(x, s)` - prefix datum (Datum form if pass-by-value).
pub fn SGITDATUM(_x: &SpGistInnerTupleData, _s: &SpGistState) -> Datum {
    unimplemented!()
}

/// C: `SGITNODEPTR(x)` - pointer to first node, past the prefix.
pub fn SGITNODEPTR(_x: &SpGistInnerTupleData) -> SpGistNodeTuple {
    unimplemented!()
}

/// SPGiST node tuple: one node within an inner tuple (on-disk; reuses
/// IndexTupleData, no null bitmap). C: `typedef IndexTupleData SpGistNodeTupleData`.
pub type SpGistNodeTupleData = IndexTupleData;
pub type SpGistNodeTuple = *mut SpGistNodeTupleData; // TODO(ptr)

/// C: `SGNTHDRSZ = MAXALIGN(sizeof(SpGistNodeTupleData))`.
pub const SGNTHDRSZ: usize = MAXALIGN(core::mem::size_of::<SpGistNodeTupleData>());

/// C: `SGNTDATAPTR(x) = (char*)x + SGNTHDRSZ`.
pub fn SGNTDATAPTR(_x: SpGistNodeTuple) -> *mut u8 {
    unimplemented!()
}

/// C: `SGNTDATUM(x, s)` - node label datum.
pub fn SGNTDATUM(_x: SpGistNodeTuple, _s: &SpGistState) -> Datum {
    unimplemented!()
}

/// SPGiST leaf tuple header (on-disk). First C word bit-packs tupstate:2 |
/// size:30; stored as a raw u32 with accessors. `t_info` packs nextOffset
/// (14 bits) plus a has-nulls flag bit, so it stays a raw u16 with accessors.
#[repr(C)]
pub struct SpGistLeafTupleData {
    /// tupstate:2 | size:30
    pub t_bits: u32,
    /// nextOffset (bits 0-13) | hasnullmask (bit 15)
    pub t_info: u16,
    /// TID of represented heap tuple
    pub heapPtr: ItemPointerData,
    /* nulls bitmap (if flag set), then leaf datum + included datums follow */
}

const _: () = assert!(core::mem::offset_of!(SpGistLeafTupleData, t_info) == 4);
const _: () = assert!(core::mem::offset_of!(SpGistLeafTupleData, heapPtr) == 6);

impl SpGistLeafTupleData {
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.t_bits & 0x3
    }
    #[inline]
    pub const fn set_tupstate(&mut self, v: u32) {
        self.t_bits = (self.t_bits & !0x3) | (v & 0x3);
    }
    #[inline]
    pub const fn size(&self) -> u32 {
        self.t_bits >> 2
    }
    #[inline]
    pub const fn set_size(&mut self, v: u32) {
        self.t_bits = (self.t_bits & 0x3) | (v << 2);
    }
    /// C: `SGLT_GET_NEXTOFFSET`.
    #[inline]
    pub const fn get_nextoffset(&self) -> OffsetNumber {
        self.t_info & 0x3FFF
    }
    /// C: `SGLT_SET_NEXTOFFSET`.
    #[inline]
    pub const fn set_nextoffset(&mut self, offset_number: OffsetNumber) {
        self.t_info = (self.t_info & 0xC000) | (offset_number & 0x3FFF);
    }
    /// C: `SGLT_GET_HASNULLMASK`.
    #[inline]
    pub const fn get_hasnullmask(&self) -> bool {
        self.t_info & 0x8000 != 0
    }
    /// C: `SGLT_SET_HASNULLMASK`.
    #[inline]
    pub const fn set_hasnullmask(&mut self, hasnulls: bool) {
        self.t_info = (self.t_info & 0x7FFF) | if hasnulls { 0x8000 } else { 0 };
    }
}

pub type SpGistLeafTuple = *mut SpGistLeafTupleData;

/// C: `SGLTHDRSZ(hasnulls)` - leaf header size, with or without nulls bitmap.
pub const fn SGLTHDRSZ(hasnulls: bool) -> usize {
    if hasnulls {
        MAXALIGN(
            core::mem::size_of::<SpGistLeafTupleData>()
                + core::mem::size_of::<crate::access::itup::IndexAttributeBitMapData>(),
        )
    } else {
        MAXALIGN(core::mem::size_of::<SpGistLeafTupleData>())
    }
}

/// C: `SGLTDATAPTR(x) = (char*)x + SGLTHDRSZ(hasnulls)`.
pub fn SGLTDATAPTR(_x: SpGistLeafTuple) -> *mut u8 {
    unimplemented!()
}

/// C: `SGLTDATUM(x, s)` - leaf datum via fetch_att.
pub fn SGLTDATUM(_x: SpGistLeafTuple, _s: &SpGistState) -> Datum {
    unimplemented!()
}

/// SPGiST dead tuple header (on-disk). Same tupstate:2 | size:30 word and
/// t_info as a leaf tuple; pointer field aligns with leaf heapPtr.
#[repr(C)]
pub struct SpGistDeadTupleData {
    /// tupstate:2 | size:30
    pub t_bits: u32,
    /// not used in dead tuples
    pub t_info: u16,
    /// redirection inside index
    pub pointer: ItemPointerData,
    /// ID of xact that inserted this tuple
    pub xid: TransactionId,
}

const _: () = assert!(core::mem::offset_of!(SpGistDeadTupleData, t_info) == 4);
const _: () = assert!(core::mem::offset_of!(SpGistDeadTupleData, pointer) == 6);

impl SpGistDeadTupleData {
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.t_bits & 0x3
    }
    #[inline]
    pub const fn set_tupstate(&mut self, v: u32) {
        self.t_bits = (self.t_bits & !0x3) | (v & 0x3);
    }
    #[inline]
    pub const fn size(&self) -> u32 {
        self.t_bits >> 2
    }
    #[inline]
    pub const fn set_size(&mut self, v: u32) {
        self.t_bits = (self.t_bits & 0x3) | (v << 2);
    }
}

pub type SpGistDeadTuple = *mut SpGistDeadTupleData; // TODO(ptr)

/// C: `SGDTSIZE = MAXALIGN(sizeof(SpGistDeadTupleData))`.
pub const SGDTSIZE: usize = MAXALIGN(core::mem::size_of::<SpGistDeadTupleData>());

/// Page capacity after allowing for fixed header and special space.
pub const SPGIST_PAGE_CAPACITY: usize = MAXALIGN_DOWN(
    BLCKSZ as usize - SizeOfPageHeaderData - MAXALIGN(core::mem::size_of::<SpGistPageOpaqueData>()),
);

/// C: `SpGistPageGetFreeSpace(p, n)` - free space, recycling up to n placeholders.
pub fn SpGistPageGetFreeSpace(_p: &Page, _n: i32) -> usize {
    unimplemented!()
}

/// C: `STORE_STATE(s, d)` - copy redirectXid/isBuild into an XLOG state struct.
pub fn STORE_STATE(s: &SpGistState, redirect_xid: &mut TransactionId, is_build: &mut bool) {
    *redirect_xid = s.redirectXid;
    *is_build = s.isBuild;
}

/*
 * The "flags" argument for SpGistGetBuffer: GBUF_LEAF, or
 * GBUF_INNER_PARITY(blockNumber); GBUF_NULLS may be OR'd in. GBUF_* packs a
 * 2-bit parity field beside the NULLS flag (bitflags appendix D: POOR),
 * so it stays a raw int with accessor functions.
 */
pub const GBUF_LEAF: i32 = 0x03;
pub const GBUF_NULLS: i32 = 0x04;
pub const GBUF_PARITY_MASK: i32 = 0x03;

/// C: `GBUF_INNER_PARITY(x) = x % 3`.
pub const fn GBUF_INNER_PARITY(x: BlockNumber) -> i32 {
    (x % 3) as i32
}

/// C: `GBUF_REQ_LEAF(flags)`.
pub const fn GBUF_REQ_LEAF(flags: i32) -> bool {
    (flags & GBUF_PARITY_MASK) == GBUF_LEAF
}

/// C: `GBUF_REQ_NULLS(flags)`.
pub const fn GBUF_REQ_NULLS(flags: i32) -> bool {
    (flags & GBUF_NULLS) != 0
}

/* reloption parameters */
pub const SPGIST_MIN_FILLFACTOR: i32 = 10;
pub const SPGIST_DEFAULT_FILLFACTOR: i32 = 80;

/* spgutils.c */

pub fn spgGetCache(_index: Relation) -> *mut SpGistCache {
    unimplemented!()
}

pub fn getSpGistTupleDesc(_index: Relation, _key_type: &mut SpGistTypeDesc) -> TupleDesc {
    unimplemented!()
}

pub fn initSpGistState(_state: &mut SpGistState, _index: Relation) {
    unimplemented!()
}

pub fn SpGistNewBuffer(_index: Relation) -> Buffer {
    unimplemented!()
}

pub fn SpGistUpdateMetaPage(_index: Relation) {
    unimplemented!()
}

/// Returns the buffer; `*isNew` out-param folded into the tuple.
pub fn SpGistGetBuffer(_index: Relation, _flags: i32, _need_space: i32) -> (Buffer, bool) {
    unimplemented!()
}

pub fn SpGistSetLastUsedPage(_index: Relation, _buffer: Buffer) {
    unimplemented!()
}

pub fn SpGistInitPage(_page: &mut Page, _f: u16) {
    unimplemented!()
}

pub fn SpGistInitBuffer(_b: Buffer, _f: u16) {
    unimplemented!()
}

pub fn SpGistInitMetapage(_page: &mut Page) {
    unimplemented!()
}

pub fn SpGistGetInnerTypeSize(_att: &SpGistTypeDesc, _datum: Datum) -> u32 {
    unimplemented!()
}

pub fn SpGistGetLeafTupleSize(
    _tuple_descriptor: TupleDesc,
    _datums: &[Datum],
    _isnulls: &[bool],
) -> usize {
    unimplemented!()
}

pub fn spgFormLeafTuple(
    _state: &mut SpGistState,
    _heap_ptr: &ItemPointerData,
    _datums: &[Datum],
    _isnulls: &[bool],
) -> SpGistLeafTuple {
    unimplemented!()
}

pub fn spgFormNodeTuple(_state: &mut SpGistState, _label: Datum, _isnull: bool) -> SpGistNodeTuple {
    unimplemented!()
}

pub fn spgFormInnerTuple(
    _state: &mut SpGistState,
    _has_prefix: bool,
    _prefix: Datum,
    _n_nodes: i32,
    _nodes: &mut [SpGistNodeTuple],
) -> SpGistInnerTuple {
    unimplemented!()
}

pub fn spgFormDeadTuple(
    _state: &mut SpGistState,
    _tupstate: i32,
    _blkno: BlockNumber,
    _offnum: OffsetNumber,
) -> SpGistDeadTuple {
    unimplemented!()
}

pub fn spgDeformLeafTuple(
    _tup: SpGistLeafTuple,
    _tuple_descriptor: TupleDesc,
    _datums: &mut [Datum],
    _isnulls: &mut [bool],
    _key_column_is_null: bool,
) {
    unimplemented!()
}

pub fn spgExtractNodeLabels(_state: &mut SpGistState, _inner_tuple: SpGistInnerTuple) -> *mut Datum {
    unimplemented!()
}

/// Returns the offset where the item was placed; `*startOffset` is in/out.
pub fn SpGistPageAddNewItem(
    _state: &mut SpGistState,
    _page: &mut Page,
    _item: *mut u8,
    _size: usize,
    _start_offset: &mut OffsetNumber,
    _error_ok: bool,
) -> OffsetNumber {
    unimplemented!()
}

/// `*res`/`*isnull` out-params folded into the return; status -> Option.
pub fn spgproperty(
    _index_oid: Oid,
    _attno: i32,
    _prop: crate::access::amapi::IndexAMProperty,
    _propname: &str,
) -> Option<(bool, bool)> {
    unimplemented!()
}

/* spgdoinsert.c */

pub fn spgUpdateNodeLink(
    _tup: SpGistInnerTuple,
    _node_n: i32,
    _blkno: BlockNumber,
    _offset: OffsetNumber,
) {
    unimplemented!()
}

pub fn spgPageIndexMultiDelete(
    _state: &mut SpGistState,
    _page: &mut Page,
    _itemnos: &[OffsetNumber],
    _nitems: i32,
    _firststate: i32,
    _reststate: i32,
    _blkno: BlockNumber,
    _offnum: OffsetNumber,
) {
    unimplemented!()
}

pub fn spgdoinsert(
    _index: Relation,
    _state: &mut SpGistState,
    _heap_ptr: &ItemPointerData,
    _datums: &mut [Datum],
    _isnulls: &mut [bool],
) -> bool {
    unimplemented!()
}

/* spgproc.c */

pub fn spg_key_orderbys_distances(
    _key: Datum,
    _is_leaf: bool,
    _orderbys: ScanKey,
    _norderbys: i32,
) -> *mut f64 {
    unimplemented!()
}

pub fn box_copy(_orig: &BOX) -> *mut BOX {
    unimplemented!()
}
