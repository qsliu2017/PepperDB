//! access/spgist_private.h - Private declarations for SP-GiST access method.

use crate::prelude::*;
use crate::access::common::indextuple::{
    IndexAttributeBitMapData, IndexTupleData,
};
use crate::access::common::scankey::ScanKey;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::index::amapi::IndexAMProperty;
use crate::access::spgist::spgproc::BOX;
use crate::lib::pairingheap::{pairingheap, pairingheap_node};
use crate::nodes::tidbitmap::TIDBitmap;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::Page;
use crate::storage::item::Item;
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::off::OffsetNumber;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::rel::Relation;

// ===========================================================================
// spgConfigOut comes from access/spgist.h, which is NOT ported.  We define a
// faithful #[repr(C)] mirror here.  (The sibling spgist opclass files each
// keep their own minimal local mirror; this is the full struct.)
// TODO: dedup once access/spgist.h is ported.
// ===========================================================================
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgConfigOut {
    pub prefixType: Oid,    /* Data type of inner-tuple prefixes */
    pub labelType: Oid,     /* Data type of inner-tuple node labels */
    pub leafType: Oid,      /* Data type of leaf-tuple values */
    pub canReturnData: bool, /* Opclass can reconstruct original data */
    pub longValuesOK: bool, /* Opclass can cope with values > 1 page */
}

// ===========================================================================
// IndexOrderByDistance comes from access/genam.h (NOT ported here).  Minimal
// local stub.
// TODO: dedup once access/genam.h is ported.
// ===========================================================================
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexOrderByDistance {
    pub value: f64,
    pub isnull: bool,
}

// ===========================================================================
// MaxIndexTuplesPerPage comes from access/itup.h (not exported there yet).
// Local stub matching the C definition:
//   (int) ((BLCKSZ - SizeOfPageHeaderData) /
//          (MAXALIGN(sizeof(IndexTupleData) + 1) + sizeof(ItemIdData)))
// TODO: dedup once access/itup.h exports it.
// ===========================================================================
pub const MaxIndexTuplesPerPage: usize = 407;

pub type SpGistPageOpaque = *mut SpGistPageOpaqueData;

#[repr(C)]
pub struct SpGistOptions {
    pub varlena_header_: int32, /* varlena header (do not touch directly!) */
    pub fillfactor: c_int,      /* page fill factor in percent (0..100) */
}

/* SPGiST leaf tuples have one key column, optionally have included columns */
pub const spgKeyColumn: c_int = 0;
pub const spgFirstIncludeColumn: c_int = 1;

/* Page numbers of fixed-location pages */
pub const SPGIST_METAPAGE_BLKNO: BlockNumber = 0; /* metapage */
pub const SPGIST_ROOT_BLKNO: BlockNumber = 1; /* root for normal entries */
pub const SPGIST_NULL_BLKNO: BlockNumber = 2; /* root for null-value entries */
pub const SPGIST_LAST_FIXED_BLKNO: BlockNumber = SPGIST_NULL_BLKNO;

#[inline]
pub fn SpGistBlockIsRoot(blkno: BlockNumber) -> bool {
    blkno == SPGIST_ROOT_BLKNO || blkno == SPGIST_NULL_BLKNO
}

#[inline]
pub fn SpGistBlockIsFixed(blkno: BlockNumber) -> bool {
    blkno <= SPGIST_LAST_FIXED_BLKNO
}

/*
 * Contents of page special space on SPGiST index pages
 */
#[repr(C)]
pub struct SpGistPageOpaqueData {
    pub flags: uint16,          /* see bit definitions below */
    pub nRedirection: uint16,   /* number of redirection tuples on page */
    pub nPlaceholder: uint16,   /* number of placeholder tuples on page */
    /* note there's no count of either LIVE or DEAD tuples ... */
    pub spgist_page_id: uint16, /* for identification of SP-GiST indexes */
}

/* Flag bits in page special space */
pub const SPGIST_META: c_int = 1 << 0;
pub const SPGIST_DELETED: c_int = 1 << 1; /* never set, but keep for backwards compatibility */
pub const SPGIST_LEAF: c_int = 1 << 2;
pub const SPGIST_NULLS: c_int = 1 << 3;

/*
 * The page ID is for the convenience of pg_filedump and similar utilities.
 */
pub const SPGIST_PAGE_ID: c_int = 0xFF82;

/*
 * Each backend keeps a cache of last-used page info in its index->rd_amcache
 * area.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SpGistLastUsedPage {
    pub blkno: BlockNumber, /* block number, or InvalidBlockNumber */
    pub freeSpace: c_int,   /* page's free space (could be obsolete!) */
}

/* Note: indexes in cachedPage[] match flag assignments for SpGistGetBuffer */
pub const SPGIST_CACHED_PAGES: usize = 8;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SpGistLUPCache {
    pub cachedPage: [SpGistLastUsedPage; SPGIST_CACHED_PAGES],
}

/*
 * metapage
 */
#[repr(C)]
pub struct SpGistMetaPageData {
    pub magicNumber: uint32,           /* for identity cross-check */
    pub lastUsedPages: SpGistLUPCache, /* shared storage of last-used info */
}

pub const SPGIST_MAGIC_NUMBER: uint32 = 0xBA0BABEE;

/*
 * Private state of index AM.  SpGistState is common to both insert and
 * search code; SpGistScanOpaque is for searches only.
 */

pub type SpGistLeafTuple = *mut SpGistLeafTupleData; /* forward reference */

/* Per-datatype info needed in SpGistState */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SpGistTypeDesc {
    pub type_: Oid,
    pub attlen: int16,
    pub attbyval: bool,
    pub attalign: c_char,
    pub attstorage: c_char,
}

#[repr(C)]
pub struct SpGistState {
    pub index: Relation, /* index we're working with */

    pub config: spgConfigOut, /* filled in by opclass config method */

    pub attType: SpGistTypeDesc,       /* type of values to be indexed/restored */
    pub attLeafType: SpGistTypeDesc,   /* type of leaf-tuple values */
    pub attPrefixType: SpGistTypeDesc, /* type of inner-tuple prefix values */
    pub attLabelType: SpGistTypeDesc,  /* type of node label values */

    /* leafTupDesc typically points to index's tupdesc, but not always */
    pub leafTupDesc: TupleDesc, /* descriptor for leaf-level tuples */

    pub deadTupleStorage: *mut c_char, /* workspace for spgFormDeadTuple */

    pub redirectXid: TransactionId, /* XID to use when creating a redirect tuple */
    pub isBuild: bool,              /* true if doing index build */
}

/* Item to be re-examined later during a search */
#[repr(C)]
pub struct SpGistSearchItem {
    pub phNode: pairingheap_node, /* pairing heap node */
    pub value: Datum,             /* value reconstructed from parent, or leafValue if isLeaf */
    pub leafTuple: SpGistLeafTuple, /* whole leaf tuple, if needed */
    pub traversalValue: *mut c_void, /* opclass-specific traverse value */
    pub level: c_int,             /* level of items on this page */
    pub heapPtr: ItemPointerData, /* heap info, if heap tuple */
    pub isNull: bool,             /* SearchItem is NULL item */
    pub isLeaf: bool,             /* SearchItem is heap item */
    pub recheck: bool,            /* qual recheck is needed */
    pub recheckDistances: bool,   /* distance recheck is needed */

    /* array with numberOfOrderBys entries */
    pub distances: [f64; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * Private state of an index scan
 */
#[repr(C)]
pub struct SpGistScanOpaqueData {
    pub state: SpGistState,      /* see above */
    pub scanQueue: *mut pairingheap, /* queue of to be visited items */
    pub tempCxt: MemoryContext,  /* short-lived memory context */
    pub traversalCxt: MemoryContext, /* single scan lifetime memory context */

    /* Control flags showing whether to search nulls and/or non-nulls */
    pub searchNulls: bool,    /* scan matches (all) null entries */
    pub searchNonNulls: bool, /* scan matches (some) non-null entries */

    /* Index quals to be passed to opclass (null-related quals removed) */
    pub numberOfKeys: c_int, /* number of index qualifier conditions */
    pub keyData: ScanKey,    /* array of index qualifier descriptors */
    pub numberOfOrderBys: c_int, /* number of ordering operators */
    pub numberOfNonNullOrderBys: c_int, /* number of ordering operators with non-NULL arguments */
    pub orderByData: ScanKey, /* array of ordering op descriptors */
    pub orderByTypes: *mut Oid, /* array of ordering op return types */
    pub nonNullOrderByOffsets: *mut c_int, /* array of offset of non-NULL ordering keys in the original array */
    pub indexCollation: Oid,               /* collation of index column */

    /* Opclass defined functions: */
    pub innerConsistentFn: FmgrInfo,
    pub leafConsistentFn: FmgrInfo,

    /* Pre-allocated workspace arrays: */
    pub zeroDistances: *mut f64,
    pub infDistances: *mut f64,

    /* These fields are only used in amgetbitmap scans: */
    pub tbm: *mut TIDBitmap, /* bitmap being filled */
    pub ntids: int64,        /* number of TIDs passed to bitmap */

    /* These fields are only used in amgettuple scans: */
    pub want_itup: bool,         /* are we reconstructing tuples? */
    pub reconTupDesc: TupleDesc, /* if so, descriptor for reconstructed tuples */
    pub nPtrs: c_int,            /* number of TIDs found on current page */
    pub iPtr: c_int,             /* index for scanning through same */
    pub heapPtrs: [ItemPointerData; MaxIndexTuplesPerPage], /* TIDs from cur page */
    pub recheck: [bool; MaxIndexTuplesPerPage], /* their recheck flags */
    pub recheckDistances: [bool; MaxIndexTuplesPerPage], /* distance recheck flags */
    pub reconTups: [HeapTuple; MaxIndexTuplesPerPage], /* reconstructed tuples */

    /* distances (for recheck) */
    pub distances: [*mut IndexOrderByDistance; MaxIndexTuplesPerPage],
}

pub type SpGistScanOpaque = *mut SpGistScanOpaqueData;

/*
 * This struct is what we actually keep in index->rd_amcache.
 */
#[repr(C)]
pub struct SpGistCache {
    pub config: spgConfigOut, /* filled in by opclass config method */

    pub attType: SpGistTypeDesc,       /* type of values to be indexed/restored */
    pub attLeafType: SpGistTypeDesc,   /* type of leaf-tuple values */
    pub attPrefixType: SpGistTypeDesc, /* type of inner-tuple prefix values */
    pub attLabelType: SpGistTypeDesc,  /* type of node label values */

    pub lastUsedPages: SpGistLUPCache, /* local storage of last-used info */
}

/*
 * SPGiST tuple types.
 */

/* values of tupstate (see README for more info) */
pub const SPGIST_LIVE: c_int = 0; /* normal live tuple (either inner or leaf) */
pub const SPGIST_REDIRECT: c_int = 1; /* temporary redirection placeholder */
pub const SPGIST_DEAD: c_int = 2; /* dead, cannot be removed because of links */
pub const SPGIST_PLACEHOLDER: c_int = 3; /* placeholder, used to preserve offsets */

/*
 * SPGiST inner tuple: list of "nodes" that subdivide a set of tuples
 *
 * In C, the first four members are bit-fields packed into one unsigned int:
 *   tupstate:2, allTheSame:1, nNodes:13, prefixSize:16
 * We mirror that as a single `c_uint` named `bits_` plus accessor helpers.
 */
#[repr(C)]
pub struct SpGistInnerTupleData {
    pub bits_: c_uint, /* tupstate:2, allTheSame:1, nNodes:13, prefixSize:16 */
    pub size: uint16,  /* total size of inner tuple */
    /* On most machines there will be a couple of wasted bytes here */
    /* prefix datum follows, then nodes */
}

pub type SpGistInnerTuple = *mut SpGistInnerTupleData;

/* these must match largest values that fit in bit fields declared above */
pub const SGITMAXNNODES: c_int = 0x1FFF;
pub const SGITMAXPREFIXSIZE: c_int = 0xFFFF;
pub const SGITMAXSIZE: c_int = 0xFFFF;

/*
 * SPGiST node tuple: one node within an inner tuple
 */
pub type SpGistNodeTupleData = IndexTupleData;

pub type SpGistNodeTuple = *mut SpGistNodeTupleData;

/*
 * SPGiST leaf tuple: carries a leaf datum and a heap tuple TID, and
 * optionally some "included" columns.
 *
 * In C, the first two members are bit-fields packed into one unsigned int:
 *   tupstate:2, size:30
 * We mirror that as a single `c_uint` named `bits_`.
 */
#[repr(C)]
pub struct SpGistLeafTupleData {
    pub bits_: c_uint, /* tupstate:2, size:30 */
    pub t_info: uint16, /* nextOffset, which links to the next tuple in chain, plus two flag bits */
    pub heapPtr: ItemPointerData, /* TID of represented heap tuple */
    /* nulls bitmap follows if the flag bit for it is set */
    /* leaf datum, then any included datums, follows on a MAXALIGN boundary */
}

/* Macros to access nextOffset and bit fields inside t_info */
#[inline]
pub unsafe fn SGLT_GET_NEXTOFFSET(spgLeafTuple: SpGistLeafTuple) -> OffsetNumber {
    ((*spgLeafTuple).t_info & 0x3FFF) as OffsetNumber
}

#[inline]
pub unsafe fn SGLT_GET_HASNULLMASK(spgLeafTuple: SpGistLeafTuple) -> bool {
    ((*spgLeafTuple).t_info & 0x8000) != 0
}

#[inline]
pub unsafe fn SGLT_SET_NEXTOFFSET(spgLeafTuple: SpGistLeafTuple, offsetNumber: OffsetNumber) {
    (*spgLeafTuple).t_info =
        ((*spgLeafTuple).t_info & 0xC000) | ((offsetNumber as uint16) & 0x3FFF);
}

#[inline]
pub unsafe fn SGLT_SET_HASNULLMASK(spgLeafTuple: SpGistLeafTuple, hasnulls: bool) {
    (*spgLeafTuple).t_info =
        ((*spgLeafTuple).t_info & 0x7FFF) | (if hasnulls { 0x8000 } else { 0 });
}

/*
 * SPGiST dead tuple: declaration for examining non-live tuples
 *
 * In C, the first two members are bit-fields packed into one unsigned int:
 *   tupstate:2, size:30
 */
#[repr(C)]
pub struct SpGistDeadTupleData {
    pub bits_: c_uint,            /* tupstate:2, size:30 */
    pub t_info: uint16,           /* not used in dead tuples */
    pub pointer: ItemPointerData, /* redirection inside index */
    pub xid: TransactionId,       /* ID of xact that inserted this tuple */
}

pub type SpGistDeadTuple = *mut SpGistDeadTupleData;

/*
 * XLOG stuff
 */

/*
 * The "flags" argument for SpGistGetBuffer.
 *
 * Note: these flag values are used as indexes into lastUsedPages.
 */
pub const GBUF_LEAF: c_int = 0x03;

#[inline]
pub fn GBUF_INNER_PARITY(x: BlockNumber) -> c_int {
    (x % 3) as c_int
}

pub const GBUF_NULLS: c_int = 0x04;

pub const GBUF_PARITY_MASK: c_int = 0x03;

#[inline]
pub fn GBUF_REQ_LEAF(flags: c_int) -> bool {
    (flags & GBUF_PARITY_MASK) == GBUF_LEAF
}

#[inline]
pub fn GBUF_REQ_NULLS(flags: c_int) -> c_int {
    flags & GBUF_NULLS
}

/* spgutils.c */

/* reloption parameters */
pub const SPGIST_MIN_FILLFACTOR: c_int = 10;
pub const SPGIST_DEFAULT_FILLFACTOR: c_int = 80;

pub unsafe fn spgGetCache(index: Relation) -> *mut SpGistCache {
    unimplemented!()
}

pub unsafe fn getSpGistTupleDesc(index: Relation, keyType: *mut SpGistTypeDesc) -> TupleDesc {
    unimplemented!()
}

pub unsafe fn initSpGistState(state: *mut SpGistState, index: Relation) {
    unimplemented!()
}

pub unsafe fn SpGistNewBuffer(index: Relation) -> Buffer {
    unimplemented!()
}

pub unsafe fn SpGistUpdateMetaPage(index: Relation) {
    unimplemented!()
}

pub unsafe fn SpGistGetBuffer(
    index: Relation,
    flags: c_int,
    needSpace: c_int,
    isNew: *mut bool,
) -> Buffer {
    unimplemented!()
}

pub unsafe fn SpGistSetLastUsedPage(index: Relation, buffer: Buffer) {
    unimplemented!()
}

pub unsafe fn SpGistInitPage(page: Page, f: uint16) {
    unimplemented!()
}

pub unsafe fn SpGistInitBuffer(b: Buffer, f: uint16) {
    unimplemented!()
}

pub unsafe fn SpGistInitMetapage(page: Page) {
    unimplemented!()
}

pub unsafe fn SpGistGetInnerTypeSize(att: *mut SpGistTypeDesc, datum: Datum) -> c_uint {
    unimplemented!()
}

pub unsafe fn SpGistGetLeafTupleSize(
    tupleDescriptor: TupleDesc,
    datums: *const Datum,
    isnulls: *const bool,
) -> Size {
    unimplemented!()
}

pub unsafe fn spgFormLeafTuple(
    state: *mut SpGistState,
    heapPtr: ItemPointer,
    datums: *const Datum,
    isnulls: *const bool,
) -> SpGistLeafTuple {
    unimplemented!()
}

pub unsafe fn spgFormNodeTuple(
    state: *mut SpGistState,
    label: Datum,
    isnull: bool,
) -> SpGistNodeTuple {
    unimplemented!()
}

pub unsafe fn spgFormInnerTuple(
    state: *mut SpGistState,
    hasPrefix: bool,
    prefix: Datum,
    nNodes: c_int,
    nodes: *mut SpGistNodeTuple,
) -> SpGistInnerTuple {
    unimplemented!()
}

pub unsafe fn spgFormDeadTuple(
    state: *mut SpGistState,
    tupstate: c_int,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> SpGistDeadTuple {
    unimplemented!()
}

pub unsafe fn spgDeformLeafTuple(
    tup: SpGistLeafTuple,
    tupleDescriptor: TupleDesc,
    datums: *mut Datum,
    isnulls: *mut bool,
    keyColumnIsNull: bool,
) {
    unimplemented!()
}

pub unsafe fn spgExtractNodeLabels(
    state: *mut SpGistState,
    innerTuple: SpGistInnerTuple,
) -> *mut Datum {
    unimplemented!()
}

pub unsafe fn SpGistPageAddNewItem(
    state: *mut SpGistState,
    page: Page,
    item: Item,
    size: Size,
    startOffset: *mut OffsetNumber,
    errorOK: bool,
) -> OffsetNumber {
    unimplemented!()
}

pub unsafe fn spgproperty(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    unimplemented!()
}

/* spgdoinsert.c */
pub unsafe fn spgUpdateNodeLink(
    tup: SpGistInnerTuple,
    nodeN: c_int,
    blkno: BlockNumber,
    offset: OffsetNumber,
) {
    unimplemented!()
}

pub unsafe fn spgPageIndexMultiDelete(
    state: *mut SpGistState,
    page: Page,
    itemnos: *mut OffsetNumber,
    nitems: c_int,
    firststate: c_int,
    reststate: c_int,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    unimplemented!()
}

pub unsafe fn spgdoinsert(
    index: Relation,
    state: *mut SpGistState,
    heapPtr: ItemPointer,
    datums: *mut Datum,
    isnulls: *mut bool,
) -> bool {
    unimplemented!()
}

/* spgproc.c */
pub unsafe fn spg_key_orderbys_distances(
    key: Datum,
    isLeaf: bool,
    orderbys: ScanKey,
    norderbys: c_int,
) -> *mut f64 {
    unimplemented!()
}

pub unsafe fn box_copy(orig: *mut BOX) -> *mut BOX {
    unimplemented!()
}
