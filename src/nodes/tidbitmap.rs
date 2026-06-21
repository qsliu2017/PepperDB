//! Translation of postgres/src/backend/nodes/tidbitmap.c
//!                (+ the private types from postgres/src/include/nodes/tidbitmap.h)
//!
//! PostgreSQL tuple-id (TID) bitmap package.
//!
//! This module provides bitmap data structures that are spiritually similar to
//! Bitmapsets, but are specially adapted to store sets of tuple identifiers
//! (TIDs), or ItemPointers.  In particular, the division of an ItemPointer into
//! BlockNumber and OffsetNumber is catered for.  Also, since we wish to be able
//! to store very large tuple sets in memory with this data structure, we support
//! "lossy" storage, in which we no longer remember individual tuple offsets on a
//! page but only the fact that a particular page needs to be visited.
//!
//! Portions Copyright (c) 2003-2025, PostgreSQL Global Development Group
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The pagetable hashtable, which the C builds by `#include "lib/simplehash.h"`
//!   with SH_PREFIX=pagetable, is the once-ported generic
//!   `crate::lib::simplehash::SimpleHash<PagetableOps>`.  PagetableOps maps the
//!   SH_* macro arguments onto trait methods (see simplehash.rs).  The C key hash
//!   is `murmurhash32(blockno)`; we reproduce that exactly.  This is the same
//!   consumer pattern as src/common/blkreftable.rs.
//!
//! * The C stores `*mut PagetableEntry` pointers in tbm->spages/schunks and in
//!   the iterator/result.  Rust's SimpleHash stores elements in a `Vec` that
//!   reallocates on grow, so raw element pointers are unstable.  But the bitmap
//!   becomes read-only as soon as iteration begins (the C enforces this with the
//!   `iterating` flag and Asserts), so once we build the sorted lists the table
//!   never grows again, and indices/pointers into it are stable for the lifetime
//!   of the iteration.  We model spages/schunks and TBMIterateResult.internal_page
//!   with `*const PagetableEntry` raw pointers into the table's element storage,
//!   obtained via `pagetable_entry_ptr`, exactly mirroring the C.
//!
//! * The whole DSA / shared (parallel bitmap scan) path is STUBBED with
//!   `unimplemented!()` + TODO(pg-port): utils/dsa.h and storage/lwlock.h are not
//!   yet ported.  `dsa_area`/`dsa_pointer`/`LWLock` are opaque pointer-only types.
//!   The backend-local (non-shared) TIDBitmap is fully real.
//!
//! * `tbm_lossify` in C uses `pagetable_start_iterate_at(tbm->lossify_start)` and
//!   records `i.cur` to spread lossification evenly across the table.  Our generic
//!   SimpleHashIterator does not expose a start-at-offset entry point nor its
//!   private `cur` cursor, so we lossify from the plain `start_iterate()` each
//!   round.  This is functionally correct (the C comment itself calls the policy a
//!   "Really stupid implementation"); only the even-spreading optimization is
//!   lost.  `lossify_start` is retained as a field for struct fidelity but unused.
//!
//! * `makeNode(TIDBitmap)`: there is no `T_TIDBitmap` in the ported NodeTag enum
//!   (nodes.rs is not ours to touch), and the local path never does
//!   `IsA(x, TIDBitmap)`.  We palloc0 a fully-zeroed struct and leave the `type`
//!   tag as `T_Invalid`.  TODO(pg-port): add T_TIDBitmap and use makeNode! once
//!   the nodetag list is extended.

use crate::prelude::*;
use core::ffi::{c_int, c_void};

use crate::access::htup_details::MaxHeapTuplesPerPage;
use crate::common::hashfn::murmurhash32;
use crate::common::int::pg_cmp_u32;
use crate::lib::simplehash::{
    SimpleHash, SimpleHashIterator, SimpleHashOps, SH_STATUS_EMPTY, SH_STATUS_IN_USE,
};
use crate::nodes::bitmapset::{bitmapword, BITS_PER_BITMAPWORD};
use crate::nodes::nodes::NodeTag;
use crate::pg_config::BLCKSZ;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::itemptr::{ItemPointer, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber};
use crate::storage::off::OffsetNumber;
use crate::utils::palloc::{MCXT_ALLOC_HUGE, MCXT_ALLOC_ZERO};

// ===========================================================================
//   STUBS for the DSA / shared (parallel) path - utils/dsa.h, storage/lwlock.h
// ===========================================================================

// TODO(pg-port): utils/dsa.h is not ported.  dsa_area is an opaque per-query
// shared-memory arena; dsa_pointer is a relative pointer into it.
/// Opaque, pointer-only (utils/dsa.h not ported).
pub type dsa_area = c_void;
/// `dsa_pointer` (utils/dsa.h): a relative pointer into a dsa_area.
pub type dsa_pointer = uint64;
/// `InvalidDsaPointer` (utils/dsa.h).
pub const InvalidDsaPointer: dsa_pointer = 0;

#[inline]
fn DsaPointerIsValid(p: dsa_pointer) -> bool {
    p != InvalidDsaPointer
}

// TODO(pg-port): storage/lwlock.h is not ported.  LWLock is an opaque lightweight
// lock; only the shared iterator touches it.
/// Opaque, pointer-only (storage/lwlock.h not ported).
pub type LWLock = c_void;

// TODO(pg-port): port.h pg_atomic_uint32 is not wired here; only the shared
// pagetable/iteration arrays use it for refcounting.
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

/* LW_EXCLUSIVE (storage/lwlock.h) */
pub const LW_EXCLUSIVE: c_int = 1;
/* LWTRANCHE_SHARED_TIDBITMAP (storage/lwlock.h) */
pub const LWTRANCHE_SHARED_TIDBITMAP: c_int = 0;

// TODO(pg-port): the following DSA / atomics / LWLock primitives are not ported
// in this crate slice; the shared (parallel) bitmap path calls them.
unsafe fn dsa_get_address(_area: *mut dsa_area, _dp: dsa_pointer) -> *mut c_void {
    crate::utils::mmgr::dsa::dsa_get_address(_area as _, _dp as _)
}
unsafe fn dsa_allocate(_area: *mut dsa_area, _size: Size) -> dsa_pointer {
    unimplemented!("dsa_allocate: DSA path not ported");
}
unsafe fn dsa_allocate0(_area: *mut dsa_area, _size: Size) -> dsa_pointer {
    unimplemented!("dsa_allocate0: DSA path not ported");
}
unsafe fn dsa_free(_area: *mut dsa_area, _dp: dsa_pointer) {
    crate::utils::mmgr::dsa::dsa_free(_area as _, _dp as _)
}
unsafe fn pg_atomic_init_u32(p: *mut pg_atomic_uint32, val: uint32) {
    (*p).value = val;
}
#[no_mangle]
unsafe fn pg_atomic_add_fetch_u32(p: *mut pg_atomic_uint32, add_: uint32) -> uint32 {
    (*p).value = (*p).value.wrapping_add(add_);
    (*p).value
}
unsafe fn pg_atomic_sub_fetch_u32(p: *mut pg_atomic_uint32, sub_: uint32) -> uint32 {
    (*p).value = (*p).value.wrapping_sub(sub_);
    (*p).value
}
unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    /* TODO(pg-port): storage/lwlock.h not ported. */
}
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    /* TODO(pg-port): storage/lwlock.h not ported. */
    true
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    /* TODO(pg-port): storage/lwlock.h not ported. */
}
unsafe fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    core::ptr::copy_nonoverlapping(src as *const u8, dest as *mut u8, n);
    dest
}
type qsort_arg_comparator =
    unsafe fn(left: *const c_void, right: *const c_void, arg: *mut c_void) -> c_int;
unsafe fn qsort_arg(
    _base: *mut c_void,
    _nel: Size,
    _elsize: Size,
    _cmp: qsort_arg_comparator,
    _arg: *mut c_void,
) {
    crate::port::qsort::qsort_arg(_base, _nel, _elsize, _cmp, _arg)
}

/*
 * PTEntryArray (tidbitmap.c): a DSA-allocated array of PagetableEntry shared
 * across processes, prefixed by an iterator refcount.  STUB shape only.
 */
#[repr(C)]
pub struct PTEntryArray {
    /// no. of iterator attached
    pub refcount: pg_atomic_uint32,
    /// PagetableEntry ptentry[FLEXIBLE_ARRAY_MEMBER]
    pub ptentry: [PagetableEntry; 0],
}

/*
 * PTIterationArray (tidbitmap.c): a DSA-allocated array of pagetable indexes,
 * prefixed by an iterator refcount.  STUB shape only.
 */
#[repr(C)]
pub struct PTIterationArray {
    /// no. of iterator attached
    pub refcount: pg_atomic_uint32,
    /// int index[FLEXIBLE_ARRAY_MEMBER]
    pub index: [c_int; 0],
}

// ===========================================================================
//                  tidbitmap.h + tidbitmap.c private constants
// ===========================================================================

/*
 * #define TBM_MAX_TUPLES_PER_PAGE  MaxHeapTuplesPerPage  (tidbitmap.h)
 */
pub const TBM_MAX_TUPLES_PER_PAGE: c_int = MaxHeapTuplesPerPage;

/*
 * #define PAGES_PER_CHUNK  (BLCKSZ / 32)
 */
pub const PAGES_PER_CHUNK: c_int = (BLCKSZ / 32) as c_int;

/* #define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD) */
#[inline]
const fn WORDNUM(x: c_int) -> c_int {
    x / BITS_PER_BITMAPWORD
}

/* #define BITNUM(x)	((x) % BITS_PER_BITMAPWORD) */
#[inline]
const fn BITNUM(x: c_int) -> c_int {
    x % BITS_PER_BITMAPWORD
}

/* number of active words for an exact page:
 * #define WORDS_PER_PAGE	((TBM_MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1) */
pub const WORDS_PER_PAGE: usize =
    ((TBM_MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1) as usize;
/* number of active words for a lossy chunk:
 * #define WORDS_PER_CHUNK  ((PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1) */
pub const WORDS_PER_CHUNK: usize = ((PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1) as usize;

/* words[] is sized to Max(WORDS_PER_PAGE, WORDS_PER_CHUNK). */
const WORDS_PER_ENTRY: usize = if WORDS_PER_PAGE > WORDS_PER_CHUNK {
    WORDS_PER_PAGE
} else {
    WORDS_PER_CHUNK
};

// ===========================================================================
//                          PagetableEntry (the hash element)
// ===========================================================================

/*
 * The hashtable entries are represented by this data structure.  For an exact
 * page, blockno is the page number and bit k of the bitmap represents tuple
 * offset k+1.  For a lossy chunk, blockno is the first page in the chunk and
 * bit k represents page blockno+k.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PagetableEntry {
    /// page number (hashtable key)
    pub blockno: BlockNumber,
    /// hash entry status
    pub status: u8,
    /// T = lossy storage, F = exact
    pub ischunk: bool,
    /// should the tuples be rechecked?
    pub recheck: bool,
    /// per-page (or per-chunk) bitmap
    pub words: [bitmapword; WORDS_PER_ENTRY],
}

/*
 * pagetable_hash: the simplehash instantiation mapping BlockNumber -> PagetableEntry.
 *
 * SH_HASH_KEY(tb, key) == murmurhash32(key); SH_EQUAL(tb, a, b) == (a == b).
 */
pub struct PagetableOps;

impl SimpleHashOps for PagetableOps {
    type Elem = PagetableEntry;
    type Key = BlockNumber;

    fn empty_elem() -> PagetableEntry {
        PagetableEntry {
            blockno: 0,
            status: SH_STATUS_EMPTY,
            ischunk: false,
            recheck: false,
            words: [0; WORDS_PER_ENTRY],
        }
    }
    fn status(e: &PagetableEntry) -> u8 {
        e.status
    }
    fn set_status(e: &mut PagetableEntry, s: u8) {
        e.status = s;
    }
    fn hash_key(key: BlockNumber) -> u32 {
        murmurhash32(key)
    }
    fn entry_hash(e: &PagetableEntry) -> u32 {
        murmurhash32(e.blockno)
    }
    fn set_key(e: &mut PagetableEntry, key: BlockNumber) {
        e.blockno = key;
    }
    fn keys_equal(e: &PagetableEntry, key: BlockNumber) -> bool {
        e.blockno == key
    }
}

type PagetableHash = SimpleHash<PagetableOps>;

// ===========================================================================
//                          TBM status / iterating enums
// ===========================================================================

/*
 * status field of TIDBitmap.  See tbm_create_pagetable for why TBM_HASH can
 * coexist with nentries <= 1.
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TBMStatus {
    /// no hashtable, nentries == 0
    TBM_EMPTY,
    /// entry1 contains the single entry
    TBM_ONE_PAGE,
    /// pagetable is valid, entry1 is not
    TBM_HASH,
}
pub use TBMStatus::*;

/*
 * Current iterating state of the TBM.
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TBMIteratingState {
    /// not yet converted to page and chunk array
    TBM_NOT_ITERATING,
    /// converted to local page and chunk array
    TBM_ITERATING_PRIVATE,
    /// converted to shared page and chunk array
    TBM_ITERATING_SHARED,
}
pub use TBMIteratingState::*;

// ===========================================================================
//                          TIDBitmap (whole bitmap)
// ===========================================================================

/*
 * Here is the representation for a whole TIDBitmap.
 *
 * NB: `pagetable` is a boxed SimpleHash (the C uses a `pagetable_hash *`).  NULL
 * == not-yet-created, mirroring the C's `pagetable == NULL` checks.
 */
#[repr(C)]
pub struct TIDBitmap {
    /// to make it a valid Node
    pub r#type: NodeTag,
    /// memory context containing me
    pub mcxt: MemoryContext,
    /// see codes above
    pub status: TBMStatus,
    /// hash table of PagetableEntry's, or NULL
    pub pagetable: *mut PagetableHash,
    /// number of entries in pagetable
    pub nentries: c_int,
    /// limit on same to meet maxbytes
    pub maxentries: c_int,
    /// number of exact entries in pagetable
    pub npages: c_int,
    /// number of lossy entries in pagetable
    pub nchunks: c_int,
    /// tbm_begin_iterate called?
    pub iterating: TBMIteratingState,
    /// offset to start lossifying hashtable at (see module note; unused here)
    pub lossify_start: uint32,
    /// used when status == TBM_ONE_PAGE
    pub entry1: PagetableEntry,
    /* these are valid when iterating is true: */
    /// sorted exact-page list, or NULL
    pub spages: *mut *const PagetableEntry,
    /// sorted lossy-chunk list, or NULL
    pub schunks: *mut *const PagetableEntry,
    /// dsa_pointer to the element array
    pub dsapagetable: dsa_pointer,
    /// dsa_pointer to the old element array
    pub dsapagetableold: dsa_pointer,
    /// dsa_pointer to the page array
    pub ptpages: dsa_pointer,
    /// dsa_pointer to the chunk array
    pub ptchunks: dsa_pointer,
    /// reference to per-query dsa area
    pub dsa: *mut dsa_area,
}

/*
 * When iterating over a backend-local bitmap in sorted order, a
 * TBMPrivateIterator is used to track our progress.
 */
#[repr(C)]
pub struct TBMPrivateIterator {
    /// TIDBitmap we're iterating over
    pub tbm: *mut TIDBitmap,
    /// next spages index
    pub spageptr: c_int,
    /// next schunks index
    pub schunkptr: c_int,
    /// next bit to check in current schunk
    pub schunkbit: c_int,
}

/*
 * Holds the shared members of the iterator so that multiple processes can
 * jointly iterate.  STUB: the shared/DSA path is not ported.
 */
#[repr(C)]
pub struct TBMSharedIteratorState {
    pub nentries: c_int,
    pub maxentries: c_int,
    pub npages: c_int,
    pub nchunks: c_int,
    pub pagetable: dsa_pointer,
    pub spages: dsa_pointer,
    pub schunks: dsa_pointer,
    pub lock: LWLock,
    pub spageptr: c_int,
    pub schunkptr: c_int,
    pub schunkbit: c_int,
}

/*
 * same as TBMPrivateIterator, but used for joint iteration; also holds a
 * reference to the shared state.  STUB.
 */
#[repr(C)]
pub struct TBMSharedIterator {
    pub state: *mut TBMSharedIteratorState,
    pub ptbase: *mut c_void,
    pub ptpages: *mut c_void,
    pub ptchunks: *mut c_void,
}

/*
 * TBMIterator (tidbitmap.h): unified private/shared iterator handle.
 *
 * The C uses an anonymous union of two pointers; both are pointer-sized, so we
 * model `i` as a single raw pointer plus the `shared` discriminator and cast.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TBMIterator {
    pub shared: bool,
    /// union { TBMPrivateIterator *; TBMSharedIterator *; }
    pub i: *mut c_void,
}

impl TBMIterator {
    #[inline]
    fn zeroed() -> Self {
        TBMIterator {
            shared: false,
            i: core::ptr::null_mut(),
        }
    }
    #[inline]
    unsafe fn private_iterator(&self) -> *mut TBMPrivateIterator {
        self.i as *mut TBMPrivateIterator
    }
    #[inline]
    unsafe fn shared_iterator(&self) -> *mut TBMSharedIterator {
        self.i as *mut TBMSharedIterator
    }
}

/*
 * Result structure for tbm_iterate (tidbitmap.h).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TBMIterateResult {
    /// block number containing tuples
    pub blockno: BlockNumber,
    pub lossy: bool,
    /// whether the tuples should be rechecked
    pub recheck: bool,
    /// pointer to the page containing the bitmap for this block (a
    /// `*const PagetableEntry` in disguise; C exposes it as `void *`)
    pub internal_page: *const PagetableEntry,
}

/*
 * tbm_exhausted (tidbitmap.h static inline): has tbm_end_iterate been called?
 */
#[inline]
pub fn tbm_exhausted(iterator: &TBMIterator) -> bool {
    iterator.i.is_null()
}

// ===========================================================================
//                  pagetable element-pointer helper
// ===========================================================================

/*
 * Get a raw, stable pointer to the PagetableEntry stored at the given bucket
 * index of `tbm`'s pagetable.  The C dereferences `*mut PagetableEntry` returned
 * by pagetable_insert/lookup/iterate directly; here we go through the bucket
 * index that SimpleHash hands back.  Valid only while the table does not grow
 * (i.e. once iteration has begun, or transiently between mutations).
 *
 * # Safety
 * `tbm->pagetable` must be non-NULL and `idx` a live bucket index.
 */
#[inline]
unsafe fn pagetable_entry_ptr(tbm: *const TIDBitmap, idx: uint32) -> *const PagetableEntry {
    (*(*tbm).pagetable).entry(idx) as *const PagetableEntry
}

// ===========================================================================
//                                  tidbitmap.c
// ===========================================================================

/*
 * tbm_create - create an initially-empty bitmap
 *
 * The bitmap will live in CurrentMemoryContext, limited to (approximately)
 * maxbytes total memory.  `dsa` selects the shared (parallel) variant; the local
 * path is the only one fully ported here.
 *
 * # Safety
 * Allocates from the current memory context; `dsa` is null or a valid dsa_area.
 */
pub unsafe fn tbm_create(maxbytes: Size, dsa: *mut dsa_area) -> *mut TIDBitmap {
    /* Create the TIDBitmap struct and zero all its fields */
    let tbm = palloc0(core::mem::size_of::<TIDBitmap>()) as *mut TIDBitmap;
    /* See module note: no T_TIDBitmap nodetag, leave type = T_Invalid (zeroed). */

    (*tbm).mcxt = CurrentMemoryContext;
    (*tbm).status = TBM_EMPTY;

    (*tbm).maxentries = tbm_calculate_entries(maxbytes);
    (*tbm).lossify_start = 0;
    (*tbm).dsa = dsa;
    (*tbm).dsapagetable = InvalidDsaPointer;
    (*tbm).dsapagetableold = InvalidDsaPointer;
    (*tbm).ptpages = InvalidDsaPointer;
    (*tbm).ptchunks = InvalidDsaPointer;

    tbm
}

/*
 * Actually create the hashtable.  Since this is a moderately expensive
 * proposition, we don't do it until we have to.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap not already in TBM_HASH state.
 */
unsafe fn tbm_create_pagetable(tbm: *mut TIDBitmap) {
    Assert!((*tbm).status != TBM_HASH);
    Assert!((*tbm).pagetable.is_null());

    // The C passes a starting size of 128 and the tbm as private_data (for the
    // DSA-aware allocator callbacks).  Our generic SimpleHash::create takes a
    // capacity in elements and uses the global allocator (the DSA allocator hook
    // is part of the stubbed shared path).
    let ht = Box::new(PagetableHash::create(128));
    (*tbm).pagetable = Box::into_raw(ht);

    /* If entry1 is valid, push it into the hashtable */
    if (*tbm).status == TBM_ONE_PAGE {
        let (idx, found) = (*(*tbm).pagetable).insert((*tbm).entry1.blockno);
        Assert!(!found);
        let oldstatus = (*(*tbm).pagetable).entry(idx).status;
        let mut e = (*tbm).entry1;
        e.status = oldstatus;
        *(*(*tbm).pagetable).entry_mut(idx) = e;
    }

    (*tbm).status = TBM_HASH;
}

/*
 * tbm_free - free a TIDBitmap
 *
 * # Safety
 * `tbm` is a valid TIDBitmap created by tbm_create.
 */
pub unsafe fn tbm_free(tbm: *mut TIDBitmap) {
    if !(*tbm).pagetable.is_null() {
        /* pagetable_destroy: reclaim the boxed SimpleHash. */
        drop(Box::from_raw((*tbm).pagetable));
        (*tbm).pagetable = core::ptr::null_mut();
    }
    if !(*tbm).spages.is_null() {
        pfree((*tbm).spages as *mut c_void);
    }
    if !(*tbm).schunks.is_null() {
        pfree((*tbm).schunks as *mut c_void);
    }
    pfree(tbm as *mut c_void);
}

/*
 * tbm_free_shared_area - free shared state.  STUB (DSA path not ported).
 *
 * # Safety
 * Never call: the shared path is unimplemented.
 */
pub unsafe fn tbm_free_shared_area(dsa: *mut dsa_area, dp: dsa_pointer) {
    let istate: *mut TBMSharedIteratorState =
        dsa_get_address(dsa, dp) as *mut TBMSharedIteratorState;

    if DsaPointerIsValid((*istate).pagetable) {
        let ptbase: *mut PTEntryArray =
            dsa_get_address(dsa, (*istate).pagetable) as *mut PTEntryArray;
        if pg_atomic_sub_fetch_u32(&raw mut (*ptbase).refcount, 1) == 0 {
            dsa_free(dsa, (*istate).pagetable);
        }
    }
    if DsaPointerIsValid((*istate).spages) {
        let ptpages: *mut PTIterationArray =
            dsa_get_address(dsa, (*istate).spages) as *mut PTIterationArray;
        if pg_atomic_sub_fetch_u32(&raw mut (*ptpages).refcount, 1) == 0 {
            dsa_free(dsa, (*istate).spages);
        }
    }
    if DsaPointerIsValid((*istate).schunks) {
        let ptchunks: *mut PTIterationArray =
            dsa_get_address(dsa, (*istate).schunks) as *mut PTIterationArray;
        if pg_atomic_sub_fetch_u32(&raw mut (*ptchunks).refcount, 1) == 0 {
            dsa_free(dsa, (*istate).schunks);
        }
    }

    dsa_free(dsa, dp);
}

/*
 * tbm_add_tuples - add some tuple IDs to a TIDBitmap
 *
 * If recheck is true, the recheck flag is set when any of these tuples are
 * reported out.
 *
 * # Safety
 * `tbm` is a valid non-iterating TIDBitmap; `tids` points to `ntids`
 * ItemPointerData.
 */
pub unsafe fn tbm_add_tuples(
    tbm: *mut TIDBitmap,
    tids: ItemPointer,
    ntids: c_int,
    recheck: bool,
) {
    let mut currblk: BlockNumber = InvalidBlockNumber;
    let mut page: *mut PagetableEntry = core::ptr::null_mut(); /* only valid when currblk is valid */

    Assert!((*tbm).iterating == TBM_NOT_ITERATING);
    let mut i: c_int = 0;
    while i < ntids {
        let blk: BlockNumber = ItemPointerGetBlockNumber(tids.add(i as usize));
        let off: OffsetNumber = ItemPointerGetOffsetNumber(tids.add(i as usize));
        let wordnum: c_int;
        let bitnum: c_int;

        /* safety check to ensure we don't overrun bit array bounds */
        if (off as c_int) < 1 || (off as c_int) > TBM_MAX_TUPLES_PER_PAGE {
            elog!(ERROR, "tuple offset out of range: {}", off);
            unreachable!();
        }

        /*
         * Look up target page unless we already did.  This saves cycles when the
         * input includes consecutive tuples on the same page.
         */
        if blk != currblk {
            if tbm_page_is_lossy(tbm, blk) {
                page = core::ptr::null_mut(); /* remember page is lossy */
            } else {
                page = tbm_get_pageentry(tbm, blk);
            }
            currblk = blk;
        }

        if page.is_null() {
            i += 1;
            continue; /* whole page is already marked */
        }

        if (*page).ischunk {
            /* The page is a lossy chunk header, set bit for itself */
            wordnum = 0;
            bitnum = 0;
        } else {
            /* Page is exact, so set bit for individual tuple */
            wordnum = WORDNUM(off as c_int - 1);
            bitnum = BITNUM(off as c_int - 1);
        }
        (*page).words[wordnum as usize] |= (1 as bitmapword) << bitnum;
        (*page).recheck |= recheck;

        if (*tbm).nentries > (*tbm).maxentries {
            tbm_lossify(tbm);
            /* Page could have been converted to lossy, so force new lookup */
            currblk = InvalidBlockNumber;
        }

        i += 1;
    }
}

/*
 * tbm_add_page - add a whole page to a TIDBitmap
 *
 * This causes the whole page to be reported (with the recheck flag) when the
 * TIDBitmap is scanned.
 *
 * # Safety
 * `tbm` is a valid non-iterating TIDBitmap.
 */
pub unsafe fn tbm_add_page(tbm: *mut TIDBitmap, pageno: BlockNumber) {
    /* Enter the page in the bitmap, or mark it lossy if already present */
    tbm_mark_page_lossy(tbm, pageno);
    /* If we went over the memory limit, lossify some more pages */
    if (*tbm).nentries > (*tbm).maxentries {
        tbm_lossify(tbm);
    }
}

/*
 * tbm_union - set union.  `a` is modified in-place, `b` is not changed.
 *
 * # Safety
 * `a` and `b` are valid TIDBitmaps; `a` is not iterating.
 */
pub unsafe fn tbm_union(a: *mut TIDBitmap, b: *const TIDBitmap) {
    Assert!((*a).iterating == TBM_NOT_ITERATING);
    /* Nothing to do if b is empty */
    if (*b).nentries == 0 {
        return;
    }
    /* Scan through chunks and pages in b, merge into a */
    if (*b).status == TBM_ONE_PAGE {
        tbm_union_page(a, &(*b).entry1);
    } else {
        Assert!((*b).status == TBM_HASH);
        let mut it: SimpleHashIterator = (*(*b).pagetable).start_iterate();
        while let Some(idx) = (*(*b).pagetable).iterate(&mut it) {
            let bpage = (*(*b).pagetable).entry(idx) as *const PagetableEntry;
            tbm_union_page(a, bpage);
        }
    }
}

/* Process one page of b during a union op.
 *
 * # Safety
 * `a` is a valid TIDBitmap; `bpage` points to a valid PagetableEntry.
 */
unsafe fn tbm_union_page(a: *mut TIDBitmap, bpage: *const PagetableEntry) {
    let apage: *mut PagetableEntry;
    let mut wordnum: c_int;

    if (*bpage).ischunk {
        /* Scan b's chunk, mark each indicated page lossy in a */
        wordnum = 0;
        while (wordnum as usize) < WORDS_PER_CHUNK {
            let mut w: bitmapword = (*bpage).words[wordnum as usize];

            if w != 0 {
                let mut pg: BlockNumber =
                    (*bpage).blockno + (wordnum * BITS_PER_BITMAPWORD) as BlockNumber;
                while w != 0 {
                    if (w & 1) != 0 {
                        tbm_mark_page_lossy(a, pg);
                    }
                    pg += 1;
                    w >>= 1;
                }
            }
            wordnum += 1;
        }
    } else if tbm_page_is_lossy(a, (*bpage).blockno) {
        /* page is already lossy in a, nothing to do */
        return;
    } else {
        apage = tbm_get_pageentry(a, (*bpage).blockno);
        if (*apage).ischunk {
            /* The page is a lossy chunk header, set bit for itself */
            (*apage).words[0] |= (1 as bitmapword) << 0;
        } else {
            /* Both pages are exact, merge at the bit level */
            wordnum = 0;
            while (wordnum as usize) < WORDS_PER_PAGE {
                (*apage).words[wordnum as usize] |= (*bpage).words[wordnum as usize];
                wordnum += 1;
            }
            (*apage).recheck |= (*bpage).recheck;
        }
    }

    if (*a).nentries > (*a).maxentries {
        tbm_lossify(a);
    }
}

/*
 * tbm_intersect - set intersection.  `a` is modified in-place, `b` unchanged.
 *
 * # Safety
 * `a` and `b` are valid TIDBitmaps; `a` is not iterating.
 */
pub unsafe fn tbm_intersect(a: *mut TIDBitmap, b: *const TIDBitmap) {
    Assert!((*a).iterating == TBM_NOT_ITERATING);
    /* Nothing to do if a is empty */
    if (*a).nentries == 0 {
        return;
    }
    /* Scan through chunks and pages in a, try to match to b */
    if (*a).status == TBM_ONE_PAGE {
        if tbm_intersect_page(a, &mut (*a).entry1, b) {
            /* Page is now empty, remove it from a */
            Assert!(!(*a).entry1.ischunk);
            (*a).npages -= 1;
            (*a).nentries -= 1;
            Assert!((*a).nentries == 0);
            (*a).status = TBM_EMPTY;
        }
    } else {
        Assert!((*a).status == TBM_HASH);
        let mut it: SimpleHashIterator = (*(*a).pagetable).start_iterate();
        while let Some(idx) = (*(*a).pagetable).iterate(&mut it) {
            let apage = (*(*a).pagetable).entry_mut(idx) as *mut PagetableEntry;
            if tbm_intersect_page(a, apage, b) {
                /* Page or chunk is now empty, remove it from a */
                if (*apage).ischunk {
                    (*a).nchunks -= 1;
                } else {
                    (*a).npages -= 1;
                }
                (*a).nentries -= 1;
                if !(*(*a).pagetable).delete((*apage).blockno) {
                    elog!(ERROR, "hash table corrupted");
                    unreachable!();
                }
            }
        }
    }
}

/*
 * Process one page of a during an intersection op.
 *
 * Returns true if apage is now empty and should be deleted from a.
 *
 * # Safety
 * `a` and `b` are valid TIDBitmaps; `apage` points to a valid PagetableEntry in
 * `a`'s table.
 */
unsafe fn tbm_intersect_page(
    a: *mut TIDBitmap,
    apage: *mut PagetableEntry,
    b: *const TIDBitmap,
) -> bool {
    let bpage: *const PagetableEntry;
    let mut wordnum: c_int;

    if (*apage).ischunk {
        /* Scan each bit in chunk, try to clear */
        let mut candelete = true;

        wordnum = 0;
        while (wordnum as usize) < WORDS_PER_CHUNK {
            let mut w: bitmapword = (*apage).words[wordnum as usize];

            if w != 0 {
                let mut neww: bitmapword = w;
                let mut pg: BlockNumber =
                    (*apage).blockno + (wordnum * BITS_PER_BITMAPWORD) as BlockNumber;
                let mut bitnum: c_int = 0;
                while w != 0 {
                    if (w & 1) != 0 {
                        if !tbm_page_is_lossy(b, pg) && tbm_find_pageentry(b, pg).is_null() {
                            /* Page is not in b at all, lose lossy bit */
                            neww &= !((1 as bitmapword) << bitnum);
                        }
                    }
                    pg += 1;
                    bitnum += 1;
                    w >>= 1;
                }
                (*apage).words[wordnum as usize] = neww;
                if neww != 0 {
                    candelete = false;
                }
            }
            wordnum += 1;
        }
        candelete
    } else if tbm_page_is_lossy(b, (*apage).blockno) {
        /*
         * Some of the tuples in 'a' might not satisfy the quals for 'b', but
         * because the page 'b' is lossy, we don't know which ones.  Mark 'a' as
         * requiring rechecks.
         */
        (*apage).recheck = true;
        false
    } else {
        let mut candelete = true;

        bpage = tbm_find_pageentry(b, (*apage).blockno);
        if !bpage.is_null() {
            /* Both pages are exact, merge at the bit level */
            Assert!(!(*bpage).ischunk);
            wordnum = 0;
            while (wordnum as usize) < WORDS_PER_PAGE {
                (*apage).words[wordnum as usize] &= (*bpage).words[wordnum as usize];
                if (*apage).words[wordnum as usize] != 0 {
                    candelete = false;
                }
                wordnum += 1;
            }
            (*apage).recheck |= (*bpage).recheck;
        }
        /* If there is no matching b page, we can just delete the a page */
        candelete
    }
}

/*
 * tbm_is_empty - is a TIDBitmap completely empty?
 *
 * # Safety
 * `tbm` is a valid TIDBitmap.
 */
pub unsafe fn tbm_is_empty(tbm: *const TIDBitmap) -> bool {
    (*tbm).nentries == 0
}

/*
 * tbm_begin_private_iterate - prepare to iterate through a TIDBitmap.
 *
 * NB: after this is called, it is no longer allowed to modify the bitmap.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap not in TBM_ITERATING_SHARED state.
 */
pub unsafe fn tbm_begin_private_iterate(tbm: *mut TIDBitmap) -> *mut TBMPrivateIterator {
    Assert!((*tbm).iterating != TBM_ITERATING_SHARED);

    let iterator =
        palloc(core::mem::size_of::<TBMPrivateIterator>()) as *mut TBMPrivateIterator;
    (*iterator).tbm = tbm;

    /* Initialize iteration pointers. */
    (*iterator).spageptr = 0;
    (*iterator).schunkptr = 0;
    (*iterator).schunkbit = 0;

    /*
     * If we have a hashtable, create and fill the sorted page lists, unless we
     * already did so for a previous iterator.  The lists are attached to the
     * bitmap (not the iterator), so several iterators can share them.
     */
    if (*tbm).status == TBM_HASH && (*tbm).iterating == TBM_NOT_ITERATING {
        let mut npages: c_int;
        let mut nchunks: c_int;

        if (*tbm).spages.is_null() && (*tbm).npages > 0 {
            (*tbm).spages = MemoryContextAlloc(
                (*tbm).mcxt,
                (*tbm).npages as Size * core::mem::size_of::<*const PagetableEntry>(),
            ) as *mut *const PagetableEntry;
        }
        if (*tbm).schunks.is_null() && (*tbm).nchunks > 0 {
            (*tbm).schunks = MemoryContextAlloc(
                (*tbm).mcxt,
                (*tbm).nchunks as Size * core::mem::size_of::<*const PagetableEntry>(),
            ) as *mut *const PagetableEntry;
        }

        npages = 0;
        nchunks = 0;
        let mut it: SimpleHashIterator = (*(*tbm).pagetable).start_iterate();
        while let Some(idx) = (*(*tbm).pagetable).iterate(&mut it) {
            let page = pagetable_entry_ptr(tbm, idx);
            if (*page).ischunk {
                *(*tbm).schunks.add(nchunks as usize) = page;
                nchunks += 1;
            } else {
                *(*tbm).spages.add(npages as usize) = page;
                npages += 1;
            }
        }
        Assert!(npages == (*tbm).npages);
        Assert!(nchunks == (*tbm).nchunks);
        if npages > 1 {
            sort_pageentry_ptrs(
                core::slice::from_raw_parts_mut((*tbm).spages, npages as usize),
            );
        }
        if nchunks > 1 {
            sort_pageentry_ptrs(
                core::slice::from_raw_parts_mut((*tbm).schunks, nchunks as usize),
            );
        }
    }

    (*tbm).iterating = TBM_ITERATING_PRIVATE;

    iterator
}

/*
 * tbm_prepare_shared_iterate - prepare shared iteration state.  STUB.
 *
 * # Safety
 * Never call: the shared/DSA path is unimplemented.
 */
pub unsafe fn tbm_prepare_shared_iterate(tbm: *mut TIDBitmap) -> dsa_pointer {
    let dp: dsa_pointer;
    let istate: *mut TBMSharedIteratorState;
    let mut ptbase: *mut PTEntryArray = core::ptr::null_mut();
    let mut ptpages: *mut PTIterationArray = core::ptr::null_mut();
    let mut ptchunks: *mut PTIterationArray = core::ptr::null_mut();

    Assert!(!(*tbm).dsa.is_null());
    Assert!((*tbm).iterating != TBM_ITERATING_PRIVATE);

    /*
     * Allocate TBMSharedIteratorState from DSA to hold the shared members and
     * lock, this will also be used by multiple worker for shared iterate.
     */
    dp = dsa_allocate0(
        (*tbm).dsa,
        core::mem::size_of::<TBMSharedIteratorState>(),
    );
    istate = dsa_get_address((*tbm).dsa, dp) as *mut TBMSharedIteratorState;

    /*
     * If we're not already iterating, create and fill the sorted page lists.
     * (If we are, the sorted page lists are already stored in the TIDBitmap,
     * and we can just reuse them.)
     */
    if (*tbm).iterating == TBM_NOT_ITERATING {
        let mut idx: c_int;
        let mut npages: c_int;
        let mut nchunks: c_int;

        /*
         * Allocate the page and chunk array memory from the DSA to share
         * across multiple processes.
         */
        if (*tbm).npages != 0 {
            (*tbm).ptpages = dsa_allocate(
                (*tbm).dsa,
                core::mem::size_of::<PTIterationArray>()
                    + (*tbm).npages as usize * core::mem::size_of::<c_int>(),
            );
            ptpages = dsa_get_address((*tbm).dsa, (*tbm).ptpages) as *mut PTIterationArray;
            pg_atomic_init_u32(&raw mut (*ptpages).refcount, 0);
        }
        if (*tbm).nchunks != 0 {
            (*tbm).ptchunks = dsa_allocate(
                (*tbm).dsa,
                core::mem::size_of::<PTIterationArray>()
                    + (*tbm).nchunks as usize * core::mem::size_of::<c_int>(),
            );
            ptchunks = dsa_get_address((*tbm).dsa, (*tbm).ptchunks) as *mut PTIterationArray;
            pg_atomic_init_u32(&raw mut (*ptchunks).refcount, 0);
        }

        /*
         * If TBM status is TBM_HASH then iterate over the pagetable and
         * convert it to page and chunk arrays.  But if it's in the
         * TBM_ONE_PAGE mode then directly allocate the space for one entry
         * from the DSA.
         */
        npages = 0;
        nchunks = 0;
        if (*tbm).status == TBM_HASH {
            ptbase = dsa_get_address((*tbm).dsa, (*tbm).dsapagetable) as *mut PTEntryArray;

            let mut i: SimpleHashIterator = (*(*tbm).pagetable).start_iterate();
            while let Some(page_idx) = (*(*tbm).pagetable).iterate(&mut i) {
                let page: *const PagetableEntry = pagetable_entry_ptr(tbm, page_idx);
                idx = page_idx as c_int;
                if (*page).ischunk {
                    *(*ptchunks).index.as_mut_ptr().add(nchunks as usize) = idx;
                    nchunks += 1;
                } else {
                    *(*ptpages).index.as_mut_ptr().add(npages as usize) = idx;
                    npages += 1;
                }
            }

            Assert!(npages == (*tbm).npages);
            Assert!(nchunks == (*tbm).nchunks);
        } else if (*tbm).status == TBM_ONE_PAGE {
            /*
             * In one page mode allocate the space for one pagetable entry,
             * initialize it, and directly store its index (i.e. 0) in the
             * page array.
             */
            (*tbm).dsapagetable = dsa_allocate(
                (*tbm).dsa,
                core::mem::size_of::<PTEntryArray>() + core::mem::size_of::<PagetableEntry>(),
            );
            ptbase = dsa_get_address((*tbm).dsa, (*tbm).dsapagetable) as *mut PTEntryArray;
            memcpy(
                (*ptbase).ptentry.as_mut_ptr() as *mut c_void,
                &raw const (*tbm).entry1 as *const c_void,
                core::mem::size_of::<PagetableEntry>(),
            );
            *(*ptpages).index.as_mut_ptr().add(0) = 0;
        }

        if !ptbase.is_null() {
            pg_atomic_init_u32(&raw mut (*ptbase).refcount, 0);
        }
        if npages > 1 {
            qsort_arg(
                (*ptpages).index.as_mut_ptr() as *mut c_void,
                npages as Size,
                core::mem::size_of::<c_int>(),
                tbm_shared_comparator,
                (*ptbase).ptentry.as_mut_ptr() as *mut c_void,
            );
        }
        if nchunks > 1 {
            qsort_arg(
                (*ptchunks).index.as_mut_ptr() as *mut c_void,
                nchunks as Size,
                core::mem::size_of::<c_int>(),
                tbm_shared_comparator,
                (*ptbase).ptentry.as_mut_ptr() as *mut c_void,
            );
        }
    }

    /*
     * Store the TBM members in the shared state so that we can share them
     * across multiple processes.
     */
    (*istate).nentries = (*tbm).nentries;
    (*istate).maxentries = (*tbm).maxentries;
    (*istate).npages = (*tbm).npages;
    (*istate).nchunks = (*tbm).nchunks;
    (*istate).pagetable = (*tbm).dsapagetable;
    (*istate).spages = (*tbm).ptpages;
    (*istate).schunks = (*tbm).ptchunks;

    ptbase = dsa_get_address((*tbm).dsa, (*tbm).dsapagetable) as *mut PTEntryArray;
    ptpages = dsa_get_address((*tbm).dsa, (*tbm).ptpages) as *mut PTIterationArray;
    ptchunks = dsa_get_address((*tbm).dsa, (*tbm).ptchunks) as *mut PTIterationArray;

    /*
     * For every shared iterator referring to pagetable and iterator array,
     * increase the refcount by 1 so that while freeing the shared iterator we
     * don't free pagetable and iterator array until its refcount becomes 0.
     */
    if !ptbase.is_null() {
        pg_atomic_add_fetch_u32(&raw mut (*ptbase).refcount, 1);
    }
    if !ptpages.is_null() {
        pg_atomic_add_fetch_u32(&raw mut (*ptpages).refcount, 1);
    }
    if !ptchunks.is_null() {
        pg_atomic_add_fetch_u32(&raw mut (*ptchunks).refcount, 1);
    }

    /* Initialize the iterator lock */
    LWLockInitialize(&raw mut (*istate).lock, LWTRANCHE_SHARED_TIDBITMAP);

    /* Initialize the shared iterator state */
    (*istate).schunkbit = 0;
    (*istate).schunkptr = 0;
    (*istate).spageptr = 0;

    (*tbm).iterating = TBM_ITERATING_SHARED;

    dp
}

/*
 * tbm_extract_page_tuple - extract the tuple offsets from a page.
 *
 * Returns the number of offsets it filled in if <= max_offsets; otherwise fills
 * in as many as fit and returns the total number of offsets in the page.
 *
 * # Safety
 * `iteritem` is a TBMIterateResult whose internal_page points to a valid exact
 * PagetableEntry; `offsets` has room for at least `max_offsets`.
 */
pub unsafe fn tbm_extract_page_tuple(
    iteritem: *const TBMIterateResult,
    offsets: *mut OffsetNumber,
    max_offsets: uint32,
) -> c_int {
    let page: *const PagetableEntry = (*iteritem).internal_page;
    let mut wordnum: c_int;
    let mut ntuples: c_int = 0;

    wordnum = 0;
    while (wordnum as usize) < WORDS_PER_PAGE {
        let mut w: bitmapword = (*page).words[wordnum as usize];

        if w != 0 {
            let mut off: c_int = wordnum * BITS_PER_BITMAPWORD + 1;

            while w != 0 {
                if (w & 1) != 0 {
                    if (ntuples as uint32) < max_offsets {
                        *offsets.add(ntuples as usize) = off as OffsetNumber;
                    }
                    ntuples += 1;
                }
                off += 1;
                w >>= 1;
            }
        }
        wordnum += 1;
    }

    ntuples
}

/*
 * tbm_advance_schunkbit - advance the schunkbit to the next set page bit.
 *
 * # Safety
 * `chunk` points to a valid lossy-chunk PagetableEntry; `schunkbitp` is writable.
 */
#[inline]
unsafe fn tbm_advance_schunkbit(chunk: *const PagetableEntry, schunkbitp: *mut c_int) {
    let mut schunkbit: c_int = *schunkbitp;

    while schunkbit < PAGES_PER_CHUNK {
        let wordnum = WORDNUM(schunkbit);
        let bitnum = BITNUM(schunkbit);

        if ((*chunk).words[wordnum as usize] & ((1 as bitmapword) << bitnum)) != 0 {
            break;
        }
        schunkbit += 1;
    }

    *schunkbitp = schunkbit;
}

/*
 * tbm_private_iterate - scan through next page of a TIDBitmap.
 *
 * Pages are delivered in numerical order.  Returns false when there are no more
 * pages (and sets tbmres->blockno = InvalidBlockNumber).
 *
 * # Safety
 * `iterator` was returned by tbm_begin_private_iterate; `tbmres` is writable.
 */
pub unsafe fn tbm_private_iterate(
    iterator: *mut TBMPrivateIterator,
    tbmres: *mut TBMIterateResult,
) -> bool {
    let tbm: *mut TIDBitmap = (*iterator).tbm;

    Assert!((*tbm).iterating == TBM_ITERATING_PRIVATE);

    /*
     * If lossy chunk pages remain, advance schunkptr/schunkbit to the next set
     * bit.
     */
    while (*iterator).schunkptr < (*tbm).nchunks {
        let chunk: *const PagetableEntry = *(*tbm).schunks.add((*iterator).schunkptr as usize);
        let mut schunkbit: c_int = (*iterator).schunkbit;

        tbm_advance_schunkbit(chunk, &mut schunkbit);
        if schunkbit < PAGES_PER_CHUNK {
            (*iterator).schunkbit = schunkbit;
            break;
        }
        /* advance to next chunk */
        (*iterator).schunkptr += 1;
        (*iterator).schunkbit = 0;
    }

    /*
     * If both chunk and per-page data remain, output the numerically earlier
     * page.
     */
    if (*iterator).schunkptr < (*tbm).nchunks {
        let chunk: *const PagetableEntry = *(*tbm).schunks.add((*iterator).schunkptr as usize);
        let chunk_blockno: BlockNumber = (*chunk).blockno + (*iterator).schunkbit as BlockNumber;

        if (*iterator).spageptr >= (*tbm).npages
            || chunk_blockno < (**(*tbm).spages.add((*iterator).spageptr as usize)).blockno
        {
            /* Return a lossy page indicator from the chunk */
            (*tbmres).blockno = chunk_blockno;
            (*tbmres).lossy = true;
            (*tbmres).recheck = true;
            (*tbmres).internal_page = core::ptr::null();
            (*iterator).schunkbit += 1;
            return true;
        }
    }

    if (*iterator).spageptr < (*tbm).npages {
        let page: *const PagetableEntry;

        /* In TBM_ONE_PAGE state, we don't allocate an spages[] array */
        if (*tbm).status == TBM_ONE_PAGE {
            page = &(*tbm).entry1;
        } else {
            page = *(*tbm).spages.add((*iterator).spageptr as usize);
        }

        (*tbmres).internal_page = page;
        (*tbmres).blockno = (*page).blockno;
        (*tbmres).lossy = false;
        (*tbmres).recheck = (*page).recheck;
        (*iterator).spageptr += 1;
        return true;
    }

    /* Nothing more in the bitmap */
    (*tbmres).blockno = InvalidBlockNumber;
    false
}

/*
 * tbm_shared_iterate - scan through next page using a shared iterator.  STUB.
 *
 * # Safety
 * Never call: the shared/DSA path is unimplemented.
 */
pub unsafe fn tbm_shared_iterate(
    iterator: *mut TBMSharedIterator,
    tbmres: *mut TBMIterateResult,
) -> bool {
    let istate: *mut TBMSharedIteratorState = (*iterator).state;
    let mut ptbase: *mut PagetableEntry = core::ptr::null_mut();
    let mut idxpages: *mut c_int = core::ptr::null_mut();
    let mut idxchunks: *mut c_int = core::ptr::null_mut();

    if !(*iterator).ptbase.is_null() {
        ptbase = (*((*iterator).ptbase as *mut PTEntryArray)).ptentry.as_mut_ptr();
    }
    if !(*iterator).ptpages.is_null() {
        idxpages = (*((*iterator).ptpages as *mut PTIterationArray)).index.as_mut_ptr();
    }
    if !(*iterator).ptchunks.is_null() {
        idxchunks = (*((*iterator).ptchunks as *mut PTIterationArray)).index.as_mut_ptr();
    }

    /* Acquire the LWLock before accessing the shared members */
    LWLockAcquire(&raw mut (*istate).lock, LW_EXCLUSIVE);

    /*
     * If lossy chunk pages remain, make sure we've advanced schunkptr/
     * schunkbit to the next set bit.
     */
    while (*istate).schunkptr < (*istate).nchunks {
        let chunk: *const PagetableEntry =
            ptbase.add(*idxchunks.add((*istate).schunkptr as usize) as usize);
        let mut schunkbit: c_int = (*istate).schunkbit;

        tbm_advance_schunkbit(chunk, &mut schunkbit);
        if schunkbit < PAGES_PER_CHUNK {
            (*istate).schunkbit = schunkbit;
            break;
        }
        /* advance to next chunk */
        (*istate).schunkptr += 1;
        (*istate).schunkbit = 0;
    }

    /*
     * If both chunk and per-page data remain, must output the numerically
     * earlier page.
     */
    if (*istate).schunkptr < (*istate).nchunks {
        let chunk: *const PagetableEntry =
            &*ptbase.add(*idxchunks.add((*istate).schunkptr as usize) as usize);
        let chunk_blockno: BlockNumber = (*chunk).blockno + (*istate).schunkbit as BlockNumber;

        if (*istate).spageptr >= (*istate).npages
            || chunk_blockno
                < (*ptbase.add(*idxpages.add((*istate).spageptr as usize) as usize)).blockno
        {
            /* Return a lossy page indicator from the chunk */
            (*tbmres).blockno = chunk_blockno;
            (*tbmres).lossy = true;
            (*tbmres).recheck = true;
            (*tbmres).internal_page = core::ptr::null();
            (*istate).schunkbit += 1;

            LWLockRelease(&raw mut (*istate).lock);
            return true;
        }
    }

    if (*istate).spageptr < (*istate).npages {
        let page: *mut PagetableEntry =
            ptbase.add(*idxpages.add((*istate).spageptr as usize) as usize);

        (*tbmres).internal_page = page;
        (*tbmres).blockno = (*page).blockno;
        (*tbmres).lossy = false;
        (*tbmres).recheck = (*page).recheck;
        (*istate).spageptr += 1;

        LWLockRelease(&raw mut (*istate).lock);

        return true;
    }

    LWLockRelease(&raw mut (*istate).lock);

    /* Nothing more in the bitmap */
    (*tbmres).blockno = InvalidBlockNumber;
    false
}

/*
 * tbm_end_private_iterate - finish an iteration over a TIDBitmap.
 *
 * # Safety
 * `iterator` was returned by tbm_begin_private_iterate and is not reused after.
 */
pub unsafe fn tbm_end_private_iterate(iterator: *mut TBMPrivateIterator) {
    pfree(iterator as *mut c_void);
}

/*
 * tbm_end_shared_iterate - finish a shared iteration.
 *
 * # Safety
 * `iterator` was returned by tbm_attach_shared_iterate and is not reused after.
 */
pub unsafe fn tbm_end_shared_iterate(iterator: *mut TBMSharedIterator) {
    pfree(iterator as *mut c_void);
}

/*
 * tbm_find_pageentry - find a PagetableEntry for the pageno.
 *
 * Returns NULL if there is no non-lossy entry for the pageno.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap.
 */
unsafe fn tbm_find_pageentry(tbm: *const TIDBitmap, pageno: BlockNumber) -> *const PagetableEntry {
    if (*tbm).nentries == 0 {
        /* in case pagetable doesn't exist */
        return core::ptr::null();
    }

    if (*tbm).status == TBM_ONE_PAGE {
        let page: *const PagetableEntry = &(*tbm).entry1;
        if (*page).blockno != pageno {
            return core::ptr::null();
        }
        Assert!(!(*page).ischunk);
        return page;
    }

    match (*(*tbm).pagetable).lookup(pageno) {
        None => core::ptr::null(),
        Some(idx) => {
            let page = pagetable_entry_ptr(tbm, idx);
            if (*page).ischunk {
                core::ptr::null() /* don't want a lossy chunk header */
            } else {
                page
            }
        }
    }
}

/*
 * tbm_get_pageentry - find or create a PagetableEntry for the pageno.
 *
 * If new, the entry is marked as an exact (non-chunk) entry.  May cause the
 * table to exceed the desired memory size; caller must call tbm_lossify() at the
 * next safe point if so.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap.
 */
unsafe fn tbm_get_pageentry(tbm: *mut TIDBitmap, pageno: BlockNumber) -> *mut PagetableEntry {
    let page: *mut PagetableEntry;
    let found: bool;

    if (*tbm).status == TBM_EMPTY {
        /* Use the fixed slot */
        page = &mut (*tbm).entry1;
        found = false;
        (*tbm).status = TBM_ONE_PAGE;
    } else {
        if (*tbm).status == TBM_ONE_PAGE {
            let p: *mut PagetableEntry = &mut (*tbm).entry1;
            if (*p).blockno == pageno {
                return p;
            }
            /* Time to switch from one page to a hashtable */
            tbm_create_pagetable(tbm);
        }

        /* Look up or create an entry */
        let (idx, f) = (*(*tbm).pagetable).insert(pageno);
        found = f;
        page = (*(*tbm).pagetable).entry_mut(idx) as *mut PagetableEntry;
    }

    /* Initialize it if not present before */
    if !found {
        let oldstatus = (*page).status;
        *page = PagetableOps::empty_elem();
        (*page).status = oldstatus;
        (*page).blockno = pageno;
        /* must count it too */
        (*tbm).nentries += 1;
        (*tbm).npages += 1;
    }

    page
}

/*
 * tbm_page_is_lossy - is the page marked as lossily stored?
 *
 * # Safety
 * `tbm` is a valid TIDBitmap.
 */
unsafe fn tbm_page_is_lossy(tbm: *const TIDBitmap, pageno: BlockNumber) -> bool {
    /* we can skip the lookup if there are no lossy chunks */
    if (*tbm).nchunks == 0 {
        return false;
    }
    Assert!((*tbm).status == TBM_HASH);

    let bitno: c_int = (pageno % PAGES_PER_CHUNK as BlockNumber) as c_int;
    let chunk_pageno: BlockNumber = pageno - bitno as BlockNumber;

    if let Some(idx) = (*(*tbm).pagetable).lookup(chunk_pageno) {
        let page = pagetable_entry_ptr(tbm, idx);
        if (*page).ischunk {
            let wordnum = WORDNUM(bitno);
            let bitnum = BITNUM(bitno);

            if ((*page).words[wordnum as usize] & ((1 as bitmapword) << bitnum)) != 0 {
                return true;
            }
        }
    }
    false
}

/*
 * tbm_mark_page_lossy - mark the page number as lossily stored.
 *
 * May cause the table to exceed the desired memory size; caller must call
 * tbm_lossify() at the next safe point if so.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap.
 */
unsafe fn tbm_mark_page_lossy(tbm: *mut TIDBitmap, pageno: BlockNumber) {
    /* We force the bitmap into hashtable mode whenever it's lossy */
    if (*tbm).status != TBM_HASH {
        tbm_create_pagetable(tbm);
    }

    let bitno: c_int = (pageno % PAGES_PER_CHUNK as BlockNumber) as c_int;
    let chunk_pageno: BlockNumber = pageno - bitno as BlockNumber;

    /*
     * Remove any extant non-lossy entry for the page.  If the page is its own
     * chunk header, however, we skip this and handle the case below.
     */
    if bitno != 0 {
        if (*(*tbm).pagetable).delete(pageno) {
            /* It was present, so adjust counts */
            (*tbm).nentries -= 1;
            (*tbm).npages -= 1; /* assume it must have been non-lossy */
        }
    }

    /* Look up or create entry for chunk-header page */
    let (idx, found) = (*(*tbm).pagetable).insert(chunk_pageno);
    let page: *mut PagetableEntry = (*(*tbm).pagetable).entry_mut(idx) as *mut PagetableEntry;

    /* Initialize it if not present before */
    if !found {
        let oldstatus = (*page).status;
        *page = PagetableOps::empty_elem();
        (*page).status = oldstatus;
        (*page).blockno = chunk_pageno;
        (*page).ischunk = true;
        /* must count it too */
        (*tbm).nentries += 1;
        (*tbm).nchunks += 1;
    } else if !(*page).ischunk {
        let oldstatus = (*page).status;
        /* chunk header page was formerly non-lossy, make it lossy */
        *page = PagetableOps::empty_elem();
        (*page).status = oldstatus;
        (*page).blockno = chunk_pageno;
        (*page).ischunk = true;
        /* we assume it had some tuple bit(s) set, so mark it lossy */
        (*page).words[0] = (1 as bitmapword) << 0;
        /* adjust counts */
        (*tbm).nchunks += 1;
        (*tbm).npages -= 1;
    }

    /* Now set the original target page's bit */
    let wordnum = WORDNUM(bitno);
    let bitnum = BITNUM(bitno);
    (*page).words[wordnum as usize] |= (1 as bitmapword) << bitnum;
}

/*
 * tbm_lossify - lose some information to get back under the memory limit.
 *
 * See module note: the C's lossify_start round-robin optimization is dropped; we
 * lossify from the start of the table each call, which is functionally correct.
 *
 * # Safety
 * `tbm` is a valid, non-iterating, TBM_HASH TIDBitmap.
 */
unsafe fn tbm_lossify(tbm: *mut TIDBitmap) {
    /*
     * XXX Really stupid implementation: this just lossifies pages in essentially
     * random order.
     *
     * Since we're called as soon as nentries exceeds maxentries, push nentries
     * down to significantly less than maxentries (we shoot for maxentries/2).
     */
    Assert!((*tbm).iterating == TBM_NOT_ITERATING);
    Assert!((*tbm).status == TBM_HASH);

    let mut it: SimpleHashIterator = (*(*tbm).pagetable).start_iterate();
    while let Some(idx) = (*(*tbm).pagetable).iterate(&mut it) {
        let page = pagetable_entry_ptr(tbm, idx);

        if (*page).ischunk {
            continue; /* already a chunk header */
        }

        /*
         * If the page would become a chunk header, we won't save anything by
         * converting it to lossy, so skip it.
         */
        if ((*page).blockno % PAGES_PER_CHUNK as BlockNumber) == 0 {
            continue;
        }

        /* This does the dirty work ... */
        tbm_mark_page_lossy(tbm, (*page).blockno);

        if (*tbm).nentries <= (*tbm).maxentries / 2 {
            /* We have made enough room. */
            break;
        }

        /*
         * Note: tbm_mark_page_lossy may have inserted a lossy chunk and deleted
         * the non-lossy entry.  We can continue the same scan, since failure to
         * visit one element or visiting the newly inserted element isn't fatal.
         */
    }

    /*
     * With a big bitmap and small work_mem, it's possible that we cannot get
     * under maxentries.  Force maxentries up to at least double the current
     * number of entries to avoid a performance sink.
     */
    if (*tbm).nentries > (*tbm).maxentries / 2 {
        (*tbm).maxentries = Min((*tbm).nentries, (c_int::MAX - 1) / 2) * 2;
    }
}

/*
 * sort helper for PagetableEntry pointers (the C uses qsort with tbm_comparator).
 *
 * tbm_comparator orders by blockno via pg_cmp_u32.
 *
 * # Safety
 * Every pointer in `slice` references a valid PagetableEntry.
 */
unsafe fn sort_pageentry_ptrs(slice: &mut [*const PagetableEntry]) {
    slice.sort_by(|&l, &r| match tbm_comparator(l, r) {
        x if x < 0 => core::cmp::Ordering::Less,
        x if x > 0 => core::cmp::Ordering::Greater,
        _ => core::cmp::Ordering::Equal,
    });
}

/*
 * tbm_comparator - qsort comparator to handle PagetableEntry pointers.
 *
 * # Safety
 * `left` and `right` reference valid PagetableEntry.
 */
unsafe fn tbm_comparator(left: *const PagetableEntry, right: *const PagetableEntry) -> c_int {
    let l: BlockNumber = (*left).blockno;
    let r: BlockNumber = (*right).blockno;
    pg_cmp_u32(l, r)
}

/*
 * tbm_shared_comparator - qsort_arg comparator over index-into-array.  STUB.
 *
 * # Safety
 * Never call: only used by the shared/DSA path.
 */
unsafe fn tbm_shared_comparator(left: *const c_void, right: *const c_void, arg: *mut c_void) -> c_int {
    let base: *const PagetableEntry = arg as *const PagetableEntry;
    let lpage: *const PagetableEntry = base.add(*(left as *const c_int) as usize);
    let rpage: *const PagetableEntry = base.add(*(right as *const c_int) as usize);

    if (*lpage).blockno < (*rpage).blockno {
        -1
    } else if (*lpage).blockno > (*rpage).blockno {
        1
    } else {
        0
    }
}

/*
 * tbm_attach_shared_iterate - attach a shared iterator state.
 *
 * # Safety
 * `dsa`/`dp` describe a live TBMSharedIteratorState set up by the leader.
 */
pub unsafe fn tbm_attach_shared_iterate(
    dsa: *mut dsa_area,
    dp: dsa_pointer,
) -> *mut TBMSharedIterator {
    /*
     * Create the TBMSharedIterator struct, with enough trailing space to
     * serve the needs of the TBMIterateResult sub-struct.
     */
    let iterator =
        palloc0(core::mem::size_of::<TBMSharedIterator>()) as *mut TBMSharedIterator;

    let istate = dsa_get_address(dsa, dp) as *mut TBMSharedIteratorState;

    (*iterator).state = istate;

    (*iterator).ptbase = dsa_get_address(dsa, (*istate).pagetable);

    if (*istate).npages != 0 {
        (*iterator).ptpages = dsa_get_address(dsa, (*istate).spages);
    }
    if (*istate).nchunks != 0 {
        (*iterator).ptchunks = dsa_get_address(dsa, (*istate).schunks);
    }

    iterator
}

/*
 * pagetable_allocate
 *
 * Callback function for allocating the memory for hashtable elements.
 * Allocate memory for hashtable elements, using DSA if available.
 *
 * Translation note: the C declares this as the SH_ALLOCATE callback for the
 * simplehash-generated `pagetable_hash`, reaching the owning bitmap through
 * `pagetable->private_data` and the local context through `pagetable->ctx`.
 * Our ported `crate::lib::simplehash::SimpleHash` keeps its elements in a `Vec`
 * and never invokes SH_ALLOCATE/SH_FREE, so this callback has no caller; we
 * translate it faithfully but take the owning `tbm` (== `pagetable->private_data`)
 * directly, which is exactly what the C extracts on its first line.  The DSA
 * branch stays stubbed in line with the rest of this file's shared path.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap.
 */
unsafe fn pagetable_allocate(tbm: *mut TIDBitmap, size: Size) -> *mut c_void {
    if (*tbm).dsa.is_null() {
        return MemoryContextAllocExtended(
            (*tbm).mcxt,
            size,
            MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO,
        );
    }

    /*
     * Save the dsapagetable reference in dsapagetableold before allocating
     * new memory so that pagetable_free can free the old entry.
     */
    let _ = size;
    unimplemented!("pagetable_allocate: DSA/shared path not ported");
    /* C also:
     * tbm->dsapagetableold = tbm->dsapagetable;
     * tbm->dsapagetable = dsa_allocate_extended(tbm->dsa,
     *                                           sizeof(PTEntryArray) + size,
     *                                           DSA_ALLOC_HUGE | DSA_ALLOC_ZERO);
     * ptbase = dsa_get_address(tbm->dsa, tbm->dsapagetable);
     * return ptbase->ptentry;
     */
}

/*
 * pagetable_free
 *
 * Callback function for freeing hash table elements.
 *
 * Translation note: see pagetable_allocate; `tbm` is `pagetable->private_data`.
 *
 * # Safety
 * `tbm` is a valid TIDBitmap; `pointer` was returned by pagetable_allocate.
 */
unsafe fn pagetable_free(tbm: *mut TIDBitmap, pointer: *mut c_void) {
    /* pfree the input pointer if DSA is not available */
    if (*tbm).dsa.is_null() {
        pfree(pointer);
    } else if DsaPointerIsValid((*tbm).dsapagetableold) {
        unimplemented!("pagetable_free: DSA/shared path not ported");
        /* C also:
         * dsa_free(tbm->dsa, tbm->dsapagetableold);
         * tbm->dsapagetableold = InvalidDsaPointer;
         */
    }
}

/*
 * tbm_calculate_entries - estimate number of hashtable entries within maxbytes.
 */
pub fn tbm_calculate_entries(maxbytes: Size) -> c_int {
    /*
     * This estimates the hash cost as sizeof(PagetableEntry), good enough for our
     * purpose.  Also count an extra Pointer per entry for the arrays created
     * during iteration readout.
     */
    let mut nbuckets: Size = maxbytes
        / (core::mem::size_of::<PagetableEntry>()
            + core::mem::size_of::<Pointer>()
            + core::mem::size_of::<Pointer>());
    nbuckets = Min(nbuckets, (c_int::MAX - 1) as Size); /* safety limit */
    nbuckets = Max(nbuckets, 16); /* sanity limit */

    nbuckets as c_int
}

/*
 * tbm_begin_iterate - create a shared or private bitmap iterator and start
 * iteration.
 *
 * `tbm` is only used to create the private iterator; dsa/dsp create the shared
 * iterator (stubbed).
 *
 * # Safety
 * `tbm` is a valid TIDBitmap; if `dsp` is valid the shared path is taken (unimpl).
 */
pub unsafe fn tbm_begin_iterate(
    tbm: *mut TIDBitmap,
    dsa: *mut dsa_area,
    dsp: dsa_pointer,
) -> TBMIterator {
    let mut iterator = TBMIterator::zeroed();

    /* Allocate a private iterator and attach the shared state to it */
    if DsaPointerIsValid(dsp) {
        iterator.shared = true;
        iterator.i = tbm_attach_shared_iterate(dsa, dsp) as *mut c_void;
    } else {
        iterator.shared = false;
        iterator.i = tbm_begin_private_iterate(tbm) as *mut c_void;
    }

    iterator
}

/*
 * tbm_end_iterate - clean up a shared or private bitmap iterator.
 *
 * # Safety
 * `iterator` is a valid, non-exhausted TBMIterator.
 */
pub unsafe fn tbm_end_iterate(iterator: *mut TBMIterator) {
    Assert!(!iterator.is_null() && !tbm_exhausted(&*iterator));

    if (*iterator).shared {
        tbm_end_shared_iterate((*iterator).shared_iterator());
    } else {
        tbm_end_private_iterate((*iterator).private_iterator());
    }

    *iterator = TBMIterator::zeroed();
}

/*
 * tbm_iterate - populate the next TBMIterateResult via the iterator.
 *
 * Returns false when there is nothing more to scan.
 *
 * # Safety
 * `iterator` and `tbmres` are valid writable pointers.
 */
pub unsafe fn tbm_iterate(iterator: *mut TBMIterator, tbmres: *mut TBMIterateResult) -> bool {
    Assert!(!iterator.is_null());
    Assert!(!tbmres.is_null());

    if (*iterator).shared {
        tbm_shared_iterate((*iterator).shared_iterator(), tbmres)
    } else {
        tbm_private_iterate((*iterator).private_iterator(), tbmres)
    }
}

// ===========================================================================
//                                  tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block::BlockNumber;
    use crate::storage::itemptr::{ItemPointerData, ItemPointerSet};

    // tbm_create allocates from CurrentMemoryContext; serialize tests that touch
    // the process-global allocator state.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    unsafe fn make_tid(blk: BlockNumber, off: OffsetNumber) -> ItemPointerData {
        let mut t = ItemPointerData {
            ip_blkid: crate::storage::block::BlockIdData { bi_hi: 0, bi_lo: 0 },
            ip_posid: 0,
        };
        ItemPointerSet(&mut t, blk, off);
        t
    }

    /// Collect (blockno, lossy, offsets) from a fresh private iteration.
    unsafe fn drain(tbm: *mut TIDBitmap) -> Vec<(BlockNumber, bool, Vec<OffsetNumber>)> {
        let mut out = Vec::new();
        let it = tbm_begin_private_iterate(tbm);
        let mut res = TBMIterateResult {
            blockno: 0,
            lossy: false,
            recheck: false,
            internal_page: core::ptr::null(),
        };
        while tbm_private_iterate(it, &mut res) {
            let mut offs = Vec::new();
            if !res.lossy {
                let mut buf = [0 as OffsetNumber; TBM_MAX_TUPLES_PER_PAGE as usize];
                let n = tbm_extract_page_tuple(&res, buf.as_mut_ptr(), buf.len() as uint32);
                for k in 0..n as usize {
                    offs.push(buf[k]);
                }
            }
            out.push((res.blockno, res.lossy, offs));
        }
        tbm_end_private_iterate(it);
        out
    }

    #[test]
    fn create_add_iterate_exact() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let tbm = tbm_create(1024 * 1024, core::ptr::null_mut());
            assert!(tbm_is_empty(tbm));

            // (block, offset) TIDs across two pages.
            let tids = [
                make_tid(5, 1),
                make_tid(5, 3),
                make_tid(5, 7),
                make_tid(10, 2),
            ];
            tbm_add_tuples(tbm, tids.as_ptr() as ItemPointer, tids.len() as c_int, false);
            assert!(!tbm_is_empty(tbm));

            let got = drain(tbm);
            // Pages delivered in numerical order, exact (non-lossy).
            assert_eq!(got.len(), 2);
            assert_eq!(got[0].0, 5);
            assert!(!got[0].1);
            assert_eq!(got[0].2, vec![1, 3, 7]);
            assert_eq!(got[1].0, 10);
            assert!(!got[1].1);
            assert_eq!(got[1].2, vec![2]);

            tbm_free(tbm);
        }
    }

    #[test]
    fn union_and_intersect() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            // a: pages {5,10}
            let a = tbm_create(1024 * 1024, core::ptr::null_mut());
            let ta = [make_tid(5, 1), make_tid(10, 2)];
            tbm_add_tuples(a, ta.as_ptr() as ItemPointer, ta.len() as c_int, false);

            // b: pages {10,20}. NB: tbm_intersect is tuple-level, so page 10's
            // tuple must match a's (offset 2) for page 10 to survive the
            // intersection; an offset that differs would (correctly) empty it.
            let b = tbm_create(1024 * 1024, core::ptr::null_mut());
            let tb = [make_tid(10, 2), make_tid(20, 1)];
            tbm_add_tuples(b, tb.as_ptr() as ItemPointer, tb.len() as c_int, false);

            // union of a copy of a with b -> {5,10,20}
            let u = tbm_create(1024 * 1024, core::ptr::null_mut());
            tbm_add_tuples(u, ta.as_ptr() as ItemPointer, ta.len() as c_int, false);
            tbm_union(u, b);
            let upages: Vec<BlockNumber> = drain(u).iter().map(|x| x.0).collect();
            assert_eq!(upages, vec![5, 10, 20]);

            // intersect a fresh copy of a with b -> {10}
            let inter = tbm_create(1024 * 1024, core::ptr::null_mut());
            tbm_add_tuples(inter, ta.as_ptr() as ItemPointer, ta.len() as c_int, false);
            tbm_intersect(inter, b);
            let ipages: Vec<BlockNumber> = drain(inter).iter().map(|x| x.0).collect();
            assert_eq!(ipages, vec![10]);

            tbm_free(a);
            tbm_free(b);
            tbm_free(u);
            tbm_free(inter);
        }
    }

    #[test]
    fn fresh_is_empty() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let tbm = tbm_create(1024 * 1024, core::ptr::null_mut());
            assert!(tbm_is_empty(tbm));
            tbm_free(tbm);
        }
    }
}
