//! src/backend/access/common/tidstore.c
//!
//! TID (ItemPointerData) storage implementation.
//!
//! TidStore is a in-memory data structure to store TIDs (ItemPointerData).
//! Internally it uses a radix tree as the storage for TIDs. The key is the
//! BlockNumber and the value is a bitmap of offsets, BlocktableEntry.
//!
//! TidStore can be shared among parallel worker processes by using
//! TidStoreCreateShared(). Other backends can attach to the shared TidStore
//! by TidStoreAttach().
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/common/tidstore.c

use crate::prelude::*;

use crate::c::*;
use std::ffi::{c_char, c_int, c_void};

use crate::storage::block::BlockNumber;

// ---------------------------------------------------------------------------
// Type aliases / external stub types
// ---------------------------------------------------------------------------

pub type bitmapword = u64; // from nodes/bitmapset.h
pub const BITS_PER_BITMAPWORD: c_int = 64;

pub type OffsetNumber = u16; // from storage/off.h
pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const MaxOffsetNumber: c_int = 2048;

pub const PG_INT8_MAX: c_int = 0x7F;

// ItemPointer from storage/itemptr.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ItemPointerData {
    pub ip_blkid: BlockIdData,
    pub ip_posid: OffsetNumber,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlockIdData {
    pub bi_hi: u16,
    pub bi_lo: u16,
}
pub type ItemPointer = *mut ItemPointerData;

// MemoryContext from utils/palloc.h / nodes/memnodes.h
pub type MemoryContext = *mut c_void;

// DSA types from utils/dsa.h
pub type dsa_area = c_void;
pub type dsa_handle = crate::c::uint32;
pub type dsa_pointer = crate::c::uint64;
pub const DSA_HANDLE_INVALID: dsa_handle = 0;
pub const DSA_DEFAULT_INIT_SEGMENT_SIZE: usize = 1 * 1024 * 1024;
pub const DSA_MAX_SEGMENT_SIZE: usize = 1 << 30;
pub const DSA_MIN_SEGMENT_SIZE: usize = 256 * 1024;

// Memory context default sizes from utils/memutils.h
pub const ALLOCSET_DEFAULT_MINSIZE: usize = 0;
pub const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;
pub const ALLOCSET_DEFAULT_MAXSIZE: usize = 8 * 1024 * 1024;

// Radix tree generated types (lib/radixtree.h templated). Stubbed as opaque.
#[repr(C)]
pub struct local_ts_radix_tree {
    _private: [u8; 0],
}
#[repr(C)]
pub struct shared_ts_radix_tree {
    _private: [u8; 0],
}
#[repr(C)]
pub struct local_ts_iter {
    _private: [u8; 0],
}
#[repr(C)]
pub struct shared_ts_iter {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

#[inline]
fn WORDNUM(x: c_int) -> c_int {
    x / BITS_PER_BITMAPWORD
}
#[inline]
fn BITNUM(x: c_int) -> c_int {
    x % BITS_PER_BITMAPWORD
}

/* number of active words for a page: */
#[inline]
fn WORDS_PER_PAGE(n: c_int) -> c_int {
    n / BITS_PER_BITMAPWORD + 1
}

/* number of offsets we can store in the header of a BlocktableEntry */
pub const NUM_FULL_OFFSETS: usize = (std::mem::size_of::<usize>()
    - std::mem::size_of::<uint8>()
    - std::mem::size_of::<int8>())
    / std::mem::size_of::<OffsetNumber>();

/*
 * This is named similarly to PagetableEntry in tidbitmap.c
 * because the two have a similar function.
 */
#[repr(C)]
pub struct BlocktableEntryHeader {
    /*
     * We need to position this member to reserve space for the backing radix
     * tree to tag the lowest bit when struct 'header' is stored inside a
     * pointer or DSA pointer.
     */
    pub flags: uint8,

    pub nwords: int8,

    /*
     * We can store a small number of offsets here to avoid wasting space with
     * a sparse bitmap.
     */
    pub full_offsets: [OffsetNumber; NUM_FULL_OFFSETS],
}

#[repr(C)]
pub struct BlocktableEntry {
    pub header: BlocktableEntryHeader,

    /*
     * We don't expect any padding space here, but to be cautious, code
     * creating new entries should zero out space up to 'words'.
     */
    pub words: [bitmapword; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * The type of 'nwords' limits the max number of words in the 'words' array.
 * This computes the max offset we can actually store in the bitmap. In
 * practice, it's almost always the same as MaxOffsetNumber.
 */
#[inline]
fn MAX_OFFSET_IN_BITMAP() -> c_int {
    std::cmp::min(BITS_PER_BITMAPWORD * PG_INT8_MAX - 1, MaxOffsetNumber)
}

#[inline]
fn MaxBlocktableEntrySize() -> usize {
    core::mem::offset_of!(BlocktableEntry, words)
        + (std::mem::size_of::<bitmapword>() * WORDS_PER_PAGE(MAX_OFFSET_IN_BITMAP()) as usize)
}

// ---------------------------------------------------------------------------
// TidStore structs
// ---------------------------------------------------------------------------

/* Per-backend state for a TidStore */
#[repr(C)]
pub union TidStoreTree {
    pub local: *mut local_ts_radix_tree,
    pub shared: *mut shared_ts_radix_tree,
}

#[repr(C)]
pub struct TidStore {
    /*
     * MemoryContext for the radix tree when using local memory, NULL for
     * shared memory
     */
    pub rt_context: MemoryContext,

    /* Storage for TIDs. Use either one depending on TidStoreIsShared() */
    pub tree: TidStoreTree,

    /* DSA area for TidStore if using shared memory */
    pub area: *mut dsa_area,
}

#[inline]
unsafe fn TidStoreIsShared(ts: *const TidStore) -> bool {
    !(*ts).area.is_null()
}

/* Iterator for TidStore */
#[repr(C)]
pub union TidStoreIterTree {
    pub shared: *mut shared_ts_iter,
    pub local: *mut local_ts_iter,
}

#[repr(C)]
pub struct TidStoreIter {
    pub ts: *mut TidStore,

    /* iterator of radix tree. Use either one depending on TidStoreIsShared() */
    pub tree_iter: TidStoreIterTree,

    /* output for the caller */
    pub output: TidStoreIterResult,
}

/*
 * Result struct for TidStoreIterateNext.  This is copyable, but should be
 * treated as opaque.  Call TidStoreGetBlockOffsets() to obtain the offsets.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TidStoreIterResult {
    pub blkno: BlockNumber,
    pub internal_page: *mut c_void,
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/*
 * Create a TidStore. The TidStore will live in the memory context that is
 * CurrentMemoryContext at the time of this call. The TID storage, backed
 * by a radix tree, will live in its child memory context, rt_context.
 *
 * "max_bytes" is not an internally-enforced limit; it is used only as a
 * hint to cap the memory block size of the memory context for TID storage.
 * This reduces space wastage due to over-allocation. If the caller wants to
 * monitor memory usage, it must compare its limit with the value reported
 * by TidStoreMemoryUsage().
 */
pub unsafe fn TidStoreCreateLocal(max_bytes: usize, insert_only: bool) -> *mut TidStore {
    let ts: *mut TidStore;
    let initBlockSize: usize = ALLOCSET_DEFAULT_INITSIZE;
    let minContextSize: usize = ALLOCSET_DEFAULT_MINSIZE;
    let mut maxBlockSize: usize = ALLOCSET_DEFAULT_MAXSIZE;

    ts = palloc0(std::mem::size_of::<TidStore>()) as *mut TidStore;

    /* choose the maxBlockSize to be no larger than 1/16 of max_bytes */
    while 16 * maxBlockSize > max_bytes {
        maxBlockSize >>= 1;
    }

    if maxBlockSize < ALLOCSET_DEFAULT_INITSIZE {
        maxBlockSize = ALLOCSET_DEFAULT_INITSIZE;
    }

    /* Create a memory context for the TID storage */
    if insert_only {
        (*ts).rt_context = BumpContextCreate(
            CurrentMemoryContext as MemoryContext,
            c"TID storage".as_ptr(),
            minContextSize,
            initBlockSize,
            maxBlockSize,
        );
    } else {
        (*ts).rt_context = AllocSetContextCreate(
            CurrentMemoryContext as MemoryContext,
            c"TID storage".as_ptr(),
            minContextSize,
            initBlockSize,
            maxBlockSize,
        );
    }

    (*ts).tree.local = local_ts_create((*ts).rt_context);

    ts
}

/*
 * Similar to TidStoreCreateLocal() but create a shared TidStore on a
 * DSA area.
 *
 * The returned object is allocated in backend-local memory.
 */
pub unsafe fn TidStoreCreateShared(max_bytes: usize, tranche_id: c_int) -> *mut TidStore {
    let ts: *mut TidStore;
    let area: *mut dsa_area;
    let mut dsa_init_size: usize = DSA_DEFAULT_INIT_SEGMENT_SIZE;
    let mut dsa_max_size: usize = DSA_MAX_SEGMENT_SIZE;

    ts = palloc0(std::mem::size_of::<TidStore>()) as *mut TidStore;

    /*
     * Choose the initial and maximum DSA segment sizes to be no longer than
     * 1/8 of max_bytes.
     */
    while 8 * dsa_max_size > max_bytes {
        dsa_max_size >>= 1;
    }

    if dsa_max_size < DSA_MIN_SEGMENT_SIZE {
        dsa_max_size = DSA_MIN_SEGMENT_SIZE;
    }

    if dsa_init_size > dsa_max_size {
        dsa_init_size = dsa_max_size;
    }

    area = dsa_create_ext(tranche_id, dsa_init_size, dsa_max_size);
    (*ts).tree.shared = shared_ts_create(area, tranche_id);
    (*ts).area = area;

    ts
}

/*
 * Attach to the shared TidStore. 'area_handle' is the DSA handle where
 * the TidStore is created. 'handle' is the dsa_pointer returned by
 * TidStoreGetHandle(). The returned object is allocated in backend-local
 * memory using the CurrentMemoryContext.
 */
pub unsafe fn TidStoreAttach(area_handle: dsa_handle, handle: dsa_pointer) -> *mut TidStore {
    let ts: *mut TidStore;
    let area: *mut dsa_area;

    Assert!(area_handle != DSA_HANDLE_INVALID);
    Assert!(DsaPointerIsValid(handle));

    /* create per-backend state */
    ts = palloc0(std::mem::size_of::<TidStore>()) as *mut TidStore;

    area = dsa_attach(area_handle);

    /* Find the shared the shared radix tree */
    (*ts).tree.shared = shared_ts_attach(area, handle);
    (*ts).area = area;

    ts
}

/*
 * Detach from a TidStore. This also detaches from radix tree and frees
 * the backend-local resources.
 */
pub unsafe fn TidStoreDetach(ts: *mut TidStore) {
    Assert!(TidStoreIsShared(ts));

    shared_ts_detach((*ts).tree.shared);
    dsa_detach((*ts).area);

    pfree(ts as *mut c_void);
}

/*
 * Lock support functions.
 *
 * We can use the radix tree's lock for shared TidStore as the data we
 * need to protect is only the shared radix tree.
 */

pub unsafe fn TidStoreLockExclusive(ts: *mut TidStore) {
    if TidStoreIsShared(ts) {
        shared_ts_lock_exclusive((*ts).tree.shared);
    }
}

pub unsafe fn TidStoreLockShare(ts: *mut TidStore) {
    if TidStoreIsShared(ts) {
        shared_ts_lock_share((*ts).tree.shared);
    }
}

pub unsafe fn TidStoreUnlock(ts: *mut TidStore) {
    if TidStoreIsShared(ts) {
        shared_ts_unlock((*ts).tree.shared);
    }
}

/*
 * Destroy a TidStore, returning all memory.
 *
 * Note that the caller must be certain that no other backend will attempt to
 * access the TidStore before calling this function. Other backend must
 * explicitly call TidStoreDetach() to free up backend-local memory associated
 * with the TidStore. The backend that calls TidStoreDestroy() must not call
 * TidStoreDetach().
 */
pub unsafe fn TidStoreDestroy(ts: *mut TidStore) {
    /* Destroy underlying radix tree */
    if TidStoreIsShared(ts) {
        shared_ts_free((*ts).tree.shared);
        dsa_detach((*ts).area);
    } else {
        local_ts_free((*ts).tree.local);
        MemoryContextDelete((*ts).rt_context);
    }

    pfree(ts as *mut c_void);
}

/*
 * Create or replace an entry for the given block and array of offsets.
 *
 * NB: This function is designed and optimized for vacuum's heap scanning
 * phase, so has some limitations:
 *
 * - The offset numbers "offsets" must be sorted in ascending order.
 * - If the block number already exists, the entry will be replaced --
 *	 there is no way to add or remove offsets from an entry.
 */
pub unsafe fn TidStoreSetBlockOffsets(
    ts: *mut TidStore,
    blkno: BlockNumber,
    offsets: *mut OffsetNumber,
    num_offsets: c_int,
) {
    // union { char data[MaxBlocktableEntrySize]; BlocktableEntry force_align_entry; }
    // Allocate aligned scratch space the size of MaxBlocktableEntrySize.
    let mut data: Vec<bitmapword> =
        vec![0; (MaxBlocktableEntrySize() + std::mem::size_of::<bitmapword>() - 1) / std::mem::size_of::<bitmapword>()];
    let page: *mut BlocktableEntry = data.as_mut_ptr() as *mut BlocktableEntry;
    let mut word: bitmapword;
    let mut wordnum: c_int;
    let mut next_word_threshold: c_int;
    let mut idx: c_int = 0;

    Assert!(num_offsets > 0);

    /* Check if the given offset numbers are ordered */
    let mut i = 1;
    while i < num_offsets {
        Assert!(*offsets.offset(i as isize) > *offsets.offset((i - 1) as isize));
        i += 1;
    }

    std::ptr::write_bytes(
        page as *mut u8,
        0,
        core::mem::offset_of!(BlocktableEntry, words),
    );

    if num_offsets <= NUM_FULL_OFFSETS as c_int {
        let mut i = 0;
        while i < num_offsets {
            let off: OffsetNumber = *offsets.offset(i as isize);

            /* safety check to ensure we don't overrun bit array bounds */
            if off == InvalidOffsetNumber || off as c_int > MAX_OFFSET_IN_BITMAP() {
                elog!(ERROR, "tuple offset out of range: {}", off);
            }

            (*page).header.full_offsets[i as usize] = off;
            i += 1;
        }

        (*page).header.nwords = 0;
    } else {
        wordnum = 0;
        next_word_threshold = BITS_PER_BITMAPWORD;
        while wordnum <= WORDNUM(*offsets.offset((num_offsets - 1) as isize) as c_int) {
            word = 0;

            while idx < num_offsets {
                let off: OffsetNumber = *offsets.offset(idx as isize);

                /* safety check to ensure we don't overrun bit array bounds */
                if off == InvalidOffsetNumber || off as c_int > MAX_OFFSET_IN_BITMAP() {
                    elog!(ERROR, "tuple offset out of range: {}", off);
                }

                if off as c_int >= next_word_threshold {
                    break;
                }

                word |= (1 as bitmapword) << BITNUM(off as c_int);
                idx += 1;
            }

            /* write out offset bitmap for this wordnum */
            *(*page).words.as_mut_ptr().offset(wordnum as isize) = word;

            wordnum += 1;
            next_word_threshold += BITS_PER_BITMAPWORD;
        }

        (*page).header.nwords = wordnum as int8;
        Assert!(
            (*page).header.nwords as c_int
                == WORDS_PER_PAGE(*offsets.offset((num_offsets - 1) as isize) as c_int)
        );
    }

    if TidStoreIsShared(ts) {
        shared_ts_set((*ts).tree.shared, blkno as u64, page);
    } else {
        local_ts_set((*ts).tree.local, blkno as u64, page);
    }
}

/* Return true if the given TID is present in the TidStore */
pub unsafe fn TidStoreIsMember(ts: *mut TidStore, tid: ItemPointer) -> bool {
    let wordnum: c_int;
    let bitnum: c_int;
    let page: *mut BlocktableEntry;
    let blk: BlockNumber = ItemPointerGetBlockNumber(tid);
    let off: OffsetNumber = ItemPointerGetOffsetNumber(tid);

    if TidStoreIsShared(ts) {
        page = shared_ts_find((*ts).tree.shared, blk as u64);
    } else {
        page = local_ts_find((*ts).tree.local, blk as u64);
    }

    /* no entry for the blk */
    if page.is_null() {
        return false;
    }

    if (*page).header.nwords == 0 {
        /* we have offsets in the header */
        for i in 0..NUM_FULL_OFFSETS {
            if (*page).header.full_offsets[i] == off {
                return true;
            }
        }
        false
    } else {
        wordnum = WORDNUM(off as c_int);
        bitnum = BITNUM(off as c_int);

        /* no bitmap for the off */
        if wordnum >= (*page).header.nwords as c_int {
            return false;
        }

        (*(*page).words.as_ptr().offset(wordnum as isize) & ((1 as bitmapword) << bitnum)) != 0
    }
}

/*
 * Prepare to iterate through a TidStore.
 *
 * The TidStoreIter struct is created in the caller's memory context, and it
 * will be freed in TidStoreEndIterate.
 *
 * The caller is responsible for locking TidStore until the iteration is
 * finished.
 */
pub unsafe fn TidStoreBeginIterate(ts: *mut TidStore) -> *mut TidStoreIter {
    let iter: *mut TidStoreIter;

    iter = palloc0(std::mem::size_of::<TidStoreIter>()) as *mut TidStoreIter;
    (*iter).ts = ts;

    if TidStoreIsShared(ts) {
        (*iter).tree_iter.shared = shared_ts_begin_iterate((*ts).tree.shared);
    } else {
        (*iter).tree_iter.local = local_ts_begin_iterate((*ts).tree.local);
    }

    iter
}

/*
 * Return a result that contains the next block number and that can be used to
 * obtain the set of offsets by calling TidStoreGetBlockOffsets().  The result
 * is copyable.
 */
pub unsafe fn TidStoreIterateNext(iter: *mut TidStoreIter) -> *mut TidStoreIterResult {
    let mut key: uint64 = 0;
    let page: *mut BlocktableEntry;

    if TidStoreIsShared((*iter).ts) {
        page = shared_ts_iterate_next((*iter).tree_iter.shared, &mut key);
    } else {
        page = local_ts_iterate_next((*iter).tree_iter.local, &mut key);
    }

    if page.is_null() {
        return std::ptr::null_mut();
    }

    (*iter).output.blkno = key as BlockNumber;
    (*iter).output.internal_page = page as *mut c_void;

    &mut (*iter).output
}

/*
 * Finish the iteration on TidStore.
 *
 * The caller is responsible for releasing any locks.
 */
pub unsafe fn TidStoreEndIterate(iter: *mut TidStoreIter) {
    if TidStoreIsShared((*iter).ts) {
        shared_ts_end_iterate((*iter).tree_iter.shared);
    } else {
        local_ts_end_iterate((*iter).tree_iter.local);
    }

    pfree(iter as *mut c_void);
}

/*
 * Return the memory usage of TidStore.
 */
pub unsafe fn TidStoreMemoryUsage(ts: *mut TidStore) -> usize {
    if TidStoreIsShared(ts) {
        shared_ts_memory_usage((*ts).tree.shared)
    } else {
        local_ts_memory_usage((*ts).tree.local)
    }
}

/*
 * Return the DSA area where the TidStore lives.
 */
pub unsafe fn TidStoreGetDSA(ts: *mut TidStore) -> *mut dsa_area {
    Assert!(TidStoreIsShared(ts));

    (*ts).area
}

pub unsafe fn TidStoreGetHandle(ts: *mut TidStore) -> dsa_pointer {
    Assert!(TidStoreIsShared(ts));

    shared_ts_get_handle((*ts).tree.shared) as dsa_pointer
}

/*
 * Given a TidStoreIterResult returned by TidStoreIterateNext(), extract the
 * offset numbers.  Returns the number of offsets filled in, if <=
 * max_offsets.  Otherwise, fills in as much as it can in the given space, and
 * returns the size of the buffer that would be needed.
 */
pub unsafe fn TidStoreGetBlockOffsets(
    result: *mut TidStoreIterResult,
    offsets: *mut OffsetNumber,
    max_offsets: c_int,
) -> c_int {
    let page: *mut BlocktableEntry = (*result).internal_page as *mut BlocktableEntry;
    let mut num_offsets: c_int = 0;
    let mut wordnum: c_int;

    if (*page).header.nwords == 0 {
        /* we have offsets in the header */
        for i in 0..NUM_FULL_OFFSETS {
            if (*page).header.full_offsets[i] != InvalidOffsetNumber {
                if num_offsets < max_offsets {
                    *offsets.offset(num_offsets as isize) = (*page).header.full_offsets[i];
                }
                num_offsets += 1;
            }
        }
    } else {
        wordnum = 0;
        while wordnum < (*page).header.nwords as c_int {
            let mut w: bitmapword = *(*page).words.as_ptr().offset(wordnum as isize);
            let mut off: c_int = wordnum * BITS_PER_BITMAPWORD;

            while w != 0 {
                if w & 1 != 0 {
                    if num_offsets < max_offsets {
                        *offsets.offset(num_offsets as isize) = off as OffsetNumber;
                    }
                    num_offsets += 1;
                }
                off += 1;
                w >>= 1;
            }
            wordnum += 1;
        }
    }

    num_offsets
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers
// ---------------------------------------------------------------------------

#[inline]
unsafe fn DsaPointerIsValid(p: dsa_pointer) -> bool {
    p != 0
}

// storage/itemptr.h
unsafe fn ItemPointerGetBlockNumber(_pointer: ItemPointer) -> BlockNumber { crate::storage::itemptr::ItemPointerGetBlockNumber(_pointer as _) }
unsafe fn ItemPointerGetOffsetNumber(_pointer: ItemPointer) -> OffsetNumber { crate::storage::itemptr::ItemPointerGetOffsetNumber(_pointer as _) }

// utils/memutils.h
unsafe fn BumpContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _minContextSize: usize,
    _initBlockSize: usize,
    _maxBlockSize: usize,
) -> MemoryContext { unimplemented!() }
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _minContextSize: usize,
    _initBlockSize: usize,
    _maxBlockSize: usize,
) -> MemoryContext { crate::backend_link_shims::AllocSetContextCreate(_parent as _, _name as _, _minContextSize, _initBlockSize, _maxBlockSize) as _ }
unsafe fn MemoryContextDelete(_context: MemoryContext) {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}

// utils/dsa.h
unsafe fn dsa_create_ext(
    _tranche_id: c_int,
    _init_segment_size: usize,
    _max_segment_size: usize,
) -> *mut dsa_area { unimplemented!() }
unsafe fn dsa_attach(_handle: dsa_handle) -> *mut dsa_area { unimplemented!() }
unsafe fn dsa_detach(_area: *mut dsa_area) { unimplemented!() }

// lib/radixtree.h (templated local_ts_* / shared_ts_*)
unsafe fn local_ts_create(_ctx: MemoryContext) -> *mut local_ts_radix_tree {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_free(_tree: *mut local_ts_radix_tree) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_set(
    _tree: *mut local_ts_radix_tree,
    _key: u64,
    _value: *mut BlocktableEntry,
) -> bool {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_find(_tree: *mut local_ts_radix_tree, _key: u64) -> *mut BlocktableEntry {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_begin_iterate(_tree: *mut local_ts_radix_tree) -> *mut local_ts_iter {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_iterate_next(
    _iter: *mut local_ts_iter,
    _key_p: *mut u64,
) -> *mut BlocktableEntry {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_end_iterate(_iter: *mut local_ts_iter) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn local_ts_memory_usage(_tree: *mut local_ts_radix_tree) -> usize {
    unimplemented!() // TODO: lib/radixtree.h
}

unsafe fn shared_ts_create(_area: *mut dsa_area, _tranche_id: c_int) -> *mut shared_ts_radix_tree {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_attach(
    _area: *mut dsa_area,
    _handle: dsa_pointer,
) -> *mut shared_ts_radix_tree {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_detach(_tree: *mut shared_ts_radix_tree) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_free(_tree: *mut shared_ts_radix_tree) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_lock_exclusive(_tree: *mut shared_ts_radix_tree) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_lock_share(_tree: *mut shared_ts_radix_tree) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_unlock(_tree: *mut shared_ts_radix_tree) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_set(
    _tree: *mut shared_ts_radix_tree,
    _key: u64,
    _value: *mut BlocktableEntry,
) -> bool {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_find(_tree: *mut shared_ts_radix_tree, _key: u64) -> *mut BlocktableEntry {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_begin_iterate(_tree: *mut shared_ts_radix_tree) -> *mut shared_ts_iter {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_iterate_next(
    _iter: *mut shared_ts_iter,
    _key_p: *mut u64,
) -> *mut BlocktableEntry {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_end_iterate(_iter: *mut shared_ts_iter) {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_memory_usage(_tree: *mut shared_ts_radix_tree) -> usize {
    unimplemented!() // TODO: lib/radixtree.h
}
unsafe fn shared_ts_get_handle(_tree: *mut shared_ts_radix_tree) -> dsa_pointer {
    unimplemented!() // TODO: lib/radixtree.h
}
