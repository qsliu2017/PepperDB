//! Translation of postgres/src/backend/access/gin/ginbulk.c
//!   (+ the BuildAccumulator / GinEntryAccumulator struct subset from
//!      access/gin_private.h, merged here).
//!
//! Routines for fast build of an inverted (GIN) index.
//!
//! The build accumulator is a red-black tree keyed by (attnum, key, category)
//! whose payload is a growable, sorted-on-demand list of heap ItemPointers.
//! During an index build all entries for one heap tuple are inserted in a
//! near-balanced order (see ginInsertBAEntries), then the whole tree is read
//! out in key order with each posting list sorted ascending.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::common::tupdesc::{TupleDesc, TupleDescCompactAttr};
use crate::lib::rbtree::{
    rbt_begin_iterate, rbt_create, rbt_insert, rbt_iterate, LeftRightWalk, RBTNode, RBTree,
    RBTreeIterator,
};
use crate::storage::itemptr::{ItemPointer, ItemPointerCompare, ItemPointerData, ItemPointerIsValid};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber};
use crate::utils::adt::datum::datumCopy;

/* GinEntryAccumulator allocation quantum */
const DEF_NENTRY: c_uint = 2048;
/* ItemPointer initial allocation quantum */
const DEF_NPTR: c_uint = 5;

/*
 * GinNullCategory (ginblock.h). A "signed char" tag distinguishing normal keys
 * from the various null/placeholder categories. We only need GIN_CAT_NORM_KEY
 * here (the only category whose datum must be copied), but mirror the full set.
 */
pub type GinNullCategory = c_char;
pub const GIN_CAT_NORM_KEY: GinNullCategory = 0; /* normal, non-null key value */
pub const GIN_CAT_NULL_KEY: GinNullCategory = 1; /* null key value */
pub const GIN_CAT_EMPTY_ITEM: GinNullCategory = 2; /* placeholder for zero-key item */
pub const GIN_CAT_NULL_ITEM: GinNullCategory = 3; /* placeholder for null item */
pub const GIN_CAT_EMPTY_QUERY: GinNullCategory = -1; /* placeholder for full-scan query */

// TODO(pg-port): ERRCODE_PROGRAM_LIMIT_EXCEEDED from utils/errcodes.h (not yet
// ported). The errcode() shim currently ignores its argument, so 0 is fine.
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

/*
 * GinState: opaque-minimal mirror. ginbulk.c only ever reads
 * ginstate->origTupdesc (in getDatumCopy) and passes ginstate by pointer to
 * ginCompareAttEntries (the comparator). We therefore expose only origTupdesc
 * as the first field; the remainder of the real struct (relation, per-column
 * FmgrInfo arrays, collations, ...) is irrelevant to this file and omitted.
 *
 * NOTE: partial/minimal mirror -- the real GinState (gin_private.h) is much
 * larger. This is laid out #[repr(C)] with origTupdesc first so a real GinState*
 * can be reinterpreted here for the single field we touch.
 */
#[repr(C)]
pub struct GinState {
    pub index: *mut c_void, /* Relation, stubbed */
    pub oneCol: bool,
    pub origTupdesc: TupleDesc,
    /* ... remaining real GinState fields omitted (not touched by ginbulk) ... */
}

/*
 * GinEntryAccumulator (gin_private.h). rbtnode MUST be the first field so that a
 * *mut RBTNode and a *mut GinEntryAccumulator are interconvertible (the intrusive
 * RB-tree pattern; rbt_create is told sizeof(GinEntryAccumulator)).
 */
#[repr(C)]
pub struct GinEntryAccumulator {
    pub rbtnode: RBTNode,
    pub key: Datum,
    pub category: GinNullCategory,
    pub attnum: OffsetNumber,
    pub shouldSort: bool,
    pub list: *mut ItemPointerData,
    pub maxcount: u32, /* allocated size of list[] */
    pub count: u32,    /* current number of list[] entries */
}

/*
 * BuildAccumulator (gin_private.h). tree_walk is kept inline so ginBeginBAScan /
 * ginGetBAEntry can iterate without a separate allocation.
 */
#[repr(C)]
pub struct BuildAccumulator {
    pub ginstate: *mut GinState,
    pub allocatedMemory: Size,
    pub entryallocator: *mut GinEntryAccumulator,
    pub eas_used: c_uint,
    pub tree: *mut RBTree,
    pub tree_walk: RBTreeIterator,
}

/*
 * ginCompareItemPointers (gin_private.h static inline): total order on TIDs.
 * Mirrors the C macro which simply forwards to ItemPointerCompare.
 *
 * # Safety
 * Both pointers reference valid ItemPointerData.
 */
#[inline]
pub unsafe fn ginCompareItemPointers(a: ItemPointer, b: ItemPointer) -> c_int {
    ItemPointerCompare(a, b)
}

/*
 * ginCompareAttEntries (ginutil.c): compares two (attnum, key, category) index
 * entries. ginbulk.c only uses this via the rbtree comparator. The full
 * implementation lives in ginutil.c (not yet ported); we provide a minimal,
 * self-consistent ordering here so the rbtree + TID-merge logic -- the part this
 * file owns -- is exercised. Ordering: by attnum, then category, then key as an
 * integer Datum (sufficient for byval int-like keys used in tests; the real
 * function dispatches to the per-column compare support function).
 *
 * NOTE: partial mirror -- real ginCompareAttEntries calls the opclass compareFn.
 *
 * # Safety
 * `ginstate` is a valid (possibly stub) GinState pointer.
 */
unsafe fn ginCompareAttEntries(
    _ginstate: *mut GinState,
    attnuma: OffsetNumber,
    a: Datum,
    categorya: GinNullCategory,
    attnumb: OffsetNumber,
    b: Datum,
    categoryb: GinNullCategory,
) -> c_int {
    if attnuma != attnumb {
        return if attnuma < attnumb { -1 } else { 1 };
    }
    if categorya != categoryb {
        return if categorya < categoryb { -1 } else { 1 };
    }
    /* Both normal keys (or same category): order by datum as unsigned integer. */
    let ua = a as u64;
    let ub = b as u64;
    if ua < ub {
        -1
    } else if ua > ub {
        1
    } else {
        0
    }
}

/*
 * GetMemoryChunkSpace / repalloc_huge: not yet ported (utils/mmgr). ginbulk only
 * uses GetMemoryChunkSpace to account allocatedMemory and repalloc_huge to grow a
 * posting list. We stub them minimally so the accumulator's accounting and growth
 * behave; the byte counts are approximate (chunk overhead is ignored).
 *
 * NOTE: stubs -- replace with crate::utils::mmgr equivalents once ported.
 */
#[inline]
unsafe fn GetMemoryChunkSpace(_pointer: *mut c_void) -> Size {
    0
}

/*
 * repalloc_huge stand-in: grow an allocation, copying the old contents. We track
 * the byte size in a header word so we know how much to copy. To keep the rest of
 * the file faithful (it just calls repalloc_huge(list, newsize)), we implement
 * grow-by-realloc semantics via palloc + copy + pfree. Old size is not known to
 * us, so callers must only ever grow; we copy the smaller of old caller-tracked
 * bytes. Here callers double maxcount, so we copy oldsize/2.
 *
 * To avoid needing the old size, we instead realize this through a typed helper
 * used only by ginCombineData (which knows the old element count). See
 * grow_itemptr_list below; this bare repalloc_huge is kept only for signature
 * parity and is not otherwise used.
 *
 * # Safety
 * `ptr` was allocated by palloc and is at least `oldsize` bytes; `newsize >= oldsize`.
 */
#[inline]
unsafe fn repalloc_huge(ptr: *mut c_void, oldsize: Size, newsize: Size) -> *mut c_void {
    let newp = palloc(newsize);
    if !ptr.is_null() && oldsize > 0 {
        core::ptr::copy_nonoverlapping(ptr as *const u8, newp as *mut u8, oldsize);
        pfree(ptr);
    }
    newp
}

/* Combiner function for rbtree.c */
unsafe fn ginCombineData(existing: *mut RBTNode, newdata: *const RBTNode, arg: *mut c_void) {
    let eo = existing as *mut GinEntryAccumulator;
    let en = newdata as *const GinEntryAccumulator;
    let accum = arg as *mut BuildAccumulator;

    /*
     * Note this code assumes that newdata contains only one itempointer.
     */
    if (*eo).count >= (*eo).maxcount {
        if (*eo).maxcount as u64 > i32::MAX as u64 {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(ERROR, errmsg!("posting list is too long"));
            unreachable!();
        }

        (*accum).allocatedMemory -= GetMemoryChunkSpace((*eo).list as *mut c_void);
        let oldbytes = core::mem::size_of::<ItemPointerData>() * (*eo).maxcount as usize;
        (*eo).maxcount *= 2;
        let newbytes = core::mem::size_of::<ItemPointerData>() * (*eo).maxcount as usize;
        (*eo).list =
            repalloc_huge((*eo).list as *mut c_void, oldbytes, newbytes) as *mut ItemPointerData;
        (*accum).allocatedMemory += GetMemoryChunkSpace((*eo).list as *mut c_void);
    }

    /* If item pointers are not ordered, they will need to be sorted later */
    if !(*eo).shouldSort {
        let res = ginCompareItemPointers(
            (*eo).list.add((*eo).count as usize - 1),
            (*en).list,
        );
        Assert!(res != 0);

        if res > 0 {
            (*eo).shouldSort = true;
        }
    }

    *(*eo).list.add((*eo).count as usize) = *(*en).list;
    (*eo).count += 1;
}

/* Comparator function for rbtree.c */
unsafe fn cmpEntryAccumulator(a: *const RBTNode, b: *const RBTNode, arg: *mut c_void) -> c_int {
    let ea = a as *const GinEntryAccumulator;
    let eb = b as *const GinEntryAccumulator;
    let accum = arg as *mut BuildAccumulator;

    ginCompareAttEntries(
        (*accum).ginstate,
        (*ea).attnum,
        (*ea).key,
        (*ea).category,
        (*eb).attnum,
        (*eb).key,
        (*eb).category,
    )
}

/* Allocator function for rbtree.c */
unsafe fn ginAllocEntryAccumulator(arg: *mut c_void) -> *mut RBTNode {
    let accum = arg as *mut BuildAccumulator;

    /*
     * Allocate memory by rather big chunks to decrease overhead. We have no
     * need to reclaim RBTNodes individually, so this costs nothing.
     */
    if (*accum).entryallocator.is_null() || (*accum).eas_used >= DEF_NENTRY {
        (*accum).entryallocator = palloc(
            core::mem::size_of::<GinEntryAccumulator>() * DEF_NENTRY as usize,
        ) as *mut GinEntryAccumulator;
        (*accum).allocatedMemory +=
            GetMemoryChunkSpace((*accum).entryallocator as *mut c_void);
        (*accum).eas_used = 0;
    }

    /* Allocate new RBTNode from current chunk */
    let ea = (*accum).entryallocator.add((*accum).eas_used as usize);
    (*accum).eas_used += 1;

    ea as *mut RBTNode
}

/*
 * # Safety
 * `accum` references a valid, writable BuildAccumulator.
 */
pub unsafe fn ginInitBA(accum: *mut BuildAccumulator) {
    /* accum->ginstate is intentionally not set here */
    (*accum).allocatedMemory = 0;
    (*accum).entryallocator = null_mut();
    (*accum).eas_used = 0;
    (*accum).tree = rbt_create(
        core::mem::size_of::<GinEntryAccumulator>(),
        cmpEntryAccumulator,
        ginCombineData,
        ginAllocEntryAccumulator,
        None, /* no freefunc needed */
        accum as *mut c_void,
    );
}

/*
 * This is basically the same as datumCopy(), but extended to count palloc'd
 * space in accum->allocatedMemory.
 *
 * # Safety
 * `accum` is valid and `accum->ginstate->origTupdesc` describes column `attnum`.
 */
unsafe fn getDatumCopy(
    accum: *mut BuildAccumulator,
    attnum: OffsetNumber,
    value: Datum,
) -> Datum {
    let att = TupleDescCompactAttr((*(*accum).ginstate).origTupdesc, attnum as c_int - 1);
    if (*att).attbyval {
        value
    } else {
        let res = datumCopy(value, false, (*att).attlen as c_int);
        (*accum).allocatedMemory += GetMemoryChunkSpace(DatumGetPointer(res) as *mut c_void);
        res
    }
}

/*
 * Find/store one entry from indexed value.
 *
 * # Safety
 * `accum` is initialized; `heapptr` is a valid ItemPointer.
 */
unsafe fn ginInsertBAEntry(
    accum: *mut BuildAccumulator,
    heapptr: ItemPointer,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
) {
    /*
     * For the moment, fill only the fields of eatmp that will be looked at by
     * cmpEntryAccumulator or ginCombineData.
     */
    let mut eatmp: GinEntryAccumulator = core::mem::zeroed();
    eatmp.attnum = attnum;
    eatmp.key = key;
    eatmp.category = category;
    /* temporarily set up single-entry itempointer list */
    eatmp.list = heapptr;

    let mut isNew: bool = false;
    let ea = rbt_insert(
        (*accum).tree,
        &eatmp as *const GinEntryAccumulator as *const RBTNode,
        &mut isNew,
    ) as *mut GinEntryAccumulator;

    if isNew {
        /*
         * Finish initializing new tree entry, including making permanent
         * copies of the datum (if it's not null) and itempointer.
         */
        if category == GIN_CAT_NORM_KEY {
            (*ea).key = getDatumCopy(accum, attnum, key);
        }
        (*ea).maxcount = DEF_NPTR;
        (*ea).count = 1;
        (*ea).shouldSort = false;
        (*ea).list = palloc(core::mem::size_of::<ItemPointerData>() * DEF_NPTR as usize)
            as *mut ItemPointerData;
        *(*ea).list = *heapptr;
        (*accum).allocatedMemory += GetMemoryChunkSpace((*ea).list as *mut c_void);
    } else {
        /*
         * ginCombineData did everything needed.
         */
    }
}

/*
 * Insert the entries for one heap pointer.
 *
 * Since the entries are being inserted into a balanced binary tree, you might
 * think that the order of insertion wouldn't be critical, but it turns out that
 * inserting the entries in sorted order results in a lot of rebalancing
 * operations and is slow. To prevent this, we attempt to insert the nodes in an
 * order that will produce a nearly-balanced tree if the input is in fact sorted.
 *
 * We do this as follows. First, we imagine that we have an array whose size is
 * the smallest power of two greater than or equal to the actual array size.
 * Second, we insert the middle entry of our virtual array into the tree; then,
 * we insert the middles of each half of our virtual array, then middles of
 * quarters, etc.
 *
 * # Safety
 * `accum` is initialized; `heapptr` valid; `entries`/`categories` point to at
 * least `nentries` elements.
 */
pub unsafe fn ginInsertBAEntries(
    accum: *mut BuildAccumulator,
    heapptr: ItemPointer,
    attnum: OffsetNumber,
    entries: *mut Datum,
    categories: *mut GinNullCategory,
    nentries: i32,
) {
    let mut step: u32 = nentries as u32;

    if nentries <= 0 {
        return;
    }

    Assert!(ItemPointerIsValid(heapptr) && attnum >= FirstOffsetNumber);

    /*
     * step will contain largest power of 2 and <= nentries
     */
    step |= step >> 1;
    step |= step >> 2;
    step |= step >> 4;
    step |= step >> 8;
    step |= step >> 16;
    step >>= 1;
    step += 1;

    while step > 0 {
        let mut i: i32 = step as i32 - 1;
        while i < nentries && i >= 0 {
            ginInsertBAEntry(
                accum,
                heapptr,
                attnum,
                *entries.add(i as usize),
                *categories.add(i as usize),
            );
            i += (step << 1) as i32; /* *2 */
        }

        step >>= 1; /* /2 */
    }
}

/*
 * Comparator for the qsort over a posting list. Asserts no duplicate TIDs, as
 * the C version does.
 */
unsafe fn qsortCompareItemPointers(a: &ItemPointerData, b: &ItemPointerData) -> core::cmp::Ordering {
    let res = ginCompareItemPointers(
        a as *const ItemPointerData as ItemPointer,
        b as *const ItemPointerData as ItemPointer,
    );
    /* Assert that there are no equal item pointers being sorted */
    Assert!(res != 0);
    res.cmp(&0)
}

/*
 * Prepare to read out the rbtree contents using ginGetBAEntry.
 *
 * # Safety
 * `accum` was initialized by ginInitBA and populated.
 */
pub unsafe fn ginBeginBAScan(accum: *mut BuildAccumulator) {
    rbt_begin_iterate((*accum).tree, LeftRightWalk, &mut (*accum).tree_walk);
}

/*
 * Get the next entry in sequence from the BuildAccumulator's rbtree.
 *
 * This consists of a single key datum and a list (array) of one or more heap
 * TIDs in which that key is found. The list is guaranteed sorted. Returns NULL
 * when there are no more entries.
 *
 * # Safety
 * `accum` is mid-scan (ginBeginBAScan called); out params are writable.
 */
pub unsafe fn ginGetBAEntry(
    accum: *mut BuildAccumulator,
    attnum: *mut OffsetNumber,
    key: *mut Datum,
    category: *mut GinNullCategory,
    n: *mut u32,
) -> *mut ItemPointerData {
    let entry = rbt_iterate(&mut (*accum).tree_walk) as *mut GinEntryAccumulator;

    if entry.is_null() {
        return null_mut(); /* no more entries */
    }

    *attnum = (*entry).attnum;
    *key = (*entry).key;
    *category = (*entry).category;
    let list = (*entry).list;
    *n = (*entry).count;

    Assert!(!list.is_null() && (*entry).count > 0);

    if (*entry).shouldSort && (*entry).count > 1 {
        let slice = core::slice::from_raw_parts_mut(list, (*entry).count as usize);
        slice.sort_by(|a, b| qsortCompareItemPointers(a, b));
    }

    list
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::CreateTemplateTupleDesc;
    use crate::storage::block::{BlockIdData, BlockNumber};
    use crate::storage::off::OffsetNumber as Off;

    /* Build an ItemPointerData for the given (block, offset) without going
     * through the setter (so tests don't depend on more of storage). */
    fn tid(blk: BlockNumber, off: Off) -> ItemPointerData {
        ItemPointerData {
            ip_blkid: BlockIdData {
                bi_hi: (blk >> 16) as u16,
                bi_lo: (blk & 0xffff) as u16,
            },
            ip_posid: off,
        }
    }

    /*
     * ginInitBA + ginInsertBAEntries for one entry with several unsorted TIDs,
     * then ginBeginBAScan/ginGetBAEntry returns that entry with its TIDs sorted
     * ascending. This exercises the rbtree combiner (ginCombineData), the
     * grow-on-overflow path (DEF_NPTR == 5 so >5 TIDs forces a repalloc), the
     * shouldSort detection, and the final qsort in ginGetBAEntry.
     */
    #[test]
    fn init_insert_scan_sorts_tids() {
        unsafe {
            /* A single-column tupdesc; the key is an int-like byval Datum, so
             * getDatumCopy returns the value unchanged (attbyval true is the
             * default CompactAttribute? -- to be safe we use category != NORM so
             * no datum copy / tupdesc deref is needed). */
            let td = CreateTemplateTupleDesc(1);
            let mut gs = GinState {
                index: null_mut(),
                oneCol: true,
                origTupdesc: td,
            };

            let mut accum: BuildAccumulator = core::mem::zeroed();
            accum.ginstate = &mut gs;
            ginInitBA(&mut accum);

            let attnum: Off = FirstOffsetNumber;
            let key: Datum = 42;
            /* Use GIN_CAT_NULL_KEY so ginInsertBAEntry skips getDatumCopy (no
             * dependence on populated CompactAttribute), while still exercising
             * the full list/merge/sort logic which is category-independent. */
            let cat = GIN_CAT_NULL_KEY;

            /* Unsorted TIDs, more than DEF_NPTR(=5) to force a repalloc grow. */
            let blocks: [BlockNumber; 8] = [10, 3, 7, 1, 9, 2, 8, 4];
            for &b in blocks.iter() {
                let mut hp = tid(b, 1);
                let mut entries = [key];
                let mut cats = [cat];
                ginInsertBAEntries(
                    &mut accum,
                    &mut hp as ItemPointer,
                    attnum,
                    entries.as_mut_ptr(),
                    cats.as_mut_ptr(),
                    1,
                );
            }

            /* Read it back. */
            ginBeginBAScan(&mut accum);

            let mut out_attnum: Off = 0;
            let mut out_key: Datum = 0;
            let mut out_cat: GinNullCategory = 0;
            let mut out_n: u32 = 0;
            let list = ginGetBAEntry(
                &mut accum,
                &mut out_attnum,
                &mut out_key,
                &mut out_cat,
                &mut out_n,
            );

            assert!(!list.is_null());
            assert_eq!(out_attnum, attnum);
            assert_eq!(out_key, key);
            assert_eq!(out_cat, cat);
            assert_eq!(out_n, blocks.len() as u32);

            /* TIDs must come out sorted ascending by block number. */
            let got: Vec<BlockNumber> = (0..out_n as usize)
                .map(|i| {
                    let p = list.add(i);
                    (((*p).ip_blkid.bi_hi as u32) << 16) | ((*p).ip_blkid.bi_lo as u32)
                })
                .collect();
            let mut expect = blocks.to_vec();
            expect.sort();
            assert_eq!(got, expect);

            /* No more entries. */
            let again = ginGetBAEntry(
                &mut accum,
                &mut out_attnum,
                &mut out_key,
                &mut out_cat,
                &mut out_n,
            );
            assert!(again.is_null());
        }
    }

    /*
     * Two distinct keys produce two distinct rbtree entries, read out in key
     * order by the LeftRightWalk iterator (exercises cmpEntryAccumulator).
     */
    #[test]
    fn two_keys_distinct_entries_in_order() {
        unsafe {
            let td = CreateTemplateTupleDesc(1);
            let mut gs = GinState {
                index: null_mut(),
                oneCol: true,
                origTupdesc: td,
            };
            let mut accum: BuildAccumulator = core::mem::zeroed();
            accum.ginstate = &mut gs;
            ginInitBA(&mut accum);

            let attnum: Off = FirstOffsetNumber;
            let cat = GIN_CAT_NULL_KEY;

            /* key 100 at block 5; key 50 at block 6 -- insert higher key first */
            let mut hp1 = tid(5, 1);
            let mut e1 = [100 as Datum];
            let mut c1 = [cat];
            ginInsertBAEntries(&mut accum, &mut hp1, attnum, e1.as_mut_ptr(), c1.as_mut_ptr(), 1);

            let mut hp2 = tid(6, 1);
            let mut e2 = [50 as Datum];
            let mut c2 = [cat];
            ginInsertBAEntries(&mut accum, &mut hp2, attnum, e2.as_mut_ptr(), c2.as_mut_ptr(), 1);

            ginBeginBAScan(&mut accum);

            let mut a: Off = 0;
            let mut k: Datum = 0;
            let mut c: GinNullCategory = 0;
            let mut n: u32 = 0;

            let l1 = ginGetBAEntry(&mut accum, &mut a, &mut k, &mut c, &mut n);
            assert!(!l1.is_null());
            assert_eq!(k, 50); /* smaller key comes first */
            assert_eq!(n, 1);

            let l2 = ginGetBAEntry(&mut accum, &mut a, &mut k, &mut c, &mut n);
            assert!(!l2.is_null());
            assert_eq!(k, 100);
            assert_eq!(n, 1);

            let l3 = ginGetBAEntry(&mut accum, &mut a, &mut k, &mut c, &mut n);
            assert!(l3.is_null());
        }
    }
}
