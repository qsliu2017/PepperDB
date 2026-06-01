//! Translation of postgres/src/backend/utils/mmgr/freepage.c
//!
//! Management of free memory pages.
//!
//! The intention of this code is to provide infrastructure for memory
//! allocators written specifically for PostgreSQL.  At least in the case
//! of dynamic shared memory, we can't simply use malloc() or even
//! relatively thin wrappers like palloc() which sit on top of it, because
//! no allocator built into the operating system will deal with relative
//! pointers.  In the future, we may find other cases in which greater
//! control over our own memory management seems desirable.
//!
//! A FreePageManager keeps track of which 4kB pages of memory are currently
//! unused from the point of view of some higher-level memory allocator.
//! Unlike a user-facing allocator such as palloc(), a FreePageManager can
//! only allocate and free in units of whole pages, and freeing an
//! allocation can only be done given knowledge of its length in pages.
//!
//! Since a free page manager has only a fixed amount of dedicated memory,
//! and since there is no underlying allocator, it uses the free pages
//! it is given to manage to store its bookkeeping data.  It keeps multiple
//! freelists of runs of pages, sorted by the size of the run; the head of
//! each freelist is stored in the FreePageManager itself, and the first
//! page of each run contains a relative pointer to the next run. See
//! FreePageManagerGetInternal for more details on how the freelists are
//! managed.
//!
//! To avoid memory fragmentation, it's important to consolidate adjacent
//! spans of pages whenever possible; otherwise, large allocation requests
//! might not be satisfied even when sufficient contiguous space is
//! available.  Therefore, in addition to the freelists, we maintain an
//! in-memory btree of free page ranges ordered by page number.  If a
//! range being freed precedes or follows a range that is already free,
//! the existing range is extended; if it exactly bridges the gap between
//! free ranges, then the two existing ranges are consolidated with the
//! newly-freed range to form one great big range of free pages.
//!
//! When there is only one range of free pages, the btree is trivial and
//! is stored within the FreePageManager proper; otherwise, pages are
//! allocated from the area under management as needed.  Even in cases
//! where memory fragmentation is very severe, only a tiny fraction of
//! the pages under management are consumed by this btree.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

// c.h: Size, bool, Min/Max.
use crate::c::{Min, Size};
// utils/elog.h: FATAL log level for elog!.
use crate::utils::elog::FATAL;
// miscadmin.h: check_stack_depth().
use crate::miscadmin::check_stack_depth;
// lib/stringinfo.h: StringInfo + append/init helpers.
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfo, StringInfoData,
};
// utils/relptr.h: relative-pointer type + access/store/copy helpers.
use crate::utils::relptr::{relptr, relptr_access, relptr_copy, relptr_is_null, relptr_offset, relptr_store};
use core::ffi::c_char;
use core::ptr;
// `Assert!`, `elog!`, `appendStringInfo!` are brought into scope crate-wide via #[macro_use].

// Magic numbers to identify various page types
const FREE_PAGE_SPAN_LEADER_MAGIC: i32 = 0xea4020f0u32 as i32;
const FREE_PAGE_LEAF_MAGIC: i32 = 0x98eae728u32 as i32;
const FREE_PAGE_INTERNAL_MAGIC: i32 = 0x19aa32c9u32 as i32;

// freepage.h: PostgreSQL normally uses 8kB pages for most things, but many
// common architecture/operating system pairings use a 4kB page size for memory
// allocation, so we do that here also.
pub const FPM_PAGE_SIZE: Size = 4096;

// freepage.h: Each freelist except for the last contains only spans of one
// particular size.  Everything larger goes on the last one.
pub const FPM_NUM_FREELISTS: usize = 129;

// Doubly linked list of spans of free pages; stored in first page of span.
#[repr(C)]
pub struct FreePageSpanLeader {
    pub magic: i32,         // always FREE_PAGE_SPAN_LEADER_MAGIC
    pub npages: Size,       // number of pages in span
    pub prev: relptr,       // RelptrFreePageSpanLeader
    pub next: relptr,       // RelptrFreePageSpanLeader
}

// Common header for btree leaf and internal pages.
#[repr(C)]
pub struct FreePageBtreeHeader {
    pub magic: i32,    // FREE_PAGE_LEAF_MAGIC or FREE_PAGE_INTERNAL_MAGIC
    pub nused: Size,   // number of items used
    pub parent: relptr, // RelptrFreePageBtree; uplink
}

// Internal key; points to next level of btree.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FreePageBtreeInternalKey {
    pub first_page: Size, // low bound for keys on child page
    pub child: relptr,    // RelptrFreePageBtree; downlink
}

// Leaf key; no payload data.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FreePageBtreeLeafKey {
    pub first_page: Size, // first page in span
    pub npages: Size,     // number of pages in span
}

// Work out how many keys will fit on a page.
// #define FPM_ITEMS_PER_INTERNAL_PAGE
//     ((FPM_PAGE_SIZE - sizeof(FreePageBtreeHeader)) / sizeof(FreePageBtreeInternalKey))
pub const FPM_ITEMS_PER_INTERNAL_PAGE: Size =
    (FPM_PAGE_SIZE - core::mem::size_of::<FreePageBtreeHeader>())
        / core::mem::size_of::<FreePageBtreeInternalKey>();
// #define FPM_ITEMS_PER_LEAF_PAGE
//     ((FPM_PAGE_SIZE - sizeof(FreePageBtreeHeader)) / sizeof(FreePageBtreeLeafKey))
pub const FPM_ITEMS_PER_LEAF_PAGE: Size =
    (FPM_PAGE_SIZE - core::mem::size_of::<FreePageBtreeHeader>())
        / core::mem::size_of::<FreePageBtreeLeafKey>();

// A btree page of either sort.  The C union of two flexible-length key arrays
// is modeled by storing the header followed by inline storage covering both
// arms; the `internal_key`/`leaf_key` accessors index into `u` reinterpreted
// as the appropriate key type.
#[repr(C)]
pub struct FreePageBtree {
    pub hdr: FreePageBtreeHeader,
    pub u: FreePageBtreeUnion,
}

#[repr(C)]
pub union FreePageBtreeUnion {
    pub internal_key: [FreePageBtreeInternalKey; FPM_ITEMS_PER_INTERNAL_PAGE],
    pub leaf_key: [FreePageBtreeLeafKey; FPM_ITEMS_PER_LEAF_PAGE],
}

// Results of a btree search
#[repr(C)]
pub struct FreePageBtreeSearchResult {
    pub page: *mut FreePageBtree,
    pub index: Size,
    pub found: bool,
    pub split_pages: u32,
}

// freepage.h: Everything we need in order to manage free pages.
#[repr(C)]
pub struct FreePageManager {
    pub self_: relptr,            // RelptrFreePageManager
    pub btree_root: relptr,       // RelptrFreePageBtree
    pub btree_recycle: relptr,    // RelptrFreePageSpanLeader
    pub btree_depth: u32,
    pub btree_recycle_count: u32,
    pub singleton_first_page: Size,
    pub singleton_npages: Size,
    pub contiguous_pages: Size,
    pub contiguous_pages_dirty: bool,
    pub freelist: [relptr; FPM_NUM_FREELISTS],
}

// freepage.h macros translated as helper functions.

// #define fpm_page_to_pointer(base, page) ((base) + FPM_PAGE_SIZE * (page))
#[inline]
unsafe fn fpm_page_to_pointer(base: *mut c_char, page: Size) -> *mut c_char {
    base.add(FPM_PAGE_SIZE * page)
}

// #define fpm_pointer_to_page(base, ptr)
//     (((Size) (((char *) (ptr)) - (base))) / FPM_PAGE_SIZE)
#[inline]
unsafe fn fpm_pointer_to_page(base: *mut c_char, ptr: *const c_char) -> Size {
    ((ptr as usize) - (base as usize)) / FPM_PAGE_SIZE
}

// #define fpm_pointer_is_page_aligned(base, ptr)
//     (((Size) (((char *) (ptr)) - (base))) % FPM_PAGE_SIZE == 0)
#[inline]
unsafe fn fpm_pointer_is_page_aligned(base: *mut c_char, ptr: *const c_char) -> bool {
    ((ptr as usize) - (base as usize)) % FPM_PAGE_SIZE == 0
}

// #define fpm_segment_base(fpm) (((char *) fpm) - relptr_offset(fpm->self))
#[inline]
unsafe fn fpm_segment_base(fpm: *mut FreePageManager) -> *mut c_char {
    (fpm as *mut c_char).sub(relptr_offset(&(*fpm).self_))
}

// Helpers to access the union arms by index, mirroring C's
// btp->u.internal_key[i] / btp->u.leaf_key[i].
#[inline]
unsafe fn internal_key(btp: *mut FreePageBtree, index: Size) -> *mut FreePageBtreeInternalKey {
    (&mut (*btp).u.internal_key as *mut FreePageBtreeInternalKey).add(index)
}

#[inline]
unsafe fn leaf_key(btp: *mut FreePageBtree, index: Size) -> *mut FreePageBtreeLeafKey {
    (&mut (*btp).u.leaf_key as *mut FreePageBtreeLeafKey).add(index)
}

/// Initialize a new, empty free page manager.
///
/// 'fpm' should reference caller-provided memory large enough to contain a
/// FreePageManager.  We'll initialize it here.
///
/// 'base' is the address to which all pointers are relative.  When managing
/// a dynamic shared memory segment, it should normally be the base of the
/// segment.  When managing backend-private memory, it can be either NULL or,
/// if managing a single contiguous extent of memory, the start of that extent.
pub unsafe fn FreePageManagerInitialize(fpm: *mut FreePageManager, base: *mut c_char) {
    let mut f: Size;

    relptr_store(base, &mut (*fpm).self_, fpm);
    relptr_store(base, &mut (*fpm).btree_root, ptr::null_mut::<FreePageBtree>());
    relptr_store(base, &mut (*fpm).btree_recycle, ptr::null_mut::<FreePageSpanLeader>());
    (*fpm).btree_depth = 0;
    (*fpm).btree_recycle_count = 0;
    (*fpm).singleton_first_page = 0;
    (*fpm).singleton_npages = 0;
    (*fpm).contiguous_pages = 0;
    (*fpm).contiguous_pages_dirty = true;

    f = 0;
    while f < FPM_NUM_FREELISTS {
        relptr_store(base, &mut (*fpm).freelist[f], ptr::null_mut::<FreePageSpanLeader>());
        f += 1;
    }
}

/// Allocate a run of pages of the given length from the free page manager.
/// The return value indicates whether we were able to satisfy the request;
/// if true, the first page of the allocation is stored in *first_page.
pub unsafe fn FreePageManagerGet(
    fpm: *mut FreePageManager,
    npages: Size,
    first_page: *mut Size,
) -> bool {
    let result: bool;
    let contiguous_pages: Size;

    result = FreePageManagerGetInternal(fpm, npages, first_page);

    // It's a bit counterintuitive, but allocating pages can actually create
    // opportunities for cleanup that create larger ranges.  We might pull a
    // key out of the btree that enables the item at the head of the btree
    // recycle list to be inserted; and then if there are more items behind it
    // one of those might cause two currently-separated ranges to merge,
    // creating a single range of contiguous pages larger than any that
    // existed previously.  It might be worth trying to improve the cleanup
    // algorithm to avoid such corner cases, but for now we just notice the
    // condition and do the appropriate reporting.
    contiguous_pages = FreePageBtreeCleanup(fpm);
    if (*fpm).contiguous_pages < contiguous_pages {
        (*fpm).contiguous_pages = contiguous_pages;
    }

    // FreePageManagerGetInternal may have set contiguous_pages_dirty.
    // Recompute contiguous_pages if so.
    FreePageManagerUpdateLargest(fpm);

    result
}

/// Compute the size of the largest run of pages that the user could
/// successfully get.
unsafe fn FreePageManagerLargestContiguous(fpm: *mut FreePageManager) -> Size {
    let base: *mut c_char;
    let mut largest: Size;

    base = fpm_segment_base(fpm);
    largest = 0;
    if !relptr_is_null(&(*fpm).freelist[FPM_NUM_FREELISTS - 1]) {
        let mut candidate: *mut FreePageSpanLeader;

        candidate = relptr_access(base, &(*fpm).freelist[FPM_NUM_FREELISTS - 1]);
        loop {
            if (*candidate).npages > largest {
                largest = (*candidate).npages;
            }
            candidate = relptr_access(base, &(*candidate).next);
            if candidate.is_null() {
                break;
            }
        }
    } else {
        let mut f: Size = FPM_NUM_FREELISTS - 1;

        loop {
            f -= 1;
            if !relptr_is_null(&(*fpm).freelist[f]) {
                largest = f + 1;
                break;
            }
            if f == 0 {
                break;
            }
        }
    }

    largest
}

/// Recompute the size of the largest run of pages that the user could
/// successfully get, if it has been marked dirty.
unsafe fn FreePageManagerUpdateLargest(fpm: *mut FreePageManager) {
    if !(*fpm).contiguous_pages_dirty {
        return;
    }

    (*fpm).contiguous_pages = FreePageManagerLargestContiguous(fpm);
    (*fpm).contiguous_pages_dirty = false;
}

/// Transfer a run of pages to the free page manager.
pub unsafe fn FreePageManagerPut(fpm: *mut FreePageManager, first_page: Size, npages: Size) {
    let mut contiguous_pages: Size;

    Assert!(npages > 0);

    // Record the new pages.
    contiguous_pages = FreePageManagerPutInternal(fpm, first_page, npages, false);

    // If the new range we inserted into the page manager was contiguous with
    // an existing range, it may have opened up cleanup opportunities.
    if contiguous_pages > npages {
        let cleanup_contiguous_pages: Size;

        cleanup_contiguous_pages = FreePageBtreeCleanup(fpm);
        if cleanup_contiguous_pages > contiguous_pages {
            contiguous_pages = cleanup_contiguous_pages;
        }
    }

    // See if we now have a new largest chunk.
    if (*fpm).contiguous_pages < contiguous_pages {
        (*fpm).contiguous_pages = contiguous_pages;
    }

    // The earlier call to FreePageManagerPutInternal may have set
    // contiguous_pages_dirty if it needed to allocate internal pages, so
    // recompute contiguous_pages if necessary.
    FreePageManagerUpdateLargest(fpm);
}

/// Produce a debugging dump of the state of a free page manager.
pub unsafe fn FreePageManagerDump(fpm: *mut FreePageManager) -> *mut c_char {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut buf: StringInfoData = core::mem::zeroed();
    let recycle: *mut FreePageSpanLeader;
    let mut dumped_any_freelist: bool = false;
    let mut f: Size;

    // Initialize output buffer.
    initStringInfo(&mut buf);

    // Dump general stuff.
    appendStringInfo!(
        &mut buf as StringInfo,
        "metadata: self {} max contiguous pages = {}\n",
        relptr_offset(&(*fpm).self_),
        (*fpm).contiguous_pages
    );

    // Dump btree.
    if (*fpm).btree_depth > 0 {
        let root: *mut FreePageBtree;

        appendStringInfo!(&mut buf as StringInfo, "btree depth {}:\n", (*fpm).btree_depth);
        root = relptr_access(base, &(*fpm).btree_root);
        FreePageManagerDumpBtree(fpm, root, ptr::null_mut(), 0, &mut buf);
    } else if (*fpm).singleton_npages > 0 {
        appendStringInfo!(
            &mut buf as StringInfo,
            "singleton: {}({})\n",
            (*fpm).singleton_first_page,
            (*fpm).singleton_npages
        );
    }

    // Dump btree recycle list.
    recycle = relptr_access(base, &(*fpm).btree_recycle);
    if !recycle.is_null() {
        appendStringInfoString(&mut buf as StringInfo, c"btree recycle:".as_ptr());
        FreePageManagerDumpSpans(fpm, recycle, 1, &mut buf);
    }

    // Dump free lists.
    f = 0;
    while f < FPM_NUM_FREELISTS {
        let span: *mut FreePageSpanLeader;

        if relptr_is_null(&(*fpm).freelist[f]) {
            f += 1;
            continue;
        }
        if !dumped_any_freelist {
            appendStringInfoString(&mut buf as StringInfo, c"freelists:\n".as_ptr());
            dumped_any_freelist = true;
        }
        appendStringInfo!(&mut buf as StringInfo, "  {}:", f + 1);
        span = relptr_access(base, &(*fpm).freelist[f]);
        FreePageManagerDumpSpans(fpm, span, f + 1, &mut buf);
        f += 1;
    }

    // And return result to caller.
    buf.data
}

/// The first_page value stored at index zero in any non-root page must match
/// the first_page value stored in its parent at the index which points to that
/// page.  So when the value stored at index zero in a btree page changes, we've
/// got to walk up the tree adjusting ancestor keys until we reach an ancestor
/// where that key isn't index zero.  This function should be called after
/// updating the first key on the target page; it will propagate the change
/// upward as far as needed.
///
/// We assume here that the first key on the page has not changed enough to
/// require changes in the ordering of keys on its ancestor pages.  Thus,
/// if we search the parent page for the first key greater than or equal to
/// the first key on the current page, the downlink to this page will be either
/// the exact index returned by the search (if the first key decreased)
/// or one less (if the first key increased).
unsafe fn FreePageBtreeAdjustAncestorKeys(fpm: *mut FreePageManager, btp: *mut FreePageBtree) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let first_page: Size;
    let mut parent: *mut FreePageBtree;
    let mut child: *mut FreePageBtree;

    // This might be either a leaf or an internal page.
    Assert!((*btp).hdr.nused > 0);
    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        Assert!((*btp).hdr.nused <= FPM_ITEMS_PER_LEAF_PAGE);
        first_page = (*leaf_key(btp, 0)).first_page;
    } else {
        Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
        Assert!((*btp).hdr.nused <= FPM_ITEMS_PER_INTERNAL_PAGE);
        first_page = (*internal_key(btp, 0)).first_page;
    }
    child = btp;

    // Loop until we find an ancestor that does not require adjustment.
    loop {
        let mut s: Size;

        parent = relptr_access(base, &(*child).hdr.parent);
        if parent.is_null() {
            break;
        }
        s = FreePageBtreeSearchInternal(parent, first_page);

        // Key is either at index s or index s-1; figure out which.
        if s >= (*parent).hdr.nused {
            Assert!(s == (*parent).hdr.nused);
            s -= 1;
        } else {
            let check: *mut FreePageBtree;

            check = relptr_access(base, &(*internal_key(parent, s)).child);
            if check != child {
                Assert!(s > 0);
                s -= 1;
            }
        }

        // Debugging double-check.
        {
            let check: *mut FreePageBtree;

            check = relptr_access(base, &(*internal_key(parent, s)).child);
            Assert!(s < (*parent).hdr.nused);
            Assert!(child == check);
        }

        // Update the parent key.
        (*internal_key(parent, s)).first_page = first_page;

        // If this is the first key in the parent, go up another level; else
        // done.
        if s > 0 {
            break;
        }
        child = parent;
    }
}

/// Attempt to reclaim space from the free-page btree.  The return value is
/// the largest range of contiguous pages created by the cleanup operation.
unsafe fn FreePageBtreeCleanup(fpm: *mut FreePageManager) -> Size {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut max_contiguous_pages: Size = 0;

    // Attempt to shrink the depth of the btree.
    while !relptr_is_null(&(*fpm).btree_root) {
        let root: *mut FreePageBtree = relptr_access(base, &(*fpm).btree_root);

        // If the root contains only one key, reduce depth by one.
        if (*root).hdr.nused == 1 {
            // Shrink depth of tree by one.
            Assert!((*fpm).btree_depth > 0);
            (*fpm).btree_depth -= 1;
            if (*root).hdr.magic == FREE_PAGE_LEAF_MAGIC {
                // If root is a leaf, convert only entry to singleton range.
                relptr_store(base, &mut (*fpm).btree_root, ptr::null_mut::<FreePageBtree>());
                (*fpm).singleton_first_page = (*leaf_key(root, 0)).first_page;
                (*fpm).singleton_npages = (*leaf_key(root, 0)).npages;
            } else {
                let newroot: *mut FreePageBtree;

                // If root is an internal page, make only child the root.
                Assert!((*root).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
                relptr_copy(&mut (*fpm).btree_root, &(*internal_key(root, 0)).child);
                newroot = relptr_access(base, &(*fpm).btree_root);
                relptr_store(base, &mut (*newroot).hdr.parent, ptr::null_mut::<FreePageBtree>());
            }
            FreePageBtreeRecycle(fpm, fpm_pointer_to_page(base, root as *const c_char));
        } else if (*root).hdr.nused == 2 && (*root).hdr.magic == FREE_PAGE_LEAF_MAGIC {
            let end_of_first: Size;
            let start_of_second: Size;

            end_of_first = (*leaf_key(root, 0)).first_page + (*leaf_key(root, 0)).npages;
            start_of_second = (*leaf_key(root, 1)).first_page;

            if end_of_first + 1 == start_of_second {
                let root_page: Size = fpm_pointer_to_page(base, root as *const c_char);

                if end_of_first == root_page {
                    FreePagePopSpanLeader(fpm, (*leaf_key(root, 0)).first_page);
                    FreePagePopSpanLeader(fpm, (*leaf_key(root, 1)).first_page);
                    (*fpm).singleton_first_page = (*leaf_key(root, 0)).first_page;
                    (*fpm).singleton_npages =
                        (*leaf_key(root, 0)).npages + (*leaf_key(root, 1)).npages + 1;
                    (*fpm).btree_depth = 0;
                    relptr_store(base, &mut (*fpm).btree_root, ptr::null_mut::<FreePageBtree>());
                    FreePagePushSpanLeader(fpm, (*fpm).singleton_first_page, (*fpm).singleton_npages);
                    Assert!(max_contiguous_pages == 0);
                    max_contiguous_pages = (*fpm).singleton_npages;
                }
            }

            // Whether it worked or not, it's time to stop.
            break;
        } else {
            // Nothing more to do.  Stop.
            break;
        }
    }

    // Attempt to free recycled btree pages.  We skip this if releasing the
    // recycled page would require a btree page split, because the page we're
    // trying to recycle would be consumed by the split, which would be
    // counterproductive.
    //
    // We also currently only ever attempt to recycle the first page on the
    // list; that could be made more aggressive, but it's not clear that the
    // complexity would be worthwhile.
    while (*fpm).btree_recycle_count > 0 {
        let btp: *mut FreePageBtree;
        let first_page: Size;
        let contiguous_pages: Size;

        btp = FreePageBtreeGetRecycled(fpm);
        first_page = fpm_pointer_to_page(base, btp as *const c_char);
        contiguous_pages = FreePageManagerPutInternal(fpm, first_page, 1, true);
        if contiguous_pages == 0 {
            FreePageBtreeRecycle(fpm, first_page);
            break;
        } else {
            if contiguous_pages > max_contiguous_pages {
                max_contiguous_pages = contiguous_pages;
            }
        }
    }

    max_contiguous_pages
}

/// Consider consolidating the given page with its left or right sibling,
/// if it's fairly empty.
unsafe fn FreePageBtreeConsolidate(fpm: *mut FreePageManager, btp: *mut FreePageBtree) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut np: *mut FreePageBtree;
    let max: Size;

    // We only try to consolidate pages that are less than a third full. We
    // could be more aggressive about this, but that might risk performing
    // consolidation only to end up splitting again shortly thereafter.  Since
    // the btree should be very small compared to the space under management,
    // our goal isn't so much to ensure that it always occupies the absolutely
    // smallest possible number of pages as to reclaim pages before things get
    // too egregiously out of hand.
    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        max = FPM_ITEMS_PER_LEAF_PAGE;
    } else {
        Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
        max = FPM_ITEMS_PER_INTERNAL_PAGE;
    }
    if (*btp).hdr.nused >= max / 3 {
        return;
    }

    // If we can fit our right sibling's keys onto this page, consolidate.
    np = FreePageBtreeFindRightSibling(base, btp);
    if !np.is_null() && (*btp).hdr.nused + (*np).hdr.nused <= max {
        if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
            ptr::copy_nonoverlapping(
                leaf_key(np, 0),
                leaf_key(btp, (*btp).hdr.nused),
                (*np).hdr.nused,
            );
            (*btp).hdr.nused += (*np).hdr.nused;
        } else {
            ptr::copy_nonoverlapping(
                internal_key(np, 0),
                internal_key(btp, (*btp).hdr.nused),
                (*np).hdr.nused,
            );
            (*btp).hdr.nused += (*np).hdr.nused;
            FreePageBtreeUpdateParentPointers(base, btp);
        }
        FreePageBtreeRemovePage(fpm, np);
        return;
    }

    // If we can fit our keys onto our left sibling's page, consolidate. In
    // this case, we move our keys onto the other page rather than vice versa,
    // to avoid having to adjust ancestor keys.
    np = FreePageBtreeFindLeftSibling(base, btp);
    if !np.is_null() && (*btp).hdr.nused + (*np).hdr.nused <= max {
        if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
            ptr::copy_nonoverlapping(
                leaf_key(btp, 0),
                leaf_key(np, (*np).hdr.nused),
                (*btp).hdr.nused,
            );
            (*np).hdr.nused += (*btp).hdr.nused;
        } else {
            ptr::copy_nonoverlapping(
                internal_key(btp, 0),
                internal_key(np, (*np).hdr.nused),
                (*btp).hdr.nused,
            );
            (*np).hdr.nused += (*btp).hdr.nused;
            FreePageBtreeUpdateParentPointers(base, np);
        }
        FreePageBtreeRemovePage(fpm, btp);
        return;
    }
}

/// Find the passed page's left sibling; that is, the page at the same level
/// of the tree whose keyspace immediately precedes ours.
unsafe fn FreePageBtreeFindLeftSibling(
    base: *mut c_char,
    btp: *mut FreePageBtree,
) -> *mut FreePageBtree {
    let mut p: *mut FreePageBtree = btp;
    let mut levels: i32 = 0;

    // Move up until we can move left.
    loop {
        let first_page: Size;
        let index: Size;

        first_page = FreePageBtreeFirstKey(p);
        p = relptr_access(base, &(*p).hdr.parent);

        if p.is_null() {
            return ptr::null_mut(); // we were passed the rightmost page
        }

        index = FreePageBtreeSearchInternal(p, first_page);
        if index > 0 {
            Assert!((*internal_key(p, index)).first_page == first_page);
            p = relptr_access(base, &(*internal_key(p, index - 1)).child);
            break;
        }
        Assert!(index == 0);
        levels += 1;
    }

    // Descend left.
    while levels > 0 {
        Assert!((*p).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
        p = relptr_access(base, &(*internal_key(p, (*p).hdr.nused - 1)).child);
        levels -= 1;
    }
    Assert!((*p).hdr.magic == (*btp).hdr.magic);

    p
}

/// Find the passed page's right sibling; that is, the page at the same level
/// of the tree whose keyspace immediately follows ours.
unsafe fn FreePageBtreeFindRightSibling(
    base: *mut c_char,
    btp: *mut FreePageBtree,
) -> *mut FreePageBtree {
    let mut p: *mut FreePageBtree = btp;
    let mut levels: i32 = 0;

    // Move up until we can move right.
    loop {
        let first_page: Size;
        let index: Size;

        first_page = FreePageBtreeFirstKey(p);
        p = relptr_access(base, &(*p).hdr.parent);

        if p.is_null() {
            return ptr::null_mut(); // we were passed the rightmost page
        }

        index = FreePageBtreeSearchInternal(p, first_page);
        if index < (*p).hdr.nused - 1 {
            Assert!((*internal_key(p, index)).first_page == first_page);
            p = relptr_access(base, &(*internal_key(p, index + 1)).child);
            break;
        }
        Assert!(index == (*p).hdr.nused - 1);
        levels += 1;
    }

    // Descend left.
    while levels > 0 {
        Assert!((*p).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
        p = relptr_access(base, &(*internal_key(p, 0)).child);
        levels -= 1;
    }
    Assert!((*p).hdr.magic == (*btp).hdr.magic);

    p
}

/// Get the first key on a btree page.
unsafe fn FreePageBtreeFirstKey(btp: *mut FreePageBtree) -> Size {
    Assert!((*btp).hdr.nused > 0);

    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        (*leaf_key(btp, 0)).first_page
    } else {
        Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
        (*internal_key(btp, 0)).first_page
    }
}

/// Get a page from the btree recycle list for use as a btree page.
unsafe fn FreePageBtreeGetRecycled(fpm: *mut FreePageManager) -> *mut FreePageBtree {
    let base: *mut c_char = fpm_segment_base(fpm);
    let victim: *mut FreePageSpanLeader = relptr_access(base, &(*fpm).btree_recycle);
    let newhead: *mut FreePageSpanLeader;

    Assert!(!victim.is_null());
    newhead = relptr_access(base, &(*victim).next);
    if !newhead.is_null() {
        relptr_copy(&mut (*newhead).prev, &(*victim).prev);
    }
    relptr_store(base, &mut (*fpm).btree_recycle, newhead);
    Assert!(fpm_pointer_is_page_aligned(base, victim as *const c_char));
    (*fpm).btree_recycle_count -= 1;
    victim as *mut FreePageBtree
}

/// Insert an item into an internal page (there must be room).
unsafe fn FreePageBtreeInsertInternal(
    base: *mut c_char,
    btp: *mut FreePageBtree,
    index: Size,
    first_page: Size,
    child: *mut FreePageBtree,
) {
    Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
    Assert!((*btp).hdr.nused < FPM_ITEMS_PER_INTERNAL_PAGE);
    Assert!(index <= (*btp).hdr.nused);
    ptr::copy(
        internal_key(btp, index),
        internal_key(btp, index + 1),
        (*btp).hdr.nused - index,
    );
    (*internal_key(btp, index)).first_page = first_page;
    relptr_store(base, &mut (*internal_key(btp, index)).child, child);
    (*btp).hdr.nused += 1;
}

/// Insert an item into a leaf page (there must be room).
unsafe fn FreePageBtreeInsertLeaf(
    btp: *mut FreePageBtree,
    index: Size,
    first_page: Size,
    npages: Size,
) {
    Assert!((*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC);
    Assert!((*btp).hdr.nused < FPM_ITEMS_PER_LEAF_PAGE);
    Assert!(index <= (*btp).hdr.nused);
    ptr::copy(
        leaf_key(btp, index),
        leaf_key(btp, index + 1),
        (*btp).hdr.nused - index,
    );
    (*leaf_key(btp, index)).first_page = first_page;
    (*leaf_key(btp, index)).npages = npages;
    (*btp).hdr.nused += 1;
}

/// Put a page on the btree recycle list.
unsafe fn FreePageBtreeRecycle(fpm: *mut FreePageManager, pageno: Size) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let head: *mut FreePageSpanLeader = relptr_access(base, &(*fpm).btree_recycle);
    let span: *mut FreePageSpanLeader;

    span = fpm_page_to_pointer(base, pageno) as *mut FreePageSpanLeader;
    (*span).magic = FREE_PAGE_SPAN_LEADER_MAGIC;
    (*span).npages = 1;
    relptr_store(base, &mut (*span).next, head);
    relptr_store(base, &mut (*span).prev, ptr::null_mut::<FreePageSpanLeader>());
    if !head.is_null() {
        relptr_store(base, &mut (*head).prev, span);
    }
    relptr_store(base, &mut (*fpm).btree_recycle, span);
    (*fpm).btree_recycle_count += 1;
}

/// Remove an item from the btree at the given position on the given page.
unsafe fn FreePageBtreeRemove(fpm: *mut FreePageManager, btp: *mut FreePageBtree, index: Size) {
    Assert!((*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC);
    Assert!(index < (*btp).hdr.nused);

    // When last item is removed, extirpate entire page from btree.
    if (*btp).hdr.nused == 1 {
        FreePageBtreeRemovePage(fpm, btp);
        return;
    }

    // Physically remove the key from the page.
    (*btp).hdr.nused -= 1;
    if index < (*btp).hdr.nused {
        ptr::copy(
            leaf_key(btp, index + 1),
            leaf_key(btp, index),
            (*btp).hdr.nused - index,
        );
    }

    // If we just removed the first key, adjust ancestor keys.
    if index == 0 {
        FreePageBtreeAdjustAncestorKeys(fpm, btp);
    }

    // Consider whether to consolidate this page with a sibling.
    FreePageBtreeConsolidate(fpm, btp);
}

/// Remove a page from the btree.  Caller is responsible for having relocated
/// any keys from this page that are still wanted.  The page is placed on the
/// recycled list.
unsafe fn FreePageBtreeRemovePage(fpm: *mut FreePageManager, btp_in: *mut FreePageBtree) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut btp: *mut FreePageBtree = btp_in;
    let mut parent: *mut FreePageBtree;
    let index: Size;
    let first_page: Size;

    loop {
        // Find parent page.
        parent = relptr_access(base, &(*btp).hdr.parent);
        if parent.is_null() {
            // We are removing the root page.
            relptr_store(base, &mut (*fpm).btree_root, ptr::null_mut::<FreePageBtree>());
            (*fpm).btree_depth = 0;
            Assert!((*fpm).singleton_first_page == 0);
            Assert!((*fpm).singleton_npages == 0);
            return;
        }

        // If the parent contains only one item, we need to remove it as well.
        if (*parent).hdr.nused > 1 {
            break;
        }
        FreePageBtreeRecycle(fpm, fpm_pointer_to_page(base, btp as *const c_char));
        btp = parent;
    }

    // Find and remove the downlink.
    first_page = FreePageBtreeFirstKey(btp);
    if (*parent).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        index = FreePageBtreeSearchLeaf(parent, first_page);
        Assert!(index < (*parent).hdr.nused);
        if index < (*parent).hdr.nused - 1 {
            ptr::copy(
                leaf_key(parent, index + 1),
                leaf_key(parent, index),
                (*parent).hdr.nused - index - 1,
            );
        }
    } else {
        index = FreePageBtreeSearchInternal(parent, first_page);
        Assert!(index < (*parent).hdr.nused);
        if index < (*parent).hdr.nused - 1 {
            ptr::copy(
                internal_key(parent, index + 1),
                internal_key(parent, index),
                (*parent).hdr.nused - index - 1,
            );
        }
    }
    (*parent).hdr.nused -= 1;
    Assert!((*parent).hdr.nused > 0);

    // Recycle the page.
    FreePageBtreeRecycle(fpm, fpm_pointer_to_page(base, btp as *const c_char));

    // Adjust ancestor keys if needed.
    if index == 0 {
        FreePageBtreeAdjustAncestorKeys(fpm, parent);
    }

    // Consider whether to consolidate the parent with a sibling.
    FreePageBtreeConsolidate(fpm, parent);
}

/// Search the btree for an entry for the given first page and initialize
/// *result with the results of the search.  result->page and result->index
/// indicate either the position of an exact match or the position at which
/// the new key should be inserted.  result->found is true for an exact match,
/// otherwise false.  result->split_pages will contain the number of additional
/// btree pages that will be needed when performing a split to insert a key.
/// Except as described above, the contents of fields in the result object are
/// undefined on return.
unsafe fn FreePageBtreeSearch(
    fpm: *mut FreePageManager,
    first_page: Size,
    result: *mut FreePageBtreeSearchResult,
) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut btp: *mut FreePageBtree = relptr_access(base, &(*fpm).btree_root);
    let mut index: Size;

    (*result).split_pages = 1;

    // If the btree is empty, there's nothing to find.
    if btp.is_null() {
        (*result).page = ptr::null_mut();
        (*result).found = false;
        return;
    }

    // Descend until we hit a leaf.
    while (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
        let child: *mut FreePageBtree;
        let found_exact: bool;

        index = FreePageBtreeSearchInternal(btp, first_page);
        found_exact =
            index < (*btp).hdr.nused && (*internal_key(btp, index)).first_page == first_page;

        // If we found an exact match we descend directly.  Otherwise, we
        // descend into the child to the left if possible so that we can find
        // the insertion point at that child's high end.
        if !found_exact && index > 0 {
            index -= 1;
        }

        // Track required split depth for leaf insert.
        if (*btp).hdr.nused >= FPM_ITEMS_PER_INTERNAL_PAGE {
            Assert!((*btp).hdr.nused == FPM_ITEMS_PER_INTERNAL_PAGE);
            (*result).split_pages += 1;
        } else {
            (*result).split_pages = 0;
        }

        // Descend to appropriate child page.
        Assert!(index < (*btp).hdr.nused);
        child = relptr_access(base, &(*internal_key(btp, index)).child);
        Assert!(relptr_access::<FreePageBtree>(base, &(*child).hdr.parent) == btp);
        btp = child;
    }

    // Track required split depth for leaf insert.
    if (*btp).hdr.nused >= FPM_ITEMS_PER_LEAF_PAGE {
        Assert!((*btp).hdr.nused == FPM_ITEMS_PER_INTERNAL_PAGE);
        (*result).split_pages += 1;
    } else {
        (*result).split_pages = 0;
    }

    // Search leaf page.
    index = FreePageBtreeSearchLeaf(btp, first_page);

    // Assemble results.
    (*result).page = btp;
    (*result).index = index;
    (*result).found =
        index < (*btp).hdr.nused && first_page == (*leaf_key(btp, index)).first_page;
}

/// Search an internal page for the first key greater than or equal to a given
/// page number.  Returns the index of that key, or one greater than the number
/// of keys on the page if none.
unsafe fn FreePageBtreeSearchInternal(btp: *mut FreePageBtree, first_page: Size) -> Size {
    let mut low: Size = 0;
    let mut high: Size = (*btp).hdr.nused;

    Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
    Assert!(high > 0 && high <= FPM_ITEMS_PER_INTERNAL_PAGE);

    while low < high {
        let mid: Size = (low + high) / 2;
        let val: Size = (*internal_key(btp, mid)).first_page;

        if first_page == val {
            return mid;
        } else if first_page < val {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    low
}

/// Search a leaf page for the first key greater than or equal to a given
/// page number.  Returns the index of that key, or one greater than the number
/// of keys on the page if none.
unsafe fn FreePageBtreeSearchLeaf(btp: *mut FreePageBtree, first_page: Size) -> Size {
    let mut low: Size = 0;
    let mut high: Size = (*btp).hdr.nused;

    Assert!((*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC);
    Assert!(high > 0 && high <= FPM_ITEMS_PER_LEAF_PAGE);

    while low < high {
        let mid: Size = (low + high) / 2;
        let val: Size = (*leaf_key(btp, mid)).first_page;

        if first_page == val {
            return mid;
        } else if first_page < val {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    low
}

/// Allocate a new btree page and move half the keys from the provided page
/// to the new page.  Caller is responsible for making sure that there's a
/// page available from fpm->btree_recycle.  Returns a pointer to the new page,
/// to which caller must add a downlink.
unsafe fn FreePageBtreeSplitPage(
    fpm: *mut FreePageManager,
    btp: *mut FreePageBtree,
) -> *mut FreePageBtree {
    let newsibling: *mut FreePageBtree;

    newsibling = FreePageBtreeGetRecycled(fpm);
    (*newsibling).hdr.magic = (*btp).hdr.magic;
    (*newsibling).hdr.nused = (*btp).hdr.nused / 2;
    relptr_copy(&mut (*newsibling).hdr.parent, &(*btp).hdr.parent);
    (*btp).hdr.nused -= (*newsibling).hdr.nused;

    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        ptr::copy_nonoverlapping(
            leaf_key(btp, (*btp).hdr.nused),
            leaf_key(newsibling, 0),
            (*newsibling).hdr.nused,
        );
    } else {
        Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
        ptr::copy_nonoverlapping(
            internal_key(btp, (*btp).hdr.nused),
            internal_key(newsibling, 0),
            (*newsibling).hdr.nused,
        );
        FreePageBtreeUpdateParentPointers(fpm_segment_base(fpm), newsibling);
    }

    newsibling
}

/// When internal pages are split or merged, the parent pointers of their
/// children must be updated.
unsafe fn FreePageBtreeUpdateParentPointers(base: *mut c_char, btp: *mut FreePageBtree) {
    let mut i: Size;

    Assert!((*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
    i = 0;
    while i < (*btp).hdr.nused {
        let child: *mut FreePageBtree;

        child = relptr_access(base, &(*internal_key(btp, i)).child);
        relptr_store(base, &mut (*child).hdr.parent, btp);
        i += 1;
    }
}

/// Debugging dump of btree data.
unsafe fn FreePageManagerDumpBtree(
    fpm: *mut FreePageManager,
    btp: *mut FreePageBtree,
    parent: *mut FreePageBtree,
    level: i32,
    buf: StringInfo,
) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let pageno: Size = fpm_pointer_to_page(base, btp as *const c_char);
    let mut index: Size;
    let check_parent: *mut FreePageBtree;

    check_stack_depth();
    check_parent = relptr_access(base, &(*btp).hdr.parent);
    appendStringInfo!(
        buf,
        "  {}@{} {}",
        pageno,
        level,
        if (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC { 'i' } else { 'l' }
    );
    if parent != check_parent {
        appendStringInfo!(
            buf,
            " [actual parent {}, expected {}]",
            fpm_pointer_to_page(base, check_parent as *const c_char),
            fpm_pointer_to_page(base, parent as *const c_char)
        );
    }
    appendStringInfoChar(buf, b':' as c_char);
    index = 0;
    while index < (*btp).hdr.nused {
        if (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
            appendStringInfo!(
                buf,
                " {}->{}",
                (*internal_key(btp, index)).first_page,
                relptr_offset(&(*internal_key(btp, index)).child) / FPM_PAGE_SIZE
            );
        } else {
            appendStringInfo!(
                buf,
                " {}({})",
                (*leaf_key(btp, index)).first_page,
                (*leaf_key(btp, index)).npages
            );
        }
        index += 1;
    }
    appendStringInfoChar(buf, b'\n' as c_char);

    if (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
        index = 0;
        while index < (*btp).hdr.nused {
            let child: *mut FreePageBtree;

            child = relptr_access(base, &(*internal_key(btp, index)).child);
            FreePageManagerDumpBtree(fpm, child, btp, level + 1, buf);
            index += 1;
        }
    }
}

/// Debugging dump of free-span data.
unsafe fn FreePageManagerDumpSpans(
    fpm: *mut FreePageManager,
    span_in: *mut FreePageSpanLeader,
    expected_pages: Size,
    buf: StringInfo,
) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut span: *mut FreePageSpanLeader = span_in;

    while !span.is_null() {
        if (*span).npages != expected_pages {
            appendStringInfo!(
                buf,
                " {}({})",
                fpm_pointer_to_page(base, span as *const c_char),
                (*span).npages
            );
        } else {
            appendStringInfo!(buf, " {}", fpm_pointer_to_page(base, span as *const c_char));
        }
        span = relptr_access(base, &(*span).next);
    }

    appendStringInfoChar(buf, b'\n' as c_char);
}

/// This function allocates a run of pages of the given length from the free
/// page manager.
unsafe fn FreePageManagerGetInternal(
    fpm: *mut FreePageManager,
    npages: Size,
    first_page: *mut Size,
) -> bool {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut victim: *mut FreePageSpanLeader = ptr::null_mut();
    let prev: *mut FreePageSpanLeader;
    let next: *mut FreePageSpanLeader;
    let mut result: FreePageBtreeSearchResult = core::mem::zeroed();
    let victim_page: Size; // placate compiler
    let mut f: Size;

    // Search for a free span.
    //
    // Right now, we use a simple best-fit policy here, but it's possible for
    // this to result in memory fragmentation if we're repeatedly asked to
    // allocate chunks just a little smaller than what we have available.
    // Hopefully, this is unlikely, because we expect most requests to be
    // single pages or superblock-sized chunks -- but no policy can be optimal
    // under all circumstances unless it has knowledge of future allocation
    // patterns.
    f = Min(npages, FPM_NUM_FREELISTS) - 1;
    while f < FPM_NUM_FREELISTS {
        // Skip empty freelists.
        if relptr_is_null(&(*fpm).freelist[f]) {
            f += 1;
            continue;
        }

        // All of the freelists except the last one contain only items of a
        // single size, so we just take the first one.  But the final free
        // list contains everything too big for any of the other lists, so we
        // need to search the list.
        if f < FPM_NUM_FREELISTS - 1 {
            victim = relptr_access(base, &(*fpm).freelist[f]);
        } else {
            let mut candidate: *mut FreePageSpanLeader;

            candidate = relptr_access(base, &(*fpm).freelist[f]);
            loop {
                if (*candidate).npages >= npages
                    && (victim.is_null() || (*victim).npages > (*candidate).npages)
                {
                    victim = candidate;
                    if (*victim).npages == npages {
                        break;
                    }
                }
                candidate = relptr_access(base, &(*candidate).next);
                if candidate.is_null() {
                    break;
                }
            }
        }
        break;
    }

    // If we didn't find an allocatable span, return failure.
    if victim.is_null() {
        return false;
    }

    // Remove span from free list.
    Assert!((*victim).magic == FREE_PAGE_SPAN_LEADER_MAGIC);
    prev = relptr_access(base, &(*victim).prev);
    next = relptr_access(base, &(*victim).next);
    if !prev.is_null() {
        relptr_copy(&mut (*prev).next, &(*victim).next);
    } else {
        relptr_copy(&mut (*fpm).freelist[f], &(*victim).next);
    }
    if !next.is_null() {
        relptr_copy(&mut (*next).prev, &(*victim).prev);
    }
    victim_page = fpm_pointer_to_page(base, victim as *const c_char);

    // Decide whether we might be invalidating contiguous_pages.
    if f == FPM_NUM_FREELISTS - 1 && (*victim).npages == (*fpm).contiguous_pages {
        // The victim span came from the oversized freelist, and had the same
        // size as the longest span.  There may or may not be another one of
        // the same size, so contiguous_pages must be recomputed just to be
        // safe.
        (*fpm).contiguous_pages_dirty = true;
    } else if f + 1 == (*fpm).contiguous_pages && relptr_is_null(&(*fpm).freelist[f]) {
        // The victim span came from a fixed sized freelist, and it was the
        // list for spans of the same size as the current longest span, and
        // the list is now empty after removing the victim.  So
        // contiguous_pages must be recomputed without a doubt.
        (*fpm).contiguous_pages_dirty = true;
    }

    // If we haven't initialized the btree yet, the victim must be the single
    // span stored within the FreePageManager itself.  Otherwise, we need to
    // update the btree.
    if relptr_is_null(&(*fpm).btree_root) {
        Assert!(victim_page == (*fpm).singleton_first_page);
        Assert!((*victim).npages == (*fpm).singleton_npages);
        Assert!((*victim).npages >= npages);
        (*fpm).singleton_first_page += npages;
        (*fpm).singleton_npages -= npages;
        if (*fpm).singleton_npages > 0 {
            FreePagePushSpanLeader(fpm, (*fpm).singleton_first_page, (*fpm).singleton_npages);
        }
    } else {
        // If the span we found is exactly the right size, remove it from the
        // btree completely.  Otherwise, adjust the btree entry to reflect the
        // still-unallocated portion of the span, and put that portion on the
        // appropriate free list.
        FreePageBtreeSearch(fpm, victim_page, &mut result);
        Assert!(result.found);
        if (*victim).npages == npages {
            FreePageBtreeRemove(fpm, result.page, result.index);
        } else {
            let key: *mut FreePageBtreeLeafKey;

            // Adjust btree to reflect remaining pages.
            Assert!((*victim).npages > npages);
            key = leaf_key(result.page, result.index);
            Assert!((*key).npages == (*victim).npages);
            (*key).first_page += npages;
            (*key).npages -= npages;
            if result.index == 0 {
                FreePageBtreeAdjustAncestorKeys(fpm, result.page);
            }

            // Put the unallocated pages back on the appropriate free list.
            FreePagePushSpanLeader(fpm, victim_page + npages, (*victim).npages - npages);
        }
    }

    // Return results to caller.
    *first_page = fpm_pointer_to_page(base, victim as *const c_char);
    true
}

/// Put a range of pages into the btree and freelists, consolidating it with
/// existing free spans just before and/or after it.  If 'soft' is true,
/// only perform the insertion if it can be done without allocating new btree
/// pages; if false, do it always.  Returns 0 if the soft flag caused the
/// insertion to be skipped, or otherwise the size of the contiguous span
/// created by the insertion.  This may be larger than npages if we're able
/// to consolidate with an adjacent range.
unsafe fn FreePageManagerPutInternal(
    fpm: *mut FreePageManager,
    first_page: Size,
    npages: Size,
    soft: bool,
) -> Size {
    let base: *mut c_char = fpm_segment_base(fpm);
    let mut result: FreePageBtreeSearchResult = core::mem::zeroed();
    let mut prevkey: *mut FreePageBtreeLeafKey = ptr::null_mut();
    let mut nextkey: *mut FreePageBtreeLeafKey = ptr::null_mut();
    let mut np: *mut FreePageBtree;
    let mut nindex: Size;

    Assert!(npages > 0);

    // We can store a single free span without initializing the btree.
    if (*fpm).btree_depth == 0 {
        if (*fpm).singleton_npages == 0 {
            // Don't have a span yet; store this one.
            (*fpm).singleton_first_page = first_page;
            (*fpm).singleton_npages = npages;
            FreePagePushSpanLeader(fpm, first_page, npages);
            return (*fpm).singleton_npages;
        } else if (*fpm).singleton_first_page + (*fpm).singleton_npages == first_page {
            // New span immediately follows sole existing span.
            (*fpm).singleton_npages += npages;
            FreePagePopSpanLeader(fpm, (*fpm).singleton_first_page);
            FreePagePushSpanLeader(fpm, (*fpm).singleton_first_page, (*fpm).singleton_npages);
            return (*fpm).singleton_npages;
        } else if first_page + npages == (*fpm).singleton_first_page {
            // New span immediately precedes sole existing span.
            FreePagePopSpanLeader(fpm, (*fpm).singleton_first_page);
            (*fpm).singleton_first_page = first_page;
            (*fpm).singleton_npages += npages;
            FreePagePushSpanLeader(fpm, (*fpm).singleton_first_page, (*fpm).singleton_npages);
            return (*fpm).singleton_npages;
        } else {
            // Not contiguous; we need to initialize the btree.
            let mut root_page: Size = 0;
            let root: *mut FreePageBtree;

            if !relptr_is_null(&(*fpm).btree_recycle) {
                root = FreePageBtreeGetRecycled(fpm);
            } else if soft {
                return 0; // Should not allocate if soft.
            } else if FreePageManagerGetInternal(fpm, 1, &mut root_page) {
                root = fpm_page_to_pointer(base, root_page) as *mut FreePageBtree;
            } else {
                // We'd better be able to get a page from the existing range.
                elog!(FATAL, "free page manager btree is corrupt");
                unreachable!();
            }

            // Create the btree and move the preexisting range into it.
            (*root).hdr.magic = FREE_PAGE_LEAF_MAGIC;
            (*root).hdr.nused = 1;
            relptr_store(base, &mut (*root).hdr.parent, ptr::null_mut::<FreePageBtree>());
            (*leaf_key(root, 0)).first_page = (*fpm).singleton_first_page;
            (*leaf_key(root, 0)).npages = (*fpm).singleton_npages;
            relptr_store(base, &mut (*fpm).btree_root, root);
            (*fpm).singleton_first_page = 0;
            (*fpm).singleton_npages = 0;
            (*fpm).btree_depth = 1;

            // Corner case: it may be that the btree root took the very last
            // free page.  In that case, the sole btree entry covers a zero
            // page run, which is invalid.  Overwrite it with the entry we're
            // trying to insert and get out.
            if (*leaf_key(root, 0)).npages == 0 {
                (*leaf_key(root, 0)).first_page = first_page;
                (*leaf_key(root, 0)).npages = npages;
                FreePagePushSpanLeader(fpm, first_page, npages);
                return npages;
            }

            // Fall through to insert the new key.
        }
    }

    // Search the btree.
    FreePageBtreeSearch(fpm, first_page, &mut result);
    Assert!(!result.found);
    if result.index > 0 {
        prevkey = leaf_key(result.page, result.index - 1);
    }
    if result.index < (*result.page).hdr.nused {
        np = result.page;
        nindex = result.index;
        nextkey = leaf_key(result.page, result.index);
    } else {
        np = FreePageBtreeFindRightSibling(base, result.page);
        nindex = 0;
        if !np.is_null() {
            nextkey = leaf_key(np, 0);
        }
    }

    // Consolidate with the previous entry if possible.
    if !prevkey.is_null() && (*prevkey).first_page + (*prevkey).npages >= first_page {
        let mut remove_next: bool = false;
        let result_size: Size;

        Assert!((*prevkey).first_page + (*prevkey).npages == first_page);
        (*prevkey).npages = (first_page - (*prevkey).first_page) + npages;

        // Check whether we can *also* consolidate with the following entry.
        if !nextkey.is_null()
            && (*prevkey).first_page + (*prevkey).npages >= (*nextkey).first_page
        {
            Assert!((*prevkey).first_page + (*prevkey).npages == (*nextkey).first_page);
            (*prevkey).npages = ((*nextkey).first_page - (*prevkey).first_page) + (*nextkey).npages;
            FreePagePopSpanLeader(fpm, (*nextkey).first_page);
            remove_next = true;
        }

        // Put the span on the correct freelist and save size.
        FreePagePopSpanLeader(fpm, (*prevkey).first_page);
        FreePagePushSpanLeader(fpm, (*prevkey).first_page, (*prevkey).npages);
        result_size = (*prevkey).npages;

        // If we consolidated with both the preceding and following entries,
        // we must remove the following entry.  We do this last, because
        // removing an element from the btree may invalidate pointers we hold
        // into the current data structure.
        //
        // NB: The btree is technically in an invalid state a this point
        // because we've already updated prevkey to cover the same key space
        // as nextkey.  FreePageBtreeRemove() shouldn't notice that, though.
        if remove_next {
            FreePageBtreeRemove(fpm, np, nindex);
        }

        return result_size;
    }

    // Consolidate with the next entry if possible.
    if !nextkey.is_null() && first_page + npages >= (*nextkey).first_page {
        let newpages: Size;

        // Compute new size for span.
        Assert!(first_page + npages == (*nextkey).first_page);
        newpages = ((*nextkey).first_page - first_page) + (*nextkey).npages;

        // Put span on correct free list.
        FreePagePopSpanLeader(fpm, (*nextkey).first_page);
        FreePagePushSpanLeader(fpm, first_page, newpages);

        // Update key in place.
        (*nextkey).first_page = first_page;
        (*nextkey).npages = newpages;

        // If reducing first key on page, ancestors might need adjustment.
        if nindex == 0 {
            FreePageBtreeAdjustAncestorKeys(fpm, np);
        }

        return (*nextkey).npages;
    }

    // Split leaf page and as many of its ancestors as necessary.
    if result.split_pages > 0 {
        // NB: We could consider various coping strategies here to avoid a
        // split; most obviously, if np != result.page, we could target that
        // page instead.   More complicated shuffling strategies could be
        // possible as well; basically, unless every single leaf page is 100%
        // full, we can jam this key in there if we try hard enough.  It's
        // unlikely that trying that hard is worthwhile, but it's possible we
        // might need to make more than no effort.  For now, we just do the
        // easy thing, which is nothing.

        // If this is a soft insert, it's time to give up.
        if soft {
            return 0;
        }

        // Check whether we need to allocate more btree pages to split.
        if result.split_pages > (*fpm).btree_recycle_count {
            let pages_needed: Size;
            let mut recycle_page: Size = 0;
            let mut i: Size;

            // Allocate the required number of pages and split each one in
            // turn.  This should never fail, because if we've got enough
            // spans of free pages kicking around that we need additional
            // storage space just to remember them all, then we should
            // certainly have enough to expand the btree, which should only
            // ever use a tiny number of pages compared to the number under
            // management.  If it does, something's badly screwed up.
            pages_needed = result.split_pages as Size - (*fpm).btree_recycle_count as Size;
            i = 0;
            while i < pages_needed {
                if !FreePageManagerGetInternal(fpm, 1, &mut recycle_page) {
                    elog!(FATAL, "free page manager btree is corrupt");
                }
                FreePageBtreeRecycle(fpm, recycle_page);
                i += 1;
            }

            // The act of allocating pages to recycle may have invalidated the
            // results of our previous btree research, so repeat it. (We could
            // recheck whether any of our split-avoidance strategies that were
            // not viable before now are, but it hardly seems worthwhile, so
            // we don't bother. Consolidation can't be possible now if it
            // wasn't previously.)
            FreePageBtreeSearch(fpm, first_page, &mut result);

            // The act of allocating pages for use in constructing our btree
            // should never cause any page to become more full, so the new
            // split depth should be no greater than the old one, and perhaps
            // less if we fortuitously allocated a chunk that freed up a slot
            // on the page we need to update.
            Assert!(result.split_pages <= (*fpm).btree_recycle_count);
        }

        // If we still need to perform a split, do it.
        if result.split_pages > 0 {
            let mut split_target: *mut FreePageBtree = result.page;
            let mut child: *mut FreePageBtree = ptr::null_mut();
            let mut key: Size = first_page;

            loop {
                let newsibling: *mut FreePageBtree;
                let parent: *mut FreePageBtree;

                // Identify parent page, which must receive downlink.
                parent = relptr_access(base, &(*split_target).hdr.parent);

                // Split the page - downlink not added yet.
                newsibling = FreePageBtreeSplitPage(fpm, split_target);

                // At this point in the loop, we're always carrying a pending
                // insertion.  On the first pass, it's the actual key we're
                // trying to insert; on subsequent passes, it's the downlink
                // that needs to be added as a result of the split performed
                // during the previous loop iteration.  Since we've just split
                // the page, there's definitely room on one of the two
                // resulting pages.
                if child.is_null() {
                    let index: Size;
                    let insert_into: *mut FreePageBtree;

                    insert_into = if key < (*leaf_key(newsibling, 0)).first_page {
                        split_target
                    } else {
                        newsibling
                    };
                    index = FreePageBtreeSearchLeaf(insert_into, key);
                    FreePageBtreeInsertLeaf(insert_into, index, key, npages);
                    if index == 0 && insert_into == split_target {
                        FreePageBtreeAdjustAncestorKeys(fpm, split_target);
                    }
                } else {
                    let index: Size;
                    let insert_into: *mut FreePageBtree;

                    insert_into = if key < (*internal_key(newsibling, 0)).first_page {
                        split_target
                    } else {
                        newsibling
                    };
                    index = FreePageBtreeSearchInternal(insert_into, key);
                    FreePageBtreeInsertInternal(base, insert_into, index, key, child);
                    relptr_store(base, &mut (*child).hdr.parent, insert_into);
                    if index == 0 && insert_into == split_target {
                        FreePageBtreeAdjustAncestorKeys(fpm, split_target);
                    }
                }

                // If the page we just split has no parent, split the root.
                if parent.is_null() {
                    let newroot: *mut FreePageBtree;

                    newroot = FreePageBtreeGetRecycled(fpm);
                    (*newroot).hdr.magic = FREE_PAGE_INTERNAL_MAGIC;
                    (*newroot).hdr.nused = 2;
                    relptr_store(base, &mut (*newroot).hdr.parent, ptr::null_mut::<FreePageBtree>());
                    (*internal_key(newroot, 0)).first_page = FreePageBtreeFirstKey(split_target);
                    relptr_store(base, &mut (*internal_key(newroot, 0)).child, split_target);
                    relptr_store(base, &mut (*split_target).hdr.parent, newroot);
                    (*internal_key(newroot, 1)).first_page = FreePageBtreeFirstKey(newsibling);
                    relptr_store(base, &mut (*internal_key(newroot, 1)).child, newsibling);
                    relptr_store(base, &mut (*newsibling).hdr.parent, newroot);
                    relptr_store(base, &mut (*fpm).btree_root, newroot);
                    (*fpm).btree_depth += 1;

                    break;
                }

                // If the parent page isn't full, insert the downlink.
                key = (*internal_key(newsibling, 0)).first_page;
                if (*parent).hdr.nused < FPM_ITEMS_PER_INTERNAL_PAGE {
                    let index: Size;

                    index = FreePageBtreeSearchInternal(parent, key);
                    FreePageBtreeInsertInternal(base, parent, index, key, newsibling);
                    relptr_store(base, &mut (*newsibling).hdr.parent, parent);
                    if index == 0 {
                        FreePageBtreeAdjustAncestorKeys(fpm, parent);
                    }
                    break;
                }

                // The parent also needs to be split, so loop around.
                child = newsibling;
                split_target = parent;
            }

            // The loop above did the insert, so just need to update the free
            // list, and we're done.
            FreePagePushSpanLeader(fpm, first_page, npages);

            return npages;
        }
    }

    // Physically add the key to the page.
    Assert!((*result.page).hdr.nused < FPM_ITEMS_PER_LEAF_PAGE);
    FreePageBtreeInsertLeaf(result.page, result.index, first_page, npages);

    // If new first key on page, ancestors might need adjustment.
    if result.index == 0 {
        FreePageBtreeAdjustAncestorKeys(fpm, result.page);
    }

    // Put it on the free list.
    FreePagePushSpanLeader(fpm, first_page, npages);

    npages
}

/// Remove a FreePageSpanLeader from the linked-list that contains it, either
/// because we're changing the size of the span, or because we're allocating it.
unsafe fn FreePagePopSpanLeader(fpm: *mut FreePageManager, pageno: Size) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let span: *mut FreePageSpanLeader;
    let next: *mut FreePageSpanLeader;
    let prev: *mut FreePageSpanLeader;

    span = fpm_page_to_pointer(base, pageno) as *mut FreePageSpanLeader;

    next = relptr_access(base, &(*span).next);
    prev = relptr_access(base, &(*span).prev);
    if !next.is_null() {
        relptr_copy(&mut (*next).prev, &(*span).prev);
    }
    if !prev.is_null() {
        relptr_copy(&mut (*prev).next, &(*span).next);
    } else {
        let f: Size = Min((*span).npages, FPM_NUM_FREELISTS) - 1;

        Assert!(relptr_offset(&(*fpm).freelist[f]) == pageno * FPM_PAGE_SIZE);
        relptr_copy(&mut (*fpm).freelist[f], &(*span).next);
    }
}

/// Initialize a new FreePageSpanLeader and put it on the appropriate free list.
unsafe fn FreePagePushSpanLeader(fpm: *mut FreePageManager, first_page: Size, npages: Size) {
    let base: *mut c_char = fpm_segment_base(fpm);
    let f: Size = Min(npages, FPM_NUM_FREELISTS) - 1;
    let head: *mut FreePageSpanLeader = relptr_access(base, &(*fpm).freelist[f]);
    let span: *mut FreePageSpanLeader;

    span = fpm_page_to_pointer(base, first_page) as *mut FreePageSpanLeader;
    (*span).magic = FREE_PAGE_SPAN_LEADER_MAGIC;
    (*span).npages = npages;
    relptr_store(base, &mut (*span).next, head);
    relptr_store(base, &mut (*span).prev, ptr::null_mut::<FreePageSpanLeader>());
    if !head.is_null() {
        relptr_store(base, &mut (*head).prev, span);
    }
    relptr_store(base, &mut (*fpm).freelist[f], span);
}
