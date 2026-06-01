//! nbtsplitloc.rs
//!   Choose split point code for Postgres btree implementation.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtsplitloc.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtsplitloc.c
//!
//! #include mapping:
//!   "postgres.h"       -> crate::prelude::*
//!   "access/nbtree.h"  -> BTPageOpaque/BT* macros/_bt_keep_natts_fast (STUB below;
//!                         no ported nbtree.h module yet -- TODO(pg-port))
//!   "common/int.h"     -> pg_cmp_s16 (crate::common::int)

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

// Real, already-ported homes.
use crate::access::common::indextuple::{IndexTuple, IndexTupleData, IndexTupleSize};
use crate::common::int::pg_cmp_s16;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{
    PageGetExactFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetPageSize,
    SizeOfPageHeaderData, Page,
};
use crate::storage::itemid::{ItemId, ItemIdData, ItemIdGetLength};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev};
use crate::utils::rel::{Relation, RelationGetRelationName};

extern "C" {
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> c_int>,
    );
}

// ----------------------------------------------------------------------------
// STUBS: symbols whose real home (access/nbtree.h, nbtutils.c) has not been
// ported yet.  Each is a minimal local declaration mirroring the sibling
// nbtdedup.rs conventions.
// TODO(pg-port): real definitions live in postgres/src/include/access/nbtree.h
// (and nbtutils.c for _bt_keep_natts_fast).
// ----------------------------------------------------------------------------

/// TODO(pg-port): BTPageOpaqueData / BTPageOpaque live in access/nbtree.h.
#[repr(C)]
pub struct BTPageOpaqueData {
    pub btpo_prev: BlockNumber,
    pub btpo_next: BlockNumber,
    pub btpo_level: u32,
    pub btpo_flags: u16,
    pub btpo_cycleid: u16,
}
pub type BTPageOpaque = *mut BTPageOpaqueData;

// nbtree.h constants.
/// TODO(pg-port): from access/nbtree.h.
pub const BTREE_NONLEAF_FILLFACTOR: c_int = 70;
/// TODO(pg-port): from access/nbtree.h.
pub const BTREE_SINGLEVAL_FILLFACTOR: c_int = 96;

/// TODO(pg-port): BTPageGetOpaque() (access/nbtree.h).
unsafe fn BTPageGetOpaque(page: Page) -> BTPageOpaque {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_RIGHTMOST() (access/nbtree.h).
unsafe fn P_RIGHTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_ISLEAF() (access/nbtree.h).
unsafe fn P_ISLEAF(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_FIRSTDATAKEY() (access/nbtree.h).
unsafe fn P_FIRSTDATAKEY(opaque: BTPageOpaque) -> OffsetNumber {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_HIKEY (access/nbtree.h).
const P_HIKEY: OffsetNumber = 1;
/// TODO(pg-port): P_FIRSTKEY (access/nbtree.h).
const P_FIRSTKEY: OffsetNumber = 2;

/// TODO(pg-port): BTGetFillFactor() (access/nbtree.h / utils/rel.h).
unsafe fn BTGetFillFactor(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): IndexRelationGetNumberOfKeyAttributes() (access/relscan.h via rel.h).
unsafe fn IndexRelationGetNumberOfKeyAttributes(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): _bt_keep_natts_fast() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_keep_natts_fast(rel: Relation, lastleft: IndexTuple, firstright: IndexTuple) -> c_int {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): BTreeTupleIsPosting() (access/nbtree.h).
unsafe fn BTreeTupleIsPosting(itup: IndexTuple) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetPostingOffset() (access/nbtree.h).
unsafe fn BTreeTupleGetPostingOffset(posting: IndexTuple) -> uint32 {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum FindSplitStrat {
    /* strategy for searching through materialized list of split points */
    SPLIT_DEFAULT,        /* give some weight to truncation */
    SPLIT_MANY_DUPLICATES, /* find minimally distinguishing point */
    SPLIT_SINGLE_VALUE,   /* leave left page almost full */
}
use FindSplitStrat::*;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SplitPoint {
    /* details of free space left by split */
    pub curdelta: int16,  /* current leftfree/rightfree delta */
    pub leftfree: int16,  /* space left on left page post-split */
    pub rightfree: int16, /* space left on right page post-split */

    /* split point identifying fields (returned by _bt_findsplitloc) */
    pub firstrightoff: OffsetNumber, /* first origpage item on rightpage */
    pub newitemonleft: bool,         /* new item goes on left, or right? */
}

#[repr(C)]
pub struct FindSplitData {
    /* context data for _bt_recsplitloc */
    pub rel: Relation,          /* index relation */
    pub origpage: Page,         /* page undergoing split */
    pub newitem: IndexTuple,    /* new item (cause of page split) */
    pub newitemsz: Size,        /* size of newitem (includes line pointer) */
    pub is_leaf: bool,          /* T if splitting a leaf page */
    pub is_rightmost: bool,     /* T if splitting rightmost page on level */
    pub newitemoff: OffsetNumber, /* where the new item is to be inserted */
    pub leftspace: c_int,       /* space available for items on left page */
    pub rightspace: c_int,      /* space available for items on right page */
    pub olddataitemstotal: c_int, /* space taken by old items */
    pub minfirstrightsz: Size,  /* smallest firstright size */

    /* candidate split point data */
    pub maxsplits: c_int,       /* maximum number of splits */
    pub nsplits: c_int,         /* current number of splits */
    pub splits: *mut SplitPoint, /* all candidate split points for page */
    pub interval: c_int,        /* current range of acceptable split points */
}

/*
 *	_bt_findsplitloc() -- find an appropriate place to split a page.
 *
 * The main goal here is to equalize the free space that will be on each
 * split page, *after accounting for the inserted tuple*.  (If we fail to
 * account for it, we might find ourselves with too little room on the page
 * that it needs to go into!)
 *
 * If the page is the rightmost page on its level, we instead try to arrange
 * to leave the left split page fillfactor% full.  In this way, when we are
 * inserting successively increasing keys (consider sequences, timestamps,
 * etc) we will end up with a tree whose pages are about fillfactor% full,
 * instead of the 50% full result that we'd get without this special case.
 * This is the same as nbtsort.c produces for a newly-created tree.  Note
 * that leaf and nonleaf pages use different fillfactors.  Note also that
 * there are a number of further special cases where fillfactor is not
 * applied in the standard way.
 *
 * We are passed the intended insert position of the new tuple, expressed as
 * the offsetnumber of the tuple it must go in front of (this could be
 * maxoff+1 if the tuple is to go at the end).  The new tuple itself is also
 * passed, since it's needed to give some weight to how effective suffix
 * truncation will be.  The implementation picks the split point that
 * maximizes the effectiveness of suffix truncation from a small list of
 * alternative candidate split points that leave each side of the split with
 * about the same share of free space.  Suffix truncation is secondary to
 * equalizing free space, except in cases with large numbers of duplicates.
 * Note that it is always assumed that caller goes on to perform truncation,
 * even with pg_upgrade'd indexes where that isn't actually the case
 * (!heapkeyspace indexes).  See nbtree/README for more information about
 * suffix truncation.
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page (which is called firstrightoff), plus a boolean
 * indicating whether the new tuple goes on the left or right page.  You
 * can think of the returned state as a point _between_ two adjacent data
 * items (lastleft and firstright data items) on an imaginary version of
 * origpage that already includes newitem.  The bool is necessary to
 * disambiguate the case where firstrightoff == newitemoff (i.e. it is
 * sometimes needed to determine if the firstright tuple for the split is
 * newitem rather than the tuple from origpage at offset firstrightoff).
 */
pub unsafe fn _bt_findsplitloc(
    rel: Relation,
    origpage: Page,
    newitemoff: OffsetNumber,
    mut newitemsz: Size,
    newitem: IndexTuple,
    newitemonleft: *mut bool,
) -> OffsetNumber {
    let opaque: BTPageOpaque;
    let mut leftspace: c_int;
    let mut rightspace: c_int;
    let olddataitemstotal: c_int;
    let mut olddataitemstoleft: c_int;
    let perfectpenalty: c_int;
    let leaffillfactor: c_int;
    let mut state: FindSplitData = std::mem::zeroed();
    let mut strategy: FindSplitStrat = SPLIT_DEFAULT;
    let mut itemid: ItemId;
    let mut offnum: OffsetNumber;
    let maxoff: OffsetNumber;
    let firstrightoff: OffsetNumber;
    let mut fillfactormult: f64;
    let mut usemult: bool = false;
    let mut leftpage: SplitPoint;
    let mut rightpage: SplitPoint;

    opaque = BTPageGetOpaque(origpage);
    maxoff = PageGetMaxOffsetNumber(origpage);

    /* Total free space available on a btree page, after fixed overhead */
    leftspace = (PageGetPageSize(origpage)
        - SizeOfPageHeaderData
        - MAXALIGN(size_of::<BTPageOpaqueData>())) as c_int;
    rightspace = leftspace;

    /* The right page will have the same high key as the old page */
    if !P_RIGHTMOST(opaque) {
        itemid = PageGetItemId(origpage, P_HIKEY);
        rightspace -= (MAXALIGN(ItemIdGetLength(itemid) as usize) + size_of::<ItemIdData>()) as c_int;
    }

    /* Count up total space in data items before actually scanning 'em */
    olddataitemstotal = rightspace - PageGetExactFreeSpace(origpage) as c_int;
    leaffillfactor = BTGetFillFactor(rel);

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    newitemsz += size_of::<ItemIdData>();
    state.rel = rel;
    state.origpage = origpage;
    state.newitem = newitem;
    state.newitemsz = newitemsz;
    state.is_leaf = P_ISLEAF(opaque);
    state.is_rightmost = P_RIGHTMOST(opaque);
    state.leftspace = leftspace;
    state.rightspace = rightspace;
    state.olddataitemstotal = olddataitemstotal;
    state.minfirstrightsz = usize::MAX; /* SIZE_MAX */
    state.newitemoff = newitemoff;

    /* newitem cannot be a posting list item */
    Assert!(!BTreeTupleIsPosting(newitem));

    /*
     * nsplits should never exceed maxoff because there will be at most as
     * many candidate split points as there are points _between_ tuples, once
     * you imagine that the new item is already on the original page (the
     * final number of splits may be slightly lower because not all points
     * between tuples will be legal).
     */
    state.maxsplits = maxoff as c_int;
    state.splits = palloc(size_of::<SplitPoint>() * state.maxsplits as usize) as *mut SplitPoint;
    state.nsplits = 0;

    /*
     * Scan through the data items and calculate space usage for a split at
     * each possible position
     */
    olddataitemstoleft = 0;

    offnum = P_FIRSTDATAKEY(opaque);
    while offnum <= maxoff {
        let itemsz: Size;

        itemid = PageGetItemId(origpage, offnum);
        itemsz = MAXALIGN(ItemIdGetLength(itemid) as usize) + size_of::<ItemIdData>();

        /*
         * When item offset number is not newitemoff, neither side of the
         * split can be newitem.  Record a split after the previous data item
         * from original page, but before the current data item from original
         * page. (_bt_recsplitloc() will reject the split when there are no
         * previous items, which we rely on.)
         */
        if offnum < newitemoff {
            _bt_recsplitloc(&mut state, offnum, false, olddataitemstoleft, itemsz);
        } else if offnum > newitemoff {
            _bt_recsplitloc(&mut state, offnum, true, olddataitemstoleft, itemsz);
        } else {
            /*
             * Record a split after all "offnum < newitemoff" original page
             * data items, but before newitem
             */
            _bt_recsplitloc(&mut state, offnum, false, olddataitemstoleft, itemsz);

            /*
             * Record a split after newitem, but before data item from
             * original page at offset newitemoff/current offset
             */
            _bt_recsplitloc(&mut state, offnum, true, olddataitemstoleft, itemsz);
        }

        olddataitemstoleft += itemsz as c_int;
        offnum = OffsetNumberNext(offnum);
    }

    /*
     * Record a split after all original page data items, but before newitem.
     * (Though only when it's possible that newitem will end up alone on new
     * right page.)
     */
    Assert!(olddataitemstoleft == olddataitemstotal);
    if newitemoff > maxoff {
        _bt_recsplitloc(&mut state, newitemoff, false, olddataitemstotal, 0);
    }

    /*
     * I believe it is not possible to fail to find a feasible split, but just
     * in case ...
     */
    if state.nsplits == 0 {
        elog!(
            ERROR,
            "could not find a feasible split point for index \"{}\"",
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /*
     * Start search for a split point among list of legal split points.  Give
     * primary consideration to equalizing available free space in each half
     * of the split initially (start with default strategy), while applying
     * rightmost and split-after-new-item optimizations where appropriate.
     * Either of the two other fallback strategies may be required for cases
     * with a large number of duplicates around the original/space-optimal
     * split point.
     *
     * Default strategy gives some weight to suffix truncation in deciding a
     * split point on leaf pages.  It attempts to select a split point where a
     * distinguishing attribute appears earlier in the new high key for the
     * left side of the split, in order to maximize the number of trailing
     * attributes that can be truncated away.  Only candidate split points
     * that imply an acceptable balance of free space on each side are
     * considered.  See _bt_defaultinterval().
     */
    if !state.is_leaf {
        /* fillfactormult only used on rightmost page */
        usemult = state.is_rightmost;
        fillfactormult = BTREE_NONLEAF_FILLFACTOR as f64 / 100.0;
    } else if state.is_rightmost {
        /* Rightmost leaf page --  fillfactormult always used */
        usemult = true;
        fillfactormult = leaffillfactor as f64 / 100.0;
    } else if _bt_afternewitemoff(&mut state, maxoff, leaffillfactor, &mut usemult) {
        /*
         * New item inserted at rightmost point among a localized grouping on
         * a leaf page -- apply "split after new item" optimization, either by
         * applying leaf fillfactor multiplier, or by choosing the exact split
         * point that leaves newitem as lastleft. (usemult is set for us.)
         */
        if usemult {
            /* fillfactormult should be set based on leaf fillfactor */
            fillfactormult = leaffillfactor as f64 / 100.0;
        } else {
            /* find precise split point after newitemoff */
            let mut found = false;
            let mut i = 0;
            while i < state.nsplits {
                let split: *mut SplitPoint = state.splits.add(i as usize);

                if (*split).newitemonleft && newitemoff == (*split).firstrightoff {
                    pfree(state.splits as *mut c_void);
                    *newitemonleft = true;
                    return newitemoff;
                }
                i += 1;
            }

            /*
             * Cannot legally split after newitemoff; proceed with split
             * without using fillfactor multiplier.  This is defensive, and
             * should never be needed in practice.
             */
            let _ = found;
            fillfactormult = 0.50;
        }
    } else {
        /* Other leaf page.  50:50 page split. */
        usemult = false;
        /* fillfactormult not used, but be tidy */
        fillfactormult = 0.50;
    }

    /*
     * Save leftmost and rightmost splits for page before original ordinal
     * sort order is lost by delta/fillfactormult sort
     */
    leftpage = *state.splits.add(0);
    rightpage = *state.splits.add((state.nsplits - 1) as usize);

    /* Give split points a fillfactormult-wise delta, and sort on deltas */
    _bt_deltasortsplits(&mut state, fillfactormult, usemult);

    /* Determine split interval for default strategy */
    state.interval = _bt_defaultinterval(&mut state);

    /*
     * Determine if default strategy/split interval will produce a
     * sufficiently distinguishing split, or if we should change strategies.
     * Alternative strategies change the range of split points that are
     * considered acceptable (split interval), and possibly change
     * fillfactormult, in order to deal with pages with a large number of
     * duplicates gracefully.
     *
     * Pass low and high splits for the entire page (actually, they're for an
     * imaginary version of the page that includes newitem).  These are used
     * when the initial split interval encloses split points that are full of
     * duplicates, and we need to consider if it's even possible to avoid
     * appending a heap TID.
     */
    perfectpenalty = _bt_strategy(&mut state, &mut leftpage, &mut rightpage, &mut strategy);

    if strategy == SPLIT_DEFAULT {
        /*
         * Default strategy worked out (always works out with internal page).
         * Original split interval still stands.
         */
    }
    /*
     * Many duplicates strategy is used when a heap TID would otherwise be
     * appended, but the page isn't completely full of logical duplicates.
     *
     * The split interval is widened to include all legal candidate split
     * points.  There might be a few as two distinct values in the whole-page
     * split interval, though it's also possible that most of the values on
     * the page are unique.  The final split point will either be to the
     * immediate left or to the immediate right of the group of duplicate
     * tuples that enclose the first/delta-optimal split point (perfect
     * penalty was set so that the lowest delta split point that avoids
     * appending a heap TID will be chosen).  Maximizing the number of
     * attributes that can be truncated away is not a goal of the many
     * duplicates strategy.
     *
     * Single value strategy is used when it is impossible to avoid appending
     * a heap TID.  It arranges to leave the left page very full.  This
     * maximizes space utilization in cases where tuples with the same
     * attribute values span many pages.  Newly inserted duplicates will tend
     * to have higher heap TID values, so we'll end up splitting to the right
     * consistently.  (Single value strategy is harmless though not
     * particularly useful with !heapkeyspace indexes.)
     */
    else if strategy == SPLIT_MANY_DUPLICATES {
        Assert!(state.is_leaf);
        /* Shouldn't try to truncate away extra user attributes */
        Assert!(perfectpenalty == IndexRelationGetNumberOfKeyAttributes(state.rel));
        /* No need to resort splits -- no change in fillfactormult/deltas */
        state.interval = state.nsplits;
    } else if strategy == SPLIT_SINGLE_VALUE {
        Assert!(state.is_leaf);
        /* Split near the end of the page */
        usemult = true;
        fillfactormult = BTREE_SINGLEVAL_FILLFACTOR as f64 / 100.0;
        /* Resort split points with new delta */
        _bt_deltasortsplits(&mut state, fillfactormult, usemult);
        /* Appending a heap TID is unavoidable, so interval of 1 is fine */
        state.interval = 1;
    }

    /*
     * Search among acceptable split points (using final split interval) for
     * the entry that has the lowest penalty, and is therefore expected to
     * maximize fan-out.  Sets *newitemonleft for us.
     */
    firstrightoff = _bt_bestsplitloc(&mut state, perfectpenalty, newitemonleft, strategy);
    pfree(state.splits as *mut c_void);

    return firstrightoff;
}

/*
 * Subroutine to record a particular point between two tuples (possibly the
 * new item) on page (ie, combination of firstrightoff and newitemonleft
 * settings) in *state for later analysis.  This is also a convenient point to
 * check if the split is legal (if it isn't, it won't be recorded).
 *
 * firstrightoff is the offset of the first item on the original page that
 * goes to the right page, and firstrightofforigpagetuplesz is the size of
 * that tuple.  firstrightoff can be > max offset, which means that all the
 * old items go to the left page and only the new item goes to the right page.
 * We don't actually use firstrightofforigpagetuplesz in that case (actually,
 * we don't use it for _any_ split where the firstright tuple happens to be
 * newitem).
 *
 * olddataitemstoleft is the total size of all old items to the left of the
 * split point that is recorded here when legal.  Should not include
 * newitemsz, since that is handled here.
 */
unsafe fn _bt_recsplitloc(
    state: *mut FindSplitData,
    firstrightoff: OffsetNumber,
    newitemonleft: bool,
    olddataitemstoleft: c_int,
    firstrightofforigpagetuplesz: Size,
) {
    let mut leftfree: int16;
    let mut rightfree: int16;
    let firstrightsz: Size;
    let mut postingsz: Size = 0;
    let newitemisfirstright: bool;

    /* Is the new item going to be split point's firstright tuple? */
    newitemisfirstright = firstrightoff == (*state).newitemoff && !newitemonleft;

    if newitemisfirstright {
        firstrightsz = (*state).newitemsz;
    } else {
        firstrightsz = firstrightofforigpagetuplesz;

        /*
         * Calculate suffix truncation space saving when firstright tuple is a
         * posting list tuple, though only when the tuple is over 64 bytes
         * including line pointer overhead (arbitrary).  This avoids accessing
         * the tuple in cases where its posting list must be very small (if
         * tuple has one at all).
         *
         * Note: We don't do this in the case where firstright tuple is
         * newitem, since newitem cannot have a posting list.
         */
        if (*state).is_leaf && firstrightsz > 64 {
            let itemid: ItemId;
            let newhighkey: IndexTuple;

            itemid = PageGetItemId((*state).origpage, firstrightoff);
            newhighkey = PageGetItem((*state).origpage, itemid) as IndexTuple;

            if BTreeTupleIsPosting(newhighkey) {
                postingsz =
                    IndexTupleSize(newhighkey) - BTreeTupleGetPostingOffset(newhighkey) as Size;
            }
        }
    }

    /* Account for all the old tuples */
    leftfree = ((*state).leftspace - olddataitemstoleft) as int16;
    rightfree = ((*state).rightspace - ((*state).olddataitemstotal - olddataitemstoleft)) as int16;

    /*
     * The first item on the right page becomes the high key of the left page;
     * therefore it counts against left space as well as right space (we
     * cannot assume that suffix truncation will make it any smaller).  When
     * index has included attributes, then those attributes of left page high
     * key will be truncated leaving that page with slightly more free space.
     * However, that shouldn't affect our ability to find valid split
     * location, since we err in the direction of being pessimistic about free
     * space on the left half.  Besides, even when suffix truncation of
     * non-TID attributes occurs, the new high key often won't even be a
     * single MAXALIGN() quantum smaller than the firstright tuple it's based
     * on.
     *
     * If we are on the leaf level, assume that suffix truncation cannot avoid
     * adding a heap TID to the left half's new high key when splitting at the
     * leaf level.  In practice the new high key will often be smaller and
     * will rarely be larger, but conservatively assume the worst case.  We do
     * go to the trouble of subtracting away posting list overhead, though
     * only when it looks like it will make an appreciable difference.
     * (Posting lists are the only case where truncation will typically make
     * the final high key far smaller than firstright, so being a bit more
     * precise there noticeably improves the balance of free space.)
     */
    if (*state).is_leaf {
        leftfree -= (firstrightsz + MAXALIGN(size_of::<ItemPointerData>()) - postingsz) as int16;
    } else {
        leftfree -= firstrightsz as int16;
    }

    /* account for the new item */
    if newitemonleft {
        leftfree -= (*state).newitemsz as int16;
    } else {
        rightfree -= (*state).newitemsz as int16;
    }

    /*
     * If we are not on the leaf level, we will be able to discard the key
     * data from the first item that winds up on the right page.
     */
    if !(*state).is_leaf {
        rightfree += firstrightsz as int16
            - (MAXALIGN(size_of::<IndexTupleData>()) + size_of::<ItemIdData>()) as int16;
    }

    /* Record split if legal */
    if leftfree >= 0 && rightfree >= 0 {
        Assert!((*state).nsplits < (*state).maxsplits);

        /* Determine smallest firstright tuple size among legal splits */
        (*state).minfirstrightsz = Min((*state).minfirstrightsz, firstrightsz);

        let slot: *mut SplitPoint = (*state).splits.add((*state).nsplits as usize);
        (*slot).curdelta = 0;
        (*slot).leftfree = leftfree;
        (*slot).rightfree = rightfree;
        (*slot).firstrightoff = firstrightoff;
        (*slot).newitemonleft = newitemonleft;
        (*state).nsplits += 1;
    }
}

/*
 * Subroutine to assign space deltas to materialized array of candidate split
 * points based on current fillfactor, and to sort array using that fillfactor
 */
unsafe fn _bt_deltasortsplits(state: *mut FindSplitData, fillfactormult: f64, usemult: bool) {
    let mut i = 0;
    while i < (*state).nsplits {
        let split: *mut SplitPoint = (*state).splits.add(i as usize);
        let mut delta: int16;

        if usemult {
            delta = (fillfactormult * (*split).leftfree as f64
                - (1.0 - fillfactormult) * (*split).rightfree as f64) as int16;
        } else {
            delta = (*split).leftfree - (*split).rightfree;
        }

        if delta < 0 {
            delta = -delta;
        }

        /* Save delta */
        (*split).curdelta = delta;
        i += 1;
    }

    qsort(
        (*state).splits as *mut c_void,
        (*state).nsplits as usize,
        size_of::<SplitPoint>(),
        Some(_bt_splitcmp),
    );
}

/*
 * qsort-style comparator used by _bt_deltasortsplits()
 */
unsafe extern "C" fn _bt_splitcmp(arg1: *const c_void, arg2: *const c_void) -> c_int {
    let split1: *const SplitPoint = arg1 as *const SplitPoint;
    let split2: *const SplitPoint = arg2 as *const SplitPoint;

    return pg_cmp_s16((*split1).curdelta, (*split2).curdelta);
}

/*
 * Subroutine to determine whether or not a non-rightmost leaf page should be
 * split immediately after the would-be original page offset for the
 * new/incoming tuple (or should have leaf fillfactor applied when new item is
 * to the right on original page).  This is appropriate when there is a
 * pattern of localized monotonically increasing insertions into a composite
 * index, where leading attribute values form local groupings, and we
 * anticipate further insertions of the same/current grouping (new item's
 * grouping) in the near future.  This can be thought of as a variation on
 * applying leaf fillfactor during rightmost leaf page splits, since cases
 * that benefit will converge on packing leaf pages leaffillfactor% full over
 * time.
 *
 * We may leave extra free space remaining on the rightmost page of a "most
 * significant column" grouping of tuples if that grouping never ends up
 * having future insertions that use the free space.  That effect is
 * self-limiting; a future grouping that becomes the "nearest on the right"
 * grouping of the affected grouping usually puts the extra free space to good
 * use.
 *
 * Caller uses optimization when routine returns true, though the exact action
 * taken by caller varies.  Caller uses original leaf page fillfactor in
 * standard way rather than using the new item offset directly when *usemult
 * was also set to true here.  Otherwise, caller applies optimization by
 * locating the legal split point that makes the new tuple the lastleft tuple
 * for the split.
 */
unsafe fn _bt_afternewitemoff(
    state: *mut FindSplitData,
    maxoff: OffsetNumber,
    leaffillfactor: c_int,
    usemult: *mut bool,
) -> bool {
    let nkeyatts: int16;
    let itemid: ItemId;
    let tup: IndexTuple;
    let keepnatts: c_int;

    Assert!((*state).is_leaf && !(*state).is_rightmost);

    nkeyatts = IndexRelationGetNumberOfKeyAttributes((*state).rel) as int16;

    /* Single key indexes not considered here */
    if nkeyatts == 1 {
        return false;
    }

    /* Ascending insertion pattern never inferred when new item is first */
    if (*state).newitemoff == P_FIRSTKEY {
        return false;
    }

    /*
     * Only apply optimization on pages with equisized tuples, since ordinal
     * keys are likely to be fixed-width.  Testing if the new tuple is
     * variable width directly might also work, but that fails to apply the
     * optimization to indexes with a numeric_ops attribute.
     *
     * Conclude that page has equisized tuples when the new item is the same
     * width as the smallest item observed during pass over page, and other
     * non-pivot tuples must be the same width as well.  (Note that the
     * possibly-truncated existing high key isn't counted in
     * olddataitemstotal, and must be subtracted from maxoff.)
     */
    if (*state).newitemsz != (*state).minfirstrightsz {
        return false;
    }
    if (*state).newitemsz * (maxoff - 1) as usize != (*state).olddataitemstotal as usize {
        return false;
    }

    /*
     * Avoid applying optimization when tuples are wider than a tuple
     * consisting of two non-NULL int8/int64 attributes (or four non-NULL
     * int4/int32 attributes)
     */
    if (*state).newitemsz
        > MAXALIGN(size_of::<IndexTupleData>() + size_of::<i64>() * 2) + size_of::<ItemIdData>()
    {
        return false;
    }

    /*
     * At least the first attribute's value must be equal to the corresponding
     * value in previous tuple to apply optimization.  New item cannot be a
     * duplicate, either.
     *
     * Handle case where new item is to the right of all items on the existing
     * page.  This is suggestive of monotonically increasing insertions in
     * itself, so the "heap TID adjacency" test is not applied here.
     */
    if (*state).newitemoff > maxoff {
        let itemid = PageGetItemId((*state).origpage, maxoff);
        let tup = PageGetItem((*state).origpage, itemid) as IndexTuple;
        let keepnatts = _bt_keep_natts_fast((*state).rel, tup, (*state).newitem);

        if keepnatts > 1 && keepnatts <= nkeyatts as c_int {
            *usemult = true;
            return true;
        }

        return false;
    }

    /*
     * "Low cardinality leading column, high cardinality suffix column"
     * indexes with a random insertion pattern (e.g., an index with a boolean
     * column, such as an index on '(book_is_in_print, book_isbn)') present us
     * with a risk of consistently misapplying the optimization.  We're
     * willing to accept very occasional misapplication of the optimization,
     * provided the cases where we get it wrong are rare and self-limiting.
     *
     * Heap TID adjacency strongly suggests that the item just to the left was
     * inserted very recently, which limits overapplication of the
     * optimization.  Besides, all inappropriate cases triggered here will
     * still split in the middle of the page on average.
     */
    itemid = PageGetItemId((*state).origpage, OffsetNumberPrev((*state).newitemoff));
    tup = PageGetItem((*state).origpage, itemid) as IndexTuple;
    /* Do cheaper test first */
    if BTreeTupleIsPosting(tup)
        || !_bt_adjacenthtid(&raw mut (*tup).t_tid, &raw mut (*(*state).newitem).t_tid)
    {
        return false;
    }
    /* Check same conditions as rightmost item case, too */
    keepnatts = _bt_keep_natts_fast((*state).rel, tup, (*state).newitem);

    if keepnatts > 1 && keepnatts <= nkeyatts as c_int {
        let interp: f64 = (*state).newitemoff as f64 / (maxoff as f64 + 1.0);
        let leaffillfactormult: f64 = leaffillfactor as f64 / 100.0;

        /*
         * Don't allow caller to split after a new item when it will result in
         * a split point to the right of the point that a leaf fillfactor
         * split would use -- have caller apply leaf fillfactor instead
         */
        *usemult = interp > leaffillfactormult;

        return true;
    }

    return false;
}

/*
 * Subroutine for determining if two heap TIDS are "adjacent".
 *
 * Adjacent means that the high TID is very likely to have been inserted into
 * heap relation immediately after the low TID, probably during the current
 * transaction.
 */
unsafe fn _bt_adjacenthtid(lowhtid: ItemPointer, highhtid: ItemPointer) -> bool {
    let lowblk: BlockNumber;
    let highblk: BlockNumber;

    lowblk = ItemPointerGetBlockNumber(lowhtid);
    highblk = ItemPointerGetBlockNumber(highhtid);

    /* Make optimistic assumption of adjacency when heap blocks match */
    if lowblk == highblk {
        return true;
    }

    /* When heap block one up, second offset should be FirstOffsetNumber */
    if lowblk + 1 == highblk && ItemPointerGetOffsetNumber(highhtid) == FirstOffsetNumber {
        return true;
    }

    return false;
}

/*
 * Subroutine to find the "best" split point among candidate split points.
 * The best split point is the split point with the lowest penalty among split
 * points that fall within current/final split interval.  Penalty is an
 * abstract score, with a definition that varies depending on whether we're
 * splitting a leaf page or an internal page.  See _bt_split_penalty() for
 * details.
 *
 * "perfectpenalty" is assumed to be the lowest possible penalty among
 * candidate split points.  This allows us to return early without wasting
 * cycles on calculating the first differing attribute for all candidate
 * splits when that clearly cannot improve our choice (or when we only want a
 * minimally distinguishing split point, and don't want to make the split any
 * more unbalanced than is necessary).
 *
 * We return the index of the first existing tuple that should go on the right
 * page, plus a boolean indicating if new item is on left of split point.
 */
unsafe fn _bt_bestsplitloc(
    state: *mut FindSplitData,
    perfectpenalty: c_int,
    newitemonleft: *mut bool,
    strategy: FindSplitStrat,
) -> OffsetNumber {
    let mut bestpenalty: c_int;
    let mut lowsplit: c_int;
    let highsplit: c_int = Min((*state).interval, (*state).nsplits);
    let mut final_: *mut SplitPoint;

    bestpenalty = c_int::MAX; /* INT_MAX */
    lowsplit = 0;
    let mut i = lowsplit;
    while i < highsplit {
        let penalty: c_int;

        penalty = _bt_split_penalty(state, (*state).splits.add(i as usize));

        if penalty < bestpenalty {
            bestpenalty = penalty;
            lowsplit = i;
        }

        if penalty <= perfectpenalty {
            break;
        }
        i += 1;
    }

    final_ = (*state).splits.add(lowsplit as usize);

    /*
     * There is a risk that the "many duplicates" strategy will repeatedly do
     * the wrong thing when there are monotonically decreasing insertions to
     * the right of a large group of duplicates.   Repeated splits could leave
     * a succession of right half pages with free space that can never be
     * used.  This must be avoided.
     *
     * Consider the example of the leftmost page in a single integer attribute
     * NULLS FIRST index which is almost filled with NULLs.  Monotonically
     * decreasing integer insertions might cause the same leftmost page to
     * split repeatedly at the same point.  Each split derives its new high
     * key from the lowest current value to the immediate right of the large
     * group of NULLs, which will always be higher than all future integer
     * insertions, directing all future integer insertions to the same
     * leftmost page.
     */
    if strategy == SPLIT_MANY_DUPLICATES
        && !(*state).is_rightmost
        && !(*final_).newitemonleft
        && (*final_).firstrightoff >= (*state).newitemoff
        && (*final_).firstrightoff < (*state).newitemoff + 9
    {
        /*
         * Avoid the problem by performing a 50:50 split when the new item is
         * just to the right of the would-be "many duplicates" split point.
         * (Note that the test used for an insert that is "just to the right"
         * of the split point is conservative.)
         */
        final_ = (*state).splits.add(0);
    }

    *newitemonleft = (*final_).newitemonleft;
    return (*final_).firstrightoff;
}

const LEAF_SPLIT_DISTANCE: f64 = 0.050;
const INTERNAL_SPLIT_DISTANCE: f64 = 0.075;

/*
 * Return a split interval to use for the default strategy.  This is a limit
 * on the number of candidate split points to give further consideration to.
 * Only a fraction of all candidate splits points (those located at the start
 * of the now-sorted splits array) fall within the split interval.  Split
 * interval is applied within _bt_bestsplitloc().
 *
 * Split interval represents an acceptable range of split points -- those that
 * have leftfree and rightfree values that are acceptably balanced.  The final
 * split point chosen is the split point with the lowest "penalty" among split
 * points in this split interval (unless we change our entire strategy, in
 * which case the interval also changes -- see _bt_strategy()).
 *
 * The "Prefix B-Trees" paper calls split interval sigma l for leaf splits,
 * and sigma b for internal ("branch") splits.  It's hard to provide a
 * theoretical justification for the size of the split interval, though it's
 * clear that a small split interval can make tuples on level L+1 much smaller
 * on average, without noticeably affecting space utilization on level L.
 * (Note that the way that we calculate split interval might need to change if
 * suffix truncation is taught to truncate tuples "within" the last
 * attribute/datum for data types like text, which is more or less how it is
 * assumed to work in the paper.)
 */
unsafe fn _bt_defaultinterval(state: *mut FindSplitData) -> c_int {
    let spaceoptimal: *mut SplitPoint;
    let tolerance: int16;
    let lowleftfree: int16;
    let lowrightfree: int16;
    let highleftfree: int16;
    let highrightfree: int16;

    /*
     * Determine leftfree and rightfree values that are higher and lower than
     * we're willing to tolerate.  Note that the final split interval will be
     * about 10% of nsplits in the common case where all non-pivot tuples
     * (data items) from a leaf page are uniformly sized.  We're a bit more
     * aggressive when splitting internal pages.
     */
    if (*state).is_leaf {
        tolerance = ((*state).olddataitemstotal as f64 * LEAF_SPLIT_DISTANCE) as int16;
    } else {
        tolerance = ((*state).olddataitemstotal as f64 * INTERNAL_SPLIT_DISTANCE) as int16;
    }

    /* First candidate split point is the most evenly balanced */
    spaceoptimal = (*state).splits;
    lowleftfree = (*spaceoptimal).leftfree - tolerance;
    lowrightfree = (*spaceoptimal).rightfree - tolerance;
    highleftfree = (*spaceoptimal).leftfree + tolerance;
    highrightfree = (*spaceoptimal).rightfree + tolerance;

    /*
     * Iterate through split points, starting from the split immediately after
     * 'spaceoptimal'.  Find the first split point that divides free space so
     * unevenly that including it in the split interval would be unacceptable.
     */
    let mut i = 1;
    while i < (*state).nsplits {
        let split: *mut SplitPoint = (*state).splits.add(i as usize);

        /* Cannot use curdelta here, since its value is often weighted */
        if (*split).leftfree < lowleftfree
            || (*split).rightfree < lowrightfree
            || (*split).leftfree > highleftfree
            || (*split).rightfree > highrightfree
        {
            return i;
        }
        i += 1;
    }

    return (*state).nsplits;
}

/*
 * Subroutine to decide whether split should use default strategy/initial
 * split interval, or whether it should finish splitting the page using
 * alternative strategies (this is only possible with leaf pages).
 *
 * Caller uses alternative strategy (or sticks with default strategy) based
 * on how *strategy is set here.  Return value is "perfect penalty", which is
 * passed to _bt_bestsplitloc() as a final constraint on how far caller is
 * willing to go to avoid appending a heap TID when using the many duplicates
 * strategy (it also saves _bt_bestsplitloc() useless cycles).
 */
unsafe fn _bt_strategy(
    state: *mut FindSplitData,
    leftpage: *mut SplitPoint,
    rightpage: *mut SplitPoint,
    strategy: *mut FindSplitStrat,
) -> c_int {
    let mut leftmost: IndexTuple;
    let mut rightmost: IndexTuple;
    let mut leftinterval: *mut SplitPoint = null_mut();
    let mut rightinterval: *mut SplitPoint = null_mut();
    let mut perfectpenalty: c_int;
    let indnkeyatts: c_int = IndexRelationGetNumberOfKeyAttributes((*state).rel);

    /* Assume that alternative strategy won't be used for now */
    *strategy = SPLIT_DEFAULT;

    /*
     * Use smallest observed firstright item size for entire page (actually,
     * entire imaginary version of page that includes newitem) as perfect
     * penalty on internal pages.  This can save cycles in the common case
     * where most or all splits (not just splits within interval) have
     * firstright tuples that are the same size.
     */
    if !(*state).is_leaf {
        return (*state).minfirstrightsz as c_int;
    }

    /*
     * Use leftmost and rightmost tuples from leftmost and rightmost splits in
     * current split interval
     */
    _bt_interval_edges(state, &mut leftinterval, &mut rightinterval);
    leftmost = _bt_split_lastleft(state, leftinterval);
    rightmost = _bt_split_firstright(state, rightinterval);

    /*
     * If initial split interval can produce a split point that will at least
     * avoid appending a heap TID in new high key, we're done.  Finish split
     * with default strategy and initial split interval.
     */
    perfectpenalty = _bt_keep_natts_fast((*state).rel, leftmost, rightmost);
    if perfectpenalty <= indnkeyatts {
        return perfectpenalty;
    }

    /*
     * Work out how caller should finish split when even their "perfect"
     * penalty for initial/default split interval indicates that the interval
     * does not contain even a single split that avoids appending a heap TID.
     *
     * Use the leftmost split's lastleft tuple and the rightmost split's
     * firstright tuple to assess every possible split.
     */
    leftmost = _bt_split_lastleft(state, leftpage);
    rightmost = _bt_split_firstright(state, rightpage);

    /*
     * If page (including new item) has many duplicates but is not entirely
     * full of duplicates, a many duplicates strategy split will be performed.
     * If page is entirely full of duplicates, a single value strategy split
     * will be performed.
     */
    perfectpenalty = _bt_keep_natts_fast((*state).rel, leftmost, rightmost);
    if perfectpenalty <= indnkeyatts {
        *strategy = SPLIT_MANY_DUPLICATES;

        /*
         * Many duplicates strategy should split at either side the group of
         * duplicates that enclose the delta-optimal split point.  Return
         * indnkeyatts rather than the true perfect penalty to make that
         * happen.  (If perfectpenalty was returned here then low cardinality
         * composite indexes could have continual unbalanced splits.)
         *
         * Note that caller won't go through with a many duplicates split in
         * rare cases where it looks like there are ever-decreasing insertions
         * to the immediate right of the split point.  This must happen just
         * before a final decision is made, within _bt_bestsplitloc().
         */
        return indnkeyatts;
    }
    /*
     * Single value strategy is only appropriate with ever-increasing heap
     * TIDs; otherwise, original default strategy split should proceed to
     * avoid pathological performance.  Use page high key to infer if this is
     * the rightmost page among pages that store the same duplicate value.
     * This should not prevent insertions of heap TIDs that are slightly out
     * of order from using single value strategy, since that's expected with
     * concurrent inserters of the same duplicate value.
     */
    else if (*state).is_rightmost {
        *strategy = SPLIT_SINGLE_VALUE;
    } else {
        let itemid: ItemId;
        let hikey: IndexTuple;

        itemid = PageGetItemId((*state).origpage, P_HIKEY);
        hikey = PageGetItem((*state).origpage, itemid) as IndexTuple;
        perfectpenalty = _bt_keep_natts_fast((*state).rel, hikey, (*state).newitem);
        if perfectpenalty <= indnkeyatts {
            *strategy = SPLIT_SINGLE_VALUE;
        } else {
            /*
             * Have caller finish split using default strategy, since page
             * does not appear to be the rightmost page for duplicates of the
             * value the page is filled with
             */
        }
    }

    return perfectpenalty;
}

/*
 * Subroutine to locate leftmost and rightmost splits for current/default
 * split interval.  Note that it will be the same split iff there is only one
 * split in interval.
 */
unsafe fn _bt_interval_edges(
    state: *mut FindSplitData,
    leftinterval: *mut *mut SplitPoint,
    rightinterval: *mut *mut SplitPoint,
) {
    let highsplit: c_int = Min((*state).interval, (*state).nsplits);
    let deltaoptimal: *mut SplitPoint;

    deltaoptimal = (*state).splits;
    *leftinterval = null_mut();
    *rightinterval = null_mut();

    /*
     * Delta is an absolute distance to optimal split point, so both the
     * leftmost and rightmost split point will usually be at the end of the
     * array
     */
    let mut i = highsplit - 1;
    while i >= 0 {
        let distant: *mut SplitPoint = (*state).splits.add(i as usize);

        if (*distant).firstrightoff < (*deltaoptimal).firstrightoff {
            if (*leftinterval).is_null() {
                *leftinterval = distant;
            }
        } else if (*distant).firstrightoff > (*deltaoptimal).firstrightoff {
            if (*rightinterval).is_null() {
                *rightinterval = distant;
            }
        } else if !(*distant).newitemonleft && (*deltaoptimal).newitemonleft {
            /*
             * "incoming tuple will become firstright" (distant) is to the
             * left of "incoming tuple will become lastleft" (delta-optimal)
             */
            Assert!((*distant).firstrightoff == (*state).newitemoff);
            if (*leftinterval).is_null() {
                *leftinterval = distant;
            }
        } else if (*distant).newitemonleft && !(*deltaoptimal).newitemonleft {
            /*
             * "incoming tuple will become lastleft" (distant) is to the right
             * of "incoming tuple will become firstright" (delta-optimal)
             */
            Assert!((*distant).firstrightoff == (*state).newitemoff);
            if (*rightinterval).is_null() {
                *rightinterval = distant;
            }
        } else {
            /* There was only one or two splits in initial split interval */
            Assert!(distant == deltaoptimal);
            if (*leftinterval).is_null() {
                *leftinterval = distant;
            }
            if (*rightinterval).is_null() {
                *rightinterval = distant;
            }
        }

        if !(*leftinterval).is_null() && !(*rightinterval).is_null() {
            return;
        }
        i -= 1;
    }

    Assert!(false);
}

/*
 * Subroutine to find penalty for caller's candidate split point.
 *
 * On leaf pages, penalty is the attribute number that distinguishes each side
 * of a split.  It's the last attribute that needs to be included in new high
 * key for left page.  It can be greater than the number of key attributes in
 * cases where a heap TID will need to be appended during truncation.
 *
 * On internal pages, penalty is simply the size of the firstright tuple for
 * the split (including line pointer overhead).  This tuple will become the
 * new high key for the left page.
 */
#[inline]
unsafe fn _bt_split_penalty(state: *mut FindSplitData, split: *mut SplitPoint) -> c_int {
    let lastleft: IndexTuple;
    let firstright: IndexTuple;

    if !(*state).is_leaf {
        let itemid: ItemId;

        if !(*split).newitemonleft && (*split).firstrightoff == (*state).newitemoff {
            return (*state).newitemsz as c_int;
        }

        itemid = PageGetItemId((*state).origpage, (*split).firstrightoff);

        return (MAXALIGN(ItemIdGetLength(itemid) as usize) + size_of::<ItemIdData>()) as c_int;
    }

    lastleft = _bt_split_lastleft(state, split);
    firstright = _bt_split_firstright(state, split);

    return _bt_keep_natts_fast((*state).rel, lastleft, firstright);
}

/*
 * Subroutine to get a lastleft IndexTuple for a split point
 */
#[inline]
unsafe fn _bt_split_lastleft(state: *mut FindSplitData, split: *mut SplitPoint) -> IndexTuple {
    let itemid: ItemId;

    if (*split).newitemonleft && (*split).firstrightoff == (*state).newitemoff {
        return (*state).newitem;
    }

    itemid = PageGetItemId((*state).origpage, OffsetNumberPrev((*split).firstrightoff));
    return PageGetItem((*state).origpage, itemid) as IndexTuple;
}

/*
 * Subroutine to get a firstright IndexTuple for a split point
 */
#[inline]
unsafe fn _bt_split_firstright(state: *mut FindSplitData, split: *mut SplitPoint) -> IndexTuple {
    let itemid: ItemId;

    if !(*split).newitemonleft && (*split).firstrightoff == (*state).newitemoff {
        return (*state).newitem;
    }

    itemid = PageGetItemId((*state).origpage, (*split).firstrightoff);
    return PageGetItem((*state).origpage, itemid) as IndexTuple;
}
