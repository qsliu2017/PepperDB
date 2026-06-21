//! gistutil.rs
//!   utilities routines for the postgres GiST index access method.
//! Translated 1:1 from postgres/src/backend/access/gist/gistutil.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!			src/backend/access/gist/gistutil.c

use crate::prelude::*;
use crate::{PG_GETARG_INT32, PG_RETURN_UINT16};

use crate::access::cmptype::{
    CompareType, COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_LE, COMPARE_LT,
    COMPARE_OVERLAP,
};
use crate::access::common::indextuple::{
    index_form_tuple, index_getattr, IndexTuple, IndexTupleData, INDEX_MAX_KEYS,
};
use crate::access::common::heaptuple::heap_form_tuple;
use crate::access::gist::gist_private::{
    gistPageRecyclable, gistXLogAssignLSN, gistXLogPageReuse, GiSTOptions, GiSTPageSize, GISTENTRY,
    GISTSTATE,
};
use crate::access::htup_details::HeapTuple;
use crate::access::index::amapi::IndexAMProperty;
use crate::access::index::amapi::IndexAMProperty::{AMPROP_DISTANCE_ORDERABLE, AMPROP_RETURNABLE};
use crate::access::stratnum::{
    StrategyNumber, InvalidStrategy, RTContainedByStrategyNumber, RTEqualStrategyNumber,
    RTGreaterEqualStrategyNumber, RTGreaterStrategyNumber, RTLessEqualStrategyNumber,
    RTLessStrategyNumber, RTOverlapStrategyNumber,
};
use crate::access::transam::FullTransactionId;
use crate::access::transam::xlogdefs::{
    FirstNormalUnloggedLSN, InvalidXLogRecPtr, XLogRecPtr, XLogRecPtrIsInvalid,
};
use crate::common::pg_prng::{pg_global_prng_state, pg_prng_bool};
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
};
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::ItemPointerSetOffsetNumber;
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberNext,
};
use crate::utils::adt::float::get_float4_infinity;
use crate::utils::fmgr::{
    FunctionCall1Coll, FunctionCall2Coll, FunctionCall3Coll, FunctionCallInfo, OidFunctionCall1Coll,
};
use crate::utils::rel::{Relation, RelationGetRelationName};

// ===========================================================================
// access/gist.h stubs (NOT yet ported in its own module).
// TODO: dedup once access/gist.h is ported (same stubs live in the sibling
// gist*.rs files).
// ===========================================================================

/// access/gist.h: vector of GISTENTRY with a leading count, as passed to the
/// union/picksplit opclass support functions.
#[repr(C)]
pub struct GistEntryVector {
    pub n: int32,
    pub vector: [GISTENTRY; FLEXIBLE_ARRAY_MEMBER],
}

/* #define GEVHDRSZ ((Size) offsetof(GistEntryVector, vector)) */
pub const GEVHDRSZ: Size = core::mem::offset_of!(GistEntryVector, vector) as Size;

/* #define gistentryinit(e, k, r, pg, o, l) ... -- initialize a GISTENTRY */
#[inline]
unsafe fn gistentryinit(
    e: *mut GISTENTRY,
    k: Datum,
    r: Relation,
    pg: Page,
    o: OffsetNumber,
    l: bool,
) {
    (*e).key = k;
    (*e).rel = r;
    (*e).page = pg;
    (*e).offset = o;
    (*e).leafkey = l;
}

/// access/gist.h: special-area struct at the end of every GiST index page.
/// TODO: dedup once access/gist.h is ported.
#[repr(C)]
pub struct GISTPageOpaqueData {
    pub nsn: GistNSN,                /* this page's update LSN */
    pub rightlink: BlockNumber,      /* next page if any */
    pub flags: uint16,               /* see bit definitions above */
    pub gist_page_id: uint16,        /* for identification of GiST indexes */
}

pub type GISTPageOpaque = *mut GISTPageOpaqueData;

/// despite the name, GistNSN is just an LSN (access/gist.h).
pub type GistNSN = XLogRecPtr;

/* page identifier stored in gist_page_id (access/gist.h) */
pub const GIST_PAGE_ID: uint16 = 0xFF81;

/* GiST page flag bits (access/gist.h) */
pub const F_LEAF: uint16 = 1 << 0;
pub const F_DELETED: uint16 = 1 << 1;

/* #define GistPageGetOpaque(page) ((GISTPageOpaque) PageGetSpecialPointer(page)) */
#[inline]
unsafe fn GistPageGetOpaque(page: Page) -> GISTPageOpaque {
    PageGetSpecialPointer(page) as GISTPageOpaque
}

/* #define GistPageIsLeaf(page) (GistPageGetOpaque(page)->flags & F_LEAF) */
#[inline]
unsafe fn GistPageIsLeaf(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_LEAF) != 0
}

/* #define GistPageIsDeleted(page) (GistPageGetOpaque(page)->flags & F_DELETED) */
#[inline]
unsafe fn GistPageIsDeleted(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_DELETED) != 0
}

/*
 * On a deleted page, we store this struct.  A deleted page doesn't contain any
 * tuples, so we don't use the normal page layout with line pointers.  Instead,
 * this struct is stored right after the standard page header.
 */
#[repr(C)]
pub struct GISTDeletedPageContents {
    pub deleteXid: FullTransactionId,
}

/* #define GistPageGetDeleteXid(page) ... */
#[inline]
unsafe fn GistPageGetDeleteXid(page: Page) -> FullTransactionId {
    (*(PageGetContents(page) as *const GISTDeletedPageContents)).deleteXid
}

/* support function numbers (access/gist.h); also defined in gistvalidate.rs */
pub const GIST_DISTANCE_PROC: int16 = 8;
pub const GIST_FETCH_PROC: int16 = 9;
pub const GIST_COMPRESS_PROC: int16 = 3;
pub const GIST_TRANSLATE_CMPTYPE_PROC: int16 = 12;

// ===========================================================================
// Local IndexTupleSize / page helpers (itup.h, storage/bufpage.h not fully
// exporting the macro forms used here). TODO: dedup once exported.
// ===========================================================================

/* #define IndexTupleSize(itup) ((Size)((itup)->t_info & INDEX_SIZE_MASK)) */
#[inline]
unsafe fn IndexTupleSize(itup: IndexTuple) -> Size {
    const INDEX_SIZE_MASK: u16 = 0x1FFF;
    ((*itup).t_info & INDEX_SIZE_MASK) as Size
}

/*
 * Write itup vector to page, has no control of free space.
 */
pub unsafe fn gistfillbuffer(page: Page, itup: *mut IndexTuple, len: c_int, mut off: OffsetNumber) {
    let mut i: c_int;

    if off == InvalidOffsetNumber {
        off = if PageIsEmpty(page) {
            FirstOffsetNumber
        } else {
            OffsetNumberNext(PageGetMaxOffsetNumber(page))
        };
    }

    i = 0;
    while i < len {
        let sz: Size = IndexTupleSize(*itup.add(i as usize));
        let l: OffsetNumber;

        l = PageAddItem(
            page,
            *itup.add(i as usize) as Item,
            sz,
            off,
            false,
            false,
        );
        if l == InvalidOffsetNumber {
            elog!(
                ERROR,
                "failed to add item to GiST index page, item {} out of {}, size {} bytes",
                i,
                len,
                sz as c_int
            );
        }
        off += 1;
        i += 1;
    }
}

/*
 * Check space for itup vector on page
 */
pub unsafe fn gistnospace(
    page: Page,
    itvec: *mut IndexTuple,
    len: c_int,
    todelete: OffsetNumber,
    freespace: Size,
) -> bool {
    let mut size: c_uint = freespace as c_uint;
    let mut deleted: c_uint = 0;
    let mut i: c_int;

    i = 0;
    while i < len {
        size += (IndexTupleSize(*itvec.add(i as usize))
            + core::mem::size_of::<ItemIdData>()) as c_uint;
        i += 1;
    }

    if todelete != InvalidOffsetNumber {
        let itup: IndexTuple =
            PageGetItem(page, PageGetItemId(page, todelete)) as IndexTuple;

        deleted = (IndexTupleSize(itup) + core::mem::size_of::<ItemIdData>()) as c_uint;
    }

    PageGetFreeSpace(page) as c_uint + deleted < size
}

pub unsafe fn gistfitpage(itvec: *mut IndexTuple, len: c_int) -> bool {
    let mut i: c_int;
    let mut size: Size = 0;

    i = 0;
    while i < len {
        size += IndexTupleSize(*itvec.add(i as usize)) + core::mem::size_of::<ItemIdData>();
        i += 1;
    }

    /* TODO: Consider fillfactor */
    size <= GiSTPageSize()
}

/*
 * Read buffer into itup vector
 */
pub unsafe fn gistextractpage(page: Page, len: *mut c_int /* out */) -> *mut IndexTuple {
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let itvec: *mut IndexTuple;

    maxoff = PageGetMaxOffsetNumber(page);
    *len = maxoff as c_int;
    itvec = palloc(core::mem::size_of::<IndexTuple>() * maxoff as usize) as *mut IndexTuple;
    i = FirstOffsetNumber;
    while i <= maxoff {
        *itvec.add((i - FirstOffsetNumber) as usize) =
            PageGetItem(page, PageGetItemId(page, i)) as IndexTuple;
        i = OffsetNumberNext(i);
    }

    itvec
}

/*
 * join two vectors into one
 */
pub unsafe fn gistjoinvector(
    mut itvec: *mut IndexTuple,
    len: *mut c_int,
    additvec: *mut IndexTuple,
    addlen: c_int,
) -> *mut IndexTuple {
    itvec = repalloc(
        itvec as *mut c_void,
        core::mem::size_of::<IndexTuple>() * ((*len) + addlen) as usize,
    ) as *mut IndexTuple;
    memmove(
        itvec.add(*len as usize) as *mut c_void,
        additvec as *const c_void,
        core::mem::size_of::<IndexTuple>() * addlen as usize,
    );
    *len += addlen;
    itvec
}

/*
 * make plain IndexTuple vector
 */

pub unsafe fn gistfillitupvec(
    vec: *mut IndexTuple,
    veclen: c_int,
    memlen: *mut c_int,
) -> *mut IndexTupleData {
    let mut ptr: *mut c_char;
    let ret: *mut c_char;
    let mut i: c_int;

    *memlen = 0;

    i = 0;
    while i < veclen {
        *memlen += IndexTupleSize(*vec.add(i as usize)) as c_int;
        i += 1;
    }

    ret = palloc(*memlen as usize) as *mut c_char;
    ptr = ret;

    i = 0;
    while i < veclen {
        memcpy(
            ptr as *mut c_void,
            *vec.add(i as usize) as *const c_void,
            IndexTupleSize(*vec.add(i as usize)),
        );
        ptr = ptr.add(IndexTupleSize(*vec.add(i as usize)));
        i += 1;
    }

    ret as *mut IndexTupleData
}

/*
 * Make unions of keys in IndexTuple vector (one union datum per index column).
 * Union Datums are returned into the attr/isnull arrays.
 * Resulting Datums aren't compressed.
 */
pub unsafe fn gistMakeUnionItVec(
    giststate: *mut GISTSTATE,
    itvec: *mut IndexTuple,
    len: c_int,
    attr: *mut Datum,
    isnull: *mut bool,
) {
    let mut i: c_int;
    let evec: *mut GistEntryVector;
    let mut attrsize: c_int = 0; /* silence compiler warning */

    evec = palloc(
        (len + 2) as usize * core::mem::size_of::<GISTENTRY>() + GEVHDRSZ,
    ) as *mut GistEntryVector;

    i = 0;
    while i < (*(*giststate).nonLeafTupdesc).natts {
        let mut j: c_int;

        /* Collect non-null datums for this column */
        (*evec).n = 0;
        j = 0;
        while j < len {
            let datum: Datum;
            let mut IsNull: bool = false;

            datum = index_getattr(
                *itvec.add(j as usize),
                i + 1,
                (*giststate).leafTupdesc,
                &raw mut IsNull,
            );
            if IsNull {
                j += 1;
                continue;
            }

            gistdentryinit(
                giststate,
                i,
                (*evec).vector.as_mut_ptr().add((*evec).n as usize),
                datum,
                null_mut(),
                null_mut(),
                0 as OffsetNumber,
                false,
                IsNull,
            );
            (*evec).n += 1;
            j += 1;
        }

        /* If this column was all NULLs, the union is NULL */
        if (*evec).n == 0 {
            *attr.add(i as usize) = 0 as Datum;
            *isnull.add(i as usize) = true;
        } else {
            if (*evec).n == 1 {
                /* unionFn may expect at least two inputs */
                (*evec).n = 2;
                *(*evec).vector.as_mut_ptr().add(1) = *(*evec).vector.as_ptr().add(0);
            }

            /* Make union and store in attr array */
            *attr.add(i as usize) = FunctionCall2Coll(
                &raw mut (*giststate).unionFn[i as usize],
                (*giststate).supportCollation[i as usize],
                PointerGetDatum(evec as *const c_void),
                PointerGetDatum(&raw mut attrsize as *const c_void),
            );

            *isnull.add(i as usize) = false;
        }
        i += 1;
    }
}

/*
 * Return an IndexTuple containing the result of applying the "union"
 * method to the specified IndexTuple vector.
 */
pub unsafe fn gistunion(
    r: Relation,
    itvec: *mut IndexTuple,
    len: c_int,
    giststate: *mut GISTSTATE,
) -> IndexTuple {
    let mut attr: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];

    gistMakeUnionItVec(
        giststate,
        itvec,
        len,
        attr.as_mut_ptr(),
        isnull.as_mut_ptr(),
    );

    gistFormTuple(giststate, r, attr.as_ptr(), isnull.as_ptr(), false)
}

/*
 * makes union of two key
 */
pub unsafe fn gistMakeUnionKey(
    giststate: *mut GISTSTATE,
    attno: c_int,
    entry1: *mut GISTENTRY,
    isnull1: bool,
    entry2: *mut GISTENTRY,
    isnull2: bool,
    dst: *mut Datum,
    dstisnull: *mut bool,
) {
    /* we need a GistEntryVector with room for exactly 2 elements */
    #[repr(C, align(8))]
    struct Storage {
        padding: [c_char; 2 * core::mem::size_of::<GISTENTRY>() + GEVHDRSZ],
    }
    let mut storage: Storage = Storage {
        padding: [0; 2 * core::mem::size_of::<GISTENTRY>() + GEVHDRSZ],
    };
    let evec: *mut GistEntryVector = storage.padding.as_mut_ptr() as *mut GistEntryVector;
    let mut dstsize: c_int = 0; /* silence compiler warning */

    (*evec).n = 2;

    if isnull1 && isnull2 {
        *dstisnull = true;
        *dst = 0 as Datum;
    } else {
        if isnull1 == false && isnull2 == false {
            *(*evec).vector.as_mut_ptr().add(0) = *entry1;
            *(*evec).vector.as_mut_ptr().add(1) = *entry2;
        } else if isnull1 == false {
            *(*evec).vector.as_mut_ptr().add(0) = *entry1;
            *(*evec).vector.as_mut_ptr().add(1) = *entry1;
        } else {
            *(*evec).vector.as_mut_ptr().add(0) = *entry2;
            *(*evec).vector.as_mut_ptr().add(1) = *entry2;
        }

        *dstisnull = false;
        *dst = FunctionCall2Coll(
            &raw mut (*giststate).unionFn[attno as usize],
            (*giststate).supportCollation[attno as usize],
            PointerGetDatum(evec as *const c_void),
            PointerGetDatum(&raw mut dstsize as *const c_void),
        );
    }
}

pub unsafe fn gistKeyIsEQ(giststate: *mut GISTSTATE, attno: c_int, a: Datum, b: Datum) -> bool {
    let mut result: bool = false; /* silence compiler warning */

    FunctionCall3Coll(
        &raw mut (*giststate).equalFn[attno as usize],
        (*giststate).supportCollation[attno as usize],
        a,
        b,
        PointerGetDatum(&raw mut result as *const c_void),
    );
    result
}

/*
 * Decompress all keys in tuple
 */
pub unsafe fn gistDeCompressAtt(
    giststate: *mut GISTSTATE,
    r: Relation,
    tuple: IndexTuple,
    p: Page,
    o: OffsetNumber,
    attdata: *mut GISTENTRY,
    isnull: *mut bool,
) {
    let mut i: c_int;

    i = 0;
    while i < IndexRelationGetNumberOfKeyAttributes(r) {
        let datum: Datum;

        datum = index_getattr(
            tuple,
            i + 1,
            (*giststate).leafTupdesc,
            isnull.add(i as usize),
        );
        gistdentryinit(
            giststate,
            i,
            attdata.add(i as usize),
            datum,
            r,
            p,
            o,
            false,
            *isnull.add(i as usize),
        );
        i += 1;
    }
}

/*
 * Forms union of oldtup and addtup, if union == oldtup then return NULL
 */
pub unsafe fn gistgetadjusted(
    r: Relation,
    oldtup: IndexTuple,
    addtup: IndexTuple,
    giststate: *mut GISTSTATE,
) -> IndexTuple {
    let mut neednew: bool = false;
    let mut oldentries: [GISTENTRY; INDEX_MAX_KEYS] = core::mem::zeroed();
    let mut addentries: [GISTENTRY; INDEX_MAX_KEYS] = core::mem::zeroed();
    let mut oldisnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut addisnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut attr: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut newtup: IndexTuple = null_mut();
    let mut i: c_int;

    gistDeCompressAtt(
        giststate,
        r,
        oldtup,
        null_mut(),
        0 as OffsetNumber,
        oldentries.as_mut_ptr(),
        oldisnull.as_mut_ptr(),
    );

    gistDeCompressAtt(
        giststate,
        r,
        addtup,
        null_mut(),
        0 as OffsetNumber,
        addentries.as_mut_ptr(),
        addisnull.as_mut_ptr(),
    );

    i = 0;
    while i < IndexRelationGetNumberOfKeyAttributes(r) {
        gistMakeUnionKey(
            giststate,
            i,
            oldentries.as_mut_ptr().add(i as usize),
            oldisnull[i as usize],
            addentries.as_mut_ptr().add(i as usize),
            addisnull[i as usize],
            attr.as_mut_ptr().add(i as usize),
            isnull.as_mut_ptr().add(i as usize),
        );

        if neednew {
            /* we already need new key, so we can skip check */
            i += 1;
            continue;
        }

        if isnull[i as usize] {
            /* union of key may be NULL if and only if both keys are NULL */
            i += 1;
            continue;
        }

        if !addisnull[i as usize] {
            if oldisnull[i as usize]
                || !gistKeyIsEQ(giststate, i, oldentries[i as usize].key, attr[i as usize])
            {
                neednew = true;
            }
        }
        i += 1;
    }

    if neednew {
        /* need to update key */
        newtup = gistFormTuple(giststate, r, attr.as_ptr(), isnull.as_ptr(), false);
        (*newtup).t_tid = (*oldtup).t_tid;
    }

    newtup
}

/*
 * Search an upper index page for the entry with lowest penalty for insertion
 * of the new index key contained in "it".
 *
 * Returns the index of the page entry to insert into.
 */
pub unsafe fn gistchoose(
    r: Relation,
    p: Page,
    it: IndexTuple, /* it has compressed entry */
    giststate: *mut GISTSTATE,
) -> OffsetNumber {
    let mut result: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut i: OffsetNumber;
    let mut best_penalty: [f32; INDEX_MAX_KEYS] = [0.0; INDEX_MAX_KEYS];
    let mut entry: GISTENTRY = core::mem::zeroed();
    let mut identry: [GISTENTRY; INDEX_MAX_KEYS] = core::mem::zeroed();
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut keep_current_best: c_int;

    Assert!(!GistPageIsLeaf(p));

    gistDeCompressAtt(
        giststate,
        r,
        it,
        null_mut(),
        0 as OffsetNumber,
        identry.as_mut_ptr(),
        isnull.as_mut_ptr(),
    );

    /* we'll return FirstOffsetNumber if page is empty (shouldn't happen) */
    result = FirstOffsetNumber;

    /*
     * The index may have multiple columns, and there's a penalty value for
     * each column.  The penalty associated with a column that appears earlier
     * in the index definition is strictly more important than the penalty of
     * a column that appears later in the index definition.
     *
     * best_penalty[j] is the best penalty we have seen so far for column j,
     * or -1 when we haven't yet examined column j.  Array entries to the
     * right of the first -1 are undefined.
     */
    best_penalty[0] = -1.0;

    /*
     * If we find a tuple that's exactly as good as the currently best one, we
     * could use either one.  When inserting a lot of tuples with the same or
     * similar keys, it's preferable to descend down the same path when
     * possible, as that's more cache-friendly.  On the other hand, if all
     * inserts land on the same leaf page after a split, we're never going to
     * insert anything to the other half of the split, and will end up using
     * only 50% of the available space.  Distributing the inserts evenly would
     * lead to better space usage, but that hurts cache-locality during
     * insertion.  To get the best of both worlds, when we find a tuple that's
     * exactly as good as the previous best, choose randomly whether to stick
     * to the old best, or use the new one.  Once we decide to stick to the
     * old best, we keep sticking to it for any subsequent equally good tuples
     * we might find.  This favors tuples with low offsets, but still allows
     * some inserts to go to other equally-good subtrees.
     *
     * keep_current_best is -1 if we haven't yet had to make a random choice
     * whether to keep the current best tuple.  If we have done so, and
     * decided to keep it, keep_current_best is 1; if we've decided to
     * replace, keep_current_best is 0.  (This state will be reset to -1 as
     * soon as we've made the replacement, but sometimes we make the choice in
     * advance of actually finding a replacement best tuple.)
     */
    keep_current_best = -1;

    /*
     * Loop over tuples on page.
     */
    maxoff = PageGetMaxOffsetNumber(p);
    Assert!(maxoff >= FirstOffsetNumber);

    i = FirstOffsetNumber;
    while i <= maxoff {
        let itup: IndexTuple = PageGetItem(p, PageGetItemId(p, i)) as IndexTuple;
        let mut zero_penalty: bool;
        let mut j: c_int;

        zero_penalty = true;

        /* Loop over index attributes. */
        j = 0;
        while j < IndexRelationGetNumberOfKeyAttributes(r) {
            let datum: Datum;
            let usize_: f32;
            let mut IsNull: bool = false;

            /* Compute penalty for this column. */
            datum = index_getattr(itup, j + 1, (*giststate).leafTupdesc, &raw mut IsNull);
            gistdentryinit(giststate, j, &raw mut entry, datum, r, p, i, false, IsNull);
            usize_ = gistpenalty(
                giststate,
                j,
                &raw mut entry,
                IsNull,
                &raw mut identry[j as usize],
                isnull[j as usize],
            );
            if usize_ > 0.0 {
                zero_penalty = false;
            }

            if best_penalty[j as usize] < 0.0 || usize_ < best_penalty[j as usize] {
                /*
                 * New best penalty for column.  Tentatively select this tuple
                 * as the target, and record the best penalty.  Then reset the
                 * next column's penalty to "unknown" (and indirectly, the
                 * same for all the ones to its right).  This will force us to
                 * adopt this tuple's penalty values as the best for all the
                 * remaining columns during subsequent loop iterations.
                 */
                result = i;
                best_penalty[j as usize] = usize_;

                if j < IndexRelationGetNumberOfKeyAttributes(r) - 1 {
                    best_penalty[(j + 1) as usize] = -1.0;
                }

                /* we have new best, so reset keep-it decision */
                keep_current_best = -1;
            } else if best_penalty[j as usize] == usize_ {
                /*
                 * The current tuple is exactly as good for this column as the
                 * best tuple seen so far.  The next iteration of this loop
                 * will compare the next column.
                 */
            } else {
                /*
                 * The current tuple is worse for this column than the best
                 * tuple seen so far.  Skip the remaining columns and move on
                 * to the next tuple, if any.
                 */
                zero_penalty = false; /* so outer loop won't exit */
                break;
            }
            j += 1;
        }

        /*
         * If we looped past the last column, and did not update "result",
         * then this tuple is exactly as good as the prior best tuple.
         */
        if j == IndexRelationGetNumberOfKeyAttributes(r) && result != i {
            if keep_current_best == -1 {
                /* we didn't make the random choice yet for this old best */
                keep_current_best = if pg_prng_bool(&raw mut pg_global_prng_state) {
                    1
                } else {
                    0
                };
            }
            if keep_current_best == 0 {
                /* we choose to use the new tuple */
                result = i;
                /* choose again if there are even more exactly-as-good ones */
                keep_current_best = -1;
            }
        }

        /*
         * If we find a tuple with zero penalty for all columns, and we've
         * decided we don't want to search for another tuple with equal
         * penalty, there's no need to examine remaining tuples; just break
         * out of the loop and return it.
         */
        if zero_penalty {
            if keep_current_best == -1 {
                /* we didn't make the random choice yet for this old best */
                keep_current_best = if pg_prng_bool(&raw mut pg_global_prng_state) {
                    1
                } else {
                    0
                };
            }
            if keep_current_best == 1 {
                break;
            }
        }
        i = OffsetNumberNext(i);
    }

    result
}

/*
 * initialize a GiST entry with a decompressed version of key
 */
pub unsafe fn gistdentryinit(
    giststate: *mut GISTSTATE,
    nkey: c_int,
    e: *mut GISTENTRY,
    k: Datum,
    r: Relation,
    pg: Page,
    o: OffsetNumber,
    l: bool,
    isNull: bool,
) {
    if !isNull {
        let dep: *mut GISTENTRY;

        gistentryinit(e, k, r, pg, o, l);

        /* there may not be a decompress function in opclass */
        if !OidIsValid((*giststate).decompressFn[nkey as usize].fn_oid) {
            return;
        }

        dep = DatumGetPointer(FunctionCall1Coll(
            &raw mut (*giststate).decompressFn[nkey as usize],
            (*giststate).supportCollation[nkey as usize],
            PointerGetDatum(e as *const c_void),
        )) as *mut GISTENTRY;
        /* decompressFn may just return the given pointer */
        if dep != e {
            gistentryinit(
                e,
                (*dep).key,
                (*dep).rel,
                (*dep).page,
                (*dep).offset,
                (*dep).leafkey,
            );
        }
    } else {
        gistentryinit(e, 0 as Datum, r, pg, o, l);
    }
}

pub unsafe fn gistFormTuple(
    giststate: *mut GISTSTATE,
    r: Relation,
    attdata: *const Datum,
    isnull: *const bool,
    isleaf: bool,
) -> IndexTuple {
    let mut compatt: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
    let res: IndexTuple;

    gistCompressValues(giststate, r, attdata, isnull, isleaf, compatt.as_mut_ptr());

    res = index_form_tuple(
        if isleaf {
            (*giststate).leafTupdesc
        } else {
            (*giststate).nonLeafTupdesc
        },
        compatt.as_ptr(),
        isnull,
    );

    /*
     * The offset number on tuples on internal pages is unused. For historical
     * reasons, it is set to 0xffff.
     */
    ItemPointerSetOffsetNumber(&raw mut (*res).t_tid, 0xffff);
    res
}

pub unsafe fn gistCompressValues(
    giststate: *mut GISTSTATE,
    r: Relation,
    attdata: *const Datum,
    isnull: *const bool,
    isleaf: bool,
    compatt: *mut Datum,
) {
    let mut i: c_int;

    /*
     * Call the compress method on each attribute.
     */
    i = 0;
    while i < IndexRelationGetNumberOfKeyAttributes(r) {
        if *isnull.add(i as usize) {
            *compatt.add(i as usize) = 0 as Datum;
        } else {
            let mut centry: GISTENTRY = core::mem::zeroed();
            let cep: *mut GISTENTRY;

            gistentryinit(
                &raw mut centry,
                *attdata.add(i as usize),
                r,
                null_mut(),
                0 as OffsetNumber,
                isleaf,
            );
            /* there may not be a compress function in opclass */
            if OidIsValid((*giststate).compressFn[i as usize].fn_oid) {
                cep = DatumGetPointer(FunctionCall1Coll(
                    &raw mut (*giststate).compressFn[i as usize],
                    (*giststate).supportCollation[i as usize],
                    PointerGetDatum(&raw mut centry as *const c_void),
                )) as *mut GISTENTRY;
            } else {
                cep = &raw mut centry;
            }
            *compatt.add(i as usize) = (*cep).key;
        }
        i += 1;
    }

    if isleaf {
        /*
         * Emplace each included attribute if any.
         */
        while i < (*(*r).rd_att).natts {
            if *isnull.add(i as usize) {
                *compatt.add(i as usize) = 0 as Datum;
            } else {
                *compatt.add(i as usize) = *attdata.add(i as usize);
            }
            i += 1;
        }
    }
}

/*
 * initialize a GiST entry with fetched value in key field
 */
unsafe fn gistFetchAtt(giststate: *mut GISTSTATE, nkey: c_int, k: Datum, r: Relation) -> Datum {
    let mut fentry: GISTENTRY = core::mem::zeroed();
    let fep: *mut GISTENTRY;

    gistentryinit(&raw mut fentry, k, r, null_mut(), 0 as OffsetNumber, false);

    fep = DatumGetPointer(FunctionCall1Coll(
        &raw mut (*giststate).fetchFn[nkey as usize],
        (*giststate).supportCollation[nkey as usize],
        PointerGetDatum(&raw mut fentry as *const c_void),
    )) as *mut GISTENTRY;

    /* fetchFn set 'key', return it to the caller */
    (*fep).key
}

/*
 * Fetch all keys in tuple.
 * Returns a new HeapTuple containing the originally-indexed data.
 */
pub unsafe fn gistFetchTuple(
    giststate: *mut GISTSTATE,
    r: Relation,
    tuple: IndexTuple,
) -> HeapTuple {
    let oldcxt: MemoryContext = MemoryContextSwitchTo((*giststate).tempCxt as crate::utils::palloc::MemoryContext);
    let mut fetchatt: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut i: c_int;

    i = 0;
    while i < IndexRelationGetNumberOfKeyAttributes(r) {
        let datum: Datum;

        datum = index_getattr(
            tuple,
            i + 1,
            (*giststate).leafTupdesc,
            &raw mut isnull[i as usize],
        );

        if (*giststate).fetchFn[i as usize].fn_oid != InvalidOid {
            if !isnull[i as usize] {
                fetchatt[i as usize] = gistFetchAtt(giststate, i, datum, r);
            } else {
                fetchatt[i as usize] = 0 as Datum;
            }
        } else if (*giststate).compressFn[i as usize].fn_oid == InvalidOid {
            /*
             * If opclass does not provide compress method that could change
             * original value, att is necessarily stored in original form.
             */
            if !isnull[i as usize] {
                fetchatt[i as usize] = datum;
            } else {
                fetchatt[i as usize] = 0 as Datum;
            }
        } else {
            /*
             * Index-only scans not supported for this column. Since the
             * planner chose an index-only scan anyway, it is not interested
             * in this column, and we can replace it with a NULL.
             */
            isnull[i as usize] = true;
            fetchatt[i as usize] = 0 as Datum;
        }
        i += 1;
    }

    /*
     * Get each included attribute.
     */
    while i < (*(*r).rd_att).natts {
        fetchatt[i as usize] = index_getattr(
            tuple,
            i + 1,
            (*giststate).leafTupdesc,
            &raw mut isnull[i as usize],
        );
        i += 1;
    }
    MemoryContextSwitchTo(oldcxt);

    heap_form_tuple(
        (*giststate).fetchTupdesc,
        fetchatt.as_mut_ptr(),
        isnull.as_mut_ptr(),
    )
}

pub unsafe fn gistpenalty(
    giststate: *mut GISTSTATE,
    attno: c_int,
    orig: *mut GISTENTRY,
    isNullOrig: bool,
    add: *mut GISTENTRY,
    isNullAdd: bool,
) -> f32 {
    let mut penalty: f32 = 0.0;

    if (*giststate).penaltyFn[attno as usize].fn_strict == false
        || (isNullOrig == false && isNullAdd == false)
    {
        FunctionCall3Coll(
            &raw mut (*giststate).penaltyFn[attno as usize],
            (*giststate).supportCollation[attno as usize],
            PointerGetDatum(orig as *const c_void),
            PointerGetDatum(add as *const c_void),
            PointerGetDatum(&raw mut penalty as *const c_void),
        );
        /* disallow negative or NaN penalty */
        if penalty.is_nan() || penalty < 0.0 {
            penalty = 0.0;
        }
    } else if isNullOrig && isNullAdd {
        penalty = 0.0;
    } else {
        /* try to prevent mixing null and non-null values */
        penalty = get_float4_infinity();
    }

    penalty
}

/*
 * Initialize a new index page
 */
pub unsafe fn gistinitpage(page: Page, f: uint32) {
    let opaque: GISTPageOpaque;

    PageInit(page, BLCKSZ as Size, core::mem::size_of::<GISTPageOpaqueData>());

    opaque = GistPageGetOpaque(page);
    (*opaque).rightlink = InvalidBlockNumber;
    (*opaque).flags = f as uint16;
    (*opaque).gist_page_id = GIST_PAGE_ID;
}

/*
 * Initialize a new index buffer
 */
pub unsafe fn GISTInitBuffer(b: Buffer, f: uint32) {
    let page: Page;

    page = BufferGetPage(b);
    gistinitpage(page, f);
}

/*
 * Verify that a freshly-read page looks sane.
 */
pub unsafe fn gistcheckpage(rel: Relation, buf: Buffer) {
    let page: Page = BufferGetPage(buf);

    /*
     * ReadBuffer verifies that every newly-read page passes
     * PageHeaderIsValid, which means it either contains a reasonably sane
     * page header or is all-zero.  We have to defend against the all-zero
     * case, however.
     */
    if PageIsNew(page) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" contains unexpected zero page at block {}",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                BufferGetBlockNumber(buf)
            )
        );
    }

    /*
     * Additionally check that the special area looks sane.
     */
    if PageGetSpecialSize(page) as Size != MAXALIGN(core::mem::size_of::<GISTPageOpaqueData>()) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" contains corrupted page at block {}",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                BufferGetBlockNumber(buf)
            )
        );
    }
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 *
 * The returned buffer is already pinned and exclusive-locked
 *
 * Caller is responsible for initializing the page by calling GISTInitBuffer
 */
pub unsafe fn gistNewBuffer(r: Relation, heaprel: Relation) -> Buffer {
    let buffer: Buffer;

    /* First, try to get a page from FSM */
    loop {
        let blkno: BlockNumber = GetFreeIndexPage(r);

        if blkno == InvalidBlockNumber {
            break; /* nothing left in FSM */
        }

        let buffer = ReadBuffer(r, blkno);

        /*
         * We have to guard against the possibility that someone else already
         * recycled this page; the buffer may be locked if so.
         */
        if ConditionalLockBuffer(buffer) {
            let page: Page = BufferGetPage(buffer);

            /*
             * If the page was never initialized, it's OK to use.
             */
            if PageIsNew(page) {
                return buffer;
            }

            gistcheckpage(r, buffer);

            /*
             * Otherwise, recycle it if deleted, and too old to have any
             * processes interested in it.
             */
            if gistPageRecyclable(page) {
                /*
                 * If we are generating WAL for Hot Standby then create a WAL
                 * record that will allow us to conflict with queries running
                 * on standby, in case they have snapshots older than the
                 * page's deleteXid.
                 */
                if XLogStandbyInfoActive() && RelationNeedsWAL(r) {
                    gistXLogPageReuse(r, heaprel, blkno, GistPageGetDeleteXid(page));
                }

                return buffer;
            }

            LockBuffer(buffer, GIST_UNLOCK);
        }

        /* Can't use it, so release buffer and try again */
        ReleaseBuffer(buffer);
    }

    /* Must extend the file */
    buffer = ExtendBufferedRel(BMR_REL(r), MAIN_FORKNUM, null_mut(), EB_LOCK_FIRST);

    buffer
}

/* Can this page be recycled yet? */
// NOTE: gistPageRecyclable is implemented elsewhere (declared in
// access/gist/gist_private.rs and imported above); the canonical definition
// lives in gistutil.c but the port keeps it in the shared module to avoid a
// duplicate symbol.  (C original reproduced below for reference.)
//
// bool gistPageRecyclable(Page page) { ... }

pub unsafe fn gistoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    const tab: [relopt_parse_elt; 2] = [
        relopt_parse_elt {
            optname: c"fillfactor".as_ptr(),
            opttype: RELOPT_TYPE_INT,
            offset: core::mem::offset_of!(GiSTOptions, fillfactor) as c_int,
        },
        relopt_parse_elt {
            optname: c"buffering".as_ptr(),
            opttype: RELOPT_TYPE_ENUM,
            offset: core::mem::offset_of!(GiSTOptions, buffering_mode) as c_int,
        },
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_GIST,
        core::mem::size_of::<GiSTOptions>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 *	gistproperty() -- Check boolean properties of indexes.
 *
 * This is optional for most AMs, but is required for GiST because the core
 * property code doesn't support AMPROP_DISTANCE_ORDERABLE.  We also handle
 * AMPROP_RETURNABLE here to save opening the rel to call gistcanreturn.
 */
pub unsafe fn gistproperty(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    _propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    let opclass: Oid;
    let mut opfamily: Oid = 0;
    let mut opcintype: Oid = 0;
    let procno: int16;

    /* Only answer column-level inquiries */
    if attno == 0 {
        return false;
    }

    /*
     * Currently, GiST distance-ordered scans require that there be a distance
     * function in the opclass with the default types (i.e. the one loaded
     * into the relcache entry, see initGISTstate).  So we assume that if such
     * a function exists, then there's a reason for it (rather than grubbing
     * through all the opfamily's operators to find an ordered one).
     *
     * Essentially the same code can test whether we support returning the
     * column data, since that's true if the opclass provides a fetch proc.
     */

    if prop == AMPROP_DISTANCE_ORDERABLE {
        procno = GIST_DISTANCE_PROC;
    } else if prop == AMPROP_RETURNABLE {
        procno = GIST_FETCH_PROC;
    } else {
        return false;
    }

    /* First we need to know the column's opclass. */
    opclass = get_index_column_opclass(index_oid, attno);
    if !OidIsValid(opclass) {
        *isnull = true;
        return true;
    }

    /* Now look up the opclass family and input datatype. */
    if !get_opclass_opfamily_and_input_type(opclass, &raw mut opfamily, &raw mut opcintype) {
        *isnull = true;
        return true;
    }

    /* And now we can check whether the function is provided. */

    *res = SearchSysCacheExists4(
        AMPROCNUM,
        ObjectIdGetDatum(opfamily),
        ObjectIdGetDatum(opcintype),
        ObjectIdGetDatum(opcintype),
        Int16GetDatum(procno),
    );

    /*
     * Special case: even without a fetch function, AMPROP_RETURNABLE is true
     * if the opclass has no compress function.
     */
    if prop == AMPROP_RETURNABLE && !*res {
        *res = !SearchSysCacheExists4(
            AMPROCNUM,
            ObjectIdGetDatum(opfamily),
            ObjectIdGetDatum(opcintype),
            ObjectIdGetDatum(opcintype),
            Int16GetDatum(GIST_COMPRESS_PROC),
        );
    }

    *isnull = false;

    true
}

/*
 * Some indexes are not WAL-logged, but we need LSNs to detect concurrent page
 * splits anyway. This function provides a fake sequence of LSNs for that
 * purpose.
 */
pub unsafe fn gistGetFakeLSN(rel: Relation) -> XLogRecPtr {
    if (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP {
        /*
         * Temporary relations are only accessible in our session, so a simple
         * backend-local counter will do.
         */
        static mut counter: XLogRecPtr = FirstNormalUnloggedLSN;

        let ret = counter;
        counter += 1;
        ret
    } else if RelationIsPermanent(rel) {
        /*
         * WAL-logging on this relation will start after commit, so its LSNs
         * must be distinct numbers smaller than the LSN at the next commit.
         * Emit a dummy WAL record if insert-LSN hasn't advanced after the
         * last call.
         */
        static mut lastlsn: XLogRecPtr = InvalidXLogRecPtr;
        let mut currlsn: XLogRecPtr = GetXLogInsertRecPtr();

        /* Shouldn't be called for WAL-logging relations */
        Assert!(!RelationNeedsWAL(rel));

        /* No need for an actual record if we already have a distinct LSN */
        if !XLogRecPtrIsInvalid(lastlsn) && lastlsn == currlsn {
            currlsn = gistXLogAssignLSN();
        }

        lastlsn = currlsn;
        currlsn
    } else {
        /*
         * Unlogged relations are accessible from other backends, and survive
         * (clean) restarts. GetFakeLSNForUnloggedRel() handles that for us.
         */
        Assert!((*(*rel).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED);
        GetFakeLSNForUnloggedRel()
    }
}

/*
 * This is a stratnum translation support function for GiST opclasses that use
 * the RT*StrategyNumber constants.
 */
pub unsafe fn gist_translate_cmptype_common(fcinfo: FunctionCallInfo) -> Datum {
    let cmptype: CompareType = PG_GETARG_INT32!(fcinfo, 0);

    match cmptype {
        COMPARE_EQ => PG_RETURN_UINT16!(RTEqualStrategyNumber),
        COMPARE_LT => PG_RETURN_UINT16!(RTLessStrategyNumber),
        COMPARE_LE => PG_RETURN_UINT16!(RTLessEqualStrategyNumber),
        COMPARE_GT => PG_RETURN_UINT16!(RTGreaterStrategyNumber),
        COMPARE_GE => PG_RETURN_UINT16!(RTGreaterEqualStrategyNumber),
        COMPARE_OVERLAP => PG_RETURN_UINT16!(RTOverlapStrategyNumber),
        COMPARE_CONTAINED_BY => PG_RETURN_UINT16!(RTContainedByStrategyNumber),
        _ => PG_RETURN_UINT16!(InvalidStrategy),
    }
}

/*
 * Returns the opclass's private stratnum used for the given compare type.
 *
 * Calls the opclass's GIST_TRANSLATE_CMPTYPE_PROC support function, if any,
 * and returns the result.  Returns InvalidStrategy if the function is not
 * defined.
 */
pub unsafe fn gisttranslatecmptype(cmptype: CompareType, opfamily: Oid) -> StrategyNumber {
    let funcid: Oid;
    let result: Datum;

    /* Check whether the function is provided. */
    funcid = get_opfamily_proc(
        opfamily,
        ANYOID,
        ANYOID,
        GIST_TRANSLATE_CMPTYPE_PROC as c_int,
    );
    if !OidIsValid(funcid) {
        return InvalidStrategy;
    }

    /* Ask the translation function */
    result = OidFunctionCall1Coll(funcid, InvalidOid, Int32GetDatum(cmptype));
    DatumGetUInt16(result)
}

// ===========================================================================
// Local stubs for unported dependencies.
// ===========================================================================

// memcpy / memmove via libc.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}

pub type Item = *mut c_char;

// reloptions.h types/values - no Rust port of access/common/reloptions.c yet.
// TODO: dedup from access/reloptions.h.
pub type relopt_kind = c_int;
pub const RELOPT_KIND_GIST: relopt_kind = 1 << 6;
pub type relopt_type = c_int;
pub const RELOPT_TYPE_INT: relopt_type = 1;
pub const RELOPT_TYPE_ENUM: relopt_type = 4;

#[repr(C)]
pub struct relopt_parse_elt {
    pub optname: *const c_char, /* option's name */
    pub opttype: relopt_type,   /* option's datatype */
    pub offset: c_int,          /* offset of field in result struct */
}

// MAXALIGN mirroring c.h; dedup once c.h MAXALIGN is universally exported.
// TODO: dedup MAXALIGN from c.h.
#[inline]
fn MAXALIGN(len: Size) -> Size {
    const MAXIMUM_ALIGNOF: Size = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// BLCKSZ from pg_config.h (default 8192). TODO: dedup BLCKSZ from pg_config.h.
const BLCKSZ: c_int = 8192;

// InvalidBlockNumber from storage/block.h. TODO: dedup.
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

// Buffer-manager / extension constants. TODO: dedup from storage/bufmgr.h.
const GIST_UNLOCK: c_int = 0; /* BUFFER_LOCK_UNLOCK */
const MAIN_FORKNUM: c_int = 0;
const EB_LOCK_FIRST: uint32 = 1 << 5;

unsafe fn IndexRelationGetNumberOfKeyAttributes(_relation: Relation) -> c_int { crate::access::nbtree::nbtdedup::IndexRelationGetNumberOfKeyAttributes(_relation) }
unsafe fn PageIsEmpty(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageInit(_page: Page, _pageSize: Size, _specialSize: Size) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageAddItem(
    _page: Page,
    _item: Item,
    _size: Size,
    _offsetNumber: OffsetNumber,
    _overwrite: bool,
    _is_heap: bool,
) -> OffsetNumber { crate::storage::bufpage::PageAddItem(_page, _item, _size, _offsetNumber, _overwrite, _is_heap) }
unsafe fn PageGetFreeSpace(_page: Page) -> Size {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetSpecialSize(_page: Page) -> uint16 {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetSpecialPointer(_page: Page) -> *mut c_char { crate::storage::bufpage::PageGetSpecialPointer(_page) }
unsafe fn PageGetContents(_page: Page) -> *mut c_char { crate::storage::bufpage::PageGetContents(_page) }
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ConditionalLockBuffer(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ExtendBufferedRel(
    _bmr: BufferManagerRelation,
    _fork: c_int,
    _strategy: *mut c_void,
    _flags: uint32,
) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BMR_REL(_p: Relation) -> BufferManagerRelation {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn GetFreeIndexPage(_rel: Relation) -> BlockNumber { crate::storage::freespace::indexfsm::GetFreeIndexPage(_rel) }
unsafe fn XLogStandbyInfoActive() -> bool { crate::access::nbtree::nbtpage::XLogStandbyInfoActive() }
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_rel) }
unsafe fn RelationIsPermanent(_rel: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetXLogInsertRecPtr() }
unsafe fn GetFakeLSNForUnloggedRel() -> XLogRecPtr { crate::access::transam::xlog::GetFakeLSNForUnloggedRel() }
unsafe fn build_reloptions(
    _reloptions: Datum,
    _validate: bool,
    _kind: relopt_kind,
    _relopt_struct_size: Size,
    _relopt_elems: *const relopt_parse_elt,
    _num_relopt_elems: c_int,
) -> *mut c_void { unimplemented!() }
unsafe fn get_index_column_opclass(_index_oid: Oid, _attno: c_int) -> Oid { crate::utils::cache::lsyscache::get_index_column_opclass(_index_oid, _attno) }
unsafe fn get_opclass_opfamily_and_input_type(
    _opclass: Oid,
    _opfamily: *mut Oid,
    _opcintype: *mut Oid,
) -> bool { crate::utils::cache::lsyscache::get_opclass_opfamily_and_input_type(_opclass, _opfamily, _opcintype) }
unsafe fn get_opfamily_proc(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _procnum: c_int,
) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn SearchSysCacheExists4(
    _cacheId: c_int,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}

// Catcache id (utils/syscache.h) and pg_type ANYOID (catalog/pg_type.h).
// TODO: dedup once syscache.h / pg_type.h are ported.
const AMPROCNUM: c_int = 5;
const ANYOID: Oid = 2276;

// relpersistence values (catalog/pg_class.h). TODO: dedup.
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;
const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;

/// storage/bufmgr.h: BufferManagerRelation, passed to ExtendBufferedRel.
/// TODO: dedup once storage/bufmgr.h is ported.
#[repr(C)]
pub struct BufferManagerRelation {
    pub rel: Relation,
    pub smgr: *mut c_void,
    pub relpersistence: c_char,
}
