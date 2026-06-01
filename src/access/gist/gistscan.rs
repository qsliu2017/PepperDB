//! access/gist/gistscan.c - routines to manage scans on GiST index relations.

use crate::prelude::*;
use crate::storage::block::BlockNumber;
use crate::access::common::tupdesc::TupleDesc;

use crate::access::common::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use crate::access::common::tupdesc::{
    CreateTemplateTupleDesc, TupleDescAttr, TupleDescInitEntry,
};
use crate::access::gist::gist_private::{
    createTempGistContext, freeGISTstate, initGISTstate, GISTSTATE, GISTScanOpaque,
    GISTScanOpaqueData, GISTSearchItem, GISTSearchItemIsHeap,
};
use crate::access::relscan::IndexScanDescData;
use crate::lib::pairingheap::{pairingheap_allocate, pairingheap_node};
use crate::utils::adt::float::float8_cmp_internal;
use crate::utils::fmgr::{fmgr_info_copy, FmgrInfo};
use crate::utils::rel::{RelationGetNumberOfAttributes, RelationGetRelationName};

use crate::access::transam::xlogdefs::InvalidXLogRecPtr;

// IndexScanDesc: amapi's public alias is an opaque *mut c_void, but this file
// needs to touch the real IndexScanDescData fields, so locally model it as a
// pointer to the ported struct. TODO: dedup once amapi exposes the concrete
// IndexScanDesc.
type IndexScanDesc = *mut IndexScanDescData;

// InvalidBlockNumber mirroring storage/block.h; dedup once exported.
// TODO: dedup InvalidBlockNumber from storage/block.h.
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

// GIST_DISTANCE_PROC mirroring access/gist.h (also defined in gistvalidate.rs).
// TODO: dedup once access/gist.h is ported.
const GIST_DISTANCE_PROC: c_int = 8;

// ---------------------------------------------------------------------------
// Locally-stubbed called functions not yet ported in their canonical modules.
// ---------------------------------------------------------------------------

// access/genam.h: RelationGetIndexScan. TODO: import once genam.c is ported.
unsafe fn RelationGetIndexScan(
    indexRelation: crate::utils::rel::Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    let _ = (indexRelation, nkeys, norderbys);
    unimplemented!()
}

// utils/lsyscache.h: get_func_rettype. TODO: import once lsyscache.c is ported.
unsafe fn get_func_rettype(funcid: Oid) -> Oid {
    let _ = funcid;
    unimplemented!()
}

// utils/rel.h: IndexRelationGetNumberOfKeyAttributes. TODO: import once exported.
unsafe fn IndexRelationGetNumberOfKeyAttributes(
    relation: crate::utils::rel::Relation,
) -> c_int {
    // relation->rd_index->indnkeyatts -- not yet modeled; mirror C macro intent.
    let _ = relation;
    unimplemented!()
}

/*
 * Pairing heap comparison function for the GISTSearchItem queue
 */
unsafe fn pairingheap_GISTSearchItem_cmp(
    a: *const pairingheap_node,
    b: *const pairingheap_node,
    arg: *mut c_void,
) -> c_int {
    let sa = a as *const GISTSearchItem;
    let sb = b as *const GISTSearchItem;
    let scan = arg as IndexScanDesc;
    let mut i: c_int;

    /* Order according to distance comparison */
    i = 0;
    while i < (*scan).numberOfOrderBys {
        let sa_d = (*sa).distances.as_ptr().add(i as usize);
        let sb_d = (*sb).distances.as_ptr().add(i as usize);
        if (*sa_d).isnull {
            if !(*sb_d).isnull {
                return -1;
            }
        } else if (*sb_d).isnull {
            return 1;
        } else {
            let cmp = -float8_cmp_internal((*sa_d).value, (*sb_d).value);

            if cmp != 0 {
                return cmp;
            }
        }
        i += 1;
    }

    /* Heap items go before inner pages, to ensure a depth-first search */
    if GISTSearchItemIsHeap(&*sa) && !GISTSearchItemIsHeap(&*sb) {
        return 1;
    }
    if !GISTSearchItemIsHeap(&*sa) && GISTSearchItemIsHeap(&*sb) {
        return -1;
    }

    0
}

/*
 * Index AM API functions for scanning GiST indexes
 */

pub unsafe fn gistbeginscan(
    r: crate::utils::rel::Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    let scan: IndexScanDesc;
    let giststate: *mut GISTSTATE;
    let so: GISTScanOpaque;
    let oldCxt: MemoryContext;

    scan = RelationGetIndexScan(r, nkeys, norderbys);

    /* First, set up a GISTSTATE with a scan-lifespan memory context */
    giststate = initGISTstate((*scan).indexRelation);

    /*
     * Everything made below is in the scanCxt, or is a child of the scanCxt,
     * so it'll all go away automatically in gistendscan.
     */
    oldCxt = MemoryContextSwitchTo((*giststate).scanCxt as *mut _);

    /* initialize opaque data */
    so = palloc0(core::mem::size_of::<GISTScanOpaqueData>()) as GISTScanOpaque;
    (*so).giststate = giststate;
    (*giststate).tempCxt = createTempGistContext();
    (*so).queue = null_mut();
    (*so).queueCxt = (*giststate).scanCxt; /* see gistrescan */

    /* workspaces with size dependent on numberOfOrderBys: */
    (*so).distances = palloc(
        core::mem::size_of::<crate::access::gist::gist_private::IndexOrderByDistance>()
            * (*scan).numberOfOrderBys as usize,
    ) as *mut _;
    (*so).qual_ok = true; /* in case there are zero keys */
    if (*scan).numberOfOrderBys > 0 {
        (*scan).xs_orderbyvals =
            palloc0(core::mem::size_of::<Datum>() * (*scan).numberOfOrderBys as usize)
                as *mut Datum;
        (*scan).xs_orderbynulls =
            palloc(core::mem::size_of::<bool>() * (*scan).numberOfOrderBys as usize) as *mut bool;
        core::ptr::write_bytes(
            (*scan).xs_orderbynulls,
            1u8,
            (*scan).numberOfOrderBys as usize,
        );
    }

    (*so).killedItems = null_mut(); /* until needed */
    (*so).numKilled = 0;
    (*so).curBlkno = InvalidBlockNumber;
    (*so).curPageLSN = InvalidXLogRecPtr;

    (*scan).opaque = so as *mut c_void;

    /*
     * All fields required for index-only scans are initialized in gistrescan,
     * as we don't know yet if we're doing an index-only scan or not.
     */

    MemoryContextSwitchTo(oldCxt);

    scan
}

pub unsafe fn gistrescan(
    scan: IndexScanDesc,
    key: *mut ScanKeyData,
    nkeys: c_int,
    orderbys: *mut ScanKeyData,
    norderbys: c_int,
) {
    let _ = (nkeys, norderbys);
    /* nkeys and norderbys arguments are ignored */
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;
    let first_time: bool;
    let mut i: c_int;
    let oldCxt: MemoryContext;

    /* rescan an existing indexscan --- reset state */

    /*
     * The first time through, we create the search queue in the scanCxt.
     * Subsequent times through, we create the queue in a separate queueCxt,
     * which is created on the second call and reset on later calls.  Thus, in
     * the common case where a scan is only rescan'd once, we just put the
     * queue in scanCxt and don't pay the overhead of making a second memory
     * context.  If we do rescan more than once, the first queue is just left
     * for dead until end of scan; this small wastage seems worth the savings
     * in the common case.
     */
    if (*so).queue.is_null() {
        /* first time through */
        Assert!((*so).queueCxt == (*(*so).giststate).scanCxt);
        first_time = true;
    } else if (*so).queueCxt == (*(*so).giststate).scanCxt {
        /* second time through */
        (*so).queueCxt = AllocSetContextCreate!(
            (*(*so).giststate).scanCxt,
            c"GiST queue context".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        ) as *mut _;
        first_time = false;
    } else {
        /* third or later time through */
        MemoryContextReset((*so).queueCxt as *mut _);
        first_time = false;
    }

    /*
     * If we're doing an index-only scan, on the first call, also initialize a
     * tuple descriptor to represent the returned index tuples and create a
     * memory context to hold them during the scan.
     */
    if (*scan).xs_want_itup && (*scan).xs_hitupdesc.is_null() {
        let natts: c_int;
        let nkeyatts: c_int;
        let mut attno: c_int;

        /*
         * The storage type of the index can be different from the original
         * datatype being indexed, so we cannot just grab the index's tuple
         * descriptor. Instead, construct a descriptor with the original data
         * types.
         */
        natts = RelationGetNumberOfAttributes((*scan).indexRelation);
        nkeyatts = IndexRelationGetNumberOfKeyAttributes((*scan).indexRelation);
        (*(*so).giststate).fetchTupdesc = CreateTemplateTupleDesc(natts);
        attno = 1;
        while attno <= nkeyatts {
            TupleDescInitEntry(
                (*(*so).giststate).fetchTupdesc,
                attno as crate::access::attnum::AttrNumber,
                null_mut(),
                *(*(*scan).indexRelation).rd_opcintype.add((attno - 1) as usize),
                -1,
                0,
            );
            attno += 1;
        }

        while attno <= natts {
            /* taking opcintype from giststate->tupdesc */
            TupleDescInitEntry(
                (*(*so).giststate).fetchTupdesc,
                attno as crate::access::attnum::AttrNumber,
                null_mut(),
                (*TupleDescAttr((*(*so).giststate).leafTupdesc, attno - 1)).atttypid,
                -1,
                0,
            );
            attno += 1;
        }
        (*scan).xs_hitupdesc = (*(*so).giststate).fetchTupdesc as *mut _;

        /* Also create a memory context that will hold the returned tuples */
        (*so).pageDataCxt = AllocSetContextCreate!(
            (*(*so).giststate).scanCxt,
            c"GiST page data context".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        ) as *mut _;
    }

    /* create new, empty pairing heap for search queue */
    oldCxt = MemoryContextSwitchTo((*so).queueCxt as *mut _);
    (*so).queue = pairingheap_allocate(
        pairingheap_GISTSearchItem_cmp,
        scan as *mut c_void,
    );
    MemoryContextSwitchTo(oldCxt);

    (*so).firstCall = true;

    /* Update scan key, if a new one is given */
    if !key.is_null() && (*scan).numberOfKeys > 0 {
        let mut fn_extras: *mut *mut c_void = null_mut();

        /*
         * If this isn't the first time through, preserve the fn_extra
         * pointers, so that if the consistentFns are using them to cache
         * data, that data is not leaked across a rescan.
         */
        if !first_time {
            fn_extras = palloc((*scan).numberOfKeys as usize * core::mem::size_of::<*mut c_void>())
                as *mut *mut c_void;
            i = 0;
            while i < (*scan).numberOfKeys {
                *fn_extras.add(i as usize) =
                    (*((*scan).keyData as *mut KeyDataView).add(i as usize)).sk_func.fn_extra;
                i += 1;
            }
        }

        core::ptr::copy_nonoverlapping(
            key as *const KeyDataView,
            (*scan).keyData as *mut KeyDataView,
            (*scan).numberOfKeys as usize,
        );

        /*
         * Modify the scan key so that the Consistent method is called for all
         * comparisons. The original operator is passed to the Consistent
         * function in the form of its strategy number, which is available
         * from the sk_strategy field, and its subtype from the sk_subtype
         * field.
         *
         * Next, if any of keys is a NULL and that key is not marked with
         * SK_SEARCHNULL/SK_SEARCHNOTNULL then nothing can be found (ie, we
         * assume all indexable operators are strict).
         */
        (*so).qual_ok = true;

        i = 0;
        while i < (*scan).numberOfKeys {
            let skey: *mut KeyDataView = (*scan).keyData as *mut KeyDataView;
            let skey = skey.add(i as usize);

            /*
             * Copy consistent support function to ScanKey structure instead
             * of function implementing filtering operator.
             */
            fmgr_info_copy(
                &mut (*skey).sk_func,
                &mut (*(*so).giststate).consistentFn[((*skey).sk_attno - 1) as usize],
                (*(*so).giststate).scanCxt as *mut _,
            );

            /* Restore prior fn_extra pointers, if not first time */
            if !first_time {
                (*skey).sk_func.fn_extra = *fn_extras.add(i as usize);
            }

            if ((*skey).sk_flags & SK_ISNULL) != 0 {
                if ((*skey).sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) == 0 {
                    (*so).qual_ok = false;
                }
            }
            i += 1;
        }

        if !first_time {
            pfree(fn_extras as *mut c_void);
        }
    }

    /* Update order-by key, if a new one is given */
    if !orderbys.is_null() && (*scan).numberOfOrderBys > 0 {
        let mut fn_extras: *mut *mut c_void = null_mut();

        /* As above, preserve fn_extra if not first time through */
        if !first_time {
            fn_extras =
                palloc((*scan).numberOfOrderBys as usize * core::mem::size_of::<*mut c_void>())
                    as *mut *mut c_void;
            i = 0;
            while i < (*scan).numberOfOrderBys {
                *fn_extras.add(i as usize) = (*((*scan).orderByData as *mut KeyDataView)
                    .add(i as usize))
                .sk_func
                .fn_extra;
                i += 1;
            }
        }

        core::ptr::copy_nonoverlapping(
            orderbys as *const KeyDataView,
            (*scan).orderByData as *mut KeyDataView,
            (*scan).numberOfOrderBys as usize,
        );

        (*so).orderByTypes =
            palloc((*scan).numberOfOrderBys as usize * core::mem::size_of::<Oid>()) as *mut Oid;

        /*
         * Modify the order-by key so that the Distance method is called for
         * all comparisons. The original operator is passed to the Distance
         * function in the form of its strategy number, which is available
         * from the sk_strategy field, and its subtype from the sk_subtype
         * field.
         */
        i = 0;
        while i < (*scan).numberOfOrderBys {
            let skey: *mut KeyDataView =
                ((*scan).orderByData as *mut KeyDataView).add(i as usize);
            let finfo: *mut FmgrInfo =
                &mut (*(*so).giststate).distanceFn[((*skey).sk_attno - 1) as usize];

            /* Check we actually have a distance function ... */
            if !OidIsValid((*finfo).fn_oid) {
                elog!(
                    ERROR,
                    "missing support function {} for attribute {} of index \"{}\"",
                    GIST_DISTANCE_PROC,
                    (*skey).sk_attno,
                    cstr_to_str(RelationGetRelationName((*scan).indexRelation))
                );
            }

            /*
             * Look up the datatype returned by the original ordering
             * operator. GiST always uses a float8 for the distance function,
             * but the ordering operator could be anything else.
             *
             * XXX: The distance function is only allowed to be lossy if the
             * ordering operator's result type is float4 or float8.  Otherwise
             * we don't know how to return the distance to the executor.  But
             * we cannot check that here, as we won't know if the distance
             * function is lossy until it returns *recheck = true for the
             * first time.
             */
            *(*so).orderByTypes.add(i as usize) = get_func_rettype((*skey).sk_func.fn_oid);

            /*
             * Copy distance support function to ScanKey structure instead of
             * function implementing ordering operator.
             */
            fmgr_info_copy(&mut (*skey).sk_func, finfo, (*(*so).giststate).scanCxt as *mut _);

            /* Restore prior fn_extra pointers, if not first time */
            if !first_time {
                (*skey).sk_func.fn_extra = *fn_extras.add(i as usize);
            }
            i += 1;
        }

        if !first_time {
            pfree(fn_extras as *mut c_void);
        }
    }

    /* any previous xs_hitup will have been pfree'd in context resets above */
    (*scan).xs_hitup = null_mut();
}

pub unsafe fn gistendscan(scan: IndexScanDesc) {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;

    /*
     * freeGISTstate is enough to clean up everything made by gistbeginscan,
     * as well as the queueCxt if there is a separate context for it.
     */
    freeGISTstate((*so).giststate);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// The relscan IndexScanDescData stores keyData/orderByData as *mut ScanKeyData
// where ScanKeyData is the opaque c_void alias from access/relscan.rs. This
// file works with the concrete ScanKeyData from access/common/scankey.rs; the
// two are identical in layout, so reinterpret through this name. TODO: dedup
// once relscan.rs uses the ported ScanKeyData.
type KeyDataView = ScanKeyData;

// Render a NUL-terminated C string for elog! formatting.
unsafe fn cstr_to_str<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}
