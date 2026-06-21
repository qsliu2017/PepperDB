//! heaptoast.rs
//!   Heap-specific definitions for external and compressed storage of variable size attributes.
//!
//! Translated 1:1 from postgres/src/backend/access/heap/heaptoast.c
//!
//! Copyright (c) 2000-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/access/heap/heaptoast.c
//!
//! INTERFACE ROUTINES
//!		heap_toast_insert_or_update -
//!			Try to make a given tuple fit into one page by compressing
//!			or moving off attributes
//!
//!		heap_toast_delete -
//!			Reclaim toast storage when a tuple is deleted
//!
//! `#include`s mapped:
//!   - access/detoast.h        -> crate::access::common::detoast
//!   - access/genam.h          -> crate::access::index::genam
//!   - access/heapam.h         -> crate::access::heap::heapam (TOAST_MAX_CHUNK_SIZE stubbed)
//!   - access/heaptoast.h      -> the TOAST_TUPLE_TARGET* / TOAST_MAX_CHUNK_SIZE consts (stubbed below)
//!   - access/toast_helper.h   -> crate::access::table::toast_helper
//!   - access/toast_internals.h-> crate::access::common::toast_internals
//!   - utils/fmgroids.h        -> F_OIDEQ / F_INT4EQ / F_INT4GE / F_INT4LE (stubbed below)

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::access::common::detoast::{detoast_attr, detoast_external_attr};
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::common::toast_internals::{
    get_toast_snapshot, toast_close_indexes, toast_open_indexes,
};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr, TupleDescCompactAttr, TYPSTORAGE_EXTENDED};
use crate::access::htup_details::{
    fastgetattr, HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleHeaderSetDatumLength,
    HeapTupleHeaderSetNatts, HeapTupleHeaderSetTypMod, HeapTupleHeaderSetTypeId, BITMAPLEN,
    HEAP2_XACT_MASK, HEAP_XACT_MASK, HEAPTUPLESIZE, MaxHeapAttributeNumber, MaxTupleAttributeNumber,
    SizeofHeapTupleHeader,
};
use crate::access::common::heaptuple::{
    heap_compute_data_size, heap_deform_tuple, heap_fill_tuple, heap_form_tuple,
};
// TODO(pg-port): real systable_*_ordered + SysScanDesc live in access/index/genam.rs
// (orphan, unwired - has its own indkey/INJECTION_POINT divergences). Stub locally.
type SysScanDesc = *mut c_void;
unsafe fn systable_beginscan_ordered(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc { unimplemented!() }
unsafe fn systable_getnext_ordered(_sysscan: SysScanDesc, _direction: ScanDirection) -> HeapTuple { unimplemented!() }
unsafe fn systable_endscan_ordered(_sysscan: SysScanDesc) { unimplemented!() }
use crate::access::sdir::{ForwardScanDirection, ScanDirection};
use crate::access::stratnum::{
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTLessEqualStrategyNumber,
};
use crate::access::table::toast_helper::{
    toast_delete_external, toast_tuple_cleanup, toast_tuple_externalize,
    toast_tuple_find_biggest_attribute, toast_tuple_init, toast_tuple_try_compression,
    ToastAttrInfo, ToastTupleContext, TOASTCOL_INCOMPRESSIBLE, TOAST_HAS_NULLS, TOAST_NEEDS_CHANGE,
};
use crate::access::attnum::AttrNumber;
use crate::catalog::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION};
use crate::storage::itemptr::ItemPointerSetInvalid;
use crate::storage::lockdefs::AccessShareLock;
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::c::varlena;
use crate::varatt::{
    VARATT_IS_EXTENDED, VARATT_IS_EXTERNAL, VARATT_IS_COMPRESSED, VARATT_IS_SHORT, VARDATA,
    VARHDRSZ, VARHDRSZ_SHORT, VARSIZE,
};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// TODO(pg-port): real TOAST_MAX_CHUNK_SIZE lives in access/heaptoast.h
// (= EXTERN_TUPLE_MAX_SIZE - MAXALIGN(SizeofHeapTupleHeader) - sizeof(Oid)
//    - sizeof(int32) - VARHDRSZ), consumed via access/heapam.h (not yet ported).
/* #define TOAST_MAX_CHUNK_SIZE ... */
const TOAST_MAX_CHUNK_SIZE: int32 = 1996;

// ----------------------------------------------------------------------------
//   access/heaptoast.h consts
//
//   The heaptoast.h target/chunk-size macros are computed from BLCKSZ and page
//   header geometry.  TOAST_MAX_CHUNK_SIZE is imported from heapam (stubbed
//   there).  TOAST_TUPLE_TARGET / TOAST_TUPLE_TARGET_MAIN are reproduced here.
// ----------------------------------------------------------------------------

// TODO(pg-port): real TOAST_TUPLE_TARGET / TOAST_TUPLE_TARGET_MAIN live in
// access/heaptoast.h (= MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE[_MAIN])).
/* #define TOAST_TUPLE_TARGET TOAST_TUPLE_THRESHOLD */
pub const TOAST_TUPLE_TARGET: Size = 2048;
pub const TOAST_TUPLE_THRESHOLD: Size = TOAST_TUPLE_TARGET; // access/heaptoast.h
/* #define TOAST_TUPLE_TARGET_MAIN MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE_MAIN) */
pub const TOAST_TUPLE_TARGET_MAIN: Size = 8160;

// TODO(pg-port): real RelationGetToastTupleTarget lives in utils/rel.h.
/*
 * RelationGetToastTupleTarget
 *		Returns the relation's toast_tuple_target.  Note multiple eval of argument!
 */
unsafe fn RelationGetToastTupleTarget(_relation: Relation, defaulttarg: Size) -> Size {
    defaulttarg
}

// TODO(pg-port): real VARSIZE_SHORT / VARDATA_SHORT live in varatt.h.
/* #define VARSIZE_SHORT(PTR) VARSIZE_1B(PTR) */
unsafe fn VARSIZE_SHORT(ptr: *const c_char) -> uint32 {
    crate::varatt::VARSIZE_1B(ptr)
}
/* #define VARDATA_SHORT(PTR) VARDATA_1B(PTR) */
unsafe fn VARDATA_SHORT(ptr: *const c_char) -> *mut c_char {
    crate::varatt::VARDATA_1B(ptr)
}

// TODO(pg-port): real F_OIDEQ / F_INT4EQ / F_INT4GE / F_INT4LE live in utils/fmgroids.h.
const F_OIDEQ: Oid = 184;
const F_INT4EQ: Oid = 65;
const F_INT4GE: Oid = 150;
const F_INT4LE: Oid = 149;

/* ----------
 * heap_toast_delete -
 *
 *	Cascaded delete toast-entries on DELETE
 * ----------
 */
pub unsafe fn heap_toast_delete(rel: Relation, oldtup: HeapTuple, is_speculative: bool) {
    let tupleDesc: TupleDesc;
    let mut toast_values: [Datum; MaxHeapAttributeNumber as usize] =
        [0 as Datum; MaxHeapAttributeNumber as usize];
    let mut toast_isnull: [bool; MaxHeapAttributeNumber as usize] =
        [false; MaxHeapAttributeNumber as usize];

    /*
     * We should only ever be called for tuples of plain relations or
     * materialized views --- recursing on a toast rel is bad news.
     */
    Assert!(
        (*(*rel).rd_rel).relkind == RELKIND_RELATION
            || (*(*rel).rd_rel).relkind == RELKIND_MATVIEW
    );

    /*
     * Get the tuple descriptor and break down the tuple into fields.
     *
     * NOTE: it's debatable whether to use heap_deform_tuple() here or just
     * heap_getattr() only the varlena columns.  The latter could win if there
     * are few varlena columns and many non-varlena ones. However,
     * heap_deform_tuple costs only O(N) while the heap_getattr way would cost
     * O(N^2) if there are many varlena columns, so it seems better to err on
     * the side of linear cost.  (We won't even be here unless there's at
     * least one varlena column, by the way.)
     */
    tupleDesc = (*rel).rd_att;

    Assert!((*tupleDesc).natts <= MaxHeapAttributeNumber);
    heap_deform_tuple(
        oldtup,
        tupleDesc,
        toast_values.as_mut_ptr(),
        toast_isnull.as_mut_ptr(),
    );

    /* Do the real work. */
    toast_delete_external(
        rel,
        toast_values.as_ptr(),
        toast_isnull.as_ptr(),
        is_speculative,
    );
}

/* ----------
 * heap_toast_insert_or_update -
 *
 *	Delete no-longer-used toast-entries and create new ones to
 *	make the new tuple fit on INSERT or UPDATE
 *
 * Inputs:
 *	newtup: the candidate new tuple to be inserted
 *	oldtup: the old row version for UPDATE, or NULL for INSERT
 *	options: options to be passed to heap_insert() for toast rows
 * Result:
 *	either newtup if no toasting is needed, or a palloc'd modified tuple
 *	that is what should actually get stored
 *
 * NOTE: neither newtup nor oldtup will be modified.  This is a change
 * from the pre-8.1 API of this routine.
 * ----------
 */
pub unsafe fn heap_toast_insert_or_update(
    rel: Relation,
    newtup: HeapTuple,
    oldtup: HeapTuple,
    mut options: c_int,
) -> HeapTuple {
    let result_tuple: HeapTuple;
    let tupleDesc: TupleDesc;
    let numAttrs: c_int;

    let mut maxDataLen: Size;
    let mut hoff: Size;

    let mut toast_isnull: [bool; MaxHeapAttributeNumber as usize] =
        [false; MaxHeapAttributeNumber as usize];
    let mut toast_oldisnull: [bool; MaxHeapAttributeNumber as usize] =
        [false; MaxHeapAttributeNumber as usize];
    let mut toast_values: [Datum; MaxHeapAttributeNumber as usize] =
        [0 as Datum; MaxHeapAttributeNumber as usize];
    let mut toast_oldvalues: [Datum; MaxHeapAttributeNumber as usize] =
        [0 as Datum; MaxHeapAttributeNumber as usize];
    let mut toast_attr: [ToastAttrInfo; MaxHeapAttributeNumber as usize] =
        [std::mem::zeroed(); MaxHeapAttributeNumber as usize];
    let mut ttc: ToastTupleContext = std::mem::zeroed();

    /*
     * Ignore the INSERT_SPECULATIVE option. Speculative insertions/super
     * deletions just normally insert/delete the toast values. It seems
     * easiest to deal with that here, instead on, potentially, multiple
     * callers.
     */
    options &= !HEAP_INSERT_SPECULATIVE;

    /*
     * We should only ever be called for tuples of plain relations or
     * materialized views --- recursing on a toast rel is bad news.
     */
    Assert!(
        (*(*rel).rd_rel).relkind == RELKIND_RELATION
            || (*(*rel).rd_rel).relkind == RELKIND_MATVIEW
    );

    /*
     * Get the tuple descriptor and break down the tuple(s) into fields.
     */
    tupleDesc = (*rel).rd_att;
    numAttrs = (*tupleDesc).natts;

    Assert!(numAttrs <= MaxHeapAttributeNumber);
    heap_deform_tuple(
        newtup,
        tupleDesc,
        toast_values.as_mut_ptr(),
        toast_isnull.as_mut_ptr(),
    );
    if !oldtup.is_null() {
        heap_deform_tuple(
            oldtup,
            tupleDesc,
            toast_oldvalues.as_mut_ptr(),
            toast_oldisnull.as_mut_ptr(),
        );
    }

    /* ----------
     * Prepare for toasting
     * ----------
     */
    ttc.ttc_rel = rel;
    ttc.ttc_values = toast_values.as_mut_ptr();
    ttc.ttc_isnull = toast_isnull.as_mut_ptr();
    if oldtup.is_null() {
        ttc.ttc_oldvalues = std::ptr::null_mut();
        ttc.ttc_oldisnull = std::ptr::null_mut();
    } else {
        ttc.ttc_oldvalues = toast_oldvalues.as_mut_ptr();
        ttc.ttc_oldisnull = toast_oldisnull.as_mut_ptr();
    }
    ttc.ttc_attr = toast_attr.as_mut_ptr();
    toast_tuple_init(&raw mut ttc);

    /* ----------
     * Compress and/or save external until data fits into target length
     *
     *	1: Inline compress attributes with attstorage EXTENDED, and store very
     *	   large attributes with attstorage EXTENDED or EXTERNAL external
     *	   immediately
     *	2: Store attributes with attstorage EXTENDED or EXTERNAL external
     *	3: Inline compress attributes with attstorage MAIN
     *	4: Store attributes with attstorage MAIN external
     * ----------
     */

    /* compute header overhead --- this should match heap_form_tuple() */
    hoff = SizeofHeapTupleHeader;
    if (ttc.ttc_flags & TOAST_HAS_NULLS) != 0 {
        hoff += BITMAPLEN(numAttrs) as Size;
    }
    hoff = MAXALIGN(hoff);
    /* now convert to a limit on the tuple data size */
    maxDataLen = RelationGetToastTupleTarget(rel, TOAST_TUPLE_TARGET) - hoff;

    /*
     * Look for attributes with attstorage EXTENDED to compress.  Also find
     * large attributes with attstorage EXTENDED or EXTERNAL, and store them
     * external.
     */
    while heap_compute_data_size(tupleDesc, toast_values.as_ptr(), toast_isnull.as_ptr())
        > maxDataLen
    {
        let biggest_attno: c_int;

        biggest_attno = toast_tuple_find_biggest_attribute(&raw mut ttc, true, false);
        if biggest_attno < 0 {
            break;
        }

        /*
         * Attempt to compress it inline, if it has attstorage EXTENDED
         */
        if (*TupleDescAttr(tupleDesc, biggest_attno)).attstorage == TYPSTORAGE_EXTENDED {
            toast_tuple_try_compression(&raw mut ttc, biggest_attno);
        } else {
            /*
             * has attstorage EXTERNAL, ignore on subsequent compression
             * passes
             */
            toast_attr[biggest_attno as usize].tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
        }

        /*
         * If this value is by itself more than maxDataLen (after compression
         * if any), push it out to the toast table immediately, if possible.
         * This avoids uselessly compressing other fields in the common case
         * where we have one long field and several short ones.
         *
         * XXX maybe the threshold should be less than maxDataLen?
         */
        if toast_attr[biggest_attno as usize].tai_size as Size > maxDataLen
            && (*(*rel).rd_rel).reltoastrelid != InvalidOid
        {
            toast_tuple_externalize(&raw mut ttc, biggest_attno, options);
        }
    }

    /*
     * Second we look for attributes of attstorage EXTENDED or EXTERNAL that
     * are still inline, and make them external.  But skip this if there's no
     * toast table to push them to.
     */
    while heap_compute_data_size(tupleDesc, toast_values.as_ptr(), toast_isnull.as_ptr())
        > maxDataLen
        && (*(*rel).rd_rel).reltoastrelid != InvalidOid
    {
        let biggest_attno: c_int;

        biggest_attno = toast_tuple_find_biggest_attribute(&raw mut ttc, false, false);
        if biggest_attno < 0 {
            break;
        }
        toast_tuple_externalize(&raw mut ttc, biggest_attno, options);
    }

    /*
     * Round 3 - this time we take attributes with storage MAIN into
     * compression
     */
    while heap_compute_data_size(tupleDesc, toast_values.as_ptr(), toast_isnull.as_ptr())
        > maxDataLen
    {
        let biggest_attno: c_int;

        biggest_attno = toast_tuple_find_biggest_attribute(&raw mut ttc, true, true);
        if biggest_attno < 0 {
            break;
        }

        toast_tuple_try_compression(&raw mut ttc, biggest_attno);
    }

    /*
     * Finally we store attributes of type MAIN externally.  At this point we
     * increase the target tuple size, so that MAIN attributes aren't stored
     * externally unless really necessary.
     */
    maxDataLen = TOAST_TUPLE_TARGET_MAIN - hoff;

    while heap_compute_data_size(tupleDesc, toast_values.as_ptr(), toast_isnull.as_ptr())
        > maxDataLen
        && (*(*rel).rd_rel).reltoastrelid != InvalidOid
    {
        let biggest_attno: c_int;

        biggest_attno = toast_tuple_find_biggest_attribute(&raw mut ttc, false, true);
        if biggest_attno < 0 {
            break;
        }

        toast_tuple_externalize(&raw mut ttc, biggest_attno, options);
    }

    /*
     * In the case we toasted any values, we need to build a new heap tuple
     * with the changed values.
     */
    if (ttc.ttc_flags & TOAST_NEEDS_CHANGE) != 0 {
        let olddata: HeapTupleHeader = (*newtup).t_data;
        let new_data: HeapTupleHeader;
        let mut new_header_len: int32;
        let new_data_len: int32;
        let new_tuple_len: int32;

        /*
         * Calculate the new size of the tuple.
         *
         * Note: we used to assume here that the old tuple's t_hoff must equal
         * the new_header_len value, but that was incorrect.  The old tuple
         * might have a smaller-than-current natts, if there's been an ALTER
         * TABLE ADD COLUMN since it was stored; and that would lead to a
         * different conclusion about the size of the null bitmap, or even
         * whether there needs to be one at all.
         */
        new_header_len = SizeofHeapTupleHeader as int32;
        if (ttc.ttc_flags & TOAST_HAS_NULLS) != 0 {
            new_header_len += BITMAPLEN(numAttrs);
        }
        new_header_len = MAXALIGN(new_header_len as usize) as int32;
        new_data_len = heap_compute_data_size(
            tupleDesc,
            toast_values.as_ptr(),
            toast_isnull.as_ptr(),
        ) as int32;
        new_tuple_len = new_header_len + new_data_len;

        /*
         * Allocate and zero the space needed, and fill HeapTupleData fields.
         */
        result_tuple = palloc0(HEAPTUPLESIZE + new_tuple_len as usize) as HeapTuple;
        (*result_tuple).t_len = new_tuple_len as uint32;
        (*result_tuple).t_self = (*newtup).t_self;
        (*result_tuple).t_tableOid = (*newtup).t_tableOid;
        new_data = (result_tuple as *mut c_char).add(HEAPTUPLESIZE) as HeapTupleHeader;
        (*result_tuple).t_data = new_data;

        /*
         * Copy the existing tuple header, but adjust natts and t_hoff.
         */
        memcpy(
            new_data as *mut c_void,
            olddata as *const c_void,
            SizeofHeapTupleHeader,
        );
        HeapTupleHeaderSetNatts(new_data, numAttrs as uint16);
        (*new_data).t_hoff = new_header_len as uint8;

        /* Copy over the data, and fill the null bitmap if needed */
        heap_fill_tuple(
            tupleDesc,
            toast_values.as_ptr(),
            toast_isnull.as_ptr(),
            (new_data as *mut c_char).add(new_header_len as usize),
            new_data_len as Size,
            &raw mut (*new_data).t_infomask,
            if (ttc.ttc_flags & TOAST_HAS_NULLS) != 0 {
                (*new_data).t_bits.as_mut_ptr()
            } else {
                std::ptr::null_mut()
            },
        );
    } else {
        result_tuple = newtup;
    }

    toast_tuple_cleanup(&raw mut ttc);

    result_tuple
}

/* ----------
 * toast_flatten_tuple -
 *
 *	"Flatten" a tuple to contain no out-of-line toasted fields.
 *	(This does not eliminate compressed or short-header datums.)
 *
 *	Note: we expect the caller already checked HeapTupleHasExternal(tup),
 *	so there is no need for a short-circuit path.
 * ----------
 */
pub unsafe fn toast_flatten_tuple(tup: HeapTuple, tupleDesc: TupleDesc) -> HeapTuple {
    let new_tuple: HeapTuple;
    let numAttrs: c_int = (*tupleDesc).natts;
    let mut i: c_int;
    let mut toast_values: [Datum; MaxTupleAttributeNumber as usize] =
        [0 as Datum; MaxTupleAttributeNumber as usize];
    let mut toast_isnull: [bool; MaxTupleAttributeNumber as usize] =
        [false; MaxTupleAttributeNumber as usize];
    let mut toast_free: [bool; MaxTupleAttributeNumber as usize] =
        [false; MaxTupleAttributeNumber as usize];

    /*
     * Break down the tuple into fields.
     */
    Assert!(numAttrs <= MaxTupleAttributeNumber);
    heap_deform_tuple(
        tup,
        tupleDesc,
        toast_values.as_mut_ptr(),
        toast_isnull.as_mut_ptr(),
    );

    memset(
        toast_free.as_mut_ptr() as *mut c_void,
        0,
        numAttrs as usize * std::mem::size_of::<bool>(),
    );

    i = 0;
    while i < numAttrs {
        /*
         * Look at non-null varlena attributes
         */
        if !toast_isnull[i as usize] && (*TupleDescCompactAttr(tupleDesc, i)).attlen == -1 {
            let mut new_value: *mut varlena;

            new_value = DatumGetPointer(toast_values[i as usize]) as *mut varlena;
            if VARATT_IS_EXTERNAL(new_value as *const c_char) {
                new_value = detoast_external_attr(new_value);
                toast_values[i as usize] = PointerGetDatum(new_value as *const c_void);
                toast_free[i as usize] = true;
            }
        }
        i += 1;
    }

    /*
     * Form the reconfigured tuple.
     */
    new_tuple = heap_form_tuple(tupleDesc, toast_values.as_ptr(), toast_isnull.as_ptr());

    /*
     * Be sure to copy the tuple's identity fields.  We also make a point of
     * copying visibility info, just in case anybody looks at those fields in
     * a syscache entry.
     */
    (*new_tuple).t_self = (*tup).t_self;
    (*new_tuple).t_tableOid = (*tup).t_tableOid;

    (*(*new_tuple).t_data).t_choice = (*(*tup).t_data).t_choice;
    (*(*new_tuple).t_data).t_ctid = (*(*tup).t_data).t_ctid;
    (*(*new_tuple).t_data).t_infomask &= !HEAP_XACT_MASK;
    (*(*new_tuple).t_data).t_infomask |= (*(*tup).t_data).t_infomask & HEAP_XACT_MASK;
    (*(*new_tuple).t_data).t_infomask2 &= !HEAP2_XACT_MASK;
    (*(*new_tuple).t_data).t_infomask2 |= (*(*tup).t_data).t_infomask2 & HEAP2_XACT_MASK;

    /*
     * Free allocated temp values
     */
    i = 0;
    while i < numAttrs {
        if toast_free[i as usize] {
            pfree(DatumGetPointer(toast_values[i as usize]) as *mut c_void);
        }
        i += 1;
    }

    new_tuple
}

/* ----------
 * toast_flatten_tuple_to_datum -
 *
 *	"Flatten" a tuple containing out-of-line toasted fields into a Datum.
 *	The result is always palloc'd in the current memory context.
 *
 *	We have a general rule that Datums of container types (rows, arrays,
 *	ranges, etc) must not contain any external TOAST pointers.  Without
 *	this rule, we'd have to look inside each Datum when preparing a tuple
 *	for storage, which would be expensive and would fail to extend cleanly
 *	to new sorts of container types.
 *
 *	However, we don't want to say that tuples represented as HeapTuples
 *	can't contain toasted fields, so instead this routine should be called
 *	when such a HeapTuple is being converted into a Datum.
 *
 *	While we're at it, we decompress any compressed fields too.  This is not
 *	necessary for correctness, but reflects an expectation that compression
 *	will be more effective if applied to the whole tuple not individual
 *	fields.  We are not so concerned about that that we want to deconstruct
 *	and reconstruct tuples just to get rid of compressed fields, however.
 *	So callers typically won't call this unless they see that the tuple has
 *	at least one external field.
 *
 *	On the other hand, in-line short-header varlena fields are left alone.
 *	If we "untoasted" them here, they'd just get changed back to short-header
 *	format anyway within heap_fill_tuple.
 * ----------
 */
pub unsafe fn toast_flatten_tuple_to_datum(
    tup: HeapTupleHeader,
    tup_len: uint32,
    tupleDesc: TupleDesc,
) -> Datum {
    let new_data: HeapTupleHeader;
    let mut new_header_len: int32;
    let new_data_len: int32;
    let new_tuple_len: int32;
    let mut tmptup: HeapTupleData = std::mem::zeroed();
    let numAttrs: c_int = (*tupleDesc).natts;
    let mut i: c_int;
    let mut has_nulls: bool = false;
    let mut toast_values: [Datum; MaxTupleAttributeNumber as usize] =
        [0 as Datum; MaxTupleAttributeNumber as usize];
    let mut toast_isnull: [bool; MaxTupleAttributeNumber as usize] =
        [false; MaxTupleAttributeNumber as usize];
    let mut toast_free: [bool; MaxTupleAttributeNumber as usize] =
        [false; MaxTupleAttributeNumber as usize];

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len = tup_len;
    ItemPointerSetInvalid(&raw mut tmptup.t_self);
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = tup;

    /*
     * Break down the tuple into fields.
     */
    Assert!(numAttrs <= MaxTupleAttributeNumber);
    heap_deform_tuple(
        &raw mut tmptup,
        tupleDesc,
        toast_values.as_mut_ptr(),
        toast_isnull.as_mut_ptr(),
    );

    memset(
        toast_free.as_mut_ptr() as *mut c_void,
        0,
        numAttrs as usize * std::mem::size_of::<bool>(),
    );

    i = 0;
    while i < numAttrs {
        /*
         * Look at non-null varlena attributes
         */
        if toast_isnull[i as usize] {
            has_nulls = true;
        } else if (*TupleDescCompactAttr(tupleDesc, i)).attlen == -1 {
            let mut new_value: *mut varlena;

            new_value = DatumGetPointer(toast_values[i as usize]) as *mut varlena;
            if VARATT_IS_EXTERNAL(new_value as *const c_char)
                || VARATT_IS_COMPRESSED(new_value as *const c_char)
            {
                new_value = detoast_attr(new_value);
                toast_values[i as usize] = PointerGetDatum(new_value as *const c_void);
                toast_free[i as usize] = true;
            }
        }
        i += 1;
    }

    /*
     * Calculate the new size of the tuple.
     *
     * This should match the reconstruction code in
     * heap_toast_insert_or_update.
     */
    new_header_len = SizeofHeapTupleHeader as int32;
    if has_nulls {
        new_header_len += BITMAPLEN(numAttrs);
    }
    new_header_len = MAXALIGN(new_header_len as usize) as int32;
    new_data_len =
        heap_compute_data_size(tupleDesc, toast_values.as_ptr(), toast_isnull.as_ptr()) as int32;
    new_tuple_len = new_header_len + new_data_len;

    new_data = palloc0(new_tuple_len as usize) as HeapTupleHeader;

    /*
     * Copy the existing tuple header, but adjust natts and t_hoff.
     */
    memcpy(
        new_data as *mut c_void,
        tup as *const c_void,
        SizeofHeapTupleHeader,
    );
    HeapTupleHeaderSetNatts(new_data, numAttrs as uint16);
    (*new_data).t_hoff = new_header_len as uint8;

    /* Set the composite-Datum header fields correctly */
    HeapTupleHeaderSetDatumLength(new_data, new_tuple_len as uint32);
    HeapTupleHeaderSetTypeId(new_data, (*tupleDesc).tdtypeid);
    HeapTupleHeaderSetTypMod(new_data, (*tupleDesc).tdtypmod);

    /* Copy over the data, and fill the null bitmap if needed */
    heap_fill_tuple(
        tupleDesc,
        toast_values.as_ptr(),
        toast_isnull.as_ptr(),
        (new_data as *mut c_char).add(new_header_len as usize),
        new_data_len as Size,
        &raw mut (*new_data).t_infomask,
        if has_nulls {
            (*new_data).t_bits.as_mut_ptr()
        } else {
            std::ptr::null_mut()
        },
    );

    /*
     * Free allocated temp values
     */
    i = 0;
    while i < numAttrs {
        if toast_free[i as usize] {
            pfree(DatumGetPointer(toast_values[i as usize]) as *mut c_void);
        }
        i += 1;
    }

    PointerGetDatum(new_data as *const c_void)
}

/* ----------
 * toast_build_flattened_tuple -
 *
 *	Build a tuple containing no out-of-line toasted fields.
 *	(This does not eliminate compressed or short-header datums.)
 *
 *	This is essentially just like heap_form_tuple, except that it will
 *	expand any external-data pointers beforehand.
 *
 *	It's not very clear whether it would be preferable to decompress
 *	in-line compressed datums while at it.  For now, we don't.
 * ----------
 */
pub unsafe fn toast_build_flattened_tuple(
    tupleDesc: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
) -> HeapTuple {
    let new_tuple: HeapTuple;
    let numAttrs: c_int = (*tupleDesc).natts;
    let mut num_to_free: c_int;
    let mut i: c_int;
    let mut new_values: [Datum; MaxTupleAttributeNumber as usize] =
        [0 as Datum; MaxTupleAttributeNumber as usize];
    let mut freeable_values: [Pointer; MaxTupleAttributeNumber as usize] =
        [std::ptr::null_mut(); MaxTupleAttributeNumber as usize];

    /*
     * We can pass the caller's isnull array directly to heap_form_tuple, but
     * we potentially need to modify the values array.
     */
    Assert!(numAttrs <= MaxTupleAttributeNumber);
    memcpy(
        new_values.as_mut_ptr() as *mut c_void,
        values as *const c_void,
        numAttrs as usize * std::mem::size_of::<Datum>(),
    );

    num_to_free = 0;
    i = 0;
    while i < numAttrs {
        /*
         * Look at non-null varlena attributes
         */
        if !*isnull.add(i as usize) && (*TupleDescCompactAttr(tupleDesc, i)).attlen == -1 {
            let mut new_value: *mut varlena;

            new_value = DatumGetPointer(new_values[i as usize]) as *mut varlena;
            if VARATT_IS_EXTERNAL(new_value as *const c_char) {
                new_value = detoast_external_attr(new_value);
                new_values[i as usize] = PointerGetDatum(new_value as *const c_void);
                freeable_values[num_to_free as usize] = new_value as Pointer;
                num_to_free += 1;
            }
        }
        i += 1;
    }

    /*
     * Form the reconfigured tuple.
     */
    new_tuple = heap_form_tuple(tupleDesc, new_values.as_ptr(), isnull);

    /*
     * Free allocated temp values
     */
    i = 0;
    while i < num_to_free {
        pfree(freeable_values[i as usize] as *mut c_void);
        i += 1;
    }

    new_tuple
}

/*
 * Fetch a TOAST slice from a heap table.
 *
 * toastrel is the relation from which chunks are to be fetched.
 * valueid identifies the TOAST value from which chunks are being fetched.
 * attrsize is the total size of the TOAST value.
 * sliceoffset is the byte offset within the TOAST value from which to fetch.
 * slicelength is the number of bytes to be fetched from the TOAST value.
 * result is the varlena into which the results should be written.
 */
pub unsafe fn heap_fetch_toast_slice(
    toastrel: Relation,
    valueid: Oid,
    attrsize: int32,
    sliceoffset: int32,
    slicelength: int32,
    result: *mut varlena,
) {
    let mut toastidxs: *mut Relation = std::ptr::null_mut();
    let mut toastkey: [ScanKeyData; 3] = std::mem::zeroed();
    let toasttupDesc: TupleDesc = (*toastrel).rd_att;
    let nscankeys: c_int;
    let toastscan: SysScanDesc;
    let mut ttup: HeapTuple;
    let mut expectedchunk: int32;
    let totalchunks: int32 = ((attrsize - 1) / TOAST_MAX_CHUNK_SIZE) + 1;
    let startchunk: c_int;
    let endchunk: c_int;
    let mut num_indexes: c_int = 0;
    let validIndex: c_int;

    /* Look for the valid index of toast relation */
    validIndex = toast_open_indexes(
        toastrel as _,
        AccessShareLock,
        &raw mut toastidxs as _,
        &raw mut num_indexes,
    );

    startchunk = sliceoffset / TOAST_MAX_CHUNK_SIZE;
    endchunk = (sliceoffset + slicelength - 1) / TOAST_MAX_CHUNK_SIZE;
    Assert!(endchunk <= totalchunks);

    /* Set up a scan key to fetch from the index. */
    ScanKeyInit(
        &raw mut toastkey[0],
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(valueid),
    );

    /*
     * No additional condition if fetching all chunks. Otherwise, use an
     * equality condition for one chunk, and a range condition otherwise.
     */
    if startchunk == 0 && endchunk == totalchunks - 1 {
        nscankeys = 1;
    } else if startchunk == endchunk {
        ScanKeyInit(
            &raw mut toastkey[1],
            2 as AttrNumber,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum(startchunk),
        );
        nscankeys = 2;
    } else {
        ScanKeyInit(
            &raw mut toastkey[1],
            2 as AttrNumber,
            BTGreaterEqualStrategyNumber,
            F_INT4GE,
            Int32GetDatum(startchunk),
        );
        ScanKeyInit(
            &raw mut toastkey[2],
            2 as AttrNumber,
            BTLessEqualStrategyNumber,
            F_INT4LE,
            Int32GetDatum(endchunk),
        );
        nscankeys = 3;
    }

    /* Prepare for scan */
    toastscan = systable_beginscan_ordered(
        toastrel,
        *toastidxs.add(validIndex as usize),
        get_toast_snapshot(),
        nscankeys,
        toastkey.as_mut_ptr(),
    );

    /*
     * Read the chunks by index
     *
     * The index is on (valueid, chunkidx) so they will come in order
     */
    expectedchunk = startchunk;
    loop {
        ttup = systable_getnext_ordered(toastscan, ForwardScanDirection);
        if ttup.is_null() {
            break;
        }

        let curchunk: int32;
        let chunk: Pointer;
        let mut isnull: bool = false;
        let chunkdata: *mut c_char;
        let chunksize: int32;
        let expected_size: int32;
        let mut chcpystrt: int32;
        let mut chcpyend: int32;

        /*
         * Have a chunk, extract the sequence number and the data
         */
        curchunk = DatumGetInt32(fastgetattr(ttup, 2, toasttupDesc, &raw mut isnull));
        Assert!(!isnull);
        chunk = DatumGetPointer(fastgetattr(ttup, 3, toasttupDesc, &raw mut isnull)) as Pointer;
        Assert!(!isnull);
        if !VARATT_IS_EXTENDED(chunk as *const c_char) {
            chunksize = VARSIZE(chunk as *const c_char) as int32 - VARHDRSZ;
            chunkdata = VARDATA(chunk as *const c_char);
        } else if VARATT_IS_SHORT(chunk as *const c_char) {
            /* could happen due to heap_form_tuple doing its thing */
            chunksize = VARSIZE_SHORT(chunk as *const c_char) as int32 - VARHDRSZ_SHORT;
            chunkdata = VARDATA_SHORT(chunk as *const c_char);
        } else {
            /* should never happen */
            elog!(
                ERROR,
                "found toasted toast chunk for toast value {} in {}",
                valueid,
                std::ffi::CStr::from_ptr(RelationGetRelationName(toastrel)).to_string_lossy()
            );
            #[allow(unreachable_code)]
            {
                chunksize = 0; /* keep compiler quiet */
                chunkdata = std::ptr::null_mut();
            }
        }

        /*
         * Some checks on the data we've found
         */
        if curchunk != expectedchunk {
            ereport!(
                ERROR,
                errmsg!(
                    "unexpected chunk number {} (expected {}) for toast value {} in {}",
                    curchunk,
                    expectedchunk,
                    valueid,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(toastrel)).to_string_lossy()
                )
            );
        }
        if curchunk > endchunk {
            ereport!(
                ERROR,
                errmsg!(
                    "unexpected chunk number {} (out of range {}..{}) for toast value {} in {}",
                    curchunk,
                    startchunk,
                    endchunk,
                    valueid,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(toastrel)).to_string_lossy()
                )
            );
        }
        expected_size = if curchunk < totalchunks - 1 {
            TOAST_MAX_CHUNK_SIZE
        } else {
            attrsize - ((totalchunks - 1) * TOAST_MAX_CHUNK_SIZE)
        };
        if chunksize != expected_size {
            ereport!(
                ERROR,
                errmsg!(
                    "unexpected chunk size {} (expected {}) in chunk {} of {} for toast value {} in {}",
                    chunksize,
                    expected_size,
                    curchunk,
                    totalchunks,
                    valueid,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(toastrel)).to_string_lossy()
                )
            );
        }

        /*
         * Copy the data into proper place in our result
         */
        chcpystrt = 0;
        chcpyend = chunksize - 1;
        if curchunk == startchunk {
            chcpystrt = sliceoffset % TOAST_MAX_CHUNK_SIZE;
        }
        if curchunk == endchunk {
            chcpyend = (sliceoffset + slicelength - 1) % TOAST_MAX_CHUNK_SIZE;
        }

        memcpy(
            VARDATA(result as *const c_char)
                .offset((curchunk * TOAST_MAX_CHUNK_SIZE - sliceoffset + chcpystrt) as isize)
                as *mut c_void,
            chunkdata.offset(chcpystrt as isize) as *const c_void,
            ((chcpyend - chcpystrt) + 1) as usize,
        );

        expectedchunk += 1;
    }

    /*
     * Final checks that we successfully fetched the datum
     */
    if expectedchunk != (endchunk + 1) {
        ereport!(
            ERROR,
            errmsg!(
                "missing chunk number {} for toast value {} in {}",
                expectedchunk,
                valueid,
                std::ffi::CStr::from_ptr(RelationGetRelationName(toastrel)).to_string_lossy()
            )
        );
    }

    /* End scan and close indexes. */
    systable_endscan_ordered(toastscan);
    toast_close_indexes(toastidxs as _, num_indexes, AccessShareLock);
}

// TODO(pg-port): real HEAP_INSERT_SPECULATIVE lives in access/heapam.h.
/* #define HEAP_INSERT_SPECULATIVE 0x0010 */
const HEAP_INSERT_SPECULATIVE: c_int = 0x0010;
