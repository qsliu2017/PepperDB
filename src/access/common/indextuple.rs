//! Translation of postgres/src/backend/access/common/indextuple.c
//!                (+ merged static inlines / struct defs from
//!                 postgres/src/include/access/itup.h)
//!
//! Index-tuple form/deform layer.  This PARALLELS heaptuple.c: index_form_tuple
//! reuses heap_compute_data_size + heap_fill_tuple to size/fill the data area,
//! and nocache_index_getattr / index_deform_tuple_internal are line-for-line the
//! same attribute walk as nocachegetattr / heap_deform_tuple, differing only in
//! the tuple header layout (IndexTupleData + optional null bitmap, vs the heap
//! tuple header).
//!
//! Byte-level correctness matters: the offsets computed here must match the
//! on-disk index-tuple layout and what index_getattr expects.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h                  -> crate::prelude
//!   access/detoast.h            -> crate::access::common::detoast
//!                                  (detoast_external_attr).
//!   access/heaptoast.h          -> only TOAST_INDEX_TARGET is referenced; it is
//!                                  defined locally below (heaptoast.h not ported
//!                                  as a module).
//!   access/htup_details.h       -> crate::access::htup_details (HEAP_HASVARWIDTH /
//!                                  HEAP_HASEXTERNAL infomask bits inspected after
//!                                  heap_fill_tuple).
//!   access/itup.h               -> THIS FILE (struct/const/inline defs merged in).
//!   access/toast_internals.h    -> crate::access::common::toast_internals
//!                                  (toast_compress_datum).
//!   access/tupdesc.h            -> crate::access::common::tupdesc
//!                                  (TupleDesc/TupleDescAttr/TupleDescCompactAttr/
//!                                  CompactAttribute/CreateTupleDescTruncatedCopy).
//!   access/tupmacs.h            -> crate::access::tupmacs (att_* / fetch_att).
//!   storage/bufpage.h           -> only used by MaxIndexTuplesPerPage; that macro
//!                                  is omitted here (bufpage.h not ported) - it is
//!                                  not used by any indextuple.c routine.
//!   storage/itemptr.h           -> crate::storage::itemptr (ItemPointerData).
//!   catalog/pg_attribute.h      -> crate::catalog::pg_attribute (Form_pg_attribute).
//!   common heap helpers         -> crate::access::common::heaptuple
//!                                  (heap_compute_data_size / heap_fill_tuple).
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: index_form_tuple, index_form_tuple_context (incl. the TOAST_INDEX_HACK
//!     detoast-external + in-line-compress passes - detoast_external_attr and
//!     toast_compress_datum are both ported), nocache_index_getattr,
//!     index_deform_tuple, index_deform_tuple_internal, CopyIndexTuple,
//!     index_truncate_tuple, and all the itup.h inline accessors
//!     (IndexTupleSize / IndexTupleHasNulls / IndexTupleHasVarwidths /
//!     IndexInfoFindDataOffset / index_getattr).
//!   STUBBED: none in this file.  Note however that heap_fill_tuple's fill_val has
//!     one stubbed sub-branch (external non-expanded TOAST pointer, needs
//!     VARSIZE_EXTERNAL).  index_form_tuple's TOAST_INDEX_HACK pass detoasts any
//!     EXTERNAL value to a plain varlena before calling heap_fill_tuple, so that
//!     stubbed branch is never reached on the index path (and the C code Asserts
//!     HEAP_HASEXTERNAL == 0 afterwards).

use crate::prelude::*;

use crate::access::common::detoast::detoast_external_attr;
use crate::access::common::heaptuple::{heap_compute_data_size, heap_fill_tuple};
use crate::access::common::toast_internals::toast_compress_datum;
use crate::access::common::tupdesc::{
    CompactAttribute, CreateTupleDescTruncatedCopy, TupleDesc, TupleDescAttr, TupleDescCompactAttr,
    TYPSTORAGE_EXTENDED, TYPSTORAGE_MAIN,
};
use crate::access::htup_details::{HEAP_HASEXTERNAL, HEAP_HASVARWIDTH};
use crate::access::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att,
};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::c::varlena;
use crate::storage::itemptr::ItemPointerData;
use crate::varatt::{VARATT_IS_EXTENDED, VARATT_IS_EXTERNAL, VARSIZE};

use core::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   errcodes.h (not ported) - errcode() shim ignores the value; kept named for
//   fidelity.
// ----------------------------------------------------------------------------

const ERRCODE_TOO_MANY_COLUMNS: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

// ----------------------------------------------------------------------------
//   pg_config_manual.h: INDEX_MAX_KEYS (also defined in nodes/pathnodes.rs; kept
//   local to avoid coupling indextuple.rs to the planner module).
// ----------------------------------------------------------------------------

pub const INDEX_MAX_KEYS: usize = 32;

// ----------------------------------------------------------------------------
//   access/heaptoast.h: TOAST_INDEX_TARGET = MaxHeapTupleSize / 16.
//
//   MaxHeapTupleSize = (BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) with BLCKSZ
//   == 8192 and SizeOfPageHeaderData == 24, giving MaxHeapTupleSize == 8160 and
//   TOAST_INDEX_TARGET == 510.  bufpage.h / heaptoast.h are not ported as
//   modules, so the constant is reproduced directly here.
// ----------------------------------------------------------------------------

const TOAST_INDEX_TARGET: uint32 = 510;

/* TOAST_INDEX_HACK is always defined in upstream indextuple.c. */

// ============================================================================
//   itup.h: index tuple header structure and t_info accessors.
// ============================================================================

/*
 * Index tuple header structure
 *
 * All index tuples start with IndexTupleData.  If the HasNulls bit is set,
 * this is followed by an IndexAttributeBitMapData.  The index attribute
 * values follow, beginning at a MAXALIGN boundary.
 *
 * MORE DATA FOLLOWS AT END OF STRUCT.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexTupleData {
    /* reference TID to heap tuple */
    pub t_tid: ItemPointerData,

    /*
     * t_info is laid out in the following fashion:
     *   15th (high) bit: has nulls
     *   14th bit: has var-width attributes
     *   13th bit: AM-defined meaning
     *   12-0 bit: size of tuple
     */
    pub t_info: u16,
}

pub type IndexTuple = *mut IndexTupleData;

/*
 * The null bitmap that optionally follows IndexTupleData.  Its size does not
 * vary with the number of attributes (see itup.h comment).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexAttributeBitMapData {
    pub bits: [bits8; (INDEX_MAX_KEYS + 8 - 1) / 8],
}

pub type IndexAttributeBitMap = *mut IndexAttributeBitMapData;

/*
 * t_info manipulation macros.
 */
pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
/* reserved for index-AM specific usage */
pub const INDEX_AM_RESERVED_BIT: u16 = 0x2000;
pub const INDEX_VAR_MASK: u16 = 0x4000;
pub const INDEX_NULL_MASK: u16 = 0x8000;

/*
 * IndexTupleSize - size of the tuple stored in the low 13 bits of t_info.
 *
 * # Safety
 * `itup` references a live IndexTupleData.
 */
#[inline]
pub unsafe fn IndexTupleSize(itup: *const IndexTupleData) -> Size {
    ((*itup).t_info & INDEX_SIZE_MASK) as Size
}

/*
 * IndexTupleHasNulls - high bit of t_info.
 *
 * # Safety
 * `itup` references a live IndexTupleData.
 */
#[inline]
pub unsafe fn IndexTupleHasNulls(itup: *const IndexTupleData) -> bool {
    ((*itup).t_info & INDEX_NULL_MASK) != 0
}

/*
 * IndexTupleHasVarwidths - 14th bit of t_info.
 *
 * # Safety
 * `itup` references a live IndexTupleData.
 */
#[inline]
pub unsafe fn IndexTupleHasVarwidths(itup: *const IndexTupleData) -> bool {
    ((*itup).t_info & INDEX_VAR_MASK) != 0
}

/*
 * IndexInfoFindDataOffset(t_info)
 *
 * Takes an infomask as argument (primarily because this needs to be usable at
 * index_form_tuple time so enough space is allocated): returns the MAXALIGN'd
 * size of the index-tuple header (with the null bitmap iff INDEX_NULL_MASK set).
 */
#[inline]
pub fn IndexInfoFindDataOffset(t_info: u16) -> Size {
    if (t_info & INDEX_NULL_MASK) == 0 {
        MAXALIGN(size_of::<IndexTupleData>())
    } else {
        MAXALIGN(size_of::<IndexTupleData>() + size_of::<IndexAttributeBitMapData>())
    }
}

/*
 * fetchatt - fetch_att over a CompactAttribute (tupmacs.h #define rendered
 * local, identical to heaptuple.rs's copy).
 *
 * # Safety
 * `att` is a live CompactAttribute; `T` points to a properly-aligned field of
 * at least attlen readable bytes.
 */
#[inline]
unsafe fn fetchatt(att: *const CompactAttribute, T: *const c_char) -> Datum {
    fetch_att(T as *const c_void, (*att).attbyval, (*att).attlen as c_int)
}

/* ----------------
 *		index_getattr
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocache_index_getattr() for the rest.
 * ----------------
 *
 * # Safety
 * `tup` is a valid IndexTuple; `tupleDesc` is live and matches it; `attnum`
 * (1-based) is positive; `isnull` is writable.
 */
#[inline]
pub unsafe fn index_getattr(
    tup: IndexTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    Assert!(PointerIsValid(isnull));
    Assert!(attnum > 0);

    *isnull = false;

    if !IndexTupleHasNulls(tup) {
        let attr = TupleDescCompactAttr(tupleDesc, attnum - 1);

        if (*attr).attcacheoff >= 0 {
            fetchatt(
                attr,
                (tup as *const c_char)
                    .add(IndexInfoFindDataOffset((*tup).t_info))
                    .add((*attr).attcacheoff as usize),
            )
        } else {
            nocache_index_getattr(tup, attnum, tupleDesc)
        }
    } else if att_isnull(
        attnum - 1,
        (tup as *const c_char).add(size_of::<IndexTupleData>()) as *const bits8,
    ) {
        *isnull = true;
        PointerGetDatum(null())
    } else {
        nocache_index_getattr(tup, attnum, tupleDesc)
    }
}

/* ----------------------------------------------------------------
 *				  index_ tuple interface routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		index_form_tuple
 *
 *		As index_form_tuple_context, but allocates the returned tuple in the
 *		CurrentMemoryContext.
 * ----------------
 *
 * # Safety
 * `tupleDescriptor` is live; `values`/`isnull` point to natts elements.
 */
pub unsafe fn index_form_tuple(
    tupleDescriptor: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
) -> IndexTuple {
    index_form_tuple_context(tupleDescriptor, values, isnull, CurrentMemoryContext)
}

/* ----------------
 *		index_form_tuple_context
 *
 *		This shouldn't leak any memory; otherwise, callers such as
 *		tuplesort_putindextuplevalues() will be very unhappy.
 *
 *		This shouldn't perform external table access provided caller
 *		does not pass values that are stored EXTERNAL.
 *
 *		Allocates returned tuple in provided 'context'.
 * ----------------
 *
 * # Safety
 * `tupleDescriptor` is live; `values`/`isnull` point to natts elements;
 * `context` is a valid MemoryContext.
 */
pub unsafe fn index_form_tuple_context(
    tupleDescriptor: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
    context: MemoryContext,
) -> IndexTuple {
    let tp: *mut c_char; /* tuple pointer */
    let tuple: IndexTuple; /* return tuple */
    let mut size: Size;
    let data_size: Size;
    let hoff: Size;
    let mut infomask: u16 = 0;
    let mut hasnull = false;
    let mut tupmask: uint16 = 0;
    let numberOfAttributes = (*tupleDescriptor).natts;

    /* TOAST_INDEX_HACK scratch arrays */
    let mut untoasted_values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut untoasted_free: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];

    if (numberOfAttributes as usize) > INDEX_MAX_KEYS {
        let _ = errcode(ERRCODE_TOO_MANY_COLUMNS);
        ereport!(
            ERROR,
            errmsg!(
                "number of index columns ({}) exceeds limit ({})",
                numberOfAttributes,
                INDEX_MAX_KEYS
            )
        );
    }

    /* TOAST_INDEX_HACK: detoast external + try in-line compression */
    for i in 0..numberOfAttributes as usize {
        let att = TupleDescAttr(tupleDescriptor, i as c_int);

        untoasted_values[i] = *values.add(i);
        untoasted_free[i] = false;

        /* Do nothing if value is NULL or not of varlena type */
        if *isnull.add(i) || (*att).attlen != -1 {
            continue;
        }

        /*
         * If value is stored EXTERNAL, must fetch it so we are not depending
         * on outside storage.  This should be improved someday.
         */
        if VARATT_IS_EXTERNAL(DatumGetPointer(*values.add(i)) as *const c_char) {
            untoasted_values[i] = PointerGetDatum(detoast_external_attr(
                DatumGetPointer(*values.add(i)) as *mut varlena,
            ) as *const c_void);
            untoasted_free[i] = true;
        }

        /*
         * If value is above size target, and is of a compressible datatype,
         * try to compress it in-line.
         */
        if !VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i]) as *const c_char)
            && VARSIZE(DatumGetPointer(untoasted_values[i]) as *const c_char) > TOAST_INDEX_TARGET
            && ((*att).attstorage == TYPSTORAGE_EXTENDED || (*att).attstorage == TYPSTORAGE_MAIN)
        {
            let cvalue = toast_compress_datum(untoasted_values[i], (*att).attcompression);

            if !DatumGetPointer(cvalue).is_null() {
                /* successful compression */
                if untoasted_free[i] {
                    pfree(DatumGetPointer(untoasted_values[i]) as *mut c_void);
                }
                untoasted_values[i] = cvalue;
                untoasted_free[i] = true;
            }
        }
    }

    for i in 0..numberOfAttributes as usize {
        if *isnull.add(i) {
            hasnull = true;
            break;
        }
    }

    if hasnull {
        infomask |= INDEX_NULL_MASK;
    }

    hoff = IndexInfoFindDataOffset(infomask);
    data_size = heap_compute_data_size(tupleDescriptor, untoasted_values.as_ptr(), isnull);
    size = hoff + data_size;
    size = MAXALIGN(size); /* be conservative */

    tp = MemoryContextAllocZero(context, size) as *mut c_char;
    tuple = tp as IndexTuple;

    heap_fill_tuple(
        tupleDescriptor,
        untoasted_values.as_ptr(),
        isnull,
        tp.add(hoff),
        data_size,
        &mut tupmask,
        if hasnull {
            tp.add(size_of::<IndexTupleData>()) as *mut bits8
        } else {
            null_mut()
        },
    );

    /* free anything detoasted/compressed above */
    for i in 0..numberOfAttributes as usize {
        if untoasted_free[i] {
            pfree(DatumGetPointer(untoasted_values[i]) as *mut c_void);
        }
    }

    /*
     * We do this because heap_fill_tuple wants to initialize a "tupmask" which
     * is used for HeapTuples, but we want an indextuple infomask. The only
     * relevant info is the "has variable attributes" field. We have already set
     * the hasnull bit above.
     */
    if (tupmask & HEAP_HASVARWIDTH) != 0 {
        infomask |= INDEX_VAR_MASK;
    }

    /* Also assert we got rid of external attributes */
    Assert!((tupmask & HEAP_HASEXTERNAL) == 0);

    /*
     * Here we make sure that the size will fit in the field reserved for it in
     * t_info.
     */
    if (size & INDEX_SIZE_MASK as Size) != size {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "index row requires {} bytes, maximum size is {}",
                size,
                INDEX_SIZE_MASK as Size
            )
        );
    }

    infomask |= size as u16;

    /*
     * initialize metadata
     */
    (*tuple).t_info = infomask;
    tuple
}

/* ----------------
 *		nocache_index_getattr
 *
 *		This gets called from index_getattr() macro, and only in cases
 *		where we can't use cacheoffset and the value is not null.
 *
 *		This caches attribute offsets in the attribute descriptor.
 *
 *		(Same code as nocachegetattr, but for IndexTuples - see -cim 5/4/91
 *		discussion in indextuple.c.)
 * ----------------
 *
 * # Safety
 * `tup` is a valid IndexTuple; `tupleDesc` is live and matches it; `attnum`
 * (1-based) is a non-null attribute present in the tuple.
 */
pub unsafe fn nocache_index_getattr(tup: IndexTuple, attnum: c_int, tupleDesc: TupleDesc) -> Datum {
    let tp: *mut c_char; /* ptr to data part of tuple */
    let mut bp: *mut bits8 = null_mut(); /* ptr to null bitmap in tuple */
    let mut slow = false; /* do we have to walk attrs? */
    let data_off: Size; /* tuple data offset */
    let mut off: c_int; /* current offset within data */

    /* ----------------
     *	 Three cases:
     *
     *	 1: No nulls and no variable-width attributes.
     *	 2: Has a null or a var-width AFTER att.
     *	 3: Has nulls or var-widths BEFORE att.
     * ----------------
     */

    data_off = IndexInfoFindDataOffset((*tup).t_info);

    let attnum = attnum - 1;

    if IndexTupleHasNulls(tup) {
        /*
         * there's a null somewhere in the tuple
         *
         * check to see if desired att is null
         */

        /* XXX "knows" t_bits are just after fixed tuple header! */
        bp = (tup as *mut c_char).add(size_of::<IndexTupleData>()) as *mut bits8;

        /*
         * Now check to see if any preceding bits are null...
         */
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;

        /* check for nulls "before" final bit of last byte */
        if ((!*bp.add(byte as usize)) & (((1 << finalbit) - 1) as bits8)) != 0 {
            slow = true;
        } else {
            /* check for nulls in any "earlier" bytes */
            for i in 0..byte {
                if *bp.add(i as usize) != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
    }

    tp = (tup as *mut c_char).add(data_off);

    if !slow {
        /*
         * If we get here, there are no nulls up to and including the target
         * attribute.  If we have a cached offset, we can use it.
         */
        let att = TupleDescCompactAttr(tupleDesc, attnum);
        if (*att).attcacheoff >= 0 {
            return fetchatt(att, tp.add((*att).attcacheoff as usize));
        }

        /*
         * Otherwise, check for non-fixed-length attrs up to and including
         * target.  If there aren't any, it's safe to cheaply initialize the
         * cached offsets for these attrs.
         */
        if IndexTupleHasVarwidths(tup) {
            for j in 0..=attnum {
                if (*TupleDescCompactAttr(tupleDesc, j)).attlen <= 0 {
                    slow = true;
                    break;
                }
            }
        }
    }

    if !slow {
        let natts = (*tupleDesc).natts;
        let mut j = 1;

        /*
         * If we get here, we have a tuple with no nulls or var-widths up to
         * and including the target attribute, so we can use the cached offset
         * ... only we don't have it yet, or we'd not have got here.  Since
         * it's cheap to compute offsets for fixed-width columns, we take the
         * opportunity to initialize the cached offsets for *all* the leading
         * fixed-width columns, in hope of avoiding future visits to this
         * routine.
         */
        (*TupleDescCompactAttr(tupleDesc, 0)).attcacheoff = 0;

        /* we might have set some offsets in the slow path previously */
        while j < natts && (*TupleDescCompactAttr(tupleDesc, j)).attcacheoff > 0 {
            j += 1;
        }

        off = (*TupleDescCompactAttr(tupleDesc, j - 1)).attcacheoff
            + (*TupleDescCompactAttr(tupleDesc, j - 1)).attlen as c_int;

        while j < natts {
            let att = TupleDescCompactAttr(tupleDesc, j);

            if (*att).attlen <= 0 {
                break;
            }

            off = att_nominal_alignby(off as usize, (*att).attalignby) as c_int;

            (*att).attcacheoff = off;

            off += (*att).attlen as c_int;

            j += 1;
        }

        Assert!(j > attnum);

        off = (*TupleDescCompactAttr(tupleDesc, attnum)).attcacheoff;
    } else {
        let mut usecache = true;
        let mut i = 0;

        /*
         * Now we know that we have to walk the tuple CAREFULLY.  But we still
         * might be able to cache some offsets for next time.
         *
         * Note - This loop is a little tricky.  For each non-null attribute,
         * we have to first account for alignment padding before the attr,
         * then advance over the attr based on its length.  Nulls have no
         * storage and no alignment padding either.  We can use/set
         * attcacheoff until we reach either a null or a var-width attribute.
         */
        off = 0;
        loop {
            /* loop exit is at "break" */
            let att = TupleDescCompactAttr(tupleDesc, i);

            if IndexTupleHasNulls(tup) && att_isnull(i, bp) {
                usecache = false;
                i += 1;
                continue; /* this cannot be the target att */
            }

            /* If we know the next offset, we can skip the rest */
            if usecache && (*att).attcacheoff >= 0 {
                off = (*att).attcacheoff;
            } else if (*att).attlen == -1 {
                /*
                 * We can only cache the offset for a varlena attribute if the
                 * offset is already suitably aligned, so that there would be
                 * no pad bytes in any case: then the offset will be valid for
                 * either an aligned or unaligned value.
                 */
                if usecache && off as usize == att_nominal_alignby(off as usize, (*att).attalignby)
                {
                    (*att).attcacheoff = off;
                } else {
                    off = att_pointer_alignby(
                        off as usize,
                        (*att).attalignby,
                        -1,
                        tp.add(off as usize),
                    ) as c_int;
                    usecache = false;
                }
            } else {
                /* not varlena, so safe to use att_nominal_alignby */
                off = att_nominal_alignby(off as usize, (*att).attalignby) as c_int;

                if usecache {
                    (*att).attcacheoff = off;
                }
            }

            if i == attnum {
                break;
            }

            off = att_addlength_pointer(off as usize, (*att).attlen as c_int, tp.add(off as usize))
                as c_int;

            if usecache && (*att).attlen <= 0 {
                usecache = false;
            }

            i += 1;
        }
    }

    fetchatt(TupleDescCompactAttr(tupleDesc, attnum), tp.add(off as usize))
}

/*
 * Convert an index tuple into Datum/isnull arrays.
 *
 * The caller must allocate sufficient storage for the output arrays.
 * (INDEX_MAX_KEYS entries should be enough.)
 *
 * This is nearly the same as heap_deform_tuple(), but for IndexTuples.
 * One difference is that the tuple should never have any missing columns.
 *
 * # Safety
 * `tup` is a valid IndexTuple; `tupleDescriptor` is live; `values`/`isnull`
 * point to at least natts elements.
 */
pub unsafe fn index_deform_tuple(
    tup: IndexTuple,
    tupleDescriptor: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) {
    /* XXX "knows" t_bits are just after fixed tuple header! */
    let bp = (tup as *mut c_char).add(size_of::<IndexTupleData>()) as *mut bits8;

    let tp = (tup as *mut c_char).add(IndexInfoFindDataOffset((*tup).t_info));

    index_deform_tuple_internal(
        tupleDescriptor,
        values,
        isnull,
        tp,
        bp,
        IndexTupleHasNulls(tup) as c_int,
    );
}

/*
 * Convert an index tuple into Datum/isnull arrays,
 * without assuming any specific layout of the index tuple header.
 *
 * Caller must supply pointer to data area, pointer to nulls bitmap
 * (which can be NULL if !hasnulls), and hasnulls flag.
 *
 * # Safety
 * `tupleDescriptor` is live; `values`/`isnull` point to at least natts elements;
 * `tp` points to the data area; `bp` is non-null if `hasnulls`.
 */
pub unsafe fn index_deform_tuple_internal(
    tupleDescriptor: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
    tp: *mut c_char,
    bp: *mut bits8,
    hasnulls: c_int,
) {
    let natts = (*tupleDescriptor).natts; /* number of atts to extract */
    let mut off: c_int = 0; /* offset in tuple data */
    let mut slow = false; /* can we use/set attcacheoff? */

    /* Assert to protect callers who allocate fixed-size arrays */
    Assert!((natts as usize) <= INDEX_MAX_KEYS);

    for attnum in 0..natts {
        let thisatt = TupleDescCompactAttr(tupleDescriptor, attnum);

        if hasnulls != 0 && att_isnull(attnum, bp) {
            *values.add(attnum as usize) = 0 as Datum;
            *isnull.add(attnum as usize) = true;
            slow = true; /* can't use attcacheoff anymore */
            continue;
        }

        *isnull.add(attnum as usize) = false;

        if !slow && (*thisatt).attcacheoff >= 0 {
            off = (*thisatt).attcacheoff;
        } else if (*thisatt).attlen == -1 {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if !slow && off as usize == att_nominal_alignby(off as usize, (*thisatt).attalignby) {
                (*thisatt).attcacheoff = off;
            } else {
                off = att_pointer_alignby(
                    off as usize,
                    (*thisatt).attalignby,
                    -1,
                    tp.add(off as usize),
                ) as c_int;
                slow = true;
            }
        } else {
            /* not varlena, so safe to use att_nominal_alignby */
            off = att_nominal_alignby(off as usize, (*thisatt).attalignby) as c_int;

            if !slow {
                (*thisatt).attcacheoff = off;
            }
        }

        *values.add(attnum as usize) = fetchatt(thisatt, tp.add(off as usize));

        off = att_addlength_pointer(off as usize, (*thisatt).attlen as c_int, tp.add(off as usize))
            as c_int;

        if (*thisatt).attlen <= 0 {
            slow = true; /* can't use attcacheoff anymore */
        }
    }
}

/*
 * Create a palloc'd copy of an index tuple.
 *
 * # Safety
 * `source` is a valid IndexTuple.
 */
pub unsafe fn CopyIndexTuple(source: IndexTuple) -> IndexTuple {
    let size = IndexTupleSize(source);
    let result = palloc(size) as IndexTuple;
    memcpy(result as *mut c_void, source as *const c_void, size);
    result
}

/*
 * Create a palloc'd copy of an index tuple, leaving only the first
 * leavenatts attributes remaining.
 *
 * Truncation is guaranteed to result in an index tuple that is no
 * larger than the original.  It is safe to use the IndexTuple with
 * the original tuple descriptor, but caller must avoid actually
 * accessing truncated attributes from returned tuple!
 *
 * It's safe to call this function with a buffer lock held, since it
 * never performs external table access.
 *
 * # Safety
 * `sourceDescriptor` is live; `source` is a valid IndexTuple matching it;
 * `leavenatts` <= natts.
 */
pub unsafe fn index_truncate_tuple(
    sourceDescriptor: TupleDesc,
    source: IndexTuple,
    leavenatts: c_int,
) -> IndexTuple {
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];

    Assert!(leavenatts <= (*sourceDescriptor).natts);

    /* Easy case: no truncation actually required */
    if leavenatts == (*sourceDescriptor).natts {
        return CopyIndexTuple(source);
    }

    /* Create temporary truncated tuple descriptor */
    let truncdesc = CreateTupleDescTruncatedCopy(sourceDescriptor, leavenatts);

    /* Deform, form copy of tuple with fewer attributes */
    index_deform_tuple(source, truncdesc, values.as_mut_ptr(), isnull.as_mut_ptr());
    let truncated = index_form_tuple(truncdesc, values.as_ptr(), isnull.as_ptr());
    (*truncated).t_tid = (*source).t_tid;
    Assert!(IndexTupleSize(truncated) <= IndexTupleSize(source));

    /*
     * Cannot leak memory here, TupleDescCopy() doesn't allocate any inner
     * structure, so, plain pfree() should clean all allocated memory
     */
    pfree(truncdesc as *mut c_void);

    truncated
}

// ============================================================================
//   Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry};
    use crate::catalog::pg_type_d::INT4OID;
    use crate::postgres::{DatumGetInt32, Int32GetDatum};

    /*
     * Build a 2-column INT4 TupleDesc ("a", "b") - both fixed-len, pass-by-value.
     * After init we explicitly populate the CompactAttributes, exactly as the
     * deform/form machinery requires (TupleDescInitBuiltinEntry already does this
     * internally, but we mirror the task's stated idiom for clarity).
     */
    unsafe fn make_td() -> TupleDesc {
        let td = CreateTemplateTupleDesc(2);
        TupleDescInitBuiltinEntry(td, 1, c"a".as_ptr(), INT4OID, -1, 0);
        TupleDescInitBuiltinEntry(td, 2, c"b".as_ptr(), INT4OID, -1, 0);
        td
    }

    #[test]
    fn form_deform_roundtrip() {
        unsafe {
            let td = make_td();

            let values: [Datum; 2] = [Int32GetDatum(42), Int32GetDatum(99)];
            let isnull: [bool; 2] = [false; 2];

            let it = index_form_tuple(td, values.as_ptr(), isnull.as_ptr());

            /* no nulls, no var-widths => those bits clear; size > 0 */
            assert!(!IndexTupleHasNulls(it));
            assert!(!IndexTupleHasVarwidths(it));
            assert!(IndexTupleSize(it) > 0);

            let mut out_values: [Datum; 2] = [0; 2];
            let mut out_isnull: [bool; 2] = [true; 2];
            index_deform_tuple(it, td, out_values.as_mut_ptr(), out_isnull.as_mut_ptr());

            assert!(!out_isnull[0]);
            assert!(!out_isnull[1]);
            assert_eq!(DatumGetInt32(out_values[0]), 42);
            assert_eq!(DatumGetInt32(out_values[1]), 99);

            /* index_getattr path returns the same values */
            let mut n = false;
            let a = index_getattr(it, 1, td, &mut n);
            assert_eq!(DatumGetInt32(a), 42);
            assert!(!n);
            let b = index_getattr(it, 2, td, &mut n);
            assert_eq!(DatumGetInt32(b), 99);
            assert!(!n);

            pfree(it as *mut c_void);
        }
    }

    #[test]
    fn null_first_roundtrip() {
        unsafe {
            let td = make_td();

            let values: [Datum; 2] = [0 as Datum, Int32GetDatum(7)];
            let isnull: [bool; 2] = [true, false];

            let it = index_form_tuple(td, values.as_ptr(), isnull.as_ptr());

            /* a null is present -> the null mask bit and the bitmap header */
            assert!(IndexTupleHasNulls(it));
            assert_eq!(
                IndexInfoFindDataOffset((*it).t_info),
                MAXALIGN(size_of::<IndexTupleData>() + size_of::<IndexAttributeBitMapData>())
            );

            let mut ov: [Datum; 2] = [0; 2];
            let mut oi: [bool; 2] = [false; 2];
            index_deform_tuple(it, td, ov.as_mut_ptr(), oi.as_mut_ptr());

            assert!(oi[0]);
            assert!(!oi[1]);
            assert_eq!(DatumGetInt32(ov[1]), 7);

            /* index_getattr reports the leading attr as null without walking */
            let mut n = false;
            let a = index_getattr(it, 1, td, &mut n);
            assert!(n);
            assert!(DatumGetPointer(a).is_null());
            let b = index_getattr(it, 2, td, &mut n);
            assert!(!n);
            assert_eq!(DatumGetInt32(b), 7);

            pfree(it as *mut c_void);
        }
    }

    #[test]
    fn copyindextuple_equal_bytes() {
        unsafe {
            let td = make_td();
            let values: [Datum; 2] = [Int32GetDatum(123), Int32GetDatum(456)];
            let isnull: [bool; 2] = [false; 2];

            let it = index_form_tuple(td, values.as_ptr(), isnull.as_ptr());
            let copy = CopyIndexTuple(it);

            let n = IndexTupleSize(it);
            assert_eq!(IndexTupleSize(copy), n);

            let a = core::slice::from_raw_parts(it as *const u8, n);
            let b = core::slice::from_raw_parts(copy as *const u8, n);
            assert_eq!(a, b);

            pfree(copy as *mut c_void);
            pfree(it as *mut c_void);
        }
    }

    #[test]
    fn truncate_to_one_attr() {
        unsafe {
            let td = make_td();
            let values: [Datum; 2] = [Int32GetDatum(5), Int32GetDatum(6)];
            let isnull: [bool; 2] = [false; 2];

            let it = index_form_tuple(td, values.as_ptr(), isnull.as_ptr());
            /* set a recognizable t_tid to confirm it is carried over */
            crate::storage::itemptr::ItemPointerSet(&mut (*it).t_tid, 0x1234, 5);

            let trunc = index_truncate_tuple(td, it, 1);

            /* truncated tuple is no larger than the original */
            assert!(IndexTupleSize(trunc) <= IndexTupleSize(it));
            /* t_tid was copied verbatim */
            assert_eq!(
                crate::storage::itemptr::ItemPointerGetBlockNumber(&(*trunc).t_tid),
                0x1234
            );
            assert_eq!(
                crate::storage::itemptr::ItemPointerGetOffsetNumber(&(*trunc).t_tid),
                5
            );

            /* leading attribute still deforms correctly under a 1-col desc */
            let onecol = CreateTupleDescTruncatedCopy(td, 1);
            let mut ov: [Datum; 1] = [0; 1];
            let mut oi: [bool; 1] = [true; 1];
            index_deform_tuple(trunc, onecol, ov.as_mut_ptr(), oi.as_mut_ptr());
            assert!(!oi[0]);
            assert_eq!(DatumGetInt32(ov[0]), 5);

            pfree(onecol as *mut c_void);
            pfree(trunc as *mut c_void);
            pfree(it as *mut c_void);
        }
    }
}
