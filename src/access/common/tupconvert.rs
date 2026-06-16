//! Translation of postgres/src/backend/access/common/tupconvert.c
//! (merged with postgres/src/include/access/tupconvert.h).
//!
//! Tuple conversion support.
//!
//! These functions provide conversion between rowtypes that are logically
//! equivalent but might have columns in a different order or different sets of
//! dropped columns.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping (tupconvert.c and tupconvert.h):
//!   postgres.h            -> crate::prelude (Datum, c-types, palloc/pfree,
//!                            elog!/ereport!/errmsg!/Assert!, null_mut, ...)
//!   access/tupconvert.h   -> this file (the header is merged in below)
//!   access/attmap.h       -> crate::access::common::attmap (AttrMap,
//!                            build_attrmap_by_position, build_attrmap_by_name,
//!                            build_attrmap_by_name_if_req, free_attrmap)
//!   access/htup.h         -> HeapTuple from crate::access::htup_details
//!   access/tupdesc.h      -> crate::access::common::tupdesc (TupleDesc,
//!                            TupleDescData.natts)
//!   access/heaptuple (heaptuple.c) -> crate::access::common::heaptuple
//!                            (heap_deform_tuple, heap_form_tuple)
//!   executor/tuptable.h   -> STUB.  TupleTableSlot is the opaque stub in
//!                            crate::nodes::execnodes; slot_getallattrs /
//!                            ExecClearTuple / ExecStoreVirtualTuple are NOT
//!                            ported, so execute_attr_map_slot is stubbed.
//!   nodes/bitmapset.h     -> crate::nodes::bitmapset (Bitmapset, bms_is_member,
//!                            bms_add_member) + FirstLowInvalidHeapAttributeNumber
//!                            from crate::access::sysattr.
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: convert_tuples_by_position, convert_tuples_by_name,
//!     convert_tuples_by_name_attrmap, execute_attr_map_tuple,
//!     execute_attr_map_cols, free_conversion_map.
//!   STUBBED: execute_attr_map_slot (needs executor/tuptable.h: TupleTableSlot
//!     accessors slot_getallattrs/ExecClearTuple/ExecStoreVirtualTuple, none of
//!     which are ported).  Signature preserved; the C body is kept as a comment.

use crate::prelude::*;

use crate::access::common::attmap::{
    build_attrmap_by_name, build_attrmap_by_name_if_req, build_attrmap_by_position, free_attrmap,
    AttrMap,
};
use crate::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::nodes::bitmapset::{bms_add_member, bms_is_member, Bitmapset};
use crate::nodes::execnodes::TupleTableSlot;
use crate::executor::tuptable::{slot_getallattrs, ExecClearTuple};
use crate::executor::execTuples::ExecStoreVirtualTuple;

use core::mem::size_of;

// ============================================================================
//   tupconvert.h
// ============================================================================

/*
 * The key component of a TupleConversionMap is an attrMap[] array with
 * one entry per output column.  This entry contains the 1-based index of
 * the corresponding input column, or zero to force a NULL value (for
 * a dropped output column).  The TupleConversionMap also contains workspace
 * arrays.
 */
#[repr(C)]
pub struct TupleConversionMap {
    pub indesc: TupleDesc,       /* tupdesc for source rowtype */
    pub outdesc: TupleDesc,      /* tupdesc for result rowtype */
    pub attrMap: *mut AttrMap,   /* indexes of input fields, or 0 for null */
    pub invalues: *mut Datum,    /* workspace for deconstructing source */
    pub inisnull: *mut bool,
    pub outvalues: *mut Datum,   /* workspace for constructing result */
    pub outisnull: *mut bool,
}

// ============================================================================
//   tupconvert.c
// ============================================================================

/*
 * Set up for tuple conversion, matching input and output columns by
 * position.  (Dropped columns are ignored in both input and output.)
 *
 * # Safety
 * `indesc` and `outdesc` must be live TupleDescs that outlive the returned map.
 */
pub unsafe fn convert_tuples_by_position(
    indesc: TupleDesc,
    outdesc: TupleDesc,
    msg: &str,
) -> *mut TupleConversionMap {
    let map: *mut TupleConversionMap;
    let mut n: c_int;
    let attrMap: *mut AttrMap;

    /* Verify compatibility and prepare attribute-number map */
    attrMap = build_attrmap_by_position(indesc, outdesc, msg);

    if attrMap.is_null() {
        /* runtime conversion is not needed */
        return null_mut();
    }

    /* Prepare the map structure */
    map = palloc(size_of::<TupleConversionMap>()) as *mut TupleConversionMap;
    (*map).indesc = indesc;
    (*map).outdesc = outdesc;
    (*map).attrMap = attrMap;
    /* preallocate workspace for Datum arrays */
    n = (*outdesc).natts + 1; /* +1 for NULL */
    (*map).outvalues = palloc(n as usize * size_of::<Datum>()) as *mut Datum;
    (*map).outisnull = palloc(n as usize * size_of::<bool>()) as *mut bool;
    n = (*indesc).natts + 1; /* +1 for NULL */
    (*map).invalues = palloc(n as usize * size_of::<Datum>()) as *mut Datum;
    (*map).inisnull = palloc(n as usize * size_of::<bool>()) as *mut bool;
    *(*map).invalues.add(0) = 0 as Datum; /* set up the NULL entry */
    *(*map).inisnull.add(0) = true;

    map
}

/*
 * Set up for tuple conversion, matching input and output columns by name.
 * (Dropped columns are ignored in both input and output.)  This is intended
 * for use when the rowtypes are related by inheritance, so we expect an exact
 * match of both type and typmod.  The error messages will be a bit unhelpful
 * unless both rowtypes are named composite types.
 *
 * # Safety
 * `indesc` and `outdesc` must be live TupleDescs that outlive the returned map.
 */
pub unsafe fn convert_tuples_by_name(
    indesc: TupleDesc,
    outdesc: TupleDesc,
) -> *mut TupleConversionMap {
    let attrMap: *mut AttrMap;

    /* Verify compatibility and prepare attribute-number map */
    attrMap = build_attrmap_by_name_if_req(indesc, outdesc, false);

    if attrMap.is_null() {
        /* runtime conversion is not needed */
        return null_mut();
    }

    convert_tuples_by_name_attrmap(indesc, outdesc, attrMap)
}

/*
 * Set up tuple conversion for input and output TupleDescs using the given
 * AttrMap.
 *
 * # Safety
 * `indesc`/`outdesc` must be live TupleDescs; `attrMap` a live AttrMap.  All
 * must outlive the returned map (which takes ownership of `attrMap`).
 */
pub unsafe fn convert_tuples_by_name_attrmap(
    indesc: TupleDesc,
    outdesc: TupleDesc,
    attrMap: *mut AttrMap,
) -> *mut TupleConversionMap {
    let mut n: c_int = (*outdesc).natts;
    let map: *mut TupleConversionMap;

    Assert!(!attrMap.is_null());

    /* Prepare the map structure */
    map = palloc(size_of::<TupleConversionMap>()) as *mut TupleConversionMap;
    (*map).indesc = indesc;
    (*map).outdesc = outdesc;
    (*map).attrMap = attrMap;
    /* preallocate workspace for Datum arrays */
    (*map).outvalues = palloc(n as usize * size_of::<Datum>()) as *mut Datum;
    (*map).outisnull = palloc(n as usize * size_of::<bool>()) as *mut bool;
    n = (*indesc).natts + 1; /* +1 for NULL */
    (*map).invalues = palloc(n as usize * size_of::<Datum>()) as *mut Datum;
    (*map).inisnull = palloc(n as usize * size_of::<bool>()) as *mut bool;
    *(*map).invalues.add(0) = 0 as Datum; /* set up the NULL entry */
    *(*map).inisnull.add(0) = true;

    map
}

/*
 * Perform conversion of a tuple according to the map.
 *
 * # Safety
 * `tuple` must be a live HeapTuple matching `map->indesc`; `map` a live map.
 */
pub unsafe fn execute_attr_map_tuple(
    tuple: HeapTuple,
    map: *mut TupleConversionMap,
) -> HeapTuple {
    let attrMap: *mut AttrMap = (*map).attrMap;
    let invalues: *mut Datum = (*map).invalues;
    let inisnull: *mut bool = (*map).inisnull;
    let outvalues: *mut Datum = (*map).outvalues;
    let outisnull: *mut bool = (*map).outisnull;

    /*
     * Extract all the values of the old tuple, offsetting the arrays so that
     * invalues[0] is left NULL and invalues[1] is the first source attribute;
     * this exactly matches the numbering convention in attrMap.
     */
    heap_deform_tuple(tuple, (*map).indesc, invalues.add(1), inisnull.add(1));

    /*
     * Transpose into proper fields of the new tuple.
     */
    Assert!((*attrMap).maplen == (*(*map).outdesc).natts);
    let mut i: c_int = 0;
    while i < (*attrMap).maplen {
        let j: c_int = *(*attrMap).attnums.add(i as usize) as c_int;

        *outvalues.add(i as usize) = *invalues.add(j as usize);
        *outisnull.add(i as usize) = *inisnull.add(j as usize);
        i += 1;
    }

    /*
     * Now form the new tuple.
     */
    heap_form_tuple((*map).outdesc, outvalues, outisnull)
}

/*
 * Perform conversion of a tuple slot according to the map.
 *
 * STUB: needs executor/tuptable.h (slot_getallattrs, ExecClearTuple,
 * ExecStoreVirtualTuple, and the TupleTableSlot tts_* fields), none of which
 * are ported yet.  TupleTableSlot is the opaque stub in nodes::execnodes.
 *
 * # Safety
 * `in_slot`/`out_slot` must be live TupleTableSlots; `attrMap` a live AttrMap.
 */
pub unsafe fn execute_attr_map_slot(
    attrMap: *mut AttrMap,
    in_slot: *mut TupleTableSlot,
    out_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let invalues: *mut Datum;
    let inisnull: *mut bool;
    let outvalues: *mut Datum;
    let outisnull: *mut bool;
    let outnatts: c_int;

    /* Sanity checks */
    Assert!(
        !(*in_slot).tts_tupleDescriptor.is_null()
            && !(*out_slot).tts_tupleDescriptor.is_null()
    );
    Assert!(!(*in_slot).tts_values.is_null() && !(*out_slot).tts_values.is_null());

    outnatts = (*(*out_slot).tts_tupleDescriptor).natts;

    /* Extract all the values of the in slot. */
    slot_getallattrs(in_slot);

    /* Before doing the mapping, clear any old contents from the out slot */
    ExecClearTuple(out_slot);

    invalues = (*in_slot).tts_values;
    inisnull = (*in_slot).tts_isnull;
    outvalues = (*out_slot).tts_values;
    outisnull = (*out_slot).tts_isnull;

    /* Transpose into proper fields of the out slot. */
    for i in 0..outnatts as usize {
        let j: c_int = *(*attrMap).attnums.add(i) as c_int - 1;

        /* attrMap->attnums[i] == 0 means it's a NULL datum. */
        if j == -1 {
            *outvalues.add(i) = 0 as Datum;
            *outisnull.add(i) = true;
        } else {
            *outvalues.add(i) = *invalues.add(j as usize);
            *outisnull.add(i) = *inisnull.add(j as usize);
        }
    }

    ExecStoreVirtualTuple(out_slot);

    out_slot
}

/*
 * Perform conversion of bitmap of columns according to the map.
 *
 * The input and output bitmaps are offset by
 * FirstLowInvalidHeapAttributeNumber to accommodate system cols, like the
 * column-bitmaps in RangeTblEntry.
 *
 * # Safety
 * `attrMap` must be a live AttrMap; `in_cols` a valid Bitmapset or null.
 */
pub unsafe fn execute_attr_map_cols(
    attrMap: *mut AttrMap,
    in_cols: *mut Bitmapset,
) -> *mut Bitmapset {
    let mut out_cols: *mut Bitmapset;
    let mut out_attnum: c_int;

    /* fast path for the common trivial case */
    if in_cols.is_null() {
        return null_mut();
    }

    /*
     * For each output column, check which input column it corresponds to.
     */
    out_cols = null_mut();

    out_attnum = FirstLowInvalidHeapAttributeNumber as c_int;
    while out_attnum <= (*attrMap).maplen {
        let in_attnum: c_int;

        if out_attnum < 0 {
            /* System column. No mapping. */
            in_attnum = out_attnum;
        } else if out_attnum == 0 {
            out_attnum += 1;
            continue;
        } else {
            /* normal user column */
            in_attnum = *(*attrMap).attnums.add((out_attnum - 1) as usize) as c_int;

            if in_attnum == 0 {
                out_attnum += 1;
                continue;
            }
        }

        if bms_is_member(in_attnum - FirstLowInvalidHeapAttributeNumber as c_int, in_cols) {
            out_cols = bms_add_member(
                out_cols,
                out_attnum - FirstLowInvalidHeapAttributeNumber as c_int,
            );
        }
        out_attnum += 1;
    }

    out_cols
}

/*
 * Free a TupleConversionMap structure.
 *
 * # Safety
 * `map` must be a live map previously produced by one of the setup routines.
 */
pub unsafe fn free_conversion_map(map: *mut TupleConversionMap) {
    /* indesc and outdesc are not ours to free */
    free_attrmap((*map).attrMap);
    pfree((*map).invalues as *mut c_void);
    pfree((*map).inisnull as *mut c_void);
    pfree((*map).outvalues as *mut c_void);
    pfree((*map).outisnull as *mut c_void);
    pfree(map as *mut c_void);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::{
        populate_compact_attribute, CreateTemplateTupleDesc, TupleDescInitBuiltinEntry,
    };
    use crate::catalog::pg_type_d::INT4OID;

    // Build a 2-column INT4 TupleDesc with the given column names.
    unsafe fn make_desc(name1: &str, name2: &str) -> TupleDesc {
        let desc = CreateTemplateTupleDesc(2);
        let c1 = std::ffi::CString::new(name1).unwrap();
        let c2 = std::ffi::CString::new(name2).unwrap();
        TupleDescInitBuiltinEntry(desc, 1, c1.as_ptr(), INT4OID, -1, 0);
        TupleDescInitBuiltinEntry(desc, 2, c2.as_ptr(), INT4OID, -1, 0);
        // TupleDescInitBuiltinEntry already populates the compact attribute, but
        // call it again explicitly to mirror the documented build sequence.
        populate_compact_attribute(desc, 0);
        populate_compact_attribute(desc, 1);
        desc
    }

    #[test]
    fn by_name_swaps_columns() {
        unsafe {
            // indesc columns ("a","b"); outdesc columns ("b","a").  The by-name
            // map is [2,1]: out col 0 ("b") <- in attno 2, out col 1 ("a") <- in
            // attno 1.  Forming a row [11,22] under indesc and converting must
            // swap the columns to [22,11] under outdesc.
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("b", "a");

            let map = convert_tuples_by_name(indesc, outdesc);
            assert!(!map.is_null(), "a swap requires a non-NULL conversion map");

            // Form the source row [11, 22] in the in-desc.
            let invals: [Datum; 2] = [11 as Datum, 22 as Datum];
            let innulls: [bool; 2] = [false, false];
            let intup = heap_form_tuple(indesc, invals.as_ptr(), innulls.as_ptr());

            // Convert.
            let outtup = execute_attr_map_tuple(intup, map);

            // Deform the result against the out-desc and assert the swap.
            let mut outvals: [Datum; 2] = [0 as Datum; 2];
            let mut outnulls: [bool; 2] = [false; 2];
            heap_deform_tuple(outtup, outdesc, outvals.as_mut_ptr(), outnulls.as_mut_ptr());

            assert!(!outnulls[0] && !outnulls[1]);
            // out col 0 == "b" == in[22]; out col 1 == "a" == in[11].
            assert_eq!(outvals[0], 22 as Datum);
            assert_eq!(outvals[1], 11 as Datum);

            free_conversion_map(map);
        }
    }

    #[test]
    fn by_position_identity_returns_null() {
        unsafe {
            // Two identical descs are physically compatible; like the C, the
            // position setup returns NULL (no runtime conversion needed).
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("a", "b");

            let map = convert_tuples_by_position(indesc, outdesc, "conversion check");
            assert!(map.is_null());
        }
    }

    #[test]
    fn by_name_identity_returns_null() {
        unsafe {
            // Same names in the same order: by-name setup also detects the
            // one-to-one match and returns NULL.
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("a", "b");

            let map = convert_tuples_by_name(indesc, outdesc);
            assert!(map.is_null());
        }
    }

    #[test]
    fn execute_attr_map_cols_remaps_user_columns() {
        unsafe {
            // build_attrmap_by_name on ("a","b") -> ("b","a") yields the swap map
            // [2,1].  execute_attr_map_cols must remap the user-column bitmap
            // accordingly.  Bitmaps are offset by FirstLowInvalidHeapAttributeNumber.
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("b", "a");
            let attrmap = build_attrmap_by_name(indesc, outdesc, false);
            assert!(!attrmap.is_null());

            let off = FirstLowInvalidHeapAttributeNumber as c_int;

            // in_cols holds only user column 1 (attno 1, i.e. "a").
            let in_cols = bms_add_member(null_mut(), 1 - off);

            let out_cols = execute_attr_map_cols(attrmap, in_cols);
            assert!(!out_cols.is_null());

            // out col with attnums[i]==1 is output attno 2 ("a" position in
            // outdesc).  So out_cols should contain output attno 2.
            assert!(bms_is_member(2 - off, out_cols));
            assert!(!bms_is_member(1 - off, out_cols));

            free_attrmap(attrmap);
        }
    }

    #[test]
    fn execute_attr_map_cols_null_is_null() {
        unsafe {
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("b", "a");
            let attrmap = build_attrmap_by_name(indesc, outdesc, false);
            // Trivial fast path: NULL in -> NULL out.
            let out_cols = execute_attr_map_cols(attrmap, null_mut());
            assert!(out_cols.is_null());
            free_attrmap(attrmap);
        }
    }
}
