//! Junk attribute support stuff.
//!
//! Source: postgres/src/backend/executor/execJunk.c
//! Merges declarations from:
//!   - src/include/nodes/execnodes.h  (JunkFilter struct -- already defined in
//!     crate::nodes::execnodes, re-exported here)
//!   - src/include/executor/executor.h (function prototypes)
//!
//! An attribute of a tuple living inside the executor can be either a normal
//! attribute or a "junk" attribute.  "junk" attributes never make it out of the
//! executor, i.e. they are never printed, returned or stored on disk.  Their
//! only purpose in life is to store some information useful only to the
//! executor, mainly the values of system attributes like "ctid", or sort key
//! columns that are not to be output.
//!
//! The general idea is the following: A target list consists of a list of
//! TargetEntry nodes containing expressions.  Each TargetEntry has a field
//! called 'resjunk'.  If the value of this field is true then the corresponding
//! attribute is a "junk" attribute.
//!
//! When we initialize a plan we call ExecInitJunkFilter to create a filter.
//! We then execute the plan, treating the resjunk attributes like any others.
//! Finally, when at the top level we get back a tuple, we can call
//! ExecFindJunkAttribute/ExecGetJunkAttribute to retrieve the values of the
//! junk attributes we are interested in, and ExecFilterJunk to remove all the
//! junk attributes from a tuple.  This new "clean" tuple is then printed,
//! inserted, or updated.

use crate::prelude::*;

use crate::nodes::execnodes::JunkFilter;
use crate::nodes::primnodes::{AttrNumber, TargetEntry};
use crate::nodes::pg_list::{list_head, lfirst, lnext, List, ListCell};
use crate::access::common::tupdesc::{TupleDesc, TupleDescCompactAttr};
use crate::executor::tuptable::{
    slot_getattr, slot_getallattrs, ExecClearTuple, TupleTableSlot,
};
use crate::executor::execTuples::{
    ExecSetSlotDescriptor, ExecStoreVirtualTuple, MakeSingleTupleTableSlot, TTSOpsVirtual,
};
use crate::{foreach, current_cell, makeNode};

// InvalidAttrNumber (from access/attnum.h).  Defined in several already-ported
// units; mirror the value here to avoid an ambiguous glob re-export.
pub const InvalidAttrNumber: AttrNumber = 0;

// ============================================================================
//   STUB: ExecCleanTypeFromTL
//
//   In C this lives in execTuples.c and builds a TupleDesc from the non-junk
//   TargetEntry list (it needs exprType/exprTypmod/exprCollation from nodeFuncs
//   plus TupleDescInitEntry from the syscache/typcache path).  Those helpers are
//   intentionally not yet ported in crate::executor::execTuples (see the note at
//   the bottom of that file), so we stub just the tupdesc-from-tlist builder.
//
//   TODO(pg-port): replace with the real ExecCleanTypeFromTL once nodeFuncs
//   exprType*/TupleDescInitEntry land; then drop this stub and import it from
//   crate::executor::execTuples.
// ============================================================================
unsafe fn ExecCleanTypeFromTL(_targetList: *mut List) -> TupleDesc {
    crate::executor::execTuples::ExecCleanTypeFromTL(_targetList as _) as _
}

/// ExecInitJunkFilter
///
/// Initialize the Junk filter.
///
/// The source targetlist is passed in.  The output tuple descriptor is built
/// from the non-junk tlist entries.  An optional resultSlot can be passed as
/// well; otherwise, we create one.
pub unsafe fn ExecInitJunkFilter(
    targetList: *mut List,
    mut slot: *mut TupleTableSlot,
) -> *mut JunkFilter {
    let junkfilter: *mut JunkFilter;
    let cleanTupType: TupleDesc;
    let cleanLength: c_int;
    let cleanMap: *mut AttrNumber;

    /*
     * Compute the tuple descriptor for the cleaned tuple.
     */
    cleanTupType = ExecCleanTypeFromTL(targetList);

    /*
     * Use the given slot, or make a new slot if we weren't given one.
     */
    if !slot.is_null() {
        ExecSetSlotDescriptor(slot, cleanTupType);
    } else {
        slot = MakeSingleTupleTableSlot(cleanTupType, &TTSOpsVirtual);
    }

    /*
     * Now calculate the mapping between the original tuple's attributes and the
     * "clean" tuple's attributes.
     *
     * The "map" is an array of "cleanLength" attribute numbers, i.e. one entry
     * for every attribute of the "clean" tuple.  The value of this entry is the
     * attribute number of the corresponding attribute of the "original" tuple.
     * (Zero indicates a NULL output attribute, but we do not use that feature in
     * this routine.)
     */
    cleanLength = (*cleanTupType).natts;
    if cleanLength > 0 {
        let mut cleanResno: AttrNumber;

        cleanMap = palloc(cleanLength as usize * core::mem::size_of::<AttrNumber>())
            as *mut AttrNumber;
        cleanResno = 0;
        foreach!(t, targetList, {
            let tle = lfirst(current_cell!(t)) as *mut TargetEntry;

            if !(*tle).resjunk {
                *cleanMap.add(cleanResno as usize) = (*tle).resno;
                cleanResno += 1;
            }
        });
        Assert!(cleanResno as c_int == cleanLength);
    } else {
        cleanMap = null_mut();
    }

    /*
     * Finally create and initialize the JunkFilter struct.
     */
    junkfilter = makeNode!(JunkFilter, T_JunkFilter);

    (*junkfilter).jf_targetList = targetList;
    (*junkfilter).jf_cleanTupType = cleanTupType;
    (*junkfilter).jf_cleanMap = cleanMap;
    (*junkfilter).jf_resultSlot = slot;

    junkfilter
}

/// ExecInitJunkFilterConversion
///
/// Initialize a JunkFilter for rowtype conversions.
///
/// Here, we are given the target "clean" tuple descriptor rather than inferring
/// it from the targetlist.  The target descriptor can contain deleted columns.
/// It is assumed that the caller has checked that the non-deleted columns match
/// up with the non-junk columns of the targetlist.
pub unsafe fn ExecInitJunkFilterConversion(
    targetList: *mut List,
    cleanTupType: TupleDesc,
    mut slot: *mut TupleTableSlot,
) -> *mut JunkFilter {
    let junkfilter: *mut JunkFilter;
    let cleanLength: c_int;
    let cleanMap: *mut AttrNumber;
    let mut t: *mut ListCell;
    let mut i: c_int;

    /*
     * Use the given slot, or make a new slot if we weren't given one.
     */
    if !slot.is_null() {
        ExecSetSlotDescriptor(slot, cleanTupType);
    } else {
        slot = MakeSingleTupleTableSlot(cleanTupType, &TTSOpsVirtual);
    }

    /*
     * Calculate the mapping between the original tuple's attributes and the
     * "clean" tuple's attributes.
     *
     * The "map" is an array of "cleanLength" attribute numbers, i.e. one entry
     * for every attribute of the "clean" tuple.  The value of this entry is the
     * attribute number of the corresponding attribute of the "original" tuple.
     * We store zero for any deleted attributes, marking that a NULL is needed in
     * the output tuple.
     */
    cleanLength = (*cleanTupType).natts;
    if cleanLength > 0 {
        cleanMap = palloc0(cleanLength as usize * core::mem::size_of::<AttrNumber>())
            as *mut AttrNumber;
        t = list_head(targetList);
        i = 0;
        while i < cleanLength {
            if (*TupleDescCompactAttr(cleanTupType, i)).attisdropped {
                i += 1;
                continue; /* map entry is already zero */
            }
            loop {
                let tle = lfirst(t) as *mut TargetEntry;

                t = lnext(targetList, t);
                if !(*tle).resjunk {
                    *cleanMap.add(i as usize) = (*tle).resno;
                    break;
                }
            }
            i += 1;
        }
    } else {
        cleanMap = null_mut();
    }

    /*
     * Finally create and initialize the JunkFilter struct.
     */
    junkfilter = makeNode!(JunkFilter, T_JunkFilter);

    (*junkfilter).jf_targetList = targetList;
    (*junkfilter).jf_cleanTupType = cleanTupType;
    (*junkfilter).jf_cleanMap = cleanMap;
    (*junkfilter).jf_resultSlot = slot;

    junkfilter
}

/// ExecFindJunkAttribute
///
/// Locate the specified junk attribute in the junk filter's targetlist, and
/// return its resno.  Returns InvalidAttrNumber if not found.
pub unsafe fn ExecFindJunkAttribute(
    junkfilter: *mut JunkFilter,
    attrName: *const c_char,
) -> AttrNumber {
    ExecFindJunkAttributeInTlist((*junkfilter).jf_targetList, attrName)
}

/// ExecFindJunkAttributeInTlist
///
/// Find a junk attribute given a subplan's targetlist (not necessarily part of a
/// JunkFilter).
pub unsafe fn ExecFindJunkAttributeInTlist(
    targetlist: *mut List,
    attrName: *const c_char,
) -> AttrNumber {
    foreach!(t, targetlist, {
        let tle = lfirst(current_cell!(t)) as *mut TargetEntry;

        if (*tle).resjunk
            && !(*tle).resname.is_null()
            && libc_strcmp((*tle).resname, attrName) == 0
        {
            /* We found it ! */
            return (*tle).resno;
        }
    });

    InvalidAttrNumber
}

/// ExecGetJunkAttribute
///
/// Given a junk attribute number and a tuple, extract the value and isnull flag
/// of the attribute.
///
/// (C `ExecGetJunkAttribute` is a static inline in executor.h.)
#[inline]
pub unsafe fn ExecGetJunkAttribute(
    slot: *mut TupleTableSlot,
    attno: AttrNumber,
    isNull: *mut bool,
) -> Datum {
    Assert!(attno > 0);
    slot_getattr(slot, attno as c_int, isNull)
}

/// ExecFilterJunk
///
/// Construct and return a slot with all the junk attributes removed.
pub unsafe fn ExecFilterJunk(
    junkfilter: *mut JunkFilter,
    slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let resultSlot: *mut TupleTableSlot;
    let cleanMap: *mut AttrNumber;
    let cleanTupType: TupleDesc;
    let cleanLength: c_int;
    let mut i: c_int;
    let values: *mut Datum;
    let isnull: *mut bool;
    let old_values: *mut Datum;
    let old_isnull: *mut bool;

    /*
     * Extract all the values of the old tuple.
     */
    slot_getallattrs(slot);
    old_values = (*slot).tts_values;
    old_isnull = (*slot).tts_isnull;

    /*
     * get info from the junk filter
     */
    cleanTupType = (*junkfilter).jf_cleanTupType;
    cleanLength = (*cleanTupType).natts;
    cleanMap = (*junkfilter).jf_cleanMap;
    resultSlot = (*junkfilter).jf_resultSlot;

    /*
     * Prepare to build a virtual result tuple.
     */
    ExecClearTuple(resultSlot);
    values = (*resultSlot).tts_values;
    isnull = (*resultSlot).tts_isnull;

    /*
     * Transpose data into proper fields of the new tuple.
     */
    i = 0;
    while i < cleanLength {
        let j: c_int = *cleanMap.add(i as usize) as c_int;

        if j == 0 {
            *values.add(i as usize) = 0 as Datum;
            *isnull.add(i as usize) = true;
        } else {
            *values.add(i as usize) = *old_values.add((j - 1) as usize);
            *isnull.add(i as usize) = *old_isnull.add((j - 1) as usize);
        }
        i += 1;
    }

    /*
     * And return the virtual tuple.
     */
    ExecStoreVirtualTuple(resultSlot)
}

// Minimal strcmp over NUL-terminated C strings (the C source uses libc strcmp on
// tle->resname, a `*mut c_char`, vs the caller-supplied attrName).  Returns 0
// when equal, like C strcmp's equal case.
#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut p = a;
    let mut q = b;
    loop {
        let ca = *p as u8;
        let cb = *q as u8;
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        p = p.add(1);
        q = q.add(1);
    }
}

// ============================================================================
//   Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry};
    use crate::catalog::pg_type_d::INT4OID;
    use crate::nodes::makefuncs::makeTargetEntry;
    use crate::nodes::pg_list::lappend;
    use crate::postgres::{DatumGetInt32, Int32GetDatum};

    // Build a TargetEntry via the canonical constructor (sets the node tag
    // correctly through xpr.type).  expr is left null -- the junk filter never
    // evaluates it; resname is a NUL-terminated C string pointer or null.
    unsafe fn make_tle(
        resno: AttrNumber,
        resname: *const c_char,
        resjunk: bool,
    ) -> *mut TargetEntry {
        makeTargetEntry(null_mut(), resno, resname as *mut c_char, resjunk)
    }

    // ExecFindJunkAttributeInTlist finds a named junk TargetEntry and skips
    // non-junk / wrongly-named ones.
    #[test]
    fn find_junk_attribute_in_tlist() {
        unsafe {
            // tlist: (1 "a" non-junk), (2 "ctid" junk), (3 "a" junk)
            let mut tlist: *mut List = null_mut();
            tlist = lappend(tlist, make_tle(1, c"a".as_ptr(), false) as *mut c_void);
            tlist = lappend(tlist, make_tle(2, c"ctid".as_ptr(), true) as *mut c_void);
            tlist = lappend(tlist, make_tle(3, c"a".as_ptr(), true) as *mut c_void);

            // "ctid" is junk -> found at resno 2.
            assert_eq!(ExecFindJunkAttributeInTlist(tlist, c"ctid".as_ptr()), 2);
            // "a" matches the JUNK entry (resno 3), not the non-junk one (resno 1).
            assert_eq!(ExecFindJunkAttributeInTlist(tlist, c"a".as_ptr()), 3);
            // missing name -> InvalidAttrNumber.
            assert_eq!(
                ExecFindJunkAttributeInTlist(tlist, c"nope".as_ptr()),
                InvalidAttrNumber
            );
        }
    }

    // ExecFilterJunk maps clean columns through jf_cleanMap, transposing values
    // from the source slot into the result slot.
    #[test]
    fn filter_junk_maps_clean_columns() {
        unsafe {
            // Source tuple has 3 attrs: a=10 (resno1), ctid=junk (resno2),
            // b=30 (resno3).  Clean tuple keeps a,b => cleanMap = [1, 3].
            let src_desc = CreateTemplateTupleDesc(3);
            TupleDescInitBuiltinEntry(src_desc, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(src_desc, 2, c"ctid".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(src_desc, 3, c"b".as_ptr(), INT4OID, -1, 0);
            let src = MakeSingleTupleTableSlot(src_desc, &TTSOpsVirtual);

            // Fill source slot as a virtual tuple [10, 20, 30].
            ExecClearTuple(src);
            *(*src).tts_values.add(0) = Int32GetDatum(10);
            *(*src).tts_values.add(1) = Int32GetDatum(20);
            *(*src).tts_values.add(2) = Int32GetDatum(30);
            *(*src).tts_isnull.add(0) = false;
            *(*src).tts_isnull.add(1) = false;
            *(*src).tts_isnull.add(2) = false;
            ExecStoreVirtualTuple(src);

            // Build a clean (2-col) desc + slot and a hand-built JunkFilter.
            let clean_desc = CreateTemplateTupleDesc(2);
            TupleDescInitBuiltinEntry(clean_desc, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(clean_desc, 2, c"b".as_ptr(), INT4OID, -1, 0);
            let result_slot = MakeSingleTupleTableSlot(clean_desc, &TTSOpsVirtual);

            let cleanMap = palloc(2 * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
            *cleanMap.add(0) = 1; // clean col 0 <- src attr 1 (a)
            *cleanMap.add(1) = 3; // clean col 1 <- src attr 3 (b)

            let jf = makeNode!(JunkFilter, T_JunkFilter);
            (*jf).jf_targetList = null_mut();
            (*jf).jf_cleanTupType = clean_desc;
            (*jf).jf_cleanMap = cleanMap;
            (*jf).jf_resultSlot = result_slot;

            let out = ExecFilterJunk(jf, src);
            slot_getallattrs(out);
            assert_eq!(DatumGetInt32(*(*out).tts_values.add(0)), 10);
            assert_eq!(DatumGetInt32(*(*out).tts_values.add(1)), 30);
            assert!(!*(*out).tts_isnull.add(0));
            assert!(!*(*out).tts_isnull.add(1));
        }
    }

    // A zero cleanMap entry yields a NULL output column.
    #[test]
    fn filter_junk_zero_map_yields_null() {
        unsafe {
            let src_desc = CreateTemplateTupleDesc(1);
            TupleDescInitBuiltinEntry(src_desc, 1, c"a".as_ptr(), INT4OID, -1, 0);
            let src = MakeSingleTupleTableSlot(src_desc, &TTSOpsVirtual);
            ExecClearTuple(src);
            *(*src).tts_values.add(0) = Int32GetDatum(99);
            *(*src).tts_isnull.add(0) = false;
            ExecStoreVirtualTuple(src);

            let clean_desc = CreateTemplateTupleDesc(2);
            TupleDescInitBuiltinEntry(clean_desc, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(clean_desc, 2, c"dropped".as_ptr(), INT4OID, -1, 0);
            let result_slot = MakeSingleTupleTableSlot(clean_desc, &TTSOpsVirtual);

            let cleanMap = palloc0(2 * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
            *cleanMap.add(0) = 1; // a
            *cleanMap.add(1) = 0; // NULL output

            let jf = makeNode!(JunkFilter, T_JunkFilter);
            (*jf).jf_targetList = null_mut();
            (*jf).jf_cleanTupType = clean_desc;
            (*jf).jf_cleanMap = cleanMap;
            (*jf).jf_resultSlot = result_slot;

            let out = ExecFilterJunk(jf, src);
            slot_getallattrs(out);
            assert_eq!(DatumGetInt32(*(*out).tts_values.add(0)), 99);
            assert!(!*(*out).tts_isnull.add(0));
            assert!(*(*out).tts_isnull.add(1));
        }
    }
}
