//! nbtpreprocesskeys.rs
//!   Preprocessing for Postgres btree scan keys.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtpreprocesskeys.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtpreprocesskeys.c
//!
//! #include mapping:
//!   "postgres.h"           -> crate::prelude::*
//!   "access/nbtree.h"      -> BTScanOpaque/BTArrayKeyInfo/BTSkipArraySupport (re-used from nbtutils)
//!   "common/int.h"         -> pg_cmp_s32 (stub below)
//!   "lib/qunique.h"        -> qunique_arg (stub below)
//!   "utils/array.h"        -> ArrayType/DatumGetArrayTypeP/ARR_ELEMTYPE/
//!                             get_typlenbyvalalign/deconstruct_array (stubs below)
//!   "utils/lsyscache.h"    -> get_opfamily_member/get_opcode (stubs below)
//!   "utils/memutils.h"     -> AllocSetContextCreate/MemoryContextSwitchTo/
//!                             MemoryContextReset/MemoryContextAlloc (from prelude)

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_assignments)]
#![allow(unused_labels)]
#![allow(unexpected_cfgs)]
#![allow(improper_ctypes)]

use crate::prelude::*;
use crate::access::nbtree::nbtree::{BTScanOpaqueData, BTScanOpaque};

use std::ffi::{c_char, c_int, c_void};
use std::mem::size_of;

use crate::c::{int16, int32, uint16, uint32, Size};

// ---------------------------------------------------------------------------
// Real, already-ported homes.
// ---------------------------------------------------------------------------
use crate::access::common::scankey::{
    ScanKey, ScanKeyData,
    SK_ISNULL, SK_SEARCHNULL, SK_SEARCHNOTNULL, SK_SEARCHARRAY,
    SK_ROW_HEADER, SK_ROW_MEMBER, SK_ROW_END,
};
use crate::access::stratnum::{
    InvalidStrategy, StrategyNumber,
    BTEqualStrategyNumber, BTLessStrategyNumber, BTLessEqualStrategyNumber,
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTMaxStrategyNumber,
};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::utils::rel::Relation;
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll};
use crate::postgres::{DatumGetBool, DatumGetInt32, DatumGetPointer, PointerGetDatum};

// ---------------------------------------------------------------------------
// Re-use types already defined in nbtutils.rs (same module).
// ---------------------------------------------------------------------------
use super::nbtutils::{
    BTArrayKeyInfo, BTSkipArraySupport,
    IndexScanDescData, IndexScanDesc,
    ScanDirection, NoMovementScanDirection,
    SK_BT_REQFWD, SK_BT_REQBKWD, SK_BT_DESC, SK_BT_NULLS_FIRST,
    SK_BT_SKIP, SK_BT_INDOPTION_SHIFT,
    BTORDER_PROC,
    INDOPTION_DESC, INDOPTION_NULLS_FIRST,
    CompactAttribute,
    TupleDescCompactAttr,
    _bt_binsrch_array_skey,
};
use crate::access::common::tupdesc::TupleDesc;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// File-local types (C: typedef struct ... { } Name).
// ---------------------------------------------------------------------------

/// C: BTScanKeyPreproc
#[repr(C)]
#[derive(Clone, Copy)]
struct BTScanKeyPreproc {
    inkey:    ScanKey,
    inkeyi:   c_int,
    arrayidx: c_int,
}

/// C: BTSortArrayContext
#[repr(C)]
struct BTSortArrayContext {
    sortproc:  *mut FmgrInfo,
    collation: Oid,
    r#reverse: bool,
}

// ---------------------------------------------------------------------------
// Stubs for symbols not yet ported.
// ---------------------------------------------------------------------------

/// TODO(pg-port): Oid type alias (already in prelude as usize; restate locally for clarity).
// (Oid comes from prelude via postgres.h.)

/// TODO(pg-port): RegProcedure = Oid (pg_proc.h).
type RegProcedure = Oid;

/// TODO(pg-port): ArrayType from utils/array.h.
#[repr(C)]
pub struct ArrayType {
    pub vl_len_: int32,
    pub ndim: c_int,
    pub dataoffset: int32,
    pub elemtype: Oid,
}

/// TODO(pg-port): BTCommuteStrategyNumber from access/stratnum.h.
#[inline]
pub unsafe fn BTCommuteStrategyNumber(strat: StrategyNumber) -> StrategyNumber {
    (BTMaxStrategyNumber + 1) - strat
}

extern "C" {
    /// TODO(pg-port): get_opfamily_member from utils/lsyscache.h.
    fn get_opfamily_member(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: StrategyNumber) -> Oid;

    /// TODO(pg-port): get_opcode from utils/lsyscache.h.
    fn get_opcode(opid: Oid) -> RegProcedure;

    /// TODO(pg-port): get_opfamily_proc from utils/lsyscache.h.
    fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: c_int) -> RegProcedure;

    /// TODO(pg-port): fmgr_info from utils/fmgr.h.
    fn fmgr_info(procedureId: Oid, finfo: *mut FmgrInfo);

    /// TODO(pg-port): fmgr_info_cxt from utils/fmgr.h.
    fn fmgr_info_cxt(procedureId: Oid, finfo: *mut FmgrInfo, mcxt: MemoryContext);

    /// TODO(pg-port): OidFunctionCall2Coll from utils/fmgr.h.
    fn OidFunctionCall2Coll(
        functionId: Oid,
        collation: Oid,
        arg1: Datum,
        arg2: Datum,
    ) -> Datum;

    /// TODO(pg-port): index_getprocinfo from access/index/indexam.c.
    pub fn index_getprocinfo(rel: Relation, attnum: c_int, procnum: uint32) -> *mut FmgrInfo;

    /// TODO(pg-port): RelationGetRelationName from utils/rel.h.
    fn RelationGetRelationName(rel: Relation) -> *const c_char;

    /// TODO(pg-port): IndexRelationGetNumberOfKeyAttributes from access/index/indexam.h.
    fn IndexRelationGetNumberOfKeyAttributes(rel: Relation) -> c_int;

    /// TODO(pg-port): get_typlenbyvalalign from utils/lsyscache.h.
    fn get_typlenbyvalalign(
        typid: Oid,
        typlen: *mut int16,
        typbyval: *mut bool,
        typalign: *mut c_char,
    );

    /// TODO(pg-port): deconstruct_array from utils/array.h.
    fn deconstruct_array(
        array: *mut ArrayType,
        elmtype: Oid,
        elmlen: int16,
        elmbyval: bool,
        elmalign: c_char,
        elemsp: *mut *mut Datum,
        nullsp: *mut *mut bool,
        nelemsp: *mut c_int,
    );

    /// TODO(pg-port): PrepareSkipSupportFromOpclass from access/nbtree.h.
    fn PrepareSkipSupportFromOpclass(
        opfamily: Oid,
        opcintype: Oid,
        r#reverse: bool,
    ) -> *mut BTSkipArraySupport;

    /// TODO(pg-port): ScanKeyEntryInitialize from access/skey.h.
    fn ScanKeyEntryInitialize(
        entry: ScanKey,
        flags: c_int,
        attributeNumber: AttrNumber,
        strategy: StrategyNumber,
        subtype: Oid,
        collation: Oid,
        procedure: RegProcedure,
        argument: Datum,
    );

    /// TODO(pg-port): qsort from stdlib.
    fn qsort(
        base: *mut c_void,
        nel: usize,
        elsize: usize,
        cmp: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );

    /// TODO(pg-port): qsort_arg from lib/qsort_arg.c.
    fn qsort_arg(
        base: *mut c_void,
        nel: usize,
        elsize: usize,
        cmp: unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void) -> c_int,
        arg: *mut c_void,
    );

    /// TODO(pg-port): qunique_arg from lib/qunique.h.
    fn qunique_arg(
        base: *mut c_void,
        nel: usize,
        elsize: usize,
        cmp: unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void) -> c_int,
        arg: *mut c_void,
    ) -> usize;

    /// TODO(pg-port): RelationGetDescr from utils/rel.h.
    fn RelationGetDescr(rel: Relation) -> TupleDesc;

    /// TODO(pg-port): MemoryContextReset from utils/memutils.h.
    fn MemoryContextReset(context: MemoryContext);

    /// TODO(pg-port): errcode from utils/elog.h.
    fn errcode(sqlerrcode: c_int) -> c_int;

    /// TODO(pg-port): errmsg_internal from utils/elog.h.
    fn errmsg_internal(fmt: *const c_char, ...) -> c_int;
}

/// TODO(pg-port): OidIsValid macro from c.h.
#[inline]
pub unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// TODO(pg-port): RegProcedureIsValid macro (same as OidIsValid).
#[inline]
pub unsafe fn RegProcedureIsValid(proc_: RegProcedure) -> bool {
    OidIsValid(proc_)
}

/// TODO(pg-port): ARR_ELEMTYPE from utils/array.h.
#[inline]
pub unsafe fn ARR_ELEMTYPE(a: *mut ArrayType) -> Oid {
    (*a).elemtype
}

/// TODO(pg-port): DatumGetArrayTypeP from utils/array.h (detoast).
#[inline]
pub unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    // TODO(pg-port): should call PG_DETOAST_DATUM when ported.
    d as *mut ArrayType
}

/// TODO(pg-port): pg_cmp_s32 from common/int.h.
#[inline]
fn pg_cmp_s32(a: i32, b: i32) -> c_int {
    a.cmp(&b) as c_int
}

/// TODO(pg-port): INDEX_MAX_KEYS from pg_config_manual.h.
use crate::pg_config_manual::INDEX_MAX_KEYS;

/// DEBUG_DISABLE_SKIP_SCAN compile-time flag (never set in normal builds).
#[cfg(debug_disable_skip_scan)]
const DEBUG_DISABLE_SKIP_SCAN: bool = true;
#[cfg(not(debug_disable_skip_scan))]
const DEBUG_DISABLE_SKIP_SCAN: bool = false;

// ===========================================================================
// Part 1 ends here.  (header + imports + types + BTSortArrayContext helpers)
// ===========================================================================

// ===========================================================================
// Part 2 -- _bt_preprocess_keys, _bt_fix_scankey_strategy,
//            _bt_mark_scankey_required
// ===========================================================================

/*
 *	_bt_preprocess_keys() -- Preprocess scan keys
 *
 * The given search-type keys (taken from scan->keyData[])
 * are copied to so->keyData[] with possible transformation.
 * scan->numberOfKeys is the number of input keys, so->numberOfKeys gets
 * the number of output keys.  Calling here a second or subsequent time
 * (during the same btrescan) is a no-op.
 * [... full C comment preserved for brevity; see C source ...]
 */
pub unsafe fn _bt_preprocess_keys(scan: IndexScanDesc) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut numberOfKeys: c_int = (*scan).numberOfKeys;
    let indoption: *mut int16 = (*(*scan).indexRelation).rd_indoption;
    let mut new_numberOfKeys: c_int;
    let mut numberOfEqualCols: c_int;
    let inkeys: ScanKey;
    let mut xform: [BTScanKeyPreproc; BTMaxStrategyNumber as usize] = [
        BTScanKeyPreproc { inkey: core::ptr::null_mut(), inkeyi: 0, arrayidx: 0 };
        BTMaxStrategyNumber as usize
    ];
    let mut test_result: bool = false;
    let mut redundant_key_kept: bool = false;
    let mut attno: AttrNumber;
    let mut arrayKeyData: ScanKey;
    let mut keyDataMap: *mut c_int = core::ptr::null_mut();
    let mut arrayidx: c_int = 0;

    if (*so).numberOfKeys > 0 {
        /*
         * Only need to do preprocessing once per btrescan, at most.  All
         * calls after the first are handled as no-ops.
         */
        return;
    }

    /* initialize result variables */
    (*so).qual_ok = true;
    (*so).numberOfKeys = 0;

    if numberOfKeys < 1 {
        return; /* done if qual-less scan */
    }

    /* If any keys are SK_SEARCHARRAY type, set up array-key info */
    arrayKeyData = _bt_preprocess_array_keys(scan, &mut numberOfKeys);
    if !(*so).qual_ok {
        /* unmatchable array, so give up */
        return;
    }

    /*
     * Treat arrayKeyData[] (a partially preprocessed copy of scan->keyData[])
     * as our input if _bt_preprocess_array_keys just allocated it, else just
     * use scan->keyData[]
     */
    let inkeys: ScanKey;
    if !arrayKeyData.is_null() {
        inkeys = arrayKeyData;

        /* Also maintain keyDataMap for remapping so->orderProcs[] later */
        keyDataMap = MemoryContextAlloc(
            (*so).arrayContext,
            (numberOfKeys as usize) * size_of::<c_int>(),
        ) as *mut c_int;

        /*
         * Also enlarge output array when it might otherwise not have room for
         * a skip array's scan key
         */
        if numberOfKeys > (*scan).numberOfKeys {
            (*so).keyData = repalloc(
                (*so).keyData as *mut c_void,
                (numberOfKeys as usize) * size_of::<ScanKeyData>(),
            ) as ScanKey;
        }
    } else {
        inkeys = (*scan).keyData;
    }

    /* we check that input keys are correctly ordered */
    if (*inkeys).sk_attno < 1 {
        elog!(ERROR, "btree index keys must be ordered by attribute");
    }

    /* We can short-circuit most of the work if there's just one key */
    if numberOfKeys == 1 {
        /* Apply indoption to scankey (might change sk_strategy!) */
        if !_bt_fix_scankey_strategy(inkeys, indoption) {
            (*so).qual_ok = false;
        }
        memcpy(
            (*so).keyData as *mut c_void,
            inkeys as *const c_void,
            size_of::<ScanKeyData>(),
        );
        (*so).numberOfKeys = 1;
        /* We can mark the qual as required if it's for first index col */
        if (*inkeys).sk_attno == 1 {
            _bt_mark_scankey_required((*so).keyData);
        }
        if !arrayKeyData.is_null() {
            /*
             * Don't call _bt_preprocess_array_keys_final in this fast path
             * (we'll miss out on the single value array transformation, but
             * that's not nearly as important when there's only one scan key)
             */
            Assert!((*(*so).keyData).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0);
            Assert!((*(*so).keyData).sk_strategy != BTEqualStrategyNumber ||
                ((*(*so).arrayKeys).scan_key == 0 &&
                 (*(*so).keyData).sk_flags as u32 & SK_BT_SKIP == 0 &&
                 OidIsValid((*(*so).orderProcs).fn_oid)));
        }
        return;
    }

    /*
     * Otherwise, do the full set of pushups.
     */
    new_numberOfKeys = 0;
    numberOfEqualCols = 0;

    /*
     * Initialize for processing of keys for attr 1.
     *
     * xform[i] points to the currently best scan key of strategy type i+1; it
     * is NULL if we haven't yet found such a key for this attr.
     */
    attno = 1;
    for x in xform.iter_mut() {
        x.inkey = core::ptr::null_mut();
        x.inkeyi = 0;
        x.arrayidx = 0;
    }

    /*
     * Loop iterates from 0 to numberOfKeys inclusive; we use the last pass to
     * handle after-last-key processing.  Actual exit from the loop is at the
     * "break" statement below.
     */
    let mut i: c_int = 0;
    loop {
        let inkey: ScanKey = inkeys.add(i as usize);
        let mut j: c_int;

        if i < numberOfKeys {
            /* Apply indoption to scankey (might change sk_strategy!) */
            if !_bt_fix_scankey_strategy(inkey, indoption) {
                /* NULL can't be matched, so give up */
                (*so).qual_ok = false;
                return;
            }
        }

        /*
         * If we are at the end of the keys for a particular attr, finish up
         * processing and emit the cleaned-up keys.
         */
        if i == numberOfKeys || (*inkey).sk_attno != attno {
            let priorNumberOfEqualCols: c_int = numberOfEqualCols;

            /* check input keys are correctly ordered */
            if i < numberOfKeys && (*inkey).sk_attno < attno {
                elog!(ERROR, "btree index keys must be ordered by attribute");
            }

            /*
             * If = has been specified, all other keys can be eliminated as
             * redundant.  Note that this is no less true if the = key is
             * SEARCHARRAY; the only real difference is that the inequality
             * key _becomes_ redundant by making _bt_compare_scankey_args
             * eliminate the subset of elements that won't need to be matched
             * (with SAOP arrays and skip arrays alike).
             *
             * If we have a case like "key = 1 AND key > 2", we set qual_ok to
             * false and abandon further processing.  We'll do the same thing
             * given a case like "key IN (0, 1) AND key > 2".
             *
             * We also have to deal with the case of "key IS NULL", which is
             * unsatisfiable in combination with any other index condition. By
             * the time we get here, that's been classified as an equality
             * check, and we've rejected any combination of it with a regular
             * equality condition; but not with other types of conditions.
             */
            if !xform[(BTEqualStrategyNumber - 1) as usize].inkey.is_null() {
                let eq: ScanKey = xform[(BTEqualStrategyNumber - 1) as usize].inkey;
                let mut array: *mut BTArrayKeyInfo = core::ptr::null_mut();
                let mut orderproc: *mut FmgrInfo = core::ptr::null_mut();

                if !arrayKeyData.is_null() && (*eq).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 {
                    let eq_in_ikey: c_int;
                    let eq_arrayidx: c_int;

                    eq_in_ikey = xform[(BTEqualStrategyNumber - 1) as usize].inkeyi;
                    eq_arrayidx = xform[(BTEqualStrategyNumber - 1) as usize].arrayidx;
                    array = (*so).arrayKeys.add((eq_arrayidx - 1) as usize);
                    orderproc = (*so).orderProcs.add(eq_in_ikey as usize);

                    Assert!((*array).scan_key == eq_in_ikey);
                    Assert!(OidIsValid((*orderproc).fn_oid));
                }

                j = BTMaxStrategyNumber as c_int;
                while { j -= 1; j } >= 0 {
                    let chk: ScanKey = xform[j as usize].inkey;

                    if chk.is_null() || j == (BTEqualStrategyNumber as c_int - 1) {
                        continue;
                    }

                    if (*eq).sk_flags as u32 & SK_SEARCHNULL as u32 != 0 {
                        /* IS NULL is contradictory to anything else */
                        (*so).qual_ok = false;
                        return;
                    }

                    if _bt_compare_scankey_args(scan, chk, eq, chk,
                                                array, orderproc,
                                                &mut test_result)
                    {
                        if !test_result {
                            /* keys proven mutually contradictory */
                            (*so).qual_ok = false;
                            return;
                        }
                        /* else discard the redundant non-equality key */
                        xform[j as usize].inkey = core::ptr::null_mut();
                        xform[j as usize].inkeyi = -1;
                    } else {
                        redundant_key_kept = true;
                    }
                }
                /* track number of attrs for which we have "=" keys */
                numberOfEqualCols += 1;
            }

            /* try to keep only one of <, <= */
            if !xform[(BTLessStrategyNumber - 1) as usize].inkey.is_null() &&
               !xform[(BTLessEqualStrategyNumber - 1) as usize].inkey.is_null()
            {
                let lt: ScanKey = xform[(BTLessStrategyNumber - 1) as usize].inkey;
                let le: ScanKey = xform[(BTLessEqualStrategyNumber - 1) as usize].inkey;

                if _bt_compare_scankey_args(scan, le, lt, le, core::ptr::null_mut(),
                                            core::ptr::null_mut(), &mut test_result)
                {
                    if test_result {
                        xform[(BTLessEqualStrategyNumber - 1) as usize].inkey = core::ptr::null_mut();
                    } else {
                        xform[(BTLessStrategyNumber - 1) as usize].inkey = core::ptr::null_mut();
                    }
                } else {
                    redundant_key_kept = true;
                }
            }

            /* try to keep only one of >, >= */
            if !xform[(BTGreaterStrategyNumber - 1) as usize].inkey.is_null() &&
               !xform[(BTGreaterEqualStrategyNumber - 1) as usize].inkey.is_null()
            {
                let gt: ScanKey = xform[(BTGreaterStrategyNumber - 1) as usize].inkey;
                let ge: ScanKey = xform[(BTGreaterEqualStrategyNumber - 1) as usize].inkey;

                if _bt_compare_scankey_args(scan, ge, gt, ge, core::ptr::null_mut(),
                                            core::ptr::null_mut(), &mut test_result)
                {
                    if test_result {
                        xform[(BTGreaterEqualStrategyNumber - 1) as usize].inkey = core::ptr::null_mut();
                    } else {
                        xform[(BTGreaterStrategyNumber - 1) as usize].inkey = core::ptr::null_mut();
                    }
                } else {
                    redundant_key_kept = true;
                }
            }

            /*
             * Emit the cleaned-up keys into the so->keyData[] array, and then
             * mark them if they are required.  They are required (possibly
             * only in one direction) if all attrs before this one had "=".
             *
             * In practice we'll rarely output non-required scan keys here;
             * typically, _bt_preprocess_array_keys has already added "=" keys
             * sufficient to form an unbroken series of "=" constraints on all
             * attrs prior to the attr from the final scan->keyData[] key.
             */
            j = BTMaxStrategyNumber as c_int;
            while { j -= 1; j } >= 0 {
                if !xform[j as usize].inkey.is_null() {
                    let outkey: ScanKey = (*so).keyData.add(new_numberOfKeys as usize);
                    new_numberOfKeys += 1;
                    memcpy(
                        outkey as *mut c_void,
                        xform[j as usize].inkey as *const c_void,
                        size_of::<ScanKeyData>(),
                    );
                    if !arrayKeyData.is_null() {
                        *keyDataMap.add((new_numberOfKeys - 1) as usize) = xform[j as usize].inkeyi;
                    }
                    if priorNumberOfEqualCols == attno as c_int - 1 {
                        _bt_mark_scankey_required(outkey);
                    }
                }
            }

            /*
             * Exit loop here if done.
             */
            if i == numberOfKeys {
                break;
            }

            /* Re-initialize for new attno */
            attno = (*inkey).sk_attno;
            for x in xform.iter_mut() {
                x.inkey = core::ptr::null_mut();
                x.inkeyi = 0;
                x.arrayidx = 0;
            }
        }

        /* check strategy this key's operator corresponds to */
        j = (*inkey).sk_strategy as c_int - 1;

        if (*inkey).sk_strategy == BTEqualStrategyNumber &&
           (*inkey).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0
        {
            /* must track how input scan keys map to arrays */
            Assert!(!arrayKeyData.is_null());
            arrayidx += 1;
        }

        /*
         * have we seen a scan key for this same attribute and using this same
         * operator strategy before now?
         */
        if xform[j as usize].inkey.is_null() {
            /* nope, so this scan key wins by default (at least for now) */
            xform[j as usize].inkey = inkey;
            xform[j as usize].inkeyi = i;
            xform[j as usize].arrayidx = arrayidx;
        } else {
            let mut orderproc: *mut FmgrInfo = core::ptr::null_mut();
            let mut array: *mut BTArrayKeyInfo = core::ptr::null_mut();

            /*
             * Seen one of these before, so keep only the more restrictive key
             * if possible
             */
            if j == (BTEqualStrategyNumber as c_int - 1) && !arrayKeyData.is_null() {
                /*
                 * Have to set up array keys
                 */
                if (*inkey).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 {
                    array = (*so).arrayKeys.add((arrayidx - 1) as usize);
                    orderproc = (*so).orderProcs.add(i as usize);

                    Assert!((*array).scan_key == i);
                    Assert!(OidIsValid((*orderproc).fn_oid));
                    Assert!((*inkey).sk_flags as u32 & SK_BT_SKIP == 0);
                } else if (*xform[j as usize].inkey).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 {
                    array = (*so).arrayKeys.add((xform[j as usize].arrayidx - 1) as usize);
                    orderproc = (*so).orderProcs.add(xform[j as usize].inkeyi as usize);

                    Assert!((*array).scan_key == xform[j as usize].inkeyi);
                    Assert!(OidIsValid((*orderproc).fn_oid));
                    Assert!((*xform[j as usize].inkey).sk_flags as u32 & SK_BT_SKIP == 0);
                }

                /*
                 * Both scan keys might have arrays, in which case we'll
                 * arbitrarily pass only one of the arrays.  That won't
                 * matter, since _bt_compare_scankey_args is aware that two
                 * SEARCHARRAY scan keys mean that _bt_preprocess_array_keys
                 * failed to eliminate redundant arrays through array merging.
                 * _bt_compare_scankey_args just returns false when it sees
                 * this; it won't even try to examine either array.
                 */
            }

            if _bt_compare_scankey_args(scan, inkey, inkey, xform[j as usize].inkey,
                                        array, orderproc, &mut test_result)
            {
                /* Have all we need to determine redundancy */
                if test_result {
                    /*
                     * New key is more restrictive, and so replaces old key...
                     */
                    if j != (BTEqualStrategyNumber as c_int - 1) ||
                       (*xform[j as usize].inkey).sk_flags as u32 & SK_SEARCHARRAY as u32 == 0
                    {
                        xform[j as usize].inkey = inkey;
                        xform[j as usize].inkeyi = i;
                        xform[j as usize].arrayidx = arrayidx;
                    } else {
                        /*
                         * ...unless we have to keep the old key because it's
                         * an array that rendered the new key redundant.  We
                         * need to make sure that we don't throw away an array
                         * scan key.  _bt_preprocess_array_keys_final expects
                         * us to keep all of the arrays that weren't already
                         * eliminated by _bt_preprocess_array_keys earlier on.
                         */
                        Assert!((*inkey).sk_flags as u32 & SK_SEARCHARRAY as u32 == 0);
                    }
                } else if j == (BTEqualStrategyNumber as c_int - 1) {
                    /* key == a && key == b, but a != b */
                    (*so).qual_ok = false;
                    return;
                }
                /* else old key is more restrictive, keep it */
            } else {
                /*
                 * We can't determine which key is more restrictive.  Push
                 * xform[j] directly to the output array, then set xform[j] to
                 * the new scan key.
                 *
                 * Note: We do things this way around so that our arrays are
                 * always in the same order as their corresponding scan keys.
                 * _bt_preprocess_array_keys_final expects this.
                 */
                let outkey: ScanKey = (*so).keyData.add(new_numberOfKeys as usize);
                new_numberOfKeys += 1;
                memcpy(
                    outkey as *mut c_void,
                    xform[j as usize].inkey as *const c_void,
                    size_of::<ScanKeyData>(),
                );
                if !arrayKeyData.is_null() {
                    *keyDataMap.add((new_numberOfKeys - 1) as usize) = xform[j as usize].inkeyi;
                }
                if numberOfEqualCols == attno as c_int - 1 {
                    _bt_mark_scankey_required(outkey);
                }
                xform[j as usize].inkey = inkey;
                xform[j as usize].inkeyi = i;
                xform[j as usize].arrayidx = arrayidx;
                redundant_key_kept = true;
            }
        }

        i += 1;
    }

    (*so).numberOfKeys = new_numberOfKeys;

    /*
     * Now that we've built a temporary mapping from so->keyData[] (output
     * scan keys) to arrayKeyData[] (our input scan keys), fix array->scan_key
     * references.  Also consolidate the so->orderProcs[] array such that it
     * can be subscripted using so->keyData[]-wise offsets.
     */
    if !arrayKeyData.is_null() {
        _bt_preprocess_array_keys_final(scan, keyDataMap);
    }

    /*
     * If there are remaining redundant inequality keys, we must make sure
     * that each index attribute has no more than one required >/>= key, and
     * no more than one required </<= key.  Attributes that have one or more
     * required = keys now must keep only one required key (the first = key).
     */
    if unlikely(redundant_key_kept) && (*so).qual_ok {
        _bt_unmark_keys(scan, keyDataMap);
    }

    /* Could pfree arrayKeyData/keyDataMap now, but not worth the cycles */
}

/*
 * Adjust a scankey's strategy and flags setting as needed for indoptions.
 *
 * We copy the appropriate indoption value into the scankey sk_flags
 * (shifting to avoid clobbering system-defined flag bits).  Also, if
 * the DESC option is set, commute (flip) the operator strategy number.
 *
 * A secondary purpose is to check for IS NULL/NOT NULL scankeys and set up
 * the strategy field correctly for them.
 *
 * Lastly, for ordinary scankeys (not IS NULL/NOT NULL), we check for a
 * NULL comparison value.  Since all btree operators are assumed strict,
 * a NULL means that the qual cannot be satisfied.  We return true if the
 * comparison value isn't NULL, or false if the scan should be abandoned.
 *
 * This function is applied to the *input* scankey structure; therefore
 * on a rescan we will be looking at already-processed scankeys.  Hence
 * we have to be careful not to re-commute the strategy if we already did it.
 * It's a bit ugly to modify the caller's copy of the scankey but in practice
 * there shouldn't be any problem, since the index's indoptions are certainly
 * not going to change while the scankey survives.
 */
pub unsafe fn _bt_fix_scankey_strategy(skey: ScanKey, indoption: *mut int16) -> bool {
    let addflags: c_int;

    addflags = (*indoption.add(((*skey).sk_attno - 1) as usize) as c_int) << SK_BT_INDOPTION_SHIFT;

    /*
     * We treat all btree operators as strict (even if they're not so marked
     * in pg_proc). This means that it is impossible for an operator condition
     * with a NULL comparison constant to succeed, and we can reject it right
     * away.
     *
     * However, we now also support "x IS NULL" clauses as search conditions,
     * so in that case keep going. The planner has not filled in any
     * particular strategy in this case, so set it to BTEqualStrategyNumber
     * --- we can treat IS NULL as an equality operator for purposes of search
     * strategy.
     *
     * Likewise, "x IS NOT NULL" is supported.  We treat that as either "less
     * than NULL" in a NULLS LAST index, or "greater than NULL" in a NULLS
     * FIRST index.
     *
     * Note: someday we might have to fill in sk_collation from the index
     * column's collation.  At the moment this is a non-issue because we'll
     * never actually call the comparison operator on a NULL.
     */
    if (*skey).sk_flags as u32 & SK_ISNULL as u32 != 0 {
        /* SK_ISNULL shouldn't be set in a row header scankey */
        Assert!((*skey).sk_flags as u32 & SK_ROW_HEADER as u32 == 0);

        /* Set indoption flags in scankey (might be done already) */
        (*skey).sk_flags |= addflags;

        /* Set correct strategy for IS NULL or NOT NULL search */
        if (*skey).sk_flags as u32 & SK_SEARCHNULL as u32 != 0 {
            (*skey).sk_strategy = BTEqualStrategyNumber;
            (*skey).sk_subtype = InvalidOid;
            (*skey).sk_collation = InvalidOid;
        } else if (*skey).sk_flags as u32 & SK_SEARCHNOTNULL as u32 != 0 {
            if (*skey).sk_flags as u32 & SK_BT_NULLS_FIRST != 0 {
                (*skey).sk_strategy = BTGreaterStrategyNumber;
            } else {
                (*skey).sk_strategy = BTLessStrategyNumber;
            }
            (*skey).sk_subtype = InvalidOid;
            (*skey).sk_collation = InvalidOid;
        } else {
            /* regular qual, so it cannot be satisfied */
            return false;
        }

        /* Needn't do the rest */
        return true;
    }

    /* Adjust strategy for DESC, if we didn't already */
    if (addflags as u32 & SK_BT_DESC != 0) &&
       ((*skey).sk_flags as u32 & SK_BT_DESC == 0)
    {
        (*skey).sk_strategy = BTCommuteStrategyNumber((*skey).sk_strategy);
    }
    (*skey).sk_flags |= addflags;

    /* If it's a row header, fix row member flags and strategies similarly */
    if (*skey).sk_flags as u32 & SK_ROW_HEADER as u32 != 0 {
        let mut subkey: ScanKey = DatumGetPointer((*skey).sk_argument) as ScanKey;

        if (*subkey).sk_flags as u32 & SK_ISNULL as u32 != 0 {
            /* First row member is NULL, so RowCompare is unsatisfiable */
            Assert!((*subkey).sk_flags as u32 & SK_ROW_MEMBER as u32 != 0);
            return false;
        }

        loop {
            Assert!((*subkey).sk_flags as u32 & SK_ROW_MEMBER as u32 != 0);
            let addflags2 = (*indoption.add(((*subkey).sk_attno - 1) as usize) as c_int)
                << SK_BT_INDOPTION_SHIFT;
            if (addflags2 as u32 & SK_BT_DESC != 0) &&
               ((*subkey).sk_flags as u32 & SK_BT_DESC == 0)
            {
                (*subkey).sk_strategy = BTCommuteStrategyNumber((*subkey).sk_strategy);
            }
            (*subkey).sk_flags |= addflags2;
            if (*subkey).sk_flags as u32 & SK_ROW_END as u32 != 0 {
                break;
            }
            subkey = subkey.add(1);
        }
    }

    true
}

/*
 * Mark a scankey as "required to continue the scan".
 *
 * Depending on the operator type, the key may be required for both scan
 * directions or just one.  Also, if the key is a row comparison header,
 * we have to mark the appropriate subsidiary ScanKeys as required.  In such
 * cases, the first subsidiary key is required, but subsequent ones are
 * required only as long as they correspond to successive index columns and
 * match the leading column as to sort direction.  Otherwise the row
 * comparison ordering is different from the index ordering and so we can't
 * stop the scan on the basis of those lower-order columns.
 *
 * Note: when we set required-key flag bits in a subsidiary scankey, we are
 * scribbling on a data structure belonging to the index AM's caller, not on
 * our private copy.  This should be OK because the marking will not change
 * from scan to scan within a query, and so we'd just re-mark the same way
 * anyway on a rescan.  Something to keep an eye on though.
 */
pub unsafe fn _bt_mark_scankey_required(skey: ScanKey) {
    let addflags: c_int;

    addflags = match (*skey).sk_strategy {
        s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber =>
            SK_BT_REQFWD as c_int,
        s if s == BTEqualStrategyNumber =>
            (SK_BT_REQFWD | SK_BT_REQBKWD) as c_int,
        s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber =>
            SK_BT_REQBKWD as c_int,
        _ => {
            elog!(ERROR, "unrecognized StrategyNumber: {}", (*skey).sk_strategy as c_int);
            0 /* keep compiler quiet */
        }
    };

    (*skey).sk_flags |= addflags;

    if (*skey).sk_flags as u32 & SK_ROW_HEADER as u32 != 0 {
        let mut subkey: ScanKey = DatumGetPointer((*skey).sk_argument) as ScanKey;
        let mut attno: AttrNumber = (*skey).sk_attno;

        /* First subkey should be same column/operator as the header */
        Assert!((*subkey).sk_attno == attno);
        Assert!((*subkey).sk_strategy == (*skey).sk_strategy);

        loop {
            Assert!((*subkey).sk_flags as u32 & SK_ROW_MEMBER as u32 != 0);
            if (*subkey).sk_attno != attno {
                break; /* non-adjacent key, so not required */
            }
            if (*subkey).sk_strategy != (*skey).sk_strategy {
                break; /* wrong direction, so not required */
            }
            (*subkey).sk_flags |= addflags;
            if (*subkey).sk_flags as u32 & SK_ROW_END as u32 != 0 {
                break;
            }
            subkey = subkey.add(1);
            attno += 1;
        }
    }
}

// ===========================================================================
// Part 3 -- _bt_compare_scankey_args, _bt_compare_array_scankey_args,
//            _bt_saoparray_shrink, _bt_skiparray_shrink,
//            _bt_skiparray_strat_adjust, _bt_skiparray_strat_decrement,
//            _bt_skiparray_strat_increment, _bt_unmark_keys,
//            _bt_reorder_array_cmp
// ===========================================================================

/*
 * Compare two scankey values using a specified operator.
 *
 * The test we want to perform is logically "leftarg op rightarg", where
 * leftarg and rightarg are the sk_argument values in those ScanKeys, and
 * the comparison operator is the one in the op ScanKey.  However, in
 * cross-data-type situations we may need to look up the correct operator in
 * the index's opfamily: it is the one having amopstrategy = op->sk_strategy
 * and amoplefttype/amoprighttype equal to the two argument datatypes.
 *
 * If the opfamily doesn't supply a complete set of cross-type operators we
 * may not be able to make the comparison.  If we can make the comparison
 * we store the operator result in *result and return true.  We return false
 * if the comparison could not be made.
 *
 * [full C comment preserved; see C source for details]
 */
pub unsafe fn _bt_compare_scankey_args(
    scan: IndexScanDesc,
    op: ScanKey,
    leftarg: ScanKey,
    rightarg: ScanKey,
    array: *mut BTArrayKeyInfo,
    orderproc: *mut FmgrInfo,
    result: *mut bool,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let mut lefttype: Oid;
    let mut righttype: Oid;
    let mut optype: Oid;
    let opcintype: Oid;
    let mut cmp_op: Oid;
    let mut strat: StrategyNumber;

    Assert!((*leftarg).sk_flags as u32 & SK_ROW_MEMBER as u32 == 0);
    Assert!((*rightarg).sk_flags as u32 & SK_ROW_MEMBER as u32 == 0);

    /*
     * First, deal with cases where one or both args are NULL.  This should
     * only happen when the scankeys represent IS NULL/NOT NULL conditions.
     */
    if ((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_ISNULL as u32 != 0 {
        let leftnull: bool;
        let rightnull: bool;

        /* Handle skip array comparison with IS NOT NULL scan key */
        if ((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_BT_SKIP != 0 {
            /* Shouldn't generate skip array in presence of IS NULL key */
            Assert!(((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_SEARCHNULL as u32 == 0);
            Assert!(((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_SEARCHNOTNULL as u32 != 0);

            /* Skip array will have no NULL element/IS NULL scan key */
            Assert!((*array).num_elems == -1);
            (*array).null_elem = false;

            /* IS NOT NULL key (could be leftarg or rightarg) now redundant */
            *result = true;
            return true;
        }

        if (*leftarg).sk_flags as u32 & SK_ISNULL as u32 != 0 {
            Assert!((*leftarg).sk_flags as u32 & (SK_SEARCHNULL | SK_SEARCHNOTNULL) as u32 != 0);
            leftnull = true;
        } else {
            leftnull = false;
        }
        if (*rightarg).sk_flags as u32 & SK_ISNULL as u32 != 0 {
            Assert!((*rightarg).sk_flags as u32 & (SK_SEARCHNULL | SK_SEARCHNOTNULL) as u32 != 0);
            rightnull = true;
        } else {
            rightnull = false;
        }

        /*
         * We treat NULL as either greater than or less than all other values.
         * Since true > false, the tests below work correctly for NULLS LAST
         * logic.  If the index is NULLS FIRST, we need to flip the strategy.
         */
        strat = (*op).sk_strategy;
        if (*op).sk_flags as u32 & SK_BT_NULLS_FIRST != 0 {
            strat = BTCommuteStrategyNumber(strat);
        }

        *result = match strat {
            s if s == BTLessStrategyNumber =>
                (leftnull as c_int) < (rightnull as c_int),
            s if s == BTLessEqualStrategyNumber =>
                (leftnull as c_int) <= (rightnull as c_int),
            s if s == BTEqualStrategyNumber =>
                leftnull == rightnull,
            s if s == BTGreaterEqualStrategyNumber =>
                (leftnull as c_int) >= (rightnull as c_int),
            s if s == BTGreaterStrategyNumber =>
                (leftnull as c_int) > (rightnull as c_int),
            _ => {
                elog!(ERROR, "unrecognized StrategyNumber: {}", strat as c_int);
                false /* keep compiler quiet */
            }
        };
        return true;
    }

    /*
     * We don't yet know how to determine redundancy when it involves a row
     * compare key (barring simple cases involving IS NULL/IS NOT NULL)
     */
    if ((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_ROW_HEADER as u32 != 0 {
        Assert!(((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_BT_SKIP == 0);
        return false;
    }

    /*
     * If either leftarg or rightarg are equality-type array scankeys, we need
     * specialized handling (since by now we know that IS NULL wasn't used)
     */
    if !array.is_null() {
        let leftarray: bool;
        let rightarray: bool;

        leftarray = (*leftarg).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 &&
                    (*leftarg).sk_strategy == BTEqualStrategyNumber;
        rightarray = (*rightarg).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 &&
                     (*rightarg).sk_strategy == BTEqualStrategyNumber;

        /*
         * _bt_preprocess_array_keys is responsible for merging together array
         * scan keys, and will do so whenever the opfamily has the required
         * cross-type support.  If it failed to do that, we handle it just
         * like the case where we can't make the comparison ourselves.
         */
        if leftarray && rightarray {
            /* Can't make the comparison */
            *result = false; /* suppress compiler warnings */
            Assert!(((*leftarg).sk_flags | (*rightarg).sk_flags) as u32 & SK_BT_SKIP == 0);
            return false;
        }

        /*
         * Otherwise we need to determine if either one of leftarg or rightarg
         * uses an array, then pass this through to a dedicated helper
         * function.
         */
        if leftarray {
            return _bt_compare_array_scankey_args(scan, leftarg, rightarg,
                                                  orderproc, array, result);
        } else if rightarray {
            return _bt_compare_array_scankey_args(scan, rightarg, leftarg,
                                                  orderproc, array, result);
        }

        /* FALL THRU */
    }

    /*
     * The opfamily we need to worry about is identified by the index column.
     */
    Assert!((*leftarg).sk_attno == (*rightarg).sk_attno);

    opcintype = (*(*rel).rd_opcintype.add(((*leftarg).sk_attno - 1) as usize));

    /*
     * Determine the actual datatypes of the ScanKey arguments.  We have to
     * support the convention that sk_subtype == InvalidOid means the opclass
     * input type; this is a hack to simplify life for ScanKeyInit().
     */
    lefttype = (*leftarg).sk_subtype;
    if lefttype == InvalidOid {
        lefttype = opcintype;
    }
    righttype = (*rightarg).sk_subtype;
    if righttype == InvalidOid {
        righttype = opcintype;
    }
    optype = (*op).sk_subtype;
    if optype == InvalidOid {
        optype = opcintype;
    }

    /*
     * If leftarg and rightarg match the types expected for the "op" scankey,
     * we can use its already-looked-up comparison function.
     */
    if lefttype == opcintype && righttype == optype {
        *result = DatumGetBool(FunctionCall2Coll(
            &mut (*op).sk_func,
            (*op).sk_collation,
            (*leftarg).sk_argument,
            (*rightarg).sk_argument,
        ));
        return true;
    }

    /*
     * Otherwise, we need to go to the syscache to find the appropriate
     * operator.  (This cannot result in infinite recursion, since no
     * indexscan initiated by syscache lookup will use cross-data-type
     * operators.)
     *
     * If the sk_strategy was flipped by _bt_fix_scankey_strategy, we have to
     * un-flip it to get the correct opfamily member.
     */
    strat = (*op).sk_strategy;
    if (*op).sk_flags as u32 & SK_BT_DESC != 0 {
        strat = BTCommuteStrategyNumber(strat);
    }

    cmp_op = get_opfamily_member(
        *(*rel).rd_opfamily.add(((*leftarg).sk_attno - 1) as usize),
        lefttype,
        righttype,
        strat,
    );
    if OidIsValid(cmp_op) {
        let cmp_proc: RegProcedure = get_opcode(cmp_op);

        if RegProcedureIsValid(cmp_proc) {
            *result = DatumGetBool(OidFunctionCall2Coll(
                cmp_proc,
                (*op).sk_collation,
                (*leftarg).sk_argument,
                (*rightarg).sk_argument,
            ));
            return true;
        }
    }

    /* Can't make the comparison */
    *result = false; /* suppress compiler warnings */
    false
}

/*
 * Compare an array scan key to a scalar scan key, eliminating contradictory
 * array elements such that the scalar scan key becomes redundant.
 *
 * If the opfamily is incomplete we may not be able to determine which
 * elements are contradictory.  When we return true we'll have validly set
 * *qual_ok, guaranteeing that at least the scalar scan key can be considered
 * redundant.  We return false if the comparison could not be made (caller
 * must keep both scan keys when this happens).
 *
 * Note: it's up to caller to deal with IS [NOT] NULL scan keys, as well as
 * row comparison scan keys.  We only deal with scalar scan keys.
 */
pub unsafe fn _bt_compare_array_scankey_args(
    scan: IndexScanDesc,
    arraysk: ScanKey,
    skey: ScanKey,
    orderproc: *mut FmgrInfo,
    array: *mut BTArrayKeyInfo,
    qual_ok: *mut bool,
) -> bool {
    Assert!((*arraysk).sk_attno == (*skey).sk_attno);
    Assert!((*arraysk).sk_flags as u32 & (SK_ISNULL | SK_ROW_HEADER | SK_ROW_MEMBER) as u32 == 0);
    Assert!((*arraysk).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 &&
            (*arraysk).sk_strategy == BTEqualStrategyNumber);
    /* don't expect to have to deal with NULLs/row comparison scan keys */
    Assert!((*skey).sk_flags as u32 & (SK_ISNULL | SK_ROW_HEADER | SK_ROW_MEMBER) as u32 == 0);
    Assert!((*skey).sk_flags as u32 & SK_SEARCHARRAY as u32 == 0 ||
            (*skey).sk_strategy != BTEqualStrategyNumber);

    /*
     * Just call the appropriate helper function based on whether it's a SAOP
     * array or a skip array.  Both helpers will set *qual_ok in passing.
     */
    if (*array).num_elems != -1 {
        _bt_saoparray_shrink(scan, arraysk, skey, orderproc, array, qual_ok)
    } else {
        _bt_skiparray_shrink(scan, skey, array, qual_ok)
    }
}

/*
 * Preprocessing of SAOP array scan key, used to determine which array
 * elements are eliminated as contradictory by a non-array scalar key.
 *
 * _bt_compare_array_scankey_args helper function.
 *
 * Array elements can be eliminated as contradictory when excluded by some
 * other operator on the same attribute.  For example, with an index scan qual
 * "WHERE a IN (1, 2, 3) AND a < 2", all array elements except the value "1"
 * are eliminated, and the < scan key is eliminated as redundant.  Cases where
 * every array element is eliminated by a redundant scalar scan key have an
 * unsatisfiable qual, which we handle by setting *qual_ok=false for caller.
 */
pub unsafe fn _bt_saoparray_shrink(
    scan: IndexScanDesc,
    arraysk: ScanKey,
    skey: ScanKey,
    orderproc: *mut FmgrInfo,
    array: *mut BTArrayKeyInfo,
    qual_ok: *mut bool,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let opcintype: Oid = *(*rel).rd_opcintype.add(((*arraysk).sk_attno - 1) as usize);
    let mut cmpresult: c_int = 0;
    let mut cmpexact: c_int = 0;
    let mut matchelem: c_int;
    let mut new_nelems: c_int = 0;
    let mut crosstypeproc: FmgrInfo = core::mem::zeroed();
    let mut orderprocp: *mut FmgrInfo = orderproc;

    Assert!((*array).num_elems > 0);
    Assert!((*arraysk).sk_flags as u32 & SK_BT_SKIP == 0);

    /*
     * _bt_binsrch_array_skey searches an array for the entry best matching a
     * datum of opclass input type for the index's attribute (on-disk type).
     * We can reuse the array's ORDER proc whenever the non-array scan key's
     * type is a match for the corresponding attribute's input opclass type.
     * Otherwise, we have to do another ORDER proc lookup so that our call to
     * _bt_binsrch_array_skey applies the correct comparator.
     *
     * Note: we have to support the convention that sk_subtype == InvalidOid
     * means the opclass input type; this is a hack to simplify life for
     * ScanKeyInit().
     */
    if (*skey).sk_subtype != opcintype && (*skey).sk_subtype != InvalidOid {
        let cmp_proc: RegProcedure;
        let arraysk_elemtype: Oid;

        /*
         * Need an ORDER proc lookup to detect redundancy/contradictoriness
         * with this pair of scankeys.
         *
         * Scalar scan key's argument will be passed to _bt_compare_array_skey
         * as its tupdatum/lefthand argument (rhs arg is for array elements).
         */
        arraysk_elemtype = if (*arraysk).sk_subtype == InvalidOid {
            *(*rel).rd_opcintype.add(((*arraysk).sk_attno - 1) as usize)
        } else {
            (*arraysk).sk_subtype
        };
        cmp_proc = get_opfamily_proc(
            *(*rel).rd_opfamily.add(((*arraysk).sk_attno - 1) as usize),
            (*skey).sk_subtype,
            arraysk_elemtype,
            BTORDER_PROC,
        );
        if !RegProcedureIsValid(cmp_proc) {
            /* Can't make the comparison */
            *qual_ok = false; /* suppress compiler warnings */
            return false;
        }

        /* We have all we need to determine redundancy/contradictoriness */
        orderprocp = &mut crosstypeproc;
        fmgr_info(cmp_proc, orderprocp);
    }

    matchelem = _bt_binsrch_array_skey(
        orderprocp,
        false,
        NoMovementScanDirection,
        (*skey).sk_argument,
        false,
        array,
        arraysk,
        &mut cmpresult,
    );

    match (*skey).sk_strategy {
        s if s == BTLessStrategyNumber => {
            cmpexact = 1; /* exclude exact match, if any */
            /* FALL THRU */
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            /* Resize, keeping elements from the start of the array */
            new_nelems = matchelem;
        }
        s if s == BTLessEqualStrategyNumber => {
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            /* Resize, keeping elements from the start of the array */
            new_nelems = matchelem;
        }
        s if s == BTEqualStrategyNumber => {
            if cmpresult != 0 {
                /* qual is unsatisfiable */
                new_nelems = 0;
            } else {
                /* Shift matching element to the start of the array, resize */
                *(*array).elem_values = *(*array).elem_values.add(matchelem as usize);
                new_nelems = 1;
            }
        }
        s if s == BTGreaterEqualStrategyNumber => {
            cmpexact = 1; /* include exact match, if any */
            /* FALL THRU */
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            /* Shift matching elements to the start of the array, resize */
            new_nelems = (*array).num_elems - matchelem;
            memmove(
                (*array).elem_values as *mut c_void,
                (*array).elem_values.add(matchelem as usize) as *const c_void,
                size_of::<Datum>() * new_nelems as usize,
            );
        }
        s if s == BTGreaterStrategyNumber => {
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            /* Shift matching elements to the start of the array, resize */
            new_nelems = (*array).num_elems - matchelem;
            memmove(
                (*array).elem_values as *mut c_void,
                (*array).elem_values.add(matchelem as usize) as *const c_void,
                size_of::<Datum>() * new_nelems as usize,
            );
        }
        _ => {
            elog!(ERROR, "unrecognized StrategyNumber: {}", (*skey).sk_strategy as c_int);
        }
    }

    Assert!(new_nelems >= 0);
    Assert!(new_nelems <= (*array).num_elems);

    (*array).num_elems = new_nelems;
    *qual_ok = new_nelems > 0;

    true
}

/*
 * Preprocessing of skip array scan key, used to determine redundancy against
 * a non-array scalar scan key (must be an inequality).
 *
 * _bt_compare_array_scankey_args helper function.
 *
 * Skip arrays work by procedurally generating their elements as needed, so we
 * just store the inequality as the skip array's low_compare or high_compare
 * (except when there's already a more restrictive low_compare/high_compare).
 * The array's final elements are the range of values that still satisfy the
 * array's final low_compare and high_compare.
 */
pub unsafe fn _bt_skiparray_shrink(
    scan: IndexScanDesc,
    skey: ScanKey,
    array: *mut BTArrayKeyInfo,
    qual_ok: *mut bool,
) -> bool {
    let mut test_result: bool = false;

    Assert!((*array).num_elems == -1);

    /*
     * Array's index attribute will be constrained by a strict operator/key.
     * Array must not "contain a NULL element" (i.e. the scan must not apply
     * "IS NULL" qual when it reaches the end of the index that stores NULLs).
     */
    (*array).null_elem = false;
    *qual_ok = true;

    /*
     * Consider if we should treat caller's scalar scan key as the skip
     * array's high_compare or low_compare.
     *
     * [full C comment about MINVAL/MAXVAL sentinel keys preserved; see C source]
     */
    match (*skey).sk_strategy {
        s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber => {
            if !(*array).high_compare.is_null() {
                /* replace existing high_compare with caller's key? */
                if !_bt_compare_scankey_args(scan, (*array).high_compare, skey,
                                             (*array).high_compare, core::ptr::null_mut(),
                                             core::ptr::null_mut(), &mut test_result)
                {
                    return false; /* can't determine more restrictive key */
                }

                if !test_result {
                    return true; /* no, just discard caller's key */
                }

                /* yes, replace existing high_compare with caller's key */
            }

            /* caller's key becomes skip array's high_compare */
            (*array).high_compare = skey;
        }
        s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber => {
            if !(*array).low_compare.is_null() {
                /* replace existing low_compare with caller's key? */
                if !_bt_compare_scankey_args(scan, (*array).low_compare, skey,
                                             (*array).low_compare, core::ptr::null_mut(),
                                             core::ptr::null_mut(), &mut test_result)
                {
                    return false; /* can't determine more restrictive key */
                }

                if !test_result {
                    return true; /* no, just discard caller's key */
                }

                /* yes, replace existing low_compare with caller's key */
            }

            /* caller's key becomes skip array's low_compare */
            (*array).low_compare = skey;
        }
        _ => {
            elog!(ERROR, "unrecognized StrategyNumber: {}", (*skey).sk_strategy as c_int);
        }
    }

    true
}

/*
 * Applies the opfamily's skip support routine to convert the skip array's >
 * low_compare key (if any) into a >= key, and to convert its < high_compare
 * key (if any) into a <= key.  Decrements the high_compare key's sk_argument,
 * and/or increments the low_compare key's sk_argument (also adjusts their
 * operator strategies, while changing the operator as appropriate).
 *
 * [full C comment preserved; see C source]
 */
pub unsafe fn _bt_skiparray_strat_adjust(
    scan: IndexScanDesc,
    arraysk: ScanKey,
    array: *mut BTArrayKeyInfo,
) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let oldContext: MemoryContext;

    /*
     * Called last among all preprocessing steps, when the skip array's final
     * low_compare and high_compare have both been chosen
     */
    Assert!((*arraysk).sk_flags as u32 & SK_BT_SKIP != 0);
    Assert!((*array).num_elems == -1 && !(*array).null_elem && !(*array).sksup.is_null());

    oldContext = MemoryContextSwitchTo((*so).arrayContext);

    if !(*array).high_compare.is_null() &&
       (*(*array).high_compare).sk_strategy == BTLessStrategyNumber
    {
        _bt_skiparray_strat_decrement(scan, arraysk, array);
    }

    if !(*array).low_compare.is_null() &&
       (*(*array).low_compare).sk_strategy == BTGreaterStrategyNumber
    {
        _bt_skiparray_strat_increment(scan, arraysk, array);
    }

    MemoryContextSwitchTo(oldContext);
}

/*
 * Convert skip array's < high_compare key into a <= key
 */
pub unsafe fn _bt_skiparray_strat_decrement(
    scan: IndexScanDesc,
    arraysk: ScanKey,
    array: *mut BTArrayKeyInfo,
) {
    let rel: Relation = (*scan).indexRelation;
    let opfamily: Oid = *(*rel).rd_opfamily.add(((*arraysk).sk_attno - 1) as usize);
    let opcintype: Oid = *(*rel).rd_opcintype.add(((*arraysk).sk_attno - 1) as usize);
    let leop: Oid;
    let cmp_proc: RegProcedure;
    let high_compare: ScanKey = (*array).high_compare;
    let orig_sk_argument: Datum = (*high_compare).sk_argument;
    let new_sk_argument: Datum;
    let mut uflow: bool = false;
    let lookupstrat: StrategyNumber;

    Assert!((*high_compare).sk_strategy == BTLessStrategyNumber);

    /*
     * Only perform the transformation when the operator type matches the
     * index attribute's input opclass type
     */
    if (*high_compare).sk_subtype != opcintype && (*high_compare).sk_subtype != InvalidOid {
        return;
    }

    /* Decrement, handling underflow by marking the qual unsatisfiable */
    new_sk_argument = ((*(*array).sksup).decrement)(rel, orig_sk_argument, &mut uflow);
    if uflow {
        let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
        (*so).qual_ok = false;
        return;
    }

    /*
     * Look up <= operator (might fail), accounting for the fact that a
     * high_compare on a DESC column already had its strategy commuted
     */
    lookupstrat = if (*high_compare).sk_flags as u32 & SK_BT_DESC != 0 {
        BTGreaterEqualStrategyNumber /* commute this too */
    } else {
        BTLessEqualStrategyNumber
    };
    leop = get_opfamily_member(opfamily, opcintype, opcintype, lookupstrat);
    if !OidIsValid(leop) {
        return;
    }
    cmp_proc = get_opcode(leop);
    if RegProcedureIsValid(cmp_proc) {
        /* Transform < high_compare key into <= key */
        fmgr_info(cmp_proc, &mut (*high_compare).sk_func);
        (*high_compare).sk_argument = new_sk_argument;
        (*high_compare).sk_strategy = BTLessEqualStrategyNumber;
    }
}

/*
 * Convert skip array's > low_compare key into a >= key
 */
pub unsafe fn _bt_skiparray_strat_increment(
    scan: IndexScanDesc,
    arraysk: ScanKey,
    array: *mut BTArrayKeyInfo,
) {
    let rel: Relation = (*scan).indexRelation;
    let opfamily: Oid = *(*rel).rd_opfamily.add(((*arraysk).sk_attno - 1) as usize);
    let opcintype: Oid = *(*rel).rd_opcintype.add(((*arraysk).sk_attno - 1) as usize);
    let geop: Oid;
    let cmp_proc: RegProcedure;
    let low_compare: ScanKey = (*array).low_compare;
    let orig_sk_argument: Datum = (*low_compare).sk_argument;
    let new_sk_argument: Datum;
    let mut oflow: bool = false;
    let lookupstrat: StrategyNumber;

    Assert!((*low_compare).sk_strategy == BTGreaterStrategyNumber);

    /*
     * Only perform the transformation when the operator type matches the
     * index attribute's input opclass type
     */
    if (*low_compare).sk_subtype != opcintype && (*low_compare).sk_subtype != InvalidOid {
        return;
    }

    /* Increment, handling overflow by marking the qual unsatisfiable */
    new_sk_argument = ((*(*array).sksup).increment)(rel, orig_sk_argument, &mut oflow);
    if oflow {
        let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
        (*so).qual_ok = false;
        return;
    }

    /*
     * Look up >= operator (might fail), accounting for the fact that a
     * low_compare on a DESC column already had its strategy commuted
     */
    lookupstrat = if (*low_compare).sk_flags as u32 & SK_BT_DESC != 0 {
        BTLessEqualStrategyNumber /* commute this too */
    } else {
        BTGreaterEqualStrategyNumber
    };
    geop = get_opfamily_member(opfamily, opcintype, opcintype, lookupstrat);
    if !OidIsValid(geop) {
        return;
    }
    cmp_proc = get_opcode(geop);
    if RegProcedureIsValid(cmp_proc) {
        /* Transform > low_compare key into >= key */
        fmgr_info(cmp_proc, &mut (*low_compare).sk_func);
        (*low_compare).sk_argument = new_sk_argument;
        (*low_compare).sk_strategy = BTGreaterEqualStrategyNumber;
    }
}

/*
 *	_bt_unmark_keys() -- make superfluous required keys nonrequired after all
 *
 * When _bt_preprocess_keys fails to eliminate one or more redundant keys, it
 * calls here to make sure that no index attribute has more than one > or >=
 * key marked required, and no more than one required < or <= key.  Attributes
 * with = keys will always get one = key as their required key.  All other
 * keys that were initially marked required get "unmarked" here.  That way,
 * _bt_first and _bt_checkkeys will reliably agree on which keys to use to
 * start and/or to end the scan.
 *
 * We also relocate keys that become/started out nonrequired to the end of
 * so->keyData[].  That way, _bt_first and _bt_checkkeys cannot fail to reach
 * a required key due to some earlier nonrequired key getting in the way.
 *
 * Only call here when _bt_compare_scankey_args returned false at least once
 * (otherwise, calling here will just waste cycles).
 */
pub unsafe fn _bt_unmark_keys(scan: IndexScanDesc, keyDataMap: *mut c_int) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut attno: AttrNumber;
    let unmarkikey: *mut bool;
    let mut nunmark: c_int;
    let mut nunmarked: c_int;
    let mut nkept: c_int;
    let mut firsti: c_int;
    let keepKeys: ScanKey;
    let unmarkKeys: ScanKey;
    let mut keepOrderProcs: *mut FmgrInfo = core::ptr::null_mut();
    let mut unmarkOrderProcs: *mut FmgrInfo = core::ptr::null_mut();
    let mut haveReqEquals: bool;
    let mut haveReqForward: bool;
    let mut haveReqBackward: bool;

    /*
     * Do an initial pass over so->keyData[] that determines which keys to
     * keep as required.  We expect so->keyData[] to still be in attribute
     * order when we're called (though we don't expect any particular order
     * among each attribute's keys).
     *
     * When both equality and inequality keys remain on a single attribute, we
     * *must* make sure that exactly one of the equalities remains required.
     * Any requiredness markings that we might leave on later keys/attributes
     * are predicated on there being required = keys on all prior columns.
     */
    unmarkikey = palloc0(((*so).numberOfKeys as usize) * size_of::<bool>()) as *mut bool;
    nunmark = 0;

    /* Set things up for first key's attribute */
    attno = (*(*so).keyData).sk_attno;
    firsti = 0;
    haveReqEquals = false;
    haveReqForward = false;
    haveReqBackward = false;
    for i in 0..(*so).numberOfKeys {
        let origkey: ScanKey = (*so).keyData.add(i as usize);

        if (*origkey).sk_attno != attno {
            /* Reset for next attribute */
            attno = (*origkey).sk_attno;
            firsti = i;

            haveReqEquals = false;
            haveReqForward = false;
            haveReqBackward = false;
        }

        /* Equalities get priority over inequalities */
        if haveReqEquals {
            /*
             * We already found the first "=" key for this attribute.  We've
             * already decided that all its other keys will be unmarked.
             */
            Assert!((*origkey).sk_flags as u32 & SK_SEARCHNULL as u32 == 0);
            *unmarkikey.add(i as usize) = true;
            nunmark += 1;
            continue;
        } else if ((*origkey).sk_flags as u32 & SK_BT_REQFWD != 0) &&
                  ((*origkey).sk_flags as u32 & SK_BT_REQBKWD != 0)
        {
            /*
             * Found the first "=" key for attno.  All other attno keys will
             * be unmarked.
             */
            Assert!((*origkey).sk_strategy == BTEqualStrategyNumber);

            haveReqEquals = true;
            for j in firsti..i {
                /* Unmark any prior inequality keys on attno after all */
                if !*unmarkikey.add(j as usize) {
                    *unmarkikey.add(j as usize) = true;
                    nunmark += 1;
                }
            }
            continue;
        }

        /* Deal with inequalities next */
        if ((*origkey).sk_flags as u32 & SK_BT_REQFWD != 0) && !haveReqForward {
            haveReqForward = true;
            continue;
        } else if ((*origkey).sk_flags as u32 & SK_BT_REQBKWD != 0) && !haveReqBackward {
            haveReqBackward = true;
            continue;
        }

        /*
         * We have either a redundant inequality key that will be unmarked, or
         * we have a key that wasn't marked required in the first place
         */
        *unmarkikey.add(i as usize) = true;
        nunmark += 1;
    }

    /* Should only be called when _bt_compare_scankey_args reported failure */
    Assert!(nunmark > 0);

    /*
     * Next, allocate temp arrays: one for required keys that'll remain
     * required, the other for all remaining keys
     */
    unmarkKeys = palloc((nunmark as usize) * size_of::<ScanKeyData>()) as ScanKey;
    keepKeys = palloc((((*so).numberOfKeys - nunmark) as usize) * size_of::<ScanKeyData>()) as ScanKey;
    nunmarked = 0;
    nkept = 0;
    if (*so).numArrayKeys != 0 {
        unmarkOrderProcs = palloc((nunmark as usize) * size_of::<FmgrInfo>()) as *mut FmgrInfo;
        keepOrderProcs = palloc((((*so).numberOfKeys - nunmark) as usize) * size_of::<FmgrInfo>()) as *mut FmgrInfo;
    }

    /*
     * Next, copy the contents of so->keyData[] into the appropriate temp
     * array.
     *
     * Scans with = array keys need us to maintain invariants around the order
     * of so->orderProcs[] and so->arrayKeys[] relative to so->keyData[].  See
     * _bt_preprocess_array_keys_final for a full explanation.
     */
    for i in 0..(*so).numberOfKeys {
        let origkey: ScanKey = (*so).keyData.add(i as usize);
        let unmark: ScanKey;

        if !*unmarkikey.add(i as usize) {
            /*
             * Key gets to keep its original requiredness markings.
             *
             * Key will stay in its original position, unless we're going to
             * unmark an earlier key (in which case this key gets moved back).
             */
            memcpy(
                keepKeys.add(nkept as usize) as *mut c_void,
                origkey as *const c_void,
                size_of::<ScanKeyData>(),
            );

            if (*so).numArrayKeys != 0 {
                if !keyDataMap.is_null() {
                    *keyDataMap.add(i as usize) = nkept;
                }
                memcpy(
                    keepOrderProcs.add(nkept as usize) as *mut c_void,
                    (*so).orderProcs.add(i as usize) as *const c_void,
                    size_of::<FmgrInfo>(),
                );
            }

            nkept += 1;
            continue;
        }

        /*
         * Key will be unmarked as needed, and moved to the end of the array,
         * next to other keys that will become (or always were) nonrequired
         */
        unmark = unmarkKeys.add(nunmarked as usize);
        memcpy(
            unmark as *mut c_void,
            origkey as *const c_void,
            size_of::<ScanKeyData>(),
        );

        if (*so).numArrayKeys != 0 {
            if !keyDataMap.is_null() {
                *keyDataMap.add(i as usize) = ((*so).numberOfKeys - nunmark) + nunmarked;
            }
            memcpy(
                unmarkOrderProcs.add(nunmarked as usize) as *mut c_void,
                (*so).orderProcs.add(i as usize) as *const c_void,
                size_of::<FmgrInfo>(),
            );
        }

        /*
         * Preprocessing only generates skip arrays when it knows that they'll
         * be the only required = key on the attr.  We'll never unmark them.
         */
        Assert!((*unmark).sk_flags as u32 & SK_BT_SKIP == 0);

        /*
         * Also shouldn't have to unmark an IS NULL or an IS NOT NULL key.
         * They aren't cross-type, so an incomplete opfamily can't matter.
         */
        Assert!((*unmark).sk_flags as u32 & SK_ISNULL as u32 == 0 ||
                (*unmark).sk_flags as u32 & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0);

        /* Clear requiredness flags on redundant key (and on any subkeys) */
        (*unmark).sk_flags &= !((SK_BT_REQFWD | SK_BT_REQBKWD) as c_int);
        if (*unmark).sk_flags as u32 & SK_ROW_HEADER as u32 != 0 {
            let mut subkey: ScanKey = DatumGetPointer((*unmark).sk_argument) as ScanKey;

            Assert!((*subkey).sk_strategy == (*unmark).sk_strategy);
            loop {
                Assert!((*subkey).sk_flags as u32 & SK_ROW_MEMBER as u32 != 0);
                (*subkey).sk_flags &= !((SK_BT_REQFWD | SK_BT_REQBKWD) as c_int);
                if (*subkey).sk_flags as u32 & SK_ROW_END as u32 != 0 {
                    break;
                }
                subkey = subkey.add(1);
            }
        }

        nunmarked += 1;
    }

    /* Copy both temp arrays back into so->keyData[] to reorder */
    Assert!(nkept == (*so).numberOfKeys - nunmark);
    Assert!(nunmarked == nunmark);
    memcpy(
        (*so).keyData as *mut c_void,
        keepKeys as *const c_void,
        size_of::<ScanKeyData>() * nkept as usize,
    );
    memcpy(
        (*so).keyData.add(nkept as usize) as *mut c_void,
        unmarkKeys as *const c_void,
        size_of::<ScanKeyData>() * nunmarked as usize,
    );

    /* Done with temp arrays */
    pfree(unmarkikey as *mut c_void);
    pfree(keepKeys as *mut c_void);
    pfree(unmarkKeys as *mut c_void);

    /*
     * Now copy so->orderProcs[] temp entries needed by scans with = array
     * keys back (just like with the so->keyData[] temp arrays)
     */
    if (*so).numArrayKeys != 0 {
        memcpy(
            (*so).orderProcs as *mut c_void,
            keepOrderProcs as *const c_void,
            size_of::<FmgrInfo>() * nkept as usize,
        );
        memcpy(
            (*so).orderProcs.add(nkept as usize) as *mut c_void,
            unmarkOrderProcs as *const c_void,
            size_of::<FmgrInfo>() * nunmarked as usize,
        );

        /* Also fix-up array->scan_key references */
        for arridx in 0..(*so).numArrayKeys {
            let array: *mut BTArrayKeyInfo = (*so).arrayKeys.add(arridx as usize);
            (*array).scan_key = *keyDataMap.add((*array).scan_key as usize);
        }

        /*
         * Sort so->arrayKeys[] based on its new BTArrayKeyInfo.scan_key
         * offsets, so that its order matches so->keyData[] order as expected
         */
        qsort(
            (*so).arrayKeys as *mut c_void,
            (*so).numArrayKeys as usize,
            size_of::<BTArrayKeyInfo>(),
            _bt_reorder_array_cmp,
        );

        /* Done with temp arrays */
        pfree(unmarkOrderProcs as *mut c_void);
        pfree(keepOrderProcs as *mut c_void);
    }
}

/*
 * qsort comparator for reordering so->arrayKeys[] BTArrayKeyInfo entries
 */
unsafe extern "C" fn _bt_reorder_array_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let arraya: *const BTArrayKeyInfo = a as *const BTArrayKeyInfo;
    let arrayb: *const BTArrayKeyInfo = b as *const BTArrayKeyInfo;

    pg_cmp_s32((*arraya).scan_key, (*arrayb).scan_key)
}

/*
 *	_bt_preprocess_array_keys() -- Preprocess SK_SEARCHARRAY scan keys
 *
 * If there are any SK_SEARCHARRAY scan keys, deconstruct the array(s) and
 * set up BTArrayKeyInfo info for each one that is an equality-type key.
 * Returns modified scan keys as input for further, standard preprocessing.
 *
 * Currently we perform two kinds of preprocessing to deal with redundancies.
 * For inequality array keys, it's sufficient to find the extreme element
 * value and replace the whole array with that scalar value.  This eliminates
 * all but one array element as redundant.  Similarly, we are capable of
 * "merging together" multiple equality array keys (from two or more input
 * scan keys) into a single output scan key containing only the intersecting
 * array elements.  This can eliminate many redundant array elements, as well
 * as eliminating whole array scan keys as redundant.  It can also allow us to
 * detect contradictory quals.
 *
 * Caller must pass *new_numberOfKeys to give us a way to change the number of
 * scan keys that caller treats as input to standard preprocessing steps.  The
 * returned array is smaller than scan->keyData[] when we could eliminate a
 * redundant array scan key (redundant with another array scan key).  It is
 * convenient for _bt_preprocess_keys caller to have to deal with no more than
 * one equality strategy array scan key per index attribute.  We'll always be
 * able to set things up that way when complete opfamilies are used.
 *
 * We're also responsible for generating skip arrays (and their associated
 * scan keys) here.  This enables skip scan.  We do this for index attributes
 * that initially lacked an equality condition within scan->keyData[], iff
 * doing so allows a later scan key (that was passed to us in scan->keyData[])
 * to be marked required by our _bt_preprocess_keys caller.
 *
 * We set the scan key references from the scan's BTArrayKeyInfo info array to
 * offsets into the temp modified input array returned to caller.  Scans that
 * have array keys should call _bt_preprocess_array_keys_final when standard
 * preprocessing steps are complete.  This will convert the scan key offset
 * references into references to the scan's so->keyData[] output scan keys.
 *
 * Note: the reason we need to return a temp scan key array, rather than just
 * modifying scan->keyData[], is that callers are permitted to call btrescan
 * without supplying a new set of scankey data.  Certain other preprocessing
 * routines (e.g., _bt_fix_scankey_strategy) _can_ modify scan->keyData[], but
 * we can't make that work here because our modifications are non-idempotent.
 */
pub unsafe fn _bt_preprocess_array_keys(
    scan: IndexScanDesc,
    new_numberOfKeys: *mut c_int,
) -> ScanKey {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let rel: Relation = (*scan).indexRelation;
    let indoption: *mut int16 = (*rel).rd_indoption;
    let mut skip_eq_ops: [Oid; INDEX_MAX_KEYS as usize] = [0; INDEX_MAX_KEYS as usize];
    let mut numArrayKeys: c_int;
    let mut numSkipArrayKeys: c_int = 0;
    let mut numArrayKeyData: c_int;
    let mut attno_skip: AttrNumber = 1;
    let mut origarrayatt: c_int = InvalidAttrNumber as c_int;
    let mut origarraykey: c_int = -1;
    let mut origelemtype: Oid = InvalidOid;
    let mut oldContext: MemoryContext;
    let arrayKeyData: ScanKey; /* modified copy of scan->keyData */

    /*
     * Check the number of input array keys within scan->keyData[] input keys
     * (also checks if we should add extra skip arrays based on input keys)
     */
    numArrayKeys = _bt_num_array_keys(scan, skip_eq_ops.as_mut_ptr(), &raw mut numSkipArrayKeys);
    (*so).skipScan = numSkipArrayKeys > 0;

    /* Quit if nothing to do. */
    if numArrayKeys == 0 {
        return std::ptr::null_mut();
    }

    /*
     * Estimated final size of arrayKeyData[] array we'll return to our caller
     * is the size of the original scan->keyData[] input array, plus space for
     * any additional skip array scan keys we'll need to generate below
     */
    numArrayKeyData = (*scan).numberOfKeys + numSkipArrayKeys;

    /*
     * Make a scan-lifespan context to hold array-associated data, or reset it
     * if we already have one from a previous rescan cycle.
     */
    if (*so).arrayContext.is_null() {
        (*so).arrayContext = AllocSetContextCreate!(
            CurrentMemoryContext,
            b"BTree array context\0".as_ptr() as *const c_char,
            ALLOCSET_SMALL_SIZES
        );
    } else {
        MemoryContextReset((*so).arrayContext);
    }

    oldContext = MemoryContextSwitchTo((*so).arrayContext);

    /* Create output scan keys in the workspace context */
    arrayKeyData = palloc(numArrayKeyData as usize * size_of::<ScanKeyData>()) as ScanKey;

    /* Allocate space for per-array data in the workspace context */
    (*so).arrayKeys =
        palloc(numArrayKeys as usize * size_of::<BTArrayKeyInfo>()) as *mut BTArrayKeyInfo;

    /* Allocate space for ORDER procs used to help _bt_checkkeys */
    (*so).orderProcs =
        palloc(numArrayKeyData as usize * size_of::<FmgrInfo>()) as *mut FmgrInfo;

    numArrayKeys = 0;
    numArrayKeyData = 0;
    let mut input_ikey: c_int = 0;
    while input_ikey < (*scan).numberOfKeys {
        let inkey: ScanKey = (*scan).keyData.add(input_ikey as usize);
        let cur: ScanKey;
        let mut sortproc: FmgrInfo = core::mem::zeroed();
        let mut sortprocp: *mut FmgrInfo = &raw mut sortproc;
        let mut elemtype: Oid;
        let mut reverse: bool;
        let mut arrayval: *mut ArrayType;
        let mut elmlen: int16 = 0;
        let mut elmbyval: bool = false;
        let mut elmalign: c_char = 0;
        let mut num_elems: c_int = 0;
        let mut elem_values: *mut Datum = std::ptr::null_mut();
        let mut elem_nulls: *mut bool = std::ptr::null_mut();
        let mut num_nonnulls: c_int;

        /* set up next output scan key */
        cur = arrayKeyData.add(numArrayKeyData as usize);

        /* Backfill skip arrays for attrs < or <= input key's attr? */
        while numSkipArrayKeys != 0 && attno_skip <= (*inkey).sk_attno {
            let opfamily: Oid = *(*rel).rd_opfamily.add(attno_skip as usize - 1);
            let opcintype: Oid = *(*rel).rd_opcintype.add(attno_skip as usize - 1);
            let collation: Oid = *(*rel).rd_indcollation.add(attno_skip as usize - 1);
            let eq_op: Oid = skip_eq_ops[attno_skip as usize - 1];
            let attr: *mut CompactAttribute;
            let cmp_proc: RegProcedure;

            if !OidIsValid(eq_op) {
                /*
                 * Attribute already has an = input key, so don't output a
                 * skip array for attno_skip.  Just copy attribute's = input
                 * key into arrayKeyData[] once outside this inner loop.
                 *
                 * Note: When we get here there must be a later attribute that
                 * lacks an equality input key, and still needs a skip array
                 * (if there wasn't then numSkipArrayKeys would be 0 by now).
                 */
                Assert!(attno_skip == (*inkey).sk_attno);
                /* inkey can't be last input key to be marked required: */
                Assert!(input_ikey < (*scan).numberOfKeys - 1);

                attno_skip += 1;
                break;
            }

            cmp_proc = get_opcode(eq_op);
            if !RegProcedureIsValid(cmp_proc) {
                elog!(
                    ERROR,
                    "missing oprcode for skipping equals operator {}",
                    eq_op
                );
            }

            ScanKeyEntryInitialize(
                cur,
                (SK_SEARCHARRAY as u32 | SK_BT_SKIP) as c_int, /* flags */
                attno_skip,                              /* skipped att number */
                BTEqualStrategyNumber,                   /* equality strategy */
                InvalidOid,                              /* opclass input subtype */
                collation,                               /* index column's collation */
                cmp_proc,                                /* equality operator's proc */
                0 as Datum,                              /* constant */
            );

            /* Initialize generic BTArrayKeyInfo fields */
            (*(*so).arrayKeys.add(numArrayKeys as usize)).scan_key = numArrayKeyData;
            (*(*so).arrayKeys.add(numArrayKeys as usize)).num_elems = -1;

            /* Initialize skip array specific BTArrayKeyInfo fields */
            attr = TupleDescCompactAttr(RelationGetDescr(rel), (attno_skip - 1) as c_int);
            reverse = (*indoption.add(attno_skip as usize - 1) & INDOPTION_DESC) != 0;
            (*(*so).arrayKeys.add(numArrayKeys as usize)).attlen = (*attr).attlen as _;
            (*(*so).arrayKeys.add(numArrayKeys as usize)).attbyval = (*attr).attbyval;
            (*(*so).arrayKeys.add(numArrayKeys as usize)).null_elem = true; /* for now */
            (*(*so).arrayKeys.add(numArrayKeys as usize)).sksup =
                PrepareSkipSupportFromOpclass(opfamily, opcintype, reverse);
            (*(*so).arrayKeys.add(numArrayKeys as usize)).low_compare = std::ptr::null_mut(); /* for now */
            (*(*so).arrayKeys.add(numArrayKeys as usize)).high_compare = std::ptr::null_mut(); /* for now */

            /*
             * We'll need a 3-way ORDER proc.  Set that up now.
             */
            _bt_setup_array_cmp(
                scan,
                cur,
                opcintype,
                &raw mut *(*so).orderProcs.add(numArrayKeyData as usize),
                std::ptr::null_mut(),
            );

            numArrayKeys += 1;
            numArrayKeyData += 1; /* keep this scan key/array */

            /* set up next output scan key */
            let cur = arrayKeyData.add(numArrayKeyData as usize);
            let _ = cur; /* used below */

            /* remember having output this skip array and scan key */
            numSkipArrayKeys -= 1;
            attno_skip += 1;
        }

        /* set up cur again after potential inner loop */
        let cur: ScanKey = arrayKeyData.add(numArrayKeyData as usize);

        /*
         * Provisionally copy scan key into arrayKeyData[] array we'll return
         * to _bt_preprocess_keys caller
         */
        *cur = *inkey;

        if (*cur).sk_flags as u32 & SK_SEARCHARRAY as u32 == 0 {
            numArrayKeyData += 1; /* keep this non-array scan key */
            input_ikey += 1;
            continue;
        }

        /*
         * Process SAOP array scan key
         */
        Assert!(
            (*cur).sk_flags as u32 & (SK_ROW_HEADER | SK_SEARCHNULL | SK_SEARCHNOTNULL) as u32 == 0
        );

        /* If array is null as a whole, the scan qual is unsatisfiable */
        if (*cur).sk_flags as u32 & SK_ISNULL as u32 != 0 {
            (*so).qual_ok = false;
            break;
        }

        /*
         * Deconstruct the array into elements
         */
        arrayval = DatumGetArrayTypeP((*cur).sk_argument);
        /* We could cache this data, but not clear it's worth it */
        get_typlenbyvalalign(
            ARR_ELEMTYPE(arrayval),
            &raw mut elmlen,
            &raw mut elmbyval,
            &raw mut elmalign,
        );
        deconstruct_array(
            arrayval,
            ARR_ELEMTYPE(arrayval),
            elmlen,
            elmbyval,
            elmalign,
            &raw mut elem_values,
            &raw mut elem_nulls,
            &raw mut num_elems,
        );

        /*
         * Compress out any null elements.  We can ignore them since we assume
         * all btree operators are strict.
         */
        num_nonnulls = 0;
        let mut j: c_int = 0;
        while j < num_elems {
            if !*elem_nulls.add(j as usize) {
                *elem_values.add(num_nonnulls as usize) = *elem_values.add(j as usize);
                num_nonnulls += 1;
            }
            j += 1;
        }

        /* We could pfree(elem_nulls) now, but not worth the cycles */

        /* If there's no non-nulls, the scan qual is unsatisfiable */
        if num_nonnulls == 0 {
            (*so).qual_ok = false;
            break;
        }

        /*
         * Determine the nominal datatype of the array elements.  We have to
         * support the convention that sk_subtype == InvalidOid means the
         * opclass input type; this is a hack to simplify life for
         * ScanKeyInit().
         */
        elemtype = (*cur).sk_subtype;
        if elemtype == InvalidOid {
            elemtype = *(*rel).rd_opcintype.add((*cur).sk_attno as usize - 1);
        }

        /*
         * If the comparison operator is not equality, then the array qual
         * degenerates to a simple comparison against the smallest or largest
         * non-null array element, as appropriate.
         */
        match (*cur).sk_strategy {
            BTLessStrategyNumber | BTLessEqualStrategyNumber => {
                (*cur).sk_argument = _bt_find_extreme_element(
                    scan,
                    cur,
                    elemtype,
                    BTGreaterStrategyNumber,
                    elem_values,
                    num_nonnulls,
                );
                numArrayKeyData += 1; /* keep this transformed scan key */
                input_ikey += 1;
                continue;
            }
            BTEqualStrategyNumber => {
                /* proceed with rest of loop */
            }
            BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
                (*cur).sk_argument = _bt_find_extreme_element(
                    scan,
                    cur,
                    elemtype,
                    BTLessStrategyNumber,
                    elem_values,
                    num_nonnulls,
                );
                numArrayKeyData += 1; /* keep this transformed scan key */
                input_ikey += 1;
                continue;
            }
            _ => {
                elog!(
                    ERROR,
                    "unrecognized StrategyNumber: {}",
                    (*cur).sk_strategy as c_int
                );
            }
        }

        /*
         * We'll need a 3-way ORDER proc to perform binary searches for the
         * next matching array element.  Set that up now.
         *
         * Array scan keys with cross-type equality operators will require a
         * separate same-type ORDER proc for sorting their array.  Otherwise,
         * sortproc just points to the same proc used during binary searches.
         */
        _bt_setup_array_cmp(
            scan,
            cur,
            elemtype,
            &raw mut *(*so).orderProcs.add(numArrayKeyData as usize),
            &raw mut sortprocp,
        );

        /*
         * Sort the non-null elements and eliminate any duplicates.  We must
         * sort in the same ordering used by the index column, so that the
         * arrays can be advanced in lockstep with the scan's progress through
         * the index's key space.
         */
        reverse = (*indoption.add((*cur).sk_attno as usize - 1) & INDOPTION_DESC) != 0;
        let num_elems_sorted =
            _bt_sort_array_elements(cur, sortprocp, reverse, elem_values, num_nonnulls);
        let num_elems = num_elems_sorted;

        if origarrayatt == (*cur).sk_attno as c_int {
            let orig: *mut BTArrayKeyInfo =
                &raw mut *(*so).arrayKeys.add(origarraykey as usize);

            /*
             * This array scan key is redundant with a previous equality
             * operator array scan key.  Merge the two arrays together to
             * eliminate contradictory non-intersecting elements (or try to).
             *
             * We merge this next array back into attribute's original array.
             */
            Assert!(
                (*arrayKeyData.add((*orig).scan_key as usize)).sk_attno == (*cur).sk_attno
            );
            Assert!(
                (*arrayKeyData.add((*orig).scan_key as usize)).sk_collation == (*cur).sk_collation
            );
            if _bt_merge_arrays(
                scan,
                cur,
                sortprocp,
                reverse,
                origelemtype,
                elemtype,
                (*orig).elem_values,
                &raw mut (*orig).num_elems,
                elem_values,
                num_elems,
            ) {
                /* Successfully eliminated this array */
                pfree(elem_values as *mut c_void);

                /*
                 * If no intersecting elements remain in the original array,
                 * the scan qual is unsatisfiable
                 */
                if (*orig).num_elems == 0 {
                    (*so).qual_ok = false;
                    break;
                }

                /* Throw away this scan key/array */
                input_ikey += 1;
                continue;
            }

            /*
             * Unable to merge this array with previous array due to a lack of
             * suitable cross-type opfamily support.  Will need to keep both
             * scan keys/arrays.
             */
        } else {
            /*
             * This array is the first for current index attribute.
             *
             * If it turns out to not be the last array (that is, if the next
             * array is redundantly applied to this same index attribute),
             * we'll then treat this array as the attribute's "original" array
             * when merging.
             */
            origarrayatt = (*cur).sk_attno as c_int;
            origarraykey = numArrayKeys;
            origelemtype = elemtype;
        }

        /* Initialize generic BTArrayKeyInfo fields */
        (*(*so).arrayKeys.add(numArrayKeys as usize)).scan_key = numArrayKeyData;
        (*(*so).arrayKeys.add(numArrayKeys as usize)).num_elems = num_elems;

        /* Initialize SAOP array specific BTArrayKeyInfo fields */
        (*(*so).arrayKeys.add(numArrayKeys as usize)).elem_values = elem_values;
        (*(*so).arrayKeys.add(numArrayKeys as usize)).cur_elem = -1; /* i.e. invalid */

        numArrayKeys += 1;
        numArrayKeyData += 1; /* keep this scan key/array */

        input_ikey += 1;
    }

    Assert!(numSkipArrayKeys == 0 || !(*so).qual_ok);

    /* Set final number of equality-type array keys */
    (*so).numArrayKeys = numArrayKeys;
    /* Set number of scan keys in arrayKeyData[] */
    *new_numberOfKeys = numArrayKeyData;

    MemoryContextSwitchTo(oldContext);

    arrayKeyData
}

/*
 *	_bt_preprocess_array_keys_final() -- fix up array scan key references
 *
 * When _bt_preprocess_array_keys performed initial array preprocessing, it
 * set each array's array->scan_key to its scankey's arrayKeyData[] offset.
 * This function handles translation of the scan key references from the
 * BTArrayKeyInfo info array, from input scan key references (to the keys in
 * arrayKeyData[]), into output references (to the keys in so->keyData[]).
 * Caller's keyDataMap[] array tells us how to perform this remapping.
 *
 * Also finalizes so->orderProcs[] for the scan.  Arrays already have an ORDER
 * proc, which might need to be repositioned to its so->keyData[]-wise offset
 * (very much like the remapping that we apply to array->scan_key references).
 * Non-array equality strategy scan keys (that survived preprocessing) don't
 * yet have an so->orderProcs[] entry, so we set one for them here.
 *
 * Also converts single-element array scan keys into equivalent non-array
 * equality scan keys, which decrements so->numArrayKeys.  It's possible that
 * this will leave this new btrescan without any arrays at all.  This isn't
 * necessary for correctness; it's just an optimization.  Non-array equality
 * scan keys are slightly faster than equivalent array scan keys at runtime.
 */
pub unsafe fn _bt_preprocess_array_keys_final(scan: IndexScanDesc, keyDataMap: *mut c_int) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let rel: Relation = (*scan).indexRelation;
    let mut arrayidx: c_int = 0;
    /* last_equal_output_ikey is PG_USED_FOR_ASSERTS_ONLY */
    let mut last_equal_output_ikey: c_int = -1;

    Assert!((*so).qual_ok);

    /*
     * Nothing for us to do when _bt_preprocess_array_keys only had to deal
     * with array inequalities
     */
    if (*so).numArrayKeys == 0 {
        return;
    }

    let mut output_ikey: c_int = 0;
    while output_ikey < (*so).numberOfKeys {
        let outkey: ScanKey = (*so).keyData.add(output_ikey as usize);
        let input_ikey: c_int;
        let mut found: bool = false; /* PG_USED_FOR_ASSERTS_ONLY */

        Assert!((*outkey).sk_strategy != InvalidStrategy);

        if (*outkey).sk_strategy != BTEqualStrategyNumber {
            output_ikey += 1;
            continue;
        }

        input_ikey = *keyDataMap.add(output_ikey as usize);

        Assert!(last_equal_output_ikey < output_ikey);
        Assert!(last_equal_output_ikey < input_ikey);
        last_equal_output_ikey = output_ikey;

        /*
         * We're lazy about looking up ORDER procs for non-array keys, since
         * not all input keys become output keys.  Take care of it now.
         */
        if (*outkey).sk_flags as u32 & SK_SEARCHARRAY as u32 == 0 {
            let elemtype: Oid;

            /* No need for an ORDER proc given an IS NULL scan key */
            if (*outkey).sk_flags as u32 & SK_SEARCHNULL as u32 != 0 {
                output_ikey += 1;
                continue;
            }

            /*
             * A non-required scan key doesn't need an ORDER proc, either
             * (unless it's associated with an array, which this one isn't)
             */
            if (*outkey).sk_flags as u32 & SK_BT_REQFWD == 0 {
                output_ikey += 1;
                continue;
            }

            elemtype = (*outkey).sk_subtype;
            let elemtype = if elemtype == InvalidOid {
                *(*rel).rd_opcintype.add((*outkey).sk_attno as usize - 1)
            } else {
                elemtype
            };

            _bt_setup_array_cmp(
                scan,
                outkey,
                elemtype,
                &raw mut *(*so).orderProcs.add(output_ikey as usize),
                std::ptr::null_mut(),
            );
            output_ikey += 1;
            continue;
        }

        /*
         * Reorder existing array scan key so->orderProcs[] entries.
         *
         * Doing this in-place is safe because preprocessing is required to
         * output all equality strategy scan keys in original input order
         * (among each group of entries against the same index attribute).
         * This is also the order that the arrays themselves appear in.
         */
        *(*so).orderProcs.add(output_ikey as usize) =
            *(*so).orderProcs.add(input_ikey as usize);

        /* Fix-up array->scan_key references for arrays */
        while arrayidx < (*so).numArrayKeys {
            let array: *mut BTArrayKeyInfo = (*so).arrayKeys.add(arrayidx as usize);

            /*
             * All skip arrays must be marked required, and final column can
             * never have a skip array
             */
            Assert!((*array).num_elems > 0 || (*array).num_elems == -1);
            Assert!(
                (*array).num_elems != -1
                    || (*outkey).sk_flags as u32 & SK_BT_REQFWD != 0
            );
            Assert!(
                (*array).num_elems != -1
                    || (*outkey).sk_attno < IndexRelationGetNumberOfKeyAttributes(rel) as i16
            );

            if (*array).scan_key == input_ikey {
                /* found it */
                (*array).scan_key = output_ikey;
                found = true;

                /*
                 * Transform array scan keys that have exactly 1 element
                 * remaining (following all prior preprocessing) into
                 * equivalent non-array scan keys.
                 */
                if (*array).num_elems == 1 {
                    (*outkey).sk_flags &= !(SK_SEARCHARRAY as c_int);
                    (*outkey).sk_argument = *(*array).elem_values;
                    (*so).numArrayKeys -= 1;

                    /* If we're out of array keys, we can quit right away */
                    if (*so).numArrayKeys == 0 {
                        return;
                    }

                    /* Shift other arrays forward */
                    memmove(
                        array as *mut c_void,
                        array.add(1) as *const c_void,
                        size_of::<BTArrayKeyInfo>()
                            * ((*so).numArrayKeys - arrayidx) as usize,
                    );

                    /*
                     * Don't increment arrayidx (there was an entry that was
                     * just shifted forward to the offset at arrayidx, which
                     * will still need to be matched)
                     */
                } else {
                    /*
                     * Any skip array low_compare and high_compare scan keys
                     * are now final.  Transform the array's > low_compare key
                     * into a >= key (and < high_compare keys into a <= key).
                     */
                    if (*array).num_elems == -1
                        && !(*array).sksup.is_null()
                        && !(*array).null_elem
                    {
                        _bt_skiparray_strat_adjust(scan, outkey, array);
                    }

                    /* Match found, so done with this array */
                    arrayidx += 1;
                }

                break;
            }

            arrayidx += 1;
        }

        Assert!(found);

        output_ikey += 1;
    }

    /*
     * Parallel index scans require space in shared memory to store the
     * current array elements (for arrays kept by preprocessing) to schedule
     * the next primitive index scan.  The underlying structure is protected
     * using an LWLock, so defensively limit its size.  In practice this can
     * only affect parallel scans that use an incomplete opfamily.
     */
    if !(*scan).parallel_scan.is_null() && (*so).numArrayKeys > INDEX_MAX_KEYS as c_int {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
            errmsg!(
                "number of array scan keys left by preprocessing ({}) exceeds the maximum allowed by parallel btree index scans ({})",
                (*so).numArrayKeys,
                INDEX_MAX_KEYS
            )
        );
    }
}

/*
 *	_bt_num_array_keys() -- determine # of BTArrayKeyInfo entries
 *
 * _bt_preprocess_array_keys helper function.  Returns the estimated size of
 * the scan's BTArrayKeyInfo array, which is guaranteed to be large enough to
 * fit every so->arrayKeys[] entry.
 *
 * Also sets *numSkipArrayKeys_out to the number of skip arrays caller must
 * add to the scan keys it'll output.  Caller must add this many skip arrays:
 * one array for each of the most significant attributes that lack a = input
 * key (IS NULL keys count as = input keys here).  The specific attributes
 * that need skip arrays are indicated by initializing skip_eq_ops_out[] arg
 * 0-based attribute offset to a valid = op strategy Oid.  We'll only ever set
 * skip_eq_ops_out[] entries to InvalidOid for attributes that already have an
 * equality key in scan->keyData[] input keys -- and only when there's some
 * later "attribute gap" for us to "fill-in" with a skip array.
 *
 * We're optimistic about skipping working out: we always add exactly the skip
 * arrays needed to maximize the number of input scan keys that can ultimately
 * be marked as required to continue the scan (but no more).  Given a
 * multi-column index on (a, b, c, d), we add skip arrays as follows:
 *
 * Input keys                        Output keys (after all preprocessing)
 * ----------                        -------------------------------------
 * a = 1                             a = 1 (no skip arrays)
 * b = 42                            skip a AND b = 42
 * a = 1 AND b = 42                  a = 1 AND b = 42 (no skip arrays)
 * a >= 1 AND b = 42                 range skip a AND b = 42
 * a = 1 AND b > 42                  a = 1 AND b > 42 (no skip arrays)
 * a >= 1 AND a <= 3 AND b = 42      range skip a AND b = 42
 * a = 1 AND c <= 27                 a = 1 AND skip b AND c <= 27
 * a = 1 AND d >= 1                  a = 1 AND skip b AND skip c AND d >= 1
 * a = 1 AND b >= 42 AND d > 1       a = 1 AND range skip b AND skip c AND d > 1
 */
pub unsafe fn _bt_num_array_keys(
    scan: IndexScanDesc,
    skip_eq_ops_out: *mut Oid,
    numSkipArrayKeys_out: *mut c_int,
) -> c_int {
    let rel: Relation = (*scan).indexRelation;
    let mut attno_skip: AttrNumber = 1;
    let mut attno_inkey: AttrNumber = 1;
    let mut attno_has_equal: bool = false;
    let mut attno_has_rowcompare: bool = false;
    let mut numSAOPArrayKeys: c_int;
    let mut numSkipArrayKeys: c_int;
    let mut prev_numSkipArrayKeys: c_int;

    Assert!((*scan).numberOfKeys != 0);

    /* Initial pass over input scan keys counts the number of SAOP arrays */
    numSAOPArrayKeys = 0;
    *numSkipArrayKeys_out = 0;
    prev_numSkipArrayKeys = 0;
    numSkipArrayKeys = 0;
    for i in 0..(*scan).numberOfKeys {
        let inkey: ScanKey = (*scan).keyData.add(i as usize);

        if (*inkey).sk_flags as u32 & SK_SEARCHARRAY as u32 != 0 {
            numSAOPArrayKeys += 1;
        }
    }

    #[cfg(feature = "DEBUG_DISABLE_SKIP_SCAN")]
    {
        /* don't attempt to add skip arrays */
        return numSAOPArrayKeys;
    }

    let mut i: c_int = 0;
    loop {
        let inkey: ScanKey = if i < (*scan).numberOfKeys {
            (*scan).keyData.add(i as usize)
        } else {
            /* sentinel: past end */
            (*scan).keyData.add((*scan).numberOfKeys as usize - 1)
        };

        /*
         * Backfill skip arrays for any wholly omitted attributes prior to
         * attno_inkey
         */
        while attno_skip < attno_inkey {
            let opfamily: Oid = *(*rel).rd_opfamily.add(attno_skip as usize - 1);
            let opcintype: Oid = *(*rel).rd_opcintype.add(attno_skip as usize - 1);

            /* Look up input opclass's equality operator (might fail) */
            *skip_eq_ops_out.add(attno_skip as usize - 1) = get_opfamily_member(
                opfamily,
                opcintype,
                opcintype,
                BTEqualStrategyNumber,
            );
            if !OidIsValid(*skip_eq_ops_out.add(attno_skip as usize - 1)) {
                /*
                 * Cannot generate a skip array for this or later attributes
                 * (input opclass lacks an equality strategy operator)
                 */
                *numSkipArrayKeys_out = prev_numSkipArrayKeys;
                return numSAOPArrayKeys + prev_numSkipArrayKeys;
            }

            /* plan on adding a backfill skip array for this attribute */
            numSkipArrayKeys += 1;
            attno_skip += 1;
        }

        prev_numSkipArrayKeys = numSkipArrayKeys;

        /*
         * Stop once past the final input scan key.  We deliberately never add
         * a skip array for the last input scan key's attribute -- even when
         * there are only inequality keys on that attribute.
         */
        if i == (*scan).numberOfKeys {
            break;
        }

        /*
         * Later preprocessing steps cannot merge a RowCompare into a skip
         * array, so stop adding skip arrays once we see one.  (Note that we
         * can backfill skip arrays before a RowCompare, which will allow keys
         * up to and including the RowCompare to be marked required.)
         *
         * Skip arrays work by maintaining a current array element value,
         * which anchors lower-order keys via an implied equality constraint.
         * This is incompatible with the current nbtree row comparison design,
         * which compares all columns together, as an indivisible group.
         * Alternative designs that can be used alongside skip arrays are
         * possible, but it's not clear that they're really worth pursuing.
         *
         * A RowCompare qual "(a, b, c) > (10, 'foo', 42)" is equivalent to
         * "(a=10 AND b='foo' AND c>42) OR (a=10 AND b>'foo') OR (a>10)".
         * Decomposing this RowCompare into these 3 disjuncts allows each
         * disjunct to be executed as a separate "single value" index scan.
         * That'll give all 3 scans the ability to add skip arrays in the
         * usual way (when there are any scalar keys after the RowCompare).
         * Under this scheme, a qual "(a, b, c) > (10, 'foo', 42) AND d = 99"
         * performs 3 separate scans, each of which can mark keys up to and
         * including its "d = 99" key as required to continue the scan.
         */
        if attno_has_rowcompare {
            break;
        }

        /*
         * Now consider next attno_inkey (or keep going if this is an
         * additional scan key against the same attribute)
         */
        if attno_inkey < (*inkey).sk_attno {
            /*
             * Now add skip array for previous scan key's attribute, though
             * only if the attribute has no equality strategy scan keys
             */
            if attno_has_equal {
                /* Attributes with an = key must have InvalidOid eq_op set */
                *skip_eq_ops_out.add(attno_skip as usize - 1) = InvalidOid;
            } else {
                let opfamily: Oid = *(*rel).rd_opfamily.add(attno_skip as usize - 1);
                let opcintype: Oid = *(*rel).rd_opcintype.add(attno_skip as usize - 1);

                /* Look up input opclass's equality operator (might fail) */
                *skip_eq_ops_out.add(attno_skip as usize - 1) = get_opfamily_member(
                    opfamily,
                    opcintype,
                    opcintype,
                    BTEqualStrategyNumber,
                );

                if !OidIsValid(*skip_eq_ops_out.add(attno_skip as usize - 1)) {
                    /*
                     * Input opclass lacks an equality strategy operator, so
                     * don't generate a skip array that definitely won't work
                     */
                    break;
                }

                /* plan on adding a backfill skip array for this attribute */
                numSkipArrayKeys += 1;
            }

            /* Set things up for this new attribute */
            attno_skip += 1;
            attno_inkey = (*inkey).sk_attno;
            attno_has_equal = false;
        }

        /*
         * Track if this attribute's scan keys include any equality strategy
         * scan keys (IS NULL keys count as equality keys here).  Also track
         * if it has any RowCompare keys.
         */
        if (*inkey).sk_strategy == BTEqualStrategyNumber
            || (*inkey).sk_flags as u32 & SK_SEARCHNULL as u32 != 0
        {
            attno_has_equal = true;
        }
        if (*inkey).sk_flags as u32 & SK_ROW_HEADER as u32 != 0 {
            attno_has_rowcompare = true;
        }

        i += 1;
    }

    *numSkipArrayKeys_out = numSkipArrayKeys;
    numSAOPArrayKeys + numSkipArrayKeys
}

/*
 * _bt_find_extreme_element() -- get least or greatest array element
 *
 * scan and skey identify the index column, whose opfamily determines the
 * comparison semantics.  strat should be BTLessStrategyNumber to get the
 * least element, or BTGreaterStrategyNumber to get the greatest.
 */
pub unsafe fn _bt_find_extreme_element(
    scan: IndexScanDesc,
    skey: ScanKey,
    elemtype: Oid,
    strat: StrategyNumber,
    elems: *mut Datum,
    nelems: c_int,
) -> Datum {
    let rel: Relation = (*scan).indexRelation;
    let cmp_op: Oid;
    let cmp_proc: RegProcedure;
    let mut flinfo: FmgrInfo = core::mem::zeroed();
    let mut result: Datum;

    /*
     * Look up the appropriate comparison operator in the opfamily.
     *
     * Note: it's possible that this would fail, if the opfamily is
     * incomplete, but it seems quite unlikely that an opfamily would omit
     * non-cross-type comparison operators for any datatype that it supports
     * at all.
     */
    Assert!((*skey).sk_strategy != BTEqualStrategyNumber);
    Assert!(OidIsValid(elemtype));
    cmp_op = get_opfamily_member(
        *(*rel).rd_opfamily.add((*skey).sk_attno as usize - 1),
        elemtype,
        elemtype,
        strat,
    );
    if !OidIsValid(cmp_op) {
        elog!(
            ERROR,
            "missing operator {}({},{}) in opfamily {}",
            strat,
            elemtype,
            elemtype,
            *(*rel).rd_opfamily.add((*skey).sk_attno as usize - 1)
        );
    }
    cmp_proc = get_opcode(cmp_op);
    if !RegProcedureIsValid(cmp_proc) {
        elog!(ERROR, "missing oprcode for operator {}", cmp_op);
    }

    fmgr_info(cmp_proc, &raw mut flinfo);

    Assert!(nelems > 0);
    result = *elems;
    let mut i: c_int = 1;
    while i < nelems {
        if DatumGetBool(FunctionCall2Coll(
            &raw mut flinfo,
            (*skey).sk_collation,
            *elems.add(i as usize),
            result,
        )) {
            result = *elems.add(i as usize);
        }
        i += 1;
    }

    result
}

/*
 * _bt_setup_array_cmp() -- Set up array comparison functions
 *
 * Sets ORDER proc in caller's orderproc argument, which is used during binary
 * searches of arrays during the index scan.  Also sets a same-type ORDER proc
 * in caller's *sortprocp argument, which is used when sorting the array.
 *
 * Preprocessing calls here with all equality strategy scan keys (when scan
 * uses equality array keys), including those not associated with any array.
 * See _bt_advance_array_keys for an explanation of why it'll need to treat
 * simple scalar equality scan keys as degenerate single element arrays.
 *
 * Caller should pass an orderproc pointing to space that'll store the ORDER
 * proc for the scan, and a *sortprocp pointing to its own separate space.
 * When calling here for a non-array scan key, sortprocp arg should be NULL.
 *
 * In the common case where we don't need to deal with cross-type operators,
 * only one ORDER proc is actually required by caller.  We'll set *sortprocp
 * to point to the same memory that caller's orderproc continues to point to.
 * Otherwise, *sortprocp will continue to point to caller's own space.  Either
 * way, *sortprocp will point to a same-type ORDER proc (since that's the only
 * safe way to sort/deduplicate the array associated with caller's scan key).
 */
pub unsafe fn _bt_setup_array_cmp(
    scan: IndexScanDesc,
    skey: ScanKey,
    elemtype: Oid,
    orderproc: *mut FmgrInfo,
    sortprocp: *mut *mut FmgrInfo,
) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let rel: Relation = (*scan).indexRelation;
    let mut cmp_proc: RegProcedure;
    let opcintype: Oid = *(*rel).rd_opcintype.add((*skey).sk_attno as usize - 1);

    Assert!((*skey).sk_strategy == BTEqualStrategyNumber);
    Assert!(OidIsValid(elemtype));

    /*
     * If scankey operator is not a cross-type comparison, we can use the
     * cached comparison function; otherwise gotta look it up in the catalogs
     */
    if elemtype == opcintype {
        /* Set same-type ORDER procs for caller */
        *orderproc = *index_getprocinfo(rel, (*skey).sk_attno as c_int, BTORDER_PROC as uint32);
        if !sortprocp.is_null() {
            *sortprocp = orderproc;
        }

        return;
    }

    /*
     * Look up the appropriate cross-type comparison function in the opfamily.
     *
     * Use the opclass input type as the left hand arg type, and the array
     * element type as the right hand arg type (since binary searches use an
     * index tuple's attribute value to search for a matching array element).
     *
     * Note: it's possible that this would fail, if the opfamily is
     * incomplete, but only in cases where it's quite likely that _bt_first
     * would fail in just the same way (had we not failed before it could).
     */
    cmp_proc = get_opfamily_proc(
        *(*rel).rd_opfamily.add((*skey).sk_attno as usize - 1),
        opcintype,
        elemtype,
        BTORDER_PROC,
    );
    if !RegProcedureIsValid(cmp_proc) {
        elog!(
            ERROR,
            "missing support function {}({},{}) for attribute {} of index \"{}\"",
            BTORDER_PROC,
            opcintype,
            elemtype,
            (*skey).sk_attno,
            core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /* Set cross-type ORDER proc for caller */
    fmgr_info_cxt(cmp_proc, orderproc, (*so).arrayContext);

    /* Done if caller doesn't actually have an array they'll need to sort */
    if sortprocp.is_null() {
        return;
    }

    /*
     * Look up the appropriate same-type comparison function in the opfamily.
     *
     * Note: it's possible that this would fail, if the opfamily is
     * incomplete, but it seems quite unlikely that an opfamily would omit
     * non-cross-type comparison procs for any datatype that it supports at
     * all.
     */
    cmp_proc = get_opfamily_proc(
        *(*rel).rd_opfamily.add((*skey).sk_attno as usize - 1),
        elemtype,
        elemtype,
        BTORDER_PROC,
    );
    if !RegProcedureIsValid(cmp_proc) {
        elog!(
            ERROR,
            "missing support function {}({},{}) for attribute {} of index \"{}\"",
            BTORDER_PROC,
            elemtype,
            elemtype,
            (*skey).sk_attno,
            core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /* Set same-type ORDER proc for caller */
    fmgr_info_cxt(cmp_proc, *sortprocp, (*so).arrayContext);
}

/*
 * _bt_sort_array_elements() -- sort and de-dup array elements
 *
 * The array elements are sorted in-place, and the new number of elements
 * after duplicate removal is returned.
 *
 * skey identifies the index column whose opfamily determines the comparison
 * semantics, and sortproc is a corresponding ORDER proc.  If reverse is true,
 * we sort in descending order.
 */
pub unsafe fn _bt_sort_array_elements(
    skey: ScanKey,
    sortproc: *mut FmgrInfo,
    reverse: bool,
    elems: *mut Datum,
    nelems: c_int,
) -> c_int {
    let mut cxt: BTSortArrayContext = BTSortArrayContext {
        sortproc: std::ptr::null_mut(),
        collation: 0,
        reverse: false,
    };

    if nelems <= 1 {
        return nelems; /* no work to do */
    }

    /* Sort the array elements */
    cxt.sortproc = sortproc;
    cxt.collation = (*skey).sk_collation;
    cxt.reverse = reverse;
    qsort_arg(
        elems as *mut c_void,
        nelems as usize,
        size_of::<Datum>(),
        _bt_compare_array_elements,
        &raw mut cxt as *mut c_void,
    );

    /* Now scan the sorted elements and remove duplicates */
    qunique_arg(
        elems as *mut c_void,
        nelems as usize,
        size_of::<Datum>(),
        _bt_compare_array_elements,
        &raw mut cxt as *mut c_void,
    ) as c_int
}

/*
 * _bt_merge_arrays() -- merge next array's elements into an original array
 *
 * Called when preprocessing encounters a pair of array equality scan keys,
 * both against the same index attribute (during initial array preprocessing).
 * Merging reorganizes caller's original array (the left hand arg) in-place,
 * without ever copying elements from one array into the other. (Mixing the
 * elements together like this would be wrong, since they don't necessarily
 * use the same underlying element type, despite all the other similarities.)
 *
 * Both arrays must have already been sorted and deduplicated by calling
 * _bt_sort_array_elements.  sortproc is the same-type ORDER proc that was
 * just used to sort and deduplicate caller's "next" array.  We'll usually be
 * able to reuse that order PROC to merge the arrays together now.  If not,
 * then we'll perform a separate ORDER proc lookup.
 *
 * If the opfamily doesn't supply a complete set of cross-type ORDER procs we
 * may not be able to determine which elements are contradictory.  If we have
 * the required ORDER proc then we return true (and validly set *nelems_orig),
 * guaranteeing that at least the next array can be considered redundant.  We
 * return false if the required comparisons cannot be made (caller must keep
 * both arrays when this happens).
 */
pub unsafe fn _bt_merge_arrays(
    scan: IndexScanDesc,
    skey: ScanKey,
    sortproc: *mut FmgrInfo,
    reverse: bool,
    origelemtype: Oid,
    nextelemtype: Oid,
    elems_orig: *mut Datum,
    nelems_orig: *mut c_int,
    elems_next: *mut Datum,
    nelems_next: c_int,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut cxt: BTSortArrayContext = BTSortArrayContext {
        sortproc: std::ptr::null_mut(),
        collation: 0,
        reverse: false,
    };
    let nelems_orig_start: c_int = *nelems_orig;
    let mut nelems_orig_merged: c_int = 0;
    let mut mergeproc: *mut FmgrInfo = sortproc;
    let mut crosstypeproc: FmgrInfo = core::mem::zeroed();

    Assert!((*skey).sk_strategy == BTEqualStrategyNumber);
    Assert!(OidIsValid(origelemtype) && OidIsValid(nextelemtype));

    if origelemtype != nextelemtype {
        let cmp_proc: RegProcedure;

        /*
         * Cross-array-element-type merging is required, so can't just reuse
         * sortproc when merging
         */
        cmp_proc = get_opfamily_proc(
            *(*rel).rd_opfamily.add((*skey).sk_attno as usize - 1),
            origelemtype,
            nextelemtype,
            BTORDER_PROC,
        );
        if !RegProcedureIsValid(cmp_proc) {
            /* Can't make the required comparisons */
            return false;
        }

        /* We have all we need to determine redundancy/contradictoriness */
        mergeproc = &raw mut crosstypeproc;
        fmgr_info_cxt(cmp_proc, mergeproc, (*so).arrayContext);
    }

    cxt.sortproc = mergeproc;
    cxt.collation = (*skey).sk_collation;
    cxt.reverse = reverse;

    let mut i: c_int = 0;
    let mut j: c_int = 0;
    while i < nelems_orig_start && j < nelems_next {
        let oelem: *mut Datum = elems_orig.add(i as usize);
        let nelem: *mut Datum = elems_next.add(j as usize);
        let res: c_int =
            _bt_compare_array_elements(oelem as *const c_void, nelem as *const c_void, &raw mut cxt as *mut c_void);

        if res == 0 {
            *elems_orig.add(nelems_orig_merged as usize) = *oelem;
            nelems_orig_merged += 1;
            i += 1;
            j += 1;
        } else if res < 0 {
            i += 1;
        } else {
            /* res > 0 */
            j += 1;
        }
    }

    *nelems_orig = nelems_orig_merged;

    true
}

/*
 * qsort_arg comparator for sorting array elements
 */
unsafe extern "C" fn _bt_compare_array_elements(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    let da: Datum = *(a as *const Datum);
    let db: Datum = *(b as *const Datum);
    let cxt: *mut BTSortArrayContext = arg as *mut BTSortArrayContext;
    let mut compare: int32;

    compare = DatumGetInt32(FunctionCall2Coll(
        (*cxt).sortproc,
        (*cxt).collation,
        da,
        db,
    ));
    if (*cxt).reverse {
        compare = INVERT_COMPARE_RESULT(compare);
    }
    compare
}
