//! Translation of postgres/src/backend/executor/execTuples.c
//!
//! Routines dealing with TupleTableSlots: resource management for tuples and the
//! "virtual"-tuple access abstraction, plus the type-info-from-tuple helpers.
//! This file holds the four TupleTableSlotOps vtables (Virtual / HeapTuple /
//! MinimalTuple / BufferHeapTuple), the slot deform workhorse, the tuple-table
//! create/delete management, and the ExecStore*/ExecFetch* accessor family.  The
//! TupleTableSlot type layer itself lives in crate::executor::tuptable.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h                  -> crate::prelude
//!   access/heaptoast.h          -> not referenced (toast paths live in heaptuple.c)
//!   access/htup_details.h       -> crate::access::htup_details
//!   access/tupdesc_details.h    -> crate::access::common::tupdesc (AttrMissing via constr)
//!   access/xact.h               -> STUB: TransactionIdIsCurrentTransactionId (xact.c
//!                                  not ported) -> the *_is_current_xact_tuple callbacks
//!                                  that need it are stubbed (unimplemented!).
//!   catalog/pg_type.h           -> RECORDOID via crate::catalog::pg_type_d (only used by
//!                                  the STUBBED type-from-targetlist helpers)
//!   funcapi.h                   -> STUB: AttInMetadata / TupOutputState / DestReceiver
//!                                  machinery not ported -> those routines stubbed.
//!   nodes/nodeFuncs.h           -> STUB: exprType/Typmod/Collation extraction for the
//!                                  type-from-targetlist helpers -> stubbed.
//!   storage/bufmgr.h            -> STUB: Buffer pin/release (ReleaseBuffer /
//!                                  IncrBufferRefCount / BufferIsValid / InvalidBuffer)
//!                                  not ported -> ALL tts_buffer_heap_* ops + the
//!                                  TTSOpsBufferHeapTuple table + ExecStore[Pinned]
//!                                  BufferHeapTuple are stubbed.
//!   utils/builtins.h            -> STUB (cstring_to_text_with_len, do_text_output_*)
//!   utils/expandeddatum.h       -> crate::utils::adt::expandeddatum (materialize path)
//!   utils/lsyscache.h           -> STUB (getTypeInputInfo, AttInMetadata helpers)
//!   utils/typcache.h            -> STUB (assign_record_type_typmod, lookup_rowtype_*)
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: the TTSOpsVirtual / TTSOpsHeapTuple / TTSOpsMinimalTuple op functions and
//!     their three static vtables; slot_deform_heap_tuple(_internal); MakeTupleTableSlot,
//!     ExecAllocTableSlot, ExecResetTupleTable, MakeSingleTupleTableSlot,
//!     ExecDropSingleTupleTableSlot, ExecSetSlotDescriptor; ExecStoreHeapTuple,
//!     ExecStoreMinimalTuple, ExecStoreVirtualTuple, ExecStoreAllNullTuple,
//!     ExecForceStoreHeapTuple (heap/virtual branches), ExecForceStoreMinimalTuple,
//!     ExecFetchSlotHeapTuple, ExecFetchSlotMinimalTuple, ExecFetchSlotHeapTupleDatum,
//!     slot_getsomeattrs_int, slot_getmissingattrs.
//!   STUBBED (signatures real, unimplemented!() + TODO(pg-port)):
//!     - tts_*_is_current_xact_tuple for heap/buffer (need
//!       TransactionIdIsCurrentTransactionId, xact.c).  Virtual/minimal variants are
//!       real (they always ereport ERROR).
//!     - ALL tts_buffer_heap_* ops, the TTSOpsBufferHeapTuple table,
//!       tts_buffer_heap_store_tuple, ExecStoreBufferHeapTuple,
//!       ExecStorePinnedBufferHeapTuple, and ExecForceStoreHeapTuple's BUFFERTUPLE
//!       branch (need storage/bufmgr.h).
//!     - ExecStoreHeapTupleDatum (needs DatumGetHeapTupleHeader, an fmgr macro).
//!     - ExecTypeFromTL / ExecCleanTypeFromTL / ExecTypeFromTLInternal /
//!       ExecTypeFromExprList / ExecTypeSetColNames (need TargetEntry + exprType*
//!       + TupleDescInitEntry).
//!     - BlessTupleDesc / TupleDescGetAttInMetadata / BuildTupleFromCStrings /
//!       HeapTupleHeaderGetDatum (need typcache / fmgr).
//!     - the begin/do/end_tup_output family + ExecInit*TupleSlot* (need DestReceiver
//!       / PlanState, not ported).

use crate::prelude::*;

use crate::executor::tuptable::*;

use crate::access::common::heaptuple::{
    heap_copy_minimal_tuple, heap_copy_tuple_as_datum, heap_copytuple, heap_deform_tuple,
    heap_form_minimal_tuple, heap_form_tuple, heap_free_minimal_tuple, heap_freetuple,
    heap_tuple_from_minimal_tuple, minimal_tuple_from_heap_tuple,
};
use crate::access::htup_details::{
    heap_getsysattr, HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleHeaderGetDatumLength,
    HeapTupleHeaderGetNatts, HeapTupleHeaderGetRawXmin, HeapTupleHasNulls, MinimalTuple,
    MINIMAL_TUPLE_OFFSET,
};
use crate::{current_cell, foreach, lfirst_node, IsA};
use crate::access::tupmacs::{att_addlength_datum, att_addlength_pointer, att_nominal_alignby, att_pointer_alignby};
use crate::access::common::tupdesc::{
    CompactAttribute, CreateTemplateTupleDesc, PinTupleDesc, ReleaseTupleDesc, TupleDesc,
    TupleDescCompactAttr, TupleDescInitEntry, TupleDescInitEntryCollation,
};
use crate::executor::execUtils::{ExecCleanTargetListLength, ExecTargetListLength};
use crate::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
use crate::nodes::primnodes::TargetEntry;
use crate::nodes::nodes::Node;

use crate::nodes::nodes::NodeTag::T_TupleTableSlot;
use crate::nodes::pg_list::{lappend, list_free, List};
use crate::nodes::primnodes::AttrNumber;

use crate::storage::itemptr::ItemPointerSetInvalid;

use crate::utils::adt::expandeddatum::{DatumGetEOHP, EOH_flatten_into, EOH_get_flat_size};

use crate::c::{uint32, TransactionId};

use core::mem::size_of;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/*
 * ERRCODE_FEATURE_NOT_SUPPORTED (errcodes.h, not yet ported).  The errcode()
 * shim ignores the value, so any placeholder is fine; kept named for fidelity.
 */
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;

// ============================================================================
//   storage/bufmgr.h shims (STUB - storage/bufmgr.h not yet ported).
//
//   Buffer is already typedef'd `pub type Buffer = c_int` in tuptable.rs (via the
//   `use crate::executor::tuptable::*` glob).  The pin/release primitives are the
//   only bufmgr surface execTuples needs; until bufmgr lands they are stubs that
//   panic, and every code path that reaches them is itself stubbed.
// ============================================================================

/* storage/buf.h: #define InvalidBuffer 0 */
const InvalidBuffer: Buffer = 0;

/* storage/bufmgr.h: BufferIsValid(bufnum) - is this a valid (non-Invalid) buffer? */
#[inline]
fn BufferIsValid(bufnum: Buffer) -> bool {
    // C also Asserts bufnum <= NBuffers; we only need the non-Invalid test here.
    bufnum != InvalidBuffer
}

/*
 * ReleaseBuffer / IncrBufferRefCount - STUB (storage/bufmgr.c not ported).
 *
 * # Safety
 * Stub: never returns.
 */
unsafe fn ReleaseBuffer(buffer: Buffer) {
    crate::storage::buffer::bufmgr::ReleaseBuffer(buffer)
}
unsafe fn IncrBufferRefCount(buffer: Buffer) {
    crate::storage::buffer::bufmgr::IncrBufferRefCount(buffer)
}

// ============================================================================
//   access/xact.h shim (STUB - access/transam/xact.c not yet ported).
// ============================================================================

/*
 * TransactionIdIsCurrentTransactionId - STUB (xact.c not ported).  Used only by
 * the heap/buffer *_is_current_xact_tuple callbacks (themselves stubbed).
 *
 * # Safety
 * Stub: never returns.
 */
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    crate::access::transam::xact::TransactionIdIsCurrentTransactionId(_xid as _) as _
}

/*
 * DatumGetHeapTupleHeader - STUB (fmgr.h macro: (HeapTupleHeader)
 * PG_DETOAST_DATUM(X)).  PG_DETOAST_DATUM relies on fmgr/toast machinery that
 * isn't fully wired here; used only by ExecStoreHeapTupleDatum.
 *
 * # Safety
 * Stub: never returns.
 */
unsafe fn DatumGetHeapTupleHeader(_data: Datum) -> HeapTupleHeader {
    // TODO(pg-port): fmgr.h DatumGetHeapTupleHeader / PG_DETOAST_DATUM.
    unimplemented!("DatumGetHeapTupleHeader: fmgr.h PG_DETOAST_DATUM not yet translated")
}

// ============================================================================
//   TupleTableSlotOps implementations.
// ============================================================================

// ----------------------------------------------------------------------------
//   TupleTableSlotOps implementation for VirtualTupleTableSlot.
// ----------------------------------------------------------------------------

unsafe fn tts_virtual_init(_slot: *mut TupleTableSlot) {}

unsafe fn tts_virtual_release(_slot: *mut TupleTableSlot) {}

unsafe fn tts_virtual_clear(slot: *mut TupleTableSlot) {
    if unlikely(TTS_SHOULDFREE(slot)) {
        let vslot = slot as *mut VirtualTupleTableSlot;

        pfree((*vslot).data as *mut c_void);
        (*vslot).data = null_mut();

        (*slot).tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    (*slot).tts_nvalid = 0;
    (*slot).tts_flags |= TTS_FLAG_EMPTY;
    ItemPointerSetInvalid(&mut (*slot).tts_tid);
}

/*
 * VirtualTupleTableSlots always have fully populated tts_values and tts_isnull
 * arrays.  So this function should never be called.
 */
unsafe fn tts_virtual_getsomeattrs(_slot: *mut TupleTableSlot, _natts: c_int) {
    elog!(
        ERROR,
        "getsomeattrs is not required to be called on a virtual tuple table slot"
    );
}

/*
 * VirtualTupleTableSlots never provide system attributes (except those handled
 * generically, such as tableoid).  We generally shouldn't get here, but provide
 * a user-friendly message if we do.
 */
unsafe fn tts_virtual_getsysattr(slot: *mut TupleTableSlot, _attnum: c_int, _isnull: *mut bool) -> Datum {
    Assert!(!TTS_EMPTY(slot));

    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("cannot retrieve a system column in this context")
    );

    #[allow(unreachable_code)]
    {
        0 /* silence compiler warnings */
    }
}

/*
 * VirtualTupleTableSlots never have storage tuples.  We generally shouldn't get
 * here, but provide a user-friendly message if we do.
 */
unsafe fn tts_virtual_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    Assert!(!TTS_EMPTY(slot));

    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("don't have transaction information for this type of tuple")
    );

    #[allow(unreachable_code)]
    {
        false /* silence compiler warnings */
    }
}

/*
 * To materialize a virtual slot all the datums that aren't passed by value have
 * to be copied into the slot's memory context.  Compute the required size, then
 * allocate enough memory to store all attributes in one go.
 */
unsafe fn tts_virtual_materialize(slot: *mut TupleTableSlot) {
    let vslot = slot as *mut VirtualTupleTableSlot;
    let desc = (*slot).tts_tupleDescriptor;
    let mut sz: Size = 0;

    /* already materialized */
    if TTS_SHOULDFREE(slot) {
        return;
    }

    /* compute size of memory required */
    for natt in 0..(*desc).natts {
        let att: *mut CompactAttribute = TupleDescCompactAttr(desc, natt);

        if (*att).attbyval || *(*slot).tts_isnull.add(natt as usize) {
            continue;
        }

        let val = *(*slot).tts_values.add(natt as usize);

        if (*att).attlen == -1
            && crate::varatt::VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val) as *const c_char)
        {
            /*
             * We want to flatten the expanded value so that the materialized
             * slot doesn't depend on it.
             */
            sz = att_nominal_alignby(sz, (*att).attalignby);
            sz += EOH_get_flat_size(DatumGetEOHP(val));
        } else {
            sz = att_nominal_alignby(sz, (*att).attalignby);
            sz = att_addlength_datum(sz, (*att).attlen as c_int, val);
        }
    }

    /* all data is byval */
    if sz == 0 {
        return;
    }

    /* allocate memory */
    let mut data = MemoryContextAlloc((*slot).tts_mcxt, sz) as *mut c_char;
    (*vslot).data = data;
    (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;

    /* and copy all attributes into the pre-allocated space */
    for natt in 0..(*desc).natts {
        let att: *mut CompactAttribute = TupleDescCompactAttr(desc, natt);

        if (*att).attbyval || *(*slot).tts_isnull.add(natt as usize) {
            continue;
        }

        let val = *(*slot).tts_values.add(natt as usize);

        if (*att).attlen == -1
            && crate::varatt::VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val) as *const c_char)
        {
            /*
             * We want to flatten the expanded value so that the materialized
             * slot doesn't depend on it.
             */
            let eoh = DatumGetEOHP(val);

            data = att_nominal_alignby(data as usize, (*att).attalignby) as *mut c_char;
            let data_length = EOH_get_flat_size(eoh);
            EOH_flatten_into(eoh, data as *mut c_void, data_length);

            *(*slot).tts_values.add(natt as usize) = PointerGetDatum(data as *const c_void);
            data = data.add(data_length);
        } else {
            data = att_nominal_alignby(data as usize, (*att).attalignby) as *mut c_char;
            let data_length = att_addlength_datum(0, (*att).attlen as c_int, val);

            memcpy(
                data as *mut c_void,
                DatumGetPointer(val) as *const c_void,
                data_length,
            );

            *(*slot).tts_values.add(natt as usize) = PointerGetDatum(data as *const c_void);
            data = data.add(data_length);
        }
    }
}

unsafe fn tts_virtual_copyslot(dstslot: *mut TupleTableSlot, srcslot: *mut TupleTableSlot) {
    let srcdesc = (*srcslot).tts_tupleDescriptor;

    tts_virtual_clear(dstslot);

    slot_getallattrs(srcslot);

    for natt in 0..(*srcdesc).natts as usize {
        *(*dstslot).tts_values.add(natt) = *(*srcslot).tts_values.add(natt);
        *(*dstslot).tts_isnull.add(natt) = *(*srcslot).tts_isnull.add(natt);
    }

    (*dstslot).tts_nvalid = (*srcdesc).natts as AttrNumber;
    (*dstslot).tts_flags &= !TTS_FLAG_EMPTY;

    /* make sure storage doesn't depend on external memory */
    tts_virtual_materialize(dstslot);
}

unsafe fn tts_virtual_copy_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    Assert!(!TTS_EMPTY(slot));

    heap_form_tuple(
        (*slot).tts_tupleDescriptor,
        (*slot).tts_values,
        (*slot).tts_isnull,
    )
}

unsafe fn tts_virtual_copy_minimal_tuple(slot: *mut TupleTableSlot, extra: Size) -> MinimalTuple {
    Assert!(!TTS_EMPTY(slot));

    heap_form_minimal_tuple(
        (*slot).tts_tupleDescriptor,
        (*slot).tts_values,
        (*slot).tts_isnull,
        extra,
    )
}

// ----------------------------------------------------------------------------
//   TupleTableSlotOps implementation for HeapTupleTableSlot.
// ----------------------------------------------------------------------------

unsafe fn tts_heap_init(_slot: *mut TupleTableSlot) {}

unsafe fn tts_heap_release(_slot: *mut TupleTableSlot) {}

unsafe fn tts_heap_clear(slot: *mut TupleTableSlot) {
    let hslot = slot as *mut HeapTupleTableSlot;

    /* Free the memory for the heap tuple if it's allowed. */
    if TTS_SHOULDFREE(slot) {
        heap_freetuple((*hslot).tuple);
        (*slot).tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    (*slot).tts_nvalid = 0;
    (*slot).tts_flags |= TTS_FLAG_EMPTY;
    ItemPointerSetInvalid(&mut (*slot).tts_tid);
    (*hslot).off = 0;
    (*hslot).tuple = null_mut();
}

unsafe fn tts_heap_getsomeattrs(slot: *mut TupleTableSlot, natts: c_int) {
    let hslot = slot as *mut HeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    slot_deform_heap_tuple(slot, (*hslot).tuple, &mut (*hslot).off, natts);
}

unsafe fn tts_heap_getsysattr(slot: *mut TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum {
    let hslot = slot as *mut HeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /*
     * In some code paths it's possible to get here with a non-materialized
     * slot, in which case we can't retrieve system columns.
     */
    if (*hslot).tuple.is_null() {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("cannot retrieve a system column in this context")
        );
    }

    heap_getsysattr((*hslot).tuple, attnum, (*slot).tts_tupleDescriptor, isnull)
}

/*
 * STUB: needs TransactionIdIsCurrentTransactionId (access/transam/xact.c).
 */
unsafe fn tts_heap_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    let hslot = slot as *mut HeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /*
     * In some code paths it's possible to get here with a non-materialized
     * slot, in which case we can't check if tuple is created by the current
     * transaction.
     */
    if (*hslot).tuple.is_null() {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("don't have a storage tuple in this context")
        );
    }

    let xmin: TransactionId = HeapTupleHeaderGetRawXmin((*(*hslot).tuple).t_data);

    // TODO(pg-port): xact.c not ported; the rest of this fn is faithful.
    TransactionIdIsCurrentTransactionId(xmin)
}

unsafe fn tts_heap_materialize(slot: *mut TupleTableSlot) {
    let hslot = slot as *mut HeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /* If slot has its tuple already materialized, nothing to do. */
    if TTS_SHOULDFREE(slot) {
        return;
    }

    let oldContext = MemoryContextSwitchTo((*slot).tts_mcxt);

    /*
     * Have to deform from scratch, otherwise tts_values[] entries could point
     * into the non-materialized tuple (which might be gone when accessed).
     */
    (*slot).tts_nvalid = 0;
    (*hslot).off = 0;

    if (*hslot).tuple.is_null() {
        (*hslot).tuple = heap_form_tuple(
            (*slot).tts_tupleDescriptor,
            (*slot).tts_values,
            (*slot).tts_isnull,
        );
    } else {
        /*
         * The tuple contained in this slot is not allocated in the memory
         * context of the given slot (else it would have TTS_FLAG_SHOULDFREE
         * set).  Copy the tuple into the given slot's memory context.
         */
        (*hslot).tuple = heap_copytuple((*hslot).tuple);
    }

    (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;

    MemoryContextSwitchTo(oldContext);
}

unsafe fn tts_heap_copyslot(dstslot: *mut TupleTableSlot, srcslot: *mut TupleTableSlot) {
    let oldcontext = MemoryContextSwitchTo((*dstslot).tts_mcxt);
    let tuple = ExecCopySlotHeapTuple(srcslot);
    MemoryContextSwitchTo(oldcontext);

    ExecStoreHeapTuple(tuple, dstslot, true);
}

unsafe fn tts_heap_get_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    let hslot = slot as *mut HeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));
    if (*hslot).tuple.is_null() {
        tts_heap_materialize(slot);
    }

    (*hslot).tuple
}

unsafe fn tts_heap_copy_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    let hslot = slot as *mut HeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));
    if (*hslot).tuple.is_null() {
        tts_heap_materialize(slot);
    }

    heap_copytuple((*hslot).tuple)
}

unsafe fn tts_heap_copy_minimal_tuple(slot: *mut TupleTableSlot, extra: Size) -> MinimalTuple {
    let hslot = slot as *mut HeapTupleTableSlot;

    if (*hslot).tuple.is_null() {
        tts_heap_materialize(slot);
    }

    minimal_tuple_from_heap_tuple((*hslot).tuple, extra)
}

unsafe fn tts_heap_store_tuple(slot: *mut TupleTableSlot, tuple: HeapTuple, shouldFree: bool) {
    let hslot = slot as *mut HeapTupleTableSlot;

    tts_heap_clear(slot);

    (*slot).tts_nvalid = 0;
    (*hslot).tuple = tuple;
    (*hslot).off = 0;
    (*slot).tts_flags &= !(TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE);
    (*slot).tts_tid = (*tuple).t_self;

    if shouldFree {
        (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;
    }
}

// ----------------------------------------------------------------------------
//   TupleTableSlotOps implementation for MinimalTupleTableSlot.
// ----------------------------------------------------------------------------

unsafe fn tts_minimal_init(slot: *mut TupleTableSlot) {
    let mslot = slot as *mut MinimalTupleTableSlot;

    /*
     * Initialize the heap tuple pointer to access attributes of the minimal
     * tuple contained in the slot as if it's a heap tuple.
     */
    (*mslot).tuple = &mut (*mslot).minhdr as *mut HeapTupleData;
}

unsafe fn tts_minimal_release(_slot: *mut TupleTableSlot) {}

unsafe fn tts_minimal_clear(slot: *mut TupleTableSlot) {
    let mslot = slot as *mut MinimalTupleTableSlot;

    if TTS_SHOULDFREE(slot) {
        heap_free_minimal_tuple((*mslot).mintuple);
        (*slot).tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    (*slot).tts_nvalid = 0;
    (*slot).tts_flags |= TTS_FLAG_EMPTY;
    ItemPointerSetInvalid(&mut (*slot).tts_tid);
    (*mslot).off = 0;
    (*mslot).mintuple = null_mut();
}

unsafe fn tts_minimal_getsomeattrs(slot: *mut TupleTableSlot, natts: c_int) {
    let mslot = slot as *mut MinimalTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    slot_deform_heap_tuple(slot, (*mslot).tuple, &mut (*mslot).off, natts);
}

/*
 * MinimalTupleTableSlots never provide system attributes.  We generally
 * shouldn't get here, but provide a user-friendly message if we do.
 */
unsafe fn tts_minimal_getsysattr(slot: *mut TupleTableSlot, _attnum: c_int, _isnull: *mut bool) -> Datum {
    Assert!(!TTS_EMPTY(slot));

    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("cannot retrieve a system column in this context")
    );

    #[allow(unreachable_code)]
    {
        0 /* silence compiler warnings */
    }
}

/*
 * Within MinimalTuple abstraction transaction information is unavailable.  We
 * generally shouldn't get here, but provide a user-friendly message if we do.
 */
unsafe fn tts_minimal_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    Assert!(!TTS_EMPTY(slot));

    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("don't have transaction information for this type of tuple")
    );

    #[allow(unreachable_code)]
    {
        false /* silence compiler warnings */
    }
}

unsafe fn tts_minimal_materialize(slot: *mut TupleTableSlot) {
    let mslot = slot as *mut MinimalTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /* If slot has its tuple already materialized, nothing to do. */
    if TTS_SHOULDFREE(slot) {
        return;
    }

    let oldContext = MemoryContextSwitchTo((*slot).tts_mcxt);

    /*
     * Have to deform from scratch, otherwise tts_values[] entries could point
     * into the non-materialized tuple (which might be gone when accessed).
     */
    (*slot).tts_nvalid = 0;
    (*mslot).off = 0;

    if (*mslot).mintuple.is_null() {
        (*mslot).mintuple = heap_form_minimal_tuple(
            (*slot).tts_tupleDescriptor,
            (*slot).tts_values,
            (*slot).tts_isnull,
            0,
        );
    } else {
        /*
         * The minimal tuple contained in this slot is not allocated in the
         * memory context of the given slot (else it would have
         * TTS_FLAG_SHOULDFREE set).  Copy the minimal tuple into the given
         * slot's memory context.
         */
        (*mslot).mintuple = heap_copy_minimal_tuple((*mslot).mintuple, 0);
    }

    (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;

    Assert!((*mslot).tuple == &mut (*mslot).minhdr as *mut HeapTupleData);

    (*mslot).minhdr.t_len = (*(*mslot).mintuple).t_len + MINIMAL_TUPLE_OFFSET as uint32;
    (*mslot).minhdr.t_data =
        ((*mslot).mintuple as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize)) as HeapTupleHeader;

    MemoryContextSwitchTo(oldContext);
}

unsafe fn tts_minimal_copyslot(dstslot: *mut TupleTableSlot, srcslot: *mut TupleTableSlot) {
    let oldcontext = MemoryContextSwitchTo((*dstslot).tts_mcxt);
    let mintuple = ExecCopySlotMinimalTuple(srcslot);
    MemoryContextSwitchTo(oldcontext);

    ExecStoreMinimalTuple(mintuple, dstslot, true);
}

unsafe fn tts_minimal_get_minimal_tuple(slot: *mut TupleTableSlot) -> MinimalTuple {
    let mslot = slot as *mut MinimalTupleTableSlot;

    if (*mslot).mintuple.is_null() {
        tts_minimal_materialize(slot);
    }

    (*mslot).mintuple
}

unsafe fn tts_minimal_copy_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    let mslot = slot as *mut MinimalTupleTableSlot;

    if (*mslot).mintuple.is_null() {
        tts_minimal_materialize(slot);
    }

    heap_tuple_from_minimal_tuple((*mslot).mintuple)
}

unsafe fn tts_minimal_copy_minimal_tuple(slot: *mut TupleTableSlot, extra: Size) -> MinimalTuple {
    let mslot = slot as *mut MinimalTupleTableSlot;

    if (*mslot).mintuple.is_null() {
        tts_minimal_materialize(slot);
    }

    heap_copy_minimal_tuple((*mslot).mintuple, extra)
}

unsafe fn tts_minimal_store_tuple(slot: *mut TupleTableSlot, mtup: MinimalTuple, shouldFree: bool) {
    let mslot = slot as *mut MinimalTupleTableSlot;

    tts_minimal_clear(slot);

    Assert!(!TTS_SHOULDFREE(slot));
    Assert!(TTS_EMPTY(slot));

    (*slot).tts_flags &= !TTS_FLAG_EMPTY;
    (*slot).tts_nvalid = 0;
    (*mslot).off = 0;

    (*mslot).mintuple = mtup;
    Assert!((*mslot).tuple == &mut (*mslot).minhdr as *mut HeapTupleData);
    (*mslot).minhdr.t_len = (*mtup).t_len + MINIMAL_TUPLE_OFFSET as uint32;
    (*mslot).minhdr.t_data =
        (mtup as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize)) as HeapTupleHeader;
    /* no need to set t_self or t_tableOid since we won't allow access */

    if shouldFree {
        (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;
    }
}

// ----------------------------------------------------------------------------
//   TupleTableSlotOps implementation for BufferHeapTupleTableSlot.
//
//   STUB: every op below needs storage/bufmgr.h (Buffer pin/release), which is
//   not yet ported.  The C bodies are preserved as comments; each op is
//   unimplemented!().  The TTSOpsBufferHeapTuple table wires them positionally so
//   it still compiles (and so TTS_IS_BUFFERTUPLE address comparisons work).
//   TODO(pg-port): translate fully once storage/buffer/bufmgr.c lands.
// ----------------------------------------------------------------------------

unsafe fn tts_buffer_heap_init(_slot: *mut TupleTableSlot) {}

unsafe fn tts_buffer_heap_release(_slot: *mut TupleTableSlot) {}

unsafe fn tts_buffer_heap_clear(slot: *mut TupleTableSlot) {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    /*
     * Free the memory for heap tuple if allowed. A tuple coming from buffer
     * can never be freed. But we may have materialized a tuple from buffer.
     * Such a tuple can be freed.
     */
    if TTS_SHOULDFREE(slot) {
        /* We should have unpinned the buffer while materializing the tuple. */
        Assert!(!BufferIsValid((*bslot).buffer));

        heap_freetuple((*bslot).base.tuple);
        (*slot).tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    if BufferIsValid((*bslot).buffer) {
        ReleaseBuffer((*bslot).buffer);
    }

    (*slot).tts_nvalid = 0;
    (*slot).tts_flags |= TTS_FLAG_EMPTY;
    ItemPointerSetInvalid(&mut (*slot).tts_tid);
    (*bslot).base.tuple = null_mut();
    (*bslot).base.off = 0;
    (*bslot).buffer = InvalidBuffer;
}

unsafe fn tts_buffer_heap_getsomeattrs(slot: *mut TupleTableSlot, natts: c_int) {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    slot_deform_heap_tuple(slot, (*bslot).base.tuple, &mut (*bslot).base.off, natts);
}

unsafe fn tts_buffer_heap_getsysattr(slot: *mut TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /*
     * In some code paths it's possible to get here with a non-materialized
     * slot, in which case we can't retrieve system columns.
     */
    if (*bslot).base.tuple.is_null() {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("cannot retrieve a system column in this context")
        );
    }

    heap_getsysattr((*bslot).base.tuple, attnum, (*slot).tts_tupleDescriptor, isnull)
}

/*
 * STUB: needs TransactionIdIsCurrentTransactionId (access/transam/xact.c).
 */
unsafe fn tts_buffer_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /*
     * In some code paths it's possible to get here with a non-materialized
     * slot, in which case we can't check if tuple is created by the current
     * transaction.
     */
    if (*bslot).base.tuple.is_null() {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("don't have a storage tuple in this context")
        );
    }

    let xmin: TransactionId = HeapTupleHeaderGetRawXmin((*(*bslot).base.tuple).t_data);

    TransactionIdIsCurrentTransactionId(xmin)
}

unsafe fn tts_buffer_heap_materialize(slot: *mut TupleTableSlot) {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    /* If slot has its tuple already materialized, nothing to do. */
    if TTS_SHOULDFREE(slot) {
        return;
    }

    let oldContext = MemoryContextSwitchTo((*slot).tts_mcxt);

    /*
     * Have to deform from scratch, otherwise tts_values[] entries could point
     * into the non-materialized tuple (which might be gone when accessed).
     */
    (*bslot).base.off = 0;
    (*slot).tts_nvalid = 0;

    if (*bslot).base.tuple.is_null() {
        /*
         * Normally BufferHeapTupleTableSlot should have a tuple + buffer
         * associated with it, unless it's materialized (which would've
         * returned above). But when it's useful to allow storing virtual
         * tuples in a buffer slot, which then also needs to be
         * materializable.
         */
        (*bslot).base.tuple = heap_form_tuple(
            (*slot).tts_tupleDescriptor,
            (*slot).tts_values,
            (*slot).tts_isnull,
        );
    } else {
        (*bslot).base.tuple = heap_copytuple((*bslot).base.tuple);

        /*
         * A heap tuple stored in a BufferHeapTupleTableSlot should have a
         * buffer associated with it, unless it's materialized or virtual.
         */
        if likely(BufferIsValid((*bslot).buffer)) {
            ReleaseBuffer((*bslot).buffer);
        }
        (*bslot).buffer = InvalidBuffer;
    }

    /*
     * We don't set TTS_FLAG_SHOULDFREE until after releasing the buffer, if
     * any.  This avoids having a transient state that would fall foul of our
     * assertions that a slot with TTS_FLAG_SHOULDFREE doesn't own a buffer.
     * In the unlikely event that ReleaseBuffer() above errors out, we'd
     * effectively leak the copied tuple, but that seems fairly harmless.
     */
    (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;

    MemoryContextSwitchTo(oldContext);
}

unsafe fn tts_buffer_heap_copyslot(dstslot: *mut TupleTableSlot, srcslot: *mut TupleTableSlot) {
    let bsrcslot = srcslot as *mut BufferHeapTupleTableSlot;
    let bdstslot = dstslot as *mut BufferHeapTupleTableSlot;

    /*
     * If the source slot is of a different kind, or is a buffer slot that has
     * been materialized / is virtual, make a new copy of the tuple. Otherwise
     * make a new reference to the in-buffer tuple.
     */
    if (*dstslot).tts_ops != (*srcslot).tts_ops
        || TTS_SHOULDFREE(srcslot)
        || (*bsrcslot).base.tuple.is_null()
    {
        ExecClearTuple(dstslot);
        (*dstslot).tts_flags &= !TTS_FLAG_EMPTY;
        let oldContext = MemoryContextSwitchTo((*dstslot).tts_mcxt);
        (*bdstslot).base.tuple = ExecCopySlotHeapTuple(srcslot);
        (*dstslot).tts_flags |= TTS_FLAG_SHOULDFREE;
        MemoryContextSwitchTo(oldContext);
    } else {
        Assert!(BufferIsValid((*bsrcslot).buffer));

        tts_buffer_heap_store_tuple(dstslot, (*bsrcslot).base.tuple,
                                    (*bsrcslot).buffer, false);

        /*
         * The HeapTupleData portion of the source tuple might be shorter
         * lived than the destination slot. Therefore copy the HeapTuple into
         * our slot's tupdata, which is guaranteed to live long enough (but
         * will still point into the buffer).
         */
        memcpy(
            &mut (*bdstslot).base.tupdata as *mut HeapTupleData as *mut c_void,
            (*bdstslot).base.tuple as *const c_void,
            size_of::<HeapTupleData>(),
        );
        (*bdstslot).base.tuple = &mut (*bdstslot).base.tupdata;
    }
}

unsafe fn tts_buffer_heap_get_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    if (*bslot).base.tuple.is_null() {
        tts_buffer_heap_materialize(slot);
    }

    (*bslot).base.tuple
}

unsafe fn tts_buffer_heap_copy_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    if (*bslot).base.tuple.is_null() {
        tts_buffer_heap_materialize(slot);
    }

    heap_copytuple((*bslot).base.tuple)
}

unsafe fn tts_buffer_heap_copy_minimal_tuple(slot: *mut TupleTableSlot, extra: Size) -> MinimalTuple {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    Assert!(!TTS_EMPTY(slot));

    if (*bslot).base.tuple.is_null() {
        tts_buffer_heap_materialize(slot);
    }

    minimal_tuple_from_heap_tuple((*bslot).base.tuple, extra)
}

/*
 * tts_buffer_heap_store_tuple - STUB (storage/bufmgr.h).  The full C body
 * (preserved below) manages the buffer pin/transfer and the same-page
 * optimization; it is used by ExecStore[Pinned]BufferHeapTuple and
 * tts_buffer_heap_copyslot, all of which are themselves stubbed.
 *
 * # Safety
 * Stub: never returns.
 */
unsafe fn tts_buffer_heap_store_tuple(
    slot: *mut TupleTableSlot,
    tuple: HeapTuple,
    buffer: Buffer,
    transfer_pin: bool,
) {
    let bslot = slot as *mut BufferHeapTupleTableSlot;

    if TTS_SHOULDFREE(slot) {
        /* materialized slot shouldn't have a buffer to release */
        Assert!(!BufferIsValid((*bslot).buffer));

        heap_freetuple((*bslot).base.tuple);
        (*slot).tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    (*slot).tts_flags &= !TTS_FLAG_EMPTY;
    (*slot).tts_nvalid = 0;
    (*bslot).base.tuple = tuple;
    (*bslot).base.off = 0;
    (*slot).tts_tid = (*tuple).t_self;

    /*
     * If tuple is on a disk page, keep the page pinned as long as we hold a
     * pointer into it.  We assume the caller already has such a pin.  If
     * transfer_pin is true, we'll transfer that pin to this slot, if not
     * we'll pin it again ourselves.
     *
     * This is coded to optimize the case where the slot previously held a
     * tuple on the same disk page: in that case releasing and re-acquiring
     * the pin is a waste of cycles.  This is a common situation during
     * seqscans, so it's worth troubling over.
     */
    if (*bslot).buffer != buffer {
        if BufferIsValid((*bslot).buffer) {
            ReleaseBuffer((*bslot).buffer);
        }

        (*bslot).buffer = buffer;

        if !transfer_pin && BufferIsValid(buffer) {
            IncrBufferRefCount(buffer);
        }
    } else if transfer_pin && BufferIsValid(buffer) {
        /*
         * In transfer_pin mode the caller won't know about the same-page
         * optimization, so we gotta release its pin.
         */
        ReleaseBuffer(buffer);
    }
}

// ============================================================================
//   slot_deform_heap_tuple{,_internal}
// ============================================================================

/*
 * slot_deform_heap_tuple_internal
 *		A helper for slot_deform_heap_tuple, with constant `slow`/`hasnulls`
 *		parameters (in C, inlined to specialize per-combination).  Returns the
 *		next attnum to deform (== natts when all done).  `offp` is in/out: the
 *		byte offset to start from, updated on return.  `slowp` is set true when
 *		subsequent deforming must use slow mode.
 *
 * # Safety
 * `slot` / `tuple` are live and consistent; `offp`/`slowp` writable.
 */
#[inline]
unsafe fn slot_deform_heap_tuple_internal(
    slot: *mut TupleTableSlot,
    tuple: HeapTuple,
    mut attnum: c_int,
    natts: c_int,
    slow: bool,
    hasnulls: bool,
    offp: *mut uint32,
    slowp: *mut bool,
) -> c_int {
    let tupleDesc = (*slot).tts_tupleDescriptor;
    let values = (*slot).tts_values;
    let isnull = (*slot).tts_isnull;
    let tup = (*tuple).t_data;
    let bp = (*tup).t_bits.as_ptr(); /* ptr to null bitmap in tuple */
    let mut slownext = false;

    let tp = (tup as *mut c_char).add((*tup).t_hoff as usize); /* ptr to tuple data */

    while attnum < natts {
        let thisatt: *mut CompactAttribute = TupleDescCompactAttr(tupleDesc, attnum);

        if hasnulls && att_isnull(attnum, bp) {
            *values.add(attnum as usize) = 0 as Datum;
            *isnull.add(attnum as usize) = true;
            if !slow {
                *slowp = true;
                return attnum + 1;
            } else {
                attnum += 1;
                continue;
            }
        }

        *isnull.add(attnum as usize) = false;

        /* calculate the offset of this attribute */
        if !slow && (*thisatt).attcacheoff >= 0 {
            *offp = (*thisatt).attcacheoff as uint32;
        } else if (*thisatt).attlen == -1 {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no pad
             * bytes in any case: then the offset will be valid for either an
             * aligned or unaligned value.
             */
            if !slow && *offp as usize == att_nominal_alignby(*offp as usize, (*thisatt).attalignby) {
                (*thisatt).attcacheoff = *offp as int32;
            } else {
                *offp = att_pointer_alignby(
                    *offp as usize,
                    (*thisatt).attalignby,
                    -1,
                    tp.add(*offp as usize),
                ) as uint32;

                if !slow {
                    slownext = true;
                }
            }
        } else {
            /* not varlena, so safe to use att_nominal_alignby */
            *offp = att_nominal_alignby(*offp as usize, (*thisatt).attalignby) as uint32;

            if !slow {
                (*thisatt).attcacheoff = *offp as int32;
            }
        }

        *values.add(attnum as usize) = fetchatt(thisatt, tp.add(*offp as usize));

        *offp = att_addlength_pointer(*offp as usize, (*thisatt).attlen as c_int, tp.add(*offp as usize))
            as uint32;

        /* check if we need to switch to slow mode */
        if !slow {
            /*
             * We're unable to deform any further if the above code set
             * 'slownext', or if this isn't a fixed-width attribute.
             */
            if slownext || (*thisatt).attlen <= 0 {
                *slowp = true;
                return attnum + 1;
            }
        }

        attnum += 1;
    }

    natts
}

/*
 * slot_deform_heap_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple into
 *		its Datum/isnull arrays, up through the natts'th column.  Incremental:
 *		slot->tts_nvalid is the number of attributes already extracted.
 *
 * # Safety
 * `slot`/`tuple` are live and consistent; `offp` writable.
 */
unsafe fn slot_deform_heap_tuple(slot: *mut TupleTableSlot, tuple: HeapTuple, offp: *mut uint32, natts: c_int) {
    let hasnulls = HeapTupleHasNulls(tuple);
    let mut attnum: c_int;
    let mut off: uint32; /* offset in tuple data */
    let mut slow: bool; /* can we use/set attcacheoff? */

    /* We can only fetch as many attributes as the tuple has. */
    let natts = Min(HeapTupleHeaderGetNatts((*tuple).t_data) as c_int, natts);

    /*
     * Check whether this is the first call for this tuple, and initialize or
     * restore loop state.
     */
    attnum = (*slot).tts_nvalid as c_int;
    if attnum == 0 {
        /* Start from the first attribute */
        off = 0;
        slow = false;
    } else {
        /* Restore state from previous execution */
        off = *offp;
        slow = TTS_SLOW(slot);
    }

    /*
     * If 'slow' isn't set, try deforming without the extra non-fixed-offset
     * checks; switch to slow mode on the first NULL or var-length attribute.
     * (In C these calls are inlined to specialize on the const args.)
     */
    if !slow {
        if !hasnulls {
            attnum = slot_deform_heap_tuple_internal(
                slot, tuple, attnum, natts, false, /* slow */
                false, /* hasnulls */
                &mut off, &mut slow,
            );
        } else {
            attnum = slot_deform_heap_tuple_internal(
                slot, tuple, attnum, natts, false, /* slow */
                true, /* hasnulls */
                &mut off, &mut slow,
            );
        }
    }

    /* If there's still work to do then we must be in slow mode */
    if attnum < natts {
        attnum = slot_deform_heap_tuple_internal(
            slot, tuple, attnum, natts, true, /* slow */
            hasnulls, &mut off, &mut slow,
        );
    }

    /*
     * Save state for next execution
     */
    (*slot).tts_nvalid = attnum as AttrNumber;
    *offp = off;
    if slow {
        (*slot).tts_flags |= TTS_FLAG_SLOW;
    } else {
        (*slot).tts_flags &= !TTS_FLAG_SLOW;
    }
}

/*
 * fetchatt(att, T): fetch_att over a CompactAttribute (tupmacs.h #define).
 *
 * # Safety
 * `att` is a live CompactAttribute; `T` points to a properly-aligned field of at
 * least attlen readable bytes.
 */
#[inline]
unsafe fn fetchatt(att: *const CompactAttribute, T: *const c_char) -> Datum {
    crate::access::tupmacs::fetch_att(T as *const c_void, (*att).attbyval, (*att).attlen as c_int)
}

/*
 * att_isnull - re-export for use here (tupmacs.h).
 */
use crate::access::tupmacs::att_isnull;

// ============================================================================
//   Predefined TupleTableSlotOps statics.
//
//   Field ORDER follows the TupleTableSlotOps struct in tuptable.rs exactly; we
//   fill every field with Some(fn)/None positionally (by name for clarity).
// ============================================================================

pub static TTSOpsVirtual: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: size_of::<VirtualTupleTableSlot>(),
    init: Some(tts_virtual_init),
    release: Some(tts_virtual_release),
    clear: Some(tts_virtual_clear),
    getsomeattrs: Some(tts_virtual_getsomeattrs),
    getsysattr: Some(tts_virtual_getsysattr),
    is_current_xact_tuple: Some(tts_virtual_is_current_xact_tuple),
    materialize: Some(tts_virtual_materialize),
    copyslot: Some(tts_virtual_copyslot),

    /*
     * A virtual tuple table slot can not "own" a heap tuple or a minimal tuple.
     */
    get_heap_tuple: None,
    get_minimal_tuple: None,
    copy_heap_tuple: Some(tts_virtual_copy_heap_tuple),
    copy_minimal_tuple: Some(tts_virtual_copy_minimal_tuple),
};

pub static TTSOpsHeapTuple: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: size_of::<HeapTupleTableSlot>(),
    init: Some(tts_heap_init),
    release: Some(tts_heap_release),
    clear: Some(tts_heap_clear),
    getsomeattrs: Some(tts_heap_getsomeattrs),
    getsysattr: Some(tts_heap_getsysattr),
    is_current_xact_tuple: Some(tts_heap_is_current_xact_tuple),
    materialize: Some(tts_heap_materialize),
    copyslot: Some(tts_heap_copyslot),
    get_heap_tuple: Some(tts_heap_get_heap_tuple),

    /* A heap tuple table slot can not "own" a minimal tuple. */
    get_minimal_tuple: None,
    copy_heap_tuple: Some(tts_heap_copy_heap_tuple),
    copy_minimal_tuple: Some(tts_heap_copy_minimal_tuple),
};

pub static TTSOpsMinimalTuple: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: size_of::<MinimalTupleTableSlot>(),
    init: Some(tts_minimal_init),
    release: Some(tts_minimal_release),
    clear: Some(tts_minimal_clear),
    getsomeattrs: Some(tts_minimal_getsomeattrs),
    getsysattr: Some(tts_minimal_getsysattr),
    is_current_xact_tuple: Some(tts_minimal_is_current_xact_tuple),
    materialize: Some(tts_minimal_materialize),
    copyslot: Some(tts_minimal_copyslot),

    /* A minimal tuple table slot can not "own" a heap tuple. */
    get_heap_tuple: None,
    get_minimal_tuple: Some(tts_minimal_get_minimal_tuple),
    copy_heap_tuple: Some(tts_minimal_copy_heap_tuple),
    copy_minimal_tuple: Some(tts_minimal_copy_minimal_tuple),
};

pub static TTSOpsBufferHeapTuple: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: size_of::<BufferHeapTupleTableSlot>(),
    init: Some(tts_buffer_heap_init),
    release: Some(tts_buffer_heap_release),
    clear: Some(tts_buffer_heap_clear),
    getsomeattrs: Some(tts_buffer_heap_getsomeattrs),
    getsysattr: Some(tts_buffer_heap_getsysattr),
    is_current_xact_tuple: Some(tts_buffer_is_current_xact_tuple),
    materialize: Some(tts_buffer_heap_materialize),
    copyslot: Some(tts_buffer_heap_copyslot),
    get_heap_tuple: Some(tts_buffer_heap_get_heap_tuple),

    /* A buffer heap tuple table slot can not "own" a minimal tuple. */
    get_minimal_tuple: None,
    copy_heap_tuple: Some(tts_buffer_heap_copy_heap_tuple),
    copy_minimal_tuple: Some(tts_buffer_heap_copy_minimal_tuple),
};

// ============================================================================
//   tuple table create/delete functions
// ============================================================================

/* --------------------------------
 *		MakeTupleTableSlot
 *
 *		Basic routine to make an empty TupleTableSlot of the given type.  If
 *		tupleDesc is specified the slot's descriptor is fixed for its lifetime,
 *		gaining some efficiency; otherwise pass NULL.
 * --------------------------------
 *
 * # Safety
 * `tupleDesc` is null or live; `tts_ops` is one of the TTSOps* statics.
 */
pub unsafe fn MakeTupleTableSlot(
    tupleDesc: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    let basesz: Size = (*tts_ops).base_slot_size;
    let allocsz: Size;

    /*
     * When a fixed descriptor is specified, we can reduce overhead by
     * allocating the entire slot in one go.
     */
    if !tupleDesc.is_null() {
        allocsz = MAXALIGN(basesz)
            + MAXALIGN((*tupleDesc).natts as usize * size_of::<Datum>())
            + MAXALIGN((*tupleDesc).natts as usize * size_of::<bool>());
    } else {
        allocsz = basesz;
    }

    let slot = palloc0(allocsz) as *mut TupleTableSlot;
    /* const for optimization purposes, OK to modify at allocation time */
    (*slot).tts_ops = tts_ops;
    (*slot).r#type = T_TupleTableSlot;
    (*slot).tts_flags |= TTS_FLAG_EMPTY;
    if !tupleDesc.is_null() {
        (*slot).tts_flags |= TTS_FLAG_FIXED;
    }
    (*slot).tts_tupleDescriptor = tupleDesc;
    (*slot).tts_mcxt = CurrentMemoryContext;
    (*slot).tts_nvalid = 0;

    if !tupleDesc.is_null() {
        (*slot).tts_values =
            (slot as *mut c_char).add(MAXALIGN(basesz)) as *mut Datum;
        (*slot).tts_isnull = (slot as *mut c_char)
            .add(MAXALIGN(basesz) + MAXALIGN((*tupleDesc).natts as usize * size_of::<Datum>()))
            as *mut bool;

        PinTupleDesc(tupleDesc);
    }

    /*
     * And allow slot type specific initialization.
     */
    ((*(*slot).tts_ops).init.unwrap())(slot);

    slot
}

/* --------------------------------
 *		ExecAllocTableSlot
 *
 *		Create a tuple table slot within a tuple table (which is just a List).
 * --------------------------------
 *
 * # Safety
 * `tupleTable` points to a writable `*mut List`; `desc`/`tts_ops` as above.
 */
pub unsafe fn ExecAllocTableSlot(
    tupleTable: *mut *mut List,
    desc: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    let slot = MakeTupleTableSlot(desc, tts_ops);

    *tupleTable = lappend(*tupleTable, slot as *mut c_void);

    slot
}

/* --------------------------------
 *		ExecResetTupleTable
 *
 *		Release any resources (buffer pins, tupdesc refcounts) held by the tuple
 *		table, and optionally release the memory occupied by the tuple table
 *		data structure.  Expected to be called by ExecEndPlan().
 * --------------------------------
 *
 * # Safety
 * `tupleTable` is a List of *mut TupleTableSlot.
 */
#[no_mangle]
pub unsafe fn ExecResetTupleTable(tupleTable: *mut List, shouldFree: bool) {
    foreach!(lc, tupleTable, {
        let slot = lfirst_node!(TupleTableSlot, T_TupleTableSlot, current_cell!(lc));

        /* Always release resources and reset the slot to empty */
        ExecClearTuple(slot);
        ((*(*slot).tts_ops).release.unwrap())(slot);
        if !(*slot).tts_tupleDescriptor.is_null() {
            ReleaseTupleDesc((*slot).tts_tupleDescriptor);
            (*slot).tts_tupleDescriptor = null_mut();
        }

        /* If shouldFree, release memory occupied by the slot itself */
        if shouldFree {
            if !TTS_FIXED(slot) {
                if !(*slot).tts_values.is_null() {
                    pfree((*slot).tts_values as *mut c_void);
                }
                if !(*slot).tts_isnull.is_null() {
                    pfree((*slot).tts_isnull as *mut c_void);
                }
            }
            pfree(slot as *mut c_void);
        }
    });

    /* If shouldFree, release the list structure */
    if shouldFree {
        list_free(tupleTable);
    }
}

/* --------------------------------
 *		MakeSingleTupleTableSlot
 *
 *		Convenience routine for a standalone TupleTableSlot not part of the main
 *		executor tuple table.
 * --------------------------------
 *
 * # Safety
 * See [`MakeTupleTableSlot`].
 */
pub unsafe fn MakeSingleTupleTableSlot(
    tupdesc: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    MakeTupleTableSlot(tupdesc, tts_ops)
}

/* --------------------------------
 *		ExecDropSingleTupleTableSlot
 *
 *		Release a TupleTableSlot made with MakeSingleTupleTableSlot.  DON'T use
 *		this on a slot that's part of a tuple table list!
 * --------------------------------
 *
 * # Safety
 * `slot` was made by MakeSingleTupleTableSlot and is not in a tuple table.
 */
pub unsafe fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot) {
    /* This should match ExecResetTupleTable's processing of one slot */
    Assert!(IsA!(slot, T_TupleTableSlot));
    ExecClearTuple(slot);
    ((*(*slot).tts_ops).release.unwrap())(slot);
    if !(*slot).tts_tupleDescriptor.is_null() {
        ReleaseTupleDesc((*slot).tts_tupleDescriptor);
    }
    if !TTS_FIXED(slot) {
        if !(*slot).tts_values.is_null() {
            pfree((*slot).tts_values as *mut c_void);
        }
        if !(*slot).tts_isnull.is_null() {
            pfree((*slot).tts_isnull as *mut c_void);
        }
    }
    pfree(slot as *mut c_void);
}

// ============================================================================
//   tuple table slot accessor functions
// ============================================================================

/* --------------------------------
 *		ExecSetSlotDescriptor
 *
 *		Set the tuple descriptor associated with the slot's tuple.  The passed
 *		descriptor must have lifespan at least equal to the slot's; if it is a
 *		reference-counted descriptor its refcount is incremented for as long as
 *		the slot holds a reference.
 * --------------------------------
 *
 * # Safety
 * `slot` is a non-fixed slot; `tupdesc` is live and long-lived enough.
 */
pub unsafe fn ExecSetSlotDescriptor(slot: *mut TupleTableSlot, tupdesc: TupleDesc) {
    Assert!(!TTS_FIXED(slot));

    /* For safety, make sure slot is empty before changing it */
    ExecClearTuple(slot);

    /*
     * Release any old descriptor.  Also release old Datum/isnull arrays if
     * present (we don't bother to check if they could be re-used).
     */
    if !(*slot).tts_tupleDescriptor.is_null() {
        ReleaseTupleDesc((*slot).tts_tupleDescriptor);
    }

    if !(*slot).tts_values.is_null() {
        pfree((*slot).tts_values as *mut c_void);
    }
    if !(*slot).tts_isnull.is_null() {
        pfree((*slot).tts_isnull as *mut c_void);
    }

    /*
     * Install the new descriptor; if it's refcounted, bump its refcount.
     */
    (*slot).tts_tupleDescriptor = tupdesc;
    PinTupleDesc(tupdesc);

    /*
     * Allocate Datum/isnull arrays of the appropriate size.  These must have
     * the same lifetime as the slot, so allocate in the slot's own context.
     */
    (*slot).tts_values = MemoryContextAlloc(
        (*slot).tts_mcxt,
        (*tupdesc).natts as usize * size_of::<Datum>(),
    ) as *mut Datum;
    (*slot).tts_isnull = MemoryContextAlloc(
        (*slot).tts_mcxt,
        (*tupdesc).natts as usize * size_of::<bool>(),
    ) as *mut bool;
}

/* --------------------------------
 *		ExecStoreHeapTuple
 *
 *		Store an on-the-fly physical tuple into a TTSOpsHeapTuple type slot.
 *		shouldFree: true if ExecClearTuple should pfree() the tuple.  Returns the
 *		passed-in slot pointer.
 * --------------------------------
 *
 * # Safety
 * `tuple` is a valid HeapTuple; `slot` is a heap-tuple slot with a tupdesc.
 */
pub unsafe fn ExecStoreHeapTuple(
    tuple: HeapTuple,
    slot: *mut TupleTableSlot,
    shouldFree: bool,
) -> *mut TupleTableSlot {
    /*
     * sanity checks
     */
    Assert!(!tuple.is_null());
    Assert!(!slot.is_null());
    Assert!(!(*slot).tts_tupleDescriptor.is_null());

    if unlikely(!TTS_IS_HEAPTUPLE(slot)) {
        elog!(ERROR, "trying to store a heap tuple into wrong type of slot");
    }
    tts_heap_store_tuple(slot, tuple, shouldFree);

    (*slot).tts_tableOid = (*tuple).t_tableOid;

    slot
}

/* --------------------------------
 *		ExecStoreBufferHeapTuple
 *
 *		STUB: store an on-disk physical tuple from a buffer into a
 *		TTSOpsBufferHeapTuple slot (acquires a pin on the buffer).  Needs
 *		storage/bufmgr.h.
 * --------------------------------
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn ExecStoreBufferHeapTuple(
    tuple: HeapTuple,
    slot: *mut TupleTableSlot,
    buffer: Buffer,
) -> *mut TupleTableSlot {
    /*
     * sanity checks
     */
    Assert!(!tuple.is_null());
    Assert!(!slot.is_null());
    Assert!(!(*slot).tts_tupleDescriptor.is_null());
    Assert!(BufferIsValid(buffer));

    if unlikely(!TTS_IS_BUFFERTUPLE(slot)) {
        elog!(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");
    }
    tts_buffer_heap_store_tuple(slot, tuple, buffer, false);

    (*slot).tts_tableOid = (*tuple).t_tableOid;

    slot
}

/*
 * Like ExecStoreBufferHeapTuple, but transfer an existing pin from the caller to
 * the slot.  STUB (storage/bufmgr.h).
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn ExecStorePinnedBufferHeapTuple(
    tuple: HeapTuple,
    slot: *mut TupleTableSlot,
    buffer: Buffer,
) -> *mut TupleTableSlot {
    /*
     * sanity checks
     */
    Assert!(!tuple.is_null());
    Assert!(!slot.is_null());
    Assert!(!(*slot).tts_tupleDescriptor.is_null());
    Assert!(BufferIsValid(buffer));

    if unlikely(!TTS_IS_BUFFERTUPLE(slot)) {
        elog!(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");
    }
    tts_buffer_heap_store_tuple(slot, tuple, buffer, true);

    (*slot).tts_tableOid = (*tuple).t_tableOid;

    slot
}

/*
 * Store a minimal tuple into a TTSOpsMinimalTuple type slot.
 *
 * # Safety
 * `mtup` is a valid MinimalTuple; `slot` is a minimal-tuple slot with a tupdesc.
 */
pub unsafe fn ExecStoreMinimalTuple(
    mtup: MinimalTuple,
    slot: *mut TupleTableSlot,
    shouldFree: bool,
) -> *mut TupleTableSlot {
    /*
     * sanity checks
     */
    Assert!(!mtup.is_null());
    Assert!(!slot.is_null());
    Assert!(!(*slot).tts_tupleDescriptor.is_null());

    if unlikely(!TTS_IS_MINIMALTUPLE(slot)) {
        elog!(ERROR, "trying to store a minimal tuple into wrong type of slot");
    }
    tts_minimal_store_tuple(slot, mtup, shouldFree);

    slot
}

/*
 * Store a HeapTuple into any kind of slot, performing conversion if necessary.
 *
 * # Safety
 * `tuple` is a valid HeapTuple; `slot` is any slot with a matching tupdesc.
 */
pub unsafe fn ExecForceStoreHeapTuple(tuple: HeapTuple, slot: *mut TupleTableSlot, shouldFree: bool) {
    if TTS_IS_HEAPTUPLE(slot) {
        ExecStoreHeapTuple(tuple, slot, shouldFree);
    } else if TTS_IS_BUFFERTUPLE(slot) {
        let bslot = slot as *mut BufferHeapTupleTableSlot;

        ExecClearTuple(slot);
        (*slot).tts_flags &= !TTS_FLAG_EMPTY;
        let oldContext = MemoryContextSwitchTo((*slot).tts_mcxt);
        (*bslot).base.tuple = heap_copytuple(tuple);
        (*slot).tts_flags |= TTS_FLAG_SHOULDFREE;
        MemoryContextSwitchTo(oldContext);

        if shouldFree {
            pfree(tuple as *mut c_void);
        }
    } else {
        ExecClearTuple(slot);
        heap_deform_tuple(
            tuple,
            (*slot).tts_tupleDescriptor,
            (*slot).tts_values,
            (*slot).tts_isnull,
        );
        ExecStoreVirtualTuple(slot);

        if shouldFree {
            ExecMaterializeSlot(slot);
            pfree(tuple as *mut c_void);
        }
    }
}

/*
 * Store a MinimalTuple into any kind of slot, performing conversion if
 * necessary.
 *
 * # Safety
 * `mtup` is a valid MinimalTuple; `slot` is any slot with a matching tupdesc.
 */
pub unsafe fn ExecForceStoreMinimalTuple(mtup: MinimalTuple, slot: *mut TupleTableSlot, shouldFree: bool) {
    if TTS_IS_MINIMALTUPLE(slot) {
        tts_minimal_store_tuple(slot, mtup, shouldFree);
    } else {
        let mut htup: HeapTupleData = core::mem::zeroed();

        ExecClearTuple(slot);

        htup.t_len = (*mtup).t_len + MINIMAL_TUPLE_OFFSET as uint32;
        htup.t_data =
            (mtup as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize)) as HeapTupleHeader;
        heap_deform_tuple(
            &mut htup,
            (*slot).tts_tupleDescriptor,
            (*slot).tts_values,
            (*slot).tts_isnull,
        );
        ExecStoreVirtualTuple(slot);

        if shouldFree {
            ExecMaterializeSlot(slot);
            pfree(mtup as *mut c_void);
        }
    }
}

/* --------------------------------
 *		ExecStoreVirtualTuple
 *			Mark a slot as containing a virtual tuple.
 *
 * Protocol: ExecClearTuple -> store data into Datum/isnull arrays ->
 * ExecStoreVirtualTuple.
 * --------------------------------
 *
 * # Safety
 * `slot` is EMPTY and has a tupdesc; the Datum/isnull arrays are populated.
 */
#[no_mangle]
pub unsafe fn ExecStoreVirtualTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    /*
     * sanity checks
     */
    Assert!(!slot.is_null());
    Assert!(!(*slot).tts_tupleDescriptor.is_null());
    Assert!(TTS_EMPTY(slot));

    (*slot).tts_flags &= !TTS_FLAG_EMPTY;
    (*slot).tts_nvalid = (*(*slot).tts_tupleDescriptor).natts as AttrNumber;

    slot
}

/* --------------------------------
 *		ExecStoreAllNullTuple
 *			Set up the slot to contain a null in every column.
 *
 * Unlike ExecClearTuple, the slot ends up full, not empty.
 * --------------------------------
 *
 * # Safety
 * `slot` has a tupdesc.
 */
pub unsafe fn ExecStoreAllNullTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    /*
     * sanity checks
     */
    Assert!(!slot.is_null());
    Assert!(!(*slot).tts_tupleDescriptor.is_null());

    /* Clear any old contents */
    ExecClearTuple(slot);

    /*
     * Fill all the columns of the virtual tuple with nulls
     */
    MemSet(
        (*slot).tts_values as *mut c_void,
        0,
        (*(*slot).tts_tupleDescriptor).natts as usize * size_of::<Datum>(),
    );
    memset(
        (*slot).tts_isnull as *mut c_void,
        true as c_int,
        (*(*slot).tts_tupleDescriptor).natts as usize * size_of::<bool>(),
    );

    ExecStoreVirtualTuple(slot)
}

/*
 * Store a HeapTuple in datum form, into a slot.  Always requires deforming it
 * and storing it in virtual form.
 *
 * STUB: needs DatumGetHeapTupleHeader (an fmgr.h macro) +
 * HeapTupleHeaderGetDatumLength; the deform/virtual-store tail is real but the
 * Datum->HeapTupleHeader unpacking is not yet ported.
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn ExecStoreHeapTupleDatum(data: Datum, slot: *mut TupleTableSlot) {
    let mut tuple: HeapTupleData = core::mem::zeroed();

    let td: HeapTupleHeader = DatumGetHeapTupleHeader(data);

    tuple.t_len = HeapTupleHeaderGetDatumLength(td);
    tuple.t_self = (*td).t_ctid;
    tuple.t_data = td;

    ExecClearTuple(slot);

    heap_deform_tuple(
        &mut tuple,
        (*slot).tts_tupleDescriptor,
        (*slot).tts_values,
        (*slot).tts_isnull,
    );
    ExecStoreVirtualTuple(slot);
}

/*
 * ExecFetchSlotHeapTuple - fetch HeapTuple representing the slot's content.
 *
 * If materialize is true, the contents are made independent from underlying
 * storage.  If shouldFree is non-NULL it is set true when the returned tuple was
 * palloc'd in the caller's context and must be freed by the caller.
 *
 * # Safety
 * `slot` is non-EMPTY; `shouldFree` is null or writable.
 */
pub unsafe fn ExecFetchSlotHeapTuple(
    slot: *mut TupleTableSlot,
    materialize: bool,
    shouldFree: *mut bool,
) -> HeapTuple {
    /*
     * sanity checks
     */
    Assert!(!slot.is_null());
    Assert!(!TTS_EMPTY(slot));

    /* Materialize the tuple so that the slot "owns" it, if requested. */
    if materialize {
        ((*(*slot).tts_ops).materialize.unwrap())(slot);
    }

    if (*(*slot).tts_ops).get_heap_tuple.is_none() {
        if !shouldFree.is_null() {
            *shouldFree = true;
        }
        ((*(*slot).tts_ops).copy_heap_tuple.unwrap())(slot)
    } else {
        if !shouldFree.is_null() {
            *shouldFree = false;
        }
        ((*(*slot).tts_ops).get_heap_tuple.unwrap())(slot)
    }
}

/* --------------------------------
 *		ExecFetchSlotMinimalTuple
 *			Fetch the slot's minimal physical tuple.
 *
 * If the slot can hold a minimal tuple (non-NULL get_minimal_tuple), the slot
 * owns the returned tuple and *shouldFree is set false (read-only).  Otherwise
 * copy_minimal_tuple is called and *shouldFree is set true.
 * --------------------------------
 *
 * # Safety
 * `slot` is non-EMPTY; `shouldFree` is null or writable.
 */
pub unsafe fn ExecFetchSlotMinimalTuple(slot: *mut TupleTableSlot, shouldFree: *mut bool) -> MinimalTuple {
    /*
     * sanity checks
     */
    Assert!(!slot.is_null());
    Assert!(!TTS_EMPTY(slot));

    if (*(*slot).tts_ops).get_minimal_tuple.is_some() {
        if !shouldFree.is_null() {
            *shouldFree = false;
        }
        ((*(*slot).tts_ops).get_minimal_tuple.unwrap())(slot)
    } else {
        if !shouldFree.is_null() {
            *shouldFree = true;
        }
        ((*(*slot).tts_ops).copy_minimal_tuple.unwrap())(slot, 0)
    }
}

/* --------------------------------
 *		ExecFetchSlotHeapTupleDatum
 *			Fetch the slot's tuple as a composite-type Datum.
 *
 *		The result is always freshly palloc'd in the caller's memory context.
 * --------------------------------
 *
 * # Safety
 * `slot` is non-EMPTY with a live tupdesc.
 */
pub unsafe fn ExecFetchSlotHeapTupleDatum(slot: *mut TupleTableSlot) -> Datum {
    let mut shouldFree: bool = false;

    /* Fetch slot's contents in regular-physical-tuple form */
    let tup = ExecFetchSlotHeapTuple(slot, false, &mut shouldFree);
    let tupdesc = (*slot).tts_tupleDescriptor;

    /* Convert to Datum form */
    let ret = heap_copy_tuple_as_datum(tup, tupdesc);

    if shouldFree {
        pfree(tup as *mut c_void);
    }

    ret
}

// ============================================================================
//   Routines for setting/accessing attributes in a slot.
// ============================================================================

/*
 * Fill in missing values for a TupleTableSlot.
 *
 * This is only exposed because it's needed for JIT compiled tuple deforming.
 * That exception aside, there should be no callers outside of this file.
 *
 * # Safety
 * `slot` is live; startAttNum/lastAttNum are within the tts_values/isnull arrays.
 */
pub unsafe fn slot_getmissingattrs(slot: *mut TupleTableSlot, startAttNum: c_int, lastAttNum: c_int) {
    let mut attrmiss: *mut crate::access::common::tupdesc::AttrMissing = null_mut();

    if !(*(*slot).tts_tupleDescriptor).constr.is_null() {
        attrmiss = (*(*(*slot).tts_tupleDescriptor).constr).missing;
    }

    if attrmiss.is_null() {
        /* no missing values array at all, so just fill everything in as NULL */
        memset(
            (*slot).tts_values.add(startAttNum as usize) as *mut c_void,
            0,
            (lastAttNum - startAttNum) as usize * size_of::<Datum>(),
        );
        memset(
            (*slot).tts_isnull.add(startAttNum as usize) as *mut c_void,
            1,
            (lastAttNum - startAttNum) as usize * size_of::<bool>(),
        );
    } else {
        /* if there is a missing values array we must process them one by one */
        for missattnum in startAttNum..lastAttNum {
            *(*slot).tts_values.add(missattnum as usize) = (*attrmiss.add(missattnum as usize)).am_value;
            *(*slot).tts_isnull.add(missattnum as usize) =
                !(*attrmiss.add(missattnum as usize)).am_present;
        }
    }
}

/*
 * slot_getsomeattrs_int - workhorse for slot_getsomeattrs()
 *
 * # Safety
 * `slot` is live; `attnum` > 0.
 */
pub unsafe fn slot_getsomeattrs_int(slot: *mut TupleTableSlot, attnum: c_int) {
    /* Check for caller errors */
    Assert!(((*slot).tts_nvalid as c_int) < attnum); /* checked in slot_getsomeattrs */
    Assert!(attnum > 0);

    if unlikely(attnum > (*(*slot).tts_tupleDescriptor).natts) {
        elog!(ERROR, "invalid attribute number {}", attnum);
    }

    /* Fetch as many attributes as possible from the underlying tuple. */
    ((*(*slot).tts_ops).getsomeattrs.unwrap())(slot, attnum);

    /*
     * If the underlying tuple doesn't have enough attributes, the tuple
     * descriptor must have the missing attributes.
     */
    if unlikely(((*slot).tts_nvalid as c_int) < attnum) {
        slot_getmissingattrs(slot, (*slot).tts_nvalid as c_int, attnum);
        (*slot).tts_nvalid = attnum as AttrNumber;
    }
}

use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::utils::fmgr::{FmgrInfo, InputFunctionCall};
use crate::utils::cache::lsyscache::getTypeInputInfo;
use crate::utils::fmgr::fmgr_info;
use crate::nodes::pg_list::list_length;
use crate::utils::adt::name::namestrcpy;

// funcapi.h: AttInMetadata.  The real def lives in crate::utils::fmgr::funcapi
// (utils/fmgr/funcapi.c), which is not yet mounted as a module, so it is not
// reachable here.  Mirror the layout locally so the type-from-tuple helpers
// stay self-consistent.
// TODO(pg-port): import from crate::utils::fmgr::funcapi once that module is wired.
#[repr(C)]
pub struct AttInMetadata {
    pub tupdesc: TupleDesc,
    pub attinfuncs: *mut FmgrInfo,
    pub attioparams: *mut Oid,
    pub atttypmods: *mut int32,
}
use crate::utils::cache::typcache::{assign_record_type_typmod, lookup_rowtype_tupdesc};
use crate::access::heap::heaptoast::toast_flatten_tuple_to_datum;
use crate::access::htup_details::{
    HeapTupleHeaderHasExternal, HeapTupleHeaderGetTypeId, HeapTupleHeaderGetTypMod,
    HeapTupleHeaderData,
};
use crate::access::common::tupdesc::{TupleDescAttr, AttrMissing};
use crate::catalog::pg_attribute::FormData_pg_attribute;
use crate::nodes::makefuncs::RECORDOID;
use crate::nodes::value::String as ValueString;
use crate::utils::palloc::palloc0;
use crate::strVal;

type Form_pg_attribute = *mut FormData_pg_attribute;

/* ----------------------------------------------------------------
 *				convenience initialization routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExecInitResultTypeTL
 *
 *		Initialize result type, using the plan node's targetlist.
 * ----------------
 */
pub unsafe fn ExecInitResultTypeTL(planstate: *mut PlanState) {
    let tupDesc: TupleDesc = ExecTypeFromTL((*(*planstate).plan).targetlist);

    (*planstate).ps_ResultTupleDesc = tupDesc;
}

/* --------------------------------
 *		ExecInit{Result,Scan,Extra}TupleSlot[TL]
 *
 *		These are convenience routines to initialize the specified slot
 *		in nodes inheriting the appropriate state.  ExecInitExtraTupleSlot
 *		is used for initializing special-purpose slots.
 * --------------------------------
 */

/* ----------------
 *		ExecInitResultSlot
 *
 *		Initialize result tuple slot, using the tuple descriptor previously
 *		computed with ExecInitResultTypeTL().
 * ----------------
 */
pub unsafe fn ExecInitResultSlot(planstate: *mut PlanState, tts_ops: *const TupleTableSlotOps) {
    let slot: *mut TupleTableSlot;

    slot = ExecAllocTableSlot(
        &raw mut (*(*planstate).state).es_tupleTable,
        (*planstate).ps_ResultTupleDesc,
        tts_ops,
    );
    (*planstate).ps_ResultTupleSlot = slot;

    (*planstate).resultopsfixed = !(*planstate).ps_ResultTupleDesc.is_null();
    (*planstate).resultops = tts_ops;
    (*planstate).resultopsset = true;
}

/* ----------------
 *		ExecInitResultTupleSlotTL
 *
 *		Initialize result tuple slot, using the plan node's targetlist.
 * ----------------
 */
pub unsafe fn ExecInitResultTupleSlotTL(
    planstate: *mut PlanState,
    tts_ops: *const TupleTableSlotOps,
) {
    ExecInitResultTypeTL(planstate);
    ExecInitResultSlot(planstate, tts_ops);
}

/* ----------------
 *		ExecInitScanTupleSlot
 * ----------------
 */
pub unsafe fn ExecInitScanTupleSlot(
    estate: *mut EState,
    scanstate: *mut ScanState,
    tupledesc: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) {
    (*scanstate).ss_ScanTupleSlot =
        ExecAllocTableSlot(&raw mut (*estate).es_tupleTable, tupledesc, tts_ops);
    (*scanstate).ps.scandesc = tupledesc;
    (*scanstate).ps.scanopsfixed = !tupledesc.is_null();
    (*scanstate).ps.scanops = tts_ops;
    (*scanstate).ps.scanopsset = true;
}

/* ----------------
 *		ExecInitExtraTupleSlot
 *
 * Return a newly created slot. If tupledesc is non-NULL the slot will have
 * that as its fixed tupledesc. Otherwise the caller needs to use
 * ExecSetSlotDescriptor() to set the descriptor before use.
 * ----------------
 */
pub unsafe fn ExecInitExtraTupleSlot(
    estate: *mut EState,
    tupledesc: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    ExecAllocTableSlot(&raw mut (*estate).es_tupleTable, tupledesc, tts_ops)
}

/* ----------------
 *		ExecInitNullTupleSlot
 *
 * Build a slot containing an all-nulls tuple of the given type.
 * This is used as a substitute for an input tuple when performing an
 * outer join.
 * ----------------
 */
pub unsafe fn ExecInitNullTupleSlot(
    estate: *mut EState,
    tupType: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot = ExecInitExtraTupleSlot(estate, tupType, tts_ops);

    ExecStoreAllNullTuple(slot)
}

/* ----------------------------------------------------------------
 *		ExecTypeFromTL
 *
 *		Generate a tuple descriptor for the result tuple of a targetlist.
 *		(A parse/plan tlist must be passed, not an ExprState tlist.)
 *		Note that resjunk columns, if any, are included in the result.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecTypeFromTL(targetList: *mut List) -> TupleDesc {
    ExecTypeFromTLInternal(targetList, false)
}

/* ----------------------------------------------------------------
 *		ExecCleanTypeFromTL
 *
 *		Same as above, but resjunk columns are omitted from the result.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecCleanTypeFromTL(targetList: *mut List) -> TupleDesc {
    ExecTypeFromTLInternal(targetList, true)
}

// ----------------------------------------------------------------
//      ExecTypeFromTLInternal
//
//      Builds a TupleDesc from a targetlist, optionally skipping junk columns.
// ----------------------------------------------------------------
unsafe fn ExecTypeFromTLInternal(targetList: *mut List, skipjunk: bool) -> TupleDesc {
    let typeInfo: TupleDesc;
    let len: c_int;
    let mut cur_resno: c_int = 1;

    if skipjunk {
        len = ExecCleanTargetListLength(targetList);
    } else {
        len = ExecTargetListLength(targetList);
    }
    typeInfo = CreateTemplateTupleDesc(len);

    foreach!(l, targetList, {
        let tle = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut TargetEntry;

        if skipjunk && (*tle).resjunk {
            continue;
        }
        TupleDescInitEntry(
            typeInfo,
            cur_resno as AttrNumber,
            (*tle).resname,
            exprType((*tle).expr as *const Node),
            exprTypmod((*tle).expr as *const Node),
            0,
        );
        TupleDescInitEntryCollation(
            typeInfo,
            cur_resno as AttrNumber,
            exprCollation((*tle).expr as *const Node),
        );
        cur_resno += 1;
    });

    typeInfo
}

/* ----------------------------------------------------------------
 *		ExecTypeFromExprList
 *
 *		Creates a tuple descriptor from a list of Exprs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecTypeFromExprList(exprList: *mut List) -> TupleDesc {
    let typeInfo: TupleDesc;
    let mut cur_resno: c_int = 1;

    typeInfo = CreateTemplateTupleDesc(list_length(exprList));

    foreach!(lc, exprList, {
        let e = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut Node;

        TupleDescInitEntry(
            typeInfo,
            cur_resno as AttrNumber,
            std::ptr::null(),
            exprType(e as *const Node),
            exprTypmod(e as *const Node),
            0,
        );
        TupleDescInitEntryCollation(
            typeInfo,
            cur_resno as AttrNumber,
            exprCollation(e as *const Node),
        );
        cur_resno += 1;
    });

    typeInfo
}

/*
 * ExecTypeSetColNames - set column names in a RECORD TupleDesc
 *
 * Column names must be provided as an alias list (list of String nodes).
 */
pub unsafe fn ExecTypeSetColNames(typeInfo: TupleDesc, namesList: *mut List) {
    let mut colno: c_int = 0;

    /* It's only OK to change col names in a not-yet-blessed RECORD type */
    debug_assert!((*typeInfo).tdtypeid == RECORDOID);
    debug_assert!((*typeInfo).tdtypmod < 0);

    foreach!(lc, namesList, {
        let cname = strVal!(crate::nodes::pg_list::lfirst(current_cell!(lc))) as *mut c_char;
        let attr: Form_pg_attribute;

        /* Guard against too-long names list (probably can't happen) */
        if colno >= (*typeInfo).natts {
            break;
        }
        attr = TupleDescAttr(typeInfo, colno);
        colno += 1;

        /*
         * Do nothing for empty aliases or dropped columns (these cases
         * probably can't arise in RECORD types, either)
         */
        if *cname == 0 || (*attr).attisdropped {
            continue;
        }

        /* OK, assign the column name */
        namestrcpy(&raw mut (*attr).attname, cname);
    });
}

/*
 * BlessTupleDesc - make a completed tuple descriptor useful for SRFs
 *
 * Rowtype Datums returned by a function must contain valid type information.
 * This happens "for free" if the tupdesc came from a relcache entry, but
 * not if we have manufactured a tupdesc for a transient RECORD datatype.
 * In that case we have to notify typcache.c of the existence of the type.
 */
#[no_mangle]
pub unsafe fn BlessTupleDesc(tupdesc: TupleDesc) -> TupleDesc {
    if (*tupdesc).tdtypeid == RECORDOID && (*tupdesc).tdtypmod < 0 {
        assign_record_type_typmod(tupdesc);
    }

    tupdesc /* just for notational convenience */
}

/*
 * TupleDescGetAttInMetadata - Build an AttInMetadata structure based on the
 * supplied TupleDesc. AttInMetadata can be used in conjunction with C strings
 * to produce a properly formed tuple.
 */
pub unsafe fn TupleDescGetAttInMetadata(tupdesc: TupleDesc) -> *mut AttInMetadata {
    let natts: c_int = (*tupdesc).natts;
    let mut i: c_int;
    let mut attinfuncid: Oid = 0;
    let attinfuncinfo: *mut FmgrInfo;
    let attioparams: *mut Oid;
    let atttypmods: *mut int32;
    let attinmeta: *mut AttInMetadata;

    attinmeta = palloc(size_of::<AttInMetadata>()) as *mut AttInMetadata;

    /* "Bless" the tupledesc so that we can make rowtype datums with it */
    (*attinmeta).tupdesc = BlessTupleDesc(tupdesc);

    /*
     * Gather info needed later to call the "in" function for each attribute
     */
    attinfuncinfo = palloc0(natts as usize * size_of::<FmgrInfo>()) as *mut FmgrInfo;
    attioparams = palloc0(natts as usize * size_of::<Oid>()) as *mut Oid;
    atttypmods = palloc0(natts as usize * size_of::<int32>()) as *mut int32;

    i = 0;
    while i < natts {
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);

        /* Ignore dropped attributes */
        if !(*att).attisdropped {
            let atttypeid: Oid = (*att).atttypid;
            getTypeInputInfo(atttypeid, &raw mut attinfuncid, attioparams.add(i as usize));
            fmgr_info(attinfuncid, attinfuncinfo.add(i as usize));
            *atttypmods.add(i as usize) = (*att).atttypmod;
        }
        i += 1;
    }
    (*attinmeta).attinfuncs = attinfuncinfo;
    (*attinmeta).attioparams = attioparams;
    (*attinmeta).atttypmods = atttypmods;

    attinmeta
}

/*
 * BuildTupleFromCStrings - build a HeapTuple given user data in C string form.
 * values is an array of C strings, one for each attribute of the return tuple.
 * A NULL string pointer indicates we want to create a NULL field.
 */
pub unsafe fn BuildTupleFromCStrings(
    attinmeta: *mut AttInMetadata,
    values: *mut *mut c_char,
) -> HeapTuple {
    let tupdesc: TupleDesc = (*attinmeta).tupdesc;
    let natts: c_int = (*tupdesc).natts;
    let dvalues: *mut Datum;
    let nulls: *mut bool;
    let mut i: c_int;
    let tuple: HeapTuple;

    dvalues = palloc(natts as usize * size_of::<Datum>()) as *mut Datum;
    nulls = palloc(natts as usize * size_of::<bool>()) as *mut bool;

    /*
     * Call the "in" function for each non-dropped attribute, even for nulls,
     * to support domains.
     */
    i = 0;
    while i < natts {
        if !(*TupleDescCompactAttr(tupdesc, i)).attisdropped {
            /* Non-dropped attributes */
            *dvalues.add(i as usize) = InputFunctionCall(
                (*attinmeta).attinfuncs.add(i as usize),
                *values.add(i as usize),
                *(*attinmeta).attioparams.add(i as usize),
                *(*attinmeta).atttypmods.add(i as usize),
            );
            if !(*values.add(i as usize)).is_null() {
                *nulls.add(i as usize) = false;
            } else {
                *nulls.add(i as usize) = true;
            }
        } else {
            /* Handle dropped attributes by setting to NULL */
            *dvalues.add(i as usize) = 0 as Datum;
            *nulls.add(i as usize) = true;
        }
        i += 1;
    }

    /*
     * Form a tuple
     */
    tuple = heap_form_tuple(tupdesc, dvalues, nulls);

    /*
     * Release locally palloc'd space.  XXX would probably be good to pfree
     * values of pass-by-reference datums, as well.
     */
    pfree(dvalues as *mut c_void);
    pfree(nulls as *mut c_void);

    tuple
}

/*
 * HeapTupleHeaderGetDatum - convert a HeapTupleHeader pointer to a Datum.
 *
 * This must *not* get applied to an on-disk tuple; the tuple should be
 * freshly made by heap_form_tuple or some wrapper routine for it (such as
 * BuildTupleFromCStrings).  Be sure also that the tupledesc used to build
 * the tuple has a properly "blessed" rowtype.
 *
 * Formerly this was a macro equivalent to PointerGetDatum, relying on the
 * fact that heap_form_tuple fills in the appropriate tuple header fields
 * for a composite Datum.  However, we now require that composite Datums not
 * contain any external TOAST pointers.  We do not want heap_form_tuple itself
 * to enforce that; more specifically, the rule applies only to actual Datums
 * and not to HeapTuple structures.  Therefore, HeapTupleHeaderGetDatum is
 * now a function that detects whether there are externally-toasted fields
 * and constructs a new tuple with inlined fields if so.  We still need
 * heap_form_tuple to insert the Datum header fields, because otherwise this
 * code would have no way to obtain a tupledesc for the tuple.
 */
#[no_mangle]
pub unsafe fn HeapTupleHeaderGetDatum(tuple: HeapTupleHeader) -> Datum {
    let result: Datum;
    let tupDesc: TupleDesc;

    /* No work if there are no external TOAST pointers in the tuple */
    if !HeapTupleHeaderHasExternal(tuple) {
        return PointerGetDatum(tuple as *const c_void);
    }

    /* Use the type data saved by heap_form_tuple to look up the rowtype */
    tupDesc = lookup_rowtype_tupdesc(
        HeapTupleHeaderGetTypeId(tuple),
        HeapTupleHeaderGetTypMod(tuple),
    );

    /* And do the flattening */
    result = toast_flatten_tuple_to_datum(
        tuple,
        HeapTupleHeaderGetDatumLength(tuple),
        tupDesc,
    );

    ReleaseTupleDesc(tupDesc);

    result
}

// ============================================================================
//   Functions for sending tuples to the frontend (or other specified
//   destination) as though it is a SELECT result.  Translated 1:1 from execTuples.c.
//   The supporting types (DestReceiver, TupOutputState) are imported from the
//   already-ported tcop/dest and executor modules.
// ============================================================================

use crate::tcop::dest::DestReceiver;
use crate::executor::executor::TupOutputState;
use crate::nodes::nodes::CmdType;
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::utils::builtins::cstring_to_text_with_len;

/*
 * Functions for sending tuples to the frontend (or other specified destination)
 * as though it is a SELECT result. These are used by utility commands that
 * need to project directly to the destination and don't need or want full
 * table function capability. Currently used by EXPLAIN and SHOW ALL.
 */
pub unsafe fn begin_tup_output_tupdesc(
    dest: *mut DestReceiver,
    tupdesc: TupleDesc,
    tts_ops: *const TupleTableSlotOps,
) -> *mut TupOutputState {
    let tstate: *mut TupOutputState;

    tstate = palloc(size_of::<TupOutputState>()) as *mut TupOutputState;

    (*tstate).slot = MakeSingleTupleTableSlot(tupdesc, tts_ops);
    (*tstate).dest = dest;

    ((*(*tstate).dest).rStartup.unwrap())((*tstate).dest, CmdType::CMD_SELECT as c_int, tupdesc);

    tstate
}

/*
 * write a single tuple
 */
pub unsafe fn do_tup_output(
    tstate: *mut TupOutputState,
    values: *const Datum,
    isnull: *const bool,
) {
    let slot: *mut TupleTableSlot = (*tstate).slot;
    let natts: c_int = (*(*slot).tts_tupleDescriptor).natts;

    /* make sure the slot is clear */
    ExecClearTuple(slot);

    /* insert data */
    memcpy(
        (*slot).tts_values as *mut c_void,
        values as *const c_void,
        natts as usize * size_of::<Datum>(),
    );
    memcpy(
        (*slot).tts_isnull as *mut c_void,
        isnull as *const c_void,
        natts as usize * size_of::<bool>(),
    );

    /* mark slot as containing a virtual tuple */
    ExecStoreVirtualTuple(slot);

    /* send the tuple to the receiver */
    ((*(*tstate).dest).receiveSlot.unwrap())(slot, (*tstate).dest);

    /* clean up */
    ExecClearTuple(slot);
}

/*
 * write a chunk of text, breaking at newline characters
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
pub unsafe fn do_text_output_multiline(tstate: *mut TupOutputState, mut txt: *const c_char) {
    let mut values: [Datum; 1] = [0 as Datum];
    let isnull: [bool; 1] = [false];

    while *txt != 0 {
        let eol: *const c_char;
        let len: c_int;

        let found = strchr(txt, b'\n' as c_int);
        if !found.is_null() {
            len = found.offset_from(txt) as c_int;
            eol = found.add(1);
        } else {
            len = strlen(txt) as c_int;
            eol = txt.add(len as usize);
        }

        values[0] = PointerGetDatum(cstring_to_text_with_len(txt, len) as *const c_void);
        do_tup_output(tstate, values.as_ptr(), isnull.as_ptr());
        pfree(DatumGetPointer(values[0]) as *mut c_void);
        txt = eol;
    }
}

pub unsafe fn end_tup_output(tstate: *mut TupOutputState) {
    ((*(*tstate).dest).rShutdown.unwrap())((*tstate).dest);
    /* note that destroying the dest is not ours to do */
    ExecDropSingleTupleTableSlot((*tstate).slot);
    pfree(tstate as *mut c_void);
}

extern "C" {
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;
}

// ============================================================================
//   Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::{
        CreateTemplateTupleDesc, TupleDescInitBuiltinEntry,
    };
    use crate::catalog::pg_type_d::INT4OID;

    /*
     * Build a 2-column (INT4 "a", INT4 "b") TupleDesc.  The builtin-entry
     * initializer also runs populate_compact_attribute, so the CompactAttribute
     * mirror (consumed by slot_deform_heap_tuple) is valid.
     */
    unsafe fn make_ab_desc() -> TupleDesc {
        let td = CreateTemplateTupleDesc(2);
        TupleDescInitBuiltinEntry(td, 1, c"a".as_ptr(), INT4OID, -1, 0);
        TupleDescInitBuiltinEntry(td, 2, c"b".as_ptr(), INT4OID, -1, 0);
        td
    }

    /*
     * Heap-tuple slot: form a [11, 22] row, store it, and read both attributes
     * back via slot_getattr (which drives tts_heap_getsomeattrs ->
     * slot_deform_heap_tuple).  Then ExecClearTuple makes the slot EMPTY.
     */
    #[test]
    fn heap_slot_store_and_getattr() {
        unsafe {
            let td = make_ab_desc();
            let slot = MakeSingleTupleTableSlot(td, &TTSOpsHeapTuple);

            let mut values: [Datum; 2] = [Int32GetDatum(11), Int32GetDatum(22)];
            let mut isnull: [bool; 2] = [false, false];
            let tuple = heap_form_tuple(td, values.as_mut_ptr(), isnull.as_mut_ptr());

            ExecStoreHeapTuple(tuple, slot, true);
            assert!(!TTS_EMPTY(slot));

            let mut isn: bool = true;
            let a = slot_getattr(slot, 1, &mut isn);
            assert!(!isn);
            assert_eq!(DatumGetInt32(a), 11);

            let b = slot_getattr(slot, 2, &mut isn);
            assert!(!isn);
            assert_eq!(DatumGetInt32(b), 22);

            ExecClearTuple(slot);
            assert!(TTS_EMPTY(slot));

            ExecDropSingleTupleTableSlot(slot);
        }
    }

    /*
     * Virtual slot: set tts_values/tts_isnull directly, mark valid with
     * ExecStoreVirtualTuple, then read them back via slot_getattr.
     */
    #[test]
    fn virtual_slot_store_and_getattr() {
        unsafe {
            let td = make_ab_desc();
            let slot = MakeSingleTupleTableSlot(td, &TTSOpsVirtual);

            /* Protocol: clear, fill arrays, store-virtual. */
            ExecClearTuple(slot);
            *(*slot).tts_values.add(0) = Int32GetDatum(101);
            *(*slot).tts_isnull.add(0) = false;
            *(*slot).tts_values.add(1) = Int32GetDatum(202);
            *(*slot).tts_isnull.add(1) = false;
            ExecStoreVirtualTuple(slot);
            assert!(!TTS_EMPTY(slot));

            let mut isn: bool = true;
            let a = slot_getattr(slot, 1, &mut isn);
            assert!(!isn);
            assert_eq!(DatumGetInt32(a), 101);

            let b = slot_getattr(slot, 2, &mut isn);
            assert!(!isn);
            assert_eq!(DatumGetInt32(b), 202);

            /* ExecStoreAllNullTuple makes every column null but the slot full. */
            ExecStoreAllNullTuple(slot);
            assert!(!TTS_EMPTY(slot));
            let _ = slot_getattr(slot, 1, &mut isn);
            assert!(isn);

            ExecDropSingleTupleTableSlot(slot);
        }
    }

    /*
     * The four op tables advertise base sizes matching their concrete slot types,
     * and the TTS_IS_* address predicates select the right one.
     */
    #[test]
    fn optable_sizes_and_identity() {
        assert_eq!(TTSOpsVirtual.base_slot_size, size_of::<VirtualTupleTableSlot>());
        assert_eq!(TTSOpsHeapTuple.base_slot_size, size_of::<HeapTupleTableSlot>());
        assert_eq!(TTSOpsMinimalTuple.base_slot_size, size_of::<MinimalTupleTableSlot>());
        assert_eq!(
            TTSOpsBufferHeapTuple.base_slot_size,
            size_of::<BufferHeapTupleTableSlot>()
        );

        unsafe {
            let td = make_ab_desc();
            let hslot = MakeSingleTupleTableSlot(td, &TTSOpsHeapTuple);
            assert!(TTS_IS_HEAPTUPLE(hslot));
            assert!(!TTS_IS_VIRTUAL(hslot));
            assert!(!TTS_IS_MINIMALTUPLE(hslot));
            assert!(!TTS_IS_BUFFERTUPLE(hslot));
            ExecDropSingleTupleTableSlot(hslot);

            let vslot = MakeSingleTupleTableSlot(td, &TTSOpsVirtual);
            assert!(TTS_IS_VIRTUAL(vslot));
            ExecDropSingleTupleTableSlot(vslot);
        }
    }
    // NOTE: the List-based ExecResetTupleTable path is not exercised here (it needs
    // a populated tuple-table List); MakeSingleTupleTableSlot/ExecDropSingleTupleTableSlot
    // cover the equivalent per-slot create/clear/release/free logic.
}
