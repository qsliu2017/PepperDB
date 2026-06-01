//! tstoreReceiver.rs
//!   An implementation of DestReceiver that stores the result tuples in
//!   a Tuplestore.
//!
//! Optionally, we can force detoasting (but not decompression) of out-of-line
//! toasted values.  This is to support cursors WITH HOLD, which must retain
//! data even if the underlying table is dropped.
//!
//! Also optionally, we can apply a tuple conversion map before storing.
//!
//! Translated 1:1 from postgres/src/backend/executor/tstoreReceiver.c
//! (PostgreSQL 18.x).  Declarations from executor/tstoreReceiver.h:
//!   CreateTuplestoreDestReceiver, SetTuplestoreDestReceiverParams.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// access/detoast.h
use crate::access::common::detoast::detoast_external_attr;
// access/tupconvert.h
use crate::access::common::tupconvert::{
    convert_tuples_by_position, execute_attr_map_slot, free_conversion_map, TupleConversionMap,
};
// access/tupdesc.h
use crate::access::common::tupdesc::{TupleDesc, TupleDescCompactAttr};
// executor/tuptable.h
use crate::executor::execTuples::{
    ExecDropSingleTupleTableSlot, MakeSingleTupleTableSlot, TTSOpsVirtual,
};
use crate::executor::tuptable::{slot_getallattrs, TupleTableSlot};
// tcop/dest.h
use crate::tcop::dest::{DestReceiver, DestTuplestore};
// varatt.h
use crate::varatt::VARATT_IS_EXTERNAL;

// -----------------------------------------------------------------------------
// STUBS for unported utils/tuplestore.h (Tuplestorestate + put routines).
// The Tuplestore module is not ported yet; model the state as an opaque type
// and make the two "put" entry points no-ops with a TODO.
// -----------------------------------------------------------------------------

/// STUB: utils/tuplestore not ported.  Opaque Tuplestorestate handle.
pub type Tuplestorestate = c_void;

/// STUB: tuplestore_puttupleslot (utils/tuplestore.c not ported).
unsafe fn tuplestore_puttupleslot(_state: *mut Tuplestorestate, _slot: *mut TupleTableSlot) {
    // TODO(pg-port): wire to utils/sort/tuplestore.rs once ported.
}

/// STUB: tuplestore_putvalues (utils/tuplestore.c not ported).
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    // TODO(pg-port): wire to utils/sort/tuplestore.rs once ported.
}

// -----------------------------------------------------------------------------

#[repr(C)]
pub struct TStoreState {
    pub pub_: DestReceiver,
    /* parameters: */
    /// where to put the data
    pub tstore: *mut Tuplestorestate,
    /// context containing tstore
    pub cxt: MemoryContext,
    /// were we told to detoast?
    pub detoast: bool,
    /// target tupdesc, or NULL if none
    pub target_tupdesc: TupleDesc,
    /// tupdesc mapping failure message
    pub map_failure_msg: *const c_char,
    /* workspace: */
    /// values array for result tuple
    pub outvalues: *mut Datum,
    /// temp values to be pfree'd
    pub tofree: *mut Datum,
    /// conversion map, if needed
    pub tupmap: *mut TupleConversionMap,
    /// slot for mapped tuples
    pub mapslot: *mut TupleTableSlot,
}

/// Prepare to receive tuples from executor.
unsafe fn tstoreStartupReceiver(self_: *mut DestReceiver, _operation: c_int, typeinfo: TupleDesc) {
    let myState = self_ as *mut TStoreState;
    let mut needtoast = false;
    let natts: c_int = (*typeinfo).natts;

    /* Check if any columns require detoast work */
    if (*myState).detoast {
        let mut i: c_int = 0;
        while i < natts {
            let attr = TupleDescCompactAttr(typeinfo, i);

            if (*attr).attisdropped {
                i += 1;
                continue;
            }
            if (*attr).attlen == -1 {
                needtoast = true;
                break;
            }
            i += 1;
        }
    }

    /* Check if tuple conversion is needed */
    if !(*myState).target_tupdesc.is_null() {
        let msg: &str = if (*myState).map_failure_msg.is_null() {
            ""
        } else {
            // map_failure_msg is a static C string; reinterpret for the &str arg.
            cstr_to_str((*myState).map_failure_msg)
        };
        (*myState).tupmap =
            convert_tuples_by_position(typeinfo, (*myState).target_tupdesc, msg);
    } else {
        (*myState).tupmap = null_mut();
    }

    /* Set up appropriate callback */
    if needtoast {
        Assert!((*myState).tupmap.is_null());
        (*myState).pub_.receiveSlot = Some(tstoreReceiveSlot_detoast);
        /* Create workspace */
        (*myState).outvalues = MemoryContextAlloc(
            (*myState).cxt,
            (natts as Size).wrapping_mul(core::mem::size_of::<Datum>() as Size),
        ) as *mut Datum;
        (*myState).tofree = MemoryContextAlloc(
            (*myState).cxt,
            (natts as Size).wrapping_mul(core::mem::size_of::<Datum>() as Size),
        ) as *mut Datum;
        (*myState).mapslot = null_mut();
    } else if !(*myState).tupmap.is_null() {
        (*myState).pub_.receiveSlot = Some(tstoreReceiveSlot_tupmap);
        (*myState).outvalues = null_mut();
        (*myState).tofree = null_mut();
        (*myState).mapslot = MakeSingleTupleTableSlot(
            (*myState).target_tupdesc,
            &TTSOpsVirtual as *const _,
        );
    } else {
        (*myState).pub_.receiveSlot = Some(tstoreReceiveSlot_notoast);
        (*myState).outvalues = null_mut();
        (*myState).tofree = null_mut();
        (*myState).mapslot = null_mut();
    }
}

/// Receive a tuple from the executor and store it in the tuplestore.
/// This is for the easy case where we don't have to detoast nor map anything.
unsafe fn tstoreReceiveSlot_notoast(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState = self_ as *mut TStoreState;

    tuplestore_puttupleslot((*myState).tstore, slot);

    true
}

/// Receive a tuple from the executor and store it in the tuplestore.
/// This is for the case where we have to detoast any toasted values.
unsafe fn tstoreReceiveSlot_detoast(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState = self_ as *mut TStoreState;
    let typeinfo: TupleDesc = (*slot).tts_tupleDescriptor;
    let natts: c_int = (*typeinfo).natts;
    let mut nfree: c_int;
    let oldcxt: MemoryContext;

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /*
     * Fetch back any out-of-line datums.  We build the new datums array in
     * myState->outvalues[] (but we can re-use the slot's isnull array). Also,
     * remember the fetched values to free afterwards.
     */
    nfree = 0;
    let mut i: c_int = 0;
    while i < natts {
        let mut val: Datum = *(*slot).tts_values.add(i as usize);
        let attr = TupleDescCompactAttr(typeinfo, i);

        if !(*attr).attisdropped
            && (*attr).attlen == -1
            && !*(*slot).tts_isnull.add(i as usize)
        {
            if VARATT_IS_EXTERNAL(DatumGetPointer(val) as *const c_char) {
                val = PointerGetDatum(detoast_external_attr(
                    DatumGetPointer(val) as *mut varlena,
                ) as *const c_void);
                *(*myState).tofree.add(nfree as usize) = val;
                nfree += 1;
            }
        }

        *(*myState).outvalues.add(i as usize) = val;
        i += 1;
    }

    /*
     * Push the modified tuple into the tuplestore.
     */
    oldcxt = MemoryContextSwitchTo((*myState).cxt);
    tuplestore_putvalues(
        (*myState).tstore,
        typeinfo,
        (*myState).outvalues,
        (*slot).tts_isnull,
    );
    MemoryContextSwitchTo(oldcxt);

    /* And release any temporary detoasted values */
    let mut k: c_int = 0;
    while k < nfree {
        pfree(DatumGetPointer(*(*myState).tofree.add(k as usize)) as *mut c_void);
        k += 1;
    }

    true
}

/// Receive a tuple from the executor and store it in the tuplestore.
/// This is for the case where we must apply a tuple conversion map.
unsafe fn tstoreReceiveSlot_tupmap(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState = self_ as *mut TStoreState;

    execute_attr_map_slot(
        (*(*myState).tupmap).attrMap,
        slot,
        (*myState).mapslot,
    );
    tuplestore_puttupleslot((*myState).tstore, (*myState).mapslot);

    true
}

/// Clean up at end of an executor run
unsafe fn tstoreShutdownReceiver(self_: *mut DestReceiver) {
    let myState = self_ as *mut TStoreState;

    /* Release workspace if any */
    if !(*myState).outvalues.is_null() {
        pfree((*myState).outvalues as *mut c_void);
    }
    (*myState).outvalues = null_mut();
    if !(*myState).tofree.is_null() {
        pfree((*myState).tofree as *mut c_void);
    }
    (*myState).tofree = null_mut();
    if !(*myState).tupmap.is_null() {
        free_conversion_map((*myState).tupmap);
    }
    (*myState).tupmap = null_mut();
    if !(*myState).mapslot.is_null() {
        ExecDropSingleTupleTableSlot((*myState).mapslot);
    }
    (*myState).mapslot = null_mut();
}

/// Destroy receiver when done with it
unsafe fn tstoreDestroyReceiver(self_: *mut DestReceiver) {
    pfree(self_ as *mut c_void);
}

/// Initially create a DestReceiver object.
pub unsafe fn CreateTuplestoreDestReceiver() -> *mut DestReceiver {
    let self_ = palloc0(core::mem::size_of::<TStoreState>() as Size) as *mut TStoreState;

    (*self_).pub_.receiveSlot = Some(tstoreReceiveSlot_notoast); /* might change */
    (*self_).pub_.rStartup = Some(tstoreStartupReceiver);
    (*self_).pub_.rShutdown = Some(tstoreShutdownReceiver);
    (*self_).pub_.rDestroy = Some(tstoreDestroyReceiver);
    (*self_).pub_.mydest = DestTuplestore;

    /* private fields will be set by SetTuplestoreDestReceiverParams */

    self_ as *mut DestReceiver
}

/// Set parameters for a TuplestoreDestReceiver
///
/// tStore: where to store the tuples
/// tContext: memory context containing tStore
/// detoast: forcibly detoast contained data?
/// target_tupdesc: if not NULL, forcibly convert tuples to this rowtype
/// map_failure_msg: error message to use if mapping to target_tupdesc fails
///
/// We don't currently support both detoast and target_tupdesc at the same
/// time, just because no existing caller needs that combination.
pub unsafe fn SetTuplestoreDestReceiverParams(
    self_: *mut DestReceiver,
    tStore: *mut Tuplestorestate,
    tContext: MemoryContext,
    detoast: bool,
    target_tupdesc: TupleDesc,
    map_failure_msg: *const c_char,
) {
    let myState = self_ as *mut TStoreState;

    Assert!(!(detoast && !target_tupdesc.is_null()));

    Assert!((*myState).pub_.mydest == DestTuplestore);
    (*myState).tstore = tStore;
    (*myState).cxt = tContext;
    (*myState).detoast = detoast;
    (*myState).target_tupdesc = target_tupdesc;
    (*myState).map_failure_msg = map_failure_msg;
}

/// Helper: reinterpret a NUL-terminated C string as a Rust &str (for the
/// `convert_tuples_by_position` msg arg, whose Rust signature takes &str where
/// C passed a `const char *`).  Falls back to "" on invalid UTF-8.
unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    let mut len = 0usize;
    while *s.add(len) != 0 {
        len += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    core::str::from_utf8(bytes).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::CreateTemplateTupleDesc;

    /// CreateTuplestoreDestReceiver wires all four fn-ptr fields (non-None) and
    /// sets mydest = DestTuplestore.
    #[test]
    fn create_wires_vtable() {
        unsafe {
            let dr = CreateTuplestoreDestReceiver();
            assert!(!dr.is_null());
            assert!((*dr).receiveSlot.is_some());
            assert!((*dr).rStartup.is_some());
            assert!((*dr).rShutdown.is_some());
            assert!((*dr).rDestroy.is_some());
            assert_eq!((*dr).mydest, DestTuplestore);
            pfree(dr as *mut c_void);
        }
    }

    /// tstoreStartupReceiver on a tupdesc with no toastable attrs selects the
    /// notoast receive path.  We compare the resulting receiveSlot fn pointer to
    /// the notoast fn pointer.
    #[test]
    fn startup_no_toast_selects_notoast() {
        unsafe {
            // Empty tupdesc (0 attrs): nothing toastable -> notoast path.
            let tupdesc: TupleDesc = CreateTemplateTupleDesc(0);

            let dr = CreateTuplestoreDestReceiver();
            // detoast=false anyway, so the notoast path is chosen.
            SetTuplestoreDestReceiverParams(dr, null_mut(), null_mut(), false, null_mut(), null());

            ((*dr).rStartup.unwrap())(dr, 0, tupdesc);

            // The chosen receiveSlot must equal the notoast fn pointer.
            let got = (*dr).receiveSlot.unwrap() as usize;
            let want = tstoreReceiveSlot_notoast
                as unsafe fn(*mut TupleTableSlot, *mut DestReceiver) -> bool
                as usize;
            assert_eq!(got, want);

            pfree(dr as *mut c_void);
        }
    }
}
