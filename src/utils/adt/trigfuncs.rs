//! Translation of postgres/src/backend/utils/adt/trigfuncs.c
//!
//! Builtin functions for useful trigger support.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::access::htup_details::{
    HeapTuple, HeapTupleHeader, HeapTupleHeaderGetNatts, SizeofHeapTupleHeader, HEAP_XACT_MASK,
};
use crate::commands::trigger::{
    TriggerData, CALLED_AS_TRIGGER, TRIGGER_FIRED_BEFORE, TRIGGER_FIRED_BY_UPDATE,
    TRIGGER_FIRED_FOR_ROW,
};
use crate::utils::fmgr::FunctionCallInfo;
use core::ffi::c_void;

extern "C" {
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

/* errcodes.h classification (the errcode() shim ignores the value). */
const ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED: c_int = 0;

/*
 * suppress_redundant_updates_trigger
 *
 * This trigger function will inhibit an update from being done if the OLD and
 * NEW records are identical.
 */
pub unsafe fn suppress_redundant_updates_trigger(fcinfo: FunctionCallInfo) -> Datum {
    let trigdata = (*fcinfo).context as *mut TriggerData;
    let newtuple: HeapTuple;
    let oldtuple: HeapTuple;
    let mut rettuple: HeapTuple;

    /* make sure it's called as a trigger */
    if !CALLED_AS_TRIGGER(fcinfo) {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            errmsg!("suppress_redundant_updates_trigger: must be called as trigger")
        );
    }

    /* and that it's called on update */
    if !TRIGGER_FIRED_BY_UPDATE((*trigdata).tg_event) {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            errmsg!("suppress_redundant_updates_trigger: must be called on update")
        );
    }

    /* and that it's called before update */
    if !TRIGGER_FIRED_BEFORE((*trigdata).tg_event) {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            errmsg!("suppress_redundant_updates_trigger: must be called before update")
        );
    }

    /* and that it's called for each row */
    if !TRIGGER_FIRED_FOR_ROW((*trigdata).tg_event) {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            errmsg!("suppress_redundant_updates_trigger: must be called for each row")
        );
    }

    /* get tuple data, set default result */
    newtuple = (*trigdata).tg_newtuple;
    rettuple = newtuple;
    oldtuple = (*trigdata).tg_trigtuple;

    let newheader: HeapTupleHeader = (*newtuple).t_data;
    let oldheader: HeapTupleHeader = (*oldtuple).t_data;

    /* if the tuple payload is the same ... */
    if (*newtuple).t_len == (*oldtuple).t_len
        && (*newheader).t_hoff == (*oldheader).t_hoff
        && HeapTupleHeaderGetNatts(newheader) == HeapTupleHeaderGetNatts(oldheader)
        && ((*newheader).t_infomask & !HEAP_XACT_MASK)
            == ((*oldheader).t_infomask & !HEAP_XACT_MASK)
        && memcmp(
            (newheader as *const c_char).add(SizeofHeapTupleHeader) as *const c_void,
            (oldheader as *const c_char).add(SizeofHeapTupleHeader) as *const c_void,
            ((*newtuple).t_len as usize) - SizeofHeapTupleHeader,
        ) == 0
    {
        /* ... then suppress the update */
        rettuple = null_mut();
    }

    PointerGetDatum(rettuple as *const c_void)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
    use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry};
    use crate::catalog::pg_type_d::INT4OID;
    use crate::commands::trigger::TRIGGER_EVENT_UPDATE;
    use crate::nodes::nodes::NodeTag;
    use crate::postgres::Int32GetDatum;

    // Build a minimal TriggerData with the two tuples set; call the trigger.
    unsafe fn run(new: HeapTuple, old: HeapTuple) -> Datum {
        let mut td: TriggerData = core::mem::zeroed();
        td.r#type = NodeTag::T_TriggerData;
        // before-update, for-each-row event
        td.tg_event = TRIGGER_EVENT_UPDATE | 0x0004 /*ROW*/ | 0x0008 /*BEFORE*/;
        td.tg_trigtuple = old;
        td.tg_newtuple = new;

        let mut fcinfo: crate::utils::fmgr::FunctionCallInfoBaseData = core::mem::zeroed();
        fcinfo.context = &mut td as *mut TriggerData as *mut crate::nodes::nodes::Node;
        suppress_redundant_updates_trigger(&mut fcinfo)
    }

    #[test]
    fn identical_tuples_are_suppressed() {
        unsafe {
            let td = CreateTemplateTupleDesc(2);
            TupleDescInitBuiltinEntry(td, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(td, 2, c"b".as_ptr(), INT4OID, -1, 0);

            let v1 = [Int32GetDatum(1), Int32GetDatum(2)];
            let v2 = [Int32GetDatum(1), Int32GetDatum(9)];
            let isnull = [false; 2];
            let same_a = heap_form_tuple(td, v1.as_ptr(), isnull.as_ptr());
            let same_b = heap_form_tuple(td, v1.as_ptr(), isnull.as_ptr());
            let diff = heap_form_tuple(td, v2.as_ptr(), isnull.as_ptr());

            // identical payload -> NULL (suppressed)
            assert!(crate::postgres::DatumGetPointer(run(same_a, same_b)).is_null());
            // different payload -> returns the new tuple unchanged
            assert!(!crate::postgres::DatumGetPointer(run(diff, same_a)).is_null());

            heap_freetuple(same_a);
            heap_freetuple(same_b);
            heap_freetuple(diff);
        }
    }
}
