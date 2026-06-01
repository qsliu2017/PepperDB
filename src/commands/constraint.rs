//! commands/constraint.c - PostgreSQL CONSTRAINT support code.

use crate::prelude::*;

use crate::access::index::amapi::IndexUniqueCheck;
use crate::access::relscan::IndexFetchTableData;
use crate::commands::trigger::{
    TriggerData, CALLED_AS_TRIGGER, TRIGGER_FIRED_AFTER, TRIGGER_FIRED_BY_INSERT,
    TRIGGER_FIRED_BY_UPDATE, TRIGGER_FIRED_FOR_ROW,
};
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{EState, ExprContext, IndexInfo};
use crate::nodes::pg_list::{List, NIL};
use crate::postgres::PointerGetDatum;
use crate::storage::itemptr::{ItemPointerData, ItemPointerSetInvalid};
use crate::storage::lockdefs::RowExclusiveLock;
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::rel::Relation;
use crate::utils::snapshot::Snapshot;

use crate::access::common::indextuple::INDEX_MAX_KEYS;

// Translatable error strings are shared with ri_triggers.c; resist the
// temptation to fold the function name into them.  Value matches the
// placeholder used by other ported trigger functions (trigfuncs.rs).
const ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED: c_int = 0;

// ---------------------------------------------------------------------------
// Local stubs for not-yet-ported callees.
// ---------------------------------------------------------------------------

// TODO(pg-port): real `index_open` in catalog/index.c / access/index/indexam.c.
unsafe fn index_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!()
}

// TODO(pg-port): real `index_close` in access/index/indexam.c.
unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!()
}

// TODO(pg-port): real `BuildIndexInfo` in catalog/index.c.
unsafe fn BuildIndexInfo(_index: Relation) -> *mut IndexInfo {
    unimplemented!()
}

// TODO(pg-port): real `FormIndexDatum` in catalog/index.c.
unsafe fn FormIndexDatum(
    _indexInfo: *mut IndexInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!()
}

// TODO(pg-port): real `index_insert` in access/index/indexam.c.
unsafe fn index_insert(
    _indexRelation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_t_ctid: *mut ItemPointerData,
    _heapRelation: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: *mut IndexInfo,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): real `index_insert_cleanup` in access/index/indexam.c.
unsafe fn index_insert_cleanup(_indexRelation: Relation, _indexInfo: *mut IndexInfo) {
    unimplemented!()
}

// TODO(pg-port): real `check_exclusion_constraint` in executor/execIndexing.c.
unsafe fn check_exclusion_constraint(
    _heap: Relation,
    _index: Relation,
    _indexInfo: *mut IndexInfo,
    _tupleid: *mut ItemPointerData,
    _values: *mut Datum,
    _isnull: *mut bool,
    _estate: *mut EState,
    _newIndex: bool,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): real `table_slot_create` in access/table/tableam.c.
unsafe fn table_slot_create(_relation: Relation, _reglist: *mut *mut List) -> *mut TupleTableSlot {
    unimplemented!()
}

// TODO(pg-port): real `table_index_fetch_begin` in access/table/tableam.h.
unsafe fn table_index_fetch_begin(_rel: Relation) -> *mut IndexFetchTableData {
    unimplemented!()
}

// TODO(pg-port): real `table_index_fetch_tuple` in access/table/tableam.h.
unsafe fn table_index_fetch_tuple(
    _scan: *mut IndexFetchTableData,
    _tid: *mut ItemPointerData,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): real `table_index_fetch_end` in access/table/tableam.h.
unsafe fn table_index_fetch_end(_scan: *mut IndexFetchTableData) {
    unimplemented!()
}

// TODO(pg-port): real `CreateExecutorState` in executor/execUtils.c.
unsafe fn CreateExecutorState() -> *mut EState {
    unimplemented!()
}

// TODO(pg-port): real `FreeExecutorState` in executor/execUtils.c.
unsafe fn FreeExecutorState(_estate: *mut EState) {
    unimplemented!()
}

// TODO(pg-port): real `GetPerTupleExprContext` macro in executor/executor.h.
unsafe fn GetPerTupleExprContext(_estate: *mut EState) -> *mut ExprContext {
    unimplemented!()
}

// TODO(pg-port): real `ExecDropSingleTupleTableSlot` in executor/execTuples.c.
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {
    unimplemented!()
}

// SnapshotSelf - TODO(pg-port): real global in utils/time/snapmgr.c.
unsafe fn snapshot_self() -> Snapshot {
    unimplemented!()
}

// tg_trigger->tgconstrindid accessor. The `Trigger` struct in commands::trigger
// is currently an opaque stub, so the constraint index OID cannot be read
// through a field. TODO(pg-port): replace with `(*tg_trigger).tgconstrindid`
// once the real `Trigger` catalog struct lands.
unsafe fn trigger_tgconstrindid(
    _tg_trigger: *mut crate::commands::trigger::Trigger,
) -> Oid {
    unimplemented!()
}

// UNIQUE_CHECK_EXISTING from access/genam.h's IndexUniqueCheck enum
// (UNIQUE_CHECK_NO=0, UNIQUE_CHECK_YES=1, UNIQUE_CHECK_PARTIAL=2,
// UNIQUE_CHECK_EXISTING=3).
const UNIQUE_CHECK_EXISTING: IndexUniqueCheck = 3;

/*
 * unique_key_recheck - trigger function to do a deferred uniqueness check.
 *
 * This now also does deferred exclusion-constraint checks, so the name is
 * somewhat historical.
 *
 * This is invoked as an AFTER ROW trigger for both INSERT and UPDATE,
 * for any rows recorded as potentially violating a deferrable unique
 * or exclusion constraint.
 *
 * This may be an end-of-statement check, a commit-time check, or a
 * check triggered by a SET CONSTRAINTS command.
 */
pub unsafe fn unique_key_recheck(fcinfo: FunctionCallInfo) -> Datum {
    let trigdata: *mut TriggerData = (*fcinfo).context as *mut TriggerData;
    let funcname = "unique_key_recheck";
    let mut checktid: ItemPointerData = core::mem::zeroed();
    let mut tmptid: ItemPointerData;
    let indexRel: Relation;
    let indexInfo: *mut IndexInfo;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let slot: *mut TupleTableSlot;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];

    /*
     * Make sure this is being called as an AFTER ROW trigger.  Note:
     * translatable error strings are shared with ri_triggers.c, so resist the
     * temptation to fold the function name into them.
     */
    if !CALLED_AS_TRIGGER(fcinfo) {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            format!("function \"{}\" was not called by trigger manager", funcname)
        );
    }

    if !TRIGGER_FIRED_AFTER((*trigdata).tg_event) || !TRIGGER_FIRED_FOR_ROW((*trigdata).tg_event) {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            format!("function \"{}\" must be fired AFTER ROW", funcname)
        );
    }

    /*
     * Get the new data that was inserted/updated.
     */
    if TRIGGER_FIRED_BY_INSERT((*trigdata).tg_event) {
        checktid = (*(*trigdata).tg_trigslot).tts_tid;
    } else if TRIGGER_FIRED_BY_UPDATE((*trigdata).tg_event) {
        checktid = (*(*trigdata).tg_newslot).tts_tid;
    } else {
        let _ = errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        ereport!(
            ERROR,
            format!(
                "function \"{}\" must be fired for INSERT or UPDATE",
                funcname
            )
        );
        #[allow(unreachable_code)]
        ItemPointerSetInvalid(&mut checktid); /* keep compiler quiet */
    }

    slot = table_slot_create((*trigdata).tg_relation, null_mut());

    /*
     * If the row pointed at by checktid is now dead (ie, inserted and then
     * deleted within our transaction), we can skip the check.  However, we
     * have to be careful, because this trigger gets queued only in response
     * to index insertions; which means it does not get queued e.g. for HOT
     * updates.  The row we are called for might now be dead, but have a live
     * HOT child, in which case we still need to make the check ---
     * effectively, we're applying the check against the live child row,
     * although we can use the values from this row since by definition all
     * columns of interest to us are the same.
     *
     * This might look like just an optimization, because the index AM will
     * make this identical test before throwing an error.  But it's actually
     * needed for correctness, because the index AM will also throw an error
     * if it doesn't find the index entry for the row.  If the row's dead then
     * it's possible the index entry has also been marked dead, and even
     * removed.
     */
    tmptid = checktid;
    {
        let scan: *mut IndexFetchTableData = table_index_fetch_begin((*trigdata).tg_relation);
        let mut call_again: bool = false;

        if !table_index_fetch_tuple(
            scan,
            &mut tmptid,
            snapshot_self(),
            slot,
            &mut call_again,
            null_mut(),
        ) {
            /*
             * All rows referenced by the index entry are dead, so skip the
             * check.
             */
            ExecDropSingleTupleTableSlot(slot);
            table_index_fetch_end(scan);
            return PointerGetDatum(null());
        }
        table_index_fetch_end(scan);
    }

    /*
     * Open the index, acquiring a RowExclusiveLock, just as if we were going
     * to update it.  (This protects against possible changes of the index
     * schema, not against concurrent updates.)
     */
    indexRel = index_open(
        trigger_tgconstrindid((*trigdata).tg_trigger),
        RowExclusiveLock,
    );
    indexInfo = BuildIndexInfo(indexRel);

    /*
     * Typically the index won't have expressions, but if it does we need an
     * EState to evaluate them.  We need it for exclusion constraints too,
     * even if they are just on simple columns.
     */
    if (*indexInfo).ii_Expressions != NIL || !(*indexInfo).ii_ExclusionOps.is_null() {
        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);
        (*econtext).ecxt_scantuple = slot;
    } else {
        estate = null_mut();
    }

    /*
     * Form the index values and isnull flags for the index entry that we need
     * to check.
     *
     * Note: if the index uses functions that are not as immutable as they are
     * supposed to be, this could produce an index tuple different from the
     * original.  The index AM can catch such errors by verifying that it
     * finds a matching index entry with the tuple's TID.  For exclusion
     * constraints we check this in check_exclusion_constraint().
     */
    FormIndexDatum(
        indexInfo,
        slot,
        estate,
        values.as_mut_ptr(),
        isnull.as_mut_ptr(),
    );

    /*
     * Now do the appropriate check.
     */
    if (*indexInfo).ii_ExclusionOps.is_null() {
        /*
         * Note: this is not a real insert; it is a check that the index entry
         * that has already been inserted is unique.  Passing the tuple's tid
         * (i.e. unmodified by table_index_fetch_tuple()) is correct even if
         * the row is now dead, because that is the TID the index will know
         * about.
         */
        index_insert(
            indexRel,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
            &mut checktid,
            (*trigdata).tg_relation,
            UNIQUE_CHECK_EXISTING,
            false,
            indexInfo,
        );

        /* Cleanup cache possibly initialized by index_insert. */
        index_insert_cleanup(indexRel, indexInfo);
    } else {
        /*
         * For exclusion constraints we just do the normal check, but now it's
         * okay to throw error.  In the HOT-update case, we must use the live
         * HOT child's TID here, else check_exclusion_constraint will think
         * the child is a conflict.
         */
        check_exclusion_constraint(
            (*trigdata).tg_relation,
            indexRel,
            indexInfo,
            &mut tmptid,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
            estate,
            false,
        );
    }

    /*
     * If that worked, then this index entry is unique or non-excluded, and we
     * are done.
     */
    if !estate.is_null() {
        FreeExecutorState(estate);
    }

    ExecDropSingleTupleTableSlot(slot);

    index_close(indexRel, RowExclusiveLock);

    PointerGetDatum(null())
}
