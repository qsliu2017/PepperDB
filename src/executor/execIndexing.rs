//! execIndexing.rs
//!   support for evaluation of indexes
//!
//! Translated 1:1 from postgres/src/backend/executor/execIndexing.c
//!
//! routines for inserting index tuples and enforcing unique and
//! exclusion constraints.
//!
//! ExecInsertIndexTuples() is the main entry point.  It's called after
//! inserting a tuple to the heap, and it inserts corresponding index tuples
//! into all indexes.  At the same time, it enforces any unique and
//! exclusion constraints (see the C source header for the full discussion of
//! unique indexes, exclusion constraints, and speculative insertion).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/execIndexing.c

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]

use crate::prelude::*;

use crate::foreach;
use crate::current_cell;
use crate::IsA;

// Re-export of the strongly typed list/walker helpers used below.
use crate::nodes::pg_list::{
    lappend_oid, lfirst_oid, list_free, list_length, list_member_oid, NIL,
};
use crate::nodes::nodeFuncs::{expression_tree_walker, tree_walker_callback};
use crate::nodes::bitmapset::{bms_free, bms_is_member, bms_union};
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
// IndexUniqueCheck (access/genam.h); genam unwired -> local.
#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub enum IndexUniqueCheck {
    UNIQUE_CHECK_NO,
    UNIQUE_CHECK_YES,
    UNIQUE_CHECK_PARTIAL,
    UNIQUE_CHECK_EXISTING,
}
use crate::access::common::scankey::ScanKeyEntryInitialize;
use crate::storage::itemptr::{
    ItemPointerEquals, ItemPointerIsValid, ItemPointerSetInvalid,
};

// ---------------------------------------------------------------------------
// Type aliases for ported dependencies (mirrors the sibling executor units).
// ---------------------------------------------------------------------------

type Relation = crate::utils::rel::Relation;
type ResultRelInfo = crate::nodes::execnodes::ResultRelInfo;
type EState = crate::nodes::execnodes::EState;
type IndexInfo = crate::nodes::execnodes::IndexInfo;
type ExprContext = crate::nodes::execnodes::ExprContext;
type ExprState = crate::nodes::execnodes::ExprState;
type TupleTableSlot = crate::executor::tuptable::TupleTableSlot;
type List = crate::nodes::pg_list::List;
type ListCell = crate::nodes::pg_list::ListCell;
type Bitmapset = crate::nodes::bitmapset::Bitmapset;
type Node = crate::nodes::nodes::Node;
type Var = crate::nodes::primnodes::Var;
type ItemPointerData = crate::storage::itemptr::ItemPointerData;
type ItemPointer = *mut ItemPointerData;
type ScanKeyData = crate::access::common::scankey::ScanKeyData;
type IndexScanDesc = crate::access::relscan::IndexScanDesc;
type SnapshotData = crate::utils::snapshot::SnapshotData;
type TransactionId = crate::c::TransactionId;
type AttrNumber = crate::access::attnum::AttrNumber;
type TupleDesc = crate::access::common::tupdesc::TupleDesc;
type Form_pg_attribute = *mut crate::catalog::pg_attribute::FormData_pg_attribute;
type NameData = crate::c::NameData;
type RelationPtr = *mut Relation;
type RangeType = c_void; /* opaque; only pointer used */
type MultirangeType = c_void; /* opaque; only pointer used */
#[repr(C)]
pub struct TypeCacheEntry { pub type_id: Oid, pub typtype: c_char }
type ScanDirection = crate::access::sdir::ScanDirection;
type XLTW_Oper = c_int;

const INDEX_MAX_KEYS: usize = 32;

// Lock modes (storage/lockdefs.h).
const RowExclusiveLock: c_int = 3;

// ScanKey flags (access/skey.h).
const SK_ISNULL: c_int = 0x0001;
const SK_SEARCHNULL: c_int = 0x0010;

const InvalidOid: Oid = 0;

// ScanDirection (access/sdir.h).
const ForwardScanDirection: ScanDirection = 1;

// XLTW_Oper values (storage/lmgr.h).
const XLTW_InsertIndex: XLTW_Oper = 1;
const XLTW_RecheckExclusionConstr: XLTW_Oper = 5;

// IndexUniqueCheck enum constants (access/genam.h).
const UNIQUE_CHECK_NO: IndexUniqueCheck = IndexUniqueCheck::UNIQUE_CHECK_NO;
const UNIQUE_CHECK_YES: IndexUniqueCheck = IndexUniqueCheck::UNIQUE_CHECK_YES;
const UNIQUE_CHECK_PARTIAL: IndexUniqueCheck = IndexUniqueCheck::UNIQUE_CHECK_PARTIAL;

// errcode values referenced by ereport() (utils/errcodes.h).
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_EXCLUSION_VIOLATION: c_int = 0;
const ERRCODE_CHECK_VIOLATION: c_int = 0;

// typtype values (catalog/pg_type.h).
const TYPTYPE_RANGE: c_char = b'r' as c_char;
const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char;

// ---------------------------------------------------------------------------
// Local stubs for as-yet-unported dependencies. Each carries a TODO pointing
// at the C file that will eventually own the real symbol.
// ---------------------------------------------------------------------------

unsafe fn RelationGetForm(relation: Relation) -> *mut crate::catalog::pg_class::FormData_pg_class { (*relation).rd_rel } // utils/rel.h: RelationGetForm
unsafe fn RelationGetRelid(relation: Relation) -> Oid { unimplemented!() /* TODO(pg-port): real RelationGetRelid lives in utils/rel.h */ }
unsafe fn RelationGetRelationName(relation: Relation) -> *mut c_char { unimplemented!() /* TODO(pg-port): real RelationGetRelationName lives in utils/rel.h */ }
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc { unimplemented!() /* TODO(pg-port): real RelationGetDescr lives in utils/rel.h */ }
unsafe fn RelationGetIndexList(relation: Relation) -> *mut List {
    crate::utils::cache::relcache::RelationGetIndexList(relation as _) as _
}
unsafe fn RelationGetIndexExpressions(relation: Relation) -> *mut List {
    crate::utils::cache::relcache::RelationGetIndexExpressions(relation as _) as _
}
unsafe fn IndexRelationGetNumberOfKeyAttributes(relation: Relation) -> c_int {
    crate::access::nbtree::nbtdedup::IndexRelationGetNumberOfKeyAttributes(relation as _) as _
}

unsafe fn index_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::index::indexam::index_open(relationId as _, lockmode as _) as _
}
unsafe fn index_close(relation: Relation, lockmode: c_int) {
    crate::access::index::indexam::index_close(relation as _, lockmode as _)
}
unsafe fn index_insert(indexRelation: Relation, values: *mut Datum, isnull: *mut bool, heap_t_ctid: ItemPointer, heapRelation: Relation, checkUnique: IndexUniqueCheck, indexUnchanged: bool, indexInfo: *mut IndexInfo) -> bool { crate::access::index::indexam::index_insert(indexRelation, values, isnull, heap_t_ctid, heapRelation, checkUnique as _, indexUnchanged, indexInfo as _) }
unsafe fn index_insert_cleanup(indexRelation: Relation, indexInfo: *mut IndexInfo) {
    crate::access::index::indexam::index_insert_cleanup(indexRelation as _, indexInfo as _)
}
unsafe fn index_beginscan(heapRelation: Relation, indexRelation: Relation, snapshot: *mut SnapshotData, instrument: *mut c_void, nkeys: c_int, norderbys: c_int) -> IndexScanDesc {
    crate::access::index::indexam::index_beginscan(heapRelation as _, indexRelation as _, snapshot as _, instrument as _, nkeys as _, norderbys as _) as _
}
unsafe fn index_rescan(scan: IndexScanDesc, keys: *mut ScanKeyData, nkeys: c_int, orderbys: *mut ScanKeyData, norderbys: c_int) {
    crate::access::index::indexam::index_rescan(scan as _, keys as _, nkeys as _, orderbys as _, norderbys as _)
}
unsafe fn index_getnext_slot(scan: IndexScanDesc, direction: ScanDirection, slot: *mut TupleTableSlot) -> bool { unimplemented!() /* TODO(pg-port): real index_getnext_slot lives in access/index/indexam.c */ }
unsafe fn index_endscan(scan: IndexScanDesc) {
    crate::access::index::indexam::index_endscan(scan as _)
}

unsafe fn BuildIndexInfo(index: Relation) -> *mut IndexInfo {
    crate::catalog::index::BuildIndexInfo(index as _) as _
}
unsafe fn BuildSpeculativeIndexInfo(index: Relation, ii: *mut IndexInfo) {
    crate::catalog::index::BuildSpeculativeIndexInfo(index as _, ii as _)
}
unsafe fn BuildIndexValueDescription(indexRelation: Relation, values: *const Datum, isnull: *const bool) -> *mut c_char {
    crate::access::index::genam::BuildIndexValueDescription(indexRelation as _, values as _, isnull as _) as _
}

unsafe fn FormIndexDatum(indexInfo: *mut IndexInfo, slot: *mut TupleTableSlot, estate: *mut EState, values: *mut Datum, isnull: *mut bool) {
    crate::catalog::index::FormIndexDatum(indexInfo as _, slot as _, estate as _, values as _, isnull as _)
}
unsafe fn ExecPrepareQual(qual: *mut List, estate: *mut EState) -> *mut ExprState {
    crate::executor::execExpr::ExecPrepareQual(qual as _, estate as _) as _
}
unsafe fn ExecQual(state: *mut ExprState, econtext: *mut ExprContext) -> bool {
    crate::executor::executor::ExecQual(state as _, econtext as _) as _
}
unsafe fn GetPerTupleExprContext(estate: *mut EState) -> *mut ExprContext { crate::executor::execUtils::GetPerTupleExprContext(estate) }
unsafe fn ExecGetUpdatedCols(relinfo: *mut ResultRelInfo, estate: *mut EState) -> *mut Bitmapset { unimplemented!() /* TODO(pg-port): real ExecGetUpdatedCols lives in executor/execUtils.c */ }
unsafe fn ExecGetExtraUpdatedCols(relinfo: *mut ResultRelInfo, estate: *mut EState) -> *mut Bitmapset { unimplemented!() /* TODO(pg-port): real ExecGetExtraUpdatedCols lives in executor/execUtils.c */ }

unsafe fn table_slot_create(relation: Relation, reglist: *mut *mut List) -> *mut TupleTableSlot {
    crate::access::table::tableam::table_slot_create(relation as _, reglist as _) as _
}
unsafe fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot) {
    crate::executor::execTuples::ExecDropSingleTupleTableSlot(slot as _)
}

unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc as _, i as _) as _
}
unsafe fn lookup_type_cache(type_id: Oid, flags: c_int) -> *mut TypeCacheEntry { unimplemented!() /* TODO(pg-port): real lookup_type_cache lives in utils/cache/typcache.c */ }

unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool {
    crate::access::transam::TransactionIdIsValid(xid as _) as _
}
unsafe fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdPrecedes(id1 as _, id2 as _) as _
}
unsafe fn GetCurrentTransactionId() -> TransactionId {
    crate::access::transam::xact::GetCurrentTransactionId() as _
}
unsafe fn XactLockTableWait(xid: TransactionId, rel: Relation, ctid: ItemPointer, oper: XLTW_Oper) { unimplemented!() /* TODO(pg-port): real XactLockTableWait lives in storage/lmgr/lmgr.c */ }
unsafe fn SpeculativeInsertionWait(xid: TransactionId, token: u32) {
    crate::storage::lmgr::lmgr::SpeculativeInsertionWait(xid as _, token as _)
}

unsafe fn InitDirtySnapshot(snapshot: &mut SnapshotData) { unimplemented!() /* TODO(pg-port): real InitDirtySnapshot lives in utils/snapmgr.h */ }

unsafe fn OidFunctionCall2Coll(functionId: Oid, collation: Oid, arg1: Datum, arg2: Datum) -> Datum {
    crate::utils::fmgr::OidFunctionCall2Coll(functionId as _, collation as _, arg1 as _, arg2 as _) as _
}

unsafe fn DatumGetRangeTypeP(d: Datum) -> *mut RangeType {
    crate::utils::adt::rangetypes::DatumGetRangeTypeP(d as _) as _
}
unsafe fn RangeIsEmpty(r: *mut RangeType) -> bool {
    crate::utils::adt::rangetypes::RangeIsEmpty(r as _) as _
}
unsafe fn DatumGetMultirangeTypeP(d: Datum) -> *mut MultirangeType { unimplemented!() /* TODO(pg-port): real DatumGetMultirangeTypeP lives in utils/multirangetypes.h */ }
unsafe fn MultirangeIsEmpty(mr: *mut MultirangeType) -> bool { unimplemented!() /* TODO(pg-port): real MultirangeIsEmpty lives in utils/adt/multirangetypes.c */ }

unsafe fn NameStr(name: NameData) -> *const c_char { unimplemented!() /* TODO(pg-port): real NameStr lives in c.h */ }

/// waitMode argument to check_exclusion_or_unique_constraint()
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CEOUC_WAIT_MODE {
    CEOUC_WAIT,
    CEOUC_NOWAIT,
    CEOUC_LIVELOCK_PREVENTING_WAIT,
}
use CEOUC_WAIT_MODE::*;

/* ----------------------------------------------------------------
 *		ExecOpenIndices
 *
 *		Find the indices associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecOpenIndices(resultRelInfo: *mut ResultRelInfo, speculative: bool) {
    let resultRelation: Relation = (*resultRelInfo).ri_RelationDesc;
    let mut indexoidlist: *mut List;
    let mut l: *mut ListCell;
    let mut len: c_int;
    let mut i: c_int;
    let mut relationDescs: RelationPtr;
    let mut indexInfoArray: *mut *mut IndexInfo;

    (*resultRelInfo).ri_NumIndices = 0;

    /* fast path if no indexes */
    if !(*RelationGetForm(resultRelation)).relhasindex {
        return;
    }

    /*
     * Get cached list of index OIDs
     */
    indexoidlist = RelationGetIndexList(resultRelation);
    len = list_length(indexoidlist);
    if len == 0 {
        return;
    }

    /* This Assert will fail if ExecOpenIndices is called twice */
    Assert!((*resultRelInfo).ri_IndexRelationDescs.is_null());

    /*
     * allocate space for result arrays
     */
    relationDescs =
        palloc(len as usize * core::mem::size_of::<Relation>()) as RelationPtr;
    indexInfoArray =
        palloc(len as usize * core::mem::size_of::<*mut IndexInfo>()) as *mut *mut IndexInfo;

    (*resultRelInfo).ri_NumIndices = len;
    (*resultRelInfo).ri_IndexRelationDescs = relationDescs;
    (*resultRelInfo).ri_IndexRelationInfo = indexInfoArray;

    /*
     * For each index, open the index relation and save pg_index info. We
     * acquire RowExclusiveLock, signifying we will update the index.
     *
     * Note: we do this even if the index is not indisready; it's not worth
     * the trouble to optimize for the case where it isn't.
     */
    i = 0;
    foreach!(l, indexoidlist, {
        let indexOid: Oid = lfirst_oid(current_cell!(l));
        let indexDesc: Relation;
        let ii: *mut IndexInfo;

        indexDesc = index_open(indexOid, RowExclusiveLock);

        /* extract index key information from the index's pg_index info */
        ii = BuildIndexInfo(indexDesc);

        /*
         * If the indexes are to be used for speculative insertion, add extra
         * information required by unique index entries.
         */
        if speculative && (*ii).ii_Unique && !(*(*indexDesc).rd_index).indisexclusion {
            BuildSpeculativeIndexInfo(indexDesc, ii);
        }

        *relationDescs.offset(i as isize) = indexDesc;
        *indexInfoArray.offset(i as isize) = ii;
        i += 1;
    });

    list_free(indexoidlist);
}

/* ----------------------------------------------------------------
 *		ExecCloseIndices
 *
 *		Close the index relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecCloseIndices(resultRelInfo: *mut ResultRelInfo) {
    let mut i: c_int;
    let numIndices: c_int;
    let indexDescs: RelationPtr;
    let indexInfos: *mut *mut IndexInfo;

    numIndices = (*resultRelInfo).ri_NumIndices;
    indexDescs = (*resultRelInfo).ri_IndexRelationDescs;
    indexInfos = (*resultRelInfo).ri_IndexRelationInfo;

    i = 0;
    while i < numIndices {
        /* This Assert will fail if ExecCloseIndices is called twice */
        Assert!(!(*indexDescs.offset(i as isize)).is_null());

        /* Give the index a chance to do some post-insert cleanup */
        index_insert_cleanup(
            *indexDescs.offset(i as isize),
            *indexInfos.offset(i as isize),
        );

        /* Drop lock acquired by ExecOpenIndices */
        index_close(*indexDescs.offset(i as isize), RowExclusiveLock);

        /* Mark the index as closed */
        *indexDescs.offset(i as isize) = core::ptr::null_mut();

        i += 1;
    }

    /*
     * We don't attempt to free the IndexInfo data structures or the arrays,
     * instead assuming that such stuff will be cleaned up automatically in
     * FreeExecutorState.
     */
}

/* ----------------------------------------------------------------
 *		ExecInsertIndexTuples
 *
 *		This routine takes care of inserting index tuples
 *		into all the relations indexing the result relation
 *		when a heap tuple is inserted into the result relation.
 *
 *		(See the C source header for the full discussion of the 'update',
 *		'onlySummarizing', 'noDupErr', 'specConflict', and 'arbiterIndexes'
 *		semantics.)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInsertIndexTuples(
    resultRelInfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
    update: bool,
    noDupErr: bool,
    specConflict: *mut bool,
    arbiterIndexes: *mut List,
    onlySummarizing: bool,
) -> *mut List {
    let tupleid: ItemPointer = &mut (*slot).tts_tid;
    let mut result: *mut List = NIL;
    let mut i: c_int;
    let numIndices: c_int;
    let relationDescs: RelationPtr;
    let heapRelation: Relation;
    let indexInfoArray: *mut *mut IndexInfo;
    let econtext: *mut ExprContext;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];

    Assert!(ItemPointerIsValid(tupleid));

    /*
     * Get information from the result relation info structure.
     */
    numIndices = (*resultRelInfo).ri_NumIndices;
    relationDescs = (*resultRelInfo).ri_IndexRelationDescs;
    indexInfoArray = (*resultRelInfo).ri_IndexRelationInfo;
    heapRelation = (*resultRelInfo).ri_RelationDesc;

    /* Sanity check: slot must belong to the same rel as the resultRelInfo. */
    Assert!((*slot).tts_tableOid == RelationGetRelid(heapRelation));

    /*
     * We will use the EState's per-tuple context for evaluating predicates
     * and index expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /*
     * for each index, form and insert the index tuple
     */
    i = 0;
    while i < numIndices {
        let indexRelation: Relation = *relationDescs.offset(i as isize);
        let indexInfo: *mut IndexInfo;
        let applyNoDupErr: bool;
        let mut checkUnique: IndexUniqueCheck;
        let indexUnchanged: bool;
        let mut satisfiesConstraint: bool;

        if indexRelation.is_null() {
            i += 1;
            continue;
        }

        indexInfo = *indexInfoArray.offset(i as isize);

        /* If the index is marked as read-only, ignore it */
        if !(*indexInfo).ii_ReadyForInserts {
            i += 1;
            continue;
        }

        /*
         * Skip processing of non-summarizing indexes if we only update
         * summarizing indexes
         */
        if onlySummarizing && !(*indexInfo).ii_Summarizing {
            i += 1;
            continue;
        }

        /* Check for partial index */
        if (*indexInfo).ii_Predicate != NIL {
            let mut predicate: *mut ExprState;

            /*
             * If predicate state not set up yet, create it (in the estate's
             * per-query context)
             */
            predicate = (*indexInfo).ii_PredicateState;
            if predicate.is_null() {
                predicate = ExecPrepareQual((*indexInfo).ii_Predicate, estate);
                (*indexInfo).ii_PredicateState = predicate;
            }

            /* Skip this index-update if the predicate isn't satisfied */
            if !ExecQual(predicate, econtext) {
                i += 1;
                continue;
            }
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(
            indexInfo,
            slot,
            estate,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
        );

        /* Check whether to apply noDupErr to this index */
        applyNoDupErr = noDupErr
            && (arbiterIndexes == NIL
                || list_member_oid(
                    arbiterIndexes,
                    (*(*indexRelation).rd_index).indexrelid,
                ));

        /*
         * The index AM does the actual insertion, plus uniqueness checking.
         *
         * For an immediate-mode unique index, we just tell the index AM to
         * throw error if not unique.
         *
         * For a deferrable unique index, we tell the index AM to just detect
         * possible non-uniqueness, and we add the index OID to the result
         * list if further checking is needed.
         *
         * For a speculative insertion (used by INSERT ... ON CONFLICT), do
         * the same as for a deferrable unique index.
         */
        if !(*(*indexRelation).rd_index).indisunique {
            checkUnique = UNIQUE_CHECK_NO;
        } else if applyNoDupErr {
            checkUnique = UNIQUE_CHECK_PARTIAL;
        } else if (*(*indexRelation).rd_index).indimmediate {
            checkUnique = UNIQUE_CHECK_YES;
        } else {
            checkUnique = UNIQUE_CHECK_PARTIAL;
        }

        /*
         * There's definitely going to be an index_insert() call for this
         * index.  If we're being called as part of an UPDATE statement,
         * consider if the 'indexUnchanged' = true hint should be passed.
         */
        indexUnchanged = update
            && index_unchanged_by_update(resultRelInfo, estate, indexInfo, indexRelation);

        satisfiesConstraint = index_insert(
            indexRelation,             /* index relation */
            values.as_mut_ptr(),       /* array of index Datums */
            isnull.as_mut_ptr(),       /* null flags */
            tupleid,                   /* tid of heap tuple */
            heapRelation,              /* heap relation */
            checkUnique,               /* type of uniqueness check to do */
            indexUnchanged,            /* UPDATE without logical change? */
            indexInfo,                 /* index AM may need this */
        );

        /*
         * If the index has an associated exclusion constraint, check that.
         * This is simpler than the process for uniqueness checks since we
         * always insert first and then check.  If the constraint is deferred,
         * we check now anyway, but don't throw error on violation or wait for
         * a conclusive outcome from a concurrent insertion; instead we'll
         * queue a recheck event.  Similarly, noDupErr callers (speculative
         * inserters) will recheck later, and wait for a conclusive outcome
         * then.
         *
         * An index for an exclusion constraint can't also be UNIQUE (not an
         * essential property, we just don't allow it in the grammar), so no
         * need to preserve the prior state of satisfiesConstraint.
         */
        if !(*indexInfo).ii_ExclusionOps.is_null() {
            let violationOK: bool;
            let waitMode: CEOUC_WAIT_MODE;

            if applyNoDupErr {
                violationOK = true;
                waitMode = CEOUC_LIVELOCK_PREVENTING_WAIT;
            } else if !(*(*indexRelation).rd_index).indimmediate {
                violationOK = true;
                waitMode = CEOUC_NOWAIT;
            } else {
                violationOK = false;
                waitMode = CEOUC_WAIT;
            }

            satisfiesConstraint = check_exclusion_or_unique_constraint(
                heapRelation,
                indexRelation,
                indexInfo,
                tupleid,
                values.as_ptr(),
                isnull.as_ptr(),
                estate,
                false,
                waitMode,
                violationOK,
                core::ptr::null_mut(),
            );
        }

        if (checkUnique == UNIQUE_CHECK_PARTIAL || !(*indexInfo).ii_ExclusionOps.is_null())
            && !satisfiesConstraint
        {
            /*
             * The tuple potentially violates the uniqueness or exclusion
             * constraint, so make a note of the index so that we can re-check
             * it later.  Speculative inserters are told if there was a
             * speculative conflict, since that always requires a restart.
             */
            result = lappend_oid(result, RelationGetRelid(indexRelation));
            if (*(*indexRelation).rd_index).indimmediate && !specConflict.is_null() {
                *specConflict = true;
            }
        }

        i += 1;
    }

    result
}

/* ----------------------------------------------------------------
 *		ExecCheckIndexConstraints
 *
 *		This routine checks if a tuple violates any unique or
 *		exclusion constraints.  Returns true if there is no conflict.
 *		Otherwise returns false, and the TID of the conflicting
 *		tuple is returned in *conflictTid.
 *
 *		If 'arbiterIndexes' is given, only those indexes are checked.
 *		NIL means all indexes.
 *
 *		Note that this doesn't lock the values in any way, so it's
 *		possible that a conflicting tuple is inserted immediately
 *		after this returns.  This can be used for either a pre-check
 *		before insertion or a re-check after finding a conflict.
 *
 *		'tupleid' should be the TID of the tuple that has been recently
 *		inserted (or can be invalid if we haven't inserted a new tuple yet).
 *		This tuple will be excluded from conflict checking.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecCheckIndexConstraints(
    resultRelInfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
    conflictTid: ItemPointer,
    tupleid: ItemPointer,
    arbiterIndexes: *mut List,
) -> bool {
    let mut i: c_int;
    let numIndices: c_int;
    let relationDescs: RelationPtr;
    let heapRelation: Relation;
    let indexInfoArray: *mut *mut IndexInfo;
    let econtext: *mut ExprContext;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut invalidItemPtr: ItemPointerData = core::mem::zeroed();
    let mut checkedIndex: bool = false;

    ItemPointerSetInvalid(conflictTid);
    ItemPointerSetInvalid(&mut invalidItemPtr);

    /*
     * Get information from the result relation info structure.
     */
    numIndices = (*resultRelInfo).ri_NumIndices;
    relationDescs = (*resultRelInfo).ri_IndexRelationDescs;
    indexInfoArray = (*resultRelInfo).ri_IndexRelationInfo;
    heapRelation = (*resultRelInfo).ri_RelationDesc;

    /*
     * We will use the EState's per-tuple context for evaluating predicates
     * and index expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /*
     * For each index, form index tuple and check if it satisfies the
     * constraint.
     */
    i = 0;
    while i < numIndices {
        let indexRelation: Relation = *relationDescs.offset(i as isize);
        let indexInfo: *mut IndexInfo;
        let satisfiesConstraint: bool;

        if indexRelation.is_null() {
            i += 1;
            continue;
        }

        indexInfo = *indexInfoArray.offset(i as isize);

        if !(*indexInfo).ii_Unique && (*indexInfo).ii_ExclusionOps.is_null() {
            i += 1;
            continue;
        }

        /* If the index is marked as read-only, ignore it */
        if !(*indexInfo).ii_ReadyForInserts {
            i += 1;
            continue;
        }

        /* When specific arbiter indexes requested, only examine them */
        if arbiterIndexes != NIL
            && !list_member_oid(arbiterIndexes, (*(*indexRelation).rd_index).indexrelid)
        {
            i += 1;
            continue;
        }

        if !(*(*indexRelation).rd_index).indimmediate {
            ereport!(
                ERROR,
                errmsg!("ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters")
            );
        }

        checkedIndex = true;

        /* Check for partial index */
        if (*indexInfo).ii_Predicate != NIL {
            let mut predicate: *mut ExprState;

            /*
             * If predicate state not set up yet, create it (in the estate's
             * per-query context)
             */
            predicate = (*indexInfo).ii_PredicateState;
            if predicate.is_null() {
                predicate = ExecPrepareQual((*indexInfo).ii_Predicate, estate);
                (*indexInfo).ii_PredicateState = predicate;
            }

            /* Skip this index-update if the predicate isn't satisfied */
            if !ExecQual(predicate, econtext) {
                i += 1;
                continue;
            }
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(
            indexInfo,
            slot,
            estate,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
        );

        satisfiesConstraint = check_exclusion_or_unique_constraint(
            heapRelation,
            indexRelation,
            indexInfo,
            tupleid,
            values.as_ptr(),
            isnull.as_ptr(),
            estate,
            false,
            CEOUC_WAIT,
            true,
            conflictTid,
        );
        if !satisfiesConstraint {
            return false;
        }

        i += 1;
    }

    if arbiterIndexes != NIL && !checkedIndex {
        elog!(ERROR, "unexpected failure to find arbiter index");
    }

    true
}

/*
 * Check for violation of an exclusion or unique constraint
 *
 * (See the C source for the full description of all parameters.)
 *
 * Returns true if OK, false if actual or potential violation
 */
unsafe fn check_exclusion_or_unique_constraint(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
    tupleid: ItemPointer,
    values: *const Datum,
    isnull: *const bool,
    estate: *mut EState,
    newIndex: bool,
    waitMode: CEOUC_WAIT_MODE,
    violationOK: bool,
    conflictTid: ItemPointer,
) -> bool {
    let constr_procs: *mut Oid;
    let constr_strats: *mut u16;
    let index_collations: *mut Oid = (*index).rd_indcollation;
    let indnkeyatts: c_int = IndexRelationGetNumberOfKeyAttributes(index);
    let mut index_scan: IndexScanDesc;
    let mut scankeys: [ScanKeyData; INDEX_MAX_KEYS] = core::mem::zeroed();
    let mut DirtySnapshot: SnapshotData = core::mem::zeroed();
    let mut i: c_int;
    let mut conflict: bool;
    let mut found_self: bool;
    let econtext: *mut ExprContext;
    let existing_slot: *mut TupleTableSlot;
    let save_scantuple: *mut TupleTableSlot;

    if !(*indexInfo).ii_ExclusionOps.is_null() {
        constr_procs = (*indexInfo).ii_ExclusionProcs;
        constr_strats = (*indexInfo).ii_ExclusionStrats;
    } else {
        constr_procs = (*indexInfo).ii_UniqueProcs;
        constr_strats = (*indexInfo).ii_UniqueStrats;
    }

    /*
     * If this is a WITHOUT OVERLAPS constraint, we must also forbid empty
     * ranges/multiranges. This must happen before we look for NULLs below, or
     * a UNIQUE constraint could insert an empty range along with a NULL
     * scalar part.
     */
    if (*indexInfo).ii_WithoutOverlaps {
        /*
         * Look up the type from the heap tuple, but check the Datum from the
         * index tuple.
         */
        let attno: AttrNumber =
            (*indexInfo).ii_IndexAttrNumbers[(indnkeyatts - 1) as usize];

        if !*isnull.offset((indnkeyatts - 1) as isize) {
            let tupdesc: TupleDesc = RelationGetDescr(heap);
            let att: Form_pg_attribute = TupleDescAttr(tupdesc, (attno - 1) as c_int);
            let typcache: *mut TypeCacheEntry = lookup_type_cache((*att).atttypid, 0);

            ExecWithoutOverlapsNotEmpty(
                heap,
                (*att).attname,
                *values.offset((indnkeyatts - 1) as isize),
                (*typcache).typtype,
                (*att).atttypid,
            );
        }
    }

    /*
     * If any of the input values are NULL, and the index uses the default
     * nulls-are-distinct mode, the constraint check is assumed to pass (i.e.,
     * we assume the operators are strict).  Otherwise, we interpret the
     * constraint as specifying IS NULL for each column whose input value is
     * NULL.
     */
    if !(*indexInfo).ii_NullsNotDistinct {
        i = 0;
        while i < indnkeyatts {
            if *isnull.offset(i as isize) {
                return true;
            }
            i += 1;
        }
    }

    /*
     * Search the tuples that are in the index for any violations, including
     * tuples that aren't visible yet.
     */
    InitDirtySnapshot(&mut DirtySnapshot);

    i = 0;
    while i < indnkeyatts {
        ScanKeyEntryInitialize(
            &mut scankeys[i as usize],
            if *isnull.offset(i as isize) {
                SK_ISNULL | SK_SEARCHNULL
            } else {
                0
            },
            (i + 1) as AttrNumber,
            *constr_strats.offset(i as isize),
            InvalidOid,
            *index_collations.offset(i as isize),
            *constr_procs.offset(i as isize),
            *values.offset(i as isize),
        );
        i += 1;
    }

    /*
     * Need a TupleTableSlot to put existing tuples in.
     *
     * To use FormIndexDatum, we have to make the econtext's scantuple point
     * to this slot.  Be sure to save and restore caller's value for
     * scantuple.
     */
    existing_slot = table_slot_create(heap, core::ptr::null_mut());

    econtext = GetPerTupleExprContext(estate);
    save_scantuple = (*econtext).ecxt_scantuple;
    (*econtext).ecxt_scantuple = existing_slot;

    /*
     * May have to restart scan from this point if a potential conflict is
     * found.
     */
    'retry: loop {
        conflict = false;
        found_self = false;
        index_scan = index_beginscan(
            heap,
            index,
            &mut DirtySnapshot,
            core::ptr::null_mut(),
            indnkeyatts,
            0,
        );
        index_rescan(
            index_scan,
            scankeys.as_mut_ptr(),
            indnkeyatts,
            core::ptr::null_mut(),
            0,
        );

        while index_getnext_slot(index_scan, ForwardScanDirection, existing_slot) {
            let xwait: TransactionId;
            let reason_wait: XLTW_Oper;
            let mut existing_values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
            let mut existing_isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
            let error_new: *mut c_char;
            let error_existing: *mut c_char;

            /*
             * Ignore the entry for the tuple we're trying to check.
             */
            if ItemPointerIsValid(tupleid)
                && ItemPointerEquals(tupleid, &mut (*existing_slot).tts_tid)
            {
                if found_self {
                    /* should not happen */
                    elog!(
                        ERROR,
                        "found self tuple multiple times in index \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
                    );
                }
                found_self = true;
                continue;
            }

            /*
             * Extract the index column values and isnull flags from the existing
             * tuple.
             */
            FormIndexDatum(
                indexInfo,
                existing_slot,
                estate,
                existing_values.as_mut_ptr(),
                existing_isnull.as_mut_ptr(),
            );

            /* If lossy indexscan, must recheck the condition */
            if (*index_scan).xs_recheck {
                if !index_recheck_constraint(
                    index,
                    constr_procs,
                    existing_values.as_ptr(),
                    existing_isnull.as_ptr(),
                    values,
                ) {
                    continue; /* tuple doesn't actually match, so no
                               * conflict */
                }
            }

            /*
             * At this point we have either a conflict or a potential conflict.
             *
             * If an in-progress transaction is affecting the visibility of this
             * tuple, we need to wait for it to complete and then recheck (unless
             * the caller requested not to).  For simplicity we do rechecking by
             * just restarting the whole scan --- this case probably doesn't
             * happen often enough to be worth trying harder, and anyway we don't
             * want to hold any index internal locks while waiting.
             */
            xwait = if TransactionIdIsValid(DirtySnapshot.xmin) {
                DirtySnapshot.xmin
            } else {
                DirtySnapshot.xmax
            };

            if TransactionIdIsValid(xwait)
                && (waitMode == CEOUC_WAIT
                    || (waitMode == CEOUC_LIVELOCK_PREVENTING_WAIT
                        && DirtySnapshot.speculativeToken != 0
                        && TransactionIdPrecedes(GetCurrentTransactionId(), xwait)))
            {
                reason_wait = if !(*indexInfo).ii_ExclusionOps.is_null() {
                    XLTW_RecheckExclusionConstr
                } else {
                    XLTW_InsertIndex
                };
                index_endscan(index_scan);
                if DirtySnapshot.speculativeToken != 0 {
                    SpeculativeInsertionWait(
                        DirtySnapshot.xmin,
                        DirtySnapshot.speculativeToken,
                    );
                } else {
                    XactLockTableWait(
                        xwait,
                        heap,
                        &mut (*existing_slot).tts_tid,
                        reason_wait,
                    );
                }
                continue 'retry;
            }

            /*
             * We have a definite conflict (or a potential one, but the caller
             * didn't want to wait).  Return it to caller, or report it.
             */
            if violationOK {
                conflict = true;
                if !conflictTid.is_null() {
                    *conflictTid = (*existing_slot).tts_tid;
                }
                break;
            }

            error_new = BuildIndexValueDescription(index, values, isnull);
            error_existing = BuildIndexValueDescription(
                index,
                existing_values.as_ptr(),
                existing_isnull.as_ptr(),
            );
            if newIndex {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not create exclusion constraint \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
                    )
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "conflicting key value violates exclusion constraint \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
                    )
                );
            }
        }

        index_endscan(index_scan);
        break;
    }

    /*
     * Ordinarily, at this point the search should have found the originally
     * inserted tuple (if any), unless we exited the loop early because of
     * conflict.  However, it is possible to define exclusion constraints for
     * which that wouldn't be true --- for instance, if the operator is <>. So
     * we no longer complain if found_self is still false.
     */

    (*econtext).ecxt_scantuple = save_scantuple;

    ExecDropSingleTupleTableSlot(existing_slot);

    !conflict
}

/*
 * Check for violation of an exclusion constraint
 *
 * This is a dumbed down version of check_exclusion_or_unique_constraint
 * for external callers. They don't need all the special modes.
 */
pub unsafe fn check_exclusion_constraint(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
    tupleid: ItemPointer,
    values: *const Datum,
    isnull: *const bool,
    estate: *mut EState,
    newIndex: bool,
) {
    check_exclusion_or_unique_constraint(
        heap,
        index,
        indexInfo,
        tupleid,
        values,
        isnull,
        estate,
        newIndex,
        CEOUC_WAIT,
        false,
        core::ptr::null_mut(),
    );
}

/*
 * Check existing tuple's index values to see if it really matches the
 * exclusion condition against the new_values.  Returns true if conflict.
 */
unsafe fn index_recheck_constraint(
    index: Relation,
    constr_procs: *const Oid,
    existing_values: *const Datum,
    existing_isnull: *const bool,
    new_values: *const Datum,
) -> bool {
    let indnkeyatts: c_int = IndexRelationGetNumberOfKeyAttributes(index);
    let mut i: c_int;

    i = 0;
    while i < indnkeyatts {
        /* Assume the exclusion operators are strict */
        if *existing_isnull.offset(i as isize) {
            return false;
        }

        if !DatumGetBool(OidFunctionCall2Coll(
            *constr_procs.offset(i as isize),
            *(*index).rd_indcollation.offset(i as isize),
            *existing_values.offset(i as isize),
            *new_values.offset(i as isize),
        )) {
            return false;
        }

        i += 1;
    }

    true
}

/*
 * Check if ExecInsertIndexTuples() should pass indexUnchanged hint.
 *
 * When the executor performs an UPDATE that requires a new round of index
 * tuples, determine if we should pass 'indexUnchanged' = true hint for one
 * single index.
 */
unsafe fn index_unchanged_by_update(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    indexInfo: *mut IndexInfo,
    indexRelation: Relation,
) -> bool {
    let updatedCols: *mut Bitmapset;
    let extraUpdatedCols: *mut Bitmapset;
    let allUpdatedCols: *mut Bitmapset;
    let mut hasexpression: bool = false;
    let idxExprs: *mut List;

    /*
     * Check cache first
     */
    if (*indexInfo).ii_CheckedUnchanged {
        return (*indexInfo).ii_IndexUnchanged;
    }
    (*indexInfo).ii_CheckedUnchanged = true;

    /*
     * Check for indexed attribute overlap with updated columns.
     *
     * Only do this for key columns.  A change to a non-key column within an
     * INCLUDE index should not be counted here.  Non-key column values are
     * opaque payload state to the index AM, a little like an extra table TID.
     *
     * Note that row-level BEFORE triggers won't affect our behavior, since
     * they don't affect the updatedCols bitmaps generally.  It doesn't seem
     * worth the trouble of checking which attributes were changed directly.
     */
    updatedCols = ExecGetUpdatedCols(resultRelInfo, estate);
    extraUpdatedCols = ExecGetExtraUpdatedCols(resultRelInfo, estate);
    let mut attr: c_int = 0;
    while attr < (*indexInfo).ii_NumIndexKeyAttrs {
        let keycol: c_int = (*indexInfo).ii_IndexAttrNumbers[attr as usize] as c_int;

        if keycol <= 0 {
            /*
             * Skip expressions for now, but remember to deal with them later
             * on
             */
            hasexpression = true;
            attr += 1;
            continue;
        }

        if bms_is_member(keycol - FirstLowInvalidHeapAttributeNumber as c_int, updatedCols)
            || bms_is_member(
                keycol - FirstLowInvalidHeapAttributeNumber as c_int,
                extraUpdatedCols,
            )
        {
            /* Changed key column -- don't hint for this index */
            (*indexInfo).ii_IndexUnchanged = false;
            return false;
        }

        attr += 1;
    }

    /*
     * When we get this far and index has no expressions, return true so that
     * index_insert() call will go on to pass 'indexUnchanged' = true hint.
     *
     * The _absence_ of an indexed key attribute that overlaps with updated
     * attributes (in addition to the total absence of indexed expressions)
     * shows that the index as a whole is logically unchanged by UPDATE.
     */
    if !hasexpression {
        (*indexInfo).ii_IndexUnchanged = true;
        return true;
    }

    /*
     * Need to pass only one bms to expression_tree_walker helper function.
     * Avoid allocating memory in common case where there are no extra cols.
     */
    if extraUpdatedCols.is_null() {
        allUpdatedCols = updatedCols;
    } else {
        allUpdatedCols = bms_union(updatedCols, extraUpdatedCols);
    }

    /*
     * We have to work slightly harder in the event of indexed expressions,
     * but the principle is the same as before: try to find columns (Vars,
     * actually) that overlap with known-updated columns.
     *
     * If we find any matching Vars, don't pass hint for index.  Otherwise
     * pass hint.
     */
    idxExprs = RelationGetIndexExpressions(indexRelation);
    hasexpression = index_expression_changed_walker(idxExprs as *mut Node, allUpdatedCols);
    list_free(idxExprs);
    if !extraUpdatedCols.is_null() {
        bms_free(allUpdatedCols);
    }

    if hasexpression {
        (*indexInfo).ii_IndexUnchanged = false;
        return false;
    }

    /*
     * Deliberately don't consider index predicates.  We should even give the
     * hint when result rel's "updated tuple" has no corresponding index
     * tuple, which is possible with a partial index (provided the usual
     * conditions are met).
     */
    (*indexInfo).ii_IndexUnchanged = true;
    true
}

/*
 * Indexed expression helper for index_unchanged_by_update().
 *
 * Returns true when Var that appears within allUpdatedCols located.
 */
unsafe fn index_expression_changed_walker(
    node: *mut Node,
    allUpdatedCols: *mut Bitmapset,
) -> bool {
    if node.is_null() {
        return false;
    }

    if IsA!(node, T_Var) {
        let var: *mut Var = node as *mut Var;

        if bms_is_member(
            (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            allUpdatedCols,
        ) {
            /* Var was updated -- indicates that we should not hint */
            return true;
        }

        /* Still haven't found a reason to not pass the hint */
        return false;
    }

    expression_tree_walker(
        node,
        Some(index_expression_changed_walker_cb),
        allUpdatedCols as *mut c_void,
    )
}

/// Thin `tree_walker_callback`-typed shim around `index_expression_changed_walker`
/// so it can be handed to `expression_tree_walker()` (which expects the canonical
/// `unsafe fn(*mut Node, *mut c_void) -> bool` signature).
unsafe fn index_expression_changed_walker_cb(node: *mut Node, context: *mut c_void) -> bool {
    index_expression_changed_walker(node, context as *mut Bitmapset)
}

/*
 * ExecWithoutOverlapsNotEmpty - raise an error if the tuple has an empty
 * range or multirange in the given attribute.
 */
unsafe fn ExecWithoutOverlapsNotEmpty(
    rel: Relation,
    attname: NameData,
    attval: Datum,
    typtype: c_char,
    atttypid: Oid,
) {
    let isempty: bool;
    let r: *mut RangeType;
    let mr: *mut MultirangeType;

    match typtype {
        x if x == TYPTYPE_RANGE => {
            r = DatumGetRangeTypeP(attval);
            isempty = RangeIsEmpty(r);
        }
        x if x == TYPTYPE_MULTIRANGE => {
            mr = DatumGetMultirangeTypeP(attval);
            isempty = MultirangeIsEmpty(mr);
        }
        _ => {
            elog!(
                ERROR,
                "WITHOUT OVERLAPS column \"{}\" is not a range or multirange",
                std::ffi::CStr::from_ptr(NameStr(attname)).to_string_lossy()
            );
            unreachable!()
        }
    }

    /* Report a CHECK_VIOLATION */
    if isempty {
        ereport!(
            ERROR,
            errmsg!(
                "empty WITHOUT OVERLAPS value found in column \"{}\" in relation \"{}\"",
                std::ffi::CStr::from_ptr(NameStr(attname)).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
}
