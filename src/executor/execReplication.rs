//! src/backend/executor/execReplication.c
//!
//! miscellaneous executor routines for logical replication
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/execReplication.c

use crate::prelude::*;

// c_char/c_int come from the prelude (core::ffi re-export); importing them again
// from std::ffi would be a duplicate import (E0252).

// ---------------------------------------------------------------------------
// Stub type aliases / placeholders for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

use crate::foreach_oid;

type TupleTableSlot = crate::executor::tuptable::TupleTableSlot;
type Relation = crate::utils::rel::Relation;
type ScanKey = crate::access::common::scankey::ScanKey;
type ScanKeyData = crate::access::common::scankey::ScanKeyData;
type IndexScanDesc = crate::access::relscan::IndexScanDesc;
type TableScanDesc = crate::access::relscan::TableScanDesc;
type SnapshotData = crate::utils::snapshot::SnapshotData;
type TransactionId = crate::c::TransactionId;
type LockTupleMode = c_int;
type TM_Result = c_int;
type TM_FailureData = crate::access::table::tableam::TM_FailureData;
type ResultRelInfo = crate::nodes::execnodes::ResultRelInfo;
type EState = crate::nodes::execnodes::EState;
type EPQState = crate::nodes::execnodes::EPQState;
type IndexInfo = crate::nodes::execnodes::IndexInfo;
type List = crate::nodes::pg_list::List;
type ConflictType = c_int;
type CmdType = c_int;
type TU_UpdateIndexes = c_int;
type ItemPointerData = crate::storage::itemptr::ItemPointerData;
type ItemPointer = *mut ItemPointerData;
type oidvector = crate::c::oidvector;
type int2vector = crate::c::int2vector;
type StrategyNumber = u16;
type RegProcedure = Oid;
type Form_pg_attribute = *mut crate::catalog::pg_attribute::FormData_pg_attribute;
type TupleDesc = crate::access::common::tupdesc::TupleDesc;
type TimestampTz = crate::c::int64;
type FmgrInfo = crate::utils::fmgr::FmgrInfo;

// ---------------------------------------------------------------------------
// Local stubs for types whose canonical module is not yet ported.
// Only the fields touched by this translation unit are modeled.
// ---------------------------------------------------------------------------

/// Extended view of FormData_pg_index that includes the variable-length
/// indkey field (int2vector) which immediately follows the fixed fields
/// in the on-disk tuple.  Only used for casting rd_index pointers --
/// never instantiated.
#[repr(C)]
struct FormData_pg_index_with_indkey {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    pub indkey: int2vector,
}

#[repr(C)]
pub struct TypeCacheEntry {
    pub eq_opr_finfo: FmgrInfo,
}

#[repr(C)]
#[derive(Default)]
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

#[repr(C)]
#[derive(Default)]
pub struct PublicationDesc {
    pub pubactions: PublicationActions,
    pub rf_valid_for_update: bool,
    pub rf_valid_for_delete: bool,
    pub cols_valid_for_update: bool,
    pub cols_valid_for_delete: bool,
    pub gencols_valid_for_update: bool,
    pub gencols_valid_for_delete: bool,
}

#[repr(C)]
pub struct ConflictTupleInfo {
    pub slot: *mut TupleTableSlot,
    pub indexoid: Oid,
    pub xmin: TransactionId,
    pub origin: crate::c::uint32,
    pub ts: TimestampTz,
}

const INDEX_MAX_KEYS: usize = 32;

// Constants used below (stubbed values; real definitions live elsewhere).
const RowExclusiveLock: c_int = 3;
const NoLock: c_int = 0;
const LockTupleShare: LockTupleMode = 1;
const LockWaitBlock: c_int = 0;
const ForwardScanDirection: c_int = 1;
const XLTW_None: c_int = 0;

const SK_ISNULL: c_int = 0x0001;
const SK_SEARCHNULL: c_int = 0x0010;

const COMPARE_EQ: c_int = 3;

const TYPECACHE_EQ_OPR_FINFO: c_int = 0x0010;

const Anum_pg_index_indclass: c_int = 10;
const INDEXRELID: c_int = 0;

const TM_Ok: TM_Result = 0;
const TM_Updated: TM_Result = 4;
const TM_Deleted: TM_Result = 5;
const TM_Invisible: TM_Result = 1;

const CMD_INSERT: CmdType = 3;
const CMD_UPDATE: CmdType = 4;
const CMD_DELETE: CmdType = 5;

const CT_INSERT_EXISTS: ConflictType = 0;
const CT_UPDATE_EXISTS: ConflictType = 0;
const CT_MULTIPLE_UNIQUE_CONFLICTS: ConflictType = 0;

const TU_None: TU_UpdateIndexes = 0;
const TU_Summarizing: TU_UpdateIndexes = 2;

const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
const REPLICA_IDENTITY_FULL: c_char = b'f' as c_char;

// ---------------------------------------------------------------------------
// Local stubs for unported helper functions.
// ---------------------------------------------------------------------------

unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: *mut core::ffi::c_void, _attno: c_int) -> Datum { unimplemented!() /* TODO: utils/syscache.c */ }
unsafe fn IndexRelationGetNumberOfKeyAttributes(_rel: Relation) -> c_int {
    crate::access::nbtree::nbtdedup::IndexRelationGetNumberOfKeyAttributes(_rel as _) as _
}
unsafe fn get_opclass_input_type(_opclass: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_opclass_input_type(_opclass as _) as _
}
unsafe fn get_opclass_family(_opclass: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_opclass_family(_opclass as _) as _
}
unsafe fn IndexAmTranslateCompareType(_cmptype: c_int, _amoid: Oid, _opfamily: Oid, _missing_ok: bool) -> StrategyNumber { unimplemented!() /* TODO: access/amapi.c */ }
unsafe fn get_opfamily_member(_opfamily: Oid, _lefttype: Oid, _righttype: Oid, _strategy: StrategyNumber) -> Oid { unimplemented!() /* TODO: utils/lsyscache.c */ }
unsafe fn get_opcode(_opno: Oid) -> RegProcedure { unimplemented!() /* TODO: utils/lsyscache.c */ }
unsafe fn ScanKeyInit(_entry: *mut ScanKeyData, _attno: c_int, _strategy: StrategyNumber, _procedure: RegProcedure, _argument: Datum) { unimplemented!() /* TODO: access/common/scankey.c */ }
unsafe fn ItemPointerIndicatesMovedPartitions(_pointer: *const ItemPointerData) -> bool { unimplemented!() /* TODO: storage/itemptr.h */ }
unsafe fn index_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    crate::access::index::indexam::index_open(_relationId as _, _lockmode as _) as _
}
unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    crate::access::index::indexam::index_close(_relation as _, _lockmode as _)
}
unsafe fn GetRelationIdentityOrPK(_rel: Relation) -> Oid { unimplemented!() /* TODO: replication/logicalrelation.c */ }
unsafe fn index_beginscan(_heapRelation: Relation, _indexRelation: Relation, _snapshot: *mut SnapshotData, _instrument: *mut core::ffi::c_void, _nkeys: c_int, _norderbys: c_int) -> IndexScanDesc {
    crate::access::index::indexam::index_beginscan(_heapRelation as _, _indexRelation as _, _snapshot as _, _instrument as _, _nkeys as _, _norderbys as _) as _
}
unsafe fn index_rescan(_scan: IndexScanDesc, _keys: ScanKey, _nkeys: c_int, _orderbys: ScanKey, _norderbys: c_int) { unimplemented!() /* TODO: access/index/indexam.c */ }
unsafe fn index_getnext_slot(_scan: IndexScanDesc, _direction: c_int, _slot: *mut TupleTableSlot) -> bool {
    crate::access::index::indexam::index_getnext_slot(_scan as _, _direction as _, _slot as _) as _
}
unsafe fn index_endscan(_scan: IndexScanDesc) {
    crate::access::index::indexam::index_endscan(_scan as _)
}
unsafe fn ExecMaterializeSlot(_slot: *mut TupleTableSlot) {
    crate::executor::tuptable::ExecMaterializeSlot(_slot as _)
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    crate::access::transam::TransactionIdIsValid(_xid as _) as _
}
unsafe fn XactLockTableWait(_xid: TransactionId, _rel: Relation, _ctid: *mut ItemPointerData, _oper: c_int) {
    crate::storage::lmgr::lmgr::XactLockTableWait(_xid as _, _rel as _, _ctid as _, _oper as _)
}
unsafe fn PushActiveSnapshot(_snap: *mut SnapshotData) {
    crate::utils::time::snapmgr::PushActiveSnapshot(_snap as _)
}
unsafe fn PopActiveSnapshot() {
    crate::utils::time::snapmgr::PopActiveSnapshot()
}
unsafe fn GetLatestSnapshot() -> *mut SnapshotData {
    crate::utils::time::snapmgr::GetLatestSnapshot() as _
}
unsafe fn GetActiveSnapshot() -> *mut SnapshotData {
    crate::utils::time::snapmgr::GetActiveSnapshot() as _
}
unsafe fn GetCurrentCommandId(_used: bool) -> c_int {
    crate::access::transam::xact::GetCurrentCommandId(_used as _) as _
}
unsafe fn table_tuple_lock(_rel: Relation, _tid: *mut ItemPointerData, _snapshot: *mut SnapshotData, _slot: *mut TupleTableSlot, _cid: c_int, _mode: LockTupleMode, _wait_policy: c_int, _flags: c_int, _tmfd: *mut TM_FailureData) -> TM_Result { unimplemented!() /* TODO: access/tableam.h */ }
unsafe fn slot_getallattrs(_slot: *mut TupleTableSlot) {
    crate::executor::tuptable::slot_getallattrs(_slot as _)
}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(_tupdesc as _, _i as _) as _
}
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry { unimplemented!() /* TODO: utils/cache/typcache.c */ }
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char { unimplemented!() /* TODO: utils/adt/format_type.c */ }
unsafe fn FunctionCall2Coll(_flinfo: *mut core::ffi::c_void, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Datum {
    crate::utils::fmgr::FunctionCall2Coll(_flinfo as _, _collation as _, _arg1 as _, _arg2 as _) as _
}
unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc { unimplemented!() /* TODO: utils/rel.h */ }
unsafe fn equalTupleDescs(_a: TupleDesc, _b: TupleDesc) -> bool {
    crate::access::common::tupdesc::equalTupleDescs(_a as _, _b as _) as _
}
unsafe fn table_beginscan(_rel: Relation, _snapshot: *mut SnapshotData, _nkeys: c_int, _key: ScanKey) -> TableScanDesc { unimplemented!() /* TODO: access/tableam.h */ }
unsafe fn table_slot_create(_rel: Relation, _reglist: *mut *mut List) -> *mut TupleTableSlot {
    crate::access::table::tableam::table_slot_create(_rel as _, _reglist as _) as _
}
unsafe fn table_rescan(_scan: TableScanDesc, _key: ScanKey) { unimplemented!() /* TODO: access/tableam.h */ }
unsafe fn table_scan_getnextslot(_scan: TableScanDesc, _direction: c_int, _slot: *mut TupleTableSlot) -> bool {
    crate::access::table::tableam::table_scan_getnextslot(_scan as _, _direction as _, _slot as _) as _
}
unsafe fn table_endscan(_scan: TableScanDesc) {
    crate::access::table::tableam::table_endscan(_scan as _)
}
unsafe fn ExecCopySlot(_dstslot: *mut TupleTableSlot, _srcslot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::tuptable::ExecCopySlot(_dstslot as _, _srcslot as _) as _
}
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {
    crate::executor::execTuples::ExecDropSingleTupleTableSlot(_slot as _)
}
unsafe fn RelationGetRelid(_rel: Relation) -> Oid { unimplemented!() /* TODO: utils/rel.h */ }
unsafe fn BuildSpeculativeIndexInfo(_index: Relation, _ii: *mut IndexInfo) {
    crate::catalog::index::BuildSpeculativeIndexInfo(_index as _, _ii as _)
}
unsafe fn ExecCheckIndexConstraints(_resultRelInfo: *mut ResultRelInfo, _slot: *mut TupleTableSlot, _estate: *mut EState, _conflictTid: *mut ItemPointerData, _tupleid: *mut ItemPointerData, _arbiterIndexes: *mut List) -> bool {
    crate::executor::execIndexing::ExecCheckIndexConstraints(_resultRelInfo as _, _slot as _, _estate as _, _conflictTid as _, _tupleid as _, _arbiterIndexes as _) as _
}
unsafe fn list_make1_oid(_x: Oid) -> *mut List {
    crate::nodes::pg_list::list_make1_oid(_x as _) as _
}
unsafe fn list_member_oid(_list: *mut List, _datum: Oid) -> bool { unimplemented!() /* TODO: nodes/list.c */ }
unsafe fn GetTupleTransactionInfo(_slot: *mut TupleTableSlot, _xmin: *mut TransactionId, _origin: *mut crate::c::uint32, _ts: *mut TimestampTz) {
    unimplemented!()
}
unsafe fn lappend(_list: *mut List, _datum: *mut core::ffi::c_void) -> *mut List { unimplemented!() /* TODO: nodes/list.c */ }
unsafe fn list_length(_list: *mut List) -> c_int {
    crate::nodes::pg_list::list_length(_list as _) as _
}
unsafe fn ReportApplyConflict(_estate: *mut EState, _resultRelInfo: *mut ResultRelInfo, _elevel: c_int, _type: ConflictType, _searchslot: *mut TupleTableSlot, _remoteslot: *mut TupleTableSlot, _conflicttuples: *mut List) { unimplemented!() /* TODO: replication/conflict.c */ }
unsafe fn ExecBRInsertTriggers(_estate: *mut EState, _relinfo: *mut ResultRelInfo, _slot: *mut TupleTableSlot) -> bool {
    crate::commands::trigger::ExecBRInsertTriggers(_estate as _, _relinfo as _, _slot as _) as _
}
unsafe fn ExecComputeStoredGenerated(_resultRelInfo: *mut ResultRelInfo, _estate: *mut EState, _slot: *mut TupleTableSlot, _cmdtype: CmdType) { unimplemented!() /* TODO: executor/nodeModifyTable.c */ }
unsafe fn ExecConstraints(_resultRelInfo: *mut ResultRelInfo, _slot: *mut TupleTableSlot, _estate: *mut EState) {
    crate::executor::execMain::ExecConstraints(_resultRelInfo as _, _slot as _, _estate as _)
}
unsafe fn ExecPartitionCheck(_resultRelInfo: *mut ResultRelInfo, _slot: *mut TupleTableSlot, _estate: *mut EState, _emitError: bool) -> bool {
    crate::executor::execMain::ExecPartitionCheck(_resultRelInfo as _, _slot as _, _estate as _, _emitError as _) as _
}
unsafe fn simple_table_tuple_insert(_rel: Relation, _slot: *mut TupleTableSlot) {
    crate::access::table::tableam::simple_table_tuple_insert(_rel as _, _slot as _)
}
unsafe fn ExecInsertIndexTuples(_resultRelInfo: *mut ResultRelInfo, _slot: *mut TupleTableSlot, _estate: *mut EState, _update: bool, _noDupErr: bool, _specConflict: *mut bool, _arbiterIndexes: *mut List, _onlySummarizing: bool) -> *mut List {
    crate::executor::execIndexing::ExecInsertIndexTuples(_resultRelInfo as _, _slot as _, _estate as _, _update as _, _noDupErr as _, _specConflict as _, _arbiterIndexes as _, _onlySummarizing as _) as _
}
unsafe fn ExecARInsertTriggers(_estate: *mut EState, _relinfo: *mut ResultRelInfo, _slot: *mut TupleTableSlot, _recheckIndexes: *mut List, _transition_capture: *mut core::ffi::c_void) {
    crate::commands::trigger::ExecARInsertTriggers(_estate as _, _relinfo as _, _slot as _, _recheckIndexes as _, _transition_capture as _)
}
unsafe fn list_free(_list: *mut List) { unimplemented!() /* TODO: nodes/list.c */ }
unsafe fn IsCatalogRelation(_relation: Relation) -> bool {
    crate::catalog::catalog::IsCatalogRelation(_relation as _) as _
}
unsafe fn ExecBRUpdateTriggers(_estate: *mut EState, _epqstate: *mut EPQState, _relinfo: *mut ResultRelInfo, _tupleid: ItemPointer, _fdw_trigtuple: *mut core::ffi::c_void, _newslot: *mut TupleTableSlot, _lockedSlot: *mut TupleTableSlot, _tmfd: *mut TM_FailureData, _is_merge_update: bool) -> bool {
    crate::commands::trigger::ExecBRUpdateTriggers(_estate as _, _epqstate as _, _relinfo as _, _tupleid as _, _fdw_trigtuple as _, _newslot as _, _lockedSlot as _, _tmfd as _, _is_merge_update as _) as _
}
unsafe fn simple_table_tuple_update(_rel: Relation, _otid: ItemPointer, _slot: *mut TupleTableSlot, _snapshot: *mut SnapshotData, _update_indexes: *mut TU_UpdateIndexes) {
    crate::access::table::tableam::simple_table_tuple_update(_rel as _, _otid as _, _slot as _, _snapshot as _, _update_indexes as _)
}
unsafe fn ExecARUpdateTriggers(_estate: *mut EState, _relinfo: *mut ResultRelInfo, _src_partinfo: *mut core::ffi::c_void, _dst_partinfo: *mut core::ffi::c_void, _tupleid: ItemPointer, _fdw_trigtuple: *mut core::ffi::c_void, _newslot: *mut TupleTableSlot, _recheckIndexes: *mut List, _transition_capture: *mut core::ffi::c_void, _is_crosspart_update: bool) {
    crate::commands::trigger::ExecARUpdateTriggers(_estate as _, _relinfo as _, _src_partinfo as _, _dst_partinfo as _, _tupleid as _, _fdw_trigtuple as _, _newslot as _, _recheckIndexes as _, _transition_capture as _, _is_crosspart_update as _)
}
unsafe fn ExecBRDeleteTriggers(_estate: *mut EState, _epqstate: *mut EPQState, _relinfo: *mut ResultRelInfo, _tupleid: ItemPointer, _fdw_trigtuple: *mut core::ffi::c_void, _epqslot: *mut *mut TupleTableSlot, _tmresult: *mut TM_Result, _tmfd: *mut TM_FailureData, _is_merge_delete: bool) -> bool {
    crate::commands::trigger::ExecBRDeleteTriggers(_estate as _, _epqstate as _, _relinfo as _, _tupleid as _, _fdw_trigtuple as _, _epqslot as _, _tmresult as _, _tmfd as _, _is_merge_delete as _) as _
}
unsafe fn simple_table_tuple_delete(_rel: Relation, _tid: ItemPointer, _snapshot: *mut SnapshotData) {
    crate::access::table::tableam::simple_table_tuple_delete(_rel as _, _tid as _, _snapshot as _)
}
unsafe fn ExecARDeleteTriggers(_estate: *mut EState, _relinfo: *mut ResultRelInfo, _tupleid: ItemPointer, _fdw_trigtuple: *mut core::ffi::c_void, _ar_delete_trig_tcs: *mut core::ffi::c_void, _is_crosspart_update: bool) {
    crate::commands::trigger::ExecARDeleteTriggers(_estate as _, _relinfo as _, _tupleid as _, _fdw_trigtuple as _, _ar_delete_trig_tcs as _, _is_crosspart_update as _)
}
unsafe fn RelationBuildPublicationDesc(_relation: Relation, _pubdesc: *mut PublicationDesc) {
    crate::utils::cache::relcache::RelationBuildPublicationDesc(_relation as _, _pubdesc as _)
}
unsafe fn RelationGetReplicaIndex(_relation: Relation) -> Oid {
    crate::utils::cache::relcache::RelationGetReplicaIndex(_relation as _) as _
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char { unimplemented!() /* TODO: utils/rel.h */ }
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int {
    crate::catalog::pg_class::errdetail_relkind_not_supported(_relkind as _) as _
}
unsafe fn InitDirtySnapshot(_snapshot: &mut SnapshotData) { unimplemented!() /* TODO: utils/snapmgr.h */ }

unsafe fn AttributeNumberIsValid(attno: c_int) -> bool { attno != 0 }
unsafe fn OidIsValid(oid: Oid) -> bool { oid != crate::postgres_ext::InvalidOid }
unsafe fn ItemPointerIsValid(_p: *const ItemPointerData) -> bool {
    crate::storage::itemptr::ItemPointerIsValid(_p as _) as _
}

unsafe fn palloc0_object_ConflictTupleInfo() -> *mut ConflictTupleInfo {
    palloc0(core::mem::size_of::<ConflictTupleInfo>()) as *mut ConflictTupleInfo
}

const NIL: *mut List = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// build_replindex_scan_key
//
// Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
// is setup to match 'rel' (*NOT* idxrel!).
//
// Returns how many columns to use for the index scan.
//
// This is not generic routine, idxrel must be PK, RI, or an index that can be
// used for REPLICA IDENTITY FULL table. See FindUsableIndexForReplicaIdentityFull()
// for details.
//
// By definition, replication identity of a rel meets all limitations associated
// with that. Note that any other index could also meet these limitations.
// ---------------------------------------------------------------------------
unsafe fn build_replindex_scan_key(
    skey: ScanKey,
    rel: Relation,
    idxrel: Relation,
    searchslot: *mut TupleTableSlot,
) -> c_int {
    let mut index_attoff: c_int;
    let mut skey_attoff: c_int = 0;
    let indclassDatum: Datum;
    let opclass: *mut oidvector;
    let indkey: *mut int2vector = &mut (*((*idxrel).rd_index as *mut FormData_pg_index_with_indkey)).indkey;

    indclassDatum = SysCacheGetAttrNotNull(
        INDEXRELID,
        (*idxrel).rd_indextuple as *mut core::ffi::c_void,
        Anum_pg_index_indclass,
    );
    opclass = DatumGetPointer(indclassDatum) as *mut oidvector;

    /* Build scankey for every non-expression attribute in the index. */
    index_attoff = 0;
    while index_attoff < IndexRelationGetNumberOfKeyAttributes(idxrel) {
        let operator: Oid;
        let optype: Oid;
        let opfamily: Oid;
        let regop: RegProcedure;
        let table_attno: c_int = *(*indkey).values.as_ptr().offset(index_attoff as isize) as c_int;
        let eq_strategy: StrategyNumber;

        if !AttributeNumberIsValid(table_attno) {
            /*
             * XXX: Currently, we don't support expressions in the scan key,
             * see code below.
             */
            index_attoff += 1;
            continue;
        }

        /*
         * Load the operator info.  We need this to get the equality operator
         * function for the scan key.
         */
        optype = get_opclass_input_type(*(*opclass).values.as_ptr().offset(index_attoff as isize));
        opfamily = get_opclass_family(*(*opclass).values.as_ptr().offset(index_attoff as isize));
        eq_strategy = IndexAmTranslateCompareType(COMPARE_EQ, (*(*idxrel).rd_rel).relam, opfamily, false);
        operator = get_opfamily_member(opfamily, optype, optype, eq_strategy);

        if !OidIsValid(operator) {
            elog!(ERROR, "missing operator {}({},{}) in opfamily {}",
                  eq_strategy, optype, optype, opfamily);
        }

        regop = get_opcode(operator);

        /* Initialize the scankey. */
        ScanKeyInit(
            skey.offset(skey_attoff as isize),
            index_attoff + 1,
            eq_strategy,
            regop,
            *(*searchslot).tts_values.offset((table_attno - 1) as isize),
        );

        (*skey.offset(skey_attoff as isize)).sk_collation =
            *(*idxrel).rd_indcollation.offset(index_attoff as isize);

        /* Check for null value. */
        if *(*searchslot).tts_isnull.offset((table_attno - 1) as isize) {
            (*skey.offset(skey_attoff as isize)).sk_flags |= (SK_ISNULL | SK_SEARCHNULL) as c_int;
        }

        skey_attoff += 1;
        index_attoff += 1;
    }

    /* There must always be at least one attribute for the index scan. */
    Assert!(skey_attoff > 0);

    skey_attoff
}

// ---------------------------------------------------------------------------
// should_refetch_tuple
//
// Helper function to check if it is necessary to re-fetch and lock the tuple
// due to concurrent modifications. This function should be called after
// invoking table_tuple_lock.
// ---------------------------------------------------------------------------
unsafe fn should_refetch_tuple(res: TM_Result, tmfd: *mut TM_FailureData) -> bool {
    let mut refetch: bool = false;

    match res {
        TM_Ok => {}
        TM_Updated => {
            /* XXX: Improve handling here */
            if ItemPointerIndicatesMovedPartitions(&(*tmfd).ctid) {
                ereport!(LOG,
                    "tuple to be locked was already moved to another partition due to concurrent update, retrying");
            } else {
                ereport!(LOG, "concurrent update, retrying");
            }
            refetch = true;
        }
        TM_Deleted => {
            /* XXX: Improve handling here */
            ereport!(LOG, "concurrent delete, retrying");
            refetch = true;
        }
        TM_Invisible => {
            elog!(ERROR, "attempted to lock invisible tuple");
        }
        _ => {
            elog!(ERROR, "unexpected table_tuple_lock status: {}", res);
        }
    }

    refetch
}

// ---------------------------------------------------------------------------
// RelationFindReplTupleByIndex
//
// Search the relation 'rel' for tuple using the index.
//
// If a matching tuple is found, lock it with lockmode, fill the slot with its
// contents, and return true.  Return false otherwise.
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe fn RelationFindReplTupleByIndex(
    rel: Relation,
    idxoid: Oid,
    lockmode: LockTupleMode,
    searchslot: *mut TupleTableSlot,
    outslot: *mut TupleTableSlot,
) -> bool {
    let mut skey: [ScanKeyData; INDEX_MAX_KEYS] = core::mem::zeroed();
    let skey_attoff: c_int;
    let scan: IndexScanDesc;
    let mut snap: SnapshotData = core::mem::zeroed();
    let mut xwait: TransactionId;
    let idxrel: Relation;
    let mut found: bool;
    let mut eq: *mut *mut TypeCacheEntry = core::ptr::null_mut();
    let isIdxSafeToSkipDuplicates: bool;

    /* Open the index. */
    idxrel = index_open(idxoid, RowExclusiveLock);

    isIdxSafeToSkipDuplicates = GetRelationIdentityOrPK(rel) == idxoid;

    InitDirtySnapshot(&mut snap);

    /* Build scan key. */
    skey_attoff = build_replindex_scan_key(skey.as_mut_ptr(), rel, idxrel, searchslot);

    /* Start an index scan. */
    scan = index_beginscan(rel, idxrel, &mut snap, core::ptr::null_mut(), skey_attoff, 0);

    'retry: loop {
        found = false;

        index_rescan(scan, skey.as_mut_ptr(), skey_attoff, core::ptr::null_mut(), 0);

        /* Try to find the tuple */
        while index_getnext_slot(scan, ForwardScanDirection, outslot) {
            /*
             * Avoid expensive equality check if the index is primary key or
             * replica identity index.
             */
            if !isIdxSafeToSkipDuplicates {
                if eq.is_null() {
                    eq = palloc0(
                        core::mem::size_of::<*mut TypeCacheEntry>()
                            * (*(*outslot).tts_tupleDescriptor).natts as usize,
                    ) as *mut *mut TypeCacheEntry;
                }

                if !tuples_equal(outslot, searchslot, eq) {
                    continue;
                }
            }

            ExecMaterializeSlot(outslot);

            xwait = if TransactionIdIsValid(snap.xmin) {
                snap.xmin
            } else {
                snap.xmax
            };

            /*
             * If the tuple is locked, wait for locking transaction to finish and
             * retry.
             */
            if TransactionIdIsValid(xwait) {
                XactLockTableWait(xwait, core::ptr::null_mut(), core::ptr::null_mut(), XLTW_None);
                continue 'retry;
            }

            /* Found our tuple and it's not locked */
            found = true;
            break;
        }

        /* Found tuple, try to lock it in the lockmode. */
        if found {
            let mut tmfd: TM_FailureData = core::mem::zeroed();
            let res: TM_Result;

            PushActiveSnapshot(GetLatestSnapshot());

            res = table_tuple_lock(
                rel,
                &mut (*outslot).tts_tid,
                GetActiveSnapshot(),
                outslot,
                GetCurrentCommandId(false),
                lockmode,
                LockWaitBlock,
                0, /* don't follow updates */
                &mut tmfd,
            );

            PopActiveSnapshot();

            if should_refetch_tuple(res, &mut tmfd) {
                continue 'retry;
            }
        }

        break;
    }

    index_endscan(scan);

    /* Don't release lock until commit. */
    index_close(idxrel, NoLock);

    found
}

// ---------------------------------------------------------------------------
// tuples_equal
//
// Compare the tuples in the slots by checking if they have equal values.
// ---------------------------------------------------------------------------
unsafe fn tuples_equal(
    slot1: *mut TupleTableSlot,
    slot2: *mut TupleTableSlot,
    eq: *mut *mut TypeCacheEntry,
) -> bool {
    let mut attrnum: c_int;

    Assert!((*(*slot1).tts_tupleDescriptor).natts == (*(*slot2).tts_tupleDescriptor).natts);

    slot_getallattrs(slot1);
    slot_getallattrs(slot2);

    /* Check equality of the attributes. */
    attrnum = 0;
    while attrnum < (*(*slot1).tts_tupleDescriptor).natts {
        let att: Form_pg_attribute;
        let mut typentry: *mut TypeCacheEntry;

        att = TupleDescAttr((*slot1).tts_tupleDescriptor, attrnum);

        /*
         * Ignore dropped and generated columns as the publisher doesn't send
         * those
         */
        if (*att).attisdropped || (*att).attgenerated != 0 {
            attrnum += 1;
            continue;
        }

        /*
         * If one value is NULL and other is not, then they are certainly not
         * equal
         */
        if *(*slot1).tts_isnull.offset(attrnum as isize) != *(*slot2).tts_isnull.offset(attrnum as isize)
        {
            return false;
        }

        /*
         * If both are NULL, they can be considered equal.
         */
        if *(*slot1).tts_isnull.offset(attrnum as isize)
            || *(*slot2).tts_isnull.offset(attrnum as isize)
        {
            attrnum += 1;
            continue;
        }

        typentry = *eq.offset(attrnum as isize);
        if typentry.is_null() {
            typentry = lookup_type_cache((*att).atttypid, TYPECACHE_EQ_OPR_FINFO);
            if !OidIsValid((*typentry).eq_opr_finfo.fn_oid) {
                ereport!(ERROR,
                    "could not identify an equality operator for type");
            }
            *eq.offset(attrnum as isize) = typentry;
        }

        if !DatumGetBool(FunctionCall2Coll(
            &mut (*typentry).eq_opr_finfo as *mut _ as *mut core::ffi::c_void,
            (*att).attcollation,
            *(*slot1).tts_values.offset(attrnum as isize),
            *(*slot2).tts_values.offset(attrnum as isize),
        )) {
            return false;
        }

        attrnum += 1;
    }

    true
}

// ---------------------------------------------------------------------------
// RelationFindReplTupleSeq
//
// Search the relation 'rel' for tuple using the sequential scan.
//
// If a matching tuple is found, lock it with lockmode, fill the slot with its
// contents, and return true.  Return false otherwise.
//
// Note that this stops on the first matching tuple.
//
// This can obviously be quite slow on tables that have more than few rows.
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe fn RelationFindReplTupleSeq(
    rel: Relation,
    lockmode: LockTupleMode,
    searchslot: *mut TupleTableSlot,
    outslot: *mut TupleTableSlot,
) -> bool {
    let scanslot: *mut TupleTableSlot;
    let scan: TableScanDesc;
    let mut snap: SnapshotData = core::mem::zeroed();
    let eq: *mut *mut TypeCacheEntry;
    let mut xwait: TransactionId;
    let mut found: bool;
    let desc: TupleDesc = RelationGetDescr(rel);

    Assert!(equalTupleDescs(desc, (*outslot).tts_tupleDescriptor));

    eq = palloc0(
        core::mem::size_of::<*mut TypeCacheEntry>()
            * (*(*outslot).tts_tupleDescriptor).natts as usize,
    ) as *mut *mut TypeCacheEntry;

    /* Start a heap scan. */
    InitDirtySnapshot(&mut snap);
    scan = table_beginscan(rel, &mut snap, 0, core::ptr::null_mut());
    scanslot = table_slot_create(rel, core::ptr::null_mut());

    'retry: loop {
        found = false;

        table_rescan(scan, core::ptr::null_mut());

        /* Try to find the tuple */
        while table_scan_getnextslot(scan, ForwardScanDirection, scanslot) {
            if !tuples_equal(scanslot, searchslot, eq) {
                continue;
            }

            found = true;
            ExecCopySlot(outslot, scanslot);

            xwait = if TransactionIdIsValid(snap.xmin) {
                snap.xmin
            } else {
                snap.xmax
            };

            /*
             * If the tuple is locked, wait for locking transaction to finish and
             * retry.
             */
            if TransactionIdIsValid(xwait) {
                XactLockTableWait(xwait, core::ptr::null_mut(), core::ptr::null_mut(), XLTW_None);
                continue 'retry;
            }

            /* Found our tuple and it's not locked */
            break;
        }

        /* Found tuple, try to lock it in the lockmode. */
        if found {
            let mut tmfd: TM_FailureData = core::mem::zeroed();
            let res: TM_Result;

            PushActiveSnapshot(GetLatestSnapshot());

            res = table_tuple_lock(
                rel,
                &mut (*outslot).tts_tid,
                GetActiveSnapshot(),
                outslot,
                GetCurrentCommandId(false),
                lockmode,
                LockWaitBlock,
                0, /* don't follow updates */
                &mut tmfd,
            );

            PopActiveSnapshot();

            if should_refetch_tuple(res, &mut tmfd) {
                continue 'retry;
            }
        }

        break;
    }

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(scanslot);

    found
}

// ---------------------------------------------------------------------------
// BuildConflictIndexInfo
//
// Build additional index information necessary for conflict detection.
// ---------------------------------------------------------------------------
unsafe fn BuildConflictIndexInfo(resultRelInfo: *mut ResultRelInfo, conflictindex: Oid) {
    let mut i: c_int = 0;
    while i < (*resultRelInfo).ri_NumIndices {
        let indexRelation: Relation = *(*resultRelInfo).ri_IndexRelationDescs.offset(i as isize);
        let indexRelationInfo: *mut IndexInfo =
            *(*resultRelInfo).ri_IndexRelationInfo.offset(i as isize);

        if conflictindex != RelationGetRelid(indexRelation) {
            i += 1;
            continue;
        }

        /*
         * This Assert will fail if BuildSpeculativeIndexInfo() is called
         * twice for the given index.
         */
        Assert!((*indexRelationInfo).ii_UniqueOps.is_null());

        BuildSpeculativeIndexInfo(indexRelation, indexRelationInfo);

        i += 1;
    }
}

// ---------------------------------------------------------------------------
// FindConflictTuple
//
// Find the tuple that violates the passed unique index (conflictindex).
//
// If the conflicting tuple is found return true, otherwise false.
//
// We lock the tuple to avoid getting it deleted before the caller can fetch
// the required information. Note that if the tuple is deleted before a lock
// is acquired, we will retry to find the conflicting tuple again.
// ---------------------------------------------------------------------------
unsafe fn FindConflictTuple(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    conflictindex: Oid,
    slot: *mut TupleTableSlot,
    conflictslot: *mut *mut TupleTableSlot,
) -> bool {
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let mut conflictTid: ItemPointerData = core::mem::zeroed();
    let mut tmfd: TM_FailureData = core::mem::zeroed();
    let mut res: TM_Result;

    *conflictslot = core::ptr::null_mut();

    /*
     * Build additional information required to check constraints violations.
     * See check_exclusion_or_unique_constraint().
     */
    BuildConflictIndexInfo(resultRelInfo, conflictindex);

    'retry: loop {
        if ExecCheckIndexConstraints(
            resultRelInfo,
            slot,
            estate,
            &mut conflictTid,
            &mut (*slot).tts_tid,
            list_make1_oid(conflictindex),
        ) {
            if !(*conflictslot).is_null() {
                ExecDropSingleTupleTableSlot(*conflictslot);
            }

            *conflictslot = core::ptr::null_mut();
            return false;
        }

        *conflictslot = table_slot_create(rel, core::ptr::null_mut());

        PushActiveSnapshot(GetLatestSnapshot());

        res = table_tuple_lock(
            rel,
            &mut conflictTid,
            GetActiveSnapshot(),
            *conflictslot,
            GetCurrentCommandId(false),
            LockTupleShare,
            LockWaitBlock,
            0, /* don't follow updates */
            &mut tmfd,
        );

        PopActiveSnapshot();

        if should_refetch_tuple(res, &mut tmfd) {
            continue 'retry;
        }

        break;
    }

    true
}

// ---------------------------------------------------------------------------
// CheckAndReportConflict
//
// Check all the unique indexes in 'recheckIndexes' for conflict with the
// tuple in 'remoteslot' and report if found.
// ---------------------------------------------------------------------------
unsafe fn CheckAndReportConflict(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    type_: ConflictType,
    recheckIndexes: *mut List,
    searchslot: *mut TupleTableSlot,
    remoteslot: *mut TupleTableSlot,
) {
    let mut conflicttuples: *mut List = NIL;
    let mut conflictslot: *mut TupleTableSlot = core::ptr::null_mut();

    /* Check all the unique indexes for conflicts */
    foreach_oid!(uniqueidx, (*resultRelInfo).ri_onConflictArbiterIndexes, {
        if list_member_oid(recheckIndexes, uniqueidx)
            && FindConflictTuple(resultRelInfo, estate, uniqueidx, remoteslot, &mut conflictslot)
        {
            let conflicttuple: *mut ConflictTupleInfo = palloc0_object_ConflictTupleInfo();

            (*conflicttuple).slot = conflictslot;
            (*conflicttuple).indexoid = uniqueidx;

            GetTupleTransactionInfo(
                conflictslot,
                &mut (*conflicttuple).xmin,
                &mut (*conflicttuple).origin,
                &mut (*conflicttuple).ts,
            );

            conflicttuples = lappend(conflicttuples, conflicttuple as *mut core::ffi::c_void);
        }
    });

    /* Report the conflict, if found */
    if !conflicttuples.is_null() {
        ReportApplyConflict(
            estate,
            resultRelInfo,
            ERROR,
            if list_length(conflicttuples) > 1 {
                CT_MULTIPLE_UNIQUE_CONFLICTS
            } else {
                type_
            },
            searchslot,
            remoteslot,
            conflicttuples,
        );
    }
}

// ---------------------------------------------------------------------------
// ExecSimpleRelationInsert
//
// Insert tuple represented in the slot to the relation, update the indexes,
// and execute any constraints and per-row triggers.
//
// Caller is responsible for opening the indexes.
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe fn ExecSimpleRelationInsert(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    slot: *mut TupleTableSlot,
) {
    let mut skip_tuple: bool = false;
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;

    /* For now we support only tables. */
    Assert!((*(*rel).rd_rel).relkind == RELKIND_RELATION);

    CheckCmdReplicaIdentity(rel, CMD_INSERT);

    /* BEFORE ROW INSERT Triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_before_row
    {
        if !ExecBRInsertTriggers(estate, resultRelInfo, slot) {
            skip_tuple = true; /* "do nothing" */
        }
    }

    if !skip_tuple {
        let mut recheckIndexes: *mut List = NIL;
        let conflictindexes: *mut List;
        let mut conflict: bool = false;

        /* Compute stored generated columns */
        if !(*(*rel).rd_att).constr.is_null()
            && (*(*(*rel).rd_att).constr).has_generated_stored
        {
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);
        }

        /* Check the constraints of the tuple */
        if !(*(*rel).rd_att).constr.is_null() {
            ExecConstraints(resultRelInfo, slot, estate);
        }
        if (*(*rel).rd_rel).relispartition {
            ExecPartitionCheck(resultRelInfo, slot, estate, true);
        }

        /* OK, store the tuple and create index entries for it */
        simple_table_tuple_insert((*resultRelInfo).ri_RelationDesc, slot);

        conflictindexes = (*resultRelInfo).ri_onConflictArbiterIndexes;

        if (*resultRelInfo).ri_NumIndices > 0 {
            recheckIndexes = ExecInsertIndexTuples(
                resultRelInfo,
                slot,
                estate,
                false,
                !conflictindexes.is_null(),
                &mut conflict,
                conflictindexes,
                false,
            );
        }

        /*
         * Checks the conflict indexes to fetch the conflicting local row and
         * reports the conflict. We perform this check here, instead of
         * performing an additional index scan before the actual insertion and
         * reporting the conflict if any conflicting rows are found. This is
         * to avoid the overhead of executing the extra scan for each INSERT
         * operation, even when no conflict arises, which could introduce
         * significant overhead to replication, particularly in cases where
         * conflicts are rare.
         *
         * XXX OTOH, this could lead to clean-up effort for dead tuples added
         * in heap and index in case of conflicts. But as conflicts shouldn't
         * be a frequent thing so we preferred to save the performance
         * overhead of extra scan before each insertion.
         */
        if conflict {
            CheckAndReportConflict(
                resultRelInfo,
                estate,
                CT_INSERT_EXISTS,
                recheckIndexes,
                core::ptr::null_mut(),
                slot,
            );
        }

        /* AFTER ROW INSERT Triggers */
        ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes, core::ptr::null_mut());

        /*
         * XXX we should in theory pass a TransitionCaptureState object to the
         * above to capture transition tuples, but after statement triggers
         * don't actually get fired by replication yet anyway
         */

        list_free(recheckIndexes);
    }
}

// ---------------------------------------------------------------------------
// ExecSimpleRelationUpdate
//
// Find the searchslot tuple and update it with data in the slot,
// update the indexes, and execute any constraints and per-row triggers.
//
// Caller is responsible for opening the indexes.
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe fn ExecSimpleRelationUpdate(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    epqstate: *mut EPQState,
    searchslot: *mut TupleTableSlot,
    slot: *mut TupleTableSlot,
) {
    let mut skip_tuple: bool = false;
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let tid: ItemPointer = &mut (*searchslot).tts_tid;

    /*
     * We support only non-system tables, with
     * check_publication_add_relation() accountable.
     */
    Assert!((*(*rel).rd_rel).relkind == RELKIND_RELATION);
    Assert!(!IsCatalogRelation(rel));

    CheckCmdReplicaIdentity(rel, CMD_UPDATE);

    /* BEFORE ROW UPDATE Triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_update_before_row
    {
        if !ExecBRUpdateTriggers(
            estate,
            epqstate,
            resultRelInfo,
            tid,
            core::ptr::null_mut(),
            slot,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            false,
        ) {
            skip_tuple = true; /* "do nothing" */
        }
    }

    if !skip_tuple {
        let mut recheckIndexes: *mut List = NIL;
        let mut update_indexes: TU_UpdateIndexes = 0;
        let conflictindexes: *mut List;
        let mut conflict: bool = false;

        /* Compute stored generated columns */
        if !(*(*rel).rd_att).constr.is_null()
            && (*(*(*rel).rd_att).constr).has_generated_stored
        {
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_UPDATE);
        }

        /* Check the constraints of the tuple */
        if !(*(*rel).rd_att).constr.is_null() {
            ExecConstraints(resultRelInfo, slot, estate);
        }
        if (*(*rel).rd_rel).relispartition {
            ExecPartitionCheck(resultRelInfo, slot, estate, true);
        }

        simple_table_tuple_update(rel, tid, slot, (*estate).es_snapshot as *mut c_void as *mut SnapshotData, &mut update_indexes);

        conflictindexes = (*resultRelInfo).ri_onConflictArbiterIndexes;

        if (*resultRelInfo).ri_NumIndices > 0 && update_indexes != TU_None {
            recheckIndexes = ExecInsertIndexTuples(
                resultRelInfo,
                slot,
                estate,
                true,
                !conflictindexes.is_null(),
                &mut conflict,
                conflictindexes,
                update_indexes == TU_Summarizing,
            );
        }

        /*
         * Refer to the comments above the call to CheckAndReportConflict() in
         * ExecSimpleRelationInsert to understand why this check is done at
         * this point.
         */
        if conflict {
            CheckAndReportConflict(
                resultRelInfo,
                estate,
                CT_UPDATE_EXISTS,
                recheckIndexes,
                searchslot,
                slot,
            );
        }

        /* AFTER ROW UPDATE Triggers */
        ExecARUpdateTriggers(
            estate,
            resultRelInfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            tid,
            core::ptr::null_mut(),
            slot,
            recheckIndexes,
            core::ptr::null_mut(),
            false,
        );

        list_free(recheckIndexes);
    }
}

// ---------------------------------------------------------------------------
// ExecSimpleRelationDelete
//
// Find the searchslot tuple and delete it, and execute any constraints
// and per-row triggers.
//
// Caller is responsible for opening the indexes.
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe fn ExecSimpleRelationDelete(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    epqstate: *mut EPQState,
    searchslot: *mut TupleTableSlot,
) {
    let mut skip_tuple: bool = false;
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let tid: ItemPointer = &mut (*searchslot).tts_tid;

    CheckCmdReplicaIdentity(rel, CMD_DELETE);

    /* BEFORE ROW DELETE Triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_delete_before_row
    {
        skip_tuple = !ExecBRDeleteTriggers(
            estate,
            epqstate,
            resultRelInfo,
            tid,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            false,
        );
    }

    if !skip_tuple {
        /* OK, delete the tuple */
        simple_table_tuple_delete(rel, tid, (*estate).es_snapshot as *mut c_void as *mut SnapshotData);

        /* AFTER ROW DELETE Triggers */
        ExecARDeleteTriggers(
            estate,
            resultRelInfo,
            tid,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            false,
        );
    }
}

// ---------------------------------------------------------------------------
// CheckCmdReplicaIdentity
//
// Check if command can be executed with current replica identity.
// ---------------------------------------------------------------------------
pub unsafe fn CheckCmdReplicaIdentity(rel: Relation, cmd: CmdType) {
    let mut pubdesc: PublicationDesc = core::mem::zeroed();

    /*
     * Skip checking the replica identity for partitioned tables, because the
     * operations are actually performed on the leaf partitions.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        return;
    }

    /* We only need to do checks for UPDATE and DELETE. */
    if cmd != CMD_UPDATE && cmd != CMD_DELETE {
        return;
    }

    /*
     * It is only safe to execute UPDATE/DELETE if the relation does not
     * publish UPDATEs or DELETEs, or all the following conditions are
     * satisfied:
     *
     * 1. All columns, referenced in the row filters from publications which
     * the relation is in, are valid - i.e. when all referenced columns are
     * part of REPLICA IDENTITY.
     *
     * 2. All columns, referenced in the column lists are valid - i.e. when
     * all columns referenced in the REPLICA IDENTITY are covered by the
     * column list.
     *
     * 3. All generated columns in REPLICA IDENTITY of the relation, are valid
     * - i.e. when all these generated columns are published.
     *
     * XXX We could optimize it by first checking whether any of the
     * publications have a row filter or column list for this relation, or if
     * the relation contains a generated column. If none of these exist and
     * the relation has replica identity then we can avoid building the
     * descriptor but as this happens only one time it doesn't seem worth the
     * additional complexity.
     */
    RelationBuildPublicationDesc(rel, &mut pubdesc);
    if cmd == CMD_UPDATE && !pubdesc.rf_valid_for_update {
        elog!(ERROR, "cannot update table \"{}\"",
              "Column used in the publication WHERE expression is not part of the replica identity.");
    } else if cmd == CMD_UPDATE && !pubdesc.cols_valid_for_update {
        elog!(ERROR, "cannot update table \"{}\"",
              "Column list used by the publication does not cover the replica identity.");
    } else if cmd == CMD_UPDATE && !pubdesc.gencols_valid_for_update {
        elog!(ERROR, "cannot update table \"{}\"",
              "Replica identity must not contain unpublished generated columns.");
    } else if cmd == CMD_DELETE && !pubdesc.rf_valid_for_delete {
        elog!(ERROR, "cannot delete from table \"{}\"",
              "Column used in the publication WHERE expression is not part of the replica identity.");
    } else if cmd == CMD_DELETE && !pubdesc.cols_valid_for_delete {
        elog!(ERROR, "cannot delete from table \"{}\"",
              "Column list used by the publication does not cover the replica identity.");
    } else if cmd == CMD_DELETE && !pubdesc.gencols_valid_for_delete {
        elog!(ERROR, "cannot delete from table \"{}\"",
              "Replica identity must not contain unpublished generated columns.");
    }

    /* If relation has replica identity we are always good. */
    if OidIsValid(RelationGetReplicaIndex(rel)) {
        return;
    }

    /* REPLICA IDENTITY FULL is also good for UPDATE/DELETE. */
    if (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
        return;
    }

    /*
     * This is UPDATE/DELETE and there is no replica identity.
     *
     * Check if the table publishes UPDATES or DELETES.
     */
    if cmd == CMD_UPDATE && pubdesc.pubactions.pubupdate {
        elog!(ERROR,
            "cannot update table \"{}\" because it does not have a replica identity and publishes updates",
            "To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.");
    } else if cmd == CMD_DELETE && pubdesc.pubactions.pubdelete {
        elog!(ERROR,
            "cannot delete from table \"{}\" because it does not have a replica identity and publishes deletes",
            "To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.");
    }
}

// ---------------------------------------------------------------------------
// CheckSubscriptionRelkind
//
// Check if we support writing into specific relkind.
//
// The nspname and relname are only needed for error reporting.
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe fn CheckSubscriptionRelkind(
    relkind: c_char,
    _nspname: *const c_char,
    _relname: *const c_char,
) {
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        elog!(ERROR, "cannot use relation \"{}.{}\" as logical replication target",
              "nspname", "relname");
    }
}
