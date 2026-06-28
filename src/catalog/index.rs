//! Translated from PostgreSQL src/include/catalog/index.h

use bitflags::bitflags;

use crate::catalog::objectaddress::ObjectAddress;
use crate::common::relpath::RelFileNumber;
use crate::nodes::execnodes::{EState, IndexInfo};
use crate::nodes::parsenodes::{IndexStmt, ReindexStmt};
use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::Oid;
use crate::access::attmap::AttrMap;
use crate::executor::tuptable::TupleTableSlot;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::Relation;
use crate::utils::snapshot::Snapshot;
use crate::utils::tuplesort::Tuplesortstate;

pub const DEFAULT_INDEX_TYPE: &str = "btree";

/// Action code for index_set_state_flags (sequential ordinal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStateFlagsAction {
    SetReady,
    SetValid,
    DropClearValid,
    DropSetDead,
}

/// Options for REINDEX.
pub struct ReindexParams {
    pub options: ReindexOpt, // bitmask of REINDEXOPT_*
    pub tablespace_oid: Oid, // InvalidOid to do nothing
}

bitflags! {
    /// Flag bits for ReindexParams->options.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReindexOpt: u32 {
        const VERBOSE = 0x01;          // print progress info
        const REPORT_PROGRESS = 0x02;  // report pgstat progress
        const MISSING_OK = 0x04;       // skip missing relations
        const CONCURRENTLY = 0x08;     // concurrent mode
    }
}

/// State info for validate_index bulkdelete callback.
pub struct ValidateIndexState {
    pub tuplesort: *mut Tuplesortstate, // TODO(ptr): for sorting the index TIDs
    pub htups: f64,
    pub itups: f64,
    pub tups_inserted: f64,
}

pub fn index_check_primary_key(
    heap_rel: &Relation,
    index_info: &IndexInfo,
    is_alter_table: bool,
    stmt: &IndexStmt,
) {
    let _ = (heap_rel, index_info, is_alter_table, stmt);
    unimplemented!()
}

bitflags! {
    /// `flags` arg of index_create.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IndexCreate: u16 {
        const IS_PRIMARY = 1 << 0;
        const ADD_CONSTRAINT = 1 << 1;
        const SKIP_BUILD = 1 << 2;
        const CONCURRENT = 1 << 3;
        const IF_NOT_EXISTS = 1 << 4;
        const PARTITIONED = 1 << 5;
        const INVALID = 1 << 6;
    }
}

bitflags! {
    /// `constr_flags` arg of index_create / index_constraint_create.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IndexConstrCreate: u16 {
        const MARK_AS_PRIMARY = 1 << 0;
        const DEFERRABLE = 1 << 1;
        const INIT_DEFERRED = 1 << 2;
        const UPDATE_INDEX = 1 << 3;
        const REMOVE_OLD_DEPS = 1 << 4;
        const WITHOUT_OVERLAPS = 1 << 5;
    }
}

// `index_create` + `index_build` are implemented in
// `crate::backend::catalog::index` (M2 form: a plain btree index on simple heap
// columns; the `&Arc<SharedState>` thread is the foundation convention, and the
// constraint / concurrency / partitioning / pg_index-row parameters are staged --
// rules.md s4/s5).
pub use crate::backend::catalog::index::{index_build, index_create, make_index_info};

pub fn index_concurrently_create_copy(
    heap_relation: &Relation,
    old_index_id: Oid,
    tablespace_oid: Oid,
    new_name: &str,
) -> Oid {
    let _ = (heap_relation, old_index_id, tablespace_oid, new_name);
    unimplemented!()
}

pub fn index_concurrently_build(heap_relation_id: Oid, index_relation_id: Oid) {
    let _ = (heap_relation_id, index_relation_id);
    unimplemented!()
}

pub fn index_concurrently_swap(new_index_id: Oid, old_index_id: Oid, old_name: &str) {
    let _ = (new_index_id, old_index_id, old_name);
    unimplemented!()
}

pub fn index_concurrently_set_dead(heap_id: Oid, index_id: Oid) {
    let _ = (heap_id, index_id);
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn index_constraint_create(
    heap_relation: &Relation,
    index_relation_id: Oid,
    parent_constraint_id: Oid,
    index_info: &IndexInfo,
    constraint_name: &str,
    constraint_type: u8,
    constr_flags: IndexConstrCreate,
    allow_system_table_mods: bool,
    is_internal: bool,
) -> ObjectAddress {
    let _ = (
        heap_relation,
        index_relation_id,
        parent_constraint_id,
        index_info,
        constraint_name,
        constraint_type,
        constr_flags,
        allow_system_table_mods,
        is_internal,
    );
    unimplemented!()
}

pub fn index_drop(index_id: Oid, concurrent: bool, concurrent_lock_mode: bool) {
    let _ = (index_id, concurrent, concurrent_lock_mode);
    unimplemented!()
}

pub fn BuildIndexInfo(index: &Relation) -> IndexInfo {
    let _ = index;
    unimplemented!()
}

pub fn BuildDummyIndexInfo(index: &Relation) -> IndexInfo {
    let _ = index;
    unimplemented!()
}

pub fn CompareIndexInfo(
    info1: &IndexInfo,
    info2: &IndexInfo,
    collations1: &[Oid],
    collations2: &[Oid],
    opfamilies1: &[Oid],
    opfamilies2: &[Oid],
    attmap: &AttrMap,
) -> bool {
    let _ = (
        info1,
        info2,
        collations1,
        collations2,
        opfamilies1,
        opfamilies2,
        attmap,
    );
    unimplemented!()
}

pub fn BuildSpeculativeIndexInfo(index: &Relation, ii: &mut IndexInfo) {
    let _ = (index, ii);
    unimplemented!()
}

/// FormIndexDatum: `values`/`isnull` out-arrays folded into the return.
pub fn FormIndexDatum(
    index_info: &IndexInfo,
    slot: &TupleTableSlot,
    estate: &EState,
) -> Vec<Option<Datum>> {
    let _ = (index_info, slot, estate);
    unimplemented!()
}


pub fn validate_index(heap_id: Oid, index_id: Oid, snapshot: Snapshot) {
    let _ = (heap_id, index_id, snapshot);
    unimplemented!()
}

pub fn index_set_state_flags(index_id: Oid, action: IndexStateFlagsAction) {
    let _ = (index_id, action);
    unimplemented!()
}

/// IndexGetRelation: InvalidOid sentinel with missing_ok -> Option.
pub fn IndexGetRelation(index_id: Oid, missing_ok: bool) -> Option<Oid> {
    let _ = (index_id, missing_ok);
    unimplemented!()
}

pub fn reindex_index(
    stmt: &ReindexStmt,
    index_id: Oid,
    skip_constraint_checks: bool,
    persistence: u8,
    params: &ReindexParams,
) {
    let _ = (stmt, index_id, skip_constraint_checks, persistence, params);
    unimplemented!()
}

bitflags! {
    /// Flag bits for reindex_relation().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReindexRel: i32 {
        const PROCESS_TOAST = 0x01;
        const SUPPRESS_INDEX_USE = 0x02;
        const CHECK_CONSTRAINTS = 0x04;
        const FORCE_INDEXES_UNLOGGED = 0x08;
        const FORCE_INDEXES_PERMANENT = 0x10;
    }
}

pub fn reindex_relation(
    stmt: &ReindexStmt,
    relid: Oid,
    flags: ReindexRel,
    params: &ReindexParams,
) -> bool {
    let _ = (stmt, relid, flags, params);
    unimplemented!()
}

pub fn ReindexIsProcessingHeap(heap_oid: Oid) -> bool {
    let _ = heap_oid;
    unimplemented!()
}

pub fn ReindexIsProcessingIndex(index_oid: Oid) -> bool {
    let _ = index_oid;
    unimplemented!()
}

pub fn ResetReindexState(nest_level: i32) {
    let _ = nest_level;
    unimplemented!()
}

pub fn EstimateReindexStateSpace() -> usize {
    unimplemented!()
}

pub fn SerializeReindexState(maxsize: usize, start_address: &mut [u8]) {
    let _ = (maxsize, start_address);
    unimplemented!()
}

pub fn RestoreReindexState(reindexstate: &[u8]) {
    let _ = reindexstate;
    unimplemented!()
}

pub fn IndexSetParentIndex(partition_idx: &Relation, parent_oid: Oid) {
    let _ = (partition_idx, parent_oid);
    unimplemented!()
}

/// Encode ItemPointer as int64 that sorts in TID order.
pub fn itemptr_encode(itemptr: &ItemPointerData) -> i64 {
    let block = itemptr.block_number();
    let offset = itemptr.offset_number();
    ((u64::from(block) << 16) | u64::from(offset)) as i64
}

/// Decode the int64 representation back to ItemPointer.
pub fn itemptr_decode(itemptr: &mut ItemPointerData, encoded: i64) {
    let block = (encoded >> 16) as u32;
    let offset = (encoded & 0xFFFF) as u16;
    itemptr.set(block, offset);
}
