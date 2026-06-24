//! Translated from PostgreSQL src/include/executor/nodeHash.h

use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{
    EState, ExprContext, HashInstrumentation, HashJoinState, HashState,
};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Hash;

// HashJoinTable / SharedHashJoinBatch are defined in executor/hashjoin.h
// (not in this batch).
// TODO(struct-forward): repoint to crate::executor::hashjoin::HashJoinTable in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::executor::hashjoin in Phase 2")]
pub type HashJoinTable = usize;

// TODO(ptr)
pub fn ExecInitHash(_node: &Hash, _estate: &mut EState, _eflags: i32) -> *mut HashState {
    unimplemented!()
}

// MultiExec returns a Node* (the built hash table, tagged via NodeTag).
// TODO(ptr)
pub fn MultiExecHash(_node: &mut HashState) -> *mut Node {
    unimplemented!()
}

pub fn ExecEndHash(_node: &mut HashState) {
    unimplemented!()
}

pub fn ExecReScanHash(_node: &mut HashState) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableCreate(_state: &mut HashState) -> HashJoinTable {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecParallelHashTableAlloc(_hashtable: HashJoinTable, _batchno: i32) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableDestroy(_hashtable: HashJoinTable) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableDetach(_hashtable: HashJoinTable) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableDetachBatch(_hashtable: HashJoinTable) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecParallelHashTableSetCurrentBatch(_hashtable: HashJoinTable, _batchno: i32) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableInsert(
    _hashtable: HashJoinTable,
    _slot: &mut TupleTableSlot,
    _hashvalue: u32,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecParallelHashTableInsert(
    _hashtable: HashJoinTable,
    _slot: &mut TupleTableSlot,
    _hashvalue: u32,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecParallelHashTableInsertCurrentBatch(
    _hashtable: HashJoinTable,
    _slot: &mut TupleTableSlot,
    _hashvalue: u32,
) {
    unimplemented!()
}

// Two out-params (bucketno, batchno) -> tuple.
#[allow(deprecated)]
pub fn ExecHashGetBucketAndBatch(_hashtable: HashJoinTable, _hashvalue: u32) -> (i32, i32) {
    unimplemented!()
}

pub fn ExecScanHashBucket(_hjstate: &mut HashJoinState, _econtext: &mut ExprContext) -> bool {
    unimplemented!()
}

pub fn ExecParallelScanHashBucket(
    _hjstate: &mut HashJoinState,
    _econtext: &mut ExprContext,
) -> bool {
    unimplemented!()
}

pub fn ExecPrepHashTableForUnmatched(_hjstate: &mut HashJoinState) {
    unimplemented!()
}

pub fn ExecParallelPrepHashTableForUnmatched(_hjstate: &mut HashJoinState) -> bool {
    unimplemented!()
}

pub fn ExecScanHashTableForUnmatched(
    _hjstate: &mut HashJoinState,
    _econtext: &mut ExprContext,
) -> bool {
    unimplemented!()
}

pub fn ExecParallelScanHashTableForUnmatched(
    _hjstate: &mut HashJoinState,
    _econtext: &mut ExprContext,
) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableReset(_hashtable: HashJoinTable) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashTableResetMatchFlags(_hashtable: HashJoinTable) {
    unimplemented!()
}

// Sizing result; four out-params (space_allowed, numbuckets, numbatches,
// num_skew_mcvs) -> named struct.
pub struct HashTableSize {
    pub space_allowed: usize,
    pub numbuckets: i32,
    pub numbatches: i32,
    pub num_skew_mcvs: i32,
}

pub fn ExecChooseHashTableSize(
    _ntuples: f64,
    _tupwidth: i32,
    _useskew: bool,
    _try_combined_hash_mem: bool,
    _parallel_workers: i32,
) -> HashTableSize {
    unimplemented!()
}

// Returns INVALID_SKEW_BUCKET_NO (-1) when not found -> Option.
#[allow(deprecated)]
pub fn ExecHashGetSkewBucket(_hashtable: HashJoinTable, _hashvalue: u32) -> Option<i32> {
    unimplemented!()
}

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};

pub fn ExecHashEstimate(_node: &mut HashState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecHashInitializeDSM(_node: &mut HashState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecHashInitializeWorker(_node: &mut HashState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}

pub fn ExecHashRetrieveInstrumentation(_node: &mut HashState) {
    unimplemented!()
}

pub fn ExecShutdownHash(_node: &mut HashState) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecHashAccumInstrumentation(
    _instrument: &mut HashInstrumentation,
    _hashtable: HashJoinTable,
) {
    unimplemented!()
}
