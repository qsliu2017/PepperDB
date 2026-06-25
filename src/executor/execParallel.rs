//! Translated from PostgreSQL src/include/executor/execParallel.h

use crate::access::parallel::{dsm_segment, shm_mq_handle, shm_toc, ParallelContext};
use crate::executor::instrument::{BufferUsage, WalUsage};
use crate::executor::tqueue::TupleQueueReader;
use crate::jit::jit::SharedJitInstrumentation;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::{EState, PlanState};

/// Opaque; SharedExecutorInstrumentation is private to execParallel.c.
pub struct SharedExecutorInstrumentation;

/// State for running a plan subtree in parallel. In-memory; DSM/DSA fields
/// collapse to owned heap state under the single-process model.
pub struct ParallelExecutorInfo {
    pub planstate: Option<Box<PlanState>>,
    pub pcxt: Option<Box<ParallelContext>>,
    pub buffer_usage: Option<Box<BufferUsage>>,
    pub wal_usage: Option<Box<WalUsage>>,
    pub instrumentation: Option<Box<SharedExecutorInstrumentation>>,
    pub jit_instrumentation: Option<Box<SharedJitInstrumentation>>,
    // dsa_area *area / dsa_pointer param_exec -> heap state (DSA tombstoned).
    pub param_exec: Vec<u8>, // serialized PARAM_EXEC parameters
    pub finished: bool,
    /* These arrays have pcxt->nworkers_launched entries: */
    pub tqueue: Vec<Box<shm_mq_handle>>,
    pub reader: Vec<Box<TupleQueueReader>>,
}

pub fn ExecInitParallelPlan(
    _planstate: &mut PlanState,
    _estate: &mut EState,
    _sendParams: &Bitmapset,
    _nworkers: i32,
    _tuples_needed: i64,
) -> Box<ParallelExecutorInfo> {
    unimplemented!()
}

pub fn ExecParallelCreateReaders(_pei: &mut ParallelExecutorInfo) {
    unimplemented!()
}

pub fn ExecParallelFinish(_pei: &mut ParallelExecutorInfo) {
    unimplemented!()
}

pub fn ExecParallelCleanup(_pei: &mut ParallelExecutorInfo) {
    unimplemented!()
}

pub fn ExecParallelReinitialize(
    _planstate: &mut PlanState,
    _pei: &mut ParallelExecutorInfo,
    _sendParams: &Bitmapset,
) {
    unimplemented!()
}

pub fn ParallelQueryMain(_seg: &mut dsm_segment, _toc: &mut shm_toc) {
    unimplemented!()
}
