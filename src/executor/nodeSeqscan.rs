//! Translated from PostgreSQL src/include/executor/nodeSeqscan.h
//!
//! The init/exec/rescan/end entry points are defined in the backend body
//! (`backend/executor/nodeSeqscan.rs`) and re-exported here under their C names.
//! They operate on `SeqScanRun` (the SeqScanState paired with its open heap scan
//! descriptor and the scan relation/snapshot) rather than a bare `SeqScanState*`,
//! mirroring the plan-state enum ownership model (rules.md s3). The parallel-scan
//! callbacks are grow guards.

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::SeqScanState;

/// PG `ExecInitSeqScan`. Returns the owned `Box<SeqScanRun>` (the C
/// `SeqScanState*` plus its scan descriptor; owned by the plan-state enum here).
pub use crate::backend::executor::nodeSeqscan::exec_init_seq_scan as ExecInitSeqScan;
/// PG `ExecEndSeqScan`.
pub use crate::backend::executor::nodeSeqscan::exec_end_seq_scan as ExecEndSeqScan;
/// PG `ExecReScanSeqScan`.
pub use crate::backend::executor::nodeSeqscan::exec_rescan_seq_scan as ExecReScanSeqScan;
/// PG `ExecSeqScan`: the `ExecProcNodeMtd` that drives the scan.
pub use crate::backend::executor::nodeSeqscan::exec_seq_scan as ExecSeqScan;

// parallel scan support (grow guards)
pub fn ExecSeqScanEstimate(_node: &mut SeqScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSeqScanInitializeDSM(_node: &mut SeqScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSeqScanReInitializeDSM(_node: &mut SeqScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSeqScanInitializeWorker(_node: &mut SeqScanState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}
