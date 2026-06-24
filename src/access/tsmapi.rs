//! Translated from PostgreSQL src/include/access/tsmapi.h

use crate::nodes::execnodes::SampleScanState;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;

/// TsmRoutine: the tablesample-method vtable, returned by a TSM handler.
///
/// Per routine-struct.md, the struct of fn pointers becomes a trait. The two
/// always-present scalar fields (`parameterTypes`, the `repeatable_*` flags)
/// become methods returning data; the always-present callbacks are required
/// methods; the `/* can be NULL */` callbacks (`InitSampleScan`,
/// `NextSampleBlock`, `EndSampleScan`) are provided default methods.
pub trait TsmRoutine {
    /// List of datatype OIDs for the arguments of the TABLESAMPLE clause.
    fn parameter_types(&self) -> Vec<Oid>;

    /// Can method produce repeatable samples across queries?
    fn repeatable_across_queries(&self) -> bool;
    /// Can method produce repeatable samples within a query?
    fn repeatable_across_scans(&self) -> bool;

    /// Plan a SampleScan: returns (estimated pages, estimated tuples).
    fn sample_scan_get_sample_size(
        &self,
        root: &PlannerInfo,
        baserel: &RelOptInfo,
        paramexprs: &[crate::nodes::nodes::Node],
    ) -> (BlockNumber, f64);

    /// BeginSampleScan: initialize per-scan sampling state.
    fn begin_sample_scan(&self, node: &mut SampleScanState, params: &[Datum], seed: u32);

    /// NextSampleTuple: next tuple offset on a block (InvalidOffsetNumber = done).
    fn next_sample_tuple(
        &self,
        node: &mut SampleScanState,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
    ) -> OffsetNumber;

    /// InitSampleScan (can be NULL): optional executor-startup hook.
    fn init_sample_scan(&self, node: &mut SampleScanState, eflags: i32) {
        let _ = (node, eflags);
    }

    /// NextSampleBlock (can be NULL): None drives a sequential scan instead.
    fn next_sample_block(
        &self,
        node: &mut SampleScanState,
        nblocks: BlockNumber,
    ) -> Option<BlockNumber> {
        let _ = (node, nblocks);
        None
    }

    /// EndSampleScan (can be NULL): optional teardown.
    fn end_sample_scan(&self, node: &mut SampleScanState) {
        let _ = node;
    }
}

/// Functions in access/tablesample/tablesample.c.
pub fn GetTsmRoutine(tsmhandler: Oid) -> Box<dyn TsmRoutine> {
    let _ = tsmhandler;
    unimplemented!()
}
