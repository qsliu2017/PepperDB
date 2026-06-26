//! Translated from PostgreSQL src/include/nodes/extensible.h

use bitflags::bitflags;

use crate::access::parallel::ParallelContext;
use crate::commands::explain_state::ExplainState;
use crate::executor::tuptable::TupleTableSlot;
use crate::lib::stringinfo::StringInfo;
use crate::nodes::execnodes::{CustomScanState, EState};
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{CustomPath, PlannerInfo, RelOptInfo};
use crate::nodes::plannodes::{CustomScan, Plan};

/// maximum length of an extensible node identifier
pub const EXTNODENAME_MAX_LEN: usize = 64;

/// An extensible node is a new type of node defined by an extension. The
/// concrete type is identified at runtime by `extnodename`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensibleNode {
    pub extnodename: Option<String>,
}

/// Callbacks for an extensible node type. "All callbacks are mandatory."
/// (routine struct -> trait; required-heavy, no supertraits.)
pub trait ExtensibleNodeMethods {
    /// identifier of this ExtensibleNodeMethods (`extnodename`)
    const EXTNODENAME: &'static str;
    /// size of an extensible node of this type in bytes (`node_size`)
    const NODE_SIZE: usize;

    /// deep copy from `oldnode` to `newnode` (`nodeCopy`)
    fn node_copy(newnode: &mut ExtensibleNode, oldnode: &ExtensibleNode);
    /// deep equality comparison (`nodeEqual`)
    fn node_equal(a: &ExtensibleNode, b: &ExtensibleNode) -> bool;
    /// serialization (`nodeOut`)
    fn node_out(str: &mut StringInfo, node: &ExtensibleNode);
    /// deserialization (`nodeRead`)
    fn node_read(node: &mut ExtensibleNode);
}

/// Object-safe view of the registry entries; registration/lookup needs a
/// dynamic handle (the trait above is the per-type impl).
pub trait ExtensibleNodeMethodsDyn {
    fn extnodename(&self) -> &str;
    fn node_size(&self) -> usize;
    fn node_copy(&self, newnode: &mut ExtensibleNode, oldnode: &ExtensibleNode);
    fn node_equal(&self, a: &ExtensibleNode, b: &ExtensibleNode) -> bool;
    fn node_out(&self, str: &mut StringInfo, node: &ExtensibleNode);
    fn node_read(&self, node: &mut ExtensibleNode);
}

pub fn RegisterExtensibleNodeMethods(_methods: &dyn ExtensibleNodeMethodsDyn) {
    unimplemented!()
}

pub fn GetExtensibleNodeMethods(
    _extnodename: &str,
    _missing_ok: bool,
) -> Option<&'static dyn ExtensibleNodeMethodsDyn> {
    unimplemented!()
}

bitflags! {
    /// Flags for custom paths, indicating what capabilities the resulting scan
    /// will have. Stored in the `flags` fields of CustomPath and CustomScan.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CustomPathSupport: u32 {
        const BACKWARD_SCAN = 0x0001;
        const MARK_RESTORE  = 0x0002;
        const PROJECTION    = 0x0004;
    }
}

/// Custom path methods (routine struct -> trait; all required).
pub trait CustomPathMethods {
    const CUSTOM_NAME: &'static str;

    /// Convert Path to a Plan (`PlanCustomPath`).
    fn plan_custom_path(
        root: &mut PlannerInfo,
        rel: &RelOptInfo,
        best_path: &CustomPath,
        tlist: Vec<Box<Node>>,
        clauses: Vec<Box<Node>>,
        custom_plans: Vec<Box<Node>>,
    ) -> Box<Plan>;

    /// `ReparameterizeCustomPathByChild`.
    fn reparameterize_custom_path_by_child(
        root: &mut PlannerInfo,
        custom_private: Vec<Box<Node>>,
        child_rel: &RelOptInfo,
    ) -> Vec<Box<Node>>;
}

/// Custom scan methods (routine struct -> trait; all required).
pub trait CustomScanMethods {
    const CUSTOM_NAME: &'static str;

    /// Create execution state (CustomScanState) from a CustomScan plan node.
    fn create_custom_scan_state(cscan: &CustomScan) -> Box<Node>;
}

/// Object-safe handle for the CustomScanMethods registry.
pub trait CustomScanMethodsDyn {
    fn custom_name(&self) -> &str;
    fn create_custom_scan_state(&self, cscan: &CustomScan) -> Box<Node>;
}

/// Execution-time methods for a CustomScanState (routine struct -> trait).
/// Required callbacks are base methods; optional groups are supertraits.
pub trait CustomExecMethods {
    const CUSTOM_NAME: &'static str;

    fn begin_custom_scan(node: &mut CustomScanState, estate: &mut EState, eflags: i32);
    fn exec_custom_scan(node: &mut CustomScanState) -> Option<Box<TupleTableSlot>>;
    fn end_custom_scan(node: &mut CustomScanState);
    fn rescan_custom_scan(node: &mut CustomScanState);
}

/// Optional methods: needed if mark/restore is supported.
pub trait CustomScanMarkRestore: CustomExecMethods {
    fn mark_pos_custom_scan(node: &mut CustomScanState);
    fn restr_pos_custom_scan(node: &mut CustomScanState);
}

/// Optional methods: needed if parallel execution is supported (all-or-none).
pub trait CustomScanParallel: CustomExecMethods {
    fn estimate_dsm_custom_scan(node: &mut CustomScanState, pcxt: &mut ParallelContext) -> usize;
    fn initialize_dsm_custom_scan(
        node: &mut CustomScanState,
        pcxt: &mut ParallelContext,
        coordinate: &mut [u8],
    );
    fn reinitialize_dsm_custom_scan(
        node: &mut CustomScanState,
        pcxt: &mut ParallelContext,
        coordinate: &mut [u8],
    );
    fn initialize_worker_custom_scan(
        node: &mut CustomScanState,
        toc: &mut ShmToc,
        coordinate: &mut [u8],
    );
    fn shutdown_custom_scan(node: &mut CustomScanState);
}

/// Optional: print additional information in EXPLAIN.
pub trait CustomScanExplain: CustomExecMethods {
    fn explain_custom_scan(
        node: &mut CustomScanState,
        ancestors: Vec<Box<Node>>,
        es: &mut ExplainState,
    );
}

pub fn RegisterCustomScanMethods(_methods: &dyn CustomScanMethodsDyn) {
    unimplemented!()
}

pub fn GetCustomScanMethods(
    _custom_name: &str,
    _missing_ok: bool,
) -> Option<&'static dyn CustomScanMethodsDyn> {
    unimplemented!()
}

/// Opaque; shm_toc shared-memory table-of-contents not ported (single-process).
#[derive(Debug)]
pub struct ShmToc;
