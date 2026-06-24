//! Translated from PostgreSQL src/include/access/spgist.h

use crate::access::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInfo, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo,
};
use crate::access::amapi::OpFamilyMember;
use crate::access::sdir::ScanDirection;
use crate::access::skey::ScanKey;
use crate::c::bytea;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::memutils::MemoryContext;
use crate::utils::rel::Relation;

/* SPGiST opclass support function numbers */
pub const SPGIST_CONFIG_PROC: u16 = 1;
pub const SPGIST_CHOOSE_PROC: u16 = 2;
pub const SPGIST_PICKSPLIT_PROC: u16 = 3;
pub const SPGIST_INNER_CONSISTENT_PROC: u16 = 4;
pub const SPGIST_LEAF_CONSISTENT_PROC: u16 = 5;
pub const SPGIST_COMPRESS_PROC: u16 = 6;
pub const SPGIST_OPTIONS_PROC: u16 = 7;
pub const SPGISTNRequiredProc: u16 = 5;
pub const SPGISTNProc: u16 = 7;

/// Argument struct for spg_config method (in).
pub struct spgConfigIn {
    /// Data type to be indexed
    pub att_type: Oid,
}

/// Argument struct for spg_config method (out).
pub struct spgConfigOut {
    /// Data type of inner-tuple prefixes
    pub prefix_type: Oid,
    /// Data type of inner-tuple node labels
    pub label_type: Oid,
    /// Data type of leaf-tuple values
    pub leaf_type: Oid,
    /// Opclass can reconstruct original data
    pub can_return_data: bool,
    /// Opclass can cope with values > 1 page
    pub long_values_ok: bool,
}

/// Argument struct for spg_choose method (in).
pub struct spgChooseIn {
    /// original datum to be indexed
    pub datum: Datum,
    /// current datum to be stored at leaf
    pub leaf_datum: Datum,
    /// current level (counting from zero)
    pub level: i32,

    /// tuple is marked all-the-same?
    pub all_the_same: bool,
    /// tuple has a prefix?
    pub has_prefix: bool,
    /// if so, the prefix value
    pub prefix_datum: Datum,
    /// number of nodes in the inner tuple
    pub n_nodes: i32,
    /// node label values (empty if none)
    pub node_labels: Vec<Datum>,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum spgChooseResultType {
    /// descend into existing node
    spgMatchNode = 1,
    /// add a node to the inner tuple
    spgAddNode,
    /// split inner tuple (change its prefix)
    spgSplitTuple,
}

/// results for spgMatchNode
pub struct spgChooseMatchNode {
    /// descend to this node (index from 0)
    pub node_n: i32,
    /// increment level by this much
    pub level_add: i32,
    /// new leaf datum
    pub rest_datum: Datum,
}

/// results for spgAddNode
pub struct spgChooseAddNode {
    /// new node's label
    pub node_label: Datum,
    /// where to insert it (index from 0)
    pub node_n: i32,
}

/// results for spgSplitTuple
pub struct spgChooseSplitTuple {
    /// new upper-level inner tuple should have a prefix?
    pub prefix_has_prefix: bool,
    /// if so, its value
    pub prefix_prefix_datum: Datum,
    /// number of nodes
    pub prefix_n_nodes: i32,
    /// their labels (empty for no labels)
    pub prefix_node_labels: Vec<Datum>,
    /// which node gets child tuple
    pub child_node_n: i32,
    /// new lower-level inner tuple should have a prefix?
    pub postfix_has_prefix: bool,
    /// if so, its value
    pub postfix_prefix_datum: Datum,
}

/// C tagged union (resultType + union) -> Rust enum.
pub enum spgChooseOut {
    MatchNode(spgChooseMatchNode),
    AddNode(spgChooseAddNode),
    SplitTuple(spgChooseSplitTuple),
}

/// Argument struct for spg_picksplit method (in).
pub struct spgPickSplitIn {
    /// number of leaf tuples
    pub n_tuples: i32,
    /// their datums (array of length nTuples)
    pub datums: Vec<Datum>,
    /// current level (counting from zero)
    pub level: i32,
}

/// Argument struct for spg_picksplit method (out).
pub struct spgPickSplitOut {
    /// new inner tuple should have a prefix?
    pub has_prefix: bool,
    /// if so, its value
    pub prefix_datum: Datum,
    /// number of nodes for new inner tuple
    pub n_nodes: i32,
    /// their labels (empty for no labels)
    pub node_labels: Vec<Datum>,
    /// node index for each leaf tuple
    pub map_tuples_to_nodes: Vec<i32>,
    /// datum to store in each new leaf tuple
    pub leaf_tuple_datums: Vec<Datum>,
}

/// Argument struct for spg_inner_consistent method (in).
pub struct spgInnerConsistentIn {
    /// array of operators and comparison values
    pub scankeys: ScanKey<'static>, // TODO(ptr): array base
    /// array of ordering operators and comparison values
    pub orderbys: ScanKey<'static>, // TODO(ptr): array base
    /// length of scankeys array
    pub nkeys: i32,
    /// length of orderbys array
    pub norderbys: i32,

    /// value reconstructed at parent
    pub reconstructed_value: Datum,
    /// opclass-specific traverse value
    pub traversal_value: *mut core::ffi::c_void, // TODO(ptr)
    /// put new traverse values here
    pub traversal_memory_context: MemoryContext,
    /// current level (counting from zero)
    pub level: i32,
    /// original data must be returned?
    pub return_data: bool,

    /// tuple is marked all-the-same?
    pub all_the_same: bool,
    /// tuple has a prefix?
    pub has_prefix: bool,
    /// if so, the prefix value
    pub prefix_datum: Datum,
    /// number of nodes in the inner tuple
    pub n_nodes: i32,
    /// node label values (empty if none)
    pub node_labels: Vec<Datum>,
}

/// Argument struct for spg_inner_consistent method (out).
pub struct spgInnerConsistentOut {
    /// number of child nodes to be visited
    pub n_nodes: i32,
    /// their indexes in the node array
    pub node_numbers: Vec<i32>,
    /// increment level by this much for each
    pub level_adds: Vec<i32>,
    /// associated reconstructed values
    pub reconstructed_values: Vec<Datum>,
    /// opclass-specific traverse values
    pub traversal_values: Vec<*mut core::ffi::c_void>, // TODO(ptr)
    /// associated distances (per node, array of doubles)
    pub distances: Vec<*mut f64>, // TODO(ptr)
}

/// Argument struct for spg_leaf_consistent method (in).
pub struct spgLeafConsistentIn {
    /// array of operators and comparison values
    pub scankeys: ScanKey<'static>, // TODO(ptr): array base
    /// array of ordering operators and comparison values
    pub orderbys: ScanKey<'static>, // TODO(ptr): array base
    /// length of scankeys array
    pub nkeys: i32,
    /// length of orderbys array
    pub norderbys: i32,

    /// value reconstructed at parent
    pub reconstructed_value: Datum,
    /// opclass-specific traverse value
    pub traversal_value: *mut core::ffi::c_void, // TODO(ptr)
    /// current level (counting from zero)
    pub level: i32,
    /// original data must be returned?
    pub return_data: bool,

    /// datum in leaf tuple
    pub leaf_datum: Datum,
}

/// Argument struct for spg_leaf_consistent method (out).
pub struct spgLeafConsistentOut {
    /// reconstructed original data, if any
    pub leaf_value: Datum,
    /// set true if operator must be rechecked
    pub recheck: bool,
    /// set true if distances must be rechecked
    pub recheck_distances: bool,
    /// associated distances
    pub distances: Vec<f64>,
}

/* spgutils.c */
pub fn spgoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}

/* spginsert.c */
pub fn spgbuild(
    _heap: Relation,
    _index: Relation,
    _index_info: &mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}
pub fn spgbuildempty(_index: Relation) {
    unimplemented!()
}
pub fn spginsert(
    _index: Relation,
    _values: &[Datum],
    _isnull: &[bool],
    _ht_ctid: &mut ItemPointerData,
    _heap_rel: Relation,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: &mut IndexInfo,
) -> bool {
    unimplemented!()
}

/* spgscan.c */
pub fn spgbeginscan(_rel: Relation, _keysz: i32, _orderbysz: i32) -> IndexScanDesc {
    unimplemented!()
}
pub fn spgendscan(_scan: IndexScanDesc) {
    unimplemented!()
}
pub fn spgrescan(
    _scan: IndexScanDesc,
    _scankey: ScanKey,
    _nscankeys: i32,
    _orderbys: ScanKey,
    _norderbys: i32,
) {
    unimplemented!()
}
pub fn spggetbitmap(_scan: IndexScanDesc, _tbm: &mut TIDBitmap) -> i64 {
    unimplemented!()
}
pub fn spggettuple(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}
pub fn spgcanreturn(_index: Relation, _attno: i32) -> bool {
    unimplemented!()
}

/* spgvacuum.c */
pub fn spgbulkdelete(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
    _callback: &mut IndexBulkDeleteCallback,
    _callback_state: *mut core::ffi::c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub fn spgvacuumcleanup(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}

/* spgvalidate.c */
pub fn spgvalidate(_opclassoid: Oid) -> bool {
    unimplemented!()
}
pub fn spgadjustmembers(
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: &mut [OpFamilyMember],
    _functions: &mut [OpFamilyMember],
) {
    unimplemented!()
}
