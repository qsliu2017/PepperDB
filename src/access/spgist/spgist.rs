//! access/spgist.h - Public header file for SP-GiST access method.

use std::ffi::{c_int, c_void};

use crate::access::common::scankey::ScanKey;
use crate::nodes::pg_list::List;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointer;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::rel::Relation;

// ---------------------------------------------------------------------------
// Referenced-but-not-(fully)-ported types. Imported from amapi.rs where it
// hosts the opaque genam aliases; otherwise stubbed locally. TODO: dedup.
// ---------------------------------------------------------------------------
use crate::access::index::amapi::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo, TIDBitmap,
};

/// TODO: dedup - real def is `bytea` (varlena) in c.rs.
pub type bytea = c_void;
/// TODO: dedup - real def `struct IndexInfo` in nodes/execnodes.h.
pub type IndexInfo = c_void;
/// TODO: dedup - real def `typedef enum ScanDirection` in access/sdir.h.
pub type ScanDirection = c_int;

/* SPGiST opclass support function numbers */
pub const SPGIST_CONFIG_PROC: c_int = 1;
pub const SPGIST_CHOOSE_PROC: c_int = 2;
pub const SPGIST_PICKSPLIT_PROC: c_int = 3;
pub const SPGIST_INNER_CONSISTENT_PROC: c_int = 4;
pub const SPGIST_LEAF_CONSISTENT_PROC: c_int = 5;
pub const SPGIST_COMPRESS_PROC: c_int = 6;
pub const SPGIST_OPTIONS_PROC: c_int = 7;
pub const SPGISTNRequiredProc: c_int = 5;
pub const SPGISTNProc: c_int = 7;

/*
 * Argument structs for spg_config method
 */
#[repr(C)]
pub struct spgConfigIn {
    pub attType: Oid, /* Data type to be indexed */
}

#[repr(C)]
pub struct spgConfigOut {
    pub prefixType: Oid,     /* Data type of inner-tuple prefixes */
    pub labelType: Oid,      /* Data type of inner-tuple node labels */
    pub leafType: Oid,       /* Data type of leaf-tuple values */
    pub canReturnData: bool, /* Opclass can reconstruct original data */
    pub longValuesOK: bool,  /* Opclass can cope with values > 1 page */
}

/*
 * Argument structs for spg_choose method
 */
#[repr(C)]
pub struct spgChooseIn {
    pub datum: Datum,     /* original datum to be indexed */
    pub leafDatum: Datum, /* current datum to be stored at leaf */
    pub level: c_int,     /* current level (counting from zero) */

    /* Data from current inner tuple */
    pub allTheSame: bool,      /* tuple is marked all-the-same? */
    pub hasPrefix: bool,       /* tuple has a prefix? */
    pub prefixDatum: Datum,    /* if so, the prefix value */
    pub nNodes: c_int,         /* number of nodes in the inner tuple */
    pub nodeLabels: *mut Datum, /* node label values (NULL if none) */
}

pub type spgChooseResultType = c_int;
pub const spgMatchNode: spgChooseResultType = 1; /* descend into existing node */
pub const spgAddNode: spgChooseResultType = 2; /* add a node to the inner tuple */
pub const spgSplitTuple: spgChooseResultType = 3; /* split inner tuple (change its prefix) */

/* results for spgMatchNode */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgChooseOut_matchNode {
    pub nodeN: c_int,       /* descend to this node (index from 0) */
    pub levelAdd: c_int,    /* increment level by this much */
    pub restDatum: Datum,   /* new leaf datum */
}

/* results for spgAddNode */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgChooseOut_addNode {
    pub nodeLabel: Datum, /* new node's label */
    pub nodeN: c_int,     /* where to insert it (index from 0) */
}

/* results for spgSplitTuple */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgChooseOut_splitTuple {
    /* Info to form new upper-level inner tuple with one child tuple */
    pub prefixHasPrefix: bool,         /* tuple should have a prefix? */
    pub prefixPrefixDatum: Datum,      /* if so, its value */
    pub prefixNNodes: c_int,           /* number of nodes */
    pub prefixNodeLabels: *mut Datum,  /* their labels (or NULL for no labels) */
    pub childNodeN: c_int,             /* which node gets child tuple */

    /* Info to form new lower-level inner tuple with all old nodes */
    pub postfixHasPrefix: bool,        /* tuple should have a prefix? */
    pub postfixPrefixDatum: Datum,     /* if so, its value */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union spgChooseOut_result {
    pub matchNode: spgChooseOut_matchNode,
    pub addNode: spgChooseOut_addNode,
    pub splitTuple: spgChooseOut_splitTuple,
}

#[repr(C)]
pub struct spgChooseOut {
    pub resultType: spgChooseResultType, /* action code, see above */
    pub result: spgChooseOut_result,
}

/*
 * Argument structs for spg_picksplit method
 */
#[repr(C)]
pub struct spgPickSplitIn {
    pub nTuples: c_int,    /* number of leaf tuples */
    pub datums: *mut Datum, /* their datums (array of length nTuples) */
    pub level: c_int,      /* current level (counting from zero) */
}

#[repr(C)]
pub struct spgPickSplitOut {
    pub hasPrefix: bool,    /* new inner tuple should have a prefix? */
    pub prefixDatum: Datum, /* if so, its value */

    pub nNodes: c_int,          /* number of nodes for new inner tuple */
    pub nodeLabels: *mut Datum, /* their labels (or NULL for no labels) */

    pub mapTuplesToNodes: *mut c_int,  /* node index for each leaf tuple */
    pub leafTupleDatums: *mut Datum,   /* datum to store in each new leaf tuple */
}

/*
 * Argument structs for spg_inner_consistent method
 */
#[repr(C)]
pub struct spgInnerConsistentIn {
    pub scankeys: ScanKey, /* array of operators and comparison values */
    pub orderbys: ScanKey, /* array of ordering operators and comparison values */
    pub nkeys: c_int,      /* length of scankeys array */
    pub norderbys: c_int,  /* length of orderbys array */

    pub reconstructedValue: Datum, /* value reconstructed at parent */
    pub traversalValue: *mut c_void, /* opclass-specific traverse value */
    pub traversalMemoryContext: MemoryContext, /* put new traverse values here */
    pub level: c_int,      /* current level (counting from zero) */
    pub returnData: bool,  /* original data must be returned? */

    /* Data from current inner tuple */
    pub allTheSame: bool,       /* tuple is marked all-the-same? */
    pub hasPrefix: bool,        /* tuple has a prefix? */
    pub prefixDatum: Datum,     /* if so, the prefix value */
    pub nNodes: c_int,          /* number of nodes in the inner tuple */
    pub nodeLabels: *mut Datum, /* node label values (NULL if none) */
}

#[repr(C)]
pub struct spgInnerConsistentOut {
    pub nNodes: c_int,                       /* number of child nodes to be visited */
    pub nodeNumbers: *mut c_int,             /* their indexes in the node array */
    pub levelAdds: *mut c_int,               /* increment level by this much for each */
    pub reconstructedValues: *mut Datum,     /* associated reconstructed values */
    pub traversalValues: *mut *mut c_void,   /* opclass-specific traverse values */
    pub distances: *mut *mut f64,            /* associated distances */
}

/*
 * Argument structs for spg_leaf_consistent method
 */
#[repr(C)]
pub struct spgLeafConsistentIn {
    pub scankeys: ScanKey, /* array of operators and comparison values */
    pub orderbys: ScanKey, /* array of ordering operators and comparison values */
    pub nkeys: c_int,      /* length of scankeys array */
    pub norderbys: c_int,  /* length of orderbys array */

    pub reconstructedValue: Datum,   /* value reconstructed at parent */
    pub traversalValue: *mut c_void, /* opclass-specific traverse value */
    pub level: c_int,                /* current level (counting from zero) */
    pub returnData: bool,            /* original data must be returned? */

    pub leafDatum: Datum, /* datum in leaf tuple */
}

#[repr(C)]
pub struct spgLeafConsistentOut {
    pub leafValue: Datum,        /* reconstructed original data, if any */
    pub recheck: bool,           /* set true if operator must be rechecked */
    pub recheckDistances: bool,  /* set true if distances must be rechecked */
    pub distances: *mut f64,     /* associated distances */
}

/* spgutils.c */
pub unsafe fn spgoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    unimplemented!()
}

/* spginsert.c */
pub unsafe fn spgbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}
pub unsafe fn spgbuildempty(index: Relation) {
    unimplemented!()
}
pub unsafe fn spginsert(
    index: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    unimplemented!()
}

/* spgscan.c */
pub unsafe fn spgbeginscan(rel: Relation, keysz: c_int, orderbysz: c_int) -> IndexScanDesc {
    unimplemented!()
}
pub unsafe fn spgendscan(scan: IndexScanDesc) {
    unimplemented!()
}
pub unsafe fn spgrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    unimplemented!()
}
pub unsafe fn spggetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> i64 {
    unimplemented!()
}
pub unsafe fn spggettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    unimplemented!()
}
pub unsafe fn spgcanreturn(index: Relation, attno: c_int) -> bool {
    unimplemented!()
}

/* spgvacuum.c */
pub unsafe fn spgbulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub unsafe fn spgvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}

/* spgvalidate.c */
pub unsafe fn spgvalidate(opclassoid: Oid) -> bool {
    unimplemented!()
}
pub unsafe fn spgadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    unimplemented!()
}
