/*-------------------------------------------------------------------------
 *
 * plancat.rs
 *    routines for accessing the system catalogs
 *
 * Translation of postgres/src/backend/optimizer/util/plancat.c
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_void};

use crate::nodes::nodes::NodeTag;
use crate::nodes::nodes::NodeTag::{
    T_Const, T_ForeignKeyOptInfo, T_IndexOptInfo, T_InferenceElem, T_NullTest,
    T_StatisticExtInfo, T_SupportRequestCost, T_SupportRequestRows,
    T_SupportRequestSelectivity, T_Var,
};
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::RTEKind::{
    RTE_CTE, RTE_FUNCTION, RTE_NAMEDTUPLESTORE, RTE_RELATION, RTE_RESULT, RTE_SUBQUERY,
    RTE_TABLEFUNC, RTE_VALUES,
};
use crate::nodes::nodes::OnConflictAction;
use crate::nodes::parsenodes::{Query, RangeTblEntry};
use crate::nodes::primnodes::{InferenceElem, OnConflictExpr};
use crate::nodes::pathnodes::{
    ForeignKeyOptInfo, IndexOptInfo, PlannerInfo, RelOptInfo, Relids, RestrictInfo,
    StatisticExtInfo,
};
use crate::nodes::pg_list::{
    lappend, lappend_oid, lcons, lfirst, lfirst_oid, list_difference, list_free, list_head,
    list_length, list_member, list_nth, lnext, List, NIL,
};
use crate::nodes::primnodes::{
    AttrNumber, Const, Expr, NullTest, NullTestType, TargetEntry, Var,
};
use crate::nodes::bitmapset::{
    bms_add_member, bms_copy, bms_equal, bms_free, bms_overlap,
};
use crate::{castNode, current_cell, foreach, list_make1, makeNode, Assert, IsA};
use crate::postgres_ext::Oid;
use crate::c::Index;
use crate::storage::lockdefs::NoLock;

/* ------------------------------------------------------------------ */
/* Stubs for unported dependencies (TODO(pg-port))                     */
/* ------------------------------------------------------------------ */

pub type Relation = *mut RelationData;
pub type BlockNumber = u32;
pub type RegProcedure = Oid;
pub type Selectivity = f64;
pub type LOCKMODE = c_int;

pub use crate::utils::rel::RelationData;

pub use crate::access::htup_details::{HeapTupleData, HeapTupleHeaderData};
pub type HeapTuple = *mut HeapTupleData;

pub use crate::catalog::pg_class::FormData_pg_class;

pub use crate::catalog::pg_index::FormData_pg_index;
pub type Form_pg_index = *mut FormData_pg_index;

pub use crate::access::common::tupdesc::{
    AttrDefault, CompactAttribute, ConstrCheck, TupleConstr, TupleDescData,
};

pub use crate::catalog::pg_attribute::FormData_pg_attribute;
pub type Form_pg_attribute = *mut FormData_pg_attribute;

/* attnullability values */
pub use crate::access::common::tupdesc::{ATTNULLABLE_UNKNOWN, ATTNULLABLE_VALID};

pub use crate::access::table::tableam::TableAmRoutine;

pub use crate::access::index::amapi::IndexAmRoutine;

pub use crate::utils::reltrigger::TriggerDesc;

#[repr(C)]
pub struct ForeignKeyCacheInfo {
    pub conoid: Oid,
    pub conrelid: Oid,
    pub confrelid: Oid,
    pub nkeys: c_int,
    pub conkey: [AttrNumber; 32],
    pub confkey: [AttrNumber; 32],
    pub conpfeqop: [Oid; 32],
    pub conenforced: bool,
}

pub use crate::nodes::pathnodes::QualCost;

pub use crate::nodes::supportnodes::{
    SupportRequestCost, SupportRequestRows, SupportRequestSelectivity,
};

pub type SpecialJoinInfo = crate::nodes::pathnodes::SpecialJoinInfo;

pub use crate::catalog::pg_proc::FormData_pg_proc;
pub type Form_pg_proc = *mut FormData_pg_proc;

pub use crate::catalog::pg_statistic_ext_data::FormData_pg_statistic_ext_data;
pub type Form_pg_statistic_ext_data = *mut FormData_pg_statistic_ext_data;

pub use crate::catalog::pg_statistic_ext::FormData_pg_statistic_ext;
pub type Form_pg_statistic_ext = *mut FormData_pg_statistic_ext;

/*
 * Canonical FormData_pg_statistic_ext omits the inline int2vector `stxkeys`
 * (a variable-length field). This helper mirrors the on-disk fixed layout up
 * to and including stxkeys so the column array can be read; used only for the
 * stxkeys access below.
 */
#[repr(C)]
struct PgStatisticExtFixed {
    oid: Oid,
    stxrelid: Oid,
    stxname: crate::c::NameData,
    stxnamespace: Oid,
    stxowner: Oid,
    stxkeys: crate::c::int2vector,
}

/* PartitionKey / PartitionDesc / PartitionScheme stubs */
pub type PartitionKey = *mut PartitionKeyData;
pub type PartitionDesc = *mut PartitionDescData;
pub type PartitionScheme = *mut PartitionSchemeData;
pub type PartitionDirectory = *mut c_void;

pub use crate::utils::cache::partcache::PartitionKeyData;

pub use crate::partitioning::partdesc::PartitionDescData;

pub use crate::nodes::pathnodes::PartitionSchemeData;

pub use crate::utils::fmgr::FmgrInfo;

pub type CompareType = c_int;
pub const COMPARE_LT: CompareType = 1;

/* constraint_exclusion GUC values */
pub const CONSTRAINT_EXCLUSION_OFF: c_int = 0;
pub const CONSTRAINT_EXCLUSION_ON: c_int = 1;
pub const CONSTRAINT_EXCLUSION_PARTITION: c_int = 2;

/* AMFLAG */
pub const AMFLAG_HAS_TID_RANGE: u32 = 1 << 0;

/* RELKIND constants */
pub const RELKIND_RELATION: i8 = b'r' as i8;
pub const RELKIND_INDEX: i8 = b'i' as i8;
pub const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
pub const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;
pub const RELKIND_PARTITIONED_INDEX: i8 = b'I' as i8;

/* INDOPTION flags */
pub const INDOPTION_DESC: i16 = 0x0001;
pub const INDOPTION_NULLS_FIRST: i16 = 0x0002;

/* BTREE_AM_OID */
pub const BTREE_AM_OID: Oid = 403;

/* STATS_EXT kinds */
pub const STATS_EXT_NDISTINCT: u8 = b'n';
pub const STATS_EXT_DEPENDENCIES: u8 = b'd';
pub const STATS_EXT_MCV: u8 = b'm';
pub const STATS_EXT_EXPRESSIONS: u8 = b'e';

/* syscache IDs used here */
pub use crate::utils::cache::syscache::{PROCOID, STATEXTDATASTXOID, STATEXTOID};

/* pg_statistic_ext attribute number */
pub const Anum_pg_statistic_ext_stxexprs: c_int = 10;

/* VAR_RETURNING_DEFAULT */
pub const VAR_RETURNING_DEFAULT: c_int = 0;

/* RESTRICT_RELKIND_FOREIGN_TABLE */
pub const RESTRICT_RELKIND_FOREIGN_TABLE: u32 = 1 << 1;

/* GUC parameter */
pub static mut constraint_exclusion: c_int = CONSTRAINT_EXCLUSION_PARTITION;

/* Hook for plugins to get control in get_relation_info() */
pub type get_relation_info_hook_type =
    Option<unsafe extern "C" fn(*mut PlannerInfo, Oid, bool, *mut RelOptInfo)>;
pub static mut get_relation_info_hook: get_relation_info_hook_type = None;

/* ------------------------------------------------------------------ */
/* Extern stubs for functions in other (unported) modules              */
/* ------------------------------------------------------------------ */

extern "C" {
    fn table_open(relationid: Oid, lockmode: LOCKMODE) -> Relation;
    fn table_close(relation: Relation, lockmode: LOCKMODE);
    fn index_open(indexoid: Oid, lockmode: LOCKMODE) -> Relation;
    fn index_close(indexrelation: Relation, lockmode: LOCKMODE);
    fn RelationGetIndexList(relation: Relation) -> *mut List;
    fn RelationGetFKeyList(relation: Relation) -> *mut List;
    fn RelationGetStatExtList(relation: Relation) -> *mut List;
    fn RelationGetIndexExpressions(indexrelation: Relation) -> *mut List;
    fn RelationGetIndexPredicate(indexrelation: Relation) -> *mut List;
    fn RelationGetIndexAttOptions(indexrelation: Relation, copy: bool) -> *mut *mut c_void;
    fn RelationGetNumberOfBlocks(relation: Relation) -> BlockNumber;
    fn RelationGetRelid(relation: Relation) -> Oid;
    fn RelationGetForm(relation: Relation) -> *mut FormData_pg_class;
    fn RelationGetNumberOfAttributes(relation: Relation) -> c_int;
    fn RelationGetDescr(relation: Relation) -> *mut TupleDescData;
    fn RelationGetPartitionKey(relation: Relation) -> PartitionKey;
    fn RelationGetPartitionQual(relation: Relation) -> *mut List;
    fn RelationGetParallelWorkers(relation: Relation, default_val: c_int) -> c_int;
    fn RelationIsPermanent(relation: Relation) -> bool;
    fn IsSystemRelation(relation: Relation) -> bool;
    fn RelationGetRelationName(relation: Relation) -> *const c_char;
    fn index_can_return(indexrelation: Relation, attno: c_int) -> bool;
    fn table_relation_estimate_size(
        rel: Relation,
        attr_widths: *mut i32,
        pages: *mut BlockNumber,
        tuples: *mut f64,
        allvisfrac: *mut f64,
    );
    fn estimate_rel_size(
        rel: Relation,
        attr_widths: *mut i32,
        pages: *mut BlockNumber,
        tuples: *mut f64,
        allvisfrac: *mut f64,
    );
    fn TupleDescAttr(tupdesc: *mut TupleDescData, attno: c_int) -> Form_pg_attribute;
    fn TupleDescCompactAttr(tupdesc: *mut TupleDescData, attno: c_int) -> *mut CompactAttribute;
    fn SystemAttributeDefinition(attno: i16) -> *const FormData_pg_attribute;
    fn ChangeVarNodes(node: *mut Node, rt_index: c_int, new_index: c_int, sublevels_up: c_int);
    fn expand_generated_columns_in_expr(
        node: *mut Node,
        relation: Relation,
        varno: Index,
    ) -> *mut Node;
    fn expandRTE(
        rte: *mut RangeTblEntry,
        varno: Index,
        sublevels_up: c_int,
        varreturningtype: c_int,
        location: c_int,
        include_dropped: bool,
        colnames: *mut *mut List,
        colvars: *mut *mut List,
    );
    fn makeVar(
        varno: Index,
        varattno: AttrNumber,
        vartype: Oid,
        vartypmod: i32,
        varcollid: Oid,
        varlevelsup: c_int,
    ) -> *mut Var;
    fn makeVarFromTargetEntry(varno: Index, tle: *mut TargetEntry) -> *mut Var;
    fn makeTargetEntry(
        expr: *mut Expr,
        resno: i16,
        resname: *mut c_char,
        resjunk: bool,
    ) -> *mut TargetEntry;
    fn copyObject(obj: *mut c_void) -> *mut c_void;
    fn equal(a: *mut c_void, b: *mut c_void) -> bool;
    fn stringToNode(str_: *const c_char) -> *mut c_void;
    fn pfree(ptr: *mut c_void);
    fn palloc(size: usize) -> *mut c_void;
    fn palloc0(size: usize) -> *mut c_void;
    fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node;
    fn canonicalize_qual(qual: *mut Expr, is_check: bool) -> *mut Expr;
    fn expression_planner(clause: *mut Expr) -> *mut Expr;
    fn make_ands_implicit(qual: *mut Expr) -> *mut List;
    fn fix_opfuncids(node: *mut Node);
    fn predicate_implied_by(
        predicate_list: *mut List,
        clause_list: *mut List,
        strong: bool,
    ) -> bool;
    fn predicate_refuted_by(
        predicate_list: *mut List,
        clause_list: *mut List,
        strong: bool,
    ) -> bool;
    fn contain_mutable_functions(node: *mut Node) -> bool;
    fn pull_varattnos(node: *mut Node, varno: Index, varattnos: *mut *mut crate::nodes::bitmapset::Bitmapset);
    fn list_concat(list1: *mut List, list2: *mut List) -> *mut List;

    /* syscache */
    fn SearchSysCache1(cacheid: c_int, key1: u64) -> HeapTuple;
    fn SearchSysCache2(cacheid: c_int, key1: u64, key2: u64) -> HeapTuple;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn HeapTupleIsValid(tuple: HeapTuple) -> bool;
    fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void;
    fn SysCacheGetAttr(
        cacheid: c_int,
        tuple: HeapTuple,
        attnum: c_int,
        isnull: *mut bool,
    ) -> u64;
    fn TextDatumGetCString(datum: u64) -> *mut c_char;
    fn ObjectIdGetDatum(oid: Oid) -> u64;
    fn BoolGetDatum(b: bool) -> u64;
    fn PointerGetDatum(ptr: *const c_void) -> u64;
    fn Int32GetDatum(i: i32) -> u64;
    fn Int16GetDatum(i: i16) -> u64;
    fn DatumGetFloat8(datum: u64) -> f64;
    fn DatumGetBool(datum: u64) -> bool;
    fn DatumGetPointer(datum: u64) -> *mut c_void;
    fn OidFunctionCall1(functionId: Oid, arg1: u64) -> u64;
    fn OidFunctionCall4Coll(
        functionId: Oid,
        collation: Oid,
        arg1: u64,
        arg2: u64,
        arg3: u64,
        arg4: u64,
    ) -> u64;
    fn OidFunctionCall5Coll(
        functionId: Oid,
        collation: Oid,
        arg1: u64,
        arg2: u64,
        arg3: u64,
        arg4: u64,
        arg5: u64,
    ) -> u64;
    fn get_oprrest(opno: Oid) -> RegProcedure;
    fn get_oprjoin(opno: Oid) -> RegProcedure;
    fn get_func_support(funcid: Oid) -> RegProcedure;
    fn get_attavgwidth(relid: Oid, attnum: c_int) -> i32;
    fn get_typavgwidth(typid: Oid, typmod: i32) -> i32;
    fn clamp_width_est(width: i64) -> i32;
    fn get_opclass_family(opclass: Oid) -> Oid;
    fn get_opclass_input_type(opclass: Oid) -> Oid;
    fn get_opfamily_member_for_cmptype(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        cmptype: CompareType,
    ) -> Oid;
    fn get_ordering_op_properties(
        opno: Oid,
        opfamily: *mut Oid,
        opcintype: *mut Oid,
        cmptype: *mut CompareType,
    ) -> bool;
    fn get_constraint_index(constraintId: Oid) -> Oid;
    fn GetForeignServerIdByRelId(relid: Oid) -> Oid;
    fn GetFdwRoutineForRelation(relation: Relation, need_info: bool) -> *mut c_void;
    fn statext_is_kind_built(tuple: HeapTuple, kind: u8) -> bool;
    fn fmgr_info_copy(
        dstinfo: *mut FmgrInfo,
        srcinfo: *mut FmgrInfo,
        dest_mcxt: *mut c_void,
    );
    fn CreatePartitionDirectory(mcxt: *mut c_void, detach: bool) -> PartitionDirectory;
    fn PartitionDirectoryLookup(partdir: PartitionDirectory, relation: Relation) -> PartitionDesc;

    /* misc */
    fn RecoveryInProgress() -> bool;
    fn TransactionIdPrecedes(id1: u32, id2: u32) -> bool;
    fn HeapTupleHeaderGetXmin(tup: *mut HeapTupleHeaderData) -> u32;
    fn OidIsValid(oid: Oid) -> bool;
    fn InvalidAttrNumber() -> AttrNumber;
    fn rt_fetch(rti: Index, rtable: *mut List) -> *mut RangeTblEntry;
    fn planner_rt_fetch(rti: Index, root: *mut PlannerInfo) -> *mut RangeTblEntry;
    fn IS_SIMPLE_REL(rel: *const RelOptInfo) -> bool;
    fn RELKIND_HAS_TABLE_AM(relkind: i8) -> bool;

    static cpu_operator_cost: f64;
    fn CurrentMemoryContext() -> *mut c_void;
    fn restrict_nonsystem_relation_kind() -> u32;
    fn FirstNormalObjectId() -> Oid;
    fn TransactionXmin() -> u32;

    fn errdetail_relkind_not_supported(relkind: i8) -> c_int;
}

/* Convenience alias for RELOPT_BASEREL / RELOPT_OTHER_MEMBER_REL -- mirrors pathnodes.h */
pub const RELOPT_BASEREL: u8 = 0;
pub const RELOPT_OTHER_MEMBER_REL: u8 = 1;

#[inline]
unsafe fn InvalidOid() -> Oid {
    0
}

/* FirstLowInvalidHeapAttributeNumber -- sysattr.h */
pub const FirstLowInvalidHeapAttributeNumber: c_int = -8;

/* SizeofHeapTupleHeader / SizeOfPageHeaderData / BLCKSZ / MAXALIGN */
pub const SizeofHeapTupleHeader: usize = 24;
pub const SizeOfPageHeaderData: usize = 24;
pub const BLCKSZ: usize = 8192;
#[inline]
fn MAXALIGN(x: usize) -> usize {
    (x + 7) & !7
}
#[inline]
fn sizeof_itemiddata() -> usize {
    core::mem::size_of::<u32>()
}

/*
 * get_relation_info -
 *    Retrieves catalog information for a given relation.
 *
 * Given the Oid of the relation, return the following info into fields
 * of the RelOptInfo struct:
 *
 *    min_attr     lowest valid AttrNumber
 *    max_attr     highest valid AttrNumber
 *    indexlist    list of IndexOptInfos for relation's indexes
 *    statlist     list of StatisticExtInfo for relation's statistic objects
 *    serverid     if it's a foreign table, the server OID
 *    fdwroutine   if it's a foreign table, the FDW function pointers
 *    pages        number of pages
 *    tuples       number of tuples
 *    rel_parallel_workers user-defined number of parallel workers
 *
 * Also, add information about the relation's foreign keys to root->fkey_list.
 *
 * Also, initialize the attr_needed[] and attr_widths[] arrays.  In most
 * cases these are left as zeroes, but sometimes we need to compute attr
 * widths here, and we may as well cache the results for costsize.c.
 *
 * If inhparent is true, all we need to do is set up the attr arrays:
 * the RelOptInfo actually represents the appendrel formed by an inheritance
 * tree, and so the parent rel's physical size and index information isn't
 * important for it, however, for partitioned tables, we do populate the
 * indexlist as the planner uses unique indexes as unique proofs for certain
 * optimizations.
 */
pub unsafe fn get_relation_info(
    root: *mut PlannerInfo,
    relationObjectId: Oid,
    inhparent: bool,
    rel: *mut RelOptInfo,
) {
    let varno: Index = (*rel).relid;
    let mut indexinfos: *mut List = NIL;

    /*
     * We need not lock the relation since it was already locked, either by
     * the rewriter or when expand_inherited_rtentry() added it to the query's
     * rangetable.
     */
    let relation = table_open(relationObjectId, NoLock);

    /*
     * Relations without a table AM can be used in a query only if they are of
     * special-cased relkinds.  This check prevents us from crashing later if,
     * for example, a view's ON SELECT rule has gone missing.  Note that
     * table_open() already rejected indexes and composite types; spell the
     * error the same way it does.
     */
    if (*relation).rd_tableam.is_null() {
        if !((*(*relation).rd_rel).relkind == RELKIND_FOREIGN_TABLE
            || (*(*relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE)
        {
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errdetail_relkind_not_supported(...) */
            ereport!(
                crate::utils::elog::ERROR,
                errmsg!(
                    "cannot open relation \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(relation))
                        .to_string_lossy()
                )
            );
        }
    }

    /* Temporary and unlogged relations are inaccessible during recovery. */
    if !RelationIsPermanent(relation) && RecoveryInProgress() {
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        ereport!(
            crate::utils::elog::ERROR,
            errmsg!("cannot access temporary or unlogged relations during recovery")
        );
    }

    (*rel).min_attr = FirstLowInvalidHeapAttributeNumber as AttrNumber + 1;
    (*rel).max_attr = RelationGetNumberOfAttributes(relation) as AttrNumber;
    (*rel).reltablespace = (*RelationGetForm(relation)).reltablespace;

    Assert!((*rel).max_attr >= (*rel).min_attr);
    let nattrs = ((*rel).max_attr - (*rel).min_attr + 1) as usize;
    (*rel).attr_needed = palloc0(nattrs * core::mem::size_of::<Relids>()) as *mut Relids;
    (*rel).attr_widths = palloc0(nattrs * core::mem::size_of::<i32>()) as *mut i32;

    /*
     * Record which columns are defined as NOT NULL.  We leave this
     * unpopulated for non-partitioned inheritance parent relations as it's
     * ambiguous as to what it means.  Some child tables may have a NOT NULL
     * constraint for a column while others may not.  We could work harder and
     * build a unioned set of all child relations notnullattnums, but there's
     * currently no need.  The RelOptInfo corresponding to the !inh
     * RangeTblEntry does get populated.
     */
    if !inhparent || (*(*relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        let natts = (*(*relation).rd_att).natts;
        for i in 0..natts {
            let attr = TupleDescCompactAttr((*relation).rd_att, i);

            Assert!((*attr).attnullability != ATTNULLABLE_UNKNOWN);

            if (*attr).attnullability == ATTNULLABLE_VALID {
                (*rel).notnullattnums =
                    bms_add_member((*rel).notnullattnums, i + 1);

                /*
                 * Per RemoveAttributeById(), dropped columns will have their
                 * attnotnull unset, so we needn't check for dropped columns
                 * in the above condition.
                 */
                Assert!(!(*attr).attisdropped);
            }
        }
    }

    /*
     * Estimate relation size --- unless it's an inheritance parent, in which
     * case the size we want is not the rel's own size but the size of its
     * inheritance tree.  That will be computed in set_append_rel_size().
     */
    if !inhparent {
        estimate_rel_size(
            relation,
            (*rel).attr_widths.offset(-((*rel).min_attr as isize)),
            &mut (*rel).pages,
            &mut (*rel).tuples,
            &mut (*rel).allvisfrac,
        );
    }

    /* Retrieve the parallel_workers reloption, or -1 if not set. */
    (*rel).rel_parallel_workers = RelationGetParallelWorkers(relation, -1);

    /*
     * Make list of indexes.  Ignore indexes on system catalogs if told to.
     * Don't bother with indexes from traditional inheritance parents.  For
     * partitioned tables, we need a list of at least unique indexes as these
     * serve as unique proofs for certain planner optimizations.  However,
     * let's not discriminate here and just record all partitioned indexes
     * whether they're unique indexes or not.
     */
    let hasindex: bool;
    if (inhparent && (*(*relation).rd_rel).relkind != RELKIND_PARTITIONED_TABLE)
        || (crate::utils::init::miscinit::IgnoreSystemIndexes && IsSystemRelation(relation))
    {
        hasindex = false;
    } else {
        hasindex = (*(*relation).rd_rel).relhasindex;
    }

    if hasindex {
        let indexoidlist = RelationGetIndexList(relation);

        /*
         * For each index, we get the same type of lock that the executor will
         * need, and do not release it.  This saves a couple of trips to the
         * shared lock manager while not creating any real loss of
         * concurrency, because no schema changes could be happening on the
         * index while we hold lock on the parent rel, and no lock type used
         * for queries blocks any other kind of index operation.
         */
        let lmode: LOCKMODE = (*(*(*root).simple_rte_array.add(varno as usize))).rellockmode;

        let mut l = list_head(indexoidlist);
        while !l.is_null() {
            let indexoid: Oid = lfirst_oid(l);
            l = lnext(indexoidlist, l);

            /*
             * Extract info from the relation descriptor for the index.
             */
            let indexRelation = index_open(indexoid, lmode);
            let index: Form_pg_index = (*indexRelation).rd_index;

            /*
             * Ignore invalid indexes, since they can't safely be used for
             * queries.  Note that this is OK because the data structure we
             * are constructing is only used by the planner --- the executor
             * still needs to insert into "invalid" indexes, if they're marked
             * indisready.
             */
            if !(*index).indisvalid {
                index_close(indexRelation, NoLock);
                continue;
            }

            /*
             * If the index is valid, but cannot yet be used, ignore it; but
             * mark the plan we are generating as transient. See
             * src/backend/access/heap/README.HOT for discussion.
             */
            if (*index).indcheckxmin
                && !TransactionIdPrecedes(
                    HeapTupleHeaderGetXmin(
                        (*((*indexRelation).rd_indextuple as *mut HeapTupleData)).t_data,
                    ),
                    TransactionXmin(),
                )
            {
                (*(*root).glob).transientPlan = true;
                index_close(indexRelation, NoLock);
                continue;
            }

            let info: *mut IndexOptInfo =
                makeNode!(IndexOptInfo, T_IndexOptInfo) as *mut IndexOptInfo;

            (*info).indexoid = (*index).indexrelid;
            (*info).reltablespace = (*RelationGetForm(indexRelation)).reltablespace;
            (*info).rel = rel;
            let ncolumns = (*index).indnatts as c_int;
            let nkeycolumns = (*index).indnkeyatts as c_int;
            (*info).ncolumns = ncolumns;
            (*info).nkeycolumns = nkeycolumns;

            (*info).indexkeys =
                palloc(core::mem::size_of::<c_int>() * ncolumns as usize) as *mut c_int;
            (*info).indexcollations =
                palloc(core::mem::size_of::<Oid>() * nkeycolumns as usize) as *mut Oid;
            (*info).opfamily =
                palloc(core::mem::size_of::<Oid>() * nkeycolumns as usize) as *mut Oid;
            (*info).opcintype =
                palloc(core::mem::size_of::<Oid>() * nkeycolumns as usize) as *mut Oid;
            (*info).canreturn =
                palloc(core::mem::size_of::<bool>() * ncolumns as usize) as *mut bool;

            for i in 0..ncolumns {
                *(*info).indexkeys.add(i as usize) =
                    *(*index).indkey.values.as_ptr().add(i as usize) as c_int;
                *(*info).canreturn.add(i as usize) =
                    index_can_return(indexRelation, i + 1);
            }

            for i in 0..nkeycolumns {
                *(*info).opfamily.add(i as usize) =
                    *(*indexRelation).rd_opfamily.add(i as usize);
                *(*info).opcintype.add(i as usize) =
                    *(*indexRelation).rd_opcintype.add(i as usize);
                *(*info).indexcollations.add(i as usize) =
                    *(*indexRelation).rd_indcollation.add(i as usize);
            }

            (*info).relam = (*(*indexRelation).rd_rel).relam;

            /*
             * We don't have an AM for partitioned indexes, so we'll just
             * NULLify the AM related fields for those.
             */
            if (*(*indexRelation).rd_rel).relkind != RELKIND_PARTITIONED_INDEX {
                /* We copy just the fields we need, not all of rd_indam */
                let amroutine = (*indexRelation).rd_indam;
                (*info).amcanorderbyop = (*amroutine).amcanorderbyop;
                (*info).amoptionalkey = (*amroutine).amoptionalkey;
                (*info).amsearcharray = (*amroutine).amsearcharray;
                (*info).amsearchnulls = (*amroutine).amsearchnulls;
                (*info).amcanparallel = (*amroutine).amcanparallel;
                (*info).amhasgettuple = (*amroutine).amgettuple.is_some();
                (*info).amhasgetbitmap = (*amroutine).amgetbitmap.is_some()
                    && (*((*relation).rd_tableam as *const TableAmRoutine))
                        .scan_bitmap_next_tuple
                        .is_some();
                (*info).amcanmarkpos = (*amroutine).ammarkpos.is_some()
                    && (*amroutine).amrestrpos.is_some();
                (*info).amcostestimate = core::mem::transmute((*amroutine).amcostestimate);
                Assert!((*info).amcostestimate.is_some());

                /* Fetch index opclass options */
                (*info).opclassoptions =
                    RelationGetIndexAttOptions(indexRelation, true) as *mut *mut crate::c::varlena;

                /*
                 * Fetch the ordering information for the index, if any.
                 */
                if (*info).relam == BTREE_AM_OID {
                    /*
                     * If it's a btree index, we can use its opfamily OIDs
                     * directly as the sort ordering opfamily OIDs.
                     */
                    Assert!((*amroutine).amcanorder);

                    (*info).sortopfamily = (*info).opfamily;
                    (*info).reverse_sort =
                        palloc(core::mem::size_of::<bool>() * nkeycolumns as usize)
                            as *mut bool;
                    (*info).nulls_first =
                        palloc(core::mem::size_of::<bool>() * nkeycolumns as usize)
                            as *mut bool;

                    for i in 0..nkeycolumns {
                        let opt: i16 =
                            *(*indexRelation).rd_indoption.add(i as usize);
                        *(*info).reverse_sort.add(i as usize) =
                            (opt & INDOPTION_DESC) != 0;
                        *(*info).nulls_first.add(i as usize) =
                            (opt & INDOPTION_NULLS_FIRST) != 0;
                    }
                } else if (*amroutine).amcanorder {
                    /*
                     * Otherwise, identify the corresponding btree opfamilies
                     * by trying to map this index's "<" operators into btree.
                     * Since "<" uniquely defines the behavior of a sort
                     * order, this is a sufficient test.
                     *
                     * XXX This method is rather slow and complicated.  It'd
                     * be better to have a way to explicitly declare the
                     * corresponding btree opfamily for each opfamily of the
                     * other index type.
                     */
                    (*info).sortopfamily =
                        palloc(core::mem::size_of::<Oid>() * nkeycolumns as usize)
                            as *mut Oid;
                    (*info).reverse_sort =
                        palloc(core::mem::size_of::<bool>() * nkeycolumns as usize)
                            as *mut bool;
                    (*info).nulls_first =
                        palloc(core::mem::size_of::<bool>() * nkeycolumns as usize)
                            as *mut bool;

                    let mut sort_ok = true;
                    'sort_loop: for i in 0..nkeycolumns {
                        let opt: i16 =
                            *(*indexRelation).rd_indoption.add(i as usize);
                        *(*info).reverse_sort.add(i as usize) =
                            (opt & INDOPTION_DESC) != 0;
                        *(*info).nulls_first.add(i as usize) =
                            (opt & INDOPTION_NULLS_FIRST) != 0;

                        let opf_i = *(*info).opfamily.add(i as usize);
                        let opct_i = *(*info).opcintype.add(i as usize);
                        let ltopr = get_opfamily_member_for_cmptype(
                            opf_i, opct_i, opct_i, COMPARE_LT,
                        );
                        let mut opfamily: Oid = 0;
                        let mut opcintype: Oid = 0;
                        let mut cmptype: CompareType = 0;
                        if OidIsValid(ltopr)
                            && get_ordering_op_properties(
                                ltopr,
                                &mut opfamily,
                                &mut opcintype,
                                &mut cmptype,
                            )
                            && opcintype == opct_i
                            && cmptype == COMPARE_LT
                        {
                            /* Successful mapping */
                            *(*info).sortopfamily.add(i as usize) = opfamily;
                        } else {
                            /* Fail ... quietly treat index as unordered */
                            (*info).sortopfamily = core::ptr::null_mut();
                            (*info).reverse_sort = core::ptr::null_mut();
                            (*info).nulls_first = core::ptr::null_mut();
                            sort_ok = false;
                            break 'sort_loop;
                        }
                    }
                    let _ = sort_ok;
                } else {
                    (*info).sortopfamily = core::ptr::null_mut();
                    (*info).reverse_sort = core::ptr::null_mut();
                    (*info).nulls_first = core::ptr::null_mut();
                }
            } else {
                (*info).amcanorderbyop = false;
                (*info).amoptionalkey = false;
                (*info).amsearcharray = false;
                (*info).amsearchnulls = false;
                (*info).amcanparallel = false;
                (*info).amhasgettuple = false;
                (*info).amhasgetbitmap = false;
                (*info).amcanmarkpos = false;
                (*info).amcostestimate = None;

                (*info).sortopfamily = core::ptr::null_mut();
                (*info).reverse_sort = core::ptr::null_mut();
                (*info).nulls_first = core::ptr::null_mut();
            }

            /*
             * Fetch the index expressions and predicate, if any.  We must
             * modify the copies we obtain from the relcache to have the
             * correct varno for the parent relation, so that they match up
             * correctly against qual clauses.
             */
            (*info).indexprs = RelationGetIndexExpressions(indexRelation);
            (*info).indpred = RelationGetIndexPredicate(indexRelation);
            if !(*info).indexprs.is_null() && varno != 1 {
                ChangeVarNodes((*info).indexprs as *mut Node, 1, varno as c_int, 0);
            }
            if !(*info).indpred.is_null() && varno != 1 {
                ChangeVarNodes((*info).indpred as *mut Node, 1, varno as c_int, 0);
            }

            /* Build targetlist using the completed indexprs data */
            (*info).indextlist = build_index_tlist(root, info, relation);

            (*info).indrestrictinfo = NIL; /* set later, in indxpath.c */
            (*info).predOK = false; /* set later, in indxpath.c */
            (*info).unique = (*index).indisunique;
            (*info).nullsnotdistinct = (*index).indnullsnotdistinct;
            (*info).immediate = (*index).indimmediate;
            (*info).hypothetical = false;

            /*
             * Estimate the index size.  If it's not a partial index, we lock
             * the number-of-tuples estimate to equal the parent table; if it
             * is partial then we have to use the same methods as we would for
             * a table, except we can be sure that the index is not larger
             * than the table.  We must ignore partitioned indexes here as
             * there are not physical indexes.
             */
            if (*(*indexRelation).rd_rel).relkind != RELKIND_PARTITIONED_INDEX {
                if (*info).indpred.is_null() {
                    (*info).pages = RelationGetNumberOfBlocks(indexRelation);
                    (*info).tuples = (*rel).tuples;
                } else {
                    let mut allvisfrac: f64 = 0.0; /* dummy */
                    estimate_rel_size(
                        indexRelation,
                        core::ptr::null_mut(),
                        &mut (*info).pages,
                        &mut (*info).tuples,
                        &mut allvisfrac,
                    );
                    if (*info).tuples > (*rel).tuples {
                        (*info).tuples = (*rel).tuples;
                    }
                }

                /*
                 * Get tree height while we have the index open
                 */
                let amroutine = (*indexRelation).rd_indam;
                if let Some(amgettreeheight) = (*amroutine).amgettreeheight {
                    (*info).tree_height = amgettreeheight(indexRelation);
                } else {
                    /* For other index types, just set it to "unknown" for now */
                    (*info).tree_height = -1;
                }
            } else {
                /* Zero these out for partitioned indexes */
                (*info).pages = 0;
                (*info).tuples = 0.0;
                (*info).tree_height = -1;
            }

            index_close(indexRelation, NoLock);

            /*
             * We've historically used lcons() here.  It'd make more sense to
             * use lappend(), but that causes the planner to change behavior
             * in cases where two indexes seem equally attractive.  For now,
             * stick with lcons() --- few tables should have so many indexes
             * that the O(N^2) behavior of lcons() is really a problem.
             */
            indexinfos = lcons(info as *mut c_void, indexinfos);
        }

        list_free(indexoidlist);
    }

    (*rel).indexlist = indexinfos;

    (*rel).statlist = get_relation_statistics(rel, relation);

    /* Grab foreign-table info using the relcache, while we have it */
    if (*(*relation).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
        /* Check if the access to foreign tables is restricted */
        if restrict_nonsystem_relation_kind() & RESTRICT_RELKIND_FOREIGN_TABLE != 0 {
            /* there must not be built-in foreign tables */
            Assert!(RelationGetRelid(relation) >= FirstNormalObjectId());

            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            ereport!(
                crate::utils::elog::ERROR,
                errmsg!("access to non-system foreign table is restricted")
            );
        }

        (*rel).serverid = GetForeignServerIdByRelId(RelationGetRelid(relation));
        (*rel).fdwroutine = GetFdwRoutineForRelation(relation, true) as *mut crate::nodes::pathnodes::FdwRoutine;
    } else {
        (*rel).serverid = InvalidOid();
        (*rel).fdwroutine = core::ptr::null_mut();
    }

    /* Collect info about relation's foreign keys, if relevant */
    get_relation_foreign_keys(root, rel, relation, inhparent);

    /* Collect info about functions implemented by the rel's table AM. */
    if !(*relation).rd_tableam.is_null()
        && (*((*relation).rd_tableam as *const TableAmRoutine))
            .scan_set_tidrange
            .is_some()
        && (*((*relation).rd_tableam as *const TableAmRoutine))
            .scan_getnextslot_tidrange
            .is_some()
    {
        (*rel).amflags |= AMFLAG_HAS_TID_RANGE;
    }

    /*
     * Collect info about relation's partitioning scheme, if any. Only
     * inheritance parents may be partitioned.
     */
    if inhparent && (*(*relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        set_relation_partition_info(root, rel, relation);
    }

    table_close(relation, NoLock);

    /*
     * Allow a plugin to editorialize on the info we obtained from the
     * catalogs.  Actions might include altering the assumed relation size,
     * removing an index, or adding a hypothetical index to the indexlist.
     */
    if let Some(hook) = get_relation_info_hook {
        hook(root, relationObjectId, inhparent, rel);
    }
}

/*
 * get_relation_foreign_keys -
 *    Retrieves foreign key information for a given relation.
 *
 * ForeignKeyOptInfos for relevant foreign keys are created and added to
 * root->fkey_list.  We do this now while we have the relcache entry open.
 * We could sometimes avoid making useless ForeignKeyOptInfos if we waited
 * until all RelOptInfos have been built, but the cost of re-opening the
 * relcache entries would probably exceed any savings.
 */
unsafe fn get_relation_foreign_keys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    relation: Relation,
    inhparent: bool,
) {
    let rtable: *mut List = (*(*root).parse).rtable;
    let mut lc;

    /*
     * If it's not a baserel, we don't care about its FKs.  Also, if the query
     * references only a single relation, we can skip the lookup since no FKs
     * could satisfy the requirements below.
     */
    if (*rel).reloptkind != crate::nodes::pathnodes::RelOptKind::RELOPT_BASEREL || list_length(rtable) < 2 {
        return;
    }

    /*
     * If it's the parent of an inheritance tree, ignore its FKs.  We could
     * make useful FK-based deductions if we found that all members of the
     * inheritance tree have equivalent FK constraints, but detecting that
     * would require code that hasn't been written.
     */
    if inhparent {
        return;
    }

    /*
     * Extract data about relation's FKs from the relcache.  Note that this
     * list belongs to the relcache and might disappear in a cache flush, so
     * we must not do any further catalog access within this function.
     */
    let cachedfkeys = RelationGetFKeyList(relation);

    /*
     * Figure out which FKs are of interest for this query, and create
     * ForeignKeyOptInfos for them.  We want only FKs that reference some
     * other RTE of the current query.  In queries containing self-joins,
     * there might be more than one other RTE for a referenced table, and we
     * should make a ForeignKeyOptInfo for each occurrence.
     *
     * Ideally, we would ignore RTEs that correspond to non-baserels, but it's
     * too hard to identify those here, so we might end up making some useless
     * ForeignKeyOptInfos.  If so, match_foreign_keys_to_quals() will remove
     * them again.
     */
    lc = list_head(cachedfkeys);
    while !lc.is_null() {
        let cachedfk = lfirst(lc) as *mut ForeignKeyCacheInfo;
        lc = lnext(cachedfkeys, lc);

        /* conrelid should always be that of the table we're considering */
        Assert!((*cachedfk).conrelid == RelationGetRelid(relation));

        /* skip constraints currently not enforced */
        if !(*cachedfk).conenforced {
            continue;
        }

        /* Scan to find other RTEs matching confrelid */
        let mut rti: Index = 0;
        let mut lc2 = list_head(rtable);
        while !lc2.is_null() {
            let rte = lfirst(lc2) as *mut RangeTblEntry;
            lc2 = lnext(rtable, lc2);

            rti += 1;
            /* Ignore if not the correct table */
            if (*rte).rtekind != RTE_RELATION || (*rte).relid != (*cachedfk).confrelid {
                continue;
            }
            /* Ignore if it's an inheritance parent; doesn't really match */
            if (*rte).inh {
                continue;
            }
            /* Ignore self-referential FKs; we only care about joins */
            if rti == (*rel).relid {
                continue;
            }

            /* OK, let's make an entry */
            let info: *mut ForeignKeyOptInfo =
                makeNode!(ForeignKeyOptInfo, T_ForeignKeyOptInfo) as *mut ForeignKeyOptInfo;
            (*info).con_relid = (*rel).relid;
            (*info).ref_relid = rti;
            (*info).nkeys = (*cachedfk).nkeys;
            core::ptr::copy_nonoverlapping(
                (*cachedfk).conkey.as_ptr(),
                (*info).conkey.as_mut_ptr(),
                (*info).conkey.len(),
            );
            core::ptr::copy_nonoverlapping(
                (*cachedfk).confkey.as_ptr(),
                (*info).confkey.as_mut_ptr(),
                (*info).confkey.len(),
            );
            core::ptr::copy_nonoverlapping(
                (*cachedfk).conpfeqop.as_ptr(),
                (*info).conpfeqop.as_mut_ptr(),
                (*info).conpfeqop.len(),
            );
            /* zero out fields to be filled by match_foreign_keys_to_quals */
            (*info).nmatched_ec = 0;
            (*info).nconst_ec = 0;
            (*info).nmatched_rcols = 0;
            (*info).nmatched_ri = 0;
            core::ptr::write_bytes(
                (*info).eclass.as_mut_ptr(),
                0,
                (*info).eclass.len(),
            );
            core::ptr::write_bytes(
                (*info).fk_eclass_member.as_mut_ptr(),
                0,
                (*info).fk_eclass_member.len(),
            );
            core::ptr::write_bytes(
                (*info).rinfos.as_mut_ptr(),
                0,
                (*info).rinfos.len(),
            );

            (*root).fkey_list = lappend((*root).fkey_list, info as *mut c_void);
        }
    }
}

/*
 * infer_arbiter_indexes -
 *    Determine the unique indexes used to arbitrate speculative insertion.
 *
 * Uses user-supplied inference clause expressions and predicate to match a
 * unique index from those defined and ready on the heap relation (target).
 * An exact match is required on columns/expressions (although they can appear
 * in any order).  However, the predicate given by the user need only restrict
 * insertion to a subset of some part of the table covered by some particular
 * unique index (in particular, a partial unique index) in order to be
 * inferred.
 *
 * The implementation does not consider which B-Tree operator class any
 * particular available unique index attribute uses, unless one was specified
 * in the inference specification. The same is true of collations.  In
 * particular, there is no system dependency on the default operator class for
 * the purposes of inference.  If no opclass (or collation) is specified, then
 * all matching indexes (that may or may not match the default in terms of
 * each attribute opclass/collation) are used for inference.
 *
 * Note: during index CONCURRENTLY operations, different transactions may
 * reference different sets of arbiter indexes. This can lead to false unique
 * constraint violations that wouldn't occur during normal operations.  For
 * more information, see insert.sgml.
 */
pub unsafe fn infer_arbiter_indexes(root: *mut PlannerInfo) -> *mut List {
    let onconflict: *mut OnConflictExpr = (*(*root).parse).onConflict;

    /* Normalized inference attributes and inference expressions: */
    let mut inferAttrs: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();
    let mut inferElems: *mut List = NIL;

    /* Results */
    let mut results: *mut List = NIL;

    /*
     * Quickly return NIL for ON CONFLICT DO NOTHING without an inference
     * specification or named constraint.  ON CONFLICT DO UPDATE statements
     * must always provide one or the other (but parser ought to have caught
     * that already).
     */
    if (*onconflict).arbiterElems.is_null() && (*onconflict).constraint == InvalidOid() {
        return NIL;
    }

    /*
     * We need not lock the relation since it was already locked, either by
     * the rewriter or when expand_inherited_rtentry() added it to the query's
     * rangetable.
     */
    let varno: Index = (*(*root).parse).resultRelation as Index;
    let rte = rt_fetch(varno, (*(*root).parse).rtable);

    let relation = table_open((*rte).relid, NoLock);

    /*
     * Build normalized/BMS representation of plain indexed attributes, as
     * well as a separate list of expression items.  This simplifies matching
     * the cataloged definition of indexes.
     */
    let mut l = list_head((*onconflict).arbiterElems);
    while !l.is_null() {
        let elem = lfirst(l) as *mut InferenceElem;
        l = lnext((*onconflict).arbiterElems, l);

        if !IsA!((*elem).expr as *const Node, T_Var) {
            /* If not a plain Var, just shove it in inferElems for now */
            inferElems = lappend(inferElems, (*elem).expr as *mut c_void);
            continue;
        }

        let var = (*elem).expr as *mut Var;
        let attno = (*var).varattno;

        if attno == 0 {
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            ereport!(
                crate::utils::elog::ERROR,
                errmsg!("whole row unique index inference specifications are not supported")
            );
        }

        inferAttrs = bms_add_member(
            inferAttrs,
            attno as c_int - FirstLowInvalidHeapAttributeNumber,
        );
    }

    /*
     * Lookup named constraint's index.  This is not immediately returned
     * because some additional sanity checks are required.
     */
    let mut indexOidFromConstraint: Oid = InvalidOid();
    if (*onconflict).constraint != InvalidOid() {
        indexOidFromConstraint = get_constraint_index((*onconflict).constraint);

        if indexOidFromConstraint == InvalidOid() {
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            ereport!(
                crate::utils::elog::ERROR,
                errmsg!("constraint in ON CONFLICT clause has no associated index")
            );
        }
    }

    /*
     * Using that representation, iterate through the list of indexes on the
     * target relation to try and find a match
     */
    let indexList = RelationGetIndexList(relation);

    let mut l = list_head(indexList);
    'index_loop: while !l.is_null() {
        let indexoid: Oid = lfirst_oid(l);
        l = lnext(indexList, l);

        /*
         * Extract info from the relation descriptor for the index.  Obtain
         * the same lock type that the executor will ultimately use.
         *
         * Let executor complain about !indimmediate case directly, because
         * enforcement needs to occur there anyway when an inference clause is
         * omitted.
         */
        let idxRel = index_open(indexoid, (*rte).rellockmode);
        let idxForm: Form_pg_index = (*idxRel).rd_index;

        if !(*idxForm).indisvalid {
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /*
         * Note that we do not perform a check against indcheckxmin (like e.g.
         * get_relation_info()) here to eliminate candidates, because
         * uniqueness checking only cares about the most recently committed
         * tuple versions.
         */

        /*
         * Look for match on "ON constraint_name" variant, which may not be
         * unique constraint.  This can only be a constraint name.
         */
        if indexOidFromConstraint == (*idxForm).indexrelid {
            if (*idxForm).indisexclusion
                && (*onconflict).action == OnConflictAction::ONCONFLICT_UPDATE
            {
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                ereport!(
                    crate::utils::elog::ERROR,
                    errmsg!(
                        "ON CONFLICT DO UPDATE not supported with exclusion constraints"
                    )
                );
            }

            results = lappend_oid(results, (*idxForm).indexrelid);
            list_free(indexList);
            index_close(idxRel, NoLock);
            table_close(relation, NoLock);
            return results;
        } else if indexOidFromConstraint != InvalidOid() {
            /* No point in further work for index in named constraint case */
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /*
         * Only considering conventional inference at this point (not named
         * constraints), so index under consideration can be immediately
         * skipped if it's not unique
         */
        if !(*idxForm).indisunique {
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /*
         * So-called unique constraints with WITHOUT OVERLAPS are really
         * exclusion constraints, so skip those too.
         */
        if (*idxForm).indisexclusion {
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /* Build BMS representation of plain (non expression) index attrs */
        let mut indexedAttrs: *mut crate::nodes::bitmapset::Bitmapset =
            core::ptr::null_mut();
        for natt in 0..(*idxForm).indnkeyatts {
            let attno = (*(*idxRel).rd_index).indkey.values[natt as usize] as c_int;

            if attno != 0 {
                indexedAttrs = bms_add_member(
                    indexedAttrs,
                    attno - FirstLowInvalidHeapAttributeNumber,
                );
            }
        }

        /* Non-expression attributes (if any) must match */
        if !bms_equal(indexedAttrs, inferAttrs) {
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /* Expression attributes (if any) must match */
        let idxExprs = RelationGetIndexExpressions(idxRel);
        if !idxExprs.is_null() && varno != 1 {
            ChangeVarNodes(idxExprs as *mut Node, 1, varno as c_int, 0);
        }

        let mut el = list_head((*onconflict).arbiterElems);
        while !el.is_null() {
            let elem = lfirst(el) as *mut InferenceElem;
            el = lnext((*onconflict).arbiterElems, el);

            /*
             * Ensure that collation/opclass aspects of inference expression
             * element match.  Even though this loop is primarily concerned
             * with matching expressions, it is a convenient point to check
             * this for both expressions and ordinary (non-expression)
             * attributes appearing as inference elements.
             */
            if !infer_collation_opclass_match(elem, idxRel, idxExprs) {
                index_close(idxRel, NoLock);
                continue 'index_loop;
            }

            /*
             * Plain Vars don't factor into count of expression elements, and
             * the question of whether or not they satisfy the index
             * definition has already been considered (they must).
             */
            if IsA!((*elem).expr as *const Node, T_Var) {
                continue;
            }

            /*
             * Might as well avoid redundant check in the rare cases where
             * infer_collation_opclass_match() is required to do real work.
             * Otherwise, check that element expression appears in cataloged
             * index definition.
             */
            if (*elem).infercollid != InvalidOid()
                || (*elem).inferopclass != InvalidOid()
                || list_member(idxExprs, (*elem).expr as *mut c_void)
            {
                continue;
            }

            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /*
         * Now that all inference elements were matched, ensure that the
         * expression elements from inference clause are not missing any
         * cataloged expressions.  This does the right thing when unique
         * indexes redundantly repeat the same attribute, or if attributes
         * redundantly appear multiple times within an inference clause.
         */
        if !list_difference(idxExprs, inferElems).is_null() {
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        /*
         * If it's a partial index, its predicate must be implied by the ON
         * CONFLICT's WHERE clause.
         */
        let predExprs = RelationGetIndexPredicate(idxRel);
        if !predExprs.is_null() && varno != 1 {
            ChangeVarNodes(predExprs as *mut Node, 1, varno as c_int, 0);
        }

        if !predicate_implied_by(predExprs, (*onconflict).arbiterWhere as *mut List, false) {
            index_close(idxRel, NoLock);
            continue 'index_loop;
        }

        results = lappend_oid(results, (*idxForm).indexrelid);
        index_close(idxRel, NoLock);
    }

    list_free(indexList);
    table_close(relation, NoLock);

    if results.is_null() {
        /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
        ereport!(
            crate::utils::elog::ERROR,
            errmsg!("there is no unique or exclusion constraint matching the ON CONFLICT specification")
        );
    }

    results
}

/*
 * infer_collation_opclass_match - ensure infer element opclass/collation match
 *
 * Given unique index inference element from inference specification, if
 * collation was specified, or if opclass was specified, verify that there is
 * at least one matching indexed attribute (occasionally, there may be more).
 * Skip this in the common case where inference specification does not include
 * collation or opclass (instead matching everything, regardless of cataloged
 * collation/opclass of indexed attribute).
 *
 * At least historically, Postgres has not offered collations or opclasses
 * with alternative-to-default notions of equality, so these additional
 * criteria should only be required infrequently.
 *
 * Don't give up immediately when an inference element matches some attribute
 * cataloged as indexed but not matching additional opclass/collation
 * criteria.  This is done so that the implementation is as forgiving as
 * possible of redundancy within cataloged index attributes (or, less
 * usefully, within inference specification elements).  If collations actually
 * differ between apparently redundantly indexed attributes (redundant within
 * or across indexes), then there really is no redundancy as such.
 *
 * Note that if an inference element specifies an opclass and a collation at
 * once, both must match in at least one particular attribute within index
 * catalog definition in order for that inference element to be considered
 * inferred/satisfied.
 */
unsafe fn infer_collation_opclass_match(
    elem: *mut InferenceElem,
    idxRel: Relation,
    idxExprs: *mut List,
) -> bool {
    let mut inferopfamily: Oid = InvalidOid(); /* OID of opclass opfamily */
    let mut inferopcinputtype: Oid = InvalidOid(); /* OID of opclass input type */
    let mut nplain: c_int = 0; /* # plain attrs observed */

    /*
     * If inference specification element lacks collation/opclass, then no
     * need to check for exact match.
     */
    if (*elem).infercollid == InvalidOid() && (*elem).inferopclass == InvalidOid() {
        return true;
    }

    /*
     * Lookup opfamily and input type, for matching indexes
     */
    if (*elem).inferopclass != 0 {
        inferopfamily = get_opclass_family((*elem).inferopclass);
        inferopcinputtype = get_opclass_input_type((*elem).inferopclass);
    }

    let natts = (*(*idxRel).rd_att).natts;
    for natt in 1..=natts {
        let opfamily = *(*idxRel).rd_opfamily.add((natt - 1) as usize);
        let opcinputtype = *(*idxRel).rd_opcintype.add((natt - 1) as usize);
        let collation = *(*idxRel).rd_indcollation.add((natt - 1) as usize);
        let attno = (*(*idxRel).rd_index).indkey.values[(natt - 1) as usize] as c_int;

        if attno != 0 {
            nplain += 1;
        }

        if (*elem).inferopclass != InvalidOid()
            && (inferopfamily != opfamily || inferopcinputtype != opcinputtype)
        {
            /* Attribute needed to match opclass, but didn't */
            continue;
        }

        if (*elem).infercollid != InvalidOid() && (*elem).infercollid != collation {
            /* Attribute needed to match collation, but didn't */
            continue;
        }

        /* If one matching index att found, good enough -- return true */
        if IsA!((*elem).expr as *const Node, T_Var) {
            if (*((*elem).expr as *const Var)).varattno as c_int == attno {
                return true;
            }
        } else if attno == 0 {
            let nattExpr = list_nth(idxExprs, (natt - 1 - nplain) as c_int) as *mut c_void;

            /*
             * Note that unlike routines like match_index_to_operand() we
             * don't need to care about RelabelType.  Neither the index
             * definition nor the inference clause should contain them.
             */
            if equal((*elem).expr as *mut c_void, nattExpr) {
                return true;
            }
        }
    }

    false
}

/*
 * estimate_rel_size - estimate # pages and # tuples in a table or index
 *
 * We also estimate the fraction of the pages that are marked all-visible in
 * the visibility map, for use in estimation of index-only scans.
 *
 * If attr_widths isn't NULL, it points to the zero-index entry of the
 * relation's attr_widths[] cache; we fill this in if we have need to compute
 * the attribute widths for estimation purposes.
 */
pub unsafe fn estimate_rel_size_local(
    rel: Relation,
    attr_widths: *mut i32,
    pages: *mut BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) {
    if RELKIND_HAS_TABLE_AM((*(*rel).rd_rel).relkind) {
        table_relation_estimate_size(rel, attr_widths, pages, tuples, allvisfrac);
    } else if (*(*rel).rd_rel).relkind == RELKIND_INDEX {
        /*
         * XXX: It'd probably be good to move this into a callback, individual
         * index types e.g. know if they have a metapage.
         */

        /* it has storage, ok to call the smgr */
        let mut curpages = RelationGetNumberOfBlocks(rel);

        /* report estimated # pages */
        *pages = curpages;
        /* quick exit if rel is clearly empty */
        if curpages == 0 {
            *tuples = 0.0;
            *allvisfrac = 0.0;
            return;
        }

        /* coerce values in pg_class to more desirable types */
        let mut relpages = (*(*rel).rd_rel).relpages as BlockNumber;
        let reltuples = (*(*rel).rd_rel).reltuples as f64;
        let relallvisible = (*(*rel).rd_rel).relallvisible as BlockNumber;

        /*
         * Discount the metapage while estimating the number of tuples. This
         * is a kluge because it assumes more than it ought to about index
         * structure.  Currently it's OK for btree, hash, and GIN indexes but
         * suspect for GiST indexes.
         */
        if relpages > 0 {
            curpages -= 1;
            relpages -= 1;
        }

        /* estimate number of tuples from previous tuple density */
        let density: f64;
        if reltuples >= 0.0 && relpages > 0 {
            density = reltuples / relpages as f64;
        } else {
            /*
             * If we have no data because the relation was never vacuumed,
             * estimate tuple width from attribute datatypes.  We assume here
             * that the pages are completely full, which is OK for tables
             * (since they've presumably not been VACUUMed yet) but is
             * probably an overestimate for indexes.  Fortunately
             * get_relation_info() can clamp the overestimate to the parent
             * table's size.
             *
             * Note: this code intentionally disregards alignment
             * considerations, because (a) that would be gilding the lily
             * considering how crude the estimate is, and (b) it creates
             * platform dependencies in the default plans which are kind of a
             * headache for regression testing.
             *
             * XXX: Should this logic be more index specific?
             */
            let tuple_width = get_rel_data_width(rel, attr_widths) as usize;
            let tuple_width =
                tuple_width + MAXALIGN(SizeofHeapTupleHeader) + sizeof_itemiddata();
            /* note: integer division is intentional here */
            density = ((BLCKSZ - SizeOfPageHeaderData) / tuple_width) as f64;
        }
        *tuples = (density * curpages as f64).round();

        /*
         * We use relallvisible as-is, rather than scaling it up like we do
         * for the pages and tuples counts, on the theory that any pages added
         * since the last VACUUM are most likely not marked all-visible.  But
         * costsize.c wants it converted to a fraction.
         */
        if relallvisible == 0 || curpages == 0 {
            *allvisfrac = 0.0;
        } else if relallvisible as f64 >= curpages as f64 {
            *allvisfrac = 1.0;
        } else {
            *allvisfrac = relallvisible as f64 / curpages as f64;
        }
    } else {
        /*
         * Just use whatever's in pg_class.  This covers foreign tables,
         * sequences, and also relkinds without storage (shouldn't get here?);
         * see initializations in AddNewRelationTuple().  Note that FDW must
         * cope if reltuples is -1!
         */
        *pages = (*(*rel).rd_rel).relpages as BlockNumber;
        *tuples = (*(*rel).rd_rel).reltuples as f64;
        *allvisfrac = 0.0;
    }
}

/*
 * get_rel_data_width
 *
 * Estimate the average width of (the data part of) the relation's tuples.
 *
 * If attr_widths isn't NULL, it points to the zero-index entry of the
 * relation's attr_widths[] cache; use and update that cache as appropriate.
 *
 * Currently we ignore dropped columns.  Ideally those should be included
 * in the result, but we haven't got any way to get info about them; and
 * since they might be mostly NULLs, treating them as zero-width is not
 * necessarily the wrong thing anyway.
 */
pub unsafe fn get_rel_data_width(rel: Relation, attr_widths: *mut i32) -> i32 {
    let mut tuple_width: i64 = 0;

    let nattrs = RelationGetNumberOfAttributes(rel);
    for i in 1..=nattrs {
        let att = TupleDescAttr((*rel).rd_att, i - 1);

        if (*att).attisdropped {
            continue;
        }

        /* use previously cached data, if any */
        if !attr_widths.is_null() && *attr_widths.add(i as usize) > 0 {
            tuple_width += *attr_widths.add(i as usize) as i64;
            continue;
        }

        /* This should match set_rel_width() in costsize.c */
        let mut item_width = get_attavgwidth(RelationGetRelid(rel), i);
        if item_width <= 0 {
            item_width = get_typavgwidth((*att).atttypid, (*att).atttypmod);
            Assert!(item_width > 0);
        }
        if !attr_widths.is_null() {
            *attr_widths.add(i as usize) = item_width;
        }
        tuple_width += item_width as i64;
    }

    clamp_width_est(tuple_width)
}

/*
 * get_relation_data_width
 *
 * External API for get_rel_data_width: same behavior except we have to
 * open the relcache entry.
 */
pub unsafe fn get_relation_data_width(relid: Oid, attr_widths: *mut i32) -> i32 {
    /* As above, assume relation is already locked */
    let relation = table_open(relid, NoLock);

    let result = get_rel_data_width(relation, attr_widths);

    table_close(relation, NoLock);

    result
}

/*
 * get_relation_constraints
 *
 * Retrieve the applicable constraint expressions of the given relation.
 * Only constraints that have been validated are considered.
 *
 * Returns a List (possibly empty) of constraint expressions.  Each one
 * has been canonicalized, and its Vars are changed to have the varno
 * indicated by rel->relid.  This allows the expressions to be easily
 * compared to expressions taken from WHERE.
 *
 * If include_noinherit is true, it's okay to include constraints that
 * are marked NO INHERIT.
 *
 * If include_notnull is true, "col IS NOT NULL" expressions are generated
 * and added to the result for each column that's marked attnotnull.
 *
 * If include_partition is true, and the relation is a partition,
 * also include the partitioning constraints.
 *
 * Note: at present this is invoked at most once per relation per planner
 * run, and in many cases it won't be invoked at all, so there seems no
 * point in caching the data in RelOptInfo.
 */
unsafe fn get_relation_constraints(
    root: *mut PlannerInfo,
    relationObjectId: Oid,
    rel: *mut RelOptInfo,
    include_noinherit: bool,
    include_notnull: bool,
    include_partition: bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let varno: Index = (*rel).relid;

    /*
     * We assume the relation has already been safely locked.
     */
    let relation = table_open(relationObjectId, NoLock);

    let constr = (*(*relation).rd_att).constr;
    if !constr.is_null() {
        let num_check = (*constr).num_check as c_int;

        for i in 0..num_check {
            /*
             * If this constraint hasn't been fully validated yet, we must
             * ignore it here.
             */
            if !(*(*constr).check.add(i as usize)).ccvalid {
                continue;
            }

            /*
             * NOT ENFORCED constraints are always marked as invalid, which
             * should have been ignored.
             */
            Assert!((*(*constr).check.add(i as usize)).ccenforced);

            /*
             * Also ignore if NO INHERIT and we weren't told that that's safe.
             */
            if (*(*constr).check.add(i as usize)).ccnoinherit && !include_noinherit {
                continue;
            }

            let mut cexpr =
                stringToNode((*(*constr).check.add(i as usize)).ccbin) as *mut Node;

            /*
             * Run each expression through const-simplification and
             * canonicalization.  This is not just an optimization, but is
             * necessary, because we will be comparing it to
             * similarly-processed qual clauses, and may fail to detect valid
             * matches without this.  This must match the processing done to
             * qual clauses in preprocess_expression()!  (We can skip the
             * stuff involving subqueries, however, since we don't allow any
             * in check constraints.)
             */
            cexpr = eval_const_expressions(root, cexpr);

            cexpr = canonicalize_qual(cexpr as *mut Expr, true) as *mut Node;

            /* Fix Vars to have the desired varno */
            if varno != 1 {
                ChangeVarNodes(cexpr, 1, varno as c_int, 0);
            }

            /*
             * Finally, convert to implicit-AND format (that is, a List) and
             * append the resulting item(s) to our output list.
             */
            result = list_concat(result, make_ands_implicit(cexpr as *mut Expr));
        }

        /* Add NOT NULL constraints in expression form, if requested */
        if include_notnull && (*constr).has_not_null {
            let natts = (*(*relation).rd_att).natts;

            for i in 1..=natts {
                let att = TupleDescCompactAttr((*relation).rd_att, i - 1);

                if (*att).attnullability == ATTNULLABLE_VALID && !(*att).attisdropped {
                    let wholeatt = TupleDescAttr((*relation).rd_att, i - 1);
                    let ntest: *mut NullTest =
                        makeNode!(NullTest, T_NullTest) as *mut NullTest;

                    (*ntest).arg = makeVar(
                        varno,
                        i as AttrNumber,
                        (*wholeatt).atttypid,
                        (*wholeatt).atttypmod,
                        (*wholeatt).attcollation,
                        0,
                    ) as *mut Expr;
                    (*ntest).nulltesttype = NullTestType::IS_NOT_NULL;

                    /*
                     * argisrow=false is correct even for a composite column,
                     * because attnotnull does not represent a SQL-spec IS NOT
                     * NULL test in such a case, just IS DISTINCT FROM NULL.
                     */
                    (*ntest).argisrow = false;
                    (*ntest).location = -1;
                    result = lappend(result, ntest as *mut c_void);
                }
            }
        }
    }

    /*
     * Add partitioning constraints, if requested.
     */
    if include_partition && (*(*relation).rd_rel).relispartition {
        /* make sure rel->partition_qual is set */
        set_baserel_partition_constraint(relation, rel);
        result = list_concat(result, (*rel).partition_qual);
    }

    /*
     * Expand virtual generated columns in the constraint expressions.
     */
    if !result.is_null() {
        result = expand_generated_columns_in_expr(result as *mut Node, relation, varno)
            as *mut List;
    }

    table_close(relation, NoLock);

    result
}

/*
 * get_relation_statistics_worker
 *      Helper for get_relation_statistics: process a single statistics object
 *      for a given stxdinherit flag value.
 */
unsafe fn get_relation_statistics_worker(
    stainfos: *mut *mut List,
    rel: *mut RelOptInfo,
    statOid: Oid,
    inh: bool,
    keys: *mut crate::nodes::bitmapset::Bitmapset,
    exprs: *mut List,
) {
    let dtup = SearchSysCache2(
        STATEXTDATASTXOID,
        ObjectIdGetDatum(statOid),
        BoolGetDatum(inh),
    );
    if !HeapTupleIsValid(dtup) {
        return;
    }

    let dataForm = GETSTRUCT(dtup) as Form_pg_statistic_ext_data;

    /* add one StatisticExtInfo for each kind built */
    if statext_is_kind_built(dtup, STATS_EXT_NDISTINCT) {
        let info: *mut StatisticExtInfo =
            makeNode!(StatisticExtInfo, T_StatisticExtInfo) as *mut StatisticExtInfo;
        (*info).statOid = statOid;
        (*info).inherit = (*dataForm).stxdinherit;
        (*info).rel = rel;
        (*info).kind = STATS_EXT_NDISTINCT as i8;
        (*info).keys = bms_copy(keys);
        (*info).exprs = exprs;
        *stainfos = lappend(*stainfos, info as *mut c_void);
    }

    if statext_is_kind_built(dtup, STATS_EXT_DEPENDENCIES) {
        let info: *mut StatisticExtInfo =
            makeNode!(StatisticExtInfo, T_StatisticExtInfo) as *mut StatisticExtInfo;
        (*info).statOid = statOid;
        (*info).inherit = (*dataForm).stxdinherit;
        (*info).rel = rel;
        (*info).kind = STATS_EXT_DEPENDENCIES as i8;
        (*info).keys = bms_copy(keys);
        (*info).exprs = exprs;
        *stainfos = lappend(*stainfos, info as *mut c_void);
    }

    if statext_is_kind_built(dtup, STATS_EXT_MCV) {
        let info: *mut StatisticExtInfo =
            makeNode!(StatisticExtInfo, T_StatisticExtInfo) as *mut StatisticExtInfo;
        (*info).statOid = statOid;
        (*info).inherit = (*dataForm).stxdinherit;
        (*info).rel = rel;
        (*info).kind = STATS_EXT_MCV as i8;
        (*info).keys = bms_copy(keys);
        (*info).exprs = exprs;
        *stainfos = lappend(*stainfos, info as *mut c_void);
    }

    if statext_is_kind_built(dtup, STATS_EXT_EXPRESSIONS) {
        let info: *mut StatisticExtInfo =
            makeNode!(StatisticExtInfo, T_StatisticExtInfo) as *mut StatisticExtInfo;
        (*info).statOid = statOid;
        (*info).inherit = (*dataForm).stxdinherit;
        (*info).rel = rel;
        (*info).kind = STATS_EXT_EXPRESSIONS as i8;
        (*info).keys = bms_copy(keys);
        (*info).exprs = exprs;
        *stainfos = lappend(*stainfos, info as *mut c_void);
    }

    ReleaseSysCache(dtup);
}

/*
 * get_relation_statistics
 *      Retrieve extended statistics defined on the table.
 *
 * Returns a List (possibly empty) of StatisticExtInfo objects describing
 * the statistics.  Note that this doesn't load the actual statistics data,
 * just the identifying metadata.  Only stats actually built are considered.
 */
unsafe fn get_relation_statistics(rel: *mut RelOptInfo, relation: Relation) -> *mut List {
    let varno: Index = (*rel).relid;
    let statoidlist: *mut List;
    let mut stainfos: *mut List = NIL;

    statoidlist = RelationGetStatExtList(relation);

    let mut lc = list_head(statoidlist);
    while !lc.is_null() {
        let statOid: Oid = lfirst_oid(lc);
        let mut keys: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();
        let mut exprs: *mut List = NIL;

        let htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
        if !HeapTupleIsValid(htup) {
            ereport!(
                ERROR,
                /* C also: elog(ERROR, "cache lookup failed for statistics object %u", statOid) */
                errmsg!("cache lookup failed for statistics object {}", statOid)
            );
        }
        let staForm = GETSTRUCT(htup) as Form_pg_statistic_ext;

        /*
         * First, build the array of columns covered.  This is ultimately
         * wasted if no stats within the object have actually been built, but
         * it doesn't seem worth troubling over that case.
         */
        let stxkeys = &(*(staForm as *const PgStatisticExtFixed)).stxkeys;
        for i in 0..stxkeys.dim1 {
            keys = bms_add_member(keys, *stxkeys.values.as_ptr().add(i as usize) as c_int);
        }

        /*
         * Preprocess expressions (if any). We read the expressions, run them
         * through eval_const_expressions, and fix the varnos.
         *
         * XXX We don't know yet if there are any data for this stats object,
         * with either stxdinherit value. But it's reasonable to assume there
         * is at least one of those, possibly both. So it's better to process
         * keys and expressions here.
         */
        {
            let mut isnull: bool = false;
            let datum = SysCacheGetAttr(
                STATEXTOID,
                htup,
                Anum_pg_statistic_ext_stxexprs,
                &mut isnull,
            );

            if !isnull {
                let exprsString = TextDatumGetCString(datum);
                exprs = stringToNode(exprsString) as *mut List;
                pfree(exprsString as *mut c_void);

                /*
                 * Run the expressions through eval_const_expressions. This is
                 * not just an optimization, but is necessary, because the
                 * planner will be comparing them to similarly-processed qual
                 * clauses, and may fail to detect valid matches without this.
                 * We must not use canonicalize_qual, however, since these
                 * aren't qual expressions.
                 */
                exprs = eval_const_expressions(core::ptr::null_mut(), exprs as *mut Node)
                    as *mut List;

                /* May as well fix opfuncids too */
                fix_opfuncids(exprs as *mut Node);

                /*
                 * Modify the copies we obtain from the relcache to have the
                 * correct varno for the parent relation, so that they match
                 * up correctly against qual clauses.
                 */
                if varno != 1 {
                    ChangeVarNodes(exprs as *mut Node, 1, varno as c_int, 0);
                }
            }
        }

        /* extract statistics for possible values of stxdinherit flag */
        get_relation_statistics_worker(&mut stainfos, rel, statOid, true, keys, exprs);
        get_relation_statistics_worker(&mut stainfos, rel, statOid, false, keys, exprs);

        ReleaseSysCache(htup);
        bms_free(keys);

        lc = lnext(statoidlist, lc);
    }

    list_free(statoidlist);

    stainfos
}

/*
 * relation_excluded_by_constraints
 *
 * Detect whether the relation need not be scanned because it has either
 * self-inconsistent restrictions, or restrictions inconsistent with the
 * relation's applicable constraints.
 *
 * Note: this examines only rel->relid, rel->reloptkind, and
 * rel->baserestrictinfo; therefore it can be called before filling in
 * other fields of the RelOptInfo.
 */
pub unsafe fn relation_excluded_by_constraints(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rte: *mut RangeTblEntry,
) -> bool {
    let include_noinherit: bool;
    let include_notnull: bool;
    let mut include_partition = false;
    let mut safe_restrictions: *mut List = NIL;
    let constraint_pred: *mut List;
    let mut safe_constraints: *mut List = NIL;

    /* As of now, constraint exclusion works only with simple relations. */
    Assert!(IS_SIMPLE_REL(rel));

    /*
     * If there are no base restriction clauses, we have no hope of proving
     * anything below, so fall out quickly.
     */
    if (*rel).baserestrictinfo == NIL {
        return false;
    }

    /*
     * Regardless of the setting of constraint_exclusion, detect
     * constant-FALSE-or-NULL restriction clauses.  Although const-folding
     * will reduce "anything AND FALSE" to just "FALSE", the baserestrictinfo
     * list can still have other members besides the FALSE constant, due to
     * qual pushdown and other mechanisms; so check them all.  This doesn't
     * fire very often, but it seems cheap enough to be worth doing anyway.
     * (Without this, we'd miss some optimizations that 9.5 and earlier found
     * via much more roundabout methods.)
     */
    {
        let mut lc = list_head((*rel).baserestrictinfo);
        while !lc.is_null() {
            let rinfo = lfirst(lc) as *mut RestrictInfo;
            let clause: *mut Expr = (*rinfo).clause;

            if !clause.is_null()
                && IsA!(clause, T_Const)
                && ((*(clause as *mut Const)).constisnull
                    || !DatumGetBool((*(clause as *mut Const)).constvalue as u64))
            {
                return true;
            }

            lc = lnext((*rel).baserestrictinfo, lc);
        }
    }

    /*
     * Skip further tests, depending on constraint_exclusion.
     */
    match constraint_exclusion {
        CONSTRAINT_EXCLUSION_OFF => {
            /* In 'off' mode, never make any further tests */
            return false;
        }
        CONSTRAINT_EXCLUSION_PARTITION => {
            /*
             * When constraint_exclusion is set to 'partition' we only handle
             * appendrel members.  Partition pruning has already been applied,
             * so there is no need to consider the rel's partition constraints
             * here.
             */
            use crate::nodes::pathnodes::RelOptKind::RELOPT_OTHER_MEMBER_REL;
            if (*rel).reloptkind == RELOPT_OTHER_MEMBER_REL {
                /* appendrel member, so process it */
            } else {
                return false;
            }
        }
        CONSTRAINT_EXCLUSION_ON => {
            /*
             * In 'on' mode, always apply constraint exclusion.  If we are
             * considering a baserel that is a partition (i.e., it was
             * directly named rather than expanded from a parent table), then
             * its partition constraints haven't been considered yet, so
             * include them in the processing here.
             */
            use crate::nodes::pathnodes::RelOptKind::RELOPT_BASEREL as RELOPT_BASEREL_ENUM;
            if (*rel).reloptkind == RELOPT_BASEREL_ENUM {
                include_partition = true;
            }
            /* always try to exclude */
        }
        _ => {}
    }

    /*
     * Check for self-contradictory restriction clauses.  We dare not make
     * deductions with non-immutable functions, but any immutable clauses that
     * are self-contradictory allow us to conclude the scan is unnecessary.
     *
     * Note: strip off RestrictInfo because predicate_refuted_by() isn't
     * expecting to see any in its predicate argument.
     */
    {
        let mut lc = list_head((*rel).baserestrictinfo);
        while !lc.is_null() {
            let rinfo = lfirst(lc) as *mut RestrictInfo;
            if !contain_mutable_functions((*rinfo).clause as *mut Node) {
                safe_restrictions = lappend(safe_restrictions, (*rinfo).clause as *mut c_void);
            }
            lc = lnext((*rel).baserestrictinfo, lc);
        }
    }

    /*
     * We can use weak refutation here, since we're comparing restriction
     * clauses with restriction clauses.
     */
    if predicate_refuted_by(safe_restrictions, safe_restrictions, true) {
        return true;
    }

    /*
     * Only plain relations have constraints, so stop here for other rtekinds.
     */
    if (*rte).rtekind != RTE_RELATION {
        return false;
    }

    /*
     * If we are scanning just this table, we can use NO INHERIT constraints,
     * but not if we're scanning its children too.  (Note that partitioned
     * tables should never have NO INHERIT constraints; but it's not necessary
     * for us to assume that here.)
     */
    include_noinherit = !(*rte).inh;

    /*
     * Currently, attnotnull constraints must be treated as NO INHERIT unless
     * this is a partitioned table.  In future we might track their
     * inheritance status more accurately, allowing this to be refined.
     *
     * XXX do we need/want to change this?
     */
    include_notnull = !(*rte).inh || (*rte).relkind == RELKIND_PARTITIONED_TABLE;

    /*
     * Fetch the appropriate set of constraint expressions.
     */
    constraint_pred = get_relation_constraints(
        root,
        (*rte).relid,
        rel,
        include_noinherit,
        include_notnull,
        include_partition,
    );

    /*
     * We do not currently enforce that CHECK constraints contain only
     * immutable functions, so it's necessary to check here. We daren't draw
     * conclusions from plan-time evaluation of non-immutable functions. Since
     * they're ANDed, we can just ignore any mutable constraints in the list,
     * and reason about the rest.
     */
    {
        let mut lc = list_head(constraint_pred);
        while !lc.is_null() {
            let pred = lfirst(lc) as *mut Node;
            if !contain_mutable_functions(pred) {
                safe_constraints = lappend(safe_constraints, pred as *mut c_void);
            }
            lc = lnext(constraint_pred, lc);
        }
    }

    /*
     * The constraints are effectively ANDed together, so we can just try to
     * refute the entire collection at once.  This may allow us to make proofs
     * that would fail if we took them individually.
     *
     * Note: we use rel->baserestrictinfo, not safe_restrictions as might seem
     * an obvious optimization.  Some of the clauses might be OR clauses that
     * have volatile and nonvolatile subclauses, and it's OK to make
     * deductions with the nonvolatile parts.
     *
     * We need strong refutation because we have to prove that the constraints
     * would yield false, not just NULL.
     */
    if predicate_refuted_by(safe_constraints, (*rel).baserestrictinfo, false) {
        return true;
    }

    false
}


/*
 * build_physical_tlist
 *
 * Build a targetlist consisting of exactly the relation's user attributes,
 * in order.  The executor can special-case such tlists to avoid a projection
 * step at runtime, so we use such tlists preferentially for scan nodes.
 *
 * Exception: if there are any dropped or missing columns, we punt and return
 * NIL.  Ideally we would like to handle these cases too.  However this
 * creates problems for ExecTypeFromTL, which may be asked to build a tupdesc
 * for a tlist that includes vars of no-longer-existent types.  In theory we
 * could dig out the required info from the pg_attribute entries of the
 * relation, but that data is not readily available to ExecTypeFromTL.
 * For now, we don't apply the physical-tlist optimization when there are
 * dropped cols.
 *
 * We also support building a "physical" tlist for subqueries, functions,
 * values lists, table expressions, and CTEs, since the same optimization can
 * occur in SubqueryScan, FunctionScan, ValuesScan, CteScan, TableFunc,
 * NamedTuplestoreScan, and WorkTableScan nodes.
 */
pub unsafe fn build_physical_tlist(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> *mut List {
    let mut tlist: *mut List = NIL;
    let varno: Index = (*rel).relid;
    let rte: *mut RangeTblEntry = planner_rt_fetch(varno, root);
    let mut colvars: *mut List = NIL;

    match (*rte).rtekind {
        RTE_RELATION => {
            /* Assume we already have adequate lock */
            let relation = table_open((*rte).relid, NoLock);
            let numattrs = RelationGetNumberOfAttributes(relation);

            for attrno in 1..=numattrs {
                let att_tup = TupleDescAttr((*relation).rd_att, attrno - 1);

                if (*att_tup).attisdropped || (*att_tup).atthasmissing {
                    /* found a dropped or missing col, so punt */
                    tlist = NIL;
                    break;
                }

                let var = makeVar(
                    varno,
                    attrno as AttrNumber,
                    (*att_tup).atttypid,
                    (*att_tup).atttypmod,
                    (*att_tup).attcollation,
                    0,
                );

                tlist = lappend(
                    tlist,
                    makeTargetEntry(
                        var as *mut Expr,
                        attrno as i16,
                        core::ptr::null_mut(),
                        false,
                    ) as *mut c_void,
                );
            }

            table_close(relation, NoLock);
        }
        RTE_SUBQUERY => {
            let subquery: *mut Query = (*rte).subquery as *mut Query;
            let mut l = list_head((*subquery).targetList);
            while !l.is_null() {
                let tle = lfirst(l) as *mut TargetEntry;

                /*
                 * A resjunk column of the subquery can be reflected as
                 * resjunk in the physical tlist; we need not punt.
                 */
                let var = makeVarFromTargetEntry(varno, tle);

                tlist = lappend(
                    tlist,
                    makeTargetEntry(
                        var as *mut Expr,
                        (*tle).resno,
                        core::ptr::null_mut(),
                        (*tle).resjunk,
                    ) as *mut c_void,
                );

                l = lnext((*subquery).targetList, l);
            }
        }
        RTE_FUNCTION | RTE_TABLEFUNC | RTE_VALUES | RTE_CTE | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
            /* Not all of these can have dropped cols, but share code anyway */
            expandRTE(
                rte,
                varno,
                0,
                VAR_RETURNING_DEFAULT,
                -1,
                true, /* include dropped */
                core::ptr::null_mut(),
                &mut colvars,
            );
            let mut l = list_head(colvars);
            while !l.is_null() {
                let var = lfirst(l) as *mut Var;

                /*
                 * A non-Var in expandRTE's output means a dropped column;
                 * must punt.
                 */
                if !IsA!(var as *mut Node, T_Var) {
                    tlist = NIL;
                    break;
                }

                tlist = lappend(
                    tlist,
                    makeTargetEntry(
                        var as *mut Expr,
                        (*var).varattno,
                        core::ptr::null_mut(),
                        false,
                    ) as *mut c_void,
                );

                l = lnext(colvars, l);
            }
        }
        _ => {
            /* caller error */
            ereport!(
                ERROR,
                /* C also: elog(ERROR, "unsupported RTE kind %d in build_physical_tlist", (int) rte->rtekind) */
                errmsg!("unsupported RTE kind {} in build_physical_tlist", (*rte).rtekind as c_int)
            );
        }
    }

    tlist
}

/*
 * build_index_tlist
 *
 * Build a targetlist representing the columns of the specified index.
 * Each column is represented by a Var for the corresponding base-relation
 * column, or an expression in base-relation Vars, as appropriate.
 *
 * There are never any dropped columns in indexes, so unlike
 * build_physical_tlist, we need no failure case.
 */
unsafe fn build_index_tlist(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    heapRelation: Relation,
) -> *mut List {
    let mut tlist: *mut List = NIL;
    let varno: Index = (*(*index).rel).relid;
    let mut indexpr_item = list_head((*index).indexprs);

    for i in 0..(*index).ncolumns {
        let indexkey = *(*index).indexkeys.add(i as usize);
        let indexvar: *mut Expr;

        if indexkey != 0 {
            /* simple column */
            let att_tup: *const FormData_pg_attribute;
            if indexkey < 0 {
                att_tup = SystemAttributeDefinition(indexkey as i16);
            } else {
                att_tup = TupleDescAttr((*heapRelation).rd_att, indexkey - 1);
            }

            indexvar = makeVar(
                varno,
                indexkey as AttrNumber,
                (*att_tup).atttypid,
                (*att_tup).atttypmod,
                (*att_tup).attcollation,
                0,
            ) as *mut Expr;
        } else {
            /* expression column */
            if indexpr_item.is_null() {
                ereport!(
                    ERROR,
                    /* C also: elog(ERROR, "wrong number of index expressions") */
                    errmsg!("wrong number of index expressions")
                );
            }
            indexvar = lfirst(indexpr_item) as *mut Expr;
            indexpr_item = lnext((*index).indexprs, indexpr_item);
        }

        tlist = lappend(
            tlist,
            makeTargetEntry(indexvar, (i + 1) as i16, core::ptr::null_mut(), false)
                as *mut c_void,
        );
    }
    if !indexpr_item.is_null() {
        ereport!(
            ERROR,
            /* C also: elog(ERROR, "wrong number of index expressions") */
            errmsg!("wrong number of index expressions")
        );
    }

    tlist
}

/*
 * restriction_selectivity
 *
 * Returns the selectivity of a specified restriction operator clause.
 * This code executes registered procedures stored in the
 * operator relation, by calling the function manager.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 */
#[no_mangle]
pub unsafe fn restriction_selectivity(
    root: *mut PlannerInfo,
    operatorid: Oid,
    args: *mut List,
    inputcollid: Oid,
    varRelid: c_int,
) -> Selectivity {
    if std::env::var_os("PDB_RX").is_some() { eprintln!("PDB_RX restriction_selectivity opid={}", operatorid); }
    let oprrest: RegProcedure = get_oprrest(operatorid);
    if std::env::var_os("PDB_RX").is_some() { eprintln!("PDB_RX got oprrest={}", oprrest); }

    /*
     * if the oprrest procedure is missing for whatever reason, use a
     * selectivity of 0.5
     */
    if !OidIsValid(oprrest) {
        return 0.5;
    }

    let result = DatumGetFloat8(OidFunctionCall4Coll(
        oprrest,
        inputcollid,
        PointerGetDatum(root as *const c_void),
        ObjectIdGetDatum(operatorid),
        PointerGetDatum(args as *const c_void),
        Int32GetDatum(varRelid),
    ));

    if result < 0.0 || result > 1.0 {
        ereport!(
            ERROR,
            /* C also: elog(ERROR, "invalid restriction selectivity: %f", result) */
            errmsg!("invalid restriction selectivity: {}", result)
        );
    }

    result as Selectivity
}

/*
 * join_selectivity
 *
 * Returns the selectivity of a specified join operator clause.
 * This code executes registered procedures stored in the
 * operator relation, by calling the function manager.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 */
#[no_mangle]
pub unsafe fn join_selectivity(
    root: *mut PlannerInfo,
    operatorid: Oid,
    args: *mut List,
    inputcollid: Oid,
    jointype: crate::nodes::nodes::JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    let oprjoin: RegProcedure = get_oprjoin(operatorid);

    /*
     * if the oprjoin procedure is missing for whatever reason, use a
     * selectivity of 0.5
     */
    if !OidIsValid(oprjoin) {
        return 0.5;
    }

    let result = DatumGetFloat8(OidFunctionCall5Coll(
        oprjoin,
        inputcollid,
        PointerGetDatum(root as *const c_void),
        ObjectIdGetDatum(operatorid),
        PointerGetDatum(args as *const c_void),
        Int16GetDatum(jointype as i16),
        PointerGetDatum(sjinfo as *const c_void),
    ));

    if result < 0.0 || result > 1.0 {
        ereport!(
            ERROR,
            /* C also: elog(ERROR, "invalid join selectivity: %f", result) */
            errmsg!("invalid join selectivity: {}", result)
        );
    }

    result as Selectivity
}

/*
 * function_selectivity
 *
 * Returns the selectivity of a specified boolean function clause.
 * This code executes registered procedures stored in the
 * pg_proc relation, by calling the function manager.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 */
pub unsafe fn function_selectivity(
    root: *mut PlannerInfo,
    funcid: Oid,
    args: *mut List,
    inputcollid: Oid,
    is_join: bool,
    varRelid: c_int,
    jointype: crate::nodes::nodes::JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    let prosupport: RegProcedure = get_func_support(funcid);

    /*
     * If no support function is provided, use our historical default
     * estimate, 0.3333333.  This seems a pretty unprincipled choice, but
     * Postgres has been using that estimate for function calls since 1992.
     * The hoariness of this behavior suggests that we should not be in too
     * much hurry to use another value.
     */
    if !OidIsValid(prosupport) {
        return 0.3333333;
    }

    let mut req = SupportRequestSelectivity {
        type_: T_SupportRequestSelectivity,
        root,
        funcid,
        args,
        inputcollid,
        is_join,
        varRelid,
        jointype,
        sjinfo,
        selectivity: -1.0, /* to catch failure to set the value */
    };

    let sresult = DatumGetPointer(OidFunctionCall1(
        prosupport,
        PointerGetDatum(&req as *const SupportRequestSelectivity as *const c_void),
    )) as *mut SupportRequestSelectivity;

    /* If support function fails, use default */
    if sresult != &mut req as *mut SupportRequestSelectivity {
        return 0.3333333;
    }

    if req.selectivity < 0.0 || req.selectivity > 1.0 {
        ereport!(
            ERROR,
            /* C also: elog(ERROR, "invalid function selectivity: %f", req.selectivity) */
            errmsg!("invalid function selectivity: {}", req.selectivity)
        );
    }

    req.selectivity as Selectivity
}

/*
 * add_function_cost
 *
 * Get an estimate of the execution cost of a function, and *add* it to
 * the contents of *cost.  The estimate may include both one-time and
 * per-tuple components, since QualCost does.
 *
 * The funcid must always be supplied.  If it is being called as the
 * implementation of a specific parsetree node (FuncExpr, OpExpr,
 * WindowFunc, etc), pass that as "node", else pass NULL.
 *
 * In some usages root might be NULL, too.
 */
pub unsafe fn add_function_cost(
    root: *mut PlannerInfo,
    funcid: Oid,
    node: *mut Node,
    cost: *mut QualCost,
) {
    let proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        ereport!(
            ERROR,
            /* C also: elog(ERROR, "cache lookup failed for function %u", funcid) */
            errmsg!("cache lookup failed for function {}", funcid)
        );
    }
    let procform = GETSTRUCT(proctup) as Form_pg_proc;

    if std::env::var_os("PDB_RX").is_some() { eprintln!("PDB_RX add_function_cost funcid={} prosupport={}", funcid, (*procform).prosupport); }
    if OidIsValid((*procform).prosupport) {
        let mut req = SupportRequestCost {
            type_: T_SupportRequestCost,
            root,
            funcid,
            node,
            startup: 0.0,
            per_tuple: 0.0,
        };

        let sresult = DatumGetPointer(OidFunctionCall1(
            (*procform).prosupport,
            PointerGetDatum(&req as *const SupportRequestCost as *const c_void),
        )) as *mut SupportRequestCost;

        if sresult == &mut req as *mut SupportRequestCost {
            /* Success, so accumulate support function's estimate into *cost */
            (*cost).startup += req.startup;
            (*cost).per_tuple += req.per_tuple;
            ReleaseSysCache(proctup);
            return;
        }
    }

    if std::env::var_os("PDB_RX").is_some() { eprintln!("PDB_RX add_function_cost fallback-to-procost"); }
    /* No support function, or it failed, so rely on procost */
    (*cost).per_tuple += (*procform).procost as f64 * cpu_operator_cost;

    ReleaseSysCache(proctup);
}

/*
 * get_function_rows
 *
 * Get an estimate of the number of rows returned by a set-returning function.
 *
 * The funcid must always be supplied.  In current usage, the calling node
 * will always be supplied, and will be either a FuncExpr or OpExpr.
 * But it's a good idea to not fail if it's NULL.
 *
 * In some usages root might be NULL, too.
 *
 * Note: this returns the unfiltered result of the support function, if any.
 * It's usually a good idea to apply clamp_row_est() to the result, but we
 * leave it to the caller to do so.
 */
pub unsafe fn get_function_rows(root: *mut PlannerInfo, funcid: Oid, node: *mut Node) -> f64 {
    let proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        ereport!(
            ERROR,
            /* C also: elog(ERROR, "cache lookup failed for function %u", funcid) */
            errmsg!("cache lookup failed for function {}", funcid)
        );
    }
    let procform = GETSTRUCT(proctup) as Form_pg_proc;

    Assert!((*procform).proretset); /* else caller error */

    if OidIsValid((*procform).prosupport) {
        let mut req = SupportRequestRows {
            type_: T_SupportRequestRows,
            root,
            funcid,
            node,
            rows: 0.0, /* just for sanity */
        };

        let sresult = DatumGetPointer(OidFunctionCall1(
            (*procform).prosupport,
            PointerGetDatum(&req as *const SupportRequestRows as *const c_void),
        )) as *mut SupportRequestRows;

        if sresult == &mut req as *mut SupportRequestRows {
            /* Success */
            ReleaseSysCache(proctup);
            return req.rows;
        }
    }

    /* No support function, or it failed, so rely on prorows */
    let result = (*procform).prorows as f64;

    ReleaseSysCache(proctup);

    result
}

/*
 * has_unique_index
 *
 * Detect whether there is a unique index on the specified attribute
 * of the specified relation, thus allowing us to conclude that all
 * the (non-null) values of the attribute are distinct.
 *
 * This function does not check the index's indimmediate property, which
 * means that uniqueness may transiently fail to hold intra-transaction.
 * That's appropriate when we are making statistical estimates, but beware
 * of using this for any correctness proofs.
 */
pub unsafe fn has_unique_index(rel: *mut RelOptInfo, attno: AttrNumber) -> bool {
    let mut ilist = list_head((*rel).indexlist);
    while !ilist.is_null() {
        let index = lfirst(ilist) as *mut IndexOptInfo;

        /*
         * Note: ignore partial indexes, since they don't allow us to conclude
         * that all attr values are distinct, *unless* they are marked predOK
         * which means we know the index's predicate is satisfied by the
         * query. We don't take any interest in expressional indexes either.
         * Also, a multicolumn unique index doesn't allow us to conclude that
         * just the specified attr is unique.
         */
        if (*index).unique
            && (*index).nkeycolumns == 1
            && *(*index).indexkeys == attno as c_int
            && ((*index).indpred == NIL || (*index).predOK)
        {
            return true;
        }

        ilist = lnext((*rel).indexlist, ilist);
    }
    false
}


/*
 * has_row_triggers
 *
 * Detect whether the specified relation has any row-level triggers for event.
 */
pub unsafe fn has_row_triggers(root: *mut PlannerInfo, rti: Index, event: CmdType) -> bool {
    let rte: *mut RangeTblEntry = planner_rt_fetch(rti, root);
    let mut result = false;

    /* Assume we already have adequate lock */
    let relation = table_open((*rte).relid, NoLock);
    let trigDesc: *mut TriggerDesc = (*relation).trigdesc as *mut TriggerDesc;

    match event {
        CmdType::CMD_INSERT => {
            if !trigDesc.is_null()
                && ((*trigDesc).trig_insert_after_row || (*trigDesc).trig_insert_before_row)
            {
                result = true;
            }
        }
        CmdType::CMD_UPDATE => {
            if !trigDesc.is_null()
                && ((*trigDesc).trig_update_after_row || (*trigDesc).trig_update_before_row)
            {
                result = true;
            }
        }
        CmdType::CMD_DELETE => {
            if !trigDesc.is_null()
                && ((*trigDesc).trig_delete_after_row || (*trigDesc).trig_delete_before_row)
            {
                result = true;
            }
        }
        /* There is no separate event for MERGE, only INSERT/UPDATE/DELETE */
        CmdType::CMD_MERGE => {
            result = false;
        }
        _ => {
            ereport!(
                ERROR,
                /* C also: elog(ERROR, "unrecognized CmdType: %d", (int) event) */
                errmsg!("unrecognized CmdType: {}", event as c_int)
            );
        }
    }

    table_close(relation, NoLock);
    result
}

/*
 * has_transition_tables
 *
 * Detect whether the specified relation has any transition tables for event.
 */
pub unsafe fn has_transition_tables(root: *mut PlannerInfo, rti: Index, event: CmdType) -> bool {
    let rte: *mut RangeTblEntry = planner_rt_fetch(rti, root);
    let mut result = false;

    Assert!((*rte).rtekind == RTE_RELATION);

    /* Currently foreign tables cannot have transition tables */
    if (*rte).relkind == RELKIND_FOREIGN_TABLE {
        return result;
    }

    /* Assume we already have adequate lock */
    let relation = table_open((*rte).relid, NoLock);
    let trigDesc: *mut TriggerDesc = (*relation).trigdesc as *mut TriggerDesc;

    match event {
        CmdType::CMD_INSERT => {
            if !trigDesc.is_null() && (*trigDesc).trig_insert_new_table {
                result = true;
            }
        }
        CmdType::CMD_UPDATE => {
            if !trigDesc.is_null()
                && ((*trigDesc).trig_update_old_table || (*trigDesc).trig_update_new_table)
            {
                result = true;
            }
        }
        CmdType::CMD_DELETE => {
            if !trigDesc.is_null() && (*trigDesc).trig_delete_old_table {
                result = true;
            }
        }
        /* There is no separate event for MERGE, only INSERT/UPDATE/DELETE */
        CmdType::CMD_MERGE => {
            result = false;
        }
        _ => {
            ereport!(
                ERROR,
                /* C also: elog(ERROR, "unrecognized CmdType: %d", (int) event) */
                errmsg!("unrecognized CmdType: {}", event as c_int)
            );
        }
    }

    table_close(relation, NoLock);
    result
}

/*
 * has_stored_generated_columns
 *
 * Does table identified by RTI have any STORED GENERATED columns?
 */
pub unsafe fn has_stored_generated_columns(root: *mut PlannerInfo, rti: Index) -> bool {
    let rte: *mut RangeTblEntry = planner_rt_fetch(rti, root);

    /* Assume we already have adequate lock */
    let relation = table_open((*rte).relid, NoLock);
    let tupdesc = RelationGetDescr(relation);
    let result = !(*tupdesc).constr.is_null()
        && (*(*tupdesc).constr).has_generated_stored;

    table_close(relation, NoLock);

    result
}

/*
 * get_dependent_generated_columns
 *
 * Get the column numbers of any STORED GENERATED columns of the relation
 * that depend on any column listed in target_cols.  Both the input and
 * result bitmapsets contain column numbers offset by
 * FirstLowInvalidHeapAttributeNumber.
 */
pub unsafe fn get_dependent_generated_columns(
    root: *mut PlannerInfo,
    rti: Index,
    target_cols: *mut crate::nodes::bitmapset::Bitmapset,
) -> *mut crate::nodes::bitmapset::Bitmapset {
    let mut dependentCols: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();
    let rte: *mut RangeTblEntry = planner_rt_fetch(rti, root);

    /* Assume we already have adequate lock */
    let relation = table_open((*rte).relid, NoLock);
    let tupdesc = RelationGetDescr(relation);
    let constr = (*tupdesc).constr;

    if !constr.is_null() && (*constr).has_generated_stored {
        for i in 0..(*constr).num_defval {
            let defval: *mut AttrDefault = (*constr).defval.add(i as usize);
            let mut attrs_used: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();

            /* skip if not generated column */
            if (*TupleDescAttr(tupdesc, (*defval).adnum as c_int - 1)).attgenerated == 0 {
                continue;
            }

            /* identify columns this generated column depends on */
            let expr = stringToNode((*defval).adbin) as *mut Node;
            pull_varattnos(expr, 1, &mut attrs_used);

            if bms_overlap(target_cols, attrs_used) {
                dependentCols = bms_add_member(
                    dependentCols,
                    (*defval).adnum as c_int - FirstLowInvalidHeapAttributeNumber,
                );
            }
        }
    }

    table_close(relation, NoLock);

    dependentCols
}

/*
 * set_relation_partition_info
 *
 * Set partitioning scheme and related information for a partitioned table.
 */
unsafe fn set_relation_partition_info(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    relation: Relation,
) {
    /*
     * Create the PartitionDirectory infrastructure if we didn't already.
     */
    if (*(*root).glob).partition_directory.is_null() {
        (*(*root).glob).partition_directory =
            CreatePartitionDirectory(CurrentMemoryContext(), true);
    }

    let partdesc = PartitionDirectoryLookup((*(*root).glob).partition_directory, relation);
    (*rel).part_scheme = find_partition_scheme(root, relation)
        as crate::nodes::pathnodes::PartitionScheme;
    Assert!(!partdesc.is_null() && !(*rel).part_scheme.is_null());
    (*rel).boundinfo = (*partdesc).boundinfo as *mut crate::nodes::pathnodes::PartitionBoundInfoData;
    (*rel).nparts = (*partdesc).nparts;
    set_baserel_partition_key_exprs(relation, rel);
    set_baserel_partition_constraint(relation, rel);
}

/*
 * find_partition_scheme
 *
 * Find or create a PartitionScheme for this Relation.
 */
unsafe fn find_partition_scheme(root: *mut PlannerInfo, relation: Relation) -> PartitionScheme {
    let partkey: PartitionKey = RelationGetPartitionKey(relation);

    /* A partitioned table should have a partition key. */
    Assert!(!partkey.is_null());

    let partnatts = (*partkey).partnatts as usize;

    /* Search for a matching partition scheme and return if found one. */
    let mut lc = list_head((*root).part_schemes);
    while !lc.is_null() {
        let part_scheme: PartitionScheme = lfirst(lc) as PartitionScheme;

        /* Match partitioning strategy and number of keys. */
        if (*partkey).strategy as c_char != (*part_scheme).strategy
            || (*partkey).partnatts != (*part_scheme).partnatts
        {
            lc = lnext((*root).part_schemes, lc);
            continue;
        }

        /* Match partition key type properties. */
        let opfam_match = core::slice::from_raw_parts((*partkey).partopfamily, partnatts)
            == core::slice::from_raw_parts((*part_scheme).partopfamily, partnatts);
        let opcintype_match = core::slice::from_raw_parts((*partkey).partopcintype, partnatts)
            == core::slice::from_raw_parts((*part_scheme).partopcintype, partnatts);
        let collation_match = core::slice::from_raw_parts((*partkey).partcollation, partnatts)
            == core::slice::from_raw_parts((*part_scheme).partcollation, partnatts);

        if !opfam_match || !opcintype_match || !collation_match {
            lc = lnext((*root).part_schemes, lc);
            continue;
        }

        /*
         * Length and byval information should match when partopcintype
         * matches.
         */
        Assert!(
            core::slice::from_raw_parts((*partkey).parttyplen, partnatts)
                == core::slice::from_raw_parts((*part_scheme).parttyplen, partnatts)
        );
        Assert!(
            core::slice::from_raw_parts((*partkey).parttypbyval, partnatts)
                == core::slice::from_raw_parts((*part_scheme).parttypbyval, partnatts)
        );

        /*
         * If partopfamily and partopcintype matched, must have the same
         * partition comparison functions.  Note that we cannot reliably
         * Assert the equality of function structs themselves for they might
         * be different across PartitionKey's, so just Assert for the function
         * OIDs.
         */
        #[cfg(debug_assertions)]
        {
            for i in 0..partnatts {
                Assert!(
                    (*(*partkey).partsupfunc.add(i)).fn_oid
                        == (*(*part_scheme).partsupfunc.add(i)).fn_oid
                );
            }
        }

        /* Found matching partition scheme. */
        return part_scheme;
    }

    /*
     * Did not find matching partition scheme. Create one copying relevant
     * information from the relcache. We need to copy the contents of the
     * array since the relcache entry may not survive after we have closed the
     * relation.
     */
    let part_scheme: PartitionScheme =
        palloc0(core::mem::size_of::<PartitionSchemeData>()) as PartitionScheme;
    (*part_scheme).strategy = (*partkey).strategy as c_char;
    (*part_scheme).partnatts = (*partkey).partnatts;

    (*part_scheme).partopfamily =
        palloc(core::mem::size_of::<Oid>() * partnatts) as *mut Oid;
    core::ptr::copy_nonoverlapping((*partkey).partopfamily, (*part_scheme).partopfamily, partnatts);

    (*part_scheme).partopcintype =
        palloc(core::mem::size_of::<Oid>() * partnatts) as *mut Oid;
    core::ptr::copy_nonoverlapping(
        (*partkey).partopcintype,
        (*part_scheme).partopcintype,
        partnatts,
    );

    (*part_scheme).partcollation =
        palloc(core::mem::size_of::<Oid>() * partnatts) as *mut Oid;
    core::ptr::copy_nonoverlapping(
        (*partkey).partcollation,
        (*part_scheme).partcollation,
        partnatts,
    );

    (*part_scheme).parttyplen =
        palloc(core::mem::size_of::<i16>() * partnatts) as *mut i16;
    core::ptr::copy_nonoverlapping((*partkey).parttyplen, (*part_scheme).parttyplen, partnatts);

    (*part_scheme).parttypbyval =
        palloc(core::mem::size_of::<bool>() * partnatts) as *mut bool;
    core::ptr::copy_nonoverlapping(
        (*partkey).parttypbyval,
        (*part_scheme).parttypbyval,
        partnatts,
    );

    (*part_scheme).partsupfunc =
        palloc(core::mem::size_of::<FmgrInfo>() * partnatts) as *mut FmgrInfo;
    for i in 0..partnatts {
        fmgr_info_copy(
            (*part_scheme).partsupfunc.add(i),
            (*partkey).partsupfunc.add(i),
            CurrentMemoryContext(),
        );
    }

    /* Add the partitioning scheme to PlannerInfo. */
    (*root).part_schemes = lappend((*root).part_schemes, part_scheme as *mut c_void);

    part_scheme
}

/*
 * set_baserel_partition_key_exprs
 *
 * Builds partition key expressions for the given base relation and fills
 * rel->partexprs.
 */
unsafe fn set_baserel_partition_key_exprs(relation: Relation, rel: *mut RelOptInfo) {
    let partkey: PartitionKey = RelationGetPartitionKey(relation);

    Assert!(IS_SIMPLE_REL(rel) && (*rel).relid > 0);

    /* A partitioned table should have a partition key. */
    Assert!(!partkey.is_null());

    let partnatts = (*partkey).partnatts as usize;
    let partexprs: *mut *mut List =
        palloc(core::mem::size_of::<*mut List>() * partnatts) as *mut *mut List;
    let mut lc = list_head((*partkey).partexprs);
    let varno: Index = (*rel).relid;

    for cnt in 0..partnatts {
        let partexpr: *mut Expr;
        let attno: AttrNumber = *(*partkey).partattrs.add(cnt);

        if attno != InvalidAttrNumber() {
            /* Single column partition key is stored as a Var node. */
            Assert!(attno > 0);

            partexpr = makeVar(
                varno,
                attno,
                *(*partkey).parttypid.add(cnt),
                *(*partkey).parttypmod.add(cnt),
                *(*partkey).parttypcoll.add(cnt),
                0,
            ) as *mut Expr;
        } else {
            if lc.is_null() {
                ereport!(
                    ERROR,
                    /* C also: elog(ERROR, "wrong number of partition key expressions") */
                    errmsg!("wrong number of partition key expressions")
                );
            }

            /* Re-stamp the expression with given varno. */
            partexpr = copyObject(lfirst(lc) as *mut c_void) as *mut Expr;
            ChangeVarNodes(partexpr as *mut Node, 1, varno as c_int, 0);
            lc = lnext((*partkey).partexprs, lc);
        }

        /* Base relations have a single expression per key. */
        *partexprs.add(cnt) = list_make1!(partexpr as *mut c_void);
    }

    (*rel).partexprs = partexprs;

    /*
     * A base relation does not have nullable partition key expressions, since
     * no outer join is involved.  We still allocate an array of empty
     * expression lists to keep partition key expression handling code simple.
     * See build_joinrel_partition_info() and match_expr_to_partition_keys().
     */
    (*rel).nullable_partexprs =
        palloc0(core::mem::size_of::<*mut List>() * partnatts) as *mut *mut List;
}

/*
 * set_baserel_partition_constraint
 *
 * Builds the partition constraint for the given base relation and sets it
 * in the given RelOptInfo.  All Var nodes are restamped with the relid of the
 * given relation.
 */
unsafe fn set_baserel_partition_constraint(relation: Relation, rel: *mut RelOptInfo) {
    if !(*rel).partition_qual.is_null() {
        /* already done */
        return;
    }

    /*
     * Run the partition quals through const-simplification similar to check
     * constraints.  We skip canonicalize_qual, though, because partition
     * quals should be in canonical form already; also, since the qual is in
     * implicit-AND format, we'd have to explicitly convert it to explicit-AND
     * format and back again.
     */
    let mut partconstr: *mut List = RelationGetPartitionQual(relation);
    if !partconstr.is_null() {
        partconstr = expression_planner(partconstr as *mut Expr) as *mut List;
        if (*rel).relid != 1 {
            ChangeVarNodes(partconstr as *mut Node, 1, (*rel).relid as c_int, 0);
        }
        (*rel).partition_qual = partconstr;
    }
}
