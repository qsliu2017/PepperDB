//! catalog/partition.c - partitioning related data structures and functions.

use crate::prelude::*;

use std::ffi::c_void;

use crate::access::attnum::AttrNumber;
use crate::access::common::attmap::{build_attrmap_by_name, AttrMap};
use crate::access::common::heaptuple::heap_freetuple;
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::relscan::SysScanDescData;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::{InheritsRelationId, PartitionedRelationId};
use crate::catalog::indexing::CatalogTupleUpdate;
use crate::catalog::pg_class::{Form_pg_class, RELKIND_PARTITIONED_TABLE};
use crate::catalog::pg_inherits::Form_pg_inherits;
use crate::catalog::pg_partitioned_table::Form_pg_partitioned_table;
use crate::nodes::bitmapset::{bms_is_member, bms_overlap, Bitmapset};
use crate::nodes::makefuncs::{make_ands_explicit, make_ands_implicit, makeBoolExpr};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{
    lappend_oid, lfirst, lfirst_oid, list_free, list_head, lnext, List, ListCell, NIL,
};
use crate::nodes::primnodes::{Expr, NOT_EXPR};
use crate::optimizer::optimizer::{canonicalize_qual, eval_const_expressions, pull_varattnos};
use crate::partitioning::partdefs::PartitionKey;
use crate::rewrite::rewriteManip::map_variable_attnos;
use crate::storage::lockdefs::{AccessShareLock, RowExclusiveLock};
use crate::utils::rel::{Relation, RelationGetDescr, RelationGetForm};
use crate::utils::snapshot::SnapshotData;

use crate::{current_cell, foreach, list_make1};

/*
 * Generated catalog constants normally produced from pg_inherits.h /
 * catalog/indexing.h.  These have not been emitted yet, so stub them here with
 * the values from PostgreSQL 18.3.
 */
// TODO(pg-port): replace with generated Anum_pg_inherits_inhrelid.
const Anum_pg_inherits_inhrelid: AttrNumber = 1;
// TODO(pg-port): replace with generated Anum_pg_inherits_inhseqno.
const Anum_pg_inherits_inhseqno: AttrNumber = 3;
// TODO(pg-port): replace with generated InheritsRelidSeqnoIndexId (indexing.h).
const InheritsRelidSeqnoIndexId: Oid = 2680;

/*
 * fmgr OIDs normally produced from utils/fmgroids.h, which is not ported yet.
 * Values match PostgreSQL 18.3.
 */
// TODO(pg-port): replace with generated F_OIDEQ.
const F_OIDEQ: RegProcedure = 184;
// TODO(pg-port): replace with generated F_INT4EQ.
const F_INT4EQ: RegProcedure = 65;

/* TODO(pg-port): access/genam.h - systable scan helpers not ported yet. */
type SysScanDesc = *mut SysScanDescData;

unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut SnapshotData,
    _nkeys: c_int,
    _key: ScanKey,
) -> SysScanDesc {
    unimplemented!()
}

unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!()
}

unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!()
}

/*
 * Syscache helpers (utils/syscache.h) not ported yet.  RELOID / PARTRELID are
 * the syscache id constants.
 */
// TODO(pg-port): replace with generated RELOID syscache id.
const RELOID: c_int = 0;
// TODO(pg-port): replace with generated PARTRELID syscache id.
const PARTRELID: c_int = 0;

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

/* TODO(pg-port): utils/rel.h - RelationGetIndexList not ported yet. */
unsafe fn RelationGetIndexList(_relation: Relation) -> *mut List {
    unimplemented!()
}

/* TODO(pg-port): utils/partcache.h - partition-key accessors not ported yet. */
unsafe fn RelationGetPartitionKey(_rel: Relation) -> PartitionKey {
    unimplemented!()
}
unsafe fn get_partition_natts(_key: PartitionKey) -> c_int {
    unimplemented!()
}
unsafe fn get_partition_exprs(_key: PartitionKey) -> *mut List {
    unimplemented!()
}
unsafe fn get_partition_col_attnum(_key: PartitionKey, _col: c_int) -> AttrNumber {
    unimplemented!()
}

/*
 * get_partition_parent
 *		Obtain direct parent of given relation
 *
 * Returns inheritance parent of a partition by scanning pg_inherits
 *
 * If the partition is in the process of being detached, an error is thrown,
 * unless even_if_detached is passed as true.
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument will have precisely one parent, it should only be called
 * when it is known that the relation is a partition.
 */
pub unsafe fn get_partition_parent(relid: Oid, even_if_detached: bool) -> Oid {
    let catalogRelation: Relation;
    let result: Oid;
    let mut detach_pending: bool = false;

    catalogRelation = table_open(InheritsRelationId, AccessShareLock);

    result = get_partition_parent_worker(catalogRelation, relid, &mut detach_pending);

    if !OidIsValid(result) {
        elog!(ERROR, "could not find tuple for parent of relation {}", relid);
    }

    if detach_pending && !even_if_detached {
        elog!(
            ERROR,
            "relation {} has no parent because it's being detached",
            relid
        );
    }

    table_close(catalogRelation, AccessShareLock);

    result
}

/*
 * get_partition_parent_worker
 *		Scan the pg_inherits relation to return the OID of the parent of the
 *		given relation
 *
 * If the partition is being detached, *detach_pending is set true (but the
 * original parent is still returned.)
 */
unsafe fn get_partition_parent_worker(
    inhRel: Relation,
    relid: Oid,
    detach_pending: *mut bool,
) -> Oid {
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = std::mem::zeroed();
    let mut result: Oid = InvalidOid;
    let tuple: HeapTuple;

    *detach_pending = false;

    ScanKeyInit(
        &mut key[0],
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_inherits_inhseqno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(1),
    );

    scan = systable_beginscan(
        inhRel,
        InheritsRelidSeqnoIndexId,
        true,
        std::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );
    tuple = systable_getnext(scan);
    if HeapTupleIsValid(tuple) {
        let form: Form_pg_inherits = GETSTRUCT(tuple) as Form_pg_inherits;

        /* Let caller know of partition being detached */
        if (*form).inhdetachpending {
            *detach_pending = true;
        }
        result = (*form).inhparent;
    }

    systable_endscan(scan);

    result
}

/*
 * get_partition_ancestors
 *		Obtain ancestors of given relation
 *
 * Returns a list of ancestors of the given relation.  The list is ordered:
 * The first element is the immediate parent and the last one is the topmost
 * parent in the partition hierarchy.
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument and each ancestor will have precisely one parent, it should
 * only be called when it is known that the relation is a partition.
 */
pub unsafe fn get_partition_ancestors(relid: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let inhRel: Relation;

    inhRel = table_open(InheritsRelationId, AccessShareLock);

    get_partition_ancestors_worker(inhRel, relid, &mut result);

    table_close(inhRel, AccessShareLock);

    result
}

/*
 * get_partition_ancestors_worker
 *		recursive worker for get_partition_ancestors
 */
unsafe fn get_partition_ancestors_worker(
    inhRel: Relation,
    relid: Oid,
    ancestors: *mut *mut List,
) {
    let parentOid: Oid;
    let mut detach_pending: bool = false;

    /*
     * Recursion ends at the topmost level, ie., when there's no parent; also
     * when the partition is being detached.
     */
    parentOid = get_partition_parent_worker(inhRel, relid, &mut detach_pending);
    if parentOid == InvalidOid || detach_pending {
        return;
    }

    *ancestors = lappend_oid(*ancestors, parentOid);
    get_partition_ancestors_worker(inhRel, parentOid, ancestors);
}

/*
 * index_get_partition
 *		Return the OID of index of the given partition that is a child
 *		of the given index, or InvalidOid if there isn't one.
 */
pub unsafe fn index_get_partition(partition: Relation, indexId: Oid) -> Oid {
    let idxlist: *mut List = RelationGetIndexList(partition);
    let l: *mut ListCell;

    foreach!(l, idxlist, {
        let partIdx: Oid = lfirst_oid(current_cell!(l));
        let tup: HeapTuple;
        let classForm: Form_pg_class;
        let ispartition: bool;

        tup = SearchSysCache1(RELOID, ObjectIdGetDatum(partIdx));
        if !HeapTupleIsValid(tup) {
            elog!(ERROR, "cache lookup failed for relation {}", partIdx);
        }
        classForm = GETSTRUCT(tup) as Form_pg_class;
        ispartition = (*classForm).relispartition;
        ReleaseSysCache(tup);
        if !ispartition {
            continue;
        }
        if get_partition_parent(partIdx, false) == indexId {
            list_free(idxlist);
            return partIdx;
        }
    });

    list_free(idxlist);
    InvalidOid
}

/*
 * map_partition_varattnos - maps varattnos of all Vars in 'expr' (that have
 * varno 'fromrel_varno') from the attnums of 'from_rel' to the attnums of
 * 'to_rel', each of which may be either a leaf partition or a partitioned
 * table, but both of which must be from the same partitioning hierarchy.
 *
 * We need this because even though all of the same column names must be
 * present in all relations in the hierarchy, and they must also have the
 * same types, the attnums may be different.
 *
 * Note: this will work on any node tree, so really the argument and result
 * should be declared "Node *".  But a substantial majority of the callers
 * are working on Lists, so it's less messy to do the casts internally.
 */
pub unsafe fn map_partition_varattnos(
    mut expr: *mut List,
    fromrel_varno: c_int,
    to_rel: Relation,
    from_rel: Relation,
) -> *mut List {
    if expr != NIL {
        let part_attmap: *mut AttrMap;
        let mut found_whole_row: bool = false;

        part_attmap = build_attrmap_by_name(
            RelationGetDescr(to_rel),
            RelationGetDescr(from_rel),
            false,
        );
        expr = map_variable_attnos(
            expr as *mut Node,
            fromrel_varno,
            0,
            part_attmap as *const _,
            (*RelationGetForm(to_rel)).reltype,
            &mut found_whole_row,
        ) as *mut List;
        /* Since we provided a to_rowtype, we may ignore found_whole_row. */
    }

    expr
}

/*
 * Checks if any of the 'attnums' is a partition key attribute for rel
 *
 * Sets *used_in_expr if any of the 'attnums' is found to be referenced in some
 * partition key expression.  It's possible for a column to be both used
 * directly and as part of an expression; if that happens, *used_in_expr may
 * end up as either true or false.  That's OK for current uses of this
 * function, because *used_in_expr is only used to tailor the error message
 * text.
 */
pub unsafe fn has_partition_attrs(
    rel: Relation,
    attnums: *mut Bitmapset,
    used_in_expr: *mut bool,
) -> bool {
    let key: PartitionKey;
    let partnatts: c_int;
    let partexprs: *mut List;
    let mut partexprs_item: *mut ListCell;
    let mut i: c_int;

    if attnums.is_null() || (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE {
        return false;
    }

    key = RelationGetPartitionKey(rel);
    partnatts = get_partition_natts(key);
    partexprs = get_partition_exprs(key);

    partexprs_item = list_head(partexprs);
    i = 0;
    while i < partnatts {
        let partattno: AttrNumber = get_partition_col_attnum(key, i);

        if partattno != 0 {
            if bms_is_member(
                (partattno - FirstLowInvalidHeapAttributeNumber) as c_int,
                attnums,
            ) {
                if !used_in_expr.is_null() {
                    *used_in_expr = false;
                }
                return true;
            }
        } else {
            /* Arbitrary expression */
            let expr: *mut Node = lfirst(partexprs_item) as *mut Node;
            let mut expr_attrs: *mut Bitmapset = std::ptr::null_mut();

            /* Find all attributes referenced */
            pull_varattnos(expr, 1, &mut expr_attrs);
            partexprs_item = lnext(partexprs, partexprs_item);

            if bms_overlap(attnums, expr_attrs) {
                if !used_in_expr.is_null() {
                    *used_in_expr = true;
                }
                return true;
            }
        }

        i += 1;
    }

    false
}

/*
 * get_default_partition_oid
 *
 * Given a relation OID, return the OID of the default partition, if one
 * exists.  Use get_default_oid_from_partdesc where possible, for
 * efficiency.
 */
pub unsafe fn get_default_partition_oid(parentId: Oid) -> Oid {
    let tuple: HeapTuple;
    let mut defaultPartId: Oid = InvalidOid;

    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(parentId));

    if HeapTupleIsValid(tuple) {
        let part_table_form: Form_pg_partitioned_table;

        part_table_form = GETSTRUCT(tuple) as Form_pg_partitioned_table;
        defaultPartId = (*part_table_form).partdefid;
        ReleaseSysCache(tuple);
    }

    defaultPartId
}

/*
 * update_default_partition_oid
 *
 * Update pg_partitioned_table.partdefid with a new default partition OID.
 */
pub unsafe fn update_default_partition_oid(parentId: Oid, defaultPartId: Oid) {
    let tuple: HeapTuple;
    let pg_partitioned_table: Relation;
    let part_table_form: Form_pg_partitioned_table;

    pg_partitioned_table = table_open(PartitionedRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(parentId));

    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for partition key of relation {}",
            parentId
        );
    }

    part_table_form = GETSTRUCT(tuple) as Form_pg_partitioned_table;
    (*part_table_form).partdefid = defaultPartId;
    CatalogTupleUpdate(pg_partitioned_table, &mut (*tuple).t_self, tuple);

    heap_freetuple(tuple);
    table_close(pg_partitioned_table, RowExclusiveLock);
}

/*
 * get_proposed_default_constraint
 *
 * This function returns the negation of new_part_constraints, which
 * would be an integral part of the default partition constraints after
 * addition of the partition to which the new_part_constraints belongs.
 */
pub unsafe fn get_proposed_default_constraint(new_part_constraints: *mut List) -> *mut List {
    let mut defPartConstraint: *mut Expr;

    defPartConstraint = make_ands_explicit(new_part_constraints);

    /*
     * Derive the partition constraints of default partition by negating the
     * given partition constraints. The partition constraint never evaluates
     * to NULL, so negating it like this is safe.
     */
    defPartConstraint = makeBoolExpr(NOT_EXPR, list_make1!(defPartConstraint), -1);

    /* Simplify, to put the negated expression into canonical form */
    defPartConstraint =
        eval_const_expressions(std::ptr::null_mut(), defPartConstraint as *mut Node) as *mut Expr;
    defPartConstraint = canonicalize_qual(defPartConstraint, true);

    make_ands_implicit(defPartConstraint)
}
