//! utils/cache/partcache.c - Support routines for manipulating partition
//! information cached in relcache.

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::attnum::AttrNumber;
use crate::access::common::relation::{relation_close, relation_open};
use crate::access::common::tupdesc::TupleDescAttr;
use crate::access::hash::hashvalidate::HASHEXTENDED_PROC;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::nbtree::nbtvalidate::BTORDER_PROC;
use crate::c::{int2vector, int32, oidvector, NameStr};
use crate::catalog::partition::{get_partition_parent, map_partition_varattnos};
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_partitioned_table::{Form_pg_partitioned_table, FormData_pg_partitioned_table};
use crate::nodes::makefuncs::makeBoolExpr;
use crate::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod, fix_opfuncids};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{
    list_concat, list_head, list_length, lnext, lfirst, linitial, List, ListCell, NIL,
};
use crate::nodes::primnodes::{Expr, BoolExprType::AND_EXPR};
use crate::nodes::read::stringToNode;
use crate::optimizer::optimizer::{eval_const_expressions, PlannerInfo};
use crate::postgres::{DatumGetPointer, ObjectIdGetDatum};
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::{AccessShareLock, NoLock};
use crate::utils::builtins::{format_type_be, TextDatumGetCString};
use crate::utils::fmgr::{fmgr_info_cxt, FmgrInfo};
use crate::utils::misc::stack_depth::check_stack_depth;
// palloc0/pfree/MemoryContextAllocZero/MemoryContextSwitchTo come from the
// prelude (crate::utils::palloc).  Only the context globals and SetParent are
// pulled from mcxt directly.
use crate::utils::mmgr::mcxt::{
    CacheMemoryContext, CurTransactionContext, MemoryContextSetParent,
};
use crate::utils::rel::{Relation, RelationGetRelationName, RelationGetRelid};

use crate::catalog::pg_class::RELKIND_PARTITIONED_TABLE;
use crate::nodes::parsenodes::PartitionStrategy;
use crate::nodes::parsenodes::PartitionStrategy::{
    PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE,
};

use crate::castNode;

/* ==================================================================== */
/*  Stubs for not-yet-translated subsystems                            */
/* ==================================================================== */

// utils/syscache.h - cache ids (STUB: syscache.c not yet ported).
const PARTRELID: c_int = 45;
const RELOID: c_int = 57;
const CLAOID: c_int = 14;

// catalog/pg_partitioned_table - attribute numbers (STUB: catalog header not
// fully ported).
const Anum_pg_partitioned_table_partclass: c_int = 6;
const Anum_pg_partitioned_table_partcollation: c_int = 7;
const Anum_pg_partitioned_table_partexprs: c_int = 8;

// catalog/pg_class - attribute number (STUB).
const Anum_pg_class_relpartbound: c_int = 34;

// utils/errcodes.h - ERRCODE_INVALID_OBJECT_DEFINITION (STUB: errcodes not yet
// ported; the errcode() shim ignores its argument).
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0;

// utils/syscache.h - SearchSysCache1 (STUB: syscache.c not yet ported).
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!("SearchSysCache1: syscache.c not yet ported")
}

// utils/syscache.h - ReleaseSysCache (STUB).
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!("ReleaseSysCache: syscache.c not yet ported")
}

// utils/syscache.h - SysCacheGetAttr (STUB).
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!("SysCacheGetAttr: syscache.c not yet ported")
}

// utils/syscache.h - SysCacheGetAttrNotNull (STUB).
unsafe fn SysCacheGetAttrNotNull(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
) -> Datum {
    unimplemented!("SysCacheGetAttrNotNull: syscache.c not yet ported")
}

// utils/lsyscache.h - get_opfamily_proc (STUB: lsyscache.c not yet ported).
unsafe fn get_opfamily_proc(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _procnum: int16,
) -> Oid {
    unimplemented!("get_opfamily_proc: lsyscache.c not yet ported")
}

// utils/lsyscache.h - get_typlenbyvalalign (STUB).
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) {
    unimplemented!("get_typlenbyvalalign: lsyscache.c not yet ported")
}

// utils/lsyscache.h - get_rel_relispartition (STUB).
unsafe fn get_rel_relispartition(_relid: Oid) -> bool {
    unimplemented!("get_rel_relispartition: lsyscache.c not yet ported")
}

// partitioning/partbounds.h - get_qual_from_partbound (STUB: partbounds.c not
// yet ported).
unsafe fn get_qual_from_partbound(_parent: Relation, _spec: *mut c_void) -> *mut List { crate::partitioning::partbounds::get_qual_from_partbound(_parent, _spec as _) }

// nodes/copyfuncs.c - copyObject() deep copy (STUB: copyfuncs.c not yet wired
// into the module tree).
unsafe fn copyObject<T>(_from: *const T) -> *mut T {
    unimplemented!("copyObject: copyfuncs.c not yet ported")
}

// utils/mmgr/mcxt.c - MemoryContextCopyAndSetIdentifier (STUB: this helper is
// not yet ported; the identifier is purely diagnostic).
unsafe fn MemoryContextCopyAndSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    // No-op: identifier tracking not yet ported.
}

/*
 * Information about the partition key of a relation (partcache.h).
 *
 * partcache.h declares this struct; partdefs.h carries only the opaque
 * PartitionKey typedef.  We define the concrete struct here, matching the C
 * layout, and treat the relcache's rd_partkey (an opaque void*) as a pointer
 * to it.
 */
#[repr(C)]
pub struct PartitionKeyData {
    pub strategy: PartitionStrategy, /* partitioning strategy */
    pub partnatts: int16,            /* number of columns in the partition key */
    pub partattrs: *mut AttrNumber,  /* attribute numbers of columns, or 0 if expr */
    pub partexprs: *mut List,        /* list of expressions, one per zero partattrs */

    pub partopfamily: *mut Oid,    /* OIDs of operator families */
    pub partopcintype: *mut Oid,   /* OIDs of opclass declared input data types */
    pub partsupfunc: *mut FmgrInfo, /* lookup info for support funcs */

    /* Partitioning collation per attribute */
    pub partcollation: *mut Oid,

    /* Type information per attribute */
    pub parttypid: *mut Oid,
    pub parttypmod: *mut int32,
    pub parttyplen: *mut int16,
    pub parttypbyval: *mut bool,
    pub parttypalign: *mut c_char,
    pub parttypcoll: *mut Oid,
}

pub type PartitionKey = *mut PartitionKeyData;

/*
 * RelationGetPartitionKey -- get partition key, if relation is partitioned
 *
 * Note: partition keys are not allowed to change after the partitioned rel
 * is created.  RelationClearRelation knows this and preserves rd_partkey
 * across relcache rebuilds, as long as the relation is open.  Therefore,
 * even though we hand back a direct pointer into the relcache entry, it's
 * safe for callers to continue to use that pointer as long as they hold
 * the relation open.
 */
pub unsafe fn RelationGetPartitionKey(rel: Relation) -> PartitionKey {
    if (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE {
        return null_mut();
    }

    if (*rel).rd_partkey.is_null() {
        RelationBuildPartitionKey(rel);
    }

    (*rel).rd_partkey as PartitionKey
}

/*
 * RelationBuildPartitionKey
 *		Build partition key data of relation, and attach to relcache
 *
 * Partitioning key data is a complex structure; to avoid complicated logic to
 * free individual elements whenever the relcache entry is flushed, we give it
 * its own memory context, a child of CacheMemoryContext, which can easily be
 * deleted on its own.  To avoid leaking memory in that context in case of an
 * error partway through this function, the context is initially created as a
 * child of CurTransactionContext and only re-parented to CacheMemoryContext
 * at the end, when no further errors are possible.  Also, we don't make this
 * context the current context except in very brief code sections, out of fear
 * that some of our callees allocate memory on their own which would be leaked
 * permanently.
 */
unsafe fn RelationBuildPartitionKey(relation: Relation) {
    let form: Form_pg_partitioned_table;
    let tuple: HeapTuple;
    let mut isnull: bool = false;
    let key: PartitionKey;
    let attrs: *mut AttrNumber;
    let opclass: *mut oidvector;
    let collation: *mut oidvector;
    let mut partexprs_item: *mut ListCell;
    let mut datum: Datum;
    let partkeycxt: MemoryContext;
    let procnum: int16;

    tuple = SearchSysCache1(
        PARTRELID,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for partition key of relation {}",
            RelationGetRelid(relation)
        );
    }

    partkeycxt = AllocSetContextCreate!(
        CurTransactionContext,
        c"partition key".as_ptr(),
        ALLOCSET_SMALL_SIZES
    ) as *mut _;
    MemoryContextCopyAndSetIdentifier(partkeycxt, RelationGetRelationName(relation));

    key = MemoryContextAllocZero(
        partkeycxt,
        core::mem::size_of::<PartitionKeyData>(),
    ) as PartitionKey;

    /* Fixed-length attributes */
    form = GETSTRUCT(tuple) as Form_pg_partitioned_table;
    /*
     * In C, key->strategy (a PartitionStrategy, i.e. a char typedef) is assigned
     * directly from form->partstrat and then validated.  Here PartitionStrategy
     * is a real enum, so validate the raw char first and map only valid codes;
     * the invalid path elog(ERROR)s (panics) before any enum is constructed.
     */
    (*key).strategy = match (*form).partstrat as u8 {
        b'l' => PARTITION_STRATEGY_LIST,
        b'r' => PARTITION_STRATEGY_RANGE,
        b'h' => PARTITION_STRATEGY_HASH,
        _ => {
            elog!(
                ERROR,
                "invalid partition strategy \"{}\"",
                (*form).partstrat as u8 as char
            );
            unreachable!()
        }
    };
    (*key).partnatts = (*form).partnatts;

    /*
     * We can rely on the first variable-length attribute being mapped to the
     * relevant field of the catalog's C struct, because all previous
     * attributes are non-nullable and fixed-length.
     *
     * The int2vector partattrs begins immediately after the fixed part of the
     * struct (right after partdefid), so compute its address explicitly.
     */
    let partattrs_vec = (form as *mut u8)
        .add(core::mem::size_of::<FormData_pg_partitioned_table>())
        as *mut int2vector;
    attrs = (*partattrs_vec).values.as_mut_ptr();

    /* But use the hard way to retrieve further variable-length attributes */
    /* Operator class */
    datum = SysCacheGetAttrNotNull(PARTRELID, tuple, Anum_pg_partitioned_table_partclass);
    opclass = DatumGetPointer(datum) as *mut oidvector;

    /* Collation */
    datum = SysCacheGetAttrNotNull(PARTRELID, tuple, Anum_pg_partitioned_table_partcollation);
    collation = DatumGetPointer(datum) as *mut oidvector;

    /* Expressions */
    datum = SysCacheGetAttr(
        PARTRELID,
        tuple,
        Anum_pg_partitioned_table_partexprs,
        &mut isnull,
    );
    if !isnull {
        let exprString: *mut c_char;
        let mut expr: *mut Node;

        exprString = TextDatumGetCString(datum);
        expr = stringToNode(exprString) as *mut Node;
        pfree(exprString as *mut c_void);

        /*
         * Run the expressions through const-simplification since the planner
         * will be comparing them to similarly-processed qual clause operands,
         * and may fail to detect valid matches without this step; fix
         * opfuncids while at it.  We don't need to bother with
         * canonicalize_qual() though, because partition expressions should be
         * in canonical form already (ie, no need for OR-merging or constant
         * elimination).
         */
        expr = eval_const_expressions(null_mut::<PlannerInfo>(), expr);
        fix_opfuncids(expr);

        let oldcxt2 = MemoryContextSwitchTo(partkeycxt);
        (*key).partexprs = copyObject(expr as *const List) as *mut List;
        MemoryContextSwitchTo(oldcxt2);
    }

    /* Allocate assorted arrays in the partkeycxt, which we'll fill below */
    let oldcxt3 = MemoryContextSwitchTo(partkeycxt);
    let n = (*key).partnatts as usize;
    (*key).partattrs = palloc0(n * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    (*key).partopfamily = palloc0(n * core::mem::size_of::<Oid>()) as *mut Oid;
    (*key).partopcintype = palloc0(n * core::mem::size_of::<Oid>()) as *mut Oid;
    (*key).partsupfunc = palloc0(n * core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;

    (*key).partcollation = palloc0(n * core::mem::size_of::<Oid>()) as *mut Oid;
    (*key).parttypid = palloc0(n * core::mem::size_of::<Oid>()) as *mut Oid;
    (*key).parttypmod = palloc0(n * core::mem::size_of::<int32>()) as *mut int32;
    (*key).parttyplen = palloc0(n * core::mem::size_of::<int16>()) as *mut int16;
    (*key).parttypbyval = palloc0(n * core::mem::size_of::<bool>()) as *mut bool;
    (*key).parttypalign = palloc0(n * core::mem::size_of::<c_char>()) as *mut c_char;
    (*key).parttypcoll = palloc0(n * core::mem::size_of::<Oid>()) as *mut Oid;
    MemoryContextSwitchTo(oldcxt3);

    /* determine support function number to search for */
    procnum = if (*key).strategy == PARTITION_STRATEGY_HASH {
        HASHEXTENDED_PROC
    } else {
        BTORDER_PROC
    };

    /* Copy partattrs and fill other per-attribute info */
    core::ptr::copy_nonoverlapping(
        attrs as *const u8,
        (*key).partattrs as *mut u8,
        n * core::mem::size_of::<int16>(),
    );
    partexprs_item = list_head((*key).partexprs);
    let mut i: c_int = 0;
    while i < (*key).partnatts as c_int {
        let attno: AttrNumber = *(*key).partattrs.add(i as usize);
        let opclasstup: HeapTuple;
        let opclassform: Form_pg_opclass;
        let funcid: Oid;

        /* Collect opfamily information */
        opclasstup = SearchSysCache1(
            CLAOID,
            ObjectIdGetDatum(*(*opclass).values.as_ptr().add(i as usize)),
        );
        if !HeapTupleIsValid(opclasstup) {
            elog!(
                ERROR,
                "cache lookup failed for opclass {}",
                *(*opclass).values.as_ptr().add(i as usize)
            );
        }

        opclassform = GETSTRUCT(opclasstup) as Form_pg_opclass;
        *(*key).partopfamily.add(i as usize) = (*opclassform).opcfamily;
        *(*key).partopcintype.add(i as usize) = (*opclassform).opcintype;

        /* Get a support function for the specified opfamily and datatypes */
        funcid = get_opfamily_proc(
            (*opclassform).opcfamily,
            (*opclassform).opcintype,
            (*opclassform).opcintype,
            procnum,
        );
        if !OidIsValid(funcid) {
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                ERROR,
                errmsg!(
                    "operator class \"{}\" of access method {} is missing support function {} for type {}",
                    cstr_to_string(NameStr(&(*opclassform).opcname)),
                    if (*key).strategy == PARTITION_STRATEGY_HASH {
                        "hash"
                    } else {
                        "btree"
                    },
                    procnum,
                    cstr_to_string(format_type_be((*opclassform).opcintype))
                )
            );
        }

        fmgr_info_cxt(funcid, (*key).partsupfunc.add(i as usize), partkeycxt);

        /* Collation */
        *(*key).partcollation.add(i as usize) =
            *(*collation).values.as_ptr().add(i as usize);

        /* Collect type information */
        if attno != 0 {
            let att = TupleDescAttr((*relation).rd_att, (attno - 1) as c_int);

            *(*key).parttypid.add(i as usize) = (*att).atttypid;
            *(*key).parttypmod.add(i as usize) = (*att).atttypmod;
            *(*key).parttypcoll.add(i as usize) = (*att).attcollation;
        } else {
            if partexprs_item.is_null() {
                elog!(ERROR, "wrong number of partition key expressions");
            }

            *(*key).parttypid.add(i as usize) = exprType(lfirst(partexprs_item) as *const Node);
            *(*key).parttypmod.add(i as usize) =
                exprTypmod(lfirst(partexprs_item) as *const Node);
            *(*key).parttypcoll.add(i as usize) =
                exprCollation(lfirst(partexprs_item) as *const Node);

            partexprs_item = lnext((*key).partexprs, partexprs_item);
        }
        get_typlenbyvalalign(
            *(*key).parttypid.add(i as usize),
            (*key).parttyplen.add(i as usize),
            (*key).parttypbyval.add(i as usize),
            (*key).parttypalign.add(i as usize),
        );

        ReleaseSysCache(opclasstup);

        i += 1;
    }

    ReleaseSysCache(tuple);

    /* Assert that we're not leaking any old data during assignments below */
    Assert!((*relation).rd_partkeycxt.is_null());
    Assert!((*relation).rd_partkey.is_null());

    /*
     * Success --- reparent our context and make the relcache point to the
     * newly constructed key
     */
    MemoryContextSetParent(partkeycxt as *mut _, CacheMemoryContext as *mut _);
    (*relation).rd_partkeycxt = partkeycxt as *mut c_void;
    (*relation).rd_partkey = key as *mut c_void;
}

/*
 * RelationGetPartitionQual
 *
 * Returns a list of partition quals
 */
pub unsafe fn RelationGetPartitionQual(rel: Relation) -> *mut List {
    /* Quick exit */
    if !(*(*rel).rd_rel).relispartition {
        return NIL;
    }

    generate_partition_qual(rel)
}

/*
 * get_partition_qual_relid
 *
 * Returns an expression tree describing the passed-in relation's partition
 * constraint.
 *
 * If the relation is not found, or is not a partition, or there is no
 * partition constraint, return NULL.  We must guard against the first two
 * cases because this supports a SQL function that could be passed any OID.
 * The last case can happen even if relispartition is true, when a default
 * partition is the only partition.
 */
pub unsafe fn get_partition_qual_relid(relid: Oid) -> *mut Expr {
    let mut result: *mut Expr = null_mut();

    /* Do the work only if this relation exists and is a partition. */
    if get_rel_relispartition(relid) {
        let rel: Relation = relation_open(relid, AccessShareLock);
        let and_args: *mut List;

        and_args = generate_partition_qual(rel);

        /* Convert implicit-AND list format to boolean expression */
        if and_args == NIL {
            result = null_mut();
        } else if list_length(and_args) > 1 {
            result = makeBoolExpr(AND_EXPR, and_args, -1);
        } else {
            result = linitial(and_args) as *mut Expr;
        }

        /* Keep the lock, to allow safe deparsing against the rel by caller. */
        relation_close(rel, NoLock);
    }

    result
}

/*
 * generate_partition_qual
 *
 * Generate partition predicate from rel's partition bound expression. The
 * function returns a NIL list if there is no predicate.
 *
 * We cache a copy of the result in the relcache entry, after constructing
 * it using the caller's context.  This approach avoids leaking any data
 * into long-lived cache contexts, especially if we fail partway through.
 */
unsafe fn generate_partition_qual(rel: Relation) -> *mut List {
    let tuple: HeapTuple;
    let oldcxt: MemoryContext;
    let boundDatum: Datum;
    let mut isnull: bool = false;
    let mut my_qual: *mut List = NIL;
    let mut result: *mut List;
    let parentrelid: Oid;
    let parent: Relation;

    /* Guard against stack overflow due to overly deep partition tree */
    check_stack_depth();

    /* If we already cached the result, just return a copy */
    if (*rel).rd_partcheckvalid {
        return copyObject((*rel).rd_partcheck as *const List) as *mut List;
    }

    /*
     * Grab at least an AccessShareLock on the parent table.  Must do this
     * even if the partition has been partially detached, because transactions
     * concurrent with the detach might still be trying to use a partition
     * descriptor that includes it.
     */
    parentrelid = get_partition_parent(RelationGetRelid(rel), true);
    parent = relation_open(parentrelid, AccessShareLock);

    /* Get pg_class.relpartbound */
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for relation {}",
            RelationGetRelid(rel)
        );
    }

    boundDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relpartbound, &mut isnull);
    if !isnull {
        let bound: *mut c_void;

        bound = castNode!(
            c_void,
            T_PartitionBoundSpec,
            stringToNode(TextDatumGetCString(boundDatum))
        );

        my_qual = get_qual_from_partbound(parent, bound);
    }

    ReleaseSysCache(tuple);

    /* Add the parent's quals to the list (if any) */
    if (*(*parent).rd_rel).relispartition {
        result = list_concat(generate_partition_qual(parent), my_qual);
    } else {
        result = my_qual;
    }

    /*
     * Change Vars to have partition's attnos instead of the parent's. We do
     * this after we concatenate the parent's quals, because we want every Var
     * in it to bear this relation's attnos. It's safe to assume varno = 1
     * here.
     */
    result = map_partition_varattnos(result, 1, rel, parent);

    /* Assert that we're not leaking any old data during assignments below */
    Assert!((*rel).rd_partcheckcxt.is_null());
    Assert!((*rel).rd_partcheck == NIL);

    /*
     * Save a copy in the relcache.  The order of these operations is fairly
     * critical to avoid memory leaks and ensure that we don't leave a corrupt
     * relcache entry if we fail partway through copyObject.
     *
     * If, as is definitely possible, the partcheck list is NIL, then we do
     * not need to make a context to hold it.
     */
    if result != NIL {
        (*rel).rd_partcheckcxt = AllocSetContextCreate!(
            CacheMemoryContext,
            c"partition constraint".as_ptr(),
            ALLOCSET_SMALL_SIZES
        ) as *mut c_void;
        MemoryContextCopyAndSetIdentifier(
            (*rel).rd_partcheckcxt as MemoryContext,
            RelationGetRelationName(rel),
        );
        oldcxt = MemoryContextSwitchTo((*rel).rd_partcheckcxt as MemoryContext);
        (*rel).rd_partcheck = copyObject(result as *const List) as *mut List;
        MemoryContextSwitchTo(oldcxt);
    } else {
        (*rel).rd_partcheck = NIL;
    }
    (*rel).rd_partcheckvalid = true;

    /* Keep the parent locked until commit */
    relation_close(parent, NoLock);

    /* Return the working copy to the caller */
    result
}

/*
 * cstr_to_string - render a C string pointer for use as an elog/ereport
 * format argument ({}).
 */
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    core::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

/*
 * PartitionKey inquiry functions (partcache.h, static inline).
 */
#[inline]
pub unsafe fn get_partition_strategy(key: PartitionKey) -> c_int {
    (*key).strategy as c_int
}

#[inline]
pub unsafe fn get_partition_natts(key: PartitionKey) -> c_int {
    (*key).partnatts as c_int
}

#[inline]
pub unsafe fn get_partition_exprs(key: PartitionKey) -> *mut List {
    (*key).partexprs
}

#[inline]
pub unsafe fn get_partition_col_attnum(key: PartitionKey, col: c_int) -> int16 {
    *(*key).partattrs.add(col as usize)
}

#[inline]
pub unsafe fn get_partition_col_typid(key: PartitionKey, col: c_int) -> Oid {
    *(*key).parttypid.add(col as usize)
}

#[inline]
pub unsafe fn get_partition_col_typmod(key: PartitionKey, col: c_int) -> int32 {
    *(*key).parttypmod.add(col as usize)
}

#[inline]
pub unsafe fn get_partition_col_collation(key: PartitionKey, col: c_int) -> Oid {
    *(*key).partcollation.add(col as usize)
}