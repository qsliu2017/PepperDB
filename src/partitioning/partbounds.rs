//! src/backend/partitioning/partbounds.c
//!
//! Support routines for manipulating partition bounds
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!       src/backend/partitioning/partbounds.c

use crate::prelude::*;
use crate::{foreach, current_cell, IsA, castNode, makeNode, linitial_node, lfirst_node};
use crate::{list_make1, list_make2, list_make3, list_make1_oid};

// miscadmin.h -- CHECK_FOR_INTERRUPTS (stubbed per-file, as in sibling files).
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{ /* TODO(pg-port): miscadmin.h */ }};
}
use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use crate::nodes::pg_list::{
    List, ListCell, NIL, lfirst, lfirst_int, lfirst_oid, lappend, lappend_int, list_length,
    list_free, list_head, lnext, llast, llast_int, linitial, list_nth,
};
use crate::nodes::parsenodes::{
    PartitionBoundSpec, PartitionRangeDatum, PartitionRangeDatumKind,
    PartitionRangeDatumKind::*,
    PartitionStrategy,
};
use crate::nodes::pathnodes::{RelOptInfo, PartitionBoundInfoData};
use crate::nodes::primnodes::{
    Expr, Const, Var, NullTest, NullTestType, BoolExpr, BoolExprType, ScalarArrayOpExpr,
    ArrayExpr, FuncExpr, CoercionForm, RelabelType,
};
use crate::nodes::nodes::{Node, NodeTag, JoinType, JoinType::*};
use crate::nodes::bitmapset::{Bitmapset, bms_add_member, bms_copy, bms_is_member, bms_overlap};
use crate::utils::rel::{Relation, RelationGetRelid};
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll};
use crate::utils::cache::partcache::{PartitionKeyData, PartitionKey, RelationGetPartitionKey};
use crate::utils::rel::RelationGetRelationName;
use crate::utils::adt::datum::{datumCopy, datumIsEqual};
use crate::nodes::nodes::JoinType::JOIN_RIGHT;
use crate::nodes::execnodes::{EState, ExprContext, ExprState, TupleTableSlot};
use crate::parser::parse_node::ParseState;
use crate::access::htup_details::HeapTuple;
use crate::access::relscan::TableScanDesc;
use crate::utils::snapshot::Snapshot;
use crate::access::sdir::ForwardScanDirection;
use crate::storage::lockdefs::{NoLock, AccessExclusiveLock, LOCKMODE};
use crate::catalog::pg_class::{
    RELKIND_RELATION, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE,
};
use crate::catalog::partition::{get_proposed_default_constraint, map_partition_varattnos};
use crate::nodes::makefuncs::make_ands_explicit;
use crate::access::table::table::{table_open, table_close};
use crate::utils::cache::lsyscache::get_rel_name;
use crate::executor::executor::{
    CreateExecutorState, FreeExecutorState, ExecCheck, ExecPrepareExpr,
    GetPerTupleExprContext, GetPerTupleMemoryContext, ResetExprContext,
};
use crate::executor::execTuples::ExecDropSingleTupleTableSlot;
use crate::access::table::tableam::table_slot_create;

// ---------------------------------------------------------------------------
// IS_OUTER_JOIN / IS_DUMMY_REL -- C macros (nodes.h / pathnodes.h). Inlined here.
// ---------------------------------------------------------------------------

/* #define IS_OUTER_JOIN(jointype) (((1 << (jointype)) & ... ) != 0) */
#[inline]
unsafe fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    (((1 << (jointype as u32))
        & ((1 << (JOIN_LEFT as u32))
            | (1 << (JOIN_FULL as u32))
            | (1 << (JOIN_RIGHT as u32))
            | (1 << (JOIN_ANTI as u32))))
        != 0)
}

/* #define IS_DUMMY_REL(r) ... is_dummy_rel(r) in pathnodes.h */
#[inline]
unsafe fn IS_DUMMY_REL(rel: *mut RelOptInfo) -> bool {
    crate::nodes::pathnodes::is_dummy_rel(rel)
}

// ---------------------------------------------------------------------------
// partition_bound_has_default / partition_bound_accepts_nulls
// (static inline in partitioning/partbounds.h).
// ---------------------------------------------------------------------------

#[inline]
pub unsafe fn partition_bound_has_default(bi: PartitionBoundInfo) -> bool {
    !bi.is_null() && (*pbi(bi)).default_index != -1
}

#[inline]
pub unsafe fn partition_bound_accepts_nulls(bi: PartitionBoundInfo) -> bool {
    !bi.is_null() && (*pbi(bi)).null_index != -1
}

// ---------------------------------------------------------------------------
// qsort / qsort_arg -- self-contained sorts over raw element arrays, matching
// the C library qsort()/qsort_arg() shapes used at the call sites.
// ---------------------------------------------------------------------------

unsafe fn qsort(
    base: *mut c_void,
    n: usize,
    size: usize,
    cmp: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> c_int>,
) {
    let cmp = cmp.unwrap();
    let mut idx: Vec<usize> = (0..n).collect();
    let bytes = base as *mut u8;
    idx.sort_by(|&a, &b| {
        let pa = bytes.add(a * size) as *const c_void;
        let pb = bytes.add(b * size) as *const c_void;
        cmp(pa, pb).cmp(&0)
    });
    apply_permutation(bytes, n, size, &idx);
}

unsafe fn qsort_arg(
    base: *mut c_void,
    n: usize,
    size: usize,
    cmp: Option<unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void) -> c_int>,
    arg: *mut c_void,
) {
    let cmp = cmp.unwrap();
    let mut idx: Vec<usize> = (0..n).collect();
    let bytes = base as *mut u8;
    idx.sort_by(|&a, &b| {
        let pa = bytes.add(a * size) as *const c_void;
        let pb = bytes.add(b * size) as *const c_void;
        cmp(pa, pb, arg).cmp(&0)
    });
    apply_permutation(bytes, n, size, &idx);
}

/* Reorder the `n` elements of `size` bytes at `bytes` per index permutation. */
unsafe fn apply_permutation(bytes: *mut u8, n: usize, size: usize, idx: &[usize]) {
    let mut tmp: Vec<u8> = vec![0u8; n * size];
    for (dst, &src) in idx.iter().enumerate() {
        ptr::copy_nonoverlapping(
            bytes.add(src * size),
            tmp.as_mut_ptr().add(dst * size),
            size,
        );
    }
    ptr::copy_nonoverlapping(tmp.as_ptr(), bytes, n * size);
}

// ---------------------------------------------------------------------------
// find_all_inheritors -- pg_inherits.h. No canonical Rust home yet; declared
// here matching the C signature so check_default_partition_contents builds.
// ---------------------------------------------------------------------------
unsafe fn find_all_inheritors(
    parentrel_id: Oid,
    lockmode: LOCKMODE,
    numparents: *mut c_int,
) -> *mut List {
    extern "C" {
        fn find_all_inheritors(
            parentrelId: Oid,
            lockmode: LOCKMODE,
            numparents: *mut c_int,
        ) -> *mut List;
    }
    find_all_inheritors(parentrel_id, lockmode, numparents)
}

// ---------------------------------------------------------------------------
// table access scan wrappers -- tableam.h. No canonical Rust home yet; declared
// here matching the C signatures used by check_default_partition_contents.
// ---------------------------------------------------------------------------
unsafe fn table_beginscan(
    rel: Relation,
    snapshot: Snapshot,
    nkeys: c_int,
    key: *mut c_void,
) -> TableScanDesc {
    #[allow(improper_ctypes)]
    extern "C" {
        fn table_beginscan(
            rel: Relation,
            snapshot: Snapshot,
            nkeys: c_int,
            key: *mut c_void,
        ) -> TableScanDesc;
    }
    table_beginscan(rel, snapshot, nkeys, key)
}

unsafe fn table_endscan(scan: TableScanDesc) {
    #[allow(improper_ctypes)]
    extern "C" {
        fn table_endscan(scan: TableScanDesc);
    }
    table_endscan(scan)
}

unsafe fn table_scan_getnextslot(
    scan: TableScanDesc,
    direction: c_int,
    slot: *mut TupleTableSlot,
) -> bool {
    #[allow(improper_ctypes)]
    extern "C" {
        fn table_scan_getnextslot(
            scan: TableScanDesc,
            direction: c_int,
            slot: *mut TupleTableSlot,
        ) -> bool;
    }
    table_scan_getnextslot(scan, direction, slot)
}

// ---------------------------------------------------------------------------
// snapshot manager wrappers -- snapmgr.h. No canonical Rust home yet.
// ---------------------------------------------------------------------------
unsafe fn GetLatestSnapshot() -> Snapshot {
    extern "C" {
        fn GetLatestSnapshot() -> Snapshot;
    }
    GetLatestSnapshot()
}

unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    extern "C" {
        fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot;
    }
    RegisterSnapshot(snapshot)
}

unsafe fn UnregisterSnapshot(snapshot: Snapshot) {
    extern "C" {
        fn UnregisterSnapshot(snapshot: Snapshot);
    }
    UnregisterSnapshot(snapshot)
}

// ---------------------------------------------------------------------------
// Re-export the real PartitionBoundInfo / PartitionKey from their canonical
// locations; partdefs.rs has opaque stubs but this file defines the real layout.
// ---------------------------------------------------------------------------
pub use crate::nodes::pathnodes::PartitionBoundInfoData as PartitionBoundInfoDataReal;
pub type PartitionBoundInfo = *mut PartitionBoundInfoData;

// ---------------------------------------------------------------------------
// Local struct definitions (file-private in C, pub here for Rust module).
// ---------------------------------------------------------------------------

/* One bound of a hash partition */
#[repr(C)]
struct PartitionHashBound {
    modulus: c_int,
    remainder: c_int,
    index: c_int,
}

/* One value coming from some (index'th) list partition */
#[repr(C)]
struct PartitionListValue {
    index: c_int,
    value: Datum,
}

/* One bound of a range partition */
#[repr(C)]
pub struct PartitionRangeBound {
    pub index: c_int,
    pub datums: *mut Datum,                  /* range bound datums */
    pub kind: *mut PartitionRangeDatumKind,  /* the kind of each datum */
    pub lower: bool,                         /* this is the lower (vs upper) bound */
}

/*
 * Mapping from partitions of a joining relation to partitions of a join
 * relation being computed (a.k.a merged partitions)
 */
#[repr(C)]
struct PartitionMap {
    nparts: c_int,              /* number of partitions */
    merged_indexes: *mut c_int, /* indexes of merged partitions */
    merged: *mut bool,          /* flags to indicate whether partitions are
                                 * merged with non-dummy partitions */
    did_remapping: bool,        /* did we re-map partitions? */
    old_indexes: *mut c_int,    /* old indexes of merged partitions if
                                 * did_remapping */
}

/* Macro for comparing two range bounds */
macro_rules! compare_range_bounds {
    ($partnatts:expr, $partsupfunc:expr, $partcollations:expr, $bound1:expr, $bound2:expr) => {
        partition_rbound_cmp(
            $partnatts,
            $partsupfunc,
            $partcollations,
            (*$bound1).datums,
            (*$bound1).kind,
            (*$bound1).lower,
            $bound2,
        )
    };
}

// ---------------------------------------------------------------------------
// Full PartitionBoundInfoData layout -- defined here because partbounds.c owns it.
// pathnodes.rs has an opaque stub; we cast through raw pointers.
// ---------------------------------------------------------------------------

/*
 * PartitionBoundInfo (defined in partitioning/partbounds.h).
 *
 * We define the concrete struct here. The opaque stub in pathnodes.rs is
 * cast-compatible because both are #[repr(C)].
 */
#[repr(C)]
pub struct PartitionBoundInfoFull {
    pub strategy: c_char,
    pub ndatums: c_int,
    pub datums: *mut *mut Datum,
    pub kind: *mut *mut PartitionRangeDatumKind,
    pub nindexes: c_int,
    pub indexes: *mut c_int,
    pub null_index: c_int,
    pub default_index: c_int,
    pub interleaved_parts: *mut Bitmapset,
}

// Convenience: cast a raw PartitionBoundInfo pointer to our full struct.
#[inline]
unsafe fn pbi(p: PartitionBoundInfo) -> *mut PartitionBoundInfoFull {
    p as *mut PartitionBoundInfoFull
}

// ---------------------------------------------------------------------------
// Public API functions
// ---------------------------------------------------------------------------

/*
 * get_qual_from_partbound
 *		Given a parser node for partition bound, return the list of executable
 *		expressions as partition constraint
 */
pub unsafe fn get_qual_from_partbound(
    parent: Relation,
    spec: *mut PartitionBoundSpec,
) -> *mut List {
    let key: PartitionKey = RelationGetPartitionKey(parent);
    let mut my_qual: *mut List = NIL;

    Assert!(!key.is_null());

    match (*key).strategy {
        PartitionStrategy::PARTITION_STRATEGY_HASH => {
            Assert!((*spec).strategy == b'h' as c_char);
            my_qual = get_qual_for_hash(parent, spec);
        }
        PartitionStrategy::PARTITION_STRATEGY_LIST => {
            Assert!((*spec).strategy == b'l' as c_char);
            my_qual = get_qual_for_list(parent, spec);
        }
        PartitionStrategy::PARTITION_STRATEGY_RANGE => {
            Assert!((*spec).strategy == b'r' as c_char);
            my_qual = get_qual_for_range(parent, spec, false);
        }
    }

    my_qual
}

/*
 *	partition_bounds_create
 *		Build a PartitionBoundInfo struct from a list of PartitionBoundSpec
 *		nodes
 *
 * This function creates a PartitionBoundInfo and fills the values of its
 * various members based on the input list.  Importantly, 'datums' array will
 * contain Datum representation of individual bounds (possibly after
 * de-duplication as in case of range bounds), sorted in a canonical order
 * defined by qsort_partition_* functions of respective partitioning methods.
 * 'indexes' array will contain as many elements as there are bounds (specific
 * exceptions to this rule are listed in the function body), which represent
 * the 0-based canonical positions of partitions.
 *
 * Upon return from this function, *mapping is set to an array of
 * list_length(boundspecs) elements, each of which maps the original index of
 * a partition to its canonical index.
 *
 * Note: The objects returned by this function are wholly allocated in the
 * current memory context.
 */
pub unsafe fn partition_bounds_create(
    boundspecs: *mut *mut PartitionBoundSpec,
    nparts: c_int,
    key: PartitionKey,
    mapping: *mut *mut c_int,
) -> PartitionBoundInfo {
    let mut i: c_int;

    Assert!(nparts > 0);

    /*
     * For each partitioning method, we first convert the partition bounds
     * from their parser node representation to the internal representation,
     * along with any additional preprocessing (such as de-duplicating range
     * bounds).  Resulting bound datums are then added to the 'datums' array
     * in PartitionBoundInfo.  For each datum added, an integer indicating the
     * canonical partition index is added to the 'indexes' array.
     *
     * For each bound, we remember its partition's position (0-based) in the
     * original list to later map it to the canonical index.
     */

    /*
     * Initialize mapping array with invalid values, this is filled within
     * each sub-routine below depending on the bound type.
     */
    *mapping = palloc(core::mem::size_of::<c_int>() * nparts as usize) as *mut c_int;
    i = 0;
    while i < nparts {
        *(*mapping).offset(i as isize) = -1;
        i += 1;
    }

    match (*key).strategy {
        PartitionStrategy::PARTITION_STRATEGY_HASH => {
            return create_hash_bounds(boundspecs, nparts, key, mapping);
        }
        PartitionStrategy::PARTITION_STRATEGY_LIST => {
            return create_list_bounds(boundspecs, nparts, key, mapping);
        }
        PartitionStrategy::PARTITION_STRATEGY_RANGE => {
            return create_range_bounds(boundspecs, nparts, key, mapping);
        }
    }
}

/*
 * partition_bounds_equal
 *
 * Are two partition bound collections logically equal?
 *
 * Used in the keep logic of relcache.c (ie, in RelationClearRelation()).
 * This is also useful when b1 and b2 are bound collections of two separate
 * relations, respectively, because PartitionBoundInfo is a canonical
 * representation of partition bounds.
 */
pub unsafe fn partition_bounds_equal(
    partnatts: c_int,
    parttyplen: *mut i16,
    parttypbyval: *mut bool,
    b1: PartitionBoundInfo,
    b2: PartitionBoundInfo,
) -> bool {
    let b1 = pbi(b1);
    let b2 = pbi(b2);
    let mut i: c_int;

    if (*b1).strategy != (*b2).strategy {
        return false;
    }
    if (*b1).ndatums != (*b2).ndatums {
        return false;
    }
    if (*b1).nindexes != (*b2).nindexes {
        return false;
    }
    if (*b1).null_index != (*b2).null_index {
        return false;
    }
    if (*b1).default_index != (*b2).default_index {
        return false;
    }

    /* For all partition strategies, the indexes[] arrays have to match */
    i = 0;
    while i < (*b1).nindexes {
        if *(*b1).indexes.offset(i as isize) != *(*b2).indexes.offset(i as isize) {
            return false;
        }
        i += 1;
    }

    /* Finally, compare the datums[] arrays */
    if (*b1).strategy == b'h' as c_char {
        /*
         * We arrange the partitions in the ascending order of their moduli
         * and remainders.  Also every modulus is factor of next larger
         * modulus.  Therefore we can safely store index of a given partition
         * in indexes array at remainder of that partition.  Also entries at
         * (remainder + N * modulus) positions in indexes array are all same
         * for (modulus, remainder) specification for any partition.  Thus the
         * datums arrays from the given bounds are the same, if and only if
         * their indexes arrays are the same.  So, it suffices to compare the
         * indexes arrays.
         *
         * Nonetheless make sure that the bounds are indeed the same when the
         * indexes match.  Hash partition bound stores modulus and remainder
         * at b1->datums[i][0] and b1->datums[i][1] position respectively.
         */
        #[cfg(any())] // USE_ASSERT_CHECKING equivalent
        {
            i = 0;
            while i < (*b1).ndatums {
                Assert!(
                    *(*(*b1).datums.offset(i as isize)).offset(0) ==
                        *(*(*b2).datums.offset(i as isize)).offset(0) &&
                    *(*(*b1).datums.offset(i as isize)).offset(1) ==
                        *(*(*b2).datums.offset(i as isize)).offset(1)
                );
                i += 1;
            }
        }
    } else {
        i = 0;
        while i < (*b1).ndatums {
            let mut j: c_int = 0;
            while j < partnatts {
                /* For range partitions, the bounds might not be finite. */
                if !(*b1).kind.is_null() {
                    /* The different kinds of bound all differ from each other */
                    if *(*(*b1).kind.offset(i as isize)).offset(j as isize) !=
                        *(*(*b2).kind.offset(i as isize)).offset(j as isize)
                    {
                        return false;
                    }
                    /*
                     * Non-finite bounds are equal without further
                     * examination.
                     */
                    if *(*(*b1).kind.offset(i as isize)).offset(j as isize) !=
                        PARTITION_RANGE_DATUM_VALUE
                    {
                        j += 1;
                        continue;
                    }
                }

                /*
                 * Compare the actual values. Note that it would be both
                 * incorrect and unsafe to invoke the comparison operator
                 * derived from the partitioning specification here.  It would
                 * be incorrect because we want the relcache entry to be
                 * updated for ANY change to the partition bounds, not just
                 * those that the partitioning operator thinks are
                 * significant.  It would be unsafe because we might reach
                 * this code in the context of an aborted transaction, and an
                 * arbitrary partitioning operator might not be safe in that
                 * context.  datumIsEqual() should be simple enough to be
                 * safe.
                 */
                if !datumIsEqual(
                    *(*(*b1).datums.offset(i as isize)).offset(j as isize),
                    *(*(*b2).datums.offset(i as isize)).offset(j as isize),
                    *parttypbyval.offset(j as isize),
                    *parttyplen.offset(j as isize) as c_int,
                ) {
                    return false;
                }
                j += 1;
            }
            i += 1;
        }
    }
    true
}

/*
 * Return a copy of given PartitionBoundInfo structure. The data types of bounds
 * are described by given partition key specification.
 *
 * Note: it's important that this function and its callees not do any catalog
 * access, nor anything else that would result in allocating memory other than
 * the returned data structure.  Since this is called in a long-lived context,
 * that would result in unwanted memory leaks.
 */
pub unsafe fn partition_bounds_copy(
    src: PartitionBoundInfo,
    key: PartitionKey,
) -> PartitionBoundInfo {
    let src = pbi(src);
    let dest: *mut PartitionBoundInfoFull;
    let mut i: c_int;
    let ndatums: c_int;
    let nindexes: c_int;
    let partnatts: c_int;
    let hash_part: bool;
    let natts: c_int;
    let bound_datums: *mut Datum;

    dest = palloc(core::mem::size_of::<PartitionBoundInfoFull>()) as *mut PartitionBoundInfoFull;

    (*dest).strategy = (*src).strategy;
    ndatums = (*src).ndatums;
    (*dest).ndatums = ndatums;
    nindexes = (*src).nindexes;
    (*dest).nindexes = nindexes;
    partnatts = (*key).partnatts as c_int;

    /* List partitioned tables have only a single partition key. */
    Assert!((*key).strategy != PartitionStrategy::PARTITION_STRATEGY_LIST || partnatts == 1);

    (*dest).datums = palloc(core::mem::size_of::<*mut Datum>() * ndatums as usize)
        as *mut *mut Datum;

    if !(*src).kind.is_null() {
        let bound_kinds: *mut PartitionRangeDatumKind;

        /* only RANGE partition should have a non-NULL kind */
        Assert!((*key).strategy == PartitionStrategy::PARTITION_STRATEGY_RANGE);

        (*dest).kind = palloc(ndatums as usize * core::mem::size_of::<*mut PartitionRangeDatumKind>())
            as *mut *mut PartitionRangeDatumKind;

        /*
         * In the loop below, to save from allocating a series of small arrays
         * for storing the PartitionRangeDatumKind, we allocate a single chunk
         * here and use a smaller portion of it for each datum.
         */
        bound_kinds = palloc(ndatums as usize * partnatts as usize *
            core::mem::size_of::<PartitionRangeDatumKind>()) as *mut PartitionRangeDatumKind;

        i = 0;
        while i < ndatums {
            *(*dest).kind.offset(i as isize) = bound_kinds.offset((i * partnatts) as isize);
            ptr::copy_nonoverlapping(
                *(*src).kind.offset(i as isize),
                *(*dest).kind.offset(i as isize),
                partnatts as usize,
            );
            i += 1;
        }
    } else {
        (*dest).kind = ptr::null_mut();
    }

    /* copy interleaved partitions for LIST partitioned tables */
    (*dest).interleaved_parts = bms_copy((*src).interleaved_parts);

    /*
     * For hash partitioning, datums array will have two elements - modulus
     * and remainder.
     */
    hash_part = (*key).strategy == PartitionStrategy::PARTITION_STRATEGY_HASH;
    natts = if hash_part { 2 } else { partnatts };
    bound_datums = palloc(ndatums as usize * natts as usize * core::mem::size_of::<Datum>())
        as *mut Datum;

    i = 0;
    while i < ndatums {
        let mut j: c_int = 0;

        *(*dest).datums.offset(i as isize) = bound_datums.offset((i * natts) as isize);

        while j < natts {
            let byval: bool;
            let typlen: i16;

            if hash_part {
                typlen = core::mem::size_of::<i32>() as i16; /* Always int4 */
                byval = true; /* int4 is pass-by-value */
            } else {
                byval = *(*key).parttypbyval.offset(j as isize);
                typlen = *(*key).parttyplen.offset(j as isize);
            }

            if (*dest).kind.is_null() ||
                *(*(*dest).kind.offset(i as isize)).offset(j as isize) ==
                    PARTITION_RANGE_DATUM_VALUE
            {
                *(*(*dest).datums.offset(i as isize)).offset(j as isize) =
                    datumCopy(
                        *(*(*src).datums.offset(i as isize)).offset(j as isize),
                        byval,
                        typlen as c_int,
                    );
            }
            j += 1;
        }
        i += 1;
    }

    (*dest).indexes = palloc(core::mem::size_of::<c_int>() * nindexes as usize) as *mut c_int;
    ptr::copy_nonoverlapping((*src).indexes, (*dest).indexes, nindexes as usize);

    (*dest).null_index = (*src).null_index;
    (*dest).default_index = (*src).default_index;

    dest as PartitionBoundInfo
}

// ---------------------------------------------------------------------------
// Part 2: partition_bounds_merge, merge_list_bounds, merge_range_bounds,
//         init/free partition_map, is_dummy_partition
// ---------------------------------------------------------------------------

/*
 * partition_bounds_merge
 *		Check to see whether every partition of 'outer_rel' matches/overlaps
 *		one partition of 'inner_rel' at most, and vice versa; and if so, build
 *		and return the partition bounds for a join relation between the rels,
 *		generating two lists of the matching/overlapping partitions, which are
 *		returned to *outer_parts and *inner_parts respectively.
 *
 * The lists contain the same number of partitions, and the partitions at the
 * same positions in the lists indicate join pairs used for partitioned join.
 * If a partition on one side matches/overlaps multiple partitions on the other
 * side, this function returns NULL, setting *outer_parts and *inner_parts to
 * NIL.
 */
pub unsafe fn partition_bounds_merge(
    partnatts: c_int,
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    jointype: JoinType,
    outer_parts: *mut *mut List,
    inner_parts: *mut *mut List,
) -> PartitionBoundInfo {
    /*
     * Currently, this function is called only from try_partitionwise_join(),
     * so the join type should be INNER, LEFT, FULL, SEMI, or ANTI.
     */
    Assert!(
        jointype == JOIN_INNER
            || jointype == JOIN_LEFT
            || jointype == JOIN_FULL
            || jointype == JOIN_SEMI
            || jointype == JOIN_ANTI
    );

    /* The partitioning strategies should be the same. */
    Assert!((*pbi((*outer_rel).boundinfo)).strategy == (*pbi((*inner_rel).boundinfo)).strategy);

    *outer_parts = NIL;
    *inner_parts = NIL;

    match (*pbi((*outer_rel).boundinfo)).strategy as u8 as char {
        'h' /* PARTITION_STRATEGY_HASH */ => {
            /*
             * For hash partitioned tables, we currently support partitioned
             * join only when they have exactly the same partition bounds.
             *
             * XXX: it might be possible to relax the restriction to support
             * cases where hash partitioned tables have missing partitions
             * and/or different moduli, but it's not clear if it would be
             * useful to support the former case since it's unusual to have
             * missing partitions.  On the other hand, it would be useful to
             * support the latter case, but in that case, there is a high
             * probability that a partition on one side will match multiple
             * partitions on the other side, which is the scenario the current
             * implementation of partitioned join can't handle.
             */
            return ptr::null_mut();
        }
        'l' /* PARTITION_STRATEGY_LIST */ => {
            return merge_list_bounds(
                partsupfunc,
                partcollation,
                outer_rel,
                inner_rel,
                jointype,
                outer_parts,
                inner_parts,
            );
        }
        'r' /* PARTITION_STRATEGY_RANGE */ => {
            return merge_range_bounds(
                partnatts,
                partsupfunc,
                partcollation,
                outer_rel,
                inner_rel,
                jointype,
                outer_parts,
                inner_parts,
            );
        }
        _ => {}
    }

    ptr::null_mut()
}

/*
 * merge_list_bounds
 *		Create the partition bounds for a join relation between list
 *		partitioned tables, if possible
 *
 * In this function we try to find sets of matching partitions from both sides
 * by comparing list values stored in their partition bounds.  Since the list
 * values appear in the ascending order, an algorithm similar to merge join is
 * used for that.  If a partition on one side doesn't have a matching
 * partition on the other side, the algorithm tries to match it with the
 * default partition on the other side if any; if not, the algorithm tries to
 * match it with a dummy partition on the other side if it's on the
 * non-nullable side of an outer join.  Also, if both sides have the default
 * partitions, the algorithm tries to match them with each other.  We give up
 * if the algorithm finds a partition matching multiple partitions on the
 * other side, which is the scenario the current implementation of partitioned
 * join can't handle.
 */
unsafe fn merge_list_bounds(
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    jointype: JoinType,
    outer_parts: *mut *mut List,
    inner_parts: *mut *mut List,
) -> PartitionBoundInfo {
    let mut merged_bounds: PartitionBoundInfo = ptr::null_mut();
    let outer_bi = pbi((*outer_rel).boundinfo);
    let inner_bi = pbi((*inner_rel).boundinfo);
    let mut outer_has_default: bool = partition_bound_has_default((*outer_rel).boundinfo);
    let mut inner_has_default: bool = partition_bound_has_default((*inner_rel).boundinfo);
    let outer_default: c_int = (*outer_bi).default_index;
    let inner_default: c_int = (*inner_bi).default_index;
    let mut outer_has_null: bool = partition_bound_accepts_nulls((*outer_rel).boundinfo);
    let mut inner_has_null: bool = partition_bound_accepts_nulls((*inner_rel).boundinfo);
    let mut outer_map: PartitionMap = core::mem::zeroed();
    let mut inner_map: PartitionMap = core::mem::zeroed();
    let mut outer_pos: c_int;
    let mut inner_pos: c_int;
    let mut next_index: c_int = 0;
    let mut null_index: c_int = -1;
    let mut default_index: c_int = -1;
    let mut merged_datums: *mut List = NIL;
    let mut merged_indexes: *mut List = NIL;

    Assert!(*outer_parts == NIL);
    Assert!(*inner_parts == NIL);
    Assert!(
        (*outer_bi).strategy == (*inner_bi).strategy
            && (*outer_bi).strategy == b'l' as c_char
    );
    /* List partitioning doesn't require kinds. */
    Assert!((*outer_bi).kind.is_null() && (*inner_bi).kind.is_null());

    init_partition_map(outer_rel, &mut outer_map);
    init_partition_map(inner_rel, &mut inner_map);

    /*
     * If the default partitions (if any) have been proven empty, deem them
     * non-existent.
     */
    if outer_has_default && is_dummy_partition(outer_rel, outer_default) {
        outer_has_default = false;
    }
    if inner_has_default && is_dummy_partition(inner_rel, inner_default) {
        inner_has_default = false;
    }

    /*
     * Merge partitions from both sides.  In each iteration we compare a pair
     * of list values, one from each side, and decide whether the
     * corresponding partitions match or not.  If the two values match
     * exactly, move to the next pair of list values, otherwise move to the
     * next list value on the side with a smaller list value.
     */
    outer_pos = 0;
    inner_pos = 0;
    'merge_loop: while outer_pos < (*outer_bi).ndatums || inner_pos < (*inner_bi).ndatums {
        let mut outer_index: c_int = -1;
        let mut inner_index: c_int = -1;
        let outer_datums: *mut Datum;
        let inner_datums: *mut Datum;
        let cmpval: c_int;
        let mut merged_datum: *mut Datum = ptr::null_mut();
        let mut merged_index: c_int = -1;

        if outer_pos < (*outer_bi).ndatums {
            /*
             * If the partition on the outer side has been proven empty,
             * ignore it and move to the next datum on the outer side.
             */
            outer_index = *(*outer_bi).indexes.offset(outer_pos as isize);
            if is_dummy_partition(outer_rel, outer_index) {
                outer_pos += 1;
                continue 'merge_loop;
            }
        }
        if inner_pos < (*inner_bi).ndatums {
            /*
             * If the partition on the inner side has been proven empty,
             * ignore it and move to the next datum on the inner side.
             */
            inner_index = *(*inner_bi).indexes.offset(inner_pos as isize);
            if is_dummy_partition(inner_rel, inner_index) {
                inner_pos += 1;
                continue 'merge_loop;
            }
        }

        /* Get the list values. */
        outer_datums = if outer_pos < (*outer_bi).ndatums {
            *(*outer_bi).datums.offset(outer_pos as isize)
        } else {
            ptr::null_mut()
        };
        inner_datums = if inner_pos < (*inner_bi).ndatums {
            *(*inner_bi).datums.offset(inner_pos as isize)
        } else {
            ptr::null_mut()
        };

        /*
         * We run this loop till both sides finish.  This allows us to avoid
         * duplicating code to handle the remaining values on the side which
         * finishes later.  For that we set the comparison parameter cmpval in
         * such a way that it appears as if the side which finishes earlier
         * has an extra value higher than any other value on the unfinished
         * side. That way we advance the values on the unfinished side till
         * all of its values are exhausted.
         */
        if outer_pos >= (*outer_bi).ndatums {
            cmpval = 1;
        } else if inner_pos >= (*inner_bi).ndatums {
            cmpval = -1;
        } else {
            Assert!(!outer_datums.is_null() && !inner_datums.is_null());
            cmpval = DatumGetInt32(FunctionCall2Coll(
                &mut *partsupfunc.offset(0),
                *partcollation.offset(0),
                *outer_datums.offset(0),
                *inner_datums.offset(0),
            ));
        }

        if cmpval == 0 {
            /* Two list values match exactly. */
            Assert!(outer_pos < (*outer_bi).ndatums);
            Assert!(inner_pos < (*inner_bi).ndatums);
            Assert!(outer_index >= 0);
            Assert!(inner_index >= 0);

            /*
             * Try merging both partitions.  If successful, add the list value
             * and index of the merged partition below.
             */
            merged_index = merge_matching_partitions(
                &mut outer_map,
                &mut inner_map,
                outer_index,
                inner_index,
                &mut next_index,
            );
            if merged_index == -1 {
                break 'merge_loop; /* goto cleanup */
            }

            merged_datum = outer_datums;

            /* Move to the next pair of list values. */
            outer_pos += 1;
            inner_pos += 1;
        } else if cmpval < 0 {
            /* A list value missing from the inner side. */
            Assert!(outer_pos < (*outer_bi).ndatums);

            /*
             * If the inner side has the default partition, or this is an
             * outer join, try to assign a merged partition to the outer
             * partition (see process_outer_partition()).  Otherwise, the
             * outer partition will not contribute to the result.
             */
            if inner_has_default || IS_OUTER_JOIN(jointype) {
                /* Get the outer partition. */
                outer_index = *(*outer_bi).indexes.offset(outer_pos as isize);
                Assert!(outer_index >= 0);
                merged_index = process_outer_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    outer_index,
                    inner_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    break 'merge_loop; /* goto cleanup */
                }
                merged_datum = outer_datums;
            }

            /* Move to the next list value on the outer side. */
            outer_pos += 1;
        } else {
            /* A list value missing from the outer side. */
            Assert!(cmpval > 0);
            Assert!(inner_pos < (*inner_bi).ndatums);

            /*
             * If the outer side has the default partition, or this is a FULL
             * join, try to assign a merged partition to the inner partition
             * (see process_inner_partition()).  Otherwise, the inner
             * partition will not contribute to the result.
             */
            if outer_has_default || jointype == JOIN_FULL {
                /* Get the inner partition. */
                inner_index = *(*inner_bi).indexes.offset(inner_pos as isize);
                Assert!(inner_index >= 0);
                merged_index = process_inner_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    inner_index,
                    outer_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    break 'merge_loop; /* goto cleanup */
                }
                merged_datum = inner_datums;
            }

            /* Move to the next list value on the inner side. */
            inner_pos += 1;
        }

        /*
         * If we assigned a merged partition, add the list value and index of
         * the merged partition if appropriate.
         */
        if merged_index >= 0 && merged_index != default_index {
            merged_datums = lappend(merged_datums, merged_datum as *mut c_void);
            merged_indexes = lappend_int(merged_indexes, merged_index);
        }
    } /* end 'merge_loop */

    /*
     * If the NULL partitions (if any) have been proven empty, deem them
     * non-existent.
     */
    if outer_has_null && is_dummy_partition(outer_rel, (*outer_bi).null_index) {
        outer_has_null = false;
    }
    if inner_has_null && is_dummy_partition(inner_rel, (*inner_bi).null_index) {
        inner_has_null = false;
    }

    /* Merge the NULL partitions if any. */
    if outer_has_null || inner_has_null {
        merge_null_partitions(
            &mut outer_map,
            &mut inner_map,
            outer_has_null,
            inner_has_null,
            (*outer_bi).null_index,
            (*inner_bi).null_index,
            jointype,
            &mut next_index,
            &mut null_index,
        );
    } else {
        Assert!(null_index == -1);
    }

    /* Merge the default partitions if any. */
    if outer_has_default || inner_has_default {
        merge_default_partitions(
            &mut outer_map,
            &mut inner_map,
            outer_has_default,
            inner_has_default,
            outer_default,
            inner_default,
            jointype,
            &mut next_index,
            &mut default_index,
        );
    } else {
        Assert!(default_index == -1);
    }

    /* If we have merged partitions, create the partition bounds. */
    if next_index > 0 {
        /* Fix the merged_indexes list if necessary. */
        if outer_map.did_remapping || inner_map.did_remapping {
            Assert!(jointype == JOIN_FULL);
            fix_merged_indexes(&mut outer_map, &mut inner_map, next_index, merged_indexes);
        }

        /* Use maps to match partitions from inputs. */
        generate_matching_part_pairs(
            outer_rel,
            inner_rel,
            &mut outer_map,
            &mut inner_map,
            next_index,
            outer_parts,
            inner_parts,
        );
        Assert!(*outer_parts != NIL);
        Assert!(*inner_parts != NIL);
        Assert!(list_length(*outer_parts) == list_length(*inner_parts));
        Assert!(list_length(*outer_parts) <= next_index);

        /* Make a PartitionBoundInfo struct to return. */
        merged_bounds = build_merged_partition_bounds(
            (*outer_bi).strategy,
            merged_datums,
            NIL,
            merged_indexes,
            null_index,
            default_index,
        );
        Assert!(!merged_bounds.is_null());
    }

    /* cleanup: Free local memory before returning. */
    list_free(merged_datums);
    list_free(merged_indexes);
    free_partition_map(&mut outer_map);
    free_partition_map(&mut inner_map);

    merged_bounds
}

/*
 * merge_range_bounds
 *		Create the partition bounds for a join relation between range
 *		partitioned tables, if possible
 */
unsafe fn merge_range_bounds(
    partnatts: c_int,
    partsupfuncs: *mut FmgrInfo,
    partcollations: *mut Oid,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    jointype: JoinType,
    outer_parts: *mut *mut List,
    inner_parts: *mut *mut List,
) -> PartitionBoundInfo {
    let mut merged_bounds: PartitionBoundInfo = ptr::null_mut();
    let outer_bi = pbi((*outer_rel).boundinfo);
    let inner_bi = pbi((*inner_rel).boundinfo);
    let mut outer_has_default: bool = partition_bound_has_default((*outer_rel).boundinfo);
    let mut inner_has_default: bool = partition_bound_has_default((*inner_rel).boundinfo);
    let outer_default: c_int = (*outer_bi).default_index;
    let inner_default: c_int = (*inner_bi).default_index;
    let mut outer_map: PartitionMap = core::mem::zeroed();
    let mut inner_map: PartitionMap = core::mem::zeroed();
    let mut outer_index: c_int;
    let mut inner_index: c_int;
    let mut outer_lb_pos: c_int;
    let mut inner_lb_pos: c_int;
    let mut outer_lb: PartitionRangeBound = core::mem::zeroed();
    let mut outer_ub: PartitionRangeBound = core::mem::zeroed();
    let mut inner_lb: PartitionRangeBound = core::mem::zeroed();
    let mut inner_ub: PartitionRangeBound = core::mem::zeroed();
    let mut next_index: c_int = 0;
    let mut default_index: c_int = -1;
    let mut merged_datums: *mut List = NIL;
    let mut merged_kinds: *mut List = NIL;
    let mut merged_indexes: *mut List = NIL;

    Assert!(*outer_parts == NIL);
    Assert!(*inner_parts == NIL);
    Assert!(
        (*outer_bi).strategy == (*inner_bi).strategy
            && (*outer_bi).strategy == b'r' as c_char
    );

    init_partition_map(outer_rel, &mut outer_map);
    init_partition_map(inner_rel, &mut inner_map);

    /*
     * If the default partitions (if any) have been proven empty, deem them
     * non-existent.
     */
    if outer_has_default && is_dummy_partition(outer_rel, outer_default) {
        outer_has_default = false;
    }
    if inner_has_default && is_dummy_partition(inner_rel, inner_default) {
        inner_has_default = false;
    }

    /*
     * Merge partitions from both sides.  In each iteration we compare a pair
     * of ranges, one from each side, and decide whether the corresponding
     * partitions match or not.  outer_lb_pos/inner_lb_pos keep track of the
     * positions of lower bounds in the datums arrays in the outer/inner
     * PartitionBoundInfos respectively.
     */
    outer_lb_pos = 0;
    inner_lb_pos = 0;
    outer_index = get_range_partition(outer_rel, (*outer_rel).boundinfo, &mut outer_lb_pos, &mut outer_lb, &mut outer_ub);
    inner_index = get_range_partition(inner_rel, (*inner_rel).boundinfo, &mut inner_lb_pos, &mut inner_lb, &mut inner_ub);

    'range_loop: while outer_index >= 0 || inner_index >= 0 {
        let overlap: bool;
        let mut ub_cmpval: c_int = 0;
        let mut lb_cmpval: c_int = 0;
        let mut merged_lb: PartitionRangeBound = PartitionRangeBound {
            index: -1, datums: ptr::null_mut(), kind: ptr::null_mut(), lower: true,
        };
        let mut merged_ub: PartitionRangeBound = PartitionRangeBound {
            index: -1, datums: ptr::null_mut(), kind: ptr::null_mut(), lower: false,
        };
        let mut merged_index: c_int = -1;

        /*
         * We run this loop till both sides finish.  This allows us to avoid
         * duplicating code to handle the remaining ranges on the side which
         * finishes later.
         */
        if outer_index == -1 {
            overlap = false;
            lb_cmpval = 1;
            ub_cmpval = 1;
        } else if inner_index == -1 {
            overlap = false;
            lb_cmpval = -1;
            ub_cmpval = -1;
        } else {
            overlap = compare_range_partitions(
                partnatts,
                partsupfuncs,
                partcollations,
                &outer_lb,
                &outer_ub,
                &inner_lb,
                &inner_ub,
                &mut lb_cmpval,
                &mut ub_cmpval,
            );
        }

        if overlap {
            /* Two ranges overlap; form a join pair. */

            let save_outer_ub: PartitionRangeBound;
            let save_inner_ub: PartitionRangeBound;

            /* Both partitions should not have been merged yet. */
            Assert!(outer_index >= 0);
            Assert!(
                *outer_map.merged_indexes.offset(outer_index as isize) == -1
                    && *outer_map.merged.offset(outer_index as isize) == false
            );
            Assert!(inner_index >= 0);
            Assert!(
                *inner_map.merged_indexes.offset(inner_index as isize) == -1
                    && *inner_map.merged.offset(inner_index as isize) == false
            );

            /*
             * Get the index of the merged partition.  Both partitions aren't
             * merged yet, so the partitions should be merged successfully.
             */
            merged_index = merge_matching_partitions(
                &mut outer_map,
                &mut inner_map,
                outer_index,
                inner_index,
                &mut next_index,
            );
            Assert!(merged_index >= 0);

            /* Get the range bounds of the merged partition. */
            get_merged_range_bounds(
                partnatts,
                partsupfuncs,
                partcollations,
                jointype,
                &outer_lb,
                &outer_ub,
                &inner_lb,
                &inner_ub,
                lb_cmpval,
                ub_cmpval,
                &mut merged_lb,
                &mut merged_ub,
            );

            /* Save the upper bounds of both partitions for use below. */
            save_outer_ub = core::ptr::read(&outer_ub);
            save_inner_ub = core::ptr::read(&inner_ub);

            /* Move to the next pair of ranges. */
            outer_index = get_range_partition(outer_rel, (*outer_rel).boundinfo, &mut outer_lb_pos, &mut outer_lb, &mut outer_ub);
            inner_index = get_range_partition(inner_rel, (*inner_rel).boundinfo, &mut inner_lb_pos, &mut inner_lb, &mut inner_ub);

            /*
             * If the range of a partition on one side overlaps the range of
             * the next partition on the other side, that will cause the
             * partition on one side to match at least two partitions on the
             * other side, which is the case that we currently don't support
             * partitioned join for; give up.
             */
            if ub_cmpval > 0
                && inner_index >= 0
                && compare_range_bounds!(partnatts, partsupfuncs, partcollations, &save_outer_ub, &inner_lb) > 0
            {
                break 'range_loop; /* goto cleanup */
            }
            if ub_cmpval < 0
                && outer_index >= 0
                && compare_range_bounds!(partnatts, partsupfuncs, partcollations, &outer_lb, &save_inner_ub) < 0
            {
                break 'range_loop; /* goto cleanup */
            }

            /*
             * A row from a non-overlapping portion (if any) of a partition on
             * one side might find its join partner in the default partition
             * (if any) on the other side, causing the same situation as
             * above; give up in that case.
             */
            if (outer_has_default && (lb_cmpval > 0 || ub_cmpval < 0))
                || (inner_has_default && (lb_cmpval < 0 || ub_cmpval > 0))
            {
                break 'range_loop; /* goto cleanup */
            }
        } else if ub_cmpval < 0 {
            /* A non-overlapping outer range. */

            /* The outer partition should not have been merged yet. */
            Assert!(outer_index >= 0);
            Assert!(
                *outer_map.merged_indexes.offset(outer_index as isize) == -1
                    && *outer_map.merged.offset(outer_index as isize) == false
            );

            /*
             * If the inner side has the default partition, or this is an
             * outer join, try to assign a merged partition to the outer
             * partition.  Otherwise, the outer partition will not contribute
             * to the result.
             */
            if inner_has_default || IS_OUTER_JOIN(jointype) {
                merged_index = process_outer_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    outer_index,
                    inner_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    break 'range_loop; /* goto cleanup */
                }
                merged_lb = core::ptr::read(&outer_lb);
                merged_ub = core::ptr::read(&outer_ub);
            }

            /* Move to the next range on the outer side. */
            outer_index = get_range_partition(outer_rel, (*outer_rel).boundinfo, &mut outer_lb_pos, &mut outer_lb, &mut outer_ub);
        } else {
            /* A non-overlapping inner range. */
            Assert!(ub_cmpval > 0);

            /* The inner partition should not have been merged yet. */
            Assert!(inner_index >= 0);
            Assert!(
                *inner_map.merged_indexes.offset(inner_index as isize) == -1
                    && *inner_map.merged.offset(inner_index as isize) == false
            );

            /*
             * If the outer side has the default partition, or this is a FULL
             * join, try to assign a merged partition to the inner partition.
             * Otherwise, the inner partition will not contribute to the result.
             */
            if outer_has_default || jointype == JOIN_FULL {
                merged_index = process_inner_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    inner_index,
                    outer_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    break 'range_loop; /* goto cleanup */
                }
                merged_lb = core::ptr::read(&inner_lb);
                merged_ub = core::ptr::read(&inner_ub);
            }

            /* Move to the next range on the inner side. */
            inner_index = get_range_partition(inner_rel, (*inner_rel).boundinfo, &mut inner_lb_pos, &mut inner_lb, &mut inner_ub);
        }

        /*
         * If we assigned a merged partition, add the range bounds and index
         * of the merged partition if appropriate.
         */
        if merged_index >= 0 && merged_index != default_index {
            add_merged_range_bounds(
                partnatts,
                partsupfuncs,
                partcollations,
                &merged_lb,
                &merged_ub,
                merged_index,
                &mut merged_datums,
                &mut merged_kinds,
                &mut merged_indexes,
            );
        }
    } /* end 'range_loop */

    /* Merge the default partitions if any. */
    if outer_has_default || inner_has_default {
        merge_default_partitions(
            &mut outer_map,
            &mut inner_map,
            outer_has_default,
            inner_has_default,
            outer_default,
            inner_default,
            jointype,
            &mut next_index,
            &mut default_index,
        );
    } else {
        Assert!(default_index == -1);
    }

    /* If we have merged partitions, create the partition bounds. */
    if next_index > 0 {
        /*
         * Unlike the case of list partitioning, we wouldn't have re-merged
         * partitions, so did_remapping should be left alone.
         */
        Assert!(!outer_map.did_remapping);
        Assert!(!inner_map.did_remapping);

        /* Use maps to match partitions from inputs. */
        generate_matching_part_pairs(
            outer_rel,
            inner_rel,
            &mut outer_map,
            &mut inner_map,
            next_index,
            outer_parts,
            inner_parts,
        );
        Assert!(*outer_parts != NIL);
        Assert!(*inner_parts != NIL);
        Assert!(list_length(*outer_parts) == list_length(*inner_parts));
        Assert!(list_length(*outer_parts) == next_index);

        /* Make a PartitionBoundInfo struct to return. */
        merged_bounds = build_merged_partition_bounds(
            (*outer_bi).strategy,
            merged_datums,
            merged_kinds,
            merged_indexes,
            -1,
            default_index,
        );
        Assert!(!merged_bounds.is_null());
    }

    /* cleanup: Free local memory before returning. */
    list_free(merged_datums);
    list_free(merged_kinds);
    list_free(merged_indexes);
    free_partition_map(&mut outer_map);
    free_partition_map(&mut inner_map);

    merged_bounds
}

/*
 * init_partition_map
 *		Initialize a PartitionMap struct for given relation
 */
unsafe fn init_partition_map(rel: *mut RelOptInfo, map: *mut PartitionMap) {
    let nparts: c_int = (*rel).nparts;
    let mut i: c_int;

    (*map).nparts = nparts;
    (*map).merged_indexes = palloc(core::mem::size_of::<c_int>() * nparts as usize) as *mut c_int;
    (*map).merged = palloc(core::mem::size_of::<bool>() * nparts as usize) as *mut bool;
    (*map).did_remapping = false;
    (*map).old_indexes = palloc(core::mem::size_of::<c_int>() * nparts as usize) as *mut c_int;
    i = 0;
    while i < nparts {
        *(*map).merged_indexes.offset(i as isize) = -1;
        *(*map).old_indexes.offset(i as isize) = -1;
        *(*map).merged.offset(i as isize) = false;
        i += 1;
    }
}

/*
 * free_partition_map
 */
unsafe fn free_partition_map(map: *mut PartitionMap) {
    pfree((*map).merged_indexes as *mut c_void);
    pfree((*map).merged as *mut c_void);
    pfree((*map).old_indexes as *mut c_void);
}

/*
 * is_dummy_partition --- has partition been proven empty?
 */
unsafe fn is_dummy_partition(rel: *mut RelOptInfo, part_index: c_int) -> bool {
    let part_rel: *mut RelOptInfo;

    Assert!(part_index >= 0);
    part_rel = *(*rel).part_rels.offset(part_index as isize);
    if part_rel.is_null() || IS_DUMMY_REL(part_rel) {
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Part 3: merge_matching_partitions, process_outer_partition,
//         process_inner_partition, merge_null_partitions,
//         merge_default_partitions, merge_partition_with_dummy
// ---------------------------------------------------------------------------

/*
 * merge_matching_partitions
 *		Try to merge given outer/inner partitions, and return the index of a
 *		merged partition produced from them if successful, -1 otherwise
 *
 * If the merged partition is newly created, *next_index is incremented.
 */
unsafe fn merge_matching_partitions(
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    outer_index: c_int,
    inner_index: c_int,
    next_index: *mut c_int,
) -> c_int {
    let outer_merged_index: c_int;
    let inner_merged_index: c_int;
    let outer_merged: bool;
    let inner_merged: bool;

    Assert!(outer_index >= 0 && outer_index < (*outer_map).nparts);
    outer_merged_index = *(*outer_map).merged_indexes.offset(outer_index as isize);
    outer_merged = *(*outer_map).merged.offset(outer_index as isize);
    Assert!(inner_index >= 0 && inner_index < (*inner_map).nparts);
    inner_merged_index = *(*inner_map).merged_indexes.offset(inner_index as isize);
    inner_merged = *(*inner_map).merged.offset(inner_index as isize);

    /*
     * Handle cases where we have already assigned a merged partition to each
     * of the given partitions.
     */
    if outer_merged_index >= 0 && inner_merged_index >= 0 {
        /*
         * If the merged partitions are the same, no need to do anything;
         * return the index of the merged partitions.  Otherwise, if each of
         * the given partitions has been merged with a dummy partition on the
         * other side, re-map them to either of the two merged partitions.
         * Otherwise, they can't be merged, so return -1.
         */
        if outer_merged_index == inner_merged_index {
            Assert!(outer_merged);
            Assert!(inner_merged);
            return outer_merged_index;
        }
        if !outer_merged && !inner_merged {
            /*
             * This can only happen for a list-partitioning case.  We re-map
             * them to the merged partition with the smaller of the two merged
             * indexes to preserve the property that the canonical order of
             * list partitions is determined by the indexes assigned to the
             * smallest list value of each partition.
             */
            if outer_merged_index < inner_merged_index {
                *(*outer_map).merged.offset(outer_index as isize) = true;
                *(*inner_map).merged_indexes.offset(inner_index as isize) = outer_merged_index;
                *(*inner_map).merged.offset(inner_index as isize) = true;
                (*inner_map).did_remapping = true;
                *(*inner_map).old_indexes.offset(inner_index as isize) = inner_merged_index;
                return outer_merged_index;
            } else {
                *(*inner_map).merged.offset(inner_index as isize) = true;
                *(*outer_map).merged_indexes.offset(outer_index as isize) = inner_merged_index;
                *(*outer_map).merged.offset(outer_index as isize) = true;
                (*outer_map).did_remapping = true;
                *(*outer_map).old_indexes.offset(outer_index as isize) = outer_merged_index;
                return inner_merged_index;
            }
        }
        return -1;
    }

    /* At least one of the given partitions should not have yet been merged. */
    Assert!(outer_merged_index == -1 || inner_merged_index == -1);

    /*
     * If neither of them has been merged, merge them.  Otherwise, if one has
     * been merged with a dummy partition on the other side (and the other
     * hasn't yet been merged with anything), re-merge them.  Otherwise, they
     * can't be merged, so return -1.
     */
    if outer_merged_index == -1 && inner_merged_index == -1 {
        let merged_index = *next_index;

        Assert!(!outer_merged);
        Assert!(!inner_merged);
        *(*outer_map).merged_indexes.offset(outer_index as isize) = merged_index;
        *(*outer_map).merged.offset(outer_index as isize) = true;
        *(*inner_map).merged_indexes.offset(inner_index as isize) = merged_index;
        *(*inner_map).merged.offset(inner_index as isize) = true;
        *next_index = *next_index + 1;
        return merged_index;
    }
    if outer_merged_index >= 0 && !(*(*outer_map).merged.offset(outer_index as isize)) {
        Assert!(inner_merged_index == -1);
        Assert!(!inner_merged);
        *(*inner_map).merged_indexes.offset(inner_index as isize) = outer_merged_index;
        *(*inner_map).merged.offset(inner_index as isize) = true;
        *(*outer_map).merged.offset(outer_index as isize) = true;
        return outer_merged_index;
    }
    if inner_merged_index >= 0 && !(*(*inner_map).merged.offset(inner_index as isize)) {
        Assert!(outer_merged_index == -1);
        Assert!(!outer_merged);
        *(*outer_map).merged_indexes.offset(outer_index as isize) = inner_merged_index;
        *(*outer_map).merged.offset(outer_index as isize) = true;
        *(*inner_map).merged.offset(inner_index as isize) = true;
        return inner_merged_index;
    }
    -1
}

/*
 * process_outer_partition
 *		Try to assign given outer partition a merged partition, and return the
 *		index of the merged partition if successful, -1 otherwise
 *
 * If the partition is newly created, *next_index is incremented.  Also, if it
 * is the default partition of the join relation, *default_index is set to the
 * index if not already done.
 */
unsafe fn process_outer_partition(
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    outer_has_default: bool,
    inner_has_default: bool,
    outer_index: c_int,
    inner_default: c_int,
    jointype: JoinType,
    next_index: *mut c_int,
    default_index: *mut c_int,
) -> c_int {
    let mut merged_index: c_int = -1;

    Assert!(outer_index >= 0);

    /*
     * If the inner side has the default partition, a row from the outer
     * partition might find its join partner in the default partition; try
     * merging the outer partition with the default partition.  Otherwise,
     * this should be an outer join, in which case the outer partition has to
     * be scanned all the way anyway; merge the outer partition with a dummy
     * partition on the other side.
     */
    if inner_has_default {
        Assert!(inner_default >= 0);

        /*
         * If the outer side has the default partition as well, the default
         * partition on the inner side will have two matching partitions on
         * the other side: the outer partition and the default partition on
         * the outer side.  Partitionwise join doesn't handle this scenario
         * yet.
         */
        if outer_has_default {
            return -1;
        }

        merged_index = merge_matching_partitions(
            outer_map, inner_map, outer_index, inner_default, next_index,
        );
        if merged_index == -1 {
            return -1;
        }

        /*
         * If this is a FULL join, the default partition on the inner side has
         * to be scanned all the way anyway, so the resulting partition will
         * contain all key values from the default partition, which any other
         * partition of the join relation will not contain.  Thus the
         * resulting partition will act as the default partition of the join
         * relation; record the index in *default_index if not already done.
         */
        if jointype == JOIN_FULL {
            if *default_index == -1 {
                *default_index = merged_index;
            } else {
                Assert!(*default_index == merged_index);
            }
        }
    } else {
        Assert!(IS_OUTER_JOIN(jointype));
        Assert!(jointype != JOIN_RIGHT);

        /* If we have already assigned a partition, no need to do anything. */
        merged_index = *(*outer_map).merged_indexes.offset(outer_index as isize);
        if merged_index == -1 {
            merged_index = merge_partition_with_dummy(outer_map, outer_index, next_index);
        }
    }
    merged_index
}

/*
 * process_inner_partition
 *		Try to assign given inner partition a merged partition, and return the
 *		index of the merged partition if successful, -1 otherwise
 *
 * If the partition is newly created, *next_index is incremented.  Also, if it
 * is the default partition of the join relation, *default_index is set to the
 * index if not already done.
 */
unsafe fn process_inner_partition(
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    outer_has_default: bool,
    inner_has_default: bool,
    inner_index: c_int,
    outer_default: c_int,
    jointype: JoinType,
    next_index: *mut c_int,
    default_index: *mut c_int,
) -> c_int {
    let mut merged_index: c_int = -1;

    Assert!(inner_index >= 0);

    /*
     * If the outer side has the default partition, a row from the inner
     * partition might find its join partner in the default partition; try
     * merging the inner partition with the default partition.  Otherwise,
     * this should be a FULL join, in which case the inner partition has to be
     * scanned all the way anyway; merge the inner partition with a dummy
     * partition on the other side.
     */
    if outer_has_default {
        Assert!(outer_default >= 0);

        /*
         * If the inner side has the default partition as well, the default
         * partition on the outer side will have two matching partitions on
         * the other side: the inner partition and the default partition on
         * the inner side.  Partitionwise join doesn't handle this scenario
         * yet.
         */
        if inner_has_default {
            return -1;
        }

        merged_index = merge_matching_partitions(
            outer_map, inner_map, outer_default, inner_index, next_index,
        );
        if merged_index == -1 {
            return -1;
        }

        /*
         * If this is an outer join, the default partition on the outer side
         * has to be scanned all the way anyway, so the resulting partition
         * will contain all key values from the default partition, which any
         * other partition of the join relation will not contain.  Thus the
         * resulting partition will act as the default partition of the join
         * relation; record the index in *default_index if not already done.
         */
        if IS_OUTER_JOIN(jointype) {
            Assert!(jointype != JOIN_RIGHT);
            if *default_index == -1 {
                *default_index = merged_index;
            } else {
                Assert!(*default_index == merged_index);
            }
        }
    } else {
        Assert!(jointype == JOIN_FULL);

        /* If we have already assigned a partition, no need to do anything. */
        merged_index = *(*inner_map).merged_indexes.offset(inner_index as isize);
        if merged_index == -1 {
            merged_index = merge_partition_with_dummy(inner_map, inner_index, next_index);
        }
    }
    merged_index
}

/*
 * merge_null_partitions
 *		Merge the NULL partitions from a join's outer and inner sides.
 *
 * If the merged partition produced from them is the NULL partition of the join
 * relation, *null_index is set to the index of the merged partition.
 *
 * Note: We assume here that the join clause for a partitioned join is strict
 * because have_partkey_equi_join() requires that the corresponding operator
 * be mergejoinable, and we currently assume that mergejoinable operators are
 * strict (see MJEvalOuterValues()/MJEvalInnerValues()).
 */
unsafe fn merge_null_partitions(
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    outer_has_null: bool,
    inner_has_null: bool,
    outer_null: c_int,
    inner_null: c_int,
    jointype: JoinType,
    next_index: *mut c_int,
    null_index: *mut c_int,
) {
    let mut consider_outer_null = false;
    let mut consider_inner_null = false;

    Assert!(outer_has_null || inner_has_null);
    Assert!(*null_index == -1);

    /*
     * Check whether the NULL partitions have already been merged and if so,
     * set the consider_outer_null/consider_inner_null flags.
     */
    if outer_has_null {
        Assert!(outer_null >= 0 && outer_null < (*outer_map).nparts);
        if *(*outer_map).merged_indexes.offset(outer_null as isize) == -1 {
            consider_outer_null = true;
        }
    }
    if inner_has_null {
        Assert!(inner_null >= 0 && inner_null < (*inner_map).nparts);
        if *(*inner_map).merged_indexes.offset(inner_null as isize) == -1 {
            consider_inner_null = true;
        }
    }

    /* If both flags are set false, we don't need to do anything. */
    if !consider_outer_null && !consider_inner_null {
        return;
    }

    if consider_outer_null && !consider_inner_null {
        Assert!(outer_has_null);

        /*
         * If this is an outer join, the NULL partition on the outer side has
         * to be scanned all the way anyway; merge the NULL partition with a
         * dummy partition on the other side.  In that case
         * consider_outer_null means that the NULL partition only contains
         * NULL values as the key values, so the merged partition will do so;
         * treat it as the NULL partition of the join relation.
         */
        if IS_OUTER_JOIN(jointype) {
            Assert!(jointype != JOIN_RIGHT);
            *null_index = merge_partition_with_dummy(outer_map, outer_null, next_index);
        }
    } else if !consider_outer_null && consider_inner_null {
        Assert!(inner_has_null);

        /*
         * If this is a FULL join, the NULL partition on the inner side has to
         * be scanned all the way anyway; merge the NULL partition with a
         * dummy partition on the other side.  In that case
         * consider_inner_null means that the NULL partition only contains
         * NULL values as the key values, so the merged partition will do so;
         * treat it as the NULL partition of the join relation.
         */
        if jointype == JOIN_FULL {
            *null_index = merge_partition_with_dummy(inner_map, inner_null, next_index);
        }
    } else {
        Assert!(consider_outer_null && consider_inner_null);
        Assert!(outer_has_null);
        Assert!(inner_has_null);

        /*
         * If this is an outer join, the NULL partition on the outer side (and
         * that on the inner side if this is a FULL join) have to be scanned
         * all the way anyway, so merge them.  Note that each of the NULL
         * partitions isn't merged yet, so they should be merged successfully.
         * Like the above, each of the NULL partitions only contains NULL
         * values as the key values, so the merged partition will do so; treat
         * it as the NULL partition of the join relation.
         *
         * Note: if this an INNER/SEMI join, the join clause will never be
         * satisfied by two NULL values (see comments above), so both the NULL
         * partitions can be eliminated.
         */
        if IS_OUTER_JOIN(jointype) {
            Assert!(jointype != JOIN_RIGHT);
            *null_index = merge_matching_partitions(
                outer_map, inner_map, outer_null, inner_null, next_index,
            );
            Assert!(*null_index >= 0);
        }
    }
}

/*
 * merge_default_partitions
 *		Merge the default partitions from a join's outer and inner sides.
 *
 * If the merged partition produced from them is the default partition of the
 * join relation, *default_index is set to the index of the merged partition.
 */
unsafe fn merge_default_partitions(
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    outer_has_default: bool,
    inner_has_default: bool,
    outer_default: c_int,
    inner_default: c_int,
    jointype: JoinType,
    next_index: *mut c_int,
    default_index: *mut c_int,
) {
    let mut outer_merged_index: c_int = -1;
    let mut inner_merged_index: c_int = -1;

    Assert!(outer_has_default || inner_has_default);

    /* Get the merged partition indexes for the default partitions. */
    if outer_has_default {
        Assert!(outer_default >= 0 && outer_default < (*outer_map).nparts);
        outer_merged_index = *(*outer_map).merged_indexes.offset(outer_default as isize);
    }
    if inner_has_default {
        Assert!(inner_default >= 0 && inner_default < (*inner_map).nparts);
        inner_merged_index = *(*inner_map).merged_indexes.offset(inner_default as isize);
    }

    if outer_has_default && !inner_has_default {
        /*
         * If this is an outer join, the default partition on the outer side
         * has to be scanned all the way anyway; if we have not yet assigned a
         * partition, merge the default partition with a dummy partition on
         * the other side.  The merged partition will act as the default
         * partition of the join relation (see comments in
         * process_inner_partition()).
         */
        if IS_OUTER_JOIN(jointype) {
            Assert!(jointype != JOIN_RIGHT);
            if outer_merged_index == -1 {
                Assert!(*default_index == -1);
                *default_index = merge_partition_with_dummy(outer_map, outer_default, next_index);
            } else {
                Assert!(*default_index == outer_merged_index);
            }
        } else {
            Assert!(*default_index == -1);
        }
    } else if !outer_has_default && inner_has_default {
        /*
         * If this is a FULL join, the default partition on the inner side has
         * to be scanned all the way anyway; if we have not yet assigned a
         * partition, merge the default partition with a dummy partition on
         * the other side.  The merged partition will act as the default
         * partition of the join relation (see comments in
         * process_outer_partition()).
         */
        if jointype == JOIN_FULL {
            if inner_merged_index == -1 {
                Assert!(*default_index == -1);
                *default_index = merge_partition_with_dummy(inner_map, inner_default, next_index);
            } else {
                Assert!(*default_index == inner_merged_index);
            }
        } else {
            Assert!(*default_index == -1);
        }
    } else {
        Assert!(outer_has_default && inner_has_default);

        /*
         * The default partitions have to be joined with each other, so merge
         * them.  Note that each of the default partitions isn't merged yet
         * (see, process_outer_partition()/process_inner_partition()), so they
         * should be merged successfully.  The merged partition will act as
         * the default partition of the join relation.
         */
        Assert!(outer_merged_index == -1);
        Assert!(inner_merged_index == -1);
        Assert!(*default_index == -1);
        *default_index = merge_matching_partitions(
            outer_map, inner_map, outer_default, inner_default, next_index,
        );
        Assert!(*default_index >= 0);
    }
}

/*
 * merge_partition_with_dummy
 *		Assign given partition a new partition of a join relation
 *
 * Note: The caller assumes that the given partition doesn't have a non-dummy
 * matching partition on the other side, but if the given partition finds the
 * matching partition later, we will adjust the assignment.
 */
unsafe fn merge_partition_with_dummy(
    map: *mut PartitionMap,
    index: c_int,
    next_index: *mut c_int,
) -> c_int {
    let merged_index = *next_index;

    Assert!(index >= 0 && index < (*map).nparts);
    Assert!(*(*map).merged_indexes.offset(index as isize) == -1);
    Assert!(!(*(*map).merged.offset(index as isize)));
    *(*map).merged_indexes.offset(index as isize) = merged_index;
    /* Leave the merged flag alone! */
    *next_index = *next_index + 1;
    merged_index
}

/*
 * fix_merged_indexes
 *		Adjust merged indexes of re-merged partitions
 */
unsafe fn fix_merged_indexes(
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    nmerged: c_int,
    merged_indexes: *mut List,
) {
    let new_indexes: *mut c_int;
    let mut merged_index: c_int;
    let mut i: c_int;

    Assert!(nmerged > 0);

    new_indexes = palloc(core::mem::size_of::<c_int>() * nmerged as usize) as *mut c_int;
    i = 0;
    while i < nmerged {
        *new_indexes.offset(i as isize) = -1;
        i += 1;
    }

    /* Build the mapping of old merged indexes to new merged indexes. */
    if (*outer_map).did_remapping {
        i = 0;
        while i < (*outer_map).nparts {
            merged_index = *(*outer_map).old_indexes.offset(i as isize);
            if merged_index >= 0 {
                *new_indexes.offset(merged_index as isize) =
                    *(*outer_map).merged_indexes.offset(i as isize);
            }
            i += 1;
        }
    }
    if (*inner_map).did_remapping {
        i = 0;
        while i < (*inner_map).nparts {
            merged_index = *(*inner_map).old_indexes.offset(i as isize);
            if merged_index >= 0 {
                *new_indexes.offset(merged_index as isize) =
                    *(*inner_map).merged_indexes.offset(i as isize);
            }
            i += 1;
        }
    }

    /* Fix the merged_indexes list using the mapping. */
    foreach!(lc, merged_indexes, {
        merged_index = lfirst_int(crate::current_cell!(lc));
        Assert!(merged_index >= 0);
        if *new_indexes.offset(merged_index as isize) >= 0 {
            /* mutate the cell in place */
            (*crate::current_cell!(lc)).int_value =
                *new_indexes.offset(merged_index as isize);
        }
    });

    pfree(new_indexes as *mut c_void);
}

/*
 * generate_matching_part_pairs
 *		Generate a pair of lists of partitions that produce merged partitions
 *
 * The lists of partitions are built in the order of merged partition indexes,
 * and returned in *outer_parts and *inner_parts.
 */
unsafe fn generate_matching_part_pairs(
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    outer_map: *mut PartitionMap,
    inner_map: *mut PartitionMap,
    nmerged: c_int,
    outer_parts: *mut *mut List,
    inner_parts: *mut *mut List,
) {
    let outer_nparts: c_int = (*outer_map).nparts;
    let inner_nparts: c_int = (*inner_map).nparts;
    let outer_indexes: *mut c_int;
    let inner_indexes: *mut c_int;
    let max_nparts: c_int;
    let mut i: c_int;

    Assert!(nmerged > 0);
    Assert!(*outer_parts == NIL);
    Assert!(*inner_parts == NIL);

    outer_indexes = palloc(core::mem::size_of::<c_int>() * nmerged as usize) as *mut c_int;
    inner_indexes = palloc(core::mem::size_of::<c_int>() * nmerged as usize) as *mut c_int;
    i = 0;
    while i < nmerged {
        *outer_indexes.offset(i as isize) = -1;
        *inner_indexes.offset(i as isize) = -1;
        i += 1;
    }

    /* Set pairs of matching partitions. */
    Assert!(outer_nparts == (*outer_rel).nparts);
    Assert!(inner_nparts == (*inner_rel).nparts);
    max_nparts = if outer_nparts > inner_nparts { outer_nparts } else { inner_nparts };
    i = 0;
    while i < max_nparts {
        if i < outer_nparts {
            let merged_index = *(*outer_map).merged_indexes.offset(i as isize);
            if merged_index >= 0 {
                Assert!(merged_index < nmerged);
                *outer_indexes.offset(merged_index as isize) = i;
            }
        }
        if i < inner_nparts {
            let merged_index = *(*inner_map).merged_indexes.offset(i as isize);
            if merged_index >= 0 {
                Assert!(merged_index < nmerged);
                *inner_indexes.offset(merged_index as isize) = i;
            }
        }
        i += 1;
    }

    /* Build the list pairs. */
    i = 0;
    while i < nmerged {
        let outer_index = *outer_indexes.offset(i as isize);
        let inner_index = *inner_indexes.offset(i as isize);

        /*
         * If both partitions are dummy, it means the merged partition that
         * had been assigned to the outer/inner partition was removed when
         * re-merging the outer/inner partition in
         * merge_matching_partitions(); ignore the merged partition.
         */
        if outer_index == -1 && inner_index == -1 {
            i += 1;
            continue;
        }

        *outer_parts = lappend(
            *outer_parts,
            if outer_index >= 0 {
                *(*outer_rel).part_rels.offset(outer_index as isize) as *mut c_void
            } else {
                ptr::null_mut()
            },
        );
        *inner_parts = lappend(
            *inner_parts,
            if inner_index >= 0 {
                *(*inner_rel).part_rels.offset(inner_index as isize) as *mut c_void
            } else {
                ptr::null_mut()
            },
        );
        i += 1;
    }

    pfree(outer_indexes as *mut c_void);
    pfree(inner_indexes as *mut c_void);
}

/*
 * build_merged_partition_bounds
 *		Create a PartitionBoundInfo struct from merged partition bounds
 */
unsafe fn build_merged_partition_bounds(
    strategy: c_char,
    merged_datums: *mut List,
    merged_kinds: *mut List,
    mut merged_indexes: *mut List,
    null_index: c_int,
    default_index: c_int,
) -> PartitionBoundInfo {
    let merged_bounds: *mut PartitionBoundInfoFull;
    let mut ndatums: c_int = list_length(merged_datums);
    let mut pos: c_int;

    merged_bounds = palloc(core::mem::size_of::<PartitionBoundInfoFull>())
        as *mut PartitionBoundInfoFull;
    (*merged_bounds).strategy = strategy;
    (*merged_bounds).ndatums = ndatums;

    (*merged_bounds).datums =
        palloc(core::mem::size_of::<*mut Datum>() * ndatums as usize) as *mut *mut Datum;
    pos = 0;
    foreach!(lc, merged_datums, {
        *(*merged_bounds).datums.offset(pos as isize) =
            lfirst(crate::current_cell!(lc)) as *mut Datum;
        pos += 1;
    });

    if strategy == b'r' as c_char {
        Assert!(list_length(merged_kinds) == ndatums);
        (*merged_bounds).kind = palloc(
            core::mem::size_of::<*mut PartitionRangeDatumKind>() * ndatums as usize,
        ) as *mut *mut PartitionRangeDatumKind;
        pos = 0;
        foreach!(lc, merged_kinds, {
            *(*merged_bounds).kind.offset(pos as isize) =
                lfirst(crate::current_cell!(lc)) as *mut PartitionRangeDatumKind;
            pos += 1;
        });

        /* There are ndatums+1 indexes in the case of range partitioning. */
        merged_indexes = lappend_int(merged_indexes, -1);
        ndatums += 1;
    } else {
        Assert!(strategy == b'l' as c_char);
        Assert!(merged_kinds == NIL);
        (*merged_bounds).kind = ptr::null_mut();
    }

    /* interleaved_parts is always NULL for join relations. */
    (*merged_bounds).interleaved_parts = ptr::null_mut();

    Assert!(list_length(merged_indexes) == ndatums);
    (*merged_bounds).nindexes = ndatums;
    (*merged_bounds).indexes =
        palloc(core::mem::size_of::<c_int>() * ndatums as usize) as *mut c_int;
    pos = 0;
    foreach!(lc, merged_indexes, {
        *(*merged_bounds).indexes.offset(pos as isize) = lfirst_int(crate::current_cell!(lc));
        pos += 1;
    });

    (*merged_bounds).null_index = null_index;
    (*merged_bounds).default_index = default_index;

    merged_bounds as PartitionBoundInfo
}

// ---------------------------------------------------------------------------
// Part 4: get_range_partition*, compare_range_partitions, get_merged_range_bounds,
//         add_merged_range_bounds, partitions_are_ordered, check_new_partition_bound,
//         check_default_partition_contents, get_hash_partition_greatest_modulus,
//         make_one_partition_rbound
// ---------------------------------------------------------------------------

/*
 * get_range_partition
 *		Get the next non-dummy partition of a range-partitioned relation,
 *		returning the index of that partition
 *
 * *lb and *ub are set to the lower and upper bounds of that partition
 * respectively, and *lb_pos is advanced to the next lower bound, if any.
 */
unsafe fn get_range_partition(
    rel: *mut RelOptInfo,
    bi: PartitionBoundInfo,
    lb_pos: *mut c_int,
    lb: *mut PartitionRangeBound,
    ub: *mut PartitionRangeBound,
) -> c_int {
    let mut part_index: c_int;

    Assert!((*pbi(bi)).strategy == b'r' as c_char);

    loop {
        part_index = get_range_partition_internal(bi, lb_pos, lb, ub);
        if part_index == -1 {
            return -1;
        }
        if !is_dummy_partition(rel, part_index) {
            break;
        }
    }

    part_index
}

unsafe fn get_range_partition_internal(
    bi: PartitionBoundInfo,
    lb_pos: *mut c_int,
    lb: *mut PartitionRangeBound,
    ub: *mut PartitionRangeBound,
) -> c_int {
    let bi = pbi(bi);

    /* Return the index as -1 if we've exhausted all lower bounds. */
    if *lb_pos >= (*bi).ndatums {
        return -1;
    }

    /* A lower bound should have at least one more bound after it. */
    Assert!(*lb_pos + 1 < (*bi).ndatums);

    /* Set the lower bound. */
    (*lb).index = *(*bi).indexes.offset(*lb_pos as isize);
    (*lb).datums = *(*bi).datums.offset(*lb_pos as isize);
    (*lb).kind = *(*bi).kind.offset(*lb_pos as isize);
    (*lb).lower = true;
    /* Set the upper bound. */
    (*ub).index = *(*bi).indexes.offset((*lb_pos + 1) as isize);
    (*ub).datums = *(*bi).datums.offset((*lb_pos + 1) as isize);
    (*ub).kind = *(*bi).kind.offset((*lb_pos + 1) as isize);
    (*ub).lower = false;

    /* The index assigned to an upper bound should be valid. */
    Assert!((*ub).index >= 0);

    /*
     * Advance the position to the next lower bound.  If there are no bounds
     * left beyond the upper bound, we have reached the last lower bound.
     */
    if *lb_pos + 2 >= (*bi).ndatums {
        *lb_pos = (*bi).ndatums;
    } else {
        /*
         * If the index assigned to the bound next to the upper bound isn't
         * valid, that is the next lower bound; else, the upper bound is also
         * the lower bound of the next range partition.
         */
        if *(*bi).indexes.offset((*lb_pos + 2) as isize) < 0 {
            *lb_pos = *lb_pos + 2;
        } else {
            *lb_pos = *lb_pos + 1;
        }
    }

    (*ub).index
}

/*
 * compare_range_partitions
 *		Compare the bounds of two range partitions, and return true if the
 *		two partitions overlap, false otherwise
 *
 * *lb_cmpval is set to -1, 0, or 1 if the outer partition's lower bound is
 * lower than, equal to, or higher than the inner partition's lower bound
 * respectively.  Likewise, *ub_cmpval is set to -1, 0, or 1 if the outer
 * partition's upper bound is lower than, equal to, or higher than the inner
 * partition's upper bound respectively.
 */
unsafe fn compare_range_partitions(
    partnatts: c_int,
    partsupfuncs: *mut FmgrInfo,
    partcollations: *mut Oid,
    outer_lb: *const PartitionRangeBound,
    outer_ub: *const PartitionRangeBound,
    inner_lb: *const PartitionRangeBound,
    inner_ub: *const PartitionRangeBound,
    lb_cmpval: *mut c_int,
    ub_cmpval: *mut c_int,
) -> bool {
    /*
     * Check if the outer partition's upper bound is lower than the inner
     * partition's lower bound; if so the partitions aren't overlapping.
     */
    if compare_range_bounds!(partnatts, partsupfuncs, partcollations, outer_ub, inner_lb) < 0 {
        *lb_cmpval = -1;
        *ub_cmpval = -1;
        return false;
    }

    /*
     * Check if the outer partition's lower bound is higher than the inner
     * partition's upper bound; if so the partitions aren't overlapping.
     */
    if compare_range_bounds!(partnatts, partsupfuncs, partcollations, outer_lb, inner_ub) > 0 {
        *lb_cmpval = 1;
        *ub_cmpval = 1;
        return false;
    }

    /* All other cases indicate overlapping partitions. */
    *lb_cmpval = compare_range_bounds!(partnatts, partsupfuncs, partcollations, outer_lb, inner_lb);
    *ub_cmpval = compare_range_bounds!(partnatts, partsupfuncs, partcollations, outer_ub, inner_ub);
    true
}

/*
 * get_merged_range_bounds
 *		Given the bounds of range partitions to be joined, determine the bounds
 *		of a merged partition produced from the range partitions
 *
 * *merged_lb and *merged_ub are set to the lower and upper bounds of the
 * merged partition.
 */
unsafe fn get_merged_range_bounds(
    partnatts: c_int,
    partsupfuncs: *mut FmgrInfo,
    partcollations: *mut Oid,
    jointype: JoinType,
    outer_lb: *const PartitionRangeBound,
    outer_ub: *const PartitionRangeBound,
    inner_lb: *const PartitionRangeBound,
    inner_ub: *const PartitionRangeBound,
    lb_cmpval: c_int,
    ub_cmpval: c_int,
    merged_lb: *mut PartitionRangeBound,
    merged_ub: *mut PartitionRangeBound,
) {
    Assert!(compare_range_bounds!(partnatts, partsupfuncs, partcollations, outer_lb, inner_lb) == lb_cmpval);
    Assert!(compare_range_bounds!(partnatts, partsupfuncs, partcollations, outer_ub, inner_ub) == ub_cmpval);

    match jointype {
        JOIN_INNER | JOIN_SEMI => {
            /*
             * An INNER/SEMI join will have the rows that fit both sides, so
             * the lower bound of the merged partition will be the higher of
             * the two lower bounds, and the upper bound of the merged
             * partition will be the lower of the two upper bounds.
             */
            *merged_lb = if lb_cmpval > 0 {
                ptr::read(outer_lb)
            } else {
                ptr::read(inner_lb)
            };
            *merged_ub = if ub_cmpval < 0 {
                ptr::read(outer_ub)
            } else {
                ptr::read(inner_ub)
            };
        }
        JOIN_LEFT | JOIN_ANTI => {
            /*
             * A LEFT/ANTI join will have all the rows from the outer side, so
             * the bounds of the merged partition will be the same as the
             * outer bounds.
             */
            *merged_lb = ptr::read(outer_lb);
            *merged_ub = ptr::read(outer_ub);
        }
        JOIN_FULL => {
            /*
             * A FULL join will have all the rows from both sides, so the
             * lower bound of the merged partition will be the lower of the
             * two lower bounds, and the upper bound of the merged partition
             * will be the higher of the two upper bounds.
             */
            *merged_lb = if lb_cmpval < 0 {
                ptr::read(outer_lb)
            } else {
                ptr::read(inner_lb)
            };
            *merged_ub = if ub_cmpval > 0 {
                ptr::read(outer_ub)
            } else {
                ptr::read(inner_ub)
            };
        }
        _ => {
            elog!(ERROR, "unrecognized join type: {}", jointype as c_int);
        }
    }
}

/*
 * add_merged_range_bounds
 *		Add the bounds of a merged partition to the lists of range bounds
 */
unsafe fn add_merged_range_bounds(
    partnatts: c_int,
    partsupfuncs: *mut FmgrInfo,
    partcollations: *mut Oid,
    merged_lb: *const PartitionRangeBound,
    merged_ub: *const PartitionRangeBound,
    merged_index: c_int,
    merged_datums: *mut *mut List,
    merged_kinds: *mut *mut List,
    merged_indexes: *mut *mut List,
) {
    let cmpval: i32;

    if (*merged_datums).is_null() {
        /* First merged partition */
        Assert!((*merged_kinds).is_null());
        Assert!((*merged_indexes).is_null());
        cmpval = 1;
    } else {
        let mut prev_ub: PartitionRangeBound = core::mem::zeroed();

        Assert!(!(*merged_datums).is_null());
        Assert!(!(*merged_kinds).is_null());
        Assert!(!(*merged_indexes).is_null());

        /* Get the last upper bound. */
        prev_ub.index = llast_int(*merged_indexes);
        prev_ub.datums = llast(*merged_datums) as *mut Datum;
        prev_ub.kind = llast(*merged_kinds) as *mut PartitionRangeDatumKind;
        prev_ub.lower = false;

        /*
         * We pass lower1 = false to partition_rbound_cmp() to prevent it from
         * considering the last upper bound to be smaller than the lower bound
         * of the merged partition when the values of the two range bounds
         * compare equal.
         */
        cmpval = partition_rbound_cmp(
            partnatts,
            partsupfuncs,
            partcollations,
            (*merged_lb).datums,
            (*merged_lb).kind,
            false,
            &prev_ub,
        );
        Assert!(cmpval >= 0);
    }

    /*
     * If the lower bound is higher than the last upper bound, add the lower
     * bound with the index as -1 indicating that that is a lower bound; else,
     * the last upper bound will be reused as the lower bound of the merged
     * partition, so skip this.
     */
    if cmpval > 0 {
        *merged_datums = lappend(*merged_datums, (*merged_lb).datums as *mut c_void);
        *merged_kinds = lappend(*merged_kinds, (*merged_lb).kind as *mut c_void);
        *merged_indexes = lappend_int(*merged_indexes, -1);
    }

    /* Add the upper bound and index of the merged partition. */
    *merged_datums = lappend(*merged_datums, (*merged_ub).datums as *mut c_void);
    *merged_kinds = lappend(*merged_kinds, (*merged_ub).kind as *mut c_void);
    *merged_indexes = lappend_int(*merged_indexes, merged_index);
}

/*
 * partitions_are_ordered
 *		Determine whether the partitions described by 'boundinfo' are ordered,
 *		that is partitions appearing earlier in the PartitionDesc sequence
 *		contain partition keys strictly less than those appearing later.
 *		Also, if NULL values are possible, they must come in the last
 *		partition defined in the PartitionDesc.  'live_parts' marks which
 *		partitions we should include when checking the ordering.  Partitions
 *		that do not appear in 'live_parts' are ignored.
 *
 * If out of order, or there is insufficient info to know the order,
 * then we return false.
 */
pub unsafe fn partitions_are_ordered(
    boundinfo: PartitionBoundInfo,
    live_parts: *mut Bitmapset,
) -> bool {
    let bi = pbi(boundinfo);
    Assert!(!boundinfo.is_null());

    match (*bi).strategy as u8 as char {
        'r' /* PARTITION_STRATEGY_RANGE */ => {
            /*
             * RANGE-type partitioning guarantees that the partitions can be
             * scanned in the order that they're defined in the PartitionDesc
             * to provide sequential, non-overlapping ranges of tuples.
             * However, if a DEFAULT partition exists and it's contained
             * within live_parts, then the partitions are not ordered.
             */
            if !partition_bound_has_default(boundinfo)
                || !bms_is_member((*bi).default_index, live_parts)
            {
                return true;
            }
        }
        'l' /* PARTITION_STRATEGY_LIST */ => {
            /*
             * LIST partitioned are ordered providing none of live_parts
             * overlap with the partitioned table's interleaved partitions.
             */
            if !bms_overlap(live_parts, (*bi).interleaved_parts) {
                return true;
            }
        }
        'h' /* PARTITION_STRATEGY_HASH */ => {}
        _ => {}
    }

    false
}

/*
 * check_new_partition_bound
 *
 * Checks if the new partition's bound overlaps any of the existing partitions
 * of parent.  Also performs additional checks as necessary per strategy.
 */
pub unsafe fn check_new_partition_bound(
    relname: *mut c_char,
    parent: Relation,
    spec: *mut PartitionBoundSpec,
    pstate: *mut ParseState,
) {
    let key: PartitionKey = RelationGetPartitionKey(parent);
    let partdesc: PartitionDesc = RelationGetPartitionDesc(parent, false);
    let boundinfo = pbi((*partdesc).boundinfo);
    let mut with: c_int = -1;
    let mut overlap: bool = false;
    let mut overlap_location: c_int = -1;

    if (*spec).is_default {
        /*
         * The default partition bound never conflicts with any other
         * partition's; if that's what we're attaching, the only possible
         * problem is that one already exists, so check for that and we're
         * done.
         */
        if (*partdesc).boundinfo.is_null() || !partition_bound_has_default((*partdesc).boundinfo) {
            return;
        }

        /* Default partition already exists, error out. */
        ereport!(
            ERROR,
            errmsg!(
                "partition \"{}\" conflicts with existing default partition \"{}\"",
                std::ffi::CStr::from_ptr(relname).to_string_lossy(),
                std::ffi::CStr::from_ptr(get_rel_name(
                    *(*partdesc).oids.offset((*boundinfo).default_index as isize)
                ))
                .to_string_lossy()
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           parser_errposition(pstate, spec->location) */
            )
        );
    }

    match (*key).strategy {
        PartitionStrategy::PARTITION_STRATEGY_HASH => {
            Assert!((*spec).strategy == b'h' as c_char);
            Assert!((*spec).remainder >= 0 && (*spec).remainder < (*spec).modulus);

            if (*partdesc).nparts > 0 {
                let greatest_modulus: c_int;
                let mut remainder: c_int;
                let offset: c_int;

                /*
                 * Check rule that every modulus must be a factor of the
                 * next larger modulus.
                 */

                /*
                 * Get the greatest (modulus, remainder) pair contained in
                 * boundinfo->datums that is less than or equal to the
                 * (spec->modulus, spec->remainder) pair.
                 */
                offset = partition_hash_bsearch((*partdesc).boundinfo, (*spec).modulus, (*spec).remainder);
                if offset < 0 {
                    let next_modulus: c_int;

                    /*
                     * All existing moduli are greater or equal, so the
                     * new one must be a factor of the smallest one, which
                     * is first in the boundinfo.
                     */
                    next_modulus = DatumGetInt32(
                        *(*(*boundinfo).datums.offset(0)).offset(0),
                    );
                    if next_modulus % (*spec).modulus != 0 {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "every hash partition modulus must be a factor of the next larger modulus"
                                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errdetail: The new modulus %d is not a factor of %d... */
                            )
                        );
                    }
                } else {
                    let prev_modulus: c_int;

                    /*
                     * We found the largest (modulus, remainder) pair less
                     * than or equal to the new one.  That modulus must be
                     * a divisor of, or equal to, the new modulus.
                     */
                    prev_modulus = DatumGetInt32(
                        *(*(*boundinfo).datums.offset(offset as isize)).offset(0),
                    );

                    if (*spec).modulus % prev_modulus != 0 {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "every hash partition modulus must be a factor of the next larger modulus"
                                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errdetail: The new modulus %d is not divisible by %d... */
                            )
                        );
                    }

                    if offset + 1 < (*boundinfo).ndatums {
                        let next_modulus: c_int;

                        /*
                         * Look at the next higher (modulus, remainder)
                         * pair.
                         */
                        next_modulus = DatumGetInt32(
                            *(*(*boundinfo).datums.offset((offset + 1) as isize)).offset(0),
                        );

                        if next_modulus % (*spec).modulus != 0 {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "every hash partition modulus must be a factor of the next larger modulus"
                                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                       errdetail: The new modulus %d is not a factor of %d... */
                                )
                            );
                        }
                    }
                }

                greatest_modulus = (*boundinfo).nindexes;
                remainder = (*spec).remainder;

                /*
                 * Normally, the lowest remainder that could conflict with
                 * the new partition is equal to the remainder specified
                 * for the new partition, but when the new partition has a
                 * modulus higher than any used so far, we need to adjust.
                 */
                if remainder >= greatest_modulus {
                    remainder = remainder % greatest_modulus;
                }

                /* Check every potentially-conflicting remainder. */
                loop {
                    if *(*boundinfo).indexes.offset(remainder as isize) != -1 {
                        overlap = true;
                        overlap_location = (*spec).location;
                        with = *(*boundinfo).indexes.offset(remainder as isize);
                        break;
                    }
                    remainder += (*spec).modulus;
                    if remainder >= greatest_modulus {
                        break;
                    }
                }
            }
        }

        PartitionStrategy::PARTITION_STRATEGY_LIST => {
            Assert!((*spec).strategy == b'l' as c_char);

            if (*partdesc).nparts > 0 {
                Assert!(
                    !(*partdesc).boundinfo.is_null()
                        && (*boundinfo).strategy == b'l' as c_char
                        && ((*boundinfo).ndatums > 0
                            || partition_bound_accepts_nulls((*partdesc).boundinfo)
                            || partition_bound_has_default((*partdesc).boundinfo))
                );

                foreach!(cell, (*spec).listdatums, {
                    let val: *mut Const =
                        lfirst_node!(Const, T_Const,
                                     crate::current_cell!(cell));

                    overlap_location = (*val).location;
                    if !(*val).constisnull {
                        let offset: c_int;
                        let mut equal: bool = false;

                        offset = partition_list_bsearch(
                            (*key).partsupfunc,
                            (*key).partcollation,
                            (*partdesc).boundinfo,
                            (*val).constvalue,
                            &mut equal,
                        );
                        if offset >= 0 && equal {
                            overlap = true;
                            with = *(*boundinfo).indexes.offset(offset as isize);
                            break;
                        }
                    } else if partition_bound_accepts_nulls((*partdesc).boundinfo) {
                        overlap = true;
                        with = (*boundinfo).null_index;
                        break;
                    }
                });
            }
        }

        PartitionStrategy::PARTITION_STRATEGY_RANGE => {
            let lower: *mut PartitionRangeBound;
            let upper: *mut PartitionRangeBound;
            let mut cmpval: i32;

            Assert!((*spec).strategy == b'r' as c_char);
            lower = make_one_partition_rbound(key, -1, (*spec).lowerdatums, true);
            upper = make_one_partition_rbound(key, -1, (*spec).upperdatums, false);

            /*
             * First check if the resulting range would be empty with
             * specified lower and upper bounds.  partition_rbound_cmp
             * cannot return zero here, since the lower-bound flags are
             * different.
             */
            cmpval = partition_rbound_cmp(
                (*key).partnatts as c_int,
                (*key).partsupfunc,
                (*key).partcollation,
                (*lower).datums,
                (*lower).kind,
                true,
                upper,
            );
            Assert!(cmpval != 0);
            if cmpval > 0 {
                /* Point to problematic key in the lower datums list. */
                let datum: *mut PartitionRangeDatum =
                    list_nth((*spec).lowerdatums, cmpval - 1) as *mut PartitionRangeDatum;

                ereport!(
                    ERROR,
                    errmsg!(
                        "empty range bound specified for partition \"{}\"",
                        std::ffi::CStr::from_ptr(relname).to_string_lossy()
                        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errdetail: Specified lower bound %s is >= upper bound %s.
                           parser_errposition(pstate, datum->location) */
                    )
                );
            }

            if (*partdesc).nparts > 0 {
                let offset: c_int;

                Assert!(
                    !(*partdesc).boundinfo.is_null()
                        && (*boundinfo).strategy == b'r' as c_char
                        && ((*boundinfo).ndatums > 0
                            || partition_bound_has_default((*partdesc).boundinfo))
                );

                /*
                 * Test whether the new lower bound (which is treated
                 * inclusively as part of the new partition) lies inside
                 * an existing partition, or in a gap.
                 */
                offset = partition_range_bsearch(
                    (*key).partnatts as c_int,
                    (*key).partsupfunc,
                    (*key).partcollation,
                    (*partdesc).boundinfo,
                    lower,
                    &mut cmpval,
                );

                if *(*boundinfo).indexes.offset((offset + 1) as isize) < 0 {
                    /*
                     * Check that the new partition will fit in the gap.
                     * For it to fit, the new upper bound must be less
                     * than or equal to the lower bound of the next
                     * partition, if there is one.
                     */
                    if offset + 1 < (*boundinfo).ndatums {
                        let datums: *mut Datum =
                            *(*boundinfo).datums.offset((offset + 1) as isize);
                        let kind: *mut PartitionRangeDatumKind =
                            *(*boundinfo).kind.offset((offset + 1) as isize);
                        let is_lower: bool =
                            *(*boundinfo).indexes.offset((offset + 1) as isize) == -1;

                        cmpval = partition_rbound_cmp(
                            (*key).partnatts as c_int,
                            (*key).partsupfunc,
                            (*key).partcollation,
                            datums,
                            kind,
                            is_lower,
                            upper,
                        );
                        if cmpval < 0 {
                            /*
                             * Point to problematic key in the upper
                             * datums list.
                             */
                            let datum: *mut PartitionRangeDatum =
                                list_nth((*spec).upperdatums, (cmpval.abs() - 1) as c_int)
                                    as *mut PartitionRangeDatum;

                            /*
                             * The new partition overlaps with the
                             * existing partition between offset + 1 and
                             * offset + 2.
                             */
                            overlap = true;
                            overlap_location = (*datum).location;
                            with = *(*boundinfo).indexes.offset((offset + 2) as isize);
                        }
                    }
                } else {
                    /*
                     * The new partition overlaps with the existing
                     * partition between offset and offset + 1.
                     */
                    let datum: *mut PartitionRangeDatum = if cmpval == 0 {
                        linitial((*spec).lowerdatums) as *mut PartitionRangeDatum
                    } else {
                        list_nth((*spec).lowerdatums, (cmpval.abs() - 1) as c_int)
                            as *mut PartitionRangeDatum
                    };
                    overlap = true;
                    overlap_location = (*datum).location;
                    with = *(*boundinfo).indexes.offset((offset + 1) as isize);
                }
            }
        }
    }

    if overlap {
        Assert!(with >= 0);
        ereport!(
            ERROR,
            errmsg!(
                "partition \"{}\" would overlap partition \"{}\"",
                std::ffi::CStr::from_ptr(relname).to_string_lossy(),
                std::ffi::CStr::from_ptr(get_rel_name(*(*partdesc).oids.offset(with as isize)))
                    .to_string_lossy()
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           parser_errposition(pstate, overlap_location) */
            )
        );
    }
}

/*
 * check_default_partition_contents
 *
 * This function checks if there exists a row in the default partition that
 * would properly belong to the new partition being added.  If it finds one,
 * it throws an error.
 */
pub unsafe fn check_default_partition_contents(
    parent: Relation,
    default_rel: Relation,
    new_spec: *mut PartitionBoundSpec,
) {
    let new_part_constraints: *mut List;
    let def_part_constraints: *mut List;
    let all_parts: *mut List;

    new_part_constraints = if (*new_spec).strategy == b'l' as c_char {
        get_qual_for_list(parent, new_spec)
    } else {
        get_qual_for_range(parent, new_spec, false)
    };
    def_part_constraints = get_proposed_default_constraint(new_part_constraints);

    /*
     * Map the Vars in the constraint expression from parent's attnos to
     * default_rel's.
     */
    let def_part_constraints: *mut List =
        map_partition_varattnos(def_part_constraints, 1, default_rel, parent);

    /*
     * If the existing constraints on the default partition imply that it will
     * not contain any row that would belong to the new partition, we can
     * avoid scanning the default partition.
     */
    if PartConstraintImpliedByRelConstraint(default_rel, def_part_constraints) {
        ereport!(
            DEBUG1,
            errmsg!(
                "updated partition constraint for default partition \"{}\" is implied by existing constraints",
                std::ffi::CStr::from_ptr(RelationGetRelationName(default_rel)).to_string_lossy()
            )
        );
        return;
    }

    /*
     * Scan the default partition and its subpartitions, and check for rows
     * that do not satisfy the revised partition constraints.
     */
    if (*(*default_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        all_parts = find_all_inheritors(
            RelationGetRelid(default_rel),
            AccessExclusiveLock,
            ptr::null_mut(),
        );
    } else {
        all_parts = list_make1_oid!(RelationGetRelid(default_rel));
    }

    foreach!(lc, all_parts, {
        let part_relid: Oid = lfirst_oid(crate::current_cell!(lc));
        let part_rel: Relation;
        let partition_constraint: *mut Expr;
        let estate: *mut EState;
        let mut partqualstate: *mut ExprState = ptr::null_mut();
        let snapshot: Snapshot;
        let econtext: *mut ExprContext;
        let scan: TableScanDesc;
        let oldcxt: MemoryContext;
        let tupslot: *mut TupleTableSlot;

        /* Lock already taken above. */
        if part_relid != RelationGetRelid(default_rel) {
            part_rel = table_open(part_relid, NoLock);

            /*
             * Map the Vars in the constraint expression from default_rel's
             * the sub-partition's.
             */
            partition_constraint = make_ands_explicit(def_part_constraints);
            let partition_constraint: *mut Expr =
                map_partition_varattnos(
                    partition_constraint as *mut List, 1, part_rel, default_rel,
                ) as *mut Expr;

            /*
             * If the partition constraints on default partition child imply
             * that it will not contain any row that would belong to the new
             * partition, we can avoid scanning the child table.
             */
            if PartConstraintImpliedByRelConstraint(part_rel, def_part_constraints) {
                ereport!(
                    DEBUG1,
                    errmsg!(
                        "updated partition constraint for default partition \"{}\" is implied by existing constraints",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(part_rel)).to_string_lossy()
                    )
                );

                table_close(part_rel, NoLock);
                continue;
            }
        } else {
            part_rel = default_rel;
            partition_constraint = make_ands_explicit(def_part_constraints);
        }

        /*
         * Only RELKIND_RELATION relations (i.e. leaf partitions) need to be
         * scanned.
         */
        if (*(*part_rel).rd_rel).relkind != RELKIND_RELATION {
            if (*(*part_rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
                ereport!(
                    WARNING,
                    errmsg!(
                        "skipped scanning foreign table \"{}\" which is a partition of default partition \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(part_rel)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(RelationGetRelationName(default_rel)).to_string_lossy()
                        /* C also: errcode(ERRCODE_CHECK_VIOLATION) */
                    )
                );
            }

            if RelationGetRelid(default_rel) != RelationGetRelid(part_rel) {
                table_close(part_rel, NoLock);
            }

            continue;
        }

        estate = CreateExecutorState();

        /* Build expression execution states for partition check quals */
        partqualstate = ExecPrepareExpr(partition_constraint, estate);

        econtext = GetPerTupleExprContext(estate);
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        tupslot = table_slot_create(part_rel, &mut (*estate).es_tupleTable);
        scan = table_beginscan(part_rel, snapshot, 0, ptr::null_mut());

        /*
         * Switch to per-tuple memory context and reset it for each tuple
         * produced, so we don't leak memory.
         */
        oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate) as MemoryContext);

        while table_scan_getnextslot(scan, ForwardScanDirection, tupslot) {
            (*econtext).ecxt_scantuple = tupslot;

            if !ExecCheck(partqualstate, econtext) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "updated partition constraint for default partition \"{}\" would be violated by some row",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(default_rel)).to_string_lossy()
                        /* C also: errcode(ERRCODE_CHECK_VIOLATION), errtable(default_rel) */
                    )
                );
            }

            ResetExprContext(econtext);
            CHECK_FOR_INTERRUPTS!();
        }

        MemoryContextSwitchTo(oldcxt);
        table_endscan(scan);
        UnregisterSnapshot(snapshot);
        ExecDropSingleTupleTableSlot(tupslot);
        FreeExecutorState(estate);

        if RelationGetRelid(default_rel) != RelationGetRelid(part_rel) {
            table_close(part_rel, NoLock); /* keep the lock until commit */
        }
    });
}

/*
 * get_hash_partition_greatest_modulus
 *
 * Returns the greatest modulus of the hash partition bound.
 * This is no longer used in the core code, but we keep it around
 * in case external modules are using it.
 */
pub unsafe fn get_hash_partition_greatest_modulus(bound: PartitionBoundInfo) -> c_int {
    Assert!(!bound.is_null() && (*pbi(bound)).strategy == b'h' as c_char);
    (*pbi(bound)).nindexes
}

/*
 * make_one_partition_rbound
 *
 * Return a PartitionRangeBound given a list of PartitionRangeDatum elements
 * and a flag telling whether the bound is lower or not.  Made into a function
 * because there are multiple sites that want to use this facility.
 */
unsafe fn make_one_partition_rbound(
    key: PartitionKey,
    index: c_int,
    datums: *mut List,
    lower: bool,
) -> *mut PartitionRangeBound {
    let bound: *mut PartitionRangeBound;
    let mut i: c_int;

    Assert!(datums != NIL);

    bound = palloc0(core::mem::size_of::<PartitionRangeBound>()) as *mut PartitionRangeBound;
    (*bound).index = index;
    (*bound).datums = palloc0((*key).partnatts as usize * core::mem::size_of::<Datum>())
        as *mut Datum;
    (*bound).kind = palloc0(
        (*key).partnatts as usize * core::mem::size_of::<PartitionRangeDatumKind>(),
    ) as *mut PartitionRangeDatumKind;
    (*bound).lower = lower;

    i = 0;
    foreach!(lc, datums, {
        let datum: *mut PartitionRangeDatum =
            lfirst_node!(PartitionRangeDatum, T_PartitionRangeDatum,
                         crate::current_cell!(lc));

        /* What's contained in this range datum? */
        *(*bound).kind.offset(i as isize) = (*datum).kind;

        if (*datum).kind == PARTITION_RANGE_DATUM_VALUE {
            let val: *mut Const =
                castNode!(Const, T_Const, (*datum).value);

            if (*val).constisnull {
                elog!(ERROR, "invalid range bound datum");
            }
            *(*bound).datums.offset(i as isize) = (*val).constvalue;
        }

        i += 1;
    });

    bound
}

// ---------------------------------------------------------------------------
// Part 5: partition_rbound_cmp, partition_rbound_datum_cmp,
//         partition_hbound_cmp, partition_list_bsearch,
//         partition_range_bsearch, partition_range_datum_bsearch,
//         partition_hash_bsearch, qsort comparators,
//         create_hash_bounds, get_non_null_list_datum_count,
//         create_list_bounds, create_range_bounds
// ---------------------------------------------------------------------------

/*
 * partition_rbound_cmp
 *
 * For two range bounds this decides whether the 1st one (specified by
 * datums1, kind1, and lower1) is <, =, or > the bound specified in *b2.
 *
 * 0 is returned if they are equal, otherwise a non-zero integer whose sign
 * indicates the ordering, and whose absolute value gives the 1-based
 * partition key number of the first mismatching column.
 */
pub unsafe fn partition_rbound_cmp(
    partnatts: c_int,
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    datums1: *mut Datum,
    kind1: *mut PartitionRangeDatumKind,
    lower1: bool,
    b2: *const PartitionRangeBound,
) -> i32 {
    let mut colnum: i32 = 0;
    let mut cmpval: i32 = 0; /* placate compiler */
    let mut i: c_int;
    let datums2: *mut Datum = (*b2).datums;
    let kind2: *mut PartitionRangeDatumKind = (*b2).kind;
    let lower2: bool = (*b2).lower;

    i = 0;
    while i < partnatts {
        /* Track column number in case we need it for result */
        colnum += 1;

        /*
         * First, handle cases where the column is unbounded, which should not
         * invoke the comparison procedure, and should not consider any later
         * columns. Note that the PartitionRangeDatumKind enum elements
         * compare the same way as the values they represent.
         */
        if (*kind1.offset(i as isize) as i32) < (*kind2.offset(i as isize) as i32) {
            return -colnum;
        } else if (*kind1.offset(i as isize) as i32) > (*kind2.offset(i as isize) as i32) {
            return colnum;
        } else if *kind1.offset(i as isize) != PARTITION_RANGE_DATUM_VALUE {
            /*
             * The column bounds are both MINVALUE or both MAXVALUE. No later
             * columns should be considered, but we still need to compare
             * whether they are upper or lower bounds.
             */
            break;
        }

        cmpval = DatumGetInt32(FunctionCall2Coll(
            &mut *partsupfunc.offset(i as isize),
            *partcollation.offset(i as isize),
            *datums1.offset(i as isize),
            *datums2.offset(i as isize),
        ));
        if cmpval != 0 {
            break;
        }
        i += 1;
    }

    /*
     * If the comparison is anything other than equal, we're done. If they
     * compare equal though, we still have to consider whether the boundaries
     * are inclusive or exclusive.  Exclusive one is considered smaller of the
     * two.
     */
    if cmpval == 0 && lower1 != lower2 {
        cmpval = if lower1 { 1 } else { -1 };
    }

    if cmpval == 0 {
        0
    } else if cmpval < 0 {
        -colnum
    } else {
        colnum
    }
}

/*
 * partition_rbound_datum_cmp
 *
 * Return whether range bound (specified in rb_datums and rb_kind)
 * is <, =, or > partition key of tuple (tuple_datums)
 *
 * n_tuple_datums, partsupfunc and partcollation give number of attributes in
 * the bounds to be compared, comparison function to be used and the collations
 * of attributes resp.
 */
pub unsafe fn partition_rbound_datum_cmp(
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    rb_datums: *mut Datum,
    rb_kind: *mut PartitionRangeDatumKind,
    tuple_datums: *mut Datum,
    n_tuple_datums: c_int,
) -> i32 {
    let mut i: c_int;
    let mut cmpval: i32 = -1;

    i = 0;
    while i < n_tuple_datums {
        if *rb_kind.offset(i as isize) == PARTITION_RANGE_DATUM_MINVALUE {
            return -1;
        } else if *rb_kind.offset(i as isize) == PARTITION_RANGE_DATUM_MAXVALUE {
            return 1;
        }

        cmpval = DatumGetInt32(FunctionCall2Coll(
            &mut *partsupfunc.offset(i as isize),
            *partcollation.offset(i as isize),
            *rb_datums.offset(i as isize),
            *tuple_datums.offset(i as isize),
        ));
        if cmpval != 0 {
            break;
        }
        i += 1;
    }

    cmpval
}

/*
 * partition_hbound_cmp
 *
 * Compares modulus first, then remainder if modulus is equal.
 */
unsafe fn partition_hbound_cmp(
    modulus1: c_int,
    remainder1: c_int,
    modulus2: c_int,
    remainder2: c_int,
) -> i32 {
    if modulus1 < modulus2 {
        return -1;
    }
    if modulus1 > modulus2 {
        return 1;
    }
    if modulus1 == modulus2 && remainder1 != remainder2 {
        return if remainder1 > remainder2 { 1 } else { -1 };
    }
    0
}

/*
 * partition_list_bsearch
 *		Returns the index of the greatest bound datum that is less than equal
 * 		to the given value or -1 if all of the bound datums are greater
 *
 * *is_equal is set to true if the bound datum at the returned index is equal
 * to the input value.
 */
pub unsafe fn partition_list_bsearch(
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    boundinfo: PartitionBoundInfo,
    value: Datum,
    is_equal: *mut bool,
) -> c_int {
    let bi = pbi(boundinfo);
    let mut lo: c_int = -1;
    let mut hi: c_int = (*bi).ndatums - 1;

    while lo < hi {
        let cmpval: i32;

        let mid = (lo + hi + 1) / 2;
        cmpval = DatumGetInt32(FunctionCall2Coll(
            &mut *partsupfunc.offset(0),
            *partcollation.offset(0),
            *(*(*bi).datums.offset(mid as isize)).offset(0),
            value,
        ));
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    lo
}

/*
 * partition_range_bsearch
 *		Returns the index of the greatest range bound that is less than or
 *		equal to the given range bound or -1 if all of the range bounds are
 *		greater
 *
 * Upon return from this function, *cmpval is set to 0 if the bound at the
 * returned index matches the input range bound exactly, otherwise a
 * non-zero integer whose sign indicates the ordering, and whose absolute
 * value gives the 1-based partition key number of the first mismatching
 * column.
 */
unsafe fn partition_range_bsearch(
    partnatts: c_int,
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    boundinfo: PartitionBoundInfo,
    probe: *const PartitionRangeBound,
    cmpval: *mut i32,
) -> c_int {
    let bi = pbi(boundinfo);
    let mut lo: c_int = -1;
    let mut hi: c_int = (*bi).ndatums - 1;

    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        *cmpval = partition_rbound_cmp(
            partnatts,
            partsupfunc,
            partcollation,
            *(*bi).datums.offset(mid as isize),
            *(*bi).kind.offset(mid as isize),
            *(*bi).indexes.offset(mid as isize) == -1,
            probe,
        );
        if *cmpval <= 0 {
            lo = mid;
            if *cmpval == 0 {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    lo
}

/*
 * partition_range_datum_bsearch
 *		Returns the index of the greatest range bound that is less than or
 *		equal to the given tuple or -1 if all of the range bounds are greater
 *
 * *is_equal is set to true if the range bound at the returned index is equal
 * to the input tuple.
 */
pub unsafe fn partition_range_datum_bsearch(
    partsupfunc: *mut FmgrInfo,
    partcollation: *mut Oid,
    boundinfo: PartitionBoundInfo,
    nvalues: c_int,
    values: *mut Datum,
    is_equal: *mut bool,
) -> c_int {
    let bi = pbi(boundinfo);
    let mut lo: c_int = -1;
    let mut hi: c_int = (*bi).ndatums - 1;

    while lo < hi {
        let cmpval: i32;

        let mid = (lo + hi + 1) / 2;
        cmpval = partition_rbound_datum_cmp(
            partsupfunc,
            partcollation,
            *(*bi).datums.offset(mid as isize),
            *(*bi).kind.offset(mid as isize),
            values,
            nvalues,
        );
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    lo
}

/*
 * partition_hash_bsearch
 *		Returns the index of the greatest (modulus, remainder) pair that is
 *		less than or equal to the given (modulus, remainder) pair or -1 if
 *		all of them are greater
 */
pub unsafe fn partition_hash_bsearch(
    boundinfo: PartitionBoundInfo,
    modulus: c_int,
    remainder: c_int,
) -> c_int {
    let bi = pbi(boundinfo);
    let mut lo: c_int = -1;
    let mut hi: c_int = (*bi).ndatums - 1;

    while lo < hi {
        let cmpval: i32;
        let bound_modulus: i32;
        let bound_remainder: i32;

        let mid = (lo + hi + 1) / 2;
        bound_modulus = DatumGetInt32(*(*(*bi).datums.offset(mid as isize)).offset(0));
        bound_remainder = DatumGetInt32(*(*(*bi).datums.offset(mid as isize)).offset(1));
        cmpval = partition_hbound_cmp(bound_modulus, bound_remainder, modulus, remainder);
        if cmpval <= 0 {
            lo = mid;
            if cmpval == 0 {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    lo
}

/*
 * qsort_partition_hbound_cmp
 *
 * Hash bounds are sorted by modulus, then by remainder.
 */
unsafe extern "C" fn qsort_partition_hbound_cmp(a: *const c_void, b: *const c_void) -> i32 {
    let h1 = &*(a as *const PartitionHashBound);
    let h2 = &*(b as *const PartitionHashBound);

    partition_hbound_cmp(h1.modulus, h1.remainder, h2.modulus, h2.remainder)
}

/*
 * qsort_partition_list_value_cmp
 *
 * Compare two list partition bound datums.
 */
unsafe extern "C" fn qsort_partition_list_value_cmp(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> i32 {
    let val1 = (*(a as *const PartitionListValue)).value;
    let val2 = (*(b as *const PartitionListValue)).value;
    let key = arg as PartitionKey;

    DatumGetInt32(FunctionCall2Coll(
        &mut *(*key).partsupfunc.offset(0),
        *(*key).partcollation.offset(0),
        val1,
        val2,
    ))
}

/*
 * qsort_partition_rbound_cmp
 *
 * Used when sorting range bounds across all range partitions.
 */
unsafe extern "C" fn qsort_partition_rbound_cmp(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> i32 {
    let b1 = *(a as *const *const PartitionRangeBound);
    let b2 = *(b as *const *const PartitionRangeBound);
    let key = arg as PartitionKey;

    compare_range_bounds!(
        (*key).partnatts as c_int,
        (*key).partsupfunc,
        (*key).partcollation,
        b1,
        b2
    )
}

/*
 * create_hash_bounds
 *		Create a PartitionBoundInfo for a hash partitioned table
 */
unsafe fn create_hash_bounds(
    boundspecs: *mut *mut PartitionBoundSpec,
    nparts: c_int,
    key: PartitionKey,
    mapping: *mut *mut c_int,
) -> PartitionBoundInfo {
    let boundinfo: *mut PartitionBoundInfoFull;
    let hbounds: *mut PartitionHashBound;
    let mut i: c_int;
    let greatest_modulus: c_int;
    let bound_datums: *mut Datum;

    boundinfo = palloc0(core::mem::size_of::<PartitionBoundInfoFull>())
        as *mut PartitionBoundInfoFull;
    (*boundinfo).strategy = b'h' as c_char;
    /* No special hash partitions. */
    (*boundinfo).null_index = -1;
    (*boundinfo).default_index = -1;

    hbounds = palloc(nparts as usize * core::mem::size_of::<PartitionHashBound>())
        as *mut PartitionHashBound;

    /* Convert from node to the internal representation */
    i = 0;
    while i < nparts {
        let spec = *boundspecs.offset(i as isize);

        if (*spec).strategy != b'h' as c_char {
            elog!(ERROR, "invalid strategy in partition bound spec");
        }

        (*hbounds.offset(i as isize)).modulus = (*spec).modulus;
        (*hbounds.offset(i as isize)).remainder = (*spec).remainder;
        (*hbounds.offset(i as isize)).index = i;
        i += 1;
    }

    /* Sort all the bounds in ascending order */
    qsort(
        hbounds as *mut c_void,
        nparts as usize,
        core::mem::size_of::<PartitionHashBound>(),
        Some(qsort_partition_hbound_cmp),
    );

    /* After sorting, moduli are now stored in ascending order. */
    greatest_modulus = (*hbounds.offset((nparts - 1) as isize)).modulus;

    (*boundinfo).ndatums = nparts;
    (*boundinfo).datums =
        palloc0(nparts as usize * core::mem::size_of::<*mut Datum>()) as *mut *mut Datum;
    (*boundinfo).kind = ptr::null_mut();
    (*boundinfo).interleaved_parts = ptr::null_mut();
    (*boundinfo).nindexes = greatest_modulus;
    (*boundinfo).indexes =
        palloc(greatest_modulus as usize * core::mem::size_of::<c_int>()) as *mut c_int;
    i = 0;
    while i < greatest_modulus {
        *(*boundinfo).indexes.offset(i as isize) = -1;
        i += 1;
    }

    /*
     * In the loop below, to save from allocating a series of small datum
     * arrays, here we just allocate a single array and below we'll just
     * assign a portion of this array per partition.
     */
    bound_datums = palloc(nparts as usize * 2 * core::mem::size_of::<Datum>()) as *mut Datum;

    /*
     * For hash partitioning, there are as many datums (modulus and remainder
     * pairs) as there are partitions.  Indexes are simply values ranging from
     * 0 to (nparts - 1).
     */
    i = 0;
    while i < nparts {
        let modulus = (*hbounds.offset(i as isize)).modulus;
        let mut remainder = (*hbounds.offset(i as isize)).remainder;

        *(*boundinfo).datums.offset(i as isize) = bound_datums.offset((i * 2) as isize);
        *(*(*boundinfo).datums.offset(i as isize)).offset(0) = Int32GetDatum(modulus);
        *(*(*boundinfo).datums.offset(i as isize)).offset(1) = Int32GetDatum(remainder);

        while remainder < greatest_modulus {
            /* overlap? */
            Assert!(*(*boundinfo).indexes.offset(remainder as isize) == -1);
            *(*boundinfo).indexes.offset(remainder as isize) = i;
            remainder += modulus;
        }

        *(*mapping).offset((*hbounds.offset(i as isize)).index as isize) = i;
        i += 1;
    }
    pfree(hbounds as *mut c_void);

    boundinfo as PartitionBoundInfo
}

/*
 * get_non_null_list_datum_count
 * 		Counts the number of non-null Datums in each partition.
 */
unsafe fn get_non_null_list_datum_count(
    boundspecs: *mut *mut PartitionBoundSpec,
    nparts: c_int,
) -> c_int {
    let mut i: c_int;
    let mut count: c_int = 0;

    i = 0;
    while i < nparts {
        foreach!(lc, (*(*boundspecs.offset(i as isize))).listdatums, {
            let val: *const Const =
                lfirst_node!(Const, T_Const,
                             crate::current_cell!(lc));
            if !(*val).constisnull {
                count += 1;
            }
        });
        i += 1;
    }

    count
}

/*
 * create_list_bounds
 *		Create a PartitionBoundInfo for a list partitioned table
 */
unsafe fn create_list_bounds(
    boundspecs: *mut *mut PartitionBoundSpec,
    nparts: c_int,
    key: PartitionKey,
    mapping: *mut *mut c_int,
) -> PartitionBoundInfo {
    let boundinfo: *mut PartitionBoundInfoFull;
    let all_values: *mut PartitionListValue;
    let mut i: c_int;
    let mut j: c_int;
    let ndatums: c_int;
    let mut next_index: c_int = 0;
    let mut default_index: c_int = -1;
    let mut null_index: c_int = -1;
    let bound_datums: *mut Datum;

    boundinfo = palloc0(core::mem::size_of::<PartitionBoundInfoFull>())
        as *mut PartitionBoundInfoFull;
    (*boundinfo).strategy = b'l' as c_char;
    /* Will be set correctly below. */
    (*boundinfo).null_index = -1;
    (*boundinfo).default_index = -1;

    ndatums = get_non_null_list_datum_count(boundspecs, nparts);
    all_values = palloc(ndatums as usize * core::mem::size_of::<PartitionListValue>())
        as *mut PartitionListValue;

    /* Create a unified list of non-null values across all partitions. */
    j = 0;
    i = 0;
    while i < nparts {
        let spec = *boundspecs.offset(i as isize);

        if (*spec).strategy != b'l' as c_char {
            elog!(ERROR, "invalid strategy in partition bound spec");
        }

        /*
         * Note the index of the partition bound spec for the default
         * partition.  There's no datum to add to the list on non-null datums
         * for this partition.
         */
        if (*spec).is_default {
            default_index = i;
            i += 1;
            continue;
        }

        foreach!(c, (*spec).listdatums, {
            let val: *const Const =
                lfirst_node!(Const, T_Const,
                             crate::current_cell!(c));

            if !(*val).constisnull {
                (*all_values.offset(j as isize)).index = i;
                (*all_values.offset(j as isize)).value = (*val).constvalue;
                j += 1;
            } else {
                /*
                 * Never put a null into the values array; save the index of
                 * the partition that stores nulls, instead.
                 */
                if null_index != -1 {
                    elog!(ERROR, "found null more than once");
                }
                null_index = i;
            }
        });
        i += 1;
    }

    /* ensure we found a Datum for every slot in the all_values array */
    Assert!(j == ndatums);

    qsort_arg(
        all_values as *mut c_void,
        ndatums as usize,
        core::mem::size_of::<PartitionListValue>(),
        Some(qsort_partition_list_value_cmp),
        key as *mut c_void,
    );

    (*boundinfo).ndatums = ndatums;
    (*boundinfo).datums =
        palloc0(ndatums as usize * core::mem::size_of::<*mut Datum>()) as *mut *mut Datum;
    (*boundinfo).kind = ptr::null_mut();
    (*boundinfo).interleaved_parts = ptr::null_mut();
    (*boundinfo).nindexes = ndatums;
    (*boundinfo).indexes =
        palloc(ndatums as usize * core::mem::size_of::<c_int>()) as *mut c_int;

    /*
     * In the loop below, to save from allocating a series of small datum
     * arrays, here we just allocate a single array and below we'll just
     * assign a portion of this array per datum.
     */
    bound_datums = palloc(ndatums as usize * core::mem::size_of::<Datum>()) as *mut Datum;

    /*
     * Copy values.  Canonical indexes are values ranging from 0 to (nparts -
     * 1) assigned to each partition such that all datums of a given partition
     * receive the same value. The value for a given partition is the index of
     * that partition's smallest datum in the all_values[] array.
     */
    i = 0;
    while i < ndatums {
        let orig_index = (*all_values.offset(i as isize)).index;

        *(*boundinfo).datums.offset(i as isize) = bound_datums.offset(i as isize);
        *(*(*boundinfo).datums.offset(i as isize)).offset(0) = datumCopy(
            (*all_values.offset(i as isize)).value,
            *(*key).parttypbyval.offset(0),
            *(*key).parttyplen.offset(0) as c_int,
        );

        /* If the old index has no mapping, assign one */
        if *(*mapping).offset(orig_index as isize) == -1 {
            *(*mapping).offset(orig_index as isize) = next_index;
            next_index += 1;
        }

        *(*boundinfo).indexes.offset(i as isize) = *(*mapping).offset(orig_index as isize);
        i += 1;
    }

    pfree(all_values as *mut c_void);

    /*
     * Set the canonical value for null_index, if any.
     *
     * It is possible that the null-accepting partition has not been assigned
     * an index yet, which could happen if such partition accepts only null
     * and hence not handled in the above loop which only looked at non-null
     * values.
     */
    if null_index != -1 {
        Assert!(null_index >= 0);
        if *(*mapping).offset(null_index as isize) == -1 {
            *(*mapping).offset(null_index as isize) = next_index;
            next_index += 1;
        }
        (*boundinfo).null_index = *(*mapping).offset(null_index as isize);
    }

    /* Set the canonical value for default_index, if any. */
    if default_index != -1 {
        /*
         * The default partition accepts any value not specified in the lists
         * of other partitions, hence it should not get mapped index while
         * assigning those for non-null datums.
         */
        Assert!(default_index >= 0);
        Assert!(*(*mapping).offset(default_index as isize) == -1);
        *(*mapping).offset(default_index as isize) = next_index;
        next_index += 1;
        (*boundinfo).default_index = *(*mapping).offset(default_index as isize);
    }

    /*
     * Calculate interleaved partitions.  Here we look for partitions which
     * might be interleaved with other partitions and set a bit in
     * interleaved_parts for any partitions which may be interleaved with
     * another partition.
     */

    /*
     * There must be multiple partitions to have any interleaved partitions,
     * otherwise there's nothing to interleave with.
     */
    if nparts > 1 {
        /*
         * Short-circuit check to see if only 1 Datum is allowed per
         * partition.  When this is true there's no need to do the more
         * expensive checks to look for interleaved values.
         */
        let accepts_nulls = if partition_bound_accepts_nulls(boundinfo as PartitionBoundInfo) { 1 } else { 0 };
        let has_default = if partition_bound_has_default(boundinfo as PartitionBoundInfo) { 1 } else { 0 };
        if (*boundinfo).ndatums + accepts_nulls + has_default != nparts {
            let mut last_index: c_int = -1;

            /*
             * Since the indexes array is sorted in Datum order, if any
             * partitions are interleaved then it will show up by the
             * partition indexes not being in ascending order.  Here we check
             * for that and record all partitions that are out of order.
             */
            i = 0;
            while i < (*boundinfo).nindexes {
                let index = *(*boundinfo).indexes.offset(i as isize);

                if index < last_index {
                    (*boundinfo).interleaved_parts = bms_add_member(
                        (*boundinfo).interleaved_parts,
                        index,
                    );
                }
                /*
                 * Otherwise, if the null_index exists in the indexes array,
                 * then the NULL partition must also allow some other Datum,
                 * therefore it's "interleaved".
                 */
                else if partition_bound_accepts_nulls(boundinfo as PartitionBoundInfo)
                    && index == (*boundinfo).null_index
                {
                    (*boundinfo).interleaved_parts = bms_add_member(
                        (*boundinfo).interleaved_parts,
                        index,
                    );
                }

                last_index = index;
                i += 1;
            }
        }

        /*
         * The DEFAULT partition is the "catch-all" partition that can contain
         * anything that does not belong to any other partition.  If there are
         * any other partitions then the DEFAULT partition must be marked as
         * interleaved.
         */
        if partition_bound_has_default(boundinfo as PartitionBoundInfo) {
            (*boundinfo).interleaved_parts = bms_add_member(
                (*boundinfo).interleaved_parts,
                (*boundinfo).default_index,
            );
        }
    }

    /* All partitions must now have been assigned canonical indexes. */
    Assert!(next_index == nparts);
    boundinfo as PartitionBoundInfo
}

/*
 * create_range_bounds
 *		Create a PartitionBoundInfo for a range partitioned table
 */
unsafe fn create_range_bounds(
    boundspecs: *mut *mut PartitionBoundSpec,
    nparts: c_int,
    key: PartitionKey,
    mapping: *mut *mut c_int,
) -> PartitionBoundInfo {
    let boundinfo: *mut PartitionBoundInfoFull;
    let mut rbounds: *mut *mut PartitionRangeBound = ptr::null_mut();
    let all_bounds: *mut *mut PartitionRangeBound;
    let mut prev: *mut PartitionRangeBound;
    let mut i: c_int;
    let mut k: c_int;
    let partnatts: c_int;
    let mut ndatums: c_int = 0;
    let mut default_index: c_int = -1;
    let mut next_index: c_int = 0;
    let bound_datums: *mut Datum;
    let bound_kinds: *mut PartitionRangeDatumKind;

    boundinfo = palloc0(core::mem::size_of::<PartitionBoundInfoFull>())
        as *mut PartitionBoundInfoFull;
    (*boundinfo).strategy = b'r' as c_char;
    /* There is no special null-accepting range partition. */
    (*boundinfo).null_index = -1;
    /* Will be set correctly below. */
    (*boundinfo).default_index = -1;

    all_bounds = palloc0(2 * nparts as usize * core::mem::size_of::<*mut PartitionRangeBound>())
        as *mut *mut PartitionRangeBound;

    /* Create a unified list of range bounds across all the partitions. */
    ndatums = 0;
    i = 0;
    while i < nparts {
        let spec = *boundspecs.offset(i as isize);
        let lower: *mut PartitionRangeBound;
        let upper: *mut PartitionRangeBound;

        if (*spec).strategy != b'r' as c_char {
            elog!(ERROR, "invalid strategy in partition bound spec");
        }

        /*
         * Note the index of the partition bound spec for the default
         * partition.  There's no datum to add to the all_bounds array for
         * this partition.
         */
        if (*spec).is_default {
            default_index = i;
            i += 1;
            continue;
        }

        lower = make_one_partition_rbound(key, i, (*spec).lowerdatums, true);
        upper = make_one_partition_rbound(key, i, (*spec).upperdatums, false);
        *all_bounds.offset(ndatums as isize) = lower;
        ndatums += 1;
        *all_bounds.offset(ndatums as isize) = upper;
        ndatums += 1;
        i += 1;
    }

    Assert!(
        ndatums == nparts * 2
            || (default_index != -1 && ndatums == (nparts - 1) * 2)
    );

    /* Sort all the bounds in ascending order */
    qsort_arg(
        all_bounds as *mut c_void,
        ndatums as usize,
        core::mem::size_of::<*mut PartitionRangeBound>(),
        Some(qsort_partition_rbound_cmp),
        key as *mut c_void,
    );

    /* Save distinct bounds from all_bounds into rbounds. */
    rbounds = palloc(ndatums as usize * core::mem::size_of::<*mut PartitionRangeBound>())
        as *mut *mut PartitionRangeBound;
    k = 0;
    prev = ptr::null_mut();
    i = 0;
    while i < ndatums {
        let cur = *all_bounds.offset(i as isize);
        let mut is_distinct = false;
        let mut j: c_int;

        /* Is the current bound distinct from the previous one? */
        j = 0;
        while j < (*key).partnatts as c_int {
            let cmpval: Datum;

            if prev.is_null() || (*cur).kind.offset(j as isize) != (*prev).kind.offset(j as isize) {
                // Compare kinds via values
                let cur_kind = *(*cur).kind.offset(j as isize) as i32;
                let prev_kind = if prev.is_null() { i32::MIN } else { *(*prev).kind.offset(j as isize) as i32 };
                if prev.is_null() || cur_kind != prev_kind {
                    is_distinct = true;
                    break;
                }
            }

            /*
             * If the bounds are both MINVALUE or MAXVALUE, stop now and treat
             * them as equal, since any values after this point must be
             * ignored.
             */
            if *(*cur).kind.offset(j as isize) != PARTITION_RANGE_DATUM_VALUE {
                break;
            }

            cmpval = FunctionCall2Coll(
                &mut *(*key).partsupfunc.offset(j as isize),
                *(*key).partcollation.offset(j as isize),
                *(*cur).datums.offset(j as isize),
                *(*prev).datums.offset(j as isize),
            );
            if DatumGetInt32(cmpval) != 0 {
                is_distinct = true;
                break;
            }
            j += 1;
        }

        /*
         * Only if the bound is distinct save it into a temporary array, i.e,
         * rbounds which is later copied into boundinfo datums array.
         */
        if is_distinct {
            *rbounds.offset(k as isize) = *all_bounds.offset(i as isize);
            k += 1;
        }

        prev = cur;
        i += 1;
    }

    pfree(all_bounds as *mut c_void);

    /* Update ndatums to hold the count of distinct datums. */
    ndatums = k;

    /*
     * Add datums to boundinfo.  Canonical indexes are values ranging from 0
     * to nparts - 1, assigned in that order to each partition's upper bound.
     * For 'datums' elements that are lower bounds, there is -1 in the
     * 'indexes' array to signify that no partition exists for the values less
     * than such a bound and greater than or equal to the previous upper
     * bound.
     */
    (*boundinfo).ndatums = ndatums;
    (*boundinfo).datums =
        palloc0(ndatums as usize * core::mem::size_of::<*mut Datum>()) as *mut *mut Datum;
    (*boundinfo).kind = palloc(ndatums as usize * core::mem::size_of::<*mut PartitionRangeDatumKind>())
        as *mut *mut PartitionRangeDatumKind;
    (*boundinfo).interleaved_parts = ptr::null_mut();

    /*
     * For range partitioning, an additional value of -1 is stored as the last
     * element of the indexes[] array.
     */
    (*boundinfo).nindexes = ndatums + 1;
    (*boundinfo).indexes =
        palloc((ndatums + 1) as usize * core::mem::size_of::<c_int>()) as *mut c_int;

    /*
     * In the loop below, to save from allocating a series of small arrays,
     * here we just allocate a single array for Datums and another for
     * PartitionRangeDatumKinds, below we'll just assign a portion of these
     * arrays in each loop.
     */
    partnatts = (*key).partnatts as c_int;
    bound_datums = palloc(ndatums as usize * partnatts as usize * core::mem::size_of::<Datum>())
        as *mut Datum;
    bound_kinds = palloc(
        ndatums as usize * partnatts as usize * core::mem::size_of::<PartitionRangeDatumKind>(),
    ) as *mut PartitionRangeDatumKind;

    i = 0;
    while i < ndatums {
        let mut j: c_int;

        *(*boundinfo).datums.offset(i as isize) = bound_datums.offset((i * partnatts) as isize);
        *(*boundinfo).kind.offset(i as isize) = bound_kinds.offset((i * partnatts) as isize);
        j = 0;
        while j < partnatts {
            if *(*(*rbounds.offset(i as isize))).kind.offset(j as isize)
                == PARTITION_RANGE_DATUM_VALUE
            {
                *(*(*boundinfo).datums.offset(i as isize)).offset(j as isize) = datumCopy(
                    *(*(*rbounds.offset(i as isize))).datums.offset(j as isize),
                    *(*key).parttypbyval.offset(j as isize),
                    *(*key).parttyplen.offset(j as isize) as c_int,
                );
            }
            *(*(*boundinfo).kind.offset(i as isize)).offset(j as isize) =
                *(*(*rbounds.offset(i as isize))).kind.offset(j as isize);
            j += 1;
        }

        /*
         * There is no mapping for invalid indexes.
         *
         * Any lower bounds in the rbounds array have invalid indexes
         * assigned, because the values between the previous bound (if there
         * is one) and this (lower) bound are not part of the range of any
         * existing partition.
         */
        if (*(*rbounds.offset(i as isize))).lower {
            *(*boundinfo).indexes.offset(i as isize) = -1;
        } else {
            let orig_index = (*(*rbounds.offset(i as isize))).index;

            /* If the old index has no mapping, assign one */
            if *(*mapping).offset(orig_index as isize) == -1 {
                *(*mapping).offset(orig_index as isize) = next_index;
                next_index += 1;
            }

            *(*boundinfo).indexes.offset(i as isize) = *(*mapping).offset(orig_index as isize);
        }
        i += 1;
    }

    pfree(rbounds as *mut c_void);

    /* Set the canonical value for default_index, if any. */
    if default_index != -1 {
        Assert!(default_index >= 0 && *(*mapping).offset(default_index as isize) == -1);
        *(*mapping).offset(default_index as isize) = next_index;
        next_index += 1;
        (*boundinfo).default_index = *(*mapping).offset(default_index as isize);
    }

    /* The extra -1 element. */
    Assert!(i == ndatums);
    *(*boundinfo).indexes.offset(i as isize) = -1;

    /* All partitions must now have been assigned canonical indexes. */
    Assert!(next_index == nparts);
    boundinfo as PartitionBoundInfo
}

// ---------------------------------------------------------------------------
// Part 6: get_partition_operator, make_partition_op_expr, get_qual_for_hash,
//         get_qual_for_list, get_qual_for_range, get_range_key_properties,
//         get_range_nulltest, compute_partition_hash_value,
//         satisfies_hash_partition
// ---------------------------------------------------------------------------

/* OID constants needed below (catalog/pg_type_d.h equivalents) */
const BOOLOID_PB: Oid = 16;
const OIDOID_PB: Oid = 26;
const INT4OID_PB: Oid = 23;
const RECORDOID_PB: Oid = 2249;

/* Strategy numbers (access/stratnum.h) */
use crate::access::stratnum::{
    StrategyNumber, BTLessStrategyNumber, BTLessEqualStrategyNumber,
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
};

/* Syscache IDs (utils/cache/syscache.h) */
const RELOID_PB: c_int = 1; /* TODO(pg-port): real value from syscache.h */
const Anum_pg_class_relpartbound_PB: AttrNumber = 34;

/* F_SATISFIES_HASH_PARTITION OID (catalog/pg_proc_d.h) */
/* TODO(pg-port): use generated constant when available */
const F_SATISFIES_HASH_PARTITION: Oid = 3408;

/* Hash partition seed (partitioning/partbounds.h) */
const HASH_PARTITION_SEED: u64 = 0x7A5B22367996DCFD;

/* Lock modes */
const ACCESS_SHARE_LOCK_PB: c_int = 1; /* AccessShareLock */
const NO_LOCK_PB: c_int = 0;           /* NoLock */

/* AttrNumber type alias */
type AttrNumber = i16;

/* ArrayType opaque */
#[repr(C)]
pub struct ArrayType {
    _unused: [u8; 0],
}

/* FcInfo placeholder for satisfies_hash_partition */
#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub flinfo: *mut FmgrInfo,
    /* ... remaining fields not needed for stub ... */
    _pad: [u8; 256],
}
pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;

/* TODO(pg-port) stubs ---------------------------------------------------- */
unsafe fn get_opfamily_member(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: StrategyNumber) -> Oid {
    /* TODO(pg-port): real impl in utils/cache/lsyscache.c */
    0
}
unsafe fn IsPolymorphicType(typid: Oid) -> bool {
    /* TODO(pg-port): real impl in catalog/pg_type.h / parse_coerce.c */
    false
}
unsafe fn type_is_array(typid: Oid) -> bool {
    /* TODO(pg-port): real impl in utils/cache/lsyscache.c */
    false
}
unsafe fn get_array_type(typid: Oid) -> Oid {
    /* TODO(pg-port): real impl in utils/cache/lsyscache.c */
    0
}
unsafe fn get_opcode(opno: Oid) -> Oid {
    /* TODO(pg-port): real impl in utils/cache/lsyscache.c */
    0
}
unsafe fn make_opclause(
    opno: Oid, opresulttype: Oid, opretset: bool,
    leftop: *mut Expr, rightop: *mut Expr,
    opcollid: Oid, inputcollid: Oid,
) -> *mut Expr {
    /* TODO(pg-port): real impl in nodes/makefuncs.c */
    ptr::null_mut()
}
unsafe fn copyObject<T>(obj: *mut T) -> *mut T {
    /* TODO(pg-port): real impl in nodes/copyfuncs.c */
    ptr::null_mut()
}
unsafe fn PartConstraintImpliedByRelConstraint(rel: Relation, constraint_list: *mut List) -> bool {
    /* TODO(pg-port): real impl in catalog/partition.c */
    false
}
unsafe fn SearchSysCache1_pb(cacheid: c_int, key1: Datum) -> HeapTuple {
    /* TODO(pg-port): use crate::utils::cache::syscache::SearchSysCache1 */
    ptr::null_mut()
}
unsafe fn SysCacheGetAttrNotNull_pb(cacheid: c_int, tup: HeapTuple, attnum: AttrNumber) -> Datum {
    /* TODO(pg-port): use crate::catalog::objectaddress_impl::SysCacheGetAttrNotNull */
    0
}
unsafe fn ReleaseSysCache_pb(tup: HeapTuple) {
    /* TODO(pg-port): use crate::utils::cache::syscache::ReleaseSysCache */
}
unsafe fn stringToNode_pb(str_: *const c_char) -> *mut c_void {
    /* TODO(pg-port): use crate::nodes::read::stringToNode */
    ptr::null_mut()
}
unsafe fn TextDatumGetCString_pb(d: Datum) -> *mut c_char {
    /* TODO(pg-port): use crate::utils::adt::varlena::TextDatumGetCString */
    ptr::null_mut()
}
unsafe fn fix_opfuncids(node: *mut Node) {
    /* TODO(pg-port): real impl in nodes/nodeFuncs.c */
}
unsafe fn ExecInitExpr(node: *mut Expr, parent: *mut c_void) -> *mut c_void {
    /* TODO(pg-port): real impl in executor/execExpr.c */
    ptr::null_mut()
}
unsafe fn ExecEvalExprSwitchContext(
    expr: *mut c_void, econtext: *mut c_void, isnull: *mut bool,
) -> Datum {
    /* TODO(pg-port): real impl in executor/execExpr.c */
    0
}
unsafe fn relation_open_pb(relid: Oid, lockmode: c_int) -> Relation {
    /* TODO(pg-port): use crate::access::common::relation::relation_open */
    ptr::null_mut()
}
unsafe fn relation_close_pb(rel: Relation, lockmode: c_int) {
    /* TODO(pg-port): use crate::access::common::relation::relation_close */
}
unsafe fn get_fn_expr_variadic_pb(flinfo: *mut FmgrInfo) -> bool {
    /* TODO(pg-port): use crate::utils::fmgr::get_fn_expr_variadic */
    false
}
unsafe fn get_fn_expr_argtype_pb(flinfo: *mut FmgrInfo, argnum: c_int) -> Oid {
    /* TODO(pg-port): use crate::utils::fmgr::get_fn_expr_argtype */
    0
}
unsafe fn fmgr_info_copy_pb(dst: *mut FmgrInfo, src: *mut FmgrInfo, ctx: MemoryContext) {
    /* TODO(pg-port): use crate::utils::fmgr::fmgr_info_copy */
}
unsafe fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> bool {
    /* TODO(pg-port): use crate::parser::parse_coerce::IsBinaryCoercible */
    false
}
unsafe fn format_type_be(typid: Oid) -> *mut c_char {
    /* TODO(pg-port): real impl in utils/adt/format_type.c */
    ptr::null_mut()
}
unsafe fn ARR_ELEMTYPE(arr: *const ArrayType) -> Oid {
    /* TODO(pg-port): use crate::utils::array::ARR_ELEMTYPE */
    0
}
unsafe fn deconstruct_array(
    arr: *mut ArrayType, elmtype: Oid, elmlen: i16, elmbyval: bool, elmalign: c_char,
    elemsp: *mut *mut Datum, nullsp: *mut *mut bool, nelemsp: *mut c_int,
) {
    /* TODO(pg-port): real impl in utils/adt/arrayfuncs.c */
}
unsafe fn get_typlenbyvalalign(typid: Oid, typlen: *mut i16, typbyval: *mut bool, typalign: *mut c_char) {
    /* TODO(pg-port): real impl in utils/cache/lsyscache.c */
}
unsafe fn MemoryContextAllocZero_pb(context: MemoryContext, size: usize) -> *mut c_void {
    /* TODO(pg-port): use crate::utils::mmgr::mcxt::MemoryContextAllocZero */
    palloc0(size)
}
fn hash_combine64(a: u64, b: u64) -> u64 {
    /* Matches common/hashfn.c hash_combine64 */
    use crate::common::hashfn::hash_combine64 as hc64;
    hc64(a, b)
}
unsafe fn DatumGetUInt64(x: Datum) -> u64 {
    /* from postgres.h */
    x as u64
}
fn UInt64GetDatum(x: u64) -> Datum {
    x as Datum
}

/* PartitionDesc accessor (partitioning/partdesc.rs stub context) */
use crate::partitioning::partdesc::{RelationGetPartitionDesc, PartitionDescData, PartitionDesc};

/* makefuncs imports */
use crate::nodes::makefuncs::{makeVar as mf_makeVar, makeConst as mf_makeConst, makeBoolExpr as mf_makeBoolExpr, makeRelabelType as mf_makeRelabelType, makeFuncExpr as mf_makeFuncExpr, makeBoolConst as mf_makeBoolConst, make_opclause as mf_make_opclause};
use crate::nodes::primnodes::CoercionForm::{COERCE_EXPLICIT_CALL, COERCE_EXPLICIT_CAST};
use crate::nodes::primnodes::BoolExprType::{OR_EXPR, AND_EXPR, NOT_EXPR};
use crate::nodes::primnodes::NullTestType::{IS_NULL, IS_NOT_NULL};

/*
 * get_partition_operator
 *
 * Return oid of the operator of the given strategy for the given partition
 * key column.  It is assumed that the partitioning key is of the same type as
 * the chosen partitioning opclass, or at least binary-compatible.  In the
 * latter case, *need_relabel is set to true if the opclass is not of a
 * polymorphic type (indicating a RelabelType node needed on top), otherwise
 * false.
 */
unsafe fn get_partition_operator_fn(
    key: PartitionKey,
    col: c_int,
    strategy: StrategyNumber,
    need_relabel: *mut bool,
) -> Oid {
    let operoid: Oid;

    /*
     * Get the operator in the partitioning opfamily using the opclass'
     * declared input type as both left- and righttype.
     */
    operoid = get_opfamily_member(
        *(*key).partopfamily.offset(col as isize),
        *(*key).partopcintype.offset(col as isize),
        *(*key).partopcintype.offset(col as isize),
        strategy,
    );
    if !OidIsValid(operoid) {
        elog!(
            ERROR,
            "missing operator {}({},{}) in partition opfamily {}",
            strategy,
            *(*key).partopcintype.offset(col as isize),
            *(*key).partopcintype.offset(col as isize),
            *(*key).partopfamily.offset(col as isize)
        );
    }

    /*
     * If the partition key column is not of the same type as the operator
     * class and not polymorphic, tell caller to wrap the non-Const expression
     * in a RelabelType.  This matches what parse_coerce.c does.
     */
    *need_relabel = *(*key).parttypid.offset(col as isize)
        != *(*key).partopcintype.offset(col as isize)
        && *(*key).partopcintype.offset(col as isize) != RECORDOID_PB
        && !IsPolymorphicType(*(*key).partopcintype.offset(col as isize));

    operoid
}

/*
 * make_partition_op_expr
 *		Returns an Expr for the given partition key column with arg1 and
 *		arg2 as its leftop and rightop, respectively
 */
unsafe fn make_partition_op_expr(
    key: PartitionKey,
    keynum: c_int,
    strategy: StrategyNumber,
    mut arg1: *mut Expr,
    arg2: *mut Expr,
) -> *mut Expr {
    let operoid: Oid;
    let mut need_relabel: bool = false;
    let mut result: *mut Expr = ptr::null_mut();

    /* Get the correct btree operator for this partitioning column */
    operoid = get_partition_operator_fn(key, keynum, strategy, &mut need_relabel);

    /*
     * Chosen operator may be such that the non-Const operand needs to be
     * coerced, so apply the same; see the comment in get_partition_operator().
     */
    if !IsA!(arg1, T_Const)
        && (need_relabel
            || *(*key).partcollation.offset(keynum as isize)
                != *(*key).parttypcoll.offset(keynum as isize))
    {
        arg1 = mf_makeRelabelType(
            arg1,
            *(*key).partopcintype.offset(keynum as isize),
            -1,
            *(*key).partcollation.offset(keynum as isize),
            COERCE_EXPLICIT_CAST,
        ) as *mut Expr;
    }

    /* Generate the actual expression */
    match (*key).strategy {
        PartitionStrategy::PARTITION_STRATEGY_LIST => {
            let elems: *mut List = arg2 as *mut List;
            let nelems: c_int = list_length(elems);

            Assert!(nelems >= 1);
            Assert!(keynum == 0);

            if nelems > 1 && !type_is_array(*(*key).parttypid.offset(keynum as isize)) {
                /* Construct an ArrayExpr for the right-hand inputs */
                let arrexpr: *mut ArrayExpr = makeNode!(ArrayExpr, T_ArrayExpr);
                (*arrexpr).array_typeid = get_array_type(*(*key).parttypid.offset(keynum as isize));
                (*arrexpr).array_collid = *(*key).parttypcoll.offset(keynum as isize);
                (*arrexpr).element_typeid = *(*key).parttypid.offset(keynum as isize);
                (*arrexpr).elements = elems;
                (*arrexpr).multidims = false;
                (*arrexpr).location = -1;

                /* Build leftop = ANY (rightop) */
                let saopexpr: *mut ScalarArrayOpExpr = makeNode!(ScalarArrayOpExpr, T_ScalarArrayOpExpr);
                (*saopexpr).opno = operoid;
                (*saopexpr).opfuncid = get_opcode(operoid);
                (*saopexpr).hashfuncid = InvalidOid;
                (*saopexpr).negfuncid = InvalidOid;
                (*saopexpr).useOr = true;
                (*saopexpr).inputcollid = *(*key).partcollation.offset(keynum as isize);
                (*saopexpr).args = list_make2!(arg1 as *mut c_void, arrexpr as *mut c_void);
                (*saopexpr).location = -1;

                result = saopexpr as *mut Expr;
            } else {
                let mut elemops: *mut List = NIL;

                foreach!(lc, elems, {
                    let elem: *mut Expr = lfirst(crate::current_cell!(lc)) as *mut Expr;
                    let elemop: *mut Expr = make_opclause(
                        operoid,
                        BOOLOID_PB,
                        false,
                        arg1,
                        elem,
                        InvalidOid,
                        *(*key).partcollation.offset(keynum as isize),
                    );
                    elemops = lappend(elemops, elemop as *mut c_void);
                });

                result = if nelems > 1 {
                    mf_makeBoolExpr(OR_EXPR, elemops, -1)
                } else {
                    linitial(elemops) as *mut Expr
                };
            }
        }
        PartitionStrategy::PARTITION_STRATEGY_RANGE => {
            result = make_opclause(
                operoid,
                BOOLOID_PB,
                false,
                arg1,
                arg2,
                InvalidOid,
                *(*key).partcollation.offset(keynum as isize),
            );
        }
        PartitionStrategy::PARTITION_STRATEGY_HASH => {
            Assert!(false);
        }
    }

    result
}

/*
 * get_qual_for_hash
 *
 * Returns a CHECK constraint expression to use as a hash partition's
 * constraint, given the parent relation and partition bound structure.
 *
 * The partition constraint for a hash partition is always a call to the
 * built-in function satisfies_hash_partition().
 */
unsafe fn get_qual_for_hash(parent: Relation, spec: *mut PartitionBoundSpec) -> *mut List {
    let key: PartitionKey = RelationGetPartitionKey(parent);
    let fexpr: *mut FuncExpr;
    let relid_const: *mut Node;
    let modulus_const: *mut Node;
    let remainder_const: *mut Node;
    let mut args: *mut List;
    let mut partexprs_item: *mut ListCell;
    let mut i: c_int;

    /* Fixed arguments. */
    relid_const = mf_makeConst(
        OIDOID_PB,
        -1,
        InvalidOid,
        core::mem::size_of::<Oid>() as i32,
        ObjectIdGetDatum(RelationGetRelid(parent)),
        false,
        true,
    ) as *mut Node;

    modulus_const = mf_makeConst(
        INT4OID_PB,
        -1,
        InvalidOid,
        core::mem::size_of::<i32>() as i32,
        Int32GetDatum((*spec).modulus),
        false,
        true,
    ) as *mut Node;

    remainder_const = mf_makeConst(
        INT4OID_PB,
        -1,
        InvalidOid,
        core::mem::size_of::<i32>() as i32,
        Int32GetDatum((*spec).remainder),
        false,
        true,
    ) as *mut Node;

    args = list_make3!(
        relid_const as *mut c_void,
        modulus_const as *mut c_void,
        remainder_const as *mut c_void
    );
    partexprs_item = list_head((*key).partexprs);

    /* Add an argument for each key column. */
    i = 0;
    while i < (*key).partnatts as c_int {
        let key_col: *mut Node;

        /* Left operand */
        if *(*key).partattrs.offset(i as isize) != 0 {
            key_col = mf_makeVar(
                1,
                *(*key).partattrs.offset(i as isize),
                *(*key).parttypid.offset(i as isize),
                *(*key).parttypmod.offset(i as isize),
                *(*key).parttypcoll.offset(i as isize),
                0,
            ) as *mut Node;
        } else {
            key_col = copyObject(lfirst(partexprs_item) as *mut c_void) as *mut Node;
            partexprs_item = lnext((*key).partexprs, partexprs_item);
        }

        args = lappend(args, key_col as *mut c_void);
        i += 1;
    }

    fexpr = mf_makeFuncExpr(
        F_SATISFIES_HASH_PARTITION,
        BOOLOID_PB,
        args,
        InvalidOid,
        InvalidOid,
        COERCE_EXPLICIT_CALL,
    );

    list_make1!(fexpr as *mut c_void)
}

/*
 * get_qual_for_list
 *
 * Returns an implicit-AND list of expressions to use as a list partition's
 * constraint, given the parent relation and partition bound structure.
 *
 * The function returns NIL for a default partition when it's the only
 * partition since in that case there is no constraint.
 */
unsafe fn get_qual_for_list(parent: Relation, spec: *mut PartitionBoundSpec) -> *mut List {
    let key: PartitionKey = RelationGetPartitionKey(parent);
    let mut result: *mut List;
    let key_col: *mut Expr;
    let opexpr: *mut Expr;
    let nulltest: *mut NullTest;
    let mut elems: *mut List = NIL;
    let mut list_has_null: bool = false;

    /*
     * Only single-column list partitioning is supported, so we are worried
     * only about the partition key with index 0.
     */
    Assert!((*key).partnatts == 1);

    /* Construct Var or expression representing the partition column */
    if *(*key).partattrs.offset(0) != 0 {
        key_col = mf_makeVar(
            1,
            *(*key).partattrs.offset(0),
            *(*key).parttypid.offset(0),
            *(*key).parttypmod.offset(0),
            *(*key).parttypcoll.offset(0),
            0,
        ) as *mut Expr;
    } else {
        key_col = copyObject(linitial((*key).partexprs) as *mut c_void) as *mut Expr;
    }

    /*
     * For default list partition, collect datums for all the partitions. The
     * default partition constraint should check that the partition key is
     * equal to none of those.
     */
    if (*spec).is_default {
        let mut idx: c_int = 0;
        let mut ndatums: c_int = 0;
        let pdesc: PartitionDesc = RelationGetPartitionDesc(parent, false);
        let boundinfo = pbi((*pdesc).boundinfo);

        if !(*pdesc).boundinfo.is_null() {
            ndatums = (*boundinfo).ndatums;

            if partition_bound_accepts_nulls((*pdesc).boundinfo) {
                list_has_null = true;
            }
        }

        /*
         * If default is the only partition, there need not be any partition
         * constraint on it.
         */
        if ndatums == 0 && !list_has_null {
            return NIL;
        }

        while idx < ndatums {
            let val: *mut Const;

            /*
             * Construct Const from known-not-null datum.  We must be careful
             * to copy the value, because our result has to be able to outlive
             * the relcache entry we're copying from.
             */
            val = mf_makeConst(
                *(*key).parttypid.offset(0),
                *(*key).parttypmod.offset(0),
                *(*key).parttypcoll.offset(0),
                *(*key).parttyplen.offset(0) as c_int,
                datumCopy(
                    *(*(*boundinfo).datums.offset(idx as isize)),
                    *(*key).parttypbyval.offset(0),
                    *(*key).parttyplen.offset(0) as c_int,
                ),
                false, /* isnull */
                *(*key).parttypbyval.offset(0),
            );

            elems = lappend(elems, val as *mut c_void);
            idx += 1;
        }
    } else {
        /*
         * Create list of Consts for the allowed values, excluding any nulls.
         */
        foreach!(cell, (*spec).listdatums, {
            let val: *const Const =
                lfirst_node!(Const, T_Const,
                             crate::current_cell!(cell));

            if (*val).constisnull {
                list_has_null = true;
            } else {
                elems = lappend(elems, copyObject(val as *mut c_void));
            }
        });
    }

    if !elems.is_null() {
        /*
         * Generate the operator expression from the non-null partition
         * values.
         */
        opexpr = make_partition_op_expr(key, 0, BTEqualStrategyNumber, key_col, elems as *mut Expr);
    } else {
        /*
         * If there are no partition values, we don't need an operator
         * expression.
         */
        opexpr = ptr::null_mut();
    }

    if !list_has_null {
        /*
         * Gin up a "col IS NOT NULL" test that will be ANDed with the main
         * expression.  This might seem redundant, but the partition routing
         * machinery needs it.
         */
        nulltest = makeNode!(NullTest, T_NullTest);
        (*nulltest).arg = key_col;
        (*nulltest).nulltesttype = IS_NOT_NULL;
        (*nulltest).argisrow = false;
        (*nulltest).location = -1;

        result = if !opexpr.is_null() {
            list_make2!(nulltest as *mut c_void, opexpr as *mut c_void)
        } else {
            list_make1!(nulltest as *mut c_void)
        };
    } else {
        /*
         * Gin up a "col IS NULL" test that will be OR'd with the main
         * expression.
         */
        nulltest = makeNode!(NullTest, T_NullTest);
        (*nulltest).arg = key_col;
        (*nulltest).nulltesttype = IS_NULL;
        (*nulltest).argisrow = false;
        (*nulltest).location = -1;

        if !opexpr.is_null() {
            let or: *mut Expr = mf_makeBoolExpr(OR_EXPR, list_make2!(nulltest as *mut c_void, opexpr as *mut c_void), -1);
            result = list_make1!(or as *mut c_void);
        } else {
            result = list_make1!(nulltest as *mut c_void);
        }
    }

    /*
     * Note that, in general, applying NOT to a constraint expression doesn't
     * necessarily invert the set of rows it accepts, because NOT (NULL) is
     * NULL.  However, the partition constraints we construct here never
     * evaluate to NULL, so applying NOT works as intended.
     */
    if (*spec).is_default {
        result = list_make1!(make_ands_explicit(result) as *mut c_void);
        result = list_make1!(mf_makeBoolExpr(NOT_EXPR, result, -1) as *mut c_void);
    }

    result
}

/*
 * get_qual_for_range
 *
 * Returns an implicit-AND list of expressions to use as a range partition's
 * constraint, given the parent relation and partition bound structure.
 *
 * For a multi-column range partition key, say (a, b, c), with (al, bl, cl)
 * as the lower bound tuple and (au, bu, cu) as the upper bound tuple, we
 * generate an expression tree of the following form:
 *
 *	(a IS NOT NULL) and (b IS NOT NULL) and (c IS NOT NULL)
 *		AND
 *	(a > al OR (a = al AND b > bl) OR (a = al AND b = bl AND c >= cl))
 *		AND
 *	(a < au OR (a = au AND b < bu) OR (a = au AND b = bu AND c < cu))
 *
 * External callers should pass for_default as false; we set it to true only
 * when recursing.
 */
unsafe fn get_qual_for_range(
    parent: Relation,
    spec: *mut PartitionBoundSpec,
    for_default: bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let key: PartitionKey = RelationGetPartitionKey(parent);

    if (*spec).is_default {
        let mut or_expr_args: *mut List = NIL;
        let pdesc: PartitionDesc = RelationGetPartitionDesc(parent, false);
        let inh_oids: *mut Oid = (*pdesc).oids;
        let nparts: c_int = (*pdesc).nparts;
        let mut k: c_int;

        k = 0;
        while k < nparts {
            let inhrelid: Oid = *inh_oids.offset(k as isize);
            let tuple: HeapTuple;
            let datum: Datum;
            let bspec: *mut PartitionBoundSpec;

            tuple = SearchSysCache1_pb(RELOID_PB, ObjectIdGetDatum(inhrelid));
            if tuple.is_null() {
                elog!(ERROR, "cache lookup failed for relation {}", inhrelid);
            }

            datum = SysCacheGetAttrNotNull_pb(RELOID_PB, tuple, Anum_pg_class_relpartbound_PB);
            bspec = stringToNode_pb(TextDatumGetCString_pb(datum) as *const c_char)
                as *mut PartitionBoundSpec;
            if !IsA!(bspec, T_PartitionBoundSpec) {
                elog!(ERROR, "expected PartitionBoundSpec");
            }

            if !(*bspec).is_default {
                let part_qual: *mut List = get_qual_for_range(parent, bspec, true);

                /*
                 * AND the constraints of the partition and add to or_expr_args
                 */
                or_expr_args = lappend(
                    or_expr_args,
                    if list_length(part_qual) > 1 {
                        mf_makeBoolExpr(AND_EXPR, part_qual, -1) as *mut c_void
                    } else {
                        linitial(part_qual)
                    },
                );
            }
            ReleaseSysCache_pb(tuple);
            k += 1;
        }

        if or_expr_args != NIL {
            let other_parts_constr: *mut Expr;

            /*
             * Combine the constraints obtained for non-default partitions
             * using OR.  As requested, each of the OR's args doesn't include
             * the NOT NULL test for partition keys (which is to avoid its
             * useless repetition).  Add the same now.
             */
            let null_tests: *mut List = get_range_nulltest(key);
            let or_part: *mut c_void = if list_length(or_expr_args) > 1 {
                mf_makeBoolExpr(OR_EXPR, or_expr_args, -1) as *mut c_void
            } else {
                linitial(or_expr_args)
            };
            let and_args: *mut List = lappend(null_tests, or_part);
            other_parts_constr = mf_makeBoolExpr(AND_EXPR, and_args, -1);

            /*
             * Finally, the default partition contains everything *NOT*
             * contained in the non-default partitions.
             */
            result = list_make1!(
                mf_makeBoolExpr(NOT_EXPR, list_make1!(other_parts_constr as *mut c_void), -1)
                    as *mut c_void
            );
        }

        return result;
    }

    /*
     * If it is the recursive call for default, we skip the get_range_nulltest
     * to avoid accumulating the NullTest on the same keys for each partition.
     */
    if !for_default {
        result = get_range_nulltest(key);
    }

    /*
     * Iterate over the key columns and check if the corresponding lower and
     * upper datums are equal using the btree equality operator for the
     * column's type.  If equal, we emit single keyCol = common_value
     * expression.  Starting from the first column for which the corresponding
     * lower and upper bound datums are not equal, we generate OR expressions
     * as shown in the function's header comment.
     */
    let mut i: c_int = 0;
    let mut partexprs_item: *mut ListCell = list_head((*key).partexprs);
    let mut partexprs_item_saved: *mut ListCell = partexprs_item;
    let lower_or_start_datum: *mut ListCell;
    let upper_or_start_datum: *mut ListCell;

    let mut cell1: *mut ListCell = list_head((*spec).lowerdatums);
    let mut cell2: *mut ListCell = list_head((*spec).upperdatums);

    while !cell1.is_null() && !cell2.is_null() {
        let ldatum: *mut PartitionRangeDatum =
            lfirst_node!(PartitionRangeDatum, T_PartitionRangeDatum,
                         cell1);
        let udatum: *mut PartitionRangeDatum =
            lfirst_node!(PartitionRangeDatum, T_PartitionRangeDatum,
                         cell2);
        let mut key_col: *mut Expr = ptr::null_mut();
        let mut lower_val: *mut Const = ptr::null_mut();
        let mut upper_val: *mut Const = ptr::null_mut();
        let test_expr: *mut Expr;
        let test_exprstate: *mut c_void;
        let test_result: Datum;
        let mut is_null: bool = false;

        /*
         * Since get_range_key_properties() modifies partexprs_item, and we
         * might need to start over from the previous expression in the later
         * part of this function, save away the current value.
         */
        partexprs_item_saved = partexprs_item;

        get_range_key_properties(
            key, i, ldatum, udatum,
            &mut partexprs_item,
            &mut key_col,
            &mut lower_val, &mut upper_val,
        );

        /*
         * If either value is NULL, the corresponding partition bound is
         * either MINVALUE or MAXVALUE, and we treat them as unequal, because
         * even if they're the same, there is no common value to equate the
         * key column with.
         */
        if lower_val.is_null() || upper_val.is_null() {
            break;
        }

        /* Create the test expression */
        let estate: *mut EState = CreateExecutorState();
        let old_cxt: MemoryContext = MemoryContextSwitchTo((*estate).es_query_cxt);
        test_expr = make_partition_op_expr(
            key, i, BTEqualStrategyNumber,
            lower_val as *mut Expr, upper_val as *mut Expr,
        );
        fix_opfuncids(test_expr as *mut Node);
        test_exprstate = ExecInitExpr(test_expr, ptr::null_mut());
        test_result = ExecEvalExprSwitchContext(
            test_exprstate, GetPerTupleExprContext(estate) as *mut c_void, &mut is_null,
        );
        MemoryContextSwitchTo(old_cxt);
        FreeExecutorState(estate);

        /* If not equal, go generate the OR expressions */
        if !DatumGetBool(test_result) {
            break;
        }

        /*
         * The bounds for the last key column can't be equal, because such a
         * range partition would never be allowed to be defined (it would have
         * an empty range otherwise).
         */
        if i == (*key).partnatts as c_int - 1 {
            elog!(ERROR, "invalid range bound specification");
        }

        /* Equal, so generate keyCol = lower_val expression */
        result = lappend(
            result,
            make_partition_op_expr(key, i, BTEqualStrategyNumber, key_col, lower_val as *mut Expr)
                as *mut c_void,
        );

        cell1 = lnext((*spec).lowerdatums, cell1);
        cell2 = lnext((*spec).upperdatums, cell2);
        i += 1;
    }

    /* First pair of lower_val and upper_val that are not equal. */
    lower_or_start_datum = cell1;
    upper_or_start_datum = cell2;

    /* OR will have as many arms as there are key columns left. */
    let num_or_arms: c_int = (*key).partnatts as c_int - i;
    let mut current_or_arm: c_int = 0;
    let mut lower_or_arms: *mut List = NIL;
    let mut upper_or_arms: *mut List = NIL;
    let mut need_next_lower_arm: bool = true;
    let mut need_next_upper_arm: bool = true;

    while current_or_arm < num_or_arms {
        let mut lower_or_arm_args: *mut List = NIL;
        let mut upper_or_arm_args: *mut List = NIL;

        /* Restart scan of columns from the i'th one */
        let mut j: c_int = i;
        partexprs_item = partexprs_item_saved;

        let mut c1: *mut ListCell = lower_or_start_datum;
        let mut c2: *mut ListCell = upper_or_start_datum;

        while !c1.is_null() && !c2.is_null() {
            let ldatum: *mut PartitionRangeDatum =
                lfirst_node!(PartitionRangeDatum, T_PartitionRangeDatum, c1);
            let ldatum_next: *mut PartitionRangeDatum = if !lnext((*spec).lowerdatums, c1).is_null() {
                castNode!(PartitionRangeDatum, T_PartitionRangeDatum,
                          lnext((*spec).lowerdatums, c1))
            } else {
                ptr::null_mut()
            };
            let udatum: *mut PartitionRangeDatum =
                lfirst_node!(PartitionRangeDatum, T_PartitionRangeDatum, c2);
            let udatum_next: *mut PartitionRangeDatum = if !lnext((*spec).upperdatums, c2).is_null() {
                castNode!(PartitionRangeDatum, T_PartitionRangeDatum,
                          lnext((*spec).upperdatums, c2))
            } else {
                ptr::null_mut()
            };

            let mut key_col: *mut Expr = ptr::null_mut();
            let mut lower_val: *mut Const = ptr::null_mut();
            let mut upper_val: *mut Const = ptr::null_mut();
            get_range_key_properties(
                key, j, ldatum, udatum,
                &mut partexprs_item,
                &mut key_col,
                &mut lower_val, &mut upper_val,
            );

            if need_next_lower_arm && !lower_val.is_null() {
                let strategy: StrategyNumber;

                /*
                 * For the non-last columns of this arm, use the EQ operator.
                 * For the last column of this arm, use GT, unless this is the
                 * last column of the whole bound check, or the next bound
                 * datum is MINVALUE, in which case use GE.
                 */
                if j - i < current_or_arm {
                    strategy = BTEqualStrategyNumber;
                } else if j == (*key).partnatts as c_int - 1
                    || (!ldatum_next.is_null()
                        && (*ldatum_next).kind == PARTITION_RANGE_DATUM_MINVALUE)
                {
                    strategy = BTGreaterEqualStrategyNumber;
                } else {
                    strategy = BTGreaterStrategyNumber;
                }

                lower_or_arm_args = lappend(
                    lower_or_arm_args,
                    make_partition_op_expr(key, j, strategy, key_col, lower_val as *mut Expr)
                        as *mut c_void,
                );
            }

            if need_next_upper_arm && !upper_val.is_null() {
                let strategy: StrategyNumber;

                /*
                 * For the non-last columns of this arm, use the EQ operator.
                 * For the last column of this arm, use LT, unless the next
                 * bound datum is MAXVALUE, in which case use LE.
                 */
                if j - i < current_or_arm {
                    strategy = BTEqualStrategyNumber;
                } else if !udatum_next.is_null()
                    && (*udatum_next).kind == PARTITION_RANGE_DATUM_MAXVALUE
                {
                    strategy = BTLessEqualStrategyNumber;
                } else {
                    strategy = BTLessStrategyNumber;
                }

                upper_or_arm_args = lappend(
                    upper_or_arm_args,
                    make_partition_op_expr(key, j, strategy, key_col, upper_val as *mut Expr)
                        as *mut c_void,
                );
            }

            /*
             * Did we generate enough of OR's arguments?  First arm considers
             * the first of the remaining columns, second arm considers first
             * two of the remaining columns, and so on.
             */
            j += 1;
            if j - i > current_or_arm {
                /*
                 * We must not emit any more arms if the new column that will
                 * be considered is unbounded, or this one was.
                 */
                if lower_val.is_null()
                    || ldatum_next.is_null()
                    || (*ldatum_next).kind != PARTITION_RANGE_DATUM_VALUE
                {
                    need_next_lower_arm = false;
                }
                if upper_val.is_null()
                    || udatum_next.is_null()
                    || (*udatum_next).kind != PARTITION_RANGE_DATUM_VALUE
                {
                    need_next_upper_arm = false;
                }
                break;
            }

            c1 = lnext((*spec).lowerdatums, c1);
            c2 = lnext((*spec).upperdatums, c2);
        }

        if lower_or_arm_args != NIL {
            lower_or_arms = lappend(
                lower_or_arms,
                if list_length(lower_or_arm_args) > 1 {
                    mf_makeBoolExpr(AND_EXPR, lower_or_arm_args, -1) as *mut c_void
                } else {
                    linitial(lower_or_arm_args)
                },
            );
        }

        if upper_or_arm_args != NIL {
            upper_or_arms = lappend(
                upper_or_arms,
                if list_length(upper_or_arm_args) > 1 {
                    mf_makeBoolExpr(AND_EXPR, upper_or_arm_args, -1) as *mut c_void
                } else {
                    linitial(upper_or_arm_args)
                },
            );
        }

        /* If no work to do in the next iteration, break away. */
        if !need_next_lower_arm && !need_next_upper_arm {
            break;
        }

        current_or_arm += 1;
    }

    /*
     * Generate the OR expressions for each of lower and upper bounds (if
     * required), and append to the list of implicitly ANDed list of
     * expressions.
     */
    if lower_or_arms != NIL {
        result = lappend(
            result,
            if list_length(lower_or_arms) > 1 {
                mf_makeBoolExpr(OR_EXPR, lower_or_arms, -1) as *mut c_void
            } else {
                linitial(lower_or_arms)
            },
        );
    }
    if upper_or_arms != NIL {
        result = lappend(
            result,
            if list_length(upper_or_arms) > 1 {
                mf_makeBoolExpr(OR_EXPR, upper_or_arms, -1) as *mut c_void
            } else {
                linitial(upper_or_arms)
            },
        );
    }

    /*
     * As noted above, for non-default, we return list with constant TRUE. If
     * the result is NIL during the recursive call for default, it implies
     * this is the only other partition which can hold every value of the key
     * except NULL. Hence we return the NullTest result skipped earlier.
     */
    if result == NIL {
        result = if for_default {
            get_range_nulltest(key)
        } else {
            list_make1!(mf_makeBoolConst(true, false) as *mut c_void)
        };
    }

    result
}

/*
 * get_range_key_properties
 *		Returns range partition key information for a given column
 *
 * This is a subroutine for get_qual_for_range, and its API is pretty
 * specialized to that caller.
 *
 * Constructs an Expr for the key column (returned in *keyCol) and Consts
 * for the lower and upper range limits (returned in *lower_val and
 * *upper_val).  For MINVALUE/MAXVALUE limits, NULL is returned instead of
 * a Const.  All of these structures are freshly palloc'd.
 *
 * *partexprs_item points to the cell containing the next expression in
 * the key->partexprs list, or NULL.  It may be advanced upon return.
 */
unsafe fn get_range_key_properties(
    key: PartitionKey,
    keynum: c_int,
    ldatum: *mut PartitionRangeDatum,
    udatum: *mut PartitionRangeDatum,
    partexprs_item: *mut *mut ListCell,
    key_col: *mut *mut Expr,
    lower_val: *mut *mut Const,
    upper_val: *mut *mut Const,
) {
    /* Get partition key expression for this column */
    if *(*key).partattrs.offset(keynum as isize) != 0 {
        *key_col = mf_makeVar(
            1,
            *(*key).partattrs.offset(keynum as isize),
            *(*key).parttypid.offset(keynum as isize),
            *(*key).parttypmod.offset(keynum as isize),
            *(*key).parttypcoll.offset(keynum as isize),
            0,
        ) as *mut Expr;
    } else {
        if (*partexprs_item).is_null() {
            elog!(ERROR, "wrong number of partition key expressions");
        }
        *key_col = copyObject(lfirst(*partexprs_item) as *mut c_void) as *mut Expr;
        *partexprs_item = lnext((*key).partexprs, *partexprs_item);
    }

    /* Get appropriate Const nodes for the bounds */
    if (*ldatum).kind == PARTITION_RANGE_DATUM_VALUE {
        *lower_val = castNode!(Const, T_Const,
                               copyObject((*ldatum).value as *mut c_void) as *mut Node);
    } else {
        *lower_val = ptr::null_mut();
    }

    if (*udatum).kind == PARTITION_RANGE_DATUM_VALUE {
        *upper_val = castNode!(Const, T_Const,
                               copyObject((*udatum).value as *mut c_void) as *mut Node);
    } else {
        *upper_val = ptr::null_mut();
    }
}

/*
 * get_range_nulltest
 *
 * A non-default range partition table does not currently allow partition
 * keys to be null, so emit an IS NOT NULL expression for each key column.
 */
unsafe fn get_range_nulltest(key: PartitionKey) -> *mut List {
    let mut result: *mut List = NIL;
    let mut nulltest: *mut NullTest;
    let mut partexprs_item: *mut ListCell;
    let mut i: c_int;

    partexprs_item = list_head((*key).partexprs);
    i = 0;
    while i < (*key).partnatts as c_int {
        let key_col: *mut Expr;

        if *(*key).partattrs.offset(i as isize) != 0 {
            key_col = mf_makeVar(
                1,
                *(*key).partattrs.offset(i as isize),
                *(*key).parttypid.offset(i as isize),
                *(*key).parttypmod.offset(i as isize),
                *(*key).parttypcoll.offset(i as isize),
                0,
            ) as *mut Expr;
        } else {
            if partexprs_item.is_null() {
                elog!(ERROR, "wrong number of partition key expressions");
            }
            key_col = copyObject(lfirst(partexprs_item) as *mut c_void) as *mut Expr;
            partexprs_item = lnext((*key).partexprs, partexprs_item);
        }

        nulltest = makeNode!(NullTest, T_NullTest);
        (*nulltest).arg = key_col;
        (*nulltest).nulltesttype = IS_NOT_NULL;
        (*nulltest).argisrow = false;
        (*nulltest).location = -1;
        result = lappend(result, nulltest as *mut c_void);
        i += 1;
    }

    result
}

/*
 * compute_partition_hash_value
 *
 * Compute the hash value for given partition key values.
 */
pub unsafe fn compute_partition_hash_value(
    partnatts: c_int,
    partsupfunc: *mut FmgrInfo,
    partcollation: *const Oid,
    values: *const Datum,
    isnull: *const bool,
) -> u64 {
    let mut i: c_int;
    let mut row_hash: u64 = 0;
    let seed: Datum = UInt64GetDatum(HASH_PARTITION_SEED);

    i = 0;
    while i < partnatts {
        /* Nulls are just ignored */
        if !*isnull.offset(i as isize) {
            let hash: Datum;

            Assert!(OidIsValid((*partsupfunc.offset(i as isize)).fn_oid));

            /*
             * Compute hash for each datum value by calling respective
             * datatype-specific hash functions of each partition key
             * attribute.
             */
            hash = FunctionCall2Coll(
                &mut *partsupfunc.offset(i as isize),
                *partcollation.offset(i as isize),
                *values.offset(i as isize),
                seed,
            );

            /* Form a single 64-bit hash value */
            row_hash = hash_combine64(row_hash, DatumGetUInt64(hash));
        }
        i += 1;
    }

    row_hash
}

/*
 * satisfies_hash_partition
 *
 * This is an SQL-callable function for use in hash partition constraints.
 * The first three arguments are the parent table OID, modulus, and remainder.
 * The remaining arguments are the value of the partitioning columns (or
 * expressions); these are hashed and the results are combined into a single
 * hash value by calling hash_combine64.
 *
 * Returns true if remainder produced when this computed single hash value is
 * divided by the given modulus is equal to given remainder, otherwise false.
 * NB: it's important that this never return null, as the constraint machinery
 * would consider that to be a "pass".
 *
 * See get_qual_for_hash() for usage.
 */
#[repr(C)]
struct ColumnsHashData {
    relid: Oid,
    nkeys: c_int,
    variadic_type: Oid,
    variadic_typlen: i16,
    variadic_typbyval: bool,
    variadic_typalign: c_char,
    partcollid: [Oid; 32], /* PARTITION_MAX_KEYS */
    /* partsupfunc follows as flexible array */
}

pub unsafe fn satisfies_hash_partition(fcinfo: FunctionCallInfo) -> Datum {
    let parent_id: Oid;
    let modulus: c_int;
    let remainder: c_int;
    let seed: Datum = UInt64GetDatum(HASH_PARTITION_SEED);
    let my_extra: *mut ColumnsHashData;
    let row_hash: u64 = 0;

    /* Return false if the parent OID, modulus, or remainder is NULL. */
    /* (PG_ARGISNULL / PG_GETARG_* macro equivalents omitted - use fcinfo) */
    /* TODO(pg-port): real arg extraction via fcinfo; stub returns false */
    return BoolGetDatum(false);
}
