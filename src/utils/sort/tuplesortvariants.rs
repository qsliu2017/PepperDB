//! Implementation of tuple sorting variants.
//!
//! IMPL: postgres/src/backend/utils/sort/tuplesortvariants.c
//!
//! This module handles the sorting of heap tuples, index tuples, or single
//! Datums.  The implementation is based on the generalized tuple sorting
//! facility given in tuplesort.c.
//!
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   "access/brin_tuple.h"      -> crate::access::brin::brin_tuple (BrinTuple)
//!   "access/gin_tuple.h"       -> crate::access::gin::gin_tuple (GinTuple, _gin_compare_tuples)
//!   "access/hash.h"            -> Bucket / _hash_hashkey2bucket (STUB - hashutil)
//!   "access/htup_details.h"    -> crate::access::htup_details (heap_getattr, MinimalTuple, ...)
//!   "access/nbtree.h"          -> BTScanInsert / _bt_mkscankey / SK_BT_* (STUB)
//!   "catalog/index.h"          -> crate::catalog::index (BuildIndexInfo, FormIndexDatum)
//!   "catalog/pg_collation.h"   -> DEFAULT_COLLATION_OID (STUB const)
//!   "executor/executor.h"      -> crate::executor (Exec* / *ExecutorState / slots)
//!   "utils/builtins.h"         -> crate::utils::builtins (format_type_be)
//!   "utils/datum.h"            -> crate::utils::adt::datum (datumCopy, datumGetSize)
//!   "utils/lsyscache.h"        -> crate::utils::cache::lsyscache (get_typlenbyval, lookup_type_cache)
//!   "utils/tuplesort.h"        -> tuplesort.rs (Tuplesortstate, SortTuple, ... -- STUB, sibling)

use crate::prelude::*;

use crate::access::common::indextuple::{
    index_deform_tuple, index_form_tuple_context, index_getattr, IndexTuple, IndexTupleData,
    IndexTupleSize, INDEX_SIZE_MASK,
};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::access::htup_details::{
    heap_getattr, HeapTuple, HeapTupleData, HeapTupleHeader, MinimalTuple, HEAPTUPLESIZE,
    MINIMAL_TUPLE_DATA_OFFSET, MINIMAL_TUPLE_OFFSET,
};
use crate::nodes::execnodes::{EState, ExprContext, IndexInfo, TupleTableSlot, INDEX_MAX_KEYS};
use crate::nodes::primnodes::AttrNumber;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};
use crate::storage::off::OffsetNumber;
use crate::utils::sort::logtape::{LogicalTape, LogicalTapeRead, LogicalTapeWrite};
use crate::utils::sort::sortsupport::{
    ApplySortAbbrevFullComparator, ApplySortComparator, PrepareSortSupportComparisonShim,
    PrepareSortSupportFromGistIndexRel, PrepareSortSupportFromIndexRel,
    PrepareSortSupportFromOrderingOp, SortSupport, SortSupportData,
};
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetNumberOfAttributes, RelationGetRelationName,
};
use core::mem::{offset_of, size_of};

// ---------------------------------------------------------------------------
// Dependencies living in other not-yet-ported .c files.  Stubbed here.
// ---------------------------------------------------------------------------

// "utils/tuplesort.h" / tuplesort.c -- the generalized sort facility.
// TODO(pg-port): replace these stubs with imports from tuplesort.rs once ported.

/// Opaque tuplesort state.  Real definition lives in tuplesort.c.
pub enum Tuplesortstate {}

/// `SortTuple` from tuplesort.c -- one entry being sorted.
#[repr(C)]
pub struct SortTuple {
    pub tuple: *mut c_void, // the tuple proper
    pub datum1: Datum,      // value of first key column
    pub isnull1: bool,      // is first key column NULL?
    pub srctape: c_int,     // source tape number
}

/// `SortCoordinate` (tuplesort.h) -- parallel sort coordination handle.
pub type SortCoordinate = *mut c_void;

/// `TuplesortMethods` callbacks live in TuplesortPublic.
pub type SortTupleComparator =
    Option<unsafe fn(a: *const SortTuple, b: *const SortTuple, state: *mut Tuplesortstate) -> c_int>;

/// `TuplesortPublic` (tuplesort.h) -- the public part of Tuplesortstate.
#[repr(C)]
pub struct TuplesortPublic {
    pub nKeys: c_int,
    pub sortopt: c_int,
    pub tuples: bool,
    pub haveDatum1: bool,
    pub maincontext: MemoryContext,
    pub sortcontext: MemoryContext,
    pub tuplecontext: MemoryContext,
    pub removeabbrev:
        Option<unsafe fn(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int)>,
    pub comparetup: SortTupleComparator,
    pub comparetup_tiebreak: SortTupleComparator,
    pub writetup:
        Option<unsafe fn(state: *mut Tuplesortstate, tape: *mut LogicalTape, stup: *mut SortTuple)>,
    pub readtup: Option<
        unsafe fn(
            state: *mut Tuplesortstate,
            stup: *mut SortTuple,
            tape: *mut LogicalTape,
            len: c_uint,
        ),
    >,
    pub freestate: Option<unsafe fn(state: *mut Tuplesortstate)>,
    pub arg: *mut c_void,
    pub sortKeys: SortSupport,
    pub onlyKey: SortSupport,
}

/// TUPLESORT_RANDOMACCESS option flag (tuplesort.h).
pub const TUPLESORT_RANDOMACCESS: c_int = 1 << 0;

/// `TuplesortstateGetPublic` (tuplesort.h) -- cast state to its public part.
// TODO(pg-port): real macro reaches the embedded TuplesortPublic in tuplesort.c.
#[allow(unused_variables)]
pub unsafe fn TuplesortstateGetPublic(state: *mut Tuplesortstate) -> *mut TuplesortPublic {
    unimplemented!("TuplesortstateGetPublic: tuplesort.c not yet ported")
}

/// `tuplesort_begin_common` (tuplesort.c). STUB.
#[allow(unused_variables)]
pub unsafe fn tuplesort_begin_common(
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    unimplemented!("tuplesort_begin_common: tuplesort.c not yet ported")
}

/// `tuplesort_puttuple_common` (tuplesort.c). STUB.
#[allow(unused_variables)]
pub unsafe fn tuplesort_puttuple_common(
    state: *mut Tuplesortstate,
    tuple: *mut SortTuple,
    useAbbrev: bool,
    tuplen: Size,
) {
    unimplemented!("tuplesort_puttuple_common: tuplesort.c not yet ported")
}

/// `tuplesort_gettuple_common` (tuplesort.c). STUB.
#[allow(unused_variables)]
pub unsafe fn tuplesort_gettuple_common(
    state: *mut Tuplesortstate,
    forward: bool,
    stup: *mut SortTuple,
) -> bool {
    unimplemented!("tuplesort_gettuple_common: tuplesort.c not yet ported")
}

/// `tuplesort_readtup_alloc` (tuplesort.c). STUB.
#[allow(unused_variables)]
pub unsafe fn tuplesort_readtup_alloc(state: *mut Tuplesortstate, tuplen: Size) -> *mut c_void {
    unimplemented!("tuplesort_readtup_alloc: tuplesort.c not yet ported")
}

/// `TupleSortUseBumpTupleCxt` (tuplesort.h). STUB.
#[allow(unused_variables)]
pub fn TupleSortUseBumpTupleCxt(opt: c_int) -> bool {
    unimplemented!("TupleSortUseBumpTupleCxt: tuplesort.c not yet ported")
}

/// `PARALLEL_SORT` (tuplesort.c). STUB.
#[allow(unused_variables)]
pub unsafe fn PARALLEL_SORT(coordinate: SortCoordinate) -> c_int {
    unimplemented!("PARALLEL_SORT: tuplesort.c not yet ported")
}

// "access/brin_tuple.h"
use crate::access::brin::brin_tuple::BrinTuple;

// "access/gin_tuple.h"
use crate::access::gin::gin_tuple::{GinTuple, _gin_compare_tuples};

// "access/hash.h" -- Bucket type and bucket-mapping helper.
// TODO(pg-port): import from crate::access::hash once hashutil is wired here.
pub type Bucket = u32;

#[allow(unused_variables)]
unsafe fn _hash_hashkey2bucket(
    hashkey: u32,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
) -> Bucket {
    unimplemented!("_hash_hashkey2bucket: access/hash not yet wired")
}

// "access/nbtree.h" -- insertion scankey construction.
// TODO(pg-port): import from crate::access::nbtree once nbtutils is wired here.

/// `ScanKeyData` (access/skey.h) -- only the fields used here.
#[repr(C)]
pub struct ScanKeyData {
    pub sk_flags: c_int,
    pub sk_attno: AttrNumber,
    pub sk_collation: Oid,
}

/// `BTScanInsertData` (access/nbtree.h) -- only scankeys[] flexible array used.
#[repr(C)]
pub struct BTScanInsertData {
    pub scankeys: [ScanKeyData; 0],
}

pub type BTScanInsert = *mut BTScanInsertData;

pub const SK_BT_DESC: c_int = 0x00010000; // access/nbtree.h
pub const SK_BT_NULLS_FIRST: c_int = 0x00020000; // access/nbtree.h
pub const BTREE_AM_OID: Oid = 403; // catalog/pg_am.dat

#[allow(unused_variables)]
unsafe fn _bt_mkscankey(rel: Relation, itup: IndexTuple) -> BTScanInsert {
    unimplemented!("_bt_mkscankey: access/nbtree not yet wired")
}

// "catalog/index.h"
use crate::catalog::index::{BuildIndexInfo, FormIndexDatum};

// "catalog/pg_collation.h"
pub const DEFAULT_COLLATION_OID: Oid = 100; // catalog/pg_collation.dat

// "executor/executor.h"
use crate::executor::execTuples::{
    ExecStoreHeapTuple, ExecStoreMinimalTuple, MakeSingleTupleTableSlot, TTSOpsHeapTuple,
};
use crate::executor::execUtils::{CreateExecutorState, FreeExecutorState, GetPerTupleExprContext};
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlotMinimalTuple};

// "utils/builtins.h"
use crate::utils::builtins::format_type_be;

// "utils/datum.h"
use crate::utils::adt::datum::{datumCopy, datumGetSize};

// "utils/lsyscache.h"
use crate::utils::cache::lsyscache::get_typlenbyval;

/// `lookup_type_cache` (utils/typcache.c). STUB.
// TODO(pg-port): import from crate::utils::cache::typcache once wired.
#[allow(unused_variables)]
unsafe fn lookup_type_cache(type_id: Oid, flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("lookup_type_cache: typcache not yet wired")
}

#[repr(C)]
pub struct TypeCacheEntry {
    pub cmp_proc_finfo: FmgrInfoStub,
}

#[repr(C)]
pub struct FmgrInfoStub {
    pub fn_oid: Oid,
}

pub const TYPECACHE_CMP_PROC_FINFO: c_int = 0x00080; // utils/typcache.h

pub const GIN_COMPARE_PROC: c_int = 1; // access/gin.h

/// `index_getprocid` (access/genam.h). STUB.
// TODO(pg-port): import from crate::access::index::genam once wired.
#[allow(unused_variables)]
unsafe fn index_getprocid(irel: Relation, attnum: AttrNumber, procnum: u16) -> Oid {
    unimplemented!("index_getprocid: access/genam not yet wired")
}

/// `BuildIndexValueDescription` (access/genam.c).
use crate::access::index::genam::BuildIndexValueDescription;

/// `ResetPerTupleExprContext` (executor/executor.h). STUB.
// TODO(pg-port): import from crate::executor::execUtils once macro is exposed.
#[allow(unused_variables)]
unsafe fn ResetPerTupleExprContext(estate: *mut EState) {
    unimplemented!("ResetPerTupleExprContext: executor macro not yet wired")
}

/// `ExecDropSingleTupleTableSlot` (executor/execTuples.c). STUB import path differs.
use crate::executor::execTuples::ExecDropSingleTupleTableSlot;

/// `heap_copytuple` (access/common/heaptuple.c).
use crate::access::common::heaptuple::{heap_copy_minimal_tuple, heap_copytuple};

/// `errtableconstraint` (utils/elog.h family). STUB -- returns a no-op tag.
// TODO(pg-port): port the error-context table-constraint helper.
#[allow(unused_variables)]
unsafe fn errtableconstraint(rel: Relation, conname: *const c_char) -> c_int {
    0
}

/// GUC `trace_sort` (utils/misc/guc_tables.c). Lives on the tuplesort facility.
use crate::utils::sort::tuplesort::trace_sort;

// ---------------------------------------------------------------------------
// sort-type codes for sort__start probes
// ---------------------------------------------------------------------------

const HEAP_SORT: c_int = 0;
const INDEX_SORT: c_int = 1;
const DATUM_SORT: c_int = 2;
const CLUSTER_SORT: c_int = 3;

// ---------------------------------------------------------------------------
// arg structs pointed to by TuplesortPublic.arg
// ---------------------------------------------------------------------------

/// Data structure pointed by "TuplesortPublic.arg" for the CLUSTER case.  Set by
/// the tuplesort_begin_cluster.
#[repr(C)]
struct TuplesortClusterArg {
    tupDesc: TupleDesc,
    indexInfo: *mut IndexInfo, // info about index being used for reference
    estate: *mut EState,       // for evaluating index expressions
}

/// Data structure pointed by "TuplesortPublic.arg" for the IndexTuple case.
/// Set by tuplesort_begin_index_xxx and used only by the IndexTuple routines.
#[repr(C)]
struct TuplesortIndexArg {
    heapRel: Relation,  // table the index is being built on
    indexRel: Relation, // index being built
}

/// Data structure pointed by "TuplesortPublic.arg" for the index_btree subcase.
#[repr(C)]
struct TuplesortIndexBTreeArg {
    index: TuplesortIndexArg,
    enforceUnique: bool,          // complain if we find duplicate tuples
    uniqueNullsNotDistinct: bool, // unique constraint null treatment
}

/// Data structure pointed by "TuplesortPublic.arg" for the index_hash subcase.
#[repr(C)]
struct TuplesortIndexHashArg {
    index: TuplesortIndexArg,
    high_mask: u32, // masks for sortable part of hash code
    low_mask: u32,
    max_buckets: u32,
}

/// Data structure pointed by "TuplesortPublic.arg" for the Datum case.
/// Set by tuplesort_begin_datum and used only by the DatumTuple routines.
#[repr(C)]
struct TuplesortDatumArg {
    /// the datatype oid of Datum's to be sorted
    datumType: Oid,
    /// we need typelen in order to know how to copy the Datums.
    datumTypeLen: c_int,
}

/// Computing BrinTuple size with only the tuple is difficult, so we want to track
/// the length referenced by the SortTuple. That's what BrinSortTuple is meant
/// to do - it's essentially a BrinTuple prefixed by its length.
#[repr(C)]
struct BrinSortTuple {
    tuplen: Size,
    tuple: BrinTuple,
}

/// Size of the BrinSortTuple, given length of the BrinTuple.
fn BRINSORTTUPLE_SIZE(len: usize) -> usize {
    offset_of!(BrinSortTuple, tuple) + len
}

// ---------------------------------------------------------------------------
// tuplesort_begin_* constructors
// ---------------------------------------------------------------------------

pub unsafe fn tuplesort_begin_heap(
    tupDesc: TupleDesc,
    nkeys: c_int,
    attNums: *mut AttrNumber,
    sortOperators: *mut Oid,
    sortCollations: *mut Oid,
    nullsFirstFlags: *mut bool,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext;
    let mut i: c_int;

    oldcontext = MemoryContextSwitchTo((*base).maincontext);

    Assert!(nkeys > 0);

    if trace_sort {
        elog!(
            LOG,
            "begin tuple sort: nkeys = {}, workMem = {}, randomAccess = {}",
            nkeys,
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = nkeys;

    // TRACE_POSTGRESQL_SORT_START(HEAP_SORT, false, nkeys, workMem,
    //                             sortopt & TUPLESORT_RANDOMACCESS,
    //                             PARALLEL_SORT(coordinate));
    let _ = (HEAP_SORT, PARALLEL_SORT(coordinate));

    (*base).removeabbrev = Some(removeabbrev_heap);
    (*base).comparetup = Some(comparetup_heap);
    (*base).comparetup_tiebreak = Some(comparetup_heap_tiebreak);
    (*base).writetup = Some(writetup_heap);
    (*base).readtup = Some(readtup_heap);
    (*base).haveDatum1 = true;
    (*base).arg = tupDesc as *mut c_void; // assume we need not copy tupDesc

    // Prepare SortSupport data for each column
    (*base).sortKeys =
        palloc0(nkeys as usize * size_of::<SortSupportData>()) as SortSupport;

    i = 0;
    while i < nkeys {
        let sortKey: SortSupport = (*base).sortKeys.add(i as usize);

        Assert!(*attNums.add(i as usize) != 0);
        Assert!(*sortOperators.add(i as usize) != 0);

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = *sortCollations.add(i as usize);
        (*sortKey).ssup_nulls_first = *nullsFirstFlags.add(i as usize);
        (*sortKey).ssup_attno = *attNums.add(i as usize);
        // Convey if abbreviation optimization is applicable in principle
        (*sortKey).abbreviate = i == 0 && (*base).haveDatum1;

        PrepareSortSupportFromOrderingOp(*sortOperators.add(i as usize), sortKey);

        i += 1;
    }

    /*
     * The "onlyKey" optimization cannot be used with abbreviated keys, since
     * tie-breaker comparisons may be required.  Typically, the optimization
     * is only of value to pass-by-value types anyway, whereas abbreviated
     * keys are typically only of value to pass-by-reference types.
     */
    if nkeys == 1 && (*(*base).sortKeys).abbrev_converter.is_none() {
        (*base).onlyKey = (*base).sortKeys;
    }

    MemoryContextSwitchTo(oldcontext);

    state
}

pub unsafe fn tuplesort_begin_cluster(
    tupDesc: TupleDesc,
    indexRel: Relation,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let indexScanKey: BTScanInsert;
    let oldcontext: MemoryContext;
    let arg: *mut TuplesortClusterArg;
    let mut i: c_int;

    Assert!((*(*indexRel).rd_rel).relam == BTREE_AM_OID);

    oldcontext = MemoryContextSwitchTo((*base).maincontext);
    arg = palloc0(size_of::<TuplesortClusterArg>()) as *mut TuplesortClusterArg;

    if trace_sort {
        elog!(
            LOG,
            "begin tuple sort: nkeys = {}, workMem = {}, randomAccess = {}",
            RelationGetNumberOfAttributes(indexRel),
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = IndexRelationGetNumberOfKeyAttributes(indexRel);

    // TRACE_POSTGRESQL_SORT_START(CLUSTER_SORT, false, base->nKeys, workMem,
    //                             sortopt & TUPLESORT_RANDOMACCESS,
    //                             PARALLEL_SORT(coordinate));
    let _ = (CLUSTER_SORT, PARALLEL_SORT(coordinate));

    (*base).removeabbrev = Some(removeabbrev_cluster);
    (*base).comparetup = Some(comparetup_cluster);
    (*base).comparetup_tiebreak = Some(comparetup_cluster_tiebreak);
    (*base).writetup = Some(writetup_cluster);
    (*base).readtup = Some(readtup_cluster);
    (*base).freestate = Some(freestate_cluster);
    (*base).arg = arg as *mut c_void;

    (*arg).indexInfo = BuildIndexInfo(indexRel);

    /*
     * If we don't have a simple leading attribute, we don't currently
     * initialize datum1, so disable optimizations that require it.
     */
    if (*(*arg).indexInfo).ii_IndexAttrNumbers[0] == 0 {
        (*base).haveDatum1 = false;
    } else {
        (*base).haveDatum1 = true;
    }

    (*arg).tupDesc = tupDesc; // assume we need not copy tupDesc

    indexScanKey = _bt_mkscankey(indexRel, null_mut());

    if !(*(*arg).indexInfo).ii_Expressions.is_null() {
        let slot: *mut TupleTableSlot;
        let econtext: *mut ExprContext;

        /*
         * We will need to use FormIndexDatum to evaluate the index
         * expressions.  To do that, we need an EState, as well as a
         * TupleTableSlot to put the table tuples into.  The econtext's
         * scantuple has to point to that slot, too.
         */
        (*arg).estate = CreateExecutorState();
        slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsHeapTuple);
        econtext = GetPerTupleExprContext((*arg).estate);
        (*econtext).ecxt_scantuple = slot;
    }

    // Prepare SortSupport data for each column
    (*base).sortKeys =
        palloc0((*base).nKeys as usize * size_of::<SortSupportData>()) as SortSupport;

    i = 0;
    while i < (*base).nKeys {
        let sortKey: SortSupport = (*base).sortKeys.add(i as usize);
        let scanKey: *mut ScanKeyData =
            (*indexScanKey).scankeys.as_ptr().add(i as usize) as *mut ScanKeyData;
        let reverse: bool;

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = (*scanKey).sk_collation;
        (*sortKey).ssup_nulls_first = ((*scanKey).sk_flags & SK_BT_NULLS_FIRST) != 0;
        (*sortKey).ssup_attno = (*scanKey).sk_attno;
        // Convey if abbreviation optimization is applicable in principle
        (*sortKey).abbreviate = i == 0 && (*base).haveDatum1;

        Assert!((*sortKey).ssup_attno != 0);

        reverse = ((*scanKey).sk_flags & SK_BT_DESC) != 0;

        PrepareSortSupportFromIndexRel(indexRel as *mut c_void, reverse, sortKey);

        i += 1;
    }

    pfree(indexScanKey as *mut c_void);

    MemoryContextSwitchTo(oldcontext);

    state
}

pub unsafe fn tuplesort_begin_index_btree(
    heapRel: Relation,
    indexRel: Relation,
    enforceUnique: bool,
    uniqueNullsNotDistinct: bool,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let indexScanKey: BTScanInsert;
    let arg: *mut TuplesortIndexBTreeArg;
    let oldcontext: MemoryContext;
    let mut i: c_int;

    oldcontext = MemoryContextSwitchTo((*base).maincontext);
    arg = palloc(size_of::<TuplesortIndexBTreeArg>()) as *mut TuplesortIndexBTreeArg;

    if trace_sort {
        elog!(
            LOG,
            "begin index sort: unique = {}, workMem = {}, randomAccess = {}",
            if enforceUnique { 't' } else { 'f' },
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = IndexRelationGetNumberOfKeyAttributes(indexRel);

    // TRACE_POSTGRESQL_SORT_START(INDEX_SORT, enforceUnique, base->nKeys, workMem,
    //                             sortopt & TUPLESORT_RANDOMACCESS,
    //                             PARALLEL_SORT(coordinate));
    let _ = (INDEX_SORT, PARALLEL_SORT(coordinate));

    (*base).removeabbrev = Some(removeabbrev_index);
    (*base).comparetup = Some(comparetup_index_btree);
    (*base).comparetup_tiebreak = Some(comparetup_index_btree_tiebreak);
    (*base).writetup = Some(writetup_index);
    (*base).readtup = Some(readtup_index);
    (*base).haveDatum1 = true;
    (*base).arg = arg as *mut c_void;

    (*arg).index.heapRel = heapRel;
    (*arg).index.indexRel = indexRel;
    (*arg).enforceUnique = enforceUnique;
    (*arg).uniqueNullsNotDistinct = uniqueNullsNotDistinct;

    indexScanKey = _bt_mkscankey(indexRel, null_mut());

    // Prepare SortSupport data for each column
    (*base).sortKeys =
        palloc0((*base).nKeys as usize * size_of::<SortSupportData>()) as SortSupport;

    i = 0;
    while i < (*base).nKeys {
        let sortKey: SortSupport = (*base).sortKeys.add(i as usize);
        let scanKey: *mut ScanKeyData =
            (*indexScanKey).scankeys.as_ptr().add(i as usize) as *mut ScanKeyData;
        let reverse: bool;

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = (*scanKey).sk_collation;
        (*sortKey).ssup_nulls_first = ((*scanKey).sk_flags & SK_BT_NULLS_FIRST) != 0;
        (*sortKey).ssup_attno = (*scanKey).sk_attno;
        // Convey if abbreviation optimization is applicable in principle
        (*sortKey).abbreviate = i == 0 && (*base).haveDatum1;

        Assert!((*sortKey).ssup_attno != 0);

        reverse = ((*scanKey).sk_flags & SK_BT_DESC) != 0;

        PrepareSortSupportFromIndexRel(indexRel as *mut c_void, reverse, sortKey);

        i += 1;
    }

    pfree(indexScanKey as *mut c_void);

    MemoryContextSwitchTo(oldcontext);

    state
}

pub unsafe fn tuplesort_begin_index_hash(
    heapRel: Relation,
    indexRel: Relation,
    high_mask: u32,
    low_mask: u32,
    max_buckets: u32,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext;
    let arg: *mut TuplesortIndexHashArg;

    oldcontext = MemoryContextSwitchTo((*base).maincontext);
    arg = palloc(size_of::<TuplesortIndexHashArg>()) as *mut TuplesortIndexHashArg;

    if trace_sort {
        elog!(
            LOG,
            "begin index sort: high_mask = 0x{:x}, low_mask = 0x{:x}, max_buckets = 0x{:x}, workMem = {}, randomAccess = {}",
            high_mask,
            low_mask,
            max_buckets,
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = 1; // Only one sort column, the hash code

    (*base).removeabbrev = Some(removeabbrev_index);
    (*base).comparetup = Some(comparetup_index_hash);
    (*base).comparetup_tiebreak = Some(comparetup_index_hash_tiebreak);
    (*base).writetup = Some(writetup_index);
    (*base).readtup = Some(readtup_index);
    (*base).haveDatum1 = true;
    (*base).arg = arg as *mut c_void;

    (*arg).index.heapRel = heapRel;
    (*arg).index.indexRel = indexRel;

    (*arg).high_mask = high_mask;
    (*arg).low_mask = low_mask;
    (*arg).max_buckets = max_buckets;

    MemoryContextSwitchTo(oldcontext);

    state
}

pub unsafe fn tuplesort_begin_index_gist(
    heapRel: Relation,
    indexRel: Relation,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext;
    let arg: *mut TuplesortIndexBTreeArg;
    let mut i: c_int;

    oldcontext = MemoryContextSwitchTo((*base).maincontext);
    arg = palloc(size_of::<TuplesortIndexBTreeArg>()) as *mut TuplesortIndexBTreeArg;

    if trace_sort {
        elog!(
            LOG,
            "begin index sort: workMem = {}, randomAccess = {}",
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = IndexRelationGetNumberOfKeyAttributes(indexRel);

    (*base).removeabbrev = Some(removeabbrev_index);
    (*base).comparetup = Some(comparetup_index_btree);
    (*base).comparetup_tiebreak = Some(comparetup_index_btree_tiebreak);
    (*base).writetup = Some(writetup_index);
    (*base).readtup = Some(readtup_index);
    (*base).haveDatum1 = true;
    (*base).arg = arg as *mut c_void;

    (*arg).index.heapRel = heapRel;
    (*arg).index.indexRel = indexRel;
    (*arg).enforceUnique = false;
    (*arg).uniqueNullsNotDistinct = false;

    // Prepare SortSupport data for each column
    (*base).sortKeys =
        palloc0((*base).nKeys as usize * size_of::<SortSupportData>()) as SortSupport;

    i = 0;
    while i < (*base).nKeys {
        let sortKey: SortSupport = (*base).sortKeys.add(i as usize);

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = *(*indexRel).rd_indcollation.add(i as usize);
        (*sortKey).ssup_nulls_first = false;
        (*sortKey).ssup_attno = (i + 1) as AttrNumber;
        // Convey if abbreviation optimization is applicable in principle
        (*sortKey).abbreviate = i == 0 && (*base).haveDatum1;

        Assert!((*sortKey).ssup_attno != 0);

        // Look for a sort support function
        PrepareSortSupportFromGistIndexRel(indexRel as *mut c_void, sortKey);

        i += 1;
    }

    MemoryContextSwitchTo(oldcontext);

    state
}

pub unsafe fn tuplesort_begin_index_brin(
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);

    if trace_sort {
        elog!(
            LOG,
            "begin index sort: workMem = {}, randomAccess = {}",
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = 1; // Only one sort column, the block number

    (*base).removeabbrev = Some(removeabbrev_index_brin);
    (*base).comparetup = Some(comparetup_index_brin);
    (*base).writetup = Some(writetup_index_brin);
    (*base).readtup = Some(readtup_index_brin);
    (*base).haveDatum1 = true;
    (*base).arg = null_mut();

    state
}

pub unsafe fn tuplesort_begin_index_gin(
    heapRel: Relation,
    indexRel: Relation,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let _ = (heapRel,);
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext;
    let mut i: c_int;
    let desc: TupleDesc = RelationGetDescr(indexRel);

    oldcontext = MemoryContextSwitchTo((*base).maincontext);

    // #ifdef TRACE_SORT
    if trace_sort {
        elog!(
            LOG,
            "begin index sort: workMem = {}, randomAccess = {}",
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }
    // #endif

    /*
     * Multi-column GIN indexes expand the row into a separate index entry for
     * attribute, and that's what we write into the tuplesort. But we still
     * need to initialize sortsupport for all the attributes.
     */
    (*base).nKeys = IndexRelationGetNumberOfKeyAttributes(indexRel);

    // Prepare SortSupport data for each column
    (*base).sortKeys =
        palloc0((*base).nKeys as usize * size_of::<SortSupportData>()) as SortSupport;

    i = 0;
    while i < (*base).nKeys {
        let sortKey: SortSupport = (*base).sortKeys.add(i as usize);
        let att = TupleDescAttr(desc, i);
        let mut cmpFunc: Oid;

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = *(*indexRel).rd_indcollation.add(i as usize);
        (*sortKey).ssup_nulls_first = false;
        (*sortKey).ssup_attno = (i + 1) as AttrNumber;
        (*sortKey).abbreviate = false;

        Assert!((*sortKey).ssup_attno != 0);

        if !OidIsValid((*sortKey).ssup_collation) {
            (*sortKey).ssup_collation = DEFAULT_COLLATION_OID;
        }

        /*
         * If the compare proc isn't specified in the opclass definition, look
         * up the index key type's default btree comparator.
         */
        cmpFunc = index_getprocid(indexRel, (i + 1) as AttrNumber, GIN_COMPARE_PROC as u16);
        if cmpFunc == InvalidOid {
            let typentry: *mut TypeCacheEntry;

            typentry = lookup_type_cache((*att).atttypid, TYPECACHE_CMP_PROC_FINFO);
            if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify a comparison function for type {}",
                        std::ffi::CStr::from_ptr(format_type_be((*att).atttypid))
                            .to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_UNDEFINED_FUNCTION)
            }

            cmpFunc = (*typentry).cmp_proc_finfo.fn_oid;
        }

        PrepareSortSupportComparisonShim(cmpFunc, sortKey);

        i += 1;
    }

    (*base).removeabbrev = Some(removeabbrev_index_gin);
    (*base).comparetup = Some(comparetup_index_gin);
    (*base).writetup = Some(writetup_index_gin);
    (*base).readtup = Some(readtup_index_gin);
    (*base).haveDatum1 = false;
    (*base).arg = null_mut();

    MemoryContextSwitchTo(oldcontext);

    state
}

pub unsafe fn tuplesort_begin_datum(
    datumType: Oid,
    sortOperator: Oid,
    sortCollation: Oid,
    nullsFirstFlag: bool,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state = tuplesort_begin_common(workMem, coordinate, sortopt);
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortDatumArg;
    let oldcontext: MemoryContext;
    let mut typlen: i16 = 0;
    let mut typbyval: bool = false;

    oldcontext = MemoryContextSwitchTo((*base).maincontext);
    arg = palloc(size_of::<TuplesortDatumArg>()) as *mut TuplesortDatumArg;

    if trace_sort {
        elog!(
            LOG,
            "begin datum sort: workMem = {}, randomAccess = {}",
            workMem,
            if sortopt & TUPLESORT_RANDOMACCESS != 0 { 't' } else { 'f' }
        );
    }

    (*base).nKeys = 1; // always a one-column sort

    // TRACE_POSTGRESQL_SORT_START(DATUM_SORT, false, 1, workMem,
    //                             sortopt & TUPLESORT_RANDOMACCESS,
    //                             PARALLEL_SORT(coordinate));
    let _ = (DATUM_SORT, PARALLEL_SORT(coordinate));

    (*base).removeabbrev = Some(removeabbrev_datum);
    (*base).comparetup = Some(comparetup_datum);
    (*base).comparetup_tiebreak = Some(comparetup_datum_tiebreak);
    (*base).writetup = Some(writetup_datum);
    (*base).readtup = Some(readtup_datum);
    (*base).haveDatum1 = true;
    (*base).arg = arg as *mut c_void;

    (*arg).datumType = datumType;

    // lookup necessary attributes of the datum type
    get_typlenbyval(datumType, &mut typlen, &mut typbyval);
    (*arg).datumTypeLen = typlen as c_int;
    (*base).tuples = !typbyval;

    // Prepare SortSupport data
    (*base).sortKeys = palloc0(size_of::<SortSupportData>()) as SortSupport;

    (*(*base).sortKeys).ssup_cxt = CurrentMemoryContext;
    (*(*base).sortKeys).ssup_collation = sortCollation;
    (*(*base).sortKeys).ssup_nulls_first = nullsFirstFlag;

    /*
     * Abbreviation is possible here only for by-reference types.  In theory,
     * a pass-by-value datatype could have an abbreviated form that is cheaper
     * to compare.  In a tuple sort, we could support that, because we can
     * always extract the original datum from the tuple as needed.  Here, we
     * can't, because a datum sort only stores a single copy of the datum; the
     * "tuple" field of each SortTuple is NULL.
     */
    (*(*base).sortKeys).abbreviate = !typbyval;

    PrepareSortSupportFromOrderingOp(sortOperator, (*base).sortKeys);

    /*
     * The "onlyKey" optimization cannot be used with abbreviated keys, since
     * tie-breaker comparisons may be required.  Typically, the optimization
     * is only of value to pass-by-value types anyway, whereas abbreviated
     * keys are typically only of value to pass-by-reference types.
     */
    if (*(*base).sortKeys).abbrev_converter.is_none() {
        (*base).onlyKey = (*base).sortKeys;
    }

    MemoryContextSwitchTo(oldcontext);

    state
}

/// `IndexRelationGetNumberOfKeyAttributes` (utils/rel.h).
// TODO(pg-port): import from crate::utils::rel once exposed.
unsafe fn IndexRelationGetNumberOfKeyAttributes(relation: Relation) -> c_int {
    (*(*relation).rd_index).indnkeyatts as c_int
}

// ---------------------------------------------------------------------------
// Additional dependencies used by the put/get and callback routines below.
// ---------------------------------------------------------------------------

// "utils/mmgr/mcxt.c" -- GetMemoryChunkSpace.
use crate::utils::mmgr::mcxt::GetMemoryChunkSpace;

// libc memcpy (used by the put routines that copy whole tuples).
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/// `LogicalTapeReadExact(tape, ptr, len)` (utils/tuplesort.h).
macro_rules! LogicalTapeReadExact {
    ($tape:expr, $ptr:expr, $len:expr) => {{
        if LogicalTapeRead($tape, $ptr, $len) != ($len as usize) {
            elog!(ERROR, "unexpected end of data");
        }
    }};
}

// ---------------------------------------------------------------------------
// tuplesort_put* -- accept input tuples
// ---------------------------------------------------------------------------

pub unsafe fn tuplesort_puttupleslot(state: *mut Tuplesortstate, slot: *mut TupleTableSlot) {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).tuplecontext);
    let tupDesc: TupleDesc = (*base).arg as TupleDesc;
    let mut stup: SortTuple = core::mem::zeroed();
    let tuple: MinimalTuple;
    let mut htup: HeapTupleData = core::mem::zeroed();
    let tuplen: Size;

    // copy the tuple into sort storage
    tuple = ExecCopySlotMinimalTuple(slot);
    stup.tuple = tuple as *mut c_void;
    // set up first-column key value
    htup.t_len = (*tuple).t_len + MINIMAL_TUPLE_OFFSET as u32;
    htup.t_data = ((tuple as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize)))
        as HeapTupleHeader;
    stup.datum1 = heap_getattr(
        &mut htup,
        (*(*base).sortKeys.add(0)).ssup_attno as c_int,
        tupDesc,
        &mut stup.isnull1,
    );

    // GetMemoryChunkSpace is not supported for bump contexts
    if TupleSortUseBumpTupleCxt((*base).sortopt) {
        tuplen = MAXALIGN((*tuple).t_len as usize);
    } else {
        tuplen = GetMemoryChunkSpace(tuple as *mut c_void);
    }

    tuplesort_puttuple_common(
        state,
        &mut stup,
        (*(*base).sortKeys).abbrev_converter.is_some() && !stup.isnull1,
        tuplen,
    );

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Accept one tuple while collecting input data for sort.
 *
 * Note that the input data is always copied; the caller need not save it.
 */
pub unsafe fn tuplesort_putheaptuple(state: *mut Tuplesortstate, mut tup: HeapTuple) {
    let mut stup: SortTuple = core::mem::zeroed();
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).tuplecontext);
    let arg: *mut TuplesortClusterArg = (*base).arg as *mut TuplesortClusterArg;
    let tuplen: Size;

    // copy the tuple into sort storage
    tup = heap_copytuple(tup);
    stup.tuple = tup as *mut c_void;

    /*
     * set up first-column key value, and potentially abbreviate, if it's a
     * simple column
     */
    if (*base).haveDatum1 {
        stup.datum1 = heap_getattr(
            tup,
            (*(*arg).indexInfo).ii_IndexAttrNumbers[0] as c_int,
            (*arg).tupDesc,
            &mut stup.isnull1,
        );
    }

    // GetMemoryChunkSpace is not supported for bump contexts
    if TupleSortUseBumpTupleCxt((*base).sortopt) {
        tuplen = MAXALIGN(HEAPTUPLESIZE + (*tup).t_len as usize);
    } else {
        tuplen = GetMemoryChunkSpace(tup as *mut c_void);
    }

    tuplesort_puttuple_common(
        state,
        &mut stup,
        (*base).haveDatum1
            && (*(*base).sortKeys).abbrev_converter.is_some()
            && !stup.isnull1,
        tuplen,
    );

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Collect one index tuple while collecting input data for sort, building
 * it from caller-supplied values.
 */
pub unsafe fn tuplesort_putindextuplevalues(
    state: *mut Tuplesortstate,
    rel: Relation,
    self_: ItemPointer,
    values: *const Datum,
    isnull: *const bool,
) {
    let mut stup: SortTuple = core::mem::zeroed();
    let tuple: IndexTuple;
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortIndexArg = (*base).arg as *mut TuplesortIndexArg;
    let tuplen: Size;

    stup.tuple = index_form_tuple_context(
        RelationGetDescr(rel),
        values,
        isnull,
        (*base).tuplecontext,
    ) as *mut c_void;
    tuple = stup.tuple as IndexTuple;
    (*tuple).t_tid = *self_;
    // set up first-column key value
    stup.datum1 = index_getattr(
        tuple,
        1,
        RelationGetDescr((*arg).indexRel),
        &mut stup.isnull1,
    );

    // GetMemoryChunkSpace is not supported for bump contexts
    if TupleSortUseBumpTupleCxt((*base).sortopt) {
        tuplen = MAXALIGN(((*tuple).t_info & INDEX_SIZE_MASK) as usize);
    } else {
        tuplen = GetMemoryChunkSpace(tuple as *mut c_void);
    }

    tuplesort_puttuple_common(
        state,
        &mut stup,
        !(*base).sortKeys.is_null()
            && (*(*base).sortKeys).abbrev_converter.is_some()
            && !stup.isnull1,
        tuplen,
    );
}

/*
 * Collect one BRIN tuple while collecting input data for sort.
 */
pub unsafe fn tuplesort_putbrintuple(state: *mut Tuplesortstate, tuple: *mut BrinTuple, size: Size) {
    let mut stup: SortTuple = core::mem::zeroed();
    let bstup: *mut BrinSortTuple;
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).tuplecontext);
    let tuplen: Size;

    // allocate space for the whole BRIN sort tuple
    bstup = palloc(BRINSORTTUPLE_SIZE(size)) as *mut BrinSortTuple;

    (*bstup).tuplen = size;
    memcpy(
        &mut (*bstup).tuple as *mut BrinTuple as *mut c_void,
        tuple as *const c_void,
        size,
    );

    stup.tuple = bstup as *mut c_void;
    stup.datum1 = (*tuple).bt_blkno as Datum;
    stup.isnull1 = false;

    // GetMemoryChunkSpace is not supported for bump contexts
    if TupleSortUseBumpTupleCxt((*base).sortopt) {
        tuplen = MAXALIGN(BRINSORTTUPLE_SIZE(size));
    } else {
        tuplen = GetMemoryChunkSpace(bstup as *mut c_void);
    }

    tuplesort_puttuple_common(
        state,
        &mut stup,
        !(*base).sortKeys.is_null()
            && (*(*base).sortKeys).abbrev_converter.is_some()
            && !stup.isnull1,
        tuplen,
    );

    MemoryContextSwitchTo(oldcontext);
}

pub unsafe fn tuplesort_putgintuple(state: *mut Tuplesortstate, tuple: *mut GinTuple, size: Size) {
    let mut stup: SortTuple = core::mem::zeroed();
    let ctup: *mut GinTuple;
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).tuplecontext);
    let tuplen: Size;

    // copy the GinTuple into the right memory context
    ctup = palloc(size) as *mut GinTuple;
    memcpy(ctup as *mut c_void, tuple as *const c_void, size);

    stup.tuple = ctup as *mut c_void;
    stup.datum1 = 0 as Datum;
    stup.isnull1 = false;

    // GetMemoryChunkSpace is not supported for bump contexts
    if TupleSortUseBumpTupleCxt((*base).sortopt) {
        tuplen = MAXALIGN(size);
    } else {
        tuplen = GetMemoryChunkSpace(ctup as *mut c_void);
    }

    tuplesort_puttuple_common(
        state,
        &mut stup,
        !(*base).sortKeys.is_null()
            && (*(*base).sortKeys).abbrev_converter.is_some()
            && !stup.isnull1,
        tuplen,
    );

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Accept one Datum while collecting input data for sort.
 *
 * If the Datum is pass-by-ref type, the value will be copied.
 */
pub unsafe fn tuplesort_putdatum(state: *mut Tuplesortstate, val: Datum, isNull: bool) {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).tuplecontext);
    let arg: *mut TuplesortDatumArg = (*base).arg as *mut TuplesortDatumArg;
    let mut stup: SortTuple = core::mem::zeroed();

    /*
     * Pass-by-value types or null values are just stored directly in
     * stup.datum1 (and stup.tuple is not used and set to NULL).
     *
     * Non-null pass-by-reference values need to be copied into memory we
     * control, and possibly abbreviated. The copied value is pointed to by
     * stup.tuple and is treated as the canonical copy (e.g. to return via
     * tuplesort_getdatum or when writing to tape); stup.datum1 gets the
     * abbreviated value if abbreviation is happening, otherwise it's
     * identical to stup.tuple.
     */

    if isNull || !(*base).tuples {
        /*
         * Set datum1 to zeroed representation for NULLs (to be consistent,
         * and to support cheap inequality tests for NULL abbreviated keys).
         */
        stup.datum1 = if !isNull { val } else { 0 as Datum };
        stup.isnull1 = isNull;
        stup.tuple = null_mut(); // no separate storage
    } else {
        stup.isnull1 = false;
        stup.datum1 = datumCopy(val, false, (*arg).datumTypeLen);
        stup.tuple = DatumGetPointer(stup.datum1) as *mut c_void;
    }

    tuplesort_puttuple_common(
        state,
        &mut stup,
        (*base).tuples && (*(*base).sortKeys).abbrev_converter.is_some() && !isNull,
        0,
    );

    MemoryContextSwitchTo(oldcontext);
}

// ---------------------------------------------------------------------------
// tuplesort_get* -- retrieve sorted tuples
// ---------------------------------------------------------------------------

/*
 * Fetch the next tuple in either forward or back direction.
 * If successful, put tuple in slot and return true; else, clear the slot
 * and return false.
 *
 * Caller may optionally be passed back abbreviated value (on true return
 * value) when abbreviation was used, which can be used to cheaply avoid
 * equality checks that might otherwise be required.  Caller can safely make a
 * determination of "non-equal tuple" based on simple binary inequality.  A
 * NULL value in leading attribute will set abbreviated value to zeroed
 * representation, which caller may rely on in abbreviated inequality check.
 *
 * If copy is true, the slot receives a tuple that's been copied into the
 * caller's memory context, so that it will stay valid regardless of future
 * manipulations of the tuplesort's state (up to and including deleting the
 * tuplesort).  If copy is false, the slot will just receive a pointer to a
 * tuple held within the tuplesort, which is more efficient, but only safe for
 * callers that are prepared to have any subsequent manipulation of the
 * tuplesort's state invalidate slot contents.
 */
pub unsafe fn tuplesort_gettupleslot(
    state: *mut Tuplesortstate,
    forward: bool,
    copy: bool,
    slot: *mut TupleTableSlot,
    abbrev: *mut Datum,
) -> bool {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).sortcontext);
    let mut stup: SortTuple = core::mem::zeroed();

    if !tuplesort_gettuple_common(state, forward, &mut stup) {
        stup.tuple = null_mut();
    }

    MemoryContextSwitchTo(oldcontext);

    if !stup.tuple.is_null() {
        // Record abbreviated key for caller
        if (*(*base).sortKeys).abbrev_converter.is_some() && !abbrev.is_null() {
            *abbrev = stup.datum1;
        }

        if copy {
            stup.tuple = heap_copy_minimal_tuple(stup.tuple as MinimalTuple, 0) as *mut c_void;
        }

        ExecStoreMinimalTuple(stup.tuple as MinimalTuple, slot, copy);
        true
    } else {
        ExecClearTuple(slot);
        false
    }
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  Returned tuple belongs to tuplesort memory
 * context, and must not be freed by caller.  Caller may not rely on tuple
 * remaining valid after any further manipulation of tuplesort.
 */
pub unsafe fn tuplesort_getheaptuple(state: *mut Tuplesortstate, forward: bool) -> HeapTuple {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).sortcontext);
    let mut stup: SortTuple = core::mem::zeroed();

    if !tuplesort_gettuple_common(state, forward, &mut stup) {
        stup.tuple = null_mut();
    }

    MemoryContextSwitchTo(oldcontext);

    stup.tuple as HeapTuple
}

/*
 * Fetch the next index tuple in either forward or back direction.
 * Returns NULL if no more tuples.  Returned tuple belongs to tuplesort memory
 * context, and must not be freed by caller.  Caller may not rely on tuple
 * remaining valid after any further manipulation of tuplesort.
 */
pub unsafe fn tuplesort_getindextuple(state: *mut Tuplesortstate, forward: bool) -> IndexTuple {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).sortcontext);
    let mut stup: SortTuple = core::mem::zeroed();

    if !tuplesort_gettuple_common(state, forward, &mut stup) {
        stup.tuple = null_mut();
    }

    MemoryContextSwitchTo(oldcontext);

    stup.tuple as IndexTuple
}

/*
 * Fetch the next BRIN tuple in either forward or back direction.
 * Returns NULL if no more tuples.  Returned tuple belongs to tuplesort memory
 * context, and must not be freed by caller.  Caller may not rely on tuple
 * remaining valid after any further manipulation of tuplesort.
 */
pub unsafe fn tuplesort_getbrintuple(
    state: *mut Tuplesortstate,
    len: *mut Size,
    forward: bool,
) -> *mut BrinTuple {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).sortcontext);
    let mut stup: SortTuple = core::mem::zeroed();
    let btup: *mut BrinSortTuple;

    if !tuplesort_gettuple_common(state, forward, &mut stup) {
        stup.tuple = null_mut();
    }

    MemoryContextSwitchTo(oldcontext);

    if stup.tuple.is_null() {
        return null_mut();
    }

    btup = stup.tuple as *mut BrinSortTuple;

    *len = (*btup).tuplen;

    &mut (*btup).tuple
}

pub unsafe fn tuplesort_getgintuple(
    state: *mut Tuplesortstate,
    len: *mut Size,
    forward: bool,
) -> *mut GinTuple {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).sortcontext);
    let mut stup: SortTuple = core::mem::zeroed();
    let tup: *mut GinTuple;

    if !tuplesort_gettuple_common(state, forward, &mut stup) {
        stup.tuple = null_mut();
    }

    MemoryContextSwitchTo(oldcontext);

    if stup.tuple.is_null() {
        return null_mut();
    }

    tup = stup.tuple as *mut GinTuple;

    *len = (*tup).tuplen as Size;

    tup
}

/*
 * Fetch the next Datum in either forward or back direction.
 * Returns false if no more datums.
 *
 * If the Datum is pass-by-ref type, the returned value is freshly palloc'd
 * in caller's context, and is now owned by the caller (this differs from
 * similar routines for other types of tuplesorts).
 *
 * Caller may optionally be passed back abbreviated value (on true return
 * value) when abbreviation was used, which can be used to cheaply avoid
 * equality checks that might otherwise be required.  Caller can safely make a
 * determination of "non-equal tuple" based on simple binary inequality.  A
 * NULL value will have a zeroed abbreviated value representation, which caller
 * may rely on in abbreviated inequality check.
 *
 * For byref Datums, if copy is true, *val is set to a copy of the Datum
 * copied into the caller's memory context, so that it will stay valid
 * regardless of future manipulations of the tuplesort's state (up to and
 * including deleting the tuplesort).  If copy is false, *val will just be
 * set to a pointer to the Datum held within the tuplesort, which is more
 * efficient, but only safe for callers that are prepared to have any
 * subsequent manipulation of the tuplesort's state invalidate slot contents.
 * For byval Datums, the value of the 'copy' parameter has no effect.
 */
pub unsafe fn tuplesort_getdatum(
    state: *mut Tuplesortstate,
    forward: bool,
    copy: bool,
    val: *mut Datum,
    isNull: *mut bool,
    abbrev: *mut Datum,
) -> bool {
    let base = TuplesortstateGetPublic(state);
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*base).sortcontext);
    let arg: *mut TuplesortDatumArg = (*base).arg as *mut TuplesortDatumArg;
    let mut stup: SortTuple = core::mem::zeroed();

    if !tuplesort_gettuple_common(state, forward, &mut stup) {
        MemoryContextSwitchTo(oldcontext);
        return false;
    }

    // Ensure we copy into caller's memory context
    MemoryContextSwitchTo(oldcontext);

    // Record abbreviated key for caller
    if (*(*base).sortKeys).abbrev_converter.is_some() && !abbrev.is_null() {
        *abbrev = stup.datum1;
    }

    if stup.isnull1 || !(*base).tuples {
        *val = stup.datum1;
        *isNull = stup.isnull1;
    } else {
        // use stup.tuple because stup.datum1 may be an abbreviation
        if copy {
            *val = datumCopy(
                PointerGetDatum(stup.tuple),
                false,
                (*arg).datumTypeLen,
            );
        } else {
            *val = PointerGetDatum(stup.tuple);
        }
        *isNull = false;
    }

    true
}

// ---------------------------------------------------------------------------
// Routines specialized for HeapTuple (actually MinimalTuple) case
// ---------------------------------------------------------------------------

unsafe fn removeabbrev_heap(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int) {
    let mut i: c_int;
    let base = TuplesortstateGetPublic(state);

    i = 0;
    while i < count {
        let mut htup: HeapTupleData = core::mem::zeroed();

        htup.t_len =
            (*((*stups.add(i as usize)).tuple as MinimalTuple)).t_len + MINIMAL_TUPLE_OFFSET as u32;
        htup.t_data = (((*stups.add(i as usize)).tuple as *mut c_char)
            .offset(-(MINIMAL_TUPLE_OFFSET as isize))) as HeapTupleHeader;
        (*stups.add(i as usize)).datum1 = heap_getattr(
            &mut htup,
            (*(*base).sortKeys.add(0)).ssup_attno as c_int,
            (*base).arg as TupleDesc,
            &mut (*stups.add(i as usize)).isnull1,
        );
        i += 1;
    }
}

unsafe fn comparetup_heap(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let sortKey: SortSupport = (*base).sortKeys;
    let compare: i32;

    // Compare the leading sort key
    compare = ApplySortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        sortKey,
    );
    if compare != 0 {
        return compare;
    }

    // Compare additional sort keys
    comparetup_heap_tiebreak(a, b, state)
}

unsafe fn comparetup_heap_tiebreak(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let mut sortKey: SortSupport = (*base).sortKeys;
    let mut ltup: HeapTupleData = core::mem::zeroed();
    let mut rtup: HeapTupleData = core::mem::zeroed();
    let tupDesc: TupleDesc;
    let mut nkey: c_int;
    let mut compare: i32;
    let mut attno: AttrNumber;
    let mut datum1: Datum;
    let mut datum2: Datum;
    let mut isnull1: bool = false;
    let mut isnull2: bool = false;

    ltup.t_len = (*((*a).tuple as MinimalTuple)).t_len + MINIMAL_TUPLE_OFFSET as u32;
    ltup.t_data =
        (((*a).tuple as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize))) as HeapTupleHeader;
    rtup.t_len = (*((*b).tuple as MinimalTuple)).t_len + MINIMAL_TUPLE_OFFSET as u32;
    rtup.t_data =
        (((*b).tuple as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize))) as HeapTupleHeader;
    tupDesc = (*base).arg as TupleDesc;

    if (*sortKey).abbrev_converter.is_some() {
        attno = (*sortKey).ssup_attno;

        datum1 = heap_getattr(&mut ltup, attno as c_int, tupDesc, &mut isnull1);
        datum2 = heap_getattr(&mut rtup, attno as c_int, tupDesc, &mut isnull2);

        compare = ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if compare != 0 {
            return compare;
        }
    }

    sortKey = sortKey.add(1);
    nkey = 1;
    while nkey < (*base).nKeys {
        attno = (*sortKey).ssup_attno;

        datum1 = heap_getattr(&mut ltup, attno as c_int, tupDesc, &mut isnull1);
        datum2 = heap_getattr(&mut rtup, attno as c_int, tupDesc, &mut isnull2);

        compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if compare != 0 {
            return compare;
        }

        nkey += 1;
        sortKey = sortKey.add(1);
    }

    0
}

unsafe fn writetup_heap(state: *mut Tuplesortstate, tape: *mut LogicalTape, stup: *mut SortTuple) {
    let base = TuplesortstateGetPublic(state);
    let tuple: MinimalTuple = (*stup).tuple as MinimalTuple;

    // the part of the MinimalTuple we'll write:
    let tupbody: *mut c_char = (tuple as *mut c_char).add(MINIMAL_TUPLE_DATA_OFFSET);
    let tupbodylen: c_uint = (*tuple).t_len - MINIMAL_TUPLE_DATA_OFFSET as u32;

    // total on-disk footprint:
    let mut tuplen: c_uint = tupbodylen + size_of::<c_int>() as c_uint;

    LogicalTapeWrite(
        tape,
        &mut tuplen as *mut c_uint as *const c_void,
        size_of::<c_uint>(),
    );
    LogicalTapeWrite(tape, tupbody as *const c_void, tupbodylen as usize);
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeWrite(
            tape,
            &mut tuplen as *mut c_uint as *const c_void,
            size_of::<c_uint>(),
        );
    }
}

unsafe fn readtup_heap(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    len: c_uint,
) {
    let tupbodylen: c_uint = len - size_of::<c_int>() as c_uint;
    let tuplen: c_uint = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET as u32;
    let tuple: MinimalTuple =
        tuplesort_readtup_alloc(state, tuplen as Size) as MinimalTuple;
    let tupbody: *mut c_char = (tuple as *mut c_char).add(MINIMAL_TUPLE_DATA_OFFSET);
    let base = TuplesortstateGetPublic(state);
    let mut htup: HeapTupleData = core::mem::zeroed();
    let mut tuplen_trail: c_uint = 0;

    // read in the tuple proper
    (*tuple).t_len = tuplen;
    LogicalTapeReadExact!(tape, tupbody as *mut c_void, tupbodylen as usize);
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeReadExact!(
            tape,
            &mut tuplen_trail as *mut c_uint as *mut c_void,
            size_of::<c_uint>()
        );
    }
    (*stup).tuple = tuple as *mut c_void;
    // set up first-column key value
    htup.t_len = (*tuple).t_len + MINIMAL_TUPLE_OFFSET as u32;
    htup.t_data =
        ((tuple as *mut c_char).offset(-(MINIMAL_TUPLE_OFFSET as isize))) as HeapTupleHeader;
    (*stup).datum1 = heap_getattr(
        &mut htup,
        (*(*base).sortKeys.add(0)).ssup_attno as c_int,
        (*base).arg as TupleDesc,
        &mut (*stup).isnull1,
    );
}

// ---------------------------------------------------------------------------
// Routines specialized for the CLUSTER case (HeapTuple data, with
// comparisons per a btree index definition)
// ---------------------------------------------------------------------------

unsafe fn removeabbrev_cluster(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int) {
    let mut i: c_int;
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortClusterArg = (*base).arg as *mut TuplesortClusterArg;

    i = 0;
    while i < count {
        let tup: HeapTuple;

        tup = (*stups.add(i as usize)).tuple as HeapTuple;
        (*stups.add(i as usize)).datum1 = heap_getattr(
            tup,
            (*(*arg).indexInfo).ii_IndexAttrNumbers[0] as c_int,
            (*arg).tupDesc,
            &mut (*stups.add(i as usize)).isnull1,
        );
        i += 1;
    }
}

unsafe fn comparetup_cluster(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let sortKey: SortSupport = (*base).sortKeys;
    let compare: i32;

    // Compare the leading sort key, if it's simple
    if (*base).haveDatum1 {
        compare = ApplySortComparator(
            (*a).datum1,
            (*a).isnull1,
            (*b).datum1,
            (*b).isnull1,
            sortKey,
        );
        if compare != 0 {
            return compare;
        }
    }

    comparetup_cluster_tiebreak(a, b, state)
}

unsafe fn comparetup_cluster_tiebreak(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortClusterArg = (*base).arg as *mut TuplesortClusterArg;
    let mut sortKey: SortSupport = (*base).sortKeys;
    let ltup: HeapTuple;
    let rtup: HeapTuple;
    let tupDesc: TupleDesc;
    let mut nkey: c_int;
    let mut compare: i32 = 0;
    let mut datum1: Datum;
    let mut datum2: Datum;
    let mut isnull1: bool = false;
    let mut isnull2: bool = false;

    ltup = (*a).tuple as HeapTuple;
    rtup = (*b).tuple as HeapTuple;
    tupDesc = (*arg).tupDesc;

    // Compare the leading sort key, if it's simple
    if (*base).haveDatum1 {
        if (*sortKey).abbrev_converter.is_some() {
            let leading: AttrNumber = (*(*arg).indexInfo).ii_IndexAttrNumbers[0];

            datum1 = heap_getattr(ltup, leading as c_int, tupDesc, &mut isnull1);
            datum2 = heap_getattr(rtup, leading as c_int, tupDesc, &mut isnull2);

            compare = ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2, sortKey);
        }
        if compare != 0 || (*base).nKeys == 1 {
            return compare;
        }
        // Compare additional columns the hard way
        sortKey = sortKey.add(1);
        nkey = 1;
    } else {
        // Must compare all keys the hard way
        nkey = 0;
    }

    if (*(*arg).indexInfo).ii_Expressions.is_null() {
        // If not expression index, just compare the proper heap attrs

        while nkey < (*base).nKeys {
            let attno: AttrNumber = (*(*arg).indexInfo).ii_IndexAttrNumbers[nkey as usize];

            datum1 = heap_getattr(ltup, attno as c_int, tupDesc, &mut isnull1);
            datum2 = heap_getattr(rtup, attno as c_int, tupDesc, &mut isnull2);

            compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
            if compare != 0 {
                return compare;
            }

            nkey += 1;
            sortKey = sortKey.add(1);
        }
    } else {
        /*
         * In the expression index case, compute the whole index tuple and
         * then compare values.  It would perhaps be faster to compute only as
         * many columns as we need to compare, but that would require
         * duplicating all the logic in FormIndexDatum.
         */
        let mut l_index_values: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
        let mut l_index_isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
        let mut r_index_values: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
        let mut r_index_isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
        let ecxt_scantuple: *mut TupleTableSlot;

        // Reset context each time to prevent memory leakage
        ResetPerTupleExprContext((*arg).estate);

        ecxt_scantuple = (*GetPerTupleExprContext((*arg).estate)).ecxt_scantuple;

        ExecStoreHeapTuple(ltup, ecxt_scantuple, false);
        FormIndexDatum(
            (*arg).indexInfo,
            ecxt_scantuple,
            (*arg).estate,
            l_index_values.as_mut_ptr(),
            l_index_isnull.as_mut_ptr(),
        );

        ExecStoreHeapTuple(rtup, ecxt_scantuple, false);
        FormIndexDatum(
            (*arg).indexInfo,
            ecxt_scantuple,
            (*arg).estate,
            r_index_values.as_mut_ptr(),
            r_index_isnull.as_mut_ptr(),
        );

        while nkey < (*base).nKeys {
            compare = ApplySortComparator(
                l_index_values[nkey as usize],
                l_index_isnull[nkey as usize],
                r_index_values[nkey as usize],
                r_index_isnull[nkey as usize],
                sortKey,
            );
            if compare != 0 {
                return compare;
            }

            nkey += 1;
            sortKey = sortKey.add(1);
        }
    }

    0
}

unsafe fn writetup_cluster(
    state: *mut Tuplesortstate,
    tape: *mut LogicalTape,
    stup: *mut SortTuple,
) {
    let base = TuplesortstateGetPublic(state);
    let tuple: HeapTuple = (*stup).tuple as HeapTuple;
    let mut tuplen: c_uint =
        (*tuple).t_len + size_of::<ItemPointerData>() as c_uint + size_of::<c_int>() as c_uint;

    // We need to store t_self, but not other fields of HeapTupleData
    LogicalTapeWrite(
        tape,
        &mut tuplen as *mut c_uint as *const c_void,
        size_of::<c_uint>(),
    );
    LogicalTapeWrite(
        tape,
        &(*tuple).t_self as *const ItemPointerData as *const c_void,
        size_of::<ItemPointerData>(),
    );
    LogicalTapeWrite(
        tape,
        (*tuple).t_data as *const c_void,
        (*tuple).t_len as usize,
    );
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeWrite(
            tape,
            &mut tuplen as *mut c_uint as *const c_void,
            size_of::<c_uint>(),
        );
    }
}

unsafe fn readtup_cluster(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    tuplen: c_uint,
) {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortClusterArg = (*base).arg as *mut TuplesortClusterArg;
    let t_len: c_uint =
        tuplen - size_of::<ItemPointerData>() as c_uint - size_of::<c_int>() as c_uint;
    let tuple: HeapTuple =
        tuplesort_readtup_alloc(state, t_len as usize + HEAPTUPLESIZE) as HeapTuple;
    let mut tuplen_trail: c_uint = 0;

    // Reconstruct the HeapTupleData header
    (*tuple).t_data = ((tuple as *mut c_char).add(HEAPTUPLESIZE)) as HeapTupleHeader;
    (*tuple).t_len = t_len;
    LogicalTapeReadExact!(
        tape,
        &mut (*tuple).t_self as *mut ItemPointerData as *mut c_void,
        size_of::<ItemPointerData>()
    );
    // We don't currently bother to reconstruct t_tableOid
    (*tuple).t_tableOid = InvalidOid;
    // Read in the tuple body
    LogicalTapeReadExact!(
        tape,
        (*tuple).t_data as *mut c_void,
        (*tuple).t_len as usize
    );
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeReadExact!(
            tape,
            &mut tuplen_trail as *mut c_uint as *mut c_void,
            size_of::<c_uint>()
        );
    }
    (*stup).tuple = tuple as *mut c_void;
    // set up first-column key value, if it's a simple column
    if (*base).haveDatum1 {
        (*stup).datum1 = heap_getattr(
            tuple,
            (*(*arg).indexInfo).ii_IndexAttrNumbers[0] as c_int,
            (*arg).tupDesc,
            &mut (*stup).isnull1,
        );
    }
}

unsafe fn freestate_cluster(state: *mut Tuplesortstate) {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortClusterArg = (*base).arg as *mut TuplesortClusterArg;

    // Free any execution state created for CLUSTER case
    if !(*arg).estate.is_null() {
        let econtext: *mut ExprContext = GetPerTupleExprContext((*arg).estate);

        ExecDropSingleTupleTableSlot((*econtext).ecxt_scantuple);
        FreeExecutorState((*arg).estate);
    }
}

// ---------------------------------------------------------------------------
// Routines specialized for IndexTuple case
//
// The btree and hash cases require separate comparison functions, but the
// IndexTuple representation is the same so the copy/write/read support
// functions can be shared.
// ---------------------------------------------------------------------------

unsafe fn removeabbrev_index(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int) {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortIndexArg = (*base).arg as *mut TuplesortIndexArg;
    let mut i: c_int;

    i = 0;
    while i < count {
        let tuple: IndexTuple;

        tuple = (*stups.add(i as usize)).tuple as IndexTuple;
        (*stups.add(i as usize)).datum1 = index_getattr(
            tuple,
            1,
            RelationGetDescr((*arg).indexRel),
            &mut (*stups.add(i as usize)).isnull1,
        );
        i += 1;
    }
}

unsafe fn comparetup_index_btree(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    /*
     * This is similar to comparetup_heap(), but expects index tuples.  There
     * is also special handling for enforcing uniqueness, and special
     * treatment for equal keys at the end.
     */
    let base = TuplesortstateGetPublic(state);
    let sortKey: SortSupport = (*base).sortKeys;
    let compare: i32;

    // Compare the leading sort key
    compare = ApplySortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        sortKey,
    );
    if compare != 0 {
        return compare;
    }

    // Compare additional sort keys
    comparetup_index_btree_tiebreak(a, b, state)
}

unsafe fn comparetup_index_btree_tiebreak(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortIndexBTreeArg = (*base).arg as *mut TuplesortIndexBTreeArg;
    let mut sortKey: SortSupport = (*base).sortKeys;
    let tuple1: IndexTuple;
    let tuple2: IndexTuple;
    let keysz: c_int;
    let tupDes: TupleDesc;
    let mut equal_hasnull: bool = false;
    let mut nkey: c_int;
    let mut compare: i32;
    let mut datum1: Datum;
    let mut datum2: Datum;
    let mut isnull1: bool = false;
    let mut isnull2: bool = false;

    tuple1 = (*a).tuple as IndexTuple;
    tuple2 = (*b).tuple as IndexTuple;
    keysz = (*base).nKeys;
    tupDes = RelationGetDescr((*arg).index.indexRel);

    if (*sortKey).abbrev_converter.is_some() {
        datum1 = index_getattr(tuple1, 1, tupDes, &mut isnull1);
        datum2 = index_getattr(tuple2, 1, tupDes, &mut isnull2);

        compare = ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if compare != 0 {
            return compare;
        }
    }

    // they are equal, so we only need to examine one null flag
    if (*a).isnull1 {
        equal_hasnull = true;
    }

    sortKey = sortKey.add(1);
    nkey = 2;
    while nkey <= keysz {
        datum1 = index_getattr(tuple1, nkey, tupDes, &mut isnull1);
        datum2 = index_getattr(tuple2, nkey, tupDes, &mut isnull2);

        compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if compare != 0 {
            return compare; // done when we find unequal attributes
        }

        // they are equal, so we only need to examine one null flag
        if isnull1 {
            equal_hasnull = true;
        }

        nkey += 1;
        sortKey = sortKey.add(1);
    }

    /*
     * If btree has asked us to enforce uniqueness, complain if two equal
     * tuples are detected (unless there was at least one NULL field and NULLS
     * NOT DISTINCT was not set).
     *
     * It is sufficient to make the test here, because if two tuples are equal
     * they *must* get compared at some stage of the sort --- otherwise the
     * sort algorithm wouldn't have checked whether one must appear before the
     * other.
     */
    if (*arg).enforceUnique && !(!(*arg).uniqueNullsNotDistinct && equal_hasnull) {
        let mut values: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
        let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
        let key_desc: *mut c_char;

        /*
         * Some rather brain-dead implementations of qsort (such as the one in
         * QNX 4) will sometimes call the comparison routine to compare a
         * value to itself, but we always use our own implementation, which
         * does not.
         */
        Assert!(tuple1 != tuple2);

        index_deform_tuple(tuple1, tupDes, values.as_mut_ptr(), isnull.as_mut_ptr());

        key_desc =
            BuildIndexValueDescription((*arg).index.indexRel, values.as_ptr(), isnull.as_ptr());

        // C also: errcode(ERRCODE_UNIQUE_VIOLATION)
        // C also: key_desc ? errdetail("Key %s is duplicated.", key_desc)
        //                  : errdetail("Duplicate keys exist.")
        // C also: errtableconstraint(arg->index.heapRel,
        //                            RelationGetRelationName(arg->index.indexRel))
        let _ = (
            key_desc,
            errtableconstraint(
                (*arg).index.heapRel,
                RelationGetRelationName((*arg).index.indexRel),
            ),
        );
        ereport!(
            ERROR,
            errmsg!(
                "could not create unique index \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName((*arg).index.indexRel))
                    .to_string_lossy()
            )
        );
    }

    /*
     * If key values are equal, we sort on ItemPointer.  This is required for
     * btree indexes, since heap TID is treated as an implicit last key
     * attribute in order to ensure that all keys in the index are physically
     * unique.
     */
    {
        let blk1: BlockNumber = ItemPointerGetBlockNumber(&(*tuple1).t_tid);
        let blk2: BlockNumber = ItemPointerGetBlockNumber(&(*tuple2).t_tid);

        if blk1 != blk2 {
            return if blk1 < blk2 { -1 } else { 1 };
        }
    }
    {
        let pos1: OffsetNumber = ItemPointerGetOffsetNumber(&(*tuple1).t_tid);
        let pos2: OffsetNumber = ItemPointerGetOffsetNumber(&(*tuple2).t_tid);

        if pos1 != pos2 {
            return if pos1 < pos2 { -1 } else { 1 };
        }
    }

    // ItemPointer values should never be equal
    Assert!(false);

    0
}

unsafe fn comparetup_index_hash(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let bucket1: Bucket;
    let bucket2: Bucket;
    let hash1: u32;
    let hash2: u32;
    let tuple1: IndexTuple;
    let tuple2: IndexTuple;
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortIndexHashArg = (*base).arg as *mut TuplesortIndexHashArg;

    /*
     * Fetch hash keys and mask off bits we don't want to sort by, so that the
     * initial sort is just on the bucket number.  We know that the first
     * column of the index tuple is the hash key.
     */
    Assert!(!(*a).isnull1);
    bucket1 = _hash_hashkey2bucket(
        DatumGetUInt32((*a).datum1),
        (*arg).max_buckets,
        (*arg).high_mask,
        (*arg).low_mask,
    );
    Assert!(!(*b).isnull1);
    bucket2 = _hash_hashkey2bucket(
        DatumGetUInt32((*b).datum1),
        (*arg).max_buckets,
        (*arg).high_mask,
        (*arg).low_mask,
    );
    if bucket1 > bucket2 {
        return 1;
    } else if bucket1 < bucket2 {
        return -1;
    }

    /*
     * If bucket values are equal, sort by hash values.  This allows us to
     * insert directly onto bucket/overflow pages, where the index tuples are
     * stored in hash order to allow fast binary search within each page.
     */
    hash1 = DatumGetUInt32((*a).datum1);
    hash2 = DatumGetUInt32((*b).datum1);
    if hash1 > hash2 {
        return 1;
    } else if hash1 < hash2 {
        return -1;
    }

    /*
     * If hash values are equal, we sort on ItemPointer.  This does not affect
     * validity of the finished index, but it may be useful to have index
     * scans in physical order.
     */
    tuple1 = (*a).tuple as IndexTuple;
    tuple2 = (*b).tuple as IndexTuple;

    {
        let blk1: BlockNumber = ItemPointerGetBlockNumber(&(*tuple1).t_tid);
        let blk2: BlockNumber = ItemPointerGetBlockNumber(&(*tuple2).t_tid);

        if blk1 != blk2 {
            return if blk1 < blk2 { -1 } else { 1 };
        }
    }
    {
        let pos1: OffsetNumber = ItemPointerGetOffsetNumber(&(*tuple1).t_tid);
        let pos2: OffsetNumber = ItemPointerGetOffsetNumber(&(*tuple2).t_tid);

        if pos1 != pos2 {
            return if pos1 < pos2 { -1 } else { 1 };
        }
    }

    // ItemPointer values should never be equal
    Assert!(false);

    0
}

/*
 * Sorting for hash indexes only uses one sort key, so this shouldn't ever be
 * called. It's only here for consistency.
 */
#[allow(unused_variables)]
unsafe fn comparetup_index_hash_tiebreak(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    Assert!(false);

    0
}

unsafe fn writetup_index(state: *mut Tuplesortstate, tape: *mut LogicalTape, stup: *mut SortTuple) {
    let base = TuplesortstateGetPublic(state);
    let tuple: IndexTuple = (*stup).tuple as IndexTuple;
    let mut tuplen: c_uint;

    tuplen = IndexTupleSize(tuple) as c_uint + size_of::<c_uint>() as c_uint;
    LogicalTapeWrite(
        tape,
        &mut tuplen as *mut c_uint as *const c_void,
        size_of::<c_uint>(),
    );
    LogicalTapeWrite(tape, tuple as *const c_void, IndexTupleSize(tuple));
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeWrite(
            tape,
            &mut tuplen as *mut c_uint as *const c_void,
            size_of::<c_uint>(),
        );
    }
}

unsafe fn readtup_index(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    len: c_uint,
) {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortIndexArg = (*base).arg as *mut TuplesortIndexArg;
    let tuplen: c_uint = len - size_of::<c_uint>() as c_uint;
    let tuple: IndexTuple = tuplesort_readtup_alloc(state, tuplen as Size) as IndexTuple;
    let mut tuplen_trail: c_uint = 0;

    LogicalTapeReadExact!(tape, tuple as *mut c_void, tuplen as usize);
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeReadExact!(
            tape,
            &mut tuplen_trail as *mut c_uint as *mut c_void,
            size_of::<c_uint>()
        );
    }
    (*stup).tuple = tuple as *mut c_void;
    // set up first-column key value
    (*stup).datum1 = index_getattr(
        tuple,
        1,
        RelationGetDescr((*arg).indexRel),
        &mut (*stup).isnull1,
    );
}

// ---------------------------------------------------------------------------
// Routines specialized for BrinTuple case
// ---------------------------------------------------------------------------

unsafe fn removeabbrev_index_brin(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int) {
    let _ = state;
    let mut i: c_int;

    i = 0;
    while i < count {
        let tuple: *mut BrinSortTuple;

        tuple = (*stups.add(i as usize)).tuple as *mut BrinSortTuple;
        (*stups.add(i as usize)).datum1 = (*tuple).tuple.bt_blkno as Datum;
        i += 1;
    }
}

unsafe fn comparetup_index_brin(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    Assert!((*TuplesortstateGetPublic(state)).haveDatum1);

    if DatumGetUInt32((*a).datum1) > DatumGetUInt32((*b).datum1) {
        return 1;
    }

    if DatumGetUInt32((*a).datum1) < DatumGetUInt32((*b).datum1) {
        return -1;
    }

    // silence compilers
    0
}

unsafe fn writetup_index_brin(
    state: *mut Tuplesortstate,
    tape: *mut LogicalTape,
    stup: *mut SortTuple,
) {
    let base = TuplesortstateGetPublic(state);
    let tuple: *mut BrinSortTuple = (*stup).tuple as *mut BrinSortTuple;
    let mut tuplen: c_uint = (*tuple).tuplen as c_uint;

    tuplen = tuplen + size_of::<c_uint>() as c_uint;
    LogicalTapeWrite(
        tape,
        &mut tuplen as *mut c_uint as *const c_void,
        size_of::<c_uint>(),
    );
    LogicalTapeWrite(
        tape,
        &(*tuple).tuple as *const BrinTuple as *const c_void,
        (*tuple).tuplen,
    );
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeWrite(
            tape,
            &mut tuplen as *mut c_uint as *const c_void,
            size_of::<c_uint>(),
        );
    }
}

unsafe fn readtup_index_brin(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    len: c_uint,
) {
    let tuple: *mut BrinSortTuple;
    let base = TuplesortstateGetPublic(state);
    let tuplen: c_uint = len - size_of::<c_uint>() as c_uint;
    let mut tuplen_trail: c_uint = 0;

    /*
     * Allocate space for the BRIN sort tuple, which is BrinTuple with an
     * extra length field.
     */
    tuple = tuplesort_readtup_alloc(state, BRINSORTTUPLE_SIZE(tuplen as usize))
        as *mut BrinSortTuple;

    (*tuple).tuplen = tuplen as Size;

    LogicalTapeReadExact!(
        tape,
        &mut (*tuple).tuple as *mut BrinTuple as *mut c_void,
        tuplen as usize
    );
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeReadExact!(
            tape,
            &mut tuplen_trail as *mut c_uint as *mut c_void,
            size_of::<c_uint>()
        );
    }
    (*stup).tuple = tuple as *mut c_void;

    // set up first-column key value, which is block number
    (*stup).datum1 = (*tuple).tuple.bt_blkno as Datum;
}

// ---------------------------------------------------------------------------
// Routines specialized for GIN case
// ---------------------------------------------------------------------------

#[allow(unused_variables)]
unsafe fn removeabbrev_index_gin(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int) {
    Assert!(false);
    elog!(ERROR, "removeabbrev_index_gin not implemented");
}

unsafe fn comparetup_index_gin(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);

    Assert!(!(*TuplesortstateGetPublic(state)).haveDatum1);

    _gin_compare_tuples(
        (*a).tuple as *mut GinTuple,
        (*b).tuple as *mut GinTuple,
        (*base).sortKeys,
    )
}

unsafe fn writetup_index_gin(
    state: *mut Tuplesortstate,
    tape: *mut LogicalTape,
    stup: *mut SortTuple,
) {
    let base = TuplesortstateGetPublic(state);
    let tuple: *mut GinTuple = (*stup).tuple as *mut GinTuple;
    let mut tuplen: c_uint = (*tuple).tuplen as c_uint;

    tuplen = tuplen + size_of::<c_uint>() as c_uint;
    LogicalTapeWrite(
        tape,
        &mut tuplen as *mut c_uint as *const c_void,
        size_of::<c_uint>(),
    );
    LogicalTapeWrite(tape, tuple as *const c_void, (*tuple).tuplen as usize);
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeWrite(
            tape,
            &mut tuplen as *mut c_uint as *const c_void,
            size_of::<c_uint>(),
        );
    }
}

unsafe fn readtup_index_gin(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    len: c_uint,
) {
    let tuple: *mut GinTuple;
    let base = TuplesortstateGetPublic(state);
    let tuplen: c_uint = len - size_of::<c_uint>() as c_uint;
    let mut tuplen_trail: c_uint = 0;

    /*
     * Allocate space for the GIN sort tuple, which already has the proper
     * length included in the header.
     */
    tuple = tuplesort_readtup_alloc(state, tuplen as Size) as *mut GinTuple;

    (*tuple).tuplen = tuplen as c_int;

    LogicalTapeReadExact!(tape, tuple as *mut c_void, tuplen as usize);
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeReadExact!(
            tape,
            &mut tuplen_trail as *mut c_uint as *mut c_void,
            size_of::<c_uint>()
        );
    }
    (*stup).tuple = tuple as *mut c_void;

    // no abbreviations (FIXME maybe use attrnum for this?)
    (*stup).datum1 = 0 as Datum;
}

// ---------------------------------------------------------------------------
// Routines specialized for DatumTuple case
// ---------------------------------------------------------------------------

unsafe fn removeabbrev_datum(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int) {
    let _ = state;
    let mut i: c_int;

    i = 0;
    while i < count {
        (*stups.add(i as usize)).datum1 = PointerGetDatum((*stups.add(i as usize)).tuple);
        i += 1;
    }
}

unsafe fn comparetup_datum(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let compare: c_int;

    compare = ApplySortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        (*base).sortKeys,
    );
    if compare != 0 {
        return compare;
    }

    comparetup_datum_tiebreak(a, b, state)
}

unsafe fn comparetup_datum_tiebreak(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let mut compare: i32 = 0;

    // if we have abbreviations, then "tuple" has the original value
    if (*(*base).sortKeys).abbrev_converter.is_some() {
        compare = ApplySortAbbrevFullComparator(
            PointerGetDatum((*a).tuple),
            (*a).isnull1,
            PointerGetDatum((*b).tuple),
            (*b).isnull1,
            (*base).sortKeys,
        );
    }

    compare
}

unsafe fn writetup_datum(state: *mut Tuplesortstate, tape: *mut LogicalTape, stup: *mut SortTuple) {
    let base = TuplesortstateGetPublic(state);
    let arg: *mut TuplesortDatumArg = (*base).arg as *mut TuplesortDatumArg;
    let waddr: *const c_void;
    let tuplen: c_uint;
    let mut writtenlen: c_uint;

    if (*stup).isnull1 {
        waddr = null();
        tuplen = 0;
    } else if !(*base).tuples {
        waddr = &(*stup).datum1 as *const Datum as *const c_void;
        tuplen = size_of::<Datum>() as c_uint;
    } else {
        waddr = (*stup).tuple;
        tuplen = datumGetSize(
            PointerGetDatum((*stup).tuple),
            false,
            (*arg).datumTypeLen,
        ) as c_uint;
        Assert!(tuplen != 0);
    }

    writtenlen = tuplen + size_of::<c_uint>() as c_uint;

    LogicalTapeWrite(
        tape,
        &mut writtenlen as *mut c_uint as *const c_void,
        size_of::<c_uint>(),
    );
    LogicalTapeWrite(tape, waddr, tuplen as usize);
    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeWrite(
            tape,
            &mut writtenlen as *mut c_uint as *const c_void,
            size_of::<c_uint>(),
        );
    }
}

unsafe fn readtup_datum(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    len: c_uint,
) {
    let base = TuplesortstateGetPublic(state);
    let tuplen: c_uint = len - size_of::<c_uint>() as c_uint;
    let mut tuplen_trail: c_uint = 0;

    if tuplen == 0 {
        // it's NULL
        (*stup).datum1 = 0 as Datum;
        (*stup).isnull1 = true;
        (*stup).tuple = null_mut();
    } else if !(*base).tuples {
        Assert!(tuplen == size_of::<Datum>() as c_uint);
        LogicalTapeReadExact!(
            tape,
            &mut (*stup).datum1 as *mut Datum as *mut c_void,
            tuplen as usize
        );
        (*stup).isnull1 = false;
        (*stup).tuple = null_mut();
    } else {
        let raddr: *mut c_void = tuplesort_readtup_alloc(state, tuplen as Size);

        LogicalTapeReadExact!(tape, raddr, tuplen as usize);
        (*stup).datum1 = PointerGetDatum(raddr);
        (*stup).isnull1 = false;
        (*stup).tuple = raddr;
    }

    if (*base).sortopt & TUPLESORT_RANDOMACCESS != 0 {
        // need trailing length word?
        LogicalTapeReadExact!(
            tape,
            &mut tuplen_trail as *mut c_uint as *mut c_void,
            size_of::<c_uint>()
        );
    }
}
