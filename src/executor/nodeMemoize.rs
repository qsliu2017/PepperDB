//! nodeMemoize.c - Routines to handle caching of results from parameterized nodes
//!
//! postgres source: src/backend/executor/nodeMemoize.c
//! companion header: src/include/executor/nodeMemoize.h
//!
//! Memoize nodes are intended to sit above parameterized nodes in the plan
//! tree in order to cache results from them.  The intention here is that a
//! repeat scan with a parameter value that has already been seen by the node
//! can fetch tuples from the cache rather than having to re-scan the inner
//! node all over again.  The query planner may choose to make use of one of
//! these when it thinks rescans for previously seen values are likely enough
//! to warrant adding the additional node.
//!
//! The method of cache we use is a hash table.  When the cache fills, we never
//! spill tuples to disk, instead, we choose to evict the least recently used
//! cache entry from the cache.  We remember the least recently used entry by
//! always pushing new entries and entries we look for onto the tail of a
//! doubly linked list.  This means that older items always bubble to the top
//! of this LRU list.
//!
//! Sometimes our callers won't run their scans to completion. For example a
//! semi-join only needs to run until it finds a matching tuple, and once it
//! does, the join operator skips to the next outer tuple and does not execute
//! the inner side again on that scan.  Because of this, we must keep track of
//! when a cache entry is complete, and by default, we know it is when we run
//! out of tuples to read during the scan.  However, there are cases where we
//! can mark the cache entry as complete without exhausting the scan of all
//! tuples.  One case is unique joins, where the join operator knows that there
//! will only be at most one match for any given outer tuple.  In order to
//! support such cases we allow the "singlerow" option to be set for the cache.
//! This option marks the cache entry as complete after we read the first tuple
//! from the subnode.
//!
//! It's possible when we're filling the cache for a given set of parameters
//! that we're unable to free enough memory to store any more tuples.  If this
//! happens then we'll have already evicted all other cache entries.  When
//! caching another tuple would cause us to exceed our memory budget, we must
//! free the entry that we're currently populating and move the state machine
//! into MEMO_CACHE_BYPASS_MODE.  This means that we'll not attempt to cache
//! any further tuples for this particular scan.  We don't have the memory for
//! it.  The state machine will be reset again on the next rescan.  If the
//! memory requirements to cache the next parameter's tuples are less
//! demanding, then that may allow us to start putting useful entries back into
//! the cache again.
//!
//! INTERFACE ROUTINES
//!     ExecMemoize         - lookup cache, exec subplan when not found
//!     ExecInitMemoize     - initialize node and subnodes
//!     ExecEndMemoize      - shutdown node and subnodes
//!     ExecReScanMemoize   - rescan the memoize node
//!
//!     ExecMemoizeEstimate     estimates DSM space needed for parallel plan
//!     ExecMemoizeInitializeDSM initialize DSM for parallel plan
//!     ExecMemoizeInitializeWorker attach to DSM info in parallel worker
//!     ExecMemoizeRetrieveInstrumentation get instrumentation from worker

// #include "postgres.h"
use crate::prelude::*;

use std::ptr::null_mut;

use crate::c::{uint32, uint64, Size};

use crate::access::common::tupdesc::{CompactAttribute, TupleDesc, TupleDescCompactAttr};
use crate::access::htup_details::{MinimalTuple, MinimalTupleData};

use crate::common::hashfn::murmurhash32;
use crate::port::pg_bitutils::pg_rotate_left32;

use crate::executor::executor::{
    ExecBuildParamSetEqual, ExecEvalExpr, ExecInitExpr, ExecInitNode, ExecInitResultTupleSlotTL,
    ExecEndNode, ExecProcNode, ExecQual, ExecReScan, ExecTypeFromExprList, ResetExprContext,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::execUtils::{ExecAssignExprContext, ExecCreateScanSlotFromOuterPlan};
use crate::executor::execTuples::{
    ExecStoreMinimalTuple, ExecStoreVirtualTuple, MakeSingleTupleTableSlot, TTSOpsMinimalTuple,
    TTSOpsVirtual,
};
use crate::executor::tuptable::{
    slot_getallattrs, ExecClearTuple, ExecCopySlot, ExecCopySlotMinimalTuple, TupIsNull,
    TupleTableSlot,
};

use crate::nodes::bitmapset::{bms_nonempty_difference, Bitmapset};
use crate::nodes::execnodes::{
    outerPlanState, EState, ExprContext, ExprState, MemoizeInstrumentation, MemoizeState,
    PlanState, ScanState, SharedMemoizeInfo,
};
use crate::nodes::pg_list::{list_nth, List};
use crate::nodes::plannodes::{outerPlan, Memoize, Plan};
use crate::nodes::primnodes::Expr;

use crate::lib::ilist::{
    dlist_delete, dlist_head, dlist_init, dlist_move_tail, dlist_mutable_iter, dlist_node,
    dlist_push_tail,
};

use crate::miscadmin::{get_hash_memory_limit, CHECK_FOR_INTERRUPTS};
use crate::utils::adt::datum::{datum_image_eq, datum_image_hash};
// Real fmgr.h FmgrInfo (the execnodes one is an opaque placeholder); we use the
// real type for sizing, pointer arithmetic, and the fmgr call interface.
use crate::utils::fmgr::{fmgr_info, FmgrInfo, FunctionCall1Coll};

// CurrentMemoryContext, MemoryContextSwitchTo, MemoryContextReset,
// MemoryContextDelete, ALLOCSET_DEFAULT_SIZES, palloc, pfree, DatumGetUInt32,
// AllocSetContextCreate!, elog!, ERROR all come from the prelude.

use crate::{castNode, dlist_container, dlist_foreach_modify, makeNode, AllocSetContextCreate, Assert};

/* States of the ExecMemoize state machine */
const MEMO_CACHE_LOOKUP: c_int = 1; /* Attempt to perform a cache lookup */
const MEMO_CACHE_FETCH_NEXT_TUPLE: c_int = 2; /* Get another tuple from the cache */
const MEMO_FILLING_CACHE: c_int = 3; /* Read outer node to fill cache */
const MEMO_CACHE_BYPASS_MODE: c_int = 4; /* Bypass mode.  Just read from our
                                          * subplan without caching anything */
const MEMO_END_OF_SCAN: c_int = 5; /* Ready for rescan */

/* Helper macros for memory accounting */
/* EMPTY_ENTRY_MEMORY_BYTES(e) = sizeof(MemoizeEntry) + sizeof(MemoizeKey) +
 *                               (e)->key->params->t_len */
#[inline]
unsafe fn EMPTY_ENTRY_MEMORY_BYTES(e: *mut MemoizeEntry) -> uint64 {
    (size_of::<MemoizeEntry>()
        + size_of::<MemoizeKey>()
        + (*(*(*e).key).params).t_len as usize) as uint64
}
/* CACHE_TUPLE_BYTES(t) = sizeof(MemoizeTuple) + (t)->mintuple->t_len */
#[inline]
unsafe fn CACHE_TUPLE_BYTES(t: *mut MemoizeTuple) -> uint64 {
    (size_of::<MemoizeTuple>() + (*(*t).mintuple).t_len as usize) as uint64
}

/* MemoizeTuple Stores an individually cached tuple */
#[repr(C)]
pub struct MemoizeTuple {
    pub mintuple: MinimalTuple, /* Cached tuple */
    pub next: *mut MemoizeTuple, /* The next tuple with the same parameter
                                  * values or NULL if it's the last one */
}

/*
 * MemoizeKey
 * The hash table key for cached entries plus the LRU list link
 */
#[repr(C)]
pub struct MemoizeKey {
    pub params: MinimalTuple,
    pub lru_node: dlist_node, /* Pointer to next/prev key in LRU list */
}

/*
 * MemoizeEntry
 *		The data struct that the cache hash table stores
 */
#[repr(C)]
pub struct MemoizeEntry {
    pub key: *mut MemoizeKey, /* Hash key for hash table lookups */
    pub tuplehead: *mut MemoizeTuple, /* Pointer to the first tuple or NULL if no
                                       * tuples are cached for this entry */
    pub hash: uint32, /* Hash value (cached) */
    pub status: c_char, /* Hash status */
    pub complete: bool, /* Did we read the outer plan to completion? */
}

/*
 * #define SH_PREFIX memoize ... #include "lib/simplehash.h"
 *
 * The simplehash.h template expansion for the memoize hash table is not yet
 * ported.  Mirror the fields used by this file (members) and stub the
 * generated routines below.
 */
#[repr(C)]
pub struct memoize_hash {
    pub members: uint32,
    pub private_data: *mut c_void,
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct memoize_iterator {
    _opaque: [u8; 0],
}

/* SH_STATUS value indicating an in-use entry (simplehash.h) */
const memoize_SH_IN_USE: c_char = 1;

unsafe fn memoize_create(
    _ctx: MemoryContext,
    _nelements: uint32,
    _private_data: *mut c_void,
) -> *mut memoize_hash {
    unimplemented!() // TODO: lib/simplehash.h (nodeMemoize.c)
}
unsafe fn memoize_insert(
    _tb: *mut memoize_hash,
    _key: *mut MemoizeKey,
    _found: *mut bool,
) -> *mut MemoizeEntry {
    unimplemented!() // TODO: lib/simplehash.h (nodeMemoize.c)
}
unsafe fn memoize_lookup(_tb: *mut memoize_hash, _key: *mut MemoizeKey) -> *mut MemoizeEntry {
    unimplemented!() // TODO: lib/simplehash.h (nodeMemoize.c)
}
unsafe fn memoize_delete_item(_tb: *mut memoize_hash, _entry: *mut MemoizeEntry) {
    unimplemented!() // TODO: lib/simplehash.h (nodeMemoize.c)
}
unsafe fn memoize_start_iterate(_tb: *mut memoize_hash, _iter: *mut memoize_iterator) {
    unimplemented!() // TODO: lib/simplehash.h (nodeMemoize.c)
}
unsafe fn memoize_iterate(
    _tb: *mut memoize_hash,
    _iter: *mut memoize_iterator,
) -> *mut MemoizeEntry {
    unimplemented!() // TODO: lib/simplehash.h (nodeMemoize.c)
}

/* ----------------------------------------------------------------
 * Stubs for not-yet-ported dependencies.
 * ---------------------------------------------------------------- */

// access/parallel.h -- ParallelContext / ParallelWorkerContext are not yet
// ported.  Mirror the fields used here so the storage/shm_toc.h calls below
// typecheck faithfully.
#[repr(C)]
pub struct ParallelContext {
    pub nworkers: c_int,
    pub estimator: shm_toc_estimator,
    pub toc: *mut shm_toc,
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct ParallelWorkerContext {
    pub toc: *mut shm_toc,
    _opaque: [u8; 0],
}

// storage/shm_toc.h
#[repr(C)]
pub struct shm_toc {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct shm_toc_estimator {
    _opaque: [u8; 0],
}
unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {
    unimplemented!() // TODO: storage/shm_toc.h
}
unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: Size) {
    unimplemented!() // TODO: storage/shm_toc.h
}
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    unimplemented!() // TODO: storage/shm_toc.h
}
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: uint64, _address: *mut c_void) {
    unimplemented!() // TODO: storage/shm_toc.h
}
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: uint64, _noError: bool) -> *mut c_void {
    unimplemented!() // TODO: storage/shm_toc.h
}

// storage/shmem.h
unsafe fn mul_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO: storage/ipc/shmem.c
}
unsafe fn add_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

// miscadmin.h -- parallel worker globals not yet ported.
unsafe fn IsParallelWorker() -> bool {
    false // TODO: miscadmin.h (ParallelWorkerNumber != PARALLEL_WORKER_INVALID)
}
static mut ParallelWorkerNumber: c_int = 0;

// utils/lsyscache.h
unsafe fn get_op_hash_functions(
    _opno: Oid,
    _lhs_procno: *mut Oid,
    _rhs_procno: *mut Oid,
) -> bool {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_opcode(_opno: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

/*
 * MemoizeHash_hash
 *		Hash function for simplehash hashtable.  'key' is unused here as we
 *		require that all table lookups first populate the MemoizeState's
 *		probeslot with the key values to be looked up.
 */
unsafe fn MemoizeHash_hash(tb: *mut memoize_hash, _key: *const MemoizeKey) -> uint32 {
    let mstate = (*tb).private_data as *mut MemoizeState;
    let econtext = (*mstate).ss.ps.ps_ExprContext;
    let oldcontext: MemoryContext;
    let pslot = (*mstate).probeslot;
    let mut hashkey: uint32 = 0;
    let numkeys = (*mstate).nkeys;

    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    if (*mstate).binary_mode {
        for i in 0..numkeys {
            let i = i as isize;
            /* combine successive hashkeys by rotating */
            hashkey = pg_rotate_left32(hashkey, 1);

            if !*(*pslot).tts_isnull.offset(i) {
                /* treat nulls as having hash key 0 */
                let attr: *mut CompactAttribute;
                let hkey: uint32;

                attr = TupleDescCompactAttr((*pslot).tts_tupleDescriptor, i as c_int);

                hkey = datum_image_hash(
                    *(*pslot).tts_values.offset(i),
                    (*attr).attbyval,
                    (*attr).attlen as c_int,
                );

                hashkey ^= hkey;
            }
        }
    } else {
        let hashfunctions = (*mstate).hashfunctions as *mut FmgrInfo;
        let collations = (*mstate).collations;

        for i in 0..numkeys {
            let i = i as isize;
            /* combine successive hashkeys by rotating */
            hashkey = pg_rotate_left32(hashkey, 1);

            if !*(*pslot).tts_isnull.offset(i) {
                /* treat nulls as having hash key 0 */
                let hkey: uint32;

                hkey = DatumGetUInt32(FunctionCall1Coll(
                    hashfunctions.offset(i),
                    *collations.offset(i),
                    *(*pslot).tts_values.offset(i),
                ));
                hashkey ^= hkey;
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);
    murmurhash32(hashkey)
}

/*
 * MemoizeHash_equal
 *		Equality function for confirming hash value matches during a hash
 *		table lookup.  'key2' is never used.  Instead the MemoizeState's
 *		probeslot is always populated with details of what's being looked up.
 */
unsafe fn MemoizeHash_equal(
    tb: *mut memoize_hash,
    key1: *const MemoizeKey,
    _key2: *const MemoizeKey,
) -> bool {
    let mstate = (*tb).private_data as *mut MemoizeState;
    let econtext = (*mstate).ss.ps.ps_ExprContext;
    let tslot = (*mstate).tableslot;
    let pslot = (*mstate).probeslot;

    /* probeslot should have already been prepared by prepare_probe_slot() */
    ExecStoreMinimalTuple((*key1).params, tslot, false);

    if (*mstate).binary_mode {
        let oldcontext: MemoryContext;
        let numkeys = (*mstate).nkeys;
        let mut r#match = true;

        oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

        slot_getallattrs(tslot);
        slot_getallattrs(pslot);

        for i in 0..numkeys {
            let i = i as isize;
            let attr: *mut CompactAttribute;

            if *(*tslot).tts_isnull.offset(i) != *(*pslot).tts_isnull.offset(i) {
                r#match = false;
                break;
            }

            /* both NULL? they're equal */
            if *(*tslot).tts_isnull.offset(i) {
                continue;
            }

            /* perform binary comparison on the two datums */
            attr = TupleDescCompactAttr((*tslot).tts_tupleDescriptor, i as c_int);
            if !datum_image_eq(
                *(*tslot).tts_values.offset(i),
                *(*pslot).tts_values.offset(i),
                (*attr).attbyval,
                (*attr).attlen as c_int,
            ) {
                r#match = false;
                break;
            }
        }

        MemoryContextSwitchTo(oldcontext);
        r#match
    } else {
        (*econtext).ecxt_innertuple = tslot;
        (*econtext).ecxt_outertuple = pslot;
        ExecQual((*mstate).cache_eq_expr, econtext)
    }
}

/*
 * Initialize the hash table to empty.  The MemoizeState's hashtable field
 * must point to NULL.
 */
unsafe fn build_hash_table(mstate: *mut MemoizeState, mut size: uint32) {
    Assert!((*mstate).hashtable.is_null());

    /* Make a guess at a good size when we're not given a valid size. */
    if size == 0 {
        size = 1024;
    }

    /* memoize_create will convert the size to a power of 2 */
    (*mstate).hashtable =
        memoize_create((*mstate).tableContext, size, mstate as *mut c_void) as *mut _;
}

/*
 * prepare_probe_slot
 *		Populate mstate's probeslot with the values from the tuple stored
 *		in 'key'.  If 'key' is NULL, then perform the population by evaluating
 *		mstate's param_exprs.
 */
#[inline]
unsafe fn prepare_probe_slot(mstate: *mut MemoizeState, key: *mut MemoizeKey) {
    let pslot = (*mstate).probeslot;
    let tslot = (*mstate).tableslot;
    let numKeys = (*mstate).nkeys;

    ExecClearTuple(pslot);

    if key.is_null() {
        let econtext = (*mstate).ss.ps.ps_ExprContext;
        let oldcontext: MemoryContext;

        oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

        /* Set the probeslot's values based on the current parameter values */
        for i in 0..numKeys {
            let i = i as isize;
            *(*pslot).tts_values.offset(i) = ExecEvalExpr(
                *(*mstate).param_exprs.offset(i),
                econtext,
                (*pslot).tts_isnull.offset(i),
            );
        }

        MemoryContextSwitchTo(oldcontext);
    } else {
        /* Process the key's MinimalTuple and store the values in probeslot */
        ExecStoreMinimalTuple((*key).params, tslot, false);
        slot_getallattrs(tslot);
        std::ptr::copy_nonoverlapping(
            (*tslot).tts_values,
            (*pslot).tts_values,
            numKeys as usize,
        );
        std::ptr::copy_nonoverlapping(
            (*tslot).tts_isnull,
            (*pslot).tts_isnull,
            numKeys as usize,
        );
    }

    ExecStoreVirtualTuple(pslot);
}

/*
 * entry_purge_tuples
 *		Remove all tuples from the cache entry pointed to by 'entry'.  This
 *		leaves an empty cache entry.  Also, update the memory accounting to
 *		reflect the removal of the tuples.
 */
#[inline]
unsafe fn entry_purge_tuples(mstate: *mut MemoizeState, entry: *mut MemoizeEntry) {
    let mut tuple = (*entry).tuplehead;
    let mut freed_mem: uint64 = 0;

    while !tuple.is_null() {
        let next = (*tuple).next;

        freed_mem += CACHE_TUPLE_BYTES(tuple);

        /* Free memory used for this tuple */
        pfree((*tuple).mintuple as *mut c_void);
        pfree(tuple as *mut c_void);

        tuple = next;
    }

    (*entry).complete = false;
    (*entry).tuplehead = null_mut();

    /* Update the memory accounting */
    (*mstate).mem_used -= freed_mem;
}

/*
 * remove_cache_entry
 *		Remove 'entry' from the cache and free memory used by it.
 */
unsafe fn remove_cache_entry(mstate: *mut MemoizeState, entry: *mut MemoizeEntry) {
    let key = (*entry).key;

    dlist_delete(&raw mut (*(*entry).key).lru_node);

    /* Remove all of the tuples from this entry */
    entry_purge_tuples(mstate, entry);

    /*
     * Update memory accounting. entry_purge_tuples should have already
     * subtracted the memory used for each cached tuple.  Here we just update
     * the amount used by the entry itself.
     */
    (*mstate).mem_used -= EMPTY_ENTRY_MEMORY_BYTES(entry);

    /* Remove the entry from the cache */
    memoize_delete_item((*mstate).hashtable as *mut memoize_hash, entry);

    pfree((*key).params as *mut c_void);
    pfree(key as *mut c_void);
}

/*
 * cache_purge_all
 *		Remove all items from the cache
 */
unsafe fn cache_purge_all(mstate: *mut MemoizeState) {
    let mut evictions: uint64 = 0;

    if !(*mstate).hashtable.is_null() {
        evictions = (*((*mstate).hashtable as *mut memoize_hash)).members as uint64;
    }

    /*
     * Likely the most efficient way to remove all items is to just reset the
     * memory context for the cache and then rebuild a fresh hash table.  This
     * saves having to remove each item one by one and pfree each cached tuple
     */
    MemoryContextReset((*mstate).tableContext);

    /* NULLify so we recreate the table on the next call */
    (*mstate).hashtable = null_mut();

    /* reset the LRU list */
    dlist_init(&raw mut (*mstate).lru_list as *mut dlist_head);
    (*mstate).last_tuple = null_mut();
    (*mstate).entry = null_mut();

    (*mstate).mem_used = 0;

    /* XXX should we add something new to track these purges? */
    (*mstate).stats.cache_evictions += evictions; /* Update Stats */
}

/*
 * cache_reduce_memory
 *		Evict older and less recently used items from the cache in order to
 *		reduce the memory consumption back to something below the
 *		MemoizeState's mem_limit.
 *
 * 'specialkey', if not NULL, causes the function to return false if the entry
 * which the key belongs to is removed from the cache.
 */
unsafe fn cache_reduce_memory(mstate: *mut MemoizeState, specialkey: *mut MemoizeKey) -> bool {
    let mut specialkey_intact = true; /* for now */
    let mut iter: dlist_mutable_iter = std::mem::zeroed();
    let mut evictions: uint64 = 0;

    /* Update peak memory usage */
    if (*mstate).mem_used > (*mstate).stats.mem_peak {
        (*mstate).stats.mem_peak = (*mstate).mem_used;
    }

    /* We expect only to be called when we've gone over budget on memory */
    Assert!((*mstate).mem_used > (*mstate).mem_limit);

    /* Start the eviction process starting at the head of the LRU list. */
    dlist_foreach_modify!(iter, &raw mut (*mstate).lru_list as *mut dlist_head, {
        let key = dlist_container!(MemoizeKey, lru_node, iter.cur);
        let entry: *mut MemoizeEntry;

        /*
         * Populate the hash probe slot in preparation for looking up this LRU
         * entry.
         */
        prepare_probe_slot(mstate, key);

        /*
         * Ideally the LRU list pointers would be stored in the entry itself
         * rather than in the key.  Unfortunately, we can't do that as the
         * simplehash.h code may resize the table and allocate new memory for
         * entries which would result in those pointers pointing to the old
         * buckets.  However, it's fine to use the key to store this as that's
         * only referenced by a pointer in the entry, which of course follows
         * the entry whenever the hash table is resized.  Since we only have a
         * pointer to the key here, we must perform a hash table lookup to
         * find the entry that the key belongs to.
         */
        entry = memoize_lookup((*mstate).hashtable as *mut memoize_hash, null_mut());

        /*
         * Sanity check that we found the entry belonging to the LRU list
         * item.  A misbehaving hash or equality function could cause the
         * entry not to be found or the wrong entry to be found.
         */
        if entry.is_null() || (*entry).key != key {
            elog!(ERROR, "could not find memoization table entry");
        }

        /*
         * If we're being called to free memory while the cache is being
         * populated with new tuples, then we'd better take some care as we
         * could end up freeing the entry which 'specialkey' belongs to.
         * Generally callers will pass 'specialkey' as the key for the cache
         * entry which is currently being populated, so we must set
         * 'specialkey_intact' to false to inform the caller the specialkey
         * entry has been removed.
         */
        if key == specialkey {
            specialkey_intact = false;
        }

        /*
         * Finally remove the entry.  This will remove from the LRU list too.
         */
        remove_cache_entry(mstate, entry);

        evictions += 1;

        /* Exit if we've freed enough memory */
        if (*mstate).mem_used <= (*mstate).mem_limit {
            break;
        }
    });

    (*mstate).stats.cache_evictions += evictions; /* Update Stats */

    specialkey_intact
}

/*
 * cache_lookup
 *		Perform a lookup to see if we've already cached tuples based on the
 *		scan's current parameters.  If we find an existing entry we move it to
 *		the end of the LRU list, set *found to true then return it.  If we
 *		don't find an entry then we create a new one and add it to the end of
 *		the LRU list.  We also update cache memory accounting and remove older
 *		entries if we go over the memory budget.  If we managed to free enough
 *		memory we return the new entry, else we return NULL.
 *
 * Callers can assume we'll never return NULL when *found is true.
 */
unsafe fn cache_lookup(mstate: *mut MemoizeState, found: *mut bool) -> *mut MemoizeEntry {
    let key: *mut MemoizeKey;
    let mut entry: *mut MemoizeEntry;
    let oldcontext: MemoryContext;

    /* prepare the probe slot with the current scan parameters */
    prepare_probe_slot(mstate, null_mut());

    /*
     * Add the new entry to the cache.  No need to pass a valid key since the
     * hash function uses mstate's probeslot, which we populated above.
     */
    entry = memoize_insert((*mstate).hashtable as *mut memoize_hash, null_mut(), found);

    if *found {
        /*
         * Move existing entry to the tail of the LRU list to mark it as the
         * most recently used item.
         */
        dlist_move_tail(
            &raw mut (*mstate).lru_list as *mut dlist_head,
            &raw mut (*(*entry).key).lru_node,
        );

        return entry;
    }

    oldcontext = MemoryContextSwitchTo((*mstate).tableContext);

    /* Allocate a new key */
    key = palloc(size_of::<MemoizeKey>()) as *mut MemoizeKey;
    (*entry).key = key;
    (*key).params = ExecCopySlotMinimalTuple((*mstate).probeslot);

    /* Update the total cache memory utilization */
    (*mstate).mem_used += EMPTY_ENTRY_MEMORY_BYTES(entry);

    /* Initialize this entry */
    (*entry).complete = false;
    (*entry).tuplehead = null_mut();

    /*
     * Since this is the most recently used entry, push this entry onto the
     * end of the LRU list.
     */
    dlist_push_tail(
        &raw mut (*mstate).lru_list as *mut dlist_head,
        &raw mut (*(*entry).key).lru_node,
    );

    (*mstate).last_tuple = null_mut();

    MemoryContextSwitchTo(oldcontext);

    /*
     * If we've gone over our memory budget, then we'll free up some space in
     * the cache.
     */
    if (*mstate).mem_used > (*mstate).mem_limit {
        /*
         * Try to free up some memory.  It's highly unlikely that we'll fail
         * to do so here since the entry we've just added is yet to contain
         * any tuples and we're able to remove any other entry to reduce the
         * memory consumption.
         */
        if !cache_reduce_memory(mstate, key) {
            return null_mut();
        }

        /*
         * The process of removing entries from the cache may have caused the
         * code in simplehash.h to shuffle elements to earlier buckets in the
         * hash table.  If it has, we'll need to find the entry again by
         * performing a lookup.  Fortunately, we can detect if this has
         * happened by seeing if the entry is still in use and that the key
         * pointer matches our expected key.
         */
        if (*entry).status != memoize_SH_IN_USE || (*entry).key != key {
            /*
             * We need to repopulate the probeslot as lookups performed during
             * the cache evictions above will have stored some other key.
             */
            prepare_probe_slot(mstate, key);

            /* Re-find the newly added entry */
            entry = memoize_lookup((*mstate).hashtable as *mut memoize_hash, null_mut());
            Assert!(!entry.is_null());
        }
    }

    entry
}

/*
 * cache_store_tuple
 *		Add the tuple stored in 'slot' to the mstate's current cache entry.
 *		The cache entry must have already been made with cache_lookup().
 *		mstate's last_tuple field must point to the tail of mstate->entry's
 *		list of tuples.
 */
unsafe fn cache_store_tuple(mstate: *mut MemoizeState, slot: *mut TupleTableSlot) -> bool {
    let tuple: *mut MemoizeTuple;
    let mut entry = (*mstate).entry as *mut MemoizeEntry;
    let oldcontext: MemoryContext;

    Assert!(!slot.is_null());
    Assert!(!entry.is_null());

    oldcontext = MemoryContextSwitchTo((*mstate).tableContext);

    tuple = palloc(size_of::<MemoizeTuple>()) as *mut MemoizeTuple;
    (*tuple).mintuple = ExecCopySlotMinimalTuple(slot);
    (*tuple).next = null_mut();

    /* Account for the memory we just consumed */
    (*mstate).mem_used += CACHE_TUPLE_BYTES(tuple);

    if (*entry).tuplehead.is_null() {
        /*
         * This is the first tuple for this entry, so just point the list head
         * to it.
         */
        (*entry).tuplehead = tuple;
    } else {
        /* push this tuple onto the tail of the list */
        (*((*mstate).last_tuple as *mut MemoizeTuple)).next = tuple;
    }

    (*mstate).last_tuple = tuple as *mut _;
    MemoryContextSwitchTo(oldcontext);

    /*
     * If we've gone over our memory budget then free up some space in the
     * cache.
     */
    if (*mstate).mem_used > (*mstate).mem_limit {
        let key = (*entry).key;

        if !cache_reduce_memory(mstate, key) {
            return false;
        }

        /*
         * The process of removing entries from the cache may have caused the
         * code in simplehash.h to shuffle elements to earlier buckets in the
         * hash table.  If it has, we'll need to find the entry again by
         * performing a lookup.  Fortunately, we can detect if this has
         * happened by seeing if the entry is still in use and that the key
         * pointer matches our expected key.
         */
        if (*entry).status != memoize_SH_IN_USE || (*entry).key != key {
            /*
             * We need to repopulate the probeslot as lookups performed during
             * the cache evictions above will have stored some other key.
             */
            prepare_probe_slot(mstate, key);

            /* Re-find the entry */
            entry = memoize_lookup((*mstate).hashtable as *mut memoize_hash, null_mut());
            (*mstate).entry = entry as *mut _;
            Assert!(!entry.is_null());
        }
    }

    true
}

unsafe fn ExecMemoize(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node = castNode!(MemoizeState, T_MemoizeState, pstate);
    let econtext = (*node).ss.ps.ps_ExprContext;
    let outerNode: *mut PlanState;
    let slot: *mut TupleTableSlot;

    CHECK_FOR_INTERRUPTS();

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    match (*node).mstatus {
        MEMO_CACHE_LOOKUP => {
            let entry: *mut MemoizeEntry;
            let outerslot: *mut TupleTableSlot;
            let mut found: bool = false;

            Assert!((*node).entry.is_null());

            /* first call? we'll need a hash table. */
            if (*node).hashtable.is_null() {
                build_hash_table(node, (*((*pstate).plan as *mut Memoize)).est_entries);
            }

            /*
             * We're only ever in this state for the first call of the
             * scan.  Here we have a look to see if we've already seen the
             * current parameters before and if we have already cached a
             * complete set of records that the outer plan will return for
             * these parameters.
             *
             * When we find a valid cache entry, we'll return the first
             * tuple from it. If not found, we'll create a cache entry and
             * then try to fetch a tuple from the outer scan.  If we find
             * one there, we'll try to cache it.
             */

            /* see if we've got anything cached for the current parameters */
            entry = cache_lookup(node, &mut found);

            if found && (*entry).complete {
                (*node).stats.cache_hits += 1; /* stats update */

                /*
                 * Set last_tuple and entry so that the state
                 * MEMO_CACHE_FETCH_NEXT_TUPLE can easily find the next
                 * tuple for these parameters.
                 */
                (*node).last_tuple = (*entry).tuplehead as *mut _;
                (*node).entry = entry as *mut _;

                /* Fetch the first cached tuple, if there is one */
                if !(*entry).tuplehead.is_null() {
                    (*node).mstatus = MEMO_CACHE_FETCH_NEXT_TUPLE;

                    slot = (*node).ss.ps.ps_ResultTupleSlot;
                    ExecStoreMinimalTuple((*(*entry).tuplehead).mintuple, slot, false);

                    return slot;
                }

                /* The cache entry is void of any tuples. */
                (*node).mstatus = MEMO_END_OF_SCAN;
                return null_mut();
            }

            /* Handle cache miss */
            (*node).stats.cache_misses += 1; /* stats update */

            if found {
                /*
                 * A cache entry was found, but the scan for that entry
                 * did not run to completion.  We'll just remove all
                 * tuples and start again.  It might be tempting to
                 * continue where we left off, but there's no guarantee
                 * the outer node will produce the tuples in the same
                 * order as it did last time.
                 */
                entry_purge_tuples(node, entry);
            }

            /* Scan the outer node for a tuple to cache */
            outerNode = outerPlanState(node as *mut PlanState);
            outerslot = ExecProcNode(outerNode);
            if TupIsNull(outerslot) {
                /*
                 * cache_lookup may have returned NULL due to failure to
                 * free enough cache space, so ensure we don't do anything
                 * here that assumes it worked. There's no need to go into
                 * bypass mode here as we're setting mstatus to end of
                 * scan.
                 */
                if !entry.is_null() {
                    (*entry).complete = true;
                }

                (*node).mstatus = MEMO_END_OF_SCAN;
                return null_mut();
            }

            (*node).entry = entry as *mut _;

            /*
             * If we failed to create the entry or failed to store the
             * tuple in the entry, then go into bypass mode.
             */
            if entry.is_null() || !cache_store_tuple(node, outerslot) {
                (*node).stats.cache_overflows += 1; /* stats update */

                (*node).mstatus = MEMO_CACHE_BYPASS_MODE;

                /*
                 * No need to clear out last_tuple as we'll stay in bypass
                 * mode until the end of the scan.
                 */
            } else {
                /*
                 * If we only expect a single row from this scan then we
                 * can mark that we're not expecting more.  This allows
                 * cache lookups to work even when the scan has not been
                 * executed to completion.
                 */
                (*entry).complete = (*node).singlerow;
                (*node).mstatus = MEMO_FILLING_CACHE;
            }

            slot = (*node).ss.ps.ps_ResultTupleSlot;
            ExecCopySlot(slot, outerslot);
            slot
        }

        MEMO_CACHE_FETCH_NEXT_TUPLE => {
            /* We shouldn't be in this state if these are not set */
            Assert!(!(*node).entry.is_null());
            Assert!(!(*node).last_tuple.is_null());

            /* Skip to the next tuple to output */
            (*node).last_tuple = (*((*node).last_tuple as *mut MemoizeTuple)).next as *mut _;

            /* No more tuples in the cache */
            if (*node).last_tuple.is_null() {
                (*node).mstatus = MEMO_END_OF_SCAN;
                return null_mut();
            }

            slot = (*node).ss.ps.ps_ResultTupleSlot;
            ExecStoreMinimalTuple(
                (*((*node).last_tuple as *mut MemoizeTuple)).mintuple,
                slot,
                false,
            );

            slot
        }

        MEMO_FILLING_CACHE => {
            let outerslot: *mut TupleTableSlot;
            let entry = (*node).entry as *mut MemoizeEntry;

            /* entry should already have been set by MEMO_CACHE_LOOKUP */
            Assert!(!entry.is_null());

            /*
             * When in the MEMO_FILLING_CACHE state, we've just had a
             * cache miss and are populating the cache with the current
             * scan tuples.
             */
            outerNode = outerPlanState(node as *mut PlanState);
            outerslot = ExecProcNode(outerNode);
            if TupIsNull(outerslot) {
                /* No more tuples.  Mark it as complete */
                (*entry).complete = true;
                (*node).mstatus = MEMO_END_OF_SCAN;
                return null_mut();
            }

            /*
             * Validate if the planner properly set the singlerow flag. It
             * should only set that if each cache entry can, at most,
             * return 1 row.
             */
            if (*entry).complete {
                elog!(ERROR, "cache entry already complete");
            }

            /* Record the tuple in the current cache entry */
            if !cache_store_tuple(node, outerslot) {
                /* Couldn't store it?  Handle overflow */
                (*node).stats.cache_overflows += 1; /* stats update */

                (*node).mstatus = MEMO_CACHE_BYPASS_MODE;

                /*
                 * No need to clear out entry or last_tuple as we'll stay
                 * in bypass mode until the end of the scan.
                 */
            }

            slot = (*node).ss.ps.ps_ResultTupleSlot;
            ExecCopySlot(slot, outerslot);
            slot
        }

        MEMO_CACHE_BYPASS_MODE => {
            let outerslot: *mut TupleTableSlot;

            /*
             * When in bypass mode we just continue to read tuples without
             * caching.  We need to wait until the next rescan before we
             * can come out of this mode.
             */
            outerNode = outerPlanState(node as *mut PlanState);
            outerslot = ExecProcNode(outerNode);
            if TupIsNull(outerslot) {
                (*node).mstatus = MEMO_END_OF_SCAN;
                return null_mut();
            }

            slot = (*node).ss.ps.ps_ResultTupleSlot;
            ExecCopySlot(slot, outerslot);
            slot
        }

        MEMO_END_OF_SCAN =>
        /*
         * We've already returned NULL for this scan, but just in case
         * something calls us again by mistake.
         */
        {
            null_mut()
        }

        _ => {
            elog!(ERROR, "unrecognized memoize state: {}", (*node).mstatus as c_int);
            #[allow(unreachable_code)]
            null_mut()
        }
    } /* switch */
}

pub unsafe fn ExecInitMemoize(
    node: *mut Memoize,
    estate: *mut EState,
    eflags: c_int,
) -> *mut MemoizeState {
    let mstate = makeNode!(MemoizeState, T_MemoizeState);
    let outerNode: *mut Plan;
    let mut i: c_int;
    let nkeys: c_int;
    let eqfuncoids: *mut Oid;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    (*mstate).ss.ps.plan = node as *mut Plan;
    (*mstate).ss.ps.state = estate;
    (*mstate).ss.ps.ExecProcNode = Some(ExecMemoize);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &raw mut (*mstate).ss.ps);

    outerNode = outerPlan(node as *mut Plan);
    /* outerPlanState(mstate) = ExecInitNode(...) -- assign to lefttree */
    *outerPlanState_lvalue(mstate) = ExecInitNode(outerNode, estate, eflags);

    /*
     * Initialize return slot and type. No need to initialize projection info
     * because this node doesn't do projections.
     */
    ExecInitResultTupleSlotTL(&raw mut (*mstate).ss.ps, &TTSOpsMinimalTuple);
    (*mstate).ss.ps.ps_ProjInfo = null_mut();

    /*
     * Initialize scan slot and type.
     */
    ExecCreateScanSlotFromOuterPlan(estate, &raw mut (*mstate).ss as *mut _, &TTSOpsMinimalTuple);

    /*
     * Set the state machine to lookup the cache.  We won't find anything
     * until we cache something, but this saves a special case to create the
     * first entry.
     */
    (*mstate).mstatus = MEMO_CACHE_LOOKUP;

    nkeys = (*node).numKeys;
    (*mstate).nkeys = nkeys;
    (*mstate).hashkeydesc = ExecTypeFromExprList((*node).param_exprs);
    (*mstate).tableslot =
        MakeSingleTupleTableSlot((*mstate).hashkeydesc, &TTSOpsMinimalTuple);
    (*mstate).probeslot = MakeSingleTupleTableSlot((*mstate).hashkeydesc, &TTSOpsVirtual);

    (*mstate).param_exprs =
        palloc(nkeys as usize * size_of::<*mut ExprState>()) as *mut *mut ExprState;
    (*mstate).collations = (*node).collations; /* Just point directly to the plan
                                                * data */
    (*mstate).hashfunctions = palloc(nkeys as usize * size_of::<FmgrInfo>()) as *mut _;

    eqfuncoids = palloc(nkeys as usize * size_of::<Oid>()) as *mut Oid;

    i = 0;
    while i < nkeys {
        let hashop = *(*node).hashOperators.offset(i as isize);
        let mut left_hashfn: Oid = 0;
        let mut right_hashfn: Oid = 0;
        let param_expr = list_nth((*node).param_exprs, i) as *mut Expr;

        if !get_op_hash_functions(hashop, &mut left_hashfn, &mut right_hashfn) {
            elog!(
                ERROR,
                "could not find hash function for hash operator {}",
                hashop
            );
        }

        fmgr_info(
            left_hashfn,
            ((*mstate).hashfunctions as *mut FmgrInfo).offset(i as isize),
        );

        *(*mstate).param_exprs.offset(i as isize) =
            ExecInitExpr(param_expr, mstate as *mut PlanState);
        *eqfuncoids.offset(i as isize) = get_opcode(hashop);

        i += 1;
    }

    (*mstate).cache_eq_expr = ExecBuildParamSetEqual(
        (*mstate).hashkeydesc,
        &TTSOpsMinimalTuple,
        &TTSOpsVirtual,
        eqfuncoids,
        (*node).collations,
        (*node).param_exprs,
        mstate as *mut PlanState,
    );

    pfree(eqfuncoids as *mut c_void);
    (*mstate).mem_used = 0;

    /* Limit the total memory consumed by the cache to this */
    (*mstate).mem_limit = get_hash_memory_limit() as uint64;

    /* A memory context dedicated for the cache */
    (*mstate).tableContext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"MemoizeHashTable".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    ) as *mut _;

    dlist_init(&raw mut (*mstate).lru_list as *mut dlist_head);
    (*mstate).last_tuple = null_mut();
    (*mstate).entry = null_mut();

    /*
     * Mark if we can assume the cache entry is completed after we get the
     * first record for it.  Some callers might not call us again after
     * getting the first match. e.g. A join operator performing a unique join
     * is able to skip to the next outer tuple after getting the first
     * matching inner tuple.  In this case, the cache entry is complete after
     * getting the first tuple.  This allows us to mark it as so.
     */
    (*mstate).singlerow = (*node).singlerow;
    (*mstate).keyparamids = (*node).keyparamids;

    /*
     * Record if the cache keys should be compared bit by bit, or logically
     * using the type's hash equality operator
     */
    (*mstate).binary_mode = (*node).binary_mode;

    /* Zero the statistics counters */
    std::ptr::write_bytes(
        &raw mut (*mstate).stats as *mut MemoizeInstrumentation,
        0,
        1,
    );

    /*
     * Because it may require a large allocation, we delay building of the
     * hash table until executor run.
     */
    (*mstate).hashtable = null_mut();

    mstate
}

pub unsafe fn ExecEndMemoize(node: *mut MemoizeState) {
    // USE_ASSERT_CHECKING block validating the memory accounting code is
    // correct in assert builds; omitted in this port (debug-only).

    /*
     * When ending a parallel worker, copy the statistics gathered by the
     * worker back into shared memory so that it can be picked up by the main
     * process to report in EXPLAIN ANALYZE.
     */
    if !(*node).shared_info.is_null() && IsParallelWorker() {
        let si: *mut MemoizeInstrumentation;

        /* Make mem_peak available for EXPLAIN */
        if (*node).stats.mem_peak == 0 {
            (*node).stats.mem_peak = (*node).mem_used;
        }

        Assert!(ParallelWorkerNumber <= (*(*node).shared_info).num_workers);
        si = (*(*node).shared_info)
            .sinstrument
            .as_mut_ptr()
            .offset(ParallelWorkerNumber as isize);
        std::ptr::copy_nonoverlapping(&raw const (*node).stats, si, 1);
    }

    /* Remove the cache context */
    MemoryContextDelete((*node).tableContext);

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanMemoize(node: *mut MemoizeState) {
    let outerPlan = outerPlanState(node as *mut PlanState);

    /* Mark that we must lookup the cache for a new set of parameters */
    (*node).mstatus = MEMO_CACHE_LOOKUP;

    /* nullify pointers used for the last scan */
    (*node).entry = null_mut();
    (*node).last_tuple = null_mut();

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }

    /*
     * Purge the entire cache if a parameter changed that is not part of the
     * cache key.
     */
    if bms_nonempty_difference((*outerPlan).chgParam, (*node).keyparamids as *const Bitmapset) {
        cache_purge_all(node);
    }
}

/*
 * ExecEstimateCacheEntryOverheadBytes
 *		For use in the query planner to help it estimate the amount of memory
 *		required to store a single entry in the cache.
 */
pub unsafe fn ExecEstimateCacheEntryOverheadBytes(ntuples: f64) -> f64 {
    size_of::<MemoizeEntry>() as f64
        + size_of::<MemoizeKey>() as f64
        + size_of::<MemoizeTuple>() as f64 * ntuples
}

/* ----------------------------------------------------------------
 *						Parallel Query Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecMemoizeEstimate
 *
 *		Estimate space required to propagate memoize statistics.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecMemoizeEstimate(node: *mut MemoizeState, pcxt: *mut ParallelContext) {
    let mut size: Size;

    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    size = mul_size((*pcxt).nworkers as Size, size_of::<MemoizeInstrumentation>());
    size = add_size(size, core::mem::offset_of!(SharedMemoizeInfo, sinstrument));
    shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, size);
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecMemoizeInitializeDSM
 *
 *		Initialize DSM space for memoize statistics.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecMemoizeInitializeDSM(node: *mut MemoizeState, pcxt: *mut ParallelContext) {
    let size: Size;

    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    size = core::mem::offset_of!(SharedMemoizeInfo, sinstrument)
        + (*pcxt).nworkers as usize * size_of::<MemoizeInstrumentation>();
    (*node).shared_info = shm_toc_allocate((*pcxt).toc, size) as *mut SharedMemoizeInfo;
    /* ensure any unfilled slots will contain zeroes */
    std::ptr::write_bytes((*node).shared_info as *mut u8, 0, size);
    (*(*node).shared_info).num_workers = (*pcxt).nworkers;
    shm_toc_insert(
        (*pcxt).toc,
        (*(*node).ss.ps.plan).plan_node_id as uint64,
        (*node).shared_info as *mut c_void,
    );
}

/* ----------------------------------------------------------------
 *		ExecMemoizeInitializeWorker
 *
 *		Attach worker to DSM space for memoize statistics.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecMemoizeInitializeWorker(
    node: *mut MemoizeState,
    pwcxt: *mut ParallelWorkerContext,
) {
    (*node).shared_info = shm_toc_lookup(
        (*pwcxt).toc,
        (*(*node).ss.ps.plan).plan_node_id as uint64,
        true,
    ) as *mut SharedMemoizeInfo;
}

/* ----------------------------------------------------------------
 *		ExecMemoizeRetrieveInstrumentation
 *
 *		Transfer memoize statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecMemoizeRetrieveInstrumentation(node: *mut MemoizeState) {
    let size: Size;
    let si: *mut SharedMemoizeInfo;

    if (*node).shared_info.is_null() {
        return;
    }

    size = core::mem::offset_of!(SharedMemoizeInfo, sinstrument)
        + (*(*node).shared_info).num_workers as usize * size_of::<MemoizeInstrumentation>();
    si = palloc(size) as *mut SharedMemoizeInfo;
    std::ptr::copy_nonoverlapping(
        (*node).shared_info as *const u8,
        si as *mut u8,
        size,
    );
    (*node).shared_info = si;
}

/*
 * outerPlanState as an lvalue: `outerPlanState(mstate) = ExecInitNode(...)`.
 * outerPlanState() reads (*node).lefttree; we need a mutable place to assign
 * the subplan's PlanState.  Return a reference to the lefttree field.
 */
#[inline]
unsafe fn outerPlanState_lvalue(mstate: *mut MemoizeState) -> *mut *mut PlanState {
    &raw mut (*(mstate as *mut PlanState)).lefttree
}
