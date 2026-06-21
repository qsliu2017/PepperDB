//! access/hash/hashsort.c - Sort tuples for insertion into a new hash index.
//
// When building a very large hash index, we pre-sort the tuples by bucket
// number to improve locality of access to the index, and thereby avoid
// thrashing.  We use tuplesort.c to sort the given index tuples into order.
//
// Note: if the number of rows in the table has been underestimated, bucket
// splits may occur during the index build.  In that case we'd be inserting
// into two or more buckets for each possible masked-off hash code value.
// That's no big problem though, since we'll still have plenty of locality of
// access.

use crate::prelude::*;

use crate::access::common::indextuple::IndexTuple;
use crate::commands::progress::PROGRESS_CREATEIDX_TUPLES_DONE;
use crate::miscadmin::{maintenance_work_mem, CHECK_FOR_INTERRUPTS};
use crate::nodes::execnodes::Tuplesortstate;
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::storage::itemptr::ItemPointer;
use crate::utils::activity::backend_progress::pgstat_progress_update_param;
use crate::utils::rel::Relation;

// ---------------------------------------------------------------------------
// Deep dependencies not yet ported - local stubs.
// ---------------------------------------------------------------------------

// utils/tuplesort.h: option bitmask. TUPLESORT_NONE == 0.
// TODO: dep not ported (utils/tuplesort.h)
const TUPLESORT_NONE: c_int = 0;

// utils/tuplesort.h - tuplesort_begin_index_hash
// TODO: dep not ported
unsafe fn tuplesort_begin_index_hash(
    _heapRel: Relation,
    _indexRel: Relation,
    _high_mask: uint32,
    _low_mask: uint32,
    _max_buckets: uint32,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _sortopt: c_int,
) -> *mut Tuplesortstate {
    unimplemented!()
}

// utils/tuplesort.h - tuplesort_end
// TODO: dep not ported
unsafe fn tuplesort_end(_state: *mut Tuplesortstate) { unimplemented!() }

// utils/tuplesort.h - tuplesort_putindextuplevalues
// TODO: dep not ported
unsafe fn tuplesort_putindextuplevalues(
    _state: *mut Tuplesortstate,
    _rel: Relation,
    _self: ItemPointer,
    _values: *const Datum,
    _isnull: *const bool,
) { unimplemented!() }

// utils/tuplesort.h - tuplesort_performsort
// TODO: dep not ported
unsafe fn tuplesort_performsort(_state: *mut Tuplesortstate) { unimplemented!() }

// utils/tuplesort.h - tuplesort_getindextuple
// TODO: dep not ported
unsafe fn tuplesort_getindextuple(_state: *mut Tuplesortstate, _forward: bool) -> IndexTuple { unimplemented!() }

// access/hash.h - _hash_doinsert
// TODO: dep not ported (access/hash/hashinsert.c)
unsafe fn _hash_doinsert(_rel: Relation, _itup: IndexTuple, _heapRel: Relation, _sorted: bool) { crate::access::hash::hashinsert::_hash_doinsert(_rel, _itup, _heapRel, _sorted) }

// The following are only referenced inside USE_ASSERT_CHECKING blocks, which we
// gate behind debug_assertions below.  Stubbed for completeness.
//
// access/hash.h - _hash_hashkey2bucket
// TODO: dep not ported
#[cfg(debug_assertions)]
unsafe fn _hash_hashkey2bucket(
    _hashkey: uint32,
    _maxbucket: uint32,
    _highmask: uint32,
    _lowmask: uint32,
) -> uint32 { crate::access::hash::hashutil::_hash_hashkey2bucket(_hashkey, _maxbucket, _highmask, _lowmask) as _ }

// access/hash.h - _hash_get_indextuple_hashkey
// TODO: dep not ported
#[cfg(debug_assertions)]
unsafe fn _hash_get_indextuple_hashkey(_itup: IndexTuple) -> uint32 { crate::access::hash::hashutil::_hash_get_indextuple_hashkey(_itup) }

// ---------------------------------------------------------------------------
// HSpool
// ---------------------------------------------------------------------------

/// Status record for spooling/sorting phase.
#[repr(C)]
pub struct HSpool {
    /// state data for tuplesort.c
    pub sortstate: *mut Tuplesortstate,
    pub index: Relation,

    // We sort the hash keys based on the buckets they belong to, then by the
    // hash values themselves, to optimize insertions onto hash pages.  The
    // masks below are used in _hash_hashkey2bucket to determine the bucket of
    // a given hash key.
    pub high_mask: uint32,
    pub low_mask: uint32,
    pub max_buckets: uint32,
}

/// create and initialize a spool structure
pub unsafe fn _h_spoolinit(heap: Relation, index: Relation, num_buckets: uint32) -> *mut HSpool {
    let hspool = palloc0(::std::mem::size_of::<HSpool>()) as *mut HSpool;

    (*hspool).index = index;

    // Determine the bitmask for hash code values.  Since there are currently
    // num_buckets buckets in the index, the appropriate mask can be computed
    // as follows.
    //
    // NOTE : This hash mask calculation should be in sync with similar
    // calculation in _hash_init_metabuffer.
    (*hspool).high_mask = pg_nextpower2_32(num_buckets + 1) - 1;
    (*hspool).low_mask = (*hspool).high_mask >> 1;
    (*hspool).max_buckets = num_buckets - 1;

    // We size the sort area as maintenance_work_mem rather than work_mem to
    // speed index creation.  This should be OK since a single backend can't run
    // multiple index creations in parallel.
    (*hspool).sortstate = tuplesort_begin_index_hash(
        heap,
        index,
        (*hspool).high_mask,
        (*hspool).low_mask,
        (*hspool).max_buckets,
        maintenance_work_mem,
        null_mut(),
        TUPLESORT_NONE,
    );

    hspool
}

/// clean up a spool structure and its substructures.
pub unsafe fn _h_spooldestroy(hspool: *mut HSpool) {
    tuplesort_end((*hspool).sortstate);
    pfree(hspool as *mut c_void);
}

/// spool an index entry into the sort file.
pub unsafe fn _h_spool(
    hspool: *mut HSpool,
    self_: ItemPointer,
    values: *const Datum,
    isnull: *const bool,
) {
    tuplesort_putindextuplevalues(
        (*hspool).sortstate,
        (*hspool).index,
        self_,
        values,
        isnull,
    );
}

/// given a spool loaded by successive calls to _h_spool, create an entire
/// index.
pub unsafe fn _h_indexbuild(hspool: *mut HSpool, heapRel: Relation) {
    let mut itup: IndexTuple;
    let mut tups_done: int64 = 0;
    #[cfg(debug_assertions)]
    let mut hashkey: uint32 = 0;

    tuplesort_performsort((*hspool).sortstate);

    loop {
        itup = tuplesort_getindextuple((*hspool).sortstate, true);
        if itup.is_null() {
            break;
        }

        // Technically, it isn't critical that hash keys be found in sorted
        // order, since this sorting is only used to increase locality of access
        // as a performance optimization.  It still seems like a good idea to
        // test tuplesort.c's handling of hash index tuple sorts through an
        // assertion, though.
        #[cfg(debug_assertions)]
        {
            let lasthashkey: uint32 = hashkey;

            hashkey = _hash_hashkey2bucket(
                _hash_get_indextuple_hashkey(itup),
                (*hspool).max_buckets,
                (*hspool).high_mask,
                (*hspool).low_mask,
            );
            Assert!(hashkey >= lasthashkey);
        }

        // the tuples are sorted by hashkey, so pass 'sorted' as true
        _hash_doinsert((*hspool).index, itup, heapRel, true);

        // allow insertion phase to be interrupted, and track progress
        CHECK_FOR_INTERRUPTS();

        tups_done += 1;
        pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tups_done);
    }
}
