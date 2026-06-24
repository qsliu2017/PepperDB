//! Translated from PostgreSQL src/include/access/heaptoast.h

use crate::access::htup_details::{MaxHeapTupleSize, SizeofHeapTupleHeader};
use crate::access::tupdesc::TupleDesc;
use crate::c::{MAXALIGN, MAXALIGN_DOWN};
use crate::pg_config::BLCKSZ;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::bufpage::SizeOfPageHeaderData;
use crate::storage::itemid::ItemIdData;
use crate::utils::relcache::Relation;
use crate::varatt::{varlena, VARHDRSZ};

/// Maximum tuple size if there are to be N tuples per page.
pub const fn MaximumBytesPerTuple(tuplesPerPage: usize) -> usize {
    MAXALIGN_DOWN(
        (BLCKSZ as usize
            - MAXALIGN(SizeOfPageHeaderData + tuplesPerPage * core::mem::size_of::<ItemIdData>()))
            / tuplesPerPage,
    )
}

pub const TOAST_TUPLES_PER_PAGE: usize = 4;
pub const TOAST_TUPLE_THRESHOLD: usize = MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE);
pub const TOAST_TUPLE_TARGET: usize = TOAST_TUPLE_THRESHOLD;

pub const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
pub const TOAST_TUPLE_TARGET_MAIN: usize = MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE_MAIN);

/// Per-datum (not per-tuple) index value compression threshold.
pub const TOAST_INDEX_TARGET: usize = MaxHeapTupleSize / 16;

pub const EXTERN_TUPLES_PER_PAGE: usize = 4; // tweak only this
pub const EXTERN_TUPLE_MAX_SIZE: usize = MaximumBytesPerTuple(EXTERN_TUPLES_PER_PAGE);

/// Max data bytes per external toast chunk. NB: changing requires an initdb.
pub const TOAST_MAX_CHUNK_SIZE: usize = EXTERN_TUPLE_MAX_SIZE
    - MAXALIGN(SizeofHeapTupleHeader)
    - core::mem::size_of::<Oid>()
    - core::mem::size_of::<i32>()
    - VARHDRSZ;

/// Called by heap_insert() and heap_update().
pub fn heap_toast_insert_or_update(
    _rel: Relation,
    _newtup: crate::access::htup::HeapTuple,
    _oldtup: crate::access::htup::HeapTuple,
    _options: i32,
) -> crate::access::htup::HeapTuple {
    unimplemented!()
}

/// Called by heap_delete().
pub fn heap_toast_delete(
    _rel: Relation,
    _oldtup: crate::access::htup::HeapTuple,
    _is_speculative: bool,
) {
    unimplemented!()
}

/// "Flatten" a tuple to contain no out-of-line toasted fields.
pub fn toast_flatten_tuple(
    _tup: crate::access::htup::HeapTuple,
    _tupleDesc: TupleDesc,
) -> crate::access::htup::HeapTuple {
    unimplemented!()
}

/// "Flatten" a tuple with out-of-line toasted fields into a Datum.
pub fn toast_flatten_tuple_to_datum(
    _tup: &crate::access::htup::HeapTupleHeaderData,
    _tup_len: u32,
    _tupleDesc: TupleDesc,
) -> Datum {
    unimplemented!()
}

/// Build a tuple containing no out-of-line toasted fields.
pub fn toast_build_flattened_tuple(
    _tupleDesc: TupleDesc,
    _values: &[Datum],
    _isnull: &[bool],
) -> crate::access::htup::HeapTuple {
    unimplemented!()
}

/// Fetch a slice from a toast value stored in a heap table (writes into result).
pub fn heap_fetch_toast_slice(
    _toastrel: Relation,
    _valueid: Oid,
    _attrsize: i32,
    _sliceoffset: i32,
    _slicelength: i32,
    _result: &mut varlena,
) {
    unimplemented!()
}
