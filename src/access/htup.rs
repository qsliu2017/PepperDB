//! Translated from PostgreSQL src/include/access/htup.h
//! POSTGRES heap tuple definitions.

pub use crate::access::htup_details::{HeapTupleHeaderData, MinimalTupleData};
use crate::c::{CommandId, TransactionId};
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;

/// HeapTupleData is an in-memory wrapper that points to a tuple. `t_data` may
/// point into a disk buffer, into a palloc'd chunk, or be NULL (failure marker);
/// see the C header for the full set of representations. In-memory: no layout
/// contract, but kept as a plain struct mirroring the C fields.
pub struct HeapTupleData {
    pub t_len: u32,                // length of *t_data
    pub t_self: ItemPointerData,   // SelfItemPointer
    pub t_tableOid: Oid,           // table the tuple came from
    pub t_data: *mut HeapTupleHeaderData, // -> tuple header and data; TODO(ptr)
}

/// C: `typedef HeapTupleData *HeapTuple` -- a pointer-to-tuple handle.
pub type HeapTuple = *mut HeapTupleData; // TODO(ptr)

pub const FIELDNO_HEAPTUPLEDATA_DATA: usize = 3;

/// MAXALIGN(sizeof(HeapTupleData)). MAXALIGN is 8 on the target platforms.
pub const HEAPTUPLESIZE: usize = (core::mem::size_of::<HeapTupleData>() + 7) & !7;

/// True iff the HeapTuple pointer is valid (non-null).
pub fn HeapTupleIsValid(tuple: Option<&HeapTupleData>) -> bool {
    tuple.is_some()
}

// HeapTupleHeader functions implemented in utils/time/combocid.c.
pub use crate::backend::utils::time::combocid::{
    HeapTupleHeaderAdjustCmax, HeapTupleHeaderGetCmax, HeapTupleHeaderGetCmin,
};

// HeapTupleHeader accessor implemented in heapam.c.
pub fn HeapTupleGetUpdateXid(_tup: &HeapTupleHeaderData) -> TransactionId {
    unimplemented!()
}
