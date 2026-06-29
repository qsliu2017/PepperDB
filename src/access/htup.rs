//! Translated from PostgreSQL src/include/access/htup.h
//! POSTGRES heap tuple definitions.

pub use crate::access::htup_details::{HeapTupleHeaderData, MinimalTupleData};
use crate::c::{CommandId, TransactionId};
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;

/// Allocate a zeroed, 8-aligned tuple body able to hold `len` bytes. `Box<[u64]>`
/// is 8-aligned (so the `HeapTupleHeaderData` overlay base is MAXALIGN'd, matching
/// the C `palloc`/`alloc_zeroed` discipline) and `Send` (so a `HeapTupleData`
/// carrying one is auto-`Send`). `len` is clamped to 1 (a zero-length body is
/// degenerate).
#[must_use]
pub fn alloc_tuple_body(len: usize) -> Box<[u64]> {
    let nwords = len.max(1).div_ceil(8);
    vec![0u64; nwords].into_boxed_slice()
}

/// Allocate a tuple body of `len` bytes copied from `src`. Used by `heap_copytuple`
/// and the per-tuple page-item copy in the scan.
///
/// SAFETY: `src` must point at `len` readable bytes.
#[must_use]
pub unsafe fn tuple_body_from_raw(src: *const u8, len: usize) -> Box<[u64]> {
    let mut body = alloc_tuple_body(len);
    // SAFETY: `body` holds >= len bytes; `src` holds len (caller contract).
    unsafe { core::ptr::copy_nonoverlapping(src, body.as_mut_ptr().cast::<u8>(), len) };
    body
}

/// HeapTupleData is an in-memory wrapper for a tuple. The body (header + data) is
/// OWNED via `t_data` (an 8-aligned `Box<[u64]>`) or absent (`None`, the C
/// `t_data == NULL` failure/sentinel marker). Owning the body (vs the old raw
/// `*mut HeapTupleHeaderData`) makes the struct genuinely `Send` -- retiring the
/// last non-shmem `unsafe impl Send` (relation-ownership-plan step 9).
///
/// Access the overlay header through [`HeapTupleData::t_data`] /
/// [`HeapTupleData::t_data_mut`], which return a raw `*mut HeapTupleHeaderData`:
/// the on-disk overlay is read with `read_unaligned` / `fetch_att` / varatt
/// accessors and must NEVER be materialized as a `&HeapTupleHeaderData` (the
/// step-11 unaligned-access discipline), so a raw pointer -- not `&` -- is the
/// right accessor shape even though the owned body is aligned.
///
/// PG's scan returns a `HeapTuple` pointing INTO the pinned buffer page; here the
/// scan copies the current page item into an owned body (a per-tuple copy on the
/// hot path -- acceptable per the plan, and stronger validity: the returned
/// pointer stays valid for the descriptor's life, independent of the pin).
pub struct HeapTupleData {
    pub t_len: u32,                  // length of the body
    pub t_self: ItemPointerData,     // SelfItemPointer
    pub t_tableOid: Oid,             // table the tuple came from
    pub body: Option<Box<[u64]>>,    // owned 8-aligned body (None == C NULL t_data)
}

impl HeapTupleData {
    /// A `None`-bodied tuple (C `t_data == NULL`): the invalid/failure marker and
    /// the freshly-initialized scan `ctup`.
    #[must_use]
    pub fn null(t_self: ItemPointerData, t_tableOid: Oid) -> Self {
        Self { t_len: 0, t_self, t_tableOid, body: None }
    }

    /// The overlay header pointer (`*mut HeapTupleHeaderData`), or null when the
    /// body is absent. Replaces the old raw `t_data` field read. The body is
    /// 8-aligned, so the cast is sound (`HeapTupleHeaderData`'s align of 4
    /// divides 8).
    #[must_use]
    pub fn t_data(&self) -> *mut HeapTupleHeaderData {
        self.body.as_ref().map_or(core::ptr::null_mut(), |body| {
            body.as_ptr().cast::<HeapTupleHeaderData>().cast_mut()
        })
    }

    /// The writable overlay header pointer, derived from a MUTABLE borrow of the
    /// owned body so writes through it are valid under Stacked Borrows (deriving a
    /// write pointer from `&self`/`t_data()` would be UB). Null when the body is
    /// absent.
    #[must_use]
    pub fn t_data_mut(&mut self) -> *mut HeapTupleHeaderData {
        self.body.as_mut().map_or(core::ptr::null_mut(), |body| {
            body.as_mut_ptr().cast::<HeapTupleHeaderData>()
        })
    }

    /// True iff the body is absent (C `t_data == NULL`).
    #[must_use]
    pub fn t_data_is_null(&self) -> bool {
        self.body.is_none()
    }
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
