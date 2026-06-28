//! Translated from PostgreSQL src/include/access/itup.h
//! POSTGRES index tuple definitions.

use crate::c::MAXALIGN;
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::INDEX_MAX_KEYS;
use crate::postgres::Datum;
use crate::storage::bufpage::SizeOfPageHeaderData;
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::ItemPointerData;

/// t_info manipulation masks. t_info packs flags (high 3 bits) beside a 13-bit
/// size, so it is NOT a flag set (bitflags appendix C): keep the raw word.
pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
/// reserved for index-AM specific usage
pub const INDEX_AM_RESERVED_BIT: u16 = 0x2000;
pub const INDEX_VAR_MASK: u16 = 0x4000;
pub const INDEX_NULL_MASK: u16 = 0x8000;

/// Index tuple header structure (on-disk).
///
/// All index tuples start with IndexTupleData. If the HasNulls bit is set, this
/// is followed by an IndexAttributeBitMapData. Attribute values follow at a
/// MAXALIGN boundary. `t_info` is a packed word:
///   bit 15: has nulls | bit 14: has var-width | bit 13: AM-defined | 12-0: size
#[repr(C)]
pub struct IndexTupleData {
    pub tid: ItemPointerData, // reference TID to heap tuple
    pub t_info: u16,            // size:13 | am_reserved:1 | var:1 | null:1
}

const _: () = assert!(core::mem::size_of::<IndexTupleData>() == 8);
const _: () = assert!(core::mem::offset_of!(IndexTupleData, t_info) == 6);

impl IndexTupleData {
    #[inline]
    pub const fn size(&self) -> usize {
        (self.t_info & INDEX_SIZE_MASK) as usize
    }
    #[inline]
    pub const fn has_nulls(&self) -> bool {
        self.t_info & INDEX_NULL_MASK != 0
    }
    #[inline]
    pub const fn has_varwidths(&self) -> bool {
        self.t_info & INDEX_VAR_MASK != 0
    }
}

/// C names the pointer `IndexTuple`; represent it as a raw pointer for now.
pub type IndexTuple = *mut IndexTupleData; // TODO(ptr)

/// Null bitmap that follows the header when HasNulls is set (on-disk, fixed).
/// Size does not vary with attribute count; it is sized for INDEX_MAX_KEYS.
#[repr(C)]
pub struct IndexAttributeBitMapData {
    pub bits: [u8; INDEX_MAX_KEYS.div_ceil(8)],
}

pub type IndexAttributeBitMap = *mut IndexAttributeBitMapData; // TODO(ptr)

/// IndexTupleSize - size field of the tuple, in bytes.
#[inline]
pub const fn IndexTupleSize(itup: &IndexTupleData) -> usize {
    (itup.t_info & INDEX_SIZE_MASK) as usize
}

#[inline]
pub const fn IndexTupleHasNulls(itup: &IndexTupleData) -> bool {
    itup.t_info & INDEX_NULL_MASK != 0
}

#[inline]
pub const fn IndexTupleHasVarwidths(itup: &IndexTupleData) -> bool {
    itup.t_info & INDEX_VAR_MASK != 0
}

/// Offset to the index attribute data, given an infomask. Takes a t_info word
/// (so it is usable at index_form_tuple time to size the allocation).
#[inline]
pub const fn IndexInfoFindDataOffset(t_info: u16) -> usize {
    if t_info & INDEX_NULL_MASK == 0 {
        MAXALIGN(core::mem::size_of::<IndexTupleData>())
    } else {
        MAXALIGN(core::mem::size_of::<IndexTupleData>() + core::mem::size_of::<IndexAttributeBitMapData>())
    }
}

// === routines in indextuple.c ===
//
// Bodies live in `crate::backend::access::common::indextuple` and are re-exported
// here. The real signatures differ from the original placeholder stubs: the
// accessors are `unsafe` (they walk a raw index-tuple block), they take
// `&TupleDescData` (a borrow of the shared descriptor, the descriptor's offset
// cache is not persisted), and `index_form_tuple_context` drops the C
// `MemoryContext` parameter (memory is tombstoned -> Rust ownership). The C name
// `CopyIndexTuple` is kept as a deprecated shim over `copy_index_tuple`.
pub use crate::backend::access::common::indextuple::{
    copy_index_tuple, index_deform_tuple, index_deform_tuple_internal, index_form_tuple,
    index_form_tuple_context, index_getattr, index_truncate_tuple, nocache_index_getattr,
};

/// C `CopyIndexTuple`: now `copy_index_tuple`.
///
/// SAFETY: `source` must point at a valid index-tuple block.
#[deprecated(note = "use copy_index_tuple")]
#[inline]
pub unsafe fn CopyIndexTuple(source: IndexTuple) -> IndexTuple {
    copy_index_tuple(source)
}

/// Upper bound on the number of index tuples that can fit on one page.
pub const MaxIndexTuplesPerPage: usize = (BLCKSZ as usize - SizeOfPageHeaderData)
    / (MAXALIGN(core::mem::size_of::<IndexTupleData>() + 1) + core::mem::size_of::<ItemIdData>());
