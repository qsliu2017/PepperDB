//! Translated from PostgreSQL src/include/utils/expandeddatum.h
//!
//! Expanded (in-memory) representations of container datums. The flattened form
//! is always a varlena; references are a kind of TOAST pointer.

use crate::utils::palloc::MemoryContext;
use crate::varatt::VARHDRSZ_EXTERNAL;
use crate::postgres::Datum;

// Size of an EXTERNAL datum containing a pointer to an expanded object.
pub const EXPANDED_POINTER_SIZE: usize =
    VARHDRSZ_EXTERNAL + core::mem::size_of::<crate::varatt::varatt_expanded>();

/// First int32 of an ExpandedObjectHeader: always -1 (no 4-byte varlena can have
/// this as its first 4 bytes), distinguishing expanded from flat inputs.
pub const EOH_HEADER_MAGIC: i32 = -1;

/// "Methods" every expanded object must provide. All callbacks are required
/// (routine-struct.md, group C), so this is a plain trait with no supertraits.
pub trait ExpandedObjectMethods {
    /// Space needed for the flattened representation (total, including header).
    fn get_flat_size(eohptr: &mut ExpandedObjectHeader) -> usize;
    /// Construct the flattened representation in caller-allocated `result`.
    /// `allocated_size` is the prior get_flat_size result (passed for cross-check).
    fn flatten_into(eohptr: &mut ExpandedObjectHeader, result: &mut [u8], allocated_size: usize);
}

/// Every expanded object embeds this header (typically inside a larger,
/// type-specific struct).
///
/// In-memory only (not on disk), but the two embedded TOAST pointers are
/// byte-laid-out, so the fields keep their C order and the pointer buffers their
/// exact size.
// Canonical definition; resolves the level-1 forward decl
// crate::varatt::ExpandedObjectHeader (a `_private: [u8;0]` placeholder marked
// TODO(struct-forward)). Phase 2 repoints crate::varatt::varatt_expanded.eohptr
// here. (postgres.h also typedefs the pointer; same target.)
pub struct ExpandedObjectHeader {
    /// Phony varlena header: always EOH_HEADER_MAGIC.
    pub vl_len_: i32,
    /// Methods required for this object type. C: `const ExpandedObjectMethods *`;
    /// modeled as a closed dispatch enum later, kept as a raw ptr for now.
    pub eoh_methods: *const (), // TODO(ptr): -> &dyn/enum of ExpandedObjectMethods
    /// Memory context owning this header and subsidiary data.
    pub eoh_context: MemoryContext,
    /// Standard read-write TOAST pointer for this object.
    pub eoh_rw_ptr: [u8; EXPANDED_POINTER_SIZE],
    /// Standard read-only TOAST pointer for this object.
    pub eoh_ro_ptr: [u8; EXPANDED_POINTER_SIZE],
}

/// Does a 4-byte-header varlena actually carry an ExpandedObjectHeader?
/// True iff its va_header == EOH_HEADER_MAGIC (as uint32).
/// SAFETY: caller guarantees ptr points at a valid 4-byte-header varlena.
#[inline]
pub unsafe fn VARATT_IS_EXPANDED_HEADER(ptr: *const u8) -> bool {
    unimplemented!() // ((varattrib_4b*)PTR)->va_4byte.va_header == (uint32)EOH_HEADER_MAGIC
}

#[inline]
pub fn EOHPGetRWDatum(eohptr: &ExpandedObjectHeader) -> Datum {
    Datum(eohptr.eoh_rw_ptr.as_ptr() as usize)
}

#[inline]
pub fn EOHPGetRODatum(eohptr: &ExpandedObjectHeader) -> Datum {
    Datum(eohptr.eoh_ro_ptr.as_ptr() as usize)
}

/// Does the Datum represent a writable expanded object?
#[inline]
pub fn DatumIsReadWriteExpandedObject(d: Datum, isnull: bool, typlen: i16) -> bool {
    if isnull || typlen != -1 {
        false
    } else {
        unimplemented!() // VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d))
    }
}

#[inline]
pub fn MakeExpandedObjectReadOnly(d: Datum, isnull: bool, typlen: i16) -> Datum {
    if isnull || typlen != -1 {
        d
    } else {
        MakeExpandedObjectReadOnlyInternal(d)
    }
}

pub fn DatumGetEOHP(d: Datum) -> *mut ExpandedObjectHeader {
    unimplemented!() // TODO(ptr)
}
pub fn EOH_init_header(
    eohptr: &mut ExpandedObjectHeader,
    methods: *const (),
    obj_context: MemoryContext,
) {
    unimplemented!()
}
pub fn EOH_get_flat_size(eohptr: &mut ExpandedObjectHeader) -> usize {
    unimplemented!()
}
pub fn EOH_flatten_into(eohptr: &mut ExpandedObjectHeader, result: &mut [u8], allocated_size: usize) {
    unimplemented!()
}
pub fn MakeExpandedObjectReadOnlyInternal(d: Datum) -> Datum {
    unimplemented!()
}
pub fn TransferExpandedObject(d: Datum, new_parent: MemoryContext) -> Datum {
    unimplemented!()
}
pub fn DeleteExpandedObject(d: Datum) {
    unimplemented!()
}
