//! Translated from PostgreSQL src/include/access/gin_tuple.h
//! Sort tuple used while building a GIN index in parallel.

#![allow(clippy::cast_ptr_alignment, reason = "PG on-disk/varlena pointer reinterpretation, faithful to C")]

use crate::access::ginblock::GinPostingList;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::sortsupport::SortSupportData;

/// Data for one key in a GIN index (a sort tuple). On-disk-ish: written to and
/// read back from a tuplesort spill, so field order/types are fixed. `data` is a
/// flexible array member (the key value + a GinPostingList of TIDs).
#[repr(C)]
pub struct GinTuple {
    pub tuplen: i32,         // length of the whole tuple
    pub attrnum: OffsetNumber, // attnum of index key
    pub keylen: u16,         // bytes in data for key value
    pub typlen: i16,         // typlen for key
    pub typbyval: bool,      // typbyval for key
    pub category: i8,        // category: normal or NULL? (C signed char)
    pub nitems: i32,         // number of TIDs in the data
    // char data[FLEXIBLE_ARRAY_MEMBER]
}

impl GinTuple {
    /// SAFETY: `self` points into a GinTuple buffer of its recorded length; the
    /// posting list begins at SHORTALIGN(data + keylen).
    pub fn get_first(&self) -> &ItemPointerData {
        let data = std::ptr::from_ref::<Self>(self).cast::<u8>().wrapping_add(core::mem::size_of::<Self>());
        let off = (self.keylen as usize + 1) & !1; // SHORTALIGN
        let list = data.wrapping_add(off).cast::<GinPostingList>();
        unsafe { &(*list).first }
    }
}

pub fn _gin_compare_tuples(_a: &GinTuple, _b: &GinTuple, _ssup: &mut SortSupportData) -> i32 {
    unimplemented!()
}
