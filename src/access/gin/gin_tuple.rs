//! access/gin_tuple.h - data for one key in a GIN index, used during parallel build sort.

use std::ffi::c_int;

use crate::c::{int16, uint16, FLEXIBLE_ARRAY_MEMBER, SHORTALIGN};
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::off::OffsetNumber; // access/ginblock.h -> OffsetNumber
use crate::access::gin::ginpostinglist::GinPostingList; // access/ginblock.h
use crate::utils::sort::sortsupport::SortSupport; // utils/sortsupport.h

/// Data for one key in a GIN index.
///
/// ```c
/// typedef struct GinTuple
/// {
///     int          tuplen;     /* length of the whole tuple */
///     OffsetNumber attrnum;    /* attnum of index key */
///     uint16       keylen;     /* bytes in data for key value */
///     int16        typlen;     /* typlen for key */
///     bool         typbyval;   /* typbyval for key */
///     signed char  category;   /* category: normal or NULL? */
///     int          nitems;     /* number of TIDs in the data */
///     char         data[FLEXIBLE_ARRAY_MEMBER];
/// } GinTuple;
/// ```
#[repr(C)]
pub struct GinTuple {
    /// length of the whole tuple
    pub tuplen: c_int,
    /// attnum of index key
    pub attrnum: OffsetNumber,
    /// bytes in data for key value
    pub keylen: uint16,
    /// typlen for key
    pub typlen: int16,
    /// typbyval for key
    pub typbyval: bool,
    /// category: normal or NULL?  (C `signed char`)
    pub category: i8,
    /// number of TIDs in the data
    pub nitems: c_int,
    /// trailing flexible array (`char data[]`)
    pub data: [std::ffi::c_char; FLEXIBLE_ARRAY_MEMBER],
}

/// ```c
/// static inline ItemPointer
/// GinTupleGetFirst(GinTuple *tup)
/// {
///     GinPostingList *list;
///     list = (GinPostingList *) SHORTALIGN(tup->data + tup->keylen);
///     return &list->first;
/// }
/// ```
///
/// # Safety
/// `tup` must point to a valid `GinTuple` whose trailing data contains a
/// `GinPostingList` at `SHORTALIGN(data + keylen)`.
#[inline]
pub unsafe fn GinTupleGetFirst(tup: *mut GinTuple) -> ItemPointer {
    // tup->data + tup->keylen, then round up to short alignment.
    let base = (*tup).data.as_ptr() as usize + (*tup).keylen as usize;
    let list = SHORTALIGN(base) as *mut GinPostingList;

    &mut (*list).first as *mut ItemPointerData
}

/// ```c
/// extern int _gin_compare_tuples(GinTuple *a, GinTuple *b, SortSupport ssup);
/// ```
pub unsafe fn _gin_compare_tuples(a: *mut GinTuple, b: *mut GinTuple, ssup: SortSupport) -> c_int {
    unimplemented!()
}
