//! Translation of postgres/src/backend/access/rmgrdesc/rmgrdesc_utils.c
//!                + postgres/src/include/access/rmgrdesc_utils.h
//!
//! Support routines shared by the per-resource-manager WAL `*_desc` functions
//! (used by pg_waldump) to format arrays of offsets/oids into a StringInfo.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::{appendStringInfoChar, appendStringInfoString, StringInfo};
use crate::storage::off::OffsetNumber;

/// Signature of the per-element formatting callback passed to `array_desc`.
pub type ElemDescFn = unsafe fn(buf: StringInfo, elem: *mut c_void, data: *mut c_void);

/// `array_desc` - format an array of `count` elements (each `elem_size` bytes)
/// into `buf` as ` [e0, e1, ...]`, delegating each element to `elem_desc`.
///
/// # Safety
/// `array` must point to at least `count * elem_size` bytes; `elem_desc` must be
/// valid for the element type.
pub unsafe fn array_desc(
    buf: StringInfo,
    array: *mut c_void,
    elem_size: usize,
    count: c_int,
    elem_desc: ElemDescFn,
    data: *mut c_void,
) {
    if count == 0 {
        appendStringInfoString(buf, c" []".as_ptr());
        return;
    }

    appendStringInfoString(buf, c" [".as_ptr());
    for i in 0..count {
        elem_desc(
            buf,
            (array as *mut c_char).add(elem_size * i as usize) as *mut c_void,
            data,
        );
        if i < count - 1 {
            appendStringInfoString(buf, c", ".as_ptr());
        }
    }
    appendStringInfoChar(buf, b']' as c_char);
}

/// `offset_elem_desc` - format a single OffsetNumber element.
pub unsafe fn offset_elem_desc(buf: StringInfo, offset: *mut c_void, _data: *mut c_void) {
    appendStringInfo!(buf, "{}", *(offset as *mut OffsetNumber));
}

/// `redirect_elem_desc` - format a pair of OffsetNumbers as `old->new`.
pub unsafe fn redirect_elem_desc(buf: StringInfo, offset: *mut c_void, _data: *mut c_void) {
    let new_offset = offset as *mut OffsetNumber;
    appendStringInfo!(buf, "{}->{}", *new_offset, *new_offset.add(1));
}

/// `oid_elem_desc` - format a single Oid element.
pub unsafe fn oid_elem_desc(buf: StringInfo, relid: *mut c_void, _data: *mut c_void) {
    appendStringInfo!(buf, "{}", *(relid as *mut Oid));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::stringinfo::{initStringInfo, StringInfoData};

    #[test]
    fn array_desc_formats_offsets() {
        unsafe {
            let mut sid: StringInfoData = core::mem::zeroed();
            initStringInfo(&mut sid as StringInfo);
            let offs: [OffsetNumber; 3] = [1, 2, 3];
            array_desc(
                &mut sid as StringInfo,
                offs.as_ptr() as *mut c_void,
                core::mem::size_of::<OffsetNumber>(),
                3,
                offset_elem_desc,
                null_mut(),
            );
            let s = std::ffi::CStr::from_ptr(sid.data).to_str().unwrap();
            assert_eq!(s, " [1, 2, 3]");
        }
    }

    #[test]
    fn array_desc_empty() {
        unsafe {
            let mut sid: StringInfoData = core::mem::zeroed();
            initStringInfo(&mut sid as StringInfo);
            array_desc(
                &mut sid as StringInfo,
                null_mut(),
                core::mem::size_of::<OffsetNumber>(),
                0,
                offset_elem_desc,
                null_mut(),
            );
            let s = std::ffi::CStr::from_ptr(sid.data).to_str().unwrap();
            assert_eq!(s, " []");
        }
    }
}
