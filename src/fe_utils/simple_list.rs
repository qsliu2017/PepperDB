//! fe_utils/simple_list.h - Simple list facilities for frontend code.
//!
//! Data structures for simple lists of OIDs, strings, and pointers.  The
//! support for these is very primitive compared to the backend's List
//! facilities, but it's all we need in, eg, pg_dump.

use std::ffi::{c_char, c_void};

use crate::postgres_ext::Oid;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SimpleOidListCell {
    pub next: *mut SimpleOidListCell,
    pub val: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SimpleOidList {
    pub head: *mut SimpleOidListCell,
    pub tail: *mut SimpleOidListCell,
}

#[repr(C)]
pub struct SimpleStringListCell {
    pub next: *mut SimpleStringListCell,
    /// true, when this string was searched and touched
    pub touched: bool,
    /// null-terminated string here (FLEXIBLE_ARRAY_MEMBER)
    pub val: [c_char; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SimpleStringList {
    pub head: *mut SimpleStringListCell,
    pub tail: *mut SimpleStringListCell,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SimplePtrListCell {
    pub next: *mut SimplePtrListCell,
    pub ptr: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SimplePtrList {
    pub head: *mut SimplePtrListCell,
    pub tail: *mut SimplePtrListCell,
}

pub unsafe fn simple_oid_list_append(list: *mut SimpleOidList, val: Oid) {
    unimplemented!()
}

pub unsafe fn simple_oid_list_member(list: *mut SimpleOidList, val: Oid) -> bool {
    unimplemented!()
}

pub unsafe fn simple_oid_list_destroy(list: *mut SimpleOidList) {
    unimplemented!()
}

pub unsafe fn simple_string_list_append(list: *mut SimpleStringList, val: *const c_char) {
    unimplemented!()
}

pub unsafe fn simple_string_list_member(list: *mut SimpleStringList, val: *const c_char) -> bool {
    unimplemented!()
}

pub unsafe fn simple_string_list_destroy(list: *mut SimpleStringList) {
    unimplemented!()
}

pub unsafe fn simple_string_list_not_touched(list: *mut SimpleStringList) -> *const c_char {
    unimplemented!()
}

pub unsafe fn simple_ptr_list_append(list: *mut SimplePtrList, ptr: *mut c_void) {
    unimplemented!()
}

pub unsafe fn simple_ptr_list_destroy(list: *mut SimplePtrList) {
    unimplemented!()
}
