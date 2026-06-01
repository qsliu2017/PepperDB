//! relptr.h - basic declarations for relative pointers.
//!
//! Relative pointers store an address relative either to the base of the
//! process's address space or some dynamic shared memory segment mapped
//! therein. In C this is a `union { type *relptr_type; Size relptr_off; }`;
//! what's stored is always a Size, never an actual pointer. The pointer arm of
//! the union exists in C only for macro type-safety tricks. In Rust we model
//! the relative pointer as the offset (a Size) directly and provide generic
//! helper functions parameterized by the pointee type.

use std::ptr;

use crate::c::Size;

/// The C `relptr(type)` / `relptr_declare(type, relptrtype)` macro produces a
/// union that always holds a `Size` offset (the pointer arm is purely for
/// type-safety). We represent that as a single-field struct carrying the
/// offset; the pointee type is supplied at the call site of the access/store
/// helpers instead of being baked into the type.
///
/// `#define relptr(type) union { type *relptr_type; Size relptr_off; }`
/// `#define relptr_declare(type, relptrtype) typedef relptr(type) relptrtype`
#[repr(C)]
#[derive(Clone, Copy)]
pub struct relptr {
    pub relptr_off: Size,
}

/// `#define relptr_access(base, rp) \
///     (... (rp).relptr_off == 0 ? NULL : (base) + (rp).relptr_off - 1))`
///
/// Returns a pointer of the pointee type `T`. `base` is a `char *`.
#[inline]
pub unsafe fn relptr_access<T>(base: *mut std::ffi::c_char, rp: &relptr) -> *mut T {
    if rp.relptr_off == 0 {
        ptr::null_mut()
    } else {
        base.add(rp.relptr_off - 1) as *mut T
    }
}

/// `#define relptr_is_null(rp) ((rp).relptr_off == 0)`
#[inline]
pub fn relptr_is_null(rp: &relptr) -> bool {
    rp.relptr_off == 0
}

/// `#define relptr_offset(rp) ((rp).relptr_off - 1)`
#[inline]
pub fn relptr_offset(rp: &relptr) -> Size {
    rp.relptr_off - 1
}

/// We use this inline to avoid double eval of "val" in relptr_store.
///
/// `static inline Size relptr_store_eval(char *base, char *val)`
#[inline]
pub unsafe fn relptr_store_eval(base: *mut std::ffi::c_char, val: *mut std::ffi::c_char) -> Size {
    if val.is_null() {
        0
    } else {
        crate::Assert!(val as usize >= base as usize);
        (val as usize) - (base as usize) + 1
    }
}

/// `#define relptr_store(base, rp, val) \
///     ((rp).relptr_off = relptr_store_eval((base), (char *) (val)))`
#[inline]
pub unsafe fn relptr_store<T>(
    base: *mut std::ffi::c_char,
    rp: &mut relptr,
    val: *mut T,
) {
    rp.relptr_off = relptr_store_eval(base, val as *mut std::ffi::c_char);
}

/// `#define relptr_copy(rp1, rp2) ((rp1).relptr_off = (rp2).relptr_off)`
#[inline]
pub fn relptr_copy(rp1: &mut relptr, rp2: &relptr) {
    rp1.relptr_off = rp2.relptr_off;
}
