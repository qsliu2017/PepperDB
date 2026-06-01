//! Translation of postgres/src/backend/utils/adt/expandeddatum.c
//! (merged with its header postgres/src/include/utils/expandeddatum.h).
//!
//! Support functions for "expanded" value representations (TOAST objects that
//! are expanded in memory into a type-specific, computation-friendly form).
//!
//! #include mapping:
//!   - "postgres.h"            -> crate::prelude::* (Datum, DatumGetPointer,
//!                                PointerGetDatum, Size, the c-types).
//!   - "utils/expandeddatum.h" -> THIS file (struct/typedef declarations merged in).
//!   - "utils/memutils.h"      -> MemoryContextSetParent / MemoryContextDelete from
//!                                crate::utils::mmgr::mcxt (the real dispatch layer);
//!                                MemoryContext from crate::utils::mmgr::memnodes.
//!   - "varatt.h" (incl. by expandeddatum.h) -> crate::varatt for the external/
//!                                expanded TOAST-pointer support (varattrib_1b_e,
//!                                VARHDRSZ_EXTERNAL, the VARTAG_* tags, and the
//!                                VARATT_IS_EXTERNAL_EXPANDED* checks).
//!
//! NOTE (port staging): the canonical `ExpandedObjectHeader` type and the
//! `varatt_expanded` TOAST pointer struct are *defined here* (in C they live in
//! expandeddatum.h / varatt.h respectively, but `varatt_expanded` references
//! `ExpandedObjectHeader`, whose full struct only exists in expandeddatum.h, so
//! keeping both together is natural). access/common/detoast.rs currently has an
//! empty placeholder `pub enum ExpandedObjectHeader {}`; that should eventually be
//! repointed to THIS definition. See the symbol notes at end of file.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/expandeddatum.c

use crate::prelude::*;
use crate::c::{int32, uint32, Size};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
// The real MemoryContext dispatch layer (mcxt.rs) + the context node type.
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::mmgr::mcxt::{MemoryContextDelete, MemoryContextSetParent};
// varatt.h support for the external-expanded TOAST pointer.
use crate::varatt::{
    varattrib_1b_e, VARATT_IS_EXTERNAL_EXPANDED, VARATT_IS_EXTERNAL_EXPANDED_RW, VARDATA_1B_E,
    VARHDRSZ_EXTERNAL, VARTAG_EXPANDED_RO, VARTAG_EXPANDED_RW,
};
use core::ffi::{c_char, c_void};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   expandeddatum.h declarations (merged in)
// ----------------------------------------------------------------------------

/// EOH_HEADER_MAGIC: always stored in the first int32 of an ExpandedObjectHeader
/// (i.e. its phony varlena header). -1 is impossible as a real 4-byte varlena
/// header, so it distinguishes an expanded header from a flat 4B varlena.
pub const EOH_HEADER_MAGIC: int32 = -1;

/// `#define VARATT_IS_EXPANDED_HEADER(PTR)`
///   `(((varattrib_4b *) (PTR))->va_4byte.va_header == (uint32) EOH_HEADER_MAGIC)`
///
/// Reads the (possibly unaligned) leading uint32 and compares it to the magic.
///
/// # Safety
/// `ptr` must point to at least 4 readable bytes.
#[inline]
pub unsafe fn VARATT_IS_EXPANDED_HEADER(ptr: *const c_void) -> bool {
    core::ptr::read_unaligned(ptr as *const uint32) == (EOH_HEADER_MAGIC as uint32)
}

/// `struct varatt_expanded` (varatt.h): a "TOAST pointer" representing an
/// out-of-line Datum stored in memory in expanded form. Stored UNALIGNED inside
/// a containing tuple, hence always accessed via memcpy.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct varatt_expanded {
    pub eohptr: *mut ExpandedObjectHeader,
}

/// Size of an EXTERNAL datum that contains a pointer to an expanded object.
/// `#define EXPANDED_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(varatt_expanded))`
pub const EXPANDED_POINTER_SIZE: usize =
    VARHDRSZ_EXTERNAL as usize + core::mem::size_of::<varatt_expanded>();

/// `typedef Size (*EOM_get_flat_size_method) (ExpandedObjectHeader *eohptr);`
pub type EOM_get_flat_size_method = unsafe extern "C" fn(eohptr: *mut ExpandedObjectHeader) -> Size;

/// `typedef void (*EOM_flatten_into_method)
///      (ExpandedObjectHeader *eohptr, void *result, Size allocated_size);`
pub type EOM_flatten_into_method =
    unsafe extern "C" fn(eohptr: *mut ExpandedObjectHeader, result: *mut c_void, allocated_size: Size);

/// `struct ExpandedObjectMethods`: the vtable of function pointers that every
/// expanded object type must provide.
#[repr(C)]
pub struct ExpandedObjectMethods {
    pub get_flat_size: EOM_get_flat_size_method,
    pub flatten_into: EOM_flatten_into_method,
}

/// `struct ExpandedObjectHeader` (expandeddatum.h). Every expanded object embeds
/// this header (typically as the first member of a larger, type-specific struct).
/// Field order MUST match the C struct exactly.
#[repr(C)]
pub struct ExpandedObjectHeader {
    /// Phony varlena header; always EOH_HEADER_MAGIC.
    pub vl_len_: int32,

    /// Pointer to the methods table for this object type.
    pub eoh_methods: *const ExpandedObjectMethods,

    /// Memory context containing this header and all subsidiary data.
    pub eoh_context: MemoryContext,

    /// Standard R/W TOAST pointer for this object is kept here.
    pub eoh_rw_ptr: [c_char; EXPANDED_POINTER_SIZE],

    /// Standard R/O TOAST pointer for this object is kept here.
    pub eoh_ro_ptr: [c_char; EXPANDED_POINTER_SIZE],
}

// ---- the inline R/W and R/O datum accessors ----

/// `EOHPGetRWDatum` - the object's standard read-write TOAST pointer as a Datum.
#[inline]
pub unsafe fn EOHPGetRWDatum(eohptr: *const ExpandedObjectHeader) -> Datum {
    PointerGetDatum((*eohptr).eoh_rw_ptr.as_ptr() as *const c_void)
}

/// `EOHPGetRODatum` - the object's standard read-only TOAST pointer as a Datum.
#[inline]
pub unsafe fn EOHPGetRODatum(eohptr: *const ExpandedObjectHeader) -> Datum {
    PointerGetDatum((*eohptr).eoh_ro_ptr.as_ptr() as *const c_void)
}

// ----------------------------------------------------------------------------
//   varatt.h helpers not (yet) present in crate::varatt.
//   These mirror the macros 1:1 and keep this file self-contained.
// ----------------------------------------------------------------------------

/// `#define VARDATA_EXTERNAL(PTR) VARDATA_1B_E(PTR)`
#[inline]
unsafe fn VARDATA_EXTERNAL(ptr: *const c_char) -> *mut c_char {
    VARDATA_1B_E(ptr)
}

/// `#define SET_VARTAG_EXTERNAL(PTR, tag) SET_VARTAG_1B_E(PTR, tag)`
/// which expands (little-endian) to:
///   `((varattrib_1b_e *) PTR)->va_header = 0x01;`
///   `((varattrib_1b_e *) PTR)->va_tag    = tag;`
#[inline]
unsafe fn SET_VARTAG_EXTERNAL(ptr: *mut c_char, tag: u8) {
    let p = ptr as *mut varattrib_1b_e;
    (*p).va_header = 0x01;
    (*p).va_tag = tag;
}

// ----------------------------------------------------------------------------
//   expandeddatum.c body
// ----------------------------------------------------------------------------

/// DatumGetEOHP
///
/// Given a Datum that is an expanded-object reference, extract the pointer.
///
/// This is a bit tedious since the pointer may not be properly aligned; compare
/// VARATT_EXTERNAL_GET_POINTER().
pub unsafe fn DatumGetEOHP(d: Datum) -> *mut ExpandedObjectHeader {
    let datum = DatumGetPointer(d) as *mut varattrib_1b_e;
    let mut ptr = varatt_expanded { eohptr: null_mut() };

    Assert!(VARATT_IS_EXTERNAL_EXPANDED(datum as *const c_char));
    memcpy(
        &mut ptr as *mut varatt_expanded as *mut c_void,
        VARDATA_EXTERNAL(datum as *const c_char) as *const c_void,
        core::mem::size_of::<varatt_expanded>(),
    );
    Assert!(VARATT_IS_EXPANDED_HEADER(ptr.eohptr as *const c_void));
    ptr.eohptr
}

/// EOH_init_header
///
/// Initialize the common header of an expanded object.
///
/// The main thing this encapsulates is initializing the TOAST pointers.
pub unsafe fn EOH_init_header(
    eohptr: *mut ExpandedObjectHeader,
    methods: *const ExpandedObjectMethods,
    obj_context: MemoryContext,
) {
    let mut ptr = varatt_expanded { eohptr: null_mut() };

    (*eohptr).vl_len_ = EOH_HEADER_MAGIC;
    (*eohptr).eoh_methods = methods;
    (*eohptr).eoh_context = obj_context;

    ptr.eohptr = eohptr;

    SET_VARTAG_EXTERNAL((*eohptr).eoh_rw_ptr.as_mut_ptr(), VARTAG_EXPANDED_RW);
    memcpy(
        VARDATA_EXTERNAL((*eohptr).eoh_rw_ptr.as_ptr()) as *mut c_void,
        &ptr as *const varatt_expanded as *const c_void,
        core::mem::size_of::<varatt_expanded>(),
    );

    SET_VARTAG_EXTERNAL((*eohptr).eoh_ro_ptr.as_mut_ptr(), VARTAG_EXPANDED_RO);
    memcpy(
        VARDATA_EXTERNAL((*eohptr).eoh_ro_ptr.as_ptr()) as *mut c_void,
        &ptr as *const varatt_expanded as *const c_void,
        core::mem::size_of::<varatt_expanded>(),
    );
}

/// EOH_get_flat_size / EOH_flatten_into
///
/// Convenience functions for invoking the "methods" of an expanded object.
pub unsafe fn EOH_get_flat_size(eohptr: *mut ExpandedObjectHeader) -> Size {
    ((*(*eohptr).eoh_methods).get_flat_size)(eohptr)
}

pub unsafe fn EOH_flatten_into(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut c_void,
    allocated_size: Size,
) {
    ((*(*eohptr).eoh_methods).flatten_into)(eohptr, result, allocated_size);
}

/// If the Datum represents a R/W expanded object, change it to R/O. Otherwise
/// return the original Datum.
///
/// Caller must ensure that the datum is a non-null varlena value. Typically this
/// is invoked via MakeExpandedObjectReadOnly(), which checks that.
pub unsafe fn MakeExpandedObjectReadOnlyInternal(d: Datum) -> Datum {
    /* Nothing to do if not a read-write expanded-object pointer */
    if !VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d) as *const c_char) {
        return d;
    }

    /* Now safe to extract the object pointer */
    let eohptr = DatumGetEOHP(d);

    /* Return the built-in read-only pointer instead of given pointer */
    EOHPGetRODatum(eohptr)
}

/// Transfer ownership of an expanded object to a new parent memory context.
/// The object must be referenced by a R/W pointer, and what we return is always
/// its "standard" R/W pointer, which is certain to have the same lifespan as the
/// object itself. (The passed-in pointer might not, and in any case wouldn't
/// provide a unique identifier if it's not that one.)
pub unsafe fn TransferExpandedObject(d: Datum, new_parent: MemoryContext) -> Datum {
    let eohptr = DatumGetEOHP(d);

    /* Assert caller gave a R/W pointer */
    Assert!(VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d) as *const c_char));

    /* Transfer ownership */
    MemoryContextSetParent((*eohptr).eoh_context, new_parent);

    /* Return the object's standard read-write pointer */
    EOHPGetRWDatum(eohptr)
}

/// Delete an expanded object (must be referenced by a R/W pointer).
pub unsafe fn DeleteExpandedObject(d: Datum) {
    let eohptr = DatumGetEOHP(d);

    /* Assert caller gave a R/W pointer */
    Assert!(VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d) as *const c_char));

    /* Kill it */
    MemoryContextDelete((*eohptr).eoh_context);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Trivial method implementations for a fake expanded object.
    unsafe extern "C" fn fake_get_flat_size(_eohptr: *mut ExpandedObjectHeader) -> Size {
        42
    }
    unsafe extern "C" fn fake_flatten_into(
        _eohptr: *mut ExpandedObjectHeader,
        result: *mut c_void,
        _allocated_size: Size,
    ) {
        // Write a recognizable byte so the test can observe dispatch occurred.
        *(result as *mut u8) = 0xAB;
    }

    static FAKE_METHODS: ExpandedObjectMethods = ExpandedObjectMethods {
        get_flat_size: fake_get_flat_size,
        flatten_into: fake_flatten_into,
    };

    #[test]
    fn vtable_dispatch_and_datum_roundtrip() {
        unsafe {
            let mut eoh = ExpandedObjectHeader {
                vl_len_: 0,
                eoh_methods: null(),
                eoh_context: null_mut(),
                eoh_rw_ptr: [0 as c_char; EXPANDED_POINTER_SIZE],
                eoh_ro_ptr: [0 as c_char; EXPANDED_POINTER_SIZE],
            };

            // Use a null context: EOH_init_header never dereferences it.
            EOH_init_header(&mut eoh, &FAKE_METHODS, null_mut());

            // Header magic was set, so VARATT_IS_EXPANDED_HEADER recognizes it.
            assert_eq!(eoh.vl_len_, EOH_HEADER_MAGIC);
            assert!(VARATT_IS_EXPANDED_HEADER(
                &eoh as *const ExpandedObjectHeader as *const c_void
            ));

            // The embedded R/W and R/O TOAST pointers must be external-expanded.
            let rw = eoh.eoh_rw_ptr.as_ptr();
            let ro = eoh.eoh_ro_ptr.as_ptr();
            assert!(VARATT_IS_EXTERNAL_EXPANDED(rw));
            assert!(VARATT_IS_EXTERNAL_EXPANDED(ro));
            assert!(VARATT_IS_EXTERNAL_EXPANDED_RW(rw));
            assert!(!VARATT_IS_EXTERNAL_EXPANDED_RW(ro));

            // vtable dispatch through EOH_get_flat_size.
            assert_eq!(EOH_get_flat_size(&mut eoh), 42);

            // vtable dispatch through EOH_flatten_into.
            let mut buf: u8 = 0;
            EOH_flatten_into(&mut eoh, &mut buf as *mut u8 as *mut c_void, 1);
            assert_eq!(buf, 0xAB);

            // DatumGetEOHP must recover the exact header pointer from either the
            // R/W or R/O datum (it reads the embedded varatt_expanded back out).
            let rw_datum = EOHPGetRWDatum(&eoh);
            let ro_datum = EOHPGetRODatum(&eoh);
            assert_eq!(DatumGetEOHP(rw_datum), &mut eoh as *mut ExpandedObjectHeader);
            assert_eq!(DatumGetEOHP(ro_datum), &mut eoh as *mut ExpandedObjectHeader);

            // MakeExpandedObjectReadOnlyInternal turns the R/W datum into the R/O
            // one, and is a no-op on a value that's already R/O.
            assert_eq!(MakeExpandedObjectReadOnlyInternal(rw_datum), ro_datum);
            assert_eq!(MakeExpandedObjectReadOnlyInternal(ro_datum), ro_datum);
        }
    }
}
