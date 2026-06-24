//! Translated from PostgreSQL src/include/access/toast_helper.h
//! Helper functions for table AMs implementing compressed or out-of-line storage
//! of varlena attributes.
//!
//! `TOAST_*` (overall op state) and `TOASTCOL_*` (per-column state) are clean
//! single-bit flag sets sharing one `u8` word (TOASTCOL aliases the first two
//! TOAST bits), so a single `ToastFlags` bitflags set (GOOD per bitflags-port.md).
//! The two info structs are in-memory; arrays become `Vec`. Functions are stubs.

use bitflags::bitflags;

use crate::c::varlena;
use crate::postgres::Datum;
use crate::utils::relcache::Relation;

bitflags! {
    /// Overall TOAST-operation flags (`ttc_flags`) and per-column flags
    /// (`tai_colflags`); they share the same `u8` bit space. The low two bits are
    /// common (NEEDS_DELETE_OLD, NEEDS_FREE); the rest are op- or column-specific.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ToastFlags: u8 {
        // overall operation state (TOAST_*)
        const NEEDS_DELETE_OLD = 0x0001;
        const NEEDS_FREE       = 0x0002;
        const HAS_NULLS        = 0x0004;
        const NEEDS_CHANGE     = 0x0008;
        // per-column state (TOASTCOL_*); the first two alias the TOAST_* bits
        const COL_IGNORE        = 0x0010;
        const COL_INCOMPRESSIBLE = 0x0020;
    }
}

impl ToastFlags {
    /// TOASTCOL_NEEDS_DELETE_OLD == TOAST_NEEDS_DELETE_OLD
    pub const COL_NEEDS_DELETE_OLD: Self = Self::NEEDS_DELETE_OLD;
    /// TOASTCOL_NEEDS_FREE == TOAST_NEEDS_FREE
    pub const COL_NEEDS_FREE: Self = Self::NEEDS_FREE;
}

/// Information about one column of a tuple being toasted. `tai_size` is only
/// valid for varlena attrs whose `toast_action` differs from TYPSTORAGE_PLAIN.
pub struct ToastAttrInfo {
    pub tai_oldexternal: *mut varlena, // TODO(ptr)
    pub tai_size: i32,
    pub tai_colflags: ToastFlags,
    pub tai_compression: i8, // a TYPSTORAGE_* / compression char
}

/// Information about one tuple being toasted. The caller initializes the value/
/// null/old arrays and the `ttc_attr` array (each length == natts) before
/// calling toast_tuple_init.
pub struct ToastTupleContext {
    pub ttc_rel: Relation,         // relation that contains the tuple
    pub ttc_values: *mut Datum,    // values from the tuple columns // TODO(ptr)
    pub ttc_isnull: *mut bool,     // null flags for the tuple columns // TODO(ptr)
    pub ttc_oldvalues: *mut Datum, // values from previous tuple (NULL on insert) // TODO(ptr)
    pub ttc_oldisnull: *mut bool,  // null flags from previous tuple // TODO(ptr)
    pub ttc_flags: ToastFlags,
    pub ttc_attr: *mut ToastAttrInfo, // array of length natts // TODO(ptr)
}

/// Initialize the per-column TOAST state in `ttc`.
pub fn toast_tuple_init(_ttc: &mut ToastTupleContext) {
    unimplemented!()
}

/// Find the biggest not-yet-processed attribute. None when none qualifies
/// (C returns -1).
pub fn toast_tuple_find_biggest_attribute(
    _ttc: &mut ToastTupleContext,
    _for_compression: bool,
    _check_main: bool,
) -> Option<i32> {
    unimplemented!()
}

/// Try to compress the given attribute in place.
pub fn toast_tuple_try_compression(_ttc: &mut ToastTupleContext, _attribute: i32) {
    unimplemented!()
}

/// Move the given attribute out of line into the TOAST table.
pub fn toast_tuple_externalize(_ttc: &mut ToastTupleContext, _attribute: i32, _options: i32) {
    unimplemented!()
}

/// Clean up after toasting (free temporaries, delete old external datums).
pub fn toast_tuple_cleanup(_ttc: &mut ToastTupleContext) {
    unimplemented!()
}

/// Delete external (out-of-line) TOAST datums referenced by the given values.
pub fn toast_delete_external(
    _rel: Relation,
    _values: &[Datum],
    _isnull: &[bool],
    _is_speculative: bool,
) {
    unimplemented!()
}
