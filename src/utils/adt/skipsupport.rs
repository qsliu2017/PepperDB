//! skipsupport.rs - Support routines for B-Tree skip scan.
//!
//! 1:1 port of postgres/src/backend/utils/adt/skipsupport.c, MERGED with the
//! declarations from utils/skipsupport.h (SkipSupportData struct, SkipSupport
//! typedef, SkipSupportIncDec fn-ptr type).
//!
//! Header mapping:
//!   "postgres.h"            -> use crate::prelude::*
//!   "access/nbtree.h"       -> BTSKIPSUPPORT_PROC const (= 6), defined locally
//!   "utils/lsyscache.h"     -> get_opfamily_proc (NOT ported - stubbed below)
//!   "utils/skipsupport.h"   -> merged into this file
//!   "utils/relcache.h"      -> Relation (opaque RelationData pointer)
//!
//! B-Tree operator classes for discrete types can optionally provide a support
//! function for skipping.  This is used during skip scans.  A B-tree operator
//! class that implements skip support provides B-tree index scans with a way of
//! enumerating and iterating through every possible value from the domain of
//! indexable values.

use crate::prelude::*;
use crate::OidFunctionCall1;

/// `Relation` from utils/relcache.h - opaque RelationData pointer.  Only used
/// in the SkipSupportIncDec callback signature here; the full RelationData
/// struct is not needed for this file.
pub type Relation = *mut c_void;

/// `BTSKIPSUPPORT_PROC` from access/nbtree.h.
///
/// B-tree operator classes may choose to offer a sixth amproc procedure
/// (BTSKIPSUPPORT_PROC).  Confirmed against nbtree.h:
///   #define BTSKIPSUPPORT_PROC  6
pub const BTSKIPSUPPORT_PROC: u16 = 6;

/// `typedef Datum (*SkipSupportIncDec) (Relation rel, Datum existing,
///                                      bool *overflow);`
///
/// Decrement/increment callback.  Returns a decremented/incremented copy of
/// caller's existing datum.  When called with a value already matching
/// low_elem (or high_elem) the function sets *overflow.
pub type SkipSupportIncDec =
    Option<unsafe extern "C" fn(rel: Relation, existing: Datum, overflow: *mut bool) -> Datum>;

/// `typedef struct SkipSupportData *SkipSupport;`
pub type SkipSupport = *mut SkipSupportData;

/// State/callbacks used by skip arrays to procedurally generate elements.
///
/// A BTSKIPSUPPORT_PROC function must set each and every field when called
/// (there are no optional fields).
#[repr(C)]
pub struct SkipSupportData {
    /// lowest sorting/leftmost non-NULL value (assuming ascending order)
    pub low_elem: Datum,
    /// highest sorting/rightmost non-NULL value (assuming ascending order)
    pub high_elem: Datum,

    /// Returns a decremented copy of caller's existing datum.
    pub decrement: SkipSupportIncDec,
    /// Returns an incremented copy of caller's existing datum.
    pub increment: SkipSupportIncDec,
}

/// `get_opfamily_proc(opfamily, lefttype, righttype, procnum)` from
/// utils/lsyscache.h.  NOT YET PORTED (lsyscache depends on the syscache, which
/// is not yet available in this crate).
///
/// STUB: always returns InvalidOid.  This makes PrepareSkipSupportFromOpclass
/// take the conservative no-skip path (returns NULL), which is always correct -
/// the B-Tree code falls back on next-key sentinel values when an opclass
/// provides no skip support function.
// TODO(pg-port): replace with the real lsyscache lookup once the syscache is
// ported; it should consult pg_amproc for (opfamily, lefttype, righttype,
// procnum).
unsafe fn get_opfamily_proc(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _procnum: u16,
) -> Oid { crate::utils::cache::lsyscache::get_opfamily_proc(_opfamily as _, _lefttype as _, _righttype as _, _procnum as _) as _ }

/// `reverse_skip_support` - factored-out helper implementing the DESC/reverse
/// transform from PrepareSkipSupportFromOpclass.
///
/// Swaps low_elem with high_elem, and swaps decrement with increment.  Kept
/// separate so the swap logic can be exercised directly by unit tests without a
/// catalog lookup.
///
/// # Safety
/// `sksup` must point to a valid, fully-initialized SkipSupportData.
unsafe fn reverse_skip_support(sksup: SkipSupport) {
    let low_elem = (*sksup).low_elem;
    let decrement = (*sksup).decrement;

    (*sksup).low_elem = (*sksup).high_elem;
    (*sksup).decrement = (*sksup).increment;

    (*sksup).high_elem = low_elem;
    (*sksup).increment = decrement;
}

/// Fill in SkipSupport given an operator class (opfamily + opcintype).
///
/// On success, returns skip support struct, allocating in caller's memory
/// context.  Otherwise returns NULL, indicating that operator class has no skip
/// support function.
///
/// # Safety
/// Performs catalog lookups and invokes the opclass-provided support function
/// via OidFunctionCall1; safe to call only in a backend with the relevant
/// catalogs available.
#[no_mangle]
pub unsafe extern "C" fn PrepareSkipSupportFromOpclass(
    opfamily: Oid,
    opcintype: Oid,
    reverse: bool,
) -> SkipSupport {
    /* Look for a skip support function */
    let skipSupportFunction =
        get_opfamily_proc(opfamily, opcintype, opcintype, BTSKIPSUPPORT_PROC);
    if !OidIsValid(skipSupportFunction) {
        return null_mut();
    }

    let sksup: SkipSupport = palloc(core::mem::size_of::<SkipSupportData>()) as SkipSupport;
    OidFunctionCall1!(skipSupportFunction, PointerGetDatum(sksup as *const c_void));

    if reverse {
        /*
         * DESC/reverse case: swap low_elem with high_elem, and swap decrement
         * with increment
         */
        reverse_skip_support(sksup);
    }

    sksup
}

#[cfg(test)]
mod tests {
    use super::*;

    // With get_opfamily_proc stubbed to InvalidOid, PrepareSkipSupportFromOpclass
    // must return NULL (the conservative no-skip path) regardless of reverse.
    #[test]
    fn prepare_returns_null_when_no_proc() {
        unsafe {
            assert!(PrepareSkipSupportFromOpclass(123 as Oid, 456 as Oid, false).is_null());
            assert!(PrepareSkipSupportFromOpclass(123 as Oid, 456 as Oid, true).is_null());
        }
    }

    unsafe extern "C" fn dec_cb(_rel: Relation, existing: Datum, _of: *mut bool) -> Datum {
        existing
    }
    unsafe extern "C" fn inc_cb(_rel: Relation, existing: Datum, _of: *mut bool) -> Datum {
        existing
    }

    // Exercise the reverse-swap logic by hand-building a SkipSupportData and
    // applying the swap directly.
    #[test]
    fn reverse_swaps_elems_and_callbacks() {
        let mut sksup = SkipSupportData {
            low_elem: 10 as Datum,
            high_elem: 99 as Datum,
            decrement: Some(dec_cb),
            increment: Some(inc_cb),
        };

        unsafe {
            reverse_skip_support(&mut sksup as SkipSupport);
        }

        // low/high swapped
        assert_eq!(sksup.low_elem, 99 as Datum);
        assert_eq!(sksup.high_elem, 10 as Datum);

        // decrement/increment swapped: decrement now points at the original
        // increment (inc_cb), and increment at the original decrement (dec_cb).
        let dec_ptr = sksup.decrement.unwrap() as usize;
        let inc_ptr = sksup.increment.unwrap() as usize;
        assert_eq!(dec_ptr, inc_cb as usize);
        assert_eq!(inc_ptr, dec_cb as usize);
    }

    // Verify struct field layout matches the C SkipSupportData (repr(C)):
    // two Datums followed by two fn-ptrs, no padding surprises.
    #[test]
    fn struct_layout() {
        assert_eq!(
            core::mem::size_of::<SkipSupportData>(),
            2 * core::mem::size_of::<Datum>() + 2 * core::mem::size_of::<usize>()
        );
        assert_eq!(core::mem::offset_of!(SkipSupportData, low_elem), 0);
        assert_eq!(
            core::mem::offset_of!(SkipSupportData, high_elem),
            core::mem::size_of::<Datum>()
        );
    }
}
