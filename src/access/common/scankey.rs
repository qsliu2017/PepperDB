//! Translation of postgres/src/backend/access/common/scankey.c
//! (merged with the parts of postgres/src/include/access/skey.h it needs:
//! the ScanKeyData struct, the ScanKey typedef, and the SK_* flag consts).
//!
//! scan key support code.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping (from scankey.c):
//!   postgres.h                  -> crate::prelude (Datum, Oid, palloc, elog!, Assert!, ...)
//!   access/skey.h               -> MERGED HERE (ScanKeyData / ScanKey / SK_* flags)
//!   catalog/pg_collation.h      -> crate::catalog::pg_known_oids (C_COLLATION_OID)
//!
//! skey.h's own `#include`s map as:
//!   access/attnum.h             -> AttrNumber from crate::nodes::primnodes
//!   access/stratnum.h           -> STUB; StrategyNumber (a uint16) defined locally below
//!                                  with a TODO. Full strategy-number consts are not ported.
//!   fmgr.h                      -> FmgrInfo / fmgr_info / fmgr_info_copy from crate::utils::fmgr
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: ScanKeyEntryInitialize, ScanKeyInit, ScanKeyEntryInitializeWithInfo,
//!     ScanKeyInitWithCollation. These are straight struct-field assignment plus a
//!     call to fmgr_info / fmgr_info_copy. fmgr_info_copy is fully real in
//!     crate::utils::fmgr (memcpy + reset fn_mcxt/fn_extra). fmgr_info delegates to
//!     fmgr_info_cxt_security, whose non-builtin path is itself stubbed (pg_proc
//!     syscache not yet ported) - so any path here that calls fmgr_info on a
//!     non-builtin Oid will hit that stub at runtime. The scankey.c logic itself is
//!     fully translated.
//!
//! Note: ScanKeyInitWithCollation does not exist in this 18.3 skey.h/scankey.c
//! snapshot (it was introduced in later versions); it is provided here per the
//! port spec as the obvious collation-taking shorthand of ScanKeyInit.

use crate::prelude::*; // Datum, Oid, InvalidOid, the c-types (int16/uint16/Size), RegProcedure,
                       // RegProcedureIsValid, MemSet, CurrentMemoryContext, Assert!, null_mut, ...
                       // (the prelude does `pub use crate::c::*`, so the stratnum/c helpers below
                       //  come in transitively - do NOT re-`use crate::c::...` or it double-imports)
use crate::catalog::pg_known_oids::C_COLLATION_OID; // catalog/pg_collation.h
use crate::nodes::primnodes::AttrNumber; // access/attnum.h
use crate::utils::fmgr::{fmgr_info, fmgr_info_copy, FmgrInfo};
use core::ffi::c_int;

/* StrategyNumber now lives in the ported access/stratnum.rs. */
pub use crate::access::stratnum::StrategyNumber;

/*
 * ===========================================================================
 *  skey.h  --  POSTGRES scan key definitions (struct + flags merged in)
 * ===========================================================================
 *
 * A ScanKey represents the application of a comparison operator between
 * a table or index column and a constant.  See the long comment in skey.h
 * (preserved upstream) for the full semantics of each sk_flags bit, row
 * comparisons, SK_SEARCHARRAY/SK_SEARCHNULL/SK_SEARCHNOTNULL, and SK_ORDER_BY.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ScanKeyData {
    pub sk_flags: c_int,            /* flags, see below */
    pub sk_attno: AttrNumber,       /* table or index column number */
    pub sk_strategy: StrategyNumber, /* operator strategy number */
    pub sk_subtype: Oid,            /* strategy subtype */
    pub sk_collation: Oid,          /* collation to use, if needed */
    pub sk_func: FmgrInfo,          /* lookup info for function to call */
    pub sk_argument: Datum,         /* data to compare */
}

pub type ScanKey = *mut ScanKeyData;

/*
 * ScanKeyData sk_flags
 *
 * sk_flags bits 0-15 are reserved for system-wide use (symbols for those
 * bits should be defined here).  Bits 16-31 are reserved for use within
 * individual index access methods.
 */
pub const SK_ISNULL: c_int = 0x0001; /* sk_argument is NULL */
pub const SK_UNARY: c_int = 0x0002; /* unary operator (not supported!) */
pub const SK_ROW_HEADER: c_int = 0x0004; /* row comparison header (see skey.h) */
pub const SK_ROW_MEMBER: c_int = 0x0008; /* row comparison member (see skey.h) */
pub const SK_ROW_END: c_int = 0x0010; /* last row comparison member */
pub const SK_SEARCHARRAY: c_int = 0x0020; /* scankey represents ScalarArrayOp */
pub const SK_SEARCHNULL: c_int = 0x0040; /* scankey represents "col IS NULL" */
pub const SK_SEARCHNOTNULL: c_int = 0x0080; /* scankey represents "col IS NOT NULL" */
pub const SK_ORDER_BY: c_int = 0x0100; /* scankey is for ORDER BY op */

/*
 * ===========================================================================
 *  scankey.c
 * ===========================================================================
 */

/*
 * ScanKeyEntryInitialize
 *		Initializes a scan key entry given all the field values.
 *		The target procedure is specified by OID (but can be invalid
 *		if SK_SEARCHNULL or SK_SEARCHNOTNULL is set).
 *
 * Note: CurrentMemoryContext at call should be as long-lived as the ScanKey
 * itself, because that's what will be used for any subsidiary info attached
 * to the ScanKey's FmgrInfo record.
 */
#[no_mangle]
pub unsafe fn ScanKeyEntryInitialize(
    entry: ScanKey,
    flags: c_int,
    attributeNumber: AttrNumber,
    strategy: StrategyNumber,
    subtype: Oid,
    collation: Oid,
    procedure: RegProcedure,
    argument: Datum,
) {
    (*entry).sk_flags = flags;
    (*entry).sk_attno = attributeNumber;
    (*entry).sk_strategy = strategy;
    (*entry).sk_subtype = subtype;
    (*entry).sk_collation = collation;
    (*entry).sk_argument = argument;
    if RegProcedureIsValid(procedure) {
        fmgr_info(procedure, &mut (*entry).sk_func);
    } else {
        Assert!((flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) != 0);
        MemSet(
            (&mut (*entry).sk_func) as *mut FmgrInfo as *mut core::ffi::c_void,
            0,
            core::mem::size_of::<FmgrInfo>(),
        );
    }
}

/*
 * ScanKeyInit
 *		Shorthand version of ScanKeyEntryInitialize: flags and subtype
 *		are assumed to be zero (the usual value), and collation is defaulted.
 *
 * This is the recommended version for hardwired lookups in system catalogs.
 * It cannot handle NULL arguments, unary operators, or nondefault operators,
 * but we need none of those features for most hardwired lookups.
 *
 * We set collation to C_COLLATION_OID always.  This is the correct value
 * for all collation-aware columns in system catalogs, and it will be ignored
 * for other column types, so it's not worth trying to be more finicky.
 *
 * Note: CurrentMemoryContext at call should be as long-lived as the ScanKey
 * itself, because that's what will be used for any subsidiary info attached
 * to the ScanKey's FmgrInfo record.
 */
pub unsafe fn ScanKeyInit(
    entry: ScanKey,
    attributeNumber: AttrNumber,
    strategy: StrategyNumber,
    procedure: RegProcedure,
    argument: Datum,
) {
    (*entry).sk_flags = 0;
    (*entry).sk_attno = attributeNumber;
    (*entry).sk_strategy = strategy;
    (*entry).sk_subtype = InvalidOid;
    (*entry).sk_collation = C_COLLATION_OID;
    (*entry).sk_argument = argument;
    fmgr_info(procedure, &mut (*entry).sk_func);
}

/*
 * ScanKeyInitWithCollation
 *		Like ScanKeyInit, but the caller supplies the collation explicitly
 *		rather than defaulting it to C_COLLATION_OID. flags and subtype are
 *		still assumed to be zero.
 *
 * Note: this convenience entry point is not present in the 18.3 scankey.c
 * snapshot; it is added per the port spec as the obvious collation-taking
 * variant of ScanKeyInit. The field-setting logic mirrors ScanKeyInit exactly
 * except that sk_collation is taken from the argument.
 */
pub unsafe fn ScanKeyInitWithCollation(
    entry: ScanKey,
    attributeNumber: AttrNumber,
    strategy: StrategyNumber,
    procedure: RegProcedure,
    argument: Datum,
    collation: Oid,
) {
    (*entry).sk_flags = 0;
    (*entry).sk_attno = attributeNumber;
    (*entry).sk_strategy = strategy;
    (*entry).sk_subtype = InvalidOid;
    (*entry).sk_collation = collation;
    (*entry).sk_argument = argument;
    fmgr_info(procedure, &mut (*entry).sk_func);
}

/*
 * ScanKeyEntryInitializeWithInfo
 *		Initializes a scan key entry using an already-completed FmgrInfo
 *		function lookup record.
 *
 * Note: CurrentMemoryContext at call should be as long-lived as the ScanKey
 * itself, because that's what will be used for any subsidiary info attached
 * to the ScanKey's FmgrInfo record.
 */
pub unsafe fn ScanKeyEntryInitializeWithInfo(
    entry: ScanKey,
    flags: c_int,
    attributeNumber: AttrNumber,
    strategy: StrategyNumber,
    subtype: Oid,
    collation: Oid,
    finfo: *mut FmgrInfo,
    argument: Datum,
) {
    (*entry).sk_flags = flags;
    (*entry).sk_attno = attributeNumber;
    (*entry).sk_strategy = strategy;
    (*entry).sk_subtype = subtype;
    (*entry).sk_collation = collation;
    (*entry).sk_argument = argument;
    fmgr_info_copy(&mut (*entry).sk_func, finfo, CurrentMemoryContext);
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;

    /// SK_* flag constants must match skey.h hex values exactly.
    #[test]
    fn sk_flag_values() {
        assert_eq!(SK_ISNULL, 0x0001);
        assert_eq!(SK_UNARY, 0x0002);
        assert_eq!(SK_ROW_HEADER, 0x0004);
        assert_eq!(SK_ROW_MEMBER, 0x0008);
        assert_eq!(SK_ROW_END, 0x0010);
        assert_eq!(SK_SEARCHARRAY, 0x0020);
        assert_eq!(SK_SEARCHNULL, 0x0040);
        assert_eq!(SK_SEARCHNOTNULL, 0x0080);
        assert_eq!(SK_ORDER_BY, 0x0100);
    }

    /// ScanKeyEntryInitialize with an invalid procedure and SK_SEARCHNULL set
    /// exercises the real field-setting + the MemSet(sk_func) branch (which
    /// does NOT call fmgr_info, so it avoids the pg_proc syscache stub).
    #[test]
    fn entry_initialize_searchnull_zeroes_func() {
        unsafe {
            let mut k = MaybeUninit::<ScanKeyData>::zeroed().assume_init();
            let p: ScanKey = &mut k;

            // Poison sk_func.fn_oid so we can prove MemSet zeroed it.
            (*p).sk_func.fn_oid = 12345 as Oid;

            ScanKeyEntryInitialize(
                p,
                SK_SEARCHNULL | SK_ISNULL,
                7 as AttrNumber,
                3 as StrategyNumber,
                InvalidOid,                       // subtype
                C_COLLATION_OID,                  // collation
                0 as RegProcedure,                // InvalidOid procedure
                0 as Datum,                       // argument
            );

            assert_eq!((*p).sk_flags, SK_SEARCHNULL | SK_ISNULL);
            assert_eq!((*p).sk_attno, 7 as AttrNumber);
            assert_eq!((*p).sk_strategy, 3 as StrategyNumber);
            assert_eq!((*p).sk_subtype, InvalidOid);
            assert_eq!((*p).sk_collation, C_COLLATION_OID);
            assert_eq!((*p).sk_argument, 0 as Datum);
            // The invalid-procedure branch MemSet's sk_func to all-zero.
            assert_eq!((*p).sk_func.fn_oid, InvalidOid);
            assert!((*p).sk_func.fn_addr.is_none());
            assert_eq!((*p).sk_func.fn_nargs, 0);
        }
    }

    /// ScanKeyEntryInitializeWithInfo copies a prebuilt FmgrInfo (real path:
    /// fmgr_info_copy is fully implemented and needs no syscache).
    #[test]
    fn entry_initialize_with_info_copies_finfo() {
        unsafe {
            let mut k = MaybeUninit::<ScanKeyData>::zeroed().assume_init();
            let p: ScanKey = &mut k;

            // A fully-formed source FmgrInfo (as if from a prior fmgr_info).
            let mut src = MaybeUninit::<FmgrInfo>::zeroed().assume_init();
            src.fn_oid = 42 as Oid;
            src.fn_nargs = 2;
            src.fn_strict = true;
            src.fn_retset = false;
            src.fn_extra = 0xDEAD_BEEFusize as *mut core::ffi::c_void; // must be cleared by copy

            ScanKeyEntryInitializeWithInfo(
                p,
                SK_ISNULL,
                4 as AttrNumber,
                5 as StrategyNumber,
                100 as Oid,        // subtype
                950 as Oid,        // collation
                &mut src,
                99 as Datum,       // argument
            );

            assert_eq!((*p).sk_flags, SK_ISNULL);
            assert_eq!((*p).sk_attno, 4 as AttrNumber);
            assert_eq!((*p).sk_strategy, 5 as StrategyNumber);
            assert_eq!((*p).sk_subtype, 100 as Oid);
            assert_eq!((*p).sk_collation, 950 as Oid);
            assert_eq!((*p).sk_argument, 99 as Datum);

            // fmgr_info_copy duplicates the lookup fields but resets fn_extra.
            assert_eq!((*p).sk_func.fn_oid, 42 as Oid);
            assert_eq!((*p).sk_func.fn_nargs, 2);
            assert_eq!((*p).sk_func.fn_strict, true);
            assert_eq!((*p).sk_func.fn_retset, false);
            assert!((*p).sk_func.fn_extra.is_null());
        }
    }

    /// ScanKeyInit / ScanKeyInitWithCollation default subtype/flags and set the
    /// collation as documented. We avoid the fmgr_info syscache stub by checking
    /// only the pure field-setting performed *before* the fmgr_info call: do that
    /// by replaying the body's assignments here against the public contract. To
    /// exercise the real routine without panicking we'd need a builtin Oid; since
    /// the builtin table is not ported, we instead assert the documented invariants
    /// that don't depend on fmgr_info by reaching it through ScanKeyEntryInitialize
    /// with a valid-looking but we cannot - so we restrict to the no-fmgr fields.
    ///
    /// Concretely: confirm InvalidOid subtype + C_COLLATION_OID default match the
    /// constants ScanKeyInit relies upon (a guard against the wrong collation OID
    /// being wired in).
    #[test]
    fn scankeyinit_defaults_constants() {
        assert_eq!(C_COLLATION_OID, 950 as Oid);
        assert_eq!(InvalidOid, 0 as Oid);
    }
}
