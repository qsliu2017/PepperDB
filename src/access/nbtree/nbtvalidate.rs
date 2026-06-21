//! Source: postgres/src/backend/access/nbtree/nbtvalidate.c
//!
//! Opclass validator for btree.
//!
//! MERGED from postgres/src/include/access/nbtree.h: the btree support
//! procedure-number constants this validator references (BTORDER_PROC /
//! BTSORTSUPPORT_PROC / BTINRANGE_PROC / BTEQUALIMAGE_PROC / BTOPTIONS_PROC /
//! BTSKIPSUPPORT_PROC), and from postgres/src/include/access/stratnum.h the five
//! btree strategy numbers (BTLessStrategyNumber .. BTGreaterStrategyNumber) and
//! BTMaxStrategyNumber.
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amvalidate.h"         -> crate::access::index::amvalidate
//!   "access/htup_details.h"       -> crate::access::htup_details (GETSTRUCT)
//!   "access/nbtree.h"             -> BTORDER_PROC .. BTSKIPSUPPORT_PROC below
//!   "access/xact.h"               -> CommandCounterIncrement (STUB: xact.c not ported)
//!   "catalog/pg_am.h"             -> BTREE_AM_OID (crate::catalog::pg_known_oids)
//!   "catalog/pg_amop.h"           -> crate::catalog::pg_amop (Form_pg_amop, AMOP_SEARCH)
//!   "catalog/pg_amproc.h"         -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"        -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_type.h"           -> BOOLOID / INT4OID / OIDOID / VOIDOID /
//!                                    INTERNALOID (pg_type_d)
//!   "utils/builtins.h"            -> format_type_be (STUB)
//!   "utils/lsyscache.h"           -> get_opfamily_name / get_opclass_input_type (STUB)
//!   "utils/regproc.h"             -> format_procedure / format_operator (STUB)
//!   "utils/syscache.h"            -> SearchSysCache1 / SearchSysCacheList1 /
//!                                    ReleaseSysCache / ReleaseCatCacheList (STUB:
//!                                    syscache/catcache not ported)
//!
//! TRANSLATION NOTES (REAL vs STUBBED):
//!
//! * The per-AM VALIDATION CONTROL FLOW is ported 1:1 and is REAL: which procnums
//!   require which signature checks (check_amproc_signature /
//!   check_amoptsproc_signature with their exact arg profiles), the invalid
//!   support-number gate, the strategy-number range check (1 ..= BTMaxStrategyNumber),
//!   the ORDER-BY rejection (amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)),
//!   the per-operator signature check (check_amop_signature -> BOOLOID), the
//!   in_range-only group skip, the usefulgroups/opclassgroup/familytypes
//!   accounting, the per-group completeness checks (the exact 5-strategy mask and
//!   the BTORDER_PROC requirement), the originally-named-opclass check, and the
//!   cross-type completeness check (usefulgroups == len(familytypes)^2).  The
//!   shared helpers come from the already-ported crate::access::index::amvalidate.
//!   btadjustmembers's loose/hard dependency control flow is REAL 1:1.
//!
//! * STUBBED (deep catalog deps; syscache/catcache/xact/lsyscache not ported):
//!     - SearchSysCache1(CLAOID, ...)            -> fetch_opclass_tuple
//!     - SearchSysCacheList1(AMOPSTRATEGY, ...)  -> search_amop_list
//!     - SearchSysCacheList1(AMPROCNUM, ...)     -> search_amproc_list
//!     - get_opfamily_name / get_opclass_input_type / format_procedure /
//!       format_operator / format_type_be / CommandCounterIncrement
//!   get_opfamily_name / format_* / format_type_be are STUBS used only to build
//!   ereport message text; they panic if reached.  The check_amproc_signature /
//!   check_amop_signature wrappers themselves call the syscache stub inside
//!   amvalidate.rs, so the entrypoint cannot run end to end without the catalog.

use crate::prelude::*;

use crate::access::htup_details::GETSTRUCT;
use crate::access::index::amapi::OpFamilyMember;
use crate::access::index::amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opclass_for_family_datatype, CatCList, CatCTup, OpFamilyOpFuncGroup,
};
use crate::catalog::pg_amop::{Form_pg_amop, AMOP_SEARCH};
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_known_oids::BTREE_AM_OID;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_type_d::{BOOLOID, INT4OID, INTERNALOID, OIDOID, VOIDOID};
use crate::nodes::pg_list::{
    lfirst, list_append_unique_oid, list_concat_copy, list_length, list_nth, List, NIL,
};
use crate::{current_cell, ereport, errmsg, foreach};

// ===========================================================================
//   nbtree.h: btree support procedure numbers (MERGED header constants)
// ===========================================================================

/// Support function (amproc) numbers for btree.  See nbtree.h for details on
/// each procedure's contract.
pub const BTORDER_PROC: i16 = 1;
pub const BTSORTSUPPORT_PROC: i16 = 2;
pub const BTINRANGE_PROC: i16 = 3;
pub const BTEQUALIMAGE_PROC: i16 = 4;
pub const BTOPTIONS_PROC: i16 = 5;
pub const BTSKIPSUPPORT_PROC: i16 = 6;

// ===========================================================================
//   stratnum.h: btree strategy numbers (MERGED header constants)
// ===========================================================================

pub const BTLessStrategyNumber: i16 = 1;
pub const BTLessEqualStrategyNumber: i16 = 2;
pub const BTEqualStrategyNumber: i16 = 3;
pub const BTGreaterEqualStrategyNumber: i16 = 4;
pub const BTGreaterStrategyNumber: i16 = 5;
pub const BTMaxStrategyNumber: i16 = 5;

// ===========================================================================
//   STUBS: deep catalog / formatting / xact dependencies (syscache, lsyscache,
//   regproc, builtins, xact -- not yet ported).
// ===========================================================================

/// STUB for `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid))` +
/// GETSTRUCT -> Form_pg_opclass.  utils/syscache.c is not ported.
unsafe fn fetch_opclass_tuple(_opclassoid: Oid) -> Form_pg_opclass {
    unimplemented!("STUB: syscache CLAOID lookup (utils/syscache.c not ported)")
}

/// STUB for `SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid))`.
unsafe fn search_amop_list(_opfamilyoid: Oid) -> *const CatCList {
    unimplemented!("STUB: catcache AMOPSTRATEGY list (utils/catcache.c not ported)")
}

/// STUB for `SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid))`.
unsafe fn search_amproc_list(_opfamilyoid: Oid) -> *const CatCList {
    unimplemented!("STUB: catcache AMPROCNUM list (utils/catcache.c not ported)")
}

/// STUB for `get_opfamily_name(opfamilyoid, false)` (utils/lsyscache.c).
fn get_opfamily_name(_opfamilyoid: Oid, _missing_ok: bool) -> String {
    unimplemented!("STUB: get_opfamily_name (utils/lsyscache.c not ported)")
}

/// STUB for `get_opclass_input_type(opclassoid)` (utils/lsyscache.c).
fn get_opclass_input_type(_opclassoid: Oid) -> Oid {
    unimplemented!("STUB: get_opclass_input_type (utils/lsyscache.c not ported)")
}

/// STUB for `CommandCounterIncrement()` (access/transam/xact.c).
fn CommandCounterIncrement() {
    unimplemented!("STUB: CommandCounterIncrement (access/transam/xact.c not ported)")
}

/// STUB for `format_procedure(procoid)` (utils/regproc.c).
fn format_procedure(_procoid: Oid) -> String {
    unimplemented!("STUB: format_procedure (utils/regproc.c not ported)")
}

/// STUB for `format_operator(operoid)` (utils/regproc.c).
fn format_operator(_operoid: Oid) -> String {
    unimplemented!("STUB: format_operator (utils/regproc.c not ported)")
}

/// STUB for `format_type_be(typeoid)` (utils/adt/format_type.c).
fn format_type_be(_typeoid: Oid) -> String {
    unimplemented!("STUB: format_type_be (utils/adt/format_type.c not ported)")
}

// ===========================================================================
//   btvalidate (REAL control flow; catalog fetches STUBBED)
// ===========================================================================

/// Validator for a btree opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family.  But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API.
///
/// # Safety
/// Walks the (stubbed) syscache lists, which must contain valid CatCTups
/// carrying Form_pg_amop / Form_pg_amproc tuples.
pub unsafe fn btvalidate(opclassoid: Oid) -> bool {
    let mut result = true;

    // Fetch opclass information
    let classform = fetch_opclass_tuple(opclassoid);
    // C: if (!HeapTupleIsValid(classtup)) elog(ERROR, "cache lookup failed for
    //    operator class %u", opclassoid); -- handled inside the (stubbed) fetch.

    let opfamilyoid = (*classform).opcfamily;
    let opcintype = (*classform).opcintype;
    // C: opclassname = NameStr(classform->opcname);
    let opclassname = name_str(&(*classform).opcname);

    // Fetch opfamily information
    let opfamilyname = get_opfamily_name(opfamilyoid, false);

    // Fetch all operators and support functions of the opfamily
    let oprlist = search_amop_list(opfamilyoid);
    let proclist = search_amproc_list(opfamilyoid);

    let oprlist_ref = &*oprlist;
    let proclist_ref = &*proclist;

    // Check individual support functions
    for i in 0..proclist_ref.n_members {
        let proctup = catclist_member_tuple(proclist, i as usize);
        let procform = GETSTRUCT(proctup) as Form_pg_amproc;
        let ok;

        // Check procedure numbers and function signatures
        match (*procform).amprocnum {
            BTORDER_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    INT4OID,
                    true,
                    2,
                    2,
                    &[(*procform).amproclefttype, (*procform).amprocrighttype],
                );
            }
            BTSORTSUPPORT_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    VOIDOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                );
            }
            BTINRANGE_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    BOOLOID,
                    true,
                    5,
                    5,
                    &[
                        (*procform).amproclefttype,
                        (*procform).amproclefttype,
                        (*procform).amprocrighttype,
                        BOOLOID,
                        BOOLOID,
                    ],
                );
            }
            BTEQUALIMAGE_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    BOOLOID,
                    true,
                    1,
                    1,
                    &[OIDOID],
                );
            }
            BTOPTIONS_PROC => {
                ok = check_amoptsproc_signature((*procform).amproc);
            }
            BTSKIPSUPPORT_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    VOIDOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                );
            }
            _ => {
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                        opfamilyname,
                        "btree",
                        format_procedure((*procform).amproc),
                        (*procform).amprocnum
                    )
                );
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains function {} with wrong signature for support number {}",
                    opfamilyname,
                    "btree",
                    format_procedure((*procform).amproc),
                    (*procform).amprocnum
                )
            );
            result = false;
        }
    }

    // Check individual operators
    for i in 0..oprlist_ref.n_members {
        let oprtup = catclist_member_tuple(oprlist, i as usize);
        let oprform = GETSTRUCT(oprtup) as Form_pg_amop;

        // Check that only allowed strategy numbers exist
        if (*oprform).amopstrategy < 1 || (*oprform).amopstrategy > BTMaxStrategyNumber {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                    opfamilyname,
                    "btree",
                    format_operator((*oprform).amopopr),
                    (*oprform).amopstrategy
                )
            );
            result = false;
        }

        // btree doesn't support ORDER BY operators
        if (*oprform).amoppurpose != AMOP_SEARCH || OidIsValid((*oprform).amopsortfamily) {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                    opfamilyname,
                    "btree",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }

        // Check operator signature --- same for all btree strategies
        if !check_amop_signature(
            (*oprform).amopopr,
            BOOLOID,
            (*oprform).amoplefttype,
            (*oprform).amoprighttype,
        ) {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                    opfamilyname,
                    "btree",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions
    let grouplist = identify_opfamily_groups(oprlist, proclist);
    let mut usefulgroups: c_int = 0;
    let mut opclassgroup: *const OpFamilyOpFuncGroup = core::ptr::null();
    let mut familytypes: *mut List = NIL;
    let glen = list_length(grouplist);
    for gi in 0..glen {
        let thisgroup = list_nth(grouplist, gi) as *const OpFamilyOpFuncGroup;

        // It is possible for an in_range support function to have a RHS type that
        // is otherwise irrelevant to the opfamily --- for instance, SQL requires
        // the datetime_ops opclass to have range support with an interval offset.
        // So, if this group appears to contain only an in_range function, ignore
        // it: it doesn't represent a pair of supported types.
        if (*thisgroup).operatorset == 0
            && (*thisgroup).functionset == (1u64 << BTINRANGE_PROC)
        {
            continue;
        }

        // Else count it as a relevant group
        usefulgroups += 1;

        // Remember the group exactly matching the test opclass
        if (*thisgroup).lefttype == opcintype && (*thisgroup).righttype == opcintype {
            opclassgroup = thisgroup;
        }

        // Identify all distinct data types handled in this opfamily.  This
        // implementation is O(N^2), but there aren't likely to be enough types in
        // the family for it to matter.
        familytypes = list_append_unique_oid(familytypes, (*thisgroup).lefttype);
        familytypes = list_append_unique_oid(familytypes, (*thisgroup).righttype);

        // Complain if there seems to be an incomplete set of either operators or
        // support functions for this datatype pair.  The sortsupport, in_range,
        // and equalimage functions are considered optional.
        if (*thisgroup).operatorset
            != ((1u64 << BTLessStrategyNumber)
                | (1u64 << BTLessEqualStrategyNumber)
                | (1u64 << BTEqualStrategyNumber)
                | (1u64 << BTGreaterEqualStrategyNumber)
                | (1u64 << BTGreaterStrategyNumber))
        {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                    opfamilyname,
                    "btree",
                    format_type_be((*thisgroup).lefttype),
                    format_type_be((*thisgroup).righttype)
                )
            );
            result = false;
        }
        if ((*thisgroup).functionset & (1u64 << BTORDER_PROC)) == 0 {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing support function for types {} and {}",
                    opfamilyname,
                    "btree",
                    format_type_be((*thisgroup).lefttype),
                    format_type_be((*thisgroup).righttype)
                )
            );
            result = false;
        }
    }

    // Check that the originally-named opclass is supported
    // (if group is there, we already checked it adequately above)
    if opclassgroup.is_null() {
        ereport!(
            INFO,
            errmsg!(
                "operator class \"{}\" of access method {} is missing operator(s)",
                opclassname,
                "btree"
            )
        );
        result = false;
    }

    // Complain if the opfamily doesn't have entries for all possible combinations
    // of its supported datatypes.  While missing cross-type operators are not
    // fatal, they do limit the planner's ability to derive additional qual
    // clauses from equivalence classes, so it seems reasonable to insist that all
    // built-in btree opfamilies be complete.
    if usefulgroups != (list_length(familytypes) * list_length(familytypes)) {
        ereport!(
            INFO,
            errmsg!(
                "operator family \"{}\" of access method {} is missing cross-type operator(s)",
                opfamilyname,
                "btree"
            )
        );
        result = false;
    }

    // C: ReleaseCatCacheList(proclist); ReleaseCatCacheList(oprlist);
    //    ReleaseSysCache(classtup); -- no-ops until syscache/catcache land.

    result
}

// ===========================================================================
//   btadjustmembers (REAL, ported 1:1)
// ===========================================================================

/// Prechecking function for adding operators/functions to a btree opfamily.
///
/// # Safety
/// `operators`/`functions` must be valid Lists of `*mut OpFamilyMember`.
pub unsafe fn btadjustmembers(
    opfamilyoid: Oid,
    mut opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    let mut opcintype: Oid;

    // Btree operators and comparison support functions are always "loose" members
    // of the opfamily if they are cross-type.  If they are not cross-type, we
    // prefer to tie them to the appropriate opclass ... but if the user hasn't
    // created one, we can't do that, and must fall back to using the opfamily
    // dependency.  (We mustn't force creation of an opclass in such a case, as
    // leaving an incomplete opclass laying about would be bad.  Throwing an error
    // is another undesirable alternative.)
    //
    // This behavior results in a bit of a dump/reload hazard, in that the order
    // of restoring objects could affect what dependencies we end up with.
    // pg_dump's existing behavior will preserve the dependency choices in most
    // cases, but not if a cross-type operator has been bound tightly into an
    // opclass.  That's a mistake anyway, so silently "fixing" it isn't awful.
    //
    // Optional support functions are always "loose" family members.
    //
    // To avoid repeated lookups, we remember the most recently used opclass's
    // input type.
    if OidIsValid(opclassoid) {
        // During CREATE OPERATOR CLASS, need CCI to see the pg_opclass row
        CommandCounterIncrement();
        opcintype = get_opclass_input_type(opclassoid);
    } else {
        opcintype = InvalidOid;
    }

    // We handle operators and support functions almost identically, so rather
    // than duplicate this code block, just join the lists.
    foreach!(lc, list_concat_copy(operators, functions), {
        let op = lfirst(current_cell!(lc)) as *mut OpFamilyMember;

        if (*op).is_func && (*op).number != BTORDER_PROC as c_int {
            // Optional support proc, so always a soft family dependency
            (*op).ref_is_hard = false;
            (*op).ref_is_family = true;
            (*op).refobjid = opfamilyoid;
        } else if (*op).lefttype != (*op).righttype {
            // Cross-type, so always a soft family dependency
            (*op).ref_is_hard = false;
            (*op).ref_is_family = true;
            (*op).refobjid = opfamilyoid;
        } else {
            // Not cross-type; is there a suitable opclass?
            if (*op).lefttype != opcintype {
                // Avoid repeating this expensive lookup, even if it fails
                opcintype = (*op).lefttype;
                opclassoid =
                    opclass_for_family_datatype(BTREE_AM_OID, opfamilyoid, opcintype);
            }
            if OidIsValid(opclassoid) {
                // Hard dependency on opclass
                (*op).ref_is_hard = true;
                (*op).ref_is_family = false;
                (*op).refobjid = opclassoid;
            } else {
                // We're stuck, so make a soft dependency on the opfamily
                (*op).ref_is_hard = false;
                (*op).ref_is_family = true;
                (*op).refobjid = opfamilyoid;
            }
        }
    });
}

// ===========================================================================
//   small local helpers
// ===========================================================================

/// `&list->members[i]->tuple` for an amvalidate CatCList.  Mirrors the (private)
/// CatCList::member_tuple accessor in amvalidate.rs; reproduced here because that
/// accessor is not exported.  CatCTup begins with `tuple: HeapTupleData`, so the
/// member pointer (a `*mut CatCTup`) can be reinterpreted as a `*const
/// HeapTupleData`.
///
/// # Safety
/// `i` must be `< (*list).n_members`, and each member pointer must reference a
/// valid CatCTup.
#[inline]
pub unsafe fn catclist_member_tuple(
    list: *const CatCList,
    i: usize,
) -> *const crate::access::htup_details::HeapTupleData {
    let base = (*list).members.as_ptr();
    let memb = *base.add(i);
    &(*(memb as *const CatCTup)).tuple as *const _
}

/// `NameStr(name)` rendered to a Rust `String` for the ereport message.  The
/// catalog Name is a fixed-size NUL-padded C string.
///
/// # Safety
/// `name` must point at a valid NameData.
pub unsafe fn name_str(name: *const NameData) -> String {
    let p = NameStr(&*name);
    let cs = core::ffi::CStr::from_ptr(p);
    cs.to_string_lossy().into_owned()
}

// ===========================================================================
//                                 TESTS
//
// The btvalidate entrypoint hits the stubbed syscache/catcache fetches, so it
// cannot be exercised end to end without a live catalog.  The REAL, isolable
// pieces are the btree procedure-number / strategy constants and the per-member
// control flow those constants drive.  We unit-test that classification
// directly: a pure mirror of the proc-switch that decides, for a given
// amprocnum, which signature check (and argument profile) the C code selects;
// the strategy-range / ORDER-BY gates; the per-group completeness masks; the
// in_range-only skip; and btadjustmembers's loose-vs-hard dependency decision.
// These assertions pin the exact control-flow constants ported above.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Which signature-check profile btvalidate's proc switch selects for a given
    /// amprocnum.  `argtypes` mirrors the C `...` exactly, including the
    /// type-derived placeholders (LEFT/RIGHT) so the in_range/order layouts are
    /// pinned without needing concrete OIDs.
    #[derive(Debug, PartialEq, Eq)]
    enum ArgT {
        Fixed(Oid),
        Left,
        Right,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum ProcCheck {
        /// check_amproc_signature(_, restype, exact, minargs, maxargs, argtypes)
        Amproc {
            restype: Oid,
            exact: bool,
            minargs: c_int,
            maxargs: c_int,
            argtypes: Vec<ArgT>,
        },
        /// check_amoptsproc_signature(_)
        Opts,
        /// out-of-range procnum: INFO + result=false, omitted (continue)
        Invalid,
    }

    fn classify_proc(amprocnum: i16) -> ProcCheck {
        match amprocnum {
            BTORDER_PROC => ProcCheck::Amproc {
                restype: INT4OID,
                exact: true,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![ArgT::Left, ArgT::Right],
            },
            BTSORTSUPPORT_PROC => ProcCheck::Amproc {
                restype: VOIDOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![ArgT::Fixed(INTERNALOID)],
            },
            BTINRANGE_PROC => ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 5,
                maxargs: 5,
                argtypes: vec![
                    ArgT::Left,
                    ArgT::Left,
                    ArgT::Right,
                    ArgT::Fixed(BOOLOID),
                    ArgT::Fixed(BOOLOID),
                ],
            },
            BTEQUALIMAGE_PROC => ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![ArgT::Fixed(OIDOID)],
            },
            BTOPTIONS_PROC => ProcCheck::Opts,
            BTSKIPSUPPORT_PROC => ProcCheck::Amproc {
                restype: VOIDOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![ArgT::Fixed(INTERNALOID)],
            },
            _ => ProcCheck::Invalid,
        }
    }

    #[test]
    fn btree_procnum_constants_are_pinned() {
        assert_eq!(BTORDER_PROC, 1);
        assert_eq!(BTSORTSUPPORT_PROC, 2);
        assert_eq!(BTINRANGE_PROC, 3);
        assert_eq!(BTEQUALIMAGE_PROC, 4);
        assert_eq!(BTOPTIONS_PROC, 5);
        assert_eq!(BTSKIPSUPPORT_PROC, 6);
    }

    #[test]
    fn btree_strategy_constants_are_pinned() {
        assert_eq!(BTLessStrategyNumber, 1);
        assert_eq!(BTLessEqualStrategyNumber, 2);
        assert_eq!(BTEqualStrategyNumber, 3);
        assert_eq!(BTGreaterEqualStrategyNumber, 4);
        assert_eq!(BTGreaterStrategyNumber, 5);
        assert_eq!(BTMaxStrategyNumber, 5);
    }

    #[test]
    fn order_proc_signature_profile() {
        // BTORDER_PROC: int4(lefttype, righttype)
        assert_eq!(
            classify_proc(BTORDER_PROC),
            ProcCheck::Amproc {
                restype: INT4OID,
                exact: true,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![ArgT::Left, ArgT::Right],
            }
        );
    }

    #[test]
    fn sortsupport_and_skipsupport_share_void_internal_profile() {
        let expected = ProcCheck::Amproc {
            restype: VOIDOID,
            exact: true,
            minargs: 1,
            maxargs: 1,
            argtypes: vec![ArgT::Fixed(INTERNALOID)],
        };
        assert_eq!(classify_proc(BTSORTSUPPORT_PROC), expected);
        // BTSKIPSUPPORT_PROC uses the identical void(internal) profile.
        assert_eq!(
            classify_proc(BTSKIPSUPPORT_PROC),
            ProcCheck::Amproc {
                restype: VOIDOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![ArgT::Fixed(INTERNALOID)],
            }
        );
    }

    #[test]
    fn inrange_proc_signature_profile() {
        // BTINRANGE_PROC: bool(left, left, right, bool, bool), exactly 5 args.
        assert_eq!(
            classify_proc(BTINRANGE_PROC),
            ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 5,
                maxargs: 5,
                argtypes: vec![
                    ArgT::Left,
                    ArgT::Left,
                    ArgT::Right,
                    ArgT::Fixed(BOOLOID),
                    ArgT::Fixed(BOOLOID),
                ],
            }
        );
    }

    #[test]
    fn equalimage_proc_signature_profile() {
        // BTEQUALIMAGE_PROC: bool(oid)
        assert_eq!(
            classify_proc(BTEQUALIMAGE_PROC),
            ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![ArgT::Fixed(OIDOID)],
            }
        );
    }

    #[test]
    fn options_proc_uses_amoptsproc_check() {
        assert_eq!(classify_proc(BTOPTIONS_PROC), ProcCheck::Opts);
    }

    #[test]
    fn unknown_procnum_is_invalid() {
        // 0 and anything past BTSKIPSUPPORT_PROC are invalid support numbers.
        assert_eq!(classify_proc(0), ProcCheck::Invalid);
        assert_eq!(classify_proc(7), ProcCheck::Invalid);
        assert_eq!(classify_proc(99), ProcCheck::Invalid);
    }

    /// Mirror of btvalidate's strategy-number gate: valid iff 1 ..= BTMaxStrategyNumber.
    fn strategy_in_range(stratnum: i16) -> bool {
        !(stratnum < 1 || stratnum > BTMaxStrategyNumber)
    }

    #[test]
    fn strategy_number_range_gate() {
        assert!(!strategy_in_range(0));
        assert!(strategy_in_range(BTLessStrategyNumber));
        assert!(strategy_in_range(BTGreaterStrategyNumber));
        // BTMaxStrategyNumber is 5; 6 is out of range.
        assert!(!strategy_in_range(6));
    }

    /// Mirror of the ORDER-BY rejection gate: btree rejects any non-search
    /// operator or any operator with a sort family.
    fn orderby_invalid(amoppurpose: c_char, amopsortfamily: Oid) -> bool {
        amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)
    }

    #[test]
    fn btree_rejects_orderby_operators() {
        assert!(!orderby_invalid(AMOP_SEARCH, InvalidOid));
        assert!(orderby_invalid(AMOP_SEARCH, 1234));
        assert!(orderby_invalid(b'o' as c_char, InvalidOid));
    }

    /// The exact operator-completeness mask btvalidate requires per group.
    const FULL_OPSET: u64 = (1u64 << BTLessStrategyNumber)
        | (1u64 << BTLessEqualStrategyNumber)
        | (1u64 << BTEqualStrategyNumber)
        | (1u64 << BTGreaterEqualStrategyNumber)
        | (1u64 << BTGreaterStrategyNumber);

    #[test]
    fn group_operator_completeness_mask() {
        // All five strategies -> complete; bits 1..=5 set, nothing else.
        assert_eq!(FULL_OPSET, 0b111110);
        // Missing any single strategy fails the equality check.
        for s in [
            BTLessStrategyNumber,
            BTLessEqualStrategyNumber,
            BTEqualStrategyNumber,
            BTGreaterEqualStrategyNumber,
            BTGreaterStrategyNumber,
        ] {
            let missing = FULL_OPSET & !(1u64 << s);
            assert_ne!(missing, FULL_OPSET, "strategy {}", s);
        }
    }

    #[test]
    fn group_requires_order_proc() {
        // The per-group support requirement is exactly the BTORDER_PROC bit.
        let has = |fs: u64| (fs & (1u64 << BTORDER_PROC)) != 0;
        assert!(has(1u64 << BTORDER_PROC));
        // sortsupport/in_range/equalimage are optional: a group with only those
        // does not satisfy the requirement.
        assert!(!has(
            (1u64 << BTSORTSUPPORT_PROC)
                | (1u64 << BTINRANGE_PROC)
                | (1u64 << BTEQUALIMAGE_PROC)
        ));
    }

    /// Mirror of the in_range-only group skip: a group with no operators and
    /// exactly the in_range function bit is ignored (not counted as useful).
    fn is_inrange_only(operatorset: u64, functionset: u64) -> bool {
        operatorset == 0 && functionset == (1u64 << BTINRANGE_PROC)
    }

    #[test]
    fn inrange_only_group_skipped() {
        assert!(is_inrange_only(0, 1u64 << BTINRANGE_PROC));
        // Has an operator -> not skipped.
        assert!(!is_inrange_only(1u64 << BTEqualStrategyNumber, 1u64 << BTINRANGE_PROC));
        // Has another function too -> not skipped.
        assert!(!is_inrange_only(
            0,
            (1u64 << BTINRANGE_PROC) | (1u64 << BTORDER_PROC)
        ));
    }

    // --- btadjustmembers loose-vs-hard dependency decision -------------------

    #[derive(Debug, PartialEq, Eq)]
    enum Dep {
        /// soft dependency on the opfamily (ref_is_hard=false, ref_is_family=true)
        SoftFamily,
        /// hard dependency on the opclass (ref_is_hard=true, ref_is_family=false)
        HardOpclass,
    }

    /// Mirror of btadjustmembers's per-member decision.  `opclass_available`
    /// stands in for OidIsValid(opclass_for_family_datatype(...)) for the
    /// non-cross-type case.
    fn classify_member(
        is_func: bool,
        number: c_int,
        lefttype: Oid,
        righttype: Oid,
        opclass_available: bool,
    ) -> Dep {
        if is_func && number != BTORDER_PROC as c_int {
            Dep::SoftFamily
        } else if lefttype != righttype {
            Dep::SoftFamily
        } else if opclass_available {
            Dep::HardOpclass
        } else {
            Dep::SoftFamily
        }
    }

    #[test]
    fn adjustmembers_optional_func_is_soft_family() {
        // Optional support proc (number != BTORDER_PROC) -> soft family always.
        assert_eq!(
            classify_member(true, BTSORTSUPPORT_PROC as c_int, 10, 10, true),
            Dep::SoftFamily
        );
        assert_eq!(
            classify_member(true, BTOPTIONS_PROC as c_int, 10, 10, false),
            Dep::SoftFamily
        );
    }

    #[test]
    fn adjustmembers_order_proc_treated_like_operator() {
        // BTORDER_PROC is the one func that falls through to the cross-type/opclass
        // logic, exactly like operators.
        assert_eq!(
            classify_member(true, BTORDER_PROC as c_int, 10, 10, true),
            Dep::HardOpclass
        );
        assert_eq!(
            classify_member(true, BTORDER_PROC as c_int, 10, 20, true),
            Dep::SoftFamily
        );
    }

    #[test]
    fn adjustmembers_crosstype_operator_is_soft_family() {
        assert_eq!(
            classify_member(false, BTEqualStrategyNumber as c_int, 10, 20, true),
            Dep::SoftFamily
        );
    }

    #[test]
    fn adjustmembers_noncrosstype_operator_depends_on_opclass_availability() {
        // Same-type operator with a suitable opclass -> hard dependency on opclass.
        assert_eq!(
            classify_member(false, BTEqualStrategyNumber as c_int, 10, 10, true),
            Dep::HardOpclass
        );
        // Same-type operator with no suitable opclass -> soft family fallback.
        assert_eq!(
            classify_member(false, BTEqualStrategyNumber as c_int, 10, 10, false),
            Dep::SoftFamily
        );
    }
}
