//! Source: postgres/src/backend/access/gin/ginvalidate.c
//!
//! Opclass validator for GIN.
//!
//! MERGED from postgres/src/include/access/gin.h: the GIN support-function
//! procedure-number constants (`GIN_COMPARE_PROC` .. `GIN_OPTIONS_PROC`) and
//! `GINNProcs`, which `ginvalidate` consults to decide which support functions
//! are required/optional and what signature each must have.
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amvalidate.h"         -> crate::access::index::amvalidate
//!   "access/gin_private.h"        -> (constants below; rest unused here)
//!   "access/htup_details.h"       -> crate::access::htup_details (GETSTRUCT)
//!   "catalog/pg_amop.h"           -> crate::catalog::pg_amop (Form_pg_amop, AMOP_SEARCH)
//!   "catalog/pg_amproc.h"         -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"        -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_type.h"           -> crate::catalog::pg_type_d (BOOLOID, INT2OID, ...)
//!   "utils/lsyscache.h"           -> get_opfamily_name (STUB: lsyscache.c not ported)
//!   "utils/regproc.h"             -> format_procedure / format_operator (STUB: regproc.c
//!                                    not ported -- used only to build the INFO message text)
//!   "utils/syscache.h"            -> SearchSysCache1 / SearchSysCacheList1 / Release*
//!                                    (STUB: syscache/catcache not ported)
//!
//! TRANSLATION NOTES (deviations from the strict 1:1 C source):
//!
//! * The catalog plumbing in `ginvalidate` -- SearchSysCache1(CLAOID),
//!   get_opfamily_name, SearchSysCacheList1(AMOPSTRATEGY / AMPROCNUM) -- is
//!   STUBBED, because utils/syscache.c, utils/catcache.c, utils/cache/lsyscache.c
//!   and utils/adt/regproc.c are not yet ported.  These are isolated behind small
//!   helper fns (`fetch_opclass`, `get_opfamily_name`, `search_amopstrategy_list`,
//!   `search_amprocnum_list`, `format_procedure`, `format_operator`) that
//!   `unimplemented!()`.  Everything else -- the per-support-function procnum
//!   dispatch, the exact `check_amproc_signature` argument lists, the operator
//!   strategy/purpose/signature checks, the group-completeness logic, and the
//!   required-vs-optional procnum rules -- is the REAL C control flow, ported 1:1.
//!
//! * `identify_opfamily_groups`, `check_amproc_signature`,
//!   `check_amoptsproc_signature`, `check_amop_signature` are the SHARED helpers
//!   already ported in access/index/amvalidate.rs; we import and call them with
//!   the exact same arguments as the C.
//!
//! * The C `ereport(INFO, (errcode(...), errmsg(fmt, ...)))` calls become
//!   `ereport!(INFO, format!(...))` -- INFO does not panic, so we set
//!   `result = false` and continue, exactly as C does.
//!
//! * `ginadjustmembers` is ported 1:1; it mutates the ref_* fields of the
//!   OpFamilyMember list elements (no catalog access needed).

use crate::prelude::*;

use crate::access::htup_details::GETSTRUCT;
use crate::access::index::amapi::OpFamilyMember;
use crate::access::index::amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, CatCList, OpFamilyOpFuncGroup,
};
use crate::catalog::pg_amop::{Form_pg_amop, AMOP_SEARCH};
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_type_d::{BOOLOID, CHAROID, INT2OID, INT4OID, INTERNALOID};
use crate::nodes::pg_list::{list_length, list_nth, List};

// ===========================================================================
//   access/gin.h: GIN support-function procedure numbers (MERGED constants).
// ===========================================================================

/// support function number 1: gin compare function
pub const GIN_COMPARE_PROC: c_int = 1;
/// support function number 2: gin extract-value function
pub const GIN_EXTRACTVALUE_PROC: c_int = 2;
/// support function number 3: gin extract-query function
pub const GIN_EXTRACTQUERY_PROC: c_int = 3;
/// support function number 4: gin consistent function
pub const GIN_CONSISTENT_PROC: c_int = 4;
/// support function number 5: gin compare-partial function
pub const GIN_COMPARE_PARTIAL_PROC: c_int = 5;
/// support function number 6: gin tri-state consistent function
pub const GIN_TRICONSISTENT_PROC: c_int = 6;
/// support function number 7: gin options function
pub const GIN_OPTIONS_PROC: c_int = 7;
/// number of GIN support function slots
pub const GINNProcs: c_int = 7;

// ===========================================================================
//   STUBS for the catalog/cache plumbing not yet ported.
// ===========================================================================

/// Minimal fetched view of the pg_opclass row that `ginvalidate` reads.
struct OpclassInfo {
    opfamilyoid: Oid,
    opcintype: Oid,
    opckeytype: Oid,
    opclassname: String,
}

/// STUB for `SearchSysCache1(CLAOID, opclassoid)` + GETSTRUCT(classform) field
/// extraction.  utils/syscache.c is not ported; panics until it is.
fn fetch_opclass(_opclassoid: Oid) -> OpclassInfo {
    // C:
    //   classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    //   if (!HeapTupleIsValid(classtup))
    //       elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
    //   classform = (Form_pg_opclass) GETSTRUCT(classtup);
    //   opfamilyoid = classform->opcfamily;
    //   opcintype   = classform->opcintype;
    //   opckeytype  = classform->opckeytype;
    //   if (!OidIsValid(opckeytype)) opckeytype = opcintype;
    //   opclassname = NameStr(classform->opcname);
    unimplemented!("STUB: syscache CLAOID lookup (utils/syscache.c not ported)")
}

/// STUB for `get_opfamily_name(opfamilyoid, false)` (utils/cache/lsyscache.c).
fn get_opfamily_name(_opfamilyoid: Oid, _missing_ok: bool) -> String {
    unimplemented!("STUB: get_opfamily_name (utils/cache/lsyscache.c not ported)")
}

/// STUB for `SearchSysCacheList1(AMOPSTRATEGY, opfamilyoid)` (utils/catcache.c).
fn search_amopstrategy_list(_opfamilyoid: Oid) -> *const CatCList {
    unimplemented!("STUB: catcache AMOPSTRATEGY list (utils/catcache.c not ported)")
}

/// STUB for `SearchSysCacheList1(AMPROCNUM, opfamilyoid)` (utils/catcache.c).
fn search_amprocnum_list(_opfamilyoid: Oid) -> *const CatCList {
    unimplemented!("STUB: catcache AMPROCNUM list (utils/catcache.c not ported)")
}

/// STUB for `format_procedure(procoid)` (utils/adt/regproc.c) -- builds the
/// human-readable function name used in the INFO message text only.
fn format_procedure(procoid: Oid) -> String {
    unimplemented!("STUB: format_procedure {} (utils/adt/regproc.c not ported)", procoid)
}

/// STUB for `format_operator(oproid)` (utils/adt/regproc.c).
fn format_operator(oproid: Oid) -> String {
    unimplemented!("STUB: format_operator {} (utils/adt/regproc.c not ported)", oproid)
}

// ===========================================================================
//   ginvalidate (control flow REAL; catalog fetches STUBBED)
// ===========================================================================

/// Validator for a GIN opclass.
///
/// # Safety
/// Dereferences the (stubbed) AMOPSTRATEGY / AMPROCNUM CatCLists, whose members
/// must carry valid Form_pg_amop / Form_pg_amproc tuples.
pub unsafe fn ginvalidate(opclassoid: Oid) -> bool {
    let mut result = true;

    // Fetch opclass information (STUB).
    let class = fetch_opclass(opclassoid);
    let opfamilyoid = class.opfamilyoid;
    let opcintype = class.opcintype;
    let opckeytype = class.opckeytype;
    let opclassname = class.opclassname;

    // Fetch opfamily information (STUB).
    let opfamilyname = get_opfamily_name(opfamilyoid, false);

    // Fetch all operators and support functions of the opfamily (STUB).
    let oprlist = search_amopstrategy_list(opfamilyoid);
    let proclist = search_amprocnum_list(opfamilyoid);
    let oprlist_ref = &*oprlist;
    let proclist_ref = &*proclist;

    // Check individual support functions.
    for i in 0..proclist_ref.n_members {
        let proctup = catclist_member_tuple(proclist_ref, i as usize);
        let procform = GETSTRUCT(proctup) as Form_pg_amproc;
        let ok: bool;

        // All GIN support functions should be registered with matching
        // left/right types.
        if (*procform).amproclefttype != (*procform).amprocrighttype {
            ereport!(
                INFO,
                format!(
                    "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                    opfamilyname, "gin",
                    format_procedure((*procform).amproc)
                )
            );
            result = false;
        }

        // We can't check signatures except within the specific opclass, since we
        // need to know the associated opckeytype in many cases.
        if (*procform).amproclefttype != opcintype {
            continue;
        }

        // Check procedure numbers and function signatures.
        match (*procform).amprocnum as c_int {
            GIN_COMPARE_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc, INT4OID, false,
                    2, 2, &[opckeytype, opckeytype],
                );
            }
            GIN_EXTRACTVALUE_PROC => {
                // Some opclasses omit nullFlags.
                ok = check_amproc_signature(
                    (*procform).amproc, INTERNALOID, false,
                    2, 3, &[opcintype, INTERNALOID, INTERNALOID],
                );
            }
            GIN_EXTRACTQUERY_PROC => {
                // Some opclasses omit nullFlags and searchMode.
                ok = check_amproc_signature(
                    (*procform).amproc, INTERNALOID, false,
                    5, 7,
                    &[opcintype, INTERNALOID, INT2OID, INTERNALOID, INTERNALOID,
                      INTERNALOID, INTERNALOID],
                );
            }
            GIN_CONSISTENT_PROC => {
                // Some opclasses omit queryKeys and nullFlags.
                ok = check_amproc_signature(
                    (*procform).amproc, BOOLOID, false,
                    6, 8,
                    &[INTERNALOID, INT2OID, opcintype, INT4OID,
                      INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID],
                );
            }
            GIN_COMPARE_PARTIAL_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc, INT4OID, false,
                    4, 4, &[opckeytype, opckeytype, INT2OID, INTERNALOID],
                );
            }
            GIN_TRICONSISTENT_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc, CHAROID, false,
                    7, 7,
                    &[INTERNALOID, INT2OID, opcintype, INT4OID,
                      INTERNALOID, INTERNALOID, INTERNALOID],
                );
            }
            GIN_OPTIONS_PROC => {
                ok = check_amoptsproc_signature((*procform).amproc);
            }
            _ => {
                ereport!(
                    INFO,
                    format!(
                        "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                        opfamilyname, "gin",
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
                format!(
                    "operator family \"{}\" of access method {} contains function {} with wrong signature for support number {}",
                    opfamilyname, "gin",
                    format_procedure((*procform).amproc),
                    (*procform).amprocnum
                )
            );
            result = false;
        }
    }

    // Check individual operators.
    for i in 0..oprlist_ref.n_members {
        let oprtup = catclist_member_tuple(oprlist_ref, i as usize);
        let oprform = GETSTRUCT(oprtup) as Form_pg_amop;

        // TODO: Check that only allowed strategy numbers exist.
        if (*oprform).amopstrategy < 1 || (*oprform).amopstrategy > 63 {
            ereport!(
                INFO,
                format!(
                    "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                    opfamilyname, "gin",
                    format_operator((*oprform).amopopr),
                    (*oprform).amopstrategy
                )
            );
            result = false;
        }

        // gin doesn't support ORDER BY operators.
        if (*oprform).amoppurpose != AMOP_SEARCH || OidIsValid((*oprform).amopsortfamily) {
            ereport!(
                INFO,
                format!(
                    "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                    opfamilyname, "gin",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }

        // Check operator signature --- same for all gin strategies.
        if !check_amop_signature(
            (*oprform).amopopr, BOOLOID,
            (*oprform).amoplefttype, (*oprform).amoprighttype,
        ) {
            ereport!(
                INFO,
                format!(
                    "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                    opfamilyname, "gin",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions.
    let grouplist = identify_opfamily_groups(oprlist, proclist);
    let mut opclassgroup: *const OpFamilyOpFuncGroup = core::ptr::null();
    let glen = list_length(grouplist);
    for i in 0..glen {
        let thisgroup = list_nth(grouplist, i) as *const OpFamilyOpFuncGroup;

        // Remember the group exactly matching the test opclass.
        if (*thisgroup).lefttype == opcintype && (*thisgroup).righttype == opcintype {
            opclassgroup = thisgroup;
        }

        // There is not a lot we can do to check the operator sets, since each GIN
        // opclass is more or less a law unto itself, and some contain only
        // operators that are binary-compatible with the opclass datatype (meaning
        // that empty operator sets can be OK).  That case also means that we
        // shouldn't insist on nonempty function sets except for the opclass's own
        // group.
    }

    // Check that the originally-named opclass is complete.
    for i in 1..=GINNProcs {
        if !opclassgroup.is_null()
            && ((*opclassgroup).functionset & ((1u64) << i)) != 0
        {
            continue; // got it
        }
        if i == GIN_COMPARE_PROC || i == GIN_COMPARE_PARTIAL_PROC || i == GIN_OPTIONS_PROC {
            continue; // optional method
        }
        if i == GIN_CONSISTENT_PROC || i == GIN_TRICONSISTENT_PROC {
            continue; // don't need both, see check below loop
        }
        ereport!(
            INFO,
            format!(
                "operator class \"{}\" of access method {} is missing support function {}",
                opclassname, "gin", i
            )
        );
        result = false;
    }
    if opclassgroup.is_null()
        || (((*opclassgroup).functionset & (1u64 << GIN_CONSISTENT_PROC)) == 0
            && ((*opclassgroup).functionset & (1u64 << GIN_TRICONSISTENT_PROC)) == 0)
    {
        ereport!(
            INFO,
            format!(
                "operator class \"{}\" of access method {} is missing support function {} or {}",
                opclassname, "gin", GIN_CONSISTENT_PROC, GIN_TRICONSISTENT_PROC
            )
        );
        result = false;
    }

    // C: ReleaseCatCacheList(proclist); ReleaseCatCacheList(oprlist);
    //    ReleaseSysCache(classtup); -- no-ops until syscache/catcache land.

    result
}

/// `&list->members[i]->tuple` for a borrowed CatCList.  Mirrors the (private)
/// `CatCList::member_tuple` accessor in amvalidate.rs; reproduced here because
/// that accessor is not pub.
///
/// # Safety
/// `i` must be `< list.n_members`, and member `i` must reference a valid CatCTup.
unsafe fn catclist_member_tuple(
    list: &CatCList,
    i: usize,
) -> *const crate::access::htup_details::HeapTupleData {
    // CatCList.members is a flexible array of `*mut CatCTup`; CatCTup's first
    // (and only mirrored) field is `tuple: HeapTupleData`, so the member pointer
    // doubles as a pointer to its embedded tuple.
    let base = list.members.as_ptr();
    let memb = *base.add(i);
    memb as *const crate::access::htup_details::HeapTupleData
}

// ===========================================================================
//   ginadjustmembers (REAL, ported 1:1)
// ===========================================================================

/// Prechecking function for adding operators/functions to a GIN opfamily.
///
/// # Safety
/// `operators` / `functions` must be valid Lists of `*mut OpFamilyMember`.
pub unsafe fn ginadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    // Operator members of a GIN opfamily should never have hard dependencies,
    // since their connection to the opfamily depends only on what the support
    // functions think, and that can be altered.  For consistency, we make all
    // soft dependencies point to the opfamily, though a soft dependency on the
    // opclass would work as well in the CREATE OPERATOR CLASS case.
    let nop = list_length(operators);
    for i in 0..nop {
        let op = list_nth(operators, i) as *mut OpFamilyMember;
        (*op).ref_is_hard = false;
        (*op).ref_is_family = true;
        (*op).refobjid = opfamilyoid;
    }

    // Required support functions should have hard dependencies.  Preferably those
    // are just dependencies on the opclass, but if we're in ALTER OPERATOR FAMILY,
    // we leave the dependency pointing at the whole opfamily.  (Given that GIN
    // opclasses generally don't share opfamilies, it seems unlikely to be worth
    // working harder.)
    let nfn = list_length(functions);
    for i in 0..nfn {
        let op = list_nth(functions, i) as *mut OpFamilyMember;
        match (*op).number {
            GIN_EXTRACTVALUE_PROC | GIN_EXTRACTQUERY_PROC => {
                // Required support function.
                (*op).ref_is_hard = true;
            }
            GIN_COMPARE_PROC
            | GIN_CONSISTENT_PROC
            | GIN_COMPARE_PARTIAL_PROC
            | GIN_TRICONSISTENT_PROC
            | GIN_OPTIONS_PROC => {
                // Optional, so force it to be a soft family dependency.
                (*op).ref_is_hard = false;
                (*op).ref_is_family = true;
                (*op).refobjid = opfamilyoid;
            }
            _ => {
                ereport!(
                    ERROR,
                    format!(
                        "support function number {} is invalid for access method {}",
                        (*op).number, "gin"
                    )
                );
            }
        }
    }
}

// ===========================================================================
//                                 TESTS
//
// The entrypoint `ginvalidate` immediately hits the stubbed CLAOID syscache
// fetch, so it cannot be exercised without a catalog.  We therefore test the
// REAL, self-contained pieces:
//   1. the GIN procnum constants and GINNProcs invariant,
//   2. the required-vs-optional procnum classification used by the
//      group-completeness loop (a pure restatement of the C's continue rules),
//   3. the ginadjustmembers dependency-rewrite logic over hand-built
//      OpFamilyMember lists (no catalog access -- fully real).
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::pg_list::{lappend, NIL};

    #[test]
    fn procnum_constants_and_count() {
        assert_eq!(GIN_COMPARE_PROC, 1);
        assert_eq!(GIN_EXTRACTVALUE_PROC, 2);
        assert_eq!(GIN_EXTRACTQUERY_PROC, 3);
        assert_eq!(GIN_CONSISTENT_PROC, 4);
        assert_eq!(GIN_COMPARE_PARTIAL_PROC, 5);
        assert_eq!(GIN_TRICONSISTENT_PROC, 6);
        assert_eq!(GIN_OPTIONS_PROC, 7);
        assert_eq!(GINNProcs, 7);
        // GINNProcs must name the highest defined support number.
        assert_eq!(GINNProcs, GIN_OPTIONS_PROC);
    }

    /// Restates the C "is this procnum allowed to be absent from the opclass
    /// group?" rule from the completeness loop: COMPARE / COMPARE_PARTIAL /
    /// OPTIONS are optional; CONSISTENT and TRICONSISTENT are individually
    /// skippable (handled by the post-loop "missing X or Y" check).
    fn optional_in_completeness_loop(i: c_int) -> bool {
        i == GIN_COMPARE_PROC
            || i == GIN_COMPARE_PARTIAL_PROC
            || i == GIN_OPTIONS_PROC
            || i == GIN_CONSISTENT_PROC
            || i == GIN_TRICONSISTENT_PROC
    }

    #[test]
    fn only_extract_procs_are_unconditionally_required() {
        // EXTRACTVALUE and EXTRACTQUERY are the only support numbers that the
        // completeness loop will flag if absent.
        for i in 1..=GINNProcs {
            let required = i == GIN_EXTRACTVALUE_PROC || i == GIN_EXTRACTQUERY_PROC;
            assert_eq!(!optional_in_completeness_loop(i), required, "procnum {}", i);
        }
    }

    // Build a List<*mut OpFamilyMember> from members; boxes kept alive by caller.
    fn member(number: c_int) -> Box<OpFamilyMember> {
        Box::new(OpFamilyMember {
            is_func: true,
            object: 0,
            number,
            lefttype: 0,
            righttype: 0,
            sortfamily: 0,
            ref_is_hard: false,
            ref_is_family: false,
            refobjid: 0,
        })
    }

    #[test]
    fn adjustmembers_required_funcs_get_hard_dep() {
        // EXTRACTVALUE / EXTRACTQUERY -> ref_is_hard = true (left family/refobjid
        // untouched, both start false/0).
        let mut ev = member(GIN_EXTRACTVALUE_PROC);
        let mut eq = member(GIN_EXTRACTQUERY_PROC);
        let mut funcs: *mut List = NIL;
        unsafe {
            funcs = lappend(funcs, ev.as_mut() as *mut OpFamilyMember as *mut c_void);
            funcs = lappend(funcs, eq.as_mut() as *mut OpFamilyMember as *mut c_void);
            ginadjustmembers(777, 0, NIL, funcs);
        }
        assert!(ev.ref_is_hard);
        assert!(eq.ref_is_hard);
        assert!(!ev.ref_is_family);
        assert_eq!(ev.refobjid, 0);
    }

    #[test]
    fn adjustmembers_optional_funcs_get_soft_family_dep() {
        for &num in &[
            GIN_COMPARE_PROC,
            GIN_CONSISTENT_PROC,
            GIN_COMPARE_PARTIAL_PROC,
            GIN_TRICONSISTENT_PROC,
            GIN_OPTIONS_PROC,
        ] {
            let mut m = member(num);
            // pre-load with hard/garbage to prove they get overwritten
            m.ref_is_hard = true;
            m.refobjid = 1;
            let mut funcs: *mut List = NIL;
            unsafe {
                funcs = lappend(funcs, m.as_mut() as *mut OpFamilyMember as *mut c_void);
                ginadjustmembers(999, 0, NIL, funcs);
            }
            assert!(!m.ref_is_hard, "num {}", num);
            assert!(m.ref_is_family, "num {}", num);
            assert_eq!(m.refobjid, 999, "num {}", num);
        }
    }

    #[test]
    fn adjustmembers_operators_get_soft_family_dep() {
        // Every operator member -> soft, family, refobjid = opfamily.
        let mut op = member(1);
        op.is_func = false;
        op.ref_is_hard = true;
        let mut ops: *mut List = NIL;
        unsafe {
            ops = lappend(ops, op.as_mut() as *mut OpFamilyMember as *mut c_void);
            ginadjustmembers(555, 0, ops, NIL);
        }
        assert!(!op.ref_is_hard);
        assert!(op.ref_is_family);
        assert_eq!(op.refobjid, 555);
    }
}
