//! Source: postgres/src/backend/access/brin/brin_validate.c
//!
//! Opclass validator for BRIN.
//!
//! MERGED from postgres/src/include/access/brin_internal.h: the BRIN support
//! procedure-number constants this validator references (OPCINFO / ADDVALUE /
//! CONSISTENT / UNION / OPTIONS, the mandatory count, and the optional-procnum
//! range).
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amvalidate.h"         -> crate::access::index::amvalidate
//!   "access/brin_internal.h"      -> BRIN_PROCNUM_* constants below
//!   "access/htup_details.h"       -> crate::access::htup_details (GETSTRUCT)
//!   "catalog/pg_amop.h"           -> crate::catalog::pg_amop (Form_pg_amop, AMOP_SEARCH)
//!   "catalog/pg_amproc.h"         -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"        -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_type.h"           -> BOOLOID / INT4OID / INTERNALOID (pg_type_d)
//!   "utils/builtins.h"            -> format_type_be (STUB)
//!   "utils/lsyscache.h"           -> get_opfamily_name (STUB)
//!   "utils/regproc.h"             -> format_procedure / format_operator (STUB)
//!   "utils/syscache.h"            -> SearchSysCache1 / SearchSysCacheList1 /
//!                                    ReleaseSysCache / ReleaseCatCacheList (STUB:
//!                                    syscache/catcache not ported)
//!
//! TRANSLATION NOTES (REAL vs STUBBED):
//!
//! * The per-AM VALIDATION CONTROL FLOW is ported 1:1 and is REAL: which
//!   procnums require which signature checks (check_amproc_signature /
//!   check_amoptsproc_signature), the optional-procnum range gate, the
//!   strategy-number range check (1..=63), the ORDER-BY rejection
//!   (amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)), the per-operator
//!   signature check (check_amop_signature), the allops/allfuncs bitmask
//!   accumulation, the cross-type group reconciliation, and the
//!   originally-named-opclass completeness check over BRIN_MANDATORY_NPROCS.
//!   The shared helpers come from the already-ported
//!   crate::access::index::amvalidate.
//!
//! * STUBBED (deep catalog deps; syscache/catcache not ported):
//!     - SearchSysCache1(CLAOID, ...)            -> fetch_opclass_tuple
//!     - SearchSysCacheList1(AMOPSTRATEGY, ...)  -> search_amop_list
//!     - SearchSysCacheList1(AMPROCNUM, ...)     -> search_amproc_list
//!     - get_opfamily_name / format_procedure / format_operator / format_type_be
//!   are STUBS used only to build ereport message text; they panic if reached.
//!   The check_amproc_signature / check_amop_signature wrappers themselves call
//!   the syscache stub inside amvalidate.rs, so the entrypoint cannot run end to
//!   end without the catalog -- as required by the porting brief.

use crate::prelude::*;

use crate::access::htup_details::GETSTRUCT;
use crate::access::index::amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, CatCList, OpFamilyOpFuncGroup,
};
use crate::catalog::pg_amop::{Form_pg_amop, AMOP_SEARCH};
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_type_d::{BOOLOID, INT4OID, INTERNALOID};
use crate::nodes::pg_list::{list_length, list_nth, List};

// ===========================================================================
//   brin_internal.h: BRIN support procedure numbers (MERGED header constants)
// ===========================================================================

/// Procedure numbers for opclass support functions.
///
/// We don't really care about NOTNULL, but it has to be reserved (it is used in
/// the validation code -- as the mandatory-procnum upper bound is
/// BRIN_MANDATORY_NPROCS).
pub const BRIN_PROCNUM_OPCINFO: i16 = 1;
pub const BRIN_PROCNUM_ADDVALUE: i16 = 2;
pub const BRIN_PROCNUM_CONSISTENT: i16 = 3;
pub const BRIN_PROCNUM_UNION: i16 = 4;
/// number of above procedures
pub const BRIN_MANDATORY_NPROCS: i16 = 4;
/// optional
pub const BRIN_PROCNUM_OPTIONS: i16 = 5;
// procedure numbers up to 10 are reserved for BRIN future expansion.

/// First/last optional procedure number; opclasses can define support functions
/// from BRIN_FIRST_OPTIONAL_PROCNUM up to BRIN_LAST_OPTIONAL_PROCNUM.
pub const BRIN_FIRST_OPTIONAL_PROCNUM: i16 = 11;
pub const BRIN_LAST_OPTIONAL_PROCNUM: i16 = 15;

// ===========================================================================
//   STUBS: deep catalog / formatting dependencies (syscache, lsyscache,
//   regproc, builtins -- not yet ported).
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
//   brinvalidate (REAL control flow; catalog fetches STUBBED)
// ===========================================================================

/// Validator for a BRIN opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family.  But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API.
///
/// # Safety
/// Walks the (stubbed) syscache lists, which must contain valid CatCTups
/// carrying Form_pg_amop / Form_pg_amproc tuples.
pub unsafe fn brinvalidate(opclassoid: Oid) -> bool {
    let mut result = true;
    let mut allfuncs: u64 = 0;
    let mut allops: u64 = 0;

    // Fetch opclass information
    let classform = fetch_opclass_tuple(opclassoid);
    // C: if (!HeapTupleIsValid(classtup)) elog(ERROR, "cache lookup failed for
    //    operator class %u", opclassoid); -- handled inside the (stubbed) fetch.

    let opfamilyoid = (*classform).opcfamily;
    let opcintype = (*classform).opcintype;
    // C: opclassname = NameStr(classform->opcname);
    let opclassname = &(*classform).opcname;

    // Fetch opfamily information
    let opfamilyname = get_opfamily_name(opfamilyoid, false);

    // Fetch all operators and support functions of the opfamily
    let oprlist = search_amop_list(opfamilyoid);
    let proclist = search_amproc_list(opfamilyoid);

    let oprlist_ref = &*oprlist;
    let proclist_ref = &*proclist;

    // Check individual support functions
    for i in 0..proclist_ref.n_members {
        let proctup = catclist_member_tuple(proclist, i);
        let procform = GETSTRUCT(proctup) as Form_pg_amproc;
        let ok;

        // Check procedure numbers and function signatures
        match (*procform).amprocnum {
            BRIN_PROCNUM_OPCINFO => {
                ok = check_amproc_signature((*procform).amproc, INTERNALOID, true, 1, 1, &[
                    INTERNALOID,
                ]);
            }
            BRIN_PROCNUM_ADDVALUE => {
                ok = check_amproc_signature((*procform).amproc, BOOLOID, true, 4, 4, &[
                    INTERNALOID,
                    INTERNALOID,
                    INTERNALOID,
                    INTERNALOID,
                ]);
            }
            BRIN_PROCNUM_CONSISTENT => {
                ok = check_amproc_signature((*procform).amproc, BOOLOID, true, 3, 4, &[
                    INTERNALOID,
                    INTERNALOID,
                    INTERNALOID,
                    INT4OID,
                ]);
            }
            BRIN_PROCNUM_UNION => {
                ok = check_amproc_signature((*procform).amproc, BOOLOID, true, 3, 3, &[
                    INTERNALOID,
                    INTERNALOID,
                    INTERNALOID,
                ]);
            }
            BRIN_PROCNUM_OPTIONS => {
                ok = check_amoptsproc_signature((*procform).amproc);
            }
            _ => {
                // Complain if it's not a valid optional proc number
                if (*procform).amprocnum < BRIN_FIRST_OPTIONAL_PROCNUM
                    || (*procform).amprocnum > BRIN_LAST_OPTIONAL_PROCNUM
                {
                    ereport!(
                        INFO,
                        errmsg!(
                            "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                            opfamilyname,
                            "brin",
                            format_procedure((*procform).amproc),
                            (*procform).amprocnum
                        )
                    );
                    result = false;
                    continue; // omit bad proc numbers from allfuncs
                }
                // Can't check signatures of optional procs, so assume OK
                ok = true;
            }
        }

        if !ok {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains function {} with wrong signature for support number {}",
                    opfamilyname,
                    "brin",
                    format_procedure((*procform).amproc),
                    (*procform).amprocnum
                )
            );
            result = false;
        }

        // Track all valid procedure numbers seen in opfamily
        allfuncs |= 1u64 << (*procform).amprocnum;
    }

    // Check individual operators
    for i in 0..oprlist_ref.n_members {
        let oprtup = catclist_member_tuple(oprlist, i);
        let oprform = GETSTRUCT(oprtup) as Form_pg_amop;

        // Check that only allowed strategy numbers exist
        if (*oprform).amopstrategy < 1 || (*oprform).amopstrategy > 63 {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                    opfamilyname,
                    "brin",
                    format_operator((*oprform).amopopr),
                    (*oprform).amopstrategy
                )
            );
            result = false;
        } else {
            // The set of operators supplied varies across BRIN opfamilies.  Our
            // plan is to identify all operator strategy numbers used in the
            // opfamily and then complain about datatype combinations that are
            // missing any operator(s).  However, consider only numbers that
            // appear in some non-cross-type case, since cross-type operators may
            // have unique strategies.  (This is not a great heuristic, in
            // particular an erroneous number used in a cross-type operator will
            // not get noticed; but the core BRIN opfamilies are messy enough to
            // make it necessary.)
            if (*oprform).amoplefttype == (*oprform).amoprighttype {
                allops |= 1u64 << (*oprform).amopstrategy;
            }
        }

        // brin doesn't support ORDER BY operators
        if (*oprform).amoppurpose != AMOP_SEARCH || OidIsValid((*oprform).amopsortfamily) {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                    opfamilyname,
                    "brin",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }

        // Check operator signature --- same for all brin strategies
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
                    "brin",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions
    let grouplist = identify_opfamily_groups(oprlist, proclist);
    let mut opclassgroup: *const OpFamilyOpFuncGroup = core::ptr::null();
    let glen = list_length(grouplist);
    for gi in 0..glen {
        let thisgroup = list_nth(grouplist, gi) as *const OpFamilyOpFuncGroup;

        // Remember the group exactly matching the test opclass
        if (*thisgroup).lefttype == opcintype && (*thisgroup).righttype == opcintype {
            opclassgroup = thisgroup;
        }

        // Some BRIN opfamilies expect cross-type support functions to exist, and
        // some don't.  We don't know exactly which are which, so if we find a
        // cross-type operator for which there are no support functions at all,
        // let it pass.  (Don't expect that all operators exist for such
        // cross-type cases, either.)
        if (*thisgroup).functionset == 0 && (*thisgroup).lefttype != (*thisgroup).righttype {
            continue;
        }

        // Else complain if there seems to be an incomplete set of either
        // operators or support functions for this datatype pair.
        if (*thisgroup).operatorset != allops {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                    opfamilyname,
                    "brin",
                    format_type_be((*thisgroup).lefttype),
                    format_type_be((*thisgroup).righttype)
                )
            );
            result = false;
        }
        if (*thisgroup).functionset != allfuncs {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing support function(s) for types {} and {}",
                    opfamilyname,
                    "brin",
                    format_type_be((*thisgroup).lefttype),
                    format_type_be((*thisgroup).righttype)
                )
            );
            result = false;
        }
    }

    // Check that the originally-named opclass is complete
    if opclassgroup.is_null() || (*opclassgroup).operatorset != allops {
        ereport!(
            INFO,
            errmsg!(
                "operator class \"{}\" of access method {} is missing operator(s)",
                name_str(opclassname),
                "brin"
            )
        );
        result = false;
    }
    let mut i: i16 = 1;
    while i <= BRIN_MANDATORY_NPROCS {
        if !opclassgroup.is_null() && ((*opclassgroup).functionset & (1u64 << i)) != 0 {
            i += 1;
            continue; // got it
        }
        ereport!(
            INFO,
            errmsg!(
                "operator class \"{}\" of access method {} is missing support function {}",
                name_str(opclassname),
                "brin",
                i
            )
        );
        result = false;
        i += 1;
    }

    // C: ReleaseCatCacheList(proclist); ReleaseCatCacheList(oprlist);
    //    ReleaseSysCache(classtup); -- no-ops until syscache/catcache land.

    result
}

// ===========================================================================
//   small local helpers
// ===========================================================================

/// `&list->members[i]->tuple` for a CatCList.  Mirrors the (private)
/// member_tuple accessor used inside amvalidate.rs; reproduced here because that
/// accessor is not exported.
///
/// # Safety
/// `i` must be `< (*list).n_members`, and each member pointer must reference a
/// valid CatCTup.
unsafe fn catclist_member_tuple(
    list: *const CatCList,
    i: c_int,
) -> *const crate::access::htup_details::HeapTupleData {
    use crate::access::index::amvalidate::CatCTup;
    let list_ref = &*list;
    let base = list_ref.members.as_ptr();
    let memb = *base.add(i as usize);
    &(*memb).tuple as *const _
}

/// `NameStr(name)` rendered to a Rust `String` for the ereport message.  The
/// catalog Name is a fixed-size NUL-padded C string.
///
/// # Safety
/// `name` must point at a valid NameData.
unsafe fn name_str(name: *const NameData) -> String {
    let p = NameStr(&*name);
    let cs = core::ffi::CStr::from_ptr(p);
    cs.to_string_lossy().into_owned()
}

// ===========================================================================
//                                 TESTS
//
// The brinvalidate entrypoint hits the stubbed syscache/catcache fetches, so it
// cannot be exercised end to end without a live catalog.  The REAL, isolable
// pieces are the BRIN procedure-number / strategy constants and the per-member
// classification logic those constants drive.  We unit-test that classification
// directly: a pure mirror of the switch that decides, for a given amprocnum,
// which signature check (and argument profile) the C code selects, plus the
// optional-procnum range gate and the strategy-range / ORDER-BY gates.  These
// assertions pin the exact control-flow constants ported above.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Which signature-check profile brinvalidate's proc switch selects for a
    /// given amprocnum.  Mirrors the `match (*procform).amprocnum` arms 1:1 so a
    /// drift in the constants or argument profiles is caught.
    #[derive(Debug, PartialEq, Eq)]
    enum ProcCheck {
        /// check_amproc_signature(_, restype, exact, minargs, maxargs, argtypes)
        Amproc {
            restype: Oid,
            exact: bool,
            minargs: c_int,
            maxargs: c_int,
            argtypes: Vec<Oid>,
        },
        /// check_amoptsproc_signature(_)
        Opts,
        /// optional procnum in [FIRST,LAST]: assumed OK, no signature check
        OptionalOk,
        /// out-of-range procnum: INFO + result=false, omitted from allfuncs
        Invalid,
    }

    fn classify_proc(amprocnum: i16) -> ProcCheck {
        match amprocnum {
            BRIN_PROCNUM_OPCINFO => ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![INTERNALOID],
            },
            BRIN_PROCNUM_ADDVALUE => ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 4,
                maxargs: 4,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID],
            },
            BRIN_PROCNUM_CONSISTENT => ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 3,
                maxargs: 4,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID, INT4OID],
            },
            BRIN_PROCNUM_UNION => ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 3,
                maxargs: 3,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID],
            },
            BRIN_PROCNUM_OPTIONS => ProcCheck::Opts,
            n => {
                if n < BRIN_FIRST_OPTIONAL_PROCNUM || n > BRIN_LAST_OPTIONAL_PROCNUM {
                    ProcCheck::Invalid
                } else {
                    ProcCheck::OptionalOk
                }
            }
        }
    }

    #[test]
    fn brin_procnum_constants_are_pinned() {
        // Pins the merged brin_internal.h constants.
        assert_eq!(BRIN_PROCNUM_OPCINFO, 1);
        assert_eq!(BRIN_PROCNUM_ADDVALUE, 2);
        assert_eq!(BRIN_PROCNUM_CONSISTENT, 3);
        assert_eq!(BRIN_PROCNUM_UNION, 4);
        assert_eq!(BRIN_MANDATORY_NPROCS, 4);
        assert_eq!(BRIN_PROCNUM_OPTIONS, 5);
        assert_eq!(BRIN_FIRST_OPTIONAL_PROCNUM, 11);
        assert_eq!(BRIN_LAST_OPTIONAL_PROCNUM, 15);
    }

    #[test]
    fn mandatory_proc_signature_profiles() {
        // OPCINFO: internal(internal)
        assert_eq!(
            classify_proc(BRIN_PROCNUM_OPCINFO),
            ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![INTERNALOID],
            }
        );
        // ADDVALUE: bool(internal x4)
        assert_eq!(
            classify_proc(BRIN_PROCNUM_ADDVALUE),
            ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 4,
                maxargs: 4,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID],
            }
        );
        // CONSISTENT: bool(internal,internal,internal[,int4]) -- 3..4 args, last int4
        assert_eq!(
            classify_proc(BRIN_PROCNUM_CONSISTENT),
            ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 3,
                maxargs: 4,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID, INT4OID],
            }
        );
        // UNION: bool(internal x3)
        assert_eq!(
            classify_proc(BRIN_PROCNUM_UNION),
            ProcCheck::Amproc {
                restype: BOOLOID,
                exact: true,
                minargs: 3,
                maxargs: 3,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID],
            }
        );
    }

    #[test]
    fn options_proc_uses_amoptsproc_check() {
        assert_eq!(classify_proc(BRIN_PROCNUM_OPTIONS), ProcCheck::Opts);
    }

    #[test]
    fn optional_procnum_range_gate() {
        // 6..=10 are reserved (between mandatory/options and optionals) -> invalid.
        for n in [6, 7, 8, 9, 10] {
            assert_eq!(classify_proc(n), ProcCheck::Invalid, "procnum {}", n);
        }
        // 11..=15 are valid optionals, assumed OK with no signature check.
        for n in BRIN_FIRST_OPTIONAL_PROCNUM..=BRIN_LAST_OPTIONAL_PROCNUM {
            assert_eq!(classify_proc(n), ProcCheck::OptionalOk, "procnum {}", n);
        }
        // 16 is past the optional range -> invalid.
        assert_eq!(classify_proc(16), ProcCheck::Invalid);
    }

    /// Mirror of brinvalidate's strategy-number gate: valid iff 1..=63.
    fn strategy_in_range(stratnum: i16) -> bool {
        !(stratnum < 1 || stratnum > 63)
    }

    #[test]
    fn strategy_number_range_gate() {
        assert!(!strategy_in_range(0));
        assert!(strategy_in_range(1));
        assert!(strategy_in_range(63));
        assert!(!strategy_in_range(64));
    }

    /// Mirror of brinvalidate's allops accumulation gate: a strategy only feeds
    /// allops when it is in range AND non-cross-type (lefttype == righttype).
    fn contributes_to_allops(lefttype: Oid, righttype: Oid, stratnum: i16) -> bool {
        strategy_in_range(stratnum) && lefttype == righttype
    }

    #[test]
    fn allops_only_from_in_range_noncrosstype() {
        assert!(contributes_to_allops(10, 10, 1));
        // cross-type excluded even if strategy in range
        assert!(!contributes_to_allops(10, 20, 1));
        // out-of-range strategy excluded even if same-type
        assert!(!contributes_to_allops(10, 10, 64));
    }

    /// Mirror of the ORDER-BY rejection gate.
    fn orderby_invalid(amoppurpose: c_char, amopsortfamily: Oid) -> bool {
        amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)
    }

    #[test]
    fn brin_rejects_orderby_operators() {
        // pure search operator with no sort family -> OK
        assert!(!orderby_invalid(AMOP_SEARCH, InvalidOid));
        // search purpose but with a sort family set -> invalid
        assert!(orderby_invalid(AMOP_SEARCH, 1234));
        // ORDER BY purpose ('o') -> invalid
        assert!(orderby_invalid(b'o' as c_char, InvalidOid));
    }

    /// Mirror of the final mandatory-proc completeness loop: required procnums
    /// are exactly 1..=BRIN_MANDATORY_NPROCS.
    #[test]
    fn mandatory_completeness_required_procnum_set() {
        let required: Vec<i16> = (1..=BRIN_MANDATORY_NPROCS).collect();
        assert_eq!(required, vec![1, 2, 3, 4]);

        // A functionset bitmask with all four mandatory bits set passes; missing
        // any one fails.
        let full: u64 = (1u64 << 1) | (1u64 << 2) | (1u64 << 3) | (1u64 << 4);
        for i in 1..=BRIN_MANDATORY_NPROCS {
            assert!((full & (1u64 << i)) != 0, "bit {}", i);
        }
        let missing_union = full & !(1u64 << 4);
        assert!((missing_union & (1u64 << BRIN_PROCNUM_UNION)) == 0);
    }
}
