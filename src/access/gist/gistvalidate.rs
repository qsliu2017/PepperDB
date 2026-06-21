//! gistvalidate.c - Opclass validator for GiST.
//!
//! MERGED from postgres/src/include/access/gist.h: the GiST support
//! procedure-number constants this validator references (GIST_CONSISTENT_PROC
//! .. GIST_TRANSLATE_CMPTYPE_PROC, and the total GISTNProcs).
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amvalidate.h"         -> crate::access::index::amvalidate
//!   "access/gist_private.h"       -> GIST_*_PROC / GISTNProcs constants below
//!   "access/htup_details.h"       -> crate::access::htup_details (GETSTRUCT)
//!   "catalog/pg_amop.h"           -> crate::catalog::pg_amop (Form_pg_amop, AMOP_SEARCH)
//!   "catalog/pg_amproc.h"         -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"        -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_type.h"           -> *OID type constants (pg_type_d)
//!   "utils/lsyscache.h"           -> get_opfamily_name / get_opfamily_proc /
//!                                    get_op_rettype / opfamily_can_sort_type
//!   "utils/regproc.h"             -> format_procedure / format_operator (STUB)
//!   "utils/syscache.h"            -> SearchSysCache1 / SearchSysCacheList1 (STUB)
//!
//! TRANSLATION NOTES (REAL vs STUBBED):
//!
//! * REAL (ported 1:1): the entire gistvalidate control flow -- the per-procnum
//!   signature-check dispatch (check_amproc_signature / check_amoptsproc_signature),
//!   the GIST_TRANSLATE_CMPTYPE_PROC extra ANYOID left/right gate, the
//!   left==right-type check on support functions, the opcintype gating
//!   (`continue` when amproclefttype != opcintype), the strategy-number check,
//!   the ORDER-BY branch (matching distance proc + opfamily_can_sort_type on the
//!   operator result type), the per-operator signature check, the cross-type
//!   group reconciliation (only remembering the opcintype/opcintype group), and
//!   the originally-named-opclass completeness check over GISTNProcs with the
//!   optional-method skip list.  opfamily_can_sort_type comes from the
//!   already-ported crate::access::index::amvalidate.  gistadjustmembers is also
//!   ported 1:1 (required vs optional support-function dependency wiring).
//!
//! * STUBBED (deep catalog deps; syscache/catcache/lsyscache/regproc not ported):
//!     - SearchSysCache1(CLAOID, ...)            -> fetch_opclass_tuple
//!     - SearchSysCacheList1(AMOPSTRATEGY, ...)  -> search_amop_list
//!     - SearchSysCacheList1(AMPROCNUM, ...)     -> search_amproc_list
//!     - get_opfamily_name / get_opfamily_proc / get_op_rettype /
//!       format_procedure / format_operator
//!   are STUBS (panic if reached).  check_amproc_signature / check_amop_signature
//!   themselves call the syscache stub inside amvalidate.rs, so the entrypoint
//!   cannot run end to end without the catalog -- as required by the brief.

use crate::prelude::*;

use crate::access::htup_details::GETSTRUCT;
use crate::access::index::amapi::OpFamilyMember;
use crate::access::index::amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opfamily_can_sort_type, CatCList, CatCTup, OpFamilyOpFuncGroup,
};
use crate::catalog::pg_amop::{Form_pg_amop, AMOP_SEARCH};
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_type_d::{
    ANYOID, BOOLOID, FLOAT8OID, INT2OID, INT4OID, INTERNALOID, OIDOID, VOIDOID,
};
use crate::nodes::pg_list::{list_length, list_nth, List};

// ===========================================================================
//   gist.h: GiST support procedure numbers (MERGED header constants)
// ===========================================================================

pub const GIST_CONSISTENT_PROC: i16 = 1;
pub const GIST_UNION_PROC: i16 = 2;
pub const GIST_COMPRESS_PROC: i16 = 3;
pub const GIST_DECOMPRESS_PROC: i16 = 4;
pub const GIST_PENALTY_PROC: i16 = 5;
pub const GIST_PICKSPLIT_PROC: i16 = 6;
pub const GIST_EQUAL_PROC: i16 = 7;
pub const GIST_DISTANCE_PROC: i16 = 8;
pub const GIST_FETCH_PROC: i16 = 9;
pub const GIST_OPTIONS_PROC: i16 = 10;
pub const GIST_SORTSUPPORT_PROC: i16 = 11;
pub const GIST_TRANSLATE_CMPTYPE_PROC: i16 = 12;
/// total number of support functions
pub const GISTNProcs: i16 = 12;

// ===========================================================================
//   STUBS: deep catalog / formatting dependencies (syscache, lsyscache,
//   regproc -- not yet ported).
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

/// STUB for `get_opfamily_proc(opfamily, lefttype, righttype, procnum)`
/// (utils/lsyscache.c).
fn get_opfamily_proc(_opfamily: Oid, _lefttype: Oid, _righttype: Oid, _procnum: i16) -> Oid {
    unimplemented!("STUB: get_opfamily_proc (utils/lsyscache.c not ported)")
}

/// STUB for `get_op_rettype(opno)` (utils/lsyscache.c).
fn get_op_rettype(_opno: Oid) -> Oid { unimplemented!() }

/// STUB for `format_procedure(procoid)` (utils/regproc.c) - message text only.
fn format_procedure(_procoid: Oid) -> String {
    unimplemented!("STUB: format_procedure (utils/regproc.c not ported)")
}

/// STUB for `format_operator(operoid)` (utils/regproc.c) - message text only.
fn format_operator(_operoid: Oid) -> String {
    unimplemented!("STUB: format_operator (utils/regproc.c not ported)")
}

// ===========================================================================
//   gistvalidate (REAL control flow; catalog fetches STUBBED)
// ===========================================================================

/// Validator for a GiST opclass.
///
/// # Safety
/// Walks the (stubbed) syscache lists, which must contain valid CatCTups
/// carrying Form_pg_amop / Form_pg_amproc tuples.
pub unsafe fn gistvalidate(opclassoid: Oid) -> bool {
    let mut result = true;

    // Fetch opclass information
    let classform = fetch_opclass_tuple(opclassoid);
    // C: if (!HeapTupleIsValid(classtup)) elog(ERROR, "cache lookup failed for
    //    operator class %u", opclassoid); -- handled inside the (stubbed) fetch.

    let opfamilyoid = (*classform).opcfamily;
    let opcintype = (*classform).opcintype;
    let mut opckeytype = (*classform).opckeytype;
    if !OidIsValid(opckeytype) {
        opckeytype = opcintype;
    }
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
        let proctup = catclist_member_tuple(proclist, i);
        let procform = GETSTRUCT(proctup) as Form_pg_amproc;
        let ok;

        // All GiST support functions should be registered with matching
        // left/right types
        if (*procform).amproclefttype != (*procform).amprocrighttype {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                    opfamilyname,
                    "gist",
                    format_procedure((*procform).amproc)
                )
            );
            result = false;
        }

        // We can't check signatures except within the specific opclass, since
        // we need to know the associated opckeytype in many cases.
        if (*procform).amproclefttype != opcintype {
            continue;
        }

        // Check procedure numbers and function signatures
        match (*procform).amprocnum {
            GIST_CONSISTENT_PROC => {
                ok = check_amproc_signature((*procform).amproc, BOOLOID, false, 5, 5, &[
                    INTERNALOID,
                    opcintype,
                    INT2OID,
                    OIDOID,
                    INTERNALOID,
                ]);
            }
            GIST_UNION_PROC => {
                ok = check_amproc_signature((*procform).amproc, opckeytype, false, 2, 2, &[
                    INTERNALOID,
                    INTERNALOID,
                ]);
            }
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_FETCH_PROC => {
                ok = check_amproc_signature((*procform).amproc, INTERNALOID, true, 1, 1, &[
                    INTERNALOID,
                ]);
            }
            GIST_PENALTY_PROC => {
                ok = check_amproc_signature((*procform).amproc, INTERNALOID, true, 3, 3, &[
                    INTERNALOID,
                    INTERNALOID,
                    INTERNALOID,
                ]);
            }
            GIST_PICKSPLIT_PROC => {
                ok = check_amproc_signature((*procform).amproc, INTERNALOID, true, 2, 2, &[
                    INTERNALOID,
                    INTERNALOID,
                ]);
            }
            GIST_EQUAL_PROC => {
                ok = check_amproc_signature((*procform).amproc, INTERNALOID, false, 3, 3, &[
                    opckeytype,
                    opckeytype,
                    INTERNALOID,
                ]);
            }
            GIST_DISTANCE_PROC => {
                ok = check_amproc_signature((*procform).amproc, FLOAT8OID, false, 5, 5, &[
                    INTERNALOID,
                    opcintype,
                    INT2OID,
                    OIDOID,
                    INTERNALOID,
                ]);
            }
            GIST_OPTIONS_PROC => {
                ok = check_amoptsproc_signature((*procform).amproc);
            }
            GIST_SORTSUPPORT_PROC => {
                ok = check_amproc_signature((*procform).amproc, VOIDOID, true, 1, 1, &[
                    INTERNALOID,
                ]);
            }
            GIST_TRANSLATE_CMPTYPE_PROC => {
                ok = check_amproc_signature((*procform).amproc, INT2OID, true, 1, 1, &[INT4OID])
                    && (*procform).amproclefttype == ANYOID
                    && (*procform).amprocrighttype == ANYOID;
            }
            _ => {
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                        opfamilyname,
                        "gist",
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
                    "gist",
                    format_procedure((*procform).amproc),
                    (*procform).amprocnum
                )
            );
            result = false;
        }
    }

    // Check individual operators
    for i in 0..oprlist_ref.n_members {
        let oprtup = catclist_member_tuple(oprlist, i);
        let oprform = GETSTRUCT(oprtup) as Form_pg_amop;
        let op_rettype;

        // TODO: Check that only allowed strategy numbers exist
        if (*oprform).amopstrategy < 1 {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                    opfamilyname,
                    "gist",
                    format_operator((*oprform).amopopr),
                    (*oprform).amopstrategy
                )
            );
            result = false;
        }

        // GiST supports ORDER BY operators
        if (*oprform).amoppurpose != AMOP_SEARCH {
            // ... but must have matching distance proc
            if !OidIsValid(get_opfamily_proc(
                opfamilyoid,
                (*oprform).amoplefttype,
                (*oprform).amoplefttype,
                GIST_DISTANCE_PROC,
            )) {
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains unsupported ORDER BY specification for operator {}",
                        opfamilyname,
                        "gist",
                        format_operator((*oprform).amopopr)
                    )
                );
                result = false;
            }
            // ... and operator result must match the claimed btree opfamily
            op_rettype = get_op_rettype((*oprform).amopopr);
            if !opfamily_can_sort_type((*oprform).amopsortfamily, op_rettype) {
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains incorrect ORDER BY opfamily specification for operator {}",
                        opfamilyname,
                        "gist",
                        format_operator((*oprform).amopopr)
                    )
                );
                result = false;
            }
        } else {
            // Search operators must always return bool
            op_rettype = BOOLOID;
        }

        // Check operator signature
        if !check_amop_signature(
            (*oprform).amopopr,
            op_rettype,
            (*oprform).amoplefttype,
            (*oprform).amoprighttype,
        ) {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                    opfamilyname,
                    "gist",
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

        // There is not a lot we can do to check the operator sets, since each
        // GiST opclass is more or less a law unto itself, and some contain only
        // operators that are binary-compatible with the opclass datatype
        // (meaning that empty operator sets can be OK).  That case also means
        // that we shouldn't insist on nonempty function sets except for the
        // opclass's own group.
    }

    // Check that the originally-named opclass is complete
    let mut i: i16 = 1;
    while i <= GISTNProcs {
        if !opclassgroup.is_null() && ((*opclassgroup).functionset & ((1u64) << i)) != 0 {
            i += 1;
            continue; // got it
        }
        if i == GIST_DISTANCE_PROC
            || i == GIST_FETCH_PROC
            || i == GIST_COMPRESS_PROC
            || i == GIST_DECOMPRESS_PROC
            || i == GIST_OPTIONS_PROC
            || i == GIST_SORTSUPPORT_PROC
            || i == GIST_TRANSLATE_CMPTYPE_PROC
        {
            i += 1;
            continue; // optional methods
        }
        ereport!(
            INFO,
            errmsg!(
                "operator class \"{}\" of access method {} is missing support function {}",
                opclassname,
                "gist",
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
//   gistadjustmembers (REAL)
// ===========================================================================

/// Prechecking function for adding operators/functions to a GiST opfamily.
///
/// # Safety
/// `operators` and `functions` must be valid Lists of `*mut OpFamilyMember`.
pub unsafe fn gistadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    // Operator members of a GiST opfamily should never have hard dependencies,
    // since their connection to the opfamily depends only on what the support
    // functions think, and that can be altered.  For consistency, we make all
    // soft dependencies point to the opfamily, though a soft dependency on the
    // opclass would work as well in the CREATE OPERATOR CLASS case.
    let nops = list_length(operators);
    for li in 0..nops {
        let op = list_nth(operators, li) as *mut OpFamilyMember;
        (*op).ref_is_hard = false;
        (*op).ref_is_family = true;
        (*op).refobjid = opfamilyoid;
    }

    // Required support functions should have hard dependencies.  Preferably
    // those are just dependencies on the opclass, but if we're in ALTER OPERATOR
    // FAMILY, we leave the dependency pointing at the whole opfamily.  (Given
    // that GiST opclasses generally don't share opfamilies, it seems unlikely to
    // be worth working harder.)
    let nfuncs = list_length(functions);
    for li in 0..nfuncs {
        let op = list_nth(functions, li) as *mut OpFamilyMember;

        match (*op).number as i16 {
            GIST_CONSISTENT_PROC | GIST_UNION_PROC | GIST_PENALTY_PROC | GIST_PICKSPLIT_PROC
            | GIST_EQUAL_PROC => {
                // Required support function
                (*op).ref_is_hard = true;
            }
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_DISTANCE_PROC | GIST_FETCH_PROC
            | GIST_OPTIONS_PROC | GIST_SORTSUPPORT_PROC | GIST_TRANSLATE_CMPTYPE_PROC => {
                // Optional, so force it to be a soft family dependency
                (*op).ref_is_hard = false;
                (*op).ref_is_family = true;
                (*op).refobjid = opfamilyoid;
            }
            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "support function number {} is invalid for access method {}",
                        (*op).number,
                        "gist"
                    )
                );
            }
        }
    }
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
    let list_ref = &*list;
    let base = list_ref.members.as_ptr();
    let memb = *base.add(i as usize) as *const CatCTup;
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
// The gistvalidate entrypoint hits the stubbed syscache/catcache fetches, so it
// cannot be exercised end to end without a live catalog.  The REAL, isolable
// pieces are the GiST procedure-number constants and the per-member
// classification logic those constants drive.  We unit-test that classification
// directly: pure mirrors of (a) the support-function signature-check dispatch,
// (b) the originally-named-opclass optional-method skip list, and
// (c) gistadjustmembers' required-vs-optional dependency classification.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gist_procnum_constants_are_pinned() {
        assert_eq!(GIST_CONSISTENT_PROC, 1);
        assert_eq!(GIST_UNION_PROC, 2);
        assert_eq!(GIST_COMPRESS_PROC, 3);
        assert_eq!(GIST_DECOMPRESS_PROC, 4);
        assert_eq!(GIST_PENALTY_PROC, 5);
        assert_eq!(GIST_PICKSPLIT_PROC, 6);
        assert_eq!(GIST_EQUAL_PROC, 7);
        assert_eq!(GIST_DISTANCE_PROC, 8);
        assert_eq!(GIST_FETCH_PROC, 9);
        assert_eq!(GIST_OPTIONS_PROC, 10);
        assert_eq!(GIST_SORTSUPPORT_PROC, 11);
        assert_eq!(GIST_TRANSLATE_CMPTYPE_PROC, 12);
        assert_eq!(GISTNProcs, 12);
    }

    /// Mirror of the support-function signature-check dispatch (which check &
    /// argument profile gistvalidate selects for a given amprocnum).
    #[derive(Debug, PartialEq, Eq)]
    enum ProcCheck {
        Amproc {
            restype: Oid,
            exact: bool,
            minargs: c_int,
            maxargs: c_int,
            argtypes: Vec<Oid>,
        },
        /// GIST_TRANSLATE_CMPTYPE_PROC: amproc check AND lefttype==righttype==ANYOID
        TranslateCmptype,
        Opts,
        Invalid,
    }

    fn classify_proc(amprocnum: i16, opcintype: Oid, opckeytype: Oid) -> ProcCheck {
        match amprocnum {
            GIST_CONSISTENT_PROC => ProcCheck::Amproc {
                restype: BOOLOID,
                exact: false,
                minargs: 5,
                maxargs: 5,
                argtypes: vec![INTERNALOID, opcintype, INT2OID, OIDOID, INTERNALOID],
            },
            GIST_UNION_PROC => ProcCheck::Amproc {
                restype: opckeytype,
                exact: false,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![INTERNALOID, INTERNALOID],
            },
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_FETCH_PROC => ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![INTERNALOID],
            },
            GIST_PENALTY_PROC => ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: true,
                minargs: 3,
                maxargs: 3,
                argtypes: vec![INTERNALOID, INTERNALOID, INTERNALOID],
            },
            GIST_PICKSPLIT_PROC => ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: true,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![INTERNALOID, INTERNALOID],
            },
            GIST_EQUAL_PROC => ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: false,
                minargs: 3,
                maxargs: 3,
                argtypes: vec![opckeytype, opckeytype, INTERNALOID],
            },
            GIST_DISTANCE_PROC => ProcCheck::Amproc {
                restype: FLOAT8OID,
                exact: false,
                minargs: 5,
                maxargs: 5,
                argtypes: vec![INTERNALOID, opcintype, INT2OID, OIDOID, INTERNALOID],
            },
            GIST_OPTIONS_PROC => ProcCheck::Opts,
            GIST_SORTSUPPORT_PROC => ProcCheck::Amproc {
                restype: VOIDOID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![INTERNALOID],
            },
            GIST_TRANSLATE_CMPTYPE_PROC => ProcCheck::TranslateCmptype,
            _ => ProcCheck::Invalid,
        }
    }

    #[test]
    fn support_signature_profiles() {
        let (intype, key) = (16 as Oid, 17 as Oid);
        assert_eq!(
            classify_proc(GIST_CONSISTENT_PROC, intype, key),
            ProcCheck::Amproc {
                restype: BOOLOID,
                exact: false,
                minargs: 5,
                maxargs: 5,
                argtypes: vec![INTERNALOID, intype, INT2OID, OIDOID, INTERNALOID],
            }
        );
        // UNION result type tracks opckeytype.
        assert_eq!(
            classify_proc(GIST_UNION_PROC, intype, key),
            ProcCheck::Amproc {
                restype: key,
                exact: false,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![INTERNALOID, INTERNALOID],
            }
        );
        // EQUAL args track opckeytype.
        assert_eq!(
            classify_proc(GIST_EQUAL_PROC, intype, key),
            ProcCheck::Amproc {
                restype: INTERNALOID,
                exact: false,
                minargs: 3,
                maxargs: 3,
                argtypes: vec![key, key, INTERNALOID],
            }
        );
        assert_eq!(classify_proc(GIST_OPTIONS_PROC, intype, key), ProcCheck::Opts);
        assert_eq!(
            classify_proc(GIST_TRANSLATE_CMPTYPE_PROC, intype, key),
            ProcCheck::TranslateCmptype
        );
        // procnum past GISTNProcs -> invalid
        assert_eq!(classify_proc(13, intype, key), ProcCheck::Invalid);
        assert_eq!(classify_proc(0, intype, key), ProcCheck::Invalid);
    }

    /// Mirror of the originally-named-opclass completeness skip list: these
    /// procnums are optional and never reported missing.
    fn is_optional_method(i: i16) -> bool {
        i == GIST_DISTANCE_PROC
            || i == GIST_FETCH_PROC
            || i == GIST_COMPRESS_PROC
            || i == GIST_DECOMPRESS_PROC
            || i == GIST_OPTIONS_PROC
            || i == GIST_SORTSUPPORT_PROC
            || i == GIST_TRANSLATE_CMPTYPE_PROC
    }

    #[test]
    fn opclass_completeness_optional_skip_list() {
        // Required (reported missing): consistent, union, penalty, picksplit, equal.
        for i in [
            GIST_CONSISTENT_PROC,
            GIST_UNION_PROC,
            GIST_PENALTY_PROC,
            GIST_PICKSPLIT_PROC,
            GIST_EQUAL_PROC,
        ] {
            assert!(!is_optional_method(i), "procnum {} should be required", i);
        }
        // Optional (skipped): compress, decompress, distance, fetch, options,
        // sortsupport, translate_cmptype.
        for i in [
            GIST_COMPRESS_PROC,
            GIST_DECOMPRESS_PROC,
            GIST_DISTANCE_PROC,
            GIST_FETCH_PROC,
            GIST_OPTIONS_PROC,
            GIST_SORTSUPPORT_PROC,
            GIST_TRANSLATE_CMPTYPE_PROC,
        ] {
            assert!(is_optional_method(i), "procnum {} should be optional", i);
        }
    }

    /// Mirror of gistadjustmembers' function classification.
    #[derive(Debug, PartialEq, Eq)]
    enum DepKind {
        Hard,
        SoftFamily,
        Error,
    }

    fn classify_func_dep(number: i16) -> DepKind {
        match number {
            GIST_CONSISTENT_PROC | GIST_UNION_PROC | GIST_PENALTY_PROC | GIST_PICKSPLIT_PROC
            | GIST_EQUAL_PROC => DepKind::Hard,
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_DISTANCE_PROC | GIST_FETCH_PROC
            | GIST_OPTIONS_PROC | GIST_SORTSUPPORT_PROC | GIST_TRANSLATE_CMPTYPE_PROC => {
                DepKind::SoftFamily
            }
            _ => DepKind::Error,
        }
    }

    #[test]
    fn adjustmembers_dependency_classification() {
        assert_eq!(classify_func_dep(GIST_CONSISTENT_PROC), DepKind::Hard);
        assert_eq!(classify_func_dep(GIST_EQUAL_PROC), DepKind::Hard);
        assert_eq!(classify_func_dep(GIST_COMPRESS_PROC), DepKind::SoftFamily);
        assert_eq!(
            classify_func_dep(GIST_TRANSLATE_CMPTYPE_PROC),
            DepKind::SoftFamily
        );
        assert_eq!(classify_func_dep(0), DepKind::Error);
        assert_eq!(classify_func_dep(13), DepKind::Error);
    }
}
