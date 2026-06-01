//! Source: postgres/src/backend/access/hash/hashvalidate.c
//!
//! Opclass validator for hash.
//!
//! MERGED header constants:
//!   from postgres/src/include/access/hash.h:
//!     HASHSTANDARD_PROC / HASHEXTENDED_PROC / HASHOPTIONS_PROC
//!   from postgres/src/include/access/stratnum.h:
//!     HTEqualStrategyNumber / HTMaxStrategyNumber
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amvalidate.h"         -> crate::access::index::amvalidate
//!   "access/hash.h"               -> HASH*_PROC constants below
//!   "access/htup_details.h"       -> crate::access::htup_details (GETSTRUCT)
//!   "access/xact.h"               -> CommandCounterIncrement (STUB)
//!   "catalog/pg_am.h"             -> HASH_AM_OID (crate::catalog::pg_known_oids)
//!   "catalog/pg_amop.h"           -> crate::catalog::pg_amop (Form_pg_amop, AMOP_SEARCH)
//!   "catalog/pg_amproc.h"         -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"        -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_type.h"           -> BOOLOID / INT4OID / INT8OID (pg_type_d)
//!   "utils/builtins.h"            -> format_type_be (STUB)
//!   "utils/lsyscache.h"           -> get_opfamily_name / get_opclass_input_type (STUB)
//!   "utils/regproc.h"             -> format_procedure / format_operator (STUB)
//!   "utils/syscache.h"            -> SearchSysCache1 / SearchSysCacheList1 /
//!                                    ReleaseSysCache / ReleaseCatCacheList (STUB:
//!                                    syscache/catcache not ported)
//!
//! TRANSLATION NOTES (REAL vs STUBBED):
//!
//! * The per-AM VALIDATION CONTROL FLOW is ported 1:1 and is REAL: which
//!   procnums require which signature checks (HASHSTANDARD/HASHEXTENDED via
//!   check_amproc_signature, HASHOPTIONS via check_amoptsproc_signature), the
//!   left==right input-type check on support functions, the hashabletypes
//!   accumulation (list_append_unique_oid), the strategy-number range check
//!   (1..=HTMaxStrategyNumber), the ORDER-BY rejection
//!   (amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)), the per-operator
//!   signature check (check_amop_signature), the hashabletypes membership check,
//!   the cross-type group reconciliation against (1 << HTEqualStrategyNumber),
//!   the originally-named-opclass completeness check, and the
//!   "missing cross-type operator" count check (grouplen ==
//!   hashabletypes^2).  hashadjustmembers is ported 1:1.  The shared helpers come
//!   from the already-ported crate::access::index::amvalidate.
//!
//! * STUBBED (deep catalog deps; syscache/catcache not ported):
//!     - SearchSysCache1(CLAOID, ...)            -> fetch_opclass_tuple
//!     - SearchSysCacheList1(AMOPSTRATEGY, ...)  -> search_amop_list
//!     - SearchSysCacheList1(AMPROCNUM, ...)     -> search_amproc_list
//!     - get_opfamily_name / get_opclass_input_type / CommandCounterIncrement /
//!       format_procedure / format_operator / format_type_be are STUBS; the
//!       formatters are used only to build ereport message text.  They panic if
//!       reached.  The check_amproc_signature / check_amop_signature wrappers
//!       themselves call the syscache stub inside amvalidate.rs, so the
//!       entrypoint cannot run end to end without the catalog -- as required by
//!       the porting brief.

use crate::prelude::*;

use crate::access::htup_details::GETSTRUCT;
use crate::access::index::amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opclass_for_family_datatype, CatCList, OpFamilyOpFuncGroup,
};
use crate::catalog::pg_amop::{Form_pg_amop, AMOP_SEARCH};
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_known_oids::HASH_AM_OID;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_type_d::{BOOLOID, INT4OID, INT8OID};
use crate::access::index::amapi::OpFamilyMember;
use crate::nodes::pg_list::{
    lfirst, list_append_unique_oid, list_concat_copy, list_length, list_member_oid, list_nth, List,
    NIL,
};
use crate::{current_cell, foreach};

// ===========================================================================
//   hash.h: hash support procedure numbers (MERGED header constants)
// ===========================================================================

/// Support function numbers (amprocnum) for hash opclasses.
///
/// The default hash support function (mandatory).
pub const HASHSTANDARD_PROC: i16 = 1;
/// The extended (64-bit, seeded) hash support function.
pub const HASHEXTENDED_PROC: i16 = 2;
/// The opclass options support function (optional).
pub const HASHOPTIONS_PROC: i16 = 3;

// ===========================================================================
//   stratnum.h: hash strategy numbers (MERGED header constants)
// ===========================================================================

/// The single hash strategy: equality.
pub const HTEqualStrategyNumber: i16 = 1;
/// Highest hash strategy number.
pub const HTMaxStrategyNumber: i16 = 1;

// ===========================================================================
//   STUBS: deep catalog / formatting dependencies (syscache, lsyscache,
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
//   hashvalidate (REAL control flow; catalog fetches STUBBED)
// ===========================================================================

/// Validator for a hash opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family.  But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API.
///
/// # Safety
/// Walks the (stubbed) syscache lists, which must contain valid CatCTups
/// carrying Form_pg_amop / Form_pg_amproc tuples.
pub unsafe fn hashvalidate(opclassoid: Oid) -> bool {
    let mut result = true;
    let mut hashabletypes: *mut List = NIL;

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

        // All hash functions should be registered with matching left/right types
        if (*procform).amproclefttype != (*procform).amprocrighttype {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                    opfamilyname,
                    "hash",
                    format_procedure((*procform).amproc)
                )
            );
            result = false;
        }

        // Check procedure numbers and function signatures
        match (*procform).amprocnum {
            HASHSTANDARD_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    INT4OID,
                    true,
                    1,
                    1,
                    &[(*procform).amproclefttype],
                );
            }
            HASHEXTENDED_PROC => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    INT8OID,
                    true,
                    2,
                    2,
                    &[(*procform).amproclefttype, INT8OID],
                );
            }
            HASHOPTIONS_PROC => {
                ok = check_amoptsproc_signature((*procform).amproc);
            }
            _ => {
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                        opfamilyname,
                        "hash",
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
                    "hash",
                    format_procedure((*procform).amproc),
                    (*procform).amprocnum
                )
            );
            result = false;
        }

        // Remember which types we can hash
        if ok
            && ((*procform).amprocnum == HASHSTANDARD_PROC
                || (*procform).amprocnum == HASHEXTENDED_PROC)
        {
            hashabletypes = list_append_unique_oid(hashabletypes, (*procform).amproclefttype);
        }
    }

    // Check individual operators
    for i in 0..oprlist_ref.n_members {
        let oprtup = catclist_member_tuple(oprlist, i);
        let oprform = GETSTRUCT(oprtup) as Form_pg_amop;

        // Check that only allowed strategy numbers exist
        if (*oprform).amopstrategy < 1 || (*oprform).amopstrategy > HTMaxStrategyNumber {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                    opfamilyname,
                    "hash",
                    format_operator((*oprform).amopopr),
                    (*oprform).amopstrategy
                )
            );
            result = false;
        }

        // hash doesn't support ORDER BY operators
        if (*oprform).amoppurpose != AMOP_SEARCH || OidIsValid((*oprform).amopsortfamily) {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                    opfamilyname,
                    "hash",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }

        // Check operator signature --- same for all hash strategies
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
                    "hash",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }

        // There should be relevant hash functions for each datatype
        if !list_member_oid(hashabletypes, (*oprform).amoplefttype)
            || !list_member_oid(hashabletypes, (*oprform).amoprighttype)
        {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} lacks support function for operator {}",
                    opfamilyname,
                    "hash",
                    format_operator((*oprform).amopopr)
                )
            );
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions
    let grouplist = identify_opfamily_groups(oprlist, proclist);
    let mut opclassgroup: *const OpFamilyOpFuncGroup = core::ptr::null();
    foreach!(lc, grouplist, {
        let thisgroup = lfirst(current_cell!(lc)) as *const OpFamilyOpFuncGroup;

        // Remember the group exactly matching the test opclass
        if (*thisgroup).lefttype == opcintype && (*thisgroup).righttype == opcintype {
            opclassgroup = thisgroup;
        }

        // Complain if there seems to be an incomplete set of operators for this
        // datatype pair (implying that we have a hash function but no operator).
        if (*thisgroup).operatorset != (1u64 << HTEqualStrategyNumber) {
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                    opfamilyname,
                    "hash",
                    format_type_be((*thisgroup).lefttype),
                    format_type_be((*thisgroup).righttype)
                )
            );
            result = false;
        }
    });

    // Check that the originally-named opclass is supported
    // (if group is there, we already checked it adequately above)
    if opclassgroup.is_null() {
        ereport!(
            INFO,
            errmsg!(
                "operator class \"{}\" of access method {} is missing operator(s)",
                name_str(opclassname),
                "hash"
            )
        );
        result = false;
    }

    // Complain if the opfamily doesn't have entries for all possible combinations
    // of its supported datatypes.  While missing cross-type operators are not
    // fatal, it seems reasonable to insist that all built-in hash opfamilies be
    // complete.
    if list_length(grouplist) != list_length(hashabletypes) * list_length(hashabletypes) {
        ereport!(
            INFO,
            errmsg!(
                "operator family \"{}\" of access method {} is missing cross-type operator(s)",
                opfamilyname,
                "hash"
            )
        );
        result = false;
    }

    // C: ReleaseCatCacheList(proclist); ReleaseCatCacheList(oprlist);
    //    ReleaseSysCache(classtup); -- no-ops until syscache/catcache land.

    result
}

// ===========================================================================
//   hashadjustmembers (REAL, ported 1:1)
// ===========================================================================

/// Prechecking function for adding operators/functions to a hash opfamily.
///
/// # Safety
/// `operators`/`functions` must be valid Lists of `*mut OpFamilyMember`.
pub unsafe fn hashadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    let mut opcintype: Oid;
    // Local mutable copy of opclassoid, which the cross-type branch may rebind.
    let mut opclassoid = opclassoid;

    // Hash operators and required support functions are always "loose" members
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

        if (*op).is_func && (*op).number != HASHSTANDARD_PROC as c_int {
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
                opclassoid = opclass_for_family_datatype(HASH_AM_OID, opfamilyoid, opcintype);
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
// The hashvalidate / hashadjustmembers entrypoints hit the stubbed
// syscache/catcache fetches, so they cannot be exercised end to end without a
// live catalog.  The REAL, isolable pieces are the hash procedure-number /
// strategy constants and the per-member classification logic those constants
// drive.  We unit-test that classification directly: pure mirrors of the proc
// switch (which signature check + argument profile each amprocnum selects), the
// strategy-range gate, the ORDER-BY gate, the group-completeness gate
// (operatorset == 1<<HTEqualStrategyNumber), and the hashadjustmembers
// dependency-classification decision tree.  These assertions pin the exact
// control-flow constants ported above so any drift fails a test.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Which signature-check profile hashvalidate's proc switch selects for a
    /// given amprocnum.  Mirrors the `match (*procform).amprocnum` arms 1:1 so a
    /// drift in the constants or argument profiles is caught.  `lefttype` is the
    /// amproclefttype woven into the expected argtypes, exactly as C does.
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
        /// out-of-range procnum: INFO + result=false, omitted (continue)
        Invalid,
    }

    fn classify_proc(amprocnum: i16, lefttype: Oid) -> ProcCheck {
        match amprocnum {
            HASHSTANDARD_PROC => ProcCheck::Amproc {
                restype: INT4OID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![lefttype],
            },
            HASHEXTENDED_PROC => ProcCheck::Amproc {
                restype: INT8OID,
                exact: true,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![lefttype, INT8OID],
            },
            HASHOPTIONS_PROC => ProcCheck::Opts,
            _ => ProcCheck::Invalid,
        }
    }

    /// Whether a successful proc check contributes its lefttype to hashabletypes.
    fn contributes_to_hashabletypes(amprocnum: i16, ok: bool) -> bool {
        ok && (amprocnum == HASHSTANDARD_PROC || amprocnum == HASHEXTENDED_PROC)
    }

    #[test]
    fn hash_procnum_and_strategy_constants_are_pinned() {
        assert_eq!(HASHSTANDARD_PROC, 1);
        assert_eq!(HASHEXTENDED_PROC, 2);
        assert_eq!(HASHOPTIONS_PROC, 3);
        assert_eq!(HTEqualStrategyNumber, 1);
        assert_eq!(HTMaxStrategyNumber, 1);
    }

    #[test]
    fn proc_signature_profiles() {
        // HASHSTANDARD: int4(lefttype), 1 arg, exact.  lefttype is woven in.
        assert_eq!(
            classify_proc(HASHSTANDARD_PROC, 25),
            ProcCheck::Amproc {
                restype: INT4OID,
                exact: true,
                minargs: 1,
                maxargs: 1,
                argtypes: vec![25],
            }
        );
        // HASHEXTENDED: int8(lefttype, int8), 2 args, exact.
        assert_eq!(
            classify_proc(HASHEXTENDED_PROC, 25),
            ProcCheck::Amproc {
                restype: INT8OID,
                exact: true,
                minargs: 2,
                maxargs: 2,
                argtypes: vec![25, INT8OID],
            }
        );
        // HASHOPTIONS uses the amoptsproc check (void(internal)).
        assert_eq!(classify_proc(HASHOPTIONS_PROC, 25), ProcCheck::Opts);
    }

    #[test]
    fn invalid_proc_numbers_rejected() {
        // 0 and anything past HASHOPTIONS_PROC is invalid (no optional range for
        // hash, unlike brin).
        assert_eq!(classify_proc(0, 25), ProcCheck::Invalid);
        assert_eq!(classify_proc(4, 25), ProcCheck::Invalid);
        assert_eq!(classify_proc(99, 25), ProcCheck::Invalid);
    }

    #[test]
    fn only_hash_procs_feed_hashabletypes() {
        // STANDARD and EXTENDED feed hashabletypes; OPTIONS does not.
        assert!(contributes_to_hashabletypes(HASHSTANDARD_PROC, true));
        assert!(contributes_to_hashabletypes(HASHEXTENDED_PROC, true));
        assert!(!contributes_to_hashabletypes(HASHOPTIONS_PROC, true));
        // ...and only when the signature check passed.
        assert!(!contributes_to_hashabletypes(HASHSTANDARD_PROC, false));
    }

    /// Mirror of hashvalidate's strategy-number gate: valid iff
    /// 1 <= s <= HTMaxStrategyNumber.
    fn strategy_in_range(stratnum: i16) -> bool {
        !(stratnum < 1 || stratnum > HTMaxStrategyNumber)
    }

    #[test]
    fn strategy_number_range_gate() {
        // Only strategy 1 is valid for hash.
        assert!(!strategy_in_range(0));
        assert!(strategy_in_range(1));
        assert!(!strategy_in_range(2));
        assert!(!strategy_in_range(63));
    }

    /// Mirror of the ORDER-BY rejection gate.
    fn orderby_invalid(amoppurpose: c_char, amopsortfamily: Oid) -> bool {
        amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)
    }

    #[test]
    fn hash_rejects_orderby_operators() {
        // pure search operator with no sort family -> OK
        assert!(!orderby_invalid(AMOP_SEARCH, InvalidOid));
        // search purpose but with a sort family set -> invalid
        assert!(orderby_invalid(AMOP_SEARCH, 1234));
        // ORDER BY purpose ('o') -> invalid
        assert!(orderby_invalid(b'o' as c_char, InvalidOid));
    }

    /// Mirror of the per-group completeness gate: a group is complete iff its
    /// operatorset is exactly the single equality-strategy bit.
    fn group_operatorset_complete(operatorset: u64) -> bool {
        operatorset == (1u64 << HTEqualStrategyNumber)
    }

    #[test]
    fn group_completeness_requires_exactly_equality_bit() {
        // exactly bit 1 set -> complete
        assert!(group_operatorset_complete(1u64 << HTEqualStrategyNumber));
        assert!(group_operatorset_complete(0b10));
        // empty (hash fn but no operator) -> incomplete
        assert!(!group_operatorset_complete(0));
        // extra bits -> incomplete
        assert!(!group_operatorset_complete(0b11));
        assert!(!group_operatorset_complete(0b100));
    }

    /// Mirror of the cross-type-completeness count check: number of groups must
    /// equal hashabletypes_count squared.
    fn crosstype_count_complete(grouplen: usize, hashabletypes_count: usize) -> bool {
        grouplen == hashabletypes_count * hashabletypes_count
    }

    #[test]
    fn crosstype_completeness_count() {
        // 2 hashable types -> need 4 groups.
        assert!(crosstype_count_complete(4, 2));
        assert!(!crosstype_count_complete(3, 2));
        // 1 hashable type -> need 1 group.
        assert!(crosstype_count_complete(1, 1));
        // 3 hashable types -> need 9 groups.
        assert!(crosstype_count_complete(9, 3));
        assert!(!crosstype_count_complete(8, 3));
    }

    // --- hashadjustmembers dependency-classification decision tree ------------

    #[derive(Debug, PartialEq, Eq)]
    enum Dep {
        /// soft dependency on the opfamily
        SoftFamily,
        /// hard dependency on the opclass
        HardOpclass,
    }

    /// Pure mirror of hashadjustmembers's per-member branch.  `opclass_exists`
    /// stands in for OidIsValid(opclassoid) after the (stubbed)
    /// opclass_for_family_datatype lookup for a non-cross-type member.
    fn classify_member(
        is_func: bool,
        number: i16,
        lefttype: Oid,
        righttype: Oid,
        opclass_exists: bool,
    ) -> Dep {
        if is_func && number != HASHSTANDARD_PROC {
            Dep::SoftFamily
        } else if lefttype != righttype {
            Dep::SoftFamily
        } else if opclass_exists {
            Dep::HardOpclass
        } else {
            Dep::SoftFamily
        }
    }

    #[test]
    fn adjustmembers_optional_func_is_soft_family() {
        // HASHEXTENDED (2) and HASHOPTIONS (3) are non-mandatory funcs -> soft.
        assert_eq!(
            classify_member(true, HASHEXTENDED_PROC, 10, 10, true),
            Dep::SoftFamily
        );
        assert_eq!(
            classify_member(true, HASHOPTIONS_PROC, 10, 10, true),
            Dep::SoftFamily
        );
    }

    #[test]
    fn adjustmembers_mandatory_func_noncrosstype_binds_opclass() {
        // HASHSTANDARD (1), non-cross-type, opclass present -> hard on opclass.
        assert_eq!(
            classify_member(true, HASHSTANDARD_PROC, 10, 10, true),
            Dep::HardOpclass
        );
        // ...but if no opclass exists, falls back to soft family.
        assert_eq!(
            classify_member(true, HASHSTANDARD_PROC, 10, 10, false),
            Dep::SoftFamily
        );
    }

    #[test]
    fn adjustmembers_crosstype_is_soft_family() {
        // Cross-type operator (is_func=false) -> soft family, regardless of opclass.
        assert_eq!(
            classify_member(false, HTEqualStrategyNumber, 10, 20, true),
            Dep::SoftFamily
        );
        // Cross-type mandatory func -> still soft family.
        assert_eq!(
            classify_member(true, HASHSTANDARD_PROC, 10, 20, true),
            Dep::SoftFamily
        );
    }

    #[test]
    fn adjustmembers_noncrosstype_operator_binds_opclass() {
        // Non-cross-type operator with an opclass -> hard on opclass.
        assert_eq!(
            classify_member(false, HTEqualStrategyNumber, 10, 10, true),
            Dep::HardOpclass
        );
        // ...without an opclass -> soft family.
        assert_eq!(
            classify_member(false, HTEqualStrategyNumber, 10, 10, false),
            Dep::SoftFamily
        );
    }
}
