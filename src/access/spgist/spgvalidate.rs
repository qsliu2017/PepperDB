//! Source: postgres/src/backend/access/spgist/spgvalidate.c
//!
//! Opclass validator for SP-GiST.
//!
//! MERGED from postgres/src/include/access/spgist.h (the bits this file needs):
//!   - the SP-GiST opclass support-function numbers
//!     (SPGIST_CONFIG_PROC .. SPGIST_OPTIONS_PROC, SPGISTNRequiredProc,
//!     SPGISTNProc)
//!   - the `spgConfigIn` / `spgConfigOut` argument structs for the config method
//! These are mirrored locally (the SP-GiST sibling opclass files do the same,
//! since access/spgist.h is not yet ported as a standalone module).
//!
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   "access/amvalidate.h"      -> crate::access::index::amvalidate
//!   "access/htup_details.h"    -> crate::access::htup_details (GETSTRUCT)
//!   "access/spgist.h"          -> the constants/structs mirrored below
//!   "catalog/pg_amop.h"        -> crate::catalog::pg_amop (Form_pg_amop, AMOP_SEARCH)
//!   "catalog/pg_amproc.h"      -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"     -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_type.h"        -> BOOLOID / VOIDOID / INTERNALOID (pg_type_d)
//!   "utils/builtins.h"         -> format_type_be (STUB)
//!   "utils/lsyscache.h"        -> get_opfamily_name / get_op_rettype (STUB)
//!   "utils/regproc.h"          -> format_procedure / format_operator (STUB)
//!   "utils/syscache.h"         -> SearchSysCache1 / SearchSysCacheList1 /
//!                                 ReleaseSysCache / ReleaseCatCacheList (STUB)
//!
//! REAL vs STUBBED:
//!
//! * The whole validation CONTROL FLOW of `spgvalidate` and `spgadjustmembers`
//!   is ported 1:1: which procnums are required, the per-procnum signature
//!   checks (incl. the config-proc leaf-type bookkeeping and the COMPRESS-proc
//!   left/right-type gating), the strategy-number range check, the operator
//!   signature check, the per-group "missing operators / missing functions"
//!   sweep (skipping the optional OPTIONS proc), and the originally-named
//!   opclass check.
//!
//! * The shared helpers are REUSED from access/index/amvalidate.rs:
//!   identify_opfamily_groups, check_amproc_signature, check_amop_signature,
//!   check_amoptsproc_signature, opfamily_can_sort_type, OpFamilyOpFuncGroup,
//!   CatCList.  (Those in turn stub the syscache fetch but contain the real
//!   comparison cores.)
//!
//! * STUBBED (deep deps, not yet ported): the SearchSysCache1(CLAOID) opclass
//!   fetch, the SearchSysCacheList1(AMOPSTRATEGY/AMPROCNUM) list scans,
//!   get_opfamily_name, get_op_rettype, format_procedure / format_operator /
//!   format_type_be (used only to build human-readable message text), and the
//!   OidFunctionCall2 invocation of the opclass config method.  These are routed
//!   through local `unimplemented!()` stubs.  The per-member classification
//!   logic that decides "is this signature acceptable for this procnum" is
//!   factored into the pure, unit-tested `classify_proc` helper.

use crate::prelude::*;

use crate::access::htup_details::GETSTRUCT;
use crate::access::index::amapi::OpFamilyMember;
use crate::access::index::amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opfamily_can_sort_type, CatCList, OpFamilyOpFuncGroup,
};
use crate::catalog::pg_amop::{Form_pg_amop, AMOP_SEARCH};
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_type_d::{BOOLOID, INTERNALOID, VOIDOID};
use crate::nodes::pg_list::{lfirst, List};
use crate::{current_cell, ereport, errmsg, foreach};

// ===========================================================================
//   access/spgist.h: SP-GiST opclass support-function numbers (mirrored)
// ===========================================================================

/// `spg_config`: report data type config (REQUIRED)
pub const SPGIST_CONFIG_PROC: c_int = 1;
/// `spg_choose`: choose method for adding a new value (REQUIRED)
pub const SPGIST_CHOOSE_PROC: c_int = 2;
/// `spg_picksplit`: split an overflowing inner tuple (REQUIRED)
pub const SPGIST_PICKSPLIT_PROC: c_int = 3;
/// `spg_inner_consistent`: search an inner tuple (REQUIRED)
pub const SPGIST_INNER_CONSISTENT_PROC: c_int = 4;
/// `spg_leaf_consistent`: search a leaf tuple (REQUIRED)
pub const SPGIST_LEAF_CONSISTENT_PROC: c_int = 5;
/// `spg_compress`: convert datum to leaf type (OPTIONAL)
pub const SPGIST_COMPRESS_PROC: c_int = 6;
/// `spg_options`: parse reloptions (OPTIONAL)
pub const SPGIST_OPTIONS_PROC: c_int = 7;
/// number of required support functions
pub const SPGISTNRequiredProc: c_int = 5;
/// total number of support functions
pub const SPGISTNProc: c_int = 7;

// ===========================================================================
//   access/spgist.h: argument structs for spg_config method (mirrored)
// ===========================================================================

/// `spgConfigIn` - input struct for the spg_config support method.
#[repr(C)]
pub struct spgConfigIn {
    /// Data type to be indexed
    pub attType: Oid,
}

/// `spgConfigOut` - output struct for the spg_config support method.
#[repr(C)]
pub struct spgConfigOut {
    /// Data type of inner-tuple prefixes
    pub prefixType: Oid,
    /// Data type of inner-tuple node labels
    pub labelType: Oid,
    /// Data type of leaf-tuple values
    pub leafType: Oid,
    /// Opclass can reconstruct original data
    pub canReturnData: bool,
    /// Opclass can cope with values > 1 page
    pub longValuesOK: bool,
}

// ===========================================================================
//   STUB deep deps (catalog/syscache/lsyscache/regproc, fmgr invocation).
// ===========================================================================

/// STUB for `SearchSysCache1(CLAOID, opclassoid)` + GETSTRUCT -> Form_pg_opclass.
/// utils/syscache.c is not ported; this panics until it is.
unsafe fn search_claoid_syscache(_opclassoid: Oid) -> Form_pg_opclass {
    unimplemented!("STUB: syscache CLAOID lookup (utils/syscache.c not ported)")
}

/// STUB for `SearchSysCacheList1(AMOPSTRATEGY, opfamilyoid)`.
unsafe fn search_amopstrategy_list(_opfamilyoid: Oid) -> *const CatCList {
    unimplemented!("STUB: catcache AMOPSTRATEGY list (utils/catcache.c not ported)")
}

/// STUB for `SearchSysCacheList1(AMPROCNUM, opfamilyoid)`.
unsafe fn search_amprocnum_list(_opfamilyoid: Oid) -> *const CatCList {
    unimplemented!("STUB: catcache AMPROCNUM list (utils/catcache.c not ported)")
}

/// STUB for `get_opfamily_name(opfamilyoid, missing_ok)` (utils/lsyscache.c).
fn get_opfamily_name(_opfamilyoid: Oid, _missing_ok: bool) -> *mut c_char { unimplemented!() }

/// STUB for `get_op_rettype(opno)` (utils/lsyscache.c).
fn get_op_rettype(_opno: Oid) -> Oid { unimplemented!() }

/// STUB for `format_procedure(procoid)` (utils/regproc.c) - message text only.
fn format_procedure(_procoid: Oid) -> *mut c_char {
    unimplemented!("STUB: format_procedure (utils/regproc.c not ported)")
}

/// STUB for `format_operator(opno)` (utils/regproc.c) - message text only.
fn format_operator(_opno: Oid) -> *mut c_char {
    unimplemented!("STUB: format_operator (utils/regproc.c not ported)")
}

/// STUB for `format_type_be(typid)` (utils/adt/format_type.c) - message text only.
fn format_type_be(_typid: Oid) -> *mut c_char {
    unimplemented!("STUB: format_type_be (utils/adt/format_type.c not ported)")
}

/// STUB for `OidFunctionCall2(func, arg1, arg2)` invocation of the opclass
/// config support method.  fmgr can dispatch, but the function must be a real
/// registered opclass method backed by the catalog, so this is stubbed here.
unsafe fn oid_function_call2_config(
    _func: Oid,
    _cfgin: *mut spgConfigIn,
    _cfgout: *mut spgConfigOut,
) {
    unimplemented!("STUB: OidFunctionCall2 of spg_config method (needs live fmgr/catalog)")
}

// ===========================================================================
//   Pure per-procnum signature classification (REAL, unit-tested).
// ===========================================================================

/// Inputs needed to classify a single SP-GiST support function, mirrored from
/// the Form_pg_amproc row plus the running config-proc bookkeeping state.
#[derive(Clone, Copy, Debug)]
pub struct ProcClassifyIn {
    pub amprocnum: c_int,
    pub amproclefttype: Oid,
    pub amprocrighttype: Oid,
    /// lefttype recorded by the CONFIG proc (InvalidOid until one is seen)
    pub config_lefttype: Oid,
    /// righttype recorded by the CONFIG proc
    pub config_righttype: Oid,
    /// the leaf type the COMPRESS proc must convert to
    pub config_leaftype: Oid,
}

/// Result of classifying a support function's procnum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcClassify {
    /// CONFIG/CHOOSE/PICKSPLIT/INNER_CONSISTENT: void(internal, internal)
    VoidInternal2,
    /// LEAF_CONSISTENT: bool(internal, internal)
    BoolInternal2,
    /// COMPRESS, but the left/right type didn't match the config proc -> always
    /// fails the signature check (ok = false, no syscache fetch)
    CompressTypeMismatch,
    /// COMPRESS with matching types: leaftype(lefttype) signature check
    Compress { leaftype: Oid, lefttype: Oid },
    /// OPTIONS: check_amoptsproc_signature
    Options,
    /// unrecognized support number -> emit "invalid support number" + continue
    Invalid,
}

/// The pure decision of the `switch (procform->amprocnum)` in spgvalidate: which
/// signature check a given support function requires.  Factored out so it can be
/// exercised without a live syscache.  (The CONFIG proc's side effects -- running
/// the config method and recording leaf/left/right types -- are handled by the
/// caller; here we only classify the *signature* requirement.)
pub fn classify_proc(input: &ProcClassifyIn) -> ProcClassify {
    match input.amprocnum {
        SPGIST_CONFIG_PROC
        | SPGIST_CHOOSE_PROC
        | SPGIST_PICKSPLIT_PROC
        | SPGIST_INNER_CONSISTENT_PROC => ProcClassify::VoidInternal2,
        SPGIST_LEAF_CONSISTENT_PROC => ProcClassify::BoolInternal2,
        SPGIST_COMPRESS_PROC => {
            if input.config_lefttype != input.amproclefttype
                || input.config_righttype != input.amprocrighttype
            {
                ProcClassify::CompressTypeMismatch
            } else {
                ProcClassify::Compress {
                    leaftype: input.config_leaftype,
                    lefttype: input.amproclefttype,
                }
            }
        }
        SPGIST_OPTIONS_PROC => ProcClassify::Options,
        _ => ProcClassify::Invalid,
    }
}

// ===========================================================================
//   spgvalidate (REAL control flow; catalog scans STUBBED)
// ===========================================================================

/// Validator for an SP-GiST opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family.  But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API.
///
/// # Safety
/// Relies on the (stubbed) syscache returning valid catalog tuples.
pub unsafe fn spgvalidate(opclassoid: Oid) -> bool {
    let mut result = true;
    let mut configOut: spgConfigOut;
    let mut configOutLefttype: Oid = InvalidOid;
    let mut configOutRighttype: Oid = InvalidOid;
    let mut configOutLeafType: Oid = InvalidOid;

    // Fetch opclass information
    let classform = search_claoid_syscache(opclassoid);

    let opfamilyoid = (*classform).opcfamily;
    let opcintype = (*classform).opcintype;
    let opckeytype = (*classform).opckeytype;
    // C: opclassname = NameStr(classform->opcname); used only for message text,
    // which is stubbed; reference the field to mirror the read.
    let _opclassname = &(*classform).opcname;

    // Fetch opfamily information (message text only; stubbed)
    let _opfamilyname = get_opfamily_name(opfamilyoid, false);

    // Fetch all operators and support functions of the opfamily
    let oprlist = search_amopstrategy_list(opfamilyoid);
    let proclist = search_amprocnum_list(opfamilyoid);
    let grouplist = identify_opfamily_groups(oprlist, proclist);

    // Check individual support functions
    let proclist_ref = &*proclist;
    for i in 0..proclist_ref.n_members {
        let proctup = proclist_member_tuple(proclist, i as usize);
        let procform = GETSTRUCT(proctup) as Form_pg_amproc;
        let mut ok = true;

        // All SP-GiST support functions should be registered with matching
        // left/right types
        if (*procform).amproclefttype != (*procform).amprocrighttype {
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                    "?", "spgist", format_proc_str((*procform).amproc)
                )
            );
            result = false;
        }

        // Check procedure numbers and function signatures
        let cls = classify_proc(&ProcClassifyIn {
            amprocnum: (*procform).amprocnum as c_int,
            amproclefttype: (*procform).amproclefttype,
            amprocrighttype: (*procform).amprocrighttype,
            config_lefttype: configOutLefttype,
            config_righttype: configOutRighttype,
            config_leaftype: configOutLeafType,
        });

        match cls {
            ProcClassify::VoidInternal2 => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    VOIDOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                );

                // The CONFIG proc additionally runs the config method and
                // records the resulting leaf/left/right types.  We perform that
                // bookkeeping here (it does not change `ok`).
                if (*procform).amprocnum as c_int == SPGIST_CONFIG_PROC {
                    let mut configIn = spgConfigIn {
                        attType: (*procform).amproclefttype,
                    };
                    configOut = core::mem::zeroed();
                    let configOut_ptr = &configOut as *const spgConfigOut as *mut spgConfigOut;

                    oid_function_call2_config(
                        (*procform).amproc,
                        &mut configIn as *mut spgConfigIn,
                        configOut_ptr,
                    );

                    configOutLefttype = (*procform).amproclefttype;
                    configOutRighttype = (*procform).amprocrighttype;

                    // Default leaf type is opckeytype or input type
                    if OidIsValid(opckeytype) {
                        configOutLeafType = opckeytype;
                    } else {
                        configOutLeafType = (*procform).amproclefttype;
                    }

                    // If some other leaf datum type is specified, warn
                    if OidIsValid((*configOut_ptr).leafType)
                        && configOutLeafType != (*configOut_ptr).leafType
                    {
                        let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
                        ereport!(
                            INFO,
                            errmsg!(
                                "SP-GiST leaf data type {} does not match declared type {}",
                                format_type_str((*configOut_ptr).leafType),
                                format_type_str(configOutLeafType)
                            )
                        );
                        result = false;
                        configOutLeafType = (*configOut_ptr).leafType;
                    }

                    // When leaf and attribute types are the same, compress
                    // function is not required and we set corresponding bit in
                    // functionset for later group consistency check.
                    if configOutLeafType == configIn.attType {
                        foreach!(lc, grouplist, {
                            let group = lfirst(current_cell!(lc)) as *mut OpFamilyOpFuncGroup;
                            if (*group).lefttype == (*procform).amproclefttype
                                && (*group).righttype == (*procform).amprocrighttype
                            {
                                (*group).functionset |= 1u64 << SPGIST_COMPRESS_PROC;
                                break;
                            }
                        });
                    }
                }
            }
            ProcClassify::BoolInternal2 => {
                ok = check_amproc_signature(
                    (*procform).amproc,
                    BOOLOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                );
            }
            ProcClassify::CompressTypeMismatch => {
                ok = false;
            }
            ProcClassify::Compress { leaftype, lefttype } => {
                ok = check_amproc_signature((*procform).amproc, leaftype, true, 1, 1, &[lefttype]);
            }
            ProcClassify::Options => {
                ok = check_amoptsproc_signature((*procform).amproc);
            }
            ProcClassify::Invalid => {
                let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                        "?", "spgist", format_proc_str((*procform).amproc), (*procform).amprocnum
                    )
                );
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains function {} with wrong signature for support number {}",
                    "?", "spgist", format_proc_str((*procform).amproc), (*procform).amprocnum
                )
            );
            result = false;
        }
    }

    // Check individual operators
    let oprlist_ref = &*oprlist;
    for i in 0..oprlist_ref.n_members {
        let oprtup = oprlist_member_tuple(oprlist, i as usize);
        let oprform = GETSTRUCT(oprtup) as Form_pg_amop;
        let op_rettype: Oid;

        // TODO: Check that only allowed strategy numbers exist
        if (*oprform).amopstrategy < 1 || (*oprform).amopstrategy > 63 {
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                    "?", "spgist", format_op_str((*oprform).amopopr), (*oprform).amopstrategy
                )
            );
            result = false;
        }

        // spgist supports ORDER BY operators
        if (*oprform).amoppurpose != AMOP_SEARCH {
            // ... and operator result must match the claimed btree opfamily
            op_rettype = get_op_rettype((*oprform).amopopr);
            if !opfamily_can_sort_type((*oprform).amopsortfamily, op_rettype) {
                let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
                ereport!(
                    INFO,
                    errmsg!(
                        "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                        "?", "spgist", format_op_str((*oprform).amopopr)
                    )
                );
                result = false;
            }
        } else {
            op_rettype = BOOLOID;
        }

        // Check operator signature --- same for all spgist strategies
        if !check_amop_signature(
            (*oprform).amopopr,
            op_rettype,
            (*oprform).amoplefttype,
            (*oprform).amoprighttype,
        ) {
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                    "?", "spgist", format_op_str((*oprform).amopopr)
                )
            );
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions
    let mut opclassgroup: *mut OpFamilyOpFuncGroup = core::ptr::null_mut();
    foreach!(lc, grouplist, {
        let thisgroup = lfirst(current_cell!(lc)) as *mut OpFamilyOpFuncGroup;

        // Remember the group exactly matching the test opclass
        if (*thisgroup).lefttype == opcintype && (*thisgroup).righttype == opcintype {
            opclassgroup = thisgroup;
        }

        // Complain if there are any datatype pairs with functions but no
        // operators.  This is about the best we can do for now to detect
        // missing operators.
        if (*thisgroup).operatorset == 0 {
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                    "?", "spgist",
                    format_type_str((*thisgroup).lefttype),
                    format_type_str((*thisgroup).righttype)
                )
            );
            result = false;
        }

        // Complain if we're missing functions for any datatype, remembering
        // that SP-GiST doesn't use cross-type support functions.
        if (*thisgroup).lefttype != (*thisgroup).righttype {
            continue;
        }

        for i in 1..=SPGISTNProc {
            if ((*thisgroup).functionset & (1u64 << i)) != 0 {
                continue; // got it
            }
            if i == SPGIST_OPTIONS_PROC {
                continue; // optional method
            }
            let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
            ereport!(
                INFO,
                errmsg!(
                    "operator family \"{}\" of access method {} is missing support function {} for type {}",
                    "?", "spgist", i, format_type_str((*thisgroup).lefttype)
                )
            );
            result = false;
        }
    });

    // Check that the originally-named opclass is supported
    // (if group is there, we already checked it adequately above)
    if opclassgroup.is_null() {
        let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
        ereport!(
            INFO,
            errmsg!(
                "operator class \"{}\" of access method {} is missing operator(s)",
                "?", "spgist"
            )
        );
        result = false;
    }

    // C: ReleaseCatCacheList(proclist); ReleaseCatCacheList(oprlist);
    //    ReleaseSysCache(classtup); -- no-ops until syscache lands.

    result
}

// ===========================================================================
//   spgadjustmembers (REAL, ported 1:1)
// ===========================================================================

/// Prechecking function for adding operators/functions to an SP-GiST opfamily.
///
/// # Safety
/// `operators`/`functions` must be valid Lists of `*mut OpFamilyMember`.
pub unsafe fn spgadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    // Operator members of an SP-GiST opfamily should never have hard
    // dependencies, since their connection to the opfamily depends only on what
    // the support functions think, and that can be altered.  For consistency, we
    // make all soft dependencies point to the opfamily, though a soft dependency
    // on the opclass would work as well in the CREATE OPERATOR CLASS case.
    foreach!(lc, operators, {
        let op = lfirst(current_cell!(lc)) as *mut OpFamilyMember;
        (*op).ref_is_hard = false;
        (*op).ref_is_family = true;
        (*op).refobjid = opfamilyoid;
    });

    // Required support functions should have hard dependencies.  Preferably
    // those are just dependencies on the opclass, but if we're in ALTER OPERATOR
    // FAMILY, we leave the dependency pointing at the whole opfamily.  (Given
    // that SP-GiST opclasses generally don't share opfamilies, it seems unlikely
    // to be worth working harder.)
    foreach!(lc, functions, {
        let op = lfirst(current_cell!(lc)) as *mut OpFamilyMember;

        match (*op).number {
            SPGIST_CONFIG_PROC
            | SPGIST_CHOOSE_PROC
            | SPGIST_PICKSPLIT_PROC
            | SPGIST_INNER_CONSISTENT_PROC
            | SPGIST_LEAF_CONSISTENT_PROC => {
                // Required support function
                (*op).ref_is_hard = true;
            }
            SPGIST_COMPRESS_PROC | SPGIST_OPTIONS_PROC => {
                // Optional, so force it to be a soft family dependency
                (*op).ref_is_hard = false;
                (*op).ref_is_family = true;
                (*op).refobjid = opfamilyoid;
            }
            _ => {
                let _ = errcode(ERRCODE_INVALID_OBJECT_DEFINITION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "support function number {} is invalid for access method {}",
                        (*op).number, "spgist"
                    )
                );
            }
        }
    });
}

// ===========================================================================
//   small helpers
// ===========================================================================

/* errcodes.h classification (errcode() shim ignores the value). */
// TODO(pg-port): ERRCODE_INVALID_OBJECT_DEFINITION from utils/errcodes.h.
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0;

/// `&proclist->members[i]->tuple` for an amvalidate CatCList.  Mirrors the
/// (private) member-access helper in amvalidate.rs; reproduced here because
/// CatCList::member_tuple is not public.
#[inline]
unsafe fn proclist_member_tuple(
    list: *const CatCList,
    i: usize,
) -> *const crate::access::htup_details::HeapTupleData {
    catclist_member_tuple(list, i)
}

#[inline]
unsafe fn oprlist_member_tuple(
    list: *const CatCList,
    i: usize,
) -> *const crate::access::htup_details::HeapTupleData {
    catclist_member_tuple(list, i)
}

/// `&list->members[i]->tuple` - reconstructs the access path CatCList uses
/// internally (its members[] is a flexible array of `*mut CatCTup`, each with a
/// leading `tuple: HeapTupleData`).
#[inline]
unsafe fn catclist_member_tuple(
    list: *const CatCList,
    i: usize,
) -> *const crate::access::htup_details::HeapTupleData {
    // CatCTup begins with `tuple: HeapTupleData`, so the member pointer (a
    // `*mut CatCTup`) can be reinterpreted as a `*const HeapTupleData`.
    let base = (*list).members.as_ptr();
    let memb = *base.add(i);
    memb as *const crate::access::htup_details::HeapTupleData
}

/// Render `format_procedure(oid)` to a String for a message.  STUB-backed.
fn format_proc_str(oid: Oid) -> String {
    let _ = format_procedure(oid);
    format!("<proc {}>", oid)
}

/// Render `format_operator(oid)`.  STUB-backed.
fn format_op_str(oid: Oid) -> String {
    let _ = format_operator(oid);
    format!("<op {}>", oid)
}

/// Render `format_type_be(oid)`.  STUB-backed.
fn format_type_str(oid: Oid) -> String {
    let _ = format_type_be(oid);
    format!("<type {}>", oid)
}

// ===========================================================================
//                                 TESTS
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- required procnum / strategy invariants (structural) -----------------

    #[test]
    fn procnum_constants_match_postgres() {
        assert_eq!(SPGIST_CONFIG_PROC, 1);
        assert_eq!(SPGIST_CHOOSE_PROC, 2);
        assert_eq!(SPGIST_PICKSPLIT_PROC, 3);
        assert_eq!(SPGIST_INNER_CONSISTENT_PROC, 4);
        assert_eq!(SPGIST_LEAF_CONSISTENT_PROC, 5);
        assert_eq!(SPGIST_COMPRESS_PROC, 6);
        assert_eq!(SPGIST_OPTIONS_PROC, 7);
        assert_eq!(SPGISTNRequiredProc, 5);
        assert_eq!(SPGISTNProc, 7);
    }

    /// The per-group "missing function" sweep iterates i in 1..=SPGISTNProc and
    /// skips only SPGIST_OPTIONS_PROC, so the set of REQUIRED procnums is
    /// {1,2,3,4,5,6} (COMPRESS is required unless leaf==attr type, which is
    /// modeled by pre-setting its bit).  Assert that invariant.
    #[test]
    fn required_procnum_set_excludes_only_options() {
        let mut required = Vec::new();
        for i in 1..=SPGISTNProc {
            if i == SPGIST_OPTIONS_PROC {
                continue;
            }
            required.push(i);
        }
        assert_eq!(required, vec![1, 2, 3, 4, 5, 6]);
    }

    // --- classify_proc (REAL per-member signature classification) ------------

    fn base(amprocnum: c_int) -> ProcClassifyIn {
        ProcClassifyIn {
            amprocnum,
            amproclefttype: 100,
            amprocrighttype: 100,
            config_lefttype: 100,
            config_righttype: 100,
            config_leaftype: 200,
        }
    }

    #[test]
    fn classify_void_internal_group() {
        for n in [
            SPGIST_CONFIG_PROC,
            SPGIST_CHOOSE_PROC,
            SPGIST_PICKSPLIT_PROC,
            SPGIST_INNER_CONSISTENT_PROC,
        ] {
            assert_eq!(classify_proc(&base(n)), ProcClassify::VoidInternal2);
        }
    }

    #[test]
    fn classify_leaf_consistent_is_bool() {
        assert_eq!(
            classify_proc(&base(SPGIST_LEAF_CONSISTENT_PROC)),
            ProcClassify::BoolInternal2
        );
    }

    #[test]
    fn classify_compress_matching_types() {
        let inp = base(SPGIST_COMPRESS_PROC);
        assert_eq!(
            classify_proc(&inp),
            ProcClassify::Compress {
                leaftype: 200,
                lefttype: 100,
            }
        );
    }

    #[test]
    fn classify_compress_type_mismatch() {
        // COMPRESS proc whose lefttype differs from the config proc's lefttype
        // must fail outright (ok=false), without a signature check.
        let mut inp = base(SPGIST_COMPRESS_PROC);
        inp.config_lefttype = 999;
        assert_eq!(classify_proc(&inp), ProcClassify::CompressTypeMismatch);

        // righttype mismatch likewise.
        let mut inp2 = base(SPGIST_COMPRESS_PROC);
        inp2.config_righttype = 999;
        assert_eq!(classify_proc(&inp2), ProcClassify::CompressTypeMismatch);
    }

    #[test]
    fn classify_options_and_invalid() {
        assert_eq!(
            classify_proc(&base(SPGIST_OPTIONS_PROC)),
            ProcClassify::Options
        );
        assert_eq!(classify_proc(&base(0)), ProcClassify::Invalid);
        assert_eq!(classify_proc(&base(8)), ProcClassify::Invalid);
        assert_eq!(classify_proc(&base(-1)), ProcClassify::Invalid);
    }
}
