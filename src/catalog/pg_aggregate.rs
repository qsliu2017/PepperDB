//! Translation of postgres/src/include/catalog/pg_aggregate.h
//!
//! The `FormData_pg_aggregate` struct: the fixed-layout part of a pg_aggregate
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length fields (agginitval, aggminitval - both `text`, guarded by
//! CATALOG_VARLEN) are NOT part of this in-memory struct - they live only in a
//! real on-disk pg_aggregate tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, int32, OidIsValid};
use crate::postgres_ext::{InvalidOid, Oid};
use core::ffi::{c_char, c_int};

use crate::nodes::pg_list::List;
use crate::nodes::parsenodes::{ObjectType::OBJECT_FUNCTION, ACL_EXECUTE};
use crate::catalog::pg_type_d::ANYOID;
use crate::catalog::catalog_oids::ProcedureRelationId;
use crate::catalog::aclchk::{aclcheck_error, object_aclcheck};
use crate::utils::adt::acl::{AclResult, AclResult::ACLCHECK_OK};
use crate::utils::elog::ERROR;
use crate::parser::parse_func::{func_get_detail, func_signature_string, FuncDetailCode,
                                FUNCDETAIL_NORMAL};
use crate::parser::parse_coerce::{enforce_generic_type_consistency, IsBinaryCoercible};
use crate::utils::cache::lsyscache::get_func_name;
use crate::miscadmin::GetUserId;
use crate::{ereport, errmsg};

const NIL: *mut List = core::ptr::null_mut();

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_aggregate - the fixed part of a pg_aggregate row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_aggregate {
    /* pg_proc OID of the aggregate itself */
    pub aggfnoid: regproc,
    /* aggregate kind, see AGGKIND_ categories below */
    pub aggkind: c_char,
    /* number of arguments that are "direct" arguments */
    pub aggnumdirectargs: int16,
    /* transition function */
    pub aggtransfn: regproc,
    /* final function (0 if none) */
    pub aggfinalfn: regproc,
    /* combine function (0 if none) */
    pub aggcombinefn: regproc,
    /* function to convert transtype to bytea (0 if none) */
    pub aggserialfn: regproc,
    /* function to convert bytea to transtype (0 if none) */
    pub aggdeserialfn: regproc,
    /* forward function for moving-aggregate mode (0 if none) */
    pub aggmtransfn: regproc,
    /* inverse function for moving-aggregate mode (0 if none) */
    pub aggminvtransfn: regproc,
    /* final function for moving-aggregate mode (0 if none) */
    pub aggmfinalfn: regproc,
    /* true to pass extra dummy arguments to aggfinalfn */
    pub aggfinalextra: bool,
    /* true to pass extra dummy arguments to aggmfinalfn */
    pub aggmfinalextra: bool,
    /* tells whether aggfinalfn modifies transition state */
    pub aggfinalmodify: c_char,
    /* tells whether aggmfinalfn modifies transition state */
    pub aggmfinalmodify: c_char,
    /* associated sort operator (0 if none) */
    pub aggsortop: Oid,
    /* type of aggregate's transition (state) data */
    pub aggtranstype: Oid,
    /* estimated size of state data (0 for default estimate) */
    pub aggtransspace: int32,
    /* type of moving-aggregate state data (0 if none) */
    pub aggmtranstype: Oid,
    /* estimated size of moving-agg state (0 for default est) */
    pub aggmtransspace: int32,
}

/*
 * Form_pg_aggregate corresponds to a pointer to a tuple with the format of the
 * pg_aggregate relation.
 */
pub type Form_pg_aggregate = *mut FormData_pg_aggregate;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/*
 * Symbolic values for aggkind column.  We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments) and from hypothetical-set aggregates
 * (which are a subclass of ordered-set aggregates in which the last
 * direct arguments have to match up in number and datatypes with the
 * aggregated arguments).
 */
pub const AGGKIND_NORMAL: c_char = b'n' as c_char;
pub const AGGKIND_ORDERED_SET: c_char = b'o' as c_char;
pub const AGGKIND_HYPOTHETICAL: c_char = b'h' as c_char;

/* Use this macro to test for "ordered-set agg including hypothetical case" */
pub fn AGGKIND_IS_ORDERED_SET(kind: c_char) -> bool {
    kind != AGGKIND_NORMAL
}

/*
 * Symbolic values for aggfinalmodify and aggmfinalmodify columns.
 * Preferably, finalfns do not modify the transition state value at all,
 * but in some cases that would cost too much performance.  We distinguish
 * "pure read only" and "trashes it arbitrarily" cases, as well as the
 * intermediate case where multiple finalfn calls are allowed but the
 * transfn cannot be applied anymore after the first finalfn call.
 */
pub const AGGMODIFY_READ_ONLY: c_char = b'r' as c_char;
pub const AGGMODIFY_SHAREABLE: c_char = b's' as c_char;
pub const AGGMODIFY_READ_WRITE: c_char = b'w' as c_char;

unsafe fn cstr_to_str(s: *const c_char) -> std::borrow::Cow<'static, str> {
    std::ffi::CStr::from_ptr(s).to_string_lossy()
}

/*
 * lookup_agg_function
 * common code for finding aggregate support functions
 *
 * fnName: possibly-schema-qualified function name
 * nargs, input_types: expected function argument types
 * variadicArgType: type of variadic argument if any, else InvalidOid
 *
 * Returns OID of function, and stores its return type into *rettype
 *
 * NB: must not scribble on input_types[], as we may re-use those
 */
unsafe fn lookup_agg_function(
    fnName: *mut List,
    nargs: c_int,
    input_types: *mut Oid,
    variadicArgType: Oid,
    rettype: *mut Oid,
) -> Oid {
    let fnOid: Oid;
    let mut retset: bool = false;
    let mut nvargs: c_int = 0;
    let mut vatype: Oid = InvalidOid;
    let mut true_oid_array: *mut Oid = core::ptr::null_mut();
    let fdresult: FuncDetailCode;
    let aclresult: AclResult;
    let mut fnOid_local: Oid = InvalidOid;
    let i: c_int;

    /*
     * func_get_detail looks up the function in the catalogs, does
     * disambiguation for polymorphic functions, handles inheritance, and
     * returns the funcid and type and set or singleton status of the
     * function's return value.  it also returns the true argument types to
     * the function.
     */
    fdresult = func_get_detail(fnName, NIL, NIL,
                               nargs, input_types, false, false, false,
                               &mut fnOid_local, rettype, &mut retset,
                               &mut nvargs, &mut vatype,
                               &mut true_oid_array, core::ptr::null_mut());
    fnOid = fnOid_local;

    /* only valid case is a normal function not returning a set */
    if fdresult != FUNCDETAIL_NORMAL || !OidIsValid(fnOid) {
        ereport!(ERROR,
            errmsg!("function {} does not exist",
                    cstr_to_str(func_signature_string(fnName, nargs,
                                                      NIL, input_types)))
            /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
        );
    }
    if retset {
        ereport!(ERROR,
            errmsg!("function {} returns a set",
                    cstr_to_str(func_signature_string(fnName, nargs,
                                                      NIL, input_types)))
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        );
    }

    /*
     * If the agg is declared to take VARIADIC ANY, the underlying functions
     * had better be declared that way too, else they may receive too many
     * parameters; but func_get_detail would have been happy with plain ANY.
     * (Probably nothing very bad would happen, but it wouldn't work as the
     * user expects.)  Other combinations should work without any special
     * pushups, given that we told func_get_detail not to expand VARIADIC.
     */
    if variadicArgType == ANYOID && vatype != ANYOID {
        ereport!(ERROR,
            errmsg!("function {} must accept VARIADIC ANY to be used in this aggregate",
                    cstr_to_str(func_signature_string(fnName, nargs,
                                                      NIL, input_types)))
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        );
    }

    /*
     * If there are any polymorphic types involved, enforce consistency, and
     * possibly refine the result type.  It's OK if the result is still
     * polymorphic at this point, though.
     */
    *rettype = enforce_generic_type_consistency(input_types,
                                                true_oid_array,
                                                nargs,
                                                *rettype,
                                                true);

    /*
     * func_get_detail will find functions requiring run-time argument type
     * coercion, but nodeAgg.c isn't prepared to deal with that
     */
    i = 0;
    let mut i = i;
    while i < nargs {
        let idx = i as usize;
        if !IsBinaryCoercible(*input_types.add(idx), *true_oid_array.add(idx)) {
            ereport!(ERROR,
                errmsg!("function {} requires run-time type coercion",
                        cstr_to_str(func_signature_string(fnName, nargs,
                                                          NIL, true_oid_array)))
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }
        i += 1;
    }

    /* Check aggregate creator has permission to call the function */
    aclresult = object_aclcheck(ProcedureRelationId, fnOid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fnOid));
    }

    fnOid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // aggfnoid is the first (key) field, sitting at offset 0.  Note there is
        // no leading oid column in pg_aggregate; aggfnoid serves as the OID key.
        assert_eq!(core::mem::offset_of!(FormData_pg_aggregate, aggfnoid), 0);
        // aggkind follows the 4-byte regproc (Oid) aggfnoid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_aggregate, aggkind),
            core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_aggregate>()
                >= core::mem::offset_of!(FormData_pg_aggregate, aggmtransspace)
                    + core::mem::size_of::<int32>()
        );
    }
}
