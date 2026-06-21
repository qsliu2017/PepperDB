//! utils/misc/rls.c - RLS-related utility functions.

use crate::prelude::*;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::catalog::catalog::FirstNormalObjectId;
use crate::catalog::catalog_oids::RelationRelationId;
use crate::catalog::pg_class::Form_pg_class;
use crate::miscadmin::{GetUserId, InNoForceRLSOperation};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::RangeVar;
use crate::storage::lockdefs::NoLock;
use crate::utils::fmgr::FunctionCallInfo;

use crate::{PG_GETARG_OID, PG_GETARG_TEXT_PP, PG_RETURN_BOOL};

// rls.h: enum result of check_enable_rls
pub const RLS_NONE: c_int = 0;
pub const RLS_NONE_ENV: c_int = 1;
pub const RLS_ENABLED: c_int = 2;

// rls.h: extern PGDLLIMPORT bool row_security;
#[allow(non_upper_case_globals)]
pub static mut row_security: bool = false;

// utils/syscache.h: cache id for pg_class indexed by oid.
const RELOID: c_int = 57;

/*
 * check_enable_rls
 *
 * Determine, based on the relation, row_security setting, and current role,
 * if RLS is applicable to this query.  RLS_NONE_ENV indicates that, while
 * RLS is not to be added for this query, a change in the environment may change
 * that.  RLS_NONE means that RLS is not on the relation at all and therefore
 * we don't need to worry about it.  RLS_ENABLED means RLS should be implemented
 * for the table and the plan cache needs to be invalidated if the environment
 * changes.
 *
 * Handle checking as another role via checkAsUser (for views, etc).  Pass
 * InvalidOid to check the current user.
 *
 * If noError is set to 'true' then we just return RLS_ENABLED instead of doing
 * an ereport() if the user has attempted to bypass RLS and they are not
 * allowed to.
 */
#[no_mangle]
pub unsafe fn check_enable_rls(relid: Oid, checkAsUser: Oid, noError: bool) -> c_int {
    let user_id: Oid = if OidIsValid(checkAsUser) {
        checkAsUser
    } else {
        GetUserId()
    };
    let tuple: HeapTuple;
    let classform: Form_pg_class;
    let relrowsecurity: bool;
    let relforcerowsecurity: bool;
    let amowner: bool;

    /* Nothing to do for built-in relations */
    if relid < FirstNormalObjectId {
        return RLS_NONE;
    }

    /* Fetch relation's relrowsecurity and relforcerowsecurity flags */
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        return RLS_NONE;
    }
    classform = GETSTRUCT(tuple) as Form_pg_class;

    relrowsecurity = (*classform).relrowsecurity;
    relforcerowsecurity = (*classform).relforcerowsecurity;

    ReleaseSysCache(tuple);

    /* Nothing to do if the relation does not have RLS */
    if !relrowsecurity {
        return RLS_NONE;
    }

    /*
     * BYPASSRLS users always bypass RLS.  Note that superusers are always
     * considered to have BYPASSRLS.
     *
     * Return RLS_NONE_ENV to indicate that this decision depends on the
     * environment (in this case, the user_id).
     */
    if has_bypassrls_privilege(user_id) {
        return RLS_NONE_ENV;
    }

    /*
     * Table owners generally bypass RLS, except if the table has been set (by
     * an owner) to FORCE ROW SECURITY, and this is not a referential
     * integrity check.
     *
     * Return RLS_NONE_ENV to indicate that this decision depends on the
     * environment (in this case, the user_id).
     */
    amowner = object_ownercheck(RelationRelationId, relid, user_id);
    if amowner {
        /*
         * If FORCE ROW LEVEL SECURITY has been set on the relation then we
         * should return RLS_ENABLED to indicate that RLS should be applied.
         * If not, or if we are in an InNoForceRLSOperation context, we return
         * RLS_NONE_ENV.
         */
        if !relforcerowsecurity || InNoForceRLSOperation() {
            return RLS_NONE_ENV;
        }
    }

    /*
     * We should apply RLS.  However, the user may turn off the row_security
     * GUC to get a forced error instead.
     */
    if !row_security && !noError {
        ereport!(
            ERROR,
            "query would be affected by row-level security policy for table (ERRCODE_INSUFFICIENT_PRIVILEGE); to disable the policy for the table's owner, use ALTER TABLE NO FORCE ROW LEVEL SECURITY"
        );
    }

    /* RLS should be fully enabled for this relation. */
    RLS_ENABLED
}

/*
 * row_security_active
 *
 * check_enable_rls wrapped as a SQL callable function except
 * RLS_NONE_ENV and RLS_NONE are the same for this purpose.
 */
pub unsafe fn row_security_active(fcinfo: FunctionCallInfo) -> Datum {
    /* By OID */
    let tableoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let rls_status: c_int;

    rls_status = check_enable_rls(tableoid, InvalidOid, true);
    PG_RETURN_BOOL!(rls_status == RLS_ENABLED)
}

pub unsafe fn row_security_active_name(fcinfo: FunctionCallInfo) -> Datum {
    /* By qualified name */
    let tablename: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let tablerel: *mut RangeVar;
    let tableoid: Oid;
    let rls_status: c_int;

    /* Look up table name.  Can't lock it - we might not have privileges. */
    tablerel = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
    tableoid = RangeVarGetRelid(tablerel, NoLock, false);

    rls_status = check_enable_rls(tableoid, InvalidOid, true);
    PG_RETURN_BOOL!(rls_status == RLS_ENABLED)
}

// ---------------------------------------------------------------------------
// Local stubs for functions not yet ported.
// ---------------------------------------------------------------------------

/// STUB: utils/cache/syscache.c SearchSysCache1.
// TODO(pg-port): utils/cache/syscache.c not ported.
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(_cacheId, _key1) as _
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(_tuple as _)
}

unsafe fn has_bypassrls_privilege(_roleid: Oid) -> bool {
    crate::catalog::aclchk::has_bypassrls_privilege(_roleid)
}

unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    crate::catalog::aclchk::object_ownercheck(_classid, _objectid, _roleid)
}

#[allow(dead_code)]
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_rel_name(_relid) as _
}

/// STUB: nodes/makefuncs.c makeRangeVarFromNameList.
// TODO(pg-port): nodes/makefuncs.c not ported.
unsafe fn makeRangeVarFromNameList(_names: *mut List) -> *mut RangeVar {
    unimplemented!("TODO(pg-port): makeRangeVarFromNameList (nodes/makefuncs.c not ported)")
}

/// STUB: utils/adt/varlena.c textToQualifiedNameList.
// TODO(pg-port): utils/adt/varlena.c not ported.
unsafe fn textToQualifiedNameList(_textval: *mut text) -> *mut List {
    unimplemented!("TODO(pg-port): textToQualifiedNameList (utils/adt/varlena.c not ported)")
}

/// STUB: catalog/namespace.c RangeVarGetRelid.
// TODO(pg-port): catalog/namespace.c not ported.
unsafe fn RangeVarGetRelid(
    _relation: *const RangeVar,
    _lockmode: crate::storage::lockdefs::LOCKMODE,
    _missing_ok: bool,
) -> Oid {
    unimplemented!("TODO(pg-port): RangeVarGetRelid (catalog/namespace.c not ported)")
}
