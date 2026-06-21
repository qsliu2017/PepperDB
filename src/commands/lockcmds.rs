//! commands/lockcmds.c - LOCK command support code.

use crate::prelude::*;
use crate::{IsA, foreach, current_cell, makeNode};

// table_open / table_close (access/table/table.c).
use crate::access::table::table::{table_close, table_open};

// LOCKMODE and lock-level constants (storage/lockdefs.h).
use crate::storage::lockdefs::{
    AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE,
};

// GetUserId (miscadmin.h / miscinit.c).
use crate::miscadmin::GetUserId;

// Parse nodes.
use crate::nodes::parsenodes::{
    LockStmt, Query, RangeTblEntry, AclMode,
    ACL_DELETE, ACL_INSERT, ACL_SELECT, ACL_TRUNCATE, ACL_UPDATE,
};
use crate::nodes::primnodes::RangeVar;
use crate::nodes::nodes::Node;

// List helpers (nodes/pg_list.c).
use crate::nodes::pg_list::{
    lappend_oid, list_delete_last, list_member_oid, lfirst, lfirst_oid, List, NIL,
};

// Tree walkers (nodes/nodeFuncs.c).
use crate::nodes::nodeFuncs::{
    expression_tree_walker, query_tree_walker, QTW_IGNORE_JOINALIASES,
};

// Relation accessor type (utils/rel.h).
use crate::utils::rel::Relation;

// pg_class relkind / relpersistence constants (catalog/pg_class.h).
use crate::catalog::pg_class::{
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW, RELPERSISTENCE_TEMP,
};

// ObjectIdGetDatum (postgres.h) - via prelude, spelled for clarity.
use crate::postgres::ObjectIdGetDatum;

// ---------------------------------------------------------------------------
// Local stubs / consts for callees not yet ported.
// ---------------------------------------------------------------------------

// utils/acl.h: AclResult / AclMode constants.
#[allow(non_camel_case_types)]
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;

// utils/acl.h: ACL_MAINTAIN (privilege bit) - not yet in parsenodes.rs.
// TODO(pg-port): use the real ACL_MAINTAIN once defined.
const ACL_MAINTAIN: AclMode = 1 << 16;

// nodes/parsenodes.h ObjectType (selector for aclcheck_error).
#[allow(non_camel_case_types)]
type ObjectType = c_int;

// access/xact.h: XACT_FLAGS_ACCESSEDTEMPNAMESPACE bit.
const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: c_int = 1 << 0;

// access/xact.h: MyXactFlags - per-transaction flag accumulator.
// TODO(pg-port): real definition lives in access/transam/xact.c (a global).
static mut MyXactFlags: c_int = 0;

// utils/syscache.h: syscache id for pg_class indexed by OID.
// TODO(pg-port): replace with the real RELOID constant once syscache.h is ported.
const RELOID: c_int = 57;

// catalog/namespace.h: RangeVarGetRelidExtended option flag bits.
const RVR_NOWAIT: c_int = 1 << 1;

/// catalog/namespace.h: RangeVarGetRelidCallback function-pointer type.
type RangeVarGetRelidCallback =
    unsafe fn(rv: *const RangeVar, relid: Oid, oldrelid: Oid, arg: *mut c_void);

/// STUB: catalog/namespace.h RangeVarGetRelidExtended.
// TODO(pg-port): catalog/namespace.c not ported.
unsafe fn RangeVarGetRelidExtended(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _flags: c_int,
    _callback: Option<RangeVarGetRelidCallback>,
    _callback_arg: *mut c_void,
) -> Oid {
    unimplemented!("TODO(pg-port): RangeVarGetRelidExtended (catalog/namespace.c not ported)")
}

/// STUB: utils/lsyscache.h get_rel_relkind.
// TODO(pg-port): utils/cache/lsyscache.c not ported.
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    unimplemented!("TODO(pg-port): get_rel_relkind (utils/cache/lsyscache.c not ported)")
}

/// STUB: utils/lsyscache.h get_rel_persistence.
// TODO(pg-port): utils/cache/lsyscache.c not ported.
unsafe fn get_rel_persistence(_relid: Oid) -> c_char {
    unimplemented!("TODO(pg-port): get_rel_persistence (utils/cache/lsyscache.c not ported)")
}

/// STUB: utils/lsyscache.h get_rel_name.
// TODO(pg-port): utils/cache/lsyscache.c not ported.
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    unimplemented!("TODO(pg-port): get_rel_name (utils/cache/lsyscache.c not ported)")
}

/// STUB: catalog/objectaddress.h get_relkind_objtype.
// TODO(pg-port): catalog/objectaddress.c not ported.
unsafe fn get_relkind_objtype(_relkind: c_char) -> ObjectType {
    unimplemented!("TODO(pg-port): get_relkind_objtype (catalog/objectaddress.c not ported)")
}

/// STUB: utils/acl.h aclcheck_error.
// TODO(pg-port): utils/adt/acl.c not ported.
unsafe fn aclcheck_error(
    _aclerr: AclResult,
    _objtype: ObjectType,
    _objectname: *const c_char,
) {
    unimplemented!("TODO(pg-port): aclcheck_error (utils/adt/acl.c not ported)")
}

/// STUB: utils/acl.h pg_class_aclcheck.
// TODO(pg-port): utils/adt/acl.c not ported.
unsafe fn pg_class_aclcheck(_table_oid: Oid, _role_id: Oid, _mode: AclMode) -> AclResult {
    unimplemented!("TODO(pg-port): pg_class_aclcheck (utils/adt/acl.c not ported)")
}

/// STUB: catalog/pg_inherits.h find_all_inheritors.
// TODO(pg-port): catalog/pg_inherits.c not ported.
unsafe fn find_all_inheritors(
    _parent_relid: Oid,
    _lockmode: LOCKMODE,
    _numparents: *mut *mut List,
) -> *mut List {
    unimplemented!("TODO(pg-port): find_all_inheritors (catalog/pg_inherits.c not ported)")
}

/// STUB: storage/lmgr.h LockRelationOid.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn LockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    unimplemented!("TODO(pg-port): LockRelationOid (storage/lmgr.c not ported)")
}

/// STUB: storage/lmgr.h ConditionalLockRelationOid.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn ConditionalLockRelationOid(_relid: Oid, _lockmode: LOCKMODE) -> bool {
    unimplemented!("TODO(pg-port): ConditionalLockRelationOid (storage/lmgr.c not ported)")
}

/// STUB: storage/lmgr.h UnlockRelationOid.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    unimplemented!("TODO(pg-port): UnlockRelationOid (storage/lmgr.c not ported)")
}

/// STUB: utils/syscache.h SearchSysCacheExists1.
// TODO(pg-port): utils/cache/syscache.c not ported.
unsafe fn SearchSysCacheExists1(_cache_id: c_int, _key1: Datum) -> bool {
    unimplemented!("TODO(pg-port): SearchSysCacheExists1 (utils/cache/syscache.c not ported)")
}

/// STUB: rewrite/rewriteHandler.h get_view_query.
// TODO(pg-port): rewrite/rewriteHandler.c not ported.
unsafe fn get_view_query(_view: Relation) -> *mut Query {
    unimplemented!("TODO(pg-port): get_view_query (rewrite/rewriteHandler.c not ported)")
}

/// STUB: utils/rel.h RelationHasSecurityInvoker.
// TODO(pg-port): inspects view->rd_options ViewOptions->security_invoker.
unsafe fn RelationHasSecurityInvoker(_relation: Relation) -> bool {
    unimplemented!("TODO(pg-port): RelationHasSecurityInvoker (utils/rel.h not ported)")
}

// ---------------------------------------------------------------------------
// LOCK TABLE
// ---------------------------------------------------------------------------
pub unsafe fn LockTableCommand(lockstmt: *mut LockStmt) {
    // Iterate over the list and process the named relations one at a time.
    foreach!(p, (*lockstmt).relations, {
        let rv = lfirst(current_cell!(p)) as *mut RangeVar;
        let recurse = (*rv).inh;

        let reloid = RangeVarGetRelidExtended(
            rv,
            (*lockstmt).mode,
            if (*lockstmt).nowait { RVR_NOWAIT } else { 0 },
            Some(RangeVarCallbackForLockTable),
            &mut (*lockstmt).mode as *mut c_int as *mut c_void,
        );

        if get_rel_relkind(reloid) == RELKIND_VIEW {
            LockViewRecurse(reloid, (*lockstmt).mode, (*lockstmt).nowait, NIL);
        } else if recurse {
            LockTableRecurse(reloid, (*lockstmt).mode, (*lockstmt).nowait);
        }
    });
}

// ---------------------------------------------------------------------------
// Before acquiring a table lock on the named table, check whether we have
// permission to do so.
// ---------------------------------------------------------------------------
unsafe fn RangeVarCallbackForLockTable(
    rv: *const RangeVar,
    relid: Oid,
    _oldrelid: Oid,
    arg: *mut c_void,
) {
    let lockmode: LOCKMODE = *(arg as *mut LOCKMODE);
    let relkind: c_char;
    let relpersistence: c_char;
    let aclresult: AclResult;

    if !OidIsValid(relid) {
        return; // doesn't exist, so no permissions check
    }
    relkind = get_rel_relkind(relid);
    if relkind == 0 {
        return; // woops, concurrently dropped; no permissions check
    }

    // Currently, we only allow plain tables or views to be locked.
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_PARTITIONED_TABLE
        && relkind != RELKIND_VIEW
    {
        ereport!(
            ERROR,
            "cannot lock relation" // errcode WRONG_OBJECT_TYPE; relname (*rv).relname; errdetail_relkind_not_supported
        );
    }

    // Make note if a temporary relation has been accessed in this transaction.
    relpersistence = get_rel_persistence(relid);
    if relpersistence == RELPERSISTENCE_TEMP {
        MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    }

    // Check permissions.
    aclresult = LockTableAclCheck(relid, lockmode, GetUserId());
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            get_relkind_objtype(get_rel_relkind(relid)),
            (*rv).relname,
        );
    }
}

// ---------------------------------------------------------------------------
// Apply LOCK TABLE recursively over an inheritance tree
//
// This doesn't check permission to perform LOCK TABLE on the child tables,
// because getting here means that the user has permission to lock the parent
// which is enough.
// ---------------------------------------------------------------------------
unsafe fn LockTableRecurse(reloid: Oid, lockmode: LOCKMODE, nowait: bool) {
    let children: *mut List = find_all_inheritors(reloid, NoLock, null_mut());

    foreach!(lc, children, {
        let childreloid = lfirst_oid(current_cell!(lc));

        // Parent already locked.
        if childreloid == reloid {
            continue;
        }

        if !nowait {
            LockRelationOid(childreloid, lockmode);
        } else if !ConditionalLockRelationOid(childreloid, lockmode) {
            // try to throw error by name; relation could be deleted...
            let relname = get_rel_name(childreloid);

            if relname.is_null() {
                continue; // child concurrently dropped, just skip it
            }
            ereport!(
                ERROR,
                "could not obtain lock on relation" // errcode LOCK_NOT_AVAILABLE; relname
            );
        }

        // Even if we got the lock, child might have been concurrently dropped.
        // If so, we can skip it.
        if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(childreloid)) {
            // Release useless lock
            UnlockRelationOid(childreloid, lockmode);
            continue;
        }
    });
}

// ---------------------------------------------------------------------------
// Apply LOCK TABLE recursively over a view
//
// All tables and views appearing in the view definition query are locked
// recursively with the same lock mode.
// ---------------------------------------------------------------------------
#[repr(C)]
struct LockViewRecurse_context {
    lockmode: LOCKMODE,         // lock mode to use
    nowait: bool,               // no wait mode
    check_as_user: Oid,         // user for checking the privilege
    viewoid: Oid,               // OID of the view to be locked
    ancestor_views: *mut List,  // OIDs of ancestor views
}

unsafe fn LockViewRecurse_walker(
    node: *mut Node,
    context: *mut LockViewRecurse_context,
) -> bool {
    if node.is_null() {
        return false;
    }

    if IsA!(node, T_Query) {
        let query = node as *mut Query;

        foreach!(rtable, (*query).rtable, {
            let rte = lfirst(current_cell!(rtable)) as *mut RangeTblEntry;
            let aclresult: AclResult;

            let relid = (*rte).relid;
            let relkind = (*rte).relkind;
            let relname = get_rel_name(relid);

            // Currently, we only allow plain tables or views to be locked.
            if relkind != RELKIND_RELATION
                && relkind != RELKIND_PARTITIONED_TABLE
                && relkind != RELKIND_VIEW
            {
                continue;
            }

            // We might be dealing with a self-referential view.  If so, we can
            // just stop recursing, since we already locked it.
            if list_member_oid((*context).ancestor_views, relid) {
                continue;
            }

            // Check permissions as the specified user.  This will either be the
            // view owner or the current user.
            aclresult =
                LockTableAclCheck(relid, (*context).lockmode, (*context).check_as_user);
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, get_relkind_objtype(relkind), relname);
            }

            // We have enough rights to lock the relation; do so.
            if !(*context).nowait {
                LockRelationOid(relid, (*context).lockmode);
            } else if !ConditionalLockRelationOid(relid, (*context).lockmode) {
                ereport!(
                    ERROR,
                    "could not obtain lock on relation" // errcode LOCK_NOT_AVAILABLE; relname
                );
            }

            if relkind == RELKIND_VIEW {
                LockViewRecurse(
                    relid,
                    (*context).lockmode,
                    (*context).nowait,
                    (*context).ancestor_views,
                );
            } else if (*rte).inh {
                LockTableRecurse(relid, (*context).lockmode, (*context).nowait);
            }
        });

        return query_tree_walker(
            query,
            Some(LockViewRecurse_walker_thunk),
            context as *mut c_void,
            QTW_IGNORE_JOINALIASES,
        );
    }

    expression_tree_walker(
        node,
        Some(LockViewRecurse_walker_thunk),
        context as *mut c_void,
    )
}

// Thunk adapting LockViewRecurse_walker to the generic tree_walker callback
// signature `unsafe fn(*mut Node, *mut c_void) -> bool`.
unsafe fn LockViewRecurse_walker_thunk(node: *mut Node, context: *mut c_void) -> bool {
    LockViewRecurse_walker(node, context as *mut LockViewRecurse_context)
}

unsafe fn LockViewRecurse(
    reloid: Oid,
    lockmode: LOCKMODE,
    nowait: bool,
    ancestor_views: *mut List,
) {
    let mut context: LockViewRecurse_context = LockViewRecurse_context {
        lockmode,
        nowait,
        check_as_user: InvalidOid,
        viewoid: InvalidOid,
        ancestor_views: null_mut(),
    };
    let view: Relation;
    let viewquery: *mut Query;

    // caller has already locked the view
    view = table_open(reloid, NoLock);
    viewquery = get_view_query(view);

    // If the view has the security_invoker property set, check permissions as
    // the current user.  Otherwise, check permissions as the view owner.
    context.lockmode = lockmode;
    context.nowait = nowait;
    if RelationHasSecurityInvoker(view) {
        context.check_as_user = GetUserId();
    } else {
        context.check_as_user = (*(*view).rd_rel).relowner;
    }
    context.viewoid = reloid;
    context.ancestor_views = lappend_oid(ancestor_views, reloid);

    LockViewRecurse_walker(viewquery as *mut Node, &mut context);

    context.ancestor_views = list_delete_last(context.ancestor_views);

    table_close(view, NoLock);
}

// ---------------------------------------------------------------------------
// Check whether the current user is permitted to lock this relation.
// ---------------------------------------------------------------------------
unsafe fn LockTableAclCheck(reloid: Oid, lockmode: LOCKMODE, userid: Oid) -> AclResult {
    let aclresult: AclResult;
    let mut aclmask: AclMode;

    // any of these privileges permit any lock mode
    aclmask = ACL_MAINTAIN | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;

    // SELECT privileges also permit ACCESS SHARE and below
    if lockmode <= AccessShareLock {
        aclmask |= ACL_SELECT;
    }

    // INSERT privileges also permit ROW EXCLUSIVE and below
    if lockmode <= RowExclusiveLock {
        aclmask |= ACL_INSERT;
    }

    aclresult = pg_class_aclcheck(reloid, userid, aclmask);

    aclresult
}
