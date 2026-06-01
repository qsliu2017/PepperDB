//! Translation of postgres/src/backend/catalog/catalog.c
//!
//! Routines concerned with catalog naming conventions and other bits of
//! hard-wired knowledge.
//!
//! #include mapping:
//!   postgres.h               -> crate::prelude::*
//!   <fcntl.h>, <unistd.h>    -> libc (only needed by GetNewRelFileNumber, STUB)
//!   access/genam.h           -> STUB (systable scans not ported)
//!   access/htup_details.h    -> STUB (GETSTRUCT / HeapTuple not ported here)
//!   access/table.h           -> STUB (table_open/close not ported)
//!   access/transam.h         -> FirstUnpinnedObjectId/FirstGenbkiObjectId/FirstNormalObjectId
//!                               (defined locally below; transam.rs only has the XID layer.
//!                               TODO(pg-port): hoist these to crate::access::transam)
//!   catalog/catalog.h        -> this file
//!   catalog/namespace.h      -> isTempToastNamespace is a local STUB (namespace.c not ported)
//!   catalog/pg_*.h           -> relation OIDs from crate::catalog::catalog_oids; namespace OIDs
//!                               from crate::catalog::pg_known_oids; shared-catalog index/toast
//!                               OIDs defined locally below (exact values from the
//!                               DECLARE_*/DECLARE_TOAST lines in the corresponding *.h headers)
//!   catalog/pg_class (Form)  -> crate::catalog::pg_class::{FormData_pg_class, Form_pg_class}
//!   miscadmin.h              -> STUB (IsBootstrapProcessingMode / superuser not ported)
//!   utils/fmgroids.h         -> STUB (F_OIDEQ etc. not needed in the ported paths)
//!   utils/fmgrprotos.h       -> STUB (PG_FUNCTION_ARGS interface not ported)
//!   utils/rel.h              -> STUB (RelationData accessors not ported)
//!   utils/snapmgr.h          -> STUB (SnapshotAny not ported)
//!   utils/syscache.h         -> STUB (SearchSysCache not ported)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::catalog::catalog_oids::{
    AuthIdRelationId, AuthMemRelationId, DatabaseRelationId, DbRoleSettingRelationId,
    LargeObjectRelationId, NamespaceRelationId, ParameterAclRelationId, RelationRelationId,
    ReplicationOriginRelationId, SharedDependRelationId, SharedDescriptionRelationId,
    SharedSecLabelRelationId, SubscriptionRelationId, TableSpaceRelationId,
};
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_known_oids::{PG_CATALOG_NAMESPACE, PG_PUBLIC_NAMESPACE, PG_TOAST_NAMESPACE};

// The relcache `Relation` type. Used only by the four `*Relation`-taking
// wrappers, which are stubbed below (RelationData / rd_rel are not ported).
use crate::nodes::execnodes::Relation;

// RelFileNumber for the stubbed GetNewRelFileNumber signature.
use crate::common::relpath::RelFileNumber;

// AttrNumber for the GetNewOidWithIndex signature (not in the prelude).
use crate::nodes::primnodes::AttrNumber;

// RELPERSISTENCE_* for the GetNewRelFileNumber relpersistence switch.
use crate::catalog::pg_class::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED};

extern "C" {
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
}

// ----------------------------------------------------------------
// Constants from access/transam.h (the C build pulls these in via
// #include "access/transam.h"). src/access/transam.rs currently only
// translates the XID layer, so define them locally.
// TODO(pg-port): hoist FirstGenbkiObjectId/FirstUnpinnedObjectId/FirstNormalObjectId
// into crate::access::transam.
// ----------------------------------------------------------------
pub const FirstGenbkiObjectId: Oid = 10000;
pub const FirstUnpinnedObjectId: Oid = 12000;
pub const FirstNormalObjectId: Oid = 16384;

// ----------------------------------------------------------------
// Shared-catalog INDEX and TOAST OIDs referenced by IsSharedRelation /
// IsCatalogTextUniqueIndexOid. These live in the various catalog/pg_*.h
// headers as DECLARE_UNIQUE_INDEX / DECLARE_INDEX / DECLARE_TOAST_WITH_MACRO
// lines; the only catalog *relation* OIDs in catalog_oids.rs are the CATALOG()
// ones, so the index/toast OIDs are defined locally here, copied 1:1 from the
// headers (value in parentheses on each DECLARE line).
// TODO(pg-port): hoist these into a generated indexing/toast OID module.
// ----------------------------------------------------------------

/* pg_authid (pg_authid.h) */
const AuthIdRolnameIndexId: Oid = 2676;
const AuthIdOidIndexId: Oid = 2677;

/* pg_auth_members (pg_auth_members.h) */
const AuthMemOidIndexId: Oid = 6303;
const AuthMemRoleMemIndexId: Oid = 2694;
const AuthMemMemRoleIndexId: Oid = 2695;
const AuthMemGrantorIndexId: Oid = 6302;

/* pg_database (pg_database.h) */
const DatabaseNameIndexId: Oid = 2671;
const DatabaseOidIndexId: Oid = 2672;
const PgDatabaseToastTable: Oid = 4177;
const PgDatabaseToastIndex: Oid = 4178;

/* pg_db_role_setting (pg_db_role_setting.h) */
const DbRoleSettingDatidRolidIndexId: Oid = 2965;
const PgDbRoleSettingToastTable: Oid = 2966;
const PgDbRoleSettingToastIndex: Oid = 2967;

/* pg_parameter_acl (pg_parameter_acl.h) */
const ParameterAclParnameIndexId: Oid = 6246;
const ParameterAclOidIndexId: Oid = 6247;
const PgParameterAclToastTable: Oid = 6244;
const PgParameterAclToastIndex: Oid = 6245;

/* pg_replication_origin (pg_replication_origin.h) */
const ReplicationOriginIdentIndex: Oid = 6001;
const ReplicationOriginNameIndex: Oid = 6002;

/* pg_shdepend (pg_shdepend.h) */
const SharedDependDependerIndexId: Oid = 1232;
const SharedDependReferenceIndexId: Oid = 1233;

/* pg_shdescription (pg_shdescription.h) */
const SharedDescriptionObjIndexId: Oid = 2397;
const PgShdescriptionToastTable: Oid = 2846;
const PgShdescriptionToastIndex: Oid = 2847;

/* pg_shseclabel (pg_shseclabel.h) */
const SharedSecLabelObjectIndexId: Oid = 3593;
const PgShseclabelToastTable: Oid = 4060;
const PgShseclabelToastIndex: Oid = 4061;

/* pg_seclabel (pg_seclabel.h) - SecLabelObjectIndexId used by IsCatalogTextUniqueIndexOid */
const SecLabelObjectIndexId: Oid = 3597;

/* pg_subscription (pg_subscription.h) */
const SubscriptionObjectIndexId: Oid = 6114;
const SubscriptionNameIndexId: Oid = 6115;
const PgSubscriptionToastTable: Oid = 4183;
const PgSubscriptionToastIndex: Oid = 4184;

/* pg_tablespace (pg_tablespace.h) */
const TablespaceOidIndexId: Oid = 2697;
const TablespaceNameIndexId: Oid = 2698;
const PgTablespaceToastTable: Oid = 4185;
const PgTablespaceToastIndex: Oid = 4186;

// TODO(pg-port): ERRCODE_* from utils/errcodes.h. The errcode() shim ignores
// the value; kept for fidelity of the stubbed SQL-callable paths.
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

/*
 * Parameters to determine when to emit a log message in GetNewOidWithIndex()
 */
const GETNEWOID_LOG_THRESHOLD: u64 = 1000000;
const GETNEWOID_LOG_MAX_INTERVAL: u64 = 128000000;

/*
 * IsSystemRelation
 *		True iff the relation is either a system catalog or a toast table.
 *		See IsCatalogRelation for the exact definition of a system catalog.
 *
 *		We treat toast tables of user relations as "system relations" for
 *		protection purposes, e.g. you can't change their schemas without
 *		special permissions.  Therefore, most uses of this function are
 *		checking whether allow_system_table_mods restrictions apply.
 *		For other purposes, consider whether you shouldn't be using
 *		IsCatalogRelation instead.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
// STUB: needs RelationGetRelid / rd_rel from the relcache (utils/rel.h), not ported.
//   return IsSystemClass(RelationGetRelid(relation), relation->rd_rel);
pub fn IsSystemRelation(_relation: Relation) -> bool {
    // TODO(pg-port): needs relcache RelationData (RelationGetRelid, rd_rel).
    unimplemented!()
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
pub fn IsSystemClass(relid: Oid, reltuple: Form_pg_class) -> bool {
    /* IsCatalogRelationOid is a bit faster, so test that first */
    IsCatalogRelationOid(relid) || IsToastClass(reltuple)
}

/*
 * IsCatalogRelation
 *		True iff the relation is a system catalog.
 *
 *		By a system catalog, we mean one that is created during the bootstrap
 *		phase of initdb.  That includes not just the catalogs per se, but
 *		also their indexes, and TOAST tables and indexes if any.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
// STUB: needs RelationGetRelid from the relcache (utils/rel.h), not ported.
//   return IsCatalogRelationOid(RelationGetRelid(relation));
pub fn IsCatalogRelation(_relation: Relation) -> bool {
    // TODO(pg-port): needs relcache RelationData (RelationGetRelid).
    unimplemented!()
}

/*
 * IsCatalogRelationOid
 *		True iff the relation identified by this OID is a system catalog.
 *
 *		By a system catalog, we mean one that is created during the bootstrap
 *		phase of initdb.  That includes not just the catalogs per se, but
 *		also their indexes, and TOAST tables and indexes if any.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
pub fn IsCatalogRelationOid(relid: Oid) -> bool {
    /*
     * We consider a relation to be a system catalog if it has a pinned OID.
     * This includes all the defined catalogs, their indexes, and their TOAST
     * tables and indexes.
     *
     * This rule excludes the relations in information_schema, which are not
     * integral to the system and can be treated the same as user relations.
     *
     * This test is reliable since an OID wraparound will skip this range of
     * OIDs; see GetNewObjectId().
     */
    (relid as Oid) < FirstUnpinnedObjectId
}

/*
 * IsCatalogTextUniqueIndexOid
 *		True iff the relation identified by this OID is a catalog UNIQUE index
 *		having a column of type "text".
 *
 *		The relcache must not use these indexes.  See the C source for the full
 *		rationale (self-deadlock avoidance via a hard-coded list).
 */
pub fn IsCatalogTextUniqueIndexOid(relid: Oid) -> bool {
    relid == ParameterAclParnameIndexId
        || relid == ReplicationOriginNameIndex
        || relid == SecLabelObjectIndexId
        || relid == SharedSecLabelObjectIndexId
}

/*
 * IsInplaceUpdateRelation
 *		True iff core code performs inplace updates on the relation.
 */
// STUB: needs RelationGetRelid from the relcache (utils/rel.h), not ported.
//   return IsInplaceUpdateOid(RelationGetRelid(relation));
pub fn IsInplaceUpdateRelation(_relation: Relation) -> bool {
    // TODO(pg-port): needs relcache RelationData (RelationGetRelid).
    unimplemented!()
}

/*
 * IsInplaceUpdateOid
 *		Like the above, but takes an OID as argument.
 */
pub fn IsInplaceUpdateOid(relid: Oid) -> bool {
    relid == RelationRelationId || relid == DatabaseRelationId
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 *
 *		Does not perform any catalog accesses.
 */
// STUB: needs RelationGetNamespace from the relcache (utils/rel.h), not ported.
//   return IsToastNamespace(RelationGetNamespace(relation));
pub fn IsToastRelation(_relation: Relation) -> bool {
    // TODO(pg-port): needs relcache RelationData (RelationGetNamespace).
    unimplemented!()
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
pub fn IsToastClass(reltuple: Form_pg_class) -> bool {
    // SAFETY: reltuple is a pointer to a FormData_pg_class fixed part, exactly
    // as the C code dereferences `reltuple->relnamespace`.
    let relnamespace = unsafe { (*reltuple).relnamespace };
    IsToastNamespace(relnamespace)
}

/*
 * IsCatalogNamespace
 *		True iff namespace is pg_catalog.
 *
 *		Does not perform any catalog accesses.
 */
pub fn IsCatalogNamespace(namespaceId: Oid) -> bool {
    namespaceId == PG_CATALOG_NAMESPACE
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast or my temporary-toast-table namespace.
 *
 *		Does not perform any catalog accesses.
 */
pub fn IsToastNamespace(namespaceId: Oid) -> bool {
    (namespaceId == PG_TOAST_NAMESPACE) || isTempToastNamespace(namespaceId)
}

/*
 * isTempToastNamespace (catalog/namespace.c)
 *		True iff the namespace is the current backend's temp-toast-table
 *		namespace.
 */
// STUB: namespace.c (which tracks the backend's temp namespace state) is not
// ported. Returns false so the PG_TOAST_NAMESPACE arm of IsToastNamespace stays
// correct; only the per-backend temp-toast case is unimplemented.
// TODO(pg-port): real isTempToastNamespace in catalog/namespace.c.
fn isTempToastNamespace(_namespaceId: Oid) -> bool {
    false
}

/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 *
 *		For some classes of objects, the prefix pg_ is reserved for
 *		system objects only.
 */
pub fn IsReservedName(name: *const c_char) -> bool {
    /* ugly coding for speed: compare against the literal "pg_" prefix */
    unsafe { strncmp(name, c"pg_".as_ptr(), 3) == 0 }
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * The set of shared relations is fairly static, so a hand-maintained list of
 * their OIDs is used (see the C source for the full rationale).
 */
pub fn IsSharedRelation(relationId: Oid) -> bool {
    /* These are the shared catalogs (look for BKI_SHARED_RELATION) */
    if relationId == AuthIdRelationId
        || relationId == AuthMemRelationId
        || relationId == DatabaseRelationId
        || relationId == DbRoleSettingRelationId
        || relationId == ParameterAclRelationId
        || relationId == ReplicationOriginRelationId
        || relationId == SharedDependRelationId
        || relationId == SharedDescriptionRelationId
        || relationId == SharedSecLabelRelationId
        || relationId == SubscriptionRelationId
        || relationId == TableSpaceRelationId
    {
        return true;
    }
    /* These are their indexes */
    if relationId == AuthIdOidIndexId
        || relationId == AuthIdRolnameIndexId
        || relationId == AuthMemMemRoleIndexId
        || relationId == AuthMemRoleMemIndexId
        || relationId == AuthMemOidIndexId
        || relationId == AuthMemGrantorIndexId
        || relationId == DatabaseNameIndexId
        || relationId == DatabaseOidIndexId
        || relationId == DbRoleSettingDatidRolidIndexId
        || relationId == ParameterAclOidIndexId
        || relationId == ParameterAclParnameIndexId
        || relationId == ReplicationOriginIdentIndex
        || relationId == ReplicationOriginNameIndex
        || relationId == SharedDependDependerIndexId
        || relationId == SharedDependReferenceIndexId
        || relationId == SharedDescriptionObjIndexId
        || relationId == SharedSecLabelObjectIndexId
        || relationId == SubscriptionNameIndexId
        || relationId == SubscriptionObjectIndexId
        || relationId == TablespaceNameIndexId
        || relationId == TablespaceOidIndexId
    {
        return true;
    }
    /* These are their toast tables and toast indexes */
    if relationId == PgDatabaseToastTable
        || relationId == PgDatabaseToastIndex
        || relationId == PgDbRoleSettingToastTable
        || relationId == PgDbRoleSettingToastIndex
        || relationId == PgParameterAclToastTable
        || relationId == PgParameterAclToastIndex
        || relationId == PgShdescriptionToastTable
        || relationId == PgShdescriptionToastIndex
        || relationId == PgShseclabelToastTable
        || relationId == PgShseclabelToastIndex
        || relationId == PgSubscriptionToastTable
        || relationId == PgSubscriptionToastIndex
        || relationId == PgTablespaceToastTable
        || relationId == PgTablespaceToastIndex
    {
        return true;
    }
    false
}

/*
 * IsPinnedObject
 *		Given the class + OID identity of a database object, report whether
 *		it is "pinned", that is not droppable because the system requires it.
 *
 * We used to represent this explicitly in pg_depend, but that proved to be
 * an undesirable amount of overhead, so now we rely on an OID range test.
 */
pub fn IsPinnedObject(classId: Oid, objectId: Oid) -> bool {
    /*
     * Objects with OIDs above FirstUnpinnedObjectId are never pinned.  Since
     * the OID generator skips this range when wrapping around, this check
     * guarantees that user-defined objects are never considered pinned.
     */
    if objectId >= FirstUnpinnedObjectId {
        return false;
    }

    /*
     * Large objects are never pinned.  We need this special case because
     * their OIDs can be user-assigned.
     */
    if classId == LargeObjectRelationId {
        return false;
    }

    /*
     * There are a few objects defined in the catalog .dat files that, as a
     * matter of policy, we prefer not to treat as pinned.  (If the user does
     * indeed drop and recreate them, they'll have new but certainly-unpinned
     * OIDs, so no problem.)
     *
     * Checking both classId and objectId is overkill, since OIDs below
     * FirstGenbkiObjectId should be globally unique, but do it anyway for
     * robustness.
     */

    /* the public namespace is not pinned */
    if classId == NamespaceRelationId && objectId == PG_PUBLIC_NAMESPACE {
        return false;
    }

    /*
     * Databases are never pinned.  (Intentional: template0/template1 can be
     * rebuilt from each other, serving as mutual backups.)
     */
    if classId == DatabaseRelationId {
        return false;
    }

    /*
     * All other initdb-created objects are pinned.
     */
    true
}

/*
 * GetNewOidWithIndex
 *		Generate a new OID that is unique within the system relation.
 *
 * Caller must have a suitable lock on the relation.
 */
// STUB: needs systable scans (access/genam.h), GetNewObjectId/CHECK_FOR_INTERRUPTS,
// bootstrap-mode tests (miscadmin.h), and SnapshotAny (utils/snapmgr.h) - none ported.
//
// C body (preserved for the eventual port):
//   Oid newOid; SysScanDesc scan; ScanKeyData key; bool collides;
//   uint64 retries = 0; uint64 retries_before_log = GETNEWOID_LOG_THRESHOLD;
//   Assert(IsSystemRelation(relation));
//   if (IsBootstrapProcessingMode()) return GetNewObjectId();
//   Assert(!IsBinaryUpgrade || RelationGetRelid(relation) != TypeRelationId);
//   do {
//       CHECK_FOR_INTERRUPTS();
//       newOid = GetNewObjectId();
//       ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(newOid));
//       scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
//       collides = HeapTupleIsValid(systable_getnext(scan));
//       systable_endscan(scan);
//       if (retries >= retries_before_log) { ereport(LOG, ...); ... }
//       retries++;
//   } while (collides);
//   if (retries > GETNEWOID_LOG_THRESHOLD) { ereport(LOG, ...); }
//   return newOid;
pub fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: AttrNumber) -> Oid {
    let _ = (GETNEWOID_LOG_THRESHOLD, GETNEWOID_LOG_MAX_INTERVAL);
    // TODO(pg-port): needs access/genam systable scans, GetNewObjectId, SnapshotAny.
    unimplemented!()
}

/*
 * GetNewRelFileNumber
 *		Generate a new relfilenumber that is unique within the
 *		database of the given tablespace.
 */
// STUB: needs RelFileLocatorBackend, relpath(), MyDatabaseId/MyDatabaseTableSpace,
// ProcNumberForTempRelations, and filesystem access() - none ported.
//
// C body (preserved): see catalog.c GetNewRelFileNumber - switch on relpersistence
// to pick a ProcNumber, build a RelFileLocatorBackend, then loop calling
// GetNewOidWithIndex()/GetNewObjectId() until relpath() names a file that doesn't
// exist (access(rpath.str, F_OK)).
pub fn GetNewRelFileNumber(
    _reltablespace: Oid,
    _pg_class: Relation,
    relpersistence: c_char,
) -> RelFileNumber {
    match relpersistence {
        RELPERSISTENCE_TEMP | RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => {}
        _ => {
            elog!(ERROR, "invalid relpersistence: {}", relpersistence as u8 as char);
        }
    }
    // TODO(pg-port): needs RelFileLocatorBackend, relpath(), MyDatabaseId, access().
    unimplemented!()
}

/*
 * pg_nextoid
 *		SQL callable interface for GetNewOidWithIndex().
 */
// STUB: needs the fmgr PG_FUNCTION_ARGS calling convention, superuser(), table_open/
// index_open, SearchSysCacheAttName/GETSTRUCT, and the relcache - none ported.
//
// C body (preserved): superuser() check; table_open(reloid)/index_open(idxoid);
// IsSystemRelation(rel) check; verify idx belongs to rel; look up the named attr via
// SearchSysCacheAttName + GETSTRUCT; verify it is type OID and is the sole index key;
// newoid = GetNewOidWithIndex(rel, idxoid, attno); release/close; PG_RETURN_OID(newoid).
pub fn pg_nextoid(_fcinfo: *mut c_void) -> Datum {
    let _ = ERRCODE_INVALID_PARAMETER_VALUE;
    let _ = ERRCODE_INSUFFICIENT_PRIVILEGE;
    // TODO(pg-port): needs fmgr SRF interface, syscache, table/index_open, relcache.
    unimplemented!()
}

/*
 * pg_stop_making_pinned_objects
 *		SQL callable interface for StopGeneratingPinnedObjectIds().
 */
// STUB: needs superuser() and StopGeneratingPinnedObjectIds() - not ported.
//
// C body (preserved):
//   if (!superuser()) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
//       errmsg("must be superuser to call %s()", "pg_stop_making_pinned_objects")));
//   StopGeneratingPinnedObjectIds();
//   PG_RETURN_VOID();
pub fn pg_stop_making_pinned_objects(_fcinfo: *mut c_void) -> Datum {
    let _ = ERRCODE_INSUFFICIENT_PRIVILEGE;
    // TODO(pg-port): needs fmgr SRF interface, superuser(), StopGeneratingPinnedObjectIds.
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_relation_oid_range() {
        // pg_class is a pinned catalog.
        assert!(IsCatalogRelationOid(RelationRelationId)); // 1259
        assert!(IsCatalogRelationOid(1259));
        // FirstNormalObjectId is a user OID; just below FirstUnpinnedObjectId is catalog.
        assert!(!IsCatalogRelationOid(16384));
        assert!(!IsCatalogRelationOid(FirstUnpinnedObjectId)); // 12000, exclusive bound
        assert!(IsCatalogRelationOid(FirstUnpinnedObjectId - 1)); // 11999
        assert!(IsCatalogRelationOid(0));
    }

    #[test]
    fn reserved_name() {
        assert!(IsReservedName(c"pg_foo".as_ptr()));
        assert!(IsReservedName(c"pg_".as_ptr()));
        assert!(!IsReservedName(c"foo".as_ptr()));
        assert!(!IsReservedName(c"p".as_ptr())); // shorter than prefix, must not match
        assert!(!IsReservedName(c"PG_foo".as_ptr())); // case-sensitive
    }

    #[test]
    fn shared_relation_list() {
        // Shared catalog.
        assert!(IsSharedRelation(DatabaseRelationId)); // 1262
        assert!(IsSharedRelation(1262));
        assert!(IsSharedRelation(AuthIdRelationId)); // 1260
        // Shared index.
        assert!(IsSharedRelation(DatabaseOidIndexId)); // 2672
        // Shared toast.
        assert!(IsSharedRelation(PgDatabaseToastTable)); // 4177
        // Non-shared: pg_class is a local catalog.
        assert!(!IsSharedRelation(RelationRelationId)); // 1259
        assert!(!IsSharedRelation(16384));
    }

    #[test]
    fn namespace_classifiers() {
        assert!(IsCatalogNamespace(PG_CATALOG_NAMESPACE)); // 11
        assert!(!IsCatalogNamespace(PG_TOAST_NAMESPACE)); // 99
        assert!(!IsCatalogNamespace(PG_PUBLIC_NAMESPACE)); // 2200
        assert!(IsToastNamespace(PG_TOAST_NAMESPACE)); // 99
        assert!(!IsToastNamespace(PG_CATALOG_NAMESPACE)); // 11
    }

    #[test]
    fn inplace_update_and_text_unique_index() {
        assert!(IsInplaceUpdateOid(RelationRelationId)); // pg_class
        assert!(IsInplaceUpdateOid(DatabaseRelationId)); // pg_database
        assert!(!IsInplaceUpdateOid(AuthIdRelationId));
        assert!(IsCatalogTextUniqueIndexOid(ParameterAclParnameIndexId));
        assert!(IsCatalogTextUniqueIndexOid(SharedSecLabelObjectIndexId));
        assert!(!IsCatalogTextUniqueIndexOid(DatabaseOidIndexId));
    }

    #[test]
    fn pinned_object_logic() {
        // User-range OID is never pinned.
        assert!(!IsPinnedObject(RelationRelationId, FirstUnpinnedObjectId));
        assert!(!IsPinnedObject(RelationRelationId, 16384));
        // Large objects never pinned.
        assert!(!IsPinnedObject(LargeObjectRelationId, 5000));
        // public namespace not pinned.
        assert!(!IsPinnedObject(NamespaceRelationId, PG_PUBLIC_NAMESPACE));
        // Databases never pinned.
        assert!(!IsPinnedObject(DatabaseRelationId, Template1DbOid_for_test()));
        // A normal low-OID catalog object is pinned.
        assert!(IsPinnedObject(NamespaceRelationId, PG_CATALOG_NAMESPACE));
    }

    // template1 has OID 1; keep the test self-contained without importing it.
    fn Template1DbOid_for_test() -> Oid {
        1
    }

    #[test]
    fn is_system_class_and_toast_class() {
        use crate::catalog::pg_class::FormData_pg_class;
        // Build a zeroed FormData_pg_class and set relnamespace to drive IsToastClass.
        let mut form: FormData_pg_class = unsafe { core::mem::zeroed() };
        form.relnamespace = PG_TOAST_NAMESPACE;
        assert!(IsToastClass(&mut form as Form_pg_class));
        // A pg_catalog-namespaced row is not a toast class, but a pinned OID makes it
        // a system class.
        form.relnamespace = PG_CATALOG_NAMESPACE;
        assert!(!IsToastClass(&mut form as Form_pg_class));
        assert!(IsSystemClass(RelationRelationId, &mut form as Form_pg_class)); // pinned OID
        // A user-range OID in pg_catalog namespace: not toast, not pinned -> not system.
        assert!(!IsSystemClass(16384, &mut form as Form_pg_class));
        // A user-range OID in toast namespace is a system class via IsToastClass.
        form.relnamespace = PG_TOAST_NAMESPACE;
        assert!(IsSystemClass(16384, &mut form as Form_pg_class));
    }
}
