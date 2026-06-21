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
    SharedSecLabelRelationId, SubscriptionRelationId, TableSpaceRelationId, TypeRelationId,
};
use crate::access::htup_details::HeapTuple;
use crate::{PG_GETARG_NAME, PG_GETARG_OID, PG_RETURN_OID, PG_RETURN_VOID};
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

// Real ported dependencies.
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, ScanKey, SysScanDesc,
};
use crate::access::htup_details::HeapTupleIsValid;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::transam::varsup::{GetNewObjectId, StopGeneratingPinnedObjectIds};
use crate::miscadmin::{superuser, IsBootstrapProcessingMode};
// utils/time/snapmgr.h: extern SnapshotData SnapshotAnyData; (only its address is
// taken, then cast to the genam Snapshot = *mut c_void).
// TODO(pg-port): wire utils/time/snapmgr
#[repr(C)]
struct SnapshotAnyDataStub {
    _opaque: [u8; 0],
}
static mut SnapshotAnyData: SnapshotAnyDataStub = SnapshotAnyDataStub { _opaque: [] };

// RelationData accessors (utils/rel.h macros).
unsafe fn RelationGetRelid(relation: Relation) -> Oid {
    (*relation).rd_id
}
unsafe fn RelationGetNamespace(relation: Relation) -> Oid {
    (*(*relation).rd_rel).relnamespace
}
unsafe fn RelationGetRelationName(relation: Relation) -> *const c_char {
    (*(*relation).rd_rel).relname.data.as_ptr()
}

// IsBinaryUpgrade (miscadmin.h); only used in an Assert in GetNewOidWithIndex.
extern "C" {
    static mut IsBinaryUpgrade: bool;
}

// utils/fmgroids.h: oideq() regproc OID.
// TODO(pg-port): import from a generated fmgroids module.
const F_OIDEQ: Oid = 184;

// catalog/pg_class index + oid-column constants for GetNewRelFileNumber.
// TODO(pg-port): hoist into a generated indexing/attribute OID module.
const ClassOidIndexId: Oid = 2662;
const Anum_pg_class_oid: AttrNumber = 1;

// GetNewRelFileNumber dependencies.
use crate::common::relpath::{GetRelationPath, RelPathStr, MAIN_FORKNUM};
use crate::storage::procnumber::ProcNumberForTempRelations;
use crate::storage::relfilelocator::RelFileLocatorBackend;
use crate::catalog::pg_known_oids::GLOBALTABLESPACE_OID;

use crate::miscadmin::{MyDatabaseId, MyDatabaseTableSpace};

pub type ProcNumber = c_int;
const INVALID_PROC_NUMBER: ProcNumber = -1;
const InvalidRelFileNumber: RelFileNumber = 0; /* InvalidOid */

extern "C" {
    fn access(path: *const c_char, mode: c_int) -> c_int;
}
const F_OK: c_int = 0;

// storage/smgr.h: relpath(rlocator, forknum) =
//   GetRelationPath(rlocator.locator.dbOid, rlocator.locator.spcOid,
//                   rlocator.locator.relNumber, rlocator.backend, forknum)
#[inline]
unsafe fn relpath(rlocator: RelFileLocatorBackend, forknum: c_int) -> RelPathStr {
    GetRelationPath(
        rlocator.locator.dbOid,
        rlocator.locator.spcOid,
        rlocator.locator.relNumber,
        rlocator.backend,
        forknum,
    )
}

// miscadmin.h. Local (non-exported) to avoid clashing with other modules'
// crate-level CHECK_FOR_INTERRUPTS; the real handler is not ported.
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{}};
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
pub unsafe fn IsSystemRelation(relation: Relation) -> bool {
    IsSystemClass(RelationGetRelid(relation), (*relation).rd_rel)
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
pub unsafe fn IsCatalogRelation(relation: Relation) -> bool {
    IsCatalogRelationOid(RelationGetRelid(relation))
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
#[no_mangle]
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
pub unsafe fn IsInplaceUpdateRelation(relation: Relation) -> bool {
    IsInplaceUpdateOid(RelationGetRelid(relation))
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
pub unsafe fn IsToastRelation(relation: Relation) -> bool {
    /*
     * What we actually check is whether the relation belongs to a pg_toast
     * namespace.  This should be equivalent because of restrictions that are
     * enforced elsewhere against creating user relations in, or moving
     * relations into/out of, a pg_toast namespace.
     */
    IsToastNamespace(RelationGetNamespace(relation))
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
fn isTempToastNamespace(namespaceId: Oid) -> bool { unsafe { crate::catalog::namespace::isTempToastNamespace(namespaceId) } }

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
#[no_mangle]
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
pub unsafe fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: AttrNumber) -> Oid {
    let mut newOid: Oid;
    let mut scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut collides: bool;
    let mut retries: u64 = 0;
    let mut retries_before_log: u64 = GETNEWOID_LOG_THRESHOLD;

    /* Only system relations are supported */
    Assert!(IsSystemRelation(relation));

    /* In bootstrap mode, we don't have any indexes to use */
    if IsBootstrapProcessingMode() {
        return GetNewObjectId();
    }

    /*
     * We should never be asked to generate a new pg_type OID during
     * pg_upgrade; doing so would risk collisions with the OIDs it wants to
     * assign.  Hitting this assert means there's some path where we failed to
     * ensure that a type OID is determined by commands in the dump script.
     */
    Assert!(!IsBinaryUpgrade || RelationGetRelid(relation) != TypeRelationId);

    /* Generate new OIDs until we find one not in the table */
    loop {
        CHECK_FOR_INTERRUPTS!();

        newOid = GetNewObjectId();

        ScanKeyInit(
            &mut key,
            oidcolumn,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(newOid),
        );

        /* see notes above about using SnapshotAny */
        scan = systable_beginscan(
            relation,
            indexId,
            true,
            (&raw mut SnapshotAnyData) as *mut c_void,
            1,
            &mut key,
        );

        collides = HeapTupleIsValid(systable_getnext(scan) as HeapTuple);

        systable_endscan(scan);

        /*
         * Log that we iterate more than GETNEWOID_LOG_THRESHOLD but have not
         * yet found OID unused in the relation. Then repeat logging with
         * exponentially increasing intervals until we iterate more than
         * GETNEWOID_LOG_MAX_INTERVAL. Finally repeat logging every
         * GETNEWOID_LOG_MAX_INTERVAL unless an unused OID is found. This
         * logic is necessary not to fill up the server log with the similar
         * messages.
         */
        if retries >= retries_before_log {
            ereport!(
                LOG,
                errmsg!(
                    "still searching for an unused OID in relation \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy()
                )
            );
            /* C also: errdetail_plural("OID candidates have been checked %" PRIu64 " time(s), but no unused OID has been found yet.", retries) */

            /*
             * Double the number of retries to do before logging next until it
             * reaches GETNEWOID_LOG_MAX_INTERVAL.
             */
            if retries_before_log * 2 <= GETNEWOID_LOG_MAX_INTERVAL {
                retries_before_log *= 2;
            } else {
                retries_before_log += GETNEWOID_LOG_MAX_INTERVAL;
            }
        }

        retries += 1;

        if !collides {
            break;
        }
    }

    /*
     * If at least one log message is emitted, also log the completion of OID
     * assignment.
     */
    if retries > GETNEWOID_LOG_THRESHOLD {
        ereport!(
            LOG,
            errmsg!(
                "new OID has been assigned in relation \"{}\" after {} retries",
                std::ffi::CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy(),
                retries
            )
        );
    }

    newOid
}

/*
 * GetNewRelFileNumber
 *		Generate a new relfilenumber that is unique within the
 *		database of the given tablespace.
 */
pub unsafe fn GetNewRelFileNumber(
    reltablespace: Oid,
    pg_class: Relation,
    relpersistence: c_char,
) -> RelFileNumber {
    let mut rlocator: RelFileLocatorBackend = core::mem::zeroed();
    let mut rpath: RelPathStr;
    let mut collides: bool;
    let procNumber: ProcNumber;

    /*
     * If we ever get here during pg_upgrade, there's something wrong; all
     * relfilenumber assignments during a binary-upgrade run should be
     * determined by commands in the dump script.
     */
    Assert!(!IsBinaryUpgrade);

    match relpersistence {
        RELPERSISTENCE_TEMP => {
            procNumber = ProcNumberForTempRelations();
        }
        RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => {
            procNumber = INVALID_PROC_NUMBER;
        }
        _ => {
            elog!(ERROR, "invalid relpersistence: {}", relpersistence as u8 as char);
            return InvalidRelFileNumber; /* placate compiler */
        }
    }

    /* This logic should match RelationInitPhysicalAddr */
    rlocator.locator.spcOid = if reltablespace != InvalidOid {
        reltablespace
    } else {
        MyDatabaseTableSpace
    };
    rlocator.locator.dbOid = if rlocator.locator.spcOid == GLOBALTABLESPACE_OID {
        InvalidOid
    } else {
        MyDatabaseId
    };

    /*
     * The relpath will vary based on the backend number, so we must
     * initialize that properly here to make sure that any collisions based on
     * filename are properly detected.
     */
    rlocator.backend = procNumber;

    loop {
        CHECK_FOR_INTERRUPTS!();

        /* Generate the OID */
        if !pg_class.is_null() {
            rlocator.locator.relNumber =
                GetNewOidWithIndex(pg_class, ClassOidIndexId, Anum_pg_class_oid);
        } else {
            rlocator.locator.relNumber = GetNewObjectId();
        }

        /* Check for existing file of same name */
        rpath = relpath(rlocator, MAIN_FORKNUM);

        if access(rpath.str.as_ptr(), F_OK) == 0 {
            /* definite collision */
            collides = true;
        } else {
            /*
             * Here we have a little bit of a dilemma: if errno is something
             * other than ENOENT, should we declare a collision and loop? In
             * practice it seems best to go ahead regardless of the errno.  If
             * there is a colliding file we will get an smgr failure when we
             * attempt to create the new relation file.
             */
            collides = false;
        }

        if !collides {
            break;
        }
    }

    rlocator.locator.relNumber
}

// fmgr / relcache / syscache dependencies for the two SQL-callable wrappers.
use crate::access::htup_details::GETSTRUCT;
use crate::access::index::indexam::{index_close, index_open};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_type_d::OIDOID;
use crate::storage::lockdefs::RowExclusiveLock;
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCacheAttName};

// utils/rel.h: IndexRelationGetNumberOfKeyAttributes(relation).
// TODO(pg-port): not exported as a real fn anywhere; reads rd_index->indnkeyatts.
unsafe fn IndexRelationGetNumberOfKeyAttributes(relation: Relation) -> c_int {
    (*(*relation).rd_index).indnkeyatts as c_int
}

// rd_index->indkey is the int2vector that begins the variable-length region of
// the pg_index tuple, immediately after the fixed FormData_pg_index fields
// (not modeled in catalog::pg_index::FormData_pg_index).  Read indkey.values[i]
// at that offset, matching the C in-memory layout.
unsafe fn IndexRelationGetKeyAttno(relation: Relation, i: usize) -> AttrNumber {
    use crate::c::int2vector;
    use crate::catalog::pg_index::FormData_pg_index;
    let base = (*relation).rd_index as *const u8;
    let indkey = base.add(core::mem::size_of::<FormData_pg_index>()) as *const int2vector;
    *(*indkey).values.as_ptr().add(i)
}

/*
 * SQL callable interface for GetNewOidWithIndex().  Outside of initdb's
 * direct insertions into catalog tables, and recovering from corruption, this
 * should rarely be needed.
 *
 * Function is intentionally not documented in the user facing docs.
 */
pub unsafe fn pg_nextoid(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let attname: Name = PG_GETARG_NAME!(fcinfo, 1);
    let idxoid: Oid = PG_GETARG_OID!(fcinfo, 2);
    let rel: Relation;
    let idx: Relation;
    let atttuple: HeapTuple;
    let attform: Form_pg_attribute;
    let attno: AttrNumber;
    let newoid: Oid;

    /*
     * As this function is not intended to be used during normal running, and
     * only supports system catalogs (which require superuser permissions to
     * modify), just checking for superuser ought to not obstruct valid
     * usecases.
     */
    if !superuser() {
        ereport!(
            ERROR,
            errmsg!("must be superuser to call {}()", "pg_nextoid")
        );
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    rel = table_open(reloid, RowExclusiveLock);
    idx = index_open(idxoid, RowExclusiveLock);

    if !IsSystemRelation(rel) {
        ereport!(
            ERROR,
            errmsg!("pg_nextoid() can only be used on system catalogs")
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    if (*(*idx).rd_index).indrelid != RelationGetRelid(rel) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" does not belong to table \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(idx)).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    atttuple = SearchSysCacheAttName(reloid, NameStr(&*attname));
    if !HeapTupleIsValid(atttuple) {
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(NameStr(&*attname)).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
    }

    attform = GETSTRUCT(atttuple) as Form_pg_attribute;
    attno = (*attform).attnum;

    if (*attform).atttypid != OIDOID {
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" is not of type oid",
                std::ffi::CStr::from_ptr(NameStr(&*attname)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    if IndexRelationGetNumberOfKeyAttributes(idx) != 1
        || IndexRelationGetKeyAttno(idx, 0) != attno
    {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" is not the index for column \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(idx)).to_string_lossy(),
                std::ffi::CStr::from_ptr(NameStr(&*attname)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    newoid = GetNewOidWithIndex(rel, idxoid, attno);

    ReleaseSysCache(atttuple);
    table_close(rel, RowExclusiveLock);
    index_close(idx, RowExclusiveLock);

    PG_RETURN_OID!(newoid);
}

/*
 * SQL callable interface for StopGeneratingPinnedObjectIds().
 *
 * This is only to be used by initdb, so it's intentionally not documented in
 * the user facing docs.
 */
pub unsafe fn pg_stop_making_pinned_objects(_fcinfo: *mut c_void) -> Datum {
    /*
     * Belt-and-suspenders check, since StopGeneratingPinnedObjectIds will
     * fail anyway in non-single-user mode.
     */
    if !superuser() {
        ereport!(
            ERROR,
            errmsg!(
                "must be superuser to call {}()",
                "pg_stop_making_pinned_objects"
            )
        );
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    StopGeneratingPinnedObjectIds();

    PG_RETURN_VOID!();
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
