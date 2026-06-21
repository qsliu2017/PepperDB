//! access/common/relation.c - generic relation_ open/close routines.
//!
//! These routines implement access to relations (tables, indexes, etc).
//! Support that's specific to subtypes of relations lives in their own files.

use crate::prelude::*;

// Relation / RelationData / LockRelId live in utils/rel.rs (the real
// #[repr(C)] RelationData with the utils/rel.h field layout).
use crate::utils::rel::{LockRelId, Relation, RelationIsValid};

// RangeVar is a PORTED parsenode (nodes/primnodes.h, via primnodes.rs).
use crate::nodes::primnodes::RangeVar;

// LOCKMODE and lock-level constants (storage/lockdefs.h).
use crate::storage::lockdefs::{AccessShareLock, NoLock, LOCKMODE};

// Bootstrap-mode predicate (miscadmin.h).
use crate::miscadmin::IsBootstrapProcessingMode;

// pgstat hook for relations (pgstat.h / pgstat_relation.c).
use crate::utils::activity::pgstat_relation::pgstat_init_relation;

// ObjectIdGetDatum (postgres.h) - re-exported via prelude's crate::postgres::*,
// but spelled out here for clarity.
use crate::postgres::ObjectIdGetDatum;

// ----------------------------------------------------------------------------
// Local stubs for not-yet-ported callees.
// ----------------------------------------------------------------------------

/// storage/lock.h: maximum number of lock modes (NoLock .. MAX_LOCKMODES-1).
const MAX_LOCKMODES: c_int = 10;

/// storage/syscache.h: syscache id for pg_class indexed by OID.
// TODO(pg-port): replace with the real RELOID constant once syscache.h is ported.
const RELOID: c_int = 57;

/// access/xact.h: XACT_FLAGS_ACCESSEDTEMPNAMESPACE bit.
const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: c_int = 1 << 0;

/// access/xact.h: MyXactFlags - per-transaction flag accumulator.
// TODO(pg-port): real definition lives in access/transam/xact.c (a global).
static mut MyXactFlags: c_int = 0;

/// STUB: storage/lmgr.h LockRelationOid.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn LockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    crate::storage::lmgr::lmgr::LockRelationOid(_relid, _lockmode)
}

/// STUB: storage/lmgr.h UnlockRelationOid.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    crate::storage::lmgr::lmgr::UnlockRelationOid(_relid, _lockmode)
}

/// STUB: storage/lmgr.h UnlockRelationId.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn UnlockRelationId(_relid: *mut LockRelId, _lockmode: LOCKMODE) {
    crate::storage::lmgr::lmgr::UnlockRelationId(_relid as _, _lockmode)
}

/// STUB: storage/lmgr.h CheckRelationLockedByMe.
// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn CheckRelationLockedByMe(
    _relation: Relation,
    _lockmode: LOCKMODE,
    _orstronger: bool,
) -> bool {
    crate::storage::lmgr::lmgr::CheckRelationLockedByMe(_relation as _, _lockmode, _orstronger)
}

/// STUB: utils/relcache.h RelationIdGetRelation.
// TODO(pg-port): utils/cache/relcache.c not ported.
unsafe fn RelationIdGetRelation(_relation_id: Oid) -> Relation {
    crate::utils::cache::relcache::RelationIdGetRelation(_relation_id) as _
}

/// STUB: utils/relcache.h RelationClose.
// TODO(pg-port): utils/cache/relcache.c not ported.
unsafe fn RelationClose(_relation: Relation) {
    crate::utils::cache::relcache::RelationClose(_relation as _)
}

/// STUB: utils/rel.h RelationUsesLocalBuffers.
// TODO(pg-port): real macro inspects relation->rd_rel->relpersistence == TEMP.
unsafe fn RelationUsesLocalBuffers(_relation: Relation) -> bool {
    false // non-temp catalogs do not use local buffers
}

/// STUB: catalog/namespace.h RangeVarGetRelid.
// TODO(pg-port): catalog/namespace.c not ported.
unsafe fn RangeVarGetRelid(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> Oid {
    crate::catalog::namespace::RangeVarGetRelid(_relation as _, _lockmode, _missing_ok)
}

/// STUB: storage/sinval.h AcceptInvalidationMessages.
// TODO(pg-port): storage/ipc/sinval.c not ported.
unsafe fn AcceptInvalidationMessages() {
    crate::utils::cache::inval::AcceptInvalidationMessages()
}

/// STUB: utils/syscache.h SearchSysCacheExists1.
// TODO(pg-port): utils/cache/syscache.c not ported.
unsafe fn SearchSysCacheExists1(_cache_id: c_int, _key1: Datum) -> bool { crate::utils::cache::syscache::SearchSysCacheExists1(_cache_id, _key1) }

// ----------------------------------------------------------------------------
// relation_open - open any relation by relation OID
//
// If lockmode is not "NoLock", the specified kind of lock is obtained on the
// relation.  (Generally, NoLock should only be used if the caller knows it has
// some appropriate lock on the relation already.)
//
// An error is raised if the relation does not exist.
//
// NB: a "relation" is anything with a pg_class entry.  The caller is expected
// to check whether the relkind is something it can handle.
// ----------------------------------------------------------------------------
pub unsafe fn relation_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    let r: Relation;

    Assert!(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    /* Get the lock before trying to open the relcache entry */
    if lockmode != NoLock {
        LockRelationOid(relationId, lockmode);
    }

    /* The relcache does all the real work... */
    r = RelationIdGetRelation(relationId);

    if !RelationIsValid(r) {
        elog!(ERROR, "could not open relation with OID {}", relationId);
    }

    /*
     * If we didn't get the lock ourselves, assert that caller holds one,
     * except in bootstrap mode where no locks are used.
     */
    Assert!(
        lockmode != NoLock
            || IsBootstrapProcessingMode()
            || CheckRelationLockedByMe(r, AccessShareLock, true)
    );

    /* Make note that we've accessed a temporary relation */
    if RelationUsesLocalBuffers(r) {
        MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    }

    pgstat_init_relation(r as _);

    r
}

// ----------------------------------------------------------------------------
// try_relation_open - open any relation by relation OID
//
// Same as relation_open, except return NULL instead of failing if the relation
// does not exist.
// ----------------------------------------------------------------------------
pub unsafe fn try_relation_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    let r: Relation;

    Assert!(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    /* Get the lock first */
    if lockmode != NoLock {
        LockRelationOid(relationId, lockmode);
    }

    /*
     * Now that we have the lock, probe to see if the relation really exists
     * or not.
     */
    if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)) {
        /* Release useless lock */
        if lockmode != NoLock {
            UnlockRelationOid(relationId, lockmode);
        }

        return null_mut();
    }

    /* Should be safe to do a relcache load */
    r = RelationIdGetRelation(relationId);

    if !RelationIsValid(r) {
        elog!(ERROR, "could not open relation with OID {}", relationId);
    }

    /* If we didn't get the lock ourselves, assert that caller holds one */
    Assert!(lockmode != NoLock || CheckRelationLockedByMe(r, AccessShareLock, true));

    /* Make note that we've accessed a temporary relation */
    if RelationUsesLocalBuffers(r) {
        MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    }

    pgstat_init_relation(r as _);

    r
}

// ----------------------------------------------------------------------------
// relation_openrv - open any relation specified by a RangeVar
//
// Same as relation_open, but the relation is specified by a RangeVar.
// ----------------------------------------------------------------------------
pub unsafe fn relation_openrv(relation: *const RangeVar, lockmode: LOCKMODE) -> Relation {
    let relOid: Oid;

    /*
     * Check for shared-cache-inval messages before trying to open the
     * relation.  This is needed even if we already hold a lock on the
     * relation, because GRANT/REVOKE are executed without taking any lock on
     * the target relation, and we want to be sure we see current ACL
     * information.  We can skip this if asked for NoLock, on the assumption
     * that such a call is not the first one in the current command, and so we
     * should be reasonably up-to-date already.  (XXX this all could stand to
     * be redesigned, but for the moment we'll keep doing this like it's been
     * done historically.)
     */
    if lockmode != NoLock {
        AcceptInvalidationMessages();
    }

    /* Look up and lock the appropriate relation using namespace search */
    relOid = RangeVarGetRelid(relation, lockmode, false);

    /* Let relation_open do the rest */
    relation_open(relOid, NoLock)
}

// ----------------------------------------------------------------------------
// relation_openrv_extended - open any relation specified by a RangeVar
//
// Same as relation_openrv, but with an additional missing_ok argument allowing
// a NULL return rather than an error if the relation is not found.  (Note that
// some other causes, such as permissions problems, will still result in an
// ereport.)
// ----------------------------------------------------------------------------
pub unsafe fn relation_openrv_extended(
    relation: *const RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> Relation {
    let relOid: Oid;

    /*
     * Check for shared-cache-inval messages before trying to open the
     * relation.  See comments in relation_openrv().
     */
    if lockmode != NoLock {
        AcceptInvalidationMessages();
    }

    /* Look up and lock the appropriate relation using namespace search */
    relOid = RangeVarGetRelid(relation, lockmode, missing_ok);

    /* Return NULL on not-found */
    if !OidIsValid(relOid) {
        return null_mut();
    }

    /* Let relation_open do the rest */
    relation_open(relOid, NoLock)
}

// ----------------------------------------------------------------------------
// relation_close - close any relation
//
// If lockmode is not "NoLock", we then release the specified lock.
//
// Note that it is often sensible to hold a lock beyond relation_close; in that
// case, the lock is released automatically at xact end.
// ----------------------------------------------------------------------------
pub unsafe fn relation_close(relation: Relation, lockmode: LOCKMODE) {
    let mut relid: LockRelId = (*relation).rd_lockInfo.lockRelId;

    Assert!(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    /* The relcache does the real work... */
    RelationClose(relation);

    if lockmode != NoLock {
        UnlockRelationId(&mut relid, lockmode);
    }
}
