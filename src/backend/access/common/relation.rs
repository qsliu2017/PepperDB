//! Generic relation open/close routines. Translated from
//! backend/access/common/relation.c.
//!
//! These wrap the relcache (`RelationIdGetRelation`/`RelationClose`) with the
//! heavyweight relation lock. Async coloring (rules.md s5): `LockRelationOid` is
//! a lock-wait leaf and is `async`, so every routine that takes a lock here is
//! `async` too. The lock-mode-checked open delegates the real work to the
//! relcache (a step-14 stub today; staged per rules.md s4).

use crate::access::xact::{set_my_xact_flags, MyXactFlags, XACT_FLAGS_ACCESSEDTEMPNAMESPACE};
use crate::backend::storage::lmgr::lmgr::{
    CheckRelationLockedByMe, LockRelationOid, UnlockRelationId, UnlockRelationOid,
};
use crate::catalog::namespace::RangeVarGetRelid;
use crate::miscadmin::is_bootstrap_processing_mode;
use crate::nodes::primnodes::RangeVar;
use crate::pgstat::pgstat_init_relation;
use crate::c::OidIsValid;
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use crate::utils::inval::AcceptInvalidationMessages;
use crate::utils::rel::relation_is_valid;
use crate::utils::relcache::{Relation, RelationClose, RelationIdGetRelation};
use crate::utils::syscache::{SearchSysCacheExists1, SysCacheIdentifier};
use crate::elog;
use crate::utils::elog::ERROR;

/// `AccessShareLock` as a raw `LOCKMODE` (the value the assert in relation.c
/// checks the relation is locked with when the caller passed `NoLock`).
const ACCESS_SHARE_LOCK: i32 = LockMode::AccessShareLock as i32;

/// `relation_open`: open any relation by OID, taking `lockmode` (unless
/// `NoLock`). Raises if the relation does not exist.
pub async fn relation_open(relation_id: Oid, lockmode: LockMode) -> Relation {
    // Get the lock before opening the relcache entry.
    if lockmode != LockMode::NoLock {
        LockRelationOid(relation_id, lockmode as i32).await;
    }

    // The relcache does the real work.
    let r = RelationIdGetRelation(relation_id).unwrap_or(core::ptr::null_mut());

    if !relation_is_valid(r) {
        elog!(ERROR, format!("could not open relation with OID {}", relation_id.0));
    }

    // If we didn't take the lock ourselves, assert the caller holds one (except
    // in bootstrap mode, which uses no locks).
    crate::assert!(
        lockmode != LockMode::NoLock
            || is_bootstrap_processing_mode()
            || CheckRelationLockedByMe(r, ACCESS_SHARE_LOCK, true)
    );

    // Note that we've accessed a temporary relation.
    // SAFETY: `r` is a valid relation (checked above).
    if unsafe { (*r).uses_local_buffers() } {
        set_my_xact_flags(MyXactFlags() | XACT_FLAGS_ACCESSEDTEMPNAMESPACE as i32);
    }

    pgstat_init_relation(r);

    r
}

/// `try_relation_open`: like [`relation_open`] but returns `None` instead of
/// failing if the relation does not exist.
pub async fn try_relation_open(relation_id: Oid, lockmode: LockMode) -> Option<Relation> {
    // Get the lock first.
    if lockmode != LockMode::NoLock {
        LockRelationOid(relation_id, lockmode as i32).await;
    }

    // Now that we hold the lock, probe whether the relation exists.
    if !SearchSysCacheExists1(SysCacheIdentifier::RELOID, ObjectIdGetDatum(relation_id)) {
        // Release the useless lock.
        if lockmode != LockMode::NoLock {
            UnlockRelationOid(relation_id, lockmode as i32);
        }
        return None;
    }

    // Safe to do a relcache load.
    let r = RelationIdGetRelation(relation_id).unwrap_or(core::ptr::null_mut());

    if !relation_is_valid(r) {
        elog!(ERROR, format!("could not open relation with OID {}", relation_id.0));
    }

    crate::assert!(
        lockmode != LockMode::NoLock || CheckRelationLockedByMe(r, ACCESS_SHARE_LOCK, true)
    );

    // SAFETY: `r` is a valid relation (checked above).
    if unsafe { (*r).uses_local_buffers() } {
        set_my_xact_flags(MyXactFlags() | XACT_FLAGS_ACCESSEDTEMPNAMESPACE as i32);
    }

    pgstat_init_relation(r);

    Some(r)
}

/// `relation_openrv`: open a relation specified by a `RangeVar`.
pub async fn relation_openrv(relation: &RangeVar, lockmode: LockMode) -> Relation {
    // Check for shared-cache-inval messages before opening (GRANT/REVOKE take no
    // lock, so we must refresh ACLs ourselves). Skipped for NoLock.
    if lockmode != LockMode::NoLock {
        AcceptInvalidationMessages();
    }

    let rel_oid =
        RangeVarGetRelid(relation, lockmode as i32, false).unwrap_or(crate::postgres_ext::InvalidOid);

    relation_open(rel_oid, LockMode::NoLock).await
}

/// `relation_openrv_extended`: like [`relation_openrv`] but `missing_ok` turns a
/// not-found relation into `None` rather than an error.
pub async fn relation_openrv_extended(
    relation: &RangeVar,
    lockmode: LockMode,
    missing_ok: bool,
) -> Option<Relation> {
    if lockmode != LockMode::NoLock {
        AcceptInvalidationMessages();
    }

    let rel_oid = RangeVarGetRelid(relation, lockmode as i32, missing_ok)
        .unwrap_or(crate::postgres_ext::InvalidOid);

    if !OidIsValid(rel_oid) {
        return None;
    }

    Some(relation_open(rel_oid, LockMode::NoLock).await)
}

/// `relation_close`: close a relation, releasing `lockmode` (unless `NoLock`).
/// Holding a lock past close is common (released at xact end), so `NoLock` is a
/// frequent argument.
pub fn relation_close(relation: Relation, lockmode: LockMode) {
    let relid = rel_lock_id(relation);

    // The relcache does the real work.
    RelationClose(relation);

    if lockmode != LockMode::NoLock {
        UnlockRelationId(&relid, lockmode as i32);
    }
}

/// The `lockRelId` carried inside a `Relation` (private; keeps the raw-pointer
/// deref out of the public `relation_close` signature).
fn rel_lock_id(relation: Relation) -> crate::utils::rel::LockRelId {
    // SAFETY: a Relation passed here is a live, open relation (PG asserts
    // RelationIsValid; rules: trust internal code).
    unsafe { (*relation).rd_lockInfo.lockRelId }
}
