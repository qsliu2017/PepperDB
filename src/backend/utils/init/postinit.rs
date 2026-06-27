//! Translated from PostgreSQL src/backend/utils/init/postinit.c
//!
//! Backend initialization. PG's `InitPostgres` runs as one long function inside
//! a freshly forked process; here it is split to match the async-task lifecycle:
//!
//! - [`backend_task_init`] is the early, catalog-free slice: assign the synthetic
//!   proc-pid, start time and backend type, and create the per-task
//!   [`Session`] (identity only; database and user unset). It is the analog of
//!   PG's `InitProcessGlobals` + the `BaseInit` local-subsystem setup that does
//!   not need the catalog, auth, or proc array. It must run on its own.
//!
//! - [`init_postgres`] runs `backend_task_init` then the deferred
//!   connect-to-database / authentication sequence, calling the existing stubs
//!   for the subsystems that are not part of this step (proc array, relcache,
//!   catalog, auth, GUC). Those calls are deliberately left on
//!   `unimplemented!()` stubs.
//!
//! - [`initialize_session_user_id`] / [`initialize_session_user_id_standalone`]
//!   set the session's user fields. The standalone (bootstrap / no-auth) variant
//!   uses the bootstrap superuser.

use crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID;
use crate::miscadmin::{is_bootstrap_processing_mode, BackendType, InitPgFlags};
use crate::postgres_ext::Oid;
use crate::session::{self, Session};
use std::sync::Arc;

/// Early per-task init: identity only, no catalog / auth / proc-array. Mirrors
/// PG `InitProcessGlobals` (start time) plus the identity assignment that
/// happened implicitly at fork. Returns the new [`Session`]; the caller publishes
/// it with [`session::scope`] for the rest of the backend's life.
///
/// Async because later steps will add I/O here (e.g. file-access init); it
/// performs no `.await` yet and touches no deferred subsystem.
pub async fn backend_task_init(backend_type: BackendType) -> Arc<Session> {
    // PG assigns MyProcPid/MyStartTime[stamp]/MyBackendType in
    // InitProcessGlobals + the postmaster child setup; Session::new bundles the
    // synthetic proc-pid and the start time, and records the backend type.
    Arc::new(Session::new(backend_type))
}

/// PG `BaseInit` -- process-local storage/buffer/lock/xlog-insert setup that runs
/// even for auxiliary processes. TODO(startup): wire the per-task access init
/// (smgrinit / InitBufferManagerAccess / InitXLogInsert / InitLockManagerAccess)
/// into the backend/aux startup sequence; the subsystems themselves now exist.
pub fn base_init() {
    // The per-task access-init calls are not yet sequenced here; the local buffer
    // pool / smgr cache / xloginsert staging are lazily scoped by their callers.
}

/// PG `InitPostgres`: full backend startup -- identity, then the deferred
/// connect-to-database + authentication sequence. Returns the resolved database
/// name (PG's `out_dbname`).
///
/// The deferred phases below land on existing stubs and are NOT implemented in
/// this step:
/// - `InitProcess` / proc-array attach (step 15)
/// - `BaseInit` local subsystems (steps 12-15)
/// - relation/catalog cache init (relcache, deferred)
/// - `PerformAuthentication` (auth, deferred)
/// - `CheckMyDatabase` (catalog, deferred)
///
/// `initialize_session_user_id*` (this step) set the resolved user on the
/// session at the end.
#[allow(unused_variables)]
pub async fn init_postgres(
    backend_type: BackendType,
    in_dbname: Option<&str>,
    dboid: Oid,
    username: Option<&str>,
    useroid: Oid,
    flags: InitPgFlags,
) -> String {
    let bootstrap = is_bootstrap_processing_mode();

    // Early, catalog-free identity slice.
    let session = backend_task_init(backend_type).await;

    session::scope(session, async move {
        // Deferred: attach to proc array (step 15), pgstat, sinval, ProcSignal.
        // init_process_phase2(); shared_inval_backend_init(); proc_signal_init();

        // Deferred: process-local subsystem setup.
        base_init();

        // Deferred: relcache / catalog cache / plan cache / portal manager.
        // relation_cache_initialize(); init_catalog_cache(); init_plan_cache();
        // relation_cache_initialize_phase2();

        // Authentication + user identity. Bootstrap / standalone use a fixed
        // superuser id; the normal path authenticates then resolves the role.
        if bootstrap {
            initialize_session_user_id_standalone();
        } else {
            // Deferred: perform_authentication(my_proc_port);
            initialize_session_user_id(username, useroid, false);
        }

        // Deferred: resolve the database (CheckMyDatabase against pg_database),
        // set up relcache phase 3, ACL framework, client encoding, GUC settings.
        // check_my_database(dbname, am_superuser, override_allow_connections);

        in_dbname.unwrap_or("").to_string()
    })
    .await
}

/// PG `InitializeSessionUserId`: resolve `username`/`useroid` to a role and set
/// the session's authenticated/session/current user. The catalog lookup of
/// `pg_authid` is deferred; until it lands we set the ids directly from the
/// caller-supplied `useroid` (the role-row fields -- canlogin, connlimit,
/// rolsuper -- come from the catalog and are TODO(catalog)).
pub fn initialize_session_user_id(_username: Option<&str>, roleid: Oid, _bypass_login_check: bool) {
    debug_assert!(!is_bootstrap_processing_mode());
    // TODO(catalog): SearchSysCache1(AUTHNAME/AUTHOID) to fetch rolname/rolsuper
    // and enforce rolcanlogin / rolconnlimit.
    let s = session::current();
    s.set_authenticated_user_id(roleid);
    // is_superuser is a catalog property; unknown without pg_authid -> false.
    set_session_authorization(roleid, false);
}

/// PG `InitializeSessionUserIdStandalone`: bootstrap / no-auth path. Uses the
/// bootstrap superuser and marks the session superuser.
pub fn initialize_session_user_id_standalone() {
    let s = session::current();
    debug_assert_eq!(s.authenticated_user_id(), crate::postgres_ext::InvalidOid);
    s.set_authenticated_user_id(BOOTSTRAP_SUPERUSERID);
    set_session_authorization(BOOTSTRAP_SUPERUSERID, true);
    crate::backend::utils::init::miscinit::set_current_role_id(
        crate::postgres_ext::InvalidOid,
        false,
    );
}

/// PG `SetSessionAuthorization` (re-exported from miscinit for the init flow).
fn set_session_authorization(userid: Oid, is_superuser: bool) {
    crate::backend::utils::init::miscinit::set_session_authorization(userid, is_superuser);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backend_task_init_populates_identity_without_catalog() {
        let s = backend_task_init(BackendType::BACKEND).await;
        assert_eq!(s.backend_type(), BackendType::BACKEND);
        assert!(s.proc_pid() > 0);
        assert!(s.start_timestamp() != 0);
        // No catalog/auth touched: user/db remain unset.
        assert_eq!(s.database_id(), crate::postgres_ext::InvalidOid);
        assert_eq!(s.authenticated_user_id(), crate::postgres_ext::InvalidOid);
    }

    #[tokio::test]
    async fn backend_task_init_assigns_distinct_proc_pids() {
        let a = backend_task_init(BackendType::BACKEND).await;
        let b = backend_task_init(BackendType::BACKEND).await;
        assert_ne!(a.proc_pid(), b.proc_pid());
    }

    #[tokio::test]
    async fn standalone_session_user_is_bootstrap_superuser() {
        let s = backend_task_init(BackendType::STANDALONE_BACKEND).await;
        session::scope(s, async {
            initialize_session_user_id_standalone();
            let cur = session::current();
            assert_eq!(cur.authenticated_user_id(), BOOTSTRAP_SUPERUSERID);
            assert_eq!(cur.session_user_id(), BOOTSTRAP_SUPERUSERID);
            assert_eq!(cur.current_user_id(), BOOTSTRAP_SUPERUSERID);
            assert!(cur.session_user_is_superuser());
        })
        .await;
    }
}
