//! Per-task session/identity state, replacing PostgreSQL's per-backend process
//! globals (the identity + user-id half of `utils/init/globals.c` and the
//! user-id statics in `miscinit.c`). No single C struct corresponds; this is the
//! per-task analog of the `Arc`-shared [`crate::shared_state::SharedState`].
//!
//! Under the single-process async model each backend is a tokio task rather than
//! a forked process, so the process globals PostgreSQL relied on become per-task
//! state. [`Session`] is published as a task-local ([`CURRENT_SESSION`]); it is a
//! sibling of the other two per-task task-locals -- `MY_PROC_SIGNAL_SLOT`
//! (interrupt slot, step 04) and `CURRENT_RESOURCE_OWNER` (resource owner, step
//! 06). Each owns its own concern; the slot and owner are NOT folded in here.
//!
//! Interior mutability: identity (`proc_pid`, start time) is fixed at task
//! creation; the user/database fields change during connection setup and `SET
//! ROLE` / security-restricted operations. Because backends run on tokio's
//! multi-thread work-stealing runtime via plain `tokio::spawn`, per-task state
//! must be `Send + Sync`: the mutable scalars use atomics and the database name
//! a `Mutex<Option<String>>`, so `Session` is `Send + Sync` and a future holding
//! `Arc<Session>` across an `.await` is `Send`.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use crate::datatype::timestamp::TimestampTz;
use crate::miscadmin::BackendType;
use crate::pgtime::pg_time_t;
use crate::postgres_ext::{InvalidOid, Oid};

/// Base value for the synthetic proc-pid counter. There is no OS `fork`, so
/// `MyProcPid` is not a real OS pid; we hand out unique per-backend ids from a
/// process-global counter. The base keeps synthetic ids clear of plausible
/// real-pid ranges for readability in logs.
const PROC_PID_BASE: i32 = 1_000_000;

/// Process-global synthetic proc-pid allocator. Each [`Session`] takes the next
/// value; ids are unique for the life of the process (no reuse).
static NEXT_PROC_PID: AtomicI32 = AtomicI32::new(PROC_PID_BASE);

/// Allocate a fresh synthetic proc-pid. Unique and monotonically increasing.
pub fn alloc_proc_pid() -> i32 {
    NEXT_PROC_PID.fetch_add(1, Ordering::Relaxed)
}

/// Per-task session state: backend identity plus the connected database and the
/// user-id stack. Holds the values that PostgreSQL kept in process globals
/// (`MyProcPid`, `MyStartTime[stamp]`, `MyBackendType`, `MyDatabaseId`, ...) and
/// the `miscinit.c` user-id statics (`AuthenticatedUserId`, `SessionUserId`,
/// `OuterUserId`, `CurrentUserId`, `SecurityRestrictionContext`).
pub struct Session {
    // --- Identity (fixed at creation, no atomic needed) ---
    /// Synthetic per-backend id (PG `MyProcPid`).
    proc_pid: i32,
    /// Backend start time, seconds since epoch (PG `MyStartTime`).
    start_time: pg_time_t,
    /// Backend start timestamp (PG `MyStartTimestamp`).
    start_timestamp: TimestampTz,
    /// Kind of backend (PG `MyBackendType`). Mutable: set during early init.
    /// Stored as the enum discriminant (`as u32`); converted at the accessor.
    backend_type: AtomicU32,

    // --- Connected database (set during connect-to-database) ---
    /// PG `MyDatabaseId` (inner `Oid` u32).
    database_id: AtomicU32,
    /// PG `MyDatabaseTableSpace` (inner `Oid` u32).
    database_tablespace: AtomicU32,
    /// PG `MyDatabaseHasLoginEventTriggers`.
    database_has_login_event_triggers: AtomicBool,
    /// The connected database name, if known. Never held across an `.await`.
    database_name: Mutex<Option<String>>,

    // --- User-id stack (miscinit.c); each is an inner `Oid` u32 ---
    /// PG `AuthenticatedUserId` -- set once at connection start, never changes.
    authenticated_user_id: AtomicU32,
    /// PG `SessionUserId` -- initially the authenticated user, changed by SET
    /// SESSION AUTHORIZATION.
    session_user_id: AtomicU32,
    /// PG `OuterUserId` -- outer-level role (SET ROLE target).
    outer_user_id: AtomicU32,
    /// PG `CurrentUserId` -- current effective user for permission checks.
    current_user_id: AtomicU32,
    /// PG `SecurityRestrictionContext` -- OR of `SECURITY_*` flags.
    sec_context: AtomicI32,
    /// PG `SessionUserIsSuperuser`.
    session_user_is_superuser: AtomicBool,
    /// PG `SetRoleIsActive` -- whether a SET ROLE is currently in effect.
    set_role_is_active: AtomicBool,

    // --- Interrupt holdoff / critical-section counters (miscadmin.h) ---
    // In PG these are per-backend (`InterruptHoldoffCount`,
    // `QueryCancelHoldoffCount`, `CritSectionCount`); a HOLD_INTERRUPTS in one
    // backend must not gate interrupt processing in another. They live here so
    // each tokio-task backend owns its own counter.
    /// PG `InterruptHoldoffCount`.
    interrupt_holdoff_count: AtomicU32,
    /// PG `QueryCancelHoldoffCount`.
    query_cancel_holdoff_count: AtomicU32,
    /// PG `CritSectionCount`.
    crit_section_count: AtomicU32,
}

impl Session {
    /// Create a session with a fresh synthetic proc-pid and the current start
    /// time, identity-only. Database and user fields are unset (`InvalidOid`);
    /// they are populated later by the connect-to-database / auth phase. No
    /// catalog, auth, or proc-array access happens here.
    pub fn new(backend_type: BackendType) -> Self {
        let start_timestamp = now_timestamptz();
        Self {
            proc_pid: alloc_proc_pid(),
            start_time: timestamptz_to_unix_secs(start_timestamp),
            start_timestamp,
            backend_type: AtomicU32::new(backend_type as u32),
            database_id: AtomicU32::new(InvalidOid.0),
            database_tablespace: AtomicU32::new(InvalidOid.0),
            database_has_login_event_triggers: AtomicBool::new(false),
            database_name: Mutex::new(None),
            authenticated_user_id: AtomicU32::new(InvalidOid.0),
            session_user_id: AtomicU32::new(InvalidOid.0),
            outer_user_id: AtomicU32::new(InvalidOid.0),
            current_user_id: AtomicU32::new(InvalidOid.0),
            sec_context: AtomicI32::new(0),
            session_user_is_superuser: AtomicBool::new(false),
            set_role_is_active: AtomicBool::new(false),
            interrupt_holdoff_count: AtomicU32::new(0),
            query_cancel_holdoff_count: AtomicU32::new(0),
            crit_section_count: AtomicU32::new(0),
        }
    }

    // --- Identity getters ---
    pub fn proc_pid(&self) -> i32 {
        self.proc_pid
    }
    pub fn start_time(&self) -> pg_time_t {
        self.start_time
    }
    pub fn start_timestamp(&self) -> TimestampTz {
        self.start_timestamp
    }
    pub fn backend_type(&self) -> BackendType {
        BackendType::from_u32(self.backend_type.load(Ordering::Relaxed))
    }
    pub fn set_backend_type(&self, t: BackendType) {
        self.backend_type.store(t as u32, Ordering::Relaxed);
    }

    // Each scalar below is an independent field with no cross-field invariant a
    // concurrent reader relies on (mutation happens within the owning task), so
    // Relaxed ordering is sufficient. The `Oid` fields store/load the inner u32.

    // --- Database ---
    pub fn database_id(&self) -> Oid {
        Oid(self.database_id.load(Ordering::Relaxed))
    }
    pub fn set_database_id(&self, oid: Oid) {
        self.database_id.store(oid.0, Ordering::Relaxed);
    }
    pub fn database_tablespace(&self) -> Oid {
        Oid(self.database_tablespace.load(Ordering::Relaxed))
    }
    pub fn set_database_tablespace(&self, oid: Oid) {
        self.database_tablespace.store(oid.0, Ordering::Relaxed);
    }
    pub fn database_has_login_event_triggers(&self) -> bool {
        self.database_has_login_event_triggers.load(Ordering::Relaxed)
    }
    pub fn set_database_has_login_event_triggers(&self, v: bool) {
        self.database_has_login_event_triggers.store(v, Ordering::Relaxed);
    }
    pub fn database_name(&self) -> Option<String> {
        self.database_name.lock().unwrap().clone()
    }
    pub fn set_database_name(&self, name: Option<String>) {
        *self.database_name.lock().unwrap() = name;
    }

    // --- User-id stack ---
    pub fn authenticated_user_id(&self) -> Oid {
        Oid(self.authenticated_user_id.load(Ordering::Relaxed))
    }
    pub fn set_authenticated_user_id(&self, oid: Oid) {
        self.authenticated_user_id.store(oid.0, Ordering::Relaxed);
    }
    pub fn session_user_id(&self) -> Oid {
        Oid(self.session_user_id.load(Ordering::Relaxed))
    }
    pub fn outer_user_id(&self) -> Oid {
        Oid(self.outer_user_id.load(Ordering::Relaxed))
    }
    pub fn current_user_id(&self) -> Oid {
        Oid(self.current_user_id.load(Ordering::Relaxed))
    }
    pub fn set_current_user_id(&self, oid: Oid) {
        self.current_user_id.store(oid.0, Ordering::Relaxed);
    }
    pub fn sec_context(&self) -> i32 {
        self.sec_context.load(Ordering::Relaxed)
    }
    pub fn set_sec_context(&self, ctx: i32) {
        self.sec_context.store(ctx, Ordering::Relaxed);
    }
    pub fn session_user_is_superuser(&self) -> bool {
        self.session_user_is_superuser.load(Ordering::Relaxed)
    }
    pub fn set_role_is_active(&self) -> bool {
        self.set_role_is_active.load(Ordering::Relaxed)
    }
    pub fn set_set_role_is_active(&self, v: bool) {
        self.set_role_is_active.store(v, Ordering::Relaxed);
    }

    /// PG `SetSessionUserId`: set the session user id + superuser flag.
    pub fn set_session_user_id(&self, oid: Oid, is_superuser: bool) {
        self.session_user_id.store(oid.0, Ordering::Relaxed);
        self.session_user_is_superuser.store(is_superuser, Ordering::Relaxed);
    }

    /// PG `SetOuterUserId`: set the outer-level user id, forcing the effective
    /// user to match. The C side also updates the `is_superuser` GUC; that is a
    /// GUC concern carried on `session_user_is_superuser` here.
    pub fn set_outer_user_id(&self, oid: Oid, _is_superuser: bool) {
        self.outer_user_id.store(oid.0, Ordering::Relaxed);
        self.current_user_id.store(oid.0, Ordering::Relaxed);
    }

    // --- Interrupt holdoff / critical-section counters ---
    // Per-task, owner-only counters; only the owning task reads/writes them and
    // never across an `.await` (they bracket sync critical sections). No
    // cross-field invariant a concurrent reader relies on, so Relaxed suffices
    // (same rationale as the other Session scalars above).
    pub fn interrupt_holdoff_count(&self) -> u32 {
        self.interrupt_holdoff_count.load(Ordering::Relaxed)
    }
    pub fn inc_interrupt_holdoff_count(&self) {
        self.interrupt_holdoff_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_interrupt_holdoff_count(&self) {
        let prev = self.interrupt_holdoff_count.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0);
    }
    pub fn query_cancel_holdoff_count(&self) -> u32 {
        self.query_cancel_holdoff_count.load(Ordering::Relaxed)
    }
    pub fn inc_query_cancel_holdoff_count(&self) {
        self.query_cancel_holdoff_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_query_cancel_holdoff_count(&self) {
        let prev = self.query_cancel_holdoff_count.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0);
    }
    pub fn crit_section_count(&self) -> u32 {
        self.crit_section_count.load(Ordering::Relaxed)
    }
    pub fn inc_crit_section_count(&self) {
        self.crit_section_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_crit_section_count(&self) {
        let prev = self.crit_section_count.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0);
    }
}

// ---------------------------------------------------------------------------
// time helpers
// ---------------------------------------------------------------------------

/// PostgreSQL epoch (2000-01-01 00:00:00 UTC) as Unix seconds.
const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;

/// Current time as a `TimestampTz` (microseconds since the PG epoch). Computed
/// directly from the system clock; `utils::timestamp::GetCurrentTimestamp` is
/// still a stub, and identity init must not depend on a deferred subsystem.
fn now_timestamptz() -> TimestampTz {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let unix_micros = dur.as_micros() as i64;
    unix_micros - PG_EPOCH_UNIX_SECS * 1_000_000
}

/// Convert a `TimestampTz` to Unix seconds (PG `timestamptz_to_time_t`).
fn timestamptz_to_unix_secs(ts: TimestampTz) -> pg_time_t {
    ts / 1_000_000 + PG_EPOCH_UNIX_SECS
}

// ---------------------------------------------------------------------------
// task_local
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// The current task's session. Published by [`scope`] for the backend task.
    /// `Arc` (not `Rc`) so a future holding it across an `.await` is `Send`.
    static CURRENT_SESSION: Arc<Session>;
}

/// The current task's session. Panics if not inside a [`scope`].
pub fn current() -> Arc<Session> {
    try_current().expect("no Session in scope for this task")
}

/// The current task's session, or `None` if not inside a [`scope`].
pub fn try_current() -> Option<Arc<Session>> {
    CURRENT_SESSION.try_with(|s| s.clone()).ok()
}

/// Run `f` (an async block) with `session` published as the task-local.
pub async fn scope<F, T>(session: Arc<Session>, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT_SESSION.scope(session, f).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_proc_pid_is_unique_and_increasing() {
        let a = alloc_proc_pid();
        let b = alloc_proc_pid();
        assert!(b > a, "proc pids must be increasing");
        assert!(a >= PROC_PID_BASE);
    }

    #[test]
    fn new_session_has_identity_and_unset_user_db() {
        let s = Session::new(BackendType::BACKEND);
        assert!(s.proc_pid() >= PROC_PID_BASE);
        assert_eq!(s.backend_type(), BackendType::BACKEND);
        assert!(s.start_timestamp() != 0);
        // user/db unset until the connect/auth phase.
        assert_eq!(s.database_id(), InvalidOid);
        assert_eq!(s.current_user_id(), InvalidOid);
    }

    #[test]
    fn user_id_setters_round_trip() {
        let s = Session::new(BackendType::BACKEND);
        s.set_session_user_id(Oid(42), true);
        s.set_outer_user_id(Oid(42), true);
        assert_eq!(s.session_user_id(), Oid(42));
        assert_eq!(s.outer_user_id(), Oid(42));
        assert_eq!(s.current_user_id(), Oid(42));
        assert!(s.session_user_is_superuser());
    }

    #[tokio::test]
    async fn task_local_current_within_and_outside_scope() {
        assert!(try_current().is_none(), "no session outside scope");
        let s = Arc::new(Session::new(BackendType::BACKEND));
        let pid = s.proc_pid();
        let got = scope(s, async { current().proc_pid() }).await;
        assert_eq!(got, pid);
        assert!(try_current().is_none(), "scope must not leak");
    }

    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn session_is_send_sync() {
        _assert_send_sync::<Session>();
        _assert_send_sync::<Arc<Session>>();
    }

    // Compiles only if the scoped future is `Send` (so `tokio::spawn` accepts it
    // on the multi-thread runtime). That is the whole point of the conversion:
    // backend futures holding `Arc<Session>` across `.await` are `Send`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scoped_session_future_is_send() {
        tokio::spawn(async move {
            let s = Arc::new(Session::new(BackendType::BACKEND));
            scope(s, async {
                let cur = current();
                cur.set_current_user_id(Oid(7));
                cur.set_database_name(Some("db".to_string()));
                tokio::task::yield_now().await;
                assert_eq!(cur.current_user_id(), Oid(7));
                assert_eq!(cur.database_name().as_deref(), Some("db"));
            })
            .await;
        })
        .await
        .unwrap();
    }
}
