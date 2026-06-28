//! Cache invalidation dispatcher. Translated from backend/utils/cache/inval.c.
//!
//! This is subtle stuff, so pay attention. When a tuple is updated or deleted,
//! the standard visibility rules consider it still valid until the next command
//! boundary, so a system-cache entry cannot simply be flushed during the update
//! or delete itself; doing so would also risk an immediate reload within the same
//! command. The correct behavior is to remember every insert, update, and delete
//! of a tuple that might live in the system caches, then perform the required
//! catcache and relcache flushes at the next command boundary. Updates are
//! recorded as a delete plus an insert. Inserted tuples must be remembered even
//! past the command so that abort can flush them, and deleted tuples so that any
//! negative entries loaded in their place can be flushed too.
//!
//! Only operations on tuples in relations that have associated catcaches need to
//! be registered, but every such operation must be, whether or not the tuple is
//! currently cached. Operations on pg_class, pg_attribute, and pg_index tuples
//! additionally queue a relcache flush for the described relation, as do
//! pg_constraint tuples for foreign keys. Catcache and relcache flush requests are
//! kept in separate lists so that all catcache flushes can be issued before
//! relcache flushes, and duplicate relcache requests for one relation are
//! collapsed. Subsystems with higher-level caches register callbacks here.
//! On a successful commit the accumulated invalidation events are broadcast to
//! other backends over the shared-invalidation message queue, after recording the
//! commit, so that they flush their own obsolete entries. A subtransaction abort
//! discards its queued events; a subtransaction commit merges them into the
//! parent's pending lists. Nontransactional changes (inplace heap updates, relmap,
//! smgr) send their invalidations immediately.
//!
//! In PepperDB the per-backend file-statics of the original become a single
//! task-local `RefCell<InvalState>`, one per backend task; the borrow is released
//! before any callback runs. The two message arrays are `Vec` backing stores and
//! the message groups index into them by range, preserving the original layout.
//! The parent-linked chain of transaction-invalidation records, allocated in the
//! top transaction memory context in PostgreSQL, becomes an owned stack whose
//! "parent" is simply the entry below the top, so that subtransaction nesting is
//! expressed by ownership rather than by manual context allocation. The
//! debug-discard-caches setting, a GUC in PostgreSQL, is a process-global atomic
//! reached through an accessor. Logical-decoding WAL emission of invalidations is
//! not yet implemented.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]
#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct; staged until GETSTRUCT lands"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "CacheInvalidate* take raw Relation/HeapTuple pointers per the C API; deref is faithful to C"
)]

use std::cell::RefCell;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::access::htup::HeapTuple;
use crate::catalog::catalog::{IsCatalogRelation, IsSharedRelation, IsToastRelation};
use crate::catalog::pg_attribute::AttributeRelationId;
use crate::catalog::pg_class::RelationRelationId;
use crate::catalog::pg_constraint::{CONSTRAINT_FOREIGN, ConstraintRelationId};
use crate::catalog::pg_index::IndexRelationId;
use crate::c::OidIsValid;
use crate::miscadmin::is_bootstrap_processing_mode;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::relfilelocator::RelFileLocatorBackend;
use crate::storage::sinval::{
    SharedInvalCatalogMsg, SharedInvalCatcacheMsg, SharedInvalRelSyncMsg, SharedInvalRelcacheMsg,
    SharedInvalRelmapMsg, SharedInvalSmgrMsg, SharedInvalSnapshotMsg, SharedInvalidationMessage,
    SHAREDINVALCATALOG_ID, SHAREDINVALRELCACHE_ID, SHAREDINVALRELMAP_ID, SHAREDINVALRELSYNC_ID,
    SHAREDINVALSMGR_ID, SHAREDINVALSNAPSHOT_ID,
};
use crate::utils::relcache::{AssertCouldGetRelation, Relation};

// inval.h fn-pointer typedefs (kept by the header).
use crate::utils::inval::{
    RelSyncCallbackFunction, RelcacheCallbackFunction, SyscacheCallbackFunction,
};

// ---------------------------------------------------------------------------
// Pending-request representation (file header comment in the C source)
// ---------------------------------------------------------------------------

/// Index of the catcache subgroup within a message group / the array pair.
const CAT_CACHE_MSGS: usize = 0;
/// Index of the relcache subgroup.
const REL_CACHE_MSGS: usize = 1;

/// Control information for one logical group of messages. `firstmsg`/`nextmsg`
/// index into `InvalState::inval_message_arrays` (one entry per subgroup).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct InvalidationMsgsGroup {
    firstmsg: [i32; 2],
    nextmsg: [i32; 2],
}

impl InvalidationMsgsGroup {
    /// PG `SetSubGroupToFollow`: empty subgroup starting where `prior` ends.
    fn set_subgroup_to_follow(&mut self, prior: &Self, subgroup: usize) {
        self.firstmsg[subgroup] = prior.nextmsg[subgroup];
        self.nextmsg[subgroup] = prior.nextmsg[subgroup];
    }

    /// PG `SetGroupToFollow`.
    fn set_group_to_follow(&mut self, prior: &Self) {
        self.set_subgroup_to_follow(prior, CAT_CACHE_MSGS);
        self.set_subgroup_to_follow(prior, REL_CACHE_MSGS);
    }

    /// PG `NumMessagesInSubGroup`.
    fn num_in_subgroup(&self, subgroup: usize) -> i32 {
        self.nextmsg[subgroup] - self.firstmsg[subgroup]
    }

    /// PG `NumMessagesInGroup`.
    fn num_in_group(&self) -> i32 {
        self.num_in_subgroup(CAT_CACHE_MSGS) + self.num_in_subgroup(REL_CACHE_MSGS)
    }
}

/// Fields common to both transactional and inplace invalidation.
#[derive(Debug, Clone, Copy, Default)]
struct InvalidationInfo {
    /// Events emitted by current command.
    current_cmd_invalid_msgs: InvalidationMsgsGroup,
    /// init file must be invalidated?
    relcache_init_file_inval: bool,
}

/// Subclass adding fields specific to transactional invalidation. The parent
/// link is implicit: the entry below this one on the stack.
#[derive(Debug, Clone, Copy, Default)]
struct TransInvalidationInfo {
    /// Base class.
    ii: InvalidationInfo,
    /// Events emitted by previous commands of this (sub)transaction.
    prior_cmd_invalid_msgs: InvalidationMsgsGroup,
    /// Subtransaction nesting depth.
    my_level: i32,
}

const MAX_SYSCACHE_CALLBACKS: usize = 64;
const MAX_RELCACHE_CALLBACKS: usize = 10;
const MAX_RELSYNC_CALLBACKS: usize = 10;

/// PG's `struct SYSCACHECALLBACK`.
#[derive(Clone, Copy)]
struct SyscacheCallback {
    id: i16,
    link: i16, // next callback index+1 for same cache, or 0
    function: SyscacheCallbackFunction,
    arg: Datum,
}

#[derive(Clone, Copy)]
struct RelcacheCallback {
    function: RelcacheCallbackFunction,
    arg: Datum,
}

#[derive(Clone, Copy)]
struct RelsyncCallback {
    function: RelSyncCallbackFunction,
    arg: Datum,
}

/// All of inval.c's file-statics, gathered per backend task.
struct InvalState {
    /// The two dense backing arrays (CatCacheMsgs=0, RelCacheMsgs=1). Groups index
    /// into these; PG kept them in TopTransactionContext.
    inval_message_arrays: [Vec<SharedInvalidationMessage>; 2],
    /// The TransInvalidationInfo stack (top is last). Empty == PG's NULL.
    trans_inval_info: Vec<TransInvalidationInfo>,
    /// Inplace-update control info, if assembling one.
    inplace_inval_info: Option<InvalidationInfo>,

    syscache_callback_list: Vec<SyscacheCallback>,
    /// Per-cacheid head: callback index+1, or 0 for none.
    syscache_callback_links: Vec<i16>,
    relcache_callback_list: Vec<RelcacheCallback>,
    relsync_callback_list: Vec<RelsyncCallback>,
}

impl InvalState {
    fn new() -> Self {
        Self {
            inval_message_arrays: [Vec::new(), Vec::new()],
            trans_inval_info: Vec::new(),
            inplace_inval_info: None,
            syscache_callback_list: Vec::new(),
            syscache_callback_links: vec![0; crate::utils::syscache::SYSCACHE_SIZE],
            relcache_callback_list: Vec::new(),
            relsync_callback_list: Vec::new(),
        }
    }
}

tokio::task_local! {
    /// PG's inval.c file-statics, per backend task.
    static INVAL_STATE: RefCell<InvalState>;
}

/// Run `f` with a fresh per-task invalidation state in scope. Used by tests and by
/// the backend bootstrap to establish the task-local before any cache activity.
pub fn scope<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    INVAL_STATE.sync_scope(RefCell::new(InvalState::new()), f)
}

/// Ensure the per-task invalidation state exists, returning whether it does. When
/// absent (no scope established) most public entry points are quiet no-ops, which
/// is the common no-DDL path that unblocks the lock manager.
fn state_present() -> bool {
    INVAL_STATE.try_with(|_| ()).is_ok()
}

/// PG `MyDatabaseId`. Read from the current session (the deprecated miscadmin
/// shim is avoided per the style rules).
fn my_database_id() -> Oid {
    crate::session::current().database_id()
}

// ---------------------------------------------------------------------------
// GUC: debug_discard_caches (PG file-global int; here a process-global atomic)
// ---------------------------------------------------------------------------

static DEBUG_DISCARD_CACHES: AtomicI32 = AtomicI32::new(0);

/// Read the `debug_discard_caches` GUC.
pub fn debug_discard_caches() -> i32 {
    DEBUG_DISCARD_CACHES.load(Ordering::Relaxed)
}

/// Set the `debug_discard_caches` GUC.
pub fn set_debug_discard_caches(value: i32) {
    DEBUG_DISCARD_CACHES.store(value, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Invalidation subgroup support functions
// ---------------------------------------------------------------------------

/// PG `AddInvalidationMessage`: append `msg` to a subgroup, growing the backing
/// array. The group must be the last active one (we append to the array end).
fn add_invalidation_message(
    arrays: &mut [Vec<SharedInvalidationMessage>; 2],
    group: &mut InvalidationMsgsGroup,
    subgroup: usize,
    msg: SharedInvalidationMessage,
) {
    let nextindex = group.nextmsg[subgroup] as usize;
    let arr = &mut arrays[subgroup];
    debug_assert!(nextindex <= arr.len());
    if nextindex == arr.len() {
        arr.push(msg);
    } else {
        arr[nextindex] = msg;
    }
    group.nextmsg[subgroup] += 1;
}

/// PG `AppendInvalidationMessageSubGroup`: append one subgroup onto another,
/// resetting the source subgroup to empty. The two must be adjacent in the array.
fn append_invalidation_message_subgroup(
    dest: &mut InvalidationMsgsGroup,
    src: &mut InvalidationMsgsGroup,
    subgroup: usize,
) {
    debug_assert_eq!(dest.nextmsg[subgroup], src.firstmsg[subgroup]);
    dest.nextmsg[subgroup] = src.nextmsg[subgroup];
    // Always re-point src past dest, so groups never share an array fragment.
    src.set_subgroup_to_follow(dest, subgroup);
}

/// PG `AppendInvalidationMessages`.
fn append_invalidation_messages(dest: &mut InvalidationMsgsGroup, src: &mut InvalidationMsgsGroup) {
    append_invalidation_message_subgroup(dest, src, CAT_CACHE_MSGS);
    append_invalidation_message_subgroup(dest, src, REL_CACHE_MSGS);
}

/// PG `ProcessMessageSubGroup`: collect a copy of a subgroup's messages. The copy
/// keeps the RefCell borrow off the (possibly re-entrant) callback path.
fn collect_subgroup(
    arrays: &[Vec<SharedInvalidationMessage>; 2],
    group: &InvalidationMsgsGroup,
    subgroup: usize,
) -> Vec<SharedInvalidationMessage> {
    let first = group.firstmsg[subgroup] as usize;
    let end = group.nextmsg[subgroup] as usize;
    arrays[subgroup][first..end].to_vec()
}

/// PG `ProcessInvalidationMessages`: catcache messages first, then relcache.
fn collect_group_ordered(
    arrays: &[Vec<SharedInvalidationMessage>; 2],
    group: &InvalidationMsgsGroup,
) -> Vec<SharedInvalidationMessage> {
    let mut out = collect_subgroup(arrays, group, CAT_CACHE_MSGS);
    out.extend(collect_subgroup(arrays, group, REL_CACHE_MSGS));
    out
}

// ---------------------------------------------------------------------------
// Invalidation group support functions (cat/rel split aware)
// ---------------------------------------------------------------------------

/// PG `AddCatcacheInvalidationMessage`.
fn add_catcache_invalidation_message(
    arrays: &mut [Vec<SharedInvalidationMessage>; 2],
    group: &mut InvalidationMsgsGroup,
    id: i32,
    hash_value: u32,
    db_id: Oid,
) {
    debug_assert!(id < i32::from(i8::MAX));
    let msg = SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
        id: id as i8,
        db_id,
        hash_value,
    });
    add_invalidation_message(arrays, group, CAT_CACHE_MSGS, msg);
}

/// PG `AddCatalogInvalidationMessage`.
fn add_catalog_invalidation_message(
    arrays: &mut [Vec<SharedInvalidationMessage>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    cat_id: Oid,
) {
    let msg = SharedInvalidationMessage::Catalog(SharedInvalCatalogMsg { db_id, cat_id });
    add_invalidation_message(arrays, group, CAT_CACHE_MSGS, msg);
}

/// PG `AddRelcacheInvalidationMessage` (dedups within the subgroup).
fn add_relcache_invalidation_message(
    arrays: &mut [Vec<SharedInvalidationMessage>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) {
    // Don't add a duplicate item. dbId need not be checked (never changes);
    // InvalidOid relId means all relations, so individual ones are redundant.
    let first = group.firstmsg[REL_CACHE_MSGS] as usize;
    let end = group.nextmsg[REL_CACHE_MSGS] as usize;
    for m in &arrays[REL_CACHE_MSGS][first..end] {
        if let SharedInvalidationMessage::Relcache(rc) = m
            && (rc.rel_id == rel_id || rc.rel_id == InvalidOid)
        {
            return;
        }
    }
    let msg = SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg { db_id, rel_id });
    add_invalidation_message(arrays, group, REL_CACHE_MSGS, msg);
}

/// PG `AddRelsyncInvalidationMessage` (relsync stored in the relcache subgroup).
fn add_relsync_invalidation_message(
    arrays: &mut [Vec<SharedInvalidationMessage>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) {
    let first = group.firstmsg[REL_CACHE_MSGS] as usize;
    let end = group.nextmsg[REL_CACHE_MSGS] as usize;
    for m in &arrays[REL_CACHE_MSGS][first..end] {
        if let SharedInvalidationMessage::RelSync(rs) = m
            && (rs.relid == rel_id || rs.relid == InvalidOid)
        {
            return;
        }
    }
    let msg = SharedInvalidationMessage::RelSync(SharedInvalRelSyncMsg { db_id, relid: rel_id });
    add_invalidation_message(arrays, group, REL_CACHE_MSGS, msg);
}

/// PG `AddSnapshotInvalidationMessage` (snapshot stored in the relcache subgroup).
fn add_snapshot_invalidation_message(
    arrays: &mut [Vec<SharedInvalidationMessage>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) {
    let first = group.firstmsg[REL_CACHE_MSGS] as usize;
    let end = group.nextmsg[REL_CACHE_MSGS] as usize;
    for m in &arrays[REL_CACHE_MSGS][first..end] {
        if let SharedInvalidationMessage::Snapshot(sn) = m
            && sn.rel_id == rel_id
        {
            return;
        }
    }
    let msg = SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg { db_id, rel_id });
    add_invalidation_message(arrays, group, REL_CACHE_MSGS, msg);
}

// ---------------------------------------------------------------------------
// private support: register-* operate on whichever InvalidationInfo is current.
//
// The C versions take an `InvalidationInfo *`. Here we identify the target by a
// `Target` (which entry of the task-local state owns the group) so a single
// borrow can mutate both the group and the backing arrays.
// ---------------------------------------------------------------------------

/// Which InvalidationInfo a register-* call targets within `InvalState`.
#[derive(Clone, Copy)]
enum Target {
    /// The top transactional stack entry.
    Trans,
    /// The inplace-update info.
    Inplace,
}

impl InvalState {
    /// Mutable access to the targeted info's current-command group + the arrays.
    fn target_mut(
        &mut self,
        target: Target,
    ) -> (&mut [Vec<SharedInvalidationMessage>; 2], &mut InvalidationInfo) {
        let info = match target {
            Target::Trans => &mut self.trans_inval_info.last_mut().expect("trans info").ii,
            Target::Inplace => self.inplace_inval_info.as_mut().expect("inplace info"),
        };
        (&mut self.inval_message_arrays, info)
    }
}

/// PG `RegisterCatcacheInvalidation`.
fn register_catcache_invalidation(
    st: &mut InvalState,
    target: Target,
    cache_id: i32,
    hash_value: u32,
    db_id: Oid,
) {
    let (arrays, info) = st.target_mut(target);
    add_catcache_invalidation_message(
        arrays,
        &mut info.current_cmd_invalid_msgs,
        cache_id,
        hash_value,
        db_id,
    );
}

/// PG `RegisterCatalogInvalidation`.
fn register_catalog_invalidation(st: &mut InvalState, target: Target, db_id: Oid, cat_id: Oid) {
    let (arrays, info) = st.target_mut(target);
    add_catalog_invalidation_message(arrays, &mut info.current_cmd_invalid_msgs, db_id, cat_id);
}

/// PG `RegisterRelcacheInvalidation`.
fn register_relcache_invalidation(st: &mut InvalState, target: Target, db_id: Oid, rel_id: Oid) {
    {
        let (arrays, info) = st.target_mut(target);
        add_relcache_invalidation_message(arrays, &mut info.current_cmd_invalid_msgs, db_id, rel_id);
    }
    // Quick hack so the next CommandCounterIncrement() runs
    // CommandEndInvalidationMessages() even for non-catalog-driven relcache inval.
    let _ = crate::backend::access::transam::xact::GetCurrentCommandId(true);

    // If the relation is one cached in the relcache init file, zap that file at
    // commit. Whole-relcache invalidation also forces the init-file zap.
    if rel_id == InvalidOid || crate::utils::relcache::RelationIdIsInInitFile(rel_id) {
        let (_arrays, info) = st.target_mut(target);
        info.relcache_init_file_inval = true;
    }
}

/// PG `RegisterRelsyncInvalidation`.
fn register_relsync_invalidation(st: &mut InvalState, target: Target, db_id: Oid, rel_id: Oid) {
    let (arrays, info) = st.target_mut(target);
    add_relsync_invalidation_message(arrays, &mut info.current_cmd_invalid_msgs, db_id, rel_id);
}

/// PG `RegisterSnapshotInvalidation`.
fn register_snapshot_invalidation(st: &mut InvalState, target: Target, db_id: Oid, rel_id: Oid) {
    let (arrays, info) = st.target_mut(target);
    add_snapshot_invalidation_message(arrays, &mut info.current_cmd_invalid_msgs, db_id, rel_id);
}

/// PG `PrepareInvalidationState`: initialize inval data for the current
/// (sub)transaction, returning `Target::Trans`. Pushes a stack entry if the top
/// is not for the current nesting level.
fn prepare_invalidation_state(st: &mut InvalState) -> Target {
    AssertCouldGetRelation();
    // Can't queue transactional messages while collecting inplace messages.
    debug_assert!(st.inplace_inval_info.is_none());

    let cur_level = crate::backend::access::transam::xact::GetCurrentTransactionNestLevel();

    if let Some(top) = st.trans_inval_info.last() {
        if top.my_level == cur_level {
            return Target::Trans;
        }
        // Deeper nesting level expected.
        debug_assert!(cur_level > top.my_level);

        // The parent must have no unprocessed current-command messages.
        assert!(
            top.ii.current_cmd_invalid_msgs.num_in_group() == 0,
            "cannot start a subtransaction when there are unprocessed inval messages"
        );

        let parent_current = top.ii.current_cmd_invalid_msgs;
        let mut my_info = TransInvalidationInfo {
            my_level: cur_level,
            ..Default::default()
        };
        my_info
            .prior_cmd_invalid_msgs
            .set_group_to_follow(&parent_current);
        let prior = my_info.prior_cmd_invalid_msgs;
        my_info.ii.current_cmd_invalid_msgs.set_group_to_follow(&prior);
        st.trans_inval_info.push(my_info);
    } else {
        // First (sub)transaction: clear any leftover array contents.
        st.inval_message_arrays[CAT_CACHE_MSGS].clear();
        st.inval_message_arrays[REL_CACHE_MSGS].clear();
        st.trans_inval_info.push(TransInvalidationInfo {
            my_level: cur_level,
            ..Default::default()
        });
    }
    Target::Trans
}

/// PG `PrepareInplaceInvalidationState`.
fn prepare_inplace_invalidation_state(st: &mut InvalState) -> Target {
    AssertCouldGetRelation();
    debug_assert!(st.inplace_inval_info.is_none());

    let mut my_info = InvalidationInfo::default();
    // Stash our messages past the end of the transactional messages, if any.
    if let Some(top) = st.trans_inval_info.last() {
        let parent_current = top.ii.current_cmd_invalid_msgs;
        my_info
            .current_cmd_invalid_msgs
            .set_group_to_follow(&parent_current);
    } else {
        st.inval_message_arrays[CAT_CACHE_MSGS].clear();
        st.inval_message_arrays[REL_CACHE_MSGS].clear();
    }
    st.inplace_inval_info = Some(my_info);
    Target::Inplace
}

// ---------------------------------------------------------------------------
// public functions
// ---------------------------------------------------------------------------

/// PG `InvalidateSystemCachesExtended`.
pub fn invalidate_system_caches_extended(debug_discard: bool) {
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    crate::utils::catcache::ResetCatalogCachesExt(debug_discard);
    crate::utils::relcache::RelationCacheInvalidate(debug_discard); // gets smgr + relmap too

    // Snapshot the callback lists (copies of Copy structs) to avoid holding the
    // RefCell borrow across the callbacks.
    let (sys, rel, rels) = INVAL_STATE
        .try_with(|cell| {
            let st = cell.borrow();
            (
                st.syscache_callback_list.clone(),
                st.relcache_callback_list.clone(),
                st.relsync_callback_list.clone(),
            )
        })
        .unwrap_or_default();

    for cc in &sys {
        (cc.function)(cc.arg, i32::from(cc.id), 0);
    }
    for cc in &rel {
        (cc.function)(cc.arg, InvalidOid);
    }
    for cc in &rels {
        (cc.function)(cc.arg, InvalidOid);
    }
}

/// PG `LocalExecuteInvalidationMessage`: process a single SI message locally.
pub fn local_execute_invalidation_message(msg: &SharedInvalidationMessage) {
    match msg {
        SharedInvalidationMessage::Catcache(cc) => {
            if cc.db_id == my_database_id() || cc.db_id == InvalidOid {
                crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
                crate::utils::syscache::SysCacheInvalidate(syscache_id(cc.id), cc.hash_value);
                call_syscache_callbacks(i32::from(cc.id), cc.hash_value);
            }
        }
        SharedInvalidationMessage::Catalog(cat) => {
            if cat.db_id == my_database_id() || cat.db_id == InvalidOid {
                crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
                crate::utils::catcache::CatalogCacheFlushCatalog(cat.cat_id);
                // CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed.
            }
        }
        SharedInvalidationMessage::Relcache(rc) => {
            if rc.db_id == my_database_id() || rc.db_id == InvalidOid {
                if rc.rel_id == InvalidOid {
                    crate::utils::relcache::RelationCacheInvalidate(false);
                } else {
                    crate::utils::relcache::RelationCacheInvalidateEntry(rc.rel_id);
                }
                let rel = INVAL_STATE
                    .try_with(|cell| cell.borrow().relcache_callback_list.clone())
                    .unwrap_or_default();
                for cc in &rel {
                    (cc.function)(cc.arg, rc.rel_id);
                }
            }
        }
        SharedInvalidationMessage::Smgr(sm) => {
            // Could have smgr entries for other databases, so no short-circuit.
            let rlocator = RelFileLocatorBackend {
                locator: sm.rlocator,
                backend: sm.backend,
            };
            smgrreleaserellocator(rlocator);
        }
        SharedInvalidationMessage::Relmap(rm) => {
            // We only care about our own database and shared catalogs.
            if rm.db_id == InvalidOid {
                crate::utils::relmapper::RelationMapInvalidate(true);
            } else if rm.db_id == my_database_id() {
                crate::utils::relmapper::RelationMapInvalidate(false);
            }
        }
        SharedInvalidationMessage::Snapshot(sn) => {
            if sn.db_id == InvalidOid || sn.db_id == my_database_id() {
                crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
            }
        }
        SharedInvalidationMessage::RelSync(rs) => {
            if rs.db_id == my_database_id() {
                call_rel_sync_callbacks(rs.relid);
            }
        }
    }
}

/// Convert a catcache message's `i8` id into a `SysCacheIdentifier`.
fn syscache_id(id: i8) -> crate::utils::syscache::SysCacheIdentifier {
    debug_assert!((id as usize) < crate::utils::syscache::SYSCACHE_SIZE);
    // SAFETY: SysCacheIdentifier is #[repr(i32)] with contiguous 0..SYSCACHE_SIZE
    // discriminants; a valid catcache id (asserted above) maps onto one of them.
    unsafe { std::mem::transmute::<i32, crate::utils::syscache::SysCacheIdentifier>(i32::from(id)) }
}

/// TODO(smgr): `smgrreleaserellocator` does not exist yet; the smgr cache is not
/// wired. Reached only when a real SHAREDINVALSMGR message is processed.
fn smgrreleaserellocator(_rlocator: RelFileLocatorBackend) {
    // TODO(smgr): close open smgr entries for the relation (smgrreleaserellocator).
}

/// PG `InvalidateSystemCaches`.
pub fn invalidate_system_caches() {
    invalidate_system_caches_extended(false);
}

/// PG `AcceptInvalidationMessages`: read + process SI messages for this backend.
///
/// With an empty queue (or no registered SI slot) this is a quiet no-op -- the
/// common no-DDL path -- which is what unblocks the lock manager.
pub fn accept_invalidation_messages() {
    // Message handlers may access catalogs only during transactions (PG guards
    // this under USE_ASSERT_CHECKING; here it stays a debug-only check that
    // tolerates being called outside a transaction scope).
    #[cfg(debug_assertions)]
    if crate::backend::access::transam::xact::is_transaction_state_or_false() {
        AssertCouldGetRelation();
    }

    crate::backend::storage::ipc::sinval::ReceiveSharedInvalidMessages(
        local_execute_invalidation_message,
        invalidate_system_caches,
    );

    // Test hook: force cache flushes whenever a flush could happen.
    if debug_discard_caches() > 0 {
        thread_local! {
            static RECURSION_DEPTH: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
        }
        RECURSION_DEPTH.with(|d| {
            if d.get() < debug_discard_caches() {
                d.set(d.get() + 1);
                invalidate_system_caches_extended(true);
                d.set(d.get() - 1);
            }
        });
    }
}

/// PG `PostPrepare_Inval`: act as though the transaction aborted.
pub fn post_prepare_inval() {
    at_eoxact_inval(false);
}

/// PG `xactGetCommittedInvalidationMessages`: collect a committing xact's messages
/// into a single contiguous array (PriorCmd then CurrentCmd, cat before rel).
/// Returns `(msgs, relcache_init_file_inval)`.
pub fn xact_get_committed_invalidation_messages() -> (Vec<SharedInvalidationMessage>, bool) {
    if !state_present() {
        return (Vec::new(), false);
    }
    INVAL_STATE.with(|cell| {
        let st = cell.borrow();
        let Some(top) = st.trans_inval_info.last() else {
            return (Vec::new(), false);
        };
        // Must be at top of stack.
        debug_assert!(top.my_level == 1 && st.trans_inval_info.len() == 1);

        let relcache_init_file_inval = top.ii.relcache_init_file_inval;
        let arrays = &st.inval_message_arrays;
        let prior = &top.prior_cmd_invalid_msgs;
        let current = &top.ii.current_cmd_invalid_msgs;

        // Maintain AtEOXact_Inval()'s processing order: cat(prior), cat(current),
        // rel(prior), rel(current).
        let mut out = Vec::new();
        out.extend(collect_subgroup(arrays, prior, CAT_CACHE_MSGS));
        out.extend(collect_subgroup(arrays, current, CAT_CACHE_MSGS));
        out.extend(collect_subgroup(arrays, prior, REL_CACHE_MSGS));
        out.extend(collect_subgroup(arrays, current, REL_CACHE_MSGS));
        (out, relcache_init_file_inval)
    })
}

/// PG `inplaceGetInvalidationMessages`.
pub fn inplace_get_invalidation_messages() -> (Vec<SharedInvalidationMessage>, bool) {
    if !state_present() {
        return (Vec::new(), false);
    }
    INVAL_STATE.with(|cell| {
        let st = cell.borrow();
        let Some(info) = st.inplace_inval_info.as_ref() else {
            return (Vec::new(), false);
        };
        let out = collect_group_ordered(&st.inval_message_arrays, &info.current_cmd_invalid_msgs);
        (out, info.relcache_init_file_inval)
    })
}

/// PG `ProcessCommittedInvalidationMessages` (redo path). The DatabasePath /
/// init-file dance is staged (relcache stubs); the send is real.
pub fn process_committed_invalidation_messages(
    msgs: &[SharedInvalidationMessage],
    relcache_init_file_inval: bool,
    _dbid: Oid,
    _tsid: Oid,
) {
    if msgs.is_empty() {
        return;
    }
    if relcache_init_file_inval {
        // PG sets DatabasePath then calls RelationCacheInitFilePreInvalidate; that
        // path is a relcache stub here (TODO: DatabasePath during recovery).
        crate::utils::relcache::RelationCacheInitFilePreInvalidate();
    }
    crate::backend::storage::ipc::sinval::SendSharedInvalidMessages(msgs);
    if relcache_init_file_inval {
        crate::utils::relcache::RelationCacheInitFilePostInvalidate();
    }
}

/// What `AtEOXact_Inval` decided to do under its short RefCell borrow.
enum Action {
    Commit {
        relcache_init_file_inval: bool,
        msgs: Vec<SharedInvalidationMessage>,
    },
    Abort(Vec<SharedInvalidationMessage>),
}

/// PG `AtEOXact_Inval`: process queued messages at end of main transaction.
pub fn at_eoxact_inval(is_commit: bool) {
    if !state_present() {
        return;
    }

    // Decide what to do under a short borrow, then act outside it (sends + local
    // execute must not hold the RefCell borrow).
    let action = INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        st.inplace_inval_info = None;
        let top = st.trans_inval_info.last().copied()?;
        debug_assert!(top.my_level == 1 && st.trans_inval_info.len() == 1);

        if is_commit {
            let relcache_init_file_inval = top.ii.relcache_init_file_inval;
            // Fold current into prior, then collect the whole prior group.
            let mut top = top;
            append_invalidation_messages(
                &mut top.prior_cmd_invalid_msgs,
                &mut top.ii.current_cmd_invalid_msgs,
            );
            let msgs = collect_group_ordered(&st.inval_message_arrays, &top.prior_cmd_invalid_msgs);
            *st.trans_inval_info.last_mut().unwrap() = top;
            Some(Action::Commit {
                relcache_init_file_inval,
                msgs,
            })
        } else {
            let msgs = collect_group_ordered(&st.inval_message_arrays, &top.prior_cmd_invalid_msgs);
            Some(Action::Abort(msgs))
        }
    });

    match action {
        None => return,
        Some(Action::Commit {
            relcache_init_file_inval,
            msgs,
        }) => {
            if relcache_init_file_inval {
                crate::utils::relcache::RelationCacheInitFilePreInvalidate();
            }
            crate::backend::storage::ipc::sinval::SendSharedInvalidMessages(&msgs);
            if relcache_init_file_inval {
                crate::utils::relcache::RelationCacheInitFilePostInvalidate();
            }
        }
        Some(Action::Abort(msgs)) => {
            for m in &msgs {
                local_execute_invalidation_message(m);
            }
        }
    }

    // Reset our state to empty.
    INVAL_STATE.with(|cell| cell.borrow_mut().trans_inval_info.clear());
}

/// PG `PreInplace_Inval`: process queued invalidation before the inplace-update
/// critical section.
pub fn pre_inplace_inval() {
    if !state_present() {
        return;
    }
    let need = INVAL_STATE.with(|cell| {
        cell.borrow()
            .inplace_inval_info
            .as_ref()
            .is_some_and(|i| i.relcache_init_file_inval)
    });
    if need {
        crate::utils::relcache::RelationCacheInitFilePreInvalidate();
    }
}

/// PG `AtInplace_Inval`: process queued invalidations after inplace buffer mutation.
pub fn at_inplace_inval() {
    if !state_present() {
        return;
    }
    let collected = INVAL_STATE.with(|cell| {
        let st = cell.borrow();
        st.inplace_inval_info.as_ref().map(|info| {
            (
                collect_group_ordered(&st.inval_message_arrays, &info.current_cmd_invalid_msgs),
                info.relcache_init_file_inval,
            )
        })
    });
    let Some((msgs, relcache_init_file_inval)) = collected else {
        return;
    };
    crate::backend::storage::ipc::sinval::SendSharedInvalidMessages(&msgs);
    if relcache_init_file_inval {
        crate::utils::relcache::RelationCacheInitFilePostInvalidate();
    }
    INVAL_STATE.with(|cell| cell.borrow_mut().inplace_inval_info = None);
}

/// PG `ForgetInplace_Inval`: discard queued-up inplace invalidations.
pub fn forget_inplace_inval() {
    if !state_present() {
        return;
    }
    INVAL_STATE.with(|cell| cell.borrow_mut().inplace_inval_info = None);
}

/// PG `AtEOSubXact_Inval`: process queued messages at end of subtransaction.
pub fn at_eosubxact_inval(is_commit: bool) {
    if !state_present() {
        return;
    }
    let my_level = crate::backend::access::transam::xact::GetCurrentTransactionNestLevel();

    // On commit, the inplace info must already be clear; on abort, clear it.
    if is_commit {
        debug_assert!(
            INVAL_STATE.with(|cell| cell.borrow().inplace_inval_info.is_none())
        );
    } else {
        INVAL_STATE.with(|cell| cell.borrow_mut().inplace_inval_info = None);
    }

    // Quick exit if no transactional messages, or not for this level.
    let top_level = INVAL_STATE.with(|cell| cell.borrow().trans_inval_info.last().map(|t| t.my_level));
    let Some(top_level) = top_level else {
        return;
    };
    if top_level != my_level {
        debug_assert!(top_level < my_level);
        return;
    }

    if is_commit {
        // If CurrentCmdInvalidMsgs still has anything, fold it into prior + run.
        command_end_invalidation_messages();

        INVAL_STATE.with(|cell| {
            let mut st = cell.borrow_mut();
            let mut me = st.trans_inval_info.pop().expect("top");
            let has_parent_for_level = st
                .trans_inval_info
                .last()
                .is_some_and(|p| p.my_level >= my_level - 1);

            if !has_parent_for_level {
                // Lazily-created stack: no suitable parent. Just lower our level.
                me.my_level -= 1;
                st.trans_inval_info.push(me);
                return;
            }

            // Pass my messages up to the parent's PriorCmdInvalidMsgs.
            let mut parent = st.trans_inval_info.pop().expect("parent");
            append_invalidation_messages(
                &mut parent.prior_cmd_invalid_msgs,
                &mut me.prior_cmd_invalid_msgs,
            );
            // Readjust parent's CurrentCmdInvalidMsgs indexes now.
            let prior = parent.prior_cmd_invalid_msgs;
            parent
                .ii
                .current_cmd_invalid_msgs
                .set_group_to_follow(&prior);
            // Pending relcache inval becomes the parent's problem too.
            if me.ii.relcache_init_file_inval {
                parent.ii.relcache_init_file_inval = true;
            }
            st.trans_inval_info.push(parent);
        });
    } else {
        let msgs = INVAL_STATE.with(|cell| {
            let st = cell.borrow();
            let me = st.trans_inval_info.last().expect("top");
            collect_group_ordered(&st.inval_message_arrays, &me.prior_cmd_invalid_msgs)
        });
        for m in &msgs {
            local_execute_invalidation_message(m);
        }
        INVAL_STATE.with(|cell| {
            cell.borrow_mut().trans_inval_info.pop();
        });
    }
}

/// PG `CommandEndInvalidationMessages`: process the current command's messages,
/// then fold them into the prior-commands group.
pub fn command_end_invalidation_messages() {
    if !state_present() {
        return;
    }
    // Quietly return if no state (bootstrap / ABORT outside a transaction).
    let msgs = INVAL_STATE.with(|cell| {
        let st = cell.borrow();
        st.trans_inval_info
            .last()
            .map(|top| collect_group_ordered(&st.inval_message_arrays, &top.ii.current_cmd_invalid_msgs))
    });
    let Some(msgs) = msgs else {
        return;
    };
    for m in &msgs {
        local_execute_invalidation_message(m);
    }

    // WAL-log per-command invalidations for wal_level=logical.
    if crate::utils::rel::XLogLogicalInfoActive() {
        log_logical_invalidations();
    }

    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let mut top = st.trans_inval_info.pop().expect("top");
        append_invalidation_messages(
            &mut top.prior_cmd_invalid_msgs,
            &mut top.ii.current_cmd_invalid_msgs,
        );
        st.trans_inval_info.push(top);
    });
}

/// PG `CacheInvalidateHeapTupleCommon`: common logic for end-of-command + inplace.
fn cache_invalidate_heap_tuple_common(
    relation: Relation,
    tuple: HeapTuple,
    newtuple: HeapTuple,
    prepare: fn(&mut InvalState) -> Target,
) {
    AssertCouldGetRelation();

    // Do nothing during bootstrap.
    if is_bootstrap_processing_mode() {
        return;
    }
    // Only system-catalog tuples can be in catcaches / affect the relcache.
    if !IsCatalogRelation(relation) {
        return;
    }
    // IsCatalogRelation() is true for TOAST tables of catalogs; skip those.
    if IsToastRelation(relation) {
        return;
    }

    let target = INVAL_STATE.with(|cell| prepare(&mut cell.borrow_mut()));

    // First let the catcache do its thing.
    let tuple_rel_id = unsafe { (*relation).rd_id };
    if crate::utils::syscache::RelationInvalidatesSnapshotsOnly(tuple_rel_id) {
        let database_id = if IsSharedRelation(tuple_rel_id) {
            InvalidOid
        } else {
            my_database_id()
        };
        INVAL_STATE.with(|cell| {
            register_snapshot_invalidation(
                &mut cell.borrow_mut(),
                target,
                database_id,
                tuple_rel_id,
            );
        });
    } else {
        // PrepareToInvalidateCacheTuple drives RegisterCatcacheInvalidation per
        // affected catcache (staged: catcache stub).
        let mut register = |cache_id: i32, hash_value: u32, db_id: Oid| {
            INVAL_STATE.with(|cell| {
                register_catcache_invalidation(
                    &mut cell.borrow_mut(),
                    target,
                    cache_id,
                    hash_value,
                    db_id,
                );
            });
        };
        crate::utils::catcache::PrepareToInvalidateCacheTuple(
            relation,
            tuple,
            newtuple,
            &mut register,
        );
    }

    // Is this tuple a primary definer of a relcache entry?
    let (relation_id, database_id) = if tuple_rel_id == RelationRelationId {
        let classtup = crate::access::htup_details::GETSTRUCT(unsafe { &*tuple })
            .cast::<crate::catalog::pg_class::FormData_pg_class>();
        let relation_id = unsafe { (*classtup).oid };
        let database_id = if unsafe { (*classtup).relisshared } {
            InvalidOid
        } else {
            my_database_id()
        };
        (relation_id, database_id)
    } else if tuple_rel_id == AttributeRelationId {
        let atttup = crate::access::htup_details::GETSTRUCT(unsafe { &*tuple })
            .cast::<crate::catalog::pg_attribute::FormData_pg_attribute>();
        // KLUGE: always MyDatabaseId, even for shared rels (can't easily tell).
        (unsafe { (*atttup).attrelid }, my_database_id())
    } else if tuple_rel_id == IndexRelationId {
        let indextup = crate::access::htup_details::GETSTRUCT(unsafe { &*tuple })
            .cast::<crate::catalog::pg_index::FormData_pg_index>();
        (unsafe { (*indextup).indexrelid }, my_database_id())
    } else if tuple_rel_id == ConstraintRelationId {
        let constrtup = crate::access::htup_details::GETSTRUCT(unsafe { &*tuple })
            .cast::<crate::catalog::pg_constraint::FormData_pg_constraint>();
        // Foreign keys are part of relcache entries; inval the table the FK is on.
        if unsafe { (*constrtup).contype } == CONSTRAINT_FOREIGN
            && OidIsValid(unsafe { (*constrtup).conrelid })
        {
            (unsafe { (*constrtup).conrelid }, my_database_id())
        } else {
            return;
        }
    } else {
        return;
    };

    INVAL_STATE.with(|cell| {
        register_relcache_invalidation(&mut cell.borrow_mut(), target, database_id, relation_id);
    });
}

/// PG `CacheInvalidateHeapTuple`.
pub fn cache_invalidate_heap_tuple(relation: Relation, tuple: HeapTuple, newtuple: HeapTuple) {
    cache_invalidate_heap_tuple_common(relation, tuple, newtuple, prepare_invalidation_state);
}

/// PG `CacheInvalidateHeapTupleInplace`.
pub fn cache_invalidate_heap_tuple_inplace(relation: Relation, key_equivalent_tuple: HeapTuple) {
    cache_invalidate_heap_tuple_common(
        relation,
        key_equivalent_tuple,
        std::ptr::null_mut(),
        prepare_inplace_invalidation_state,
    );
}

/// PG `CacheInvalidateCatalog`.
pub fn cache_invalidate_catalog(catalog_id: Oid) {
    let database_id = if IsSharedRelation(catalog_id) {
        InvalidOid
    } else {
        my_database_id()
    };
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let target = prepare_invalidation_state(&mut st);
        register_catalog_invalidation(&mut st, target, database_id, catalog_id);
    });
}

/// PG `CacheInvalidateRelcache`.
pub fn cache_invalidate_relcache(relation: Relation) {
    let relation_id = unsafe { (*relation).rd_id };
    let database_id = if unsafe { (*(*relation).rd_rel).relisshared } {
        InvalidOid
    } else {
        my_database_id()
    };
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let target = prepare_invalidation_state(&mut st);
        register_relcache_invalidation(&mut st, target, database_id, relation_id);
    });
}

/// PG `CacheInvalidateRelcacheAll`.
pub fn cache_invalidate_relcache_all() {
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let target = prepare_invalidation_state(&mut st);
        register_relcache_invalidation(&mut st, target, InvalidOid, InvalidOid);
    });
}

/// PG `CacheInvalidateRelcacheByTuple`.
pub fn cache_invalidate_relcache_by_tuple(class_tuple: HeapTuple) {
    let classtup = crate::access::htup_details::GETSTRUCT(unsafe { &*class_tuple })
        .cast::<crate::catalog::pg_class::FormData_pg_class>();
    let relation_id = unsafe { (*classtup).oid };
    let database_id = if unsafe { (*classtup).relisshared } {
        InvalidOid
    } else {
        my_database_id()
    };
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let target = prepare_invalidation_state(&mut st);
        register_relcache_invalidation(&mut st, target, database_id, relation_id);
    });
}

/// PG `CacheInvalidateRelcacheByRelid`.
pub fn cache_invalidate_relcache_by_relid(relid: Oid) {
    let tup = crate::utils::syscache::SearchSysCache1(
        crate::utils::syscache::SysCacheIdentifier::RELOID,
        crate::postgres::ObjectIdGetDatum(relid),
    );
    let Some(tup) = tup else {
        panic!("cache lookup failed for relation {}", relid.0);
    };
    cache_invalidate_relcache_by_tuple(tup);
    crate::utils::syscache::ReleaseSysCache(tup);
}

/// PG `CacheInvalidateRelSync`.
pub fn cache_invalidate_rel_sync(relid: Oid) {
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let target = prepare_invalidation_state(&mut st);
        register_relsync_invalidation(&mut st, target, my_database_id(), relid);
    });
}

/// PG `CacheInvalidateRelSyncAll`.
pub fn cache_invalidate_rel_sync_all() {
    cache_invalidate_rel_sync(InvalidOid);
}

/// PG `CacheInvalidateSmgr`: nontransactional, sent immediately.
pub fn cache_invalidate_smgr(rlocator: RelFileLocatorBackend) {
    let msg = SharedInvalidationMessage::Smgr(SharedInvalSmgrMsg {
        backend: rlocator.backend,
        rlocator: rlocator.locator,
    });
    crate::backend::storage::ipc::sinval::SendSharedInvalidMessages(&[msg]);
}

/// PG `CacheInvalidateRelmap`: nontransactional, sent immediately.
pub fn cache_invalidate_relmap(database_id: Oid) {
    let msg = SharedInvalidationMessage::Relmap(SharedInvalRelmapMsg { db_id: database_id });
    crate::backend::storage::ipc::sinval::SendSharedInvalidMessages(&[msg]);
}

/// PG `CacheRegisterSyscacheCallback`.
pub fn cache_register_syscache_callback(cacheid: i32, func: SyscacheCallbackFunction, arg: Datum) {
    assert!(
        cacheid >= 0 && (cacheid as usize) < crate::utils::syscache::SYSCACHE_SIZE,
        "invalid cache ID: {cacheid}"
    );
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        assert!(
            st.syscache_callback_list.len() < MAX_SYSCACHE_CALLBACKS,
            "out of syscache_callback_list slots"
        );
        let new_index = st.syscache_callback_list.len();
        let head = st.syscache_callback_links[cacheid as usize];
        if head == 0 {
            // First callback for this cache.
            st.syscache_callback_links[cacheid as usize] = new_index as i16 + 1;
        } else {
            // Add to end of chain so older callbacks are called first.
            let mut i = (head - 1) as usize;
            while st.syscache_callback_list[i].link > 0 {
                i = (st.syscache_callback_list[i].link - 1) as usize;
            }
            st.syscache_callback_list[i].link = new_index as i16 + 1;
        }
        st.syscache_callback_list.push(SyscacheCallback {
            id: cacheid as i16,
            link: 0,
            function: func,
            arg,
        });
    });
}

/// PG `CacheRegisterRelcacheCallback`.
pub fn cache_register_relcache_callback(func: RelcacheCallbackFunction, arg: Datum) {
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        assert!(
            st.relcache_callback_list.len() < MAX_RELCACHE_CALLBACKS,
            "out of relcache_callback_list slots"
        );
        st.relcache_callback_list
            .push(RelcacheCallback { function: func, arg });
    });
}

/// PG `CacheRegisterRelSyncCallback`.
pub fn cache_register_rel_sync_callback(func: RelSyncCallbackFunction, arg: Datum) {
    INVAL_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        assert!(
            st.relsync_callback_list.len() < MAX_RELSYNC_CALLBACKS,
            "out of relsync_callback_list slots"
        );
        st.relsync_callback_list
            .push(RelsyncCallback { function: func, arg });
    });
}

/// PG `CallSyscacheCallbacks`: dispatch every callback registered for `cacheid`,
/// in registration order (oldest first). Borrow is dropped before each call.
pub fn call_syscache_callbacks(cacheid: i32, hashvalue: u32) {
    assert!(
        cacheid >= 0 && (cacheid as usize) < crate::utils::syscache::SYSCACHE_SIZE,
        "invalid cache ID: {cacheid}"
    );
    // Walk the link chain under a borrow, collecting (function, arg) to call after.
    let to_call: Vec<(SyscacheCallbackFunction, Datum)> = INVAL_STATE
        .try_with(|cell| {
            let st = cell.borrow();
            let mut out = Vec::new();
            let mut i = i32::from(st.syscache_callback_links[cacheid as usize]) - 1;
            while i >= 0 {
                let cc = &st.syscache_callback_list[i as usize];
                debug_assert_eq!(i32::from(cc.id), cacheid);
                out.push((cc.function, cc.arg));
                i = i32::from(cc.link) - 1;
            }
            out
        })
        .unwrap_or_default();
    for (func, arg) in to_call {
        func(arg, cacheid, hashvalue);
    }
}

/// PG `CallRelSyncCallbacks`.
pub fn call_rel_sync_callbacks(relid: Oid) {
    let to_call: Vec<(RelSyncCallbackFunction, Datum)> = INVAL_STATE
        .try_with(|cell| {
            cell.borrow()
                .relsync_callback_list
                .iter()
                .map(|cc| (cc.function, cc.arg))
                .collect()
        })
        .unwrap_or_default();
    for (func, arg) in to_call {
        func(arg, relid);
    }
}

/// PG `LogLogicalInvalidations`: emit WAL for the current command's invalidations
/// (only when wal_level=logical). The message assembly is faithful; the final WAL
/// insertion needs the XLogCtl handle, which is staged (TODO).
pub fn log_logical_invalidations() {
    if !state_present() {
        return;
    }
    let (catmsgs, relmsgs) = INVAL_STATE.with(|cell| {
        let st = cell.borrow();
        match st.trans_inval_info.last() {
            None => (Vec::new(), Vec::new()),
            Some(top) => {
                let g = &top.ii.current_cmd_invalid_msgs;
                (
                    collect_subgroup(&st.inval_message_arrays, g, CAT_CACHE_MSGS),
                    collect_subgroup(&st.inval_message_arrays, g, REL_CACHE_MSGS),
                )
            }
        }
    });
    let nmsgs = catmsgs.len() + relmsgs.len();
    if nmsgs == 0 {
        return;
    }

    crate::access::xloginsert::XLogBeginInsert();
    let xlrec = crate::access::xact::xl_xact_invals {
        nmsgs: nmsgs as i32,
    };
    let header = unsafe {
        std::slice::from_raw_parts(
            std::ptr::addr_of!(xlrec).cast::<u8>(),
            crate::access::xact::MinSizeOfXactInvals,
        )
    };
    crate::access::xloginsert::XLogRegisterData(header);
    register_msgs(&catmsgs);
    register_msgs(&relmsgs);
    // TODO(wal-logical): XLogInsert(RM_XACT_ID, XLOG_XACT_INVALIDATIONS) needs the
    // XLogCtl handle (async); reached only under wal_level=logical.
}

/// Register a batch of messages with the WAL record being assembled.
fn register_msgs(msgs: &[SharedInvalidationMessage]) {
    if msgs.is_empty() {
        return;
    }
    let mut bytes = Vec::with_capacity(msgs.len() * SIZEOF_SHARED_INVAL_MSG);
    for msg in msgs {
        serialize_shared_inval_msg(msg, &mut bytes);
    }
    crate::access::xloginsert::XLogRegisterData(&bytes);
}

/// Wire size of PG's `union SharedInvalidationMessage` (packed into 16 bytes).
const SIZEOF_SHARED_INVAL_MSG: usize = 16;

/// Serialize one message in PG's `union SharedInvalidationMessage` on-disk image:
/// a fixed 16-byte slot, leading `int8 id`, fields at the C union's offsets/padding
/// (native-endian, matching PG's memcpy-of-struct WAL/commit-record encoding).
fn serialize_shared_inval_msg(msg: &SharedInvalidationMessage, out: &mut Vec<u8>) {
    let start = out.len();
    out.resize(start + SIZEOF_SHARED_INVAL_MSG, 0);
    let slot = &mut out[start..start + SIZEOF_SHARED_INVAL_MSG];
    match *msg {
        SharedInvalidationMessage::Catcache(m) => {
            slot[0] = m.id as u8; // cache id (>= 0) is the id field
            slot[4..8].copy_from_slice(&m.db_id.0.to_ne_bytes());
            slot[8..12].copy_from_slice(&m.hash_value.to_ne_bytes());
        }
        SharedInvalidationMessage::Catalog(m) => {
            slot[0] = SHAREDINVALCATALOG_ID as u8;
            slot[4..8].copy_from_slice(&m.db_id.0.to_ne_bytes());
            slot[8..12].copy_from_slice(&m.cat_id.0.to_ne_bytes());
        }
        SharedInvalidationMessage::Relcache(m) => {
            slot[0] = SHAREDINVALRELCACHE_ID as u8;
            slot[4..8].copy_from_slice(&m.db_id.0.to_ne_bytes());
            slot[8..12].copy_from_slice(&m.rel_id.0.to_ne_bytes());
        }
        SharedInvalidationMessage::Smgr(m) => {
            // PG packs the procno into backend_hi (int8) + backend_lo (uint16).
            slot[0] = SHAREDINVALSMGR_ID as u8;
            slot[1] = (m.backend >> 16) as i8 as u8;
            slot[2..4].copy_from_slice(&((m.backend & 0xffff) as u16).to_ne_bytes());
            slot[4..8].copy_from_slice(&m.rlocator.spcOid.0.to_ne_bytes());
            slot[8..12].copy_from_slice(&m.rlocator.dbOid.0.to_ne_bytes());
            slot[12..16].copy_from_slice(&m.rlocator.relNumber.0.to_ne_bytes());
        }
        SharedInvalidationMessage::Relmap(m) => {
            slot[0] = SHAREDINVALRELMAP_ID as u8;
            slot[4..8].copy_from_slice(&m.db_id.0.to_ne_bytes());
        }
        SharedInvalidationMessage::Snapshot(m) => {
            slot[0] = SHAREDINVALSNAPSHOT_ID as u8;
            slot[4..8].copy_from_slice(&m.db_id.0.to_ne_bytes());
            slot[8..12].copy_from_slice(&m.rel_id.0.to_ne_bytes());
        }
        SharedInvalidationMessage::RelSync(m) => {
            slot[0] = SHAREDINVALRELSYNC_ID as u8;
            slot[4..8].copy_from_slice(&m.db_id.0.to_ne_bytes());
            slot[8..12].copy_from_slice(&m.relid.0.to_ne_bytes());
        }
    }
}

#[cfg(test)]
mod tests;
