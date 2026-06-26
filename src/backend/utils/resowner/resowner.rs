//! Rewritten from PostgreSQL src/backend/utils/resowner/resowner.c
//!
//! Resource-owner tracking, redesigned for the single-process async model.
//!
//! PG's resowner.c is a generic `(Datum value, ResourceOwnerDesc*)` registry: a
//! small array spilling to an open-addressing hash, with the release order
//! reconstructed at teardown by sorting on the per-kind descriptor's phase +
//! priority. That whole machine is replaced here.
//!
//! In Rust a tracked resource is just a `FnOnce()` closure that releases it
//! (drops a buffer pin, unregisters a snapshot, closes a file). An owner keeps
//! one [`GenSlab`] of these closures per release phase. Registering returns a
//! [`ResourceGuard`] whose `Drop` releases the resource on scope exit -- the
//! common pin/read/unpin path needs no explicit teardown and is panic/cancel
//! safe. The owner's phased teardown ([`ResourceOwner::release_all`]) drains any
//! still-registered closures in phase, then priority, then LIFO order, matching
//! PG's transaction-end semantics.
//!
//! Generational dedup ([`GenSlab`]'s key) is what lets early-release and phased
//! teardown coexist: whichever runs first removes the entry and advances the
//! slot generation, so the other one's key no longer resolves and is a no-op. A
//! resource is released exactly once.
//!
//! Locking: the inner state lives behind a std `Mutex` held only for the short
//! slab mutations. A release closure must NEVER run while the lock is held (it
//! could re-enter the owner), so every release path collects the closures under
//! the lock, drops the guard, then calls them.

use std::sync::{Arc, Mutex, Weak};

use crate::elog;
use crate::storage::procnumber::{GenSlab, Key};
use crate::utils::elog::WARNING;
use crate::utils::resowner::{ResourceReleasePhase, ResourceReleasePriority};

/// A registered resource: its within-phase priority, a monotonic sequence for
/// LIFO tie-breaking, an optional name for the commit-time leak warning, and the
/// release closure. `Send` + `'static` so an owner can be moved across tasks.
struct Entry {
    priority: ResourceReleasePriority,
    seq: u64,
    name: &'static str,
    release: Box<dyn FnOnce() + Send>,
}

/// One slab per release phase. Indexed by phase ordinal - 1.
type Phases = [GenSlab<Entry>; 3];

fn phase_index(phase: ResourceReleasePhase) -> usize {
    phase as usize - 1
}

/// All phases, in teardown order.
const PHASE_ORDER: [ResourceReleasePhase; 3] = [
    ResourceReleasePhase::BeforeLocks,
    ResourceReleasePhase::Locks,
    ResourceReleasePhase::AfterLocks,
];

/// Mutable per-owner state behind one short-critical-section `Mutex`.
struct Locked {
    phases: Phases,
    next_seq: u64,
}

/// A clonable handle to a resource owner (PG's `ResourceOwner`, formerly a raw
/// `*mut ResourceOwnerData`). Owners form a tree; children are released before
/// their parent.
#[derive(Clone)]
pub struct ResourceOwner(Arc<OwnerInner>);

struct OwnerInner {
    name: String,
    locked: Mutex<Locked>,
    parent: Mutex<Weak<OwnerInner>>,
    children: Mutex<Vec<ResourceOwner>>,
}

impl ResourceOwner {
    /// Identity comparison (PG compared raw `ResourceOwner` pointers). Two handles
    /// are the same owner iff they share the inner `Arc`.
    pub fn ptr_eq(&self, other: &ResourceOwner) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Create an owner, optionally registered as a child of `parent`
    /// (PG's `ResourceOwnerCreate`).
    pub fn create(parent: Option<&ResourceOwner>, name: &str) -> ResourceOwner {
        let owner = ResourceOwner(Arc::new(OwnerInner {
            name: name.to_string(),
            locked: Mutex::new(Locked {
                phases: std::array::from_fn(|_| GenSlab::new()),
                next_seq: 0,
            }),
            parent: Mutex::new(Weak::new()),
            children: Mutex::new(Vec::new()),
        }));
        if let Some(parent) = parent {
            *owner.0.parent.lock().unwrap() = Arc::downgrade(&parent.0);
            parent.0.children.lock().unwrap().push(owner.clone());
        }
        owner
    }

    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Register a resource in `phase` at `priority`; the returned guard releases
    /// it on `Drop` (or via `release_now`/`forget`). `name` labels the resource
    /// in the commit-time leak warning. (PG's `ResourceOwnerRemember`.)
    pub fn remember(
        &self,
        phase: ResourceReleasePhase,
        priority: ResourceReleasePriority,
        name: &'static str,
        release: impl FnOnce() + Send + 'static,
    ) -> ResourceGuard {
        let mut locked = self.0.locked.lock().unwrap();
        let seq = locked.next_seq;
        locked.next_seq += 1;
        let key = locked.phases[phase_index(phase)].insert(Entry {
            priority,
            seq,
            name,
            release: Box::new(release),
        });
        ResourceGuard {
            owner: Arc::downgrade(&self.0),
            phase,
            key,
        }
    }

    /// Release everything registered in one `phase`: children first, then this
    /// owner's entries in priority-ascending, seq-descending (LIFO) order. On
    /// commit, a non-empty phase means a leaked resource: warn before releasing.
    /// (PG's `ResourceOwnerRelease` for a single phase.)
    pub fn release(&self, phase: ResourceReleasePhase, is_commit: bool, is_top_level: bool) {
        // Snapshot child handles under the lock, then drop it before recursing:
        // a release closure could re-enter the owner tree and we must not hold
        // `children` across that.
        let children: Vec<ResourceOwner> = self.0.children.lock().unwrap().clone();
        for child in &children {
            child.release(phase, is_commit, is_top_level);
        }

        // Drain the phase slab and sort under the lock, but run the closures
        // only after the guard drops (a closure could re-enter this owner).
        let entries: Vec<Entry> = {
            let mut locked = self.0.locked.lock().unwrap();
            let slab = &mut locked.phases[phase_index(phase)];
            let keys: Vec<Key<Entry>> = slab.iter().map(|(key, _)| key).collect();
            let mut entries: Vec<Entry> = keys.into_iter().filter_map(|key| slab.remove(key)).collect();
            // priority ascending, then seq descending (LIFO within a priority).
            entries.sort_by(|a, b| {
                a.priority
                    .cmp(&b.priority)
                    .then(b.seq.cmp(&a.seq))
            });
            entries
        };

        // Run each release closure isolated: a panic here must not skip the
        // remaining resources, abort later phases, or (worst case, when this is
        // the abort-path teardown already running inside an unwind) abort the
        // process. Mirrors PG's per-resource PG_TRY in resowner.c. AssertUnwindSafe
        // is sound: on panic we discard that resource and move on, never reusing
        // its broken state.
        for entry in entries {
            if is_commit {
                elog!(WARNING, format!("resource was not closed: {}", entry.name));
            }
            let release = entry.release;
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || release())) {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".to_string());
                elog!(
                    WARNING,
                    format!("resource release panicked during teardown: {msg}")
                );
            }
        }
    }

    /// Transaction-end teardown: release every phase in order. (PG calls
    /// `ResourceOwnerRelease` once per phase from xact.c.)
    pub fn release_all(&self, is_commit: bool, is_top_level: bool) {
        for phase in PHASE_ORDER {
            self.release(phase, is_commit, is_top_level);
        }
    }

    /// Detach from the parent and drop the owner (PG's `ResourceOwnerDelete`).
    /// Asserts every phase is empty (resources must have been released first).
    pub fn delete(self) {
        {
            let locked = self.0.locked.lock().unwrap();
            debug_assert!(
                locked.phases.iter().all(GenSlab::is_empty),
                "ResourceOwnerDelete: owner {} still owns resources",
                self.0.name
            );
        }
        // Delete children first (each detaches itself from us).
        let children = std::mem::take(&mut *self.0.children.lock().unwrap());
        for child in children {
            child.delete();
        }
        self.new_parent(None);
    }

    /// Parent owner, if any (PG's `ResourceOwnerGetParent`).
    pub fn parent(&self) -> Option<ResourceOwner> {
        self.0.parent.lock().unwrap().upgrade().map(ResourceOwner)
    }

    /// Reassign to a new parent, detaching from the old one
    /// (PG's `ResourceOwnerNewParent`).
    pub fn new_parent(&self, new_parent: Option<&ResourceOwner>) {
        if let Some(old) = self.0.parent.lock().unwrap().upgrade() {
            old.children
                .lock()
                .unwrap()
                .retain(|c| !Arc::ptr_eq(&c.0, &self.0));
        }
        match new_parent {
            Some(parent) => {
                *self.0.parent.lock().unwrap() = Arc::downgrade(&parent.0);
                parent.0.children.lock().unwrap().push(self.clone());
            }
            None => *self.0.parent.lock().unwrap() = Weak::new(),
        }
    }
}

/// RAII handle for one registered resource. `Drop` releases it (scope-exit /
/// early release / unwind). Generational dedup makes this a no-op if the owner's
/// phased teardown already drained the entry.
pub struct ResourceGuard {
    owner: Weak<OwnerInner>,
    phase: ResourceReleasePhase,
    key: Key<Entry>,
}

impl ResourceGuard {
    /// Take the entry out of the owner, if it still resolves. Used by `Drop`,
    /// `release_now`, and `forget`. Lock dropped before the caller acts.
    fn take(&self) -> Option<Entry> {
        let owner = self.owner.upgrade()?;
        let mut locked = owner.locked.lock().unwrap();
        locked.phases[phase_index(self.phase)].remove(self.key)
    }

    /// Deregister WITHOUT releasing: the caller has taken ownership of the
    /// resource (PG's `ResourceOwnerForget`).
    pub fn forget(self) {
        let _ = self.take();
        std::mem::forget(self); // skip Drop's release
    }

    /// Release immediately, consuming the guard.
    pub fn release_now(self) {
        // Drop does the work; just let it run.
        drop(self);
    }
}

impl Drop for ResourceGuard {
    fn drop(&mut self) {
        if let Some(entry) = self.take() {
            (entry.release)();
        }
    }
}

// ---------------------------------------------------------------------------
// task-local current owner (PG's CurrentResourceOwner process global)
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// The current task's owner. Published by [`scope`].
    static CURRENT_RESOURCE_OWNER: ResourceOwner;
}

// TODO(step14): CurTransactionResourceOwner / TopTransactionResourceOwner /
// AuxProcessResourceOwner become Session state when transactions land. For now
// only the CurrentResourceOwner accessor exists.

/// The current task's owner. Panics if not inside a [`scope`].
pub fn current() -> ResourceOwner {
    try_current().expect("no ResourceOwner in scope for this task")
}

/// The current task's owner, or `None` if not inside a [`scope`].
pub fn try_current() -> Option<ResourceOwner> {
    CURRENT_RESOURCE_OWNER.try_with(|o| o.clone()).ok()
}

/// Run `f` with `owner` published as the task-local current owner.
pub async fn scope<F, T>(owner: ResourceOwner, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT_RESOURCE_OWNER.scope(owner, f).await
}

// ---------------------------------------------------------------------------
// Aux-process owner (PG's CreateAuxProcessResourceOwner / ReleaseAuxProcessResources)
// ---------------------------------------------------------------------------

/// Create an owner for an auxiliary task. (PG also wires an on-exit callback;
/// under the single-process model the caller releases it explicitly.)
pub fn create_aux_process_resource_owner() -> ResourceOwner {
    ResourceOwner::create(None, "AuxiliaryProcess")
}

/// Release all resources of an aux owner (does not delete it).
pub fn release_aux_process_resources(owner: &ResourceOwner, is_commit: bool) {
    owner.release_all(is_commit, true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::resowner::{
        RELEASE_PRIO_BUFFER_IOS, RELEASE_PRIO_BUFFER_PINS, RELEASE_PRIO_FILES,
    };
    use std::sync::{Arc, Mutex};

    type Log = Arc<Mutex<Vec<&'static str>>>;

    fn record(log: &Log, id: &'static str) -> impl FnOnce() + Send + 'static {
        let log = log.clone();
        move || log.lock().unwrap().push(id)
    }

    #[test]
    fn phase_order_independent_of_registration_order() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        // Register out of phase order.
        let g3 = owner.remember(ResourceReleasePhase::AfterLocks, 100, "after", record(&log, "after"));
        let g1 = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "before", record(&log, "before"));
        let g2 = owner.remember(ResourceReleasePhase::Locks, 100, "locks", record(&log, "locks"));
        std::mem::forget(g1);
        std::mem::forget(g2);
        std::mem::forget(g3);

        owner.release_all(false, true);
        assert_eq!(*log.lock().unwrap(), vec!["before", "locks", "after"]);
    }

    #[test]
    fn within_phase_priority_then_lifo() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        // Same phase, mixed priorities; two share a priority to prove LIFO.
        let a = owner.remember(ResourceReleasePhase::BeforeLocks, RELEASE_PRIO_BUFFER_PINS, "pin1", record(&log, "pin1"));
        let b = owner.remember(ResourceReleasePhase::BeforeLocks, RELEASE_PRIO_BUFFER_IOS, "io1", record(&log, "io1"));
        let c = owner.remember(ResourceReleasePhase::BeforeLocks, RELEASE_PRIO_BUFFER_IOS, "io2", record(&log, "io2"));
        for g in [a, b, c] {
            std::mem::forget(g);
        }

        owner.release_all(false, true);
        // io priority (100) before pin priority (200); within io, LIFO -> io2 then io1.
        assert_eq!(*log.lock().unwrap(), vec!["io2", "io1", "pin1"]);
    }

    #[test]
    fn early_release_then_teardown_no_double_release() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        let g = owner.remember(ResourceReleasePhase::AfterLocks, RELEASE_PRIO_FILES, "f", record(&log, "f"));
        drop(g); // early release
        assert_eq!(*log.lock().unwrap(), vec!["f"]);

        owner.release_all(false, true); // must not release again
        assert_eq!(*log.lock().unwrap(), vec!["f"]);
    }

    #[test]
    fn teardown_then_stale_guard_drop_is_noop() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        let g = owner.remember(ResourceReleasePhase::AfterLocks, RELEASE_PRIO_FILES, "f", record(&log, "f"));

        owner.release_all(false, true); // drains the entry, releases once
        assert_eq!(*log.lock().unwrap(), vec!["f"]);

        drop(g); // stale key -> no-op, no panic, no double release
        assert_eq!(*log.lock().unwrap(), vec!["f"]);
    }

    #[test]
    fn children_release_before_parent() {
        let log: Log = Arc::default();
        let parent = ResourceOwner::create(None, "parent");
        let child = ResourceOwner::create(Some(&parent), "child");

        let pg = parent.remember(ResourceReleasePhase::BeforeLocks, 100, "p", record(&log, "p"));
        let cg = child.remember(ResourceReleasePhase::BeforeLocks, 100, "c", record(&log, "c"));
        std::mem::forget(pg);
        std::mem::forget(cg);

        parent.release_all(false, true);
        assert_eq!(*log.lock().unwrap(), vec!["c", "p"]);
    }

    #[test]
    fn forget_releases_nothing() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        let g = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "x", record(&log, "x"));
        g.forget();

        owner.release_all(false, true);
        assert!(log.lock().unwrap().is_empty(), "forgotten resource must never release");
    }

    #[test]
    fn release_now_releases_once() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        let g = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "x", record(&log, "x"));
        g.release_now();
        assert_eq!(*log.lock().unwrap(), vec!["x"]);
        owner.release_all(false, true);
        assert_eq!(*log.lock().unwrap(), vec!["x"]);
    }

    #[test]
    fn commit_leak_warning_path_does_not_panic() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        let g = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "leaked", record(&log, "leaked"));
        std::mem::forget(g);
        // is_commit = true with a non-empty phase exercises the WARNING path.
        owner.release_all(true, true);
        assert_eq!(*log.lock().unwrap(), vec!["leaked"]);
    }

    #[test]
    fn panic_in_release_closure_is_isolated() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        // Three resources in one phase, same priority; the middle (by LIFO) panics.
        let a = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "a", record(&log, "a"));
        let log2 = log.clone();
        let b = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "b", move || {
            log2.lock().unwrap().push("b-start");
            panic!("boom");
        });
        let c = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "c", record(&log, "c"));
        for g in [a, b, c] {
            std::mem::forget(g);
        }

        // Must not propagate the panic.
        owner.release_all(false, true);

        let entries = log.lock().unwrap().clone();
        // The two non-panicking resources released; the panicking one started.
        assert!(entries.contains(&"a"), "a must release: {entries:?}");
        assert!(entries.contains(&"c"), "c must release: {entries:?}");
        assert!(entries.contains(&"b-start"), "b's closure must have run: {entries:?}");
    }

    #[test]
    fn panic_in_earlier_phase_does_not_block_later_phase() {
        let log: Log = Arc::default();
        let owner = ResourceOwner::create(None, "t");
        let early = owner.remember(ResourceReleasePhase::BeforeLocks, 100, "early", || panic!("early boom"));
        let late = owner.remember(ResourceReleasePhase::AfterLocks, 100, "late", record(&log, "late"));
        std::mem::forget(early);
        std::mem::forget(late);

        owner.release_all(false, true);
        assert_eq!(*log.lock().unwrap(), vec!["late"]);
    }

    #[test]
    fn panicking_child_closure_does_not_abort_parent_teardown() {
        let log: Log = Arc::default();
        let parent = ResourceOwner::create(None, "parent");
        let child = ResourceOwner::create(Some(&parent), "child");

        let cg = child.remember(ResourceReleasePhase::BeforeLocks, 100, "c", || panic!("child boom"));
        let pg = parent.remember(ResourceReleasePhase::BeforeLocks, 100, "p", record(&log, "p"));
        std::mem::forget(cg);
        std::mem::forget(pg);

        // Child releases first and panics; parent must still tear down.
        parent.release_all(false, true);
        assert_eq!(*log.lock().unwrap(), vec!["p"]);
    }

    #[test]
    fn new_parent_reparents_and_redirects_teardown() {
        let log: Log = Arc::default();
        let a = ResourceOwner::create(None, "A");
        let b = ResourceOwner::create(None, "B");
        let c = ResourceOwner::create(Some(&a), "C");

        c.new_parent(Some(&b));

        // A no longer parents C; B does.
        assert!(
            !a.0.children.lock().unwrap().iter().any(|ch| Arc::ptr_eq(&ch.0, &c.0)),
            "A's children must no longer contain C"
        );
        assert!(
            b.0.children.lock().unwrap().iter().any(|ch| Arc::ptr_eq(&ch.0, &c.0)),
            "B's children must contain C"
        );

        let cg = c.remember(ResourceReleasePhase::BeforeLocks, 100, "c", record(&log, "c"));
        std::mem::forget(cg);

        a.release_all(false, true); // C is no longer under A
        assert!(log.lock().unwrap().is_empty(), "C must not release via A: {:?}", *log.lock().unwrap());

        b.release_all(false, true); // C is under B now
        assert_eq!(*log.lock().unwrap(), vec!["c"]);
    }

    #[test]
    fn delete_detaches_from_parent() {
        let parent = ResourceOwner::create(None, "parent");
        let child = ResourceOwner::create(Some(&parent), "child");

        // Drained owner: delete must succeed and detach from the parent.
        child.clone().delete();
        assert!(
            !parent.0.children.lock().unwrap().iter().any(|ch| Arc::ptr_eq(&ch.0, &child.0)),
            "parent's children must no longer contain the deleted child"
        );
    }

    #[tokio::test]
    async fn scope_publishes_current() {
        let owner = ResourceOwner::create(None, "task");
        assert!(try_current().is_none());
        scope(owner.clone(), async {
            assert_eq!(current().name(), "task");
        })
        .await;
    }
}
