//! Translated from PostgreSQL src/include/utils/resowner.h
//
// Resource-owner tracking (query-lifespan resources freed at the right time).
// Release happens in three ordered phases (an enum) at integer priorities
// (consts). The real type and behavior live in the backend module
// (`crate::backend::utils::resowner::resowner`) as idiomatic RAII; this header
// keeps the phase enum + RELEASE_PRIO_* constants and re-exports the type, with
// the C-named free functions kept as deprecated shims for cross-reference.

/// Release phases, in order (sequential ordinal enum, C starts at 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ResourceReleasePhase {
    BeforeLocks = 1,
    Locks,
    AfterLocks,
}

pub type ResourceReleasePriority = u32;

// Built-in BEFORE_LOCKS priorities.
pub const RELEASE_PRIO_BUFFER_IOS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_BUFFER_PINS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_RELCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_DSMS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_JIT_CONTEXTS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_CRYPTOHASH_CONTEXTS: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_HMAC_CONTEXTS: ResourceReleasePriority = 700;

// Built-in AFTER_LOCKS priorities.
pub const RELEASE_PRIO_CATCACHE_REFS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_CATCACHE_LIST_REFS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_PLANCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_TUPDESC_REFS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_SNAPSHOT_REFS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_FILES: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_WAITEVENTSETS: ResourceReleasePriority = 700;

pub const RELEASE_PRIO_FIRST: ResourceReleasePriority = 1;
pub const RELEASE_PRIO_LAST: ResourceReleasePriority = u32::MAX;

// The real types live in the backend module; re-export them on the header API
// surface so `crate::utils::resowner::ResourceOwner` keeps resolving.
pub use crate::backend::utils::resowner::resowner::{
    create_aux_process_resource_owner, current, release_aux_process_resources, scope, try_current,
    ResourceGuard, ResourceOwner,
};

// TODO(step14): CurrentResourceOwner/CurTransactionResourceOwner/
// TopTransactionResourceOwner/AuxProcessResourceOwner were process globals.
// CurrentResourceOwner is now task-local (use `current()`); the others become
// Session state with transactions.

// === TOMBSTONED by redesign ===
//
// The generic `(Datum value, ResourceOwnerDesc*)` array+hash registry is gone:
// a tracked resource is now a release closure captured at the `remember` call
// site, not a Datum keyed by a per-kind descriptor. Consequently:
//
//   * ResourceOwnerDesc (per-kind callback table) -- subsumed by the closure
//     passed to `ResourceOwner::remember(phase, priority, name, release)`.
//   * ResourceReleaseCallback / Register/UnregisterResourceReleaseCallback --
//     loadable-module hooks; no extensions in this design (000).
//   * ResourceOwnerEnlarge -- the per-phase GenSlab grows itself.
//
// They are kept below only as deprecated no-ops / type aliases so mechanical
// port call sites still compile.

/// Tombstoned: per-kind descriptor subsumed by the `remember` closure. Kept as a
/// minimal value so any `&ResourceOwnerDesc` call site still type-checks.
#[deprecated(note = "pass a release closure to ResourceOwner::remember instead")]
pub struct ResourceOwnerDesc {
    pub name: &'static str,
    pub release_phase: ResourceReleasePhase,
    pub release_priority: ResourceReleasePriority,
}

/// Tombstoned: loadable-module release hook (no extensions).
#[deprecated(note = "loadable-module hooks removed; define release closures")]
pub type ResourceReleaseCallback = fn(phase: ResourceReleasePhase, is_commit: bool, is_top_level: bool);

// === generic routines: deprecated C-named shims delegating to methods ===

#[deprecated(note = "use ResourceOwner::create")]
#[inline]
pub fn ResourceOwnerCreate(parent: Option<&ResourceOwner>, name: &str) -> ResourceOwner {
    ResourceOwner::create(parent, name)
}

#[deprecated(note = "use owner.release(phase, is_commit, is_top_level)")]
#[inline]
pub fn ResourceOwnerRelease(
    owner: &ResourceOwner,
    phase: ResourceReleasePhase,
    is_commit: bool,
    is_top_level: bool,
) {
    owner.release(phase, is_commit, is_top_level)
}

#[deprecated(note = "use owner.delete()")]
#[inline]
pub fn ResourceOwnerDelete(owner: ResourceOwner) {
    owner.delete()
}

#[deprecated(note = "use owner.parent()")]
#[inline]
pub fn ResourceOwnerGetParent(owner: &ResourceOwner) -> Option<ResourceOwner> {
    owner.parent()
}

#[deprecated(note = "use owner.new_parent(new_parent)")]
#[inline]
pub fn ResourceOwnerNewParent(owner: &ResourceOwner, new_parent: Option<&ResourceOwner>) {
    owner.new_parent(new_parent)
}

/// Tombstoned: the per-phase slab grows itself; reservation before acquire is
/// unnecessary. No-op.
#[deprecated(note = "no-op: the GenSlab grows itself")]
#[inline]
pub fn ResourceOwnerEnlarge(_owner: &ResourceOwner) {}

#[deprecated(note = "use owner.remember(phase, priority, name, release_closure)")]
#[inline]
#[allow(deprecated)]
pub fn ResourceOwnerRemember(
    owner: &ResourceOwner,
    kind: &ResourceOwnerDesc,
    release: impl FnOnce() + Send + 'static,
) -> ResourceGuard {
    owner.remember(kind.release_phase, kind.release_priority, kind.name, release)
}

#[deprecated(note = "use ResourceGuard::forget")]
#[inline]
pub fn ResourceOwnerForget(guard: ResourceGuard) {
    guard.forget()
}

/// Tombstoned: the "release all of a kind" scan existed for the Datum-hash
/// registry. With typed closures, hold the relevant guards and drop them. No-op.
#[deprecated(note = "no-op: hold and drop the relevant ResourceGuards")]
#[inline]
#[allow(deprecated)]
pub fn ResourceOwnerReleaseAllOfKind(_owner: &ResourceOwner, _kind: &ResourceOwnerDesc) {}

/// Tombstoned: loadable-module hook (no extensions). No-op.
#[deprecated(note = "loadable-module hooks removed")]
#[inline]
#[allow(deprecated)]
pub fn RegisterResourceReleaseCallback(_callback: ResourceReleaseCallback) {}

/// Tombstoned: loadable-module hook (no extensions). No-op.
#[deprecated(note = "loadable-module hooks removed")]
#[inline]
#[allow(deprecated)]
pub fn UnregisterResourceReleaseCallback(_callback: ResourceReleaseCallback) {}

#[deprecated(note = "use create_aux_process_resource_owner()")]
#[inline]
pub fn CreateAuxProcessResourceOwner() -> ResourceOwner {
    create_aux_process_resource_owner()
}

#[deprecated(note = "use release_aux_process_resources(owner, is_commit)")]
#[inline]
pub fn ReleaseAuxProcessResources(owner: &ResourceOwner, is_commit: bool) {
    release_aux_process_resources(owner, is_commit)
}

// === special support: local lock management ===
//
// TODO(step15): real locks don't exist yet. Locks register in the Locks phase
// via the generic mechanism (owner.remember(Locks, ...)). These C-named shims
// are thin stubs to be wired once LOCALLOCK release lands.

#[deprecated(note = "TODO(step15): register the lock release in the Locks phase via owner.remember")]
#[inline]
pub fn ResourceOwnerRememberLock(_owner: &ResourceOwner) {
    // TODO(step15): owner.remember(Locks, RELEASE_PRIO_FIRST, "lock", || release(locallock))
}

#[deprecated(note = "TODO(step15): drop the lock's ResourceGuard")]
#[inline]
pub fn ResourceOwnerForgetLock(_owner: &ResourceOwner) {
    // TODO(step15)
}

// === special support: AIO ===
//
// TODO(step F1): AIO handles register in the BeforeLocks phase. Stubs until the
// AIO subsystem lands.

#[deprecated(note = "TODO(F1): register the AIO handle release via owner.remember")]
#[inline]
pub fn ResourceOwnerRememberAioHandle(_owner: &ResourceOwner) {
    // TODO(F1)
}

#[deprecated(note = "TODO(F1): drop the AIO handle's ResourceGuard")]
#[inline]
pub fn ResourceOwnerForgetAioHandle(_owner: &ResourceOwner) {
    // TODO(F1)
}
