//! Translated from PostgreSQL src/include/utils/resowner.h
//
// Resource-owner tracking (query-lifespan resources freed at the right time).
// In-memory subsystem. ResourceOwner is opaque in C; kept as an opaque handle here.
// Release happens in three ordered phases (an enum) at integer priorities (consts).
// Per-kind behavior (ResourceOwnerDesc) is a small callback table.

use crate::postgres::Datum;

/// Opaque resource-owner handle (`struct ResourceOwnerData *`). TODO(memory):
/// becomes an owned/RAII type under single-process model.
pub enum ResourceOwnerData {}
pub type ResourceOwner = *mut ResourceOwnerData;

// Globally known ResourceOwners (process globals). TODO(global): Session-thread.
pub static mut CurrentResourceOwner: ResourceOwner = std::ptr::null_mut();
pub static mut CurTransactionResourceOwner: ResourceOwner = std::ptr::null_mut();
pub static mut TopTransactionResourceOwner: ResourceOwner = std::ptr::null_mut();
pub static mut AuxProcessResourceOwner: ResourceOwner = std::ptr::null_mut();

/// Release phases, in order (sequential ordinal enum, C starts at 1).
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

/// Per-resource-kind callbacks. `DebugPrint` is optional (NULL -> generic format).
pub struct ResourceOwnerDesc {
    pub name: &'static str,
    pub release_phase: ResourceReleasePhase,
    pub release_priority: ResourceReleasePriority,
    pub release_resource: fn(res: Datum),
    pub debug_print: Option<fn(res: Datum) -> String>,
}

/// Loadable-module hook invoked during ResourceOwnerRelease. The `void *arg` becomes
/// a captured closure at the registration site (function-mapping.md 6.3).
pub type ResourceReleaseCallback = fn(phase: ResourceReleasePhase, is_commit: bool, is_top_level: bool);

// === generic routines ===

pub fn ResourceOwnerCreate(_parent: ResourceOwner, _name: &str) -> ResourceOwner {
    unimplemented!()
}
pub fn ResourceOwnerRelease(
    _owner: ResourceOwner,
    _phase: ResourceReleasePhase,
    _is_commit: bool,
    _is_top_level: bool,
) {
    unimplemented!()
}
pub fn ResourceOwnerDelete(_owner: ResourceOwner) {
    unimplemented!()
}
pub fn ResourceOwnerGetParent(_owner: ResourceOwner) -> ResourceOwner {
    unimplemented!()
}
pub fn ResourceOwnerNewParent(_owner: ResourceOwner, _newparent: ResourceOwner) {
    unimplemented!()
}

pub fn ResourceOwnerEnlarge(_owner: ResourceOwner) {
    unimplemented!()
}
pub fn ResourceOwnerRemember(_owner: ResourceOwner, _value: Datum, _kind: &ResourceOwnerDesc) {
    unimplemented!()
}
pub fn ResourceOwnerForget(_owner: ResourceOwner, _value: Datum, _kind: &ResourceOwnerDesc) {
    unimplemented!()
}

pub fn ResourceOwnerReleaseAllOfKind(_owner: ResourceOwner, _kind: &ResourceOwnerDesc) {
    unimplemented!()
}

pub fn RegisterResourceReleaseCallback(_callback: ResourceReleaseCallback) {
    unimplemented!()
}
pub fn UnregisterResourceReleaseCallback(_callback: ResourceReleaseCallback) {
    unimplemented!()
}

pub fn CreateAuxProcessResourceOwner() {
    unimplemented!()
}
pub fn ReleaseAuxProcessResources(_is_commit: bool) {
    unimplemented!()
}

// === special support: local lock management ===

// TODO(struct-forward): real LOCALLOCK in storage/lock.h (lock/lock.h).
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage::lock::LOCALLOCK in Phase 2")]
pub enum LOCALLOCK {}

#[allow(deprecated)]
pub fn ResourceOwnerRememberLock(_owner: ResourceOwner, _locallock: *mut LOCALLOCK) {
    unimplemented!()
}
#[allow(deprecated)]
pub fn ResourceOwnerForgetLock(_owner: ResourceOwner, _locallock: *mut LOCALLOCK) {
    unimplemented!()
}

// === special support: AIO ===

// TODO(struct-forward): real dlist_node in lib/ilist.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::lib::ilist::dlist_node in Phase 2")]
pub struct dlist_node;

#[allow(deprecated)]
pub fn ResourceOwnerRememberAioHandle(_owner: ResourceOwner, _ioh_node: *mut dlist_node) {
    unimplemented!()
}
#[allow(deprecated)]
pub fn ResourceOwnerForgetAioHandle(_owner: ResourceOwner, _ioh_node: *mut dlist_node) {
    unimplemented!()
}
