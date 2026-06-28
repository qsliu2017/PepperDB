//! Combo command ID support routines. Translated from backend/utils/time/combocid.c.
//!
//! A heap tuple header overlays `cmin` and `cmax` in a single field to save
//! space, which works because a tuple is rarely both inserted and deleted by
//! the same transaction and neither value needs to survive past that
//! transaction. When the inserting transaction does delete its own tuple, a
//! "combo" command id is stored in the header instead, and this module maps it
//! back to the real `(cmin, cmax)` pair. The mapping lives in two structures:
//! an array indexed by combo cid, and a hash table keyed by `(cmin, cmax)` so
//! that repeated pairs reuse an existing combo cid. The structures are
//! transaction-local and discarded at end of transaction.
//!
//! In PostgreSQL the array and hash table are allocated in
//! `TopTransactionContext` of a single backend process. Here they are held in a
//! per-task `RefCell<ComboCidState>` carried by a tokio task-local, so each
//! concurrent backend has its own state; `combocid_scope` establishes it for
//! the duration of a backend's work and `at_eo_xact_combo_cid` clears it at end
//! of transaction. All entry points are synchronous, so the borrow of the
//! task-local is never held across an await point. The parallel-worker
//! serialization routines preserve PostgreSQL's wire layout: an `i32` count
//! followed by that many `(cmin, cmax)` `u32` pairs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::cell::RefCell;
use std::collections::HashMap;

use crate::access::htup_details::{HEAP_COMBOCID, HEAP_MOVED, HeapTupleHeaderData};
use crate::c::CommandId;

/// combocid.c `ComboCidKeyData`: the (cmin, cmax) pair used as a hash key.
/// `#[repr(C)]` locks the no-padding invariant the serialization size math
/// relies on (C combocid.c:252 "We assume there is no struct padding").
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ComboCidKeyData {
    pub cmin: CommandId,
    pub cmax: CommandId,
}

/// combocid.c `ComboCidEntryData`: a key plus the combo cid it maps to.
#[derive(Clone, Copy)]
pub struct ComboCidEntryData {
    pub key: ComboCidKeyData,
    pub combocid: CommandId,
}

/// Per-task combo-cid state. `map` reuses combo cids for repeated (cmin, cmax)
/// pairs (C `comboHash`); `entries[i]` is the pair for combo cid `i`
/// (C `comboCids` array, indexed by combo cid).
#[derive(Default)]
struct ComboCidState {
    map: HashMap<ComboCidKeyData, CommandId>,
    entries: Vec<ComboCidEntryData>,
}

tokio::task_local! {
    static COMBO_CID_STATE: RefCell<ComboCidState>;
}

/// Run `f` with a fresh per-task combo-cid state in scope (backend entry).
pub async fn combocid_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    COMBO_CID_STATE
        .scope(RefCell::new(ComboCidState::default()), f)
        .await
}

/// True iff a per-task combo-cid state is in scope (some tests run outside one).
fn in_scope() -> bool {
    COMBO_CID_STATE.try_with(|_| ()).is_ok()
}

// ===========================================================================
// External API
// ===========================================================================

/// combocid.c `HeapTupleHeaderGetCmin`.
pub fn HeapTupleHeaderGetCmin(tup: &HeapTupleHeaderData) -> CommandId {
    let cid = tup.get_raw_command_id();

    debug_assert_eq!((tup.t_infomask & HEAP_MOVED), 0);
    // Assert(TransactionIdIsCurrentTransactionId(GetXmin)): xact.c, step 14d.

    if (tup.t_infomask & HEAP_COMBOCID) != 0 {
        get_real_cmin(cid)
    } else {
        cid
    }
}

/// combocid.c `HeapTupleHeaderGetCmax`.
pub fn HeapTupleHeaderGetCmax(tup: &HeapTupleHeaderData) -> CommandId {
    let cid = tup.get_raw_command_id();

    debug_assert_eq!((tup.t_infomask & HEAP_MOVED), 0);
    // Assert(CritSectionCount > 0 || IsCurrentTransactionId(GetUpdateXid)):
    // the xid check is xact.c (step 14d); CritSectionCount lives in miscadmin.

    if (tup.t_infomask & HEAP_COMBOCID) != 0 {
        get_real_cmax(cid)
    } else {
        cid
    }
}

/// combocid.c `HeapTupleHeaderAdjustCmax`. Returns (cmax, iscombo) (C out-params).
///
/// Staging: the "inserted by our own (sub)transaction" test needs
/// `TransactionIdIsCurrentTransactionId` (xact.c, step 14d). Until then a tuple
/// whose xmin is not committed is conservatively treated as ours, which matches
/// C behavior for the single-backend foundation (no other live writers yet).
pub fn HeapTupleHeaderAdjustCmax(tup: &HeapTupleHeaderData, cmax: CommandId) -> (CommandId, bool) {
    if !tup.xmin_committed()
        && crate::access::xact::TransactionIdIsCurrentTransactionId(tup.get_raw_xmin())
    {
        let cmin = HeapTupleHeaderGetCmin(tup);
        (get_combo_command_id(cmin, cmax), true)
    } else {
        (cmax, false)
    }
}

/// combocid.c `AtEOXact_ComboCid`: forget all combo cids at end of transaction.
pub fn at_eo_xact_combo_cid() {
    if in_scope() {
        COMBO_CID_STATE.with(|s| {
            let mut s = s.borrow_mut();
            s.map.clear();
            s.entries.clear();
        });
    }
}

// ===========================================================================
// Internal routines
// ===========================================================================

/// combocid.c `GetComboCommandId`: combo cid mapping `cmin` -> `cmax`, reusing
/// an existing combo cid when the pair has been seen before.
pub fn get_combo_command_id(cmin: CommandId, cmax: CommandId) -> CommandId {
    let key = ComboCidKeyData { cmin, cmax };
    COMBO_CID_STATE.with(|s| {
        let mut s = s.borrow_mut();
        if let Some(&existing) = s.map.get(&key) {
            return existing;
        }
        let combocid = CommandId(s.entries.len() as u32);
        s.entries.push(ComboCidEntryData { key, combocid });
        s.map.insert(key, combocid);
        combocid
    })
}

/// combocid.c `GetRealCmin`.
fn get_real_cmin(combocid: CommandId) -> CommandId {
    COMBO_CID_STATE.with(|s| {
        let s = s.borrow();
        s.entries[combocid.0 as usize].key.cmin
    })
}

/// combocid.c `GetRealCmax`.
fn get_real_cmax(combocid: CommandId) -> CommandId {
    COMBO_CID_STATE.with(|s| {
        let s = s.borrow();
        s.entries[combocid.0 as usize].key.cmax
    })
}

// ===========================================================================
// Parallel-worker serialization of combo-cid state
// ===========================================================================

/// combocid.c `EstimateComboCIDStateSpace`: bytes to serialize the state.
/// Layout: an `i32` count followed by `count` (cmin, cmax) `u32` pairs.
pub fn estimate_combo_cid_state_space() -> usize {
    let used = COMBO_CID_STATE.with(|s| s.borrow().entries.len());
    combo_cid_state_size(used)
}

/// Serialized size for `used` combo cids; checked like C `add_size`/`mul_size`.
fn combo_cid_state_size(used: usize) -> usize {
    debug_assert_eq!(std::mem::size_of::<ComboCidKeyData>(), 8);
    let size = std::mem::size_of::<ComboCidKeyData>()
        .checked_mul(used)
        .and_then(|n| n.checked_add(std::mem::size_of::<i32>()));
    // TODO(panic): migrate to Result + ?
    assert!(size.is_some(), "requested size overflows usize");
    size.unwrap()
}

/// combocid.c `SerializeComboCIDState`: write count + (cmin, cmax) pairs into
/// `start_address`. Panics (elog ERROR) if `maxsize` is too small.
pub fn serialize_combo_cid_state(maxsize: usize, start_address: &mut [u8]) {
    let used = COMBO_CID_STATE.with(|s| s.borrow().entries.len());

    let needed = combo_cid_state_size(used);
    // TODO(panic): migrate to Result + ?
    assert!(!(needed > maxsize || needed > start_address.len()), "not enough space to serialize ComboCID state");

    start_address[0..4].copy_from_slice(&(used as i32).to_ne_bytes());
    COMBO_CID_STATE.with(|s| {
        let s = s.borrow();
        let mut off = 4;
        for e in &s.entries {
            start_address[off..off + 4].copy_from_slice(&e.key.cmin.0.to_ne_bytes());
            start_address[off + 4..off + 8].copy_from_slice(&e.key.cmax.0.to_ne_bytes());
            off += 8;
        }
    });
}

/// combocid.c `RestoreComboCIDState`: reconstruct the combo cids from a
/// serialized buffer. Valid only when this backend has no combo cids yet.
pub fn restore_combo_cid_state(combo_cid_state: &[u8]) {
    debug_assert!(COMBO_CID_STATE.with(|s| s.borrow().entries.is_empty()));

    let num_elements = i32::from_ne_bytes(combo_cid_state[0..4].try_into().unwrap());
    let mut off = 4;
    for i in 0..num_elements {
        let cmin = CommandId(u32::from_ne_bytes(
            combo_cid_state[off..off + 4].try_into().unwrap(),
        ));
        let cmax = CommandId(u32::from_ne_bytes(
            combo_cid_state[off + 4..off + 8].try_into().unwrap(),
        ));
        off += 8;
        let cid = get_combo_command_id(cmin, cmax);
        // TODO(panic): migrate to Result + ?
        assert!(cid.0 == i as u32, "unexpected command ID while restoring combo CIDs");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn combo_cid_stable_and_distinct() {
        combocid_scope(async {
            let a = get_combo_command_id(CommandId(1), CommandId(2));
            let b = get_combo_command_id(CommandId(1), CommandId(2));
            assert_eq!(a, b, "same (cmin,cmax) reuses the combo cid");

            let c = get_combo_command_id(CommandId(3), CommandId(4));
            assert_ne!(a, c, "different pair gets a different combo cid");
            assert_eq!(a, CommandId(0));
            assert_eq!(c, CommandId(1));

            assert_eq!(get_real_cmin(a), CommandId(1));
            assert_eq!(get_real_cmax(a), CommandId(2));
            assert_eq!(get_real_cmin(c), CommandId(3));
            assert_eq!(get_real_cmax(c), CommandId(4));
        })
        .await;
    }

    #[tokio::test]
    async fn at_eo_xact_resets() {
        combocid_scope(async {
            let a = get_combo_command_id(CommandId(7), CommandId(9));
            assert_eq!(a, CommandId(0));
            at_eo_xact_combo_cid();
            // After reset the next pair starts numbering from 0 again.
            let b = get_combo_command_id(CommandId(10), CommandId(11));
            assert_eq!(b, CommandId(0));
        })
        .await;
    }

    #[tokio::test]
    async fn serialize_roundtrip() {
        combocid_scope(async {
            get_combo_command_id(CommandId(1), CommandId(2));
            get_combo_command_id(CommandId(5), CommandId(8));
            let sz = estimate_combo_cid_state_space();
            let mut buf = vec![0u8; sz];
            serialize_combo_cid_state(sz, &mut buf);

            // Restore into a fresh task and verify the cids line up.
            combocid_scope(async move {
                restore_combo_cid_state(&buf);
                assert_eq!(
                    get_combo_command_id(CommandId(1), CommandId(2)),
                    CommandId(0)
                );
                assert_eq!(
                    get_combo_command_id(CommandId(5), CommandId(8)),
                    CommandId(1)
                );
            })
            .await;
        })
        .await;
    }
}
