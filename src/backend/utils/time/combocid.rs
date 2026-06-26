//! Translated from PostgreSQL src/backend/utils/time/combocid.c
//!
//! Combo command ID support. cmin and cmax are overlaid in one tuple-header
//! field; when the inserting transaction also deletes the tuple we store a
//! "combo" command id that maps back to the real (cmin, cmax) pair through a
//! per-backend array + hash table.
//!
//! Per-task state (rules s6.1 / s7): the C file kept `comboHash`/`comboCids` in
//! TopTransactionContext, destroyed at end of xact. Here they are a per-task
//! `task_local!` `RefCell<ComboCidState>`; `AtEOXact_ComboCid` resets it. NEVER
//! hold the `RefCell` borrow across `.await` (every function here is sync, so
//! there is no `.await` to begin with).

use std::cell::RefCell;
use std::collections::HashMap;

use crate::access::htup_details::{HeapTupleHeaderData, HEAP_COMBOCID, HEAP_MOVED};
use crate::c::CommandId;

/// combocid.c `ComboCidKeyData`: the (cmin, cmax) pair used as a hash key.
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

    debug_assert!((tup.t_infomask & HEAP_MOVED) == 0);
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

    debug_assert!((tup.t_infomask & HEAP_MOVED) == 0);
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
    std::mem::size_of::<i32>() + std::mem::size_of::<ComboCidKeyData>() * used
}

/// combocid.c `SerializeComboCIDState`: write count + (cmin, cmax) pairs into
/// `start_address`. Panics (elog ERROR) if `maxsize` is too small.
pub fn serialize_combo_cid_state(maxsize: usize, start_address: &mut [u8]) {
    let used = COMBO_CID_STATE.with(|s| s.borrow().entries.len());

    let needed = std::mem::size_of::<i32>() + std::mem::size_of::<ComboCidKeyData>() * used;
    if needed > maxsize || needed > start_address.len() {
        // TODO(panic): migrate to Result + ?
        panic!("not enough space to serialize ComboCID state");
    }

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
        if cid.0 != i as u32 {
            // TODO(panic): migrate to Result + ?
            panic!("unexpected command ID while restoring combo CIDs");
        }
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
                assert_eq!(get_combo_command_id(CommandId(1), CommandId(2)), CommandId(0));
                assert_eq!(get_combo_command_id(CommandId(5), CommandId(8)), CommandId(1));
            })
            .await;
        })
        .await;
    }
}
