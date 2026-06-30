//! Hash build-side node. Translated from backend/executor/nodeHash.c
//! (disposition: full for the M7 single in-memory batch; multi-batch spill,
//! skew optimization, and parallel hash are clean grow guards).
//!
//! A `Hash` node sits on the inner side of a `HashJoin`. It is NOT a tuple source
//! (its `ExecProcNode` is an error): the parent `HashJoin` drives it once via
//! `MultiExecHash`, which drains the Hash's child, evaluates the inner hashkey
//! expression(s) for each row, and inserts the row into the hash table keyed by the
//! combined hash value. The table is a single in-memory batch (`HashJoinTable`).
//!
//! Slot ownership / Send: the table owns the inner rows as deformed `(values,
//! isnull)` arrays grouped into buckets (a `HashMap<u64, Vec<usize>>` over a row
//! vector). `ecxt_outertuple` is set to the Hash's child row while the inner
//! hashkey is evaluated (the Hash has a single input -> OUTER_VAR after setrefs).
//! No lock/RefCell across the child `.await` (rules.md s5/s10).

use std::collections::HashMap;
use std::sync::Arc;

use crate::backend::executor::execExpr::exec_init_expr;
use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, ExprContext, ExprState, HashState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Hash;
use crate::postgres::Datum;
use crate::shared_state::SharedState;

/// A built single-batch hash table: the inner rows (deformed) plus a bucket map
/// from the combined hash value to the indexes of rows that hashed there. Owned +
/// Send.
#[derive(Default)]
pub struct HashJoinTable {
    pub rows: Vec<(Vec<Datum>, Vec<bool>)>,
    /// hash value -> indexes into `rows`. NULL-keyed rows are dropped (a NULL key
    /// never equals anything under the hashjoin's equality clause).
    pub buckets: HashMap<u64, Vec<usize>>,
}

/// Run-state pairing the PG `HashState` with its child plan-state, the compiled
/// inner hashkey expressions, and the (built-on-demand) table.
pub struct HashRun<'rel> {
    pub state: Box<HashState>,
    pub child: Box<PlanStateNode<'rel>>,
    /// the inner hashkey expressions (one per hash clause), compiled to scalar
    /// ExprStates that read the child row via `ecxt_outertuple` (OUTER_VAR).
    pub hashkeys: Vec<Box<ExprState>>,
    /// a scratch slot the child row is snapshotted into while the hashkey runs.
    pub key_slot: Box<TupleTableSlot>,
    /// the built table, populated by `multi_exec_hash`.
    pub table: Option<Box<HashJoinTable>>,
}

/// PG `ExecInitHash`: build the HashState over an initialized child and compile the
/// inner hashkey expressions.
pub fn exec_init_hash<'rel>(
    node: &Hash,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<HashRun<'rel>> {
    let _ = estate;
    let child_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitHash: child has no result descriptor"));

    let hashkeys: Vec<Box<ExprState>> = node
        .hashkeys
        .iter()
        .map(|k| {
            exec_init_expr(Some(k), None)
                .unwrap_or_else(|| unimplemented!("ExecInitHash: empty hashkey expr"))
        })
        .collect();

    Box::new(HashRun {
        state: Box::new(HashState {
            ps: PlanState {
                plan: Some(Node::Hash(Box::new(node.clone()))),
                ..PlanState::default()
            },
            ..HashState::default()
        }),
        child: Box::new(child),
        hashkeys,
        key_slot: make_tuple_table_slot(Some(child_desc), &TTS_OPS_VIRTUAL),
        table: None,
    })
}

/// PG `MultiExecHash`: drain the Hash's child, evaluating the inner hashkey for each
/// row and inserting it into the table bucketed by the combined hash value. Builds
/// the table once (idempotent: returns the existing table on a second call).
pub async fn multi_exec_hash(shared: Option<&Arc<SharedState>>, run: &mut HashRun<'_>) {
    if run.table.is_some() {
        return;
    }
    let mut table = Box::new(HashJoinTable::default());
    let mut econtext = ExprContext::default();

    while let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await {
        let (vals, nulls) = snapshot_slot(slot);
        store_into(&mut run.key_slot, &vals, &nulls);

        // Evaluate the inner hashkey(s) against the child row (OUTER_VAR).
        econtext.ecxt_outertuple = Some(std::mem::replace(
            &mut run.key_slot,
            make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
        ));
        let hashval = compute_hash(&mut run.hashkeys, &mut econtext);
        if let Some(s) = econtext.ecxt_outertuple.take() {
            run.key_slot = s;
        }

        if let Some(h) = hashval {
            let idx = table.rows.len();
            table.rows.push((vals, nulls));
            table.buckets.entry(h).or_default().push(idx);
        }
        // A NULL hash key never matches under the equality clause -> drop the row.
    }

    run.table = Some(table);
}

/// Evaluate the hashkey ExprStates and fold them into one combined hash value.
/// Returns `None` if any key is NULL (a NULL key matches nothing). The econtext's
/// source tuple (`ecxt_outertuple` for build, `ecxt_outertuple` for probe) must be
/// set by the caller before this runs.
pub fn compute_hash(hashkeys: &mut [Box<ExprState>], econtext: &mut ExprContext) -> Option<u64> {
    let mut acc: u64 = 0;
    for key in hashkeys.iter_mut() {
        let mut isnull = false;
        let evalfunc = key
            .evalfunc
            .unwrap_or_else(|| unimplemented!("compute_hash: hashkey not ready"));
        let datum = evalfunc(key, econtext, &mut isnull);
        if isnull {
            return None;
        }
        acc = mix_hash(acc, hash_datum(datum));
    }
    Some(acc)
}

/// Hash a Datum by its raw bits. For the M7 by-value key types (int4/int8/oid/bool)
/// the Datum bits ARE the canonical value, so equal values hash equally. By-ref
/// (text/varlena) hashing grows with the type-specific hash opclass functions.
fn hash_datum(d: Datum) -> u64 {
    // FNV-1a over the 8 datum bytes.
    let bytes = (d.0 as u64).to_le_bytes();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Combine two hash values (order-sensitive, for multi-key joins).
fn mix_hash(acc: u64, h: u64) -> u64 {
    acc.rotate_left(5).wrapping_add(h).wrapping_add(0x9e37_79b9_7f4a_7c15)
}

/// Store deformed `(values, isnull)` into a virtual slot.
fn store_into(slot: &mut TupleTableSlot, values: &[Datum], isnull: &[bool]) {
    ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(values);
    slot.isnull[..n].copy_from_slice(isnull);
    exec_store_virtual_tuple(slot);
}

/// PG `ExecEndHash`: tear down the child.
pub fn exec_end_hash(shared: Option<&Arc<SharedState>>, run: &mut HashRun<'_>) {
    exec_end_node(shared, &mut run.child);
}
