//! WindowAgg node executor. Translated from backend/executor/nodeWindowAgg.c
//! (disposition: full leaf for the reachable frame modes -- ROWS frames + the
//! default RANGE UNBOUNDED PRECEDING .. CURRENT ROW; window aggregates reuse the M5
//! agg trans/final path; the true window functions dispatch by OID into
//! `windowfuncs`).
//!
//! Execution mirrors PG's WindowAgg: the input arrives already sorted (a `Sort`
//! below orders by PARTITION BY keys then ORDER BY keys), so the node detects a
//! partition boundary by a change in the partition keys and spools each partition's
//! rows into a buffer (PG's tuplestore; here an owned `Vec` of row snapshots -- the
//! partition-local, per-task `Send` state). For every row in the partition it
//! evaluates each window function over its frame:
//!
//!  - a "window aggregate" (an ordinary aggregate invoked with OVER, `winagg`) runs
//!    the aggregate's transition/final functions over the rows in the frame (the
//!    M5 `nodeAgg` primitives, re-applied per row -- the incremental/inverse-transfn
//!    optimization is staged);
//!  - a true "window function" (row_number/rank/...) is dispatched by `winfnoid`
//!    into `windowfuncs`, reading the partition through a `WindowObject` (current
//!    position, peer comparison, partition-local rank, frame head/tail args).
//!
//! The output tlist is the query's final tlist; a WindowFunc whose `winref` is NOT
//! this node's was computed by a lower WindowAgg and is copied through from the
//! child output column at the same position (PG's multi-window stacking).
//!
//! GROW/STAGE (clean `not_yet_reachable`): GROUPS mode, frame EXCLUDE clauses, RANGE
//! OFFSET bounds (need the in_range support function), the run-condition early-stop,
//! and FILTER. The reachable frames are ROWS {UNBOUNDED PRECEDING|CURRENT ROW|n
//! PRECEDING|n FOLLOWING|UNBOUNDED FOLLOWING} and the default RANGE frame.

use std::cell::Cell;
use std::sync::Arc;

use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::backend::executor::execTuples::{
    exec_store_virtual_tuple, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::nodeAgg::{
    agg_initval, call_transition_nullable, is_strict_minmax, lookup_aggregate,
};
use crate::backend::executor::nodeGroup::grouping_equal;
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::backend::utils::adt::windowfuncs::WindowArg;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::fmgr::{OidFunctionCall1, OidFunctionCall2};
use crate::nodes::execnodes::{EState, PlanState, ScanState, WindowAggState};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::FrameOptions;
use crate::nodes::plannodes::WindowAgg;
use crate::nodes::primnodes::WindowFunc;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// One row snapshot (values + null flags), the per-task `Send` partition buffer cell.
type Row = (Vec<Datum>, Vec<bool>);

/// Resolved per-window-function metadata. `winagg` distinguishes a window aggregate
/// (run the trans/final fns over the frame) from a true window function (dispatch by
/// `winfnoid`). For a window aggregate the input column position is the sole Var arg.
struct PerFunc {
    /// the output tlist position (1-based resno) this function fills.
    resno: i16,
    /// the WindowFunc's winref (matches the node's winref when computed here).
    winref: crate::c::Index,
    /// true = window aggregate (trans/final); false = true window function.
    winagg: bool,
    /// window function OID (for the true-window-function dispatch).
    winfnoid: Oid,
    /// the function's argument column positions (1-based) over the child output.
    arg_cols: Vec<i16>,
    /// window-aggregate transition/final metadata (when `winagg`).
    transfn: Oid,
    finalfn: Oid,
    init_value: Option<Datum>,
    num_input: i32,
}

/// Run-state pairing the PG `WindowAggState` with its child plan-state and the
/// resolved window metadata. Mirrors `AggRun`: the C node holds the child + the
/// per-func arrays + the tuplestore via pointers; the Rust island wrapper owns them.
pub struct WindowAggRun<'rel> {
    pub state: Box<WindowAggState>,
    pub child: Box<PlanStateNode<'rel>>,
    /// this node's winref (the WindowFuncs it computes).
    winref: crate::c::Index,
    /// PARTITION BY column positions (1-based) over the child output + their types.
    part_cols: Vec<i16>,
    part_types: Vec<Oid>,
    /// ORDER BY (peer) column positions + types.
    ord_cols: Vec<i16>,
    ord_types: Vec<Oid>,
    frame_options: i32,
    /// constant ROWS-frame offsets (start/end), when the bound is `n PRECEDING/
    /// FOLLOWING` (already int8 Consts from the planner).
    start_offset: Option<i64>,
    end_offset: Option<i64>,
    /// the per-function metadata, one per WindowFunc in the output tlist.
    funcs: Vec<PerFunc>,
    /// the output tlist (final tlist; Vars + WindowFuncs).
    targetlist: Vec<Node>,
    /// the current partition's spooled rows (the buffer).
    partition: Vec<Row>,
    /// the cursor into `partition` for the next output row.
    cursor: usize,
    /// the first row of the NEXT partition, read ahead at a boundary.
    pending: Option<Row>,
    /// whether the child is fully drained.
    input_done: bool,
    /// partition-local per-function scratch (the rank state), one Cell per func.
    local_rank: Vec<Cell<i64>>,
}

/// The WindowObject API surface (PG's `WindowObjectData`), exposing the partition
/// buffer + the current row position to a window function. Interior mutability
/// (`Cell`) backs the partition-local rank state the rank family keeps.
pub struct WindowObject<'a> {
    partition: &'a [Row],
    current: i64,
    ord_cols: &'a [i16],
    ord_types: &'a [Oid],
    arg_cols: &'a [i16],
    frame_head: i64,
    frame_tail: i64, // one past the last in-frame row
    rank_cell: &'a Cell<i64>,
}

/// The result of a window-function evaluation: a Datum value or SQL NULL.
pub struct WindowFuncResult {
    pub value: Datum,
    pub is_null: bool,
}

impl WindowFuncResult {
    #[must_use]
    pub fn value(v: Datum) -> Self {
        Self { value: v, is_null: false }
    }
    #[must_use]
    pub fn null() -> Self {
        Self { value: Datum(0), is_null: true }
    }
    #[must_use]
    pub fn from_nullable(v: Option<Datum>) -> Self {
        v.map_or_else(Self::null, Self::value)
    }
}

impl WindowObject<'_> {
    /// PG `WinGetCurrentPosition`: the 0-based position of the current row.
    #[must_use]
    pub fn current_position(&self) -> i64 {
        self.current
    }

    /// PG `WinGetPartitionRowCount`: the partition's total row count.
    #[must_use]
    pub fn partition_row_count(&self) -> i64 {
        self.partition.len() as i64
    }

    /// PG `WinGetFuncArgCurrent` count: the number of declared function arguments.
    #[must_use]
    pub fn num_args(&self) -> usize {
        self.arg_cols.len()
    }

    /// PG `WinRowsArePeers`: whether rows at `a`,`b` are equal on the ORDER BY
    /// columns (the peer relation). With no ORDER BY, all rows are peers.
    #[must_use]
    pub fn rows_are_peers(&self, a: i64, b: i64) -> bool {
        if self.ord_cols.is_empty() {
            return true;
        }
        if a < 0 || b < 0 || a >= self.partition_row_count() || b >= self.partition_row_count() {
            return false;
        }
        let ra = &self.partition[a as usize];
        let rb = &self.partition[b as usize];
        grouping_equal(self.ord_cols, self.ord_types, &ra.0, &ra.1, &rb.0, &rb.1)
    }

    /// PG `WinGetFuncArgCurrent`: function argument `argno` evaluated on the current
    /// row. Returns the raw Datum (NULL flags handled by the caller via the nullable
    /// variant); used for the scalar offset/bucket args.
    #[must_use]
    pub fn func_arg_current(&self, argno: usize) -> Option<Datum> {
        self.func_arg_current_nullable(argno)
    }

    /// PG `WinGetFuncArgCurrent` (nullable): argument `argno` on the current row, or
    /// None when it is SQL NULL.
    #[must_use]
    pub fn func_arg_current_nullable(&self, argno: usize) -> Option<Datum> {
        self.arg_at(self.current, argno)
    }

    /// PG `WinGetFuncArgInPartition` (SEEK_CURRENT): argument `argno` `relpos` rows
    /// from the current row within the partition. OutOfPartition when the target is
    /// before the first / after the last row.
    #[must_use]
    pub fn func_arg_in_partition(&self, argno: usize, relpos: i64) -> WindowArg {
        let target = self.current + relpos;
        if target < 0 || target >= self.partition_row_count() {
            return WindowArg::OutOfPartition;
        }
        WindowArg::InRange(self.arg_at(target, argno))
    }

    /// PG `WinGetFuncArgInFrame` (SEEK_HEAD, relpos 0): argument `argno` of the
    /// frame's first row, or None when the frame is empty.
    #[must_use]
    pub fn func_arg_in_frame_head(&self, argno: usize) -> Option<Datum> {
        if self.frame_head >= self.frame_tail {
            return None;
        }
        self.arg_at(self.frame_head, argno)
    }

    /// PG `WinGetFuncArgInFrame` (SEEK_TAIL, relpos 0): argument `argno` of the
    /// frame's last row, or None when the frame is empty.
    #[must_use]
    pub fn func_arg_in_frame_tail(&self, argno: usize) -> Option<Datum> {
        if self.frame_head >= self.frame_tail {
            return None;
        }
        self.arg_at(self.frame_tail - 1, argno)
    }

    /// PG `WinGetFuncArgInFrame` (SEEK_HEAD, relpos n): argument `argno` of the
    /// frame's nth row (0-based within the frame), or None when out of frame.
    #[must_use]
    pub fn func_arg_in_frame_nth(&self, argno: usize, n: i64) -> Option<Datum> {
        let target = self.frame_head + n;
        if target < self.frame_head || target >= self.frame_tail {
            return None;
        }
        self.arg_at(target, argno)
    }

    /// PG `WinGetPartitionLocalMemory` rank cell: the partition-local rank state.
    #[must_use]
    pub fn local_rank(&self) -> i64 {
        self.rank_cell.get()
    }
    pub fn set_local_rank(&self, v: i64) {
        self.rank_cell.set(v);
    }

    /// Read function argument `argno` (its child output column) at partition row
    /// `pos`, or None when the column is SQL NULL.
    fn arg_at(&self, pos: i64, argno: usize) -> Option<Datum> {
        let &col = self.arg_cols.get(argno)?;
        let row = &self.partition[pos as usize];
        let idx = (col - 1) as usize;
        if row.1[idx] {
            None
        } else {
            Some(row.0[idx])
        }
    }
}

/// PG `ExecInitWindowAgg`: build the WindowAggState over an initialized child.
/// Resolves each WindowFunc in the targetlist into a `PerFunc` (a window aggregate's
/// trans/final via pg_aggregate; a true window function by `winfnoid`), and sizes the
/// result slot from the node's (final) tlist rowtype.
pub fn exec_init_window_agg<'rel>(
    node: &WindowAgg,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<WindowAggRun<'rel>> {
    let _ = estate;

    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitWindowAgg: child has no result descriptor"));

    // Resolve the WindowFuncs in the targetlist that belong to THIS node's window.
    let mut funcs: Vec<PerFunc> = Vec::new();
    for te in &node.plan.targetlist {
        let Node::TargetEntry(te) = te else { continue };
        let resno = te.resno;
        if let Some(Node::WindowFunc(w)) = te.expr.as_ref() {
            funcs.push(resolve_windowfunc(w, resno));
        }
    }

    let result_desc =
        crate::backend::executor::execTuples::exec_clean_type_from_tl(&node.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);

    let col_type = |c: i16| outer_desc.attr((c - 1) as usize).atttypid;
    let part_cols = node.part_col_idx.clone();
    let part_types: Vec<Oid> = part_cols.iter().map(|&c| col_type(c)).collect();
    let ord_cols = node.ord_col_idx.clone();
    let ord_types: Vec<Oid> = ord_cols.iter().map(|&c| col_type(c)).collect();

    let mut ps = PlanState { plan: Some(Node::WindowAgg(Box::new(node.clone()))), ..PlanState::default() };
    ps.ps_result_tuple_desc = Some(Arc::clone(&result_desc));
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.scandesc = Some(Arc::clone(&outer_desc));
    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: None,
    };

    let local_rank = funcs.iter().map(|_| Cell::new(0)).collect();

    Box::new(WindowAggRun {
        state: Box::new(WindowAggState {
            ss,
            numfuncs: i32::try_from(funcs.len()).unwrap_or(0),
            ..window_agg_state_defaults()
        }),
        child: Box::new(child),
        winref: node.winref,
        part_cols,
        part_types,
        ord_cols,
        ord_types,
        frame_options: node.frame_options,
        start_offset: const_int8(node.start_offset.as_ref()),
        end_offset: const_int8(node.end_offset.as_ref()),
        funcs,
        targetlist: node.plan.targetlist.clone(),
        partition: Vec::new(),
        cursor: 0,
        pending: None,
        input_done: false,
        local_rank,
    })
}

/// Resolve a WindowFunc into its per-function execution metadata. A window aggregate
/// (`winagg`) reads its transition/final functions from pg_aggregate; a true window
/// function carries its `winfnoid` for dispatch. The argument columns are the (1-based)
/// child output positions of the WindowFunc's Var args (rewritten by setrefs).
fn resolve_windowfunc(w: &WindowFunc, resno: i16) -> PerFunc {
    let arg_cols: Vec<i16> = w
        .args
        .iter()
        .filter_map(|a| match a {
            Node::Var(v) => Some(v.varattno),
            _ => None,
        })
        .collect();

    if w.winagg {
        let (transfn, finalfn, transtype, _init) = lookup_aggregate(w.winfnoid);
        let init_value = agg_initval(w.winfnoid, transtype);
        // count(*) over a window has no input column; otherwise one Var arg.
        let num_input = i32::from(!(w.winstar || arg_cols.is_empty()));
        PerFunc {
            resno,
            winref: w.winref,
            winagg: true,
            winfnoid: w.winfnoid,
            arg_cols,
            transfn,
            finalfn,
            init_value,
            num_input,
        }
    } else {
        PerFunc {
            resno,
            winref: w.winref,
            winagg: false,
            winfnoid: w.winfnoid,
            arg_cols,
            transfn: crate::postgres_ext::InvalidOid,
            finalfn: crate::postgres_ext::InvalidOid,
            init_value: None,
            num_input: 0,
        }
    }
}

/// PG `ExecWindowAgg`: return the next windowed output row. Spools one partition at a
/// time, then walks its rows; each row is projected through the final tlist with the
/// window functions evaluated over the row's frame.
pub async fn exec_window_agg<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut WindowAggRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    loop {
        // Emit remaining rows of the current partition.
        if run.cursor < run.partition.len() {
            let pos = run.cursor;
            run.cursor += 1;
            return Some(project_row(run, pos));
        }
        // Current partition exhausted: spool the next one (or finish).
        if run.input_done && run.pending.is_none() {
            return None;
        }
        spool_partition(shared, run).await;
        if run.partition.is_empty() {
            return None;
        }
    }
}

/// Spool the next partition into `run.partition`: keep reading child rows while they
/// share the partition keys with the partition's first row; the first row of the next
/// partition is stashed in `run.pending`. Resets the per-partition cursor + rank.
async fn spool_partition(shared: Option<&Arc<SharedState>>, run: &mut WindowAggRun<'_>) {
    run.partition.clear();
    run.cursor = 0;
    for cell in &run.local_rank {
        cell.set(0);
    }

    // Seed with the pending row (or read the first row of the first partition).
    let first = match run.pending.take() {
        Some(r) => Some(r),
        None => {
            if run.input_done {
                None
            } else {
                let next = Box::pin(exec_proc_node(shared, &mut run.child)).await.map(snapshot_slot);
                if next.is_none() {
                    run.input_done = true;
                }
                next
            }
        }
    };
    let Some(first) = first else { return };
    run.partition.push(first);

    // Keep reading while the partition keys match the first row.
    loop {
        let Some(next) = Box::pin(exec_proc_node(shared, &mut run.child)).await.map(snapshot_slot)
        else {
            run.input_done = true;
            break;
        };
        let same = {
            let f = &run.partition[0];
            grouping_equal(&run.part_cols, &run.part_types, &next.0, &next.1, &f.0, &f.1)
        };
        if same {
            run.partition.push(next);
        } else {
            run.pending = Some(next);
            break;
        }
    }
}

/// Project one partition row through the final tlist into the result slot. Vars are
/// copied from the row; a WindowFunc of this node's winref is evaluated over the
/// row's frame; a WindowFunc of a lower window is copied from the child output (it
/// was computed by the child WindowAgg at the same column position).
fn project_row<'r>(run: &'r mut WindowAggRun<'_>, pos: usize) -> &'r mut TupleTableSlot {
    // Compute every window function value for this row up front (immutable borrows of
    // the partition), keyed by output resno.
    let mut func_vals: Vec<(i16, Datum, bool)> = Vec::with_capacity(run.funcs.len());
    for (i, pf) in run.funcs.iter().enumerate() {
        let (v, n) = if pf.winref == run.winref {
            eval_window_func(run, pf, i, pos)
        } else {
            // Lower window's column: read from the child output (this row's snapshot).
            let idx = (pf.resno - 1) as usize;
            let row = &run.partition[pos];
            (row.0[idx], row.1[idx])
        };
        func_vals.push((pf.resno, v, n));
    }

    let targetlist = run.targetlist.clone();
    let row = run.partition[pos].clone();

    let mut out_values: Vec<Datum> = Vec::new();
    let mut out_nulls: Vec<bool> = Vec::new();
    for te in &targetlist {
        let Node::TargetEntry(te) = te else { continue };
        if te.resjunk {
            continue;
        }
        match te.expr.as_ref() {
            Some(Node::WindowFunc(_)) => {
                let (_, v, n) = func_vals
                    .iter()
                    .find(|(r, _, _)| *r == te.resno)
                    .copied()
                    .unwrap_or((te.resno, Datum(0), true));
                out_values.push(v);
                out_nulls.push(n);
            }
            Some(Node::Var(v)) => {
                let idx = (v.varattno - 1) as usize;
                out_values.push(row.0[idx]);
                out_nulls.push(row.1[idx]);
            }
            _ => unimplemented!("ExecWindowAgg: non-Var/WindowFunc tlist entry not yet reachable"),
        }
    }

    let slot = run
        .state
        .ss
        .ps
        .ps_result_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecWindowAgg: no result slot"));
    ExecClearTuple(slot);
    let n = out_values.len();
    slot.values[..n].copy_from_slice(&out_values);
    slot.isnull[..n].copy_from_slice(&out_nulls);
    exec_store_virtual_tuple(slot);
    slot
}

/// Evaluate one window function (this node's winref) at partition position `pos`.
/// `func_idx` indexes the partition-local rank cell. Window aggregates aggregate over
/// the row's frame; true window functions dispatch by `winfnoid` through `windowfuncs`.
fn eval_window_func(run: &WindowAggRun<'_>, pf: &PerFunc, func_idx: usize, pos: usize) -> (Datum, bool) {
    let pos = pos as i64;
    let (frame_head, frame_tail) = compute_frame(run, pos);

    if pf.winagg {
        return frame_aggregate(run, pf, frame_head, frame_tail);
    }

    let winobj = WindowObject {
        partition: &run.partition,
        current: pos,
        ord_cols: &run.ord_cols,
        ord_types: &run.ord_types,
        arg_cols: &pf.arg_cols,
        frame_head,
        frame_tail,
        rank_cell: &run.local_rank[func_idx],
    };
    let result = dispatch_window_func(pf.winfnoid, &winobj);
    (result.value, result.is_null)
}

/// Aggregate a window aggregate over `[frame_head, frame_tail)`: seed the transition
/// state, advance it over each in-frame row's input column, then finalize. Reuses the
/// M5 `nodeAgg` transition/final primitives (the incremental/inverse-transfn frame
/// optimization is staged -- the milestone recomputes per row, which is correct).
fn frame_aggregate(run: &WindowAggRun<'_>, pf: &PerFunc, frame_head: i64, frame_tail: i64) -> (Datum, bool) {
    let mut value = pf.init_value.unwrap_or(Datum(0));
    let mut is_null = pf.init_value.is_none();

    let mut row = frame_head.max(0);
    while row < frame_tail && (row as usize) < run.partition.len() {
        if pf.num_input == 0 {
            // count(*): increment the running int8 state.
            value = OidFunctionCall1(pf.transfn, value)
                .unwrap_or_else(|| unreachable!("count transfn returns non-NULL"));
            is_null = false;
        } else {
            let col = pf.arg_cols.first().copied().unwrap_or(0);
            let r = &run.partition[row as usize];
            let idx = (col - 1) as usize;
            if !r.1[idx] {
                let input = r.0[idx];
                if is_null {
                    if pf.finalfn == crate::postgres_ext::InvalidOid && is_strict_minmax(pf.transfn) {
                        value = input;
                        is_null = false;
                    } else if let Some(v) = call_transition_nullable(pf.transfn, None, input) {
                        value = v;
                        is_null = false;
                    }
                } else {
                    value = OidFunctionCall2(pf.transfn, value, input)
                        .unwrap_or_else(|| unreachable!("transfn non-NULL once state non-null"));
                }
            }
        }
        row += 1;
    }

    if pf.finalfn != crate::postgres_ext::InvalidOid {
        if is_null {
            return (Datum(0), true);
        }
        return OidFunctionCall1(pf.finalfn, value).map_or((Datum(0), true), |v| (v, false));
    }
    (value, is_null)
}

/// Compute the `[frame_head, frame_tail)` row range (frame_tail is one past the last
/// in-frame row) for the current position. Handles the reachable ROWS bounds and the
/// default RANGE UNBOUNDED PRECEDING .. CURRENT ROW; GROUPS / RANGE-OFFSET / EXCLUDE
/// are staged.
fn compute_frame(run: &WindowAggRun<'_>, pos: i64) -> (i64, i64) {
    let opts = FrameOptions::from_bits_truncate(run.frame_options);
    let nrows = run.partition.len() as i64;

    if opts.intersects(FrameOptions::GROUPS) {
        unimplemented!("ExecWindowAgg: GROUPS frame mode not yet reachable");
    }
    if opts.intersects(FrameOptions::EXCLUSION) {
        unimplemented!("ExecWindowAgg: frame EXCLUDE clause not yet reachable");
    }

    let is_rows = opts.contains(FrameOptions::ROWS);

    // Frame head.
    let head = if opts.contains(FrameOptions::START_UNBOUNDED_PRECEDING) {
        0
    } else if opts.contains(FrameOptions::START_CURRENT_ROW) {
        if is_rows {
            pos
        } else {
            // RANGE CURRENT ROW: the first peer of the current row.
            range_peer_head(run, pos)
        }
    } else if opts.contains(FrameOptions::START_OFFSET_PRECEDING) {
        require_rows(is_rows, "frame start n PRECEDING");
        (pos - run.start_offset.unwrap_or(0)).max(0)
    } else if opts.contains(FrameOptions::START_OFFSET_FOLLOWING) {
        require_rows(is_rows, "frame start n FOLLOWING");
        pos + run.start_offset.unwrap_or(0)
    } else {
        unimplemented!("ExecWindowAgg: unsupported frame start option");
    };

    // Frame tail (exclusive).
    let tail = if opts.contains(FrameOptions::END_UNBOUNDED_FOLLOWING) {
        nrows
    } else if opts.contains(FrameOptions::END_CURRENT_ROW) {
        if is_rows {
            pos + 1
        } else {
            // RANGE CURRENT ROW: one past the last peer of the current row.
            range_peer_tail(run, pos)
        }
    } else if opts.contains(FrameOptions::END_OFFSET_PRECEDING) {
        require_rows(is_rows, "frame end n PRECEDING");
        pos - run.end_offset.unwrap_or(0) + 1
    } else if opts.contains(FrameOptions::END_OFFSET_FOLLOWING) {
        require_rows(is_rows, "frame end n FOLLOWING");
        pos + run.end_offset.unwrap_or(0) + 1
    } else {
        unimplemented!("ExecWindowAgg: unsupported frame end option");
    };

    (head.clamp(0, nrows), tail.clamp(0, nrows))
}

fn require_rows(is_rows: bool, what: &str) {
    if !is_rows {
        unimplemented!("ExecWindowAgg: RANGE {what} (needs in_range) not yet reachable");
    }
}

/// RANGE CURRENT ROW frame head: the first row that is a peer of `pos` (scanning
/// back over equal-ORDER-BY rows). With no ORDER BY the whole partition is one peer
/// group, so the head is 0.
fn range_peer_head(run: &WindowAggRun<'_>, pos: i64) -> i64 {
    if run.ord_cols.is_empty() {
        return 0;
    }
    let mut h = pos;
    while h > 0 && peers(run, h - 1, pos) {
        h -= 1;
    }
    h
}

/// RANGE CURRENT ROW frame tail (exclusive): one past the last peer of `pos`.
fn range_peer_tail(run: &WindowAggRun<'_>, pos: i64) -> i64 {
    let nrows = run.partition.len() as i64;
    if run.ord_cols.is_empty() {
        return nrows;
    }
    let mut t = pos + 1;
    while t < nrows && peers(run, t, pos) {
        t += 1;
    }
    t
}

/// Whether partition rows `a`,`b` are peers on the ORDER BY columns.
fn peers(run: &WindowAggRun<'_>, a: i64, b: i64) -> bool {
    let ra = &run.partition[a as usize];
    let rb = &run.partition[b as usize];
    grouping_equal(&run.ord_cols, &run.ord_types, &ra.0, &ra.1, &rb.0, &rb.1)
}

/// Dispatch a true window function by its `winfnoid` into `windowfuncs`. The
/// reachable set is row_number/rank/dense_rank/percent_rank/cume_dist/ntile/lag/lead/
/// first_value/last_value/nth_value (their pg_proc OIDs). Unhandled window functions
/// fail loudly (a clean grow guard).
fn dispatch_window_func(winfnoid: Oid, winobj: &WindowObject) -> WindowFuncResult {
    use crate::backend::utils::adt::windowfuncs as wf;
    match winfnoid.get() {
        3100 => wf::window_row_number(winobj),
        3101 => wf::window_rank(winobj),
        3102 => wf::window_dense_rank(winobj),
        3103 => wf::window_percent_rank(winobj),
        3104 => wf::window_cume_dist(winobj),
        3105 => wf::window_ntile(winobj),
        // lag: 3106 (anyelement), 3107 (+offset), 3108 (+offset+default).
        3106..=3108 => wf::window_lag(winobj),
        // lead: 3109, 3110, 3111.
        3109..=3111 => wf::window_lead(winobj),
        3112 => wf::window_first_value(winobj),
        3113 => wf::window_last_value(winobj),
        3114 => wf::window_nth_value(winobj),
        other => unimplemented!("ExecWindowAgg: window function OID {other} not yet reachable"),
    }
}

/// PG `ExecEndWindowAgg`: tear down the child.
pub fn exec_end_window_agg(shared: Option<&Arc<SharedState>>, run: &mut WindowAggRun<'_>) {
    exec_end_node(shared, &mut run.child);
}

/// The constant int8 value of a ROWS frame OFFSET expression (a planner Const), or
/// None when the bound carries no offset.
fn const_int8(expr: Option<&Node>) -> Option<i64> {
    match expr {
        Some(Node::Const(c)) if !c.constisnull => Some(crate::postgres::DatumGetInt64(c.constvalue)),
        Some(_) => unimplemented!("ExecWindowAgg: non-Const frame offset not yet reachable"),
        None => None,
    }
}

/// A zero-initialized `WindowAggState` (the C `makeNode` defaults). The OpaqueState /
/// MemoryContext / tuplestore fields are unused by the island run-state and stay at
/// their defaults.
fn window_agg_state_defaults() -> WindowAggState {
    use crate::nodes::execnodes::WindowAggStatus;
    #[allow(deprecated)]
    WindowAggState {
        ss: ScanState {
            ps: PlanState::default(),
            ss_current_relation: None,
            ss_current_scan_desc: None,
            ss_scan_tuple_slot: None,
        },
        funcs: Vec::new(),
        numfuncs: 0,
        numaggs: 0,
        perfunc: None,
        peragg: None,
        part_eqfunction: None,
        ord_eqfunction: None,
        buffer: None,
        current_ptr: 0,
        framehead_ptr: 0,
        frametail_ptr: 0,
        grouptail_ptr: 0,
        spooled_rows: 0,
        currentpos: 0,
        frameheadpos: 0,
        frametailpos: 0,
        agg_winobj: None,
        aggregatedbase: 0,
        aggregatedupto: 0,
        status: Some(WindowAggStatus::RUN),
        frame_options: 0,
        start_offset: None,
        end_offset: None,
        start_offset_value: Datum(0),
        end_offset_value: Datum(0),
        start_in_range_func: crate::nodes::execnodes::FmgrInfo,
        end_in_range_func: crate::nodes::execnodes::FmgrInfo,
        in_range_coll: crate::postgres_ext::InvalidOid,
        in_range_asc: true,
        in_range_nulls_first: false,
        use_pass_through: false,
        top_window: false,
        runcondition: None,
        currentgroup: 0,
        frameheadgroup: 0,
        frametailgroup: 0,
        groupheadpos: 0,
        grouptailpos: 0,
        partcontext: crate::nodes::execnodes::MemoryContext,
        aggcontext: crate::nodes::execnodes::MemoryContext,
        curaggcontext: crate::nodes::execnodes::MemoryContext,
        tmpcontext: None,
        all_first: true,
        partition_spooled: false,
        next_partition: true,
        more_partitions: false,
        framehead_valid: false,
        frametail_valid: false,
        grouptail_valid: false,
        first_part_slot: None,
        framehead_slot: None,
        frametail_slot: None,
        agg_row_slot: None,
        temp_slot_1: None,
        temp_slot_2: None,
    }
}

#[cfg(test)]
mod tests {
    //! Node-level WindowAgg tests over a hand-built plan + a pre-sorted TupleSource
    //! child. The true window functions (row_number/rank/dense_rank) dispatch by OID
    //! without touching the catalog, so these need no initdb. Covers per-partition
    //! row_number, rank/dense_rank peer handling, and the staged-frame guard.
    use super::*;
    use std::sync::Arc;

    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, TTSOpsVirtual, TupleTableSlot};
    use crate::nodes::parsenodes::FrameOptions;
    use crate::nodes::plannodes::{Plan, WindowAgg};
    use crate::nodes::primnodes::{TargetEntry, WindowFunc};
    use crate::postgres::DatumGetInt64;

    const INT4OID: Oid = Oid::new(23);
    const INT8OID: Oid = Oid::new(20);
    const INVALID: Oid = crate::postgres_ext::InvalidOid;

    fn desc2() -> TupleDesc {
        let mut d = TupleDescData::create_template(2);
        for (i, name) in ["g", "v"].iter().enumerate() {
            d.init_builtin_entry((i + 1) as i16, name, INT4OID, -1, 0);
            d.init_entry_collation((i + 1) as i16, INVALID);
        }
        Arc::new(d)
    }

    fn make_source(desc: &TupleDesc, rows: Vec<[i32; 2]>) -> PlanStateNode<'static> {
        let slots: Vec<Box<TupleTableSlot>> = rows
            .into_iter()
            .map(|r| {
                let mut slot = make_tuple_table_slot(Some(Arc::clone(desc)), &TTSOpsVirtual);
                for (i, v) in r.iter().enumerate() {
                    slot.values[i] = crate::postgres::Int32GetDatum(*v);
                    slot.isnull[i] = false;
                }
                exec_store_virtual_tuple(&mut slot);
                slot
            })
            .collect();
        PlanStateNode::test_tuple_source(Arc::clone(desc), slots)
    }

    fn empty_plan(targetlist: Vec<Node>) -> Plan {
        Plan {
            disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
            parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
            targetlist, qual: Vec::new(), lefttree: None, righttree: None,
            init_plan: Vec::new(), ext_param: None, all_param: None,
        }
    }

    fn te(expr: Node, resno: i16) -> Node {
        Node::TargetEntry(Box::new(TargetEntry {
            expr: Some(expr), resno, resname: None, ressortgroupref: 0,
            resorigtbl: INVALID, resorigcol: 0, resjunk: false,
        }))
    }

    /// A true window function call (no args) returning int8, for `winfnoid`.
    fn winfunc(winfnoid: Oid) -> Node {
        Node::WindowFunc(Box::new(WindowFunc {
            winfnoid, wintype: INT8OID, wincollid: INVALID, inputcollid: INVALID,
            args: Vec::new(), aggfilter: None, runCondition: Vec::new(),
            winref: 1, winstar: false, winagg: false, location: -1,
        }))
    }

    /// Build a WindowAgg over the child: output (v, fn1[, fn2]); PARTITION BY the
    /// given cols; ORDER BY column 2 (v) for peer detection; default ROWS frame.
    fn window_node(funcs: &[Oid], part_cols: Vec<i16>, frame_options: i32) -> WindowAgg {
        let v_var = Node::Var(Box::new(crate::backend::nodes::makefuncs::make_var(
            1, 2, INT4OID, -1, INVALID, 0,
        )));
        let mut tlist = vec![te(v_var, 1)];
        for (i, &f) in funcs.iter().enumerate() {
            tlist.push(te(winfunc(f), (i + 2) as i16));
        }
        WindowAgg {
            plan: empty_plan(tlist),
            winname: None,
            winref: 1,
            part_num_cols: i32::try_from(part_cols.len()).unwrap_or(0),
            part_col_idx: part_cols,
            part_operators: vec![Oid::new(96)],
            part_collations: vec![INVALID],
            ord_num_cols: 1,
            ord_col_idx: vec![2],
            ord_operators: vec![Oid::new(96)],
            ord_collations: vec![INVALID],
            frame_options,
            start_offset: None,
            end_offset: None,
            run_condition: Vec::new(),
            run_condition_orig: Vec::new(),
            start_in_range_func: INVALID,
            end_in_range_func: INVALID,
            in_range_coll: INVALID,
            in_range_asc: true,
            in_range_nulls_first: false,
            top_window: true,
        }
    }

    fn rows_default_frame() -> i32 {
        (FrameOptions::NONDEFAULT
            | FrameOptions::ROWS
            | FrameOptions::START_UNBOUNDED_PRECEDING
            | FrameOptions::END_CURRENT_ROW)
            .bits()
    }

    async fn run_window(node: WindowAgg, child: PlanStateNode<'static>, ncols: usize) -> Vec<Vec<i64>> {
        let mut estate = EState::default();
        let mut run = exec_init_window_agg(&node, &mut estate, child);
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_window_agg(None, &mut run)).await else { break };
            let row = (1..=ncols as i32)
                .map(|a| slot_getattr(slot, a).map_or(i64::MIN, |d| d.0 as i64))
                .collect();
            out.push(row);
        }
        exec_end_window_agg(None, &mut run);
        out
    }

    /// row_number() OVER (ORDER BY v) -> 1..n over the (pre-sorted) input.
    #[tokio::test]
    async fn row_number_over_order_by() {
        let desc = desc2();
        // pre-sorted by v: one partition (no PARTITION BY).
        let child = make_source(&desc, vec![[1, 5], [1, 10], [1, 20], [1, 20]]);
        let node = window_node(&[Oid::new(3100)], vec![], rows_default_frame());
        let got = run_window(node, child, 2).await;
        let rn: Vec<i64> = got.iter().map(|r| DatumGetInt64(Datum(r[1] as usize))).collect();
        assert_eq!(rn, vec![1, 2, 3, 4], "row_number 1..n");
    }

    /// rank() and dense_rank() OVER (ORDER BY v) -> peers (v=20) share rank.
    #[tokio::test]
    async fn rank_and_dense_rank_peers() {
        let desc = desc2();
        let child = make_source(&desc, vec![[1, 5], [1, 10], [1, 20], [1, 20]]);
        // two funcs: rank (3101), dense_rank (3102).
        let node = window_node(&[Oid::new(3101), Oid::new(3102)], vec![], rows_default_frame());
        let got = run_window(node, child, 3).await;
        let rank: Vec<i64> = got.iter().map(|r| DatumGetInt64(Datum(r[1] as usize))).collect();
        let dense: Vec<i64> = got.iter().map(|r| DatumGetInt64(Datum(r[2] as usize))).collect();
        assert_eq!(rank, vec![1, 2, 3, 3], "rank with a peer at the end");
        assert_eq!(dense, vec![1, 2, 3, 3], "dense_rank with a peer at the end");
    }

    /// row_number() OVER (PARTITION BY g ORDER BY v) restarts per partition.
    #[tokio::test]
    async fn row_number_partitioned() {
        let desc = desc2();
        // pre-sorted by (g, v).
        let child = make_source(&desc, vec![[1, 10], [1, 20], [2, 5], [2, 15], [2, 25]]);
        let node = window_node(&[Oid::new(3100)], vec![1], rows_default_frame());
        let got = run_window(node, child, 2).await;
        let rn: Vec<i64> = got.iter().map(|r| DatumGetInt64(Datum(r[1] as usize))).collect();
        assert_eq!(rn, vec![1, 2, 1, 2, 3], "row_number restarts at g=2");
    }

    /// A GROUPS-mode frame is a clean not_yet_reachable.
    #[tokio::test]
    #[should_panic(expected = "GROUPS frame mode not yet reachable")]
    async fn groups_frame_is_staged() {
        let desc = desc2();
        let child = make_source(&desc, vec![[1, 5], [1, 10]]);
        let groups_frame = (FrameOptions::NONDEFAULT
            | FrameOptions::GROUPS
            | FrameOptions::START_UNBOUNDED_PRECEDING
            | FrameOptions::END_CURRENT_ROW)
            .bits();
        // count(*)-style over a GROUPS frame would aggregate; here a row_number with a
        // GROUPS frame still triggers the frame computation guard.
        let node = window_node(&[Oid::new(3112)], vec![], groups_frame); // first_value
        let _ = run_window(node, child, 2).await;
    }
}
