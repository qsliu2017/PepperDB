//! Built-in window functions. Translated from backend/utils/adt/windowfuncs.c
//! (disposition: full leaf for the reachable functions).
//!
//! In C each window function is a `PGFunction` that pulls a `WindowObject` out of
//! `fcinfo->context` and drives the WindowObject API (WinGetCurrentPosition,
//! WinRowsArePeers, WinGetPartitionLocalMemory, WinGetFuncArgInPartition/InFrame).
//! Threading a mutable executor capsule through `fcinfo->context` (an
//! `Option<Box<Node>>`) would force a raw pointer into the Node enum, so this port
//! dispatches the reachable window functions by their `winfnoid` directly inside
//! `nodeWindowAgg` (mirroring how `nodeAgg` resolves aggregates by OID). The pure
//! per-row logic lives here, taking a `WindowObject` borrow that exposes the same
//! API surface the C functions use; `nodeWindowAgg` owns the partition spool the
//! WindowObject reads.
//!
//! Implemented (the reachable set): row_number, rank, dense_rank, percent_rank,
//! cume_dist, ntile, lag, lead, first_value, last_value, nth_value. The OFFSET /
//! default variants of lag/lead are handled by reading the call's extra arguments.

use crate::backend::executor::nodeWindowAgg::{WindowFuncResult, WindowObject};
use crate::postgres::{Datum, DatumGetInt32, Int64GetDatum};

/// PG `window_row_number`: the 1-based position of the current row in the partition.
#[must_use]
pub fn window_row_number(winobj: &WindowObject) -> WindowFuncResult {
    let curpos = winobj.current_position();
    WindowFuncResult::value(Int64GetDatum(curpos + 1))
}

/// PG `window_rank`: rank with gaps. The rank is the current row's 1-based position
/// the first time a new peer group starts; equal-ORDER-BY peers share the rank.
#[must_use]
pub fn window_rank(winobj: &WindowObject) -> WindowFuncResult {
    // rank_up: the current row starts a new peer group iff it is position 0 or is not
    // a peer of the prior row.
    let curpos = winobj.current_position();
    let up = curpos == 0 || !winobj.rows_are_peers(curpos - 1, curpos);
    let rank = winobj.local_rank();
    let rank = if up { curpos + 1 } else { rank };
    winobj.set_local_rank(rank);
    WindowFuncResult::value(Int64GetDatum(rank))
}

/// PG `window_dense_rank`: rank without gaps. Increments by 1 at each new peer group.
#[must_use]
pub fn window_dense_rank(winobj: &WindowObject) -> WindowFuncResult {
    let curpos = winobj.current_position();
    let up = curpos == 0 || !winobj.rows_are_peers(curpos - 1, curpos);
    let mut rank = winobj.local_rank();
    if up {
        rank += 1;
    }
    winobj.set_local_rank(rank);
    WindowFuncResult::value(Int64GetDatum(rank))
}

/// PG `window_percent_rank`: (rank - 1) / (totalrows - 1), as float8. A single-row
/// partition yields 0.
#[must_use]
pub fn window_percent_rank(winobj: &WindowObject) -> WindowFuncResult {
    let curpos = winobj.current_position();
    let up = curpos == 0 || !winobj.rows_are_peers(curpos - 1, curpos);
    let rank = if up { curpos + 1 } else { winobj.local_rank() };
    winobj.set_local_rank(rank);
    let total = winobj.partition_row_count();
    #[allow(
        clippy::cast_precision_loss,
        reason = "row counts within f64 exact-integer range for the reachable partitions"
    )]
    let pr = if total <= 1 { 0.0 } else { (rank - 1) as f64 / (total - 1) as f64 };
    WindowFuncResult::value(crate::postgres::Float8GetDatum(pr))
}

/// PG `window_cume_dist`: (# rows preceding or peer with current) / totalrows.
#[must_use]
pub fn window_cume_dist(winobj: &WindowObject) -> WindowFuncResult {
    let curpos = winobj.current_position();
    let total = winobj.partition_row_count();
    // Count rows that are <= the current row's peer group (the highest position in
    // the current peer group + 1).
    let mut rank = curpos + 1;
    let mut row = rank;
    while row < total {
        if !winobj.rows_are_peers(row - 1, row) {
            break;
        }
        rank += 1;
        row += 1;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "row counts within f64 exact-integer range for the reachable partitions"
    )]
    let cd = rank as f64 / total as f64;
    WindowFuncResult::value(crate::postgres::Float8GetDatum(cd))
}

/// PG `window_ntile`: divide the partition into `n` ~equal buckets; return the
/// current row's 1-based bucket number. The first `total % n` buckets get one extra
/// row.
#[must_use]
pub fn window_ntile(winobj: &WindowObject) -> WindowFuncResult {
    let nbuckets_datum = winobj.func_arg_current(1);
    let Some(nb) = nbuckets_datum else {
        return WindowFuncResult::null(); // NULL bucket count -> NULL
    };
    let nbuckets = i64::from(DatumGetInt32(nb));
    if nbuckets <= 0 {
        return WindowFuncResult::null();
    }
    let total = winobj.partition_row_count();
    let curpos = winobj.current_position();
    let base = total / nbuckets; // rows per bucket (floor)
    let remainder = total % nbuckets; // first `remainder` buckets get one extra
    let bigsize = (base + 1) * remainder; // rows covered by the larger buckets
    let bucket = if curpos < bigsize {
        curpos / (base + 1) + 1
    } else {
        remainder + (curpos - bigsize) / base.max(1) + 1
    };
    WindowFuncResult::value(Int64GetDatum(bucket))
}

/// PG `window_lag` / `window_lag_with_offset` / `..._and_default`: the argument value
/// `offset` rows BEFORE the current row in the partition, or the default (arg 3) /
/// NULL when out of partition.
#[must_use]
pub fn window_lag(winobj: &WindowObject) -> WindowFuncResult {
    lag_lead(winobj, false)
}

/// PG `window_lead` / variants: the argument value `offset` rows AFTER the current
/// row in the partition, or the default / NULL when out of partition.
#[must_use]
pub fn window_lead(winobj: &WindowObject) -> WindowFuncResult {
    lag_lead(winobj, true)
}

/// Shared lag/lead body: read the offset (arg 1, default 1) and optional default
/// (arg 2), then fetch arg 0 at +/- offset within the partition.
fn lag_lead(winobj: &WindowObject, forward: bool) -> WindowFuncResult {
    let offset = match winobj.num_args() {
        1 => 1,
        _ => match winobj.func_arg_current(1) {
            Some(d) => i64::from(DatumGetInt32(d)),
            None => return WindowFuncResult::null(), // NULL offset -> NULL
        },
    };
    let relpos = if forward { offset } else { -offset };
    match winobj.func_arg_in_partition(0, relpos) {
        WindowArg::InRange(v) => WindowFuncResult::from_nullable(v),
        WindowArg::OutOfPartition => {
            // Out of partition: the default (arg 2) if present, else NULL.
            if winobj.num_args() >= 3 {
                WindowFuncResult::from_nullable(winobj.func_arg_current_nullable(2))
            } else {
                WindowFuncResult::null()
            }
        }
    }
}

/// PG `window_first_value`: the argument value of the frame's first row.
#[must_use]
pub fn window_first_value(winobj: &WindowObject) -> WindowFuncResult {
    WindowFuncResult::from_nullable(winobj.func_arg_in_frame_head(0))
}

/// PG `window_last_value`: the argument value of the frame's last row.
#[must_use]
pub fn window_last_value(winobj: &WindowObject) -> WindowFuncResult {
    WindowFuncResult::from_nullable(winobj.func_arg_in_frame_tail(0))
}

/// PG `window_nth_value`: the argument value of the frame's nth row (1-based, arg 1).
#[must_use]
pub fn window_nth_value(winobj: &WindowObject) -> WindowFuncResult {
    let Some(nth_d) = winobj.func_arg_current(1) else {
        return WindowFuncResult::null();
    };
    let nth = DatumGetInt32(nth_d);
    if nth <= 0 {
        return WindowFuncResult::null();
    }
    WindowFuncResult::from_nullable(winobj.func_arg_in_frame_nth(0, i64::from(nth) - 1))
}

/// The result of a partition-relative argument fetch (lag/lead): either the value
/// (possibly SQL NULL) or "outside the partition" (lag/lead substitute the default).
pub enum WindowArg {
    InRange(Option<Datum>),
    OutOfPartition,
}
