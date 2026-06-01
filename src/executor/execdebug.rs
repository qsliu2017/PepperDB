//! execdebug.h - #defines governing debugging behaviour in the executor.
//!
//! XXX this is all pretty old and crufty.  Newer code tends to use elog()
//! for debug printouts, because that's more flexible than printf().
//!
//! The EXEC_NESTLOOPDEBUG / EXEC_SORTDEBUG / EXEC_MERGEJOINDEBUG compile-time
//! flags are OFF by default in PostgreSQL, so the per-node debug macros expand
//! to nothing.  We mirror that default here: the debug helpers are no-op
//! `#[inline] pub fn`s.  Only T_OR_F and NULL_OR_TUPLE are unconditionally
//! defined in the C header.

use std::ffi::c_int;

use crate::executor::tuptable::{TupleTableSlot, TupIsNull};

/* ----------------------------------------------------------------
 *		#defines controlled by above definitions
 * ----------------------------------------------------------------
 */

/// `#define T_OR_F(b) ((b) ? "true" : "false")`
#[inline]
pub fn T_OR_F(b: bool) -> &'static str {
    if b { "true" } else { "false" }
}

/// `#define NULL_OR_TUPLE(slot) (TupIsNull(slot) ? "null" : "a tuple")`
#[inline]
pub unsafe fn NULL_OR_TUPLE(slot: *mut TupleTableSlot) -> &'static str {
    if TupIsNull(slot) { "null" } else { "a tuple" }
}

/* ----------------
 *		nest loop debugging defines
 *
 *		EXEC_NESTLOOPDEBUG is undefined by default, so these are no-ops.
 * ----------------
 */
#[inline]
pub fn NL_nodeDisplay<L>(_l: L) {}
#[inline]
pub fn NL_printf(_s: &str) {}
#[inline]
pub fn NL1_printf<A>(_s: &str, _a: A) {}
#[inline]
pub fn ENL1_printf(_message: &str) {}

/* ----------------
 *		sort node debugging defines
 *
 *		EXEC_SORTDEBUG is undefined by default, so these are no-ops.
 * ----------------
 */
#[inline]
pub fn SO_nodeDisplay<L>(_l: L) {}
#[inline]
pub fn SO_printf(_s: &str) {}
#[inline]
pub fn SO1_printf<P>(_s: &str, _p: P) {}
#[inline]
pub fn SO2_printf<P1, P2>(_s: &str, _p1: P1, _p2: P2) {}

/* ----------------
 *		merge join debugging defines
 *
 *		EXEC_MERGEJOINDEBUG is undefined by default, so these are no-ops.
 * ----------------
 */
#[inline]
pub fn MJ_nodeDisplay<L>(_l: L) {}
#[inline]
pub fn MJ_printf(_s: &str) {}
#[inline]
pub fn MJ1_printf<P>(_s: &str, _p: P) {}
#[inline]
pub fn MJ2_printf<P1, P2>(_s: &str, _p1: P1, _p2: P2) {}
#[inline]
pub fn MJ_debugtup<S>(_slot: S) {}
#[inline]
pub fn MJ_dump<S>(_state: S) {}
#[inline]
pub fn MJ_DEBUG_COMPARE(_res: c_int) {}
#[inline]
pub fn MJ_DEBUG_QUAL<C, R>(_clause: C, _res: R) {}
#[inline]
pub fn MJ_DEBUG_PROC_NODE<S>(_slot: S) {}
