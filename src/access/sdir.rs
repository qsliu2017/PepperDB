//! access/sdir.h - POSTGRES scan direction definitions.

use crate::c::*;
use std::ffi::c_int;

/*
 * Defines the direction for scanning a table or an index.  Scans are never
 * invoked using NoMovementScanDirection.  For convenience, we use the values
 * -1 and 1 for backward and forward scans.  This allows us to perform a few
 * mathematical tricks such as what is done in ScanDirectionCombine.
 *
 * NB: the canonical home for ScanDirection is here. plannodes.rs currently
 * carries a `pub type ScanDirection = c_int;` stub awaiting this file - the
 * main agent should dedup that and re-point uses at this module.
 */
pub type ScanDirection = c_int;

pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const ForwardScanDirection: ScanDirection = 1;

/*
 * Determine the net effect of two direction specifications.
 * This relies on having ForwardScanDirection = +1, BackwardScanDirection = -1,
 * and will probably not do what you want if applied to any other values.
 */
#[inline]
pub fn ScanDirectionCombine(a: ScanDirection, b: ScanDirection) -> ScanDirection {
    a * b
}

/*
 * ScanDirectionIsValid
 *		True iff scan direction is valid.
 */
#[inline]
pub fn ScanDirectionIsValid(direction: ScanDirection) -> bool {
    BackwardScanDirection <= direction && direction <= ForwardScanDirection
}

/*
 * ScanDirectionIsBackward
 *		True iff scan direction is backward.
 */
#[inline]
pub fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    direction == BackwardScanDirection
}

/*
 * ScanDirectionIsNoMovement
 *		True iff scan direction indicates no movement.
 */
#[inline]
pub fn ScanDirectionIsNoMovement(direction: ScanDirection) -> bool {
    direction == NoMovementScanDirection
}

/*
 * ScanDirectionIsForward
 *		True iff scan direction is forward.
 */
#[inline]
pub fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    direction == ForwardScanDirection
}
