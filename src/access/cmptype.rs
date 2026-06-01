//! access/cmptype.h - POSTGRES compare type definitions.

use std::ffi::c_int;

// CompareType - fundamental semantics of certain operators
//
// These enum symbols represent the fundamental semantics of certain operators
// that the system needs to have some hardcoded knowledge about.  (For
// example, RowCompareExpr needs to know which operators can be determined to
// act like =, <>, <, etc.)  Index access methods map (some of) strategy
// numbers to these values so that the system can know about the meaning of
// (some of) the operators without needing hardcoded knowledge of index AM's
// strategy numbering.
//
// XXX Currently, this mapping is not fully developed and most values are
// chosen to match btree strategy numbers, which is not going to work very
// well for other access methods.
//
// Project convention: C enum -> `pub type X = c_int` alias plus `pub const`
// variants (matches src/access/index/amapi.rs CompareType handling).
pub type CompareType = c_int;

pub const COMPARE_INVALID: CompareType = 0;
pub const COMPARE_LT: CompareType = 1; // BTLessStrategyNumber
pub const COMPARE_LE: CompareType = 2; // BTLessEqualStrategyNumber
pub const COMPARE_EQ: CompareType = 3; // BTEqualStrategyNumber
pub const COMPARE_GE: CompareType = 4; // BTGreaterEqualStrategyNumber
pub const COMPARE_GT: CompareType = 5; // BTGreaterStrategyNumber
pub const COMPARE_NE: CompareType = 6; // no such btree strategy
pub const COMPARE_OVERLAP: CompareType = 7;
pub const COMPARE_CONTAINED_BY: CompareType = 8;
