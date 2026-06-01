//! Translation of postgres/src/include/access/stratnum.h
//!
//! Strategy numbers identify the semantics that particular operators have with
//! respect to particular operator classes.  In some cases a strategy subtype
//! (an OID) is used as further information.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::uint16;

/// Operator-class strategy number.
pub type StrategyNumber = uint16;

pub const InvalidStrategy: StrategyNumber = 0;

/* Strategy numbers for B-tree indexes. */
pub const BTLessStrategyNumber: StrategyNumber = 1;
pub const BTLessEqualStrategyNumber: StrategyNumber = 2;
pub const BTEqualStrategyNumber: StrategyNumber = 3;
pub const BTGreaterEqualStrategyNumber: StrategyNumber = 4;
pub const BTGreaterStrategyNumber: StrategyNumber = 5;
pub const BTMaxStrategyNumber: StrategyNumber = 5;

/* Strategy numbers for hash indexes (only equality is valid). */
pub const HTEqualStrategyNumber: StrategyNumber = 1;
pub const HTMaxStrategyNumber: StrategyNumber = 1;

/*
 * Strategy numbers common to (some) GiST, SP-GiST and BRIN opclasses.  The
 * first few come from the R-Tree indexing method (hence the names); the others
 * have been added over time as needed.
 */
pub const RTLeftStrategyNumber: StrategyNumber = 1; /* for << */
pub const RTOverLeftStrategyNumber: StrategyNumber = 2; /* for &< */
pub const RTOverlapStrategyNumber: StrategyNumber = 3; /* for && */
pub const RTOverRightStrategyNumber: StrategyNumber = 4; /* for &> */
pub const RTRightStrategyNumber: StrategyNumber = 5; /* for >> */
pub const RTSameStrategyNumber: StrategyNumber = 6; /* for ~= */
pub const RTContainsStrategyNumber: StrategyNumber = 7; /* for @> */
pub const RTContainedByStrategyNumber: StrategyNumber = 8; /* for <@ */
pub const RTOverBelowStrategyNumber: StrategyNumber = 9; /* for &<| */
pub const RTBelowStrategyNumber: StrategyNumber = 10; /* for <<| */
pub const RTAboveStrategyNumber: StrategyNumber = 11; /* for |>> */
pub const RTOverAboveStrategyNumber: StrategyNumber = 12; /* for |&> */
pub const RTOldContainsStrategyNumber: StrategyNumber = 13; /* old spelling of @> */
pub const RTOldContainedByStrategyNumber: StrategyNumber = 14; /* old spelling of <@ */
pub const RTKNNSearchStrategyNumber: StrategyNumber = 15; /* for <-> (distance) */
pub const RTContainsElemStrategyNumber: StrategyNumber = 16; /* range types @> elem */
pub const RTAdjacentStrategyNumber: StrategyNumber = 17; /* for -|- */
pub const RTEqualStrategyNumber: StrategyNumber = 18; /* for = */
pub const RTNotEqualStrategyNumber: StrategyNumber = 19; /* for != */
pub const RTLessStrategyNumber: StrategyNumber = 20; /* for < */
pub const RTLessEqualStrategyNumber: StrategyNumber = 21; /* for <= */
pub const RTGreaterStrategyNumber: StrategyNumber = 22; /* for > */
pub const RTGreaterEqualStrategyNumber: StrategyNumber = 23; /* for >= */
pub const RTSubStrategyNumber: StrategyNumber = 24; /* for inet >> */
pub const RTSubEqualStrategyNumber: StrategyNumber = 25; /* for inet <<= */
pub const RTSuperStrategyNumber: StrategyNumber = 26; /* for inet << */
pub const RTSuperEqualStrategyNumber: StrategyNumber = 27; /* for inet >>= */
pub const RTPrefixStrategyNumber: StrategyNumber = 28; /* for text ^@ */
pub const RTOldBelowStrategyNumber: StrategyNumber = 29; /* old spelling of <<| */
pub const RTOldAboveStrategyNumber: StrategyNumber = 30; /* old spelling of |>> */
pub const RTMaxStrategyNumber: StrategyNumber = 30;

// NOTE: access/common/scankey.rs defines its own local `StrategyNumber` alias;
// repoint it to import from here when convenient (both are uint16, no clash).
