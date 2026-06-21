//! Translation of postgres/src/backend/storage/freespace/indexfsm.c
//! (declarations from postgres/src/include/storage/indexfsm.h).
//!
//! POSTGRES free space map for quickly finding free pages in index relations.
//!
//! This is similar to the FSM used for heap, in freespace.c, but instead of
//! tracking the *amount* of free space on pages, we only track whether pages
//! are completely free or in-use. We use the same FSM implementation as for
//! heaps, using 0 to denote used pages, and (BLCKSZ - 1) for unused. The
//! whole-page nature is why these are thin wrappers that simply pass 0 or
//! BLCKSZ-1 (and BLCKSZ/2 as the search threshold) to the generic FSM.
//!
//! NOTE: the four underlying generic FSM routines (freespace.c) are NOT yet
//! ported, so they are stubbed as `unimplemented!`. The wrapper bodies below
//! are translated 1:1 and are correct-shape, but are NOT runnable until
//! storage/freespace.c is translated.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::nodes::execnodes::Relation;
use crate::pg_config::BLCKSZ;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use core::ffi::c_int;

// ---------------------------------------------------------------------------
// STUBS: storage/freespace.c is not yet ported. These mirror the C prototypes
// from storage/freespace.h. The `Size` (amount of free space) argument is an
// `int` in C, represented here as `c_int`.
// ---------------------------------------------------------------------------

/// STUB for freespace.c:GetPageWithFreeSpace.
///
/// # Safety
/// `rel` must be a valid Relation once freespace.c is ported.
unsafe fn GetPageWithFreeSpace(rel: Relation, spaceNeeded: c_int) -> BlockNumber {
    crate::storage::freespace::freespace::GetPageWithFreeSpace(rel as _, spaceNeeded as crate::c::Size)
}

/// STUB for freespace.c:RecordPageWithFreeSpace.
///
/// # Safety
/// `rel` must be a valid Relation once freespace.c is ported.
unsafe fn RecordPageWithFreeSpace(rel: Relation, heapBlk: BlockNumber, spaceAvail: c_int) {
    crate::storage::freespace::freespace::RecordPageWithFreeSpace(rel as _, heapBlk, spaceAvail as crate::c::Size)
}

/// STUB for freespace.c:FreeSpaceMapVacuum.
///
/// # Safety
/// `rel` must be a valid Relation once freespace.c is ported.
unsafe fn FreeSpaceMapVacuum(rel: Relation) {
    crate::storage::freespace::freespace::FreeSpaceMapVacuum(rel as _);
}

// ---------------------------------------------------------------------------
// Exported routines
// ---------------------------------------------------------------------------

/// GetFreeIndexPage - return a free page from the FSM
///
/// As a side effect, the page is marked as used in the FSM.
///
/// # Safety
/// `rel` must reference a valid index relation.
pub unsafe fn GetFreeIndexPage(rel: Relation) -> BlockNumber {
    let blkno: BlockNumber = GetPageWithFreeSpace(rel, (BLCKSZ / 2) as c_int);

    if blkno != InvalidBlockNumber {
        RecordUsedIndexPage(rel, blkno);
    }

    blkno
}

/// RecordFreeIndexPage - mark a page as free in the FSM
///
/// # Safety
/// `rel` must reference a valid index relation.
#[no_mangle]
pub unsafe fn RecordFreeIndexPage(rel: Relation, freeBlock: BlockNumber) {
    RecordPageWithFreeSpace(rel, freeBlock, (BLCKSZ - 1) as c_int);
}

/// RecordUsedIndexPage - mark a page as used in the FSM
///
/// # Safety
/// `rel` must reference a valid index relation.
pub unsafe fn RecordUsedIndexPage(rel: Relation, usedBlock: BlockNumber) {
    RecordPageWithFreeSpace(rel, usedBlock, 0);
}

/// IndexFreeSpaceMapVacuum - scan and fix any inconsistencies in the FSM
///
/// # Safety
/// `rel` must reference a valid index relation.
#[no_mangle]
pub unsafe fn IndexFreeSpaceMapVacuum(rel: Relation) {
    FreeSpaceMapVacuum(rel);
}

#[cfg(test)]
mod tests {
    use super::*;

    // No meaningful behavioral test is possible without storage/freespace.c.
    // Verify only the constants/thresholds the wrappers depend on.
    #[test]
    fn fsm_thresholds() {
        assert_eq!(BLCKSZ, 8192);
        // GetFreeIndexPage searches with BLCKSZ/2 (== 4096, integer division).
        assert_eq!(BLCKSZ / 2, 4096);
        // RecordFreeIndexPage marks "completely free" as BLCKSZ-1.
        assert_eq!(BLCKSZ - 1, 8191);
        assert_eq!(InvalidBlockNumber, 0xFFFF_FFFF);
    }
}
