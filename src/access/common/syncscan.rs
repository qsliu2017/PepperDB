//! access/common/syncscan.c - scan synchronization support
//!
//! When multiple backends run a sequential scan on the same table, we try to
//! keep them synchronized to reduce the overall I/O needed.  We keep track of
//! the scan position of each table in a small fixed-size shared-memory LRU
//! list, and start new scans close to where the previous scan(s) are.

use crate::prelude::*;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};

use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorEquals};
use crate::common::relpath::RelFileNumber;
use crate::pg_config::BLCKSZ;
use crate::utils::rel::Relation;

// storage/relfilelocator.h: InvalidRelFileNumber (== InvalidOid).
const InvalidRelFileNumber: RelFileNumber = InvalidOid;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees.
// ---------------------------------------------------------------------------

/// storage/lwlock.h LWLockMode.
pub type LWLockMode = c_int;
pub const LW_EXCLUSIVE: LWLockMode = 0;

/// storage/lwlock.h: opaque LWLock identifier.  In C SyncScanLock is a named
/// built-in lock; we model the argument we pass to the lock routines.
pub type LWLockId = c_int;

/// lwlocknames.h SyncScanLock. TODO: real lock id from the built-in lock list.
const SyncScanLock: LWLockId = 0;

/// storage/lwlock.h LWLockAcquire(). TODO: not yet ported.
unsafe fn LWLockAcquire(_lock: LWLockId, _mode: LWLockMode) -> bool {
    unimplemented!()
}

/// storage/lwlock.h LWLockRelease(). TODO: not yet ported.
unsafe fn LWLockRelease(_lock: LWLockId) {
    unimplemented!()
}

/// storage/lwlock.h LWLockConditionalAcquire(). TODO: not yet ported.
unsafe fn LWLockConditionalAcquire(_lock: LWLockId, _mode: LWLockMode) -> bool { unimplemented!() }

/// storage/shmem.h ShmemInitStruct(). TODO: not yet ported.
unsafe fn ShmemInitStruct(
    _name: *const c_char,
    _size: Size,
    found_ptr: *mut bool,
) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size as Size, found_ptr)
}

// miscadmin.h IsUnderPostmaster (extern global).
use crate::utils::init::globals::IsUnderPostmaster;

// ---------------------------------------------------------------------------
// GUC variables
// ---------------------------------------------------------------------------

// #ifdef TRACE_SYNCSCAN: trace_syncscan is omitted (TRACE_SYNCSCAN undefined).

/// Size of the LRU list.
///
/// Note: the code assumes that SYNC_SCAN_NELEM > 1.
const SYNC_SCAN_NELEM: usize = 20;

/// Interval between reports of the location of the current scan, in pages.
///
/// Note: This should be smaller than the ring size we use for bulk reads.
const SYNC_SCAN_REPORT_INTERVAL: BlockNumber = (128 * 1024 / BLCKSZ) as BlockNumber;

/// The scan locations structure is essentially a doubly-linked LRU with head
/// and tail pointer, designed to hold a fixed maximum number of elements in
/// fixed-size shared memory.
#[derive(Clone, Copy)]
struct ss_scan_location_t {
    relfilelocator: RelFileLocator, /* identity of a relation */
    location: BlockNumber,          /* last-reported location in the relation */
}

struct ss_lru_item_t {
    prev: *mut ss_lru_item_t,
    next: *mut ss_lru_item_t,
    location: ss_scan_location_t,
}

#[repr(C)]
struct ss_scan_locations_t {
    head: *mut ss_lru_item_t,
    tail: *mut ss_lru_item_t,
    items: [ss_lru_item_t; SYNC_SCAN_NELEM], /* FLEXIBLE_ARRAY_MEMBER: SYNC_SCAN_NELEM items */
}

/// SizeOfScanLocations(N): offsetof(items) + N * sizeof(ss_lru_item_t).
fn SizeOfScanLocations(n: usize) -> Size {
    (std::mem::offset_of!(ss_scan_locations_t, items)
        + n * std::mem::size_of::<ss_lru_item_t>()) as Size
}

/// Pointer to struct in shared memory.
static mut scan_locations: *mut ss_scan_locations_t = null_mut();

/// SyncScanShmemSize --- report amount of shared memory space needed
pub fn SyncScanShmemSize() -> Size {
    SizeOfScanLocations(SYNC_SCAN_NELEM)
}

/// SyncScanShmemInit --- initialize this module's shared memory
pub unsafe fn SyncScanShmemInit() {
    let mut found: bool = false;

    scan_locations = ShmemInitStruct(
        c"Sync Scan Locations List".as_ptr(),
        SizeOfScanLocations(SYNC_SCAN_NELEM),
        &mut found,
    ) as *mut ss_scan_locations_t;

    if !IsUnderPostmaster {
        // Initialize shared memory area
        Assert!(!found);

        let sl = &mut *scan_locations;
        // Use a raw base pointer to the items array so that wiring each node's
        // prev/next to its neighbours doesn't require overlapping &mut borrows.
        let items_ptr = sl.items.as_mut_ptr();
        sl.head = items_ptr;
        sl.tail = items_ptr.add(SYNC_SCAN_NELEM - 1);

        for i in 0..SYNC_SCAN_NELEM {
            let item = &mut *items_ptr.add(i);

            // Initialize all slots with invalid values. As scans are started,
            // these invalid entries will fall off the LRU list and get
            // replaced with real entries.
            item.location.relfilelocator.spcOid = InvalidOid;
            item.location.relfilelocator.dbOid = InvalidOid;
            item.location.relfilelocator.relNumber = InvalidRelFileNumber;
            item.location.location = InvalidBlockNumber;

            item.prev = if i > 0 {
                items_ptr.add(i - 1)
            } else {
                null_mut()
            };
            item.next = if i < SYNC_SCAN_NELEM - 1 {
                items_ptr.add(i + 1)
            } else {
                null_mut()
            };
        }
    } else {
        Assert!(found);
    }
}

/// ss_search --- search the scan_locations structure for an entry with the
/// given relfilelocator.
///
/// If "set" is true, the location is updated to the given location.  If no
/// entry for the given relfilelocator is found, it will be created at the head
/// of the list with the given location, even if "set" is false.
///
/// In any case, the location after possible update is returned.
///
/// Caller is responsible for having acquired suitable lock on the shared
/// data structure.
unsafe fn ss_search(
    relfilelocator: RelFileLocator,
    location: BlockNumber,
    set: bool,
) -> BlockNumber {
    let sl = &mut *scan_locations;
    let mut item: *mut ss_lru_item_t = sl.head;
    loop {
        let match_: bool =
            RelFileLocatorEquals(&(*item).location.relfilelocator, &relfilelocator);

        if match_ || (*item).next.is_null() {
            // If we reached the end of list and no match was found, take over
            // the last entry
            if !match_ {
                (*item).location.relfilelocator = relfilelocator;
                (*item).location.location = location;
            } else if set {
                (*item).location.location = location;
            }

            // Move the entry to the front of the LRU list
            if item != sl.head {
                // unlink
                if item == sl.tail {
                    sl.tail = (*item).prev;
                }
                (*(*item).prev).next = (*item).next;
                if !(*item).next.is_null() {
                    (*(*item).next).prev = (*item).prev;
                }

                // link
                (*item).prev = null_mut();
                (*item).next = sl.head;
                (*sl.head).prev = item;
                sl.head = item;
            }

            return (*item).location.location;
        }

        item = (*item).next;
    }

    // not reached
}

/// ss_get_location --- get the optimal starting location for scan
///
/// Returns the last-reported location of a sequential scan on the relation, or
/// 0 if no valid location is found.
///
/// We expect the caller has just done RelationGetNumberOfBlocks(), and so that
/// number is passed in rather than computing it again.  The result is
/// guaranteed less than relnblocks (assuming that's > 0).
pub unsafe fn ss_get_location(rel: Relation, relnblocks: BlockNumber) -> BlockNumber {
    let mut startloc: BlockNumber;

    LWLockAcquire(SyncScanLock, LW_EXCLUSIVE);
    startloc = ss_search(core::mem::transmute((*rel).rd_locator), 0, false);
    LWLockRelease(SyncScanLock);

    // If the location is not a valid block number for this scan, start at 0.
    //
    // This can happen if for instance a VACUUM truncated the table since the
    // location was saved.
    if startloc >= relnblocks {
        startloc = 0;
    }

    // #ifdef TRACE_SYNCSCAN: trace logging omitted.

    startloc
}

/// ss_report_location --- update the current scan location
///
/// Writes an entry into the shared Sync Scan state of the form
/// (relfilelocator, blocknumber), overwriting any existing entry for the same
/// relfilelocator.
pub unsafe fn ss_report_location(rel: Relation, location: BlockNumber) {
    // #ifdef TRACE_SYNCSCAN: trace logging omitted.

    // To reduce lock contention, only report scan progress every N pages. For
    // the same reason, don't block if the lock isn't immediately available.
    // Missing a few updates isn't critical, it just means that a new scan that
    // wants to join the pack will start a little bit behind the head of the
    // scan.  Hopefully the pages are still in OS cache and the scan catches up
    // quickly.
    if (location % SYNC_SCAN_REPORT_INTERVAL) == 0 {
        if LWLockConditionalAcquire(SyncScanLock, LW_EXCLUSIVE) {
            let _ = ss_search(core::mem::transmute((*rel).rd_locator), location, true);
            LWLockRelease(SyncScanLock);
        }
        // #ifdef TRACE_SYNCSCAN: missed-update trace logging omitted.
    }
}
