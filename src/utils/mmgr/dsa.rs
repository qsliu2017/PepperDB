//! Translation of postgres/src/backend/utils/mmgr/dsa.c
//!
//! Dynamic shared memory areas.
//!
//! This module provides dynamic shared memory areas which are built on top of
//! DSM segments.  While dsm.c allows segments of memory of shared memory to be
//! created and shared between backends, it isn't designed to deal with small
//! objects.  A DSA area is a shared memory heap usually backed by one or more
//! DSM segments which can allocate memory using dsa_allocate() and dsa_free().
//! Alternatively, it can be created in pre-existing shared memory, including a
//! DSM segment, and then create extra DSM segments as required.  Unlike the
//! regular system heap, it deals in pseudo-pointers which must be converted to
//! backend-local pointers before they are dereferenced.  These pseudo-pointers
//! can however be shared with other backends, and can be used to construct
//! shared data structures.
//!
//! Each DSA area manages a set of DSM segments, adding new segments as
//! required and detaching them when they are no longer needed.  Each segment
//! contains a number of 4KB pages, a free page manager for tracking
//! consecutive runs of free pages, and a page map for tracking the source of
//! objects allocated on each page.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/utils/mmgr/dsa.c

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused_parens)]

use core::ffi::{c_char, c_int, c_void};

// c.h: Size, uint8/16/32/64, MAXALIGN, Min, unlikely.
use crate::c::{uint16, uint32, uint64, uint8, Min, Size, MAXALIGN};
// c.rs: unlikely() lives in crate::c.
use crate::c::unlikely;
// postgres.h: Datum and pointer conversions.
use crate::postgres::{DatumGetPointer, Datum, PointerGetDatum};
// utils/palloc.h: palloc/pfree.
use crate::utils::palloc::{palloc, pfree};
// utils/memutils.h: alloc size validity checks.
use crate::utils::memutils::{AllocHugeSizeIsValid, AllocSizeIsValid};
// port/pg_bitutils.h: leftmost set bit position over size_t.
use crate::port::pg_bitutils::pg_leftmost_one_pos_size_t;
// elog/ereport macros.
use crate::{elog, ereport, errmsg, lengthof, Assert};

// ----------------------------------------------------------------------------
// Types and constants from utils/dsa.h (merged in).
// ----------------------------------------------------------------------------

// On 64-bit, dsa_pointer is a 64-bit value with a 40-bit offset.
pub type dsa_pointer = uint64;

// The number of bytes used to represent a dsa_pointer.
pub const SIZEOF_DSA_POINTER: usize = 8;

// The number of bits used to represent the offset part of a dsa_pointer.
pub const DSA_OFFSET_WIDTH: usize = 40; // 1024 segments of size up to 1TB

// A sentinel value for dsa_pointer used to indicate failure to allocate.
pub const InvalidDsaPointer: dsa_pointer = 0;

// The default size of the initial DSM segment that backs a dsa_area.
pub const DSA_DEFAULT_INIT_SEGMENT_SIZE: Size = 1 * 1024 * 1024;

// The minimum size of a DSM segment.
pub const DSA_MIN_SEGMENT_SIZE: Size = 256 * 1024;

// The maximum size of a DSM segment.
pub const DSA_MAX_SEGMENT_SIZE: Size = (1 as Size) << DSA_OFFSET_WIDTH;

// Flags for dsa_allocate_extended.
pub const DSA_ALLOC_HUGE: c_int = 0x01; // allow huge allocation (> 1 GB)
pub const DSA_ALLOC_NO_OOM: c_int = 0x02; // no failure if out-of-memory
pub const DSA_ALLOC_ZERO: c_int = 0x04; // zero allocated memory

// Check if a dsa_pointer value is valid.
#[inline]
pub fn DsaPointerIsValid(x: dsa_pointer) -> bool {
    x != InvalidDsaPointer
}

// The type used for dsa_area handles (a dsm_handle for the first segment).
pub type dsa_handle = dsm_handle;

// Sentinel value to use for invalid dsa_handles.
pub const DSA_HANDLE_INVALID: dsa_handle = DSM_HANDLE_INVALID;

// ----------------------------------------------------------------------------
// Stubbed dependencies that live in other .c files (TODO(pg-port)).
// ----------------------------------------------------------------------------

// storage/dsm.h: dsm_handle and DSM_HANDLE_INVALID.
pub type dsm_handle = uint32;
pub const DSM_HANDLE_INVALID: dsm_handle = 0;

// storage/dsm.h: opaque DSM segment handle (backend-local).
#[repr(C)]
pub struct dsm_segment {
    pub _private: [u8; 0],
}

// utils/resowner.h: opaque resource owner; the global current resource owner.
pub type ResourceOwner = *mut c_void;

// storage/lwlock.h: LWLock and lock modes.
#[repr(C)]
pub struct LWLock {
    pub _private: [u8; 0],
}
pub type LWLockMode = c_int;
pub const LW_EXCLUSIVE: LWLockMode = 0;

// utils/freepage.h: FreePageManager and FPM_PAGE_SIZE.
#[repr(C)]
pub struct FreePageManager {
    pub _private: [u8; 0],
}
// freepage.h: FPM_PAGE_SIZE is BLCKSZ (8192) here.
pub const FPM_PAGE_SIZE: Size = 8192;

// errcodes used by ereport (folded into "C also:" comments below).
// ERRCODE_OUT_OF_MEMORY, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE.

// TODO(pg-port): storage/dsm.c
unsafe fn dsm_create(_size: Size, _flags: c_int) -> *mut dsm_segment {
    unimplemented!("dsm_create: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_attach(_h: dsm_handle) -> *mut dsm_segment {
    unimplemented!("dsm_attach: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_segment_address(_seg: *mut dsm_segment) -> *mut c_void {
    unimplemented!("dsm_segment_address: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_segment_handle(_seg: *mut dsm_segment) -> dsm_handle {
    unimplemented!("dsm_segment_handle: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_pin_segment(_seg: *mut dsm_segment) {
    unimplemented!("dsm_pin_segment: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_unpin_segment(_handle: dsm_handle) {
    unimplemented!("dsm_unpin_segment: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_detach(_seg: *mut dsm_segment) {
    unimplemented!("dsm_detach: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn dsm_pin_mapping(_seg: *mut dsm_segment) {
    unimplemented!("dsm_pin_mapping: storage/dsm.c not yet ported")
}
// TODO(pg-port): storage/dsm.c
unsafe fn on_dsm_detach(
    _seg: *mut dsm_segment,
    _function: unsafe fn(*mut dsm_segment, Datum),
    _arg: Datum,
) {
    unimplemented!("on_dsm_detach: storage/dsm.c not yet ported")
}

// TODO(pg-port): storage/lwlock.c
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: LWLockMode) -> bool {
    unimplemented!("LWLockAcquire: storage/lwlock.c not yet ported")
}
// TODO(pg-port): storage/lwlock.c
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!("LWLockRelease: storage/lwlock.c not yet ported")
}
// TODO(pg-port): storage/lwlock.c
unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    unimplemented!("LWLockInitialize: storage/lwlock.c not yet ported")
}
// TODO(pg-port): storage/lwlock.c
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    unimplemented!("LWLockHeldByMe: storage/lwlock.c not yet ported")
}

// TODO(pg-port): utils/freepage.c
unsafe fn FreePageManagerInitialize(_fpm: *mut FreePageManager, _base: *mut c_char) {
    unimplemented!("FreePageManagerInitialize: utils/freepage.c not yet ported")
}
// TODO(pg-port): utils/freepage.c
unsafe fn FreePageManagerGet(
    _fpm: *mut FreePageManager,
    _npages: Size,
    _first_page: *mut Size,
) -> bool {
    unimplemented!("FreePageManagerGet: utils/freepage.c not yet ported")
}
// TODO(pg-port): utils/freepage.c
unsafe fn FreePageManagerPut(_fpm: *mut FreePageManager, _first_page: Size, _npages: Size) {
    unimplemented!("FreePageManagerPut: utils/freepage.c not yet ported")
}
// TODO(pg-port): utils/freepage.c
unsafe fn fpm_largest(_fpm: *mut FreePageManager) -> Size {
    unimplemented!("fpm_largest: utils/freepage.c not yet ported")
}
// freepage.h: fpm_size_to_pages(size) - number of pages needed to hold size.
#[inline]
fn fpm_size_to_pages(size: Size) -> Size {
    (size + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE
}

// TODO(pg-port): utils/resowner/resowner.c - CurrentResourceOwner global.
static mut CurrentResourceOwner: ResourceOwner = core::ptr::null_mut();

// port/atomics.h: read barrier. TODO(pg-port): real barrier in port/atomics.
#[inline]
fn pg_read_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Acquire);
}

// ----------------------------------------------------------------------------
// dsa.c private constants and macros.
// ----------------------------------------------------------------------------

// How many segments to create before we double the segment size.
const DSA_NUM_SEGMENTS_AT_EACH_SIZE: usize = 2;

// The maximum number of DSM segments that an area can own.
const DSA_MAX_SEGMENTS: usize = {
    let cap = 1usize << ((SIZEOF_DSA_POINTER * 8) - DSA_OFFSET_WIDTH);
    if 1024 < cap {
        1024
    } else {
        cap
    }
};

// The bitmask for extracting the offset from a dsa_pointer.
const DSA_OFFSET_BITMASK: dsa_pointer = ((1 as dsa_pointer) << DSA_OFFSET_WIDTH) - 1;

// Number of pages (see FPM_PAGE_SIZE) per regular superblock.
const DSA_PAGES_PER_SUPERBLOCK: Size = 16;

// A magic number used as a sanity check for following DSM segments.
const DSA_SEGMENT_HEADER_MAGIC: uint32 = 0x0ce26608;

// Build a dsa_pointer given a segment number and offset.
#[inline]
fn DSA_MAKE_POINTER(segment_number: Size, offset: Size) -> dsa_pointer {
    ((segment_number as dsa_pointer) << DSA_OFFSET_WIDTH) | (offset as dsa_pointer)
}

// Extract the segment number from a dsa_pointer.
#[inline]
fn DSA_EXTRACT_SEGMENT_NUMBER(dp: dsa_pointer) -> dsa_segment_index {
    (dp >> DSA_OFFSET_WIDTH) as dsa_segment_index
}

// Extract the offset from a dsa_pointer.
#[inline]
fn DSA_EXTRACT_OFFSET(dp: dsa_pointer) -> Size {
    (dp & DSA_OFFSET_BITMASK) as Size
}

// The type used for index segment indexes (zero based).
pub type dsa_segment_index = Size;

// Sentinel value for dsa_segment_index indicating 'none' or 'end'.
const DSA_SEGMENT_INDEX_NONE: dsa_segment_index = !(0 as dsa_segment_index);

// How many bins of segments do we have?
const DSA_NUM_SEGMENT_BINS: usize = 16;

// What is the lowest bin that holds segments that *might* have n contiguous
// free pages?
#[inline]
fn contiguous_pages_to_segment_bin(n: Size) -> Size {
    let bin: Size;

    if n == 0 {
        bin = 0;
    } else {
        bin = pg_leftmost_one_pos_size_t(n as uint64) as Size + 1;
    }

    Min(bin, DSA_NUM_SEGMENT_BINS - 1)
}

// Macros for access to locks.
#[inline]
unsafe fn DSA_AREA_LOCK(area: *mut dsa_area) -> *mut LWLock {
    &mut (*(*area).control).lock
}
#[inline]
unsafe fn DSA_SCLASS_LOCK(area: *mut dsa_area, sclass: usize) -> *mut LWLock {
    &mut (*(*area).control).pools[sclass].lock
}

// The header for an individual segment.
#[repr(C)]
pub struct dsa_segment_header {
    // Sanity check magic value.
    pub magic: uint32,
    // Total number of pages in this segment (excluding metadata area).
    pub usable_pages: Size,
    // Total size of this segment in bytes.
    pub size: Size,
    // Index of the segment that precedes this one in the same segment bin.
    pub prev: dsa_segment_index,
    // Index of the segment that follows this one in the same segment bin.
    pub next: dsa_segment_index,
    // The index of the bin that contains this segment.
    pub bin: Size,
    // A flag raised to indicate that this segment is being returned to the OS.
    pub freed: bool,
}

// Metadata for one superblock.
#[repr(C)]
pub struct dsa_area_span {
    pub pool: dsa_pointer,     // Containing pool.
    pub prevspan: dsa_pointer, // Previous span.
    pub nextspan: dsa_pointer, // Next span.
    pub start: dsa_pointer,    // Starting address.
    pub npages: Size,          // Length of span in pages.
    pub size_class: uint16,    // Size class.
    pub ninitialized: uint16,  // Maximum number of objects ever allocated.
    pub nallocatable: uint16,  // Number of objects currently allocatable.
    pub firstfree: uint16,     // First object on free list.
    pub nmax: uint16,          // Maximum number of objects ever possible.
    pub fclass: uint16,        // Current fullness class.
}

// Given a pointer to an object in a span, access the index of the next free
// object in the same span (ie in the span's freelist) as an L-value.
#[inline]
unsafe fn NextFreeObjectIndex_get(object: *mut c_char) -> uint16 {
    *(object as *mut uint16)
}
#[inline]
unsafe fn NextFreeObjectIndex_set(object: *mut c_char, value: uint16) {
    *(object as *mut uint16) = value;
}

// The possible allocation sizes for small objects.
static dsa_size_classes: [uint16; 40] = [
    core::mem::size_of::<dsa_area_span>() as uint16,
    0, // special size classes
    8, 16, 24, 32, 40, 48, 56, 64, // 8 classes separated by 8 bytes
    80, 96, 112, 128, // 4 classes separated by 16 bytes
    160, 192, 224, 256, // 4 classes separated by 32 bytes
    320, 384, 448, 512, // 4 classes separated by 64 bytes
    640, 768, 896, 1024, // 4 classes separated by 128 bytes
    1280, 1560, 1816, 2048, // 4 classes separated by ~256 bytes
    2616, 3120, 3640, 4096, // 4 classes separated by ~512 bytes
    5456, 6552, 7280, 8192, // 4 classes separated by ~1024 bytes
];
const DSA_NUM_SIZE_CLASSES: usize = 40;

// Special size classes.
const DSA_SCLASS_BLOCK_OF_SPANS: usize = 0;
const DSA_SCLASS_SPAN_LARGE: usize = 1;

// The following lookup table is used to map the size of small objects
// (less than 1kB) onto the corresponding size class.
static dsa_size_class_map: [uint8; 128] = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15, 16, 16,
    16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 20, 20,
    20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
    22, 22, 22, 22, 22, 22, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25,
    25, 25, 25, 25, 25, 25,
];
const DSA_SIZE_CLASS_MAP_QUANTUM: Size = 8;

// Superblocks are binned by how full they are.
const DSA_FULLNESS_CLASSES: usize = 4;

// A dsa_area_pool represents a set of objects of a given size class.
#[repr(C)]
pub struct dsa_area_pool {
    // A lock protecting access to this pool.
    pub lock: LWLock,
    // A set of linked lists of spans, arranged by fullness.
    pub spans: [dsa_pointer; DSA_FULLNESS_CLASSES],
}

// The control block for an area.
#[repr(C)]
pub struct dsa_area_control {
    // The segment header for the first segment.
    pub segment_header: dsa_segment_header,
    // The handle for this area.
    pub handle: dsa_handle,
    // The handles of the segments owned by this area.
    pub segment_handles: [dsm_handle; DSA_MAX_SEGMENTS],
    // Lists of segments, binned by maximum contiguous run of free pages.
    pub segment_bins: [dsa_segment_index; DSA_NUM_SEGMENT_BINS],
    // The object pools for each size class.
    pub pools: [dsa_area_pool; DSA_NUM_SIZE_CLASSES],
    // initial allocation segment size
    pub init_segment_size: Size,
    // maximum allocation segment size
    pub max_segment_size: Size,
    // The total size of all active segments.
    pub total_segment_size: Size,
    // The maximum total size of backing storage we are allowed.
    pub max_total_segment_size: Size,
    // Highest used segment index in the history of this area.
    pub high_segment_index: dsa_segment_index,
    // The reference count for this area.
    pub refcnt: c_int,
    // A flag indicating that this area has been pinned.
    pub pinned: bool,
    // The number of times that segments have been freed.
    pub freed_segment_counter: Size,
    // The LWLock tranche ID.
    pub lwlock_tranche_id: c_int,
    // The general lock (protects everything except object pools).
    pub lock: LWLock,
}

// Given a pointer to a pool, find a dsa_pointer.
#[inline]
unsafe fn DsaAreaPoolToDsaPointer(area: *mut dsa_area, p: *mut dsa_area_pool) -> dsa_pointer {
    DSA_MAKE_POINTER(
        0,
        (p as *mut c_char).offset_from((*area).control as *mut c_char) as Size,
    )
}

// A dsa_segment_map is stored within the backend-private memory of each
// individual backend.
#[repr(C)]
pub struct dsa_segment_map {
    pub segment: *mut dsm_segment,          // DSM segment
    pub mapped_address: *mut c_char,        // Address at which segment is mapped
    pub header: *mut dsa_segment_header,    // Header (same as mapped_address)
    pub fpm: *mut FreePageManager,          // Free page manager within segment.
    pub pagemap: *mut dsa_pointer,          // Page map within segment.
}

// Per-backend state for a storage area.
#[repr(C)]
pub struct dsa_area {
    // Pointer to the control object in shared memory.
    pub control: *mut dsa_area_control,
    // All the mappings are owned by this. NULL if session lifespan.
    pub resowner: ResourceOwner,
    // This backend's array of segment maps, ordered by segment index.
    pub segment_maps: [dsa_segment_map; DSA_MAX_SEGMENTS],
    // The highest segment index this backend has ever mapped.
    pub high_segment_index: dsa_segment_index,
    // The last observed freed_segment_counter.
    pub freed_segment_counter: Size,
}

const DSA_SPAN_NOTHING_FREE: uint16 = !0u16;
const DSA_SUPERBLOCK_SIZE: Size = DSA_PAGES_PER_SUPERBLOCK * FPM_PAGE_SIZE;

// Given a pointer to a segment_map, obtain a segment index number.
#[inline]
unsafe fn get_segment_index(area: *mut dsa_area, segment_map_ptr: *mut dsa_segment_map) -> dsa_segment_index {
    segment_map_ptr.offset_from(&mut (*area).segment_maps[0] as *mut dsa_segment_map) as dsa_segment_index
}

// ----------------------------------------------------------------------------
// Public functions.
// ----------------------------------------------------------------------------

// Create a new shared area in a new DSM segment.  Further DSM segments will
// be allocated as required to extend the available space.
pub unsafe fn dsa_create_ext(
    tranche_id: c_int,
    init_segment_size: Size,
    max_segment_size: Size,
) -> *mut dsa_area {
    let segment: *mut dsm_segment;
    let area: *mut dsa_area;

    // Create the DSM segment that will hold the shared control object and the
    // first segment of usable space.
    segment = dsm_create(init_segment_size, 0);

    // All segments backing this area are pinned, so that DSA can explicitly
    // control their lifetime.
    dsm_pin_segment(segment);

    // Create a new DSA area with the control object in this segment.
    area = create_internal(
        dsm_segment_address(segment),
        init_segment_size,
        tranche_id,
        dsm_segment_handle(segment),
        segment,
        init_segment_size,
        max_segment_size,
    );

    // Clean up when the control segment detaches.
    on_dsm_detach(
        segment,
        dsa_on_dsm_detach_release_in_place,
        PointerGetDatum(dsm_segment_address(segment)),
    );

    area
}

// Create a new shared area in an existing shared memory space, which may be
// either DSM or Postmaster-initialized memory.
pub unsafe fn dsa_create_in_place_ext(
    place: *mut c_void,
    size: Size,
    tranche_id: c_int,
    segment: *mut dsm_segment,
    init_segment_size: Size,
    max_segment_size: Size,
) -> *mut dsa_area {
    let area: *mut dsa_area;

    area = create_internal(
        place,
        size,
        tranche_id,
        DSM_HANDLE_INVALID,
        core::ptr::null_mut(),
        init_segment_size,
        max_segment_size,
    );

    // Clean up when the control segment detaches, if a containing DSM segment
    // was provided.
    if !segment.is_null() {
        on_dsm_detach(segment, dsa_on_dsm_detach_release_in_place, PointerGetDatum(place));
    }

    area
}

// Obtain a handle that can be passed to other processes so that they can
// attach to the given area.
pub unsafe fn dsa_get_handle(area: *mut dsa_area) -> dsa_handle {
    Assert!((*(*area).control).handle != DSA_HANDLE_INVALID);
    (*(*area).control).handle
}

// Attach to an area given a handle generated by dsa_get_handle.
pub unsafe fn dsa_attach(handle: dsa_handle) -> *mut dsa_area {
    let segment: *mut dsm_segment;
    let area: *mut dsa_area;

    // An area handle is really a DSM segment handle for the first segment, so
    // we go ahead and attach to that.
    segment = dsm_attach(handle);
    if segment.is_null() {
        ereport!(
            ERROR,
            errmsg!("could not attach to dynamic shared area")
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        );
    }

    area = attach_internal(dsm_segment_address(segment), segment, handle);

    // Clean up when the control segment detaches.
    on_dsm_detach(
        segment,
        dsa_on_dsm_detach_release_in_place,
        PointerGetDatum(dsm_segment_address(segment)),
    );

    area
}

// Attach to an area that was created with dsa_create_in_place.
pub unsafe fn dsa_attach_in_place(place: *mut c_void, segment: *mut dsm_segment) -> *mut dsa_area {
    let area: *mut dsa_area;

    area = attach_internal(place, core::ptr::null_mut(), DSA_HANDLE_INVALID);

    // Clean up when the control segment detaches, if a containing DSM segment
    // was provided.
    if !segment.is_null() {
        on_dsm_detach(segment, dsa_on_dsm_detach_release_in_place, PointerGetDatum(place));
    }

    area
}

// Release a DSA area that was produced by dsa_create_in_place or
// dsa_attach_in_place.  Suitable for on_dsm_detach.
pub unsafe fn dsa_on_dsm_detach_release_in_place(_segment: *mut dsm_segment, place: Datum) {
    dsa_release_in_place(DatumGetPointer(place) as *mut c_void);
}

// Release a DSA area that was produced by dsa_create_in_place or
// dsa_attach_in_place.  Suitable for on_shmem_exit or before_shmem_exit.
pub unsafe fn dsa_on_shmem_exit_release_in_place(_code: c_int, place: Datum) {
    dsa_release_in_place(DatumGetPointer(place) as *mut c_void);
}

// Release a DSA area that was produced by dsa_create_in_place or
// dsa_attach_in_place.
pub unsafe fn dsa_release_in_place(place: *mut c_void) {
    let control: *mut dsa_area_control = place as *mut dsa_area_control;
    let mut i: c_int;

    LWLockAcquire(&mut (*control).lock, LW_EXCLUSIVE);
    Assert!((*control).segment_header.magic == (DSA_SEGMENT_HEADER_MAGIC ^ (*control).handle ^ 0));
    Assert!((*control).refcnt > 0);
    (*control).refcnt -= 1;
    if (*control).refcnt == 0 {
        i = 0;
        while i <= (*control).high_segment_index as c_int {
            let handle: dsm_handle;

            handle = (*control).segment_handles[i as usize];
            if handle != DSM_HANDLE_INVALID {
                dsm_unpin_segment(handle);
            }
            i += 1;
        }
    }
    LWLockRelease(&mut (*control).lock);
}

// Keep a DSA area attached until end of session or explicit detach.
pub unsafe fn dsa_pin_mapping(area: *mut dsa_area) {
    let mut i: c_int;

    if !(*area).resowner.is_null() {
        (*area).resowner = core::ptr::null_mut();

        i = 0;
        while i <= (*area).high_segment_index as c_int {
            if !(*area).segment_maps[i as usize].segment.is_null() {
                dsm_pin_mapping((*area).segment_maps[i as usize].segment);
            }
            i += 1;
        }
    }
}

// Allocate memory in this storage area.
pub unsafe fn dsa_allocate_extended(area: *mut dsa_area, size: Size, flags: c_int) -> dsa_pointer {
    let size_class: uint16;
    let start_pointer: dsa_pointer;
    let mut segment_map: *mut dsa_segment_map;
    let result: dsa_pointer;

    Assert!(size > 0);

    // Sanity check on huge individual allocation size.
    if ((flags & DSA_ALLOC_HUGE) != 0 && !AllocHugeSizeIsValid(size))
        || ((flags & DSA_ALLOC_HUGE) == 0 && !AllocSizeIsValid(size))
    {
        elog!(ERROR, "invalid DSA memory alloc request size {}", size);
    }

    // If bigger than the largest size class, just grab a run of pages from the
    // free page manager, instead of allocating an object from a pool.
    if size > dsa_size_classes[lengthof!(dsa_size_classes) - 1] as Size {
        let npages: Size = fpm_size_to_pages(size);
        let mut first_page: Size = 0;
        let span_pointer: dsa_pointer;
        let pool: *mut dsa_area_pool = &mut (*(*area).control).pools[DSA_SCLASS_SPAN_LARGE];

        // Obtain a span object.
        span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS as c_int);
        if !DsaPointerIsValid(span_pointer) {
            // Raise error unless asked not to.
            if (flags & DSA_ALLOC_NO_OOM) == 0 {
                ereport!(
                    ERROR,
                    errmsg!("out of memory")
                    // C also: errcode(ERRCODE_OUT_OF_MEMORY),
                    // errdetail("Failed on DSA request of size {}.", size)
                );
            }
            return InvalidDsaPointer;
        }

        LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);

        // Find a segment from which to allocate.
        segment_map = get_best_segment(area, npages);
        if segment_map.is_null() {
            segment_map = make_new_segment(area, npages);
        }
        if segment_map.is_null() {
            // Can't make any more segments: game over.
            LWLockRelease(DSA_AREA_LOCK(area));
            dsa_free(area, span_pointer);

            // Raise error unless asked not to.
            if (flags & DSA_ALLOC_NO_OOM) == 0 {
                ereport!(
                    ERROR,
                    errmsg!("out of memory")
                    // C also: errcode(ERRCODE_OUT_OF_MEMORY),
                    // errdetail("Failed on DSA request of size {}.", size)
                );
            }
            return InvalidDsaPointer;
        }

        // Ask the free page manager for a run of pages.  This should always
        // succeed.  If it does fail, use FATAL to kill the process.
        if !FreePageManagerGet((*segment_map).fpm, npages, &mut first_page) {
            elog!(FATAL, "dsa_allocate could not find {} free pages", npages);
        }
        LWLockRelease(DSA_AREA_LOCK(area));

        start_pointer = DSA_MAKE_POINTER(
            get_segment_index(area, segment_map),
            first_page * FPM_PAGE_SIZE,
        );

        // Initialize span and pagemap.
        LWLockAcquire(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE), LW_EXCLUSIVE);
        init_span(area, span_pointer, pool, start_pointer, npages, DSA_SCLASS_SPAN_LARGE as uint16);
        *(*segment_map).pagemap.add(first_page) = span_pointer;
        LWLockRelease(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE));

        // Zero-initialize the memory if requested.
        if (flags & DSA_ALLOC_ZERO) != 0 {
            core::ptr::write_bytes(dsa_get_address(area, start_pointer) as *mut u8, 0, size);
        }

        return start_pointer;
    }

    // Map allocation to a size class.
    if size < lengthof!(dsa_size_class_map) * DSA_SIZE_CLASS_MAP_QUANTUM {
        let mapidx: usize;

        // For smaller sizes we have a lookup table...
        mapidx = ((size + DSA_SIZE_CLASS_MAP_QUANTUM - 1) / DSA_SIZE_CLASS_MAP_QUANTUM) - 1;
        size_class = dsa_size_class_map[mapidx] as uint16;
    } else {
        let mut min: uint16;
        let mut max: uint16;

        // ... and for the rest we search by binary chop.
        min = dsa_size_class_map[lengthof!(dsa_size_class_map) - 1] as uint16;
        max = (lengthof!(dsa_size_classes) - 1) as uint16;

        while min < max {
            let mid: uint16 = (min + max) / 2;
            let class_size: uint16 = dsa_size_classes[mid as usize];

            if (class_size as Size) < size {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        size_class = min;
    }
    Assert!(size <= dsa_size_classes[size_class as usize] as Size);
    Assert!(size_class == 0 || size > dsa_size_classes[size_class as usize - 1] as Size);

    // Attempt to allocate an object from the appropriate pool.
    result = alloc_object(area, size_class as c_int);

    // Check for failure to allocate.
    if !DsaPointerIsValid(result) {
        // Raise error unless asked not to.
        if (flags & DSA_ALLOC_NO_OOM) == 0 {
            ereport!(
                ERROR,
                errmsg!("out of memory")
                // C also: errcode(ERRCODE_OUT_OF_MEMORY),
                // errdetail("Failed on DSA request of size {}.", size)
            );
        }
        return InvalidDsaPointer;
    }

    // Zero-initialize the memory if requested.
    if (flags & DSA_ALLOC_ZERO) != 0 {
        core::ptr::write_bytes(dsa_get_address(area, result) as *mut u8, 0, size);
    }

    result
}

// Free memory obtained with dsa_allocate.
pub unsafe fn dsa_free(area: *mut dsa_area, dp: dsa_pointer) {
    let segment_map: *mut dsa_segment_map;
    let pageno: c_int;
    let span_pointer: dsa_pointer;
    let span: *mut dsa_area_span;
    let superblock: *mut c_char;
    let object: *mut c_char;
    let size: Size;
    let size_class: c_int;

    // Make sure we don't have a stale segment in the slot 'dp' refers to.
    check_for_freed_segments(area);

    // Locate the object, span and pool.
    segment_map = get_segment_by_index(area, DSA_EXTRACT_SEGMENT_NUMBER(dp));
    pageno = (DSA_EXTRACT_OFFSET(dp) / FPM_PAGE_SIZE) as c_int;
    span_pointer = *(*segment_map).pagemap.add(pageno as usize);
    span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
    superblock = dsa_get_address(area, (*span).start) as *mut c_char;
    object = dsa_get_address(area, dp) as *mut c_char;
    size_class = (*span).size_class as c_int;
    size = dsa_size_classes[size_class as usize] as Size;

    // Special case for large objects that live in a special span: we return
    // those pages directly to the free page manager and free the span.
    if (*span).size_class as usize == DSA_SCLASS_SPAN_LARGE {
        // C #ifdef CLOBBER_FREED_MEMORY: memset(object, 0x7f, npages * FPM_PAGE_SIZE).

        // Give pages back to free page manager.
        LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
        FreePageManagerPut(
            (*segment_map).fpm,
            DSA_EXTRACT_OFFSET((*span).start) / FPM_PAGE_SIZE,
            (*span).npages,
        );

        // Move segment to appropriate bin if necessary.
        rebin_segment(area, segment_map);
        LWLockRelease(DSA_AREA_LOCK(area));

        // Unlink span.
        LWLockAcquire(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE), LW_EXCLUSIVE);
        unlink_span(area, span);
        LWLockRelease(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE));
        // Free the span object so it can be reused.
        dsa_free(area, span_pointer);
        return;
    }

    // C #ifdef CLOBBER_FREED_MEMORY: memset(object, 0x7f, size).

    LWLockAcquire(DSA_SCLASS_LOCK(area, size_class as usize), LW_EXCLUSIVE);

    // Put the object on the span's freelist.
    Assert!(object >= superblock);
    Assert!(object < superblock.add(DSA_SUPERBLOCK_SIZE));
    Assert!((object.offset_from(superblock) as Size) % size == 0);
    NextFreeObjectIndex_set(object, (*span).firstfree);
    (*span).firstfree = (object.offset_from(superblock) as Size / size) as uint16;
    (*span).nallocatable += 1;

    // See if the span needs to moved to a different fullness class, or be
    // freed so its pages can be given back to the segment.
    if (*span).nallocatable == 1 && (*span).fclass as usize == DSA_FULLNESS_CLASSES - 1 {
        // The block was completely full and is located in the highest-numbered
        // fullness class, which is never scanned for free chunks.  We must move
        // it to the next-lower fullness class.
        unlink_span(area, span);
        add_span_to_fullness_class(area, span, span_pointer, (DSA_FULLNESS_CLASSES - 2) as c_int);

        // If this is the only span, and there is no active span, then we should
        // probably move this span to fullness class 1.
    } else if (*span).nallocatable == (*span).nmax
        && ((*span).fclass != 1 || (*span).prevspan != InvalidDsaPointer)
    {
        // This entire block is free, and it's not the active block for this
        // size class.  Return the memory to the free page manager.
        destroy_superblock(area, span_pointer);
    }

    LWLockRelease(DSA_SCLASS_LOCK(area, size_class as usize));
}

// Obtain a backend-local address for a dsa_pointer.
pub unsafe fn dsa_get_address(area: *mut dsa_area, dp: dsa_pointer) -> *mut c_void {
    let index: dsa_segment_index;
    let offset: Size;

    // Convert InvalidDsaPointer to NULL.
    if !DsaPointerIsValid(dp) {
        return core::ptr::null_mut();
    }

    // Process any requests to detach from freed segments.
    check_for_freed_segments(area);

    // Break the dsa_pointer into its components.
    index = DSA_EXTRACT_SEGMENT_NUMBER(dp);
    offset = DSA_EXTRACT_OFFSET(dp);
    Assert!(index < DSA_MAX_SEGMENTS);

    // Check if we need to cause this segment to be mapped in.
    if unlikely((*area).segment_maps[index].mapped_address.is_null()) {
        // Call for effect (we don't need the result).
        get_segment_by_index(area, index);
    }

    (*area).segment_maps[index].mapped_address.add(offset) as *mut c_void
}

// Pin this area, so that it will continue to exist even if all backends
// detach from it.
pub unsafe fn dsa_pin(area: *mut dsa_area) {
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    if (*(*area).control).pinned {
        LWLockRelease(DSA_AREA_LOCK(area));
        elog!(ERROR, "dsa_area already pinned");
    }
    (*(*area).control).pinned = true;
    (*(*area).control).refcnt += 1;
    LWLockRelease(DSA_AREA_LOCK(area));
}

// Undo the effects of dsa_pin.
pub unsafe fn dsa_unpin(area: *mut dsa_area) {
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    Assert!((*(*area).control).refcnt > 1);
    if !(*(*area).control).pinned {
        LWLockRelease(DSA_AREA_LOCK(area));
        elog!(ERROR, "dsa_area not pinned");
    }
    (*(*area).control).pinned = false;
    (*(*area).control).refcnt -= 1;
    LWLockRelease(DSA_AREA_LOCK(area));
}

// Set the total size limit for this area.
pub unsafe fn dsa_set_size_limit(area: *mut dsa_area, limit: Size) {
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    (*(*area).control).max_total_segment_size = limit;
    LWLockRelease(DSA_AREA_LOCK(area));
}

// Return the total size of all active segments.
pub unsafe fn dsa_get_total_size(area: *mut dsa_area) -> Size {
    let size: Size;

    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    size = (*(*area).control).total_segment_size;
    LWLockRelease(DSA_AREA_LOCK(area));

    size
}

// Aggressively free all spare memory in the hope of returning DSM segments to
// the operating system.
pub unsafe fn dsa_trim(area: *mut dsa_area) {
    let mut size_class: c_int;

    // Trim in reverse pool order so we get to the spans-of-spans last.
    size_class = DSA_NUM_SIZE_CLASSES as c_int - 1;
    while size_class >= 0 {
        let pool: *mut dsa_area_pool = &mut (*(*area).control).pools[size_class as usize];
        let mut span_pointer: dsa_pointer;

        if size_class as usize == DSA_SCLASS_SPAN_LARGE {
            // Large object frees give back segments aggressively already.
            size_class -= 1;
            continue;
        }

        // Search fullness class 1 only.
        LWLockAcquire(DSA_SCLASS_LOCK(area, size_class as usize), LW_EXCLUSIVE);
        span_pointer = (*pool).spans[1];
        while DsaPointerIsValid(span_pointer) {
            let span: *mut dsa_area_span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
            let next: dsa_pointer = (*span).nextspan;

            if (*span).nallocatable == (*span).nmax {
                destroy_superblock(area, span_pointer);
            }

            span_pointer = next;
        }
        LWLockRelease(DSA_SCLASS_LOCK(area, size_class as usize));

        size_class -= 1;
    }
}

// Print out debugging information about the internal state of the shared
// memory area.
pub unsafe fn dsa_dump(area: *mut dsa_area) {
    let mut i: Size;
    let mut j: Size;

    // Note: This gives an inconsistent snapshot as it acquires and releases
    // individual locks as it goes...

    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    check_for_freed_segments_locked(area);
    eprintln!("dsa_area handle {:x}:", (*(*area).control).handle);
    eprintln!("  max_total_segment_size: {}", (*(*area).control).max_total_segment_size);
    eprintln!("  total_segment_size: {}", (*(*area).control).total_segment_size);
    eprintln!("  refcnt: {}", (*(*area).control).refcnt);
    eprintln!("  pinned: {}", if (*(*area).control).pinned { 't' } else { 'f' });
    eprintln!("  segment bins:");
    i = 0;
    while i < DSA_NUM_SEGMENT_BINS {
        if (*(*area).control).segment_bins[i] != DSA_SEGMENT_INDEX_NONE {
            let mut segment_index: dsa_segment_index;

            if i == 0 {
                eprintln!("    segment bin {} (no contiguous free pages):", i);
            } else {
                eprintln!(
                    "    segment bin {} (at least {} contiguous pages free):",
                    i,
                    1 << (i - 1)
                );
            }
            segment_index = (*(*area).control).segment_bins[i];
            while segment_index != DSA_SEGMENT_INDEX_NONE {
                let segment_map: *mut dsa_segment_map;

                segment_map = get_segment_by_index(area, segment_index);

                eprintln!(
                    "      segment index {}, usable_pages = {}, contiguous_pages = {}, mapped at {:p}",
                    segment_index,
                    (*(*segment_map).header).usable_pages,
                    fpm_largest((*segment_map).fpm),
                    (*segment_map).mapped_address
                );
                segment_index = (*(*segment_map).header).next;
            }
        }
        i += 1;
    }
    LWLockRelease(DSA_AREA_LOCK(area));

    eprintln!("  pools:");
    i = 0;
    while i < DSA_NUM_SIZE_CLASSES {
        let mut found: bool = false;

        LWLockAcquire(DSA_SCLASS_LOCK(area, i), LW_EXCLUSIVE);
        j = 0;
        while j < DSA_FULLNESS_CLASSES {
            if DsaPointerIsValid((*(*area).control).pools[i].spans[j]) {
                found = true;
            }
            j += 1;
        }
        if found {
            if i == DSA_SCLASS_BLOCK_OF_SPANS {
                eprintln!("    pool for blocks of span objects:");
            } else if i == DSA_SCLASS_SPAN_LARGE {
                eprintln!("    pool for large object spans:");
            } else {
                eprintln!(
                    "    pool for size class {} (object size {} bytes):",
                    i, dsa_size_classes[i]
                );
            }
            j = 0;
            while j < DSA_FULLNESS_CLASSES {
                if !DsaPointerIsValid((*(*area).control).pools[i].spans[j]) {
                    eprintln!("      fullness class {} is empty", j);
                } else {
                    let mut span_pointer: dsa_pointer = (*(*area).control).pools[i].spans[j];

                    eprintln!("      fullness class {}:", j);
                    while DsaPointerIsValid(span_pointer) {
                        let span: *mut dsa_area_span;

                        span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
                        eprintln!(
                            "        span descriptor at {:016x}, superblock at {:016x}, pages = {}, objects free = {}/{}",
                            span_pointer,
                            (*span).start,
                            (*span).npages,
                            (*span).nallocatable,
                            (*span).nmax
                        );
                        span_pointer = (*span).nextspan;
                    }
                }
                j += 1;
            }
        }
        LWLockRelease(DSA_SCLASS_LOCK(area, i));
        i += 1;
    }
}

// Return the smallest size that you can successfully provide to
// dsa_create_in_place.
pub fn dsa_minimum_size() -> Size {
    let mut size: Size;
    let mut pages: c_int = 0;

    size = MAXALIGN(core::mem::size_of::<dsa_area_control>())
        + MAXALIGN(core::mem::size_of::<FreePageManager>());

    // Figure out how many pages we need, including the page map...
    while ((size + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE) > pages as Size {
        pages += 1;
        size += core::mem::size_of::<dsa_pointer>();
    }

    pages as Size * FPM_PAGE_SIZE
}

// Workhorse function for dsa_create and dsa_create_in_place.
unsafe fn create_internal(
    place: *mut c_void,
    size: Size,
    tranche_id: c_int,
    control_handle: dsm_handle,
    control_segment: *mut dsm_segment,
    init_segment_size: Size,
    max_segment_size: Size,
) -> *mut dsa_area {
    let control: *mut dsa_area_control;
    let area: *mut dsa_area;
    let segment_map: *mut dsa_segment_map;
    let usable_pages: Size;
    let total_pages: Size;
    let mut metadata_bytes: Size;
    let mut i: c_int;

    // Check the initial and maximum block sizes.
    Assert!(init_segment_size >= DSA_MIN_SEGMENT_SIZE);
    Assert!(max_segment_size >= init_segment_size);
    Assert!(max_segment_size <= DSA_MAX_SEGMENT_SIZE);

    // Sanity check on the space we have to work in.
    if size < dsa_minimum_size() {
        elog!(
            ERROR,
            "dsa_area space must be at least {}, but {} provided",
            dsa_minimum_size(),
            size
        );
    }

    // Now figure out how much space is usable.
    total_pages = size / FPM_PAGE_SIZE;
    metadata_bytes = MAXALIGN(core::mem::size_of::<dsa_area_control>())
        + MAXALIGN(core::mem::size_of::<FreePageManager>())
        + total_pages * core::mem::size_of::<dsa_pointer>();
    // Add padding up to next page boundary.
    if metadata_bytes % FPM_PAGE_SIZE != 0 {
        metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
    }
    Assert!(metadata_bytes <= size);
    usable_pages = (size - metadata_bytes) / FPM_PAGE_SIZE;

    // Initialize the dsa_area_control object located at the start of the space.
    control = place as *mut dsa_area_control;
    core::ptr::write_bytes(place as *mut u8, 0, core::mem::size_of::<dsa_area_control>());
    (*control).segment_header.magic = DSA_SEGMENT_HEADER_MAGIC ^ control_handle ^ 0;
    (*control).segment_header.next = DSA_SEGMENT_INDEX_NONE;
    (*control).segment_header.prev = DSA_SEGMENT_INDEX_NONE;
    (*control).segment_header.usable_pages = usable_pages;
    (*control).segment_header.freed = false;
    (*control).segment_header.size = size;
    (*control).handle = control_handle;
    (*control).init_segment_size = init_segment_size;
    (*control).max_segment_size = max_segment_size;
    (*control).max_total_segment_size = !(0 as Size);
    (*control).total_segment_size = size;
    (*control).segment_handles[0] = control_handle;
    i = 0;
    while i < DSA_NUM_SEGMENT_BINS as c_int {
        (*control).segment_bins[i as usize] = DSA_SEGMENT_INDEX_NONE;
        i += 1;
    }
    (*control).refcnt = 1;
    (*control).lwlock_tranche_id = tranche_id;

    // Create the dsa_area object that this backend will use to access the area.
    area = palloc(core::mem::size_of::<dsa_area>()) as *mut dsa_area;
    (*area).control = control;
    (*area).resowner = CurrentResourceOwner;
    core::ptr::write_bytes(
        (*area).segment_maps.as_mut_ptr() as *mut u8,
        0,
        core::mem::size_of::<dsa_segment_map>() * DSA_MAX_SEGMENTS,
    );
    (*area).high_segment_index = 0;
    (*area).freed_segment_counter = 0;
    LWLockInitialize(&mut (*control).lock, (*control).lwlock_tranche_id);
    i = 0;
    while i < DSA_NUM_SIZE_CLASSES as c_int {
        LWLockInitialize(DSA_SCLASS_LOCK(area, i as usize), (*control).lwlock_tranche_id);
        i += 1;
    }

    // Set up the segment map for this process's mapping.
    segment_map = &mut (*area).segment_maps[0];
    (*segment_map).segment = control_segment;
    (*segment_map).mapped_address = place as *mut c_char;
    (*segment_map).header = place as *mut dsa_segment_header;
    (*segment_map).fpm = (*segment_map)
        .mapped_address
        .add(MAXALIGN(core::mem::size_of::<dsa_area_control>())) as *mut FreePageManager;
    (*segment_map).pagemap = (*segment_map).mapped_address.add(
        MAXALIGN(core::mem::size_of::<dsa_area_control>())
            + MAXALIGN(core::mem::size_of::<FreePageManager>()),
    ) as *mut dsa_pointer;

    // Set up the free page map.
    FreePageManagerInitialize((*segment_map).fpm, (*segment_map).mapped_address);
    // There can be 0 usable pages if size is dsa_minimum_size().

    if usable_pages > 0 {
        FreePageManagerPut((*segment_map).fpm, metadata_bytes / FPM_PAGE_SIZE, usable_pages);
    }

    // Put this segment into the appropriate bin.
    (*control).segment_bins[contiguous_pages_to_segment_bin(usable_pages)] = 0;
    (*(*segment_map).header).bin = contiguous_pages_to_segment_bin(usable_pages);

    area
}

// Workhorse function for dsa_attach and dsa_attach_in_place.
unsafe fn attach_internal(
    place: *mut c_void,
    segment: *mut dsm_segment,
    handle: dsa_handle,
) -> *mut dsa_area {
    let control: *mut dsa_area_control;
    let area: *mut dsa_area;
    let segment_map: *mut dsa_segment_map;

    control = place as *mut dsa_area_control;
    Assert!((*control).handle == handle);
    Assert!((*control).segment_handles[0] == handle);
    Assert!((*control).segment_header.magic == (DSA_SEGMENT_HEADER_MAGIC ^ handle ^ 0));

    // Build the backend-local area object.
    area = palloc(core::mem::size_of::<dsa_area>()) as *mut dsa_area;
    (*area).control = control;
    (*area).resowner = CurrentResourceOwner;
    core::ptr::write_bytes(
        &mut (*area).segment_maps[0] as *mut dsa_segment_map as *mut u8,
        0,
        core::mem::size_of::<dsa_segment_map>() * DSA_MAX_SEGMENTS,
    );
    (*area).high_segment_index = 0;

    // Set up the segment map for this process's mapping.
    segment_map = &mut (*area).segment_maps[0];
    (*segment_map).segment = segment; // NULL for in-place
    (*segment_map).mapped_address = place as *mut c_char;
    (*segment_map).header = (*segment_map).mapped_address as *mut dsa_segment_header;
    (*segment_map).fpm = (*segment_map)
        .mapped_address
        .add(MAXALIGN(core::mem::size_of::<dsa_area_control>())) as *mut FreePageManager;
    (*segment_map).pagemap = (*segment_map).mapped_address.add(
        MAXALIGN(core::mem::size_of::<dsa_area_control>())
            + MAXALIGN(core::mem::size_of::<FreePageManager>()),
    ) as *mut dsa_pointer;

    // Bump the reference count.
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    if (*control).refcnt == 0 {
        // We can't attach to a DSA area that has already been destroyed.
        ereport!(
            ERROR,
            errmsg!("could not attach to dynamic shared area")
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        );
    }
    (*control).refcnt += 1;
    (*area).freed_segment_counter = (*(*area).control).freed_segment_counter;
    LWLockRelease(DSA_AREA_LOCK(area));

    area
}

// Add a new span to fullness class 1 of the indicated pool.
unsafe fn init_span(
    area: *mut dsa_area,
    span_pointer: dsa_pointer,
    pool: *mut dsa_area_pool,
    start: dsa_pointer,
    npages: Size,
    size_class: uint16,
) {
    let span: *mut dsa_area_span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
    let obsize: Size = dsa_size_classes[size_class as usize] as Size;

    // The per-pool lock must be held because we manipulate the span list for
    // this pool.
    Assert!(LWLockHeldByMe(DSA_SCLASS_LOCK(area, size_class as usize)));

    // Push this span onto the front of the span list for fullness class 1.
    if DsaPointerIsValid((*pool).spans[1]) {
        let head: *mut dsa_area_span = dsa_get_address(area, (*pool).spans[1]) as *mut dsa_area_span;

        (*head).prevspan = span_pointer;
    }
    (*span).pool = DsaAreaPoolToDsaPointer(area, pool);
    (*span).nextspan = (*pool).spans[1];
    (*span).prevspan = InvalidDsaPointer;
    (*pool).spans[1] = span_pointer;

    (*span).start = start;
    (*span).npages = npages;
    (*span).size_class = size_class;
    (*span).ninitialized = 0;
    if size_class as usize == DSA_SCLASS_BLOCK_OF_SPANS {
        // A block-of-spans contains its own descriptor, so mark one object as
        // initialized and reduce the count of allocatable objects by one.
        (*span).ninitialized = 1;
        (*span).nallocatable = (FPM_PAGE_SIZE / obsize - 1) as uint16;
    } else if size_class as usize != DSA_SCLASS_SPAN_LARGE {
        (*span).nallocatable = (DSA_SUPERBLOCK_SIZE / obsize) as uint16;
    }
    (*span).firstfree = DSA_SPAN_NOTHING_FREE;
    (*span).nmax = (*span).nallocatable;
    (*span).fclass = 1;
}

// Transfer the first span in one fullness class to the head of another
// fullness class.
unsafe fn transfer_first_span(
    area: *mut dsa_area,
    pool: *mut dsa_area_pool,
    fromclass: c_int,
    toclass: c_int,
) -> bool {
    let span_pointer: dsa_pointer;
    let span: *mut dsa_area_span;
    let nextspan: *mut dsa_area_span;

    // Can't do it if source list is empty.
    span_pointer = (*pool).spans[fromclass as usize];
    if !DsaPointerIsValid(span_pointer) {
        return false;
    }

    // Remove span from head of source list.
    span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
    (*pool).spans[fromclass as usize] = (*span).nextspan;
    if DsaPointerIsValid((*span).nextspan) {
        nextspan = dsa_get_address(area, (*span).nextspan) as *mut dsa_area_span;
        (*nextspan).prevspan = InvalidDsaPointer;
    }

    // Add span to head of target list.
    (*span).nextspan = (*pool).spans[toclass as usize];
    (*pool).spans[toclass as usize] = span_pointer;
    if DsaPointerIsValid((*span).nextspan) {
        nextspan = dsa_get_address(area, (*span).nextspan) as *mut dsa_area_span;
        (*nextspan).prevspan = span_pointer;
    }
    (*span).fclass = toclass as uint16;

    true
}

// Allocate one object of the requested size class from the given area.
#[inline]
unsafe fn alloc_object(area: *mut dsa_area, size_class: c_int) -> dsa_pointer {
    let pool: *mut dsa_area_pool = &mut (*(*area).control).pools[size_class as usize];
    let span: *mut dsa_area_span;
    let block: dsa_pointer;
    let mut result: dsa_pointer;
    let object: *mut c_char;
    let size: Size;

    // Even though ensure_active_superblock can in turn call alloc_object, that's
    // always from a different pool, and the order of lock acquisition is always
    // the same, so it's OK that we hold this lock for the duration.
    Assert!(!LWLockHeldByMe(DSA_SCLASS_LOCK(area, size_class as usize)));
    LWLockAcquire(DSA_SCLASS_LOCK(area, size_class as usize), LW_EXCLUSIVE);

    // If there's no active superblock, we must successfully obtain one or fail
    // the request.
    if !DsaPointerIsValid((*pool).spans[1])
        && !ensure_active_superblock(area, pool, size_class)
    {
        result = InvalidDsaPointer;
    } else {
        // There should be a block in fullness class 1 at this point, and it
        // should never be completely full.
        Assert!(DsaPointerIsValid((*pool).spans[1]));
        span = dsa_get_address(area, (*pool).spans[1]) as *mut dsa_area_span;
        Assert!((*span).nallocatable > 0);
        block = (*span).start;
        Assert!(size_class < DSA_NUM_SIZE_CLASSES as c_int);
        size = dsa_size_classes[size_class as usize] as Size;
        if (*span).firstfree != DSA_SPAN_NOTHING_FREE {
            result = block + (*span).firstfree as dsa_pointer * size as dsa_pointer;
            object = dsa_get_address(area, result) as *mut c_char;
            (*span).firstfree = NextFreeObjectIndex_get(object);
        } else {
            result = block + (*span).ninitialized as dsa_pointer * size as dsa_pointer;
            (*span).ninitialized += 1;
        }
        (*span).nallocatable -= 1;

        // If it's now full, move it to the highest-numbered fullness class.
        if (*span).nallocatable == 0 {
            transfer_first_span(area, pool, 1, DSA_FULLNESS_CLASSES as c_int - 1);
        }
    }

    Assert!(LWLockHeldByMe(DSA_SCLASS_LOCK(area, size_class as usize)));
    LWLockRelease(DSA_SCLASS_LOCK(area, size_class as usize));

    result
}

// Ensure an active (i.e. fullness class 1) superblock, unless all existing
// superblocks are completely full and no more can be allocated.
unsafe fn ensure_active_superblock(
    area: *mut dsa_area,
    pool: *mut dsa_area_pool,
    size_class: c_int,
) -> bool {
    let mut span_pointer: dsa_pointer;
    let start_pointer: dsa_pointer;
    let obsize: Size = dsa_size_classes[size_class as usize] as Size;
    let nmax: Size;
    let mut fclass: c_int;
    let mut npages: Size = 1;
    let mut first_page: Size = 0;
    let mut i: Size;
    let mut segment_map: *mut dsa_segment_map;

    Assert!(LWLockHeldByMe(DSA_SCLASS_LOCK(area, size_class as usize)));

    // Compute the number of objects that will fit in a block of this size class.
    if size_class as usize == DSA_SCLASS_BLOCK_OF_SPANS {
        nmax = FPM_PAGE_SIZE / obsize - 1;
    } else {
        nmax = DSA_SUPERBLOCK_SIZE / obsize;
    }

    // If fullness class 1 is empty, try to find a span to put in it by scanning
    // higher-numbered fullness classes (excluding the last one).
    fclass = 2;
    while fclass < DSA_FULLNESS_CLASSES as c_int - 1 {
        span_pointer = (*pool).spans[fclass as usize];

        while DsaPointerIsValid(span_pointer) {
            let tfclass: c_int;
            let span: *mut dsa_area_span;
            let mut nextspan: *mut dsa_area_span;
            let prevspan: *mut dsa_area_span;
            let next_span_pointer: dsa_pointer;

            span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
            next_span_pointer = (*span).nextspan;

            // Figure out what fullness class should contain this span.
            tfclass = ((nmax - (*span).nallocatable as Size) * (DSA_FULLNESS_CLASSES - 1) / nmax)
                as c_int;

            // Look up next span.
            if DsaPointerIsValid((*span).nextspan) {
                nextspan = dsa_get_address(area, (*span).nextspan) as *mut dsa_area_span;
            } else {
                nextspan = core::ptr::null_mut();
            }

            // If utilization has dropped enough that this now belongs in some
            // other fullness class, move it there.
            if tfclass < fclass {
                // Remove from the current fullness class list.
                if (*pool).spans[fclass as usize] == span_pointer {
                    // It was the head; remove it.
                    Assert!(!DsaPointerIsValid((*span).prevspan));
                    (*pool).spans[fclass as usize] = (*span).nextspan;
                    if !nextspan.is_null() {
                        (*nextspan).prevspan = InvalidDsaPointer;
                    }
                } else {
                    // It was not the head.
                    Assert!(DsaPointerIsValid((*span).prevspan));
                    prevspan = dsa_get_address(area, (*span).prevspan) as *mut dsa_area_span;
                    (*prevspan).nextspan = (*span).nextspan;
                }
                if !nextspan.is_null() {
                    (*nextspan).prevspan = (*span).prevspan;
                }

                // Push onto the head of the new fullness class list.
                (*span).nextspan = (*pool).spans[tfclass as usize];
                (*pool).spans[tfclass as usize] = span_pointer;
                (*span).prevspan = InvalidDsaPointer;
                if DsaPointerIsValid((*span).nextspan) {
                    nextspan = dsa_get_address(area, (*span).nextspan) as *mut dsa_area_span;
                    (*nextspan).prevspan = span_pointer;
                }
                (*span).fclass = tfclass as uint16;
            }

            // Advance to next span on list.
            span_pointer = next_span_pointer;
        }

        // Stop now if we found a suitable block.
        if DsaPointerIsValid((*pool).spans[1]) {
            return true;
        }

        fclass += 1;
    }

    // If there are no blocks that properly belong in fullness class 1, pick one
    // from some other fullness class and move it there anyway.
    Assert!(!DsaPointerIsValid((*pool).spans[1]));
    fclass = 2;
    while fclass < DSA_FULLNESS_CLASSES as c_int - 1 {
        if transfer_first_span(area, pool, fclass, 1) {
            return true;
        }
        fclass += 1;
    }
    if !DsaPointerIsValid((*pool).spans[1]) && transfer_first_span(area, pool, 0, 1) {
        return true;
    }

    // We failed to find an existing span with free objects, so we need to
    // allocate a new superblock and construct a new span to manage it.
    if size_class as usize != DSA_SCLASS_BLOCK_OF_SPANS {
        span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS as c_int);
        if !DsaPointerIsValid(span_pointer) {
            return false;
        }
        npages = DSA_PAGES_PER_SUPERBLOCK;
    } else {
        span_pointer = InvalidDsaPointer;
    }

    // Find or create a segment and allocate the superblock.
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    segment_map = get_best_segment(area, npages);
    if segment_map.is_null() {
        segment_map = make_new_segment(area, npages);
        if segment_map.is_null() {
            LWLockRelease(DSA_AREA_LOCK(area));
            return false;
        }
    }

    // This shouldn't happen: get_best_segment() or make_new_segment() promised
    // that we can successfully allocate npages.
    if !FreePageManagerGet((*segment_map).fpm, npages, &mut first_page) {
        elog!(
            FATAL,
            "dsa_allocate could not find {} free pages for superblock",
            npages
        );
    }
    LWLockRelease(DSA_AREA_LOCK(area));

    // Compute the start of the superblock.
    start_pointer = DSA_MAKE_POINTER(
        get_segment_index(area, segment_map),
        first_page * FPM_PAGE_SIZE,
    );

    // If this is a block-of-spans, carve the descriptor right out of the
    // allocated space.
    if size_class as usize == DSA_SCLASS_BLOCK_OF_SPANS {
        span_pointer = start_pointer;
    }

    // Initialize span and pagemap.
    init_span(area, span_pointer, pool, start_pointer, npages, size_class as uint16);
    i = 0;
    while i < npages {
        *(*segment_map).pagemap.add(first_page + i) = span_pointer;
        i += 1;
    }

    true
}

// Return the segment map corresponding to a given segment index, mapping the
// segment in if necessary.
unsafe fn get_segment_by_index(area: *mut dsa_area, index: dsa_segment_index) -> *mut dsa_segment_map {
    if unlikely((*area).segment_maps[index].mapped_address.is_null()) {
        let handle: dsm_handle;
        let segment: *mut dsm_segment;
        let segment_map: *mut dsa_segment_map;
        let oldowner: ResourceOwner;

        // If we are reached by dsa_free or dsa_get_address, there must be at
        // least one object allocated in the referenced segment.
        handle = (*(*area).control).segment_handles[index];

        // It's an error to try to access an unused slot.
        if handle == DSM_HANDLE_INVALID {
            elog!(ERROR, "dsa_area could not attach to a segment that has been freed");
        }

        oldowner = CurrentResourceOwner;
        CurrentResourceOwner = (*area).resowner;
        segment = dsm_attach(handle);
        CurrentResourceOwner = oldowner;
        if segment.is_null() {
            elog!(ERROR, "dsa_area could not attach to segment");
        }
        segment_map = &mut (*area).segment_maps[index];
        (*segment_map).segment = segment;
        (*segment_map).mapped_address = dsm_segment_address(segment) as *mut c_char;
        (*segment_map).header = (*segment_map).mapped_address as *mut dsa_segment_header;
        (*segment_map).fpm = (*segment_map)
            .mapped_address
            .add(MAXALIGN(core::mem::size_of::<dsa_segment_header>())) as *mut FreePageManager;
        (*segment_map).pagemap = (*segment_map).mapped_address.add(
            MAXALIGN(core::mem::size_of::<dsa_segment_header>())
                + MAXALIGN(core::mem::size_of::<FreePageManager>()),
        ) as *mut dsa_pointer;

        // Remember the highest index this backend has ever mapped.
        if (*area).high_segment_index < index {
            (*area).high_segment_index = index;
        }

        Assert!(
            (*(*segment_map).header).magic
                == (DSA_SEGMENT_HEADER_MAGIC ^ (*(*area).control).handle ^ index as uint32)
        );
    }

    // Either way we can assert that we aren't returning a freed segment.
    Assert!(!(*(*area).segment_maps[index].header).freed);

    &mut (*area).segment_maps[index]
}

// Return a superblock to the free page manager.  If the underlying segment has
// become entirely free, then return it to the operating system.
unsafe fn destroy_superblock(area: *mut dsa_area, span_pointer: dsa_pointer) {
    let span: *mut dsa_area_span = dsa_get_address(area, span_pointer) as *mut dsa_area_span;
    let size_class: c_int = (*span).size_class as c_int;
    let segment_map: *mut dsa_segment_map;

    // Remove it from its fullness class list.
    unlink_span(area, span);

    // Note: Here we acquire the area lock while we already hold a per-pool lock.
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    check_for_freed_segments_locked(area);
    segment_map = get_segment_by_index(area, DSA_EXTRACT_SEGMENT_NUMBER((*span).start));
    FreePageManagerPut(
        (*segment_map).fpm,
        DSA_EXTRACT_OFFSET((*span).start) / FPM_PAGE_SIZE,
        (*span).npages,
    );
    // Check if the segment is now entirely free.
    if fpm_largest((*segment_map).fpm) == (*(*segment_map).header).usable_pages {
        let index: dsa_segment_index = get_segment_index(area, segment_map);

        // If it's not the segment with extra control data, free it.
        if index != 0 {
            // Give it back to the OS, and allow other backends to detect that
            // they need to detach.
            unlink_segment(area, segment_map);
            (*(*segment_map).header).freed = true;
            Assert!((*(*area).control).total_segment_size >= (*(*segment_map).header).size);
            (*(*area).control).total_segment_size -= (*(*segment_map).header).size;
            dsm_unpin_segment(dsm_segment_handle((*segment_map).segment));
            dsm_detach((*segment_map).segment);
            (*(*area).control).segment_handles[index] = DSM_HANDLE_INVALID;
            (*(*area).control).freed_segment_counter += 1;
            (*segment_map).segment = core::ptr::null_mut();
            (*segment_map).header = core::ptr::null_mut();
            (*segment_map).mapped_address = core::ptr::null_mut();
        }
    }

    // Move segment to appropriate bin if necessary.
    if !(*segment_map).header.is_null() {
        rebin_segment(area, segment_map);
    }

    LWLockRelease(DSA_AREA_LOCK(area));

    // Span-of-spans blocks store the span which describes them within the block
    // itself, so freeing the storage implicitly frees the descriptor also.
    if size_class as usize != DSA_SCLASS_BLOCK_OF_SPANS {
        dsa_free(area, span_pointer);
    }
}

unsafe fn unlink_span(area: *mut dsa_area, span: *mut dsa_area_span) {
    if DsaPointerIsValid((*span).nextspan) {
        let next: *mut dsa_area_span = dsa_get_address(area, (*span).nextspan) as *mut dsa_area_span;

        (*next).prevspan = (*span).prevspan;
    }
    if DsaPointerIsValid((*span).prevspan) {
        let prev: *mut dsa_area_span = dsa_get_address(area, (*span).prevspan) as *mut dsa_area_span;

        (*prev).nextspan = (*span).nextspan;
    } else {
        let pool: *mut dsa_area_pool = dsa_get_address(area, (*span).pool) as *mut dsa_area_pool;

        (*pool).spans[(*span).fclass as usize] = (*span).nextspan;
    }
}

unsafe fn add_span_to_fullness_class(
    area: *mut dsa_area,
    span: *mut dsa_area_span,
    span_pointer: dsa_pointer,
    fclass: c_int,
) {
    let pool: *mut dsa_area_pool = dsa_get_address(area, (*span).pool) as *mut dsa_area_pool;

    if DsaPointerIsValid((*pool).spans[fclass as usize]) {
        let head: *mut dsa_area_span =
            dsa_get_address(area, (*pool).spans[fclass as usize]) as *mut dsa_area_span;

        (*head).prevspan = span_pointer;
    }
    (*span).prevspan = InvalidDsaPointer;
    (*span).nextspan = (*pool).spans[fclass as usize];
    (*pool).spans[fclass as usize] = span_pointer;
    (*span).fclass = fclass as uint16;
}

// Detach from an area that was either created or attached to by this process.
pub unsafe fn dsa_detach(area: *mut dsa_area) {
    let mut i: c_int;

    // Detach from all segments.
    i = 0;
    while i <= (*area).high_segment_index as c_int {
        if !(*area).segment_maps[i as usize].segment.is_null() {
            dsm_detach((*area).segment_maps[i as usize].segment);
        }
        i += 1;
    }

    // Note that 'detaching' doesn't include 'releasing'.

    // Free the backend-local area object.
    pfree(area as *mut c_void);
}

// Unlink a segment from the bin that contains it.
unsafe fn unlink_segment(area: *mut dsa_area, segment_map: *mut dsa_segment_map) {
    if (*(*segment_map).header).prev != DSA_SEGMENT_INDEX_NONE {
        let prev: *mut dsa_segment_map;

        prev = get_segment_by_index(area, (*(*segment_map).header).prev);
        (*(*prev).header).next = (*(*segment_map).header).next;
    } else {
        Assert!(
            (*(*area).control).segment_bins[(*(*segment_map).header).bin]
                == get_segment_index(area, segment_map)
        );
        (*(*area).control).segment_bins[(*(*segment_map).header).bin] = (*(*segment_map).header).next;
    }
    if (*(*segment_map).header).next != DSA_SEGMENT_INDEX_NONE {
        let next: *mut dsa_segment_map;

        next = get_segment_by_index(area, (*(*segment_map).header).next);
        (*(*next).header).prev = (*(*segment_map).header).prev;
    }
}

// Find a segment that could satisfy a request for 'npages' of contiguous
// memory, or return NULL if none can be found.
unsafe fn get_best_segment(area: *mut dsa_area, npages: Size) -> *mut dsa_segment_map {
    let mut bin: Size;

    Assert!(LWLockHeldByMe(DSA_AREA_LOCK(area)));
    check_for_freed_segments_locked(area);

    // Start searching from the first bin that *might* have enough contiguous
    // pages.
    bin = contiguous_pages_to_segment_bin(npages);
    while bin < DSA_NUM_SEGMENT_BINS {
        // The minimum contiguous size that any segment in this bin should have.
        let threshold: Size = (1 as Size) << (bin - 1);
        let mut segment_index: dsa_segment_index;

        // Search this bin for a segment with enough contiguous space.
        segment_index = (*(*area).control).segment_bins[bin];
        while segment_index != DSA_SEGMENT_INDEX_NONE {
            let segment_map: *mut dsa_segment_map;
            let next_segment_index: dsa_segment_index;
            let contiguous_pages: Size;

            segment_map = get_segment_by_index(area, segment_index);
            next_segment_index = (*(*segment_map).header).next;
            contiguous_pages = fpm_largest((*segment_map).fpm);

            // Not enough for the request, still enough for this bin.
            if contiguous_pages >= threshold && contiguous_pages < npages {
                segment_index = next_segment_index;
                continue;
            }

            // Re-bin it if it's no longer in the appropriate bin.
            if contiguous_pages < threshold {
                rebin_segment(area, segment_map);

                // But fall through to see if it's enough to satisfy this request
                // anyway....
            }

            // Check if we are done.
            if contiguous_pages >= npages {
                return segment_map;
            }

            // Continue searching the same bin.
            segment_index = next_segment_index;
        }

        bin += 1;
    }

    // Not found.
    core::ptr::null_mut()
}

// Create a new segment that can handle at least requested_pages.
unsafe fn make_new_segment(area: *mut dsa_area, requested_pages: Size) -> *mut dsa_segment_map {
    let mut new_index: dsa_segment_index;
    let mut metadata_bytes: Size;
    let mut total_size: Size;
    let total_pages: Size;
    let mut usable_pages: Size;
    let segment_map: *mut dsa_segment_map;
    let segment: *mut dsm_segment;
    let oldowner: ResourceOwner;

    Assert!(LWLockHeldByMe(DSA_AREA_LOCK(area)));

    // Find a segment slot that is not in use (linearly for now).
    new_index = 1;
    while new_index < DSA_MAX_SEGMENTS {
        if (*(*area).control).segment_handles[new_index] == DSM_HANDLE_INVALID {
            break;
        }
        new_index += 1;
    }
    if new_index == DSA_MAX_SEGMENTS {
        return core::ptr::null_mut();
    }

    // If the total size limit is already exceeded, then we exit early and avoid
    // arithmetic wraparound in the unsigned expressions below.
    if (*(*area).control).total_segment_size >= (*(*area).control).max_total_segment_size {
        return core::ptr::null_mut();
    }

    // The size should be at least as big as requested, and at least big enough
    // to follow a geometric series that approximately doubles the total storage.
    total_size = (*(*area).control).init_segment_size
        * ((1 as Size) << (new_index / DSA_NUM_SEGMENTS_AT_EACH_SIZE));
    total_size = Min(total_size, (*(*area).control).max_segment_size);
    total_size = Min(
        total_size,
        (*(*area).control).max_total_segment_size - (*(*area).control).total_segment_size,
    );

    total_pages = total_size / FPM_PAGE_SIZE;
    metadata_bytes = MAXALIGN(core::mem::size_of::<dsa_segment_header>())
        + MAXALIGN(core::mem::size_of::<FreePageManager>())
        + core::mem::size_of::<dsa_pointer>() * total_pages;

    // Add padding up to next page boundary.
    if metadata_bytes % FPM_PAGE_SIZE != 0 {
        metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
    }
    if total_size <= metadata_bytes {
        return core::ptr::null_mut();
    }
    usable_pages = (total_size - metadata_bytes) / FPM_PAGE_SIZE;
    Assert!(metadata_bytes + usable_pages * FPM_PAGE_SIZE <= total_size);

    // See if that is enough...
    if requested_pages > usable_pages {
        // We'll make an odd-sized segment, working forward from the requested
        // number of pages.
        usable_pages = requested_pages;
        metadata_bytes = MAXALIGN(core::mem::size_of::<dsa_segment_header>())
            + MAXALIGN(core::mem::size_of::<FreePageManager>())
            + usable_pages * core::mem::size_of::<dsa_pointer>();

        // Add padding up to next page boundary.
        if metadata_bytes % FPM_PAGE_SIZE != 0 {
            metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
        }
        total_size = metadata_bytes + usable_pages * FPM_PAGE_SIZE;

        // Is that too large for dsa_pointer's addressing scheme?
        if total_size > DSA_MAX_SEGMENT_SIZE {
            return core::ptr::null_mut();
        }

        // Would that exceed the limit?
        if total_size
            > (*(*area).control).max_total_segment_size - (*(*area).control).total_segment_size
        {
            return core::ptr::null_mut();
        }
    }

    // Create the segment.
    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = (*area).resowner;
    segment = dsm_create(total_size, 0);
    CurrentResourceOwner = oldowner;
    if segment.is_null() {
        return core::ptr::null_mut();
    }
    dsm_pin_segment(segment);

    // Store the handle in shared memory to be found by index.
    (*(*area).control).segment_handles[new_index] = dsm_segment_handle(segment);
    // Track the highest segment index in the history of the area.
    if (*(*area).control).high_segment_index < new_index {
        (*(*area).control).high_segment_index = new_index;
    }
    // Track the highest segment index this backend has ever mapped.
    if (*area).high_segment_index < new_index {
        (*area).high_segment_index = new_index;
    }
    // Track total size of all segments.
    (*(*area).control).total_segment_size += total_size;
    Assert!((*(*area).control).total_segment_size <= (*(*area).control).max_total_segment_size);

    // Build a segment map for this segment in this backend.
    segment_map = &mut (*area).segment_maps[new_index];
    (*segment_map).segment = segment;
    (*segment_map).mapped_address = dsm_segment_address(segment) as *mut c_char;
    (*segment_map).header = (*segment_map).mapped_address as *mut dsa_segment_header;
    (*segment_map).fpm = (*segment_map)
        .mapped_address
        .add(MAXALIGN(core::mem::size_of::<dsa_segment_header>())) as *mut FreePageManager;
    (*segment_map).pagemap = (*segment_map).mapped_address.add(
        MAXALIGN(core::mem::size_of::<dsa_segment_header>())
            + MAXALIGN(core::mem::size_of::<FreePageManager>()),
    ) as *mut dsa_pointer;

    // Set up the free page map.
    FreePageManagerInitialize((*segment_map).fpm, (*segment_map).mapped_address);
    FreePageManagerPut((*segment_map).fpm, metadata_bytes / FPM_PAGE_SIZE, usable_pages);

    // Set up the segment header and put it in the appropriate bin.
    (*(*segment_map).header).magic =
        DSA_SEGMENT_HEADER_MAGIC ^ (*(*area).control).handle ^ new_index as uint32;
    (*(*segment_map).header).usable_pages = usable_pages;
    (*(*segment_map).header).size = total_size;
    (*(*segment_map).header).bin = contiguous_pages_to_segment_bin(usable_pages);
    (*(*segment_map).header).prev = DSA_SEGMENT_INDEX_NONE;
    (*(*segment_map).header).next =
        (*(*area).control).segment_bins[(*(*segment_map).header).bin];
    (*(*segment_map).header).freed = false;
    (*(*area).control).segment_bins[(*(*segment_map).header).bin] = new_index;
    if (*(*segment_map).header).next != DSA_SEGMENT_INDEX_NONE {
        let next: *mut dsa_segment_map = get_segment_by_index(area, (*(*segment_map).header).next);

        Assert!((*(*next).header).bin == (*(*segment_map).header).bin);
        (*(*next).header).prev = new_index;
    }

    segment_map
}

// Check if any segments have been freed by destroy_superblock, so we can detach
// from them in this backend.
unsafe fn check_for_freed_segments(area: *mut dsa_area) {
    let freed_segment_counter: Size;

    // Any other process that has freed a segment has incremented
    // freed_segment_counter while holding an LWLock.
    pg_read_barrier();
    freed_segment_counter = (*(*area).control).freed_segment_counter;
    if unlikely((*area).freed_segment_counter != freed_segment_counter) {
        // Check all currently mapped segments to find what's been freed.
        LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
        check_for_freed_segments_locked(area);
        LWLockRelease(DSA_AREA_LOCK(area));
    }
}

// Workhorse for check_for_freed_segments(), and also used directly in path
// where the area lock is already held.
unsafe fn check_for_freed_segments_locked(area: *mut dsa_area) {
    let freed_segment_counter: Size;
    let mut i: c_int;

    Assert!(LWLockHeldByMe(DSA_AREA_LOCK(area)));
    freed_segment_counter = (*(*area).control).freed_segment_counter;
    if unlikely((*area).freed_segment_counter != freed_segment_counter) {
        i = 0;
        while i <= (*area).high_segment_index as c_int {
            if !(*area).segment_maps[i as usize].header.is_null()
                && (*(*area).segment_maps[i as usize].header).freed
            {
                dsm_detach((*area).segment_maps[i as usize].segment);
                (*area).segment_maps[i as usize].segment = core::ptr::null_mut();
                (*area).segment_maps[i as usize].header = core::ptr::null_mut();
                (*area).segment_maps[i as usize].mapped_address = core::ptr::null_mut();
            }
            i += 1;
        }
        (*area).freed_segment_counter = freed_segment_counter;
    }
}

// Re-bin segment if it's no longer in the appropriate bin.
unsafe fn rebin_segment(area: *mut dsa_area, segment_map: *mut dsa_segment_map) {
    let new_bin: Size;
    let segment_index: dsa_segment_index;

    new_bin = contiguous_pages_to_segment_bin(fpm_largest((*segment_map).fpm));
    if (*(*segment_map).header).bin == new_bin {
        return;
    }

    // Remove it from its current bin.
    unlink_segment(area, segment_map);

    // Push it onto the front of its new bin.
    segment_index = get_segment_index(area, segment_map);
    (*(*segment_map).header).prev = DSA_SEGMENT_INDEX_NONE;
    (*(*segment_map).header).next = (*(*area).control).segment_bins[new_bin];
    (*(*segment_map).header).bin = new_bin;
    (*(*area).control).segment_bins[new_bin] = segment_index;
    if (*(*segment_map).header).next != DSA_SEGMENT_INDEX_NONE {
        let next: *mut dsa_segment_map;

        next = get_segment_by_index(area, (*(*segment_map).header).next);
        Assert!((*(*next).header).bin == new_bin);
        (*(*next).header).prev = segment_index;
    }
}
