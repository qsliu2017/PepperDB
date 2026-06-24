//! Translated from PostgreSQL src/include/storage/fsm_internals.h
//!
//! Internal functions for the free space map.

use crate::c::MAXALIGN;
use crate::pg_config::BLCKSZ;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{Page, PageMut, SizeOfPageHeaderData};

/// Structure of an FSM page (on-disk). `fp_nodes` is a trailing flexible array
/// (the binary tree stored as an array); only the fixed header is a field here.
#[repr(C)]
pub struct FSMPageData {
    /// next slot to return, round-robin; int (updated without exclusive lock)
    pub fp_next_slot: i32,
    // fp_nodes: [u8; FLEXIBLE_ARRAY_MEMBER] follows; access via slice accessor.
}

const _: () = assert!(core::mem::offset_of!(FSMPageData, fp_next_slot) == 0);
/// offsetof(FSMPageData, fp_nodes): the fixed header size before the node array.
pub const SIZE_OF_FSM_PAGE_DATA: usize = core::mem::size_of::<FSMPageData>();

/// Number of nodes in total on an FSM page (internal to fsmpage.c).
pub const NODES_PER_PAGE: usize =
    (BLCKSZ as usize) - MAXALIGN(SizeOfPageHeaderData) - SIZE_OF_FSM_PAGE_DATA;

pub const NON_LEAF_NODES_PER_PAGE: usize = (BLCKSZ as usize) / 2 - 1;
pub const LEAF_NODES_PER_PAGE: usize = NODES_PER_PAGE - NON_LEAF_NODES_PER_PAGE;

/// Number of FSM "slots" on a FSM page (use outside fsmpage.c).
pub const SLOTS_PER_FSM_PAGE: usize = LEAF_NODES_PER_PAGE;

/// Returns a slot with at least `minvalue` free space, or None if none. (C
/// returns -1 when not found.)
pub fn fsm_search_avail(
    _buf: Buffer,
    _minvalue: u8,
    _advancenext: bool,
    _exclusive_lock_held: bool,
) -> Option<i32> {
    unimplemented!()
}
pub fn fsm_get_avail(_page: Page, _slot: i32) -> u8 {
    unimplemented!()
}
pub fn fsm_get_max_avail(_page: Page) -> u8 {
    unimplemented!()
}
pub fn fsm_set_avail(_page: PageMut, _slot: i32, _value: u8) -> bool {
    unimplemented!()
}
pub fn fsm_truncate_avail(_page: PageMut, _nslots: i32) -> bool {
    unimplemented!()
}
pub fn fsm_rebuild_page(_page: PageMut) -> bool {
    unimplemented!()
}
