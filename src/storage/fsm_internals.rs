//! Translated from PostgreSQL src/include/storage/fsm_internals.h
//!
//! Internal functions for the free space map.

use crate::c::MAXALIGN;
use crate::pg_config::BLCKSZ;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};

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

// The page-content logic lives in `backend::storage::freespace::fsmpage` as
// methods on the `FsmPage` / `FsmPageMut` views (rules.md 3). The C-named free
// functions below are `#[deprecated]` shims; new code uses the methods directly.

use crate::backend::storage::freespace::fsmpage::{FsmPage, FsmPageMut};

/// C: `fsm_search_avail`. In C this takes a `Buffer`; here the page-content
/// search is [`FsmPageMut::search_avail`]. The `Buffer`-based form cannot resolve
/// the page without the buffer pool handle, so callers in freespace.rs hold the
/// page directly; this shim is retained only for cross-reference.
#[deprecated(note = "use `FsmPageMut::new(page).search_avail(minvalue, advancenext)`")]
pub fn fsm_search_avail(
    _buf: Buffer,
    _minvalue: u8,
    _advancenext: bool,
    _exclusive_lock_held: bool,
) -> Option<i32> {
    unimplemented!("use FsmPageMut::search_avail on the locked page")
}

/// C: `fsm_get_avail`. Use [`FsmPage::get_avail`].
#[deprecated(note = "use `FsmPage::new(page).get_avail(slot)`")]
pub fn fsm_get_avail(page: &Page, slot: i32) -> u8 {
    FsmPage::new(page).get_avail(slot as usize)
}

/// C: `fsm_get_max_avail`. Use [`FsmPage::get_max_avail`].
#[deprecated(note = "use `FsmPage::new(page).get_max_avail()`")]
pub fn fsm_get_max_avail(page: &Page) -> u8 {
    FsmPage::new(page).get_max_avail()
}

/// C: `fsm_set_avail`. Use [`FsmPageMut::set_avail`].
#[deprecated(note = "use `FsmPageMut::new(page).set_avail(slot, value)`")]
pub fn fsm_set_avail(page: &mut Page, slot: i32, value: u8) -> bool {
    FsmPageMut::new(page).set_avail(slot as usize, value)
}

/// C: `fsm_truncate_avail`. Use [`FsmPageMut::truncate_avail`].
#[deprecated(note = "use `FsmPageMut::new(page).truncate_avail(nslots)`")]
pub fn fsm_truncate_avail(page: &mut Page, nslots: i32) -> bool {
    FsmPageMut::new(page).truncate_avail(nslots as usize)
}

/// C: `fsm_rebuild_page`. Use [`FsmPageMut::rebuild_page`].
#[deprecated(note = "use `FsmPageMut::new(page).rebuild_page()`")]
pub fn fsm_rebuild_page(page: &mut Page) -> bool {
    FsmPageMut::new(page).rebuild_page()
}
