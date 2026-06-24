//! Translated from PostgreSQL src/include/access/bufmask.h
//!
//! Buffer masking routines: mask bits in a page that can differ between WAL
//! generation and WAL apply, used for WAL consistency checking. A `Page` is a
//! mutable byte slice here (`crate::storage::bufpage::PageMut`).

use crate::storage::bufpage::PageMut;

/// Marker used to mask pages consistently.
pub const MASK_MARKER: u8 = 0;

pub fn mask_page_lsn_and_checksum(page: PageMut) {
    let _ = page;
    unimplemented!()
}

pub fn mask_page_hint_bits(page: PageMut) {
    let _ = page;
    unimplemented!()
}

pub fn mask_unused_space(page: PageMut) {
    let _ = page;
    unimplemented!()
}

pub fn mask_lp_flags(page: PageMut) {
    let _ = page;
    unimplemented!()
}

pub fn mask_page_content(page: PageMut) {
    let _ = page;
    unimplemented!()
}
