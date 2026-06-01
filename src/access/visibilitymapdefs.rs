//! access/visibilitymapdefs.h - macros for accessing contents of visibility map pages

use crate::c::uint8;

/// Number of bits for one heap page
pub const BITS_PER_HEAPBLOCK: uint8 = 2;

/* Flags for bit map */
pub const VISIBILITYMAP_ALL_VISIBLE: uint8 = 0x01;
pub const VISIBILITYMAP_ALL_FROZEN: uint8 = 0x02;
/// OR of all valid visibilitymap flags bits
pub const VISIBILITYMAP_VALID_BITS: uint8 = 0x03;

/*
 * To detect recovery conflicts during logical decoding on a standby, we need
 * to know if a table is a user catalog table. For that we add an additional
 * bit into xl_heap_visible.flags, in addition to the above.
 *
 * NB: VISIBILITYMAP_XLOG_* may not be passed to visibilitymap_set().
 */
pub const VISIBILITYMAP_XLOG_CATALOG_REL: uint8 = 0x04;
pub const VISIBILITYMAP_XLOG_VALID_BITS: uint8 =
    VISIBILITYMAP_VALID_BITS | VISIBILITYMAP_XLOG_CATALOG_REL;
