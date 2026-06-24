//! Translated from PostgreSQL src/include/access/visibilitymapdefs.h

use bitflags::bitflags;

/// Number of bits for one heap page.
pub const BITS_PER_HEAPBLOCK: u32 = 2;

bitflags! {
    /// Visibility map page flag bits (also used in xl_heap_visible.flags).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct VisibilityMapFlags: u8 {
        const ALL_VISIBLE = 0x01;
        const ALL_FROZEN  = 0x02;
        /// OR of all valid visibilitymap flag bits.
        const VALID_BITS  = Self::ALL_VISIBLE.bits() | Self::ALL_FROZEN.bits();
        /// Extra xl_heap_visible bit: table is a user catalog table.
        /// NB: may not be passed to visibilitymap_set().
        const XLOG_CATALOG_REL = 0x04;
        /// OR of all bits valid in xl_heap_visible.flags.
        const XLOG_VALID_BITS = Self::VALID_BITS.bits() | Self::XLOG_CATALOG_REL.bits();
    }
}
