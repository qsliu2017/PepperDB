//! Translated from PostgreSQL src/include/access/brin_page.h
//! Prototypes and definitions for BRIN page layouts. On-disk page structs.

use bitflags::bitflags;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;

/// Special area of BRIN pages. Sized so it occupies the last MAXALIGN element of
/// each page; on the targets MAXALIGN is 8, so the vector is 4 uint16s.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BrinSpecialSpace {
    pub vector: [u16; 8 / core::mem::size_of::<u16>()],
}

const _: () = assert!(core::mem::size_of::<BrinSpecialSpace>() == 8);

// Special space on all BRIN pages stores a "type" identifier (last half-word).
pub const BRIN_PAGETYPE_META: u16 = 0xF091;
pub const BRIN_PAGETYPE_REVMAP: u16 = 0xF092;
pub const BRIN_PAGETYPE_REGULAR: u16 = 0xF093;

bitflags! {
    /// Flags for BrinSpecialSpace (clean single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BrinEvacuate: u16 {
        const EVACUATE_PAGE = 1 << 0;
    }
}

impl BrinSpecialSpace {
    /// Page type: last half-word in the special space (for pg_filedump etc.).
    pub const fn page_type(&self) -> u16 {
        self.vector[self.vector.len() - 1]
    }
    /// Flags: second-to-last half-word.
    pub const fn flags(&self) -> u16 {
        self.vector[self.vector.len() - 2]
    }
    pub const fn is_meta(&self) -> bool {
        self.page_type() == BRIN_PAGETYPE_META
    }
    pub const fn is_revmap(&self) -> bool {
        self.page_type() == BRIN_PAGETYPE_REVMAP
    }
    pub const fn is_regular(&self) -> bool {
        self.page_type() == BRIN_PAGETYPE_REGULAR
    }
}

/// BRIN metapage layout. On-disk.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BrinMetaPageData {
    pub brinMagic: u32,
    pub brinVersion: u32,
    pub pagesPerRange: BlockNumber,
    pub lastRevmapPage: BlockNumber,
}

const _: () = assert!(core::mem::size_of::<BrinMetaPageData>() == 16);

pub const BRIN_CURRENT_VERSION: u32 = 1;
pub const BRIN_META_MAGIC: u32 = 0xA8109CFA;
pub const BRIN_METAPAGE_BLKNO: BlockNumber = 0;

/// Revmap page contents: an array of TIDs filling all available page space. The
/// C struct declares a single-element array as a stand-in for a flexible array
/// member; the real length is computed from page geometry (REVMAP_PAGE_MAXITEMS).
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct RevmapContents {
    pub rm_tids: [ItemPointerData; 1],
}
