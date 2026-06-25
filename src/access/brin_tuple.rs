//! Translated from PostgreSQL src/include/access/brin_tuple.h
//! Declarations for dealing with BRIN-specific tuples.

use crate::access::attnum::AttrNumber;
use crate::access::brin_internal::BrinDesc;
use crate::postgres::Datum;
use crate::storage::block::BlockNumber;
use crate::utils::memutils::MemoryContext;

/// BRIN opclasses may register a serialization callback when the on-disk and
/// in-memory representations differ. C: `void (*)(BrinDesc*, Datum, Datum*)`.
pub type brin_serialize_callback_type = fn(bdesc: &mut BrinDesc, src: Datum, dst: &mut Datum);

/// One BrinValues per indexed column within a BRIN index tuple (in-memory).
pub struct BrinValues {
    /// index attribute number
    pub attno: AttrNumber,
    /// are there any nulls in the page range?
    pub hasnulls: bool,
    /// are all values nulls in the page range?
    pub allnulls: bool,
    /// current accumulated values
    pub values: *mut Datum, // TODO(ptr)
    /// expanded accumulated values
    pub mem_value: Datum,
    pub context: MemoryContext,
    pub serialize: Option<brin_serialize_callback_type>,
}

/// In-memory BRIN index tuple; decodable only with an appropriate BrinDesc.
/// C trailing `columns[FLEXIBLE_ARRAY_MEMBER]` becomes a `Vec` (in-memory FAM).
pub struct BrinMemTuple {
    /// this is a placeholder tuple
    pub placeholder: bool,
    /// range represents no tuples
    pub empty_range: bool,
    /// heap blkno that the tuple is for
    pub blkno: BlockNumber,
    /// memcxt holding the columns values
    pub context: MemoryContext,
    /// values array (output for brin_deform_tuple)
    pub values: *mut Datum, // TODO(ptr)
    /// allnulls array
    pub allnulls: *mut bool, // TODO(ptr)
    /// hasnulls array
    pub hasnulls: *mut bool, // TODO(ptr)
    /// per-column values (C FAM tail)
    pub columns: Vec<BrinValues>,
}

/// info bit layout (on-disk): bit 7 has-nulls, bit 6 placeholder,
/// bit 5 empty-range, bits 4-0 data offset. It packs an offset number beside
/// the flags, so it stays a raw byte with accessor methods (not bitflags).
pub const BRIN_OFFSET_MASK: u8 = 0x1F;
pub const BRIN_EMPTY_RANGE_MASK: u8 = 0x20;
pub const BRIN_PLACEHOLDER_MASK: u8 = 0x40;
pub const BRIN_NULLS_MASK: u8 = 0x80;

/// On-disk BRIN index tuple header. Followed by an optional nulls bitmask
/// (two bits per indexed column) and opclass-defined Datum values per column.
#[repr(C)]
pub struct BrinTuple {
    /// heap block number that the tuple is for
    pub blkno: BlockNumber,
    /// see BRIN_*_MASK for info bit layout
    pub info: u8,
}

const _: () = assert!(core::mem::offset_of!(BrinTuple, info) == 4);

/// C: `SizeOfBrinTuple = offsetof(BrinTuple, info) + sizeof(uint8)`.
pub const SizeOfBrinTuple: usize = core::mem::offset_of!(BrinTuple, info) + size_of::<u8>();

impl BrinTuple {
    /// C: `BrinTupleDataOffset` - offset of data past the header/null bitmask.
    #[inline]
    pub const fn data_offset(&self) -> usize {
        (self.info & BRIN_OFFSET_MASK) as usize
    }
    /// C: `BrinTupleHasNulls`.
    #[inline]
    pub const fn has_nulls(&self) -> bool {
        self.info & BRIN_NULLS_MASK != 0
    }
    /// C: `BrinTupleIsPlaceholder`.
    #[inline]
    pub const fn is_placeholder(&self) -> bool {
        self.info & BRIN_PLACEHOLDER_MASK != 0
    }
    /// C: `BrinTupleIsEmptyRange`.
    #[inline]
    pub const fn is_empty_range(&self) -> bool {
        self.info & BRIN_EMPTY_RANGE_MASK != 0
    }
}

pub fn brin_form_tuple(
    _brdesc: &mut BrinDesc,
    _blkno: BlockNumber,
    _tuple: &mut BrinMemTuple,
    _size: &mut usize,
) -> *mut BrinTuple {
    unimplemented!()
}

pub fn brin_form_placeholder_tuple(
    _brdesc: &mut BrinDesc,
    _blkno: BlockNumber,
    _size: &mut usize,
) -> *mut BrinTuple {
    unimplemented!()
}

pub fn brin_free_tuple(_tuple: &mut BrinTuple) {
    unimplemented!()
}

pub fn brin_copy_tuple(
    _tuple: &mut BrinTuple,
    _len: usize,
    _dest: &mut BrinTuple,
    _destsz: &mut usize,
) -> *mut BrinTuple {
    unimplemented!()
}

pub fn brin_tuples_equal(_a: &BrinTuple, _alen: usize, _b: &BrinTuple, _blen: usize) -> bool {
    unimplemented!()
}

pub fn brin_new_memtuple(_brdesc: &mut BrinDesc) -> *mut BrinMemTuple {
    unimplemented!()
}

pub fn brin_memtuple_initialize(
    _dtuple: &mut BrinMemTuple,
    _brdesc: &mut BrinDesc,
) -> *mut BrinMemTuple {
    unimplemented!()
}

pub fn brin_deform_tuple(
    _brdesc: &mut BrinDesc,
    _tuple: &mut BrinTuple,
    _d_memtuple: &mut BrinMemTuple,
) -> *mut BrinMemTuple {
    unimplemented!()
}
