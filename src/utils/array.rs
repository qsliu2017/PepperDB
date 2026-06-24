//! Translated from PostgreSQL src/include/utils/array.h

use crate::c::{bits8, MAXALIGN};
use crate::fmgr::FmgrInfo;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::expandeddatum::ExpandedObjectHeader;
use crate::utils::palloc::MemoryContext;

/// Maximum number of array subscripts (arbitrary limit).
pub const MAXDIM: usize = 6;

// MaxAllocSize lives in utils/memutils.h (= 0x3fffffff, "1 gigabyte - 1").
const MAX_ALLOC_SIZE: usize = 0x3fffffff;
/// Max elements in an array (~quarter billion; bounds Datum-array palloc).
pub const MaxArraySize: usize = MAX_ALLOC_SIZE / core::mem::size_of::<Datum>();

/// On-disk varlena array header. The dimensions/lower-bounds/null-bitmap/data
/// tail follows in the same buffer (see ARR_* accessors). Layout must not change.
#[repr(C)]
pub struct ArrayType {
    pub vl_len_: i32,     // varlena header -- use VARSIZE/SET_VARSIZE
    pub ndim: i32,        // # of dimensions
    pub dataoffset: i32,  // offset to data, or 0 if no bitmap
    pub elemtype: Oid,    // element type OID
}
const _: () = assert!(core::mem::size_of::<ArrayType>() == 16);
const _: () = assert!(core::mem::offset_of!(ArrayType, elemtype) == 12);

/// ID for debugging crosschecks.
pub const EA_MAGIC: i32 = 689375833;

/// Expanded (deconstructed) array, living in a private memory context. In-memory.
pub struct ExpandedArrayHeader {
    pub hdr: ExpandedObjectHeader,
    pub ea_magic: i32,

    pub ndims: i32,
    pub dims: Vec<i32>,   // array dimensions
    pub lbound: Vec<i32>, // index lower bounds per dimension

    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,

    pub dvalues: Option<Vec<Datum>>, // Datum representation, if built
    pub dnulls: Option<Vec<bool>>,   // is-null flags, if any nulls
    pub dvalueslen: i32,
    pub nelems: i32,

    pub flat_size: usize, // current flat-equivalent size, or 0 if unknown

    pub fvalue: Option<Box<ArrayType>>, // fully detoasted flat array, if valid
    pub fstartptr: *mut u8,             // start of flat data area; TODO(ptr)
    pub fendptr: *mut u8,               // end+1 of flat data area; TODO(ptr)
}

/// Either a flat varlena array or an expanded one. The C union is a tagged
/// access pattern (VARATT_IS_EXPANDED_HEADER); model as an enum.
pub enum AnyArrayType {
    Flat(ArrayType),
    Expanded(ExpandedArrayHeader),
}

/// Working state for accumArrayResult() and friends (scalar inputs). In-memory.
pub struct ArrayBuildState {
    pub mcontext: MemoryContext,
    pub dvalues: Vec<Datum>,
    pub dnulls: Vec<bool>,
    pub alen: i32,
    pub nelems: i32,
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub private_cxt: bool,
}

/// Working state for accumArrayResultArr() and friends (array inputs).
pub struct ArrayBuildStateArr {
    pub mcontext: MemoryContext,
    pub data: Vec<u8>,
    pub nullbitmap: Option<Vec<bits8>>,
    pub abytes: i32,
    pub nbytes: i32,
    pub aitems: i32,
    pub nitems: i32,
    pub ndims: i32,
    pub dims: [i32; MAXDIM],
    pub lbs: [i32; MAXDIM],
    pub array_type: Oid,
    pub element_type: Oid,
    pub private_cxt: bool,
}

/// Working state handling either scalar or array inputs (exactly one is set).
pub enum ArrayBuildStateAny {
    Scalar(Box<ArrayBuildState>),
    Array(Box<ArrayBuildStateArr>),
}

/// Cached type metadata for array manipulation.
pub struct ArrayMetaState {
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typdelim: u8,
    pub typioparam: Oid,
    pub typiofunc: Oid,
    pub proc: FmgrInfo,
}

/// Private state for array_map (caller-provided).
pub struct ArrayMapState {
    pub inp_extra: ArrayMetaState,
    pub ret_extra: ArrayMetaState,
}

// ArrayIteratorData is private in arrayfuncs.c; opaque handle.
pub struct ArrayIteratorData {
    _private: [u8; 0],
}
pub type ArrayIterator = *mut ArrayIteratorData; // TODO(ptr)

impl ArrayType {
    pub fn arr_size(&self) -> usize {
        self.vl_len_ as usize // VARSIZE
    }
    pub fn arr_ndim(&self) -> i32 {
        self.ndim
    }
    pub fn arr_hasnull(&self) -> bool {
        self.dataoffset != 0
    }
    pub fn arr_elemtype(&self) -> Oid {
        self.elemtype
    }

    /// ARR_DIMS: dimensions array, immediately after the fixed header.
    /// SAFETY: `self` points into an array buffer of its recorded length.
    pub fn arr_dims(&self) -> &[i32] {
        let base = (self as *const Self).cast::<u8>();
        unsafe {
            core::slice::from_raw_parts(
                base.add(core::mem::size_of::<ArrayType>()).cast::<i32>(),
                self.ndim as usize,
            )
        }
    }

    /// ARR_LBOUND: lower-bounds array, after the dimensions array.
    /// SAFETY: as arr_dims.
    pub fn arr_lbound(&self) -> &[i32] {
        let base = (self as *const Self).cast::<u8>();
        let off = core::mem::size_of::<ArrayType>()
            + core::mem::size_of::<i32>() * self.ndim as usize;
        unsafe {
            core::slice::from_raw_parts(base.add(off).cast::<i32>(), self.ndim as usize)
        }
    }

    /// ARR_DATA_OFFSET: byte offset to the actual array data.
    pub fn arr_data_offset(&self) -> usize {
        if self.arr_hasnull() {
            self.dataoffset as usize
        } else {
            Self::arr_overhead_nonulls(self.ndim as usize)
        }
    }

    /// ARR_OVERHEAD_NONULLS: total header size with no null bitmap.
    pub fn arr_overhead_nonulls(ndims: usize) -> usize {
        MAXALIGN(core::mem::size_of::<ArrayType>() + 2 * core::mem::size_of::<i32>() * ndims)
    }

    /// ARR_OVERHEAD_WITHNULLS: total header size including a null bitmap.
    pub fn arr_overhead_withnulls(ndims: usize, nitems: usize) -> usize {
        MAXALIGN(
            core::mem::size_of::<ArrayType>()
                + 2 * core::mem::size_of::<i32>() * ndims
                + (nitems + 7) / 8,
        )
    }
}

// GUC parameter.
pub static mut Array_nulls: bool = false;

// arrayfuncs.c -------------------------------------------------------------

pub fn CopyArrayEls(
    _array: &mut ArrayType,
    _values: &[Datum],
    _nulls: &[bool],
    _nitems: i32,
    _typlen: i32,
    _typbyval: bool,
    _typalign: u8,
    _freedata: bool,
) {
    unimplemented!()
}

// array_get_element / array_ref: out-param isNull -> Option<Datum>.
pub fn array_get_element(
    _arraydatum: Datum,
    _indx: &[i32],
    _arraytyplen: i32,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> Option<Datum> {
    unimplemented!()
}

pub fn array_set_element(
    _arraydatum: Datum,
    _indx: &[i32],
    _data_value: Datum,
    _isnull: bool,
    _arraytyplen: i32,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> Datum {
    unimplemented!()
}

pub fn array_get_slice(
    _arraydatum: Datum,
    _upper_indx: &[i32],
    _lower_indx: &[i32],
    _upper_provided: &[bool],
    _lower_provided: &[bool],
    _arraytyplen: i32,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> Datum {
    unimplemented!()
}

pub fn array_set_slice(
    _arraydatum: Datum,
    _upper_indx: &[i32],
    _lower_indx: &[i32],
    _upper_provided: &[bool],
    _lower_provided: &[bool],
    _src_array_datum: Datum,
    _isnull: bool,
    _arraytyplen: i32,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> Datum {
    unimplemented!()
}

pub fn array_ref(
    _array: &ArrayType,
    _indx: &[i32],
    _arraytyplen: i32,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> Option<Datum> {
    unimplemented!()
}

pub fn array_set(
    _array: &mut ArrayType,
    _indx: &[i32],
    _data_value: Datum,
    _isnull: bool,
    _arraytyplen: i32,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> *mut ArrayType {
    unimplemented!()
}

// array_map takes planner ExprState/ExprContext; out of scope for the skeleton.
pub fn array_map(_arrayd: Datum, _ret_type: Oid, _amstate: &mut ArrayMapState) -> Datum {
    unimplemented!()
}

pub fn array_bitmap_copy(
    _destbitmap: &mut [bits8],
    _destoffset: i32,
    _srcbitmap: &[bits8],
    _srcoffset: i32,
    _nitems: i32,
) {
    unimplemented!()
}

pub fn construct_array(
    _elems: &[Datum],
    _elmtype: Oid,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> *mut ArrayType {
    unimplemented!()
}

pub fn construct_array_builtin(_elems: &[Datum], _elmtype: Oid) -> *mut ArrayType {
    unimplemented!()
}

pub fn construct_md_array(
    _elems: &[Datum],
    _nulls: &[bool],
    _ndims: i32,
    _dims: &[i32],
    _lbs: &[i32],
    _elmtype: Oid,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> *mut ArrayType {
    unimplemented!()
}

pub fn construct_empty_array(_elmtype: Oid) -> *mut ArrayType {
    unimplemented!()
}

pub fn construct_empty_expanded_array(
    _element_type: Oid,
    _parentcontext: MemoryContext,
    _metacache: &mut ArrayMetaState,
) -> *mut ExpandedArrayHeader {
    unimplemented!()
}

// deconstruct_array: out-params (elems, nulls, nelems) -> tuple.
pub fn deconstruct_array(
    _array: &ArrayType,
    _elmtype: Oid,
    _elmlen: i32,
    _elmbyval: bool,
    _elmalign: u8,
) -> (Vec<Datum>, Vec<bool>, i32) {
    unimplemented!()
}

pub fn deconstruct_array_builtin(
    _array: &ArrayType,
    _elmtype: Oid,
) -> (Vec<Datum>, Vec<bool>, i32) {
    unimplemented!()
}

pub fn array_contains_nulls(_array: &ArrayType) -> bool {
    unimplemented!()
}

pub fn initArrayResult(
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
) -> *mut ArrayBuildState {
    unimplemented!()
}

pub fn initArrayResultWithSize(
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
    _initsize: i32,
) -> *mut ArrayBuildState {
    unimplemented!()
}

pub fn accumArrayResult(
    _astate: *mut ArrayBuildState,
    _dvalue: Datum,
    _disnull: bool,
    _element_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildState {
    unimplemented!()
}

pub fn makeArrayResult(_astate: &mut ArrayBuildState, _rcontext: MemoryContext) -> Datum {
    unimplemented!()
}

pub fn makeMdArrayResult(
    _astate: &mut ArrayBuildState,
    _ndims: i32,
    _dims: &[i32],
    _lbs: &[i32],
    _rcontext: MemoryContext,
    _release: bool,
) -> Datum {
    unimplemented!()
}

pub fn initArrayResultArr(
    _array_type: Oid,
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
) -> *mut ArrayBuildStateArr {
    unimplemented!()
}

pub fn accumArrayResultArr(
    _astate: *mut ArrayBuildStateArr,
    _dvalue: Datum,
    _disnull: bool,
    _array_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildStateArr {
    unimplemented!()
}

pub fn makeArrayResultArr(
    _astate: &mut ArrayBuildStateArr,
    _rcontext: MemoryContext,
    _release: bool,
) -> Datum {
    unimplemented!()
}

pub fn initArrayResultAny(
    _input_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
) -> *mut ArrayBuildStateAny {
    unimplemented!()
}

pub fn accumArrayResultAny(
    _astate: *mut ArrayBuildStateAny,
    _dvalue: Datum,
    _disnull: bool,
    _input_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildStateAny {
    unimplemented!()
}

pub fn makeArrayResultAny(
    _astate: &mut ArrayBuildStateAny,
    _rcontext: MemoryContext,
    _release: bool,
) -> Datum {
    unimplemented!()
}

pub fn array_create_iterator(
    _arr: &mut ArrayType,
    _slice_ndim: i32,
    _mstate: Option<&mut ArrayMetaState>,
) -> ArrayIterator {
    unimplemented!()
}

// array_iterate: out-params (value, isnull) -> Option<Option<Datum>>;
// outer None = iterator exhausted (the C bool), inner None = SQL NULL element.
pub fn array_iterate(_iterator: ArrayIterator) -> Option<Option<Datum>> {
    unimplemented!()
}

pub fn array_free_iterator(_iterator: ArrayIterator) {
    unimplemented!()
}

// arrayutils.c -------------------------------------------------------------

pub fn ArrayGetOffset(_n: i32, _dim: &[i32], _lb: &[i32], _indx: &[i32]) -> i32 {
    unimplemented!()
}

pub fn ArrayGetNItems(_ndim: i32, _dims: &[i32]) -> i32 {
    unimplemented!()
}

// _safe variant: escontext soft error -> Option.
pub fn ArrayGetNItemsSafe(_ndim: i32, _dims: &[i32]) -> Option<i32> {
    unimplemented!()
}

pub fn ArrayCheckBounds(_ndim: i32, _dims: &[i32], _lb: &[i32]) {
    unimplemented!()
}

pub fn ArrayCheckBoundsSafe(_ndim: i32, _dims: &[i32], _lb: &[i32]) -> bool {
    unimplemented!()
}

// mda_get_range / mda_get_offset_values: fill caller-provided out slices.
pub fn mda_get_range(_n: i32, _span: &mut [i32], _st: &[i32], _endp: &[i32]) {
    unimplemented!()
}

pub fn mda_get_prod(_n: i32, _range: &[i32], _prod: &mut [i32]) {
    unimplemented!()
}

pub fn mda_get_offset_values(_n: i32, _dist: &mut [i32], _prod: &[i32], _span: &[i32]) {
    unimplemented!()
}

pub fn mda_next_tuple(_n: i32, _curr: &mut [i32], _span: &[i32]) -> i32 {
    unimplemented!()
}

// out-param n -> tuple (slice + count).
pub fn ArrayGetIntegerTypmods(_arr: &ArrayType) -> (Vec<i32>, i32) {
    unimplemented!()
}

// array_expanded.c ---------------------------------------------------------

pub fn expand_array(
    _arraydatum: Datum,
    _parentcontext: MemoryContext,
    _metacache: Option<&mut ArrayMetaState>,
) -> Datum {
    unimplemented!()
}

pub fn DatumGetExpandedArray(_d: Datum) -> *mut ExpandedArrayHeader {
    unimplemented!()
}

pub fn DatumGetExpandedArrayX(
    _d: Datum,
    _metacache: &mut ArrayMetaState,
) -> *mut ExpandedArrayHeader {
    unimplemented!()
}

pub fn DatumGetAnyArrayP(_d: Datum) -> *mut AnyArrayType {
    unimplemented!()
}

pub fn deconstruct_expanded_array(_eah: &mut ExpandedArrayHeader) {
    unimplemented!()
}
