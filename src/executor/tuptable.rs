//! Translated from PostgreSQL src/include/executor/tuptable.h

use bitflags::bitflags;

use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::access::htup_details::MinimalTupleData;
use crate::access::tupdesc::TupleDesc;
use crate::access::sysattr::{SELF_ITEM_POINTER_ATTRIBUTE_NUMBER, TABLE_OID_ATTRIBUTE_NUMBER};
use crate::postgres::{Datum, ObjectIdGetDatum, PointerGetDatum};
use crate::postgres_ext::Oid;
use crate::storage::buf::Buffer;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::palloc::MemoryContext;

// C: `typedef MinimalTupleData *MinimalTuple`. htup_details defines the value
// struct; the pointer handle is not aliased there, so define it here.
pub type MinimalTuple = *mut MinimalTupleData; // TODO(ptr)

bitflags! {
    /// TTS_FLAG_* boolean states (`flags`). GOOD: clean single-bit set.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TtsFlags: u16 {
        const EMPTY      = 1 << 1;  // slot is empty
        const SHOULDFREE = 1 << 2;  // should pfree tuple "owned" by the slot
        const SLOW       = 1 << 3;  // saved state for slot_deform_heap_tuple
        const FIXED      = 1 << 4;  // fixed tuple descriptor
    }
}

/// Base tuple table slot type. In-memory: no layout contract.
pub struct TupleTableSlot {
    pub flags: TtsFlags,           // Boolean states
    pub nvalid: i16,               // # of valid values in values
    pub ops: &'static dyn TupleTableSlotOps, // implementation of slot; TODO(ptr)
    pub tupleDescriptor: TupleDesc, // slot's tuple descriptor
    pub values: *mut Datum,        // current per-attribute values; TODO(ptr)
    pub isnull: *mut bool,         // current per-attribute isnull flags; TODO(ptr)
    pub mcxt: MemoryContext,       // slot itself is in this context
    pub tid: ItemPointerData,      // stored tuple's tid
    pub tableOid: Oid,             // table oid of tuple
}

/// Routines for a TupleTableSlot implementation (was `TupleTableSlotOps`,
/// a struct of fn pointers). routine-struct group C: per-instance behaviour
/// table. `get_heap_tuple`/`get_minimal_tuple` are set NULL when the slot
/// cannot own that tuple form -> Option-returning default methods (None).
pub trait TupleTableSlotOps {
    /// Minimum size of the slot (C `base_slot_size`).
    fn base_slot_size(&self) -> usize;

    fn init(&self, slot: &mut TupleTableSlot);
    fn release(&self, slot: &mut TupleTableSlot);
    fn clear(&self, slot: &mut TupleTableSlot);
    fn getsomeattrs(&self, slot: &mut TupleTableSlot, natts: i32);

    /// System attribute as a datum; None where C set `*isnull = true`.
    fn getsysattr(&self, slot: &mut TupleTableSlot, attnum: i32) -> Option<Datum>;

    fn is_current_xact_tuple(&self, slot: &TupleTableSlot) -> bool;
    fn materialize(&self, slot: &mut TupleTableSlot);
    fn copyslot(&self, dstslot: &mut TupleTableSlot, srcslot: &TupleTableSlot);

    /// Heap tuple owned by the slot; None if the slot can't own one (NULL cb).
    fn get_heap_tuple(&self, _slot: &mut TupleTableSlot) -> Option<HeapTuple> {
        None
    }

    /// Minimal tuple owned by the slot; None if the slot can't own one.
    fn get_minimal_tuple(&self, _slot: &mut TupleTableSlot) -> Option<MinimalTuple> {
        None
    }

    fn copy_heap_tuple(&self, slot: &mut TupleTableSlot) -> HeapTuple;
    fn copy_minimal_tuple(&self, slot: &mut TupleTableSlot, extra: usize) -> MinimalTuple;
}

// Predefined ops singletons identify the slot type. Concrete impls live in
// execTuples.c (Phase 2); declared here as the builtin set.
// extern const TupleTableSlotOps TTSOps{Virtual,HeapTuple,MinimalTuple,BufferHeapTuple}

/// Virtual slot: Datum/isnull arrays are authoritative.
pub struct VirtualTupleTableSlot {
    pub base: TupleTableSlot,
    pub data: *mut u8, // data for materialized slots; TODO(ptr)
}

pub struct HeapTupleTableSlot {
    pub base: TupleTableSlot,
    pub tuple: HeapTuple,      // physical tuple
    pub off: u32,             // saved state for slot_deform_heap_tuple
    pub tupdata: HeapTupleData, // optional workspace for storing tuple
}

/// Heap tuple residing in a buffer.
pub struct BufferHeapTupleTableSlot {
    pub base: HeapTupleTableSlot,
    pub buffer: Buffer, // tuple's buffer, or InvalidBuffer
}

pub struct MinimalTupleTableSlot {
    pub base: TupleTableSlot,
    pub tuple: HeapTuple,       // tuple wrapper
    pub mintuple: MinimalTuple, // minimal tuple, or NULL if none
    pub minhdr: HeapTupleData,  // workspace for minimal-tuple-only case
    pub off: u32,              // saved state for slot_deform_heap_tuple
}

pub const FIELDNO_TUPLETABLESLOT_FLAGS: usize = 1;
pub const FIELDNO_TUPLETABLESLOT_NVALID: usize = 2;
pub const FIELDNO_TUPLETABLESLOT_TUPLEDESCRIPTOR: usize = 4;
pub const FIELDNO_TUPLETABLESLOT_VALUES: usize = 5;
pub const FIELDNO_TUPLETABLESLOT_ISNULL: usize = 6;
pub const FIELDNO_HEAPTUPLETABLESLOT_TUPLE: usize = 1;
pub const FIELDNO_HEAPTUPLETABLESLOT_OFF: usize = 2;
pub const FIELDNO_MINIMALTUPLETABLESLOT_TUPLE: usize = 1;
pub const FIELDNO_MINIMALTUPLETABLESLOT_OFF: usize = 4;

/// TTS_EMPTY(slot)
pub fn tts_empty(slot: &TupleTableSlot) -> bool {
    slot.flags.contains(TtsFlags::EMPTY)
}

/// TTS_SHOULDFREE(slot)
pub fn tts_shouldfree(slot: &TupleTableSlot) -> bool {
    slot.flags.contains(TtsFlags::SHOULDFREE)
}

/// TTS_SLOW(slot)
pub fn tts_slow(slot: &TupleTableSlot) -> bool {
    slot.flags.contains(TtsFlags::SLOW)
}

/// TTS_FIXED(slot)
pub fn tts_fixed(slot: &TupleTableSlot) -> bool {
    slot.flags.contains(TtsFlags::FIXED)
}

/// TupIsNull -- is a TupleTableSlot empty? (None or empty)
pub fn tup_is_null(slot: Option<&TupleTableSlot>) -> bool {
    slot.is_none_or(tts_empty)
}

// === in executor/execTuples.c ===

pub fn MakeTupleTableSlot(
    _tupleDesc: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecAllocTableSlot(
    _tupleTable: &mut Vec<*mut TupleTableSlot>,
    _desc: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecResetTupleTable(_tupleTable: &mut Vec<*mut TupleTableSlot>, _shouldFree: bool) {
    unimplemented!()
}

pub fn MakeSingleTupleTableSlot(
    _tupdesc: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecDropSingleTupleTableSlot(_slot: &mut TupleTableSlot) {
    unimplemented!()
}

pub fn ExecSetSlotDescriptor(_slot: &mut TupleTableSlot, _tupdesc: TupleDesc) {
    unimplemented!()
}

pub fn ExecStoreHeapTuple(
    _tuple: HeapTuple,
    _slot: &mut TupleTableSlot,
    _shouldFree: bool,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecForceStoreHeapTuple(_tuple: HeapTuple, _slot: &mut TupleTableSlot, _shouldFree: bool) {
    unimplemented!()
}

pub fn ExecStoreBufferHeapTuple(
    _tuple: HeapTuple,
    _slot: &mut TupleTableSlot,
    _buffer: Buffer,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecStorePinnedBufferHeapTuple(
    _tuple: HeapTuple,
    _slot: &mut TupleTableSlot,
    _buffer: Buffer,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecStoreMinimalTuple(
    _mtup: MinimalTuple,
    _slot: &mut TupleTableSlot,
    _shouldFree: bool,
) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecForceStoreMinimalTuple(_mtup: MinimalTuple, _slot: &mut TupleTableSlot, _shouldFree: bool) {
    unimplemented!()
}

pub fn ExecStoreVirtualTuple(_slot: &mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecStoreAllNullTuple(_slot: &mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!()
}

pub fn ExecStoreHeapTupleDatum(_data: Datum, _slot: &mut TupleTableSlot) {
    unimplemented!()
}

/// ExecFetchSlotHeapTuple: returns the tuple and whether the caller should free.
pub fn ExecFetchSlotHeapTuple(
    _slot: &mut TupleTableSlot,
    _materialize: bool,
) -> (HeapTuple, bool) {
    unimplemented!()
}

/// ExecFetchSlotMinimalTuple: returns the tuple and whether the caller should free.
pub fn ExecFetchSlotMinimalTuple(_slot: &mut TupleTableSlot) -> (MinimalTuple, bool) {
    unimplemented!()
}

pub fn ExecFetchSlotHeapTupleDatum(_slot: &mut TupleTableSlot) -> Datum {
    unimplemented!()
}

pub fn slot_getmissingattrs(_slot: &mut TupleTableSlot, _startAttNum: i32, _lastAttNum: i32) {
    unimplemented!()
}

pub fn slot_getsomeattrs_int(_slot: &mut TupleTableSlot, _attnum: i32) {
    unimplemented!()
}

// === inline accessors (translated in full) ===

/// Force slot's Datum/isnull arrays valid up through `attnum`.
pub fn slot_getsomeattrs(slot: &mut TupleTableSlot, attnum: i32) {
    if (slot.nvalid as i32) < attnum {
        slot_getsomeattrs_int(slot, attnum);
    }
}

/// Force all entries of the slot's Datum/isnull arrays valid.
pub fn slot_getallattrs(slot: &mut TupleTableSlot) {
    let natts = unsafe { (*slot.tupleDescriptor).natts };
    slot_getsomeattrs(slot, natts);
}

/// Detect whether an attribute of the slot is null, without fetching it.
pub fn slot_attisnull(slot: &mut TupleTableSlot, attnum: i32) -> bool {
    debug_assert!(attnum > 0);

    if attnum > slot.nvalid as i32 {
        slot_getsomeattrs(slot, attnum);
    }

    unsafe { *slot.isnull.add((attnum - 1) as usize) }
}

/// Fetch one attribute of the slot's contents. None == SQL NULL (folds isnull).
pub fn slot_getattr(slot: &mut TupleTableSlot, attnum: i32) -> Option<Datum> {
    debug_assert!(attnum > 0);

    if attnum > slot.nvalid as i32 {
        slot_getsomeattrs(slot, attnum);
    }

    let idx = (attnum - 1) as usize;
    unsafe {
        if *slot.isnull.add(idx) {
            None
        } else {
            Some(*slot.values.add(idx))
        }
    }
}

/// Fetch a system attribute of the slot's current tuple. None == SQL NULL.
pub fn slot_getsysattr(slot: &mut TupleTableSlot, attnum: i32) -> Option<Datum> {
    debug_assert!(attnum < 0); // caller error

    if attnum == TABLE_OID_ATTRIBUTE_NUMBER as i32 {
        return Some(ObjectIdGetDatum(slot.tableOid));
    } else if attnum == SELF_ITEM_POINTER_ATTRIBUTE_NUMBER as i32 {
        return Some(PointerGetDatum(
            (&slot.tid as *const ItemPointerData).cast(),
        ));
    }

    let ops = slot.ops;
    ops.getsysattr(slot, attnum)
}

/// slot_is_current_xact_tuple
pub fn slot_is_current_xact_tuple(slot: &TupleTableSlot) -> bool {
    let ops = slot.ops;
    ops.is_current_xact_tuple(slot)
}

/// ExecClearTuple - clear the slot's contents.
pub fn ExecClearTuple(slot: &mut TupleTableSlot) {
    let ops = slot.ops;
    ops.clear(slot);
}

/// ExecMaterializeSlot - force a slot into the "materialized" state.
pub fn ExecMaterializeSlot(slot: &mut TupleTableSlot) {
    let ops = slot.ops;
    ops.materialize(slot);
}

/// ExecCopySlotHeapTuple - return HeapTuple allocated in caller's context.
pub fn ExecCopySlotHeapTuple(slot: &mut TupleTableSlot) -> HeapTuple {
    debug_assert!(!tts_empty(slot));
    let ops = slot.ops;
    ops.copy_heap_tuple(slot)
}

/// ExecCopySlotMinimalTuple - return MinimalTuple allocated in caller's context.
pub fn ExecCopySlotMinimalTuple(slot: &mut TupleTableSlot) -> MinimalTuple {
    let ops = slot.ops;
    ops.copy_minimal_tuple(slot, 0)
}

/// ExecCopySlotMinimalTupleExtra - like above, with extra leading bytes.
pub fn ExecCopySlotMinimalTupleExtra(slot: &mut TupleTableSlot, extra: usize) -> MinimalTuple {
    let ops = slot.ops;
    ops.copy_minimal_tuple(slot, extra)
}

/// ExecCopySlot - copy one slot's contents into another.
pub fn ExecCopySlot<'a>(
    dstslot: &'a mut TupleTableSlot,
    srcslot: &TupleTableSlot,
) -> &'a mut TupleTableSlot {
    debug_assert!(!tts_empty(srcslot));
    let ops = dstslot.ops;
    ops.copyslot(dstslot, srcslot);
    dstslot
}
