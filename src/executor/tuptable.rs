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
///
/// The C `tts_values`/`tts_isnull` flexible arrays (palloc'd in the same block
/// as the slot, length `tupleDescriptor->natts`) become owned `Vec`s here: there
/// is no layout contract, nothing outside this file pointer-walked them, and an
/// owning `Vec` removes the raw-pointer aliasing the C model relied on.
#[derive(Clone)]
pub struct TupleTableSlot {
    pub flags: TtsFlags,           // Boolean states
    pub nvalid: i16,               // # of valid values in values
    pub ops: &'static dyn TupleTableSlotOps, // implementation of slot; TODO(ptr)
    pub tupleDescriptor: Option<TupleDesc>, // slot's tuple descriptor (None if unset)
    pub values: Vec<Datum>,        // current per-attribute values (C tts_values[])
    pub isnull: Vec<bool>,         // current per-attribute isnull flags (C tts_isnull[])
    pub mcxt: (),                  // slot's owning context; tombstoned in this port (no live MemoryContext handle), keeps the slot genuinely Send
    pub tid: ItemPointerData,      // stored tuple's tid
    pub tableOid: Oid,             // table oid of tuple
}

/// Routines for a TupleTableSlot implementation (was `TupleTableSlotOps`,
/// a struct of fn pointers). routine-struct group C: per-instance behaviour
/// table. `get_heap_tuple`/`get_minimal_tuple` are set NULL when the slot
/// cannot own that tuple form -> Option-returning default methods (None).
///
/// `Send + Sync`: the impls are stateless zero-sized vtables (TTSOpsVirtual/
/// HeapTuple/MinimalTuple/BufferHeapTuple), so a `&'static dyn TupleTableSlotOps`
/// is `Send` (requires `dyn: Sync`), keeping `TupleTableSlot` genuinely `Send`.
pub trait TupleTableSlotOps: Send + Sync {
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
// execTuples.c; declared here as the builtin set. Step 08 lands the virtual one;
// the heap/minimal/buffer ops grow with heapam.
// extern const TupleTableSlotOps TTSOps{Virtual,HeapTuple,MinimalTuple,BufferHeapTuple}

/// PG `TTSOpsVirtual`: the `&'static dyn TupleTableSlotOps` for virtual slots.
pub use crate::backend::executor::execTuples::TTS_OPS_VIRTUAL as TTSOpsVirtual;

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
// Slots are owned `Box<TupleTableSlot>` in this port (the C single-palloc block
// becomes one allocation owning its value/null Vecs); `ExecAllocTableSlot`
// returns the slot's index in the tuple table rather than a raw pointer.

pub use crate::backend::executor::execTuples::exec_alloc_table_slot as ExecAllocTableSlot;
pub use crate::backend::executor::execTuples::exec_reset_tuple_table as ExecResetTupleTable;
pub use crate::backend::executor::execTuples::make_single_tuple_table_slot as MakeSingleTupleTableSlot;
pub use crate::backend::executor::execTuples::make_tuple_table_slot as MakeTupleTableSlot;

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

/// PG `ExecStoreVirtualTuple`. The C return-the-slot chaining is dropped (the
/// caller already holds the slot); see the backend body.
pub use crate::backend::executor::execTuples::exec_store_virtual_tuple as ExecStoreVirtualTuple;
pub use crate::backend::executor::execTuples::exec_store_all_null_tuple as ExecStoreAllNullTuple;

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
    if i32::from(slot.nvalid) < attnum {
        slot_getsomeattrs_int(slot, attnum);
    }
}

/// Force all entries of the slot's Datum/isnull arrays valid.
pub fn slot_getallattrs(slot: &mut TupleTableSlot) {
    let natts = slot
        .tupleDescriptor
        .as_ref()
        .map_or(0, |d| d.natts);
    slot_getsomeattrs(slot, natts);
}

/// Detect whether an attribute of the slot is null, without fetching it.
pub fn slot_attisnull(slot: &mut TupleTableSlot, attnum: i32) -> bool {
    debug_assert!(attnum > 0);

    if attnum > i32::from(slot.nvalid) {
        slot_getsomeattrs(slot, attnum);
    }

    slot.isnull[(attnum - 1) as usize]
}

/// Fetch one attribute of the slot's contents. None == SQL NULL (folds isnull).
pub fn slot_getattr(slot: &mut TupleTableSlot, attnum: i32) -> Option<Datum> {
    debug_assert!(attnum > 0);

    if attnum > i32::from(slot.nvalid) {
        slot_getsomeattrs(slot, attnum);
    }

    let idx = (attnum - 1) as usize;
    if slot.isnull[idx] {
        None
    } else {
        Some(slot.values[idx])
    }
}

/// Convenience: map a `slot_getattr` result to `Option<i32>` (None == SQL NULL).
/// Folds the common int4-column read used by the sort/group/limit node tests.
#[must_use]
pub fn DatumGetInt32_opt(d: Option<Datum>) -> Option<i32> {
    d.map(crate::postgres::DatumGetInt32)
}

/// Fetch a system attribute of the slot's current tuple. None == SQL NULL.
pub fn slot_getsysattr(slot: &mut TupleTableSlot, attnum: i32) -> Option<Datum> {
    debug_assert!(attnum < 0); // caller error

    if attnum == i32::from(TABLE_OID_ATTRIBUTE_NUMBER) {
        return Some(ObjectIdGetDatum(slot.tableOid));
    } else if attnum == i32::from(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER) {
        return Some(PointerGetDatum(
            (&raw const slot.tid).cast(),
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

