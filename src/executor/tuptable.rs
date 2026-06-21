//! Translation of postgres/src/include/executor/tuptable.h
//!
//! Tuple table support: the TupleTableSlot row abstraction over the various
//! physical/virtual tuple representations the executor passes around, the
//! TupleTableSlotOps vtable, the four concrete slot types, the TTS_FLAG_*
//! bits and their predicate macros, and the `static inline` accessors.
//!
//! The execTuples.c implementation (slot_getsomeattrs_int, the TTSOps* static
//! vtables, ExecStore*/ExecFetch*/MakeTupleTableSlot/etc.) lives in a separate
//! file, crate::executor::execTuples, which is referenced here for the inline
//! wrappers and forward-declared below.
//!
//! #include mapping:
//!   access/htup.h          -> (HeapTuple machinery) crate::access::htup_details
//!   access/htup_details.h  -> crate::access::htup_details {HeapTuple, HeapTupleData,
//!                             MinimalTuple}
//!   access/sysattr.h       -> crate::access::sysattr {TableOidAttributeNumber,
//!                             SelfItemPointerAttributeNumber}
//!   access/tupdesc.h       -> crate::access::common::tupdesc {TupleDesc, TupleDescData}
//!   storage/buf.h          -> STUB: Buffer (storage/buf.h not yet ported); a local
//!                             `pub type Buffer = c_int;` is defined below with a TODO.
//!
//! Cross-module notes:
//!   - NodeTag from crate::nodes::nodes.  T_TupleTableSlot is NOT yet present in
//!     the NodeTag enum (it sits between the executor *State tags and the
//!     replication command tags); the integrator must add it.  See tests / the
//!     MISSING SYMBOLS note in the porting summary.
//!   - The bodies that need execTuples.c symbols call through
//!     crate::executor::execTuples::NAME so wiring is automatic once that module
//!     lands.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::nodes::nodes::NodeTag;
use crate::nodes::primnodes::AttrNumber;
use crate::nodes::pg_list::List;

use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::{HeapTuple, HeapTupleData, MinimalTuple};
use crate::access::sysattr::{SelfItemPointerAttributeNumber, TableOidAttributeNumber};

use crate::storage::itemptr::ItemPointerData;

// storage/buf.h is not yet ported.  In C, Buffer is `typedef int Buffer;`.
// TODO(pg-port): replace with crate::storage::buf::Buffer once storage/buf.h lands.
pub type Buffer = c_int;

// ----------------------------------------------------------------------------
// TTS_FLAG_* bits + predicate macros (header lines: `#define TTS_FLAG_* (1<<n)`).
// ----------------------------------------------------------------------------

/// true = slot is empty
pub const TTS_FLAG_EMPTY: uint16 = 1 << 1;

/// should pfree tuple "owned" by the slot?
pub const TTS_FLAG_SHOULDFREE: uint16 = 1 << 2;

/// saved state for slot_deform_heap_tuple
pub const TTS_FLAG_SLOW: uint16 = 1 << 3;

/// fixed tuple descriptor
pub const TTS_FLAG_FIXED: uint16 = 1 << 4;

/// `TTS_EMPTY(slot)` -- is the EMPTY flag set?
#[inline]
pub unsafe fn TTS_EMPTY(slot: *const TupleTableSlot) -> bool {
    ((*slot).tts_flags & TTS_FLAG_EMPTY) != 0
}

/// `TTS_SHOULDFREE(slot)` -- does the slot own (and should it pfree) the tuple?
#[inline]
pub unsafe fn TTS_SHOULDFREE(slot: *const TupleTableSlot) -> bool {
    ((*slot).tts_flags & TTS_FLAG_SHOULDFREE) != 0
}

/// `TTS_SLOW(slot)` -- slot_deform_heap_tuple saved state flag.
#[inline]
pub unsafe fn TTS_SLOW(slot: *const TupleTableSlot) -> bool {
    ((*slot).tts_flags & TTS_FLAG_SLOW) != 0
}

/// `TTS_FIXED(slot)` -- fixed tuple descriptor flag.
#[inline]
pub unsafe fn TTS_FIXED(slot: *const TupleTableSlot) -> bool {
    ((*slot).tts_flags & TTS_FLAG_FIXED) != 0
}

// ----------------------------------------------------------------------------
// base tuple table slot type
// ----------------------------------------------------------------------------

/// base tuple table slot type
#[repr(C)]
pub struct TupleTableSlot {
    pub r#type: NodeTag,
    /* FIELDNO_TUPLETABLESLOT_FLAGS 1 */
    /// Boolean states
    pub tts_flags: uint16,
    /* FIELDNO_TUPLETABLESLOT_NVALID 2 */
    /// # of valid values in tts_values
    pub tts_nvalid: AttrNumber,
    /// implementation of slot
    //
    // C: `const TupleTableSlotOps *const tts_ops` -- a const pointer to a const
    // vtable; modeled here as a plain `*const TupleTableSlotOps`.
    pub tts_ops: *const TupleTableSlotOps,
    /* FIELDNO_TUPLETABLESLOT_TUPLEDESCRIPTOR 4 */
    /// slot's tuple descriptor
    pub tts_tupleDescriptor: TupleDesc,
    /* FIELDNO_TUPLETABLESLOT_VALUES 5 */
    /// current per-attribute values
    pub tts_values: *mut Datum,
    /* FIELDNO_TUPLETABLESLOT_ISNULL 6 */
    /// current per-attribute isnull flags
    pub tts_isnull: *mut bool,
    /// slot itself is in this context
    pub tts_mcxt: MemoryContext,
    /// stored tuple's tid
    pub tts_tid: ItemPointerData,
    /// table oid of tuple
    pub tts_tableOid: Oid,
}

// Field-number macros for tts fields, used by JIT/expression compilation in C.
pub const FIELDNO_TUPLETABLESLOT_FLAGS: usize = 1;
pub const FIELDNO_TUPLETABLESLOT_NVALID: usize = 2;
pub const FIELDNO_TUPLETABLESLOT_TUPLEDESCRIPTOR: usize = 4;
pub const FIELDNO_TUPLETABLESLOT_VALUES: usize = 5;
pub const FIELDNO_TUPLETABLESLOT_ISNULL: usize = 6;

// ----------------------------------------------------------------------------
// routines for a TupleTableSlot implementation (the vtable)
//
// NOTE: the fn-pointer fields are modeled as `Option<unsafe fn(...)>` to match
// the sibling convention in nodes/execnodes.rs (e.g. ExecProcNodeMtd,
// ExprStateEvalFunc), since this is an internal pure-Rust port with no FFI ABI
// boundary.  Field ORDER is exact -- execTuples.rs builds the static TTSOps*
// instances positionally.
// ----------------------------------------------------------------------------

/// routines for a TupleTableSlot implementation
#[repr(C)]
pub struct TupleTableSlotOps {
    /// Minimum size of the slot
    pub base_slot_size: Size,

    /// Initialization.
    pub init: Option<unsafe fn(slot: *mut TupleTableSlot)>,

    /// Destruction.
    pub release: Option<unsafe fn(slot: *mut TupleTableSlot)>,

    /// Clear the contents of the slot. Only the contents are expected to be
    /// cleared and not the tuple descriptor. Typically frees the memory
    /// allocated for the tuple contained in the slot.
    pub clear: Option<unsafe fn(slot: *mut TupleTableSlot)>,

    /// Fill up first natts entries of tts_values and tts_isnull arrays with
    /// values from the tuple contained in the slot. May be called with natts
    /// more than the number of attributes available in the tuple, in which case
    /// it should set tts_nvalid to the number of returned columns.
    pub getsomeattrs: Option<unsafe fn(slot: *mut TupleTableSlot, natts: c_int)>,

    /// Returns value of the given system attribute as a datum and sets isnull
    /// to false, if it's not NULL. Throws an error if the slot type does not
    /// support system attributes.
    pub getsysattr:
        Option<unsafe fn(slot: *mut TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum>,

    /// Check if the tuple is created by the current transaction. Throws an
    /// error if the slot doesn't contain the storage tuple.
    pub is_current_xact_tuple: Option<unsafe fn(slot: *mut TupleTableSlot) -> bool>,

    /// Make the contents of the slot solely depend on the slot, and not on
    /// underlying resources (like another memory context, buffers, etc).
    pub materialize: Option<unsafe fn(slot: *mut TupleTableSlot)>,

    /// Copy the contents of the source slot into the destination slot's own
    /// context. Invoked using callback of the destination slot.  'dstslot' and
    /// 'srcslot' can be assumed to have the same number of attributes.
    pub copyslot: Option<unsafe fn(dstslot: *mut TupleTableSlot, srcslot: *mut TupleTableSlot)>,

    /// Return a heap tuple "owned" by the slot. It is slot's responsibility to
    /// free the memory consumed by the heap tuple. If the slot can not "own" a
    /// heap tuple, it should not implement this callback and should set it NULL.
    pub get_heap_tuple: Option<unsafe fn(slot: *mut TupleTableSlot) -> HeapTuple>,

    /// Return a minimal tuple "owned" by the slot. It is slot's responsibility
    /// to free the memory consumed by the minimal tuple. If the slot can not
    /// "own" a minimal tuple, it should not implement this callback and should
    /// set it NULL.
    pub get_minimal_tuple: Option<unsafe fn(slot: *mut TupleTableSlot) -> MinimalTuple>,

    /// Return a copy of heap tuple representing the contents of the slot. The
    /// copy needs to be palloc'd in the current memory context. The slot itself
    /// is expected to remain unaffected. Not expected to have meaningful "system
    /// columns" in the copy. The copy is not "owned" by the slot.
    pub copy_heap_tuple: Option<unsafe fn(slot: *mut TupleTableSlot) -> HeapTuple>,

    /// Return a copy of minimal tuple representing the contents of the slot. The
    /// copy needs to be palloc'd in the current memory context. The slot itself
    /// is expected to remain unaffected. Not expected to have meaningful "system
    /// columns" in the copy. The copy is not "owned" by the slot.
    ///
    /// The copy has "extra" bytes (maxaligned and zeroed) available before the
    /// tuple, useful so callers may store extra data along with the minimal
    /// tuple without an additional allocation.
    pub copy_minimal_tuple:
        Option<unsafe fn(slot: *mut TupleTableSlot, extra: Size) -> MinimalTuple>,
}

// ----------------------------------------------------------------------------
// Predefined TupleTableSlotOps for various types of slots.  The same are used
// to identify the type of a given slot.
//
// These const statics are *defined* in execTuples.c; forward-reference them as
// items in crate::executor::execTuples and base TTS_IS_* on those addresses.
// Until execTuples lands, TTS_IS_* are TODO stubs (see below).
// ----------------------------------------------------------------------------
//
// extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVirtual;
// extern PGDLLIMPORT const TupleTableSlotOps TTSOpsHeapTuple;
// extern PGDLLIMPORT const TupleTableSlotOps TTSOpsMinimalTuple;
// extern PGDLLIMPORT const TupleTableSlotOps TTSOpsBufferHeapTuple;
//   -> crate::executor::execTuples::{TTSOpsVirtual, TTSOpsHeapTuple,
//      TTSOpsMinimalTuple, TTSOpsBufferHeapTuple}

/// `TTS_IS_VIRTUAL(slot)` -- is the slot a virtual slot?
//
// TODO(pg-port): wire to &crate::executor::execTuples::TTSOpsVirtual once
// execTuples.rs defines the static vtables.
#[inline]
pub unsafe fn TTS_IS_VIRTUAL(slot: *const TupleTableSlot) -> bool {
    (*slot).tts_ops == &crate::executor::execTuples::TTSOpsVirtual as *const TupleTableSlotOps
}

/// `TTS_IS_HEAPTUPLE(slot)` -- is the slot a (palloc'd) heap-tuple slot?
#[inline]
pub unsafe fn TTS_IS_HEAPTUPLE(slot: *const TupleTableSlot) -> bool {
    (*slot).tts_ops == &crate::executor::execTuples::TTSOpsHeapTuple as *const TupleTableSlotOps
}

/// `TTS_IS_MINIMALTUPLE(slot)` -- is the slot a minimal-tuple slot?
#[inline]
pub unsafe fn TTS_IS_MINIMALTUPLE(slot: *const TupleTableSlot) -> bool {
    (*slot).tts_ops == &crate::executor::execTuples::TTSOpsMinimalTuple as *const TupleTableSlotOps
}

/// `TTS_IS_BUFFERTUPLE(slot)` -- is the slot a buffer-resident heap-tuple slot?
#[inline]
pub unsafe fn TTS_IS_BUFFERTUPLE(slot: *const TupleTableSlot) -> bool {
    (*slot).tts_ops
        == &crate::executor::execTuples::TTSOpsBufferHeapTuple as *const TupleTableSlotOps
}

// ----------------------------------------------------------------------------
// Tuple table slot implementations.
//
// In C each carries `pg_node_attr(abstract)` (a codegen annotation, no runtime
// effect) and embeds the base slot as the first field.
// ----------------------------------------------------------------------------

/// Virtual slot: Datum/isnull arrays are authoritative; `data` holds the
/// materialized backing store (when materialized).
#[repr(C)]
pub struct VirtualTupleTableSlot {
    pub base: TupleTableSlot,
    /// data for materialized slots
    pub data: *mut c_char,
}

/// Slot holding a palloc'd physical heap tuple.
#[repr(C)]
pub struct HeapTupleTableSlot {
    pub base: TupleTableSlot,
    /* FIELDNO_HEAPTUPLETABLESLOT_TUPLE 1 */
    /// physical tuple
    pub tuple: HeapTuple,
    /* FIELDNO_HEAPTUPLETABLESLOT_OFF 2 */
    /// saved state for slot_deform_heap_tuple
    pub off: uint32,
    /// optional workspace for storing tuple
    pub tupdata: HeapTupleData,
}

pub const FIELDNO_HEAPTUPLETABLESLOT_TUPLE: usize = 1;
pub const FIELDNO_HEAPTUPLETABLESLOT_OFF: usize = 2;

/// heap tuple residing in a buffer.  Note: the base is a HeapTupleTableSlot
/// (not the bare TupleTableSlot) in C.
#[repr(C)]
pub struct BufferHeapTupleTableSlot {
    pub base: HeapTupleTableSlot,

    /// If buffer is not InvalidBuffer, then the slot is holding a pin on the
    /// indicated buffer page; drop the pin when we release the slot's reference
    /// to that buffer.  (TTS_FLAG_SHOULDFREE should not be set in such a case,
    /// since presumably base.tuple is pointing into the buffer.)
    ///
    /// tuple's buffer, or InvalidBuffer
    pub buffer: Buffer,
}

/// Slot holding a "minimal" tuple.  `tuple` points at `minhdr`, whose fields
/// are set for access to the minimal tuple; in particular minhdr.t_data points
/// MINIMAL_TUPLE_OFFSET bytes before mintuple, so column extraction treats it
/// identically to regular physical tuples.
#[repr(C)]
pub struct MinimalTupleTableSlot {
    pub base: TupleTableSlot,
    /* FIELDNO_MINIMALTUPLETABLESLOT_TUPLE 1 */
    /// tuple wrapper
    pub tuple: HeapTuple,
    /// minimal tuple, or NULL if none
    pub mintuple: MinimalTuple,
    /// workspace for minimal-tuple-only case
    pub minhdr: HeapTupleData,
    /* FIELDNO_MINIMALTUPLETABLESLOT_OFF 4 */
    /// saved state for slot_deform_heap_tuple
    pub off: uint32,
}

pub const FIELDNO_MINIMALTUPLETABLESLOT_TUPLE: usize = 1;
pub const FIELDNO_MINIMALTUPLETABLESLOT_OFF: usize = 4;

// ----------------------------------------------------------------------------
// TupIsNull -- is a TupleTableSlot empty (or NULL)?
// ----------------------------------------------------------------------------

/// `TupIsNull(slot)` -- the slot pointer is NULL, or the slot is EMPTY.
#[inline]
pub unsafe fn TupIsNull(slot: *const TupleTableSlot) -> bool {
    slot.is_null() || TTS_EMPTY(slot)
}

// ----------------------------------------------------------------------------
// Forward declarations of execTuples.c routines (NOT static inline in the
// header; defined in crate::executor::execTuples).  Listed here for reference:
//
//   MakeTupleTableSlot(tupleDesc, tts_ops) -> *mut TupleTableSlot
//   ExecAllocTableSlot(tupleTable: *mut *mut List, desc, tts_ops) -> *mut TupleTableSlot
//   ExecResetTupleTable(tupleTable: *mut List, shouldFree: bool)
//   MakeSingleTupleTableSlot(tupdesc, tts_ops) -> *mut TupleTableSlot
//   ExecDropSingleTupleTableSlot(slot)
//   ExecSetSlotDescriptor(slot, tupdesc)
//   ExecStoreHeapTuple(tuple, slot, shouldFree) -> *mut TupleTableSlot
//   ExecForceStoreHeapTuple(tuple, slot, shouldFree)
//   ExecStoreBufferHeapTuple(tuple, slot, buffer) -> *mut TupleTableSlot
//   ExecStorePinnedBufferHeapTuple(tuple, slot, buffer) -> *mut TupleTableSlot
//   ExecStoreMinimalTuple(mtup, slot, shouldFree) -> *mut TupleTableSlot
//   ExecForceStoreMinimalTuple(mtup, slot, shouldFree)
//   ExecStoreVirtualTuple(slot) -> *mut TupleTableSlot          (extern, NOT inline)
//   ExecStoreAllNullTuple(slot) -> *mut TupleTableSlot          (extern, NOT inline)
//   ExecStoreHeapTupleDatum(data, slot)
//   ExecFetchSlotHeapTuple(slot, materialize, shouldFree) -> HeapTuple
//   ExecFetchSlotMinimalTuple(slot, shouldFree) -> MinimalTuple
//   ExecFetchSlotHeapTupleDatum(slot) -> Datum
//   slot_getmissingattrs(slot, startAttNum, lastAttNum)
//   slot_getsomeattrs_int(slot, attnum)
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// static inline accessors (header's `#ifndef FRONTEND` block).
// ----------------------------------------------------------------------------

/// This function forces the entries of the slot's Datum/isnull arrays to be
/// valid at least up through the attnum'th entry.
#[inline]
pub unsafe fn slot_getsomeattrs(slot: *mut TupleTableSlot, attnum: c_int) {
    if ((*slot).tts_nvalid as c_int) < attnum {
        crate::executor::execTuples::slot_getsomeattrs_int(slot, attnum);
    }
}

/// slot_getallattrs -- force ALL entries of the slot's Datum/isnull arrays to
/// be valid.  Caller may then extract data directly from those arrays.
#[inline]
#[no_mangle]
pub unsafe fn slot_getallattrs(slot: *mut TupleTableSlot) {
    slot_getsomeattrs(slot, (*(*slot).tts_tupleDescriptor).natts);
}

/// slot_attisnull -- detect whether an attribute of the slot is null, without
/// actually fetching it.
#[inline]
pub unsafe fn slot_attisnull(slot: *mut TupleTableSlot, attnum: c_int) -> bool {
    Assert!(attnum > 0);

    if attnum > (*slot).tts_nvalid as c_int {
        slot_getsomeattrs(slot, attnum);
    }

    *(*slot).tts_isnull.offset((attnum - 1) as isize)
}

/// slot_getattr -- fetch one attribute of the slot's contents.
#[inline]
pub unsafe fn slot_getattr(slot: *mut TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum {
    Assert!(attnum > 0);

    if attnum > (*slot).tts_nvalid as c_int {
        slot_getsomeattrs(slot, attnum);
    }

    *isnull = *(*slot).tts_isnull.offset((attnum - 1) as isize);

    *(*slot).tts_values.offset((attnum - 1) as isize)
}

/// slot_getsysattr -- fetch a system attribute of the slot's current tuple.
///
/// If the slot type does not contain system attributes, this will throw an
/// error.  Callers should ensure the slot type supports system attributes.
#[inline]
pub unsafe fn slot_getsysattr(slot: *mut TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum {
    Assert!(attnum < 0); /* caller error */

    if attnum == TableOidAttributeNumber as c_int {
        *isnull = false;
        return ObjectIdGetDatum((*slot).tts_tableOid);
    } else if attnum == SelfItemPointerAttributeNumber as c_int {
        *isnull = false;
        return PointerGetDatum(&(*slot).tts_tid as *const ItemPointerData as *const c_void);
    }

    /* Fetch the system attribute from the underlying tuple. */
    ((*(*slot).tts_ops).getsysattr.unwrap())(slot, attnum, isnull)
}

/// slot_is_current_xact_tuple -- check if the slot's current tuple is created
/// by the current transaction.
///
/// If the slot does not contain a storage tuple, this will throw an error.
#[inline]
pub unsafe fn slot_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    ((*(*slot).tts_ops).is_current_xact_tuple.unwrap())(slot)
}

/// ExecClearTuple -- clear the slot's contents.
#[inline]
#[no_mangle]
pub unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    ((*(*slot).tts_ops).clear.unwrap())(slot);

    slot
}

/// ExecMaterializeSlot -- force a slot into the "materialized" state.
///
/// This causes the slot's tuple to be a local copy not dependent on any
/// external storage (i.e. pointing into a Buffer, or having allocations in
/// another memory context).
#[inline]
pub unsafe fn ExecMaterializeSlot(slot: *mut TupleTableSlot) {
    ((*(*slot).tts_ops).materialize.unwrap())(slot);
}

/// ExecCopySlotHeapTuple -- return HeapTuple allocated in caller's context.
#[no_mangle]
pub unsafe fn ExecCopySlotHeapTuple(slot: *mut TupleTableSlot) -> HeapTuple {
    Assert!(!TTS_EMPTY(slot));

    ((*(*slot).tts_ops).copy_heap_tuple.unwrap())(slot)
}

/// ExecCopySlotMinimalTuple -- return MinimalTuple allocated in caller's context.
#[inline]
pub unsafe fn ExecCopySlotMinimalTuple(slot: *mut TupleTableSlot) -> MinimalTuple {
    ((*(*slot).tts_ops).copy_minimal_tuple.unwrap())(slot, 0)
}

/// ExecCopySlotMinimalTupleExtra -- return MinimalTuple allocated in caller's
/// context, with `extra` bytes (maxaligned and zeroed) before the tuple for
/// data the caller wishes to store along with the tuple (without an additional
/// allocation).
#[inline]
pub unsafe fn ExecCopySlotMinimalTupleExtra(
    slot: *mut TupleTableSlot,
    extra: Size,
) -> MinimalTuple {
    ((*(*slot).tts_ops).copy_minimal_tuple.unwrap())(slot, extra)
}

/// ExecCopySlot -- copy one slot's contents into another.
///
/// If a source's system attributes are to be accessed in the target slot, the
/// target slot and source slot types need to match.  Currently 'dstslot' and
/// 'srcslot' must have the same number of attributes.
#[inline]
#[no_mangle]
pub unsafe fn ExecCopySlot(
    dstslot: *mut TupleTableSlot,
    srcslot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    Assert!(!TTS_EMPTY(srcslot));
    Assert!(srcslot != dstslot);
    Assert!(
        (*(*dstslot).tts_tupleDescriptor).natts == (*(*srcslot).tts_tupleDescriptor).natts
    );

    ((*(*dstslot).tts_ops).copyslot.unwrap())(dstslot, srcslot);

    dstslot
}

// ----------------------------------------------------------------------------
// Tests: layout / flag-value / TTS_EMPTY sanity.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn tts_flag_values() {
        assert_eq!(TTS_FLAG_EMPTY, 0x0002);
        assert_eq!(TTS_FLAG_SHOULDFREE, 0x0004);
        assert_eq!(TTS_FLAG_SLOW, 0x0008);
        assert_eq!(TTS_FLAG_FIXED, 0x0010);
    }

    #[test]
    fn fieldno_consts() {
        assert_eq!(FIELDNO_TUPLETABLESLOT_FLAGS, 1);
        assert_eq!(FIELDNO_TUPLETABLESLOT_NVALID, 2);
        assert_eq!(FIELDNO_TUPLETABLESLOT_TUPLEDESCRIPTOR, 4);
        assert_eq!(FIELDNO_TUPLETABLESLOT_VALUES, 5);
        assert_eq!(FIELDNO_TUPLETABLESLOT_ISNULL, 6);
        assert_eq!(FIELDNO_HEAPTUPLETABLESLOT_TUPLE, 1);
        assert_eq!(FIELDNO_HEAPTUPLETABLESLOT_OFF, 2);
        assert_eq!(FIELDNO_MINIMALTUPLETABLESLOT_TUPLE, 1);
        assert_eq!(FIELDNO_MINIMALTUPLETABLESLOT_OFF, 4);
    }

    #[test]
    fn slot_layout() {
        // type (NodeTag) is first; tts_flags follows immediately.
        assert_eq!(offset_of!(TupleTableSlot, r#type), 0);
        // tts_flags (uint16) comes right after the NodeTag, and tts_nvalid
        // (AttrNumber = i16) packs adjacent to it before the pointer-aligned
        // tts_ops.
        assert!(offset_of!(TupleTableSlot, tts_flags) < offset_of!(TupleTableSlot, tts_nvalid));
        assert!(offset_of!(TupleTableSlot, tts_nvalid) < offset_of!(TupleTableSlot, tts_ops));
        // Pointer fields are in declared order.
        assert!(offset_of!(TupleTableSlot, tts_ops) < offset_of!(TupleTableSlot, tts_tupleDescriptor));
        assert!(
            offset_of!(TupleTableSlot, tts_tupleDescriptor) < offset_of!(TupleTableSlot, tts_values)
        );
        assert!(offset_of!(TupleTableSlot, tts_values) < offset_of!(TupleTableSlot, tts_isnull));
        assert!(offset_of!(TupleTableSlot, tts_isnull) < offset_of!(TupleTableSlot, tts_mcxt));
        assert!(offset_of!(TupleTableSlot, tts_mcxt) < offset_of!(TupleTableSlot, tts_tid));
        assert!(offset_of!(TupleTableSlot, tts_tid) < offset_of!(TupleTableSlot, tts_tableOid));
    }

    #[test]
    fn concrete_slots_embed_base_first() {
        assert_eq!(offset_of!(VirtualTupleTableSlot, base), 0);
        assert_eq!(offset_of!(HeapTupleTableSlot, base), 0);
        assert_eq!(offset_of!(BufferHeapTupleTableSlot, base), 0);
        assert_eq!(offset_of!(MinimalTupleTableSlot, base), 0);

        // The concrete slots are at least as large as their base.
        assert!(size_of::<VirtualTupleTableSlot>() >= size_of::<TupleTableSlot>());
        assert!(size_of::<HeapTupleTableSlot>() >= size_of::<TupleTableSlot>());
        // BufferHeapTupleTableSlot's base is a HeapTupleTableSlot.
        assert!(size_of::<BufferHeapTupleTableSlot>() >= size_of::<HeapTupleTableSlot>());
        assert!(size_of::<MinimalTupleTableSlot>() >= size_of::<TupleTableSlot>());
    }

    #[test]
    fn vtable_first_field_is_size() {
        // base_slot_size leads the vtable so execTuples builds statics positionally.
        assert_eq!(offset_of!(TupleTableSlotOps, base_slot_size), 0);
        // fn-pointer Options are pointer-sized/aligned.
        assert_eq!(align_of::<TupleTableSlotOps>(), align_of::<usize>());
    }

    #[test]
    fn tts_empty_reads_flag() {
        // Hand-build a zeroed slot; EMPTY flag is clear.
        let mut slot: TupleTableSlot = unsafe { core::mem::zeroed() };
        unsafe {
            assert!(!TTS_EMPTY(&slot));
            assert!(!TTS_SHOULDFREE(&slot));
            assert!(!TTS_SLOW(&slot));
            assert!(!TTS_FIXED(&slot));
        }

        slot.tts_flags = TTS_FLAG_EMPTY;
        unsafe {
            assert!(TTS_EMPTY(&slot));
            assert!(!TTS_SHOULDFREE(&slot));
        }

        slot.tts_flags = TTS_FLAG_SHOULDFREE | TTS_FLAG_FIXED;
        unsafe {
            assert!(!TTS_EMPTY(&slot));
            assert!(TTS_SHOULDFREE(&slot));
            assert!(!TTS_SLOW(&slot));
            assert!(TTS_FIXED(&slot));
        }
    }

    #[test]
    fn tup_is_null_on_null_ptr() {
        let null_slot: *const TupleTableSlot = core::ptr::null();
        unsafe {
            assert!(TupIsNull(null_slot));
        }
    }
}
