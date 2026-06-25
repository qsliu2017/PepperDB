//! Translated from PostgreSQL src/include/access/htup_details.h
//! POSTGRES heap tuple header definitions.

#![allow(deprecated)]

use crate::access::htup::HeapTupleData;
use crate::access::tupdesc::TupleDesc;
use crate::c::{
    bits8, CommandId, FrozenTransactionId, InvalidTransactionId, TransactionId,
};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::{ItemPointerData, SpecTokenOffsetNumber};

// MaxTupleAttributeNumber limits the number of (user) columns in a tuple.
pub const MaxTupleAttributeNumber: i32 = 1664; // 8 * 208

// MaxHeapAttributeNumber limits the number of (user) columns in a table.
pub const MaxHeapAttributeNumber: i32 = 1600; // 8 * 200

// === choice: a union overlaying transaction (on-disk heap) fields with Datum
// (in-memory composite) fields. Both arms are 3x u32 = 12 bytes; modelled as a
// #[repr(C)] union mirroring the C layout. Inside HeapTupleFields, field3 is
// itself a union (t_cid / t_xvac), both u32, here collapsed to one u32 field.

/// On-disk transaction fields (HEAP case).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct HeapTupleFields {
    pub xmin: TransactionId, // inserting xact ID
    pub xmax: TransactionId, // deleting or locking xact ID
    // union { CommandId t_cid; TransactionId t_xvac; } field3 -- both 4 bytes.
    pub field3: u32,
}

const _: () = assert!(core::mem::size_of::<HeapTupleFields>() == 12);

/// In-memory composite-Datum fields (DATUM case), overlaying the xact fields.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DatumTupleFields {
    pub len_: i32,   // varlena header (do not touch directly!)
    pub typmod: i32, // -1, or identifier of a record type
    pub typeid: Oid, // composite type OID, or RECORDOID
}

const _: () = assert!(core::mem::size_of::<DatumTupleFields>() == 12);

#[repr(C)]
pub union HeapTupleHeaderChoice {
    pub t_heap: HeapTupleFields,
    pub t_datum: DatumTupleFields,
}

const _: () = assert!(core::mem::size_of::<HeapTupleHeaderChoice>() == 12);

/// On-disk heap tuple header. Fixed part is 23 bytes; the trailing nulls bitmap
/// (`t_bits`) and user data are an on-disk FAM accessed via slice helpers.
#[repr(C)]
pub struct HeapTupleHeaderData {
    pub choice: HeapTupleHeaderChoice,
    pub ctid: ItemPointerData, // current TID of this or newer tuple (or spec token)
    // Fields below here must match MinimalTupleData!
    pub t_infomask2: u16, // number of attributes + various flags
    pub t_infomask: u16,  // various flag bits, see below
    pub t_hoff: u8,       // sizeof header incl. bitmap, padding
    // bits8 t_bits[FLEXIBLE_ARRAY_MEMBER] -- nulls bitmap, on-disk FAM (see t_bits()).
}

pub const FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2: usize = 2;
pub const FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK: usize = 3;
pub const FIELDNO_HEAPTUPLEHEADERDATA_HOFF: usize = 4;
pub const FIELDNO_HEAPTUPLEHEADERDATA_BITS: usize = 5;

// offsetof(HeapTupleHeaderData, t_bits): the 23-byte fixed header. Note the C
// struct has no trailing padding (the FAM is char), so we assert offsets rather
// than size_of (Rust would pad the struct to align 4).
pub const SizeofHeapTupleHeader: usize = 23;
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, choice) == 0);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, ctid) == 12);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_infomask2) == 18);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_infomask) == 20);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_hoff) == 22);

// === information stored in t_infomask ===
pub const HEAP_HASNULL: u16 = 0x0001; // has null attribute(s)
pub const HEAP_HASVARWIDTH: u16 = 0x0002; // has variable-width attribute(s)
pub const HEAP_HASEXTERNAL: u16 = 0x0004; // has external stored attribute(s)
pub const HEAP_HASOID_OLD: u16 = 0x0008; // has an object-id field
pub const HEAP_XMAX_KEYSHR_LOCK: u16 = 0x0010; // xmax is a key-shared locker
pub const HEAP_COMBOCID: u16 = 0x0020; // t_cid is a combo CID
pub const HEAP_XMAX_EXCL_LOCK: u16 = 0x0040; // xmax is exclusive locker
pub const HEAP_XMAX_LOCK_ONLY: u16 = 0x0080; // xmax, if valid, is only a locker

// xmax is a shared locker
pub const HEAP_XMAX_SHR_LOCK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

pub const HEAP_LOCK_MASK: u16 =
    HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100; // xmin committed
pub const HEAP_XMIN_INVALID: u16 = 0x0200; // xmin invalid/aborted
pub const HEAP_XMIN_FROZEN: u16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400; // xmax committed
pub const HEAP_XMAX_INVALID: u16 = 0x0800; // xmax invalid/aborted
pub const HEAP_XMAX_IS_MULTI: u16 = 0x1000; // xmax is a MultiXactId
pub const HEAP_UPDATED: u16 = 0x2000; // this is UPDATEd version of row
pub const HEAP_MOVED_OFF: u16 = 0x4000; // moved by pre-9.0 VACUUM FULL (upgrade only)
pub const HEAP_MOVED_IN: u16 = 0x8000; // moved by pre-9.0 VACUUM FULL (upgrade only)
pub const HEAP_MOVED: u16 = HEAP_MOVED_OFF | HEAP_MOVED_IN;

pub const HEAP_XACT_MASK: u16 = 0xFFF0; // visibility-related bits

// turn these all off when Xmax is to change
pub const HEAP_XMAX_BITS: u16 = HEAP_XMAX_COMMITTED
    | HEAP_XMAX_INVALID
    | HEAP_XMAX_IS_MULTI
    | HEAP_LOCK_MASK
    | HEAP_XMAX_LOCK_ONLY;

// === information stored in t_infomask2 ===
// NOTE: t_infomask2 packs an 11-bit natts count beside flag bits; per bitflags
// appendix C it stays a raw u16 with accessor methods, NOT a bitflags set.
pub const HEAP_NATTS_MASK: u16 = 0x07FF; // 11 bits for number of attributes
// bits 0x1800 are available
pub const HEAP_KEYS_UPDATED: u16 = 0x2000; // updated+key cols modified, or deleted
pub const HEAP_HOT_UPDATED: u16 = 0x4000; // tuple was HOT-updated
pub const HEAP_ONLY_TUPLE: u16 = 0x8000; // this is heap-only tuple

pub const HEAP2_XACT_MASK: u16 = 0xE000; // visibility-related bits

// HEAP_TUPLE_HAS_MATCH: temporary hash-join flag, overlaid on HEAP_ONLY_TUPLE.
pub const HEAP_TUPLE_HAS_MATCH: u16 = HEAP_ONLY_TUPLE; // tuple has a join match

/// A tuple is only locked (not updated by its Xmax) if HEAP_XMAX_LOCK_ONLY is
/// set; or, for pg_upgrade's sake, if Xmax is not a multi and EXCL_LOCK is set.
pub const fn HEAP_XMAX_IS_LOCKED_ONLY(infomask: u16) -> bool {
    (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        || (infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK
}

/// Detect a tuple share-locked in 9.2 or earlier and then pg_upgrade'd.
pub const fn HEAP_LOCKED_UPGRADED(infomask: u16) -> bool {
    (infomask & HEAP_XMAX_IS_MULTI) != 0
        && (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        && (infomask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)) == 0
}

/// True iff a shared lock is applied to the tuple.
pub const fn HEAP_XMAX_IS_SHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_SHR_LOCK
}

/// True iff an exclusive lock is applied to the tuple.
pub const fn HEAP_XMAX_IS_EXCL_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_EXCL_LOCK
}

/// True iff a key-share lock is applied to the tuple.
pub const fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_KEYSHR_LOCK
}

// StaticAssertDecl(MaxOffsetNumber < SpecTokenOffsetNumber, ...)
const _: () =
    assert!(crate::storage::off::MAX_OFFSET_NUMBER < SpecTokenOffsetNumber);

// === HeapTupleHeader accessor functions (macros/inlines -> methods) ===
// The choice union reads are safe: which arm is live is determined by usage
// context (xact vs datum), exactly as in C. The unsafe is contained here.
impl HeapTupleHeaderData {
    /// Raw xmin (the xid originally used to insert the tuple).
    pub fn get_raw_xmin(&self) -> TransactionId {
        unsafe { self.choice.t_heap.xmin }
    }

    /// Xmin, resolving a frozen tuple to FrozenTransactionId.
    pub fn get_xmin(&self) -> TransactionId {
        if self.xmin_frozen() {
            FrozenTransactionId
        } else {
            self.get_raw_xmin()
        }
    }

    pub fn set_xmin(&mut self, xid: TransactionId) {
        self.choice.t_heap.xmin = xid;
    }

    pub fn xmin_committed(&self) -> bool {
        (self.t_infomask & HEAP_XMIN_COMMITTED) != 0
    }

    pub fn xmin_invalid(&self) -> bool {
        (self.t_infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) == HEAP_XMIN_INVALID
    }

    pub fn xmin_frozen(&self) -> bool {
        (self.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
    }

    pub fn set_xmin_committed(&mut self) {
        debug_assert!(!self.xmin_invalid());
        self.t_infomask |= HEAP_XMIN_COMMITTED;
    }

    pub fn set_xmin_invalid(&mut self) {
        debug_assert!(!self.xmin_committed());
        self.t_infomask |= HEAP_XMIN_INVALID;
    }

    pub fn set_xmin_frozen(&mut self) {
        debug_assert!(!self.xmin_invalid());
        self.t_infomask |= HEAP_XMIN_FROZEN;
    }

    /// Raw Xmax field. Use get_update_xid to resolve a possible MultiXactId.
    pub fn get_raw_xmax(&self) -> TransactionId {
        unsafe { self.choice.t_heap.xmax }
    }

    pub fn set_xmax(&mut self, xid: TransactionId) {
        self.choice.t_heap.xmax = xid;
    }

    /// Xmax that updated the tuple, resolving a MultiXactId when necessary
    /// (may incur multixact I/O).
    pub fn get_update_xid(&self) -> TransactionId {
        if (self.t_infomask & HEAP_XMAX_INVALID) == 0
            && (self.t_infomask & HEAP_XMAX_IS_MULTI) != 0
            && (self.t_infomask & HEAP_XMAX_LOCK_ONLY) == 0
        {
            unimplemented!() // HeapTupleGetUpdateXid(self)
        } else {
            self.get_raw_xmax()
        }
    }

    /// Raw command id from the header (combo or not); see get_cmin/get_cmax.
    pub fn get_raw_command_id(&self) -> CommandId {
        CommandId(unsafe { self.choice.t_heap.field3 })
    }

    pub fn set_cmin(&mut self, cid: CommandId) {
        debug_assert!((self.t_infomask & HEAP_MOVED) == 0);
        self.choice.t_heap.field3 = cid.0;
        self.t_infomask &= !HEAP_COMBOCID;
    }

    pub fn set_cmax(&mut self, cid: CommandId, iscombo: bool) {
        debug_assert!((self.t_infomask & HEAP_MOVED) == 0);
        self.choice.t_heap.field3 = cid.0;
        if iscombo {
            self.t_infomask |= HEAP_COMBOCID;
        } else {
            self.t_infomask &= !HEAP_COMBOCID;
        }
    }

    pub fn get_xvac(&self) -> TransactionId {
        if (self.t_infomask & HEAP_MOVED) != 0 {
            TransactionId(unsafe { self.choice.t_heap.field3 })
        } else {
            InvalidTransactionId
        }
    }

    pub fn set_xvac(&mut self, xid: TransactionId) {
        debug_assert!((self.t_infomask & HEAP_MOVED) != 0);
        self.choice.t_heap.field3 = xid.0;
    }

    pub fn is_speculative(&self) -> bool {
        self.ctid.offset_number_no_check() == SpecTokenOffsetNumber
    }

    pub fn get_speculative_token(&self) -> BlockNumber {
        debug_assert!(self.is_speculative());
        self.ctid.block_number_no_check()
    }

    pub fn set_speculative_token(&mut self, token: BlockNumber) {
        self.ctid.set(token, SpecTokenOffsetNumber);
    }

    pub fn indicates_moved_partitions(&self) -> bool {
        self.ctid.indicates_moved_partitions()
    }

    pub fn set_moved_partitions(&mut self) {
        self.ctid.set_moved_partitions();
    }

    /// Datum length (VARSIZE of the overlaid varlena header). TODO(varlena).
    pub fn get_datum_length(&self) -> u32 {
        unimplemented!() // VARSIZE(tup)
    }

    pub fn set_datum_length(&mut self, _len: u32) {
        unimplemented!() // SET_VARSIZE(tup, len)
    }

    pub fn get_type_id(&self) -> Oid {
        unsafe { self.choice.t_datum.typeid }
    }

    pub fn set_type_id(&mut self, typeid: Oid) {
        self.choice.t_datum.typeid = typeid;
    }

    pub fn get_typmod(&self) -> i32 {
        unsafe { self.choice.t_datum.typmod }
    }

    pub fn set_typmod(&mut self, typmod: i32) {
        self.choice.t_datum.typmod = typmod;
    }

    /// HOT-updated: also requires the updater not known aborted.
    pub fn is_hot_updated(&self) -> bool {
        (self.t_infomask2 & HEAP_HOT_UPDATED) != 0
            && (self.t_infomask & HEAP_XMAX_INVALID) == 0
            && !self.xmin_invalid()
    }

    pub fn set_hot_updated(&mut self) {
        self.t_infomask2 |= HEAP_HOT_UPDATED;
    }

    pub fn clear_hot_updated(&mut self) {
        self.t_infomask2 &= !HEAP_HOT_UPDATED;
    }

    pub fn is_heap_only(&self) -> bool {
        (self.t_infomask2 & HEAP_ONLY_TUPLE) != 0
    }

    pub fn set_heap_only(&mut self) {
        self.t_infomask2 |= HEAP_ONLY_TUPLE;
    }

    pub fn clear_heap_only(&mut self) {
        self.t_infomask2 &= !HEAP_ONLY_TUPLE;
    }

    /// Number of attributes (low 11 bits of t_infomask2). Shared with MinimalTuple.
    pub fn get_natts(&self) -> u16 {
        self.t_infomask2 & HEAP_NATTS_MASK
    }

    pub fn set_natts(&mut self, natts: u16) {
        self.t_infomask2 = (self.t_infomask2 & !HEAP_NATTS_MASK) | natts;
    }

    pub fn has_external(&self) -> bool {
        (self.t_infomask & HEAP_HASEXTERNAL) != 0
    }

    /// Nulls bitmap (on-disk FAM at t_bits). Present only if HEAP_HASNULL.
    /// SAFETY: `self` must point into a tuple buffer with `natts` columns.
    pub unsafe fn t_bits(&self, natts: usize) -> &[bits8] {
        let base = (self as *const Self).cast::<u8>().add(SizeofHeapTupleHeader);
        core::slice::from_raw_parts(base, BITMAPLEN(natts as i32) as usize)
    }
}

pub fn HeapTupleHeaderXminFrozen(tup: &HeapTupleHeaderData) -> bool {
    tup.xmin_frozen()
}

/// Computes size of null bitmap given number of data columns.
pub const fn BITMAPLEN(natts: i32) -> i32 {
    (natts + 7) / 8
}

// MaxHeapTupleSize = BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)).
// SizeOfPageHeaderData = 24, sizeof(ItemIdData) = 4, MAXALIGN(28) = 32.
pub const MaxHeapTupleSize: usize = 8192 - 32;
/// MinHeapTupleSize = MAXALIGN(SizeofHeapTupleHeader) = MAXALIGN(23) = 24.
pub const MinHeapTupleSize: usize = 24;

// MaxHeapTuplesPerPage = (BLCKSZ - SizeOfPageHeaderData)
//                        / (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))
//                      = (8192 - 24) / (24 + 4) = 8168 / 28 = 291.
pub const MaxHeapTuplesPerPage: i32 = 291;

/// Arbitrary upper limit on declared size of char(n)-like data fields (10 MB).
pub const MaxAttrSize: usize = 10 * 1024 * 1024;

// === MinimalTuple (on-disk transient executor tuple) ===
// MINIMAL_TUPLE_OFFSET = (offsetof(HeapTupleHeaderData, t_infomask2) - 4)
//                        / MAXIMUM_ALIGNOF * MAXIMUM_ALIGNOF = (18-4)/8*8 = 8.
pub const MINIMAL_TUPLE_OFFSET: usize = 8;
// MINIMAL_TUPLE_PADDING = (18 - 4) % 8 = 6.
pub const MINIMAL_TUPLE_PADDING: usize = 6;

/// MinimalTuple: a HeapTupleHeader without xact info or ctid, padded so that
/// offsetof(t_infomask2) is congruent mod MAXALIGN with the full tuple.
#[repr(C)]
pub struct MinimalTupleData {
    pub t_len: u32, // actual length of minimal tuple
    pub mt_padding: [u8; MINIMAL_TUPLE_PADDING],
    // Fields below here must match HeapTupleHeaderData!
    pub t_infomask2: u16, // number of attributes + various flags
    pub t_infomask: u16,  // various flag bits, see below
    pub t_hoff: u8,       // sizeof header incl. bitmap, padding
    // bits8 t_bits[FLEXIBLE_ARRAY_MEMBER] -- nulls bitmap, on-disk FAM.
}

pub const SizeofMinimalTupleHeader: usize = 13; // offsetof(MinimalTupleData, t_bits)
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_len) == 0);
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_infomask2) == 10);
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_infomask) == 12);
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_hoff) == 14);

// MINIMAL_TUPLE_DATA_OFFSET = offsetof(MinimalTupleData, t_infomask2) = 10.
pub const MINIMAL_TUPLE_DATA_OFFSET: usize = 10;

impl MinimalTupleData {
    pub fn has_match(&self) -> bool {
        (self.t_infomask2 & HEAP_TUPLE_HAS_MATCH) != 0
    }

    pub fn set_match(&mut self) {
        self.t_infomask2 |= HEAP_TUPLE_HAS_MATCH;
    }

    pub fn clear_match(&mut self) {
        self.t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
    }
}

// === HeapTuple (in-memory wrapper) accessor functions ===
// These delegate to HeapTupleHeaderData methods via tuple.t_data, typed
// *mut crate::access::htup::HeapTupleHeaderData; bodies are stubbed.

/// Address of the user data following the header. TODO(ptr).
pub fn GETSTRUCT(_tuple: &HeapTupleData) -> *mut u8 {
    unimplemented!()
}

pub fn HeapTupleHasNulls(_tuple: &HeapTupleData) -> bool {
    unimplemented!() // (t_data->t_infomask & HEAP_HASNULL) != 0
}

pub fn HeapTupleNoNulls(tuple: &HeapTupleData) -> bool {
    !HeapTupleHasNulls(tuple)
}

pub fn HeapTupleHasVarWidth(_tuple: &HeapTupleData) -> bool {
    unimplemented!() // (t_data->t_infomask & HEAP_HASVARWIDTH) != 0
}

pub fn HeapTupleAllFixed(tuple: &HeapTupleData) -> bool {
    !HeapTupleHasVarWidth(tuple)
}

pub fn HeapTupleHasExternal(_tuple: &HeapTupleData) -> bool {
    unimplemented!() // (t_data->t_infomask & HEAP_HASEXTERNAL) != 0
}

pub fn HeapTupleIsHotUpdated(_tuple: &HeapTupleData) -> bool {
    unimplemented!() // t_data->is_hot_updated()
}

pub fn HeapTupleSetHotUpdated(_tuple: &HeapTupleData) {
    unimplemented!() // t_data->set_hot_updated()
}

pub fn HeapTupleClearHotUpdated(_tuple: &HeapTupleData) {
    unimplemented!() // t_data->clear_hot_updated()
}

pub fn HeapTupleIsHeapOnly(_tuple: &HeapTupleData) -> bool {
    unimplemented!() // t_data->is_heap_only()
}

pub fn HeapTupleSetHeapOnly(_tuple: &HeapTupleData) {
    unimplemented!() // t_data->set_heap_only()
}

pub fn HeapTupleClearHeapOnly(_tuple: &HeapTupleData) {
    unimplemented!() // t_data->clear_heap_only()
}

// === prototypes for functions in common/heaptuple.c ===

pub fn heap_compute_data_size(
    _tuple_desc: &TupleDesc,
    _values: &[Datum],
    _isnull: &[bool],
) -> usize {
    unimplemented!()
}

/// Returns (infomask, bitmap) updates via the data buffer (C out-params).
pub fn heap_fill_tuple(
    _tuple_desc: &TupleDesc,
    _values: &[Datum],
    _isnull: &[bool],
    _data: &mut [u8],
    _data_size: usize,
    _infomask: &mut u16,
    _bit: &mut [bits8],
) {
    unimplemented!()
}

pub fn heap_attisnull(_tup: &HeapTupleData, _attnum: i32, _tuple_desc: &TupleDesc) -> bool {
    unimplemented!()
}

pub fn nocachegetattr(_tup: &HeapTupleData, _attnum: i32, _tuple_desc: &TupleDesc) -> Datum {
    unimplemented!()
}

/// Returns (value, isnull) (C out-param isnull folded into the tuple).
pub fn heap_getsysattr(
    _tup: &HeapTupleData,
    _attnum: i32,
    _tuple_desc: &TupleDesc,
) -> (Datum, bool) {
    unimplemented!()
}

/// Returns (value, isnull).
pub fn getmissingattr(_tuple_desc: &TupleDesc, _attnum: i32) -> (Datum, bool) {
    unimplemented!()
}

pub fn heap_copytuple(_tuple: &HeapTupleData) -> HeapTupleData {
    unimplemented!()
}

pub fn heap_copytuple_with_tuple(_src: &HeapTupleData, _dest: &mut HeapTupleData) {
    unimplemented!()
}

pub fn heap_copy_tuple_as_datum(_tuple: &HeapTupleData, _tuple_desc: &TupleDesc) -> Datum {
    unimplemented!()
}

pub fn heap_form_tuple(
    _tuple_descriptor: &TupleDesc,
    _values: &[Datum],
    _isnull: &[bool],
) -> HeapTupleData {
    unimplemented!()
}

pub fn heap_modify_tuple(
    _tuple: &HeapTupleData,
    _tuple_desc: &TupleDesc,
    _repl_values: &[Datum],
    _repl_isnull: &[bool],
    _do_replace: &[bool],
) -> HeapTupleData {
    unimplemented!()
}

pub fn heap_modify_tuple_by_cols(
    _tuple: &HeapTupleData,
    _tuple_desc: &TupleDesc,
    _repl_cols: &[i32],
    _repl_values: &[Datum],
    _repl_isnull: &[bool],
) -> HeapTupleData {
    unimplemented!()
}

/// Deforms a tuple into (values, isnull) (C out-param arrays).
pub fn heap_deform_tuple(_tuple: &HeapTupleData, _tuple_desc: &TupleDesc) -> (Vec<Datum>, Vec<bool>) {
    unimplemented!()
}

pub fn heap_freetuple(_htup: HeapTupleData) {
    unimplemented!()
}

pub fn heap_form_minimal_tuple(
    _tuple_descriptor: &TupleDesc,
    _values: &[Datum],
    _isnull: &[bool],
    _extra: usize,
) -> *mut MinimalTupleData {
    unimplemented!() // TODO(ptr)
}

pub fn heap_free_minimal_tuple(_mtup: *mut MinimalTupleData) {
    unimplemented!() // TODO(ptr)
}

pub fn heap_copy_minimal_tuple(_mtup: *mut MinimalTupleData, _extra: usize) -> *mut MinimalTupleData {
    unimplemented!() // TODO(ptr)
}

pub fn heap_tuple_from_minimal_tuple(_mtup: *mut MinimalTupleData) -> HeapTupleData {
    unimplemented!() // TODO(ptr)
}

pub fn minimal_tuple_from_heap_tuple(_htup: &HeapTupleData, _extra: usize) -> *mut MinimalTupleData {
    unimplemented!() // TODO(ptr)
}

pub fn varsize_any(_p: *mut u8) -> usize {
    unimplemented!() // TODO(ptr)
}

pub fn heap_expand_tuple(_source_tuple: &HeapTupleData, _tuple_desc: &TupleDesc) -> HeapTupleData {
    unimplemented!()
}

pub fn minimal_expand_tuple(
    _source_tuple: &HeapTupleData,
    _tuple_desc: &TupleDesc,
) -> *mut MinimalTupleData {
    unimplemented!() // TODO(ptr)
}

/// Fetch a user attribute's value as (value, isnull). attnum MUST be valid and
/// must not be a system attribute; use heap_getattr if in doubt.
pub fn fastgetattr(
    _tup: &HeapTupleData,
    _attnum: i32,
    _tuple_desc: &TupleDesc,
) -> (Datum, bool) {
    unimplemented!()
}

/// Extract an attribute of a heap tuple as (value, isnull), range-checked, and
/// works for either system or user attributes.
pub fn heap_getattr(
    _tup: &HeapTupleData,
    _attnum: i32,
    _tuple_desc: &TupleDesc,
) -> (Datum, bool) {
    unimplemented!()
}
