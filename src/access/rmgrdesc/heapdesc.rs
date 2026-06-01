//! Translation of postgres/src/backend/access/rmgrdesc/heapdesc.c
//!                + the WAL record structs/opcodes MERGED from
//!                  postgres/src/include/access/heapam_xlog.h
//!
//! rmgr descriptor routines for access/heap/heapam.c, used by pg_waldump.
//! Two resource managers share this file: RM_HEAP (heap_desc/heap_identify)
//! and RM_HEAP2 (heap2_desc/heap2_identify).
//!
//! Notes / conventions:
//!   - XLogReaderState: opaque (`c_void`). XLogRecGetData / XLogRecGetInfo /
//!     XLogRecHasBlockData / XLogRecGetBlockData are STUBBED (return null/0)
//!     until access/xlogreader.h is ported. TODO markers retained.
//!   - The opcode tables and #[repr(C)] WAL struct layouts are REAL.
//!   - XLR_INFO_MASK = 0x0F: the *_identify functions keep the HIGH nibble.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::rmgrdesc::rmgrdesc_utils::{
    array_desc, offset_elem_desc, oid_elem_desc, redirect_elem_desc,
};
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetBlockData, XLogRecGetData, XLogRecGetInfo, XLogRecHasBlockData,
    XLR_INFO_MASK,
};
use crate::access::rmgrdesc::standbydesc::{
    standby_desc_invalidations, RelFileLocator, SharedInvalidationMessage,
};
use crate::c::CommandId;
use crate::lib::stringinfo::{appendStringInfoChar, appendStringInfoString, StringInfo};
use crate::prelude::*;
use crate::storage::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};

/// OffsetNumber (storage/off.h) -- the per-page item index type.
pub type OffsetNumber = uint16;
/// TransactionId (access/transam.h) -- a 32-bit transaction id.
pub type TransactionId = uint32;

// ---------------------------------------------------------------------------
// WAL opcodes (heapam_xlog.h)
//
// XLOG stores info in the high 4 bits of the record's xl_info field: 3 bits
// of opcode + 1 init-page bit.  XLOG_HEAP_OPMASK applies to both RM_HEAP and
// RM_HEAP2 opcodes.
// ---------------------------------------------------------------------------

pub const XLOG_HEAP_INSERT: uint8 = 0x00;
pub const XLOG_HEAP_DELETE: uint8 = 0x10;
pub const XLOG_HEAP_UPDATE: uint8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: uint8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: uint8 = 0x40;
pub const XLOG_HEAP_CONFIRM: uint8 = 0x50;
pub const XLOG_HEAP_LOCK: uint8 = 0x60;
pub const XLOG_HEAP_INPLACE: uint8 = 0x70;

pub const XLOG_HEAP_OPMASK: uint8 = 0x70;
/// When we insert the 1st item on a new page (INSERT/UPDATE/HOT_UPDATE/
/// MULTI_INSERT) we can restore the entire page in redo.
pub const XLOG_HEAP_INIT_PAGE: uint8 = 0x80;

// RM_HEAP2 opcodes.
pub const XLOG_HEAP2_REWRITE: uint8 = 0x00;
pub const XLOG_HEAP2_PRUNE_ON_ACCESS: uint8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: uint8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: uint8 = 0x30;
pub const XLOG_HEAP2_VISIBLE: uint8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: uint8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: uint8 = 0x60;
pub const XLOG_HEAP2_NEW_CID: uint8 = 0x70;

// ---------------------------------------------------------------------------
// insert/multi_insert flag values
// ---------------------------------------------------------------------------
pub const XLH_INSERT_ALL_VISIBLE_CLEARED: uint8 = 1 << 0;
pub const XLH_INSERT_LAST_IN_MULTI: uint8 = 1 << 1;
pub const XLH_INSERT_IS_SPECULATIVE: uint8 = 1 << 2;
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: uint8 = 1 << 3;
pub const XLH_INSERT_ON_TOAST_RELATION: uint8 = 1 << 4;
pub const XLH_INSERT_ALL_FROZEN_SET: uint8 = 1 << 5;

// ---------------------------------------------------------------------------
// update flag values
// ---------------------------------------------------------------------------
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: uint8 = 1 << 0;
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: uint8 = 1 << 1;
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: uint8 = 1 << 2;
pub const XLH_UPDATE_CONTAINS_OLD_KEY: uint8 = 1 << 3;
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: uint8 = 1 << 4;
pub const XLH_UPDATE_PREFIX_FROM_OLD: uint8 = 1 << 5;
pub const XLH_UPDATE_SUFFIX_FROM_OLD: uint8 = 1 << 6;
pub const XLH_UPDATE_CONTAINS_OLD: uint8 =
    XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY;

// ---------------------------------------------------------------------------
// delete flag values
// ---------------------------------------------------------------------------
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: uint8 = 1 << 0;
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: uint8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: uint8 = 1 << 2;
pub const XLH_DELETE_IS_SUPER: uint8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: uint8 = 1 << 4;
pub const XLH_DELETE_CONTAINS_OLD: uint8 =
    XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY;

// ---------------------------------------------------------------------------
// truncate flag values
// ---------------------------------------------------------------------------
pub const XLH_TRUNCATE_CASCADE: uint8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: uint8 = 1 << 1;

// ---------------------------------------------------------------------------
// xl_heap_prune flags (XLHP_*)
// ---------------------------------------------------------------------------
/// recovery conflict during logical decoding on standby
pub const XLHP_IS_CATALOG_REL: uint8 = 1 << 1;
/// replaying the record requires a cleanup-lock
pub const XLHP_CLEANUP_LOCK: uint8 = 1 << 2;
/// a snapshot conflict horizon XID follows (unaligned)
pub const XLHP_HAS_CONFLICT_HORIZON: uint8 = 1 << 3;
/// an xlhp_freeze_plans sub-record is present
pub const XLHP_HAS_FREEZE_PLANS: uint8 = 1 << 4;
pub const XLHP_HAS_REDIRECTIONS: uint8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: uint8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: uint8 = 1 << 7;

// freeze-plan frzflags (0x01 was XLH_FREEZE_XMIN)
pub const XLH_FREEZE_XVAC: uint8 = 0x02;
pub const XLH_INVALID_XVAC: uint8 = 0x04;

// ---------------------------------------------------------------------------
// infobits_set flags
// ---------------------------------------------------------------------------
pub const XLHL_XMAX_IS_MULTI: uint8 = 0x01;
pub const XLHL_XMAX_LOCK_ONLY: uint8 = 0x02;
pub const XLHL_XMAX_EXCL_LOCK: uint8 = 0x04;
pub const XLHL_XMAX_KEYSHR_LOCK: uint8 = 0x08;
pub const XLHL_KEYS_UPDATED: uint8 = 0x10;

/// xl_heap_lock / xl_heap_lock_updated flag field
pub const XLH_LOCK_ALL_FROZEN_CLEARED: uint8 = 0x01;

// ---------------------------------------------------------------------------
// WAL record structs (heapam_xlog.h)
// ---------------------------------------------------------------------------

/// xl_heap_delete: what we need to know about a delete.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_delete {
    pub xmax: TransactionId,    /* xmax of the deleted tuple */
    pub offnum: OffsetNumber,   /* deleted tuple's offset */
    pub infobits_set: uint8,    /* infomask bits */
    pub flags: uint8,
}

/// xl_heap_truncate: dbId, then nrelids, flags, then relids[] (flex array).
#[repr(C)]
pub struct xl_heap_truncate {
    pub dbId: Oid,
    pub nrelids: uint32,
    pub flags: uint8,
    pub relids: [Oid; 0],
}

/// xl_heap_header: the saved fixed part of an inserted/updated tuple.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_header {
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
}

/// xl_heap_insert: what we need to know about an insert.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_insert {
    pub offnum: OffsetNumber, /* inserted tuple's offset */
    pub flags: uint8,
    /* xl_heap_header & TUPLE DATA in backup block 0 */
}

/// xl_heap_multi_insert: header for a multi-insert; offsets[] omitted when the
/// whole page is reinitialized (XLOG_HEAP_INIT_PAGE).
#[repr(C)]
pub struct xl_heap_multi_insert {
    pub flags: uint8,
    pub ntuples: uint16,
    pub offsets: [OffsetNumber; 0],
}

/// xl_multi_insert_tuple: per-tuple header within block 0's data.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_multi_insert_tuple {
    pub datalen: uint16, /* size of tuple data that follows */
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    /* TUPLE DATA FOLLOWS AT END OF STRUCT */
}

/// xl_heap_update: what we need to know about update|hot_update.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_update {
    pub old_xmax: TransactionId,  /* xmax of the old tuple */
    pub old_offnum: OffsetNumber, /* old tuple's offset */
    pub old_infobits_set: uint8,  /* infomask bits to set on old tuple */
    pub flags: uint8,
    pub new_xmax: TransactionId,  /* xmax of the new tuple */
    pub new_offnum: OffsetNumber, /* new tuple's offset */
}

/// xl_heap_prune: main pruning/freezing record header. The XLHP_HAS_* flags
/// indicate which sub-records follow in block 0's data; a conflict horizon XID
/// follows unaligned in the main data if XLHP_HAS_CONFLICT_HORIZON is set.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_prune {
    pub reason: uint8,
    pub flags: uint8,
}

/// xlhp_freeze_plan: how to freeze a group of one or more heap tuples.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xlhp_freeze_plan {
    pub xmax: TransactionId,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub frzflags: uint8,
    /* Length of individual page offset numbers array for this plan */
    pub ntuples: uint16,
}

/// xlhp_freeze_plans: nplans followed by plans[] (flex array).
#[repr(C)]
pub struct xlhp_freeze_plans {
    pub nplans: uint16,
    pub plans: [xlhp_freeze_plan; 0],
}

/// xlhp_prune_items: generic sub-record holding redirect/dead/unused offsets.
/// For the REDIRECTIONS variant there are 2*ntargets OffsetNumbers in `data`.
#[repr(C)]
pub struct xlhp_prune_items {
    pub ntargets: uint16,
    pub data: [OffsetNumber; 0],
}

/// xl_heap_lock: what we need to know about a lock.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_lock {
    pub xmax: TransactionId,  /* might be a MultiXactId */
    pub offnum: OffsetNumber, /* locked tuple's offset on page */
    pub infobits_set: uint8,  /* infomask and infomask2 bits to set */
    pub flags: uint8,         /* XLH_LOCK_* flag bits */
}

/// xl_heap_lock_updated: locking an updated version of a row.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_lock_updated {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: uint8,
    pub flags: uint8,
}

/// xl_heap_confirm: confirmation of a speculative insertion.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_confirm {
    pub offnum: OffsetNumber, /* confirmed tuple's offset on page */
}

/// xl_heap_inplace: an in-place update, carrying shared-inval messages.
#[repr(C)]
pub struct xl_heap_inplace {
    pub offnum: OffsetNumber,         /* updated tuple's offset on page */
    pub dbId: Oid,                    /* MyDatabaseId */
    pub tsId: Oid,                    /* MyDatabaseTableSpace */
    pub relcacheInitFileInval: bool,  /* invalidate relcache init files */
    pub nmsgs: c_int,                 /* number of shared inval msgs */
    pub msgs: [SharedInvalidationMessage; 0],
}

/// xl_heap_visible: setting a visibility-map bit.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_visible {
    pub snapshotConflictHorizon: TransactionId,
    pub flags: uint8,
}

/// xl_heap_new_cid: a new combo-cid mapping, logged for logical decoding.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_new_cid {
    pub top_xid: TransactionId,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId, /* just for debugging */
    pub target_locator: RelFileLocator,
    pub target_tid: ItemPointerData,
}

// Size macros (offsetof of the first byte past the named field).
/// SizeOfHeapPrune: bytes of the fixed xl_heap_prune header (reason+flags).
pub const SizeOfHeapPrune: usize = 2;

// ---------------------------------------------------------------------------
// desc helpers
// ---------------------------------------------------------------------------

/// Map a Rust bool to PostgreSQL's 'T'/'F' rendering for `%c` columns.
#[inline]
fn tf(b: bool) -> char {
    if b {
        'T'
    } else {
        'F'
    }
}

/// infobits_desc: render `keyname: [FLAG, FLAG]` from an infobits byte.
/// NOTE: "keyname" must not have trailing spaces or punctuation.
///
/// # Safety
/// `buf` must be a valid StringInfo.
unsafe fn infobits_desc(buf: StringInfo, infobits: uint8, keyname: &str) {
    appendStringInfo!(buf, "{}: [", keyname);

    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        appendStringInfoString(buf, c"IS_MULTI, ".as_ptr());
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        appendStringInfoString(buf, c"LOCK_ONLY, ".as_ptr());
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        appendStringInfoString(buf, c"EXCL_LOCK, ".as_ptr());
    }
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        appendStringInfoString(buf, c"KEYSHR_LOCK, ".as_ptr());
    }
    if infobits & XLHL_KEYS_UPDATED != 0 {
        appendStringInfoString(buf, c"KEYS_UPDATED, ".as_ptr());
    }

    // Truncate away the trailing ", " if any flag was emitted.
    let b = &mut *buf;
    if *b.data.add((b.len - 1) as usize) == b' ' as c_char {
        b.len -= 2;
        *b.data.add(b.len as usize) = b'\0' as c_char;
    }

    appendStringInfoChar(buf, b']' as c_char);
}

/// truncate_flags_desc: render `flags: [CASCADE, RESTART_SEQS]`.
///
/// # Safety
/// `buf` must be a valid StringInfo.
unsafe fn truncate_flags_desc(buf: StringInfo, flags: uint8) {
    appendStringInfoString(buf, c"flags: [".as_ptr());

    if flags & XLH_TRUNCATE_CASCADE != 0 {
        appendStringInfoString(buf, c"CASCADE, ".as_ptr());
    }
    if flags & XLH_TRUNCATE_RESTART_SEQS != 0 {
        appendStringInfoString(buf, c"RESTART_SEQS, ".as_ptr());
    }

    let b = &mut *buf;
    if *b.data.add((b.len - 1) as usize) == b' ' as c_char {
        b.len -= 2;
        *b.data.add(b.len as usize) = b'\0' as c_char;
    }

    appendStringInfoChar(buf, b']' as c_char);
}

/// plan_elem_desc: array_desc callback rendering one xlhp_freeze_plan, then
/// advancing the shared `frz_offsets` cursor past this plan's offsets.
///
/// `data` points to an `*mut OffsetNumber` (the running frz_offsets cursor).
///
/// # Safety
/// `plan` points to a valid xlhp_freeze_plan; `data` to a valid
/// `*mut OffsetNumber`; the cursor must have ntuples readable OffsetNumbers.
unsafe fn plan_elem_desc(buf: StringInfo, plan: *mut c_void, data: *mut c_void) {
    let new_plan = plan as *mut xlhp_freeze_plan;
    let offsets = data as *mut *mut OffsetNumber;

    appendStringInfo!(
        buf,
        "{{ xmax: {}, infomask: {}, infomask2: {}, ntuples: {}",
        (*new_plan).xmax,
        (*new_plan).t_infomask,
        (*new_plan).t_infomask2,
        (*new_plan).ntuples
    );

    appendStringInfoString(buf, c", offsets:".as_ptr());
    array_desc(
        buf,
        *offsets as *mut c_void,
        core::mem::size_of::<OffsetNumber>(),
        (*new_plan).ntuples as c_int,
        offset_elem_desc,
        null_mut(),
    );

    *offsets = (*offsets).add((*new_plan).ntuples as usize);

    appendStringInfoString(buf, c" }".as_ptr());
}

/// heap_xlog_deserialize_prune_and_freeze: given a MAXALIGNed block-data buffer
/// (from XLogRecGetBlockData) and the xl_heap_prune flags, slice out the
/// freeze-plan and redirect/dead/unused OffsetNumber sub-records. Shared between
/// heap2_redo and heap2_desc (frontend pg_waldump).
///
/// # Safety
/// `cursor` must point at a valid serialized prune-and-freeze block-data buffer
/// consistent with `flags`; all out-pointers must be valid.
pub unsafe fn heap_xlog_deserialize_prune_and_freeze(
    mut cursor: *mut c_char,
    flags: uint8,
    nplans: *mut c_int,
    plans: *mut *mut xlhp_freeze_plan,
    frz_offsets: *mut *mut OffsetNumber,
    nredirected: *mut c_int,
    redirected: *mut *mut OffsetNumber,
    ndead: *mut c_int,
    nowdead: *mut *mut OffsetNumber,
    nunused: *mut c_int,
    nowunused: *mut *mut OffsetNumber,
) {
    if flags & XLHP_HAS_FREEZE_PLANS != 0 {
        let freeze_plans = cursor as *mut xlhp_freeze_plans;

        *nplans = (*freeze_plans).nplans as c_int;
        *plans = (*freeze_plans).plans.as_mut_ptr();

        // offsetof(xlhp_freeze_plans, plans)
        cursor = cursor.add(core::mem::offset_of!(xlhp_freeze_plans, plans));
        cursor = cursor.add(core::mem::size_of::<xlhp_freeze_plan>() * (*nplans) as usize);
    } else {
        *nplans = 0;
        *plans = null_mut();
    }

    if flags & XLHP_HAS_REDIRECTIONS != 0 {
        let subrecord = cursor as *mut xlhp_prune_items;

        *nredirected = (*subrecord).ntargets as c_int;
        *redirected = (*subrecord).data.as_mut_ptr();

        cursor = cursor.add(core::mem::offset_of!(xlhp_prune_items, data));
        // sizeof(OffsetNumber[2]) * nredirected
        cursor = cursor.add(core::mem::size_of::<OffsetNumber>() * 2 * (*nredirected) as usize);
    } else {
        *nredirected = 0;
        *redirected = null_mut();
    }

    if flags & XLHP_HAS_DEAD_ITEMS != 0 {
        let subrecord = cursor as *mut xlhp_prune_items;

        *ndead = (*subrecord).ntargets as c_int;
        *nowdead = (*subrecord).data.as_mut_ptr();

        cursor = cursor.add(core::mem::offset_of!(xlhp_prune_items, data));
        cursor = cursor.add(core::mem::size_of::<OffsetNumber>() * (*ndead) as usize);
    } else {
        *ndead = 0;
        *nowdead = null_mut();
    }

    if flags & XLHP_HAS_NOW_UNUSED_ITEMS != 0 {
        let subrecord = cursor as *mut xlhp_prune_items;

        *nunused = (*subrecord).ntargets as c_int;
        *nowunused = (*subrecord).data.as_mut_ptr();

        cursor = cursor.add(core::mem::offset_of!(xlhp_prune_items, data));
        cursor = cursor.add(core::mem::size_of::<OffsetNumber>() * (*nunused) as usize);
    } else {
        *nunused = 0;
        *nowunused = null_mut();
    }

    *frz_offsets = cursor as *mut OffsetNumber;
}

// ---------------------------------------------------------------------------
// public desc / identify
// ---------------------------------------------------------------------------

/// heap_desc: format an RM_HEAP WAL record into `buf`.
///
/// # Safety
/// `record` is an opaque XLogReaderState pointer; the stubbed XLogRecGetData
/// data pointer is cast to the per-opcode struct. `buf` must be valid.
pub unsafe fn heap_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let mut info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if info == XLOG_HEAP_INSERT {
        let xlrec = rec as *mut xl_heap_insert;
        appendStringInfo!(
            buf,
            "off: {}, flags: 0x{:02X}",
            (*xlrec).offnum,
            (*xlrec).flags
        );
    } else if info == XLOG_HEAP_DELETE {
        let xlrec = rec as *mut xl_heap_delete;
        appendStringInfo!(buf, "xmax: {}, off: {}, ", (*xlrec).xmax, (*xlrec).offnum);
        infobits_desc(buf, (*xlrec).infobits_set, "infobits");
        appendStringInfo!(buf, ", flags: 0x{:02X}", (*xlrec).flags);
    } else if info == XLOG_HEAP_UPDATE {
        let xlrec = rec as *mut xl_heap_update;
        appendStringInfo!(
            buf,
            "old_xmax: {}, old_off: {}, ",
            (*xlrec).old_xmax,
            (*xlrec).old_offnum
        );
        infobits_desc(buf, (*xlrec).old_infobits_set, "old_infobits");
        appendStringInfo!(
            buf,
            ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
            (*xlrec).flags,
            (*xlrec).new_xmax,
            (*xlrec).new_offnum
        );
    } else if info == XLOG_HEAP_HOT_UPDATE {
        let xlrec = rec as *mut xl_heap_update;
        appendStringInfo!(
            buf,
            "old_xmax: {}, old_off: {}, ",
            (*xlrec).old_xmax,
            (*xlrec).old_offnum
        );
        infobits_desc(buf, (*xlrec).old_infobits_set, "old_infobits");
        appendStringInfo!(
            buf,
            ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
            (*xlrec).flags,
            (*xlrec).new_xmax,
            (*xlrec).new_offnum
        );
    } else if info == XLOG_HEAP_TRUNCATE {
        let xlrec = rec as *mut xl_heap_truncate;
        truncate_flags_desc(buf, (*xlrec).flags);
        appendStringInfo!(buf, ", nrelids: {}", (*xlrec).nrelids);
        appendStringInfoString(buf, c", relids:".as_ptr());
        array_desc(
            buf,
            (*xlrec).relids.as_mut_ptr() as *mut c_void,
            core::mem::size_of::<Oid>(),
            (*xlrec).nrelids as c_int,
            oid_elem_desc,
            null_mut(),
        );
    } else if info == XLOG_HEAP_CONFIRM {
        let xlrec = rec as *mut xl_heap_confirm;
        appendStringInfo!(buf, "off: {}", (*xlrec).offnum);
    } else if info == XLOG_HEAP_LOCK {
        let xlrec = rec as *mut xl_heap_lock;
        appendStringInfo!(buf, "xmax: {}, off: {}, ", (*xlrec).xmax, (*xlrec).offnum);
        infobits_desc(buf, (*xlrec).infobits_set, "infobits");
        appendStringInfo!(buf, ", flags: 0x{:02X}", (*xlrec).flags);
    } else if info == XLOG_HEAP_INPLACE {
        let xlrec = rec as *mut xl_heap_inplace;
        appendStringInfo!(buf, "off: {}", (*xlrec).offnum);
        standby_desc_invalidations(
            buf,
            (*xlrec).nmsgs,
            (*xlrec).msgs.as_mut_ptr(),
            (*xlrec).dbId,
            (*xlrec).tsId,
            (*xlrec).relcacheInitFileInval,
        );
    }
}

/// heap2_desc: format an RM_HEAP2 WAL record into `buf`.
///
/// # Safety
/// As for `heap_desc`.
pub unsafe fn heap2_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let mut info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if info == XLOG_HEAP2_PRUNE_ON_ACCESS
        || info == XLOG_HEAP2_PRUNE_VACUUM_SCAN
        || info == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
    {
        let xlrec = rec as *mut xl_heap_prune;

        if (*xlrec).flags & XLHP_HAS_CONFLICT_HORIZON != 0 {
            let mut conflict_xid: TransactionId = 0;
            core::ptr::copy_nonoverlapping(
                rec.add(SizeOfHeapPrune) as *const u8,
                &mut conflict_xid as *mut TransactionId as *mut u8,
                core::mem::size_of::<TransactionId>(),
            );
            appendStringInfo!(buf, "snapshotConflictHorizon: {}", conflict_xid);
        }

        appendStringInfo!(
            buf,
            ", isCatalogRel: {}",
            tf((*xlrec).flags & XLHP_IS_CATALOG_REL != 0)
        );

        if XLogRecHasBlockData(record, 0) {
            let mut datalen: usize = 0;
            let mut redirected: *mut OffsetNumber = null_mut();
            let mut nowdead: *mut OffsetNumber = null_mut();
            let mut nowunused: *mut OffsetNumber = null_mut();
            let mut nredirected: c_int = 0;
            let mut nunused: c_int = 0;
            let mut ndead: c_int = 0;
            let mut nplans: c_int = 0;
            let mut plans: *mut xlhp_freeze_plan = null_mut();
            let mut frz_offsets: *mut OffsetNumber = null_mut();

            let cursor = XLogRecGetBlockData(record, 0, &mut datalen);

            heap_xlog_deserialize_prune_and_freeze(
                cursor,
                (*xlrec).flags,
                &mut nplans,
                &mut plans,
                &mut frz_offsets,
                &mut nredirected,
                &mut redirected,
                &mut ndead,
                &mut nowdead,
                &mut nunused,
                &mut nowunused,
            );

            appendStringInfo!(
                buf,
                ", nplans: {}, nredirected: {}, ndead: {}, nunused: {}",
                nplans,
                nredirected,
                ndead,
                nunused
            );

            if nplans > 0 {
                appendStringInfoString(buf, c", plans:".as_ptr());
                array_desc(
                    buf,
                    plans as *mut c_void,
                    core::mem::size_of::<xlhp_freeze_plan>(),
                    nplans,
                    plan_elem_desc,
                    &mut frz_offsets as *mut *mut OffsetNumber as *mut c_void,
                );
            }

            if nredirected > 0 {
                appendStringInfoString(buf, c", redirected:".as_ptr());
                array_desc(
                    buf,
                    redirected as *mut c_void,
                    core::mem::size_of::<OffsetNumber>() * 2,
                    nredirected,
                    redirect_elem_desc,
                    null_mut(),
                );
            }

            if ndead > 0 {
                appendStringInfoString(buf, c", dead:".as_ptr());
                array_desc(
                    buf,
                    nowdead as *mut c_void,
                    core::mem::size_of::<OffsetNumber>(),
                    ndead,
                    offset_elem_desc,
                    null_mut(),
                );
            }

            if nunused > 0 {
                appendStringInfoString(buf, c", unused:".as_ptr());
                array_desc(
                    buf,
                    nowunused as *mut c_void,
                    core::mem::size_of::<OffsetNumber>(),
                    nunused,
                    offset_elem_desc,
                    null_mut(),
                );
            }
        }
    } else if info == XLOG_HEAP2_VISIBLE {
        let xlrec = rec as *mut xl_heap_visible;
        appendStringInfo!(
            buf,
            "snapshotConflictHorizon: {}, flags: 0x{:02X}",
            (*xlrec).snapshotConflictHorizon,
            (*xlrec).flags
        );
    } else if info == XLOG_HEAP2_MULTI_INSERT {
        let xlrec = rec as *mut xl_heap_multi_insert;
        let isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;

        appendStringInfo!(
            buf,
            "ntuples: {}, flags: 0x{:02X}",
            (*xlrec).ntuples as c_int,
            (*xlrec).flags
        );

        if XLogRecHasBlockData(record, 0) && !isinit {
            appendStringInfoString(buf, c", offsets:".as_ptr());
            array_desc(
                buf,
                (*xlrec).offsets.as_mut_ptr() as *mut c_void,
                core::mem::size_of::<OffsetNumber>(),
                (*xlrec).ntuples as c_int,
                offset_elem_desc,
                null_mut(),
            );
        }
    } else if info == XLOG_HEAP2_LOCK_UPDATED {
        let xlrec = rec as *mut xl_heap_lock_updated;
        appendStringInfo!(buf, "xmax: {}, off: {}, ", (*xlrec).xmax, (*xlrec).offnum);
        infobits_desc(buf, (*xlrec).infobits_set, "infobits");
        appendStringInfo!(buf, ", flags: 0x{:02X}", (*xlrec).flags);
    } else if info == XLOG_HEAP2_NEW_CID {
        let xlrec = rec as *mut xl_heap_new_cid;
        appendStringInfo!(
            buf,
            "rel: {}/{}/{}, tid: {}/{}",
            (*xlrec).target_locator.spcOid,
            (*xlrec).target_locator.dbOid,
            (*xlrec).target_locator.relNumber,
            ItemPointerGetBlockNumber(&(*xlrec).target_tid),
            ItemPointerGetOffsetNumber(&(*xlrec).target_tid)
        );
        appendStringInfo!(
            buf,
            ", cmin: {}, cmax: {}, combo: {}",
            (*xlrec).cmin,
            (*xlrec).cmax,
            (*xlrec).combocid
        );
    }
}

/// heap_identify: RM_HEAP opcode -> human-readable name (null on unknown).
/// Keeps the HIGH nibble (opcode + init bit) by masking off XLR_INFO_MASK.
pub fn heap_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        x if x == XLOG_HEAP_INSERT => b"INSERT\0",
        x if x == XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE => b"INSERT+INIT\0",
        x if x == XLOG_HEAP_DELETE => b"DELETE\0",
        x if x == XLOG_HEAP_UPDATE => b"UPDATE\0",
        x if x == XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE => b"UPDATE+INIT\0",
        x if x == XLOG_HEAP_HOT_UPDATE => b"HOT_UPDATE\0",
        x if x == XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE => b"HOT_UPDATE+INIT\0",
        x if x == XLOG_HEAP_TRUNCATE => b"TRUNCATE\0",
        x if x == XLOG_HEAP_CONFIRM => b"HEAP_CONFIRM\0",
        x if x == XLOG_HEAP_LOCK => b"LOCK\0",
        x if x == XLOG_HEAP_INPLACE => b"INPLACE\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

/// heap2_identify: RM_HEAP2 opcode -> human-readable name (null on unknown).
pub fn heap2_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        x if x == XLOG_HEAP2_PRUNE_ON_ACCESS => b"PRUNE_ON_ACCESS\0",
        x if x == XLOG_HEAP2_PRUNE_VACUUM_SCAN => b"PRUNE_VACUUM_SCAN\0",
        x if x == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => b"PRUNE_VACUUM_CLEANUP\0",
        x if x == XLOG_HEAP2_VISIBLE => b"VISIBLE\0",
        x if x == XLOG_HEAP2_MULTI_INSERT => b"MULTI_INSERT\0",
        x if x == XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE => b"MULTI_INSERT+INIT\0",
        x if x == XLOG_HEAP2_LOCK_UPDATED => b"LOCK_UPDATED\0",
        x if x == XLOG_HEAP2_NEW_CID => b"NEW_CID\0",
        x if x == XLOG_HEAP2_REWRITE => b"REWRITE\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn cstr<'a>(p: *const c_char) -> &'a str {
        core::ffi::CStr::from_ptr(p).to_str().unwrap()
    }

    #[test]
    fn heap_identify_table() {
        unsafe {
            assert_eq!(cstr(heap_identify(XLOG_HEAP_INSERT)), "INSERT");
            assert_eq!(
                cstr(heap_identify(XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE)),
                "INSERT+INIT"
            );
            assert_eq!(cstr(heap_identify(XLOG_HEAP_DELETE)), "DELETE");
            assert_eq!(cstr(heap_identify(XLOG_HEAP_UPDATE)), "UPDATE");
            assert_eq!(
                cstr(heap_identify(XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE)),
                "UPDATE+INIT"
            );
            assert_eq!(cstr(heap_identify(XLOG_HEAP_HOT_UPDATE)), "HOT_UPDATE");
            assert_eq!(
                cstr(heap_identify(XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE)),
                "HOT_UPDATE+INIT"
            );
            assert_eq!(cstr(heap_identify(XLOG_HEAP_TRUNCATE)), "TRUNCATE");
            assert_eq!(cstr(heap_identify(XLOG_HEAP_CONFIRM)), "HEAP_CONFIRM");
            assert_eq!(cstr(heap_identify(XLOG_HEAP_LOCK)), "LOCK");
            assert_eq!(cstr(heap_identify(XLOG_HEAP_INPLACE)), "INPLACE");
        }
    }

    #[test]
    fn heap_identify_flags_and_unknown() {
        // A record-with-flags is OPCODE | 0x0F (low nibble); identify masks
        // those off and still resolves the opcode.
        unsafe {
            assert_eq!(cstr(heap_identify(XLOG_HEAP_INSERT | 0x0F)), "INSERT");
            assert_eq!(cstr(heap_identify(XLOG_HEAP_LOCK | 0x0F)), "LOCK");
        }
        // 0x90 is a high-nibble opcode RM_HEAP does not define -> null.
        assert!(heap_identify(0x90).is_null());
    }

    #[test]
    fn heap2_identify_table() {
        unsafe {
            assert_eq!(
                cstr(heap2_identify(XLOG_HEAP2_PRUNE_ON_ACCESS)),
                "PRUNE_ON_ACCESS"
            );
            assert_eq!(
                cstr(heap2_identify(XLOG_HEAP2_PRUNE_VACUUM_SCAN)),
                "PRUNE_VACUUM_SCAN"
            );
            assert_eq!(
                cstr(heap2_identify(XLOG_HEAP2_PRUNE_VACUUM_CLEANUP)),
                "PRUNE_VACUUM_CLEANUP"
            );
            assert_eq!(cstr(heap2_identify(XLOG_HEAP2_VISIBLE)), "VISIBLE");
            assert_eq!(cstr(heap2_identify(XLOG_HEAP2_MULTI_INSERT)), "MULTI_INSERT");
            assert_eq!(
                cstr(heap2_identify(XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE)),
                "MULTI_INSERT+INIT"
            );
            assert_eq!(cstr(heap2_identify(XLOG_HEAP2_LOCK_UPDATED)), "LOCK_UPDATED");
            assert_eq!(cstr(heap2_identify(XLOG_HEAP2_NEW_CID)), "NEW_CID");
            assert_eq!(cstr(heap2_identify(XLOG_HEAP2_REWRITE)), "REWRITE");
        }
        // REWRITE is opcode 0x00; an unknown high nibble like 0x80 (INIT bit
        // alone, no opcode) is not in the heap2 table -> null.
        assert!(heap2_identify(0x80).is_null());
    }

    #[test]
    fn wal_struct_layouts() {
        use core::mem::{align_of, offset_of, size_of};

        // xl_heap_delete: xmax(4) off(2) infobits(1) flags(1) = 8, 4-align.
        assert_eq!(offset_of!(xl_heap_delete, xmax), 0);
        assert_eq!(offset_of!(xl_heap_delete, offnum), 4);
        assert_eq!(offset_of!(xl_heap_delete, infobits_set), 6);
        assert_eq!(offset_of!(xl_heap_delete, flags), 7);
        assert_eq!(size_of::<xl_heap_delete>(), 8);

        // xl_heap_insert: offnum(2) flags(1) -> SizeOfHeapInsert == 3.
        assert_eq!(offset_of!(xl_heap_insert, offnum), 0);
        assert_eq!(offset_of!(xl_heap_insert, flags), 2);

        // xl_heap_update field offsets (4-byte aligned TransactionIds).
        assert_eq!(offset_of!(xl_heap_update, old_xmax), 0);
        assert_eq!(offset_of!(xl_heap_update, old_offnum), 4);
        assert_eq!(offset_of!(xl_heap_update, old_infobits_set), 6);
        assert_eq!(offset_of!(xl_heap_update, flags), 7);
        assert_eq!(offset_of!(xl_heap_update, new_xmax), 8);
        assert_eq!(offset_of!(xl_heap_update, new_offnum), 12);

        // xlhp_freeze_plan: xmax(4) infomask2(2) infomask(2) frzflags(1)
        // ntuples(2) -> 4-align, internal pad after frzflags.
        assert_eq!(offset_of!(xlhp_freeze_plan, xmax), 0);
        assert_eq!(offset_of!(xlhp_freeze_plan, t_infomask2), 4);
        assert_eq!(offset_of!(xlhp_freeze_plan, t_infomask), 6);
        assert_eq!(offset_of!(xlhp_freeze_plan, frzflags), 8);
        assert_eq!(offset_of!(xlhp_freeze_plan, ntuples), 10);

        // xl_heap_visible: TransactionId then a uint8 flag.
        assert_eq!(offset_of!(xl_heap_visible, snapshotConflictHorizon), 0);
        assert_eq!(offset_of!(xl_heap_visible, flags), 4);

        // SizeOfHeapPrune is the fixed 2-byte header (reason + flags).
        assert_eq!(SizeOfHeapPrune, 2);
        assert_eq!(size_of::<xl_heap_prune>(), 2);

        // xlhp_prune_items flex header: data[] starts right after ntargets.
        assert_eq!(offset_of!(xlhp_prune_items, data), 2);
        assert!(align_of::<xlhp_freeze_plan>() >= 4);
    }
}
