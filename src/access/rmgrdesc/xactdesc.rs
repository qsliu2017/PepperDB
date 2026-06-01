//! Translation of postgres/src/backend/access/rmgrdesc/xactdesc.c
//!                + the WAL record structs / parsed structs / XLOG_XACT_*
//!                  opcodes / XACT_XINFO_* flags merged in from
//!                  postgres/src/include/access/xact.h.
//!
//! rmgr descriptor routines for the Transaction Rmgr (RM_XACT_ID). These
//! routines are shared between the backend (WAL replay) and frontend
//! (pg_waldump), which is why the variable-length commit/abort/prepare WAL
//! formats are decoded here into easier-to-consume xl_xact_parsed_* structs.
//!
//! Header mapping:
//!   access/xact.h            -> the xl_xact_* WAL record structs, the
//!                               xl_xact_parsed_* deconstructed structs, the
//!                               XLOG_XACT_* opcodes, the XLOG_XACT_HAS_INFO /
//!                               XLOG_XACT_OPMASK info-byte masks, the
//!                               XACT_XINFO_* / XACT_COMPLETION_* xinfo flags,
//!                               and the Min/MinSizeOf* offsets.
//!   access/transam.h         -> TransactionId, TransactionIdIsValid (reused
//!                               from crate::access::transam).
//!   storage/sinval.h         -> SharedInvalidationMessage (reused from
//!                               crate::access::rmgrdesc::standbydesc).
//!   storage/relfilelocator.h -> RelFileLocator (reused from standbydesc).
//!   storage/standbydefs.h    -> standby_desc_invalidations (reused).
//!   replication/origin.h     -> RepOriginId, InvalidRepOriginId.
//!   utils/timestamp.h        -> TimestampTz, timestamptz_to_str (STUB).
//!   common/relpath.h         -> GetRelationPath (relpathperm), MAIN_FORKNUM.
//!   lib/stringinfo.h         -> StringInfo, appendStringInfo!, append*String.
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`).
//!   - XLogRecGetData / XLogRecGetInfo / XLogRecGetOrigin: stubbed to return
//!     null / 0 / 0 with a TODO. The desc body reads its record from the
//!     stubbed pointer, so it compiles and is runtime-stubbed until a real
//!     reader feeds it real bytes.
//!   - timestamptz_to_str: utils/adt/timestamp.c not ported; stubbed to a fixed
//!     placeholder c-string. TODO: wire to the real formatter once ported.
//!
//! The struct layouts, the XLOG_XACT_* opcode values, the XACT_XINFO_* flag
//! values, the Parse* pointer-walks, and the xact_identify name table are REAL
//! (faithful to xact.h / xactdesc.c). The desc output text reproduces the C
//! output exactly (same labels, same order).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::rmgrdesc::standbydesc::{
    standby_desc_invalidations, RelFileLocator, SharedInvalidationMessage,
};
use crate::access::transam::TransactionIdIsValid;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLogRecGetOrigin, XLR_INFO_MASK,
};
use crate::common::relpath::{GetRelationPath, MAIN_FORKNUM};
use crate::lib::stringinfo::{appendStringInfoString, StringInfo};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (c.h / access/transam.h / datatype/timestamp.h / origin.h)
// ---------------------------------------------------------------------------

pub type TransactionId = uint32;
pub type TimestampTz = int64;
pub type XLogRecPtr = uint64;

/// Replication-origin identifier (replication/origin.h: typedef uint16).
pub type RepOriginId = uint16;
/// InvalidRepOriginId (replication/origin.h).
pub const InvalidRepOriginId: RepOriginId = 0;

/// INVALID_PROC_NUMBER (storage/procnumber.h): no temp-rel backend. Private in
/// the (not-yet-ported) header, redeclared with the same value for relpathperm.
const INVALID_PROC_NUMBER: c_int = -1;

/// Maximum size of Global Transaction ID (including '\0') (access/xact.h).
pub const GIDSIZE: usize = 200;

/// STUB: format a TimestampTz to a string (utils/adt/timestamp.c not ported).
/// Returns a fixed placeholder c-string. TODO: wire to the real formatter.
///
/// Returns a `*const c_char` (matches the C signature) so the desc routines can
/// hand it straight to appendStringInfoString.
pub fn timestamptz_to_str(_dt: TimestampTz) -> *const c_char {
    // TODO: real impl in utils/adt/timestamp.c (not ported).
    c"(timestamp)".as_ptr()
}

// ---------------------------------------------------------------------------
// Transaction-related XLOG opcodes (access/xact.h)
// ---------------------------------------------------------------------------
//
// XLOG allows storing info in the high 4 bits of xl_info: 3 bits for the
// opcode and 1 for the optional-flag (xinfo) variable.

pub const XLOG_XACT_COMMIT: uint8 = 0x00;
pub const XLOG_XACT_PREPARE: uint8 = 0x10;
pub const XLOG_XACT_ABORT: uint8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: uint8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: uint8 = 0x40;
pub const XLOG_XACT_ASSIGNMENT: uint8 = 0x50;
pub const XLOG_XACT_INVALIDATIONS: uint8 = 0x60;
/* free opcode 0x70 */

/// mask for filtering opcodes out of xl_info
pub const XLOG_XACT_OPMASK: uint8 = 0x70;

/// does this record have a 'xinfo' field or not
pub const XLOG_XACT_HAS_INFO: uint8 = 0x80;

// ---------------------------------------------------------------------------
// xinfo flags (access/xact.h): which optional sections are present
// ---------------------------------------------------------------------------

pub const XACT_XINFO_HAS_DBINFO: uint32 = 1 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: uint32 = 1 << 1;
pub const XACT_XINFO_HAS_RELFILELOCATORS: uint32 = 1 << 2;
pub const XACT_XINFO_HAS_INVALS: uint32 = 1 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: uint32 = 1 << 4;
pub const XACT_XINFO_HAS_ORIGIN: uint32 = 1 << 5;
pub const XACT_XINFO_HAS_AE_LOCKS: uint32 = 1 << 6;
pub const XACT_XINFO_HAS_GID: uint32 = 1 << 7;
pub const XACT_XINFO_HAS_DROPPED_STATS: uint32 = 1 << 8;

// xinfo flags signalling additional recovery actions.
pub const XACT_COMPLETION_APPLY_FEEDBACK: uint32 = 1 << 29;
pub const XACT_COMPLETION_UPDATE_RELCACHE_FILE: uint32 = 1 << 30;
pub const XACT_COMPLETION_FORCE_SYNC_COMMIT: uint32 = 1 << 31;

/// XactCompletionApplyFeedback(xinfo) (access/xact.h).
#[inline]
pub fn XactCompletionApplyFeedback(xinfo: uint32) -> bool {
    (xinfo & XACT_COMPLETION_APPLY_FEEDBACK) != 0
}

/// XactCompletionRelcacheInitFileInval(xinfo) (access/xact.h).
#[inline]
pub fn XactCompletionRelcacheInitFileInval(xinfo: uint32) -> bool {
    (xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE) != 0
}

/// XactCompletionForceSyncCommit(xinfo) (access/xact.h).
#[inline]
pub fn XactCompletionForceSyncCommit(xinfo: uint32) -> bool {
    (xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT) != 0
}

// ---------------------------------------------------------------------------
// WAL record structs (access/xact.h)
// ---------------------------------------------------------------------------

/// xl_xact_assignment: top-level XID plus a FLEXIBLE_ARRAY_MEMBER `xsub[]` of
/// assigned subxact XIDs.
#[repr(C)]
pub struct xl_xact_assignment {
    pub xtop: TransactionId, /* assigned XID's top-level XID */
    pub nsubxacts: c_int,    /* number of subtransaction XIDs */
    pub xsub: [TransactionId; 0], /* assigned subxids */
}

/// MinSizeOfXactAssignment = offsetof(xl_xact_assignment, xsub).
pub const MinSizeOfXactAssignment: usize = core::mem::offset_of!(xl_xact_assignment, xsub);

/// xl_xact_xinfo: the optional xinfo flag word. Uses 4 bytes even though only
/// two are required, so following records don't have to care about alignment.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_xinfo {
    pub xinfo: uint32,
}

/// xl_xact_dbinfo: database/tablespace OIDs (XACT_XINFO_HAS_DBINFO).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_dbinfo {
    pub dbId: Oid, /* MyDatabaseId */
    pub tsId: Oid, /* MyDatabaseTableSpace */
}

/// xl_xact_subxacts: `nsubxacts` followed by FLEXIBLE_ARRAY_MEMBER `subxacts[]`.
#[repr(C)]
pub struct xl_xact_subxacts {
    pub nsubxacts: c_int, /* number of subtransaction XIDs */
    pub subxacts: [TransactionId; 0],
}
/// MinSizeOfXactSubxacts = offsetof(xl_xact_subxacts, subxacts).
pub const MinSizeOfXactSubxacts: usize = core::mem::offset_of!(xl_xact_subxacts, subxacts);

/// xl_xact_relfilelocators: `nrels` followed by FLEXIBLE_ARRAY_MEMBER
/// `xlocators[]`.
#[repr(C)]
pub struct xl_xact_relfilelocators {
    pub nrels: c_int, /* number of relations */
    pub xlocators: [RelFileLocator; 0],
}
/// MinSizeOfXactRelfileLocators = offsetof(xl_xact_relfilelocators, xlocators).
pub const MinSizeOfXactRelfileLocators: usize =
    core::mem::offset_of!(xl_xact_relfilelocators, xlocators);

/// xl_xact_stats_item: a transactionally dropped statistics entry. Declared in
/// xact.h (rather than pgstat.h) so the WAL format is readable by frontends.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_stats_item {
    pub kind: c_int,
    pub dboid: Oid,
    /// PgStat_HashKey.objid, stored as two uint32 so every field of the
    /// surrounding records is a multiple of sizeof(int).
    pub objid_lo: uint32,
    pub objid_hi: uint32,
}

/// xl_xact_stats_items: `nitems` followed by FLEXIBLE_ARRAY_MEMBER `items[]`.
#[repr(C)]
pub struct xl_xact_stats_items {
    pub nitems: c_int,
    pub items: [xl_xact_stats_item; 0],
}
/// MinSizeOfXactStatsItems = offsetof(xl_xact_stats_items, items).
pub const MinSizeOfXactStatsItems: usize = core::mem::offset_of!(xl_xact_stats_items, items);

/// xl_xact_invals: `nmsgs` followed by FLEXIBLE_ARRAY_MEMBER `msgs[]`.
#[repr(C)]
pub struct xl_xact_invals {
    pub nmsgs: c_int, /* number of shared inval msgs */
    pub msgs: [SharedInvalidationMessage; 0],
}
/// MinSizeOfXactInvals = offsetof(xl_xact_invals, msgs).
pub const MinSizeOfXactInvals: usize = core::mem::offset_of!(xl_xact_invals, msgs);

/// xl_xact_twophase: the original transaction's XID (XACT_XINFO_HAS_TWOPHASE).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_twophase {
    pub xid: TransactionId,
}

/// xl_xact_origin: replication-origin LSN/timestamp. Stored UNALIGNED in the
/// WAL stream, so it must be memcpy'd onto the stack before use.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_origin {
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// xl_xact_commit: a minimal commit record is just `xact_time`; the optional
/// sub-records follow in the WAL payload as indicated by the xinfo flags.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_commit {
    pub xact_time: TimestampTz, /* time of commit */
                                /* optional sub-records follow per xinfo */
}
/// MinSizeOfXactCommit = offsetof(xl_xact_commit, xact_time) +
/// sizeof(TimestampTz).
pub const MinSizeOfXactCommit: usize =
    core::mem::offset_of!(xl_xact_commit, xact_time) + core::mem::size_of::<TimestampTz>();

/// xl_xact_abort: a minimal abort record is just `xact_time`; optional
/// sub-records follow per the xinfo flags (no invalidation messages).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_abort {
    pub xact_time: TimestampTz, /* time of abort */
}
/// MinSizeOfXactAbort = sizeof(xl_xact_abort).
pub const MinSizeOfXactAbort: usize = core::mem::size_of::<xl_xact_abort>();

/// xl_xact_prepare: fixed header of a PREPARE record. The GID, subxact XIDs,
/// commit/abort rel locators, commit/abort stats and inval msgs follow it in
/// the WAL payload (each MAXALIGN-padded).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_xact_prepare {
    pub magic: uint32,       /* format identifier */
    pub total_len: uint32,   /* actual file length */
    pub xid: TransactionId,  /* original transaction XID */
    pub database: Oid,       /* OID of database it was in */
    pub prepared_at: TimestampTz, /* time of preparation */
    pub owner: Oid,          /* user running the transaction */
    pub nsubxacts: int32,    /* number of following subxact XIDs */
    pub ncommitrels: int32,  /* number of delete-on-commit rels */
    pub nabortrels: int32,   /* number of delete-on-abort rels */
    pub ncommitstats: int32, /* number of stats to drop on commit */
    pub nabortstats: int32,  /* number of stats to drop on abort */
    pub ninvalmsgs: int32,   /* number of cache invalidation messages */
    pub initfileinval: bool, /* does relcache init file need invalidation? */
    pub gidlen: uint16,      /* length of the GID - GID follows the header */
    pub origin_lsn: XLogRecPtr, /* lsn of this record at origin node */
    pub origin_timestamp: TimestampTz, /* time of prepare at origin node */
}

// ---------------------------------------------------------------------------
// Deconstructed (parsed) commit/abort/prepare structs (access/xact.h)
// ---------------------------------------------------------------------------

/// xl_xact_parsed_commit: easier-to-consume form produced by
/// ParseCommitRecord(). xl_xact_parsed_prepare is a typedef alias of this.
#[repr(C)]
pub struct xl_xact_parsed_commit {
    pub xact_time: TimestampTz,
    pub xinfo: uint32,

    pub dbId: Oid, /* MyDatabaseId */
    pub tsId: Oid, /* MyDatabaseTableSpace */

    pub nsubxacts: c_int,
    pub subxacts: *mut TransactionId,

    pub nrels: c_int,
    pub xlocators: *mut RelFileLocator,

    pub nstats: c_int,
    pub stats: *mut xl_xact_stats_item,

    pub nmsgs: c_int,
    pub msgs: *mut SharedInvalidationMessage,

    pub twophase_xid: TransactionId, /* only for 2PC */
    pub twophase_gid: [c_char; GIDSIZE], /* only for 2PC */
    pub nabortrels: c_int,           /* only for 2PC */
    pub abortlocators: *mut RelFileLocator, /* only for 2PC */
    pub nabortstats: c_int,          /* only for 2PC */
    pub abortstats: *mut xl_xact_stats_item, /* only for 2PC */

    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// xl_xact_parsed_prepare == xl_xact_parsed_commit (typedef in xact.h).
pub type xl_xact_parsed_prepare = xl_xact_parsed_commit;

/// xl_xact_parsed_abort: easier-to-consume form produced by ParseAbortRecord().
#[repr(C)]
pub struct xl_xact_parsed_abort {
    pub xact_time: TimestampTz,
    pub xinfo: uint32,

    pub dbId: Oid, /* MyDatabaseId */
    pub tsId: Oid, /* MyDatabaseTableSpace */

    pub nsubxacts: c_int,
    pub subxacts: *mut TransactionId,

    pub nrels: c_int,
    pub xlocators: *mut RelFileLocator,

    pub nstats: c_int,
    pub stats: *mut xl_xact_stats_item,

    pub twophase_xid: TransactionId, /* only for 2PC */
    pub twophase_gid: [c_char; GIDSIZE], /* only for 2PC */

    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

// ---------------------------------------------------------------------------
// Parse routines: decode the variable WAL layout into a parsed_* struct
// ---------------------------------------------------------------------------

/// LSN_FORMAT_ARGS(lsn): split a 64-bit LSN into (high uint32, low uint32) for
/// the "%X/%X" rendering (access/xlogdefs.h).
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (uint32, uint32) {
    ((lsn >> 32) as uint32, lsn as uint32)
}

/// strlcpy semantics: copy a NUL-terminated C string from `src` into `dst`,
/// truncating to at most dstsize-1 bytes and always NUL-terminating.
unsafe fn strlcpy_into(dst: &mut [c_char], src: *const c_char) {
    if dst.is_empty() {
        return;
    }
    let cap = dst.len() - 1;
    let mut i = 0usize;
    while i < cap {
        let ch = *src.add(i);
        if ch == 0 {
            break;
        }
        dst[i] = ch;
        i += 1;
    }
    dst[i] = 0;
}

/// ParseCommitRecord: faithful pointer-walk of the xinfo-gated sections of a
/// commit (or commit-prepared) WAL record into `parsed`.
///
/// # Safety
/// `xlrec` must point to a valid xl_xact_commit followed by the optional
/// sections indicated by `info`/xinfo; `parsed` must be a valid out pointer.
pub unsafe fn ParseCommitRecord(
    info: uint8,
    xlrec: *mut xl_xact_commit,
    parsed: *mut xl_xact_parsed_commit,
) {
    let mut data = (xlrec as *mut c_char).add(MinSizeOfXactCommit);

    core::ptr::write_bytes(parsed as *mut u8, 0, core::mem::size_of::<xl_xact_parsed_commit>());

    /* default, if no XLOG_XACT_HAS_INFO is present */
    (*parsed).xinfo = 0;

    (*parsed).xact_time = (*xlrec).xact_time;

    if info & XLOG_XACT_HAS_INFO != 0 {
        let xl_xinfo = data as *mut xl_xact_xinfo;
        (*parsed).xinfo = (*xl_xinfo).xinfo;
        data = data.add(core::mem::size_of::<xl_xact_xinfo>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        let xl_dbinfo = data as *mut xl_xact_dbinfo;
        (*parsed).dbId = (*xl_dbinfo).dbId;
        (*parsed).tsId = (*xl_dbinfo).tsId;
        data = data.add(core::mem::size_of::<xl_xact_dbinfo>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let xl_subxacts = data as *mut xl_xact_subxacts;
        (*parsed).nsubxacts = (*xl_subxacts).nsubxacts;
        (*parsed).subxacts = (*xl_subxacts).subxacts.as_mut_ptr();
        data = data.add(MinSizeOfXactSubxacts);
        data = data.add((*parsed).nsubxacts as usize * core::mem::size_of::<TransactionId>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        let xl_rellocators = data as *mut xl_xact_relfilelocators;
        (*parsed).nrels = (*xl_rellocators).nrels;
        (*parsed).xlocators = (*xl_rellocators).xlocators.as_mut_ptr();
        data = data.add(MinSizeOfXactRelfileLocators);
        data =
            data.add((*xl_rellocators).nrels as usize * core::mem::size_of::<RelFileLocator>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_DROPPED_STATS != 0 {
        let xl_drops = data as *mut xl_xact_stats_items;
        (*parsed).nstats = (*xl_drops).nitems;
        (*parsed).stats = (*xl_drops).items.as_mut_ptr();
        data = data.add(MinSizeOfXactStatsItems);
        data = data.add((*xl_drops).nitems as usize * core::mem::size_of::<xl_xact_stats_item>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_INVALS != 0 {
        let xl_invals = data as *mut xl_xact_invals;
        (*parsed).nmsgs = (*xl_invals).nmsgs;
        (*parsed).msgs = (*xl_invals).msgs.as_mut_ptr();
        data = data.add(MinSizeOfXactInvals);
        data = data
            .add((*xl_invals).nmsgs as usize * core::mem::size_of::<SharedInvalidationMessage>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        let xl_twophase = data as *mut xl_xact_twophase;
        (*parsed).twophase_xid = (*xl_twophase).xid;
        data = data.add(core::mem::size_of::<xl_xact_twophase>());

        if (*parsed).xinfo & XACT_XINFO_HAS_GID != 0 {
            strlcpy_into(&mut (*parsed).twophase_gid, data);
            data = data.add(strlen_c(data) + 1);
        }
    }

    /* Note: no alignment is guaranteed after this point */

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        /* no alignment is guaranteed, so copy onto stack */
        let mut xl_origin: xl_xact_origin = core::mem::zeroed();
        core::ptr::copy_nonoverlapping(
            data,
            &mut xl_origin as *mut xl_xact_origin as *mut c_char,
            core::mem::size_of::<xl_xact_origin>(),
        );
        (*parsed).origin_lsn = xl_origin.origin_lsn;
        (*parsed).origin_timestamp = xl_origin.origin_timestamp;
        data = data.add(core::mem::size_of::<xl_xact_origin>());
    }

    /* silence unused-assignment on the final advance */
    let _ = data;
}

/// ParseAbortRecord: faithful pointer-walk of the xinfo-gated sections of an
/// abort (or abort-prepared) WAL record into `parsed`.
///
/// # Safety
/// `xlrec` must point to a valid xl_xact_abort followed by the optional
/// sections indicated by `info`/xinfo; `parsed` must be a valid out pointer.
pub unsafe fn ParseAbortRecord(
    info: uint8,
    xlrec: *mut xl_xact_abort,
    parsed: *mut xl_xact_parsed_abort,
) {
    let mut data = (xlrec as *mut c_char).add(MinSizeOfXactAbort);

    core::ptr::write_bytes(parsed as *mut u8, 0, core::mem::size_of::<xl_xact_parsed_abort>());

    /* default, if no XLOG_XACT_HAS_INFO is present */
    (*parsed).xinfo = 0;

    (*parsed).xact_time = (*xlrec).xact_time;

    if info & XLOG_XACT_HAS_INFO != 0 {
        let xl_xinfo = data as *mut xl_xact_xinfo;
        (*parsed).xinfo = (*xl_xinfo).xinfo;
        data = data.add(core::mem::size_of::<xl_xact_xinfo>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        let xl_dbinfo = data as *mut xl_xact_dbinfo;
        (*parsed).dbId = (*xl_dbinfo).dbId;
        (*parsed).tsId = (*xl_dbinfo).tsId;
        data = data.add(core::mem::size_of::<xl_xact_dbinfo>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let xl_subxacts = data as *mut xl_xact_subxacts;
        (*parsed).nsubxacts = (*xl_subxacts).nsubxacts;
        (*parsed).subxacts = (*xl_subxacts).subxacts.as_mut_ptr();
        data = data.add(MinSizeOfXactSubxacts);
        data = data.add((*parsed).nsubxacts as usize * core::mem::size_of::<TransactionId>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        let xl_rellocator = data as *mut xl_xact_relfilelocators;
        (*parsed).nrels = (*xl_rellocator).nrels;
        (*parsed).xlocators = (*xl_rellocator).xlocators.as_mut_ptr();
        data = data.add(MinSizeOfXactRelfileLocators);
        data =
            data.add((*xl_rellocator).nrels as usize * core::mem::size_of::<RelFileLocator>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_DROPPED_STATS != 0 {
        let xl_drops = data as *mut xl_xact_stats_items;
        (*parsed).nstats = (*xl_drops).nitems;
        (*parsed).stats = (*xl_drops).items.as_mut_ptr();
        data = data.add(MinSizeOfXactStatsItems);
        data = data.add((*xl_drops).nitems as usize * core::mem::size_of::<xl_xact_stats_item>());
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        let xl_twophase = data as *mut xl_xact_twophase;
        (*parsed).twophase_xid = (*xl_twophase).xid;
        data = data.add(core::mem::size_of::<xl_xact_twophase>());

        if (*parsed).xinfo & XACT_XINFO_HAS_GID != 0 {
            strlcpy_into(&mut (*parsed).twophase_gid, data);
            data = data.add(strlen_c(data) + 1);
        }
    }

    /* Note: no alignment is guaranteed after this point */

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        /* no alignment is guaranteed, so copy onto stack */
        let mut xl_origin: xl_xact_origin = core::mem::zeroed();
        core::ptr::copy_nonoverlapping(
            data,
            &mut xl_origin as *mut xl_xact_origin as *mut c_char,
            core::mem::size_of::<xl_xact_origin>(),
        );
        (*parsed).origin_lsn = xl_origin.origin_lsn;
        (*parsed).origin_timestamp = xl_origin.origin_timestamp;
        data = data.add(core::mem::size_of::<xl_xact_origin>());
    }

    let _ = data;
}

/// ParsePrepareRecord: decode the MAXALIGN-padded sections that trail the
/// fixed xl_xact_prepare header into `parsed`.
///
/// # Safety
/// `xlrec` must point to a valid xl_xact_prepare followed by its padded GID /
/// subxact / rel-locator / stats / inval sections; `parsed` must be valid.
pub unsafe fn ParsePrepareRecord(
    _info: uint8,
    xlrec: *mut xl_xact_prepare,
    parsed: *mut xl_xact_parsed_prepare,
) {
    let mut bufptr = (xlrec as *mut c_char).add(MAXALIGN(core::mem::size_of::<xl_xact_prepare>()));

    core::ptr::write_bytes(parsed as *mut u8, 0, core::mem::size_of::<xl_xact_parsed_prepare>());

    (*parsed).xact_time = (*xlrec).prepared_at;
    (*parsed).origin_lsn = (*xlrec).origin_lsn;
    (*parsed).origin_timestamp = (*xlrec).origin_timestamp;
    (*parsed).twophase_xid = (*xlrec).xid;
    (*parsed).dbId = (*xlrec).database;
    (*parsed).nsubxacts = (*xlrec).nsubxacts;
    (*parsed).nrels = (*xlrec).ncommitrels;
    (*parsed).nabortrels = (*xlrec).nabortrels;
    (*parsed).nstats = (*xlrec).ncommitstats;
    (*parsed).nabortstats = (*xlrec).nabortstats;
    (*parsed).nmsgs = (*xlrec).ninvalmsgs;

    /* strncpy of exactly gidlen bytes (no implicit NUL beyond what's copied) */
    {
        let gidlen = (*xlrec).gidlen as usize;
        let dst = (*parsed).twophase_gid.as_mut_ptr();
        for i in 0..gidlen.min(GIDSIZE) {
            *dst.add(i) = *bufptr.add(i);
        }
    }
    bufptr = bufptr.add(MAXALIGN((*xlrec).gidlen as usize));

    (*parsed).subxacts = bufptr as *mut TransactionId;
    bufptr = bufptr.add(MAXALIGN(
        (*xlrec).nsubxacts as usize * core::mem::size_of::<TransactionId>(),
    ));

    (*parsed).xlocators = bufptr as *mut RelFileLocator;
    bufptr = bufptr.add(MAXALIGN(
        (*xlrec).ncommitrels as usize * core::mem::size_of::<RelFileLocator>(),
    ));

    (*parsed).abortlocators = bufptr as *mut RelFileLocator;
    bufptr = bufptr.add(MAXALIGN(
        (*xlrec).nabortrels as usize * core::mem::size_of::<RelFileLocator>(),
    ));

    (*parsed).stats = bufptr as *mut xl_xact_stats_item;
    bufptr = bufptr.add(MAXALIGN(
        (*xlrec).ncommitstats as usize * core::mem::size_of::<xl_xact_stats_item>(),
    ));

    (*parsed).abortstats = bufptr as *mut xl_xact_stats_item;
    bufptr = bufptr.add(MAXALIGN(
        (*xlrec).nabortstats as usize * core::mem::size_of::<xl_xact_stats_item>(),
    ));

    (*parsed).msgs = bufptr as *mut SharedInvalidationMessage;
    bufptr = bufptr.add(MAXALIGN(
        (*xlrec).ninvalmsgs as usize * core::mem::size_of::<SharedInvalidationMessage>(),
    ));

    let _ = bufptr;
}

/// strlen for a *const c_char (libc strlen substitute).
unsafe fn strlen_c(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// ---------------------------------------------------------------------------
// desc helpers
// ---------------------------------------------------------------------------

/// xact_desc_relations: append "; <label>:" plus the relpath of each locator.
///
/// # Safety
/// `xlocators` must point to `nrels` valid RelFileLocator entries.
unsafe fn xact_desc_relations(
    buf: StringInfo,
    label: *const c_char,
    nrels: c_int,
    xlocators: *mut RelFileLocator,
) {
    if nrels > 0 {
        // C: appendStringInfo(buf, "; %s:", label) - label is a *const c_char.
        appendStringInfoString(buf, c"; ".as_ptr());
        appendStringInfoString(buf, label);
        appendStringInfoString(buf, c":".as_ptr());
        for i in 0..nrels {
            let loc = &*xlocators.add(i as usize);
            // relpathperm(loc, MAIN_FORKNUM) == GetRelationPath(dbOid, spcOid,
            //   relNumber, INVALID_PROC_NUMBER, MAIN_FORKNUM).str
            let path = GetRelationPath(
                loc.dbOid,
                loc.spcOid,
                loc.relNumber,
                INVALID_PROC_NUMBER,
                MAIN_FORKNUM,
            );
            appendStringInfoString(buf, c" ".as_ptr());
            appendStringInfoString(buf, path.str.as_ptr());
        }
    }
}

/// xact_desc_subxacts: append "; subxacts:" plus each subxact XID.
///
/// # Safety
/// `subxacts` must point to `nsubxacts` valid TransactionId entries.
unsafe fn xact_desc_subxacts(buf: StringInfo, nsubxacts: c_int, subxacts: *mut TransactionId) {
    if nsubxacts > 0 {
        appendStringInfoString(buf, c"; subxacts:".as_ptr());
        for i in 0..nsubxacts {
            appendStringInfo!(buf, " {}", *subxacts.add(i as usize));
        }
    }
}

/// xact_desc_stats: append "; <label>dropped stats:" plus each dropped entry.
///
/// # Safety
/// `dropped_stats` must point to `ndropped` valid xl_xact_stats_item entries.
unsafe fn xact_desc_stats(
    buf: StringInfo,
    label: *const c_char,
    ndropped: c_int,
    dropped_stats: *mut xl_xact_stats_item,
) {
    if ndropped > 0 {
        appendStringInfoString(buf, c"; ".as_ptr());
        appendStringInfoString(buf, label);
        appendStringInfoString(buf, c"dropped stats:".as_ptr());
        for i in 0..ndropped {
            let it = &*dropped_stats.add(i as usize);
            let objid: uint64 = ((it.objid_hi as uint64) << 32) | it.objid_lo as uint64;
            // C: appendStringInfo(buf, " %d/%u/%" PRIu64, kind, dboid, objid)
            appendStringInfo!(buf, " {}/{}/{}", it.kind, it.dboid, objid);
        }
    }
}

/// xact_desc_commit: parse + render a commit (or commit-prepared) record.
///
/// # Safety
/// `xlrec` must point to a valid commit record; `buf` valid StringInfo.
unsafe fn xact_desc_commit(
    buf: StringInfo,
    info: uint8,
    xlrec: *mut xl_xact_commit,
    origin_id: RepOriginId,
) {
    let mut parsed: xl_xact_parsed_commit = core::mem::zeroed();
    ParseCommitRecord(info, xlrec, &mut parsed);

    /* If this is a prepared xact, show the xid of the original xact */
    if TransactionIdIsValid(parsed.twophase_xid) {
        appendStringInfo!(buf, "{}: ", parsed.twophase_xid);
    }

    appendStringInfoString(buf, timestamptz_to_str((*xlrec).xact_time));

    xact_desc_relations(buf, c"rels".as_ptr(), parsed.nrels, parsed.xlocators);
    xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts);
    xact_desc_stats(buf, c"".as_ptr(), parsed.nstats, parsed.stats);

    standby_desc_invalidations(
        buf,
        parsed.nmsgs,
        parsed.msgs,
        parsed.dbId,
        parsed.tsId,
        XactCompletionRelcacheInitFileInval(parsed.xinfo),
    );

    if XactCompletionApplyFeedback(parsed.xinfo) {
        appendStringInfoString(buf, c"; apply_feedback".as_ptr());
    }

    if XactCompletionForceSyncCommit(parsed.xinfo) {
        appendStringInfoString(buf, c"; sync".as_ptr());
    }

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        let (lsn_hi, lsn_lo) = LSN_FORMAT_ARGS(parsed.origin_lsn);
        // C: "; origin: node %u, lsn %X/%X, at %s"
        appendStringInfo!(buf, "; origin: node {}, lsn {:X}/{:X}, at ", origin_id, lsn_hi, lsn_lo);
        appendStringInfoString(buf, timestamptz_to_str(parsed.origin_timestamp));
    }
}

/// xact_desc_abort: parse + render an abort (or abort-prepared) record.
///
/// # Safety
/// `xlrec` must point to a valid abort record; `buf` valid StringInfo.
unsafe fn xact_desc_abort(
    buf: StringInfo,
    info: uint8,
    xlrec: *mut xl_xact_abort,
    origin_id: RepOriginId,
) {
    let mut parsed: xl_xact_parsed_abort = core::mem::zeroed();
    ParseAbortRecord(info, xlrec, &mut parsed);

    /* If this is a prepared xact, show the xid of the original xact */
    if TransactionIdIsValid(parsed.twophase_xid) {
        appendStringInfo!(buf, "{}: ", parsed.twophase_xid);
    }

    appendStringInfoString(buf, timestamptz_to_str((*xlrec).xact_time));

    xact_desc_relations(buf, c"rels".as_ptr(), parsed.nrels, parsed.xlocators);
    xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts);

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        let (lsn_hi, lsn_lo) = LSN_FORMAT_ARGS(parsed.origin_lsn);
        appendStringInfo!(buf, "; origin: node {}, lsn {:X}/{:X}, at ", origin_id, lsn_hi, lsn_lo);
        appendStringInfoString(buf, timestamptz_to_str(parsed.origin_timestamp));
    }

    xact_desc_stats(buf, c"".as_ptr(), parsed.nstats, parsed.stats);
}

/// xact_desc_prepare: parse + render a PREPARE record.
///
/// # Safety
/// `xlrec` must point to a valid prepare record; `buf` valid StringInfo.
unsafe fn xact_desc_prepare(
    buf: StringInfo,
    info: uint8,
    xlrec: *mut xl_xact_prepare,
    origin_id: RepOriginId,
) {
    let mut parsed: xl_xact_parsed_prepare = core::mem::zeroed();
    ParsePrepareRecord(info, xlrec, &mut parsed);

    // C: appendStringInfo(buf, "gid %s: ", parsed.twophase_gid) - the gid is a
    // local char[GIDSIZE], rendered as a NUL-terminated string.
    appendStringInfoString(buf, c"gid ".as_ptr());
    appendStringInfoString(buf, parsed.twophase_gid.as_ptr());
    appendStringInfoString(buf, c": ".as_ptr());
    appendStringInfoString(buf, timestamptz_to_str(parsed.xact_time));

    xact_desc_relations(buf, c"rels(commit)".as_ptr(), parsed.nrels, parsed.xlocators);
    xact_desc_relations(
        buf,
        c"rels(abort)".as_ptr(),
        parsed.nabortrels,
        parsed.abortlocators,
    );
    xact_desc_stats(buf, c"commit ".as_ptr(), parsed.nstats, parsed.stats);
    xact_desc_stats(buf, c"abort ".as_ptr(), parsed.nabortstats, parsed.abortstats);
    xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts);

    standby_desc_invalidations(
        buf,
        parsed.nmsgs,
        parsed.msgs,
        parsed.dbId,
        parsed.tsId,
        (*xlrec).initfileinval,
    );

    /*
     * Check if the replication origin has been set in this record in the same
     * way as PrepareRedoAdd().
     */
    if origin_id != InvalidRepOriginId {
        let (lsn_hi, lsn_lo) = LSN_FORMAT_ARGS(parsed.origin_lsn);
        appendStringInfo!(buf, "; origin: node {}, lsn {:X}/{:X}, at ", origin_id, lsn_hi, lsn_lo);
        appendStringInfoString(buf, timestamptz_to_str(parsed.origin_timestamp));
    }
}

/// xact_desc_assignment: append "subxacts:" plus each assigned subxact XID.
///
/// # Safety
/// `xlrec` must point to a valid xl_xact_assignment with `nsubxacts` xsub[].
unsafe fn xact_desc_assignment(buf: StringInfo, xlrec: *mut xl_xact_assignment) {
    appendStringInfoString(buf, c"subxacts:".as_ptr());
    let xsub = (*xlrec).xsub.as_ptr();
    for i in 0..(*xlrec).nsubxacts {
        appendStringInfo!(buf, " {}", *xsub.add(i as usize));
    }
}

// ---------------------------------------------------------------------------
// public desc / identify
// ---------------------------------------------------------------------------

/// xact_desc: dispatch on (XLogRecGetInfo(record) & XLOG_XACT_OPMASK), cast the
/// record payload per opcode and append the field summary. Reproduces the C
/// output text exactly.
///
/// # Safety
/// `record` is an opaque WAL-reader pointer; `buf` must be a valid StringInfo.
pub unsafe fn xact_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

    if info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED {
        let xlrec = rec as *mut xl_xact_commit;
        xact_desc_commit(buf, XLogRecGetInfo(record), xlrec, XLogRecGetOrigin(record));
    } else if info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED {
        let xlrec = rec as *mut xl_xact_abort;
        xact_desc_abort(buf, XLogRecGetInfo(record), xlrec, XLogRecGetOrigin(record));
    } else if info == XLOG_XACT_PREPARE {
        let xlrec = rec as *mut xl_xact_prepare;
        xact_desc_prepare(buf, XLogRecGetInfo(record), xlrec, XLogRecGetOrigin(record));
    } else if info == XLOG_XACT_ASSIGNMENT {
        let xlrec = rec as *mut xl_xact_assignment;
        /*
         * Note that we ignore the WAL record's xid, since we're more
         * interested in the top-level xid that issued the record and which
         * xids are being reported here.
         */
        appendStringInfo!(buf, "xtop {}: ", (*xlrec).xtop);
        xact_desc_assignment(buf, xlrec);
    } else if info == XLOG_XACT_INVALIDATIONS {
        let xlrec = rec as *mut xl_xact_invals;
        standby_desc_invalidations(
            buf,
            (*xlrec).nmsgs,
            (*xlrec).msgs.as_mut_ptr(),
            0, /* InvalidOid */
            0, /* InvalidOid */
            false,
        );
    }
}

/// xact_identify: map a transaction opcode (info byte, masked with
/// XLOG_XACT_OPMASK) to its name string, or null for an unknown opcode.
pub fn xact_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & XLOG_XACT_OPMASK {
        XLOG_XACT_COMMIT => b"COMMIT\0",
        XLOG_XACT_PREPARE => b"PREPARE\0",
        XLOG_XACT_ABORT => b"ABORT\0",
        XLOG_XACT_COMMIT_PREPARED => b"COMMIT_PREPARED\0",
        XLOG_XACT_ABORT_PREPARED => b"ABORT_PREPARED\0",
        XLOG_XACT_ASSIGNMENT => b"ASSIGNMENT\0",
        XLOG_XACT_INVALIDATIONS => b"INVALIDATION\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::stringinfo::{initStringInfo, StringInfoData};
    use core::ffi::CStr;
    use core::mem::offset_of;

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (XLOG_XACT_COMMIT, "COMMIT"),
            (XLOG_XACT_PREPARE, "PREPARE"),
            (XLOG_XACT_ABORT, "ABORT"),
            (XLOG_XACT_COMMIT_PREPARED, "COMMIT_PREPARED"),
            (XLOG_XACT_ABORT_PREPARED, "ABORT_PREPARED"),
            (XLOG_XACT_ASSIGNMENT, "ASSIGNMENT"),
            (XLOG_XACT_INVALIDATIONS, "INVALIDATION"),
        ];
        for &(op, name) in cases {
            let p = xact_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should map to {}", op, name);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name);
        }
    }

    #[test]
    fn identify_ignores_info_flag_bits() {
        // High XLOG_XACT_HAS_INFO bit + the low XLR flag nibble must be ignored
        // (only the XLOG_XACT_OPMASK 0x70 bits select the opcode).
        let p = xact_identify(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO | XLR_INFO_MASK);
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "COMMIT");
    }

    #[test]
    fn identify_unknown_returns_null() {
        // free opcode 0x70
        assert!(xact_identify(0x70).is_null());
    }

    #[test]
    fn opcode_and_mask_values_are_real() {
        assert_eq!(XLOG_XACT_COMMIT, 0x00);
        assert_eq!(XLOG_XACT_PREPARE, 0x10);
        assert_eq!(XLOG_XACT_ABORT, 0x20);
        assert_eq!(XLOG_XACT_COMMIT_PREPARED, 0x30);
        assert_eq!(XLOG_XACT_ABORT_PREPARED, 0x40);
        assert_eq!(XLOG_XACT_ASSIGNMENT, 0x50);
        assert_eq!(XLOG_XACT_INVALIDATIONS, 0x60);
        assert_eq!(XLOG_XACT_OPMASK, 0x70);
        assert_eq!(XLOG_XACT_HAS_INFO, 0x80);
    }

    #[test]
    fn xinfo_flag_values_are_real() {
        assert_eq!(XACT_XINFO_HAS_DBINFO, 1 << 0);
        assert_eq!(XACT_XINFO_HAS_SUBXACTS, 1 << 1);
        assert_eq!(XACT_XINFO_HAS_RELFILELOCATORS, 1 << 2);
        assert_eq!(XACT_XINFO_HAS_INVALS, 1 << 3);
        assert_eq!(XACT_XINFO_HAS_TWOPHASE, 1 << 4);
        assert_eq!(XACT_XINFO_HAS_ORIGIN, 1 << 5);
        assert_eq!(XACT_XINFO_HAS_AE_LOCKS, 1 << 6);
        assert_eq!(XACT_XINFO_HAS_GID, 1 << 7);
        assert_eq!(XACT_XINFO_HAS_DROPPED_STATS, 1 << 8);
        assert_eq!(XACT_COMPLETION_APPLY_FEEDBACK, 1 << 29);
        assert_eq!(XACT_COMPLETION_UPDATE_RELCACHE_FILE, 1 << 30);
        assert_eq!(XACT_COMPLETION_FORCE_SYNC_COMMIT, 1 << 31);
    }

    #[test]
    fn layout_min_sizes() {
        // MinSizeOfXactCommit = offsetof(xact_time) + sizeof(TimestampTz) = 8.
        assert_eq!(MinSizeOfXactCommit, 8);
        // MinSizeOfXactAbort = sizeof(xl_xact_abort) = 8.
        assert_eq!(MinSizeOfXactAbort, 8);
        // Flexible-array offsets.
        assert_eq!(MinSizeOfXactSubxacts, offset_of!(xl_xact_subxacts, subxacts));
        assert_eq!(MinSizeOfXactSubxacts, 4);
        assert_eq!(
            MinSizeOfXactRelfileLocators,
            offset_of!(xl_xact_relfilelocators, xlocators)
        );
        assert_eq!(MinSizeOfXactRelfileLocators, 4);
        assert_eq!(MinSizeOfXactStatsItems, offset_of!(xl_xact_stats_items, items));
        assert_eq!(MinSizeOfXactInvals, offset_of!(xl_xact_invals, msgs));
        assert_eq!(MinSizeOfXactAssignment, offset_of!(xl_xact_assignment, xsub));
        assert_eq!(MinSizeOfXactAssignment, 8);
    }

    #[test]
    fn layout_xl_xact_stats_item() {
        // int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi -- 16 bytes.
        assert_eq!(offset_of!(xl_xact_stats_item, kind), 0);
        assert_eq!(offset_of!(xl_xact_stats_item, dboid), 4);
        assert_eq!(offset_of!(xl_xact_stats_item, objid_lo), 8);
        assert_eq!(offset_of!(xl_xact_stats_item, objid_hi), 12);
        assert_eq!(core::mem::size_of::<xl_xact_stats_item>(), 16);
    }

    #[test]
    fn layout_xl_xact_origin_is_unaligned_pair() {
        assert_eq!(offset_of!(xl_xact_origin, origin_lsn), 0);
        assert_eq!(offset_of!(xl_xact_origin, origin_timestamp), 8);
        assert_eq!(core::mem::size_of::<xl_xact_origin>(), 16);
    }

    /// ParseCommitRecord on a hand-built minimal commit record (xinfo = 0):
    /// just the xl_xact_commit header, no XLOG_XACT_HAS_INFO. Everything else
    /// in `parsed` must be zeroed and xact_time must round-trip.
    #[test]
    fn parse_commit_minimal_record() {
        unsafe {
            let mut xlrec = xl_xact_commit {
                xact_time: 0x0123_4567_89ab_cdef,
            };
            let mut parsed: xl_xact_parsed_commit = core::mem::zeroed();
            // info has no XLOG_XACT_HAS_INFO bit -> no xinfo section is read.
            ParseCommitRecord(XLOG_XACT_COMMIT, &mut xlrec, &mut parsed);

            assert_eq!(parsed.xact_time, 0x0123_4567_89ab_cdef);
            assert_eq!(parsed.xinfo, 0);
            assert_eq!(parsed.nsubxacts, 0);
            assert_eq!(parsed.nrels, 0);
            assert_eq!(parsed.nstats, 0);
            assert_eq!(parsed.nmsgs, 0);
            assert!(parsed.subxacts.is_null());
            assert!(parsed.xlocators.is_null());
            assert!(parsed.msgs.is_null());
            assert_eq!(parsed.twophase_xid, 0);
            assert_eq!(parsed.twophase_gid[0], 0);
        }
    }

    /// xact_desc_commit text on the same minimal record (xinfo = 0): no
    /// twophase xid prefix, just the (stubbed) timestamp string, no trailing
    /// sections.
    #[test]
    fn desc_commit_minimal_text() {
        unsafe {
            let mut xlrec = xl_xact_commit { xact_time: 0 };
            let mut sid: StringInfoData = core::mem::zeroed();
            initStringInfo(&mut sid as StringInfo);
            xact_desc_commit(
                &mut sid as StringInfo,
                XLOG_XACT_COMMIT,
                &mut xlrec,
                InvalidRepOriginId,
            );
            let s = CStr::from_ptr(sid.data).to_str().unwrap();
            // Only the stubbed timestamp placeholder is emitted.
            assert_eq!(s, "(timestamp)");
        }
    }
}
