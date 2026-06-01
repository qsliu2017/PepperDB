//! Translation of postgres/src/backend/access/rmgrdesc/standbydesc.c
//!                + the WAL record structs / XLOG_* opcodes it reads from
//!                  postgres/src/include/storage/standbydefs.h (and the
//!                  xl_standby_lock / SharedInvalidationMessage layouts merged
//!                  in from storage/lockdefs.h and storage/sinval.h).
//!
//! rmgr descriptor routines for the Standby Rmgr (RM_STANDBY_ID), used by
//! pg_waldump. standby_desc casts the WAL record payload to the appropriate
//! struct (selected by the record's info byte) and appends a human-readable
//! summary of its fields; standby_identify maps an opcode to its name string.
//! standby_desc_invalidations is also reused by non-standby records that carry
//! analogous shared-invalidation fields.
//!
//! Header mapping:
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo,
//!                               appendStringInfo!, appendStringInfoString)
//!   storage/standbydefs.h    -> XLOG_STANDBY_LOCK / XLOG_RUNNING_XACTS /
//!                               XLOG_INVALIDATIONS opcodes + the
//!                               xl_standby_locks / xl_running_xacts /
//!                               xl_invalidations record structs
//!   storage/lockdefs.h       -> xl_standby_lock
//!   storage/sinval.h         -> SharedInvalidationMessage union + the
//!                               SHAREDINVAL*_ID type codes
//!   c.h types                -> uint8/uint16/uint32, bool, TransactionId, Oid
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the XLOG_STANDBY_* opcode values, the SHAREDINVAL*_ID
//! codes, and the standby_identify name table are REAL (faithful to
//! standbydefs.h / lockdefs.h / sinval.h / standbydesc.c). The desc output text
//! reproduces the C output exactly (same labels, same order).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK};
use crate::lib::stringinfo::{appendStringInfoString, StringInfo};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from c.h)
// ---------------------------------------------------------------------------

pub type TransactionId = uint32;

// ---------------------------------------------------------------------------
// XLOG message types (from storage/standbydefs.h)
// ---------------------------------------------------------------------------

pub const XLOG_STANDBY_LOCK: uint8 = 0x00;
pub const XLOG_RUNNING_XACTS: uint8 = 0x10;
pub const XLOG_INVALIDATIONS: uint8 = 0x20;

// ---------------------------------------------------------------------------
// SharedInvalidationMessage codes + layout (from storage/sinval.h)
// ---------------------------------------------------------------------------

pub const SHAREDINVALCATALOG_ID: int8 = -1;
pub const SHAREDINVALRELCACHE_ID: int8 = -2;
pub const SHAREDINVALSMGR_ID: int8 = -3;
pub const SHAREDINVALRELMAP_ID: int8 = -4;
pub const SHAREDINVALSNAPSHOT_ID: int8 = -5;
pub const SHAREDINVALRELSYNC_ID: int8 = -6;

/// RelFileLocator (spcOid, dbOid, relNumber) -- from storage/relfilelocator.h.
/// Defined locally per project convention (not trivially importable here).
pub type RelFileNumber = uint32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalCatcacheMsg {
    pub id: int8,
    pub dbId: Oid,
    pub hashValue: uint32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalCatalogMsg {
    pub id: int8,
    pub dbId: Oid,
    pub catId: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalRelcacheMsg {
    pub id: int8,
    pub dbId: Oid,
    pub relId: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalSmgrMsg {
    /* note: field layout chosen to pack into 16 bytes */
    pub id: int8,
    pub backend_hi: int8,
    pub backend_lo: uint16,
    pub rlocator: RelFileLocator,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalRelmapMsg {
    pub id: int8,
    pub dbId: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalSnapshotMsg {
    pub id: int8,
    pub dbId: Oid,
    pub relId: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalRelSyncMsg {
    pub id: int8,
    pub dbId: Oid,
    pub relid: Oid,
}

/// SharedInvalidationMessage union -- the first int8 `id` field discriminates.
#[repr(C)]
#[derive(Clone, Copy)]
pub union SharedInvalidationMessage {
    pub id: int8,
    pub cc: SharedInvalCatcacheMsg,
    pub cat: SharedInvalCatalogMsg,
    pub rc: SharedInvalRelcacheMsg,
    pub sm: SharedInvalSmgrMsg,
    pub rm: SharedInvalRelmapMsg,
    pub sn: SharedInvalSnapshotMsg,
    pub rs: SharedInvalRelSyncMsg,
}

// ---------------------------------------------------------------------------
// WAL record structs (from storage/standbydefs.h + storage/lockdefs.h)
// ---------------------------------------------------------------------------

/// xl_standby_lock (storage/lockdefs.h): one held AccessExclusiveLock entry.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_standby_lock {
    pub xid: TransactionId, /* xid of holder of AccessExclusiveLock */
    pub dbOid: Oid,         /* DB containing table */
    pub relOid: Oid,        /* OID of table */
}

/// xl_standby_locks: `nlocks` followed by a FLEXIBLE_ARRAY_MEMBER `locks[]`.
#[repr(C)]
pub struct xl_standby_locks {
    pub nlocks: int32, /* number of entries in locks array */
    pub locks: [xl_standby_lock; 0],
}

/// xl_running_xacts: snapshot of running transactions, with a
/// FLEXIBLE_ARRAY_MEMBER `xids[]` of length xcnt + subxcnt.
#[repr(C)]
pub struct xl_running_xacts {
    pub xcnt: int32,             /* # of xact ids in xids[] */
    pub subxcnt: int32,          /* # of subxact ids in xids[] */
    pub subxid_overflow: bool,   /* snapshot overflowed, subxids missing */
    pub nextXid: TransactionId,  /* xid from TransamVariables->nextXid */
    pub oldestRunningXid: TransactionId, /* *not* oldestXmin */
    pub latestCompletedXid: TransactionId, /* so we can set xmax */
    pub xids: [TransactionId; 0],
}

/// xl_invalidations: shared-inval messages emitted at xidless commit, with a
/// FLEXIBLE_ARRAY_MEMBER `msgs[]` of length nmsgs.
#[repr(C)]
pub struct xl_invalidations {
    pub dbId: Oid,                 /* MyDatabaseId */
    pub tsId: Oid,                 /* MyDatabaseTableSpace */
    pub relcacheInitFileInval: bool, /* invalidate relcache init files */
    pub nmsgs: int32,              /* number of shared inval msgs */
    pub msgs: [SharedInvalidationMessage; 0],
}

// ---------------------------------------------------------------------------
// desc helpers
// ---------------------------------------------------------------------------

unsafe fn standby_desc_running_xacts(buf: StringInfo, xlrec: *mut xl_running_xacts) {
    appendStringInfo!(
        buf,
        "nextXid {} latestCompletedXid {} oldestRunningXid {}",
        (*xlrec).nextXid,
        (*xlrec).latestCompletedXid,
        (*xlrec).oldestRunningXid
    );

    if (*xlrec).xcnt > 0 {
        appendStringInfo!(buf, "; {} xacts:", (*xlrec).xcnt);
        let xids = (*xlrec).xids.as_ptr();
        for i in 0..(*xlrec).xcnt {
            appendStringInfo!(buf, " {}", *xids.add(i as usize));
        }
    }

    if (*xlrec).subxid_overflow {
        appendStringInfoString(buf, c"; subxid overflowed".as_ptr());
    }

    if (*xlrec).subxcnt > 0 {
        appendStringInfo!(buf, "; {} subxacts:", (*xlrec).subxcnt);
        let xids = (*xlrec).xids.as_ptr();
        for i in 0..(*xlrec).subxcnt {
            let idx = ((*xlrec).xcnt + i) as usize;
            appendStringInfo!(buf, " {}", *xids.add(idx));
        }
    }
}

// ---------------------------------------------------------------------------
// public desc / identify
// ---------------------------------------------------------------------------

/// standby_desc: dispatch on (XLogRecGetInfo(record) & ~XLR_INFO_MASK), cast
/// the record payload per opcode and append the field summary.
///
/// # Safety
/// `record` is an opaque WAL reader pointer; `buf` must be a valid StringInfo.
pub unsafe fn standby_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_STANDBY_LOCK {
        let xlrec = rec as *mut xl_standby_locks;
        let locks = (*xlrec).locks.as_ptr();
        for i in 0..(*xlrec).nlocks {
            let lk = &*locks.add(i as usize);
            appendStringInfo!(
                buf,
                "xid {} db {} rel {} ",
                lk.xid,
                lk.dbOid,
                lk.relOid
            );
        }
    } else if info == XLOG_RUNNING_XACTS {
        let xlrec = rec as *mut xl_running_xacts;
        standby_desc_running_xacts(buf, xlrec);
    } else if info == XLOG_INVALIDATIONS {
        let xlrec = rec as *mut xl_invalidations;
        standby_desc_invalidations(
            buf,
            (*xlrec).nmsgs,
            (*xlrec).msgs.as_ptr() as *mut SharedInvalidationMessage,
            (*xlrec).dbId,
            (*xlrec).tsId,
            (*xlrec).relcacheInitFileInval,
        );
    }
}

/// standby_identify: opcode -> name string (null on unknown).
pub fn standby_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        XLOG_STANDBY_LOCK => b"LOCK\0",
        XLOG_RUNNING_XACTS => b"RUNNING_XACTS\0",
        XLOG_INVALIDATIONS => b"INVALIDATIONS\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

/// standby_desc_invalidations: render a list of shared-invalidation messages.
/// Also used by non-standby records having analogous invalidation fields.
///
/// # Safety
/// `msgs` must point to `nmsgs` valid SharedInvalidationMessage entries.
pub unsafe fn standby_desc_invalidations(
    buf: StringInfo,
    nmsgs: int32,
    msgs: *mut SharedInvalidationMessage,
    dbId: Oid,
    tsId: Oid,
    relcacheInitFileInval: bool,
) {
    /* Do nothing if there are no invalidation messages */
    if nmsgs <= 0 {
        return;
    }

    if relcacheInitFileInval {
        appendStringInfo!(
            buf,
            "; relcache init file inval dbid {} tsid {}",
            dbId,
            tsId
        );
    }

    appendStringInfoString(buf, c"; inval msgs:".as_ptr());
    for i in 0..nmsgs {
        let msg = &*msgs.add(i as usize);
        let id = msg.id;
        if id >= 0 {
            appendStringInfo!(buf, " catcache {}", id);
        } else if id == SHAREDINVALCATALOG_ID {
            appendStringInfo!(buf, " catalog {}", msg.cat.catId);
        } else if id == SHAREDINVALRELCACHE_ID {
            appendStringInfo!(buf, " relcache {}", msg.rc.relId);
        } else if id == SHAREDINVALSMGR_ID {
            /* not expected, but print something anyway */
            appendStringInfoString(buf, c" smgr".as_ptr());
        } else if id == SHAREDINVALRELMAP_ID {
            /* not expected, but print something anyway */
            appendStringInfo!(buf, " relmap db {}", msg.rm.dbId);
        } else if id == SHAREDINVALSNAPSHOT_ID {
            appendStringInfo!(buf, " snapshot {}", msg.sn.relId);
        } else if id == SHAREDINVALRELSYNC_ID {
            appendStringInfo!(buf, " relsync {}", msg.rs.relid);
        } else {
            appendStringInfo!(buf, " unrecognized id {}", id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;
    use core::mem::offset_of;

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (XLOG_STANDBY_LOCK, "LOCK"),
            (XLOG_RUNNING_XACTS, "RUNNING_XACTS"),
            (XLOG_INVALIDATIONS, "INVALIDATIONS"),
        ];
        for &(op, name) in cases {
            let p = standby_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should map to {}", op, name);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name);
        }
    }

    #[test]
    fn identify_ignores_info_mask_bits() {
        // The high (XLR_INFO_MASK) bits must be ignored.
        let p = standby_identify(XLOG_RUNNING_XACTS | XLR_INFO_MASK);
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "RUNNING_XACTS");
    }

    #[test]
    fn identify_unknown_returns_null() {
        assert!(standby_identify(0x70).is_null());
    }

    #[test]
    fn opcode_values_are_real() {
        assert_eq!(XLOG_STANDBY_LOCK, 0x00);
        assert_eq!(XLOG_RUNNING_XACTS, 0x10);
        assert_eq!(XLOG_INVALIDATIONS, 0x20);
    }

    #[test]
    fn sinval_id_codes_are_real() {
        assert_eq!(SHAREDINVALCATALOG_ID, -1);
        assert_eq!(SHAREDINVALRELCACHE_ID, -2);
        assert_eq!(SHAREDINVALSMGR_ID, -3);
        assert_eq!(SHAREDINVALRELMAP_ID, -4);
        assert_eq!(SHAREDINVALSNAPSHOT_ID, -5);
        assert_eq!(SHAREDINVALRELSYNC_ID, -6);
    }

    #[test]
    fn layout_xl_standby_lock() {
        // xid(u32) dbOid(u32) relOid(u32) -- tightly packed, size 12.
        assert_eq!(offset_of!(xl_standby_lock, xid), 0);
        assert_eq!(offset_of!(xl_standby_lock, dbOid), 4);
        assert_eq!(offset_of!(xl_standby_lock, relOid), 8);
        assert_eq!(core::mem::size_of::<xl_standby_lock>(), 12);
    }

    #[test]
    fn layout_xl_running_xacts() {
        // int xcnt; int subxcnt; bool subxid_overflow; then 4-byte aligned
        // TransactionId fields (bool occupies 1 byte, 3 bytes padding follow).
        assert_eq!(offset_of!(xl_running_xacts, xcnt), 0);
        assert_eq!(offset_of!(xl_running_xacts, subxcnt), 4);
        assert_eq!(offset_of!(xl_running_xacts, subxid_overflow), 8);
        assert_eq!(offset_of!(xl_running_xacts, nextXid), 12);
        assert_eq!(offset_of!(xl_running_xacts, oldestRunningXid), 16);
        assert_eq!(offset_of!(xl_running_xacts, latestCompletedXid), 20);
        // MinSizeOf == offset of the flexible xids[] member.
        assert_eq!(offset_of!(xl_running_xacts, xids), 24);
    }

    #[test]
    fn layout_xl_invalidations() {
        assert_eq!(offset_of!(xl_invalidations, dbId), 0);
        assert_eq!(offset_of!(xl_invalidations, tsId), 4);
        assert_eq!(offset_of!(xl_invalidations, relcacheInitFileInval), 8);
        assert_eq!(offset_of!(xl_invalidations, nmsgs), 12);
        // MinSizeOfInvalidations == offsetof(xl_invalidations, msgs).
        assert_eq!(offset_of!(xl_invalidations, msgs), 16);
    }

    #[test]
    fn layout_shared_inval_smgr_packs_to_16() {
        // id(1) backend_hi(1) backend_lo(2) rlocator(12) == 16 bytes.
        assert_eq!(core::mem::size_of::<SharedInvalSmgrMsg>(), 16);
        assert_eq!(offset_of!(SharedInvalSmgrMsg, rlocator), 4);
    }
}
