//! rmgr descriptor routines for access/transam/xact.c. Translated from
//! backend/access/rmgrdesc/xactdesc.c: `xact_desc` / `xact_identify`.
//!
//! C renders `xact_time` via `timestamptz_to_str`; that formatter is a deferred
//! stub in this port, so the timestamp is rendered as its raw microsecond value
//! (`ts <n>`) to keep the description panic-free and deterministic.

use std::fmt::Write as _;

use super::rmgr_info;
use super::util::{i32_at, i64_at, u32_at};
use crate::access::xact::{
    XACT_XINFO_HAS_DBINFO, XACT_XINFO_HAS_SUBXACTS, XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED,
    XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_HAS_INFO,
    XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK, XLOG_XACT_PREPARE,
};
use crate::access::xlogreader::DecodedXLogRecord;

/// Render `xact_time` (deferred formatter -> raw microsecond value).
fn xact_time_str(t: i64) -> String {
    format!("ts {t}")
}

/// Parse the sub-record chain shared by commit/abort and render the common tail
/// (time; subxacts). `data` begins at the fixed record header (MinSizeOfXact*).
/// `has_info` is the XLOG_XACT_HAS_INFO bit from `xl_info`.
fn desc_commit_abort(rec: &[u8], has_info: bool) -> String {
    // xl_xact_commit/abort begin with xact_time: i64.
    let xact_time = i64_at(rec, 0);
    let mut cursor = 8usize; // past xact_time

    let mut xinfo: u32 = 0;
    if has_info {
        xinfo = u32_at(rec, cursor);
        cursor += 4; // sizeof(xl_xact_xinfo)
    }

    if xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        cursor += 8; // sizeof(xl_xact_dbinfo) = dbId + tsId
    }

    let mut out = xact_time_str(xact_time);

    if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let nsub = i32_at(rec, cursor);
        cursor += 4; // MinSizeOfXactSubxacts
        if nsub > 0 {
            out.push_str("; subxacts:");
            for i in 0..nsub as usize {
                let _ = write!(out, " {}", u32_at(rec, cursor + i * 4));
            }
        }
    }

    out
}

/// C `xact_desc`.
#[must_use]
pub fn xact_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let full_info = record.header.info;
    let info = full_info & XLOG_XACT_OPMASK;
    let has_info = full_info & XLOG_XACT_HAS_INFO != 0;

    if info == XLOG_XACT_COMMIT
        || info == XLOG_XACT_COMMIT_PREPARED
        || info == XLOG_XACT_ABORT
        || info == XLOG_XACT_ABORT_PREPARED
    {
        // Commit and abort share the sub-record chain layout (time; subxacts).
        desc_commit_abort(rec, has_info)
    } else if info == XLOG_XACT_PREPARE {
        // xl_xact_prepare: prepared_at is at a fixed offset; render it.
        // magic:u32, total_len:u32, xid:u32, database:u32, prepared_at:i64 @ 16.
        xact_time_str(i64_at(rec, 16))
    } else if info == XLOG_XACT_ASSIGNMENT {
        // xl_xact_assignment { xtop: u32, nsubxacts: i32, xsub: [u32] }
        let xtop = u32_at(rec, 0);
        let nsub = i32_at(rec, 4);
        let mut out = format!("xtop {xtop}: subxacts:");
        for i in 0..nsub.max(0) as usize {
            let _ = write!(out, " {}", u32_at(rec, 8 + i * 4));
        }
        out
    } else if info == XLOG_XACT_INVALIDATIONS {
        // Invalidation-message rendering deferred (standby_desc_invalidations).
        String::new()
    } else {
        String::new()
    }
}

/// C `xact_identify`.
#[must_use]
pub fn xact_identify(info: u8) -> Option<&'static str> {
    Some(match info & XLOG_XACT_OPMASK {
        x if x == XLOG_XACT_COMMIT => "COMMIT",
        x if x == XLOG_XACT_PREPARE => "PREPARE",
        x if x == XLOG_XACT_ABORT => "ABORT",
        x if x == XLOG_XACT_COMMIT_PREPARED => "COMMIT_PREPARED",
        x if x == XLOG_XACT_ABORT_PREPARED => "ABORT_PREPARED",
        x if x == XLOG_XACT_ASSIGNMENT => "ASSIGNMENT",
        x if x == XLOG_XACT_INVALIDATIONS => "INVALIDATION",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const XACT_RMID: u8 = 1;

    #[test]
    fn commit_renders_time() {
        // xl_xact_commit { xact_time: i64 }, no XLOG_XACT_HAS_INFO.
        let data = 1234567i64.to_ne_bytes();
        let rec = record_with_data(XACT_RMID, XLOG_XACT_COMMIT, &data);
        assert_eq!(xact_identify(XLOG_XACT_COMMIT), Some("COMMIT"));
        assert_eq!(xact_desc(&rec), "ts 1234567");
    }

    #[test]
    fn commit_with_subxacts() {
        // xact_time:i64, xinfo:u32 (HAS_SUBXACTS), nsubxacts:i32=2, subxacts.
        let mut data = Vec::new();
        data.extend_from_slice(&100i64.to_ne_bytes());
        data.extend_from_slice(&XACT_XINFO_HAS_SUBXACTS.to_ne_bytes());
        data.extend_from_slice(&2i32.to_ne_bytes());
        data.extend_from_slice(&11u32.to_ne_bytes());
        data.extend_from_slice(&12u32.to_ne_bytes());
        // info carries XLOG_XACT_HAS_INFO high bit.
        let rec = record_with_data(XACT_RMID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data);
        assert_eq!(xact_desc(&rec), "ts 100; subxacts: 11 12");
    }

    #[test]
    fn abort_identify() {
        assert_eq!(xact_identify(XLOG_XACT_ABORT), Some("ABORT"));
    }
}
