//! rmgr descriptor routines for access/transam/xlog.c. Translated from
//! backend/access/rmgrdesc/xlogdesc.c: `xlog_desc` / `xlog_identify`.
//!
//! C renders timestamps via `timestamptz_to_str`; that formatter is a deferred
//! stub here, so timestamp-bearing records (END_OF_RECOVERY, OVERWRITE_CONTRECORD)
//! render the raw microsecond value, keeping descriptions panic-free.

use super::rmgr_info;
use super::util::{i32_at, lsn_str, u32_at, u64_at};
use crate::access::xlogreader::DecodedXLogRecord;
use crate::catalog::pg_control::{
    XLOG_BACKUP_END, XLOG_CHECKPOINT_ONLINE, XLOG_CHECKPOINT_REDO, XLOG_CHECKPOINT_SHUTDOWN,
    XLOG_END_OF_RECOVERY, XLOG_FPI, XLOG_FPI_FOR_HINT, XLOG_FPW_CHANGE, XLOG_NEXTOID, XLOG_NOOP,
    XLOG_OVERWRITE_CONTRECORD, XLOG_PARAMETER_CHANGE, XLOG_RESTORE_POINT, XLOG_SWITCH,
};

/// Textual `wal_level` (C `get_wal_level_string`): 0=minimal, 1=replica, 2=logical.
fn wal_level_str(wal_level: i32) -> &'static str {
    match wal_level {
        0 => "minimal",
        1 => "replica",
        2 => "logical",
        _ => "?",
    }
}

/// C `xlog_desc`.
#[must_use]
#[allow(
    clippy::similar_names,
    reason = "field names (next_oid/next_multi/...) are the faithful CheckPoint names"
)]
pub fn xlog_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info);

    if info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE {
        // CheckPoint layout (LE, 8-byte align):
        //   redo:u64 @0, ThisTimeLineID:u32 @8, PrevTimeLineID:u32 @12,
        //   fullPageWrites:bool @16 (pad to 4), wal_level:i32 @20,
        //   nextXid:FullTransactionId(u64) @24, nextOid:u32 @32,
        //   nextMulti:u32 @36, nextMultiOffset:u32 @40, oldestXid:u32 @44,
        //   oldestXidDB:u32 @48, oldestMulti:u32 @52, oldestMultiDB:u32 @56,
        //   time:i64 @64, oldestCommitTsXid:u32 @72, newestCommitTsXid:u32 @76,
        //   oldestActiveXid:u32 @80.
        let redo = u64_at(rec, 0);
        let this_tli = u32_at(rec, 8);
        let prev_tli = u32_at(rec, 12);
        let fpw = rec.get(16).copied().unwrap_or(0) != 0;
        let wal_level = i32_at(rec, 20);
        let next_xid = u64_at(rec, 24);
        let epoch = (next_xid >> 32) as u32;
        let xid = next_xid as u32;
        let next_oid = u32_at(rec, 32);
        let next_multi = u32_at(rec, 36);
        let next_multi_off = u32_at(rec, 40);
        let oldest_xid = u32_at(rec, 44);
        let oldest_xid_db = u32_at(rec, 48);
        let oldest_multi = u32_at(rec, 52);
        let oldest_multi_db = u32_at(rec, 56);
        let oldest_cts = u32_at(rec, 72);
        let newest_cts = u32_at(rec, 76);
        let oldest_active = u32_at(rec, 80);
        format!(
            "redo {}; tli {this_tli}; prev tli {prev_tli}; fpw {}; wal_level {}; xid {epoch}:{xid}; oid {next_oid}; multi {next_multi}; offset {next_multi_off}; oldest xid {oldest_xid} in DB {oldest_xid_db}; oldest multi {oldest_multi} in DB {oldest_multi_db}; oldest/newest commit timestamp xid: {oldest_cts}/{newest_cts}; oldest running xid {oldest_active}; {}",
            lsn_str(redo),
            if fpw { "true" } else { "false" },
            wal_level_str(wal_level),
            if info == XLOG_CHECKPOINT_SHUTDOWN { "shutdown" } else { "online" }
        )
    } else if info == XLOG_NEXTOID {
        format!("{}", u32_at(rec, 0))
    } else if info == XLOG_RESTORE_POINT {
        // xl_restore_point { time: i64, name: [u8; MAXFNAMELEN] }; render name.
        let name_bytes = rec.get(8..).unwrap_or(&[]);
        let end = name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
        String::from_utf8_lossy(&name_bytes[..end]).into_owned()
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        String::new()
    } else if info == XLOG_BACKUP_END {
        lsn_str(u64_at(rec, 0))
    } else if info == XLOG_PARAMETER_CHANGE {
        // xl_parameter_change (i32 x6 then 2 bools).
        let max_connections = i32_at(rec, 0);
        let max_worker_processes = i32_at(rec, 4);
        let max_wal_senders = i32_at(rec, 8);
        let max_prepared_xacts = i32_at(rec, 12);
        let max_locks_per_xact = i32_at(rec, 16);
        let wal_level = i32_at(rec, 20);
        let wal_log_hints = rec.get(24).copied().unwrap_or(0) != 0;
        let track_commit_ts = rec.get(25).copied().unwrap_or(0) != 0;
        format!(
            "max_connections={max_connections} max_worker_processes={max_worker_processes} max_wal_senders={max_wal_senders} max_prepared_xacts={max_prepared_xacts} max_locks_per_xact={max_locks_per_xact} wal_level={} wal_log_hints={} track_commit_timestamp={}",
            wal_level_str(wal_level),
            if wal_log_hints { "on" } else { "off" },
            if track_commit_ts { "on" } else { "off" }
        )
    } else if info == XLOG_FPW_CHANGE {
        let fpw = rec.first().copied().unwrap_or(0) != 0;
        if fpw { "true".to_string() } else { "false".to_string() }
    } else if info == XLOG_END_OF_RECOVERY {
        // xl_end_of_recovery { end_time: i64, ThisTimeLineID: u32, PrevTimeLineID: u32, wal_level: i32 }
        let end_time = super::util::i64_at(rec, 0);
        let this_tli = u32_at(rec, 8);
        let prev_tli = u32_at(rec, 12);
        let wal_level = i32_at(rec, 16);
        format!(
            "tli {this_tli}; prev tli {prev_tli}; time ts {end_time}; wal_level {}",
            wal_level_str(wal_level)
        )
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        // xl_overwrite_contrecord { overwritten_lsn: u64, overwrite_time: i64 }
        let lsn = u64_at(rec, 0);
        let time = super::util::i64_at(rec, 8);
        format!("lsn {}; time ts {time}", lsn_str(lsn))
    } else if info == XLOG_CHECKPOINT_REDO {
        format!("wal_level {}", wal_level_str(i32_at(rec, 0)))
    } else {
        String::new()
    }
}

/// C `xlog_identify`.
#[must_use]
pub fn xlog_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_CHECKPOINT_SHUTDOWN => "CHECKPOINT_SHUTDOWN",
        x if x == XLOG_CHECKPOINT_ONLINE => "CHECKPOINT_ONLINE",
        x if x == XLOG_NOOP => "NOOP",
        x if x == XLOG_NEXTOID => "NEXTOID",
        x if x == XLOG_SWITCH => "SWITCH",
        x if x == XLOG_BACKUP_END => "BACKUP_END",
        x if x == XLOG_PARAMETER_CHANGE => "PARAMETER_CHANGE",
        x if x == XLOG_RESTORE_POINT => "RESTORE_POINT",
        x if x == XLOG_FPW_CHANGE => "FPW_CHANGE",
        x if x == XLOG_END_OF_RECOVERY => "END_OF_RECOVERY",
        x if x == XLOG_OVERWRITE_CONTRECORD => "OVERWRITE_CONTRECORD",
        x if x == XLOG_FPI => "FPI",
        x if x == XLOG_FPI_FOR_HINT => "FPI_FOR_HINT",
        x if x == XLOG_CHECKPOINT_REDO => "CHECKPOINT_REDO",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const XLOG_RMID: u8 = 0;

    #[test]
    fn nextoid_renders() {
        let data = 99999u32.to_ne_bytes();
        let rec = record_with_data(XLOG_RMID, XLOG_NEXTOID, &data);
        assert_eq!(xlog_identify(XLOG_NEXTOID), Some("NEXTOID"));
        assert_eq!(xlog_desc(&rec), "99999");
    }

    #[test]
    fn checkpoint_shutdown_renders_fields() {
        // Build a CheckPoint body (88 bytes covering through oldestActiveXid).
        let mut data = vec![0u8; 88];
        data[0..8].copy_from_slice(&0x0100_0000_2000u64.to_ne_bytes()); // redo
        data[8..12].copy_from_slice(&1u32.to_ne_bytes()); // ThisTimeLineID
        data[12..16].copy_from_slice(&1u32.to_ne_bytes()); // PrevTimeLineID
        data[16] = 1; // fullPageWrites
        data[20..24].copy_from_slice(&1i32.to_ne_bytes()); // wal_level = replica
        data[24..32].copy_from_slice(&5000u64.to_ne_bytes()); // nextXid (epoch 0, xid 5000)
        data[32..36].copy_from_slice(&16400u32.to_ne_bytes()); // nextOid
        let rec = record_with_data(XLOG_RMID, XLOG_CHECKPOINT_SHUTDOWN, &data);
        assert_eq!(
            xlog_identify(XLOG_CHECKPOINT_SHUTDOWN),
            Some("CHECKPOINT_SHUTDOWN")
        );
        let s = xlog_desc(&rec);
        assert!(s.contains("wal_level replica"), "rendered: {s}");
        assert!(s.contains("xid 0:5000"), "rendered: {s}");
        assert!(s.contains("oid 16400"), "rendered: {s}");
        assert!(s.contains("fpw true"), "rendered: {s}");
        assert!(s.ends_with("shutdown"), "rendered: {s}");
    }
}
