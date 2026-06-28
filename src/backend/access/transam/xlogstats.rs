//! WAL statistics accumulation. Translated from backend/access/transam/xlogstats.c.
//!
//! Provides the functions that tally write-ahead-log usage as records are read
//! back. A record's total length is split into its full-page-image (FPI) part
//! and everything else, and those byte counts plus a record tally are folded
//! into running totals. Statistics are kept two ways: per resource manager
//! (rmgr), and per record type, where a record type is identified by the rmgr
//! plus the four bits of the info field that belong to the rmgr (sixteen
//! possible entries per rmgr). XACT records are special-cased: the high bit of
//! those four is an optional flag, so it is masked off and only the three-bit
//! opcode identifies the record. These counts back tools such as pg_waldump and
//! the WAL-statistics views.
//!
//! The logic is pure value computation over an already-decoded record and does
//! not depend on shared state, so it is a faithful translation of the C source.
//! The C out-parameters of `XLogRecGetLen` are folded into a returned
//! `(rec_len, fpi_len)` tuple. Both entry points also have header-compatible
//! shims taking an `XLogReaderState`, which read its most recently decoded
//! record.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use crate::access::rmgrlist::RmgrId as BuiltinRmgrId;
use crate::access::xlogreader::{DecodedXLogRecord, XLogReaderState};
use crate::access::xlogstats::XLogStats;

/// PG `XLogRecGetLen`: split a decoded record's total length into `(rec_len,
/// fpi_len)`, where `fpi_len` is the bytes occupied by full-page images and
/// `rec_len` is everything else. Out-params folded into the returned tuple.
pub fn rec_get_len(record: &DecodedXLogRecord) -> (u32, u32) {
    // Sum the FPI bytes across all block references that carry an image.
    let mut fpi_len: u32 = 0;
    for block_id in 0..=record.max_block_id() {
        if !record.has_block_ref(block_id) {
            continue;
        }
        if record.has_block_image(block_id as usize) {
            fpi_len += u32::from(record.block(block_id as usize).bimg_len);
        }
    }
    let rec_len = record.total_len() - fpi_len;
    (rec_len, fpi_len)
}

/// PG `XLogRecStoreStats`: fold a decoded record into the running [`XLogStats`].
pub fn store_stats(stats: &mut XLogStats, record: &DecodedXLogRecord) {
    stats.count += 1;

    let rmid = record.rmid();
    let (rec_len, fpi_len) = rec_get_len(record);

    // Per-rmgr statistics.
    let r = &mut stats.rmgr_stats[rmid as usize];
    r.count += 1;
    r.rec_len += u64::from(rec_len);
    r.fpi_len += u64::from(fpi_len);

    // Per-record statistics: keyed by RmgrId plus the four rmgr-owned high bits
    // of xl_info (sixteen possible entries per RmgrId).
    let mut recid = record.info() >> 4;

    // XACT records use the first of those four bits as an optional flag and the
    // remaining three as the opcode; mask to the opcode so flag variants merge.
    if rmid == BuiltinRmgrId::Xact as u8 {
        recid &= 0x07;
    }

    let rs = &mut stats.record_stats[rmid as usize][recid as usize];
    rs.count += 1;
    rs.rec_len += u64::from(rec_len);
    rs.fpi_len += u64::from(fpi_len);
}

/// Header-compatible `XLogRecGetLen(record)` shim: reads the reader's most
/// recently decoded record. Out-params folded into the returned tuple.
pub fn XLogRecGetLen(record: &mut XLogReaderState) -> (u32, u32) {
    let decoded = record.record.as_ref().expect("no decoded record");
    rec_get_len(decoded)
}

/// Header-compatible `XLogRecStoreStats(stats, record)` shim.
pub fn XLogRecStoreStats(stats: &mut XLogStats, record: &mut XLogReaderState) {
    let decoded = record.record.as_ref().expect("no decoded record");
    store_stats(stats, decoded);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::xlogdefs::{XLogRecPtr, INVALID_XLOG_REC_PTR};
    use crate::access::xlogreader::{DecodedBkpBlock, DecodedXLogRecord};
    use crate::access::xlogrecord::XLogRecord;
    use crate::access::xlogstats::XLogStats;
    use crate::common::relpath::ForkNumber;
    use crate::postgres_ext::Oid;
    use crate::storage::relfilelocator::RelFileLocator;

    fn empty_stats() -> XLogStats {
        XLogStats::default()
    }

    fn bkp_block(bimg_len: u16, has_image: bool) -> DecodedBkpBlock {
        DecodedBkpBlock {
            in_use: true,
            rlocator: RelFileLocator { spcOid: Oid(1), dbOid: Oid(2), relNumber: Oid(3) },
            forknum: ForkNumber::MAIN_FORKNUM,
            blkno: 0,
            prefetch_buffer: crate::storage::buf::INVALID_BUFFER,
            flags: 0,
            has_image,
            apply_image: has_image,
            bkp_image: None,
            hole_offset: 0,
            hole_length: 0,
            bimg_len,
            bimg_info: 0,
            has_data: false,
            data: None,
            data_len: 0,
            data_bufsz: 0,
        }
    }

    fn decoded(rmid: u8, info: u8, tot_len: u32, blocks: Vec<DecodedBkpBlock>) -> DecodedXLogRecord {
        let max_block_id = blocks.len() as i32 - 1;
        DecodedXLogRecord {
            size: 0,
            oversized: false,
            lsn: XLogRecPtr(100),
            next_lsn: XLogRecPtr(100 + u64::from(tot_len)),
            header: XLogRecord {
                tot_len,
                xid: crate::c::InvalidTransactionId,
                prev: INVALID_XLOG_REC_PTR,
                info,
                rmid,
                crc: 0,
            },
            record_origin: crate::replication::origin::InvalidRepOriginId,
            toplevel_xid: crate::c::InvalidTransactionId,
            main_data: None,
            main_data_len: 0,
            max_block_id,
            blocks,
        }
    }

    #[test]
    fn rec_get_len_splits_fpi() {
        // tot_len 1000, one block with a 700-byte image -> rec_len 300, fpi 700.
        let rec = decoded(0, 0, 1000, vec![bkp_block(700, true)]);
        assert_eq!(rec_get_len(&rec), (300, 700));
    }

    #[test]
    fn rec_get_len_no_image() {
        let rec = decoded(0, 0, 250, vec![bkp_block(0, false)]);
        assert_eq!(rec_get_len(&rec), (250, 0));
    }

    #[test]
    fn store_stats_tallies_per_rmgr_and_record() {
        let mut stats = empty_stats();
        // Heap rmgr (id 10), info high nibble 0x30 -> recid 3.
        let rmid = BuiltinRmgrId::Heap as u8;
        let rec = decoded(rmid, 0x30, 1000, vec![bkp_block(600, true)]);
        store_stats(&mut stats, &rec);

        assert_eq!(stats.count, 1);
        assert_eq!(stats.rmgr_stats[rmid as usize].count, 1);
        assert_eq!(stats.rmgr_stats[rmid as usize].rec_len, 400);
        assert_eq!(stats.rmgr_stats[rmid as usize].fpi_len, 600);
        assert_eq!(stats.record_stats[rmid as usize][3].count, 1);
        assert_eq!(stats.record_stats[rmid as usize][3].fpi_len, 600);
    }

    #[test]
    fn store_stats_masks_xact_opcode() {
        let mut stats = empty_stats();
        let rmid = BuiltinRmgrId::Xact as u8;
        // info 0xB0 -> high nibble 0xB = 0b1011; masked to opcode 0x3.
        let rec = decoded(rmid, 0xB0, 100, vec![]);
        store_stats(&mut stats, &rec);
        assert_eq!(stats.record_stats[rmid as usize][3].count, 1);
        // The unmasked slot 0xB must stay empty.
        assert_eq!(stats.record_stats[rmid as usize][0xB].count, 0);
    }
}
