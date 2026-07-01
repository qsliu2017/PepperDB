//! Directory module: src/backend/access/rmgrdesc
//!
//! rmgr descriptor routines: each resource manager's `rm_desc` (render a decoded
//! WAL record's fields as a human-readable string, for pg_waldump / recovery
//! logging) and `rm_identify` (the record-type tag name). These render over the
//! [`DecodedXLogRecord`](crate::access::xlogreader::DecodedXLogRecord) that the
//! WAL reader produces; the per-record dispatch by rmid lives in
//! [`describe_wal_record`].
//!
//! The 8 modules here cover the reachable rmgrs (heap, heap2, btree, xact, xlog,
//! clog, smgr, dbase, seq). The other ~14 rmgrdesc files (gin/gist/brin/spg/
//! hash/logical/standby/...) are staged: their records fall through to a safe
//! fallback string (never a panic), and they land as their access methods do.

pub mod clogdesc;
pub mod dbasedesc;
pub mod heapdesc;
pub mod nbtdesc;
pub mod seqdesc;
pub mod smgrdesc;
pub mod xactdesc;
pub mod xlogdesc;

mod util;

use crate::access::rmgr::RmgrId;
use crate::access::rmgrlist::RmgrId as BuiltinRmgrId;
use crate::access::xlogreader::DecodedXLogRecord;
use crate::access::xlogrecord::XLR_INFO_MASK;

/// PG `xlog_outdesc` core: render a decoded WAL record as `"<Rmgr>/<TAG>: <desc>"`,
/// dispatching by `xl_rmid` to the resource manager's `rm_desc` + `rm_identify`.
///
/// The rmgr-owned bits of `xl_info` (high nibble) select the record type. An
/// unregistered rmid, or a record type an implemented rmgr does not recognize,
/// falls back to a safe placeholder instead of panicking - descriptions must
/// never crash recovery or pg_waldump.
#[must_use]
pub fn describe_wal_record(record: &DecodedXLogRecord) -> String {
    let rmid = record.header.rmid;
    let info = record.header.info;

    let (rm_name, tag, body) = match builtin(rmid) {
        Some(BuiltinRmgrId::Heap) => (
            BuiltinRmgrId::Heap.name(),
            heapdesc::heap_identify(info),
            heapdesc::heap_desc(record),
        ),
        Some(BuiltinRmgrId::Heap2) => (
            BuiltinRmgrId::Heap2.name(),
            heapdesc::heap2_identify(info),
            heapdesc::heap2_desc(record),
        ),
        Some(BuiltinRmgrId::Btree) => (
            BuiltinRmgrId::Btree.name(),
            nbtdesc::btree_identify(info),
            nbtdesc::btree_desc(record),
        ),
        Some(BuiltinRmgrId::Xact) => (
            BuiltinRmgrId::Xact.name(),
            xactdesc::xact_identify(info),
            xactdesc::xact_desc(record),
        ),
        Some(BuiltinRmgrId::Xlog) => (
            BuiltinRmgrId::Xlog.name(),
            xlogdesc::xlog_identify(info),
            xlogdesc::xlog_desc(record),
        ),
        Some(BuiltinRmgrId::Clog) => (
            BuiltinRmgrId::Clog.name(),
            clogdesc::clog_identify(info),
            clogdesc::clog_desc(record),
        ),
        Some(BuiltinRmgrId::Smgr) => (
            BuiltinRmgrId::Smgr.name(),
            smgrdesc::smgr_identify(info),
            smgrdesc::smgr_desc(record),
        ),
        Some(BuiltinRmgrId::Dbase) => (
            BuiltinRmgrId::Dbase.name(),
            dbasedesc::dbase_identify(info),
            dbasedesc::dbase_desc(record),
        ),
        Some(BuiltinRmgrId::Seq) => (
            BuiltinRmgrId::Seq.name(),
            seqdesc::seq_identify(info),
            seqdesc::seq_desc(record),
        ),
        // A registered-but-not-yet-described rmgr (gin/gist/brin/...), or an
        // unregistered id: safe fallback, no panic.
        other => {
            let name = other.map_or("UNKNOWN", BuiltinRmgrId::name);
            (name, None, String::new())
        }
    };

    let tag = tag.unwrap_or("UNKNOWN");
    format!("{rm_name}/{tag}: {body}")
}

/// The builtin `RmgrId` for a raw id, or `None` if it is not a registered
/// builtin (a custom / out-of-range id). Never panics.
fn builtin(rmid: RmgrId) -> Option<BuiltinRmgrId> {
    Some(match rmid {
        0 => BuiltinRmgrId::Xlog,
        1 => BuiltinRmgrId::Xact,
        2 => BuiltinRmgrId::Smgr,
        3 => BuiltinRmgrId::Clog,
        4 => BuiltinRmgrId::Dbase,
        5 => BuiltinRmgrId::Tblspc,
        6 => BuiltinRmgrId::Multixact,
        7 => BuiltinRmgrId::Relmap,
        8 => BuiltinRmgrId::Standby,
        9 => BuiltinRmgrId::Heap2,
        10 => BuiltinRmgrId::Heap,
        11 => BuiltinRmgrId::Btree,
        12 => BuiltinRmgrId::Hash,
        13 => BuiltinRmgrId::Gin,
        14 => BuiltinRmgrId::Gist,
        15 => BuiltinRmgrId::Seq,
        16 => BuiltinRmgrId::Spgist,
        17 => BuiltinRmgrId::Brin,
        18 => BuiltinRmgrId::CommitTs,
        19 => BuiltinRmgrId::Replorigin,
        20 => BuiltinRmgrId::Generic,
        21 => BuiltinRmgrId::Logicalmsg,
        _ => return None,
    })
}

/// The rmgr-owned info bits (high nibble): `info & ~XLR_INFO_MASK`, the value the
/// `*_identify` / `*_desc` routines switch on.
#[inline]
#[must_use]
pub(crate) fn rmgr_info(info: u8) -> u8 {
    info & !XLR_INFO_MASK
}

/// Test-only builders for [`DecodedXLogRecord`]s: the desc routines render over a
/// decoded record's fields, so tests assemble a minimal one directly (rmid +
/// info + main_data), bypassing the full WAL read path.
#[cfg(test)]
pub(crate) mod test_util {
    use crate::access::xlogdefs::{RepOriginId, XLogRecPtr, INVALID_XLOG_REC_PTR};
    use crate::access::xlogreader::DecodedXLogRecord;
    use crate::access::xlogrecord::XLogRecord;
    use crate::c::{InvalidTransactionId, TransactionId};

    /// A decoded record carrying `rmid`/`info` and `main_data`, one block flagged
    /// as carrying `block0` data when `Some` (so `get_block_data(0)` succeeds).
    #[must_use]
    pub fn record_with_data_and_block(
        rmid: u8,
        info: u8,
        main_data: &[u8],
        block0: Option<&[u8]>,
    ) -> DecodedXLogRecord {
        use crate::access::xlogreader::DecodedBkpBlock;
        use crate::common::relpath::ForkNumber;
        use crate::postgres_ext::Oid;
        use crate::storage::buf::INVALID_BUFFER;
        use crate::storage::relfilelocator::RelFileLocator;

        let mut blk = DecodedBkpBlock {
            in_use: false,
            rlocator: RelFileLocator {
                spcOid: Oid::new(0),
                dbOid: Oid::new(0),
                relNumber: Oid::new(0),
            },
            forknum: ForkNumber::MAIN_FORKNUM,
            blkno: 0,
            prefetch_buffer: INVALID_BUFFER,
            flags: 0,
            has_image: false,
            apply_image: false,
            bkp_image: None,
            hole_offset: 0,
            hole_length: 0,
            bimg_len: 0,
            bimg_info: 0,
            has_data: false,
            data: None,
            data_len: 0,
            data_bufsz: 0,
        };
        let max_block_id = if let Some(d) = block0 {
            blk.in_use = true;
            blk.has_data = true;
            blk.data = Some(d.to_vec());
            blk.data_len = u16::try_from(d.len()).unwrap_or(u16::MAX);
            0
        } else {
            -1
        };

        DecodedXLogRecord {
            size: 0,
            oversized: false,
            lsn: XLogRecPtr(0),
            next_lsn: INVALID_XLOG_REC_PTR,
            header: XLogRecord {
                tot_len: 0,
                xid: TransactionId(0),
                prev: INVALID_XLOG_REC_PTR,
                info,
                rmid,
                crc: 0,
            },
            record_origin: RepOriginId(0),
            toplevel_xid: InvalidTransactionId,
            main_data: Some(main_data.to_vec()),
            main_data_len: u32::try_from(main_data.len()).unwrap_or(u32::MAX),
            max_block_id,
            blocks: vec![blk],
        }
    }

    /// [`record_with_data_and_block`] with no block data.
    #[must_use]
    pub fn record_with_data(rmid: u8, info: u8, main_data: &[u8]) -> DecodedXLogRecord {
        record_with_data_and_block(rmid, info, main_data, None)
    }
}

#[cfg(test)]
mod tests {
    use super::describe_wal_record;
    use super::test_util::record_with_data;

    #[test]
    fn dispatches_heap_insert() {
        // Heap rmid = 10, XLOG_HEAP_INSERT = 0x00. xl_heap_insert { off: 5, flags: 0x03 }.
        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_ne_bytes());
        data.push(0x03);
        let rec = record_with_data(10, crate::access::heapam_xlog::XLOG_HEAP_INSERT, &data);
        assert_eq!(describe_wal_record(&rec), "Heap/INSERT: off: 5, flags: 0x03");
    }

    #[test]
    fn dispatches_xact_commit() {
        // Xact rmid = 1, XLOG_XACT_COMMIT = 0x00. xl_xact_commit { xact_time: i64 }.
        let data = 777i64.to_ne_bytes();
        let rec = record_with_data(1, crate::access::xact::XLOG_XACT_COMMIT, &data);
        assert_eq!(describe_wal_record(&rec), "Transaction/COMMIT: ts 777");
    }

    #[test]
    fn unknown_rmid_is_safe_fallback_not_panic() {
        // An out-of-range rmid must fall back to "UNKNOWN/UNKNOWN: " without panic.
        let rec = record_with_data(200, 0x00, &[]);
        let s = describe_wal_record(&rec);
        assert_eq!(s, "UNKNOWN/UNKNOWN: ");
    }

    #[test]
    fn staged_rmgr_is_safe_fallback_not_panic() {
        // A registered-but-not-described rmgr (Gin = 13): named, empty body, no panic.
        let rec = record_with_data(13, 0x00, &[]);
        let s = describe_wal_record(&rec);
        assert_eq!(s, "Gin/UNKNOWN: ");
    }
}
