//! rmgr descriptor routines for catalog/storage.c. Translated from
//! backend/access/rmgrdesc/smgrdesc.c: `smgr_desc` / `smgr_identify`.

use super::rmgr_info;
use super::util::{i32_at, u32_at};
use crate::access::xlogreader::DecodedXLogRecord;
use crate::catalog::storage_xlog::{XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE};
use crate::common::relpath::{relpathperm, ForkNumber};
use crate::postgres_ext::Oid;
use crate::storage::relfilelocator::RelFileLocator;

/// Fork number from the on-disk enum discriminant (only the four defined forks
/// occur in a valid record).
fn fork_from(n: u32) -> ForkNumber {
    match n {
        1 => ForkNumber::FSM_FORKNUM,
        2 => ForkNumber::VISIBILITYMAP_FORKNUM,
        3 => ForkNumber::INIT_FORKNUM,
        _ => ForkNumber::MAIN_FORKNUM,
    }
}

/// C `smgr_desc`.
#[must_use]
pub fn smgr_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info);

    if info == XLOG_SMGR_CREATE {
        // xl_smgr_create { rlocator: RelFileLocator(3xu32), forkNum: ForkNumber(i32) }
        let rlocator = RelFileLocator {
            spcOid: Oid::new(u32_at(rec, 0)),
            dbOid: Oid::new(u32_at(rec, 4)),
            relNumber: Oid::new(u32_at(rec, 8)),
        };
        let fork = fork_from(u32_at(rec, 12));
        relpathperm(rlocator, fork).str
    } else if info == XLOG_SMGR_TRUNCATE {
        // xl_smgr_truncate { blkno: u32, rlocator: RelFileLocator(3xu32), flags: i32 }
        let blkno = u32_at(rec, 0);
        let rlocator = RelFileLocator {
            spcOid: Oid::new(u32_at(rec, 4)),
            dbOid: Oid::new(u32_at(rec, 8)),
            relNumber: Oid::new(u32_at(rec, 12)),
        };
        let flags = i32_at(rec, 16);
        format!(
            "{} to {blkno} blocks flags {flags}",
            relpathperm(rlocator, ForkNumber::MAIN_FORKNUM).str
        )
    } else {
        String::new()
    }
}

/// C `smgr_identify`.
#[must_use]
pub fn smgr_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_SMGR_CREATE => "CREATE",
        x if x == XLOG_SMGR_TRUNCATE => "TRUNCATE",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const SMGR_RMID: u8 = 2;

    #[test]
    fn create_renders_relpath() {
        let mut data = Vec::new();
        data.extend_from_slice(&1663u32.to_ne_bytes()); // spc
        data.extend_from_slice(&5u32.to_ne_bytes()); // db
        data.extend_from_slice(&16384u32.to_ne_bytes()); // rel
        data.extend_from_slice(&0i32.to_ne_bytes()); // MAIN_FORKNUM
        let rec = record_with_data(SMGR_RMID, XLOG_SMGR_CREATE, &data);
        assert_eq!(smgr_identify(XLOG_SMGR_CREATE), Some("CREATE"));
        // relpathperm renders the tablespace/db/rel path; the rel number appears.
        let s = smgr_desc(&rec);
        assert!(s.contains("16384"), "rendered: {s}");
    }

    #[test]
    fn truncate_renders_blocks() {
        let mut data = Vec::new();
        data.extend_from_slice(&99u32.to_ne_bytes()); // blkno
        data.extend_from_slice(&1663u32.to_ne_bytes()); // spc
        data.extend_from_slice(&5u32.to_ne_bytes()); // db
        data.extend_from_slice(&16384u32.to_ne_bytes()); // rel
        data.extend_from_slice(&7i32.to_ne_bytes()); // flags
        let rec = record_with_data(SMGR_RMID, XLOG_SMGR_TRUNCATE, &data);
        assert_eq!(smgr_identify(XLOG_SMGR_TRUNCATE), Some("TRUNCATE"));
        let s = smgr_desc(&rec);
        assert!(s.contains("to 99 blocks flags 7"), "rendered: {s}");
    }
}
