//! rmgr descriptor routines for access/transam/clog.c. Translated from
//! backend/access/rmgrdesc/clogdesc.c: `clog_desc` / `clog_identify`.

use super::rmgr_info;
use super::util::{i64_at, u32_at};
use crate::access::clog::{CLOG_TRUNCATE, CLOG_ZEROPAGE};
use crate::access::xlogreader::DecodedXLogRecord;

/// C `clog_desc`.
#[must_use]
pub fn clog_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info);

    if info == CLOG_ZEROPAGE {
        // int64 pageno
        format!("page {}", i64_at(rec, 0))
    } else if info == CLOG_TRUNCATE {
        // xl_clog_truncate { pageno: i64, oldestXact: u32, oldestXactDb: Oid }
        let pageno = i64_at(rec, 0);
        let oldest_xact = u32_at(rec, 8);
        format!("page {pageno}; oldestXact {oldest_xact}")
    } else {
        String::new()
    }
}

/// C `clog_identify`.
#[must_use]
pub fn clog_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == CLOG_ZEROPAGE => "ZEROPAGE",
        x if x == CLOG_TRUNCATE => "TRUNCATE",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const CLOG_RMID: u8 = 3;

    #[test]
    fn zeropage_renders() {
        let data = 42i64.to_ne_bytes();
        let rec = record_with_data(CLOG_RMID, CLOG_ZEROPAGE, &data);
        assert_eq!(clog_identify(CLOG_ZEROPAGE), Some("ZEROPAGE"));
        assert_eq!(clog_desc(&rec), "page 42");
    }

    #[test]
    fn truncate_renders() {
        let mut data = Vec::new();
        data.extend_from_slice(&100i64.to_ne_bytes());
        data.extend_from_slice(&555u32.to_ne_bytes());
        data.extend_from_slice(&0u32.to_ne_bytes());
        let rec = record_with_data(CLOG_RMID, CLOG_TRUNCATE, &data);
        assert_eq!(clog_identify(CLOG_TRUNCATE), Some("TRUNCATE"));
        assert_eq!(clog_desc(&rec), "page 100; oldestXact 555");
    }
}
