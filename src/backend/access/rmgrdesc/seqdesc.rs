//! rmgr descriptor routines for commands/sequence.c. Translated from
//! backend/access/rmgrdesc/seqdesc.c: `seq_desc` / `seq_identify`.

use super::rmgr_info;
use super::util::u32_at;
use crate::access::xlogreader::DecodedXLogRecord;
use crate::commands::sequence::XLOG_SEQ_LOG;

/// C `seq_desc`.
#[must_use]
pub fn seq_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info);

    if info == XLOG_SEQ_LOG {
        // xl_seq_rec { locator: RelFileLocator(3xu32) }
        let spc = u32_at(rec, 0);
        let db = u32_at(rec, 4);
        let rel = u32_at(rec, 8);
        format!("rel {spc}/{db}/{rel}")
    } else {
        String::new()
    }
}

/// C `seq_identify`.
#[must_use]
pub fn seq_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_SEQ_LOG => "LOG",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const SEQ_RMID: u8 = 15;

    #[test]
    fn seq_log_renders() {
        let mut data = Vec::new();
        data.extend_from_slice(&1663u32.to_ne_bytes());
        data.extend_from_slice(&5u32.to_ne_bytes());
        data.extend_from_slice(&16384u32.to_ne_bytes());
        let rec = record_with_data(SEQ_RMID, XLOG_SEQ_LOG, &data);
        assert_eq!(seq_identify(XLOG_SEQ_LOG), Some("LOG"));
        assert_eq!(seq_desc(&rec), "rel 1663/5/16384");
    }
}
