//! rmgr descriptor routines for commands/dbcommands.c. Translated from
//! backend/access/rmgrdesc/dbasedesc.c: `dbase_desc` / `dbase_identify`.

use std::fmt::Write as _;

use super::rmgr_info;
use super::util::{i32_at, u32_at};
use crate::access::xlogreader::DecodedXLogRecord;
use crate::commands::dbcommands_xlog::{
    XLOG_DBASE_CREATE_FILE_COPY, XLOG_DBASE_CREATE_WAL_LOG, XLOG_DBASE_DROP,
};

/// C `dbase_desc`.
#[must_use]
pub fn dbase_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info);

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        // xl_dbase_create_file_copy_rec { db_id, tablespace_id, src_db_id, src_tablespace_id }
        let db_id = u32_at(rec, 0);
        let tablespace_id = u32_at(rec, 4);
        let src_db_id = u32_at(rec, 8);
        let src_tablespace_id = u32_at(rec, 12);
        format!("copy dir {src_tablespace_id}/{src_db_id} to {tablespace_id}/{db_id}")
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        // xl_dbase_create_wal_log_rec { db_id, tablespace_id }
        let db_id = u32_at(rec, 0);
        let tablespace_id = u32_at(rec, 4);
        format!("create dir {tablespace_id}/{db_id}")
    } else if info == XLOG_DBASE_DROP {
        // xl_dbase_drop_rec { db_id: Oid, ntablespaces: i32, tablespace_ids: [Oid] }
        let db_id = u32_at(rec, 0);
        let ntablespaces = i32_at(rec, 4);
        let mut out = String::from("dir");
        for i in 0..ntablespaces.max(0) as usize {
            let ts = u32_at(rec, 8 + i * 4);
            let _ = write!(out, " {ts}/{db_id}");
        }
        out
    } else {
        String::new()
    }
}

/// C `dbase_identify`.
#[must_use]
pub fn dbase_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_DBASE_CREATE_FILE_COPY => "CREATE_FILE_COPY",
        x if x == XLOG_DBASE_CREATE_WAL_LOG => "CREATE_WAL_LOG",
        x if x == XLOG_DBASE_DROP => "DROP",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const DBASE_RMID: u8 = 4;

    #[test]
    fn create_wal_log_renders() {
        let mut data = Vec::new();
        data.extend_from_slice(&16400u32.to_ne_bytes()); // db_id
        data.extend_from_slice(&1663u32.to_ne_bytes()); // tablespace_id
        let rec = record_with_data(DBASE_RMID, XLOG_DBASE_CREATE_WAL_LOG, &data);
        assert_eq!(
            dbase_identify(XLOG_DBASE_CREATE_WAL_LOG),
            Some("CREATE_WAL_LOG")
        );
        assert_eq!(dbase_desc(&rec), "create dir 1663/16400");
    }

    #[test]
    fn drop_renders_tablespaces() {
        let mut data = Vec::new();
        data.extend_from_slice(&16400u32.to_ne_bytes()); // db_id
        data.extend_from_slice(&2i32.to_ne_bytes()); // ntablespaces
        data.extend_from_slice(&1663u32.to_ne_bytes());
        data.extend_from_slice(&1664u32.to_ne_bytes());
        let rec = record_with_data(DBASE_RMID, XLOG_DBASE_DROP, &data);
        assert_eq!(dbase_identify(XLOG_DBASE_DROP), Some("DROP"));
        assert_eq!(dbase_desc(&rec), "dir 1663/16400 1664/16400");
    }
}
