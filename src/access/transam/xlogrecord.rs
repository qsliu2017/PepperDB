//! WAL record serialization and deserialization.
//!
//! Record header (simplified XLogRecord):
//!   xl_tot_len (4B), xl_xid (4B), xl_prev (8B), xl_info (1B), xl_rmid (1B),
//!   padding (2B), xl_crc (4B) = 24 bytes total.
//!
//! Resource manager IDs: RM_HEAP_ID = 1 (matches PostgreSQL).
//! Heap record types: INSERT=0x00, DELETE=0x10, UPDATE=0x20.

pub const RECORD_HEADER_SIZE: usize = 24;

pub const RM_HEAP_ID: u8 = 1;
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub xl_xid: u32,
    pub xl_info: u8,
    pub xl_rmid: u8,
    pub data: Vec<u8>,
}

/// Serialize a WAL record into bytes (header + data).
pub fn serialize_record(rec: &WalRecord, prev_lsn: u64) -> Vec<u8> {
    let tot_len = (RECORD_HEADER_SIZE + rec.data.len()) as u32;
    let mut buf = Vec::with_capacity(tot_len as usize);

    buf.extend_from_slice(&tot_len.to_le_bytes()); // 0: xl_tot_len
    buf.extend_from_slice(&rec.xl_xid.to_le_bytes()); // 4: xl_xid
    buf.extend_from_slice(&prev_lsn.to_le_bytes()); // 8: xl_prev
    buf.push(rec.xl_info); // 16: xl_info
    buf.push(rec.xl_rmid); // 17: xl_rmid
    buf.extend_from_slice(&[0u8; 2]); // 18: padding
                                      // CRC placeholder at byte 20, compute over header[0..20] + data
    let crc_data_len = 20 + rec.data.len();
    let mut crc_buf = Vec::with_capacity(crc_data_len);
    crc_buf.extend_from_slice(&buf[..20]);
    crc_buf.extend_from_slice(&rec.data);
    let crc = crc32c::crc32c(&crc_buf);
    buf.extend_from_slice(&crc.to_le_bytes()); // 20: xl_crc

    buf.extend_from_slice(&rec.data); // 24+: data
    buf
}

/// Deserialize a WAL record from bytes. Returns (record, bytes_consumed).
pub fn deserialize_record(buf: &[u8]) -> Option<(WalRecord, usize)> {
    if buf.len() < RECORD_HEADER_SIZE {
        return None;
    }

    let tot_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if tot_len < RECORD_HEADER_SIZE || buf.len() < tot_len {
        return None;
    }

    let xl_xid = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let xl_info = buf[16];
    let xl_rmid = buf[17];
    let stored_crc = u32::from_le_bytes(buf[20..24].try_into().unwrap());

    let data_len = tot_len - RECORD_HEADER_SIZE;
    let data = buf[RECORD_HEADER_SIZE..tot_len].to_vec();

    // Verify CRC: header[0..20] + data
    let mut crc_buf = Vec::with_capacity(20 + data_len);
    crc_buf.extend_from_slice(&buf[..20]);
    crc_buf.extend_from_slice(&data);
    let computed_crc = crc32c::crc32c(&crc_buf);
    if stored_crc != computed_crc {
        return None;
    }

    Some((
        WalRecord {
            xl_xid,
            xl_info,
            xl_rmid,
            data,
        },
        tot_len,
    ))
}

// -- Heap record data builders ------------------------------------------------

/// Build INSERT record data: relfilenode (4B), blkno (4B), item_offset (2B), tuple bytes.
pub fn build_heap_insert_data(rfn: u32, blkno: u32, item_off: u16, tuple: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(10 + tuple.len());
    data.extend_from_slice(&rfn.to_le_bytes());
    data.extend_from_slice(&blkno.to_le_bytes());
    data.extend_from_slice(&item_off.to_le_bytes());
    data.extend_from_slice(tuple);
    data
}

/// Build DELETE record data: relfilenode (4B), blkno (4B), item_offset (2B).
pub fn build_heap_delete_data(rfn: u32, blkno: u32, item_off: u16) -> Vec<u8> {
    let mut data = Vec::with_capacity(10);
    data.extend_from_slice(&rfn.to_le_bytes());
    data.extend_from_slice(&blkno.to_le_bytes());
    data.extend_from_slice(&item_off.to_le_bytes());
    data
}

/// Parse heap record common fields: (relfilenode, blkno, item_offset).
pub fn parse_heap_data(data: &[u8]) -> Option<(u32, u32, u16)> {
    if data.len() < 10 {
        return None;
    }
    let rfn = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let blkno = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let item_off = u16::from_le_bytes(data[8..10].try_into().unwrap());
    Some((rfn, blkno, item_off))
}

/// Extract tuple bytes from an INSERT/UPDATE record.
pub fn parse_heap_tuple(data: &[u8]) -> Option<&[u8]> {
    if data.len() > 10 {
        Some(&data[10..])
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn record_serde_round_trip() {
        let rec = WalRecord {
            xl_xid: 42,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: vec![1, 2, 3, 4],
        };
        let bytes = serialize_record(&rec, 0x1000);
        let (deserialized, consumed) = deserialize_record(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(deserialized.xl_xid, 42);
        assert_eq!(deserialized.xl_info, XLOG_HEAP_INSERT);
        assert_eq!(deserialized.xl_rmid, RM_HEAP_ID);
        assert_eq!(deserialized.data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn record_crc_detects_corruption() {
        let rec = WalRecord {
            xl_xid: 1,
            xl_info: 0,
            xl_rmid: 0,
            data: vec![0xFF],
        };
        let mut bytes = serialize_record(&rec, 0);
        bytes[24] ^= 0x01; // flip data byte
        assert!(deserialize_record(&bytes).is_none());
    }
}
