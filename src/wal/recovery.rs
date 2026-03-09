//! WAL recovery: read segment files and replay records for crash recovery.
//! On unclean shutdown (pg_control state != DB_SHUTDOWNED), replays WAL from
//! the last checkpoint LSN, applying heap changes idempotently (skip if
//! page pd_lsn >= record LSN).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::storage::disk::{DiskManager, PAGE_SIZE};
use crate::storage::heap;

use super::record::{self, WalRecord};
use super::writer;

const WAL_PAGE_SIZE: usize = 8192;
const PAGE_HEADER_SIZE: usize = 16;
const SEGMENT_SIZE: usize = 16 * 1024 * 1024;
const XLP_MAGIC: u16 = 0xD106;

/// Sequential WAL reader: reads records from segment files starting at a given LSN.
pub struct WalReader {
    wal_dir: PathBuf,
    current_lsn: u64,
    segment_buf: Option<Vec<u8>>,
    segment_no: u64,
}

impl WalReader {
    pub fn new(wal_dir: &Path, start_lsn: u64) -> Self {
        let segment_no = start_lsn / SEGMENT_SIZE as u64;
        Self {
            wal_dir: wal_dir.to_owned(),
            current_lsn: start_lsn,
            segment_buf: None,
            segment_no,
        }
    }

    /// Read next WAL record. Returns (lsn, record) or None if no more records.
    pub fn next_record(&mut self) -> Option<(u64, WalRecord)> {
        loop {
            // Load segment if needed
            if self.segment_buf.is_none() {
                let path = writer::segment_path(&self.wal_dir, self.segment_no);
                match fs::read(&path) {
                    Ok(data) if data.len() == SEGMENT_SIZE => {
                        self.segment_buf = Some(data);
                    }
                    _ => return None,
                }
            }

            let seg = self.segment_buf.as_ref().unwrap();
            let seg_offset = (self.current_lsn % SEGMENT_SIZE as u64) as usize;

            // At page boundary, skip page header
            let page_offset = seg_offset % WAL_PAGE_SIZE;
            if page_offset == 0 {
                // Verify page header magic
                if seg_offset + PAGE_HEADER_SIZE > SEGMENT_SIZE {
                    return None;
                }
                let magic = u16::from_le_bytes([seg[seg_offset], seg[seg_offset + 1]]);
                if magic != XLP_MAGIC {
                    return None;
                }
                self.current_lsn += PAGE_HEADER_SIZE as u64;
                continue;
            }

            // Check if we've gone past the segment
            if seg_offset >= SEGMENT_SIZE {
                self.segment_no += 1;
                self.segment_buf = None;
                continue;
            }

            // Collect record bytes, handling page boundaries
            let record_lsn = self.current_lsn;

            // Read tot_len first (may span page boundary)
            let header_bytes = self.collect_bytes(4)?;
            let tot_len = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap()) as usize;

            if !(record::RECORD_HEADER_SIZE..=SEGMENT_SIZE).contains(&tot_len) {
                return None;
            }

            // Rewind and read full record
            self.current_lsn = record_lsn;
            let record_bytes = self.collect_bytes(tot_len)?;

            match record::deserialize_record(&record_bytes) {
                Some((rec, _)) => return Some((record_lsn, rec)),
                None => return None,
            }
        }
    }

    /// Collect `needed` bytes from WAL, skipping page headers.
    fn collect_bytes(&mut self, needed: usize) -> Option<Vec<u8>> {
        let mut buf = Vec::with_capacity(needed);
        while buf.len() < needed {
            // Load segment if needed
            if self.segment_buf.is_none() {
                let path = writer::segment_path(&self.wal_dir, self.segment_no);
                match fs::read(&path) {
                    Ok(data) if data.len() == SEGMENT_SIZE => {
                        self.segment_buf = Some(data);
                    }
                    _ => return None,
                }
            }

            let seg = self.segment_buf.as_ref().unwrap();
            let seg_offset = (self.current_lsn % SEGMENT_SIZE as u64) as usize;

            // At page boundary, skip header
            let page_offset = seg_offset % WAL_PAGE_SIZE;
            if page_offset == 0 {
                if seg_offset + PAGE_HEADER_SIZE > SEGMENT_SIZE {
                    return None;
                }
                self.current_lsn += PAGE_HEADER_SIZE as u64;
                continue;
            }

            if seg_offset >= SEGMENT_SIZE {
                self.segment_no += 1;
                self.segment_buf = None;
                continue;
            }

            let space_in_page = WAL_PAGE_SIZE - page_offset;
            let remaining = needed - buf.len();
            let can_read = space_in_page.min(remaining);
            buf.extend_from_slice(&seg[seg_offset..seg_offset + can_read]);
            self.current_lsn += can_read as u64;
        }
        Some(buf)
    }
}

/// Replay WAL from `redo_lsn` forward, applying heap changes idempotently.
/// Returns the LSN after the last replayed record.
pub fn recover(disk: &DiskManager, wal_dir: &Path, redo_lsn: u64) -> io::Result<u64> {
    let mut reader = WalReader::new(wal_dir, redo_lsn);
    let mut last_lsn = redo_lsn;

    while let Some((lsn, rec)) = reader.next_record() {
        if rec.xl_rmid == record::RM_HEAP_ID {
            apply_heap_record(disk, lsn, &rec);
        }
        last_lsn = reader.current_lsn;
    }

    Ok(last_lsn)
}

fn apply_heap_record(disk: &DiskManager, lsn: u64, rec: &WalRecord) {
    let Some((rfn, blkno, item_off)) = record::parse_heap_data(&rec.data) else {
        return;
    };

    let mut page = [0u8; PAGE_SIZE];
    let num_pages = disk.num_pages(rfn);

    match rec.xl_info {
        record::XLOG_HEAP_INSERT => {
            let Some(tuple) = record::parse_heap_tuple(&rec.data) else {
                return;
            };
            if blkno < num_pages {
                disk.read_page(rfn, blkno, &mut page);
                if heap::get_page_lsn(&page) >= lsn {
                    return; // Already applied
                }
                if heap::insert_tuple(&mut page, tuple, blkno).is_ok() {
                    heap::set_page_lsn(&mut page, lsn);
                    disk.write_page(rfn, blkno, &page);
                }
            } else {
                // Need to create new page(s) up to blkno
                heap::init_page(&mut page);
                if heap::insert_tuple(&mut page, tuple, blkno).is_ok() {
                    heap::set_page_lsn(&mut page, lsn);
                    disk.write_page(rfn, blkno, &page);
                }
            }
        }
        record::XLOG_HEAP_DELETE | record::XLOG_HEAP_UPDATE => {
            if blkno >= num_pages {
                return;
            }
            disk.read_page(rfn, blkno, &mut page);
            if heap::get_page_lsn(&page) >= lsn {
                return; // Already applied
            }
            heap::mark_tuple_dead(&mut page, item_off);
            heap::set_page_lsn(&mut page, lsn);
            disk.write_page(rfn, blkno, &page);
        }
        _ => {}
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wal::record::{WalRecord, RM_HEAP_ID, XLOG_HEAP_INSERT};
    use crate::wal::writer::WalWriter;

    #[test]
    fn reader_reads_written_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("pg_wal");

        let mut w = WalWriter::new(&wal_dir, 0);
        let rec1 = WalRecord {
            xl_xid: 1,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: vec![0xAA; 20],
        };
        let rec2 = WalRecord {
            xl_xid: 2,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: vec![0xBB; 30],
        };
        let lsn1 = w.append(&rec1);
        let lsn2 = w.append(&rec2);
        w.flush();

        let mut r = WalReader::new(&wal_dir, lsn1);
        let (rlsn1, rrec1) = r.next_record().unwrap();
        assert_eq!(rlsn1, lsn1);
        assert_eq!(rrec1.xl_xid, 1);
        assert_eq!(rrec1.data, vec![0xAA; 20]);

        let (rlsn2, rrec2) = r.next_record().unwrap();
        assert_eq!(rlsn2, lsn2);
        assert_eq!(rrec2.xl_xid, 2);
        assert_eq!(rrec2.data, vec![0xBB; 30]);
    }

    #[test]
    fn crash_recovery_replays_wal() {
        use crate::catalog::Column;
        use crate::types::TypeId;

        let dir = tempfile::tempdir().unwrap();
        let disk = DiskManager::new(dir.path(), 5);
        let wal_dir = dir.path().join("pg_wal");
        let mut w = WalWriter::new(&wal_dir, 0);

        // Create heap file and insert a tuple via WAL
        disk.create_heap_file(16384);
        let cols = vec![Column {
            name: "a".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        }];
        let tuple = heap::build_tuple(&[crate::types::Datum::Int4(42)], &cols);

        let mut page = [0u8; PAGE_SIZE];
        heap::init_page(&mut page);
        let item_idx = heap::insert_tuple(&mut page, &tuple, 0).unwrap();
        let wal_data = record::build_heap_insert_data(16384, 0, item_idx, &tuple);
        let rec = WalRecord {
            xl_xid: 1,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: wal_data,
        };
        let lsn = w.append(&rec);
        w.flush();

        // Simulate crash: page was NOT written to disk
        // Now recover
        let _end_lsn = recover(&disk, &wal_dir, lsn).unwrap();

        // Verify the tuple was recovered
        let mut read_page = [0u8; PAGE_SIZE];
        disk.read_page(16384, 0, &mut read_page);
        let datums = heap::read_tuple(&read_page, 0, &cols).unwrap();
        assert_eq!(datums, vec![crate::types::Datum::Int4(42)]);
    }

    #[test]
    fn idempotent_replay() {
        use crate::catalog::Column;
        use crate::types::TypeId;

        let dir = tempfile::tempdir().unwrap();
        let disk = DiskManager::new(dir.path(), 5);
        let wal_dir = dir.path().join("pg_wal");
        let mut w = WalWriter::new(&wal_dir, 0);

        disk.create_heap_file(16384);
        let cols = vec![Column {
            name: "a".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        }];
        let tuple = heap::build_tuple(&[crate::types::Datum::Int4(99)], &cols);
        let wal_data = record::build_heap_insert_data(16384, 0, 0, &tuple);
        let rec = WalRecord {
            xl_xid: 1,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: wal_data,
        };
        let lsn = w.append(&rec);
        w.flush();

        // Replay twice
        recover(&disk, &wal_dir, lsn).unwrap();
        recover(&disk, &wal_dir, lsn).unwrap();

        // Should still have exactly 1 tuple
        let mut page = [0u8; PAGE_SIZE];
        disk.read_page(16384, 0, &mut page);
        assert_eq!(heap::num_items(&page), 1);
        let datums = heap::read_tuple(&page, 0, &cols).unwrap();
        assert_eq!(datums, vec![crate::types::Datum::Int4(99)]);
    }
}
