//! WAL writer -- appends records to 16MB segment files in `pg_wal/`.
//!
//! Segment files are named `{TLI:08X}{SegHi:08X}{SegLo:08X}` (24 hex chars).
//! Each 8KB page starts with a page header (XLogPageHeaderData):
//!   xlp_magic (2B), xlp_info (2B), xlp_tli (4B), xlp_pageaddr (8B) = 16 bytes.
//!
//! Records can span pages; continuation pages get XLP_FIRST_IS_CONTRECORD flag.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::xlogrecord as record;

const SEGMENT_SIZE: usize = 16 * 1024 * 1024; // 16MB
const WAL_PAGE_SIZE: usize = 8192;
const PAGE_HEADER_SIZE: usize = 16;

const XLP_MAGIC: u16 = 0xD106;
const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;

pub struct WalWriter {
    wal_dir: PathBuf,
    current_lsn: u64,
    prev_lsn: u64,
    /// In-memory buffer for current segment
    segment_buf: Vec<u8>,
    /// Current segment number
    segment_no: u64,
    /// Position within current segment
    segment_pos: usize,
}

impl WalWriter {
    pub fn new(wal_dir: &Path, start_lsn: u64) -> Self {
        fs::create_dir_all(wal_dir).expect("failed to create pg_wal directory");
        let segment_no = start_lsn / SEGMENT_SIZE as u64;
        let segment_pos = (start_lsn % SEGMENT_SIZE as u64) as usize;

        let mut writer = Self {
            wal_dir: wal_dir.to_owned(),
            current_lsn: start_lsn,
            prev_lsn: 0,
            segment_buf: vec![0u8; SEGMENT_SIZE],
            segment_no,
            segment_pos,
        };

        // If starting at position 0 in a segment, write the first page header
        if segment_pos == 0 {
            writer.write_page_header(0, false);
            writer.segment_pos = PAGE_HEADER_SIZE;
            writer.current_lsn = segment_no * SEGMENT_SIZE as u64 + PAGE_HEADER_SIZE as u64;
        }

        writer
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Append a WAL record. Returns the LSN where the record starts.
    pub fn append(&mut self, rec: &record::WalRecord) -> u64 {
        let record_bytes = record::serialize_record(rec, self.prev_lsn);
        let record_lsn = self.current_lsn;

        let mut remaining = &record_bytes[..];
        let mut is_first = true;

        while !remaining.is_empty() {
            // Check if we need to cross into a new segment
            if self.segment_pos >= SEGMENT_SIZE {
                self.flush();
                self.segment_no += 1;
                self.segment_buf = vec![0u8; SEGMENT_SIZE];
                self.segment_pos = 0;
                self.write_page_header(0, !is_first);
                self.segment_pos = PAGE_HEADER_SIZE;
                self.current_lsn = self.segment_no * SEGMENT_SIZE as u64 + PAGE_HEADER_SIZE as u64;
                continue;
            }

            let page_offset = self.segment_pos % WAL_PAGE_SIZE;

            // At page boundary, write new page header
            if page_offset == 0 {
                self.write_page_header(self.segment_pos, !is_first);
                self.segment_pos += PAGE_HEADER_SIZE;
                self.current_lsn += PAGE_HEADER_SIZE as u64;
                continue;
            }

            let space_in_page = WAL_PAGE_SIZE - page_offset;
            let can_write = space_in_page.min(remaining.len());
            self.segment_buf[self.segment_pos..self.segment_pos + can_write]
                .copy_from_slice(&remaining[..can_write]);
            self.segment_pos += can_write;
            self.current_lsn += can_write as u64;
            remaining = &remaining[can_write..];
            is_first = false;
        }

        self.prev_lsn = record_lsn;
        record_lsn
    }

    /// Flush current segment to disk.
    pub fn flush(&mut self) {
        let path = segment_path(&self.wal_dir, self.segment_no);
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("failed to open WAL segment for write");
        f.write_all(&self.segment_buf)
            .expect("failed to write WAL segment");
        f.flush().expect("WAL flush failed");
    }

    fn write_page_header(&mut self, offset: usize, is_cont: bool) {
        let page_addr = self.segment_no * SEGMENT_SIZE as u64 + offset as u64;
        let info: u16 = if is_cont { XLP_FIRST_IS_CONTRECORD } else { 0 };
        self.segment_buf[offset..offset + 2].copy_from_slice(&XLP_MAGIC.to_le_bytes());
        self.segment_buf[offset + 2..offset + 4].copy_from_slice(&info.to_le_bytes());
        self.segment_buf[offset + 4..offset + 8].copy_from_slice(&1u32.to_le_bytes()); // tli=1
        self.segment_buf[offset + 8..offset + 16].copy_from_slice(&page_addr.to_le_bytes());
    }
}

/// Build PG-style segment filename: {TLI:08X}{SegHi:08X}{SegLo:08X}
pub fn segment_path(wal_dir: &Path, segment_no: u64) -> PathBuf {
    let seg_hi = (segment_no >> 32) as u32;
    let seg_lo = segment_no as u32;
    wal_dir.join(format!("{:08X}{:08X}{:08X}", 1u32, seg_hi, seg_lo))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::access::transam::xlogrecord::{WalRecord, RM_HEAP_ID, XLOG_HEAP_INSERT};

    #[test]
    fn segment_file_naming() {
        let dir = Path::new("/tmp/wal");
        assert_eq!(
            segment_path(dir, 0).file_name().unwrap().to_str().unwrap(),
            "000000010000000000000000"
        );
        assert_eq!(
            segment_path(dir, 1).file_name().unwrap().to_str().unwrap(),
            "000000010000000000000001"
        );
    }

    #[test]
    fn basic_append_and_flush() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("pg_wal");
        let mut writer = WalWriter::new(&wal_dir, 0);

        let rec = WalRecord {
            xl_xid: 1,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: vec![0xAB; 10],
        };
        let lsn = writer.append(&rec);
        assert_eq!(lsn, PAGE_HEADER_SIZE as u64);
        assert!(writer.current_lsn() > lsn);

        writer.flush();
        let seg_path = segment_path(&wal_dir, 0);
        assert!(seg_path.exists());
        let data = std::fs::read(&seg_path).unwrap();
        assert_eq!(data.len(), SEGMENT_SIZE);
    }

    #[test]
    fn wal_page_headers_at_correct_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("pg_wal");
        let mut writer = WalWriter::new(&wal_dir, 0);

        // Write enough data to fill first page and start second
        let rec = WalRecord {
            xl_xid: 1,
            xl_info: 0,
            xl_rmid: 0,
            data: vec![0u8; WAL_PAGE_SIZE], // large enough to span pages
        };
        writer.append(&rec);
        writer.flush();

        let data = std::fs::read(segment_path(&wal_dir, 0)).unwrap();
        // First page header at offset 0
        assert_eq!(u16::from_le_bytes([data[0], data[1]]), XLP_MAGIC);
        // Second page header at offset 8192
        assert_eq!(
            u16::from_le_bytes([data[WAL_PAGE_SIZE], data[WAL_PAGE_SIZE + 1]]),
            XLP_MAGIC
        );
        // Second page should be continuation
        let info = u16::from_le_bytes([data[WAL_PAGE_SIZE + 2], data[WAL_PAGE_SIZE + 3]]);
        assert_ne!(info & XLP_FIRST_IS_CONTRECORD, 0);
    }

    #[test]
    fn record_spanning_page_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("pg_wal");
        let mut writer = WalWriter::new(&wal_dir, 0);

        // Fill up most of first page, then write a record that must span
        let small = WalRecord {
            xl_xid: 1,
            xl_info: 0,
            xl_rmid: 0,
            data: vec![0u8; WAL_PAGE_SIZE - PAGE_HEADER_SIZE - record::RECORD_HEADER_SIZE - 100],
        };
        writer.append(&small);

        // This should cross the page boundary
        let spanning = WalRecord {
            xl_xid: 2,
            xl_info: XLOG_HEAP_INSERT,
            xl_rmid: RM_HEAP_ID,
            data: vec![0xBB; 200],
        };
        let lsn = writer.append(&spanning);
        assert!(lsn > 0);

        // Verify second page exists with continuation header
        writer.flush();
        let data = std::fs::read(segment_path(&wal_dir, 0)).unwrap();
        let info = u16::from_le_bytes([data[WAL_PAGE_SIZE + 2], data[WAL_PAGE_SIZE + 3]]);
        assert_ne!(info & XLP_FIRST_IS_CONTRECORD, 0);
    }
}
