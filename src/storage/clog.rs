//! CLOG (Commit Log) -- 2 bits per XID tracking transaction status.
//! Stored in `pg_xact/` directory. Each 8KB page holds 32768 XIDs.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const CLOG_BITS_PER_XID: usize = 2;
const XIDS_PER_BYTE: usize = 4;
const CLOG_PAGE_SIZE: usize = 8192;
const XIDS_PER_PAGE: usize = CLOG_PAGE_SIZE * XIDS_PER_BYTE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum XidStatus {
    InProgress = 0,
    Committed = 1,
    Aborted = 2,
}

pub struct Clog {
    dir: PathBuf,
    /// In-memory cache of pages
    pages: HashMap<u32, Vec<u8>>,
}

impl Clog {
    pub fn new(dir: &Path) -> Self {
        fs::create_dir_all(dir).expect("failed to create pg_xact directory");
        Self {
            dir: dir.to_owned(),
            pages: HashMap::new(),
        }
    }

    pub fn set_status(&mut self, xid: u32, status: XidStatus) {
        let page_no = xid as usize / XIDS_PER_PAGE;
        let offset_in_page = xid as usize % XIDS_PER_PAGE;
        let byte_idx = offset_in_page / XIDS_PER_BYTE;
        let bit_shift = (offset_in_page % XIDS_PER_BYTE) * CLOG_BITS_PER_XID;

        let page = self.load_page(page_no as u32);
        let mask = !(0x03u8 << bit_shift);
        page[byte_idx] = (page[byte_idx] & mask) | ((status as u8) << bit_shift);
        self.write_page(page_no as u32);
    }

    pub fn get_status(&mut self, xid: u32) -> XidStatus {
        let page_no = xid as usize / XIDS_PER_PAGE;
        let offset_in_page = xid as usize % XIDS_PER_PAGE;
        let byte_idx = offset_in_page / XIDS_PER_BYTE;
        let bit_shift = (offset_in_page % XIDS_PER_BYTE) * CLOG_BITS_PER_XID;

        let page = self.load_page(page_no as u32);
        let bits = (page[byte_idx] >> bit_shift) & 0x03;
        match bits {
            1 => XidStatus::Committed,
            2 => XidStatus::Aborted,
            _ => XidStatus::InProgress,
        }
    }

    fn load_page(&mut self, page_no: u32) -> &mut Vec<u8> {
        if !self.pages.contains_key(&page_no) {
            let path = self.page_path(page_no);
            let data = if path.exists() {
                let mut f = fs::File::open(&path).expect("failed to open clog page");
                let mut buf = vec![0u8; CLOG_PAGE_SIZE];
                f.seek(SeekFrom::Start(0)).ok();
                let _ = f.read(&mut buf);
                buf
            } else {
                vec![0u8; CLOG_PAGE_SIZE]
            };
            self.pages.insert(page_no, data);
        }
        self.pages.get_mut(&page_no).unwrap()
    }

    fn write_page(&self, page_no: u32) {
        let path = self.page_path(page_no);
        if let Some(data) = self.pages.get(&page_no) {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .expect("failed to write clog page");
            f.write_all(data).expect("clog write failed");
        }
    }

    fn page_path(&self, page_no: u32) -> PathBuf {
        self.dir.join(format!("{:04X}", page_no))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clog_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let clog_dir = dir.path().join("pg_xact");
        let mut clog = Clog::new(&clog_dir);

        assert_eq!(clog.get_status(1), XidStatus::InProgress);
        clog.set_status(1, XidStatus::Committed);
        assert_eq!(clog.get_status(1), XidStatus::Committed);
        clog.set_status(2, XidStatus::Aborted);
        assert_eq!(clog.get_status(2), XidStatus::Aborted);
        assert_eq!(clog.get_status(3), XidStatus::InProgress);
    }

    #[test]
    fn clog_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let clog_dir = dir.path().join("pg_xact");
        {
            let mut clog = Clog::new(&clog_dir);
            clog.set_status(100, XidStatus::Committed);
            clog.set_status(101, XidStatus::Aborted);
        }
        // Re-open
        let mut clog = Clog::new(&clog_dir);
        assert_eq!(clog.get_status(100), XidStatus::Committed);
        assert_eq!(clog.get_status(101), XidStatus::Aborted);
    }
}
