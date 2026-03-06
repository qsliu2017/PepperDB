//! DiskManager -- raw 8KB page I/O per table file.
//! Each table is stored as one file named {base_path}/{oid}.

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::types::OID;

pub const PAGE_SIZE: usize = 8192;

pub struct DiskManager {
    base_path: PathBuf,
}

impl DiskManager {
    pub fn new(base_path: &std::path::Path) -> Self {
        fs::create_dir_all(base_path).expect("failed to create data directory");
        Self {
            base_path: base_path.to_owned(),
        }
    }

    fn file_path(&self, oid: OID) -> PathBuf {
        self.base_path.join(oid.to_string())
    }

    pub fn create_heap_file(&self, oid: OID) {
        fs::File::create(self.file_path(oid)).expect("failed to create heap file");
    }

    pub fn read_page(&self, oid: OID, page_id: u32, buf: &mut [u8; PAGE_SIZE]) {
        let mut f = fs::File::open(self.file_path(oid)).expect("failed to open heap file");
        f.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
            .expect("seek failed");
        f.read_exact(buf).expect("read failed");
    }

    pub fn write_page(&self, oid: OID, page_id: u32, buf: &[u8; PAGE_SIZE]) {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.file_path(oid))
            .expect("failed to open heap file for write");
        f.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
            .expect("seek failed");
        f.write_all(buf).expect("write failed");
    }

    pub fn delete_heap_file(&self, oid: OID) {
        let _ = fs::remove_file(self.file_path(oid));
    }

    pub fn num_pages(&self, oid: OID) -> u32 {
        let path = self.file_path(oid);
        match fs::metadata(&path) {
            Ok(meta) => (meta.len() / PAGE_SIZE as u64) as u32,
            Err(_) => 0,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn round_trip_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path());
        dm.create_heap_file(1);

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xAB;
        page[PAGE_SIZE - 1] = 0xCD;
        dm.write_page(1, 0, &page);

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(1, 0, &mut read_buf);
        assert_eq!(read_buf[0], 0xAB);
        assert_eq!(read_buf[PAGE_SIZE - 1], 0xCD);
    }

    #[test]
    fn num_pages_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path());
        dm.create_heap_file(2);
        assert_eq!(dm.num_pages(2), 0);

        let page = [0u8; PAGE_SIZE];
        dm.write_page(2, 0, &page);
        assert_eq!(dm.num_pages(2), 1);

        dm.write_page(2, 1, &page);
        assert_eq!(dm.num_pages(2), 2);
    }
}
