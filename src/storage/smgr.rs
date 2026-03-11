//! Storage manager -- raw 8KB page I/O per table file.
//! Table files stored under `base/{db_oid}/{relfilenode}` (per-database)
//! and `global/{relfilenode}` (shared catalogs), matching PostgreSQL layout.
//! All pages get PG-compatible checksums (pd_checksum at bytes 8-9).

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::storage::bufpage::{Page, PAGE_SIZE};
use crate::types::OID;

pub struct DiskManager {
    base_path: PathBuf,
    db_oid: OID,
}

impl DiskManager {
    pub fn new(base_path: &std::path::Path, db_oid: OID) -> Self {
        let db_dir = base_path.join("base").join(db_oid.to_string());
        let global_dir = base_path.join("global");
        fs::create_dir_all(&db_dir).expect("failed to create base/{db_oid} directory");
        fs::create_dir_all(&global_dir).expect("failed to create global directory");
        Self {
            base_path: base_path.to_owned(),
            db_oid,
        }
    }

    pub fn base_path(&self) -> &std::path::Path {
        &self.base_path
    }

    pub fn db_oid(&self) -> OID {
        self.db_oid
    }

    fn file_path(&self, relfilenode: OID) -> PathBuf {
        self.base_path
            .join("base")
            .join(self.db_oid.to_string())
            .join(relfilenode.to_string())
    }

    fn global_file_path(&self, relfilenode: OID) -> PathBuf {
        self.base_path.join("global").join(relfilenode.to_string())
    }

    /// Path for a relation fork (e.g., "_fsm", "_vm") in the per-database directory.
    pub fn fork_file_path(&self, relfilenode: OID, suffix: &str) -> PathBuf {
        self.base_path
            .join("base")
            .join(self.db_oid.to_string())
            .join(format!("{}{}", relfilenode, suffix))
    }

    // -- Per-database (base/) methods -----------------------------------------

    pub fn create_heap_file(&self, relfilenode: OID) {
        fs::File::create(self.file_path(relfilenode)).expect("failed to create heap file");
        fs::File::create(self.fork_file_path(relfilenode, "_fsm"))
            .expect("failed to create FSM file");
        fs::File::create(self.fork_file_path(relfilenode, "_vm"))
            .expect("failed to create VM file");
    }

    pub fn read_page(&self, relfilenode: OID, page_id: u32, buf: &mut Page) {
        let mut f = fs::File::open(self.file_path(relfilenode)).expect("failed to open heap file");
        f.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
            .expect("seek failed");
        f.read_exact(&mut buf.0).expect("read failed");
        assert!(
            buf.verify_checksum(page_id),
            "page checksum mismatch: relfilenode={}, page_id={}",
            relfilenode,
            page_id
        );
    }

    pub fn write_page(&self, relfilenode: OID, page_id: u32, buf: &Page) {
        let mut page = buf.clone();
        page.set_checksum(page_id);
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.file_path(relfilenode))
            .expect("failed to open heap file for write");
        f.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
            .expect("seek failed");
        f.write_all(&page.0).expect("write failed");
    }

    pub fn delete_heap_file(&self, relfilenode: OID) {
        let _ = fs::remove_file(self.file_path(relfilenode));
        let _ = fs::remove_file(self.fork_file_path(relfilenode, "_fsm"));
        let _ = fs::remove_file(self.fork_file_path(relfilenode, "_vm"));
    }

    pub fn num_pages(&self, relfilenode: OID) -> u32 {
        let path = self.file_path(relfilenode);
        match fs::metadata(&path) {
            Ok(meta) => (meta.len() / PAGE_SIZE as u64) as u32,
            Err(_) => 0,
        }
    }

    // -- Global (global/) methods ---------------------------------------------

    pub fn create_global_file(&self, relfilenode: OID) {
        fs::File::create(self.global_file_path(relfilenode)).expect("failed to create global file");
    }

    pub fn read_global_page(&self, relfilenode: OID, page_id: u32, buf: &mut Page) {
        let mut f =
            fs::File::open(self.global_file_path(relfilenode)).expect("failed to open global file");
        f.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
            .expect("seek failed");
        f.read_exact(&mut buf.0).expect("read failed");
        assert!(
            buf.verify_checksum(page_id),
            "global page checksum mismatch: relfilenode={}, page_id={}",
            relfilenode,
            page_id
        );
    }

    pub fn write_global_page(&self, relfilenode: OID, page_id: u32, buf: &Page) {
        let mut page = buf.clone();
        page.set_checksum(page_id);
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.global_file_path(relfilenode))
            .expect("failed to open global file for write");
        f.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
            .expect("seek failed");
        f.write_all(&page.0).expect("write failed");
    }

    pub fn num_global_pages(&self, relfilenode: OID) -> u32 {
        let path = self.global_file_path(relfilenode);
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
    fn dir_structure() {
        let dir = tempfile::tempdir().unwrap();
        let _dm = DiskManager::new(dir.path(), 5);
        assert!(dir.path().join("base/5").is_dir());
        assert!(dir.path().join("global").is_dir());
    }

    #[test]
    fn file_path_layout() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        assert_eq!(dm.file_path(16384), dir.path().join("base/5/16384"));
        assert_eq!(dm.global_file_path(1259), dir.path().join("global/1259"));
    }

    #[test]
    fn round_trip_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        dm.create_heap_file(1);

        let mut page = Page::new();
        page.init();
        page[28] = 0xAB; // write after header
        dm.write_page(1, 0, &page);

        let mut read_buf = Page::new();
        dm.read_page(1, 0, &mut read_buf);
        assert_eq!(read_buf[28], 0xAB);
    }

    #[test]
    fn num_pages_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        dm.create_heap_file(2);
        assert_eq!(dm.num_pages(2), 0);

        let mut page = Page::new();
        page.init();
        dm.write_page(2, 0, &page);
        assert_eq!(dm.num_pages(2), 1);

        dm.write_page(2, 1, &page);
        assert_eq!(dm.num_pages(2), 2);
    }

    #[test]
    fn global_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        dm.create_global_file(1259);

        let mut page = Page::new();
        page.init();
        dm.write_global_page(1259, 0, &page);

        let mut read_buf = Page::new();
        dm.read_global_page(1259, 0, &mut read_buf);
        assert_eq!(dm.num_global_pages(1259), 1);
    }
}
