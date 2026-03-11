//! Visibility Map: 1 bit per heap page. Bit set = all tuples on page are frozen.
//! One VM byte covers 8 heap pages. The VM file is a flat byte array.

use crate::storage::smgr::DiskManager;
use crate::types::OID;

pub trait VisibilityMap {
    fn vm_is_frozen(&self, relfilenode: OID, page_id: u32) -> bool;
    fn vm_set_frozen(&self, relfilenode: OID, page_id: u32);
    fn vm_clear_frozen(&self, relfilenode: OID, page_id: u32);
}

impl VisibilityMap for DiskManager {
    fn vm_is_frozen(&self, relfilenode: OID, page_id: u32) -> bool {
        let path = self.fork_file_path(relfilenode, "_vm");
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(_) => return false,
        };
        let byte_idx = page_id as usize / 8;
        let bit_idx = page_id as usize % 8;
        if byte_idx < data.len() {
            (data[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }

    fn vm_set_frozen(&self, relfilenode: OID, page_id: u32) {
        let path = self.fork_file_path(relfilenode, "_vm");
        let mut data = std::fs::read(&path).unwrap_or_default();
        let byte_idx = page_id as usize / 8;
        let bit_idx = page_id as usize % 8;
        if data.len() <= byte_idx {
            data.resize(byte_idx + 1, 0);
        }
        data[byte_idx] |= 1 << bit_idx;
        std::fs::write(&path, &data).expect("failed to write VM");
    }

    fn vm_clear_frozen(&self, relfilenode: OID, page_id: u32) {
        let path = self.fork_file_path(relfilenode, "_vm");
        let mut data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(_) => return,
        };
        let byte_idx = page_id as usize / 8;
        let bit_idx = page_id as usize % 8;
        if byte_idx < data.len() {
            data[byte_idx] &= !(1 << bit_idx);
            std::fs::write(&path, &data).expect("failed to write VM");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn vm_set_and_check() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        dm.create_heap_file(100);

        // Create VM file
        let vm_path = dm.fork_file_path(100, "_vm");
        std::fs::write(&vm_path, &[]).unwrap();

        assert!(!dm.vm_is_frozen(100, 0));
        assert!(!dm.vm_is_frozen(100, 7));

        dm.vm_set_frozen(100, 0);
        assert!(dm.vm_is_frozen(100, 0));
        assert!(!dm.vm_is_frozen(100, 1));

        dm.vm_set_frozen(100, 7);
        assert!(dm.vm_is_frozen(100, 7));

        dm.vm_clear_frozen(100, 0);
        assert!(!dm.vm_is_frozen(100, 0));
        assert!(dm.vm_is_frozen(100, 7));
    }
}
