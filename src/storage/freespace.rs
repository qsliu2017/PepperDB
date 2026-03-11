//! Free Space Map: tracks available space per heap page.
//! Each FSM byte represents one heap page's free space in 32-byte units
//! (0 = full, 255 = ~8KB free). One FSM page (8KB) tracks 8192 heap pages.

use crate::storage::smgr::DiskManager;
use crate::types::OID;

/// Granularity: each FSM byte represents free space in 32-byte steps.
const FSM_UNIT: usize = 32;

/// Convert free bytes to FSM category (0-255).
const fn bytes_to_cat(bytes: usize) -> u8 {
    let cat = bytes / FSM_UNIT;
    if cat > 255 {
        255
    } else {
        cat as u8
    }
}

/// Convert FSM category back to minimum free bytes.
const fn cat_to_bytes(cat: u8) -> usize {
    cat as usize * FSM_UNIT
}

pub trait FreeSpaceMap {
    fn fsm_search(&self, relfilenode: OID, needed: usize) -> Option<u32>;
    fn fsm_update(&self, relfilenode: OID, page_id: u32, free_bytes: usize);
    fn fsm_get_free_space(&self, relfilenode: OID, page_id: u32) -> usize;
}

impl FreeSpaceMap for DiskManager {
    fn fsm_search(&self, relfilenode: OID, needed: usize) -> Option<u32> {
        let needed_cat = bytes_to_cat(needed);
        let path = self.fork_file_path(relfilenode, "_fsm");
        let file_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) as usize;
        if file_len == 0 {
            return None;
        }

        let data = std::fs::read(&path).ok()?;
        data.iter()
            .position(|&cat| cat >= needed_cat)
            .map(|page_id| page_id as u32)
    }

    fn fsm_update(&self, relfilenode: OID, page_id: u32, free_bytes: usize) {
        let path = self.fork_file_path(relfilenode, "_fsm");
        let cat = bytes_to_cat(free_bytes);
        let offset = page_id as usize;

        let mut data = std::fs::read(&path).unwrap_or_default();
        if data.len() <= offset {
            data.resize(offset + 1, 0);
        }
        data[offset] = cat;
        std::fs::write(&path, &data).expect("failed to write FSM");
    }

    fn fsm_get_free_space(&self, relfilenode: OID, page_id: u32) -> usize {
        let path = self.fork_file_path(relfilenode, "_fsm");
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(_) => return 0,
        };
        let offset = page_id as usize;
        if offset < data.len() {
            cat_to_bytes(data[offset])
        } else {
            0
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::bufpage::Page;

    #[test]
    fn fsm_search_and_update() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        dm.create_heap_file(100);

        // Create FSM file
        let fsm_path = dm.fork_file_path(100, "_fsm");
        std::fs::write(&fsm_path, &[]).unwrap();

        // No space initially
        assert_eq!(dm.fsm_search(100, 100), None);

        // Record that page 0 has 4000 bytes free
        dm.fsm_update(100, 0, 4000);
        assert!(dm.fsm_search(100, 100).is_some());
        assert_eq!(dm.fsm_search(100, 100), Some(0));

        // Need more than available
        assert_eq!(dm.fsm_search(100, 5000), None);
    }

    #[test]
    fn page_free_space_calculation() {
        let mut page = Page::new();
        page.init();
        // Fresh page: upper=8192, lower=28, free = 8192-28-4 = 8160
        assert_eq!(page.free_space(), 8160);
    }

    #[test]
    fn bytes_to_cat_round_trip() {
        assert_eq!(bytes_to_cat(0), 0);
        assert_eq!(bytes_to_cat(31), 0);
        assert_eq!(bytes_to_cat(32), 1);
        assert_eq!(bytes_to_cat(8160), 255);
        assert_eq!(cat_to_bytes(1), 32);
        assert_eq!(cat_to_bytes(255), 8160);
    }
}
