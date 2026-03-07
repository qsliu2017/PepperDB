//! Free Space Map: tracks available space per heap page.
//! Each FSM byte represents one heap page's free space in 32-byte units
//! (0 = full, 255 = ~8KB free). One FSM page (8KB) tracks 8192 heap pages.

use crate::storage::disk::{DiskManager, PAGE_SIZE};
use crate::types::OID;

/// Granularity: each FSM byte represents free space in 32-byte steps.
const FSM_UNIT: usize = 32;

/// Convert free bytes to FSM category (0-255).
fn bytes_to_cat(bytes: usize) -> u8 {
    let cat = bytes / FSM_UNIT;
    if cat > 255 {
        255
    } else {
        cat as u8
    }
}

/// Convert FSM category back to minimum free bytes.
fn cat_to_bytes(cat: u8) -> usize {
    cat as usize * FSM_UNIT
}

/// Search the FSM for a page with at least `needed` bytes free.
pub fn search(disk: &DiskManager, relfilenode: OID, needed: usize) -> Option<u32> {
    let needed_cat = bytes_to_cat(needed);
    let path = disk.fork_file_path(relfilenode, "_fsm");
    let file_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) as usize;
    if file_len == 0 {
        return None;
    }

    let data = std::fs::read(&path).ok()?;
    for (page_id, &cat) in data.iter().enumerate() {
        if cat >= needed_cat {
            return Some(page_id as u32);
        }
    }
    None
}

/// Update the FSM entry for a heap page.
pub fn update(disk: &DiskManager, relfilenode: OID, page_id: u32, free_bytes: usize) {
    let path = disk.fork_file_path(relfilenode, "_fsm");
    let cat = bytes_to_cat(free_bytes);
    let offset = page_id as usize;

    let mut data = std::fs::read(&path).unwrap_or_default();
    if data.len() <= offset {
        data.resize(offset + 1, 0);
    }
    data[offset] = cat;
    std::fs::write(&path, &data).expect("failed to write FSM");
}

/// Get free space for a page (returns bytes).
pub fn get_free_space(disk: &DiskManager, relfilenode: OID, page_id: u32) -> usize {
    let path = disk.fork_file_path(relfilenode, "_fsm");
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

/// Compute free space on a heap page from pd_upper - pd_lower - ITEM_ID_SIZE.
pub fn page_free_space(page: &[u8; PAGE_SIZE]) -> usize {
    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    let pd_upper = u16::from_le_bytes([page[14], page[15]]) as usize;
    if pd_upper > pd_lower + 4 {
        pd_upper - pd_lower - 4 // subtract 4 for the next ItemId slot
    } else {
        0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::heap;

    #[test]
    fn fsm_search_and_update() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        dm.create_heap_file(100);

        // Create FSM file
        let fsm_path = dm.fork_file_path(100, "_fsm");
        std::fs::write(&fsm_path, &[]).unwrap();

        // No space initially
        assert_eq!(search(&dm, 100, 100), None);

        // Record that page 0 has 4000 bytes free
        update(&dm, 100, 0, 4000);
        assert!(search(&dm, 100, 100).is_some());
        assert_eq!(search(&dm, 100, 100), Some(0));

        // Need more than available
        assert_eq!(search(&dm, 100, 5000), None);
    }

    #[test]
    fn page_free_space_calculation() {
        let mut page = [0u8; PAGE_SIZE];
        heap::init_page(&mut page);
        // Fresh page: upper=8192, lower=28, free = 8192-28-4 = 8160
        assert_eq!(page_free_space(&page), 8160);
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
