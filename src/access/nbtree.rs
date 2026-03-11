//! B-tree index pages: same 28B PageHeader, BTPageOpaqueData (16B) at page end,
//! index tuples with 8B IndexTupleData header (t_tid 6B + t_info 2B) + key data.
//! Metapage (page 0) stores root pointer. Leaf pages form a linked list.

use crate::storage::bufpage::{
    pack_item_id, read_u16, read_u32, unpack_item_id, write_u16, write_u32, Page, HEADER_SIZE,
    ITEM_ID_SIZE, LP_NORMAL, PAGE_SIZE, PD_LOWER, PD_PAGESIZE_VERSION, PD_SPECIAL, PD_UPPER,
    PG_PAGE_SIZE_VERSION,
};
use crate::storage::smgr::DiskManager;
use crate::types::{Datum, TypeId, OID};
use std::cmp::Ordering;

// -- BTPageOpaqueData (16 bytes at end of page) -------------------------------

const BT_OPAQUE_SIZE: usize = 16;
const BT_SPECIAL: usize = PAGE_SIZE - BT_OPAQUE_SIZE; // 8176

// Offsets within opaque data (relative to pd_special)
const BTPO_PREV: usize = 0;
const BTPO_NEXT: usize = 4;
const BTPO_LEVEL: usize = 8;
const BTPO_FLAGS: usize = 12;

const BTP_LEAF: u16 = 1;
const BTP_ROOT: u16 = 2;
const BTP_META: u16 = 8;

const BT_NO_PAGE: u32 = u32::MAX;

// -- Meta page (page 0) data after header -------------------------------------

const BTM_MAGIC_OFF: usize = HEADER_SIZE;
const BTM_VERSION_OFF: usize = HEADER_SIZE + 4;
const BTM_ROOT_OFF: usize = HEADER_SIZE + 8;
const BTM_LEVEL_OFF: usize = HEADER_SIZE + 12;
const BT_META_MAGIC: u32 = 0x053162;
const BT_META_VERSION: u32 = 4;

// -- Index tuple header (8 bytes) ---------------------------------------------

const INDEX_TUPLE_HDR: usize = 8;

// -- BTreePage trait ----------------------------------------------------------

/// Heap tuple pointer stored in an index tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemPointer {
    pub block_id: u32,
    pub offset_num: u16,
}

trait BTreePage {
    fn init_meta(&mut self);
    fn init_btree(&mut self, level: u32, flags: u16);
    fn read_meta(&self) -> (u32, u32);
    fn write_meta(&mut self, root: u32, level: u32);
    fn read_opaque_flags(&self) -> u16;
    fn read_opaque_level(&self) -> u32;
    fn read_opaque_next(&self) -> u32;
    fn read_index_tuple(&self, item_index: u16, key_type: TypeId) -> Option<(ItemPointer, Datum)>;
    fn insert_index_tuple(&mut self, tuple: &[u8]) -> Result<u16, ()>;
    fn insert_sorted(&mut self, tuple: &[u8], key: &Datum, key_type: TypeId) -> Result<u16, ()>;
    fn rebuild_btree_page(&mut self, tuples: &[Vec<u8>]) -> Result<(), ()>;
    fn find_child(&self, key: &Datum, key_type: TypeId) -> u32;
}

impl BTreePage for Page {
    fn init_meta(&mut self) {
        self.0.fill(0);
        write_u16(&mut self.0, PD_LOWER, HEADER_SIZE as u16 + 16);
        write_u16(&mut self.0, PD_UPPER, BT_SPECIAL as u16);
        write_u16(&mut self.0, PD_SPECIAL, BT_SPECIAL as u16);
        write_u16(&mut self.0, PD_PAGESIZE_VERSION, PG_PAGE_SIZE_VERSION);
        write_u32(&mut self.0, BTM_MAGIC_OFF, BT_META_MAGIC);
        write_u32(&mut self.0, BTM_VERSION_OFF, BT_META_VERSION);
        write_u32(&mut self.0, BTM_ROOT_OFF, BT_NO_PAGE);
        write_u32(&mut self.0, BTM_LEVEL_OFF, 0);
        write_u16(&mut self.0, BT_SPECIAL + BTPO_FLAGS, BTP_META);
    }

    fn init_btree(&mut self, level: u32, flags: u16) {
        self.0.fill(0);
        write_u16(&mut self.0, PD_LOWER, HEADER_SIZE as u16);
        write_u16(&mut self.0, PD_UPPER, BT_SPECIAL as u16);
        write_u16(&mut self.0, PD_SPECIAL, BT_SPECIAL as u16);
        write_u16(&mut self.0, PD_PAGESIZE_VERSION, PG_PAGE_SIZE_VERSION);
        write_u32(&mut self.0, BT_SPECIAL + BTPO_PREV, BT_NO_PAGE);
        write_u32(&mut self.0, BT_SPECIAL + BTPO_NEXT, BT_NO_PAGE);
        write_u32(&mut self.0, BT_SPECIAL + BTPO_LEVEL, level);
        write_u16(&mut self.0, BT_SPECIAL + BTPO_FLAGS, flags);
    }

    fn read_meta(&self) -> (u32, u32) {
        (
            read_u32(&self.0, BTM_ROOT_OFF),
            read_u32(&self.0, BTM_LEVEL_OFF),
        )
    }

    fn write_meta(&mut self, root: u32, level: u32) {
        write_u32(&mut self.0, BTM_ROOT_OFF, root);
        write_u32(&mut self.0, BTM_LEVEL_OFF, level);
    }

    fn read_opaque_flags(&self) -> u16 {
        read_u16(&self.0, BT_SPECIAL + BTPO_FLAGS)
    }

    fn read_opaque_level(&self) -> u32 {
        read_u32(&self.0, BT_SPECIAL + BTPO_LEVEL)
    }

    fn read_opaque_next(&self) -> u32 {
        read_u32(&self.0, BT_SPECIAL + BTPO_NEXT)
    }

    fn read_index_tuple(&self, item_index: u16, key_type: TypeId) -> Option<(ItemPointer, Datum)> {
        let item_id_off = HEADER_SIZE + (item_index as usize) * ITEM_ID_SIZE;
        let pd_lower = read_u16(&self.0, PD_LOWER) as usize;
        if item_id_off + ITEM_ID_SIZE > pd_lower {
            return None;
        }
        let item_id = read_u32(&self.0, item_id_off);
        let (offset, flags, length) = unpack_item_id(item_id);
        if flags != LP_NORMAL || (offset == 0 && length == 0) {
            return None;
        }
        let off = offset as usize;
        let tid = ItemPointer {
            block_id: read_u32(&self.0, off),
            offset_num: read_u16(&self.0, off + 4),
        };
        let key_data = &self.0[off + INDEX_TUPLE_HDR..off + length as usize];
        let key = decode_key(key_data, key_type);
        Some((tid, key))
    }

    fn insert_index_tuple(&mut self, tuple: &[u8]) -> Result<u16, ()> {
        let pd_lower = read_u16(&self.0, PD_LOWER) as usize;
        let pd_upper = read_u16(&self.0, PD_UPPER) as usize;

        let needed_lower = pd_lower + ITEM_ID_SIZE;
        let needed_upper = pd_upper - tuple.len();
        if needed_lower > needed_upper {
            return Err(());
        }

        let tuple_offset = pd_upper - tuple.len();
        self.0[tuple_offset..tuple_offset + tuple.len()].copy_from_slice(tuple);

        let item_index = (pd_lower - HEADER_SIZE) / ITEM_ID_SIZE;
        let item_id = pack_item_id(tuple_offset as u16, LP_NORMAL, tuple.len() as u16);
        write_u32(&mut self.0, pd_lower, item_id);

        write_u16(&mut self.0, PD_LOWER, needed_lower as u16);
        write_u16(&mut self.0, PD_UPPER, tuple_offset as u16);

        Ok(item_index as u16)
    }

    fn insert_sorted(&mut self, tuple: &[u8], key: &Datum, key_type: TypeId) -> Result<u16, ()> {
        let n = self.num_items();

        // Find insertion position
        let mut pos = n;
        for i in 0..n {
            if let Some((_, existing_key)) = self.read_index_tuple(i, key_type) {
                if datum_cmp(key, &existing_key) != Ordering::Greater {
                    pos = i;
                    break;
                }
            }
        }

        // If inserting at end, just append
        if pos == n {
            return self.insert_index_tuple(tuple);
        }

        // Otherwise: rebuild page with new tuple inserted at pos
        let mut tuples: Vec<Vec<u8>> = Vec::with_capacity(n as usize + 1);
        for i in 0..n {
            let item_id_off = HEADER_SIZE + (i as usize) * ITEM_ID_SIZE;
            let item_id = read_u32(&self.0, item_id_off);
            let (offset, _, length) = unpack_item_id(item_id);
            let off = offset as usize;
            let len = length as usize;
            tuples.push(self.0[off..off + len].to_vec());
        }
        tuples.insert(pos as usize, tuple.to_vec());

        self.rebuild_btree_page(&tuples)?;
        Ok(pos)
    }

    fn rebuild_btree_page(&mut self, tuples: &[Vec<u8>]) -> Result<(), ()> {
        // Save opaque data
        let mut opaque = [0u8; BT_OPAQUE_SIZE];
        opaque.copy_from_slice(&self.0[BT_SPECIAL..BT_SPECIAL + BT_OPAQUE_SIZE]);

        // Clear data area
        let version = read_u16(&self.0, PD_PAGESIZE_VERSION);
        self.0[HEADER_SIZE..BT_SPECIAL].fill(0);

        let new_lower = HEADER_SIZE + tuples.len() * ITEM_ID_SIZE;
        let mut upper = BT_SPECIAL;

        for (slot, tup) in tuples.iter().enumerate() {
            upper -= tup.len();
            if new_lower > upper {
                return Err(()); // doesn't fit
            }
            self.0[upper..upper + tup.len()].copy_from_slice(tup);
            let item_id = pack_item_id(upper as u16, LP_NORMAL, tup.len() as u16);
            write_u32(&mut self.0, HEADER_SIZE + slot * ITEM_ID_SIZE, item_id);
        }

        write_u16(&mut self.0, PD_LOWER, new_lower as u16);
        write_u16(&mut self.0, PD_UPPER, upper as u16);
        write_u16(&mut self.0, PD_SPECIAL, BT_SPECIAL as u16);
        write_u16(&mut self.0, PD_PAGESIZE_VERSION, version);
        self.0[BT_SPECIAL..BT_SPECIAL + BT_OPAQUE_SIZE].copy_from_slice(&opaque);
        Ok(())
    }

    fn find_child(&self, key: &Datum, key_type: TypeId) -> u32 {
        let n = self.num_items();
        if n == 0 {
            return BT_NO_PAGE;
        }
        let (first, _) = self.read_index_tuple(0, key_type).unwrap();
        let mut child = first.block_id;
        for i in 1..n {
            if let Some((ptr, sep)) = self.read_index_tuple(i, key_type) {
                if datum_cmp(key, &sep) != Ordering::Less {
                    child = ptr.block_id;
                } else {
                    break;
                }
            }
        }
        child
    }
}

// -- Index tuple encoding -----------------------------------------------------

/// Build an index tuple: [block_id 4B][offset_num 2B][t_info 2B][key_bytes...]
fn build_index_tuple(tid: ItemPointer, key: &Datum, key_type: TypeId) -> Vec<u8> {
    let key_bytes = encode_key(key, key_type);
    let total_len = INDEX_TUPLE_HDR + key_bytes.len();
    let mut tup = vec![0u8; INDEX_TUPLE_HDR];
    // t_tid: block_id (4B) + offset_num (2B)
    tup[0..4].copy_from_slice(&tid.block_id.to_le_bytes());
    tup[4..6].copy_from_slice(&tid.offset_num.to_le_bytes());
    // t_info: size in low 13 bits
    tup[6..8].copy_from_slice(&(total_len as u16).to_le_bytes());
    tup.extend_from_slice(&key_bytes);
    tup
}

// -- Key encoding/decoding ----------------------------------------------------

fn encode_key(key: &Datum, _key_type: TypeId) -> Vec<u8> {
    match key {
        Datum::Bool(b) => vec![if *b { 1 } else { 0 }],
        Datum::Int2(v) => v.to_le_bytes().to_vec(),
        Datum::Int4(v) => v.to_le_bytes().to_vec(),
        Datum::Int8(v) => v.to_le_bytes().to_vec(),
        Datum::Float4(v) => v.to_le_bytes().to_vec(),
        Datum::Float8(v) => v.to_le_bytes().to_vec(),
        Datum::Text(s) => {
            // Length-prefixed: 4B LE length + bytes (no varlena header in index)
            let mut v = (s.len() as u32).to_le_bytes().to_vec();
            v.extend_from_slice(s.as_bytes());
            v
        }
        Datum::Null => vec![],
    }
}

fn decode_key(data: &[u8], key_type: TypeId) -> Datum {
    match key_type {
        TypeId::Bool => Datum::Bool(data[0] != 0),
        TypeId::Int2 => Datum::Int2(i16::from_le_bytes([data[0], data[1]])),
        TypeId::Int4 => Datum::Int4(i32::from_le_bytes(data[0..4].try_into().unwrap())),
        TypeId::Int8 => Datum::Int8(i64::from_le_bytes(data[0..8].try_into().unwrap())),
        TypeId::Float4 => Datum::Float4(f32::from_le_bytes(data[0..4].try_into().unwrap())),
        TypeId::Float8 => Datum::Float8(f64::from_le_bytes(data[0..8].try_into().unwrap())),
        TypeId::Text => {
            let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            Datum::Text(String::from_utf8_lossy(&data[4..4 + len]).into_owned())
        }
    }
}

/// Compare two Datum values for B-tree ordering.
pub fn datum_cmp(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (Datum::Bool(a), Datum::Bool(b)) => a.cmp(b),
        (Datum::Int2(a), Datum::Int2(b)) => a.cmp(b),
        (Datum::Int4(a), Datum::Int4(b)) => a.cmp(b),
        (Datum::Int8(a), Datum::Int8(b)) => a.cmp(b),
        (Datum::Float4(a), Datum::Float4(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Datum::Float8(a), Datum::Float8(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Datum::Text(a), Datum::Text(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// Minimum datum for a type (used for leftmost internal page entries).
fn min_datum(key_type: TypeId) -> Datum {
    match key_type {
        TypeId::Bool => Datum::Bool(false),
        TypeId::Int2 => Datum::Int2(i16::MIN),
        TypeId::Int4 => Datum::Int4(i32::MIN),
        TypeId::Int8 => Datum::Int8(i64::MIN),
        TypeId::Float4 => Datum::Float4(f32::NEG_INFINITY),
        TypeId::Float8 => Datum::Float8(f64::NEG_INFINITY),
        TypeId::Text => Datum::Text(String::new()),
    }
}

// -- High-level B-tree operations ---------------------------------------------

/// Create a new B-tree index file with a meta page.
pub fn create_index(disk: &DiskManager, relfilenode: OID) {
    disk.create_heap_file(relfilenode);
    let mut meta = Page::new();
    meta.init_meta();
    disk.write_page(relfilenode, 0, &meta);
}

/// Insert a key + heap TID into the B-tree index.
pub fn bt_insert(
    disk: &DiskManager,
    index_rfn: OID,
    key: &Datum,
    heap_tid: ItemPointer,
    key_type: TypeId,
) {
    let tuple = build_index_tuple(heap_tid, key, key_type);

    // Read meta page
    let mut meta = Page::new();
    disk.read_page(index_rfn, 0, &mut meta);
    let (root_blk, tree_level) = meta.read_meta();

    if root_blk == BT_NO_PAGE {
        // Empty tree: create root leaf
        let mut page = Page::new();
        page.init_btree(0, BTP_LEAF | BTP_ROOT);
        page.insert_sorted(&tuple, key, key_type)
            .expect("first insert must fit");
        let root_id = 1u32;
        disk.write_page(index_rfn, root_id, &page);
        meta.write_meta(root_id, 0);
        disk.write_page(index_rfn, 0, &meta);
        return;
    }

    // Find leaf page, tracking path for splits
    let mut path: Vec<u32> = Vec::new();
    let mut current = root_blk;
    let mut page = Page::new();

    loop {
        disk.read_page(index_rfn, current, &mut page);
        let flags = page.read_opaque_flags();
        if (flags & BTP_LEAF) != 0 {
            break;
        }
        // Internal page: find child to descend
        path.push(current);
        let n = page.num_items();
        if n == 0 {
            return; // corrupt
        }
        let (first_ptr, _) = page.read_index_tuple(0, key_type).unwrap();
        let mut child = first_ptr.block_id;
        for i in 1..n {
            if let Some((ptr, sep_key)) = page.read_index_tuple(i, key_type) {
                if datum_cmp(key, &sep_key) != Ordering::Less {
                    child = ptr.block_id;
                } else {
                    break;
                }
            }
        }
        current = child;
    }

    // Try insert into leaf
    if page.insert_sorted(&tuple, key, key_type).is_ok() {
        disk.write_page(index_rfn, current, &page);
        return;
    }

    // Leaf is full -- split
    split_and_insert(
        disk, index_rfn, current, &mut page, &tuple, key, key_type, &path, tree_level,
    );
}

/// Split a page and insert the new tuple, propagating splits up.
#[allow(clippy::too_many_arguments)]
fn split_and_insert(
    disk: &DiskManager,
    index_rfn: OID,
    page_id: u32,
    page: &mut Page,
    tuple: &[u8],
    key: &Datum,
    key_type: TypeId,
    path: &[u32],
    _tree_level: u32,
) {
    let level = page.read_opaque_level();
    let flags = page.read_opaque_flags();
    let old_next = page.read_opaque_next();

    // Collect all tuples + new one, sorted
    let n = page.num_items();
    let mut all_tuples: Vec<(Datum, Vec<u8>)> = Vec::with_capacity(n as usize + 1);
    for i in 0..n {
        let item_id_off = HEADER_SIZE + (i as usize) * ITEM_ID_SIZE;
        let item_id = read_u32(&page.0, item_id_off);
        let (offset, _, length) = unpack_item_id(item_id);
        let off = offset as usize;
        let len = length as usize;
        let tup_data = page.0[off..off + len].to_vec();
        let k = decode_key(&tup_data[INDEX_TUPLE_HDR..], key_type);
        all_tuples.push((k, tup_data));
    }
    // Insert new tuple in sorted position
    let mut insert_pos = all_tuples.len();
    for (i, (k, _)) in all_tuples.iter().enumerate() {
        if datum_cmp(key, k) != Ordering::Greater {
            insert_pos = i;
            break;
        }
    }
    all_tuples.insert(insert_pos, (key.clone(), tuple.to_vec()));

    let mid = all_tuples.len() / 2;
    let left_tuples: Vec<Vec<u8>> = all_tuples[..mid].iter().map(|(_, t)| t.clone()).collect();
    let right_tuples: Vec<Vec<u8>> = all_tuples[mid..].iter().map(|(_, t)| t.clone()).collect();
    let separator = all_tuples[mid].0.clone();

    // Allocate new right page
    let new_page_id = disk.num_pages(index_rfn);

    // Rebuild left page (keep same page_id)
    let left_flags = flags & !BTP_ROOT; // remove root flag if splitting root
    page.init_btree(level, left_flags);
    page.rebuild_btree_page(&left_tuples)
        .expect("left split must fit");
    // Set next to new page
    write_u32(&mut page.0, BT_SPECIAL + BTPO_NEXT, new_page_id);
    disk.write_page(index_rfn, page_id, page);

    // Create right page
    let mut right = Page::new();
    let right_flags = if (flags & BTP_LEAF) != 0 { BTP_LEAF } else { 0 };
    right.init_btree(level, right_flags);
    right
        .rebuild_btree_page(&right_tuples)
        .expect("right split must fit");
    write_u32(&mut right.0, BT_SPECIAL + BTPO_PREV, page_id);
    write_u32(&mut right.0, BT_SPECIAL + BTPO_NEXT, old_next);
    disk.write_page(index_rfn, new_page_id, &right);

    // Update old_next's prev pointer
    if old_next != BT_NO_PAGE {
        let mut next_page = Page::new();
        disk.read_page(index_rfn, old_next, &mut next_page);
        write_u32(&mut next_page.0, BT_SPECIAL + BTPO_PREV, new_page_id);
        disk.write_page(index_rfn, old_next, &next_page);
    }

    // Insert separator into parent
    let sep_tuple = build_index_tuple(
        ItemPointer {
            block_id: new_page_id,
            offset_num: 0,
        },
        &separator,
        key_type,
    );

    if (flags & BTP_ROOT) != 0 {
        // We split the root -- create new root
        let new_root_id = disk.num_pages(index_rfn);
        let mut new_root = Page::new();
        new_root.init_btree(level + 1, BTP_ROOT);

        // First entry: pointer to left child (with "minus infinity" -- we use Int4 MIN as dummy)
        let left_ptr = build_index_tuple(
            ItemPointer {
                block_id: page_id,
                offset_num: 0,
            },
            &min_datum(key_type),
            key_type,
        );
        new_root
            .insert_index_tuple(&left_ptr)
            .expect("root insert must fit");
        new_root
            .insert_index_tuple(&sep_tuple)
            .expect("root insert must fit");
        disk.write_page(index_rfn, new_root_id, &new_root);

        // Update meta
        let mut meta = Page::new();
        disk.read_page(index_rfn, 0, &mut meta);
        meta.write_meta(new_root_id, level + 1);
        disk.write_page(index_rfn, 0, &meta);
    } else if let Some(&parent_id) = path.last() {
        // Insert separator into parent internal page
        let mut parent = Page::new();
        disk.read_page(index_rfn, parent_id, &mut parent);
        if parent
            .insert_sorted(&sep_tuple, &separator, key_type)
            .is_ok()
        {
            disk.write_page(index_rfn, parent_id, &parent);
        } else {
            // Parent needs split too -- recursive
            let parent_path = &path[..path.len() - 1];
            split_and_insert(
                disk,
                index_rfn,
                parent_id,
                &mut parent,
                &sep_tuple,
                &separator,
                key_type,
                parent_path,
                _tree_level,
            );
        }
    }
}

/// Search for all heap TIDs matching an exact key.
pub fn bt_search(
    disk: &DiskManager,
    index_rfn: OID,
    key: &Datum,
    key_type: TypeId,
) -> Vec<ItemPointer> {
    let mut meta = Page::new();
    disk.read_page(index_rfn, 0, &mut meta);
    let (root_blk, _) = meta.read_meta();
    if root_blk == BT_NO_PAGE {
        return vec![];
    }

    // Descend to leaf
    let mut current = root_blk;
    let mut page = Page::new();
    loop {
        disk.read_page(index_rfn, current, &mut page);
        let flags = page.read_opaque_flags();
        if (flags & BTP_LEAF) != 0 {
            break;
        }
        current = page.find_child(key, key_type);
    }

    // Scan leaf (and follow next pages for duplicates)
    let mut results = Vec::new();
    loop {
        let n = page.num_items();
        for i in 0..n {
            if let Some((tid, k)) = page.read_index_tuple(i, key_type) {
                match datum_cmp(&k, key) {
                    Ordering::Equal => results.push(tid),
                    Ordering::Greater => return results,
                    Ordering::Less => {}
                }
            }
        }
        let next = page.read_opaque_next();
        if next == BT_NO_PAGE {
            break;
        }
        disk.read_page(index_rfn, next, &mut page);
    }
    results
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn meta_page_round_trip() {
        let mut page = Page::new();
        page.init_meta();
        let (root, level) = page.read_meta();
        assert_eq!(root, BT_NO_PAGE);
        assert_eq!(level, 0);

        page.write_meta(1, 2);
        let (root, level) = page.read_meta();
        assert_eq!(root, 1);
        assert_eq!(level, 2);
    }

    #[test]
    fn index_tuple_round_trip() {
        let tid = ItemPointer {
            block_id: 5,
            offset_num: 3,
        };
        let key = Datum::Int4(42);
        let tuple = build_index_tuple(tid, &key, TypeId::Int4);
        assert_eq!(tuple.len(), INDEX_TUPLE_HDR + 4);

        let mut page = Page::new();
        page.init_btree(0, BTP_LEAF | BTP_ROOT);
        page.insert_index_tuple(&tuple).unwrap();

        let (read_tid, read_key) = page.read_index_tuple(0, TypeId::Int4).unwrap();
        assert_eq!(read_tid, tid);
        assert_eq!(read_key, key);
    }

    #[test]
    fn text_key_round_trip() {
        let tid = ItemPointer {
            block_id: 0,
            offset_num: 1,
        };
        let key = Datum::Text("hello".into());
        let tuple = build_index_tuple(tid, &key, TypeId::Text);

        let mut page = Page::new();
        page.init_btree(0, BTP_LEAF | BTP_ROOT);
        page.insert_index_tuple(&tuple).unwrap();

        let (_, read_key) = page.read_index_tuple(0, TypeId::Text).unwrap();
        assert_eq!(read_key, Datum::Text("hello".into()));
    }

    #[test]
    fn single_insert_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        let rfn = 50000;
        create_index(&dm, rfn);

        let tid = ItemPointer {
            block_id: 0,
            offset_num: 1,
        };
        bt_insert(&dm, rfn, &Datum::Int4(42), tid, TypeId::Int4);

        let results = bt_search(&dm, rfn, &Datum::Int4(42), TypeId::Int4);
        assert_eq!(results, vec![tid]);

        let empty = bt_search(&dm, rfn, &Datum::Int4(99), TypeId::Int4);
        assert!(empty.is_empty());
    }

    #[test]
    fn multiple_inserts_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        let rfn = 50001;
        create_index(&dm, rfn);

        let keys = [30, 10, 50, 20, 40];
        for (i, &k) in keys.iter().enumerate() {
            let tid = ItemPointer {
                block_id: 0,
                offset_num: i as u16 + 1,
            };
            bt_insert(&dm, rfn, &Datum::Int4(k), tid, TypeId::Int4);
        }

        // Search each key
        for (i, &k) in keys.iter().enumerate() {
            let results = bt_search(&dm, rfn, &Datum::Int4(k), TypeId::Int4);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].offset_num, i as u16 + 1);
        }
    }

    #[test]
    fn duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        let rfn = 50002;
        create_index(&dm, rfn);

        for i in 0..5 {
            let tid = ItemPointer {
                block_id: 0,
                offset_num: i,
            };
            bt_insert(&dm, rfn, &Datum::Int4(42), tid, TypeId::Int4);
        }

        let results = bt_search(&dm, rfn, &Datum::Int4(42), TypeId::Int4);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn page_split() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        let rfn = 50003;
        create_index(&dm, rfn);

        // Insert enough entries to trigger a page split
        for i in 0..600 {
            let tid = ItemPointer {
                block_id: i / 255,
                offset_num: (i % 255) as u16 + 1,
            };
            bt_insert(&dm, rfn, &Datum::Int4(i as i32), tid, TypeId::Int4);
        }

        // Verify we can find all of them
        for i in 0..600 {
            let results = bt_search(&dm, rfn, &Datum::Int4(i as i32), TypeId::Int4);
            assert_eq!(results.len(), 1, "missing key {}", i);
        }

        // Verify tree has more than 2 pages (meta + at least 2 leaves + root)
        assert!(dm.num_pages(rfn) > 3);
    }
}
