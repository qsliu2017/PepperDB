//! Heap tuple operations matching PostgreSQL's on-disk format.
//!
//! Tuples have a 23-byte header (t_xmin, t_xmax, t_cid, t_ctid, t_infomask2,
//! t_infomask, t_hoff) followed by an optional null bitmap and MAXALIGN-padded
//! column data. Variable-length columns (Text) use PostgreSQL varlena encoding.
//! All integers and floats are stored little-endian (native x86/ARM).
//!
//! Page-level operations (init, checksums, ItemId packing) are in storage::bufpage.

pub mod visibilitymap;

use crate::access::transam::clog::{Clog, XidStatus};
use crate::access::transam::{Snapshot, BOOTSTRAP_XID, FROZEN_XID};
use crate::catalog::Column;
use crate::storage::bufpage::{
    maxalign, pack_item_id, read_u16, read_u32, unpack_item_id, write_u16, write_u32, Page,
    HEADER_SIZE, ITEM_ID_SIZE, LP_NORMAL, PAGE_SIZE, PD_LOWER, PD_PAGESIZE_VERSION, PD_SPECIAL,
    PD_UPPER,
};
use crate::types::{Datum, TypeId};

// -- Tuple header offsets (23 bytes) ------------------------------------------

const T_XMIN: usize = 0;
const T_XMAX: usize = 4;
const T_CTID_BLKID: usize = 12;
const T_CTID_POSID: usize = 16;
const T_INFOMASK2: usize = 18;
const T_INFOMASK: usize = 20;
const T_HOFF: usize = 22;
const TUPLE_HEADER_SIZE: usize = 23;

// -- t_infomask flags ---------------------------------------------------------

const HEAP_HASNULL: u16 = 0x0001;
const HEAP_HASVARWIDTH: u16 = 0x0002;
const HEAP_XMIN_COMMITTED: u16 = 0x0100;
const HEAP_XMIN_FROZEN: u16 = 0x0200;
const HEAP_XMAX_INVALID: u16 = 0x0800;

// -- HeapAccessMethod trait ---------------------------------------------------

pub trait HeapAccessMethod {
    #[allow(clippy::result_unit_err)]
    fn insert_tuple(&mut self, tuple: &[u8], block_id: u32) -> Result<u16, ()>;
    fn read_tuple(&self, item_index: u16, columns: &[Column]) -> Option<Vec<Datum>>;
    fn mark_tuple_dead(&mut self, item_index: u16);
    fn mark_tuple_dead_with_xid(&mut self, item_index: u16, xid: u32);
    fn tuple_visible(&self, item_index: u16, snapshot: &Snapshot, clog: &mut Clog) -> bool;
    fn read_tuple_mvcc(
        &self,
        item_index: u16,
        columns: &[Column],
        snapshot: &Snapshot,
        clog: &mut Clog,
    ) -> Option<Vec<Datum>>;
    fn compact_page(&mut self, clog: &mut Clog) -> u16;
    fn freeze_tuples(&mut self, clog: &mut Clog) -> u16;
}

impl HeapAccessMethod for Page {
    /// Insert a pre-built tuple into the page. Patches t_ctid with (block_id, item_index+1).
    /// Returns the 0-based item index.
    #[allow(clippy::result_unit_err)]
    fn insert_tuple(&mut self, tuple: &[u8], block_id: u32) -> Result<u16, ()> {
        let pd_lower = read_u16(&self.0, PD_LOWER) as usize;
        let pd_upper = read_u16(&self.0, PD_UPPER) as usize;

        let tuple_size = tuple.len();
        let needed_lower = pd_lower + ITEM_ID_SIZE;
        let needed_upper = pd_upper - tuple_size;

        if needed_lower > needed_upper {
            return Err(());
        }

        // Write tuple at end of free space
        let tuple_offset = pd_upper - tuple_size;
        self.0[tuple_offset..tuple_offset + tuple_size].copy_from_slice(tuple);

        // Patch t_ctid with actual location
        write_u32(&mut self.0, tuple_offset + T_CTID_BLKID, block_id);
        let item_index = (pd_lower - HEADER_SIZE) / ITEM_ID_SIZE;
        write_u16(
            &mut self.0,
            tuple_offset + T_CTID_POSID,
            (item_index + 1) as u16,
        );

        // Write packed ItemId at pd_lower
        let item_id = pack_item_id(tuple_offset as u16, LP_NORMAL, tuple_size as u16);
        write_u32(&mut self.0, pd_lower, item_id);

        // Update pd_lower and pd_upper
        write_u16(&mut self.0, PD_LOWER, needed_lower as u16);
        write_u16(&mut self.0, PD_UPPER, tuple_offset as u16);

        Ok(item_index as u16)
    }

    /// Read and deserialize a tuple at the given item index.
    fn read_tuple(&self, item_index: u16, columns: &[Column]) -> Option<Vec<Datum>> {
        let item_id_off = HEADER_SIZE + (item_index as usize) * ITEM_ID_SIZE;
        let pd_lower = read_u16(&self.0, PD_LOWER) as usize;
        if item_id_off + ITEM_ID_SIZE > pd_lower {
            return None;
        }

        let item_id = read_u32(&self.0, item_id_off);
        let (offset, flags, length) = unpack_item_id(item_id);
        if flags != LP_NORMAL {
            return None;
        }
        let offset = offset as usize;
        let length = length as usize;
        if offset == 0 && length == 0 {
            return None;
        }

        // Check if tuple is dead (t_xmax != 0 and HEAP_XMAX_INVALID not set)
        let t_xmax = read_u32(&self.0, offset + T_XMAX);
        let t_infomask = read_u16(&self.0, offset + T_INFOMASK);
        if t_xmax != 0 && (t_infomask & HEAP_XMAX_INVALID) == 0 {
            return None;
        }

        let t_hoff = self.0[offset + T_HOFF] as usize;
        let has_null = (t_infomask & HEAP_HASNULL) != 0;
        let ncols = columns.len();
        let data = &self.0[offset + t_hoff..offset + length];

        let mut result = Vec::with_capacity(ncols);
        let mut pos = 0usize;

        for (i, col) in columns.iter().enumerate() {
            if has_null {
                let byte = self.0[offset + TUPLE_HEADER_SIZE + i / 8];
                if (byte & (1 << (i % 8))) == 0 {
                    result.push(Datum::Null);
                    continue;
                }
            }

            let (datum, advance) = read_column_datum(data, pos, col.type_id);
            result.push(datum);
            pos += advance;
        }

        Some(result)
    }

    /// Mark a tuple as dead by setting t_xmax=1 and clearing HEAP_XMAX_INVALID.
    fn mark_tuple_dead(&mut self, item_index: u16) {
        self.mark_tuple_dead_with_xid(item_index, 1);
    }

    /// Mark a tuple as dead with a specific xid in t_xmax.
    fn mark_tuple_dead_with_xid(&mut self, item_index: u16, xid: u32) {
        let item_id_off = HEADER_SIZE + (item_index as usize) * ITEM_ID_SIZE;
        let item_id = read_u32(&self.0, item_id_off);
        let (offset, _, _) = unpack_item_id(item_id);
        let offset = offset as usize;

        write_u32(&mut self.0, offset + T_XMAX, xid);

        let infomask = read_u16(&self.0, offset + T_INFOMASK);
        write_u16(
            &mut self.0,
            offset + T_INFOMASK,
            infomask & !HEAP_XMAX_INVALID,
        );
    }

    /// Check MVCC visibility of a tuple. Returns true if the tuple should be visible
    /// to the given snapshot according to HeapTupleSatisfiesMVCC rules.
    fn tuple_visible(&self, item_index: u16, snapshot: &Snapshot, clog: &mut Clog) -> bool {
        let item_id_off = HEADER_SIZE + (item_index as usize) * ITEM_ID_SIZE;
        let pd_lower = read_u16(&self.0, PD_LOWER) as usize;
        if item_id_off + ITEM_ID_SIZE > pd_lower {
            return false;
        }

        let item_id = read_u32(&self.0, item_id_off);
        let (offset, flags, length) = unpack_item_id(item_id);
        if flags != LP_NORMAL || (offset == 0 && length == 0) {
            return false;
        }
        let offset = offset as usize;

        let t_xmin = read_u32(&self.0, offset + T_XMIN);
        let t_xmax = read_u32(&self.0, offset + T_XMAX);
        let t_infomask = read_u16(&self.0, offset + T_INFOMASK);

        // Check if inserting xact is visible
        let xmin_visible = if t_xmin == FROZEN_XID
            || t_xmin == BOOTSTRAP_XID
            || (t_infomask & HEAP_XMIN_COMMITTED) != 0
        {
            true
        } else {
            let status = clog.get_status(t_xmin);
            if status == XidStatus::Committed {
                xid_visible_in_snapshot(t_xmin, snapshot)
            } else {
                false
            }
        };

        if !xmin_visible {
            return false;
        }

        // Check if deleting xact makes it invisible
        if t_xmax == 0 || (t_infomask & HEAP_XMAX_INVALID) != 0 {
            return true; // not deleted
        }

        // t_xmax is set and HEAP_XMAX_INVALID is clear -- check if delete is visible
        let xmax_committed = clog.get_status(t_xmax) == XidStatus::Committed;
        if !xmax_committed {
            return true; // deleter hasn't committed
        }

        // Deleter committed -- tuple is invisible if delete is visible in snapshot
        !xid_visible_in_snapshot(t_xmax, snapshot)
    }

    /// Read a tuple using MVCC visibility (snapshot + CLOG).
    fn read_tuple_mvcc(
        &self,
        item_index: u16,
        columns: &[Column],
        snapshot: &Snapshot,
        clog: &mut Clog,
    ) -> Option<Vec<Datum>> {
        if !self.tuple_visible(item_index, snapshot, clog) {
            return None;
        }
        read_tuple_data(&self.0, item_index, columns)
    }

    /// Compact a page: remove dead tuples (t_xmax committed), defragment live tuples.
    /// Returns the number of dead tuples reclaimed.
    fn compact_page(&mut self, clog: &mut Clog) -> u16 {
        let n = self.num_items();
        let mut reclaimed = 0u16;

        // Collect live tuples: (item_index, tuple_bytes)
        let mut live: Vec<(u16, Vec<u8>)> = Vec::new();
        for i in 0..n {
            let item_id_off = HEADER_SIZE + (i as usize) * ITEM_ID_SIZE;
            let item_id = read_u32(&self.0, item_id_off);
            let (offset, flags, length) = unpack_item_id(item_id);
            if flags != LP_NORMAL || (offset == 0 && length == 0) {
                reclaimed += 1;
                continue;
            }

            let offset = offset as usize;
            let t_xmax = read_u32(&self.0, offset + T_XMAX);
            let t_infomask = read_u16(&self.0, offset + T_INFOMASK);

            // Dead: t_xmax set, HEAP_XMAX_INVALID clear, and xmax committed
            let is_dead = t_xmax != 0
                && (t_infomask & HEAP_XMAX_INVALID) == 0
                && clog.get_status(t_xmax) == XidStatus::Committed;

            if is_dead {
                reclaimed += 1;
            } else {
                let length = length as usize;
                live.push((i, self.0[offset..offset + length].to_vec()));
            }
        }

        if reclaimed == 0 {
            return 0;
        }

        // Rebuild page: keep header, rewrite ItemIds and tuples
        let lsn = self.0[0..8].to_vec();
        let checksum_bytes = self.0[8..10].to_vec();
        let flags_bytes = self.0[10..12].to_vec();
        let version_bytes = self.0[18..20].to_vec();

        // Clear ItemId area and tuple area
        self.0[HEADER_SIZE..].fill(0);

        // Reset pd_lower/pd_upper
        let new_pd_lower = HEADER_SIZE + live.len() * ITEM_ID_SIZE;
        let mut upper = PAGE_SIZE;

        for (slot, (_, tuple_data)) in live.iter().enumerate() {
            let tup_len = tuple_data.len();
            upper -= tup_len;

            // Write tuple data
            self.0[upper..upper + tup_len].copy_from_slice(tuple_data);

            // Patch t_ctid to new location
            let block_id = read_u32(&tuple_data.to_vec(), T_CTID_BLKID);
            write_u32(&mut self.0, upper + T_CTID_BLKID, block_id);
            write_u16(&mut self.0, upper + T_CTID_POSID, (slot + 1) as u16);

            // Write ItemId
            let item_id = pack_item_id(upper as u16, LP_NORMAL, tup_len as u16);
            write_u32(&mut self.0, HEADER_SIZE + slot * ITEM_ID_SIZE, item_id);
        }

        write_u16(&mut self.0, PD_LOWER, new_pd_lower as u16);
        write_u16(&mut self.0, PD_UPPER, upper as u16);
        write_u16(&mut self.0, PD_SPECIAL, PAGE_SIZE as u16);
        write_u16(
            &mut self.0,
            PD_PAGESIZE_VERSION,
            read_u16(&version_bytes, 0),
        );
        self.0[0..8].copy_from_slice(&lsn);
        self.0[8..10].copy_from_slice(&checksum_bytes);
        self.0[10..12].copy_from_slice(&flags_bytes);

        reclaimed
    }

    /// Freeze committed tuples: set t_xmin = FROZEN_XID (2) and HEAP_XMIN_FROZEN flag
    /// for tuples whose xmin is committed. Returns count of frozen tuples.
    fn freeze_tuples(&mut self, clog: &mut Clog) -> u16 {
        let n = self.num_items();
        let mut frozen_count = 0u16;

        for i in 0..n {
            let item_id_off = HEADER_SIZE + (i as usize) * ITEM_ID_SIZE;
            let item_id = read_u32(&self.0, item_id_off);
            let (offset, flags, _) = unpack_item_id(item_id);
            if flags != LP_NORMAL || offset == 0 {
                continue;
            }
            let offset = offset as usize;

            let t_xmin = read_u32(&self.0, offset + T_XMIN);
            let t_infomask = read_u16(&self.0, offset + T_INFOMASK);

            // Skip already frozen tuples
            if t_xmin == FROZEN_XID || (t_infomask & HEAP_XMIN_FROZEN) != 0 {
                continue;
            }

            // Freeze if xmin is committed
            let is_committed = t_xmin == BOOTSTRAP_XID
                || (t_infomask & HEAP_XMIN_COMMITTED) != 0
                || clog.get_status(t_xmin) == XidStatus::Committed;

            if is_committed {
                write_u32(&mut self.0, offset + T_XMIN, FROZEN_XID);
                let new_infomask = t_infomask | HEAP_XMIN_FROZEN | HEAP_XMIN_COMMITTED;
                write_u16(&mut self.0, offset + T_INFOMASK, new_infomask);
                frozen_count += 1;
            }
        }

        frozen_count
    }
}

// -- Free functions (not page-scoped) -----------------------------------------

/// Read a single column value from tuple data at the given position.
/// Returns (datum, bytes_consumed_from_pos). Handles alignment internally.
fn read_column_datum(data: &[u8], pos: usize, type_id: TypeId) -> (Datum, usize) {
    if type_id == TypeId::Text {
        // Peek at first byte to determine varlena format
        let first = data[pos];
        if (first & 0x01) != 0 {
            // Short varlena: 1B header, alignment=1
            let total_len = (first >> 1) as usize;
            let str_len = total_len - 1;
            let s = String::from_utf8_lossy(&data[pos + 1..pos + 1 + str_len]).into_owned();
            return (Datum::Text(s), total_len);
        }
        // Standard 4B varlena -- align to 4
        let align = 4usize;
        let pad = (align - (pos % align)) % align;
        let aligned = pos + pad;
        let varlena_hdr = u32::from_le_bytes(data[aligned..aligned + 4].try_into().unwrap());
        let total_len = (varlena_hdr >> 2) as usize;
        let str_len = total_len - 4;
        let s = String::from_utf8_lossy(&data[aligned + 4..aligned + 4 + str_len]).into_owned();
        return (Datum::Text(s), pad + total_len);
    }

    let align = type_id.align();
    let pad = (align - (pos % align)) % align;
    let aligned = pos + pad;

    let (datum, size) = match type_id {
        TypeId::Bool => (Datum::Bool(data[aligned] != 0), 1),
        TypeId::Int2 => {
            let val = i16::from_le_bytes([data[aligned], data[aligned + 1]]);
            (Datum::Int2(val), 2)
        }
        TypeId::Int4 => {
            let val = i32::from_le_bytes(data[aligned..aligned + 4].try_into().unwrap());
            (Datum::Int4(val), 4)
        }
        TypeId::Int8 => {
            let val = i64::from_le_bytes(data[aligned..aligned + 8].try_into().unwrap());
            (Datum::Int8(val), 8)
        }
        TypeId::Float4 => {
            let val = f32::from_le_bytes(data[aligned..aligned + 4].try_into().unwrap());
            (Datum::Float4(val), 4)
        }
        TypeId::Float8 => {
            let val = f64::from_le_bytes(data[aligned..aligned + 8].try_into().unwrap());
            (Datum::Float8(val), 8)
        }
        TypeId::Text => unreachable!(),
    };
    (datum, pad + size)
}

/// Build a complete on-disk tuple with bootstrap xid=1 (pre-committed).
pub fn build_tuple(values: &[Datum], columns: &[Column]) -> Vec<u8> {
    build_tuple_with_xid(values, columns, 1, true)
}

/// Build a complete on-disk tuple (header + bitmap + aligned data).
/// If `committed` is true, sets HEAP_XMIN_COMMITTED hint bit.
/// t_ctid is zeroed and will be patched by insert_tuple.
pub fn build_tuple_with_xid(
    values: &[Datum],
    columns: &[Column],
    xid: u32,
    committed: bool,
) -> Vec<u8> {
    let ncols = values.len();
    let has_null = values.iter().any(|v| matches!(v, Datum::Null));
    let has_varwidth = columns.iter().any(|c| c.type_id == TypeId::Text);

    let bitmap_bytes = if has_null { ncols.div_ceil(8) } else { 0 };
    let t_hoff = maxalign(TUPLE_HEADER_SIZE + bitmap_bytes);

    let mut tuple = vec![0u8; t_hoff];

    write_u32(&mut tuple, T_XMIN, xid);
    // t_xmax = 0, t_cid = 0, t_ctid = (0,0) -- already zeroed

    // t_infomask2: ncols in low 11 bits
    write_u16(&mut tuple, T_INFOMASK2, ncols as u16);

    // t_infomask
    let mut infomask: u16 = HEAP_XMAX_INVALID;
    if committed {
        infomask |= HEAP_XMIN_COMMITTED;
    }
    if has_null {
        infomask |= HEAP_HASNULL;
    }
    if has_varwidth {
        infomask |= HEAP_HASVARWIDTH;
    }
    write_u16(&mut tuple, T_INFOMASK, infomask);

    // t_hoff
    tuple[T_HOFF] = t_hoff as u8;

    // Null bitmap (bit set = NOT null, matching PostgreSQL convention)
    if has_null {
        for (i, v) in values.iter().enumerate() {
            if !matches!(v, Datum::Null) {
                tuple[TUPLE_HEADER_SIZE + i / 8] |= 1 << (i % 8);
            }
        }
    }

    // Column data with per-type alignment
    for (i, v) in values.iter().enumerate() {
        if matches!(v, Datum::Null) {
            continue;
        }
        // Short varlena (text <= 126 bytes) has alignment 1
        let align = match v {
            Datum::Text(s) if s.len() <= 126 => 1,
            _ => columns[i].type_id.align(),
        };
        let data_offset = tuple.len() - t_hoff;
        let misalign = data_offset % align;
        if misalign != 0 {
            tuple.resize(tuple.len() + align - misalign, 0);
        }

        match v {
            Datum::Null => unreachable!(),
            Datum::Bool(b) => tuple.push(if *b { 1 } else { 0 }),
            Datum::Int2(n) => tuple.extend_from_slice(&n.to_le_bytes()),
            Datum::Int4(n) => tuple.extend_from_slice(&n.to_le_bytes()),
            Datum::Int8(n) => tuple.extend_from_slice(&n.to_le_bytes()),
            Datum::Float4(f) => tuple.extend_from_slice(&f.to_le_bytes()),
            Datum::Float8(f) => tuple.extend_from_slice(&f.to_le_bytes()),
            Datum::Text(s) => {
                if s.len() <= 126 {
                    // Short varlena: 1B header, no alignment needed
                    let total_len = 1 + s.len();
                    let hdr = ((total_len as u8) << 1) | 0x01;
                    tuple.push(hdr);
                    tuple.extend_from_slice(s.as_bytes());
                } else {
                    // Standard 4B varlena header
                    let total_len = 4 + s.len();
                    let hdr = (total_len as u32) << 2;
                    tuple.extend_from_slice(&hdr.to_le_bytes());
                    tuple.extend_from_slice(s.as_bytes());
                }
            }
        }
    }

    tuple
}

/// Returns true if a committed XID is visible in the given snapshot.
fn xid_visible_in_snapshot(xid: u32, snapshot: &Snapshot) -> bool {
    if xid < snapshot.xmin {
        return true;
    }
    if xid >= snapshot.xmax {
        return false;
    }
    // In [xmin, xmax) -- check if it was in-progress at snapshot time
    !snapshot.xip.contains(&xid)
}

/// Read tuple data without visibility checks (used internally by read_tuple_mvcc).
fn read_tuple_data(
    buf: &[u8; PAGE_SIZE],
    item_index: u16,
    columns: &[Column],
) -> Option<Vec<Datum>> {
    let item_id_off = HEADER_SIZE + (item_index as usize) * ITEM_ID_SIZE;
    let pd_lower = read_u16(buf, PD_LOWER) as usize;
    if item_id_off + ITEM_ID_SIZE > pd_lower {
        return None;
    }

    let item_id = read_u32(buf, item_id_off);
    let (offset, flags, length) = unpack_item_id(item_id);
    if flags != LP_NORMAL {
        return None;
    }
    let offset = offset as usize;
    let length = length as usize;
    if offset == 0 && length == 0 {
        return None;
    }

    let t_hoff = buf[offset + T_HOFF] as usize;
    let t_infomask = read_u16(buf, offset + T_INFOMASK);
    let has_null = (t_infomask & HEAP_HASNULL) != 0;
    let ncols = columns.len();
    let data = &buf[offset + t_hoff..offset + length];

    let mut result = Vec::with_capacity(ncols);
    let mut pos = 0usize;

    for (i, col) in columns.iter().enumerate() {
        if has_null {
            let byte = buf[offset + TUPLE_HEADER_SIZE + i / 8];
            if (byte & (1 << (i % 8))) == 0 {
                result.push(Datum::Null);
                continue;
            }
        }

        let (datum, advance) = read_column_datum(data, pos, col.type_id);
        result.push(datum);
        pos += advance;
    }

    Some(result)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::bufpage::{
        pack_item_id, read_u16, read_u32, unpack_item_id, Page, HEADER_SIZE, LP_NORMAL, PAGE_SIZE,
        PD_LOWER, PD_PAGESIZE_VERSION, PD_SPECIAL, PD_UPPER,
    };

    fn col(name: &str, tid: TypeId, num: u16) -> Column {
        Column {
            name: name.into(),
            type_id: tid,
            col_num: num,
            typmod: -1,
        }
    }

    #[test]
    fn init_and_insert() {
        let mut page = Page::new();
        page.init();
        assert_eq!(page.num_items(), 0);

        let cols = vec![col("a", TypeId::Int4, 0), col("b", TypeId::Int4, 1)];
        let tuple = build_tuple(&[Datum::Int4(42), Datum::Int4(100)], &cols);
        let idx = page.insert_tuple(&tuple, 0).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(page.num_items(), 1);

        let datums = page.read_tuple(0, &cols).unwrap();
        assert_eq!(datums, vec![Datum::Int4(42), Datum::Int4(100)]);
    }

    #[test]
    fn fill_page_to_capacity() {
        let mut page = Page::new();
        page.init();

        let cols = vec![col("a", TypeId::Int4, 0)];
        let tuple = build_tuple(&[Datum::Int4(1)], &cols);
        // tuple = 24 (header, MAXALIGN(23)) + 4 (Int4) = 28 bytes
        assert_eq!(tuple.len(), 28);

        let mut count = 0u16;
        while page.insert_tuple(&tuple, 0).is_ok() {
            count += 1;
        }
        assert_eq!(page.num_items(), count);
        // Each slot = 28 (tuple) + 4 (ItemId) = 32 bytes
        // Available = 8192 - 28 (header) = 8164; 8164 / 32 = 255
        assert_eq!(count, 255);
    }

    #[test]
    fn tuple_serde_round_trip() {
        let values = vec![Datum::Int4(-1), Datum::Int4(i32::MAX), Datum::Int4(0)];
        let cols = vec![
            col("x", TypeId::Int4, 0),
            col("y", TypeId::Int4, 1),
            col("z", TypeId::Int4, 2),
        ];
        let tuple = build_tuple(&values, &cols);
        let mut page = Page::new();
        page.init();
        page.insert_tuple(&tuple, 0).unwrap();
        let out = page.read_tuple(0, &cols).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn null_bitmap_round_trip() {
        let values = vec![Datum::Int4(1), Datum::Null, Datum::Int4(3)];
        let cols = vec![
            col("a", TypeId::Int4, 0),
            col("b", TypeId::Int4, 1),
            col("c", TypeId::Int4, 2),
        ];
        let tuple = build_tuple(&values, &cols);
        let mut page = Page::new();
        page.init();
        page.insert_tuple(&tuple, 0).unwrap();
        let out = page.read_tuple(0, &cols).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn text_round_trip() {
        let values = vec![Datum::Text("hello".into()), Datum::Int4(42)];
        let cols = vec![col("s", TypeId::Text, 0), col("n", TypeId::Int4, 1)];
        let tuple = build_tuple(&values, &cols);
        let mut page = Page::new();
        page.init();
        page.insert_tuple(&tuple, 0).unwrap();
        let out = page.read_tuple(0, &cols).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn mixed_types_round_trip() {
        let values = vec![
            Datum::Bool(true),
            Datum::Int2(42),
            Datum::Int8(1_000_000_000_000),
            Datum::Float4(3.14),
            Datum::Float8(2.718281828),
        ];
        let cols = vec![
            col("a", TypeId::Bool, 0),
            col("b", TypeId::Int2, 1),
            col("c", TypeId::Int8, 2),
            col("d", TypeId::Float4, 3),
            col("e", TypeId::Float8, 4),
        ];
        let tuple = build_tuple(&values, &cols);
        let mut page = Page::new();
        page.init();
        page.insert_tuple(&tuple, 0).unwrap();
        let out = page.read_tuple(0, &cols).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn read_tuple_out_of_bounds() {
        let mut page = Page::new();
        page.init();
        let cols = vec![col("a", TypeId::Int4, 0)];
        assert!(page.read_tuple(0, &cols).is_none());
        assert!(page.read_tuple(100, &cols).is_none());
    }

    // -- Sprint 1 tests -------------------------------------------------------

    #[test]
    fn page_header_layout() {
        let mut page = Page::new();
        page.init();

        // pd_lsn (bytes 0-7) = 0
        assert_eq!(&page[0..8], &[0u8; 8]);
        // pd_checksum (bytes 8-9) = 0
        assert_eq!(read_u16(&*page, 8), 0);
        // pd_flags (bytes 10-11) = 0
        assert_eq!(read_u16(&*page, 10), 0);
        // pd_lower (bytes 12-13) = 28 (HEADER_SIZE)
        assert_eq!(read_u16(&*page, PD_LOWER), 28);
        // pd_upper (bytes 14-15) = 8192 (PAGE_SIZE)
        assert_eq!(read_u16(&*page, PD_UPPER), PAGE_SIZE as u16);
        // pd_special (bytes 16-17) = 8192
        assert_eq!(read_u16(&*page, PD_SPECIAL), PAGE_SIZE as u16);
        // pd_pagesize_version (bytes 18-19) = 0x2004
        assert_eq!(read_u16(&*page, PD_PAGESIZE_VERSION), 0x2004);
        // pd_prune_xid (bytes 20-23) = 0
        assert_eq!(read_u32(&*page, 20), 0);
        // Remaining header bytes (24-27) = 0
        assert_eq!(read_u32(&*page, 24), 0);
    }

    #[test]
    fn item_id_bitfield() {
        // Round-trip
        let (off, flags, len) = unpack_item_id(pack_item_id(8164, LP_NORMAL, 28));
        assert_eq!((off, flags, len), (8164, LP_NORMAL, 28));

        // Known bit pattern: offset=100, flags=1 (LP_NORMAL), length=32
        // u32 = (100 & 0x7FFF) | ((1 & 0x3) << 15) | ((32 & 0x7FFF) << 17)
        //      = 100 | 0x8000 | (32 << 17)
        //      = 100 | 32768 | 4194304 = 4227172
        let packed = pack_item_id(100, LP_NORMAL, 32);
        assert_eq!(packed, 100 | (1 << 15) | (32 << 17));
        let (o, f, l) = unpack_item_id(packed);
        assert_eq!((o, f, l), (100, 1, 32));
    }

    // -- Sprint 3: Byte-level verification tests ------------------------------

    #[test]
    fn pg_compatible_page_header() {
        let mut page = Page::new();
        page.init();

        // Verify all 28 header bytes
        let mut expected = [0u8; 28];
        expected[12..14].copy_from_slice(&28u16.to_le_bytes()); // pd_lower
        expected[14..16].copy_from_slice(&8192u16.to_le_bytes()); // pd_upper
        expected[16..18].copy_from_slice(&8192u16.to_le_bytes()); // pd_special
        expected[18..20].copy_from_slice(&0x2004u16.to_le_bytes()); // pd_pagesize_version
        assert_eq!(&page[..28], &expected);
    }

    #[test]
    fn pg_compatible_item_id() {
        let mut page = Page::new();
        page.init();

        let cols = vec![col("a", TypeId::Int4, 0), col("b", TypeId::Int4, 1)];
        let tuple = build_tuple(&[Datum::Int4(1), Datum::Int4(2)], &cols);
        let tuple_len = tuple.len(); // 24 (hdr) + 8 (two Int4) = 32
        page.insert_tuple(&tuple, 0).unwrap();

        // ItemId at byte 28 (first slot after 28-byte header)
        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, flags, len) = unpack_item_id(item_id);
        assert_eq!(off, (PAGE_SIZE - tuple_len) as u16); // 8192 - 32 = 8160
        assert_eq!(flags, LP_NORMAL);
        assert_eq!(len, tuple_len as u16);
    }

    #[test]
    fn pg_compatible_tuple_two_int4() {
        let mut page = Page::new();
        page.init();

        let cols = vec![col("a", TypeId::Int4, 0), col("b", TypeId::Int4, 1)];
        let tuple = build_tuple(&[Datum::Int4(42), Datum::Int4(100)], &cols);
        page.insert_tuple(&tuple, 5).unwrap();

        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, _, _) = unpack_item_id(item_id);
        let base = off as usize;

        // t_xmin = 1
        assert_eq!(read_u32(&*page, base + T_XMIN), 1);
        // t_xmax = 0
        assert_eq!(read_u32(&*page, base + T_XMAX), 0);
        // t_cid = 0
        assert_eq!(read_u32(&*page, base + 8), 0);
        // t_ctid = (block=5, offset=1)
        assert_eq!(read_u32(&*page, base + T_CTID_BLKID), 5);
        assert_eq!(read_u16(&*page, base + T_CTID_POSID), 1);
        // t_infomask2 = 2 (ncols)
        assert_eq!(read_u16(&*page, base + T_INFOMASK2), 2);
        // t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID = 0x0900
        assert_eq!(
            read_u16(&*page, base + T_INFOMASK),
            HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID
        );
        // t_hoff = 24 (MAXALIGN(23))
        assert_eq!(page[base + T_HOFF], 24);

        // Data at offset 24: two little-endian Int4 values
        let data_off = base + 24;
        assert_eq!(
            i32::from_le_bytes(page[data_off..data_off + 4].try_into().unwrap()),
            42
        );
        assert_eq!(
            i32::from_le_bytes(page[data_off + 4..data_off + 8].try_into().unwrap()),
            100
        );
    }

    #[test]
    fn pg_compatible_tuple_with_null() {
        let mut page = Page::new();
        page.init();

        let cols = vec![
            col("a", TypeId::Int4, 0),
            col("b", TypeId::Int4, 1),
            col("c", TypeId::Int4, 2),
        ];
        let tuple = build_tuple(&[Datum::Int4(10), Datum::Null, Datum::Int4(30)], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, _, _) = unpack_item_id(item_id);
        let base = off as usize;

        // t_infomask should include HEAP_HASNULL
        let infomask = read_u16(&*page, base + T_INFOMASK);
        assert_ne!(infomask & HEAP_HASNULL, 0);

        // t_hoff = MAXALIGN(23 + 1) = 24 (bitmap for 3 cols = 1 byte)
        assert_eq!(page[base + T_HOFF], 24);

        // Null bitmap at byte 23: bits 0,2 set (cols a,c non-null), bit 1 clear (col b null)
        // = 0b00000101 = 5
        assert_eq!(page[base + TUPLE_HEADER_SIZE], 0b00000101);

        // Data: Int4(10) then Int4(30) -- col b skipped
        let data_off = base + 24;
        assert_eq!(
            i32::from_le_bytes(page[data_off..data_off + 4].try_into().unwrap()),
            10
        );
        assert_eq!(
            i32::from_le_bytes(page[data_off + 4..data_off + 8].try_into().unwrap()),
            30
        );
    }

    #[test]
    fn pg_compatible_varlena_text() {
        let mut page = Page::new();
        page.init();

        let cols = vec![col("t", TypeId::Text, 0)];
        let tuple = build_tuple(&[Datum::Text("hello".into())], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, _, _) = unpack_item_id(item_id);
        let base = off as usize;

        // HEAP_HASVARWIDTH should be set
        let infomask = read_u16(&*page, base + T_INFOMASK);
        assert_ne!(infomask & HEAP_HASVARWIDTH, 0);

        // Short varlena: 1B header, total_len = 1 + 5 = 6, header = (6 << 1) | 0x01 = 13
        let data_off = base + 24;
        assert_eq!(page[data_off], 13); // short varlena header byte
        assert_eq!(&page[data_off + 1..data_off + 6], b"hello");
    }

    #[test]
    fn pg_compatible_alignment_bool_int8() {
        let mut page = Page::new();
        page.init();

        // Bool (align=1, size=1) then Int8 (align=8, size=8)
        let cols = vec![col("b", TypeId::Bool, 0), col("n", TypeId::Int8, 1)];
        let tuple = build_tuple(&[Datum::Bool(true), Datum::Int8(42)], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, _, _) = unpack_item_id(item_id);
        let base = off as usize;
        let data_off = base + 24; // t_hoff

        // Bool at data+0
        assert_eq!(page[data_off], 1);
        // 7 bytes padding (align to 8)
        assert_eq!(&page[data_off + 1..data_off + 8], &[0u8; 7]);
        // Int8 at data+8
        assert_eq!(
            i64::from_le_bytes(page[data_off + 8..data_off + 16].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn pg_compatible_alignment_mixed() {
        let mut page = Page::new();
        page.init();

        // Int4 (align=4) -> Bool (align=1) -> Int4 (align=4)
        let cols = vec![
            col("a", TypeId::Int4, 0),
            col("b", TypeId::Bool, 1),
            col("c", TypeId::Int4, 2),
        ];
        let tuple = build_tuple(&[Datum::Int4(1), Datum::Bool(true), Datum::Int4(2)], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, _, _) = unpack_item_id(item_id);
        let base = off as usize;
        let data_off = base + 24;

        // Int4 at data+0
        assert_eq!(
            i32::from_le_bytes(page[data_off..data_off + 4].try_into().unwrap()),
            1
        );
        // Bool at data+4 (align=1, no padding needed after 4 bytes)
        assert_eq!(page[data_off + 4], 1);
        // 3 bytes padding to align to 4
        assert_eq!(&page[data_off + 5..data_off + 8], &[0u8; 3]);
        // Int4 at data+8
        assert_eq!(
            i32::from_le_bytes(page[data_off + 8..data_off + 12].try_into().unwrap()),
            2
        );
    }

    #[test]
    fn mark_dead_clears_xmax_invalid() {
        let mut page = Page::new();
        page.init();

        let cols = vec![col("a", TypeId::Int4, 0)];
        let tuple = build_tuple(&[Datum::Int4(1)], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        // Before: tuple is live
        assert!(page.read_tuple(0, &cols).is_some());

        page.mark_tuple_dead(0);

        // After: tuple is dead
        assert!(page.read_tuple(0, &cols).is_none());

        // Verify t_xmax=1 and HEAP_XMAX_INVALID cleared
        let item_id = read_u32(&*page, HEADER_SIZE);
        let (off, _, _) = unpack_item_id(item_id);
        let base = off as usize;
        assert_eq!(read_u32(&*page, base + T_XMAX), 1);
        assert_eq!(read_u16(&*page, base + T_INFOMASK) & HEAP_XMAX_INVALID, 0);
    }

    // -- Page checksum tests --------------------------------------------------

    #[test]
    fn checksum_round_trip() {
        let mut page = Page::new();
        page.init();
        let cols = vec![col("a", TypeId::Int4, 0)];
        let tuple = build_tuple(&[Datum::Int4(42)], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        page.set_checksum(0);
        assert!(page.verify_checksum(0));
    }

    #[test]
    fn checksum_corruption() {
        let mut page = Page::new();
        page.init();
        page.set_checksum(0);
        assert!(page.verify_checksum(0));

        // Flip a data byte
        page[100] ^= 0xFF;
        assert!(!page.verify_checksum(0));
    }

    #[test]
    fn checksum_zero_page() {
        let page = Page::new();
        assert!(page.verify_checksum(0));
        assert!(page.verify_checksum(42));
    }

    #[test]
    fn checksum_block_number_matters() {
        let mut page = Page::new();
        page.init();
        page.set_checksum(0);
        assert!(page.verify_checksum(0));
        // Same page with different block number should fail
        assert!(!page.verify_checksum(1));
    }

    // -- VACUUM tests ---------------------------------------------------------

    fn make_clog() -> (tempfile::TempDir, Clog) {
        let dir = tempfile::tempdir().unwrap();
        let clog = Clog::new(&dir.path().join("pg_xact"));
        (dir, clog)
    }

    #[test]
    fn vacuum_reclaims_space() {
        let (_dir, mut clog) = make_clog();
        clog.set_status(1, XidStatus::Committed); // bootstrap xid

        let mut page = Page::new();
        page.init();
        let cols = vec![col("a", TypeId::Int4, 0)];

        // Insert 10 tuples
        for i in 0..10 {
            let tuple = build_tuple(&[Datum::Int4(i)], &cols);
            page.insert_tuple(&tuple, 0).unwrap();
        }
        assert_eq!(page.num_items(), 10);

        // Mark 5 as dead (xid=1 which is committed)
        for i in 0..5 {
            page.mark_tuple_dead(i);
        }

        let reclaimed = page.compact_page(&mut clog);
        assert_eq!(reclaimed, 5);
        assert_eq!(page.num_items(), 5);

        // Verify remaining tuples are readable
        for i in 0..5 {
            let datums = page.read_tuple(i, &cols).unwrap();
            assert_eq!(datums, vec![Datum::Int4((i + 5) as i32)]);
        }
    }

    #[test]
    fn vacuum_freeze() {
        let (_dir, mut clog) = make_clog();
        clog.set_status(1, XidStatus::Committed);

        let mut page = Page::new();
        page.init();
        let cols = vec![col("a", TypeId::Int4, 0)];
        let tuple = build_tuple(&[Datum::Int4(42)], &cols);
        page.insert_tuple(&tuple, 0).unwrap();

        let frozen = page.freeze_tuples(&mut clog);
        assert_eq!(frozen, 1);

        // Verify xmin = FROZEN_XID (2)
        let item_id = read_u32(&*page, HEADER_SIZE);
        let (offset, _, _) = unpack_item_id(item_id);
        assert_eq!(read_u32(&*page, offset as usize + T_XMIN), FROZEN_XID);
        let infomask = read_u16(&*page, offset as usize + T_INFOMASK);
        assert_ne!(infomask & HEAP_XMIN_FROZEN, 0);

        // Freezing again should be a no-op
        let frozen2 = page.freeze_tuples(&mut clog);
        assert_eq!(frozen2, 0);
    }
}
