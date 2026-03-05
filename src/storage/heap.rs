// Heap page layout following PostgreSQL's page structure:
//   [PageHeader 24B] [ItemId array ->] [... free space ...] [<- Tuples]
//
// Tuples have a minimal 8-byte header (t_xmin, t_xmax) followed by column
// data (big-endian Int4 values concatenated).

use crate::catalog::Column;
use crate::storage::disk::PAGE_SIZE;
use crate::types::{Datum, TypeId};

// PageHeader offsets (24 bytes total)
const PD_LOWER: usize = 12; // u16 at byte 12
const PD_UPPER: usize = 14; // u16 at byte 14
const HEADER_SIZE: usize = 24;

// ItemId: 4 bytes (offset u16 + length u16)
const ITEM_ID_SIZE: usize = 4;

// Tuple header: t_xmin(4) + t_xmax(4) = 8 bytes
const TUPLE_HEADER_SIZE: usize = 8;

fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

fn write_u16(buf: &mut [u8], off: usize, val: u16) {
    let bytes = val.to_le_bytes();
    buf[off] = bytes[0];
    buf[off + 1] = bytes[1];
}

pub fn init_page(buf: &mut [u8; PAGE_SIZE]) {
    buf.fill(0);
    write_u16(buf, PD_LOWER, HEADER_SIZE as u16);
    write_u16(buf, PD_UPPER, PAGE_SIZE as u16);
    // pd_special = PAGE_SIZE (no special space)
    write_u16(buf, 16, PAGE_SIZE as u16);
}

#[allow(clippy::result_unit_err)]
pub fn insert_tuple(buf: &mut [u8; PAGE_SIZE], tuple_data: &[u8]) -> Result<u16, ()> {
    let pd_lower = read_u16(buf, PD_LOWER) as usize;
    let pd_upper = read_u16(buf, PD_UPPER) as usize;

    let tuple_size = TUPLE_HEADER_SIZE + tuple_data.len();
    let needed_lower = pd_lower + ITEM_ID_SIZE;
    let needed_upper = pd_upper - tuple_size;

    if needed_lower > needed_upper {
        return Err(());
    }

    // Write tuple at the end of free space
    let tuple_offset = pd_upper - tuple_size;
    // Zero the tuple header (t_xmin=0, t_xmax=0)
    buf[tuple_offset..tuple_offset + TUPLE_HEADER_SIZE].fill(0);
    buf[tuple_offset + TUPLE_HEADER_SIZE..tuple_offset + tuple_size]
        .copy_from_slice(tuple_data);

    // Write ItemId at pd_lower
    let item_index = (pd_lower - HEADER_SIZE) / ITEM_ID_SIZE;
    write_u16(buf, pd_lower, tuple_offset as u16);
    write_u16(buf, pd_lower + 2, tuple_size as u16);

    // Update pd_lower and pd_upper
    write_u16(buf, PD_LOWER, needed_lower as u16);
    write_u16(buf, PD_UPPER, tuple_offset as u16);

    Ok(item_index as u16)
}

pub fn get_tuple(buf: &[u8; PAGE_SIZE], item_index: u16) -> Option<&[u8]> {
    let item_id_off = HEADER_SIZE + (item_index as usize) * ITEM_ID_SIZE;
    let pd_lower = read_u16(buf, PD_LOWER) as usize;
    if item_id_off + ITEM_ID_SIZE > pd_lower {
        return None;
    }
    let offset = read_u16(buf, item_id_off) as usize;
    let length = read_u16(buf, item_id_off + 2) as usize;
    if offset == 0 && length == 0 {
        return None;
    }
    // Skip tuple header, return just the data portion
    Some(&buf[offset + TUPLE_HEADER_SIZE..offset + length])
}

pub fn num_items(buf: &[u8; PAGE_SIZE]) -> u16 {
    let pd_lower = read_u16(buf, PD_LOWER) as usize;
    ((pd_lower - HEADER_SIZE) / ITEM_ID_SIZE) as u16
}

pub fn serialize_tuple(values: &[Datum]) -> Vec<u8> {
    let mut data = Vec::new();
    for v in values {
        match v {
            Datum::Int4(i) => data.extend_from_slice(&i.to_be_bytes()),
            Datum::Null => data.extend_from_slice(&0i32.to_be_bytes()),
        }
    }
    data
}

pub fn deserialize_tuple(data: &[u8], columns: &[Column]) -> Vec<Datum> {
    let mut result = Vec::with_capacity(columns.len());
    let mut offset = 0;
    for col in columns {
        match col.type_id {
            TypeId::Int4 => {
                let val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                result.push(Datum::Int4(val));
                offset += 4;
            }
        }
    }
    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn init_and_insert() {
        let mut page = [0u8; PAGE_SIZE];
        init_page(&mut page);
        assert_eq!(num_items(&page), 0);

        let data = serialize_tuple(&[Datum::Int4(42), Datum::Int4(100)]);
        let idx = insert_tuple(&mut page, &data).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(num_items(&page), 1);

        let read_back = get_tuple(&page, 0).unwrap();
        let cols = vec![
            Column { name: "a".into(), type_id: TypeId::Int4, col_num: 0 },
            Column { name: "b".into(), type_id: TypeId::Int4, col_num: 1 },
        ];
        let datums = deserialize_tuple(read_back, &cols);
        assert_eq!(datums, vec![Datum::Int4(42), Datum::Int4(100)]);
    }

    #[test]
    fn fill_page_to_capacity() {
        let mut page = [0u8; PAGE_SIZE];
        init_page(&mut page);

        let data = serialize_tuple(&[Datum::Int4(1)]);
        let mut count = 0u16;
        while insert_tuple(&mut page, &data).is_ok() {
            count += 1;
        }
        assert_eq!(num_items(&page), count);
        // Each tuple = 8 (header) + 4 (data) = 12 bytes, plus 4 byte ItemId = 16 per tuple
        // Available = 8192 - 24 = 8168; 8168 / 16 = 510
        assert_eq!(count, 510);
    }

    #[test]
    fn tuple_serde_round_trip() {
        let values = vec![Datum::Int4(-1), Datum::Int4(i32::MAX), Datum::Int4(0)];
        let data = serialize_tuple(&values);
        let cols = vec![
            Column { name: "x".into(), type_id: TypeId::Int4, col_num: 0 },
            Column { name: "y".into(), type_id: TypeId::Int4, col_num: 1 },
            Column { name: "z".into(), type_id: TypeId::Int4, col_num: 2 },
        ];
        let out = deserialize_tuple(&data, &cols);
        assert_eq!(out, values);
    }

    #[test]
    fn get_tuple_out_of_bounds() {
        let mut page = [0u8; PAGE_SIZE];
        init_page(&mut page);
        assert!(get_tuple(&page, 0).is_none());
        assert!(get_tuple(&page, 100).is_none());
    }
}
