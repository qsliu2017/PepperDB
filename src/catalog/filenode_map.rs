//! pg_filenode.map -- bootstrap mapping from catalog OID to relfilenode.
//! Fixed 512-byte file in `global/pg_filenode.map`.
//!
//! Layout: magic (4B), num_mappings (4B), array of (oid, rfn) pairs (8B each),
//! zero-padding to byte 504, next_oid (4B at offset 504), CRC-32C (4B at offset 508).

use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::types::OID;

const MAP_FILE_SIZE: usize = 512;
const MAP_MAGIC: u32 = 0x592717;
const HEADER_SIZE: usize = 8; // magic + num_mappings
const ENTRY_SIZE: usize = 8; // oid (4B) + rfn (4B)
const NEXT_OID_OFFSET: usize = 504;
const CRC_OFFSET: usize = 508;

pub fn write_filenode_map(path: &Path, mappings: &[(OID, OID)], next_oid: OID) -> io::Result<()> {
    let mut buf = [0u8; MAP_FILE_SIZE];
    buf[0..4].copy_from_slice(&MAP_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&(mappings.len() as u32).to_le_bytes());

    for (i, &(oid, rfn)) in mappings.iter().enumerate() {
        let off = HEADER_SIZE + i * ENTRY_SIZE;
        buf[off..off + 4].copy_from_slice(&oid.to_le_bytes());
        buf[off + 4..off + 8].copy_from_slice(&rfn.to_le_bytes());
    }

    buf[NEXT_OID_OFFSET..NEXT_OID_OFFSET + 4].copy_from_slice(&next_oid.to_le_bytes());

    let crc = crc32c::crc32c(&buf[..CRC_OFFSET]);
    buf[CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());

    std::fs::write(path, buf)
}

pub fn read_filenode_map(path: &Path) -> io::Result<(HashMap<OID, OID>, OID)> {
    let buf = std::fs::read(path)?;
    if buf.len() != MAP_FILE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pg_filenode.map: unexpected file size",
        ));
    }

    let stored_crc = u32::from_le_bytes(buf[CRC_OFFSET..].try_into().unwrap());
    let computed_crc = crc32c::crc32c(&buf[..CRC_OFFSET]);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pg_filenode.map: CRC mismatch",
        ));
    }

    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != MAP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pg_filenode.map: bad magic",
        ));
    }

    let num = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let mut mappings = HashMap::with_capacity(num);
    for i in 0..num {
        let off = HEADER_SIZE + i * ENTRY_SIZE;
        let oid = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
        let rfn = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
        mappings.insert(oid, rfn);
    }

    let next_oid = u32::from_le_bytes(
        buf[NEXT_OID_OFFSET..NEXT_OID_OFFSET + 4]
            .try_into()
            .unwrap(),
    );

    Ok((mappings, next_oid))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn filenode_map_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pg_filenode.map");
        let mappings = vec![(1259, 1259), (1249, 1249)];
        write_filenode_map(&path, &mappings, 16384).unwrap();

        let (map, next_oid) = read_filenode_map(&path).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map[&1259], 1259);
        assert_eq!(map[&1249], 1249);
        assert_eq!(next_oid, 16384);
    }
}
