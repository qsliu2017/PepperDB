//! pg_control -- persistent system state written to `global/pg_control`.
//! Subset of PostgreSQL's ControlFileData at exact byte offsets, so tools
//! like `pg_controldata` can parse the file.

use std::io;
use std::path::Path;

/// Database state, matching PostgreSQL's DBState enum values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum DBState {
    Shutdowned = 1,
    InProduction = 4,
}

impl DBState {
    fn from_u32(v: u32) -> Option<Self> {
        match v {
            1 => Some(Self::Shutdowned),
            4 => Some(Self::InProduction),
            _ => None,
        }
    }
}

/// Subset of PostgreSQL's ControlFileData.
#[derive(Debug, Clone)]
pub struct ControlFileData {
    pub system_identifier: u64,  // offset 0
    pub pg_control_version: u32, // offset 8
    pub catalog_version_no: u32, // offset 12
    pub state: DBState,          // offset 16
    // 4 bytes padding at offset 20
    pub checkpoint_redo: u64, // offset 24 (checkPointCopy.redo)
    // 8 bytes gap at offset 32
    pub timeline_id: u32, // offset 40 (checkPointCopy.ThisTimeLineID)
    // gap from 44 to 216
    pub data_checksum_version: u32, // offset 216
    pub blcksz: u32,                // offset 220
    pub relseg_size: u32,           // offset 224
    // 4 bytes gap at offset 228
    pub max_align: u32, // offset 232
}

/// Size of the control file on disk (one 8KB page).
const CONTROL_FILE_SIZE: usize = crate::storage::bufpage::PAGE_SIZE;

/// CRC covers bytes 0..CRC_OFFSET, CRC stored at last 4 bytes.
const CRC_OFFSET: usize = CONTROL_FILE_SIZE - 4;

impl Default for ControlFileData {
    fn default() -> Self {
        Self {
            system_identifier: 0,
            pg_control_version: 1300,
            catalog_version_no: 202307071,
            state: DBState::Shutdowned,
            checkpoint_redo: 0,
            timeline_id: 1,
            data_checksum_version: 1,
            blcksz: 8192,
            relseg_size: 131072,
            max_align: 8,
        }
    }
}

pub fn write_control_file(path: &Path, data: &ControlFileData) -> io::Result<()> {
    let mut buf = [0u8; CONTROL_FILE_SIZE];
    // system_identifier @ 0
    buf[0..8].copy_from_slice(&data.system_identifier.to_le_bytes());
    // pg_control_version @ 8
    buf[8..12].copy_from_slice(&data.pg_control_version.to_le_bytes());
    // catalog_version_no @ 12
    buf[12..16].copy_from_slice(&data.catalog_version_no.to_le_bytes());
    // state @ 16
    buf[16..20].copy_from_slice(&(data.state as u32).to_le_bytes());
    // checkpoint_redo @ 24
    buf[24..32].copy_from_slice(&data.checkpoint_redo.to_le_bytes());
    // timeline_id @ 40
    buf[40..44].copy_from_slice(&data.timeline_id.to_le_bytes());
    // data_checksum_version @ 216
    buf[216..220].copy_from_slice(&data.data_checksum_version.to_le_bytes());
    // blcksz @ 220
    buf[220..224].copy_from_slice(&data.blcksz.to_le_bytes());
    // relseg_size @ 224
    buf[224..228].copy_from_slice(&data.relseg_size.to_le_bytes());
    // max_align @ 232
    buf[232..236].copy_from_slice(&data.max_align.to_le_bytes());
    // CRC-32C of all preceding bytes
    let crc = crc32c::crc32c(&buf[..CRC_OFFSET]);
    buf[CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());

    std::fs::write(path, buf)
}

pub fn read_control_file(path: &Path) -> io::Result<ControlFileData> {
    let buf = std::fs::read(path)?;
    if buf.len() != CONTROL_FILE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pg_control: unexpected file size",
        ));
    }
    // Verify CRC
    let stored_crc = u32::from_le_bytes(buf[CRC_OFFSET..].try_into().unwrap());
    let computed_crc = crc32c::crc32c(&buf[..CRC_OFFSET]);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pg_control: CRC mismatch",
        ));
    }

    let r32 = |off: usize| u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
    let r64 = |off: usize| u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());

    let state = DBState::from_u32(r32(16))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "pg_control: invalid state"))?;

    Ok(ControlFileData {
        system_identifier: r64(0),
        pg_control_version: r32(8),
        catalog_version_no: r32(12),
        state,
        checkpoint_redo: r64(24),
        timeline_id: r32(40),
        data_checksum_version: r32(216),
        blcksz: r32(220),
        relseg_size: r32(224),
        max_align: r32(232),
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn control_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pg_control");
        let data = ControlFileData {
            system_identifier: 0xDEAD_BEEF_CAFE_BABE,
            checkpoint_redo: 0x1000,
            ..Default::default()
        };
        write_control_file(&path, &data).unwrap();
        let read = read_control_file(&path).unwrap();
        assert_eq!(read.system_identifier, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(read.pg_control_version, 1300);
        assert_eq!(read.catalog_version_no, 202307071);
        assert_eq!(read.state, DBState::Shutdowned);
        assert_eq!(read.checkpoint_redo, 0x1000);
        assert_eq!(read.timeline_id, 1);
        assert_eq!(read.data_checksum_version, 1);
        assert_eq!(read.blcksz, 8192);
        assert_eq!(read.relseg_size, 131072);
        assert_eq!(read.max_align, 8);
    }

    #[test]
    fn control_file_crc_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pg_control");
        let data = ControlFileData::default();
        write_control_file(&path, &data).unwrap();

        // Flip a byte
        let mut buf = std::fs::read(&path).unwrap();
        buf[0] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        let err = read_control_file(&path).unwrap_err();
        assert!(err.to_string().contains("CRC mismatch"));
    }

    #[test]
    fn control_file_state_transitions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pg_control");

        // Init: shutdowned
        let mut data = ControlFileData {
            system_identifier: 42,
            ..Default::default()
        };
        assert_eq!(data.state, DBState::Shutdowned);
        write_control_file(&path, &data).unwrap();

        // Startup: in production
        data.state = DBState::InProduction;
        write_control_file(&path, &data).unwrap();
        let read = read_control_file(&path).unwrap();
        assert_eq!(read.state, DBState::InProduction);

        // Clean shutdown
        data.state = DBState::Shutdowned;
        write_control_file(&path, &data).unwrap();
        let read = read_control_file(&path).unwrap();
        assert_eq!(read.state, DBState::Shutdowned);
    }
}
