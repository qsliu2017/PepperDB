//! Translated from PostgreSQL src/include/postmaster/syslogger.h

use bitflags::bitflags;

// POSIX guarantees PIPE_BUF >= 512; PG caps the chunk at 64K. On Linux/macOS
// PIPE_BUF is 512, so use that (matches the C fallback path).
pub const PIPE_CHUNK_SIZE: i32 = 512;

// flag bits for PipeProtoHeader.flags (PIPE_PROTO_*).
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PipeProtoFlags: u8 {
        const IS_LAST     = 0x01;   // last chunk of message?
        const DEST_STDERR = 0x10;
        const DEST_CSVLOG = 0x20;
        const DEST_JSONLOG = 0x40;
    }
}

// On-disk/IPC framing header written to the syslogger pipe. The trailing
// `data[FLEXIBLE_ARRAY_MEMBER]` is a view over the surrounding buffer.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PipeProtoHeader {
    pub nuls: [u8; 2], // always \0\0
    pub len: u16,      // size of this chunk (data only)
    pub pid: i32,      // writer's pid
    pub flags: u8,     // bitmask of PIPE_PROTO_*
}

pub const PIPE_HEADER_SIZE: usize = core::mem::offset_of!(PipeProtoHeaderLayout, data);
pub const PIPE_MAX_PAYLOAD: i32 = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE as i32;

// Layout helper to compute PIPE_HEADER_SIZE = offsetof(PipeProtoHeader, data).
// The C struct packs data right after `flags` with no padding (1+... = 9 bytes).
#[repr(C)]
struct PipeProtoHeaderLayout {
    nuls: [u8; 2],
    len: u16,
    pid: i32,
    flags: u8,
    data: [u8; 0],
}

// GUC options.
pub static mut LOGGING_COLLECTOR: bool = false;
pub static mut LOG_ROTATION_AGE: i32 = 0;
pub static mut LOG_ROTATION_SIZE: i32 = 0;
pub static mut LOG_DIRECTORY: Option<String> = None;
pub static mut LOG_FILENAME: Option<String> = None;
pub static mut LOG_TRUNCATE_ON_ROTATION: bool = false;
pub static mut LOG_FILE_MODE: i32 = 0;

pub const LOG_METAINFO_DATAFILE: &str = "current_logfiles";
pub const LOG_METAINFO_DATAFILE_TMP: &str = "current_logfiles.tmp";

/// C: `int SysLogger_Start(int child_slot)`.
pub fn syslogger_start(_child_slot: i32) -> i32 {
    unimplemented!()
}

pub fn write_syslogger_file(_buffer: &[u8], _destination: i32) {
    unimplemented!()
}

pub fn syslogger_main(_startup_data: &[u8]) -> ! {
    unimplemented!()
}

pub fn check_logrotate_signal() -> bool {
    unimplemented!()
}

pub fn remove_logrotate_signal_files() {
    unimplemented!()
}
