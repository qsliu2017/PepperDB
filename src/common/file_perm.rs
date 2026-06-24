//! Translated from PostgreSQL src/include/common/file_perm.h
//! File and directory permission masks (POSIX mode bits, Linux/macOS values).

// POSIX mode bits (sys/stat.h), constant on both target platforms.
const S_IRWXU: u32 = 0o700;
const S_IRUSR: u32 = 0o400;
const S_IWUSR: u32 = 0o200;
const S_IRWXG: u32 = 0o070;
const S_IRGRP: u32 = 0o040;
const S_IWGRP: u32 = 0o020;
const S_IXGRP: u32 = 0o010;
const S_IRWXO: u32 = 0o007;

/// Mask: only the owner may read/write (the default).
pub const PG_MODE_MASK_OWNER: u32 = S_IRWXG | S_IRWXO;

/// Mask: also allow group read/execute.
pub const PG_MODE_MASK_GROUP: u32 = S_IWGRP | S_IRWXO;

/// Default mode for creating directories.
pub const PG_DIR_MODE_OWNER: u32 = S_IRWXU;

/// Directory mode allowing group read/execute.
pub const PG_DIR_MODE_GROUP: u32 = S_IRWXU | S_IRGRP | S_IXGRP;

/// Default mode for creating files.
pub const PG_FILE_MODE_OWNER: u32 = S_IRUSR | S_IWUSR;

/// File mode allowing group read.
pub const PG_FILE_MODE_GROUP: u32 = S_IRUSR | S_IWUSR | S_IRGRP;

// Process-global mode state in C; becomes session/task state in Phase 2.
pub static mut PG_DIR_CREATE_MODE: u32 = PG_DIR_MODE_OWNER;
pub static mut PG_FILE_CREATE_MODE: u32 = PG_FILE_MODE_OWNER;
pub static mut PG_MODE_MASK: u32 = PG_MODE_MASK_OWNER;

/// Set the create-mode/mask globals from the provided data-directory mode.
pub fn set_data_directory_create_perm(data_dir_mode: u32) {
    let _ = data_dir_mode;
    unimplemented!()
}

/// Set permissions/mask from the mode of an existing data directory.
pub fn get_data_directory_create_perm(data_dir: &str) -> std::io::Result<()> {
    let _ = data_dir;
    unimplemented!()
}
