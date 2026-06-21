//! common/controldata_utils.h - common code for pg_controldata output.

use std::ffi::{c_char, c_void};

// catalog/pg_control.h not yet ported; minimal opaque stub.
// TODO: dedup -> use crate::catalog::pg_control::ControlFileData when it lands.
pub type ControlFileData = c_void;

pub unsafe fn get_controlfile(
    DataDir: *const c_char,
    crc_ok_p: *mut bool,
) -> *mut ControlFileData {
    unimplemented!()
}

pub unsafe fn get_controlfile_by_exact_path(
    ControlFilePath: *const c_char,
    crc_ok_p: *mut bool,
) -> *mut ControlFileData {
    unimplemented!()
}

pub unsafe fn update_controlfile(
    DataDir: *const c_char,
    ControlFile: *mut ControlFileData,
    do_sync: bool,
) {
    use crate::catalog::pg_control::{ControlFileData as CF, PG_CONTROL_FILE_SIZE};
    use crate::port::pg_crc32c::{COMP_CRC32C, FIN_CRC32C, INIT_CRC32C};

    let cf = ControlFile as *mut CF;

    // Recompute the CRC over everything up to the crc field.
    (*cf).crc = INIT_CRC32C();
    (*cf).crc = COMP_CRC32C((*cf).crc, cf as *const c_void, core::mem::offset_of!(CF, crc));
    (*cf).crc = FIN_CRC32C((*cf).crc);

    // Write a zero-padded full-size control file image.
    let mut buffer = [0u8; PG_CONTROL_FILE_SIZE];
    core::ptr::copy_nonoverlapping(cf as *const u8, buffer.as_mut_ptr(), core::mem::size_of::<CF>());

    let mut path = [0 as c_char; 1024];
    libc::snprintf(
        path.as_mut_ptr(),
        1024,
        b"%s/global/pg_control\0".as_ptr() as *const c_char,
        DataDir,
    );

    let fd = libc::open(path.as_ptr(), libc::O_RDWR);
    if fd < 0 {
        return;
    }
    libc::pwrite(fd, buffer.as_ptr() as *const c_void, PG_CONTROL_FILE_SIZE, 0);
    if do_sync {
        libc::fsync(fd);
    }
    libc::close(fd);
}
