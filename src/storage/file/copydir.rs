//! storage/file/copydir.c - copies a directory

use crate::prelude::*;

use crate::common::file_utils::{
    fsync_fname, get_dirent_type, PGFileType, PGFILETYPE_DIR, PGFILETYPE_REG,
};
use crate::miscadmin::{enableFsync, CHECK_FOR_INTERRUPTS};
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::MAXPGPATH;

use std::ffi::{c_char, c_int, c_void};

// ---------------------------------------------------------------------------
// libc primitives used verbatim from the C source.
// ---------------------------------------------------------------------------
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn __error() -> *mut c_int;
}

// errno accessor (macOS uses __error()).  TODO: dedup with a central errno shim.
#[inline]
unsafe fn errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *__error() = v;
}

// off_t modeled as i64 for the port (matches common/file_utils.rs).
type off_t = i64;

const ENOSPC: c_int = 28;

// ---------------------------------------------------------------------------
// struct dirent / DIR.
//
// The dirent stub in common/file_utils.rs is opaque (no d_name), but the
// directory walk below needs d_name.  Read d_name through the same
// platform-specific offset trick used by common/rmtree.rs / conffiles.rs.
// TODO: dedup once a shared, field-bearing dirent definition exists.
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
#[repr(C)]
struct dirent {
    _private: [u8; 0],
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct DIR {
    _private: [u8; 0],
}

#[inline]
unsafe fn dirent_d_name(de: *const dirent) -> *const c_char {
    #[cfg(target_os = "macos")]
    let off: isize = 21;
    #[cfg(not(target_os = "macos"))]
    let off: isize = 19;
    (de as *const u8).offset(off) as *const c_char
}

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees (storage/fd.h, pgstat, etc.).
// ---------------------------------------------------------------------------

// storage/fd.h: AllocateDir/ReadDir/FreeDir.  TODO: port storage/file/fd.c.
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!()
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!()
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!()
}

// storage/fd.h: MakePGDirectory.  TODO: port storage/file/fd.c.
unsafe fn MakePGDirectory(_directoryName: *const c_char) -> c_int {
    unimplemented!()
}

// storage/fd.h: OpenTransientFile/CloseTransientFile.  TODO: port storage/file/fd.c.
unsafe fn OpenTransientFile(_fileName: *const c_char, _fileFlags: c_int) -> c_int {
    unimplemented!()
}
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!()
}

// storage/fd.h: pg_flush_data.  TODO: port storage/file/fd.c.
unsafe fn pg_flush_data(_fd: c_int, _offset: off_t, _nbytes: off_t) {
    unimplemented!()
}

// utils/elog.h: errcode_for_file_access().  TODO: port from elog.c.
unsafe fn errcode_for_file_access() -> c_int {
    0
}

// pgstat wait-event reporting.  TODO: port utils/activity/wait_event.c.
unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {}
unsafe fn pgstat_report_wait_end() {}

// pgstat.h wait events.  TODO: generate from wait_event_names.txt.
const WAIT_EVENT_COPY_FILE_READ: u32 = 0;
const WAIT_EVENT_COPY_FILE_WRITE: u32 = 0;

// ---------------------------------------------------------------------------
// GUCs.
// ---------------------------------------------------------------------------
// storage/copydir.h: typedef enum FileCopyMethod
pub type FileCopyMethod = c_int;
pub const FILE_COPY_METHOD_COPY: FileCopyMethod = 0;
pub const FILE_COPY_METHOD_CLONE: FileCopyMethod = 1;

pub static mut file_copy_method: c_int = FILE_COPY_METHOD_COPY;

/// copydir: copy a directory
///
/// If recurse is false, subdirectories are ignored.  Anything that's not
/// a directory or a regular file is ignored.
///
/// This function uses the file_copy_method GUC.  New uses of this function must
/// be documented in doc/src/sgml/config.sgml.
pub unsafe fn copydir(fromdir: *const c_char, todir: *const c_char, recurse: bool) {
    let xldir: *mut DIR;
    let mut xlde: *mut dirent;
    let mut fromfile = [0 as c_char; MAXPGPATH * 2];
    let mut tofile = [0 as c_char; MAXPGPATH * 2];

    if MakePGDirectory(todir) != 0 {
        let _ = errcode_for_file_access();
        elog!(
            ERROR,
            "could not create directory \"{}\": %m",
            cstr(todir)
        );
    }

    let xldir = AllocateDir(fromdir);

    loop {
        xlde = ReadDir(xldir, fromdir);
        if xlde.is_null() {
            break;
        }

        let xlde_type: PGFileType;

        // If we got a cancel signal during the copy of the directory, quit
        CHECK_FOR_INTERRUPTS();

        let d_name = dirent_d_name(xlde);
        if strcmp(d_name, c".".as_ptr()) == 0 || strcmp(d_name, c"..".as_ptr()) == 0 {
            continue;
        }

        snprintf(
            fromfile.as_mut_ptr(),
            core::mem::size_of_val(&fromfile),
            c"%s/%s".as_ptr(),
            fromdir,
            d_name,
        );
        snprintf(
            tofile.as_mut_ptr(),
            core::mem::size_of_val(&tofile),
            c"%s/%s".as_ptr(),
            todir,
            d_name,
        );

        xlde_type = get_dirent_type(
            fromfile.as_ptr(),
            xlde as *const crate::common::file_utils::dirent,
            false,
            ERROR,
        );

        if xlde_type == PGFILETYPE_DIR {
            // recurse to handle subdirectories
            if recurse {
                copydir(fromfile.as_ptr(), tofile.as_ptr(), true);
            }
        } else if xlde_type == PGFILETYPE_REG {
            if file_copy_method == FILE_COPY_METHOD_CLONE {
                clone_file(fromfile.as_ptr(), tofile.as_ptr());
            } else {
                copy_file(fromfile.as_ptr(), tofile.as_ptr());
            }
        }
    }
    FreeDir(xldir);

    // Be paranoid here and fsync all files to ensure the copy is really done.
    // But if fsync is disabled, we're done.
    if !enableFsync {
        return;
    }

    let xldir = AllocateDir(todir);

    loop {
        xlde = ReadDir(xldir, todir);
        if xlde.is_null() {
            break;
        }

        let d_name = dirent_d_name(xlde);
        if strcmp(d_name, c".".as_ptr()) == 0 || strcmp(d_name, c"..".as_ptr()) == 0 {
            continue;
        }

        snprintf(
            tofile.as_mut_ptr(),
            core::mem::size_of_val(&tofile),
            c"%s/%s".as_ptr(),
            todir,
            d_name,
        );

        // We don't need to sync subdirectories here since the recursive
        // copydir will do it before it returns
        if get_dirent_type(
            tofile.as_ptr(),
            xlde as *const crate::common::file_utils::dirent,
            false,
            ERROR,
        ) == PGFILETYPE_REG
        {
            fsync_fname(tofile.as_ptr(), false);
        }
    }
    FreeDir(xldir);

    // It's important to fsync the destination directory itself as individual
    // file fsyncs don't guarantee that the directory entry for the file is
    // synced. Recent versions of ext4 have made the window much wider but
    // it's been true for ext3 and other filesystems in the past.
    fsync_fname(todir, true);
}

// Size of copy buffer (read and write requests)
const COPY_BUF_SIZE: usize = 8 * BLCKSZ;

// Size of data flush requests.  It seems beneficial on most platforms to
// do this every 1MB or so.  But macOS, at least with early releases of
// APFS, is really unfriendly to small mmap/msync requests, so there do it
// only every 32MB.
#[cfg(target_os = "macos")]
const FLUSH_DISTANCE: off_t = 32 * 1024 * 1024;
#[cfg(not(target_os = "macos"))]
const FLUSH_DISTANCE: off_t = 1024 * 1024;

// fcntl.h open flags.
const O_RDONLY: c_int = 0x0000;
const O_RDWR: c_int = 0x0002;
#[cfg(target_os = "macos")]
const O_CREAT: c_int = 0x0200;
#[cfg(not(target_os = "macos"))]
const O_CREAT: c_int = 0o100;
#[cfg(target_os = "macos")]
const O_EXCL: c_int = 0x0800;
#[cfg(not(target_os = "macos"))]
const O_EXCL: c_int = 0o200;
#[cfg(target_os = "macos")]
const O_WRONLY: c_int = 0x0001;
#[cfg(not(target_os = "macos"))]
const O_WRONLY: c_int = 0o1;

/// copy one file
pub unsafe fn copy_file(fromfile: *const c_char, tofile: *const c_char) {
    let buffer: *mut c_char;
    let srcfd: c_int;
    let dstfd: c_int;
    let mut nbytes: c_int;
    let mut offset: off_t;
    let mut flush_offset: off_t;

    // Use palloc to ensure we get a maxaligned buffer
    buffer = palloc(COPY_BUF_SIZE) as *mut c_char;

    // Open the files
    srcfd = OpenTransientFile(fromfile, O_RDONLY | PG_BINARY);
    if srcfd < 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not open file \"{}\": %m", cstr(fromfile));
    }

    dstfd = OpenTransientFile(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
    if dstfd < 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not create file \"{}\": %m", cstr(tofile));
    }

    // Do the data copying.
    flush_offset = 0;
    offset = 0;
    loop {
        // If we got a cancel signal during the copy of the file, quit
        CHECK_FOR_INTERRUPTS();

        // We fsync the files later, but during the copy, flush them every so
        // often to avoid spamming the cache and hopefully get the kernel to
        // start writing them out before the fsync comes.
        if offset - flush_offset >= FLUSH_DISTANCE {
            pg_flush_data(dstfd, flush_offset, offset - flush_offset);
            flush_offset = offset;
        }

        pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
        nbytes = read(srcfd, buffer as *mut c_void, COPY_BUF_SIZE) as c_int;
        pgstat_report_wait_end();
        if nbytes < 0 {
            let _ = errcode_for_file_access();
            elog!(ERROR, "could not read file \"{}\": %m", cstr(fromfile));
        }
        if nbytes == 0 {
            break;
        }
        set_errno(0);
        pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);
        if write(dstfd, buffer as *const c_void, nbytes as usize) as c_int != nbytes {
            // if write didn't set errno, assume problem is no disk space
            if errno() == 0 {
                set_errno(ENOSPC);
            }
            let _ = errcode_for_file_access();
            elog!(ERROR, "could not write to file \"{}\": %m", cstr(tofile));
        }
        pgstat_report_wait_end();

        offset += nbytes as off_t;
    }

    if offset > flush_offset {
        pg_flush_data(dstfd, flush_offset, offset - flush_offset);
    }

    if CloseTransientFile(dstfd) != 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not close file \"{}\": %m", cstr(tofile));
    }

    if CloseTransientFile(srcfd) != 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not close file \"{}\": %m", cstr(fromfile));
    }

    pfree(buffer as *mut c_void);
}

/// clone one file
///
/// On platforms with copy_file_range(2) (Linux et al.) this uses the kernel
/// reflink-aware copy.  macOS would use copyfile(3) with COPYFILE_CLONE_FORCE;
/// that path is left as a TODO since the port currently models the generic
/// HAVE_COPY_FILE_RANGE branch.
unsafe fn clone_file(fromfile: *const c_char, tofile: *const c_char) {
    let srcfd: c_int;
    let dstfd: c_int;
    let mut nbytes: isize;

    srcfd = OpenTransientFile(fromfile, O_RDONLY | PG_BINARY);
    if srcfd < 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not open file \"{}\": %m", cstr(fromfile));
    }

    dstfd = OpenTransientFile(tofile, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
    if dstfd < 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not create file \"{}\": %m", cstr(tofile));
    }

    loop {
        // Don't copy too much at once, so we can check for interrupts from
        // time to time if it falls back to a slow copy.
        CHECK_FOR_INTERRUPTS();
        pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_COPY);
        nbytes = copy_file_range(srcfd, null_mut(), dstfd, null_mut(), 1024 * 1024, 0);
        if nbytes < 0 && errno() != EINTR {
            let _ = errcode_for_file_access();
            elog!(
                ERROR,
                "could not clone file \"{}\" to \"{}\": %m",
                cstr(fromfile),
                cstr(tofile)
            );
        }
        pgstat_report_wait_end();

        if nbytes == 0 {
            break;
        }
    }

    if CloseTransientFile(dstfd) != 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not close file \"{}\": %m", cstr(tofile));
    }

    if CloseTransientFile(srcfd) != 0 {
        let _ = errcode_for_file_access();
        elog!(ERROR, "could not close file \"{}\": %m", cstr(fromfile));
    }
}

const EINTR: c_int = 4;
const WAIT_EVENT_COPY_FILE_COPY: u32 = 0;

// copy_file_range(2): Linux reflink-aware copy.  TODO: port real syscall shim;
// on non-Linux this would map to copyfile(3).  Modeled here per the
// HAVE_COPY_FILE_RANGE branch of the C source.
unsafe fn copy_file_range(
    _fd_in: c_int,
    _off_in: *mut off_t,
    _fd_out: c_int,
    _off_out: *mut off_t,
    _len: usize,
    _flags: c_uint,
) -> isize {
    unimplemented!()
}

// Helper: render a NUL-terminated C string for elog! runtime formatting.
// TODO: dedup - centralize a cstr() display shim for %m/%s elog ports.
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        return String::from("(null)");
    }
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}
