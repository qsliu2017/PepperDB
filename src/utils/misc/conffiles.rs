//! utils/misc/conffiles.c - utilities for handling configuration files (GUC/auth).

use crate::prelude::*;

use crate::common::file_utils::{
    get_dirent_type, PGFileType, PGFILETYPE_DIR, PGFILETYPE_ERROR,
};
use crate::miscadmin::DataDir;
use crate::port::path::{canonicalize_path, get_parent_directory, join_path_components};
use crate::port::port_api::is_absolute_path;
use crate::port::qsort::{pg_qsort, pg_qsort_strcmp};
use crate::port::strlcpy::strlcpy;

use crate::pg_config_manual::MAXPGPATH;

// TODO(pg-port): ERRCODE_INVALID_PARAMETER_VALUE from utils/errcodes.h.
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// ---------------------------------------------------------------------------
// libc string primitives used verbatim from the C source.
// ---------------------------------------------------------------------------
extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// struct dirent / DIR.
//
// The dirent stub in common/file_utils.rs is opaque (no d_name), but the
// directory walk below needs d_name.  We read d_name through the same
// platform-specific offset trick used by common/rmtree.rs.  TODO: dedup once a
// shared, field-bearing dirent definition exists.
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
// Stubs for not-yet-ported callees (storage/fd.h, utils/elog.h psprintf, etc.).
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

// utils/palloc.h family helper psprintf (varargs printf into palloc'd buffer).
// TODO: port utils/mmgr/mcxt.c psprintf; for now accept a single pre-formatted
// string used by the two call sites' format ("could not open directory \"%s\""
// and "could not stat file \"%s\"").
unsafe fn psprintf(_fmt: *const c_char, _arg: *const c_char) -> *mut c_char {
    unimplemented!()
}

// utils/elog.h: errcode_for_file_access().  TODO: port from elog.c.
unsafe fn errcode_for_file_access() -> c_int {
    0
}

/*
 * AbsoluteConfigLocation
 *
 * Given a configuration file or directory location that may be a relative
 * path, return an absolute one.  We consider the location to be relative to
 * the directory holding the calling file, or to DataDir if no calling file.
 */
pub unsafe fn AbsoluteConfigLocation(
    location: *const c_char,
    calling_file: *const c_char,
) -> *mut c_char {
    if is_absolute_path(location) {
        pstrdup(location)
    } else {
        let mut abs_path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        if calling_file != null() {
            strlcpy(
                abs_path.as_mut_ptr(),
                calling_file,
                core::mem::size_of_val(&abs_path) as Size,
            );
            get_parent_directory(abs_path.as_mut_ptr());
            join_path_components(
                abs_path.as_mut_ptr(),
                abs_path.as_ptr(),
                location,
            );
            canonicalize_path(abs_path.as_mut_ptr());
        } else {
            Assert!(DataDir != null_mut());
            join_path_components(abs_path.as_mut_ptr(), DataDir, location);
            canonicalize_path(abs_path.as_mut_ptr());
        }
        pstrdup(abs_path.as_ptr())
    }
}

/*
 * GetConfFilesInDir
 *
 * Returns the list of config files located in a directory, in alphabetical
 * order.  On error, returns NULL with details about the error stored in
 * "err_msg".
 */
pub unsafe fn GetConfFilesInDir(
    includedir: *const c_char,
    calling_file: *const c_char,
    elevel: c_int,
    num_filenames: *mut c_int,
    err_msg: *mut *mut c_char,
) -> *mut *mut c_char {
    let directory: *mut c_char;
    let d: *mut DIR;
    let mut de: *mut dirent;
    let mut filenames: *mut *mut c_char = null_mut();
    let mut size_filenames: c_int;

    /*
     * Reject directory name that is all-blank (including empty), as that
     * leads to confusion --- we'd read the containing directory, typically
     * resulting in recursive inclusion of the same file(s).
     */
    if strspn(includedir, c" \t\r\n".as_ptr()) == strlen(includedir) {
        ereport!(
            elevel,
            "empty configuration directory name: invalid parameter value"
        );
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        *err_msg = c"empty configuration directory name".as_ptr() as *mut c_char;
        return null_mut();
    }

    directory = AbsoluteConfigLocation(includedir, calling_file);
    d = AllocateDir(directory);
    if d == null_mut() {
        ereport!(elevel, "could not open configuration directory");
        let _ = errcode_for_file_access();
        *err_msg = psprintf(c"could not open directory \"%s\"".as_ptr(), directory);
        // goto cleanup
        if d != null_mut() {
            FreeDir(d);
        }
        pfree(directory as *mut c_void);
        return filenames;
    }

    /*
     * Read the directory and put the filenames in an array, so we can sort
     * them prior to caller processing the contents.
     */
    size_filenames = 32;
    filenames = palloc(size_filenames as Size * core::mem::size_of::<*mut c_char>() as Size)
        as *mut *mut c_char;
    *num_filenames = 0;

    loop {
        de = ReadDir(d, directory);
        if de == null_mut() {
            break;
        }

        let de_type: PGFileType;
        let mut filename: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        /*
         * Only parse files with names ending in ".conf".  Explicitly reject
         * files starting with ".".  This excludes things like "." and "..",
         * as well as typical hidden files, backup files, and editor debris.
         */
        let d_name = dirent_d_name(de);
        let d_name_len = strlen(d_name);
        if d_name_len < 6 {
            continue;
        }
        if *d_name == b'.' as c_char {
            continue;
        }
        if strcmp(d_name.add(d_name_len - 5), c".conf".as_ptr()) != 0 {
            continue;
        }

        join_path_components(filename.as_mut_ptr(), directory, d_name);
        canonicalize_path(filename.as_mut_ptr());
        de_type = get_dirent_type(
            filename.as_ptr(),
            de as *const _ as *const crate::common::file_utils::dirent,
            true,
            elevel,
        );
        if de_type == PGFILETYPE_ERROR {
            *err_msg = psprintf(c"could not stat file \"%s\"".as_ptr(), filename.as_ptr());
            pfree(filenames as *mut c_void);
            filenames = null_mut();
            // goto cleanup
            if d != null_mut() {
                FreeDir(d);
            }
            pfree(directory as *mut c_void);
            return filenames;
        } else if de_type != PGFILETYPE_DIR {
            /* Add file to array, increasing its size in blocks of 32 */
            if *num_filenames >= size_filenames {
                size_filenames += 32;
                filenames = repalloc(
                    filenames as *mut c_void,
                    size_filenames as Size * core::mem::size_of::<*mut c_char>() as Size,
                ) as *mut *mut c_char;
            }
            *filenames.offset(*num_filenames as isize) = pstrdup(filename.as_ptr());
            *num_filenames += 1;
        }
    }

    /* Sort the files by name before leaving */
    if *num_filenames > 0 {
        pg_qsort(
            filenames as *mut c_void,
            *num_filenames as usize,
            core::mem::size_of::<*mut c_char>(),
            pg_qsort_strcmp,
        );
    }

    // cleanup:
    if d != null_mut() {
        FreeDir(d);
    }
    pfree(directory as *mut c_void);
    filenames
}
