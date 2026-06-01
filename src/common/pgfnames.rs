//! Translation of postgres/src/common/pgfnames.c
//!   directory handling functions
//!
//! This is a src/common/ file shared between frontend and backend.  In the
//! backend build pg_log_warning expands to elog(WARNING, ...); in the frontend
//! build it comes from common/logging.h.  We do not have either wired up at the
//! granularity this file needs, so pg_log_warning is stubbed here as a local
//! no-op (NOTE: real PostgreSQL emits a warning containing the errno via %m on
//! opendir/readdir/closedir failure -- we drop the message but preserve the
//! control flow: NULL return on opendir failure, NULL-terminated array otherwise).
//!
//! palloc/repalloc/pstrdup/pfree come from crate::utils::palloc via the prelude,
//! matching the backend (!FRONTEND) lowering of pstrdup; the C also uses
//! pg_strdup in the frontend case but the logic is identical.

use crate::prelude::*;

// libc directory-iteration API.  We declare only what this file touches.
// `dirent` here is a minimal #[repr(C)] view: pgfnames only ever reads
// `de->d_name`, so the leading fields are layout padding to reach d_name.
//
// On the platforms PostgreSQL targets, `struct dirent` begins with
// d_ino / d_off / d_reclen / d_type before the NUL-terminated d_name[] array.
// We model exactly that prefix so the offset of d_name is correct, and size
// d_name at 256 (NAME_MAX + 1) which is ample for any single entry name we
// then immediately pstrdup out of the buffer.
#[repr(C)]
struct dirent {
    d_ino: u64,
    d_off: u64,
    d_reclen: u16,
    d_type: u8,
    d_name: [c_char; 256],
}

extern "C" {
    fn opendir(name: *const c_char) -> *mut c_void;
    fn readdir(dirp: *mut c_void) -> *mut dirent;
    fn closedir(dirp: *mut c_void) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// Stub for common/logging.h pg_log_warning / backend elog(WARNING).
// Real builds format `msg` with errno (%m); here it is a no-op.
unsafe fn pg_log_warning(_msg: &str) {}

/// pgfnames
///
/// return a list of the names of objects in the argument directory.  Caller
/// must call pgfnames_cleanup later to free the memory allocated by this
/// function.  Returns a NULL-terminated `*mut *mut c_char`, or null on
/// opendir failure.
pub unsafe fn pgfnames(path: *const c_char) -> *mut *mut c_char {
    let mut numnames: c_int = 0;
    let mut fnsize: c_int = 200; // enough for many small dbs

    let dir = opendir(path);
    if dir.is_null() {
        pg_log_warning("could not open directory");
        return null_mut();
    }

    let mut filenames =
        palloc((fnsize as Size) * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

    loop {
        let file = readdir(dir);
        if file.is_null() {
            break;
        }

        let dname = (*file).d_name.as_ptr();

        // skip "." and ".."
        let dot = b".\0".as_ptr() as *const c_char;
        let dotdot = b"..\0".as_ptr() as *const c_char;
        if strcmp(dname, dot) != 0 && strcmp(dname, dotdot) != 0 {
            if numnames + 1 >= fnsize {
                fnsize *= 2;
                filenames = repalloc(
                    filenames as *mut c_void,
                    (fnsize as Size) * core::mem::size_of::<*mut c_char>(),
                ) as *mut *mut c_char;
            }
            *filenames.offset(numnames as isize) = pstrdup(dname);
            numnames += 1;
        }
    }

    // C reads errno after the loop to detect readdir failure; we cannot
    // portably distinguish end-of-directory from error through this minimal
    // binding, so we simply finalize the array.  (pg_log_warning was a no-op
    // anyway.)

    *filenames.offset(numnames as isize) = null_mut();

    if closedir(dir) != 0 {
        pg_log_warning("could not close directory");
    }

    filenames
}

/// pgfnames_cleanup
///
/// deallocate memory used for filenames
pub unsafe fn pgfnames_cleanup(filenames: *mut *mut c_char) {
    let mut fn_: *mut *mut c_char = filenames;
    while !(*fn_).is_null() {
        pfree(*fn_ as *mut c_void);
        fn_ = fn_.offset(1);
    }
    pfree(filenames as *mut c_void);
}

#[cfg(test)]
mod tests {
    use super::*;

    // We cannot portably exercise the real opendir/readdir path in a unit
    // test, so we validate pgfnames_cleanup against a hand-built array shaped
    // exactly like pgfnames' output: 2 pstrdup'd entries followed by a NULL
    // terminator, all in a palloc'd pointer block.  This frees without crash.
    #[test]
    fn cleanup_frees_handbuilt_array() {
        unsafe {
            let n: Size = 3; // 2 names + NULL terminator
            let arr =
                palloc(n * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

            *arr.offset(0) = pstrdup(b"alpha\0".as_ptr() as *const c_char);
            *arr.offset(1) = pstrdup(b"beta\0".as_ptr() as *const c_char);
            *arr.offset(2) = null_mut();

            pgfnames_cleanup(arr);
        }
    }
}
