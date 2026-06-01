//! Translation of postgres/src/common/archive.c
//!
//! Common WAL archive routines.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::common::percentrepl::replace_percent_placeholders;
use crate::port::path::make_native_path;
use core::ffi::{c_char, c_void};

/*
 * BuildRestoreCommand
 *
 * Builds a restore command to retrieve a file from WAL archives, replacing the
 * supported aliases (%f xlogfname, %r lastRestartPointFname, %p xlogpath) with
 * the caller-supplied values.  Result is a palloc'd string; an error is thrown
 * if a required value is NULL but its alias appears in the command.
 *
 * # Safety
 * The string arguments are NUL-terminated C strings (or null where allowed).
 */
pub unsafe fn BuildRestoreCommand(
    restoreCommand: *const c_char,
    xlogpath: *const c_char,
    xlogfname: *const c_char,
    lastRestartPointFname: *const c_char,
) -> *mut c_char {
    let mut nativePath: *mut c_char = null_mut();
    let result: *mut c_char;

    if !xlogpath.is_null() {
        nativePath = pstrdup(xlogpath);
        make_native_path(nativePath);
    }

    /* letters "frp" -> %f=xlogfname, %r=lastRestartPointFname, %p=nativePath */
    result = replace_percent_placeholders(
        restoreCommand,
        c"restore_command".as_ptr(),
        c"frp".as_ptr(),
        &[xlogfname, lastRestartPointFname, nativePath as *const c_char],
    );

    if !nativePath.is_null() {
        pfree(nativePath as *mut c_void);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn build_restore_command_substitutes() {
        unsafe {
            let cmd = BuildRestoreCommand(
                c"cp %r/%f %p".as_ptr(),
                c"/data/pg_wal/00000001".as_ptr(),
                c"000000010000000000000005".as_ptr(),
                c"000000010000000000000001".as_ptr(),
            );
            assert!(cstr_eq(
                cmd,
                "cp 000000010000000000000001/000000010000000000000005 /data/pg_wal/00000001"
            ));
        }
    }
}
