//! Translation of postgres/src/common/relpath.c (+ the relpath.h types it needs).
//!
//! Shared frontend/backend code to construct the file-system paths of relations
//! and databases (fork names, GetDatabasePath, GetRelationPath).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_int, c_uint};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

/* ---- relpath.h ---- */

/// A relation's "number" (its relfilenode), an Oid.
pub type RelFileNumber = Oid;

/// ForkNumber: which physical fork of a relation.  Modeled as c_int (C enum).
pub type ForkNumber = c_int;
pub const InvalidForkNumber: ForkNumber = -1;
pub const MAIN_FORKNUM: ForkNumber = 0;
pub const FSM_FORKNUM: ForkNumber = 1;
pub const VISIBILITYMAP_FORKNUM: ForkNumber = 2;
pub const INIT_FORKNUM: ForkNumber = 3;
pub const MAX_FORKNUM: ForkNumber = INIT_FORKNUM;

/// Max characters of a fork name (relpath.h).
pub const FORKNAMECHARS: usize = 4;

// TODO(pg-port): these are build-generated.  PG_TBLSPC_DIR is fixed; the version
// directory is "PG_" PG_MAJORVERSION "_" CATALOG_VERSION_NO - a representative
// PG 18 value is used here until the catalog version is wired through.
const PG_TBLSPC_DIR: &core::ffi::CStr = c"pg_tblspc";
const TABLESPACE_VERSION_DIRECTORY: &core::ffi::CStr = c"PG_18_202505071";

// catalog/pg_tablespace_d.h (well-known fixed OIDs).
const GLOBALTABLESPACE_OID: Oid = 1664;
const DEFAULTTABLESPACE_OID: Oid = 1663;

// storage/procnumber.h
const INVALID_PROC_NUMBER: c_int = -1;

/// REL_PATH_STR_MAXLEN: a safe upper bound on a relation path (the C value is a
/// computed macro; this is >= it).
pub const REL_PATH_STR_MAXLEN: usize = 96;

/// In-place relation-path result (relpath.h), used in critical sections.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelPathStr {
    pub str: [c_char; REL_PATH_STR_MAXLEN + 1],
}

/// The fork-name string for a given fork number (the C `forkNames[]` table).
#[inline]
fn forkname(fork: ForkNumber) -> *const c_char {
    match fork {
        MAIN_FORKNUM => c"main".as_ptr(),
        FSM_FORKNUM => c"fsm".as_ptr(),
        VISIBILITYMAP_FORKNUM => c"vm".as_ptr(),
        INIT_FORKNUM => c"init".as_ptr(),
        _ => c"".as_ptr(),
    }
}

/* errcodes.h (errcode() shim ignores the value). */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

/*
 * forkname_to_number - look up a fork name.
 *
 * # Safety
 * `forkName` is a valid NUL-terminated C string.
 */
pub unsafe fn forkname_to_number(forkName: *const c_char) -> ForkNumber {
    let mut fork_num: ForkNumber = 0;
    while fork_num <= MAX_FORKNUM {
        if strcmp(forkname(fork_num), forkName) == 0 {
            return fork_num;
        }
        fork_num += 1;
    }
    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
    ereport!(ERROR, errmsg!("invalid fork name"));
    InvalidForkNumber
}

/*
 * forkname_chars - if `str` begins with a fork name, return its length and the
 * fork number; else 0 and InvalidForkNumber.
 *
 * # Safety
 * `str` valid for reading until NUL; `fork` null or writable.
 */
pub unsafe fn forkname_chars(str: *const c_char, fork: *mut ForkNumber) -> c_int {
    let mut fork_num: ForkNumber = 1;
    while fork_num <= MAX_FORKNUM {
        let len = strlen(forkname(fork_num));
        if strncmp(forkname(fork_num), str, len) == 0 {
            if !fork.is_null() {
                *fork = fork_num;
            }
            return len as c_int;
        }
        fork_num += 1;
    }
    if !fork.is_null() {
        *fork = InvalidForkNumber;
    }
    0
}

/*
 * GetDatabasePath - path to a database directory (palloc'd string).
 */
pub unsafe fn GetDatabasePath(dbOid: Oid, spcOid: Oid) -> *mut c_char {
    let buf = palloc(REL_PATH_STR_MAXLEN + 1) as *mut c_char;
    if spcOid == GLOBALTABLESPACE_OID {
        /* Shared system relations live in {datadir}/global */
        Assert!(dbOid == 0);
        snprintf(buf, REL_PATH_STR_MAXLEN + 1, c"global".as_ptr());
    } else if spcOid == DEFAULTTABLESPACE_OID {
        snprintf(buf, REL_PATH_STR_MAXLEN + 1, c"base/%u".as_ptr(), dbOid as c_uint);
    } else {
        snprintf(
            buf,
            REL_PATH_STR_MAXLEN + 1,
            c"%s/%u/%s/%u".as_ptr(),
            PG_TBLSPC_DIR.as_ptr(),
            spcOid as c_uint,
            TABLESPACE_VERSION_DIRECTORY.as_ptr(),
            dbOid as c_uint,
        );
    }
    buf
}

/*
 * GetRelationPath - path to a relation's file, returned in-place in a RelPathStr.
 */
pub unsafe fn GetRelationPath(
    dbOid: Oid,
    spcOid: Oid,
    relNumber: RelFileNumber,
    procNumber: c_int,
    forkNumber: ForkNumber,
) -> RelPathStr {
    let mut rp = RelPathStr { str: [0; REL_PATH_STR_MAXLEN + 1] };
    let cap = REL_PATH_STR_MAXLEN + 1;
    let p = rp.str.as_mut_ptr();

    if spcOid == GLOBALTABLESPACE_OID {
        Assert!(dbOid == 0);
        Assert!(procNumber == INVALID_PROC_NUMBER);
        if forkNumber != MAIN_FORKNUM {
            snprintf(p, cap, c"global/%u_%s".as_ptr(), relNumber as c_uint, forkname(forkNumber));
        } else {
            snprintf(p, cap, c"global/%u".as_ptr(), relNumber as c_uint);
        }
    } else if spcOid == DEFAULTTABLESPACE_OID {
        if procNumber == INVALID_PROC_NUMBER {
            if forkNumber != MAIN_FORKNUM {
                snprintf(p, cap, c"base/%u/%u_%s".as_ptr(), dbOid as c_uint, relNumber as c_uint, forkname(forkNumber));
            } else {
                snprintf(p, cap, c"base/%u/%u".as_ptr(), dbOid as c_uint, relNumber as c_uint);
            }
        } else if forkNumber != MAIN_FORKNUM {
            snprintf(p, cap, c"base/%u/t%d_%u_%s".as_ptr(), dbOid as c_uint, procNumber, relNumber as c_uint, forkname(forkNumber));
        } else {
            snprintf(p, cap, c"base/%u/t%d_%u".as_ptr(), dbOid as c_uint, procNumber, relNumber as c_uint);
        }
    } else if procNumber == INVALID_PROC_NUMBER {
        if forkNumber != MAIN_FORKNUM {
            snprintf(p, cap, c"%s/%u/%s/%u/%u_%s".as_ptr(), PG_TBLSPC_DIR.as_ptr(), spcOid as c_uint, TABLESPACE_VERSION_DIRECTORY.as_ptr(), dbOid as c_uint, relNumber as c_uint, forkname(forkNumber));
        } else {
            snprintf(p, cap, c"%s/%u/%s/%u/%u".as_ptr(), PG_TBLSPC_DIR.as_ptr(), spcOid as c_uint, TABLESPACE_VERSION_DIRECTORY.as_ptr(), dbOid as c_uint, relNumber as c_uint);
        }
    } else if forkNumber != MAIN_FORKNUM {
        snprintf(p, cap, c"%s/%u/%s/%u/t%d_%u_%s".as_ptr(), PG_TBLSPC_DIR.as_ptr(), spcOid as c_uint, TABLESPACE_VERSION_DIRECTORY.as_ptr(), dbOid as c_uint, procNumber, relNumber as c_uint, forkname(forkNumber));
    } else {
        snprintf(p, cap, c"%s/%u/%s/%u/t%d_%u".as_ptr(), PG_TBLSPC_DIR.as_ptr(), spcOid as c_uint, TABLESPACE_VERSION_DIRECTORY.as_ptr(), dbOid as c_uint, procNumber, relNumber as c_uint);
    }

    rp
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn rp_eq(rp: &RelPathStr, want: &str) -> bool {
        let mut n = 0usize;
        while rp.str[n] != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(rp.str.as_ptr() as *const u8, n) == want.as_bytes()
    }
    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn paths_and_forknames() {
        unsafe {
            // default tablespace, main fork
            let rp = GetRelationPath(5, DEFAULTTABLESPACE_OID, 16384, INVALID_PROC_NUMBER, MAIN_FORKNUM);
            assert!(rp_eq(&rp, "base/5/16384"));
            // fsm fork
            let rp = GetRelationPath(5, DEFAULTTABLESPACE_OID, 16384, INVALID_PROC_NUMBER, FSM_FORKNUM);
            assert!(rp_eq(&rp, "base/5/16384_fsm"));
            // global tablespace
            let rp = GetRelationPath(0, GLOBALTABLESPACE_OID, 1262, INVALID_PROC_NUMBER, MAIN_FORKNUM);
            assert!(rp_eq(&rp, "global/1262"));
            // temp rel (procNumber 3) in default tablespace
            let rp = GetRelationPath(5, DEFAULTTABLESPACE_OID, 100, 3, MAIN_FORKNUM);
            assert!(rp_eq(&rp, "base/5/t3_100"));

            // GetDatabasePath
            assert!(cstr_eq(GetDatabasePath(5, DEFAULTTABLESPACE_OID), "base/5"));
            assert!(cstr_eq(GetDatabasePath(0, GLOBALTABLESPACE_OID), "global"));

            // fork name lookup
            assert_eq!(forkname_to_number(c"vm".as_ptr()), VISIBILITYMAP_FORKNUM);
            let mut f: ForkNumber = -99;
            assert_eq!(forkname_chars(c"fsm123".as_ptr(), &mut f), 3);
            assert_eq!(f, FSM_FORKNUM);
            assert_eq!(forkname_chars(c"xyz".as_ptr(), &mut f), 0);
            assert_eq!(f, InvalidForkNumber);
        }
    }
}
