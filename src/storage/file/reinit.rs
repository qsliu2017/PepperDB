//! storage/file/reinit.c - Reinitialization of unlogged relations

use crate::prelude::*;

use crate::common::relpath::{
    forkname_chars, ForkNumber, RelFileNumber, INIT_FORKNUM, InvalidForkNumber, MAIN_FORKNUM,
};
use crate::pg_config_manual::MAXPGPATH;
use crate::postmaster::startup::begin_startup_progress_phase;
use crate::storage::file::copydir::copy_file;
use crate::utils::hash::dynahash::{
    hash_create, hash_destroy, hash_get_num_entries, hash_search, HASHACTION, HASHCTL, HTAB,
    HASH_BLOBS, HASH_CONTEXT, HASH_ELEM,
};

use std::ffi::{c_char, c_int, c_void};

// storage/reinit.h: operation flags for ResetUnloggedRelations.
pub const UNLOGGED_RELATION_INIT: c_int = 0x0001;
pub const UNLOGGED_RELATION_CLEANUP: c_int = 0x0002;

// InvalidRelFileNumber (relpath.h / common/relpath.h).
const InvalidRelFileNumber: RelFileNumber = 0;

// PG_TBLSPC_DIR / TABLESPACE_VERSION_DIRECTORY are private in common/relpath.rs;
// re-declare the same constants here (TODO: export from common/relpath.rs).
const PG_TBLSPC_DIR: &core::ffi::CStr = c"pg_tblspc";
const TABLESPACE_VERSION_DIRECTORY: &core::ffi::CStr = c"PG_18_202505071";

// PG_UINT32_MAX from c.h.
const PG_UINT32_MAX: c_ulong = u32::MAX as c_ulong;

// ---------------------------------------------------------------------------
// libc primitives used verbatim from the C source.
// ---------------------------------------------------------------------------
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strlen(s: *const c_char) -> usize;
    fn strtoul(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *__error() = v;
}

const ENOENT: c_int = 2;

// ---------------------------------------------------------------------------
// struct dirent / DIR.
//
// The dirent stub in common/file_utils.rs is opaque (no d_name); the directory
// walk below needs d_name, read through the same platform-specific offset trick
// used by storage/file/copydir.rs.  TODO: dedup once a shared, field-bearing
// dirent definition exists.
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
// Stubs for not-yet-ported callees.
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

// common/file_utils.h: fsync_fname.  TODO: import once common/file_utils.rs
// exposes it for this path (copydir.rs uses it through its own import).
unsafe fn fsync_fname(_fname: *const c_char, _isdir: bool) {
    unimplemented!()
}

// utils/elog.h: errcode_for_file_access.  TODO: real ereport machinery.
unsafe fn errcode_for_file_access() -> c_int {
    0
}

// storage/startup.h: ereport_startup_progress.  In C this is a macro that
// conditionally reports progress; modeled here as a no-op accepting the
// formatted path.  TODO: real startup-progress reporting.
unsafe fn ereport_startup_progress(_fmt: *const c_char, _path: *const c_char) {
    let _ = (_fmt, _path);
}

// Helper to stringify a C string for elog! formatting.
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}

// ---------------------------------------------------------------------------
// reinit.c
// ---------------------------------------------------------------------------

/// hash key for the unlogged relation OID table.
#[allow(non_camel_case_types)]
#[repr(C)]
struct unlogged_relation_entry {
    relnumber: RelFileNumber, // hash key
}

/*
 * Reset unlogged relations from before the last restart.
 *
 * If op includes UNLOGGED_RELATION_CLEANUP, we remove all forks of any
 * relation with an "init" fork, except for the "init" fork itself.
 *
 * If op includes UNLOGGED_RELATION_INIT, we copy the "init" fork to the main
 * fork.
 */
pub unsafe fn ResetUnloggedRelations(op: c_int) {
    let mut temp_path: [c_char;
        MAXPGPATH + 1 + PG_TBLSPC_DIR.to_bytes_with_nul().len()
            + 1 + TABLESPACE_VERSION_DIRECTORY.to_bytes_with_nul().len()] =
        [0; MAXPGPATH + 1 + PG_TBLSPC_DIR.to_bytes_with_nul().len()
            + 1 + TABLESPACE_VERSION_DIRECTORY.to_bytes_with_nul().len()];
    let spc_dir: *mut DIR;
    let mut spc_de: *mut dirent;
    let tmpctx: MemoryContext;
    let oldctx: MemoryContext;

    /* Log it. */
    elog!(
        DEBUG1,
        "resetting unlogged relations: cleanup {} init {}",
        ((op & UNLOGGED_RELATION_CLEANUP) != 0) as c_int,
        ((op & UNLOGGED_RELATION_INIT) != 0) as c_int
    );

    /*
     * Just to be sure we don't leak any memory, let's create a temporary
     * memory context for this operation.
     */
    tmpctx = AllocSetContextCreate!(
        CurrentMemoryContext,
        "ResetUnloggedRelations\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES
    ) as *mut _;
    oldctx = MemoryContextSwitchTo(tmpctx);

    /* Prepare to report progress resetting unlogged relations. */
    begin_startup_progress_phase();

    /*
     * First process unlogged files in pg_default ($PGDATA/base)
     */
    ResetUnloggedRelationsInTablespaceDir(c"base".as_ptr(), op);

    /*
     * Cycle through directories for all non-default tablespaces.
     */
    spc_dir = AllocateDir(PG_TBLSPC_DIR.as_ptr());

    loop {
        spc_de = ReadDir(spc_dir, PG_TBLSPC_DIR.as_ptr());
        if spc_de.is_null() {
            break;
        }

        let d_name = dirent_d_name(spc_de);
        if strcmp(d_name, c".".as_ptr()) == 0 || strcmp(d_name, c"..".as_ptr()) == 0 {
            continue;
        }

        snprintf(
            temp_path.as_mut_ptr(),
            std::mem::size_of_val(&temp_path),
            c"%s/%s/%s".as_ptr(),
            PG_TBLSPC_DIR.as_ptr(),
            d_name,
            TABLESPACE_VERSION_DIRECTORY.as_ptr(),
        );
        ResetUnloggedRelationsInTablespaceDir(temp_path.as_ptr(), op);
    }

    FreeDir(spc_dir);

    /*
     * Restore memory context.
     */
    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(tmpctx);
}

/*
 * Process one tablespace directory for ResetUnloggedRelations
 */
unsafe fn ResetUnloggedRelationsInTablespaceDir(tsdirname: *const c_char, op: c_int) {
    let ts_dir: *mut DIR;
    let mut de: *mut dirent;
    let mut dbspace_path: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];

    ts_dir = AllocateDir(tsdirname);

    /*
     * If we get ENOENT on a tablespace directory, log it and return.  This
     * can happen if a previous DROP TABLESPACE crashed between removing the
     * tablespace directory and removing the symlink in pg_tblspc.  We don't
     * really want to prevent database startup in that scenario, so let it
     * pass instead.  Any other type of error will be reported by ReadDir
     * (causing a startup failure).
     */
    if ts_dir.is_null() && errno() == ENOENT {
        let _ = errcode_for_file_access();
        elog!(LOG, "could not open directory \"{}\": %m", cstr(tsdirname));
        return;
    }

    loop {
        de = ReadDir(ts_dir, tsdirname);
        if de.is_null() {
            break;
        }

        let d_name = dirent_d_name(de);

        /*
         * We're only interested in the per-database directories, which have
         * numeric names.  Note that this code will also (properly) ignore "."
         * and "..".
         */
        if strspn(d_name, c"0123456789".as_ptr()) != strlen(d_name) {
            continue;
        }

        snprintf(
            dbspace_path.as_mut_ptr(),
            std::mem::size_of_val(&dbspace_path),
            c"%s/%s".as_ptr(),
            tsdirname,
            d_name,
        );

        if op & UNLOGGED_RELATION_INIT != 0 {
            ereport_startup_progress(
                c"resetting unlogged relations (init), elapsed time: %ld.%02d s, current path: %s"
                    .as_ptr(),
                dbspace_path.as_ptr(),
            );
        } else if op & UNLOGGED_RELATION_CLEANUP != 0 {
            ereport_startup_progress(
                c"resetting unlogged relations (cleanup), elapsed time: %ld.%02d s, current path: %s"
                    .as_ptr(),
                dbspace_path.as_ptr(),
            );
        }

        ResetUnloggedRelationsInDbspaceDir(dbspace_path.as_ptr(), op);
    }

    FreeDir(ts_dir);
}

/*
 * Process one per-dbspace directory for ResetUnloggedRelations
 */
unsafe fn ResetUnloggedRelationsInDbspaceDir(dbspacedirname: *const c_char, op: c_int) {
    let mut dbspace_dir: *mut DIR;
    let mut de: *mut dirent;
    let mut rm_path: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];

    /* Caller must specify at least one operation. */
    Assert!((op & (UNLOGGED_RELATION_CLEANUP | UNLOGGED_RELATION_INIT)) != 0);

    /*
     * Cleanup is a two-pass operation.  First, we go through and identify all
     * the files with init forks.  Then, we go through again and nuke
     * everything with the same OID except the init fork.
     */
    if (op & UNLOGGED_RELATION_CLEANUP) != 0 {
        let hash: *mut HTAB;
        let mut ctl: HASHCTL = std::mem::zeroed();

        /*
         * It's possible that someone could create a ton of unlogged relations
         * in the same database & tablespace, so we'd better use a hash table
         * rather than an array or linked list to keep track of which files
         * need to be reset.  Otherwise, this cleanup operation would be
         * O(n^2).
         */
        ctl.keysize = std::mem::size_of::<Oid>();
        ctl.entrysize = std::mem::size_of::<unlogged_relation_entry>();
        ctl.hcxt = CurrentMemoryContext;
        hash = hash_create(
            c"unlogged relation OIDs".as_ptr(),
            32,
            &ctl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
        );

        /* Scan the directory. */
        dbspace_dir = AllocateDir(dbspacedirname);
        loop {
            de = ReadDir(dbspace_dir, dbspacedirname);
            if de.is_null() {
                break;
            }

            let mut forkNum: ForkNumber = 0;
            let mut segno: c_uint = 0;
            let mut ent: unlogged_relation_entry = unlogged_relation_entry { relnumber: 0 };

            /* Skip anything that doesn't look like a relation data file. */
            if !parse_filename_for_nontemp_relation(
                dirent_d_name(de),
                &mut ent.relnumber,
                &mut forkNum,
                &mut segno,
            ) {
                continue;
            }

            /* Also skip it unless this is the init fork. */
            if forkNum != INIT_FORKNUM {
                continue;
            }

            /*
             * Put the RelFileNumber into the hash table, if it isn't already.
             */
            hash_search(
                hash,
                &ent as *const _ as *const c_void,
                HASHACTION::HASH_ENTER,
                null_mut(),
            );
        }

        /* Done with the first pass. */
        FreeDir(dbspace_dir);

        /*
         * If we didn't find any init forks, there's no point in continuing;
         * we can bail out now.
         */
        if hash_get_num_entries(hash) == 0 {
            hash_destroy(hash);
            return;
        }

        /*
         * Now, make a second pass and remove anything that matches.
         */
        dbspace_dir = AllocateDir(dbspacedirname);
        loop {
            de = ReadDir(dbspace_dir, dbspacedirname);
            if de.is_null() {
                break;
            }

            let mut forkNum: ForkNumber = 0;
            let mut segno: c_uint = 0;
            let mut ent: unlogged_relation_entry = unlogged_relation_entry { relnumber: 0 };

            /* Skip anything that doesn't look like a relation data file. */
            if !parse_filename_for_nontemp_relation(
                dirent_d_name(de),
                &mut ent.relnumber,
                &mut forkNum,
                &mut segno,
            ) {
                continue;
            }

            /* We never remove the init fork. */
            if forkNum == INIT_FORKNUM {
                continue;
            }

            /*
             * See whether the OID portion of the name shows up in the hash
             * table.  If so, nuke it!
             */
            if !hash_search(
                hash,
                &ent as *const _ as *const c_void,
                HASHACTION::HASH_FIND,
                null_mut(),
            )
            .is_null()
            {
                snprintf(
                    rm_path.as_mut_ptr(),
                    std::mem::size_of_val(&rm_path),
                    c"%s/%s".as_ptr(),
                    dbspacedirname,
                    dirent_d_name(de),
                );
                if unlink(rm_path.as_ptr()) < 0 {
                    let _ = errcode_for_file_access();
                    elog!(ERROR, "could not remove file \"{}\": %m", cstr(rm_path.as_ptr()));
                } else {
                    elog!(DEBUG2, "unlinked file \"{}\"", cstr(rm_path.as_ptr()));
                }
            }
        }

        /* Cleanup is complete. */
        FreeDir(dbspace_dir);
        hash_destroy(hash);
    }

    /*
     * Initialization happens after cleanup is complete: we copy each init
     * fork file to the corresponding main fork file.  Note that if we are
     * asked to do both cleanup and init, we may never get here: if the
     * cleanup code determines that there are no init forks in this dbspace,
     * it will return before we get to this point.
     */
    if (op & UNLOGGED_RELATION_INIT) != 0 {
        /* Scan the directory. */
        dbspace_dir = AllocateDir(dbspacedirname);
        loop {
            de = ReadDir(dbspace_dir, dbspacedirname);
            if de.is_null() {
                break;
            }

            let mut forkNum: ForkNumber = 0;
            let mut relNumber: RelFileNumber = 0;
            let mut segno: c_uint = 0;
            let mut srcpath: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
            let mut dstpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

            /* Skip anything that doesn't look like a relation data file. */
            if !parse_filename_for_nontemp_relation(
                dirent_d_name(de),
                &mut relNumber,
                &mut forkNum,
                &mut segno,
            ) {
                continue;
            }

            /* Also skip it unless this is the init fork. */
            if forkNum != INIT_FORKNUM {
                continue;
            }

            /* Construct source pathname. */
            snprintf(
                srcpath.as_mut_ptr(),
                std::mem::size_of_val(&srcpath),
                c"%s/%s".as_ptr(),
                dbspacedirname,
                dirent_d_name(de),
            );

            /* Construct destination pathname. */
            if segno == 0 {
                snprintf(
                    dstpath.as_mut_ptr(),
                    std::mem::size_of_val(&dstpath),
                    c"%s/%u".as_ptr(),
                    dbspacedirname,
                    relNumber as c_uint,
                );
            } else {
                snprintf(
                    dstpath.as_mut_ptr(),
                    std::mem::size_of_val(&dstpath),
                    c"%s/%u.%u".as_ptr(),
                    dbspacedirname,
                    relNumber as c_uint,
                    segno,
                );
            }

            /* OK, we're ready to perform the actual copy. */
            elog!(
                DEBUG2,
                "copying {} to {}",
                cstr(srcpath.as_ptr()),
                cstr(dstpath.as_ptr())
            );
            copy_file(srcpath.as_ptr(), dstpath.as_ptr());
        }

        FreeDir(dbspace_dir);

        /*
         * copy_file() above has already called pg_flush_data() on the files
         * it created. Now we need to fsync those files, because a checkpoint
         * won't do it for us while we're in recovery. We do this in a
         * separate pass to allow the kernel to perform all the flushes
         * (especially the metadata ones) at once.
         */
        dbspace_dir = AllocateDir(dbspacedirname);
        loop {
            de = ReadDir(dbspace_dir, dbspacedirname);
            if de.is_null() {
                break;
            }

            let mut relNumber: RelFileNumber = 0;
            let mut forkNum: ForkNumber = 0;
            let mut segno: c_uint = 0;
            let mut mainpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

            /* Skip anything that doesn't look like a relation data file. */
            if !parse_filename_for_nontemp_relation(
                dirent_d_name(de),
                &mut relNumber,
                &mut forkNum,
                &mut segno,
            ) {
                continue;
            }

            /* Also skip it unless this is the init fork. */
            if forkNum != INIT_FORKNUM {
                continue;
            }

            /* Construct main fork pathname. */
            if segno == 0 {
                snprintf(
                    mainpath.as_mut_ptr(),
                    std::mem::size_of_val(&mainpath),
                    c"%s/%u".as_ptr(),
                    dbspacedirname,
                    relNumber as c_uint,
                );
            } else {
                snprintf(
                    mainpath.as_mut_ptr(),
                    std::mem::size_of_val(&mainpath),
                    c"%s/%u.%u".as_ptr(),
                    dbspacedirname,
                    relNumber as c_uint,
                    segno,
                );
            }

            fsync_fname(mainpath.as_ptr(), false);
        }

        FreeDir(dbspace_dir);

        /*
         * Lastly, fsync the database directory itself, ensuring the
         * filesystem remembers the file creations and deletions we've done.
         * We don't bother with this during a call that does only
         * UNLOGGED_RELATION_CLEANUP, because if recovery crashes before we
         * get to doing UNLOGGED_RELATION_INIT, we'll redo the cleanup step
         * too at the next startup attempt.
         */
        fsync_fname(dbspacedirname, true);
    }
}

/*
 * Basic parsing of putative relation filenames.
 *
 * This function returns true if the file appears to be in the correct format
 * for a non-temporary relation and false otherwise.
 *
 * If it returns true, it sets *relnumber, *fork, and *segno to the values
 * extracted from the filename. If it returns false, these values are set to
 * InvalidRelFileNumber, InvalidForkNumber, and 0, respectively.
 */
pub unsafe fn parse_filename_for_nontemp_relation(
    mut name: *const c_char,
    relnumber: *mut RelFileNumber,
    fork: *mut ForkNumber,
    segno: *mut c_uint,
) -> bool {
    let n: c_ulong;
    let s: c_ulong;
    let f: ForkNumber;
    let mut endp: *mut c_char = null_mut();

    *relnumber = InvalidRelFileNumber;
    *fork = InvalidForkNumber;
    *segno = 0;

    /*
     * Relation filenames should begin with a digit that is not a zero. By
     * rejecting cases involving leading zeroes, the caller can assume that
     * there's only one possible string of characters that could have produced
     * any given value for *relnumber.
     *
     * (To be clear, we don't expect files with names like 0017.3 to exist at
     * all -- but if 0017.3 does exist, it's a non-relation file, not part of
     * the main fork for relfilenode 17.)
     */
    if *name < b'1' as c_char || *name > b'9' as c_char {
        return false;
    }

    /*
     * Parse the leading digit string. If the value is out of range, we
     * conclude that this isn't a relation file at all.
     */
    set_errno(0);
    n = strtoul(name, &mut endp, 10);
    if errno() != 0 || name == endp as *const c_char || n == 0 || n > PG_UINT32_MAX {
        return false;
    }
    name = endp;

    /* Check for a fork name. */
    if *name != b'_' as c_char {
        f = MAIN_FORKNUM;
    } else {
        let forkchar: c_int;
        let mut ftmp: ForkNumber = 0;

        forkchar = forkname_chars(name.add(1), &mut ftmp);
        if forkchar <= 0 {
            return false;
        }
        f = ftmp;
        name = name.add(forkchar as usize + 1);
    }

    /* Check for a segment number. */
    if *name != b'.' as c_char {
        s = 0;
    } else {
        /* Reject leading zeroes, just like we do for RelFileNumber. */
        if *name.add(1) < b'1' as c_char || *name.add(1) > b'9' as c_char {
            return false;
        }

        set_errno(0);
        s = strtoul(name.add(1), &mut endp, 10);
        if errno() != 0
            || name.add(1) == endp as *const c_char
            || s == 0
            || s > PG_UINT32_MAX
        {
            return false;
        }
        name = endp;
    }

    /* Now we should be at the end. */
    if *name != b'\0' as c_char {
        return false;
    }

    /* Set out parameters and return. */
    *relnumber = n as RelFileNumber;
    *fork = f;
    *segno = s as c_uint;
    true
}
