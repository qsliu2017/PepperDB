//! storage/file/fileset.c - Management of named temporary files.
//!
//! FileSets provide a temporary namespace (think directory) so that files can
//! be discovered by name.
//!
//! FileSets can be used by backends when the temporary files need to be
//! opened/closed multiple times and the underlying files need to survive across
//! transactions.

use crate::prelude::*;

use crate::lengthof;

use crate::common::hashfn::hash_any;
use crate::miscadmin::{MyDatabaseTableSpace, MyProcPid};
use crate::pg_config_manual::MAXPGPATH;
use crate::postgres_ext::{InvalidOid, Oid};

use core::ffi::{c_char, c_int};

// INT_MAX from <limits.h>.
const INT_MAX: uint32 = c_int::MAX as uint32;

// storage/fd.h - File is an index into the virtual file descriptor table.
pub type File = c_int;

// common/file_utils.h - PG_TEMP_FILE_PREFIX. Kept as a C string literal here so
// it can be spliced into snprintf format strings.
const PG_TEMP_FILE_PREFIX: &[u8] = b"pgsql_tmp\0";

/*
 * storage/fileset.h - A set of temporary files.
 */
#[repr(C)]
pub struct FileSet {
    pub creator_pid: crate::miscadmin::pid_t, /* PID of the creating process */
    pub number: uint32,                       /* per-PID identifier */
    pub ntablespaces: c_int,                  /* number of tablespaces to use */
    pub tablespaces: [Oid; 8], /* OIDs of tablespaces to use. Assumes that it's
                                * rare that there more than temp tablespaces. */
}

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

// -------------------------------------------------------------------------
// STUBBED dependencies (not yet ported).
// -------------------------------------------------------------------------

// commands/tablespace.h
// TODO: port commands/tablespace.c
unsafe fn PrepareTempTablespaces() {
    unimplemented!()
}
// TODO: port commands/tablespace.c
unsafe fn GetTempTablespaces(_tableSpaces: *mut Oid, _numSpaces: c_int) -> c_int {
    unimplemented!()
}
// TODO: port commands/tablespace.c
unsafe fn TempTablespacePath(_path: *mut c_char, _tablespace: Oid) {
    unimplemented!()
}

// storage/fd.h
// TODO: port storage/file/fd.c
unsafe fn PathNameCreateTemporaryFile(_path: *const c_char, _error_on_failure: bool) -> File {
    unimplemented!()
}
// TODO: port storage/file/fd.c
unsafe fn PathNameOpenTemporaryFile(_path: *const c_char, _mode: c_int) -> File {
    unimplemented!()
}
// TODO: port storage/file/fd.c
unsafe fn PathNameDeleteTemporaryFile(_path: *const c_char, _error_on_failure: bool) -> bool {
    unimplemented!()
}
// TODO: port storage/file/fd.c
unsafe fn PathNameCreateTemporaryDir(_basedir: *const c_char, _directory: *const c_char) {
    unimplemented!()
}
// TODO: port storage/file/fd.c
unsafe fn PathNameDeleteTemporaryDir(_dirname: *const c_char) {
    unimplemented!()
}

/*
 * Initialize a space for temporary files. This API can be used by shared
 * fileset as well as if the temporary files are used only by single backend
 * but the files need to be opened and closed multiple times and also the
 * underlying files need to survive across transactions.
 *
 * The callers are expected to explicitly remove such files by using
 * FileSetDelete/FileSetDeleteAll.
 *
 * Files will be distributed over the tablespaces configured in
 * temp_tablespaces.
 *
 * Under the covers the set is one or more directories which will eventually
 * be deleted.
 */
#[no_mangle]
pub unsafe fn FileSetInit(fileset: *mut FileSet) {
    static mut counter: uint32 = 0;

    (*fileset).creator_pid = MyProcPid;
    (*fileset).number = counter;
    counter = (counter + 1) % INT_MAX;

    /* Capture the tablespace OIDs so that all backends agree on them. */
    PrepareTempTablespaces();
    (*fileset).ntablespaces = GetTempTablespaces(
        &mut (*fileset).tablespaces[0],
        lengthof!((*fileset).tablespaces) as c_int,
    );
    if (*fileset).ntablespaces == 0 {
        /* If the GUC is empty, use current database's default tablespace */
        (*fileset).tablespaces[0] = MyDatabaseTableSpace;
        (*fileset).ntablespaces = 1;
    } else {
        /*
         * An entry of InvalidOid means use the default tablespace for the
         * current database.  Replace that now, to be sure that all users of
         * the FileSet agree on what to do.
         */
        let mut i: c_int = 0;
        while i < (*fileset).ntablespaces {
            if (*fileset).tablespaces[i as usize] == InvalidOid {
                (*fileset).tablespaces[i as usize] = MyDatabaseTableSpace;
            }
            i += 1;
        }
    }
}

/*
 * Create a new file in the given set.
 */
pub unsafe fn FileSetCreate(fileset: *mut FileSet, name: *const c_char) -> File {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut file: File;

    FilePath(path.as_mut_ptr(), fileset, name);
    file = PathNameCreateTemporaryFile(path.as_ptr(), false);

    /* If we failed, see if we need to create the directory on demand. */
    if file <= 0 {
        let mut tempdirpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let mut filesetpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let tablespace: Oid = ChooseTablespace(fileset, name);

        TempTablespacePath(tempdirpath.as_mut_ptr(), tablespace);
        FileSetPath(filesetpath.as_mut_ptr(), fileset, tablespace);
        PathNameCreateTemporaryDir(tempdirpath.as_ptr(), filesetpath.as_ptr());
        file = PathNameCreateTemporaryFile(path.as_ptr(), true);
    }

    file
}

/*
 * Open a file that was created with FileSetCreate()
 */
pub unsafe fn FileSetOpen(fileset: *mut FileSet, name: *const c_char, mode: c_int) -> File {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let file: File;

    FilePath(path.as_mut_ptr(), fileset, name);
    file = PathNameOpenTemporaryFile(path.as_ptr(), mode);

    file
}

/*
 * Delete a file that was created with FileSetCreate().
 *
 * Return true if the file existed, false if didn't.
 */
pub unsafe fn FileSetDelete(
    fileset: *mut FileSet,
    name: *const c_char,
    error_on_failure: bool,
) -> bool {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    FilePath(path.as_mut_ptr(), fileset, name);

    PathNameDeleteTemporaryFile(path.as_ptr(), error_on_failure)
}

/*
 * Delete all files in the set.
 */
pub unsafe fn FileSetDeleteAll(fileset: *mut FileSet) {
    let mut dirpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /*
     * Delete the directory we created in each tablespace.  Doesn't fail
     * because we use this in error cleanup paths, but can generate LOG
     * message on IO error.
     */
    let mut i: c_int = 0;
    while i < (*fileset).ntablespaces {
        FileSetPath(dirpath.as_mut_ptr(), fileset, (*fileset).tablespaces[i as usize]);
        PathNameDeleteTemporaryDir(dirpath.as_ptr());
        i += 1;
    }
}

/*
 * Build the path for the directory holding the files backing a FileSet in a
 * given tablespace.
 */
unsafe fn FileSetPath(path: *mut c_char, fileset: *mut FileSet, tablespace: Oid) {
    let mut tempdirpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    TempTablespacePath(tempdirpath.as_mut_ptr(), tablespace);
    snprintf(
        path,
        MAXPGPATH,
        b"%s/%s%lu.%u.fileset\0".as_ptr() as *const c_char,
        tempdirpath.as_ptr(),
        PG_TEMP_FILE_PREFIX.as_ptr() as *const c_char,
        (*fileset).creator_pid as core::ffi::c_ulong,
        (*fileset).number,
    );
}

/*
 * Sorting has to determine which tablespace a given temporary file belongs in.
 */
unsafe fn ChooseTablespace(fileset: *const FileSet, name: *const c_char) -> Oid {
    let hash: uint32 =
        hash_any(name as *const core::ffi::c_uchar, strlen(name) as c_int) as uint32;

    (*fileset).tablespaces[(hash % (*fileset).ntablespaces as uint32) as usize]
}

/*
 * Compute the full path of a file in a FileSet.
 */
unsafe fn FilePath(path: *mut c_char, fileset: *mut FileSet, name: *const c_char) {
    let mut dirpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    FileSetPath(dirpath.as_mut_ptr(), fileset, ChooseTablespace(fileset, name));
    snprintf(
        path,
        MAXPGPATH,
        b"%s/%s\0".as_ptr() as *const c_char,
        dirpath.as_ptr(),
        name,
    );
}
