//! basebackup_server.c - store basebackup archives on the server.
//!
//! Source: postgres/src/backend/backup/basebackup_server.c
//!
//! #include mapping:
//!   "postgres.h"                  -> use crate::prelude::*
//!   "access/xact.h"               -> StartTransactionCommand/CommitTransactionCommand (STUB local)
//!   "backup/basebackup_sink.h"    -> crate::backup::basebackup_sink (PORTED)
//!   "catalog/pg_authid.h"         -> ROLE_PG_WRITE_SERVER_FILES (crate::catalog::pg_known_oids, PORTED)
//!   "miscadmin.h"                 -> crate::miscadmin::GetUserId (PORTED, currently unimplemented body)
//!   "storage/fd.h"                -> File / off_t / VFD ops (STUB local; VFD layer not ported)
//!   "utils/acl.h"                 -> has_privs_of_role (STUB local; acl.c not ported)
//!   "utils/wait_event.h"          -> WAIT_EVENT_BASEBACKUP_* (STUB local consts)

use crate::prelude::*;

use crate::backup::basebackup_sink::{
    bbsink, bbsink_forward_archive_contents, bbsink_forward_begin_archive,
    bbsink_forward_begin_backup, bbsink_forward_begin_manifest, bbsink_forward_cleanup,
    bbsink_forward_end_archive, bbsink_forward_end_backup, bbsink_forward_end_manifest,
    bbsink_forward_manifest_contents, bbsink_ops,
};
use crate::catalog::pg_known_oids::ROLE_PG_WRITE_SERVER_FILES;
use crate::common::file_utils::durable_rename;
use crate::miscadmin::GetUserId;
use crate::port::pgcheckdir::pg_check_dir;
use crate::port::port_api::is_absolute_path;

// ---------------------------------------------------------------------------
// storage/fd.h types. The virtual file descriptor layer is not ported yet.
// walsummaryfuncs.rs uses the same local aliases; mirror them here.
// ---------------------------------------------------------------------------
pub type File = c_int;
pub type off_t = i64;

// O_* flags from <fcntl.h>. On the platforms PG targets these are the standard
// POSIX values; mirror them as plain constants for the open-flag argument.
const O_CREAT: c_int = 0o100;
const O_EXCL: c_int = 0o200;
const O_WRONLY: c_int = 0o1;

// utils/wait_event.h: wait-event identifiers. STUB: the wait-event subsystem
// isn't ported; these are passed opaquely to FileWrite/FileSync.
// TODO: import real WaitEventIO enum once wait_event is ported.
const WAIT_EVENT_BASEBACKUP_WRITE: uint32 = 0;
const WAIT_EVENT_BASEBACKUP_SYNC: uint32 = 0;

// ---------------------------------------------------------------------------
// Local stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

// utils/mmgr/mcxt.c: psprintf("%s/%s", dir, name). PepperDB has no central
// varargs psprintf yet; build "<dir>/<name>" into a palloc'd NUL-terminated
// buffer, matching the call sites here.
// TODO: import the real psprintf once ported.
unsafe fn psprintf_join(dir: *const c_char, name: *const c_char) -> *mut c_char {
    let dir_len = strlen_c(dir);
    let name_len = strlen_c(name);
    let total = dir_len + 1 + name_len + 1; // '/' + NUL
    let out = palloc(total) as *mut c_char;
    let p = out as *mut u8;
    core::ptr::copy_nonoverlapping(dir as *const u8, p, dir_len);
    *p.add(dir_len) = b'/';
    core::ptr::copy_nonoverlapping(name as *const u8, p.add(dir_len + 1), name_len);
    *p.add(dir_len + 1 + name_len) = 0;
    out
}

// strlen on a C string (length not counting the NUL).
unsafe fn strlen_c(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// utils/acl.c: role-membership check. STUB: acl.c not ported.
// TODO: import has_privs_of_role once acl.c is ported.
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

// access/xact.c: transaction control. STUB: xact.c not ported.
// TODO: import StartTransactionCommand/CommitTransactionCommand once xact.c is ported.
unsafe fn StartTransactionCommand() {
    unimplemented!()
}
unsafe fn CommitTransactionCommand() {
    unimplemented!()
}

// storage/fd.c: create the data directory (or a subdirectory) with the proper
// permissions. STUB: fd.c not ported.
// TODO: import MakePGDirectory once fd.c is ported.
unsafe fn MakePGDirectory(_directoryName: *const c_char) -> c_int {
    unimplemented!()
}

// storage/fd.c: virtual file descriptor operations. STUB: the VFD layer (fd.c)
// is not ported yet.
// TODO: import these once fd.c is ported.
unsafe fn PathNameOpenFile(_fileName: *const c_char, _fileFlags: c_int) -> File {
    unimplemented!()
}
unsafe fn FileWrite(
    _file: File,
    _buffer: *const c_char,
    _amount: Size,
    _offset: off_t,
    _wait_event_info: uint32,
) -> c_int {
    unimplemented!()
}
unsafe fn FileSync(_file: File, _wait_event_info: uint32) -> c_int {
    unimplemented!()
}
unsafe fn FileClose(_file: File) {
    unimplemented!()
}
unsafe fn FilePathName(_file: File) -> *mut c_char {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// bbsink_server: a sink that stores backup archives on the server.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct bbsink_server {
    /// Common information for all types of sink.
    pub base: bbsink,

    /// Directory in which backup is to be stored.
    pub pathname: *mut c_char,

    /// Currently open file (or 0 if nothing open).
    pub file: File,

    /// Current file position.
    pub filepos: off_t,
}

static bbsink_server_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_forward_begin_backup),
    begin_archive: Some(bbsink_server_begin_archive),
    archive_contents: Some(bbsink_server_archive_contents),
    end_archive: Some(bbsink_server_end_archive),
    begin_manifest: Some(bbsink_server_begin_manifest),
    manifest_contents: Some(bbsink_server_manifest_contents),
    end_manifest: Some(bbsink_server_end_manifest),
    end_backup: Some(bbsink_forward_end_backup),
    cleanup: Some(bbsink_forward_cleanup),
};

/// Create a new 'server' bbsink.
pub unsafe fn bbsink_server_new(next: *mut bbsink, pathname: *mut c_char) -> *mut bbsink {
    let sink = palloc0(core::mem::size_of::<bbsink_server>()) as *mut bbsink_server;

    (*sink).base.bbs_ops = &bbsink_server_ops;
    (*sink).pathname = pathname;
    (*sink).base.bbs_next = next;

    /* Replication permission is not sufficient in this case. */
    StartTransactionCommand();
    if !has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES) {
        ereport!(
            ERROR,
            "permission denied to create backup stored on server"
        );
    }
    CommitTransactionCommand();

    /*
     * It's not a good idea to store your backups in the same directory that
     * you're backing up. If we allowed a relative path here, that could
     * easily happen accidentally, so we don't. The user could still
     * accomplish the same thing by including the absolute path to $PGDATA in
     * the pathname, but that's likely an intentional bad decision rather than
     * an accident.
     */
    if !is_absolute_path(pathname) {
        ereport!(
            ERROR,
            "relative path not allowed for backup stored on server"
        );
    }

    match pg_check_dir(pathname) {
        0 => {
            /*
             * Does not exist, so create it using the same permissions we'd
             * use for a new subdirectory of the data directory itself.
             */
            if MakePGDirectory(pathname) < 0 {
                elog!(ERROR, "could not create directory \"{}\": %m", cstr_to_str(pathname));
            }
        }
        1 => {
            /* Exists, empty. */
        }
        2 | 3 | 4 => {
            /* Exists, not empty. */
            elog!(ERROR, "directory \"{}\" exists but is not empty", cstr_to_str(pathname));
        }
        _ => {
            /* Access problem. */
            elog!(ERROR, "could not access directory \"{}\": %m", cstr_to_str(pathname));
        }
    }

    &raw mut (*sink).base
}

/// Open the correct output file for this archive.
unsafe fn bbsink_server_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    let mysink = sink as *mut bbsink_server;

    Assert!((*mysink).file == 0);
    Assert!((*mysink).filepos == 0);

    let filename = psprintf_join((*mysink).pathname, archive_name);

    (*mysink).file = PathNameOpenFile(filename, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    if (*mysink).file <= 0 {
        elog!(ERROR, "could not create file \"{}\": %m", cstr_to_str(filename));
    }

    pfree(filename as *mut c_void);

    bbsink_forward_begin_archive(sink, archive_name);
}

/// Write the data to the output file.
unsafe fn bbsink_server_archive_contents(sink: *mut bbsink, len: Size) {
    let mysink = sink as *mut bbsink_server;

    let nbytes = FileWrite(
        (*mysink).file,
        (*mysink).base.bbs_buffer,
        len,
        (*mysink).filepos,
        WAIT_EVENT_BASEBACKUP_WRITE,
    );

    if nbytes as Size != len {
        if nbytes < 0 {
            elog!(
                ERROR,
                "could not write file \"{}\": %m",
                cstr_to_str(FilePathName((*mysink).file))
            );
        }
        /* short write: complain appropriately */
        elog!(
            ERROR,
            "could not write file \"{}\": wrote only {} of {} bytes at offset {}",
            cstr_to_str(FilePathName((*mysink).file)),
            nbytes,
            len as c_int,
            (*mysink).filepos as u32
        );
    }

    (*mysink).filepos += nbytes as off_t;

    bbsink_forward_archive_contents(sink, len);
}

/// fsync and close the current output file.
unsafe fn bbsink_server_end_archive(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_server;

    /*
     * We intentionally don't use data_sync_elevel here, because the server
     * shouldn't PANIC just because we can't guarantee that the backup has
     * been written down to disk. Running recovery won't fix anything in this
     * case anyway.
     */
    if FileSync((*mysink).file, WAIT_EVENT_BASEBACKUP_SYNC) < 0 {
        elog!(
            ERROR,
            "could not fsync file \"{}\": %m",
            cstr_to_str(FilePathName((*mysink).file))
        );
    }

    /* We're done with this file now. */
    FileClose((*mysink).file);
    (*mysink).file = 0;
    (*mysink).filepos = 0;

    bbsink_forward_end_archive(sink);
}

/// Open the output file to which we will write the manifest.
///
/// Just like pg_basebackup, we write the manifest first under a temporary
/// name and then rename it into place after fsync. That way, if the manifest
/// is there and under the correct name, the user can be sure that the backup
/// completed.
unsafe fn bbsink_server_begin_manifest(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_server;

    Assert!((*mysink).file == 0);

    let tmp_filename = psprintf_join((*mysink).pathname, c"backup_manifest.tmp".as_ptr());

    (*mysink).file = PathNameOpenFile(tmp_filename, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    if (*mysink).file <= 0 {
        elog!(ERROR, "could not create file \"{}\": %m", cstr_to_str(tmp_filename));
    }

    pfree(tmp_filename as *mut c_void);

    bbsink_forward_begin_manifest(sink);
}

/// Each chunk of manifest data is sent using a CopyData message.
unsafe fn bbsink_server_manifest_contents(sink: *mut bbsink, len: Size) {
    let mysink = sink as *mut bbsink_server;

    let nbytes = FileWrite(
        (*mysink).file,
        (*mysink).base.bbs_buffer,
        len,
        (*mysink).filepos,
        WAIT_EVENT_BASEBACKUP_WRITE,
    );

    if nbytes as Size != len {
        if nbytes < 0 {
            elog!(
                ERROR,
                "could not write file \"{}\": %m",
                cstr_to_str(FilePathName((*mysink).file))
            );
        }
        /* short write: complain appropriately */
        elog!(
            ERROR,
            "could not write file \"{}\": wrote only {} of {} bytes at offset {}",
            cstr_to_str(FilePathName((*mysink).file)),
            nbytes,
            len as c_int,
            (*mysink).filepos as u32
        );
    }

    (*mysink).filepos += nbytes as off_t;

    bbsink_forward_manifest_contents(sink, len);
}

/// fsync the backup manifest, close the file, and then rename it into place.
unsafe fn bbsink_server_end_manifest(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_server;

    /* We're done with this file now. */
    FileClose((*mysink).file);
    (*mysink).file = 0;

    /*
     * Rename it into place. This also fsyncs the temporary file, so we don't
     * need to do that here. We don't use data_sync_elevel here for the same
     * reasons as in bbsink_server_end_archive.
     */
    let tmp_filename = psprintf_join((*mysink).pathname, c"backup_manifest.tmp".as_ptr());
    let filename = psprintf_join((*mysink).pathname, c"backup_manifest".as_ptr());
    durable_rename(tmp_filename, filename);
    pfree(filename as *mut c_void);
    pfree(tmp_filename as *mut c_void);

    bbsink_forward_end_manifest(sink);
}

// Helper: render a NUL-terminated C string as a Rust &str for use in elog!
// runtime-arg messages (lossy on invalid UTF-8). Local helper, not from C.
unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "(null)";
    }
    let len = strlen_c(s);
    match core::str::from_utf8(core::slice::from_raw_parts(s as *const u8, len)) {
        Ok(v) => v,
        Err(_) => "(invalid)",
    }
}
