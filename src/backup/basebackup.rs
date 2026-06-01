//! Code for taking a base backup and streaming it to a standby.
//!
//! Source: postgres/src/backend/backup/basebackup.c
//!
//! #include mapping:
//!   "postgres.h"                          -> use crate::prelude::*
//!   "access/xlog_internal.h"              -> crate::access::transam::xlog_internal
//!   "access/xlogbackup.h"                 -> crate::access::transam::xlogbackup
//!   "backup/backup_manifest.h"            -> crate::backup::backup_manifest
//!   "backup/basebackup.h"                 -> merged here (basebackup_options, prototypes)
//!   "backup/basebackup_incremental.h"     -> crate::backup::basebackup_incremental
//!   "backup/basebackup_sink.h"            -> crate::backup::basebackup_sink
//!   "backup/basebackup_target.h"          -> crate::backup::basebackup_target
//!   "catalog/pg_tablespace_d.h"           -> crate::catalog::pg_known_oids
//!   "commands/defrem.h"                   -> defGet* helpers (STUB below)
//!   "common/compression.h"                -> crate::common::compression
//!   "common/file_perm.h"                  -> crate::common::file_perm
//!   "common/file_utils.h"                 -> (not directly used here)
//!   "lib/stringinfo.h"                    -> crate::lib::stringinfo
//!   "miscadmin.h"                         -> crate::miscadmin
//!   "nodes/pg_list.h"                     -> crate::nodes::pg_list
//!   "pgstat.h"                            -> pgstat_* (homes below)
//!   "pgtar.h"                             -> crate::pgtar
//!   "postmaster/syslogger.h"              -> LOG_METAINFO_DATAFILE_TMP (STUB)
//!   "postmaster/walsummarizer.h"          -> crate::postmaster::walsummarizer
//!   "replication/walsender.h"             -> crate::replication::walsender
//!   "replication/walsender_private.h"     -> crate::replication::walsender_private
//!   "storage/bufpage.h"                   -> crate::storage::bufpage
//!   "storage/checksum.h"                  -> crate::storage::checksum
//!   "storage/reinit.h"                    -> crate::storage::file::reinit
//!   "utils/relcache.h"                    -> RELCACHE_INIT_FILENAME (STUB)
//!   "utils/resowner.h"                    -> crate::utils::resowner::resowner

use crate::prelude::*;

use core::ffi::{c_longlong, CStr};

use crate::access::transam::xlog::{
    do_pg_backup_start, do_pg_backup_stop, get_backup_status, wal_segment_size, CheckXLogRemoved,
    DataChecksumsEnabled, RecoveryInProgress, SessionBackupState, SESSION_BACKUP_RUNNING,
};
use crate::access::transam::xlog::do_pg_abort_backup;
use crate::access::transam::xlogbackup::{build_backup_content, BackupState};
use crate::access::transam::xlog_internal::{
    IsTLHistoryFileName, IsXLogFileName, XLByteToPrevSeg, XLByteToSeg, XLogFileName,
    XLogFromFileName, StatusFilePath, MAXFNAMELEN, XLOG_CONTROL_FILE, XLOGDIR,
};
use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::backup::backup_manifest::{
    backup_manifest_info, backup_manifest_option, AddFileToBackupManifest,
    AddWALInfoToBackupManifest, FreeBackupManifest, InitializeBackupManifest, SendBackupManifest,
    MANIFEST_OPTION_FORCE_ENCODE, MANIFEST_OPTION_NO, MANIFEST_OPTION_YES,
};
use crate::backup::basebackup_incremental::{
    FileBackupMethod, GetFileBackupMethod, GetIncrementalFileSize, IncrementalBackupInfo,
    PrepareForIncrementalBackup, BACK_UP_FILE_FULLY, BACK_UP_FILE_INCREMENTALLY, INCREMENTAL_MAGIC,
};
use crate::backup::basebackup_copy::{bbsink_copystream_new, tablespaceinfo};
use crate::backup::basebackup_gzip::bbsink_gzip_new;
use crate::backup::basebackup_lz4::bbsink_lz4_new;
use crate::backup::basebackup_progress::{
    basebackup_progress_done, basebackup_progress_estimate_backup_size,
    basebackup_progress_transfer_wal, basebackup_progress_wait_checkpoint,
    basebackup_progress_wait_wal_archive, bbsink_progress_new,
};
use crate::backup::basebackup_sink::{
    bbsink, bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_cleanup,
    bbsink_end_archive, bbsink_end_backup, bbsink_state,
};
use crate::backup::basebackup_target::{
    BaseBackupGetSink, BaseBackupGetTargetHandle, BaseBackupTargetHandle,
};
use crate::backup::basebackup_throttle::bbsink_throttle_new;
use crate::backup::basebackup_zstd::bbsink_zstd_new;
use crate::catalog::pg_known_oids::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID};
use crate::common::checksum_helper::{
    pg_checksum_context, pg_checksum_init, pg_checksum_parse_type, pg_checksum_type,
    pg_checksum_update, CHECKSUM_TYPE_CRC32C, CHECKSUM_TYPE_NONE,
};
use crate::common::compression::{
    parse_compress_algorithm, parse_compress_specification, pg_compress_algorithm,
    pg_compress_specification, validate_compress_specification, PG_COMPRESSION_GZIP,
    PG_COMPRESSION_LZ4, PG_COMPRESSION_NONE, PG_COMPRESSION_ZSTD,
};
use crate::common::file_perm::{pg_dir_create_mode, pg_file_create_mode};
use crate::common::relpath::{
    ForkNumber, RelFileNumber, INIT_FORKNUM, InvalidForkNumber,
};
use crate::lib::stringinfo::{destroyStringInfo, makeStringInfo, StringInfo, StringInfoData};
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{list_sort, lappend, linitial, List, ListCell, NIL};
use crate::nodes::replnodes::BaseBackupCmd;
use crate::pg_config::{BLCKSZ, RELSEG_SIZE};
use crate::pg_config_manual::MAXPGPATH;
use crate::pgtar::{
    tarCreateHeader, tarError, tarPaddingBytesRequired, TAR_BLOCK_SIZE, TAR_NAME_TOO_LONG, TAR_OK,
    TAR_SYMLINK_TOO_LONG,
};
use crate::postgres_ext::{atooid, InvalidOid, Oid};
use crate::postmaster::walsummarizer::summarize_wal;
use crate::replication::walsender_private::{WalSndSetState, WALSNDSTATE_BACKUP};
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, PageGetLSN, PageHeader, PageIsNew};
use crate::storage::checksum::pg_checksum_page;
use crate::storage::file::fd::{
    dirent, AllocateDir, CloseTransientFile, FreeDir, OpenTransientFile, ReadDir,
    looks_like_temp_rel_name, DIR,
};
use crate::storage::file::reinit::parse_filename_for_nontemp_relation;
use crate::utils::activity::pgstat_database::{
    pgstat_prepare_report_checksum_failure, pgstat_report_checksum_failures_in_db,
};
use crate::{current_cell, ereport, errmsg, foreach};

// ===========================================================================
// Stubs for as-yet-unported dependencies (functions defined in OTHER .c files).
// ===========================================================================

// commands/defrem.h: option-value extraction helpers (defGetString lives in
// commands/define.c, not yet wired here). TODO: import from crate::commands::define.
unsafe fn defGetString(def: *mut DefElem) -> *mut c_char {
    let _ = def;
    unimplemented!("defGetString: commands/define.c not yet wired into basebackup");
}
unsafe fn defGetBoolean(def: *mut DefElem) -> bool {
    let _ = def;
    unimplemented!("defGetBoolean: commands/define.c not yet wired into basebackup");
}
unsafe fn defGetInt64(def: *mut DefElem) -> int64 {
    let _ = def;
    unimplemented!("defGetInt64: commands/define.c not yet wired into basebackup");
}

// utils/relcache.h: cache-init file name. STUB (real value from relcache.h).
const RELCACHE_INIT_FILENAME: &CStr = c"pg_internal.init";

// guc.h: PG_AUTOCONF_FILENAME. STUB.
const PG_AUTOCONF_FILENAME: &CStr = c"postgresql.auto.conf";

// postmaster/syslogger.h: current-logfile metainfo temp file. STUB.
const LOG_METAINFO_DATAFILE_TMP: &CStr = c"current_logfiles.tmp";

// xlog backup label / tablespace map file names (xlog_internal.h / xlogbackup.h). STUB.
const BACKUP_LABEL_FILE: &CStr = c"backup_label";
const TABLESPACE_MAP: &CStr = c"tablespace_map";

// catalog/pg_tablespace_d.h: relative path of the version-specific directory
// inside a tablespace, and the pg_tblspc directory itself. STUB: build-generated
// in C; mirror the values used elsewhere in the crate (see common/relpath.rs).
const TABLESPACE_VERSION_DIRECTORY: &CStr = c"PG_18_202504291";
const PG_TBLSPC_DIR: &CStr = c"pg_tblspc";

// common/relpath.h: PG_TEMP_FILE_PREFIX. STUB.
const PG_TEMP_FILE_PREFIX: &CStr = c"pgsql_tmp";

// storage/fd.h-adjacent directory names removed/recreated at startup. STUB.
const PG_STAT_TMP_DIR: &CStr = c"pg_stat_tmp";
const PG_REPLSLOT_DIR: &CStr = c"pg_replslot";
const PG_DYNSHMEM_DIR: &CStr = c"pg_dynshmem";

// replication/basebackup.h: clamping range for the max_rate option. STUB.
const MAX_RATE_LOWER: c_int = 32;
const MAX_RATE_UPPER: c_int = 1048576;

// resowner.h / proc.c: the auxiliary-process resource owner and the global
// current resource owner, plus the release helper. STUB (resowner internals and
// AuxProcessResourceOwner are not yet wired into this unit).
static mut AuxProcessResourceOwner: *mut c_void = null_mut();
static mut CurrentResourceOwner: *mut c_void = null_mut();
unsafe fn ReleaseAuxProcessResources(isCommit: bool) {
    let _ = isCommit;
    // TODO(pg-port): utils/resowner/resowner.c -- ReleaseAuxProcessResources.
}

// miscadmin.h: CHECK_FOR_INTERRUPTS. STUB (interrupt machinery not yet wired).
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        // TODO(pg-port): miscadmin.h -- CHECK_FOR_INTERRUPTS().
    }};
}
use CHECK_FOR_INTERRUPTS;

// utils/ps_status.h: process-title controls. STUB.
static mut update_process_title: bool = true;
unsafe fn set_ps_display(activity: *const c_char) {
    let _ = activity;
    // TODO(pg-port): utils/misc/ps_status.c -- set_ps_display.
}

// pgstat.h: wait-event reporting around file reads. STUB (no-op).
const WAIT_EVENT_BASEBACKUP_READ: uint32 = 0;
unsafe fn pgstat_report_wait_start(wait_event_info: uint32) {
    let _ = wait_event_info;
    // TODO(pg-port): utils/activity/wait_event.c -- pgstat_report_wait_start.
}
unsafe fn pgstat_report_wait_end() {
    // TODO(pg-port): utils/activity/wait_event.c -- pgstat_report_wait_end.
}

// PG_ENSURE_ERROR_CLEANUP / PG_TRY: faithful longjmp-based cleanup is not yet
// available at the crate root, so model them as plain block execution (matching
// other translated units such as commands/matview.rs). TODO: utils/elog.h.
macro_rules! PG_ENSURE_ERROR_CLEANUP {
    ($body:block) => {{
        $body
    }};
}
use PG_ENSURE_ERROR_CLEANUP;

macro_rules! PG_TRY {
    ($try_block:block, $finally_block:block) => {{
        $try_block
        $finally_block
    }};
}
use PG_TRY;

// psprintf: variadic in C. The two call sites here use simple %u/%s formats, so
// provide a tiny palloc-backed formatter rather than a faithful printf. The
// resulting C string is NUL-terminated and owned by the current memory context.
unsafe fn psprintf_cstr(s: &str) -> *mut c_char {
    let bytes = s.as_bytes();
    let buf = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, bytes.len());
    *buf.add(bytes.len()) = 0;
    buf
}

// snprintf into a fixed C buffer using a Rust-formatted string. Truncates to fit
// and always NUL-terminates, mirroring the bounded snprintf calls in the source.
unsafe fn snprintf_into(buf: *mut c_char, cap: usize, s: &str) {
    if cap == 0 {
        return;
    }
    let bytes = s.as_bytes();
    let n = core::cmp::min(bytes.len(), cap - 1);
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, n);
    *buf.add(n) = 0;
}

// Render a NUL-terminated C string pointer for use with Rust formatting.
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    CStr::from_ptr(p).to_string_lossy().into_owned()
}

// ===========================================================================
// Local constants / types (basebackup.h merged + file statics).
// ===========================================================================

// How much data do we want to send in one CopyData message? Note that this may
// also result in reading the underlying files in chunks of this size.
//
// NB: The buffer size is required to be a multiple of the system block size, so
// use that value instead if it's bigger than our preference.
const SINK_BUFFER_LENGTH: c_int = if 32768 > BLCKSZ { 32768 } else { BLCKSZ as c_int };

#[repr(C)]
pub struct basebackup_options {
    pub label: *const c_char,
    pub progress: bool,
    pub fastcheckpoint: bool,
    pub nowait: bool,
    pub includewal: bool,
    pub incremental: bool,
    pub maxrate: uint32,
    pub sendtblspcmapfile: bool,
    pub send_to_client: bool,
    pub use_copytblspc: bool,
    pub target_handle: *mut BaseBackupTargetHandle,
    pub manifest: backup_manifest_option,
    pub compression: pg_compress_algorithm,
    pub compression_specification: pg_compress_specification,
    pub manifest_checksum_type: pg_checksum_type,
}

// Was the backup currently in-progress initiated in recovery mode?
static mut backup_started_in_recovery: bool = false;

// Total number of checksum failures during base backup.
static mut total_checksum_failures: c_longlong = 0;

// Do not verify checksums.
static mut noverify_checksums: bool = false;

// Definition of one element part of an exclusion list, used for paths part of
// checksum validation or base backups.  "name" is the name of the file or path
// to check for exclusion.  If "match_prefix" is true, any items matching the
// name as prefix are excluded.
struct exclude_list_item {
    name: *const c_char,
    match_prefix: bool,
}

// The contents of these directories are removed or recreated during server
// start so they are not included in backups.  The directories themselves are
// kept and included as empty to preserve access permissions.
//
// Note: this list should be kept in sync with the filter lists in pg_rewind's
// filemap.c.
static excludeDirContents: [*const c_char; 8] = [
    // Skip temporary statistics files. PG_STAT_TMP_DIR must be skipped because
    // extensions like pg_stat_statements store data there.
    PG_STAT_TMP_DIR.as_ptr(),
    // It is generally not useful to backup the contents of this directory even
    // if the intention is to restore to another primary. See backup.sgml for a
    // more detailed description.
    PG_REPLSLOT_DIR.as_ptr(),
    // Contents removed on startup, see dsm_cleanup_for_mmap().
    PG_DYNSHMEM_DIR.as_ptr(),
    // Contents removed on startup, see AsyncShmemInit().
    c"pg_notify".as_ptr(),
    // Old contents are loaded for possible debugging but are not required for
    // normal operation, see SerialInit().
    c"pg_serial".as_ptr(),
    // Contents removed on startup, see DeleteAllExportedSnapshotFiles().
    c"pg_snapshots".as_ptr(),
    // Contents zeroed on startup, see StartupSUBTRANS().
    c"pg_subtrans".as_ptr(),
    // end of list
    null(),
];

// List of files excluded from backups.
static excludeFiles: [exclude_list_item; 9] = [
    // Skip auto conf temporary file.
    exclude_list_item { name: c"postgresql.auto.conf.tmp".as_ptr(), match_prefix: false },
    // Skip current log file temporary file
    exclude_list_item { name: c"current_logfiles.tmp".as_ptr(), match_prefix: false },
    // Skip relation cache because it is rebuilt on startup.  This includes
    // temporary files.
    exclude_list_item { name: c"pg_internal.init".as_ptr(), match_prefix: true },
    // backup_label and tablespace_map should not exist in a running cluster
    // capable of doing an online backup, but exclude them just in case.
    exclude_list_item { name: c"backup_label".as_ptr(), match_prefix: false },
    exclude_list_item { name: c"tablespace_map".as_ptr(), match_prefix: false },
    // If there's a backup_manifest, it belongs to a backup that was used to
    // start this server. It is *not* correct for this backup. Our
    // backup_manifest is injected into the backup separately if users want it.
    exclude_list_item { name: c"backup_manifest".as_ptr(), match_prefix: false },
    exclude_list_item { name: c"postmaster.pid".as_ptr(), match_prefix: false },
    exclude_list_item { name: c"postmaster.opts".as_ptr(), match_prefix: false },
    // end of list
    exclude_list_item { name: null(), match_prefix: false },
];

// ===========================================================================
// Actually do a base backup for the specified tablespaces.
//
// This is split out mainly to avoid complaints about "variable might be
// clobbered by longjmp" from stupider versions of gcc.
// ===========================================================================
unsafe fn perform_base_backup(
    opt: *mut basebackup_options,
    sink: *mut bbsink,
    ib: *mut IncrementalBackupInfo,
) {
    let mut state: bbsink_state = core::mem::zeroed();
    let mut endptr: XLogRecPtr = 0;
    let mut endtli: TimeLineID = 0;
    let mut manifest: backup_manifest_info = core::mem::zeroed();
    let backup_state: *mut BackupState;
    let tablespace_map: StringInfo;

    // Initial backup state, insofar as we know it now.
    state.tablespaces = NIL;
    state.tablespace_num = 0;
    state.bytes_done = 0;
    state.bytes_total = 0;
    state.bytes_total_is_valid = false;

    // we're going to use a BufFile, so we need a ResourceOwner
    Assert!(!AuxProcessResourceOwner.is_null());
    Assert!(
        CurrentResourceOwner == AuxProcessResourceOwner || CurrentResourceOwner.is_null()
    );
    CurrentResourceOwner = AuxProcessResourceOwner;

    backup_started_in_recovery = RecoveryInProgress();

    InitializeBackupManifest(&mut manifest, (*opt).manifest, (*opt).manifest_checksum_type);

    total_checksum_failures = 0;

    // Allocate backup related variables.
    backup_state = palloc0(core::mem::size_of::<BackupState>()) as *mut BackupState;
    tablespace_map = makeStringInfo();

    basebackup_progress_wait_checkpoint();
    do_pg_backup_start(
        (*opt).label,
        (*opt).fastcheckpoint,
        &mut state.tablespaces,
        backup_state,
        tablespace_map,
    );

    state.startptr = (*backup_state).startpoint;
    state.starttli = (*backup_state).starttli;

    // Once do_pg_backup_start has been called, ensure that any failure causes
    // us to abort the backup so we don't "leak" a backup counter. For this
    // reason, *all* functionality between do_pg_backup_start() and the end of
    // do_pg_backup_stop() should be inside the error cleanup block!

    // C also: PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, BoolGetDatum(false));
    PG_ENSURE_ERROR_CLEANUP!({
        let newti: *mut tablespaceinfo;

        // If this is an incremental backup, execute preparatory steps.
        if !ib.is_null() {
            PrepareForIncrementalBackup(ib, backup_state);
        }

        // Add a node for the base directory at the end
        newti = palloc0(core::mem::size_of::<tablespaceinfo>()) as *mut tablespaceinfo;
        (*newti).size = -1;
        state.tablespaces = lappend(state.tablespaces, newti as *mut c_void);

        // Calculate the total backup size by summing up the size of each
        // tablespace
        if (*opt).progress {
            basebackup_progress_estimate_backup_size();

            foreach!(lc, state.tablespaces, {
                let tmp = lfirst(current_cell!(lc)) as *mut tablespaceinfo;

                if (*tmp).path.is_null() {
                    (*tmp).size = sendDir(
                        sink,
                        c".".as_ptr(),
                        1,
                        true,
                        state.tablespaces,
                        true,
                        null_mut(),
                        InvalidOid,
                        null_mut(),
                    );
                } else {
                    (*tmp).size = sendTablespace(
                        sink,
                        (*tmp).path,
                        (*tmp).oid,
                        true,
                        null_mut(),
                        null_mut(),
                    );
                }
                state.bytes_total += (*tmp).size as uint64;
            });
            state.bytes_total_is_valid = true;
        }

        // notify basebackup sink about start of backup
        bbsink_begin_backup(sink, &mut state, SINK_BUFFER_LENGTH);

        // Send off our tablespaces one by one
        foreach!(lc, state.tablespaces, {
            let ti = lfirst(current_cell!(lc)) as *mut tablespaceinfo;

            if (*ti).path.is_null() {
                let mut statbuf: stat = core::mem::zeroed();
                let mut sendtblspclinks = true;
                let backup_label: *mut c_char;

                bbsink_begin_archive(sink, c"base.tar".as_ptr());

                // In the main tar, include the backup_label first...
                backup_label = build_backup_content(backup_state, false);
                sendFileWithContent(sink, BACKUP_LABEL_FILE.as_ptr(), backup_label, -1, &mut manifest);
                pfree(backup_label as *mut c_void);

                // Then the tablespace_map file, if required...
                if (*opt).sendtblspcmapfile {
                    sendFileWithContent(
                        sink,
                        TABLESPACE_MAP.as_ptr(),
                        (*tablespace_map).data,
                        -1,
                        &mut manifest,
                    );
                    sendtblspclinks = false;
                }

                // Then the bulk of the files...
                sendDir(
                    sink,
                    c".".as_ptr(),
                    1,
                    false,
                    state.tablespaces,
                    sendtblspclinks,
                    &mut manifest,
                    InvalidOid,
                    ib,
                );

                // ... and pg_control after everything else.
                let control = cstring_xlog_control();
                if lstat(control.as_ptr(), &mut statbuf) != 0 {
                    // C also: errcode_for_file_access()
                    ereport!(
                        ERROR,
                        errmsg!("could not stat file \"{}\": %m", cstr(control.as_ptr()))
                    );
                }
                sendFile(
                    sink,
                    control.as_ptr(),
                    control.as_ptr(),
                    &mut statbuf,
                    false,
                    InvalidOid,
                    InvalidOid,
                    InvalidRelFileNumber,
                    0,
                    &mut manifest,
                    0,
                    null_mut(),
                    0,
                );
            } else {
                let archive_name = psprintf_cstr(&format!("{}.tar", (*ti).oid));

                bbsink_begin_archive(sink, archive_name);

                sendTablespace(sink, (*ti).path, (*ti).oid, false, &mut manifest, ib);
            }

            // If we're including WAL, and this is the main data directory we
            // don't treat this as the end of the tablespace. Instead, we will
            // include the xlog files below and stop afterwards. This is safe
            // since the main data directory is always sent *last*.
            if (*opt).includewal && (*ti).path.is_null() {
                Assert!(crate::nodes::pg_list::lnext(state.tablespaces, current_cell!(lc)).is_null());
            } else {
                // Properly terminate the tarfile.
                // C also: StaticAssertDecl(2 * TAR_BLOCK_SIZE <= BLCKSZ,
                //                          "BLCKSZ too small for 2 tar blocks");
                core::ptr::write_bytes((*sink).bbs_buffer, 0, (2 * TAR_BLOCK_SIZE) as usize);
                bbsink_archive_contents(sink, (2 * TAR_BLOCK_SIZE) as Size);

                // OK, that's the end of the archive.
                bbsink_end_archive(sink);
            }
        });

        basebackup_progress_wait_wal_archive(&mut state);
        do_pg_backup_stop(backup_state, !(*opt).nowait);

        endptr = (*backup_state).stoppoint;
        endtli = (*backup_state).stoptli;

        // Deallocate backup-related variables.
        destroyStringInfo(tablespace_map);
        pfree(backup_state as *mut c_void);
    });
    // C also: PG_END_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, BoolGetDatum(false));
    let _ = do_pg_abort_backup; // referenced only by the (elided) cleanup handler

    if (*opt).includewal {
        // We've left the last tar file "open", so we can now append the
        // required WAL files to it.
        let mut pathbuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let mut segno: XLogSegNo = 0;
        let mut startsegno: XLogSegNo = 0;
        let mut endsegno: XLogSegNo = 0;
        let mut statbuf: stat = core::mem::zeroed();
        let mut historyFileList: *mut List = NIL;
        let mut walFileList: *mut List = NIL;
        let mut firstoff: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
        let mut lastoff: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
        let dir: *mut DIR;
        let mut de: *mut dirent;
        let mut tli: TimeLineID = 0;

        basebackup_progress_transfer_wal();

        // I'd rather not worry about timelines here, so scan pg_wal and include
        // all WAL files in the range between 'startptr' and 'endptr', regardless
        // of the timeline the file is stamped with. If there are some spurious
        // WAL files belonging to timelines that don't belong in this server's
        // history, they will be included too. Normally there shouldn't be such
        // files, but if there are, there's little harm in including them.
        XLByteToSeg(state.startptr, &mut startsegno, wal_segment_size);
        XLogFileName(firstoff.as_mut_ptr(), state.starttli, startsegno, wal_segment_size);
        XLByteToPrevSeg(endptr, &mut endsegno, wal_segment_size);
        XLogFileName(lastoff.as_mut_ptr(), endtli, endsegno, wal_segment_size);

        dir = AllocateDir(c"pg_wal".as_ptr());
        loop {
            de = ReadDir(dir, c"pg_wal".as_ptr());
            if de.is_null() {
                break;
            }
            // Does it look like a WAL segment, and is it in the range?
            if IsXLogFileName((*de).d_name.as_ptr())
                && strcmp_off(&(*de).d_name, 8, &firstoff, 8) >= 0
                && strcmp_off(&(*de).d_name, 8, &lastoff, 8) <= 0
            {
                walFileList = lappend(walFileList, pstrdup((*de).d_name.as_ptr()) as *mut c_void);
            }
            // Does it look like a timeline history file?
            else if IsTLHistoryFileName((*de).d_name.as_ptr()) {
                historyFileList =
                    lappend(historyFileList, pstrdup((*de).d_name.as_ptr()) as *mut c_void);
            }
        }
        FreeDir(dir);

        // Before we go any further, check that none of the WAL segments we need
        // were removed.
        CheckXLogRemoved(startsegno, state.starttli);

        // Sort the WAL filenames.  We want to send the files in order from
        // oldest to newest, to reduce the chance that a file is recycled before
        // we get a chance to send it over.
        list_sort(walFileList, compareWalFileNames);

        // There must be at least one xlog file in the pg_wal directory, since
        // we are doing backup-including-xlog.
        if walFileList == NIL {
            ereport!(ERROR, errmsg!("could not find any WAL files"));
        }

        // Sanity check: the first and last segment should cover startptr and
        // endptr, with no gaps in between.
        XLogFromFileName(
            linitial(walFileList) as *const c_char,
            &mut tli,
            &mut segno,
            wal_segment_size,
        );
        if segno != startsegno {
            let mut startfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

            XLogFileName(startfname.as_mut_ptr(), state.starttli, startsegno, wal_segment_size);
            ereport!(
                ERROR,
                errmsg!("could not find WAL file \"{}\"", cstr(startfname.as_ptr()))
            );
        }
        foreach!(lc, walFileList, {
            let walFileName = lfirst(current_cell!(lc)) as *mut c_char;
            let currsegno: XLogSegNo = segno;
            let nextsegno: XLogSegNo = segno + 1;

            XLogFromFileName(walFileName, &mut tli, &mut segno, wal_segment_size);
            if !(nextsegno == segno || currsegno == segno) {
                let mut nextfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

                XLogFileName(nextfname.as_mut_ptr(), tli, nextsegno, wal_segment_size);
                ereport!(
                    ERROR,
                    errmsg!("could not find WAL file \"{}\"", cstr(nextfname.as_ptr()))
                );
            }
        });
        if segno != endsegno {
            let mut endfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

            XLogFileName(endfname.as_mut_ptr(), endtli, endsegno, wal_segment_size);
            ereport!(
                ERROR,
                errmsg!("could not find WAL file \"{}\"", cstr(endfname.as_ptr()))
            );
        }

        // Ok, we have everything we need. Send the WAL files.
        foreach!(lc, walFileList, {
            let walFileName = lfirst(current_cell!(lc)) as *mut c_char;
            let fd: c_int;
            let mut cnt: isize;
            let mut len: pgoff_t = 0;

            snprintf_into(
                pathbuf.as_mut_ptr(),
                MAXPGPATH,
                &format!("{}/{}", XLOGDIR, cstr(walFileName)),
            );
            XLogFromFileName(walFileName, &mut tli, &mut segno, wal_segment_size);

            fd = OpenTransientFile(pathbuf.as_ptr(), O_RDONLY | PG_BINARY);
            if fd < 0 {
                let save_errno = errno();

                // Most likely reason for this is that the file was already
                // removed by a checkpoint, so check for that to get a better
                // error message.
                CheckXLogRemoved(segno, tli);

                set_errno(save_errno);
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!("could not open file \"{}\": %m", cstr(pathbuf.as_ptr()))
                );
            }

            if fstat(fd, &mut statbuf) != 0 {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!("could not stat file \"{}\": %m", cstr(pathbuf.as_ptr()))
                );
            }
            if statbuf.st_size != wal_segment_size as off_t {
                CheckXLogRemoved(segno, tli);
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!("unexpected WAL file size \"{}\"", cstr(walFileName))
                );
            }

            // send the WAL file itself
            _tarWriteHeader(sink, pathbuf.as_ptr(), null(), &mut statbuf, false);

            loop {
                cnt = basebackup_read_file(
                    fd,
                    (*sink).bbs_buffer,
                    Min((*sink).bbs_buffer_length, (wal_segment_size as pgoff_t - len) as Size),
                    len as off_t,
                    pathbuf.as_ptr(),
                    true,
                );
                if cnt <= 0 {
                    break;
                }
                CheckXLogRemoved(segno, tli);
                bbsink_archive_contents(sink, cnt as Size);

                len += cnt as pgoff_t;

                if len == wal_segment_size as pgoff_t {
                    break;
                }
            }

            if len != wal_segment_size as pgoff_t {
                CheckXLogRemoved(segno, tli);
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!("unexpected WAL file size \"{}\"", cstr(walFileName))
                );
            }

            // wal_segment_size is a multiple of TAR_BLOCK_SIZE, so no need for
            // padding.
            Assert!(wal_segment_size % TAR_BLOCK_SIZE == 0);

            CloseTransientFile(fd);

            // Mark file as archived, otherwise files can get archived again
            // after promotion of a new node. This is in line with walreceiver.c
            // always doing an XLogArchiveForceDone() after a complete segment.
            StatusFilePath(pathbuf.as_mut_ptr(), walFileName, c".done".as_ptr());
            sendFileWithContent(sink, pathbuf.as_ptr(), c"".as_ptr(), -1, &mut manifest);
        });

        // Send timeline history files too. Only the latest timeline history
        // file is required for recovery, and even that only if there happens to
        // be a timeline switch in the first WAL segment that contains the
        // checkpoint record, or if we're taking a base backup from a standby
        // server and the target timeline changes while the backup is taken. But
        // they are small and highly useful for debugging purposes, so better
        // include them all, always.
        foreach!(lc, historyFileList, {
            let fname = lfirst(current_cell!(lc)) as *mut c_char;

            snprintf_into(
                pathbuf.as_mut_ptr(),
                MAXPGPATH,
                &format!("{}/{}", XLOGDIR, cstr(fname)),
            );

            if lstat(pathbuf.as_ptr(), &mut statbuf) != 0 {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!("could not stat file \"{}\": %m", cstr(pathbuf.as_ptr()))
                );
            }

            sendFile(
                sink,
                pathbuf.as_ptr(),
                pathbuf.as_ptr(),
                &mut statbuf,
                false,
                InvalidOid,
                InvalidOid,
                InvalidRelFileNumber,
                0,
                &mut manifest,
                0,
                null_mut(),
                0,
            );

            // unconditionally mark file as archived
            StatusFilePath(pathbuf.as_mut_ptr(), fname, c".done".as_ptr());
            sendFileWithContent(sink, pathbuf.as_ptr(), c"".as_ptr(), -1, &mut manifest);
        });

        // Properly terminate the tar file.
        // C also: StaticAssertStmt(2 * TAR_BLOCK_SIZE <= BLCKSZ,
        //                          "BLCKSZ too small for 2 tar blocks");
        core::ptr::write_bytes((*sink).bbs_buffer, 0, (2 * TAR_BLOCK_SIZE) as usize);
        bbsink_archive_contents(sink, (2 * TAR_BLOCK_SIZE) as Size);

        // OK, that's the end of the archive.
        bbsink_end_archive(sink);
    }

    AddWALInfoToBackupManifest(&mut manifest, state.startptr, state.starttli, endptr, endtli);

    SendBackupManifest(&mut manifest, sink);

    bbsink_end_backup(sink, endptr, endtli);

    if total_checksum_failures != 0 {
        if total_checksum_failures > 1 {
            // C also: errmsg_plural("%lld total checksum verification failure",
            //                       "%lld total checksum verification failures", ...)
            ereport!(
                WARNING,
                errmsg!("{} total checksum verification failures", total_checksum_failures)
            );
        }

        // C also: errcode(ERRCODE_DATA_CORRUPTED)
        ereport!(
            ERROR,
            errmsg!("checksum verification failure during base backup")
        );
    }

    // Make sure to free the manifest before the resource owners as manifests
    // use cryptohash contexts that may depend on resource owners (like OpenSSL).
    FreeBackupManifest(&mut manifest);

    // clean up the resource owner we created
    ReleaseAuxProcessResources(true);

    basebackup_progress_done();
}

// list_sort comparison function, to compare log/seg portion of WAL segment
// filenames, ignoring the timeline portion.
unsafe fn compareWalFileNames(a: *const ListCell, b: *const ListCell) -> c_int {
    let fna = lfirst(a as *mut ListCell) as *const c_char;
    let fnb = lfirst(b as *mut ListCell) as *const c_char;

    libc_strcmp(fna.add(8), fnb.add(8))
}

// Parse the base backup options passed down by the parser
unsafe fn parse_basebackup_options(options: *mut List, opt: *mut basebackup_options) {
    let mut o_label = false;
    let mut o_progress = false;
    let mut o_checkpoint = false;
    let mut o_nowait = false;
    let mut o_wal = false;
    let mut o_incremental = false;
    let mut o_maxrate = false;
    let mut o_tablespace_map = false;
    let mut o_noverify_checksums = false;
    let mut o_manifest = false;
    let mut o_manifest_checksums = false;
    let mut o_target = false;
    let mut o_target_detail = false;
    let mut target_str: *mut c_char = null_mut();
    let mut target_detail_str: *mut c_char = null_mut();
    let mut o_compression = false;
    let mut o_compression_detail = false;
    let mut compression_detail_str: *mut c_char = null_mut();

    core::ptr::write_bytes(opt as *mut u8, 0, core::mem::size_of::<basebackup_options>());
    (*opt).manifest = MANIFEST_OPTION_NO;
    (*opt).manifest_checksum_type = CHECKSUM_TYPE_CRC32C;
    (*opt).compression = PG_COMPRESSION_NONE;
    (*opt).compression_specification.algorithm = PG_COMPRESSION_NONE;

    foreach!(lopt, options, {
        let defel = lfirst(current_cell!(lopt)) as *mut DefElem;
        let defname = (*defel).defname;

        if libc_strcmp(defname, c"label".as_ptr()) == 0 {
            if o_label {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            (*opt).label = defGetString(defel);
            o_label = true;
        } else if libc_strcmp(defname, c"progress".as_ptr()) == 0 {
            if o_progress {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            (*opt).progress = defGetBoolean(defel);
            o_progress = true;
        } else if libc_strcmp(defname, c"checkpoint".as_ptr()) == 0 {
            let optval = defGetString(defel);

            if o_checkpoint {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            if pg_strcasecmp(optval, c"fast".as_ptr()) == 0 {
                (*opt).fastcheckpoint = true;
            } else if pg_strcasecmp(optval, c"spread".as_ptr()) == 0 {
                (*opt).fastcheckpoint = false;
            } else {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(
                    ERROR,
                    errmsg!("unrecognized checkpoint type: \"{}\"", cstr(optval))
                );
            }
            o_checkpoint = true;
        } else if libc_strcmp(defname, c"wait".as_ptr()) == 0 {
            if o_nowait {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            (*opt).nowait = !defGetBoolean(defel);
            o_nowait = true;
        } else if libc_strcmp(defname, c"wal".as_ptr()) == 0 {
            if o_wal {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            (*opt).includewal = defGetBoolean(defel);
            o_wal = true;
        } else if libc_strcmp(defname, c"incremental".as_ptr()) == 0 {
            if o_incremental {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            (*opt).incremental = defGetBoolean(defel);
            if (*opt).incremental && !summarize_wal {
                // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                ereport!(
                    ERROR,
                    errmsg!("incremental backups cannot be taken unless WAL summarization is enabled")
                );
            }
            o_incremental = true;
        } else if libc_strcmp(defname, c"max_rate".as_ptr()) == 0 {
            let maxrate: int64;

            if o_maxrate {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }

            maxrate = defGetInt64(defel);
            if maxrate < MAX_RATE_LOWER as int64 || maxrate > MAX_RATE_UPPER as int64 {
                // C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                ereport!(
                    ERROR,
                    errmsg!(
                        "{} is outside the valid range for parameter \"{}\" ({} .. {})",
                        maxrate as c_int,
                        "MAX_RATE",
                        MAX_RATE_LOWER,
                        MAX_RATE_UPPER
                    )
                );
            }

            (*opt).maxrate = maxrate as uint32;
            o_maxrate = true;
        } else if libc_strcmp(defname, c"tablespace_map".as_ptr()) == 0 {
            if o_tablespace_map {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            (*opt).sendtblspcmapfile = defGetBoolean(defel);
            o_tablespace_map = true;
        } else if libc_strcmp(defname, c"verify_checksums".as_ptr()) == 0 {
            if o_noverify_checksums {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            noverify_checksums = !defGetBoolean(defel);
            o_noverify_checksums = true;
        } else if libc_strcmp(defname, c"manifest".as_ptr()) == 0 {
            let optval = defGetString(defel);
            let mut manifest_bool = false;

            if o_manifest {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            if parse_bool(optval, &mut manifest_bool) {
                if manifest_bool {
                    (*opt).manifest = MANIFEST_OPTION_YES;
                } else {
                    (*opt).manifest = MANIFEST_OPTION_NO;
                }
            } else if pg_strcasecmp(optval, c"force-encode".as_ptr()) == 0 {
                (*opt).manifest = MANIFEST_OPTION_FORCE_ENCODE;
            } else {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(
                    ERROR,
                    errmsg!("unrecognized manifest option: \"{}\"", cstr(optval))
                );
            }
            o_manifest = true;
        } else if libc_strcmp(defname, c"manifest_checksums".as_ptr()) == 0 {
            let optval = defGetString(defel);

            if o_manifest_checksums {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            if !pg_checksum_parse_type(optval, &mut (*opt).manifest_checksum_type) {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(
                    ERROR,
                    errmsg!("unrecognized checksum algorithm: \"{}\"", cstr(optval))
                );
            }
            o_manifest_checksums = true;
        } else if libc_strcmp(defname, c"target".as_ptr()) == 0 {
            if o_target {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            target_str = defGetString(defel);
            o_target = true;
        } else if libc_strcmp(defname, c"target_detail".as_ptr()) == 0 {
            let optval = defGetString(defel);

            if o_target_detail {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            target_detail_str = optval;
            o_target_detail = true;
        } else if libc_strcmp(defname, c"compression".as_ptr()) == 0 {
            let optval = defGetString(defel);

            if o_compression {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            if !parse_compress_algorithm(optval, &mut (*opt).compression) {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(
                    ERROR,
                    errmsg!("unrecognized compression algorithm: \"{}\"", cstr(optval))
                );
            }
            o_compression = true;
        } else if libc_strcmp(defname, c"compression_detail".as_ptr()) == 0 {
            if o_compression_detail {
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
                ereport!(ERROR, errmsg!("duplicate option \"{}\"", cstr(defname)));
            }
            compression_detail_str = defGetString(defel);
            o_compression_detail = true;
        } else {
            // C also: errcode(ERRCODE_SYNTAX_ERROR)
            ereport!(
                ERROR,
                errmsg!("unrecognized base backup option: \"{}\"", cstr(defname))
            );
        }
    });

    if (*opt).label.is_null() {
        (*opt).label = c"base backup".as_ptr();
    }
    if (*opt).manifest == MANIFEST_OPTION_NO {
        if o_manifest_checksums {
            // C also: errcode(ERRCODE_SYNTAX_ERROR)
            ereport!(
                ERROR,
                errmsg!("manifest checksums require a backup manifest")
            );
        }
        (*opt).manifest_checksum_type = CHECKSUM_TYPE_NONE;
    }

    if target_str.is_null() {
        if !target_detail_str.is_null() {
            // C also: errcode(ERRCODE_SYNTAX_ERROR)
            ereport!(
                ERROR,
                errmsg!("target detail cannot be used without target")
            );
        }
        (*opt).use_copytblspc = true;
        (*opt).send_to_client = true;
    } else if libc_strcmp(target_str, c"client".as_ptr()) == 0 {
        if !target_detail_str.is_null() {
            // C also: errcode(ERRCODE_SYNTAX_ERROR)
            ereport!(
                ERROR,
                errmsg!(
                    "target \"{}\" does not accept a target detail",
                    cstr(target_str)
                )
            );
        }
        (*opt).send_to_client = true;
    } else {
        (*opt).target_handle = BaseBackupGetTargetHandle(target_str, target_detail_str);
    }

    if o_compression_detail && !o_compression {
        // C also: errcode(ERRCODE_SYNTAX_ERROR)
        ereport!(
            ERROR,
            errmsg!("compression detail cannot be specified unless compression is enabled")
        );
    }

    if o_compression {
        let error_detail: *mut c_char;

        parse_compress_specification(
            (*opt).compression,
            compression_detail_str,
            &mut (*opt).compression_specification,
        );
        error_detail = validate_compress_specification(&mut (*opt).compression_specification);
        if !error_detail.is_null() {
            // C also: errcode(ERRCODE_SYNTAX_ERROR)
            ereport!(
                ERROR,
                errmsg!("invalid compression specification: {}", cstr(error_detail))
            );
        }
    }
}

// SendBaseBackup() - send a complete base backup.
//
// The function will put the system into backup mode like pg_backup_start() does,
// so that the backup is consistent even though we read directly from the
// filesystem, bypassing the buffer cache.
pub unsafe fn SendBaseBackup(cmd: *mut BaseBackupCmd, mut ib: *mut IncrementalBackupInfo) {
    let mut opt: basebackup_options = core::mem::zeroed();
    let mut sink: *mut bbsink;
    let status: SessionBackupState = get_backup_status();

    if status == SESSION_BACKUP_RUNNING {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            ERROR,
            errmsg!("a backup is already in progress in this session")
        );
    }

    parse_basebackup_options((*cmd).options, &mut opt);

    WalSndSetState(WALSNDSTATE_BACKUP);

    if update_process_title {
        let mut activitymsg: [c_char; 50] = [0; 50];

        snprintf_into(
            activitymsg.as_mut_ptr(),
            50,
            &format!("sending backup \"{}\"", cstr(opt.label)),
        );
        set_ps_display(activitymsg.as_ptr());
    }

    // If we're asked to perform an incremental backup and the user has not
    // supplied a manifest, that's an ERROR.
    //
    // If we're asked to perform a full backup and the user did supply a
    // manifest, just ignore it.
    if !opt.incremental {
        ib = null_mut();
    } else if ib.is_null() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            ERROR,
            errmsg!("must UPLOAD_MANIFEST before performing an incremental BASE_BACKUP")
        );
    }

    // If the target is specifically 'client' then set up to stream the backup
    // to the client; otherwise, it's being sent someplace else and should not
    // be sent to the client. BaseBackupGetSink has the job of setting up a sink
    // to send the backup data wherever it needs to go.
    sink = bbsink_copystream_new(opt.send_to_client);
    if !opt.target_handle.is_null() {
        sink = BaseBackupGetSink(opt.target_handle, sink);
    }

    // Set up network throttling, if client requested it
    if opt.maxrate > 0 {
        sink = bbsink_throttle_new(sink, opt.maxrate);
    }

    // Set up server-side compression, if client requested it
    if opt.compression == PG_COMPRESSION_GZIP {
        sink = bbsink_gzip_new(sink, &mut opt.compression_specification);
    } else if opt.compression == PG_COMPRESSION_LZ4 {
        sink = bbsink_lz4_new(sink, &mut opt.compression_specification);
    } else if opt.compression == PG_COMPRESSION_ZSTD {
        sink = bbsink_zstd_new(sink, &mut opt.compression_specification);
    }

    // Set up progress reporting.
    sink = bbsink_progress_new(sink, opt.progress);

    // Perform the base backup, but make sure we clean up the bbsink even if an
    // error occurs.
    PG_TRY!(
        {
            perform_base_backup(&mut opt, sink, ib);
        },
        {
            // PG_FINALLY
            bbsink_cleanup(sink);
        }
    );
}

// Inject a file with given name and content in the output tar stream.
//
// "len" can optionally be set to an arbitrary length of data sent.  If set to
// -1, the content sent is treated as a string with strlen() as length.
unsafe fn sendFileWithContent(
    sink: *mut bbsink,
    filename: *const c_char,
    mut content: *const c_char,
    mut len: c_int,
    manifest: *mut backup_manifest_info,
) {
    let mut statbuf: stat = core::mem::zeroed();
    let mut bytes_done: c_int = 0;
    let mut checksum_ctx: pg_checksum_context = core::mem::zeroed();

    if pg_checksum_init(&mut checksum_ctx, (*manifest).checksum_type) < 0 {
        elog!(
            ERROR,
            "could not initialize checksum of file \"{}\"",
            cstr(filename)
        );
    }

    if len < 0 {
        len = libc_strlen(content) as c_int;
    }

    // Construct a stat struct for the file we're injecting in the tar.

    // Windows doesn't have the concept of uid and gid
    statbuf.st_uid = geteuid();
    statbuf.st_gid = getegid();
    statbuf.st_mtime = time(null_mut());
    statbuf.st_mode = pg_file_create_mode as mode_t;
    statbuf.st_size = len as off_t;

    _tarWriteHeader(sink, filename, null(), &mut statbuf, false);

    if pg_checksum_update(&mut checksum_ctx, content as *mut uint8, len as c_int) < 0 {
        elog!(
            ERROR,
            "could not update checksum of file \"{}\"",
            cstr(filename)
        );
    }

    while bytes_done < len {
        let remaining = (len - bytes_done) as Size;
        let nbytes = Min((*sink).bbs_buffer_length, remaining);

        core::ptr::copy_nonoverlapping(content, (*sink).bbs_buffer, nbytes);
        bbsink_archive_contents(sink, nbytes);
        bytes_done += nbytes as c_int;
        content = content.add(nbytes);
    }

    _tarWritePadding(sink, len);

    AddFileToBackupManifest(
        manifest,
        InvalidOid,
        filename,
        len as Size,
        statbuf.st_mtime as pg_time_t,
        &mut checksum_ctx,
    );
}

// Include the tablespace directory pointed to by 'path' in the output tar
// stream.  If 'sizeonly' is true, we just calculate a total length and return
// it, without actually sending anything.
//
// Only used to send auxiliary tablespaces, not PGDATA.
unsafe fn sendTablespace(
    sink: *mut bbsink,
    path: *mut c_char,
    spcoid: Oid,
    sizeonly: bool,
    manifest: *mut backup_manifest_info,
    ib: *mut IncrementalBackupInfo,
) -> int64 {
    let mut size: int64;
    let mut pathbuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut statbuf: stat = core::mem::zeroed();

    // 'path' points to the tablespace location, but we only want to include the
    // version directory in it that belongs to us.
    snprintf_into(
        pathbuf.as_mut_ptr(),
        MAXPGPATH,
        &format!(
            "{}/{}",
            cstr(path),
            TABLESPACE_VERSION_DIRECTORY.to_str().unwrap()
        ),
    );

    // Store a directory entry in the tar file so we get the permissions right.
    if lstat(pathbuf.as_ptr(), &mut statbuf) != 0 {
        if errno() != ENOENT {
            // C also: errcode_for_file_access()
            ereport!(
                ERROR,
                errmsg!(
                    "could not stat file or directory \"{}\": %m",
                    cstr(pathbuf.as_ptr())
                )
            );
        }

        // If the tablespace went away while scanning, it's no error.
        return 0;
    }

    size = _tarWriteHeader(
        sink,
        TABLESPACE_VERSION_DIRECTORY.as_ptr(),
        null(),
        &mut statbuf,
        sizeonly,
    );

    // Send all the files in the tablespace version directory
    size += sendDir(
        sink,
        pathbuf.as_ptr(),
        libc_strlen(path) as c_int,
        sizeonly,
        NIL,
        true,
        manifest,
        spcoid,
        ib,
    );

    size
}

// Include all files from the given directory in the output tar stream. If
// 'sizeonly' is true, we just calculate a total length and return it, without
// actually sending anything.
//
// Omit any directory in the tablespaces list, to avoid backing up tablespaces
// twice when they were created inside PGDATA.
//
// If sendtblspclinks is true, we need to include symlink information in the tar
// file. If not, we can skip that as it will be sent separately in the
// tablespace_map file.
unsafe fn sendDir(
    sink: *mut bbsink,
    path: *const c_char,
    basepathlen: c_int,
    sizeonly: bool,
    tablespaces: *mut List,
    sendtblspclinks: bool,
    manifest: *mut backup_manifest_info,
    spcoid: Oid,
    ib: *mut IncrementalBackupInfo,
) -> int64 {
    let dir: *mut DIR;
    let mut de: *mut dirent;
    let mut pathbuf: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
    let mut statbuf: stat = core::mem::zeroed();
    let mut size: int64 = 0;
    let lastDir: *const c_char; // Split last dir from parent path.
    let mut isRelationDir = false; // Does directory contain relations?
    let mut isGlobalDir = false;
    let mut dboid: Oid = InvalidOid;
    let mut relative_block_numbers: *mut BlockNumber = null_mut();

    // Since this array is relatively large, avoid putting it on the stack. But
    // we don't need it at all if this is not an incremental backup.
    if !ib.is_null() {
        relative_block_numbers =
            palloc(core::mem::size_of::<BlockNumber>() * RELSEG_SIZE) as *mut BlockNumber;
    }

    // Determine if the current path is a database directory that can contain
    // relations.
    //
    // Start by finding the location of the delimiter between the parent path
    // and the current path.
    lastDir = last_dir_separator(path);

    // Does this path look like a database path (i.e. all digits)?
    if !lastDir.is_null()
        && libc_strspn(lastDir.add(1), c"0123456789".as_ptr()) == libc_strlen(lastDir.add(1))
    {
        // Part of path that contains the parent directory.
        let parentPathLen = lastDir.offset_from(path) as c_int;

        // Mark path as a database directory if the parent path is either
        // $PGDATA/base or a tablespace version path.
        let tvd_len = (TABLESPACE_VERSION_DIRECTORY.to_bytes().len()) as c_int;
        if libc_strncmp(path, c"./base".as_ptr(), parentPathLen as usize) == 0
            || (parentPathLen >= tvd_len
                && libc_strncmp(
                    lastDir.offset(-(tvd_len as isize)),
                    TABLESPACE_VERSION_DIRECTORY.as_ptr(),
                    tvd_len as usize,
                ) == 0)
        {
            isRelationDir = true;
            dboid = atooid(&cstr(lastDir.add(1)));
        }
    } else if libc_strcmp(path, c"./global".as_ptr()) == 0 {
        isRelationDir = true;
        isGlobalDir = true;
    }

    dir = AllocateDir(path);
    loop {
        de = ReadDir(dir, path);
        if de.is_null() {
            break;
        }
        let mut excludeIdx: usize;
        let mut excludeFound: bool;
        let mut relfilenumber: RelFileNumber = InvalidRelFileNumber;
        let mut relForkNum: ForkNumber = InvalidForkNumber;
        let mut segno: c_uint = 0;
        let mut isRelationFile = false;

        // Skip special stuff
        if libc_strcmp((*de).d_name.as_ptr(), c".".as_ptr()) == 0
            || libc_strcmp((*de).d_name.as_ptr(), c"..".as_ptr()) == 0
        {
            continue;
        }

        // Skip temporary files
        if libc_strncmp(
            (*de).d_name.as_ptr(),
            PG_TEMP_FILE_PREFIX.as_ptr(),
            PG_TEMP_FILE_PREFIX.to_bytes().len(),
        ) == 0
        {
            continue;
        }

        // Skip macOS system files
        if libc_strcmp((*de).d_name.as_ptr(), c".DS_Store".as_ptr()) == 0 {
            continue;
        }

        // Check if the postmaster has signaled us to exit, and abort with an
        // error in that case. The error handler further up will call
        // do_pg_abort_backup() for us. Also check that if the backup was started
        // while still in recovery, the server wasn't promoted.
        // do_pg_backup_stop() will check that too, but it's better to stop the
        // backup early than continue to the end and fail there.
        CHECK_FOR_INTERRUPTS!();
        if RecoveryInProgress() != backup_started_in_recovery {
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            //         errhint("This means that the backup being taken is corrupt
            //                  and should not be used. Try taking another online backup.")
            ereport!(
                ERROR,
                errmsg!("the standby was promoted during online backup")
            );
        }

        // Scan for files that should be excluded
        excludeFound = false;
        excludeIdx = 0;
        while !excludeFiles[excludeIdx].name.is_null() {
            let mut cmplen = libc_strlen(excludeFiles[excludeIdx].name);

            if !excludeFiles[excludeIdx].match_prefix {
                cmplen += 1;
            }
            if libc_strncmp((*de).d_name.as_ptr(), excludeFiles[excludeIdx].name, cmplen) == 0 {
                elog!(DEBUG1, "file \"{}\" excluded from backup", cstr((*de).d_name.as_ptr()));
                excludeFound = true;
                break;
            }
            excludeIdx += 1;
        }

        if excludeFound {
            continue;
        }

        // If there could be non-temporary relation files in this directory, try
        // to parse the filename.
        if isRelationDir {
            isRelationFile = parse_filename_for_nontemp_relation(
                (*de).d_name.as_ptr(),
                &mut relfilenumber,
                &mut relForkNum,
                &mut segno,
            );
        }

        // Exclude all forks for unlogged tables except the init fork
        if isRelationFile && relForkNum != INIT_FORKNUM {
            let mut initForkFile: [c_char; MAXPGPATH] = [0; MAXPGPATH];

            // If any other type of fork, check if there is an init fork with the
            // same RelFileNumber. If so, the file can be excluded.
            snprintf_into(
                initForkFile.as_mut_ptr(),
                MAXPGPATH,
                &format!("{}/{}_init", cstr(path), relfilenumber),
            );

            if lstat(initForkFile.as_ptr(), &mut statbuf) == 0 {
                elog!(
                    DEBUG2,
                    "unlogged relation file \"{}\" excluded from backup",
                    cstr((*de).d_name.as_ptr())
                );

                continue;
            }
        }

        // Exclude temporary relations
        if OidIsValid(dboid) && looks_like_temp_rel_name((*de).d_name.as_ptr()) {
            elog!(
                DEBUG2,
                "temporary relation file \"{}\" excluded from backup",
                cstr((*de).d_name.as_ptr())
            );

            continue;
        }

        snprintf_into(
            pathbuf.as_mut_ptr(),
            MAXPGPATH * 2,
            &format!("{}/{}", cstr(path), cstr((*de).d_name.as_ptr())),
        );

        // Skip pg_control here to back up it last
        if libc_strcmp(pathbuf.as_ptr(), cstring_dot_xlog_control().as_ptr()) == 0 {
            continue;
        }

        if lstat(pathbuf.as_ptr(), &mut statbuf) != 0 {
            if errno() != ENOENT {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not stat file or directory \"{}\": %m",
                        cstr(pathbuf.as_ptr())
                    )
                );
            }

            // If the file went away while scanning, it's not an error.
            continue;
        }

        // Scan for directories whose contents should be excluded
        excludeFound = false;
        excludeIdx = 0;
        while !excludeDirContents[excludeIdx].is_null() {
            if libc_strcmp((*de).d_name.as_ptr(), excludeDirContents[excludeIdx]) == 0 {
                elog!(
                    DEBUG1,
                    "contents of directory \"{}\" excluded from backup",
                    cstr((*de).d_name.as_ptr())
                );
                convert_link_to_directory(pathbuf.as_ptr(), &mut statbuf);
                size += _tarWriteHeader(
                    sink,
                    pathbuf.as_ptr().add((basepathlen + 1) as usize),
                    null(),
                    &mut statbuf,
                    sizeonly,
                );
                excludeFound = true;
                break;
            }
            excludeIdx += 1;
        }

        if excludeFound {
            continue;
        }

        // We can skip pg_wal, the WAL segments need to be fetched from the WAL
        // archive anyway. But include it as an empty directory anyway, so we get
        // permissions right.
        if libc_strcmp(pathbuf.as_ptr(), c"./pg_wal".as_ptr()) == 0 {
            // If pg_wal is a symlink, write it as a directory anyway
            convert_link_to_directory(pathbuf.as_ptr(), &mut statbuf);
            size += _tarWriteHeader(
                sink,
                pathbuf.as_ptr().add((basepathlen + 1) as usize),
                null(),
                &mut statbuf,
                sizeonly,
            );

            // Also send archive_status and summaries directories (by hackishly
            // reusing statbuf from above ...).
            size += _tarWriteHeader(
                sink,
                c"./pg_wal/archive_status".as_ptr(),
                null(),
                &mut statbuf,
                sizeonly,
            );
            size += _tarWriteHeader(
                sink,
                c"./pg_wal/summaries".as_ptr(),
                null(),
                &mut statbuf,
                sizeonly,
            );

            continue; // don't recurse into pg_wal
        }

        // Allow symbolic links in pg_tblspc only
        if libc_strcmp(path, c"./pg_tblspc".as_ptr()) == 0 && S_ISLNK(statbuf.st_mode) {
            let mut linkpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
            let rllen: c_int;

            rllen = readlink(pathbuf.as_ptr(), linkpath.as_mut_ptr(), MAXPGPATH) as c_int;
            if rllen < 0 {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not read symbolic link \"{}\": %m",
                        cstr(pathbuf.as_ptr())
                    )
                );
            }
            if rllen as usize >= MAXPGPATH {
                // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                ereport!(
                    ERROR,
                    errmsg!(
                        "symbolic link \"{}\" target is too long",
                        cstr(pathbuf.as_ptr())
                    )
                );
            }
            linkpath[rllen as usize] = 0;

            size += _tarWriteHeader(
                sink,
                pathbuf.as_ptr().add((basepathlen + 1) as usize),
                linkpath.as_ptr(),
                &mut statbuf,
                sizeonly,
            );
        } else if S_ISDIR(statbuf.st_mode) {
            let mut skip_this_dir = false;

            // Store a directory entry in the tar file so we can get the
            // permissions right.
            size += _tarWriteHeader(
                sink,
                pathbuf.as_ptr().add((basepathlen + 1) as usize),
                null(),
                &mut statbuf,
                sizeonly,
            );

            // Call ourselves recursively for a directory, unless it happens to
            // be a separate tablespace located within PGDATA.
            foreach!(lc, tablespaces, {
                let ti = lfirst(current_cell!(lc)) as *mut tablespaceinfo;

                // ti->rpath is the tablespace relative path within PGDATA, or
                // NULL if the tablespace has been properly located somewhere
                // else.
                //
                // Skip past the leading "./" in pathbuf when comparing.
                if !(*ti).rpath.is_null()
                    && libc_strcmp((*ti).rpath, pathbuf.as_ptr().add(2)) == 0
                {
                    skip_this_dir = true;
                    break;
                }
            });

            // skip sending directories inside pg_tblspc, if not required.
            if libc_strcmp(pathbuf.as_ptr(), c"./pg_tblspc".as_ptr()) == 0 && !sendtblspclinks {
                skip_this_dir = true;
            }

            if !skip_this_dir {
                size += sendDir(
                    sink,
                    pathbuf.as_ptr(),
                    basepathlen,
                    sizeonly,
                    tablespaces,
                    sendtblspclinks,
                    manifest,
                    spcoid,
                    ib,
                );
            }
        } else if S_ISREG(statbuf.st_mode) {
            let mut sent = false;
            let mut num_blocks_required: c_uint = 0;
            let mut truncation_block_length: c_uint = 0;
            let mut tarfilenamebuf: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
            let mut tarfilename: *const c_char = pathbuf.as_ptr().add((basepathlen + 1) as usize);
            let mut method: FileBackupMethod = BACK_UP_FILE_FULLY;

            if !ib.is_null() && isRelationFile {
                let relspcoid: Oid;
                let lookup_path: *mut c_char;

                if OidIsValid(spcoid) {
                    relspcoid = spcoid;
                    lookup_path = psprintf_cstr(&format!(
                        "{}/{}/{}",
                        PG_TBLSPC_DIR.to_str().unwrap(),
                        spcoid,
                        cstr(tarfilename)
                    ));
                } else {
                    if isGlobalDir {
                        relspcoid = GLOBALTABLESPACE_OID;
                    } else {
                        relspcoid = DEFAULTTABLESPACE_OID;
                    }
                    lookup_path = pstrdup(tarfilename);
                }

                method = GetFileBackupMethod(
                    ib,
                    lookup_path,
                    dboid,
                    relspcoid,
                    relfilenumber,
                    relForkNum,
                    segno,
                    statbuf.st_size as Size,
                    &mut num_blocks_required,
                    relative_block_numbers,
                    &mut truncation_block_length,
                );
                if method == BACK_UP_FILE_INCREMENTALLY {
                    statbuf.st_size = GetIncrementalFileSize(num_blocks_required) as off_t;
                    snprintf_into(
                        tarfilenamebuf.as_mut_ptr(),
                        MAXPGPATH * 2,
                        &format!(
                            "{}/INCREMENTAL.{}",
                            cstr(path.add((basepathlen + 1) as usize)),
                            cstr((*de).d_name.as_ptr())
                        ),
                    );
                    tarfilename = tarfilenamebuf.as_ptr();
                }

                pfree(lookup_path as *mut c_void);
            }

            if !sizeonly {
                sent = sendFile(
                    sink,
                    pathbuf.as_ptr(),
                    tarfilename,
                    &mut statbuf,
                    true,
                    dboid,
                    spcoid,
                    relfilenumber,
                    segno,
                    manifest,
                    num_blocks_required,
                    if method == BACK_UP_FILE_INCREMENTALLY {
                        relative_block_numbers
                    } else {
                        null_mut()
                    },
                    truncation_block_length,
                );
            }

            if sent || sizeonly {
                // Add size.
                size += statbuf.st_size as int64;

                // Pad to a multiple of the tar block size.
                size += tarPaddingBytesRequired(statbuf.st_size as Size) as int64;

                // Size of the header for the file.
                size += TAR_BLOCK_SIZE as int64;
            }
        } else {
            ereport!(
                WARNING,
                errmsg!("skipping special file \"{}\"", cstr(pathbuf.as_ptr()))
            );
        }
    }

    if !relative_block_numbers.is_null() {
        pfree(relative_block_numbers as *mut c_void);
    }

    FreeDir(dir);
    size
}

// Given the member, write the TAR header & send the file.
//
// If 'missing_ok' is true, will not throw an error if the file is not found.
//
// If dboid is anything other than InvalidOid then any checksum failures
// detected will get reported to the cumulative stats system.
//
// If the file is to be sent incrementally, then num_incremental_blocks should be
// the number of blocks to be sent, and incremental_blocks an array of block
// numbers relative to the start of the current segment. If the whole file is to
// be sent, then incremental_blocks should be NULL, and num_incremental_blocks
// can have any value, as it will be ignored.
//
// Returns true if the file was successfully sent, false if 'missing_ok', and the
// file did not exist.
unsafe fn sendFile(
    sink: *mut bbsink,
    readfilename: *const c_char,
    tarfilename: *const c_char,
    statbuf: *mut stat,
    missing_ok: bool,
    dboid: Oid,
    spcoid: Oid,
    relfilenumber: RelFileNumber,
    segno: c_uint,
    manifest: *mut backup_manifest_info,
    num_incremental_blocks: c_uint,
    incremental_blocks: *mut BlockNumber,
    truncation_block_length: c_uint,
) -> bool {
    let fd: c_int;
    let mut blkno: BlockNumber = 0;
    let mut checksum_failures: c_int = 0;
    let mut cnt: off_t;
    let mut bytes_done: pgoff_t = 0;
    let mut verify_checksum = false;
    let mut checksum_ctx: pg_checksum_context = core::mem::zeroed();
    let mut ibindex: c_int = 0;

    if pg_checksum_init(&mut checksum_ctx, (*manifest).checksum_type) < 0 {
        elog!(
            ERROR,
            "could not initialize checksum of file \"{}\"",
            cstr(readfilename)
        );
    }

    fd = OpenTransientFile(readfilename, O_RDONLY | PG_BINARY);
    if fd < 0 {
        if errno() == ENOENT && missing_ok {
            return false;
        }
        // C also: errcode_for_file_access()
        ereport!(
            ERROR,
            errmsg!("could not open file \"{}\": %m", cstr(readfilename))
        );
    }

    _tarWriteHeader(sink, tarfilename, null(), statbuf, false);

    // Checksums are verified in multiples of BLCKSZ, so the buffer length should
    // be a multiple of the block size as well.
    Assert!(((*sink).bbs_buffer_length % BLCKSZ) == 0);

    // If we weren't told not to verify checksums, and if checksums are enabled
    // for this cluster, and if this is a relation file, then verify the
    // checksum.
    if !noverify_checksums && DataChecksumsEnabled() && RelFileNumberIsValid(relfilenumber) {
        verify_checksum = true;
    }

    // If we're sending an incremental file, write the file header.
    if !incremental_blocks.is_null() {
        let magic: c_uint = INCREMENTAL_MAGIC;
        let mut header_bytes_done: Size = 0;
        let mut padding: [c_char; BLCKSZ] = [0; BLCKSZ];
        let paddinglen: Size;

        // Emit header data.
        push_to_sink(
            sink,
            &mut checksum_ctx,
            &mut header_bytes_done,
            &magic as *const c_uint as *mut c_void,
            core::mem::size_of::<c_uint>(),
        );
        push_to_sink(
            sink,
            &mut checksum_ctx,
            &mut header_bytes_done,
            &num_incremental_blocks as *const c_uint as *mut c_void,
            core::mem::size_of::<c_uint>(),
        );
        push_to_sink(
            sink,
            &mut checksum_ctx,
            &mut header_bytes_done,
            &truncation_block_length as *const c_uint as *mut c_void,
            core::mem::size_of::<c_uint>(),
        );
        push_to_sink(
            sink,
            &mut checksum_ctx,
            &mut header_bytes_done,
            incremental_blocks as *mut c_void,
            core::mem::size_of::<BlockNumber>() * num_incremental_blocks as usize,
        );

        // Add padding to align header to a multiple of BLCKSZ, but only if the
        // incremental file has some blocks, and the alignment is actually needed
        // (i.e. header is not already a multiple of BLCKSZ). If there are no
        // blocks we don't want to make the file unnecessarily large, as that
        // might make some filesystem optimizations impossible.
        if (num_incremental_blocks > 0) && (header_bytes_done % BLCKSZ != 0) {
            paddinglen = BLCKSZ - (header_bytes_done % BLCKSZ);

            core::ptr::write_bytes(padding.as_mut_ptr(), 0, paddinglen);
            bytes_done += paddinglen as pgoff_t;

            push_to_sink(
                sink,
                &mut checksum_ctx,
                &mut header_bytes_done,
                padding.as_mut_ptr() as *mut c_void,
                paddinglen,
            );
        }

        // Flush out any data still in the buffer so it's again empty.
        if header_bytes_done > 0 {
            bbsink_archive_contents(sink, header_bytes_done);
            if pg_checksum_update(
                &mut checksum_ctx,
                (*sink).bbs_buffer as *mut uint8,
                header_bytes_done as c_int,
            ) < 0
            {
                elog!(ERROR, "could not update checksum of base backup");
            }
        }

        // Update our notion of file position.
        bytes_done += core::mem::size_of::<c_uint>() as pgoff_t;
        bytes_done += core::mem::size_of::<c_uint>() as pgoff_t;
        bytes_done += core::mem::size_of::<c_uint>() as pgoff_t;
        bytes_done +=
            (core::mem::size_of::<BlockNumber>() * num_incremental_blocks as usize) as pgoff_t;
    }

    // Loop until we read the amount of data the caller told us to expect. The
    // file could be longer, if it was extended while we were sending it, but for
    // a base backup we can ignore such extended data. It will be restored from
    // WAL.
    loop {
        // Determine whether we've read all the data that we need, and if not,
        // read some more.
        if incremental_blocks.is_null() {
            let remaining = ((*statbuf).st_size - bytes_done) as Size;

            // If we've read the required number of bytes, then it's time to
            // stop.
            if bytes_done >= (*statbuf).st_size {
                break;
            }

            // Read as many bytes as will fit in the buffer, or however many are
            // left to read, whichever is less.
            cnt = read_file_data_into_buffer(
                sink,
                readfilename,
                fd,
                bytes_done,
                remaining,
                blkno + segno * RELSEG_SIZE as c_uint,
                verify_checksum,
                &mut checksum_failures,
            );
        } else {
            let relative_blkno: BlockNumber;

            // If we've read all the blocks, then it's time to stop.
            if ibindex >= num_incremental_blocks as c_int {
                break;
            }

            // Read just one block, whichever one is the next that we're supposed
            // to include.
            relative_blkno = *incremental_blocks.add(ibindex as usize);
            ibindex += 1;
            cnt = read_file_data_into_buffer(
                sink,
                readfilename,
                fd,
                relative_blkno as off_t * BLCKSZ as off_t,
                BLCKSZ,
                relative_blkno + segno * RELSEG_SIZE as c_uint,
                verify_checksum,
                &mut checksum_failures,
            );

            // If we get a partial read, that must mean that the relation is
            // being truncated. Ultimately, it should be truncated to a multiple
            // of BLCKSZ, since this path should only be reached for relation
            // files, but we might transiently observe an intermediate value.
            //
            // It should be fine to treat this just as if the entire block had
            // been truncated away - i.e. fill this and all later blocks with
            // zeroes. WAL replay will fix things up.
            if cnt < BLCKSZ as off_t {
                break;
            }
        }

        // If the amount of data we were able to read was not a multiple of
        // BLCKSZ, we cannot verify checksums, which are block-level.
        if verify_checksum && (cnt % BLCKSZ as off_t != 0) {
            ereport!(
                WARNING,
                errmsg!(
                    "could not verify checksum in file \"{}\", block {}: read buffer size {} and page size {} differ",
                    cstr(readfilename),
                    blkno,
                    cnt as c_int,
                    BLCKSZ
                )
            );
            verify_checksum = false;
        }

        // If we hit end-of-file, a concurrent truncation must have occurred.
        // That's not an error condition, because WAL replay will fix things up.
        if cnt == 0 {
            break;
        }

        // Update block number and # of bytes done for next loop iteration.
        blkno += (cnt / BLCKSZ as off_t) as BlockNumber;
        bytes_done += cnt as pgoff_t;

        // Make sure incremental files with block data are properly aligned
        // (header is a multiple of BLCKSZ, blocks are BLCKSZ too).
        Assert!(
            !((!incremental_blocks.is_null() && num_incremental_blocks > 0)
                && (bytes_done % BLCKSZ as pgoff_t != 0))
        );

        // Archive the data we just read.
        bbsink_archive_contents(sink, cnt as Size);

        // Also feed it to the checksum machinery.
        if pg_checksum_update(&mut checksum_ctx, (*sink).bbs_buffer as *mut uint8, cnt as c_int) < 0
        {
            elog!(ERROR, "could not update checksum of base backup");
        }
    }

    // If the file was truncated while we were sending it, pad it with zeros
    while bytes_done < (*statbuf).st_size {
        let remaining = ((*statbuf).st_size - bytes_done) as Size;
        let nbytes = Min((*sink).bbs_buffer_length, remaining);

        MemSet((*sink).bbs_buffer as *mut c_void, 0, nbytes);
        if pg_checksum_update(&mut checksum_ctx, (*sink).bbs_buffer as *mut uint8, nbytes as c_int)
            < 0
        {
            elog!(ERROR, "could not update checksum of base backup");
        }
        bbsink_archive_contents(sink, nbytes);
        bytes_done += nbytes as pgoff_t;
    }

    // Pad to a block boundary, per tar format requirements. (This small piece of
    // data is probably not worth throttling, and is not checksummed because it's
    // not actually part of the file.)
    _tarWritePadding(sink, bytes_done as c_int);

    CloseTransientFile(fd);

    if checksum_failures > 1 {
        // C also: errmsg_plural("file \"%s\" has a total of %d checksum verification failure",
        //                       "file \"%s\" has a total of %d checksum verification failures", ...)
        ereport!(
            WARNING,
            errmsg!(
                "file \"{}\" has a total of {} checksum verification failures",
                cstr(readfilename),
                checksum_failures
            )
        );

        pgstat_prepare_report_checksum_failure(dboid);
        pgstat_report_checksum_failures_in_db(dboid, checksum_failures);
    }

    total_checksum_failures += checksum_failures as c_longlong;

    AddFileToBackupManifest(
        manifest,
        spcoid,
        tarfilename,
        (*statbuf).st_size as Size,
        (*statbuf).st_mtime as pg_time_t,
        &mut checksum_ctx,
    );

    true
}

// Read some more data from the file into the bbsink's buffer, verifying
// checksums as required.
//
// 'offset' is the file offset from which we should begin to read, and 'length'
// is the amount of data that should be read. The actual amount of data read will
// be less than the requested amount if the bbsink's buffer isn't big enough to
// hold it all, or if the underlying file has been truncated. The return value is
// the number of bytes actually read.
//
// 'blkno' is the block number of the first page in the bbsink's buffer relative
// to the start of the relation.
//
// 'verify_checksum' indicates whether we should try to verify checksums for the
// blocks we read. If we do this, we'll update *checksum_failures and issue
// warnings as appropriate.
unsafe fn read_file_data_into_buffer(
    sink: *mut bbsink,
    readfilename: *const c_char,
    fd: c_int,
    offset: off_t,
    length: Size,
    blkno: BlockNumber,
    verify_checksum: bool,
    checksum_failures: *mut c_int,
) -> off_t {
    let mut cnt: off_t;
    let mut i: c_int;
    let mut page: *mut c_char;

    // Try to read some more data.
    cnt = basebackup_read_file(
        fd,
        (*sink).bbs_buffer,
        Min((*sink).bbs_buffer_length, length),
        offset,
        readfilename,
        true,
    ) as off_t;

    // Can't verify checksums if read length is not a multiple of BLCKSZ.
    if !verify_checksum || (cnt % BLCKSZ as off_t) != 0 {
        return cnt;
    }

    // Verify checksum for each block.
    i = 0;
    while (i as off_t) < cnt / BLCKSZ as off_t {
        let reread_cnt: c_int;
        let mut expected_checksum: uint16 = 0;

        page = (*sink).bbs_buffer.add(BLCKSZ * i as usize);

        // If the page is OK, go on to the next one.
        if verify_page_checksum(
            page,
            (*(*sink).bbs_state).startptr,
            blkno + i as BlockNumber,
            &mut expected_checksum,
        ) {
            i += 1;
            continue;
        }

        // Retry the block on the first failure.  It's possible that we read the
        // first 4K page of the block just before postgres updated the entire
        // block so it ends up looking torn to us. If, before we retry the read,
        // the concurrent write of the block finishes, the page LSN will be
        // updated and we'll realize that we should ignore this block.
        //
        // There's no guarantee that this will actually happen, though: the torn
        // write could take an arbitrarily long time to complete. Retrying
        // multiple times wouldn't fix this problem, either, though it would
        // reduce the chances of it happening in practice. The only real fix here
        // seems to be to have some kind of interlock that allows us to wait until
        // we can be certain that no write to the block is in progress. Since we
        // don't have any such thing right now, we just do this and hope for the
        // best.
        reread_cnt = basebackup_read_file(
            fd,
            (*sink).bbs_buffer.add(BLCKSZ * i as usize),
            BLCKSZ,
            offset + BLCKSZ as off_t * i as off_t,
            readfilename,
            false,
        ) as c_int;
        if reread_cnt == 0 {
            // If we hit end-of-file, a concurrent truncation must have occurred,
            // so reduce cnt to reflect only the blocks already processed and
            // break out of this loop.
            cnt = BLCKSZ as off_t * i as off_t;
            break;
        }

        // If the page now looks OK, go on to the next one.
        if verify_page_checksum(
            page,
            (*(*sink).bbs_state).startptr,
            blkno + i as BlockNumber,
            &mut expected_checksum,
        ) {
            i += 1;
            continue;
        }

        // Handle checksum failure.
        *checksum_failures += 1;
        if *checksum_failures <= 5 {
            ereport!(
                WARNING,
                errmsg!(
                    "checksum verification failed in file \"{}\", block {}: calculated {:X} but expected {:X}",
                    cstr(readfilename),
                    blkno + i as BlockNumber,
                    expected_checksum,
                    (*(page as PageHeader)).pd_checksum
                )
            );
        }
        if *checksum_failures == 5 {
            ereport!(
                WARNING,
                errmsg!(
                    "further checksum verification failures in file \"{}\" will not be reported",
                    cstr(readfilename)
                )
            );
        }

        i += 1;
    }

    cnt
}

// Push data into a bbsink.
//
// It's better, when possible, to read data directly into the bbsink's buffer,
// rather than using this function to copy it into the buffer; this function is
// for cases where that approach is not practical.
//
// bytes_done should point to a count of the number of bytes that are currently
// used in the bbsink's buffer. Upon return, the bytes identified by data and
// length will have been copied into the bbsink's buffer, flushing as required,
// and *bytes_done will have been updated accordingly. If the buffer was flushed,
// the previous contents will also have been fed to checksum_ctx.
//
// Note that after one or more calls to this function it is the caller's
// responsibility to perform any required final flush.
unsafe fn push_to_sink(
    sink: *mut bbsink,
    checksum_ctx: *mut pg_checksum_context,
    bytes_done: *mut Size,
    mut data: *mut c_void,
    mut length: Size,
) {
    while length > 0 {
        let bytes_to_copy: Size;

        // We use < here rather than <= so that if the data exactly fills the
        // remaining buffer space, we trigger a flush now.
        if length < (*sink).bbs_buffer_length - *bytes_done {
            // Append remaining data to buffer.
            core::ptr::copy_nonoverlapping(
                data as *const c_char,
                (*sink).bbs_buffer.add(*bytes_done),
                length,
            );
            *bytes_done += length;
            return;
        }

        // Copy until buffer is full and flush it.
        bytes_to_copy = (*sink).bbs_buffer_length - *bytes_done;
        core::ptr::copy_nonoverlapping(
            data as *const c_char,
            (*sink).bbs_buffer.add(*bytes_done),
            bytes_to_copy,
        );
        data = (data as *mut c_char).add(bytes_to_copy) as *mut c_void;
        length -= bytes_to_copy;
        bbsink_archive_contents(sink, (*sink).bbs_buffer_length);
        if pg_checksum_update(
            checksum_ctx,
            (*sink).bbs_buffer as *mut uint8,
            (*sink).bbs_buffer_length as c_int,
        ) < 0
        {
            elog!(ERROR, "could not update checksum");
        }
        *bytes_done = 0;
    }
}

// Try to verify the checksum for the provided page, if it seems appropriate to
// do so.
//
// Returns true if verification succeeds or if we decide not to check it, and
// false if verification fails. When return false, it also sets
// *expected_checksum to the computed value.
unsafe fn verify_page_checksum(
    page: Page,
    start_lsn: XLogRecPtr,
    blkno: BlockNumber,
    expected_checksum: *mut uint16,
) -> bool {
    let phdr: PageHeader;
    let checksum: uint16;

    // Only check pages which have not been modified since the start of the base
    // backup. Otherwise, they might have been written only halfway and the
    // checksum would not be valid.  However, replaying WAL would reinstate the
    // correct page in this case. We also skip completely new pages, since they
    // don't have a checksum yet.
    if PageIsNew(page) || PageGetLSN(page) >= start_lsn {
        return true;
    }

    // Perform the actual checksum calculation.
    checksum = pg_checksum_page(page, blkno);

    // See whether it matches the value from the page.
    phdr = page as PageHeader;
    if (*phdr).pd_checksum == checksum {
        return true;
    }
    *expected_checksum = checksum;
    false
}

unsafe fn _tarWriteHeader(
    sink: *mut bbsink,
    filename: *const c_char,
    linktarget: *const c_char,
    statbuf: *mut stat,
    sizeonly: bool,
) -> int64 {
    let rc: tarError;

    if !sizeonly {
        // As of this writing, the smallest supported block size is 1kB, which is
        // twice TAR_BLOCK_SIZE. Since the buffer size is required to be a
        // multiple of BLCKSZ, it should be safe to assume that the buffer is
        // large enough to fit an entire tar block. We double-check by means of
        // these assertions.
        // C also: StaticAssertDecl(TAR_BLOCK_SIZE <= BLCKSZ, "BLCKSZ too small for tar block");
        Assert!((*sink).bbs_buffer_length >= TAR_BLOCK_SIZE as Size);

        rc = tarCreateHeader(
            (*sink).bbs_buffer,
            filename,
            linktarget,
            (*statbuf).st_size as pgoff_t,
            (*statbuf).st_mode,
            (*statbuf).st_uid,
            (*statbuf).st_gid,
            (*statbuf).st_mtime as time_t,
        );

        match rc {
            TAR_OK => {}
            TAR_NAME_TOO_LONG => {
                // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                ereport!(
                    ERROR,
                    errmsg!("file name too long for tar format: \"{}\"", cstr(filename))
                );
            }
            TAR_SYMLINK_TOO_LONG => {
                // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                ereport!(
                    ERROR,
                    errmsg!(
                        "symbolic link target too long for tar format: file name \"{}\", target \"{}\"",
                        cstr(filename),
                        cstr(linktarget)
                    )
                );
            }
            _ => {
                elog!(ERROR, "unrecognized tar error: {}", rc);
            }
        }

        bbsink_archive_contents(sink, TAR_BLOCK_SIZE as Size);
    }

    TAR_BLOCK_SIZE as int64
}

// Pad with zero bytes out to a multiple of TAR_BLOCK_SIZE.
unsafe fn _tarWritePadding(sink: *mut bbsink, len: c_int) {
    let pad = tarPaddingBytesRequired(len as Size) as c_int;

    // As in _tarWriteHeader, it should be safe to assume that the buffer is
    // large enough that we don't need to do this in multiple chunks.
    Assert!((*sink).bbs_buffer_length >= TAR_BLOCK_SIZE as Size);
    Assert!(pad <= TAR_BLOCK_SIZE);

    if pad > 0 {
        MemSet((*sink).bbs_buffer as *mut c_void, 0, pad as Size);
        bbsink_archive_contents(sink, pad as Size);
    }
}

// If the entry in statbuf is a link, then adjust statbuf to make it look like a
// directory, so that it will be written that way.
unsafe fn convert_link_to_directory(pathbuf: *const c_char, statbuf: *mut stat) {
    let _ = pathbuf;
    // If symlink, write it as a directory anyway
    if S_ISLNK((*statbuf).st_mode) {
        (*statbuf).st_mode = S_IFDIR | pg_dir_create_mode as mode_t;
    }
}

// Read some data from a file, setting a wait event and reporting any error
// encountered.
//
// If partial_read_ok is false, also report an error if the number of bytes read
// is not equal to the number of bytes requested.
//
// Returns the number of bytes read.
unsafe fn basebackup_read_file(
    fd: c_int,
    buf: *mut c_char,
    nbytes: Size,
    offset: off_t,
    filename: *const c_char,
    partial_read_ok: bool,
) -> isize {
    let rc: isize;

    pgstat_report_wait_start(WAIT_EVENT_BASEBACKUP_READ);
    rc = pg_pread(fd, buf as *mut c_void, nbytes, offset);
    pgstat_report_wait_end();

    if rc < 0 {
        // C also: errcode_for_file_access()
        ereport!(
            ERROR,
            errmsg!("could not read file \"{}\": %m", cstr(filename))
        );
    }
    if !partial_read_ok && rc > 0 && rc as Size != nbytes {
        // C also: errcode_for_file_access()
        ereport!(
            ERROR,
            errmsg!(
                "could not read file \"{}\": read {} of {}",
                cstr(filename),
                rc,
                nbytes
            )
        );
    }

    rc
}

// ===========================================================================
// Local POSIX/libc and small-helper definitions.
//
// These back the system-call and C-string surface that basebackup.c uses but
// that has no canonical crate-wide home yet (mirrors the per-file stubs in
// access/transam/xlogarchive.rs and friends).
// ===========================================================================

pub use crate::port::pgstrcasecmp::pg_strcasecmp;
pub use crate::port::port_api::pg_pread;
pub use crate::utils::adt::bool::parse_bool;

// POSIX scalar types (non-WIN32 targets). TODO: dedup with port/tar.rs et al.
#[allow(non_camel_case_types)]
pub type off_t = i64;
#[allow(non_camel_case_types)]
pub type pgoff_t = i64;
#[allow(non_camel_case_types)]
pub type time_t = c_long;
#[allow(non_camel_case_types)]
pub type pg_time_t = int64;
#[allow(non_camel_case_types)]
pub type mode_t = c_uint;

// open(2) flags. PG_BINARY is 0 on non-Windows platforms.
const O_RDONLY: c_int = 0;
const PG_BINARY: c_int = 0;

// errno value for "no such file or directory".
const ENOENT: c_int = 2;

// st_mode test/compose macros (sys/stat.h), with the standard octal values used
// on the platforms PepperDB targets.
const S_IFMT: mode_t = 0o170000;
const S_IFDIR: mode_t = 0o040000;
const S_IFREG: mode_t = 0o100000;
const S_IFLNK: mode_t = 0o120000;
#[inline]
fn S_ISDIR(m: mode_t) -> bool {
    (m & S_IFMT) == S_IFDIR
}
#[inline]
fn S_ISREG(m: mode_t) -> bool {
    (m & S_IFMT) == S_IFREG
}
#[inline]
fn S_ISLNK(m: mode_t) -> bool {
    (m & S_IFMT) == S_IFLNK
}

// relpath.h: InvalidRelFileNumber / RelFileNumberIsValid. STUB (mirrors the
// per-file definitions elsewhere in the crate; RelFileNumber = Oid).
const InvalidRelFileNumber: RelFileNumber = InvalidOid;
#[inline]
fn RelFileNumberIsValid(relnumber: RelFileNumber) -> bool {
    relnumber != InvalidRelFileNumber
}

// c.h: OidIsValid. STUB (mirrors per-file definitions in tcop/utility.rs etc.).
#[inline]
fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

// `struct stat` projection covering exactly the fields basebackup.c touches.
// TODO(pg-port): replace with a faithful sys/stat.h binding crate-wide.
#[repr(C)]
struct stat {
    st_mode: mode_t,
    st_uid: c_uint,
    st_gid: c_uint,
    st_size: off_t,
    st_mtime: time_t,
}

// libc/syscall stubs (the real implementations come from the C runtime; not yet
// wired into this unit). TODO(pg-port): bind to platform libc.
unsafe fn lstat(path: *const c_char, buf: *mut stat) -> c_int {
    let _ = (path, buf);
    unimplemented!("lstat: <sys/stat.h> not yet bound");
}
unsafe fn fstat(fd: c_int, buf: *mut stat) -> c_int {
    let _ = (fd, buf);
    unimplemented!("fstat: <sys/stat.h> not yet bound");
}
unsafe fn readlink(path: *const c_char, buf: *mut c_char, bufsiz: usize) -> isize {
    let _ = (path, buf, bufsiz);
    unimplemented!("readlink: <unistd.h> not yet bound");
}
unsafe fn time(tloc: *mut time_t) -> time_t {
    let _ = tloc;
    unimplemented!("time: <time.h> not yet bound");
}
unsafe fn geteuid() -> c_uint {
    unimplemented!("geteuid: <unistd.h> not yet bound");
}
unsafe fn getegid() -> c_uint {
    unimplemented!("getegid: <unistd.h> not yet bound");
}

// Darwin exposes errno through __error(); read/write it through that.
unsafe fn errno() -> c_int {
    extern "C" {
        fn __error() -> *mut c_int;
    }
    *__error()
}
unsafe fn set_errno(value: c_int) {
    extern "C" {
        fn __error() -> *mut c_int;
    }
    *__error() = value;
}

// libc string primitives used directly (renamed to avoid clashing with the
// crate's port shims). TODO(pg-port): route through a single libc binding.
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" {
        fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    }
    strcmp(a, b)
}
unsafe fn libc_strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    extern "C" {
        fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    }
    strncmp(a, b, n)
}
unsafe fn libc_strlen(s: *const c_char) -> usize {
    extern "C" {
        fn strlen(s: *const c_char) -> usize;
    }
    strlen(s)
}
unsafe fn libc_strspn(s: *const c_char, accept: *const c_char) -> usize {
    extern "C" {
        fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    }
    strspn(s, accept)
}

// strcmp(a + aoff, b + boff): the C source compares WAL filenames from byte 8
// onward (skipping the timeline portion). Operates on raw NUL-terminated names.
unsafe fn strcmp_off<const N: usize, const M: usize>(
    a: &[c_char; N],
    aoff: usize,
    b: &[c_char; M],
    boff: usize,
) -> c_int {
    libc_strcmp(a.as_ptr().add(aoff), b.as_ptr().add(boff))
}

// "global/pg_control" (XLOG_CONTROL_FILE) as a NUL-terminated C buffer.
unsafe fn cstring_xlog_control() -> [c_char; 32] {
    let mut buf: [c_char; 32] = [0; 32];
    snprintf_into(buf.as_mut_ptr(), 32, XLOG_CONTROL_FILE);
    buf
}

// "./" XLOG_CONTROL_FILE, used by sendDir to skip pg_control during the scan.
unsafe fn cstring_dot_xlog_control() -> [c_char; 40] {
    let mut buf: [c_char; 40] = [0; 40];
    snprintf_into(buf.as_mut_ptr(), 40, &format!("./{}", XLOG_CONTROL_FILE));
    buf
}
