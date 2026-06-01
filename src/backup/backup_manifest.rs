//! backup_manifest.c - code for generating and sending a backup manifest

use crate::prelude::*;
type pg_time_t = i64;

use crate::foreach;

use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, enlargeStringInfo, initStringInfo, StringInfoData,
};
use crate::appendStringInfo;

use crate::nodes::pg_list::{lfirst, List, ListCell};

use crate::common::checksum_helper::{
    pg_checksum_context, pg_checksum_final, pg_checksum_type, pg_checksum_type_name,
    CHECKSUM_TYPE_NONE, PG_CHECKSUM_MAX_LENGTH,
};

use crate::common::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_ctx, pg_cryptohash_error, pg_cryptohash_final,
    pg_cryptohash_free, pg_cryptohash_init, pg_cryptohash_update, PG_SHA256,
};

use crate::common::sha2::{PG_SHA256_DIGEST_LENGTH, PG_SHA256_DIGEST_STRING_LENGTH};

use crate::backup::basebackup_sink::{
    bbsink, bbsink_begin_manifest, bbsink_end_manifest, bbsink_manifest_contents,
};

use crate::mb::pg_wchar::{pg_verify_mbstr, PG_UTF8};

use crate::utils::adt::encode::hex_encode;
use crate::utils::adt::json::escape_json_with_len;

use crate::pgtime::{pg_gmtime, pg_strftime};

use crate::access::transam::xlogreader::{TimeLineID, XLogRecPtr, XLogRecPtrIsInvalid};
use crate::access::transam::xlogdefs::LSN_FORMAT_ARGS;

// MAXPGPATH (pg_config_manual.h)
const MAXPGPATH: usize = 1024;

// common/relpath.h
const PG_TBLSPC_DIR: &core::ffi::CStr = c"pg_tblspc";

/*
 * OidIsValid (c.h)
 */
#[inline]
fn OidIsValid(objectId: Oid) -> bool {
    objectId != crate::postgres_ext::InvalidOid
}

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/* ---------------------------------------------------------------------------
 * Types / functions not yet ported - local stubs.
 * ------------------------------------------------------------------------- */

// storage/buffile.h - opaque BufFile.
pub enum BufFile {}

// TODO(pg-port): BufFile not yet ported (storage/buffile.c).
unsafe fn BufFileCreateTemp(_interXact: bool) -> *mut BufFile {
    unimplemented!()
}
// TODO(pg-port): BufFile not yet ported.
unsafe fn BufFileSeek(
    _file: *mut BufFile,
    _fileno: c_int,
    _offset: i64,
    _whence: c_int,
) -> c_int {
    unimplemented!()
}
// TODO(pg-port): BufFile not yet ported.
unsafe fn BufFileReadExact(_file: *mut BufFile, _ptr: *mut c_void, _size: Size) {
    unimplemented!()
}
// TODO(pg-port): BufFile not yet ported.
unsafe fn BufFileWrite(_file: *mut BufFile, _ptr: *const c_void, _size: Size) {
    unimplemented!()
}
// TODO(pg-port): BufFile not yet ported.
unsafe fn BufFileClose(_file: *mut BufFile) {
    unimplemented!()
}

// access/timeline.h - TimeLineHistoryEntry.
#[repr(C)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    pub begin: XLogRecPtr, /* inclusive */
    pub end: XLogRecPtr,   /* exclusive, 0 means infinity */
}

// TODO(pg-port): timeline.c not yet ported.
unsafe fn readTimeLineHistory(_targetTLI: TimeLineID) -> *mut List {
    unimplemented!()
}

// TODO(pg-port): xlog.c not yet ported.
unsafe fn GetSystemIdentifier() -> uint64 {
    unimplemented!()
}

/* SEEK_SET (stdio.h) */
const SEEK_SET: c_int = 0;

/*
 * manifest_option (backup_manifest.h)
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum manifest_option {
    MANIFEST_OPTION_YES,
    MANIFEST_OPTION_NO,
    MANIFEST_OPTION_FORCE_ENCODE,
}
pub use manifest_option::*;
pub type backup_manifest_option = manifest_option;

/*
 * backup_manifest_info (backup_manifest.h)
 */
#[repr(C)]
pub struct backup_manifest_info {
    pub buffile: *mut BufFile,
    pub checksum_type: pg_checksum_type,
    pub manifest_ctx: *mut pg_cryptohash_ctx,
    pub manifest_size: uint64,
    pub force_encode: bool,
    pub first_file: bool,
    pub still_checksumming: bool,
}

/*
 * Does the user want a backup manifest?
 *
 * It's simplest to always have a manifest_info object, so that we don't need
 * checks for NULL pointers in too many places. However, if the user doesn't
 * want a manifest, we set manifest->buffile to NULL.
 */
#[inline]
unsafe fn IsManifestEnabled(manifest: *mut backup_manifest_info) -> bool {
    !(*manifest).buffile.is_null()
}

/*
 * Initialize state so that we can construct a backup manifest.
 *
 * NB: Although the checksum type for the data files is configurable, the
 * checksum for the manifest itself always uses SHA-256. See comments in
 * SendBackupManifest.
 */
pub unsafe fn InitializeBackupManifest(
    manifest: *mut backup_manifest_info,
    want_manifest: backup_manifest_option,
    manifest_checksum_type: pg_checksum_type,
) {
    core::ptr::write_bytes(manifest, 0, 1);
    (*manifest).checksum_type = manifest_checksum_type;

    if want_manifest == MANIFEST_OPTION_NO {
        (*manifest).buffile = null_mut();
    } else {
        (*manifest).buffile = BufFileCreateTemp(false);
        (*manifest).manifest_ctx = pg_cryptohash_create(PG_SHA256);
        if pg_cryptohash_init((*manifest).manifest_ctx) < 0 {
            elog!(
                ERROR,
                "failed to initialize checksum of backup manifest"
            );
        }
    }

    (*manifest).manifest_size = 0;
    (*manifest).force_encode = want_manifest == MANIFEST_OPTION_FORCE_ENCODE;
    (*manifest).first_file = true;
    (*manifest).still_checksumming = true;

    if want_manifest != MANIFEST_OPTION_NO {
        AppendToManifest(
            manifest,
            format!(
                "{{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\
                 \"System-Identifier\": {},\n\
                 \"Files\": [",
                GetSystemIdentifier()
            ),
        );
    }
}

/*
 * Free resources assigned to a backup manifest constructed.
 */
pub unsafe fn FreeBackupManifest(manifest: *mut backup_manifest_info) {
    pg_cryptohash_free((*manifest).manifest_ctx);
    (*manifest).manifest_ctx = null_mut();
}

/*
 * Add an entry to the backup manifest for a file.
 */
pub unsafe fn AddFileToBackupManifest(
    manifest: *mut backup_manifest_info,
    spcoid: Oid,
    mut pathname: *const c_char,
    size: Size,
    mtime: pg_time_t,
    checksum_ctx: *mut pg_checksum_context,
) {
    let mut pathbuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let pathlen: c_int;
    let mut buf: StringInfoData = core::mem::zeroed();

    if !IsManifestEnabled(manifest) {
        return;
    }

    /*
     * If this file is part of a tablespace, the pathname passed to this
     * function will be relative to the tar file that contains it. We want the
     * pathname relative to the data directory (ignoring the intermediate
     * symlink traversal).
     */
    if OidIsValid(spcoid) {
        snprintf(
            pathbuf.as_mut_ptr(),
            core::mem::size_of_val(&pathbuf),
            c"%s/%u/%s".as_ptr(),
            PG_TBLSPC_DIR.as_ptr(),
            spcoid as c_uint,
            pathname,
        );
        pathname = pathbuf.as_ptr();
    }

    /*
     * Each file's entry needs to be separated from any entry that follows by
     * a comma, but there's no comma before the first one or after the last
     * one. To make that work, adding a file to the manifest starts by
     * terminating the most recently added line, with a comma if appropriate,
     * but does not terminate the line inserted for this file.
     */
    initStringInfo(&mut buf);
    if (*manifest).first_file {
        appendStringInfoChar(&mut buf, b'\n' as c_char);
        (*manifest).first_file = false;
    } else {
        appendStringInfoString(&mut buf, c",\n".as_ptr());
    }

    /*
     * Write the relative pathname to this file out to the manifest. The
     * manifest is always stored in UTF-8, so we have to encode paths that are
     * not valid in that encoding.
     */
    pathlen = strlen(pathname) as c_int;
    if !(*manifest).force_encode
        && pg_verify_mbstr(PG_UTF8 as c_int, pathname, pathlen, true)
    {
        appendStringInfoString(&mut buf, c"{ \"Path\": ".as_ptr());
        escape_json_with_len(&mut buf, pathname, pathlen);
        appendStringInfoString(&mut buf, c", ".as_ptr());
    } else {
        appendStringInfoString(&mut buf, c"{ \"Encoded-Path\": \"".as_ptr());
        enlargeStringInfo(&mut buf, 2 * pathlen);
        buf.len += hex_encode(
            pathname,
            pathlen as usize,
            buf.data.add(buf.len as usize),
        ) as c_int;
        appendStringInfoString(&mut buf, c"\", ".as_ptr());
    }

    appendStringInfo!(&mut buf, "\"Size\": {}, ", size);

    /*
     * Convert last modification time to a string and append it to the
     * manifest. Since it's not clear what time zone to use and since time
     * zone definitions can change, possibly causing confusion, use GMT
     * always.
     */
    appendStringInfoString(&mut buf, c"\"Last-Modified\": \"".as_ptr());
    enlargeStringInfo(&mut buf, 128);
    buf.len += pg_strftime(
        buf.data.add(buf.len as usize),
        128,
        c"%Y-%m-%d %H:%M:%S %Z".as_ptr(),
        pg_gmtime(&mtime),
    ) as c_int;
    appendStringInfoChar(&mut buf, b'"' as c_char);

    /* Add checksum information. */
    if (*checksum_ctx).r#type != CHECKSUM_TYPE_NONE {
        let mut checksumbuf: [uint8; PG_CHECKSUM_MAX_LENGTH] = [0; PG_CHECKSUM_MAX_LENGTH];
        let checksumlen: c_int;

        checksumlen = pg_checksum_final(checksum_ctx, checksumbuf.as_mut_ptr());
        if checksumlen < 0 {
            elog!(
                ERROR,
                "could not finalize checksum of file \"{}\"",
                cstr(pathname)
            );
        }

        appendStringInfo!(
            &mut buf,
            ", \"Checksum-Algorithm\": \"{}\", \"Checksum\": \"",
            cstr(pg_checksum_type_name((*checksum_ctx).r#type))
        );
        enlargeStringInfo(&mut buf, 2 * checksumlen);
        buf.len += hex_encode(
            checksumbuf.as_ptr() as *const c_char,
            checksumlen as usize,
            buf.data.add(buf.len as usize),
        ) as c_int;
        appendStringInfoChar(&mut buf, b'"' as c_char);
    }

    /* Close out the object. */
    appendStringInfoString(&mut buf, c" }".as_ptr());

    /* OK, add it to the manifest. */
    AppendStringToManifest(manifest, buf.data);

    /* Avoid leaking memory. */
    pfree(buf.data as *mut c_void);
}

/*
 * Add information about the WAL that will need to be replayed when restoring
 * this backup to the manifest.
 */
pub unsafe fn AddWALInfoToBackupManifest(
    manifest: *mut backup_manifest_info,
    startptr: XLogRecPtr,
    starttli: TimeLineID,
    mut endptr: XLogRecPtr,
    endtli: TimeLineID,
) {
    let timelines: *mut List;
    let mut first_wal_range = true;
    let mut found_start_timeline = false;

    if !IsManifestEnabled(manifest) {
        return;
    }

    /* Terminate the list of files. */
    AppendStringToManifest(manifest, c"\n],\n".as_ptr());

    /* Read the timeline history for the ending timeline. */
    timelines = readTimeLineHistory(endtli);

    /* Start a list of LSN ranges. */
    AppendStringToManifest(manifest, c"\"WAL-Ranges\": [\n".as_ptr());

    foreach!(lc, timelines, {
        let entry = lfirst(current_cell(&lc)) as *mut TimeLineHistoryEntry;
        let tl_beginptr: XLogRecPtr;

        /*
         * We only care about timelines that were active during the backup.
         * Skip any that ended before the backup started. (Note that if
         * entry->end is InvalidXLogRecPtr, it means that the timeline has not
         * yet ended.)
         */
        if !XLogRecPtrIsInvalid((*entry).end) && (*entry).end < startptr {
            continue;
        }

        /*
         * Because the timeline history file lists newer timelines before
         * older ones, the first timeline we encounter that is new enough to
         * matter ought to match the ending timeline of the backup.
         */
        if first_wal_range && endtli != (*entry).tli {
            ereport!(
                ERROR,
                &format!(
                    "expected end timeline {} but found timeline {}",
                    endtli,
                    (*entry).tli
                )
            );
        }

        /*
         * If this timeline entry matches with the timeline on which the
         * backup started, WAL needs to be checked from the start LSN of the
         * backup.  If this entry refers to a newer timeline, WAL needs to be
         * checked since the beginning of this timeline, so use the LSN where
         * the timeline began.
         */
        if starttli == (*entry).tli {
            tl_beginptr = startptr;
        } else {
            tl_beginptr = (*entry).begin;

            /*
             * If we reach a TLI that has no valid beginning LSN, there can't
             * be any more timelines in the history after this point, so we'd
             * better have arrived at the expected starting TLI. If not,
             * something's gone horribly wrong.
             */
            if XLogRecPtrIsInvalid((*entry).begin) {
                ereport!(
                    ERROR,
                    &format!(
                        "expected start timeline {} but found timeline {}",
                        starttli,
                        (*entry).tli
                    )
                );
            }
        }

        let (begin_hi, begin_lo) = LSN_FORMAT_ARGS(tl_beginptr);
        let (end_hi, end_lo) = LSN_FORMAT_ARGS(endptr);
        AppendToManifest(
            manifest,
            format!(
                "{}{{ \"Timeline\": {}, \"Start-LSN\": \"{:X}/{:X}\", \"End-LSN\": \"{:X}/{:X}\" }}",
                if first_wal_range { "" } else { ",\n" },
                (*entry).tli,
                begin_hi,
                begin_lo,
                end_hi,
                end_lo
            ),
        );

        if starttli == (*entry).tli {
            found_start_timeline = true;
            break;
        }

        endptr = (*entry).begin;
        first_wal_range = false;
    });

    /*
     * The last entry in the timeline history for the ending timeline should
     * be the ending timeline itself. Verify that this is what we observed.
     */
    if !found_start_timeline {
        ereport!(
            ERROR,
            &format!(
                "start timeline {} not found in history of timeline {}",
                starttli, endtli
            )
        );
    }

    /* Terminate the list of WAL ranges. */
    AppendStringToManifest(manifest, c"\n],\n".as_ptr());
}

/*
 * Finalize the backup manifest, and send it to the client.
 */
pub unsafe fn SendBackupManifest(manifest: *mut backup_manifest_info, sink: *mut bbsink) {
    let mut checksumbuf: [uint8; PG_SHA256_DIGEST_LENGTH] = [0; PG_SHA256_DIGEST_LENGTH];
    let mut checksumstringbuf: [c_char; PG_SHA256_DIGEST_STRING_LENGTH] =
        [0; PG_SHA256_DIGEST_STRING_LENGTH];
    let mut manifest_bytes_done: Size = 0;

    if !IsManifestEnabled(manifest) {
        return;
    }

    /*
     * Append manifest checksum, so that the problems with the manifest itself
     * can be detected.
     *
     * We always use SHA-256 for this, regardless of what algorithm is chosen
     * for checksumming the files.  If we ever want to make the checksum
     * algorithm used for the manifest file variable, the client will need a
     * way to figure out which algorithm to use as close to the beginning of
     * the manifest file as possible, to avoid having to read the whole thing
     * twice.
     */
    (*manifest).still_checksumming = false;
    if pg_cryptohash_final(
        (*manifest).manifest_ctx,
        checksumbuf.as_mut_ptr(),
        core::mem::size_of_val(&checksumbuf),
    ) < 0
    {
        elog!(
            ERROR,
            "failed to finalize checksum of backup manifest"
        );
    }
    AppendStringToManifest(manifest, c"\"Manifest-Checksum\": \"".as_ptr());

    hex_encode(
        checksumbuf.as_ptr() as *const c_char,
        core::mem::size_of_val(&checksumbuf),
        checksumstringbuf.as_mut_ptr(),
    );
    checksumstringbuf[PG_SHA256_DIGEST_STRING_LENGTH - 1] = b'\0' as c_char;

    AppendStringToManifest(manifest, checksumstringbuf.as_ptr());
    AppendStringToManifest(manifest, c"\"}\n".as_ptr());

    /*
     * We've written all the data to the manifest file.  Rewind the file so
     * that we can read it all back.
     */
    if BufFileSeek((*manifest).buffile, 0, 0, SEEK_SET) != 0 {
        ereport!(ERROR, "could not rewind temporary file");
    }

    /*
     * Send the backup manifest.
     */
    bbsink_begin_manifest(sink);
    while manifest_bytes_done < (*manifest).manifest_size as Size {
        let bytes_to_read: Size;

        bytes_to_read = Min(
            (*sink).bbs_buffer_length,
            (*manifest).manifest_size as Size - manifest_bytes_done,
        );
        BufFileReadExact(
            (*manifest).buffile,
            (*sink).bbs_buffer as *mut c_void,
            bytes_to_read,
        );
        bbsink_manifest_contents(sink, bytes_to_read);
        manifest_bytes_done += bytes_to_read;
    }
    bbsink_end_manifest(sink);

    /* Release resources */
    BufFileClose((*manifest).buffile);
}

/*
 * Append a cstring to the manifest.
 */
unsafe fn AppendStringToManifest(manifest: *mut backup_manifest_info, s: *const c_char) {
    let len = strlen(s) as c_int;

    Assert!(!manifest.is_null());
    if (*manifest).still_checksumming {
        if pg_cryptohash_update((*manifest).manifest_ctx, s as *const uint8, len as Size) < 0 {
            elog!(
                ERROR,
                "failed to update checksum of backup manifest"
            );
        }
    }
    BufFileWrite((*manifest).buffile, s as *const c_void, len as Size);
    (*manifest).manifest_size += len as uint64;
}

/*
 * Convenience helper for appending data to the backup manifest.  In C this is
 * a macro that psprintf()s and then AppendStringToManifest()s; here the caller
 * passes a pre-formatted Rust String.
 */
unsafe fn AppendToManifest(manifest: *mut backup_manifest_info, s: String) {
    let cs = std::ffi::CString::new(s).unwrap();
    AppendStringToManifest(manifest, cs.as_ptr());
}

/*
 * Min (c.h)
 */
#[inline]
fn Min(x: Size, y: Size) -> Size {
    if x < y {
        x
    } else {
        y
    }
}

/*
 * Read the current ListCell from a foreach loop state (pg_list.h current_cell).
 */
#[inline]
unsafe fn current_cell(state: &crate::nodes::pg_list::ForEachState) -> *mut ListCell {
    (*state.l).elements.add(state.i as usize)
}

/*
 * Helper to read a NUL-terminated C string for {} formatting in elog!/ereport!.
 */
#[inline]
unsafe fn cstr(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "(null)";
    }
    let len = strlen(s);
    core::str::from_utf8_unchecked(core::slice::from_raw_parts(s as *const u8, len))
}
