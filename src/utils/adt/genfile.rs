//! src/backend/utils/adt/genfile.c
//!
//! genfile.c
//!		Functions for direct access to files
//!
//!
//! Copyright (c) 2004-2025, PostgreSQL Global Development Group
//!
//! Author: Andreas Pflug <pgadmin@pse-consulting.de>
//!
//! IDENTIFICATION
//!	  src/backend/utils/adt/genfile.c

use crate::prelude::*;

// PG_RETURN_* are #[macro_export] macros living at the crate root (defined in
// utils/fmgr.rs); import them so the `!`-call sites below resolve.
use crate::{PG_RETURN_BYTEA_P, PG_RETURN_DATUM, PG_RETURN_NULL, PG_RETURN_TEXT_P};

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, Size};

// ---------------------------------------------------------------------------
// Local stubs / external type placeholders
// ---------------------------------------------------------------------------

type text = c_void;
type bytea = c_void;
type HeapTuple = *mut c_void;
type TupleDesc = *mut c_void;
type Oid = crate::postgres_ext::Oid;
type AttrNumber = crate::access::attnum::AttrNumber;
type TimestampTz = crate::miscadmin::TimestampTz;

type FunctionCallInfo = *mut c_void;
type ReturnSetInfo = c_void;

// FILE / DIR / dirent are opaque C types
type FILE = c_void;
type DIR = c_void;

#[repr(C)]
struct dirent {
    _opaque: [u8; 0],
}

#[repr(C)]
struct stat {
    st_size: i64,
    st_atime: i64,
    st_mtime: i64,
    st_ctime: i64,
    st_mode: u32,
}

// StringInfo (stub mirror of stringinfo.c layout)
#[repr(C)]
struct StringInfoData {
    data: *mut c_char,
    len: c_int,
    maxlen: c_int,
    cursor: c_int,
}

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// errno access stub
unsafe fn get_errno() -> c_int {
    unimplemented!() // TODO: utils/adt/genfile.c (errno)
}

const ENOENT: c_int = 2;

// Constants
const VARHDRSZ: usize = 4;
// MaxAllocSize comes from the prelude (crate::utils::memutils::MaxAllocSize).
const MAXPGPATH: usize = crate::pg_config_manual::MAXPGPATH;

const PG_BINARY_R: *const c_char = c"rb".as_ptr();

const MIN_READ_SIZE: usize = 4096;

// SRF / materialize flags
const MAT_SRF_USE_EXPECTED_DESC: c_int = 0x01;

// Type OIDs (catalog/pg_type)
const INT8OID: Oid = 20;
const TIMESTAMPTZOID: Oid = 1184;
const BOOLOID: Oid = 16;

// Role OID (catalog/pg_authid)
const ROLE_PG_READ_SERVER_FILES: Oid = 4569;

// Tablespace OIDs / syscache id (catalog/pg_tablespace_d, utils/syscache)
const DEFAULTTABLESPACE_OID: Oid = 1663;
const TABLESPACEOID: c_int = 69; // TODO: utils/syscache cache id

// WAL / dir path constants (access/xlog_internal, replication/slot, postmaster/syslogger)
const XLOGDIR: &str = "pg_wal";
const PG_LOGICAL_SNAPSHOTS_DIR: *const c_char = c"pg_logical/snapshots".as_ptr();
const PG_LOGICAL_MAPPINGS_DIR: *const c_char = c"pg_logical/mappings".as_ptr();
const PG_REPLSLOT_DIR: *const c_char = c"pg_replslot".as_ptr();

// External globals
extern "C" {
    static DataDir: *const c_char;
    static Log_directory: *const c_char;
}

// ---------------------------------------------------------------------------
// Stub helpers for unported dependencies
// ---------------------------------------------------------------------------

unsafe fn text_to_cstring(_arg: *mut text) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn canonicalize_path(_path: *mut c_char) {
    unimplemented!() // TODO: port/path.c
}
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!() // TODO: utils/adt/acl.c
}
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn is_absolute_path(_path: *const c_char) -> bool {
    unimplemented!() // TODO: port/path.c
}
unsafe fn path_is_prefix_of_path(_path1: *const c_char, _path2: *const c_char) -> bool {
    unimplemented!() // TODO: port/path.c
}
unsafe fn path_is_relative_and_below_cwd(_path: *const c_char) -> bool {
    unimplemented!() // TODO: port/path.c
}

unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut FILE {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn FreeFile(_file: *mut FILE) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn fseeko(_file: *mut FILE, _offset: i64, _whence: c_int) -> c_int {
    unimplemented!() // TODO: libc fseeko
}
unsafe fn fread(_ptr: *mut c_void, _size: usize, _nmemb: usize, _file: *mut FILE) -> usize {
    unimplemented!() // TODO: libc fread
}
unsafe fn feof(_file: *mut FILE) -> c_int {
    unimplemented!() // TODO: libc feof
}
unsafe fn ferror(_file: *mut FILE) -> c_int {
    unimplemented!() // TODO: libc ferror
}

const SEEK_SET: c_int = 0;
const SEEK_END: c_int = 2;

unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn enlargeStringInfo(_str: *mut StringInfoData, _needed: c_int) {
    unimplemented!() // TODO: lib/stringinfo.c
}

unsafe fn VARDATA(_buf: *mut bytea) -> *mut c_char {
    unimplemented!() // TODO: c.h varlena macros
}
unsafe fn VARSIZE(_buf: *mut bytea) -> c_int {
    unimplemented!() // TODO: c.h varlena macros
}
unsafe fn SET_VARSIZE(_buf: *mut bytea, _len: usize) {
    unimplemented!() // TODO: c.h varlena macros
}

unsafe fn pg_verifymbstr(_mbstr: *const c_char, _len: c_int, _noerror: bool) -> bool {
    unimplemented!() // TODO: mb/mbutils.c
}

unsafe fn stat_fn(_path: *const c_char, _buf: *mut stat) -> c_int {
    unimplemented!() // TODO: libc stat
}

unsafe fn S_ISDIR(_mode: u32) -> bool {
    unimplemented!() // TODO: sys/stat.h
}
unsafe fn S_ISREG(_mode: u32) -> bool {
    unimplemented!() // TODO: sys/stat.h
}

unsafe fn time_t_to_timestamptz(_tm: i64) -> TimestampTz {
    unimplemented!() // TODO: utils/adt/timestamp.c
}
unsafe fn TimestampTzGetDatum(_t: TimestampTz) -> Datum {
    unimplemented!() // TODO: utils/adt/timestamp.h
}

unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attnum: AttrNumber,
    _attname: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn heap_form_tuple(_tupdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO: funcapi.h
}

unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn dirent_d_name(_de: *mut dirent) -> *mut c_char {
    unimplemented!() // TODO: dirent.h d_name field
}

unsafe fn InitMaterializedSRF(fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn tuplestore_putvalues(
    _state: *mut c_void,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}
unsafe fn rsinfo_setResult(_rsinfo: *mut ReturnSetInfo) -> *mut c_void {
    unimplemented!() // TODO: nodes/execnodes.h ReturnSetInfo
}
unsafe fn rsinfo_setDesc(_rsinfo: *mut ReturnSetInfo) -> TupleDesc {
    unimplemented!() // TODO: nodes/execnodes.h ReturnSetInfo
}
unsafe fn fcinfo_resultinfo(fcinfo: FunctionCallInfo) -> *mut ReturnSetInfo {
    unimplemented!() // TODO: fmgr.h FunctionCallInfo
}

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/builtins.h
}

unsafe fn SearchSysCacheExists1(_cacheid: c_int, _key1: Datum) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn TempTablespacePath(_path: *mut c_char, _tablespace: Oid) {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn SearchNamedReplicationSlot(_name: *const c_char, _need_lock: bool) -> bool {
    unimplemented!() // TODO: replication/slot.c
}

// PG_FUNCTION_ARGS helpers
unsafe fn PG_GETARG_TEXT_PP(_n: c_int) -> *mut text {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_GETARG_INT64(_n: c_int) -> int64 {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_GETARG_BOOL(_n: c_int) -> bool {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_GETARG_OID(_n: c_int) -> Oid {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_ARGISNULL(_n: c_int) -> bool {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_NARGS() -> c_int {
    unimplemented!() // TODO: fmgr.h
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/*
 * Convert a "text" filename argument to C string, and check it's allowable.
 *
 * Filename may be absolute or relative to the DataDir, but we only allow
 * absolute paths that match DataDir or Log_directory.
 *
 * This does a privilege check against the 'pg_read_server_files' role, so
 * this function is really only appropriate for callers who are only checking
 * 'read' access.  Do not use this function if you are looking for a check
 * for 'write' or 'program' access without updating it to access the type
 * of check as an argument and checking the appropriate role membership.
 */
unsafe fn convert_and_check_filename(arg: *mut text) -> *mut c_char {
    let filename: *mut c_char;

    filename = text_to_cstring(arg);
    canonicalize_path(filename); /* filename can change length here */

    /*
     * Roles with privileges of the 'pg_read_server_files' role are allowed to
     * access any files on the server as the PG user, so no need to do any
     * further checks here.
     */
    if has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES) {
        return filename;
    }

    /*
     * User isn't a member of the pg_read_server_files role, so check if it's
     * allowable
     */
    if is_absolute_path(filename) {
        /*
         * Allow absolute paths if within DataDir or Log_directory, even
         * though Log_directory might be outside DataDir.
         */
        if !path_is_prefix_of_path(DataDir, filename)
            && (!is_absolute_path(Log_directory)
                || !path_is_prefix_of_path(Log_directory, filename))
        {
            ereport!(ERROR, "absolute path not allowed");
        }
    } else if !path_is_relative_and_below_cwd(filename) {
        ereport!(ERROR, "path must be in or below the data directory");
    }

    filename
}

/*
 * Read a section of a file, returning it as bytea
 *
 * Caller is responsible for all permissions checking.
 *
 * We read the whole of the file when bytes_to_read is negative.
 */
unsafe fn read_binary_file(
    filename: *const c_char,
    seek_offset: int64,
    bytes_to_read: int64,
    missing_ok: bool,
) -> *mut bytea {
    let buf: *mut bytea;
    let mut nbytes: usize = 0;
    let file: *mut FILE;

    /* clamp request size to what we can actually deliver */
    if bytes_to_read > (MaxAllocSize - VARHDRSZ) as int64 {
        ereport!(ERROR, "requested length too large");
    }

    file = AllocateFile(filename, PG_BINARY_R);
    if file.is_null() {
        if missing_ok && get_errno() == ENOENT {
            return std::ptr::null_mut();
        } else {
            elog!(
                ERROR,
                "could not open file \"{}\" for reading: %m",
                cstr_display(filename)
            );
            unreachable!();
        }
    }

    if fseeko(
        file,
        seek_offset,
        if seek_offset >= 0 { SEEK_SET } else { SEEK_END },
    ) != 0
    {
        elog!(ERROR, "could not seek in file \"{}\": %m", cstr_display(filename));
        unreachable!();
    }

    if bytes_to_read >= 0 {
        /* If passed explicit read size just do it */
        buf = palloc(bytes_to_read as Size + VARHDRSZ) as *mut bytea;

        nbytes = fread(VARDATA(buf) as *mut c_void, 1, bytes_to_read as usize, file);
    } else {
        /* Negative read size, read rest of file */
        let mut sbuf: StringInfoData = std::mem::zeroed();

        initStringInfo(&mut sbuf);
        /* Leave room in the buffer for the varlena length word */
        sbuf.len += VARHDRSZ as c_int;
        Assert!((sbuf.len as usize) < sbuf.maxlen as usize);

        while !(feof(file) != 0 || ferror(file) != 0) {
            let rbytes: usize;

            /* Minimum amount to read at a time */
            // #define MIN_READ_SIZE 4096

            /*
             * If not at end of file, and sbuf.len is equal to MaxAllocSize -
             * 1, then either the file is too large, or there is nothing left
             * to read. Attempt to read one more byte to see if the end of
             * file has been reached. If not, the file is too large; we'd
             * rather give the error message for that ourselves.
             */
            if sbuf.len as usize == MaxAllocSize - 1 {
                let mut rbuf: [c_char; 1] = [0];

                if fread(rbuf.as_mut_ptr() as *mut c_void, 1, 1, file) != 0 || feof(file) == 0 {
                    ereport!(ERROR, "file length too large");
                } else {
                    break;
                }
            }

            /* OK, ensure that we can read at least MIN_READ_SIZE */
            enlargeStringInfo(&mut sbuf, MIN_READ_SIZE as c_int);

            /*
             * stringinfo.c likes to allocate in powers of 2, so it's likely
             * that much more space is available than we asked for.  Use all
             * of it, rather than making more fread calls than necessary.
             */
            rbytes = fread(
                sbuf.data.add(sbuf.len as usize) as *mut c_void,
                1,
                (sbuf.maxlen - sbuf.len - 1) as usize,
                file,
            );
            sbuf.len += rbytes as c_int;
            nbytes += rbytes;
        }

        /* Now we can commandeer the stringinfo's buffer as the result */
        buf = sbuf.data as *mut bytea;
    }

    if ferror(file) != 0 {
        elog!(ERROR, "could not read file \"{}\": %m", cstr_display(filename));
        unreachable!();
    }

    SET_VARSIZE(buf, nbytes + VARHDRSZ);

    FreeFile(file);

    buf
}

/*
 * Similar to read_binary_file, but we verify that the contents are valid
 * in the database encoding.
 */
unsafe fn read_text_file(
    filename: *const c_char,
    seek_offset: int64,
    bytes_to_read: int64,
    missing_ok: bool,
) -> *mut text {
    let buf: *mut bytea;

    buf = read_binary_file(filename, seek_offset, bytes_to_read, missing_ok);

    if !buf.is_null() {
        /* Make sure the input is valid */
        pg_verifymbstr(VARDATA(buf), VARSIZE(buf) - VARHDRSZ as c_int, false);

        /* OK, we can cast it to text safely */
        buf as *mut text
    } else {
        std::ptr::null_mut()
    }
}

/*
 * Read a section of a file, returning it as text
 *
 * No superuser check done here- instead privileges are handled by the
 * GRANT system.
 *
 * If read_to_eof is true, bytes_to_read must be -1, otherwise negative values
 * are not allowed for bytes_to_read.
 */
unsafe fn pg_read_file_common(
    filename_t: *mut text,
    seek_offset: int64,
    bytes_to_read: int64,
    read_to_eof: bool,
    missing_ok: bool,
) -> *mut text {
    if read_to_eof {
        Assert!(bytes_to_read == -1);
    } else if bytes_to_read < 0 {
        ereport!(ERROR, "requested length cannot be negative");
    }

    read_text_file(
        convert_and_check_filename(filename_t),
        seek_offset,
        bytes_to_read,
        missing_ok,
    )
}

/*
 * Read a section of a file, returning it as bytea
 *
 * Parameters are interpreted the same as pg_read_file_common().
 */
unsafe fn pg_read_binary_file_common(
    filename_t: *mut text,
    seek_offset: int64,
    bytes_to_read: int64,
    read_to_eof: bool,
    missing_ok: bool,
) -> *mut bytea {
    if read_to_eof {
        Assert!(bytes_to_read == -1);
    } else if bytes_to_read < 0 {
        ereport!(ERROR, "requested length cannot be negative");
    }

    read_binary_file(
        convert_and_check_filename(filename_t),
        seek_offset,
        bytes_to_read,
        missing_ok,
    )
}

/*
 * Wrapper functions for the variants of SQL functions pg_read_file() and
 * pg_read_binary_file().
 *
 * These are necessary to pass the sanity check in opr_sanity, which checks
 * that all built-in functions that share the implementing C function take
 * the same number of arguments.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_read_file_off_len(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let seek_offset: int64 = PG_GETARG_INT64(1);
    let bytes_to_read: int64 = PG_GETARG_INT64(2);
    let ret: *mut text;

    ret = pg_read_file_common(filename_t, seek_offset, bytes_to_read, false, false);
    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_file_off_len_missing(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let seek_offset: int64 = PG_GETARG_INT64(1);
    let bytes_to_read: int64 = PG_GETARG_INT64(2);
    let missing_ok: bool = PG_GETARG_BOOL(3);
    let ret: *mut text;

    ret = pg_read_file_common(filename_t, seek_offset, bytes_to_read, false, missing_ok);

    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_file_all(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let ret: *mut text;

    ret = pg_read_file_common(filename_t, 0, -1, true, false);

    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_file_all_missing(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let missing_ok: bool = PG_GETARG_BOOL(1);
    let ret: *mut text;

    ret = pg_read_file_common(filename_t, 0, -1, true, missing_ok);

    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_binary_file_off_len(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let seek_offset: int64 = PG_GETARG_INT64(1);
    let bytes_to_read: int64 = PG_GETARG_INT64(2);
    let ret: *mut text;

    ret = pg_read_binary_file_common(filename_t, seek_offset, bytes_to_read, false, false)
        as *mut text;
    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_BYTEA_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_binary_file_off_len_missing(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let seek_offset: int64 = PG_GETARG_INT64(1);
    let bytes_to_read: int64 = PG_GETARG_INT64(2);
    let missing_ok: bool = PG_GETARG_BOOL(3);
    let ret: *mut text;

    ret = pg_read_binary_file_common(filename_t, seek_offset, bytes_to_read, false, missing_ok)
        as *mut text;
    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_BYTEA_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_binary_file_all(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let ret: *mut text;

    ret = pg_read_binary_file_common(filename_t, 0, -1, true, false) as *mut text;

    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_BYTEA_P!(ret)
}

#[no_mangle]
pub unsafe extern "C" fn pg_read_binary_file_all_missing(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let missing_ok: bool = PG_GETARG_BOOL(1);
    let ret: *mut text;

    ret = pg_read_binary_file_common(filename_t, 0, -1, true, missing_ok) as *mut text;

    if ret.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_BYTEA_P!(ret)
}

/*
 * stat a file
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_file(fcinfo: FunctionCallInfo) -> Datum {
    let filename_t: *mut text = PG_GETARG_TEXT_PP(0);
    let filename: *mut c_char;
    let mut fst: stat = std::mem::zeroed();
    let mut values: [Datum; 6] = [0; 6];
    let mut isnull: [bool; 6] = [false; 6];
    let tuple: HeapTuple;
    let tupdesc: TupleDesc;
    let mut missing_ok: bool = false;

    /* check the optional argument */
    if PG_NARGS() == 2 {
        missing_ok = PG_GETARG_BOOL(1);
    }

    filename = convert_and_check_filename(filename_t);

    if stat_fn(filename, &mut fst) < 0 {
        if missing_ok && get_errno() == ENOENT {
            PG_RETURN_NULL!(fcinfo);
        } else {
            elog!(ERROR, "could not stat file \"{}\": %m", cstr_display(filename));
            unreachable!();
        }
    }

    /*
     * This record type had better match the output parameters declared for me
     * in pg_proc.h.
     */
    tupdesc = CreateTemplateTupleDesc(6);
    TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"size".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(
        tupdesc,
        2 as AttrNumber,
        c"access".as_ptr(),
        TIMESTAMPTZOID,
        -1,
        0,
    );
    TupleDescInitEntry(
        tupdesc,
        3 as AttrNumber,
        c"modification".as_ptr(),
        TIMESTAMPTZOID,
        -1,
        0,
    );
    TupleDescInitEntry(
        tupdesc,
        4 as AttrNumber,
        c"change".as_ptr(),
        TIMESTAMPTZOID,
        -1,
        0,
    );
    TupleDescInitEntry(
        tupdesc,
        5 as AttrNumber,
        c"creation".as_ptr(),
        TIMESTAMPTZOID,
        -1,
        0,
    );
    TupleDescInitEntry(tupdesc, 6 as AttrNumber, c"isdir".as_ptr(), BOOLOID, -1, 0);
    BlessTupleDesc(tupdesc);

    memset(
        isnull.as_mut_ptr() as *mut c_void,
        false as c_int,
        std::mem::size_of_val(&isnull),
    );

    values[0] = Int64GetDatum(fst.st_size as int64);
    values[1] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_atime));
    values[2] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_mtime));
    /* Unix has file status change time, while Win32 has creation time */
    // #if !defined(WIN32) && !defined(__CYGWIN__)
    values[3] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
    isnull[4] = true;
    // #endif
    values[5] = BoolGetDatum(S_ISDIR(fst.st_mode));

    tuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), isnull.as_mut_ptr());

    pfree(filename as *mut c_void);

    PG_RETURN_DATUM!(HeapTupleGetDatum(tuple))
}

/*
 * stat a file (1 argument version)
 *
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_file_1arg(fcinfo: FunctionCallInfo) -> Datum {
    pg_stat_file(fcinfo)
}

/*
 * List a directory (returns the filenames only)
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_dir(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = fcinfo_resultinfo(fcinfo);
    let location: *mut c_char;
    let mut missing_ok: bool = false;
    let mut include_dot_dirs: bool = false;
    let dirdesc: *mut DIR;
    let mut de: *mut dirent;

    location = convert_and_check_filename(PG_GETARG_TEXT_PP(0));

    /* check the optional arguments */
    if PG_NARGS() == 3 {
        if !PG_ARGISNULL(1) {
            missing_ok = PG_GETARG_BOOL(1);
        }
        if !PG_ARGISNULL(2) {
            include_dot_dirs = PG_GETARG_BOOL(2);
        }
    }

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    dirdesc = AllocateDir(location);
    if dirdesc.is_null() {
        /* Return empty tuplestore if appropriate */
        if missing_ok && get_errno() == ENOENT {
            return 0 as Datum;
        }
        /* Otherwise, we can let ReadDir() throw the error */
    }

    loop {
        de = ReadDir(dirdesc, location);
        if de.is_null() {
            break;
        }

        let mut values: [Datum; 1] = [0; 1];
        let mut nulls: [bool; 1] = [false; 1];

        let d_name = dirent_d_name(de);
        if !include_dot_dirs
            && (strcmp(d_name, c".".as_ptr()) == 0 || strcmp(d_name, c"..".as_ptr()) == 0)
        {
            continue;
        }

        values[0] = CStringGetTextDatum(d_name);
        nulls[0] = false;

        tuplestore_putvalues(
            rsinfo_setResult(rsinfo),
            rsinfo_setDesc(rsinfo),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    FreeDir(dirdesc);
    0 as Datum
}

/*
 * List a directory (1 argument version)
 *
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_dir_1arg(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir(fcinfo)
}

/*
 * Generic function to return a directory listing of files.
 *
 * If the directory isn't there, silently return an empty set if missing_ok.
 * Other unreadable-directory cases throw an error.
 */
unsafe fn pg_ls_dir_files(fcinfo: FunctionCallInfo, dir: *const c_char, missing_ok: bool) -> Datum {
    let rsinfo: *mut ReturnSetInfo = fcinfo_resultinfo(fcinfo);
    let dirdesc: *mut DIR;
    let mut de: *mut dirent;

    InitMaterializedSRF(fcinfo, 0);

    /*
     * Now walk the directory.  Note that we must do this within a single SRF
     * call, not leave the directory open across multiple calls, since we
     * can't count on the SRF being run to completion.
     */
    dirdesc = AllocateDir(dir);
    if dirdesc.is_null() {
        /* Return empty tuplestore if appropriate */
        if missing_ok && get_errno() == ENOENT {
            return 0 as Datum;
        }
        /* Otherwise, we can let ReadDir() throw the error */
    }

    loop {
        de = ReadDir(dirdesc, dir);
        if de.is_null() {
            break;
        }

        let mut values: [Datum; 3] = [0; 3];
        let mut nulls: [bool; 3] = [false; 3];
        let mut path: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
        let mut attrib: stat = std::mem::zeroed();

        let d_name = dirent_d_name(de);

        /* Skip hidden files */
        if *d_name == b'.' as c_char {
            continue;
        }

        /* Get the file info */
        snprintf(
            path.as_mut_ptr(),
            std::mem::size_of_val(&path),
            c"%s/%s".as_ptr(),
            dir,
            d_name,
        );
        if stat_fn(path.as_ptr(), &mut attrib) < 0 {
            /* Ignore concurrently-deleted files, else complain */
            if get_errno() == ENOENT {
                continue;
            }
            elog!(
                ERROR,
                "could not stat file \"{}\": %m",
                cstr_display(path.as_ptr())
            );
            unreachable!();
        }

        /* Ignore anything but regular files */
        if !S_ISREG(attrib.st_mode) {
            continue;
        }

        values[0] = CStringGetTextDatum(d_name);
        values[1] = Int64GetDatum(attrib.st_size as int64);
        values[2] = TimestampTzGetDatum(time_t_to_timestamptz(attrib.st_mtime));
        memset(
            nulls.as_mut_ptr() as *mut c_void,
            0,
            std::mem::size_of_val(&nulls),
        );

        tuplestore_putvalues(
            rsinfo_setResult(rsinfo),
            rsinfo_setDesc(rsinfo),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    FreeDir(dirdesc);
    0 as Datum
}

/* Function to return the list of files in the log directory */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_logdir(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir_files(fcinfo, Log_directory, false)
}

/* Function to return the list of files in the WAL directory */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_waldir(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir_files(fcinfo, c"pg_wal".as_ptr(), false)
}

/*
 * Generic function to return the list of files in pgsql_tmp
 */
unsafe fn pg_ls_tmpdir(fcinfo: FunctionCallInfo, tblspc: Oid) -> Datum {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    if !SearchSysCacheExists1(TABLESPACEOID, ObjectIdGetDatum(tblspc)) {
        elog!(ERROR, "tablespace with OID {} does not exist", tblspc);
    }

    TempTablespacePath(path.as_mut_ptr(), tblspc);
    pg_ls_dir_files(fcinfo, path.as_ptr(), true)
}

/*
 * Function to return the list of temporary files in the pg_default tablespace's
 * pgsql_tmp directory
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_tmpdir_noargs(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_tmpdir(fcinfo, DEFAULTTABLESPACE_OID)
}

/*
 * Function to return the list of temporary files in the specified tablespace's
 * pgsql_tmp directory
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_tmpdir_1arg(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_tmpdir(fcinfo, PG_GETARG_OID(0))
}

/*
 * Function to return the list of files in the WAL archive status directory.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_archive_statusdir(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir_files(fcinfo, c"pg_wal/archive_status".as_ptr(), true)
}

/*
 * Function to return the list of files in the WAL summaries directory.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_summariesdir(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir_files(fcinfo, c"pg_wal/summaries".as_ptr(), true)
}

/*
 * Function to return the list of files in the PG_LOGICAL_SNAPSHOTS_DIR
 * directory.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_logicalsnapdir(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir_files(fcinfo, PG_LOGICAL_SNAPSHOTS_DIR, false)
}

/*
 * Function to return the list of files in the PG_LOGICAL_MAPPINGS_DIR
 * directory.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_logicalmapdir(fcinfo: FunctionCallInfo) -> Datum {
    pg_ls_dir_files(fcinfo, PG_LOGICAL_MAPPINGS_DIR, false)
}

/*
 * Function to return the list of files in the PG_REPLSLOT_DIR/<slot_name>
 * directory.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ls_replslotdir(fcinfo: FunctionCallInfo) -> Datum {
    let slotname_t: *mut text;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let slotname: *mut c_char;

    slotname_t = PG_GETARG_TEXT_PP(0);

    slotname = text_to_cstring(slotname_t);

    if !SearchNamedReplicationSlot(slotname, true) {
        elog!(
            ERROR,
            "replication slot \"{}\" does not exist",
            cstr_display(slotname)
        );
    }

    snprintf(
        path.as_mut_ptr(),
        std::mem::size_of_val(&path),
        c"%s/%s".as_ptr(),
        PG_REPLSLOT_DIR,
        slotname,
    );

    pg_ls_dir_files(fcinfo, path.as_ptr(), false)
}

// Helper to render a C string for elog!/% formatting (%m / %s replacement).
unsafe fn cstr_display(s: *const c_char) -> String {
    if s.is_null() {
        return String::from("(null)");
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}
