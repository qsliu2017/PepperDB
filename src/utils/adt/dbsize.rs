//! src/backend/utils/adt/dbsize.c
//!
//! dbsize.c
//!		Database object size functions, and related inquiries
//!
//! Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/utils/adt/dbsize.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, uint32, uint64, uint8};
use crate::pg_config_manual::MAXPGPATH;
use crate::postgres_ext::Oid;
use crate::nodes::pg_list::{lfirst_oid, List, ListCell};
use crate::{current_cell, foreach, PG_GETARG_OID, PG_GETARG_INT64, PG_GETARG_NAME, PG_GETARG_TEXT_PP, PG_RETURN_NULL, PG_RETURN_INT64, PG_RETURN_OID, PG_RETURN_TEXT_P};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strtol(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> std::ffi::c_long;
}

/* Divide by two and round away from zero */
// #define half_rounded(x)   (((x) + ((x) < 0 ? -1 : 1)) / 2)
#[inline]
fn half_rounded(x: int64) -> int64 {
    (x + if x < 0 { -1 } else { 1 }) / 2
}

/* Units used in pg_size_pretty functions.  All units must be powers of 2 */
#[repr(C)]
struct size_pretty_unit {
    name: *const c_char,    /* bytes, kB, MB, GB etc */
    limit: uint32,          /* upper limit, prior to half rounding after
                             * converting to this unit. */
    round: bool,            /* do half rounding for this unit */
    unitbits: uint8,        /* (1 << unitbits) bytes to make 1 of this
                             * unit */
}

/* When adding units here also update the docs and the error message in pg_size_bytes */
static size_pretty_units: [size_pretty_unit; 7] = [
    size_pretty_unit { name: c"bytes".as_ptr(), limit: 10 * 1024, round: false, unitbits: 0 },
    size_pretty_unit { name: c"kB".as_ptr(), limit: 20 * 1024 - 1, round: true, unitbits: 10 },
    size_pretty_unit { name: c"MB".as_ptr(), limit: 20 * 1024 - 1, round: true, unitbits: 20 },
    size_pretty_unit { name: c"GB".as_ptr(), limit: 20 * 1024 - 1, round: true, unitbits: 30 },
    size_pretty_unit { name: c"TB".as_ptr(), limit: 20 * 1024 - 1, round: true, unitbits: 40 },
    size_pretty_unit { name: c"PB".as_ptr(), limit: 20 * 1024 - 1, round: true, unitbits: 50 },
    size_pretty_unit { name: std::ptr::null(), limit: 0, round: false, unitbits: 0 },
];

/* Additional unit aliases accepted by pg_size_bytes */
#[repr(C)]
struct size_bytes_unit_alias {
    alias: *const c_char,
    unit_index: c_int, /* corresponding size_pretty_units element */
}

/* When adding units here also update the docs and the error message in pg_size_bytes */
static size_bytes_aliases: [size_bytes_unit_alias; 2] = [
    size_bytes_unit_alias { alias: c"B".as_ptr(), unit_index: 0 },
    size_bytes_unit_alias { alias: std::ptr::null(), unit_index: 0 },
];

/* Return physical size of directory contents, or 0 if dir doesn't exist */
unsafe fn db_dir_size(path: *const c_char) -> int64 {
    let mut dirsize: int64 = 0;
    let mut direntry: *mut dirent;
    let dirdesc: *mut DIR;
    let mut filename: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];

    dirdesc = AllocateDir(path);

    if dirdesc.is_null() {
        return 0;
    }

    loop {
        direntry = ReadDir(dirdesc, path);
        if direntry.is_null() {
            break;
        }

        let mut fst: stat = std::mem::zeroed();

        CHECK_FOR_INTERRUPTS!();

        if strcmp((*direntry).d_name.as_ptr(), c".".as_ptr()) == 0
            || strcmp((*direntry).d_name.as_ptr(), c"..".as_ptr()) == 0
        {
            continue;
        }

        snprintf(
            filename.as_mut_ptr(),
            std::mem::size_of_val(&filename),
            c"%s/%s".as_ptr(),
            path,
            (*direntry).d_name.as_ptr(),
        );

        if stat(filename.as_ptr(), &mut fst) < 0 {
            if errno() == ENOENT {
                continue;
            } else {
                ereport!(ERROR, "could not stat file");
            }
        }
        dirsize += (*fst.as_st_size()) as int64;
    }

    FreeDir(dirdesc);
    dirsize
}

/*
 * calculate size of database in all tablespaces
 */
unsafe fn calculate_database_size(dbOid: Oid) -> int64 {
    let totalsize: int64;
    let dirdesc: *mut DIR;
    let mut direntry: *mut dirent;
    let mut dirpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut pathname: [c_char; MAXPGPATH + 21 + TABLESPACE_VERSION_DIRECTORY_SIZE] =
        [0; MAXPGPATH + 21 + TABLESPACE_VERSION_DIRECTORY_SIZE];
    let aclresult: AclResult;

    /*
     * User must have connect privilege for target database or have privileges
     * of pg_read_all_stats
     */
    aclresult = object_aclcheck(DatabaseRelationId, dbOid, GetUserId(), ACL_CONNECT);
    if aclresult != ACLCHECK_OK && !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) {
        aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(dbOid));
    }

    /* Shared storage in pg_global is not counted */

    /* Include pg_default storage */
    snprintf(
        pathname.as_mut_ptr(),
        std::mem::size_of_val(&pathname),
        c"base/%u".as_ptr(),
        dbOid,
    );
    let mut totalsize = db_dir_size(pathname.as_ptr());

    /* Scan the non-default tablespaces */
    snprintf(dirpath.as_mut_ptr(), MAXPGPATH, PG_TBLSPC_DIR.as_ptr());
    let dirdesc = AllocateDir(dirpath.as_ptr());

    loop {
        direntry = ReadDir(dirdesc, dirpath.as_ptr());
        if direntry.is_null() {
            break;
        }

        CHECK_FOR_INTERRUPTS!();

        if strcmp((*direntry).d_name.as_ptr(), c".".as_ptr()) == 0
            || strcmp((*direntry).d_name.as_ptr(), c"..".as_ptr()) == 0
        {
            continue;
        }

        snprintf(
            pathname.as_mut_ptr(),
            std::mem::size_of_val(&pathname),
            c"%s/%s/%s/%u".as_ptr(),
            PG_TBLSPC_DIR.as_ptr(),
            (*direntry).d_name.as_ptr(),
            TABLESPACE_VERSION_DIRECTORY.as_ptr(),
            dbOid,
        );
        totalsize += db_dir_size(pathname.as_ptr());
    }

    FreeDir(dirdesc);

    let _ = aclresult;
    let _ = totalsize;
    totalsize
}

#[no_mangle]
pub unsafe extern "C" fn pg_database_size_oid(fcinfo: FunctionCallInfo) -> Datum {
    let dbOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let size: int64;

    /*
     * Not needed for correctness, but avoid non-user-facing error message
     * later if the database doesn't exist.
     */
    if !SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(dbOid)) {
        ereport!(ERROR, "database with OID does not exist");
    }

    size = calculate_database_size(dbOid);

    if size == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT64!(size)
}

#[no_mangle]
pub unsafe extern "C" fn pg_database_size_name(fcinfo: FunctionCallInfo) -> Datum {
    let dbName: *mut Name = PG_GETARG_NAME!(fcinfo, 0);
    let dbOid: Oid = get_database_oid(NameStr!(*dbName), false);
    let size: int64;

    size = calculate_database_size(dbOid);

    if size == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT64!(size)
}

/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
unsafe fn calculate_tablespace_size(tblspcOid: Oid) -> int64 {
    let mut tblspcPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut pathname: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
    let mut totalsize: int64 = 0;
    let dirdesc: *mut DIR;
    let mut direntry: *mut dirent;
    let aclresult: AclResult;

    /*
     * User must have privileges of pg_read_all_stats or have CREATE privilege
     * for target tablespace, either explicitly granted or implicitly because
     * it is default for current database.
     */
    if tblspcOid != MyDatabaseTableSpace
        && !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)
    {
        aclresult = object_aclcheck(TableSpaceRelationId, tblspcOid, GetUserId(), ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TABLESPACE, get_tablespace_name(tblspcOid));
        }
    }

    if tblspcOid == DEFAULTTABLESPACE_OID {
        snprintf(tblspcPath.as_mut_ptr(), MAXPGPATH, c"base".as_ptr());
    } else if tblspcOid == GLOBALTABLESPACE_OID {
        snprintf(tblspcPath.as_mut_ptr(), MAXPGPATH, c"global".as_ptr());
    } else {
        snprintf(
            tblspcPath.as_mut_ptr(),
            MAXPGPATH,
            c"%s/%u/%s".as_ptr(),
            PG_TBLSPC_DIR.as_ptr(),
            tblspcOid,
            TABLESPACE_VERSION_DIRECTORY.as_ptr(),
        );
    }

    dirdesc = AllocateDir(tblspcPath.as_ptr());

    if dirdesc.is_null() {
        return -1;
    }

    loop {
        direntry = ReadDir(dirdesc, tblspcPath.as_ptr());
        if direntry.is_null() {
            break;
        }

        let mut fst: stat = std::mem::zeroed();

        CHECK_FOR_INTERRUPTS!();

        if strcmp((*direntry).d_name.as_ptr(), c".".as_ptr()) == 0
            || strcmp((*direntry).d_name.as_ptr(), c"..".as_ptr()) == 0
        {
            continue;
        }

        snprintf(
            pathname.as_mut_ptr(),
            std::mem::size_of_val(&pathname),
            c"%s/%s".as_ptr(),
            tblspcPath.as_ptr(),
            (*direntry).d_name.as_ptr(),
        );

        if stat(pathname.as_ptr(), &mut fst) < 0 {
            if errno() == ENOENT {
                continue;
            } else {
                ereport!(ERROR, "could not stat file");
            }
        }

        if S_ISDIR(*fst.as_st_mode()) {
            totalsize += db_dir_size(pathname.as_ptr());
        }

        totalsize += (*fst.as_st_size()) as int64;
    }

    FreeDir(dirdesc);

    totalsize
}

#[no_mangle]
pub unsafe extern "C" fn pg_tablespace_size_oid(fcinfo: FunctionCallInfo) -> Datum {
    let tblspcOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let size: int64;

    /*
     * Not needed for correctness, but avoid non-user-facing error message
     * later if the tablespace doesn't exist.
     */
    if !SearchSysCacheExists1(TABLESPACEOID, ObjectIdGetDatum(tblspcOid)) {
        ereport!(ERROR, "tablespace with OID does not exist");
    }

    size = calculate_tablespace_size(tblspcOid);

    if size < 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT64!(size)
}

#[no_mangle]
pub unsafe extern "C" fn pg_tablespace_size_name(fcinfo: FunctionCallInfo) -> Datum {
    let tblspcName: *mut Name = PG_GETARG_NAME!(fcinfo, 0);
    let tblspcOid: Oid = get_tablespace_oid(NameStr!(*tblspcName), false);
    let size: int64;

    size = calculate_tablespace_size(tblspcOid);

    if size < 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT64!(size)
}

/*
 * calculate size of (one fork of) a relation
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
unsafe fn calculate_relation_size(
    rfn: *mut RelFileLocator,
    backend: ProcNumber,
    forknum: ForkNumber,
) -> int64 {
    let mut totalsize: int64 = 0;
    let relationpath: RelPathStr;
    let mut pathname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut segcount: std::ffi::c_uint;

    relationpath = relpathbackend(*rfn, backend, forknum);

    segcount = 0;
    loop {
        let mut fst: stat = std::mem::zeroed();

        CHECK_FOR_INTERRUPTS!();

        if segcount == 0 {
            snprintf(
                pathname.as_mut_ptr(),
                MAXPGPATH,
                c"%s".as_ptr(),
                relationpath.str_.as_ptr(),
            );
        } else {
            snprintf(
                pathname.as_mut_ptr(),
                MAXPGPATH,
                c"%s.%u".as_ptr(),
                relationpath.str_.as_ptr(),
                segcount,
            );
        }

        if stat(pathname.as_ptr(), &mut fst) < 0 {
            if errno() == ENOENT {
                break;
            } else {
                ereport!(ERROR, "could not stat file");
            }
        }
        totalsize += (*fst.as_st_size()) as int64;

        segcount += 1;
    }

    totalsize
}

#[no_mangle]
pub unsafe extern "C" fn pg_relation_size(fcinfo: FunctionCallInfo) -> Datum {
    let relOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let forkName: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let rel: Relation;
    let size: int64;

    rel = try_relation_open(relOid, AccessShareLock);

    /*
     * Before 9.2, we used to throw an error if the relation didn't exist, but
     * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
     * less robust, because while we scan pg_class with an MVCC snapshot,
     * someone else might drop the table. It's better to return NULL for
     * already-dropped tables than throw an error and abort the whole query.
     */
    if rel.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    size = calculate_relation_size(
        &mut (*rel).rd_locator,
        (*rel).rd_backend,
        forkname_to_number(text_to_cstring(forkName)),
    );

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64!(size)
}

/*
 * Calculate total on-disk size of a TOAST relation, including its indexes.
 * Must not be applied to non-TOAST relations.
 */
unsafe fn calculate_toast_table_size(toastrelid: Oid) -> int64 {
    let mut size: int64 = 0;
    let toastRel: Relation;
    let mut forkNum: ForkNumber;
    let indexlist: *mut List;

    toastRel = relation_open(toastrelid, AccessShareLock);

    /* toast heap size, including FSM and VM size */
    forkNum = 0;
    while forkNum <= MAX_FORKNUM {
        size += calculate_relation_size(&mut (*toastRel).rd_locator, (*toastRel).rd_backend, forkNum);
        forkNum += 1;
    }

    /* toast index size, including FSM and VM size */
    indexlist = RelationGetIndexList(toastRel);

    /* Size is calculated using all the indexes available */
    foreach!(lc, indexlist, {
        let toastIdxRel: Relation;

        toastIdxRel = relation_open(lfirst_oid(current_cell!(lc)), AccessShareLock);
        let mut forkNum: ForkNumber = 0;
        while forkNum <= MAX_FORKNUM {
            size += calculate_relation_size(
                &mut (*toastIdxRel).rd_locator,
                (*toastIdxRel).rd_backend,
                forkNum,
            );
            forkNum += 1;
        }

        relation_close(toastIdxRel, AccessShareLock);
    });
    list_free(indexlist);
    relation_close(toastRel, AccessShareLock);

    size
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
unsafe fn calculate_table_size(rel: Relation) -> int64 {
    let mut size: int64 = 0;
    let mut forkNum: ForkNumber;

    /*
     * heap size, including FSM and VM
     */
    forkNum = 0;
    while forkNum <= MAX_FORKNUM {
        size += calculate_relation_size(&mut (*rel).rd_locator, (*rel).rd_backend, forkNum);
        forkNum += 1;
    }

    /*
     * Size of toast relation
     */
    if OidIsValid((*(*rel).rd_rel).reltoastrelid) {
        size += calculate_toast_table_size((*(*rel).rd_rel).reltoastrelid);
    }

    size
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
unsafe fn calculate_indexes_size(rel: Relation) -> int64 {
    let mut size: int64 = 0;

    /*
     * Aggregate all indexes on the given relation
     */
    if (*(*rel).rd_rel).relhasindex {
        let index_oids: *mut List = RelationGetIndexList(rel);

        foreach!(cell, index_oids, {
            let idxOid: Oid = lfirst_oid(current_cell!(cell));
            let idxRel: Relation;
            let mut forkNum: ForkNumber;

            idxRel = relation_open(idxOid, AccessShareLock);

            forkNum = 0;
            while forkNum <= MAX_FORKNUM {
                size += calculate_relation_size(&mut (*idxRel).rd_locator, (*idxRel).rd_backend, forkNum);
                forkNum += 1;
            }

            relation_close(idxRel, AccessShareLock);
        });

        list_free(index_oids);
    }

    size
}

#[no_mangle]
pub unsafe extern "C" fn pg_table_size(fcinfo: FunctionCallInfo) -> Datum {
    let relOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let rel: Relation;
    let size: int64;

    rel = try_relation_open(relOid, AccessShareLock);

    if rel.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    size = calculate_table_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64!(size)
}

#[no_mangle]
pub unsafe extern "C" fn pg_indexes_size(fcinfo: FunctionCallInfo) -> Datum {
    let relOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let rel: Relation;
    let size: int64;

    rel = try_relation_open(relOid, AccessShareLock);

    if rel.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    size = calculate_indexes_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64!(size)
}

/*
 *	Compute the on-disk size of all files for the relation,
 *	including heap data, index data, toast data, FSM, VM.
 */
unsafe fn calculate_total_relation_size(rel: Relation) -> int64 {
    let mut size: int64;

    /*
     * Aggregate the table size, this includes size of the heap, toast and
     * toast index with free space and visibility map
     */
    size = calculate_table_size(rel);

    /*
     * Add size of all attached indexes as well
     */
    size += calculate_indexes_size(rel);

    size
}

#[no_mangle]
pub unsafe extern "C" fn pg_total_relation_size(fcinfo: FunctionCallInfo) -> Datum {
    let relOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let rel: Relation;
    let size: int64;

    rel = try_relation_open(relOid, AccessShareLock);

    if rel.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    size = calculate_total_relation_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64!(size)
}

/*
 * formatting with size units
 */
#[no_mangle]
pub unsafe extern "C" fn pg_size_pretty(fcinfo: FunctionCallInfo) -> Datum {
    let mut size: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut buf: [c_char; 64] = [0; 64];
    let mut idx: usize = 0;

    while !size_pretty_units[idx].name.is_null() {
        let bits: uint8;
        let abs_size: uint64 = if size < 0 {
            0u64.wrapping_sub(size as uint64)
        } else {
            size as uint64
        };

        /*
         * Use this unit if there are no more units or the absolute size is
         * below the limit for the current unit.
         */
        if size_pretty_units[idx + 1].name.is_null() || abs_size < size_pretty_units[idx].limit as uint64 {
            if size_pretty_units[idx].round {
                size = half_rounded(size);
            }

            snprintf(
                buf.as_mut_ptr(),
                std::mem::size_of_val(&buf),
                c"%lld %s".as_ptr(),
                size as std::ffi::c_longlong,
                size_pretty_units[idx].name,
            );
            break;
        }

        /*
         * Determine the number of bits to use to build the divisor.  We may
         * need to use 1 bit less than the difference between this and the
         * next unit if the next unit uses half rounding.  Or we may need to
         * shift an extra bit if this unit uses half rounding and the next one
         * does not.  We use division rather than shifting right by this
         * number of bits to ensure positive and negative values are rounded
         * in the same way.
         */
        bits = (size_pretty_units[idx + 1].unitbits as i32
            - size_pretty_units[idx].unitbits as i32
            - (size_pretty_units[idx + 1].round == true) as i32
            + (size_pretty_units[idx].round == true) as i32) as uint8;
        size /= (1i64) << bits;

        idx += 1;
    }

    PG_RETURN_TEXT_P!(cstring_to_text(buf.as_ptr()))
}

unsafe fn numeric_to_cstring(n: Numeric) -> *mut c_char {
    let d: Datum = NumericGetDatum(n);

    DatumGetCString(DirectFunctionCall1(numeric_out, d))
}

unsafe fn numeric_is_less(a: Numeric, b: Numeric) -> bool {
    let da: Datum = NumericGetDatum(a);
    let db: Datum = NumericGetDatum(b);

    DatumGetBool(DirectFunctionCall2(numeric_lt, da, db))
}

unsafe fn numeric_absolute(n: Numeric) -> Numeric {
    let d: Datum = NumericGetDatum(n);
    let result: Datum;

    result = DirectFunctionCall1(numeric_abs, d);
    DatumGetNumeric(result)
}

unsafe fn numeric_half_rounded(n: Numeric) -> Numeric {
    let mut d: Datum = NumericGetDatum(n);
    let zero: Datum;
    let one: Datum;
    let two: Datum;
    let result: Datum;

    zero = NumericGetDatum(int64_to_numeric(0));
    one = NumericGetDatum(int64_to_numeric(1));
    two = NumericGetDatum(int64_to_numeric(2));

    if DatumGetBool(DirectFunctionCall2(numeric_ge, d, zero)) {
        d = DirectFunctionCall2(numeric_add, d, one);
    } else {
        d = DirectFunctionCall2(numeric_sub, d, one);
    }

    result = DirectFunctionCall2(numeric_div_trunc, d, two);
    DatumGetNumeric(result)
}

unsafe fn numeric_truncated_divide(n: Numeric, divisor: int64) -> Numeric {
    let d: Datum = NumericGetDatum(n);
    let divisor_numeric: Datum;
    let result: Datum;

    divisor_numeric = NumericGetDatum(int64_to_numeric(divisor));
    result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
    DatumGetNumeric(result)
}

#[no_mangle]
pub unsafe extern "C" fn pg_size_pretty_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let mut size: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut result: *mut c_char = std::ptr::null_mut();
    let mut idx: usize = 0;

    while !size_pretty_units[idx].name.is_null() {
        let shiftby: std::ffi::c_uint;

        /* use this unit if there are no more units or we're below the limit */
        if size_pretty_units[idx + 1].name.is_null()
            || numeric_is_less(
                numeric_absolute(size),
                int64_to_numeric(size_pretty_units[idx].limit as int64),
            )
        {
            if size_pretty_units[idx].round {
                size = numeric_half_rounded(size);
            }

            result = psprintf(
                c"%s %s".as_ptr(),
                numeric_to_cstring(size),
                size_pretty_units[idx].name,
            );
            break;
        }

        /*
         * Determine the number of bits to use to build the divisor.  We may
         * need to use 1 bit less than the difference between this and the
         * next unit if the next unit uses half rounding.  Or we may need to
         * shift an extra bit if this unit uses half rounding and the next one
         * does not.
         */
        shiftby = (size_pretty_units[idx + 1].unitbits as i32
            - size_pretty_units[idx].unitbits as i32
            - (size_pretty_units[idx + 1].round == true) as i32
            + (size_pretty_units[idx].round == true) as i32) as std::ffi::c_uint;
        size = numeric_truncated_divide(size, (1i64) << shiftby);

        idx += 1;
    }

    PG_RETURN_TEXT_P!(cstring_to_text(result))
}

/*
 * Convert a human-readable size to a size in bytes
 */
#[no_mangle]
pub unsafe extern "C" fn pg_size_bytes(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let str_: *mut c_char;
    let mut strptr: *mut c_char;
    let mut endptr: *mut c_char;
    let saved_char: c_char;
    let mut num: Numeric;
    let result: int64;
    let mut have_digits: bool = false;

    str_ = text_to_cstring(arg);

    /* Skip leading whitespace */
    strptr = str_;
    while isspace(*strptr as std::ffi::c_uchar as c_int) != 0 {
        strptr = strptr.add(1);
    }

    /* Check that we have a valid number and determine where it ends */
    endptr = strptr;

    /* Part (1): sign */
    if *endptr == b'-' as c_char || *endptr == b'+' as c_char {
        endptr = endptr.add(1);
    }

    /* Part (2): main digit string */
    if isdigit(*endptr as std::ffi::c_uchar as c_int) != 0 {
        have_digits = true;
        loop {
            endptr = endptr.add(1);
            if isdigit(*endptr as std::ffi::c_uchar as c_int) == 0 {
                break;
            }
        }
    }

    /* Part (3): optional decimal point and fractional digits */
    if *endptr == b'.' as c_char {
        endptr = endptr.add(1);
        if isdigit(*endptr as std::ffi::c_uchar as c_int) != 0 {
            have_digits = true;
            loop {
                endptr = endptr.add(1);
                if isdigit(*endptr as std::ffi::c_uchar as c_int) == 0 {
                    break;
                }
            }
        }
    }

    /* Complain if we don't have a valid number at this point */
    if !have_digits {
        ereport!(ERROR, "invalid size");
    }

    /* Part (4): optional exponent */
    if *endptr == b'e' as c_char || *endptr == b'E' as c_char {
        let exponent: std::ffi::c_long;
        let mut cp: *mut c_char = std::ptr::null_mut();

        /*
         * Note we might one day support EB units, so if what follows 'E'
         * isn't a number, just treat it all as a unit to be parsed.
         */
        exponent = strtol(endptr.add(1), &mut cp, 10);
        let _ = exponent; /* Silence -Wunused-result warnings */
        if cp > endptr.add(1) {
            endptr = cp;
        }
    }

    /*
     * Parse the number, saving the next character, which may be the first
     * character of the unit string.
     */
    saved_char = *endptr;
    *endptr = b'\0' as c_char;

    num = DatumGetNumeric(DirectFunctionCall3(
        numeric_in,
        CStringGetDatum(strptr),
        ObjectIdGetDatum(InvalidOid),
        Int32GetDatum(-1),
    ));

    *endptr = saved_char;

    /* Skip whitespace between number and unit */
    strptr = endptr;
    while isspace(*strptr as std::ffi::c_uchar as c_int) != 0 {
        strptr = strptr.add(1);
    }

    /* Handle possible unit */
    if *strptr != b'\0' as c_char {
        let mut unit_idx: usize = 0;
        let mut found = false;
        let mut multiplier: int64;

        /* Trim any trailing whitespace */
        endptr = str_.add(VARSIZE_ANY_EXHDR!(arg) as usize - 1);

        while isspace(*endptr as std::ffi::c_uchar as c_int) != 0 {
            endptr = endptr.sub(1);
        }

        endptr = endptr.add(1);
        *endptr = b'\0' as c_char;

        while !size_pretty_units[unit_idx].name.is_null() {
            /* Parse the unit case-insensitively */
            if pg_strcasecmp(strptr, size_pretty_units[unit_idx].name) == 0 {
                found = true;
                break;
            }
            unit_idx += 1;
        }

        /* If not found, look in table of aliases */
        if !found {
            let mut a_idx: usize = 0;
            while !size_bytes_aliases[a_idx].alias.is_null() {
                if pg_strcasecmp(strptr, size_bytes_aliases[a_idx].alias) == 0 {
                    unit_idx = size_bytes_aliases[a_idx].unit_index as usize;
                    found = true;
                    break;
                }
                a_idx += 1;
            }
        }

        /* Verify we found a valid unit in the loop above */
        if !found {
            ereport!(ERROR, "invalid size");
        }

        multiplier = (1i64) << size_pretty_units[unit_idx].unitbits;

        if multiplier > 1 {
            let mul_num: Numeric;

            mul_num = int64_to_numeric(multiplier);

            num = DatumGetNumeric(DirectFunctionCall2(
                numeric_mul,
                NumericGetDatum(mul_num),
                NumericGetDatum(num),
            ));
        }

        let _ = multiplier;
    }

    result = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(num)));

    PG_RETURN_INT64!(result)
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *		SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.  We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot even though the relations have been dropped.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_relation_filenode(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: RelFileNumber;
    let tuple: HeapTuple;
    let relform: Form_pg_class;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        PG_RETURN_NULL!(fcinfo);
    }
    relform = GETSTRUCT!(tuple) as Form_pg_class;

    if RELKIND_HAS_STORAGE!((*relform).relkind) {
        if (*relform).relfilenode != 0 {
            result = (*relform).relfilenode;
        } else {
            /* Consult the relation mapper */
            result = RelationMapOidToFilenumber(relid, (*relform).relisshared);
        }
    } else {
        /* no storage, return NULL */
        result = InvalidRelFileNumber;
    }

    ReleaseSysCache(tuple);

    if !RelFileNumberIsValid(result) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_OID!(result)
}

/*
 * Get the relation via (reltablespace, relfilenumber)
 *
 * This is expected to be used when somebody wants to match an individual file
 * on the filesystem back to its table. That's not trivially possible via
 * pg_class, because that doesn't contain the relfilenumbers of shared and nailed
 * tables.
 *
 * We don't fail but return NULL if we cannot find a mapping.
 *
 * Temporary relations are not detected, returning NULL (see
 * RelidByRelfilenumber() for the reasons).
 *
 * InvalidOid can be passed instead of the current database's default
 * tablespace.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_filenode_relation(fcinfo: FunctionCallInfo) -> Datum {
    let reltablespace: Oid = PG_GETARG_OID!(fcinfo, 0);
    let relfilenumber: RelFileNumber = PG_GETARG_OID!(fcinfo, 1);
    let heaprel: Oid;

    /* test needed so RelidByRelfilenumber doesn't misbehave */
    if !RelFileNumberIsValid(relfilenumber) {
        PG_RETURN_NULL!(fcinfo);
    }

    heaprel = RelidByRelfilenumber(reltablespace, relfilenumber);

    if !OidIsValid(heaprel) {
        PG_RETURN_NULL!(fcinfo);
    } else {
        PG_RETURN_OID!(heaprel)
    }
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_relation_filepath(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let tuple: HeapTuple;
    let relform: Form_pg_class;
    let mut rlocator: RelFileLocator = std::mem::zeroed();
    let backend: ProcNumber;
    let path: RelPathStr;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        PG_RETURN_NULL!(fcinfo);
    }
    relform = GETSTRUCT!(tuple) as Form_pg_class;

    if RELKIND_HAS_STORAGE!((*relform).relkind) {
        /* This logic should match RelationInitPhysicalAddr */
        if (*relform).reltablespace != 0 {
            rlocator.spcOid = (*relform).reltablespace;
        } else {
            rlocator.spcOid = MyDatabaseTableSpace;
        }
        if rlocator.spcOid == GLOBALTABLESPACE_OID {
            rlocator.dbOid = InvalidOid;
        } else {
            rlocator.dbOid = MyDatabaseId;
        }
        if (*relform).relfilenode != 0 {
            rlocator.relNumber = (*relform).relfilenode;
        } else {
            /* Consult the relation mapper */
            rlocator.relNumber = RelationMapOidToFilenumber(relid, (*relform).relisshared);
        }
    } else {
        /* no storage, return NULL */
        rlocator.relNumber = InvalidRelFileNumber;
        /* some compilers generate warnings without these next two lines */
        rlocator.dbOid = InvalidOid;
        rlocator.spcOid = InvalidOid;
    }

    if !RelFileNumberIsValid(rlocator.relNumber) {
        ReleaseSysCache(tuple);
        PG_RETURN_NULL!(fcinfo);
    }

    /* Determine owning backend. */
    match (*relform).relpersistence as u8 {
        RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => {
            backend = INVALID_PROC_NUMBER;
        }
        RELPERSISTENCE_TEMP => {
            if isTempOrTempToastNamespace((*relform).relnamespace) {
                backend = ProcNumberForTempRelations();
            } else {
                /* Do it the hard way. */
                backend = GetTempNamespaceProcNumber((*relform).relnamespace);
                Assert!(backend != INVALID_PROC_NUMBER);
            }
        }
        _ => {
            elog!(ERROR, "invalid relpersistence: {}", (*relform).relpersistence);
            #[allow(unreachable_code)]
            {
                backend = INVALID_PROC_NUMBER; /* placate compiler */
            }
        }
    }

    ReleaseSysCache(tuple);

    path = relpathbackend(rlocator, backend, MAIN_FORKNUM);

    PG_RETURN_TEXT_P!(cstring_to_text(path.str_.as_ptr()))
}

// ----------------------------------------------------------------
// Local stubs for unported dependencies
// ----------------------------------------------------------------

const TABLESPACE_VERSION_DIRECTORY_SIZE: usize = 32;

type AclResult = c_int;
type ProcNumber = c_int;
type ForkNumber = c_int;
type RelFileNumber = Oid;
type Relation = *mut RelationData;
type HeapTuple = *mut c_void;
type Form_pg_class = *mut FormData_pg_class;
type Numeric = *mut c_void;
type FunctionCallInfo = *mut c_void;
type text = c_void;
type Name = NameData;

#[repr(C)]
struct NameData {
    data: [c_char; crate::pg_config_manual::NAMEDATALEN],
}

#[repr(C)]
struct RelationData {
    rd_locator: RelFileLocator,
    rd_backend: ProcNumber,
    rd_rel: Form_pg_class,
}

#[repr(C)]
struct FormData_pg_class {
    reltoastrelid: Oid,
    relhasindex: bool,
    relkind: c_char,
    relfilenode: Oid,
    relisshared: bool,
    reltablespace: Oid,
    relpersistence: c_char,
    relnamespace: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RelFileLocator {
    spcOid: Oid,
    dbOid: Oid,
    relNumber: RelFileNumber,
}

#[repr(C)]
struct RelPathStr {
    str_: [c_char; MAXPGPATH],
}

#[repr(C)]
struct DIR {
    _private: [u8; 0],
}

#[repr(C)]
struct dirent {
    d_name: [c_char; 256],
}

#[repr(C)]
struct stat {
    _private: [u8; 256],
}
impl stat {
    unsafe fn as_st_size(&self) -> *const i64 {
        unimplemented!() // TODO: sys/stat.h
    }
    unsafe fn as_st_mode(&self) -> *const u32 {
        unimplemented!() // TODO: sys/stat.h
    }
}

// Constants
const ENOENT: c_int = 2;
const ACLCHECK_OK: AclResult = 0;
const ACL_CONNECT: c_int = 0;
const ACL_CREATE: c_int = 0;
const OBJECT_DATABASE: c_int = 0;
const OBJECT_TABLESPACE: c_int = 0;
const DatabaseRelationId: Oid = 1262;
const TableSpaceRelationId: Oid = 1213;
const ROLE_PG_READ_ALL_STATS: Oid = 3375;
const DATABASEOID: c_int = 0;
const TABLESPACEOID: c_int = 0;
const RELOID: c_int = 0;
const DEFAULTTABLESPACE_OID: Oid = 1663;
const GLOBALTABLESPACE_OID: Oid = 1664;
const InvalidOid: Oid = 0;
const InvalidRelFileNumber: RelFileNumber = 0;
const AccessShareLock: c_int = 1;
const MAX_FORKNUM: ForkNumber = 3;
const MAIN_FORKNUM: ForkNumber = 0;
const INVALID_PROC_NUMBER: ProcNumber = -1;
const RELPERSISTENCE_UNLOGGED: u8 = b'u';
const RELPERSISTENCE_PERMANENT: u8 = b'p';
const RELPERSISTENCE_TEMP: u8 = b't';

const PG_TBLSPC_DIR: &core::ffi::CStr = c"pg_tblspc";
const TABLESPACE_VERSION_DIRECTORY: &core::ffi::CStr = c"PG_18_202504071";

// Function stubs
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!() // TODO: storage/fd.c
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!() // TODO: storage/fd.c
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!() // TODO: storage/fd.c
}
unsafe fn stat(_path: *const c_char, _buf: *mut stat) -> c_int {
    unimplemented!() // TODO: sys/stat.h
}
unsafe fn errno() -> c_int {
    unimplemented!() // TODO: errno.h
}
fn S_ISDIR(_mode: u32) -> bool {
    unimplemented!() // TODO: sys/stat.h
}
unsafe fn isspace(_c: c_int) -> c_int {
    unimplemented!() // TODO: ctype.h
}
unsafe fn isdigit(_c: c_int) -> c_int {
    unimplemented!() // TODO: ctype.h
}
unsafe fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: c_int) -> AclResult {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!() // TODO: utils/adt/acl.c
}
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: c_int, _objectname: *const c_char) {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn GetUserId() -> Oid {
    unimplemented!() // TODO: utils/init/miscinit.c
}
unsafe fn get_database_name(_dbid: Oid) -> *const c_char {
    unimplemented!() // TODO: commands/dbcommands.c
}
unsafe fn get_database_oid(_dbname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: commands/dbcommands.c
}
unsafe fn get_tablespace_name(_spc_oid: Oid) -> *const c_char {
    unimplemented!() // TODO: commands/tablespace.c
}
unsafe fn get_tablespace_oid(_tablespacename: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: commands/tablespace.c
}
unsafe fn SearchSysCacheExists1(_cacheId: c_int, _key1: Datum) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn try_relation_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/common/relation.c
}
unsafe fn relation_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/common/relation.c
}
unsafe fn relation_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/common/relation.c
}
unsafe fn RelationGetIndexList(_relation: Relation) -> *mut List {
    unimplemented!() // TODO: utils/cache/relcache.c
}
unsafe fn relpathbackend(_rlocator: RelFileLocator, _backend: ProcNumber, _forknumber: ForkNumber) -> RelPathStr {
    unimplemented!() // TODO: common/relpath.c
}
unsafe fn forkname_to_number(_forkName: *mut c_char) -> ForkNumber {
    unimplemented!() // TODO: common/relpath.c
}
unsafe fn text_to_cstring(_t: *const text) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut text {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn RelationMapOidToFilenumber(_relationId: Oid, _shared: bool) -> RelFileNumber {
    unimplemented!() // TODO: utils/cache/relmapper.c
}
unsafe fn RelidByRelfilenumber(_reltablespace: Oid, _relfilenumber: RelFileNumber) -> Oid {
    unimplemented!() // TODO: utils/cache/relfilenumbermap.c
}
unsafe fn isTempOrTempToastNamespace(_namespaceId: Oid) -> bool {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn ProcNumberForTempRelations() -> ProcNumber {
    unimplemented!() // TODO: storage/ipc/procsignal.c
}
unsafe fn GetTempNamespaceProcNumber(_namespaceId: Oid) -> ProcNumber {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn pg_strcasecmp(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!() // TODO: port/pgstrcasecmp.c
}
unsafe fn psprintf(_fmt: *const c_char, ...) -> *mut c_char {
    unimplemented!() // TODO: common/psprintf.c
}
unsafe fn list_free(_list: *mut List) {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn int64_to_numeric(_val: int64) -> Numeric {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe fn DirectFunctionCall1(_func: PGFunction, _arg1: Datum) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}
unsafe fn DirectFunctionCall2(_func: PGFunction, _arg1: Datum, _arg2: Datum) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}
unsafe fn DirectFunctionCall3(_func: PGFunction, _arg1: Datum, _arg2: Datum, _arg3: Datum) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}

type PGFunction = unsafe extern "C" fn(FunctionCallInfo) -> Datum;

unsafe extern "C" fn numeric_out(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_lt(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_abs(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_ge(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_add(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_sub(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_div_trunc(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_mul(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}
unsafe extern "C" fn numeric_int8(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric.c
}

// Datum conversion stubs
unsafe fn NumericGetDatum(_x: Numeric) -> Datum {
    unimplemented!() // TODO: utils/numeric.h
}
unsafe fn DatumGetNumeric(_d: Datum) -> Numeric {
    unimplemented!() // TODO: utils/numeric.h
}
unsafe fn DatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO: postgres.h
}
unsafe fn DatumGetBool(_d: Datum) -> bool {
    unimplemented!() // TODO: postgres.h
}
unsafe fn DatumGetInt64(_d: Datum) -> int64 {
    unimplemented!() // TODO: postgres.h
}
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int32GetDatum(_x: i32) -> Datum {
    unimplemented!() // TODO: postgres.h
}

// Inline-function-like stubs (from headers)
unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}
unsafe fn RelFileNumberIsValid(relnumber: RelFileNumber) -> bool {
    relnumber != InvalidRelFileNumber
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

// Macro-like helpers
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        // TODO: miscadmin.h
    }};
}
use CHECK_FOR_INTERRUPTS;

macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *mut c_char
    };
}
use NameStr;

macro_rules! GETSTRUCT {
    ($tuple:expr) => {
        GETSTRUCT_impl($tuple)
    };
}
use GETSTRUCT;
unsafe fn GETSTRUCT_impl(_tuple: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO: access/htup_details.h
}

macro_rules! RELKIND_HAS_STORAGE {
    ($relkind:expr) => {
        RELKIND_HAS_STORAGE_impl($relkind)
    };
}
use RELKIND_HAS_STORAGE;
unsafe fn RELKIND_HAS_STORAGE_impl(_relkind: c_char) -> bool {
    unimplemented!() // TODO: catalog/pg_class.h
}

macro_rules! VARSIZE_ANY_EXHDR {
    ($ptr:expr) => {
        VARSIZE_ANY_EXHDR_impl($ptr)
    };
}
use VARSIZE_ANY_EXHDR;
unsafe fn VARSIZE_ANY_EXHDR_impl(_ptr: *mut text) -> usize {
    unimplemented!() // TODO: c.h (postgres.h varatt)
}

// PG_FUNCTION argument/return macros (fmgr.h)
macro_rules! PG_GETARG_NUMERIC {
    ($fcinfo:expr, $n:expr) => {
        DatumGetNumeric(PG_GETARG_DATUM($fcinfo, $n))
    };
}
use PG_GETARG_NUMERIC;

unsafe fn PG_GETARG_DATUM(_fcinfo: FunctionCallInfo, _n: c_int) -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn set_isnull(_fcinfo: FunctionCallInfo, _isnull: bool) {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn DatumGetObjectId(_d: Datum) -> Oid {
    unimplemented!() // TODO: postgres.h
}
unsafe fn DatumGetName(_d: Datum) -> *mut Name {
    unimplemented!() // TODO: postgres.h
}
unsafe fn DatumGetTextPP(_d: Datum) -> *mut text {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int64GetDatum(_x: int64) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn PointerGetDatum(_p: *mut c_void) -> Datum {
    unimplemented!() // TODO: postgres.h
}

// MyDatabaseId / MyDatabaseTableSpace (miscadmin.h)
static MyDatabaseTableSpace: Oid = 0; // TODO: miscadmin.h
static MyDatabaseId: Oid = 0; // TODO: miscadmin.h
