/*-------------------------------------------------------------------------
 *
 * mbutils.c
 *	  This file contains functions for encoding conversion.
 *
 * The string-conversion functions in this file share some API quirks.
 * Note the following:
 *
 * The functions return a palloc'd, null-terminated string if conversion
 * is required.  However, if no conversion is performed, the given source
 * string pointer is returned as-is.
 *
 * Although the presence of a length argument means that callers can pass
 * non-null-terminated strings, care is required because the same string
 * will be passed back if no conversion occurs.  Such callers *must* check
 * whether result == src and handle that case differently.
 *
 * If the source and destination encodings are the same, the source string
 * is returned without any verification; it's assumed to be valid data.
 * If that might not be the case, the caller is responsible for validating
 * the string using a separate call to pg_verify_mbstr().  Whenever the
 * source and destination encodings are different, the functions ensure that
 * the result is validly encoded according to the destination encoding.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/mbutils.c
 *
 *-------------------------------------------------------------------------
 */

// #include "postgres.h"
// #include "access/xact.h"
// #include "catalog/namespace.h"
// #include "mb/pg_wchar.h"
// #include "utils/fmgrprotos.h"
// #include "utils/memdebug.h"
// #include "utils/memutils.h"
// #include "utils/relcache.h"
// #include "varatt.h"

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_assignments)]

use crate::prelude::*;

use crate::access::transam::xact::IsTransactionState;
use crate::catalog::namespace::FindDefaultConversionProc;
use crate::common::encnames::{pg_char_to_encoding, pg_encoding_to_char};
use crate::mb::pg_wchar::{
    is_valid_unicode_codepoint, pg_encoding_mblen_or_incomplete, pg_wchar_strlen,
    MAX_CONVERSION_GROWTH,
};
use crate::mb::wchar::{
    mbchar_verifier, mbcharacter_incrementer, mblen_converter, pg_enc2name,
    pg_encoding_max_length, pg_utf_mblen, pg_wchar, pg_wchar_table, unicode_to_utf8,
    MAX_MULTIBYTE_CHAR_LEN, PG_EUC_JP, PG_SQL_ASCII, PG_UTF8, PG_VALID_BE_ENCODING,
    PG_VALID_ENCODING, PG_VALID_FE_ENCODING, SS2, SS3,
};
use crate::common::encnames::pg_enc2name_tbl;
use crate::nodes::pg_list::{lcons, lfirst, List, NIL};
use crate::utils::fmgr::{
    fmgr_info_cxt, FmgrInfo, FunctionCall6Coll, FunctionCallInfo, OidFunctionCall6Coll,
};
use crate::utils::memutils::MaxAllocHugeSize;
use crate::utils::mmgr::mcxt::MemoryContextAllocHuge;
use crate::utils::adt::name::namein;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
// NameStr / NameData / Name / IS_HIGHBIT_SET / bytea / int32 / Size / Datum /
// VARHDRSZ come through the prelude (crate::c / crate::postgres).
use crate::{current_cell, foreach, foreach_delete_current};
use crate::{
    DirectFunctionCall1, DirectFunctionCall3, PG_FREE_IF_COPY, PG_GETARG_BYTEA_PP,
    PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_NAME, PG_RETURN_BYTEA_P, PG_RETURN_DATUM,
    PG_RETURN_INT32, PG_RETURN_NULL,
};

// MemoryContextAlloc / MemoryContextSwitchTo / TopMemoryContext /
// CurrentMemoryContext / MemoryContext / MaxAllocSize all come through the
// prelude (crate::utils::palloc + crate::utils::memutils).  IS_HIGHBIT_SET,
// VARHDRSZ, bytea, int32, Size, Datum come through crate::c / crate::postgres.

use core::ffi::{c_char, c_int, CStr};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn memchr(s: *const c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// VALGRIND_CHECK_MEM_IS_DEFINED is a no-op outside Valgrind-instrumented builds,
// matching the production macro expansion (memdebug.h).
macro_rules! VALGRIND_CHECK_MEM_IS_DEFINED {
    ($p:expr, $s:expr) => {};
}

// TODO(pg-port): AssertCouldGetRelation lives in utils/cache/relcache.c, not yet
// ported.  In assert-enabled builds it checks we hold a snapshot/relation; a
// no-op is faithful to the production (non-assert) build.
#[inline]
unsafe fn AssertCouldGetRelation() {}

// FunctionCall6/OidFunctionCall6: the C fmgr.h macros pass InvalidOid as the
// collation.  These thin wrappers delegate to the *Coll variants exactly as
// the C macros expand.
#[inline]
unsafe fn FunctionCall6(
    flinfo: *mut FmgrInfo,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
) -> Datum {
    FunctionCall6Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6)
}

#[inline]
unsafe fn OidFunctionCall6(
    functionId: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
) -> Datum {
    OidFunctionCall6Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6)
}

/*
 * We maintain a simple linked list caching the fmgr lookup info for the
 * currently selected conversion functions, as well as any that have been
 * selected previously in the current session.  (We remember previous
 * settings because we must be able to restore a previous setting during
 * transaction rollback, without doing any fresh catalog accesses.)
 *
 * Since we'll never release this data, we just keep it in TopMemoryContext.
 */
#[repr(C)]
struct ConvProcInfo {
    s_encoding: c_int, /* server and client encoding IDs */
    c_encoding: c_int,
    to_server_info: FmgrInfo, /* lookup info for conversion procs */
    to_client_info: FmgrInfo,
}

static mut ConvProcList: *mut List = NIL; /* List of ConvProcInfo */

/*
 * These variables point to the currently active conversion functions,
 * or are NULL when no conversion is needed.
 */
static mut ToServerConvProc: *mut FmgrInfo = null_mut();
static mut ToClientConvProc: *mut FmgrInfo = null_mut();

/*
 * This variable stores the conversion function to convert from UTF-8
 * to the server encoding.  It's NULL if the server encoding *is* UTF-8,
 * or if we lack a conversion function for this.
 */
static mut Utf8ToServerConvProc: *mut FmgrInfo = null_mut();

/*
 * These variables track the currently-selected encodings.
 */
static mut ClientEncoding: *const pg_enc2name = &pg_enc2name_tbl[PG_SQL_ASCII as usize];
static mut DatabaseEncoding: *const pg_enc2name = &pg_enc2name_tbl[PG_SQL_ASCII as usize];
static mut MessageEncoding: *const pg_enc2name = &pg_enc2name_tbl[PG_SQL_ASCII as usize];

/*
 * During backend startup we can't set client encoding because we (a)
 * can't look up the conversion functions, and (b) may not know the database
 * encoding yet either.  So SetClientEncoding() just accepts anything and
 * remembers it for InitializeClientEncoding() to apply later.
 */
static mut backend_startup_complete: bool = false;
static mut pending_client_encoding: c_int = PG_SQL_ASCII as c_int;

/*
 * Prepare for a future call to SetClientEncoding.  Success should mean
 * that SetClientEncoding is guaranteed to succeed for this encoding request.
 *
 * (But note that success before backend_startup_complete does not guarantee
 * success after ...)
 *
 * Returns 0 if okay, -1 if not (bad encoding or can't support conversion)
 */
pub unsafe fn PrepareClientEncoding(encoding: c_int) -> c_int {
    let current_server_encoding: c_int;

    if !PG_VALID_FE_ENCODING(encoding) {
        return -1;
    }

    /* Can't do anything during startup, per notes above */
    if !backend_startup_complete {
        return 0;
    }

    current_server_encoding = GetDatabaseEncoding();

    /*
     * Check for cases that require no conversion function.
     */
    if current_server_encoding == encoding
        || current_server_encoding == PG_SQL_ASCII as c_int
        || encoding == PG_SQL_ASCII as c_int
    {
        return 0;
    }

    if IsTransactionState() {
        /*
         * If we're in a live transaction, it's safe to access the catalogs,
         * so look up the functions.  We repeat the lookup even if the info is
         * already cached, so that we can react to changes in the contents of
         * pg_conversion.
         */
        let to_server_proc: Oid;
        let to_client_proc: Oid;
        let convinfo: *mut ConvProcInfo;
        let oldcontext: MemoryContext;

        to_server_proc = FindDefaultConversionProc(encoding, current_server_encoding);
        if !OidIsValid(to_server_proc) {
            return -1;
        }
        to_client_proc = FindDefaultConversionProc(current_server_encoding, encoding);
        if !OidIsValid(to_client_proc) {
            return -1;
        }

        /*
         * Load the fmgr info into TopMemoryContext (could still fail here)
         */
        convinfo = MemoryContextAlloc(TopMemoryContext, core::mem::size_of::<ConvProcInfo>())
            as *mut ConvProcInfo;
        (*convinfo).s_encoding = current_server_encoding;
        (*convinfo).c_encoding = encoding;
        fmgr_info_cxt(to_server_proc, &mut (*convinfo).to_server_info, TopMemoryContext);
        fmgr_info_cxt(to_client_proc, &mut (*convinfo).to_client_info, TopMemoryContext);

        /* Attach new info to head of list */
        oldcontext = MemoryContextSwitchTo(TopMemoryContext);
        ConvProcList = lcons(convinfo as *mut c_void, ConvProcList);
        MemoryContextSwitchTo(oldcontext);

        /*
         * We cannot yet remove any older entry for the same encoding pair,
         * since it could still be in use.  SetClientEncoding will clean up.
         */

        0 /* success */
    } else {
        /*
         * If we're not in a live transaction, the only thing we can do is
         * restore a previous setting using the cache.  This covers all
         * transaction-rollback cases.  The only case it might not work for is
         * trying to change client_encoding on the fly by editing
         * postgresql.conf and SIGHUP'ing.  Which would probably be a stupid
         * thing to do anyway.
         */
        foreach!(lc, ConvProcList, {
            let oldinfo: *mut ConvProcInfo = lfirst(current_cell!(lc)) as *mut ConvProcInfo;

            if (*oldinfo).s_encoding == current_server_encoding
                && (*oldinfo).c_encoding == encoding
            {
                return 0;
            }
        });

        -1 /* it's not cached, so fail */
    }
}

/*
 * Set the active client encoding and set up the conversion-function pointers.
 * PrepareClientEncoding should have been called previously for this encoding.
 *
 * Returns 0 if okay, -1 if not (bad encoding or can't support conversion)
 */
pub unsafe fn SetClientEncoding(encoding: c_int) -> c_int {
    let current_server_encoding: c_int;
    let mut found: bool;

    if !PG_VALID_FE_ENCODING(encoding) {
        return -1;
    }

    /* Can't do anything during startup, per notes above */
    if !backend_startup_complete {
        pending_client_encoding = encoding;
        return 0;
    }

    current_server_encoding = GetDatabaseEncoding();

    /*
     * Check for cases that require no conversion function.
     */
    if current_server_encoding == encoding
        || current_server_encoding == PG_SQL_ASCII as c_int
        || encoding == PG_SQL_ASCII as c_int
    {
        ClientEncoding = &pg_enc2name_tbl[encoding as usize];
        ToServerConvProc = null_mut();
        ToClientConvProc = null_mut();
        return 0;
    }

    /*
     * Search the cache for the entry previously prepared by
     * PrepareClientEncoding; if there isn't one, we lose.  While at it,
     * release any duplicate entries so that repeated Prepare/Set cycles don't
     * leak memory.
     */
    found = false;
    foreach!(lc, ConvProcList, {
        let convinfo: *mut ConvProcInfo = lfirst(current_cell!(lc)) as *mut ConvProcInfo;

        if (*convinfo).s_encoding == current_server_encoding
            && (*convinfo).c_encoding == encoding
        {
            if !found {
                /* Found newest entry, so set up */
                ClientEncoding = &pg_enc2name_tbl[encoding as usize];
                ToServerConvProc = &mut (*convinfo).to_server_info;
                ToClientConvProc = &mut (*convinfo).to_client_info;
                found = true;
            } else {
                /* Duplicate entry, release it */
                ConvProcList = foreach_delete_current!(ConvProcList, lc);
                pfree(convinfo as *mut c_void);
            }
        }
    });

    if found {
        0 /* success */
    } else {
        -1 /* it's not cached, so fail */
    }
}

/*
 * Initialize client encoding conversions.
 *		Called from InitPostgres() once during backend startup.
 */
pub unsafe fn InitializeClientEncoding() {
    let current_server_encoding: c_int;

    Assert!(!backend_startup_complete);
    backend_startup_complete = true;

    if PrepareClientEncoding(pending_client_encoding) < 0
        || SetClientEncoding(pending_client_encoding) < 0
    {
        /*
         * Oops, the requested conversion is not available. We couldn't fail
         * before, but we can now.
         */
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        ereport!(
            FATAL,
            errmsg!(
                "conversion between {} and {} is not supported",
                CStr::from_ptr(pg_enc2name_tbl[pending_client_encoding as usize].name)
                    .to_string_lossy(),
                CStr::from_ptr(GetDatabaseEncodingName()).to_string_lossy()
            )
        );
    }

    /*
     * Also look up the UTF8-to-server conversion function if needed.  Since
     * the server encoding is fixed within any one backend process, we don't
     * have to do this more than once.
     */
    current_server_encoding = GetDatabaseEncoding();
    if current_server_encoding != PG_UTF8 as c_int
        && current_server_encoding != PG_SQL_ASCII as c_int
    {
        let utf8_to_server_proc: Oid;

        AssertCouldGetRelation();
        utf8_to_server_proc =
            FindDefaultConversionProc(PG_UTF8 as c_int, current_server_encoding);
        /* If there's no such conversion, just leave the pointer as NULL */
        if OidIsValid(utf8_to_server_proc) {
            let finfo: *mut FmgrInfo;

            finfo = MemoryContextAlloc(TopMemoryContext, core::mem::size_of::<FmgrInfo>())
                as *mut FmgrInfo;
            fmgr_info_cxt(utf8_to_server_proc, finfo, TopMemoryContext);
            /* Set Utf8ToServerConvProc only after data is fully valid */
            Utf8ToServerConvProc = finfo;
        }
    }
}

/*
 * returns the current client encoding
 */
pub unsafe fn pg_get_client_encoding() -> c_int {
    (*ClientEncoding).encoding as c_int
}

/*
 * returns the current client encoding name
 */
pub unsafe fn pg_get_client_encoding_name() -> *const c_char {
    (*ClientEncoding).name
}

/*
 * Convert src string to another encoding (general case).
 *
 * See the notes about string conversion functions at the top of this file.
 */
pub unsafe fn pg_do_encoding_conversion(
    src: *mut c_uchar,
    len: c_int,
    src_encoding: c_int,
    dest_encoding: c_int,
) -> *mut c_uchar {
    let result: *mut c_uchar;
    let proc_: Oid;

    if len <= 0 {
        return src; /* empty string is always valid */
    }

    if src_encoding == dest_encoding {
        return src; /* no conversion required, assume valid */
    }

    if dest_encoding == PG_SQL_ASCII as c_int {
        return src; /* any string is valid in SQL_ASCII */
    }

    if src_encoding == PG_SQL_ASCII as c_int {
        /* No conversion is possible, but we must validate the result */
        pg_verify_mbstr(dest_encoding, src as *const c_char, len, false);
        return src;
    }

    if !IsTransactionState() {
        /* shouldn't happen */
        elog!(ERROR, "cannot perform encoding conversion outside a transaction");
    }

    proc_ = FindDefaultConversionProc(src_encoding, dest_encoding);
    if !OidIsValid(proc_) {
        // C also: errcode(ERRCODE_UNDEFINED_FUNCTION)
        ereport!(
            ERROR,
            errmsg!(
                "default conversion function for encoding \"{}\" to \"{}\" does not exist",
                CStr::from_ptr(pg_encoding_to_char(src_encoding)).to_string_lossy(),
                CStr::from_ptr(pg_encoding_to_char(dest_encoding)).to_string_lossy()
            )
        );
    }

    /*
     * Allocate space for conversion result, being wary of integer overflow.
     *
     * len * MAX_CONVERSION_GROWTH is typically a vast overestimate of the
     * required space, so it might exceed MaxAllocSize even though the result
     * would actually fit.  We do not want to hand back a result string that
     * exceeds MaxAllocSize, because callers might not cope gracefully --- but
     * if we just allocate more than that, and don't use it, that's fine.
     */
    if (len as Size) >= (MaxAllocHugeSize / (MAX_CONVERSION_GROWTH as Size)) {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        //         errdetail("String of %d bytes is too long for encoding conversion.", len)
        ereport!(ERROR, errmsg!("out of memory"));
    }

    result = MemoryContextAllocHuge(
        CurrentMemoryContext,
        (len as Size) * (MAX_CONVERSION_GROWTH as Size) + 1,
    ) as *mut c_uchar;

    OidFunctionCall6(
        proc_,
        Int32GetDatum(src_encoding),
        Int32GetDatum(dest_encoding),
        CStringGetDatum(src as *const c_char),
        CStringGetDatum(result as *const c_char),
        Int32GetDatum(len),
        BoolGetDatum(false),
    );

    /*
     * If the result is large, it's worth repalloc'ing to release any extra
     * space we asked for.  The cutoff here is somewhat arbitrary, but we
     * *must* check when len * MAX_CONVERSION_GROWTH exceeds MaxAllocSize.
     */
    if len > 1000000 {
        let resultlen: Size = strlen(result as *const c_char);

        if resultlen >= MaxAllocSize {
            // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            //         errdetail("String of %d bytes is too long for encoding conversion.", len)
            ereport!(ERROR, errmsg!("out of memory"));
        }

        return repalloc(result as *mut c_void, resultlen + 1) as *mut c_uchar;
    }

    result
}

/*
 * Convert src string to another encoding.
 *
 * This function has a different API than the other conversion functions.
 * The caller should've looked up the conversion function using
 * FindDefaultConversionProc().  Unlike the other functions, the converted
 * result is not palloc'd.  It is written to the caller-supplied buffer
 * instead.
 *
 * src_encoding   - encoding to convert from
 * dest_encoding  - encoding to convert to
 * src, srclen    - input buffer and its length in bytes
 * dest, destlen  - destination buffer and its size in bytes
 *
 * The output is null-terminated.
 *
 * If destlen < srclen * MAX_CONVERSION_INPUT_LENGTH + 1, the converted output
 * wouldn't necessarily fit in the output buffer, and the function will not
 * convert the whole input.
 *
 * TODO: The conversion function interface is not great.  Firstly, it
 * would be nice to pass through the destination buffer size to the
 * conversion function, so that if you pass a shorter destination buffer, it
 * could still continue to fill up the whole buffer.  Currently, we have to
 * assume worst case expansion and stop the conversion short, even if there
 * is in fact space left in the destination buffer.  Secondly, it would be
 * nice to return the number of bytes written to the caller, to avoid a call
 * to strlen().
 */
pub unsafe fn pg_do_encoding_conversion_buf(
    proc_: Oid,
    src_encoding: c_int,
    dest_encoding: c_int,
    src: *mut c_uchar,
    mut srclen: c_int,
    dest: *mut c_uchar,
    destlen: c_int,
    noError: bool,
) -> c_int {
    let result: Datum;

    /*
     * If the destination buffer is not large enough to hold the result in the
     * worst case, limit the input size passed to the conversion function.
     */
    if (srclen as Size) >= ((destlen - 1) as Size / (MAX_CONVERSION_GROWTH as Size)) {
        srclen = ((destlen - 1) as Size / (MAX_CONVERSION_GROWTH as Size)) as c_int;
    }

    result = OidFunctionCall6(
        proc_,
        Int32GetDatum(src_encoding),
        Int32GetDatum(dest_encoding),
        CStringGetDatum(src as *const c_char),
        CStringGetDatum(dest as *const c_char),
        Int32GetDatum(srclen),
        BoolGetDatum(noError),
    );
    DatumGetInt32(result)
}

/*
 * Convert string to encoding encoding_name. The source
 * encoding is the DB encoding.
 *
 * BYTEA convert_to(TEXT string, NAME encoding_name)
 */
pub unsafe fn pg_convert_to(fcinfo: FunctionCallInfo) -> Datum {
    let string: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let dest_encoding_name: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let src_encoding_name: Datum =
        DirectFunctionCall1!(namein, CStringGetDatum((*DatabaseEncoding).name));
    let result: Datum;

    /*
     * pg_convert expects a bytea as its first argument. We're passing it a
     * text argument here, relying on the fact that they are both in fact
     * varlena types, and thus structurally identical.
     */
    result = DirectFunctionCall3!(pg_convert, string, src_encoding_name, dest_encoding_name);

    PG_RETURN_DATUM!(result)
}

/*
 * Convert string from encoding encoding_name. The destination
 * encoding is the DB encoding.
 *
 * TEXT convert_from(BYTEA string, NAME encoding_name)
 */
pub unsafe fn pg_convert_from(fcinfo: FunctionCallInfo) -> Datum {
    let string: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let src_encoding_name: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let dest_encoding_name: Datum =
        DirectFunctionCall1!(namein, CStringGetDatum((*DatabaseEncoding).name));
    let result: Datum;

    result = DirectFunctionCall3!(pg_convert, string, src_encoding_name, dest_encoding_name);

    /*
     * pg_convert returns a bytea, which we in turn return as text, relying on
     * the fact that they are both in fact varlena types, and thus
     * structurally identical. Although not all bytea values are valid text,
     * in this case it will be because we've told pg_convert to return one
     * that is valid as text in the current database encoding.
     */
    PG_RETURN_DATUM!(result)
}

/*
 * Convert string between two arbitrary encodings.
 *
 * BYTEA convert(BYTEA string, NAME src_encoding_name, NAME dest_encoding_name)
 */
pub unsafe fn pg_convert(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let src_encoding_name: *const c_char = NameStr(&*PG_GETARG_NAME!(fcinfo, 1));
    let src_encoding: c_int = pg_char_to_encoding(src_encoding_name);
    let dest_encoding_name: *const c_char = NameStr(&*PG_GETARG_NAME!(fcinfo, 2));
    let dest_encoding: c_int = pg_char_to_encoding(dest_encoding_name);
    let src_str: *const c_char;
    let dest_str: *mut c_char;
    let retval: *mut bytea;
    let mut len: c_int;

    if src_encoding < 0 {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(
            ERROR,
            errmsg!(
                "invalid source encoding name \"{}\"",
                CStr::from_ptr(src_encoding_name).to_string_lossy()
            )
        );
    }
    if dest_encoding < 0 {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(
            ERROR,
            errmsg!(
                "invalid destination encoding name \"{}\"",
                CStr::from_ptr(dest_encoding_name).to_string_lossy()
            )
        );
    }

    /* make sure that source string is valid */
    len = VARSIZE_ANY_EXHDR(string as *const c_char) as c_int;
    src_str = VARDATA_ANY(string as *const c_char);
    pg_verify_mbstr(src_encoding, src_str, len, false);

    /* perform conversion */
    dest_str = pg_do_encoding_conversion(
        src_str as *mut c_uchar,
        len,
        src_encoding,
        dest_encoding,
    ) as *mut c_char;

    /* return source string if no conversion happened */
    if dest_str as *const c_char == src_str {
        PG_RETURN_BYTEA_P!(string);
    }

    /*
     * build bytea data type structure.
     */
    len = strlen(dest_str) as c_int;
    retval = palloc((len + VARHDRSZ) as usize) as *mut bytea;
    SET_VARSIZE(retval as *mut c_char, len + VARHDRSZ);
    memcpy(
        VARDATA(retval as *const c_char) as *mut c_void,
        dest_str as *const c_void,
        len as usize,
    );
    pfree(dest_str as *mut c_void);

    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY!(fcinfo, string, 0);

    PG_RETURN_BYTEA_P!(retval)
}

/*
 * get the length of the string considered as text in the specified
 * encoding. Raises an error if the data is not valid in that
 * encoding.
 *
 * INT4 length (BYTEA string, NAME src_encoding_name)
 */
pub unsafe fn length_in_encoding(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let src_encoding_name: *const c_char = NameStr(&*PG_GETARG_NAME!(fcinfo, 1));
    let src_encoding: c_int = pg_char_to_encoding(src_encoding_name);
    let src_str: *const c_char;
    let len: c_int;
    let retval: c_int;

    if src_encoding < 0 {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(
            ERROR,
            errmsg!(
                "invalid encoding name \"{}\"",
                CStr::from_ptr(src_encoding_name).to_string_lossy()
            )
        );
    }

    len = VARSIZE_ANY_EXHDR(string as *const c_char) as c_int;
    src_str = VARDATA_ANY(string as *const c_char);

    retval = pg_verify_mbstr_len(src_encoding, src_str, len, false);

    PG_RETURN_INT32!(retval)
}

/*
 * Get maximum multibyte character length in the specified encoding.
 *
 * Note encoding is specified numerically, not by name as above.
 */
pub unsafe fn pg_encoding_max_length_sql(fcinfo: FunctionCallInfo) -> Datum {
    let encoding: c_int = PG_GETARG_INT32!(fcinfo, 0);

    if PG_VALID_ENCODING(encoding) {
        PG_RETURN_INT32!(pg_wchar_table[encoding as usize].maxmblen)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

/*
 * Convert client encoding to server encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 */
pub unsafe fn pg_client_to_server(s: *const c_char, len: c_int) -> *mut c_char {
    pg_any_to_server(s, len, (*ClientEncoding).encoding as c_int)
}

/*
 * Convert any encoding to server encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 *
 * Unlike the other string conversion functions, this will apply validation
 * even if encoding == DatabaseEncoding->encoding.  This is because this is
 * used to process data coming in from outside the database, and we never
 * want to just assume validity.
 */
pub unsafe fn pg_any_to_server(s: *const c_char, len: c_int, encoding: c_int) -> *mut c_char {
    if len <= 0 {
        return s as *mut c_char; /* empty string is always valid */
    }

    if encoding == (*DatabaseEncoding).encoding as c_int || encoding == PG_SQL_ASCII as c_int {
        /*
         * No conversion is needed, but we must still validate the data.
         */
        pg_verify_mbstr((*DatabaseEncoding).encoding as c_int, s, len, false);
        return s as *mut c_char;
    }

    if (*DatabaseEncoding).encoding as c_int == PG_SQL_ASCII as c_int {
        /*
         * No conversion is possible, but we must still validate the data,
         * because the client-side code might have done string escaping using
         * the selected client_encoding.  If the client encoding is ASCII-safe
         * then we just do a straight validation under that encoding.  For an
         * ASCII-unsafe encoding we have a problem: we dare not pass such data
         * to the parser but we have no way to convert it.  We compromise by
         * rejecting the data if it contains any non-ASCII characters.
         */
        if PG_VALID_BE_ENCODING(encoding) {
            pg_verify_mbstr(encoding, s, len, false);
        } else {
            let mut i: c_int = 0;

            while i < len {
                if *s.add(i as usize) == b'\0' as c_char
                    || IS_HIGHBIT_SET(*s.add(i as usize) as u8)
                {
                    // C also: errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid byte value for encoding \"{}\": 0x{:02x}",
                            CStr::from_ptr(pg_enc2name_tbl[PG_SQL_ASCII as usize].name)
                                .to_string_lossy(),
                            *s.add(i as usize) as c_uchar
                        )
                    );
                }
                i += 1;
            }
        }
        return s as *mut c_char;
    }

    /* Fast path if we can use cached conversion function */
    if encoding == (*ClientEncoding).encoding as c_int {
        return perform_default_encoding_conversion(s, len, true);
    }

    /* General case ... will not work outside transactions */
    pg_do_encoding_conversion(
        s as *mut c_uchar,
        len,
        encoding,
        (*DatabaseEncoding).encoding as c_int,
    ) as *mut c_char
}

/*
 * Convert server encoding to client encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 */
pub unsafe fn pg_server_to_client(s: *const c_char, len: c_int) -> *mut c_char {
    pg_server_to_any(s, len, (*ClientEncoding).encoding as c_int)
}

/*
 * Convert server encoding to any encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 */
pub unsafe fn pg_server_to_any(s: *const c_char, len: c_int, encoding: c_int) -> *mut c_char {
    if len <= 0 {
        return s as *mut c_char; /* empty string is always valid */
    }

    if encoding == (*DatabaseEncoding).encoding as c_int || encoding == PG_SQL_ASCII as c_int {
        return s as *mut c_char; /* assume data is valid */
    }

    if (*DatabaseEncoding).encoding as c_int == PG_SQL_ASCII as c_int {
        /* No conversion is possible, but we must validate the result */
        pg_verify_mbstr(encoding, s, len, false);
        return s as *mut c_char;
    }

    /* Fast path if we can use cached conversion function */
    if encoding == (*ClientEncoding).encoding as c_int {
        return perform_default_encoding_conversion(s, len, false);
    }

    /* General case ... will not work outside transactions */
    pg_do_encoding_conversion(
        s as *mut c_uchar,
        len,
        (*DatabaseEncoding).encoding as c_int,
        encoding,
    ) as *mut c_char
}

/*
 *	Perform default encoding conversion using cached FmgrInfo. Since
 *	this function does not access database at all, it is safe to call
 *	outside transactions.  If the conversion has not been set up by
 *	SetClientEncoding(), no conversion is performed.
 */
unsafe fn perform_default_encoding_conversion(
    src: *const c_char,
    len: c_int,
    is_client_to_server: bool,
) -> *mut c_char {
    let mut result: *mut c_char;
    let src_encoding: c_int;
    let dest_encoding: c_int;
    let flinfo: *mut FmgrInfo;

    if is_client_to_server {
        src_encoding = (*ClientEncoding).encoding as c_int;
        dest_encoding = (*DatabaseEncoding).encoding as c_int;
        flinfo = ToServerConvProc;
    } else {
        src_encoding = (*DatabaseEncoding).encoding as c_int;
        dest_encoding = (*ClientEncoding).encoding as c_int;
        flinfo = ToClientConvProc;
    }

    if flinfo.is_null() {
        return src as *mut c_char;
    }

    /*
     * Allocate space for conversion result, being wary of integer overflow.
     * See comments in pg_do_encoding_conversion.
     */
    if (len as Size) >= (MaxAllocHugeSize / (MAX_CONVERSION_GROWTH as Size)) {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        //         errdetail("String of %d bytes is too long for encoding conversion.", len)
        ereport!(ERROR, errmsg!("out of memory"));
    }

    result = MemoryContextAllocHuge(
        CurrentMemoryContext,
        (len as Size) * (MAX_CONVERSION_GROWTH as Size) + 1,
    ) as *mut c_char;

    FunctionCall6(
        flinfo,
        Int32GetDatum(src_encoding),
        Int32GetDatum(dest_encoding),
        CStringGetDatum(src),
        CStringGetDatum(result),
        Int32GetDatum(len),
        BoolGetDatum(false),
    );

    /*
     * Release extra space if there might be a lot --- see comments in
     * pg_do_encoding_conversion.
     */
    if len > 1000000 {
        let resultlen: Size = strlen(result);

        if resultlen >= MaxAllocSize {
            // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            //         errdetail("String of %d bytes is too long for encoding conversion.", len)
            ereport!(ERROR, errmsg!("out of memory"));
        }

        result = repalloc(result as *mut c_void, resultlen + 1) as *mut c_char;
    }

    result
}

/*
 * Convert a single Unicode code point into a string in the server encoding.
 *
 * The code point given by "c" is converted and stored at *s, which must
 * have at least MAX_UNICODE_EQUIVALENT_STRING+1 bytes available.
 * The output will have a trailing '\0'.  Throws error if the conversion
 * cannot be performed.
 *
 * Note that this relies on having previously looked up any required
 * conversion function.  That's partly for speed but mostly because the parser
 * may call this outside any transaction, or in an aborted transaction.
 */
pub unsafe fn pg_unicode_to_server(c: pg_wchar, s: *mut c_uchar) {
    let mut c_as_utf8: [c_uchar; MAX_MULTIBYTE_CHAR_LEN as usize + 1] =
        [0; MAX_MULTIBYTE_CHAR_LEN as usize + 1];
    let c_as_utf8_len: c_int;
    let server_encoding: c_int;

    /*
     * Complain if invalid Unicode code point.  The choice of errcode here is
     * debatable, but really our caller should have checked this anyway.
     */
    if !is_valid_unicode_codepoint(c) {
        // C also: errcode(ERRCODE_SYNTAX_ERROR)
        ereport!(ERROR, errmsg!("invalid Unicode code point"));
    }

    /* Otherwise, if it's in ASCII range, conversion is trivial */
    if c <= 0x7F {
        *s.add(0) = c as c_uchar;
        *s.add(1) = b'\0';
        return;
    }

    /* If the server encoding is UTF-8, we just need to reformat the code */
    server_encoding = GetDatabaseEncoding();
    if server_encoding == PG_UTF8 as c_int {
        unicode_to_utf8(c, s);
        *s.add(pg_utf_mblen(s) as usize) = b'\0';
        return;
    }

    /* For all other cases, we must have a conversion function available */
    if Utf8ToServerConvProc.is_null() {
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        ereport!(
            ERROR,
            errmsg!(
                "conversion between {} and {} is not supported",
                CStr::from_ptr(pg_enc2name_tbl[PG_UTF8 as usize].name).to_string_lossy(),
                CStr::from_ptr(GetDatabaseEncodingName()).to_string_lossy()
            )
        );
    }

    /* Construct UTF-8 source string */
    unicode_to_utf8(c, c_as_utf8.as_mut_ptr());
    c_as_utf8_len = pg_utf_mblen(c_as_utf8.as_ptr());
    c_as_utf8[c_as_utf8_len as usize] = b'\0';

    /* Convert, or throw error if we can't */
    FunctionCall6(
        Utf8ToServerConvProc,
        Int32GetDatum(PG_UTF8 as c_int),
        Int32GetDatum(server_encoding),
        CStringGetDatum(c_as_utf8.as_ptr() as *const c_char),
        CStringGetDatum(s as *const c_char),
        Int32GetDatum(c_as_utf8_len),
        BoolGetDatum(false),
    );
}

/*
 * Convert a single Unicode code point into a string in the server encoding.
 *
 * Same as pg_unicode_to_server(), except that we don't throw errors,
 * but simply return false on conversion failure.
 */
pub unsafe fn pg_unicode_to_server_noerror(c: pg_wchar, s: *mut c_uchar) -> bool {
    let mut c_as_utf8: [c_uchar; MAX_MULTIBYTE_CHAR_LEN as usize + 1] =
        [0; MAX_MULTIBYTE_CHAR_LEN as usize + 1];
    let c_as_utf8_len: c_int;
    let converted_len: c_int;
    let server_encoding: c_int;

    /* Fail if invalid Unicode code point */
    if !is_valid_unicode_codepoint(c) {
        return false;
    }

    /* Otherwise, if it's in ASCII range, conversion is trivial */
    if c <= 0x7F {
        *s.add(0) = c as c_uchar;
        *s.add(1) = b'\0';
        return true;
    }

    /* If the server encoding is UTF-8, we just need to reformat the code */
    server_encoding = GetDatabaseEncoding();
    if server_encoding == PG_UTF8 as c_int {
        unicode_to_utf8(c, s);
        *s.add(pg_utf_mblen(s) as usize) = b'\0';
        return true;
    }

    /* For all other cases, we must have a conversion function available */
    if Utf8ToServerConvProc.is_null() {
        return false;
    }

    /* Construct UTF-8 source string */
    unicode_to_utf8(c, c_as_utf8.as_mut_ptr());
    c_as_utf8_len = pg_utf_mblen(c_as_utf8.as_ptr());
    c_as_utf8[c_as_utf8_len as usize] = b'\0';

    /* Convert, but without throwing error if we can't */
    converted_len = DatumGetInt32(FunctionCall6(
        Utf8ToServerConvProc,
        Int32GetDatum(PG_UTF8 as c_int),
        Int32GetDatum(server_encoding),
        CStringGetDatum(c_as_utf8.as_ptr() as *const c_char),
        CStringGetDatum(s as *const c_char),
        Int32GetDatum(c_as_utf8_len),
        BoolGetDatum(true),
    ));

    /* Conversion was successful iff it consumed the whole input */
    converted_len == c_as_utf8_len
}

/* convert a multibyte string to a wchar */
pub unsafe fn pg_mb2wchar(from: *const c_char, to: *mut pg_wchar) -> c_int {
    (pg_wchar_table[(*DatabaseEncoding).encoding as usize].mb2wchar_with_len.unwrap())(
        from as *const c_uchar,
        to,
        strlen(from) as c_int,
    )
}

/* convert a multibyte string to a wchar with a limited length */
pub unsafe fn pg_mb2wchar_with_len(from: *const c_char, to: *mut pg_wchar, len: c_int) -> c_int {
    (pg_wchar_table[(*DatabaseEncoding).encoding as usize].mb2wchar_with_len.unwrap())(
        from as *const c_uchar,
        to,
        len,
    )
}

/* same, with any encoding */
pub unsafe fn pg_encoding_mb2wchar_with_len(
    encoding: c_int,
    from: *const c_char,
    to: *mut pg_wchar,
    len: c_int,
) -> c_int {
    (pg_wchar_table[encoding as usize].mb2wchar_with_len.unwrap())(from as *const c_uchar, to, len)
}

/* convert a wchar string to a multibyte */
pub unsafe fn pg_wchar2mb(from: *const pg_wchar, to: *mut c_char) -> c_int {
    (pg_wchar_table[(*DatabaseEncoding).encoding as usize].wchar2mb_with_len.unwrap())(
        from,
        to as *mut c_uchar,
        pg_wchar_strlen(from) as c_int,
    )
}

/* convert a wchar string to a multibyte with a limited length */
pub unsafe fn pg_wchar2mb_with_len(from: *const pg_wchar, to: *mut c_char, len: c_int) -> c_int {
    (pg_wchar_table[(*DatabaseEncoding).encoding as usize].wchar2mb_with_len.unwrap())(
        from,
        to as *mut c_uchar,
        len,
    )
}

/* same, with any encoding */
pub unsafe fn pg_encoding_wchar2mb_with_len(
    encoding: c_int,
    from: *const pg_wchar,
    to: *mut c_char,
    len: c_int,
) -> c_int {
    (pg_wchar_table[encoding as usize].wchar2mb_with_len.unwrap())(from, to as *mut c_uchar, len)
}

/*
 * Returns the byte length of a multibyte character sequence in a
 * null-terminated string.  Raises an illegal byte sequence error if the
 * sequence would hit a null terminator.
 *
 * The caller is expected to have checked for a terminator at *mbstr == 0
 * before calling, but some callers want 1 in that case, so this function
 * continues that tradition.
 *
 * This must only be used for strings that have a null-terminator to enable
 * bounds detection.
 */
pub unsafe fn pg_mblen_cstr(mbstr: *const c_char) -> c_int {
    let length: c_int =
        (pg_wchar_table[(*DatabaseEncoding).encoding as usize].mblen.unwrap())(mbstr as *const c_uchar);

    /*
     * The .mblen functions return 1 when given a pointer to a terminator.
     * Some callers depend on that, so we tolerate it for now.  Well-behaved
     * callers check the leading byte for a terminator *before* calling.
     */
    let mut i: c_int = 1;
    while i < length {
        if unlikely(*mbstr.add(i as usize) == 0) {
            report_invalid_encoding_db(mbstr, length, i);
        }
        i += 1;
    }

    /*
     * String should be NUL-terminated, but checking that would make typical
     * callers O(N^2), tripling Valgrind check-world time.  Unless
     * VALGRIND_EXPENSIVE, check 1 byte after each actual character.  (If we
     * found a character, not a terminator, the next byte must be a terminator
     * or the start of the next character.)  If the caller iterates the whole
     * string, the last call will diagnose a missing terminator.
     */
    if *mbstr.add(0) != b'\0' as c_char {
        // #ifdef VALGRIND_EXPENSIVE
        //     VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, strlen(mbstr));
        // #else
        VALGRIND_CHECK_MEM_IS_DEFINED!(mbstr.add(length as usize), 1);
        // #endif
    }

    length
}

/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * [mbstr, end) of at least one byte in size.  Raises an illegal byte sequence
 * error if the sequence would exceed the range.
 */
pub unsafe fn pg_mblen_range(mbstr: *const c_char, end: *const c_char) -> c_int {
    let length: c_int =
        (pg_wchar_table[(*DatabaseEncoding).encoding as usize].mblen.unwrap())(mbstr as *const c_uchar);

    Assert!(end > mbstr);

    if unlikely(mbstr.add(length as usize) > end) {
        report_invalid_encoding_db(mbstr, length, end.offset_from(mbstr) as c_int);
    }

    // #ifdef VALGRIND_EXPENSIVE
    //     VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, end - mbstr);
    // #else
    VALGRIND_CHECK_MEM_IS_DEFINED!(mbstr, length);
    // #endif

    length
}

/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * extending for 'limit' bytes, which must be at least one.  Raises an illegal
 * byte sequence error if the sequence would exceed the range.
 */
pub unsafe fn pg_mblen_with_len(mbstr: *const c_char, limit: c_int) -> c_int {
    let length: c_int =
        (pg_wchar_table[(*DatabaseEncoding).encoding as usize].mblen.unwrap())(mbstr as *const c_uchar);

    Assert!(limit >= 1);

    if unlikely(length > limit) {
        report_invalid_encoding_db(mbstr, length, limit);
    }

    // #ifdef VALGRIND_EXPENSIVE
    //     VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, limit);
    // #else
    VALGRIND_CHECK_MEM_IS_DEFINED!(mbstr, length);
    // #endif

    length
}

/*
 * Returns the length of a multibyte character sequence, without any
 * validation of bounds.
 *
 * PLEASE NOTE:  This function can only be used safely if the caller has
 * already verified the input string, since otherwise there is a risk of
 * overrunning the buffer if the string is invalid.  A prior call to a
 * pg_mbstrlen* function suffices.
 */
pub unsafe fn pg_mblen_unbounded(mbstr: *const c_char) -> c_int {
    let length: c_int =
        (pg_wchar_table[(*DatabaseEncoding).encoding as usize].mblen.unwrap())(mbstr as *const c_uchar);

    VALGRIND_CHECK_MEM_IS_DEFINED!(mbstr, length);

    length
}

/*
 * Historical name for pg_mblen_unbounded().  Should not be used and will be
 * removed in a later version.
 */
pub unsafe fn pg_mblen(mbstr: *const c_char) -> c_int {
    pg_mblen_unbounded(mbstr)
}

/* returns the display length of a multibyte character */
pub unsafe fn pg_dsplen(mbstr: *const c_char) -> c_int {
    (pg_wchar_table[(*DatabaseEncoding).encoding as usize].dsplen.unwrap())(mbstr as *const c_uchar)
}

/* returns the length (counted in wchars) of a multibyte string */
pub unsafe fn pg_mbstrlen(mut mbstr: *const c_char) -> c_int {
    let mut len: c_int = 0;

    /* optimization for single byte encoding */
    if pg_database_encoding_max_length() == 1 {
        return strlen(mbstr) as c_int;
    }

    while *mbstr != 0 {
        mbstr = mbstr.add(pg_mblen_cstr(mbstr) as usize);
        len += 1;
    }
    len
}

/* returns the length (counted in wchars) of a multibyte string
 * (stops at the first of "limit" or a NUL)
 */
pub unsafe fn pg_mbstrlen_with_len(mut mbstr: *const c_char, mut limit: c_int) -> c_int {
    let mut len: c_int = 0;

    /* optimization for single byte encoding */
    if pg_database_encoding_max_length() == 1 {
        return limit;
    }

    while limit > 0 && *mbstr != 0 {
        let l: c_int = pg_mblen_with_len(mbstr, limit);

        limit -= l;
        mbstr = mbstr.add(l as usize);
        len += 1;
    }
    len
}

/*
 * returns the byte length of a multibyte string
 * (not necessarily NULL terminated)
 * that is no longer than limit.
 * this function does not break multibyte character boundary.
 */
pub unsafe fn pg_mbcliplen(mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    pg_encoding_mbcliplen((*DatabaseEncoding).encoding as c_int, mbstr, len, limit)
}

/*
 * pg_mbcliplen with specified encoding; string must be valid in encoding
 */
pub unsafe fn pg_encoding_mbcliplen(
    encoding: c_int,
    mut mbstr: *const c_char,
    mut len: c_int,
    limit: c_int,
) -> c_int {
    let mblen_fn: mblen_converter;
    let mut clen: c_int = 0;
    let mut l: c_int;

    /* optimization for single byte encoding */
    if pg_encoding_max_length(encoding) == 1 {
        return cliplen(mbstr, len, limit);
    }

    mblen_fn = pg_wchar_table[encoding as usize].mblen;

    while len > 0 && *mbstr != 0 {
        l = (mblen_fn.unwrap())(mbstr as *const c_uchar);
        if (clen + l) > limit {
            break;
        }
        clen += l;
        if clen == limit {
            break;
        }
        len -= l;
        mbstr = mbstr.add(l as usize);
    }
    clen
}

/*
 * Similar to pg_mbcliplen except the limit parameter specifies the
 * character length, not the byte length.
 */
pub unsafe fn pg_mbcharcliplen(mut mbstr: *const c_char, mut len: c_int, limit: c_int) -> c_int {
    let mut clen: c_int = 0;
    let mut nch: c_int = 0;
    let mut l: c_int;

    /* optimization for single byte encoding */
    if pg_database_encoding_max_length() == 1 {
        return cliplen(mbstr, len, limit);
    }

    while len > 0 && *mbstr != 0 {
        l = pg_mblen_with_len(mbstr, len);
        nch += 1;
        if nch > limit {
            break;
        }
        clen += l;
        len -= l;
        mbstr = mbstr.add(l as usize);
    }
    clen
}

/* mbcliplen for any single-byte encoding */
unsafe fn cliplen(str: *const c_char, mut len: c_int, limit: c_int) -> c_int {
    let mut l: c_int = 0;

    len = Min(len, limit);
    while l < len && *str.add(l as usize) != 0 {
        l += 1;
    }
    l
}

pub unsafe fn SetDatabaseEncoding(encoding: c_int) {
    if !PG_VALID_BE_ENCODING(encoding) {
        elog!(ERROR, "invalid database encoding: {}", encoding);
    }

    DatabaseEncoding = &pg_enc2name_tbl[encoding as usize];
    Assert!((*DatabaseEncoding).encoding as c_int == encoding);
}

pub unsafe fn SetMessageEncoding(encoding: c_int) {
    /* Some calls happen before we can elog()! */
    Assert!(PG_VALID_ENCODING(encoding));

    MessageEncoding = &pg_enc2name_tbl[encoding as usize];
    Assert!((*MessageEncoding).encoding as c_int == encoding);
}

// #ifdef ENABLE_NLS
/*
 * Make one bind_textdomain_codeset() call, translating a pg_enc to a gettext
 * codeset.  Fails for MULE_INTERNAL, an encoding unknown to gettext; can also
 * fail for gettext-internal causes like out-of-memory.
 */
#[cfg(feature = "enable_nls")]
unsafe fn raw_pg_bind_textdomain_codeset(domainname: *const c_char, encoding: c_int) -> bool {
    use crate::common::encnames::pg_enc2gettext_tbl;
    use crate::utils::mmgr::mcxt::CurrentMemoryContext;

    let elog_ok = !CurrentMemoryContext.is_null();

    if !PG_VALID_ENCODING(encoding) || pg_enc2gettext_tbl[encoding as usize].is_null() {
        return false;
    }

    if !bind_textdomain_codeset(domainname, pg_enc2gettext_tbl[encoding as usize]).is_null() {
        return true;
    }

    if elog_ok {
        elog!(LOG, "bind_textdomain_codeset failed");
    } else {
        crate::utils::error::elog_impl::write_stderr(
            c"bind_textdomain_codeset failed".as_ptr(),
        );
    }

    false
}

/*
 * Bind a gettext message domain to the codeset corresponding to the database
 * encoding.  For SQL_ASCII, instead bind to the codeset implied by LC_CTYPE.
 * Return the MessageEncoding implied by the new settings.
 *
 * On most platforms, gettext defaults to the codeset implied by LC_CTYPE.
 * When that matches the database encoding, we don't need to do anything.  In
 * CREATE DATABASE, we enforce or trust that the locale's codeset matches the
 * database encoding, except for the C locale.  (On Windows, we also permit a
 * discrepancy under the UTF8 encoding.)  For the C locale, explicitly bind
 * gettext to the right codeset.
 *
 * On Windows, gettext defaults to the Windows ANSI code page.  This is a
 * convenient departure for software that passes the strings to Windows ANSI
 * APIs, but we don't do that.  Compel gettext to use database encoding or,
 * failing that, the LC_CTYPE encoding as it would on other platforms.
 *
 * This function is called before elog() and palloc() are usable.
 */
#[cfg(feature = "enable_nls")]
pub unsafe fn pg_bind_textdomain_codeset(domainname: *const c_char) -> c_int {
    use crate::utils::mmgr::mcxt::CurrentMemoryContext;

    let elog_ok = !CurrentMemoryContext.is_null();
    let encoding = GetDatabaseEncoding();
    let mut new_msgenc: c_int;

    // #ifndef WIN32
    let ctype = setlocale(LC_CTYPE, std::ptr::null());

    if pg_strcasecmp(ctype, c"C".as_ptr()) == 0 || pg_strcasecmp(ctype, c"POSIX".as_ptr()) == 0
    // #endif
    {
        if encoding != PG_SQL_ASCII as c_int
            && raw_pg_bind_textdomain_codeset(domainname, encoding)
        {
            return encoding;
        }
    }

    new_msgenc = pg_get_encoding_from_locale(std::ptr::null(), elog_ok);
    if new_msgenc < 0 {
        new_msgenc = PG_SQL_ASCII as c_int;
    }

    // #ifdef WIN32
    // if !raw_pg_bind_textdomain_codeset(domainname, new_msgenc) {
    //     /* On failure, the old message encoding remains valid. */
    //     return GetMessageEncoding();
    // }
    // #endif

    new_msgenc
}

#[cfg(feature = "enable_nls")]
extern "C" {
    fn bind_textdomain_codeset(domainname: *const c_char, codeset: *const c_char) -> *mut c_char;
}

#[cfg(feature = "enable_nls")]
extern "C" {
    fn setlocale(category: c_int, locale: *const c_char) -> *mut c_char;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn pg_get_encoding_from_locale(ctype: *const c_char, write_message: bool) -> c_int;
}

#[cfg(feature = "enable_nls")]
const LC_CTYPE: c_int = 2;
// #endif /* ENABLE_NLS */

/*
 * The database encoding, also called the server encoding, represents the
 * encoding of data stored in text-like data types.  Affected types include
 * cstring, text, varchar, name, xml, and json.
 */
pub unsafe fn GetDatabaseEncoding() -> c_int {
    (*DatabaseEncoding).encoding as c_int
}

pub unsafe fn GetDatabaseEncodingName() -> *const c_char {
    (*DatabaseEncoding).name
}

pub unsafe fn getdatabaseencoding(_fcinfo: FunctionCallInfo) -> Datum {
    DirectFunctionCall1!(namein, CStringGetDatum((*DatabaseEncoding).name))
}

pub unsafe fn pg_client_encoding(_fcinfo: FunctionCallInfo) -> Datum {
    DirectFunctionCall1!(namein, CStringGetDatum((*ClientEncoding).name))
}

pub unsafe fn PG_char_to_encoding(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut NameData = PG_GETARG_NAME!(fcinfo, 0);

    PG_RETURN_INT32!(pg_char_to_encoding(NameStr(&*s)))
}

pub unsafe fn PG_encoding_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let encoding: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let encoding_name: *const c_char = pg_encoding_to_char(encoding);

    DirectFunctionCall1!(namein, CStringGetDatum(encoding_name))
}

/*
 * gettext() returns messages in this encoding.  This often matches the
 * database encoding, but it differs for SQL_ASCII databases, for processes
 * not attached to a database, and under a database encoding lacking iconv
 * support (MULE_INTERNAL).
 */
pub unsafe fn GetMessageEncoding() -> c_int {
    (*MessageEncoding).encoding as c_int
}

/*
 * Convert from MessageEncoding to a palloc'd, null terminated utf16 string.
 * The output parameter utf16len is set to the number of UTF16 code points,
 * not counting the null terminator.  Returns NULL on SQL_ASCII or error.
 * (WIN32-only in C.)
 */
#[cfg(windows)]
unsafe fn pgwin32_message_to_UTF16(
    str: *const c_char,
    mut len: c_int,
    utf16len: *mut c_int,
) -> *mut WCHAR {
    extern "C" {
        fn MultiByteToWideChar(
            code_page: UINT,
            dw_flags: u32,
            lp_multi_byte_str: *const c_char,
            cb_multi_byte: c_int,
            lp_wide_char_str: *mut WCHAR,
            cch_wide_char: c_int,
        ) -> c_int;
    }
    const CP_UTF8: UINT = 65001;

    let msgenc = GetMessageEncoding();
    let utf16: *mut WCHAR;
    let dstlen: c_int;

    if msgenc == PG_SQL_ASCII as c_int {
        /* No conversion is possible, and SQL_ASCII is never utf16. */
        return null_mut();
    }

    let codepage: UINT = pg_enc2name_tbl[msgenc as usize].codepage;

    /*
     * Use MultiByteToWideChar directly if there is a corresponding codepage,
     * or double conversion through UTF8 if not.
     */
    if codepage != 0 {
        utf16 = palloc(core::mem::size_of::<WCHAR>() * (len as usize + 1)) as *mut WCHAR;
        dstlen = MultiByteToWideChar(codepage, 0, str, len, utf16, len);
        *utf16.add(dstlen as usize) = 0 as WCHAR;
    } else {
        let utf8: *mut c_char;

        /*
         * XXX pg_do_encoding_conversion() requires a transaction.  In the
         * absence of one, hope for the input to be valid UTF8.
         */
        if IsTransactionState() {
            utf8 = pg_do_encoding_conversion(
                str as *mut c_uchar,
                len,
                msgenc,
                PG_UTF8 as c_int,
            ) as *mut c_char;
            if utf8 != str as *mut c_char {
                len = strlen(utf8) as c_int;
            }
        } else {
            utf8 = str as *mut c_char;
        }

        utf16 = palloc(core::mem::size_of::<WCHAR>() * (len as usize + 1)) as *mut WCHAR;
        dstlen = MultiByteToWideChar(CP_UTF8, 0, utf8, len, utf16, len);
        *utf16.add(dstlen as usize) = 0 as WCHAR;

        if utf8 != str as *mut c_char {
            pfree(utf8 as *mut c_void);
        }
    }

    if dstlen == 0 && len > 0 {
        pfree(utf16 as *mut c_void);
        return null_mut(); /* error */
    }

    if !utf16len.is_null() {
        *utf16len = dstlen;
    }
    utf16
}

/*
 * Generic character incrementer function.
 *
 * Not knowing anything about the properties of the encoding in use, we just
 * keep incrementing the last byte until we get a validly-encoded result,
 * or we run out of values to try.  We don't bother to try incrementing
 * higher-order bytes, so there's no growth in runtime for wider characters.
 * (If we did try to do that, we'd need to consider the likelihood that 255
 * is not a valid final byte in the encoding.)
 */
unsafe extern "C" fn pg_generic_charinc(charptr: *mut c_uchar, len: c_int) -> bool {
    let lastbyte: *mut c_uchar = charptr.add((len - 1) as usize);
    let mbverify: mbchar_verifier;

    /* We can just invoke the character verifier directly. */
    mbverify = pg_wchar_table[GetDatabaseEncoding() as usize].mbverifychar;

    while *lastbyte < 255u8 {
        *lastbyte += 1;
        if (mbverify.unwrap())(charptr, len) == len {
            return true;
        }
    }

    false
}

/*
 * UTF-8 character incrementer function.
 *
 * For a one-byte character less than 0x7F, we just increment the byte.
 *
 * For a multibyte character, every byte but the first must fall between 0x80
 * and 0xBF; and the first byte must be between 0xC0 and 0xF4.  We increment
 * the last byte that's not already at its maximum value.  If we can't find a
 * byte that's less than the maximum allowable value, we simply fail.  We also
 * need some special-case logic to skip regions used for surrogate pair
 * handling, as those should not occur in valid UTF-8.
 *
 * Note that we don't reset lower-order bytes back to their minimums, since
 * we can't afford to make an exhaustive search (see make_greater_string).
 */
unsafe extern "C" fn pg_utf8_increment(charptr: *mut c_uchar, length: c_int) -> bool {
    #[allow(unused_assignments)]
    let mut a: c_uchar;
    let limit: c_uchar;

    // The C switch falls through cases 4 -> 3 -> 2 -> 1, with `break` exiting the
    // switch.  We model it with nested labeled blocks: 'sw is the whole switch;
    // each inner block is one case.  Entering case N runs blocks N..1 in order
    // (the fall-through), and a C `break` becomes `break 'sw`.  We dispatch the
    // entry point by replicating the case-label ranges (default rejects 5,6).
    'sw: {
        // default: reject lengths 5 and 6 for now
        if length >= 5 {
            return false;
        }

        'case4: {
            if length != 4 {
                break 'case4;
            }
            a = *charptr.add(3);
            if a < 0xBF {
                *charptr.add(3) += 1;
                break 'sw;
            }
            /* FALL THRU */
        }
        'case3: {
            if length < 3 {
                break 'case3;
            }
            a = *charptr.add(2);
            if a < 0xBF {
                *charptr.add(2) += 1;
                break 'sw;
            }
            /* FALL THRU */
        }
        'case2: {
            if length < 2 {
                break 'case2;
            }
            a = *charptr.add(1);
            match *charptr {
                0xED => {
                    limit = 0x9F;
                }
                0xF4 => {
                    limit = 0x8F;
                }
                _ => {
                    limit = 0xBF;
                }
            }
            if a < limit {
                *charptr.add(1) += 1;
                break 'sw;
            }
            /* FALL THRU */
        }
        // case 1:
        a = *charptr;
        if a == 0x7F || a == 0xDF || a == 0xEF || a == 0xF4 {
            return false;
        }
        *charptr.add(0) += 1;
    }

    true
}

/*
 * EUC-JP character incrementer function.
 *
 * If the sequence starts with SS2 (0x8e), it must be a two-byte sequence
 * representing JIS X 0201 characters with the second byte ranging between
 * 0xa1 and 0xdf.  We just increment the last byte if it's less than 0xdf,
 * and otherwise rewrite the whole sequence to 0xa1 0xa1.
 *
 * If the sequence starts with SS3 (0x8f), it must be a three-byte sequence
 * in which the last two bytes range between 0xa1 and 0xfe.  The last byte
 * is incremented if possible, otherwise the second-to-last byte.
 *
 * If the sequence starts with a value other than the above and its MSB
 * is set, it must be a two-byte sequence representing JIS X 0208 characters
 * with both bytes ranging between 0xa1 and 0xfe.  The last byte is
 * incremented if possible, otherwise the second-to-last byte.
 *
 * Otherwise, the sequence is a single-byte ASCII character. It is
 * incremented up to 0x7f.
 */
unsafe extern "C" fn pg_eucjp_increment(charptr: *mut c_uchar, length: c_int) -> bool {
    let c1: c_uchar;
    let mut c2: c_uchar;
    let mut i: c_int;

    c1 = *charptr;

    match c1 {
        SS2 => {
            /* JIS X 0201 */
            if length != 2 {
                return false;
            }

            c2 = *charptr.add(1);

            if c2 >= 0xdf {
                *charptr.add(0) = 0xa1;
                *charptr.add(1) = 0xa1;
            } else if c2 < 0xa1 {
                *charptr.add(1) = 0xa1;
            } else {
                *charptr.add(1) += 1;
            }
        }

        SS3 => {
            /* JIS X 0212 */
            if length != 3 {
                return false;
            }

            i = 2;
            while i > 0 {
                c2 = *charptr.add(i as usize);
                if c2 < 0xa1 {
                    *charptr.add(i as usize) = 0xa1;
                    return true;
                } else if c2 < 0xfe {
                    *charptr.add(i as usize) += 1;
                    return true;
                }
                i -= 1;
            }

            /* Out of 3-byte code region */
            return false;
        }

        _ => {
            if IS_HIGHBIT_SET(c1) {
                /* JIS X 0208? */
                if length != 2 {
                    return false;
                }

                i = 1;
                while i >= 0 {
                    c2 = *charptr.add(i as usize);
                    if c2 < 0xa1 {
                        *charptr.add(i as usize) = 0xa1;
                        return true;
                    } else if c2 < 0xfe {
                        *charptr.add(i as usize) += 1;
                        return true;
                    }
                    i -= 1;
                }

                /* Out of 2 byte code region */
                return false;
            } else {
                /* ASCII, single byte */
                if c1 > 0x7e {
                    return false;
                }
                *charptr.add(0) += 1;
            }
        }
    }

    true
}

/*
 * get the character incrementer for the encoding for the current database
 */
pub unsafe fn pg_database_encoding_character_incrementer() -> mbcharacter_incrementer {
    /*
     * Eventually it might be best to add a field to pg_wchar_table[], but for
     * now we just use a switch.
     */
    match GetDatabaseEncoding() {
        x if x == PG_UTF8 as c_int => Some(pg_utf8_increment),
        x if x == PG_EUC_JP as c_int => Some(pg_eucjp_increment),
        _ => Some(pg_generic_charinc),
    }
}

/*
 * fetch maximum length of the encoding for the current database
 */
pub unsafe fn pg_database_encoding_max_length() -> c_int {
    pg_wchar_table[GetDatabaseEncoding() as usize].maxmblen
}

/*
 * Verify mbstr to make sure that it is validly encoded in the current
 * database encoding.  Otherwise same as pg_verify_mbstr().
 */
pub unsafe fn pg_verifymbstr(mbstr: *const c_char, len: c_int, noError: bool) -> bool {
    pg_verify_mbstr(GetDatabaseEncoding(), mbstr, len, noError)
}

/*
 * Verify mbstr to make sure that it is validly encoded in the specified
 * encoding.
 */
pub unsafe fn pg_verify_mbstr(encoding: c_int, mbstr: *const c_char, len: c_int, noError: bool) -> bool {
    let oklen: c_int;

    Assert!(PG_VALID_ENCODING(encoding));

    oklen = (pg_wchar_table[encoding as usize].mbverifystr.unwrap())(mbstr as *const c_uchar, len);
    if oklen != len {
        if noError {
            return false;
        }
        report_invalid_encoding(encoding, mbstr.add(oklen as usize), len - oklen);
    }
    true
}

/*
 * Verify mbstr to make sure that it is validly encoded in the specified
 * encoding.
 *
 * mbstr is not necessarily zero terminated; length of mbstr is
 * specified by len.
 *
 * If OK, return length of string in the encoding.
 * If a problem is found, return -1 when noError is
 * true; when noError is false, ereport() a descriptive message.
 *
 * Note: We cannot use the faster encoding-specific mbverifystr() function
 * here, because we need to count the number of characters in the string.
 */
pub unsafe fn pg_verify_mbstr_len(
    encoding: c_int,
    mut mbstr: *const c_char,
    mut len: c_int,
    noError: bool,
) -> c_int {
    let mbverifychar: mbchar_verifier;
    let mut mb_len: c_int;

    Assert!(PG_VALID_ENCODING(encoding));

    /*
     * In single-byte encodings, we need only reject nulls (\0).
     */
    if pg_encoding_max_length(encoding) <= 1 {
        let nullpos: *const c_char = memchr(mbstr as *const c_void, 0, len as usize) as *const c_char;

        if nullpos.is_null() {
            return len;
        }
        if noError {
            return -1;
        }
        report_invalid_encoding(encoding, nullpos, 1);
    }

    /* fetch function pointer just once */
    mbverifychar = pg_wchar_table[encoding as usize].mbverifychar;

    mb_len = 0;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*mbstr as u8) {
            if *mbstr != b'\0' as c_char {
                mb_len += 1;
                mbstr = mbstr.add(1);
                len -= 1;
                continue;
            }
            if noError {
                return -1;
            }
            report_invalid_encoding(encoding, mbstr, len);
        }

        l = (mbverifychar.unwrap())(mbstr as *const c_uchar, len);

        if l < 0 {
            if noError {
                return -1;
            }
            report_invalid_encoding(encoding, mbstr, len);
        }

        mbstr = mbstr.add(l as usize);
        len -= l;
        mb_len += 1;
    }
    mb_len
}

/*
 * check_encoding_conversion_args: check arguments of a conversion function
 *
 * "expected" arguments can be either an encoding ID or -1 to indicate that
 * the caller will check whether it accepts the ID.
 *
 * Note: the errors here are not really user-facing, so elog instead of
 * ereport seems sufficient.  Also, we trust that the "expected" encoding
 * arguments are valid encoding IDs, but we don't trust the actuals.
 */
pub unsafe fn check_encoding_conversion_args(
    src_encoding: c_int,
    dest_encoding: c_int,
    len: c_int,
    expected_src_encoding: c_int,
    expected_dest_encoding: c_int,
) {
    if !PG_VALID_ENCODING(src_encoding) {
        elog!(ERROR, "invalid source encoding ID: {}", src_encoding);
    }
    if src_encoding != expected_src_encoding && expected_src_encoding >= 0 {
        elog!(
            ERROR,
            "expected source encoding \"{}\", but got \"{}\"",
            CStr::from_ptr(pg_enc2name_tbl[expected_src_encoding as usize].name).to_string_lossy(),
            CStr::from_ptr(pg_enc2name_tbl[src_encoding as usize].name).to_string_lossy()
        );
    }
    if !PG_VALID_ENCODING(dest_encoding) {
        elog!(ERROR, "invalid destination encoding ID: {}", dest_encoding);
    }
    if dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0 {
        elog!(
            ERROR,
            "expected destination encoding \"{}\", but got \"{}\"",
            CStr::from_ptr(pg_enc2name_tbl[expected_dest_encoding as usize].name).to_string_lossy(),
            CStr::from_ptr(pg_enc2name_tbl[dest_encoding as usize].name).to_string_lossy()
        );
    }
    if len < 0 {
        elog!(ERROR, "encoding conversion length must not be negative");
    }
}

/*
 * report_invalid_encoding: complain about invalid multibyte character
 *
 * note: len is remaining length of string, not length of character;
 * len must be greater than zero (or we'd neglect initializing "buf").
 */
#[no_mangle]
pub unsafe fn report_invalid_encoding(encoding: c_int, mbstr: *const c_char, len: c_int) -> ! {
    let l: c_int = pg_encoding_mblen_or_incomplete(encoding, mbstr, len as Size);

    report_invalid_encoding_int(encoding, mbstr, l, len)
}

unsafe fn report_invalid_encoding_int(
    encoding: c_int,
    mbstr: *const c_char,
    mblen: c_int,
    len: c_int,
) -> ! {
    let mut buf = String::new();
    let mut j: c_int;
    let mut jlimit: c_int;

    jlimit = Min(mblen, len);
    jlimit = Min(jlimit, 8); /* prevent buffer overrun */

    j = 0;
    while j < jlimit {
        buf.push_str(&format!("0x{:02x}", *mbstr.add(j as usize) as c_uchar));
        if j < jlimit - 1 {
            buf.push(' ');
        }
        j += 1;
    }

    // C also: errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
    ereport!(
        ERROR,
        errmsg!(
            "invalid byte sequence for encoding \"{}\": {}",
            CStr::from_ptr(pg_enc2name_tbl[encoding as usize].name).to_string_lossy(),
            buf
        )
    );
    unreachable!()
}

unsafe fn report_invalid_encoding_db(mbstr: *const c_char, mblen: c_int, len: c_int) -> ! {
    report_invalid_encoding_int(GetDatabaseEncoding(), mbstr, mblen, len)
}

/*
 * report_untranslatable_char: complain about untranslatable character
 *
 * note: len is remaining length of string, not length of character;
 * len must be greater than zero (or we'd neglect initializing "buf").
 */
pub unsafe fn report_untranslatable_char(
    src_encoding: c_int,
    dest_encoding: c_int,
    mbstr: *const c_char,
    len: c_int,
) -> ! {
    let l: c_int;
    let mut buf = String::new();
    let mut j: c_int;
    let mut jlimit: c_int;

    /*
     * We probably could use plain pg_encoding_mblen(), because
     * gb18030_to_utf8() verifies before it converts.  All conversions should.
     * For src_encoding!=GB18030, len>0 meets pg_encoding_mblen() needs.  Even
     * so, be defensive, since a buggy conversion might pass invalid data.
     * This is not a performance-critical path.
     */
    l = pg_encoding_mblen_or_incomplete(src_encoding, mbstr, len as Size);
    jlimit = Min(l, len);
    jlimit = Min(jlimit, 8); /* prevent buffer overrun */

    j = 0;
    while j < jlimit {
        buf.push_str(&format!("0x{:02x}", *mbstr.add(j as usize) as c_uchar));
        if j < jlimit - 1 {
            buf.push(' ');
        }
        j += 1;
    }

    // C also: errcode(ERRCODE_UNTRANSLATABLE_CHARACTER)
    ereport!(
        ERROR,
        errmsg!(
            "character with byte sequence {} in encoding \"{}\" has no equivalent in encoding \"{}\"",
            buf,
            CStr::from_ptr(pg_enc2name_tbl[src_encoding as usize].name).to_string_lossy(),
            CStr::from_ptr(pg_enc2name_tbl[dest_encoding as usize].name).to_string_lossy()
        )
    );
    unreachable!()
}

// #ifdef WIN32: pgwin32_message_to_UTF16 is Windows-only and not built in this
// port (Darwin/non-WIN32).
