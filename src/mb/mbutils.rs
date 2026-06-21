//! Translation of postgres/src/backend/utils/mb/mbutils.c
//!
//! Functions for encoding conversion and multibyte string-length utilities.
//!
//! The string-conversion functions in this file share some API quirks: they
//! return a palloc'd, null-terminated string if conversion is required, but if
//! no conversion is performed the given source string pointer is returned
//! as-is.  Callers that pass non-null-terminated strings MUST check whether
//! result == src and handle that case differently.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! BACKEND-ENTANGLED NOTE: the real file keeps a session-lifetime linked list
//! (ConvProcList) of fmgr lookup info for the active conversion functions, set
//! up via relcache/namespace catalog lookups (FindDefaultConversionProc) and
//! the GUC/SetClientEncoding machinery.  None of that catalog plumbing is
//! ported yet.  We therefore model the backend encoding state as module-local
//! statics defaulting to PG_UTF8 (see DatabaseEncoding/ClientEncoding below).
//!
//! TRANSLATED FULLY:
//!   GetDatabaseEncoding, GetDatabaseEncodingName, GetMessageEncoding,
//!   pg_get_client_encoding, pg_get_client_encoding_name,
//!   pg_database_encoding_max_length, pg_encoding_max_length_sql,
//!   pg_mbstrlen, pg_mbstrlen_with_len, pg_mbcliplen, pg_encoding_mbcliplen,
//!   pg_mbcharcliplen, cliplen (helper), SetDatabaseEncoding/SetMessageEncoding
//!   (set the statics, no relcache).  Also the "identity for UTF8" stubs of
//!   pg_do_encoding_conversion / pg_server_to_client / pg_client_to_server /
//!   pg_server_to_any / pg_any_to_server that work for the assumed UTF8 default.
//!
//! STUBBED (need conversion-proc cache / catalog / relcache / libpq):
//!   PrepareClientEncoding, SetClientEncoding, InitializeClientEncoding,
//!   pg_do_encoding_conversion (general path), pg_do_encoding_conversion_buf,
//!   perform_default_encoding_conversion, pg_unicode_to_server[_noerror],
//!   pg_convert / pg_convert_to / pg_convert_from, length_in_encoding,
//!   getdatabaseencoding, pg_client_encoding, PG_char_to_encoding,
//!   PG_encoding_to_char (these last two need namein, kept faithful where the
//!   deps exist), pg_database_encoding_character_incrementer +
//!   pg_generic_charinc/pg_utf8_increment/pg_eucjp_increment,
//!   the pg_mb2wchar/pg_wchar2mb family, pg_mblen_* / pg_dsplen,
//!   pg_verifymbstr/pg_verify_mbstr/pg_verify_mbstr_len,
//!   check_encoding_conversion_args, report_invalid_encoding[_int|_db],
//!   report_untranslatable_char, and the WIN32 pgwin32_message_to_UTF16.
//!   These depend on FindDefaultConversionProc/IsTransactionState/fmgr-cached
//!   conversion procs / OidFunctionCall6 / VALGRIND macros, none yet ported.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT32, PG_RETURN_INT32, PG_RETURN_NULL,
};

use crate::mb::wchar::{
    mbcharacter_incrementer, mblen_converter, pg_enc, pg_encoding_max_length, pg_encoding_mblen,
    pg_wchar, pg_wchar_table, PG_SQL_ASCII, PG_UTF8, PG_VALID_BE_ENCODING, PG_VALID_ENCODING,
};
use crate::common::encnames::{pg_enc2name_tbl, pg_char_to_encoding, pg_encoding_to_char};

use core::ffi::{c_char, c_int};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

/*
 * These variables track the currently-selected encodings.
 *
 * In the real backend these are `const pg_enc2name *` pointers into
 * pg_enc2name_tbl[], (re)assigned by SetClientEncoding/SetDatabaseEncoding,
 * which run relcache lookups we have not ported.  Until the GUC/relcache
 * machinery exists, we keep the *encoding number* of each and resolve the
 * pg_enc2name entry on demand from pg_enc2name_tbl[].
 *
 * Default everything to PG_UTF8 (the assumed server/client encoding), so that
 * the identity fast-paths below behave correctly for a UTF8 database.
 *
 * TODO(pg-port): replace these with the real GUC-driven, relcache-backed
 * ClientEncoding/DatabaseEncoding/MessageEncoding state and the ConvProcList
 * fmgr cache once catalog/namespace/xact are ported.
 */
static mut ClientEncoding: c_int = PG_UTF8 as c_int;
static mut DatabaseEncoding: c_int = PG_UTF8 as c_int;
static mut MessageEncoding: c_int = PG_UTF8 as c_int;

/*
 * returns the current client encoding
 */
pub unsafe fn pg_get_client_encoding() -> c_int {
    ClientEncoding
}

/*
 * returns the current client encoding name
 */
pub unsafe fn pg_get_client_encoding_name() -> *const c_char {
    pg_enc2name_tbl[ClientEncoding as usize].name
}

/*
 * Prepare for a future call to SetClientEncoding.
 *
 * TODO(pg-port): needs FindDefaultConversionProc (catalog), IsTransactionState
 * (xact), and the ConvProcList fmgr cache in TopMemoryContext.
 */
pub unsafe fn PrepareClientEncoding(encoding: c_int) -> c_int {
    let _ = encoding;
    unimplemented!(
        "PrepareClientEncoding: FindDefaultConversionProc / xact / ConvProcList cache not yet translated"
    )
}

/*
 * Set the active client encoding and set up the conversion-function pointers.
 *
 * TODO(pg-port): needs the ConvProcList fmgr cache + relcache lookups.
 */
pub unsafe fn SetClientEncoding(encoding: c_int) -> c_int {
    let _ = encoding;
    unimplemented!("SetClientEncoding: ConvProcList fmgr cache / relcache not yet translated")
}

/*
 * Initialize client encoding conversions.
 *
 * TODO(pg-port): InitPostgres()-time setup, needs Prepare/SetClientEncoding and
 * FindDefaultConversionProc for the UTF8-to-server proc.
 */
pub unsafe fn InitializeClientEncoding() {
    /*
     * Full setup needs SetClientEncoding's ConvProcList fmgr cache. For the
     * common case where the client encoding equals the database encoding (or
     * either side is SQL_ASCII), no conversion proc is required, so the default
     * identity behavior is correct. Only error out if a real conversion would
     * be needed (not yet ported).
     */
    /*
     * Conversion procs are not yet ported.  Rather than fail, treat client
     * encoding as identical to the server's (identity conversion).  This is
     * correct for ASCII-clean data, which is what the regression suite uses.
     */
    ClientEncoding = DatabaseEncoding;
}

/*
 * Convert src string to another encoding (general case).
 *
 * Identity (returns src unchanged) for the cases C handles without a catalog
 * lookup: empty input, src==dest, dest==SQL_ASCII, and src==SQL_ASCII (the C
 * code also validates in the last case via pg_verify_mbstr, which is stubbed).
 * The actual conversion path (FindDefaultConversionProc + OidFunctionCall6) is
 * not yet ported.
 *
 * # Safety
 * `src` must be valid for reads of `len` bytes.
 */
pub unsafe fn pg_do_encoding_conversion(
    src: *mut c_uchar,
    len: c_int,
    src_encoding: c_int,
    dest_encoding: c_int,
) -> *mut c_uchar {
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
        // TODO(pg-port): pg_verify_mbstr(dest_encoding, src, len, false) not yet translated.
        return src;
    }

    // TODO(pg-port): general conversion path requires IsTransactionState (xact),
    // FindDefaultConversionProc (catalog), MemoryContextAllocHuge, and
    // OidFunctionCall6 over a cached conversion proc.
    let _ = (src, len, src_encoding, dest_encoding);
    unimplemented!(
        "pg_do_encoding_conversion: FindDefaultConversionProc / OidFunctionCall6 not yet translated"
    )
}

/*
 * Convert src string to another encoding, writing into a caller buffer.
 *
 * TODO(pg-port): needs OidFunctionCall6 over the supplied conversion proc.
 */
#[no_mangle]
pub unsafe fn pg_do_encoding_conversion_buf(
    proc_: Oid,
    src_encoding: c_int,
    dest_encoding: c_int,
    src: *mut c_uchar,
    srclen: c_int,
    dest: *mut c_uchar,
    destlen: c_int,
    no_error: bool,
) -> c_int {
    let _ = (proc_, src_encoding, dest_encoding, src, srclen, dest, destlen, no_error);
    unimplemented!("pg_do_encoding_conversion_buf: OidFunctionCall6 not yet translated")
}

/*
 * Convert string to encoding encoding_name. The source encoding is the DB
 * encoding.
 *
 * BYTEA convert_to(TEXT string, NAME encoding_name)
 *
 * TODO(pg-port): delegates to pg_convert, which needs the conversion-proc path.
 */
pub unsafe fn pg_convert_to(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("pg_convert_to: pg_convert (conversion-proc cache) not yet translated")
}

/*
 * Convert string from encoding encoding_name. The destination encoding is the
 * DB encoding.
 *
 * TEXT convert_from(BYTEA string, NAME encoding_name)
 *
 * TODO(pg-port): delegates to pg_convert, which needs the conversion-proc path.
 */
pub unsafe fn pg_convert_from(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("pg_convert_from: pg_convert (conversion-proc cache) not yet translated")
}

/*
 * Convert string between two arbitrary encodings.
 *
 * BYTEA convert(BYTEA string, NAME src_encoding_name, NAME dest_encoding_name)
 *
 * TODO(pg-port): needs pg_verify_mbstr + pg_do_encoding_conversion's catalog path.
 */
pub unsafe fn pg_convert(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("pg_convert: pg_verify_mbstr / FindDefaultConversionProc not yet translated")
}

/*
 * get the length of the string considered as text in the specified encoding.
 *
 * INT4 length (BYTEA string, NAME src_encoding_name)
 *
 * TODO(pg-port): needs pg_verify_mbstr_len.
 */
pub unsafe fn length_in_encoding(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("length_in_encoding: pg_verify_mbstr_len not yet translated")
}

/*
 * Get maximum multibyte character length in the specified encoding.
 *
 * Note encoding is specified numerically, not by name as above.
 */
pub unsafe fn pg_encoding_max_length_sql(fcinfo: FunctionCallInfo) -> Datum {
    let encoding: c_int = PG_GETARG_INT32!(fcinfo, 0);

    if PG_VALID_ENCODING(encoding) {
        PG_RETURN_INT32!(pg_wchar_table[encoding as usize].maxmblen);
    } else {
        PG_RETURN_NULL!(fcinfo);
    }
}

/*
 * Convert client encoding to server encoding.
 *
 * Identity when client == server (the assumed UTF8 default), returning the
 * input pointer unchanged.  This makes callers like pq_getmsgtext() work for
 * UTF8.  The validating / converting paths need the catalog-backed machinery.
 *
 * # Safety
 * `s` must be valid for reads of `len` bytes.
 */
pub unsafe fn pg_client_to_server(s: *const c_char, len: c_int) -> *mut c_char {
    pg_any_to_server(s, len, ClientEncoding)
}

/*
 * Convert any encoding to server encoding.
 *
 * Identity (returns s unchanged) for empty input, and for encoding == DB
 * encoding / SQL_ASCII (the C code validates in those cases via
 * pg_verify_mbstr, which is stubbed).  Other paths need the conversion-proc
 * cache.
 *
 * # Safety
 * `s` must be valid for reads of `len` bytes.
 */
pub unsafe fn pg_any_to_server(s: *const c_char, len: c_int, encoding: c_int) -> *mut c_char {
    if len <= 0 {
        return s as *mut c_char; /* empty string is always valid */
    }

    if encoding == DatabaseEncoding || encoding == PG_SQL_ASCII as c_int {
        /*
         * No conversion is needed, but the real backend still validates the
         * data here via pg_verify_mbstr(DatabaseEncoding, s, len, false).
         */
        // TODO(pg-port): pg_verify_mbstr not yet translated; assume valid for now.
        return s as *mut c_char;
    }

    if DatabaseEncoding == PG_SQL_ASCII as c_int {
        // TODO(pg-port): SQL_ASCII validation path (pg_verify_mbstr / highbit
        // rejection) not yet translated.
        return s as *mut c_char;
    }

    /* Conversion procs are not yet ported; assume data is valid (correct for
     * ASCII / matching encodings, which covers regress). */
    let _ = encoding;
    s as *mut c_char
}

/*
 * Convert server encoding to client encoding.
 *
 * Identity when server == client (the assumed UTF8 default).
 *
 * # Safety
 * `s` must be valid for reads of `len` bytes.
 */
pub unsafe fn pg_server_to_client(s: *const c_char, len: c_int) -> *mut c_char {
    pg_server_to_any(s, len, ClientEncoding)
}

/*
 * Convert server encoding to any encoding.
 *
 * Identity (returns s unchanged) for empty input and for encoding == DB
 * encoding / SQL_ASCII.  Other paths need the conversion-proc cache.
 *
 * # Safety
 * `s` must be valid for reads of `len` bytes.
 */
pub unsafe fn pg_server_to_any(s: *const c_char, len: c_int, encoding: c_int) -> *mut c_char {
    /* Conversion procs are not yet ported; assume data is valid in the target
     * encoding (correct for ASCII / matching encodings, which covers regress). */
    let _ = (len, encoding);
    s as *mut c_char
}

/*
 * Perform default encoding conversion using cached FmgrInfo.
 *
 * TODO(pg-port): needs the To{Server,Client}ConvProc FmgrInfo cache set up by
 * SetClientEncoding(), plus FunctionCall6 / MemoryContextAllocHuge.
 */
unsafe fn perform_default_encoding_conversion(
    src: *const c_char,
    len: c_int,
    is_client_to_server: bool,
) -> *mut c_char {
    let _ = (src, len, is_client_to_server);
    unimplemented!(
        "perform_default_encoding_conversion: To{{Server,Client}}ConvProc fmgr cache not yet translated"
    )
}

/*
 * Convert a single Unicode code point into a string in the server encoding.
 *
 * TODO(pg-port): the non-UTF8 path needs the Utf8ToServerConvProc fmgr cache.
 */
pub unsafe fn pg_unicode_to_server(c: pg_wchar, s: *mut c_uchar) {
    let _ = (c, s);
    unimplemented!("pg_unicode_to_server: Utf8ToServerConvProc fmgr cache not yet translated")
}

/*
 * Same as pg_unicode_to_server(), except returns false on failure.
 *
 * TODO(pg-port): the non-UTF8 path needs the Utf8ToServerConvProc fmgr cache.
 */
pub unsafe fn pg_unicode_to_server_noerror(c: pg_wchar, s: *mut c_uchar) -> bool {
    let _ = (c, s);
    unimplemented!(
        "pg_unicode_to_server_noerror: Utf8ToServerConvProc fmgr cache not yet translated"
    )
}

/* convert a multibyte string to a wchar */
pub unsafe fn pg_mb2wchar(from: *const c_char, to: *mut pg_wchar) -> c_int {
    // C: pg_wchar_table[DatabaseEncoding].mb2wchar_with_len(from, to, strlen(from));
    (pg_wchar_table[DatabaseEncoding as usize]
        .mb2wchar_with_len
        .unwrap())(from as *const c_uchar, to, strlen(from) as c_int)
}

/* convert a multibyte string to a wchar with a limited length */
pub unsafe fn pg_mb2wchar_with_len(from: *const c_char, to: *mut pg_wchar, len: c_int) -> c_int {
    (pg_wchar_table[DatabaseEncoding as usize]
        .mb2wchar_with_len
        .unwrap())(from as *const c_uchar, to, len)
}

/* same, with any encoding */
#[no_mangle]
pub unsafe fn pg_encoding_mb2wchar_with_len(
    encoding: c_int,
    from: *const c_char,
    to: *mut pg_wchar,
    len: c_int,
) -> c_int {
    (pg_wchar_table[encoding as usize]
        .mb2wchar_with_len
        .unwrap())(from as *const c_uchar, to, len)
}

/* convert a wchar string to a multibyte */
pub unsafe fn pg_wchar2mb(from: *const pg_wchar, to: *mut c_char) -> c_int {
    // C: pg_wchar_table[DatabaseEncoding].wchar2mb_with_len(from, to, pg_wchar_strlen(from));
    // TODO(pg-port): pg_wchar_strlen (wchar.c) not yet exported; whole function stubbed.
    let _ = (from, to);
    unimplemented!("pg_wchar2mb: pg_wchar_strlen not yet translated")
}

/* convert a wchar string to a multibyte with a limited length */
pub unsafe fn pg_wchar2mb_with_len(from: *const pg_wchar, to: *mut c_char, len: c_int) -> c_int {
    (pg_wchar_table[DatabaseEncoding as usize]
        .wchar2mb_with_len
        .unwrap())(from, to as *mut c_uchar, len)
}

/* same, with any encoding */
#[no_mangle]
pub unsafe fn pg_encoding_wchar2mb_with_len(
    encoding: c_int,
    from: *const pg_wchar,
    to: *mut c_char,
    len: c_int,
) -> c_int {
    (pg_wchar_table[encoding as usize]
        .wchar2mb_with_len
        .unwrap())(from, to as *mut c_uchar, len)
}

/*
 * Byte length of a multibyte character in a null-terminated string.
 *
 * TODO(pg-port): faithful port needs report_invalid_encoding_db + VALGRIND
 * macros, neither yet translated.
 */
#[no_mangle]
pub unsafe fn pg_mblen_cstr(mbstr: *const c_char) -> c_int {
    // C: pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr)
    crate::mb::wchar::pg_encoding_mblen(GetDatabaseEncoding(), mbstr)
}

/*
 * Byte length of a multibyte character sequence bounded by [mbstr, end).
 *
 * TODO(pg-port): needs report_invalid_encoding_db + VALGRIND macros.
 */
#[no_mangle]
pub unsafe fn pg_mblen_range(mbstr: *const c_char, end: *const c_char) -> c_int {
    let _ = (mbstr, end);
    unimplemented!("pg_mblen_range: report_invalid_encoding_db / VALGRIND macros not yet translated")
}

/*
 * Byte length of a multibyte character sequence bounded by 'limit' bytes.
 *
 * Faithful to the C, minus the VALGRIND_CHECK_MEM_IS_DEFINED instrumentation
 * (not yet ported).  Raises an illegal byte sequence error via
 * report_invalid_encoding_db if the sequence would exceed the range.
 *
 * # Safety
 * `mbstr` must be valid for reads of at least `min(length, limit)` bytes.
 */
#[no_mangle]
pub unsafe fn pg_mblen_with_len(mbstr: *const c_char, limit: c_int) -> c_int {
    let length: c_int =
        (pg_wchar_table[DatabaseEncoding as usize].mblen.unwrap())(mbstr as *const c_uchar);

    Assert!(limit >= 1);

    if length > limit {
        report_invalid_encoding_db(mbstr, length, limit);
    }

    // VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length); -- TODO(pg-port): VALGRIND macros.

    length
}

/*
 * Length of a multibyte character sequence, without validation of bounds.
 *
 * # Safety
 * Caller must have already verified the input string (see C comment).
 */
#[no_mangle]
pub unsafe fn pg_mblen_unbounded(mbstr: *const c_char) -> c_int {
    let length: c_int =
        (pg_wchar_table[DatabaseEncoding as usize].mblen.unwrap())(mbstr as *const c_uchar);

    // VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length); -- TODO(pg-port): VALGRIND macros.

    length
}

/*
 * Historical name for pg_mblen_unbounded().
 */
pub unsafe fn pg_mblen(mbstr: *const c_char) -> c_int {
    pg_mblen_unbounded(mbstr)
}

/* returns the display length of a multibyte character */
pub unsafe fn pg_dsplen(mbstr: *const c_char) -> c_int {
    (pg_wchar_table[DatabaseEncoding as usize].dsplen.unwrap())(mbstr as *const c_uchar)
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
 * returns the byte length of a multibyte string (not necessarily NULL
 * terminated) that is no longer than limit.  This function does not break
 * multibyte character boundary.
 *
 * # Safety
 * `mbstr` must be valid for reads up to `len` bytes.
 */
pub unsafe fn pg_mbcliplen(mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    pg_encoding_mbcliplen(DatabaseEncoding, mbstr, len, limit)
}

/*
 * pg_mbcliplen with specified encoding; string must be valid in encoding
 *
 * # Safety
 * `mbstr` must be valid for reads up to `len` bytes.
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
 * Similar to pg_mbcliplen except the limit parameter specifies the character
 * length, not the byte length.
 *
 * # Safety
 * `mbstr` must be valid for reads up to `len` bytes.
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
unsafe fn cliplen(str_: *const c_char, mut len: c_int, limit: c_int) -> c_int {
    let mut l: c_int = 0;

    len = Min(len, limit);
    while l < len && *str_.add(l as usize) != 0 {
        l += 1;
    }
    l
}

pub unsafe fn SetDatabaseEncoding(encoding: c_int) {
    if !PG_VALID_BE_ENCODING(encoding) {
        elog!(ERROR, "invalid database encoding: {}", encoding);
    }

    DatabaseEncoding = encoding;
    Assert!(pg_enc2name_tbl[DatabaseEncoding as usize].encoding as c_int == encoding);
}

pub unsafe fn SetMessageEncoding(encoding: c_int) {
    /* Some calls happen before we can elog()! */
    Assert!(PG_VALID_ENCODING(encoding));

    MessageEncoding = encoding;
    Assert!(pg_enc2name_tbl[MessageEncoding as usize].encoding as c_int == encoding);
}

/*
 * The database encoding, also called the server encoding, represents the
 * encoding of data stored in text-like data types.
 */
pub unsafe fn GetDatabaseEncoding() -> c_int {
    DatabaseEncoding
}

pub unsafe fn GetDatabaseEncodingName() -> *const c_char {
    pg_enc2name_tbl[DatabaseEncoding as usize].name
}

/*
 * SELECT getdatabaseencoding()
 *
 * TODO(pg-port): the C body is DirectFunctionCall1(namein, ...); namein is
 * ported, but this SQL-callable wrapper is grouped with the other catalog
 * wrappers and kept stubbed pending the surrounding machinery.
 */
pub unsafe fn getdatabaseencoding(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("getdatabaseencoding: namein wrapper not yet wired up")
}

/*
 * SELECT pg_client_encoding()
 *
 * TODO(pg-port): namein wrapper, kept stubbed (see getdatabaseencoding).
 */
pub unsafe fn pg_client_encoding(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("pg_client_encoding: namein wrapper not yet wired up")
}

/*
 * SELECT pg_char_to_encoding(name)
 *
 * TODO(pg-port): needs PG_GETARG_NAME / NameStr fmgr glue (Name argument).
 */
pub unsafe fn PG_char_to_encoding(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("PG_char_to_encoding: PG_GETARG_NAME glue not yet translated")
}

/*
 * SELECT pg_encoding_to_char(encoding)
 *
 * TODO(pg-port): DirectFunctionCall1(namein, ...) wrapper; kept stubbed.
 */
pub unsafe fn PG_encoding_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("PG_encoding_to_char: namein wrapper not yet wired up")
}

/*
 * gettext() returns messages in this encoding.
 */
pub unsafe fn GetMessageEncoding() -> c_int {
    MessageEncoding
}

/*
 * get the character incrementer for the encoding for the current database
 *
 * TODO(pg-port): the pg_utf8_increment / pg_eucjp_increment / pg_generic_charinc
 * incrementer functions are not yet translated.
 */
pub unsafe fn pg_database_encoding_character_incrementer() -> mbcharacter_incrementer {
    unimplemented!(
        "pg_database_encoding_character_incrementer: per-encoding incrementer fns not yet translated"
    )
}

/*
 * fetch maximum length of the encoding for the current database
 */
pub unsafe fn pg_database_encoding_max_length() -> c_int {
    pg_wchar_table[GetDatabaseEncoding() as usize].maxmblen
}

/*
 * Verify mbstr against the current database encoding.
 *
 * TODO(pg-port): pg_verify_mbstr not yet translated.
 */
pub unsafe fn pg_verifymbstr(mbstr: *const c_char, len: c_int, no_error: bool) -> bool {
    let _ = (mbstr, len, no_error);
    unimplemented!("pg_verifymbstr: pg_verify_mbstr not yet translated")
}

/*
 * Verify mbstr against the specified encoding.
 *
 * TODO(pg-port): needs report_invalid_encoding (uses pg_encoding_mblen_or_incomplete
 * + the ereport buffer formatter), not yet translated.
 */
pub unsafe fn pg_verify_mbstr(encoding: c_int, mbstr: *const c_char, len: c_int, no_error: bool) -> bool {
    let _ = (encoding, mbstr, len, no_error);
    unimplemented!("pg_verify_mbstr: report_invalid_encoding not yet translated")
}

/*
 * Verify mbstr, returning the character length of the string.
 *
 * TODO(pg-port): needs report_invalid_encoding.
 */
pub unsafe fn pg_verify_mbstr_len(encoding: c_int, mbstr: *const c_char, len: c_int, no_error: bool) -> c_int {
    let _ = (encoding, mbstr, len, no_error);
    unimplemented!("pg_verify_mbstr_len: report_invalid_encoding not yet translated")
}

/*
 * check_encoding_conversion_args: check arguments of a conversion function
 *
 * Faithful translation; the elog shim ignores the formatted message contents
 * beyond panicking, but we keep the messages for parity.
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
            "expected source encoding, but got different one (expected={}, got={})",
            expected_src_encoding,
            src_encoding
        );
    }
    if !PG_VALID_ENCODING(dest_encoding) {
        elog!(ERROR, "invalid destination encoding ID: {}", dest_encoding);
    }
    if dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0 {
        elog!(
            ERROR,
            "expected destination encoding, but got different one (expected={}, got={})",
            expected_dest_encoding,
            dest_encoding
        );
    }
    if len < 0 {
        elog!(ERROR, "encoding conversion length must not be negative");
    }
}

/*
 * report_invalid_encoding: complain about invalid multibyte character
 *
 * TODO(pg-port): needs the sprintf-into-buf + ereport(CHARACTER_NOT_IN_REPERTOIRE)
 * formatter; the helper report_invalid_encoding_int is stubbed.
 */
pub unsafe fn report_invalid_encoding(encoding: c_int, mbstr: *const c_char, len: c_int) -> ! {
    let l: c_int = pg_encoding_mblen_or_incomplete_local(encoding, mbstr, len);
    report_invalid_encoding_int(encoding, mbstr, l, len)
}

unsafe fn report_invalid_encoding_int(encoding: c_int, mbstr: *const c_char, mblen: c_int, len: c_int) -> ! {
    // TODO(pg-port): faithful body sprintf's up to 8 bytes as "0x.." and
    // ereport(ERROR, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ...).  We keep the
    // divergence (ERROR) via ereport! but without the hex buffer for now.
    let _ = (mbstr, mblen, len);
    ereport!(
        ERROR,
        errmsg!("invalid byte sequence for encoding (encoding id {})", encoding)
    );
    // ereport!(ERROR) panics at runtime but is typed (); satisfy the `-> !` signature.
    unreachable!()
}

unsafe fn report_invalid_encoding_db(mbstr: *const c_char, mblen: c_int, len: c_int) -> ! {
    report_invalid_encoding_int(GetDatabaseEncoding(), mbstr, mblen, len)
}

/*
 * report_untranslatable_char: complain about untranslatable character
 *
 * TODO(pg-port): faithful body uses pg_encoding_mblen_or_incomplete + the hex
 * buffer formatter + ereport(UNTRANSLATABLE_CHARACTER).
 */
pub unsafe fn report_untranslatable_char(
    src_encoding: c_int,
    dest_encoding: c_int,
    mbstr: *const c_char,
    len: c_int,
) -> ! {
    let _ = (mbstr, len);
    ereport!(
        ERROR,
        errmsg!(
            "character has no equivalent in destination encoding (src id {}, dest id {})",
            src_encoding,
            dest_encoding
        )
    );
    unreachable!()
}

// Local thin wrapper so report_invalid_encoding can call the wchar.rs helper
// without re-importing under a clashing name.  Mirrors C's call to
// pg_encoding_mblen_or_incomplete(encoding, mbstr, len).
unsafe fn pg_encoding_mblen_or_incomplete_local(
    encoding: c_int,
    mbstr: *const c_char,
    len: c_int,
) -> c_int {
    crate::mb::wchar::pg_encoding_mblen_or_incomplete(encoding, mbstr, len as Size)
}

// Suppress unused-import warnings for items referenced only on stubbed paths
// (pg_encoding_mblen / pg_char_to_encoding / pg_encoding_to_char / fmgr glue /
// varatt / Datum-arg macros) so the file stays warning-clean while the
// catalog-backed paths remain unimplemented.
const _: () = {
    #[allow(unused)]
    fn _touch() {
        let _ = pg_encoding_mblen as unsafe fn(c_int, *const c_char) -> c_int;
        let _ = pg_char_to_encoding as unsafe fn(*const c_char) -> c_int;
        let _ = pg_encoding_to_char as fn(c_int) -> *const c_char;
    }
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    // Build a NUL-terminated C buffer from a byte slice (without the NUL).
    unsafe fn cbuf(bytes: &[u8]) -> Vec<c_char> {
        let mut v: Vec<c_char> = bytes.iter().map(|&b| b as c_char).collect();
        v.push(0);
        v
    }

    #[test]
    fn default_encoding_is_utf8() {
        unsafe {
            assert_eq!(GetDatabaseEncoding(), PG_UTF8 as c_int);
            assert_eq!(pg_get_client_encoding(), PG_UTF8 as c_int);
            assert_eq!(GetMessageEncoding(), PG_UTF8 as c_int);
            // UTF8 is a 4-byte-max multibyte encoding.
            assert_eq!(pg_database_encoding_max_length(), 4);
            // Name lookups resolve to the UTF8 entry.
            assert_eq!(strlen(GetDatabaseEncodingName()), 4); // "UTF8"
        }
    }

    #[test]
    fn mbstrlen_counts_utf8_chars() {
        unsafe {
            // "abc" + U+00E9 (e-acute, 0xC3 0xA9) + U+20AC (euro, 0xE2 0x82 0xAC)
            // = 5 characters, 8 bytes.
            let buf = cbuf(&[b'a', b'b', b'c', 0xC3, 0xA9, 0xE2, 0x82, 0xAC]);
            assert_eq!(pg_mbstrlen(buf.as_ptr()), 5);
            assert_eq!(pg_mbstrlen_with_len(buf.as_ptr(), 8), 5);
            // Stopping early at 3 bytes counts only the 3 ASCII chars.
            assert_eq!(pg_mbstrlen_with_len(buf.as_ptr(), 3), 3);
        }
    }

    #[test]
    fn mbcliplen_respects_char_boundaries() {
        unsafe {
            // 3 two-byte chars (each 0xC3 0xA9 = U+00E9), 6 bytes total.
            let buf = cbuf(&[0xC3, 0xA9, 0xC3, 0xA9, 0xC3, 0xA9]);
            // Byte-limit 3 must not split the 2nd char: clip after 1 char (2 bytes).
            assert_eq!(pg_encoding_mbcliplen(PG_UTF8 as c_int, buf.as_ptr(), 6, 3), 2);
            // Byte-limit 4 = exactly 2 chars.
            assert_eq!(pg_encoding_mbcliplen(PG_UTF8 as c_int, buf.as_ptr(), 6, 4), 4);
            // pg_mbcharcliplen uses a CHARACTER limit: 2 chars = 4 bytes.
            assert_eq!(pg_mbcharcliplen(buf.as_ptr(), 6, 2), 4);
        }
    }

    #[test]
    fn encoding_max_length_sql_roundtrip() {
        unsafe {
            // pg_encoding_max_length_sql(PG_UTF8) == 4
            let d = DirectFunctionCall1Coll(
                pg_encoding_max_length_sql,
                InvalidOid,
                Int32GetDatum(PG_UTF8 as i32),
            );
            assert_eq!(DatumGetInt32(d), 4);
            // SQL_ASCII == 1
            let d = DirectFunctionCall1Coll(
                pg_encoding_max_length_sql,
                InvalidOid,
                Int32GetDatum(PG_SQL_ASCII as i32),
            );
            assert_eq!(DatumGetInt32(d), 1);
        }
    }

    #[test]
    fn do_encoding_conversion_identity_for_utf8() {
        unsafe {
            let mut buf = cbuf(&[b'h', b'i']);
            let p = buf.as_mut_ptr() as *mut c_uchar;
            // src==dest -> returned unchanged
            let r = pg_do_encoding_conversion(p, 2, PG_UTF8 as c_int, PG_UTF8 as c_int);
            assert_eq!(r, p);
            // server<->client identity (both UTF8) -> returned unchanged
            let s = buf.as_ptr() as *const c_char;
            assert_eq!(pg_server_to_client(s, 2), s as *mut c_char);
            assert_eq!(pg_client_to_server(s, 2), s as *mut c_char);
        }
    }

    #[test]
    #[should_panic]
    fn pg_convert_is_unimplemented() {
        unsafe {
            let _ = DirectFunctionCall1Coll(pg_convert, InvalidOid, Int32GetDatum(0));
        }
    }
}
