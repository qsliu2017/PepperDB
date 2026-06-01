//! rowtypes.rs
//!   I/O and comparison functions for generic composite types.
//! Translated 1:1 from postgres/src/backend/utils/adt/rowtypes.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   postgres.h                 -> crate::prelude::*
//!   <ctype.h>                  -> extern "C" isspace
//!   access/detoast.h           -> PG_DETOAST_DATUM_PACKED (crate::PG_DETOAST_DATUM_PACKED)
//!   access/htup_details.h      -> HeapTupleData / HeapTupleHeader / heap_form_tuple /
//!                                 heap_deform_tuple / heap_freetuple /
//!                                 HeapTupleHeaderGet{TypeId,TypMod,DatumLength}
//!   catalog/pg_type.h          -> RECORDOID (crate::catalog::pg_type_d)
//!   funcapi.h                  -> fmgr machinery (crate::utils::fmgr)
//!   libpq/pqformat.h           -> pq_* (crate::libpq::pqformat)
//!   miscadmin.h                -> check_stack_depth (crate::miscadmin)
//!   utils/builtins.h           -> format_type_be / format_type_extended /
//!                                 FORMAT_TYPE_ALLOW_INVALID
//!   utils/datum.h              -> datum_image_eq (crate::utils::adt::datum)
//!   utils/lsyscache.h          -> getType*Info (STUB - lsyscache.c not yet ported)
//!   utils/typcache.h           -> TypeCacheEntry / lookup_type_cache /
//!                                 lookup_rowtype_tupdesc (STUB - typcache.c not yet
//!                                 ported)

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;

use std::ffi::CStr;

use crate::{
    PG_FREE_IF_COPY, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_OID,
    PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_DATUM,
    PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_UINT32, PG_RETURN_UINT64,
};
use crate::{InitFunctionCallInfoData, FunctionCallInvoke, LOCAL_FCINFO};
use crate::Assert;
use crate::PG_DETOAST_DATUM_PACKED;

use crate::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple, heap_freetuple};
use crate::access::common::tupdesc::{ReleaseTupleDesc, TupleDescAttr};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::{
    HeapTupleData, HeapTupleHeader, HeapTupleHeaderGetDatumLength, HeapTupleHeaderGetTypMod,
    HeapTupleHeaderGetTypeId,
};
use crate::catalog::pg_type_d::RECORDOID;
use crate::lib::stringinfo::{
    appendStringInfoChar, initReadOnlyStringInfo, initStringInfo, resetStringInfo, StringInfo,
    StringInfoData,
};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_sendbytes, pq_sendint32,
};
use crate::miscadmin::check_stack_depth;
use crate::nodes::nodes::Node;
use crate::storage::itemptr::ItemPointerSetInvalid;
use crate::utils::adt::datum::datum_image_eq;
use crate::utils::builtins::{format_type_be, format_type_extended, FORMAT_TYPE_ALLOW_INVALID};
use crate::utils::fmgr::{
    fmgr_info_cxt, FmgrInfo, FunctionCallInfo, InputFunctionCallSafe, OutputFunctionCall,
    ReceiveFunctionCall, SendFunctionCall,
};
use crate::access::common::detoast::toast_raw_datum_size;
use crate::varatt::{VARDATA, VARDATA_ANY, VARSIZE};
use crate::appendStringInfoCharMacro;

extern "C" {
    fn isspace(ch: c_int) -> c_int;
}

extern "C" {
    #[link_name = "memcmp"]
    fn libc_memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
}

/*
 * ereturn(escontext, dummy, (...)) used by record_in for the anonymous-composite
 * case.  Mirrors crate::utils::adt::xid8funcs's local ereturn pattern: the elog
 * shim emits at ERROR level (errcode/errdetail dropped per porting convention).
 *
 * Defined here (textually before first use) since macro_rules! is not hoisted.
 */
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{
        let _ = &$escontext;
        crate::utils::elog::emit_log(ERROR, &$($arg)*, file!(), line!());
        return $dummy;
    }};
}

/* utils/errcodes.h: error classification codes.  The elog shim ignores these. */
// TODO(pg-port): ERRCODE_* from utils/errcodes.h.
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;
const ERRCODE_INVALID_BINARY_REPRESENTATION: c_int = 0;
const ERRCODE_UNDEFINED_FUNCTION: c_int = 0;

/*
 * catalog/catalog.h: first OID assigned by genbki.pl (built-in objects have
 * OIDs strictly below this).
 */
// TODO(pg-port): real FirstGenbkiObjectId lives in catalog/catalog.rs
const FirstGenbkiObjectId: Oid = 10000;

/* ---------------------------------------------------------------------------
 * STUBBED typcache.h / lsyscache.h surface
 *
 * The real typcache (utils/cache/typcache.c) and lsyscache (utils/cache/
 * lsyscache.c) are not yet ported.  We model the exact pieces rowtypes.c
 * touches so the surrounding logic translates 1:1.
 * ---------------------------------------------------------------------------
 */

/* utils/typcache.h: bits requested from lookup_type_cache */
const TYPECACHE_CMP_PROC_FINFO: c_int = 0x0080;
const TYPECACHE_EQ_OPR_FINFO: c_int = 0x0040;
const TYPECACHE_HASH_PROC_FINFO: c_int = 0x0200;
const TYPECACHE_HASH_EXTENDED_PROC_FINFO: c_int = 0x4000;

/*
 * utils/typcache.h: TypeCacheEntry.  rowtypes.c only reads type_id and the
 * lazily-filled FmgrInfo fields below.
 */
#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub cmp_proc_finfo: FmgrInfo,
    pub eq_opr_finfo: FmgrInfo,
    pub hash_proc_finfo: FmgrInfo,
    pub hash_extended_proc_finfo: FmgrInfo,
}

// TODO(pg-port): real lookup_type_cache lives in utils/cache/typcache.rs
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("lookup_type_cache: utils/cache/typcache.c not yet translated")
}

// TODO(pg-port): real lookup_rowtype_tupdesc lives in utils/cache/typcache.rs
unsafe fn lookup_rowtype_tupdesc(_type_id: Oid, _typmod: int32) -> TupleDesc {
    unimplemented!("lookup_rowtype_tupdesc: utils/cache/typcache.c not yet translated")
}

// TODO(pg-port): real getTypeInputInfo lives in utils/cache/lsyscache.rs
unsafe fn getTypeInputInfo(_typ: Oid, _typinput: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!("getTypeInputInfo: utils/cache/lsyscache.c not yet translated")
}

// TODO(pg-port): real getTypeOutputInfo lives in utils/cache/lsyscache.rs
unsafe fn getTypeOutputInfo(_typ: Oid, _typoutput: *mut Oid, _typisvarlena: *mut bool) {
    unimplemented!("getTypeOutputInfo: utils/cache/lsyscache.c not yet translated")
}

// TODO(pg-port): real getTypeBinaryInputInfo lives in utils/cache/lsyscache.rs
unsafe fn getTypeBinaryInputInfo(_typ: Oid, _typreceive: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!("getTypeBinaryInputInfo: utils/cache/lsyscache.c not yet translated")
}

// TODO(pg-port): real getTypeBinaryOutputInfo lives in utils/cache/lsyscache.rs
unsafe fn getTypeBinaryOutputInfo(_typ: Oid, _typsend: *mut Oid, _typisvarlena: *mut bool) {
    unimplemented!("getTypeBinaryOutputInfo: utils/cache/lsyscache.c not yet translated")
}

/*
 * fmgr.h: DatumGetHeapTupleHeader / PG_GETARG_HEAPTUPLEHEADER /
 * PG_RETURN_HEAPTUPLEHEADER.  These are not yet in fmgr.rs, so we provide local
 * equivalents (they simply reinterpret the (de-toasted) Datum as a
 * HeapTupleHeader pointer; the no-detoast forms are used here because the inputs
 * are already in-memory composite Datums).
 */
// TODO(pg-port): real DatumGetHeapTupleHeader (with detoast) lives in fmgr.h
unsafe fn DatumGetHeapTupleHeader(d: Datum) -> HeapTupleHeader {
    PG_DETOAST_DATUM_PACKED!(d) as HeapTupleHeader
}

/*
 * structure to cache metadata needed for record I/O
 */
#[repr(C)]
struct ColumnIOData {
    column_type: Oid,
    typiofunc: Oid,
    typioparam: Oid,
    typisvarlena: bool,
    proc_: FmgrInfo,
}

#[repr(C)]
struct RecordIOData {
    record_type: Oid,
    record_typmod: int32,
    ncolumns: c_int,
    columns: [ColumnIOData; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * structure to cache metadata needed for record comparison
 */
#[repr(C)]
struct ColumnCompareData {
    typentry: *mut TypeCacheEntry, /* has everything we need, actually */
}

#[repr(C)]
struct RecordCompareData {
    ncolumns: c_int, /* allocated length of columns[] */
    record1_type: Oid,
    record1_typmod: int32,
    record2_type: Oid,
    record2_typmod: int32,
    columns: [ColumnCompareData; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * record_in		- input routine for any composite type.
 */
pub unsafe fn record_in(fcinfo: FunctionCallInfo) -> Datum {
    let string = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_char; /* PG_GETARG_CSTRING(0) */
    let tupType = PG_GETARG_OID!(fcinfo, 1);
    let tupTypmod = PG_GETARG_INT32!(fcinfo, 2);
    let escontext: *mut Node = (*fcinfo).context;
    let result: HeapTupleHeader;
    let tupdesc: TupleDesc;
    let tuple;
    let mut my_extra: *mut RecordIOData;
    let mut needComma = false;
    let ncolumns: c_int;
    let mut i: c_int;
    let mut ptr: *mut c_char;
    let values: *mut Datum;
    let nulls: *mut bool;
    let mut buf: StringInfoData = core::mem::zeroed();

    check_stack_depth(); /* recurses for record-type columns */

    /*
     * Give a friendly error message if we did not get enough info to identify
     * the target record type.  (lookup_rowtype_tupdesc would fail anyway, but
     * with a non-user-friendly message.)  In ordinary SQL usage, we'll get -1
     * for typmod, since composite types and RECORD have no type modifiers at
     * the SQL level, and thus must fail for RECORD.  However some callers can
     * supply a valid typmod, and then we can do something useful for RECORD.
     */
    if tupType == RECORDOID && tupTypmod < 0 {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!("input of anonymous composite types is not implemented")
        );
    }

    /*
     * This comes from the composite type's pg_type.oid and stores system oids
     * in user tables, specifically DatumTupleFields. This oid must be
     * preserved by binary upgrades.
     */
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = (*tupdesc).natts;

    /*
     * We arrange to look up the needed I/O info just once per series of calls,
     * assuming the record type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
    if my_extra.is_null() || (*my_extra).ncolumns != ncolumns {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
        (*my_extra).record_type = InvalidOid;
        (*my_extra).record_typmod = 0;
    }

    if (*my_extra).record_type != tupType || (*my_extra).record_typmod != tupTypmod {
        MemSet(
            my_extra as *mut c_void,
            0,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        (*my_extra).record_type = tupType;
        (*my_extra).record_typmod = tupTypmod;
        (*my_extra).ncolumns = ncolumns;
    }

    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;

    /*
     * Scan the string.  We use "buf" to accumulate the de-quoted data for each
     * column, which is then fed to the appropriate input converter.
     */
    ptr = string;
    /* Allow leading whitespace */
    while *ptr != 0 && isspace(*ptr as c_uchar as c_int) != 0 {
        ptr = ptr.add(1);
    }
    let lead = *ptr;
    ptr = ptr.add(1);
    if lead != b'(' as c_char {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        errsave_emit(
            escontext,
            &format!(
                "malformed record literal: \"{}\"",
                CStr::from_ptr(string).to_string_lossy()
            ),
        );
        return record_in_fail(fcinfo, tupdesc);
    }

    initStringInfo(&raw mut buf);

    i = 0;
    while i < ncolumns {
        let att = TupleDescAttr(tupdesc, i);
        let column_info = (*my_extra).columns.as_mut_ptr().add(i as usize);
        let column_type = (*att).atttypid;
        let column_data: *mut c_char;

        /* Ignore dropped columns in datatype, but fill with nulls */
        if (*att).attisdropped {
            *values.add(i as usize) = 0 as Datum;
            *nulls.add(i as usize) = true;
            i += 1;
            continue;
        }

        if needComma {
            /* Skip comma that separates prior field from this one */
            if *ptr == b',' as c_char {
                ptr = ptr.add(1);
            } else {
                /* *ptr must be ')' */
                let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                errsave_emit(
                    escontext,
                    &format!(
                        "malformed record literal: \"{}\"",
                        CStr::from_ptr(string).to_string_lossy()
                    ),
                );
                return record_in_fail(fcinfo, tupdesc);
            }
        }

        /* Check for null: completely empty input means null */
        if *ptr == b',' as c_char || *ptr == b')' as c_char {
            column_data = null_mut();
            *nulls.add(i as usize) = true;
        } else {
            /* Extract string for this column */
            let mut inquote = false;

            resetStringInfo(&raw mut buf);
            while inquote || !(*ptr == b',' as c_char || *ptr == b')' as c_char) {
                let ch = *ptr;
                ptr = ptr.add(1);

                if ch == b'\0' as c_char {
                    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                    errsave_emit(
                        escontext,
                        &format!(
                            "malformed record literal: \"{}\"",
                            CStr::from_ptr(string).to_string_lossy()
                        ),
                    );
                    return record_in_fail(fcinfo, tupdesc);
                }
                if ch == b'\\' as c_char {
                    if *ptr == b'\0' as c_char {
                        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                        errsave_emit(
                            escontext,
                            &format!(
                                "malformed record literal: \"{}\"",
                                CStr::from_ptr(string).to_string_lossy()
                            ),
                        );
                        return record_in_fail(fcinfo, tupdesc);
                    }
                    appendStringInfoChar(&raw mut buf, *ptr);
                    ptr = ptr.add(1);
                } else if ch == b'"' as c_char {
                    if !inquote {
                        inquote = true;
                    } else if *ptr == b'"' as c_char {
                        /* doubled quote within quote sequence */
                        appendStringInfoChar(&raw mut buf, *ptr);
                        ptr = ptr.add(1);
                    } else {
                        inquote = false;
                    }
                } else {
                    appendStringInfoChar(&raw mut buf, ch);
                }
            }

            column_data = buf.data;
            *nulls.add(i as usize) = false;
        }

        /*
         * Convert the column value
         */
        if (*column_info).column_type != column_type {
            getTypeInputInfo(
                column_type,
                &raw mut (*column_info).typiofunc,
                &raw mut (*column_info).typioparam,
            );
            fmgr_info_cxt(
                (*column_info).typiofunc,
                &raw mut (*column_info).proc_,
                (*(*fcinfo).flinfo).fn_mcxt,
            );
            (*column_info).column_type = column_type;
        }

        if !InputFunctionCallSafe(
            &raw mut (*column_info).proc_,
            column_data,
            (*column_info).typioparam,
            (*att).atttypmod,
            escontext,
            values.add(i as usize),
        ) {
            return record_in_fail(fcinfo, tupdesc);
        }

        /*
         * Prep for next column
         */
        needComma = true;

        i += 1;
    }

    let endp = *ptr;
    ptr = ptr.add(1);
    if endp != b')' as c_char {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        errsave_emit(
            escontext,
            &format!(
                "malformed record literal: \"{}\"",
                CStr::from_ptr(string).to_string_lossy()
            ),
        );
        return record_in_fail(fcinfo, tupdesc);
    }
    /* Allow trailing whitespace */
    while *ptr != 0 && isspace(*ptr as c_uchar as c_int) != 0 {
        ptr = ptr.add(1);
    }
    if *ptr != 0 {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        errsave_emit(
            escontext,
            &format!(
                "malformed record literal: \"{}\"",
                CStr::from_ptr(string).to_string_lossy()
            ),
        );
        return record_in_fail(fcinfo, tupdesc);
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);

    /*
     * We cannot return tuple->t_data because heap_form_tuple allocates it as
     * part of a larger chunk, and our caller may expect to be able to pfree our
     * result.  So must copy the info into a new palloc chunk.
     */
    result = palloc((*tuple).t_len as usize) as HeapTupleHeader;
    std::ptr::copy_nonoverlapping(
        (*tuple).t_data as *const u8,
        result as *mut u8,
        (*tuple).t_len as usize,
    );

    heap_freetuple(tuple);
    pfree(buf.data as *mut c_void);
    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_HEAPTUPLEHEADER(result)
}

/*
 * exit here once we've done lookup_rowtype_tupdesc (the C `fail:` label).
 */
unsafe fn record_in_fail(fcinfo: FunctionCallInfo, tupdesc: TupleDesc) -> Datum {
    ReleaseTupleDesc(tupdesc);
    return_null(fcinfo)
}

/*
 * record_out		- output routine for any composite type.
 */
pub unsafe fn record_out(fcinfo: FunctionCallInfo) -> Datum {
    let rec = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let tupType: Oid;
    let tupTypmod: int32;
    let tupdesc: TupleDesc;
    let mut tuple: HeapTupleData = core::mem::zeroed();
    let mut my_extra: *mut RecordIOData;
    let mut needComma = false;
    let ncolumns: c_int;
    let mut i: c_int;
    let values: *mut Datum;
    let nulls: *mut bool;
    let mut buf: StringInfoData = core::mem::zeroed();

    check_stack_depth(); /* recurses for record-type columns */

    /* Extract type info from the tuple itself */
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = (*tupdesc).natts;

    /* Build a temporary HeapTuple control structure */
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&raw mut tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    /*
     * We arrange to look up the needed I/O info just once per series of calls,
     * assuming the record type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
    if my_extra.is_null() || (*my_extra).ncolumns != ncolumns {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
        (*my_extra).record_type = InvalidOid;
        (*my_extra).record_typmod = 0;
    }

    if (*my_extra).record_type != tupType || (*my_extra).record_typmod != tupTypmod {
        MemSet(
            my_extra as *mut c_void,
            0,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        (*my_extra).record_type = tupType;
        (*my_extra).record_typmod = tupTypmod;
        (*my_extra).ncolumns = ncolumns;
    }

    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Break down the tuple into fields */
    heap_deform_tuple(&raw mut tuple, tupdesc, values, nulls);

    /* And build the result string */
    initStringInfo(&raw mut buf);

    appendStringInfoChar(&raw mut buf, b'(' as c_char);

    i = 0;
    while i < ncolumns {
        let att = TupleDescAttr(tupdesc, i);
        let column_info = (*my_extra).columns.as_mut_ptr().add(i as usize);
        let column_type = (*att).atttypid;
        let attr: Datum;
        let value: *mut c_char;
        let mut tmp: *mut c_char;
        let mut nq: bool;

        /* Ignore dropped columns in datatype */
        if (*att).attisdropped {
            i += 1;
            continue;
        }

        if needComma {
            appendStringInfoChar(&raw mut buf, b',' as c_char);
        }
        needComma = true;

        if *nulls.add(i as usize) {
            /* emit nothing... */
            i += 1;
            continue;
        }

        /*
         * Convert the column value to text
         */
        if (*column_info).column_type != column_type {
            getTypeOutputInfo(
                column_type,
                &raw mut (*column_info).typiofunc,
                &raw mut (*column_info).typisvarlena,
            );
            fmgr_info_cxt(
                (*column_info).typiofunc,
                &raw mut (*column_info).proc_,
                (*(*fcinfo).flinfo).fn_mcxt,
            );
            (*column_info).column_type = column_type;
        }

        attr = *values.add(i as usize);
        value = OutputFunctionCall(&raw mut (*column_info).proc_, attr);

        /* Detect whether we need double quotes for this value */
        nq = *value.add(0) == b'\0' as c_char; /* force quotes for empty string */
        tmp = value;
        while *tmp != 0 {
            let ch = *tmp;

            if ch == b'"' as c_char
                || ch == b'\\' as c_char
                || ch == b'(' as c_char
                || ch == b')' as c_char
                || ch == b',' as c_char
                || isspace(ch as c_uchar as c_int) != 0
            {
                nq = true;
                break;
            }
            tmp = tmp.add(1);
        }

        /* And emit the string */
        if nq {
            appendStringInfoCharMacro!(&raw mut buf, b'"' as c_char);
        }
        tmp = value;
        while *tmp != 0 {
            let ch = *tmp;

            if ch == b'"' as c_char || ch == b'\\' as c_char {
                appendStringInfoCharMacro!(&raw mut buf, ch);
            }
            appendStringInfoCharMacro!(&raw mut buf, ch);
            tmp = tmp.add(1);
        }
        if nq {
            appendStringInfoCharMacro!(&raw mut buf, b'"' as c_char);
        }

        i += 1;
    }

    appendStringInfoChar(&raw mut buf, b')' as c_char);

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_CSTRING!(buf.data)
}

/*
 * record_recv		- binary input routine for any composite type.
 */
pub unsafe fn record_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let tupType = PG_GETARG_OID!(fcinfo, 1);
    let tupTypmod = PG_GETARG_INT32!(fcinfo, 2);
    let result: HeapTupleHeader;
    let tupdesc: TupleDesc;
    let tuple;
    let mut my_extra: *mut RecordIOData;
    let ncolumns: c_int;
    let usercols: c_int;
    let mut validcols: c_int;
    let mut i: c_int;
    let values: *mut Datum;
    let nulls: *mut bool;

    check_stack_depth(); /* recurses for record-type columns */

    /*
     * Give a friendly error message if we did not get enough info to identify
     * the target record type.  (lookup_rowtype_tupdesc would fail anyway, but
     * with a non-user-friendly message.)  In ordinary SQL usage, we'll get -1
     * for typmod, since composite types and RECORD have no type modifiers at
     * the SQL level, and thus must fail for RECORD.  However some callers can
     * supply a valid typmod, and then we can do something useful for RECORD.
     */
    if tupType == RECORDOID && tupTypmod < 0 {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("input of anonymous composite types is not implemented")
        );
    }

    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = (*tupdesc).natts;

    /*
     * We arrange to look up the needed I/O info just once per series of calls,
     * assuming the record type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
    if my_extra.is_null() || (*my_extra).ncolumns != ncolumns {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
        (*my_extra).record_type = InvalidOid;
        (*my_extra).record_typmod = 0;
    }

    if (*my_extra).record_type != tupType || (*my_extra).record_typmod != tupTypmod {
        MemSet(
            my_extra as *mut c_void,
            0,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        (*my_extra).record_type = tupType;
        (*my_extra).record_typmod = tupTypmod;
        (*my_extra).ncolumns = ncolumns;
    }

    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Fetch number of columns user thinks it has */
    usercols = pq_getmsgint(buf, 4) as c_int;

    /* Need to scan to count nondeleted columns */
    validcols = 0;
    i = 0;
    while i < ncolumns {
        if !(*TupleDescAttr(tupdesc, i)).attisdropped {
            validcols += 1;
        }
        i += 1;
    }
    if usercols != validcols {
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        ereport!(
            ERROR,
            errmsg!("wrong number of columns: {}, expected {}", usercols, validcols)
        );
    }

    /* Process each column */
    i = 0;
    while i < ncolumns {
        let att = TupleDescAttr(tupdesc, i);
        let column_info = (*my_extra).columns.as_mut_ptr().add(i as usize);
        let column_type = (*att).atttypid;
        let coltypoid: Oid;
        let itemlen: c_int;
        let mut item_buf: StringInfoData = core::mem::zeroed();
        let bufptr: StringInfo;

        /* Ignore dropped columns in datatype, but fill with nulls */
        if (*att).attisdropped {
            *values.add(i as usize) = 0 as Datum;
            *nulls.add(i as usize) = true;
            i += 1;
            continue;
        }

        /* Check column type recorded in the data */
        coltypoid = pq_getmsgint(buf, core::mem::size_of::<Oid>() as c_int) as Oid;

        /*
         * From a security standpoint, it doesn't matter whether the input's
         * column type matches what we expect: the column type's receive
         * function has to be robust enough to cope with invalid data.  However,
         * from a user-friendliness standpoint, it's nicer to complain about
         * type mismatches than to throw "improper binary format" errors.  But
         * there's a problem: only built-in types have OIDs that are stable
         * enough to believe that a mismatch is a real issue.  So complain only
         * if both OIDs are in the built-in range.  Otherwise, carry on with the
         * column type we "should" be getting.
         */
        if coltypoid != column_type
            && coltypoid < FirstGenbkiObjectId
            && column_type < FirstGenbkiObjectId
        {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "binary data has type {} ({}) instead of expected {} ({}) in record column {}",
                    coltypoid,
                    CStr::from_ptr(format_type_extended(coltypoid, -1, FORMAT_TYPE_ALLOW_INVALID as bits16))
                        .to_string_lossy(),
                    column_type,
                    CStr::from_ptr(format_type_extended(column_type, -1, FORMAT_TYPE_ALLOW_INVALID as bits16))
                        .to_string_lossy(),
                    i + 1
                )
            );
        }

        /* Get and check the item length */
        itemlen = pq_getmsgint(buf, 4) as c_int;
        if itemlen < -1 || itemlen > ((*buf).len - (*buf).cursor) {
            let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
            ereport!(ERROR, errmsg!("insufficient data left in message"));
        }

        if itemlen == -1 {
            /* -1 length means NULL */
            bufptr = null_mut();
            *nulls.add(i as usize) = true;
        } else {
            /*
             * Rather than copying data around, we just initialize a StringInfo
             * pointing to the correct portion of the message buffer.
             */
            let strbuff = (*buf).data.add((*buf).cursor as usize);
            (*buf).cursor += itemlen;
            initReadOnlyStringInfo(&raw mut item_buf, strbuff, itemlen);

            bufptr = &raw mut item_buf;
            *nulls.add(i as usize) = false;
        }

        /* Now call the column's receiveproc */
        if (*column_info).column_type != column_type {
            getTypeBinaryInputInfo(
                column_type,
                &raw mut (*column_info).typiofunc,
                &raw mut (*column_info).typioparam,
            );
            fmgr_info_cxt(
                (*column_info).typiofunc,
                &raw mut (*column_info).proc_,
                (*(*fcinfo).flinfo).fn_mcxt,
            );
            (*column_info).column_type = column_type;
        }

        *values.add(i as usize) = ReceiveFunctionCall(
            &raw mut (*column_info).proc_,
            bufptr,
            (*column_info).typioparam,
            (*att).atttypmod,
        );

        if !bufptr.is_null() {
            /* Trouble if it didn't eat the whole buffer */
            if item_buf.cursor != itemlen {
                let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
                ereport!(
                    ERROR,
                    errmsg!("improper binary format in record column {}", i + 1)
                );
            }
        }

        i += 1;
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);

    /*
     * We cannot return tuple->t_data because heap_form_tuple allocates it as
     * part of a larger chunk, and our caller may expect to be able to pfree our
     * result.  So must copy the info into a new palloc chunk.
     */
    result = palloc((*tuple).t_len as usize) as HeapTupleHeader;
    std::ptr::copy_nonoverlapping(
        (*tuple).t_data as *const u8,
        result as *mut u8,
        (*tuple).t_len as usize,
    );

    heap_freetuple(tuple);
    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_HEAPTUPLEHEADER(result)
}

/*
 * record_send		- binary output routine for any composite type.
 */
pub unsafe fn record_send(fcinfo: FunctionCallInfo) -> Datum {
    let rec = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let tupType: Oid;
    let tupTypmod: int32;
    let tupdesc: TupleDesc;
    let mut tuple: HeapTupleData = core::mem::zeroed();
    let mut my_extra: *mut RecordIOData;
    let ncolumns: c_int;
    let mut validcols: c_int;
    let mut i: c_int;
    let values: *mut Datum;
    let nulls: *mut bool;
    let mut buf: StringInfoData = core::mem::zeroed();

    check_stack_depth(); /* recurses for record-type columns */

    /* Extract type info from the tuple itself */
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = (*tupdesc).natts;

    /* Build a temporary HeapTuple control structure */
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&raw mut tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    /*
     * We arrange to look up the needed I/O info just once per series of calls,
     * assuming the record type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
    if my_extra.is_null() || (*my_extra).ncolumns != ncolumns {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordIOData;
        (*my_extra).record_type = InvalidOid;
        (*my_extra).record_typmod = 0;
    }

    if (*my_extra).record_type != tupType || (*my_extra).record_typmod != tupTypmod {
        MemSet(
            my_extra as *mut c_void,
            0,
            core::mem::offset_of!(RecordIOData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        (*my_extra).record_type = tupType;
        (*my_extra).record_typmod = tupTypmod;
        (*my_extra).ncolumns = ncolumns;
    }

    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Break down the tuple into fields */
    heap_deform_tuple(&raw mut tuple, tupdesc, values, nulls);

    /* And build the result string */
    pq_begintypsend(&raw mut buf);

    /* Need to scan to count nondeleted columns */
    validcols = 0;
    i = 0;
    while i < ncolumns {
        if !(*TupleDescAttr(tupdesc, i)).attisdropped {
            validcols += 1;
        }
        i += 1;
    }
    pq_sendint32(&raw mut buf, validcols as uint32);

    i = 0;
    while i < ncolumns {
        let att = TupleDescAttr(tupdesc, i);
        let column_info = (*my_extra).columns.as_mut_ptr().add(i as usize);
        let column_type = (*att).atttypid;
        let attr: Datum;
        let outputbytes: *mut bytea;

        /* Ignore dropped columns in datatype */
        if (*att).attisdropped {
            i += 1;
            continue;
        }

        pq_sendint32(&raw mut buf, column_type as uint32);

        if *nulls.add(i as usize) {
            /* emit -1 data length to signify a NULL */
            pq_sendint32(&raw mut buf, (-1i32) as uint32);
            i += 1;
            continue;
        }

        /*
         * Convert the column value to binary
         */
        if (*column_info).column_type != column_type {
            getTypeBinaryOutputInfo(
                column_type,
                &raw mut (*column_info).typiofunc,
                &raw mut (*column_info).typisvarlena,
            );
            fmgr_info_cxt(
                (*column_info).typiofunc,
                &raw mut (*column_info).proc_,
                (*(*fcinfo).flinfo).fn_mcxt,
            );
            (*column_info).column_type = column_type;
        }

        attr = *values.add(i as usize);
        outputbytes = SendFunctionCall(&raw mut (*column_info).proc_, attr);
        pq_sendint32(
            &raw mut buf,
            (VARSIZE(outputbytes as *const c_char) as int32 - VARHDRSZ) as uint32,
        );
        pq_sendbytes(
            &raw mut buf,
            VARDATA(outputbytes as *const c_char) as *const c_void,
            VARSIZE(outputbytes as *const c_char) as int32 - VARHDRSZ,
        );

        i += 1;
    }

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf))
}

/*
 * record_cmp()
 * Internal comparison function for records.
 *
 * Returns -1, 0 or 1
 *
 * Do not assume that the two inputs are exactly the same record type; for
 * instance we might be comparing an anonymous ROW() construct against a named
 * composite type.  We will compare as long as they have the same number of
 * non-dropped columns of the same types.
 */
unsafe fn record_cmp(fcinfo: FunctionCallInfo) -> c_int {
    let record1 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let record2 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 1);
    let mut result: c_int = 0;
    let tupType1: Oid;
    let tupType2: Oid;
    let tupTypmod1: int32;
    let tupTypmod2: int32;
    let tupdesc1: TupleDesc;
    let tupdesc2: TupleDesc;
    let mut tuple1: HeapTupleData = core::mem::zeroed();
    let mut tuple2: HeapTupleData = core::mem::zeroed();
    let ncolumns1: c_int;
    let ncolumns2: c_int;
    let mut my_extra: *mut RecordCompareData;
    let ncols: c_int;
    let values1: *mut Datum;
    let values2: *mut Datum;
    let nulls1: *mut bool;
    let nulls2: *mut bool;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut j: c_int;

    check_stack_depth(); /* recurses for record-type columns */

    /* Extract type info from the tuples */
    tupType1 = HeapTupleHeaderGetTypeId(record1);
    tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
    tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
    ncolumns1 = (*tupdesc1).natts;
    tupType2 = HeapTupleHeaderGetTypeId(record2);
    tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
    tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
    ncolumns2 = (*tupdesc2).natts;

    /* Build temporary HeapTuple control structures */
    tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
    ItemPointerSetInvalid(&raw mut tuple1.t_self);
    tuple1.t_tableOid = InvalidOid;
    tuple1.t_data = record1;
    tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
    ItemPointerSetInvalid(&raw mut tuple2.t_self);
    tuple2.t_tableOid = InvalidOid;
    tuple2.t_data = record2;

    /*
     * We arrange to look up the needed comparison info just once per series of
     * calls, assuming the record types don't change underneath us.
     */
    ncols = Max(ncolumns1, ncolumns2);
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
    if my_extra.is_null() || (*my_extra).ncolumns < ncols {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordCompareData, columns)
                + ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
        (*my_extra).ncolumns = ncols;
        (*my_extra).record1_type = InvalidOid;
        (*my_extra).record1_typmod = 0;
        (*my_extra).record2_type = InvalidOid;
        (*my_extra).record2_typmod = 0;
    }

    if (*my_extra).record1_type != tupType1
        || (*my_extra).record1_typmod != tupTypmod1
        || (*my_extra).record2_type != tupType2
        || (*my_extra).record2_typmod != tupTypmod2
    {
        MemSet(
            (*my_extra).columns.as_mut_ptr() as *mut c_void,
            0,
            ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        (*my_extra).record1_type = tupType1;
        (*my_extra).record1_typmod = tupTypmod1;
        (*my_extra).record2_type = tupType2;
        (*my_extra).record2_typmod = tupTypmod2;
    }

    /* Break down the tuples into fields */
    values1 = palloc(ncolumns1 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls1 = palloc(ncolumns1 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple1, tupdesc1, values1, nulls1);
    values2 = palloc(ncolumns2 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls2 = palloc(ncolumns2 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple2, tupdesc2, values2, nulls2);

    /*
     * Scan corresponding columns, allowing for dropped columns in different
     * places in the two rows.  i1 and i2 are physical column indexes, j is the
     * logical column index.
     */
    i1 = 0;
    i2 = 0;
    j = 0;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        let att1;
        let att2;
        let mut typentry: *mut TypeCacheEntry;
        let mut collation: Oid;

        /*
         * Skip dropped columns
         */
        if i1 < ncolumns1 && (*TupleDescAttr(tupdesc1, i1)).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && (*TupleDescAttr(tupdesc2, i2)).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break; /* we'll deal with mismatch below loop */
        }

        att1 = TupleDescAttr(tupdesc1, i1);
        att2 = TupleDescAttr(tupdesc2, i2);

        /*
         * Have two matching columns, they must be same type
         */
        if (*att1).atttypid != (*att2).atttypid {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "cannot compare dissimilar column types {} and {} at record column {}",
                    CStr::from_ptr(format_type_be((*att1).atttypid)).to_string_lossy(),
                    CStr::from_ptr(format_type_be((*att2).atttypid)).to_string_lossy(),
                    j + 1
                )
            );
        }

        /*
         * If they're not same collation, we don't complain here, but the
         * comparison function might.
         */
        collation = (*att1).attcollation;
        if collation != (*att2).attcollation {
            collation = InvalidOid;
        }

        /*
         * Lookup the comparison function if not done already
         */
        typentry = (*(*my_extra).columns.as_mut_ptr().add(j as usize)).typentry;
        if typentry.is_null() || (*typentry).type_id != (*att1).atttypid {
            typentry = lookup_type_cache((*att1).atttypid, TYPECACHE_CMP_PROC_FINFO);
            if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
                let _ = errcode(ERRCODE_UNDEFINED_FUNCTION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify a comparison function for type {}",
                        CStr::from_ptr(format_type_be((*typentry).type_id)).to_string_lossy()
                    )
                );
            }
            (*(*my_extra).columns.as_mut_ptr().add(j as usize)).typentry = typentry;
        }

        /*
         * We consider two NULLs equal; NULL > not-NULL.
         */
        if !*nulls1.add(i1 as usize) || !*nulls2.add(i2 as usize) {
            LOCAL_FCINFO!(locfcinfo, 2);
            let cmpresult: int32;

            if *nulls1.add(i1 as usize) {
                /* arg1 is greater than arg2 */
                result = 1;
                break;
            }
            if *nulls2.add(i2 as usize) {
                /* arg1 is less than arg2 */
                result = -1;
                break;
            }

            /* Compare the pair of elements */
            InitFunctionCallInfoData!(
                locfcinfo,
                &raw mut (*typentry).cmp_proc_finfo,
                2,
                collation,
                null_mut(),
                null_mut()
            );
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = *values1.add(i1 as usize);
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).value = *values2.add(i2 as usize);
            (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
            cmpresult = DatumGetInt32(FunctionCallInvoke!(locfcinfo));

            /* We don't expect comparison support functions to return null */
            Assert!(!(*locfcinfo).isnull);

            if cmpresult < 0 {
                /* arg1 is less than arg2 */
                result = -1;
                break;
            } else if cmpresult > 0 {
                /* arg1 is greater than arg2 */
                result = 1;
                break;
            }
        }

        /* equal, so continue to next column */
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    /*
     * If we didn't break out of the loop early, check for column count
     * mismatch.  (We do not report such mismatch if we found unequal column
     * values; is that a feature or a bug?)
     */
    if result == 0 {
        if i1 != ncolumns1 || i2 != ncolumns2 {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!("cannot compare record types with different numbers of columns")
            );
        }
    }

    pfree(values1 as *mut c_void);
    pfree(nulls1 as *mut c_void);
    pfree(values2 as *mut c_void);
    pfree(nulls2 as *mut c_void);
    ReleaseTupleDesc(tupdesc1);
    ReleaseTupleDesc(tupdesc2);

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, record1, 0);
    PG_FREE_IF_COPY!(fcinfo, record2, 1);

    result
}

/*
 * record_eq :
 *		  compares two records for equality
 * result :
 *		  returns true if the records are equal, false otherwise.
 *
 * Note: we do not use record_cmp here, since equality may be meaningful in
 * datatypes that don't have a total ordering (and hence no btree support).
 */
pub unsafe fn record_eq(fcinfo: FunctionCallInfo) -> Datum {
    let record1 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let record2 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 1);
    let mut result = true;
    let tupType1: Oid;
    let tupType2: Oid;
    let tupTypmod1: int32;
    let tupTypmod2: int32;
    let tupdesc1: TupleDesc;
    let tupdesc2: TupleDesc;
    let mut tuple1: HeapTupleData = core::mem::zeroed();
    let mut tuple2: HeapTupleData = core::mem::zeroed();
    let ncolumns1: c_int;
    let ncolumns2: c_int;
    let mut my_extra: *mut RecordCompareData;
    let ncols: c_int;
    let values1: *mut Datum;
    let values2: *mut Datum;
    let nulls1: *mut bool;
    let nulls2: *mut bool;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut j: c_int;

    check_stack_depth(); /* recurses for record-type columns */

    /* Extract type info from the tuples */
    tupType1 = HeapTupleHeaderGetTypeId(record1);
    tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
    tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
    ncolumns1 = (*tupdesc1).natts;
    tupType2 = HeapTupleHeaderGetTypeId(record2);
    tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
    tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
    ncolumns2 = (*tupdesc2).natts;

    /* Build temporary HeapTuple control structures */
    tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
    ItemPointerSetInvalid(&raw mut tuple1.t_self);
    tuple1.t_tableOid = InvalidOid;
    tuple1.t_data = record1;
    tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
    ItemPointerSetInvalid(&raw mut tuple2.t_self);
    tuple2.t_tableOid = InvalidOid;
    tuple2.t_data = record2;

    /*
     * We arrange to look up the needed comparison info just once per series of
     * calls, assuming the record types don't change underneath us.
     */
    ncols = Max(ncolumns1, ncolumns2);
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
    if my_extra.is_null() || (*my_extra).ncolumns < ncols {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordCompareData, columns)
                + ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
        (*my_extra).ncolumns = ncols;
        (*my_extra).record1_type = InvalidOid;
        (*my_extra).record1_typmod = 0;
        (*my_extra).record2_type = InvalidOid;
        (*my_extra).record2_typmod = 0;
    }

    if (*my_extra).record1_type != tupType1
        || (*my_extra).record1_typmod != tupTypmod1
        || (*my_extra).record2_type != tupType2
        || (*my_extra).record2_typmod != tupTypmod2
    {
        MemSet(
            (*my_extra).columns.as_mut_ptr() as *mut c_void,
            0,
            ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        (*my_extra).record1_type = tupType1;
        (*my_extra).record1_typmod = tupTypmod1;
        (*my_extra).record2_type = tupType2;
        (*my_extra).record2_typmod = tupTypmod2;
    }

    /* Break down the tuples into fields */
    values1 = palloc(ncolumns1 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls1 = palloc(ncolumns1 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple1, tupdesc1, values1, nulls1);
    values2 = palloc(ncolumns2 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls2 = palloc(ncolumns2 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple2, tupdesc2, values2, nulls2);

    /*
     * Scan corresponding columns, allowing for dropped columns in different
     * places in the two rows.  i1 and i2 are physical column indexes, j is the
     * logical column index.
     */
    i1 = 0;
    i2 = 0;
    j = 0;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        LOCAL_FCINFO!(locfcinfo, 2);
        let att1;
        let att2;
        let mut typentry: *mut TypeCacheEntry;
        let mut collation: Oid;
        let oprresult: bool;

        /*
         * Skip dropped columns
         */
        if i1 < ncolumns1 && (*TupleDescAttr(tupdesc1, i1)).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && (*TupleDescAttr(tupdesc2, i2)).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break; /* we'll deal with mismatch below loop */
        }

        att1 = TupleDescAttr(tupdesc1, i1);
        att2 = TupleDescAttr(tupdesc2, i2);

        /*
         * Have two matching columns, they must be same type
         */
        if (*att1).atttypid != (*att2).atttypid {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "cannot compare dissimilar column types {} and {} at record column {}",
                    CStr::from_ptr(format_type_be((*att1).atttypid)).to_string_lossy(),
                    CStr::from_ptr(format_type_be((*att2).atttypid)).to_string_lossy(),
                    j + 1
                )
            );
        }

        /*
         * If they're not same collation, we don't complain here, but the
         * equality function might.
         */
        collation = (*att1).attcollation;
        if collation != (*att2).attcollation {
            collation = InvalidOid;
        }

        /*
         * Lookup the equality function if not done already
         */
        typentry = (*(*my_extra).columns.as_mut_ptr().add(j as usize)).typentry;
        if typentry.is_null() || (*typentry).type_id != (*att1).atttypid {
            typentry = lookup_type_cache((*att1).atttypid, TYPECACHE_EQ_OPR_FINFO);
            if !OidIsValid((*typentry).eq_opr_finfo.fn_oid) {
                let _ = errcode(ERRCODE_UNDEFINED_FUNCTION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify an equality operator for type {}",
                        CStr::from_ptr(format_type_be((*typentry).type_id)).to_string_lossy()
                    )
                );
            }
            (*(*my_extra).columns.as_mut_ptr().add(j as usize)).typentry = typentry;
        }

        /*
         * We consider two NULLs equal; NULL > not-NULL.
         */
        if !*nulls1.add(i1 as usize) || !*nulls2.add(i2 as usize) {
            if *nulls1.add(i1 as usize) || *nulls2.add(i2 as usize) {
                result = false;
                break;
            }

            /* Compare the pair of elements */
            InitFunctionCallInfoData!(
                locfcinfo,
                &raw mut (*typentry).eq_opr_finfo,
                2,
                collation,
                null_mut(),
                null_mut()
            );
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = *values1.add(i1 as usize);
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).value = *values2.add(i2 as usize);
            (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
            oprresult = DatumGetBool(FunctionCallInvoke!(locfcinfo));
            if (*locfcinfo).isnull || !oprresult {
                result = false;
                break;
            }
        }

        /* equal, so continue to next column */
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    /*
     * If we didn't break out of the loop early, check for column count
     * mismatch.  (We do not report such mismatch if we found unequal column
     * values; is that a feature or a bug?)
     */
    if result {
        if i1 != ncolumns1 || i2 != ncolumns2 {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!("cannot compare record types with different numbers of columns")
            );
        }
    }

    pfree(values1 as *mut c_void);
    pfree(nulls1 as *mut c_void);
    pfree(values2 as *mut c_void);
    pfree(nulls2 as *mut c_void);
    ReleaseTupleDesc(tupdesc1);
    ReleaseTupleDesc(tupdesc2);

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, record1, 0);
    PG_FREE_IF_COPY!(fcinfo, record2, 1);

    PG_RETURN_BOOL!(result)
}

pub unsafe fn record_ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(!DatumGetBool(record_eq(fcinfo)))
}

pub unsafe fn record_lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_cmp(fcinfo) < 0)
}

pub unsafe fn record_gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_cmp(fcinfo) > 0)
}

pub unsafe fn record_le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_cmp(fcinfo) <= 0)
}

pub unsafe fn record_ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_cmp(fcinfo) >= 0)
}

pub unsafe fn btrecordcmp(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(record_cmp(fcinfo))
}

pub unsafe fn record_larger(fcinfo: FunctionCallInfo) -> Datum {
    if record_cmp(fcinfo) > 0 {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 0))
    } else {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 1))
    }
}

pub unsafe fn record_smaller(fcinfo: FunctionCallInfo) -> Datum {
    if record_cmp(fcinfo) < 0 {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 0))
    } else {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 1))
    }
}

/*
 * record_image_cmp :
 * Internal byte-oriented comparison function for records.
 *
 * Returns -1, 0 or 1
 *
 * Note: The normal concepts of "equality" do not apply here; different
 * representation of values considered to be equal are not considered to be
 * identical.  As an example, for the citext type 'A' and 'a' are equal, but
 * they are not identical.
 */
unsafe fn record_image_cmp(fcinfo: FunctionCallInfo) -> c_int {
    let record1 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let record2 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 1);
    let mut result: c_int = 0;
    let tupType1: Oid;
    let tupType2: Oid;
    let tupTypmod1: int32;
    let tupTypmod2: int32;
    let tupdesc1: TupleDesc;
    let tupdesc2: TupleDesc;
    let mut tuple1: HeapTupleData = core::mem::zeroed();
    let mut tuple2: HeapTupleData = core::mem::zeroed();
    let ncolumns1: c_int;
    let ncolumns2: c_int;
    let mut my_extra: *mut RecordCompareData;
    let ncols: c_int;
    let values1: *mut Datum;
    let values2: *mut Datum;
    let nulls1: *mut bool;
    let nulls2: *mut bool;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut j: c_int;

    /* Extract type info from the tuples */
    tupType1 = HeapTupleHeaderGetTypeId(record1);
    tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
    tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
    ncolumns1 = (*tupdesc1).natts;
    tupType2 = HeapTupleHeaderGetTypeId(record2);
    tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
    tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
    ncolumns2 = (*tupdesc2).natts;

    /* Build temporary HeapTuple control structures */
    tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
    ItemPointerSetInvalid(&raw mut tuple1.t_self);
    tuple1.t_tableOid = InvalidOid;
    tuple1.t_data = record1;
    tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
    ItemPointerSetInvalid(&raw mut tuple2.t_self);
    tuple2.t_tableOid = InvalidOid;
    tuple2.t_data = record2;

    /*
     * We arrange to look up the needed comparison info just once per series of
     * calls, assuming the record types don't change underneath us.
     */
    ncols = Max(ncolumns1, ncolumns2);
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
    if my_extra.is_null() || (*my_extra).ncolumns < ncols {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordCompareData, columns)
                + ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
        (*my_extra).ncolumns = ncols;
        (*my_extra).record1_type = InvalidOid;
        (*my_extra).record1_typmod = 0;
        (*my_extra).record2_type = InvalidOid;
        (*my_extra).record2_typmod = 0;
    }

    if (*my_extra).record1_type != tupType1
        || (*my_extra).record1_typmod != tupTypmod1
        || (*my_extra).record2_type != tupType2
        || (*my_extra).record2_typmod != tupTypmod2
    {
        MemSet(
            (*my_extra).columns.as_mut_ptr() as *mut c_void,
            0,
            ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        (*my_extra).record1_type = tupType1;
        (*my_extra).record1_typmod = tupTypmod1;
        (*my_extra).record2_type = tupType2;
        (*my_extra).record2_typmod = tupTypmod2;
    }

    /* Break down the tuples into fields */
    values1 = palloc(ncolumns1 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls1 = palloc(ncolumns1 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple1, tupdesc1, values1, nulls1);
    values2 = palloc(ncolumns2 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls2 = palloc(ncolumns2 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple2, tupdesc2, values2, nulls2);

    /*
     * Scan corresponding columns, allowing for dropped columns in different
     * places in the two rows.  i1 and i2 are physical column indexes, j is the
     * logical column index.
     */
    i1 = 0;
    i2 = 0;
    j = 0;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        let att1;
        let att2;

        /*
         * Skip dropped columns
         */
        if i1 < ncolumns1 && (*TupleDescAttr(tupdesc1, i1)).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && (*TupleDescAttr(tupdesc2, i2)).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break; /* we'll deal with mismatch below loop */
        }

        att1 = TupleDescAttr(tupdesc1, i1);
        att2 = TupleDescAttr(tupdesc2, i2);

        /*
         * Have two matching columns, they must be same type
         */
        if (*att1).atttypid != (*att2).atttypid {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "cannot compare dissimilar column types {} and {} at record column {}",
                    CStr::from_ptr(format_type_be((*att1).atttypid)).to_string_lossy(),
                    CStr::from_ptr(format_type_be((*att2).atttypid)).to_string_lossy(),
                    j + 1
                )
            );
        }

        /*
         * The same type should have the same length (or both should be
         * variable).
         */
        Assert!((*att1).attlen == (*att2).attlen);

        /*
         * We consider two NULLs equal; NULL > not-NULL.
         */
        if !*nulls1.add(i1 as usize) || !*nulls2.add(i2 as usize) {
            let mut cmpresult: c_int = 0;

            if *nulls1.add(i1 as usize) {
                /* arg1 is greater than arg2 */
                result = 1;
                break;
            }
            if *nulls2.add(i2 as usize) {
                /* arg1 is less than arg2 */
                result = -1;
                break;
            }

            /* Compare the pair of elements */
            if (*att1).attbyval {
                if *values1.add(i1 as usize) != *values2.add(i2 as usize) {
                    cmpresult = if *values1.add(i1 as usize) < *values2.add(i2 as usize) {
                        -1
                    } else {
                        1
                    };
                }
            } else if (*att1).attlen > 0 {
                cmpresult = libc_memcmp(
                    DatumGetPointer(*values1.add(i1 as usize)) as *const c_void,
                    DatumGetPointer(*values2.add(i2 as usize)) as *const c_void,
                    (*att1).attlen as Size,
                );
            } else if (*att1).attlen == -1 {
                let len1: Size;
                let len2: Size;
                let arg1val: *mut varlena;
                let arg2val: *mut varlena;

                len1 = toast_raw_datum_size(*values1.add(i1 as usize));
                len2 = toast_raw_datum_size(*values2.add(i2 as usize));
                arg1val = PG_DETOAST_DATUM_PACKED!(*values1.add(i1 as usize));
                arg2val = PG_DETOAST_DATUM_PACKED!(*values2.add(i2 as usize));

                cmpresult = libc_memcmp(
                    VARDATA_ANY(arg1val as *const c_char) as *const c_void,
                    VARDATA_ANY(arg2val as *const c_char) as *const c_void,
                    Min(len1, len2) - VARHDRSZ as Size,
                );
                if cmpresult == 0 && len1 != len2 {
                    cmpresult = if len1 < len2 { -1 } else { 1 };
                }

                if arg1val as Pointer != *values1.add(i1 as usize) as Pointer {
                    pfree(arg1val as *mut c_void);
                }
                if arg2val as Pointer != *values2.add(i2 as usize) as Pointer {
                    pfree(arg2val as *mut c_void);
                }
            } else {
                elog!(ERROR, "unexpected attlen: {}", (*att1).attlen);
            }

            if cmpresult < 0 {
                /* arg1 is less than arg2 */
                result = -1;
                break;
            } else if cmpresult > 0 {
                /* arg1 is greater than arg2 */
                result = 1;
                break;
            }
        }

        /* equal, so continue to next column */
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    /*
     * If we didn't break out of the loop early, check for column count
     * mismatch.  (We do not report such mismatch if we found unequal column
     * values; is that a feature or a bug?)
     */
    if result == 0 {
        if i1 != ncolumns1 || i2 != ncolumns2 {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!("cannot compare record types with different numbers of columns")
            );
        }
    }

    pfree(values1 as *mut c_void);
    pfree(nulls1 as *mut c_void);
    pfree(values2 as *mut c_void);
    pfree(nulls2 as *mut c_void);
    ReleaseTupleDesc(tupdesc1);
    ReleaseTupleDesc(tupdesc2);

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, record1, 0);
    PG_FREE_IF_COPY!(fcinfo, record2, 1);

    result
}

/*
 * record_image_eq :
 *		  compares two records for identical contents, based on byte images
 * result :
 *		  returns true if the records are identical, false otherwise.
 *
 * Note: we do not use record_image_cmp here, since we can avoid de-toasting for
 * unequal lengths this way.
 */
pub unsafe fn record_image_eq(fcinfo: FunctionCallInfo) -> Datum {
    let record1 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let record2 = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 1);
    let mut result = true;
    let tupType1: Oid;
    let tupType2: Oid;
    let tupTypmod1: int32;
    let tupTypmod2: int32;
    let tupdesc1: TupleDesc;
    let tupdesc2: TupleDesc;
    let mut tuple1: HeapTupleData = core::mem::zeroed();
    let mut tuple2: HeapTupleData = core::mem::zeroed();
    let ncolumns1: c_int;
    let ncolumns2: c_int;
    let mut my_extra: *mut RecordCompareData;
    let ncols: c_int;
    let values1: *mut Datum;
    let values2: *mut Datum;
    let nulls1: *mut bool;
    let nulls2: *mut bool;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut j: c_int;

    /* Extract type info from the tuples */
    tupType1 = HeapTupleHeaderGetTypeId(record1);
    tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
    tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
    ncolumns1 = (*tupdesc1).natts;
    tupType2 = HeapTupleHeaderGetTypeId(record2);
    tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
    tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
    ncolumns2 = (*tupdesc2).natts;

    /* Build temporary HeapTuple control structures */
    tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
    ItemPointerSetInvalid(&raw mut tuple1.t_self);
    tuple1.t_tableOid = InvalidOid;
    tuple1.t_data = record1;
    tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
    ItemPointerSetInvalid(&raw mut tuple2.t_self);
    tuple2.t_tableOid = InvalidOid;
    tuple2.t_data = record2;

    /*
     * We arrange to look up the needed comparison info just once per series of
     * calls, assuming the record types don't change underneath us.
     */
    ncols = Max(ncolumns1, ncolumns2);
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
    if my_extra.is_null() || (*my_extra).ncolumns < ncols {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordCompareData, columns)
                + ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
        (*my_extra).ncolumns = ncols;
        (*my_extra).record1_type = InvalidOid;
        (*my_extra).record1_typmod = 0;
        (*my_extra).record2_type = InvalidOid;
        (*my_extra).record2_typmod = 0;
    }

    if (*my_extra).record1_type != tupType1
        || (*my_extra).record1_typmod != tupTypmod1
        || (*my_extra).record2_type != tupType2
        || (*my_extra).record2_typmod != tupTypmod2
    {
        MemSet(
            (*my_extra).columns.as_mut_ptr() as *mut c_void,
            0,
            ncols as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        (*my_extra).record1_type = tupType1;
        (*my_extra).record1_typmod = tupTypmod1;
        (*my_extra).record2_type = tupType2;
        (*my_extra).record2_typmod = tupTypmod2;
    }

    /* Break down the tuples into fields */
    values1 = palloc(ncolumns1 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls1 = palloc(ncolumns1 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple1, tupdesc1, values1, nulls1);
    values2 = palloc(ncolumns2 as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls2 = palloc(ncolumns2 as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple2, tupdesc2, values2, nulls2);

    /*
     * Scan corresponding columns, allowing for dropped columns in different
     * places in the two rows.  i1 and i2 are physical column indexes, j is the
     * logical column index.
     */
    i1 = 0;
    i2 = 0;
    j = 0;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        let att1;
        let att2;

        /*
         * Skip dropped columns
         */
        if i1 < ncolumns1 && (*TupleDescAttr(tupdesc1, i1)).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && (*TupleDescAttr(tupdesc2, i2)).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break; /* we'll deal with mismatch below loop */
        }

        att1 = TupleDescAttr(tupdesc1, i1);
        att2 = TupleDescAttr(tupdesc2, i2);

        /*
         * Have two matching columns, they must be same type
         */
        if (*att1).atttypid != (*att2).atttypid {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "cannot compare dissimilar column types {} and {} at record column {}",
                    CStr::from_ptr(format_type_be((*att1).atttypid)).to_string_lossy(),
                    CStr::from_ptr(format_type_be((*att2).atttypid)).to_string_lossy(),
                    j + 1
                )
            );
        }

        /*
         * We consider two NULLs equal; NULL > not-NULL.
         */
        if !*nulls1.add(i1 as usize) || !*nulls2.add(i2 as usize) {
            if *nulls1.add(i1 as usize) || *nulls2.add(i2 as usize) {
                result = false;
                break;
            }

            /* Compare the pair of elements */
            result = datum_image_eq(
                *values1.add(i1 as usize),
                *values2.add(i2 as usize),
                (*att1).attbyval,
                (*att2).attlen as c_int,
            );
            if !result {
                break;
            }
        }

        /* equal, so continue to next column */
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    /*
     * If we didn't break out of the loop early, check for column count
     * mismatch.  (We do not report such mismatch if we found unequal column
     * values; is that a feature or a bug?)
     */
    if result {
        if i1 != ncolumns1 || i2 != ncolumns2 {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!("cannot compare record types with different numbers of columns")
            );
        }
    }

    pfree(values1 as *mut c_void);
    pfree(nulls1 as *mut c_void);
    pfree(values2 as *mut c_void);
    pfree(nulls2 as *mut c_void);
    ReleaseTupleDesc(tupdesc1);
    ReleaseTupleDesc(tupdesc2);

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, record1, 0);
    PG_FREE_IF_COPY!(fcinfo, record2, 1);

    PG_RETURN_BOOL!(result)
}

pub unsafe fn record_image_ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(!DatumGetBool(record_image_eq(fcinfo)))
}

pub unsafe fn record_image_lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_image_cmp(fcinfo) < 0)
}

pub unsafe fn record_image_gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_image_cmp(fcinfo) > 0)
}

pub unsafe fn record_image_le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_image_cmp(fcinfo) <= 0)
}

pub unsafe fn record_image_ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(record_image_cmp(fcinfo) >= 0)
}

pub unsafe fn btrecordimagecmp(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(record_image_cmp(fcinfo))
}

/*
 * Row type hash functions
 */

pub unsafe fn hash_record(fcinfo: FunctionCallInfo) -> Datum {
    let record = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let mut result: uint32 = 0;
    let tupType: Oid;
    let tupTypmod: int32;
    let tupdesc: TupleDesc;
    let mut tuple: HeapTupleData = core::mem::zeroed();
    let ncolumns: c_int;
    let mut my_extra: *mut RecordCompareData;
    let values: *mut Datum;
    let nulls: *mut bool;

    check_stack_depth(); /* recurses for record-type columns */

    /* Extract type info from tuple */
    tupType = HeapTupleHeaderGetTypeId(record);
    tupTypmod = HeapTupleHeaderGetTypMod(record);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = (*tupdesc).natts;

    /* Build temporary HeapTuple control structure */
    tuple.t_len = HeapTupleHeaderGetDatumLength(record);
    ItemPointerSetInvalid(&raw mut tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = record;

    /*
     * We arrange to look up the needed hashing info just once per series of
     * calls, assuming the record type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
    if my_extra.is_null() || (*my_extra).ncolumns < ncolumns {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordCompareData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
        (*my_extra).ncolumns = ncolumns;
        (*my_extra).record1_type = InvalidOid;
        (*my_extra).record1_typmod = 0;
    }

    if (*my_extra).record1_type != tupType || (*my_extra).record1_typmod != tupTypmod {
        MemSet(
            (*my_extra).columns.as_mut_ptr() as *mut c_void,
            0,
            ncolumns as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        (*my_extra).record1_type = tupType;
        (*my_extra).record1_typmod = tupTypmod;
    }

    /* Break down the tuple into fields */
    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple, tupdesc, values, nulls);

    let mut i: c_int = 0;
    while i < ncolumns {
        let att;
        let mut typentry: *mut TypeCacheEntry;
        let element_hash: uint32;

        att = TupleDescAttr(tupdesc, i);

        if (*att).attisdropped {
            i += 1;
            continue;
        }

        /*
         * Lookup the hash function if not done already
         */
        typentry = (*(*my_extra).columns.as_mut_ptr().add(i as usize)).typentry;
        if typentry.is_null() || (*typentry).type_id != (*att).atttypid {
            typentry = lookup_type_cache((*att).atttypid, TYPECACHE_HASH_PROC_FINFO);
            if !OidIsValid((*typentry).hash_proc_finfo.fn_oid) {
                let _ = errcode(ERRCODE_UNDEFINED_FUNCTION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify a hash function for type {}",
                        CStr::from_ptr(format_type_be((*typentry).type_id)).to_string_lossy()
                    )
                );
            }
            (*(*my_extra).columns.as_mut_ptr().add(i as usize)).typentry = typentry;
        }

        /* Compute hash of element */
        if *nulls.add(i as usize) {
            element_hash = 0;
        } else {
            LOCAL_FCINFO!(locfcinfo, 1);

            InitFunctionCallInfoData!(
                locfcinfo,
                &raw mut (*typentry).hash_proc_finfo,
                1,
                (*att).attcollation,
                null_mut(),
                null_mut()
            );
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = *values.add(i as usize);
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            element_hash = DatumGetUInt32(FunctionCallInvoke!(locfcinfo));

            /* We don't expect hash support functions to return null */
            Assert!(!(*locfcinfo).isnull);
        }

        /* see hash_array() */
        result = (result << 5).wrapping_sub(result).wrapping_add(element_hash);

        i += 1;
    }

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    ReleaseTupleDesc(tupdesc);

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, record, 0);

    PG_RETURN_UINT32!(result)
}

pub unsafe fn hash_record_extended(fcinfo: FunctionCallInfo) -> Datum {
    let record = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);
    let seed = PG_GETARG_INT64!(fcinfo, 1) as uint64;
    let mut result: uint64 = 0;
    let tupType: Oid;
    let tupTypmod: int32;
    let tupdesc: TupleDesc;
    let mut tuple: HeapTupleData = core::mem::zeroed();
    let ncolumns: c_int;
    let mut my_extra: *mut RecordCompareData;
    let values: *mut Datum;
    let nulls: *mut bool;

    check_stack_depth(); /* recurses for record-type columns */

    /* Extract type info from tuple */
    tupType = HeapTupleHeaderGetTypeId(record);
    tupTypmod = HeapTupleHeaderGetTypMod(record);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = (*tupdesc).natts;

    /* Build temporary HeapTuple control structure */
    tuple.t_len = HeapTupleHeaderGetDatumLength(record);
    ItemPointerSetInvalid(&raw mut tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = record;

    /*
     * We arrange to look up the needed hashing info just once per series of
     * calls, assuming the record type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
    if my_extra.is_null() || (*my_extra).ncolumns < ncolumns {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::offset_of!(RecordCompareData, columns)
                + ncolumns as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut RecordCompareData;
        (*my_extra).ncolumns = ncolumns;
        (*my_extra).record1_type = InvalidOid;
        (*my_extra).record1_typmod = 0;
    }

    if (*my_extra).record1_type != tupType || (*my_extra).record1_typmod != tupTypmod {
        MemSet(
            (*my_extra).columns.as_mut_ptr() as *mut c_void,
            0,
            ncolumns as usize * core::mem::size_of::<ColumnCompareData>(),
        );
        (*my_extra).record1_type = tupType;
        (*my_extra).record1_typmod = tupTypmod;
    }

    /* Break down the tuple into fields */
    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;
    heap_deform_tuple(&raw mut tuple, tupdesc, values, nulls);

    let mut i: c_int = 0;
    while i < ncolumns {
        let att;
        let mut typentry: *mut TypeCacheEntry;
        let element_hash: uint64;

        att = TupleDescAttr(tupdesc, i);

        if (*att).attisdropped {
            i += 1;
            continue;
        }

        /*
         * Lookup the hash function if not done already
         */
        typentry = (*(*my_extra).columns.as_mut_ptr().add(i as usize)).typentry;
        if typentry.is_null() || (*typentry).type_id != (*att).atttypid {
            typentry = lookup_type_cache((*att).atttypid, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
            if !OidIsValid((*typentry).hash_extended_proc_finfo.fn_oid) {
                let _ = errcode(ERRCODE_UNDEFINED_FUNCTION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify an extended hash function for type {}",
                        CStr::from_ptr(format_type_be((*typentry).type_id)).to_string_lossy()
                    )
                );
            }
            (*(*my_extra).columns.as_mut_ptr().add(i as usize)).typentry = typentry;
        }

        /* Compute hash of element */
        if *nulls.add(i as usize) {
            element_hash = 0;
        } else {
            LOCAL_FCINFO!(locfcinfo, 2);

            InitFunctionCallInfoData!(
                locfcinfo,
                &raw mut (*typentry).hash_extended_proc_finfo,
                2,
                (*att).attcollation,
                null_mut(),
                null_mut()
            );
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = *values.add(i as usize);
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).value = Int64GetDatum(seed as int64);
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            element_hash = DatumGetUInt64(FunctionCallInvoke!(locfcinfo));

            /* We don't expect hash support functions to return null */
            Assert!(!(*locfcinfo).isnull);
        }

        /* see hash_array_extended() */
        result = (result << 5).wrapping_sub(result).wrapping_add(element_hash);

        i += 1;
    }

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    ReleaseTupleDesc(tupdesc);

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, record, 0);

    PG_RETURN_UINT64!(result)
}

/* ---------------------------------------------------------------------------
 * Local helpers that stand in for C macros without a direct Rust equivalent.
 * ---------------------------------------------------------------------------
 */

/*
 * fmgr.h: PG_GETARG_HEAPTUPLEHEADER(n).  Fetches argument n as a (de-toasted)
 * HeapTupleHeader.  Written as a fn since the macro form is not yet in fmgr.rs.
 */
// TODO(pg-port): real PG_GETARG_HEAPTUPLEHEADER macro lives in fmgr.h
#[inline]
unsafe fn PG_GETARG_HEAPTUPLEHEADER(fcinfo: FunctionCallInfo, n: c_int) -> HeapTupleHeader {
    DatumGetHeapTupleHeader(PG_GETARG_DATUM!(fcinfo, n))
}

/*
 * fmgr.h: PG_RETURN_HEAPTUPLEHEADER(x).  Returns a HeapTupleHeader as a Datum.
 */
// TODO(pg-port): real PG_RETURN_HEAPTUPLEHEADER macro lives in fmgr.h
#[inline]
unsafe fn PG_RETURN_HEAPTUPLEHEADER(x: HeapTupleHeader) -> Datum {
    PG_RETURN_POINTER!(x)
}

/* The C `fail:` path of record_in ends in PG_RETURN_NULL(). */
#[inline]
unsafe fn return_null(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_NULL!(fcinfo)
}

/*
 * errsave(escontext, (...)) expansion: either soft-report into escontext or
 * ereport(ERROR).  The elog shim emits at ERROR level; per the porting
 * convention only the errmsg text survives (errcode/errdetail dropped).
 */
unsafe fn errsave_emit(escontext: *mut Node, msg: &str) {
    let _ = escontext;
    crate::utils::elog::emit_log(ERROR, msg, file!(), line!());
}
