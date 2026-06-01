/*-------------------------------------------------------------------------
 *
 * copyfromparse.c -> copyfromparse.rs
 *		Parse CSV/text/binary format for COPY FROM.
 *
 * This file contains routines to parse the text, CSV and binary input
 * formats.  The main entry point is NextCopyFrom(), which parses the
 * next input line and returns it as Datums.
 *
 * In text/CSV mode, the parsing happens in multiple stages:
 *
 * [data source] --> raw_buf --> input_buf --> line_buf --> attribute_buf
 *                1.          2.            3.           4.
 *
 * 1. CopyLoadRawBuf() reads raw data from the input file or client, and
 *    places it into 'raw_buf'.
 *
 * 2. CopyConvertBuf() calls the encoding conversion function to convert
 *    the data in 'raw_buf' from client to server encoding, placing the
 *    converted result in 'input_buf'.
 *
 * 3. CopyReadLine() parses the data in 'input_buf', one line at a time.
 *    It is responsible for finding the next newline marker, taking quote and
 *    escape characters into account according to the COPY options.  The line
 *    is copied into 'line_buf', with quotes and escape characters still
 *    intact.
 *
 * 4. CopyReadAttributesText/CSV() function takes the input line from
 *    'line_buf', and splits it into fields, unescaping the data as required.
 *    The fields are stored in 'attribute_buf', and 'raw_fields' array holds
 *    pointers to each field.
 *
 * If encoding conversion is not required, a shortcut is taken in step 2 to
 * avoid copying the data unnecessarily.  The 'input_buf' pointer is set to
 * point directly to 'raw_buf', so that CopyLoadRawBuf() loads the raw data
 * directly into 'input_buf'.  CopyConvertBuf() then merely validates that
 * the data is valid in the current encoding.
 *
 * In binary mode, the pipeline is much simpler.  Input is loaded into
 * 'raw_buf', and encoding conversion is done in the datatype-specific
 * receive functions, if required.  'input_buf' and 'line_buf' are not used,
 * but 'attribute_buf' is used as a temporary buffer to hold one attribute's
 * data when it's passed the receive function.
 *
 * 'raw_buf' is always 64 kB in size (RAW_BUF_SIZE).  'input_buf' is also
 * 64 kB (INPUT_BUF_SIZE), if encoding conversion is required.  'line_buf'
 * and 'attribute_buf' are expanded on demand, to hold the longest line
 * encountered so far.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfromparse.c
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int, c_void};
use std::mem::size_of;

use crate::prelude::*;
use crate::c::{uint16, uint32, uint64, Size};
use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::commands::copy::{
    CopyFormatOptions, CopyHeaderChoice, CopyLogVerbosityChoice, CopyOnErrorChoice,
    COPY_HEADER_MATCH, COPY_LOG_VERBOSITY_VERBOSE, COPY_ON_ERROR_STOP,
};
use crate::commands::copyapi::CopyFromState;
use crate::commands::copyfrom_internal::{
    CopyFromStateData, CopySource, EolType,
    EOL_CR, EOL_CRNL, EOL_NL, EOL_UNKNOWN,
    COPY_FILE, COPY_FRONTEND, COPY_CALLBACK,
    INPUT_BUF_SIZE, RAW_BUF_SIZE,
    INPUT_BUF_BYTES, RAW_BUF_BYTES,
};
use crate::executor::executor::ExecEvalExpr;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, enlargeStringInfo, initStringInfo, makeStringInfo,
    resetStringInfo, StringInfoData, StringInfo,
};
use crate::libpq::pqformat::{
    pq_beginmessage, pq_copymsgbytes, pq_endmessage, pq_getmsgstring,
    pq_sendint16, pq_sendbyte,
};
use crate::libpq::protocol::{
    PqMsg_CopyInResponse, PqMsg_CopyData, PqMsg_CopyDone, PqMsg_CopyFail,
    PqMsg_Flush, PqMsg_Sync,
};
use crate::libpq::libpq::{PQ_LARGE_MESSAGE_LIMIT, PQ_SMALL_MESSAGE_LIMIT};
use crate::mb::pg_wchar::{
    GetDatabaseEncoding, MAX_CONVERSION_INPUT_LENGTH,
    pg_encoding_max_length, pg_encoding_verifymbstr,
    pg_do_encoding_conversion_buf, report_invalid_encoding,
};
use crate::mb::mbutils::pg_mbcliplen;
use crate::miscadmin::{HOLD_CANCEL_INTERRUPTS, RESUME_CANCEL_INTERRUPTS};
use crate::nodes::execnodes::{ExprContext, ExprState};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{list_length, list_nth_int, List, ListCell};
use crate::port::pg_bswap::{pg_ntoh16, pg_ntoh32};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::activity::backend_progress::pgstat_progress_update_param;
use crate::utils::fmgr::{FmgrInfo, InputFunctionCallSafe, ReceiveFunctionCall};
use crate::utils::rel::Relation;
use crate::commands::progress::PROGRESS_COPY_BYTES_PROCESSED;
use crate::c::{IS_HIGHBIT_SET, MemSet};

/* NOTE: there's a copy of this in copyto.c */
static BinarySignature: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";

/* TODO(pg-port): pq_getbyte from libpq/libpq-be.h */
unsafe fn pq_getbyte() -> c_int {
    unimplemented!() // TODO(pg-port): real pq_getbyte in libpq/be-fsstubs.c
}

/* TODO(pg-port): pq_startmsgread from libpq/libpq.h */
unsafe fn pq_startmsgread() {
    unimplemented!() // TODO(pg-port): real pq_startmsgread in libpq
}

/* TODO(pg-port): pq_getmessage from libpq/libpq.h */
unsafe fn pq_getmessage(_buf: StringInfo, _maxsize: Size) -> c_int {
    unimplemented!() // TODO(pg-port): real pq_getmessage in libpq
}

/* TODO(pg-port): pq_flush from libpq/libpq.h */
unsafe fn pq_flush() -> c_int {
    unimplemented!() // TODO(pg-port): real pq_flush in libpq
}

/* TODO(pg-port): RelationGetDescr from utils/rel.h */
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    unimplemented!() // TODO(pg-port): real RelationGetDescr in utils/rel.h
}

/* TODO(pg-port): TupleDescAttr from access/tupdesc.h */
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> *mut FormData_pg_attribute {
    unimplemented!() // TODO(pg-port): real TupleDescAttr in access/tupdesc.h
}

/* TODO(pg-port): Form_pg_attribute and FormData_pg_attribute from catalog/pg_attribute.h */
pub type Form_pg_attribute = *mut FormData_pg_attribute;
#[repr(C)]
pub struct FormData_pg_attribute {
    pub attname: NameData,
    pub atttypid: Oid,
    pub attnum: AttrNumber,
    pub atttypmod: i32,
    pub attisdropped: bool,
    pub attgenerated: c_char,
}
#[repr(C)]
pub struct NameData {
    _data: [c_char; 64],
}

/* TODO(pg-port): NameStr from c.h */
#[allow(non_snake_case)]
unsafe fn NameStr(name: &NameData) -> *const c_char {
    name._data.as_ptr()
}

/* TODO(pg-port): namestrcmp from utils/builtins.h */
unsafe fn namestrcmp(_name: *const NameData, _str_: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): real namestrcmp in utils/builtins.h
}

/* TODO(pg-port): strncmp from libc */
unsafe fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int {
    extern "C" { fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int; }
    strncmp(s1, s2, n)
}

/* TODO(pg-port): repalloc from utils/palloc.h */
unsafe fn repalloc(ptr: *mut c_void, size: Size) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real repalloc in utils/palloc.h
}

/* TODO(pg-port): pg_verifymbstr from mb/pg_wchar.h */
unsafe fn pg_verifymbstr(_mbstr: *const c_char, _len: c_int, _noError: bool) -> bool {
    unimplemented!() // TODO(pg-port): real pg_verifymbstr in mb/pg_wchar.h
}

/* TODO(pg-port): CopyLimitPrintoutLength from commands/copyfrom.rs */
unsafe fn CopyLimitPrintoutLength(str_: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): real CopyLimitPrintoutLength in commands/copyfrom.rs
}

/* TODO(pg-port): memmove from libc */
#[inline]
unsafe fn memmove_(dst: *mut c_void, src: *const c_void, n: usize) {
    extern "C" { fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void; }
    memmove(dst, src, n);
}

/* TODO(pg-port): memcpy from libc */
#[inline]
unsafe fn memcpy_(dst: *mut c_void, src: *const c_void, n: usize) {
    extern "C" { fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void; }
    memcpy(dst, src, n);
}

/* TODO(pg-port): memcmp from libc */
#[inline]
unsafe fn memcmp_(s1: *const c_void, s2: *const c_void, n: usize) -> c_int {
    extern "C" { fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int; }
    memcmp(s1, s2, n)
}

/* TODO(pg-port): fread from libc */
unsafe fn fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize {
    extern "C" { fn fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize; }
    fread(ptr, size, nmemb, stream)
}

/* TODO(pg-port): ferror from libc */
unsafe fn ferror(stream: *mut c_void) -> c_int {
    extern "C" { fn ferror(stream: *mut c_void) -> c_int; }
    ferror(stream)
}

/* TODO(pg-port): isxdigit / tolower from <ctype.h> */
unsafe fn isxdigit(c: u8) -> bool {
    (c >= b'0' && c <= b'9') || (c >= b'a' && c <= b'f') || (c >= b'A' && c <= b'F')
}
unsafe fn tolower(c: u8) -> u8 {
    if c >= b'A' && c <= b'Z' { c + 32 } else { c }
}
unsafe fn isdigit_c(c: u8) -> bool {
    c >= b'0' && c <= b'9'
}

#[inline]
fn ISOCTAL(c: u8) -> bool { c >= b'0' && c <= b'7' }
#[inline]
fn OCTVALUE(c: u8) -> u8 { c - b'0' }

pub unsafe fn ReceiveCopyBegin(cstate: CopyFromState) {
    let cs = cstate as *mut CopyFromStateData;
    let mut buf: StringInfoData = core::mem::zeroed();
    let natts: c_int = list_length((*cs).attnumlist);
    let format: i16 = if (*cs).opts.binary { 1 } else { 0 };
    let mut i: c_int;

    pq_beginmessage(&mut buf, PqMsg_CopyInResponse as c_char);
    pq_sendbyte(&mut buf, format as u8); /* overall format */
    pq_sendint16(&mut buf, natts as u16);
    i = 0;
    while i < natts {
        pq_sendint16(&mut buf, format as u16); /* per-column formats */
        i += 1;
    }
    pq_endmessage(&mut buf);
    (*cs).copy_src = COPY_FRONTEND;
    (*cs).fe_msgbuf = makeStringInfo();
    /* We *must* flush here to ensure FE knows it can send. */
    pq_flush();
}

pub unsafe fn ReceiveCopyBinaryHeader(cstate: CopyFromState) {
    let mut readSig: [u8; 11] = [0u8; 11];
    let mut tmp: i32 = 0;

    /* Signature */
    if CopyReadBinaryData(cstate, readSig.as_mut_ptr() as *mut c_char, 11) != 11
        || memcmp_(
            readSig.as_ptr() as *const c_void,
            BinarySignature.as_ptr() as *const c_void,
            11,
        ) != 0
    {
        ereport!(ERROR, errmsg!("COPY file signature not recognized"));
    }
    /* Flags field */
    if !CopyGetInt32(cstate, &mut tmp) {
        ereport!(ERROR, errmsg!("invalid COPY file header (missing flags)"));
    }
    if (tmp & (1 << 16)) != 0 {
        ereport!(ERROR, errmsg!("invalid COPY file header (WITH OIDS)"));
    }
    tmp &= !(1 << 16);
    if (tmp >> 16) != 0 {
        ereport!(ERROR, errmsg!("unrecognized critical flags in COPY file header"));
    }
    /* Header extension length */
    if !CopyGetInt32(cstate, &mut tmp) || tmp < 0 {
        ereport!(ERROR, errmsg!("invalid COPY file header (missing length)"));
    }
    /* Skip extension header, if present */
    while tmp > 0 {
        tmp -= 1;
        if CopyReadBinaryData(cstate, readSig.as_mut_ptr() as *mut c_char, 1) != 1 {
            ereport!(ERROR, errmsg!("invalid COPY file header (wrong length)"));
        }
    }
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.  The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 */
unsafe fn CopyGetData(
    cstate: CopyFromState,
    databuf: *mut c_void,
    minread: c_int,
    maxread: c_int,
) -> c_int {
    let cs = cstate as *mut CopyFromStateData;
    let mut bytesread: c_int = 0;

    match (*cs).copy_src {
        COPY_FILE => {
            bytesread =
                fread(databuf, 1, maxread as usize, (*cs).copy_file as *mut c_void) as c_int;
            if ferror((*cs).copy_file as *mut c_void) != 0 {
                ereport!(ERROR, errmsg!("could not read from COPY file: {}", "I/O error"));
            }
            if bytesread == 0 {
                (*cs).raw_reached_eof = true;
            }
        }
        COPY_FRONTEND => {
            let mut maxread_mut = maxread;
            let mut databuf_mut = databuf;
            while maxread_mut > 0 && bytesread < minread && !(*cs).raw_reached_eof {
                let avail: c_int;

                while (*(*cs).fe_msgbuf).cursor >= (*(*cs).fe_msgbuf).len {
                    /* Try to receive another message */
                    let mtype: c_int;
                    let maxmsglen: Size;

                    // readmessage:
                    'readmessage: loop {
                        HOLD_CANCEL_INTERRUPTS!();
                        pq_startmsgread();
                        mtype = pq_getbyte();
                        if mtype == -1
                        /* EOF */
                        {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "unexpected EOF on client connection with an open transaction"
                                )
                            );
                        }
                        /* Validate message type and set packet size limit */
                        let maxmsglen_val: Size;
                        if mtype == PqMsg_CopyData as c_int {
                            maxmsglen_val = PQ_LARGE_MESSAGE_LIMIT;
                        } else if mtype == PqMsg_CopyDone as c_int
                            || mtype == PqMsg_CopyFail as c_int
                            || mtype == PqMsg_Flush as c_int
                            || mtype == PqMsg_Sync as c_int
                        {
                            maxmsglen_val = PQ_SMALL_MESSAGE_LIMIT as Size;
                        } else {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "unexpected message type 0x{:02X} during COPY from stdin",
                                    mtype
                                )
                            );
                            maxmsglen_val = 0; /* keep compiler quiet */
                        }
                        /* Now collect the message body */
                        if pq_getmessage((*cs).fe_msgbuf, maxmsglen_val) != 0 {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "unexpected EOF on client connection with an open transaction"
                                )
                            );
                        }
                        RESUME_CANCEL_INTERRUPTS!();
                        /* ... and process it */
                        if mtype == PqMsg_CopyData as c_int {
                            break 'readmessage;
                        } else if mtype == PqMsg_CopyDone as c_int {
                            /* COPY IN correctly terminated by frontend */
                            (*cs).raw_reached_eof = true;
                            return bytesread;
                        } else if mtype == PqMsg_CopyFail as c_int {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "COPY from stdin failed: {}",
                                    std::ffi::CStr::from_ptr(pq_getmsgstring((*cs).fe_msgbuf))
                                        .to_string_lossy()
                                )
                            );
                        } else if mtype == PqMsg_Flush as c_int
                            || mtype == PqMsg_Sync as c_int
                        {
                            /*
                             * Ignore Flush/Sync for the convenience of client
                             * libraries (such as libpq) that may send those
                             * without noticing that the command they just
                             * sent was COPY.
                             */
                            /* goto readmessage -- loop again */
                        } else {
                            Assert!(false); /* NOT REACHED */
                        }
                    }
                }
                avail = (*(*cs).fe_msgbuf).len - (*(*cs).fe_msgbuf).cursor;
                let avail = if avail > maxread_mut { maxread_mut } else { avail };
                pq_copymsgbytes((*cs).fe_msgbuf, databuf_mut, avail);
                databuf_mut = (databuf_mut as *mut u8).add(avail as usize) as *mut c_void;
                maxread_mut -= avail;
                bytesread += avail;
            }
        }
        COPY_CALLBACK => {
            bytesread = ((*cs).data_source_cb.unwrap())(databuf, minread, maxread);
        }
        _ => {}
    }

    bytesread
}

/*
 * These functions do apply some data conversion
 */

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
#[inline]
unsafe fn CopyGetInt32(cstate: CopyFromState, val: *mut i32) -> bool {
    let mut buf: u32 = 0;

    if CopyReadBinaryData(cstate, &mut buf as *mut u32 as *mut c_char, size_of::<u32>() as c_int)
        != size_of::<u32>() as c_int
    {
        *val = 0; /* suppress compiler warning */
        return false;
    }
    *val = pg_ntoh32(buf) as i32;
    true
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
#[inline]
unsafe fn CopyGetInt16(cstate: CopyFromState, val: *mut i16) -> bool {
    let mut buf: u16 = 0;

    if CopyReadBinaryData(cstate, &mut buf as *mut u16 as *mut c_char, size_of::<u16>() as c_int)
        != size_of::<u16>() as c_int
    {
        *val = 0; /* suppress compiler warning */
        return false;
    }
    *val = pg_ntoh16(buf) as i16;
    true
}

/*
 * Perform encoding conversion on data in 'raw_buf', writing the converted
 * data into 'input_buf'.
 *
 * On entry, there must be some data to convert in 'raw_buf'.
 */
unsafe fn CopyConvertBuf(cstate: CopyFromState) {
    let cs = cstate as *mut CopyFromStateData;

    /*
     * If the file and server encoding are the same, no encoding conversion is
     * required.  However, we still need to verify that the input is valid for
     * the encoding.
     */
    if !(*cs).need_transcoding {
        /*
         * When conversion is not required, input_buf and raw_buf are the
         * same.  raw_buf_len is the total number of bytes in the buffer, and
         * input_buf_len tracks how many of those bytes have already been
         * verified.
         */
        let preverifiedlen: c_int = (*cs).input_buf_len;
        let unverifiedlen: c_int = (*cs).raw_buf_len - (*cs).input_buf_len;
        let nverified: c_int;

        if unverifiedlen == 0 {
            /*
             * If no more raw data is coming, report the EOF to the caller.
             */
            if (*cs).raw_reached_eof {
                (*cs).input_reached_eof = true;
            }
            return;
        }

        /*
         * Verify the new data, including any residual unverified bytes from
         * previous round.
         */
        nverified = pg_encoding_verifymbstr(
            (*cs).file_encoding,
            (*cs).raw_buf.add(preverifiedlen as usize),
            unverifiedlen,
        );
        if nverified == 0 {
            /*
             * Could not verify anything.
             *
             * If there is no more raw input data coming, it means that there
             * was an incomplete multi-byte sequence at the end.  Also, if
             * there's "enough" input left, we should be able to verify at
             * least one character, and a failure to do so means that we've
             * hit an invalid byte sequence.
             */
            if (*cs).raw_reached_eof
                || unverifiedlen >= pg_encoding_max_length((*cs).file_encoding)
            {
                (*cs).input_reached_error = true;
            }
            return;
        }
        (*cs).input_buf_len += nverified;
    } else {
        /*
         * Encoding conversion is needed.
         */
        let nbytes: c_int;
        let src: *mut u8;
        let srclen: c_int;
        let dst: *mut u8;
        let dstlen: c_int;
        let convertedlen: c_int;

        if RAW_BUF_BYTES(cs) == 0 {
            /*
             * If no more raw data is coming, report the EOF to the caller.
             */
            if (*cs).raw_reached_eof {
                (*cs).input_reached_eof = true;
            }
            return;
        }

        /*
         * First, copy down any unprocessed data.
         */
        let nbytes_val: c_int = INPUT_BUF_BYTES(cs);
        if nbytes_val > 0 && (*cs).input_buf_index > 0 {
            memmove_(
                (*cs).input_buf as *mut c_void,
                (*cs).input_buf.add((*cs).input_buf_index as usize) as *const c_void,
                nbytes_val as usize,
            );
        }
        (*cs).input_buf_index = 0;
        (*cs).input_buf_len = nbytes_val;
        *(*cs).input_buf.add(nbytes_val as usize) = b'\0' as c_char;

        src = (*cs).raw_buf.add((*cs).raw_buf_index as usize) as *mut u8;
        srclen = (*cs).raw_buf_len - (*cs).raw_buf_index;
        dst = (*cs).input_buf.add((*cs).input_buf_len as usize) as *mut u8;
        dstlen = INPUT_BUF_SIZE - (*cs).input_buf_len + 1;

        /*
         * Do the conversion.  This might stop short, if there is an invalid
         * byte sequence in the input.  We'll convert as much as we can in
         * that case.
         *
         * Note: Even if we hit an invalid byte sequence, we don't report the
         * error until all the valid bytes have been consumed.  The input
         * might contain an end-of-input marker (\.), and we don't want to
         * report an error if the invalid byte sequence is after the
         * end-of-input marker.  We might unnecessarily convert some data
         * after the end-of-input marker as long as it's valid for the
         * encoding, but that's harmless.
         */
        convertedlen = pg_do_encoding_conversion_buf(
            (*cs).conversion_proc,
            (*cs).file_encoding,
            GetDatabaseEncoding(),
            src,
            srclen,
            dst,
            dstlen,
            true,
        );
        if convertedlen == 0 {
            /*
             * Could not convert anything.  If there is no more raw input data
             * coming, it means that there was an incomplete multi-byte
             * sequence at the end.  Also, if there is plenty of input left,
             * we should be able to convert at least one character, so a
             * failure to do so must mean that we've hit a byte sequence
             * that's invalid.
             */
            if (*cs).raw_reached_eof || srclen >= MAX_CONVERSION_INPUT_LENGTH {
                (*cs).input_reached_error = true;
            }
            return;
        }
        (*cs).raw_buf_index += convertedlen;
        /* strlen of dst */
        let mut dst_end = dst;
        while *dst_end != 0 {
            dst_end = dst_end.add(1);
        }
        (*cs).input_buf_len += (dst_end as usize - dst as usize) as c_int;
    }
}

/*
 * Report an encoding or conversion error.
 */
unsafe fn CopyConversionError(cstate: CopyFromState) {
    let cs = cstate as *mut CopyFromStateData;

    Assert!((*cs).raw_buf_len > 0);
    Assert!((*cs).input_reached_error);

    if !(*cs).need_transcoding {
        /*
         * Everything up to input_buf_len was successfully verified, and
         * input_buf_len points to the invalid or incomplete character.
         */
        report_invalid_encoding(
            (*cs).file_encoding,
            (*cs).raw_buf.add((*cs).input_buf_len as usize),
            (*cs).raw_buf_len - (*cs).input_buf_len,
        );
    } else {
        /*
         * raw_buf_index points to the invalid or untranslatable character. We
         * let the conversion routine report the error, because it can provide
         * a more specific error message than we could here.  An earlier call
         * to the conversion routine in CopyConvertBuf() detected that there
         * is an error, now we call the conversion routine again with
         * noError=false, to have it throw the error.
         */
        let src: *mut u8 = (*cs).raw_buf.add((*cs).raw_buf_index as usize) as *mut u8;
        let srclen: c_int = (*cs).raw_buf_len - (*cs).raw_buf_index;
        let dst: *mut u8 = (*cs).input_buf.add((*cs).input_buf_len as usize) as *mut u8;
        let dstlen: c_int = INPUT_BUF_SIZE - (*cs).input_buf_len + 1;

        let _ = pg_do_encoding_conversion_buf(
            (*cs).conversion_proc,
            (*cs).file_encoding,
            GetDatabaseEncoding(),
            src,
            srclen,
            dst,
            dstlen,
            false,
        );

        /*
         * The conversion routine should have reported an error, so this
         * should not be reached.
         */
        elog!(ERROR, "encoding conversion failed without error");
    }
}

/*
 * Load more data from data source to raw_buf.
 *
 * If RAW_BUF_BYTES(cstate) > 0, the unprocessed bytes are moved to the
 * beginning of the buffer, and we load new data after that.
 */
unsafe fn CopyLoadRawBuf(cstate: CopyFromState) {
    let cs = cstate as *mut CopyFromStateData;
    let nbytes: c_int;
    let inbytes: c_int;

    /*
     * In text mode, if encoding conversion is not required, raw_buf and
     * input_buf point to the same buffer.  Their len/index better agree, too.
     */
    if (*cs).raw_buf == (*cs).input_buf {
        Assert!(!(*cs).need_transcoding);
        Assert!((*cs).raw_buf_index == (*cs).input_buf_index);
        Assert!((*cs).input_buf_len <= (*cs).raw_buf_len);
    }

    /*
     * Copy down the unprocessed data if any.
     */
    let nbytes_val: c_int = RAW_BUF_BYTES(cs);
    if nbytes_val > 0 && (*cs).raw_buf_index > 0 {
        memmove_(
            (*cs).raw_buf as *mut c_void,
            (*cs).raw_buf.add((*cs).raw_buf_index as usize) as *const c_void,
            nbytes_val as usize,
        );
    }
    (*cs).raw_buf_len -= (*cs).raw_buf_index;
    (*cs).raw_buf_index = 0;

    /*
     * If raw_buf and input_buf are in fact the same buffer, adjust the
     * input_buf variables, too.
     */
    if (*cs).raw_buf == (*cs).input_buf {
        (*cs).input_buf_len -= (*cs).input_buf_index;
        (*cs).input_buf_index = 0;
    }

    /* Load more data */
    let nbytes_after = RAW_BUF_BYTES(cs);
    let inbytes_val = CopyGetData(
        cstate,
        (*cs).raw_buf.add((*cs).raw_buf_len as usize) as *mut c_void,
        1,
        RAW_BUF_SIZE - (*cs).raw_buf_len,
    );
    let nbytes_total = nbytes_after + inbytes_val;
    *(*cs).raw_buf.add(nbytes_total as usize) = b'\0' as c_char;
    (*cs).raw_buf_len = nbytes_total;

    (*cs).bytes_processed += inbytes_val as uint64;
    pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, (*cs).bytes_processed as i64);

    if inbytes_val == 0 {
        (*cs).raw_reached_eof = true;
    }
}

/*
 * CopyLoadInputBuf loads some more data into input_buf
 *
 * On return, at least one more input character is loaded into
 * input_buf, or input_reached_eof is set.
 *
 * If INPUT_BUF_BYTES(cstate) > 0, the unprocessed bytes are moved to the start
 * of the buffer and then we load more data after that.
 */
unsafe fn CopyLoadInputBuf(cstate: CopyFromState) {
    let cs = cstate as *mut CopyFromStateData;
    let nbytes: c_int = INPUT_BUF_BYTES(cs);

    /*
     * The caller has updated input_buf_index to indicate how much of the
     * input has been consumed and isn't needed anymore.  If input_buf is the
     * same physical area as raw_buf, update raw_buf_index accordingly.
     */
    if (*cs).raw_buf == (*cs).input_buf {
        Assert!(!(*cs).need_transcoding);
        Assert!((*cs).input_buf_index >= (*cs).raw_buf_index);
        (*cs).raw_buf_index = (*cs).input_buf_index;
    }

    loop {
        /* If we now have some unconverted data, try to convert it */
        CopyConvertBuf(cstate);

        /* If we now have some more input bytes ready, return them */
        if INPUT_BUF_BYTES(cs) > nbytes {
            return;
        }

        /*
         * If we reached an invalid byte sequence, or we're at an incomplete
         * multi-byte character but there is no more raw input data, report
         * conversion error.
         */
        if (*cs).input_reached_error {
            CopyConversionError(cstate);
        }

        /* no more input, and everything has been converted */
        if (*cs).input_reached_eof {
            break;
        }

        /* Try to load more raw data */
        Assert!(!(*cs).raw_reached_eof);
        CopyLoadRawBuf(cstate);
    }
}

/*
 * CopyReadBinaryData
 *
 * Reads up to 'nbytes' bytes from cstate->copy_file via cstate->raw_buf
 * and writes them to 'dest'.  Returns the number of bytes read (which
 * would be less than 'nbytes' only if we reach EOF).
 */
unsafe fn CopyReadBinaryData(
    cstate: CopyFromState,
    dest: *mut c_char,
    nbytes: c_int,
) -> c_int {
    let cs = cstate as *mut CopyFromStateData;
    let mut copied_bytes: c_int = 0;

    if RAW_BUF_BYTES(cs) >= nbytes {
        /* Enough bytes are present in the buffer. */
        memcpy_(
            dest as *mut c_void,
            (*cs).raw_buf.add((*cs).raw_buf_index as usize) as *const c_void,
            nbytes as usize,
        );
        (*cs).raw_buf_index += nbytes;
        copied_bytes = nbytes;
    } else {
        /*
         * Not enough bytes in the buffer, so must read from the file.  Need
         * to loop since 'nbytes' could be larger than the buffer size.
         */
        let mut dest_ptr = dest;
        loop {
            let copy_bytes: c_int;

            /* Load more data if buffer is empty. */
            if RAW_BUF_BYTES(cs) == 0 {
                CopyLoadRawBuf(cstate);
                if (*cs).raw_reached_eof {
                    break; /* EOF */
                }
            }

            /* Transfer some bytes. */
            copy_bytes = {
                let a = nbytes - copied_bytes;
                let b = RAW_BUF_BYTES(cs);
                if a < b { a } else { b }
            };
            memcpy_(
                dest_ptr as *mut c_void,
                (*cs).raw_buf.add((*cs).raw_buf_index as usize) as *const c_void,
                copy_bytes as usize,
            );
            (*cs).raw_buf_index += copy_bytes;
            dest_ptr = dest_ptr.add(copy_bytes as usize);
            copied_bytes += copy_bytes;

            if copied_bytes >= nbytes {
                break;
            }
        }
    }

    copied_bytes
}

/*
 * This function is exposed for use by extensions that read raw fields in the
 * next line. See NextCopyFromRawFieldsInternal() for details.
 */
pub unsafe fn NextCopyFromRawFields(
    cstate: CopyFromState,
    fields: *mut *mut *mut c_char,
    nfields: *mut c_int,
) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    NextCopyFromRawFieldsInternal(cstate, fields, nfields, (*cs).opts.csv_mode)
}

/*
 * Workhorse for NextCopyFromRawFields().
 *
 * Read raw fields in the next line for COPY FROM in text or csv mode. Return
 * false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 *
 * We use pg_attribute_always_inline to reduce function call overhead
 * and to help compilers to optimize away the 'is_csv' condition when called
 * by internal functions such as CopyFromTextLikeOneRow().
 */
#[inline(always)]
unsafe fn NextCopyFromRawFieldsInternal(
    cstate: CopyFromState,
    fields: *mut *mut *mut c_char,
    nfields: *mut c_int,
    is_csv: bool,
) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    let mut fldct: c_int;
    let mut done: bool;

    /* only available for text or csv input */
    Assert!(!(*cs).opts.binary);

    /* on input check that the header line is correct if needed */
    if (*cs).cur_lineno == 0 && (*cs).opts.header_line != 0 /* COPY_HEADER_FALSE */ {
        let mut cur: *mut ListCell;
        let tupDesc: TupleDesc;

        tupDesc = RelationGetDescr((*cs).rel);

        (*cs).cur_lineno += 1;
        done = CopyReadLine(cstate, is_csv);

        if (*cs).opts.header_line == COPY_HEADER_MATCH {
            let mut fldnum: c_int;

            if is_csv {
                fldct = CopyReadAttributesCSV(cstate);
            } else {
                fldct = CopyReadAttributesText(cstate);
            }

            if fldct != list_length((*cs).attnumlist) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "wrong number of fields in header line: got {}, expected {}",
                        fldct,
                        list_length((*cs).attnumlist)
                    )
                );
            }

            fldnum = 0;
            // foreach over attnumlist
            {
                let list = (*cs).attnumlist;
                if !list.is_null() {
                    let n = list_length(list);
                    for idx in 0..n {
                        let attnum: c_int = list_nth_int(list, idx);
                        let colName: *mut c_char;
                        let attr: Form_pg_attribute =
                            TupleDescAttr(tupDesc, attnum - 1);

                        Assert!(fldnum < (*cs).max_fields);

                        colName = *(*cs).raw_fields.add(fldnum as usize);
                        fldnum += 1;
                        if colName.is_null() {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "column name mismatch in header line field {}: got null value (\"{}\"), expected \"{}\"",
                                    fldnum,
                                    std::ffi::CStr::from_ptr((*cs).opts.null_print).to_string_lossy(),
                                    std::ffi::CStr::from_ptr(NameStr(&(*attr).attname)).to_string_lossy()
                                )
                            );
                        }

                        if namestrcmp(&(*attr).attname, colName) != 0 {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "column name mismatch in header line field {}: got \"{}\", expected \"{}\"",
                                    fldnum,
                                    std::ffi::CStr::from_ptr(colName).to_string_lossy(),
                                    std::ffi::CStr::from_ptr(NameStr(&(*attr).attname)).to_string_lossy()
                                )
                            );
                        }
                    }
                }
            }
        }

        if done {
            return false;
        }
    }

    (*cs).cur_lineno += 1;

    /* Actually read the line into memory here */
    done = CopyReadLine(cstate, is_csv);

    /*
     * EOF at start of line means we're done.  If we see EOF after some
     * characters, we act as though it was newline followed by EOF, ie,
     * process the line and then exit loop on next iteration.
     */
    if done && (*(*cs).line_buf.data as u8) == 0 && (*cs).line_buf.len == 0 {
        return false;
    }

    /* Parse the line into de-escaped field values */
    if is_csv {
        fldct = CopyReadAttributesCSV(cstate);
    } else {
        fldct = CopyReadAttributesText(cstate);
    }

    *fields = (*cs).raw_fields;
    *nfields = fldct;
    true
}

/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each column that is
 * either not read from the file or is using the DEFAULT option of COPY FROM.
 * It can be NULL when no default values are used, i.e. when all columns are
 * read from the file, and DEFAULT option is unset.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 */
pub unsafe fn NextCopyFrom(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    let tupDesc: TupleDesc;
    let num_phys_attrs: AttrNumber;
    let num_defaults: AttrNumber = (*cs).num_defaults;
    let mut i: c_int;
    let defmap: *mut c_int = (*cs).defmap;
    let defexprs: *mut *mut ExprState = (*cs).defexprs;

    tupDesc = RelationGetDescr((*cs).rel);
    num_phys_attrs = (*tupDesc).natts;

    /* Initialize all values for row to NULL */
    MemSet(
        values as *mut c_void,
        0,
        num_phys_attrs as Size * size_of::<Datum>(),
    );
    MemSet(
        nulls as *mut c_void,
        1, /* true */
        num_phys_attrs as Size * size_of::<bool>(),
    );
    MemSet(
        (*cs).defaults as *mut c_void,
        0,
        num_phys_attrs as Size * size_of::<bool>(),
    );

    /* Get one row from source */
    if !((*(*cs).routine).CopyFromOneRow.unwrap())(cstate, econtext, values, nulls) {
        return false;
    }

    /*
     * Now compute and insert any defaults available for the columns not
     * provided by the input data.  Anything not processed here or above will
     * remain NULL.
     */
    i = 0;
    while i < num_defaults as c_int {
        /*
         * The caller must supply econtext and have switched into the
         * per-tuple memory context in it.
         */
        Assert!(!econtext.is_null());
        Assert!(CurrentMemoryContext == (*econtext).ecxt_per_tuple_memory as MemoryContext);

        let m: usize = *defmap.add(i as usize) as usize;
        *values.add(m) = ExecEvalExpr(
            *defexprs.add(m),
            econtext,
            nulls.add(m),
        );
        i += 1;
    }

    true
}

/* Implementation of the per-row callback for text format */
pub unsafe fn CopyFromTextOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    CopyFromTextLikeOneRow(cstate, econtext, values, nulls, false)
}

/* Implementation of the per-row callback for CSV format */
pub unsafe fn CopyFromCSVOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    CopyFromTextLikeOneRow(cstate, econtext, values, nulls, true)
}

/*
 * Workhorse for CopyFromTextOneRow() and CopyFromCSVOneRow().
 *
 * We use pg_attribute_always_inline to reduce function call overhead
 * and to help compilers to optimize away the 'is_csv' condition.
 */
#[inline(always)]
unsafe fn CopyFromTextLikeOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
    is_csv: bool,
) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    let tupDesc: TupleDesc;
    let attr_count: AttrNumber;
    let in_functions: *mut FmgrInfo = (*cs).in_functions;
    let typioparams: *mut Oid = (*cs).typioparams;
    let defexprs: *mut *mut ExprState = (*cs).defexprs;
    let mut field_strings: *mut *mut c_char = core::ptr::null_mut();
    let mut fldct: c_int = 0;
    let mut fieldno: c_int;
    let mut string: *mut c_char;

    tupDesc = RelationGetDescr((*cs).rel);
    attr_count = list_length((*cs).attnumlist) as AttrNumber;

    /* read raw fields in the next line */
    if !NextCopyFromRawFieldsInternal(cstate, &mut field_strings, &mut fldct, is_csv) {
        return false;
    }

    /* check for overflowing fields */
    if attr_count > 0 && fldct > attr_count as c_int {
        ereport!(ERROR, errmsg!("extra data after last expected column"));
    }

    fieldno = 0;

    /* Loop to read the user attributes on the line. */
    {
        let list = (*cs).attnumlist;
        if !list.is_null() {
            let n = list_length(list);
            for idx in 0..n {
                let attnum: c_int = list_nth_int(list, idx);
                let m: c_int = attnum - 1;
                let att: Form_pg_attribute = TupleDescAttr(tupDesc, m);

                if fieldno >= fldct {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "missing data for column \"{}\"",
                            std::ffi::CStr::from_ptr(NameStr(&(*att).attname))
                                .to_string_lossy()
                        )
                    );
                }
                string = *field_strings.add(fieldno as usize);
                fieldno += 1;

                if !(*cs).convert_select_flags.is_null()
                    && !*(*cs).convert_select_flags.add(m as usize)
                {
                    /* ignore input field, leaving column as NULL */
                    continue;
                }

                if is_csv {
                    if string.is_null() && (*cs).opts.force_notnull_flags[m as usize] {
                        /*
                         * FORCE_NOT_NULL option is set and column is NULL - convert
                         * it to the NULL string.
                         */
                        string = (*cs).opts.null_print;
                    } else if !string.is_null()
                        && (*cs).opts.force_null_flags[m as usize]
                        && strncmp(string, (*cs).opts.null_print, (*cs).opts.null_print_len as usize) == 0
                        && {
                            // also check exact length
                            let mut len = 0usize;
                            let mut p = string;
                            while *p != 0 { p = p.add(1); len += 1; }
                            len == (*cs).opts.null_print_len as usize
                        }
                    {
                        /*
                         * FORCE_NULL option is set and column matches the NULL
                         * string. It must have been quoted, or otherwise the string
                         * would already have been set to NULL. Convert it to NULL as
                         * specified.
                         */
                        string = core::ptr::null_mut();
                    }
                }

                (*cs).cur_attname = NameStr(&(*att).attname);
                (*cs).cur_attval = string;

                if !string.is_null() {
                    *nulls.add(m as usize) = false;
                }

                if *(*cs).defaults.add(m as usize) {
                    /* We must have switched into the per-tuple memory context */
                    Assert!(!econtext.is_null());
                    Assert!(CurrentMemoryContext == (*econtext).ecxt_per_tuple_memory as MemoryContext);

                    *values.add(m as usize) = ExecEvalExpr(
                        *defexprs.add(m as usize),
                        econtext,
                        nulls.add(m as usize),
                    );
                }
                /*
                 * If ON_ERROR is specified with IGNORE, skip rows with soft errors
                 */
                else if !InputFunctionCallSafe(
                    in_functions.add(m as usize),
                    string,
                    *typioparams.add(m as usize),
                    (*att).atttypmod,
                    (*cs).escontext as *mut Node,
                    values.add(m as usize),
                ) {
                    Assert!((*cs).opts.on_error != COPY_ON_ERROR_STOP);

                    (*cs).num_errors += 1;

                    if (*cs).opts.log_verbosity == COPY_LOG_VERBOSITY_VERBOSE {
                        /*
                         * Since we emit line number and column info in the below
                         * notice message, we suppress error context information other
                         * than the relation name.
                         */
                        Assert!(!(*cs).relname_only);
                        (*cs).relname_only = true;

                        if !(*cs).cur_attval.is_null() {
                            let attval: *mut c_char = CopyLimitPrintoutLength((*cs).cur_attval);
                            ereport!(
                                NOTICE,
                                errmsg!(
                                    "skipping row due to data type incompatibility at line {} for column \"{}\": \"{}\"",
                                    (*cs).cur_lineno,
                                    std::ffi::CStr::from_ptr((*cs).cur_attname).to_string_lossy(),
                                    std::ffi::CStr::from_ptr(attval).to_string_lossy()
                                )
                            );
                            pfree(attval as *mut c_void);
                        } else {
                            ereport!(
                                NOTICE,
                                errmsg!(
                                    "skipping row due to data type incompatibility at line {} for column \"{}\": null input",
                                    (*cs).cur_lineno,
                                    std::ffi::CStr::from_ptr((*cs).cur_attname).to_string_lossy()
                                )
                            );
                        }

                        /* reset relname_only */
                        (*cs).relname_only = false;
                    }

                    return true;
                }

                (*cs).cur_attname = core::ptr::null();
                (*cs).cur_attval = core::ptr::null();
            }
        }
    }

    Assert!(fieldno == attr_count as c_int);

    true
}

/* Implementation of the per-row callback for binary format */
pub unsafe fn CopyFromBinaryOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    let tupDesc: TupleDesc;
    let attr_count: AttrNumber;
    let in_functions: *mut FmgrInfo = (*cs).in_functions;
    let typioparams: *mut Oid = (*cs).typioparams;
    let mut fld_count: i16 = 0;

    tupDesc = RelationGetDescr((*cs).rel);
    attr_count = list_length((*cs).attnumlist) as AttrNumber;

    (*cs).cur_lineno += 1;

    if !CopyGetInt16(cstate, &mut fld_count) {
        /* EOF detected (end of file, or protocol-level EOF) */
        return false;
    }

    if fld_count == -1 {
        /*
         * Received EOF marker.  Wait for the protocol-level EOF, and complain
         * if it doesn't come immediately.  In COPY FROM STDIN, this ensures
         * that we correctly handle CopyFail, if client chooses to send that
         * now.  When copying from file, we could ignore the rest of the file
         * like in text mode, but we choose to be consistent with the COPY
         * FROM STDIN case.
         */
        let mut dummy: u8 = 0;

        if CopyReadBinaryData(cstate, &mut dummy as *mut u8 as *mut c_char, 1) > 0 {
            ereport!(ERROR, errmsg!("received copy data after EOF marker"));
        }
        return false;
    }

    if fld_count != attr_count as i16 {
        ereport!(
            ERROR,
            errmsg!(
                "row field count is {}, expected {}",
                fld_count as c_int,
                attr_count as c_int
            )
        );
    }

    {
        let list = (*cs).attnumlist;
        if !list.is_null() {
            let n = list_length(list);
            for idx in 0..n {
                let attnum: c_int = list_nth_int(list, idx);
                let m: c_int = attnum - 1;
                let att: Form_pg_attribute = TupleDescAttr(tupDesc, m);

                (*cs).cur_attname = NameStr(&(*att).attname);
                *values.add(m as usize) = CopyReadBinaryAttribute(
                    cstate,
                    in_functions.add(m as usize),
                    *typioparams.add(m as usize),
                    (*att).atttypmod,
                    nulls.add(m as usize),
                );
                (*cs).cur_attname = core::ptr::null();
            }
        }
    }

    true
}

/*
 * Read the next input line and stash it in line_buf.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.  The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
unsafe fn CopyReadLine(cstate: CopyFromState, is_csv: bool) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    let result: bool;

    resetStringInfo(&mut (*cs).line_buf);
    (*cs).line_buf_valid = false;

    /* Parse data and transfer into line_buf */
    result = CopyReadLineText(cstate, is_csv);

    if result {
        /*
         * Reached EOF.  In protocol version 3, we should ignore anything
         * after \. up to the protocol end of copy data.  (XXX maybe better
         * not to treat \. as special?)
         */
        if (*cs).copy_src == COPY_FRONTEND {
            let mut inbytes: c_int;

            loop {
                inbytes = CopyGetData(
                    cstate,
                    (*cs).input_buf as *mut c_void,
                    1,
                    INPUT_BUF_SIZE,
                );
                if inbytes <= 0 { break; }
            }
            (*cs).input_buf_index = 0;
            (*cs).input_buf_len = 0;
            (*cs).raw_buf_index = 0;
            (*cs).raw_buf_len = 0;
        }
    } else {
        /*
         * If we didn't hit EOF, then we must have transferred the EOL marker
         * to line_buf along with the data.  Get rid of it.
         */
        match (*cs).eol_type {
            EOL_NL => {
                Assert!((*cs).line_buf.len >= 1);
                Assert!(
                    *(*cs).line_buf.data.add((*cs).line_buf.len as usize - 1) == b'\n' as c_char
                );
                (*cs).line_buf.len -= 1;
                *(*cs).line_buf.data.add((*cs).line_buf.len as usize) = b'\0' as c_char;
            }
            EOL_CR => {
                Assert!((*cs).line_buf.len >= 1);
                Assert!(
                    *(*cs).line_buf.data.add((*cs).line_buf.len as usize - 1) == b'\r' as c_char
                );
                (*cs).line_buf.len -= 1;
                *(*cs).line_buf.data.add((*cs).line_buf.len as usize) = b'\0' as c_char;
            }
            EOL_CRNL => {
                Assert!((*cs).line_buf.len >= 2);
                Assert!(
                    *(*cs).line_buf.data.add((*cs).line_buf.len as usize - 2) == b'\r' as c_char
                );
                Assert!(
                    *(*cs).line_buf.data.add((*cs).line_buf.len as usize - 1) == b'\n' as c_char
                );
                (*cs).line_buf.len -= 2;
                *(*cs).line_buf.data.add((*cs).line_buf.len as usize) = b'\0' as c_char;
            }
            EOL_UNKNOWN => {
                /* shouldn't get here */
                Assert!(false);
            }
            _ => {}
        }
    }

    /* Now it's safe to use the buffer in error messages */
    (*cs).line_buf_valid = true;

    result
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
unsafe fn CopyReadLineText(cstate: CopyFromState, is_csv: bool) -> bool {
    let cs = cstate as *mut CopyFromStateData;
    let copy_input_buf: *mut c_char;
    let mut input_buf_ptr: c_int;
    let mut copy_buf_len: c_int;
    let mut need_data: bool = false;
    let mut hit_eof: bool = false;
    let mut result: bool = false;

    /* CSV variables */
    let mut in_quote: bool = false;
    let mut last_was_esc: bool = false;
    let mut quotec: u8 = b'\0';
    let mut escapec: u8 = b'\0';

    if is_csv {
        quotec = *(*cs).opts.quote as u8;
        escapec = *(*cs).opts.escape as u8;
        /* ignore special escape processing if it's the same as quotec */
        if quotec == escapec {
            escapec = b'\0';
        }
    }

    /*
     * The objective of this loop is to transfer the entire next input line
     * into line_buf.  Hence, we only care for detecting newlines (\r and/or
     * \n) and the end-of-copy marker (\.).
     *
     * In CSV mode, \r and \n inside a quoted field are just part of the data
     * value and are put in line_buf.  We keep just enough state to know if we
     * are currently in a quoted field or not.
     *
     * The input has already been converted to the database encoding.  All
     * supported server encodings have the property that all bytes in a
     * multi-byte sequence have the high bit set, so a multibyte character
     * cannot contain any newline or escape characters embedded in the
     * multibyte sequence.  Therefore, we can process the input byte-by-byte,
     * regardless of the encoding.
     *
     * For speed, we try to move data from input_buf to line_buf in chunks
     * rather than one character at a time.  input_buf_ptr points to the next
     * character to examine; any characters from input_buf_index to
     * input_buf_ptr have been determined to be part of the line, but not yet
     * transferred to line_buf.
     *
     * For a little extra speed within the loop, we copy input_buf and
     * input_buf_len into local variables.
     */
    copy_input_buf = (*cs).input_buf;
    input_buf_ptr = (*cs).input_buf_index;
    copy_buf_len = (*cs).input_buf_len;

    'outer: loop {
        let prev_raw_ptr: c_int;
        let c: u8;

        /*
         * Load more data if needed.
         *
         * TODO: We could just force four bytes of read-ahead and avoid the
         * many calls to IF_NEED_REFILL_AND_NOT_EOF_CONTINUE().  That was
         * unsafe with the old v2 COPY protocol, but we don't support that
         * anymore.
         */
        if input_buf_ptr >= copy_buf_len || need_data {
            /* REFILL_LINEBUF */
            if input_buf_ptr > (*cs).input_buf_index {
                appendBinaryStringInfo(
                    &mut (*cs).line_buf,
                    (*cs).input_buf.add((*cs).input_buf_index as usize) as *const c_void,
                    input_buf_ptr - (*cs).input_buf_index,
                );
                (*cs).input_buf_index = input_buf_ptr;
            }

            CopyLoadInputBuf(cstate);
            /* update our local variables */
            hit_eof = (*cs).input_reached_eof;
            input_buf_ptr = (*cs).input_buf_index;
            copy_buf_len = (*cs).input_buf_len;

            /*
             * If we are completely out of data, break out of the loop,
             * reporting EOF.
             */
            if INPUT_BUF_BYTES(cs) <= 0 {
                result = true;
                break 'outer;
            }
            need_data = false;
        }

        /* OK to fetch a character */
        prev_raw_ptr = input_buf_ptr;
        c = *copy_input_buf.add(input_buf_ptr as usize) as u8;
        input_buf_ptr += 1;

        if is_csv {
            /*
             * If character is '\r', we may need to look ahead below.  Force
             * fetch of the next character if we don't already have it.  We
             * need to do this before changing CSV state, in case '\r' is also
             * the quote or escape character.
             */
            if c == b'\r' {
                /* IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0) */
                if input_buf_ptr + 0 >= copy_buf_len && !hit_eof {
                    input_buf_ptr = prev_raw_ptr; /* undo fetch */
                    need_data = true;
                    continue 'outer;
                }
            }

            /*
             * Dealing with quotes and escapes here is mildly tricky. If the
             * quote char is also the escape char, there's no problem - we
             * just use the char as a toggle. If they are different, we need
             * to ensure that we only take account of an escape inside a
             * quoted field and immediately preceding a quote char, and not
             * the second in an escape-escape sequence.
             */
            if in_quote && c == escapec {
                last_was_esc = !last_was_esc;
            }
            if c == quotec && !last_was_esc {
                in_quote = !in_quote;
            }
            if c != escapec {
                last_was_esc = false;
            }

            /*
             * Updating the line count for embedded CR and/or LF chars is
             * necessarily a little fragile - this test is probably about the
             * best we can do.  (XXX it's arguable whether we should do this
             * at all --- is cur_lineno a physical or logical count?)
             */
            if in_quote
                && c == (if (*cs).eol_type == EOL_NL { b'\n' } else { b'\r' })
            {
                (*cs).cur_lineno += 1;
            }
        }

        /* Process \r */
        if c == b'\r' && (!is_csv || !in_quote) {
            /* Check for \r\n on first line, _and_ handle \r\n. */
            if (*cs).eol_type == EOL_UNKNOWN || (*cs).eol_type == EOL_CRNL {
                /*
                 * If need more data, go back to loop top to load it.
                 *
                 * Note that if we are at EOF, c will wind up as '\0' because
                 * of the guaranteed pad of input_buf.
                 */
                /* IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0) */
                if input_buf_ptr + 0 >= copy_buf_len && !hit_eof {
                    input_buf_ptr = prev_raw_ptr; /* undo fetch */
                    need_data = true;
                    continue 'outer;
                }

                /* get next char */
                let c2 = *copy_input_buf.add(input_buf_ptr as usize) as u8;

                if c2 == b'\n' {
                    input_buf_ptr += 1; /* eat newline */
                    (*cs).eol_type = EOL_CRNL; /* in case not set yet */
                } else {
                    /* found \r, but no \n */
                    if (*cs).eol_type == EOL_CRNL {
                        if !is_csv {
                            ereport!(ERROR, errmsg!("literal carriage return found in data"));
                        } else {
                            ereport!(ERROR, errmsg!("unquoted carriage return found in data"));
                        }
                    }

                    /*
                     * if we got here, it is the first line and we didn't find
                     * \n, so don't consume the peeked character
                     */
                    (*cs).eol_type = EOL_CR;
                }
            } else if (*cs).eol_type == EOL_NL {
                if !is_csv {
                    ereport!(ERROR, errmsg!("literal carriage return found in data"));
                } else {
                    ereport!(ERROR, errmsg!("unquoted carriage return found in data"));
                }
            }
            /* If reach here, we have found the line terminator */
            break 'outer;
        }

        /* Process \n */
        if c == b'\n' && (!is_csv || !in_quote) {
            if (*cs).eol_type == EOL_CR || (*cs).eol_type == EOL_CRNL {
                if !is_csv {
                    ereport!(ERROR, errmsg!("literal newline found in data"));
                } else {
                    ereport!(ERROR, errmsg!("unquoted newline found in data"));
                }
            }
            (*cs).eol_type = EOL_NL; /* in case not set yet */
            /* If reach here, we have found the line terminator */
            break 'outer;
        }

        /*
         * Process backslash, except in CSV mode where backslash is a normal
         * character.
         */
        if c == b'\\' && !is_csv {
            let c2: u8;

            /* IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0) */
            if input_buf_ptr + 0 >= copy_buf_len && !hit_eof {
                input_buf_ptr = prev_raw_ptr; /* undo fetch */
                need_data = true;
                continue 'outer;
            }
            /* IF_NEED_REFILL_AND_EOF_BREAK(0) */
            if input_buf_ptr + 0 >= copy_buf_len && hit_eof {
                /* backslash just before EOF, treat as data char */
                result = true;
                break 'outer;
            }

            /* -----
             * get next character
             * Note: we do not change c so if it isn't \., we can fall
             * through and continue processing.
             * -----
             */
            c2 = *copy_input_buf.add(input_buf_ptr as usize) as u8;

            if c2 == b'.' {
                input_buf_ptr += 1; /* consume the '.' */
                if (*cs).eol_type == EOL_CRNL {
                    /* Get the next character */
                    /* IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0) */
                    if input_buf_ptr + 0 >= copy_buf_len && !hit_eof {
                        input_buf_ptr = prev_raw_ptr; /* undo fetch */
                        need_data = true;
                        continue 'outer;
                    }
                    /* if hit_eof, c2 will become '\0' */
                    let c2b = *copy_input_buf.add(input_buf_ptr as usize) as u8;
                    input_buf_ptr += 1;

                    if c2b == b'\n' {
                        ereport!(
                            ERROR,
                            errmsg!("end-of-copy marker does not match previous newline style")
                        );
                    } else if c2b != b'\r' {
                        ereport!(
                            ERROR,
                            errmsg!("end-of-copy marker is not alone on its line")
                        );
                    }
                }

                /* Get the next character */
                /* IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0) */
                if input_buf_ptr + 0 >= copy_buf_len && !hit_eof {
                    input_buf_ptr = prev_raw_ptr; /* undo fetch */
                    need_data = true;
                    continue 'outer;
                }
                /* if hit_eof, c2 will become '\0' */
                let c2c = *copy_input_buf.add(input_buf_ptr as usize) as u8;
                input_buf_ptr += 1;

                if c2c != b'\r' && c2c != b'\n' {
                    ereport!(
                        ERROR,
                        errmsg!("end-of-copy marker is not alone on its line")
                    );
                }

                if ((*cs).eol_type == EOL_NL && c2c != b'\n')
                    || ((*cs).eol_type == EOL_CRNL && c2c != b'\n')
                    || ((*cs).eol_type == EOL_CR && c2c != b'\r')
                {
                    ereport!(
                        ERROR,
                        errmsg!("end-of-copy marker does not match previous newline style")
                    );
                }

                /*
                 * If there is any data on this line before the \., complain.
                 */
                if (*cs).line_buf.len > 0 || prev_raw_ptr > (*cs).input_buf_index {
                    ereport!(
                        ERROR,
                        errmsg!("end-of-copy marker is not alone on its line")
                    );
                }

                /*
                 * Discard the \. and newline, then report EOF.
                 */
                (*cs).input_buf_index = input_buf_ptr;
                result = true; /* report EOF */
                break 'outer;
            } else {
                /*
                 * If we are here, it means we found a backslash followed by
                 * something other than a period.  In non-CSV mode, anything
                 * after a backslash is special, so we skip over that second
                 * character too.  If we didn't do that \\. would be
                 * considered an eof-of copy, while in non-CSV mode it is a
                 * literal backslash followed by a period.
                 */
                input_buf_ptr += 1;
            }
        }
    } /* end of outer loop */

    /*
     * Transfer any still-uncopied data to line_buf.
     */
    /* REFILL_LINEBUF */
    if input_buf_ptr > (*cs).input_buf_index {
        appendBinaryStringInfo(
            &mut (*cs).line_buf,
            (*cs).input_buf.add((*cs).input_buf_index as usize) as *const c_void,
            input_buf_ptr - (*cs).input_buf_index,
        );
        (*cs).input_buf_index = input_buf_ptr;
    }

    result
}

/*
 *	Return decimal value for a hexadecimal digit
 */
unsafe fn GetDecimalFromHex(hex: u8) -> c_int {
    if isdigit_c(hex) {
        (hex - b'0') as c_int
    } else {
        (tolower(hex) - b'a' + 10) as c_int
    }
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf.  We use attribute_buf to hold the result
 * strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
unsafe fn CopyReadAttributesText(cstate: CopyFromState) -> c_int {
    let cs = cstate as *mut CopyFromStateData;
    let delimc: u8 = *(*cs).opts.delim as u8;
    let mut fieldno: c_int;
    let mut output_ptr: *mut c_char;
    let mut cur_ptr: *mut c_char;
    let line_end_ptr: *mut c_char;

    /*
     * We need a special case for zero-column tables: check that the input
     * line is empty, and return.
     */
    if (*cs).max_fields <= 0 {
        if (*cs).line_buf.len != 0 {
            ereport!(ERROR, errmsg!("extra data after last expected column"));
        }
        return 0;
    }

    resetStringInfo(&mut (*cs).attribute_buf);

    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.  We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
     */
    if (*cs).attribute_buf.maxlen <= (*cs).line_buf.len {
        enlargeStringInfo(&mut (*cs).attribute_buf, (*cs).line_buf.len);
    }
    output_ptr = (*cs).attribute_buf.data;

    /* set pointer variables for loop */
    cur_ptr = (*cs).line_buf.data;
    line_end_ptr = (*cs).line_buf.data.add((*cs).line_buf.len as usize);

    /* Outer loop iterates over fields */
    fieldno = 0;
    loop {
        let mut found_delim: bool = false;
        let start_ptr: *mut c_char;
        let mut end_ptr: *mut c_char;
        let input_len: c_int;
        let mut saw_non_ascii: bool = false;

        /* Make sure there is enough space for the next value */
        if fieldno >= (*cs).max_fields {
            (*cs).max_fields *= 2;
            (*cs).raw_fields = repalloc(
                (*cs).raw_fields as *mut c_void,
                (*cs).max_fields as Size * size_of::<*mut c_char>(),
            ) as *mut *mut c_char;
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;
        *(*cs).raw_fields.add(fieldno as usize) = output_ptr;

        /*
         * Scan data for field.
         *
         * Note that in this loop, we are scanning to locate the end of field
         * and also speculatively performing de-escaping.  Once we find the
         * end-of-field, we can match the raw field contents against the null
         * marker string.  Only after that comparison fails do we know that
         * de-escaping is actually the right thing to do; therefore we *must
         * not* throw any syntax errors before we've done the null-marker
         * check.
         */
        loop {
            let c: u8;

            end_ptr = cur_ptr;
            if cur_ptr >= line_end_ptr {
                break;
            }
            c = *cur_ptr as u8;
            cur_ptr = cur_ptr.add(1);
            if c == delimc {
                found_delim = true;
                break;
            }
            if c == b'\\' {
                if cur_ptr >= line_end_ptr {
                    break;
                }
                let c2 = *cur_ptr as u8;
                cur_ptr = cur_ptr.add(1);
                let out_c: u8 = match c2 {
                    b'0'..=b'7' => {
                        /* handle \013 */
                        let mut val: u8 = OCTVALUE(c2);
                        if cur_ptr < line_end_ptr {
                            let c3 = *cur_ptr as u8;
                            if ISOCTAL(c3) {
                                cur_ptr = cur_ptr.add(1);
                                val = (val << 3) + OCTVALUE(c3);
                                if cur_ptr < line_end_ptr {
                                    let c4 = *cur_ptr as u8;
                                    if ISOCTAL(c4) {
                                        cur_ptr = cur_ptr.add(1);
                                        val = (val << 3) + OCTVALUE(c4);
                                    }
                                }
                            }
                        }
                        let result_c = val & 0o377;
                        if result_c == 0 || IS_HIGHBIT_SET(result_c) {
                            saw_non_ascii = true;
                        }
                        result_c
                    }
                    b'x' => {
                        /* Handle \x3F */
                        let mut out = c2; /* default: literal x if no hex follows */
                        if cur_ptr < line_end_ptr {
                            let hexchar = *cur_ptr as u8;
                            if isxdigit(hexchar) {
                                let mut val: c_int = GetDecimalFromHex(hexchar);
                                cur_ptr = cur_ptr.add(1);
                                if cur_ptr < line_end_ptr {
                                    let hexchar2 = *cur_ptr as u8;
                                    if isxdigit(hexchar2) {
                                        cur_ptr = cur_ptr.add(1);
                                        val = (val << 4) + GetDecimalFromHex(hexchar2);
                                    }
                                }
                                let result_c = (val & 0xff) as u8;
                                if result_c == 0 || IS_HIGHBIT_SET(result_c) {
                                    saw_non_ascii = true;
                                }
                                out = result_c;
                            }
                        }
                        out
                    }
                    b'b' => b'\x08',
                    b'f' => b'\x0c',
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'v' => b'\x0b',
                    /* in all other cases, take the char after '\' literally */
                    other => other,
                };

                /* Add out_c to output string */
                *output_ptr = out_c as c_char;
                output_ptr = output_ptr.add(1);
                continue;
            }

            /* Add c to output string */
            *output_ptr = c as c_char;
            output_ptr = output_ptr.add(1);
        }

        /* Check whether raw input matched null marker */
        input_len = end_ptr as c_int - start_ptr as c_int;
        if input_len == (*cs).opts.null_print_len
            && strncmp(start_ptr, (*cs).opts.null_print, input_len as usize) == 0
        {
            *(*cs).raw_fields.add(fieldno as usize) = core::ptr::null_mut();
        }
        /* Check whether raw input matched default marker */
        else if fieldno < list_length((*cs).attnumlist)
            && !(*cs).opts.default_print.is_null()
            && input_len == (*cs).opts.default_print_len
            && strncmp(start_ptr, (*cs).opts.default_print, input_len as usize) == 0
        {
            /* fieldno is 0-indexed and attnum is 1-indexed */
            let m: c_int = list_nth_int((*cs).attnumlist, fieldno) - 1;

            if !(*(*cs).defexprs.add(m as usize)).is_null() {
                /* defaults contain entries for all physical attributes */
                *(*cs).defaults.add(m as usize) = true;
            } else {
                let tupDesc: TupleDesc = RelationGetDescr((*cs).rel);
                let att: Form_pg_attribute = TupleDescAttr(tupDesc, m);

                ereport!(
                    ERROR,
                    errmsg!(
                        "unexpected default marker in COPY data"
                    )
                );
            }
        } else {
            /*
             * At this point we know the field is supposed to contain data.
             *
             * If we de-escaped any non-7-bit-ASCII chars, make sure the
             * resulting string is valid data for the db encoding.
             */
            if saw_non_ascii {
                let fld: *mut c_char = *(*cs).raw_fields.add(fieldno as usize);
                pg_verifymbstr(fld, output_ptr as c_int - fld as c_int, false);
            }
        }

        /* Terminate attribute value in output area */
        *output_ptr = b'\0' as c_char;
        output_ptr = output_ptr.add(1);

        fieldno += 1;
        /* Done if we hit EOL instead of a delim */
        if !found_delim {
            break;
        }
    }

    /* Clean up state of attribute_buf */
    output_ptr = output_ptr.sub(1);
    Assert!(*output_ptr == b'\0' as c_char);
    (*cs).attribute_buf.len = output_ptr as c_int - (*cs).attribute_buf.data as c_int;

    fieldno
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
unsafe fn CopyReadAttributesCSV(cstate: CopyFromState) -> c_int {
    let cs = cstate as *mut CopyFromStateData;
    let delimc: u8 = *(*cs).opts.delim as u8;
    let quotec: u8 = *(*cs).opts.quote as u8;
    let escapec: u8 = *(*cs).opts.escape as u8;
    let mut fieldno: c_int;
    let mut output_ptr: *mut c_char;
    let mut cur_ptr: *mut c_char;
    let line_end_ptr: *mut c_char;

    /*
     * We need a special case for zero-column tables: check that the input
     * line is empty, and return.
     */
    if (*cs).max_fields <= 0 {
        if (*cs).line_buf.len != 0 {
            ereport!(ERROR, errmsg!("extra data after last expected column"));
        }
        return 0;
    }

    resetStringInfo(&mut (*cs).attribute_buf);

    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.  We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
     */
    if (*cs).attribute_buf.maxlen <= (*cs).line_buf.len {
        enlargeStringInfo(&mut (*cs).attribute_buf, (*cs).line_buf.len);
    }
    output_ptr = (*cs).attribute_buf.data;

    /* set pointer variables for loop */
    cur_ptr = (*cs).line_buf.data;
    line_end_ptr = (*cs).line_buf.data.add((*cs).line_buf.len as usize);

    /* Outer loop iterates over fields */
    fieldno = 0;
    'field_loop: loop {
        let mut found_delim: bool = false;
        let mut saw_quote: bool = false;
        let start_ptr: *mut c_char;
        let mut end_ptr: *mut c_char;
        let input_len: c_int;

        /* Make sure there is enough space for the next value */
        if fieldno >= (*cs).max_fields {
            (*cs).max_fields *= 2;
            (*cs).raw_fields = repalloc(
                (*cs).raw_fields as *mut c_void,
                (*cs).max_fields as Size * size_of::<*mut c_char>(),
            ) as *mut *mut c_char;
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;
        *(*cs).raw_fields.add(fieldno as usize) = output_ptr;

        /*
         * Scan data for field,
         *
         * The loop starts in "not quote" mode and then toggles between that
         * and "in quote" mode. The loop exits normally if it is in "not
         * quote" mode and a delimiter or line end is seen.
         */
        'scan: loop {
            /* Not in quote */
            'not_quote: loop {
                end_ptr = cur_ptr;
                if cur_ptr >= line_end_ptr {
                    break 'scan; /* goto endfield */
                }
                let c = *cur_ptr as u8;
                cur_ptr = cur_ptr.add(1);
                /* unquoted field delimiter */
                if c == delimc {
                    found_delim = true;
                    break 'scan; /* goto endfield */
                }
                /* start of quoted field (or part of field) */
                if c == quotec {
                    saw_quote = true;
                    break 'not_quote; /* enter in-quote loop */
                }
                /* Add c to output string */
                *output_ptr = c as c_char;
                output_ptr = output_ptr.add(1);
            }

            /* In quote */
            loop {
                end_ptr = cur_ptr;
                if cur_ptr >= line_end_ptr {
                    ereport!(ERROR, errmsg!("unterminated CSV quoted field"));
                }

                let c = *cur_ptr as u8;
                cur_ptr = cur_ptr.add(1);

                /* escape within a quoted field */
                if c == escapec {
                    /*
                     * peek at the next char if available, and escape it if it
                     * is an escape char or a quote char
                     */
                    if cur_ptr < line_end_ptr {
                        let nextc = *cur_ptr as u8;

                        if nextc == escapec || nextc == quotec {
                            *output_ptr = nextc as c_char;
                            output_ptr = output_ptr.add(1);
                            cur_ptr = cur_ptr.add(1);
                            continue;
                        }
                    }
                }

                /*
                 * end of quoted field. Must do this test after testing for
                 * escape in case quote char and escape char are the same
                 * (which is the common case).
                 */
                if c == quotec {
                    break; /* exit in-quote loop, back to not-quote loop */
                }

                /* Add c to output string */
                *output_ptr = c as c_char;
                output_ptr = output_ptr.add(1);
            }
        }
        /* endfield: */

        /* Terminate attribute value in output area */
        *output_ptr = b'\0' as c_char;
        output_ptr = output_ptr.add(1);

        /* Check whether raw input matched null marker */
        input_len = end_ptr as c_int - start_ptr as c_int;
        if !saw_quote
            && input_len == (*cs).opts.null_print_len
            && strncmp(start_ptr, (*cs).opts.null_print, input_len as usize) == 0
        {
            *(*cs).raw_fields.add(fieldno as usize) = core::ptr::null_mut();
        }
        /* Check whether raw input matched default marker */
        else if fieldno < list_length((*cs).attnumlist)
            && !(*cs).opts.default_print.is_null()
            && input_len == (*cs).opts.default_print_len
            && strncmp(start_ptr, (*cs).opts.default_print, input_len as usize) == 0
        {
            /* fieldno is 0-index and attnum is 1-index */
            let m: c_int = list_nth_int((*cs).attnumlist, fieldno) - 1;

            if !(*(*cs).defexprs.add(m as usize)).is_null() {
                /* defaults contain entries for all physical attributes */
                *(*cs).defaults.add(m as usize) = true;
            } else {
                let tupDesc: TupleDesc = RelationGetDescr((*cs).rel);
                let att: Form_pg_attribute = TupleDescAttr(tupDesc, m);

                ereport!(
                    ERROR,
                    errmsg!(
                        "unexpected default marker in COPY data"
                    )
                );
            }
        }

        fieldno += 1;
        /* Done if we hit EOL instead of a delim */
        if !found_delim {
            break 'field_loop;
        }
    }

    /* Clean up state of attribute_buf */
    output_ptr = output_ptr.sub(1);
    Assert!(*output_ptr == b'\0' as c_char);
    (*cs).attribute_buf.len = output_ptr as c_int - (*cs).attribute_buf.data as c_int;

    fieldno
}

/*
 * Read a binary attribute
 */
unsafe fn CopyReadBinaryAttribute(
    cstate: CopyFromState,
    flinfo: *mut FmgrInfo,
    typioparam: Oid,
    typmod: i32,
    isnull: *mut bool,
) -> Datum {
    let cs = cstate as *mut CopyFromStateData;
    let mut fld_size: i32 = 0;
    let result: Datum;

    if !CopyGetInt32(cstate, &mut fld_size) {
        ereport!(ERROR, errmsg!("unexpected EOF in COPY data"));
    }
    if fld_size == -1 {
        *isnull = true;
        return ReceiveFunctionCall(flinfo, core::ptr::null_mut(), typioparam, typmod);
    }
    if fld_size < 0 {
        ereport!(ERROR, errmsg!("invalid field size"));
    }

    /* reset attribute_buf to empty, and load raw data in it */
    resetStringInfo(&mut (*cs).attribute_buf);

    enlargeStringInfo(&mut (*cs).attribute_buf, fld_size);
    if CopyReadBinaryData(cstate, (*cs).attribute_buf.data, fld_size) != fld_size {
        ereport!(ERROR, errmsg!("unexpected EOF in COPY data"));
    }

    (*cs).attribute_buf.len = fld_size;
    *(*cs).attribute_buf.data.add(fld_size as usize) = b'\0' as c_char;

    /* Call the column type's binary input converter */
    result = ReceiveFunctionCall(flinfo, &mut (*cs).attribute_buf, typioparam, typmod);

    /* Trouble if it didn't eat the whole buffer */
    if (*cs).attribute_buf.cursor != (*cs).attribute_buf.len {
        ereport!(ERROR, errmsg!("incorrect binary data format"));
    }

    *isnull = false;
    result
}
