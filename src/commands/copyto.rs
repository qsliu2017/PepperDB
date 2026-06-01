//! src/backend/commands/copyto.c
//!     COPY <table> TO file/program/client
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!     src/backend/commands/copyto.c
//!
//! The shared COPY state (CopyFormatOptions, copy_data_dest_cb, CopyToState
//! handle, ProcessCopyOptions, CopyGetAttnums) lives in commands/copy.c and is
//! imported from crate::commands::copy.  CopyToStateData (the per-command state
//! struct) is private to copyto.c, so we define the full struct here; copy.rs
//! keeps only an opaque stub for it.

use crate::prelude::*;
use crate::nodes::nodes::Node;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{bytea, int16, int32, uint16, uint32, uint64, IS_HIGHBIT_SET, MemSet, NameData};
use crate::varatt::{VARDATA, VARSIZE};

// Shared COPY types/options/helpers (commands/copy.c).
use crate::commands::copy::{
    copy_data_dest_cb, CopyFormatOptions, CopyToState, CopyToStateData, CopyGetAttnums,
    ProcessCopyOptions,
};
// COPY format-routine API (commands/copyapi.h).
use crate::commands::copyapi::CopyToRoutine;
// COPY HEADER choice enum (commands/copy.h).
use crate::commands::copy::{CopyHeaderChoice, COPY_HEADER_FALSE};
// Progress-reporting parameter ids (commands/progress.h).
use crate::commands::progress::{
    PROGRESS_COPY_BYTES_PROCESSED, PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_TO,
    PROGRESS_COPY_TUPLES_PROCESSED, PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_CALLBACK,
    PROGRESS_COPY_TYPE_FILE, PROGRESS_COPY_TYPE_PIPE, PROGRESS_COPY_TYPE_PROGRAM,
};

// List handling (nodes/pg_list.h).
use crate::nodes::pg_list::{
    lfirst_int, list_length, list_member_int, list_member_oid, List, ListCell, NIL,
};
use crate::nodes::nodes::CmdType::{CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UPDATE};
use crate::nodes::parsenodes::{Query, RawStmt, QuerySource::QSRC_NON_INSTEAD_RULE,
    QuerySource::QSRC_QUAL_INSTEAD_RULE, CURSOR_OPT_PARALLEL_OK};
use crate::nodes::plannodes::PlannedStmt;

// Parse state, relations, tuple descriptors.
use crate::parser::parse_node::ParseState;
use crate::utils::rel::{Relation, RelationGetDescr, RelationGetNumberOfAttributes,
    RelationGetRelationName, RelationGetRelid};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_class::{
    RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW,
};

// Executor / query descriptor.
use crate::executor::execdesc::{CreateQueryDesc, FreeQueryDesc, QueryDesc};
use crate::executor::executor::{ExecutorEnd, ExecutorFinish, ExecutorRun, ExecutorStart};
use crate::executor::tuptable::{slot_getallattrs, TupleTableSlot};
use crate::executor::execTuples::ExecDropSingleTupleTableSlot;
use crate::access::sdir::ForwardScanDirection;
use crate::access::relscan::TableScanDesc;

// Destination receiver (tcop/dest.h).
use crate::tcop::dest::{CommandDest, CreateDestReceiver, DestReceiver, DestCopyOut, DestRemote};

// libpq COPY-protocol framing (libpq/libpq.h, libpq/pqformat.h, libpq/protocol.h).
use crate::libpq::libpq::pq_putmessage;
use crate::libpq::pqformat::{pq_beginmessage, pq_endmessage, pq_putemptymessage, pq_sendint16};
use crate::libpq::protocol::{PqMsg_CopyData, PqMsg_CopyDone, PqMsg_CopyOutResponse};

// StringInfo (lib/stringinfo.h).
use crate::lib::stringinfo::{
    appendBinaryStringInfo, makeStringInfo, resetStringInfo, StringInfo, StringInfoData,
};

// fmgr output/send-function machinery (utils/fmgr.h).
use crate::utils::fmgr::{fmgr_info, FmgrInfo, OutputFunctionCall, SendFunctionCall};

// Multibyte / encoding helpers (mb/pg_wchar.h).
use crate::mb::pg_wchar::{
    pg_encoding_mblen, pg_get_client_encoding, pg_server_to_any, GetDatabaseEncoding,
    PG_ENCODING_IS_CLIENT_ONLY, PG_SQL_ASCII,
};

// Snapshot (utils/snapshot.h).
use crate::utils::snapshot::{InvalidSnapshot, Snapshot};

// Progress command type (utils/backend_progress.h).
use crate::utils::activity::backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command, pgstat_progress_update_multi_param,
    pgstat_progress_update_param, ProgressCommandType::PROGRESS_COMMAND_COPY,
};

// Byte-swapping for network byte order (port/pg_bswap.h).
use crate::port::pg_bswap::{pg_hton16, pg_hton32};

// Absolute-path check (port.h).
use crate::port::port_api::{is_absolute_path, wait_result_to_str};

// Crate-root macros.
use crate::{ereport, errmsg, foreach, foreach_int, Assert};
use crate::c::NameStr;
use crate::lfirst_node;

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

/* ===================================================================
 * Local stubs for symbols whose canonical home is not yet ported.
 * =================================================================== */

// TODO(pg-port): real getTypeOutputInfo / getTypeBinaryOutputInfo live in
// utils/cache/lsyscache.c (TYPEOID syscache) -- not yet translated.
unsafe fn getTypeOutputInfo(_type: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!("getTypeOutputInfo: utils/cache/lsyscache.c not yet translated")
}
unsafe fn getTypeBinaryOutputInfo(_type: Oid, _typSend: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!("getTypeBinaryOutputInfo: utils/cache/lsyscache.c not yet translated")
}

// TODO(pg-port): real table scan AM wrappers live in access/tableam.h.
unsafe fn table_beginscan(
    _rel: Relation,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut c_void,
) -> TableScanDesc {
    unimplemented!() // TODO: access/tableam.h
}
unsafe fn table_scan_getnextslot(
    _sscan: TableScanDesc,
    _direction: crate::access::sdir::ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/tableam.h
}
unsafe fn table_endscan(_scan: TableScanDesc) {
    unimplemented!() // TODO: access/tableam.h
}
unsafe fn table_slot_create(_rel: Relation, _reglist: *mut *mut List) -> *mut TupleTableSlot {
    unimplemented!() // TODO: access/table/tableam.c
}

// TODO(pg-port): planner/rewriter entry points live in tcop/postgres.c.
unsafe fn pg_analyze_and_rewrite_fixedparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *const Oid,
    _numParams: c_int,
    _queryEnv: *mut c_void,
) -> *mut List {
    unimplemented!() // TODO: tcop/postgres.c
}
unsafe fn pg_plan_query(
    _querytree: *mut Query,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: *mut c_void,
) -> *mut PlannedStmt {
    unimplemented!() // TODO: tcop/postgres.c
}

// TODO(pg-port): snapshot management lives in utils/time/snapmgr.c.
unsafe fn GetActiveSnapshot() -> Snapshot {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn PushCopiedSnapshot(_snapshot: Snapshot) {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn UpdateActiveSnapshotCommandId() {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn PopActiveSnapshot() {
    unimplemented!() // TODO: utils/time/snapmgr.c
}

// TODO(pg-port): file/pipe descriptor helpers live in storage/file/fd.c.
unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut FILE {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn FreeFile(_file: *mut FILE) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn OpenPipeStream(_command: *const c_char, _mode: *const c_char) -> *mut FILE {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn ClosePipeStream(_file: *mut FILE) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}

// TODO(pg-port): RelationIsPopulated lives in utils/rel.h.
unsafe fn RelationIsPopulated(_rel: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

// TODO(pg-port): whereToSendOutput is a global in tcop/postgres.c; not yet ported.
static mut whereToSendOutput: CommandDest = DestRemote;

// Opaque libc FILE handle (stdio.h).  PG_BINARY_W is the fopen mode for COPY TO.
#[repr(C)]
pub struct FILE {
    _private: [u8; 0],
}
extern "C" {
    static mut stdout: *mut FILE;
    fn fwrite(ptr: *const c_void, size: usize, nmemb: usize, stream: *mut FILE) -> usize;
    fn ferror(stream: *mut FILE) -> c_int;
    fn fileno(stream: *mut FILE) -> c_int;
    fn fstat(fd: c_int, buf: *mut StatBuf) -> c_int;
    fn umask(mask: ModeT) -> ModeT;
    #[allow(improper_ctypes)]
    static mut errno: c_int;
}

// PG_BINARY_W (c.h / win32 compatibility): "wb" on platforms with a binary mode.
const PG_BINARY_W: *const c_char = c"w".as_ptr();

// errno values (errno.h).
const EPIPE: c_int = 32;
const ENOENT: c_int = 2;
const EACCES: c_int = 13;

// mode_t and stat-related constants (sys/stat.h).
type ModeT = u32;
const S_IWGRP: ModeT = 0o20;
const S_IWOTH: ModeT = 0o2;
const S_IFMT: ModeT = 0o170000;
const S_IFDIR: ModeT = 0o040000;
#[inline]
fn S_ISDIR(m: ModeT) -> bool {
    (m & S_IFMT) == S_IFDIR
}
// Minimal struct stat: we only read st_mode.  Sized to comfortably cover the
// platform struct so fstat() doesn't overrun.
#[repr(C)]
struct StatBuf {
    _pad: [u8; 256],
}
#[inline]
unsafe fn stat_mode(st: &StatBuf) -> ModeT {
    // st_mode lives at a platform-dependent offset; this stub reads it via a
    // dedicated helper once fd.c/stat are ported.  For now treat as zero.
    // TODO(pg-port): use real struct stat from storage/fd.c.
    let _ = st;
    0
}

/*
 * Represents the different dest cases we need to worry about at
 * the bottom level
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CopyDest {
    COPY_FILE,     /* to file (or a piped program) */
    COPY_FRONTEND, /* to frontend */
    COPY_CALLBACK, /* to callback function */
}
pub use CopyDest::*;

/*
 * This struct contains all the state variables used throughout a COPY TO
 * operation.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is true
 * when we have to do it the hard way.
 *
 * NOTE: the C definition is `typedef struct CopyToStateData { ... }` and is
 * private to copyto.c; copy.c keeps only an opaque handle.  We define the full
 * layout here.  copy.rs's stub `CopyToStateData` is a zero-sized placeholder; we
 * re-declare the real struct under the same path so the shared `CopyToState`
 * handle (`*mut CopyToStateData`) points at it.
 */
#[repr(C)]
pub struct CopyToStateDataFull {
    /* format-specific routines */
    pub routine: *const CopyToRoutine,

    /* low-level state data */
    pub copy_dest: CopyDest,      /* type of copy source/destination */
    pub copy_file: *mut FILE,     /* used if copy_dest == COPY_FILE */
    pub fe_msgbuf: StringInfo,    /* used for all dests during COPY TO */

    pub file_encoding: c_int,         /* file or remote side's character encoding */
    pub need_transcoding: bool,       /* file encoding diff from server? */
    pub encoding_embeds_ascii: bool,  /* ASCII can be non-first byte? */

    /* parameters from the COPY command */
    pub rel: Relation,                  /* relation to copy to */
    pub queryDesc: *mut QueryDesc,      /* executable query to copy from */
    pub attnumlist: *mut List,          /* integer list of attnums to copy */
    pub filename: *mut c_char,          /* filename, or NULL for STDOUT */
    pub is_program: bool,               /* is 'filename' a program to popen? */
    pub data_dest_cb: copy_data_dest_cb, /* function for writing data */

    pub opts: CopyFormatOptions,
    pub whereClause: *mut Node, /* WHERE condition (or NULL) */

    /*
     * Working state
     */
    pub copycontext: MemoryContext, /* per-copy execution context */

    pub out_functions: *mut FmgrInfo, /* lookup info for output functions */
    pub rowcontext: MemoryContext,    /* per-row evaluation context */
    pub bytes_processed: uint64,      /* number of bytes processed so far */
}

// Bridge: copy.rs declares `CopyToState = *mut CopyToStateData`, with
// CopyToStateData being an opaque stub.  Reinterpret a CopyToState handle as the
// full struct defined above.
#[inline]
unsafe fn cs(cstate: CopyToState) -> *mut CopyToStateDataFull {
    cstate as *mut CopyToStateDataFull
}

/* DestReceiver for COPY (query) TO */
#[repr(C)]
pub struct DR_copy {
    pub pub_: DestReceiver, /* publicly-known function pointers */
    pub cstate: CopyToState, /* CopyToStateData for the command */
    pub processed: uint64,  /* # of tuples processed */
}

/* NOTE: there's a copy of this in copyfromparse.c */
static BinarySignature: [c_char; 11] = [
    b'P' as c_char,
    b'G' as c_char,
    b'C' as c_char,
    b'O' as c_char,
    b'P' as c_char,
    b'Y' as c_char,
    b'\n' as c_char,
    0o377u8 as c_char,
    b'\r' as c_char,
    b'\n' as c_char,
    0,
];

/*
 * COPY TO routines for built-in formats.
 *
 * CSV and text formats share the same TextLike routines except for the
 * one-row callback.
 */

/* text format */
static CopyToRoutineText: CopyToRoutine = CopyToRoutine {
    CopyToStart: Some(CopyToTextLikeStart),
    CopyToOutFunc: Some(CopyToTextLikeOutFunc),
    CopyToOneRow: Some(CopyToTextOneRow),
    CopyToEnd: Some(CopyToTextLikeEnd),
};

/* CSV format */
static CopyToRoutineCSV: CopyToRoutine = CopyToRoutine {
    CopyToStart: Some(CopyToTextLikeStart),
    CopyToOutFunc: Some(CopyToTextLikeOutFunc),
    CopyToOneRow: Some(CopyToCSVOneRow),
    CopyToEnd: Some(CopyToTextLikeEnd),
};

/* binary format */
static CopyToRoutineBinary: CopyToRoutine = CopyToRoutine {
    CopyToStart: Some(CopyToBinaryStart),
    CopyToOutFunc: Some(CopyToBinaryOutFunc),
    CopyToOneRow: Some(CopyToBinaryOneRow),
    CopyToEnd: Some(CopyToBinaryEnd),
};

/* Return a COPY TO routine for the given options */
unsafe fn CopyToGetRoutine(opts: *const CopyFormatOptions) -> *const CopyToRoutine {
    if (*opts).csv_mode {
        return &CopyToRoutineCSV;
    } else if (*opts).binary {
        return &CopyToRoutineBinary;
    }

    /* default is text */
    &CopyToRoutineText
}

/* Implementation of the start callback for text and CSV formats */
unsafe extern "C" fn CopyToTextLikeStart(cstate: CopyToState, tupDesc: TupleDesc) {
    let cstate = cs(cstate);

    /*
     * For non-binary copy, we need to convert null_print to file encoding,
     * because it will be sent directly with CopySendString.
     */
    if (*cstate).need_transcoding {
        (*cstate).opts.null_print_client = pg_server_to_any(
            (*cstate).opts.null_print,
            (*cstate).opts.null_print_len,
            (*cstate).file_encoding,
        );
    }

    /* if a header has been requested send the line */
    if (*cstate).opts.header_line != COPY_HEADER_FALSE {
        let cur: *mut ListCell;
        let mut hdr_delim: bool = false;

        foreach!(cur, (*cstate).attnumlist, {
            let attnum: c_int = lfirst_int(crate::current_cell!(cur));
            let colname: *mut c_char;

            if hdr_delim {
                CopySendChar(cstate as CopyToState, *(*cstate).opts.delim);
            }
            hdr_delim = true;

            colname =
                NameStr(&(*TupleDescAttr(tupDesc, attnum - 1)).attname) as *mut c_char;

            if (*cstate).opts.csv_mode {
                CopyAttributeOutCSV(cstate as CopyToState, colname, false);
            } else {
                CopyAttributeOutText(cstate as CopyToState, colname);
            }
        });

        CopySendTextLikeEndOfRow(cstate as CopyToState);
    }
}

/*
 * Implementation of the outfunc callback for text and CSV formats. Assign
 * the output function data to the given *finfo.
 */
unsafe extern "C" fn CopyToTextLikeOutFunc(_cstate: CopyToState, atttypid: Oid, finfo: *mut FmgrInfo) {
    let mut func_oid: Oid = 0;
    let mut is_varlena: bool = false;

    /* Set output function for an attribute */
    getTypeOutputInfo(atttypid, &mut func_oid, &mut is_varlena);
    fmgr_info(func_oid, finfo);
}

/* Implementation of the per-row callback for text format */
unsafe extern "C" fn CopyToTextOneRow(cstate: CopyToState, slot: *mut TupleTableSlot) {
    CopyToTextLikeOneRow(cstate, slot, false);
}

/* Implementation of the per-row callback for CSV format */
unsafe extern "C" fn CopyToCSVOneRow(cstate: CopyToState, slot: *mut TupleTableSlot) {
    CopyToTextLikeOneRow(cstate, slot, true);
}

/*
 * Workhorse for CopyToTextOneRow() and CopyToCSVOneRow().
 *
 * We use pg_attribute_always_inline to reduce function call overhead
 * and to help compilers to optimize away the 'is_csv' condition.
 */
#[inline(always)]
unsafe fn CopyToTextLikeOneRow(cstate: CopyToState, slot: *mut TupleTableSlot, is_csv: bool) {
    let csd = cs(cstate);
    let mut need_delim: bool = false;
    let out_functions: *mut FmgrInfo = (*csd).out_functions;

    foreach_int!(attnum, (*csd).attnumlist, {
        let value: Datum = *(*slot).tts_values.offset((attnum - 1) as isize);
        let isnull: bool = *(*slot).tts_isnull.offset((attnum - 1) as isize);

        if need_delim {
            CopySendChar(cstate, *(*csd).opts.delim);
        }
        need_delim = true;

        if isnull {
            CopySendString(cstate, (*csd).opts.null_print_client);
        } else {
            let string: *mut c_char;

            string = OutputFunctionCall(out_functions.offset((attnum - 1) as isize), value);

            if is_csv {
                CopyAttributeOutCSV(
                    cstate,
                    string,
                    *(*csd).opts.force_quote_flags.offset((attnum - 1) as isize),
                );
            } else {
                CopyAttributeOutText(cstate, string);
            }
        }
    });

    CopySendTextLikeEndOfRow(cstate);
}

/* Implementation of the end callback for text and CSV formats */
unsafe extern "C" fn CopyToTextLikeEnd(_cstate: CopyToState) {
    /* Nothing to do here */
}

/*
 * Implementation of the start callback for binary format. Send a header
 * for a binary copy.
 */
unsafe extern "C" fn CopyToBinaryStart(cstate: CopyToState, _tupDesc: TupleDesc) {
    let tmp: int32;

    /* Signature */
    CopySendData(cstate, BinarySignature.as_ptr() as *const c_void, 11);
    /* Flags field */
    tmp = 0;
    CopySendInt32(cstate, tmp);
    /* No header extension */
    let tmp2: int32 = 0;
    CopySendInt32(cstate, tmp2);
    let _ = tmp;
}

/*
 * Implementation of the outfunc callback for binary format. Assign
 * the binary output function to the given *finfo.
 */
unsafe extern "C" fn CopyToBinaryOutFunc(_cstate: CopyToState, atttypid: Oid, finfo: *mut FmgrInfo) {
    let mut func_oid: Oid = 0;
    let mut is_varlena: bool = false;

    /* Set output function for an attribute */
    getTypeBinaryOutputInfo(atttypid, &mut func_oid, &mut is_varlena);
    fmgr_info(func_oid, finfo);
}

/* Implementation of the per-row callback for binary format */
unsafe extern "C" fn CopyToBinaryOneRow(cstate: CopyToState, slot: *mut TupleTableSlot) {
    let csd = cs(cstate);
    let out_functions: *mut FmgrInfo = (*csd).out_functions;

    /* Binary per-tuple header */
    CopySendInt16(cstate, list_length((*csd).attnumlist) as int16);

    foreach_int!(attnum, (*csd).attnumlist, {
        let value: Datum = *(*slot).tts_values.offset((attnum - 1) as isize);
        let isnull: bool = *(*slot).tts_isnull.offset((attnum - 1) as isize);

        if isnull {
            CopySendInt32(cstate, -1);
        } else {
            let outputbytes: *mut bytea;

            outputbytes = SendFunctionCall(out_functions.offset((attnum - 1) as isize), value);
            CopySendInt32(
                cstate,
                (VARSIZE(outputbytes as *const c_char) as int32) - VARHDRSZ,
            );
            CopySendData(
                cstate,
                VARDATA(outputbytes as *const c_char) as *const c_void,
                (VARSIZE(outputbytes as *const c_char) as int32 - VARHDRSZ) as c_int,
            );
        }
    });

    CopySendEndOfRow(cstate);
}

/* Implementation of the end callback for binary format */
unsafe extern "C" fn CopyToBinaryEnd(cstate: CopyToState) {
    /* Generate trailer for a binary copy */
    CopySendInt16(cstate, -1);
    /* Need to flush out the trailer */
    CopySendEndOfRow(cstate);
}

/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
unsafe fn SendCopyBegin(cstate: CopyToState) {
    let csd = cs(cstate);
    let mut buf: StringInfoData = core::mem::zeroed();
    let natts: c_int = list_length((*csd).attnumlist);
    let format: int16 = if (*csd).opts.binary { 1 } else { 0 };
    let mut i: c_int;

    pq_beginmessage(&mut buf, PqMsg_CopyOutResponse as c_char);
    pq_sendbyte(&mut buf, format as uint8); /* overall format */
    pq_sendint16(&mut buf, natts as uint16);
    i = 0;
    while i < natts {
        pq_sendint16(&mut buf, format as uint16); /* per-column formats */
        i += 1;
    }
    pq_endmessage(&mut buf);
    (*csd).copy_dest = COPY_FRONTEND;
}

unsafe fn SendCopyEnd(cstate: CopyToState) {
    let csd = cs(cstate);
    /* Shouldn't have any unsent data */
    Assert!((*(*csd).fe_msgbuf).len == 0);
    /* Send Copy Done message */
    pq_putemptymessage(PqMsg_CopyDone as c_char);
}

/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
unsafe fn CopySendData(cstate: CopyToState, databuf: *const c_void, datasize: c_int) {
    let csd = cs(cstate);
    appendBinaryStringInfo((*csd).fe_msgbuf, databuf, datasize);
}

unsafe fn CopySendString(cstate: CopyToState, str: *const c_char) {
    let csd = cs(cstate);
    appendBinaryStringInfo((*csd).fe_msgbuf, str as *const c_void, strlen(str) as c_int);
}

unsafe fn CopySendChar(cstate: CopyToState, c: c_char) {
    let csd = cs(cstate);
    crate::appendStringInfoCharMacro!((*csd).fe_msgbuf, c);
}

unsafe fn CopySendEndOfRow(cstate: CopyToState) {
    let csd = cs(cstate);
    let fe_msgbuf: StringInfo = (*csd).fe_msgbuf;

    match (*csd).copy_dest {
        COPY_FILE => {
            if fwrite(
                (*fe_msgbuf).data as *const c_void,
                (*fe_msgbuf).len as usize,
                1,
                (*csd).copy_file,
            ) != 1
                || ferror((*csd).copy_file) != 0
            {
                if (*csd).is_program {
                    if errno == EPIPE {
                        /*
                         * The pipe will be closed automatically on error at
                         * the end of transaction, but we might get a better
                         * error message from the subprocess' exit code than
                         * just "Broken Pipe"
                         */
                        ClosePipeToProgram(cstate);

                        /*
                         * If ClosePipeToProgram() didn't throw an error, the
                         * program terminated normally, but closed the pipe
                         * first. Restore errno, and throw an error.
                         */
                        errno = EPIPE;
                    }
                    ereport!(ERROR, "could not write to COPY program: %m");
                } else {
                    ereport!(ERROR, "could not write to COPY file: %m");
                }
            }
        }
        COPY_FRONTEND => {
            /* Dump the accumulated row as one CopyData message */
            let _ = pq_putmessage(
                PqMsg_CopyData as c_char,
                (*fe_msgbuf).data,
                (*fe_msgbuf).len as Size,
            );
        }
        COPY_CALLBACK => {
            (*csd).data_dest_cb.unwrap()((*fe_msgbuf).data as *mut c_void, (*fe_msgbuf).len);
        }
    }

    /* Update the progress */
    (*csd).bytes_processed += (*fe_msgbuf).len as uint64;
    pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, (*csd).bytes_processed as i64);

    resetStringInfo(fe_msgbuf);
}

/*
 * Wrapper function of CopySendEndOfRow for text and CSV formats. Sends the
 * line termination and do common appropriate things for the end of row.
 */
#[inline]
unsafe fn CopySendTextLikeEndOfRow(cstate: CopyToState) {
    let csd = cs(cstate);
    match (*csd).copy_dest {
        COPY_FILE => {
            /* Default line termination depends on platform */
            // #ifndef WIN32
            CopySendChar(cstate, b'\n' as c_char);
            // #else
            // CopySendString(cstate, "\r\n");
            // #endif
        }
        COPY_FRONTEND => {
            /* The FE/BE protocol uses \n as newline for all platforms */
            CopySendChar(cstate, b'\n' as c_char);
        }
        _ => {}
    }

    /* Now take the actions related to the end of a row */
    CopySendEndOfRow(cstate);
}

/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
#[inline]
unsafe fn CopySendInt32(cstate: CopyToState, val: int32) {
    let buf: uint32;

    buf = pg_hton32(val as uint32);
    CopySendData(
        cstate,
        &buf as *const uint32 as *const c_void,
        core::mem::size_of::<uint32>() as c_int,
    );
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
#[inline]
unsafe fn CopySendInt16(cstate: CopyToState, val: int16) {
    let buf: uint16;

    buf = pg_hton16(val as uint16);
    CopySendData(
        cstate,
        &buf as *const uint16 as *const c_void,
        core::mem::size_of::<uint16>() as c_int,
    );
}

// pq_sendbyte (libpq/pqformat.h) is `pq_sendint8`; provide a thin wrapper to
// match the C call site name used by SendCopyBegin.
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: uint8) {
    crate::libpq::pqformat::pq_sendint8(buf, byt);
}

/*
 * Closes the pipe to an external program, checking the pclose() return code.
 */
unsafe fn ClosePipeToProgram(cstate: CopyToState) {
    let csd = cs(cstate);
    let pclose_rc: c_int;

    Assert!((*csd).is_program);

    pclose_rc = ClosePipeStream((*csd).copy_file);
    if pclose_rc == -1 {
        ereport!(ERROR, "could not close pipe to external command: %m");
    } else if pclose_rc != 0 {
        let _ = wait_result_to_str(pclose_rc);
        ereport!(ERROR, "program \"{}\" failed");
    }
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
unsafe fn EndCopy(cstate: CopyToState) {
    let csd = cs(cstate);
    if (*csd).is_program {
        ClosePipeToProgram(cstate);
    } else {
        if !(*csd).filename.is_null() && FreeFile((*csd).copy_file) != 0 {
            ereport!(ERROR, "could not close file \"{}\": %m");
        }
    }

    pgstat_progress_end_command();

    MemoryContextDelete((*csd).copycontext);
    pfree(cstate as *mut c_void);
}

/*
 * Setup CopyToState to read tuples from a table or a query for COPY TO.
 *
 * 'rel': Relation to be copied
 * 'raw_query': Query whose results are to be copied
 * 'queryRelId': OID of base relation to convert to a query (for RLS)
 * 'filename': Name of server-local file to write, NULL for STDOUT
 * 'is_program': true if 'filename' is program to execute
 * 'data_dest_cb': Callback that processes the output data
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyToState, to be passed to DoCopyTo() and related functions.
 */
pub unsafe fn BeginCopyTo(
    pstate: *mut ParseState,
    rel: Relation,
    raw_query: *mut RawStmt,
    queryRelId: Oid,
    filename: *const c_char,
    is_program: bool,
    data_dest_cb: copy_data_dest_cb,
    attnamelist: *mut List,
    options: *mut List,
) -> CopyToState {
    let cstate: *mut CopyToStateDataFull;
    let pipe: bool = filename.is_null() && data_dest_cb.is_none();
    let tupDesc: TupleDesc;
    let num_phys_attrs: c_int;
    let oldcontext: MemoryContext;
    let progress_cols: [c_int; 2] = [PROGRESS_COPY_COMMAND, PROGRESS_COPY_TYPE];
    let mut progress_vals: [i64; 2] = [PROGRESS_COPY_COMMAND_TO as i64, 0];

    if !rel.is_null() && (*(*rel).rd_rel).relkind != RELKIND_RELATION {
        if (*(*rel).rd_rel).relkind == RELKIND_VIEW {
            ereport!(ERROR, "cannot copy from view \"{}\"");
        } else if (*(*rel).rd_rel).relkind == RELKIND_MATVIEW {
            if !RelationIsPopulated(rel) {
                ereport!(ERROR, "cannot copy from unpopulated materialized view \"{}\"");
            }
        } else if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
            ereport!(ERROR, "cannot copy from foreign table \"{}\"");
        } else if (*(*rel).rd_rel).relkind == RELKIND_SEQUENCE {
            ereport!(ERROR, "cannot copy from sequence \"{}\"");
        } else if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            ereport!(ERROR, "cannot copy from partitioned table \"{}\"");
        } else {
            ereport!(ERROR, "cannot copy from non-table relation \"{}\"");
        }
    }

    /* Allocate workspace and zero all fields */
    cstate = palloc0(core::mem::size_of::<CopyToStateDataFull>()) as *mut CopyToStateDataFull;

    /*
     * We allocate everything used by a cstate in a new memory context. This
     * avoids memory leaks during repeated use of COPY in a query.
     */
    (*cstate).copycontext =
        AllocSetContextCreate!(CurrentMemoryContext, c"COPY".as_ptr(), ALLOCSET_DEFAULT_SIZES);

    oldcontext = MemoryContextSwitchTo((*cstate).copycontext);

    /* Extract options from the statement node tree */
    ProcessCopyOptions(pstate, &mut (*cstate).opts, false /* is_from */, options);

    /* Set format routine */
    (*cstate).routine = CopyToGetRoutine(&(*cstate).opts);

    /* Process the source/target relation or query */
    if !rel.is_null() {
        Assert!(raw_query.is_null());

        (*cstate).rel = rel;

        tupDesc = RelationGetDescr((*cstate).rel);
    } else {
        let rewritten: *mut List;
        let query: *mut Query;
        let plan: *mut PlannedStmt;
        let dest: *mut DestReceiver;

        (*cstate).rel = core::ptr::null_mut();

        /*
         * Run parse analysis and rewrite.  Note this also acquires sufficient
         * locks on the source table(s).
         */
        rewritten = pg_analyze_and_rewrite_fixedparams(
            raw_query,
            (*pstate).p_sourcetext,
            core::ptr::null(),
            0,
            core::ptr::null_mut(),
        );

        /* check that we got back something we can work with */
        if rewritten == NIL {
            ereport!(ERROR, "DO INSTEAD NOTHING rules are not supported for COPY");
        } else if list_length(rewritten) > 1 {
            let lc: *mut ListCell;

            /* examine queries to determine which error message to issue */
            foreach!(lc, rewritten, {
                let q: *mut Query = lfirst_node!(Query, T_Query, crate::current_cell!(lc));

                if (*q).querySource == QSRC_QUAL_INSTEAD_RULE {
                    ereport!(ERROR, "conditional DO INSTEAD rules are not supported for COPY");
                }
                if (*q).querySource == QSRC_NON_INSTEAD_RULE {
                    ereport!(ERROR, "DO ALSO rules are not supported for COPY");
                }
            });

            ereport!(ERROR, "multi-statement DO INSTEAD rules are not supported for COPY");
        }

        query = lfirst_node!(Query, T_Query, crate::nodes::pg_list::list_head(rewritten));

        /* The grammar allows SELECT INTO, but we don't support that */
        if !(*query).utilityStmt.is_null()
            && crate::IsA!((*query).utilityStmt, T_CreateTableAsStmt)
        {
            ereport!(ERROR, "COPY (SELECT INTO) is not supported");
        }

        /* The only other utility command we could see is NOTIFY */
        if !(*query).utilityStmt.is_null() {
            ereport!(ERROR, "COPY query must not be a utility command");
        }

        /*
         * Similarly the grammar doesn't enforce the presence of a RETURNING
         * clause, but this is required here.
         */
        if (*query).commandType != CMD_SELECT && (*query).returningList == NIL {
            Assert!(
                (*query).commandType == CMD_INSERT
                    || (*query).commandType == CMD_UPDATE
                    || (*query).commandType == CMD_DELETE
                    || (*query).commandType == CMD_MERGE
            );

            ereport!(ERROR, "COPY query must have a RETURNING clause");
        }

        /* plan the query */
        plan = pg_plan_query(
            query,
            (*pstate).p_sourcetext,
            CURSOR_OPT_PARALLEL_OK,
            core::ptr::null_mut(),
        );

        /*
         * With row-level security and a user using "COPY relation TO", we
         * have to convert the "COPY relation TO" to a query-based COPY (eg:
         * "COPY (SELECT * FROM ONLY relation) TO"), to allow the rewriter to
         * add in any RLS clauses.
         *
         * When this happens, we are passed in the relid of the originally
         * found relation (which we have locked).  As the planner will look up
         * the relation again, we double-check here to make sure it found the
         * same one that we have locked.
         */
        if queryRelId != InvalidOid {
            /*
             * Note that with RLS involved there may be multiple relations,
             * and while the one we need is almost certainly first, we don't
             * make any guarantees of that in the planner, so check the whole
             * list and make sure we find the original relation.
             */
            if !list_member_oid((*plan).relationOids, queryRelId) {
                ereport!(ERROR, "relation referenced by COPY statement has changed");
            }
        }

        /*
         * Use a snapshot with an updated command ID to ensure this query sees
         * results of any previously executed queries.
         */
        PushCopiedSnapshot(GetActiveSnapshot());
        UpdateActiveSnapshotCommandId();

        /* Create dest receiver for COPY OUT */
        dest = CreateDestReceiver(DestCopyOut);
        (*(dest as *mut DR_copy)).cstate = cstate as CopyToState;

        /* Create a QueryDesc requesting no output */
        (*cstate).queryDesc = CreateQueryDesc(
            plan,
            (*pstate).p_sourcetext,
            GetActiveSnapshot() as *mut crate::nodes::execnodes::SnapshotData,
            InvalidSnapshot as *mut crate::nodes::execnodes::SnapshotData,
            dest,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            0,
        );

        /*
         * Call ExecutorStart to prepare the plan for execution.
         *
         * ExecutorStart computes a result tupdesc for us
         */
        ExecutorStart((*cstate).queryDesc, 0);

        tupDesc = (*(*cstate).queryDesc).tupDesc;
    }

    /* Generate or convert list of attributes to process */
    (*cstate).attnumlist = CopyGetAttnums(tupDesc, (*cstate).rel, attnamelist);

    num_phys_attrs = (*tupDesc).natts;

    /* Convert FORCE_QUOTE name list to per-column flags, check validity */
    (*cstate).opts.force_quote_flags =
        palloc0(num_phys_attrs as Size * core::mem::size_of::<bool>()) as *mut bool;
    if (*cstate).opts.force_quote_all {
        MemSet(
            (*cstate).opts.force_quote_flags as *mut c_void,
            1,
            num_phys_attrs as Size * core::mem::size_of::<bool>(),
        );
    } else if !(*cstate).opts.force_quote.is_null() {
        let attnums: *mut List;
        let cur: *mut ListCell;

        attnums = CopyGetAttnums(tupDesc, (*cstate).rel, (*cstate).opts.force_quote);

        foreach!(cur, attnums, {
            let attnum: c_int = lfirst_int(crate::current_cell!(cur));
            let attr: Form_pg_attribute = TupleDescAttr(tupDesc, attnum - 1);

            if !list_member_int((*cstate).attnumlist, attnum) {
                let _ = attr;
                /*- translator: %s is the name of a COPY option, e.g. FORCE_NOT_NULL */
                ereport!(ERROR, "{} column \"{}\" not referenced by COPY");
            }
            *(*cstate).opts.force_quote_flags.offset((attnum - 1) as isize) = true;
        });
    }

    /* Use client encoding when ENCODING option is not specified. */
    if (*cstate).opts.file_encoding < 0 {
        (*cstate).file_encoding = pg_get_client_encoding();
    } else {
        (*cstate).file_encoding = (*cstate).opts.file_encoding;
    }

    /*
     * Set up encoding conversion info if the file and server encodings differ
     * (see also pg_server_to_any).
     */
    if (*cstate).file_encoding == GetDatabaseEncoding() || (*cstate).file_encoding == PG_SQL_ASCII {
        (*cstate).need_transcoding = false;
    } else {
        (*cstate).need_transcoding = true;
    }

    /* See Multibyte encoding comment above */
    (*cstate).encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY((*cstate).file_encoding);

    (*cstate).copy_dest = COPY_FILE; /* default */

    if data_dest_cb.is_some() {
        progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK as i64;
        (*cstate).copy_dest = COPY_CALLBACK;
        (*cstate).data_dest_cb = data_dest_cb;
    } else if pipe {
        progress_vals[1] = PROGRESS_COPY_TYPE_PIPE as i64;

        Assert!(!is_program); /* the grammar does not allow this */
        if whereToSendOutput != DestRemote {
            (*cstate).copy_file = stdout;
        }
    } else {
        (*cstate).filename = pstrdup(filename);
        (*cstate).is_program = is_program;

        if is_program {
            progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM as i64;
            (*cstate).copy_file = OpenPipeStream((*cstate).filename, PG_BINARY_W);
            if (*cstate).copy_file.is_null() {
                ereport!(ERROR, "could not execute command \"{}\": %m");
            }
        } else {
            let oumask: ModeT; /* Pre-existing umask value */
            let mut st: StatBuf = core::mem::zeroed();

            progress_vals[1] = PROGRESS_COPY_TYPE_FILE as i64;

            /*
             * Prevent write to relative path ... too easy to shoot oneself in
             * the foot by overwriting a database file ...
             */
            if !is_absolute_path(filename) {
                ereport!(ERROR, "relative path not allowed for COPY to file");
            }

            oumask = umask(S_IWGRP | S_IWOTH);
            // PG_TRY()/PG_FINALLY(): restore umask even on error.  In this port
            // AllocateFile does not longjmp, so a straight-line call + restore
            // is faithful.
            (*cstate).copy_file = AllocateFile((*cstate).filename, PG_BINARY_W);
            umask(oumask);
            if (*cstate).copy_file.is_null() {
                /* copy errno because ereport subfunctions might change it */
                let save_errno: c_int = errno;

                let _ = save_errno == ENOENT || save_errno == EACCES;
                ereport!(ERROR, "could not open file \"{}\" for writing: %m");
            }

            if fstat(fileno((*cstate).copy_file), &mut st) != 0 {
                ereport!(ERROR, "could not stat file \"{}\": %m");
            }

            if S_ISDIR(stat_mode(&st)) {
                ereport!(ERROR, "\"{}\" is a directory");
            }
        }
    }

    /* initialize progress */
    pgstat_progress_start_command(
        PROGRESS_COMMAND_COPY,
        if !(*cstate).rel.is_null() {
            RelationGetRelid((*cstate).rel)
        } else {
            InvalidOid
        },
    );
    pgstat_progress_update_multi_param(2, progress_cols.as_ptr(), progress_vals.as_ptr());

    (*cstate).bytes_processed = 0;

    MemoryContextSwitchTo(oldcontext);

    cstate as CopyToState
}

/*
 * Clean up storage and release resources for COPY TO.
 */
pub unsafe fn EndCopyTo(cstate: CopyToState) {
    let csd = cs(cstate);
    if !(*csd).queryDesc.is_null() {
        /* Close down the query and free resources. */
        ExecutorFinish((*csd).queryDesc);
        ExecutorEnd((*csd).queryDesc);
        FreeQueryDesc((*csd).queryDesc);
        PopActiveSnapshot();
    }

    /* Clean up storage */
    EndCopy(cstate);
}

/*
 * Copy from relation or query TO file.
 *
 * Returns the number of rows processed.
 */
pub unsafe fn DoCopyTo(cstate: CopyToState) -> uint64 {
    let csd = cs(cstate);
    let pipe: bool = (*csd).filename.is_null() && (*csd).data_dest_cb.is_none();
    let fe_copy: bool = pipe && whereToSendOutput == DestRemote;
    let tupDesc: TupleDesc;
    let num_phys_attrs: c_int;
    let cur: *mut ListCell;
    let processed: uint64;

    if fe_copy {
        SendCopyBegin(cstate);
    }

    if !(*csd).rel.is_null() {
        tupDesc = RelationGetDescr((*csd).rel);
    } else {
        tupDesc = (*(*csd).queryDesc).tupDesc;
    }
    num_phys_attrs = (*tupDesc).natts;
    (*csd).opts.null_print_client = (*csd).opts.null_print; /* default */

    /* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
    (*csd).fe_msgbuf = makeStringInfo();

    /* Get info about the columns we need to process. */
    (*csd).out_functions =
        palloc(num_phys_attrs as Size * core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
    foreach!(cur, (*csd).attnumlist, {
        let attnum: c_int = lfirst_int(crate::current_cell!(cur));
        let attr: Form_pg_attribute = TupleDescAttr(tupDesc, attnum - 1);

        (*(*csd).routine).CopyToOutFunc.unwrap()(
            cstate,
            (*attr).atttypid,
            (*csd).out_functions.offset((attnum - 1) as isize),
        );
    });

    /*
     * Create a temporary memory context that we can reset once per row to
     * recover palloc'd memory.  This avoids any problems with leaks inside
     * datatype output routines, and should be faster than retail pfree's
     * anyway.  (We don't need a whole econtext as CopyFrom does.)
     */
    (*csd).rowcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"COPY TO".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    (*(*csd).routine).CopyToStart.unwrap()(cstate, tupDesc);

    if !(*csd).rel.is_null() {
        let slot: *mut TupleTableSlot;
        let scandesc: TableScanDesc;
        let mut proc: uint64;

        scandesc = table_beginscan((*csd).rel, GetActiveSnapshot(), 0, core::ptr::null_mut());
        slot = table_slot_create((*csd).rel, core::ptr::null_mut());

        proc = 0;
        while table_scan_getnextslot(scandesc, ForwardScanDirection, slot) {
            crate::miscadmin::CHECK_FOR_INTERRUPTS();

            /* Deconstruct the tuple ... */
            slot_getallattrs(slot);

            /* Format and send the data */
            CopyOneRowTo(cstate, slot);

            /*
             * Increment the number of processed tuples, and report the
             * progress.
             */
            proc += 1;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, proc as i64);
        }

        ExecDropSingleTupleTableSlot(slot);
        table_endscan(scandesc);

        processed = proc;
    } else {
        /* run the plan --- the dest receiver will send tuples */
        ExecutorRun((*csd).queryDesc, ForwardScanDirection, 0);
        processed = (*((*(*csd).queryDesc).dest as *mut DR_copy)).processed;
    }

    (*(*csd).routine).CopyToEnd.unwrap()(cstate);

    MemoryContextDelete((*csd).rowcontext);

    if fe_copy {
        SendCopyEnd(cstate);
    }

    processed
}

/*
 * Emit one row during DoCopyTo().
 */
#[inline]
unsafe fn CopyOneRowTo(cstate: CopyToState, slot: *mut TupleTableSlot) {
    let csd = cs(cstate);
    let oldcontext: MemoryContext;

    MemoryContextReset((*csd).rowcontext);
    oldcontext = MemoryContextSwitchTo((*csd).rowcontext);

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    (*(*csd).routine).CopyToOneRow.unwrap()(cstate, slot);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
// #define DUMPSOFAR() do { if (ptr > start) CopySendData(cstate, start, ptr - start); } while (0)
macro_rules! DUMPSOFAR {
    ($cstate:expr, $start:expr, $ptr:expr) => {{
        if $ptr > $start {
            CopySendData(
                $cstate,
                $start as *const c_void,
                $ptr.offset_from($start) as c_int,
            );
        }
    }};
}

unsafe fn CopyAttributeOutText(cstate: CopyToState, string: *const c_char) {
    let csd = cs(cstate);
    let mut ptr: *const c_char;
    let mut start: *const c_char;
    let mut c: c_char;
    let delimc: c_char = *(*csd).opts.delim;

    if (*csd).need_transcoding {
        ptr = pg_server_to_any(string, strlen(string) as c_int, (*csd).file_encoding);
    } else {
        ptr = string;
    }

    /*
     * We have to grovel through the string searching for control characters
     * and instances of the delimiter character.  In most cases, though, these
     * are infrequent.  To avoid overhead from calling CopySendData once per
     * character, we dump out all characters between escaped characters in a
     * single call.  The loop invariant is that the data from "start" to "ptr"
     * can be sent literally, but hasn't yet been.
     *
     * We can skip pg_encoding_mblen() overhead when encoding is safe, because
     * in valid backend encodings, extra bytes of a multibyte character never
     * look like ASCII.  This loop is sufficiently performance-critical that
     * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
     * of the normal safe-encoding path.
     */
    if (*csd).encoding_embeds_ascii {
        start = ptr;
        while {
            c = *ptr;
            c != b'\0' as c_char
        } {
            if (c as u8) < 0x20u8 {
                /*
                 * \r and \n must be escaped, the others are traditional. We
                 * prefer to dump these using the C-like notation, rather than
                 * a backslash and the literal character, because it makes the
                 * dump file a bit more proof against Microsoftish data
                 * mangling.
                 */
                match c as u8 {
                    b'\x08' => c = b'b' as c_char,
                    b'\x0c' => c = b'f' as c_char,
                    b'\n' => c = b'n' as c_char,
                    b'\r' => c = b'r' as c_char,
                    b'\t' => c = b't' as c_char,
                    b'\x0b' => c = b'v' as c_char,
                    _ => {
                        /* If it's the delimiter, must backslash it */
                        if c == delimc {
                            // break out of match; fall through to escape
                        } else {
                            /* All ASCII control chars are length 1 */
                            ptr = ptr.add(1);
                            continue; /* fall to end of loop */
                        }
                    }
                }
                /* if we get here, we need to convert the control char */
                DUMPSOFAR!(cstate, start, ptr);
                CopySendChar(cstate, b'\\' as c_char);
                CopySendChar(cstate, c);
                ptr = ptr.add(1);
                start = ptr; /* do not include char in next run */
            } else if c == b'\\' as c_char || c == delimc {
                DUMPSOFAR!(cstate, start, ptr);
                CopySendChar(cstate, b'\\' as c_char);
                start = ptr; /* we include char in next run */
                ptr = ptr.add(1);
            } else if IS_HIGHBIT_SET(c as u8) {
                ptr = ptr.add(pg_encoding_mblen((*csd).file_encoding, ptr) as usize);
            } else {
                ptr = ptr.add(1);
            }
        }
    } else {
        start = ptr;
        while {
            c = *ptr;
            c != b'\0' as c_char
        } {
            if (c as u8) < 0x20u8 {
                /*
                 * \r and \n must be escaped, the others are traditional. We
                 * prefer to dump these using the C-like notation, rather than
                 * a backslash and the literal character, because it makes the
                 * dump file a bit more proof against Microsoftish data
                 * mangling.
                 */
                match c as u8 {
                    b'\x08' => c = b'b' as c_char,
                    b'\x0c' => c = b'f' as c_char,
                    b'\n' => c = b'n' as c_char,
                    b'\r' => c = b'r' as c_char,
                    b'\t' => c = b't' as c_char,
                    b'\x0b' => c = b'v' as c_char,
                    _ => {
                        /* If it's the delimiter, must backslash it */
                        if c == delimc {
                            // fall through to escape
                        } else {
                            /* All ASCII control chars are length 1 */
                            ptr = ptr.add(1);
                            continue; /* fall to end of loop */
                        }
                    }
                }
                /* if we get here, we need to convert the control char */
                DUMPSOFAR!(cstate, start, ptr);
                CopySendChar(cstate, b'\\' as c_char);
                CopySendChar(cstate, c);
                ptr = ptr.add(1);
                start = ptr; /* do not include char in next run */
            } else if c == b'\\' as c_char || c == delimc {
                DUMPSOFAR!(cstate, start, ptr);
                CopySendChar(cstate, b'\\' as c_char);
                start = ptr; /* we include char in next run */
                ptr = ptr.add(1);
            } else {
                ptr = ptr.add(1);
            }
        }
    }

    DUMPSOFAR!(cstate, start, ptr);
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
unsafe fn CopyAttributeOutCSV(cstate: CopyToState, string: *const c_char, mut use_quote: bool) {
    let csd = cs(cstate);
    let mut ptr: *const c_char;
    let mut start: *const c_char;
    let mut c: c_char;
    let delimc: c_char = *(*csd).opts.delim;
    let quotec: c_char = *(*csd).opts.quote;
    let escapec: c_char = *(*csd).opts.escape;
    let single_attr: bool = list_length((*csd).attnumlist) == 1;

    /* force quoting if it matches null_print (before conversion!) */
    if !use_quote && strcmp(string, (*csd).opts.null_print) == 0 {
        use_quote = true;
    }

    if (*csd).need_transcoding {
        ptr = pg_server_to_any(string, strlen(string) as c_int, (*csd).file_encoding);
    } else {
        ptr = string;
    }

    /*
     * Make a preliminary pass to discover if it needs quoting
     */
    if !use_quote {
        /*
         * Quote '\.' if it appears alone on a line, so that it will not be
         * interpreted as an end-of-data marker.  (PG 18 and up will not
         * interpret '\.' in CSV that way, except in embedded-in-SQL data; but
         * we want the data to be loadable by older versions too.  Also, this
         * avoids breaking clients that are still using PQgetline().)
         */
        if single_attr && strcmp(ptr, c"\\.".as_ptr()) == 0 {
            use_quote = true;
        } else {
            let mut tptr: *const c_char = ptr;

            while {
                c = *tptr;
                c != b'\0' as c_char
            } {
                if c == delimc || c == quotec || c == b'\n' as c_char || c == b'\r' as c_char {
                    use_quote = true;
                    break;
                }
                if IS_HIGHBIT_SET(c as u8) && (*csd).encoding_embeds_ascii {
                    tptr = tptr.add(pg_encoding_mblen((*csd).file_encoding, tptr) as usize);
                } else {
                    tptr = tptr.add(1);
                }
            }
        }
    }

    if use_quote {
        CopySendChar(cstate, quotec);

        /*
         * We adopt the same optimization strategy as in CopyAttributeOutText
         */
        start = ptr;
        while {
            c = *ptr;
            c != b'\0' as c_char
        } {
            if c == quotec || c == escapec {
                DUMPSOFAR!(cstate, start, ptr);
                CopySendChar(cstate, escapec);
                start = ptr; /* we include char in next run */
            }
            if IS_HIGHBIT_SET(c as u8) && (*csd).encoding_embeds_ascii {
                ptr = ptr.add(pg_encoding_mblen((*csd).file_encoding, ptr) as usize);
            } else {
                ptr = ptr.add(1);
            }
        }
        DUMPSOFAR!(cstate, start, ptr);

        CopySendChar(cstate, quotec);
    } else {
        /* If it doesn't need quoting, we can just dump it as-is */
        CopySendString(cstate, ptr);
    }
}

/*
 * copy_dest_startup --- executor startup
 */
unsafe fn copy_dest_startup(_self_: *mut DestReceiver, _operation: c_int, _typeinfo: TupleDesc) {
    /* no-op */
}

/*
 * copy_dest_receive --- receive one tuple
 */
unsafe fn copy_dest_receive(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState: *mut DR_copy = self_ as *mut DR_copy;
    let cstate: CopyToState = (*myState).cstate;

    /* Send the data */
    CopyOneRowTo(cstate, slot);

    /* Increment the number of processed tuples, and report the progress */
    (*myState).processed += 1;
    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, (*myState).processed as i64);

    true
}

/*
 * copy_dest_shutdown --- executor end
 */
unsafe fn copy_dest_shutdown(_self_: *mut DestReceiver) {
    /* no-op */
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
unsafe fn copy_dest_destroy(self_: *mut DestReceiver) {
    pfree(self_ as *mut c_void);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
pub unsafe fn CreateCopyDestReceiver() -> *mut DestReceiver {
    let self_: *mut DR_copy = palloc(core::mem::size_of::<DR_copy>()) as *mut DR_copy;

    (*self_).pub_.receiveSlot = Some(copy_dest_receive);
    (*self_).pub_.rStartup = Some(copy_dest_startup);
    (*self_).pub_.rShutdown = Some(copy_dest_shutdown);
    (*self_).pub_.rDestroy = Some(copy_dest_destroy);
    (*self_).pub_.mydest = DestCopyOut;

    (*self_).cstate = core::ptr::null_mut(); /* will be set later */
    (*self_).processed = 0;

    self_ as *mut DestReceiver
}
