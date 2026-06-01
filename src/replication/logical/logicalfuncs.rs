//! logicalfuncs.rs
//!   Support functions for using logical decoding and management of
//!   logical replication slots via SQL.
//!
//! Translated 1:1 from postgres/src/backend/replication/logical/logicalfuncs.c
//!
//! Copyright (c) 2012-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/replication/logical/logicalfuncs.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::{int32, int64, Size, TransactionId};
use crate::postgres_ext::Oid;

// access/xlogutils.h
use crate::access::transam::xlogutils::{read_local_xlog_page, wal_segment_close, wal_segment_open};
// nodes/makefuncs.h, nodes/value.h, nodes/pg_list.h, nodes/parsenodes.h
use crate::nodes::makefuncs::makeDefElem;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lappend, List};
use crate::nodes::value::makeString;
// replication/message.h
use crate::replication::message::LogLogicalMessage;
// utils/adt/pg_lsn.h
use crate::utils::adt::pg_lsn::LSNGetDatum;
// utils/adt/varlena.h
use crate::utils::adt::varlena::cstring_to_text_with_len;
// utils/builtins.h
use crate::utils::builtins::TextDatumGetCString;

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/* Private data for writing out data */
#[repr(C)]
pub struct DecodingOutputState {
    pub tupstore: *mut Tuplestorestate,
    pub tupdesc: TupleDesc,
    pub binary_output: bool,
    pub returned_rows: int64,
}

/*
 * Prepare for an output plugin write.
 */
unsafe fn LogicalOutputPrepareWrite(
    ctx: *mut LogicalDecodingContext,
    _lsn: XLogRecPtr,
    _xid: TransactionId,
    _last_write: bool,
) {
    resetStringInfo((*ctx).out);
}

/*
 * Perform output plugin write into tuplestore.
 */
unsafe fn LogicalOutputWrite(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    _last_write: bool,
) {
    let mut values: [Datum; 3] = [0; 3];
    let mut nulls: [bool; 3] = [false; 3];
    let p: *mut DecodingOutputState;

    /* SQL Datums can only be of a limited length... */
    if (*(*ctx).out).len > MaxAllocSize as c_int - VARHDRSZ as c_int {
        elog!(ERROR, "too much output for sql interface");
    }

    p = (*ctx).output_writer_private as *mut DecodingOutputState;

    memset(
        nulls.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of::<[bool; 3]>(),
    );
    values[0] = LSNGetDatum(lsn);
    values[1] = TransactionIdGetDatum(xid);

    /*
     * Assert ctx->out is in database encoding when we're writing textual
     * output.
     */
    if !(*p).binary_output {
        Assert!(pg_verify_mbstr(
            GetDatabaseEncoding(),
            (*(*ctx).out).data,
            (*(*ctx).out).len,
            false
        ));
    }

    /* ick, but cstring_to_text_with_len works for bytea perfectly fine */
    values[2] = PointerGetDatum(cstring_to_text_with_len((*(*ctx).out).data, (*(*ctx).out).len)
        as *const c_void);

    tuplestore_putvalues((*p).tupstore, (*p).tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    (*p).returned_rows += 1;
}

/*
 * Helper function for the various SQL callable logical decoding functions.
 */
unsafe fn pg_logical_slot_get_changes_guts(
    fcinfo: FunctionCallInfo,
    confirm: bool,
    binary: bool,
) -> Datum {
    let name: Name;
    let upto_lsn: XLogRecPtr;
    let upto_nchanges: int32;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let per_query_ctx: MemoryContext;
    let oldcontext: MemoryContext;
    let end_of_wal: XLogRecPtr;
    let wait_for_wal_lsn: XLogRecPtr;
    let ctx: *mut LogicalDecodingContext;
    let old_resowner: ResourceOwner = CurrentResourceOwner;
    let arr: *mut ArrayType;
    let ndim: Size;
    let mut options: *mut List = NIL as *mut List;
    let p: *mut DecodingOutputState;

    CheckSlotPermissions();

    CheckLogicalDecodingRequirements();

    if PG_ARGISNULL(fcinfo, 0) {
        ereport!(
            ERROR,
            errmsg!("slot name must not be null")
        );
    }
    name = PG_GETARG_NAME(fcinfo, 0);

    if PG_ARGISNULL(fcinfo, 1) {
        upto_lsn = InvalidXLogRecPtr;
    } else {
        upto_lsn = PG_GETARG_LSN(fcinfo, 1);
    }

    if PG_ARGISNULL(fcinfo, 2) {
        upto_nchanges = InvalidXLogRecPtr as int32;
    } else {
        upto_nchanges = PG_GETARG_INT32(fcinfo, 2);
    }

    if PG_ARGISNULL(fcinfo, 3) {
        ereport!(
            ERROR,
            errmsg!("options array must not be null")
        );
    }
    arr = PG_GETARG_ARRAYTYPE_P(fcinfo, 3);

    /* state to write output to */
    p = palloc0(std::mem::size_of::<DecodingOutputState>()) as *mut DecodingOutputState;

    (*p).binary_output = binary;

    per_query_ctx = (*(*rsinfo).econtext).ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx as crate::utils::palloc::MemoryContext);

    /* Deconstruct options array */
    ndim = ARR_NDIM(arr) as Size;
    if ndim > 1 {
        ereport!(
            ERROR,
            errmsg!("array must be one-dimensional")
        );
    } else if array_contains_nulls(arr) {
        ereport!(
            ERROR,
            errmsg!("array must not contain nulls")
        );
    } else if ndim == 1 {
        let nelems: c_int;
        let mut datum_opts: *mut Datum = null_mut();
        let mut i: c_int;

        Assert!(ARR_ELEMTYPE(arr) == TEXTOID);

        deconstruct_array_builtin(arr, TEXTOID, &mut datum_opts, null_mut(), &mut nelems);

        if nelems % 2 != 0 {
            ereport!(
                ERROR,
                errmsg!("array must have even number of elements")
            );
        }

        i = 0;
        while i < nelems {
            let optname: *mut c_char = TextDatumGetCString(*datum_opts.offset(i as isize));
            let opt: *mut c_char = TextDatumGetCString(*datum_opts.offset((i + 1) as isize));

            options = lappend(
                options,
                makeDefElem(optname, makeString(opt) as *mut Node, -1) as *mut c_void,
            );
            i += 2;
        }
    }

    InitMaterializedSRF(fcinfo, 0);
    (*p).tupstore = (*rsinfo).setResult;
    (*p).tupdesc = (*rsinfo).setDesc;

    /*
     * Compute the current end-of-wal.
     */
    if !RecoveryInProgress() {
        end_of_wal = GetFlushRecPtr(null_mut());
    } else {
        end_of_wal = GetXLogReplayRecPtr(null_mut());
    }

    ReplicationSlotAcquire(NameStr(*name), true, true);

    // PG_TRY();
    {
        /* restart at slot's confirmed_flush */
        ctx = CreateDecodingContext(
            InvalidXLogRecPtr,
            options,
            false,
            XL_ROUTINE(read_local_xlog_page, wal_segment_open, wal_segment_close),
            Some(LogicalOutputPrepareWrite),
            Some(LogicalOutputWrite),
            None,
        );

        MemoryContextSwitchTo(oldcontext);

        /*
         * Check whether the output plugin writes textual output if that's
         * what we need.
         */
        if !binary && (*ctx).options.output_type != OUTPUT_PLUGIN_TEXTUAL_OUTPUT {
            ereport!(
                ERROR,
                errmsg!(
                    "logical decoding output plugin \"{}\" produces binary output, but function \"{}\" expects textual data",
                    std::ffi::CStr::from_ptr(NameStr((*MyReplicationSlot).data.plugin)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_procedure((*(*fcinfo).flinfo).fn_oid)).to_string_lossy()
                )
            );
        }

        /*
         * Wait for specified streaming replication standby servers (if any)
         * to confirm receipt of WAL up to wait_for_wal_lsn.
         */
        if XLogRecPtrIsInvalid(upto_lsn) {
            wait_for_wal_lsn = end_of_wal;
        } else {
            wait_for_wal_lsn = Min(upto_lsn, end_of_wal);
        }

        WaitForStandbyConfirmation(wait_for_wal_lsn);

        (*ctx).output_writer_private = p as *mut c_void;

        /*
         * Decoding of WAL must start at restart_lsn so that the entirety of
         * xacts that committed after the slot's confirmed_flush can be
         * accumulated into reorder buffers.
         */
        XLogBeginRead((*ctx).reader, (*MyReplicationSlot).data.restart_lsn);

        /* invalidate non-timetravel entries */
        InvalidateSystemCaches();

        /* Decode until we run out of records */
        while (*(*ctx).reader).EndRecPtr < end_of_wal {
            let record: *mut XLogRecord;
            let mut errm: *mut c_char = null_mut();

            record = XLogReadRecord((*ctx).reader, &mut errm);
            if !errm.is_null() {
                elog!(
                    ERROR,
                    "could not find record for logical decoding: {}",
                    std::ffi::CStr::from_ptr(errm).to_string_lossy()
                );
            }

            /*
             * The {begin_txn,change,commit_txn}_wrapper callbacks above will
             * store the description into our tuplestore.
             */
            if !record.is_null() {
                LogicalDecodingProcessRecord(ctx, (*ctx).reader);
            }

            /* check limits */
            if upto_lsn != InvalidXLogRecPtr && upto_lsn <= (*(*ctx).reader).EndRecPtr {
                break;
            }
            if upto_nchanges != 0 && upto_nchanges as int64 <= (*p).returned_rows {
                break;
            }
            CHECK_FOR_INTERRUPTS();
        }

        /*
         * Logical decoding could have clobbered CurrentResourceOwner during
         * transaction management, so restore the executor's value.  (This is
         * a kluge, but it's not worth cleaning up right now.)
         */
        CurrentResourceOwner = old_resowner;

        /*
         * Next time, start where we left off. (Hunting things, the family
         * business..)
         */
        if (*(*ctx).reader).EndRecPtr != InvalidXLogRecPtr && confirm {
            LogicalConfirmReceivedLocation((*(*ctx).reader).EndRecPtr);

            /*
             * If only the confirmed_flush_lsn has changed the slot won't get
             * marked as dirty by the above. Callers on the walsender
             * interface are expected to keep track of their own progress and
             * don't need it written out. But SQL-interface users cannot
             * specify their own start positions and it's harder for them to
             * keep track of their progress, so we should make more of an
             * effort to save it for them.
             *
             * Dirty the slot so it's written out at the next checkpoint.
             * We'll still lose its position on crash, as documented, but it's
             * better than always losing the position even on clean restart.
             */
            ReplicationSlotMarkDirty();
        }

        /* free context, call shutdown callback */
        FreeDecodingContext(ctx);

        ReplicationSlotRelease();
        InvalidateSystemCaches();
    }
    // PG_CATCH();
    // {
    //     /* clear all timetravel entries */
    //     InvalidateSystemCaches();
    //
    //     PG_RE_THROW();
    // }
    // PG_END_TRY();

    0 as Datum
}

/*
 * SQL function returning the changestream as text, consuming the data.
 */
pub unsafe fn pg_logical_slot_get_changes(fcinfo: FunctionCallInfo) -> Datum {
    pg_logical_slot_get_changes_guts(fcinfo, true, false)
}

/*
 * SQL function returning the changestream as text, only peeking ahead.
 */
pub unsafe fn pg_logical_slot_peek_changes(fcinfo: FunctionCallInfo) -> Datum {
    pg_logical_slot_get_changes_guts(fcinfo, false, false)
}

/*
 * SQL function returning the changestream in binary, consuming the data.
 */
pub unsafe fn pg_logical_slot_get_binary_changes(fcinfo: FunctionCallInfo) -> Datum {
    pg_logical_slot_get_changes_guts(fcinfo, true, true)
}

/*
 * SQL function returning the changestream in binary, only peeking ahead.
 */
pub unsafe fn pg_logical_slot_peek_binary_changes(fcinfo: FunctionCallInfo) -> Datum {
    pg_logical_slot_get_changes_guts(fcinfo, false, true)
}

/*
 * SQL function for writing logical decoding message into WAL.
 */
pub unsafe fn pg_logical_emit_message_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let transactional: bool = PG_GETARG_BOOL(fcinfo, 0);
    let prefix: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP(fcinfo, 1));
    let data: *mut bytea = PG_GETARG_BYTEA_PP(fcinfo, 2);
    let flush: bool = PG_GETARG_BOOL(fcinfo, 3);
    let lsn: XLogRecPtr;

    lsn = LogLogicalMessage(
        prefix,
        VARDATA_ANY(data),
        VARSIZE_ANY_EXHDR(data),
        transactional,
        flush,
    );
    PG_RETURN_LSN!(lsn)
}

pub unsafe fn pg_logical_emit_message_text(fcinfo: FunctionCallInfo) -> Datum {
    /* bytea and text are compatible */
    pg_logical_emit_message_bytea(fcinfo)
}

// ---------------------------------------------------------------------------
// Local macro shims / helpers (mirroring sibling slotfuncs.rs conventions;
// not #[macro_export] to avoid colliding with same-named crate-root macros).
// ---------------------------------------------------------------------------

macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *mut c_char
    };
}
use NameStr;

macro_rules! Min {
    ($a:expr, $b:expr) => {
        if $a < $b {
            $a
        } else {
            $b
        }
    };
}
use Min;

macro_rules! PG_RETURN_LSN {
    ($x:expr) => {
        return ($x) as Datum
    };
}
use PG_RETURN_LSN;

#[allow(non_snake_case)]
fn XLogRecPtrIsInvalid(lsn: XLogRecPtr) -> bool {
    lsn == InvalidXLogRecPtr
}

// ARR_* varlena array accessors (utils/array.h).
#[allow(non_snake_case)]
unsafe fn ARR_NDIM(_arr: *mut ArrayType) -> c_int {
    unimplemented!() // TODO(pg-port): utils/array.h ARR_NDIM lives in utils/adt/array
}
#[allow(non_snake_case)]
unsafe fn ARR_ELEMTYPE(_arr: *mut ArrayType) -> Oid {
    unimplemented!() // TODO(pg-port): utils/array.h ARR_ELEMTYPE lives in utils/adt/array
}

// VARDATA_ANY / VARSIZE_ANY_EXHDR (c.h / postgres.h varlena access).
#[allow(non_snake_case)]
unsafe fn VARDATA_ANY(_ptr: *mut bytea) -> *mut c_char {
    unimplemented!() // TODO(pg-port): postgres.h VARDATA_ANY
}
#[allow(non_snake_case)]
unsafe fn VARSIZE_ANY_EXHDR(_ptr: *mut bytea) -> Size {
    unimplemented!() // TODO(pg-port): postgres.h VARSIZE_ANY_EXHDR
}

// ---------------------------------------------------------------------------
// Type aliases / external dependencies (stubbed where not yet ported).
// ---------------------------------------------------------------------------

// utils/array.h
pub enum ArrayType {}
// utils/sort/tuplestore.h
pub enum Tuplestorestate {}
// access/common/tupdesc.h
#[allow(non_camel_case_types)]
pub type TupleDesc = *mut c_void;
// utils/adt/varlena.h
#[allow(non_camel_case_types)]
pub type text = c_void;
#[allow(non_camel_case_types)]
pub type bytea = c_void;
// utils/resowner.h
#[allow(non_camel_case_types)]
pub type ResourceOwner = *mut c_void;
// access/xlogreader.h
#[allow(non_camel_case_types)]
pub type XLogReadPageCB = *mut c_void;
pub enum XLogRecord {}

#[repr(C)]
pub struct XLogReaderState {
    pub EndRecPtr: XLogRecPtr,
}

// fmgr.h - composite function-call info (only fields we touch).
#[repr(C)]
pub struct FmgrInfo {
    pub fn_oid: Oid,
}
#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub flinfo: *mut FmgrInfo,
    pub resultinfo: *mut c_void,
}
#[allow(non_camel_case_types)]
pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;

// nodes/execnodes.h - ReturnSetInfo / ExprContext (only fields we touch).
#[repr(C)]
pub struct ExprContext {
    pub ecxt_per_query_memory: MemoryContext,
}
#[repr(C)]
pub struct ReturnSetInfo {
    pub econtext: *mut ExprContext,
    pub setResult: *mut Tuplestorestate,
    pub setDesc: TupleDesc,
}

// lib/stringinfo.h
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
#[allow(non_camel_case_types)]
pub type StringInfo = *mut StringInfoData;

// replication/logical.h - OutputPluginOptions / output plugin callbacks.
#[allow(non_camel_case_types)]
pub type OutputPluginOutputType = c_int;
pub const OUTPUT_PLUGIN_TEXTUAL_OUTPUT: OutputPluginOutputType = 1;

#[repr(C)]
pub struct OutputPluginOptions {
    pub output_type: OutputPluginOutputType,
    pub receive_rewrites: bool,
}

// replication/logical.h - LogicalDecodingContext (only fields we touch).
#[repr(C)]
pub struct LogicalDecodingContext {
    pub out: StringInfo,
    pub reader: *mut XLogReaderState,
    pub options: OutputPluginOptions,
    pub output_writer_private: *mut c_void,
}

// replication/slot.h - ReplicationSlot (only fields we touch).
#[repr(C)]
pub struct ReplicationSlotPersistentData {
    pub plugin: NameData,
    pub restart_lsn: XLogRecPtr,
}
#[repr(C)]
pub struct ReplicationSlot {
    pub data: ReplicationSlotPersistentData,
}

// c.h - NameData / Name.
#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}
#[allow(non_camel_case_types)]
pub type Name = *mut NameData;

// Output plugin write callback signatures.
#[allow(non_camel_case_types)]
pub type LogicalOutputPluginWriterPrepareWrite =
    unsafe fn(*mut LogicalDecodingContext, XLogRecPtr, TransactionId, bool);
#[allow(non_camel_case_types)]
pub type LogicalOutputPluginWriterWrite =
    unsafe fn(*mut LogicalDecodingContext, XLogRecPtr, TransactionId, bool);
#[allow(non_camel_case_types)]
pub type LogicalOutputPluginWriterUpdateProgress =
    unsafe fn(*mut LogicalDecodingContext, XLogRecPtr, TransactionId, bool);

// Constants
#[allow(non_upper_case_globals)]
pub const NIL: *mut c_void = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

// Globals (stubs)
#[allow(non_upper_case_globals)]
pub static mut MyReplicationSlot: *mut ReplicationSlot = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
pub static mut CurrentResourceOwner: ResourceOwner = std::ptr::null_mut();

// ---------------------------------------------------------------------------
// Function stubs (symbols not yet ported elsewhere in the tree).
// ---------------------------------------------------------------------------

unsafe fn CheckSlotPermissions() {
    unimplemented!() // TODO(pg-port): real CheckSlotPermissions lives in replication/slot.c
}
unsafe fn CheckLogicalDecodingRequirements() {
    unimplemented!() // TODO(pg-port): real CheckLogicalDecodingRequirements lives in replication/logical/logical.c
}
unsafe fn ReplicationSlotAcquire(_name: *mut c_char, _nowait: bool, _error_if_invalid: bool) {
    unimplemented!() // TODO(pg-port): real ReplicationSlotAcquire lives in replication/slot.c
}
unsafe fn ReplicationSlotRelease() {
    unimplemented!() // TODO(pg-port): real ReplicationSlotRelease lives in replication/slot.c
}
unsafe fn ReplicationSlotMarkDirty() {
    unimplemented!() // TODO(pg-port): real ReplicationSlotMarkDirty lives in replication/slot.c
}
unsafe fn CreateDecodingContext(
    _start_lsn: XLogRecPtr,
    _output_plugin_options: *mut List,
    _need_full_snapshot: bool,
    _xl_routine: *mut c_void,
    _prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    _do_write: Option<LogicalOutputPluginWriterWrite>,
    _update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> *mut LogicalDecodingContext {
    unimplemented!() // TODO(pg-port): real CreateDecodingContext lives in replication/logical/logical.c
}
unsafe fn FreeDecodingContext(_ctx: *mut LogicalDecodingContext) {
    unimplemented!() // TODO(pg-port): real FreeDecodingContext lives in replication/logical/logical.c
}
unsafe fn LogicalDecodingProcessRecord(
    _ctx: *mut LogicalDecodingContext,
    _record: *mut XLogReaderState,
) {
    unimplemented!() // TODO(pg-port): real LogicalDecodingProcessRecord lives in replication/logical/decode.c
}
unsafe fn LogicalConfirmReceivedLocation(_lsn: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real LogicalConfirmReceivedLocation lives in replication/logical/logical.c
}
unsafe fn WaitForStandbyConfirmation(_wait_for_lsn: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real WaitForStandbyConfirmation lives in replication/walsender.c
}
unsafe fn InvalidateSystemCaches() {
    unimplemented!() // TODO(pg-port): real InvalidateSystemCaches lives in utils/cache/inval.c
}
unsafe fn RecoveryInProgress() -> bool {
    unimplemented!() // TODO(pg-port): real RecoveryInProgress lives in access/transam/xlog.c
}
unsafe fn GetFlushRecPtr(_insertTLI: *mut c_void) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real GetFlushRecPtr lives in access/transam/xlog.c
}
unsafe fn GetXLogReplayRecPtr(_replayTLI: *mut c_void) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real GetXLogReplayRecPtr lives in access/transam/xlogrecovery.c
}
unsafe fn XLogBeginRead(_state: *mut XLogReaderState, _rec_ptr: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real XLogBeginRead lives in access/transam/xlogreader.c
}
unsafe fn XLogReadRecord(
    _state: *mut XLogReaderState,
    _errormsg: *mut *mut c_char,
) -> *mut XLogRecord {
    unimplemented!() // TODO(pg-port): real XLogReadRecord lives in access/transam/xlogreader.c
}
unsafe fn array_contains_nulls(_array: *mut ArrayType) -> bool {
    unimplemented!() // TODO(pg-port): real array_contains_nulls lives in utils/adt/arrayfuncs.c
}
unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() // TODO(pg-port): real deconstruct_array_builtin lives in utils/adt/arrayfuncs.c
}
unsafe fn resetStringInfo(_str: StringInfo) {
    unimplemented!() // TODO(pg-port): real resetStringInfo lives in lib/stringinfo.c
}
unsafe fn pg_verify_mbstr(
    _encoding: c_int,
    _mbstr: *const c_char,
    _len: c_int,
    _noError: bool,
) -> bool {
    unimplemented!() // TODO(pg-port): real pg_verify_mbstr lives in mb/mbutils.c
}
unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!() // TODO(pg-port): real GetDatabaseEncoding lives in mb/mbutils.c
}
unsafe fn format_procedure(_procedure_oid: Oid) -> *mut c_char {
    unimplemented!() // TODO(pg-port): real format_procedure lives in utils/adt/regproc.c
}
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO(pg-port): real InitMaterializedSRF lives in utils/fmgr/funcapi.c
}
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.c
}
unsafe fn text_to_cstring(_t: *const text) -> *mut c_char {
    unimplemented!() // TODO(pg-port): real text_to_cstring lives in utils/adt/varlena.c
}

// XL_ROUTINE(.page_read = ..., .segment_open = ..., .segment_close = ...)
// builds an anonymous XLogReaderRoutine; stubbed pending access/xlogreader.h.
#[allow(non_snake_case)]
unsafe fn XL_ROUTINE(
    _page_read: XLogReadPageCB,
    _segment_open: unsafe extern "C" fn(),
    _segment_close: unsafe extern "C" fn(),
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real XL_ROUTINE lives in access/xlogreader.h
}

// fmgr.h PG_GETARG_* / CHECK_FOR_INTERRUPTS shims (not yet provided by the
// ported fmgr macro set).
unsafe fn PG_ARGISNULL(_fcinfo: FunctionCallInfo, _n: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real PG_ARGISNULL lives in fmgr.h
}
unsafe fn PG_GETARG_NAME(_fcinfo: FunctionCallInfo, _n: c_int) -> Name {
    unimplemented!() // TODO(pg-port): real PG_GETARG_NAME lives in fmgr.h
}
unsafe fn PG_GETARG_LSN(_fcinfo: FunctionCallInfo, _n: c_int) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real PG_GETARG_LSN lives in utils/pg_lsn.h
}
unsafe fn PG_GETARG_INT32(_fcinfo: FunctionCallInfo, _n: c_int) -> int32 {
    unimplemented!() // TODO(pg-port): real PG_GETARG_INT32 lives in fmgr.h
}
unsafe fn PG_GETARG_BOOL(_fcinfo: FunctionCallInfo, _n: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real PG_GETARG_BOOL lives in fmgr.h
}
unsafe fn PG_GETARG_ARRAYTYPE_P(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): real PG_GETARG_ARRAYTYPE_P lives in utils/array.h
}
unsafe fn PG_GETARG_TEXT_PP(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut text {
    unimplemented!() // TODO(pg-port): real PG_GETARG_TEXT_PP lives in fmgr.h
}
unsafe fn PG_GETARG_BYTEA_PP(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut bytea {
    unimplemented!() // TODO(pg-port): real PG_GETARG_BYTEA_PP lives in fmgr.h
}
#[allow(non_snake_case)]
unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): real CHECK_FOR_INTERRUPTS lives in miscadmin.h
}
