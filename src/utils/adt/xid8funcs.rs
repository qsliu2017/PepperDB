//! src/backend/utils/adt/xid8funcs.c
//!
//! Export internal transaction IDs to user level.
//!
//! Note that only top-level transaction IDs are exposed to user sessions.
//! This is important because xid8s frequently persist beyond the global
//! xmin horizon, or may even be shipped to other machines, so we cannot
//! rely on being able to correlate subtransaction IDs with their parents
//! via functions such as SubTransGetTopmostTransaction().
//!
//! These functions are used to support the txid_XXX functions and the newer
//! pg_current_xact_id, pg_current_snapshot and related fmgr functions, since
//! the only difference between them is whether they expose xid8 or int8 values
//! to users.  The txid_XXX variants should eventually be dropped.
//!
//!
//!	Copyright (c) 2003-2025, PostgreSQL Global Development Group
//!	Author: Jan Wieck, Afilias USA INC.
//!	64-bit txids: Marko Kreen, Skype Technologies
//!
//!	src/backend/utils/adt/xid8funcs.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int32, int64, uint32, uint64, Size, TransactionId};

// Full transaction ID type and arithmetic helpers (access/transam.h).
use crate::access::transam::{
    FullTransactionId, FullTransactionIdEquals, FullTransactionIdFollowsOrEquals,
    FullTransactionIdFromU64, FullTransactionIdIsValid, FullTransactionIdPrecedes,
    InvalidFullTransactionId, TransactionIdIsNormal, TransactionIdIsValid,
    U64FromFullTransactionId, XidFromFullTransactionId,
};

// xid8 <-> Datum conversions (utils/xid8.h).
use crate::utils::xid8::{DatumGetFullTransactionId, FullTransactionIdGetDatum};

// Snapshot (utils/snapshot.h).
use crate::utils::snapshot::{Snapshot, SnapshotData};

// StringInfo machinery (lib/stringinfo.h).
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, initStringInfo, makeStringInfo, StringInfo,
    StringInfoData,
};

// Node (nodes/nodes.h) -- used by the escontext soft-error path.
use crate::nodes::nodes::Node;

// fmgr call info (fmgr.h).
use crate::utils::fmgr::FunctionCallInfo;

// varlena size accessors (varatt.h).
use crate::varatt::{SET_VARSIZE, VARSIZE};

// pqformat send/recv helpers (libpq/pqformat.h).
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_getmsgint64, pq_sendint32, pq_sendint64,
};

// #[macro_export] macros live at the crate root.
use crate::{appendStringInfo, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_CSTRING, PG_RETURN_BYTEA_P, PG_RETURN_BOOL, PG_GETARG_POINTER, PG_GETARG_CSTRING, PG_GETARG_OID, PG_GETARG_INT32, PG_ARGISNULL};

/* ---------------------------------------------------------------------------
 * Local shims for fmgr/funcapi/lwlock helpers that are spelled with
 * function-call syntax in the translated source but are macros (or are not
 * yet ported) upstream.  These keep the translation 1:1 without changing the
 * logic.
 * ------------------------------------------------------------------------- */

// Assert(cond) is a macro upstream but is invoked here with call syntax.
#[inline]
unsafe fn Assert(_cond: bool) {
    debug_assert!(_cond);
}

// PG_GETARG_* wrappers (fmgr.h macros invoked with call syntax).
#[inline]
unsafe fn PG_GETARG_CSTRING(fcinfo: FunctionCallInfo, n: c_int) -> *mut c_char {
    crate::PG_GETARG_CSTRING!(fcinfo, n)
}
#[inline]
unsafe fn PG_GETARG_POINTER(fcinfo: FunctionCallInfo, n: c_int) -> *mut c_void {
    crate::PG_GETARG_POINTER!(fcinfo, n)
}
#[inline]
unsafe fn PG_GETARG_VARLENA_P(fcinfo: FunctionCallInfo, n: c_int) -> *mut c_void {
    crate::PG_GETARG_VARLENA_P!(fcinfo, n) as *mut c_void
}
#[inline]
unsafe fn PG_GETARG_FULLTRANSACTIONID(fcinfo: FunctionCallInfo, n: c_int) -> FullTransactionId {
    DatumGetFullTransactionId(crate::PG_GETARG_DATUM!(fcinfo, n))
}

// PG_RETURN_* wrappers (fmgr.h macros invoked with call syntax, here used as
// the tail expression of an extern "C" fn -> Datum).
#[inline]
unsafe fn PG_RETURN_POINTER(x: *mut c_void) -> Datum {
    PointerGetDatum(x as *const c_void)
}
#[inline]
unsafe fn PG_RETURN_BOOL(x: bool) -> Datum {
    BoolGetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_CSTRING(x: *mut c_char) -> Datum {
    CStringGetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_TEXT_P(x: *mut c_void) -> Datum {
    PointerGetDatum(x as *const c_void)
}
#[inline]
unsafe fn PG_RETURN_BYTEA_P(x: *mut c_void) -> Datum {
    PointerGetDatum(x as *const c_void)
}
#[inline]
unsafe fn PG_RETURN_FULLTRANSACTIONID(x: FullTransactionId) -> Datum {
    FullTransactionIdGetDatum(x)
}

// FuncCallContext (funcapi.h) -- minimal layout matching the fields used here.
#[repr(C)]
struct FuncCallContext {
    call_cntr: u64,
    max_calls: u64,
    user_fctx: *mut c_void,
    attinmeta: *mut c_void,
    multi_call_memory_ctx: MemoryContext,
    tuple_desc: *mut c_void,
}

// SRF support (funcapi.h) -- not yet ported; stubbed locally.
unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(
    _fcinfo: FunctionCallInfo,
    _fctx: *mut FuncCallContext,
    _result: Datum,
) -> Datum {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fcinfo: FunctionCallInfo, _fctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}

macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => {
        srf_is_firstcall($fcinfo)
    };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => {
        srf_firstcall_init($fcinfo)
    };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => {
        srf_percall_setup($fcinfo)
    };
}
macro_rules! SRF_RETURN_NEXT {
    ($fcinfo:expr, $fctx:expr, $result:expr) => {
        return srf_return_next($fcinfo, $fctx, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($fcinfo:expr, $fctx:expr) => {
        return srf_return_done($fcinfo, $fctx)
    };
}

// ereturn(escontext, dummy, ...) soft-error macro (elog.h) -- not yet ported.
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{
        let _ = &$escontext;
        $crate::utils::elog::emit_log(ERROR, &format!($($arg)*), file!(), line!());
        return $dummy;
    }};
}

// LWLock machinery (storage/lwlock.h) -- not yet ported; stubbed locally.
type LWLock = c_void;
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
enum LWMode {
    LW_SHARED,
}
static mut XactTruncationLock: *mut LWLock = std::ptr::null_mut();
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: LWMode) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

// TransamVariables shared state (access/transam.h) -- not yet ported.
#[repr(C)]
struct VariableCacheData {
    oldestClogXid: TransactionId,
}
#[allow(non_upper_case_globals)]
static mut TransamVariables: *mut VariableCacheData = std::ptr::null_mut();

/*
 * If defined, use bsearch() function for searching for xid8s in snapshots
 * that have more than the specified number of values.
 */
const USE_BSEARCH_IF_NXIP_GREATER: u32 = 30;

/*
 * Snapshot containing FullTransactionIds.
 */
#[repr(C)]
struct pg_snapshot {
    /*
     * 4-byte length hdr, should not be touched directly.
     *
     * Explicit embedding is ok as we want always correct alignment anyway.
     */
    __varsz: int32,

    nxip: uint32, /* number of fxids in xip array */
    xmin: FullTransactionId,
    xmax: FullTransactionId,
    /* in-progress fxids, xmin <= xip[i] < xmax: */
    xip: [FullTransactionId; crate::c::FLEXIBLE_ARRAY_MEMBER],
}

#[inline]
fn PG_SNAPSHOT_SIZE(nxip: usize) -> usize {
    core::mem::offset_of!(pg_snapshot, xip) + std::mem::size_of::<FullTransactionId>() * nxip
}

#[inline]
fn PG_SNAPSHOT_MAX_NXIP() -> usize {
    (MaxAllocSize - core::mem::offset_of!(pg_snapshot, xip)) / std::mem::size_of::<FullTransactionId>()
}

/*
 * Compile-time limits on the procarray (MAX_BACKENDS processes plus
 * MAX_BACKENDS prepared transactions) guarantee nxip won't be too large.
 *
 * StaticAssertDecl(MAX_BACKENDS * 2 <= PG_SNAPSHOT_MAX_NXIP,
 *                  "possible overflow in pg_current_snapshot()");
 */

/*
 * Helper to get a TransactionId from a 64-bit xid with wraparound detection.
 *
 * It is an ERROR if the xid is in the future.  Otherwise, returns true if
 * the transaction is still new enough that we can determine whether it
 * committed and false otherwise.  If *extracted_xid is not NULL, it is set
 * to the low 32 bits of the transaction ID (i.e. the actual XID, without the
 * epoch).
 *
 * The caller must hold XactTruncationLock since it's dealing with arbitrary
 * XIDs, and must continue to hold it until it's done with any clog lookups
 * relating to those XIDs.
 */
unsafe fn TransactionIdInRecentPast(
    fxid: FullTransactionId,
    extracted_xid: *mut TransactionId,
) -> bool {
    let xid: TransactionId = XidFromFullTransactionId(fxid);
    let now_fullxid: FullTransactionId;
    let oldest_clog_xid: TransactionId;
    let oldest_clog_fxid: FullTransactionId;

    now_fullxid = ReadNextFullTransactionId();

    if !extracted_xid.is_null() {
        *extracted_xid = xid;
    }

    if !TransactionIdIsValid(xid) {
        return false;
    }

    /* For non-normal transaction IDs, we can ignore the epoch. */
    if !TransactionIdIsNormal(xid) {
        return true;
    }

    /* If the transaction ID is in the future, throw an error. */
    if !FullTransactionIdPrecedes(fxid, now_fullxid) {
        elog!(
            ERROR,
            "transaction ID {} is in the future",
            U64FromFullTransactionId(fxid)
        );
        unreachable!();
    }

    /*
     * TransamVariables->oldestClogXid is protected by XactTruncationLock, but
     * we don't acquire that lock here.  Instead, we require the caller to
     * acquire it, because the caller is presumably going to look up the
     * returned XID.  If we took and released the lock within this function, a
     * CLOG truncation could occur before the caller finished with the XID.
     */
    Assert(LWLockHeldByMe(XactTruncationLock));

    /*
     * If fxid is not older than TransamVariables->oldestClogXid, the relevant
     * CLOG entry is guaranteed to still exist.
     *
     * TransamVariables->oldestXid governs allowable XIDs.  Usually,
     * oldestClogXid==oldestXid.  It's also possible for oldestClogXid to
     * follow oldestXid, in which case oldestXid might advance after our
     * ReadNextFullTransactionId() call.  If oldestXid has advanced, that
     * advancement reinstated the usual oldestClogXid==oldestXid.  Whether or
     * not that happened, oldestClogXid is allowable relative to now_fullxid.
     */
    oldest_clog_xid = (*TransamVariables).oldestClogXid;
    oldest_clog_fxid = FullTransactionIdFromAllowableAt(now_fullxid, oldest_clog_xid);
    !FullTransactionIdPrecedes(fxid, oldest_clog_fxid)
}

/*
 * txid comparator for qsort/bsearch
 */
unsafe extern "C" fn cmp_fxid(aa: *const c_void, bb: *const c_void) -> c_int {
    let a: FullTransactionId = *(aa as *const FullTransactionId);
    let b: FullTransactionId = *(bb as *const FullTransactionId);

    if FullTransactionIdPrecedes(a, b) {
        return -1;
    }
    if FullTransactionIdPrecedes(b, a) {
        return 1;
    }
    0
}

/*
 * Sort a snapshot's txids, so we can use bsearch() later.  Also remove
 * any duplicates.
 *
 * For consistency of on-disk representation, we always sort even if bsearch
 * will not be used.
 */
unsafe fn sort_snapshot(snap: *mut pg_snapshot) {
    if (*snap).nxip > 1 {
        qsort(
            (*snap).xip.as_mut_ptr() as *mut c_void,
            (*snap).nxip as usize,
            std::mem::size_of::<FullTransactionId>(),
            cmp_fxid,
        );
        (*snap).nxip = qunique(
            (*snap).xip.as_mut_ptr() as *mut c_void,
            (*snap).nxip as usize,
            std::mem::size_of::<FullTransactionId>(),
            cmp_fxid,
        ) as uint32;
    }
}

/*
 * check fxid visibility.
 */
unsafe fn is_visible_fxid(value: FullTransactionId, snap: *const pg_snapshot) -> bool {
    if FullTransactionIdPrecedes(value, (*snap).xmin) {
        true
    } else if !FullTransactionIdPrecedes(value, (*snap).xmax) {
        false
    } else if (*snap).nxip > USE_BSEARCH_IF_NXIP_GREATER {
        let res: *const c_void;

        res = bsearch(
            &value as *const FullTransactionId as *const c_void,
            (*snap).xip.as_ptr() as *const c_void,
            (*snap).nxip as usize,
            std::mem::size_of::<FullTransactionId>(),
            cmp_fxid,
        );
        /* if found, transaction is still in progress */
        res.is_null()
    } else {
        let mut i: uint32 = 0;

        while i < (*snap).nxip {
            if FullTransactionIdEquals(value, *(*snap).xip.as_ptr().add(i as usize)) {
                return false;
            }
            i += 1;
        }
        true
    }
}

/*
 * helper functions to use StringInfo for pg_snapshot creation.
 */

unsafe fn buf_init(xmin: FullTransactionId, xmax: FullTransactionId) -> StringInfo {
    let mut snap: pg_snapshot = std::mem::zeroed();
    let buf: StringInfo;

    snap.xmin = xmin;
    snap.xmax = xmax;
    snap.nxip = 0;

    buf = makeStringInfo();
    appendBinaryStringInfo(
        buf,
        &snap as *const pg_snapshot as *const c_char,
        PG_SNAPSHOT_SIZE(0) as c_int,
    );
    buf
}

unsafe fn buf_add_txid(buf: StringInfo, fxid: FullTransactionId) {
    let snap: *mut pg_snapshot = (*buf).data as *mut pg_snapshot;

    /* do this before possible realloc */
    (*snap).nxip += 1;

    appendBinaryStringInfo(
        buf,
        &fxid as *const FullTransactionId as *const c_char,
        std::mem::size_of::<FullTransactionId>() as c_int,
    );
}

unsafe fn buf_finalize(buf: StringInfo) -> *mut pg_snapshot {
    let snap: *mut pg_snapshot = (*buf).data as *mut pg_snapshot;

    SET_VARSIZE(snap as *mut c_void, (*buf).len);

    /* buf is not needed anymore */
    (*buf).data = std::ptr::null_mut();
    pfree(buf as *mut c_void);

    snap
}

/*
 * parse snapshot from cstring
 */
unsafe fn parse_snapshot(str_arg: *const c_char, escontext: *mut Node) -> *mut pg_snapshot {
    let xmin: FullTransactionId;
    let xmax: FullTransactionId;
    let mut last_val: FullTransactionId = InvalidFullTransactionId;
    let mut val: FullTransactionId;
    let str_start: *const c_char = str_arg;
    let mut endp: *mut c_char = std::ptr::null_mut();
    let buf: StringInfo;

    let mut str = str_arg;

    'bad_format: {
        xmin = FullTransactionIdFromU64(strtou64(str, &mut endp, 10));
        if *endp != b':' as c_char {
            break 'bad_format;
        }
        str = endp.add(1);

        xmax = FullTransactionIdFromU64(strtou64(str, &mut endp, 10));
        if *endp != b':' as c_char {
            break 'bad_format;
        }
        str = endp.add(1);

        /* it should look sane */
        if !FullTransactionIdIsValid(xmin)
            || !FullTransactionIdIsValid(xmax)
            || FullTransactionIdPrecedes(xmax, xmin)
        {
            break 'bad_format;
        }

        /* allocate buffer */
        buf = buf_init(xmin, xmax);

        /* loop over values */
        while *str != b'\0' as c_char {
            /* read next value */
            val = FullTransactionIdFromU64(strtou64(str, &mut endp, 10));
            str = endp;

            /* require the input to be in order */
            if FullTransactionIdPrecedes(val, xmin)
                || FullTransactionIdFollowsOrEquals(val, xmax)
                || FullTransactionIdPrecedes(val, last_val)
            {
                break 'bad_format;
            }

            /* skip duplicates */
            if !FullTransactionIdEquals(val, last_val) {
                buf_add_txid(buf, val);
            }
            last_val = val;

            if *str == b',' as c_char {
                str = str.add(1);
            } else if *str != b'\0' as c_char {
                break 'bad_format;
            }
        }

        return buf_finalize(buf);
    }

    // bad_format:
    ereturn!(
        escontext,
        std::ptr::null_mut(),
        "invalid input syntax for type {}: \"{}\"",
        "pg_snapshot",
        str_start
    );
}

/*
 * pg_current_xact_id() returns xid8
 *
 *	Return the current toplevel full transaction ID.
 *	If the current transaction does not have one, one is assigned.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_current_xact_id(_fcinfo: FunctionCallInfo) -> Datum {
    /*
     * Must prevent during recovery because if an xid is not assigned we try
     * to assign one, which would fail. Programs already rely on this function
     * to always return a valid current xid, so we should not change this to
     * return NULL or similar invalid xid.
     */
    PreventCommandDuringRecovery(c"pg_current_xact_id()".as_ptr());

    PG_RETURN_FULLTRANSACTIONID(GetTopFullTransactionId())
}

/*
 * Same as pg_current_xact_id() but doesn't assign a new xid if there
 * isn't one yet.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_current_xact_id_if_assigned(fcinfo: FunctionCallInfo) -> Datum {
    let topfxid: FullTransactionId = GetTopFullTransactionIdIfAny();

    if !FullTransactionIdIsValid(topfxid) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_FULLTRANSACTIONID(topfxid)
}

/*
 * pg_current_snapshot() returns pg_snapshot
 *
 *		Return current snapshot
 *
 * Note that only top-transaction XIDs are included in the snapshot.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_current_snapshot(_fcinfo: FunctionCallInfo) -> Datum {
    let snap: *mut pg_snapshot;
    let nxip: uint32;
    let mut i: uint32;
    let cur: Snapshot;
    let next_fxid: FullTransactionId = ReadNextFullTransactionId();

    cur = GetActiveSnapshot();
    if cur.is_null() {
        elog!(ERROR, "no active snapshot set");
        unreachable!();
    }

    /* allocate */
    nxip = (*cur).xcnt;
    snap = palloc(PG_SNAPSHOT_SIZE(nxip as usize)) as *mut pg_snapshot;

    /*
     * Fill.  This is the current backend's active snapshot, so MyProc->xmin
     * is <= all these XIDs.  As long as that remains so, oldestXid can't
     * advance past any of these XIDs.  Hence, these XIDs remain allowable
     * relative to next_fxid.
     */
    (*snap).xmin = FullTransactionIdFromAllowableAt(next_fxid, (*cur).xmin);
    (*snap).xmax = FullTransactionIdFromAllowableAt(next_fxid, (*cur).xmax);
    (*snap).nxip = nxip;
    i = 0;
    while i < nxip {
        *(*snap).xip.as_mut_ptr().add(i as usize) =
            FullTransactionIdFromAllowableAt(next_fxid, *(*cur).xip.add(i as usize));
        i += 1;
    }

    /*
     * We want them guaranteed to be in ascending order.  This also removes
     * any duplicate xids.  Normally, an XID can only be assigned to one
     * backend, but when preparing a transaction for two-phase commit, there
     * is a transient state when both the original backend and the dummy
     * PGPROC entry reserved for the prepared transaction hold the same XID.
     */
    sort_snapshot(snap);

    /* set size after sorting, because it may have removed duplicate xips */
    SET_VARSIZE(snap as *mut c_void, PG_SNAPSHOT_SIZE((*snap).nxip as usize) as int32);

    PG_RETURN_POINTER!(snap as *mut c_void)
}

/*
 * pg_snapshot_in(cstring) returns pg_snapshot
 *
 *		input function for type pg_snapshot
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_in(fcinfo: FunctionCallInfo) -> Datum {
    let str_arg: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let snap: *mut pg_snapshot;

    snap = parse_snapshot(str_arg, (*fcinfo).context);

    PG_RETURN_POINTER!(snap as *mut c_void)
}

/*
 * pg_snapshot_out(pg_snapshot) returns cstring
 *
 *		output function for type pg_snapshot
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_out(fcinfo: FunctionCallInfo) -> Datum {
    let snap: *mut pg_snapshot = PG_GETARG_VARLENA_P(fcinfo, 0) as *mut pg_snapshot;
    let mut str: StringInfoData = std::mem::zeroed();
    let mut i: uint32;

    initStringInfo(&mut str);

    appendStringInfo!(
        &mut str,
        "{}:",
        U64FromFullTransactionId((*snap).xmin)
    );
    appendStringInfo!(
        &mut str,
        "{}:",
        U64FromFullTransactionId((*snap).xmax)
    );

    i = 0;
    while i < (*snap).nxip {
        if i > 0 {
            appendStringInfoChar(&mut str, b',' as c_char);
        }
        appendStringInfo!(
            &mut str,
            "{}",
            U64FromFullTransactionId(*(*snap).xip.as_ptr().add(i as usize))
        );
        i += 1;
    }

    PG_RETURN_CSTRING!(str.data)
}

/*
 * pg_snapshot_recv(internal) returns pg_snapshot
 *
 *		binary input function for type pg_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let snap: *mut pg_snapshot;
    let mut last: FullTransactionId = InvalidFullTransactionId;
    let mut nxip: c_int;
    let mut i: c_int;
    let xmin: FullTransactionId;
    let xmax: FullTransactionId;

    'bad_format: {
        /* load and validate nxip */
        nxip = pq_getmsgint(buf, 4);
        if nxip < 0 || nxip as usize > PG_SNAPSHOT_MAX_NXIP() {
            break 'bad_format;
        }

        xmin = FullTransactionIdFromU64(pq_getmsgint64(buf) as uint64);
        xmax = FullTransactionIdFromU64(pq_getmsgint64(buf) as uint64);
        if !FullTransactionIdIsValid(xmin)
            || !FullTransactionIdIsValid(xmax)
            || FullTransactionIdPrecedes(xmax, xmin)
        {
            break 'bad_format;
        }

        snap = palloc(PG_SNAPSHOT_SIZE(nxip as usize)) as *mut pg_snapshot;
        (*snap).xmin = xmin;
        (*snap).xmax = xmax;

        i = 0;
        while i < nxip {
            let cur: FullTransactionId = FullTransactionIdFromU64(pq_getmsgint64(buf) as uint64);

            if FullTransactionIdPrecedes(cur, last)
                || FullTransactionIdPrecedes(cur, xmin)
                || FullTransactionIdPrecedes(xmax, cur)
            {
                break 'bad_format;
            }

            /* skip duplicate xips */
            if FullTransactionIdEquals(cur, last) {
                i -= 1;
                nxip -= 1;
                i += 1;
                continue;
            }

            *(*snap).xip.as_mut_ptr().add(i as usize) = cur;
            last = cur;
            i += 1;
        }
        (*snap).nxip = nxip as uint32;
        SET_VARSIZE(snap as *mut c_void, PG_SNAPSHOT_SIZE(nxip as usize) as int32);
        return PG_RETURN_POINTER!(snap as *mut c_void);
    }

    // bad_format:
    ereport!(ERROR, "invalid external pg_snapshot data");
    #[allow(unreachable_code)]
    PG_RETURN_POINTER!(std::ptr::null_mut()) /* keep compiler quiet */
}

/*
 * pg_snapshot_send(pg_snapshot) returns bytea
 *
 *		binary output function for type pg_snapshot
 *
 *		format: int4 nxip, u64 xmin, u64 xmax, u64 xip...
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_send(fcinfo: FunctionCallInfo) -> Datum {
    let snap: *mut pg_snapshot = PG_GETARG_VARLENA_P(fcinfo, 0) as *mut pg_snapshot;
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut i: uint32;

    pq_begintypsend(&mut buf);
    pq_sendint32(&mut buf, (*snap).nxip);
    pq_sendint64(&mut buf, U64FromFullTransactionId((*snap).xmin) as int64);
    pq_sendint64(&mut buf, U64FromFullTransactionId((*snap).xmax) as int64);
    i = 0;
    while i < (*snap).nxip {
        pq_sendint64(
            &mut buf,
            U64FromFullTransactionId(*(*snap).xip.as_ptr().add(i as usize)) as int64,
        );
        i += 1;
    }
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf) as *mut c_void)
}

/*
 * pg_visible_in_snapshot(xid8, pg_snapshot) returns bool
 *
 *		is txid visible in snapshot ?
 */
#[no_mangle]
pub unsafe extern "C" fn pg_visible_in_snapshot(fcinfo: FunctionCallInfo) -> Datum {
    let value: FullTransactionId = PG_GETARG_FULLTRANSACTIONID(fcinfo, 0);
    let snap: *mut pg_snapshot = PG_GETARG_VARLENA_P(fcinfo, 1) as *mut pg_snapshot;

    PG_RETURN_BOOL!(is_visible_fxid(value, snap))
}

/*
 * pg_snapshot_xmin(pg_snapshot) returns xid8
 *
 *		return snapshot's xmin
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_xmin(fcinfo: FunctionCallInfo) -> Datum {
    let snap: *mut pg_snapshot = PG_GETARG_VARLENA_P(fcinfo, 0) as *mut pg_snapshot;

    PG_RETURN_FULLTRANSACTIONID((*snap).xmin)
}

/*
 * pg_snapshot_xmax(pg_snapshot) returns xid8
 *
 *		return snapshot's xmax
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_xmax(fcinfo: FunctionCallInfo) -> Datum {
    let snap: *mut pg_snapshot = PG_GETARG_VARLENA_P(fcinfo, 0) as *mut pg_snapshot;

    PG_RETURN_FULLTRANSACTIONID((*snap).xmax)
}

/*
 * pg_snapshot_xip(pg_snapshot) returns setof xid8
 *
 *		return in-progress xid8s in snapshot.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_snapshot_xip(fcinfo: FunctionCallInfo) -> Datum {
    let mut fctx: *mut FuncCallContext;
    let snap: *mut pg_snapshot;
    let value: FullTransactionId;

    /* on first call initialize fctx and get copy of snapshot */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        let arg: *mut pg_snapshot = PG_GETARG_VARLENA_P(fcinfo, 0) as *mut pg_snapshot;

        fctx = SRF_FIRSTCALL_INIT!(fcinfo);

        /* make a copy of user snapshot */
        let snap_copy: *mut pg_snapshot =
            MemoryContextAlloc((*fctx).multi_call_memory_ctx, VARSIZE(arg as *mut c_void) as Size)
                as *mut pg_snapshot;
        std::ptr::copy_nonoverlapping(
            arg as *const u8,
            snap_copy as *mut u8,
            VARSIZE(arg as *mut c_void) as usize,
        );

        (*fctx).user_fctx = snap_copy as *mut c_void;
    }

    /* return values one-by-one */
    fctx = SRF_PERCALL_SETUP!(fcinfo);
    snap = (*fctx).user_fctx as *mut pg_snapshot;
    if (*fctx).call_cntr < (*snap).nxip as u64 {
        value = *(*snap).xip.as_ptr().add((*fctx).call_cntr as usize);
        SRF_RETURN_NEXT!(fcinfo, fctx, FullTransactionIdGetDatum(value))
    } else {
        SRF_RETURN_DONE!(fcinfo, fctx)
    }
}

/*
 * Report the status of a recent transaction ID, or null for wrapped,
 * truncated away or otherwise too old XIDs.
 *
 * The passed epoch-qualified xid is treated as a normal xid, not a
 * multixact id.
 *
 * If it points to a committed subxact the result is the subxact status even
 * though the parent xact may still be in progress or may have aborted.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_xact_status(fcinfo: FunctionCallInfo) -> Datum {
    let status: *const c_char;
    let fxid: FullTransactionId = PG_GETARG_FULLTRANSACTIONID(fcinfo, 0);
    let mut xid: TransactionId = 0;

    /*
     * We must protect against concurrent truncation of clog entries to avoid
     * an I/O error on SLRU lookup.
     */
    LWLockAcquire(XactTruncationLock, LWMode::LW_SHARED);
    if TransactionIdInRecentPast(fxid, &mut xid) {
        Assert(TransactionIdIsValid(xid));

        /*
         * Like when doing visibility checks on a row, check whether the
         * transaction is still in progress before looking into the CLOG.
         * Otherwise we would incorrectly return "committed" for a transaction
         * that is committing and has already updated the CLOG, but hasn't
         * removed its XID from the proc array yet. (See comment on that race
         * condition at the top of heapam_visibility.c)
         */
        if TransactionIdIsInProgress(xid) {
            status = c"in progress".as_ptr();
        } else if TransactionIdDidCommit(xid) {
            status = c"committed".as_ptr();
        } else {
            /* it must have aborted or crashed */
            status = c"aborted".as_ptr();
        }
    } else {
        status = std::ptr::null();
    }
    LWLockRelease(XactTruncationLock);

    if status.is_null() {
        PG_RETURN_NULL!(fcinfo);
    } else {
        PG_RETURN_TEXT_P(cstring_to_text(status) as *mut c_void)
    }
}

/* ---------------------------------------------------------------------------
 * Local stubs for as-yet unported helper functions / externs.
 * ------------------------------------------------------------------------- */

unsafe fn ReadNextFullTransactionId() -> FullTransactionId {
    unimplemented!() // TODO: access/transam.c
}
unsafe fn FullTransactionIdFromAllowableAt(
    _rel: FullTransactionId,
    _xid: TransactionId,
) -> FullTransactionId {
    unimplemented!() // TODO: access/transam.c
}
unsafe fn GetTopFullTransactionId() -> FullTransactionId {
    unimplemented!() // TODO: access/xact.c
}
unsafe fn GetTopFullTransactionIdIfAny() -> FullTransactionId {
    unimplemented!() // TODO: access/xact.c
}
unsafe fn PreventCommandDuringRecovery(_cmd: *const c_char) {
    unimplemented!() // TODO: access/xact.c
}
unsafe fn GetActiveSnapshot() -> Snapshot {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn TransactionIdIsInProgress(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: storage/ipc/procarray.c
}
unsafe fn TransactionIdDidCommit(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: access/transam/transam.c
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut c_void {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn qunique(
    _array: *mut c_void,
    _elements: usize,
    _width: usize,
    _compare: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) -> usize {
    unimplemented!() // TODO: lib/qunique.h
}

extern "C" {
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *const c_void;
    fn strtou64(str: *const c_char, endptr: *mut *mut c_char, base: c_int) -> uint64;
}
