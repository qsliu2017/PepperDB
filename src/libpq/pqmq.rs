//! libpq/pqmq.c - Use the frontend/backend protocol for communication over a shm_mq

use crate::prelude::*;

use crate::lib::stringinfo::StringInfo;
use crate::libpq::libpq::PQcommMethods;
use crate::libpq::pqformat::{pq_getmsgbyte, pq_getmsgend, pq_getmsgrawstring};
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, MyLatch, Latch};
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};
use crate::tcop::dest::CommandDest;
use crate::tcop::tcopprot::whereToSendOutput;
use crate::utils::builtins::pg_strtoint32;
use crate::utils::init::globals::{FrontendProtocol, ProtocolVersion};
use crate::replication::logicalworker::IsLogicalParallelApplyWorker;

use core::ffi::c_int;

const EOF: c_int = -1;

// ---------------------------------------------------------------------------
// Types / externs not yet ported. shm_mq lives in storage/ipc/shm_mq.c, dsm in
// storage/ipc/dsm.c, latch in storage/ipc/latch.c, procsignal in
// storage/ipc/procsignal.c, ErrorData in utils/error/elog.c, parallel in
// access/transam/parallel.c.  Stub locally until those land.
// TODO: replace with real definitions once ported.
// ---------------------------------------------------------------------------

pub type shm_mq_handle = c_void; // storage/shm_mq.h
pub type dsm_segment = c_void; // storage/dsm.h

// shm_mq_iovec (shm_mq.h)
#[repr(C)]
pub struct shm_mq_iovec {
    pub data: *const c_char,
    pub len: Size,
}

// shm_mq_result (shm_mq.h)
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum shm_mq_result {
    SHM_MQ_SUCCESS,
    SHM_MQ_WOULD_BLOCK,
    SHM_MQ_DETACHED,
}
use shm_mq_result::*;

// ErrorData (utils/elog.h) - only the fields used by pq_parse_errornotice are
// modeled here; reconcile when elog.c lands.
// TODO: unify with the real ErrorData once ported.
#[repr(C)]
pub struct ErrorData {
    pub elevel: c_int,
    pub sqlerrcode: c_int,
    pub message: *mut c_char,
    pub detail: *mut c_char,
    pub hint: *mut c_char,
    pub context: *mut c_char,
    pub internalquery: *mut c_char,
    pub internalpos: c_int,
    pub cursorpos: c_int,
    pub schema_name: *mut c_char,
    pub table_name: *mut c_char,
    pub column_name: *mut c_char,
    pub datatype_name: *mut c_char,
    pub constraint_name: *mut c_char,
    pub funcname: *const c_char,
    pub filename: *const c_char,
    pub lineno: c_int,
    pub assoc_context: MemoryContext,
}

// PG_PROTOCOL_LATEST (pqcomm.h): protocol version 3.0 -> (3<<16)|0
const PG_PROTOCOL_LATEST: ProtocolVersion = (3u32 << 16) | 0;

// WaitLatch flags (storage/latch.h)
const WL_LATCH_SET: c_int = 1 << 0;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// WaitEventIPC (utils/wait_event.h)
const WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE: u32 = 0;

// ProcSignalReason (storage/procsignal.h)
type ProcSignalReason = c_int;
const PROCSIG_PARALLEL_MESSAGE: ProcSignalReason = 0;
const PROCSIG_PARALLEL_APPLY_MESSAGE: ProcSignalReason = 0;

// MAKE_SQLSTATE (utils/elog.h): pack five chars into the error code.
#[inline]
fn MAKE_SQLSTATE(ch1: c_char, ch2: c_char, ch3: c_char, ch4: c_char, ch5: c_char) -> c_int {
    ((ch1 as c_int & 0x3F))
        | ((ch2 as c_int & 0x3F) << 6)
        | ((ch3 as c_int & 0x3F) << 12)
        | ((ch4 as c_int & 0x3F) << 18)
        | ((ch5 as c_int & 0x3F) << 24)
}

// --- stubs for not-yet-ported called functions ---

// shm_mq.c
unsafe fn shm_mq_detach(_mqh: *mut shm_mq_handle) {
    unimplemented!()
} // TODO: storage/ipc/shm_mq.c

unsafe fn shm_mq_sendv(
    _mqh: *mut shm_mq_handle,
    _iov: *const shm_mq_iovec,
    _iovcnt: c_int,
    _nowait: bool,
    _force_flush: bool,
) -> shm_mq_result {
    unimplemented!()
} // TODO: storage/ipc/shm_mq.c

// dsm.c
type on_dsm_detach_callback = unsafe extern "C" fn(seg: *mut dsm_segment, arg: Datum);
unsafe fn on_dsm_detach(_seg: *mut dsm_segment, _function: on_dsm_detach_callback, _arg: Datum) {
    unimplemented!()
} // TODO: storage/ipc/dsm.c

// latch.c
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout: i64,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!()
} // TODO: storage/ipc/latch.c

unsafe fn ResetLatch(_latch: *mut Latch) {
    unimplemented!()
} // TODO: storage/ipc/latch.c

// procsignal.c
unsafe fn SendProcSignal(
    _pid: pid_t,
    _reason: ProcSignalReason,
    _procNumber: ProcNumber,
) -> c_int {
    unimplemented!()
} // TODO: storage/ipc/procsignal.c

// parallel.c
unsafe fn IsParallelWorker() -> bool {
    unimplemented!()
} // TODO: access/transam/parallel.c

type pid_t = i32;

// PqCommMethods (libpq.h): in C this is an extern global pointer that we
// reassign here.  The real extern declaration in libpq.rs is `*const` (not
// assignable), so we keep a private mutable copy that the methods table swaps
// in.  TODO: reconcile with the libpq.c-owned global once that is ported.
static mut PqCommMethods: *const PQcommMethods = null();

static mut pq_mq_handle: *mut shm_mq_handle = null_mut();
static mut pq_mq_busy: bool = false;
static mut pq_mq_parallel_leader_pid: pid_t = 0;
static mut pq_mq_parallel_leader_proc_number: ProcNumber = INVALID_PROC_NUMBER;

static PqCommMqMethods: PQcommMethods = PQcommMethods {
    comm_reset: Some(mq_comm_reset),
    flush: Some(mq_flush),
    flush_if_writable: Some(mq_flush_if_writable),
    is_send_pending: Some(mq_is_send_pending),
    putmessage: Some(mq_putmessage),
    putmessage_noblock: Some(mq_putmessage_noblock),
};

/*
 * Arrange to redirect frontend/backend protocol messages to a shared-memory
 * message queue.
 */
pub unsafe fn pq_redirect_to_shm_mq(seg: *mut dsm_segment, mqh: *mut shm_mq_handle) {
    PqCommMethods = &raw const PqCommMqMethods;
    pq_mq_handle = mqh;
    whereToSendOutput = CommandDest::DestRemote;
    FrontendProtocol = PG_PROTOCOL_LATEST;
    on_dsm_detach(seg, pq_cleanup_redirect_to_shm_mq, 0 as Datum);
}

/*
 * When the DSM that contains our shm_mq goes away, we need to stop sending
 * messages to it.
 */
unsafe extern "C" fn pq_cleanup_redirect_to_shm_mq(_seg: *mut dsm_segment, _arg: Datum) {
    pq_mq_handle = null_mut();
    whereToSendOutput = CommandDest::DestNone;
}

/*
 * Arrange to SendProcSignal() to the parallel leader each time we transmit
 * message data via the shm_mq.
 */
pub unsafe fn pq_set_parallel_leader(pid: pid_t, procNumber: ProcNumber) {
    Assert!(PqCommMethods == &raw const PqCommMqMethods);
    pq_mq_parallel_leader_pid = pid;
    pq_mq_parallel_leader_proc_number = procNumber;
}

unsafe extern "C" fn mq_comm_reset() {
    /* Nothing to do. */
}

unsafe extern "C" fn mq_flush() -> c_int {
    /* Nothing to do. */
    0
}

unsafe extern "C" fn mq_flush_if_writable() -> c_int {
    /* Nothing to do. */
    0
}

unsafe extern "C" fn mq_is_send_pending() -> bool {
    /* There's never anything pending. */
    false
}

/*
 * Transmit a libpq protocol message to the shared memory message queue
 * selected via pq_mq_handle.  We don't include a length word, because the
 * receiver will know the length of the message from shm_mq_receive().
 */
unsafe extern "C" fn mq_putmessage(msgtype: c_char, s: *const c_char, len: Size) -> c_int {
    let mut iov: [shm_mq_iovec; 2] = [
        shm_mq_iovec { data: null(), len: 0 },
        shm_mq_iovec { data: null(), len: 0 },
    ];
    let mut result: shm_mq_result;

    /*
     * If we're sending a message, and we have to wait because the queue is
     * full, and then we get interrupted, and that interrupt results in trying
     * to send another message, we respond by detaching the queue.  There's no
     * way to return to the original context, but even if there were, just
     * queueing the message would amount to indefinitely postponing the
     * response to the interrupt.  So we do this instead.
     */
    if pq_mq_busy {
        if !pq_mq_handle.is_null() {
            shm_mq_detach(pq_mq_handle);
        }
        pq_mq_handle = null_mut();
        return EOF;
    }

    /*
     * If the message queue is already gone, just ignore the message. This
     * doesn't necessarily indicate a problem; for example, DEBUG messages can
     * be generated late in the shutdown sequence, after all DSMs have already
     * been detached.
     */
    if pq_mq_handle.is_null() {
        return 0;
    }

    pq_mq_busy = true;

    iov[0].data = &msgtype;
    iov[0].len = 1;
    iov[1].data = s;
    iov[1].len = len;

    Assert!(!pq_mq_handle.is_null());

    loop {
        /*
         * Immediately notify the receiver by passing force_flush as true so
         * that the shared memory value is updated before we send the parallel
         * message signal right after this.
         */
        result = shm_mq_sendv(pq_mq_handle, iov.as_ptr(), 2, true, true);

        if pq_mq_parallel_leader_pid != 0 {
            if IsLogicalParallelApplyWorker() {
                SendProcSignal(
                    pq_mq_parallel_leader_pid,
                    PROCSIG_PARALLEL_APPLY_MESSAGE,
                    pq_mq_parallel_leader_proc_number,
                );
            } else {
                Assert!(IsParallelWorker());
                SendProcSignal(
                    pq_mq_parallel_leader_pid,
                    PROCSIG_PARALLEL_MESSAGE,
                    pq_mq_parallel_leader_proc_number,
                );
            }
        }

        if result != SHM_MQ_WOULD_BLOCK {
            break;
        }

        WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE,
        );
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();
    }

    pq_mq_busy = false;

    Assert!(result == SHM_MQ_SUCCESS || result == SHM_MQ_DETACHED);
    if result != SHM_MQ_SUCCESS {
        return EOF;
    }
    0
}

unsafe extern "C" fn mq_putmessage_noblock(_msgtype: c_char, _s: *const c_char, _len: Size) {
    /*
     * While the shm_mq machinery does support sending a message in
     * non-blocking mode, there's currently no way to try sending beginning to
     * send the message that doesn't also commit us to completing the
     * transmission.  This could be improved in the future, but for now we
     * don't need it.
     */
    elog!(ERROR, "not currently supported");
}

/*
 * Parse an ErrorResponse or NoticeResponse payload and populate an ErrorData
 * structure with the results.
 */
pub unsafe fn pq_parse_errornotice(msg: StringInfo, edata: *mut ErrorData) {
    /* Initialize edata with reasonable defaults. */
    MemSet(edata as *mut c_void, 0, core::mem::size_of::<ErrorData>());
    (*edata).elevel = ERROR;
    (*edata).assoc_context = CurrentMemoryContext;

    /* Loop over fields and extract each one. */
    loop {
        let code = pq_getmsgbyte(msg) as c_char;
        let value: *const c_char;

        if code == b'\0' as c_char {
            pq_getmsgend(msg);
            break;
        }
        value = pq_getmsgrawstring(msg);

        match code as u8 {
            PG_DIAG_SEVERITY => {
                /* ignore, trusting we'll get a nonlocalized version */
            }
            PG_DIAG_SEVERITY_NONLOCALIZED => {
                if strcmp(value, c"DEBUG".as_ptr()) == 0 {
                    /*
                     * We can't reconstruct the exact DEBUG level, but
                     * presumably it was >= client_min_messages, so select
                     * DEBUG1 to ensure we'll pass it on to the client.
                     */
                    (*edata).elevel = DEBUG1;
                } else if strcmp(value, c"LOG".as_ptr()) == 0 {
                    /*
                     * It can't be LOG_SERVER_ONLY, or the worker wouldn't
                     * have sent it to us; so LOG is the correct value.
                     */
                    (*edata).elevel = LOG;
                } else if strcmp(value, c"INFO".as_ptr()) == 0 {
                    (*edata).elevel = INFO;
                } else if strcmp(value, c"NOTICE".as_ptr()) == 0 {
                    (*edata).elevel = NOTICE;
                } else if strcmp(value, c"WARNING".as_ptr()) == 0 {
                    (*edata).elevel = WARNING;
                } else if strcmp(value, c"ERROR".as_ptr()) == 0 {
                    (*edata).elevel = ERROR;
                } else if strcmp(value, c"FATAL".as_ptr()) == 0 {
                    (*edata).elevel = FATAL;
                } else if strcmp(value, c"PANIC".as_ptr()) == 0 {
                    (*edata).elevel = PANIC;
                } else {
                    elog!(ERROR, "unrecognized error severity: \"{}\"", cstr(value));
                }
            }
            PG_DIAG_SQLSTATE => {
                if strlen(value) != 5 {
                    elog!(ERROR, "invalid SQLSTATE: \"{}\"", cstr(value));
                }
                (*edata).sqlerrcode = MAKE_SQLSTATE(
                    *value.add(0),
                    *value.add(1),
                    *value.add(2),
                    *value.add(3),
                    *value.add(4),
                );
            }
            PG_DIAG_MESSAGE_PRIMARY => {
                (*edata).message = pstrdup(value);
            }
            PG_DIAG_MESSAGE_DETAIL => {
                (*edata).detail = pstrdup(value);
            }
            PG_DIAG_MESSAGE_HINT => {
                (*edata).hint = pstrdup(value);
            }
            PG_DIAG_STATEMENT_POSITION => {
                (*edata).cursorpos = pg_strtoint32(value);
            }
            PG_DIAG_INTERNAL_POSITION => {
                (*edata).internalpos = pg_strtoint32(value);
            }
            PG_DIAG_INTERNAL_QUERY => {
                (*edata).internalquery = pstrdup(value);
            }
            PG_DIAG_CONTEXT => {
                (*edata).context = pstrdup(value);
            }
            PG_DIAG_SCHEMA_NAME => {
                (*edata).schema_name = pstrdup(value);
            }
            PG_DIAG_TABLE_NAME => {
                (*edata).table_name = pstrdup(value);
            }
            PG_DIAG_COLUMN_NAME => {
                (*edata).column_name = pstrdup(value);
            }
            PG_DIAG_DATATYPE_NAME => {
                (*edata).datatype_name = pstrdup(value);
            }
            PG_DIAG_CONSTRAINT_NAME => {
                (*edata).constraint_name = pstrdup(value);
            }
            PG_DIAG_SOURCE_FILE => {
                (*edata).filename = pstrdup(value);
            }
            PG_DIAG_SOURCE_LINE => {
                (*edata).lineno = pg_strtoint32(value);
            }
            PG_DIAG_SOURCE_FUNCTION => {
                (*edata).funcname = pstrdup(value);
            }
            _ => {
                elog!(ERROR, "unrecognized error field code: {}", code as c_int);
            }
        }
    }
}

// libc shims (string.h).
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> Size;
}

// Helper to render a NUL-terminated C string in an elog! format argument.
unsafe fn cstr(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "(null)";
    }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("(invalid utf8)")
}
