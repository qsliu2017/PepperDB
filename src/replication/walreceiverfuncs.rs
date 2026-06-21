//! replication/walreceiverfuncs.c - startup-process <-> walreceiver communication.
//!
//! This file contains functions used by the startup process to communicate
//! with the walreceiver process. Functions implementing walreceiver itself
//! are in walreceiver.c.

use crate::prelude::*;
use crate::pg_config_manual::NAMEDATALEN;
type sig_atomic_t = std::ffi::c_int;

// Spinlock primitives (storage/spin.h).
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::storage::lmgr::s_lock::slock_t;

// Condition variable primitives (storage/condition_variable.h).
use crate::storage::lmgr::condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableInit, ConditionVariablePrepareToSleep, ConditionVariableSleep, Latch,
};

// Atomic u64 ops (port/atomics.h).
use crate::port::atomics::generic::{pg_atomic_init_u64_impl, pg_atomic_read_u64_impl};
use crate::port::atomics::pg_atomic_uint64;

// XLog segment offset helper (access/xlog_internal.h).
use crate::access::transam::xlog_internal::XLogSegmentOffset;

// Postmaster signaling (storage/pmsignal.h).
use crate::storage::ipc::pmsignal::{PMSignalReason, SendPostmasterSignal};

// Latch (storage/latch.h).
use crate::storage::ipc::latch::SetLatch;

// Proc number type (storage/procnumber.h).
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

// strlcpy (port.h).
use crate::port::strlcpy::strlcpy;

// MemSet (c.h).
use crate::c::MemSet;

// Replication WAL types (access/xlogdefs.h).
pub type XLogRecPtr = uint64;
pub type TimeLineID = uint32;

// Timestamp / time types.
pub type TimestampTz = int64;
pub type pg_time_t = int64;

// MAXCONNINFO: maximum size of a connection string (replication/walreceiver.h).
pub const MAXCONNINFO: usize = 1024;

// NI_MAXHOST (netdb.h); not yet ported, mirror the common glibc value.
const NI_MAXHOST: usize = 1025;

/*
 * How long to wait for walreceiver to start up after requesting
 * postmaster to launch it. In seconds.
 */
const WALRCV_STARTUP_TIMEOUT: pg_time_t = 10;

/*
 * PMSIGNAL_START_WALRECEIVER index within the PMSignalReason enum
 * (storage/pmsignal.h).
 */
const PMSIGNAL_START_WALRECEIVER: PMSignalReason = 7;

/*
 * Wait event for waiting on walreceiver to exit (utils/wait_event.h); not yet
 * ported, mirror the constant.
 */
const WAIT_EVENT_WAL_RECEIVER_EXIT: uint32 = 0;

/* replication/walreceiver.h: WalRcvState */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum WalRcvState {
    WALRCV_STOPPED,    /* stopped and mustn't start up again */
    WALRCV_STARTING,   /* launched, but the process hasn't initialized yet */
    WALRCV_STREAMING,  /* walreceiver is streaming */
    WALRCV_WAITING,    /* stopped streaming, waiting for orders */
    WALRCV_RESTARTING, /* asked to restart streaming */
    WALRCV_STOPPING,   /* requested to stop, but still running */
}
use WalRcvState::*;

/* Shared memory area for management of walreceiver process. */
#[repr(C)]
pub struct WalRcvData {
    pub procno: ProcNumber,
    pub pid: pid_t,

    pub walRcvState: WalRcvState,
    pub walRcvStoppedCV: ConditionVariable,

    pub startTime: pg_time_t,

    pub receiveStart: XLogRecPtr,
    pub receiveStartTLI: TimeLineID,

    pub flushedUpto: XLogRecPtr,
    pub receivedTLI: TimeLineID,

    pub latestChunkStart: XLogRecPtr,

    pub lastMsgSendTime: TimestampTz,
    pub lastMsgReceiptTime: TimestampTz,

    pub latestWalEnd: XLogRecPtr,
    pub latestWalEndTime: TimestampTz,

    pub conninfo: [c_char; MAXCONNINFO],

    pub sender_host: [c_char; NI_MAXHOST],
    pub sender_port: c_int,

    pub slotname: [c_char; NAMEDATALEN],

    pub is_temp_slot: bool,

    pub ready_to_display: bool,

    pub mutex: slock_t, /* locks shared variables shown above */

    pub writtenUpto: pg_atomic_uint64,

    pub force_reply: sig_atomic_t, /* used as a bool */
}

pub static mut WalRcv: *mut WalRcvData = null_mut();

/* pid_t (sys/types.h). */
#[allow(non_camel_case_types)]
type pid_t = c_int;

/* signal numbers (signal.h). */
const SIGTERM: c_int = 15;

/* ---- Stubs for not-yet-ported callees ---- */

/* C library time() (time.h). */
unsafe fn time(_t: *mut pg_time_t) -> pg_time_t {
    unimplemented!()
}

/* C library kill() (signal.h). */
unsafe fn kill(pid: pid_t, sig: c_int) -> c_int { todo!("TODO(pg-port): kill") }

/* storage/shmem.h: ShmemInitStruct. */
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _foundPtr: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size, _foundPtr)
}

/* storage/shmem.h: add_size. */
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}

/*
 * storage/proc.h: PGPROC.  Only the procLatch field is referenced here; mirror
 * just enough of the layout.
 */
#[repr(C)]
pub struct PGPROC {
    pub procLatch: Latch,
}

/* storage/proc.h: GetPGProcByNumber. */
pub unsafe fn GetPGProcByNumber(_procno: ProcNumber) -> *mut PGPROC {
    unimplemented!()
}

/* access/xlogrecovery.h: GetXLogReplayRecPtr. */
unsafe fn GetXLogReplayRecPtr(replayTLI: *mut TimeLineID) -> XLogRecPtr { crate::access::transam::xlogrecovery::GetXLogReplayRecPtr(replayTLI as _) }

/* access/xlogrecovery.h: GetCurrentChunkReplayStartTime. */
unsafe fn GetCurrentChunkReplayStartTime() -> TimestampTz { crate::access::transam::xlogrecovery::GetCurrentChunkReplayStartTime() }

/* utils/timestamp.h: GetCurrentTimestamp. */
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}

/* utils/timestamp.h: TimestampDifferenceMilliseconds. */
unsafe fn TimestampDifferenceMilliseconds(start_time: TimestampTz, stop_time: TimestampTz) -> c_long { crate::utils::adt::timestamp::TimestampDifferenceMilliseconds(start_time as _, stop_time as _) }

/*
 * wal_segment_size is a process-global initialized from the control file; it is
 * not yet ported as a real global, so mirror it as a local extern-style global.
 */
static mut wal_segment_size: c_int = 0;

/* port/atomics.h: pg_atomic_init_u64 thin wrapper over the impl. */
#[inline]
unsafe fn pg_atomic_init_u64(ptr: *mut pg_atomic_uint64, val: uint64) {
    pg_atomic_init_u64_impl(&*ptr, val);
}

/* port/atomics.h: pg_atomic_read_u64 thin wrapper over the impl. */
#[inline]
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> uint64 {
    pg_atomic_read_u64_impl(&*ptr)
}

/* Report shared memory space needed by WalRcvShmemInit */
pub unsafe fn WalRcvShmemSize() -> Size {
    let mut size: Size = 0;

    size = add_size(size, size_of::<WalRcvData>());

    size
}

/* Allocate and initialize walreceiver-related shared memory */
pub unsafe fn WalRcvShmemInit() {
    let mut found: bool = false;

    WalRcv = ShmemInitStruct(
        c"Wal Receiver Ctl".as_ptr(),
        WalRcvShmemSize(),
        &mut found,
    ) as *mut WalRcvData;

    if !found {
        /* First time through, so initialize */
        MemSet(WalRcv as *mut c_void, 0, WalRcvShmemSize());
        (*WalRcv).walRcvState = WALRCV_STOPPED;
        ConditionVariableInit(&mut (*WalRcv).walRcvStoppedCV);
        SpinLockInit(&mut (*WalRcv).mutex);
        pg_atomic_init_u64(&mut (*WalRcv).writtenUpto, 0);
        (*WalRcv).procno = INVALID_PROC_NUMBER;
    }
}

/* Is walreceiver running (or starting up)? */
pub unsafe fn WalRcvRunning() -> bool {
    let walrcv: *mut WalRcvData = WalRcv;
    let mut state: WalRcvState;
    let startTime: pg_time_t;

    SpinLockAcquire(&mut (*walrcv).mutex);

    state = (*walrcv).walRcvState;
    startTime = (*walrcv).startTime;

    SpinLockRelease(&mut (*walrcv).mutex);

    /*
     * If it has taken too long for walreceiver to start up, give up. Setting
     * the state to STOPPED ensures that if walreceiver later does start up
     * after all, it will see that it's not supposed to be running and die
     * without doing anything.
     */
    if state == WALRCV_STARTING {
        let now: pg_time_t = time(null_mut()) as pg_time_t;

        if (now - startTime) > WALRCV_STARTUP_TIMEOUT {
            let mut stopped: bool = false;

            SpinLockAcquire(&mut (*walrcv).mutex);
            if (*walrcv).walRcvState == WALRCV_STARTING {
                (*walrcv).walRcvState = WALRCV_STOPPED;
                state = WALRCV_STOPPED;
                stopped = true;
            }
            SpinLockRelease(&mut (*walrcv).mutex);

            if stopped {
                ConditionVariableBroadcast(&mut (*walrcv).walRcvStoppedCV);
            }
        }
    }

    if state != WALRCV_STOPPED {
        true
    } else {
        false
    }
}

/*
 * Is walreceiver running and streaming (or at least attempting to connect,
 * or starting up)?
 */
pub unsafe fn WalRcvStreaming() -> bool {
    let walrcv: *mut WalRcvData = WalRcv;
    let mut state: WalRcvState;
    let startTime: pg_time_t;

    SpinLockAcquire(&mut (*walrcv).mutex);

    state = (*walrcv).walRcvState;
    startTime = (*walrcv).startTime;

    SpinLockRelease(&mut (*walrcv).mutex);

    /*
     * If it has taken too long for walreceiver to start up, give up. Setting
     * the state to STOPPED ensures that if walreceiver later does start up
     * after all, it will see that it's not supposed to be running and die
     * without doing anything.
     */
    if state == WALRCV_STARTING {
        let now: pg_time_t = time(null_mut()) as pg_time_t;

        if (now - startTime) > WALRCV_STARTUP_TIMEOUT {
            let mut stopped: bool = false;

            SpinLockAcquire(&mut (*walrcv).mutex);
            if (*walrcv).walRcvState == WALRCV_STARTING {
                (*walrcv).walRcvState = WALRCV_STOPPED;
                state = WALRCV_STOPPED;
                stopped = true;
            }
            SpinLockRelease(&mut (*walrcv).mutex);

            if stopped {
                ConditionVariableBroadcast(&mut (*walrcv).walRcvStoppedCV);
            }
        }
    }

    if state == WALRCV_STREAMING || state == WALRCV_STARTING || state == WALRCV_RESTARTING {
        true
    } else {
        false
    }
}

/*
 * Stop walreceiver (if running) and wait for it to die.
 * Executed by the Startup process.
 */
pub unsafe fn ShutdownWalRcv() {
    let walrcv: *mut WalRcvData = WalRcv;
    let mut walrcvpid: pid_t = 0;
    let mut stopped: bool = false;

    /*
     * Request walreceiver to stop. Walreceiver will switch to WALRCV_STOPPED
     * mode once it's finished, and will also request postmaster to not
     * restart itself.
     */
    SpinLockAcquire(&mut (*walrcv).mutex);
    match (*walrcv).walRcvState {
        WALRCV_STOPPED => {}
        WALRCV_STARTING => {
            (*walrcv).walRcvState = WALRCV_STOPPED;
            stopped = true;
        }

        WALRCV_STREAMING | WALRCV_WAITING | WALRCV_RESTARTING => {
            (*walrcv).walRcvState = WALRCV_STOPPING;
            /* fall through */
            walrcvpid = (*walrcv).pid;
        }
        WALRCV_STOPPING => {
            walrcvpid = (*walrcv).pid;
        }
    }
    SpinLockRelease(&mut (*walrcv).mutex);

    /* Unnecessary but consistent. */
    if stopped {
        ConditionVariableBroadcast(&mut (*walrcv).walRcvStoppedCV);
    }

    /*
     * Signal walreceiver process if it was still running.
     */
    if walrcvpid != 0 {
        kill(walrcvpid, SIGTERM);
    }

    /*
     * Wait for walreceiver to acknowledge its death by setting state to
     * WALRCV_STOPPED.
     */
    ConditionVariablePrepareToSleep(&mut (*walrcv).walRcvStoppedCV);
    while WalRcvRunning() {
        ConditionVariableSleep(&mut (*walrcv).walRcvStoppedCV, WAIT_EVENT_WAL_RECEIVER_EXIT);
    }
    ConditionVariableCancelSleep();
}

/*
 * Request postmaster to start walreceiver.
 *
 * "recptr" indicates the position where streaming should begin.  "conninfo"
 * is a libpq connection string to use.  "slotname" is, optionally, the name
 * of a replication slot to acquire.  "create_temp_slot" indicates to create
 * a temporary slot when no "slotname" is given.
 *
 * WAL receivers do not directly load GUC parameters used for the connection
 * to the primary, and rely on the values passed down by the caller of this
 * routine instead.  Hence, the addition of any new parameters should happen
 * through this code path.
 */
pub unsafe fn RequestXLogStreaming(
    tli: TimeLineID,
    mut recptr: XLogRecPtr,
    conninfo: *const c_char,
    slotname: *const c_char,
    create_temp_slot: bool,
) {
    let walrcv: *mut WalRcvData = WalRcv;
    let mut launch: bool = false;
    let now: pg_time_t = time(null_mut()) as pg_time_t;
    let walrcv_proc: ProcNumber;

    /*
     * We always start at the beginning of the segment. That prevents a broken
     * segment (i.e., with no records in the first half of a segment) from
     * being created by XLOG streaming, which might cause trouble later on if
     * the segment is e.g archived.
     */
    if XLogSegmentOffset(recptr, wal_segment_size) != 0 {
        recptr -= XLogSegmentOffset(recptr, wal_segment_size);
    }

    SpinLockAcquire(&mut (*walrcv).mutex);

    /* It better be stopped if we try to restart it */
    Assert!((*walrcv).walRcvState == WALRCV_STOPPED || (*walrcv).walRcvState == WALRCV_WAITING);

    if conninfo != null() {
        strlcpy((*walrcv).conninfo.as_mut_ptr(), conninfo, MAXCONNINFO);
    } else {
        (*walrcv).conninfo[0] = b'\0' as c_char;
    }

    /*
     * Use configured replication slot if present, and ignore the value of
     * create_temp_slot as the slot name should be persistent.  Otherwise, use
     * create_temp_slot to determine whether this WAL receiver should create a
     * temporary slot by itself and use it, or not.
     */
    if slotname != null() && *slotname != b'\0' as c_char {
        strlcpy((*walrcv).slotname.as_mut_ptr(), slotname, NAMEDATALEN);
        (*walrcv).is_temp_slot = false;
    } else {
        (*walrcv).slotname[0] = b'\0' as c_char;
        (*walrcv).is_temp_slot = create_temp_slot;
    }

    if (*walrcv).walRcvState == WALRCV_STOPPED {
        launch = true;
        (*walrcv).walRcvState = WALRCV_STARTING;
    } else {
        (*walrcv).walRcvState = WALRCV_RESTARTING;
    }
    (*walrcv).startTime = now;

    /*
     * If this is the first startup of walreceiver (on this timeline),
     * initialize flushedUpto and latestChunkStart to the starting point.
     */
    if (*walrcv).receiveStart == 0 || (*walrcv).receivedTLI != tli {
        (*walrcv).flushedUpto = recptr;
        (*walrcv).receivedTLI = tli;
        (*walrcv).latestChunkStart = recptr;
    }
    (*walrcv).receiveStart = recptr;
    (*walrcv).receiveStartTLI = tli;

    walrcv_proc = (*walrcv).procno;

    SpinLockRelease(&mut (*walrcv).mutex);

    if launch {
        SendPostmasterSignal(PMSIGNAL_START_WALRECEIVER);
    } else if walrcv_proc != INVALID_PROC_NUMBER {
        SetLatch(&mut (*GetPGProcByNumber(walrcv_proc)).procLatch as *mut _ as *mut _);
    }
}

/*
 * Returns the last+1 byte position that walreceiver has flushed.
 *
 * Optionally, returns the previous chunk start, that is the first byte
 * written in the most recent walreceiver flush cycle.  Callers not
 * interested in that value may pass NULL for latestChunkStart. Same for
 * receiveTLI.
 */
pub unsafe fn GetWalRcvFlushRecPtr(
    latestChunkStart: *mut XLogRecPtr,
    receiveTLI: *mut TimeLineID,
) -> XLogRecPtr {
    let walrcv: *mut WalRcvData = WalRcv;
    let recptr: XLogRecPtr;

    SpinLockAcquire(&mut (*walrcv).mutex);
    recptr = (*walrcv).flushedUpto;
    if !latestChunkStart.is_null() {
        *latestChunkStart = (*walrcv).latestChunkStart;
    }
    if !receiveTLI.is_null() {
        *receiveTLI = (*walrcv).receivedTLI;
    }
    SpinLockRelease(&mut (*walrcv).mutex);

    recptr
}

/*
 * Returns the last+1 byte position that walreceiver has written.
 * This returns a recently written value without taking a lock.
 */
pub unsafe fn GetWalRcvWriteRecPtr() -> XLogRecPtr {
    let walrcv: *mut WalRcvData = WalRcv;

    pg_atomic_read_u64(&mut (*walrcv).writtenUpto)
}

/*
 * Returns the replication apply delay in ms or -1
 * if the apply delay info is not available
 */
pub unsafe fn GetReplicationApplyDelay() -> c_int {
    let walrcv: *mut WalRcvData = WalRcv;
    let receivePtr: XLogRecPtr;
    let replayPtr: XLogRecPtr;
    let chunkReplayStartTime: TimestampTz;

    SpinLockAcquire(&mut (*walrcv).mutex);
    receivePtr = (*walrcv).flushedUpto;
    SpinLockRelease(&mut (*walrcv).mutex);

    replayPtr = GetXLogReplayRecPtr(null_mut());

    if receivePtr == replayPtr {
        return 0;
    }

    chunkReplayStartTime = GetCurrentChunkReplayStartTime();

    if chunkReplayStartTime == 0 {
        return -1;
    }

    TimestampDifferenceMilliseconds(chunkReplayStartTime, GetCurrentTimestamp()) as c_int
}

/*
 * Returns the network latency in ms, note that this includes any
 * difference in clock settings between the servers, as well as timezone.
 */
pub unsafe fn GetReplicationTransferLatency() -> c_int {
    let walrcv: *mut WalRcvData = WalRcv;
    let lastMsgSendTime: TimestampTz;
    let lastMsgReceiptTime: TimestampTz;

    SpinLockAcquire(&mut (*walrcv).mutex);
    lastMsgSendTime = (*walrcv).lastMsgSendTime;
    lastMsgReceiptTime = (*walrcv).lastMsgReceiptTime;
    SpinLockRelease(&mut (*walrcv).mutex);

    TimestampDifferenceMilliseconds(lastMsgSendTime, lastMsgReceiptTime) as c_int
}
