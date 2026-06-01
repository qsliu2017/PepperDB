//! src/backend/storage/aio/method_worker.c
//!
//! AIO - perform AIO using worker processes
//!
//! IO workers consume IOs from a shared memory submission queue, run
//! traditional synchronous system calls, and perform the shared completion
//! handling immediately.  Client code submits most requests by pushing IOs
//! into the submission queue, and waits (if necessary) using condition
//! variables.  Some IOs cannot be performed in another process due to lack of
//! infrastructure for reopening the file, and must processed synchronously by
//! the client code when submitted.
//!
//! So that the submitter can make just one system call when submitting a batch
//! of IOs, wakeups "fan out"; each woken IO worker can wake two more. XXX This
//! could be improved by using futexes instead of latches to wake N waiters.
//!
//! This method of AIO is available in all builds on all operating systems, and
//! is the default.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/aio/method_worker.c

use crate::prelude::*;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

/* How many workers should each worker wake up if needed? */
const IO_WORKER_WAKEUP_FANOUT: usize = 2;

#[repr(C)]
pub struct PgAioWorkerSubmissionQueue {
    pub size: uint32,
    pub mask: uint32,
    pub head: uint32,
    pub tail: uint32,
    pub sqes: [c_int; FLEXIBLE_ARRAY_MEMBER],
}

#[repr(C)]
pub struct PgAioWorkerSlot {
    pub latch: *mut Latch,
    pub in_use: bool,
}

#[repr(C)]
pub struct PgAioWorkerControl {
    pub idle_worker_mask: uint64,
    pub workers: [PgAioWorkerSlot; FLEXIBLE_ARRAY_MEMBER],
}

pub const pgaio_worker_ops: IoMethodOps = IoMethodOps {
    shmem_size: Some(pgaio_worker_shmem_size),
    shmem_init: Some(pgaio_worker_shmem_init),

    needs_synchronous_execution: Some(pgaio_worker_needs_synchronous_execution),
    submit: Some(pgaio_worker_submit),
    ..IoMethodOps::DEFAULT
};

/* GUCs */
pub static mut io_workers: c_int = 3;

static mut io_worker_queue_size: c_int = 64;
static mut MyIoWorkerId: c_int = 0;
static mut io_worker_submission_queue: *mut PgAioWorkerSubmissionQueue = std::ptr::null_mut();
static mut io_worker_control: *mut PgAioWorkerControl = std::ptr::null_mut();

unsafe fn pgaio_worker_queue_shmem_size(queue_size: *mut c_int) -> Size {
    /* Round size up to next power of two so we can make a mask. */
    *queue_size = pg_nextpower2_32(io_worker_queue_size as uint32) as c_int;

    core::mem::offset_of!(PgAioWorkerSubmissionQueue, sqes)
        + std::mem::size_of::<c_int>() * (*queue_size as Size)
}

unsafe fn pgaio_worker_control_shmem_size() -> Size {
    core::mem::offset_of!(PgAioWorkerControl, workers)
        + std::mem::size_of::<PgAioWorkerSlot>() * (MAX_IO_WORKERS as Size)
}

unsafe extern "C" fn pgaio_worker_shmem_size() -> Size {
    let sz: Size;
    let mut queue_size: c_int = 0;

    sz = pgaio_worker_queue_shmem_size(&mut queue_size);
    let sz = add_size(sz, pgaio_worker_control_shmem_size());

    sz
}

unsafe extern "C" fn pgaio_worker_shmem_init(first_time: bool) {
    let mut found: bool = false;
    let mut queue_size: c_int = 0;

    io_worker_submission_queue = ShmemInitStruct(
        c"AioWorkerSubmissionQueue".as_ptr(),
        pgaio_worker_queue_shmem_size(&mut queue_size),
        &mut found,
    ) as *mut PgAioWorkerSubmissionQueue;
    if !found {
        (*io_worker_submission_queue).size = queue_size as uint32;
        (*io_worker_submission_queue).head = 0;
        (*io_worker_submission_queue).tail = 0;
    }

    io_worker_control = ShmemInitStruct(
        c"AioWorkerControl".as_ptr(),
        pgaio_worker_control_shmem_size(),
        &mut found,
    ) as *mut PgAioWorkerControl;
    if !found {
        (*io_worker_control).idle_worker_mask = 0;
        for i in 0..MAX_IO_WORKERS {
            let slot = (*io_worker_control).workers.as_mut_ptr().add(i as usize);
            (*slot).latch = std::ptr::null_mut();
            (*slot).in_use = false;
        }
    }
}

unsafe fn pgaio_worker_choose_idle() -> c_int {
    let worker: c_int;

    if (*io_worker_control).idle_worker_mask == 0 {
        return -1;
    }

    /* Find the lowest bit position, and clear it. */
    worker = pg_rightmost_one_pos64((*io_worker_control).idle_worker_mask);
    (*io_worker_control).idle_worker_mask &= !(1u64 << worker);
    Assert!((*(*io_worker_control).workers.as_ptr().add(worker as usize)).in_use);

    worker
}

unsafe fn pgaio_worker_submission_queue_insert(ioh: *mut PgAioHandle) -> bool {
    let queue: *mut PgAioWorkerSubmissionQueue;
    let new_head: uint32;

    queue = io_worker_submission_queue;
    new_head = ((*queue).head + 1) & ((*queue).size - 1);
    if new_head == (*queue).tail {
        pgaio_debug!(
            DEBUG3,
            "io queue is full, at {} elements",
            (*io_worker_submission_queue).size
        );
        return false; /* full */
    }

    *(*queue).sqes.as_mut_ptr().add((*queue).head as usize) = pgaio_io_get_id(ioh);
    (*queue).head = new_head;

    true
}

unsafe fn pgaio_worker_submission_queue_consume() -> c_int {
    let queue: *mut PgAioWorkerSubmissionQueue;
    let result: c_int;

    queue = io_worker_submission_queue;
    if (*queue).tail == (*queue).head {
        return -1; /* empty */
    }

    result = *(*queue).sqes.as_ptr().add((*queue).tail as usize);
    (*queue).tail = ((*queue).tail + 1) & ((*queue).size - 1);

    result
}

unsafe fn pgaio_worker_submission_queue_depth() -> uint32 {
    let mut head: uint32;
    let tail: uint32;

    head = (*io_worker_submission_queue).head;
    tail = (*io_worker_submission_queue).tail;

    if tail > head {
        head += (*io_worker_submission_queue).size;
    }

    Assert!(head >= tail);

    head - tail
}

unsafe extern "C" fn pgaio_worker_needs_synchronous_execution(ioh: *mut PgAioHandle) -> bool {
    !IsUnderPostmaster
        || ((*ioh).flags & PGAIO_HF_REFERENCES_LOCAL as u8) != 0
        || !pgaio_io_can_reopen(ioh)
}

unsafe fn pgaio_worker_submit_internal(num_staged_ios: c_int, staged_ios: *mut *mut PgAioHandle) {
    let mut synchronous_ios: [*mut PgAioHandle; PGAIO_SUBMIT_BATCH_SIZE] =
        [std::ptr::null_mut(); PGAIO_SUBMIT_BATCH_SIZE];
    let mut nsync: c_int = 0;
    let mut wakeup: *mut Latch = std::ptr::null_mut();
    let mut worker: c_int;

    Assert!(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE as c_int);

    LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
    for i in 0..num_staged_ios {
        Assert!(!pgaio_worker_needs_synchronous_execution(
            *staged_ios.add(i as usize)
        ));
        if !pgaio_worker_submission_queue_insert(*staged_ios.add(i as usize)) {
            /*
             * We'll do it synchronously, but only after we've sent as many as
             * we can to workers, to maximize concurrency.
             */
            synchronous_ios[nsync as usize] = *staged_ios.add(i as usize);
            nsync += 1;
            continue;
        }

        if wakeup.is_null() {
            /* Choose an idle worker to wake up if we haven't already. */
            worker = pgaio_worker_choose_idle();
            if worker >= 0 {
                wakeup = (*(*io_worker_control).workers.as_ptr().add(worker as usize)).latch;
            }

            pgaio_debug_io!(
                DEBUG4,
                *staged_ios.add(i as usize),
                "choosing worker {}",
                worker
            );
        }
    }
    LWLockRelease(AioWorkerSubmissionQueueLock);

    if !wakeup.is_null() {
        SetLatch(wakeup);
    }

    /* Run whatever is left synchronously. */
    if nsync > 0 {
        for i in 0..nsync {
            pgaio_io_perform_synchronously(synchronous_ios[i as usize]);
        }
    }
}

unsafe extern "C" fn pgaio_worker_submit(
    num_staged_ios: uint16,
    staged_ios: *mut *mut PgAioHandle,
) -> c_int {
    for i in 0..num_staged_ios {
        let ioh: *mut PgAioHandle = *staged_ios.add(i as usize);

        pgaio_io_prepare_submit(ioh);
    }

    pgaio_worker_submit_internal(num_staged_ios as c_int, staged_ios);

    num_staged_ios as c_int
}

/*
 * on_shmem_exit() callback that releases the worker's slot in
 * io_worker_control.
 */
unsafe extern "C" fn pgaio_worker_die(code: c_int, arg: Datum) {
    LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
    Assert!((*(*io_worker_control).workers.as_ptr().add(MyIoWorkerId as usize)).in_use);
    Assert!((*(*io_worker_control).workers.as_ptr().add(MyIoWorkerId as usize)).latch == MyLatch);

    (*io_worker_control).idle_worker_mask &= !(1u64 << MyIoWorkerId);
    (*(*io_worker_control).workers.as_mut_ptr().add(MyIoWorkerId as usize)).in_use = false;
    (*(*io_worker_control).workers.as_mut_ptr().add(MyIoWorkerId as usize)).latch =
        std::ptr::null_mut();
    LWLockRelease(AioWorkerSubmissionQueueLock);
}

/*
 * Register the worker in shared memory, assign MyIoWorkerId and register a
 * shutdown callback to release registration.
 */
unsafe fn pgaio_worker_register() {
    MyIoWorkerId = -1;

    /*
     * XXX: This could do with more fine-grained locking. But it's also not
     * very common for the number of workers to change at the moment...
     */
    LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);

    for i in 0..MAX_IO_WORKERS {
        let slot = (*io_worker_control).workers.as_mut_ptr().add(i as usize);
        if !(*slot).in_use {
            Assert!((*slot).latch.is_null());
            (*slot).in_use = true;
            MyIoWorkerId = i;
            break;
        } else {
            Assert!(!(*slot).latch.is_null());
        }
    }

    if MyIoWorkerId == -1 {
        elog!(ERROR, "couldn't find a free worker slot");
    }

    (*io_worker_control).idle_worker_mask |= 1u64 << MyIoWorkerId;
    (*(*io_worker_control).workers.as_mut_ptr().add(MyIoWorkerId as usize)).latch = MyLatch;
    LWLockRelease(AioWorkerSubmissionQueueLock);

    on_shmem_exit(Some(pgaio_worker_die), 0);
}

unsafe extern "C" fn pgaio_worker_error_callback(arg: *mut c_void) {
    let owner: ProcNumber;
    let owner_proc: *mut PGPROC;
    let owner_pid: int32;
    let ioh: *mut PgAioHandle = arg as *mut PgAioHandle;

    if ioh.is_null() {
        return;
    }

    Assert!((*ioh).owner_procno != MyProcNumber);
    Assert!(MyBackendType == B_IO_WORKER);

    owner = (*ioh).owner_procno;
    owner_proc = GetPGProcByNumber(owner);
    owner_pid = (*owner_proc).pid;

    errcontext!("I/O worker executing I/O on behalf of process {}", owner_pid);
}

pub unsafe extern "C" fn IoWorkerMain(startup_data: *const c_void, startup_data_len: Size) {
    let mut local_sigjmp_buf: sigjmp_buf = std::mem::zeroed();
    let mut error_ioh: *mut PgAioHandle = std::ptr::null_mut();
    let mut errcallback: ErrorContextCallback = std::mem::zeroed();
    let mut error_errno: c_int = 0;
    let mut cmd: [c_char; 128] = [0; 128];

    MyBackendType = B_IO_WORKER;
    AuxiliaryProcessMainCommon();

    pqsignal(SIGHUP, SignalHandlerForConfigReload as usize);
    pqsignal(SIGINT, die as usize); /* to allow manually triggering worker restart */

    /*
     * Ignore SIGTERM, will get explicit shutdown via SIGUSR2 later in the
     * shutdown sequence, similar to checkpointer.
     */
    pqsignal(SIGTERM, SIG_IGN);
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler as usize);
    pqsignal(SIGUSR2, SignalHandlerForShutdownRequest as usize);

    /* also registers a shutdown callback to unregister */
    pgaio_worker_register();

    sprintf(cmd.as_mut_ptr(), c"%d".as_ptr(), MyIoWorkerId);
    set_ps_display(cmd.as_ptr());

    errcallback.callback = Some(pgaio_worker_error_callback);
    errcallback.previous = error_context_stack;
    error_context_stack = &mut errcallback;

    /* see PostgresMain() */
    if sigsetjmp(&mut local_sigjmp_buf, 1) != 0 {
        error_context_stack = std::ptr::null_mut();
        HOLD_INTERRUPTS!();

        EmitErrorReport();

        /*
         * In the - very unlikely - case that the IO failed in a way that
         * raises an error we need to mark the IO as failed.
         *
         * Need to do just enough error recovery so that we can mark the IO as
         * failed and then exit (postmaster will start a new worker).
         */
        LWLockReleaseAll();

        if !error_ioh.is_null() {
            /* should never fail without setting error_errno */
            Assert!(error_errno != 0);

            set_errno(error_errno);

            START_CRIT_SECTION!();
            pgaio_io_process_completion(error_ioh, -error_errno);
            END_CRIT_SECTION!();
        }

        proc_exit(1);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &mut local_sigjmp_buf;

    sigprocmask(SIG_SETMASK, &UnBlockSig, std::ptr::null_mut());

    while !ShutdownRequestPending {
        let io_index: uint32;
        let mut latches: [*mut Latch; IO_WORKER_WAKEUP_FANOUT] =
            [std::ptr::null_mut(); IO_WORKER_WAKEUP_FANOUT];
        let mut nlatches: c_int = 0;
        let mut nwakeups: c_int = 0;
        let mut worker: c_int;

        /*
         * Try to get a job to do.
         *
         * The lwlock acquisition also provides the necessary memory barrier
         * to ensure that we don't see an outdated data in the handle.
         */
        LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
        io_index = pgaio_worker_submission_queue_consume() as uint32;
        if io_index == (-1i32) as uint32 {
            /*
             * Nothing to do.  Mark self idle.
             *
             * XXX: Invent some kind of back pressure to reduce useless
             * wakeups?
             */
            (*io_worker_control).idle_worker_mask |= 1u64 << MyIoWorkerId;
        } else {
            /* Got one.  Clear idle flag. */
            (*io_worker_control).idle_worker_mask &= !(1u64 << MyIoWorkerId);

            /* See if we can wake up some peers. */
            nwakeups = Min(
                pgaio_worker_submission_queue_depth() as c_int,
                IO_WORKER_WAKEUP_FANOUT as c_int,
            );
            for _i in 0..nwakeups {
                worker = pgaio_worker_choose_idle();
                if worker < 0 {
                    break;
                }
                latches[nlatches as usize] =
                    (*(*io_worker_control).workers.as_ptr().add(worker as usize)).latch;
                nlatches += 1;
            }
        }
        LWLockRelease(AioWorkerSubmissionQueueLock);

        for i in 0..nlatches {
            SetLatch(latches[i as usize]);
        }

        if io_index != (-1i32) as uint32 {
            let ioh: *mut PgAioHandle;

            ioh = (*pgaio_ctl).io_handles.add(io_index as usize);
            error_ioh = ioh;
            errcallback.arg = ioh as *mut c_void;

            pgaio_debug_io!(DEBUG4, ioh, "worker {} processing IO", MyIoWorkerId);

            /*
             * Prevent interrupts between pgaio_io_reopen() and
             * pgaio_io_perform_synchronously() that otherwise could lead to
             * the FD getting closed in that window.
             */
            HOLD_INTERRUPTS!();

            /*
             * It's very unlikely, but possible, that reopen fails. E.g. due
             * to memory allocations failing or file permissions changing or
             * such.  In that case we need to fail the IO.
             *
             * There's not really a good errno we can report here.
             */
            error_errno = ENOENT;
            pgaio_io_reopen(ioh);

            /*
             * To be able to exercise the reopen-fails path, allow injection
             * points to trigger a failure at this point.
             */
            INJECTION_POINT!("aio-worker-after-reopen", ioh);

            error_errno = 0;
            error_ioh = std::ptr::null_mut();

            /*
             * As part of IO completion the buffer will be marked as NOACCESS,
             * until the buffer is pinned again - which never happens in io
             * workers. Therefore the next time there is IO for the same
             * buffer, the memory will be considered inaccessible. To avoid
             * that, explicitly allow access to the memory before reading data
             * into it.
             */
            // #ifdef USE_VALGRIND - not enabled

            /*
             * We don't expect this to ever fail with ERROR or FATAL, no need
             * to keep error_ioh set to the IO.
             * pgaio_io_perform_synchronously() contains a critical section to
             * ensure we don't accidentally fail.
             */
            pgaio_io_perform_synchronously(ioh);

            RESUME_INTERRUPTS!();
            errcallback.arg = std::ptr::null_mut();
        } else {
            WaitLatch(
                MyLatch,
                (WL_LATCH_SET | WL_EXIT_ON_PM_DEATH) as c_int,
                -1,
                WAIT_EVENT_IO_WORKER_MAIN,
            );
            ResetLatch(MyLatch);
        }

        CHECK_FOR_INTERRUPTS();

        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
    }

    error_context_stack = errcallback.previous;
    proc_exit(0);
}

pub unsafe fn pgaio_workers_enabled() -> bool {
    io_method == IOMETHOD_WORKER
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

pub use crate::storage::aio_internal::PgAioHandle;
pub type Latch = c_void;
#[repr(C)]
pub struct PGPROC {
    pub pid: c_int,
}
pub type ProcNumber = c_int;
pub type sigjmp_buf = [c_int; 64];

#[repr(C)]
pub struct IoMethodOps {
    pub shmem_size: Option<unsafe extern "C" fn() -> Size>,
    pub shmem_init: Option<unsafe extern "C" fn(bool)>,
    pub needs_synchronous_execution: Option<unsafe extern "C" fn(*mut PgAioHandle) -> bool>,
    pub submit: Option<unsafe extern "C" fn(uint16, *mut *mut PgAioHandle) -> c_int>,
}

impl IoMethodOps {
    pub const DEFAULT: IoMethodOps = IoMethodOps {
        shmem_size: None,
        shmem_init: None,
        needs_synchronous_execution: None,
        submit: None,
    };
}

#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(*mut c_void)>,
    pub arg: *mut c_void,
}

#[repr(C)]
pub struct PgAioCtl {
    pub io_handles: *mut PgAioHandle,
}

pub const MAX_IO_WORKERS: c_int = 32;
pub const PGAIO_SUBMIT_BATCH_SIZE: usize = 32;
pub const PGAIO_HF_REFERENCES_LOCAL: c_int = 1;

pub const DEBUG3: c_int = 14;
pub const DEBUG4: c_int = 15;
pub const ERROR: c_int = 21;

pub const LW_EXCLUSIVE: c_int = 0;

pub const IOMETHOD_WORKER: c_int = 1;

pub const SIGHUP: c_int = 1;
pub const SIGINT: c_int = 2;
pub const SIGTERM: c_int = 15;
pub const SIGALRM: c_int = 14;
pub const SIGPIPE: c_int = 13;
pub const SIGUSR1: c_int = 30;
pub const SIGUSR2: c_int = 31;
pub const SIG_IGN: usize = 1;
pub const SIG_SETMASK: c_int = 2;

pub const B_IO_WORKER: c_int = 0;
pub const PGC_SIGHUP: c_int = 0;

pub const WL_LATCH_SET: c_int = 1;
pub const WL_EXIT_ON_PM_DEATH: c_int = 16;
pub const WAIT_EVENT_IO_WORKER_MAIN: uint32 = 0;

pub const ENOENT: c_int = 2;

pub static mut AioWorkerSubmissionQueueLock: *mut c_void = std::ptr::null_mut();
pub static mut MyLatch: *mut Latch = std::ptr::null_mut();
pub static mut IsUnderPostmaster: bool = false;
pub static mut MyBackendType: c_int = 0;
pub static mut MyProcNumber: ProcNumber = 0;
pub static mut error_context_stack: *mut ErrorContextCallback = std::ptr::null_mut();
pub static mut PG_exception_stack: *mut sigjmp_buf = std::ptr::null_mut();
pub static mut ShutdownRequestPending: bool = false;
pub static mut ConfigReloadPending: bool = false;
pub static mut UnBlockSig: c_int = 0;
pub static mut pgaio_ctl: *mut PgAioCtl = std::ptr::null_mut();
pub static mut io_method: c_int = 0;

// Local no-op shims. These names already exist as #[macro_export] macros at the
// crate root (pgaio_debug / pgaio_debug_io live in crate::storage::aio_internal,
// whose real expansions reference symbols not yet ported). To avoid crate-root
// name collisions, these are plain module-local macros, not #[macro_export].
macro_rules! pgaio_debug {
    ($level:expr, $($arg:tt)*) => {{
        let _ = $level;
    }};
}
pub(crate) use pgaio_debug;

macro_rules! pgaio_debug_io {
    ($level:expr, $ioh:expr, $($arg:tt)*) => {{
        let _ = $level;
        let _ = $ioh;
    }};
}
pub(crate) use pgaio_debug_io;

macro_rules! errcontext {
    ($($arg:tt)*) => {{}};
}
pub(crate) use errcontext;

macro_rules! HOLD_INTERRUPTS {
    () => {{}};
}
pub(crate) use HOLD_INTERRUPTS;

macro_rules! RESUME_INTERRUPTS {
    () => {{}};
}
pub(crate) use RESUME_INTERRUPTS;

macro_rules! START_CRIT_SECTION {
    () => {{}};
}
pub(crate) use START_CRIT_SECTION;

macro_rules! END_CRIT_SECTION {
    () => {{}};
}
pub(crate) use END_CRIT_SECTION;

macro_rules! INJECTION_POINT {
    ($name:expr, $arg:expr) => {{
        let _ = $arg;
    }};
}
pub(crate) use INJECTION_POINT;

#[inline]
fn Min(a: c_int, b: c_int) -> c_int {
    if a < b {
        a
    } else {
        b
    }
}

unsafe fn set_errno(e: c_int) {
    // TODO: errno set; placeholder
    let _ = e;
}

unsafe fn pg_nextpower2_32(num: uint32) -> uint32 {
    unimplemented!() // TODO: port/pg_bitutils.h
}

unsafe fn pg_rightmost_one_pos64(word: uint64) -> c_int {
    unimplemented!() // TODO: port/pg_bitutils.h
}

unsafe fn add_size(s1: Size, s2: Size) -> Size {
    unimplemented!() // TODO: storage/shmem.h
}

unsafe fn ShmemInitStruct(name: *const c_char, size: Size, found_ptr: *mut bool) -> *mut c_void {
    unimplemented!() // TODO: storage/shmem.h
}

unsafe fn pgaio_io_get_id(ioh: *mut PgAioHandle) -> c_int {
    unimplemented!() // TODO: storage/aio.h
}

unsafe fn pgaio_io_can_reopen(ioh: *mut PgAioHandle) -> bool {
    unimplemented!() // TODO: storage/aio_internal.h
}

unsafe fn pgaio_io_perform_synchronously(ioh: *mut PgAioHandle) {
    unimplemented!() // TODO: storage/aio_internal.h
}

unsafe fn pgaio_io_prepare_submit(ioh: *mut PgAioHandle) {
    unimplemented!() // TODO: storage/aio_internal.h
}

unsafe fn pgaio_io_process_completion(ioh: *mut PgAioHandle, result: c_int) {
    unimplemented!() // TODO: storage/aio_internal.h
}

unsafe fn pgaio_io_reopen(ioh: *mut PgAioHandle) {
    unimplemented!() // TODO: storage/aio_internal.h
}

unsafe fn LWLockAcquire(lock: *mut c_void, mode: c_int) -> bool {
    unimplemented!() // TODO: storage/lwlock.h
}

unsafe fn LWLockRelease(lock: *mut c_void) {
    unimplemented!() // TODO: storage/lwlock.h
}

unsafe fn LWLockReleaseAll() {
    unimplemented!() // TODO: storage/lwlock.h
}

unsafe fn SetLatch(latch: *mut Latch) {
    unimplemented!() // TODO: storage/latch.h
}

unsafe fn ResetLatch(latch: *mut Latch) {
    unimplemented!() // TODO: storage/latch.h
}

unsafe fn WaitLatch(latch: *mut Latch, wakeEvents: c_int, timeout: i64, wait_event_info: uint32) -> c_int {
    unimplemented!() // TODO: storage/latch.h
}

unsafe fn on_shmem_exit(function: Option<unsafe extern "C" fn(c_int, Datum)>, arg: Datum) {
    unimplemented!() // TODO: storage/ipc.h
}

unsafe fn AuxiliaryProcessMainCommon() {
    unimplemented!() // TODO: postmaster/auxprocess.h
}

unsafe fn pqsignal(signo: c_int, func: usize) -> usize {
    unimplemented!() // TODO: libpq/pqsignal.h
}

unsafe extern "C" fn SignalHandlerForConfigReload(signo: c_int) {
    unimplemented!() // TODO: postmaster/interrupt.h
}

unsafe extern "C" fn SignalHandlerForShutdownRequest(signo: c_int) {
    unimplemented!() // TODO: postmaster/interrupt.h
}

unsafe extern "C" fn die(signo: c_int) {
    unimplemented!() // TODO: tcop/tcopprot.h
}

unsafe extern "C" fn procsignal_sigusr1_handler(signo: c_int) {
    unimplemented!() // TODO: storage/procsignal.h
}

unsafe fn set_ps_display(activity: *const c_char) {
    unimplemented!() // TODO: utils/ps_status.h
}

unsafe fn EmitErrorReport() {
    unimplemented!() // TODO: utils/elog.h
}

unsafe fn proc_exit(code: c_int) {
    unimplemented!() // TODO: storage/ipc.h
}

unsafe fn GetPGProcByNumber(procno: ProcNumber) -> *mut PGPROC {
    unimplemented!() // TODO: storage/proc.h
}

unsafe fn ProcessConfigFile(context: c_int) {
    unimplemented!() // TODO: utils/guc.h
}

unsafe fn sigsetjmp(env: *mut sigjmp_buf, savesigs: c_int) -> c_int {
    unimplemented!() // TODO: setjmp.h
}

unsafe fn sigprocmask(how: c_int, set: *const c_int, oldset: *mut c_int) -> c_int {
    unimplemented!() // TODO: signal.h
}

extern "C" {
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
}
