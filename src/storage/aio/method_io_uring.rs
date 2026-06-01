//! src/backend/storage/aio/method_io_uring.c
//!
//! AIO - perform AIO using Linux' io_uring
//!
//! For now we create one io_uring instance for each backend. These io_uring
//! instances have to be created in postmaster, during startup, to allow other
//! backends to process IO completions, if the issuing backend is currently
//! busy doing other things. Other backends may not use another backend's
//! io_uring instance to submit IO, that'd require additional locking that
//! would likely be harmful for performance.
//!
//! We likely will want to introduce a backend-local io_uring instance in the
//! future, e.g. for FE/BE network IO.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/aio/method_io_uring.c

use crate::prelude::*;

// This is a platform file: io_uring is Linux-only. Everything below is
// gated by IOMETHOD_IO_URING_ENABLED in the C source. On Darwin we cannot
// build against <liburing.h>, so the Linux-only bodies are gated with
// #[cfg(any())] /* TODO(pg-port): Linux io_uring */ and a Darwin-safe stub
// for the public API is provided at the bottom.

/* number of completions processed at once */
const PGAIO_MAX_LOCAL_COMPLETED_IO: usize = 32;

const PG_CACHE_LINE_SIZE: usize = 128;

/*
 * Per-backend state when using io_method=io_uring
 *
 * Align the whole struct to a cacheline boundary, to prevent false sharing
 * between completion_lock and prior backend's io_uring_ring.
 */
#[repr(C, align(128))]
pub struct PgAioUringContext {
    /*
     * Multiple backends can process completions for this backend's io_uring
     * instance (e.g. when the backend issuing IO is busy doing something
     * else).  To make that safe we have to ensure that only a single backend
     * gets io completions from the io_uring instance at a time.
     */
    pub completion_lock: LWLock,

    pub io_uring_ring: io_uring,
}

/*
 * Information about the capabilities that io_uring has.
 *
 * Depending on liburing and kernel version different features are
 * supported. At least for the kernel a kernel version check does not suffice
 * as various vendors do backport features to older kernels :(.
 */
#[repr(C)]
pub struct PgAioUringCaps {
    pub checked: bool,
    /* -1 if io_uring_queue_init_mem() is unsupported */
    pub mem_init_size: c_int,
}

pub const pgaio_uring_ops: IoMethodOps = IoMethodOps {
    /*
     * While io_uring mostly is OK with FDs getting closed while the IO is in
     * flight, that is not true for IOs submitted with IOSQE_ASYNC.
     *
     * See
     * https://postgr.es/m/5ons2rtmwarqqhhexb3dnqulw5rjgwgoct57vpdau4rujlrffj%403fls6d2mkiwc
     */
    wait_on_fd_before_close: true,

    shmem_size: Some(pgaio_uring_shmem_size),
    shmem_init: Some(pgaio_uring_shmem_init),
    init_backend: Some(pgaio_uring_init_backend),

    submit: Some(pgaio_uring_submit),
    wait_one: Some(pgaio_uring_wait_one),
    ..IoMethodOps::DEFAULT
};

/* PgAioUringContexts for all backends */
static mut pgaio_uring_contexts: *mut PgAioUringContext = std::ptr::null_mut();

/* the current backend's context */
static mut pgaio_my_uring_context: *mut PgAioUringContext = std::ptr::null_mut();

static mut pgaio_uring_caps: PgAioUringCaps = PgAioUringCaps {
    checked: false,
    mem_init_size: -1,
};

unsafe fn pgaio_uring_procs() -> uint32 {
    /*
     * We can subtract MAX_IO_WORKERS here as io workers are never used at the
     * same time as io_method=io_uring.
     */
    (MaxBackends + NUM_AUXILIARY_PROCS - MAX_IO_WORKERS) as uint32
}

/*
 * Initializes pgaio_uring_caps, unless that's already done.
 */
#[cfg(any())] /* TODO(pg-port): Linux io_uring */
unsafe fn pgaio_uring_check_capabilities() {
    if pgaio_uring_caps.checked {
        return;
    }

    /*
     * By default io_uring creates a shared memory mapping for each io_uring
     * instance, leading to a large number of memory mappings. Unfortunately a
     * large number of memory mappings slows things down, backend exit is
     * particularly affected.  To address that, newer kernels (6.5) support
     * using user-provided memory for the memory, by putting the relevant
     * memory into shared memory we don't need any additional mappings.
     *
     * To know whether this is supported, we unfortunately need to probe the
     * kernel by trying to create a ring with userspace-provided memory. This
     * also has a secondary benefit: We can determine precisely how much
     * memory we need for each io_uring instance.
     */
    // #if defined(HAVE_IO_URING_QUEUE_INIT_MEM) && defined(IORING_SETUP_NO_MMAP)
    {
        let mut test_ring: io_uring = std::mem::zeroed();
        let mut ring_size: Size;
        let ring_ptr: *mut c_void;
        let mut p: io_uring_params = std::mem::zeroed();
        let ret: c_int;

        /*
         * Liburing does not yet provide an API to query how much memory a
         * ring will need. So we over-estimate it here. As the memory is freed
         * just below that's small temporary waste of memory.
         *
         * 1MB is more than enough for rings within io_max_concurrency's
         * range.
         */
        ring_size = 1024 * 1024;

        /*
         * Hard to believe a system exists where 1MB would not be a multiple
         * of the page size. But it's cheap to ensure...
         */
        ring_size -= ring_size % (sysconf(_SC_PAGESIZE) as Size);

        ring_ptr = mmap(
            std::ptr::null_mut(),
            ring_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            -1,
            0,
        );
        if ring_ptr == MAP_FAILED {
            elog!(
                ERROR,
                "mmap({}) to determine io_uring_queue_init_mem() support failed: {}",
                ring_size,
                pg_strerror_m()
            );
        }

        ret = io_uring_queue_init_mem(
            io_max_concurrency,
            &mut test_ring,
            &mut p,
            ring_ptr,
            ring_size,
        );
        if ret > 0 {
            pgaio_uring_caps.mem_init_size = ret;

            elog!(
                DEBUG1,
                "can use combined memory mapping for io_uring, each ring needs {} bytes",
                ret
            );

            /* clean up the created ring, it was just for a test */
            io_uring_queue_exit(&mut test_ring);
        } else {
            /*
             * There are different reasons for ring creation to fail, but it's
             * ok to treat that just as io_uring_queue_init_mem() not being
             * supported. We'll report a more detailed error in
             * pgaio_uring_shmem_init().
             */
            set_errno(-ret);
            elog!(
                DEBUG1,
                "cannot use combined memory mapping for io_uring, ring creation failed: {}",
                pg_strerror_m()
            );
        }

        if munmap(ring_ptr, ring_size) != 0 {
            elog!(ERROR, "munmap() failed: {}", pg_strerror_m());
        }
    }
    // #else: elog(DEBUG1, "can't use combined memory mapping for io_uring, kernel or liburing too old");

    pgaio_uring_caps.checked = true;
}

/* Darwin-safe stub: io_uring is not available. */
#[cfg(not(any()))]
unsafe fn pgaio_uring_check_capabilities() {
    if pgaio_uring_caps.checked {
        return;
    }
    /* TODO(pg-port): Linux io_uring - combined memory mapping unsupported here. */
    pgaio_uring_caps.checked = true;
}

/*
 * Memory for all PgAioUringContext instances
 */
unsafe fn pgaio_uring_context_shmem_size() -> Size {
    mul_size(
        pgaio_uring_procs() as Size,
        std::mem::size_of::<PgAioUringContext>() as Size,
    )
}

/*
 * Memory for the combined memory used by io_uring instances. Returns 0 if
 * that is not supported by kernel/liburing.
 */
unsafe fn pgaio_uring_ring_shmem_size() -> Size {
    let mut sz: Size = 0;

    if pgaio_uring_caps.mem_init_size > 0 {
        /*
         * Memory for rings needs to be allocated to the page boundary,
         * reserve space. Luckily it does not need to be aligned to hugepage
         * boundaries, even if huge pages are used.
         */
        sz = add_size(sz, sysconf(_SC_PAGESIZE) as Size);
        sz = add_size(
            sz,
            mul_size(
                pgaio_uring_procs() as Size,
                pgaio_uring_caps.mem_init_size as Size,
            ),
        );
    }

    sz
}

unsafe extern "C" fn pgaio_uring_shmem_size() -> Size {
    let mut sz: Size;

    /*
     * Kernel and liburing support for various features influences how much
     * shmem we need, perform the necessary checks.
     */
    pgaio_uring_check_capabilities();

    sz = pgaio_uring_context_shmem_size();
    sz = add_size(sz, pgaio_uring_ring_shmem_size());

    sz
}

unsafe extern "C" fn pgaio_uring_shmem_init(first_time: bool) {
    let _ = first_time;
    let TotalProcs: c_int = pgaio_uring_procs() as c_int;
    let mut found: bool = false;
    let mut shmem: *mut c_char;
    let mut ring_mem_remain: Size = 0;
    let mut ring_mem_next: *mut c_char = std::ptr::null_mut();

    /*
     * We allocate memory for all PgAioUringContext instances and, if
     * supported, the memory required for each of the io_uring instances, in
     * one ShmemInitStruct().
     */
    shmem = ShmemInitStruct(
        c"AioUringContext".as_ptr(),
        pgaio_uring_shmem_size(),
        &mut found,
    ) as *mut c_char;
    if found {
        return;
    }

    pgaio_uring_contexts = shmem as *mut PgAioUringContext;
    shmem = shmem.add(pgaio_uring_context_shmem_size());

    /* if supported, handle memory alignment / sizing for io_uring memory */
    if pgaio_uring_caps.mem_init_size > 0 {
        ring_mem_remain = pgaio_uring_ring_shmem_size();
        ring_mem_next = shmem;

        /* align to page boundary, see also pgaio_uring_ring_shmem_size() */
        ring_mem_next = TYPEALIGN(sysconf(_SC_PAGESIZE) as Size, ring_mem_next as usize) as *mut c_char;

        /* account for alignment */
        ring_mem_remain -= ring_mem_next as usize - shmem as usize;
        shmem = shmem.add(ring_mem_next as usize - shmem as usize);

        shmem = shmem.add(ring_mem_remain);
    }
    let _ = shmem;

    for contextno in 0..TotalProcs {
        let context: *mut PgAioUringContext = pgaio_uring_contexts.add(contextno as usize);
        let ret: c_int;

        /*
         * Right now a high TotalProcs will cause problems in two ways:
         *
         * - RLIMIT_NOFILE needs to be big enough to allow all
         * io_uring_queue_init() calls to succeed.
         *
         * - RLIMIT_NOFILE needs to be big enough to still have enough file
         * descriptors to satisfy set_max_safe_fds() left over. Or, even
         * better, have max_files_per_process left over FDs.
         *
         * We probably should adjust the soft RLIMIT_NOFILE to ensure that.
         *
         *
         * XXX: Newer versions of io_uring support sharing the workers that
         * execute some asynchronous IOs between io_uring instances. It might
         * be worth using that - also need to evaluate if that causes
         * noticeable additional contention?
         */

        /*
         * If supported (c.f. pgaio_uring_check_capabilities()), create ring
         * with its data in shared memory. Otherwise fall back io_uring
         * creating a memory mapping for each ring.
         */
        ret = pgaio_uring_init_one_ring(context, &mut ring_mem_next, &mut ring_mem_remain);

        if ret < 0 {
            let mut hint: *mut c_char = std::ptr::null_mut();
            let mut err: c_int = ERRCODE_INTERNAL_ERROR;

            /* add hints for some failures that errno explains sufficiently */
            if -ret == EPERM {
                err = ERRCODE_INSUFFICIENT_PRIVILEGE;
                hint = gettext(c"Check if io_uring is disabled via /proc/sys/kernel/io_uring_disabled.".as_ptr()) as *mut c_char;
            } else if -ret == EMFILE {
                err = ERRCODE_INSUFFICIENT_RESOURCES;
                hint = psprintf(
                    gettext(c"Consider increasing \"ulimit -n\" to at least %d.".as_ptr()),
                    TotalProcs + max_files_per_process,
                );
            } else if -ret == ENOSYS {
                err = ERRCODE_FEATURE_NOT_SUPPORTED;
                hint = gettext(c"The kernel does not support io_uring.".as_ptr()) as *mut c_char;
            }

            /* update errno to allow %m to work */
            set_errno(-ret);

            let _ = err;
            /* C also: errcode(err); errhint("%s", hint) when hint != NULL */
            if !hint.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not setup io_uring queue: {}",
                        pg_strerror_m()
                    )
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not setup io_uring queue: {}",
                        pg_strerror_m()
                    )
                );
            }
        }

        LWLockInitialize(&mut (*context).completion_lock, LWTRANCHE_AIO_URING_COMPLETION);
    }
}

/*
 * Helper for pgaio_uring_shmem_init(): create one ring, advancing the shared
 * ring-memory cursor when combined memory mapping is in use.
 */
#[cfg(any())] /* TODO(pg-port): Linux io_uring */
unsafe fn pgaio_uring_init_one_ring(
    context: *mut PgAioUringContext,
    ring_mem_next: *mut *mut c_char,
    ring_mem_remain: *mut Size,
) -> c_int {
    let ret: c_int;
    // #if defined(HAVE_IO_URING_QUEUE_INIT_MEM) && defined(IORING_SETUP_NO_MMAP)
    if pgaio_uring_caps.mem_init_size > 0 {
        let mut p: io_uring_params = std::mem::zeroed();

        ret = io_uring_queue_init_mem(
            io_max_concurrency,
            &mut (*context).io_uring_ring,
            &mut p,
            *ring_mem_next as *mut c_void,
            *ring_mem_remain,
        );

        *ring_mem_remain -= ret as Size;
        *ring_mem_next = (*ring_mem_next).add(ret as usize);
    } else {
        ret = io_uring_queue_init(io_max_concurrency, &mut (*context).io_uring_ring, 0);
    }
    ret
}

/* Darwin-safe stub. */
#[cfg(not(any()))]
unsafe fn pgaio_uring_init_one_ring(
    context: *mut PgAioUringContext,
    ring_mem_next: *mut *mut c_char,
    ring_mem_remain: *mut Size,
) -> c_int {
    let _ = (context, ring_mem_next, ring_mem_remain);
    /* TODO(pg-port): Linux io_uring - no ring to initialize on this platform. */
    0
}

unsafe extern "C" fn pgaio_uring_init_backend() {
    Assert!((MyProcNumber as uint32) < pgaio_uring_procs());

    pgaio_my_uring_context = pgaio_uring_contexts.add(MyProcNumber as usize);
}

#[cfg(any())] /* TODO(pg-port): Linux io_uring */
unsafe extern "C" fn pgaio_uring_submit(
    num_staged_ios: uint16,
    staged_ios: *mut *mut PgAioHandle,
) -> c_int {
    let uring_instance: *mut io_uring = &mut (*pgaio_my_uring_context).io_uring_ring;
    let mut in_flight_before: c_int = dclist_count(&mut (*pgaio_my_backend).in_flight_ios);

    Assert!(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE as uint16);

    for i in 0..num_staged_ios {
        let ioh: *mut PgAioHandle = *staged_ios.add(i as usize);
        let sqe: *mut io_uring_sqe;

        sqe = io_uring_get_sqe(uring_instance);

        if sqe.is_null() {
            elog!(ERROR, "io_uring submission queue is unexpectedly full");
        }

        pgaio_io_prepare_submit(ioh);
        pgaio_uring_sq_from_io(ioh, sqe);

        /*
         * io_uring executes IO in process context if possible. That's
         * generally good, as it reduces context switching. When performing a
         * lot of buffered IO that means that copying between page cache and
         * userspace memory happens in the foreground, as it can't be
         * offloaded to DMA hardware as is possible when using direct IO. When
         * executing a lot of buffered IO this causes io_uring to be slower
         * than worker mode, as worker mode parallelizes the copying. io_uring
         * can be told to offload work to worker threads instead.
         *
         * If an IO is buffered IO and we already have IOs in flight or
         * multiple IOs are being submitted, we thus tell io_uring to execute
         * the IO in the background. We don't do so for the first few IOs
         * being submitted as executing in this process' context has lower
         * latency.
         */
        if in_flight_before > 4 && ((*ioh).flags & PGAIO_HF_BUFFERED as u8) != 0 {
            io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
        }

        in_flight_before += 1;
    }

    loop {
        let ret: c_int;

        pgstat_report_wait_start(WAIT_EVENT_AIO_IO_URING_SUBMIT);
        ret = io_uring_submit(uring_instance);
        pgstat_report_wait_end();

        if ret == -EINTR {
            pgaio_debug!(
                DEBUG3,
                "aio method uring: submit EINTR, nios: {}",
                num_staged_ios
            );
        } else if ret < 0 {
            /*
             * The io_uring_enter() manpage suggests that the appropriate
             * reaction to EAGAIN is:
             *
             * "The application should wait for some completions and try
             * again"
             *
             * However, it seems unlikely that that would help in our case, as
             * we apply a low limit to the number of outstanding IOs and thus
             * also outstanding completions, making it unlikely that we'd get
             * EAGAIN while the OS is in good working order.
             *
             * Additionally, it would be problematic to just wait here, our
             * caller might hold critical locks. It'd possibly lead to
             * delaying the crash-restart that seems likely to occur when the
             * kernel is under such heavy memory pressure.
             *
             * Update errno to allow %m to work.
             */
            set_errno(-ret);
            elog!(PANIC, "io_uring submit failed: {}", pg_strerror_m());
        } else if ret != num_staged_ios as c_int {
            /* likely unreachable, but if it is, we would need to re-submit */
            elog!(
                PANIC,
                "io_uring submit submitted only {} of {}",
                ret,
                num_staged_ios
            );
        } else {
            pgaio_debug!(
                DEBUG4,
                "aio method uring: submitted {} IOs",
                num_staged_ios
            );
            break;
        }
    }

    num_staged_ios as c_int
}

/* Darwin-safe stub. */
#[cfg(not(any()))]
unsafe extern "C" fn pgaio_uring_submit(
    num_staged_ios: uint16,
    staged_ios: *mut *mut PgAioHandle,
) -> c_int {
    let _ = staged_ios;
    /* TODO(pg-port): Linux io_uring - no ring to submit to on this platform. */
    num_staged_ios as c_int
}

unsafe extern "C" fn pgaio_uring_completion_error_callback(arg: *mut c_void) {
    let owner: ProcNumber;
    let owner_proc: *mut PGPROC;
    let owner_pid: int32;
    let ioh: *mut PgAioHandle = arg as *mut PgAioHandle;

    if ioh.is_null() {
        return;
    }

    /* No need for context if a backend is completing the IO for itself */
    if (*ioh).owner_procno == MyProcNumber {
        return;
    }

    owner = (*ioh).owner_procno;
    owner_proc = GetPGProcByNumber(owner);
    owner_pid = (*owner_proc).pid;

    errcontext!("completing I/O on behalf of process {}", owner_pid);
}

#[cfg(any())] /* TODO(pg-port): Linux io_uring */
unsafe fn pgaio_uring_drain_locked(context: *mut PgAioUringContext) {
    let mut ready: c_int;
    let orig_ready: c_int;
    let mut errcallback: ErrorContextCallback = std::mem::zeroed();

    Assert!(LWLockHeldByMeInMode(&mut (*context).completion_lock, LW_EXCLUSIVE));

    errcallback.callback = Some(pgaio_uring_completion_error_callback);
    errcallback.previous = error_context_stack;
    error_context_stack = &mut errcallback;

    /*
     * Don't drain more events than available right now. Otherwise it's
     * plausible that one backend could get stuck, for a while, receiving CQEs
     * without actually processing them.
     */
    orig_ready = io_uring_cq_ready(&mut (*context).io_uring_ring) as c_int;
    ready = orig_ready;

    while ready > 0 {
        let mut cqes: [*mut io_uring_cqe; PGAIO_MAX_LOCAL_COMPLETED_IO] =
            [std::ptr::null_mut(); PGAIO_MAX_LOCAL_COMPLETED_IO];
        let ncqes: uint32;

        START_CRIT_SECTION!();
        ncqes = io_uring_peek_batch_cqe(
            &mut (*context).io_uring_ring,
            cqes.as_mut_ptr(),
            Min(PGAIO_MAX_LOCAL_COMPLETED_IO as c_int, ready) as c_uint,
        );
        Assert!(ncqes <= ready as uint32);

        ready -= ncqes as c_int;

        for i in 0..ncqes {
            let cqe: *mut io_uring_cqe = cqes[i as usize];
            let ioh: *mut PgAioHandle = io_uring_cqe_get_data(cqe) as *mut PgAioHandle;
            let result: c_int = (*cqe).res;

            errcallback.arg = ioh as *mut c_void;

            io_uring_cqe_seen(&mut (*context).io_uring_ring, cqe);

            pgaio_io_process_completion(ioh, result);
            errcallback.arg = std::ptr::null_mut();
        }

        END_CRIT_SECTION!();

        pgaio_debug!(
            DEBUG3,
            "drained {}/{}, now expecting {}",
            ncqes,
            orig_ready,
            io_uring_cq_ready(&mut (*context).io_uring_ring)
        );
    }

    error_context_stack = errcallback.previous;
}

/* Darwin-safe stub. */
#[cfg(not(any()))]
unsafe fn pgaio_uring_drain_locked(context: *mut PgAioUringContext) {
    let _ = context;
    /* TODO(pg-port): Linux io_uring - no completion queue to drain on this platform. */
}

#[cfg(any())] /* TODO(pg-port): Linux io_uring */
unsafe extern "C" fn pgaio_uring_wait_one(ioh: *mut PgAioHandle, ref_generation: uint64) {
    let mut state: PgAioHandleState = std::mem::zeroed();
    let owner_procno: ProcNumber = (*ioh).owner_procno;
    let owner_context: *mut PgAioUringContext = pgaio_uring_contexts.add(owner_procno as usize);
    let mut expect_cqe: bool;
    let mut waited: c_int = 0;

    /*
     * XXX: It would be nice to have a smarter locking scheme, nearly all the
     * time the backend owning the ring will consume the completions, making
     * the locking unnecessarily expensive.
     */
    LWLockAcquire(&mut (*owner_context).completion_lock, LW_EXCLUSIVE);

    loop {
        pgaio_debug_io!(
            DEBUG3,
            ioh,
            "wait_one io_gen: {}, ref_gen: {}, cycle {}",
            (*ioh).generation,
            ref_generation,
            waited
        );

        if pgaio_io_was_recycled(ioh, ref_generation, &mut state)
            || state != PGAIO_HS_SUBMITTED
        {
            /* the IO was completed by another backend */
            break;
        } else if io_uring_cq_ready(&mut (*owner_context).io_uring_ring) != 0 {
            /* no need to wait in the kernel, io_uring has a completion */
            expect_cqe = true;
        } else {
            let ret: c_int;
            let mut cqes: *mut io_uring_cqe = std::ptr::null_mut();

            /* need to wait in the kernel */
            pgstat_report_wait_start(WAIT_EVENT_AIO_IO_URING_EXECUTION);
            ret = io_uring_wait_cqes(
                &mut (*owner_context).io_uring_ring,
                &mut cqes,
                1,
                std::ptr::null(),
                std::ptr::null(),
            );
            pgstat_report_wait_end();

            if ret == -EINTR {
                continue;
            } else if ret != 0 {
                /* see comment after io_uring_submit() */
                set_errno(-ret);
                elog!(PANIC, "io_uring wait failed: {}", pg_strerror_m());
            } else {
                Assert!(!cqes.is_null());
                expect_cqe = true;
                waited += 1;
            }
        }

        if expect_cqe {
            pgaio_uring_drain_locked(owner_context);
        }
    }

    LWLockRelease(&mut (*owner_context).completion_lock);

    pgaio_debug!(DEBUG3, "wait_one with {} sleeps", waited);
}

/* Darwin-safe stub. */
#[cfg(not(any()))]
unsafe extern "C" fn pgaio_uring_wait_one(ioh: *mut PgAioHandle, ref_generation: uint64) {
    let _ = (ioh, ref_generation);
    /* TODO(pg-port): Linux io_uring - nothing to wait on on this platform. */
}

#[cfg(any())] /* TODO(pg-port): Linux io_uring */
unsafe fn pgaio_uring_sq_from_io(ioh: *mut PgAioHandle, sqe: *mut io_uring_sqe) {
    let iov: *mut iovec;

    match (*ioh).op as PgAioOp {
        PGAIO_OP_READV => {
            iov = (*pgaio_ctl).iovecs.add((*ioh).iovec_off as usize);
            if (*ioh).op_data.read.iov_length == 1 {
                io_uring_prep_read(
                    sqe,
                    (*ioh).op_data.read.fd,
                    (*iov).iov_base,
                    (*iov).iov_len as c_uint,
                    (*ioh).op_data.read.offset,
                );
            } else {
                io_uring_prep_readv(
                    sqe,
                    (*ioh).op_data.read.fd,
                    iov,
                    (*ioh).op_data.read.iov_length as c_uint,
                    (*ioh).op_data.read.offset,
                );
            }
        }

        PGAIO_OP_WRITEV => {
            iov = (*pgaio_ctl).iovecs.add((*ioh).iovec_off as usize);
            if (*ioh).op_data.write.iov_length == 1 {
                io_uring_prep_write(
                    sqe,
                    (*ioh).op_data.write.fd,
                    (*iov).iov_base,
                    (*iov).iov_len as c_uint,
                    (*ioh).op_data.write.offset,
                );
            } else {
                io_uring_prep_writev(
                    sqe,
                    (*ioh).op_data.write.fd,
                    iov,
                    (*ioh).op_data.write.iov_length as c_uint,
                    (*ioh).op_data.write.offset,
                );
            }
        }

        PGAIO_OP_INVALID => {
            elog!(ERROR, "trying to prepare invalid IO operation for execution");
        }

        _ => {}
    }

    io_uring_sqe_set_data(sqe, ioh as *mut c_void);
}

/* Darwin-safe stub. */
#[cfg(not(any()))]
unsafe fn pgaio_uring_sq_from_io(ioh: *mut PgAioHandle, sqe: *mut io_uring_sqe) {
    let _ = (ioh, sqe);
    /* TODO(pg-port): Linux io_uring - no submission queue entry to fill on this platform. */
}
