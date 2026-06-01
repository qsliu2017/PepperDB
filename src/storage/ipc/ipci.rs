//! storage/ipc/ipci.c - POSTGRES inter-process communication initialization code.

use crate::prelude::*;

use crate::miscadmin::process_shmem_requests_in_progress;
use crate::utils::hash::dynahash::hash_estimate_size;
use crate::utils::init::globals::IsUnderPostmaster;

use crate::storage::pg_shmem::{
    GetHugePageSize, PGSharedMemoryCreate, PGShmemHeader, DEFAULT_SHARED_MEMORY_TYPE,
};
use crate::storage::pg_sema::{PGReserveSemaphores, PGSemaphoreShmemSize};

// storage/shmem.h add_size(): overflow-checked addition of shared sizes.
// Reported as a real helper in dynahash but not exported there; mirror the
// arithmetic locally as the other storage units do.
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    let result = s1.wrapping_add(s2);
    if result < s1 {
        ereport!(ERROR, "requested shared memory size overflows size_t");
    }
    result
}

/*
 * shmem.h ShmemIndexEnt / SHMEM_INDEX_SIZE.  The shmem.c index hashtable has
 * not been ported yet; reproduce the sizing constants locally so
 * CalculateShmemSize() estimates the same block as upstream.
 */
const SHMEM_INDEX_SIZE: c_long = 64;
#[repr(C)]
struct ShmemIndexEnt {
    key: [c_char; 48], /* SHMEM_INDEX_KEYSIZE */
    location: *mut c_void,
    size: Size,
    allocated_size: Size,
}

/* GUC context / source enum values (utils/guc.h). */
const PGC_INTERNAL: c_int = 0;
const PGC_S_DYNAMIC_DEFAULT: c_int = 0;

/*
 * Type of the shmem startup hook (storage/ipc.h).  Modules with shmem
 * allocations install one of these; not yet wired up.
 */
type shmem_startup_hook_type = Option<unsafe extern "C" fn()>;

/* GUCs */
#[no_mangle]
pub static mut shared_memory_type: c_int = DEFAULT_SHARED_MEMORY_TYPE;

#[no_mangle]
pub static mut shmem_startup_hook: shmem_startup_hook_type = None;

static mut total_addin_request: Size = 0;

/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This may only be called via the shmem_request_hook of a library that is
 * loaded into the postmaster via shared_preload_libraries.  Calls from
 * elsewhere will fail.
 */
pub unsafe fn RequestAddinShmemSpace(size: Size) {
    if !process_shmem_requests_in_progress {
        elog!(
            FATAL,
            "cannot request additional shared memory outside shmem_request_hook"
        );
    }
    total_addin_request = add_size(total_addin_request, size);
}

/*
 * CalculateShmemSize
 *		Calculates the amount of shared memory and number of semaphores needed.
 *
 * If num_semaphores is not NULL, it will be set to the number of semaphores
 * required.
 */
pub unsafe fn CalculateShmemSize(num_semaphores: *mut c_int) -> Size {
    let mut size: Size;
    let numSemas: c_int;

    /* Compute number of semaphores we'll need */
    numSemas = ProcGlobalSemas();

    /* Return the number of semaphores if requested by the caller */
    if !num_semaphores.is_null() {
        *num_semaphores = numSemas;
    }

    /*
     * Size of the Postgres shared-memory block is estimated via moderately-
     * accurate estimates for the big hogs, plus 100K for the stuff that's too
     * small to bother with estimating.
     *
     * We take some care to ensure that the total size request doesn't
     * overflow size_t.  If this gets through, we don't need to be so careful
     * during the actual allocation phase.
     */
    size = 100000;
    size = add_size(size, PGSemaphoreShmemSize(numSemas));
    size = add_size(
        size,
        hash_estimate_size(SHMEM_INDEX_SIZE, size_of::<ShmemIndexEnt>()),
    );
    size = add_size(size, dsm_estimate_size());
    size = add_size(size, DSMRegistryShmemSize());
    size = add_size(size, BufferManagerShmemSize());
    size = add_size(size, LockManagerShmemSize());
    size = add_size(size, PredicateLockShmemSize());
    size = add_size(size, ProcGlobalShmemSize());
    size = add_size(size, XLogPrefetchShmemSize());
    size = add_size(size, VarsupShmemSize());
    size = add_size(size, XLOGShmemSize());
    size = add_size(size, XLogRecoveryShmemSize());
    size = add_size(size, CLOGShmemSize());
    size = add_size(size, CommitTsShmemSize());
    size = add_size(size, SUBTRANSShmemSize());
    size = add_size(size, TwoPhaseShmemSize());
    size = add_size(size, BackgroundWorkerShmemSize());
    size = add_size(size, MultiXactShmemSize());
    size = add_size(size, LWLockShmemSize());
    size = add_size(size, ProcArrayShmemSize());
    size = add_size(size, BackendStatusShmemSize());
    size = add_size(size, SharedInvalShmemSize());
    size = add_size(size, PMSignalShmemSize());
    size = add_size(size, ProcSignalShmemSize());
    size = add_size(size, CheckpointerShmemSize());
    size = add_size(size, AutoVacuumShmemSize());
    size = add_size(size, ReplicationSlotsShmemSize());
    size = add_size(size, ReplicationOriginShmemSize());
    size = add_size(size, WalSndShmemSize());
    size = add_size(size, WalRcvShmemSize());
    size = add_size(size, WalSummarizerShmemSize());
    size = add_size(size, PgArchShmemSize());
    size = add_size(size, ApplyLauncherShmemSize());
    size = add_size(size, BTreeShmemSize());
    size = add_size(size, SyncScanShmemSize());
    size = add_size(size, AsyncShmemSize());
    size = add_size(size, StatsShmemSize());
    size = add_size(size, WaitEventCustomShmemSize());
    size = add_size(size, InjectionPointShmemSize());
    size = add_size(size, SlotSyncShmemSize());
    size = add_size(size, AioShmemSize());

    /* include additional requested shmem from preload libraries */
    size = add_size(size, total_addin_request);

    /* might as well round it off to a multiple of a typical page size */
    size = add_size(size, 8192 - (size % 8192));

    size
}

/*
 * EXEC_BACKEND mode is not defined for this target, so
 * AttachSharedMemoryStructs is omitted (PG guards it with #ifdef
 * EXEC_BACKEND).
 */

/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 */
pub unsafe fn CreateSharedMemoryAndSemaphores() {
    let mut shim: *mut PGShmemHeader = null_mut();
    let seghdr: *mut PGShmemHeader;
    let size: Size;
    let mut numSemas: c_int = 0;

    Assert!(!IsUnderPostmaster);

    /* Compute the size of the shared-memory block */
    size = CalculateShmemSize(&mut numSemas);
    elog!(DEBUG3, "invoking IpcMemoryCreate(size={})", size);

    /*
     * Create the shmem segment
     */
    seghdr = PGSharedMemoryCreate(size, &mut shim);

    /*
     * Make sure that huge pages are never reported as "unknown" while the
     * server is running.
     */
    Assert!(libc_strcmp_unknown(GetConfigOption(
        b"huge_pages_status\0".as_ptr() as *const c_char,
        false,
        false
    )) != 0);

    InitShmemAccess(seghdr as *mut c_void);

    /*
     * Create semaphores.  (This is done here for historical reasons.  We used
     * to support emulating spinlocks with semaphores, which required
     * initializing semaphores early.)
     */
    PGReserveSemaphores(numSemas);

    /*
     * Set up shared memory allocation mechanism
     */
    InitShmemAllocation();

    /* Initialize subsystems */
    CreateOrAttachShmemStructs();

    /* Initialize dynamic shared memory facilities. */
    dsm_postmaster_startup(shim);

    /*
     * Now give loadable modules a chance to set up their shmem allocations
     */
    if let Some(hook) = shmem_startup_hook {
        hook();
    }
}

/*
 * Initialize various subsystems, setting up their data structures in
 * shared memory.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 */
unsafe fn CreateOrAttachShmemStructs() {
    /*
     * Now initialize LWLocks, which do shared memory allocation and are
     * needed for InitShmemIndex.
     */
    CreateLWLocks();

    /*
     * Set up shmem.c index hashtable
     */
    InitShmemIndex();

    dsm_shmem_init();
    DSMRegistryShmemInit();

    /*
     * Set up xlog, clog, and buffers
     */
    VarsupShmemInit();
    XLOGShmemInit();
    XLogPrefetchShmemInit();
    XLogRecoveryShmemInit();
    CLOGShmemInit();
    CommitTsShmemInit();
    SUBTRANSShmemInit();
    MultiXactShmemInit();
    BufferManagerShmemInit();

    /*
     * Set up lock manager
     */
    LockManagerShmemInit();

    /*
     * Set up predicate lock manager
     */
    PredicateLockShmemInit();

    /*
     * Set up process table
     */
    if !IsUnderPostmaster {
        InitProcGlobal();
    }
    ProcArrayShmemInit();
    BackendStatusShmemInit();
    TwoPhaseShmemInit();
    BackgroundWorkerShmemInit();

    /*
     * Set up shared-inval messaging
     */
    SharedInvalShmemInit();

    /*
     * Set up interprocess signaling mechanisms
     */
    PMSignalShmemInit();
    ProcSignalShmemInit();
    CheckpointerShmemInit();
    AutoVacuumShmemInit();
    ReplicationSlotsShmemInit();
    ReplicationOriginShmemInit();
    WalSndShmemInit();
    WalRcvShmemInit();
    WalSummarizerShmemInit();
    PgArchShmemInit();
    ApplyLauncherShmemInit();
    SlotSyncShmemInit();

    /*
     * Set up other modules that need some shared memory space
     */
    BTreeShmemInit();
    SyncScanShmemInit();
    AsyncShmemInit();
    StatsShmemInit();
    WaitEventCustomShmemInit();
    InjectionPointShmemInit();
    AioShmemInit();
}

/*
 * InitializeShmemGUCs
 *
 * This function initializes runtime-computed GUCs related to the amount of
 * shared memory required for the current configuration.
 */
pub unsafe fn InitializeShmemGUCs() {
    let mut buf: [c_char; 64] = [0; 64];
    let size_b: Size;
    let size_mb: Size;
    let mut hp_size: Size = 0;
    let mut num_semas: c_int = 0;

    /*
     * Calculate the shared memory size and round up to the nearest megabyte.
     */
    size_b = CalculateShmemSize(&mut num_semas);
    size_mb = add_size(size_b, (1024 * 1024) - 1) / (1024 * 1024);
    sprintf_size(&mut buf, size_mb);
    SetConfigOption(
        b"shared_memory_size\0".as_ptr() as *const c_char,
        buf.as_ptr(),
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );

    /*
     * Calculate the number of huge pages required.
     */
    GetHugePageSize(&mut hp_size, null_mut());
    if hp_size != 0 {
        let hp_required: Size;

        hp_required = add_size(size_b / hp_size, 1);
        sprintf_size(&mut buf, hp_required);
        SetConfigOption(
            b"shared_memory_size_in_huge_pages\0".as_ptr() as *const c_char,
            buf.as_ptr(),
            PGC_INTERNAL,
            PGC_S_DYNAMIC_DEFAULT,
        );
    }

    sprintf_int(&mut buf, num_semas);
    SetConfigOption(
        b"num_os_semaphores\0".as_ptr() as *const c_char,
        buf.as_ptr(),
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );
}

/*
 * sprintf("%zu") / sprintf("%d") helpers: render an integer into the local
 * char buffer as a NUL-terminated C string (the C code uses sprintf into a
 * fixed buffer).
 */
unsafe fn sprintf_size(buf: &mut [c_char; 64], val: Size) {
    let s = format!("{}\0", val);
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b as c_char;
    }
}

unsafe fn sprintf_int(buf: &mut [c_char; 64], val: c_int) {
    let s = format!("{}\0", val);
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b as c_char;
    }
}

/* strcmp("unknown", x) != 0 -- returns nonzero when x is not "unknown". */
unsafe fn libc_strcmp_unknown(s: *const c_char) -> c_int {
    if s.is_null() {
        return 1;
    }
    let mut i = 0isize;
    let unknown = b"unknown";
    loop {
        let c = *s.offset(i) as u8;
        let u = if (i as usize) < unknown.len() {
            unknown[i as usize]
        } else {
            0
        };
        if c != u {
            return (c as c_int) - (u as c_int);
        }
        if c == 0 {
            return 0;
        }
        i += 1;
    }
}

/* ------------------------------------------------------------------------
 * Local stubs for not-yet-ported callees.
 * Each corresponds to a real PostgreSQL function pulled in by one of the
 * #include'd headers; replace with `use crate::...` once ported.
 * ------------------------------------------------------------------------ */

// access/transam.h ProcGlobalSemas()
unsafe fn ProcGlobalSemas() -> c_int {
    unimplemented!()
} // TODO

// storage/dsm.h dsm_estimate_size(), dsm_postmaster_startup(), dsm_shmem_init()
unsafe fn dsm_estimate_size() -> Size {
    unimplemented!()
} // TODO
unsafe fn dsm_postmaster_startup(_shim: *mut PGShmemHeader) {
    unimplemented!()
} // TODO
unsafe fn dsm_shmem_init() {
    unimplemented!()
} // TODO

// storage/dsm_registry.h DSMRegistryShmemSize(), DSMRegistryShmemInit()
unsafe fn DSMRegistryShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn DSMRegistryShmemInit() {
    unimplemented!()
} // TODO

// storage/bufmgr.h BufferManagerShmemSize(), BufferManagerShmemInit()
unsafe fn BufferManagerShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn BufferManagerShmemInit() {
    unimplemented!()
} // TODO

// storage/lock manager (storage/lock.h) LockManagerShmemSize(), LockManagerShmemInit()
unsafe fn LockManagerShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn LockManagerShmemInit() {
    unimplemented!()
} // TODO

// storage/predicate.h PredicateLockShmemSize(), PredicateLockShmemInit()
unsafe fn PredicateLockShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn PredicateLockShmemInit() {
    unimplemented!()
} // TODO

// storage/proc.h ProcGlobalShmemSize(), InitProcGlobal()
unsafe fn ProcGlobalShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn InitProcGlobal() {
    unimplemented!()
} // TODO

// access/xlogprefetcher.h XLogPrefetchShmemSize(), XLogPrefetchShmemInit()
unsafe fn XLogPrefetchShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn XLogPrefetchShmemInit() {
    unimplemented!()
} // TODO

// access/transam/varsup VarsupShmemSize(), VarsupShmemInit()
unsafe fn VarsupShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn VarsupShmemInit() {
    unimplemented!()
} // TODO

// access/xlog.h XLOGShmemSize(), XLOGShmemInit()
unsafe fn XLOGShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn XLOGShmemInit() {
    unimplemented!()
} // TODO

// access/xlogrecovery.h XLogRecoveryShmemSize(), XLogRecoveryShmemInit()
unsafe fn XLogRecoveryShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn XLogRecoveryShmemInit() {
    unimplemented!()
} // TODO

// access/clog.h CLOGShmemSize(), CLOGShmemInit()
unsafe fn CLOGShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn CLOGShmemInit() {
    unimplemented!()
} // TODO

// access/commit_ts.h CommitTsShmemSize(), CommitTsShmemInit()
unsafe fn CommitTsShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn CommitTsShmemInit() {
    unimplemented!()
} // TODO

// access/subtrans.h SUBTRANSShmemSize(), SUBTRANSShmemInit()
unsafe fn SUBTRANSShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn SUBTRANSShmemInit() {
    unimplemented!()
} // TODO

// access/twophase.h TwoPhaseShmemSize(), TwoPhaseShmemInit()
unsafe fn TwoPhaseShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn TwoPhaseShmemInit() {
    unimplemented!()
} // TODO

// postmaster/bgworker_internals.h BackgroundWorkerShmemSize(), BackgroundWorkerShmemInit()
unsafe fn BackgroundWorkerShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn BackgroundWorkerShmemInit() {
    unimplemented!()
} // TODO

// access/multixact.h MultiXactShmemSize(), MultiXactShmemInit()
unsafe fn MultiXactShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn MultiXactShmemInit() {
    unimplemented!()
} // TODO

// storage/lwlock.h LWLockShmemSize(), CreateLWLocks()
unsafe fn LWLockShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn CreateLWLocks() {
    unimplemented!()
} // TODO

// storage/procarray.h ProcArrayShmemSize(), ProcArrayShmemInit()
unsafe fn ProcArrayShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn ProcArrayShmemInit() {
    unimplemented!()
} // TODO

// pgstat.h / utils/backend_status.h BackendStatusShmemSize(), BackendStatusShmemInit(), StatsShmemSize(), StatsShmemInit()
unsafe fn BackendStatusShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn BackendStatusShmemInit() {
    unimplemented!()
} // TODO
unsafe fn StatsShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn StatsShmemInit() {
    unimplemented!()
} // TODO

// storage/sinvaladt.h SharedInvalShmemSize(), SharedInvalShmemInit()
unsafe fn SharedInvalShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn SharedInvalShmemInit() {
    unimplemented!()
} // TODO

// storage/pmsignal.h PMSignalShmemSize(), PMSignalShmemInit()
unsafe fn PMSignalShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn PMSignalShmemInit() {
    unimplemented!()
} // TODO

// storage/procsignal.h ProcSignalShmemSize(), ProcSignalShmemInit()
unsafe fn ProcSignalShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn ProcSignalShmemInit() {
    unimplemented!()
} // TODO

// postmaster/bgwriter.h CheckpointerShmemSize(), CheckpointerShmemInit()
unsafe fn CheckpointerShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn CheckpointerShmemInit() {
    unimplemented!()
} // TODO

// postmaster/autovacuum.h AutoVacuumShmemSize(), AutoVacuumShmemInit()
unsafe fn AutoVacuumShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn AutoVacuumShmemInit() {
    unimplemented!()
} // TODO

// replication/slot.h ReplicationSlotsShmemSize(), ReplicationSlotsShmemInit()
unsafe fn ReplicationSlotsShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn ReplicationSlotsShmemInit() {
    unimplemented!()
} // TODO

// replication/origin.h ReplicationOriginShmemSize(), ReplicationOriginShmemInit()
unsafe fn ReplicationOriginShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn ReplicationOriginShmemInit() {
    unimplemented!()
} // TODO

// replication/walsender.h WalSndShmemSize(), WalSndShmemInit()
unsafe fn WalSndShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn WalSndShmemInit() {
    unimplemented!()
} // TODO

// replication/walreceiver.h WalRcvShmemSize(), WalRcvShmemInit()
unsafe fn WalRcvShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn WalRcvShmemInit() {
    unimplemented!()
} // TODO

// postmaster/walsummarizer.h WalSummarizerShmemSize(), WalSummarizerShmemInit()
unsafe fn WalSummarizerShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn WalSummarizerShmemInit() {
    unimplemented!()
} // TODO

// postmaster/pgarch.h PgArchShmemSize(), PgArchShmemInit()
unsafe fn PgArchShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn PgArchShmemInit() {
    unimplemented!()
} // TODO

// replication/logicallauncher.h ApplyLauncherShmemSize(), ApplyLauncherShmemInit()
unsafe fn ApplyLauncherShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn ApplyLauncherShmemInit() {
    unimplemented!()
} // TODO

// replication/slotsync.h SlotSyncShmemSize(), SlotSyncShmemInit()
unsafe fn SlotSyncShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn SlotSyncShmemInit() {
    unimplemented!()
} // TODO

// access/nbtree.h BTreeShmemSize(), BTreeShmemInit()
unsafe fn BTreeShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn BTreeShmemInit() {
    unimplemented!()
} // TODO

// access/syncscan.h SyncScanShmemSize(), SyncScanShmemInit()
unsafe fn SyncScanShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn SyncScanShmemInit() {
    unimplemented!()
} // TODO

// commands/async.h AsyncShmemSize(), AsyncShmemInit()
unsafe fn AsyncShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn AsyncShmemInit() {
    unimplemented!()
} // TODO

// utils/wait_event.h WaitEventCustomShmemSize(), WaitEventCustomShmemInit()
unsafe fn WaitEventCustomShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn WaitEventCustomShmemInit() {
    unimplemented!()
} // TODO

// utils/injection_point.h InjectionPointShmemSize(), InjectionPointShmemInit()
unsafe fn InjectionPointShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn InjectionPointShmemInit() {
    unimplemented!()
} // TODO

// storage/aio_subsys.h AioShmemSize(), AioShmemInit()
unsafe fn AioShmemSize() -> Size {
    unimplemented!()
} // TODO
unsafe fn AioShmemInit() {
    unimplemented!()
} // TODO

// storage/shmem.h InitShmemAccess(), InitShmemAllocation(), InitShmemIndex()
unsafe fn InitShmemAccess(_seghdr: *mut c_void) {
    unimplemented!()
} // TODO
unsafe fn InitShmemAllocation() {
    unimplemented!()
} // TODO
unsafe fn InitShmemIndex() {
    unimplemented!()
} // TODO

// utils/guc.h GetConfigOption(), SetConfigOption()
unsafe fn GetConfigOption(
    _name: *const c_char,
    _missing_ok: bool,
    _restrict_privileged: bool,
) -> *const c_char {
    unimplemented!()
} // TODO
unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: c_int,
) {
    unimplemented!()
} // TODO
