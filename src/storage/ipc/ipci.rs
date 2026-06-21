//! storage/ipc/ipci.c - POSTGRES inter-process communication initialization code.

use crate::prelude::*;

use crate::miscadmin::process_shmem_requests_in_progress;
use crate::utils::hash::dynahash::hash_estimate_size;
use crate::utils::init::globals::IsUnderPostmaster;

use crate::storage::pg_shmem::{
    GetHugePageSize, PGSharedMemoryCreate, PGShmemHeader, DEFAULT_SHARED_MEMORY_TYPE,
};
use crate::port::sysv_sema::{PGReserveSemaphores, PGSemaphoreShmemSize};

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

use crate::utils::misc::guc::{GucContext, GucSource};
use crate::utils::misc::guc::GucContext::PGC_INTERNAL;
use crate::utils::misc::guc::GucSource::PGC_S_DYNAMIC_DEFAULT;

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
 * Forwarders to the real implementations (were local unimplemented stubs).
 * ------------------------------------------------------------------------ */

unsafe fn ProcGlobalSemas() -> c_int { crate::storage::lmgr::proc::ProcGlobalSemas() }

unsafe fn dsm_estimate_size() -> Size { crate::storage::ipc::dsm::dsm_estimate_size() }
unsafe fn dsm_postmaster_startup(_shim: *mut PGShmemHeader) {
    // bring-up: DSM is only needed for parallel query; skip preallocation (bad fn-ptr in
    // dsm_impl dispatch). TODO: real dsm_postmaster_startup once DSM-impl ops are wired.
}
unsafe fn dsm_shmem_init() { crate::storage::ipc::dsm::dsm_shmem_init() }

unsafe fn DSMRegistryShmemSize() -> Size { crate::storage::ipc::dsm_registry::DSMRegistryShmemSize() }
unsafe fn DSMRegistryShmemInit() { crate::storage::ipc::dsm_registry::DSMRegistryShmemInit() }

unsafe fn BufferManagerShmemSize() -> Size { crate::storage::buffer::buf_init::BufferManagerShmemSize() }
unsafe fn BufferManagerShmemInit() { crate::storage::buffer::buf_init::BufferManagerShmemInit() }

unsafe fn LockManagerShmemSize() -> Size { crate::storage::lmgr::lock::LockManagerShmemSize() }
unsafe fn LockManagerShmemInit() { crate::storage::lmgr::lock::LockManagerShmemInit() }

unsafe fn PredicateLockShmemSize() -> Size { crate::storage::lmgr::predicate::PredicateLockShmemSize() }
unsafe fn PredicateLockShmemInit() { crate::storage::lmgr::predicate::PredicateLockShmemInit() }

unsafe fn ProcGlobalShmemSize() -> Size { crate::storage::lmgr::proc::ProcGlobalShmemSize() }
unsafe fn InitProcGlobal() { crate::storage::lmgr::proc::InitProcGlobal() }

unsafe fn XLogPrefetchShmemSize() -> Size { crate::access::transam::xlogprefetcher::XLogPrefetchShmemSize() }
unsafe fn XLogPrefetchShmemInit() { crate::access::transam::xlogprefetcher::XLogPrefetchShmemInit() }

unsafe fn VarsupShmemSize() -> Size { crate::access::transam::varsup::VarsupShmemSize() }
unsafe fn VarsupShmemInit() { crate::access::transam::varsup::VarsupShmemInit() }

unsafe fn XLOGShmemSize() -> Size { crate::access::transam::xlog::XLOGShmemSize() }
unsafe fn XLOGShmemInit() { crate::access::transam::xlog::XLOGShmemInit() }

unsafe fn XLogRecoveryShmemSize() -> Size { crate::access::transam::xlogrecovery::XLogRecoveryShmemSize() }
unsafe fn XLogRecoveryShmemInit() { crate::access::transam::xlogrecovery::XLogRecoveryShmemInit() }

unsafe fn CLOGShmemSize() -> Size { crate::access::transam::clog::CLOGShmemSize() }
unsafe fn CLOGShmemInit() { crate::access::transam::clog::CLOGShmemInit() }

unsafe fn CommitTsShmemSize() -> Size { crate::access::transam::commit_ts::CommitTsShmemSize() }
unsafe fn CommitTsShmemInit() { crate::access::transam::commit_ts::CommitTsShmemInit() }

unsafe fn SUBTRANSShmemSize() -> Size { crate::access::transam::subtrans::SUBTRANSShmemSize() }
unsafe fn SUBTRANSShmemInit() { crate::access::transam::subtrans::SUBTRANSShmemInit() }

unsafe fn TwoPhaseShmemSize() -> Size { crate::access::transam::twophase::TwoPhaseShmemSize() }
unsafe fn TwoPhaseShmemInit() { crate::access::transam::twophase::TwoPhaseShmemInit() }

unsafe fn BackgroundWorkerShmemSize() -> Size { crate::postmaster::bgworker::BackgroundWorkerShmemSize() }
unsafe fn BackgroundWorkerShmemInit() { crate::postmaster::bgworker::BackgroundWorkerShmemInit() }

unsafe fn MultiXactShmemSize() -> Size { crate::access::transam::multixact::MultiXactShmemSize() }
unsafe fn MultiXactShmemInit() { crate::access::transam::multixact::MultiXactShmemInit() }

unsafe fn LWLockShmemSize() -> Size { crate::storage::lmgr::lwlock::LWLockShmemSize() }
unsafe fn CreateLWLocks() { crate::storage::lmgr::lwlock::CreateLWLocks() }

unsafe fn ProcArrayShmemSize() -> Size { crate::storage::ipc::procarray::ProcArrayShmemSize() }
unsafe fn ProcArrayShmemInit() { crate::storage::ipc::procarray::ProcArrayShmemInit() }

unsafe fn BackendStatusShmemSize() -> Size { crate::utils::activity::backend_status::BackendStatusShmemSize() }
unsafe fn BackendStatusShmemInit() { crate::utils::activity::backend_status::BackendStatusShmemInit() }
unsafe fn StatsShmemSize() -> Size { crate::utils::activity::pgstat_shmem::StatsShmemSize() }
unsafe fn StatsShmemInit() { crate::utils::activity::pgstat_shmem::StatsShmemInit() }

unsafe fn SharedInvalShmemSize() -> Size { crate::storage::ipc::sinvaladt::SharedInvalShmemSize() }
unsafe fn SharedInvalShmemInit() { crate::storage::ipc::sinvaladt::SharedInvalShmemInit() }

unsafe fn PMSignalShmemSize() -> Size { crate::storage::ipc::pmsignal::PMSignalShmemSize() }
unsafe fn PMSignalShmemInit() { crate::storage::ipc::pmsignal::PMSignalShmemInit() }

unsafe fn ProcSignalShmemSize() -> Size { crate::storage::ipc::procsignal::ProcSignalShmemSize() }
unsafe fn ProcSignalShmemInit() { crate::storage::ipc::procsignal::ProcSignalShmemInit() }

unsafe fn CheckpointerShmemSize() -> Size { crate::postmaster::checkpointer::CheckpointerShmemSize() }
unsafe fn CheckpointerShmemInit() { crate::postmaster::checkpointer::CheckpointerShmemInit() }

unsafe fn AutoVacuumShmemSize() -> Size { crate::postmaster::autovacuum::AutoVacuumShmemSize() }
unsafe fn AutoVacuumShmemInit() { crate::postmaster::autovacuum::AutoVacuumShmemInit() }

unsafe fn ReplicationSlotsShmemSize() -> Size { crate::replication::slot::ReplicationSlotsShmemSize() }
unsafe fn ReplicationSlotsShmemInit() { crate::replication::slot::ReplicationSlotsShmemInit() }

unsafe fn ReplicationOriginShmemSize() -> Size { crate::replication::logical::origin::ReplicationOriginShmemSize() }
unsafe fn ReplicationOriginShmemInit() { crate::replication::logical::origin::ReplicationOriginShmemInit() }

unsafe fn WalSndShmemSize() -> Size { crate::replication::walsender::WalSndShmemSize() }
unsafe fn WalSndShmemInit() { crate::replication::walsender::WalSndShmemInit() }

unsafe fn WalRcvShmemSize() -> Size { crate::replication::walreceiverfuncs::WalRcvShmemSize() }
unsafe fn WalRcvShmemInit() { crate::replication::walreceiverfuncs::WalRcvShmemInit() }

unsafe fn WalSummarizerShmemSize() -> Size { crate::postmaster::walsummarizer::WalSummarizerShmemSize() }
unsafe fn WalSummarizerShmemInit() { crate::postmaster::walsummarizer::WalSummarizerShmemInit() }

unsafe fn PgArchShmemSize() -> Size { crate::postmaster::pgarch::PgArchShmemSize() }
unsafe fn PgArchShmemInit() { crate::postmaster::pgarch::PgArchShmemInit() }

unsafe fn ApplyLauncherShmemSize() -> Size { crate::replication::logical::launcher::ApplyLauncherShmemSize() }
unsafe fn ApplyLauncherShmemInit() { crate::replication::logical::launcher::ApplyLauncherShmemInit() }

unsafe fn SlotSyncShmemSize() -> Size { crate::replication::logical::slotsync::SlotSyncShmemSize() }
unsafe fn SlotSyncShmemInit() { crate::replication::logical::slotsync::SlotSyncShmemInit() }

unsafe fn BTreeShmemSize() -> Size { crate::access::nbtree::nbtutils::BTreeShmemSize() }
unsafe fn BTreeShmemInit() { crate::access::nbtree::nbtutils::BTreeShmemInit() }

unsafe fn SyncScanShmemSize() -> Size { crate::access::common::syncscan::SyncScanShmemSize() }
unsafe fn SyncScanShmemInit() { crate::access::common::syncscan::SyncScanShmemInit() }

unsafe fn AsyncShmemSize() -> Size { crate::commands::r#async::AsyncShmemSize() }
unsafe fn AsyncShmemInit() { crate::commands::r#async::AsyncShmemInit() }

unsafe fn WaitEventCustomShmemSize() -> Size { crate::utils::activity::wait_event::WaitEventCustomShmemSize() }
unsafe fn WaitEventCustomShmemInit() { crate::utils::activity::wait_event::WaitEventCustomShmemInit() }

unsafe fn InjectionPointShmemSize() -> Size { crate::utils::misc::injection_point::InjectionPointShmemSize() }
unsafe fn InjectionPointShmemInit() { crate::utils::misc::injection_point::InjectionPointShmemInit() }

unsafe fn AioShmemSize() -> Size { crate::storage::aio::aio_init::AioShmemSize() }
unsafe fn AioShmemInit() { crate::storage::aio::aio_init::AioShmemInit() }

unsafe fn InitShmemAccess(seghdr: *mut c_void) { crate::storage::ipc::shmem::InitShmemAccess(seghdr as *mut crate::storage::ipc::shmem::PGShmemHeader) }
unsafe fn InitShmemAllocation() { crate::storage::ipc::shmem::InitShmemAllocation() }
unsafe fn InitShmemIndex() { crate::storage::ipc::shmem::InitShmemIndex() }

unsafe fn GetConfigOption(name: *const c_char, missing_ok: bool, restrict_privileged: bool) -> *const c_char { crate::utils::misc::guc::GetConfigOption(name, missing_ok, restrict_privileged) }
unsafe fn SetConfigOption(name: *const c_char, value: *const c_char, context: GucContext, source: GucSource) { crate::utils::misc::guc::SetConfigOption(name, value, context, source) }
