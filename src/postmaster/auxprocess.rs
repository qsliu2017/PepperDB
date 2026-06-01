//! postmaster/auxprocess.c - functions related to auxiliary processes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/postmaster/auxprocess.c

use crate::prelude::*;

// ---- Imports of already-ported dependencies ----

// miscadmin.h
use crate::miscadmin::{
    GetProcessingMode, InitProcessing, IgnoreSystemIndexes, IsUnderPostmaster, BaseInit,
    NormalProcessing, SetProcessingMode,
};
// utils/mmgr/mcxt.rs
use crate::utils::mmgr::mcxt::{MemoryContextDelete, PostmasterContext};
// utils/resowner/resowner.rs
use crate::utils::resowner::resowner::CreateAuxProcessResourceOwner;

// ---- Local stubs for not-yet-ported dependencies ----

// utils/ps_status.h
unsafe fn init_ps_display(_fixed_part: *const c_char) {
    unimplemented!() // TODO: dep not ported (utils/ps_status.c)
}

// storage/proc.h
unsafe fn InitAuxiliaryProcess() {
    unimplemented!() // TODO: dep not ported (storage/lmgr/proc.c)
}

// storage/procsignal.h
unsafe fn ProcSignalInit(_cancel_key: *mut u8, _cancel_key_len: c_int) {
    unimplemented!() // TODO: dep not ported (storage/ipc/procsignal.c)
}

// pgstat.h
unsafe fn pgstat_beinit() {
    unimplemented!() // TODO: dep not ported (utils/activity/pgstat.c)
}
unsafe fn pgstat_bestart_initial() {
    unimplemented!() // TODO: dep not ported (utils/activity/backend_status.c)
}
unsafe fn pgstat_bestart_final() {
    unimplemented!() // TODO: dep not ported (utils/activity/backend_status.c)
}
unsafe fn pgstat_report_wait_end() {
    // TODO: dep not ported (utils/activity/wait_event.c / pgstat header inline)
}

// storage/ipc.h: typedef void (*pg_on_exit_callback)(int code, Datum arg);
type pg_on_exit_callback = unsafe fn(code: c_int, arg: Datum);
unsafe fn before_shmem_exit(_function: pg_on_exit_callback, _arg: Datum) {
    unimplemented!() // TODO: dep not ported (storage/ipc/ipc.c)
}

// storage/lwlock.h
unsafe fn LWLockReleaseAll() {
    unimplemented!() // TODO: dep not ported (storage/lmgr/lwlock.c)
}

// storage/condition_variable.h
unsafe fn ConditionVariableCancelSleep() -> bool {
    unimplemented!() // TODO: dep not ported (storage/lmgr/condition_variable.c)
}

/// AuxiliaryProcessMainCommon
///
/// Common initialization code for auxiliary processes, such as the bgwriter,
/// walwriter, walreceiver, and the startup process.
pub unsafe fn AuxiliaryProcessMainCommon() {
    Assert!(IsUnderPostmaster);

    /* Release postmaster's working memory context */
    if !PostmasterContext.is_null() {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = core::ptr::null_mut();
    }

    init_ps_display(core::ptr::null());

    Assert!(GetProcessingMode() == InitProcessing);

    IgnoreSystemIndexes = true;

    /*
     * As an auxiliary process, we aren't going to do the full InitPostgres
     * pushups, but there are a couple of things that need to get lit up even
     * in an auxiliary process.
     */

    /*
     * Create a PGPROC so we can use LWLocks and access shared memory.
     */
    InitAuxiliaryProcess();

    BaseInit();

    ProcSignalInit(core::ptr::null_mut(), 0);

    /*
     * Auxiliary processes don't run transactions, but they may need a
     * resource owner anyway to manage buffer pins acquired outside
     * transactions (and, perhaps, other things in future).
     */
    CreateAuxProcessResourceOwner();

    /* Initialize backend status information */
    pgstat_beinit();
    pgstat_bestart_initial();
    pgstat_bestart_final();

    /* register a before-shutdown callback for LWLock cleanup */
    before_shmem_exit(ShutdownAuxiliaryProcess, 0 as Datum);

    SetProcessingMode(NormalProcessing);
}

/// Begin shutdown of an auxiliary process.  This is approximately the equivalent
/// of ShutdownPostgres() in postinit.c.  We can't run transactions in an
/// auxiliary process, so most of the work of AbortTransaction() is not needed,
/// but we do need to make sure we've released any LWLocks we are holding.
/// (This is only critical during an error exit.)
unsafe fn ShutdownAuxiliaryProcess(_code: c_int, _arg: Datum) {
    LWLockReleaseAll();
    ConditionVariableCancelSleep();
    pgstat_report_wait_end();
}
