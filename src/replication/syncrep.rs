/*-------------------------------------------------------------------------
 *
 * syncrep.rs
 *
 * Synchronous replication is new as of PostgreSQL 9.1.
 *
 * If requested, transaction commits wait until their commit LSN are
 * acknowledged by the synchronous standbys.
 *
 * This module contains the code for waiting and release of backends.
 * All code in this module executes on the primary. The core streaming
 * replication transport remains within WALreceiver/WALsender modules.
 *
 * The essence of this design is that it isolates all logic about
 * waiting/releasing onto the primary. The primary defines which standbys
 * it wishes to wait for. The standbys are completely unaware of the
 * durability requirements of transactions on the primary, reducing the
 * complexity of the code and streamlining both standby operations and
 * network bandwidth because there is no requirement to ship
 * per-transaction state information.
 *
 * Replication is either synchronous or not synchronous (async). If it is
 * async, we just fastpath out of here. If it is sync, then we wait for
 * the write, flush or apply location on the standby before releasing
 * the waiting backend. Further complexity in that interaction is
 * expected in later releases.
 *
 * The best performing way to manage the waiting backends is to have a
 * single ordered queue of waiting backends, so that we can avoid
 * searching the through all waiters each time we receive a reply.
 *
 * In 9.5 or before only a single standby could be considered as
 * synchronous. In 9.6 we support a priority-based multiple synchronous
 * standbys. In 10.0 a quorum-based multiple synchronous standbys is also
 * supported. The number of synchronous standbys that transactions
 * must wait for replies from is specified in synchronous_standby_names.
 * This parameter also specifies a list of standby names and the method
 * (FIRST and ANY) to choose synchronous standbys from the listed ones.
 *
 * The method FIRST specifies a priority-based synchronous replication
 * and makes transaction commits wait until their WAL records are
 * replicated to the requested number of synchronous standbys chosen based
 * on their priorities. The standbys whose names appear earlier in the list
 * are given higher priority and will be considered as synchronous.
 * Other standby servers appearing later in this list represent potential
 * synchronous standbys. If any of the current synchronous standbys
 * disconnects for whatever reason, it will be replaced immediately with
 * the next-highest-priority standby.
 *
 * The method ANY specifies a quorum-based synchronous replication
 * and makes transaction commits wait until their WAL records are
 * replicated to at least the requested number of synchronous standbys
 * in the list. All the standbys appearing in the list are considered as
 * candidates for quorum synchronous standbys.
 *
 * If neither FIRST nor ANY is specified, FIRST is used as the method.
 * This is for backward compatibility with 9.6 or before where only a
 * priority-based sync replication was supported.
 *
 * Before the standbys chosen from synchronous_standby_names can
 * become the synchronous standbys they must have caught up with
 * the primary; that may take some time. Once caught up,
 * the standbys which are considered as synchronous at that moment
 * will release waiters from the queue.
 *
 * Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/replication/syncrep.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify, dlist_reverse_foreach};

use crate::access::transam::xlogdefs::{XLogRecPtr, InvalidXLogRecPtr, XLogRecPtrIsInvalid,
                                        LSN_FORMAT_ARGS};
use crate::c::{uint8, Size};
use crate::lib::ilist::{
    dlist_head, dlist_iter, dlist_mutable_iter, dlist_node, dlist_node_is_detached,
    dlist_push_head, dlist_insert_after, dlist_delete_thoroughly,
};
use crate::replication::walsender_private::{
    WalSnd, WalSndCtl, WalSndCtlData, WalSndState,
    WALSNDSTATE_STREAMING, WALSNDSTATE_STOPPING,
    SYNC_STANDBY_INIT, SYNC_STANDBY_DEFINED,
    MyWalSnd, NUM_SYNC_REP_WAIT_MODE,
};
use crate::storage::spin::{SpinLockAcquire, SpinLockRelease};
use crate::storage::ipc::latch::{
    Latch, WaitLatch, ResetLatch, SetLatch,
    WL_LATCH_SET, WL_POSTMASTER_DEATH,
};
use crate::utils::guc_hooks::GucSource;
use crate::utils::misc::ps_status::{
    update_process_title, set_ps_display_suffix, set_ps_display_remove_suffix,
};
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::common::int::pg_cmp_u64;

// ---------------------------------------------------------------------------
// Symbols whose home modules are not yet ported - local stubs
// ---------------------------------------------------------------------------

/// STUB: `PGPROC` from storage/proc.h.
/// TODO(pg-port): real PGPROC lives in storage/proc.h
#[repr(C)]
pub struct PGPROC {
    /// LSN this backend is waiting for
    pub waitLSN: XLogRecPtr,
    /// waiting state (SYNC_REP_NOT_WAITING / WAITING / WAIT_COMPLETE)
    pub syncRepState: c_int,
    /// queue link for sync rep queue
    pub syncRepLinks: dlist_node,
    /// process latch (wake target)
    pub procLatch: Latch,
}

extern "C" {
    /// STUB: MyProc - this backend's PGPROC entry.
    /// TODO(pg-port): real MyProc lives in storage/proc.h / utils/init/globals.c
    pub static mut MyProc: *mut PGPROC;
}

/// STUB: LWLock.
/// TODO(pg-port): real LWLock lives in storage/lwlock.h
#[repr(C)]
pub struct LWLock {
    _opaque: [u8; 0],
}

pub const LW_EXCLUSIVE: c_int = 0; // TODO(pg-port): storage/lwlock.h
pub const LW_SHARED: c_int = 1;    // TODO(pg-port): storage/lwlock.h

extern "C" {
    /// STUB: SyncRepLock from storage/lwlock.h / storage/lwlocklist.h.
    /// TODO(pg-port): real SyncRepLock lives in storage/lwlock.h
    pub static mut SyncRepLock: *mut LWLock;
}

/// STUB: LWLockAcquire.
/// TODO(pg-port): real LWLockAcquire lives in storage/lwlock.h
#[inline]
unsafe fn LWLockAcquire(lock: *mut LWLock, mode: c_int) -> bool {
    unimplemented!("LWLockAcquire: storage/lwlock.h")
}

/// STUB: LWLockRelease.
/// TODO(pg-port): real LWLockRelease lives in storage/lwlock.h
#[inline]
unsafe fn LWLockRelease(lock: *mut LWLock) {
    unimplemented!("LWLockRelease: storage/lwlock.h")
}

/// STUB: LWLockHeldByMeInMode.
/// TODO(pg-port): real LWLockHeldByMeInMode lives in storage/lwlock.h
#[inline]
#[cfg(debug_assertions)]
unsafe fn LWLockHeldByMeInMode(lock: *mut LWLock, mode: c_int) -> bool {
    true // stub - always true in release
}

// STUB: InterruptHoldoffCount from miscadmin.h.
// TODO(pg-port): real InterruptHoldoffCount lives in tcop/tcopprot.h / miscadmin.h
extern "C" {
    pub static mut InterruptHoldoffCount: u32;
    /// STUB: ProcDiePending.
    /// TODO(pg-port): real ProcDiePending lives in tcop/tcopprot.h / miscadmin.h
    pub static mut ProcDiePending: i32;
    /// STUB: QueryCancelPending.
    /// TODO(pg-port): real QueryCancelPending lives in tcop/tcopprot.h / miscadmin.h
    pub static mut QueryCancelPending: i32;
    /// STUB: whereToSendOutput.
    /// TODO(pg-port): real whereToSendOutput lives in tcop/tcopprot.h
    pub static mut whereToSendOutput: c_int;
    /// STUB: application_name GUC.
    /// TODO(pg-port): real application_name lives in utils/guc_tables.c
    pub static mut application_name: *mut c_char;
    /// STUB: max_wal_senders GUC.
    /// TODO(pg-port): real max_wal_senders lives in replication/walsender.c
    pub static mut max_wal_senders: c_int;
    /// STUB: synchronous_commit GUC (int-valued enum).
    /// TODO(pg-port): real synchronous_commit lives in access/xact.c
    pub static mut synchronous_commit: c_int;
    /// STUB: am_cascading_walsender.
    /// TODO(pg-port): real am_cascading_walsender lives in replication/walsender.c
    pub static mut am_cascading_walsender: bool;
    /// STUB: MyLatch.
    /// TODO(pg-port): real MyLatch lives in utils/init/globals.c
    pub static mut MyLatch: *mut Latch;
}

/// STUB: DestNone - whereToSendOutput value.
/// TODO(pg-port): real DestNone lives in tcop/dest.h
pub const DestNone: c_int = 0;

/// STUB: WAIT_EVENT_SYNC_REP.
/// TODO(pg-port): real WAIT_EVENT_SYNC_REP lives in utils/activity/wait_event_names.h
pub const WAIT_EVENT_SYNC_REP: u32 = 0;

/// STUB: ERRCODE_ADMIN_SHUTDOWN.
/// TODO(pg-port): real value lives in utils/errcodes.h
pub const ERRCODE_ADMIN_SHUTDOWN: c_int = 0;

/// STUB: ERRCODE_SYNTAX_ERROR.
/// TODO(pg-port): real value lives in utils/errcodes.h
pub const ERRCODE_SYNTAX_ERROR: c_int = 0;

/// STUB: pg_read_barrier - acquire fence.
/// TODO(pg-port): real pg_read_barrier lives in port/atomics.h
#[inline]
unsafe fn pg_read_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Acquire);
}

/// STUB: pg_write_barrier - release fence.
/// TODO(pg-port): real pg_write_barrier lives in port/atomics.h
#[inline]
unsafe fn pg_write_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Release);
}

/// STUB: guc_malloc - GUC-context allocator.
/// TODO(pg-port): real guc_malloc lives in utils/guc.c
#[inline]
unsafe fn guc_malloc(elevel: c_int, size: Size) -> *mut c_void {
    unimplemented!("guc_malloc: utils/guc.c")
}

/// STUB: GUC_check_errcode macro shim.
/// TODO(pg-port): real GUC_check_errcode lives in utils/guc.h
macro_rules! GUC_check_errcode {
    ($code:expr) => {
        let _ = $code;
    };
}

/// STUB: GUC_check_errdetail macro shim.
/// TODO(pg-port): real GUC_check_errdetail lives in utils/guc.h
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {
        eprintln!("GUC check errdetail: {}", format!($($arg)*));
    };
}

/// STUB: GUC_check_errmsg macro shim.
/// TODO(pg-port): real GUC_check_errmsg lives in utils/guc.h
macro_rules! GUC_check_errmsg {
    ($($arg:tt)*) => {
        eprintln!("GUC check errmsg: {}", format!($($arg)*));
    };
}

/// STUB: syncrep_scanner_init - lexer initialisation generated from syncrep_scanner.l.
/// TODO(pg-port): real syncrep_scanner_init lives in replication/syncrep_scanner.l
#[inline]
pub unsafe fn syncrep_scanner_init(str: *const c_char, yyscannerp: *mut *mut c_void) {
    unimplemented!("syncrep_scanner_init: replication/syncrep_scanner.l")
}

/// STUB: syncrep_scanner_finish - lexer teardown generated from syncrep_scanner.l.
/// TODO(pg-port): real syncrep_scanner_finish lives in replication/syncrep_scanner.l
#[inline]
pub unsafe fn syncrep_scanner_finish(yyscanner: *mut c_void) {
    unimplemented!("syncrep_scanner_finish: replication/syncrep_scanner.l")
}

/// STUB: syncrep_yyparse - parser generated from syncrep_gram.y.
/// TODO(pg-port): real syncrep_yyparse lives in replication/syncrep_gram.y
#[inline]
pub unsafe fn syncrep_yyparse(
    syncrep_parse_result_p: *mut *mut SyncRepConfigData,
    syncrep_parse_error_msg_p: *mut *mut c_char,
    yyscanner: *mut c_void,
) -> c_int {
    unimplemented!("syncrep_yyparse: replication/syncrep_gram.y")
}

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// Synchronous-commit level constants (access/xact.h)
// ---------------------------------------------------------------------------

/// STUB: SYNCHRONOUS_COMMIT_LOCAL_FLUSH.
/// TODO(pg-port): real enum SyncCommitLevel lives in access/xact.h
pub const SYNCHRONOUS_COMMIT_LOCAL_FLUSH: c_int = 2;
/// STUB: SYNCHRONOUS_COMMIT_REMOTE_WRITE.
/// TODO(pg-port): real enum SyncCommitLevel lives in access/xact.h
pub const SYNCHRONOUS_COMMIT_REMOTE_WRITE: c_int = 3;
/// STUB: SYNCHRONOUS_COMMIT_REMOTE_FLUSH.
/// TODO(pg-port): real enum SyncCommitLevel lives in access/xact.h
pub const SYNCHRONOUS_COMMIT_REMOTE_FLUSH: c_int = 4;
/// STUB: SYNCHRONOUS_COMMIT_REMOTE_APPLY.
/// TODO(pg-port): real enum SyncCommitLevel lives in access/xact.h
pub const SYNCHRONOUS_COMMIT_REMOTE_APPLY: c_int = 5;

// ---------------------------------------------------------------------------
// Public constants merged from syncrep.h
// ---------------------------------------------------------------------------

/* SyncRepWaitMode */
pub const SYNC_REP_NO_WAIT: c_int = -1;
pub const SYNC_REP_WAIT_WRITE: c_int = 0;
pub const SYNC_REP_WAIT_FLUSH: c_int = 1;
pub const SYNC_REP_WAIT_APPLY: c_int = 2;

// NUM_SYNC_REP_WAIT_MODE is re-exported from walsender_private to avoid duplication.
// The canonical value 3 matches WAIT_WRITE/FLUSH/APPLY above.

/* syncRepState */
pub const SYNC_REP_NOT_WAITING: c_int = 0;
pub const SYNC_REP_WAITING: c_int = 1;
pub const SYNC_REP_WAIT_COMPLETE: c_int = 2;

/* syncrep_method of SyncRepConfigData */
pub const SYNC_REP_PRIORITY: u8 = 0;
pub const SYNC_REP_QUORUM: u8 = 1;

// ---------------------------------------------------------------------------
// Public struct declarations merged from syncrep.h
// ---------------------------------------------------------------------------

/// SyncRepGetCandidateStandbys returns an array of these structs,
/// one per candidate synchronous walsender.
#[repr(C)]
pub struct SyncRepStandbyData {
    /* Copies of relevant fields from WalSnd shared-memory struct */
    pub pid: c_int,   // pid_t
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,
    pub sync_standby_priority: c_int,
    /* Index of this walsender in the WalSnd shared-memory array */
    pub walsnd_index: c_int,
    /* This flag indicates whether this struct is about our own process */
    pub is_me: bool,
}

/// Struct for the configuration of synchronous replication.
///
/// Note: this must be a flat representation that can be held in a single
/// chunk of malloc'd memory, so that it can be stored as the "extra" data
/// for the synchronous_standby_names GUC.
#[repr(C)]
pub struct SyncRepConfigData {
    /// total size of this struct, in bytes
    pub config_size: c_int,
    /// number of sync standbys that we need to wait for
    pub num_sync: c_int,
    /// method to choose sync standbys
    pub syncrep_method: uint8,
    /// number of members in the following list
    pub nmembers: c_int,
    /// member_names contains nmembers consecutive nul-terminated C strings
    /// (flexible array member - zero-sized in Rust, accessed via raw pointer)
    pub member_names: [c_char; 0],
}

// ---------------------------------------------------------------------------
// Module-level globals
// ---------------------------------------------------------------------------

/// User-settable parameter for sync rep (synchronous_standby_names GUC value).
pub static mut SyncRepStandbyNames: *mut c_char = core::ptr::null_mut();

/// Whether to announce the next takeover as sync standby.
static mut announce_next_takeover: bool = true;

/// Parsed configuration from synchronous_standby_names.
pub static mut SyncRepConfig: *mut SyncRepConfigData = core::ptr::null_mut();

/// Current wait mode derived from synchronous_commit GUC.
static mut SyncRepWaitMode: c_int = SYNC_REP_NO_WAIT;

// ---------------------------------------------------------------------------
// Helper macros / inlines
// ---------------------------------------------------------------------------

/// SyncStandbysDefined() - true when SyncRepStandbyNames is non-empty.
#[inline]
unsafe fn SyncStandbysDefined() -> bool {
    !SyncRepStandbyNames.is_null() && *SyncRepStandbyNames != 0
}

/// SyncRepRequested() - true when syncrep could be active.
/// Merged from syncrep.h macro.
#[inline]
pub unsafe fn SyncRepRequested() -> bool {
    max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH
}

// ===========================================================
// Synchronous Replication functions for normal user backends
// ===========================================================

/// Wait for synchronous replication, if requested by user.
///
/// Initially backends start in state SYNC_REP_NOT_WAITING and then
/// change that state to SYNC_REP_WAITING before adding ourselves
/// to the wait queue. During SyncRepWakeQueue() a WALSender changes
/// the state to SYNC_REP_WAIT_COMPLETE once replication is confirmed.
/// This backend then resets its state to SYNC_REP_NOT_WAITING.
///
/// 'lsn' represents the LSN to wait for.  'commit' indicates whether this LSN
/// represents a commit record.  If it doesn't, then we wait only for the WAL
/// to be flushed if synchronous_commit is set to the higher level of
/// remote_apply, because only commit records provide apply feedback.
pub unsafe fn SyncRepWaitForLSN(lsn: XLogRecPtr, commit: bool) {
    let mode: c_int;

    /*
     * This should be called while holding interrupts during a transaction
     * commit to prevent the follow-up shared memory queue cleanups to be
     * influenced by external interruptions.
     */
    Assert!(InterruptHoldoffCount > 0);

    /*
     * Fast exit if user has not requested sync replication, or there are no
     * sync replication standby names defined.
     *
     * Since this routine gets called every commit time, it's important to
     * exit quickly if sync replication is not requested.
     *
     * We check WalSndCtl->sync_standbys_status flag without the lock and exit
     * immediately if SYNC_STANDBY_INIT is set (the checkpointer has
     * initialized this data) but SYNC_STANDBY_DEFINED is missing (no sync
     * replication requested).
     *
     * If SYNC_STANDBY_DEFINED is set, we need to check the status again later
     * while holding the lock, to check the flag and operate the sync rep
     * queue atomically.  This is necessary to avoid the race condition
     * described in SyncRepUpdateSyncStandbysDefined().  On the other hand, if
     * SYNC_STANDBY_DEFINED is not set, the lock is not necessary because we
     * don't touch the queue.
     */
    let walsndctl_volatile = WalSndCtl as *const WalSndCtlData;
    if !SyncRepRequested()
        || (((*walsndctl_volatile).sync_standbys_status) & (SYNC_STANDBY_INIT | SYNC_STANDBY_DEFINED))
            == SYNC_STANDBY_INIT
    {
        return;
    }

    /* Cap the level for anything other than commit to remote flush only. */
    if commit {
        mode = SyncRepWaitMode;
    } else {
        mode = core::cmp::min(SyncRepWaitMode, SYNC_REP_WAIT_FLUSH);
    }

    Assert!(dlist_node_is_detached(&(*MyProc).syncRepLinks));
    Assert!(!WalSndCtl.is_null());

    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    Assert!((*MyProc).syncRepState == SYNC_REP_NOT_WAITING);

    /*
     * We don't wait for sync rep if SYNC_STANDBY_DEFINED is not set.  See
     * SyncRepUpdateSyncStandbysDefined().
     *
     * Also check that the standby hasn't already replied. Unlikely race
     * condition but we'll be fetching that cache line anyway so it's likely
     * to be a low cost check.
     *
     * If the sync standby data has not been initialized yet
     * (SYNC_STANDBY_INIT is not set), fall back to a check based on the LSN,
     * then do a direct GUC check.
     */
    if (*WalSndCtl).sync_standbys_status & SYNC_STANDBY_INIT != 0 {
        if ((*WalSndCtl).sync_standbys_status & SYNC_STANDBY_DEFINED) == 0
            || lsn <= (*WalSndCtl).lsn[mode as usize]
        {
            LWLockRelease(SyncRepLock);
            return;
        }
    } else if lsn <= (*WalSndCtl).lsn[mode as usize] {
        /*
         * The LSN is older than what we need to wait for.  The sync standby
         * data has not been initialized yet, but we are OK to not wait
         * because we know that there is no point in doing so based on the
         * LSN.
         */
        LWLockRelease(SyncRepLock);
        return;
    } else if !SyncStandbysDefined() {
        /*
         * If we are here, the sync standby data has not been initialized yet,
         * and the LSN is newer than what need to wait for, so we have fallen
         * back to the best thing we could do in this case: a check on
         * SyncStandbysDefined() to see if the GUC is set or not.
         *
         * When the GUC has a value, we wait until the checkpointer updates
         * the status data because we cannot be sure yet if we should wait or
         * not. Here, the GUC has *no* value, we are sure that there is no
         * point to wait; this matters for example when initializing a
         * cluster, where we should never wait, and no sync standbys is the
         * default behavior.
         */
        LWLockRelease(SyncRepLock);
        return;
    }

    /*
     * Set our waitLSN so WALSender will know when to wake us, and add
     * ourselves to the queue.
     */
    (*MyProc).waitLSN = lsn;
    (*MyProc).syncRepState = SYNC_REP_WAITING;
    SyncRepQueueInsert(mode);
    Assert!(SyncRepQueueIsOrderedByLSN(mode));
    LWLockRelease(SyncRepLock);

    /* Alter ps display to show waiting for sync rep. */
    if update_process_title {
        let (hi, lo) = LSN_FORMAT_ARGS(lsn);
        let msg = format!("waiting for {}/{}\0", hi, lo);
        set_ps_display_suffix(msg.as_ptr() as *const c_char);
    }

    /*
     * Wait for specified LSN to be confirmed.
     *
     * Each proc has its own wait latch, so we perform a normal latch
     * check/wait loop here.
     */
    loop {
        let rc: c_int;

        /* Must reset the latch before testing state. */
        ResetLatch(MyLatch);

        /*
         * Acquiring the lock is not needed, the latch ensures proper
         * barriers. If it looks like we're done, we must really be done,
         * because once walsender changes the state to SYNC_REP_WAIT_COMPLETE,
         * it will never update it again, so we can't be seeing a stale value
         * in that case.
         */
        if (*MyProc).syncRepState == SYNC_REP_WAIT_COMPLETE {
            break;
        }

        /*
         * If a wait for synchronous replication is pending, we can neither
         * acknowledge the commit nor raise ERROR or FATAL.  The latter would
         * lead the client to believe that the transaction aborted, which is
         * not true: it's already committed locally. The former is no good
         * either: the client has requested synchronous replication, and is
         * entitled to assume that an acknowledged commit is also replicated,
         * which might not be true. So in this case we issue a WARNING (which
         * some clients may be able to interpret) and shut off further output.
         * We do NOT reset ProcDiePending, so that the process will die after
         * the commit is cleaned up.
         */
        if ProcDiePending != 0 {
            ereport!(WARNING,
                errmsg!("canceling the wait for synchronous replication and terminating connection due to administrator command"));
            whereToSendOutput = DestNone;
            SyncRepCancelWait();
            break;
        }

        /*
         * It's unclear what to do if a query cancel interrupt arrives.  We
         * can't actually abort at this point, but ignoring the interrupt
         * altogether is not helpful, so we just terminate the wait with a
         * suitable warning.
         */
        if QueryCancelPending != 0 {
            QueryCancelPending = 0;
            ereport!(WARNING,
                errmsg!("canceling wait for synchronous replication due to user request"));
            SyncRepCancelWait();
            break;
        }

        /*
         * Wait on latch.  Any condition that should wake us up will set the
         * latch, so no need for timeout.
         */
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
                       WAIT_EVENT_SYNC_REP);

        /*
         * If the postmaster dies, we'll probably never get an acknowledgment,
         * because all the wal sender processes will exit. So just bail out.
         */
        if rc & WL_POSTMASTER_DEATH != 0 {
            ProcDiePending = 1;
            whereToSendOutput = DestNone;
            SyncRepCancelWait();
            break;
        }
    }

    /*
     * WalSender has checked our LSN and has removed us from queue. Clean up
     * state and leave.  It's OK to reset these shared memory fields without
     * holding SyncRepLock, because any walsenders will ignore us anyway when
     * we're not on the queue.  We need a read barrier to make sure we see the
     * changes to the queue link (this might be unnecessary without
     * assertions, but better safe than sorry).
     */
    pg_read_barrier();
    Assert!(dlist_node_is_detached(&(*MyProc).syncRepLinks));
    (*MyProc).syncRepState = SYNC_REP_NOT_WAITING;
    (*MyProc).waitLSN = 0;

    /* reset ps display to remove the suffix */
    if update_process_title {
        set_ps_display_remove_suffix();
    }
}

/// Insert MyProc into the specified SyncRepQueue, maintaining sorted invariant.
///
/// Usually we will go at tail of queue, though it's possible that we arrive
/// here out of order, so start at tail and work back to insertion point.
unsafe fn SyncRepQueueInsert(mode: c_int) {
    let queue: *mut dlist_head;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE as c_int);
    queue = &mut (*WalSndCtl).SyncRepQueue[mode as usize];

    dlist_reverse_foreach!(iter, queue, {
        let proc_: *mut PGPROC = dlist_container!(PGPROC, syncRepLinks, iter.cur);

        /*
         * Stop at the queue element that we should insert after to ensure the
         * queue is ordered by LSN.
         */
        if (*proc_).waitLSN < (*MyProc).waitLSN {
            dlist_insert_after(&mut (*proc_).syncRepLinks, &mut (*MyProc).syncRepLinks);
            return;
        }
    });

    /*
     * If we get here, the list was either empty, or this process needs to be
     * at the head.
     */
    dlist_push_head(queue, &mut (*MyProc).syncRepLinks);
}

/// Acquire SyncRepLock and cancel any wait currently in progress.
unsafe fn SyncRepCancelWait() {
    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    if !dlist_node_is_detached(&(*MyProc).syncRepLinks) {
        dlist_delete_thoroughly(&mut (*MyProc).syncRepLinks);
    }
    (*MyProc).syncRepState = SYNC_REP_NOT_WAITING;
    LWLockRelease(SyncRepLock);
}

pub unsafe fn SyncRepCleanupAtProcExit() {
    /*
     * First check if we are removed from the queue without the lock to not
     * slow down backend exit.
     */
    if !dlist_node_is_detached(&(*MyProc).syncRepLinks) {
        LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

        /* maybe we have just been removed, so recheck */
        if !dlist_node_is_detached(&(*MyProc).syncRepLinks) {
            dlist_delete_thoroughly(&mut (*MyProc).syncRepLinks);
        }

        LWLockRelease(SyncRepLock);
    }
}

// ===========================================================
// Synchronous Replication functions for wal sender processes
// ===========================================================

/// Take any action required to initialise sync rep state from config
/// data. Called at WALSender startup and after each SIGHUP.
pub unsafe fn SyncRepInitConfig() {
    let priority: c_int;

    /*
     * Determine if we are a potential sync standby and remember the result
     * for handling replies from standby.
     */
    priority = SyncRepGetStandbyPriority();
    if (*MyWalSnd).sync_standby_priority != priority {
        SpinLockAcquire(&mut (*MyWalSnd).mutex);
        (*MyWalSnd).sync_standby_priority = priority;
        SpinLockRelease(&mut (*MyWalSnd).mutex);

        elog!(DEBUG1, "standby \"{}\" now has synchronous standby priority {}",
              core::ffi::CStr::from_ptr(application_name).to_string_lossy(),
              priority);
    }
}

/// Update the LSNs on each queue based upon our latest state. This
/// implements a simple policy of first-valid-sync-standby-releases-waiter.
///
/// Other policies are possible, which would change what we do here and
/// perhaps also which information we store as well.
pub unsafe fn SyncRepReleaseWaiters() {
    let walsndctl: *mut WalSndCtlData = WalSndCtl;
    let mut writePtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut flushPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut applyPtr: XLogRecPtr = InvalidXLogRecPtr;
    let got_recptr: bool;
    let mut am_sync: bool = false;
    let mut numwrite: c_int = 0;
    let mut numflush: c_int = 0;
    let mut numapply: c_int = 0;

    /*
     * If this WALSender is serving a standby that is not on the list of
     * potential sync standbys then we have nothing to do. If we are still
     * starting up, still running base backup or the current flush position is
     * still invalid, then leave quickly also.  Streaming or stopping WAL
     * senders are allowed to release waiters.
     */
    if (*MyWalSnd).sync_standby_priority == 0
        || ((*MyWalSnd).state != WALSNDSTATE_STREAMING
            && (*MyWalSnd).state != WALSNDSTATE_STOPPING)
        || XLogRecPtrIsInvalid((*MyWalSnd).flush)
    {
        announce_next_takeover = true;
        return;
    }

    /*
     * We're a potential sync standby. Release waiters if there are enough
     * sync standbys and we are considered as sync.
     */
    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

    /*
     * Check whether we are a sync standby or not, and calculate the synced
     * positions among all sync standbys.  (Note: although this step does not
     * of itself require holding SyncRepLock, it seems like a good idea to do
     * it after acquiring the lock.  This ensures that the WAL pointers we use
     * to release waiters are newer than any previous execution of this
     * routine used.)
     */
    got_recptr = SyncRepGetSyncRecPtr(&mut writePtr, &mut flushPtr, &mut applyPtr, &mut am_sync);

    /*
     * If we are managing a sync standby, though we weren't prior to this,
     * then announce we are now a sync standby.
     */
    if announce_next_takeover && am_sync {
        announce_next_takeover = false;

        if (*SyncRepConfig).syncrep_method == SYNC_REP_PRIORITY {
            elog!(LOG, "standby \"{}\" is now a synchronous standby with priority {}",
                  core::ffi::CStr::from_ptr(application_name).to_string_lossy(),
                  (*MyWalSnd).sync_standby_priority);
        } else {
            elog!(LOG, "standby \"{}\" is now a candidate for quorum synchronous standby",
                  core::ffi::CStr::from_ptr(application_name).to_string_lossy());
        }
    }

    /*
     * If the number of sync standbys is less than requested or we aren't
     * managing a sync standby then just leave.
     */
    if !got_recptr || !am_sync {
        LWLockRelease(SyncRepLock);
        announce_next_takeover = !am_sync;
        return;
    }

    /*
     * Set the lsn first so that when we wake backends they will release up to
     * this location.
     */
    if (*walsndctl).lsn[SYNC_REP_WAIT_WRITE as usize] < writePtr {
        (*walsndctl).lsn[SYNC_REP_WAIT_WRITE as usize] = writePtr;
        numwrite = SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
    }
    if (*walsndctl).lsn[SYNC_REP_WAIT_FLUSH as usize] < flushPtr {
        (*walsndctl).lsn[SYNC_REP_WAIT_FLUSH as usize] = flushPtr;
        numflush = SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
    }
    if (*walsndctl).lsn[SYNC_REP_WAIT_APPLY as usize] < applyPtr {
        (*walsndctl).lsn[SYNC_REP_WAIT_APPLY as usize] = applyPtr;
        numapply = SyncRepWakeQueue(false, SYNC_REP_WAIT_APPLY);
    }

    LWLockRelease(SyncRepLock);

    let (whi, wlo) = LSN_FORMAT_ARGS(writePtr);
    let (fhi, flo) = LSN_FORMAT_ARGS(flushPtr);
    let (ahi, alo) = LSN_FORMAT_ARGS(applyPtr);
    elog!(DEBUG3,
          "released {} procs up to write {}/{}, {} procs up to flush {}/{}, {} procs up to apply {}/{}",
          numwrite, whi, wlo,
          numflush, fhi, flo,
          numapply, ahi, alo);
}

/// Calculate the synced Write, Flush and Apply positions among sync standbys.
///
/// Return false if the number of sync standbys is less than
/// synchronous_standby_names specifies. Otherwise return true and
/// store the positions into *writePtr, *flushPtr and *applyPtr.
///
/// On return, *am_sync is set to true if this walsender is connecting to
/// sync standby. Otherwise it's set to false.
unsafe fn SyncRepGetSyncRecPtr(
    writePtr: *mut XLogRecPtr,
    flushPtr: *mut XLogRecPtr,
    applyPtr: *mut XLogRecPtr,
    am_sync: *mut bool,
) -> bool {
    let mut sync_standbys: *mut SyncRepStandbyData = core::ptr::null_mut();
    let num_standbys: c_int;
    let mut i: c_int;

    /* Initialize default results */
    *writePtr = InvalidXLogRecPtr;
    *flushPtr = InvalidXLogRecPtr;
    *applyPtr = InvalidXLogRecPtr;
    *am_sync = false;

    /* Quick out if not even configured to be synchronous */
    if SyncRepConfig.is_null() {
        return false;
    }

    /* Get standbys that are considered as synchronous at this moment */
    num_standbys = SyncRepGetCandidateStandbys(&mut sync_standbys);

    /* Am I among the candidate sync standbys? */
    i = 0;
    while i < num_standbys {
        if (*sync_standbys.offset(i as isize)).is_me {
            *am_sync = true;
            break;
        }
        i += 1;
    }

    /*
     * Nothing more to do if we are not managing a sync standby or there are
     * not enough synchronous standbys.
     */
    if !(*am_sync) || num_standbys < (*SyncRepConfig).num_sync {
        pfree(sync_standbys as *mut c_void);
        return false;
    }

    /*
     * In a priority-based sync replication, the synced positions are the
     * oldest ones among sync standbys. In a quorum-based, they are the Nth
     * latest ones.
     *
     * SyncRepGetNthLatestSyncRecPtr() also can calculate the oldest
     * positions. But we use SyncRepGetOldestSyncRecPtr() for that calculation
     * because it's a bit more efficient.
     *
     * XXX If the numbers of current and requested sync standbys are the same,
     * we can use SyncRepGetOldestSyncRecPtr() to calculate the synced
     * positions even in a quorum-based sync replication.
     */
    if (*SyncRepConfig).syncrep_method == SYNC_REP_PRIORITY {
        SyncRepGetOldestSyncRecPtr(writePtr, flushPtr, applyPtr,
                                   sync_standbys, num_standbys);
    } else {
        SyncRepGetNthLatestSyncRecPtr(writePtr, flushPtr, applyPtr,
                                      sync_standbys, num_standbys,
                                      (*SyncRepConfig).num_sync as u8);
    }

    pfree(sync_standbys as *mut c_void);
    true
}

/// Calculate the oldest Write, Flush and Apply positions among sync standbys.
unsafe fn SyncRepGetOldestSyncRecPtr(
    writePtr: *mut XLogRecPtr,
    flushPtr: *mut XLogRecPtr,
    applyPtr: *mut XLogRecPtr,
    sync_standbys: *mut SyncRepStandbyData,
    num_standbys: c_int,
) {
    let mut i: c_int = 0;

    /*
     * Scan through all sync standbys and calculate the oldest Write, Flush
     * and Apply positions.  We assume *writePtr et al were initialized to
     * InvalidXLogRecPtr.
     */
    while i < num_standbys {
        let write: XLogRecPtr = (*sync_standbys.offset(i as isize)).write;
        let flush: XLogRecPtr = (*sync_standbys.offset(i as isize)).flush;
        let apply: XLogRecPtr = (*sync_standbys.offset(i as isize)).apply;

        if XLogRecPtrIsInvalid(*writePtr) || *writePtr > write {
            *writePtr = write;
        }
        if XLogRecPtrIsInvalid(*flushPtr) || *flushPtr > flush {
            *flushPtr = flush;
        }
        if XLogRecPtrIsInvalid(*applyPtr) || *applyPtr > apply {
            *applyPtr = apply;
        }
        i += 1;
    }
}

/// Calculate the Nth latest Write, Flush and Apply positions among sync
/// standbys.
unsafe fn SyncRepGetNthLatestSyncRecPtr(
    writePtr: *mut XLogRecPtr,
    flushPtr: *mut XLogRecPtr,
    applyPtr: *mut XLogRecPtr,
    sync_standbys: *mut SyncRepStandbyData,
    num_standbys: c_int,
    nth: u8,
) {
    let write_array: *mut XLogRecPtr;
    let flush_array: *mut XLogRecPtr;
    let apply_array: *mut XLogRecPtr;
    let mut i: c_int;

    /* Should have enough candidates, or somebody messed up */
    Assert!(nth > 0 && (nth as c_int) <= num_standbys);

    write_array = palloc(core::mem::size_of::<XLogRecPtr>() * num_standbys as usize) as *mut XLogRecPtr;
    flush_array = palloc(core::mem::size_of::<XLogRecPtr>() * num_standbys as usize) as *mut XLogRecPtr;
    apply_array = palloc(core::mem::size_of::<XLogRecPtr>() * num_standbys as usize) as *mut XLogRecPtr;

    i = 0;
    while i < num_standbys {
        *write_array.offset(i as isize) = (*sync_standbys.offset(i as isize)).write;
        *flush_array.offset(i as isize) = (*sync_standbys.offset(i as isize)).flush;
        *apply_array.offset(i as isize) = (*sync_standbys.offset(i as isize)).apply;
        i += 1;
    }

    /* Sort each array in descending order */
    let write_slice = core::slice::from_raw_parts_mut(write_array, num_standbys as usize);
    let flush_slice = core::slice::from_raw_parts_mut(flush_array, num_standbys as usize);
    let apply_slice = core::slice::from_raw_parts_mut(apply_array, num_standbys as usize);
    write_slice.sort_unstable_by(|a, b| b.cmp(a)); // descending
    flush_slice.sort_unstable_by(|a, b| b.cmp(a));
    apply_slice.sort_unstable_by(|a, b| b.cmp(a));

    /* Get Nth latest Write, Flush, Apply positions */
    *writePtr = *write_array.offset((nth as c_int - 1) as isize);
    *flushPtr = *flush_array.offset((nth as c_int - 1) as isize);
    *applyPtr = *apply_array.offset((nth as c_int - 1) as isize);

    pfree(write_array as *mut c_void);
    pfree(flush_array as *mut c_void);
    pfree(apply_array as *mut c_void);
}

/// Compare lsn in order to sort array in descending order.
unsafe extern "C" fn cmp_lsn(a: *const c_void, b: *const c_void) -> c_int {
    let lsn1: XLogRecPtr = *(a as *const XLogRecPtr);
    let lsn2: XLogRecPtr = *(b as *const XLogRecPtr);

    pg_cmp_u64(lsn2, lsn1)
}

/// Return data about walsenders that are candidates to be sync standbys.
///
/// *standbys is set to a palloc'd array of structs of per-walsender data,
/// and the number of valid entries (candidate sync senders) is returned.
/// (This might be more or fewer than num_sync; caller must check.)
pub unsafe fn SyncRepGetCandidateStandbys(standbys: *mut *mut SyncRepStandbyData) -> c_int {
    let mut i: c_int;
    let mut n: c_int;

    /* Create result array */
    *standbys = palloc(max_wal_senders as usize * core::mem::size_of::<SyncRepStandbyData>())
        as *mut SyncRepStandbyData;

    /* Quick exit if sync replication is not requested */
    if SyncRepConfig.is_null() {
        return 0;
    }

    /* Collect raw data from shared memory */
    n = 0;
    i = 0;
    while i < max_wal_senders {
        let walsnd: *const WalSnd; // Use volatile pointer to prevent code rearrangement
        let stby: *mut SyncRepStandbyData;
        let state: WalSndState; // not included in SyncRepStandbyData

        walsnd = &(*WalSndCtl).walsnds[i as usize] as *const WalSnd;
        stby = (*standbys).offset(n as isize);

        SpinLockAcquire(&mut (*(walsnd as *mut WalSnd)).mutex);
        (*stby).pid = (*(walsnd as *const WalSnd)).pid;
        state = (*(walsnd as *const WalSnd)).state;
        (*stby).write = (*(walsnd as *const WalSnd)).write;
        (*stby).flush = (*(walsnd as *const WalSnd)).flush;
        (*stby).apply = (*(walsnd as *const WalSnd)).apply;
        (*stby).sync_standby_priority = (*(walsnd as *const WalSnd)).sync_standby_priority;
        SpinLockRelease(&mut (*(walsnd as *mut WalSnd)).mutex);

        /* Must be active */
        if (*stby).pid == 0 {
            i += 1;
            continue;
        }

        /* Must be streaming or stopping */
        if state != WALSNDSTATE_STREAMING && state != WALSNDSTATE_STOPPING {
            i += 1;
            continue;
        }

        /* Must be synchronous */
        if (*stby).sync_standby_priority == 0 {
            i += 1;
            continue;
        }

        /* Must have a valid flush position */
        if XLogRecPtrIsInvalid((*stby).flush) {
            i += 1;
            continue;
        }

        /* OK, it's a candidate */
        (*stby).walsnd_index = i;
        (*stby).is_me = (walsnd == MyWalSnd as *const WalSnd);
        n += 1;
        i += 1;
    }

    /*
     * In quorum mode, we return all the candidates.  In priority mode, if we
     * have too many candidates then return only the num_sync ones of highest
     * priority.
     */
    if (*SyncRepConfig).syncrep_method == SYNC_REP_PRIORITY && n > (*SyncRepConfig).num_sync {
        /* Sort by priority ... */
        let slice = core::slice::from_raw_parts_mut(*standbys, n as usize);
        slice.sort_unstable_by(|a, b| standby_priority_comparator_inner(a, b));
        /* ... then report just the first num_sync ones */
        n = (*SyncRepConfig).num_sync;
    }

    n
}

/// Comparator to sort SyncRepStandbyData entries by priority (inner Rust version).
fn standby_priority_comparator_inner(
    sa: &SyncRepStandbyData,
    sb: &SyncRepStandbyData,
) -> core::cmp::Ordering {
    /* First, sort by increasing priority value */
    if sa.sync_standby_priority != sb.sync_standby_priority {
        return sa.sync_standby_priority.cmp(&sb.sync_standby_priority);
    }

    /*
     * We might have equal priority values; arbitrarily break ties by position
     * in the WalSnd array.  (This is utterly bogus, since that is arrival
     * order dependent, but there are regression tests that rely on it.)
     */
    sa.walsnd_index.cmp(&sb.walsnd_index)
}

/// qsort comparator to sort SyncRepStandbyData entries by priority.
unsafe extern "C" fn standby_priority_comparator(a: *const c_void, b: *const c_void) -> c_int {
    let sa: &SyncRepStandbyData = &*(a as *const SyncRepStandbyData);
    let sb: &SyncRepStandbyData = &*(b as *const SyncRepStandbyData);

    /* First, sort by increasing priority value */
    if sa.sync_standby_priority != sb.sync_standby_priority {
        return sa.sync_standby_priority - sb.sync_standby_priority;
    }

    /*
     * We might have equal priority values; arbitrarily break ties by position
     * in the WalSnd array.
     */
    sa.walsnd_index - sb.walsnd_index
}

/// Check if we are in the list of sync standbys, and if so, determine
/// priority sequence. Return priority if set, or zero to indicate that
/// we are not a potential sync standby.
///
/// Compare the parameter SyncRepStandbyNames against the application_name
/// for this WALSender, or allow any name if we find a wildcard "*".
unsafe fn SyncRepGetStandbyPriority() -> c_int {
    let mut standby_name: *const c_char;
    let mut priority: c_int;
    let mut found = false;

    /*
     * Since synchronous cascade replication is not allowed, we always set the
     * priority of cascading walsender to zero.
     */
    if am_cascading_walsender {
        return 0;
    }

    if !SyncStandbysDefined() || SyncRepConfig.is_null() {
        return 0;
    }

    standby_name = (*SyncRepConfig).member_names.as_ptr();
    priority = 1;
    while priority <= (*SyncRepConfig).nmembers {
        if pg_strcasecmp(standby_name, application_name) == 0
            || strcmp(standby_name, c"*".as_ptr()) == 0
        {
            found = true;
            break;
        }
        standby_name = standby_name.add(strlen(standby_name) + 1);
        priority += 1;
    }

    if !found {
        return 0;
    }

    /*
     * In quorum-based sync replication, all the standbys in the list have the
     * same priority, one.
     */
    if (*SyncRepConfig).syncrep_method == SYNC_REP_PRIORITY {
        priority
    } else {
        1
    }
}

/// Walk the specified queue from head.  Set the state of any backends that
/// need to be woken, remove them from the queue, and then wake them.
/// Pass all = true to wake whole queue; otherwise, just wake up to
/// the walsender's LSN.
///
/// The caller must hold SyncRepLock in exclusive mode.
unsafe fn SyncRepWakeQueue(all: bool, mode: c_int) -> c_int {
    let walsndctl: *mut WalSndCtlData = WalSndCtl;
    let mut numprocs: c_int = 0;
    let mut iter: dlist_mutable_iter = core::mem::zeroed();

    Assert!(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE as c_int);
    #[cfg(debug_assertions)]
    Assert!(LWLockHeldByMeInMode(SyncRepLock, LW_EXCLUSIVE));
    Assert!(SyncRepQueueIsOrderedByLSN(mode));

    dlist_foreach_modify!(iter, &mut (*WalSndCtl).SyncRepQueue[mode as usize], {
        let proc_: *mut PGPROC = dlist_container!(PGPROC, syncRepLinks, iter.cur);

        /*
         * Assume the queue is ordered by LSN
         */
        if !all && (*walsndctl).lsn[mode as usize] < (*proc_).waitLSN {
            return numprocs;
        }

        /*
         * Remove from queue.
         */
        dlist_delete_thoroughly(&mut (*proc_).syncRepLinks);

        /*
         * SyncRepWaitForLSN() reads syncRepState without holding the lock, so
         * make sure that it sees the queue link being removed before the
         * syncRepState change.
         */
        pg_write_barrier();

        /*
         * Set state to complete; see SyncRepWaitForLSN() for discussion of
         * the various states.
         */
        (*proc_).syncRepState = SYNC_REP_WAIT_COMPLETE;

        /*
         * Wake only when we have set state and removed from queue.
         */
        SetLatch(&mut (*proc_).procLatch);

        numprocs += 1;
    });

    numprocs
}

/// The checkpointer calls this as needed to update the shared
/// sync_standbys_status flag, so that backends don't remain permanently wedged
/// if synchronous_standby_names is unset.  It's safe to check the current value
/// without the lock, because it's only ever updated by one process.  But we
/// must take the lock to change it.
pub unsafe fn SyncRepUpdateSyncStandbysDefined() {
    let sync_standbys_defined = SyncStandbysDefined();

    if sync_standbys_defined
        != (((*WalSndCtl).sync_standbys_status & SYNC_STANDBY_DEFINED) != 0)
    {
        LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

        /*
         * If synchronous_standby_names has been reset to empty, it's futile
         * for backends to continue waiting.  Since the user no longer wants
         * synchronous replication, we'd better wake them up.
         */
        if !sync_standbys_defined {
            let mut i: c_int = 0;
            while i < NUM_SYNC_REP_WAIT_MODE as c_int {
                SyncRepWakeQueue(true, i);
                i += 1;
            }
        }

        /*
         * Only allow people to join the queue when there are synchronous
         * standbys defined.  Without this interlock, there's a race
         * condition: we might wake up all the current waiters; then, some
         * backend that hasn't yet reloaded its config might go to sleep on
         * the queue (and never wake up).  This prevents that.
         */
        (*WalSndCtl).sync_standbys_status = SYNC_STANDBY_INIT
            | (if sync_standbys_defined { SYNC_STANDBY_DEFINED } else { 0 });

        LWLockRelease(SyncRepLock);
    } else if ((*WalSndCtl).sync_standbys_status & SYNC_STANDBY_INIT) == 0 {
        LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

        /*
         * Note that there is no need to wake up the queues here.  We would
         * reach this path only if SyncStandbysDefined() returns false, or it
         * would mean that some backends are waiting with the GUC set.  See
         * SyncRepWaitForLSN().
         */
        Assert!(!SyncStandbysDefined());

        /*
         * Even if there is no sync standby defined, let the readers of this
         * information know that the sync standby data has been initialized.
         * This can just be done once, hence the previous check on
         * SYNC_STANDBY_INIT to avoid useless work.
         */
        (*WalSndCtl).sync_standbys_status |= SYNC_STANDBY_INIT;

        LWLockRelease(SyncRepLock);
    }
}

/// Assert that the SyncRepQueue for mode is ordered by LSN (debug only).
#[cfg(debug_assertions)]
unsafe fn SyncRepQueueIsOrderedByLSN(mode: c_int) -> bool {
    let mut lastLSN: XLogRecPtr;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE as c_int);

    lastLSN = 0;

    dlist_foreach!(iter, &mut (*WalSndCtl).SyncRepQueue[mode as usize], {
        let proc_: *mut PGPROC = dlist_container!(PGPROC, syncRepLinks, iter.cur);

        /*
         * Check the queue is ordered by LSN and that multiple procs don't
         * have matching LSNs
         */
        if (*proc_).waitLSN <= lastLSN {
            return false;
        }

        lastLSN = (*proc_).waitLSN;
    });

    true
}

#[cfg(not(debug_assertions))]
#[inline]
unsafe fn SyncRepQueueIsOrderedByLSN(_mode: c_int) -> bool {
    true
}

// ===========================================================
// Synchronous Replication functions executed by any process
// ===========================================================

pub unsafe fn check_synchronous_standby_names(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    if !(*newval).is_null() && *(*newval) != 0 {
        let mut scanner: *mut c_void = core::ptr::null_mut();
        let parse_rc: c_int;
        let pconf: *mut SyncRepConfigData;

        /* Result of parsing is returned in one of these two variables */
        let mut syncrep_parse_result: *mut SyncRepConfigData = core::ptr::null_mut();
        let mut syncrep_parse_error_msg: *mut c_char = core::ptr::null_mut();

        /* Parse the synchronous_standby_names string */
        syncrep_scanner_init(*newval, &mut scanner);
        parse_rc = syncrep_yyparse(
            &mut syncrep_parse_result,
            &mut syncrep_parse_error_msg,
            scanner,
        );
        syncrep_scanner_finish(scanner);

        if parse_rc != 0 || syncrep_parse_result.is_null() {
            GUC_check_errcode!(ERRCODE_SYNTAX_ERROR);
            if !syncrep_parse_error_msg.is_null() {
                GUC_check_errdetail!("{}", core::ffi::CStr::from_ptr(syncrep_parse_error_msg).to_string_lossy());
            } else {
                /* translator: %s is a GUC name */
                GUC_check_errdetail!("\"{}\" parser failed.", "synchronous_standby_names");
            }
            return false;
        }

        if (*syncrep_parse_result).num_sync <= 0 {
            GUC_check_errmsg!(
                "number of synchronous standbys ({}) must be greater than zero",
                (*syncrep_parse_result).num_sync
            );
            return false;
        }

        /* GUC extra value must be guc_malloc'd, not palloc'd */
        pconf = guc_malloc(LOG, (*syncrep_parse_result).config_size as Size) as *mut SyncRepConfigData;
        if pconf.is_null() {
            return false;
        }
        memcpy(pconf as *mut c_void,
               syncrep_parse_result as *const c_void,
               (*syncrep_parse_result).config_size as usize);

        *extra = pconf as *mut c_void;

        /*
         * We need not explicitly clean up syncrep_parse_result.  It, and any
         * other cruft generated during parsing, will be freed when the
         * current memory context is deleted.  (This code is generally run in
         * a short-lived context used for config file processing, so that will
         * not be very long.)
         */
    } else {
        *extra = core::ptr::null_mut();
    }

    true
}

pub unsafe fn assign_synchronous_standby_names(newval: *const c_char, extra: *mut c_void) {
    SyncRepConfig = extra as *mut SyncRepConfigData;
}

pub unsafe fn assign_synchronous_commit(newval: c_int, extra: *mut c_void) {
    match newval {
        n if n == SYNCHRONOUS_COMMIT_REMOTE_WRITE => {
            SyncRepWaitMode = SYNC_REP_WAIT_WRITE;
        }
        n if n == SYNCHRONOUS_COMMIT_REMOTE_FLUSH => {
            SyncRepWaitMode = SYNC_REP_WAIT_FLUSH;
        }
        n if n == SYNCHRONOUS_COMMIT_REMOTE_APPLY => {
            SyncRepWaitMode = SYNC_REP_WAIT_APPLY;
        }
        _ => {
            SyncRepWaitMode = SYNC_REP_NO_WAIT;
        }
    }
}
