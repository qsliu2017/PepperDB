//! replication/walsender_private.h - private definitions from replication/walsender.c.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::ffi::{c_char, c_int, c_void};

use crate::c::{bits8, FLEXIBLE_ARRAY_MEMBER};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::lib::ilist::dlist_head;
use crate::nodes::nodes::Node;
use crate::nodes::replnodes::ReplicationKind;

// ---------------------------------------------------------------------------
// Locally-stubbed types referenced by this header whose owning headers are not
// yet ported. Replace with real imports when those modules land.
// ---------------------------------------------------------------------------

/// STUB: `pid_t` (Unix/macOS = c_int). TODO: dedup when a canonical port lands.
pub type pid_t = c_int;

/// STUB: `TimeOffset` from datatype/timestamp.h (int64). TODO: dedup.
pub type TimeOffset = i64;

/// STUB: `TimestampTz` from datatype/timestamp.h (int64). TODO: dedup.
pub type TimestampTz = i64;

/// STUB: `slock_t` (spinlock) from storage/s_lock.h (machine dependent).
/// TODO: dedup when storage/spin.h / s_lock.h lands.
pub type slock_t = c_int;

/// Canonical ConditionVariable (real layout: spinlock + wakeup proclist), so the
/// WalSndCtlData CV fields are correctly sized/offset and WalSndShmemSize accounts
/// for them (a zero-size c_void stub aliased all three CVs at one offset).
pub use crate::storage::lmgr::condition_variable::ConditionVariable;

/// STUB: `NUM_SYNC_REP_WAIT_MODE` from replication/syncrep.h.  Mirrors the C
/// value (SYNC_REP_WAIT_WRITE, _FLUSH, _APPLY => 3).
/// TODO: dedup when replication/syncrep.h lands.
pub const NUM_SYNC_REP_WAIT_MODE: usize = 3;

/// STUB: `union YYSTYPE` (forward-declared in this header; real definition in
/// the generated replication grammar). TODO: dedup when repl_gram lands.
pub type YYSTYPE = c_void;

// ---------------------------------------------------------------------------
// WalSndState
// ---------------------------------------------------------------------------

pub type WalSndState = c_int;
pub const WALSNDSTATE_STARTUP: WalSndState = 0;
pub const WALSNDSTATE_BACKUP: WalSndState = 1;
pub const WALSNDSTATE_CATCHUP: WalSndState = 2;
pub const WALSNDSTATE_STREAMING: WalSndState = 3;
pub const WALSNDSTATE_STOPPING: WalSndState = 4;

/// Each walsender has a WalSnd struct in shared memory.
///
/// This struct is protected by its 'mutex' spinlock field, except that some
/// members are only written by the walsender process itself, and thus that
/// process is free to read those members without holding spinlock.  pid and
/// needreload always require the spinlock to be held for all accesses.
#[repr(C)]
pub struct WalSnd {
    /// this walsender's PID, or 0 if not active
    pub pid: pid_t,

    /// this walsender's state
    pub state: WalSndState,
    /// WAL has been sent up to this point
    pub sentPtr: XLogRecPtr,
    /// does currently-open file need to be reloaded?
    pub needreload: bool,

    /// The xlog locations that have been written, flushed, and applied by
    /// standby-side. These may be invalid if the standby-side has not offered
    /// values yet.
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,

    /// Measured lag times, or -1 for unknown/none.
    pub writeLag: TimeOffset,
    pub flushLag: TimeOffset,
    pub applyLag: TimeOffset,

    /// The priority order of the standby managed by this WALSender, as listed
    /// in synchronous_standby_names, or 0 if not-listed.
    pub sync_standby_priority: c_int,

    /// Protects shared variables in this structure.
    pub mutex: slock_t,

    /// Timestamp of the last message received from standby.
    pub replyTime: TimestampTz,

    pub kind: ReplicationKind,
}

extern "C" {
    pub static mut MyWalSnd: *mut WalSnd;
}

/// There is one WalSndCtl struct for the whole database cluster
#[repr(C)]
pub struct WalSndCtlData {
    /// Synchronous replication queue with one queue per request type.
    /// Protected by SyncRepLock.
    pub SyncRepQueue: [dlist_head; NUM_SYNC_REP_WAIT_MODE],

    /// Current location of the head of the queue. All waiters should have a
    /// waitLSN that follows this value. Protected by SyncRepLock.
    pub lsn: [XLogRecPtr; NUM_SYNC_REP_WAIT_MODE],

    /// Status of data related to the synchronous standbys.  Waiting backends
    /// can't reload the config file safely, so checkpointer updates this value
    /// as needed. Protected by SyncRepLock.
    pub sync_standbys_status: bits8,

    /// used as a registry of physical / logical walsenders to wake
    pub wal_flush_cv: ConditionVariable,
    pub wal_replay_cv: ConditionVariable,

    /// Used by physical walsenders holding slots specified in
    /// synchronized_standby_slots to wake up logical walsenders holding
    /// logical failover slots when a walreceiver confirms the receipt of LSN.
    pub wal_confirm_rcv_cv: ConditionVariable,

    pub walsnds: [WalSnd; FLEXIBLE_ARRAY_MEMBER],
}

// Flags for WalSndCtlData->sync_standbys_status

/// Is the synchronous standby data initialized from the GUC?  This is set the
/// first time synchronous_standby_names is processed by the checkpointer.
pub const SYNC_STANDBY_INIT: bits8 = 1 << 0;

/// Is the synchronous standby data defined?  This is set when
/// synchronous_standby_names has some data, after being processed by the
/// checkpointer.
pub const SYNC_STANDBY_DEFINED: bits8 = 1 << 1;

extern "C" {
    pub static mut WalSndCtl: *mut WalSndCtlData;
}

pub unsafe fn WalSndSetState(state: WalSndState) {
    let _ = state;
    unimplemented!()
}

/// Internal functions for parsing the replication grammar, in repl_gram.y and
/// repl_scanner.l

#[allow(non_camel_case_types)]
pub type yyscan_t = *mut c_void;

pub unsafe fn replication_yyparse(
    replication_parse_result_p: *mut *mut Node,
    yyscanner: yyscan_t,
) -> c_int {
    let _ = (replication_parse_result_p, yyscanner);
    unimplemented!()
}

pub unsafe fn replication_yylex(
    yylval_param: *mut YYSTYPE,
    yyscanner: yyscan_t,
) -> c_int {
    let _ = (yylval_param, yyscanner);
    unimplemented!()
}

/// pg_noreturn
pub unsafe fn replication_yyerror(
    replication_parse_result_p: *mut *mut Node,
    yyscanner: yyscan_t,
    message: *const c_char,
) -> ! {
    let _ = (replication_parse_result_p, yyscanner, message);
    unimplemented!()
}

pub unsafe fn replication_scanner_init(str: *const c_char, yyscannerp: *mut yyscan_t) {
    let _ = (str, yyscannerp);
    unimplemented!()
}

pub unsafe fn replication_scanner_finish(yyscanner: yyscan_t) {
    let _ = yyscanner;
    unimplemented!()
}

pub unsafe fn replication_scanner_is_replication_command(yyscanner: yyscan_t) -> bool {
    let _ = yyscanner;
    unimplemented!()
}
