//! Translated from PostgreSQL src/include/replication/walreceiver.h
//! Exports from replication/walreceiverfuncs.c.
//!
//! Per routine-struct.md: `WalReceiverFunctionsType` (a hook table of fn
//! pointers, NULL-checked at call sites - appendix B) becomes a `WalReceiver`
//! trait; the `walrcv_*` dispatch macros become free fns that forward to the
//! installed implementation. `WalRcvData` is shared-memory state - per
//! LEVEL2-NOTES the shmem/spinlock/CV/atomics collapse under single-process to
//! owned fields + std/atomic types.

use std::sync::atomic::AtomicU64;

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::c::NAMEDATALEN;
use crate::datatype::timestamp::TimestampTz;
use crate::port::pgsocket;
use crate::postgres_ext::Oid;
use crate::replication::walsender::{CRSSnapshotAction, max_wal_senders};
use crate::storage::procnumber::ProcNumber;
use crate::utils::tuplestore::Tuplestorestate;
use crate::access::tupdesc::TupleDesc;
use crate::pgtime::pg_time_t;

// user-settable parameters (GUCs -> session/global state later)
pub static mut wal_receiver_status_interval: i32 = 0;
pub static mut wal_receiver_timeout: i32 = 0;
pub static mut hot_standby_feedback: bool = false;

// EnableHotStandby is owned by access/xlog.h, not yet translated there.
// Rule 7: define locally, repoint in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::access::xlog::EnableHotStandby in Phase 2")]
pub static mut EnableHotStandby: bool = false; // TODO(struct-forward)

/// Maximum size of a connection string.
pub const MAXCONNINFO: usize = 1024;

/// Maximum host name length (C: `NI_MAXHOST` from <netdb.h>).
pub const NI_MAXHOST: usize = 1025;

/// Can the standby accept a replication connection from another standby?
/// C inline macro `AllowCascadeReplication()`.
pub fn AllowCascadeReplication() -> bool {
    unsafe { EnableHotStandby && max_wal_senders > 0 }
}

/// Values for `WalRcv->walRcvState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRcvState {
    Stopped,    // stopped and mustn't start up again
    Starting,   // launched, but the process hasn't initialized yet
    Streaming,  // walreceiver is streaming
    Waiting,    // stopped streaming, waiting for orders
    Restarting, // asked to restart streaming
    Stopping,   // requested to stop, but still running
}

/// Management state for the walreceiver process. In C this is a shared-memory
/// struct guarded by `mutex`/`walRcvStoppedCV`; single-process simplification
/// drops the spinlock and condition variable (LEVEL2-NOTES). In-memory.
pub struct WalRcvData {
    pub procno: ProcNumber,
    pub pid: i32,
    pub wal_rcv_state: WalRcvState,
    // walRcvStoppedCV: ConditionVariable -> tokio::sync::Notify (dropped here)
    pub start_time: pg_time_t,
    pub receive_start: XLogRecPtr,
    pub receive_start_tli: TimeLineID,
    pub flushed_upto: XLogRecPtr,
    pub received_tli: TimeLineID,
    pub latest_chunk_start: XLogRecPtr,
    pub last_msg_send_time: TimestampTz,
    pub last_msg_receipt_time: TimestampTz,
    pub latest_wal_end: XLogRecPtr,
    pub latest_wal_end_time: TimestampTz,
    pub conninfo: [u8; MAXCONNINFO],
    pub sender_host: [u8; NI_MAXHOST],
    pub sender_port: i32,
    pub slotname: [u8; NAMEDATALEN],
    pub is_temp_slot: bool,
    pub ready_to_display: bool,
    // slock_t mutex -> dropped (single-process)
    pub written_upto: AtomicU64, // pg_atomic_uint64
    pub force_reply: bool,       // sig_atomic_t used as a bool
}

pub static mut WalRcv: Option<*mut WalRcvData> = None; // TODO(ptr): Arc-shared

/// Streaming options passed to `walrcv_startstreaming`. The C tagged union over
/// physical/logical is modelled as a Rust enum. In-memory.
pub struct WalRcvStreamOptions {
    pub slotname: Option<String>, // name of the replication slot or NULL
    pub startpoint: XLogRecPtr,   // LSN of starting point
    pub proto: WalRcvStreamProto,
}

/// The physical/logical `proto` union of `WalRcvStreamOptions`.
pub enum WalRcvStreamProto {
    Physical {
        startpoint_tli: TimeLineID,
    },
    Logical {
        proto_version: u32,
        publication_names: Vec<String>,
        binary: bool,
        streaming_str: Option<String>,
        twophase: bool,
        origin: Option<String>,
    },
}

/// Opaque per-module connection handle (C: incomplete `struct WalReceiverConn`).
pub struct WalReceiverConn {
    _private: [u8; 0],
}

/// Status of walreceiver query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRcvExecStatus {
    Error,       // there was an error when executing the query
    OkCommand,   // query executed utility or replication command
    OkTuples,    // query returned tuples
    OkCopyIn,    // query started COPY FROM
    OkCopyOut,   // query started COPY TO
    OkCopyBoth,  // query started COPY BOTH replication protocol
}

/// Return value for `walrcv_exec`: status plus any tuples. In-memory.
pub struct WalRcvExecResult {
    pub status: WalRcvExecStatus,
    pub sqlstate: i32,
    pub err: Option<String>,
    pub tuplestore: Option<Box<Tuplestorestate>>,
    pub tupledesc: TupleDesc,
}

/// WAL receiver - libpqwalreceiver hooks. C: `WalReceiverFunctionsType`, a
/// struct of fn pointers. Each method maps a `walrcv_*_fn`; optional callbacks
/// are NULL-checked in C, so they remain required here (load-time wiring).
pub trait WalReceiver {
    /// `walrcv_connect`: establish a connection; C returns NULL + `**err`, so
    /// `Result`.
    fn connect(
        &self,
        conninfo: &str,
        replication: bool,
        logical: bool,
        must_use_password: bool,
        appname: &str,
    ) -> Result<Box<WalReceiverConn>, String>;

    /// `walrcv_check_conninfo`: parse and validate the connection string.
    fn check_conninfo(&self, conninfo: &str, must_use_password: bool);

    /// `walrcv_get_conninfo`: user-displayable conninfo (sensitive fields hidden).
    fn get_conninfo(&self, conn: &WalReceiverConn) -> String;

    /// `walrcv_get_senderinfo`: out-params `**sender_host, *sender_port` -> tuple.
    fn get_senderinfo(&self, conn: &WalReceiverConn) -> (String, i32);

    /// `walrcv_identify_system`: returns system ID; out-param `*primary_tli` ->
    /// tuple.
    fn identify_system(&self, conn: &WalReceiverConn) -> (String, TimeLineID);

    /// `walrcv_get_dbname_from_conninfo`.
    fn get_dbname_from_conninfo(&self, conninfo: &str) -> Option<String>;

    /// `walrcv_server_version`.
    fn server_version(&self, conn: &WalReceiverConn) -> i32;

    /// `walrcv_readtimelinehistoryfile`: out-params `**filename, **content, *size`
    /// -> (filename, content bytes).
    fn read_timeline_history_file(
        &self,
        conn: &WalReceiverConn,
        tli: TimeLineID,
    ) -> (String, Vec<u8>);

    /// `walrcv_startstreaming`: true if switched to copy-both mode.
    fn start_streaming(&self, conn: &WalReceiverConn, options: &WalRcvStreamOptions) -> bool;

    /// `walrcv_endstreaming`: out-param `*next_tli` (0 if none) -> `Option`.
    fn end_streaming(&self, conn: &WalReceiverConn) -> Option<TimeLineID>;

    /// `walrcv_receive`: returns the message bytes, or the socket to wait on when
    /// nothing is available, or `None` when the cluster ended the COPY.
    /// C: len / 0+`*wait_fd` / -1.
    fn receive(&self, conn: &WalReceiverConn) -> WalRcvReceive;

    /// `walrcv_send`.
    fn send(&self, conn: &WalReceiverConn, buffer: &[u8]);

    /// `walrcv_create_slot`: out-param `*lsn` -> tuple; returns the exported
    /// snapshot name for a logical slot (`None` for physical).
    fn create_slot(
        &self,
        conn: &WalReceiverConn,
        slotname: &str,
        temporary: bool,
        two_phase: bool,
        failover: bool,
        snapshot_action: CRSSnapshotAction,
    ) -> (Option<String>, XLogRecPtr);

    /// `walrcv_alter_slot`: the `const bool *` skippable args -> `Option<bool>`.
    fn alter_slot(
        &self,
        conn: &WalReceiverConn,
        slotname: &str,
        failover: Option<bool>,
        two_phase: Option<bool>,
    );

    /// `walrcv_get_backend_pid`.
    fn get_backend_pid(&self, conn: &WalReceiverConn) -> i32;

    /// `walrcv_exec`.
    fn exec(&self, conn: &WalReceiverConn, query: &str, ret_types: &[Oid]) -> WalRcvExecResult;

    /// `walrcv_disconnect`.
    fn disconnect(&self, conn: Box<WalReceiverConn>);
}

/// Result of `WalReceiver::receive` (C: data len / 0+`wait_fd` / -1).
pub enum WalRcvReceive {
    Data(Vec<u8>),     // bytes available
    WouldBlock(pgsocket), // nothing yet; wait on this fd
    Ended,             // the cluster ended the COPY
}

/// The installed walreceiver implementation (C: `WalReceiverFunctions`). A boxed
/// trait object is the runtime-pluggable module table; load-time wired once.
pub static mut WalReceiverFunctions: Option<&'static dyn WalReceiver> = None;

// The walrcv_* dispatch macros become thin forwarders over WalReceiverFunctions.

fn funcs() -> &'static dyn WalReceiver {
    unsafe { WalReceiverFunctions.expect("WalReceiverFunctions not installed") }
}

pub fn walrcv_connect(
    conninfo: &str,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: &str,
) -> Result<Box<WalReceiverConn>, String> {
    funcs().connect(conninfo, replication, logical, must_use_password, appname)
}

pub fn walrcv_check_conninfo(conninfo: &str, must_use_password: bool) {
    funcs().check_conninfo(conninfo, must_use_password)
}

pub fn walrcv_get_conninfo(conn: &WalReceiverConn) -> String {
    funcs().get_conninfo(conn)
}

pub fn walrcv_get_senderinfo(conn: &WalReceiverConn) -> (String, i32) {
    funcs().get_senderinfo(conn)
}

pub fn walrcv_identify_system(conn: &WalReceiverConn) -> (String, TimeLineID) {
    funcs().identify_system(conn)
}

pub fn walrcv_get_dbname_from_conninfo(conninfo: &str) -> Option<String> {
    funcs().get_dbname_from_conninfo(conninfo)
}

pub fn walrcv_server_version(conn: &WalReceiverConn) -> i32 {
    funcs().server_version(conn)
}

pub fn walrcv_readtimelinehistoryfile(conn: &WalReceiverConn, tli: TimeLineID) -> (String, Vec<u8>) {
    funcs().read_timeline_history_file(conn, tli)
}

pub fn walrcv_startstreaming(conn: &WalReceiverConn, options: &WalRcvStreamOptions) -> bool {
    funcs().start_streaming(conn, options)
}

pub fn walrcv_endstreaming(conn: &WalReceiverConn) -> Option<TimeLineID> {
    funcs().end_streaming(conn)
}

pub fn walrcv_receive(conn: &WalReceiverConn) -> WalRcvReceive {
    funcs().receive(conn)
}

pub fn walrcv_send(conn: &WalReceiverConn, buffer: &[u8]) {
    funcs().send(conn, buffer)
}

pub fn walrcv_create_slot(
    conn: &WalReceiverConn,
    slotname: &str,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    snapshot_action: CRSSnapshotAction,
) -> (Option<String>, XLogRecPtr) {
    funcs().create_slot(conn, slotname, temporary, two_phase, failover, snapshot_action)
}

pub fn walrcv_alter_slot(
    conn: &WalReceiverConn,
    slotname: &str,
    failover: Option<bool>,
    two_phase: Option<bool>,
) {
    funcs().alter_slot(conn, slotname, failover, two_phase)
}

pub fn walrcv_get_backend_pid(conn: &WalReceiverConn) -> i32 {
    funcs().get_backend_pid(conn)
}

pub fn walrcv_exec(conn: &WalReceiverConn, query: &str, ret_types: &[Oid]) -> WalRcvExecResult {
    funcs().exec(conn, query, ret_types)
}

pub fn walrcv_disconnect(conn: Box<WalReceiverConn>) {
    funcs().disconnect(conn)
}

/// C inline `walrcv_clear_result`: frees the result and its owned members. In
/// Rust ownership handles this on drop, so consuming the value is the port.
pub fn walrcv_clear_result(_walres: Option<WalRcvExecResult>) {}

// prototypes for functions in walreceiver.c
// C: `pg_noreturn void WalReceiverMain(const void *startup_data, size_t len)`.
pub fn WalReceiverMain(startup_data: &[u8]) -> ! {
    unimplemented!()
}

pub fn WalRcvForceReply() {
    unimplemented!()
}

// prototypes for functions in walreceiverfuncs.c
// WalRcvShmemSize/WalRcvShmemInit are shmem setup (single-process); kept as
// stubs for API parity.
pub fn WalRcvShmemSize() -> usize {
    unimplemented!()
}

pub fn WalRcvShmemInit() {
    unimplemented!()
}

pub fn ShutdownWalRcv() {
    unimplemented!()
}

pub fn WalRcvStreaming() -> bool {
    unimplemented!()
}

pub fn WalRcvRunning() -> bool {
    unimplemented!()
}

pub fn RequestXLogStreaming(
    tli: TimeLineID,
    recptr: XLogRecPtr,
    conninfo: &str,
    slotname: &str,
    create_temp_slot: bool,
) {
    unimplemented!()
}

/// C: out-params `*latestChunkStart, *receiveTLI` -> tuple alongside the return.
pub fn GetWalRcvFlushRecPtr() -> (XLogRecPtr, XLogRecPtr, TimeLineID) {
    unimplemented!()
}

pub fn GetWalRcvWriteRecPtr() -> XLogRecPtr {
    unimplemented!()
}

pub fn GetReplicationApplyDelay() -> i32 {
    unimplemented!()
}

pub fn GetReplicationTransferLatency() -> i32 {
    unimplemented!()
}
