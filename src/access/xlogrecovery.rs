//! Translated from PostgreSQL src/include/access/xlogrecovery.h

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::catalog::pg_control::ControlFileData;
use crate::datatype::timestamp::TimestampTz;
use crate::lib::stringinfo::StringInfo;

/// Recovery target type. Only set during PITR, not in standby mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryTargetType {
    Unset,
    Xid,
    Time,
    Name,
    Lsn,
    Immediate,
}

/// Recovery target TimeLine goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryTargetTimeLineGoal {
    ControlFile,
    Latest,
    Numeric,
}

/// Recovery pause states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryPauseState {
    NotPaused,       // pause not requested
    PauseRequested,  // pause requested, but not yet paused
    Paused,          // recovery is paused
}

// User-settable GUC parameters + GUC-derived state.
// TODO(global-state): move to a threaded Session/recovery context.
pub static mut recoveryTargetInclusive: bool = false;
pub static mut recoveryTargetAction: i32 = 0;
pub static mut recovery_min_apply_delay: i32 = 0;
pub static mut PrimaryConnInfo: Option<String> = None;
pub static mut PrimarySlotName: Option<String> = None;
pub static mut recoveryRestoreCommand: Option<String> = None;
pub static mut recoveryEndCommand: Option<String> = None;
pub static mut archiveCleanupCommand: Option<String> = None;
pub static mut recoveryTargetXid: TransactionId = TransactionId(0);
pub static mut recovery_target_time_string: Option<String> = None;
pub static mut recoveryTargetTime: TimestampTz = 0;
pub static mut recoveryTargetName: Option<String> = None;
pub static mut recoveryTargetLSN: XLogRecPtr = XLogRecPtr(0);
pub static mut recoveryTarget: RecoveryTargetType = RecoveryTargetType::Unset;
pub static mut wal_receiver_create_temp_slot: bool = false;
pub static mut recoveryTargetTimeLineGoal: RecoveryTargetTimeLineGoal =
    RecoveryTargetTimeLineGoal::ControlFile;
pub static mut recoveryTargetTLIRequested: TimeLineID = TimeLineID(0);
pub static mut recoveryTargetTLI: TimeLineID = TimeLineID(0);
pub static mut reachedConsistency: bool = false;
pub static mut StandbyMode: bool = false;

pub fn XLogRecoveryShmemSize() -> usize {
    unimplemented!()
}
pub fn XLogRecoveryShmemInit() {
    unimplemented!()
}

/// Result of InitWalRecovery (C out-params folded into a struct).
pub struct InitWalRecoveryResult {
    pub wasShutdown: bool,
    pub haveBackupLabel: bool,
    pub haveTblspcMap: bool,
}
pub fn InitWalRecovery(_ControlFile: &mut ControlFileData) -> InitWalRecoveryResult {
    unimplemented!()
}
pub fn PerformWalRecovery() {
    unimplemented!()
}

/// Where/why recovery ended (returned by FinishWalRecovery).
pub struct EndOfWalRecoveryInfo {
    pub lastRec: XLogRecPtr,   // start of last valid or applied record
    pub lastRecTLI: TimeLineID,
    pub endOfLog: XLogRecPtr,  // end of last valid or applied record
    pub endOfLogTLI: TimeLineID,
    pub lastPageBeginPtr: XLogRecPtr, // LSN of page that contains endOfLog
    pub lastPage: Option<Vec<u8>>,    // copy of last page up to endOfLog (None at boundary)
    pub abortedRecPtr: XLogRecPtr,
    pub missingContrecPtr: XLogRecPtr,
    pub recoveryStopReason: String,
    pub standby_signal_file_found: bool,
    pub recovery_signal_file_found: bool,
}

pub fn FinishWalRecovery() -> EndOfWalRecoveryInfo {
    unimplemented!()
}
pub fn ShutdownWalRecovery() {
    unimplemented!()
}
pub fn RemovePromoteSignalFiles() {
    unimplemented!()
}

pub fn HotStandbyActive() -> bool {
    unimplemented!()
}
/// Returns the replay rec ptr plus its TLI (out-param folded into a tuple).
pub fn GetXLogReplayRecPtr() -> (XLogRecPtr, TimeLineID) {
    unimplemented!()
}
pub fn GetRecoveryPauseState() -> RecoveryPauseState {
    unimplemented!()
}
pub fn SetRecoveryPause(_recoveryPause: bool) {
    unimplemented!()
}
/// Returns (receipt time, from_stream) (out-params folded into a tuple).
pub fn GetXLogReceiptTime() -> (TimestampTz, bool) {
    unimplemented!()
}
pub fn GetLatestXTime() -> TimestampTz {
    unimplemented!()
}
pub fn GetCurrentChunkReplayStartTime() -> TimestampTz {
    unimplemented!()
}
/// Returns the current replay rec ptr plus the replay-end TLI (out-param tuple).
pub fn GetCurrentReplayRecPtr() -> (XLogRecPtr, TimeLineID) {
    unimplemented!()
}

pub fn PromoteIsTriggered() -> bool {
    unimplemented!()
}
pub fn CheckPromoteSignal() -> bool {
    unimplemented!()
}
pub fn WakeupRecovery() {
    unimplemented!()
}

pub fn StartupRequestWalReceiverRestart() {
    unimplemented!()
}
pub fn XLogRequestWalReceiverReply() {
    unimplemented!()
}

pub fn RecoveryRequiresIntParameter(_param_name: &str, _currValue: i32, _minValue: i32) {
    unimplemented!()
}

pub fn xlog_outdesc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
