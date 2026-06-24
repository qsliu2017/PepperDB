//! Translated from PostgreSQL src/include/utils/timeout.h
//!
//! Multiplexes SIGALRM for multiple timeout reasons. Under the async model the
//! SIGALRM mechanism becomes timers, but the API/identifiers translate directly.

use crate::datatype::timestamp::TimestampTz;

/// Timeout reasons. On simultaneous fire they are serviced in declaration order.
/// USER_TIMEOUT marks the first user-definable id; reasons up to MAX_TIMEOUTS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TimeoutId {
    StartupPacketTimeout = 0,
    DeadlockTimeout,
    LockTimeout,
    StatementTimeout,
    StandbyDeadlockTimeout,
    StandbyTimeout,
    StandbyLockTimeout,
    IdleInTransactionSessionTimeout,
    TransactionTimeout,
    IdleSessionTimeout,
    IdleStatsUpdateTimeout,
    ClientConnectionCheckTimeout,
    StartupProgressTimeout,
    UserTimeout,
}

/// First user-definable timeout reason.
pub const USER_TIMEOUT: i32 = TimeoutId::UserTimeout as i32;
/// Maximum number of timeout reasons.
pub const MAX_TIMEOUTS: i32 = USER_TIMEOUT + 10;

/// Callback signature (`void (*)(void)`). The threaded state is captured later.
pub type timeout_handler_proc = fn();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutType {
    After,
    At,
    Every,
}

/// Parameters for setting multiple timeouts at once.
pub struct EnableTimeoutParams {
    pub id: TimeoutId,
    pub r#type: TimeoutType,
    pub delay_ms: i32,        // only for After/Every
    pub fin_time: TimestampTz, // only for At
}

/// Parameters for clearing multiple timeouts at once.
pub struct DisableTimeoutParams {
    pub id: TimeoutId,
    pub keep_indicator: bool,
}

// timeout setup
pub fn InitializeTimeouts() {
    unimplemented!()
}
pub fn RegisterTimeout(id: TimeoutId, handler: timeout_handler_proc) -> TimeoutId {
    unimplemented!()
}
pub fn reschedule_timeouts() {
    unimplemented!()
}

// timeout operation
pub fn enable_timeout_after(id: TimeoutId, delay_ms: i32) {
    unimplemented!()
}
pub fn enable_timeout_every(id: TimeoutId, fin_time: TimestampTz, delay_ms: i32) {
    unimplemented!()
}
pub fn enable_timeout_at(id: TimeoutId, fin_time: TimestampTz) {
    unimplemented!()
}
pub fn enable_timeouts(timeouts: &[EnableTimeoutParams]) {
    unimplemented!()
}
pub fn disable_timeout(id: TimeoutId, keep_indicator: bool) {
    unimplemented!()
}
pub fn disable_timeouts(timeouts: &[DisableTimeoutParams]) {
    unimplemented!()
}
pub fn disable_all_timeouts(keep_indicators: bool) {
    unimplemented!()
}

// accessors
pub fn get_timeout_active(id: TimeoutId) -> bool {
    unimplemented!()
}
pub fn get_timeout_indicator(id: TimeoutId, reset_indicator: bool) -> bool {
    unimplemented!()
}
pub fn get_timeout_start_time(id: TimeoutId) -> TimestampTz {
    unimplemented!()
}
pub fn get_timeout_finish_time(id: TimeoutId) -> TimestampTz {
    unimplemented!()
}
