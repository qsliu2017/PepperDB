//! Translated from PostgreSQL src/include/tcop/backend_startup.h
//
// Prototypes for backend_startup.c. In-memory types. log_connections is a flag
// word -> bitflags (LogConnectionOption); the rest is plain state.

use crate::datatype::timestamp::TimestampTz;

use bitflags::bitflags;

// GUCs. TODO(global): move into a Session/server-config context.
pub static mut Trace_connection_negotiation: bool = false;
pub static mut log_connections: u32 = 0;
pub static mut log_connections_string: Option<String> = None;

// Other globals.
pub static mut conn_timing: Option<ConnectionTiming> = None;

/// Passed from postmaster to backend: whether to accept the connection or just
/// send an error and close. Sequential ordinals -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CAC_state {
    OK,
    STARTUP,
    SHUTDOWN,
    RECOVERY,
    NOTHOTSTANDBY,
    TOOMANY,
}

/// Information passed from postmaster to backend in 'startup_data'.
#[derive(Debug, Clone, Copy)]
pub struct BackendStartupData {
    pub canAcceptConnections: CAC_state,
    /// When the client socket is created (client/wal-sender connections only).
    pub socket_created: TimestampTz,
    /// When the postmaster initiates process creation (client/wal-sender only).
    pub fork_started: TimestampTz,
}

bitflags! {
    /// Granular control over which messages to log for the log_connections GUC.
    /// Single-bit aspects plus composite aliases ON/ALL.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct LogConnectionOption: u32 {
        const RECEIPT         = 1 << 0;
        const AUTHENTICATION  = 1 << 1;
        const AUTHORIZATION   = 1 << 2;
        const SETUP_DURATIONS = 1 << 3;
        // Backwards-compat alias for the aspects logged in PG < 18.
        const ON  = Self::RECEIPT.bits() | Self::AUTHENTICATION.bits() | Self::AUTHORIZATION.bits();
        const ALL = Self::RECEIPT.bits()
            | Self::AUTHENTICATION.bits()
            | Self::AUTHORIZATION.bits()
            | Self::SETUP_DURATIONS.bits();
    }
}

/// Timings of connection establishment/setup stages, for the setup_durations log.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionTiming {
    pub socket_create: TimestampTz,
    pub ready_for_use: TimestampTz,
    pub fork_start: TimestampTz,
    pub fork_end: TimestampTz,
    pub auth_start: TimestampTz,
    pub auth_end: TimestampTz,
}

/// pg_noreturn in C. TODO(panic).
pub fn BackendMain(_startup_data: &[u8]) -> ! {
    unimplemented!()
}
