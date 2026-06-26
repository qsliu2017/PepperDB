//! Translated from PostgreSQL src/include/libpq/protocol.h
//
// Wire-protocol request/response codes. In-memory constants only (NOT repr(C));
// these are single-byte message-type tags read/written by the protocol layer.

// Request codes sent by the frontend.
pub const PQMSG_BIND: u8 = b'B';
pub const PQMSG_CLOSE: u8 = b'C';
pub const PQMSG_DESCRIBE: u8 = b'D';
pub const PQMSG_EXECUTE: u8 = b'E';
pub const PQMSG_FUNCTION_CALL: u8 = b'F';
pub const PQMSG_FLUSH: u8 = b'H';
pub const PQMSG_PARSE: u8 = b'P';
pub const PQMSG_QUERY: u8 = b'Q';
pub const PQMSG_SYNC: u8 = b'S';
pub const PQMSG_TERMINATE: u8 = b'X';
pub const PQMSG_COPY_FAIL: u8 = b'f';
pub const PQMSG_GSS_RESPONSE: u8 = b'p';
pub const PQMSG_PASSWORD_MESSAGE: u8 = b'p';
pub const PQMSG_SASL_INITIAL_RESPONSE: u8 = b'p';
pub const PQMSG_SASL_RESPONSE: u8 = b'p';

// Response codes sent by the backend.
pub const PQMSG_PARSE_COMPLETE: u8 = b'1';
pub const PQMSG_BIND_COMPLETE: u8 = b'2';
pub const PQMSG_CLOSE_COMPLETE: u8 = b'3';
pub const PQMSG_NOTIFICATION_RESPONSE: u8 = b'A';
pub const PQMSG_COMMAND_COMPLETE: u8 = b'C';
pub const PQMSG_DATA_ROW: u8 = b'D';
pub const PQMSG_ERROR_RESPONSE: u8 = b'E';
pub const PQMSG_COPY_IN_RESPONSE: u8 = b'G';
pub const PQMSG_COPY_OUT_RESPONSE: u8 = b'H';
pub const PQMSG_EMPTY_QUERY_RESPONSE: u8 = b'I';
pub const PQMSG_BACKEND_KEY_DATA: u8 = b'K';
pub const PQMSG_NOTICE_RESPONSE: u8 = b'N';
pub const PQMSG_AUTHENTICATION_REQUEST: u8 = b'R';
pub const PQMSG_PARAMETER_STATUS: u8 = b'S';
pub const PQMSG_ROW_DESCRIPTION: u8 = b'T';
pub const PQMSG_FUNCTION_CALL_RESPONSE: u8 = b'V';
pub const PQMSG_COPY_BOTH_RESPONSE: u8 = b'W';
pub const PQMSG_READY_FOR_QUERY: u8 = b'Z';
pub const PQMSG_NO_DATA: u8 = b'n';
pub const PQMSG_PORTAL_SUSPENDED: u8 = b's';
pub const PQMSG_PARAMETER_DESCRIPTION: u8 = b't';
pub const PQMSG_NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';

// Codes sent by both the frontend and backend.
pub const PQMSG_COPY_DONE: u8 = b'c';
pub const PQMSG_COPY_DATA: u8 = b'd';

// Code sent by parallel workers to leader processes.
pub const PQMSG_PROGRESS: u8 = b'P';

// Authentication request codes sent by the backend.
// (6 is available: it was used for SCM creds, not supported any more.)
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthRequest {
    Ok = 0,        // User is authenticated
    Krb4 = 1,      // Kerberos V4. Not supported any more.
    Krb5 = 2,      // Kerberos V5. Not supported any more.
    Password = 3,  // Password
    Crypt = 4,     // crypt password. Not supported any more.
    Md5 = 5,       // md5 password
    Gss = 7,       // GSSAPI without wrap()
    GssCont = 8,   // Continue GSS exchanges
    Sspi = 9,      // SSPI negotiate without wrap()
    Sasl = 10,     // Begin SASL authentication
    SaslCont = 11, // Continue SASL authentication
    SaslFin = 12,  // Final SASL message
}

impl AuthRequest {
    /// Maximum AUTH_REQ_* value.
    pub const MAX: Self = Self::SaslFin;
}
