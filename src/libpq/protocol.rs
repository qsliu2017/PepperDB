//! libpq/protocol.h - Definitions of the request/response codes for the wire protocol.

use std::ffi::c_int;

/* These are the request codes sent by the frontend. */

pub const PqMsg_Bind: u8 = b'B';
pub const PqMsg_Close: u8 = b'C';
pub const PqMsg_Describe: u8 = b'D';
pub const PqMsg_Execute: u8 = b'E';
pub const PqMsg_FunctionCall: u8 = b'F';
pub const PqMsg_Flush: u8 = b'H';
pub const PqMsg_Parse: u8 = b'P';
pub const PqMsg_Query: u8 = b'Q';
pub const PqMsg_Sync: u8 = b'S';
pub const PqMsg_Terminate: u8 = b'X';
pub const PqMsg_CopyFail: u8 = b'f';
pub const PqMsg_GSSResponse: u8 = b'p';
pub const PqMsg_PasswordMessage: u8 = b'p';
pub const PqMsg_SASLInitialResponse: u8 = b'p';
pub const PqMsg_SASLResponse: u8 = b'p';

/* These are the response codes sent by the backend. */

pub const PqMsg_ParseComplete: u8 = b'1';
pub const PqMsg_BindComplete: u8 = b'2';
pub const PqMsg_CloseComplete: u8 = b'3';
pub const PqMsg_NotificationResponse: u8 = b'A';
pub const PqMsg_CommandComplete: u8 = b'C';
pub const PqMsg_DataRow: u8 = b'D';
pub const PqMsg_ErrorResponse: u8 = b'E';
pub const PqMsg_CopyInResponse: u8 = b'G';
pub const PqMsg_CopyOutResponse: u8 = b'H';
pub const PqMsg_EmptyQueryResponse: u8 = b'I';
pub const PqMsg_BackendKeyData: u8 = b'K';
pub const PqMsg_NoticeResponse: u8 = b'N';
pub const PqMsg_AuthenticationRequest: u8 = b'R';
pub const PqMsg_ParameterStatus: u8 = b'S';
pub const PqMsg_RowDescription: u8 = b'T';
pub const PqMsg_FunctionCallResponse: u8 = b'V';
pub const PqMsg_CopyBothResponse: u8 = b'W';
pub const PqMsg_ReadyForQuery: u8 = b'Z';
pub const PqMsg_NoData: u8 = b'n';
pub const PqMsg_PortalSuspended: u8 = b's';
pub const PqMsg_ParameterDescription: u8 = b't';
pub const PqMsg_NegotiateProtocolVersion: u8 = b'v';

/* These are the codes sent by both the frontend and backend. */

pub const PqMsg_CopyDone: u8 = b'c';
pub const PqMsg_CopyData: u8 = b'd';

/* These are the codes sent by parallel workers to leader processes. */

pub const PqMsg_Progress: u8 = b'P';

/* These are the authentication request codes sent by the backend. */

pub const AUTH_REQ_OK: c_int = 0; /* User is authenticated  */
pub const AUTH_REQ_KRB4: c_int = 1; /* Kerberos V4. Not supported any more. */
pub const AUTH_REQ_KRB5: c_int = 2; /* Kerberos V5. Not supported any more. */
pub const AUTH_REQ_PASSWORD: c_int = 3; /* Password */
pub const AUTH_REQ_CRYPT: c_int = 4; /* crypt password. Not supported any more. */
pub const AUTH_REQ_MD5: c_int = 5; /* md5 password */
/* 6 is available.  It was used for SCM creds, not supported any more. */
pub const AUTH_REQ_GSS: c_int = 7; /* GSSAPI without wrap() */
pub const AUTH_REQ_GSS_CONT: c_int = 8; /* Continue GSS exchanges */
pub const AUTH_REQ_SSPI: c_int = 9; /* SSPI negotiate without wrap() */
pub const AUTH_REQ_SASL: c_int = 10; /* Begin SASL authentication */
pub const AUTH_REQ_SASL_CONT: c_int = 11; /* Continue SASL authentication */
pub const AUTH_REQ_SASL_FIN: c_int = 12; /* Final SASL message */
pub const AUTH_REQ_MAX: c_int = AUTH_REQ_SASL_FIN; /* maximum AUTH_REQ_* value */
