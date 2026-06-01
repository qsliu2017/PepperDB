//! utils/wait_classes.h - Definitions related to wait event classes

use crate::c::uint32;

// ----------
// Wait Classes
// ----------
pub const PG_WAIT_LWLOCK: uint32 = 0x01000000;
pub const PG_WAIT_LOCK: uint32 = 0x03000000;
pub const PG_WAIT_BUFFERPIN: uint32 = 0x04000000;
pub const PG_WAIT_ACTIVITY: uint32 = 0x05000000;
pub const PG_WAIT_CLIENT: uint32 = 0x06000000;
pub const PG_WAIT_EXTENSION: uint32 = 0x07000000;
pub const PG_WAIT_IPC: uint32 = 0x08000000;
pub const PG_WAIT_TIMEOUT: uint32 = 0x09000000;
pub const PG_WAIT_IO: uint32 = 0x0A000000;
pub const PG_WAIT_INJECTIONPOINT: uint32 = 0x0B000000;
