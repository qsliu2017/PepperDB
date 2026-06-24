//! Translated from PostgreSQL src/include/utils/wait_classes.h
//
// Wait-event class codes. These are mutually-exclusive class selectors packed into
// the high byte of a wait-event word (e.g. 0x03/0x05 are not powers of two), NOT an
// OR-able flag set -- so plain u32 consts, kept byte-exact (they are protocol/stats
// visible in pg_stat_activity.wait_event_type).

pub const PG_WAIT_LWLOCK: u32 = 0x0100_0000;
pub const PG_WAIT_LOCK: u32 = 0x0300_0000;
pub const PG_WAIT_BUFFERPIN: u32 = 0x0400_0000;
pub const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
pub const PG_WAIT_CLIENT: u32 = 0x0600_0000;
pub const PG_WAIT_EXTENSION: u32 = 0x0700_0000;
pub const PG_WAIT_IPC: u32 = 0x0800_0000;
pub const PG_WAIT_TIMEOUT: u32 = 0x0900_0000;
pub const PG_WAIT_IO: u32 = 0x0A00_0000;
pub const PG_WAIT_INJECTIONPOINT: u32 = 0x0B00_0000;
