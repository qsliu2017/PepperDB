//! libpq/libpq-fs.h - definitions for using Inversion file system routines (ie, large objects)

/*
 *	Read/write mode flags for inversion (large object) calls
 */

pub const INV_WRITE: i32 = 0x00020000;
pub const INV_READ: i32 = 0x00040000;
