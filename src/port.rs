//! Translated from PostgreSQL src/include/port.h

// === scaffold: child modules (Phase 0) ===
#[path = "port/atomics.rs"]
pub mod atomics;
#[path = "port/cygwin.rs"]
pub mod cygwin;
#[path = "port/darwin.rs"]
pub mod darwin;
#[path = "port/freebsd.rs"]
pub mod freebsd;
#[path = "port/linux.rs"]
pub mod linux;
#[path = "port/netbsd.rs"]
pub mod netbsd;
#[path = "port/openbsd.rs"]
pub mod openbsd;
#[path = "port/pg_bitutils.rs"]
pub mod pg_bitutils;
#[path = "port/pg_bswap.rs"]
pub mod pg_bswap;
#[path = "port/pg_crc32c.rs"]
pub mod pg_crc32c;
#[path = "port/pg_iovec.rs"]
pub mod pg_iovec;
#[path = "port/pg_lfind.rs"]
pub mod pg_lfind;
#[path = "port/pg_numa.rs"]
pub mod pg_numa;
#[path = "port/pg_pthread.rs"]
pub mod pg_pthread;
#[path = "port/simd.rs"]
pub mod simd;
#[path = "port/solaris.rs"]
pub mod solaris;
#[path = "port/win32.rs"]
pub mod win32;
#[path = "port/win32_msvc/mod.rs"]
pub mod win32_msvc;
#[path = "port/win32_port.rs"]
pub mod win32_port;
#[path = "port/win32ntdll.rs"]
pub mod win32ntdll;
// === end scaffold ===
