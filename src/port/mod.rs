//! Portability layer (postgres/src/port + postgres/src/include/port).
//!
//! Replacements and helpers for routines whose availability or behavior varies
//! across platforms.

pub mod sysv_shmem;
pub mod sysv_sema;
pub mod atomics;
pub mod bsearch_arg;
pub mod cygwin;
pub mod darwin;
pub mod explicit_bzero;
pub mod freebsd;
pub mod getopt;
pub mod linux;
pub mod netbsd;
pub mod openbsd;
pub mod pg_iovec;
pub mod pg_lfind;
pub mod pg_numa;
pub mod posix_sema;
pub mod pg_pthread;
pub mod port_api;
pub mod simd;
pub mod solaris;
pub mod win32_port;
pub mod win32ntdll;
pub mod getopt_long;
pub mod getpeereid;
pub mod inet_aton;
pub mod inet_net_ntop;
pub mod noblock;
pub mod path;
pub mod pg_bitutils;
pub mod pg_bswap;
pub mod pg_crc32c;
pub mod pg_strong_random;
pub mod pgcheckdir;
pub mod pgmkdirp;
pub mod pgsleep;
pub mod pgstrcasecmp;
pub mod pthread_barrier_wait;
pub mod qsort;
pub mod quotes;
pub mod strerror;
pub mod strtof;
pub mod strlcat;
pub mod strlcpy;
pub mod strnlen;
pub mod strsep;
pub mod tar;
pub mod timingsafe_bcmp;

// Windows-MSVC POSIX-compatibility shim trees: faithful translations that are
// only meaningful on a Windows build. Gated off on all other targets so their
// Windows-system-type references don't affect the (non-Windows) build.
#[cfg(windows)]
pub mod win32;
#[cfg(windows)]
pub mod win32_msvc;
pub mod atomics_backend;
pub mod pgstrsignal;
