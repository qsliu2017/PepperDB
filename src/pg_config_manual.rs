//! pg_config_manual.h - PostgreSQL manual configuration settings and limits.
//!
//! This file contains various configuration symbols and limits.  In all cases,
//! changing them is only useful in very rare situations or for developers.
//!
//! Many entries in the C header are conditional on build-time / platform macros
//! (SIZEOF_VOID_P, WIN32, HAVE_SYNC_FILE_RANGE, USE_OPENSSL, USE_ASSERT_CHECKING,
//! etc.).  Per project policy we do NOT use cfg(feature = "..."), so we emit the
//! default (non-feature) branch unconditionally for a typical 64-bit, non-Windows,
//! non-assert, OpenSSL-less build, with comments noting the original C gate.

#![allow(dead_code)]

use crate::c::Size;

/*
 * This is the default value for wal_segment_size to be used when initdb is run
 * without the --wal-segsize option.  It must be a valid segment size.
 */
pub const DEFAULT_XLOG_SEG_SIZE: Size = 16 * 1024 * 1024;

/*
 * Maximum length for identifiers (e.g. table names, column names,
 * function names).  Names actually are limited to one fewer byte than this,
 * because the length must include a trailing zero byte.
 *
 * Changing this requires an initdb.
 */
pub const NAMEDATALEN: usize = 64;

/*
 * Maximum number of arguments to a function.
 *
 * The minimum value is 8 (GIN indexes use 8-argument support functions).
 * The maximum possible value is around 600 (limited by index tuple size in
 * pg_proc's index; BLCKSZ larger than 8K would allow more).
 */
pub const FUNC_MAX_ARGS: usize = 100;

/*
 * When creating a product derived from PostgreSQL with changes that cause
 * incompatibilities for loadable modules, it is recommended to change this
 * string so that dfmgr.c can refuse to load incompatible modules with a clean
 * error message.
 */
pub const FMGR_ABI_EXTRA: &str = "PostgreSQL";

/*
 * Maximum number of columns in an index.  There is little point in making
 * this anything but a multiple of 32, because the main cost is associated
 * with index tuple header size (see access/itup.h).
 *
 * Changing this requires an initdb.
 */
pub const INDEX_MAX_KEYS: usize = 32;

/*
 * Maximum number of columns in a partition key
 */
pub const PARTITION_MAX_KEYS: usize = 32;

/*
 * Decide whether built-in 8-byte types, including float8, int8, and
 * timestamp, are passed by value.  This is on by default if sizeof(Datum) >= 8
 * (that is, on 64-bit platforms).
 *
 * C gate: #if SIZEOF_VOID_P >= 8 -> #define USE_FLOAT8_BYVAL 1
 * We target 64-bit, so define it unconditionally.
 */
pub const USE_FLOAT8_BYVAL: c_int = 1;

/*
 * MAXPGPATH: standard size of a pathname buffer in PostgreSQL (hence,
 * maximum usable pathname length is one less).
 */
pub const MAXPGPATH: usize = 1024;

/*
 * You can try changing this if you have a machine with bytes of
 * another size, but no guarantee...
 */
pub const BITS_PER_BYTE: usize = 8;

/*
 * Preferred alignment for disk I/O buffers.
 */
pub const ALIGNOF_BUFFER: usize = 32;

/*
 * EXEC_BACKEND is only defined on Windows (because there is no fork()).
 *
 * C gate: #if defined(WIN32) && !defined(__CYGWIN__) -> #define EXEC_BACKEND
 * Non-Windows default: not defined.  (No Rust symbol emitted for the macro
 * itself; cfg-gated code elsewhere should check the platform directly.)
 */
// (EXEC_BACKEND intentionally not defined for non-Windows builds.)

/*
 * USE_POSIX_FADVISE controls whether Postgres will attempt to use the
 * posix_fadvise() kernel call.
 *
 * C gate: #if HAVE_DECL_POSIX_FADVISE && defined(HAVE_POSIX_FADVISE)
 * Default (non-feature) build: not defined.
 */
// (USE_POSIX_FADVISE intentionally not defined in the default build.)

/*
 * USE_PREFETCH code should be compiled only if we have a way to implement
 * prefetching.
 *
 * C gate: #ifdef USE_POSIX_FADVISE -> #define USE_PREFETCH
 * Default build: not defined.
 */
// (USE_PREFETCH intentionally not defined in the default build.)

/*
 * Default and maximum values for backend_flush_after, bgwriter_flush_after
 * and checkpoint_flush_after; measured in blocks.  Currently, these are
 * enabled by default if sync_file_range() exists, ie, only on Linux.
 *
 * C gate: #ifdef HAVE_SYNC_FILE_RANGE.  We emit the non-Linux default branch
 * (all zero).  See the C header for the Linux values (64 and 32).
 */
pub const DEFAULT_BACKEND_FLUSH_AFTER: c_int = 0;
pub const DEFAULT_BGWRITER_FLUSH_AFTER: c_int = 0;
pub const DEFAULT_CHECKPOINT_FLUSH_AFTER: c_int = 0;
/* upper limit for all three variables */
pub const WRITEBACK_MAX_PENDING_FLUSHES: c_int = 256;

/*
 * USE_SSL code should be compiled only when compiling with an SSL
 * implementation.
 *
 * C gate: #ifdef USE_OPENSSL -> #define USE_SSL
 * Default build: not defined.
 */
// (USE_SSL intentionally not defined without an SSL implementation.)

/*
 * This is the default directory in which AF_UNIX socket files are placed.
 *
 * C gate: #ifndef WIN32 -> "/tmp"; #else -> "".  Non-Windows default below.
 */
pub const DEFAULT_PGSOCKET_DIR: &str = "/tmp";

/*
 * This is the default event source for Windows event log.
 */
pub const DEFAULT_EVENT_SOURCE: &str = "PostgreSQL";

/*
 * Assumed cache line size.  This doesn't affect correctness, but can be used
 * for low-level optimizations.
 */
pub const PG_CACHE_LINE_SIZE: usize = 128;

/*
 * Assumed alignment requirement for direct I/O.  4K corresponds to common
 * sector and memory page size.
 */
pub const PG_IO_ALIGN_SIZE: usize = 4096;

/*
 *------------------------------------------------------------------------
 * The following symbols are for enabling debugging code, not for
 * controlling user-visible features or resource limits.
 *
 * In the C header all of these are commented-out / assert-gated #defines.
 * For the default (non-assert) build none of them are active, so no Rust
 * symbols are emitted.  They are listed here for documentation / dedup.
 *------------------------------------------------------------------------
 */
// FORCE_JSON_PSTACK            - not defined (commented out in C).
// USE_VALGRIND                 - not defined (commented out in C).
// CLOBBER_FREED_MEMORY         - C gate: #ifdef USE_ASSERT_CHECKING; not defined.
// MEMORY_CONTEXT_CHECKING      - C gate: USE_ASSERT_CHECKING || USE_VALGRIND; not defined.
// RANDOMIZE_ALLOCATED_MEMORY   - not defined (commented out in C).
// DISCARD_CACHES_ENABLED       - C gate: USE_ASSERT_CHECKING; not defined.
// RECOVER_RELATION_BUILD_MEMORY- not defined (commented out in C).
// DEBUG_NODE_TESTS_ENABLED     - C gate: USE_ASSERT_CHECKING; not defined.
// REALLOCATE_BITMAPSETS        - not defined (commented out in C).
// LOCK_DEBUG                   - not defined (commented out in C).
// WAL_DEBUG                    - not defined (commented out in C).
// TRACE_SYNCSCAN               - not defined (commented out in C).

use std::ffi::c_int;
