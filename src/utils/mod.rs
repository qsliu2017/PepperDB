//! Backend `utils` subsystem (postgres/src/backend/utils, postgres/src/include/utils).
//!
//! Only the foundational pieces needed by leaf modules are present so far:
//! the memory allocator interface (`palloc`/`pfree`), error reporting
//! (`elog`/`ereport`), and allocation limits (`memutils`).

pub mod aclchk_internal;
pub mod activity;
pub mod adt;
pub mod array;
pub mod arrayaccess;
pub mod builtins;
pub mod bytea;
pub mod cache;
pub mod elog;
pub mod error;
pub mod xid8;
pub mod fmgr;
pub mod fmgrtab;
pub mod fmgrtab_gen;
pub mod geo_decls;
pub mod guc_hooks;
pub mod hash;
pub mod index_selfuncs;
pub mod init;
pub mod mb;
pub mod memutils;
pub mod misc;
pub mod mmgr;
pub mod palloc;
pub mod pgstat_kind;
pub mod pidfile;
pub mod portal;
pub mod rel;
pub mod relptr;
pub mod reltrigger;
pub mod resowner;
pub mod snapshot;
pub mod sort;
pub mod time;
pub mod wait_classes;
