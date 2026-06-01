//! access/rmgrlist.h - the master list of WAL resource managers (X-macro database).
//!
//! In C this header is a pure X-macro "list" file with deliberately no include
//! guard.  It contains a sequence of `PG_RMGR(symname, name, redo, desc,
//! identify, startup, cleanup, mask, decode)` invocations; the caller defines
//! the `PG_RMGR` macro before `#include`-ing this file to extract whatever
//! projection it needs (e.g. rmgr.h builds the `RmgrIds` enum from `symname`,
//! and rmgr.c builds the `RmgrTable[]` array of callbacks).
//!
//! Rust has no textual-include / X-macro mechanism, so this module materializes
//! the two projections that the rest of the tree actually consumes:
//!   * the `RM_*_ID` integer constants (their values are defined by the *order*
//!     of the entries below - new entries must be appended, never inserted),
//!   * a `RmgrListEntry` const table carrying the textual name and the C
//!     identifier names of each rmgr's callbacks, for tooling/desc use.
//!
//! The actual callback function pointers live in rmgr.c (RmgrTable[]); that is a
//! .c file, not a header, and is not part of this header translation.  Here we
//! only record the *names* of the callbacks as &str so automatic tools can
//! reproduce the C list verbatim.
//!
//! NOTE: changes to this list possibly need an XLOG_PAGE_MAGIC bump (per the C
//! header).

use crate::c::uint8;

/// rmgr.h: resource manager ID.  Re-declared locally to keep this module
/// self-contained; the canonical definition is `crate::access::transam::xlogreader::RmgrId`.
// TODO: dedup RmgrId with access/transam/xlogreader.rs (mirror of rmgr.h's `typedef uint8 RmgrId`).
pub type RmgrId = uint8;

//
// Resource manager IDs.
//
// These mirror the `RmgrIds` enum that rmgr.h builds out of this list via the
// `PG_RMGR(symname, ...)` X-macro: each `symname` becomes the next consecutive
// enum value, starting at 0.  The numerical values are stored in WAL records,
// so the ordering here is part of the on-disk format and must not change.
//

/// XLOG resource manager (id 0).
pub const RM_XLOG_ID: RmgrId = 0;
/// Transaction (commit/abort) resource manager (id 1).
pub const RM_XACT_ID: RmgrId = 1;
/// Storage manager resource manager (id 2).
pub const RM_SMGR_ID: RmgrId = 2;
/// CLOG resource manager (id 3).
pub const RM_CLOG_ID: RmgrId = 3;
/// Database resource manager (id 4).
pub const RM_DBASE_ID: RmgrId = 4;
/// Tablespace resource manager (id 5).
pub const RM_TBLSPC_ID: RmgrId = 5;
/// MultiXact resource manager (id 6).
pub const RM_MULTIXACT_ID: RmgrId = 6;
/// RelMap resource manager (id 7).
pub const RM_RELMAP_ID: RmgrId = 7;
/// Standby resource manager (id 8).
pub const RM_STANDBY_ID: RmgrId = 8;
/// Heap2 resource manager (id 9).
pub const RM_HEAP2_ID: RmgrId = 9;
/// Heap resource manager (id 10).
pub const RM_HEAP_ID: RmgrId = 10;
/// B-tree resource manager (id 11).
pub const RM_BTREE_ID: RmgrId = 11;
/// Hash resource manager (id 12).
pub const RM_HASH_ID: RmgrId = 12;
/// GIN resource manager (id 13).
pub const RM_GIN_ID: RmgrId = 13;
/// GiST resource manager (id 14).
pub const RM_GIST_ID: RmgrId = 14;
/// Sequence resource manager (id 15).
pub const RM_SEQ_ID: RmgrId = 15;
/// SP-GiST resource manager (id 16).
pub const RM_SPGIST_ID: RmgrId = 16;
/// BRIN resource manager (id 17).
pub const RM_BRIN_ID: RmgrId = 17;
/// Commit timestamp resource manager (id 18).
pub const RM_COMMIT_TS_ID: RmgrId = 18;
/// Replication origin resource manager (id 19).
pub const RM_REPLORIGIN_ID: RmgrId = 19;
/// Generic (extension page) resource manager (id 20).
pub const RM_GENERIC_ID: RmgrId = 20;
/// Logical decoding message resource manager (id 21).
pub const RM_LOGICALMSG_ID: RmgrId = 21;

/// One past the last builtin RmgrId (the `RM_NEXT_ID` of rmgr.h's `RmgrIds`
/// enum, formed from the entry that follows the last `PG_RMGR` line).  With the
/// 22 entries above, the next id is 22.
///
/// NOTE: PostgreSQL 18.3's rmgrlist.h ends at `RM_LOGICALMSG_ID`, so this list
/// has 22 builtin entries and `RM_NEXT_ID == 22`.  (access/transam/xlogreader.rs
/// and xlogstats.rs use a placeholder of 25/24 because rmgr.h itself was not yet
/// ported there; the authoritative count is the length of *this* list.)
// TODO: reconcile RM_NEXT_ID (22 here) with the 25 placeholder in
// access/transam/xlogstats.rs / xlogreader.rs once rmgr.h is wired up.
pub const RM_NEXT_ID: RmgrId = 22;

/// A single row of the C `rmgrlist.h` X-macro table: the arguments of one
/// `PG_RMGR(symname, name, redo, desc, identify, startup, cleanup, mask, decode)`.
///
/// `redo`/`desc`/`identify`/`startup`/`cleanup`/`mask`/`decode` hold the *names*
/// of the C callback identifiers (or `None` where the C list passed `NULL`);
/// the function pointers themselves are assembled in rmgr.c, not in this header.
#[derive(Clone, Copy, Debug)]
pub struct RmgrListEntry {
    /// The `RM_*_ID` symbol name (e.g. "RM_XLOG_ID").
    pub symname: &'static str,
    /// Human-readable name (e.g. "XLOG"), stored in WAL dumps.
    pub name: &'static str,
    /// The numeric RmgrId, equal to the entry's position in this list.
    pub id: RmgrId,
    /// rm_redo callback name.
    pub redo: &'static str,
    /// rm_desc callback name.
    pub desc: &'static str,
    /// rm_identify callback name.
    pub identify: &'static str,
    /// rm_startup callback name, or None for NULL.
    pub startup: Option<&'static str>,
    /// rm_cleanup callback name, or None for NULL.
    pub cleanup: Option<&'static str>,
    /// rm_mask callback name, or None for NULL.
    pub mask: Option<&'static str>,
    /// rm_decode callback name, or None for NULL.
    pub decode: Option<&'static str>,
}

/// The resource manager list, verbatim from access/rmgrlist.h.  Indexed by
/// RmgrId (each entry's `id` equals its index).  This is the Rust projection of
/// the X-macro sequence; rmgr.c's `RmgrTable[]` is built from the same data.
pub static RMGR_LIST: [RmgrListEntry; RM_NEXT_ID as usize] = [
    // symname, name, redo, desc, identify, startup, cleanup, mask, decode
    RmgrListEntry { symname: "RM_XLOG_ID", name: "XLOG", id: RM_XLOG_ID,
        redo: "xlog_redo", desc: "xlog_desc", identify: "xlog_identify",
        startup: None, cleanup: None, mask: None, decode: Some("xlog_decode") },
    RmgrListEntry { symname: "RM_XACT_ID", name: "Transaction", id: RM_XACT_ID,
        redo: "xact_redo", desc: "xact_desc", identify: "xact_identify",
        startup: None, cleanup: None, mask: None, decode: Some("xact_decode") },
    RmgrListEntry { symname: "RM_SMGR_ID", name: "Storage", id: RM_SMGR_ID,
        redo: "smgr_redo", desc: "smgr_desc", identify: "smgr_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_CLOG_ID", name: "CLOG", id: RM_CLOG_ID,
        redo: "clog_redo", desc: "clog_desc", identify: "clog_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_DBASE_ID", name: "Database", id: RM_DBASE_ID,
        redo: "dbase_redo", desc: "dbase_desc", identify: "dbase_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_TBLSPC_ID", name: "Tablespace", id: RM_TBLSPC_ID,
        redo: "tblspc_redo", desc: "tblspc_desc", identify: "tblspc_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_MULTIXACT_ID", name: "MultiXact", id: RM_MULTIXACT_ID,
        redo: "multixact_redo", desc: "multixact_desc", identify: "multixact_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_RELMAP_ID", name: "RelMap", id: RM_RELMAP_ID,
        redo: "relmap_redo", desc: "relmap_desc", identify: "relmap_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_STANDBY_ID", name: "Standby", id: RM_STANDBY_ID,
        redo: "standby_redo", desc: "standby_desc", identify: "standby_identify",
        startup: None, cleanup: None, mask: None, decode: Some("standby_decode") },
    RmgrListEntry { symname: "RM_HEAP2_ID", name: "Heap2", id: RM_HEAP2_ID,
        redo: "heap2_redo", desc: "heap2_desc", identify: "heap2_identify",
        startup: None, cleanup: None, mask: Some("heap_mask"), decode: Some("heap2_decode") },
    RmgrListEntry { symname: "RM_HEAP_ID", name: "Heap", id: RM_HEAP_ID,
        redo: "heap_redo", desc: "heap_desc", identify: "heap_identify",
        startup: None, cleanup: None, mask: Some("heap_mask"), decode: Some("heap_decode") },
    RmgrListEntry { symname: "RM_BTREE_ID", name: "Btree", id: RM_BTREE_ID,
        redo: "btree_redo", desc: "btree_desc", identify: "btree_identify",
        startup: Some("btree_xlog_startup"), cleanup: Some("btree_xlog_cleanup"),
        mask: Some("btree_mask"), decode: None },
    RmgrListEntry { symname: "RM_HASH_ID", name: "Hash", id: RM_HASH_ID,
        redo: "hash_redo", desc: "hash_desc", identify: "hash_identify",
        startup: None, cleanup: None, mask: Some("hash_mask"), decode: None },
    RmgrListEntry { symname: "RM_GIN_ID", name: "Gin", id: RM_GIN_ID,
        redo: "gin_redo", desc: "gin_desc", identify: "gin_identify",
        startup: Some("gin_xlog_startup"), cleanup: Some("gin_xlog_cleanup"),
        mask: Some("gin_mask"), decode: None },
    RmgrListEntry { symname: "RM_GIST_ID", name: "Gist", id: RM_GIST_ID,
        redo: "gist_redo", desc: "gist_desc", identify: "gist_identify",
        startup: Some("gist_xlog_startup"), cleanup: Some("gist_xlog_cleanup"),
        mask: Some("gist_mask"), decode: None },
    RmgrListEntry { symname: "RM_SEQ_ID", name: "Sequence", id: RM_SEQ_ID,
        redo: "seq_redo", desc: "seq_desc", identify: "seq_identify",
        startup: None, cleanup: None, mask: Some("seq_mask"), decode: None },
    RmgrListEntry { symname: "RM_SPGIST_ID", name: "SPGist", id: RM_SPGIST_ID,
        redo: "spg_redo", desc: "spg_desc", identify: "spg_identify",
        startup: Some("spg_xlog_startup"), cleanup: Some("spg_xlog_cleanup"),
        mask: Some("spg_mask"), decode: None },
    RmgrListEntry { symname: "RM_BRIN_ID", name: "BRIN", id: RM_BRIN_ID,
        redo: "brin_redo", desc: "brin_desc", identify: "brin_identify",
        startup: None, cleanup: None, mask: Some("brin_mask"), decode: None },
    RmgrListEntry { symname: "RM_COMMIT_TS_ID", name: "CommitTs", id: RM_COMMIT_TS_ID,
        redo: "commit_ts_redo", desc: "commit_ts_desc", identify: "commit_ts_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_REPLORIGIN_ID", name: "ReplicationOrigin", id: RM_REPLORIGIN_ID,
        redo: "replorigin_redo", desc: "replorigin_desc", identify: "replorigin_identify",
        startup: None, cleanup: None, mask: None, decode: None },
    RmgrListEntry { symname: "RM_GENERIC_ID", name: "Generic", id: RM_GENERIC_ID,
        redo: "generic_redo", desc: "generic_desc", identify: "generic_identify",
        startup: None, cleanup: None, mask: Some("generic_mask"), decode: None },
    RmgrListEntry { symname: "RM_LOGICALMSG_ID", name: "LogicalMessage", id: RM_LOGICALMSG_ID,
        redo: "logicalmsg_redo", desc: "logicalmsg_desc", identify: "logicalmsg_identify",
        startup: None, cleanup: None, mask: None, decode: Some("logicalmsg_decode") },
];
