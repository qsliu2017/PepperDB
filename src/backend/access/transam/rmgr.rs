//! Resource manager definitions and lookup. Translated from backend/access/transam/rmgr.c.
//!
//! Every WAL record carries an `RmgrId` in its `xl_rmid` field naming the resource
//! manager that owns it. PostgreSQL keeps a fixed `RmgrTable` array of `RmgrData`
//! entries - structs of function pointers (`rm_redo`, `rm_desc`, `rm_identify`,
//! `rm_startup`, `rm_cleanup`, ...) - indexed by that id. Recovery, `pg_waldump`,
//! and logical decoding look up an rmgr by id to apply, describe, or decode a
//! record. The builtin set is declared by the `PG_RMGR(...)` list in
//! `access/rmgrlist.h`; ids beyond the builtins are reserved for custom resource
//! managers loaded from `shared_preload_libraries`. `RmgrStartup`/`RmgrCleanup`
//! iterate the table to drive per-rmgr recovery hooks, and `RmgrNotFound` reports
//! an error when a record names an unregistered id.
//!
//! Each entry exposes its method handlers: `redo` applies a decoded record during
//! recovery, `desc` renders a human-readable description, `identify` names the
//! record type encoded in the rmgr-owned bits of `xl_info`, and the optional
//! `startup`/`cleanup` callbacks bracket recovery for managers that need them.
//!
//! Here the table of function pointers becomes the [`Rmgr`] trait plus a
//! `match`-based [`GetRmgr`]: each builtin is a zero-sized unit struct implementing
//! `Rmgr`, and `GetRmgr` returns a `&'static dyn Rmgr` for an id. Because the
//! builtins are immutable zero-sized values dispatched by a `match`, there is no
//! mutable shared table and no lock; the custom-rmgr registration path
//! (`RegisterCustomRmgr`) and the `pg_get_wal_resource_managers` SQL function are
//! not implemented here.
//!
//! The per-resource-manager handlers (heap, btree, transaction, ... redo and desc)
//! are not yet ported, so the builtins inherit the trait's default `redo`/`desc`/
//! `identify`, which are inert no-ops rather than panics - an unfired record must
//! not abort recovery. Only `name()` is overridden, returning the correct
//! PostgreSQL name. `redo` takes the non-generic [`DecodedXLogRecord`] (the slice
//! of a WAL reader's state a redo handler actually reads), keeping the trait and
//! `GetRmgr` independent of the reader's generic parameter. `RmgrNotFound` raises a
//! PostgreSQL-style error carrying the original message and hint.

use crate::access::rmgr::{RmgrId, RM_N_BUILTIN_IDS};
use crate::access::rmgrlist::RmgrId as BuiltinRmgrId;
use crate::access::xlogreader::DecodedXLogRecord;

/// Method table for a resource manager (PG's `RmgrData` struct of function
/// pointers -> a trait). Only `name` is required; `redo`/`desc`/`identify` default
/// to the deferred no-op (per-AM handlers land later) and the optional
/// `startup`/`cleanup` default to no-ops (C's NULL callbacks).
pub trait Rmgr {
    /// PG `rm_name`.
    fn name(&self) -> &'static str;

    /// PG `rm_redo`: apply a decoded record during recovery. Deferred default:
    /// per-AM redo is not yet ported, so this is a no-op (NOT a panic).
    fn redo(&self, _record: &DecodedXLogRecord) {}

    /// PG `rm_desc`: render a human-readable description of a record. Deferred
    /// default: empty.
    fn desc(&self, _record: &DecodedXLogRecord) -> String {
        String::new()
    }

    /// PG `rm_identify`: name the record type for the rmgr-owned `xl_info` bits.
    /// Deferred default: unknown.
    fn identify(&self, _info: u8) -> Option<&'static str> {
        None
    }

    /// PG `rm_startup`: optional per-rmgr recovery startup. Default no-op.
    fn startup(&self) {}

    /// PG `rm_cleanup`: optional per-rmgr recovery cleanup. Default no-op.
    fn cleanup(&self) {}
}

/// Declare one zero-sized builtin rmgr struct + its `Rmgr` impl (only `name`
/// overridden; the rest use the deferred trait defaults), plus a `&'static`
/// const handle for the `GetRmgr` match.
macro_rules! builtin_rmgr {
    ($struct:ident, $konst:ident, $id:expr) => {
        pub struct $struct;
        impl Rmgr for $struct {
            fn name(&self) -> &'static str {
                $id.name()
            }
        }
        const $konst: &'static dyn Rmgr = &$struct;
    };
    // Variant that also wires the rmgr descriptor routines (rm_desc/rm_identify),
    // for the rmgrs whose rmgrdesc bodies are ported (step 49).
    ($struct:ident, $konst:ident, $id:expr, $desc:path, $identify:path) => {
        pub struct $struct;
        impl Rmgr for $struct {
            fn name(&self) -> &'static str {
                $id.name()
            }
            fn desc(&self, record: &DecodedXLogRecord) -> String {
                $desc(record)
            }
            fn identify(&self, info: u8) -> Option<&'static str> {
                $identify(info)
            }
        }
        const $konst: &'static dyn Rmgr = &$struct;
    };
}

use crate::backend::access::rmgrdesc::{
    clogdesc, dbasedesc, heapdesc, nbtdesc, seqdesc, smgrdesc, xactdesc, xlogdesc,
};

// Mirrors the PG_RMGR(...) list in access/rmgrlist.h, in id order. The rmgrs with
// ported descriptor bodies wire rm_desc/rm_identify; the rest inherit the inert
// deferred defaults (empty desc, None identify) until their AMs land.
builtin_rmgr!(XlogRmgr, XLOG_RMGR, BuiltinRmgrId::Xlog, xlogdesc::xlog_desc, xlogdesc::xlog_identify);
builtin_rmgr!(XactRmgr, XACT_RMGR, BuiltinRmgrId::Xact, xactdesc::xact_desc, xactdesc::xact_identify);
builtin_rmgr!(SmgrRmgr, SMGR_RMGR, BuiltinRmgrId::Smgr, smgrdesc::smgr_desc, smgrdesc::smgr_identify);
builtin_rmgr!(ClogRmgr, CLOG_RMGR, BuiltinRmgrId::Clog, clogdesc::clog_desc, clogdesc::clog_identify);
builtin_rmgr!(DbaseRmgr, DBASE_RMGR, BuiltinRmgrId::Dbase, dbasedesc::dbase_desc, dbasedesc::dbase_identify);
builtin_rmgr!(TblspcRmgr, TBLSPC_RMGR, BuiltinRmgrId::Tblspc);
builtin_rmgr!(MultixactRmgr, MULTIXACT_RMGR, BuiltinRmgrId::Multixact);
builtin_rmgr!(RelmapRmgr, RELMAP_RMGR, BuiltinRmgrId::Relmap);
builtin_rmgr!(StandbyRmgr, STANDBY_RMGR, BuiltinRmgrId::Standby);
builtin_rmgr!(Heap2Rmgr, HEAP2_RMGR, BuiltinRmgrId::Heap2, heapdesc::heap2_desc, heapdesc::heap2_identify);
builtin_rmgr!(HeapRmgr, HEAP_RMGR, BuiltinRmgrId::Heap, heapdesc::heap_desc, heapdesc::heap_identify);
builtin_rmgr!(BtreeRmgr, BTREE_RMGR, BuiltinRmgrId::Btree, nbtdesc::btree_desc, nbtdesc::btree_identify);
builtin_rmgr!(HashRmgr, HASH_RMGR, BuiltinRmgrId::Hash);
builtin_rmgr!(GinRmgr, GIN_RMGR, BuiltinRmgrId::Gin);
builtin_rmgr!(GistRmgr, GIST_RMGR, BuiltinRmgrId::Gist);
builtin_rmgr!(SeqRmgr, SEQ_RMGR, BuiltinRmgrId::Seq);
builtin_rmgr!(SpgistRmgr, SPGIST_RMGR, BuiltinRmgrId::Spgist);
builtin_rmgr!(BrinRmgr, BRIN_RMGR, BuiltinRmgrId::Brin);
builtin_rmgr!(CommitTsRmgr, COMMIT_TS_RMGR, BuiltinRmgrId::CommitTs);
builtin_rmgr!(ReploriginRmgr, REPLORIGIN_RMGR, BuiltinRmgrId::Replorigin);
builtin_rmgr!(GenericRmgr, GENERIC_RMGR, BuiltinRmgrId::Generic);
builtin_rmgr!(LogicalmsgRmgr, LOGICALMSG_RMGR, BuiltinRmgrId::Logicalmsg);

/// The builtin rmgr ids in id order; the iteration order for `RmgrStartup` /
/// `RmgrCleanup` and the existence check.
const BUILTIN_RMGR_IDS: [BuiltinRmgrId; RM_N_BUILTINS] = [
    BuiltinRmgrId::Xlog,
    BuiltinRmgrId::Xact,
    BuiltinRmgrId::Smgr,
    BuiltinRmgrId::Clog,
    BuiltinRmgrId::Dbase,
    BuiltinRmgrId::Tblspc,
    BuiltinRmgrId::Multixact,
    BuiltinRmgrId::Relmap,
    BuiltinRmgrId::Standby,
    BuiltinRmgrId::Heap2,
    BuiltinRmgrId::Heap,
    BuiltinRmgrId::Btree,
    BuiltinRmgrId::Hash,
    BuiltinRmgrId::Gin,
    BuiltinRmgrId::Gist,
    BuiltinRmgrId::Seq,
    BuiltinRmgrId::Spgist,
    BuiltinRmgrId::Brin,
    BuiltinRmgrId::CommitTs,
    BuiltinRmgrId::Replorigin,
    BuiltinRmgrId::Generic,
    BuiltinRmgrId::Logicalmsg,
];

/// Builtin rmgr count = canonical C `RM_N_BUILTIN_IDS` (single source of truth).
const RM_N_BUILTINS: usize = RM_N_BUILTIN_IDS as usize;

/// C `GetRmgr(rmid)`: the resource manager for `rmid`. Panics on an unregistered
/// id (the C macro asserts `RmgrIdExists`; the prior table lookup likewise
/// panicked / asserted on an empty slot).
#[allow(non_snake_case)]
pub fn GetRmgr(rmid: RmgrId) -> &'static dyn Rmgr {
    match rmid {
        x if x == BuiltinRmgrId::Xlog as RmgrId => XLOG_RMGR,
        x if x == BuiltinRmgrId::Xact as RmgrId => XACT_RMGR,
        x if x == BuiltinRmgrId::Smgr as RmgrId => SMGR_RMGR,
        x if x == BuiltinRmgrId::Clog as RmgrId => CLOG_RMGR,
        x if x == BuiltinRmgrId::Dbase as RmgrId => DBASE_RMGR,
        x if x == BuiltinRmgrId::Tblspc as RmgrId => TBLSPC_RMGR,
        x if x == BuiltinRmgrId::Multixact as RmgrId => MULTIXACT_RMGR,
        x if x == BuiltinRmgrId::Relmap as RmgrId => RELMAP_RMGR,
        x if x == BuiltinRmgrId::Standby as RmgrId => STANDBY_RMGR,
        x if x == BuiltinRmgrId::Heap2 as RmgrId => HEAP2_RMGR,
        x if x == BuiltinRmgrId::Heap as RmgrId => HEAP_RMGR,
        x if x == BuiltinRmgrId::Btree as RmgrId => BTREE_RMGR,
        x if x == BuiltinRmgrId::Hash as RmgrId => HASH_RMGR,
        x if x == BuiltinRmgrId::Gin as RmgrId => GIN_RMGR,
        x if x == BuiltinRmgrId::Gist as RmgrId => GIST_RMGR,
        x if x == BuiltinRmgrId::Seq as RmgrId => SEQ_RMGR,
        x if x == BuiltinRmgrId::Spgist as RmgrId => SPGIST_RMGR,
        x if x == BuiltinRmgrId::Brin as RmgrId => BRIN_RMGR,
        x if x == BuiltinRmgrId::CommitTs as RmgrId => COMMIT_TS_RMGR,
        x if x == BuiltinRmgrId::Replorigin as RmgrId => REPLORIGIN_RMGR,
        x if x == BuiltinRmgrId::Generic as RmgrId => GENERIC_RMGR,
        x if x == BuiltinRmgrId::Logicalmsg as RmgrId => LOGICALMSG_RMGR,
        // C routes an unregistered id through RmgrNotFound (errmsg + errhint).
        other => RmgrNotFound(other),
    }
}

/// C `RmgrIdExists(rmid)`: whether `rmid` names a registered (builtin) rmgr.
#[allow(non_snake_case)]
pub fn RmgrIdExists(rmid: RmgrId) -> bool {
    (rmid as usize) < RM_N_BUILTINS
}

/// Start up all resource managers (PG `RmgrStartup`).
#[allow(non_snake_case)]
pub fn RmgrStartup() {
    for id in BUILTIN_RMGR_IDS {
        GetRmgr(id as RmgrId).startup();
    }
}

/// Clean up all resource managers (PG `RmgrCleanup`).
#[allow(non_snake_case)]
pub fn RmgrCleanup() {
    for id in BUILTIN_RMGR_IDS {
        GetRmgr(id as RmgrId).cleanup();
    }
}

/// C `RmgrNotFound`: ereport(ERROR) when a record carries an unrecognized
/// RmgrId. Carries PG's errmsg + errhint; ereport(ERROR) -> panic per error model.
#[allow(non_snake_case)]
pub fn RmgrNotFound(rmid: RmgrId) -> ! {
    panic!(
        "resource manager with ID {rmid} not registered\n\
         HINT: Include the extension module that implements this resource manager in \"shared_preload_libraries\"."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xlog_rmgr_is_named_xlog() {
        let rmid = BuiltinRmgrId::Xlog as RmgrId;
        assert!(RmgrIdExists(rmid));
        assert_eq!(GetRmgr(rmid).name(), "XLOG");
    }

    #[test]
    fn a_few_builtin_names() {
        assert_eq!(GetRmgr(BuiltinRmgrId::Xact as RmgrId).name(), "Transaction");
        assert_eq!(GetRmgr(BuiltinRmgrId::Heap as RmgrId).name(), "Heap");
        assert_eq!(GetRmgr(BuiltinRmgrId::Btree as RmgrId).name(), "Btree");
        assert_eq!(GetRmgr(BuiltinRmgrId::Logicalmsg as RmgrId).name(), "LogicalMessage");
    }

    #[test]
    fn all_builtins_resolve_with_their_name() {
        for id in BUILTIN_RMGR_IDS {
            let rmid = id as RmgrId;
            assert!(RmgrIdExists(rmid), "builtin rmid {rmid} missing");
            assert_eq!(GetRmgr(rmid).name(), id.name());
        }
    }

    #[test]
    fn unregistered_id_is_rejected() {
        // One past the last builtin is an unregistered id.
        let unused = BuiltinRmgrId::MAX_ID as RmgrId + 1;
        assert!(!RmgrIdExists(unused));
    }

    #[test]
    #[should_panic(expected = "not registered")]
    fn get_rmgr_panics_on_unregistered_id() {
        let unused = BuiltinRmgrId::MAX_ID as RmgrId + 1;
        let _ = GetRmgr(unused);
    }

    #[test]
    fn deferred_defaults_are_inert() {
        // The deferred identify default returns None (not unimplemented!()):
        // per-AM handlers land later, and an unfired record must not panic. Hash
        // has no ported rmgrdesc body yet, so it still uses the trait default.
        assert_eq!(GetRmgr(BuiltinRmgrId::Hash as RmgrId).identify(0), None);
    }

    #[test]
    fn ported_rmgr_identify_is_wired() {
        // The rmgrs with ported rmgrdesc bodies (step 49) override identify.
        assert_eq!(
            GetRmgr(BuiltinRmgrId::Xact as RmgrId).identify(0),
            Some("COMMIT")
        );
    }

    #[test]
    fn startup_cleanup_are_noops() {
        // Builtins use the no-op startup/cleanup defaults; these must not touch
        // the deferred redo/desc handlers.
        RmgrStartup();
        RmgrCleanup();
    }
}
