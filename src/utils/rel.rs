//! Translated from PostgreSQL src/include/utils/rel.h
#![allow(
    clippy::cast_ptr_alignment,
    reason = "PG rd_options/varlena pointer reinterpretation, faithful to C"
)]
//! POSTGRES relation descriptor (a/k/a relcache entry) definitions.
//!
//! In-memory (no layout contract). `RelationData` is the full relcache entry;
//! this is the real home of the type that utils/relcache.rs forward-declares
//! (its placeholder `RelationData` + `Arc<RelationData> = *mut RelationData` alias get
//! repointed here in Phase 2). The many `RelationGet*`/`RelationIs*`/
//! `RelationNeeds*` accessor macros become methods on `RelationData`.

use crate::access::htup::HeapTupleData;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::access::tupdesc::TupleDesc;
use crate::access::xlog::{WalLevel, WAL_LEVEL};
use crate::c::{RegProcedure, SubTransactionId, TransactionId};
use crate::catalog::catalog::IsCatalogRelation;
use crate::catalog::pg_class::{
    FormData_pg_class, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_RELATION,
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
};
use crate::catalog::pg_index::FormData_pg_index;
use crate::catalog::pg_publication::PublicationDesc;
use crate::common::relpath::InvalidRelFileNumber;
use crate::fmgr::FmgrInfo;
use crate::nodes::bitmapset::Bitmapset;
use crate::partitioning::partdefs::{PartitionDesc, PartitionKey};
use crate::postgres_ext::Oid;
use crate::rewrite::prs2lock::RuleLock;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::procnumber::ProcNumber;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;
use crate::utils::reltrigger::TriggerDesc;

// These belong to lmgr.h but are declared here so a LockInfoData field can live
// in a Arc<RelationData>.

/// Identifies a relation by (relId, dbId) for the lock manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockRelId {
    pub relId: Oid, // a relation identifier
    pub dbId: Oid,  // a database identifier
}

/// Lock manager bookkeeping carried inside a Arc<RelationData>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockInfoData {
    pub lockRelId: LockRelId,
}

/// C `typedef LockInfoData *LockInfo;`.
pub type LockInfo = *mut LockInfoData; // TODO(ptr)

// Opaque raw-pointer targets; the canonical table AM (crate::access::tableam::TableAm)
// and FDW API (crate::foreign::fdwapi::FdwRoutine) are traits, not thin structs.
pub struct TableAmRoutine {
    _private: [u8; 0],
}

pub struct FdwRoutine {
    _private: [u8; 0],
}

/// Contents of a relation cache entry. Shared-immutable behind `Arc`: build-once
/// fields are read through `&RelationData`; the genuinely post-share-mutable
/// fields (`rd_refcnt`, the `rd_*valid` flags, `rd_smgr`) are interior-mutable
/// (atomics / a small `Mutex`). The smgr handle is an owned `Box<SmgrRelation>`,
/// so every field is `Send`/`Sync` and `RelationData` is genuinely auto-Send.
pub struct RelationData {
    pub rd_locator: RelFileLocator, // relation physical identifier
    /// Cached smgr file handle, or `None`. Interior-mutable (lazily opened in
    /// `smgr()`, cleared on `close_smgr`). The relcache owns the handle for the
    /// entry's lifetime (`SmgrRelation` is `Send`, so this is genuinely `Send`).
    pub rd_smgr: Mutex<Option<Box<SmgrRelation>>>,
    pub rd_refcnt: AtomicI32,    // reference count (PG resowner pin count)
    pub rd_backend: ProcNumber,  // owning backend's proc number, if temp rel
    pub rd_islocaltemp: bool,    // rel is a temp rel of this session
    pub rd_isnailed: bool,       // rel is nailed in cache
    pub rd_isvalid: AtomicBool,  // relcache entry is valid
    pub rd_indexvalid: AtomicBool, // is rd_indexlist valid?
    pub rd_statvalid: AtomicBool,  // is rd_statlist valid?

    // Subtransaction bookkeeping; accuracy is critical to RelationNeedsWAL().
    pub rd_createSubid: SubTransactionId, // rel was created in current xact
    pub rd_newRelfilelocatorSubid: SubTransactionId, // highest subxact changing rd_locator to current value
    pub rd_firstRelfilelocatorSubid: SubTransactionId, // highest subxact changing rd_locator to any value
    pub rd_droppedSubid: SubTransactionId,             // dropped with another Subid set

    pub rd_rel: Option<Box<FormData_pg_class>>, // RELATION tuple (relcache owns the row)
    pub rd_att: Option<TupleDesc>,       // tuple descriptor (None until built)
    pub rd_id: Oid,                      // relation's object id
    pub rd_lockInfo: LockInfoData,       // lock mgr's info for locking relation
    pub rd_rules: Option<Box<RuleLock>>, // rewrite rules, or None
    pub rd_rulescxt: (),                 // private memory cxt for rd_rules (tombstoned)
    pub trigdesc: Option<Box<TriggerDesc>>, // trigger info, or None
    /// Row security policies. Unused in this port (the target `RowSecurityDesc`
    /// holds a tombstoned `MemoryContext` and is `!Send`); a presence flag.
    pub rd_rsdesc: Option<()>,

    // data managed by RelationGetFKeyList:
    pub rd_fkeylist: Vec<ForeignKeyCacheInfo>, // list of ForeignKeyCacheInfo
    pub rd_fkeyvalid: bool,                    // true if list has been computed

    // data managed by RelationGetPartitionKey:
    pub rd_partkey: Option<PartitionKey>, // partition key, or None
    pub rd_partkeycxt: (),                // private context for rd_partkey (tombstoned)

    // data managed by RelationGetPartitionDesc:
    pub rd_partdesc: Option<PartitionDesc>, // partition descriptor, or None
    pub rd_pdcxt: (),                       // private context for rd_partdesc (tombstoned)

    // Same as above, for partdescs that omit detached partitions:
    pub rd_partdesc_nodetached: Option<PartitionDesc>, // partdesc w/o detached parts
    pub rd_pddcxt: (),                                  // for rd_partdesc_nodetached (tombstoned)
    pub rd_partdesc_nodetached_xmin: TransactionId,    // pg_inherits.xmin of the excluded partition

    // data managed by RelationGetPartitionQual:
    pub rd_partcheck: Vec<String>, // partition CHECK quals (node trees)
    pub rd_partcheckvalid: bool,   // true if list has been computed
    pub rd_partcheckcxt: (),       // private cxt for rd_partcheck (tombstoned)

    // data managed by RelationGetIndexList:
    pub rd_indexlist: Vec<Oid>,  // list of OIDs of indexes on relation
    pub rd_pkindex: Oid,         // OID of (deferrable?) primary key, if any
    pub rd_ispkdeferrable: bool, // is rd_pkindex a deferrable PK?
    pub rd_replidindex: Oid,     // OID of replica identity index, if any

    // data managed by RelationGetStatExtList:
    pub rd_statlist: Vec<Oid>, // list of OIDs of extended stats

    // data managed by RelationGetIndexAttrBitmap:
    pub rd_attrsvalid: AtomicBool,             // are bitmaps of attrs valid?
    pub rd_keyattr: Option<Bitmapset>,         // cols that can be ref'd by foreign keys
    pub rd_pkattr: Option<Bitmapset>,          // cols included in primary key
    pub rd_idattr: Option<Bitmapset>,          // included in replica identity index
    pub rd_hotblockingattr: Option<Bitmapset>, // cols blocking HOT update
    pub rd_summarizedattr: Option<Bitmapset>,  // cols indexed by summarizing indexes

    pub rd_pubdesc: Option<Box<PublicationDesc>>, // publication descriptor, or None

    /// Parsed pg_class.reloptions varlena blob, or None ("use defaults"). Owned
    /// bytes (MAXALIGN'd, reinterpreted as StdRdOptions/ViewOptions on read).
    pub rd_options: Option<Box<[u8]>>,

    /// OID of the handler for this relation (index AM or table AM handler fn).
    pub rd_amhandler: Oid,

    /// Table access method. Opaque handle, or None (the AM is the closed
    /// `crate::access::tableam::TableAm` trait in this port).
    pub rd_tableam: Option<Box<TableAmRoutine>>,

    // Non-null only for an index relation:
    pub rd_index: Option<Box<FormData_pg_index>>, // pg_index tuple describing this index
    /// All of the pg_index tuple. Unused in this port (`rd_index` holds the form);
    /// kept as a placeholder presence flag to stay genuinely `Send`.
    pub rd_indextuple: Option<()>,

    // Index access support info (index relation only):
    pub rd_indexcxt: (),                     // private memory cxt for this stuff (tombstoned)
    pub rd_indam: Option<Box<IndexAmRoutineHandle>>, // index AM's API struct, or None
    pub rd_opfamily: Vec<Oid>,      // OIDs of op families for each index col
    pub rd_opcintype: Vec<Oid>,     // OIDs of opclass declared input data types
    pub rd_support: Vec<RegProcedure>, // OIDs of support procedures
    pub rd_supportinfo: Vec<FmgrInfo>, // lookup info for support procedures
    pub rd_indoption: Vec<i16>,     // per-column AM-specific flags
    pub rd_indexprs: Vec<String>,   // index expression trees, if any
    pub rd_indpred: Vec<String>,    // index predicate tree, if any
    pub rd_exclops: Vec<Oid>,       // OIDs of exclusion operators, if any
    pub rd_exclprocs: Vec<Oid>,     // OIDs of exclusion ops' procs, if any
    pub rd_exclstrats: Vec<u16>,    // exclusion ops' strategy numbers, if any
    pub rd_indcollation: Vec<Oid>,  // OIDs of index collations
    /// Parsed opclass-specific options. Unused in this port; kept as a presence
    /// flag (the per-column bytea blobs are not yet built).
    pub rd_opcoptions: Option<()>,

    /// Available for index/table AMs to cache private data (reset on inval).
    /// Unused in this port; a presence flag, kept `Send`.
    pub rd_amcache: Option<()>,

    /// Cached FDW function pointers, or None (foreign-table support).
    pub rd_fdwroutine: Option<Box<FdwRoutine>>,

    /// Real TOAST table's OID for CLUSTER/rewrite, or InvalidOid.
    pub rd_toastoid: Oid,

    pub pgstat_enabled: bool, // should relation stats be counted
    /// Statistics collection area. Unused in this port (the target struct is
    /// self-referential / `!Send`); a presence flag, kept `Send`.
    pub pgstat_info: Option<()>,
}

// rd_indam points at the index AM API struct. In the routine-struct port the AM
// is the closed enum crate::access::amapi::IndexAmKind, not a thin struct, so
// this stays an opaque raw-pointer target.
pub struct IndexAmRoutineHandle {
    _private: [u8; 0],
}

// C `typedef struct RelationData *Relation;` -- the relcache entry handle. The
// handle alias is retired: holders write `Arc<RelationData>` (owner: relcache
// slot, scans, executor registry hold clones) or `&RelationData` (reads deref
// to a borrow) explicitly. `RelationData` is genuinely auto-`Send` + auto-`Sync`
// (every field is an owned `Send`/`Sync` type:
// `Box`/`Vec`/atomics/scalars/`Mutex<Option<Box<SmgrRelation>>>`); no `unsafe
// impl` is needed (this retired the keystone consolidation impl).

/// Information the relcache caches about foreign key constraints (an image of
/// pg_constraint columns). A Node subclass in C; the per-FK-column arrays are
/// fixed-size (at most INDEX_MAX_KEYS).
pub struct ForeignKeyCacheInfo {
    pub conoid: Oid,       // oid of the constraint itself
    pub conrelid: Oid,     // relation constrained by the foreign key
    pub confrelid: Oid,    // relation referenced by the foreign key
    pub nkeys: i32,        // number of columns in the foreign key
    pub conenforced: bool, // is enforced?
    // each has nkeys valid entries:
    pub conkey: [i16; crate::pg_config_manual::INDEX_MAX_KEYS], // cols in referencing table
    pub confkey: [i16; crate::pg_config_manual::INDEX_MAX_KEYS], // cols in referenced table
    pub conpfeqop: [Oid; crate::pg_config_manual::INDEX_MAX_KEYS], // PK = FK operator OIDs
}

/// autovacuum-related reloptions (part of StdRdOptions).
pub struct AutoVacOpts {
    pub enabled: bool,
    pub vacuum_threshold: i32,
    pub vacuum_max_threshold: i32,
    pub vacuum_ins_threshold: i32,
    pub analyze_threshold: i32,
    pub vacuum_cost_limit: i32,
    pub freeze_min_age: i32,
    pub freeze_max_age: i32,
    pub freeze_table_age: i32,
    pub multixact_freeze_min_age: i32,
    pub multixact_freeze_max_age: i32,
    pub multixact_freeze_table_age: i32,
    pub log_min_duration: i32,
    pub vacuum_cost_delay: f64,
    pub vacuum_scale_factor: f64,
    pub vacuum_ins_scale_factor: f64,
    pub analyze_scale_factor: f64,
}

/// StdRdOptions->vacuum_index_cleanup values.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdRdOptIndexCleanup {
    AUTO = 0,
    OFF,
    ON,
}

/// Standard contents of rd_options for heaps. On-disk-ish: it begins with a
/// varlena header (`vl_len_`) and is stored as the relation's reloptions blob,
/// so kept `#[repr(C)]` with the leading varlena length word.
#[repr(C)]
pub struct StdRdOptions {
    pub vl_len_: i32,             // varlena header (do not touch directly!)
    pub fillfactor: i32,          // page fill factor in percent (0..100)
    pub toast_tuple_target: i32,  // target for tuple toasting
    pub autovacuum: AutoVacOpts,  // autovacuum-related options
    pub user_catalog_table: bool, // use as an additional catalog relation
    pub parallel_workers: i32,    // max number of parallel workers
    pub vacuum_index_cleanup: StdRdOptIndexCleanup, // controls index vacuuming
    pub vacuum_truncate: bool,    // enables vacuum to truncate a relation
    pub vacuum_truncate_set: bool, // whether vacuum_truncate is set
    pub vacuum_max_eager_freeze_failure_rate: f64, // 0 if disabled, -1 if unspecified
}

pub const HEAP_MIN_FILLFACTOR: i32 = 10;
pub const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

/// ViewOptions->check_option values.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewOptCheckOption {
    NOT_SET,
    LOCAL,
    CASCADED,
}

/// Contents of rd_options for views. Varlena-prefixed like StdRdOptions.
#[repr(C)]
pub struct ViewOptions {
    pub vl_len_: i32, // varlena header (do not touch directly!)
    pub security_barrier: bool,
    pub security_invoker: bool,
    pub check_option: ViewOptCheckOption,
}

// BLCKSZ from the build config; the target uses the default 8KB page.
const BLCKSZ: i32 = 8192;

/// True if WAL is needed even for new/truncated rels (wal_level >= replica).
/// Replaces the C `XLogIsNeeded()` macro.
fn xlog_is_needed() -> bool {
    // SAFETY: WAL_LEVEL is process/session config; read-only here.
    let level = unsafe { WAL_LEVEL };
    level >= WalLevel::Replica as i32
}

/// True if logical decoding info must be logged (wal_level == logical).
/// Replaces the C `XLogLogicalInfoActive()` macro.
fn xlog_logical_info_active() -> bool {
    let level = unsafe { WAL_LEVEL };
    level >= WalLevel::Logical as i32
}

/// PG `XLogLogicalInfoActive()` macro, header-facing name.
#[allow(non_snake_case, reason = "mirrors the C macro name")]
pub fn XLogLogicalInfoActive() -> bool {
    xlog_logical_info_active()
}

impl RelationData {
    /// A zeroed relation descriptor (C `AllocateRelationDesc`'s `palloc0`). All
    /// pointers null, lists empty, flags false; the relcache fills the load-bearing
    /// fields (`rd_id`/`rd_rel`/`rd_att`/`rd_isnailed`/...) after this.
    #[must_use]
    pub fn blank() -> Self {
        use crate::c::{InvalidSubTransactionId, InvalidTransactionId};
        use crate::common::relpath::InvalidRelFileNumber;
        use crate::postgres_ext::InvalidOid;
        use crate::storage::procnumber::INVALID_PROC_NUMBER;
        Self {
            rd_locator: RelFileLocator {
                spcOid: InvalidOid,
                dbOid: InvalidOid,
                relNumber: InvalidRelFileNumber,
            },
            rd_smgr: Mutex::new(None),
            rd_refcnt: AtomicI32::new(0),
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isnailed: false,
            rd_isvalid: AtomicBool::new(false),
            rd_indexvalid: AtomicBool::new(false),
            rd_statvalid: AtomicBool::new(false),
            rd_createSubid: InvalidSubTransactionId,
            rd_newRelfilelocatorSubid: InvalidSubTransactionId,
            rd_firstRelfilelocatorSubid: InvalidSubTransactionId,
            rd_droppedSubid: InvalidSubTransactionId,
            rd_rel: None,
            rd_att: None,
            rd_id: InvalidOid,
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: InvalidOid, dbId: InvalidOid },
            },
            rd_rules: None,
            rd_rulescxt: (),
            trigdesc: None,
            rd_rsdesc: None,
            rd_fkeylist: Vec::new(),
            rd_fkeyvalid: false,
            rd_partkey: None,
            rd_partkeycxt: (),
            rd_partdesc: None,
            rd_pdcxt: (),
            rd_partdesc_nodetached: None,
            rd_pddcxt: (),
            rd_partdesc_nodetached_xmin: InvalidTransactionId,
            rd_partcheck: Vec::new(),
            rd_partcheckvalid: false,
            rd_partcheckcxt: (),
            rd_indexlist: Vec::new(),
            rd_pkindex: InvalidOid,
            rd_ispkdeferrable: false,
            rd_replidindex: InvalidOid,
            rd_statlist: Vec::new(),
            rd_attrsvalid: AtomicBool::new(false),
            rd_keyattr: None,
            rd_pkattr: None,
            rd_idattr: None,
            rd_hotblockingattr: None,
            rd_summarizedattr: None,
            rd_pubdesc: None,
            rd_options: None,
            rd_amhandler: InvalidOid,
            rd_tableam: None,
            rd_index: None,
            rd_indextuple: None,
            rd_indexcxt: (),
            rd_indam: None,
            rd_opfamily: Vec::new(),
            rd_opcintype: Vec::new(),
            rd_support: Vec::new(),
            rd_supportinfo: Vec::new(),
            rd_indoption: Vec::new(),
            rd_indexprs: Vec::new(),
            rd_indpred: Vec::new(),
            rd_exclops: Vec::new(),
            rd_exclprocs: Vec::new(),
            rd_exclstrats: Vec::new(),
            rd_indcollation: Vec::new(),
            rd_opcoptions: None,
            rd_amcache: None,
            rd_fdwroutine: None,
            rd_toastoid: InvalidOid,
            pgstat_enabled: false,
            pgstat_info: None,
        }
    }

    /// The pg_class form (`rd_rel`); panics if the relcache entry is not built.
    fn rel(&self) -> &FormData_pg_class {
        self.rd_rel
            .as_deref()
            .unwrap_or_else(|| unreachable!("relcache entry has a pg_class form"))
    }

    /// Reinterpret the reloptions blob as `T` (StdRdOptions/ViewOptions). `None`
    /// when no reloptions are set. The blob is MAXALIGN'd reloptions bytes.
    fn rd_options_as<T>(&self) -> Option<&T> {
        let blob = self.rd_options.as_deref()?;
        // SAFETY: a non-None reloptions blob is a MAXALIGN'd buffer beginning with
        // the requested option struct (faithful to PG's varlena reinterpretation).
        Some(unsafe { &*blob.as_ptr().cast::<T>() })
    }

    /// RelationGetToastTupleTarget: toast_tuple_target, or `defaulttarg`.
    pub fn toast_tuple_target(&self, defaulttarg: i32) -> i32 {
        self.rd_options_as::<StdRdOptions>().map_or(defaulttarg, |o| o.toast_tuple_target)
    }

    /// RelationGetFillFactor: fillfactor, or `defaultff`.
    pub fn fill_factor(&self, defaultff: i32) -> i32 {
        self.rd_options_as::<StdRdOptions>().map_or(defaultff, |o| o.fillfactor)
    }

    /// RelationGetTargetPageUsage: desired space usage per page in bytes.
    pub fn target_page_usage(&self, defaultff: i32) -> i32 {
        BLCKSZ * self.fill_factor(defaultff) / 100
    }

    /// RelationGetTargetPageFreeSpace: desired free space per page in bytes.
    pub fn target_page_free_space(&self, defaultff: i32) -> i32 {
        BLCKSZ * (100 - self.fill_factor(defaultff)) / 100
    }

    /// RelationIsUsedAsCatalogTable: treat as a catalog table for logical decoding.
    pub fn is_used_as_catalog_table(&self) -> bool {
        let Some(opts) = self.rd_options_as::<StdRdOptions>() else {
            return false;
        };
        let relkind = self.rel().relkind;
        if relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW {
            opts.user_catalog_table
        } else {
            false
        }
    }

    /// RelationGetParallelWorkers: parallel_workers reloption, or `defaultpw`.
    pub fn parallel_workers(&self, defaultpw: i32) -> i32 {
        self.rd_options_as::<StdRdOptions>().map_or(defaultpw, |o| o.parallel_workers)
    }

    /// RelationIsSecurityView: whether the view is a security barrier view.
    pub fn is_security_view(&self) -> bool {
        self.rd_options_as::<ViewOptions>().is_some_and(|o| o.security_barrier)
    }

    /// RelationHasSecurityInvoker: whether the view has security_invoker set.
    pub fn has_security_invoker(&self) -> bool {
        self.rd_options_as::<ViewOptions>().is_some_and(|o| o.security_invoker)
    }

    /// RelationHasCheckOption: view has a local or cascaded check option.
    pub fn has_check_option(&self) -> bool {
        self.rd_options_as::<ViewOptions>()
            .is_some_and(|o| o.check_option != ViewOptCheckOption::NOT_SET)
    }

    /// RelationHasLocalCheckOption: view defined with the local check option.
    pub fn has_local_check_option(&self) -> bool {
        self.rd_options_as::<ViewOptions>()
            .is_some_and(|o| o.check_option == ViewOptCheckOption::LOCAL)
    }

    /// RelationHasCascadedCheckOption: view defined with the cascaded check option.
    pub fn has_cascaded_check_option(&self) -> bool {
        self.rd_options_as::<ViewOptions>()
            .is_some_and(|o| o.check_option == ViewOptCheckOption::CASCADED)
    }

    /// RelationHasReferenceCountZero.
    pub fn has_reference_count_zero(&self) -> bool {
        self.rd_refcnt.load(Ordering::Relaxed) == 0
    }

    /// Increment the PG resowner pin count (`RelationIncrementReferenceCount`).
    pub fn incr_ref_count(&self) {
        self.rd_refcnt.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the PG resowner pin count (`RelationDecrementReferenceCount`),
    /// saturating at zero.
    pub fn decr_ref_count(&self) {
        let _ = self.rd_refcnt.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
            (c > 0).then_some(c - 1)
        });
    }

    /// RelationGetForm: the pg_class tuple.
    pub fn form(&self) -> &FormData_pg_class {
        self.rel()
    }

    /// RelationGetRelid: the relation's OID.
    pub fn relid(&self) -> Oid {
        self.rd_id
    }

    /// RelationGetNumberOfAttributes.
    pub fn number_of_attributes(&self) -> i16 {
        self.rel().relnatts
    }

    /// The pg_index form (`rd_index`); panics if this is not a built index entry.
    fn index_form(&self) -> &FormData_pg_index {
        self.rd_index
            .as_deref()
            .unwrap_or_else(|| unreachable!("index relcache entry has a pg_index form"))
    }

    /// IndexRelationGetNumberOfAttributes (index relation only).
    pub fn index_number_of_attributes(&self) -> i16 {
        self.index_form().indnatts
    }

    /// IndexRelationGetNumberOfKeyAttributes (index relation only).
    pub fn index_number_of_key_attributes(&self) -> i16 {
        self.index_form().indnkeyatts
    }

    /// RelationGetDescr: the tuple descriptor. C returns the (non-null) pointer;
    /// here a clone of the `Arc` handle (a relcache entry always has one built).
    pub fn descr(&self) -> TupleDesc {
        Arc::clone(
            self.rd_att
                .as_ref()
                .unwrap_or_else(|| unreachable!("relcache entry has a tuple descriptor")),
        )
    }

    /// RelationGetNamespace: the rel's namespace OID.
    pub fn namespace(&self) -> Oid {
        self.rel().relnamespace
    }

    /// RelationIsMapped: uses the relfilenumber map.
    pub fn is_mapped(&self) -> bool {
        let rel = self.rel();
        crate::catalog::pg_class::RELKIND_HAS_STORAGE(rel.relkind)
            && rel.relfilenode == InvalidRelFileNumber
    }

    /// RelationGetSmgr: open the smgr handle if needed, then return it. The handle
    /// is interior-mutable (a `Mutex<Option<Box<SmgrRelation>>>`); the returned raw
    /// pointer is the stable address of the relcache-owned `Box`, valid while the
    /// relation is open. (No struct field stores this pointer across a task, so no
    /// `unsafe impl Send` is needed -- the owner is the `Box`.)
    pub fn smgr(&self) -> *mut SmgrRelation {
        let mut h = self.rd_smgr.lock();
        let reln = h.get_or_insert_with(|| {
            // smgropen returns an owned handle in this port (the pin GC is a no-op).
            Box::new(crate::storage::smgr::SmgrRelation::open(self.rd_locator, self.rd_backend))
        });
        std::ptr::from_mut::<SmgrRelation>(reln.as_mut())
    }

    /// RelationCloseSmgr: close at the smgr level, if open.
    pub fn close_smgr(&self) {
        let taken = self.rd_smgr.lock().take();
        if let Some(mut reln) = taken {
            reln.close();
        }
    }

    /// RelationGetTargetBlock: current insertion target, or InvalidBlockNumber.
    pub fn target_block(&self) -> BlockNumber {
        self.rd_smgr.lock().as_ref().map_or(INVALID_BLOCK_NUMBER, |r| r.targblock)
    }

    /// RelationSetTargetBlock: set the current insertion target block.
    pub fn set_target_block(&self, targblock: BlockNumber) {
        let smgr = self.smgr();
        // SAFETY: `smgr` is the stable address of the relcache-owned smgr Box,
        // just opened above; no other task accesses this entry's handle.
        unsafe { (*smgr).targblock = targblock };
    }

    /// RelationIsPermanent.
    pub fn is_permanent(&self) -> bool {
        self.rel().relpersistence == RELPERSISTENCE_PERMANENT
    }

    /// RelationNeedsWAL: false if wal_level=minimal and the rel was created or
    /// truncated in the current transaction.
    pub fn needs_wal(&self) -> bool {
        self.is_permanent()
            && (xlog_is_needed()
                || (self.rd_createSubid == crate::c::InvalidSubTransactionId
                    && self.rd_firstRelfilelocatorSubid == crate::c::InvalidSubTransactionId))
    }

    /// RelationUsesLocalBuffers: pages are stored in local buffers (temp rel).
    pub fn uses_local_buffers(&self) -> bool {
        self.rel().relpersistence == RELPERSISTENCE_TEMP
    }

    /// RELATION_IS_LOCAL: temp or newly created in the current transaction.
    pub fn is_local(&self) -> bool {
        self.rd_islocaltemp || self.rd_createSubid != crate::c::InvalidSubTransactionId
    }

    /// RELATION_IS_OTHER_TEMP: a temp relation belonging to another session.
    pub fn is_other_temp(&self) -> bool {
        self.rel().relpersistence == RELPERSISTENCE_TEMP && !self.rd_islocaltemp
    }

    /// RelationIsScannable: false only for an unpopulated matview.
    pub fn is_scannable(&self) -> bool {
        self.rel().relispopulated
    }

    /// RelationIsPopulated.
    pub fn is_populated(&self) -> bool {
        self.rel().relispopulated
    }

    /// RelationIsAccessibleInLogicalDecoding.
    pub fn is_accessible_in_logical_decoding(&self) -> bool {
        xlog_logical_info_active()
            && self.needs_wal()
            && (IsCatalogRelation(self) || self.is_used_as_catalog_table())
    }

    /// RelationIsLogicallyLogged.
    pub fn is_logically_logged(&self) -> bool {
        xlog_logical_info_active()
            && self.needs_wal()
            && self.rel().relkind != RELKIND_FOREIGN_TABLE
            && !IsCatalogRelation(self)
    }
}

/// RelationGetRelationName: the rel's name (unique within its namespace).
pub fn relation_get_relation_name(relation: &RelationData) -> String {
    let name = &relation.rel().relname;
    let bytes = crate::c::NameStr(name);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// RelationIsValid: true iff the descriptor handle is present. With the `Arc`
/// handle a `Arc<RelationData>` is always valid; nullable handles are `Option<Arc<RelationData>>`.
pub fn relation_is_valid(relation: &Option<Arc<RelationData>>) -> bool {
    relation.is_some()
}

// routines in utils/cache/relcache.c
pub fn RelationIncrementReferenceCount(rel: &RelationData) {
    rel.incr_ref_count();
}

pub fn RelationDecrementReferenceCount(rel: &RelationData) {
    rel.decr_ref_count();
}

#[cfg(test)]
mod send_sync_assert {
    fn assert_send_sync<T: Send + Sync>() {}
    /// `RelationData` is genuinely auto-`Send` + auto-`Sync` (no `unsafe impl`).
    #[test]
    fn relationdata_is_send_sync() {
        assert_send_sync::<super::RelationData>();
    }
}
