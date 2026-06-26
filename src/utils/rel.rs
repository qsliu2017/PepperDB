//! Translated from PostgreSQL src/include/utils/rel.h
#![allow(
    clippy::cast_ptr_alignment,
    reason = "PG rd_options/varlena pointer reinterpretation, faithful to C"
)]
//! POSTGRES relation descriptor (a/k/a relcache entry) definitions.
//!
//! In-memory (no layout contract). `RelationData` is the full relcache entry;
//! this is the real home of the type that utils/relcache.rs forward-declares
//! (its placeholder `RelationData` + `Relation = *mut RelationData` alias get
//! repointed here in Phase 2). The many `RelationGet*`/`RelationIs*`/
//! `RelationNeeds*` accessor macros become methods on `RelationData`.

use crate::access::htup::HeapTupleData;
use crate::access::tupdesc::TupleDesc;
use crate::access::xlog::{WalLevel, WAL_LEVEL};
use crate::c::{bytea, RegProcedure, SubTransactionId, TransactionId};
use crate::catalog::catalog::IsCatalogRelation;
use crate::catalog::pg_class::{
    Form_pg_class, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_RELATION,
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
};
use crate::catalog::pg_index::Form_pg_index;
use crate::catalog::pg_publication::PublicationDesc;
use crate::common::relpath::InvalidRelFileNumber;
use crate::fmgr::FmgrInfo;
use crate::nodes::bitmapset::Bitmapset;
use crate::partitioning::partdefs::{PartitionDesc, PartitionKey};
use crate::pgstat::PgStat_TableStatus;
use crate::postgres_ext::Oid;
use crate::rewrite::prs2lock::RuleLock;
use crate::rewrite::rowsecurity::RowSecurityDesc;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::procnumber::ProcNumber;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;
use crate::utils::palloc::MemoryContext;
use crate::utils::reltrigger::TriggerDesc;

// These belong to lmgr.h but are declared here so a LockInfoData field can live
// in a Relation.

/// Identifies a relation by (relId, dbId) for the lock manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockRelId {
    pub relId: Oid, // a relation identifier
    pub dbId: Oid,  // a database identifier
}

/// Lock manager bookkeeping carried inside a Relation.
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

/// Contents of a relation cache entry. In-memory: C pointers become raw
/// pointers (`*mut`/`*const`, ownership is the relcache's) pending Phase 2, and
/// C `List *` fields become `Vec`. The trailing variable-length nature does not
/// apply (no FAM here).
pub struct RelationData {
    pub rd_locator: RelFileLocator, // relation physical identifier
    pub rd_smgr: *mut SmgrRelation, // cached file handle, or null // TODO(ptr)
    pub rd_refcnt: i32,             // reference count
    pub rd_backend: ProcNumber,     // owning backend's proc number, if temp rel
    pub rd_islocaltemp: bool,       // rel is a temp rel of this session
    pub rd_isnailed: bool,          // rel is nailed in cache
    pub rd_isvalid: bool,           // relcache entry is valid
    pub rd_indexvalid: bool,        // is rd_indexlist valid?
    pub rd_statvalid: bool,         // is rd_statlist valid?

    // Subtransaction bookkeeping; accuracy is critical to RelationNeedsWAL().
    pub rd_createSubid: SubTransactionId, // rel was created in current xact
    pub rd_newRelfilelocatorSubid: SubTransactionId, // highest subxact changing rd_locator to current value
    pub rd_firstRelfilelocatorSubid: SubTransactionId, // highest subxact changing rd_locator to any value
    pub rd_droppedSubid: SubTransactionId,             // dropped with another Subid set

    pub rd_rel: Form_pg_class,           // RELATION tuple
    pub rd_att: TupleDesc,               // tuple descriptor
    pub rd_id: Oid,                      // relation's object id
    pub rd_lockInfo: LockInfoData,       // lock mgr's info for locking relation
    pub rd_rules: *mut RuleLock,         // rewrite rules // TODO(ptr)
    pub rd_rulescxt: MemoryContext,      // private memory cxt for rd_rules, if any
    pub trigdesc: *mut TriggerDesc,      // trigger info, or null // TODO(ptr)
    pub rd_rsdesc: *mut RowSecurityDesc, // row security policies, or null // TODO(ptr)

    // data managed by RelationGetFKeyList:
    pub rd_fkeylist: Vec<ForeignKeyCacheInfo>, // list of ForeignKeyCacheInfo
    pub rd_fkeyvalid: bool,                    // true if list has been computed

    // data managed by RelationGetPartitionKey:
    pub rd_partkey: Option<PartitionKey>, // partition key, or None
    pub rd_partkeycxt: MemoryContext,     // private context for rd_partkey, if any

    // data managed by RelationGetPartitionDesc:
    pub rd_partdesc: Option<PartitionDesc>, // partition descriptor, or None
    pub rd_pdcxt: MemoryContext,            // private context for rd_partdesc, if any

    // Same as above, for partdescs that omit detached partitions:
    pub rd_partdesc_nodetached: Option<PartitionDesc>, // partdesc w/o detached parts
    pub rd_pddcxt: MemoryContext,                      // for rd_partdesc_nodetached, if any
    pub rd_partdesc_nodetached_xmin: TransactionId,    // pg_inherits.xmin of the excluded partition

    // data managed by RelationGetPartitionQual:
    pub rd_partcheck: Vec<String>, // partition CHECK quals (node trees)
    pub rd_partcheckvalid: bool,   // true if list has been computed
    pub rd_partcheckcxt: MemoryContext, // private cxt for rd_partcheck, if any

    // data managed by RelationGetIndexList:
    pub rd_indexlist: Vec<Oid>,  // list of OIDs of indexes on relation
    pub rd_pkindex: Oid,         // OID of (deferrable?) primary key, if any
    pub rd_ispkdeferrable: bool, // is rd_pkindex a deferrable PK?
    pub rd_replidindex: Oid,     // OID of replica identity index, if any

    // data managed by RelationGetStatExtList:
    pub rd_statlist: Vec<Oid>, // list of OIDs of extended stats

    // data managed by RelationGetIndexAttrBitmap:
    pub rd_attrsvalid: bool,                   // are bitmaps of attrs valid?
    pub rd_keyattr: Option<Bitmapset>,         // cols that can be ref'd by foreign keys
    pub rd_pkattr: Option<Bitmapset>,          // cols included in primary key
    pub rd_idattr: Option<Bitmapset>,          // included in replica identity index
    pub rd_hotblockingattr: Option<Bitmapset>, // cols blocking HOT update
    pub rd_summarizedattr: Option<Bitmapset>,  // cols indexed by summarizing indexes

    pub rd_pubdesc: *mut PublicationDesc, // publication descriptor, or null // TODO(ptr)

    /// Parsed pg_class.reloptions. Null means "use defaults".
    pub rd_options: *mut bytea, // TODO(ptr)

    /// OID of the handler for this relation (index AM or table AM handler fn).
    pub rd_amhandler: Oid,

    /// Table access method.
    pub rd_tableam: *const TableAmRoutine, // TODO(ptr)

    // Non-null only for an index relation:
    pub rd_index: Form_pg_index, // pg_index tuple describing this index
    pub rd_indextuple: *mut HeapTupleData, // all of pg_index tuple // TODO(ptr)

    // Index access support info (index relation only):
    pub rd_indexcxt: MemoryContext, // private memory cxt for this stuff
    pub rd_indam: *mut IndexAmRoutineHandle, // index AM's API struct // TODO(ptr)
    pub rd_opfamily: *mut Oid,      // OIDs of op families for each index col // TODO(ptr)
    pub rd_opcintype: *mut Oid,     // OIDs of opclass declared input data types // TODO(ptr)
    pub rd_support: *mut RegProcedure, // OIDs of support procedures // TODO(ptr)
    pub rd_supportinfo: *mut FmgrInfo, // lookup info for support procedures // TODO(ptr)
    pub rd_indoption: *mut i16,     // per-column AM-specific flags // TODO(ptr)
    pub rd_indexprs: Vec<String>,   // index expression trees, if any
    pub rd_indpred: Vec<String>,    // index predicate tree, if any
    pub rd_exclops: *mut Oid,       // OIDs of exclusion operators, if any // TODO(ptr)
    pub rd_exclprocs: *mut Oid,     // OIDs of exclusion ops' procs, if any // TODO(ptr)
    pub rd_exclstrats: *mut u16,    // exclusion ops' strategy numbers, if any // TODO(ptr)
    pub rd_indcollation: *mut Oid,  // OIDs of index collations // TODO(ptr)
    pub rd_opcoptions: *mut *mut bytea, // parsed opclass-specific options // TODO(ptr)

    /// Available for index/table AMs to cache private data (reset on inval).
    pub rd_amcache: *mut (), // TODO(ptr)

    /// Cached FDW function pointers, or null (foreign-table support).
    pub rd_fdwroutine: *mut FdwRoutine, // TODO(ptr)

    /// Real TOAST table's OID for CLUSTER/rewrite, or InvalidOid.
    pub rd_toastoid: Oid,

    pub pgstat_enabled: bool, // should relation stats be counted
    pub pgstat_info: *mut PgStat_TableStatus, // statistics collection area // TODO(ptr)
}

// rd_indam points at the index AM API struct. In the routine-struct port the AM
// is the closed enum crate::access::amapi::IndexAmKind, not a thin struct, so
// this stays an opaque raw-pointer target.
pub struct IndexAmRoutineHandle {
    _private: [u8; 0],
}

/// `typedef struct RelationData *Relation;` -- the relcache entry handle. Must
/// match utils/relcache.rs's alias signature.
pub type Relation = *mut RelationData; // TODO(ptr)

pub const InvalidRelation: Relation = core::ptr::null_mut();

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

impl RelationData {
    /// RelationGetToastTupleTarget: toast_tuple_target, or `defaulttarg`.
    pub fn toast_tuple_target(&self, defaulttarg: i32) -> i32 {
        if self.rd_options.is_null() {
            defaulttarg
        } else {
            unsafe { (*(self.rd_options as *const StdRdOptions)).toast_tuple_target }
        }
    }

    /// RelationGetFillFactor: fillfactor, or `defaultff`.
    pub fn fill_factor(&self, defaultff: i32) -> i32 {
        if self.rd_options.is_null() {
            defaultff
        } else {
            unsafe { (*(self.rd_options as *const StdRdOptions)).fillfactor }
        }
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
        if self.rd_options.is_null() {
            return false;
        }
        let relkind = unsafe { (*self.rd_rel).relkind };
        if relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW {
            unsafe { (*(self.rd_options as *const StdRdOptions)).user_catalog_table }
        } else {
            false
        }
    }

    /// RelationGetParallelWorkers: parallel_workers reloption, or `defaultpw`.
    pub fn parallel_workers(&self, defaultpw: i32) -> i32 {
        if self.rd_options.is_null() {
            defaultpw
        } else {
            unsafe { (*(self.rd_options as *const StdRdOptions)).parallel_workers }
        }
    }

    /// RelationIsSecurityView: whether the view is a security barrier view.
    pub fn is_security_view(&self) -> bool {
        if self.rd_options.is_null() {
            false
        } else {
            unsafe { (*(self.rd_options as *const ViewOptions)).security_barrier }
        }
    }

    /// RelationHasSecurityInvoker: whether the view has security_invoker set.
    pub fn has_security_invoker(&self) -> bool {
        if self.rd_options.is_null() {
            false
        } else {
            unsafe { (*(self.rd_options as *const ViewOptions)).security_invoker }
        }
    }

    /// RelationHasCheckOption: view has a local or cascaded check option.
    pub fn has_check_option(&self) -> bool {
        if self.rd_options.is_null() {
            return false;
        }
        unsafe {
            (*(self.rd_options as *const ViewOptions)).check_option
                != ViewOptCheckOption::NOT_SET
        }
    }

    /// RelationHasLocalCheckOption: view defined with the local check option.
    pub fn has_local_check_option(&self) -> bool {
        if self.rd_options.is_null() {
            return false;
        }
        unsafe {
            (*(self.rd_options as *const ViewOptions)).check_option
                == ViewOptCheckOption::LOCAL
        }
    }

    /// RelationHasCascadedCheckOption: view defined with the cascaded check option.
    pub fn has_cascaded_check_option(&self) -> bool {
        if self.rd_options.is_null() {
            return false;
        }
        unsafe {
            (*(self.rd_options as *const ViewOptions)).check_option
                == ViewOptCheckOption::CASCADED
        }
    }

    /// RelationHasReferenceCountZero.
    pub fn has_reference_count_zero(&self) -> bool {
        self.rd_refcnt == 0
    }

    /// RelationGetForm: the pg_class tuple.
    pub fn form(&self) -> Form_pg_class {
        self.rd_rel
    }

    /// RelationGetRelid: the relation's OID.
    pub fn relid(&self) -> Oid {
        self.rd_id
    }

    /// RelationGetNumberOfAttributes.
    pub fn number_of_attributes(&self) -> i16 {
        unsafe { (*self.rd_rel).relnatts }
    }

    /// IndexRelationGetNumberOfAttributes (index relation only).
    pub fn index_number_of_attributes(&self) -> i16 {
        unsafe { (*self.rd_index).indnatts }
    }

    /// IndexRelationGetNumberOfKeyAttributes (index relation only).
    pub fn index_number_of_key_attributes(&self) -> i16 {
        unsafe { (*self.rd_index).indnkeyatts }
    }

    /// RelationGetDescr: the tuple descriptor.
    pub fn descr(&self) -> TupleDesc {
        self.rd_att
    }

    /// RelationGetNamespace: the rel's namespace OID.
    pub fn namespace(&self) -> Oid {
        unsafe { (*self.rd_rel).relnamespace }
    }

    /// RelationIsMapped: uses the relfilenumber map.
    pub fn is_mapped(&self) -> bool {
        let relkind = unsafe { (*self.rd_rel).relkind };
        crate::catalog::pg_class::RELKIND_HAS_STORAGE(relkind)
            && unsafe { (*self.rd_rel).relfilenode } == InvalidRelFileNumber
    }

    /// RelationGetSmgr: open the smgr handle if needed, then return it.
    pub fn smgr(&mut self) -> *mut SmgrRelation {
        if self.rd_smgr.is_null() {
            // smgropen returns an owned handle in this port; pin it.
            let reln = crate::storage::smgr::SmgrRelation::open(self.rd_locator, self.rd_backend);
            self.rd_smgr = Box::into_raw(Box::new(reln));
            unsafe { crate::storage::smgr::smgrpin(&mut *self.rd_smgr) };
        }
        self.rd_smgr
    }

    /// RelationCloseSmgr: close at the smgr level, if open.
    pub fn close_smgr(&mut self) {
        if !self.rd_smgr.is_null() {
            unsafe {
                crate::storage::smgr::smgrunpin(&mut *self.rd_smgr);
                (*self.rd_smgr).close();
                drop(Box::from_raw(self.rd_smgr));
            }
            self.rd_smgr = core::ptr::null_mut();
        }
    }

    /// RelationGetTargetBlock: current insertion target, or InvalidBlockNumber.
    pub fn target_block(&self) -> BlockNumber {
        if self.rd_smgr.is_null() {
            INVALID_BLOCK_NUMBER
        } else {
            unsafe { (*self.rd_smgr).targblock }
        }
    }

    /// RelationSetTargetBlock: set the current insertion target block.
    pub fn set_target_block(&mut self, targblock: BlockNumber) {
        let smgr = self.smgr();
        unsafe { (*smgr).targblock = targblock };
    }

    /// RelationIsPermanent.
    pub fn is_permanent(&self) -> bool {
        (unsafe { (*self.rd_rel).relpersistence }) == RELPERSISTENCE_PERMANENT
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
        (unsafe { (*self.rd_rel).relpersistence }) == RELPERSISTENCE_TEMP
    }

    /// RELATION_IS_LOCAL: temp or newly created in the current transaction.
    pub fn is_local(&self) -> bool {
        self.rd_islocaltemp || self.rd_createSubid != crate::c::InvalidSubTransactionId
    }

    /// RELATION_IS_OTHER_TEMP: a temp relation belonging to another session.
    pub fn is_other_temp(&self) -> bool {
        (unsafe { (*self.rd_rel).relpersistence }) == RELPERSISTENCE_TEMP && !self.rd_islocaltemp
    }

    /// RelationIsScannable: false only for an unpopulated matview.
    pub fn is_scannable(&self) -> bool {
        unsafe { (*self.rd_rel).relispopulated }
    }

    /// RelationIsPopulated.
    pub fn is_populated(&self) -> bool {
        unsafe { (*self.rd_rel).relispopulated }
    }

    /// RelationIsAccessibleInLogicalDecoding.
    pub fn is_accessible_in_logical_decoding(&self) -> bool {
        xlog_logical_info_active()
            && self.needs_wal()
            && (IsCatalogRelation(self.as_relcache_handle()) || self.is_used_as_catalog_table())
    }

    /// RelationIsLogicallyLogged.
    pub fn is_logically_logged(&self) -> bool {
        xlog_logical_info_active()
            && self.needs_wal()
            && unsafe { (*self.rd_rel).relkind } != RELKIND_FOREIGN_TABLE
            && !IsCatalogRelation(self.as_relcache_handle())
    }

    /// Bridge to the level-5 relcache `Relation` handle type, which forward-
    /// declares a placeholder `RelationData`. The two types unify later; until
    /// then catalog.rs's signatures take the relcache handle, so reborrow `self`
    /// through it.
    fn as_relcache_handle(&self) -> crate::utils::relcache::Relation {
        std::ptr::from_ref::<Self>(self).cast_mut()
    }
}

/// RelationGetRelationName: the rel's name (unique within its namespace).
pub fn relation_get_relation_name(relation: &RelationData) -> String {
    let name = unsafe { &(*relation.rd_rel).relname };
    let bytes = crate::c::NameStr(name);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// RelationIsValid: true iff the descriptor handle is non-null.
pub fn relation_is_valid(relation: Relation) -> bool {
    !relation.is_null()
}

// routines in utils/cache/relcache.c
pub fn RelationIncrementReferenceCount(_rel: Relation) {
    unimplemented!()
}

pub fn RelationDecrementReferenceCount(_rel: Relation) {
    unimplemented!()
}
