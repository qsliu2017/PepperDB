//! Translated from PostgreSQL src/include/utils/typcache.h
//! Type cache definitions. Speeds lookup of type info not directly in pg_type.

use crate::access::tupdesc::TupleDesc;
use crate::fmgr::FmgrInfo;
use crate::nodes::execnodes::DomainConstraintState;
use crate::postgres_ext::Oid;
use crate::utils::palloc::{MemoryContext, MemoryContextCallback};
use bitflags::bitflags;

// DomainConstraintCache is opaque (known only within typcache.c). Local placeholder.
pub struct DomainConstraintCache {
    _private: [u8; 0],
}

// TypeCacheEnumData is opaque (known only within typcache.c). Local placeholder.
pub struct TypeCacheEnumData {
    _private: [u8; 0],
}

/// Per-type cache entry. In-memory (no layout contract). The intrusive `nextDomain`
/// list and the `*TypeCacheEntry` self-references are raw pointers for now; TODO(ptr)
/// once the cache owns its entries (likely a `HashMap<Oid, TypeCacheEntry>`).
pub struct TypeCacheEntry {
    /// OID of the data type (hash lookup key; MUST BE FIRST in C).
    pub type_id: Oid,
    pub type_id_hash: u32,

    // Subsidiary info copied from the pg_type row.
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typstorage: u8,
    pub typtype: u8,
    pub typrelid: Oid,
    pub typsubscript: Oid,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typcollation: Oid,

    // Info from opfamily entries (InvalidOid if no match / not yet requested).
    pub btree_opf: Oid,
    pub btree_opintype: Oid,
    pub hash_opf: Oid,
    pub hash_opintype: Oid,
    pub eq_opr: Oid,
    pub lt_opr: Oid,
    pub gt_opr: Oid,
    pub cmp_proc: Oid,
    pub hash_proc: Oid,
    pub hash_extended_proc: Oid,

    // Pre-set-up fmgr call info, cached to avoid leaks in repeated array_eq etc.
    pub eq_opr_finfo: FmgrInfo,
    pub cmp_proc_finfo: FmgrInfo,
    pub hash_proc_finfo: FmgrInfo,
    pub hash_extended_proc_finfo: FmgrInfo,

    // Tuple descriptor if composite (ref-counted). None if not composite / unset.
    pub tup_desc: Option<TupleDesc>,
    pub tup_desc_identifier: u64,

    // Range-type fields (TYPECACHE_RANGE_INFO). Zeroed if not a range / unset.
    pub rngelemtype: *mut TypeCacheEntry, // range's element type; TODO(ptr)
    pub rng_opfamily: Oid,
    pub rng_collation: Oid,
    pub rng_cmp_proc_finfo: FmgrInfo,
    pub rng_canonical_finfo: FmgrInfo,
    pub rng_subdiff_finfo: FmgrInfo,

    // Multirange field (TYPECACHE_MULTIRANGE_INFO).
    pub rngtype: *mut TypeCacheEntry, // multirange's underlying range type; TODO(ptr)

    // Domain base type/typmod (zeroed if not a domain / unset).
    pub domain_base_type: Oid,
    pub domain_base_typmod: i32,

    // Domain constraint data. None if not domain / no constraints / unset.
    pub domain_data: *mut DomainConstraintCache, // TODO(ptr)

    /// Flags about what has been computed (internal to typcache.c).
    pub flags: TCFlags,

    // Private enum-type info. None if not enum / unset.
    pub enum_data: *mut TypeCacheEnumData, // TODO(ptr)

    // Intrusive list of all known domain-type cache entries.
    pub next_domain: *mut TypeCacheEntry, // TODO(ptr)
}

bitflags! {
    /// Bit flags indicating which fields a caller needs set (the `flags` arg to
    /// `lookup_type_cache`). C: `TYPECACHE_*` request bits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TypeCacheFlags: i32 {
        const EQ_OPR                   = 0x00001;
        const LT_OPR                   = 0x00002;
        const GT_OPR                   = 0x00004;
        const CMP_PROC                 = 0x00008;
        const HASH_PROC                = 0x00010;
        const EQ_OPR_FINFO             = 0x00020;
        const CMP_PROC_FINFO           = 0x00040;
        const HASH_PROC_FINFO          = 0x00080;
        const TUPDESC                  = 0x00100;
        const BTREE_OPFAMILY           = 0x00200;
        const HASH_OPFAMILY            = 0x00400;
        const RANGE_INFO               = 0x00800;
        const DOMAIN_BASE_INFO         = 0x01000;
        const DOMAIN_CONSTR_INFO       = 0x02000;
        const HASH_EXTENDED_PROC       = 0x04000;
        const HASH_EXTENDED_PROC_FINFO = 0x08000;
        const MULTIRANGE_INFO          = 0x10000;
    }
}

bitflags! {
    /// Internal status flags stored in `TypeCacheEntry::flags` (C: `TCFLAGS_*`,
    /// defined in typcache.c). The skeleton models the field as this bitflags set;
    /// the concrete bit values are filled in when typcache.c is ported.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct TCFlags: i32 {
        const _RESERVED = 0;
    }
}

/// This value will not equal any valid tupledesc identifier, nor 0.
pub const INVALID_TUPLEDESC_IDENTIFIER: u64 = 1;

/// Long-lived reference to a domain's constraint set. In-memory.
pub struct DomainConstraintRef {
    pub constraints: Vec<DomainConstraintState>, // DomainConstraintState nodes
    pub refctx: MemoryContext,                   // context holding the ref
    pub tcache: *mut TypeCacheEntry,             // typcache entry for domain; TODO(ptr)
    pub need_exprstate: bool,                    // caller needs check_exprstate?

    // Private to typcache.c.
    pub dcc: *mut DomainConstraintCache, // current constraints, or null; TODO(ptr)
    pub callback: MemoryContextCallback, // releases refcount when done
}

// SharedRecordTypmodRegistry is shared-memory state for record typmods. Single
// process: the shmem backing collapses to owned heap state. Opaque placeholder.
pub struct SharedRecordTypmodRegistry {
    _private: [u8; 0],
}

pub fn lookup_type_cache(_type_id: Oid, _flags: TypeCacheFlags) -> *mut TypeCacheEntry {
    unimplemented!() // TODO(ptr): return a borrow into the owning cache
}

pub fn InitDomainConstraintRef(
    _type_id: Oid,
    _ref_: &mut DomainConstraintRef,
    _refctx: MemoryContext,
    _need_exprstate: bool,
) {
    unimplemented!()
}

pub fn UpdateDomainConstraintRef(_ref_: &mut DomainConstraintRef) {
    unimplemented!()
}

pub fn DomainHasConstraints(_type_id: Oid) -> bool {
    unimplemented!()
}

pub fn lookup_rowtype_tupdesc(_type_id: Oid, _typmod: i32) -> TupleDesc {
    unimplemented!()
}

pub fn lookup_rowtype_tupdesc_noerror(
    _type_id: Oid,
    _typmod: i32,
    _no_error: bool,
) -> Option<TupleDesc> {
    unimplemented!()
}

pub fn lookup_rowtype_tupdesc_copy(_type_id: Oid, _typmod: i32) -> TupleDesc {
    unimplemented!()
}

pub fn lookup_rowtype_tupdesc_domain(
    _type_id: Oid,
    _typmod: i32,
    _no_error: bool,
) -> Option<TupleDesc> {
    unimplemented!()
}

pub fn assign_record_type_typmod(_tup_desc: TupleDesc) {
    unimplemented!()
}

pub fn assign_record_type_identifier(_type_id: Oid, _typmod: i32) -> u64 {
    unimplemented!()
}

/// Compare two enum values. Returns <0, 0, >0 (C qsort-style 3-way result).
pub fn compare_values_of_enum(_tcache: &TypeCacheEntry, _arg1: Oid, _arg2: Oid) -> i32 {
    unimplemented!()
}

// Shared-memory registry ops. Single-process: the dsm_segment/dsa_area params drop.
pub fn SharedRecordTypmodRegistryEstimate() -> usize {
    unimplemented!()
}

pub fn SharedRecordTypmodRegistryInit(_registry: &mut SharedRecordTypmodRegistry) {
    unimplemented!()
}

pub fn SharedRecordTypmodRegistryAttach(_registry: &mut SharedRecordTypmodRegistry) {
    unimplemented!()
}

pub fn AtEOXact_TypeCache() {
    unimplemented!()
}

pub fn AtEOSubXact_TypeCache() {
    unimplemented!()
}
