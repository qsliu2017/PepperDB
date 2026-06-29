//! Translated from PostgreSQL src/include/utils/relcache.h
//! Relation descriptor cache definitions.

use std::sync::Arc;

use crate::access::tupdesc::TupleDesc;
use crate::c::SubTransactionId;
use crate::catalog::pg_publication::PublicationDesc;
use crate::common::relpath::RelFileNumber;
use crate::nodes::bitmapset::Bitmapset;
use crate::postgres_ext::Oid;
pub use crate::utils::rel::RelationData;

// C: `typedef struct RelationData *Relation;` (a relcache entry handle) and
// `typedef Relation *RelationPtr;` (an array of relations, executor index
// scans). Both handle aliases are retired: holders write `Arc<RelationData>`
// (the shared, reference-counted owner; see [`crate::utils::rel`]) /
// `&RelationData` / `Option<Arc<RelationData>>` explicitly, and an array of
// relations is a `Vec<Arc<RelationData>>` / `&[Arc<RelationData>]`.

/// Name of relcache init file(s), used to speed up backend startup.
pub const RELCACHE_INIT_FILENAME: &str = "pg_internal.init";

/// No-op outside assert builds; here a plain stub.
pub fn AssertCouldGetRelation() {}

// Routines to open (lookup) and close a relcache entry. Lookup can miss -> Option.
// Bodies in crate::backend::utils::cache::relcache (step 14).
pub use crate::backend::utils::cache::relcache::relation_id_get_relation as RelationIdGetRelation;
pub use crate::backend::utils::cache::relcache::relation_close as RelationClose;

// Routines to compute/retrieve additional cached information.
// C `List *` returns map to `Vec<T>` per the container table; element types per use.

/// List of ForeignKeyCacheInfo.
pub fn RelationGetFKeyList(_relation: &RelationData) -> Vec<Oid> {
    unimplemented!()
}

/// OIDs of indexes on the relation.
pub fn RelationGetIndexList(_relation: &RelationData) -> Vec<Oid> {
    unimplemented!()
}

/// OIDs of extended statistics objects.
pub fn RelationGetStatExtList(_relation: &RelationData) -> Vec<Oid> {
    unimplemented!()
}

/// OID of the (deferrable?) primary key index. InvalidOid sentinel -> None.
pub fn RelationGetPrimaryKeyIndex(_relation: &RelationData, _deferrable_ok: bool) -> Option<Oid> {
    unimplemented!()
}

/// OID of the replica identity index. InvalidOid sentinel -> None.
pub fn RelationGetReplicaIndex(_relation: &RelationData) -> Option<Oid> {
    unimplemented!()
}

/// Index expression trees (one per indexed expression).
pub fn RelationGetIndexExpressions(_relation: &RelationData) -> Vec<String> {
    unimplemented!()
}

pub fn RelationGetDummyIndexExpressions(_relation: &RelationData) -> Vec<String> {
    unimplemented!()
}

/// Index predicate tree.
pub fn RelationGetIndexPredicate(_relation: &RelationData) -> Vec<String> {
    unimplemented!()
}

/// Parsed per-column opclass-specific options (one varlena per index column).
pub fn RelationGetIndexAttOptions(_relation: &RelationData, _copy: bool) -> Vec<Option<Vec<u8>>> {
    unimplemented!()
}

/// Which set of columns to return by RelationGetIndexAttrBitmap.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    KEY,
    PRIMARY_KEY,
    IDENTITY_KEY,
    HOT_BLOCKING,
    SUMMARIZED,
}

pub fn RelationGetIndexAttrBitmap(
    _relation: &RelationData,
    _attr_kind: IndexAttrBitmapKind,
) -> Bitmapset {
    unimplemented!()
}

pub fn RelationGetIdentityKeyBitmap(_relation: &RelationData) -> Bitmapset {
    unimplemented!()
}

/// Exclusion-constraint info: returns (operators, procs, strategies) (C out-params).
pub fn RelationGetExclusionInfo(_index_relation: &RelationData) -> (Vec<Oid>, Vec<Oid>, Vec<u16>) {
    unimplemented!()
}

pub use crate::backend::utils::cache::relcache::relation_init_index_access_info as RelationInitIndexAccessInfo;

pub fn RelationBuildPublicationDesc(_relation: &RelationData, _pubdesc: &mut PublicationDesc) {
    unimplemented!()
}

pub fn RelationInitTableAccessMethod(_relation: &RelationData) {
    unimplemented!()
}

// Routines to support ereport() reports of relation-related errors. These return
// an int "dummy" value used by errcode-chaining in C; kept as i32 stubs.
pub fn errtable(_rel: &RelationData) -> i32 {
    unimplemented!()
}

pub fn errtablecol(_rel: &RelationData, _attnum: i32) -> i32 {
    unimplemented!()
}

pub fn errtablecolname(_rel: &RelationData, _colname: &str) -> i32 {
    unimplemented!()
}

pub fn errtableconstraint(_rel: &RelationData, _conname: &str) -> i32 {
    unimplemented!()
}

// Routines for backend startup.
pub use crate::backend::utils::cache::relcache::relation_cache_initialize as RelationCacheInitialize;

/// PG `RelationCacheInitializePhase2`: fake up relcache entries for the nailed
/// SHARED catalogs (pg_database/pg_authid/...). Those are deep-deferred; when this
/// lands it calls `crate::bootstrap::bootstrap::formrdesc` per shared
/// `BootstrapCatalog`, using the descriptor it returns as the nailed entry's
/// `rd_att`.
pub fn RelationCacheInitializePhase2() {
    unimplemented!()
}

/// PG `RelationCacheInitializePhase3`: fake up relcache entries for the nailed
/// LOCAL catalogs (pg_class/pg_attribute/pg_proc/pg_type), then load the real
/// pg_class rows. The fake-up step is `crate::bootstrap::bootstrap::formrdesc`
/// over `crate::bootstrap::bootstrap::FORMRDESC_CATALOGS`; step 14 wraps the
/// returned `TupleDesc` in a nailed `RelationData` (see the formrdesc doc for the
/// staged `rd_rel`/RelationCacheInsert wiring).
pub use crate::backend::utils::cache::relcache::relation_cache_initialize_phase3 as RelationCacheInitializePhase3;

/// Create a relcache entry for an about-to-be-created relation.
pub fn RelationBuildLocalRelation(
    _relname: &str,
    _relnamespace: Oid,
    _tup_desc: TupleDesc,
    _relid: Oid,
    _accessmtd: Oid,
    _relfilenumber: RelFileNumber,
    _reltablespace: Oid,
    _shared_relation: bool,
    _mapped_relation: bool,
    _relpersistence: u8,
    _relkind: u8,
) -> Arc<RelationData> {
    unimplemented!()
}

// Routines to manage assignment of new relfilenumber to a relation.
pub fn RelationSetNewRelfilenumber(_relation: &RelationData, _persistence: u8) {
    unimplemented!()
}

pub fn RelationAssumeNewRelfilelocator(_relation: &RelationData) {
    unimplemented!()
}

// Routines for flushing/rebuilding relcache entries.
pub fn RelationForgetRelation(_rid: Oid) {
    unimplemented!()
}

pub fn RelationCacheInvalidateEntry(_relation_id: Oid) {
    unimplemented!()
}

pub fn RelationCacheInvalidate(_debug_discard: bool) {
    unimplemented!()
}

/// No-op outside assert builds.
pub fn AssertPendingSyncs_RelationCache() {}

pub fn AtEOXact_RelationCache(_is_commit: bool) {
    unimplemented!()
}

pub fn AtEOSubXact_RelationCache(
    _is_commit: bool,
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
) {
    unimplemented!()
}

// Routines to help manage rebuilding of relcache init files.
pub fn RelationIdIsInInitFile(_relation_id: Oid) -> bool {
    unimplemented!()
}

pub fn RelationCacheInitFilePreInvalidate() {
    unimplemented!()
}

pub fn RelationCacheInitFilePostInvalidate() {
    unimplemented!()
}

pub fn RelationCacheInitFileRemove() {
    unimplemented!()
}

// Globals (relcache.c / catcache.c / postinit.c). TODO(state): become Session state.
pub static mut criticalRelcachesBuilt: bool = false;
pub static mut criticalSharedRelcachesBuilt: bool = false;
