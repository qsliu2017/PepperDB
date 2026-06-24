//! Translated from PostgreSQL src/include/utils/relcache.h
//! Relation descriptor cache definitions.

use crate::access::tupdesc::TupleDesc;
use crate::c::SubTransactionId;
use crate::common::relpath::RelFileNumber;
use crate::nodes::bitmapset::Bitmapset;
use crate::postgres_ext::Oid;

// RelationData's full (large, in-memory) definition lives in utils/rel.h (a later
// level). Rule 7: opaque local placeholder, repointed in Phase 2. `Relation` is a
// handle to it; modeled as a raw pointer (ownership is the relcache's, not the
// caller's). TODO(ptr): becomes a borrow/Arc once rel.rs lands.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::rel::RelationData in Phase 2")]
pub struct RelationData {
    _private: [u8; 0],
}

/// C: `typedef struct RelationData *Relation;` -- a relcache entry handle.
#[allow(deprecated)]
pub type Relation = *mut RelationData; // TODO(ptr)

/// C: `typedef Relation *RelationPtr;` -- array of relations (executor index scans).
pub type RelationPtr = *mut Relation; // TODO(ptr)

/// Name of relcache init file(s), used to speed up backend startup.
pub const RELCACHE_INIT_FILENAME: &str = "pg_internal.init";

/// No-op outside assert builds; here a plain stub.
pub fn AssertCouldGetRelation() {}

// Routines to open (lookup) and close a relcache entry. Lookup can miss -> Option.
pub fn RelationIdGetRelation(_relation_id: Oid) -> Option<Relation> {
    unimplemented!()
}

pub fn RelationClose(_relation: Relation) {
    unimplemented!()
}

// Routines to compute/retrieve additional cached information.
// C `List *` returns map to `Vec<T>` per the container table; element types per use.

/// List of ForeignKeyCacheInfo. TODO(struct-forward): element type once defined.
pub fn RelationGetFKeyList(_relation: Relation) -> Vec<Oid> {
    unimplemented!()
}

/// OIDs of indexes on the relation.
pub fn RelationGetIndexList(_relation: Relation) -> Vec<Oid> {
    unimplemented!()
}

/// OIDs of extended statistics objects.
pub fn RelationGetStatExtList(_relation: Relation) -> Vec<Oid> {
    unimplemented!()
}

/// OID of the (deferrable?) primary key index. InvalidOid sentinel -> None.
pub fn RelationGetPrimaryKeyIndex(_relation: Relation, _deferrable_ok: bool) -> Option<Oid> {
    unimplemented!()
}

/// OID of the replica identity index. InvalidOid sentinel -> None.
pub fn RelationGetReplicaIndex(_relation: Relation) -> Option<Oid> {
    unimplemented!()
}

/// Index expression trees (one per indexed expression).
pub fn RelationGetIndexExpressions(_relation: Relation) -> Vec<String> {
    unimplemented!()
}

pub fn RelationGetDummyIndexExpressions(_relation: Relation) -> Vec<String> {
    unimplemented!()
}

/// Index predicate tree.
pub fn RelationGetIndexPredicate(_relation: Relation) -> Vec<String> {
    unimplemented!()
}

/// Parsed per-column opclass-specific options (one varlena per index column).
pub fn RelationGetIndexAttOptions(_relation: Relation, _copy: bool) -> Vec<Option<Vec<u8>>> {
    unimplemented!()
}

/// Which set of columns to return by RelationGetIndexAttrBitmap.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    INDEX_ATTR_BITMAP_KEY,
    INDEX_ATTR_BITMAP_PRIMARY_KEY,
    INDEX_ATTR_BITMAP_IDENTITY_KEY,
    INDEX_ATTR_BITMAP_HOT_BLOCKING,
    INDEX_ATTR_BITMAP_SUMMARIZED,
}

pub fn RelationGetIndexAttrBitmap(
    _relation: Relation,
    _attr_kind: IndexAttrBitmapKind,
) -> Bitmapset {
    unimplemented!()
}

pub fn RelationGetIdentityKeyBitmap(_relation: Relation) -> Bitmapset {
    unimplemented!()
}

/// Exclusion-constraint info: returns (operators, procs, strategies) (C out-params).
pub fn RelationGetExclusionInfo(_index_relation: Relation) -> (Vec<Oid>, Vec<Oid>, Vec<u16>) {
    unimplemented!()
}

pub fn RelationInitIndexAccessInfo(_relation: Relation) {
    unimplemented!()
}

// PublicationDesc's definition lives in catalog/pg_publication.h (later level).
// Rule 7: opaque local placeholder, repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::catalog::pg_publication::PublicationDesc in Phase 2")]
pub struct PublicationDesc {
    _private: [u8; 0],
}

#[allow(deprecated)]
pub fn RelationBuildPublicationDesc(_relation: Relation, _pubdesc: &mut PublicationDesc) {
    unimplemented!()
}

pub fn RelationInitTableAccessMethod(_relation: Relation) {
    unimplemented!()
}

// Routines to support ereport() reports of relation-related errors. These return
// an int "dummy" value used by errcode-chaining in C; kept as i32 stubs.
pub fn errtable(_rel: Relation) -> i32 {
    unimplemented!()
}

pub fn errtablecol(_rel: Relation, _attnum: i32) -> i32 {
    unimplemented!()
}

pub fn errtablecolname(_rel: Relation, _colname: &str) -> i32 {
    unimplemented!()
}

pub fn errtableconstraint(_rel: Relation, _conname: &str) -> i32 {
    unimplemented!()
}

// Routines for backend startup.
pub fn RelationCacheInitialize() {
    unimplemented!()
}

pub fn RelationCacheInitializePhase2() {
    unimplemented!()
}

pub fn RelationCacheInitializePhase3() {
    unimplemented!()
}

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
) -> Relation {
    unimplemented!()
}

// Routines to manage assignment of new relfilenumber to a relation.
pub fn RelationSetNewRelfilenumber(_relation: Relation, _persistence: u8) {
    unimplemented!()
}

pub fn RelationAssumeNewRelfilelocator(_relation: Relation) {
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
