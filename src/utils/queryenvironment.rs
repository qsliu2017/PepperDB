//! Translated from PostgreSQL src/include/utils/queryenvironment.h
//! Access to functions to mutate the query environment and retrieve ENR data.

use crate::access::tupdesc::TupleDesc;
use crate::postgres_ext::Oid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EphemeralNameRelationType {
    ENR_NAMED_TUPLESTORE, // named tuplestore relation; e.g., deltas
}

/// Metadata for an ephemeral named relation. Either `reliddesc` (oid of a
/// relation to get the tupdesc from) or `tupdesc` is used, never both.
pub struct EphemeralNamedRelationMetadataData {
    pub name: String,                    // name used to identify the relation
    pub reliddesc: Oid,                  // oid of relation to get tupdesc
    pub tupdesc: TupleDesc,              // description of result rows
    pub enrtype: EphemeralNameRelationType, // to identify type of relation
    pub enrtuples: f64,                  // estimated number of tuples
}

pub type EphemeralNamedRelationMetadata = *mut EphemeralNamedRelationMetadataData; // TODO(ptr)

/// Ephemeral Named Relation data; used for parsing named relations not in the
/// catalog, like transition tables in AFTER triggers.
pub struct EphemeralNamedRelationData {
    pub md: EphemeralNamedRelationMetadataData,
    pub reldata: *mut (), // execution-time access structure; void *. TODO(ptr)
}

pub type EphemeralNamedRelation = *mut EphemeralNamedRelationData; // TODO(ptr)

/// Opaque outside queryenvironment.c; implementation may change without
/// touching callers.
pub struct QueryEnvironment {
    _private: [u8; 0],
}

pub fn create_queryEnv() -> *mut QueryEnvironment {
    unimplemented!()
}

pub fn get_visible_ENR_metadata(
    _query_env: &mut QueryEnvironment,
    _refname: &str,
) -> EphemeralNamedRelationMetadata {
    unimplemented!()
}

pub fn register_ENR(_query_env: &mut QueryEnvironment, _enr: EphemeralNamedRelation) {
    unimplemented!()
}

pub fn unregister_ENR(_query_env: &mut QueryEnvironment, _name: &str) {
    unimplemented!()
}

pub fn get_ENR(_query_env: &mut QueryEnvironment, _name: &str) -> EphemeralNamedRelation {
    unimplemented!()
}

pub fn ENRMetadataGetTupDesc(_enrmd: EphemeralNamedRelationMetadata) -> TupleDesc {
    unimplemented!()
}
