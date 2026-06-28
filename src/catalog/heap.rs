//! Translated from PostgreSQL src/include/catalog/heap.h

#![allow(
    clippy::boxed_local,
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use bitflags::bitflags;

use crate::access::attnum::AttrNumber;
use crate::access::tupdesc::TupleDesc;
use crate::c::{MultiXactId, TransactionId};
use crate::catalog::indexing::CatalogIndexState;
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_attribute::{FormData_pg_attribute, FormExtraData_pg_attribute};
use crate::common::relpath::RelFileNumber;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{ConstrType, PartitionBoundSpec};
use crate::nodes::primnodes::OnCommitAction;
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

bitflags! {
    /// CHKATYPE_* flags for CheckAttributeType/CheckAttributeNamesTypes.
    /// GOOD: clean single-bit set.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ChkAType: i32 {
        const ANYARRAY    = 0x01; // allow ANYARRAY
        const ANYRECORD   = 0x02; // allow RECORD and RECORD[]
        const IS_PARTKEY  = 0x04; // attname is part key # not column
        const IS_VIRTUAL  = 0x08; // is virtual generated column
    }
}

/// Untransformed default for a column.
pub struct RawColumnDefault {
    pub attnum: AttrNumber,         // attribute to attach default to
    pub raw_default: Node,     // default value (untransformed parse tree)
    pub generated: u8,              // attgenerated setting (was char)
}

/// A transformed (cooked) constraint or default.
pub struct CookedConstraint {
    pub contype: ConstrType,    // DEFAULT, CHECK, NOTNULL
    pub conoid: Oid,            // constr OID if created, else Invalid
    pub name: Option<String>,   // name, or None if none
    pub attnum: AttrNumber,     // which attr (only for NOTNULL, DEFAULT)
    pub expr: Node,        // transformed default or check expr
    pub is_enforced: bool,      // is enforced? (only for CHECK)
    pub skip_validation: bool,  // skip validation? (only for CHECK)
    pub is_local: bool,         // constraint has local (non-inherited) def
    pub inhcount: i16,          // number of times constraint is inherited
    pub is_no_inherit: bool,    // local def and cannot be inherited
}

// The storage-level `heap_create` and the `heap_create_with_catalog` orchestrator
// are implemented in `crate::backend::catalog::heap` (M2 form: a plain heap table;
// the `&Arc<SharedState>` thread is the foundation convention, and the toast /
// array-type / defaults / mapped-relation parameters are staged -- rules.md s4/s5).
pub use crate::backend::catalog::heap::{
    heap_create, heap_create_with_catalog,
};

pub fn heap_drop_with_catalog(_relid: Oid) {
    unimplemented!()
}

pub fn heap_truncate(_relids: Vec<Oid>) {
    unimplemented!()
}

pub fn heap_truncate_one_rel(_rel: Relation) {
    unimplemented!()
}

pub fn heap_truncate_check_FKs(_relations: Vec<Relation>, _temp_tables: bool) {
    unimplemented!()
}

pub fn heap_truncate_find_FKs(_relation_ids: Vec<Oid>) -> Vec<Oid> {
    unimplemented!()
}

pub fn InsertPgAttributeTuples(
    _pg_attribute_rel: Relation,
    _tupdesc: TupleDesc,
    _new_rel_oid: Oid,
    _tupdesc_extra: &[FormExtraData_pg_attribute],
    _indstate: CatalogIndexState,
) {
    unimplemented!()
}

pub fn InsertPgClassTuple(
    _pg_class_desc: Relation,
    _new_rel_desc: Relation,
    _new_rel_oid: Oid,
    _relacl: Datum,
    _reloptions: Datum,
) {
    unimplemented!()
}

pub fn AddRelationNewConstraints(
    _rel: Relation,
    _new_col_defaults: Vec<RawColumnDefault>,
    _new_constraints: Vec<Node>,
    _allow_merge: bool,
    _is_local: bool,
    _is_internal: bool,
    _query_string: &str,
) -> Vec<CookedConstraint> {
    unimplemented!()
}

pub fn AddRelationNotNullConstraints(
    _rel: Relation,
    _constraints: Vec<Node>,
    _old_notnulls: Vec<CookedConstraint>,
    _existing_constraints: Vec<CookedConstraint>,
) -> Vec<CookedConstraint> {
    unimplemented!()
}

pub fn RelationClearMissing(_rel: Relation) {
    unimplemented!()
}

pub fn StoreAttrMissingVal(_rel: Relation, _attnum: AttrNumber, _missingval: Datum) {
    unimplemented!()
}

pub fn SetAttrMissing(_relid: Oid, _attname: &str, _value: &str) {
    unimplemented!()
}

pub fn cookDefault(
    _pstate: &mut ParseState,
    _raw_default: Node,
    _atttypid: Oid,
    _atttypmod: i32,
    _attname: &str,
    _attgenerated: u8,
) -> Node {
    unimplemented!()
}

pub fn DeleteRelationTuple(_relid: Oid) {
    unimplemented!()
}

pub fn DeleteAttributeTuples(_relid: Oid) {
    unimplemented!()
}

pub fn DeleteSystemAttributeTuples(_relid: Oid) {
    unimplemented!()
}

pub fn RemoveAttributeById(_relid: Oid, _attnum: AttrNumber) {
    unimplemented!()
}

pub fn CopyStatistics(_fromrelid: Oid, _torelid: Oid) {
    unimplemented!()
}

pub fn RemoveStatistics(_relid: Oid, _attnum: AttrNumber) {
    unimplemented!()
}

/// Returns the static definition for a system attribute by number.
pub fn SystemAttributeDefinition(_attno: AttrNumber) -> &'static FormData_pg_attribute {
    unimplemented!()
}

/// System attribute by name; None if no such attribute.
pub fn SystemAttributeByName(_attname: &str) -> Option<&'static FormData_pg_attribute> {
    unimplemented!()
}

pub fn CheckAttributeNamesTypes(_tupdesc: TupleDesc, _relkind: u8, _flags: ChkAType) {
    unimplemented!()
}

pub fn CheckAttributeType(
    _attname: &str,
    _atttypid: Oid,
    _attcollation: Oid,
    _containing_rowtypes: Vec<Oid>,
    _flags: ChkAType,
) {
    unimplemented!()
}

// pg_partitioned_table catalog manipulation functions

pub fn StorePartitionKey(
    _rel: Relation,
    _strategy: u8,
    _partnatts: i16,
    _partattrs: &[AttrNumber],
    _partexprs: Vec<Node>,
    _partopclass: &[Oid],
    _partcollation: &[Oid],
) {
    unimplemented!()
}

pub fn RemovePartitionKeyByRelId(_relid: Oid) {
    unimplemented!()
}

pub fn StorePartitionBound(_rel: Relation, _parent: Relation, _bound: &PartitionBoundSpec) {
    unimplemented!()
}
