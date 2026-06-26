//! Translated from PostgreSQL src/include/utils/partcache.h

use crate::access::attnum::AttrNumber;
use crate::fmgr::FmgrInfo;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::PartitionStrategy;
use crate::postgres_ext::Oid;

// PG's `Expr *` (abstract expression supertype) maps to the Node enum.
use crate::nodes::nodes::Node as Expr;
use crate::utils::relcache::Relation;

/// Information about the partition key of a relation. This is the real body of
/// the `crate::partitioning::partdefs::PartitionKeyData` forward-decl.
pub struct PartitionKeyData {
    /// partitioning strategy
    pub strategy: PartitionStrategy,
    /// number of columns in the partition key
    pub partnatts: i16,
    /// attribute numbers of key columns, 0 if an expr
    pub partattrs: Vec<AttrNumber>,
    /// expressions in the key, one per zero-valued partattrs
    pub partexprs: Vec<Expr>,

    /// OIDs of operator families
    pub partopfamily: Vec<Oid>,
    /// OIDs of opclass declared input data types
    pub partopcintype: Vec<Oid>,
    /// lookup info for support funcs
    pub partsupfunc: Vec<FmgrInfo>,

    /// partitioning collation per attribute
    pub partcollation: Vec<Oid>,

    // Type information per attribute
    pub parttypid: Vec<Oid>,
    pub parttypmod: Vec<i32>,
    pub parttyplen: Vec<i16>,
    pub parttypbyval: Vec<bool>,
    pub parttypalign: Vec<u8>,
    pub parttypcoll: Vec<Oid>,
}

pub fn RelationGetPartitionKey(_rel: Relation) -> Option<&'static PartitionKeyData> {
    unimplemented!()
}
pub fn RelationGetPartitionQual(_rel: Relation) -> Vec<Node> {
    unimplemented!()
}
pub fn get_partition_qual_relid(_relid: Oid) -> Option<Expr> {
    unimplemented!()
}

// PartitionKey inquiry functions (C static inline).
pub fn get_partition_strategy(key: &PartitionKeyData) -> PartitionStrategy {
    key.strategy
}
pub fn get_partition_natts(key: &PartitionKeyData) -> i32 {
    i32::from(key.partnatts)
}
pub fn get_partition_exprs(key: &PartitionKeyData) -> &[Expr] {
    &key.partexprs
}

// One-column inquiry functions.
pub fn get_partition_col_attnum(key: &PartitionKeyData, col: usize) -> i16 {
    key.partattrs[col]
}
pub fn get_partition_col_typid(key: &PartitionKeyData, col: usize) -> Oid {
    key.parttypid[col]
}
pub fn get_partition_col_typmod(key: &PartitionKeyData, col: usize) -> i32 {
    key.parttypmod[col]
}
pub fn get_partition_col_collation(key: &PartitionKeyData, col: usize) -> Oid {
    key.partcollation[col]
}
