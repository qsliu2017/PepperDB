//! Translated from PostgreSQL src/include/catalog/partition.h

#![allow(
    clippy::needless_pass_by_value,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;

/// Seed for the extended hash function.
pub const HASH_PARTITION_SEED: u64 = 0x7A5B22367996DCFD;

/// InvalidOid sentinel -> None when the relation has no parent.
pub fn get_partition_parent(relid: Oid, even_if_detached: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_partition_ancestors(relid: Oid) -> Vec<Oid> {
    unimplemented!()
}

pub fn index_get_partition(partition: &RelationData, index_id: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn map_partition_varattnos(
    expr: Vec<Node>,
    fromrel_varno: i32,
    to_rel: &RelationData,
    from_rel: &RelationData,
) -> Vec<Node> {
    unimplemented!()
}

/// C: bool return + `bool *used_in_expr` out-param -> (found, used_in_expr).
pub fn has_partition_attrs(rel: &RelationData, attnums: &Bitmapset) -> (bool, bool) {
    unimplemented!()
}

pub fn get_default_partition_oid(parent_id: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn update_default_partition_oid(parent_id: Oid, default_part_id: Oid) {
    unimplemented!()
}

pub fn get_proposed_default_constraint(new_part_constraints: Vec<Node>) -> Vec<Node> {
    unimplemented!()
}
