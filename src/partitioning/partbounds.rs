//! Translated from PostgreSQL src/include/partitioning/partbounds.h
//!
//! This module defines `PartitionBoundInfoData`, resolving the forward decl in
//! crate::partitioning::partdefs.

use crate::fmgr::FmgrInfo;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::JoinType;
use crate::nodes::parsenodes::{
    PartitionBoundSpec, PartitionRangeDatumKind, PartitionStrategy,
};
use crate::nodes::pathnodes::RelOptInfo;
use crate::parser::parse_node::ParseState;
use crate::partitioning::partdefs::{PartitionBoundInfo, PartitionKey};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;

/// PartitionBoundInfoData encapsulates a set of partition bounds. In-memory
/// planner/descriptor state (not on-disk), so modeled idiomatically.
///
/// This is the canonical definition; `crate::partitioning::partdefs` forwards
/// `PartitionBoundInfoData` to here (TODO(struct-forward) in partdefs).
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionBoundInfoData {
    /// hash, list or range?
    pub strategy: PartitionStrategy,
    /// datum-tuples, one per bound (`datums[ndatums][partnatts]` in C)
    pub datums: Vec<Vec<Datum>>,
    /// kind of each range bound datum; None for hash and list
    pub kind: Option<Vec<Vec<PartitionRangeDatumKind>>>,
    /// partition indexes of possibly-interleaved partitions (LIST only)
    pub interleaved_parts: Option<Bitmapset>,
    /// partition indexes
    pub indexes: Vec<i32>,
    /// index of the null-accepting partition; -1 if none
    pub null_index: i32,
    /// index of the default partition; -1 if none
    pub default_index: i32,
}

/// C: `partition_bound_accepts_nulls(bi)`
pub fn partition_bound_accepts_nulls(bi: &PartitionBoundInfoData) -> bool {
    bi.null_index != -1
}

/// C: `partition_bound_has_default(bi)`
pub fn partition_bound_has_default(bi: &PartitionBoundInfoData) -> bool {
    bi.default_index != -1
}

pub fn get_hash_partition_greatest_modulus(_bound: &PartitionBoundInfoData) -> i32 {
    unimplemented!()
}

pub fn compute_partition_hash_value(
    _partnatts: i32,
    _partsupfunc: &[FmgrInfo],
    _partcollation: &[Oid],
    _values: &[Datum],
    _isnull: &[bool],
) -> u64 {
    unimplemented!()
}

pub fn get_qual_from_partbound(
    _parent: Relation,
    _spec: &PartitionBoundSpec,
) -> Vec<Box<crate::nodes::nodes::Node>> {
    unimplemented!()
}

/// Returns the created bound info, plus the per-partition `mapping` out-param.
pub fn partition_bounds_create(
    _boundspecs: &[PartitionBoundSpec],
    _nparts: i32,
    _key: PartitionKey,
) -> (PartitionBoundInfo, Vec<i32>) {
    unimplemented!()
}

pub fn partition_bounds_equal(
    _partnatts: i32,
    _parttyplen: &[i16],
    _parttypbyval: &[bool],
    _b1: &PartitionBoundInfoData,
    _b2: &PartitionBoundInfoData,
) -> bool {
    unimplemented!()
}

pub fn partition_bounds_copy(
    _src: &PartitionBoundInfoData,
    _key: PartitionKey,
) -> PartitionBoundInfo {
    unimplemented!()
}

/// Returns the merged bound info, plus the `outer_parts`/`inner_parts` lists.
pub fn partition_bounds_merge(
    _partnatts: i32,
    _partsupfunc: &[FmgrInfo],
    _partcollation: &[Oid],
    _outer_rel: &RelOptInfo,
    _inner_rel: &RelOptInfo,
    _jointype: JoinType,
) -> Option<(PartitionBoundInfo, Vec<i32>, Vec<i32>)> {
    unimplemented!()
}

pub fn partitions_are_ordered(_boundinfo: &PartitionBoundInfoData, _live_parts: &Bitmapset) -> bool {
    unimplemented!()
}

pub fn check_new_partition_bound(
    _relname: &str,
    _parent: Relation,
    _spec: &PartitionBoundSpec,
    _pstate: &mut ParseState,
) {
    unimplemented!()
}

pub fn check_default_partition_contents(
    _parent: Relation,
    _default_rel: Relation,
    _new_spec: &PartitionBoundSpec,
) {
    unimplemented!()
}

pub fn partition_rbound_datum_cmp(
    _partsupfunc: &[FmgrInfo],
    _partcollation: &[Oid],
    _rb_datums: &[Datum],
    _rb_kind: &[PartitionRangeDatumKind],
    _tuple_datums: &[Datum],
) -> i32 {
    unimplemented!()
}

/// Returns the bound index, plus the `is_equal` out-param.
pub fn partition_list_bsearch(
    _partsupfunc: &[FmgrInfo],
    _partcollation: &[Oid],
    _boundinfo: &PartitionBoundInfoData,
    _value: Datum,
) -> (i32, bool) {
    unimplemented!()
}

/// Returns the bound index, plus the `is_equal` out-param.
pub fn partition_range_datum_bsearch(
    _partsupfunc: &[FmgrInfo],
    _partcollation: &[Oid],
    _boundinfo: &PartitionBoundInfoData,
    _values: &[Datum],
) -> (i32, bool) {
    unimplemented!()
}

pub fn partition_hash_bsearch(
    _boundinfo: &PartitionBoundInfoData,
    _modulus: i32,
    _remainder: i32,
) -> i32 {
    unimplemented!()
}
