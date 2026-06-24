//! Translated from PostgreSQL src/include/partitioning/partdesc.h

use crate::partitioning::partdefs::{PartitionBoundInfo, PartitionDesc, PartitionDirectory};
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;
use crate::utils::relcache::Relation;

/// Information about partitions of a partitioned table.
///
/// Resolves the `crate::partitioning::partdefs::PartitionDescData` forward decl.
pub struct PartitionDescData {
    /// Number of partitions
    pub nparts: i32,
    /// Are there any detached partitions?
    pub detached_exist: bool,
    /// 'nparts' partition OIDs in order of their bounds
    pub oids: Vec<Oid>,
    /// 'nparts' flags: is each oids[] element a leaf partition?
    pub is_leaf: Vec<bool>,
    /// collection of partition bounds
    pub boundinfo: PartitionBoundInfo,

    // Caching fields to cache lookups in get_partition_for_tuple():
    /// Index into the PartitionBoundInfo's datum array for the last found
    /// partition or -1 if none.
    pub last_found_datum_index: i32,
    /// Partition index of the last found partition or -1 if none yet.
    pub last_found_part_index: i32,
    /// Run-length of consecutive matches at last_found_datum_index.
    pub last_found_count: i32,
}

pub fn relation_get_partition_desc(_rel: Relation, _omit_detached: bool) -> PartitionDesc {
    unimplemented!()
}

pub fn create_partition_directory(
    _mcxt: MemoryContext,
    _omit_detached: bool,
) -> PartitionDirectory {
    unimplemented!()
}

pub fn partition_directory_lookup(_pdir: PartitionDirectory, _rel: Relation) -> PartitionDesc {
    unimplemented!()
}

pub fn destroy_partition_directory(_pdir: PartitionDirectory) {
    unimplemented!()
}

pub fn get_default_oid_from_partdesc(_partdesc: &PartitionDescData) -> Oid {
    unimplemented!()
}
