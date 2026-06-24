//! Translated from PostgreSQL src/include/partitioning/partdefs.h
//! Base definitions for partitioned table handling.
//!
//! These are all opaque forward declarations whose real struct bodies live in
//! other partitioning headers. TODO(struct-forward).

#[deprecated(note = "TODO(struct-forward): repoint to crate::partitioning::partbounds::PartitionBoundInfoData in Phase 2")]
pub struct PartitionBoundInfoData;
#[allow(deprecated)]
pub type PartitionBoundInfo = Box<PartitionBoundInfoData>; // TODO(ptr)

#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::partcache::PartitionKeyData in Phase 2")]
pub struct PartitionKeyData;
#[allow(deprecated)]
pub type PartitionKey = Box<PartitionKeyData>; // TODO(ptr)

#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::PartitionBoundSpec in Phase 2")]
pub struct PartitionBoundSpec;

#[deprecated(note = "TODO(struct-forward): repoint to crate::partitioning::partdesc::PartitionDescData in Phase 2")]
pub struct PartitionDescData;
#[allow(deprecated)]
pub type PartitionDesc = Box<PartitionDescData>; // TODO(ptr)

#[deprecated(note = "TODO(struct-forward): repoint to crate::partitioning::partdesc::PartitionDirectoryData in Phase 2")]
pub struct PartitionDirectoryData;
#[allow(deprecated)]
pub type PartitionDirectory = Box<PartitionDirectoryData>; // TODO(ptr)
