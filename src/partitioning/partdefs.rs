//! Translated from PostgreSQL src/include/partitioning/partdefs.h
//! Base definitions for partitioned table handling.
//!
//! These are forward declarations whose real struct bodies live in other
//! partitioning headers; the pointer typedefs are re-exported from here.

use crate::partitioning::partbounds::PartitionBoundInfoData;
use crate::partitioning::partdesc::PartitionDescData;
use crate::utils::partcache::PartitionKeyData;

pub use crate::nodes::parsenodes::PartitionBoundSpec;

pub type PartitionBoundInfo = Box<PartitionBoundInfoData>; // TODO(ptr)

pub type PartitionKey = Box<PartitionKeyData>; // TODO(ptr)

pub type PartitionDesc = Box<PartitionDescData>; // TODO(ptr)

/// Opaque; PartitionDirectoryData body (partcache.c-private) not ported.
pub struct PartitionDirectoryData;
pub type PartitionDirectory = Box<PartitionDirectoryData>; // TODO(ptr)
