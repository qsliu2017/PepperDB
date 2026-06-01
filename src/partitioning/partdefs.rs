//! partitioning/partdefs.h - Base definitions for partitioned table handling.

use std::ffi::c_void;

// All of the following are forward declarations of structs defined in other
// (as-yet-unported) headers. Stub them locally as opaque types.
// TODO: dedup when partbounds.h / partcache.h / partdesc.h / parsenodes.h land.

/// struct PartitionBoundInfoData (defined in partbounds.c)
pub type PartitionBoundInfoData = c_void;
pub type PartitionBoundInfo = *mut PartitionBoundInfoData;

/// struct PartitionKeyData (defined in partcache.h)
pub type PartitionKeyData = c_void;
pub type PartitionKey = *mut PartitionKeyData;

/// struct PartitionBoundSpec (defined in parsenodes.h)
pub type PartitionBoundSpec = c_void;

/// struct PartitionDescData (defined in partdesc.h)
pub type PartitionDescData = c_void;
pub type PartitionDesc = *mut PartitionDescData;

/// struct PartitionDirectoryData (defined in partdesc.h)
pub type PartitionDirectoryData = c_void;
pub type PartitionDirectory = *mut PartitionDirectoryData;
