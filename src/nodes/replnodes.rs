//! Translation of postgres/src/include/nodes/replnodes.h
//!
//! Definitions for replication grammar parse nodes (the replication-command
//! protocol: IDENTIFY_SYSTEM, BASE_BACKUP, CREATE/DROP/ALTER_REPLICATION_SLOT,
//! START_REPLICATION, etc.).

use crate::prelude::*;
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::List;
use core::ffi::c_char;

// access/xlogdefs.h scalar typedefs (that header is not yet translated).
// TODO(pg-port): translate access/xlogdefs.h.
pub type TimeLineID = uint32;
pub type XLogRecPtr = uint64;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicationKind {
    REPLICATION_KIND_PHYSICAL,
    REPLICATION_KIND_LOGICAL,
}
pub use ReplicationKind::*;

/// IDENTIFY_SYSTEM command
#[repr(C)]
pub struct IdentifySystemCmd {
    pub r#type: NodeTag,
}

/// BASE_BACKUP command
#[repr(C)]
pub struct BaseBackupCmd {
    pub r#type: NodeTag,
    pub options: *mut List,
}

/// CREATE_REPLICATION_SLOT command
#[repr(C)]
pub struct CreateReplicationSlotCmd {
    pub r#type: NodeTag,
    pub slotname: *mut c_char,
    pub kind: ReplicationKind,
    pub plugin: *mut c_char,
    pub temporary: bool,
    pub options: *mut List,
}

/// DROP_REPLICATION_SLOT command
#[repr(C)]
pub struct DropReplicationSlotCmd {
    pub r#type: NodeTag,
    pub slotname: *mut c_char,
    pub wait: bool,
}

/// ALTER_REPLICATION_SLOT command
#[repr(C)]
pub struct AlterReplicationSlotCmd {
    pub r#type: NodeTag,
    pub slotname: *mut c_char,
    pub options: *mut List,
}

/// START_REPLICATION command
#[repr(C)]
pub struct StartReplicationCmd {
    pub r#type: NodeTag,
    pub kind: ReplicationKind,
    pub slotname: *mut c_char,
    pub timeline: TimeLineID,
    pub startpoint: XLogRecPtr,
    pub options: *mut List,
}

/// READ_REPLICATION_SLOT command
#[repr(C)]
pub struct ReadReplicationSlotCmd {
    pub r#type: NodeTag,
    pub slotname: *mut c_char,
}

/// TIMELINE_HISTORY command
#[repr(C)]
pub struct TimeLineHistoryCmd {
    pub r#type: NodeTag,
    pub timeline: TimeLineID,
}

/// UPLOAD_MANIFEST command
#[repr(C)]
pub struct UploadManifestCmd {
    pub r#type: NodeTag,
}
