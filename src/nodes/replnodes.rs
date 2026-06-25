//! Translated from PostgreSQL src/include/nodes/replnodes.h

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::nodes::nodes::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationKind {
    PHYSICAL,
    LOGICAL,
}

/// IDENTIFY_SYSTEM command.
#[derive(Debug, Clone, PartialEq)]
pub struct IdentifySystemCmd;

/// BASE_BACKUP command.
#[derive(Debug, Clone, PartialEq)]
pub struct BaseBackupCmd {
    pub options: Vec<Box<Node>>,
}

/// CREATE_REPLICATION_SLOT command.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateReplicationSlotCmd {
    pub slotname: Option<String>,
    pub kind: ReplicationKind,
    pub plugin: Option<String>,
    pub temporary: bool,
    pub options: Vec<Box<Node>>,
}

/// DROP_REPLICATION_SLOT command.
#[derive(Debug, Clone, PartialEq)]
pub struct DropReplicationSlotCmd {
    pub slotname: Option<String>,
    pub wait: bool,
}

/// ALTER_REPLICATION_SLOT command.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterReplicationSlotCmd {
    pub slotname: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// START_REPLICATION command.
#[derive(Debug, Clone, PartialEq)]
pub struct StartReplicationCmd {
    pub kind: ReplicationKind,
    pub slotname: Option<String>,
    pub timeline: TimeLineID,
    pub startpoint: XLogRecPtr,
    pub options: Vec<Box<Node>>,
}

/// READ_REPLICATION_SLOT command.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadReplicationSlotCmd {
    pub slotname: Option<String>,
}

/// TIMELINE_HISTORY command.
#[derive(Debug, Clone, PartialEq)]
pub struct TimeLineHistoryCmd {
    pub timeline: TimeLineID,
}

/// UPLOAD_MANIFEST command.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadManifestCmd;
