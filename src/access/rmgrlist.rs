//! Translated from PostgreSQL src/include/access/rmgrlist.h

// The resource manager list. Order defines each rmgr's numerical ID, stored in
// WAL records. Translated as a `#[repr(u8)]` enum plus a name() method; the
// redo/desc/identify/startup/cleanup/mask/decode function pointers are wired up
// in a later phase via the rmgr trait/registry.

/// Resource manager ID, stored in WAL records.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmgrId {
    Xlog = 0,
    Xact = 1,
    Smgr = 2,
    Clog = 3,
    Dbase = 4,
    Tblspc = 5,
    Multixact = 6,
    Relmap = 7,
    Standby = 8,
    Heap2 = 9,
    Heap = 10,
    Btree = 11,
    Hash = 12,
    Gin = 13,
    Gist = 14,
    Seq = 15,
    Spgist = 16,
    Brin = 17,
    CommitTs = 18,
    Replorigin = 19,
    Generic = 20,
    Logicalmsg = 21,
}

impl RmgrId {
    /// Highest assigned resource manager ID.
    pub const MAX_ID: Self = Self::Logicalmsg;

    /// Textual name (from the PG_RMGR list).
    pub fn name(self) -> &'static str {
        match self {
            Self::Xlog => "XLOG",
            Self::Xact => "Transaction",
            Self::Smgr => "Storage",
            Self::Clog => "CLOG",
            Self::Dbase => "Database",
            Self::Tblspc => "Tablespace",
            Self::Multixact => "MultiXact",
            Self::Relmap => "RelMap",
            Self::Standby => "Standby",
            Self::Heap2 => "Heap2",
            Self::Heap => "Heap",
            Self::Btree => "Btree",
            Self::Hash => "Hash",
            Self::Gin => "Gin",
            Self::Gist => "Gist",
            Self::Seq => "Sequence",
            Self::Spgist => "SPGist",
            Self::Brin => "BRIN",
            Self::CommitTs => "CommitTs",
            Self::Replorigin => "ReplicationOrigin",
            Self::Generic => "Generic",
            Self::Logicalmsg => "LogicalMessage",
        }
    }
}
