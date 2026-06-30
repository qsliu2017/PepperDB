//! Translated from PostgreSQL src/include/storage/relfilelocator.h

use crate::common::relpath::RelFileNumber;
use crate::postgres_ext::Oid;
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

/// All we need to physically access a relation, except the backend's proc
/// number (provided separately). Used as a hashtable key: no padding allowed,
/// which is satisfied by all-Oid fields. This is the canonical definition that
/// the level-2 `common/relpath` forward-decl repoints to in Phase 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct RelFileLocator {
    pub spcOid: Oid,              // tablespace
    pub dbOid: Oid,              // database (0 for shared relations)
    pub relNumber: RelFileNumber, // relation (pg_class.relfilenode)
}

const _: () = assert!(core::mem::size_of::<RelFileLocator>() == 12);
const _: () = assert!(core::mem::offset_of!(RelFileLocator, spcOid) == 0);
const _: () = assert!(core::mem::offset_of!(RelFileLocator, dbOid) == 4);
const _: () = assert!(core::mem::offset_of!(RelFileLocator, relNumber) == 8);

impl RelFileLocator {
    /// Compares relNumber first (most likely to differ), per PG.
    pub const fn equals(&self, other: &Self) -> bool {
        self.relNumber.get() == other.relNumber.get()
            && self.dbOid.get() == other.dbOid.get()
            && self.spcOid.get() == other.spcOid.get()
    }
}

/// A relfilelocator augmented with the owning backend's proc number.
/// `backend` is INVALID_PROC_NUMBER for regular (shared) relations, or the
/// owning backend for transient backend-local relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelFileLocatorBackend {
    pub locator: RelFileLocator,
    pub backend: ProcNumber,
}

impl RelFileLocatorBackend {
    /// True iff this locates a backend-local (temp) relation.
    pub const fn is_temp(&self) -> bool {
        self.backend != INVALID_PROC_NUMBER
    }

    /// Compares relNumber first, then dbOid, backend, spcOid, per PG.
    pub const fn equals(&self, other: &Self) -> bool {
        self.locator.relNumber.get() == other.locator.relNumber.get()
            && self.locator.dbOid.get() == other.locator.dbOid.get()
            && self.backend == other.backend
            && self.locator.spcOid.get() == other.locator.spcOid.get()
    }
}
