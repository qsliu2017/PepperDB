//! storage/relfilelocator.h - Physical access information for relations.

use crate::common::relpath::RelFileNumber;
use crate::postgres_ext::Oid;
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

/*
 * RelFileLocator must provide all that we need to know to physically access
 * a relation, with the exception of the backend's proc number, which can be
 * provided separately.  See header comment in relfilelocator.h for full
 * semantics of each field.
 *
 * Note: various places use RelFileLocator in hashtable keys.  Therefore,
 * there *must not* be any unused padding bytes in this struct.  That
 * should be safe as long as all the fields are of type Oid.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,             /* tablespace */
    pub dbOid: Oid,              /* database */
    pub relNumber: RelFileNumber, /* relation */
}

/*
 * Augmenting a relfilelocator with the backend's proc number provides all the
 * information we need to locate the physical storage.  'backend' is
 * INVALID_PROC_NUMBER for regular relations (those accessible to more than
 * one backend), or the owning backend's proc number for backend-local
 * relations.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocatorBackend {
    pub locator: RelFileLocator,
    pub backend: ProcNumber,
}

#[inline]
pub fn RelFileLocatorBackendIsTemp(rlocator: &RelFileLocatorBackend) -> bool {
    rlocator.backend != INVALID_PROC_NUMBER
}

/*
 * Note: RelFileLocatorEquals and RelFileLocatorBackendEquals compare relNumber
 * first since that is most likely to be different in two unequal
 * RelFileLocators.  It is probably redundant to compare spcOid if the other
 * fields are found equal, but do it anyway to be sure.  Likewise for checking
 * the backend number in RelFileLocatorBackendEquals.
 */
#[inline]
pub fn RelFileLocatorEquals(locator1: &RelFileLocator, locator2: &RelFileLocator) -> bool {
    locator1.relNumber == locator2.relNumber
        && locator1.dbOid == locator2.dbOid
        && locator1.spcOid == locator2.spcOid
}

#[inline]
pub fn RelFileLocatorBackendEquals(
    locator1: &RelFileLocatorBackend,
    locator2: &RelFileLocatorBackend,
) -> bool {
    locator1.locator.relNumber == locator2.locator.relNumber
        && locator1.locator.dbOid == locator2.locator.dbOid
        && locator1.backend == locator2.backend
        && locator1.locator.spcOid == locator2.locator.spcOid
}
