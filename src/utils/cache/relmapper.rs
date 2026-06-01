//! src/backend/utils/cache/relmapper.c
//!
//! Catalog-to-filenumber mapping
//!
//! For most tables, the physical file underlying the table is specified by
//! pg_class.relfilenode.  However, that obviously won't work for pg_class
//! itself, nor for the other "nailed" catalogs for which we have to be able
//! to set up working Relation entries without access to pg_class.  It also
//! does not work for shared catalogs, since there is no practical way to
//! update other databases' pg_class entries when relocating a shared catalog.
//! Therefore, for these special catalogs (henceforth referred to as "mapped
//! catalogs") we rely on a separately maintained file that shows the mapping
//! from catalog OIDs to filenumbers.  Each database has a map file for
//! its local mapped catalogs, and there is a separate map file for shared
//! catalogs.  Mapped catalogs have zero in their pg_class.relfilenode entries.
//!
//! Relocation of a normal table is committed (ie, the new physical file becomes
//! authoritative) when the pg_class row update commits.  For mapped catalogs,
//! the act of updating the map file is effectively commit of the relocation.
//! We postpone the file update till just before commit of the transaction
//! doing the rewrite, but there is necessarily a window between.  Therefore
//! mapped catalogs can only be relocated by operations such as VACUUM FULL
//! and CLUSTER, which make no transactionally-significant changes: it must be
//! safe for the new file to replace the old, even if the transaction itself
//! aborts.  An important factor here is that the indexes and toast table of
//! a mapped catalog must also be mapped, so that the rewrites/relocations of
//! all these files commit in a single map file update rather than being tied
//! to transaction commit.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::pg_config_manual::MAXPGPATH;

// ---- types from relmapper.h (merged in) ----

/* ----------------
 *		relmap-related XLOG entries
 * ----------------
 */

pub const XLOG_RELMAP_UPDATE: u8 = 0x00;

#[repr(C)]
pub struct xl_relmap_update {
    pub dbid: Oid,                              /* database ID, or 0 for shared map */
    pub tsid: Oid,                              /* database's tablespace, or pg_global */
    pub nbytes: int32,                          /* size of relmap data */
    pub data: [c_char; 0],                      /* FLEXIBLE_ARRAY_MEMBER */
}

/* MinSizeOfRelmapUpdate offsetof(xl_relmap_update, data) */
pub const MinSizeOfRelmapUpdate: usize = core::mem::offset_of!(xl_relmap_update, data);

// ---- stub aliases for external types ----

type RelFileNumber = crate::common::relpath::RelFileNumber;
type pg_crc32c = u32;
type XLogReaderState = c_void;
type XLogRecPtr = crate::access::transam::xlogdefs::XLogRecPtr;
type StringInfo = *mut c_void;

// ---- relmapper.c ----

/*
 * The map file is critical data: we have no automatic method for recovering
 * from loss or corruption of it.  We use a CRC so that we can detect
 * corruption.  Since the file might be more than one standard-size disk
 * sector in size, we cannot rely on overwrite-in-place. Instead, we generate
 * a new file and rename it into place, atomically replacing the original file.
 *
 * Entries in the mappings[] array are in no particular order.  We could
 * speed searching by insisting on OID order, but it really shouldn't be
 * worth the trouble given the intended size of the mapping sets.
 */
const RELMAPPER_FILENAME: &[u8] = b"pg_filenode.map\0";
const RELMAPPER_TEMP_FILENAME: &[u8] = b"pg_filenode.map.tmp\0";

const RELMAPPER_FILEMAGIC: int32 = 0x592717; /* version ID value */

/*
 * There's no need for this constant to have any particular value, and we
 * can raise it as necessary if we end up with more mapped relations. For
 * now, we just pick a round number that is modestly larger than the expected
 * number of mappings.
 */
const MAX_MAPPINGS: usize = 64;

#[repr(C)]
#[derive(Copy, Clone)]
struct RelMapping {
    mapoid: Oid,                    /* OID of a catalog */
    mapfilenumber: RelFileNumber,   /* its rel file number */
}

#[repr(C)]
#[derive(Copy, Clone)]
struct RelMapFile {
    magic: int32,                          /* always RELMAPPER_FILEMAGIC */
    num_mappings: int32,                   /* number of valid RelMapping entries */
    mappings: [RelMapping; MAX_MAPPINGS],
    crc: pg_crc32c,                        /* CRC of all above */
}

/*
 * State for serializing local and shared relmappings for parallel workers
 * (active states only).  See notes on active_* and pending_* updates state.
 */
#[repr(C)]
#[derive(Copy, Clone)]
struct SerializedActiveRelMaps {
    active_shared_updates: RelMapFile,
    active_local_updates: RelMapFile,
}

/*
 * The currently known contents of the shared map file and our database's
 * local map file are stored here.  These can be reloaded from disk
 * immediately whenever we receive an update sinval message.
 */
static mut shared_map: RelMapFile = ZERO_RELMAPFILE;
static mut local_map: RelMapFile = ZERO_RELMAPFILE;

/*
 * We use the same RelMapFile data structure to track uncommitted local
 * changes in the mappings (but note the magic and crc fields are not made
 * valid in these variables).  Currently, map updates are not allowed within
 * subtransactions, so one set of transaction-level changes is sufficient.
 *
 * The active_xxx variables contain updates that are valid in our transaction
 * and should be honored by RelationMapOidToFilenumber.  The pending_xxx
 * variables contain updates we have been told about that aren't active yet;
 * they will become active at the next CommandCounterIncrement.  This setup
 * lets map updates act similarly to updates of pg_class rows, ie, they
 * become visible only at the next CommandCounterIncrement boundary.
 *
 * Active shared and active local updates are serialized by the parallel
 * infrastructure, and deserialized within parallel workers.
 */
static mut active_shared_updates: RelMapFile = ZERO_RELMAPFILE;
static mut active_local_updates: RelMapFile = ZERO_RELMAPFILE;
static mut pending_shared_updates: RelMapFile = ZERO_RELMAPFILE;
static mut pending_local_updates: RelMapFile = ZERO_RELMAPFILE;

const ZERO_RELMAPPING: RelMapping = RelMapping {
    mapoid: 0,
    mapfilenumber: 0,
};
const ZERO_RELMAPFILE: RelMapFile = RelMapFile {
    magic: 0,
    num_mappings: 0,
    mappings: [ZERO_RELMAPPING; MAX_MAPPINGS],
    crc: 0,
};

/*
 * RelationMapOidToFilenumber
 *
 * The raison d' etre ... given a relation OID, look up its filenumber.
 *
 * Although shared and local relation OIDs should never overlap, the caller
 * always knows which we need --- so pass that information to avoid useless
 * searching.
 *
 * Returns InvalidRelFileNumber if the OID is not known (which should never
 * happen, but the caller is in a better position to report a meaningful
 * error).
 */
pub unsafe fn RelationMapOidToFilenumber(relationId: Oid, shared: bool) -> RelFileNumber {
    let mut map: *const RelMapFile;
    let mut i: int32;

    /* If there are active updates, believe those over the main maps */
    if shared {
        map = &raw const active_shared_updates;
        i = 0;
        while i < (*map).num_mappings {
            if relationId == (*map).mappings[i as usize].mapoid {
                return (*map).mappings[i as usize].mapfilenumber;
            }
            i += 1;
        }
        map = &raw const shared_map;
        i = 0;
        while i < (*map).num_mappings {
            if relationId == (*map).mappings[i as usize].mapoid {
                return (*map).mappings[i as usize].mapfilenumber;
            }
            i += 1;
        }
    } else {
        map = &raw const active_local_updates;
        i = 0;
        while i < (*map).num_mappings {
            if relationId == (*map).mappings[i as usize].mapoid {
                return (*map).mappings[i as usize].mapfilenumber;
            }
            i += 1;
        }
        map = &raw const local_map;
        i = 0;
        while i < (*map).num_mappings {
            if relationId == (*map).mappings[i as usize].mapoid {
                return (*map).mappings[i as usize].mapfilenumber;
            }
            i += 1;
        }
    }

    InvalidRelFileNumber
}

/*
 * RelationMapFilenumberToOid
 *
 * Do the reverse of the normal direction of mapping done in
 * RelationMapOidToFilenumber.
 *
 * This is not supposed to be used during normal running but rather for
 * information purposes when looking at the filesystem or xlog.
 *
 * Returns InvalidOid if the OID is not known; this can easily happen if the
 * relfilenumber doesn't pertain to a mapped relation.
 */
pub unsafe fn RelationMapFilenumberToOid(filenumber: RelFileNumber, shared: bool) -> Oid {
    let mut map: *const RelMapFile;
    let mut i: int32;

    /* If there are active updates, believe those over the main maps */
    if shared {
        map = &raw const active_shared_updates;
        i = 0;
        while i < (*map).num_mappings {
            if filenumber == (*map).mappings[i as usize].mapfilenumber {
                return (*map).mappings[i as usize].mapoid;
            }
            i += 1;
        }
        map = &raw const shared_map;
        i = 0;
        while i < (*map).num_mappings {
            if filenumber == (*map).mappings[i as usize].mapfilenumber {
                return (*map).mappings[i as usize].mapoid;
            }
            i += 1;
        }
    } else {
        map = &raw const active_local_updates;
        i = 0;
        while i < (*map).num_mappings {
            if filenumber == (*map).mappings[i as usize].mapfilenumber {
                return (*map).mappings[i as usize].mapoid;
            }
            i += 1;
        }
        map = &raw const local_map;
        i = 0;
        while i < (*map).num_mappings {
            if filenumber == (*map).mappings[i as usize].mapfilenumber {
                return (*map).mappings[i as usize].mapoid;
            }
            i += 1;
        }
    }

    InvalidOid
}

/*
 * RelationMapOidToFilenumberForDatabase
 *
 * Like RelationMapOidToFilenumber, but reads the mapping from the indicated
 * path instead of using the one for the current database.
 */
pub unsafe fn RelationMapOidToFilenumberForDatabase(
    dbpath: *mut c_char,
    relationId: Oid,
) -> RelFileNumber {
    let mut map: RelMapFile = ZERO_RELMAPFILE;
    let mut i: c_int;

    /* Read the relmap file from the source database. */
    read_relmap_file(&mut map, dbpath, false, ERROR);

    /* Iterate over the relmap entries to find the input relation OID. */
    i = 0;
    while i < map.num_mappings {
        if relationId == map.mappings[i as usize].mapoid {
            return map.mappings[i as usize].mapfilenumber;
        }
        i += 1;
    }

    InvalidRelFileNumber
}

/*
 * RelationMapCopy
 *
 * Copy relmapfile from source db path to the destination db path and WAL log
 * the operation. This is intended for use in creating a new relmap file
 * for a database that doesn't have one yet, not for replacing an existing
 * relmap file.
 */
pub unsafe fn RelationMapCopy(
    dbid: Oid,
    tsid: Oid,
    srcdbpath: *mut c_char,
    dstdbpath: *mut c_char,
) {
    let mut map: RelMapFile = ZERO_RELMAPFILE;

    /*
     * Read the relmap file from the source database.
     */
    read_relmap_file(&mut map, srcdbpath, false, ERROR);

    /*
     * Write the same data into the destination database's relmap file.
     *
     * No sinval is needed because no one can be connected to the destination
     * database yet.
     *
     * There's no point in trying to preserve files here. The new database
     * isn't usable yet anyway, and won't ever be if we can't install a relmap
     * file.
     */
    LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
    write_relmap_file(&mut map, true, false, false, dbid, tsid, dstdbpath as *const c_char);
    LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapUpdateMap
 *
 * Install a new relfilenumber mapping for the specified relation.
 *
 * If immediate is true (or we're bootstrapping), the mapping is activated
 * immediately.  Otherwise it is made pending until CommandCounterIncrement.
 */
pub unsafe fn RelationMapUpdateMap(
    relationId: Oid,
    fileNumber: RelFileNumber,
    shared: bool,
    immediate: bool,
) {
    let map: *mut RelMapFile;

    if IsBootstrapProcessingMode() {
        /*
         * In bootstrap mode, the mapping gets installed in permanent map.
         */
        if shared {
            map = &raw mut shared_map;
        } else {
            map = &raw mut local_map;
        }
    } else {
        /*
         * We don't currently support map changes within subtransactions, or
         * when in parallel mode.  This could be done with more bookkeeping
         * infrastructure, but it doesn't presently seem worth it.
         */
        if GetCurrentTransactionNestLevel() > 1 {
            elog!(ERROR, "cannot change relation mapping within subtransaction");
        }

        if IsInParallelMode() {
            elog!(ERROR, "cannot change relation mapping in parallel mode");
        }

        if immediate {
            /* Make it active, but only locally */
            if shared {
                map = &raw mut active_shared_updates;
            } else {
                map = &raw mut active_local_updates;
            }
        } else {
            /* Make it pending */
            if shared {
                map = &raw mut pending_shared_updates;
            } else {
                map = &raw mut pending_local_updates;
            }
        }
    }
    apply_map_update(map, relationId, fileNumber, true);
}

/*
 * apply_map_update
 *
 * Insert a new mapping into the given map variable, replacing any existing
 * mapping for the same relation.
 *
 * In some cases the caller knows there must be an existing mapping; pass
 * add_okay = false to draw an error if not.
 */
unsafe fn apply_map_update(
    map: *mut RelMapFile,
    relationId: Oid,
    fileNumber: RelFileNumber,
    add_okay: bool,
) {
    let mut i: int32;

    /* Replace any existing mapping */
    i = 0;
    while i < (*map).num_mappings {
        if relationId == (*map).mappings[i as usize].mapoid {
            (*map).mappings[i as usize].mapfilenumber = fileNumber;
            return;
        }
        i += 1;
    }

    /* Nope, need to add a new mapping */
    if !add_okay {
        elog!(
            ERROR,
            "attempt to apply a mapping to unmapped relation {}",
            relationId
        );
    }
    if (*map).num_mappings as usize >= MAX_MAPPINGS {
        elog!(ERROR, "ran out of space in relation map");
    }
    let n = (*map).num_mappings as usize;
    (*map).mappings[n].mapoid = relationId;
    (*map).mappings[n].mapfilenumber = fileNumber;
    (*map).num_mappings += 1;
}

/*
 * merge_map_updates
 *
 * Merge all the updates in the given pending-update map into the target map.
 * This is just a bulk form of apply_map_update.
 */
unsafe fn merge_map_updates(map: *mut RelMapFile, updates: *const RelMapFile, add_okay: bool) {
    let mut i: int32 = 0;

    while i < (*updates).num_mappings {
        apply_map_update(
            map,
            (*updates).mappings[i as usize].mapoid,
            (*updates).mappings[i as usize].mapfilenumber,
            add_okay,
        );
        i += 1;
    }
}

/*
 * RelationMapRemoveMapping
 *
 * Remove a relation's entry in the map.  This is only allowed for "active"
 * (but not committed) local mappings.  We need it so we can back out the
 * entry for the transient target file when doing VACUUM FULL/CLUSTER on
 * a mapped relation.
 */
pub unsafe fn RelationMapRemoveMapping(relationId: Oid) {
    let map: *mut RelMapFile = &raw mut active_local_updates;
    let mut i: int32 = 0;

    while i < (*map).num_mappings {
        if relationId == (*map).mappings[i as usize].mapoid {
            /* Found it, collapse it out */
            (*map).mappings[i as usize] = (*map).mappings[((*map).num_mappings - 1) as usize];
            (*map).num_mappings -= 1;
            return;
        }
        i += 1;
    }
    elog!(
        ERROR,
        "could not find temporary mapping for relation {}",
        relationId
    );
}

/*
 * RelationMapInvalidate
 *
 * This routine is invoked for SI cache flush messages.  We must re-read
 * the indicated map file.  However, we might receive a SI message in a
 * process that hasn't yet, and might never, load the mapping files;
 * for example the autovacuum launcher, which *must not* try to read
 * a local map since it is attached to no particular database.
 * So, re-read only if the map is valid now.
 */
pub unsafe fn RelationMapInvalidate(shared: bool) {
    if shared {
        if shared_map.magic == RELMAPPER_FILEMAGIC {
            load_relmap_file(true, false);
        }
    } else {
        if local_map.magic == RELMAPPER_FILEMAGIC {
            load_relmap_file(false, false);
        }
    }
}

/*
 * RelationMapInvalidateAll
 *
 * Reload all map files.  This is used to recover from SI message buffer
 * overflow: we can't be sure if we missed an inval message.
 * Again, reload only currently-valid maps.
 */
pub unsafe fn RelationMapInvalidateAll() {
    if shared_map.magic == RELMAPPER_FILEMAGIC {
        load_relmap_file(true, false);
    }
    if local_map.magic == RELMAPPER_FILEMAGIC {
        load_relmap_file(false, false);
    }
}

/*
 * AtCCI_RelationMap
 *
 * Activate any "pending" relation map updates at CommandCounterIncrement time.
 */
pub unsafe fn AtCCI_RelationMap() {
    if pending_shared_updates.num_mappings != 0 {
        merge_map_updates(
            &raw mut active_shared_updates,
            &raw const pending_shared_updates,
            true,
        );
        pending_shared_updates.num_mappings = 0;
    }
    if pending_local_updates.num_mappings != 0 {
        merge_map_updates(
            &raw mut active_local_updates,
            &raw const pending_local_updates,
            true,
        );
        pending_local_updates.num_mappings = 0;
    }
}

/*
 * AtEOXact_RelationMap
 *
 * Handle relation mapping at main-transaction commit or abort.
 *
 * During commit, this must be called as late as possible before the actual
 * transaction commit, so as to minimize the window where the transaction
 * could still roll back after committing map changes.  Although nothing
 * critically bad happens in such a case, we still would prefer that it
 * not happen, since we'd possibly be losing useful updates to the relations'
 * pg_class row(s).
 *
 * During abort, we just have to throw away any pending map changes.
 * Normal post-abort cleanup will take care of fixing relcache entries.
 * Parallel worker commit/abort is handled by resetting active mappings
 * that may have been received from the leader process.  (There should be
 * no pending updates in parallel workers.)
 */
pub unsafe fn AtEOXact_RelationMap(isCommit: bool, isParallelWorker: bool) {
    if isCommit && !isParallelWorker {
        /*
         * We should not get here with any "pending" updates.  (We could
         * logically choose to treat such as committed, but in the current
         * code this should never happen.)
         */
        Assert!(pending_shared_updates.num_mappings == 0);
        Assert!(pending_local_updates.num_mappings == 0);

        /*
         * Write any active updates to the actual map files, then reset them.
         */
        if active_shared_updates.num_mappings != 0 {
            perform_relmap_update(true, &raw const active_shared_updates);
            active_shared_updates.num_mappings = 0;
        }
        if active_local_updates.num_mappings != 0 {
            perform_relmap_update(false, &raw const active_local_updates);
            active_local_updates.num_mappings = 0;
        }
    } else {
        /* Abort or parallel worker --- drop all local and pending updates */
        Assert!(!isParallelWorker || pending_shared_updates.num_mappings == 0);
        Assert!(!isParallelWorker || pending_local_updates.num_mappings == 0);

        active_shared_updates.num_mappings = 0;
        active_local_updates.num_mappings = 0;
        pending_shared_updates.num_mappings = 0;
        pending_local_updates.num_mappings = 0;
    }
}

/*
 * AtPrepare_RelationMap
 *
 * Handle relation mapping at PREPARE.
 *
 * Currently, we don't support preparing any transaction that changes the map.
 */
pub unsafe fn AtPrepare_RelationMap() {
    if active_shared_updates.num_mappings != 0
        || active_local_updates.num_mappings != 0
        || pending_shared_updates.num_mappings != 0
        || pending_local_updates.num_mappings != 0
    {
        ereport!(
            ERROR,
            "cannot PREPARE a transaction that modified relation mapping"
        );
    }
}

/*
 * CheckPointRelationMap
 *
 * This is called during a checkpoint.  It must ensure that any relation map
 * updates that were WAL-logged before the start of the checkpoint are
 * securely flushed to disk and will not need to be replayed later.  This
 * seems unlikely to be a performance-critical issue, so we use a simple
 * method: we just take and release the RelationMappingLock.  This ensures
 * that any already-logged map update is complete, because write_relmap_file
 * will fsync the map file before the lock is released.
 */
pub unsafe fn CheckPointRelationMap() {
    LWLockAcquire(RelationMappingLock, LW_SHARED);
    LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapFinishBootstrap
 *
 * Write out the initial relation mapping files at the completion of
 * bootstrap.  All the mapped files should have been made known to us
 * via RelationMapUpdateMap calls.
 */
pub unsafe fn RelationMapFinishBootstrap() {
    Assert!(IsBootstrapProcessingMode());

    /* Shouldn't be anything "pending" ... */
    Assert!(active_shared_updates.num_mappings == 0);
    Assert!(active_local_updates.num_mappings == 0);
    Assert!(pending_shared_updates.num_mappings == 0);
    Assert!(pending_local_updates.num_mappings == 0);

    /* Write the files; no WAL or sinval needed */
    LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
    write_relmap_file(
        &raw mut shared_map,
        false,
        false,
        false,
        InvalidOid,
        GLOBALTABLESPACE_OID,
        c"global".as_ptr(),
    );
    write_relmap_file(
        &raw mut local_map,
        false,
        false,
        false,
        MyDatabaseId,
        MyDatabaseTableSpace,
        DatabasePath,
    );
    LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapInitialize
 *
 * This initializes the mapper module at process startup.  We can't access the
 * database yet, so just make sure the maps are empty.
 */
pub unsafe fn RelationMapInitialize() {
    /* The static variables should initialize to zeroes, but let's be sure */
    shared_map.magic = 0; /* mark it not loaded */
    local_map.magic = 0;
    shared_map.num_mappings = 0;
    local_map.num_mappings = 0;
    active_shared_updates.num_mappings = 0;
    active_local_updates.num_mappings = 0;
    pending_shared_updates.num_mappings = 0;
    pending_local_updates.num_mappings = 0;
}

/*
 * RelationMapInitializePhase2
 *
 * This is called to prepare for access to pg_database during startup.
 * We should be able to read the shared map file now.
 */
pub unsafe fn RelationMapInitializePhase2() {
    /*
     * In bootstrap mode, the map file isn't there yet, so do nothing.
     */
    if IsBootstrapProcessingMode() {
        return;
    }

    /*
     * Load the shared map file, die on error.
     */
    load_relmap_file(true, false);
}

/*
 * RelationMapInitializePhase3
 *
 * This is called as soon as we have determined MyDatabaseId and set up
 * DatabasePath.  At this point we should be able to read the local map file.
 */
pub unsafe fn RelationMapInitializePhase3() {
    /*
     * In bootstrap mode, the map file isn't there yet, so do nothing.
     */
    if IsBootstrapProcessingMode() {
        return;
    }

    /*
     * Load the local map file, die on error.
     */
    load_relmap_file(false, false);
}

/*
 * EstimateRelationMapSpace
 *
 * Estimate space needed to pass active shared and local relmaps to parallel
 * workers.
 */
pub unsafe fn EstimateRelationMapSpace() -> Size {
    core::mem::size_of::<SerializedActiveRelMaps>() as Size
}

/*
 * SerializeRelationMap
 *
 * Serialize active shared and local relmap state for parallel workers.
 */
pub unsafe fn SerializeRelationMap(maxSize: Size, startAddress: *mut c_char) {
    let relmaps: *mut SerializedActiveRelMaps;

    Assert!(maxSize >= EstimateRelationMapSpace());

    relmaps = startAddress as *mut SerializedActiveRelMaps;
    (*relmaps).active_shared_updates = active_shared_updates;
    (*relmaps).active_local_updates = active_local_updates;
}

/*
 * RestoreRelationMap
 *
 * Restore active shared and local relmap state within a parallel worker.
 */
pub unsafe fn RestoreRelationMap(startAddress: *mut c_char) {
    let relmaps: *mut SerializedActiveRelMaps;

    if active_shared_updates.num_mappings != 0
        || active_local_updates.num_mappings != 0
        || pending_shared_updates.num_mappings != 0
        || pending_local_updates.num_mappings != 0
    {
        elog!(ERROR, "parallel worker has existing mappings");
    }

    relmaps = startAddress as *mut SerializedActiveRelMaps;
    active_shared_updates = (*relmaps).active_shared_updates;
    active_local_updates = (*relmaps).active_local_updates;
}

/*
 * load_relmap_file -- load the shared or local map file
 *
 * Because these files are essential for access to core system catalogs,
 * failure to load either of them is a fatal error.
 *
 * Note that the local case requires DatabasePath to be set up.
 */
unsafe fn load_relmap_file(shared: bool, lock_held: bool) {
    if shared {
        read_relmap_file(
            &raw mut shared_map,
            c"global".as_ptr() as *mut c_char,
            lock_held,
            FATAL,
        );
    } else {
        read_relmap_file(&raw mut local_map, DatabasePath as *mut c_char, lock_held, FATAL);
    }
}

/*
 * read_relmap_file -- load data from any relation mapper file
 *
 * dbpath must be the relevant database path, or "global" for shared relations.
 *
 * RelationMappingLock will be acquired released unless lock_held = true.
 *
 * Errors will be reported at the indicated elevel, which should be at least
 * ERROR.
 */
unsafe fn read_relmap_file(map: *mut RelMapFile, dbpath: *mut c_char, lock_held: bool, elevel: c_int) {
    let mut mapfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut crc: pg_crc32c = 0;
    let fd: c_int;
    let r: c_int;

    Assert!(elevel >= ERROR);

    /*
     * Grab the lock to prevent the file from being updated while we read it,
     * unless the caller is already holding the lock.  If the file is updated
     * shortly after we look, the sinval signaling mechanism will make us
     * re-read it before we are able to access any relation that's affected by
     * the change.
     */
    if !lock_held {
        LWLockAcquire(RelationMappingLock, LW_SHARED);
    }

    /*
     * Open the target file.
     *
     * Because Windows isn't happy about the idea of renaming over a file that
     * someone has open, we only open this file after acquiring the lock, and
     * for the same reason, we close it before releasing the lock. That way,
     * by the time write_relmap_file() acquires an exclusive lock, no one else
     * will have it open.
     */
    snprintf(
        mapfilename.as_mut_ptr(),
        core::mem::size_of_val(&mapfilename),
        c"%s/%s".as_ptr(),
        dbpath,
        RELMAPPER_FILENAME.as_ptr() as *const c_char,
    );
    fd = OpenTransientFile(mapfilename.as_ptr(), O_RDONLY | PG_BINARY);
    if fd < 0 {
        ereport!(elevel, "could not open file");
    }

    /* Now read the data. */
    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_READ);
    r = read(fd, map as *mut c_void, core::mem::size_of::<RelMapFile>());
    if r != core::mem::size_of::<RelMapFile>() as c_int {
        if r < 0 {
            ereport!(elevel, "could not read file");
        } else {
            ereport!(elevel, "could not read file");
        }
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(elevel, "could not close file");
    }

    if !lock_held {
        LWLockRelease(RelationMappingLock);
    }

    /* check for correct magic number, etc */
    if (*map).magic != RELMAPPER_FILEMAGIC
        || (*map).num_mappings < 0
        || (*map).num_mappings as usize > MAX_MAPPINGS
    {
        ereport!(elevel, "relation mapping file contains invalid data");
    }

    /* verify the CRC */
    INIT_CRC32C(&mut crc);
    COMP_CRC32C(
        &mut crc,
        map as *const c_void,
        core::mem::offset_of!(RelMapFile, crc),
    );
    FIN_CRC32C(&mut crc);

    if !EQ_CRC32C(crc, (*map).crc) {
        ereport!(elevel, "relation mapping file contains incorrect checksum");
    }
}

/*
 * Write out a new shared or local map file with the given contents.
 *
 * The magic number and CRC are automatically updated in *newmap.  On
 * success, we copy the data to the appropriate permanent static variable.
 *
 * If write_wal is true then an appropriate WAL message is emitted.
 * (It will be false for bootstrap and WAL replay cases.)
 *
 * If send_sinval is true then a SI invalidation message is sent.
 * (This should be true except in bootstrap case.)
 *
 * If preserve_files is true then the storage manager is warned not to
 * delete the files listed in the map.
 *
 * Because this may be called during WAL replay when MyDatabaseId,
 * DatabasePath, etc aren't valid, we require the caller to pass in suitable
 * values. Pass dbpath as "global" for the shared map.
 *
 * The caller is also responsible for being sure no concurrent map update
 * could be happening.
 */
unsafe fn write_relmap_file(
    newmap: *mut RelMapFile,
    write_wal: bool,
    send_sinval: bool,
    preserve_files: bool,
    dbid: Oid,
    tsid: Oid,
    dbpath: *const c_char,
) {
    let fd: c_int;
    let mut mapfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut maptempfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /*
     * Even without concurrent use of this map, CheckPointRelationMap() relies
     * on this locking.  Without it, a restore of a base backup taken after
     * this function's XLogInsert() and before its durable_rename() would not
     * have the changes.  wal_level=minimal doesn't need the lock, but this
     * isn't performance-critical enough for such a micro-optimization.
     */
    Assert!(LWLockHeldByMeInMode(RelationMappingLock, LW_EXCLUSIVE));

    /*
     * Fill in the overhead fields and update CRC.
     */
    (*newmap).magic = RELMAPPER_FILEMAGIC;
    if (*newmap).num_mappings < 0 || (*newmap).num_mappings as usize > MAX_MAPPINGS {
        elog!(ERROR, "attempt to write bogus relation mapping");
    }

    INIT_CRC32C(&mut (*newmap).crc);
    COMP_CRC32C(
        &mut (*newmap).crc,
        newmap as *const c_void,
        core::mem::offset_of!(RelMapFile, crc),
    );
    FIN_CRC32C(&mut (*newmap).crc);

    /*
     * Construct filenames -- a temporary file that we'll create to write the
     * data initially, and then the permanent name to which we will rename it.
     */
    snprintf(
        mapfilename.as_mut_ptr(),
        core::mem::size_of_val(&mapfilename),
        c"%s/%s".as_ptr(),
        dbpath,
        RELMAPPER_FILENAME.as_ptr() as *const c_char,
    );
    snprintf(
        maptempfilename.as_mut_ptr(),
        core::mem::size_of_val(&maptempfilename),
        c"%s/%s".as_ptr(),
        dbpath,
        RELMAPPER_TEMP_FILENAME.as_ptr() as *const c_char,
    );

    /*
     * Open a temporary file. If a file already exists with this name, it must
     * be left over from a previous crash, so we can overwrite it. Concurrent
     * calls to this function are not allowed.
     */
    fd = OpenTransientFile(
        maptempfilename.as_ptr(),
        O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
    );
    if fd < 0 {
        ereport!(ERROR, "could not open file");
    }

    /* Write new data to the file. */
    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_WRITE);
    if write(fd, newmap as *const c_void, core::mem::size_of::<RelMapFile>())
        != core::mem::size_of::<RelMapFile>() as c_int
    {
        /* if write didn't set errno, assume problem is no disk space */
        if errno() == 0 {
            set_errno(ENOSPC);
        }
        ereport!(ERROR, "could not write file");
    }
    pgstat_report_wait_end();

    /* And close the file. */
    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, "could not close file");
    }

    if write_wal {
        let mut xlrec: xl_relmap_update = core::mem::zeroed();
        let lsn: XLogRecPtr;

        /* now errors are fatal ... */
        START_CRIT_SECTION();

        xlrec.dbid = dbid;
        xlrec.tsid = tsid;
        xlrec.nbytes = core::mem::size_of::<RelMapFile>() as int32;

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_char, MinSizeOfRelmapUpdate as c_int);
        XLogRegisterData(newmap as *mut c_char, core::mem::size_of::<RelMapFile>() as c_int);

        lsn = XLogInsert(RM_RELMAP_ID, XLOG_RELMAP_UPDATE);

        /* As always, WAL must hit the disk before the data update does */
        XLogFlush(lsn);
    }

    /*
     * durable_rename() does all the hard work of making sure that we rename
     * the temporary file into place in a crash-safe manner.
     *
     * NB: Although we instruct durable_rename() to use ERROR, we will often
     * be in a critical section at this point; if so, ERROR will become PANIC.
     */
    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_REPLACE);
    durable_rename(maptempfilename.as_ptr(), mapfilename.as_ptr(), ERROR);
    pgstat_report_wait_end();

    /*
     * Now that the file is safely on disk, send sinval message to let other
     * backends know to re-read it.  We must do this inside the critical
     * section: if for some reason we fail to send the message, we have to
     * force a database-wide PANIC.  Otherwise other backends might continue
     * execution with stale mapping information, which would be catastrophic
     * as soon as others began to use the now-committed data.
     */
    if send_sinval {
        CacheInvalidateRelmap(dbid);
    }

    /*
     * Make sure that the files listed in the map are not deleted if the outer
     * transaction aborts.  This had better be within the critical section
     * too: it's not likely to fail, but if it did, we'd arrive at transaction
     * abort with the files still vulnerable.  PANICing will leave things in a
     * good state on-disk.
     *
     * Note: we're cheating a little bit here by assuming that mapped files
     * are either in pg_global or the database's default tablespace.
     */
    if preserve_files {
        let mut i: int32 = 0;

        while i < (*newmap).num_mappings {
            let mut rlocator: RelFileLocator = core::mem::zeroed();

            rlocator.spcOid = tsid;
            rlocator.dbOid = dbid;
            rlocator.relNumber = (*newmap).mappings[i as usize].mapfilenumber;
            RelationPreserveStorage(rlocator, false);
            i += 1;
        }
    }

    /* Critical section done */
    if write_wal {
        END_CRIT_SECTION();
    }
}

/*
 * Merge the specified updates into the appropriate "real" map,
 * and write out the changes.  This function must be used for committing
 * updates during normal multiuser operation.
 */
unsafe fn perform_relmap_update(shared: bool, updates: *const RelMapFile) {
    let mut newmap: RelMapFile = ZERO_RELMAPFILE;

    /*
     * Anyone updating a relation's mapping info should take exclusive lock on
     * that rel and hold it until commit.  This ensures that there will not be
     * concurrent updates on the same mapping value; but there could easily be
     * concurrent updates on different values in the same file. We cover that
     * by acquiring the RelationMappingLock, re-reading the target file to
     * ensure it's up to date, applying the updates, and writing the data
     * before releasing RelationMappingLock.
     *
     * There is only one RelationMappingLock.  In principle we could try to
     * have one per mapping file, but it seems unlikely to be worth the
     * trouble.
     */
    LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);

    /* Be certain we see any other updates just made */
    load_relmap_file(shared, true);

    /* Prepare updated data in a local variable */
    if shared {
        memcpy(
            &raw mut newmap as *mut c_void,
            &raw const shared_map as *const c_void,
            core::mem::size_of::<RelMapFile>(),
        );
    } else {
        memcpy(
            &raw mut newmap as *mut c_void,
            &raw const local_map as *const c_void,
            core::mem::size_of::<RelMapFile>(),
        );
    }

    /*
     * Apply the updates to newmap.  No new mappings should appear, unless
     * somebody is adding indexes to system catalogs.
     */
    merge_map_updates(&raw mut newmap, updates, allowSystemTableMods);

    /* Write out the updated map and do other necessary tasks */
    write_relmap_file(
        &raw mut newmap,
        true,
        true,
        true,
        if shared { InvalidOid } else { MyDatabaseId },
        if shared { GLOBALTABLESPACE_OID } else { MyDatabaseTableSpace },
        if shared {
            c"global".as_ptr()
        } else {
            DatabasePath
        },
    );

    /*
     * We successfully wrote the updated file, so it's now safe to rely on the
     * new values in this process, too.
     */
    if shared {
        memcpy(
            &raw mut shared_map as *mut c_void,
            &raw const newmap as *const c_void,
            core::mem::size_of::<RelMapFile>(),
        );
    } else {
        memcpy(
            &raw mut local_map as *mut c_void,
            &raw const newmap as *const c_void,
            core::mem::size_of::<RelMapFile>(),
        );
    }

    /* Now we can release the lock */
    LWLockRelease(RelationMappingLock);
}

/*
 * RELMAP resource manager's routines
 */
pub unsafe fn relmap_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in relmap records */
    Assert!(!XLogRecHasAnyBlockRefs(record));

    if info == XLOG_RELMAP_UPDATE {
        let xlrec: *mut xl_relmap_update = XLogRecGetData(record) as *mut xl_relmap_update;
        let mut newmap: RelMapFile = ZERO_RELMAPFILE;
        let dbpath: *mut c_char;

        if (*xlrec).nbytes != core::mem::size_of::<RelMapFile>() as int32 {
            elog!(
                PANIC,
                "relmap_redo: wrong size {} in relmap update record",
                (*xlrec).nbytes
            );
        }
        memcpy(
            &raw mut newmap as *mut c_void,
            (*xlrec).data.as_ptr() as *const c_void,
            core::mem::size_of_val(&newmap),
        );

        /* We need to construct the pathname for this database */
        dbpath = GetDatabasePath((*xlrec).dbid, (*xlrec).tsid);

        /*
         * Write out the new map and send sinval, but of course don't write a
         * new WAL entry.  There's no surrounding transaction to tell to
         * preserve files, either.
         *
         * There shouldn't be anyone else updating relmaps during WAL replay,
         * but grab the lock to interlock against load_relmap_file().
         *
         * Note that we use the same WAL record for updating the relmap of an
         * existing database as we do for creating a new database. In the
         * latter case, taking the relmap log and sending sinval messages is
         * unnecessary, but harmless. If we wanted to avoid it, we could add a
         * flag to the WAL record to indicate which operation is being
         * performed.
         */
        LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
        write_relmap_file(
            &raw mut newmap,
            false,
            true,
            false,
            (*xlrec).dbid,
            (*xlrec).tsid,
            dbpath,
        );
        LWLockRelease(RelationMappingLock);

        pfree(dbpath as *mut c_void);
    } else {
        elog!(PANIC, "relmap_redo: unknown op code {}", info);
    }
}

// ----------------------------------------------------------------------------
// extern "C" libc functions
// ----------------------------------------------------------------------------
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

unsafe fn errno() -> c_int {
    unimplemented!() // TODO: errno access
}
unsafe fn set_errno(_e: c_int) {
    unimplemented!() // TODO: errno access
}

// ----------------------------------------------------------------------------
// Local stubs for unported helpers
// ----------------------------------------------------------------------------

const InvalidRelFileNumber: RelFileNumber = 0;
// ERROR / FATAL / PANIC come from crate::utils::elog via the prelude.
const O_RDONLY: c_int = 0; // TODO: fcntl.h
const O_WRONLY: c_int = 1; // TODO: fcntl.h
const O_CREAT: c_int = 0o100; // TODO: fcntl.h
const O_TRUNC: c_int = 0o1000; // TODO: fcntl.h
const PG_BINARY: c_int = 0; // TODO: port.h
const ENOSPC: c_int = 28; // TODO: errno.h

const allowSystemTableMods: bool = false; // TODO: utils/guc.h
const RM_RELMAP_ID: u8 = 0; // TODO: access/rmgrlist.h
const XLR_INFO_MASK: uint8 = 0x0F; // TODO: access/xlogrecord.h

#[derive(Copy, Clone)]
struct RelFileLocator {
    spcOid: Oid,
    dbOid: Oid,
    relNumber: RelFileNumber,
}

unsafe fn LWLockAcquire(_lock: *mut c_void, _mode: c_int) -> bool {
    unimplemented!() // TODO: storage/lwlock.h
}
unsafe fn LWLockRelease(_lock: *mut c_void) {
    unimplemented!() // TODO: storage/lwlock.h
}
unsafe fn LWLockHeldByMeInMode(_lock: *mut c_void, _mode: c_int) -> bool {
    unimplemented!() // TODO: storage/lwlock.h
}

// RelationMappingLock is a predefined LWLock pointer.
const RelationMappingLock: *mut c_void = core::ptr::null_mut(); // TODO: storage/lwlocknames.h
const LW_EXCLUSIVE: c_int = 0; // TODO: storage/lwlock.h
const LW_SHARED: c_int = 1; // TODO: storage/lwlock.h

unsafe fn IsBootstrapProcessingMode() -> bool {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn IsInParallelMode() -> bool {
    unimplemented!() // TODO: access/xact.h
}
unsafe fn GetCurrentTransactionNestLevel() -> c_int {
    unimplemented!() // TODO: access/xact.h
}

// InvalidOid comes from crate::postgres_ext via the prelude.
const GLOBALTABLESPACE_OID: Oid = 1664; // TODO: catalog/pg_tablespace_d.h

// MyDatabaseId / MyDatabaseTableSpace / DatabasePath are globals from miscadmin.h
static mut MyDatabaseId: Oid = 0; // TODO: miscadmin.h
static mut MyDatabaseTableSpace: Oid = 0; // TODO: miscadmin.h
const DatabasePath: *const c_char = core::ptr::null(); // TODO: miscadmin.h

unsafe fn OpenTransientFile(_filename: *const c_char, _flags: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}
unsafe fn durable_rename(_oldfile: *const c_char, _newfile: *const c_char, _elevel: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {
    unimplemented!() // TODO: utils/wait_event.h
}
unsafe fn pgstat_report_wait_end() {
    unimplemented!() // TODO: utils/wait_event.h
}
const WAIT_EVENT_RELATION_MAP_READ: u32 = 0; // TODO: utils/wait_event.h
const WAIT_EVENT_RELATION_MAP_WRITE: u32 = 0; // TODO: utils/wait_event.h
const WAIT_EVENT_RELATION_MAP_REPLACE: u32 = 0; // TODO: utils/wait_event.h

unsafe fn INIT_CRC32C(_crc: *mut pg_crc32c) {
    unimplemented!() // TODO: port/pg_crc32c.h
}
unsafe fn COMP_CRC32C(_crc: *mut pg_crc32c, _data: *const c_void, _len: usize) {
    unimplemented!() // TODO: port/pg_crc32c.h
}
unsafe fn FIN_CRC32C(_crc: *mut pg_crc32c) {
    unimplemented!() // TODO: port/pg_crc32c.h
}
unsafe fn EQ_CRC32C(_c1: pg_crc32c, _c2: pg_crc32c) -> bool {
    unimplemented!() // TODO: port/pg_crc32c.h
}

unsafe fn START_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn END_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterData(_data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogInsert(_rmid: u8, _info: u8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogFlush(_record: XLogRecPtr) {
    unimplemented!() // TODO: access/xlog.h
}

unsafe fn CacheInvalidateRelmap(_databaseId: Oid) {
    unimplemented!() // TODO: utils/inval.h
}
unsafe fn RelationPreserveStorage(_rlocator: RelFileLocator, _atCommit: bool) {
    unimplemented!() // TODO: catalog/storage.h
}

unsafe fn GetDatabasePath(_dbnode: Oid, _spcnode: Oid) -> *mut c_char {
    unimplemented!() // TODO: common/relpath.h
}

unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 {
    unimplemented!() // TODO: access/xlogreader.h
}
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char {
    unimplemented!() // TODO: access/xlogreader.h
}
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool {
    unimplemented!() // TODO: access/xlogreader.h
}
