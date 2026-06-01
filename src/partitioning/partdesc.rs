//! src/backend/partitioning/partdesc.c
//!
//! Support routines for manipulating partition descriptors
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!       src/backend/partitioning/partdesc.c

use crate::prelude::*;
use crate::{foreach, current_cell, IsA};
use crate::nodes::pg_list::lfirst;
use crate::nodes::pathnodes::PartitionBoundInfoData;
use crate::utils::rel::OpaquePtr;

use std::ffi::c_int;
use std::ptr;

// merged from src/include/partitioning/partdesc.h

/*
 * Information about partitions of a partitioned table.
 *
 * For partitioned tables where detached partitions exist, we only cache
 * descriptors that include all partitions, including detached; when we're
 * requested a descriptor without the detached partitions, we create one
 * afresh each time.  (The reason for this is that the set of detached
 * partitions that are visible to each caller depends on the snapshot it has,
 * so it's pretty much impossible to evict a descriptor from cache at the
 * right time.)
 */
#[repr(C)]
pub struct PartitionDescData {
    pub nparts: c_int,              /* Number of partitions */
    pub detached_exist: bool,       /* Are there any detached partitions? */
    pub oids: *mut Oid,             /* Array of 'nparts' elements containing
                                     * partition OIDs in order of their bounds */
    pub is_leaf: *mut bool,         /* Array of 'nparts' elements storing whether
                                     * the corresponding 'oids' element belongs to
                                     * a leaf partition or not */
    pub boundinfo: PartitionBoundInfo, /* collection of partition bounds */

    /* Caching fields to cache lookups in get_partition_for_tuple() */

    /*
     * Index into the PartitionBoundInfo's datum array for the last found
     * partition or -1 if none.
     */
    pub last_found_datum_index: c_int,

    /*
     * Partition index of the last found partition or -1 if none has been
     * found yet.
     */
    pub last_found_part_index: c_int,

    /*
     * For LIST partitioning, this is the number of times in a row that the
     * datum we're looking for a partition for matches the datum in the
     * last_found_datum_index index of the boundinfo->datums array.  For RANGE
     * partitioning, this is the number of times in a row we've found that the
     * datum we're looking for a partition for falls into the range of the
     * partition corresponding to the last_found_datum_index index of the
     * boundinfo->datums array.
     */
    pub last_found_count: c_int,
}

pub type PartitionDesc = *mut PartitionDescData;

// Stub forward types from partitioning/partdefs.h and utils/relcache.h.
pub type Relation = *mut crate::utils::rel::RelationData;
pub type PartitionBoundInfo = *mut PartitionBoundInfoData;
pub type PartitionKey = *mut PartitionKeyData;
pub type PartitionBoundSpec = *mut PartitionBoundSpecData;
pub type PartitionDirectory = *mut PartitionDirectoryData;

#[repr(C)]
pub struct PartitionKeyData {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct PartitionBoundSpecData {
    pub is_default: bool,
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct PartitionDirectoryData {
    pdir_mcxt: MemoryContext,
    pdir_hash: *mut HTAB,
    omit_detached: bool,
}

#[repr(C)]
struct PartitionDirectoryEntry {
    reloid: Oid,
    rel: Relation,
    pd: PartitionDesc,
}

/*
 * RelationGetPartitionDesc -- get partition descriptor, if relation is partitioned
 *
 * We keep two partdescs in relcache: rd_partdesc includes all partitions
 * (even those being concurrently marked detached), while rd_partdesc_nodetached
 * omits (some of) those.  We store the pg_inherits.xmin value for the latter,
 * to determine whether it can be validly reused in each case, since that
 * depends on the active snapshot.
 *
 * Note: we arrange for partition descriptors to not get freed until the
 * relcache entry's refcount goes to zero (see hacks in RelationClose,
 * RelationClearRelation, and RelationBuildPartitionDesc).  Therefore, even
 * though we hand back a direct pointer into the relcache entry, it's safe
 * for callers to continue to use that pointer as long as (a) they hold the
 * relation open, and (b) they hold a relation lock strong enough to ensure
 * that the data doesn't become stale.
 */
pub unsafe fn RelationGetPartitionDesc(rel: Relation, omit_detached: bool) -> PartitionDesc {
    Assert!((*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE);

    /*
     * If relcache has a partition descriptor, use that.  However, we can only
     * do so when we are asked to include all partitions including detached;
     * and also when we know that there are no detached partitions.
     *
     * If there is no active snapshot, detached partitions aren't omitted
     * either, so we can use the cached descriptor too in that case.
     */
    if likely(!(*rel).rd_partdesc.is_null()
        && (!(*((*rel).rd_partdesc as PartitionDesc)).detached_exist
            || !omit_detached
            || !ActiveSnapshotSet()))
    {
        return (*rel).rd_partdesc as PartitionDesc;
    }

    /*
     * If we're asked to omit detached partitions, we may be able to use a
     * cached descriptor too.  We determine that based on the pg_inherits.xmin
     * that was saved alongside that descriptor: if the xmin that was not in
     * progress for that active snapshot is also not in progress for the
     * current active snapshot, then we can use it.  Otherwise build one from
     * scratch.
     */
    if omit_detached && !(*rel).rd_partdesc_nodetached.is_null() && ActiveSnapshotSet() {
        let activesnap: Snapshot;

        Assert!(TransactionIdIsValid((*rel).rd_partdesc_nodetached_xmin));
        activesnap = GetActiveSnapshot();

        if !XidInMVCCSnapshot((*rel).rd_partdesc_nodetached_xmin, activesnap) {
            return (*rel).rd_partdesc_nodetached as PartitionDesc;
        }
    }

    RelationBuildPartitionDesc(rel, omit_detached)
}

/*
 * RelationBuildPartitionDesc
 *		Form rel's partition descriptor, and store in relcache entry
 *
 * Partition descriptor is a complex structure; to avoid complicated logic to
 * free individual elements whenever the relcache entry is flushed, we give it
 * its own memory context, a child of CacheMemoryContext, which can easily be
 * deleted on its own.  To avoid leaking memory in that context in case of an
 * error partway through this function, the context is initially created as a
 * child of CurTransactionContext and only re-parented to CacheMemoryContext
 * at the end, when no further errors are possible.  Also, we don't make this
 * context the current context except in very brief code sections, out of fear
 * that some of our callees allocate memory on their own which would be leaked
 * permanently.
 *
 * As a special case, partition descriptors that are requested to omit
 * partitions being detached (and which contain such partitions) are transient
 * and are not associated with the relcache entry.  Such descriptors only last
 * through the requesting Portal, so we use the corresponding memory context
 * for them.
 */
unsafe fn RelationBuildPartitionDesc(rel: Relation, omit_detached: bool) -> PartitionDesc {
    let partdesc: PartitionDesc;
    let mut boundinfo: PartitionBoundInfo = ptr::null_mut();
    let mut inhoids: *mut List;
    let mut boundspecs: *mut PartitionBoundSpec = ptr::null_mut();
    let mut oids: *mut Oid = ptr::null_mut();
    let mut is_leaf: *mut bool = ptr::null_mut();
    let mut detached_exist: bool;
    let is_omit: bool;
    let mut detached_xmin: TransactionId;
    let mut i: c_int;
    let mut nparts: c_int;
    let mut retried: bool = false;
    let key: PartitionKey = RelationGetPartitionKey(rel);
    let new_pdcxt: MemoryContext;
    let oldcxt: MemoryContext;
    let mut mapping: *mut c_int = ptr::null_mut();

    'retry: loop {
        /*
         * Get partition oids from pg_inherits.  This uses a single snapshot to
         * fetch the list of children, so while more children may be getting added
         * or removed concurrently, whatever this function returns will be
         * accurate as of some well-defined point in time.
         */
        detached_exist = false;
        detached_xmin = InvalidTransactionId;
        inhoids = find_inheritance_children_extended(
            RelationGetRelid(rel),
            omit_detached,
            NoLock,
            &mut detached_exist,
            &mut detached_xmin,
        );

        nparts = list_length(inhoids);

        /* Allocate working arrays for OIDs, leaf flags, and boundspecs. */
        if nparts > 0 {
            oids = palloc(nparts as usize * size_of::<Oid>()) as *mut Oid;
            is_leaf = palloc(nparts as usize * size_of::<bool>()) as *mut bool;
            boundspecs =
                palloc(nparts as usize * size_of::<*mut PartitionBoundSpec>()) as *mut PartitionBoundSpec;
        }

        /* Collect bound spec nodes for each partition. */
        i = 0;
        foreach!(cell, inhoids, {
            let inhrelid: Oid = lfirst_oid(current_cell!(cell));
            let mut tuple: HeapTuple;
            let mut boundspec: PartitionBoundSpec = ptr::null_mut();

            /* Try fetching the tuple from the catcache, for speed. */
            tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(inhrelid));
            if HeapTupleIsValid(tuple) {
                let datum: Datum;
                let mut isnull: bool = false;

                datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relpartbound, &mut isnull);
                if !isnull {
                    boundspec = stringToNode(TextDatumGetCString(datum)) as PartitionBoundSpec;
                }
                ReleaseSysCache(tuple);
            }

            /*
             * Two problems are possible here.  First, a concurrent ATTACH
             * PARTITION might be in the process of adding a new partition, but
             * the syscache doesn't have it, or its copy of it does not yet have
             * its relpartbound set.  We cannot just AcceptInvalidationMessages(),
             * because the other process might have already removed itself from
             * the ProcArray but not yet added its invalidation messages to the
             * shared queue.  We solve this problem by reading pg_class directly
             * for the desired tuple.
             *
             * If the partition recently detached is also dropped, we get no tuple
             * from the scan.  In that case, we also retry, and next time through
             * here, we don't see that partition anymore.
             *
             * The other problem is that DETACH CONCURRENTLY is in the process of
             * removing a partition, which happens in two steps: first it marks it
             * as "detach pending", commits, then unsets relpartbound.  If
             * find_inheritance_children_extended included that partition but we
             * below we see that DETACH CONCURRENTLY has reset relpartbound for
             * it, we'd see an inconsistent view.  (The inconsistency is seen
             * because table_open below reads invalidation messages.)  We protect
             * against this by retrying find_inheritance_children_extended().
             */
            if boundspec.is_null() {
                let pg_class: Relation;
                let scan: SysScanDesc;
                let mut key: [ScanKeyData; 1] = std::mem::zeroed();

                pg_class = table_open(RelationRelationId, AccessShareLock);
                ScanKeyInit(
                    &mut key[0],
                    Anum_pg_class_oid,
                    BTEqualStrategyNumber as StrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(inhrelid),
                );
                scan = systable_beginscan(
                    pg_class,
                    ClassOidIndexId,
                    true,
                    ptr::null_mut(),
                    1,
                    key.as_mut_ptr(),
                );

                /*
                 * We could get one tuple from the scan (the normal case), or zero
                 * tuples if the table has been dropped meanwhile.
                 */
                tuple = systable_getnext(scan);
                if HeapTupleIsValid(tuple) {
                    let datum: Datum;
                    let mut isnull: bool = false;

                    datum = heap_getattr(
                        tuple,
                        Anum_pg_class_relpartbound as c_int,
                        RelationGetDescr(pg_class),
                        &mut isnull,
                    );
                    if !isnull {
                        boundspec = stringToNode(TextDatumGetCString(datum)) as PartitionBoundSpec;
                    }
                }
                systable_endscan(scan);
                table_close(pg_class, AccessShareLock);

                /*
                 * If we still don't get a relpartbound value (either because
                 * boundspec is null or because there was no tuple), then it must
                 * be because of DETACH CONCURRENTLY.  Restart from the top, as
                 * explained above.  We only do this once, for two reasons: first,
                 * only one DETACH CONCURRENTLY session could affect us at a time,
                 * since each of them would have to wait for the snapshot under
                 * which this is running; and second, to avoid possible infinite
                 * loops in case of catalog corruption.
                 *
                 * Note that the current memory context is short-lived enough, so
                 * we needn't worry about memory leaks here.
                 */
                if boundspec.is_null() && !retried {
                    AcceptInvalidationMessages();
                    retried = true;
                    continue 'retry;
                }
            }

            /* Sanity checks. */
            if boundspec.is_null() {
                elog!(ERROR, "missing relpartbound for relation {}", inhrelid);
            }
            if !IsA!(boundspec, T_PartitionBoundSpec) {
                elog!(ERROR, "invalid relpartbound for relation {}", inhrelid);
            }

            /*
             * If the PartitionBoundSpec says this is the default partition, its
             * OID should match pg_partitioned_table.partdefid; if not, the
             * catalog is corrupt.
             */
            if (*boundspec).is_default {
                let partdefid: Oid;

                partdefid = get_default_partition_oid(RelationGetRelid(rel));
                if partdefid != inhrelid {
                    elog!(ERROR, "expected partdefid {}, but got {}", inhrelid, partdefid);
                }
            }

            /* Save results. */
            *oids.offset(i as isize) = inhrelid;
            *is_leaf.offset(i as isize) =
                get_rel_relkind(inhrelid) != RELKIND_PARTITIONED_TABLE;
            *boundspecs.offset(i as isize) = boundspec;
            i += 1;
        });

        break;
    }

    /*
     * Create PartitionBoundInfo and mapping, working in the caller's context.
     * This could fail, but we haven't done any damage if so.
     */
    if nparts > 0 {
        boundinfo = partition_bounds_create(boundspecs, nparts, key, &mut mapping);
    }

    /*
     * Now build the actual relcache partition descriptor, copying all the
     * data into a new, small context.  As per above comment, we don't make
     * this a long-lived context until it's finished.
     */
    new_pdcxt = AllocSetContextCreate(
        CurTransactionContext,
        c"partition descriptor".as_ptr(),
        ALLOCSET_SMALL_SIZES,
    );
    MemoryContextCopyAndSetIdentifier(new_pdcxt, RelationGetRelationName(rel));

    partdesc =
        MemoryContextAllocZero(new_pdcxt, size_of::<PartitionDescData>()) as *mut PartitionDescData;
    (*partdesc).nparts = nparts;
    (*partdesc).detached_exist = detached_exist;
    /* If there are no partitions, the rest of the partdesc can stay zero */
    if nparts > 0 {
        oldcxt = MemoryContextSwitchTo(new_pdcxt);
        (*partdesc).boundinfo = partition_bounds_copy(boundinfo, key);

        /* Initialize caching fields for speeding up ExecFindPartition */
        (*partdesc).last_found_datum_index = -1;
        (*partdesc).last_found_part_index = -1;
        (*partdesc).last_found_count = 0;

        (*partdesc).oids = palloc(nparts as usize * size_of::<Oid>()) as *mut Oid;
        (*partdesc).is_leaf = palloc(nparts as usize * size_of::<bool>()) as *mut bool;

        /*
         * Assign OIDs from the original array into mapped indexes of the
         * result array.  The order of OIDs in the former is defined by the
         * catalog scan that retrieved them, whereas that in the latter is
         * defined by canonicalized representation of the partition bounds.
         * Also save leaf-ness of each partition.
         */
        i = 0;
        while i < nparts {
            let index: c_int = *mapping.offset(i as isize);

            *(*partdesc).oids.offset(index as isize) = *oids.offset(i as isize);
            *(*partdesc).is_leaf.offset(index as isize) = *is_leaf.offset(i as isize);
            i += 1;
        }
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * Are we working with the partdesc that omits the detached partition, or
     * the one that includes it?
     *
     * Note that if a partition was found by the catalog's scan to have been
     * detached, but the pg_inherit tuple saying so was not visible to the
     * active snapshot (find_inheritance_children_extended will not have set
     * detached_xmin in that case), we consider there to be no "omittable"
     * detached partitions.
     */
    is_omit = omit_detached
        && detached_exist
        && ActiveSnapshotSet()
        && TransactionIdIsValid(detached_xmin);

    /*
     * We have a fully valid partdesc.  Reparent it so that it has the right
     * lifespan.
     */
    MemoryContextSetParent(new_pdcxt, CacheMemoryContext);

    /*
     * Store it into relcache.
     *
     * But first, a kluge: if there's an old context for this type of
     * descriptor, it contains an old partition descriptor that may still be
     * referenced somewhere.  Preserve it, while not leaking it, by
     * reattaching it as a child context of the new one.  Eventually it will
     * get dropped by either RelationClose or RelationClearRelation. (We keep
     * the regular partdesc in rd_pdcxt, and the partdesc-excluding-
     * detached-partitions in rd_pddcxt.)
     */
    if is_omit {
        if !(*rel).rd_pddcxt.is_null() {
            MemoryContextSetParent((*rel).rd_pddcxt as MemoryContext, new_pdcxt);
        }
        (*rel).rd_pddcxt = new_pdcxt as OpaquePtr;
        (*rel).rd_partdesc_nodetached = partdesc as OpaquePtr;

        /*
         * For partdescs built excluding detached partitions, which we save
         * separately, we also record the pg_inherits.xmin of the detached
         * partition that was omitted; this informs a future potential user of
         * such a cached partdesc to only use it after cross-checking that the
         * xmin is indeed visible to the snapshot it is going to be working
         * with.
         */
        Assert!(TransactionIdIsValid(detached_xmin));
        (*rel).rd_partdesc_nodetached_xmin = detached_xmin;
    } else {
        if !(*rel).rd_pdcxt.is_null() {
            MemoryContextSetParent((*rel).rd_pdcxt as MemoryContext, new_pdcxt);
        }
        (*rel).rd_pdcxt = new_pdcxt as OpaquePtr;
        (*rel).rd_partdesc = partdesc as OpaquePtr;
    }

    partdesc
}

/*
 * CreatePartitionDirectory
 *		Create a new partition directory object.
 */
pub unsafe fn CreatePartitionDirectory(mcxt: MemoryContext, omit_detached: bool) -> PartitionDirectory {
    let oldcontext: MemoryContext = MemoryContextSwitchTo(mcxt);
    let pdir: PartitionDirectory;
    let mut ctl: HASHCTL = std::mem::zeroed();

    pdir = palloc(size_of::<PartitionDirectoryData>()) as PartitionDirectory;
    (*pdir).pdir_mcxt = mcxt;

    ctl.keysize = size_of::<Oid>() as Size;
    ctl.entrysize = size_of::<PartitionDirectoryEntry>() as Size;
    ctl.hcxt = mcxt;

    (*pdir).pdir_hash = hash_create(
        c"partition directory".as_ptr(),
        256,
        &mut ctl,
        (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT) as c_int,
    );
    (*pdir).omit_detached = omit_detached;

    MemoryContextSwitchTo(oldcontext);
    pdir
}

/*
 * PartitionDirectoryLookup
 *		Look up the partition descriptor for a relation in the directory.
 *
 * The purpose of this function is to ensure that we get the same
 * PartitionDesc for each relation every time we look it up.  In the
 * face of concurrent DDL, different PartitionDescs may be constructed with
 * different views of the catalog state, but any single particular OID
 * will always get the same PartitionDesc for as long as the same
 * PartitionDirectory is used.
 */
pub unsafe fn PartitionDirectoryLookup(pdir: PartitionDirectory, rel: Relation) -> PartitionDesc {
    let pde: *mut PartitionDirectoryEntry;
    let mut relid: Oid = RelationGetRelid(rel);
    let mut found: bool = false;

    pde = hash_search(
        (*pdir).pdir_hash,
        &mut relid as *mut Oid as *mut c_void,
        HASHACTION::HASH_ENTER,
        &mut found,
    ) as *mut PartitionDirectoryEntry;
    if !found {
        /*
         * We must keep a reference count on the relation so that the
         * PartitionDesc to which we are pointing can't get destroyed.
         */
        RelationIncrementReferenceCount(rel);
        (*pde).rel = rel;
        (*pde).pd = RelationGetPartitionDesc(rel, (*pdir).omit_detached);
        Assert!(!(*pde).pd.is_null());
    }
    (*pde).pd
}

/*
 * DestroyPartitionDirectory
 *		Destroy a partition directory.
 *
 * Release the reference counts we're holding.
 */
pub unsafe fn DestroyPartitionDirectory(pdir: PartitionDirectory) {
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut pde: *mut PartitionDirectoryEntry;

    hash_seq_init(&mut status, (*pdir).pdir_hash);
    loop {
        pde = hash_seq_search(&mut status) as *mut PartitionDirectoryEntry;
        if pde.is_null() {
            break;
        }
        RelationDecrementReferenceCount((*pde).rel);
    }
}

/*
 * get_default_oid_from_partdesc
 *
 * Given a partition descriptor, return the OID of the default partition, if
 * one exists; else, return InvalidOid.
 */
pub unsafe fn get_default_oid_from_partdesc(partdesc: PartitionDesc) -> Oid {
    if !partdesc.is_null()
        && !(*partdesc).boundinfo.is_null()
        && partition_bound_has_default((*partdesc).boundinfo)
    {
        return *(*partdesc).oids.offset(
            (*((*partdesc).boundinfo as *mut crate::partitioning::partbounds::PartitionBoundInfoFull))
                .default_index as isize,
        );
    }

    InvalidOid
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

pub use crate::utils::palloc::MemoryContext;
pub type Snapshot = *mut SnapshotData;
pub type HeapTuple = *mut crate::access::htup_details::HeapTupleData;
pub type SysScanDesc = *mut SysScanDescData;
pub type HTAB = HTABData;
pub type StrategyNumber = u16;

#[repr(C)]
pub struct SnapshotData {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct SysScanDescData {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct HTABData {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct ScanKeyData {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
    pub hcxt: MemoryContext,
}
#[repr(C)]
pub struct HASH_SEQ_STATUS {
    _opaque: [u8; 0],
}
#[allow(non_camel_case_types)]
#[repr(C)]
pub enum HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}
pub use HASHACTION::*;

pub const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
pub const InvalidTransactionId: TransactionId = 0;
pub const NoLock: c_int = 0;
pub const AccessShareLock: c_int = 1;
pub const RELOID: c_int = 0;
pub const Anum_pg_class_relpartbound: c_int = 0;
pub const Anum_pg_class_oid: c_int = 0;
pub const BTEqualStrategyNumber: c_int = 3;
pub const F_OIDEQ: Oid = 184;
pub const RelationRelationId: Oid = 1259;
pub const ClassOidIndexId: Oid = 2662;
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_BLOBS: c_int = 0x0010;
pub const HASH_CONTEXT: c_int = 0x0040;

#[inline]
unsafe fn likely(b: bool) -> bool {
    b
}

#[allow(non_snake_case)]
unsafe fn ActiveSnapshotSet() -> bool {
    unimplemented!() // TODO: utils/snapmgr.c
}
#[allow(non_snake_case)]
unsafe fn GetActiveSnapshot() -> Snapshot {
    unimplemented!() // TODO: utils/snapmgr.c
}
#[allow(non_snake_case)]
unsafe fn XidInMVCCSnapshot(_xid: TransactionId, _snap: Snapshot) -> bool {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
#[allow(non_snake_case)]
unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}
#[allow(non_snake_case)]
unsafe fn RelationGetPartitionKey(_rel: Relation) -> PartitionKey {
    unimplemented!() // TODO: utils/cache/partcache.c
}
#[allow(non_snake_case)]
unsafe fn RelationGetRelid(rel: Relation) -> Oid {
    (*rel).rd_id
}
#[allow(non_snake_case)]
unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc {
    unimplemented!() // TODO: utils/rel.h
}
#[allow(non_snake_case)]
unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}
#[allow(non_snake_case)]
unsafe fn RelationIncrementReferenceCount(_rel: Relation) {
    unimplemented!() // TODO: utils/cache/relcache.c
}
#[allow(non_snake_case)]
unsafe fn RelationDecrementReferenceCount(_rel: Relation) {
    unimplemented!() // TODO: utils/cache/relcache.c
}
unsafe fn find_inheritance_children_extended(
    _parentrelid: Oid,
    _omit_detached: bool,
    _lockmode: c_int,
    _detached_exist: *mut bool,
    _detached_xmin: *mut TransactionId,
) -> *mut List {
    unimplemented!() // TODO: catalog/pg_inherits.c
}
#[allow(non_snake_case)]
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
#[allow(non_snake_case)]
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
#[allow(non_snake_case)]
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn stringToNode(_str: *mut c_char) -> *mut crate::nodes::nodes::Node {
    unimplemented!() // TODO: nodes/read.c
}
#[allow(non_snake_case)]
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO: utils/builtins.h
}
#[allow(non_snake_case)]
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}
#[allow(non_snake_case)]
unsafe fn AcceptInvalidationMessages() {
    unimplemented!() // TODO: utils/cache/inval.c
}
#[allow(non_snake_case)]
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}
#[allow(non_snake_case)]
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/table/table.c
}
#[allow(non_snake_case)]
unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attributeNumber: c_int,
    _strategy: StrategyNumber,
    _procedure: Oid,
    _argument: Datum,
) {
    unimplemented!() // TODO: access/common/scankey.c
}
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!() // TODO: access/index/genam.c
}
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!() // TODO: access/index/genam.c
}
unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!() // TODO: access/index/genam.c
}
unsafe fn heap_getattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupleDesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: access/common/heaptuple.c
}
#[allow(non_snake_case)]
unsafe fn get_default_partition_oid(_parentId: Oid) -> Oid {
    unimplemented!() // TODO: catalog/partition.c
}
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn partition_bounds_create(
    _boundspecs: *mut PartitionBoundSpec,
    _nparts: c_int,
    _key: PartitionKey,
    _mapping: *mut *mut c_int,
) -> PartitionBoundInfo {
    unimplemented!() // TODO: partitioning/partbounds.c
}
unsafe fn partition_bounds_copy(
    _src: PartitionBoundInfo,
    _key: PartitionKey,
) -> PartitionBoundInfo {
    unimplemented!() // TODO: partitioning/partbounds.c
}
unsafe fn partition_bound_has_default(_boundinfo: PartitionBoundInfo) -> bool {
    unimplemented!() // TODO: partitioning/partbounds.h
}
#[allow(non_snake_case)]
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _flags: c_int,
) -> MemoryContext {
    unimplemented!() // TODO: utils/mmgr/aset.c
}
pub const ALLOCSET_SMALL_SIZES: c_int = 0;
#[allow(non_snake_case)]
unsafe fn MemoryContextCopyAndSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
#[allow(non_snake_case)]
unsafe fn MemoryContextAllocZero(_context: MemoryContext, _size: Size) -> *mut c_void {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
#[allow(non_snake_case)]
unsafe fn MemoryContextSetParent(_context: MemoryContext, _new_parent: MemoryContext) {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *mut c_void,
    _action: HASHACTION,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

use std::ffi::{c_char, c_long, c_void};

// Externs that are normally provided elsewhere; refer to canonical types.
use crate::nodes::pg_list::List;
type TupleDesc = *mut crate::access::common::tupdesc::TupleDescData;

#[allow(non_snake_case)]
unsafe fn lfirst_oid(cell: *mut crate::nodes::pg_list::ListCell) -> Oid {
    (*cell).oid_value
}
#[allow(non_snake_case)]
unsafe fn list_length(l: *const List) -> c_int {
    if l.is_null() {
        0
    } else {
        (*l).length
    }
}

// Stub global memory contexts.
extern "C" {
    pub static mut CurTransactionContext: MemoryContext;
    pub static mut CacheMemoryContext: MemoryContext;
}
