//! Translation of postgres/src/include/catalog/pg_inherits.h
//!
//! FormData_pg_inherits - records table inheritance / partitioning parent links.
//! This header has no CATALOG_VARLEN section, so all columns are fixed-layout.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{
    HeapTuple, HeapTupleHeaderGetXmin, HeapTupleIsValid, GETSTRUCT,
};
use crate::access::index::genam::SysScanDesc;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};
use crate::access::transam::xact::InvalidTransactionId;
use crate::access::transam::transam::TransactionIdFollows;
use crate::catalog::catalog_oids::InheritsRelationId;
use crate::catalog::indexing::{CatalogTupleDelete, CatalogTupleInsert};
use crate::catalog::pg_class::Form_pg_class;
use crate::nodes::pg_list::{
    lappend_int, lappend_oid, lfirst_int_mut, lfirst_oid, list_free, list_length,
    list_member_oid, list_nth_cell, List, ListCell,
};
use crate::parser::parse_type::{typeOrDomainTypeRelid, typeidTypeRelid};
use crate::port::qsort::pg_qsort;
use crate::storage::lmgr::lmgr::{LockRelationOid, UnlockRelationOid};
use crate::storage::lockdefs::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use crate::utils::adt::oid::oid_cmp;
use crate::utils::hash::dynahash::{
    hash_create, hash_destroy, hash_search, HASHCTL, HASH_BLOBS, HASH_CONTEXT, HASH_ELEM,
    HASH_ENTER, HTAB,
};
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1};
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::{current_cell, foreach, list_make1_int, list_make1_oid};

/*
 * snapmgr.h helpers - the utils/time/snapmgr module is not yet wired into the
 * crate tree, so stub these locally until it lands.
 */
// TODO(pg-port): import from utils::time::snapmgr once wired.
unsafe fn ActiveSnapshotSet() -> bool {
    unimplemented!("TODO(pg-port): ActiveSnapshotSet")
}
// TODO(pg-port): import from utils::time::snapmgr once wired.
unsafe fn GetActiveSnapshot() -> Snapshot {
    unimplemented!("TODO(pg-port): GetActiveSnapshot")
}
// TODO(pg-port): import from utils::time::snapmgr once wired.
unsafe fn XidInMVCCSnapshot(_xid: TransactionId, _snapshot: Snapshot) -> bool {
    unimplemented!("TODO(pg-port): XidInMVCCSnapshot")
}

/*
 * access/genam.h scan helpers and access/htup_details.h tuple helpers.
 *
 * genam's systable_* use a placeholder `HeapTuple = *mut c_void` alias; we
 * work with the real HeapTupleData here, so wrap the genam entry points to
 * return the concrete pointer type.
 */
use crate::utils::snapshot::SnapshotData;

type Snapshot = *mut SnapshotData;

unsafe fn systable_beginscan(
    heap_relation: Relation,
    index_id: Oid,
    index_ok: bool,
    snapshot: Snapshot,
    nkeys: c_int,
    key: ScanKey,
) -> SysScanDesc {
    crate::access::index::genam::systable_beginscan(
        heap_relation,
        index_id,
        index_ok,
        snapshot as *mut c_void,
        nkeys,
        key,
    )
}

unsafe fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple {
    crate::access::index::genam::systable_getnext(sysscan) as HeapTuple
}

unsafe fn systable_endscan(sysscan: SysScanDesc) {
    crate::access::index::genam::systable_endscan(sysscan)
}

/*
 * Catalog column / index OIDs normally produced from generated headers
 * (pg_inherits.h, indexing.h), which are not all ported yet. Values match
 * PostgreSQL 18.3.
 */
// TODO(pg-port): replace with generated Anum_pg_inherits_inhrelid.
const Anum_pg_inherits_inhrelid: AttrNumber = 1;
// TODO(pg-port): replace with generated Anum_pg_inherits_inhparent.
const Anum_pg_inherits_inhparent: AttrNumber = 2;
// TODO(pg-port): replace with generated Anum_pg_inherits_inhseqno.
const Anum_pg_inherits_inhseqno: AttrNumber = 3;
// TODO(pg-port): replace with generated Anum_pg_inherits_inhdetachpending.
const Anum_pg_inherits_inhdetachpending: AttrNumber = 4;
// TODO(pg-port): replace with generated Natts_pg_inherits.
const Natts_pg_inherits: usize = 4;
// TODO(pg-port): replace with generated InheritsParentIndexId (indexing.h).
const InheritsParentIndexId: Oid = 2187;
// TODO(pg-port): replace with generated InheritsRelidSeqnoIndexId (indexing.h).
const InheritsRelidSeqnoIndexId: Oid = 2680;

/*
 * fmgr OID normally produced from utils/fmgroids.h, not ported yet.
 * Value matches PostgreSQL 18.3.
 */
// TODO(pg-port): replace with generated F_OIDEQ.
const F_OIDEQ: RegProcedure = 184;

// TODO(pg-port): replace with generated RELOID syscache id (syscache.h).
const RELOID: c_int = 52;

// TODO(pg-port): SearchSysCacheExists1 (utils/cache/syscache.c not ported as macro yet).
unsafe fn SearchSysCacheExists1(_cacheId: c_int, _key1: Datum) -> bool {
    unimplemented!("TODO(pg-port): SearchSysCacheExists1")
}

/*
 * Entry of a hash table used in find_all_inheritors. See below.
 */
#[repr(C)]
struct SeenRelsEntry {
    rel_id: Oid,        /* relation oid */
    list_index: c_int,  /* its position in output list(s) */
}

/*
 * find_inheritance_children
 *
 * Returns a list containing the OIDs of all relations which
 * inherit *directly* from the relation with OID 'parentrelId'.
 *
 * The specified lock type is acquired on each child relation (but not on the
 * given rel; caller should already have locked it).  If lockmode is NoLock
 * then no locks are acquired, but caller must beware of race conditions
 * against possible DROPs of child relations.
 *
 * Partitions marked as being detached are omitted; see
 * find_inheritance_children_extended for details.
 */
pub unsafe fn find_inheritance_children(parentrelId: Oid, lockmode: LOCKMODE) -> *mut List {
    find_inheritance_children_extended(parentrelId, true, lockmode, null_mut(), null_mut())
}

/*
 * find_inheritance_children_extended
 *
 * As find_inheritance_children, with more options regarding detached
 * partitions.
 *
 * If a partition's pg_inherits row is marked "detach pending",
 * *detached_exist (if not null) is set true.
 *
 * If omit_detached is true and there is an active snapshot (not the same as
 * the catalog snapshot used to scan pg_inherits!) and a pg_inherits tuple
 * marked "detach pending" is visible to that snapshot, then that partition is
 * omitted from the output list.  This makes partitions invisible depending on
 * whether the transaction that marked those partitions as detached appears
 * committed to the active snapshot.  In addition, *detached_xmin (if not null)
 * is set to the xmin of the row of the detached partition.
 */
pub unsafe fn find_inheritance_children_extended(
    parentrelId: Oid,
    omit_detached: bool,
    lockmode: LOCKMODE,
    detached_exist: *mut bool,
    detached_xmin: *mut TransactionId,
) -> *mut List {
    let mut list: *mut List = null_mut(); /* NIL */
    let relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let mut inheritsTuple: HeapTuple;
    let mut inhrelid: Oid;
    let mut oidarr: *mut Oid;
    let mut maxoids: c_int;
    let mut numoids: c_int;
    let mut i: c_int;

    /*
     * Can skip the scan if pg_class shows the relation has never had a
     * subclass.
     */
    if !has_subclass(parentrelId) {
        return null_mut(); /* NIL */
    }

    /*
     * Scan pg_inherits and build a working array of subclass OIDs.
     */
    maxoids = 32;
    oidarr = palloc(maxoids as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    numoids = 0;

    relation = table_open(InheritsRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(parentrelId),
    );

    scan = systable_beginscan(relation, InheritsParentIndexId, true, null_mut(), 1, key.as_mut_ptr());

    loop {
        inheritsTuple = systable_getnext(scan);
        if inheritsTuple.is_null() {
            break;
        }

        /*
         * Cope with partitions concurrently being detached.  When we see a
         * partition marked "detach pending", we omit it from the returned set
         * of visible partitions if caller requested that and the tuple's xmin
         * does not appear in progress to the active snapshot.  (If there's no
         * active snapshot set, that means we're not running a user query, so
         * it's OK to always include detached partitions in that case; if the
         * xmin is still running to the active snapshot, then the partition
         * has not been detached yet and so we include it.)
         *
         * The reason for this hack is that we want to avoid seeing the
         * partition as alive in RI queries during REPEATABLE READ or
         * SERIALIZABLE transactions: such queries use a different snapshot
         * than the one used by regular (user) queries.
         */
        if (*(GETSTRUCT(inheritsTuple) as Form_pg_inherits)).inhdetachpending {
            if !detached_exist.is_null() {
                *detached_exist = true;
            }

            if omit_detached && ActiveSnapshotSet() {
                let xmin: TransactionId;
                let snap;

                xmin = HeapTupleHeaderGetXmin((*inheritsTuple).t_data);
                snap = GetActiveSnapshot();

                if !XidInMVCCSnapshot(xmin, snap) {
                    if !detached_xmin.is_null() {
                        /*
                         * Two detached partitions should not occur (see
                         * checks in MarkInheritDetached), but if they do,
                         * track the newer of the two.  Make sure to warn the
                         * user, so that they can clean up.  Since this is
                         * just a cross-check against potentially corrupt
                         * catalogs, we don't make it a full-fledged error
                         * message.
                         */
                        if *detached_xmin != InvalidTransactionId {
                            elog!(WARNING, "more than one partition pending detach found for table with OID {}", parentrelId);
                            if TransactionIdFollows(xmin, *detached_xmin) {
                                *detached_xmin = xmin;
                            }
                        } else {
                            *detached_xmin = xmin;
                        }
                    }

                    /* Don't add the partition to the output list */
                    continue;
                }
            }
        }

        inhrelid = (*(GETSTRUCT(inheritsTuple) as Form_pg_inherits)).inhrelid;
        if numoids >= maxoids {
            maxoids *= 2;
            oidarr = repalloc(
                oidarr as *mut c_void,
                maxoids as usize * core::mem::size_of::<Oid>(),
            ) as *mut Oid;
        }
        *oidarr.add(numoids as usize) = inhrelid;
        numoids += 1;
    }

    systable_endscan(scan);

    table_close(relation, AccessShareLock);

    /*
     * If we found more than one child, sort them by OID.  This ensures
     * reasonably consistent behavior regardless of the vagaries of an
     * indexscan.  This is important since we need to be sure all backends
     * lock children in the same order to avoid needless deadlocks.
     */
    if numoids > 1 {
        pg_qsort(
            oidarr as *mut c_void,
            numoids as usize,
            core::mem::size_of::<Oid>(),
            oid_cmp,
        );
    }

    /*
     * Acquire locks and build the result list.
     */
    i = 0;
    while i < numoids {
        inhrelid = *oidarr.add(i as usize);

        if lockmode != NoLock {
            /* Get the lock to synchronize against concurrent drop */
            LockRelationOid(inhrelid, lockmode);

            /*
             * Now that we have the lock, double-check to see if the relation
             * really exists or not.  If not, assume it was dropped while we
             * waited to acquire lock, and ignore it.
             */
            if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(inhrelid)) {
                /* Release useless lock */
                UnlockRelationOid(inhrelid, lockmode);
                /* And ignore this relation */
                i += 1;
                continue;
            }
        }

        list = lappend_oid(list, inhrelid);
        i += 1;
    }

    pfree(oidarr as *mut c_void);

    list
}

/*
 * find_all_inheritors -
 *		Returns a list of relation OIDs including the given rel plus
 *		all relations that inherit from it, directly or indirectly.
 *		Optionally, it also returns the number of parents found for
 *		each such relation within the inheritance tree rooted at the
 *		given rel.
 *
 * The specified lock type is acquired on all child relations (but not on the
 * given rel; caller should already have locked it).  If lockmode is NoLock
 * then no locks are acquired, but caller must beware of race conditions
 * against possible DROPs of child relations.
 *
 * NB - No current callers of this routine are interested in children being
 * concurrently detached, so there's no provision to include them.
 */
pub unsafe fn find_all_inheritors(
    parentrelId: Oid,
    lockmode: LOCKMODE,
    numparents: *mut *mut List,
) -> *mut List {
    /* hash table for O(1) rel_oid -> rel_numparents cell lookup */
    let seen_rels: *mut HTAB;
    let mut ctl: HASHCTL = core::mem::zeroed();
    let mut rels_list: *mut List;
    let rel_numparents: *mut List;
    let l: *mut ListCell;

    ctl.keysize = core::mem::size_of::<Oid>() as Size;
    ctl.entrysize = core::mem::size_of::<SeenRelsEntry>() as Size;
    ctl.hcxt = CurrentMemoryContext;

    seen_rels = hash_create(
        c"find_all_inheritors temporary table".as_ptr(),
        32, /* start small and extend */
        &mut ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    /*
     * We build a list starting with the given rel and adding all direct and
     * indirect children.  We can use a single list as both the record of
     * already-found rels and the agenda of rels yet to be scanned for more
     * children.  This is a bit tricky but works because the foreach() macro
     * doesn't fetch the next list element until the bottom of the loop.  Note
     * that we can't keep pointers into the output lists; but an index is
     * sufficient.
     */
    rels_list = list_make1_oid(parentrelId);
    rel_numparents = list_make1_int(0);

    foreach!(l, rels_list, {
        let currentrel: Oid = lfirst_oid(current_cell!(l));
        let currentchildren: *mut List;
        let lc: *mut ListCell;

        /* Get the direct children of this rel */
        currentchildren = find_inheritance_children(currentrel, lockmode);

        /*
         * Add to the queue only those children not already seen. This avoids
         * making duplicate entries in case of multiple inheritance paths from
         * the same parent.  (It'll also keep us from getting into an infinite
         * loop, though theoretically there can't be any cycles in the
         * inheritance graph anyway.)
         */
        foreach!(lc, currentchildren, {
            let child_oid: Oid = lfirst_oid(current_cell!(lc));
            let mut found: bool = false;
            let hash_entry: *mut SeenRelsEntry;

            hash_entry = hash_search(
                seen_rels,
                &child_oid as *const Oid as *const c_void,
                HASH_ENTER,
                &mut found,
            ) as *mut SeenRelsEntry;
            if found {
                /* if the rel is already there, bump number-of-parents counter */
                let numparents_cell: *mut ListCell;

                numparents_cell = list_nth_cell(rel_numparents, (*hash_entry).list_index);
                *lfirst_int_mut(numparents_cell) += 1;
            } else {
                /* if it's not there, add it. expect 1 parent, initially. */
                (*hash_entry).list_index = list_length(rels_list);
                rels_list = lappend_oid(rels_list, child_oid);
                let _ = lappend_int(rel_numparents, 1);
            }
        });
    });

    if !numparents.is_null() {
        *numparents = rel_numparents;
    } else {
        list_free(rel_numparents);
    }

    hash_destroy(seen_rels);

    rels_list
}

/*
 * has_subclass - does this relation have any children?
 *
 * In the current implementation, has_subclass returns whether a
 * particular class *might* have a subclass. It will not return the
 * correct result if a class had a subclass which was later dropped.
 * This is because relhassubclass in pg_class is not updated immediately
 * when a subclass is dropped, primarily because of concurrency concerns.
 *
 * Currently has_subclass is only used as an efficiency hack to skip
 * unnecessary inheritance searches, so this is OK.  Note that ANALYZE
 * on a childless table will clean up the obsolete relhassubclass flag.
 *
 * Although this doesn't actually touch pg_inherits, it seems reasonable
 * to keep it here since it's normally used with the other routines here.
 */
pub unsafe fn has_subclass(relationId: Oid) -> bool {
    let tuple: HeapTuple;
    let result: bool;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relationId);
    }

    result = (*(GETSTRUCT(tuple) as Form_pg_class)).relhassubclass;
    ReleaseSysCache(tuple);
    result
}

/*
 * has_superclass - does this relation inherit from another?
 *
 * Unlike has_subclass, this can be relied on to give an accurate answer.
 * However, the caller must hold a lock on the given relation so that it
 * can't be concurrently added to or removed from an inheritance hierarchy.
 */
pub unsafe fn has_superclass(relationId: Oid) -> bool {
    let catalog: Relation;
    let scan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let result: bool;

    catalog = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut skey,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relationId),
    );
    scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true, null_mut(), 1, &mut skey);
    result = HeapTupleIsValid(systable_getnext(scan));
    systable_endscan(scan);
    table_close(catalog, AccessShareLock);

    result
}

/*
 * Given two type OIDs, determine whether the first is a complex type
 * (class type) that inherits from the second.
 *
 * This essentially asks whether the first type is guaranteed to be coercible
 * to the second.  Therefore, we allow the first type to be a domain over a
 * complex type that inherits from the second; that creates no difficulties.
 * But the second type cannot be a domain.
 */
pub unsafe fn typeInheritsFrom(subclassTypeId: Oid, superclassTypeId: Oid) -> bool {
    let mut result: bool = false;
    let subclassRelid: Oid;
    let superclassRelid: Oid;
    let inhrel: Relation;
    let mut visited: *mut List;
    let mut queue: *mut List;
    let queue_item: *mut ListCell;

    /* We need to work with the associated relation OIDs */
    subclassRelid = typeOrDomainTypeRelid(subclassTypeId);
    if subclassRelid == InvalidOid {
        return false; /* not a complex type or domain over one */
    }
    superclassRelid = typeidTypeRelid(superclassTypeId);
    if superclassRelid == InvalidOid {
        return false; /* not a complex type */
    }

    /* No point in searching if the superclass has no subclasses */
    if !has_subclass(superclassRelid) {
        return false;
    }

    /*
     * Begin the search at the relation itself, so add its relid to the queue.
     */
    queue = list_make1_oid(subclassRelid);
    visited = null_mut(); /* NIL */

    inhrel = table_open(InheritsRelationId, AccessShareLock);

    /*
     * Use queue to do a breadth-first traversal of the inheritance graph from
     * the relid supplied up to the root.  Notice that we append to the queue
     * inside the loop --- this is okay because the foreach() macro doesn't
     * advance queue_item until the next loop iteration begins.
     */
    foreach!(queue_item, queue, {
        let this_relid: Oid = lfirst_oid(current_cell!(queue_item));
        let mut skey: ScanKeyData = core::mem::zeroed();
        let inhscan: SysScanDesc;
        let mut inhtup: HeapTuple;

        /*
         * If we've seen this relid already, skip it.  This avoids extra work
         * in multiple-inheritance scenarios, and also protects us from an
         * infinite loop in case there is a cycle in pg_inherits (though
         * theoretically that shouldn't happen).
         */
        if list_member_oid(visited, this_relid) {
            continue;
        }

        /*
         * Okay, this is a not-yet-seen relid. Add it to the list of
         * already-visited OIDs, then find all the types this relid inherits
         * from and add them to the queue.
         */
        visited = lappend_oid(visited, this_relid);

        ScanKeyInit(
            &mut skey,
            Anum_pg_inherits_inhrelid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(this_relid),
        );

        inhscan = systable_beginscan(inhrel, InheritsRelidSeqnoIndexId, true, null_mut(), 1, &mut skey);

        loop {
            inhtup = systable_getnext(inhscan);
            if inhtup.is_null() {
                break;
            }
            let inh = GETSTRUCT(inhtup) as Form_pg_inherits;
            let inhparent: Oid = (*inh).inhparent;

            /* If this is the target superclass, we're done */
            if inhparent == superclassRelid {
                result = true;
                break;
            }

            /* Else add to queue */
            queue = lappend_oid(queue, inhparent);
        }

        systable_endscan(inhscan);

        if result {
            break;
        }
    });

    /* clean up ... */
    table_close(inhrel, AccessShareLock);

    list_free(visited);
    list_free(queue);

    result
}

/*
 * Create a single pg_inherits row with the given data
 */
pub unsafe fn StoreSingleInheritance(relationId: Oid, parentOid: Oid, seqNumber: int32) {
    let mut values: [Datum; Natts_pg_inherits] = [0; Natts_pg_inherits];
    let mut nulls: [bool; Natts_pg_inherits] = [false; Natts_pg_inherits];
    let tuple: HeapTuple;
    let inhRelation: Relation;

    inhRelation = table_open(InheritsRelationId, RowExclusiveLock);

    /*
     * Make the pg_inherits entry
     */
    values[(Anum_pg_inherits_inhrelid - 1) as usize] = ObjectIdGetDatum(relationId);
    values[(Anum_pg_inherits_inhparent - 1) as usize] = ObjectIdGetDatum(parentOid);
    values[(Anum_pg_inherits_inhseqno - 1) as usize] = Int32GetDatum(seqNumber);
    values[(Anum_pg_inherits_inhdetachpending - 1) as usize] = BoolGetDatum(false);

    nulls = [false; Natts_pg_inherits];

    tuple = heap_form_tuple(RelationGetDescr(inhRelation), values.as_ptr(), nulls.as_ptr());

    CatalogTupleInsert(inhRelation, tuple);

    heap_freetuple(tuple);

    table_close(inhRelation, RowExclusiveLock);
}

/*
 * DeleteInheritsTuple
 *
 * Delete pg_inherits tuples with the given inhrelid.  inhparent may be given
 * as InvalidOid, in which case all tuples matching inhrelid are deleted;
 * otherwise only delete tuples with the specified inhparent.
 *
 * expect_detach_pending is the expected state of the inhdetachpending flag.
 * If the catalog row does not match that state, an error is raised.
 *
 * childname is the partition name, if a table; pass NULL for regular
 * inheritance or when working with other relation kinds.
 *
 * Returns whether at least one row was deleted.
 */
pub unsafe fn DeleteInheritsTuple(
    inhrelid: Oid,
    inhparent: Oid,
    expect_detach_pending: bool,
    childname: *const c_char,
) -> bool {
    let mut found: bool = false;
    let catalogRelation: Relation;
    let mut key: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut inheritsTuple: HeapTuple;

    /*
     * Find pg_inherits entries by inhrelid.
     */
    catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(inhrelid),
    );
    scan = systable_beginscan(catalogRelation, InheritsRelidSeqnoIndexId, true, null_mut(), 1, &mut key);

    loop {
        inheritsTuple = systable_getnext(scan);
        if !HeapTupleIsValid(inheritsTuple) {
            break;
        }
        let parent: Oid;

        /* Compare inhparent if it was given, and do the actual deletion. */
        parent = (*(GETSTRUCT(inheritsTuple) as Form_pg_inherits)).inhparent;
        if !OidIsValid(inhparent) || parent == inhparent {
            let detach_pending: bool;

            detach_pending = (*(GETSTRUCT(inheritsTuple) as Form_pg_inherits)).inhdetachpending;

            /*
             * Raise error depending on state.  This should only happen for
             * partitions, but we have no way to cross-check.
             */
            if detach_pending && !expect_detach_pending {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot detach partition \"{}\"",
                        if !childname.is_null() {
                            std::ffi::CStr::from_ptr(childname).to_string_lossy()
                        } else {
                            std::borrow::Cow::Borrowed("unknown relation")
                        }
                    )
                );
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
                /* C also: errdetail("The partition is being detached concurrently or has an unfinished detach.") */
                /* C also: errhint("Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation.") */
            }
            if !detach_pending && expect_detach_pending {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot complete detaching partition \"{}\"",
                        if !childname.is_null() {
                            std::ffi::CStr::from_ptr(childname).to_string_lossy()
                        } else {
                            std::borrow::Cow::Borrowed("unknown relation")
                        }
                    )
                );
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
                /* C also: errdetail("There's no pending concurrent detach.") */
            }

            CatalogTupleDelete(catalogRelation, &mut (*inheritsTuple).t_self);
            found = true;
        }
    }

    /* Done */
    systable_endscan(scan);
    table_close(catalogRelation, RowExclusiveLock);

    found
}

/*
 * Return whether the pg_inherits tuple for a partition has the "detach
 * pending" flag set.
 */
pub unsafe fn PartitionHasPendingDetach(partoid: Oid) -> bool {
    let catalogRelation: Relation;
    let mut key: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut inheritsTuple: HeapTuple;

    /* We don't have a good way to verify it is in fact a partition */

    /*
     * Find the pg_inherits entry by inhrelid.  (There should only be one.)
     */
    catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(partoid),
    );
    scan = systable_beginscan(catalogRelation, InheritsRelidSeqnoIndexId, true, null_mut(), 1, &mut key);

    loop {
        inheritsTuple = systable_getnext(scan);
        if !HeapTupleIsValid(inheritsTuple) {
            break;
        }
        let detached: bool;

        detached = (*(GETSTRUCT(inheritsTuple) as Form_pg_inherits)).inhdetachpending;

        /* Done */
        systable_endscan(scan);
        table_close(catalogRelation, RowExclusiveLock);

        return detached;
    }

    elog!(ERROR, "relation {} is not a partition", partoid);
    #[allow(unreachable_code)]
    false /* keep compiler quiet */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_inherits {
    /// OID of the child relation.
    pub inhrelid: Oid,
    /// OID of the parent relation.
    pub inhparent: Oid,
    /// 1-based position of this parent among the child's parents.
    pub inhseqno: int32,
    /// True while a concurrent partition detach is in progress.
    pub inhdetachpending: bool,
}

pub type Form_pg_inherits = *mut FormData_pg_inherits;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(core::mem::offset_of!(FormData_pg_inherits, inhparent), 4);
        assert_eq!(core::mem::offset_of!(FormData_pg_inherits, inhseqno), 8);
        assert!(
            core::mem::size_of::<FormData_pg_inherits>()
                >= core::mem::offset_of!(FormData_pg_inherits, inhdetachpending) + 1
        );
    }
}
