//! Translation of postgres/src/include/catalog/pg_subscription.h
//!
//! The `FormData_pg_subscription` struct: the fixed-layout part of a
//! pg_subscription catalog row.  As in the C header, the struct as compiled
//! into the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length fields (subconninfo, subslotname, subsynccommit,
//! subpublications[], suborigin, guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_subscription tuple and
//! are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{uint64, NameData};
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Datum, DatumGetName, ObjectIdGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::OidIsValid;
use crate::nodes::pg_list::{lappend, list_free_deep, List, ListCell, NIL};
use crate::nodes::value::makeString;
use crate::utils::adt::arrayfuncs::deconstruct_array_builtin;
use crate::utils::array::ArrayType;
use crate::utils::adt::pg_lsn::{DatumGetLSN, LSNGetDatum};
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::palloc::{palloc, pfree, pstrdup};
use crate::lib::stringinfo::StringInfo;
use crate::catalog::pg_subscription_rel::{Form_pg_subscription_rel, SUBREL_STATE_READY, SUBREL_STATE_UNKNOWN};
use crate::{castNode, elog, ereport, errmsg, foreach, current_cell, strVal, Assert};
use crate::nodes::pg_list::lfirst;
use crate::utils::elog::ERROR;
use core::ffi::{c_char, c_int, c_void};
use core::ptr;

/* access/htup_details.h NameStr - the C macro that turns a NameData field into a
 * char pointer. */
macro_rules! NameStr {
    ($n:expr) => {
        $n.data.as_ptr() as *const c_char
    };
}

/* InvalidXLogRecPtr - access/transam/xlogdefs.h. */
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

/* TEXTOID - OID of the text type (see catalog/pg_type.dat). */
const TEXTOID: Oid = 25;

/* XLogRecPtr is a C typedef for uint64 (a byte position in the WAL). */
pub type XLogRecPtr = uint64;

/*
 * FormData_pg_subscription - the fixed part of a pg_subscription row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_subscription {
    /* oid */
    pub oid: Oid,
    /* Database the subscription is in. */
    pub subdbid: Oid,
    /* All changes finished at this LSN are skipped */
    pub subskiplsn: XLogRecPtr,
    /* Name of the subscription */
    pub subname: NameData,
    /* Owner of the subscription */
    pub subowner: Oid,
    /* True if the subscription is enabled (the worker should be running) */
    pub subenabled: bool,
    /* True if the subscription wants the publisher to send data in binary */
    pub subbinary: bool,
    /* Stream in-progress transactions. See LOGICALREP_STREAM_xxx constants. */
    pub substream: c_char,
    /* Stream two-phase transactions */
    pub subtwophasestate: c_char,
    /* True if a worker error should cause the subscription to be disabled */
    pub subdisableonerr: bool,
    /* Must connection use a password? */
    pub subpasswordrequired: bool,
    /* True if replication should execute as the subscription owner */
    pub subrunasowner: bool,
    /* True if the associated replication slots in the upstream database are
     * enabled to be synchronized to the standbys. */
    pub subfailover: bool,
}

/*
 * Form_pg_subscription corresponds to a pointer to a row with the format of the
 * pg_subscription relation.
 */
pub type Form_pg_subscription = *mut FormData_pg_subscription;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/*
 * two_phase tri-state values. See comments atop worker.c to know more about
 * these states.
 */
pub const LOGICALREP_TWOPHASE_STATE_DISABLED: c_char = b'd' as c_char;
pub const LOGICALREP_TWOPHASE_STATE_PENDING: c_char = b'p' as c_char;
pub const LOGICALREP_TWOPHASE_STATE_ENABLED: c_char = b'e' as c_char;

/*
 * The subscription will request the publisher to only send changes that do not
 * have any origin.
 */
pub const LOGICALREP_ORIGIN_NONE: &str = "none";

/*
 * The subscription will request the publisher to send changes regardless
 * of their origin.
 */
pub const LOGICALREP_ORIGIN_ANY: &str = "any";

/* Disallow streaming in-progress transactions. */
pub const LOGICALREP_STREAM_OFF: c_char = b'f' as c_char;

/*
 * Streaming in-progress transactions are written to a temporary file and
 * applied only after the transaction is committed on upstream.
 */
pub const LOGICALREP_STREAM_ON: c_char = b't' as c_char;

/*
 * Streaming in-progress transactions are applied immediately via a parallel
 * apply worker.
 */
pub const LOGICALREP_STREAM_PARALLEL: c_char = b'p' as c_char;

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list of strings is pallocated here.
 */
unsafe fn textarray_to_stringlist(textarray: *mut ArrayType) -> *mut List {
    let mut elems: *mut Datum = ptr::null_mut();
    let mut nelems: c_int = 0;
    let mut res: *mut List = NIL;

    deconstruct_array_builtin(textarray, TEXTOID, &mut elems, ptr::null_mut(), &mut nelems);

    if nelems == 0 {
        return NIL;
    }

    let mut i: c_int = 0;
    while i < nelems {
        res = lappend(
            res,
            makeString(TextDatumGetCString(*elems.add(i as usize))) as *mut c_void,
        );
        i += 1;
    }

    res
}

/*
 * Subscription - in-memory representation of a pg_subscription row, as built by
 * GetSubscription().  This mirrors the struct in catalog/pg_subscription.h.
 */
#[repr(C)]
pub struct Subscription {
    pub oid: Oid,            /* Oid of the subscription */
    pub dbid: Oid,           /* Oid of the database which subscription is in */
    pub skiplsn: XLogRecPtr, /* All changes finished at this LSN are skipped */
    pub name: *mut c_char,   /* Name of the subscription */
    pub owner: Oid,          /* Oid of the subscription owner */
    pub enabled: bool,       /* Indicates if the subscription is enabled */
    pub binary: bool,        /* Indicates if the subscription wants data in
                              * binary format */
    pub stream: c_char,      /* Allow streaming in-progress transactions */
    pub twophasestate: c_char, /* Allow streaming two-phase transactions */
    pub disableonerr: bool,  /* Indicates if the subscription should be
                              * automatically disabled if a worker error
                              * occurs */
    pub passwordrequired: bool, /* Must connection use a password? */
    pub runasowner: bool,    /* Run replication as subscription owner */
    pub failover: bool,      /* True if the associated replication slots
                              * (i.e. the main slot and the table sync
                              * slots) in the upstream database are enabled
                              * to be synchronized to the standbys. */
    pub conninfo: *mut c_char, /* Connection string to the publisher */
    pub slotname: *mut c_char, /* Name of the replication slot */
    pub synccommit: *mut c_char, /* Synchronous commit setting for worker */
    pub publications: *mut List, /* List of publication names to subscribe to */
    pub origin: *mut c_char, /* Only publish data originating from the
                              * specified origin */
    pub ownersuperuser: bool, /* Is the subscription owner a superuser? */
}

/*
 * SubscriptionRelState - the state of one table in a subscription, as returned
 * by GetSubscriptionRelations().
 */
#[repr(C)]
pub struct SubscriptionRelState {
    pub relid: Oid,
    pub state: c_char,
    pub lsn: XLogRecPtr,
}

/* ----------------------------------------------------------------
 * Catalog OIDs, attribute numbers, lock modes, and scan-strategy
 * constants used below.  TODO(pg-port): pull these from the real
 * catalog/pg_class.h, catalog/pg_subscription{,_rel}.h, storage/lockdefs.h
 * and access/stratnum.h headers once those are ported.
 * ----------------------------------------------------------------
 */
const SubscriptionRelationId: Oid = 6100;
const SubscriptionRelRelationId: Oid = 6102;

const Natts_pg_subscription: usize = 22;
const Anum_pg_subscription_subdbid: c_int = 2;
const Anum_pg_subscription_subenabled: usize = 6;
const Anum_pg_subscription_subconninfo: c_int = 13;
const Anum_pg_subscription_subslotname: c_int = 14;
const Anum_pg_subscription_subsynccommit: c_int = 15;
const Anum_pg_subscription_subpublications: c_int = 16;
const Anum_pg_subscription_suborigin: c_int = 17;

const Natts_pg_subscription_rel: usize = 4;
const Anum_pg_subscription_rel_srsubid: c_int = 1;
const Anum_pg_subscription_rel_srrelid: c_int = 2;
const Anum_pg_subscription_rel_srsubstate: usize = 3;
const Anum_pg_subscription_rel_srsublsn: c_int = 4;

/* syscache ids - utils/syscache.h */
const SUBSCRIPTIONOID: c_int = 0;
const SUBSCRIPTIONRELMAP: c_int = 0;

/* storage/lockdefs.h */
const NoLock: c_int = 0;
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 5;

/* access/stratnum.h */
const BTEqualStrategyNumber: u16 = 3;

/* utils/fmgroids.h */
const F_OIDEQ: Oid = 184;
const F_CHARNE: Oid = 1245;

/* access/sdir.h */
const ForwardScanDirection: c_int = 1;

/*
 * Add a comma-separated list of publication names to the 'dest' string.
 */
pub unsafe fn GetPublicationsStr(
    publications: *mut List,
    dest: StringInfo,
    quote_literal: bool,
) {
    let mut first = true;

    Assert!(publications != NIL);

    foreach!(lc, publications, {
        let pubname: *mut c_char = strVal!(lfirst(current_cell!(lc)));

        if first {
            first = false;
        } else {
            appendStringInfoString(dest, c", ".as_ptr());
        }

        if quote_literal {
            appendStringInfoString(dest, quote_literal_cstr(pubname));
        } else {
            appendStringInfoChar(dest, b'"' as c_char);
            appendStringInfoString(dest, pubname);
            appendStringInfoChar(dest, b'"' as c_char);
        }
    });
}

/*
 * Fetch the subscription from the syscache.
 */
pub unsafe fn GetSubscription(subid: Oid, missing_ok: bool) -> *mut Subscription {
    let tup: HeapTuple;
    let sub: *mut Subscription;
    let subform: Form_pg_subscription;
    let mut datum: Datum;
    let mut isnull: bool = false;

    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if !HeapTupleIsValid(tup) {
        if missing_ok {
            return ptr::null_mut();
        }

        elog!(ERROR, "cache lookup failed for subscription {}", subid);
    }

    subform = GETSTRUCT(tup) as Form_pg_subscription;

    sub = palloc(core::mem::size_of::<Subscription>()) as *mut Subscription;
    (*sub).oid = subid;
    (*sub).dbid = (*subform).subdbid;
    (*sub).skiplsn = (*subform).subskiplsn;
    (*sub).name = pstrdup(NameStr!((*subform).subname));
    (*sub).owner = (*subform).subowner;
    (*sub).enabled = (*subform).subenabled;
    (*sub).binary = (*subform).subbinary;
    (*sub).stream = (*subform).substream;
    (*sub).twophasestate = (*subform).subtwophasestate;
    (*sub).disableonerr = (*subform).subdisableonerr;
    (*sub).passwordrequired = (*subform).subpasswordrequired;
    (*sub).runasowner = (*subform).subrunasowner;
    (*sub).failover = (*subform).subfailover;

    /* Get conninfo */
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subconninfo);
    (*sub).conninfo = TextDatumGetCString(datum);

    /* Get slotname */
    datum = SysCacheGetAttr(
        SUBSCRIPTIONOID,
        tup,
        Anum_pg_subscription_subslotname,
        &mut isnull,
    );
    if !isnull {
        (*sub).slotname = pstrdup(NameStr!(*DatumGetName(datum)));
    } else {
        (*sub).slotname = ptr::null_mut();
    }

    /* Get synccommit */
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subsynccommit);
    (*sub).synccommit = TextDatumGetCString(datum);

    /* Get publications */
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subpublications);
    (*sub).publications = textarray_to_stringlist(DatumGetArrayTypeP(datum));

    /* Get origin */
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_suborigin);
    (*sub).origin = TextDatumGetCString(datum);

    /* Is the subscription owner a superuser? */
    (*sub).ownersuperuser = superuser_arg((*sub).owner);

    ReleaseSysCache(tup);

    sub
}

/*
 * Return number of subscriptions defined in given database.
 * Used by dropdb() to check if database can indeed be dropped.
 */
pub unsafe fn CountDBSubscriptions(dbid: Oid) -> c_int {
    let mut nsubs: c_int = 0;
    let rel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut scankey,
        Anum_pg_subscription_subdbid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(dbid),
    );

    scan = systable_beginscan(rel, InvalidOid, false, ptr::null_mut(), 1, &mut scankey);

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        nsubs += 1;
    }

    systable_endscan(scan);

    table_close(rel, NoLock);

    nsubs
}

/*
 * Free memory allocated by subscription struct.
 */
pub unsafe fn FreeSubscription(sub: *mut Subscription) {
    pfree((*sub).name as *mut c_void);
    pfree((*sub).conninfo as *mut c_void);
    if !(*sub).slotname.is_null() {
        pfree((*sub).slotname as *mut c_void);
    }
    list_free_deep((*sub).publications);
    pfree(sub as *mut c_void);
}

/*
 * Disable the given subscription.
 */
pub unsafe fn DisableSubscription(subid: Oid) {
    let rel: Relation;
    let mut nulls: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];
    let mut replaces: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];
    let mut values: [Datum; Natts_pg_subscription] = [0 as Datum; Natts_pg_subscription];
    let mut tup: HeapTuple;

    /* Look up the subscription in the catalog */
    rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for subscription {}", subid);
    }

    LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

    /* Form a new tuple. */
    nulls = [false; Natts_pg_subscription];
    replaces = [false; Natts_pg_subscription];

    /* Set the subscription to disabled. */
    values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(false);
    replaces[Anum_pg_subscription_subenabled - 1] = true;

    /* Update the catalog */
    tup = heap_modify_tuple(
        tup,
        RelationGetDescr(rel),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
        replaces.as_mut_ptr(),
    );
    CatalogTupleUpdate(rel, &mut (*tup).t_self, tup);
    heap_freetuple(tup);

    table_close(rel, NoLock);
}

/*
 * Add new state record for a subscription table.
 *
 * If retain_lock is true, then don't release the locks taken in this function.
 * We normally release the locks at the end of transaction but in binary-upgrade
 * mode, we expect to release those immediately.
 */
pub unsafe fn AddSubscriptionRelState(
    subid: Oid,
    relid: Oid,
    state: c_char,
    sublsn: XLogRecPtr,
    retain_lock: bool,
) {
    let rel: Relation;
    let mut tup: HeapTuple;
    let mut nulls: [bool; Natts_pg_subscription_rel] = [false; Natts_pg_subscription_rel];
    let mut values: [Datum; Natts_pg_subscription_rel] = [0 as Datum; Natts_pg_subscription_rel];

    LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

    rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);

    /* Try finding existing mapping. */
    tup = SearchSysCacheCopy2(
        SUBSCRIPTIONRELMAP,
        ObjectIdGetDatum(relid),
        ObjectIdGetDatum(subid),
    );
    if HeapTupleIsValid(tup) {
        elog!(
            ERROR,
            "subscription table {} in subscription {} already exists",
            relid,
            subid
        );
    }

    /* Form the tuple. */
    nulls = [false; Natts_pg_subscription_rel];
    values[Anum_pg_subscription_rel_srsubid as usize - 1] = ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_rel_srrelid as usize - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);
    if sublsn != InvalidXLogRecPtr {
        values[Anum_pg_subscription_rel_srsublsn as usize - 1] = LSNGetDatum(sublsn);
    } else {
        nulls[Anum_pg_subscription_rel_srsublsn as usize - 1] = true;
    }

    tup = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    /* Insert tuple into catalog. */
    CatalogTupleInsert(rel, tup);

    heap_freetuple(tup);

    /* Cleanup. */
    if retain_lock {
        table_close(rel, NoLock);
    } else {
        table_close(rel, RowExclusiveLock);
        UnlockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
    }
}

/*
 * Update the state of a subscription table.
 */
pub unsafe fn UpdateSubscriptionRelState(
    subid: Oid,
    relid: Oid,
    state: c_char,
    sublsn: XLogRecPtr,
    already_locked: bool,
) {
    let rel: Relation;
    let mut tup: HeapTuple;
    let mut nulls: [bool; Natts_pg_subscription_rel] = [false; Natts_pg_subscription_rel];
    let mut values: [Datum; Natts_pg_subscription_rel] = [0 as Datum; Natts_pg_subscription_rel];
    let mut replaces: [bool; Natts_pg_subscription_rel] = [false; Natts_pg_subscription_rel];

    if already_locked {
        /*
         * USE_ASSERT_CHECKING: the C code asserts that the caller already holds
         * RowExclusiveLock on pg_subscription_rel and AccessShareLock on the
         * subscription object.  TODO(pg-port): port CheckRelationOidLockedByMe /
         * LockHeldByMe assertions.
         */
        rel = table_open(SubscriptionRelRelationId, NoLock);
    } else {
        LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
        rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);
    }

    /* Try finding existing mapping. */
    tup = SearchSysCacheCopy2(
        SUBSCRIPTIONRELMAP,
        ObjectIdGetDatum(relid),
        ObjectIdGetDatum(subid),
    );
    if !HeapTupleIsValid(tup) {
        elog!(
            ERROR,
            "subscription table {} in subscription {} does not exist",
            relid,
            subid
        );
    }

    /* Update the tuple. */
    nulls = [false; Natts_pg_subscription_rel];
    replaces = [false; Natts_pg_subscription_rel];

    replaces[Anum_pg_subscription_rel_srsubstate - 1] = true;
    values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);

    replaces[Anum_pg_subscription_rel_srsublsn as usize - 1] = true;
    if sublsn != InvalidXLogRecPtr {
        values[Anum_pg_subscription_rel_srsublsn as usize - 1] = LSNGetDatum(sublsn);
    } else {
        nulls[Anum_pg_subscription_rel_srsublsn as usize - 1] = true;
    }

    tup = heap_modify_tuple(
        tup,
        RelationGetDescr(rel),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
        replaces.as_mut_ptr(),
    );

    /* Update the catalog. */
    CatalogTupleUpdate(rel, &mut (*tup).t_self, tup);

    /* Cleanup. */
    table_close(rel, NoLock);
}

/*
 * Get state of subscription table.
 *
 * Returns SUBREL_STATE_UNKNOWN when the table is not in the subscription.
 */
pub unsafe fn GetSubscriptionRelState(
    subid: Oid,
    relid: Oid,
    sublsn: *mut XLogRecPtr,
) -> c_char {
    let tup: HeapTuple;
    let substate: c_char;
    let mut isnull: bool = false;
    let d: Datum;
    let rel: Relation;

    /*
     * This is to avoid the race condition with AlterSubscription which tries
     * to remove this relstate.
     */
    rel = table_open(SubscriptionRelRelationId, AccessShareLock);

    /* Try finding the mapping. */
    tup = SearchSysCache2(
        SUBSCRIPTIONRELMAP,
        ObjectIdGetDatum(relid),
        ObjectIdGetDatum(subid),
    );

    if !HeapTupleIsValid(tup) {
        table_close(rel, AccessShareLock);
        *sublsn = InvalidXLogRecPtr;
        return SUBREL_STATE_UNKNOWN;
    }

    /* Get the state. */
    substate = (*(GETSTRUCT(tup) as Form_pg_subscription_rel)).srsubstate;

    /* Get the LSN */
    d = SysCacheGetAttr(
        SUBSCRIPTIONRELMAP,
        tup,
        Anum_pg_subscription_rel_srsublsn,
        &mut isnull,
    );
    if isnull {
        *sublsn = InvalidXLogRecPtr;
    } else {
        *sublsn = DatumGetLSN(d);
    }

    /* Cleanup */
    ReleaseSysCache(tup);

    table_close(rel, AccessShareLock);

    substate
}

/*
 * Drop subscription relation mapping. These can be for a particular
 * subscription, or for a particular relation, or both.
 */
pub unsafe fn RemoveSubscriptionRel(subid: Oid, relid: Oid) {
    let rel: Relation;
    let scan: TableScanDesc;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;
    let mut nkeys: usize = 0;

    rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);

    if OidIsValid(subid) {
        ScanKeyInit(
            &mut skey[nkeys],
            Anum_pg_subscription_rel_srsubid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(subid),
        );
        nkeys += 1;
    }

    if OidIsValid(relid) {
        ScanKeyInit(
            &mut skey[nkeys],
            Anum_pg_subscription_rel_srrelid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(relid),
        );
        nkeys += 1;
    }

    /* Do the search and delete what we found. */
    scan = table_beginscan_catalog(rel, nkeys as c_int, skey.as_mut_ptr());
    loop {
        tup = heap_getnext(scan, ForwardScanDirection);
        if !HeapTupleIsValid(tup) {
            break;
        }

        let subrel: Form_pg_subscription_rel = GETSTRUCT(tup) as Form_pg_subscription_rel;

        /*
         * We don't allow to drop the relation mapping when the table
         * synchronization is in progress unless the caller updates the
         * corresponding subscription as well. This is to ensure that we don't
         * leave tablesync slots or origins in the system when the
         * corresponding table is dropped.
         */
        if !OidIsValid(subid) && (*subrel).srsubstate != SUBREL_STATE_READY {
            ereport!(
                ERROR,
                errmsg!(
                    "could not drop relation mapping for subscription \"{}\"",
                    std::ffi::CStr::from_ptr(get_subscription_name((*subrel).srsubid, false))
                        .to_string_lossy()
                )
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                /* C also: errdetail("Table synchronization for relation \"%s\" is in progress and is in state \"%c\".", get_rel_name(relid), subrel->srsubstate) */
                /* C also: errhint("Use %s to enable subscription if not already enabled or use %s to drop the subscription.", "ALTER SUBSCRIPTION ... ENABLE", "DROP SUBSCRIPTION ...") */
            );
        }

        CatalogTupleDelete(rel, &mut (*tup).t_self);
    }
    table_endscan(scan);

    table_close(rel, RowExclusiveLock);
}

/*
 * Does the subscription have any relations?
 *
 * Use this function only to know true/false, and when you have no need for the
 * List returned by GetSubscriptionRelations.
 */
pub unsafe fn HasSubscriptionRelations(subid: Oid) -> bool {
    let rel: Relation;
    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
    let scan: SysScanDesc;
    let has_subrels: bool;

    rel = table_open(SubscriptionRelRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_subscription_rel_srsubid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(subid),
    );

    scan = systable_beginscan(rel, InvalidOid, false, ptr::null_mut(), 1, skey.as_mut_ptr());

    /* If even a single tuple exists then the subscription has tables. */
    has_subrels = HeapTupleIsValid(systable_getnext(scan));

    /* Cleanup */
    systable_endscan(scan);
    table_close(rel, AccessShareLock);

    has_subrels
}

/*
 * Get the relations for the subscription.
 *
 * If not_ready is true, return only the relations that are not in a ready
 * state, otherwise return all the relations of the subscription.  The
 * returned list is palloc'ed in the current memory context.
 */
pub unsafe fn GetSubscriptionRelations(subid: Oid, not_ready: bool) -> *mut List {
    let mut res: *mut List = NIL;
    let rel: Relation;
    let mut tup: HeapTuple;
    let mut nkeys: usize = 0;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;

    rel = table_open(SubscriptionRelRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[nkeys],
        Anum_pg_subscription_rel_srsubid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(subid),
    );
    nkeys += 1;

    if not_ready {
        ScanKeyInit(
            &mut skey[nkeys],
            Anum_pg_subscription_rel_srsubstate as c_int,
            BTEqualStrategyNumber,
            F_CHARNE,
            CharGetDatum(SUBREL_STATE_READY),
        );
        nkeys += 1;
    }

    scan = systable_beginscan(rel, InvalidOid, false, ptr::null_mut(), nkeys as c_int, skey.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }

        let subrel: Form_pg_subscription_rel = GETSTRUCT(tup) as Form_pg_subscription_rel;
        let relstate: *mut SubscriptionRelState;
        let d: Datum;
        let mut isnull: bool = false;

        relstate = palloc(core::mem::size_of::<SubscriptionRelState>()) as *mut SubscriptionRelState;
        (*relstate).relid = (*subrel).srrelid;
        (*relstate).state = (*subrel).srsubstate;
        d = SysCacheGetAttr(
            SUBSCRIPTIONRELMAP,
            tup,
            Anum_pg_subscription_rel_srsublsn,
            &mut isnull,
        );
        if isnull {
            (*relstate).lsn = InvalidXLogRecPtr;
        } else {
            (*relstate).lsn = DatumGetLSN(d);
        }

        res = lappend(res, relstate as *mut c_void);
    }

    /* Cleanup */
    systable_endscan(scan);
    table_close(rel, AccessShareLock);

    res
}

/* ----------------------------------------------------------------
 * TODO(pg-port): local stubs for catalog-access helpers that are not yet
 * ported anywhere in the crate.  Replace with imports once their real homes
 * land (access/genam.h, access/heapam.h, access/tableam.h, catalog/indexing.h,
 * utils/syscache.h, utils/rel.h, utils/lsyscache.h, storage/lmgr.h).
 * ----------------------------------------------------------------
 */

use crate::access::htup_details::HeapTuple;
use crate::storage::itemptr::ItemPointer;

/* utils/rel.h Relation, access/relscan.h scan descriptors, access/skey.h */
type Relation = *mut c_void;
type SysScanDesc = *mut c_void;
type TableScanDesc = *mut c_void;
type TupleDesc = *mut c_void;

#[repr(C)]
#[derive(Clone, Copy)]
struct ScanKeyData {
    sk_attno: c_int,
    sk_strategy: u16,
    sk_func: Oid,
    sk_argument: Datum,
}

unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO(pg-port): access/table/table.c table_open
}

unsafe fn table_close(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): access/table/table.c table_close
}

unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc {
    unimplemented!() // TODO(pg-port): utils/rel.h RelationGetDescr
}

unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attributeNumber: c_int,
    _strategy: u16,
    _procedure: Oid,
    _argument: Datum,
) {
    unimplemented!() // TODO(pg-port): access/common/scankey.c ScanKeyInit
}

unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!() // TODO(pg-port): access/index/genam.c systable_beginscan
}

unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/index/genam.c systable_getnext
}

unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!() // TODO(pg-port): access/index/genam.c systable_endscan
}

unsafe fn table_beginscan_catalog(
    _relation: Relation,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> TableScanDesc {
    unimplemented!() // TODO(pg-port): access/table/tableam.c table_beginscan_catalog
}

unsafe fn heap_getnext(_scan: TableScanDesc, _direction: c_int) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/heap/heapam.c heap_getnext
}

unsafe fn table_endscan(_scan: TableScanDesc) {
    unimplemented!() // TODO(pg-port): access/table/tableam.c table_endscan
}

unsafe fn HeapTupleIsValid(_tuple: HeapTuple) -> bool {
    unimplemented!() // TODO(pg-port): access/htup.h HeapTupleIsValid
}

unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/htup_details.h GETSTRUCT
}

unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c heap_form_tuple
}

unsafe fn heap_modify_tuple(
    _tuple: HeapTuple,
    _tupleDesc: TupleDesc,
    _replValues: *mut Datum,
    _replIsnull: *mut bool,
    _doReplace: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c heap_modify_tuple
}

unsafe fn heap_freetuple(_htup: HeapTuple) {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c heap_freetuple
}

unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {
    unimplemented!() // TODO(pg-port): catalog/indexing.c CatalogTupleInsert
}

unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: ItemPointer, _tup: HeapTuple) {
    unimplemented!() // TODO(pg-port): catalog/indexing.c CatalogTupleUpdate
}

unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: ItemPointer) {
    unimplemented!() // TODO(pg-port): catalog/indexing.c CatalogTupleDelete
}

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c SearchSysCache1
}

unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c SearchSysCache2
}

unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.h SearchSysCacheCopy1
}

unsafe fn SearchSysCacheCopy2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.h SearchSysCacheCopy2
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c ReleaseSysCache
}

unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c SysCacheGetAttr
}

unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int) -> Datum {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c SysCacheGetAttrNotNull
}

unsafe fn LockSharedObject(_classId: Oid, _objectId: Oid, _objsubid: u16, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c LockSharedObject
}

unsafe fn UnlockSharedObject(_classId: Oid, _objectId: Oid, _objsubid: u16, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c UnlockSharedObject
}

unsafe fn superuser_arg(_roleid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/misc/superuser.c superuser_arg
}

unsafe fn get_subscription_name(_subid: Oid, _missing_ok: bool) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c get_subscription_name
}

unsafe fn quote_literal_cstr(_rawstr: *mut c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/adt/quote.c quote_literal_cstr
}

unsafe fn appendStringInfoString(_str: StringInfo, _s: *const c_char) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.c appendStringInfoString
}

unsafe fn appendStringInfoChar(_str: StringInfo, _ch: c_char) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.c appendStringInfoChar
}

unsafe fn DatumGetArrayTypeP(_x: Datum) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): utils/array.h DatumGetArrayTypeP
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // subdbid sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_subscription, subdbid), 4);
        // The struct must at least span through its last fixed field
        // (subfailover, a bool).
        assert!(
            core::mem::size_of::<FormData_pg_subscription>()
                >= core::mem::offset_of!(FormData_pg_subscription, subfailover)
                    + core::mem::size_of::<bool>()
        );
    }
}
