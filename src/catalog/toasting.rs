//! catalog/toasting.c - routines to support creation of toast tables.

use crate::prelude::*;
use crate::pg_config_manual::NAMEDATALEN;
use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;

use crate::{list_make2, makeNode};

use crate::access::common::heaptuple::heap_freetuple;
use crate::access::common::scankey::ScanKeyInit;
use crate::access::common::toast_internals::InvalidCompressionMethod;
use crate::access::common::tupdesc::{
    CreateTemplateTupleDesc, TupleDescAttr, TupleDescInitEntry, TYPSTORAGE_PLAIN,
};
use crate::access::htup_details::{GETSTRUCT, HeapTuple, HeapTupleData, HeapTupleIsValid};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open, table_openrv};
use crate::catalog::binary_upgrade::binary_upgrade_next_toast_pg_class_oid;
use crate::catalog::catalog::IsCatalogRelation;
use crate::catalog::catalog_oids::RelationRelationId;
use crate::catalog::indexing::CatalogTupleUpdate;
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_class::{
    Form_pg_class, FormData_pg_class, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_TOASTVALUE,
};
use crate::catalog::pg_known_oids::{
    BTREE_AM_OID, INT4_BTREE_OPS_OID, OID_BTREE_OPS_OID, PG_TOAST_NAMESPACE,
};
use crate::catalog::pg_type_d::{BYTEAOID, INT4OID, OIDOID};
use crate::miscadmin::{IsBinaryUpgrade, IsBootstrapProcessingMode};
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::makefuncs::makeRangeVar;
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::{List, NIL};
use crate::nodes::primnodes::ONCOMMIT_NOOP;
use crate::storage::itemptr::ItemPointer;
use crate::storage::lockdefs::{
    AccessExclusiveLock, NoLock, RowExclusiveLock, ShareLock, LOCKMODE,
};
use crate::utils::rel::{Relation, RelationGetRelid};

use core::ffi::CStr;

// ----------------------------------------------------------------------------
// Local const stubs for generated-catalog symbols not yet ported.
// ----------------------------------------------------------------------------

/* syscache id for pg_class by OID (utils/syscache.h) */
const RELOID: c_int = 0;
/* pg_class index id (catalog/indexing.h) */
const ClassOidIndexId: Oid = 2662;
/* pg_class.oid attribute number */
const Anum_pg_class_oid: AttrNumber = 1;
/* fmgr oid for oideq (utils/fmgroids.h) */
const F_OIDEQ: RegProcedure = 184;
/* index_create flag (catalog/index.h) */
const INDEX_CREATE_IS_PRIMARY: uint16 = 1 << 0;
/* dependency type (catalog/dependency.h): DEPENDENCY_INTERNAL == 'i' */
const DEPENDENCY_INTERNAL: c_char = b'i' as c_char;

// ----------------------------------------------------------------------------
// Local stubs for called functions not yet ported.
// ----------------------------------------------------------------------------

// TODO: not ported - catalog/heap.c
unsafe fn heap_create_with_catalog(
    _relname: *const c_char,
    _relnamespace: Oid,
    _reltablespace: Oid,
    _relid: Oid,
    _reltypeid: Oid,
    _reloftypeid: Oid,
    _ownerid: Oid,
    _accessmtd: Oid,
    _tupdesc: TupleDesc,
    _cooked_constraints: *mut List,
    _relkind: c_char,
    _relpersistence: c_char,
    _shared_relation: bool,
    _mapped_relation: bool,
    _oncommit: crate::nodes::primnodes::OnCommitAction,
    _reloptions: Datum,
    _use_user_acl: bool,
    _allow_system_table_mods: bool,
    _is_internal: bool,
    _relrewrite: Oid,
    _typaddress: *mut ObjectAddress,
) -> Oid {
    unimplemented!()
}

// TODO: not ported - catalog/index.c
unsafe fn index_create(
    _heapRelation: Relation,
    _indexRelationName: *const c_char,
    _indexRelationId: Oid,
    _parentIndexRelid: Oid,
    _parentConstraintId: Oid,
    _relFileNumber: Oid,
    _indexInfo: *mut IndexInfo,
    _indexColNames: *mut List,
    _accessMethodId: Oid,
    _tableSpaceId: Oid,
    _collationIds: *mut Oid,
    _opclassIds: *mut Oid,
    _opclassOptions: *mut Datum,
    _coloptions: *mut int16,
    _stattargets: *mut Datum,
    _reloptions: Datum,
    _flags: uint16,
    _constr_flags: uint16,
    _allow_system_table_mods: bool,
    _is_internal: bool,
    _constraintId: *mut Oid,
) -> Oid {
    unimplemented!()
}

// TODO: not ported - access/transam/xact.c
unsafe fn CommandCounterIncrement() {
    unimplemented!()
}

// TODO: not ported - utils/cache/syscache.c
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO: not ported - catalog/pg_depend.c
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_char,
) {
    unimplemented!()
}

// TODO: not ported - catalog/namespace.c
unsafe fn isTempOrTempToastNamespace(_namespaceId: Oid) -> bool {
    unimplemented!()
}

// TODO: not ported - catalog/namespace.c
unsafe fn GetTempToastNamespace() -> Oid {
    unimplemented!()
}

// TODO: not ported - utils/cache/relcache.c (RelationIsMapped macro)
unsafe fn RelationIsMapped(_relation: Relation) -> bool {
    unimplemented!()
}

// TODO: not ported - access/table/tableam.c
unsafe fn table_relation_needs_toast_table(_rel: Relation) -> bool {
    unimplemented!()
}

// TODO: not ported - access/table/tableam.c
unsafe fn table_relation_toast_am(_rel: Relation) -> Oid {
    unimplemented!()
}

// TODO: not ported - access/index/genam.c (systable_inplace_update)
unsafe fn systable_inplace_update_begin(
    _relation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut crate::access::common::scankey::ScanKeyData,
    _oldtupcopy: *mut HeapTuple,
    _state: *mut *mut c_void,
) {
    unimplemented!()
}

// TODO: not ported - access/index/genam.c (systable_inplace_update)
unsafe fn systable_inplace_update_finish(_state: *mut c_void, _tuple: HeapTuple) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// CreateToastTable variants
//		If the table needs a toast table, and doesn't already have one,
//		then create a toast table for it.
//
// reloptions for the toast table can be passed, too.  Pass (Datum) 0
// for default reloptions.
// ----------------------------------------------------------------------------

pub unsafe fn AlterTableCreateToastTable(relOid: Oid, reloptions: Datum, lockmode: LOCKMODE) {
    CheckAndCreateToastTable(relOid, reloptions, lockmode, true, InvalidOid);
}

pub unsafe fn NewHeapCreateToastTable(
    relOid: Oid,
    reloptions: Datum,
    lockmode: LOCKMODE,
    OIDOldToast: Oid,
) {
    CheckAndCreateToastTable(relOid, reloptions, lockmode, false, OIDOldToast);
}

pub unsafe fn NewRelationCreateToastTable(relOid: Oid, reloptions: Datum) {
    CheckAndCreateToastTable(relOid, reloptions, AccessExclusiveLock, false, InvalidOid);
}

unsafe fn CheckAndCreateToastTable(
    relOid: Oid,
    reloptions: Datum,
    lockmode: LOCKMODE,
    check: bool,
    OIDOldToast: Oid,
) {
    let rel: Relation;

    rel = table_open(relOid, lockmode);

    /* create_toast_table does all the work */
    create_toast_table(
        rel,
        InvalidOid,
        InvalidOid,
        reloptions,
        lockmode,
        check,
        OIDOldToast,
    );

    table_close(rel, NoLock);
}

/*
 * Create a toast table during bootstrap
 *
 * Here we need to prespecify the OIDs of the toast table and its index
 */
pub unsafe fn BootstrapToastTable(relName: *mut c_char, toastOid: Oid, toastIndexOid: Oid) {
    let rel: Relation;

    rel = table_openrv(makeRangeVar(null_mut(), relName, -1), AccessExclusiveLock);

    if (*(*rel).rd_rel).relkind != RELKIND_RELATION
        && (*(*rel).rd_rel).relkind != RELKIND_MATVIEW
    {
        elog!(
            ERROR,
            "\"{}\" is not a table or materialized view",
            CStr::from_ptr(relName).to_string_lossy()
        );
    }

    /* create_toast_table does all the work */
    if !create_toast_table(
        rel,
        toastOid,
        toastIndexOid,
        0 as Datum,
        AccessExclusiveLock,
        false,
        InvalidOid,
    ) {
        elog!(
            ERROR,
            "\"{}\" does not require a toast table",
            CStr::from_ptr(relName).to_string_lossy()
        );
    }

    table_close(rel, NoLock);
}

/*
 * create_toast_table --- internal workhorse
 *
 * rel is already opened and locked
 * toastOid and toastIndexOid are normally InvalidOid, but during
 * bootstrap they can be nonzero to specify hand-assigned OIDs
 */
unsafe fn create_toast_table(
    rel: Relation,
    toastOid: Oid,
    toastIndexOid: Oid,
    reloptions: Datum,
    lockmode: LOCKMODE,
    check: bool,
    OIDOldToast: Oid,
) -> bool {
    let relOid: Oid = RelationGetRelid(rel);
    let mut reltup: HeapTuple = core::ptr::null_mut();
    let tupdesc: TupleDesc;
    let shared_relation: bool;
    let mapped_relation: bool;
    let toast_rel: Relation;
    let class_rel: Relation;
    let toast_relid: Oid;
    let namespaceid: Oid;
    let mut toast_relname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let mut toast_idxname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let indexInfo: *mut IndexInfo;
    let mut collationIds: [Oid; 2] = [0; 2];
    let mut opclassIds: [Oid; 2] = [0; 2];
    let mut coloptions: [int16; 2] = [0; 2];
    let mut baseobject: ObjectAddress = core::mem::zeroed();
    let mut toastobject: ObjectAddress = core::mem::zeroed();

    /*
     * Is it already toasted?
     */
    if (*(*rel).rd_rel).reltoastrelid != InvalidOid {
        return false;
    }

    /*
     * Check to see whether the table actually needs a TOAST table.
     */
    if !IsBinaryUpgrade {
        /* Normal mode, normal check */
        if !needs_toast_table(rel) {
            return false;
        }
    } else {
        /*
         * In binary-upgrade mode, create a TOAST table if and only if
         * pg_upgrade told us to (ie, a TOAST table OID has been provided).
         *
         * This indicates that the old cluster had a TOAST table for the
         * current table.  We must create a TOAST table to receive the old
         * TOAST file, even if the table seems not to need one.
         */
        if !OidIsValid(binary_upgrade_next_toast_pg_class_oid) {
            return false;
        }
    }

    /*
     * If requested check lockmode is sufficient. This is a cross check in
     * case of errors or conflicting decisions in earlier code.
     */
    if check && lockmode != AccessExclusiveLock {
        elog!(ERROR, "AccessExclusiveLock required to add toast table.");
    }

    /*
     * Create the toast table and its index
     */
    snprintf_toast(&mut toast_relname, "pg_toast_", relOid, "");
    snprintf_toast(&mut toast_idxname, "pg_toast_", relOid, "_index");

    /* this is pretty painful...  need a tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitEntry(
        tupdesc,
        1 as AttrNumber,
        c"chunk_id".as_ptr(),
        OIDOID,
        -1,
        0,
    );
    TupleDescInitEntry(
        tupdesc,
        2 as AttrNumber,
        c"chunk_seq".as_ptr(),
        INT4OID,
        -1,
        0,
    );
    TupleDescInitEntry(
        tupdesc,
        3 as AttrNumber,
        c"chunk_data".as_ptr(),
        BYTEAOID,
        -1,
        0,
    );

    /*
     * Ensure that the toast table doesn't itself get toasted, or we'll be
     * toast :-(.  This is essential for chunk_data because type bytea is
     * toastable; hit the other two just to be sure.
     */
    (*TupleDescAttr(tupdesc, 0)).attstorage = TYPSTORAGE_PLAIN;
    (*TupleDescAttr(tupdesc, 1)).attstorage = TYPSTORAGE_PLAIN;
    (*TupleDescAttr(tupdesc, 2)).attstorage = TYPSTORAGE_PLAIN;

    /* Toast field should not be compressed */
    (*TupleDescAttr(tupdesc, 0)).attcompression = InvalidCompressionMethod;
    (*TupleDescAttr(tupdesc, 1)).attcompression = InvalidCompressionMethod;
    (*TupleDescAttr(tupdesc, 2)).attcompression = InvalidCompressionMethod;

    /*
     * Toast tables for regular relations go in pg_toast; those for temp
     * relations go into the per-backend temp-toast-table namespace.
     */
    if isTempOrTempToastNamespace((*(*rel).rd_rel).relnamespace) {
        namespaceid = GetTempToastNamespace();
    } else {
        namespaceid = PG_TOAST_NAMESPACE;
    }

    /* Toast table is shared if and only if its parent is. */
    shared_relation = (*(*rel).rd_rel).relisshared;

    /* It's mapped if and only if its parent is, too */
    mapped_relation = RelationIsMapped(rel);

    toast_relid = heap_create_with_catalog(
        toast_relname.as_ptr(),
        namespaceid,
        (*(*rel).rd_rel).reltablespace,
        toastOid,
        InvalidOid,
        InvalidOid,
        (*(*rel).rd_rel).relowner,
        table_relation_toast_am(rel),
        tupdesc,
        NIL,
        RELKIND_TOASTVALUE,
        (*(*rel).rd_rel).relpersistence,
        shared_relation,
        mapped_relation,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        true,
        OIDOldToast,
        null_mut(),
    );
    Assert!(toast_relid != InvalidOid);

    /* make the toast relation visible, else table_open will fail */
    CommandCounterIncrement();

    /* ShareLock is not really needed here, but take it anyway */
    toast_rel = table_open(toast_relid, ShareLock);

    /*
     * Create unique index on chunk_id, chunk_seq.
     *
     * NOTE: the normal TOAST access routines could actually function with a
     * single-column index on chunk_id only. However, the slice access
     * routines use both columns for faster access to an individual chunk. In
     * addition, we want it to be unique as a check against the possibility of
     * duplicate TOAST chunk OIDs. The index might also be a little more
     * efficient this way, since btree isn't all that happy with large numbers
     * of equal keys.
     */

    indexInfo = makeNode!(IndexInfo, T_IndexInfo);
    (*indexInfo).ii_NumIndexAttrs = 2;
    (*indexInfo).ii_NumIndexKeyAttrs = 2;
    (*indexInfo).ii_IndexAttrNumbers[0] = 1;
    (*indexInfo).ii_IndexAttrNumbers[1] = 2;
    (*indexInfo).ii_Expressions = NIL;
    (*indexInfo).ii_ExpressionsState = NIL;
    (*indexInfo).ii_Predicate = NIL;
    (*indexInfo).ii_PredicateState = null_mut();
    (*indexInfo).ii_ExclusionOps = null_mut();
    (*indexInfo).ii_ExclusionProcs = null_mut();
    (*indexInfo).ii_ExclusionStrats = null_mut();
    (*indexInfo).ii_Unique = true;
    (*indexInfo).ii_NullsNotDistinct = false;
    (*indexInfo).ii_ReadyForInserts = true;
    (*indexInfo).ii_CheckedUnchanged = false;
    (*indexInfo).ii_IndexUnchanged = false;
    (*indexInfo).ii_Concurrent = false;
    (*indexInfo).ii_BrokenHotChain = false;
    (*indexInfo).ii_ParallelWorkers = 0;
    (*indexInfo).ii_Am = BTREE_AM_OID;
    (*indexInfo).ii_AmCache = null_mut();
    (*indexInfo).ii_Context = CurrentMemoryContext;

    collationIds[0] = InvalidOid;
    collationIds[1] = InvalidOid;

    opclassIds[0] = OID_BTREE_OPS_OID;
    opclassIds[1] = INT4_BTREE_OPS_OID;

    coloptions[0] = 0;
    coloptions[1] = 0;

    index_create(
        toast_rel,
        toast_idxname.as_ptr(),
        toastIndexOid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        indexInfo,
        list_make2!(c"chunk_id".as_ptr(), c"chunk_seq".as_ptr()),
        BTREE_AM_OID,
        (*(*rel).rd_rel).reltablespace,
        collationIds.as_mut_ptr(),
        opclassIds.as_mut_ptr(),
        null_mut(),
        coloptions.as_mut_ptr(),
        null_mut(),
        0 as Datum,
        INDEX_CREATE_IS_PRIMARY,
        0,
        true,
        true,
        null_mut(),
    );

    table_close(toast_rel, NoLock);

    /*
     * Store the toast table's OID in the parent relation's pg_class row
     */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);

    if !IsBootstrapProcessingMode() {
        /* normal case, use a transactional update */
        reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
        if !HeapTupleIsValid(reltup) {
            elog!(ERROR, "cache lookup failed for relation {}", relOid);
        }

        (*(GETSTRUCT(reltup) as Form_pg_class)).reltoastrelid = toast_relid;

        CatalogTupleUpdate(class_rel, &mut (*reltup).t_self as ItemPointer, reltup);
    } else {
        /* While bootstrapping, we cannot UPDATE, so overwrite in-place */

        let mut key: [crate::access::common::scankey::ScanKeyData; 1] = core::mem::zeroed();
        let mut state: *mut c_void = null_mut();

        ScanKeyInit(
            &mut key[0],
            Anum_pg_class_oid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(relOid),
        );
        systable_inplace_update_begin(
            class_rel,
            ClassOidIndexId,
            true,
            null_mut(),
            1,
            key.as_mut_ptr(),
            &mut reltup,
            &mut state,
        );
        if !HeapTupleIsValid(reltup) {
            elog!(ERROR, "cache lookup failed for relation {}", relOid);
        }

        (*(GETSTRUCT(reltup) as Form_pg_class)).reltoastrelid = toast_relid;

        systable_inplace_update_finish(state, reltup);
    }

    heap_freetuple(reltup);

    table_close(class_rel, RowExclusiveLock);

    /*
     * Register dependency from the toast table to the main, so that the toast
     * table will be deleted if the main is.  Skip this in bootstrap mode.
     */
    if !IsBootstrapProcessingMode() {
        baseobject.classId = RelationRelationId;
        baseobject.objectId = relOid;
        baseobject.objectSubId = 0;
        toastobject.classId = RelationRelationId;
        toastobject.objectId = toast_relid;
        toastobject.objectSubId = 0;

        recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
    }

    /*
     * Make changes visible
     */
    CommandCounterIncrement();

    true
}

/*
 * Check to see whether the table needs a TOAST table.
 */
unsafe fn needs_toast_table(rel: Relation) -> bool {
    /*
     * No need to create a TOAST table for partitioned tables.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        return false;
    }

    /*
     * We cannot allow toasting a shared relation after initdb (because
     * there's no way to mark it toasted in other databases' pg_class).
     */
    if (*(*rel).rd_rel).relisshared && !IsBootstrapProcessingMode() {
        return false;
    }

    /*
     * Ignore attempts to create toast tables on catalog tables after initdb.
     * Which catalogs get toast tables is explicitly chosen in catalog/pg_*.h.
     * (We could get here via some ALTER TABLE command if the catalog doesn't
     * have a toast table.)
     */
    if IsCatalogRelation(rel) && !IsBootstrapProcessingMode() {
        return false;
    }

    /* Otherwise, let the AM decide. */
    table_relation_needs_toast_table(rel)
}

/*
 * Helper replicating snprintf(buf, sizeof(buf), "<prefix>%u<suffix>", relOid).
 * The C source uses snprintf into NAMEDATALEN-sized buffers; this writes the
 * formatted, NUL-terminated string into the fixed-size buffer.
 */
unsafe fn snprintf_toast(
    buf: &mut [c_char; NAMEDATALEN],
    prefix: &str,
    relOid: Oid,
    suffix: &str,
) {
    let s = format!("{}{}{}", prefix, relOid, suffix);
    let bytes = s.as_bytes();
    let n = core::cmp::min(bytes.len(), NAMEDATALEN - 1);
    for i in 0..n {
        buf[i] = bytes[i] as c_char;
    }
    buf[n] = 0;
}
