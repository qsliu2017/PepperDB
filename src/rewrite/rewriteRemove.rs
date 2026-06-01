//! rewrite/rewriteRemove.c - routines for removing rewrite rules.

use crate::prelude::*;

use std::ffi::c_void;

use crate::access::attnum::AttrNumber;
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::relscan::SysScanDescData;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog::IsSystemRelation;
use crate::catalog::catalog_oids::RewriteRelationId;
use crate::catalog::pg_rewrite::Form_pg_rewrite;
use crate::miscadmin::allowSystemTableMods;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lockdefs::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::utils::snapshot::SnapshotData;

/* TODO(pg-port): utils/inval.h - shared cache invalidation not ported yet. */
unsafe fn CacheInvalidateRelcache(_relation: Relation) {
    unimplemented!()
}

/*
 * The index OID for pg_rewrite's OID column and the OID column's attribute
 * number. catalog/pg_rewrite.h / catalog/indexing.h have not provided these
 * generated constants in the port yet.
 */
// TODO(pg-port): replace with the generated catalog/indexing.h constant.
const RewriteOidIndexId: Oid = 2692;
// TODO(pg-port): replace with the generated Anum_pg_rewrite_oid (pg_rewrite.h).
const Anum_pg_rewrite_oid: AttrNumber = 1;

/*
 * F_OIDEQ - the regproc OID of the oideq() built-in comparison function.
 * utils/fmgroids.h is not ported; the value below matches PostgreSQL 18.3.
 */
// TODO(pg-port): replace with the generated utils/fmgroids.h constant.
const F_OIDEQ: RegProcedure = 184;

/* TODO(pg-port): access/genam.h - systable scan helpers not ported yet. */
type SysScanDesc = *mut SysScanDescData;

unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut SnapshotData,
    _nkeys: c_int,
    _key: ScanKey,
) -> SysScanDesc {
    unimplemented!()
}

unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!()
}

unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!()
}

/* TODO(pg-port): catalog/indexing.h - heap+index delete helper not ported yet. */
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut ItemPointerData) {
    unimplemented!()
}

/*
 * Guts of rule deletion.
 */
pub unsafe fn RemoveRewriteRuleById(ruleOid: Oid) {
    let RewriteRelation: Relation;
    let mut skey: [ScanKeyData; 1] = std::mem::zeroed();
    let rcscan: SysScanDesc;
    let event_relation: Relation;
    let tuple: HeapTuple;
    let eventRelationOid: Oid;

    /*
     * Open the pg_rewrite relation.
     */
    RewriteRelation = table_open(RewriteRelationId, RowExclusiveLock);

    /*
     * Find the tuple for the target rule.
     */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_rewrite_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(ruleOid),
    );

    rcscan = systable_beginscan(
        RewriteRelation,
        RewriteOidIndexId,
        true,
        std::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    tuple = systable_getnext(rcscan);

    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "could not find tuple for rule");
    }

    /*
     * We had better grab AccessExclusiveLock to ensure that no queries are
     * going on that might depend on this rule.  (Note: a weaker lock would
     * suffice if it's not an ON SELECT rule.)
     */
    eventRelationOid = (*(GETSTRUCT(tuple) as Form_pg_rewrite)).ev_class;
    event_relation = table_open(eventRelationOid, AccessExclusiveLock);

    if !allowSystemTableMods && IsSystemRelation(event_relation) {
        ereport!(
            ERROR,
            "permission denied: \"%s\" is a system catalog"
        );
        let _ = RelationGetRelationName(event_relation);
    }

    /*
     * Now delete the pg_rewrite tuple for the rule
     */
    CatalogTupleDelete(RewriteRelation, &mut (*tuple).t_self);

    systable_endscan(rcscan);

    table_close(RewriteRelation, RowExclusiveLock);

    /*
     * Issue shared-inval notice to force all backends (including me!) to
     * update relcache entries with the new rule set.
     */
    CacheInvalidateRelcache(event_relation);

    /* Close rel, but keep lock till commit... */
    table_close(event_relation, NoLock);
}
