//! rewrite/rewriteSupport.c - rule support routines (rule existence, relhasrules, oid lookup)

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::common::heaptuple::heap_freetuple;
use crate::access::htup_details::{GETSTRUCT, HeapTuple, HeapTupleIsValid};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::RelationRelationId;
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_rewrite::Form_pg_rewrite;
use crate::storage::lockdefs::RowExclusiveLock;
use crate::utils::rel::Relation;

use crate::ereport;
use crate::elog;

// ---------------------------------------------------------------------------
// Syscache identifiers used by this file. The real syscache subsystem
// (utils/cache/syscache.c) is not yet ported, so we define the cache-id
// constants locally and stub the lookup functions below.
// TODO: dep not ported - utils/cache/syscache.c
// ---------------------------------------------------------------------------
#[allow(non_upper_case_globals)]
const RULERELNAME: c_int = 60; // syscache id placeholder
#[allow(non_upper_case_globals)]
const RELOID: c_int = 57; // syscache id placeholder

// TODO: dep not ported - utils/cache/syscache.c
unsafe fn SearchSysCacheExists2(cacheId: c_int, key1: Datum, key2: Datum) -> bool {
    crate::utils::cache::syscache::SearchSysCacheExists2(cacheId, key1, key2)
}

// TODO: dep not ported - utils/cache/syscache.c
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple {
    crate::catalog::objectaddress_impl::SearchSysCacheCopy1(cacheId, key1)
}

// TODO: dep not ported - utils/cache/syscache.c
unsafe fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache2(cacheId, key1, key2)
}

// TODO: dep not ported - utils/cache/syscache.c
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(tuple)
}

// TODO: dep not ported - catalog/indexing.c
unsafe fn CatalogTupleUpdate(
    heapRel: Relation,
    otid: *mut crate::storage::itemptr::ItemPointerData,
    tup: HeapTuple,
) {
    crate::catalog::indexing::CatalogTupleUpdate(heapRel, otid as _, tup)
}

// TODO: dep not ported - utils/cache/inval.c
unsafe fn CacheInvalidateRelcacheByTuple(classTuple: HeapTuple) {
    crate::utils::cache::inval::CacheInvalidateRelcacheByTuple(classTuple)
}

// TODO: dep not ported - utils/cache/lsyscache.c
unsafe fn get_rel_name(relid: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_rel_name(relid)
}

/*
 * Is there a rule by the given name?
 */
#[allow(non_snake_case)]
pub unsafe fn IsDefinedRewriteRule(owningRel: Oid, ruleName: *const c_char) -> bool {
    SearchSysCacheExists2(
        RULERELNAME,
        ObjectIdGetDatum(owningRel),
        PointerGetDatum(ruleName as *const c_void),
    )
}

/*
 * SetRelationRuleStatus
 *		Set the value of the relation's relhasrules field in pg_class.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing relcache
 * entries to be flushed or updated with the new set of rules for the table.
 * This must happen even if we find that no change is needed in the pg_class
 * row.
 */
#[allow(non_snake_case)]
pub unsafe fn SetRelationRuleStatus(relationId: Oid, relHasRules: bool) {
    let relationRelation: Relation;
    let tuple: HeapTuple;
    let classForm: Form_pg_class;

    /*
     * Find the tuple to update in pg_class, using syscache for the lookup.
     */
    relationRelation = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relationId));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation");
    }
    classForm = GETSTRUCT(tuple) as Form_pg_class;

    if (*classForm).relhasrules != relHasRules {
        /* Do the update */
        (*classForm).relhasrules = relHasRules;

        CatalogTupleUpdate(relationRelation, &mut (*tuple).t_self, tuple);
    } else {
        /* no need to change tuple, but force relcache rebuild anyway */
        CacheInvalidateRelcacheByTuple(tuple);
    }

    heap_freetuple(tuple);
    table_close(relationRelation, RowExclusiveLock);
}

/*
 * Find rule oid.
 *
 * If missing_ok is false, throw an error if rule name not found.  If
 * true, just return InvalidOid.
 */
#[allow(non_snake_case)]
pub unsafe fn get_rewrite_oid(relid: Oid, rulename: *const c_char, missing_ok: bool) -> Oid {
    let tuple: HeapTuple;
    let ruleform: Form_pg_rewrite;
    let ruleoid: Oid;

    /* Find the rule's pg_rewrite tuple, get its OID */
    tuple = SearchSysCache2(
        RULERELNAME,
        ObjectIdGetDatum(relid),
        PointerGetDatum(rulename as *const c_void),
    );
    if !HeapTupleIsValid(tuple) {
        if missing_ok {
            return InvalidOid;
        }
        let _ = get_rel_name(relid);
        ereport!(
            ERROR,
            "rule for relation does not exist"
        );
    }
    ruleform = GETSTRUCT(tuple) as Form_pg_rewrite;
    debug_assert_eq!(relid, (*ruleform).ev_class);
    ruleoid = (*ruleform).oid;
    ReleaseSysCache(tuple);
    ruleoid
}
