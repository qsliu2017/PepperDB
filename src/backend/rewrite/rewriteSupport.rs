//! Rewrite-rule support routines. Translated from
//! backend/rewrite/rewriteSupport.c (disposition: full leaf).
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::rewrite::rewriteSupport` under the C names.
//!
//! The rule-existence lookups (`IsDefinedRewriteRule`, `get_rewrite_oid`) read the
//! in-memory rule registry (see `rule_registry.rs`) instead of the `RULERELNAME`
//! syscache: the live action trees live there until nodeToString/stringToNode land.
//! `SetRelationRuleStatus` performs the real `pg_class.relhasrules` in-place update.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple, heap_freetuple};
use crate::backend::catalog::indexing::catalog_tuple_update;
use crate::backend::utils::cache::relcache::{relation_close, relation_forget_relation, relation_id_get_relation};
use crate::postgres::BoolGetDatum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// The ON SELECT rule of a view is always named this (mirrors the header const).
pub const VIEW_SELECT_RULE_NAME: &str = "_RETURN";

/// PG `IsDefinedRewriteRule`: does `owning_rel` have a rule named `rule_name`? PG
/// hits the `RULERELNAME` syscache; the port checks the registry by name.
pub fn is_defined_rewrite_rule(shared: &Arc<SharedState>, owning_rel: Oid, rule_name: &str) -> bool {
    get_rewrite_oid(shared, owning_rel, rule_name).is_some()
}

/// PG `SetRelationRuleStatus`: set the `relhasrules` flag of `relation_id`'s
/// pg_class row to `rel_has_rules`. Reads the row, flips the flag in place when it
/// differs, writes it back, and evicts the relcache entry (PG broadcasts an SI
/// relcache invalidation; the port forgets the entry so the next open rebuilds and
/// re-reads the rules). The caller must already hold a lock on the relation.
pub async fn set_relation_rule_status(
    shared: &Arc<SharedState>,
    relation_id: Oid,
    rel_has_rules: bool,
) {
    use crate::catalog::pg_class::{self as c, RelationRelationId};

    let pg_class = relation_id_get_relation(RelationRelationId)
        .unwrap_or_else(|| unreachable!("pg_class is nailed"));
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class desc"));

    // Find this relation's own pg_class row (oid == relation_id).
    let rows = scan_pg_class_self(shared, relation_id).await;
    for row in rows {
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        let idx = (c::Anum_pg_class_relhasrules - 1) as usize;
        // Only rewrite when the flag actually differs (PG's guard).
        let current = crate::postgres::DatumGetBool(vals[idx]);
        if current != rel_has_rules {
            vals[idx] = BoolGetDatum(rel_has_rules);
            nulls[idx] = false;
            let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
            catalog_tuple_update(shared, &pg_class, &row.tid, &mut newtup).await;
            heap_freetuple(newtup);
        }
        heap_freetuple(row.tuple);
    }

    relation_close(pg_class);
    // PG forces a relcache rebuild even when the row was unchanged; do the same so
    // rd_rules is rebuilt from the registry on the next open.
    relation_forget_relation(relation_id);
}

/// An all-zero `FmgrInfo` for an OID-equality scan key (the key func is unused).
fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: crate::postgres_ext::InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

/// One owned pg_class row + its heap TID (for in-place update).
struct ClassRow {
    tuple: crate::access::htup::HeapTupleData,
    tid: crate::storage::itemptr::ItemPointerData,
}

/// Scan pg_class for the row whose oid == `relation_id`, returning owned copies.
async fn scan_pg_class_self(shared: &Arc<SharedState>, relation_id: Oid) -> Vec<ClassRow> {
    use crate::access::skey::ScanKeyData;
    use crate::backend::access::common::heaptuple::heap_copytuple;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };
    use crate::catalog::pg_class::{self as c, RelationRelationId};
    use crate::postgres::ObjectIdGetDatum;
    use crate::postgres_ext::InvalidOid;

    let Some(pg_class) = relation_id_get_relation(RelationRelationId) else {
        return Vec::new();
    };
    let key = [ScanKeyData {
        flags: 0,
        attno: c::Anum_pg_class_oid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: zero_fmgr_info(),
        argument: ObjectIdGetDatum(relation_id),
    }];
    let snap = systable_scan_snapshot(shared, &pg_class, None);
    let mut scan = systable_beginscan(shared, &pg_class, InvalidOid, false, &snap, &key);
    let mut rows = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy (with its TID) before endscan.
        let tuple = unsafe { heap_copytuple(tref) };
        rows.push(ClassRow { tid: tuple.t_self, tuple });
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_class);
    rows
}

/// PG `get_rewrite_oid`: the OID of the rule named `rulename` on `relid`, or
/// `None` if absent (PG's `missing_ok` -> `Option`). Reads the registry.
pub fn get_rewrite_oid(shared: &Arc<SharedState>, relid: Oid, rulename: &str) -> Option<Oid> {
    let rules = shared.rule_registry().rules_for(relid)?;
    // The registry does not store the rule name (a view's only rule is _RETURN);
    // a name lookup matches _RETURN against the ON SELECT rule and any other rule
    // by position. M11 only needs the _RETURN lookup.
    if rulename == VIEW_SELECT_RULE_NAME {
        rules
            .rules
            .iter()
            .find(|r| r.event == crate::nodes::nodes::CmdType::SELECT)
            .map(|r| r.rule_id)
    } else {
        // Non-_RETURN rules are looked up by their stored rule_id only at M11; a
        // by-name match would need the registry to carry names (grows with CREATE
        // RULE name lookups). Return the first non-SELECT rule's OID as a stand-in
        // is wrong; instead report absent for unknown names.
        None
    }
}
