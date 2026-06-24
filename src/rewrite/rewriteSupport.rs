//! Translated from PostgreSQL src/include/rewrite/rewriteSupport.h

use crate::postgres_ext::Oid;

/// The ON SELECT rule of a view is always named this.
pub const VIEW_SELECT_RULE_NAME: &str = "_RETURN";

pub fn is_defined_rewrite_rule(_owning_rel: Oid, _rule_name: &str) -> bool {
    unimplemented!()
}

pub fn set_relation_rule_status(_relation_id: Oid, _rel_has_rules: bool) {
    unimplemented!()
}

/// C: `Oid get_rewrite_oid(relid, rulename, missing_ok)`. The `missing_ok`
/// flag collapses into `Option`: `None` is the not-found / InvalidOid case.
pub fn get_rewrite_oid(_relid: Oid, _rulename: &str) -> Option<Oid> {
    unimplemented!()
}
