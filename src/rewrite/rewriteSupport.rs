//! Translated from PostgreSQL src/include/rewrite/rewriteSupport.h

/// The ON SELECT rule of a view is always named this.
pub const VIEW_SELECT_RULE_NAME: &str = "_RETURN";

pub use crate::backend::rewrite::rewriteSupport::get_rewrite_oid;
pub use crate::backend::rewrite::rewriteSupport::is_defined_rewrite_rule as IsDefinedRewriteRule;
pub use crate::backend::rewrite::rewriteSupport::set_relation_rule_status as SetRelationRuleStatus;
