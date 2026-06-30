//! Translated from PostgreSQL src/include/rewrite/rewriteDefine.h

pub const RULE_FIRES_ON_ORIGIN: u8 = b'O';
pub const RULE_FIRES_ALWAYS: u8 = b'A';
pub const RULE_FIRES_ON_REPLICA: u8 = b'R';
pub const RULE_DISABLED: u8 = b'D';

pub use crate::backend::rewrite::rewriteDefine::define_query_rewrite as DefineQueryRewrite;
pub use crate::backend::rewrite::rewriteDefine::define_rule as DefineRule;
