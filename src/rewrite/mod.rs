//! Query rewriter (postgres/src/backend/rewrite + postgres/src/include/rewrite).
//!
//! So far: the expression/query-tree manipulation mutators (`rewriteManip`),
//! used by both the rewriter and the planner.

pub mod prs2lock;
pub mod rewriteDefine;
pub mod rewriteManip;
pub mod rewriteRemove;
pub mod rewriteSupport;
pub mod rewriteSearchCycle;
pub mod rowsecurity;
pub mod rewriteHandler;
