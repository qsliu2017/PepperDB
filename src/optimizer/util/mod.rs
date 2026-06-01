//! Planner/optimizer utility routines (postgres/src/backend/optimizer/util).
//!
//! So far: variable-reference analysis (`var`), RestrictInfo construction
//! (`restrictinfo`), and join-clause bookkeeping (`joininfo`).

pub mod appendinfo;
pub mod clauses;
pub mod pathnode;
pub mod joininfo;
pub mod orclauses;
pub mod paramassign;
pub mod placeholder;
pub mod predtest;
pub mod plancat;
pub mod relnode;
pub mod restrictinfo;
pub mod tlist;
pub mod var;
pub mod inherit;
