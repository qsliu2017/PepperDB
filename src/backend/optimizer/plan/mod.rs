//! Directory module: src/backend/optimizer/plan. Mirrors
//! ref/postgres/src/backend/optimizer/plan/.

pub mod analyzejoins;
pub mod createplan;
pub mod initsplan;
pub mod planagg;
pub mod planmain;
pub mod planner;
pub mod setrefs;
pub mod subselect;

#[cfg(test)]
mod setop_cte_tests;
