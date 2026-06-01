//! Planner plan-creation phase (postgres/src/backend/optimizer/plan).
//!
//! So far: the main query-planning entry (`planmain`).

pub mod analyzejoins;
pub mod initsplan;
pub mod subselect;
pub mod planner;
pub mod createplan;
pub mod setrefs;
pub mod planmain;
pub mod planagg;
