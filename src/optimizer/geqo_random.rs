//! Translated from PostgreSQL src/include/optimizer/geqo_random.h
//! random number generator
//! -- parts of this are adapted from D. Whitley's Genitor algorithm --

use crate::nodes::pathnodes::PlannerInfo;

pub fn geqo_set_seed(root: &mut PlannerInfo, seed: f64) {
    unimplemented!()
}

/// geqo_rand returns a random float value in the range [0.0, 1.0)
pub fn geqo_rand(root: &mut PlannerInfo) -> f64 {
    unimplemented!()
}

/// geqo_randint returns integer value between lower and upper inclusive
pub fn geqo_randint(root: &mut PlannerInfo, upper: i32, lower: i32) -> i32 {
    unimplemented!()
}
