//! Translated from PostgreSQL src/include/utils/index_selfuncs.h
//! Index cost estimation functions for standard index access methods.
//!
//! Each C function has the `amcostestimate_function` shape (root, path,
//! loop_count + five out-params: startup/total cost, selectivity, correlation,
//! pages). The out-params collapse into the existing `IndexCostEstimate` struct
//! from `crate::access::amapi`, matching `IndexAm::cost_estimate`'s return.

use crate::access::amapi::IndexCostEstimate;
use crate::nodes::pathnodes::{IndexPath, PlannerInfo};

// Functions in selfuncs.c

pub fn brincostestimate(
    _root: &PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> IndexCostEstimate {
    unimplemented!()
}

pub fn btcostestimate(
    _root: &PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> IndexCostEstimate {
    unimplemented!()
}

pub fn hashcostestimate(
    _root: &PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> IndexCostEstimate {
    unimplemented!()
}

pub fn gistcostestimate(
    _root: &PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> IndexCostEstimate {
    unimplemented!()
}

pub fn spgcostestimate(
    _root: &PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> IndexCostEstimate {
    unimplemented!()
}

pub fn gincostestimate(
    _root: &PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> IndexCostEstimate {
    unimplemented!()
}
