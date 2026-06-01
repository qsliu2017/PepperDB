//! index_selfuncs.h - Index cost estimation functions for standard index access methods.
//!
//! Note: this is split out of selfuncs.h mainly to avoid importing all of the
//! planner's data structures into the non-planner parts of the index AMs.

use crate::nodes::nodes::{Cost, Selectivity};
use crate::nodes::pathnodes::{IndexPath, PlannerInfo};

/* Functions in selfuncs.c */

pub unsafe fn brincostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn btcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn hashcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn gistcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn spgcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn gincostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!()
}
