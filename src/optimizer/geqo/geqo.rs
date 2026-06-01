//! optimizer/geqo.h - prototypes for various files in optimizer/geqo

use std::ffi::c_int;

use crate::common::pg_prng::pg_prng_state;
use crate::nodes::nodes::Cost;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::nodes::pg_list::List;
use crate::optimizer::geqo::geqo_gene::Gene;

/* GEQO debug flag */
/*
 #define GEQO_DEBUG
 */

/* choose one recombination mechanism here */
/*
 #define ERX
 #define PMX
 #define CX
 #define PX
 #define OX1
 #define OX2
 */
// #define ERX
pub const ERX: () = ();

/*
 * Configuration options
 *
 * If you change these, update backend/utils/misc/postgresql.conf.sample
 */
// extern PGDLLIMPORT int Geqo_effort; /* 1 .. 10, knob for adjustment of defaults */
pub static mut Geqo_effort: c_int = 0;

pub const DEFAULT_GEQO_EFFORT: c_int = 5;
pub const MIN_GEQO_EFFORT: c_int = 1;
pub const MAX_GEQO_EFFORT: c_int = 10;

// extern PGDLLIMPORT int Geqo_pool_size; /* 2 .. inf, or 0 to use default */
pub static mut Geqo_pool_size: c_int = 0;

// extern PGDLLIMPORT int Geqo_generations; /* 1 .. inf, or 0 to use default */
pub static mut Geqo_generations: c_int = 0;

// extern PGDLLIMPORT double Geqo_selection_bias;
pub static mut Geqo_selection_bias: f64 = 0.0;

pub const DEFAULT_GEQO_SELECTION_BIAS: f64 = 2.0;
pub const MIN_GEQO_SELECTION_BIAS: f64 = 1.5;
pub const MAX_GEQO_SELECTION_BIAS: f64 = 2.0;

// extern PGDLLIMPORT double Geqo_seed; /* 0 .. 1 */
pub static mut Geqo_seed: f64 = 0.0;

/*
 * Private state for a GEQO run --- accessible via root->join_search_private
 */
#[repr(C)]
pub struct GeqoPrivateData {
    pub initial_rels: *mut List,      /* the base relations we are joining */
    pub random_state: pg_prng_state,  /* PRNG state */
}

/* routines in geqo_main.c */
pub unsafe fn geqo(
    root: *mut PlannerInfo,
    number_of_rels: c_int,
    initial_rels: *mut List,
) -> *mut RelOptInfo {
    unimplemented!()
}

/* routines in geqo_eval.c */
pub unsafe fn geqo_eval(root: *mut PlannerInfo, tour: *mut Gene, num_gene: c_int) -> Cost {
    unimplemented!()
}

pub unsafe fn gimme_tree(
    root: *mut PlannerInfo,
    tour: *mut Gene,
    num_gene: c_int,
) -> *mut RelOptInfo {
    unimplemented!()
}
