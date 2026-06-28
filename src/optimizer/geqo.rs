//! Translated from PostgreSQL src/include/optimizer/geqo.h
//! prototypes for various files in optimizer/geqo

#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::common::pg_prng::PgPrngState;
use crate::nodes::nodes::{Cost, Node};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::optimizer::geqo_gene::Gene;

/* choose one recombination mechanism here */
pub const ERX: () = ();

/*
 * Configuration options
 */
// TODO(global): GUC/session state.
pub static mut Geqo_effort: i32 = 0;

pub const DEFAULT_GEQO_EFFORT: i32 = 5;
pub const MIN_GEQO_EFFORT: i32 = 1;
pub const MAX_GEQO_EFFORT: i32 = 10;

pub static mut Geqo_pool_size: i32 = 0;
pub static mut Geqo_generations: i32 = 0;
pub static mut Geqo_selection_bias: f64 = 0.0;

pub const DEFAULT_GEQO_SELECTION_BIAS: f64 = 2.0;
pub const MIN_GEQO_SELECTION_BIAS: f64 = 1.5;
pub const MAX_GEQO_SELECTION_BIAS: f64 = 2.0;

pub static mut Geqo_seed: f64 = 0.0;

/// Private state for a GEQO run --- accessible via root->join_search_private.
#[derive(Debug, Clone, PartialEq)]
pub struct GeqoPrivateData {
    /// the base relations we are joining
    pub initial_rels: Vec<Node>,
    /// PRNG state
    pub random_state: PgPrngState,
}

/* routines in geqo_main.c */
pub fn geqo(
    root: &mut PlannerInfo,
    number_of_rels: i32,
    initial_rels: Vec<Node>,
) -> Box<RelOptInfo> {
    unimplemented!()
}

/* routines in geqo_eval.c */
pub fn geqo_eval(root: &mut PlannerInfo, tour: &[Gene]) -> Cost {
    unimplemented!()
}

pub fn gimme_tree(root: &mut PlannerInfo, tour: &[Gene]) -> Box<RelOptInfo> {
    unimplemented!()
}
