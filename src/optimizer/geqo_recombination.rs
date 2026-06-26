//! Translated from PostgreSQL src/include/optimizer/geqo_recombination.h
//! prototypes for recombination in the genetic query optimizer
//! -- parts of this are adapted from D. Whitley's Genitor algorithm --

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo_gene::Gene;

pub fn init_tour(root: &mut PlannerInfo, tour: &mut [Gene], num_gene: i32) {
    unimplemented!()
}

/// edge recombination crossover [ERX]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// list of edges
    pub edge_list: [Gene; 4],
    pub total_edges: i32,
    pub unused_edges: i32,
}

pub fn alloc_edge_table(root: &mut PlannerInfo, num_gene: i32) -> Vec<Edge> {
    unimplemented!()
}

pub fn free_edge_table(root: &mut PlannerInfo, edge_table: &mut [Edge]) {
    unimplemented!()
}

pub fn gimme_edge_table(
    root: &mut PlannerInfo,
    tour1: &[Gene],
    tour2: &[Gene],
    num_gene: i32,
    edge_table: &mut [Edge],
) -> f32 {
    unimplemented!()
}

pub fn gimme_tour(
    root: &mut PlannerInfo,
    edge_table: &[Edge],
    new_gene: &mut [Gene],
    num_gene: i32,
) -> i32 {
    unimplemented!()
}

/// indicator for gene from dad
pub const DAD: i32 = 1;
/// indicator for gene from mom
pub const MOM: i32 = 0;

/// partially matched crossover [PMX]
pub fn pmx(
    root: &mut PlannerInfo,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
) {
    unimplemented!()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct City {
    pub tour2_position: i32,
    pub tour1_position: i32,
    pub used: i32,
    pub select_list: i32,
}

pub fn alloc_city_table(root: &mut PlannerInfo, num_gene: i32) -> Vec<City> {
    unimplemented!()
}

pub fn free_city_table(root: &mut PlannerInfo, city_table: &mut [City]) {
    unimplemented!()
}

/// cycle crossover [CX]
pub fn cx(
    root: &mut PlannerInfo,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) -> i32 {
    unimplemented!()
}

/// position crossover [PX]
pub fn px(
    root: &mut PlannerInfo,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    unimplemented!()
}

/// order crossover [OX1] according to Davis
pub fn ox1(
    root: &mut PlannerInfo,
    mom: &[Gene],
    dad: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    unimplemented!()
}

/// order crossover [OX2] according to Syswerda
pub fn ox2(
    root: &mut PlannerInfo,
    mom: &[Gene],
    dad: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    unimplemented!()
}
