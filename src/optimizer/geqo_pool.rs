//! Translated from PostgreSQL src/include/optimizer/geqo_pool.h
//! pool representation in optimizer/geqo

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo_gene::{Chromosome, Pool};

pub fn alloc_pool(root: &mut PlannerInfo, pool_size: i32, string_length: i32) -> Box<Pool> {
    unimplemented!()
}

pub fn free_pool(root: &mut PlannerInfo, pool: &mut Pool) {
    unimplemented!()
}

pub fn random_init_pool(root: &mut PlannerInfo, pool: &mut Pool) {
    unimplemented!()
}

pub fn alloc_chromo(root: &mut PlannerInfo, string_length: i32) -> Box<Chromosome> {
    unimplemented!()
}

pub fn free_chromo(root: &mut PlannerInfo, chromo: &mut Chromosome) {
    unimplemented!()
}

pub fn spread_chromo(root: &mut PlannerInfo, chromo: &Chromosome, pool: &mut Pool) {
    unimplemented!()
}

pub fn sort_pool(root: &mut PlannerInfo, pool: &mut Pool) {
    unimplemented!()
}
