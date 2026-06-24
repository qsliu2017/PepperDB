//! Translated from PostgreSQL src/include/optimizer/geqo_mutation.h
//! prototypes for mutation functions in optimizer/geqo

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo_gene::Gene;

pub fn geqo_mutation(root: &mut PlannerInfo, tour: &mut [Gene], num_gene: i32) {
    unimplemented!()
}
