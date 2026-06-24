//! Translated from PostgreSQL src/include/optimizer/geqo_copy.h
//! prototypes for copy functions in optimizer/geqo

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo_gene::Chromosome;

pub fn geqo_copy(
    root: &mut PlannerInfo,
    chromo1: &mut Chromosome,
    chromo2: &Chromosome,
    string_length: i32,
) {
    unimplemented!()
}
