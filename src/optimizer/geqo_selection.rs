//! Translated from PostgreSQL src/include/optimizer/geqo_selection.h
//! prototypes for selection routines in optimizer/geqo

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo_gene::{Chromosome, Pool};

pub fn geqo_selection(
    root: &mut PlannerInfo,
    momma: &mut Chromosome,
    daddy: &mut Chromosome,
    pool: &Pool,
    bias: f64,
) {
    unimplemented!()
}
