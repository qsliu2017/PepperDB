//! Translated from PostgreSQL src/include/optimizer/geqo_misc.h

// GEQO debug printout routines (only compiled under GEQO_DEBUG in C).
// FILE* output maps to a Rust io::Write sink; stubbed for the skeleton.

#[deprecated(note = "TODO(struct-forward): repoint to crate::optimizer::geqo_gene::Pool in Phase 2")]
pub struct Pool; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::optimizer::geqo_recombination::Edge in Phase 2")]
pub struct Edge; // TODO(struct-forward)

#[allow(deprecated)]
pub fn print_pool(_pool: &Pool, _start: i32, _stop: i32) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn print_gen(_pool: &Pool, _generation: i32) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn print_edge_table(_edge_table: &Edge, _num_gene: i32) {
    unimplemented!()
}
