//! Genetic query optimizer (postgres/src/backend/optimizer/geqo).
//!
//! So far: the PMX crossover operator (`geqo_pmx`).

pub mod geqo_copy;
pub mod geqo_cx;
pub mod geqo_erx;
pub mod geqo_eval;
pub mod geqo_main;
pub mod geqo;
pub mod geqo_gene;
pub mod geqo_misc;
pub mod geqo_mutation;
pub mod geqo_ox1;
pub mod geqo_ox2;
pub mod geqo_pmx;
pub mod geqo_pool;
pub mod geqo_px;
pub mod geqo_random;
pub mod geqo_recombination;
pub mod geqo_selection;
