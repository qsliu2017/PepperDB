//! Translated from PostgreSQL src/include/optimizer/joininfo.h
//! prototypes for joininfo.c.

pub use crate::backend::optimizer::util::joininfo::{
    add_join_clause_to_rels, have_relevant_joinclause, remove_join_clause_from_rels,
};
