//! Translated from PostgreSQL src/include/optimizer/geqo_gene.h

use crate::nodes::nodes::Cost;

/// A single gene; int is fine in place of Relid (C: "don't change it!").
pub type Gene = i32;

/// One genome: a permutation of genes plus its fitness.
#[derive(Debug, Clone, PartialEq)]
pub struct Chromosome {
    pub string: Vec<Gene>,
    pub worth: Cost,
}

/// A population of chromosomes.
#[derive(Debug, Clone, PartialEq)]
pub struct Pool {
    pub data: Vec<Chromosome>,
    pub size: i32,
    pub string_length: i32,
}
