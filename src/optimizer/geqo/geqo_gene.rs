//! optimizer/geqo_gene.h - genome representation in optimizer/geqo

use std::ffi::c_int;

use crate::nodes::nodes::Cost;

/* we presume that int instead of Relid
   is o.k. for Gene; so don't change it! */
pub type Gene = c_int;

#[repr(C)]
pub struct Chromosome {
    pub string: *mut Gene,
    pub worth: Cost,
}

#[repr(C)]
pub struct Pool {
    pub data: *mut Chromosome,
    pub size: c_int,
    pub string_length: c_int,
}
