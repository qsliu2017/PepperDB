//! B-tree access method (postgres/src/backend/access/nbtree).
//!
//! So far: the datatype comparison support functions (`nbtcompare`) and the
//! opclass validator (`nbtvalidate`).

pub mod nbtutils;
pub mod nbtxlog;
pub mod nbtsplitloc;
pub mod nbtdedup;
pub mod nbtcompare;
pub mod nbtvalidate;
pub mod nbtpage;
pub mod nbtinsert;
pub mod nbtsearch;
pub mod nbtsort;
pub mod nbtree;
pub mod nbtpreprocesskeys;
