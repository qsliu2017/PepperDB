//! Backend `access` subsystem (postgres/src/backend/access + postgres/src/include/access).
//!
//! Only the transaction-ID type layer (transam types/macros) is present so far.

pub mod attnum;
pub mod brin;
pub mod cmptype;
pub mod common;
pub mod heap;
pub mod sequence;
pub mod gin;
pub mod gist;
pub mod hash;
pub mod htup_details;
pub mod index;
pub mod nbtree;
pub mod relscan;
pub mod rmgrdesc;
pub mod rmgrlist;
pub mod sdir;
pub mod spgist;
pub mod stratnum;
pub mod sysattr;
pub mod table;
pub mod tablesample;
pub mod transam;
pub mod tsmapi;
pub mod tupmacs;
pub mod valid;
pub mod visibilitymapdefs;
