//! WAL resource-manager record descriptions (postgres/src/backend/access/rmgrdesc).
//!
//! Used by pg_waldump to format WAL records. So far: hash + SP-GiST.

pub mod brindesc;
pub mod clogdesc;
pub mod committsdesc;
pub mod dbasedesc;
pub mod genericdesc;
pub mod gindesc;
pub mod gistdesc;
pub mod hashdesc;
pub mod heapdesc;
pub mod logicalmsgdesc;
pub mod mxactdesc;
pub mod nbtdesc;
pub mod relmapdesc;
pub mod replorigindesc;
pub mod rmgrdesc_utils;
pub mod seqdesc;
pub mod smgrdesc;
pub mod spgdesc;
pub mod standbydesc;
pub mod tblspcdesc;
pub mod xactdesc;
pub mod xlogdesc;
