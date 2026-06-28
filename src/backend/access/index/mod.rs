//! Directory module: src/backend/access/index
//!
//! Generic index access (indexam/genam/amapi/amvalidate) bodies. Headers live
//! under `src/access/` and re-export these via `pub use`.

pub mod genam;
pub mod amapi;
pub mod indexam;
