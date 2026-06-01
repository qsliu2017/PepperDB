//! Generic index access-method support (postgres/src/backend/access/index).
//!
//! So far: the index AM API descriptor (`amapi`) and the shared opclass
//! validation helpers (`amvalidate`).

pub mod amapi;
pub mod amvalidate;
pub mod genam;
pub mod indexam;
