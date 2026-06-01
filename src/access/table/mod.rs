//! Generic table access-method support (postgres/src/backend/access/table).
//!
//! So far: the table AM API descriptor plumbing (`tableamapi`) and the
//! relation-open convenience wrappers (`table`).

pub mod table;
pub mod tableamapi;
pub mod toast_helper;
pub mod tableam;
