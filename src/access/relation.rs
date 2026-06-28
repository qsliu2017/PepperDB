//! Translated from PostgreSQL src/include/access/relation.h
//!
//! The bodies live in `crate::backend::access::common::relation` and are
//! re-exported here. They are `async` (rules.md s5): `relation_open` /
//! `try_relation_open` / `relation_openrv*` take the heavyweight relation lock,
//! and `LockRelationOid` is a lock-wait leaf, so the open routines are async.
//! `relation_close` stays synchronous (it only releases). `LockMode` (the lock-
//! mode enum) replaces the original `LockMode` param type unchanged.

pub use crate::backend::access::common::relation::{
    relation_close, relation_open, relation_openrv, relation_openrv_extended, try_relation_open,
};
