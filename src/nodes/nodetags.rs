//! Translated from PostgreSQL src/include/nodes/nodetags.h
//!
//! RESOLVED-BY-DESIGN (not a build artifact here): in C, `nodetags.h` is emitted
//! by `gen_node_support.pl` as the `NodeTag` enum. In this port a node's tag IS
//! the discriminant of `crate::nodes::nodes::Node` (a real Rust tagged enum), so
//! there is no separate `NodeTag` type or generated tag list - `IsA`/`nodeTag`
//! become a `match` on `Node`. copy/equal are `#[derive(Clone, PartialEq)]` on the
//! node structs; the textual out/read serializer is a future `#[derive(Node)]`.
