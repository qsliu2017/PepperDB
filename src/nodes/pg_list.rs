//! Tombstone: src/include/nodes/pg_list.h
//!
//! PostgreSQL's `List`/`ListCell` (an expansible array that is itself a Node, in
//! four flavors T_List/T_IntList/T_OidList/T_XidList) is replaced by Rust's
//! `Vec<T>` per the translation-rules container table, with the element type
//! chosen per use site:
//!   - `List *` of nodes  -> `Vec<Box<Node>>` (or `Vec<Node>` once Node is sized)
//!   - `IntList`          -> `Vec<i32>`
//!   - `OidList`          -> `Vec<Oid>`
//!   - `XidList`          -> `Vec<TransactionId>`
//!
//! Idiom mapping: `NIL` = empty `Vec`; `foreach` = `.iter()`; `lappend` = `push`;
//! `list_length` = `.len()`; `linitial`/`lnth` = indexing; `list_concat` =
//! `extend`. The PG-specific set helpers (`list_union`/`list_difference`/
//! `list_member`/`list_delete_ptr`/...) become iterator combinators or small free
//! functions introduced at the call sites that need them. There is no `List` type.
//!
//! The `list.c` body is correspondingly a tombstone (see
//! `crate::backend::nodes::list`): nothing to re-export here.
