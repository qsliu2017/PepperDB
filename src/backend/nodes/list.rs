//! Tombstone: backend/nodes/list.c
//!
//! PostgreSQL's `List`/`ListCell` (an expansible array that is itself a `Node`, in
//! the `T_List`/`T_IntList`/`T_OidList`/`T_XidList` flavors) is replaced wholesale
//! by Rust's `Vec<T>` per the container mapping (see `crate::nodes::pg_list`).
//! There is no `List` type and therefore no `list.c` machinery to translate:
//!
//!   * construction (`list_make1`/`lappend`/`lcons`/`list_concat`) -> `vec![..]`,
//!     `Vec::push`, `Vec::insert(0, ..)`, `Vec::extend`;
//!   * access (`linitial`/`lnth`/`llast`/`list_length`/`foreach`) -> indexing,
//!     `.len()`, `.iter()`;
//!   * the set helpers (`list_member`/`list_union`/`list_difference`/
//!     `list_delete_ptr`/`list_concat_unique`/...) -> iterator combinators or
//!     small free functions introduced at the call site that needs them.
//!
//! `Vec`'s growth strategy and `Drop`-based freeing subsume the C allocator
//! mechanics (`enlarge_list`, the `ListCell` arena, `list_free`/`list_free_deep`).
//! As later milestones reach a call site that used a PG-specific set helper with
//! no direct `Vec`/iterator equivalent, the helper is added here as a `grow`
//! free function. None is reachable for M1.
