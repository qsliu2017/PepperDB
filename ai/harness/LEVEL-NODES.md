# Node-system translation addendum (for agents translating node-defining headers)

PostgreSQL nodes (parse/plan/exec/primitive nodes) carry a leading `NodeTag type`
and are passed around as `Node *`, recovering the concrete type from the tag. In
this port there is NO `NodeTag`: `crate::nodes::nodes::Node` is ONE enum whose
discriminant IS the tag, and each concrete node is a variant carrying its struct.

## What each node-defining module does

1. Define every concrete node as a struct WITHOUT the `NodeTag type` header field:
   `#[derive(Debug, Clone, PartialEq)] pub struct Var { ... }`.
   (A node is a C struct whose first field is `NodeTag type` / has `pg_node_attr`.)
2. A C type that is a plain `enum` (e.g. `BoolExprType`, `CoercionForm`,
   `OnCommitAction`) is a normal Rust `enum`, NOT a Node variant.
3. The abstract bases `Node` and `Expr` are NOT redefined as structs here:
   - a `Node *` field  -> `Option<Box<Node>>` (nullable) or `Box<Node>` (non-null)
   - an `Expr *` field  -> same (Expr is a subset of Node): `Option<Box<Node>>`
   - `Expr` as an embedded first field of a node (the `Expr xpr;` common header)
     -> DROP it (it only carried the tag).

## Field type mapping (skeleton)

| C field | Rust |
|---|---|
| `Node *` / `Expr *` (nullable) | `Option<Box<Node>>` |
| `Node *` / `Expr *` (always set) | `Box<Node>` |
| `List *` of node ptrs | `Vec<Box<Node>>` (NIL -> empty) |
| `List *` of Oid / int | `Vec<Oid>` / `Vec<i32>` |
| `Bitmapset *` | `crate::nodes::bitmapset::Bitmapset` (Option if nullable) |
| `Datum` | `crate::postgres::Datum` |
| `Oid` | `crate::postgres_ext::Oid` |
| `char *` | `Option<String>` |
| `ParseLoc` / `int location` | `crate::nodes::nodes::ParseLoc` (i32) |
| `bool` / `int` / `int16` ... | `bool` / `i32` / `i16` ... |

Concrete sub-structs that are NOT themselves nodes (no tag) stay plain structs and
are used by value (not `Box<Node>`).

## Registering variants in `enum Node` (ONLY the designated node agent edits nodes.rs)

In `src/nodes/nodes.rs`, ensure the enum and (later) its derives:
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    // one line per concrete node type, payload = its struct (boxed):
    Var(Box<crate::nodes::primnodes::Var>),
    Const(Box<crate::nodes::primnodes::Const>),
    // ...
}
```
Add one variant per concrete node struct you defined, named after the C node
(drop any `Data` suffix). Box the payload (nodes nest, so the enum must stay sized).
Keep variants grouped by source module with a `// from nodes/<module>.rs` comment.
Do NOT remove existing items in nodes.rs (CmdType, JoinType, the value re-exports,
etc.). Use `#[derive(Debug, Clone, PartialEq)]` (NOT Eq - cost/selectivity f64s).

If two modules define a same-named node, qualify the variant name; otherwise match
the C name exactly.
