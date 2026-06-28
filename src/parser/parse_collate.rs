//! Translated from PostgreSQL src/include/parser/parse_collate.h

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

/// PG `assign_query_collations`. See `crate::backend::parser::parse_collate`.
pub use crate::backend::parser::parse_collate::assign_query_collations;

/// PG `assign_list_collations`. See `crate::backend::parser::parse_collate`.
pub use crate::backend::parser::parse_collate::assign_list_collations;

/// PG `assign_expr_collations`. See `crate::backend::parser::parse_collate`.
pub use crate::backend::parser::parse_collate::assign_expr_collations;

/// PG `select_common_collation`. See `crate::backend::parser::parse_collate`.
pub use crate::backend::parser::parse_collate::select_common_collation;
