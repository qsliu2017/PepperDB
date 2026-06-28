//! Translated from PostgreSQL src/include/parser/parse_expr.h

/// GUC parameter.
pub static mut Transform_null_equals: bool = false;

/// PG `transformExpr`. See `crate::backend::parser::parse_expr`.
pub use crate::backend::parser::parse_expr::transformExpr;

/// PG `ParseExprKindName`. See `crate::backend::parser::parse_expr`.
pub use crate::backend::parser::parse_expr::ParseExprKindName;
