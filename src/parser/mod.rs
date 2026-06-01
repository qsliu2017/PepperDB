//! The SQL parser (postgres/src/backend/parser + postgres/src/include/parser).
//!
//! So far: scanner-support helpers. The flex scanner (scan.l) and bison grammar
//! (gram.y) plus the analysis phase (analyze.c, parse_*.c) are future work.

pub mod parse_utilcmd;
pub mod analyze;
pub mod parse_target;
pub mod parse_agg;
pub mod parse_func;
pub mod parse_coerce;
pub mod parse_clause;
pub mod parse_expr;
pub mod parse_relation;
pub mod parse_cte;
pub mod kwlist;
pub mod parse_enr;
pub mod parse_merge;
pub mod parse_node;
pub mod parse_param;
pub mod parsetree;
pub mod scanner;
pub mod scansup;
pub mod parser;
pub mod parse_type;
pub mod parse_oper;
pub mod parse_collate;
pub mod parse_jsontable;
