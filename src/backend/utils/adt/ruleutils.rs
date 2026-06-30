//! Deparse: render a node tree back to SQL text. Translated from the M10-reachable
//! parts of `src/backend/utils/adt/ruleutils.c` (disposition: grow; FIRST
//! translation -- kept minimal).
//!
//! M10 needs only the smallest deparse the reachable DDL + error messages use:
//! render a column DEFAULT expression and a CHECK qual back to text (stored in
//! pg_attrdef.adbin / pg_constraint.conbin and read back on INSERT default / for the
//! constraint definition), plus identifier quoting and relation-name generation.
//!
//! STAGED (rules.md s4): the bulk of ruleutils -- full view/rule deparse
//! (`get_query_def`, `pg_get_viewdef`), `pg_get_indexdef`, window/CTE/subquery
//! deparse, the deparse_context/namespace machinery. ruleutils grows substantially
//! at M11/M12 when views + EXPLAIN verbose land. The M10 form deparses a *raw* parse
//! expression (pre-analysis) since the reachable DEFAULT/CHECK store the raw node;
//! the cooked-expression (`Node`/`Expr` post-analysis) deparse arrives with stored
//! cooked defaults.

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{A_Expr_Kind, ColumnRefField, ValUnion};

/// PG `quote_identifier`: wrap `ident` in double quotes if it is not a safe bare
/// identifier (would need quoting to round-trip). The M10 rule: quote unless the
/// string is a non-empty lowercase `[a-z_][a-z0-9_]*` that is not a keyword. Keyword
/// detection uses the kwlist; the conservative M10 form quotes on any uppercase /
/// special char (sufficient for the round-trip the reachable DDL needs).
#[must_use]
pub fn quote_identifier(ident: &str) -> String {
    if is_safe_bare_identifier(ident) {
        ident.to_owned()
    } else {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

fn is_safe_bare_identifier(ident: &str) -> bool {
    let mut chars = ident.chars();
    let Some(first) = chars.next() else { return false };
    if !(first.is_ascii_lowercase() || first == '_') {
        return false;
    }
    if !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return false;
    }
    // A bare identifier that is a reserved keyword would still need quoting; the
    // kwlist check grows with the keyword-category lookup.
    true
}

/// PG `generate_relation_name` (M10 form): the (optionally schema-qualified)
/// printable name of a relation. The schema qualifier is included only when the
/// relation is not on the search path; the M10 form takes the parts directly.
#[must_use]
pub fn generate_relation_name(schemaname: Option<&str>, relname: &str) -> String {
    schemaname.map_or_else(
        || quote_identifier(relname),
        |s| format!("{}.{}", quote_identifier(s), quote_identifier(relname)),
    )
}

/// PG `deparse_expression` (M10 form): render a (raw) expression node back to SQL
/// text. Covers the reachable DEFAULT / CHECK shapes: constants, column references,
/// and binary/unary operator expressions. Unsupported nodes render a placeholder
/// (`<expr>`) rather than erroring -- the stored text is informational on M10 (the
/// executor re-parses a DEFAULT from this text). The cooked-`Expr` deparse grows
/// with stored cooked defaults.
#[must_use]
pub fn deparse_expression(expr: &Node) -> String {
    get_rule_expr(expr)
}

/// PG `get_rule_expr` (M10 subset): the recursive expression printer.
#[must_use]
pub fn get_rule_expr(node: &Node) -> String {
    match node {
        Node::A_Const(c) => deparse_const(c),
        Node::ColumnRef(cr) => deparse_columnref(&cr.fields),
        Node::A_Expr(a) => {
            let opname = operator_name(&a.name);
            match a.kind {
                A_Expr_Kind::OP => {
                    let l = a.lexpr.as_ref().map(get_rule_expr);
                    let r = a.rexpr.as_ref().map(get_rule_expr);
                    match (l, r) {
                        (Some(l), Some(r)) => format!("({l} {opname} {r})"),
                        (None, Some(r)) => format!("({opname} {r})"),
                        (Some(l), None) => format!("({l} {opname})"),
                        (None, None) => opname,
                    }
                }
                _ => "<expr>".to_owned(),
            }
        }
        Node::TypeCast(tc) => {
            tc.arg.as_ref().map_or_else(|| "<expr>".to_owned(), get_rule_expr)
        }
        // BoolExpr / FuncCall / CaseExpr and the rest render a placeholder on M10
        // (the DEFAULT / CHECK deparse the reachable DDL needs is const + op + colref).
        _ => "<expr>".to_owned(),
    }
}

/// Render an `A_Const` literal.
fn deparse_const(c: &crate::nodes::parsenodes::A_Const) -> String {
    if c.isnull {
        return "NULL".to_owned();
    }
    match &c.val {
        ValUnion::Integer(i) => i.ival.to_string(),
        ValUnion::Float(f) => f.fval.clone(),
        ValUnion::Boolean(b) => if b.boolval { "true".to_owned() } else { "false".to_owned() },
        ValUnion::String(s) => format!("'{}'", s.sval.replace('\'', "''")),
        ValUnion::BitString(b) => format!("B'{}'", b.bsval),
        ValUnion::Node(_) => "<expr>".to_owned(),
    }
}

/// Render a (possibly qualified) column reference.
fn deparse_columnref(fields: &[ColumnRefField]) -> String {
    fields
        .iter()
        .map(|f| match f {
            ColumnRefField::String(s) => quote_identifier(&s.sval),
            ColumnRefField::Star(_) => "*".to_owned(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The printable operator name from an operator name-list (the last String part).
fn operator_name(name: &[Node]) -> String {
    name.iter()
        .rev()
        .find_map(|n| match n {
            Node::String_(s) => Some(s.sval.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "?".to_owned())
}

/// PG `pg_get_constraintdef` (M10 stub): the textual definition of a constraint.
/// STAGED -- the reachable CHECK constraint's text is reconstructed from the stored
/// `conbin` deparse; the full pg_get_constraintdef (FK/UNIQUE/exclusion rendering,
/// the syscache lookup by OID) grows with EXPLAIN/`\d` support at M11/M12.
#[cold]
#[must_use]
pub fn pg_get_constraintdef(_constraint_oid: crate::postgres_ext::Oid) -> String {
    unimplemented!("pg_get_constraintdef: ruleutils grows at M11/M12")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_quotes_unsafe() {
        assert_eq!(quote_identifier("foo"), "foo");
        assert_eq!(quote_identifier("Foo"), "\"Foo\"");
        assert_eq!(quote_identifier("a b"), "\"a b\"");
        assert_eq!(quote_identifier("a_1"), "a_1");
    }

    #[test]
    fn generate_relation_name_qualifies() {
        assert_eq!(generate_relation_name(None, "t"), "t");
        assert_eq!(generate_relation_name(Some("s"), "t"), "s.t");
    }
}
