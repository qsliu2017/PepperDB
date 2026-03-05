use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub enum Statement {
    Select(Vec<Expr>),
}

#[derive(Debug)]
pub enum Expr {
    Integer(i64),
    StringLiteral(String),
}

pub fn parse(sql: &str) -> PgWireResult<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            e.to_string(),
        )))
    })?;

    statements.into_iter().map(convert_statement).collect()
}

fn convert_statement(stmt: ast::Statement) -> PgWireResult<Statement> {
    match stmt {
        ast::Statement::Query(query) => convert_query(*query),
        _ => Err(PgWireError::UserError(Box::new(
            pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "0A000".to_owned(),
                "Unsupported statement".to_owned(),
            ),
        ))),
    }
}

fn convert_query(query: ast::Query) -> PgWireResult<Statement> {
    match *query.body {
        ast::SetExpr::Select(select) => {
            let exprs: PgWireResult<Vec<Expr>> = select
                .projection
                .into_iter()
                .map(|item| match item {
                    ast::SelectItem::UnnamedExpr(expr) => convert_expr(expr),
                    _ => Err(unsupported("Unsupported select item")),
                })
                .collect();
            Ok(Statement::Select(exprs?))
        }
        _ => Err(unsupported("Unsupported query type")),
    }
}

fn convert_expr(expr: ast::Expr) -> PgWireResult<Expr> {
    match expr {
        ast::Expr::Value(v) => convert_value(v),
        _ => Err(unsupported("Unsupported expression")),
    }
}

fn convert_value(value: ast::Value) -> PgWireResult<Expr> {
    match value {
        ast::Value::Number(n, _) => {
            let i: i64 = n.parse().map_err(|_| unsupported("Invalid number"))?;
            Ok(Expr::Integer(i))
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::StringLiteral(s)),
        _ => Err(unsupported("Unsupported value type")),
    }
}

fn unsupported(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        msg.to_owned(),
    )))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_select_integer() {
        let stmts = parse("SELECT 1;").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(exprs) => {
                assert_eq!(exprs.len(), 1);
                assert!(matches!(exprs[0], Expr::Integer(1)));
            }
        }
    }

    #[test]
    fn parse_select_string() {
        let stmts = parse("SELECT 'hello';").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(exprs) => {
                assert_eq!(exprs.len(), 1);
                match &exprs[0] {
                    Expr::StringLiteral(s) => assert_eq!(s, "hello"),
                    _ => panic!("Expected StringLiteral"),
                }
            }
        }
    }

    #[test]
    fn parse_error_on_unsupported() {
        assert!(parse("CREATE TABLE foo (id INT);").is_err());
    }
}
