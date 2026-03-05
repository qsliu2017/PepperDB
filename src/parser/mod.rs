// SQL parser: converts SQL text into our AST via sqlparser-rs.

use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::types::TypeId;

#[derive(Debug)]
pub enum Statement {
    Select(SelectStmt),
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
}

#[derive(Debug)]
pub struct SelectStmt {
    pub target: SelectTarget,
}

#[derive(Debug)]
pub enum SelectTarget {
    Expressions(Vec<Expr>),
    FromTable { table_name: String },
}

#[derive(Debug)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub type_id: TypeId,
}

#[derive(Debug)]
pub struct InsertStmt {
    pub table_name: String,
    pub values: Vec<Vec<Expr>>,
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
        ast::Statement::CreateTable(ct) => convert_create_table(ct),
        ast::Statement::Insert(ins) => convert_insert(ins),
        _ => Err(unsupported("Unsupported statement")),
    }
}

fn convert_create_table(ct: ast::CreateTable) -> PgWireResult<Statement> {
    let table_name = ct.name.to_string();
    let columns: PgWireResult<Vec<ColumnDef>> = ct
        .columns
        .into_iter()
        .map(|col| {
            let type_id = match &col.data_type {
                ast::DataType::Int(_)
                | ast::DataType::Int4(_)
                | ast::DataType::Integer(_) => TypeId::Int4,
                other => return Err(unsupported(&format!("Unsupported type: {}", other))),
            };
            Ok(ColumnDef {
                name: col.name.value,
                type_id,
            })
        })
        .collect();
    Ok(Statement::CreateTable(CreateTableStmt {
        table_name,
        columns: columns?,
    }))
}

fn convert_insert(ins: ast::Insert) -> PgWireResult<Statement> {
    let table_name = ins.table_name.to_string();
    let source = ins.source.ok_or_else(|| unsupported("INSERT without VALUES"))?;
    match *source.body {
        ast::SetExpr::Values(ast::Values { rows, .. }) => {
            let values: PgWireResult<Vec<Vec<Expr>>> = rows
                .into_iter()
                .map(|row| row.into_iter().map(convert_expr).collect())
                .collect();
            Ok(Statement::Insert(InsertStmt {
                table_name,
                values: values?,
            }))
        }
        _ => Err(unsupported("Unsupported INSERT source")),
    }
}

fn convert_query(query: ast::Query) -> PgWireResult<Statement> {
    match *query.body {
        ast::SetExpr::Select(select) => {
            // Check for SELECT * FROM table
            let has_wildcard = select.projection.len() == 1
                && matches!(select.projection[0], ast::SelectItem::Wildcard(_));
            if has_wildcard && !select.from.is_empty() {
                let table_name = select.from[0].relation.to_string();
                return Ok(Statement::Select(SelectStmt {
                    target: SelectTarget::FromTable { table_name },
                }));
            }

            // Existing literal expression path
            let exprs: PgWireResult<Vec<Expr>> = select
                .projection
                .into_iter()
                .map(|item| match item {
                    ast::SelectItem::UnnamedExpr(expr) => convert_expr(expr),
                    _ => Err(unsupported("Unsupported select item")),
                })
                .collect();
            Ok(Statement::Select(SelectStmt {
                target: SelectTarget::Expressions(exprs?),
            }))
        }
        _ => Err(unsupported("Unsupported query type")),
    }
}

fn convert_expr(expr: ast::Expr) -> PgWireResult<Expr> {
    match expr {
        ast::Expr::Value(v) => convert_value(v),
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => match *expr {
            ast::Expr::Value(ast::Value::Number(n, _)) => {
                let i: i64 = n.parse().map_err(|_| unsupported("Invalid number"))?;
                Ok(Expr::Integer(-i))
            }
            _ => Err(unsupported("Unsupported unary expression")),
        },
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
            Statement::Select(s) => match &s.target {
                SelectTarget::Expressions(exprs) => {
                    assert_eq!(exprs.len(), 1);
                    assert!(matches!(exprs[0], Expr::Integer(1)));
                }
                _ => panic!("Expected Expressions"),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_select_string() {
        let stmts = parse("SELECT 'hello';").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(s) => match &s.target {
                SelectTarget::Expressions(exprs) => {
                    assert_eq!(exprs.len(), 1);
                    match &exprs[0] {
                        Expr::StringLiteral(s) => assert_eq!(s, "hello"),
                        _ => panic!("Expected StringLiteral"),
                    }
                }
                _ => panic!("Expected Expressions"),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_create_table() {
        let stmts = parse("CREATE TABLE foo (id INT, val INT4);").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "foo");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].type_id, TypeId::Int4);
                assert_eq!(ct.columns[1].name, "val");
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn parse_insert() {
        let stmts = parse("INSERT INTO t VALUES (1, 2), (3, 4);").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "t");
                assert_eq!(ins.values.len(), 2);
                assert_eq!(ins.values[0].len(), 2);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn parse_select_star_from() {
        let stmts = parse("SELECT * FROM t;").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(s) => match &s.target {
                SelectTarget::FromTable { table_name } => assert_eq!(table_name, "t"),
                _ => panic!("Expected FromTable"),
            },
            _ => panic!("Expected Select"),
        }
    }
}
