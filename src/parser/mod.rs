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
    pub targets: Vec<ResTarget>,
    pub from: Option<String>,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug)]
pub enum ResTarget {
    Wildcard,
    Expr(Expr),
}

#[derive(Debug)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
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

#[derive(Debug, Clone)]
pub enum Expr {
    Integer(i64),
    StringLiteral(String),
    Bool(bool),
    ColumnRef(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Minus,
    Not,
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
    let order_by = if let Some(ob) = query.order_by {
        ob.exprs
            .into_iter()
            .map(|e| {
                Ok(OrderByExpr {
                    expr: convert_expr(e.expr)?,
                    asc: e.asc.unwrap_or(true),
                })
            })
            .collect::<PgWireResult<Vec<_>>>()?
    } else {
        vec![]
    };

    match *query.body {
        ast::SetExpr::Select(select) => {
            let targets = select
                .projection
                .into_iter()
                .map(|item| match item {
                    ast::SelectItem::Wildcard(_) => Ok(ResTarget::Wildcard),
                    ast::SelectItem::UnnamedExpr(expr) => {
                        Ok(ResTarget::Expr(convert_expr(expr)?))
                    }
                    _ => Err(unsupported("Unsupported select item")),
                })
                .collect::<PgWireResult<Vec<_>>>()?;

            let from = if !select.from.is_empty() {
                Some(select.from[0].relation.to_string())
            } else {
                None
            };

            let where_clause = select.selection.map(convert_expr).transpose()?;

            Ok(Statement::Select(SelectStmt {
                targets,
                from,
                where_clause,
                order_by,
            }))
        }
        _ => Err(unsupported("Unsupported query type")),
    }
}

fn convert_expr(expr: ast::Expr) -> PgWireResult<Expr> {
    match expr {
        ast::Expr::Value(v) => convert_value(v),
        ast::Expr::Identifier(ident) => Ok(Expr::ColumnRef(ident.value)),
        ast::Expr::UnaryOp { op, expr } => match op {
            ast::UnaryOperator::Minus => match *expr {
                ast::Expr::Value(ast::Value::Number(n, _)) => {
                    let i: i64 = n.parse().map_err(|_| unsupported("Invalid number"))?;
                    Ok(Expr::Integer(-i))
                }
                other => Ok(Expr::UnaryOp {
                    op: UnaryOp::Minus,
                    expr: Box::new(convert_expr(other)?),
                }),
            },
            ast::UnaryOperator::Not => Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(convert_expr(*expr)?),
            }),
            _ => Err(unsupported("Unsupported unary operator")),
        },
        ast::Expr::BinaryOp { left, op, right } => {
            let bin_op = convert_binop(op)?;
            Ok(Expr::BinaryOp {
                left: Box::new(convert_expr(*left)?),
                op: bin_op,
                right: Box::new(convert_expr(*right)?),
            })
        }
        ast::Expr::Nested(inner) => convert_expr(*inner),
        _ => Err(unsupported("Unsupported expression")),
    }
}

fn convert_binop(op: ast::BinaryOperator) -> PgWireResult<BinOp> {
    match op {
        ast::BinaryOperator::Plus => Ok(BinOp::Add),
        ast::BinaryOperator::Minus => Ok(BinOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinOp::Div),
        ast::BinaryOperator::Eq => Ok(BinOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinOp::NotEq),
        ast::BinaryOperator::Lt => Ok(BinOp::Lt),
        ast::BinaryOperator::Gt => Ok(BinOp::Gt),
        ast::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        ast::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        ast::BinaryOperator::And => Ok(BinOp::And),
        ast::BinaryOperator::Or => Ok(BinOp::Or),
        _ => Err(unsupported("Unsupported binary operator")),
    }
}

fn convert_value(value: ast::Value) -> PgWireResult<Expr> {
    match value {
        ast::Value::Number(n, _) => {
            let i: i64 = n.parse().map_err(|_| unsupported("Invalid number"))?;
            Ok(Expr::Integer(i))
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::StringLiteral(s)),
        ast::Value::Boolean(b) => Ok(Expr::Bool(b)),
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
            Statement::Select(s) => {
                assert_eq!(s.targets.len(), 1);
                assert!(matches!(&s.targets[0], ResTarget::Expr(Expr::Integer(1))));
                assert!(s.from.is_none());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_select_string() {
        let stmts = parse("SELECT 'hello';").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.targets.len(), 1);
                match &s.targets[0] {
                    ResTarget::Expr(Expr::StringLiteral(s)) => assert_eq!(s, "hello"),
                    _ => panic!("Expected StringLiteral"),
                }
            }
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
            Statement::Select(s) => {
                assert_eq!(s.targets.len(), 1);
                assert!(matches!(&s.targets[0], ResTarget::Wildcard));
                assert_eq!(s.from.as_deref(), Some("t"));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_select_columns() {
        let stmts = parse("SELECT a, b FROM t;").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.targets.len(), 2);
                assert!(
                    matches!(&s.targets[0], ResTarget::Expr(Expr::ColumnRef(n)) if n == "a")
                );
                assert!(
                    matches!(&s.targets[1], ResTarget::Expr(Expr::ColumnRef(n)) if n == "b")
                );
                assert_eq!(s.from.as_deref(), Some("t"));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_where_clause() {
        let stmts = parse("SELECT * FROM t WHERE a > 5;").unwrap();
        match &stmts[0] {
            Statement::Select(s) => assert!(s.where_clause.is_some()),
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_order_by() {
        let stmts = parse("SELECT * FROM t ORDER BY a DESC;").unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 1);
                assert!(!s.order_by[0].asc);
            }
            _ => panic!("Expected Select"),
        }
    }
}
