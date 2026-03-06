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
    Update(UpdateStmt),
    Delete(DeleteStmt),
    DropTable(DropTableStmt),
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
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug)]
pub struct DropTableStmt {
    pub table_name: String,
    pub if_exists: bool,
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
    Float(f64),
    StringLiteral(String),
    Bool(bool),
    Null,
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
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        type_id: TypeId,
    },
    Coalesce(Vec<Expr>),
    NullIf(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    Concat,
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
        ast::Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => convert_update(table, assignments, selection),
        ast::Statement::Delete(del) => convert_delete(del),
        ast::Statement::Drop {
            object_type: ast::ObjectType::Table,
            if_exists,
            names,
            ..
        } => {
            let table_name = names
                .into_iter()
                .next()
                .ok_or_else(|| unsupported("DROP TABLE requires a name"))?
                .to_string();
            Ok(Statement::DropTable(DropTableStmt {
                table_name,
                if_exists,
            }))
        }
        _ => Err(unsupported("Unsupported statement")),
    }
}

fn convert_create_table(ct: ast::CreateTable) -> PgWireResult<Statement> {
    let table_name = ct.name.to_string();
    let if_not_exists = ct.if_not_exists;
    let columns: PgWireResult<Vec<ColumnDef>> = ct
        .columns
        .into_iter()
        .map(|col| {
            let type_id = convert_data_type(&col.data_type)?;
            Ok(ColumnDef {
                name: col.name.value,
                type_id,
            })
        })
        .collect();
    Ok(Statement::CreateTable(CreateTableStmt {
        table_name,
        columns: columns?,
        if_not_exists,
    }))
}

fn convert_update(
    table: ast::TableWithJoins,
    assignments: Vec<ast::Assignment>,
    selection: Option<ast::Expr>,
) -> PgWireResult<Statement> {
    let table_name = table.relation.to_string();
    let assigns = assignments
        .into_iter()
        .map(|a| {
            let col_name = match a.target {
                ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                _ => return Err(unsupported("Unsupported assignment target")),
            };
            let expr = convert_expr(a.value)?;
            Ok((col_name, expr))
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let where_clause = selection.map(convert_expr).transpose()?;
    Ok(Statement::Update(UpdateStmt {
        table_name,
        assignments: assigns,
        where_clause,
    }))
}

fn convert_delete(del: ast::Delete) -> PgWireResult<Statement> {
    let table_name = match del.from {
        ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => {
            tables
                .into_iter()
                .next()
                .ok_or_else(|| unsupported("DELETE requires FROM"))?
                .relation
                .to_string()
        }
    };
    let where_clause = del.selection.map(convert_expr).transpose()?;
    Ok(Statement::Delete(DeleteStmt {
        table_name,
        where_clause,
    }))
}

fn convert_data_type(dt: &ast::DataType) -> PgWireResult<TypeId> {
    match dt {
        ast::DataType::Boolean => Ok(TypeId::Bool),
        ast::DataType::SmallInt(_) | ast::DataType::Int2(_) => Ok(TypeId::Int2),
        ast::DataType::Int(_) | ast::DataType::Int4(_) | ast::DataType::Integer(_) => {
            Ok(TypeId::Int4)
        }
        ast::DataType::BigInt(_) | ast::DataType::Int8(_) => Ok(TypeId::Int8),
        ast::DataType::Real | ast::DataType::Float4 => Ok(TypeId::Float4),
        ast::DataType::DoublePrecision | ast::DataType::Float8 => Ok(TypeId::Float8),
        ast::DataType::Text
        | ast::DataType::Varchar(_)
        | ast::DataType::CharVarying(_)
        | ast::DataType::Char(_)
        | ast::DataType::Character(_) => Ok(TypeId::Text),
        other => Err(unsupported(&format!("Unsupported type: {}", other))),
    }
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
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Expr::Integer(-i))
                    } else {
                        let f: f64 = n.parse().map_err(|_| unsupported("Invalid number"))?;
                        Ok(Expr::Float(-f))
                    }
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
        ast::Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(convert_expr(*inner)?))),
        ast::Expr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(convert_expr(*inner)?))),
        ast::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand = operand.map(|e| convert_expr(*e)).transpose()?.map(Box::new);
            let conds = conditions
                .into_iter()
                .map(convert_expr)
                .collect::<PgWireResult<Vec<_>>>()?;
            let res = results
                .into_iter()
                .map(convert_expr)
                .collect::<PgWireResult<Vec<_>>>()?;
            let else_r = else_result
                .map(|e| convert_expr(*e))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case {
                operand,
                conditions: conds,
                results: res,
                else_result: else_r,
            })
        }
        ast::Expr::Cast {
            expr, data_type, ..
        } => {
            let type_id = convert_data_type(&data_type)?;
            Ok(Expr::Cast {
                expr: Box::new(convert_expr(*expr)?),
                type_id,
            })
        }
        ast::Expr::Function(func) => convert_function(func),
        _ => Err(unsupported("Unsupported expression")),
    }
}

fn convert_function(func: ast::Function) -> PgWireResult<Expr> {
    let name = func.name.to_string().to_uppercase();
    let args = match func.args {
        ast::FunctionArguments::List(list) => list
            .args
            .into_iter()
            .map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => convert_expr(e),
                _ => Err(unsupported("Unsupported function argument")),
            })
            .collect::<PgWireResult<Vec<_>>>()?,
        _ => vec![],
    };

    match name.as_str() {
        "COALESCE" => Ok(Expr::Coalesce(args)),
        "NULLIF" => {
            if args.len() != 2 {
                return Err(unsupported("NULLIF requires exactly 2 arguments"));
            }
            let mut it = args.into_iter();
            Ok(Expr::NullIf(
                Box::new(it.next().unwrap()),
                Box::new(it.next().unwrap()),
            ))
        }
        _ => Err(unsupported(&format!("Unsupported function: {}", name))),
    }
}

fn convert_binop(op: ast::BinaryOperator) -> PgWireResult<BinOp> {
    match op {
        ast::BinaryOperator::Plus => Ok(BinOp::Add),
        ast::BinaryOperator::Minus => Ok(BinOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinOp::Div),
        ast::BinaryOperator::Modulo => Ok(BinOp::Mod),
        ast::BinaryOperator::Eq => Ok(BinOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinOp::NotEq),
        ast::BinaryOperator::Lt => Ok(BinOp::Lt),
        ast::BinaryOperator::Gt => Ok(BinOp::Gt),
        ast::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        ast::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        ast::BinaryOperator::And => Ok(BinOp::And),
        ast::BinaryOperator::Or => Ok(BinOp::Or),
        ast::BinaryOperator::StringConcat => Ok(BinOp::Concat),
        _ => Err(unsupported("Unsupported binary operator")),
    }
}

fn convert_value(value: ast::Value) -> PgWireResult<Expr> {
    match value {
        ast::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Expr::Integer(i))
            } else {
                let f: f64 = n.parse().map_err(|_| unsupported("Invalid number"))?;
                Ok(Expr::Float(f))
            }
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::StringLiteral(s)),
        ast::Value::Boolean(b) => Ok(Expr::Bool(b)),
        ast::Value::Null => Ok(Expr::Null),
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
    fn parse_select_float() {
        let stmts = parse("SELECT 3.14;").unwrap();
        match &stmts[0] {
            Statement::Select(s) => match &s.targets[0] {
                ResTarget::Expr(Expr::Float(f)) => assert!((*f - 3.14).abs() < 1e-10),
                other => panic!("Expected Float, got {:?}", other),
            },
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn parse_select_null() {
        let stmts = parse("SELECT NULL;").unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert!(matches!(&s.targets[0], ResTarget::Expr(Expr::Null)));
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
    fn parse_create_table_types() {
        let stmts = parse(
            "CREATE TABLE t (a boolean, b smallint, c bigint, d real, e double precision, f text, g varchar);",
        )
        .unwrap();
        match &stmts[0] {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].type_id, TypeId::Bool);
                assert_eq!(ct.columns[1].type_id, TypeId::Int2);
                assert_eq!(ct.columns[2].type_id, TypeId::Int8);
                assert_eq!(ct.columns[3].type_id, TypeId::Float4);
                assert_eq!(ct.columns[4].type_id, TypeId::Float8);
                assert_eq!(ct.columns[5].type_id, TypeId::Text);
                assert_eq!(ct.columns[6].type_id, TypeId::Text);
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

    #[test]
    fn parse_is_null() {
        let stmts = parse("SELECT * FROM t WHERE a IS NULL;").unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert!(matches!(s.where_clause, Some(Expr::IsNull(_))));
            }
            _ => panic!("Expected Select"),
        }
    }
}
