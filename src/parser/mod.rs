// SQL parser: converts sqlparser-rs AST to PepperDB AST for DDL/DML.
// SELECT queries bypass this module and go directly to DataFusion.

use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::ast;

use crate::types::TypeId;

#[derive(Debug)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    DropTable(DropTableStmt),
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

// Expression types kept for UPDATE SET / WHERE evaluation
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
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Minus,
    Not,
}

/// Convert a sqlparser statement to our AST. Called by the executor for
/// non-SELECT statements. SELECT goes to DataFusion directly.
pub fn convert_statement(stmt: ast::Statement) -> PgWireResult<Statement> {
    match stmt {
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
        _ => Err(unsupported("Unsupported expression")),
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
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        let dialect = PostgreSqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        convert_statement(stmts.remove(0)).unwrap()
    }

    #[test]
    fn parse_create_table() {
        match parse_one("CREATE TABLE foo (id INT, val INT4);") {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "foo");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].type_id, TypeId::Int4);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_types() {
        match parse_one(
            "CREATE TABLE t (a boolean, b smallint, c bigint, d real, e double precision, f text, g varchar);",
        ) {
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
        match parse_one("INSERT INTO t VALUES (1, 2), (3, 4);") {
            Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "t");
                assert_eq!(ins.values.len(), 2);
                assert_eq!(ins.values[0].len(), 2);
            }
            _ => panic!("Expected Insert"),
        }
    }
}
