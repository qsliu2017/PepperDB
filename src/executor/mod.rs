// Query executor: runs parsed statements against the Database.

use std::cmp::Ordering;
use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};

use crate::catalog::Column;
use crate::parser::{BinOp, Expr, ResTarget, Statement, UnaryOp};
use crate::storage::disk::PAGE_SIZE;
use crate::storage::heap;
use crate::types::{Datum, TypeId};
use crate::Database;

pub fn execute(stmt: Statement, db: &Database) -> PgWireResult<Response<'static>> {
    match stmt {
        Statement::Select(s) => execute_select(db, s),
        Statement::CreateTable(ct) => execute_create_table(db, ct),
        Statement::Insert(ins) => execute_insert(db, ins),
    }
}

// -- SELECT ----------------------------------------------------------------

struct ResolvedTarget {
    name: String,
    expr: Expr,
    pg_type: Type,
}

fn execute_select(
    db: &Database,
    sel: crate::parser::SelectStmt,
) -> PgWireResult<Response<'static>> {
    let table_info = if let Some(ref table_name) = sel.from {
        let catalog = db.catalog.lock().unwrap();
        let table = catalog.get_table(table_name).ok_or_else(|| {
            user_error(
                "42P01",
                &format!("relation \"{}\" does not exist", table_name),
            )
        })?;
        Some((table.oid, table.columns.clone()))
    } else {
        None
    };

    let columns: &[Column] = match table_info {
        Some((_, ref cols)) => cols,
        None => &[],
    };

    let resolved = resolve_targets(&sel.targets, columns)?;

    // Scan rows (or single virtual row if no FROM)
    let mut tuples = if let Some((oid, _)) = table_info {
        scan_table(db, oid, columns)?
    } else {
        vec![vec![]]
    };

    // WHERE
    if let Some(ref wh) = sel.where_clause {
        tuples.retain(|row| matches!(eval_expr(wh, row, columns), Datum::Bool(true)));
    }

    // ORDER BY
    if !sel.order_by.is_empty() {
        tuples.sort_by(|a, b| {
            for ob in &sel.order_by {
                let va = eval_expr(&ob.expr, a, columns);
                let vb = eval_expr(&ob.expr, b, columns);
                let c = compare_datums(&va, &vb);
                let c = if ob.asc { c } else { c.reverse() };
                if c != Ordering::Equal {
                    return c;
                }
            }
            Ordering::Equal
        });
    }

    // Build schema
    let fields: Vec<FieldInfo> = resolved
        .iter()
        .map(|rt| {
            FieldInfo::new(
                rt.name.clone(),
                None,
                None,
                rt.pg_type.clone(),
                FieldFormat::Text,
            )
        })
        .collect();
    let schema = Arc::new(fields);

    // Encode rows
    let mut rows = Vec::new();
    for tuple in &tuples {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for rt in &resolved {
            encode_datum(&mut encoder, &eval_expr(&rt.expr, tuple, columns))?;
        }
        rows.push(encoder.finish());
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
}

fn resolve_targets(
    targets: &[ResTarget],
    columns: &[Column],
) -> PgWireResult<Vec<ResolvedTarget>> {
    let mut resolved = Vec::new();
    for target in targets {
        match target {
            ResTarget::Wildcard => {
                for col in columns {
                    resolved.push(ResolvedTarget {
                        name: col.name.clone(),
                        expr: Expr::ColumnRef(col.name.clone()),
                        pg_type: type_id_to_pg(col.type_id),
                    });
                }
            }
            ResTarget::Expr(expr) => {
                resolved.push(ResolvedTarget {
                    name: expr_name(expr),
                    expr: expr.clone(),
                    pg_type: infer_type(expr, columns)?,
                });
            }
        }
    }
    Ok(resolved)
}

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef(name) => name.clone(),
        _ => "?column?".to_owned(),
    }
}

fn infer_type(expr: &Expr, columns: &[Column]) -> PgWireResult<Type> {
    match expr {
        Expr::Integer(_) => Ok(Type::INT4),
        Expr::StringLiteral(_) => Ok(Type::TEXT),
        Expr::Bool(_) => Ok(Type::BOOL),
        Expr::ColumnRef(name) => {
            let col = columns.iter().find(|c| c.name == *name).ok_or_else(|| {
                user_error("42703", &format!("column \"{}\" does not exist", name))
            })?;
            Ok(type_id_to_pg(col.type_id))
        }
        Expr::BinaryOp { op, left, .. } => match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => infer_type(left, columns),
            _ => Ok(Type::BOOL),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOp::Minus => infer_type(expr, columns),
            UnaryOp::Not => Ok(Type::BOOL),
        },
    }
}

fn type_id_to_pg(type_id: TypeId) -> Type {
    match type_id {
        TypeId::Int4 => Type::INT4,
    }
}

// -- Expression evaluation ------------------------------------------------

fn eval_expr(expr: &Expr, row: &[Datum], columns: &[Column]) -> Datum {
    match expr {
        Expr::Integer(i) => Datum::Int4(*i as i32),
        Expr::StringLiteral(s) => Datum::Text(s.clone()),
        Expr::Bool(b) => Datum::Bool(*b),
        Expr::ColumnRef(name) => {
            for (i, col) in columns.iter().enumerate() {
                if col.name == *name {
                    return row[i].clone();
                }
            }
            Datum::Null
        }
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, row, columns);
            let r = eval_expr(right, row, columns);
            eval_binop(*op, &l, &r)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, row, columns);
            match op {
                UnaryOp::Minus => match v {
                    Datum::Int4(i) => Datum::Int4(-i),
                    _ => Datum::Null,
                },
                UnaryOp::Not => match v {
                    Datum::Bool(b) => Datum::Bool(!b),
                    _ => Datum::Null,
                },
            }
        }
    }
}

fn eval_binop(op: BinOp, left: &Datum, right: &Datum) -> Datum {
    match (left, right) {
        (Datum::Int4(l), Datum::Int4(r)) => match op {
            BinOp::Add => Datum::Int4(l.wrapping_add(*r)),
            BinOp::Sub => Datum::Int4(l.wrapping_sub(*r)),
            BinOp::Mul => Datum::Int4(l.wrapping_mul(*r)),
            BinOp::Div => {
                if *r == 0 {
                    return Datum::Null;
                }
                Datum::Int4(l / r)
            }
            BinOp::Eq => Datum::Bool(l == r),
            BinOp::NotEq => Datum::Bool(l != r),
            BinOp::Lt => Datum::Bool(l < r),
            BinOp::Gt => Datum::Bool(l > r),
            BinOp::LtEq => Datum::Bool(l <= r),
            BinOp::GtEq => Datum::Bool(l >= r),
            _ => Datum::Null,
        },
        (Datum::Bool(l), Datum::Bool(r)) => match op {
            BinOp::And => Datum::Bool(*l && *r),
            BinOp::Or => Datum::Bool(*l || *r),
            BinOp::Eq => Datum::Bool(l == r),
            BinOp::NotEq => Datum::Bool(l != r),
            _ => Datum::Null,
        },
        _ => Datum::Null,
    }
}

fn compare_datums(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (Datum::Int4(a), Datum::Int4(b)) => a.cmp(b),
        (Datum::Bool(a), Datum::Bool(b)) => a.cmp(b),
        (Datum::Text(a), Datum::Text(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn encode_datum(encoder: &mut DataRowEncoder, datum: &Datum) -> PgWireResult<()> {
    match datum {
        Datum::Int4(i) => encoder.encode_field(i),
        Datum::Bool(b) => encoder.encode_field(b),
        Datum::Text(s) => encoder.encode_field(s),
        Datum::Null => encoder.encode_field(&None::<i32>),
    }
}

fn scan_table(db: &Database, oid: u32, columns: &[Column]) -> PgWireResult<Vec<Vec<Datum>>> {
    let disk = db.disk.lock().unwrap();
    let num_pages = disk.num_pages(oid);
    let mut tuples = Vec::new();
    let mut page = [0u8; PAGE_SIZE];

    for page_id in 0..num_pages {
        disk.read_page(oid, page_id, &mut page);
        let n = heap::num_items(&page);
        for item_idx in 0..n {
            if let Some(data) = heap::get_tuple(&page, item_idx) {
                tuples.push(heap::deserialize_tuple(data, columns));
            }
        }
    }
    Ok(tuples)
}

// -- CREATE TABLE ---------------------------------------------------------

fn execute_create_table(
    db: &Database,
    ct: crate::parser::CreateTableStmt,
) -> PgWireResult<Response<'static>> {
    let columns: Vec<Column> = ct
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| Column {
            name: c.name.clone(),
            type_id: c.type_id,
            col_num: i as u16,
        })
        .collect();

    let oid = {
        let mut catalog = db.catalog.lock().unwrap();
        catalog.create_table(&ct.table_name, columns).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "42P07".to_owned(),
                e,
            )))
        })?
    };

    {
        let disk = db.disk.lock().unwrap();
        disk.create_heap_file(oid);
    }

    Ok(Response::Execution(Tag::new("CREATE TABLE")))
}

// -- INSERT ---------------------------------------------------------------

fn execute_insert(
    db: &Database,
    ins: crate::parser::InsertStmt,
) -> PgWireResult<Response<'static>> {
    let (oid, columns) = {
        let catalog = db.catalog.lock().unwrap();
        let table = catalog.get_table(&ins.table_name).ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("relation \"{}\" does not exist", ins.table_name),
            )))
        })?;
        (table.oid, table.columns.clone())
    };

    let mut count = 0u64;
    let disk = db.disk.lock().unwrap();

    for row_exprs in &ins.values {
        if row_exprs.len() != columns.len() {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!(
                        "INSERT has {} expressions but table has {} columns",
                        row_exprs.len(),
                        columns.len()
                    ),
                ),
            )));
        }

        let datums: Vec<Datum> = row_exprs
            .iter()
            .zip(columns.iter())
            .map(|(expr, col)| expr_to_datum(expr, col.type_id))
            .collect::<PgWireResult<Vec<_>>>()?;

        let tuple_data = heap::serialize_tuple(&datums);

        let num_pages = disk.num_pages(oid);
        let mut page = [0u8; PAGE_SIZE];

        if num_pages == 0 {
            heap::init_page(&mut page);
            if heap::insert_tuple(&mut page, &tuple_data).is_err() {
                return Err(user_error("42000", "Tuple too large for page"));
            }
            disk.write_page(oid, 0, &page);
        } else {
            let last_page_id = num_pages - 1;
            disk.read_page(oid, last_page_id, &mut page);
            if heap::insert_tuple(&mut page, &tuple_data).is_ok() {
                disk.write_page(oid, last_page_id, &page);
            } else {
                // Last page full, allocate new page
                let new_page_id = num_pages;
                heap::init_page(&mut page);
                if heap::insert_tuple(&mut page, &tuple_data).is_err() {
                    return Err(user_error("42000", "Tuple too large for page"));
                }
                disk.write_page(oid, new_page_id, &page);
            }
        }
        count += 1;
    }

    Ok(Response::Execution(Tag::new(&format!("INSERT 0 {}", count))))
}

fn expr_to_datum(expr: &Expr, type_id: TypeId) -> PgWireResult<Datum> {
    match (expr, type_id) {
        (Expr::Integer(i), TypeId::Int4) => Ok(Datum::Int4(*i as i32)),
        _ => Err(user_error("42804", "Type mismatch in expression")),
    }
}

fn user_error(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::parser;

    #[test]
    fn execute_select_integer() {
        let stmts = parser::parse("SELECT 1;").unwrap();
        let db = Database::new(std::path::Path::new("/tmp/pepper_test_unused"));
        let resp = execute(stmts.into_iter().next().unwrap(), &db).unwrap();
        assert!(matches!(resp, Response::Query(_)));
    }
}
