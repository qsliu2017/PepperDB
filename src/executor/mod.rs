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
        Expr::Float(_) => Ok(Type::FLOAT8),
        Expr::StringLiteral(_) => Ok(Type::TEXT),
        Expr::Bool(_) => Ok(Type::BOOL),
        Expr::Null => Ok(Type::TEXT),
        Expr::ColumnRef(name) => {
            let col = columns.iter().find(|c| c.name == *name).ok_or_else(|| {
                user_error("42703", &format!("column \"{}\" does not exist", name))
            })?;
            Ok(type_id_to_pg(col.type_id))
        }
        Expr::BinaryOp { op, left, right } => match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => {
                let lt = infer_type(left, columns)?;
                let rt = infer_type(right, columns)?;
                Ok(promote_pg_type(&lt, &rt))
            }
            _ => Ok(Type::BOOL),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOp::Minus => infer_type(expr, columns),
            UnaryOp::Not => Ok(Type::BOOL),
        },
        Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(Type::BOOL),
    }
}

fn promote_pg_type(a: &Type, b: &Type) -> Type {
    let rank = |t: &Type| -> u8 {
        if *t == Type::INT2 {
            1
        } else if *t == Type::INT4 {
            2
        } else if *t == Type::INT8 {
            3
        } else if *t == Type::FLOAT4 {
            4
        } else if *t == Type::FLOAT8 {
            5
        } else {
            2
        }
    };
    let ra = rank(a);
    let rb = rank(b);
    if ra >= rb { a.clone() } else { b.clone() }
}

fn type_id_to_pg(type_id: TypeId) -> Type {
    match type_id {
        TypeId::Bool => Type::BOOL,
        TypeId::Int2 => Type::INT2,
        TypeId::Int4 => Type::INT4,
        TypeId::Int8 => Type::INT8,
        TypeId::Float4 => Type::FLOAT4,
        TypeId::Float8 => Type::FLOAT8,
        TypeId::Text => Type::TEXT,
    }
}

// -- Expression evaluation ------------------------------------------------

fn eval_expr(expr: &Expr, row: &[Datum], columns: &[Column]) -> Datum {
    match expr {
        Expr::Integer(i) => Datum::Int4(*i as i32),
        Expr::Float(f) => Datum::Float8(*f),
        Expr::StringLiteral(s) => Datum::Text(s.clone()),
        Expr::Bool(b) => Datum::Bool(*b),
        Expr::Null => Datum::Null,
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
            eval_unary(*op, &v)
        }
        Expr::IsNull(inner) => {
            let v = eval_expr(inner, row, columns);
            Datum::Bool(v == Datum::Null)
        }
        Expr::IsNotNull(inner) => {
            let v = eval_expr(inner, row, columns);
            Datum::Bool(v != Datum::Null)
        }
    }
}

fn eval_unary(op: UnaryOp, v: &Datum) -> Datum {
    match op {
        UnaryOp::Minus => match v {
            Datum::Int2(i) => Datum::Int2(-i),
            Datum::Int4(i) => Datum::Int4(-i),
            Datum::Int8(i) => Datum::Int8(-i),
            Datum::Float4(f) => Datum::Float4(-f),
            Datum::Float8(f) => Datum::Float8(-f),
            _ => Datum::Null,
        },
        UnaryOp::Not => match v {
            Datum::Bool(b) => Datum::Bool(!b),
            _ => Datum::Null,
        },
    }
}

/// Promote two datums to a common numeric type for arithmetic/comparison.
fn promote(a: &Datum, b: &Datum) -> (Datum, Datum) {
    match (a, b) {
        // Same types -- no promotion needed
        (Datum::Int4(_), Datum::Int4(_))
        | (Datum::Int2(_), Datum::Int2(_))
        | (Datum::Int8(_), Datum::Int8(_))
        | (Datum::Float4(_), Datum::Float4(_))
        | (Datum::Float8(_), Datum::Float8(_)) => (a.clone(), b.clone()),

        // Promote to wider integer
        (Datum::Int2(l), Datum::Int4(_)) => (Datum::Int4(*l as i32), b.clone()),
        (Datum::Int4(_), Datum::Int2(r)) => (a.clone(), Datum::Int4(*r as i32)),
        (Datum::Int2(l), Datum::Int8(_)) => (Datum::Int8(*l as i64), b.clone()),
        (Datum::Int8(_), Datum::Int2(r)) => (a.clone(), Datum::Int8(*r as i64)),
        (Datum::Int4(l), Datum::Int8(_)) => (Datum::Int8(*l as i64), b.clone()),
        (Datum::Int8(_), Datum::Int4(r)) => (a.clone(), Datum::Int8(*r as i64)),

        // Int + Float -> Float8
        (Datum::Float4(l), Datum::Float8(_)) => (Datum::Float8(*l as f64), b.clone()),
        (Datum::Float8(_), Datum::Float4(r)) => (a.clone(), Datum::Float8(*r as f64)),

        (Datum::Int2(l), Datum::Float4(_)) | (Datum::Int2(l), Datum::Float8(_)) => {
            (Datum::Float8(*l as f64), to_f64(b))
        }
        (Datum::Float4(_), Datum::Int2(r)) | (Datum::Float8(_), Datum::Int2(r)) => {
            (to_f64(a), Datum::Float8(*r as f64))
        }
        (Datum::Int4(l), Datum::Float4(_)) | (Datum::Int4(l), Datum::Float8(_)) => {
            (Datum::Float8(*l as f64), to_f64(b))
        }
        (Datum::Float4(_), Datum::Int4(r)) | (Datum::Float8(_), Datum::Int4(r)) => {
            (to_f64(a), Datum::Float8(*r as f64))
        }
        (Datum::Int8(l), Datum::Float4(_)) | (Datum::Int8(l), Datum::Float8(_)) => {
            (Datum::Float8(*l as f64), to_f64(b))
        }
        (Datum::Float4(_), Datum::Int8(r)) | (Datum::Float8(_), Datum::Int8(r)) => {
            (to_f64(a), Datum::Float8(*r as f64))
        }

        _ => (a.clone(), b.clone()),
    }
}

fn to_f64(d: &Datum) -> Datum {
    match d {
        Datum::Int2(i) => Datum::Float8(*i as f64),
        Datum::Int4(i) => Datum::Float8(*i as f64),
        Datum::Int8(i) => Datum::Float8(*i as f64),
        Datum::Float4(f) => Datum::Float8(*f as f64),
        Datum::Float8(_) => d.clone(),
        _ => Datum::Null,
    }
}

fn eval_binop(op: BinOp, left: &Datum, right: &Datum) -> Datum {
    // NULL propagation: any op with NULL yields NULL (except AND/OR short-circuit)
    match op {
        BinOp::And => {
            // false AND NULL = false
            if matches!(left, Datum::Bool(false)) || matches!(right, Datum::Bool(false)) {
                return Datum::Bool(false);
            }
            if *left == Datum::Null || *right == Datum::Null {
                return Datum::Null;
            }
            if let (Datum::Bool(l), Datum::Bool(r)) = (left, right) {
                return Datum::Bool(*l && *r);
            }
            return Datum::Null;
        }
        BinOp::Or => {
            // true OR NULL = true
            if matches!(left, Datum::Bool(true)) || matches!(right, Datum::Bool(true)) {
                return Datum::Bool(true);
            }
            if *left == Datum::Null || *right == Datum::Null {
                return Datum::Null;
            }
            if let (Datum::Bool(l), Datum::Bool(r)) = (left, right) {
                return Datum::Bool(*l || *r);
            }
            return Datum::Null;
        }
        _ => {}
    }

    if *left == Datum::Null || *right == Datum::Null {
        return Datum::Null;
    }

    // Bool equality
    if let (Datum::Bool(l), Datum::Bool(r)) = (left, right) {
        return match op {
            BinOp::Eq => Datum::Bool(l == r),
            BinOp::NotEq => Datum::Bool(l != r),
            _ => Datum::Null,
        };
    }

    // Text operations
    if let (Datum::Text(l), Datum::Text(r)) = (left, right) {
        return match op {
            BinOp::Eq => Datum::Bool(l == r),
            BinOp::NotEq => Datum::Bool(l != r),
            BinOp::Lt => Datum::Bool(l < r),
            BinOp::Gt => Datum::Bool(l > r),
            BinOp::LtEq => Datum::Bool(l <= r),
            BinOp::GtEq => Datum::Bool(l >= r),
            _ => Datum::Null,
        };
    }

    // Numeric: promote then operate
    let (l, r) = promote(left, right);

    match (&l, &r) {
        (Datum::Int2(l), Datum::Int2(r)) => eval_int_op(op, *l as i64, *r as i64, 2),
        (Datum::Int4(l), Datum::Int4(r)) => eval_int_op(op, *l as i64, *r as i64, 4),
        (Datum::Int8(l), Datum::Int8(r)) => eval_int_op(op, *l, *r, 8),
        (Datum::Float4(l), Datum::Float4(r)) => eval_float_op(op, *l as f64, *r as f64, 4),
        (Datum::Float8(l), Datum::Float8(r)) => eval_float_op(op, *l, *r, 8),
        _ => Datum::Null,
    }
}

fn eval_int_op(op: BinOp, l: i64, r: i64, size: u8) -> Datum {
    let wrap = |v: i64| -> Datum {
        match size {
            2 => Datum::Int2(v as i16),
            4 => Datum::Int4(v as i32),
            _ => Datum::Int8(v),
        }
    };
    match op {
        BinOp::Add => wrap(l.wrapping_add(r)),
        BinOp::Sub => wrap(l.wrapping_sub(r)),
        BinOp::Mul => wrap(l.wrapping_mul(r)),
        BinOp::Div => {
            if r == 0 {
                return Datum::Null;
            }
            wrap(l / r)
        }
        BinOp::Eq => Datum::Bool(l == r),
        BinOp::NotEq => Datum::Bool(l != r),
        BinOp::Lt => Datum::Bool(l < r),
        BinOp::Gt => Datum::Bool(l > r),
        BinOp::LtEq => Datum::Bool(l <= r),
        BinOp::GtEq => Datum::Bool(l >= r),
        _ => Datum::Null,
    }
}

fn eval_float_op(op: BinOp, l: f64, r: f64, size: u8) -> Datum {
    let wrap = |v: f64| -> Datum {
        match size {
            4 => Datum::Float4(v as f32),
            _ => Datum::Float8(v),
        }
    };
    match op {
        BinOp::Add => wrap(l + r),
        BinOp::Sub => wrap(l - r),
        BinOp::Mul => wrap(l * r),
        BinOp::Div => {
            if r == 0.0 {
                return Datum::Null;
            }
            wrap(l / r)
        }
        BinOp::Eq => Datum::Bool(l == r),
        BinOp::NotEq => Datum::Bool(l != r),
        BinOp::Lt => Datum::Bool(l < r),
        BinOp::Gt => Datum::Bool(l > r),
        BinOp::LtEq => Datum::Bool(l <= r),
        BinOp::GtEq => Datum::Bool(l >= r),
        _ => Datum::Null,
    }
}

fn compare_datums(a: &Datum, b: &Datum) -> Ordering {
    if *a == Datum::Null && *b == Datum::Null {
        return Ordering::Equal;
    }
    if *a == Datum::Null {
        return Ordering::Greater; // NULLs sort last (ASC)
    }
    if *b == Datum::Null {
        return Ordering::Less;
    }
    let (pa, pb) = promote(a, b);
    match (&pa, &pb) {
        (Datum::Int2(a), Datum::Int2(b)) => a.cmp(b),
        (Datum::Int4(a), Datum::Int4(b)) => a.cmp(b),
        (Datum::Int8(a), Datum::Int8(b)) => a.cmp(b),
        (Datum::Float4(a), Datum::Float4(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Datum::Float8(a), Datum::Float8(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Datum::Bool(a), Datum::Bool(b)) => a.cmp(b),
        (Datum::Text(a), Datum::Text(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn encode_datum(encoder: &mut DataRowEncoder, datum: &Datum) -> PgWireResult<()> {
    match datum {
        Datum::Bool(b) => encoder.encode_field(b),
        Datum::Int2(i) => encoder.encode_field(i),
        Datum::Int4(i) => encoder.encode_field(i),
        Datum::Int8(i) => encoder.encode_field(i),
        Datum::Float4(f) => encoder.encode_field(f),
        Datum::Float8(f) => encoder.encode_field(f),
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
    match expr {
        Expr::Null => Ok(Datum::Null),
        Expr::Bool(b) => match type_id {
            TypeId::Bool => Ok(Datum::Bool(*b)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::Integer(i) => match type_id {
            TypeId::Int2 => Ok(Datum::Int2(*i as i16)),
            TypeId::Int4 => Ok(Datum::Int4(*i as i32)),
            TypeId::Int8 => Ok(Datum::Int8(*i)),
            TypeId::Float4 => Ok(Datum::Float4(*i as f32)),
            TypeId::Float8 => Ok(Datum::Float8(*i as f64)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::Float(f) => match type_id {
            TypeId::Float4 => Ok(Datum::Float4(*f as f32)),
            TypeId::Float8 => Ok(Datum::Float8(*f)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::StringLiteral(s) => match type_id {
            TypeId::Text => Ok(Datum::Text(s.clone())),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
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
