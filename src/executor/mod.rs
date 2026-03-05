// Query executor: runs parsed statements against the Database.

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use std::sync::Arc;

use crate::catalog::Column;
use crate::parser::{Expr, SelectTarget, Statement};
use crate::storage::disk::PAGE_SIZE;
use crate::storage::heap;
use crate::types::{Datum, TypeId};
use crate::Database;

pub fn execute(stmt: Statement, db: &Database) -> PgWireResult<Response<'static>> {
    match stmt {
        Statement::Select(s) => match s.target {
            SelectTarget::Expressions(exprs) => execute_select_exprs(exprs),
            SelectTarget::FromTable { table_name } => execute_select_from(db, &table_name),
        },
        Statement::CreateTable(ct) => execute_create_table(db, ct),
        Statement::Insert(ins) => execute_insert(db, ins),
    }
}

fn execute_select_exprs(exprs: Vec<Expr>) -> PgWireResult<Response<'static>> {
    let fields: Vec<FieldInfo> = exprs
        .iter()
        .map(|expr| {
            let ty = match expr {
                Expr::Integer(_) => Type::INT4,
                Expr::StringLiteral(_) => Type::TEXT,
            };
            FieldInfo::new("?column?".to_owned(), None, None, ty, FieldFormat::Text)
        })
        .collect();

    let schema = Arc::new(fields);
    let mut encoder = DataRowEncoder::new(schema.clone());
    for expr in &exprs {
        match expr {
            Expr::Integer(i) => encoder.encode_field(&i)?,
            Expr::StringLiteral(s) => encoder.encode_field(&s)?,
        }
    }
    let row = encoder.finish();

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![row]),
    )))
}

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

fn execute_select_from(db: &Database, table_name: &str) -> PgWireResult<Response<'static>> {
    let (oid, columns) = {
        let catalog = db.catalog.lock().unwrap();
        let table = catalog.get_table(table_name).ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("relation \"{}\" does not exist", table_name),
            )))
        })?;
        (table.oid, table.columns.clone())
    };

    let fields: Vec<FieldInfo> = columns
        .iter()
        .map(|col| {
            let ty = match col.type_id {
                TypeId::Int4 => Type::INT4,
            };
            FieldInfo::new(col.name.clone(), None, None, ty, FieldFormat::Text)
        })
        .collect();
    let schema = Arc::new(fields);

    let disk = db.disk.lock().unwrap();
    let num_pages = disk.num_pages(oid);
    let mut rows = Vec::new();
    let mut page = [0u8; PAGE_SIZE];

    for page_id in 0..num_pages {
        disk.read_page(oid, page_id, &mut page);
        let n = heap::num_items(&page);
        for item_idx in 0..n {
            if let Some(data) = heap::get_tuple(&page, item_idx) {
                let datums = heap::deserialize_tuple(data, &columns);
                let mut encoder = DataRowEncoder::new(schema.clone());
                for datum in &datums {
                    match datum {
                        Datum::Int4(i) => encoder.encode_field(i)?,
                        Datum::Null => encoder.encode_field(&None::<i32>)?,
                    }
                }
                rows.push(encoder.finish());
            }
        }
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
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
    use crate::parser::SelectStmt;

    #[test]
    fn execute_select_integer() {
        let stmt = Statement::Select(SelectStmt {
            target: SelectTarget::Expressions(vec![Expr::Integer(1)]),
        });
        let db = Database::new(std::path::Path::new("/tmp/pepper_test_unused"));
        let resp = execute(stmt, &db).unwrap();
        assert!(matches!(resp, Response::Query(_)));
    }
}
