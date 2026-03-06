// Query executor: routes SELECT to DataFusion, handles DDL/DML directly.

use std::any::Any;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::catalog::Column;
use crate::parser::{self, BinOp, Expr, Statement, UnaryOp};
use crate::storage::disk::{DiskManager, PAGE_SIZE};
use crate::storage::heap;
use crate::types::{Datum, TypeId};
use crate::Database;

/// Main entry point: takes raw SQL, classifies, and routes.
pub async fn execute_sql(sql: &str, db: &Database) -> PgWireResult<Response<'static>> {
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| {
        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            e.to_string(),
        )))
    })?;

    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| user_error("42601", "empty query"))?;

    match &stmt {
        ast::Statement::Query(_) => execute_select_df(sql, db).await,
        _ => {
            let our_stmt = parser::convert_statement(stmt)?;
            execute(our_stmt, db)
        }
    }
}

fn execute(stmt: Statement, db: &Database) -> PgWireResult<Response<'static>> {
    match stmt {
        Statement::CreateTable(ct) => execute_create_table(db, ct),
        Statement::Insert(ins) => execute_insert(db, ins),
        Statement::Update(upd) => execute_update(db, upd),
        Statement::Delete(del) => execute_delete(db, del),
        Statement::DropTable(dt) => execute_drop_table(db, dt),
    }
}

// -- DataFusion SELECT -----------------------------------------------------

async fn execute_select_df(sql: &str, db: &Database) -> PgWireResult<Response<'static>> {
    // Register all catalog tables with DataFusion
    register_all_tables(db)?;

    let df = db
        .session
        .sql(sql)
        .await
        .map_err(|e| df_to_pg(&e, sql))?;
    let batches = df.collect().await.map_err(|e| df_to_pg(&e, sql))?;

    batches_to_response(batches)
}

fn register_all_tables(db: &Database) -> PgWireResult<()> {
    let catalog = db.catalog.lock().unwrap();
    for table in catalog.all_tables() {
        let provider = HeapTableProvider {
            arrow_schema: columns_to_arrow_schema(&table.columns),
            oid: table.oid,
            columns: table.columns.clone(),
            disk: db.disk.clone(),
        };
        // Deregister first (ignore error if not exists), then register fresh
        let _ = db.session.deregister_table(&table.name);
        db.session
            .register_table(&table.name, Arc::new(provider))
            .map_err(|e| df_to_pg(&e, ""))?;
    }
    Ok(())
}

// -- HeapTableProvider -----------------------------------------------------

struct HeapTableProvider {
    arrow_schema: SchemaRef,
    oid: u32,
    columns: Vec<Column>,
    disk: Arc<Mutex<DiskManager>>,
}

impl std::fmt::Debug for HeapTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapTableProvider")
            .field("oid", &self.oid)
            .finish()
    }
}

#[async_trait]
impl TableProvider for HeapTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batch = self.scan_heap()?;
        let mem = MemTable::try_new(self.arrow_schema.clone(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

impl HeapTableProvider {
    fn scan_heap(&self) -> datafusion::error::Result<RecordBatch> {
        let disk = self.disk.lock().unwrap();
        let num_pages = disk.num_pages(self.oid);
        let mut tuples = Vec::new();
        let mut page = [0u8; PAGE_SIZE];

        for page_id in 0..num_pages {
            disk.read_page(self.oid, page_id, &mut page);
            let n = heap::num_items(&page);
            for item_idx in 0..n {
                if let Some(data) = heap::get_tuple(&page, item_idx) {
                    tuples.push(heap::deserialize_tuple(data, &self.columns));
                }
            }
        }

        tuples_to_record_batch(&tuples, &self.columns, &self.arrow_schema)
    }
}

// -- Arrow conversion helpers ----------------------------------------------

fn columns_to_arrow_schema(columns: &[Column]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, type_id_to_arrow(c.type_id), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn type_id_to_arrow(tid: TypeId) -> DataType {
    match tid {
        TypeId::Bool => DataType::Boolean,
        TypeId::Int2 => DataType::Int16,
        TypeId::Int4 => DataType::Int32,
        TypeId::Int8 => DataType::Int64,
        TypeId::Float4 => DataType::Float32,
        TypeId::Float8 => DataType::Float64,
        TypeId::Text => DataType::Utf8,
    }
}

fn tuples_to_record_batch(
    tuples: &[Vec<Datum>],
    columns: &[Column],
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(col_idx, col)| build_array(tuples, col_idx, col.type_id))
        .collect();
    RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
        datafusion::error::DataFusionError::ArrowError(e, None)
    })
}

fn build_array(tuples: &[Vec<Datum>], col_idx: usize, tid: TypeId) -> ArrayRef {
    match tid {
        TypeId::Bool => {
            let vals: Vec<Option<bool>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Bool(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(BooleanArray::from(vals))
        }
        TypeId::Int2 => {
            let vals: Vec<Option<i16>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Int2(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(Int16Array::from(vals))
        }
        TypeId::Int4 => {
            let vals: Vec<Option<i32>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Int4(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(Int32Array::from(vals))
        }
        TypeId::Int8 => {
            let vals: Vec<Option<i64>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Int8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(vals))
        }
        TypeId::Float4 => {
            let vals: Vec<Option<f32>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Float4(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(Float32Array::from(vals))
        }
        TypeId::Float8 => {
            let vals: Vec<Option<f64>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Float8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(vals))
        }
        TypeId::Text => {
            let vals: Vec<Option<&str>> = tuples
                .iter()
                .map(|r| match &r[col_idx] {
                    Datum::Text(v) => Some(v.as_str()),
                    _ => None,
                })
                .collect();
            Arc::new(StringArray::from(vals))
        }
    }
}

// -- RecordBatch to pgwire Response ----------------------------------------

fn batches_to_response(batches: Vec<RecordBatch>) -> PgWireResult<Response<'static>> {
    let arrow_schema = if let Some(first) = batches.first() {
        first.schema()
    } else {
        let schema = Arc::new(vec![]);
        return Ok(Response::Query(QueryResponse::new(
            schema,
            stream::iter(vec![]),
        )));
    };
    let fields: Vec<FieldInfo> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            // DataFusion names literal columns like "Int64(1)". PostgreSQL uses "?column?".
            let name = if f.name().contains('(') && f.name().contains(')') {
                "?column?".to_owned()
            } else {
                f.name().clone()
            };
            FieldInfo::new(
                name,
                None,
                None,
                arrow_to_pg_type(f.data_type()),
                FieldFormat::Text,
            )
        })
        .collect();
    let schema = Arc::new(fields);

    let mut rows = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for col_idx in 0..batch.num_columns() {
                encode_arrow_value(&mut encoder, batch.column(col_idx), row_idx)?;
            }
            rows.push(encoder.finish());
        }
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
}

fn arrow_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT,
        _ => Type::TEXT,
    }
}

fn encode_arrow_value(
    encoder: &mut DataRowEncoder,
    array: &ArrayRef,
    row: usize,
) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&None::<i32>);
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        _ => encoder.encode_field(&None::<i32>),
    }
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

    {
        let mut catalog = db.catalog.lock().unwrap();
        match catalog.create_table(&ct.table_name, columns) {
            Ok(oid) => {
                let disk = db.disk.lock().unwrap();
                disk.create_heap_file(oid);
            }
            Err(e) => {
                if ct.if_not_exists {
                    return Ok(Response::Execution(Tag::new("CREATE TABLE")));
                }
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new("ERROR".to_owned(), "42P07".to_owned(), e),
                )));
            }
        }
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
            user_error(
                "42P01",
                &format!("relation \"{}\" does not exist", ins.table_name),
            )
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
        insert_tuple_to_heap(&disk, oid, &tuple_data)?;
        count += 1;
    }

    Ok(Response::Execution(Tag::new(&format!("INSERT 0 {}", count))))
}

// -- UPDATE ---------------------------------------------------------------

fn execute_update(
    db: &Database,
    upd: crate::parser::UpdateStmt,
) -> PgWireResult<Response<'static>> {
    let (oid, columns) = {
        let catalog = db.catalog.lock().unwrap();
        let table = catalog.get_table(&upd.table_name).ok_or_else(|| {
            user_error(
                "42P01",
                &format!("relation \"{}\" does not exist", upd.table_name),
            )
        })?;
        (table.oid, table.columns.clone())
    };

    let disk = db.disk.lock().unwrap();
    let num_pages = disk.num_pages(oid);
    let mut page = [0u8; PAGE_SIZE];
    let mut count = 0u64;
    let mut new_tuples: Vec<Vec<Datum>> = Vec::new();

    for page_id in 0..num_pages {
        disk.read_page(oid, page_id, &mut page);
        let n = heap::num_items(&page);
        for item_idx in 0..n {
            if let Some(data) = heap::get_tuple(&page, item_idx) {
                let datums = heap::deserialize_tuple(data, &columns);
                let matches = match &upd.where_clause {
                    Some(wh) => matches!(eval_expr(wh, &datums, &columns), Datum::Bool(true)),
                    None => true,
                };
                if matches {
                    let mut new_row = datums.clone();
                    for (col_name, expr) in &upd.assignments {
                        if let Some(idx) = columns.iter().position(|c| c.name == *col_name) {
                            new_row[idx] = eval_expr(expr, &datums, &columns);
                        }
                    }
                    new_tuples.push(new_row);
                    heap::mark_tuple_dead(&mut page, item_idx);
                    count += 1;
                }
            }
        }
        disk.write_page(oid, page_id, &page);
    }

    for new_row in &new_tuples {
        let tuple_data = heap::serialize_tuple(new_row);
        insert_tuple_to_heap(&disk, oid, &tuple_data)?;
    }

    Ok(Response::Execution(Tag::new(&format!("UPDATE {}", count))))
}

// -- DELETE ---------------------------------------------------------------

fn execute_delete(
    db: &Database,
    del: crate::parser::DeleteStmt,
) -> PgWireResult<Response<'static>> {
    let (oid, columns) = {
        let catalog = db.catalog.lock().unwrap();
        let table = catalog.get_table(&del.table_name).ok_or_else(|| {
            user_error(
                "42P01",
                &format!("relation \"{}\" does not exist", del.table_name),
            )
        })?;
        (table.oid, table.columns.clone())
    };

    let disk = db.disk.lock().unwrap();
    let num_pages = disk.num_pages(oid);
    let mut page = [0u8; PAGE_SIZE];
    let mut count = 0u64;

    for page_id in 0..num_pages {
        disk.read_page(oid, page_id, &mut page);
        let n = heap::num_items(&page);
        for item_idx in 0..n {
            if let Some(data) = heap::get_tuple(&page, item_idx) {
                let datums = heap::deserialize_tuple(data, &columns);
                let matches = match &del.where_clause {
                    Some(wh) => matches!(eval_expr(wh, &datums, &columns), Datum::Bool(true)),
                    None => true,
                };
                if matches {
                    heap::mark_tuple_dead(&mut page, item_idx);
                    count += 1;
                }
            }
        }
        disk.write_page(oid, page_id, &page);
    }

    Ok(Response::Execution(Tag::new(&format!("DELETE {}", count))))
}

// -- DROP TABLE -----------------------------------------------------------

fn execute_drop_table(
    db: &Database,
    dt: crate::parser::DropTableStmt,
) -> PgWireResult<Response<'static>> {
    let oid = {
        let mut catalog = db.catalog.lock().unwrap();
        match catalog.drop_table(&dt.table_name) {
            Ok(oid) => oid,
            Err(e) => {
                if dt.if_exists {
                    return Ok(Response::Execution(Tag::new("DROP TABLE")));
                }
                return Err(user_error("42P01", &e));
            }
        }
    };

    let _ = db.session.deregister_table(&dt.table_name);
    {
        let disk = db.disk.lock().unwrap();
        disk.delete_heap_file(oid);
    }

    Ok(Response::Execution(Tag::new("DROP TABLE")))
}

// -- Expression evaluation (for UPDATE/DELETE WHERE) -----------------------

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

fn promote(a: &Datum, b: &Datum) -> (Datum, Datum) {
    match (a, b) {
        (Datum::Int4(_), Datum::Int4(_))
        | (Datum::Int2(_), Datum::Int2(_))
        | (Datum::Int8(_), Datum::Int8(_))
        | (Datum::Float4(_), Datum::Float4(_))
        | (Datum::Float8(_), Datum::Float8(_)) => (a.clone(), b.clone()),
        (Datum::Int2(l), Datum::Int4(_)) => (Datum::Int4(*l as i32), b.clone()),
        (Datum::Int4(_), Datum::Int2(r)) => (a.clone(), Datum::Int4(*r as i32)),
        (Datum::Int2(l), Datum::Int8(_)) => (Datum::Int8(*l as i64), b.clone()),
        (Datum::Int8(_), Datum::Int2(r)) => (a.clone(), Datum::Int8(*r as i64)),
        (Datum::Int4(l), Datum::Int8(_)) => (Datum::Int8(*l as i64), b.clone()),
        (Datum::Int8(_), Datum::Int4(r)) => (a.clone(), Datum::Int8(*r as i64)),
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
    match op {
        BinOp::And => {
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

    if let (Datum::Bool(l), Datum::Bool(r)) = (left, right) {
        return match op {
            BinOp::Eq => Datum::Bool(l == r),
            BinOp::NotEq => Datum::Bool(l != r),
            _ => Datum::Null,
        };
    }

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
            if r == 0 { Datum::Null } else { wrap(l / r) }
        }
        BinOp::Mod => {
            if r == 0 { Datum::Null } else { wrap(l % r) }
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
            if r == 0.0 { Datum::Null } else { wrap(l / r) }
        }
        BinOp::Mod => {
            if r == 0.0 { Datum::Null } else { wrap(l % r) }
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

// -- Helpers ---------------------------------------------------------------

fn insert_tuple_to_heap(
    disk: &DiskManager,
    oid: u32,
    tuple_data: &[u8],
) -> PgWireResult<()> {
    let num_pages = disk.num_pages(oid);
    let mut page = [0u8; PAGE_SIZE];

    if num_pages == 0 {
        heap::init_page(&mut page);
        if heap::insert_tuple(&mut page, tuple_data).is_err() {
            return Err(user_error("42000", "Tuple too large for page"));
        }
        disk.write_page(oid, 0, &page);
    } else {
        let last_page_id = num_pages - 1;
        disk.read_page(oid, last_page_id, &mut page);
        if heap::insert_tuple(&mut page, tuple_data).is_ok() {
            disk.write_page(oid, last_page_id, &page);
        } else {
            let new_page_id = num_pages;
            heap::init_page(&mut page);
            if heap::insert_tuple(&mut page, tuple_data).is_err() {
                return Err(user_error("42000", "Tuple too large for page"));
            }
            disk.write_page(oid, new_page_id, &page);
        }
    }
    Ok(())
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

fn df_to_pg(e: &datafusion::error::DataFusionError, sql: &str) -> PgWireError {
    let msg = e.to_string();

    // Translate DataFusion errors to PostgreSQL-style messages
    if msg.contains("not found") {
        // Extract table name from SQL for a cleaner error
        let table = sql
            .split_whitespace()
            .skip_while(|w| !w.eq_ignore_ascii_case("FROM"))
            .nth(1)
            .unwrap_or("unknown");
        let table = table.trim_end_matches(';');
        return user_error(
            "42P01",
            &format!("relation \"{}\" does not exist", table),
        );
    }

    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        msg,
    )))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn execute_select_integer() {
        let db = Database::new(std::path::Path::new("/tmp/pepper_test_unused"));
        let resp = execute_sql("SELECT 1;", &db).await.unwrap();
        assert!(matches!(resp, Response::Query(_)));
    }
}
