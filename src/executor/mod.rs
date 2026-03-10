//! Query executor: routes SELECT to DataFusion, handles DDL/DML directly.

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

use crate::catalog::bootstrap;
use crate::catalog::Column;
use crate::parser::{self, BinOp, Expr, Statement, UnaryOp};
use crate::storage::disk::{DiskManager, PAGE_SIZE};
use crate::storage::{fsm, heap, vm};
use crate::txn::TxnManager;
use crate::types::{Datum, TypeId};
use crate::wal::record::{self as wal_record, WalRecord};
use crate::Database;

impl Database {
    /// Main entry point: takes raw SQL, classifies, and routes.
    pub async fn execute_sql(&self, sql: &str) -> PgWireResult<Response<'static>> {
        // Handle VACUUM manually (not in sqlparser-rs AST).
        // Strip SQL comments before checking.
        let stripped: String = sql
            .lines()
            .map(|l| l.split("--").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join(" ");
        let trimmed = stripped.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        if upper == "VACUUM" || upper.starts_with("VACUUM ") {
            let table = trimmed[6..].trim(); // skip "VACUUM"
            let table_name = if table.is_empty() {
                None
            } else {
                Some(table.to_string())
            };
            return self.execute(Statement::Vacuum(parser::VacuumStmt { table_name }));
        }

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
            ast::Statement::Query(_) => self.execute_select_df(sql).await,
            _ => {
                let our_stmt = parser::convert_statement(stmt)?;
                self.execute(our_stmt)
            }
        }
    }

    fn execute(&self, stmt: Statement) -> PgWireResult<Response<'static>> {
        match stmt {
            Statement::CreateTable(ct) => self.execute_create_table(ct),
            Statement::CreateIndex(ci) => self.execute_create_index(ci),
            Statement::Insert(ins) => self.execute_insert(ins),
            Statement::Update(upd) => self.execute_update(upd),
            Statement::Delete(del) => self.execute_delete(del),
            Statement::DropTable(dt) => self.execute_drop_table(dt),
            Statement::Vacuum(v) => self.execute_vacuum(v),
        }
    }

    // -- DataFusion SELECT -------------------------------------------------

    async fn execute_select_df(&self, sql: &str) -> PgWireResult<Response<'static>> {
        // Rewrite constant HAVING clauses (DataFusion doesn't support them).
        let rewritten = rewrite_constant_having(sql);
        let sql = rewritten.as_deref().unwrap_or(sql);

        self.register_all_tables()?;

        let df = self.session.sql(sql).await.map_err(|e| df_to_pg(&e, sql))?;
        let arrow_schema = df.schema().inner().clone();
        let batches = df.collect().await.map_err(|e| df_to_pg(&e, sql))?;

        batches_to_response(batches, &arrow_schema)
    }

    fn register_all_tables(&self) -> PgWireResult<()> {
        let catalog = self.catalog.lock().unwrap();
        for table in catalog.all_tables() {
            let provider = HeapTableProvider {
                arrow_schema: columns_to_arrow_schema(&table.columns),
                oid: table.oid,
                columns: table.columns.clone(),
                disk: self.disk.clone(),
                txn: self.txn.clone(),
            };
            let _ = self.session.deregister_table(&table.name);
            self.session
                .register_table(&table.name, Arc::new(provider))
                .map_err(|e| df_to_pg(&e, ""))?;
        }
        Ok(())
    }
}

// -- HeapTableProvider -----------------------------------------------------

struct HeapTableProvider {
    arrow_schema: SchemaRef,
    oid: u32,
    columns: Vec<Column>,
    disk: Arc<Mutex<DiskManager>>,
    txn: Arc<Mutex<TxnManager>>,
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
        let mut txn = self.txn.lock().unwrap();
        let snapshot = txn.take_snapshot();
        let num_pages = disk.num_pages(self.oid);
        let mut tuples = Vec::new();
        let mut page = [0u8; PAGE_SIZE];

        for page_id in 0..num_pages {
            disk.read_page(self.oid, page_id, &mut page);
            let n = heap::num_items(&page);
            for item_idx in 0..n {
                if let Some(datums) =
                    heap::read_tuple_mvcc(&page, item_idx, &self.columns, &snapshot, txn.clog())
                {
                    tuples.push(datums);
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
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
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

fn batches_to_response(
    batches: Vec<RecordBatch>,
    arrow_schema: &Schema,
) -> PgWireResult<Response<'static>> {
    // Track which columns are function/expression results (need trailing-space
    // trimming for CHAR(N) semantics -- PG implicitly casts char->text for functions).
    let is_func_col: Vec<bool> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().contains('(') && !f.name().starts_with(|c: char| c.is_ascii_uppercase()))
        .collect();

    let fields: Vec<FieldInfo> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            // DataFusion names columns by expression text. PostgreSQL rules:
            // - function calls like "lower(c)" -> "lower"
            // - DataFusion internal names like "Int64(1)" -> "?column?"
            let name = if let Some(func) = f.name().split('(').next() {
                if !f.name().contains('(') {
                    f.name().clone()
                } else if func.starts_with(|c: char| c.is_ascii_uppercase()) {
                    "?column?".to_owned()
                } else {
                    func.to_owned()
                }
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

    let rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(move |row_idx| (batch, row_idx)))
        .map(|(batch, row_idx)| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for (col_idx, &trim) in is_func_col.iter().enumerate() {
                encode_arrow_value(&mut encoder, batch.column(col_idx), row_idx, trim)?;
            }
            encoder.finish()
        })
        .collect();

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
    trim_trailing_spaces: bool,
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
            if trim_trailing_spaces {
                encoder.encode_field(&a.value(row).trim_end())
            } else {
                encoder.encode_field(&a.value(row))
            }
        }
        _ => encoder.encode_field(&None::<i32>),
    }
}

impl Database {
    // -- CREATE TABLE -----------------------------------------------------

    fn execute_create_table(
        &self,
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
                typmod: c.typmod,
            })
            .collect();

        {
            let mut catalog = self.catalog.lock().unwrap();
            match catalog.create_table(&ct.table_name, columns.clone()) {
                Ok(oid) => {
                    let disk = self.disk.lock().unwrap();
                    disk.create_heap_file(oid);
                    bootstrap::insert_pg_class_row(
                        &disk,
                        oid,
                        &ct.table_name,
                        columns.len() as i16,
                    );
                    bootstrap::insert_pg_attribute_rows(&disk, oid, &columns);
                    let _ = bootstrap::update_next_oid(&disk, catalog.next_oid());
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

    // -- INSERT -----------------------------------------------------------

    fn execute_insert(&self, ins: crate::parser::InsertStmt) -> PgWireResult<Response<'static>> {
        let (oid, columns) = {
            let catalog = self.catalog.lock().unwrap();
            let table = catalog.get_table(&ins.table_name).ok_or_else(|| {
                user_error(
                    "42P01",
                    &format!("relation \"{}\" does not exist", ins.table_name),
                )
            })?;
            (table.oid, table.columns.clone())
        };

        let mut count = 0u64;
        let disk = self.disk.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut txn = self.txn.lock().unwrap();
        let xid = txn.assign_xid();

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
                .map(|(expr, col)| expr_to_datum(expr, col))
                .collect::<PgWireResult<Vec<_>>>()?;

            let tuple = heap::build_tuple_with_xid(&datums, &columns, xid, false);
            let (pg_id, item_idx) = insert_tuple_to_heap(&disk, &mut wal, oid, &tuple, xid)?;

            // Maintain indexes
            let catalog = self.catalog.lock().unwrap();
            let indexes: Vec<_> = catalog
                .get_indexes_for_table(oid)
                .into_iter()
                .cloned()
                .collect();
            drop(catalog);
            for idx in &indexes {
                let col_pos = columns.iter().position(|c| c.name == idx.column_name);
                if let Some(pos) = col_pos {
                    let key = &datums[pos];
                    if *key != Datum::Null {
                        let tid = crate::storage::btree::ItemPointer {
                            block_id: pg_id,
                            offset_num: item_idx,
                        };
                        crate::storage::btree::bt_insert(&disk, idx.oid, key, tid, idx.key_type);
                    }
                }
            }

            count += 1;
        }

        txn.commit(xid);

        Ok(Response::Execution(Tag::new(&format!(
            "INSERT 0 {}",
            count
        ))))
    }

    // -- UPDATE -----------------------------------------------------------

    fn execute_update(&self, upd: crate::parser::UpdateStmt) -> PgWireResult<Response<'static>> {
        let (oid, columns) = {
            let catalog = self.catalog.lock().unwrap();
            let table = catalog.get_table(&upd.table_name).ok_or_else(|| {
                user_error(
                    "42P01",
                    &format!("relation \"{}\" does not exist", upd.table_name),
                )
            })?;
            (table.oid, table.columns.clone())
        };

        let disk = self.disk.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut txn = self.txn.lock().unwrap();
        let xid = txn.assign_xid();
        let num_pages = disk.num_pages(oid);
        let mut page = [0u8; PAGE_SIZE];
        let mut count = 0u64;
        let mut new_tuples: Vec<Vec<Datum>> = Vec::new();

        for page_id in 0..num_pages {
            disk.read_page(oid, page_id, &mut page);
            let n = heap::num_items(&page);
            let mut page_modified = false;
            for item_idx in 0..n {
                if let Some(datums) = heap::read_tuple(&page, item_idx, &columns) {
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
                        heap::mark_tuple_dead_with_xid(&mut page, item_idx, xid);
                        let wal_data = wal_record::build_heap_delete_data(oid, page_id, item_idx);
                        let rec = WalRecord {
                            xl_xid: xid,
                            xl_info: wal_record::XLOG_HEAP_DELETE,
                            xl_rmid: wal_record::RM_HEAP_ID,
                            data: wal_data,
                        };
                        let lsn = wal.append(&rec);
                        heap::set_page_lsn(&mut page, lsn);
                        page_modified = true;
                        count += 1;
                    }
                }
            }
            if page_modified {
                wal.flush();
                disk.write_page(oid, page_id, &page);
            }
        }

        let indexes: Vec<_> = {
            let catalog = self.catalog.lock().unwrap();
            catalog
                .get_indexes_for_table(oid)
                .into_iter()
                .cloned()
                .collect()
        };
        for new_row in &new_tuples {
            let tuple = heap::build_tuple_with_xid(new_row, &columns, xid, false);
            let (pg_id, item_idx) = insert_tuple_to_heap(&disk, &mut wal, oid, &tuple, xid)?;
            for idx in &indexes {
                let col_pos = columns.iter().position(|c| c.name == idx.column_name);
                if let Some(pos) = col_pos {
                    let key = &new_row[pos];
                    if *key != Datum::Null {
                        let tid = crate::storage::btree::ItemPointer {
                            block_id: pg_id,
                            offset_num: item_idx,
                        };
                        crate::storage::btree::bt_insert(&disk, idx.oid, key, tid, idx.key_type);
                    }
                }
            }
        }

        txn.commit(xid);

        Ok(Response::Execution(Tag::new(&format!("UPDATE {}", count))))
    }

    // -- DELETE -----------------------------------------------------------

    fn execute_delete(&self, del: crate::parser::DeleteStmt) -> PgWireResult<Response<'static>> {
        let (oid, columns) = {
            let catalog = self.catalog.lock().unwrap();
            let table = catalog.get_table(&del.table_name).ok_or_else(|| {
                user_error(
                    "42P01",
                    &format!("relation \"{}\" does not exist", del.table_name),
                )
            })?;
            (table.oid, table.columns.clone())
        };

        let disk = self.disk.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut txn = self.txn.lock().unwrap();
        let xid = txn.assign_xid();
        let num_pages = disk.num_pages(oid);
        let mut page = [0u8; PAGE_SIZE];
        let mut count = 0u64;

        for page_id in 0..num_pages {
            disk.read_page(oid, page_id, &mut page);
            let n = heap::num_items(&page);
            let mut page_modified = false;
            for item_idx in 0..n {
                if let Some(datums) = heap::read_tuple(&page, item_idx, &columns) {
                    let matches = match &del.where_clause {
                        Some(wh) => matches!(eval_expr(wh, &datums, &columns), Datum::Bool(true)),
                        None => true,
                    };
                    if matches {
                        heap::mark_tuple_dead_with_xid(&mut page, item_idx, xid);
                        let wal_data = wal_record::build_heap_delete_data(oid, page_id, item_idx);
                        let rec = WalRecord {
                            xl_xid: xid,
                            xl_info: wal_record::XLOG_HEAP_DELETE,
                            xl_rmid: wal_record::RM_HEAP_ID,
                            data: wal_data,
                        };
                        let lsn = wal.append(&rec);
                        heap::set_page_lsn(&mut page, lsn);
                        page_modified = true;
                        count += 1;
                    }
                }
            }
            if page_modified {
                wal.flush();
                disk.write_page(oid, page_id, &page);
            }
        }

        txn.commit(xid);

        Ok(Response::Execution(Tag::new(&format!("DELETE {}", count))))
    }

    // -- DROP TABLE -------------------------------------------------------

    fn execute_drop_table(
        &self,
        dt: crate::parser::DropTableStmt,
    ) -> PgWireResult<Response<'static>> {
        // Collect index OIDs before dropping from catalog
        let index_oids: Vec<u32> = {
            let catalog = self.catalog.lock().unwrap();
            if let Some(table) = catalog.get_table(&dt.table_name) {
                catalog
                    .get_indexes_for_table(table.oid)
                    .iter()
                    .map(|i| i.oid)
                    .collect()
            } else {
                vec![]
            }
        };

        let oid = {
            let mut catalog = self.catalog.lock().unwrap();
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

        let _ = self.session.deregister_table(&dt.table_name);
        {
            let disk = self.disk.lock().unwrap();
            disk.delete_heap_file(oid);
            for idx_oid in &index_oids {
                disk.delete_heap_file(*idx_oid);
                bootstrap::drop_pg_catalog_rows(&disk, *idx_oid);
            }
            bootstrap::drop_pg_catalog_rows(&disk, oid);
        }

        Ok(Response::Execution(Tag::new("DROP TABLE")))
    }

    // -- CREATE INDEX ---------------------------------------------------------

    fn execute_create_index(
        &self,
        ci: crate::parser::CreateIndexStmt,
    ) -> PgWireResult<Response<'static>> {
        let (table_oid, columns) = {
            let catalog = self.catalog.lock().unwrap();
            let table = catalog.get_table(&ci.table_name).ok_or_else(|| {
                user_error(
                    "42P01",
                    &format!("relation \"{}\" does not exist", ci.table_name),
                )
            })?;
            (table.oid, table.columns.clone())
        };

        let col = columns
            .iter()
            .find(|c| c.name == ci.column_name)
            .ok_or_else(|| {
                user_error(
                    "42703",
                    &format!("column \"{}\" does not exist", ci.column_name),
                )
            })?;
        let key_type = col.type_id;
        let col_idx = col.col_num as usize;

        let index_oid = {
            let mut catalog = self.catalog.lock().unwrap();
            match catalog.create_index(&ci.index_name, table_oid, &ci.column_name, key_type) {
                Ok(oid) => oid,
                Err(e) => return Err(user_error("42P07", &e)),
            }
        };

        {
            let disk = self.disk.lock().unwrap();
            crate::storage::btree::create_index(&disk, index_oid);

            // Populate index from existing heap data
            let num_pages = disk.num_pages(table_oid);
            let mut page = [0u8; PAGE_SIZE];
            for page_id in 0..num_pages {
                disk.read_page(table_oid, page_id, &mut page);
                let n = heap::num_items(&page);
                for item_idx in 0..n {
                    if let Some(datums) = heap::read_tuple(&page, item_idx, &columns) {
                        let key = &datums[col_idx];
                        if *key == Datum::Null {
                            continue;
                        }
                        let tid = crate::storage::btree::ItemPointer {
                            block_id: page_id,
                            offset_num: item_idx,
                        };
                        crate::storage::btree::bt_insert(&disk, index_oid, key, tid, key_type);
                    }
                }
            }

            // Persist index metadata
            crate::catalog::bootstrap::insert_pg_class_row(&disk, index_oid, &ci.index_name, 0);
            let _ = crate::catalog::bootstrap::update_next_oid(&disk, {
                let catalog = self.catalog.lock().unwrap();
                catalog.next_oid()
            });
        }

        Ok(Response::Execution(Tag::new("CREATE INDEX")))
    }

    // -- VACUUM ---------------------------------------------------------------

    fn execute_vacuum(&self, v: crate::parser::VacuumStmt) -> PgWireResult<Response<'static>> {
        let tables: Vec<(u32, String)> = {
            let catalog = self.catalog.lock().unwrap();
            match &v.table_name {
                Some(name) => {
                    let table = catalog.get_table(name).ok_or_else(|| {
                        user_error("42P01", &format!("relation \"{}\" does not exist", name))
                    })?;
                    vec![(table.oid, table.name.clone())]
                }
                None => catalog
                    .all_tables()
                    .iter()
                    .map(|t| (t.oid, t.name.clone()))
                    .collect(),
            }
        };

        let disk = self.disk.lock().unwrap();
        let mut txn = self.txn.lock().unwrap();

        for (oid, _) in &tables {
            let num_pages = disk.num_pages(*oid);
            let mut page = [0u8; PAGE_SIZE];
            let mut all_frozen = true;
            for page_id in 0..num_pages {
                disk.read_page(*oid, page_id, &mut page);
                let compacted = heap::compact_page(&mut page, txn.clog());
                let frozen = heap::freeze_tuples(&mut page, txn.clog());
                if compacted > 0 || frozen > 0 {
                    disk.write_page(*oid, page_id, &page);
                }
                // Update FSM with current free space
                fsm::update(&disk, *oid, page_id, fsm::page_free_space(&page));
                // Check if all tuples on page are frozen
                let n = heap::num_items(&page);
                let page_all_frozen = n > 0
                    && (0..n).all(|i| {
                        let item_id_off = 28 + (i as usize) * 4;
                        let item_id = u32::from_le_bytes(
                            page[item_id_off..item_id_off + 4].try_into().unwrap(),
                        );
                        let offset = (item_id & 0x7FFF) as usize;
                        let flags = ((item_id >> 15) & 0x3) as u8;
                        if flags != 1 || offset == 0 {
                            return true; // dead/unused -- skip
                        }
                        let xmin = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
                        xmin == 2 // FROZEN_XID
                    });
                if page_all_frozen {
                    vm::set_frozen(&disk, *oid, page_id);
                }
                if !page_all_frozen {
                    all_frozen = false;
                }
            }
            let _ = all_frozen; // suppress unused warning
        }

        Ok(Response::Execution(Tag::new("VACUUM")))
    }
}

// -- Expression evaluation (for UPDATE/DELETE WHERE) -----------------------

fn eval_expr(expr: &Expr, row: &[Datum], columns: &[Column]) -> Datum {
    match expr {
        Expr::Integer(i) => Datum::Int4(*i as i32),
        Expr::Float(f) => Datum::Float8(*f),
        Expr::StringLiteral(s) => Datum::Text(s.clone()),
        Expr::Bool(b) => Datum::Bool(*b),
        Expr::Null => Datum::Null,
        Expr::ColumnRef(name) => columns
            .iter()
            .position(|col| col.name == *name)
            .map(|i| row[i].clone())
            .unwrap_or(Datum::Null),
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
            if r == 0 {
                Datum::Null
            } else {
                wrap(l / r)
            }
        }
        BinOp::Mod => {
            if r == 0 {
                Datum::Null
            } else {
                wrap(l % r)
            }
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
                Datum::Null
            } else {
                wrap(l / r)
            }
        }
        BinOp::Mod => {
            if r == 0.0 {
                Datum::Null
            } else {
                wrap(l % r)
            }
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
    wal: &mut crate::wal::writer::WalWriter,
    oid: u32,
    tuple: &[u8],
    xid: u32,
) -> PgWireResult<(u32, u16)> {
    let num_pages = disk.num_pages(oid);
    let mut page = [0u8; PAGE_SIZE];

    // Try FSM first to find a page with enough space
    let (page_id, item_idx) = if let Some(target) = fsm::search(disk, oid, tuple.len() + 4) {
        disk.read_page(oid, target, &mut page);
        if let Ok(idx) = heap::insert_tuple(&mut page, tuple, target) {
            (target, idx)
        } else {
            // FSM was stale; fall through to append
            insert_new_or_last(disk, oid, tuple, num_pages, &mut page)?
        }
    } else if num_pages == 0 {
        heap::init_page(&mut page);
        let idx = heap::insert_tuple(&mut page, tuple, 0)
            .map_err(|()| user_error("42000", "Tuple too large for page"))?;
        (0u32, idx)
    } else {
        insert_new_or_last(disk, oid, tuple, num_pages, &mut page)?
    };

    let wal_data = wal_record::build_heap_insert_data(oid, page_id, item_idx, tuple);
    let rec = WalRecord {
        xl_xid: xid,
        xl_info: wal_record::XLOG_HEAP_INSERT,
        xl_rmid: wal_record::RM_HEAP_ID,
        data: wal_data,
    };
    let lsn = wal.append(&rec);
    wal.flush();
    heap::set_page_lsn(&mut page, lsn);
    disk.write_page(oid, page_id, &page);

    // Update FSM with remaining free space
    fsm::update(disk, oid, page_id, fsm::page_free_space(&page));
    // Clear VM frozen bit since we just modified this page
    vm::clear_frozen(disk, oid, page_id);

    Ok((page_id, item_idx))
}

fn insert_new_or_last(
    disk: &DiskManager,
    oid: u32,
    tuple: &[u8],
    num_pages: u32,
    page: &mut [u8; PAGE_SIZE],
) -> PgWireResult<(u32, u16)> {
    let last = num_pages - 1;
    disk.read_page(oid, last, page);
    if let Ok(idx) = heap::insert_tuple(page, tuple, last) {
        Ok((last, idx))
    } else {
        let new_id = num_pages;
        heap::init_page(page);
        let idx = heap::insert_tuple(page, tuple, new_id)
            .map_err(|()| user_error("42000", "Tuple too large for page"))?;
        Ok((new_id, idx))
    }
}

fn expr_to_datum(expr: &Expr, col: &Column) -> PgWireResult<Datum> {
    match expr {
        Expr::Null => Ok(Datum::Null),
        Expr::Bool(b) => match col.type_id {
            TypeId::Bool => Ok(Datum::Bool(*b)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::Integer(i) => match col.type_id {
            TypeId::Int2 => Ok(Datum::Int2(*i as i16)),
            TypeId::Int4 => Ok(Datum::Int4(*i as i32)),
            TypeId::Int8 => Ok(Datum::Int8(*i)),
            TypeId::Float4 => Ok(Datum::Float4(*i as f32)),
            TypeId::Float8 => Ok(Datum::Float8(*i as f64)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::Float(f) => match col.type_id {
            TypeId::Float4 => Ok(Datum::Float4(*f as f32)),
            TypeId::Float8 => Ok(Datum::Float8(*f)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::StringLiteral(s) => match col.type_id {
            TypeId::Text => {
                // char(n): pad with spaces to typmod length
                let val = if col.typmod > 0 {
                    format!("{:<width$}", s, width = col.typmod as usize)
                } else {
                    s.clone()
                };
                Ok(Datum::Text(val))
            }
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

fn user_error_with_position(code: &str, msg: &str, pos: Option<usize>) -> PgWireError {
    let mut info =
        pgwire::error::ErrorInfo::new("ERROR".to_owned(), code.to_owned(), msg.to_owned());
    if let Some(p) = pos {
        info.position = Some(p.to_string());
    }
    PgWireError::UserError(Box::new(info))
}

/// Find 1-indexed byte position of a bare column name in the SQL.
fn find_column_position(sql: &str, qualified_col: &str, in_having: bool) -> Option<usize> {
    let bare = qualified_col.rsplit('.').next()?;
    let upper = sql.to_ascii_uppercase();
    let search_start = if in_having {
        upper.find("HAVING").map(|i| i + 6)?
    } else {
        upper.find("SELECT").map(|i| i + 6)?
    };
    let search_end = if !in_having {
        upper.find("FROM").unwrap_or(sql.len())
    } else {
        sql.len()
    };
    let search_area = &sql[search_start..search_end];
    for (i, _) in search_area.match_indices(bare) {
        let abs_pos = search_start + i;
        let before = if abs_pos > 0 {
            sql.as_bytes()[abs_pos - 1]
        } else {
            b' '
        };
        let after_pos = abs_pos + bare.len();
        let after = if after_pos < sql.len() {
            sql.as_bytes()[after_pos]
        } else {
            b' '
        };
        if !before.is_ascii_alphanumeric()
            && before != b'_'
            && !after.is_ascii_alphanumeric()
            && after != b'_'
        {
            return Some(abs_pos + 1); // 1-indexed
        }
    }
    None
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
        return user_error("42P01", &format!("relation \"{}\" does not exist", table));
    }

    // GROUP BY violation: projection references non-aggregate column
    if let Some(rest) = msg.strip_prefix(
        "Error during planning: Projection references non-aggregate values: Expression ",
    ) {
        if let Some(col) = rest.split(" could not").next() {
            let pg_msg = format!(
                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                col
            );
            let pos = find_column_position(sql, col, false);
            return user_error_with_position("42803", &pg_msg, pos);
        }
    }

    // GROUP BY violation: HAVING references non-aggregate column
    if let Some(rest) = msg.strip_prefix("Error during planning: HAVING clause references: ") {
        if let Some(expr_str) = rest
            .strip_suffix(" must appear in the GROUP BY clause or be used in an aggregate function")
        {
            let col = expr_str
                .split(|c: char| !c.is_alphanumeric() && c != '.' && c != '_')
                .next()
                .unwrap_or(expr_str);
            let pg_msg = format!(
                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                col
            );
            let pos = find_column_position(sql, col, true);
            return user_error_with_position("42803", &pg_msg, pos);
        }
    }

    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        msg,
    )))
}

// -- Constant HAVING rewrite ------------------------------------------------

fn is_constant_expr(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(_) => true,
        ast::Expr::BinaryOp { left, right, .. } => {
            is_constant_expr(left) && is_constant_expr(right)
        }
        ast::Expr::UnaryOp { expr, .. } => is_constant_expr(expr),
        ast::Expr::Nested(inner) => is_constant_expr(inner),
        _ => false,
    }
}

fn eval_constant_i64(expr: &ast::Expr) -> Option<i64> {
    match expr {
        ast::Expr::Value(ast::Value::Number(s, _)) => s.parse().ok(),
        ast::Expr::Nested(inner) => eval_constant_i64(inner),
        _ => None,
    }
}

fn eval_constant_bool(expr: &ast::Expr) -> Option<bool> {
    match expr {
        ast::Expr::Value(ast::Value::Boolean(b)) => Some(*b),
        ast::Expr::BinaryOp { left, op, right } => {
            let l = eval_constant_i64(left)?;
            let r = eval_constant_i64(right)?;
            Some(match op {
                ast::BinaryOperator::Lt => l < r,
                ast::BinaryOperator::Gt => l > r,
                ast::BinaryOperator::Eq => l == r,
                ast::BinaryOperator::LtEq => l <= r,
                ast::BinaryOperator::GtEq => l >= r,
                ast::BinaryOperator::NotEq => l != r,
                _ => return None,
            })
        }
        ast::Expr::Nested(inner) => eval_constant_bool(inner),
        _ => None,
    }
}

fn select_has_only_literals(select: &ast::Select) -> bool {
    select.projection.iter().all(|item| match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            is_constant_expr(expr)
        }
        _ => false,
    })
}

/// Rewrite queries with constant HAVING clauses that DataFusion rejects.
/// Returns Some(rewritten_sql) if rewriting was needed, None otherwise.
fn rewrite_constant_having(sql: &str) -> Option<String> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let stmt = stmts.into_iter().next()?;
    let ast::Statement::Query(mut query) = stmt else {
        return None;
    };

    // Check for constant HAVING
    let result = {
        let ast::SetExpr::Select(ref select) = *query.body else {
            return None;
        };
        let having = select.having.as_ref()?;
        if !is_constant_expr(having) {
            return None;
        }
        eval_constant_bool(having)?
    };

    let ast::SetExpr::Select(ref mut select) = *query.body else {
        unreachable!()
    };
    select.having = None;

    if result {
        // Constant-true: return 1 row (implicit single group)
        if select_has_only_literals(select) {
            select.from.clear();
            select.selection = None;
        } else {
            query.limit = Some(ast::Expr::Value(ast::Value::Number("1".to_string(), false)));
        }
    } else {
        // Constant-false: return 0 rows
        let false_expr = ast::Expr::Value(ast::Value::Boolean(false));
        select.selection = Some(match select.selection.take() {
            Some(existing) => ast::Expr::BinaryOp {
                left: Box::new(existing),
                op: ast::BinaryOperator::And,
                right: Box::new(false_expr),
            },
            None => false_expr,
        });
    }

    Some(query.to_string())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn execute_select_integer() {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::new(tmp.path());
        let resp = db.execute_sql("SELECT 1;").await.unwrap();
        assert!(matches!(resp, Response::Query(_)));
    }
}
