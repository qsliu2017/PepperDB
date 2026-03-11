//! Query executor: routes SELECT to DataFusion, handles DDL/DML directly.

use std::any::Any;
use std::collections::HashMap;
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

        match stmt {
            ast::Statement::Query(_) => {
                // Intercept pg_input_error_info() table function
                if let Some(resp) = self.try_pg_input_error_info(sql) {
                    return resp;
                }
                self.execute_select_df(sql).await
            }
            ast::Statement::CreateTable(ref ct) if ct.query.is_some() => {
                self.execute_create_table_as(sql, stmt).await
            }
            ast::Statement::Delete(ref del) => {
                // Check for invalid table name reference when alias is defined
                if let Some(err) = check_delete_alias_conflict(del, sql) {
                    return Err(err);
                }
                let our_stmt = parser::convert_statement(stmt)
                    .map_err(|e| add_input_syntax_position(e, sql))?;
                self.execute(our_stmt)
            }
            _ => {
                let our_stmt = parser::convert_statement(stmt).map_err(|e| {
                    // Add cursor position for "invalid input syntax" errors from typed string parsing
                    add_input_syntax_position(e, sql)
                })?;
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

    // -- pg_input_error_info table function ----------------------------------

    /// Handle `SELECT * FROM pg_input_error_info(value, type)` as a special case.
    fn try_pg_input_error_info(&self, sql: &str) -> Option<PgWireResult<Response<'static>>> {
        let lower = sql.to_ascii_lowercase();
        if !lower.contains("pg_input_error_info") {
            return None;
        }
        // Extract arguments: pg_input_error_info('value', 'type')
        // Use paren-depth matching to handle types like varchar(4).
        let start = lower.find("pg_input_error_info(")?;
        let args_start = start + "pg_input_error_info(".len();
        let args_end = {
            let mut depth = 1i32;
            let mut end = None;
            for (i, ch) in sql[args_start..].char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(args_start + i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            end?
        };
        let args_str = &sql[args_start..args_end];
        let parts: Vec<&str> = args_str.split(',').collect();
        if parts.len() != 2 {
            return None;
        }
        let value = parts[0].trim().trim_matches('\'');
        let type_name = parts[1].trim().trim_matches('\'');

        let (message, sql_error_code) = if !crate::udfs::validate_input_public(value, type_name) {
            let tn_lower = type_name.to_ascii_lowercase();
            if tn_lower.starts_with("varchar(") || tn_lower.starts_with("character varying(") {
                let msg = format!(
                    "value too long for type character varying({})",
                    &type_name[type_name.find('(').unwrap() + 1..type_name.find(')').unwrap()]
                );
                (msg, "22001".to_string())
            } else if tn_lower.starts_with("char(") || tn_lower.starts_with("character(") {
                let msg = format!(
                    "value too long for type character({})",
                    &type_name[type_name.find('(').unwrap() + 1..type_name.find(')').unwrap()]
                );
                (msg, "22001".to_string())
            } else {
                let msg = match type_name {
                    "bool" | "boolean" => {
                        format!("invalid input syntax for type boolean: \"{}\"", value)
                    }
                    _ => format!("invalid input syntax for type {}: \"{}\"", type_name, value),
                };
                (msg, "22P02".to_string())
            }
        } else {
            (String::new(), String::new())
        };

        let schema = Arc::new(vec![
            FieldInfo::new("message".into(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("detail".into(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("hint".into(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new(
                "sql_error_code".into(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            ),
        ]);
        let schema2 = schema.clone();
        let row = {
            let mut encoder = DataRowEncoder::new(schema2);
            encoder.encode_field(&message).ok()?;
            encoder.encode_field(&"").ok()?; // detail
            encoder.encode_field(&"").ok()?; // hint
            encoder.encode_field(&sql_error_code).ok()?;
            encoder.finish().ok()?
        };
        Some(Ok(Response::Query(QueryResponse::new(
            schema,
            stream::iter(vec![Ok(row)]),
        ))))
    }

    // -- DataFusion SELECT -------------------------------------------------

    async fn execute_select_df(&self, sql: &str) -> PgWireResult<Response<'static>> {
        // Rewrite PG's internal "char" type casts to pg_char_cast() UDF.
        let rewritten_char = rewrite_pg_char_casts(sql);
        let sql = rewritten_char.as_deref().unwrap_or(sql);

        // Rewrite constant HAVING clauses (DataFusion doesn't support them).
        let rewritten_having = rewrite_constant_having(sql);
        let sql = rewritten_having.as_deref().unwrap_or(sql);

        // Add aliases for bare CAST expressions so column names match PG.
        let rewritten_cast = rewrite_cast_aliases(sql);
        let sql = rewritten_cast.as_deref().unwrap_or(sql);

        // Rewrite ORDER BY expressions matching GROUP BY keys.
        let rewritten_ob = rewrite_order_by_group_by(sql);
        let (exec_sql, strip_cols) = match &rewritten_ob {
            Some((rw, orig_len)) => (rw.as_str(), Some(*orig_len)),
            None => (sql, None),
        };

        self.register_all_tables()?;

        let result = self.session.sql(exec_sql).await;
        match result {
            Ok(df) => {
                let arrow_schema = df.schema().inner().clone();
                let batches = df.collect().await.map_err(|e| df_to_pg(&e, sql))?;
                if let Some(orig_len) = strip_cols {
                    return batches_to_response_trimmed(batches, &arrow_schema, orig_len);
                }
                batches_to_response(batches, &arrow_schema)
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Projections require unique expression names") {
                    return self.execute_select_df_dedup(sql).await;
                }
                Err(df_to_pg(&e, sql))
            }
        }
    }

    /// Execute a SELECT with duplicate projections by aliasing duplicates.
    async fn execute_select_df_dedup(&self, sql: &str) -> PgWireResult<Response<'static>> {
        let (rewritten, dup_map) = rewrite_duplicate_projections(sql).ok_or_else(|| {
            user_error("XX000", "internal: failed to rewrite duplicate projections")
        })?;
        let df = self
            .session
            .sql(&rewritten)
            .await
            .map_err(|e| df_to_pg(&e, sql))?;
        let arrow_schema = df.schema().inner().clone();
        let batches = df.collect().await.map_err(|e| df_to_pg(&e, sql))?;

        // Fix column names: replace aliases with their first occurrence's name
        let fields: Vec<Arc<Field>> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if let Some(&first_idx) = dup_map.get(&i) {
                    Arc::new(Field::new(
                        arrow_schema.field(first_idx).name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    ))
                } else {
                    f.clone()
                }
            })
            .collect();
        let fixed_schema = Schema::new(fields);
        batches_to_response(batches, &fixed_schema)
    }

    // -- CREATE TABLE AS SELECT (CTAS) ----------------------------------------

    async fn execute_create_table_as(
        &self,
        sql: &str,
        stmt: ast::Statement,
    ) -> PgWireResult<Response<'static>> {
        let ast::Statement::CreateTable(ct) = stmt else {
            unreachable!()
        };
        let table_name = ct.name.to_string().to_ascii_lowercase();
        let select_sql = ct.query.unwrap().to_string();

        // Apply ORDER BY / GROUP BY rewrite if needed
        let rewritten_ob = rewrite_order_by_group_by(&select_sql);
        let (exec_sql, orig_col_count) = match &rewritten_ob {
            Some((rw, orig_len)) => (rw.as_str(), Some(*orig_len)),
            None => (select_sql.as_str(), None),
        };

        // Execute the SELECT via DataFusion
        self.register_all_tables()?;
        let df = self
            .session
            .sql(exec_sql)
            .await
            .map_err(|e| df_to_pg(&e, sql))?;
        let arrow_schema = df.schema().inner().clone();
        let batches = df.collect().await.map_err(|e| df_to_pg(&e, sql))?;

        // Derive columns from Arrow schema (only original columns, not rewrite extras)
        let col_count = orig_col_count.unwrap_or(arrow_schema.fields().len());
        let columns: Vec<Column> = arrow_schema
            .fields()
            .iter()
            .take(col_count)
            .enumerate()
            .map(|(i, f)| {
                let type_id = arrow_to_type_id(f.data_type());
                Column {
                    name: f.name().clone(),
                    type_id,
                    col_num: i as u16,
                    typmod: -1,
                }
            })
            .collect();

        let oid = self.create_table_catalog(&table_name, &columns)?;

        let mut count = 0u64;
        let disk = self.disk.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut txn = self.txn.lock().unwrap();
        let xid = txn.assign_xid();

        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let datums: Vec<Datum> = columns
                    .iter()
                    .enumerate()
                    .map(|(col_idx, col)| {
                        arrow_value_to_datum(batch.column(col_idx), row_idx, col.type_id)
                    })
                    .collect();
                let tuple = heap::build_tuple_with_xid(&datums, &columns, xid, false);
                insert_tuple_to_heap(&disk, &mut wal, oid, &tuple, xid)?;
                count += 1;
            }
        }
        txn.commit(xid);

        Ok(Response::Execution(Tag::new(&format!("SELECT {}", count))))
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
                toast_store: self.toast_store.clone(),
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
    toast_store: Arc<Mutex<HashMap<u64, String>>>,
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
                    let datums = resolve_toast_markers(datums, &self.toast_store);
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

fn arrow_to_type_id(dt: &DataType) -> TypeId {
    match dt {
        DataType::Boolean => TypeId::Bool,
        DataType::Int16 => TypeId::Int2,
        DataType::Int32 => TypeId::Int4,
        DataType::Int64 => TypeId::Int8,
        DataType::Float32 => TypeId::Float4,
        DataType::Float64 => TypeId::Float8,
        DataType::Utf8 | DataType::LargeUtf8 => TypeId::Text,
        _ => TypeId::Text,
    }
}

fn arrow_value_to_datum(array: &ArrayRef, row: usize, type_id: TypeId) -> Datum {
    if array.is_null(row) {
        return Datum::Null;
    }
    match type_id {
        TypeId::Bool => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Datum::Bool(a.value(row))
        }
        TypeId::Int2 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Datum::Int2(a.value(row))
        }
        TypeId::Int4 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Datum::Int4(a.value(row))
        }
        TypeId::Int8 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Datum::Int8(a.value(row))
        }
        TypeId::Float4 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Datum::Float4(a.value(row))
        }
        TypeId::Float8 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Datum::Float8(a.value(row))
        }
        TypeId::Text => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Datum::Text(a.value(row).to_string())
        }
    }
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
            // - expressions with embedded type constructors like "t.a / Int64(2)" -> "?column?"
            // - boolean expressions like "t.a AND t.b" -> "?column?"
            // - CAST(... AS Type) -> lowercase type name (e.g. "bool")
            let name = df_column_to_pg_name(f.name());
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

/// Like batches_to_response but only includes the first `n` columns (strips extras).
/// Safe because batches_to_response iterates by schema length, not batch width.
fn batches_to_response_trimmed(
    batches: Vec<RecordBatch>,
    arrow_schema: &Schema,
    n: usize,
) -> PgWireResult<Response<'static>> {
    let trimmed_fields: Vec<Arc<Field>> = arrow_schema.fields().iter().take(n).cloned().collect();
    let trimmed_schema = Schema::new(trimmed_fields);
    batches_to_response(batches, &trimmed_schema)
}

/// Map a DataFusion column name to PostgreSQL display name.
fn df_column_to_pg_name(name: &str) -> String {
    // Boolean/logical expressions -> ?column?
    if name.contains(" AND ") || name.contains(" OR ") {
        return "?column?".to_owned();
    }
    // CAST(... AS Type) -> lowercase type name
    if let Some(cast_type) = extract_cast_type(name) {
        return cast_type;
    }
    if !name.contains('(') {
        return name.to_owned();
    }
    // Function calls: name starts with a simple lowercase identifier before "("
    // (even if arguments contain type constructors like Utf8("..."))
    if let Some(func) = name.split('(').next() {
        if func.starts_with(|c: char| c.is_ascii_lowercase())
            && func.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            // DataFusion canonicalizes some function names; map back to PG names
            return match func {
                "character_length" => "char_length".to_owned(),
                _ => func.to_owned(),
            };
        }
    }
    // Type constructors, expressions, or uppercase names -> ?column?
    "?column?".to_owned()
}

/// Extract the target type name from a CAST expression column name.
/// "CAST(Int64(0) AS Boolean)" -> Some("bool")
fn extract_cast_type(name: &str) -> Option<String> {
    let rest = name.strip_prefix("CAST(")?;
    let as_idx = rest.rfind(" AS ")?;
    let type_part = rest[as_idx + 4..].trim_end_matches(')').trim();
    Some(
        match type_part {
            "Boolean" => "bool",
            "Int16" => "int2",
            "Int32" => "int4",
            "Int64" => "int8",
            "Float32" => "float4",
            "Float64" => "float8",
            "Utf8" | "LargeUtf8" => "text",
            _ => return None,
        }
        .to_owned(),
    )
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

    /// Create table in catalog + disk, returning the assigned OID.
    fn create_table_catalog(&self, table_name: &str, columns: &[Column]) -> PgWireResult<u32> {
        self.create_table_catalog_with_serials(table_name, columns, Vec::new())
    }

    fn create_table_catalog_with_serials(
        &self,
        table_name: &str,
        columns: &[Column],
        serial_columns: Vec<usize>,
    ) -> PgWireResult<u32> {
        let mut catalog = self.catalog.lock().unwrap();
        let oid = catalog
            .create_table_with_serials(table_name, columns.to_vec(), serial_columns)
            .map_err(|e| user_error("42P07", &e))?;
        let disk = self.disk.lock().unwrap();
        disk.create_heap_file(oid);
        bootstrap::insert_pg_class_row(&disk, oid, table_name, columns.len() as i16);
        bootstrap::insert_pg_attribute_rows(&disk, oid, columns);
        let _ = bootstrap::update_next_oid(&disk, catalog.next_oid());
        Ok(oid)
    }

    fn execute_create_table(
        &self,
        ct: crate::parser::CreateTableStmt,
    ) -> PgWireResult<Response<'static>> {
        let serial_columns: Vec<usize> = ct
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_serial)
            .map(|(i, _)| i)
            .collect();
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

        // TEMP tables shadow any existing table with the same name
        if ct.temporary {
            let mut catalog = self.catalog.lock().unwrap();
            catalog.shadow_table(&ct.table_name);
            drop(catalog);
        }

        match self.create_table_catalog_with_serials(&ct.table_name, &columns, serial_columns) {
            Ok(_) => Ok(Response::Execution(Tag::new("CREATE TABLE"))),
            Err(e) if ct.if_not_exists => {
                let _ = e;
                Ok(Response::Execution(Tag::new("CREATE TABLE")))
            }
            Err(e) => Err(e),
        }
    }

    // -- INSERT -----------------------------------------------------------

    fn execute_insert(&self, ins: crate::parser::InsertStmt) -> PgWireResult<Response<'static>> {
        let (oid, columns, serial_columns, serial_counter) = {
            let catalog = self.catalog.lock().unwrap();
            let table = catalog.get_table(&ins.table_name).ok_or_else(|| {
                user_error(
                    "42P01",
                    &format!("relation \"{}\" does not exist", ins.table_name),
                )
            })?;
            (
                table.oid,
                table.columns.clone(),
                table.serial_columns.clone(),
                table.serial_counter.clone(),
            )
        };

        // Build column index mapping when INSERT specifies columns
        let col_map: Option<Vec<usize>> = ins
            .columns
            .as_ref()
            .map(|insert_cols| {
                insert_cols
                    .iter()
                    .map(|name| {
                        columns.iter().position(|c| c.name == *name).ok_or_else(|| {
                            user_error("42703", &format!("column \"{}\" does not exist", name))
                        })
                    })
                    .collect::<PgWireResult<Vec<_>>>()
            })
            .transpose()?;

        let indexes: Vec<_> = {
            let catalog = self.catalog.lock().unwrap();
            catalog
                .get_indexes_for_table(oid)
                .into_iter()
                .cloned()
                .collect()
        };

        let mut count = 0u64;
        let disk = self.disk.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut txn = self.txn.lock().unwrap();
        let xid = txn.assign_xid();

        for row_exprs in &ins.values {
            if col_map.is_none() && row_exprs.len() != columns.len() {
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

            let datums: Vec<Datum> = if let Some(ref map) = col_map {
                // Partial INSERT: build full datum vector, fill defaults
                let mut datums = vec![Datum::Null; columns.len()];
                for (expr_idx, &col_idx) in map.iter().enumerate() {
                    datums[col_idx] = expr_to_datum(&row_exprs[expr_idx], &columns[col_idx])?;
                }
                // Auto-fill SERIAL columns that are still NULL
                for &si in &serial_columns {
                    if datums[si] == Datum::Null {
                        let val = serial_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        datums[si] = Datum::Int4(val);
                    }
                }
                datums
            } else {
                row_exprs
                    .iter()
                    .zip(columns.iter())
                    .map(|(expr, col)| expr_to_datum(expr, col))
                    .collect::<PgWireResult<Vec<_>>>()?
            };

            // TOAST: if tuple would be too large for a page, replace large text with markers
            let datums = toast_if_needed(datums, &columns, self);

            let tuple = heap::build_tuple_with_xid(&datums, &columns, xid, false);
            let (pg_id, item_idx) = insert_tuple_to_heap(&disk, &mut wal, oid, &tuple, xid)?;

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
        Expr::FunctionCall { name, args } => {
            let arg_vals: Vec<Datum> = args.iter().map(|a| eval_expr(a, row, columns)).collect();
            eval_function(name, &arg_vals)
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

/// Resolve TOAST markers in a tuple's datums, replacing markers with full values.
fn resolve_toast_markers(datums: Vec<Datum>, store: &Mutex<HashMap<u64, String>>) -> Vec<Datum> {
    datums
        .into_iter()
        .map(|d| match &d {
            Datum::Text(s) if s.starts_with(crate::TOAST_MARKER) => {
                let id_str = &s[crate::TOAST_MARKER.len()..];
                if let Ok(id) = id_str.parse::<u64>() {
                    if let Some(val) = store.lock().unwrap().get(&id) {
                        return Datum::Text(val.clone());
                    }
                }
                d
            }
            _ => d,
        })
        .collect()
}

/// Max tuple data that fits on a page (conservative estimate).
const MAX_TUPLE_SIZE: usize = PAGE_SIZE - 128;

fn estimate_tuple_size(datums: &[Datum], columns: &[Column]) -> usize {
    datums
        .iter()
        .zip(columns.iter())
        .map(|(d, c)| match d {
            Datum::Text(s) => s.len() + 4, // varlena header
            _ => c.type_id.len().max(4) as usize,
        })
        .sum::<usize>()
        + 32 // tuple header + null bitmap
}

/// Replace large text values with TOAST markers if tuple would be too large.
fn toast_if_needed(mut datums: Vec<Datum>, columns: &[Column], db: &Database) -> Vec<Datum> {
    if estimate_tuple_size(&datums, columns) <= MAX_TUPLE_SIZE {
        return datums;
    }
    // Toast the largest text values first until we fit
    loop {
        let (largest_idx, largest_len) = datums
            .iter()
            .enumerate()
            .filter_map(|(i, d)| match d {
                Datum::Text(s) if !s.starts_with(crate::TOAST_MARKER) => Some((i, s.len())),
                _ => None,
            })
            .max_by_key(|(_, len)| *len)
            .unwrap_or((0, 0));
        if largest_len < 64 {
            break;
        }
        if let Datum::Text(val) = std::mem::replace(&mut datums[largest_idx], Datum::Null) {
            let marker = db.toast_store_value(val);
            datums[largest_idx] = Datum::Text(marker);
        }
        if estimate_tuple_size(&datums, columns) <= MAX_TUPLE_SIZE {
            break;
        }
    }
    datums
}

fn eval_function(name: &str, args: &[Datum]) -> Datum {
    match name {
        "repeat" if args.len() == 2 => {
            if let (Datum::Text(s), n) = (&args[0], &args[1]) {
                let count = match n {
                    Datum::Int4(i) => *i as usize,
                    Datum::Int8(i) => *i as usize,
                    _ => return Datum::Null,
                };
                Datum::Text(s.repeat(count))
            } else {
                Datum::Null
            }
        }
        _ => Datum::Null,
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
            TypeId::Text => coerce_text_value(&i.to_string(), col.typmod),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::Float(f) => match col.type_id {
            TypeId::Float4 => Ok(Datum::Float4(*f as f32)),
            TypeId::Float8 => Ok(Datum::Float8(*f)),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::StringLiteral(s) => match col.type_id {
            TypeId::Text => coerce_text_value(s, col.typmod),
            _ => Err(user_error("42804", "Type mismatch in expression")),
        },
        Expr::FunctionCall { name, args } => {
            let arg_vals: Vec<Datum> = args
                .iter()
                .map(|a| Ok(eval_expr(a, &[], &[])))
                .collect::<PgWireResult<Vec<_>>>()?;
            Ok(eval_function(name, &arg_vals))
        }
        _ => Err(user_error("42804", "Type mismatch in expression")),
    }
}

/// Apply char(n)/varchar(n) length enforcement and padding.
///
/// typmod > 0  → char(n): trim trailing spaces, check length, pad to n
/// typmod < -1 → varchar(n): trim trailing spaces, check length, store trimmed
/// typmod = -1 → text: store as-is
fn coerce_text_value(s: &str, typmod: i32) -> PgWireResult<Datum> {
    if typmod > 0 {
        // char(n)
        let max_len = typmod as usize;
        let trimmed = s.trim_end_matches(' ');
        if trimmed.len() > max_len {
            return Err(user_error(
                "22001",
                &format!("value too long for type character({})", max_len),
            ));
        }
        Ok(Datum::Text(format!("{:<width$}", trimmed, width = max_len)))
    } else if typmod < -1 {
        // varchar(n)
        let max_len = -(typmod + 1) as usize;
        let trimmed = s.trim_end_matches(' ');
        if trimmed.len() > max_len {
            return Err(user_error(
                "22001",
                &format!("value too long for type character varying({})", max_len),
            ));
        }
        Ok(Datum::Text(trimmed.to_string()))
    } else {
        Ok(Datum::Text(s.to_string()))
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

/// Add cursor position to "invalid input syntax" errors from parser.
fn add_input_syntax_position(err: PgWireError, sql: &str) -> PgWireError {
    if let PgWireError::UserError(ref info) = err {
        if info.message.starts_with("invalid input syntax for type") {
            // Extract the value from the error: ...type boolean: "XXX"
            if let Some(start) = info.message.rfind('"') {
                let val_start = info.message[..start].rfind('"');
                if let Some(vs) = val_start {
                    let val = &info.message[vs + 1..start];
                    // Find the string literal position in SQL
                    if let Some(pos) = sql.find(&format!("'{}'", val)).map(|p| p + 1) {
                        let mut new_info = pgwire::error::ErrorInfo::new(
                            info.severity.clone(),
                            info.code.clone(),
                            info.message.clone(),
                        );
                        new_info.position = Some(pos.to_string());
                        return PgWireError::UserError(Box::new(new_info));
                    }
                }
            }
        }
    }
    err
}

/// Extract the original string literal from SQL that was cast to boolean.
/// For `'  tru e '::text::boolean`, returns `"  tru e "`.
fn extract_cast_source_literal(sql: &str, trimmed_val: &str) -> Option<String> {
    // Find a string literal in the SQL that contains the trimmed value
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let start = i + 1;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    let literal = &sql[start..i];
                    if literal.trim() == trimmed_val && literal != trimmed_val {
                        return Some(literal.to_string());
                    }
                    break;
                }
                i += 1;
            }
        }
        i += 1;
    }
    None
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
    if let Some(pos) = find_bare_in_range(sql, bare, search_start, search_end) {
        return Some(pos);
    }
    // Fallback: search ORDER BY region (for GROUP BY violations in ORDER BY)
    if !in_having {
        if let Some(order_start) = upper.find("ORDER BY").map(|i| i + 8) {
            return find_bare_in_range(sql, bare, order_start, sql.len());
        }
    }
    None
}

fn find_bare_in_range(sql: &str, bare: &str, start: usize, end: usize) -> Option<usize> {
    let search_area = &sql[start..end];
    for (i, _) in search_area.match_indices(bare) {
        let abs_pos = start + i;
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

/// Find 1-indexed position of a bare word after a keyword like "GROUP BY".
fn find_bare_word_position(sql: &str, word: &str, after_keyword: &str) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let start = upper.find(&after_keyword.to_ascii_uppercase())? + after_keyword.len();
    find_bare_in_range(sql, word, start, sql.len())
}

/// Find 1-indexed position of the LAST bare occurrence of a word in SQL.
/// Excludes qualified references like `x.b` (preceded by `.`).
fn find_last_bare_position(sql: &str, word: &str) -> Option<usize> {
    let mut last = None;
    for (i, _) in sql.match_indices(word) {
        let before = if i > 0 { sql.as_bytes()[i - 1] } else { b' ' };
        let after_pos = i + word.len();
        let after = if after_pos < sql.len() {
            sql.as_bytes()[after_pos]
        } else {
            b' '
        };
        if !before.is_ascii_alphanumeric()
            && before != b'_'
            && before != b'.'
            && !after.is_ascii_alphanumeric()
            && after != b'_'
        {
            last = Some(i + 1);
        }
    }
    last
}

fn df_to_pg(e: &datafusion::error::DataFusionError, sql: &str) -> PgWireError {
    let msg = e.to_string();

    // Translate DataFusion errors to PostgreSQL-style messages

    // Boolean cast errors: "Arrow error: Cast error: Cannot cast value 'X' to value of Boolean type"
    if msg.contains("Cannot cast value") && msg.contains("Boolean") {
        if let Some(val) = msg
            .split("Cannot cast value '")
            .nth(1)
            .and_then(|s| s.split("' to value").next())
        {
            // Check if this is a `bool 'value'` typed-string syntax (has position)
            // or a `::boolean` cast (no position)
            let is_typed_string = sql
                .to_ascii_lowercase()
                .contains(&format!("bool '{}'", val.to_ascii_lowercase()));
            if is_typed_string {
                let pg_msg = format!("invalid input syntax for type boolean: \"{}\"", val);
                // Find position of the string literal after "bool"
                let lower = sql.to_ascii_lowercase();
                let pos = lower
                    .find(&format!("bool '{}'", val.to_ascii_lowercase()))
                    .map(|p| p + 5 + 1); // skip "bool " and 1-index
                return user_error_with_position("22P02", &pg_msg, pos);
            } else {
                // ::boolean cast - use original (untrimmed) value, no position
                let original_val = extract_cast_source_literal(sql, val).unwrap_or(val.to_string());
                let pg_msg = format!(
                    "invalid input syntax for type boolean: \"{}\"",
                    original_val
                );
                return user_error("22P02", &pg_msg);
            }
        }
    }

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

    // GROUP BY position out of range
    if msg.contains("Cannot find column with position") {
        if let Some(n_str) = msg
            .split("position ")
            .nth(1)
            .and_then(|s| s.split(' ').next())
        {
            let pg_msg = format!("GROUP BY position {} is not in select list", n_str);
            let pos = find_bare_word_position(sql, n_str, "GROUP BY");
            return user_error_with_position("42P10", &pg_msg, pos);
        }
    }

    // Ambiguous column reference
    if msg.contains("Ambiguous reference to unqualified field") {
        if let Some(col) = msg.split("field ").nth(1).map(|s| s.trim()) {
            let pg_msg = format!("column reference \"{}\" is ambiguous", col);
            let pos = find_last_bare_position(sql, col);
            return user_error_with_position("42702", &pg_msg, pos);
        }
    }

    // GROUP BY violation: "No field named X" after grouping (ORDER BY column not in GROUP BY)
    if msg.contains("No field named") && msg.contains("Valid fields are") {
        if let Some(rest) = msg.split("No field named ").nth(1) {
            if let Some(col) = rest.split('.').nth(1).and_then(|s| s.split('.').next()) {
                let col = col.trim();
                // Extract table.col for the PG-style message
                let qualified = rest.split('.').take(2).collect::<Vec<_>>().join(".");
                let pg_msg = format!(
                    "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    qualified
                );
                let pos = find_column_position(sql, col, false);
                return user_error_with_position("42803", &pg_msg, pos);
            }
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

// -- Duplicate projection rewrite -------------------------------------------

/// Rewrite SELECT with duplicate expressions to use unique aliases.
/// Returns (rewritten_sql, map of col_index -> first_occurrence_index).
fn rewrite_duplicate_projections(sql: &str) -> Option<(String, HashMap<usize, usize>)> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let mut stmt = stmts.into_iter().next()?;
    let ast::Statement::Query(ref mut query) = stmt else {
        return None;
    };
    let ast::SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };

    let expr_strings: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                e.to_string()
            }
            _ => item.to_string(),
        })
        .collect();

    let mut first_occ: HashMap<String, usize> = HashMap::new();
    let mut dup_map: HashMap<usize, usize> = HashMap::new();
    let mut has_dups = false;

    for (i, key) in expr_strings.iter().enumerate() {
        if let Some(&first) = first_occ.get(key) {
            dup_map.insert(i, first);
            has_dups = true;
        } else {
            first_occ.insert(key.clone(), i);
        }
    }
    if !has_dups {
        return None;
    }

    // Alias duplicate occurrences (second+)
    for &i in dup_map.keys() {
        let alias = format!("__pepper_dup_{}", i);
        let item = &mut select.projection[i];
        if let ast::SelectItem::UnnamedExpr(expr) = item {
            *item = ast::SelectItem::ExprWithAlias {
                expr: expr.clone(),
                alias: ast::Ident::new(alias),
            };
        }
    }

    Some((stmt.to_string(), dup_map))
}

// -- ORDER BY / GROUP BY rewrite -------------------------------------------

/// Rewrite bare CAST expressions to include an alias matching PG column naming.
/// e.g. `SELECT 0::boolean` -> `SELECT 0::boolean AS bool`
fn rewrite_cast_aliases(sql: &str) -> Option<String> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let mut stmt = stmts.into_iter().next()?;
    let ast::Statement::Query(ref mut query) = stmt else {
        return None;
    };
    let ast::SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };
    let mut changed = false;
    for item in &mut select.projection {
        if let ast::SelectItem::UnnamedExpr(ast::Expr::Cast { data_type, .. }) = item {
            let alias = pg_type_alias(data_type);
            if let Some(alias) = alias {
                let orig = std::mem::replace(item, ast::SelectItem::Wildcard(Default::default()));
                if let ast::SelectItem::UnnamedExpr(expr) = orig {
                    *item = ast::SelectItem::ExprWithAlias {
                        expr,
                        alias: ast::Ident::new(alias),
                    };
                    changed = true;
                }
            }
        }
    }
    if changed {
        Some(stmt.to_string())
    } else {
        None
    }
}

/// Map a sqlparser DataType to PG's canonical column alias.
fn pg_type_alias(dt: &ast::DataType) -> Option<&'static str> {
    match dt {
        ast::DataType::Bool | ast::DataType::Boolean => Some("bool"),
        ast::DataType::SmallInt(_) | ast::DataType::Int2(_) => Some("int2"),
        ast::DataType::Int(_) | ast::DataType::Int4(_) | ast::DataType::Integer(_) => Some("int4"),
        ast::DataType::BigInt(_) | ast::DataType::Int8(_) => Some("int8"),
        ast::DataType::Real | ast::DataType::Float4 => Some("float4"),
        ast::DataType::DoublePrecision | ast::DataType::Float8 => Some("float8"),
        ast::DataType::Text => Some("text"),
        _ => None,
    }
}

/// Check if a data type is PG's internal `"char"` type (double-quoted).
fn is_pg_char_type(dt: &ast::DataType) -> bool {
    if let ast::DataType::Custom(name, params) = dt {
        if params.is_empty() {
            let idents = name.0.as_slice();
            if idents.len() == 1 && idents[0].value == "char" && idents[0].quote_style == Some('"')
            {
                return true;
            }
        }
    }
    false
}

/// Build a `pg_char_cast(expr)` function call AST node.
fn make_pg_char_call(expr: ast::Expr) -> ast::Expr {
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName(vec![ast::Ident::new("pg_char_cast")]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: ast::FunctionArguments::None,
        uses_odbc_syntax: false,
    })
}

/// Recursively replace `"char"` type casts with `pg_char_cast()` calls.
fn rewrite_pg_char_expr(expr: &mut ast::Expr) {
    match expr {
        ast::Expr::Cast {
            data_type,
            expr: inner,
            ..
        } => {
            // Recurse into the inner expression first
            rewrite_pg_char_expr(inner);
            if is_pg_char_type(data_type) {
                let inner_owned =
                    std::mem::replace(inner.as_mut(), ast::Expr::Value(ast::Value::Null));
                *expr = make_pg_char_call(inner_owned);
            }
        }
        ast::Expr::Nested(inner) => rewrite_pg_char_expr(inner),
        ast::Expr::UnaryOp { expr: inner, .. } => rewrite_pg_char_expr(inner),
        ast::Expr::BinaryOp { left, right, .. } => {
            rewrite_pg_char_expr(left);
            rewrite_pg_char_expr(right);
        }
        ast::Expr::Function(f) => {
            if let ast::FunctionArguments::List(ref mut list) = f.args {
                for arg in &mut list.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                    | ast::FunctionArg::Named {
                        arg: ast::FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        rewrite_pg_char_expr(e);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Rewrite `"char"` type casts to `pg_char_cast()` UDF calls and add
/// appropriate column aliases. Returns Some(rewritten_sql) if changes made.
fn rewrite_pg_char_casts(sql: &str) -> Option<String> {
    if !sql.contains("\"char\"") {
        return None;
    }
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let mut stmt = stmts.into_iter().next()?;
    let ast::Statement::Query(ref mut query) = stmt else {
        return None;
    };
    let ast::SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };
    let mut changed = false;
    for item in &mut select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(ref mut expr) => {
                // Determine alias from outermost type before rewriting
                let alias = outermost_char_alias(expr);
                rewrite_pg_char_expr(expr);
                if let Some(alias) = alias {
                    let owned =
                        std::mem::replace(item, ast::SelectItem::Wildcard(Default::default()));
                    if let ast::SelectItem::UnnamedExpr(e) = owned {
                        *item = ast::SelectItem::ExprWithAlias {
                            expr: e,
                            alias: ast::Ident::new(alias),
                        };
                        changed = true;
                    }
                } else {
                    changed = true; // still changed if inner rewrite happened
                }
            }
            ast::SelectItem::ExprWithAlias { ref mut expr, .. } => {
                rewrite_pg_char_expr(expr);
                changed = true;
            }
            _ => {}
        }
    }
    if changed {
        Some(stmt.to_string())
    } else {
        None
    }
}

/// Determine the column alias for expressions involving `"char"` casts.
/// Returns "char" if outermost cast is to "char", "text" if outermost
/// is to text (with inner "char"), etc.
fn outermost_char_alias(expr: &ast::Expr) -> Option<&'static str> {
    match expr {
        ast::Expr::Cast {
            data_type,
            expr: inner,
            ..
        } => {
            if is_pg_char_type(data_type) {
                Some("char")
            } else if contains_pg_char_cast(inner) {
                pg_type_alias(data_type)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check if an expression tree contains any `"char"` type cast.
fn contains_pg_char_cast(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Cast {
            data_type,
            expr: inner,
            ..
        } => is_pg_char_type(data_type) || contains_pg_char_cast(inner),
        _ => false,
    }
}

/// Rewrite ORDER BY expressions that match GROUP BY keys but use raw columns
/// that DataFusion can't resolve after grouping. Adds GROUP BY key to SELECT
/// with an alias and rewrites ORDER BY to reference the alias.
/// Returns (rewritten_sql, original_select_count).
fn rewrite_order_by_group_by(sql: &str) -> Option<(String, usize)> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let mut stmt = stmts.into_iter().next()?;
    let ast::Statement::Query(ref mut query) = stmt else {
        return None;
    };
    let order_by = query.order_by.as_mut()?;
    let ast::SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };

    let group_by_exprs: Vec<ast::Expr> = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
        _ => return None,
    };
    if group_by_exprs.is_empty() {
        return None;
    }

    let orig_select_len = select.projection.len();
    let mut aliases: Vec<(ast::Expr, String)> = Vec::new();
    let mut modified = false;

    for item in order_by.exprs.iter_mut() {
        // Skip positional references (e.g. ORDER BY 1)
        if matches!(&item.expr, ast::Expr::Value(ast::Value::Number(..))) {
            continue;
        }
        for gb in &group_by_exprs {
            if matches!(gb, ast::Expr::Value(ast::Value::Number(..))) {
                continue;
            }
            if item.expr == *gb {
                let alias = format!("__pepper_ob_{}", aliases.len());
                aliases.push((gb.clone(), alias.clone()));
                item.expr = ast::Expr::Identifier(ast::Ident::new(&alias));
                modified = true;
                break;
            }
        }
    }
    if !modified {
        return None;
    }

    for (expr, alias) in aliases {
        select.projection.push(ast::SelectItem::ExprWithAlias {
            expr,
            alias: ast::Ident::new(alias),
        });
    }

    Some((stmt.to_string(), orig_select_len))
}

/// Check if a DELETE's WHERE clause references the original table name when an alias is defined.
fn check_delete_alias_conflict(del: &ast::Delete, sql: &str) -> Option<PgWireError> {
    let table_with_joins = match &del.from {
        ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => {
            tables.first()?
        }
    };
    let (table_name, alias) = match &table_with_joins.relation {
        ast::TableFactor::Table { name, alias, .. } => {
            let tname = name.to_string().to_ascii_lowercase();
            let aname = alias.as_ref().map(|a| a.name.value.to_ascii_lowercase());
            (tname, aname?)
        }
        _ => return None,
    };
    // Walk the WHERE clause looking for CompoundIdentifier with the original table name
    let selection = del.selection.as_ref()?;
    let bad_ref = find_table_ref_in_expr(selection, &table_name);
    if !bad_ref {
        return None;
    }
    // Compute cursor position for the table name reference in WHERE
    let pos = find_where_table_ref_pos(sql, &table_name);
    let mut info = pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        "42P01".to_owned(),
        format!(
            "invalid reference to FROM-clause entry for table \"{}\"",
            table_name
        ),
    );
    if let Some(p) = pos {
        info.position = Some(p.to_string());
    }
    info.hint = Some(format!(
        "Perhaps you meant to reference the table alias \"{}\".",
        alias
    ));
    Some(PgWireError::UserError(Box::new(info)))
}

fn find_table_ref_in_expr(expr: &ast::Expr, table_name: &str) -> bool {
    match expr {
        ast::Expr::CompoundIdentifier(parts) => {
            parts.len() >= 2 && parts[0].value.to_ascii_lowercase() == *table_name
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            find_table_ref_in_expr(left, table_name) || find_table_ref_in_expr(right, table_name)
        }
        ast::Expr::UnaryOp { expr, .. } => find_table_ref_in_expr(expr, table_name),
        ast::Expr::Nested(inner) => find_table_ref_in_expr(inner, table_name),
        ast::Expr::IsNull(inner) | ast::Expr::IsNotNull(inner) => {
            find_table_ref_in_expr(inner, table_name)
        }
        _ => false,
    }
}

fn find_where_table_ref_pos(sql: &str, table_name: &str) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let where_start = upper.find("WHERE")? + 5;
    find_bare_in_range(sql, table_name, where_start, sql.len())
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
