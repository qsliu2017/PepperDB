//! PostgreSQL-style regression test harness.
//!
//! Each test has a .sql input file and a .out expected output file,
//! mirroring PostgreSQL's src/test/regress/ layout. Both sql/ and
//! expected/ are symlinked to the PostgreSQL 18 submodule, so passing
//! a test means PepperDB output matches PostgreSQL exactly.

use std::path::PathBuf;

use futures::StreamExt;
use pgwire::api::results::{FieldInfo, Response};
use pgwire::api::Type;
use pgwire::error::PgWireError;

use pepper_db::Database;

// -- DataRow value extraction -------------------------------------------

fn extract_row_values(data: &[u8], schema: &[FieldInfo], null_display: &str) -> Vec<String> {
    let mut values = Vec::with_capacity(schema.len());
    let mut pos = 0;
    for fi in schema {
        let len = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            values.push(null_display.to_string());
        } else {
            let end = pos + len as usize;
            let mut val = String::from_utf8_lossy(&data[pos..end]).into_owned();
            if *fi.datatype() == Type::BOOL {
                match val.as_str() {
                    "true" => val = "t".into(),
                    "false" => val = "f".into(),
                    _ => {}
                }
            }
            values.push(val);
            pos = end;
        }
    }
    values
}

// -- psql table formatting (matches psql aligned output) ----------------

fn is_numeric_type(t: &Type) -> bool {
    *t == Type::INT2
        || *t == Type::INT4
        || *t == Type::INT8
        || *t == Type::FLOAT4
        || *t == Type::FLOAT8
}

fn format_table(schema: &[FieldInfo], rows: &[Vec<String>]) -> String {
    let headers: Vec<&str> = schema.iter().map(|f| f.name()).collect();

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            widths[i] = widths[i].max(val.len());
        }
    }

    let mut out = String::new();

    // Header (center-aligned, like psql)
    for (i, hdr) in headers.iter().enumerate() {
        if i > 0 {
            out.push('|');
        }
        let pad = widths[i] - hdr.len();
        let left = pad / 2;
        let right = pad - left;
        out.push(' ');
        out.push_str(&" ".repeat(left));
        out.push_str(hdr);
        out.push_str(&" ".repeat(right));
        out.push(' ');
    }
    out.push('\n');

    // Separator
    for (i, &w) in widths.iter().enumerate() {
        if i > 0 {
            out.push('+');
        }
        out.push_str(&"-".repeat(w + 2));
    }
    out.push('\n');

    // Data rows
    let right_align: Vec<bool> = schema
        .iter()
        .map(|f| is_numeric_type(f.datatype()))
        .collect();
    let last = widths.len().saturating_sub(1);
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 {
                out.push('|');
            }
            out.push(' ');
            if right_align[i] {
                out.push_str(&format!("{:>width$}", val, width = widths[i]));
            } else if i == last {
                // psql omits trailing padding on the last column
                out.push_str(val);
            } else {
                out.push_str(&format!("{:<width$}", val, width = widths[i]));
            }
            // psql omits trailing space on the last column of data rows
            if i < last {
                out.push(' ');
            }
        }
        out.push('\n');
    }

    // Footer
    if rows.len() == 1 {
        out.push_str("(1 row)\n");
    } else {
        out.push_str(&format!("({} rows)\n", rows.len()));
    }

    // Blank line after query results (matches psql)
    out.push('\n');
    out
}

// -- Response formatting ------------------------------------------------

fn format_error(msg: &str) -> String {
    format!("ERROR:  {}\n", msg)
}

/// Find which line and column a 1-indexed cursor position falls on.
fn find_line_col(sql: &str, pos: usize) -> (usize, &str, usize) {
    let pos0 = pos.saturating_sub(1);
    let mut offset = 0;
    for (i, line) in sql.lines().enumerate() {
        if pos0 < offset + line.len() {
            return (i + 1, line, pos - offset);
        }
        offset += line.len() + 1; // +1 for \n
    }
    (1, sql.lines().next().unwrap_or(sql), pos)
}

fn format_error_with_position(msg: &str, pos: Option<usize>, sql: &str) -> String {
    let mut out = format!("ERROR:  {}\n", msg);
    if let Some(pos) = pos {
        let (line_num, raw_line, col) = find_line_col(sql, pos);
        let line_text = raw_line.replace('\t', " ");
        let prefix = format!("LINE {}: ", line_num);

        const DISPLAY_SIZE: usize = 60;
        const MIN_RIGHT_CXT: usize = 10;
        let curs0 = col - 1; // 0-indexed cursor in line

        if line_text.len() <= DISPLAY_SIZE {
            out.push_str(&prefix);
            out.push_str(&line_text);
            out.push('\n');
            let spaces = prefix.len() + curs0;
            out.extend(std::iter::repeat(' ').take(spaces));
            out.push_str("^\n");
        } else {
            let max_start = line_text.len() - DISPLAY_SIZE;
            let want_start = curs0.saturating_sub(DISPLAY_SIZE - MIN_RIGHT_CXT);
            let start = want_start.min(max_start);
            let end = (start + DISPLAY_SIZE).min(line_text.len());

            out.push_str(&prefix);
            if start > 0 {
                out.push_str("...");
            }
            out.push_str(&line_text[start..end]);
            if end < line_text.len() {
                out.push_str("...");
            }
            out.push('\n');

            let display_col = curs0 - start + if start > 0 { 3 } else { 0 };
            let spaces = prefix.len() + display_col;
            out.extend(std::iter::repeat(' ').take(spaces));
            out.push_str("^\n");
        }
    }
    out
}

fn format_response(resp: Response, null_display: &str) -> String {
    match resp {
        Response::Execution(_) => String::new(),
        Response::Query(qr) => {
            let schema = qr.row_schema();
            let mut data_rows = qr.data_rows();
            let mut rows = Vec::new();
            while let Some(Ok(dr)) = futures::executor::block_on(data_rows.next()) {
                rows.push(extract_row_values(&dr.data, &schema, null_display));
            }
            format_table(&schema, &rows)
        }
        Response::Error(info) => format_error(&info.message),
        _ => String::new(),
    }
}

// -- psql meta-command parsing ------------------------------------------

/// Parse `\pset null 'value'` and return the null display string.
fn parse_pset_null(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("\\pset")?;
    let rest = rest.trim_start();
    let rest = rest.strip_prefix("null")?;
    let rest = rest.trim_start();
    // Value may be quoted with single quotes
    if let Some(inner) = rest.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        Some(inner.to_string())
    } else {
        Some(rest.to_string())
    }
}

// -- SQL runner ---------------------------------------------------------

/// Count net block-comment depth change in a line (handles `/* */` nesting).
fn block_comment_delta(line: &str) -> i32 {
    let mut delta = 0i32;
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            delta += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            delta -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    delta
}

/// Strip `/* ... */` block comments from SQL (for DataFusion compatibility).
fn strip_block_comments(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut depth = 0i32;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            // Replace comment with a space (preserves token separation)
            if depth == 0 {
                result.push(' ');
            }
            i += 2;
            continue;
        }
        if depth == 0 {
            result.push(bytes[i] as char);
        }
        i += 1;
    }
    result
}

/// Check if a line's SQL content ends with `;`, ignoring trailing `--` comments.
fn line_ends_with_semicolon(line: &str) -> bool {
    // Find the effective end: before any -- comment (but not inside a string)
    let mut in_string = false;
    let bytes = line.as_bytes();
    let mut effective_end = bytes.len();
    let mut i = 0;
    while i < bytes.len() {
        if in_string {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2; // escaped quote
                    continue;
                }
                in_string = false;
            }
        } else {
            if bytes[i] == b'\'' {
                in_string = true;
            } else if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                effective_end = i;
                break;
            }
        }
        i += 1;
    }
    let trimmed = line[..effective_end].trim_end();
    trimmed.ends_with(';')
}

async fn run_sql(sql: &str, db: &Database) -> String {
    let mut out = String::new();
    let mut stmt_lines: Vec<&str> = Vec::new();
    let mut comment_depth: i32 = 0;
    let mut null_display = String::new();

    for line in sql.lines() {
        // Skip blank lines (psql -a does not echo them)
        if line.trim().is_empty() {
            continue;
        }

        // Handle psql meta-commands (lines starting with \)
        if stmt_lines.is_empty() && line.trim_start().starts_with('\\') {
            if let Some(val) = parse_pset_null(line) {
                null_display = val;
            }
            // psql -a echoes \pset commands
            out.push_str(line);
            out.push('\n');
            continue;
        }

        let prev_depth = comment_depth;
        comment_depth += block_comment_delta(line);

        // Inside or entering a standalone block comment (no active statement)
        if stmt_lines.is_empty()
            && (prev_depth > 0 || (prev_depth == 0 && comment_depth != 0 && !line.contains(';')))
        {
            // Echo block comment lines (psql echoes them)
            out.push_str(line);
            out.push('\n');
            continue;
        }

        // If we were inside a block comment and just closed it with no
        // active statement, echo and continue
        if stmt_lines.is_empty() && prev_depth > 0 && comment_depth == 0 {
            out.push_str(line);
            out.push('\n');
            continue;
        }

        // Comment-only line outside a statement: echo it
        if stmt_lines.is_empty() && line.trim_start().starts_with("--") {
            out.push_str(line);
            out.push('\n');
            continue;
        }

        stmt_lines.push(line);

        // Statement complete when line has ; (before any -- comment) and
        // we're not inside a block comment
        if comment_depth == 0 && line_ends_with_semicolon(line) {
            // Echo the SQL exactly as written
            for &sl in &stmt_lines {
                out.push_str(sl);
                out.push('\n');
            }

            let stmt_sql = stmt_lines.join("\n");
            // Strip block comments before sending to DataFusion
            let clean_sql = strip_block_comments(&stmt_sql);
            match db.execute_sql(&clean_sql).await {
                Ok(resp) => out.push_str(&format_response(resp, &null_display)),
                Err(PgWireError::UserError(info)) => {
                    let pos = info.position.as_ref().and_then(|p| p.parse::<usize>().ok());
                    out.push_str(&format_error_with_position(&info.message, pos, &clean_sql));
                    if let Some(hint) = &info.hint {
                        out.push_str(&format!("HINT:  {}\n", hint));
                    }
                }
                Err(e) => {
                    out.push_str(&format_error(&e.to_string()));
                }
            }

            stmt_lines.clear();
        }
    }

    // Echo any trailing block comment lines (like "/* and this is the end of the file */")
    // that were accumulated but never formed a statement
    if comment_depth == 0 && !stmt_lines.is_empty() {
        // These are trailing comment-only content
        for &sl in &stmt_lines {
            out.push_str(sl);
            out.push('\n');
        }
    }

    out
}

// -- Test runner --------------------------------------------------------

/// Pre-populate tables from test_setup.sql that specific PG tests depend on.
async fn setup_test_tables(name: &str, db: &Database) {
    let setup_sql: &[&str] = match name {
        "varchar" => &[
            "CREATE TABLE VARCHAR_TBL(f1 varchar(4));",
            "INSERT INTO VARCHAR_TBL (f1) VALUES ('a');",
            "INSERT INTO VARCHAR_TBL (f1) VALUES ('ab');",
            "INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd');",
            "INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd    ');",
        ],
        _ => &[],
    };
    for sql in setup_sql {
        let _ = db.execute_sql(sql).await;
    }
}

async fn run_regress_test(name: &str) {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/regress");
    let sql_path = base.join("sql").join(format!("{}.sql", name));
    let out_path = base.join("expected").join(format!("{}.out", name));

    let sql = std::fs::read_to_string(&sql_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", sql_path.display(), e));

    let tmp = tempfile::tempdir().expect("failed to create tempdir");
    let db = Database::new(tmp.path());
    setup_test_tables(name, &db).await;
    let actual = run_sql(&sql, &db).await;

    let expected = std::fs::read_to_string(&out_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", out_path.display(), e));

    if actual != expected {
        let mut diff = String::new();
        diff.push_str(&format!("--- {}\n", out_path.display()));
        diff.push_str("+++ actual\n");
        for line in diff::lines(&expected, &actual) {
            match line {
                diff::Result::Left(l) => diff.push_str(&format!("-{}\n", l)),
                diff::Result::Right(r) => diff.push_str(&format!("+{}\n", r)),
                diff::Result::Both(b, _) => diff.push_str(&format!(" {}\n", b)),
            }
        }
        panic!("regression test '{}' output mismatch\n\n{}", name, diff);
    }
}

// -- Test declarations --------------------------------------------------

macro_rules! regress_test {
    ($($name:ident),* $(,)?) => {
        $( #[tokio::test] async fn $name() { run_regress_test(stringify!($name)).await; } )*
    };
}

// Raw-identifier tests (Rust keywords as test names)
// #[tokio::test] async fn r#async() { run_regress_test("async").await; }
// #[tokio::test] async fn r#box() { run_regress_test("box").await; }
// #[tokio::test] async fn r#enum() { run_regress_test("enum").await; }
// #[tokio::test] async fn r#union() { run_regress_test("union").await; }

regress_test!(
    comments,
    md5,
    // advisory_lock,
    // aggregates,
    // alter_generic,
    // alter_operator,
    // alter_table,
    // amutils,
    // arrays,
    // bit,
    // bitmapops,
    boolean,
    // brin,
    // brin_bloom,
    // brin_multi,
    // btree_index,
    // case,
    // char,
    // circle,
    // cluster,
    // collate,
    // combocid,
    // combocid,
    // compression,
    // constraints,
    // conversion,
    // copy,
    // copy2,
    // copydml,
    // copyencoding,
    // copyselect,
    // create_aggregate,
    // create_am,
    // create_cast,
    // create_function_c,
    // create_function_sql,
    // create_index,
    // create_index_spgist,
    // create_misc,
    // create_operator,
    // create_procedure,
    // create_role,
    // create_schema,
    // create_table,
    // create_table_like,
    // create_type,
    // create_view,
    // database,
    // date,
    // dbsize,
    delete,
    // dependency,
    // domain,
    // drop_if_exists,
    // drop_operator,
    // encoding,
    // equivclass,
    // errors,
    // euc_kr,
    // event_trigger,
    // event_trigger_login,
    // explain,
    // expressions,
    // fast_default,
    // float4,
    // float8,
    // foreign_data,
    // foreign_key,
    // functional_deps,
    // generated_stored,
    // generated_virtual,
    // geometry,
    // gin,
    // gist,
    // groupingsets,
    // guc,
    // hash_func,
    // hash_index,
    // hash_part,
    // horology,
    // identity,
    // incremental_sort,
    // index_including,
    // index_including_gist,
    // indexing,
    // indirect_toast,
    // inet,
    // infinite_recurse,
    // inherit,
    // init_privs,
    // insert,
    // insert_conflict,
    // int2,
    // int4,
    // int8,
    // interval,
    // join,
    // join_hash,
    // json,
    // json_encoding,
    // jsonb,
    // jsonb_jsonpath,
    // jsonpath,
    // jsonpath_encoding,
    // largeobject,
    // limit,
    // line,
    // lock,
    // lseg,
    // macaddr,
    // macaddr8,
    // maintain_every,
    // matview,
    // md5,
    // memoize,
    // merge,
    // misc,
    // misc_functions,
    // misc_sanity,
    // money,
    // multirangetypes,
    // mvcc,
    // name,
    // namespace,
    // numa,
    // numeric,
    // numeric_big,
    // numerology,
    // object_address,
    // oid,
    // oidjoins,
    // opr_sanity,
    // partition_aggregate,
    // partition_info,
    // partition_join,
    // partition_prune,
    // password,
    // path,
    // pg_lsn,
    // plancache,
    // plpgsql,
    // point,
    // polygon,
    // polymorphism,
    // portals,
    // portals_p2,
    // predicate,
    // prepare,
    // prepared_xacts,
    // privileges,
    // psql,
    // psql_crosstab,
    // psql_pipeline,
    // publication,
    // random,
    // rangefuncs,
    // rangetypes,
    // regex,
    // regproc,
    // reindex_catalog,
    // reloptions,
    // replica_identity,
    // returning,
    // roleattributes,
    // rowsecurity,
    // rowtypes,
    // rules,
    // sanity_check,
    // security_label,
    // select,
    // select_distinct,
    // select_distinct_on,
    select_having,
    select_implicit,
    // select_into,
    // select_parallel,
    // select_views,
    // sequence,
    // spgist,
    // sqljson,
    // sqljson_jsontable,
    // sqljson_queryfuncs,
    // stats,
    // stats_ext,
    // stats_import,
    // strings,
    // subscription,
    // subselect,
    // sysviews,
    // tablesample,
    // tablespace,
    // temp,
    // test_setup,
    // text,
    // tid,
    // tidrangescan,
    // tidscan,
    // time,
    // timestamp,
    // timestamptz,
    // timetz,
    // transactions,
    // triggers,
    // truncate,
    // tsdicts,
    // tsearch,
    // tsrf,
    // tstypes,
    // tuplesort,
    // txid,
    // type_sanity,
    // typed_table,
    // unicode,
    // updatable_views,
    // update,
    // uuid,
    // vacuum,
    // vacuum_parallel,
    varchar,
    // window,
    // with,
    // without_overlaps,
    // write_parallel,
    // xid,
    // xml,
    // xmlmap,
);
