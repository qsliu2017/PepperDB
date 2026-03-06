// PostgreSQL-style regression test harness.
//
// Each test has a .sql input file and a .out expected output file,
// mirroring PostgreSQL's src/test/regress/ layout.  Run with
// UPDATE_EXPECT=1 to regenerate .out files.

use std::path::PathBuf;

use futures::StreamExt;
use pgwire::api::results::{FieldInfo, Response};
use pgwire::api::Type;
use pgwire::error::PgWireError;
use pgwire::messages::response::CommandComplete;

use pepper_db::{executor, parser, Database};

// -- Statement splitting ------------------------------------------------

fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| format!("{};", s))
        .collect()
}

// -- DataRow value extraction -------------------------------------------

fn extract_row_values(data: &[u8], ncols: usize) -> Vec<String> {
    let mut values = Vec::with_capacity(ncols);
    let mut pos = 0;
    for _ in 0..ncols {
        let len = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            values.push(String::new());
        } else {
            let end = pos + len as usize;
            values.push(String::from_utf8_lossy(&data[pos..end]).into_owned());
            pos = end;
        }
    }
    values
}

// -- psql table formatting ----------------------------------------------

fn format_table(schema: &[FieldInfo], rows: &[Vec<String>]) -> String {
    let headers: Vec<&str> = schema.iter().map(|f| f.name()).collect();

    // Column widths
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if val.len() > widths[i] {
                widths[i] = val.len();
            }
        }
    }

    let mut out = String::new();

    // Header
    for (i, hdr) in headers.iter().enumerate() {
        if i > 0 {
            out.push_str(" | ");
        } else {
            out.push(' ');
        }
        out.push_str(&format!("{:<width$}", hdr, width = widths[i]));
    }
    out.push('\n');

    // Separator
    for (i, &w) in widths.iter().enumerate() {
        if i > 0 {
            out.push_str("-+-");
        }
        out.push_str(&"-".repeat(w + 1));
    }
    out.push('\n');

    // Data rows
    let right_align: Vec<bool> = schema
        .iter()
        .map(|f| {
            let t = f.datatype();
            *t == Type::INT2
                || *t == Type::INT4
                || *t == Type::INT8
                || *t == Type::FLOAT4
                || *t == Type::FLOAT8
        })
        .collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 {
                out.push_str(" | ");
            } else {
                out.push(' ');
            }
            if right_align[i] {
                out.push_str(&format!("{:>width$}", val, width = widths[i]));
            } else {
                out.push_str(&format!("{:<width$}", val, width = widths[i]));
            }
        }
        out.push('\n');
    }

    // Footer
    let nrows = rows.len();
    if nrows == 1 {
        out.push_str("(1 row)\n");
    } else {
        out.push_str(&format!("({} rows)\n", nrows));
    }

    out
}

// -- Response formatting ------------------------------------------------

fn format_error(msg: &str) -> String {
    format!("ERROR:  {}\n", msg)
}

fn format_response(resp: Response) -> String {
    match resp {
        Response::Execution(tag) => {
            let cc = CommandComplete::from(tag);
            format!("{}\n", cc.tag)
        }
        Response::Query(qr) => {
            let schema = qr.row_schema();
            let ncols = schema.len();
            let mut data_rows = qr.data_rows();
            let mut rows = Vec::new();
            while let Some(Ok(dr)) = futures::executor::block_on(data_rows.next()) {
                rows.push(extract_row_values(&dr.data, ncols));
            }
            format_table(&schema, &rows)
        }
        Response::Error(info) => format_error(&info.message),
        _ => String::new(),
    }
}

// -- SQL runner ---------------------------------------------------------

fn run_sql(sql: &str, db: &Database) -> String {
    let mut out = String::new();
    for stmt_sql in split_statements(sql) {
        // Echo the SQL (like psql -e)
        out.push_str(&stmt_sql);
        out.push('\n');

        // Parse
        let stmts = match parser::parse(&stmt_sql) {
            Ok(s) => s,
            Err(PgWireError::UserError(info)) => {
                out.push_str(&format_error(&info.message));
                continue;
            }
            Err(e) => {
                out.push_str(&format_error(&e.to_string()));
                continue;
            }
        };

        // Execute
        for stmt in stmts {
            match executor::execute(stmt, db) {
                Ok(resp) => out.push_str(&format_response(resp)),
                Err(PgWireError::UserError(info)) => {
                    out.push_str(&format_error(&info.message));
                }
                Err(e) => {
                    out.push_str(&format_error(&e.to_string()));
                }
            }
        }
    }
    out
}

// -- Test runner --------------------------------------------------------

fn run_regress_test(name: &str) {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/regress");
    let sql_path = base.join("sql").join(format!("{}.sql", name));
    let out_path = base.join("expected").join(format!("{}.out", name));

    let sql = std::fs::read_to_string(&sql_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", sql_path.display(), e));

    let tmp = tempfile::tempdir().expect("failed to create tempdir");
    let db = Database::new(tmp.path());
    let actual = run_sql(&sql, &db);

    if std::env::var("UPDATE_EXPECT").as_deref() == Ok("1") {
        std::fs::write(&out_path, &actual)
            .unwrap_or_else(|e| panic!("failed to write {}: {}", out_path.display(), e));
        return;
    }

    let expected = std::fs::read_to_string(&out_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", out_path.display(), e));

    if actual != expected {
        // Show a unified-style diff
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
        panic!(
            "regression test '{}' output mismatch\n\n\
             Rerun with UPDATE_EXPECT=1 to update.\n\n{}",
            name, diff
        );
    }
}

// -- Test declarations --------------------------------------------------

macro_rules! regress_test {
    ($($name:ident),* $(,)?) => {
        $( #[test] fn $name() { run_regress_test(stringify!($name)); } )*
    };
}

regress_test!(basics, select, orderby, boolean, int2, int8, float, text, null, update, delete, drop_table, int4, case_expr, expressions, strings);
