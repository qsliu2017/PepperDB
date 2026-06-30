//! `commands/copyto.c`: COPY TO -- export rows to a server file.
//!
//! `copy_to_relation` scans a heap relation; `copy_to_query` runs a query through
//! SPI. Each live row is formatted (per-attribute output funcs, the delimiter, the
//! NULL marker, CSV quoting) into an in-memory buffer that is written to the file in
//! one `spawn_blocking` call -- no lock is held across the `.await`.
//!
//! M13: text + CSV to a file path. STDOUT (the CopyData wire path) and BINARY are
//! staged in `do_copy`.

use std::sync::Arc;

use crate::backend::commands::copy::{copy_attrs, CopyAttr, CopyFormatOptions, CopyHeaderChoice};
use crate::nodes::nodes::Node;
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Format a header line for the given column names into `out`.
fn copy_one_header(out: &mut String, colnames: &[String], opts: &CopyFormatOptions) {
    for (i, name) in colnames.iter().enumerate() {
        if i > 0 {
            out.push(opts.delim);
        }
        if opts.csv_mode {
            attribute_out_csv(out, name, opts, false);
        } else {
            attribute_out_text(out, name, opts);
        }
    }
    out.push('\n');
}

/// PG `CopyAttributeOutText`: write `string` to `out`, escaping control chars, the
/// backslash, and the delimiter (the text-format escaping rules).
fn attribute_out_text(out: &mut String, string: &str, opts: &CopyFormatOptions) {
    let delimc = opts.delim;
    for c in string.chars() {
        match c {
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0B}' => out.push_str("\\v"),
            '\\' => out.push_str("\\\\"),
            c if c == delimc => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
}

/// PG `CopyAttributeOutCSV`: write `string` to `out`, quoting it if it contains the
/// delimiter, quote char, a newline/CR, or (when `force_quote`) unconditionally.
fn attribute_out_csv(out: &mut String, string: &str, opts: &CopyFormatOptions, force_quote: bool) {
    let quotec = opts.quote;
    let escapec = opts.escape;
    let delimc = opts.delim;

    // Force quoting if the value equals the NULL marker (so it round-trips as data).
    let mut use_quote = force_quote || string == opts.null_print;
    if !use_quote {
        use_quote = string
            .chars()
            .any(|c| c == delimc || c == quotec || c == '\n' || c == '\r');
    }

    if use_quote {
        out.push(quotec);
        for c in string.chars() {
            if c == quotec || c == escapec {
                out.push(escapec);
            }
            out.push(c);
        }
        out.push(quotec);
    } else {
        out.push_str(string);
    }
}

/// Format one row's `values`/`isnull` (already restricted to the COPY columns) into
/// `out` followed by a newline.
fn copy_one_row_to(
    out: &mut String,
    values: &[Datum],
    isnull: &[bool],
    out_funcs: &[Oid],
    opts: &CopyFormatOptions,
) {
    for (i, (&datum, &is_null)) in values.iter().zip(isnull.iter()).enumerate() {
        if i > 0 {
            out.push(opts.delim);
        }
        if is_null {
            out.push_str(&opts.null_print);
        } else {
            let string =
                crate::backend::utils::fmgr::fmgr::OidOutputFunctionCall(out_funcs[i], datum);
            if opts.csv_mode {
                attribute_out_csv(out, &string, opts, false);
            } else {
                attribute_out_text(out, &string, opts);
            }
        }
    }
    out.push('\n');
}

/// Resolve the text output-function OID for each COPY column, warming the TYPEOID
/// syscache for each column type so the per-row sync reads hit.
async fn output_func_oids(shared: &Arc<SharedState>, typoids: &[Oid]) -> Vec<Oid> {
    let mut out = Vec::with_capacity(typoids.len());
    for &typoid in typoids {
        let (funcoid, _) =
            crate::backend::utils::cache::lsyscache::get_type_output_info_populate(shared, typoid)
                .await;
        out.push(funcoid);
    }
    out
}

/// Write `buf` to `filename` (one positional write on a blocking thread).
async fn write_copy_file(filename: &str, buf: Vec<u8>) {
    let path = filename.to_string();
    tokio::task::spawn_blocking(move || {
        std::io::Write::write_all(
            &mut std::fs::File::create(&path)
                .unwrap_or_else(|e| copy_io_error(&path, &e)),
            &buf,
        )
        .unwrap_or_else(|e| copy_io_error(&path, &e));
    })
    .await
    .unwrap_or_else(|e| unreachable!("COPY TO write task panicked: {e}"));
}

#[cold]
fn copy_io_error(path: &str, e: &std::io::Error) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |ed: &mut crate::utils::elog::ErrorData| {
        ed.errcode(crate::utils::errcodes::ERRCODE_IO_ERROR)
            .errmsg(format!("could not open file \"{path}\" for writing: {e}"));
    });
    unreachable!("ereport!(ERROR) does not return")
}

/// PG `DoCopyTo` (relation path): scan the heap and write the selected columns of
/// each live row to the file. Returns the processed-row count.
pub async fn copy_to_relation(
    shared: &Arc<SharedState>,
    rel: &RelationData,
    attnums: &[i16],
    opts: &CopyFormatOptions,
    filename: &str,
) -> u64 {
    use crate::access::sdir::ScanDirection;
    use crate::backend::access::common::heaptuple::heap_deform_tuple;
    use crate::backend::access::table::tableam::{table_beginscan, table_endscan, table_scan_getnext};
    use crate::backend::access::transam::xact::GetCurrentCommandId;
    use crate::backend::utils::time::snapmgr::GetTransactionSnapshot;

    let attrs = copy_attrs(rel, attnums);
    let typoids: Vec<Oid> = attrs.iter().map(|a| a.typoid).collect();
    let out_funcs = output_func_oids(shared, &typoids).await;
    let desc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("COPY TO relation has a rowtype descriptor"));

    let mut out = String::new();
    if opts.header_line == CopyHeaderChoice::True {
        let colnames: Vec<String> = attrs
            .iter()
            .map(|a| crate::backend::commands::copy::att_name(desc.attr(a.attnum0())))
            .collect();
        copy_one_header(&mut out, &colnames, opts);
    }

    let mut snap = GetTransactionSnapshot(shared)
        .unwrap_or_else(|| unreachable!("COPY TO runs inside a transaction snapshot"));
    Arc::make_mut(&mut snap).curcid = GetCurrentCommandId(false);
    let snap_ref: &crate::utils::snapshot::SnapshotData = &snap;

    let mut processed: u64 = 0;
    let mut scan = table_beginscan(rel, snap_ref);
    let mut row_vals: Vec<Datum> = Vec::with_capacity(attrs.len());
    let mut row_nulls: Vec<bool> = Vec::with_capacity(attrs.len());
    loop {
        let tuple = table_scan_getnext(shared, &mut scan, ScanDirection::Forward).await;
        let Some(tuple) = tuple else { break };
        // SAFETY: `tuple` references scan.ctup (owned page-item copy), valid until
        // the next getnext; deform reads its header + data bytes.
        let htd = unsafe { &*tuple };
        let (values, isnull) = unsafe { heap_deform_tuple(htd, desc) };

        row_vals.clear();
        row_nulls.clear();
        for a in &attrs {
            row_vals.push(values[a.attnum0()]);
            row_nulls.push(isnull[a.attnum0()]);
        }
        copy_one_row_to(&mut out, &row_vals, &row_nulls, &out_funcs, opts);
        processed += 1;
    }
    table_endscan(shared, &mut scan);

    write_copy_file(filename, out.into_bytes()).await;
    processed
}

/// PG `DoCopyTo` (query path): plan + run the query (reusing the surrounding
/// command's transaction + active snapshot, like the wire SELECT path) and write
/// its result rows to the file. Returns the processed-row count.
pub async fn copy_to_query(
    shared: &Arc<SharedState>,
    _pstate: &mut ParseState,
    query: &Node,
    opts: &CopyFormatOptions,
    filename: &str,
) -> u64 {
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::backend::parser::analyze::transform_stmt_async_pub;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::backend::tcop::pquery::run_plan_into_store;
    use crate::backend::utils::sort::tuplestore::{tuplestore_gettupleslot, tuplestore_rescan};

    // Analyze + rewrite + plan the COPY's SelectStmt directly (no SPI transaction
    // management; this runs inside the COPY utility command's transaction).
    let mut pstate = crate::backend::parser::parse_node::make_parsestate(None);
    let analyzed = Box::pin(transform_stmt_async_pub(shared, &mut pstate, query)).await;
    let mut q = query_rewrite(*analyzed).remove(0);
    let plan = standard_planner(&mut q, "", 0, None);

    let (mut store, tupdesc, _processed) =
        Box::pin(run_plan_into_store(shared, &plan, "", None)).await;
    tuplestore_rescan(&mut store);

    let desc = tupdesc.unwrap_or_else(|| unreachable!("COPY (query) TO produced a result descriptor"));
    let natts = desc.natts as usize;
    let typoids: Vec<Oid> = (0..natts).map(|i| desc.attr(i).atttypid).collect();
    let out_funcs = output_func_oids(shared, &typoids).await;

    let mut out = String::new();
    if opts.header_line == CopyHeaderChoice::True {
        let colnames: Vec<String> = (0..natts)
            .map(|i| crate::backend::commands::copy::att_name(desc.attr(i)))
            .collect();
        copy_one_header(&mut out, &colnames, opts);
    }

    let mut slot = crate::backend::executor::execTuples::make_single_tuple_table_slot(
        Some(desc.clone()),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );
    let mut processed: u64 = 0;
    while tuplestore_gettupleslot(&mut store, true, false, &mut slot) {
        let n = slot.nvalid.max(0) as usize;
        copy_one_row_to(&mut out, &slot.values[..n], &slot.isnull[..n], &out_funcs, opts);
        processed += 1;
    }

    write_copy_file(filename, out.into_bytes()).await;
    processed
}

use crate::postgres_ext::Oid;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::commands::copy::CopyFormatOptions;

    fn text_opts() -> CopyFormatOptions {
        CopyFormatOptions { null_print: "\\N".into(), delim: '\t', ..CopyFormatOptions::default() }
    }

    fn csv_opts() -> CopyFormatOptions {
        CopyFormatOptions {
            csv_mode: true,
            null_print: String::new(),
            delim: ',',
            quote: '"',
            escape: '"',
            ..CopyFormatOptions::default()
        }
    }

    #[test]
    fn text_output_escapes_delim_backslash_newline() {
        let opts = text_opts();
        let mut out = String::new();
        attribute_out_text(&mut out, "a\tb\\c\nd", &opts);
        assert_eq!(out, "a\\tb\\\\c\\nd");
    }

    #[test]
    fn csv_output_quotes_when_needed() {
        let opts = csv_opts();
        // Plain value: no quoting.
        let mut out = String::new();
        attribute_out_csv(&mut out, "plain", &opts, false);
        assert_eq!(out, "plain");

        // Contains comma + quote: quoted, inner quote doubled.
        let mut out = String::new();
        attribute_out_csv(&mut out, "a,\"b\"", &opts, false);
        assert_eq!(out, "\"a,\"\"b\"\"\"");

        // Embedded newline forces quoting.
        let mut out = String::new();
        attribute_out_csv(&mut out, "x\ny", &opts, false);
        assert_eq!(out, "\"x\ny\"");
    }

    #[test]
    fn header_line_text() {
        let opts = text_opts();
        let mut out = String::new();
        copy_one_header(&mut out, &["a".into(), "b".into()], &opts);
        assert_eq!(out, "a\tb\n");
    }
}
