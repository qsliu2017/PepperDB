//! `commands/copyfrom.c`: COPY FROM -- bulk load rows from a server file.
//!
//! `copy_from` reads the whole file, skips the optional header, parses each line
//! into fields (text or CSV via `copyfromparse`), runs the per-attribute input
//! funcs to build a Datum row, forms a heap tuple, and inserts it. AFTER ROW INSERT
//! triggers (the RI / FK system triggers) are queued per row exactly as the
//! `ModifyTable` insert path does; they fire at end of the surrounding query.
//!
//! M13: text + CSV from a file path. Columns not in the COPY column list are loaded
//! as NULL (the DEFAULT-expression expansion is staged). BEFORE ROW triggers and
//! CHECK-constraint evaluation are staged (their executor hooks are no-ops in this
//! milestone); the AFTER ROW / RI path is real. STDIN and BINARY are staged.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::commands::copy::{copy_attrs, copy_get_attnums, CopyFormatOptions, CopyHeaderChoice};
use crate::backend::commands::copyfromparse::{
    read_attributes_csv, read_attributes_text, CopyReadState,
};
use crate::nodes::parsenodes::CopyStmt;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Read the whole COPY input file into memory on a blocking thread.
async fn read_copy_file(filename: &str) -> Vec<u8> {
    let path = filename.to_string();
    tokio::task::spawn_blocking(move || std::fs::read(&path).map_err(|e| (path.clone(), e)))
        .await
        .unwrap_or_else(|e| unreachable!("COPY FROM read task panicked: {e}"))
        .unwrap_or_else(|(path, e)| {
            crate::ereport!(crate::utils::elog::ERROR, |ed: &mut crate::utils::elog::ErrorData| {
                ed.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FILE)
                    .errmsg(format!("could not open file \"{path}\" for reading: {e}"));
            });
            unreachable!("ereport!(ERROR) does not return")
        })
}

/// PG `BeginCopyFrom` + `CopyFrom`: load the file into the relation. Returns the
/// number of rows inserted.
pub async fn copy_from(
    shared: &Arc<SharedState>,
    rel: &RelationData,
    stmt: &CopyStmt,
    opts: &CopyFormatOptions,
    filename: &str,
) -> u64 {
    let attnums = copy_get_attnums(rel, &stmt.attlist);
    let attrs = copy_attrs(rel, &attnums);
    // Resolve (input-func OID, typioparam) per COPY column, warming the TYPEOID
    // syscache so the per-row input-func calls hit.
    let mut in_info: Vec<(Oid, Oid)> = Vec::with_capacity(attrs.len());
    for a in &attrs {
        in_info.push(
            crate::backend::utils::cache::lsyscache::get_type_input_info_populate(shared, a.typoid)
                .await,
        );
    }

    let desc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("COPY FROM relation has a rowtype descriptor"))
        .clone();
    let natts = desc.natts as usize;

    // AFTER ROW INSERT trigger descriptor (RI/FK), built on demand like ModifyTable.
    let relid = rel.rd_id;
    let has_triggers = rel.rd_rel.as_ref().is_some_and(|r| r.relhastriggers);
    let trigdesc = if has_triggers {
        crate::backend::commands::trigger::relation_build_triggers(shared, relid).await
    } else {
        None
    };

    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);

    let data = read_copy_file(filename).await;
    let mut reader = CopyReadState::new(&data, opts);

    // HEADER: skip the first line (MATCH validation is staged).
    if opts.header_line != CopyHeaderChoice::False {
        let _ = reader.read_line();
    }

    let desc_opt = Some(desc.clone());
    let mut processed: u64 = 0;
    while let Some(line) = reader.read_line() {
        // A blank trailing line is not a row.
        if line.is_empty() && reader.at_eof() {
            break;
        }
        let fields = if opts.csv_mode {
            read_attributes_csv(&line, opts)
        } else {
            read_attributes_text(&line, opts)
        };

        if fields.len() != attrs.len() {
            crate::ereport!(crate::utils::elog::ERROR, |ed: &mut crate::utils::elog::ErrorData| {
                let (kind, want, got) = if fields.len() < attrs.len() {
                    ("missing data for column", attrs.len(), fields.len())
                } else {
                    ("extra data after last expected column", attrs.len(), fields.len())
                };
                ed.errcode(crate::utils::errcodes::ERRCODE_BAD_COPY_FILE_FORMAT)
                    .errmsg(format!("{kind} (expected {want} fields, got {got})"));
            });
        }

        // Build the full physical row: COPY columns from input, others NULL.
        let mut values: Vec<Datum> = vec![Datum(0); natts];
        let mut isnull: Vec<bool> = vec![true; natts];
        for (idx, field) in fields.iter().enumerate() {
            let attr = &attrs[idx];
            let phys = attr.attnum as usize - 1;
            match field {
                None => {
                    values[phys] = Datum(0);
                    isnull[phys] = true;
                }
                Some(text) => {
                    let (funcoid, typioparam) = in_info[idx];
                    // A NULL result keeps the pre-initialized NULL slot.
                    if let Some(d) = crate::backend::utils::fmgr::fmgr::OidInputFunctionCall(
                        funcoid, text, typioparam, attr.typmod,
                    ) {
                        values[phys] = d;
                        isnull[phys] = false;
                    }
                }
            }
        }

        let mut tuple = heap_form_tuple(&desc, &values, &isnull);
        // Box::pin the (deep) insert future to cap async stack growth in debug builds.
        Box::pin(heap_insert(shared, rel, &mut tuple, cid, 0)).await;
        heap_freetuple(tuple);

        // ExecARInsertTriggers: queue AFTER ROW INSERT triggers (the RI FK check).
        crate::backend::commands::trigger::exec_ar_insert_triggers(
            trigdesc.as_ref(),
            relid,
            &values,
            &isnull,
            &desc_opt,
        );
        processed += 1;
    }

    processed
}
