//! `commands/copy.c`: the COPY command dispatch.
//!
//! `do_copy` is the entry point reached from `standard_process_utility`. It parses
//! the `CopyStmt` options into a `CopyFormatOptions`, opens the target relation
//! (or remembers the query for `COPY (query) TO`), and routes to the COPY TO
//! (`copyto.rs`) or COPY FROM (`copyfrom.rs`) machinery.
//!
//! M13 scope: text + CSV formats to/from a server file path. STDIN/STDOUT pipe and
//! PROGRAM are staged (they need the CopyData wire-message path / a subprocess);
//! BINARY format input/output is staged. The format-option parsing, the relation
//! and query paths, the per-attribute input/output funcs, the bulk heap insert, and
//! AFTER ROW trigger / RI firing are all real.

use std::sync::Arc;

use crate::parser::parse_node::ParseState;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CopyStmt, DefElem, ValUnion};
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;

/// PG `CopyHeaderChoice`: whether a header line is present (and, for FROM, whether
/// it must match the column names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyHeaderChoice {
    False,
    True,
    Match,
}

/// PG `CopyFormatOptions` (M13 subset): the parsed COPY formatting options. The
/// error-handling (`on_error`/`reject_limit`/`log_verbosity`), encoding, and the
/// force_* CSV flag lists are staged.
#[derive(Debug, Clone)]
pub struct CopyFormatOptions {
    /// binary format?
    pub binary: bool,
    /// CSV format?
    pub csv_mode: bool,
    /// header line present?
    pub header_line: CopyHeaderChoice,
    /// NULL marker string (e.g. `\N` for text, `` for CSV).
    pub null_print: String,
    /// column delimiter (one byte).
    pub delim: char,
    /// CSV quote char.
    pub quote: char,
    /// CSV escape char.
    pub escape: char,
}

impl Default for CopyFormatOptions {
    fn default() -> Self {
        Self {
            binary: false,
            csv_mode: false,
            header_line: CopyHeaderChoice::False,
            null_print: String::new(),
            delim: '\t',
            quote: '"',
            escape: '"',
        }
    }
}

/// Raise a COPY syntax `ERROR` (catchable). Used for option conflicts and the
/// not-yet-supported COPY variants.
#[cold]
#[allow(
    clippy::needless_pass_by_value,
    reason = "ereport!'s Fn closure borrows the message; an owned String lets it clone"
)]
pub(super) fn copy_error(code: i32, msg: String) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(code).errmsg(msg.clone());
    });
    unreachable!("ereport!(ERROR) does not return")
}

/// Extract a one-byte (single-`char`) string option value from a DefElem, raising
/// if it is absent, multi-char, or a newline/CR.
fn def_get_char(def: &DefElem, optname: &str) -> char {
    let s = def_get_string(def, optname);
    let mut chars = s.chars();
    let (Some(c), None) = (chars.next(), chars.next()) else {
        copy_error(
            crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!("COPY {optname} must be a single one-byte character"),
        );
    };
    c
}

/// Extract a string option value from a DefElem. The grammar builds option values
/// as `A_Const(String)` (legacy + new generic strings) or `String_` value nodes.
fn def_get_string(def: &DefElem, optname: &str) -> String {
    let Some(arg) = def.arg.as_ref() else {
        copy_error(
            crate::utils::errcodes::ERRCODE_SYNTAX_ERROR,
            format!("COPY option \"{optname}\" requires a value"),
        );
    };
    match arg {
        Node::A_Const(c) => match &c.val {
            ValUnion::String(s) => s.sval.clone(),
            ValUnion::Integer(i) => i.ival.to_string(),
            other => copy_error(
                crate::utils::errcodes::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("COPY option \"{optname}\" has an unsupported value: {other:?}"),
            ),
        },
        Node::String_(s) => s.sval.clone(),
        other => copy_error(
            crate::utils::errcodes::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("COPY option \"{optname}\" has an unsupported value: {other:?}"),
        ),
    }
}

/// Extract a boolean option value from a DefElem. A missing value means `true`
/// (e.g. bare `HEADER`); `A_Const(Integer)` 0/1 and the string forms are handled.
fn def_get_boolean(def: &DefElem) -> bool {
    let Some(arg) = def.arg.as_ref() else {
        return true;
    };
    match arg {
        Node::A_Const(c) => match &c.val {
            ValUnion::Integer(i) => i.ival != 0,
            ValUnion::Boolean(b) => b.boolval,
            ValUnion::String(s) => matches!(s.sval.to_ascii_lowercase().as_str(), "true" | "on" | "1"),
            _ => true,
        },
        Node::String_(s) => matches!(s.sval.to_ascii_lowercase().as_str(), "true" | "on" | "1"),
        _ => true,
    }
}

/// PG `ProcessCopyOptions`: fold the `CopyStmt` option list into a
/// `CopyFormatOptions`, applying the format defaults and the incompatibility
/// checks (the common subset). `is_from` toggles the FROM-only defaults.
#[allow(clippy::too_many_lines, reason = "faithful flat fold over the COPY option set + the defaults/validation block")]
pub fn process_copy_options(options: &[Node], is_from: bool) -> CopyFormatOptions {
    use crate::utils::errcodes::{
        ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
    };

    let mut out = CopyFormatOptions::default();
    let mut format_specified = false;
    let mut header_specified = false;
    let mut delim_specified = false;
    let mut null_specified = false;
    let mut quote_specified = false;
    let mut escape_specified = false;

    for opt in options {
        let Node::DefElem(defel) = opt else {
            unreachable!("COPY option list holds DefElem nodes");
        };
        let name = defel.defname.as_deref().unwrap_or("");
        match name {
            "format" => {
                if format_specified {
                    copy_error(ERRCODE_SYNTAX_ERROR, "conflicting or redundant options".into());
                }
                format_specified = true;
                match def_get_string(defel, "format").as_str() {
                    "text" => {}
                    "csv" => out.csv_mode = true,
                    "binary" => out.binary = true,
                    other => copy_error(
                        ERRCODE_INVALID_PARAMETER_VALUE,
                        format!("COPY format \"{other}\" not recognized"),
                    ),
                }
            }
            "delimiter" => {
                if delim_specified {
                    copy_error(ERRCODE_SYNTAX_ERROR, "conflicting or redundant options".into());
                }
                delim_specified = true;
                out.delim = def_get_char(defel, "delimiter");
            }
            "null" => {
                if null_specified {
                    copy_error(ERRCODE_SYNTAX_ERROR, "conflicting or redundant options".into());
                }
                null_specified = true;
                out.null_print = def_get_string(defel, "null");
            }
            "header" => {
                if header_specified {
                    copy_error(ERRCODE_SYNTAX_ERROR, "conflicting or redundant options".into());
                }
                header_specified = true;
                // HEADER MATCH (FROM only) arrives as the string "match".
                if let Some(Node::String_(s)) = defel.arg.as_ref()
                    && s.sval.eq_ignore_ascii_case("match")
                {
                    out.header_line = CopyHeaderChoice::Match;
                    continue;
                }
                out.header_line = if def_get_boolean(defel) {
                    CopyHeaderChoice::True
                } else {
                    CopyHeaderChoice::False
                };
            }
            "quote" => {
                if quote_specified {
                    copy_error(ERRCODE_SYNTAX_ERROR, "conflicting or redundant options".into());
                }
                quote_specified = true;
                out.quote = def_get_char(defel, "quote");
            }
            "escape" => {
                if escape_specified {
                    copy_error(ERRCODE_SYNTAX_ERROR, "conflicting or redundant options".into());
                }
                escape_specified = true;
                out.escape = def_get_char(defel, "escape");
            }
            other => copy_error(
                ERRCODE_SYNTAX_ERROR,
                format!("option \"{other}\" not recognized"),
            ),
        }
    }

    // Incompatible-option checks (before applying defaults).
    if out.binary && (delim_specified || null_specified) {
        copy_error(ERRCODE_SYNTAX_ERROR, "cannot specify DELIMITER or NULL in BINARY mode".into());
    }

    // Defaults for omitted options.
    if !delim_specified {
        out.delim = if out.csv_mode { ',' } else { '\t' };
    }
    if !null_specified {
        out.null_print = if out.csv_mode { String::new() } else { "\\N".into() };
    }

    // Validate (the common subset).
    if out.null_print.contains(['\r', '\n']) {
        copy_error(
            ERRCODE_INVALID_PARAMETER_VALUE,
            "COPY null representation cannot use newline or carriage return".into(),
        );
    }
    if !out.csv_mode
        && "\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(out.delim)
    {
        copy_error(
            ERRCODE_INVALID_PARAMETER_VALUE,
            format!("COPY delimiter cannot be \"{}\"", out.delim),
        );
    }
    if quote_specified && !out.csv_mode {
        copy_error(ERRCODE_FEATURE_NOT_SUPPORTED, "COPY QUOTE requires CSV mode".into());
    }
    if escape_specified && !out.csv_mode {
        copy_error(ERRCODE_FEATURE_NOT_SUPPORTED, "COPY ESCAPE requires CSV mode".into());
    }
    if out.csv_mode && out.delim == out.quote {
        copy_error(
            ERRCODE_INVALID_PARAMETER_VALUE,
            "COPY delimiter and quote must be different".into(),
        );
    }
    if header_specified && out.header_line == CopyHeaderChoice::Match && !is_from {
        copy_error(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot use HEADER MATCH with COPY TO".into(),
        );
    }

    out
}

/// PG `CopyGetAttnums`: resolve the COPY column list to a vector of 1-based attnums.
/// An empty `attlist` means all non-dropped columns in attnum order. Errors on an
/// unknown or duplicated column name.
pub(super) fn copy_get_attnums(rel: &RelationData, attlist: &[Node]) -> Vec<i16> {
    let desc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("COPY relation has a rowtype descriptor"));

    if attlist.is_empty() {
        let mut attnums = Vec::new();
        for i in 0..desc.natts as usize {
            let att = desc.attr(i);
            if att.attisdropped {
                continue;
            }
            attnums.push(att.attnum);
        }
        return attnums;
    }

    let mut attnums = Vec::with_capacity(attlist.len());
    for node in attlist {
        let Node::String_(name_node) = node else {
            unreachable!("COPY attlist holds String value nodes");
        };
        let name = &name_node.sval;
        let mut found = None;
        for i in 0..desc.natts as usize {
            let att = desc.attr(i);
            if att.attisdropped {
                continue;
            }
            if att_name(att) == *name {
                found = Some(att.attnum);
                break;
            }
        }
        let Some(attnum) = found else {
            copy_error(
                crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN,
                format!("column \"{name}\" of relation \"{}\" does not exist", rel_name(rel)),
            );
        };
        if attnums.contains(&attnum) {
            copy_error(
                crate::utils::errcodes::ERRCODE_DUPLICATE_COLUMN,
                format!("column \"{name}\" specified more than once"),
            );
        }
        attnums.push(attnum);
    }
    attnums
}

/// Read a `FormData_pg_attribute`'s `attname` as a String.
pub(super) fn att_name(att: &crate::catalog::pg_attribute::FormData_pg_attribute) -> String {
    let bytes = crate::c::NameStr(&att.attname);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

fn rel_name(rel: &RelationData) -> String {
    rel.rd_rel
        .as_ref()
        .map(|r| {
            let bytes = crate::c::NameStr(&r.relname);
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            String::from_utf8_lossy(&bytes[..end]).into_owned()
        })
        .unwrap_or_default()
}

/// `CopyAttr.attnum` is read in `copyto`/`copyfrom`; keep the field non-dead.
impl CopyAttr {
    #[must_use]
    pub(super) fn attnum0(&self) -> usize {
        self.attnum as usize - 1
    }
}

/// The type-IO metadata of one COPY column: the type OID, typmod, and the resolved
/// input or output function OID.
pub(super) struct CopyAttr {
    pub attnum: i16,
    pub typoid: Oid,
    pub typmod: i32,
}

/// Build the per-column `CopyAttr` list for the given attnums (1-based).
pub(super) fn copy_attrs(rel: &RelationData, attnums: &[i16]) -> Vec<CopyAttr> {
    let desc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("COPY relation has a rowtype descriptor"));
    attnums
        .iter()
        .map(|&attnum| {
            let att = desc.attr(attnum as usize - 1);
            CopyAttr { attnum, typoid: att.atttypid, typmod: att.atttypmod }
        })
        .collect()
}

/// Open the COPY target relation from its `RangeVar`. Mirrors the parse-analysis
/// `open_table_for_parse` path (resolve the OID via the async catalog scan, ensure
/// the relcache entry is built) rather than `table_openrv`, since this milestone's
/// wire backend has no registered PGPROC for the `AcceptInvalidationMessages` that
/// `relation_openrv` performs. The faithful heavyweight lock is approximated by the
/// relcache build (single-statement, no concurrent DDL).
async fn copy_open_relation(shared: &Arc<SharedState>, rv: &crate::nodes::primnodes::RangeVar) -> Arc<RelationData> {
    use crate::backend::catalog::namespace::range_var_get_relid;
    use crate::backend::utils::cache::relcache::{relation_build_desc, relation_id_get_relation};

    let oid = range_var_get_relid(shared, rv.schemaname.as_deref(), rv.relname.as_deref().unwrap_or("")).await;
    let Some(oid) = oid else {
        copy_error(
            crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE,
            format!("relation \"{}\" does not exist", rv.relname.as_deref().unwrap_or("")),
        );
    };
    if let Some(rel) = relation_id_get_relation(oid) {
        return rel;
    }
    relation_build_desc(shared, oid).await.unwrap_or_else(|| {
        copy_error(
            crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE,
            format!("relation \"{}\" does not exist", rv.relname.as_deref().unwrap_or("")),
        )
    })
}

/// PG `DoCopy`: the COPY dispatch. Returns the processed-row count.
pub async fn do_copy(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &CopyStmt,
    _stmt_location: i32,
    _stmt_len: i32,
) -> u64 {
    use crate::utils::errcodes::{ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR};

    // STDIN/STDOUT (pipe) and PROGRAM are staged: a server file path is required.
    if stmt.is_program {
        copy_error(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "COPY ... PROGRAM is not yet supported".into(),
        );
    }
    let Some(filename) = stmt.filename.clone() else {
        copy_error(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "COPY to/from STDIN/STDOUT is not yet supported (use a file path)".into(),
        );
    };

    if !stmt.is_from && stmt.whereClause.is_some() {
        copy_error(ERRCODE_SYNTAX_ERROR, "WHERE clause not allowed with COPY TO".into());
    }

    let opts = process_copy_options(&stmt.options, stmt.is_from);
    if opts.binary {
        copy_error(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "COPY BINARY format is not yet supported".into(),
        );
    }

    if stmt.is_from {
        let Some(relvar) = stmt.relation.as_ref() else {
            unreachable!("COPY FROM always names a relation");
        };
        let rel = copy_open_relation(shared, relvar).await;
        crate::backend::commands::copyfrom::copy_from(shared, &rel, stmt, &opts, &filename).await
    } else if let Some(query) = stmt.query.as_ref() {
        // COPY (query) TO: run the query through SPI and stream its rows out.
        crate::backend::commands::copyto::copy_to_query(shared, pstate, query, &opts, &filename)
            .await
    } else {
        let Some(relvar) = stmt.relation.as_ref() else {
            unreachable!("COPY TO names a relation or a query");
        };
        let rel = copy_open_relation(shared, relvar).await;
        let attnums = copy_get_attnums(&rel, &stmt.attlist);
        crate::backend::commands::copyto::copy_to_relation(shared, &rel, &attnums, &opts, &filename)
            .await
    }
}

use crate::shared_state::SharedState;
