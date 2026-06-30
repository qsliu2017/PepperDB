//! Generic ALTER / RENAME dispatch by object type. Translated from the
//! M10-reachable parts of `src/backend/commands/alter.c` (disposition: full leaf
//! for the dispatch; the per-object work lives in the object's command module).
//!
//! `exec_rename_stmt` is the `RenameStmt` dispatcher: route a RENAME to the right
//! catalog routine by `renameType`. M10 reaches RENAME of a table/index/view and of
//! a column; `exec_alter_object_schema_stmt` / `alter_object_namespace` (SET SCHEMA)
//! are staged guards (the namespace machinery for the relocate path is M11+).
//!
//! Async coloring (rules.md s5): the renames mutate catalog rows (buffer pool), so
//! the dispatchers are `async` and thread `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::catalog::namespace::range_var_get_relid;
use crate::nodes::parsenodes::{ObjectType, RenameStmt};
use crate::shared_state::SharedState;

/// Panic for an ALTER/RENAME object kind not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `ExecRenameStmt`: dispatch a RENAME by object type. A column rename
/// (`renameType == COLUMN`) routes to `renameatt`; a relation rename
/// (TABLE/INDEX/VIEW/...) routes to `RenameRelation`. The non-relation object
/// renames (TYPE, SCHEMA, FUNCTION, ...) STAGE.
pub async fn exec_rename_stmt(shared: &Arc<SharedState>, stmt: &RenameStmt) {
    match stmt.renameType {
        ObjectType::COLUMN => rename_column(shared, stmt).await,
        ObjectType::TABLE
        | ObjectType::INDEX
        | ObjectType::VIEW
        | ObjectType::MATVIEW
        | ObjectType::SEQUENCE
        | ObjectType::FOREIGN_TABLE => rename_relation(shared, stmt).await,
        other => not_yet_reachable(&format!("ExecRenameStmt: {other:?}")),
    }
}

/// PG `RenameRelation` (the relcache+catalog rename driver): resolve the relation,
/// then `RenameRelationInternal`.
async fn rename_relation(shared: &Arc<SharedState>, stmt: &RenameStmt) {
    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("ALTER ... RENAME names a relation"));
    let relname = relation
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("RangeVar names the relation"));
    let relid = range_var_get_relid(shared, relation.schemaname.as_deref(), relname).await;
    let Some(relid) = relid else {
        if stmt.missing_ok {
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("relation \"{relname}\" does not exist, skipping"));
            });
            return;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!("relation \"{relname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };
    let newname = stmt
        .newname
        .as_deref()
        .unwrap_or_else(|| unreachable!("RENAME TO names the new name"));
    crate::backend::commands::tablecmds::rename_relation(shared, relid, newname).await;
}

/// PG `renameatt` driver: resolve the relation, then `renameatt`.
async fn rename_column(shared: &Arc<SharedState>, stmt: &RenameStmt) {
    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("ALTER TABLE ... RENAME COLUMN names a relation"));
    let relname = relation
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("RangeVar names the relation"));
    let relid = range_var_get_relid(shared, relation.schemaname.as_deref(), relname).await;
    let Some(relid) = relid else {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!("relation \"{relname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };
    let oldname = stmt
        .subname
        .as_deref()
        .unwrap_or_else(|| unreachable!("RENAME COLUMN names the old column"));
    let newname = stmt
        .newname
        .as_deref()
        .unwrap_or_else(|| unreachable!("RENAME COLUMN names the new name"));
    crate::backend::commands::tablecmds::rename_att(shared, relid, oldname, newname).await;
}

/// PG `ExecAlterObjectSchemaStmt` / `AlterObjectNamespace`: relocate an object to a
/// new schema (ALTER ... SET SCHEMA). STAGED (rules.md s4): the namespace-relocate
/// machinery (RangeVarGetAndCheckCreationNamespace + pg_class.relnamespace update +
/// the dependent-object move) lands with multi-schema support at M11+.
#[cold]
pub fn exec_alter_object_schema_stmt(_stmt: &crate::nodes::parsenodes::AlterObjectSchemaStmt) {
    not_yet_reachable("ExecAlterObjectSchemaStmt: SET SCHEMA");
}
