//! Generic table open/close. Translated from backend/access/table/table.c.
//!
//! Thin wrappers over `relation_open`/`relation_openrv`/`relation_close` (step
//! 11, `backend/access/common/relation.rs`) that additionally reject opening an
//! index as a table. Async coloring (rules.md s5): the open routines take the
//! heavyweight relation lock (a lock-wait leaf), so they are `async`;
//! `table_close` only releases and stays sync.

use crate::backend::access::common::relation::{relation_close, relation_open, relation_openrv};
use crate::catalog::pg_class::RELKIND_INDEX;
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use crate::utils::elog::ERROR;
use crate::utils::relcache::Relation;

/// `table_open`: open a table (or other non-index relation) by OID, taking
/// `lockmode`. Errors if the relation is an index (use `index_open`).
pub async fn table_open(relation_id: Oid, lockmode: LockMode) -> Relation {
    let r = relation_open(relation_id, lockmode).await;
    validate_relation_kind(r);
    r
}

/// `table_openrv`: open a table by `RangeVar`, taking `lockmode`.
pub async fn table_openrv(relation: &RangeVar, lockmode: LockMode) -> Relation {
    let r = relation_openrv(relation, lockmode).await;
    validate_relation_kind(r);
    r
}

/// `table_close`: close a relation, releasing `lockmode` unless `NoLock`.
pub fn table_close(relation: Relation, lockmode: LockMode) {
    relation_close(relation, lockmode);
}

/// `validate_relation_kind`: reject an index opened as a table (the C check in
/// table_open / table_openrv).
fn validate_relation_kind(r: Relation) {
    // SAFETY: `r` is a live, open relation returned by relation_open*.
    let relkind = unsafe { (*(*r).rd_rel).relkind };
    if relkind == RELKIND_INDEX {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{}\" is an index", crate::utils::rel::relation_get_relation_name(unsafe { &*r })));
        });
    }
}
