//! Generic routines for table related code.
//!
//! Port of postgres/src/backend/access/table/table.c (PostgreSQL 18.3),
//! merged with the relevant declarations from access/table.h.
//!
//! This file contains `table_` routines that implement access to tables (in
//! contrast to other relation types like indexes) that are independent of
//! individual table access methods.
//!
//! NOTE: `relation_open` / `relation_openrv` / `relation_openrv_extended` /
//! `try_relation_open` / `relation_close` live in access/relation.c, which has
//! NOT been ported yet. They are STUBBED here with `unimplemented!()`. The
//! `table_open` / `try_table_open` / `table_openrv` / `table_openrv_extended` /
//! `table_close` bodies themselves are faithful 1:1 translations (they call the
//! stubbed `relation_open` family then run the REAL `validate_relation_kind`),
//! so they are correct in shape but not yet runnable end-to-end until
//! access/relation.c lands.

use crate::prelude::*;

// Relation (= *mut RelationData) is the PORTED handle type from the executor
// node defs (utils/rel.h `typedef struct RelationData *Relation`).
use crate::nodes::execnodes::{Relation, RelationData};

// RangeVar is a PORTED parsenode (nodes/primnodes.h, via primnodes.rs).
use crate::nodes::primnodes::RangeVar;

// Form_pg_class is the PORTED `*mut FormData_pg_class` (the fixed part of a
// pg_class row), and the RELKIND_* discriminants are PORTED catalog consts.
use crate::catalog::pg_class::{
    Form_pg_class, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
};

/// `typedef int LOCKMODE;` (storage/lockdefs.h).
pub type LOCKMODE = c_int;

// ----------------------------------------------------------------------------
// Stubs for the access/relation.c family (NOT yet ported).
//
// Each mirrors the C signature so the `table_` wrappers below are exact
// translations over them. They panic via `unimplemented!()` rather than
// returning, so the value-position `table_*` functions need no trailing
// `unreachable!()`.
// ----------------------------------------------------------------------------

/// STUB: `relation_open` (access/relation.c not ported).
unsafe fn relation_open(_relation_id: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!("TODO(pg-port): relation_open (access/relation.c not ported)")
}

/// STUB: `try_relation_open` (access/relation.c not ported).
unsafe fn try_relation_open(_relation_id: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!("TODO(pg-port): try_relation_open (access/relation.c not ported)")
}

/// STUB: `relation_openrv` (access/relation.c not ported).
unsafe fn relation_openrv(_relation: *const RangeVar, _lockmode: LOCKMODE) -> Relation {
    unimplemented!("TODO(pg-port): relation_openrv (access/relation.c not ported)")
}

/// STUB: `relation_openrv_extended` (access/relation.c not ported).
unsafe fn relation_openrv_extended(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> Relation {
    unimplemented!("TODO(pg-port): relation_openrv_extended (access/relation.c not ported)")
}

/// STUB: `relation_close` (access/relation.c not ported).
unsafe fn relation_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!("TODO(pg-port): relation_close (access/relation.c not ported)")
}

/// `validate_relation_kind` -- check the relation's kind.
///
/// Make sure relkind is not index, partitioned index, or composite type.
///
/// REAL translation: reads `r->rd_rel->relkind` and `ereport(ERROR)`s for the
/// disallowed kinds. (`errdetail_relkind_not_supported` is folded into the
/// message text since that helper is not yet ported.)
unsafe fn validate_relation_kind(r: Relation) {
    let relkind = (*(*r).rd_rel).relkind;

    if relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX
        || relkind == RELKIND_COMPOSITE_TYPE
    {
        // C: ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
        //          errmsg("cannot open relation \"%s\"", RelationGetRelationName(r)),
        //          errdetail_relkind_not_supported(r->rd_rel->relkind)));
        ereport!(
            ERROR,
            errmsg!(
                "cannot open relation: relkind '{}' not supported as a table",
                relkind as u8 as char
            )
        );
    }
}

/// `table_open` -- open a table relation by relation OID.
///
/// This is essentially `relation_open` plus a check that the relation is not an
/// index nor a composite type. (The caller should also check that it's not a
/// view or foreign table before assuming it has storage.)
pub unsafe fn table_open(relation_id: Oid, lockmode: LOCKMODE) -> Relation {
    let r = relation_open(relation_id, lockmode);

    validate_relation_kind(r);

    r
}

/// `try_table_open` -- open a table relation by relation OID.
///
/// Same as `table_open`, except return NULL instead of failing if the relation
/// does not exist.
pub unsafe fn try_table_open(relation_id: Oid, lockmode: LOCKMODE) -> Relation {
    let r = try_relation_open(relation_id, lockmode);

    // leave if table does not exist
    if r.is_null() {
        return null_mut();
    }

    validate_relation_kind(r);

    r
}

/// `table_openrv` -- open a table relation specified by a RangeVar node.
///
/// As `table_open`, but the relation is specified by a RangeVar.
pub unsafe fn table_openrv(relation: *const RangeVar, lockmode: LOCKMODE) -> Relation {
    let r = relation_openrv(relation, lockmode);

    validate_relation_kind(r);

    r
}

/// `table_openrv_extended` -- open a table relation specified by a RangeVar
/// node.
///
/// As `table_openrv`, but optionally return NULL instead of failing for
/// relation-not-found.
pub unsafe fn table_openrv_extended(
    relation: *const RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> Relation {
    let r = relation_openrv_extended(relation, lockmode, missing_ok);

    if !r.is_null() {
        validate_relation_kind(r);
    }

    r
}

/// `table_close` -- close a table.
///
/// If lockmode is not "NoLock", we then release the specified lock.
///
/// Note that it is often sensible to hold a lock beyond `relation_close`; in
/// that case, the lock is released automatically at xact end.
pub unsafe fn table_close(relation: Relation, lockmode: LOCKMODE) {
    relation_close(relation, lockmode);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::pg_class::{FormData_pg_class, RELKIND_RELATION};
    use std::mem::MaybeUninit;

    // Build a real (zeroed) RelationData whose rd_rel Form has the relkind we
    // choose, then exercise validate_relation_kind through the Relation handle.
    unsafe fn make_relation(relkind: c_char) -> (Box<RelationData>, Box<FormData_pg_class>) {
        // Zero a FormData_pg_class and set only relkind (never partially init an
        // uninit struct piecemeal -- zero whole, then write the one field).
        let mut form: Box<MaybeUninit<FormData_pg_class>> = Box::new(MaybeUninit::zeroed());
        (*form.as_mut_ptr()).relkind = relkind;
        let form: Box<FormData_pg_class> = Box::from_raw(Box::into_raw(form) as *mut FormData_pg_class);

        let mut rel: Box<MaybeUninit<RelationData>> = Box::new(MaybeUninit::zeroed());
        (*rel.as_mut_ptr()).rd_rel = &*form as *const FormData_pg_class as Form_pg_class;
        let rel: Box<RelationData> = Box::from_raw(Box::into_raw(rel) as *mut RelationData);
        (rel, form)
    }

    #[test]
    fn rejects_index() {
        unsafe {
            let (rel, _form) = make_relation(RELKIND_INDEX);
            let r = &*rel as *const RelationData as Relation;
            let res = std::panic::catch_unwind(|| validate_relation_kind(r));
            assert!(res.is_err(), "an index relkind must be rejected");
        }
    }

    #[test]
    fn rejects_composite_type() {
        unsafe {
            let (rel, _form) = make_relation(RELKIND_COMPOSITE_TYPE);
            let r = &*rel as *const RelationData as Relation;
            let res = std::panic::catch_unwind(|| validate_relation_kind(r));
            assert!(res.is_err(), "a composite-type relkind must be rejected");
        }
    }

    #[test]
    fn accepts_relation() {
        unsafe {
            let (rel, _form) = make_relation(RELKIND_RELATION);
            let r = &*rel as *const RelationData as Relation;
            let res = std::panic::catch_unwind(|| validate_relation_kind(r));
            assert!(res.is_ok(), "an ordinary table relkind must be accepted");
        }
    }
}
