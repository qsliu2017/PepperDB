//! sequence.rs
//!   Generic routines for sequence-related code.
//!
//! This file contains sequence_ routines that implement access to sequences
//! (in contrast to other relation types like indexes).
//!
//! Translated 1:1 from postgres/src/backend/access/sequence/sequence.c
//!
//! #include "postgres.h"          -> crate::prelude::*
//! #include "access/relation.h"   -> crate::access::common::relation (relation_open/close)
//! #include "utils/rel.h"         -> crate::utils::rel (Relation, RelationGetRelationName, rd_rel)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::common::relation::{relation_close, relation_open};
use crate::catalog::pg_class::RELKIND_SEQUENCE;
use crate::storage::lockdefs::LOCKMODE;
use crate::utils::rel::{Relation, RelationGetRelationName};

// errdetail_relkind_not_supported lives in catalog/pg_class.c (not yet ported to a
// wired canonical home); stub locally as sibling ports do.  Returns 0 (errdetail
// appends to the in-flight ereport, value is unused by callers).
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int {
    0
}

/* ----------------
 *		sequence_open - open a sequence relation by relation OID
 *
 *		This is essentially relation_open plus check that the relation
 *		is a sequence.
 * ----------------
 */
pub unsafe fn sequence_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    let r: Relation;

    r = relation_open(relationId, lockmode);

    validate_relation_kind(r);

    r
}

/* ----------------
 *		sequence_close - close a sequence
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond relation_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
pub unsafe fn sequence_close(relation: Relation, lockmode: LOCKMODE) {
    relation_close(relation, lockmode);
}

/* ----------------
 *		validate_relation_kind - check the relation's kind
 *
 *		Make sure relkind is from a sequence.
 * ----------------
 */
#[inline]
unsafe fn validate_relation_kind(r: Relation) {
    if (*(*r).rd_rel).relkind != RELKIND_SEQUENCE {
        // C passes errcode(ERRCODE_WRONG_OBJECT_TYPE) and
        // errdetail_relkind_not_supported(relkind) to ereport; our ereport! macro
        // takes only (level, msg), so fold to errmsg! as sibling ports do.
        let _ = errdetail_relkind_not_supported((*(*r).rd_rel).relkind);
        ereport!(
            ERROR,
            errmsg!(
                "cannot open relation \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(r)).to_string_lossy()
            )
        );
    }
}
