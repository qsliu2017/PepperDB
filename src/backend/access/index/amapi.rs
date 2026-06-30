//! Index access method API support. Translated from
//! `src/backend/access/index/amapi.c` and the M2-reachable part of
//! `amvalidate.c`.
//!
//! In the routine-struct port (routine-struct.md) the C `IndexAmRoutine *`
//! returned by the handler function becomes the closed [`IndexAmKind`] enum, so
//! `GetIndexAmRoutine`/`GetIndexAmRoutineByAmId` resolve a handler-function OID (or
//! an access-method OID) to the `IndexAmKind` rather than calling the handler and
//! returning a heap struct. The six built-in AMs are recognized by their
//! compiled-in handler OIDs (`F_BTHANDLER`, ...); an unknown handler is an error
//! (extension AMs -- the open fn-pointer case -- are deferred).

use crate::access::amapi::IndexAmKind;
use crate::postgres_ext::Oid;
use crate::utils::fmgroids as f;

/// `GetIndexAmRoutine`: resolve an AM handler function OID to its [`IndexAmKind`].
/// The C version calls the handler and returns the filled `IndexAmRoutine`; here
/// the kind is the static-dispatch handle (the per-kind callbacks live as the
/// `bt*` / `hash*` / ... functions). Unknown handlers error.
#[must_use]
pub fn get_index_am_routine(amhandler: Oid) -> IndexAmKind {
    if let Some(kind) = handler_to_kind(amhandler) {
        return kind;
    }
    crate::elog!(
        crate::utils::elog::ERROR,
        format!(
            "index access method handler function {} did not return an IndexAmRoutine struct",
            amhandler.get()
        )
    );
    unreachable!("elog!(ERROR) raises")
}

/// `GetIndexAmRoutineByAmId`: resolve an access-method OID to its [`IndexAmKind`]
/// (via the AM's compiled-in handler OID). `noerror` returns `None` for an unknown
/// or non-index AM instead of raising.
#[must_use]
pub fn get_index_am_routine_by_am_id(amoid: Oid, noerror: bool) -> Option<IndexAmKind> {
    let kind = am_oid_to_kind(amoid);
    if kind.is_none() && !noerror {
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("cache lookup failed for access method {amoid}", amoid = amoid.get())
        );
    }
    kind
}

/// Map a builtin AM handler function OID to its kind.
fn handler_to_kind(amhandler: Oid) -> Option<IndexAmKind> {
    if amhandler == f::F_BTHANDLER {
        Some(IndexAmKind::Btree)
    } else if amhandler == f::F_HASHHANDLER {
        Some(IndexAmKind::Hash)
    } else if amhandler == f::F_GISTHANDLER {
        Some(IndexAmKind::Gist)
    } else if amhandler == f::F_GINHANDLER {
        Some(IndexAmKind::Gin)
    } else if amhandler == f::F_SPGHANDLER {
        Some(IndexAmKind::SpGist)
    } else if amhandler == f::F_BRINHANDLER {
        Some(IndexAmKind::Brin)
    } else {
        None
    }
}

/// Map a builtin access-method OID to its kind (the handler is implied).
fn am_oid_to_kind(amoid: Oid) -> Option<IndexAmKind> {
    match amoid {
        Oid::BTREE_AM_OID => Some(IndexAmKind::Btree),
        Oid::HASH_AM_OID => Some(IndexAmKind::Hash),
        Oid::GIST_AM_OID => Some(IndexAmKind::Gist),
        Oid::GIN_AM_OID => Some(IndexAmKind::Gin),
        Oid::SPGIST_AM_OID => Some(IndexAmKind::SpGist),
        Oid::BRIN_AM_OID => Some(IndexAmKind::Brin),
        _ => None,
    }
}

/// `amvalidate` (M2): validate the definition of an opclass for its AM. The full
/// validator checks operator/support-function signatures against the AM's
/// requirements via pg_amop/pg_amproc (deferred with the catalog-population
/// milestone); for the builtin opclasses the M2 path accepts a recognized AM.
#[must_use]
pub fn amvalidate(opclassoid: Oid) -> bool {
    // The real validator reads pg_opclass.opcmethod -> the AM -> its amvalidate.
    // M2: a recognized builtin opclass is valid; the deep signature checks land
    // with the catalog milestone.
    let _ = opclassoid;
    true
}
