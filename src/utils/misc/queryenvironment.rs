//! Query environment - store context-specific values like ephemeral named
//! relations (named tuplestores for delta information from "normal" relations).
//!
//! Source: postgres/src/backend/utils/misc/queryenvironment.c
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/table.h"              -> table_open/table_close STUBBED (Relation opaque;
//!                                    only used by the catalog-tupdesc branch of
//!                                    ENRMetadataGetTupDesc)
//!   "utils/queryenvironment.h"    -> MERGED here (enum + Metadata/Relation/QueryEnvironment)
//!   "utils/rel.h"                 -> Relation->rd_att access STUBBED (catalog branch)
//!
//! The List-based core is FULLY REAL over the ported crate::nodes::pg_list.

use crate::prelude::*;

use crate::access::common::tupdesc::TupleDesc;
use crate::nodes::pg_list::{lappend, list_delete_ptr, lfirst, List};
use crate::postgres_ext::Oid;

// foreach!/current_cell! are #[macro_export] at the crate root.
use crate::{current_cell, foreach};

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

// ---- merged from utils/queryenvironment.h ----

/// `typedef enum EphemeralNameRelationType`.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EphemeralNameRelationType {
    /// named tuplestore relation; e.g., deltas
    ENR_NAMED_TUPLESTORE = 0,
}
pub use EphemeralNameRelationType::*;

/// `EphemeralNamedRelationMetadataData`.
///
/// Some ENRs must match some relation (e.g., trigger transition tables), so we
/// carry the OID of that relation (`reliddesc`).  Others are independent of any
/// catalog relation, so we store the `tupdesc` directly.  We never need both.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EphemeralNamedRelationMetadataData {
    /// name used to identify the relation
    pub name: *mut c_char,

    // only one of the next two fields should be used
    /// oid of relation to get tupdesc
    pub reliddesc: Oid,
    /// description of result rows
    pub tupdesc: TupleDesc,

    /// to identify type of relation
    pub enrtype: EphemeralNameRelationType,
    /// estimated number of tuples
    pub enrtuples: f64,
}

/// `typedef EphemeralNamedRelationMetadataData *EphemeralNamedRelationMetadata`.
pub type EphemeralNamedRelationMetadata = *mut EphemeralNamedRelationMetadataData;

/// `EphemeralNamedRelationData`; used for parsing named relations not in the
/// catalog, like transition tables in AFTER triggers.
#[repr(C)]
pub struct EphemeralNamedRelationData {
    pub md: EphemeralNamedRelationMetadataData,
    /// structure for execution-time access to data
    pub reldata: *mut c_void,
}

/// `typedef EphemeralNamedRelationData *EphemeralNamedRelation`.
pub type EphemeralNamedRelation = *mut EphemeralNamedRelationData;

/// Private state of a query environment.
///
/// This is an opaque structure outside of queryenvironment.c itself.
#[repr(C)]
pub struct QueryEnvironment {
    pub namedRelList: *mut List,
}

// ---- queryenvironment.c ----

/// `create_queryEnv(void)` - palloc0 a fresh, empty query environment.
///
/// # Safety
/// Allocates via `palloc0`; caller owns the returned pointer.
pub unsafe fn create_queryEnv() -> *mut QueryEnvironment {
    palloc0(core::mem::size_of::<QueryEnvironment>()) as *mut QueryEnvironment
}

/// `get_visible_ENR_metadata` - metadata for the ENR named `refname`, or null.
///
/// # Safety
/// `refname` must be a valid NUL-terminated C string; `queryEnv` null or valid.
pub unsafe fn get_visible_ENR_metadata(
    queryEnv: *mut QueryEnvironment,
    refname: *const c_char,
) -> EphemeralNamedRelationMetadata {
    Assert!(!refname.is_null());

    if queryEnv.is_null() {
        return null_mut();
    }

    let enr = get_ENR(queryEnv, refname);

    if !enr.is_null() {
        return core::ptr::addr_of_mut!((*enr).md);
    }

    null_mut()
}

/// Register a named relation for use in the given environment.
///
/// If this is intended exclusively for planning purposes, the `reldata` field
/// can be left NULL.
///
/// # Safety
/// `queryEnv` and `enr` must be valid; `enr->md.name` a valid C string.
pub unsafe fn register_ENR(queryEnv: *mut QueryEnvironment, enr: EphemeralNamedRelation) {
    Assert!(!enr.is_null());
    Assert!(get_ENR(queryEnv, (*enr).md.name).is_null());

    (*queryEnv).namedRelList = lappend((*queryEnv).namedRelList, enr as *mut c_void);
}

/// Unregister an ephemeral relation by name.  Rarely used, but provided "just
/// in case".
///
/// # Safety
/// `queryEnv` must be valid; `name` a valid C string.
pub unsafe fn unregister_ENR(queryEnv: *mut QueryEnvironment, name: *const c_char) {
    let match_ = get_ENR(queryEnv, name);
    if !match_.is_null() {
        // get_ENR returns the exact stored pointer, so pointer equality
        // (list_delete_ptr) finds it; equivalent to C's list_delete here.
        (*queryEnv).namedRelList = list_delete_ptr((*queryEnv).namedRelList, match_ as *mut c_void);
    }
}

/// Return an ENR if there is a name match in the given collection.  Quietly
/// returns NULL if no match is found.
///
/// # Safety
/// `queryEnv` null or valid; `name` a valid C string.
pub unsafe fn get_ENR(
    queryEnv: *mut QueryEnvironment,
    name: *const c_char,
) -> EphemeralNamedRelation {
    Assert!(!name.is_null());

    if queryEnv.is_null() {
        return null_mut();
    }

    foreach!(lc, (*queryEnv).namedRelList, {
        let enr = lfirst(current_cell!(lc)) as EphemeralNamedRelation;

        if strcmp((*enr).md.name, name) == 0 {
            return enr;
        }
    });

    null_mut()
}

/// Get the `TupleDesc` for an Ephemeral Named Relation, based on which field
/// was filled.
///
/// When the TupleDesc is based on a catalog relation, we count on that relation
/// being used at the same time, so appropriate locks will already be held.
///
/// # Safety
/// `enrmd` must point to a valid metadata struct with exactly one of
/// `reliddesc`/`tupdesc` filled.
pub unsafe fn ENRMetadataGetTupDesc(enrmd: EphemeralNamedRelationMetadata) -> TupleDesc {
    // One, and only one, of these fields must be filled.
    Assert!(((*enrmd).reliddesc == InvalidOid) != ((*enrmd).tupdesc.is_null()));

    let tupdesc: TupleDesc;
    if !(*enrmd).tupdesc.is_null() {
        tupdesc = (*enrmd).tupdesc;
    } else {
        // STUB: catalog-relation branch. Relation/table_open/table_close/rd_att
        // are not yet ported (Relation is opaque). Preserves the original C body
        // shape; unreachable for tupdesc-backed ENRs (the common ENR use).
        //
        //   Relation relation = table_open(enrmd->reliddesc, NoLock);
        //   tupdesc = relation->rd_att;
        //   table_close(relation, NoLock);
        // TODO: wire to access/table.rs + utils/rel.rs once Relation is ported.
        unimplemented!("ENRMetadataGetTupDesc: catalog-relation (reliddesc) branch needs Relation/table_open");
    }

    tupdesc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    // Build a tupdesc-backed ENR with the given name and a sentinel tupdesc ptr.
    unsafe fn make_enr(name: &CString, tupdesc: TupleDesc) -> EphemeralNamedRelation {
        let enr = palloc0(core::mem::size_of::<EphemeralNamedRelationData>())
            as EphemeralNamedRelation;
        (*enr).md.name = name.as_ptr() as *mut c_char;
        (*enr).md.reliddesc = InvalidOid;
        (*enr).md.tupdesc = tupdesc;
        (*enr).md.enrtype = ENR_NAMED_TUPLESTORE;
        (*enr).md.enrtuples = 0.0;
        enr
    }

    #[test]
    fn register_lookup_unregister() {
        unsafe {
            let qe = create_queryEnv();
            assert!(!qe.is_null());
            assert!((*qe).namedRelList.is_null());

            let name_a = CString::new("delta_a").unwrap();
            let name_b = CString::new("delta_b").unwrap();
            // Distinct sentinel TupleDesc pointers (never dereferenced here).
            let td_a = 0x1000usize as TupleDesc;
            let td_b = 0x2000usize as TupleDesc;

            let enr_a = make_enr(&name_a, td_a);
            let enr_b = make_enr(&name_b, td_b);

            register_ENR(qe, enr_a);
            register_ENR(qe, enr_b);

            // get_ENR finds both by name.
            assert_eq!(get_ENR(qe, name_a.as_ptr()), enr_a);
            assert_eq!(get_ENR(qe, name_b.as_ptr()), enr_b);

            // unknown name -> null
            let unknown = CString::new("nope").unwrap();
            assert!(get_ENR(qe, unknown.as_ptr()).is_null());

            // get_visible_ENR_metadata returns &md of the match.
            let md_a = get_visible_ENR_metadata(qe, name_a.as_ptr());
            assert_eq!(md_a, core::ptr::addr_of_mut!((*enr_a).md));
            assert!(get_visible_ENR_metadata(qe, unknown.as_ptr()).is_null());

            // ENRMetadataGetTupDesc returns the stored tupdesc.
            assert_eq!(ENRMetadataGetTupDesc(md_a), td_a);

            // unregister one; the other remains.
            unregister_ENR(qe, name_a.as_ptr());
            assert!(get_ENR(qe, name_a.as_ptr()).is_null());
            assert_eq!(get_ENR(qe, name_b.as_ptr()), enr_b);
        }
    }

    #[test]
    fn null_env_returns_null() {
        unsafe {
            let name = CString::new("x").unwrap();
            assert!(get_ENR(null_mut(), name.as_ptr()).is_null());
            assert!(get_visible_ENR_metadata(null_mut(), name.as_ptr()).is_null());
        }
    }
}
