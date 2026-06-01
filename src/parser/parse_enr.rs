//! parser support routines dealing with ephemeral named relations.
//!
//! Source: postgres/src/backend/parser/parse_enr.c
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   "parser/parse_enr.h"       -> MERGED here (the two extern decls)
//!     which pulls in "parser/parse_node.h" (ParseState) -> minimal local STUB
//!     (see `ParseState` below; only `p_queryEnv` is modeled).
//!
//! The ENR-lookup logic is FULLY REAL over the already-ported
//! crate::utils::misc::queryenvironment.

use crate::prelude::*;

use crate::utils::misc::queryenvironment::{
    get_visible_ENR_metadata, EphemeralNamedRelationMetadata, QueryEnvironment,
};
/* Use the real ParseState from parse_node (it has p_queryEnv). */
use crate::parser::parse_node::ParseState;

/// `name_matches_visible_ENR` - true iff an ENR named `refname` is visible in
/// the parse state's query environment.
///
/// # Safety
/// `pstate` must be a valid pointer; `refname` a valid NUL-terminated C string.
pub unsafe fn name_matches_visible_ENR(pstate: *mut ParseState, refname: *const c_char) -> bool {
    !get_visible_ENR_metadata((*pstate).p_queryEnv as *mut QueryEnvironment, refname).is_null()
}

/// `get_visible_ENR` - metadata for the ENR named `refname` visible in the parse
/// state's query environment, or null.
///
/// # Safety
/// `pstate` must be a valid pointer; `refname` a valid NUL-terminated C string.
pub unsafe fn get_visible_ENR(
    pstate: *mut ParseState,
    refname: *const c_char,
) -> EphemeralNamedRelationMetadata {
    get_visible_ENR_metadata((*pstate).p_queryEnv as *mut QueryEnvironment, refname)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::misc::queryenvironment::{
        create_queryEnv, register_ENR, EphemeralNamedRelationData, ENR_NAMED_TUPLESTORE,
    };
    use crate::postgres_ext::InvalidOid;

    // Build a stack-resident ENR with the given C-string name and register it.
    // The name pointer must outlive the environment usage.
    unsafe fn make_enr(name: *mut c_char) -> *mut EphemeralNamedRelationData {
        let enr = palloc0(core::mem::size_of::<EphemeralNamedRelationData>())
            as *mut EphemeralNamedRelationData;
        (*enr).md.name = name;
        (*enr).md.reliddesc = InvalidOid;
        (*enr).md.tupdesc = null_mut();
        (*enr).md.enrtype = ENR_NAMED_TUPLESTORE;
        (*enr).md.enrtuples = 0.0;
        (*enr).reldata = null_mut();
        enr
    }

    #[test]
    fn visible_enr_lookup() {
        unsafe {
            let env = create_queryEnv();
            /* use zeroed ParseState and set only p_queryEnv */
            let mut pstate: ParseState = core::mem::zeroed();
            pstate.p_queryEnv = env as *mut core::ffi::c_void;

            // c-string literals are NUL-terminated.
            let present = b"delta\0".as_ptr() as *mut c_char;
            let absent = b"missing\0".as_ptr() as *const c_char;

            register_ENR(env, make_enr(present));

            let p = &mut pstate as *mut ParseState;
            assert!(name_matches_visible_ENR(p, present as *const c_char));
            assert!(!name_matches_visible_ENR(p, absent));

            assert!(!get_visible_ENR(p, present as *const c_char).is_null());
            assert!(get_visible_ENR(p, absent).is_null());
        }
    }

    #[test]
    fn null_query_env_is_not_visible() {
        unsafe {
            let mut pstate: ParseState = core::mem::zeroed();
            /* p_queryEnv already null from zeroed() */
            let p = &mut pstate as *mut ParseState;
            let name = b"x\0".as_ptr() as *const c_char;
            assert!(!name_matches_visible_ENR(p, name));
            assert!(get_visible_ENR(p, name).is_null());
        }
    }
}
