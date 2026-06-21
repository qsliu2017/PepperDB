//! utils/misc/superuser.c - the superuser() function; determines if a user has
//! superuser privilege.
//!
//! All code should use either of these two functions to find out whether a given
//! user is a superuser, rather than examining pg_authid.rolsuper directly, so that
//! the escape hatch built in for the single-user case works.

use crate::prelude::*;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::catalog::pg_authid::Form_pg_authid;
use crate::catalog::pg_known_oids::BOOTSTRAP_SUPERUSERID;
use crate::miscadmin::GetUserId;

// access/transam.h - InvalidOid is provided via prelude (postgres_ext); OidIsValid
// from c.h via crate::c.

extern "C" {
    // miscadmin.h
    static mut IsUnderPostmaster: bool;
}

// utils/syscache.h - SysCacheIdentifier value for pg_authid lookup by Oid.
// TODO: not ported - utils/syscache.h.
use crate::utils::cache::syscache_ids_gen::AUTHOID;

// TODO: not ported - utils/syscache.h.
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(_cacheId, _key1) as _
}

// TODO: not ported - utils/syscache.h.
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(_tuple as _)
}

// TODO: not ported - utils/inval.h. SyscacheCallbackFunction signature.
type SyscacheCallbackFunction = unsafe fn(arg: Datum, cacheid: c_int, hashvalue: uint32);

// TODO: not ported - utils/inval.h.
unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
    crate::utils::cache::inval::CacheRegisterSyscacheCallback(_cacheid, core::mem::transmute(_func), _arg)
}

/*
 * In common cases the same roleid (ie, the session or current ID) will
 * be queried repeatedly.  So we maintain a simple one-entry cache for
 * the status of the last requested roleid.  The cache can be flushed
 * at need by watching for cache update events on pg_authid.
 */
static mut last_roleid: Oid = InvalidOid; /* InvalidOid == cache not valid */
static mut last_roleid_is_super: bool = false;
static mut roleid_callback_registered: bool = false;

/*
 * The Postgres user running this command has Postgres superuser privileges
 */
pub unsafe fn superuser() -> bool {
    superuser_arg(GetUserId())
}

/*
 * The specified role has Postgres superuser privileges
 */
pub unsafe fn superuser_arg(roleid: Oid) -> bool {
    let result: bool;
    let rtup: HeapTuple;

    /* Quick out for cache hit */
    if OidIsValid(last_roleid) && last_roleid == roleid {
        return last_roleid_is_super;
    }

    /* Special escape path in case you deleted all your users. */
    if !IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID {
        return true;
    }

    /* OK, look up the information in pg_authid */
    rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if HeapTupleIsValid(rtup) {
        result = (*(GETSTRUCT(rtup) as Form_pg_authid)).rolsuper;
        ReleaseSysCache(rtup);
    } else {
        /* Report "not superuser" for invalid roleids */
        result = false;
    }

    /* If first time through, set up callback for cache flushes */
    if !roleid_callback_registered {
        CacheRegisterSyscacheCallback(AUTHOID, RoleidCallback, 0 as Datum);
        roleid_callback_registered = true;
    }

    /* Cache the result for next time */
    last_roleid = roleid;
    last_roleid_is_super = result;

    result
}

/*
 * RoleidCallback
 *		Syscache inval callback function
 */
unsafe fn RoleidCallback(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    /* Invalidate our local cache in case role's superuserness changed */
    last_roleid = InvalidOid;
}
