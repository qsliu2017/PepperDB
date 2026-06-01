//! Translation of postgres/src/backend/utils/adt/version.c
//!
//! Returns the PostgreSQL version string.
//!
//! Copyright (c) 1998-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::postgres::PointerGetDatum;
use crate::utils::adt::varlena::cstring_to_text;
use core::ffi::c_void;

// pg_config.h's PG_VERSION_STR (build-generated).  TODO(pg-port): emit the real
// platform/compiler string from the build; this is the PepperDB stand-in.
const PG_VERSION_STR: &core::ffi::CStr =
    c"PostgreSQL 18.3 (PepperDB, a Rust translation of the PostgreSQL backend)";

pub unsafe fn pgsql_version(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    // PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
    return PointerGetDatum(cstring_to_text(PG_VERSION_STR.as_ptr()) as *const c_void);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::DatumGetPointer;
    use crate::utils::adt::varlena::text_to_cstring;

    #[test]
    fn version_starts_with_postgresql() {
        unsafe {
            // pgsql_version ignores fcinfo, so call it directly with a null pointer.
            let d = pgsql_version(core::ptr::null_mut());
            let s = text_to_cstring(DatumGetPointer(d) as *const crate::c::text);
            let mut n = 0usize;
            while *s.add(n) != 0 {
                n += 1;
            }
            let got = core::slice::from_raw_parts(s as *const u8, n);
            assert!(got.starts_with(b"PostgreSQL 18.3"));
        }
    }
}
