//! utils/fmgrtab.h - The function manager's table of internal functions.

use std::ffi::c_char;
use std::ffi::c_int;

use crate::c::uint16;
use crate::c::PG_UINT16_MAX;
use crate::postgres_ext::Oid;
use crate::utils::fmgr::PGFunction;

/*
 * This table stores info about all the built-in functions (ie, functions
 * that are compiled into the Postgres executable).
 */
#[repr(C)]
pub struct FmgrBuiltin {
    pub foid: Oid,             /* OID of the function */
    pub nargs: i16,            /* 0..FUNC_MAX_ARGS, or -1 if variable count */
    pub strict: bool,          /* T if function is "strict" */
    pub retset: bool,          /* T if function returns a set */
    pub funcName: *const c_char, /* C name of the function */
    pub func: PGFunction,      /* pointer to compiled function */
}

// FmgrBuiltin embeds a PGFunction fn-pointer; harmless here (placeholder until
// the generated fmgrtab.c lands as real Rust statics).
#[allow(improper_ctypes)]
extern "C" {
    pub static fmgr_builtins: [FmgrBuiltin; 0];

    /* number of entries in table */
    pub static fmgr_nbuiltins: c_int;

    /* highest function OID in table */
    pub static fmgr_last_builtin_oid: Oid;
}

/*
 * Mapping from a builtin function's OID to its index in the fmgr_builtins
 * array.  This is indexed from 0 through fmgr_last_builtin_oid.
 */
pub const InvalidOidBuiltinMapping: uint16 = PG_UINT16_MAX;

extern "C" {
    pub static fmgr_builtin_oid_index: [uint16; 0];
}
