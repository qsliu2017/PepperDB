//! Translated from PostgreSQL src/include/utils/fmgrtab.h

use crate::fmgr::PGFunction;
use crate::postgres_ext::Oid;

/// Info about a built-in (compiled-in) function. On-disk-irrelevant; in-memory table.
pub struct FmgrBuiltin {
    pub foid: Oid,
    pub nargs: i16,   // 0..FUNC_MAX_ARGS, or -1 if variable count
    pub strict: bool, // T if function is "strict"
    pub retset: bool, // T if function returns a set
    pub func_name: &'static str,
    pub func: PGFunction,
}

/// PG_UINT16_MAX sentinel for "no such builtin OID".
pub const InvalidOidBuiltinMapping: u16 = u16::MAX;

// TODO(generated): fmgr_builtins[], fmgr_nbuiltins, fmgr_last_builtin_oid, and
// fmgr_builtin_oid_index[] are emitted from pg_proc.dat (build.rs). Stubbed.
pub static fmgr_builtins: &[FmgrBuiltin] = &[];
pub const fmgr_nbuiltins: usize = 0;
pub const fmgr_last_builtin_oid: Oid = Oid(0);
pub static fmgr_builtin_oid_index: &[u16] = &[];
