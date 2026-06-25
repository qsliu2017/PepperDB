//! Translated from PostgreSQL src/include/utils/fmgrtab.h
//!
//! The builtin-function table (`fmgrtab.c` data). In C it is emitted by
//! `Gen_fmgrtab.pl` from `pg_proc.dat`; here `build.rs` reads the same `.dat` and
//! emits the table into OUT_DIR. `func` (the compiled C entry point) has no Rust
//! address yet, so it is `Option<PGFunction>` and is `None` for every row until
//! the builtins are implemented.

use crate::fmgr::PGFunction;
use crate::postgres_ext::Oid;

/// Info about a built-in (compiled-in) function. In-memory table.
pub struct FmgrBuiltin {
    pub foid: Oid,
    pub nargs: i16,   // 0..FUNC_MAX_ARGS, or -1 if variable count
    pub strict: bool, // T if function is "strict"
    pub retset: bool, // T if function returns a set
    pub func_name: &'static str,
    pub func: Option<PGFunction>, // compiled entry point; None until implemented
}

/// PG_UINT16_MAX sentinel for "no such builtin OID".
pub const InvalidOidBuiltinMapping: u16 = u16::MAX;

include!(concat!(env!("OUT_DIR"), "/fmgrtab_generated.rs"));
