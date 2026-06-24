//! Translated from PostgreSQL src/include/bootstrap/bootstrap.h

use crate::nodes::execnodes::IndexInfo;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

/// Max number of attributes in a relation supported at bootstrap time.
pub const MAXATTR: usize = 40;

pub const BOOTCOL_NULL_AUTO: i32 = 1;
pub const BOOTCOL_NULL_FORCE_NULL: i32 = 2;
pub const BOOTCOL_NULL_FORCE_NOT_NULL: i32 = 3;

// Process-global bootstrap state. These are process-globals in PG; under the
// single-process async model they migrate to a threaded Session/context later.
pub static mut boot_reldesc: Relation = core::ptr::null_mut();
pub static mut numattr: i32 = 0;
// Form_pg_attribute attrtypes[MAXATTR] -- pointer array; TODO(ptr).

/// pg_noreturn: BootstrapModeMain never returns.
pub fn BootstrapModeMain(argv: &[String], check_only: bool) -> ! {
    let _ = (argv, check_only);
    unimplemented!()
}

pub fn closerel(relname: &str) {
    let _ = relname;
    unimplemented!()
}

pub fn boot_openrel(relname: &str) {
    let _ = relname;
    unimplemented!()
}

pub fn DefineAttr(name: &str, type_: &str, attnum: i32, nullness: i32) {
    let _ = (name, type_, attnum, nullness);
    unimplemented!()
}

pub fn InsertOneTuple() {
    unimplemented!()
}

pub fn InsertOneValue(value: &str, i: i32) {
    let _ = (value, i);
    unimplemented!()
}

pub fn InsertOneNull(i: i32) {
    let _ = i;
    unimplemented!()
}

pub fn index_register(heap: Oid, ind: Oid, index_info: &IndexInfo) {
    let _ = (heap, ind, index_info);
    unimplemented!()
}

pub fn build_indices() {
    unimplemented!()
}

/// boot_get_type_io_data: 8 out-params folded into a named struct.
pub struct BootTypeIoData {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typdelim: u8,
    pub typioparam: Oid,
    pub typinput: Oid,
    pub typoutput: Oid,
}

pub fn boot_get_type_io_data(typid: Oid) -> BootTypeIoData {
    let _ = typid;
    unimplemented!()
}

// Bootstrap bki scanner/parser (boot_yyparse/boot_yylex...): the flex/bison
// grammar is regenerated from a Rust parser later; stub the entry points. The
// opaque `yyscan_t` (void *) and YYSTYPE union are scanner-internal.
pub fn boot_yyparse() -> i32 {
    unimplemented!()
}
