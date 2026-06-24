//! Translated from PostgreSQL src/include/access/reloptions.h
//! Core support for relation and tablespace options (pg_class.reloptions and
//! pg_tablespace.spcoptions).
//!
//! `relopt_type` is a true ordinal enum. `relopt_kind` is OR-combined into the
//! `bits32 kinds` field of `relopt_gen`, so it ports to `bitflags` (the LOCAL=0
//! base and the MAX sentinel become `empty()` / a `const`). The C `union values`
//! in `relopt_value` becomes a Rust enum. Validation/fill function pointers
//! become closure type aliases (function-mapping.md 6.3). The many `add_*` /
//! `*_reloptions` functions are stubs.

use bitflags::bitflags;

use crate::access::htup::HeapTuple;
use crate::access::tupdesc::TupleDesc;
use crate::c::{bits32, varlena};
use crate::postgres::Datum;

// LOCKMODE is `typedef int LOCKMODE` (storage/lock.h); not yet translated.
// TODO(struct-forward): repoint to crate::storage::lock::LOCKMODE in Phase 2.
pub type LOCKMODE = i32;

// `amoptions_function` is a typedef in access/amapi.h that was not emitted there
// (amapi.rs mapped the callback to a trait method). reloptions.h still references
// it for index reloptions parsing; model it as a closure type here.
// TODO(struct-forward): repoint to crate::access::amapi in Phase 2.
pub type AmoptionsFunction<'a> = dyn Fn(Datum, bool) -> *mut varlena + 'a;

/// Types supported by reloptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum relopt_type {
    RELOPT_TYPE_BOOL,
    RELOPT_TYPE_INT,
    RELOPT_TYPE_REAL,
    RELOPT_TYPE_ENUM,
    RELOPT_TYPE_STRING,
}

bitflags! {
    /// Kinds supported by reloptions. OR-combined into `relopt_gen.kinds`
    /// (a `bits32`), so a flag set: LOCAL is `empty()`. `MAX` (1<<30) is a
    /// non-default sentinel kept as an associated const.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct relopt_kind: bits32 {
        const HEAP        = 1 << 0;
        const TOAST       = 1 << 1;
        const BTREE       = 1 << 2;
        const HASH        = 1 << 3;
        const GIN         = 1 << 4;
        const GIST        = 1 << 5;
        const ATTRIBUTE   = 1 << 6;
        const TABLESPACE  = 1 << 7;
        const SPGIST      = 1 << 8;
        const VIEW        = 1 << 9;
        const BRIN        = 1 << 10;
        const PARTITIONED = 1 << 11;
    }
}

impl relopt_kind {
    /// RELOPT_KIND_LOCAL = 0
    pub const LOCAL: Self = Self::empty();
    /// RELOPT_KIND_LAST_DEFAULT = RELOPT_KIND_PARTITIONED
    pub const LAST_DEFAULT: Self = Self::PARTITIONED;
    /// RELOPT_KIND_MAX = (1 << 30)
    pub const MAX: bits32 = 1 << 30;
}

/// Reloption namespaces allowed for heaps -- currently only TOAST.
pub const HEAP_RELOPT_NAMESPACES: [Option<&str>; 2] = [Some("toast"), None];

/// Generic struct to hold shared reloption data.
pub struct relopt_gen {
    pub name: String,    // must be first in C (list-termination marker)
    pub desc: String,
    pub kinds: relopt_kind,
    pub lockmode: LOCKMODE,
    pub namelen: i32,
    pub r#type: relopt_type,
}

/// A parsed reloption value (the C `union values` -> a Rust enum).
pub enum RelOptValue {
    Bool(bool),
    Int(i32),
    Real(f64),
    Enum(i32),
    String(String),
}

/// Holds a parsed value.
pub struct relopt_value {
    pub r#gen: *mut relopt_gen, // TODO(ptr)
    pub isset: bool,
    pub values: RelOptValue,
}

/// reloption record for bool-typed options.
pub struct relopt_bool {
    pub r#gen: relopt_gen,
    pub default_val: bool,
}

/// reloption record for int-typed options.
pub struct relopt_int {
    pub r#gen: relopt_gen,
    pub default_val: i32,
    pub min: i32,
    pub max: i32,
}

/// reloption record for real-typed options.
pub struct relopt_real {
    pub r#gen: relopt_gen,
    pub default_val: f64,
    pub min: f64,
    pub max: f64,
}

/// One member of the array of acceptable values of an enum reloption.
pub struct relopt_enum_elt_def {
    pub string_val: String,
    pub symbol_val: i32,
}

/// reloption record for enum-typed options.
pub struct relopt_enum {
    pub r#gen: relopt_gen,
    pub members: Vec<relopt_enum_elt_def>,
    pub default_val: i32,
    pub detailmsg: String,
}

/// Validation routine for a string reloption value.
pub type validate_string_relopt<'a> = dyn Fn(&str) + 'a;
/// Fill routine for a string reloption; returns the size written to `ptr`.
pub type fill_string_relopt<'a> = dyn Fn(&str, *mut core::ffi::c_void) -> usize + 'a;
/// Validation routine for a whole parsed option set.
pub type relopts_validator<'a> = dyn Fn(*mut core::ffi::c_void, &mut [relopt_value]) + 'a;

/// reloption record for string-typed options.
pub struct relopt_string {
    pub r#gen: relopt_gen,
    pub default_len: i32,
    pub default_isnull: bool,
    pub validate_cb: Option<Box<validate_string_relopt<'static>>>,
    pub fill_cb: Option<Box<fill_string_relopt<'static>>>,
    pub default_val: Option<String>,
}

/// Table datatype for build_reloptions(): maps an option name to a struct field.
pub struct relopt_parse_elt {
    pub optname: String, // option's name
    pub opttype: relopt_type, // option's datatype
    pub offset: i32,     // offset of field in result struct
    /// Optional offset of a field recording whether the option was explicitly
    /// set (only used if > 0; see C comment).
    pub isset_offset: i32,
}

/// Local reloption definition.
pub struct local_relopt {
    pub option: *mut relopt_gen, // option definition // TODO(ptr)
    pub offset: i32,             // offset of parsed value in bytea structure
}

/// Holds local reloption data for build_local_reloptions(). The two C `List *`
/// fields become `Vec`s.
pub struct local_relopts {
    pub options: Vec<local_relopt>,
    pub validators: Vec<Box<relopts_validator<'static>>>,
    pub relopt_struct_size: usize, // size of parsed bytea structure
}

/// Get a pointer to a string reloption's value once parsed. The C macro indexes
/// into the StdRdOptions byte buffer at `member`'s recorded offset (0 -> NULL).
/// Modeled as a slice accessor; offset 0 means "unset".
pub fn get_string_reloption(optstruct: &[u8], member: usize) -> Option<&[u8]> {
    if member == 0 {
        None
    } else {
        Some(&optstruct[member..])
    }
}

// --- Functions in access/common/reloptions.c (stubs) ---

/// Allocate a new custom reloption kind bit.
pub fn add_reloption_kind() -> relopt_kind {
    unimplemented!()
}

pub fn add_bool_reloption(
    _kinds: bits32,
    _name: &str,
    _desc: &str,
    _default_val: bool,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn add_int_reloption(
    _kinds: bits32,
    _name: &str,
    _desc: &str,
    _default_val: i32,
    _min_val: i32,
    _max_val: i32,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn add_real_reloption(
    _kinds: bits32,
    _name: &str,
    _desc: &str,
    _default_val: f64,
    _min_val: f64,
    _max_val: f64,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn add_enum_reloption(
    _kinds: bits32,
    _name: &str,
    _desc: &str,
    _members: &[relopt_enum_elt_def],
    _default_val: i32,
    _detailmsg: &str,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn add_string_reloption(
    _kinds: bits32,
    _name: &str,
    _desc: &str,
    _default_val: Option<&str>,
    _validator: Option<&validate_string_relopt>,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn init_local_reloptions(_relopts: &mut local_relopts, _relopt_struct_size: usize) {
    unimplemented!()
}

pub fn register_reloptions_validator(
    _relopts: &mut local_relopts,
    _validator: Box<relopts_validator<'static>>,
) {
    unimplemented!()
}

pub fn add_local_bool_reloption(
    _relopts: &mut local_relopts,
    _name: &str,
    _desc: &str,
    _default_val: bool,
    _offset: i32,
) {
    unimplemented!()
}

pub fn add_local_int_reloption(
    _relopts: &mut local_relopts,
    _name: &str,
    _desc: &str,
    _default_val: i32,
    _min_val: i32,
    _max_val: i32,
    _offset: i32,
) {
    unimplemented!()
}

pub fn add_local_real_reloption(
    _relopts: &mut local_relopts,
    _name: &str,
    _desc: &str,
    _default_val: f64,
    _min_val: f64,
    _max_val: f64,
    _offset: i32,
) {
    unimplemented!()
}

pub fn add_local_enum_reloption(
    _relopts: &mut local_relopts,
    _name: &str,
    _desc: &str,
    _members: &[relopt_enum_elt_def],
    _default_val: i32,
    _detailmsg: &str,
    _offset: i32,
) {
    unimplemented!()
}

pub fn add_local_string_reloption(
    _relopts: &mut local_relopts,
    _name: &str,
    _desc: &str,
    _default_val: Option<&str>,
    _validator: Option<&validate_string_relopt>,
    _filler: Option<&fill_string_relopt>,
    _offset: i32,
) {
    unimplemented!()
}

/// transformRelOptions: merge `def_list` into `old_options`.
pub fn transformRelOptions(
    _old_options: Datum,
    _def_list: &[*mut core::ffi::c_void],
    _namspace: &str,
    _valid_nsps: &[&str],
    _accept_oids_off: bool,
    _is_reset: bool,
) -> Datum {
    unimplemented!()
}

/// untransformRelOptions: decode an options Datum into a list of definitions.
pub fn untransformRelOptions(_options: Datum) -> Vec<*mut core::ffi::c_void> {
    unimplemented!()
}

/// extractRelOptions: pull reloptions out of a pg_class/pg_attribute tuple.
/// None when the tuple has no reloptions.
pub fn extractRelOptions(
    _tuple: HeapTuple,
    _tupdesc: TupleDesc,
    _amoptions: Option<&AmoptionsFunction>,
) -> Option<*mut varlena> {
    unimplemented!()
}

/// build_reloptions: parse and validate reloptions into an AM-specific struct.
pub fn build_reloptions(
    _reloptions: Datum,
    _validate: bool,
    _kind: relopt_kind,
    _relopt_struct_size: usize,
    _relopt_elems: &[relopt_parse_elt],
) -> *mut core::ffi::c_void {
    unimplemented!()
}

pub fn build_local_reloptions(
    _relopts: &local_relopts,
    _options: Datum,
    _validate: bool,
) -> *mut core::ffi::c_void {
    unimplemented!()
}

pub fn default_reloptions(
    _reloptions: Datum,
    _validate: bool,
    _kind: relopt_kind,
) -> Option<*mut varlena> {
    unimplemented!()
}

pub fn heap_reloptions(_relkind: u8, _reloptions: Datum, _validate: bool) -> Option<*mut varlena> {
    unimplemented!()
}

pub fn view_reloptions(_reloptions: Datum, _validate: bool) -> Option<*mut varlena> {
    unimplemented!()
}

pub fn partitioned_table_reloptions(
    _reloptions: Datum,
    _validate: bool,
) -> Option<*mut varlena> {
    unimplemented!()
}

pub fn index_reloptions(
    _amoptions: Option<&AmoptionsFunction>,
    _reloptions: Datum,
    _validate: bool,
) -> Option<*mut varlena> {
    unimplemented!()
}

pub fn attribute_reloptions(_reloptions: Datum, _validate: bool) -> Option<*mut varlena> {
    unimplemented!()
}

pub fn tablespace_reloptions(_reloptions: Datum, _validate: bool) -> Option<*mut varlena> {
    unimplemented!()
}

/// AlterTableGetRelOptionsLockLevel: minimum lock level for an ALTER TABLE SET.
pub fn AlterTableGetRelOptionsLockLevel(_def_list: &[*mut core::ffi::c_void]) -> LOCKMODE {
    unimplemented!()
}
