//! Translated from PostgreSQL src/include/utils/lsyscache.h
#![allow(clippy::needless_pass_by_value, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use bitflags::bitflags;

use crate::access::attnum::AttrNumber;
use crate::access::cmptype::CompareType;
use crate::access::htup::HeapTuple;
use crate::c::{text, RegProcedure};
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// avoid including subscripting.h here
pub struct SubscriptRoutines {
    _private: (),
}

/// Result list element for get_op_index_interpretation.
pub struct OpIndexInterpretation {
    pub opfamily_id: Oid,
    pub cmptype: CompareType,
    pub oplefttype: Oid,
    pub oprighttype: Oid,
}

/// I/O function selector for get_type_io_data.
pub enum IOFuncSelector {
    Input,
    Output,
    Receive,
    Send,
}

bitflags! {
    /// Flag bits for get_attstatsslot.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AttStatsSlotFlags: i32 {
        const VALUES  = 0x01;
        const NUMBERS = 0x02;
    }
}

/// Result struct for get_attstatsslot.
pub struct AttStatsSlot {
    /// Always filled: actual staop for the found slot.
    pub staop: Oid,
    /// Always filled: actual collation for the found slot.
    pub stacoll: Oid,
    /// Filled if VALUES: actual datatype of the values.
    pub valuetype: Oid,
    /// Slot's "values" array (None if not requested/absent).
    pub values: Option<Vec<Datum>>,
    /// Slot's "numbers" array (None if not requested/absent).
    pub numbers: Option<Vec<f32>>,
    // C's nvalues/nnumbers and the private values_arr/numbers_arr backing
    // pointers collapse into the Vec lengths and ownership.
}

/// 6-output folding of get_type_io_data (function-mapping 5.3).
pub struct TypeIoData {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typdelim: u8,
    pub typioparam: Oid,
    pub func: Oid,
}

// get_attavgwidth_hook: plugin hook (Oid relid, AttrNumber attnum) -> int32.
// Modeled as an optional callback slot; threaded state via closure.
// pub static mut get_attavgwidth_hook: Option<...> -- omitted in skeleton.

pub fn op_in_opfamily(opno: Oid, opfamily: Oid) -> bool {
    unimplemented!()
}

pub fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> i32 {
    unimplemented!()
}

/// InvalidOid sentinel -> Option.
pub fn get_op_opfamily_sortfamily(opno: Oid, opfamily: Oid) -> Option<Oid> {
    unimplemented!()
}

/// C out-params `(int *strategy, Oid *lefttype, Oid *righttype)` -> tuple.
pub fn get_op_opfamily_properties(
    opno: Oid,
    opfamily: Oid,
    ordering_op: bool,
) -> (i32, Oid, Oid) {
    unimplemented!()
}

pub fn get_opfamily_member(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> Option<Oid> {
    unimplemented!()
}

pub fn get_opfamily_member_for_cmptype(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    cmptype: CompareType,
) -> Option<Oid> {
    unimplemented!()
}

/// Returns false when not found; on success yields (opfamily, opcintype, cmptype).
pub fn get_ordering_op_properties(opno: Oid) -> Option<(Oid, Oid, CompareType)> {
    unimplemented!()
}

/// C `bool *reverse` out-param + InvalidOid sentinel -> Option<(Oid, bool)>.
pub fn get_equality_op_for_ordering_op(opno: Oid) -> Option<(Oid, bool)> {
    unimplemented!()
}

pub fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> Option<Oid> {
    unimplemented!()
}

// Join-operator interpretation bodies live in the backend module (rules.md s3);
// re-export under the C names. M7 builtin-table form (pg_amop list-scan staged).
pub use crate::backend::utils::cache::lsyscache::{
    get_mergejoin_opfamilies, get_op_index_interpretation, op_hashjoinable, op_mergejoinable,
};

/// C out-params `(Oid *lhs_opno, Oid *rhs_opno)` + bool success.
pub fn get_compatible_hash_operators(opno: Oid) -> Option<(Oid, Oid)> {
    unimplemented!()
}

/// C out-params `(RegProcedure *lhs_procno, RegProcedure *rhs_procno)` + bool.
pub fn get_op_hash_functions(opno: Oid) -> Option<(RegProcedure, RegProcedure)> {
    unimplemented!()
}

pub fn equality_ops_are_compatible(opno1: Oid, opno2: Oid) -> bool {
    unimplemented!()
}

pub fn comparison_ops_are_compatible(opno1: Oid, opno2: Oid) -> bool {
    unimplemented!()
}

pub fn collations_agree_on_equality(coll1: Oid, coll2: Oid) -> bool {
    unimplemented!()
}

pub fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> Option<Oid> {
    unimplemented!()
}

/// missing_ok dropped: caller decides via Option (function-mapping 4).
pub fn get_attname(relid: Oid, attnum: AttrNumber, missing_ok: bool) -> Option<String> {
    unimplemented!()
}

/// InvalidAttrNumber sentinel -> Option.
pub fn get_attnum(relid: Oid, attname: &str) -> Option<AttrNumber> {
    unimplemented!()
}

pub fn get_attgenerated(relid: Oid, attnum: AttrNumber) -> u8 {
    unimplemented!()
}

pub fn get_atttype(relid: Oid, attnum: AttrNumber) -> Option<Oid> {
    unimplemented!()
}

/// C out-params `(Oid *typid, int32 *typmod, Oid *collid)` -> tuple.
pub fn get_atttypetypmodcoll(relid: Oid, attnum: AttrNumber) -> (Oid, i32, Oid) {
    unimplemented!()
}

pub fn get_attoptions(relid: Oid, attnum: i16) -> Datum {
    unimplemented!()
}

pub fn get_cast_oid(sourcetypeid: Oid, targettypeid: Oid, missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_collation_name(colloid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_collation_isdeterministic(colloid: Oid) -> bool {
    unimplemented!()
}

pub fn get_constraint_name(conoid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_constraint_index(conoid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_constraint_type(conoid: Oid) -> u8 {
    unimplemented!()
}

pub fn get_language_name(langoid: Oid, missing_ok: bool) -> Option<String> {
    unimplemented!()
}

pub fn get_opclass_family(opclass: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_opclass_input_type(opclass: Oid) -> Option<Oid> {
    unimplemented!()
}

/// C out-params `(Oid *opfamily, Oid *opcintype)` + bool success.
pub fn get_opclass_opfamily_and_input_type(opclass: Oid) -> Option<(Oid, Oid)> {
    unimplemented!()
}

pub fn get_opclass_method(opclass: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_opfamily_method(opfid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_opfamily_name(opfid: Oid, missing_ok: bool) -> Option<String> {
    unimplemented!()
}

pub fn get_opcode(opno: Oid) -> RegProcedure {
    unimplemented!()
}

pub fn get_opname(opno: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_op_rettype(opno: Oid) -> Option<Oid> {
    unimplemented!()
}

/// C out-params `(Oid *lefttype, Oid *righttype)` -> tuple. Body lives in the
/// backend module (warm OPEROID syscache + builtin fallback).
pub use crate::backend::utils::cache::lsyscache::op_input_types;

pub fn op_strict(opno: Oid) -> bool {
    unimplemented!()
}

pub fn op_volatile(opno: Oid) -> u8 {
    unimplemented!()
}

pub fn get_commutator(opno: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_negator(opno: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_oprrest(opno: Oid) -> RegProcedure {
    unimplemented!()
}

pub fn get_oprjoin(opno: Oid) -> RegProcedure {
    unimplemented!()
}

pub fn get_func_name(funcid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_func_namespace(funcid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub use crate::backend::utils::cache::lsyscache::get_func_rettype;

pub fn get_func_nargs(funcid: Oid) -> i32 {
    unimplemented!()
}

/// C `Oid get_func_signature(funcid, Oid **argtypes, int *nargs)` -- returns
/// rettype + fills argtypes/nargs out-params -> (rettype, argtypes).
pub fn get_func_signature(funcid: Oid) -> (Oid, Vec<Oid>) {
    unimplemented!()
}

pub fn get_func_variadictype(funcid: Oid) -> Oid {
    unimplemented!()
}

pub use crate::backend::utils::cache::lsyscache::get_func_retset;

pub fn func_strict(funcid: Oid) -> bool {
    unimplemented!()
}

pub fn func_volatile(funcid: Oid) -> u8 {
    unimplemented!()
}

pub fn func_parallel(funcid: Oid) -> u8 {
    unimplemented!()
}

pub fn get_func_prokind(funcid: Oid) -> u8 {
    unimplemented!()
}

pub fn get_func_leakproof(funcid: Oid) -> bool {
    unimplemented!()
}

pub fn get_func_support(funcid: Oid) -> RegProcedure {
    unimplemented!()
}

/// InvalidOid sentinel -> Option.
pub fn get_relname_relid(relname: &str, relnamespace: Oid) -> Option<Oid> {
    unimplemented!()
}

pub use crate::backend::utils::cache::lsyscache::get_rel_name;

pub use crate::backend::utils::cache::lsyscache::get_rel_namespace;

pub fn get_rel_type_id(relid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_rel_relkind(relid: Oid) -> u8 {
    unimplemented!()
}

pub fn get_rel_relispartition(relid: Oid) -> bool {
    unimplemented!()
}

pub fn get_rel_tablespace(relid: Oid) -> Oid {
    unimplemented!()
}

pub fn get_rel_persistence(relid: Oid) -> u8 {
    unimplemented!()
}

pub fn get_rel_relam(relid: Oid) -> Oid {
    unimplemented!()
}

pub fn get_transform_fromsql(typid: Oid, langid: Oid, trftypes: &[Oid]) -> Option<Oid> {
    unimplemented!()
}

pub fn get_transform_tosql(typid: Oid, langid: Oid, trftypes: &[Oid]) -> Option<Oid> {
    unimplemented!()
}

pub fn get_typisdefined(typid: Oid) -> bool {
    unimplemented!()
}

pub fn get_typlen(typid: Oid) -> i16 {
    unimplemented!()
}

pub fn get_typbyval(typid: Oid) -> bool {
    unimplemented!()
}

/// C out-params `(int16 *typlen, bool *typbyval)` -> tuple. Body in
/// crate::backend::utils::cache::lsyscache (warm-syscache hit + builtin fallback).
pub use crate::backend::utils::cache::lsyscache::get_typlenbyval;

/// C out-params `(int16 *typlen, bool *typbyval, char *typalign)` -> tuple.
pub use crate::backend::utils::cache::lsyscache::get_typlenbyvalalign;

pub fn getTypeIOParam(type_tuple: &HeapTuple) -> Oid {
    unimplemented!()
}

/// 6 mixed-type out-params -> TypeIoData struct (function-mapping 5.3).
pub fn get_type_io_data(typid: Oid, which_func: IOFuncSelector) -> TypeIoData {
    unimplemented!()
}

pub fn get_typstorage(typid: Oid) -> u8 {
    unimplemented!()
}

pub fn get_typdefault(typid: Oid) -> Option<Node> {
    unimplemented!()
}

pub fn get_typtype(typid: Oid) -> u8 {
    unimplemented!()
}

pub fn type_is_rowtype(typid: Oid) -> bool {
    unimplemented!()
}

pub fn type_is_enum(typid: Oid) -> bool {
    unimplemented!()
}

pub fn type_is_range(typid: Oid) -> bool {
    unimplemented!()
}

pub fn type_is_multirange(typid: Oid) -> bool {
    unimplemented!()
}

/// C out-params `(char *typcategory, bool *typispreferred)` -> tuple. Reads pg_type
/// via a warm TYPEOID hit (M4, step 23; used by `select_common_type`).
#[must_use]
pub fn get_type_category_preferred(typid: Oid) -> (u8, bool) {
    let (cat, pref) = crate::backend::utils::cache::lsyscache::get_type_category_preferred(typid);
    (cat as u8, pref)
}

pub fn get_typ_typrelid(typid: Oid) -> Oid {
    unimplemented!()
}

/// InvalidOid sentinel -> Option (not an array element type otherwise).
pub fn get_element_type(typid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_array_type(typid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_promoted_array_type(typid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_base_element_type(typid: Oid) -> Option<Oid> {
    unimplemented!()
}

/// C out-params `(Oid *typInput, Oid *typIOParam)` -> tuple. Reads pg_type via a
/// warm TYPEOID hit (M4, step 23; used by CoerceViaIO + the unknown-literal cast).
pub use crate::backend::utils::cache::lsyscache::get_type_input_info as getTypeInputInfo;

/// C out-params `(Oid *typOutput, bool *typIsVarlena)` -> tuple.
///
/// Step-14: the step-02 shim is GONE. This now reads pg_type through the real
/// syscache (a warm TYPEOID hit); the body is in
/// `crate::backend::utils::cache::lsyscache::get_type_output_info`. For the
/// catalog-less M1 SELECT-1 window (no pg_type heap), it falls back to the builtin
/// int2/4/8 output map there.
pub use crate::backend::utils::cache::lsyscache::get_type_output_info as getTypeOutputInfo;

/// C out-params `(Oid *typReceive, Oid *typIOParam)` -> tuple.
pub fn getTypeBinaryInputInfo(r#type: Oid) -> (Oid, Oid) {
    unimplemented!()
}

/// C out-params `(Oid *typSend, bool *typIsVarlena)` -> tuple.
pub fn getTypeBinaryOutputInfo(r#type: Oid) -> (Oid, bool) {
    unimplemented!()
}

pub fn get_typmodin(typid: Oid) -> Oid {
    unimplemented!()
}

pub fn get_typcollation(typid: Oid) -> Oid {
    unimplemented!()
}

pub fn type_is_collatable(typid: Oid) -> bool {
    unimplemented!()
}

/// C `RegProcedure get_typsubscript(typid, Oid *typelemp)` -> (regproc, typelem).
pub fn get_typsubscript(typid: Oid) -> (RegProcedure, Oid) {
    unimplemented!()
}

/// C returns routines + `Oid *typelemp` out-param -> (routines, typelem).
pub fn getSubscriptingRoutines(typid: Oid) -> (Option<&'static SubscriptRoutines>, Oid) {
    unimplemented!()
}

pub fn getBaseType(typid: Oid) -> Oid {
    getBaseTypeAndTypmod(typid, -1).0
}

/// C `Oid getBaseTypeAndTypmod(typid, int32 *typmod)` -> (basetype, typmod). M4
/// (step 23) has no domains, so the base type is the type itself and the typmod is
/// unchanged. The domain-unwrapping loop grows with the domain milestone.
#[must_use]
pub fn getBaseTypeAndTypmod(typid: Oid, typmod: i32) -> (Oid, i32) {
    (typid, typmod)
}

pub fn get_typavgwidth(typid: Oid, typmod: i32) -> i32 {
    unimplemented!()
}

pub fn get_attavgwidth(relid: Oid, attnum: AttrNumber) -> i32 {
    unimplemented!()
}

/// Returns false when no matching slot found; fills sslot on success.
pub fn get_attstatsslot(
    sslot: &mut AttStatsSlot,
    statstuple: &HeapTuple,
    reqkind: i32,
    reqop: Oid,
    flags: AttStatsSlotFlags,
) -> bool {
    unimplemented!()
}

pub fn free_attstatsslot(sslot: &mut AttStatsSlot) {
    unimplemented!()
}

pub fn get_namespace_name(nspid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_namespace_name_or_temp(nspid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_range_subtype(range_oid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_range_collation(range_oid: Oid) -> Oid {
    unimplemented!()
}

pub fn get_range_multirange(range_oid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_multirange_range(multirange_oid: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_index_column_opclass(index_oid: Oid, attno: i32) -> Option<Oid> {
    unimplemented!()
}

pub fn get_index_isreplident(index_oid: Oid) -> bool {
    unimplemented!()
}

pub fn get_index_isvalid(index_oid: Oid) -> bool {
    unimplemented!()
}

pub fn get_index_isclustered(index_oid: Oid) -> bool {
    unimplemented!()
}

pub fn get_publication_oid(pubname: &str, missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_publication_name(pubid: Oid, missing_ok: bool) -> Option<String> {
    unimplemented!()
}

pub fn get_subscription_oid(subname: &str, missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_subscription_name(subid: Oid, missing_ok: bool) -> Option<String> {
    unimplemented!()
}

/// type_is_array: has an element type.
pub fn type_is_array(typid: Oid) -> bool {
    get_element_type(typid).is_some()
}

/// type_is_array_domain: plain array or domain over array.
pub fn type_is_array_domain(typid: Oid) -> bool {
    get_base_element_type(typid).is_some()
}
