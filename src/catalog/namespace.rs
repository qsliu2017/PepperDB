//! Translated from PostgreSQL src/include/catalog/namespace.h

use crate::c::SubTransactionId;
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::storage::lock::LOCKMODE;
use crate::storage::procnumber::ProcNumber;

/// One candidate function/operator found by namespace lookup. PG's
/// `_FuncCandidateList` is an intrusive singly-linked list; the API hands back a
/// `Vec<FuncCandidate>` (`FuncCandidateList`) instead.
pub struct FuncCandidate {
    pub pathpos: i32,
    /// The function or operator's OID.
    pub oid: Oid,
    /// Either pronargs or length(proallargtypes).
    pub nominalnargs: i32,
    pub nargs: i32,
    /// Number of args to become variadic array.
    pub nvargs: i32,
    /// Number of defaulted args.
    pub ndargs: i32,
    /// Args' positional indexes, if a named call.
    pub argnumbers: Option<Vec<i32>>,
    /// Arg types.
    pub args: Vec<Oid>,
}

pub type FuncCandidateList = Vec<FuncCandidate>;

/// Result of `checkTempNamespaceStatus`.
pub enum TempNamespaceStatus {
    /// Nonexistent, or non-temp namespace.
    NotTemp,
    /// Exists, belongs to no active session.
    Idle,
    /// Belongs to some active session.
    InUse,
}

/// Structure for the `xxxSearchPathMatcher` functions.
pub struct SearchPathMatcher {
    /// OIDs of explicitly named schemas.
    pub schemas: Vec<Oid>,
    /// Implicitly prepend pg_catalog?
    pub add_catalog: bool,
    /// Implicitly prepend temp schema?
    pub add_temp: bool,
    /// For quick detection of equality to active path; private to namespace.c.
    pub generation: u64,
}

/// Option flag bits for `RangeVarGetRelidExtended()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RVROption {
    /// Don't error if relation doesn't exist.
    MissingOk = 1 << 0,
    /// Error if relation cannot be locked.
    NoWait = 1 << 1,
    /// Skip if relation cannot be locked.
    SkipLocked = 1 << 2,
}

/// `void *callback_arg` folds into the captured closure environment.
pub type RangeVarGetRelidCallback<'a> = dyn FnMut(&RangeVar, Oid, Oid) + 'a;

/// `RangeVarGetRelid(relation, lockmode, missing_ok)` macro -> this fn; the
/// missing_ok bool becomes the `Option` result.
pub fn RangeVarGetRelid(relation: &RangeVar, lockmode: LOCKMODE, _missing_ok: bool) -> Option<Oid> {
    RangeVarGetRelidExtended(relation, lockmode, 0, None)
}

pub fn RangeVarGetRelidExtended(
    _relation: &RangeVar,
    _lockmode: LOCKMODE,
    _flags: u32,
    _callback: Option<&mut RangeVarGetRelidCallback>,
) -> Option<Oid> {
    unimplemented!()
}

pub fn RangeVarGetCreationNamespace(_new_relation: &RangeVar) -> Oid {
    unimplemented!()
}

/// Returns the creation namespace plus the existing relation OID (if any).
pub fn RangeVarGetAndCheckCreationNamespace(
    _relation: &RangeVar,
    _lockmode: LOCKMODE,
) -> (Oid, Option<Oid>) {
    unimplemented!()
}

pub fn RangeVarAdjustRelationPersistence(_new_relation: &mut RangeVar, _nspid: Oid) {
    unimplemented!()
}

pub fn RelnameGetRelid(_relname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn RelationIsVisible(_relid: Oid) -> bool {
    unimplemented!()
}

pub fn TypenameGetTypid(_typname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn TypenameGetTypidExtended(_typname: &str, _temp_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn TypeIsVisible(_typid: Oid) -> bool {
    unimplemented!()
}

pub fn FuncnameGetCandidates(
    _names: &[Oid],
    _nargs: i32,
    _argnames: &[Oid],
    _expand_variadic: bool,
    _expand_defaults: bool,
    _include_out_arguments: bool,
    _missing_ok: bool,
) -> FuncCandidateList {
    unimplemented!()
}

pub fn FunctionIsVisible(_funcid: Oid) -> bool {
    unimplemented!()
}

pub fn OpernameGetOprid(_names: &[Oid], _oprleft: Oid, _oprright: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn OpernameGetCandidates(
    _names: &[Oid],
    _oprkind: u8,
    _missing_schema_ok: bool,
) -> FuncCandidateList {
    unimplemented!()
}

pub fn OperatorIsVisible(_oprid: Oid) -> bool {
    unimplemented!()
}

pub fn OpclassnameGetOpcid(_amid: Oid, _opcname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn OpclassIsVisible(_opcid: Oid) -> bool {
    unimplemented!()
}

pub fn OpfamilynameGetOpfid(_amid: Oid, _opfname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn OpfamilyIsVisible(_opfid: Oid) -> bool {
    unimplemented!()
}

pub fn CollationGetCollid(_collname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn CollationIsVisible(_collid: Oid) -> bool {
    unimplemented!()
}

pub fn ConversionGetConid(_conname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn ConversionIsVisible(_conid: Oid) -> bool {
    unimplemented!()
}

pub fn get_statistics_object_oid(_names: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn StatisticsObjIsVisible(_stxid: Oid) -> bool {
    unimplemented!()
}

pub fn get_ts_parser_oid(_names: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn TSParserIsVisible(_prs_id: Oid) -> bool {
    unimplemented!()
}

pub fn get_ts_dict_oid(_names: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn TSDictionaryIsVisible(_dict_id: Oid) -> bool {
    unimplemented!()
}

pub fn get_ts_template_oid(_names: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn TSTemplateIsVisible(_tmpl_id: Oid) -> bool {
    unimplemented!()
}

pub fn get_ts_config_oid(_names: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn TSConfigIsVisible(_cfgid: Oid) -> bool {
    unimplemented!()
}

/// Returns the (optional namespace name, object name) parts.
pub fn DeconstructQualifiedName(_names: &[Oid]) -> (Option<String>, String) {
    unimplemented!()
}

pub fn LookupNamespaceNoError(_nspname: &str) -> Option<Oid> {
    unimplemented!()
}

/// `missing_ok` bool folds into the `Option` result.
pub fn LookupExplicitNamespace(_nspname: &str, _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_namespace_oid(_nspname: &str, _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn LookupCreationNamespace(_nspname: &str) -> Oid {
    unimplemented!()
}

pub fn CheckSetNamespace(_old_nsp_oid: Oid, _nsp_oid: Oid) {
    unimplemented!()
}

/// Returns the creation namespace plus the object name.
pub fn QualifiedNameGetCreationNamespace(_names: &[Oid]) -> (Oid, String) {
    unimplemented!()
}

pub fn makeRangeVarFromNameList(_names: &[Oid]) -> Box<RangeVar> {
    unimplemented!()
}

pub fn NameListToString(_names: &[Oid]) -> String {
    unimplemented!()
}

pub fn NameListToQuotedString(_names: &[Oid]) -> String {
    unimplemented!()
}

pub fn isTempNamespace(_namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn isTempToastNamespace(_namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn isTempOrTempToastNamespace(_namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn isAnyTempNamespace(_namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn isOtherTempNamespace(_namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn checkTempNamespaceStatus(_namespace_id: Oid) -> TempNamespaceStatus {
    unimplemented!()
}

pub fn GetTempNamespaceProcNumber(_namespace_id: Oid) -> ProcNumber {
    unimplemented!()
}

pub fn GetTempToastNamespace() -> Oid {
    unimplemented!()
}

/// Returns (tempNamespaceId, tempToastNamespaceId).
pub fn GetTempNamespaceState() -> (Oid, Oid) {
    unimplemented!()
}

pub fn SetTempNamespaceState(_temp_namespace_id: Oid, _temp_toast_namespace_id: Oid) {
    unimplemented!()
}

pub fn ResetTempTableNamespace() {
    unimplemented!()
}

/// The `MemoryContext context` arg is dropped under the single-process arena model.
pub fn GetSearchPathMatcher() -> Box<SearchPathMatcher> {
    unimplemented!()
}

pub fn CopySearchPathMatcher(_path: &SearchPathMatcher) -> Box<SearchPathMatcher> {
    unimplemented!()
}

pub fn SearchPathMatchesCurrentEnvironment(_path: &SearchPathMatcher) -> bool {
    unimplemented!()
}

pub fn get_collation_oid(_collname: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_conversion_oid(_conname: &[Oid], _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn FindDefaultConversionProc(_for_encoding: i32, _to_encoding: i32) -> Option<Oid> {
    unimplemented!()
}

/// Initialization & transaction cleanup code.
pub fn InitializeSearchPath() {
    unimplemented!()
}

pub fn AtEOXact_Namespace(_is_commit: bool, _parallel: bool) {
    unimplemented!()
}

pub fn AtEOSubXact_Namespace(
    _is_commit: bool,
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
) {
    unimplemented!()
}

/// search_path GUC variable.
pub static mut namespace_search_path: Option<String> = None;

pub fn fetch_search_path(_include_implicit: bool) -> Vec<Oid> {
    unimplemented!()
}

/// Fills `sarray` with up to its length of namespace OIDs; returns the count.
pub fn fetch_search_path_array(_sarray: &mut [Oid]) -> i32 {
    unimplemented!()
}
