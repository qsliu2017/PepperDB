//! Translated from PostgreSQL src/include/commands/defrem.h

#![allow(
    clippy::boxed_local,
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::cmptype::CompareType;
use crate::access::stratnum::StrategyNumber;
use crate::access::tupdesc::TupleDesc;
use crate::c::{oidvector, text};
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{
    AlterFdwStmt, AlterFunctionStmt, AlterForeignServerStmt, AlterOpFamilyStmt, AlterOperatorStmt,
    AlterStatsStmt, AlterTSConfigurationStmt, AlterTSDictionaryStmt, AlterUserMappingStmt,
    CallStmt, CreateAmStmt, CreateCastStmt, CreateFdwStmt, CreateForeignServerStmt,
    CreateForeignTableStmt, CreateFunctionStmt, CreateOpClassStmt, CreateOpFamilyStmt,
    CreateStatsStmt, CreateTransformStmt, CreateUserMappingStmt, DefElem, DoStmt, DropStmt,
    DropUserMappingStmt, ImportForeignSchemaStmt, IndexStmt, ObjectType, ReindexStmt, TypeName,
};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;
use crate::utils::array::ArrayType;

/* commands/dropcmds.c */
pub fn RemoveObjects(_stmt: &DropStmt) {
    unimplemented!()
}

/* commands/indexcmds.c */
pub fn DefineIndex(
    _tableId: Oid,
    _stmt: &IndexStmt,
    _indexRelationId: Oid,
    _parentIndexId: Oid,
    _parentConstraintId: Oid,
    _total_parts: i32,
    _is_alter_table: bool,
    _check_rights: bool,
    _check_not_in_use: bool,
    _skip_build: bool,
    _quiet: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn ExecReindex(_pstate: &mut ParseState, _stmt: &ReindexStmt, _isTopLevel: bool) {
    unimplemented!()
}

pub fn makeObjectName(_name1: &str, _name2: &str, _label: &str) -> String {
    unimplemented!()
}

pub fn ChooseRelationName(
    _name1: &str,
    _name2: &str,
    _label: &str,
    _namespaceid: Oid,
    _isconstraint: bool,
) -> String {
    unimplemented!()
}

pub fn CheckIndexCompatible(
    _oldId: Oid,
    _accessMethodName: &str,
    _attributeList: &[Node],
    _exclusionOpNames: &[Node],
    _isWithoutOverlaps: bool,
) -> bool {
    unimplemented!()
}

pub fn GetDefaultOpClass(_type_id: Oid, _am_id: Oid) -> Oid {
    unimplemented!()
}

pub fn ResolveOpClass(
    _opclass: &[Node],
    _attrType: Oid,
    _accessMethodName: &str,
    _accessMethodId: Oid,
) -> Oid {
    unimplemented!()
}

// two out-params (opid, strat) folded into the returned tuple.
pub fn GetOperatorFromCompareType(
    _opclass: Oid,
    _rhstype: Oid,
    _cmptype: CompareType,
) -> (Oid, StrategyNumber) {
    unimplemented!()
}

/* commands/functioncmds.c */
pub fn CreateFunction(_pstate: &mut ParseState, _stmt: &CreateFunctionStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RemoveFunctionById(_funcOid: Oid) {
    unimplemented!()
}

pub fn AlterFunction(_pstate: &mut ParseState, _stmt: &AlterFunctionStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn CreateCast(_stmt: &CreateCastStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn CreateTransform(_stmt: &CreateTransformStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn IsThereFunctionInNamespace(
    _proname: &str,
    _pronargs: i32,
    _proargtypes: &oidvector,
    _nspOid: Oid,
) {
    unimplemented!()
}

pub fn ExecuteDoStmt(_pstate: &mut ParseState, _stmt: &DoStmt, _atomic: bool) {
    unimplemented!()
}

pub fn ExecuteCallStmt(
    _stmt: &CallStmt,
    _params: ParamListInfo,
    _atomic: bool,
    _dest: &mut dyn DestReceiver,
) {
    unimplemented!()
}

pub fn CallStmtResultDesc(_stmt: &CallStmt) -> TupleDesc {
    unimplemented!()
}

// missing_ok -> Option (InvalidOid sentinel collapses to None).
pub fn get_transform_oid(_type_id: Oid, _lang_id: Oid) -> Option<Oid> {
    unimplemented!()
}

/// 11 out-params of interpret_function_parameter_list, gathered into a struct.
pub struct InterpretedFunctionParameters {
    pub parameterTypes: oidvector,
    pub parameterTypes_list: Vec<Node>,
    pub allParameterTypes: ArrayType,
    pub parameterModes: ArrayType,
    pub parameterNames: ArrayType,
    pub inParameterNames_list: Vec<Node>,
    pub parameterDefaults: Vec<Node>,
    pub variadicArgType: Oid,
    pub requiredResultType: Oid,
}

pub fn interpret_function_parameter_list(
    _pstate: &mut ParseState,
    _parameters: &[Node],
    _languageOid: Oid,
    _objtype: ObjectType,
) -> InterpretedFunctionParameters {
    unimplemented!()
}

/* commands/operatorcmds.c */
pub fn DefineOperator(_names: Vec<Node>, _parameters: Vec<Node>) -> ObjectAddress {
    unimplemented!()
}

pub fn RemoveOperatorById(_operOid: Oid) {
    unimplemented!()
}

pub fn AlterOperator(_stmt: &AlterOperatorStmt) -> ObjectAddress {
    unimplemented!()
}

/* commands/statscmds.c */
pub fn CreateStatistics(_stmt: &CreateStatsStmt, _check_rights: bool) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterStatistics(_stmt: &AlterStatsStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RemoveStatisticsById(_statsOid: Oid) {
    unimplemented!()
}

pub fn RemoveStatisticsDataById(_statsOid: Oid, _inh: bool) {
    unimplemented!()
}

// missing_ok -> Option (InvalidOid sentinel collapses to None).
pub fn StatisticsGetRelation(_statId: Oid) -> Option<Oid> {
    unimplemented!()
}

/* commands/aggregatecmds.c */
pub fn DefineAggregate(
    _pstate: &mut ParseState,
    _name: Vec<Node>,
    _args: Vec<Node>,
    _oldstyle: bool,
    _parameters: Vec<Node>,
    _replace: bool,
) -> ObjectAddress {
    unimplemented!()
}

/* commands/opclasscmds.c */
pub fn DefineOpClass(_stmt: &CreateOpClassStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn DefineOpFamily(_stmt: &CreateOpFamilyStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterOpFamily(_stmt: &AlterOpFamilyStmt) -> Oid {
    unimplemented!()
}

pub fn IsThereOpClassInNamespace(_opcname: &str, _opcmethod: Oid, _opcnamespace: Oid) {
    unimplemented!()
}

pub fn IsThereOpFamilyInNamespace(_opfname: &str, _opfmethod: Oid, _opfnamespace: Oid) {
    unimplemented!()
}

// missing_ok -> Option (InvalidOid sentinel collapses to None).
pub fn get_opclass_oid(_amID: Oid, _opclassname: &[Node]) -> Option<Oid> {
    unimplemented!()
}

pub fn get_opfamily_oid(_amID: Oid, _opfamilyname: &[Node]) -> Option<Oid> {
    unimplemented!()
}

/* commands/tsearchcmds.c */
pub fn DefineTSParser(_names: Vec<Node>, _parameters: Vec<Node>) -> ObjectAddress {
    unimplemented!()
}

pub fn DefineTSDictionary(_names: Vec<Node>, _parameters: Vec<Node>) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterTSDictionary(_stmt: &AlterTSDictionaryStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn DefineTSTemplate(_names: Vec<Node>, _parameters: Vec<Node>) -> ObjectAddress {
    unimplemented!()
}

// out-param copied folded into the returned tuple.
pub fn DefineTSConfiguration(
    _names: Vec<Node>,
    _parameters: Vec<Node>,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

pub fn RemoveTSConfigurationById(_cfgId: Oid) {
    unimplemented!()
}

pub fn AlterTSConfiguration(_stmt: &AlterTSConfigurationStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn serialize_deflist(_deflist: &[Node]) -> text {
    unimplemented!()
}

pub fn deserialize_deflist(_txt: Datum) -> Vec<Node> {
    unimplemented!()
}

/* commands/foreigncmds.c */
pub fn AlterForeignServerOwner(_name: &str, _newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterForeignServerOwner_oid(_arg0: Oid, _newOwnerId: Oid) {
    unimplemented!()
}

pub fn AlterForeignDataWrapperOwner(_name: &str, _newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterForeignDataWrapperOwner_oid(_fwdId: Oid, _newOwnerId: Oid) {
    unimplemented!()
}

pub fn CreateForeignDataWrapper(_pstate: &mut ParseState, _stmt: &CreateFdwStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterForeignDataWrapper(_pstate: &mut ParseState, _stmt: &AlterFdwStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn CreateForeignServer(_stmt: &CreateForeignServerStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterForeignServer(_stmt: &AlterForeignServerStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn CreateUserMapping(_stmt: &CreateUserMappingStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterUserMapping(_stmt: &AlterUserMappingStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RemoveUserMapping(_stmt: &DropUserMappingStmt) -> Oid {
    unimplemented!()
}

pub fn CreateForeignTable(_stmt: &CreateForeignTableStmt, _relid: Oid) {
    unimplemented!()
}

pub fn ImportForeignSchema(_stmt: &ImportForeignSchemaStmt) {
    unimplemented!()
}

pub fn transformGenericOptions(
    _catalogId: Oid,
    _oldOptions: Datum,
    _options: &[Node],
    _fdwvalidator: Oid,
) -> Datum {
    unimplemented!()
}

/* commands/amcmds.c */
pub fn CreateAccessMethod(_stmt: &CreateAmStmt) -> ObjectAddress {
    unimplemented!()
}

// missing_ok -> Option (InvalidOid sentinel collapses to None).
pub fn get_index_am_oid(_amname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn get_table_am_oid(_amname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn get_am_oid(_amname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn get_am_name(_amOid: Oid) -> String {
    unimplemented!()
}

/* support routines in commands/define.c */

pub use crate::backend::commands::define::{
    defGetBoolean, defGetInt32, defGetInt64, defGetNumeric, defGetObjectId, defGetQualifiedName,
    defGetString, defGetStringList, defGetTypeLength, defGetTypeName,
};

// pg_noreturn -> diverging fn (-> !).
pub fn errorConflictingDefElem(_defel: &DefElem, _pstate: &mut ParseState) -> ! {
    unimplemented!()
}
