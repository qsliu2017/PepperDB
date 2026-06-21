//! commands/defrem.h - POSTGRES define and remove utility definitions.

use std::ffi::{c_char, c_int};

use crate::access::cmptype::CompareType;
use crate::access::stratnum::StrategyNumber;
use crate::c::{int32, int64, text};
use crate::catalog::objectaccess::ObjectAddress;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{
    AlterFdwStmt, AlterForeignServerStmt, AlterFunctionStmt, AlterOpFamilyStmt, AlterOperatorStmt,
    AlterStatsStmt, AlterTSConfigurationStmt,
    AlterTSDictionaryStmt, AlterUserMappingStmt, CallStmt, CreateAmStmt, CreateCastStmt,
    CreateFdwStmt, CreateForeignServerStmt, CreateForeignTableStmt, CreateFunctionStmt,
    CreateOpClassStmt, CreateOpFamilyStmt, CreateStatsStmt, CreateTransformStmt,
    CreateUserMappingStmt, DefElem, DoStmt, DropStmt, DropUserMappingStmt, ImportForeignSchemaStmt,
    IndexStmt, ObjectType, ReindexStmt, TypeName,
};
use crate::nodes::pg_list::List;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;

// ParseState: canonical home (parser/parse_node.h) not yet ported; use stub.
// TODO: dedup when parse_node.h lands.
use crate::nodes::params::ParseState;

// oidvector lives in c.rs in this port.
use crate::c::oidvector;
// ArrayType from utils/array.h.
use crate::utils::array::ArrayType;
// TupleDesc from access/common/tupdesc.h.
use crate::access::common::tupdesc::TupleDesc;

/* commands/dropcmds.c */
pub unsafe fn RemoveObjects(stmt: *mut DropStmt) {
    unimplemented!()
}

/* commands/indexcmds.c */
pub unsafe fn DefineIndex(
    tableId: Oid,
    stmt: *mut IndexStmt,
    indexRelationId: Oid,
    parentIndexId: Oid,
    parentConstraintId: Oid,
    total_parts: c_int,
    is_alter_table: bool,
    check_rights: bool,
    check_not_in_use: bool,
    skip_build: bool,
    quiet: bool,
) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn ExecReindex(pstate: *mut ParseState, stmt: *const ReindexStmt, isTopLevel: bool) {
    unimplemented!()
}
pub unsafe fn makeObjectName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn ChooseRelationName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
    namespaceid: Oid,
    isconstraint: bool,
) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn CheckIndexCompatible(
    oldId: Oid,
    accessMethodName: *const c_char,
    attributeList: *const List,
    exclusionOpNames: *const List,
    isWithoutOverlaps: bool,
) -> bool {
    unimplemented!()
}
pub unsafe fn GetDefaultOpClass(type_id: Oid, am_id: Oid) -> Oid {
    crate::commands::indexcmds::GetDefaultOpClass_full(type_id, am_id)
}
pub unsafe fn ResolveOpClass(
    opclass: *const List,
    attrType: Oid,
    accessMethodName: *const c_char,
    accessMethodId: Oid,
) -> Oid {
    unimplemented!()
}
pub unsafe fn GetOperatorFromCompareType(
    opclass: Oid,
    rhstype: Oid,
    cmptype: CompareType,
    opid: *mut Oid,
    strat: *mut StrategyNumber,
) {
    unimplemented!()
}

/* commands/functioncmds.c */
pub unsafe fn CreateFunction(pstate: *mut ParseState, stmt: *mut CreateFunctionStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn RemoveFunctionById(funcOid: Oid) {
    unimplemented!()
}
pub unsafe fn AlterFunction(pstate: *mut ParseState, stmt: *mut AlterFunctionStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn CreateCast(stmt: *mut CreateCastStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn CreateTransform(stmt: *mut CreateTransformStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn IsThereFunctionInNamespace(
    proname: *const c_char,
    pronargs: c_int,
    proargtypes: *mut oidvector,
    nspOid: Oid,
) {
    unimplemented!()
}
pub unsafe fn ExecuteDoStmt(pstate: *mut ParseState, stmt: *mut DoStmt, atomic: bool) {
    unimplemented!()
}
pub unsafe fn ExecuteCallStmt(
    stmt: *mut CallStmt,
    params: ParamListInfo,
    atomic: bool,
    dest: *mut DestReceiver,
) {
    unimplemented!()
}
pub unsafe fn CallStmtResultDesc(stmt: *mut CallStmt) -> TupleDesc {
    unimplemented!()
}
pub unsafe fn get_transform_oid(type_id: Oid, lang_id: Oid, missing_ok: bool) -> Oid {
    unimplemented!()
}
pub unsafe fn interpret_function_parameter_list(
    pstate: *mut ParseState,
    parameters: *mut List,
    languageOid: Oid,
    objtype: ObjectType,
    parameterTypes: *mut *mut oidvector,
    parameterTypes_list: *mut *mut List,
    allParameterTypes: *mut *mut ArrayType,
    parameterModes: *mut *mut ArrayType,
    parameterNames: *mut *mut ArrayType,
    inParameterNames_list: *mut *mut List,
    parameterDefaults: *mut *mut List,
    variadicArgType: *mut Oid,
    requiredResultType: *mut Oid,
) {
    unimplemented!()
}

/* commands/operatorcmds.c */
pub unsafe fn DefineOperator(names: *mut List, parameters: *mut List) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn RemoveOperatorById(operOid: Oid) {
    unimplemented!()
}
pub unsafe fn AlterOperator(stmt: *mut AlterOperatorStmt) -> ObjectAddress {
    unimplemented!()
}

/* commands/statscmds.c */
pub unsafe fn CreateStatistics(stmt: *mut CreateStatsStmt, check_rights: bool) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterStatistics(stmt: *mut AlterStatsStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn RemoveStatisticsById(statsOid: Oid) {
    unimplemented!()
}
pub unsafe fn RemoveStatisticsDataById(statsOid: Oid, inh: bool) {
    unimplemented!()
}
pub unsafe fn StatisticsGetRelation(statId: Oid, missing_ok: bool) -> Oid {
    unimplemented!()
}

/* commands/aggregatecmds.c */
pub unsafe fn DefineAggregate(
    pstate: *mut ParseState,
    name: *mut List,
    args: *mut List,
    oldstyle: bool,
    parameters: *mut List,
    replace: bool,
) -> ObjectAddress {
    unimplemented!()
}

/* commands/opclasscmds.c */
pub unsafe fn DefineOpClass(stmt: *mut CreateOpClassStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn DefineOpFamily(stmt: *mut CreateOpFamilyStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterOpFamily(stmt: *mut AlterOpFamilyStmt) -> Oid {
    unimplemented!()
}
pub unsafe fn IsThereOpClassInNamespace(
    opcname: *const c_char,
    opcmethod: Oid,
    opcnamespace: Oid,
) {
    unimplemented!()
}
pub unsafe fn IsThereOpFamilyInNamespace(
    opfname: *const c_char,
    opfmethod: Oid,
    opfnamespace: Oid,
) {
    unimplemented!()
}
pub unsafe fn get_opclass_oid(amID: Oid, opclassname: *mut List, missing_ok: bool) -> Oid {
    unimplemented!()
}
pub unsafe fn get_opfamily_oid(amID: Oid, opfamilyname: *mut List, missing_ok: bool) -> Oid {
    unimplemented!()
}

/* commands/tsearchcmds.c */
pub unsafe fn DefineTSParser(names: *mut List, parameters: *mut List) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn DefineTSDictionary(names: *mut List, parameters: *mut List) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterTSDictionary(stmt: *mut AlterTSDictionaryStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn DefineTSTemplate(names: *mut List, parameters: *mut List) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn DefineTSConfiguration(
    names: *mut List,
    parameters: *mut List,
    copied: *mut ObjectAddress,
) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn RemoveTSConfigurationById(cfgId: Oid) {
    unimplemented!()
}
pub unsafe fn AlterTSConfiguration(stmt: *mut AlterTSConfigurationStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn serialize_deflist(deflist: *mut List) -> *mut text {
    unimplemented!()
}
pub unsafe fn deserialize_deflist(txt: Datum) -> *mut List {
    unimplemented!()
}

/* commands/foreigncmds.c */
pub unsafe fn AlterForeignServerOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterForeignServerOwner_oid(arg0: Oid, newOwnerId: Oid) {
    unimplemented!()
}
pub unsafe fn AlterForeignDataWrapperOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterForeignDataWrapperOwner_oid(fwdId: Oid, newOwnerId: Oid) {
    unimplemented!()
}
pub unsafe fn CreateForeignDataWrapper(
    pstate: *mut ParseState,
    stmt: *mut CreateFdwStmt,
) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterForeignDataWrapper(
    pstate: *mut ParseState,
    stmt: *mut AlterFdwStmt,
) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn CreateForeignServer(stmt: *mut CreateForeignServerStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterForeignServer(stmt: *mut AlterForeignServerStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn CreateUserMapping(stmt: *mut CreateUserMappingStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn AlterUserMapping(stmt: *mut AlterUserMappingStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn RemoveUserMapping(stmt: *mut DropUserMappingStmt) -> Oid {
    unimplemented!()
}
pub unsafe fn CreateForeignTable(stmt: *mut CreateForeignTableStmt, relid: Oid) {
    unimplemented!()
}
pub unsafe fn ImportForeignSchema(stmt: *mut ImportForeignSchemaStmt) {
    unimplemented!()
}
pub unsafe fn transformGenericOptions(
    catalogId: Oid,
    oldOptions: Datum,
    options: *mut List,
    fdwvalidator: Oid,
) -> Datum {
    unimplemented!()
}

/* commands/amcmds.c */
pub unsafe fn CreateAccessMethod(stmt: *mut CreateAmStmt) -> ObjectAddress {
    unimplemented!()
}
pub unsafe fn get_index_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    unimplemented!()
}
pub unsafe fn get_table_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    unimplemented!()
}
pub unsafe fn get_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    unimplemented!()
}
pub unsafe fn get_am_name(amOid: Oid) -> *mut c_char {
    unimplemented!()
}

/* support routines in commands/define.c */

pub unsafe fn defGetString(def: *mut DefElem) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn defGetNumeric(def: *mut DefElem) -> f64 {
    unimplemented!()
}
pub unsafe fn defGetBoolean(def: *mut DefElem) -> bool {
    unimplemented!()
}
pub unsafe fn defGetInt32(def: *mut DefElem) -> int32 {
    unimplemented!()
}
pub unsafe fn defGetInt64(def: *mut DefElem) -> int64 {
    unimplemented!()
}
pub unsafe fn defGetObjectId(def: *mut DefElem) -> Oid {
    unimplemented!()
}
pub unsafe fn defGetQualifiedName(def: *mut DefElem) -> *mut List {
    unimplemented!()
}
pub unsafe fn defGetTypeName(def: *mut DefElem) -> *mut TypeName {
    unimplemented!()
}
pub unsafe fn defGetTypeLength(def: *mut DefElem) -> c_int {
    unimplemented!()
}
pub unsafe fn defGetStringList(def: *mut DefElem) -> *mut List {
    unimplemented!()
}
/* pg_noreturn: errorConflictingDefElem does not return. */
pub unsafe fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState) -> ! {
    unimplemented!()
}
