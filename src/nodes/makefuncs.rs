//! Translated from PostgreSQL src/include/nodes/makefuncs.h
//! Constructors for existing node types; bodies stubbed.

use crate::access::attnum::AttrNumber;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    A_Expr, A_Expr_Kind, ColumnDef, Constraint, DefElem, DefElemAction, FuncCall, GroupingSet,
    GroupingSetKind, JsonTablePathSpec, RangeTblEntry, TypeName, VacuumRelation,
};
use crate::nodes::primnodes::{
    Alias, Const, FromExpr, FuncExpr, JsonBehavior, JsonBehaviorType, JsonEncoding, JsonFormat,
    JsonFormatType, JsonTablePath, JsonValueExpr, JsonValueType, RangeVar, RelabelType, TargetEntry,
    Var,
};
use crate::nodes::value::String_;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// Common enum args; CoercionForm/BoolExprType live in primnodes.
use crate::nodes::primnodes::{BoolExprType, CoercionForm};

// IndexInfo lives in execnodes.
use crate::nodes::execnodes::IndexInfo;

pub fn makeA_Expr(
    _kind: A_Expr_Kind,
    _name: Vec<Box<Node>>,
    _lexpr: Option<Box<Node>>,
    _rexpr: Option<Box<Node>>,
    _location: i32,
) -> A_Expr {
    unimplemented!()
}

pub fn makeSimpleA_Expr(
    _kind: A_Expr_Kind,
    _name: &str,
    _lexpr: Option<Box<Node>>,
    _rexpr: Option<Box<Node>>,
    _location: i32,
) -> A_Expr {
    unimplemented!()
}

pub fn makeVar(
    _varno: i32,
    _varattno: AttrNumber,
    _vartype: Oid,
    _vartypmod: i32,
    _varcollid: Oid,
    _varlevelsup: usize,
) -> Var {
    unimplemented!()
}

pub fn makeVarFromTargetEntry(_varno: i32, _tle: &TargetEntry) -> Var {
    unimplemented!()
}

pub fn makeWholeRowVar(
    _rte: &RangeTblEntry,
    _varno: i32,
    _varlevelsup: usize,
    _allow_scalar: bool,
) -> Var {
    unimplemented!()
}

pub fn makeTargetEntry(
    _expr: Option<Box<Node>>,
    _resno: AttrNumber,
    _resname: Option<String>,
    _resjunk: bool,
) -> TargetEntry {
    unimplemented!()
}

pub fn flatCopyTargetEntry(_src_tle: &TargetEntry) -> TargetEntry {
    unimplemented!()
}

pub fn makeFromExpr(_fromlist: Vec<Box<Node>>, _quals: Option<Box<Node>>) -> FromExpr {
    unimplemented!()
}

pub fn makeConst(
    _consttype: Oid,
    _consttypmod: i32,
    _constcollid: Oid,
    _constlen: i32,
    _constvalue: Datum,
    _constisnull: bool,
    _constbyval: bool,
) -> Const {
    unimplemented!()
}

pub fn makeNullConst(_consttype: Oid, _consttypmod: i32, _constcollid: Oid) -> Const {
    unimplemented!()
}

pub fn makeBoolConst(_value: bool, _isnull: bool) -> Box<Node> {
    unimplemented!()
}

pub fn makeBoolExpr(_boolop: BoolExprType, _args: Vec<Box<Node>>, _location: i32) -> Box<Node> {
    unimplemented!()
}

pub fn makeAlias(_aliasname: &str, _colnames: Vec<Box<Node>>) -> Alias {
    unimplemented!()
}

pub fn makeRelabelType(
    _arg: Option<Box<Node>>,
    _rtype: Oid,
    _rtypmod: i32,
    _rcollid: Oid,
    _rformat: CoercionForm,
) -> RelabelType {
    unimplemented!()
}

pub fn makeRangeVar(_schemaname: Option<String>, _relname: Option<String>, _location: i32) -> RangeVar {
    unimplemented!()
}

pub fn makeNotNullConstraint(_colname: &String_) -> Constraint {
    unimplemented!()
}

pub fn makeTypeName(_typnam: &str) -> TypeName {
    unimplemented!()
}

pub fn makeTypeNameFromNameList(_names: Vec<Box<Node>>) -> TypeName {
    unimplemented!()
}

pub fn makeTypeNameFromOid(_type_oid: Oid, _typmod: i32) -> TypeName {
    unimplemented!()
}

pub fn makeColumnDef(_colname: &str, _type_oid: Oid, _typmod: i32, _coll_oid: Oid) -> ColumnDef {
    unimplemented!()
}

pub fn makeFuncExpr(
    _funcid: Oid,
    _rettype: Oid,
    _args: Vec<Box<Node>>,
    _funccollid: Oid,
    _inputcollid: Oid,
    _fformat: CoercionForm,
) -> FuncExpr {
    unimplemented!()
}

pub fn makeFuncCall(
    _name: Vec<Box<Node>>,
    _args: Vec<Box<Node>>,
    _funcformat: CoercionForm,
    _location: i32,
) -> FuncCall {
    unimplemented!()
}

pub fn make_opclause(
    _opno: Oid,
    _opresulttype: Oid,
    _opretset: bool,
    _leftop: Option<Box<Node>>,
    _rightop: Option<Box<Node>>,
    _opcollid: Oid,
    _inputcollid: Oid,
) -> Box<Node> {
    unimplemented!()
}

pub fn make_andclause(_andclauses: Vec<Box<Node>>) -> Box<Node> {
    unimplemented!()
}

pub fn make_orclause(_orclauses: Vec<Box<Node>>) -> Box<Node> {
    unimplemented!()
}

pub fn make_notclause(_notclause: Box<Node>) -> Box<Node> {
    unimplemented!()
}

pub fn make_and_qual(_qual1: Option<Box<Node>>, _qual2: Option<Box<Node>>) -> Box<Node> {
    unimplemented!()
}

pub fn make_ands_explicit(_andclauses: Vec<Box<Node>>) -> Box<Node> {
    unimplemented!()
}

pub fn make_ands_implicit(_clause: Option<Box<Node>>) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn makeIndexInfo(
    _numattrs: i32,
    _numkeyattrs: i32,
    _amoid: Oid,
    _expressions: Vec<Box<Node>>,
    _predicates: Vec<Box<Node>>,
    _unique: bool,
    _nulls_not_distinct: bool,
    _isready: bool,
    _concurrent: bool,
    _summarizing: bool,
    _withoutoverlaps: bool,
) -> IndexInfo {
    unimplemented!()
}

pub fn makeStringConst(_str: &str, _location: i32) -> Box<Node> {
    unimplemented!()
}

pub fn makeDefElem(_name: &str, _arg: Option<Box<Node>>, _location: i32) -> DefElem {
    unimplemented!()
}

pub fn makeDefElemExtended(
    _name_space: Option<String>,
    _name: &str,
    _arg: Option<Box<Node>>,
    _defaction: DefElemAction,
    _location: i32,
) -> DefElem {
    unimplemented!()
}

pub fn makeGroupingSet(_kind: GroupingSetKind, _content: Vec<Box<Node>>, _location: i32) -> GroupingSet {
    unimplemented!()
}

pub fn makeVacuumRelation(
    _relation: Option<Box<RangeVar>>,
    _oid: Oid,
    _va_cols: Vec<Box<Node>>,
) -> VacuumRelation {
    unimplemented!()
}

pub fn makeJsonFormat(_typ: JsonFormatType, _encoding: JsonEncoding, _location: i32) -> JsonFormat {
    unimplemented!()
}

pub fn makeJsonValueExpr(
    _raw_expr: Option<Box<Node>>,
    _formatted_expr: Option<Box<Node>>,
    _format: Option<Box<JsonFormat>>,
) -> JsonValueExpr {
    unimplemented!()
}

pub fn makeJsonKeyValue(_key: Option<Box<Node>>, _value: Option<Box<Node>>) -> Box<Node> {
    unimplemented!()
}

pub fn makeJsonIsPredicate(
    _expr: Option<Box<Node>>,
    _format: Option<Box<JsonFormat>>,
    _item_type: JsonValueType,
    _unique_keys: bool,
    _location: i32,
) -> Box<Node> {
    unimplemented!()
}

pub fn makeJsonBehavior(
    _btype: JsonBehaviorType,
    _expr: Option<Box<Node>>,
    _location: i32,
) -> JsonBehavior {
    unimplemented!()
}

pub fn makeJsonTablePath(_pathvalue: &Const, _pathname: Option<String>) -> JsonTablePath {
    unimplemented!()
}

pub fn makeJsonTablePathSpec(
    _string: Option<String>,
    _name: Option<String>,
    _string_location: i32,
    _name_location: i32,
) -> JsonTablePathSpec {
    unimplemented!()
}
