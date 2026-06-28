//! Creator functions for the most frequently created node types. Translated from
//! backend/nodes/makefuncs.c.
//!
//! These are non-type-centric free functions (PG names `makeConst`, `makeVar`,
//! `makeTargetEntry`, ...). The bodies live here as snake_case `pub fn`s with the
//! C symbol in the doc comment; the header `crate::nodes::makefuncs` re-exports
//! each under its C name so call sites keep resolving.
//!
//! PG's `List *` is tombstoned to `Vec` (see `crate::nodes::pg_list`), so the
//! `list_make1`/`NIL` idioms become `vec![..]`/`Vec::new()`. `makeNode(T)` is just
//! constructing the struct value; `pstrdup` is `String` ownership.
//!
//! A handful of constructors (`makeSimpleA_Expr`, `makeTypeName`,
//! `makeNotNullConstraint`) build a node list whose elements are value nodes
//! (`T_String`). Value nodes are not yet `Node` enum variants (see
//! `crate::nodes::value`), so those constructors stage to `unimplemented!()` until
//! the node-defining pass adds the variants; none is reachable for M1.

use crate::access::attnum::AttrNumber;
use crate::catalog::genbki::BOOLOID;
use crate::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    A_Const, A_Expr, A_Expr_Kind, ColumnDef, ConstrType, Constraint, DefElem, DefElemAction,
    FuncCall, GroupingSet, GroupingSetKind, JsonKeyValue, JsonTablePathSpec, RangeTblEntry,
    TypeName, VacuumRelation, ValUnion,
};
use crate::nodes::primnodes::{
    Alias, BoolExpr, BoolExprType, CoercionForm, Const, FromExpr, FuncExpr, JsonBehavior,
    JsonBehaviorType, JsonEncoding, JsonFormat, JsonFormatType, JsonIsPredicate, JsonTablePath,
    JsonValueExpr, JsonValueType, OpExpr, RangeVar, RelabelType, TargetEntry, Var, VarReturningType,
};
use crate::nodes::value::{makeString, String_};
use crate::postgres::{Datum, DatumGetBool};
use crate::postgres_ext::{InvalidOid, Oid};

/// PG `makeA_Expr`: makes an `A_Expr` node.
pub fn make_a_expr(
    kind: A_Expr_Kind,
    name: Vec<Node>,
    lexpr: Option<Node>,
    rexpr: Option<Node>,
    location: i32,
) -> A_Expr {
    A_Expr {
        kind,
        name,
        lexpr,
        rexpr,
        rexpr_list_start: -1,
        rexpr_list_end: -1,
        location,
    }
}

/// PG `makeSimpleA_Expr`: as `make_a_expr`, given a simple (unqualified) operator name.
pub fn make_simple_a_expr(
    _kind: A_Expr_Kind,
    _name: &str,
    _lexpr: Option<Node>,
    _rexpr: Option<Node>,
    _location: i32,
) -> A_Expr {
    // name = list_make1(makeString(name)); a T_String value node is not yet a
    // Node enum variant (crate::nodes::value). Not reachable for M1.
    unimplemented!("makeSimpleA_Expr: T_String value node not yet a Node variant")
}

/// PG `makeVar`: creates a `Var` node.
///
/// Only a few callers need a `Var` with a non-default `varreturningtype`,
/// non-null `varnullingrels`, or `varnosyn`/`varattnosyn` different from
/// `varno`/`varattno`; those are set to sensible defaults here.
#[allow(clippy::cast_sign_loss, reason = "mirrors C: varnosyn = (Index) varno")]
pub fn make_var(
    varno: i32,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: i32,
    varcollid: Oid,
    varlevelsup: crate::c::Index,
) -> Var {
    Var {
        varno,
        varattno,
        vartype,
        vartypmod,
        varcollid,
        varnullingrels: None,
        varlevelsup,
        varreturningtype: VarReturningType::DEFAULT,
        varnosyn: varno as crate::c::Index,
        varattnosyn: varattno,
        location: -1,
    }
}

/// PG `makeVarFromTargetEntry`: create a same-level `Var` from a `TargetEntry`.
pub fn make_var_from_target_entry(varno: i32, tle: &TargetEntry) -> Var {
    let expr = tle.expr.as_ref();
    make_var(
        varno,
        tle.resno,
        expr.map_or(InvalidOid, exprType),
        expr.map_or(-1, exprTypmod),
        expr.map_or(InvalidOid, exprCollation),
        0,
    )
}

/// PG `makeWholeRowVar`: creates a `Var` referencing a whole row of an RTE.
pub fn make_whole_row_var(
    _rte: &RangeTblEntry,
    _varno: i32,
    _varlevelsup: crate::c::Index,
    _allow_scalar: bool,
) -> Var {
    // Reaches get_rel_type_id / type_is_rowtype / RTE rtekind handling not
    // translated for M1.
    unimplemented!("makeWholeRowVar: RTE rowtype lookup deferred")
}

/// PG `makeTargetEntry`: creates a `TargetEntry` node.
pub fn make_target_entry(
    expr: Option<Node>,
    resno: AttrNumber,
    resname: Option<String>,
    resjunk: bool,
) -> TargetEntry {
    TargetEntry {
        expr,
        resno,
        resname,
        ressortgroupref: 0,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk,
    }
}

/// PG `flatCopyTargetEntry`: duplicate a `TargetEntry` without copying substructure.
pub fn flat_copy_target_entry(src_tle: &TargetEntry) -> TargetEntry {
    src_tle.clone()
}

/// PG `makeFromExpr`: creates a `FromExpr` node.
pub fn make_from_expr(fromlist: Vec<Node>, quals: Option<Node>) -> FromExpr {
    FromExpr { fromlist, quals }
}

/// PG `makeConst`: creates a `Const` node.
pub fn make_const(
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: i32,
    constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
) -> Const {
    // A varlena value must be forced to non-expanded (non-toasted) form so equal()
    // sees a consistent representation. Varlena consts are not reachable for M1
    // (byval int/bool only); the detoast path reaches a not-yet-translated subsystem.
    if !constisnull && constlen == -1 {
        unimplemented!("makeConst: varlena detoast deferred")
    }
    Const {
        consttype,
        consttypmod,
        constcollid,
        constlen,
        constvalue,
        constisnull,
        constbyval,
        location: -1,
    }
}

/// PG `makeNullConst`: creates a `Const` representing a typed NULL.
pub fn make_null_const(consttype: Oid, consttypmod: i32, constcollid: Oid) -> Const {
    let (typ_len, typ_byval) = crate::utils::lsyscache::get_typlenbyval(consttype);
    make_const(
        consttype,
        consttypmod,
        constcollid,
        i32::from(typ_len),
        Datum(0),
        true,
        typ_byval,
    )
}

/// PG `makeBoolConst`: creates a `Const` representing a boolean value (or NULL).
pub fn make_bool_const(value: bool, isnull: bool) -> Node {
    // pg_type.h hardwires size of bool as 1.
    Node::Const(Box::new(make_const(
        BOOLOID,
        -1,
        InvalidOid,
        1,
        Datum(usize::from(value)),
        isnull,
        true,
    )))
}

/// PG `makeBoolExpr`: creates a `BoolExpr` node.
pub fn make_bool_expr(boolop: BoolExprType, args: Vec<Node>, location: i32) -> Node {
    Node::BoolExpr(Box::new(BoolExpr {
        boolop,
        args,
        location,
    }))
}

/// PG `makeAlias`: creates an `Alias` node. The name is copied; `colnames` is not.
pub fn make_alias(aliasname: &str, colnames: Vec<crate::nodes::value::String_>) -> Alias {
    Alias {
        aliasname: Some(aliasname.to_owned()),
        colnames,
    }
}

/// PG `makeRelabelType`: creates a `RelabelType` node.
pub fn make_relabel_type(
    arg: Option<Node>,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
) -> RelabelType {
    RelabelType {
        arg,
        resulttype: rtype,
        resulttypmod: rtypmod,
        resultcollid: rcollid,
        relabelformat: rformat,
        location: -1,
    }
}

/// PG `makeRangeVar`: creates a `RangeVar` node (oversimplified case).
pub fn make_range_var(
    schemaname: Option<String>,
    relname: Option<String>,
    location: i32,
) -> RangeVar {
    RangeVar {
        catalogname: None,
        schemaname,
        relname,
        inh: true,
        relpersistence: crate::catalog::pg_class::RELPERSISTENCE_PERMANENT,
        alias: None,
        location,
    }
}

/// PG `makeNotNullConstraint`: creates a `Constraint` for NOT NULL constraints.
pub fn make_not_null_constraint(_colname: &String_) -> Constraint {
    // keys = list_make1(colname); a T_String value node is not yet a Node enum
    // variant (crate::nodes::value). Not reachable for M1.
    let _ = ConstrType::NOTNULL;
    unimplemented!("makeNotNullConstraint: T_String value node not yet a Node variant")
}

/// PG `makeTypeName`: build a `TypeName` for an unqualified name.
pub fn make_type_name(_typnam: &str) -> TypeName {
    // makeTypeNameFromNameList(list_make1(makeString(typnam))); a T_String value
    // node is not yet a Node enum variant (crate::nodes::value). Not for M1.
    unimplemented!("makeTypeName: T_String value node not yet a Node variant")
}

/// PG `makeTypeNameFromNameList`: build a `TypeName` from a `String` name list.
pub fn make_type_name_from_name_list(names: Vec<Node>) -> TypeName {
    TypeName {
        names,
        typeOid: InvalidOid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: -1,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

/// PG `makeTypeNameFromOid`: build a `TypeName` for a type known by OID/typmod.
pub fn make_type_name_from_oid(type_oid: Oid, typmod: i32) -> TypeName {
    TypeName {
        names: Vec::new(),
        typeOid: type_oid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: typmod,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

/// PG `makeColumnDef`: build a simple `ColumnDef` (type/collation by OID).
pub fn make_column_def(colname: &str, type_oid: Oid, typmod: i32, coll_oid: Oid) -> ColumnDef {
    ColumnDef {
        colname: Some(colname.to_owned()),
        typeName: Some(Box::new(make_type_name_from_oid(type_oid, typmod))),
        compression: None,
        inhcount: 0,
        is_local: true,
        is_not_null: false,
        is_from_type: false,
        storage: 0,
        storage_name: None,
        raw_default: None,
        cooked_default: None,
        identity: 0,
        identitySequence: None,
        generated: 0,
        collClause: None,
        collOid: coll_oid,
        constraints: Vec::new(),
        fdwoptions: Vec::new(),
        location: -1,
    }
}

/// PG `makeFuncExpr`: build a `FuncExpr` for a function call (args pre-transformed).
pub fn make_func_expr(
    funcid: Oid,
    rettype: Oid,
    args: Vec<Node>,
    funccollid: Oid,
    inputcollid: Oid,
    fformat: CoercionForm,
) -> FuncExpr {
    FuncExpr {
        funcid,
        funcresulttype: rettype,
        funcretset: false,
        funcvariadic: false,
        funcformat: fformat,
        funccollid,
        inputcollid,
        args,
        location: -1,
    }
}

/// PG `make_opclause`: create an operator clause from operator info and operands.
///
/// Pass `rightop = None` to create a single-operand clause. `leftop` is always
/// supplied by callers (the C parameter is a non-NULL `Expr *`).
pub fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: Option<Node>,
    rightop: Option<Node>,
    opcollid: Oid,
    inputcollid: Oid,
) -> Node {
    let leftop = leftop.unwrap_or_else(|| unreachable!("make_opclause: leftop is NULL"));
    let args = match rightop {
        Some(r) => vec![leftop, r],
        None => vec![leftop],
    };
    Node::OpExpr(Box::new(OpExpr {
        opno,
        opfuncid: InvalidOid,
        opresulttype,
        opretset,
        opcollid,
        inputcollid,
        args,
        location: -1,
    }))
}

/// PG `make_andclause`: create an AND clause from a list of subclauses.
pub fn make_andclause(andclauses: Vec<Node>) -> Node {
    make_bool_expr(BoolExprType::AND_EXPR, andclauses, -1)
}

/// PG `make_orclause`: create an OR clause from a list of subclauses.
pub fn make_orclause(orclauses: Vec<Node>) -> Node {
    make_bool_expr(BoolExprType::OR_EXPR, orclauses, -1)
}

/// PG `make_notclause`: create a NOT clause negating the given expression.
pub fn make_notclause(notclause: Node) -> Node {
    make_bool_expr(BoolExprType::NOT_EXPR, vec![notclause], -1)
}

/// PG `make_and_qual`: AND two qual conditions; a NULL nodetree means TRUE.
pub fn make_and_qual(qual1: Option<Node>, qual2: Option<Node>) -> Option<Node> {
    match (qual1, qual2) {
        (None, q) | (q, None) => q,
        (Some(q1), Some(q2)) => Some(make_andclause(vec![q1, q2])),
    }
}

/// PG `make_ands_explicit`: convert an implicit-AND clause list to one expression.
/// An empty list is equivalent to TRUE.
pub fn make_ands_explicit(mut andclauses: Vec<Node>) -> Node {
    match andclauses.len() {
        0 => make_bool_const(true, false),
        1 => andclauses
            .pop()
            .unwrap_or_else(|| make_bool_const(true, false)),
        _ => make_andclause(andclauses),
    }
}

/// PG `make_ands_implicit`: convert a boolean expression to an implicit-AND list.
/// NULL and a constant-TRUE both map to an empty list.
pub fn make_ands_implicit(clause: Option<Node>) -> Vec<Node> {
    // NULL -> NIL list == TRUE (parser leaves qual NULL for no WHERE).
    let Some(c) = clause else {
        return Vec::new();
    };
    match c {
        Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR => b.args,
        // constant TRUE input -> NIL list
        Node::Const(ref k) if !k.constisnull && DatumGetBool(k.constvalue) => Vec::new(),
        other => vec![other],
    }
}

/// PG `makeIndexInfo`: create an `IndexInfo` node.
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "mirrors makeIndexInfo C signature 1:1"
)]
pub fn make_index_info(
    _numattrs: i32,
    _numkeyattrs: i32,
    _amoid: Oid,
    _expressions: Vec<Node>,
    _predicates: Vec<Node>,
    _unique: bool,
    _nulls_not_distinct: bool,
    _isready: bool,
    _concurrent: bool,
    _summarizing: bool,
    _withoutoverlaps: bool,
) -> crate::nodes::execnodes::IndexInfo {
    // ii_Context = CurrentMemoryContext (tombstoned); IndexInfo carries a
    // MemoryContext/ExprState substructure with no faithful M1 analog. Not
    // reachable for M1.
    unimplemented!("makeIndexInfo: MemoryContext/ExprState substructure deferred")
}

/// PG `makeGroupingSet`: create a `GroupingSet` node.
pub fn make_grouping_set(
    kind: GroupingSetKind,
    content: Vec<Node>,
    location: i32,
) -> GroupingSet {
    GroupingSet {
        kind,
        content,
        location,
    }
}

/// PG `makeVacuumRelation`: create a `VacuumRelation` node.
pub fn make_vacuum_relation(
    relation: Option<Box<RangeVar>>,
    oid: Oid,
    va_cols: Vec<Node>,
) -> VacuumRelation {
    VacuumRelation {
        relation,
        oid,
        va_cols,
    }
}

/// PG `makeStringConst`: build an `A_Const` node of type `T_String` for a string.
pub fn make_string_const(str: &str, location: i32) -> Node {
    Node::A_Const(Box::new(A_Const {
        val: ValUnion::String(makeString(str.to_owned())),
        isnull: false,
        location,
    }))
}

/// PG `makeDefElem`: build a `DefElem` (unqualified name, no special action).
pub fn make_def_elem(name: &str, arg: Option<Node>, location: i32) -> DefElem {
    DefElem {
        defnamespace: None,
        defname: Some(name.to_owned()),
        arg,
        defaction: DefElemAction::UNSPEC,
        location,
    }
}

/// PG `makeDefElemExtended`: build a `DefElem` with all fields specified.
pub fn make_def_elem_extended(
    name_space: Option<String>,
    name: &str,
    arg: Option<Node>,
    defaction: DefElemAction,
    location: i32,
) -> DefElem {
    DefElem {
        defnamespace: name_space,
        defname: Some(name.to_owned()),
        arg,
        defaction,
        location,
    }
}

/// PG `makeFuncCall`: initialize a `FuncCall` with the always-required info.
pub fn make_func_call(
    name: Vec<Node>,
    args: Vec<Node>,
    funcformat: CoercionForm,
    location: i32,
) -> FuncCall {
    FuncCall {
        funcname: name,
        args,
        agg_order: Vec::new(),
        agg_filter: None,
        over: None,
        agg_within_group: false,
        agg_star: false,
        agg_distinct: false,
        func_variadic: false,
        funcformat,
        location,
    }
}

/// PG `makeJsonFormat`: creates a `JsonFormat` node.
pub fn make_json_format(typ: JsonFormatType, encoding: JsonEncoding, location: i32) -> JsonFormat {
    JsonFormat {
        format_type: typ,
        encoding,
        location,
    }
}

/// PG `makeJsonValueExpr`: creates a `JsonValueExpr` node.
pub fn make_json_value_expr(
    raw_expr: Option<Node>,
    formatted_expr: Option<Node>,
    format: Option<Box<JsonFormat>>,
) -> JsonValueExpr {
    JsonValueExpr {
        raw_expr,
        formatted_expr,
        format,
    }
}

/// PG `makeJsonBehavior`: creates a `JsonBehavior` node.
pub fn make_json_behavior(
    btype: JsonBehaviorType,
    expr: Option<Node>,
    location: i32,
) -> JsonBehavior {
    JsonBehavior {
        btype,
        expr,
        coerce: false,
        location,
    }
}

/// PG `makeJsonKeyValue`: creates a `JsonKeyValue` node.
pub fn make_json_key_value(key: Option<Node>, value: Option<Node>) -> Node {
    // C: n->value = castNode(JsonValueExpr, value).
    let value = value.map(|v| match v {
        Node::JsonValueExpr(jve) => jve,
        _ => unreachable!("makeJsonKeyValue: value is not a JsonValueExpr"),
    });
    Node::JsonKeyValue(Box::new(JsonKeyValue { key, value }))
}

/// PG `makeJsonIsPredicate`: creates a `JsonIsPredicate` node.
pub fn make_json_is_predicate(
    expr: Option<Node>,
    format: Option<Box<JsonFormat>>,
    item_type: JsonValueType,
    unique_keys: bool,
    location: i32,
) -> Node {
    Node::JsonIsPredicate(Box::new(JsonIsPredicate {
        expr,
        format,
        item_type,
        unique_keys,
        location,
    }))
}

/// PG `makeJsonTablePath`: make a `JsonTablePath` for a path string and name.
pub fn make_json_table_path(pathvalue: &Const, pathname: Option<String>) -> JsonTablePath {
    JsonTablePath {
        value: Some(Box::new(pathvalue.clone())),
        name: pathname,
    }
}

/// PG `makeJsonTablePathSpec`: make a `JsonTablePathSpec` from a path string and name.
pub fn make_json_table_path_spec(
    string: Option<String>,
    name: Option<String>,
    string_location: i32,
    name_location: i32,
) -> JsonTablePathSpec {
    let string = string.unwrap_or_else(|| unreachable!("makeJsonTablePathSpec: string is NULL"));
    JsonTablePathSpec {
        string: Some(make_string_const(&string, string_location)),
        name,
        name_location,
        location: string_location,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::nodeFuncs::exprType;

    #[test]
    fn make_const_sets_fields_and_unknown_location() {
        let c = make_const(INT4OID, -1, InvalidOid, 4, Datum(42), false, true);
        assert_eq!(c.consttype, INT4OID);
        assert_eq!(c.constlen, 4);
        assert_eq!(c.constvalue, Datum(42));
        assert!(!c.constisnull);
        assert!(c.constbyval);
        assert_eq!(c.location, -1); // "unknown"
    }

    #[test]
    #[ignore = "needs get_typlenbyval (lsyscache), arrives step 02+"]
    fn make_null_const_is_null() {
        let c = make_null_const(BOOLOID, -1, InvalidOid);
        assert!(c.constisnull);
        assert_eq!(c.consttype, BOOLOID);
        assert_eq!(c.constvalue, Datum(0));
    }

    #[test]
    fn make_bool_const_is_bool_typed_const_node() {
        let n = make_bool_const(true, false);
        let Node::Const(c) = &n else {
            panic!("expected Const node");
        };
        assert_eq!(c.consttype, BOOLOID);
        assert_eq!(c.constlen, 1);
        assert!(DatumGetBool(c.constvalue));
        assert!(c.constbyval);
    }

    #[test]
    fn make_var_defaults() {
        let v = make_var(3, 2, INT4OID, -1, InvalidOid, 0);
        assert_eq!(v.varno, 3);
        assert_eq!(v.varattno, 2);
        assert_eq!(v.vartype, INT4OID);
        assert_eq!(v.varreturningtype, VarReturningType::DEFAULT);
        assert!(v.varnullingrels.is_none());
        assert_eq!(v.varnosyn, 3);
        assert_eq!(v.varattnosyn, 2);
        assert_eq!(v.location, -1);
    }

    #[test]
    fn make_target_entry_zeroes_extra_fields() {
        let expr = Some(Node::Const(Box::new(make_const(
            INT4OID,
            -1,
            InvalidOid,
            4,
            Datum(7),
            false,
            true,
        ))));
        let tle = make_target_entry(expr, 1, Some("c".to_owned()), false);
        assert_eq!(tle.resno, 1);
        assert_eq!(tle.resname.as_deref(), Some("c"));
        assert!(!tle.resjunk);
        assert_eq!(tle.ressortgroupref, 0);
        assert_eq!(tle.resorigtbl, InvalidOid);
        assert_eq!(tle.resorigcol, 0);
    }

    #[test]
    fn make_var_from_target_entry_pulls_type_from_expr() {
        let expr = Node::Const(Box::new(make_const(
            INT4OID,
            -1,
            InvalidOid,
            4,
            Datum(7),
            false,
            true,
        )));
        let tle = make_target_entry(Some(expr), 5, None, false);
        let v = make_var_from_target_entry(2, &tle);
        assert_eq!(v.varno, 2);
        assert_eq!(v.varattno, 5);
        assert_eq!(v.vartype, INT4OID);
        assert_eq!(exprType(&Node::Var(Box::new(v))), INT4OID);
    }

    #[test]
    fn make_ands_explicit_empty_is_true_const() {
        let n = make_ands_explicit(Vec::new());
        let Node::Const(c) = &n else {
            panic!("expected Const");
        };
        assert_eq!(c.consttype, BOOLOID);
        assert!(DatumGetBool(c.constvalue));
    }

    #[test]
    fn make_ands_explicit_single_returns_clause() {
        let clause = make_bool_const(false, false);
        let n = make_ands_explicit(vec![clause]);
        assert!(matches!(&n, Node::Const(_)));
    }

    #[test]
    fn make_ands_implicit_null_and_true_yield_empty() {
        assert!(make_ands_implicit(None).is_empty());
        let true_const = make_bool_const(true, false);
        assert!(make_ands_implicit(Some(true_const)).is_empty());
    }

    #[test]
    fn make_ands_implicit_splits_andclause() {
        let a = make_bool_const(false, false);
        let b = make_bool_const(false, false);
        let and = make_andclause(vec![a, b]);
        assert_eq!(make_ands_implicit(Some(and)).len(), 2);
    }

    #[test]
    fn make_and_qual_collapses_nulls() {
        let q = make_bool_const(false, false);
        assert!(make_and_qual(None, None).is_none());
        assert!(matches!(make_and_qual(Some(q), None), Some(b) if matches!(b, Node::Const(_))));
    }

    #[test]
    fn make_opclause_binary_has_two_args() {
        let l = make_bool_const(false, false);
        let r = make_bool_const(true, false);
        let n = make_opclause(InvalidOid, BOOLOID, false, Some(l), Some(r), InvalidOid, InvalidOid);
        let Node::OpExpr(o) = &n else {
            panic!("expected OpExpr");
        };
        assert_eq!(o.args.len(), 2);
        assert_eq!(o.opfuncid, InvalidOid);
        assert_eq!(o.location, -1);
    }
}
