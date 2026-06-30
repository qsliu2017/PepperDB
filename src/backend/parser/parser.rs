//! Translated from PostgreSQL src/backend/parser/parser.c
//!
//! Driver for the "raw" parser (the flex+bison phases). `raw_parser` feeds the
//! logos lexer (scan.rs) into the lalrpop grammar (gram.lalrpop) and returns the
//! list of RawStmt nodes. This module also holds the small `makeXxxConst` /
//! `makeRawStmt` node constructors that gram.y keeps as static helpers at the foot
//! of the grammar file; lalrpop semantic actions cannot hold Rust fn bodies as
//! cleanly as bison, so they live here and the grammar calls them.

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    A_Const, A_Star, ColumnRef, ColumnRefField, CreateStmt, IndexElem, IndexStmt, InsertStmt,
    RawStmt, ResTarget, SelectStmt, SetOperation, SortByDir, SortByNulls, TypeName, ValUnion,
};
use crate::nodes::primnodes::{OnCommitAction, OverridingKind, RangeVar};
use crate::nodes::value::{makeFloat, makeInteger, makeString};
use crate::parser::parser::RawParseMode;

// The parse tree's node-tree currency is the `Node` enum value itself: each
// variant already boxes its payload (`Node::A_Const(Box<A_Const>)`), so a `Node`
// is one pointer wide and needs no second box at the field/list level. gram.y's
// `Node *` thus maps to `Node`, and its `List *` of nodes to `Vec<Node>`.

/// gram.y `ResTarget *`.
pub type ResTargetBox = Box<ResTarget>;
/// gram.y `SelectStmt *`.
pub type SelectStmtBox = Box<crate::nodes::parsenodes::SelectStmt>;
/// gram.y `List *` of RawStmt (the parser result).
pub type RawStmtVec = Vec<Node>;

/// PG `makeIntConst`: an A_Const holding a T_Integer value.
pub fn make_int_const(val: i32) -> Node {
    a_const(ValUnion::Integer(makeInteger(val)))
}

/// PG `makeFloatConst`: an A_Const holding a T_Float value (kept as its text).
pub fn make_float_const(text: String) -> Node {
    a_const(ValUnion::Float(makeFloat(text)))
}

/// PG `makeStringConst`: an A_Const holding a T_String value.
pub fn make_string_const(text: String) -> Node {
    a_const(ValUnion::String(makeString(text)))
}

/// Build an A_Const node from an already-constructed value (location currently -1
/// pending location threading through the lexer; PG passes the `@N` token loc).
fn a_const(val: ValUnion) -> Node {
    Node::A_Const(Box::new(A_Const { val, isnull: false, location: -1 }))
}

/// PG `doNegate`: an integer A_Const flips its sign in place; a float A_Const gets
/// a leading '-' prepended to its text (gram.y's `doNegateFloat`). Any other
/// operand becomes the unary-minus operator A_Expr (`-` with a NULL left operand),
/// resolved to int4um/etc. at analysis time.
pub fn do_negate(arg: Node) -> Node {
    let Node::A_Const(mut c) = arg else {
        // Non-constant operand: makeSimpleA_Expr(AEXPR_OP, "-", NULL, arg).
        let a = crate::nodes::makefuncs::makeSimpleA_Expr(
            crate::nodes::parsenodes::A_Expr_Kind::OP,
            "-",
            None,
            Some(arg),
            -1,
        );
        return Node::A_Expr(Box::new(a));
    };
    match &mut c.val {
        ValUnion::Integer(i) => i.ival = -i.ival,
        ValUnion::Float(f) => {
            if f.fval.starts_with('-') {
                f.fval.remove(0);
            } else {
                f.fval.insert(0, '-');
            }
        }
        // A non-numeric constant (e.g. a string) also routes through the '-'
        // operator A_Expr (PG falls through to makeSimpleA_Expr); reachable once
        // string operands hit unary minus, not on the M3 numeric path.
        _ => {
            let a = crate::nodes::makefuncs::makeSimpleA_Expr(
                crate::nodes::parsenodes::A_Expr_Kind::OP,
                "-",
                None,
                Some(Node::A_Const(c)),
                -1,
            );
            return Node::A_Expr(Box::new(a));
        }
    }
    Node::A_Const(c)
}

/// gram.y `target_el: '*'` - a ResTarget whose value is a ColumnRef of one A_Star.
pub fn make_star_target() -> Node {
    let cr = ColumnRef {
        fields: vec![ColumnRefField::Star(A_Star {})],
        location: -1,
    };
    let rt = ResTarget {
        name: None,
        indirection: Vec::new(),
        val: Some(Node::ColumnRef(Box::new(cr))),
        location: -1,
    };
    Node::ResTarget(Box::new(rt))
}

/// gram.y `columnref`: build a ColumnRef from a dotted name-part list. The last
/// part may be `*` in PG; M2's grammar only constructs name parts here (the
/// `table.*` form grows with the indirection machinery), so every part is a String
/// field.
pub fn make_column_ref(parts: Vec<String>) -> Node {
    let fields = parts.into_iter().map(|p| ColumnRefField::String(makeString(p))).collect();
    Node::ColumnRef(Box::new(ColumnRef { fields, location: -1 }))
}

/// gram.y `table_ref: relation_expr`: a plain table name in FROM becomes a
/// RangeVar node (the from_clause/table_ref item). Subqueries grow at their
/// milestones; the optional alias is applied by `apply_rangevar_alias` first.
pub fn make_range_var_table_ref(relation: RangeVar) -> Node {
    Node::RangeVar(Box::new(relation))
}

/// gram.y `alias_clause`: build an `Alias` (no column-alias list yet).
pub fn make_alias(name: String) -> crate::nodes::primnodes::Alias {
    crate::nodes::primnodes::Alias { aliasname: Some(name), colnames: Vec::new() }
}

/// Apply an optional alias to a FROM-item RangeVar (sets `RangeVar.alias`).
pub fn apply_rangevar_alias(
    mut relation: RangeVar,
    alias: Option<crate::nodes::primnodes::Alias>,
) -> RangeVar {
    relation.alias = alias.map(Box::new);
    relation
}

/// gram.y `joined_table`: build a `JoinExpr` from the join type, the two arms, and
/// the ON-qual or USING column list. A USING list builds the `usingClause`
/// (resolved to equality quals at analysis time); ON builds `quals`.
pub fn make_join_expr(
    jointype: crate::nodes::nodes::JoinType,
    larg: Node,
    rarg: Node,
    quals: Option<Node>,
    using_cols: Vec<String>,
) -> Node {
    use crate::nodes::value::makeString;
    let using_clause = using_cols
        .into_iter()
        .map(|c| Node::String_(makeString(c)))
        .collect();
    Node::JoinExpr(Box::new(crate::nodes::primnodes::JoinExpr {
        jointype,
        isNatural: false,
        larg: Some(larg),
        rarg: Some(rarg),
        usingClause: using_clause,
        join_using_alias: None,
        quals,
        alias: None,
        rtindex: 0,
    }))
}

/// Apply an optional alias to a parenthesized joined table (`( a JOIN b ... ) x`).
pub fn apply_join_alias(join: Node, alias: Option<crate::nodes::primnodes::Alias>) -> Node {
    let Node::JoinExpr(mut j) = join else {
        return join;
    };
    j.alias = alias.map(Box::new);
    Node::JoinExpr(j)
}

/// gram.y `a_expr <op> a_expr`: a simple binary operator A_Expr (AEXPR_OP). The
/// operator name is a one-element list of a String value node (makeSimpleA_Expr).
pub fn make_a_expr(op: &str, lexpr: Node, rexpr: Node) -> Node {
    use crate::nodes::parsenodes::A_Expr_Kind;
    let a = crate::nodes::makefuncs::makeSimpleA_Expr(
        A_Expr_Kind::OP,
        op,
        Some(lexpr),
        Some(rexpr),
        -1,
    );
    Node::A_Expr(Box::new(a))
}

/// gram.y `a_expr AND a_expr` (makeAndExpr): flatten a left-nested AND chain into
/// one BoolExpr(AND_EXPR).
pub fn make_and_expr(lexpr: Node, rexpr: Node) -> Node {
    bool_chain(crate::nodes::primnodes::BoolExprType::AND_EXPR, lexpr, rexpr)
}

/// gram.y `a_expr OR a_expr` (makeOrExpr): flatten a left-nested OR chain.
pub fn make_or_expr(lexpr: Node, rexpr: Node) -> Node {
    bool_chain(crate::nodes::primnodes::BoolExprType::OR_EXPR, lexpr, rexpr)
}

/// Shared body of makeAndExpr/makeOrExpr: if `lexpr` is already a BoolExpr of the
/// same boolop, append `rexpr` to its args (PG's on-sight flattening); else build
/// a new two-arg BoolExpr.
fn bool_chain(boolop: crate::nodes::primnodes::BoolExprType, lexpr: Node, rexpr: Node) -> Node {
    if let Node::BoolExpr(mut b) = lexpr {
        if b.boolop == boolop {
            b.args.push(rexpr);
            return Node::BoolExpr(b);
        }
        return Node::BoolExpr(Box::new(crate::nodes::primnodes::BoolExpr {
            boolop,
            args: vec![Node::BoolExpr(b), rexpr],
            location: -1,
        }));
    }
    Node::BoolExpr(Box::new(crate::nodes::primnodes::BoolExpr {
        boolop,
        args: vec![lexpr, rexpr],
        location: -1,
    }))
}

/// gram.y `NOT a_expr` (makeNotExpr): a one-arg BoolExpr(NOT_EXPR).
pub fn make_not_expr(expr: Node) -> Node {
    Node::BoolExpr(Box::new(crate::nodes::primnodes::BoolExpr {
        boolop: crate::nodes::primnodes::BoolExprType::NOT_EXPR,
        args: vec![expr],
        location: -1,
    }))
}

/// gram.y `makeTypeCast`: a TypeCast node coercing `arg` to `type_name` (the
/// explicit `CAST(... AS t)` / `... :: t` display form is chosen at analysis time).
pub fn make_type_cast(arg: Node, type_name: TypeName) -> Node {
    Node::TypeCast(Box::new(crate::nodes::parsenodes::TypeCast {
        arg: Some(arg),
        typeName: Some(Box::new(type_name)),
        location: -1,
    }))
}

/// gram.y `makeStringConstCast`: a string A_Const wrapped in a TypeCast to
/// `type_name` -- the typed-literal form `typename 'string'` (e.g. `DATE '...'`).
pub fn make_string_const_cast(text: String, type_name: TypeName) -> Node {
    make_type_cast(make_string_const(text), type_name)
}

/// gram.y `case_expr`: a CaseExpr from the optional test arg, the WHEN/THEN arms
/// (a list of CaseWhen nodes), and the optional ELSE default.
pub fn make_case_expr(arg: Option<Node>, whens: Vec<Node>, defresult: Option<Node>) -> Node {
    Node::CaseExpr(Box::new(crate::nodes::primnodes::CaseExpr {
        casetype: crate::postgres_ext::InvalidOid,
        casecollid: crate::postgres_ext::InvalidOid,
        arg,
        args: whens,
        defresult,
        location: -1,
    }))
}

/// gram.y `when_clause`: a CaseWhen node (the condition + result of one arm).
pub fn make_case_when(expr: Node, result: Node) -> Node {
    Node::CaseWhen(Box::new(crate::nodes::primnodes::CaseWhen {
        expr: Some(expr),
        result: Some(result),
        location: -1,
    }))
}

/// gram.y `COALESCE '(' expr_list ')'`: a CoalesceExpr over the argument list.
pub fn make_coalesce_expr(args: Vec<Node>) -> Node {
    Node::CoalesceExpr(Box::new(crate::nodes::primnodes::CoalesceExpr {
        coalescetype: crate::postgres_ext::InvalidOid,
        coalescecollid: crate::postgres_ext::InvalidOid,
        args,
        location: -1,
    }))
}

/// gram.y `GREATEST`/`LEAST '(' expr_list ')'`: a MinMaxExpr over the arguments.
/// `is_greatest` selects IS_GREATEST vs IS_LEAST.
pub fn make_min_max_expr(is_greatest: bool, args: Vec<Node>) -> Node {
    use crate::nodes::primnodes::MinMaxOp;
    Node::MinMaxExpr(Box::new(crate::nodes::primnodes::MinMaxExpr {
        minmaxtype: crate::postgres_ext::InvalidOid,
        minmaxcollid: crate::postgres_ext::InvalidOid,
        inputcollid: crate::postgres_ext::InvalidOid,
        op: if is_greatest { MinMaxOp::GREATEST } else { MinMaxOp::LEAST },
        args,
        location: -1,
    }))
}

/// gram.y `NULLIF '(' a_expr ',' a_expr ')'`: makeSimpleA_Expr(AEXPR_NULLIF, "=").
pub fn make_nullif_expr(lexpr: Node, rexpr: Node) -> Node {
    use crate::nodes::parsenodes::A_Expr_Kind;
    let a = crate::nodes::makefuncs::makeSimpleA_Expr(
        A_Expr_Kind::NULLIF,
        "=",
        Some(lexpr),
        Some(rexpr),
        -1,
    );
    Node::A_Expr(Box::new(a))
}

/// gram.y `func_application`: build a FuncCall node from an (unqualified) function
/// name and a positional argument list (the EXPLICIT_CALL display form).
pub fn make_func_call(name_parts: Vec<String>, args: Vec<Node>) -> Node {
    let funcname = name_parts
        .into_iter()
        .map(|p| Node::String_(makeString(p)))
        .collect();
    let fc = crate::nodes::makefuncs::makeFuncCall(
        funcname,
        args,
        crate::nodes::primnodes::CoercionForm::EXPLICIT_CALL,
        -1,
    );
    Node::FuncCall(Box::new(fc))
}

/// gram.y `func_application: func_name '(' '*' ')'` -> a `FuncCall` with
/// `agg_star`. The classic `count(*)` form.
pub fn make_agg_star_call(name_parts: Vec<String>) -> Node {
    let Node::FuncCall(mut fc) = make_func_call(name_parts, Vec::new()) else {
        unreachable!("make_func_call yields a FuncCall");
    };
    fc.agg_star = true;
    Node::FuncCall(fc)
}

/// gram.y `func_application: func_name '(' DISTINCT func_arg_list ')'` -> a
/// `FuncCall` with `agg_distinct`.
pub fn make_distinct_func_call(name_parts: Vec<String>, args: Vec<Node>) -> Node {
    let Node::FuncCall(mut fc) = make_func_call(name_parts, args) else {
        unreachable!("make_func_call yields a FuncCall");
    };
    fc.agg_distinct = true;
    Node::FuncCall(fc)
}

/// gram.y `sortby: a_expr opt_asc_desc opt_nulls_order` -> a `SortBy` node.
pub fn make_sortby(
    expr: Node,
    dir: crate::nodes::parsenodes::SortByDir,
    nulls: crate::nodes::parsenodes::SortByNulls,
) -> Node {
    Node::SortBy(Box::new(crate::nodes::parsenodes::SortBy {
        node: Some(expr),
        sortby_dir: dir,
        sortby_nulls: nulls,
        useOp: Vec::new(),
        location: -1,
    }))
}

/// gram.y `insertSelectOptions`: stamp ORDER BY / LIMIT / OFFSET onto the
/// SelectStmt built by `simple_select`. M5 supports a leaf simple_select (no WITH /
/// set-op wrapper, which PG rejects multiple sort/limit clauses for).
pub fn insert_select_options(
    stmt: Node,
    sort_clause: Vec<Node>,
    limit_offset: Option<Node>,
    limit_count: Option<Node>,
) -> Node {
    let Node::SelectStmt(mut sel) = stmt else {
        unreachable!("insert_select_options over a SelectStmt");
    };
    if !sort_clause.is_empty() {
        sel.sortClause = sort_clause;
    }
    if limit_offset.is_some() {
        sel.limitOffset = limit_offset;
    }
    if limit_count.is_some() {
        sel.limitCount = limit_count;
    }
    Node::SelectStmt(sel)
}

/// gram.y `insert_rest: VALUES ...`: wrap the parsed VALUES rows in a SelectStmt
/// carrying `valuesLists`, exactly as gram.y builds the VALUES clause. Each row is
/// a RowExpr carrier (see gram.lalrpop ValuesRow). The targetList/fromClause are
/// empty for a VALUES SelectStmt.
pub fn make_values_select(values_lists: Vec<Node>) -> Node {
    Node::SelectStmt(Box::new(empty_select_stmt(Vec::new(), Vec::new(), values_lists)))
}

/// gram.y `InsertStmt`: build the raw InsertStmt node (M2 plain form). WITH / ON
/// CONFLICT / RETURNING / OVERRIDING grow at their milestones.
pub fn make_insert_stmt(relation: RangeVar, cols: Vec<Node>, select_stmt: Option<Node>) -> Node {
    Node::InsertStmt(Box::new(InsertStmt {
        relation: Some(Box::new(relation)),
        cols,
        selectStmt: select_stmt,
        onConflictClause: None,
        returningClause: None,
        withClause: None,
        r#override: OverridingKind::NOT_SET,
    }))
}

/// A zero-default SelectStmt (makeNode(SelectStmt) semantics) carrying the given
/// target list, FROM clause, and VALUES lists; every other clause is empty. Shared
/// by the grammar's SimpleSelect and the VALUES wrapper.
fn empty_select_stmt(
    target_list: Vec<Node>,
    from_clause: Vec<Node>,
    values_lists: Vec<Node>,
) -> SelectStmt {
    SelectStmt {
        distinctClause: Vec::new(),
        intoClause: None,
        targetList: target_list,
        fromClause: from_clause,
        whereClause: None,
        groupClause: Vec::new(),
        groupDistinct: false,
        havingClause: None,
        windowClause: Vec::new(),
        valuesLists: values_lists,
        sortClause: Vec::new(),
        limitOffset: None,
        limitCount: None,
        limitOption: crate::nodes::nodes::LimitOption::COUNT,
        lockingClause: Vec::new(),
        withClause: None,
        op: SetOperation::NONE,
        all: false,
        larg: None,
        rarg: None,
    }
}

/// gram.y `CreateStmt: CREATE ... TABLE qualified_name '(' OptTableElementList ')'`
/// (the M2 plain form). Builds the raw `CreateStmt` node; the inheritance /
/// partition / OF-type / WITH / ON COMMIT / tablespace / access-method clauses
/// default to empty and grow at their milestones.
pub fn make_create_stmt(relation: RangeVar, table_elts: Vec<Node>) -> Node {
    Node::CreateStmt(Box::new(CreateStmt {
        relation: Some(Box::new(relation)),
        tableElts: table_elts,
        inhRelations: Vec::new(),
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: Vec::new(),
        nnconstraints: Vec::new(),
        options: Vec::new(),
        oncommit: OnCommitAction::NOOP,
        tablespacename: None,
        accessMethod: None,
        if_not_exists: false,
    }))
}

/// gram.y `IndexStmt: CREATE [UNIQUE] INDEX [name] ON qualified_name
/// [USING am] '(' index_params ')'` (the M6 plain form). Builds the raw
/// `IndexStmt`; CONCURRENTLY / IF NOT EXISTS / INCLUDE / WITH / tablespace /
/// WHERE (partial) / constraint forms default to empty and grow at their milestones.
pub fn make_index_stmt(
    unique: bool,
    idxname: Option<String>,
    relation: RangeVar,
    access_method: Option<String>,
    index_params: Vec<Node>,
) -> Node {
    use crate::postgres_ext::InvalidOid;
    Node::IndexStmt(Box::new(IndexStmt {
        idxname,
        relation: Some(Box::new(relation)),
        accessMethod: access_method,
        tableSpace: None,
        indexParams: index_params,
        indexIncludingParams: Vec::new(),
        options: Vec::new(),
        whereClause: None,
        excludeOpNames: Vec::new(),
        idxcomment: None,
        indexOid: InvalidOid,
        oldNumber: InvalidOid,
        oldCreateSubid: crate::c::SubTransactionId(0),
        oldFirstRelfilelocatorSubid: crate::c::SubTransactionId(0),
        unique,
        nulls_not_distinct: false,
        primary: false,
        isconstraint: false,
        iswithoutoverlaps: false,
        deferrable: false,
        initdeferred: false,
        transformed: false,
        concurrent: false,
        if_not_exists: false,
        reset_default_tblspc: false,
    }))
}

/// gram.y `index_elem: ColId opt_asc_desc ...` (the M6 plain form). A bare column
/// name with optional ASC/DESC; opclasses / expressions / collations / NULLS
/// ordering grow at their milestones.
pub fn make_index_elem(name: String, ordering: SortByDir) -> Node {
    Node::IndexElem(Box::new(IndexElem {
        name: Some(name),
        expr: None,
        indexcolname: None,
        collation: Vec::new(),
        opclass: Vec::new(),
        opclassopts: Vec::new(),
        ordering,
        nulls_ordering: SortByNulls::DEFAULT,
    }))
}

/// gram.y `columnDef: ColId Typename ...` (the M2 plain form). Builds a `ColumnDef`
/// carrying just the name and type; storage / compression / collation and the
/// constraint list (ColQualList) grow at their milestones.
pub fn make_column_def_elt(colname: String, type_name: TypeName) -> Node {
    Node::ColumnDef(Box::new(crate::nodes::parsenodes::ColumnDef {
        colname: Some(colname),
        typeName: Some(Box::new(type_name)),
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
        collOid: crate::postgres_ext::InvalidOid,
        constraints: Vec::new(),
        fdwoptions: Vec::new(),
        location: -1,
    }))
}

/// gram.y `SystemTypeName(name)`: a `pg_catalog`-qualified built-in type name
/// (`makeTypeNameFromNameList(["pg_catalog", name])`), used by the `Numeric`
/// productions (e.g. `int`/`integer` -> `pg_catalog.int4`).
pub fn system_type_name(name: &str) -> TypeName {
    crate::nodes::makefuncs::makeTypeNameFromNameList(vec![
        makeString("pg_catalog".to_owned()),
        makeString(name.to_owned()),
    ])
}

/// gram.y `GenericType`: a bare (unqualified) type name resolved by catalog lookup
/// at analysis time (`makeTypeNameFromNameList([name])`).
pub fn generic_type_name(name: String) -> TypeName {
    crate::nodes::makefuncs::makeTypeNameFromNameList(vec![makeString(name)])
}

/// PG `makeRawStmt`: wrap a top-level statement node in a RawStmt. The
/// stmt_location / stmt_len text bookkeeping that gram.y threads from `@N` is
/// added when location tracking is wired through the lexer.
pub fn make_raw_stmt(stmt: Node) -> Node {
    Node::RawStmt(Box::new(RawStmt {
        stmt: Some(stmt),
        stmt_location: 0,
        stmt_len: 0,
    }))
}

/// PG `raw_parser`: parse `s` into a list of RawStmt nodes. Only RAW_PARSE_DEFAULT
/// (a `;`-separated command list) is supported in M1; the MODE_* alternate entry
/// points (type-name / plpgsql) are added with that grammar.
///
/// A lexing or grammar error raises `ereport(ERROR, syntax error)` through the
/// normal unwind path (error.md s3) - never a bare panic.
pub fn raw_parser(s: &str, mode: RawParseMode) -> RawStmtVec {
    if mode != RawParseMode::Default {
        unimplemented!("raw_parser modes other than DEFAULT are deferred");
    }

    let tokens = crate::backend::parser::scan::lex(s);
    match crate::backend::parser::gram::StmtmultiParser::new().parse(tokens) {
        Ok(list) => list,
        Err(err) => syntax_error(s, &err),
    }
}

/// Raise a PG-style "syntax error" for a lalrpop parse error, locating it like
/// PG's `scanner_yyerror`. Diverges (>= ERROR).
fn syntax_error(
    src: &str,
    err: &lalrpop_util::ParseError<
        i32,
        crate::backend::parser::scan::Token,
        crate::backend::parser::scan::LexError,
    >,
) -> ! {
    use lalrpop_util::ParseError;
    let loc: usize = match err {
        ParseError::InvalidToken { location }
        | ParseError::UnrecognizedToken { token: (location, _, _), .. }
        | ParseError::ExtraToken { token: (location, _, _) } => *location as usize,
        ParseError::UnrecognizedEof { .. } => src.len(),
        ParseError::User { error } => error.location as usize,
    };
    let at = if loc < src.len() {
        format!(" at or near \"{}\"", &src[loc..])
    } else {
        " at end of input".to_string()
    };
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("syntax error{at}"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> RawStmtVec {
        raw_parser(s, RawParseMode::Default)
    }

    /// Unwrap the single SelectStmt out of a one-statement parse.
    fn one_select(list: &RawStmtVec) -> &crate::nodes::parsenodes::SelectStmt {
        assert_eq!(list.len(), 1, "expected exactly one RawStmt");
        let Node::RawStmt(rs) = &list[0] else { panic!("not a RawStmt") };
        let Some(stmt) = &rs.stmt else { panic!("empty RawStmt") };
        let Node::SelectStmt(sel) = stmt else { panic!("not a SelectStmt") };
        sel
    }

    fn int_of_restarget(node: &Node) -> i32 {
        let Node::ResTarget(rt) = node else { panic!("not a ResTarget") };
        let Some(val) = &rt.val else { panic!("ResTarget has no val") };
        let Node::A_Const(c) = val else { panic!("ResTarget val not A_Const") };
        let ValUnion::Integer(i) = &c.val else { panic!("A_Const not Integer") };
        i.ival
    }

    #[test]
    fn select_one() {
        let list = parse("SELECT 1");
        let sel = one_select(&list);
        assert_eq!(sel.targetList.len(), 1);
        assert_eq!(int_of_restarget(&sel.targetList[0]), 1);
    }

    #[test]
    fn select_one_trailing_semicolon() {
        let list = parse("SELECT 1;");
        let sel = one_select(&list);
        assert_eq!(int_of_restarget(&sel.targetList[0]), 1);
    }

    #[test]
    fn two_statements() {
        let list = parse("SELECT 1; SELECT 2;");
        assert_eq!(list.len(), 2);
        // each is a SelectStmt
        for (i, stmt) in list.iter().enumerate() {
            let Node::RawStmt(rs) = stmt else { panic!("not RawStmt") };
            let Node::SelectStmt(sel) = rs.stmt.as_ref().unwrap() else { panic!() };
            assert_eq!(int_of_restarget(&sel.targetList[0]), (i + 1) as i32);
        }
    }

    #[test]
    fn select_large_literal() {
        let list = parse("SELECT 42");
        assert_eq!(int_of_restarget(&one_select(&list).targetList[0]), 42);
    }

    #[test]
    fn select_negative_literal() {
        let list = parse("SELECT -7");
        assert_eq!(int_of_restarget(&one_select(&list).targetList[0]), -7);
    }

    #[test]
    fn select_overflow_literal_is_float() {
        let list = parse("SELECT 9999999999");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::A_Const(c) = rt.val.as_ref().unwrap() else { panic!() };
        assert!(matches!(&c.val, ValUnion::Float(f) if f.fval == "9999999999"));
    }

    #[test]
    fn select_star() {
        let list = parse("SELECT *");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!("not ResTarget") };
        let Node::ColumnRef(cr) = rt.val.as_ref().unwrap() else { panic!("not ColumnRef") };
        assert_eq!(cr.fields.len(), 1);
        assert!(matches!(&cr.fields[0], ColumnRefField::Star(_)));
    }

    #[test]
    fn empty_input_is_empty_list() {
        assert!(parse("").is_empty());
        assert!(parse(";").is_empty());
    }

    /// Unwrap the single CreateStmt out of a one-statement parse.
    fn one_create(list: &RawStmtVec) -> &CreateStmt {
        assert_eq!(list.len(), 1, "expected exactly one RawStmt");
        let Node::RawStmt(rs) = &list[0] else { panic!("not a RawStmt") };
        let Node::CreateStmt(c) = rs.stmt.as_ref().expect("non-empty RawStmt") else {
            panic!("not a CreateStmt")
        };
        c
    }

    fn column(c: &CreateStmt, i: usize) -> &crate::nodes::parsenodes::ColumnDef {
        let Node::ColumnDef(cd) = &c.tableElts[i] else { panic!("not a ColumnDef") };
        cd
    }

    #[test]
    fn create_table_one_int_column() {
        let list = parse("CREATE TABLE t (a int)");
        let c = one_create(&list);
        assert_eq!(c.relation.as_ref().unwrap().relname.as_deref(), Some("t"));
        assert_eq!(c.tableElts.len(), 1);
        let col = column(c, 0);
        assert_eq!(col.colname.as_deref(), Some("a"));
        // `int` -> pg_catalog.int4 (a SystemTypeName 2-part name).
        let tn = col.typeName.as_ref().unwrap();
        let names: Vec<&str> = tn.names.iter().map(|s| s.sval.as_str()).collect();
        assert_eq!(names, ["pg_catalog", "int4"]);
    }

    #[test]
    fn create_table_two_columns() {
        let list = parse("CREATE TABLE t (a int, b integer)");
        let c = one_create(&list);
        assert_eq!(c.tableElts.len(), 2);
        assert_eq!(column(c, 0).colname.as_deref(), Some("a"));
        assert_eq!(column(c, 1).colname.as_deref(), Some("b"));
    }

    #[test]
    fn create_table_generic_type_name() {
        // A non-keyword type identifier parses as a bare (unqualified) GenericType
        // (the keyword-spelled type names like `text` grow as ColId admits those
        // keyword categories).
        let list = parse("CREATE TABLE t (a myt)");
        let c = one_create(&list);
        let tn = column(c, 0).typeName.as_ref().unwrap();
        let names: Vec<&str> = tn.names.iter().map(|s| s.sval.as_str()).collect();
        assert_eq!(names, ["myt"]);
    }

    #[test]
    fn create_table_schema_qualified() {
        let list = parse("CREATE TABLE public.t (a int)");
        let c = one_create(&list);
        let rv = c.relation.as_ref().unwrap();
        assert_eq!(rv.schemaname.as_deref(), Some("public"));
        assert_eq!(rv.relname.as_deref(), Some("t"));
    }

    #[test]
    fn select_star_from_table_parses() {
        let list = parse("SELECT * FROM t");
        let sel = one_select(&list);
        assert_eq!(sel.fromClause.len(), 1);
        let Node::RangeVar(rv) = &sel.fromClause[0] else { panic!("FROM item not a RangeVar") };
        assert_eq!(rv.relname.as_deref(), Some("t"));
        // `*` target.
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::ColumnRef(cr) = rt.val.as_ref().unwrap() else { panic!() };
        assert!(matches!(&cr.fields[0], ColumnRefField::Star(_)));
    }

    #[test]
    fn select_column_from_table_parses() {
        let list = parse("SELECT a FROM t");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::ColumnRef(cr) = rt.val.as_ref().unwrap() else { panic!("not a ColumnRef") };
        assert!(matches!(&cr.fields[0], ColumnRefField::String(s) if s.sval == "a"));
    }

    #[test]
    fn insert_values_parses() {
        let list = parse("INSERT INTO t VALUES (1)");
        assert_eq!(list.len(), 1);
        let Node::RawStmt(rs) = &list[0] else { panic!("not a RawStmt") };
        let Node::InsertStmt(ins) = rs.stmt.as_ref().unwrap() else { panic!("not an InsertStmt") };
        assert_eq!(ins.relation.as_ref().unwrap().relname.as_deref(), Some("t"));
        assert!(ins.cols.is_empty(), "no explicit column list");
        // VALUES wrapped as a SelectStmt with one RowExpr-carried row.
        let Node::SelectStmt(sel) = ins.selectStmt.as_ref().unwrap() else { panic!("source not a SelectStmt") };
        assert_eq!(sel.valuesLists.len(), 1);
        let Node::RowExpr(row) = &sel.valuesLists[0] else { panic!("row not a RowExpr") };
        assert_eq!(row.args.len(), 1);
        assert!(matches!(&row.args[0], Node::A_Const(_)));
    }

    #[test]
    fn insert_with_column_list_parses() {
        let list = parse("INSERT INTO t (a) VALUES (1)");
        let Node::RawStmt(rs) = &list[0] else { panic!() };
        let Node::InsertStmt(ins) = rs.stmt.as_ref().unwrap() else { panic!() };
        assert_eq!(ins.cols.len(), 1);
        let Node::ResTarget(rt) = &ins.cols[0] else { panic!("col not a ResTarget") };
        assert_eq!(rt.name.as_deref(), Some("a"));
    }

    #[test]
    fn select_binary_op_parses() {
        let list = parse("SELECT a + 1 FROM t");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::A_Expr(a) = rt.val.as_ref().unwrap() else { panic!("not an A_Expr") };
        assert!(matches!(a.kind, crate::nodes::parsenodes::A_Expr_Kind::OP));
        // name is a one-element list of the String "+".
        let Node::String_(s) = &a.name[0] else { panic!("op name not a String node") };
        assert_eq!(s.sval, "+");
        assert!(matches!(a.lexpr, Some(Node::ColumnRef(_))));
        assert!(matches!(a.rexpr, Some(Node::A_Const(_))));
    }

    #[test]
    fn precedence_mul_binds_tighter_than_add() {
        // a + b * c  parses as  a + (b * c)
        let list = parse("SELECT a + b * c");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::A_Expr(add) = rt.val.as_ref().unwrap() else { panic!() };
        let Node::String_(s) = &add.name[0] else { panic!() };
        assert_eq!(s.sval, "+");
        // right operand is the `b * c` A_Expr.
        let Some(Node::A_Expr(mul)) = &add.rexpr else { panic!("rhs not the mul A_Expr") };
        let Node::String_(ms) = &mul.name[0] else { panic!() };
        assert_eq!(ms.sval, "*");
    }

    #[test]
    fn where_clause_comparison_parses() {
        let list = parse("SELECT a FROM t WHERE a > 0");
        let sel = one_select(&list);
        let Some(Node::A_Expr(a)) = &sel.whereClause else { panic!("no WHERE A_Expr") };
        let Node::String_(s) = &a.name[0] else { panic!() };
        assert_eq!(s.sval, ">");
    }

    #[test]
    fn bool_and_or_not_parse_to_boolexpr() {
        // `a AND b OR NOT c` -> OR( AND(a,b), NOT(c) )
        let list = parse("SELECT a FROM t WHERE a AND b OR NOT c");
        let sel = one_select(&list);
        let Some(Node::BoolExpr(or)) = &sel.whereClause else { panic!("top not BoolExpr") };
        assert!(matches!(or.boolop, crate::nodes::primnodes::BoolExprType::OR_EXPR));
        assert_eq!(or.args.len(), 2);
        let Node::BoolExpr(and) = &or.args[0] else { panic!("lhs not AND") };
        assert!(matches!(and.boolop, crate::nodes::primnodes::BoolExprType::AND_EXPR));
        let Node::BoolExpr(not) = &or.args[1] else { panic!("rhs not NOT") };
        assert!(matches!(not.boolop, crate::nodes::primnodes::BoolExprType::NOT_EXPR));
    }

    #[test]
    fn and_chain_flattens() {
        // a AND b AND c -> one AND BoolExpr with three args.
        let list = parse("SELECT a FROM t WHERE a AND b AND c");
        let sel = one_select(&list);
        let Some(Node::BoolExpr(and)) = &sel.whereClause else { panic!() };
        assert!(matches!(and.boolop, crate::nodes::primnodes::BoolExprType::AND_EXPR));
        assert_eq!(and.args.len(), 3);
    }

    #[test]
    fn cast_operator_and_keyword_parse_to_typecast() {
        // `a :: numeric` and `CAST(a AS numeric)` both yield a TypeCast.
        for sql in ["SELECT a::numeric FROM t", "SELECT CAST(a AS numeric) FROM t"] {
            let list = parse(sql);
            let sel = one_select(&list);
            let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
            let Node::TypeCast(tc) = rt.val.as_ref().unwrap() else { panic!("not a TypeCast: {sql}") };
            let names: Vec<&str> = tc.typeName.as_ref().unwrap().names.iter().map(|s| s.sval.as_str()).collect();
            assert_eq!(names, ["pg_catalog", "numeric"]);
        }
    }

    #[test]
    fn case_expr_parses() {
        let list = parse("SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM t");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::CaseExpr(c) = rt.val.as_ref().unwrap() else { panic!("not a CaseExpr") };
        assert!(c.arg.is_none(), "searched CASE has no test arg");
        assert_eq!(c.args.len(), 1);
        assert!(c.defresult.is_some(), "ELSE present");
        let Node::CaseWhen(_) = &c.args[0] else { panic!("arm not a CaseWhen") };
    }

    #[test]
    fn coalesce_nullif_greatest_least_parse() {
        let Node::CoalesceExpr(c) = expr_of(&parse("SELECT COALESCE(a, 0) FROM t")) else { panic!("not Coalesce") };
        assert_eq!(c.args.len(), 2);

        let Node::A_Expr(a) = expr_of(&parse("SELECT NULLIF(a, 0) FROM t")) else { panic!("not A_Expr") };
        assert!(matches!(a.kind, crate::nodes::parsenodes::A_Expr_Kind::NULLIF));

        let Node::MinMaxExpr(g) = expr_of(&parse("SELECT GREATEST(a, 0) FROM t")) else { panic!("not MinMax") };
        assert!(matches!(g.op, crate::nodes::primnodes::MinMaxOp::GREATEST));
        let Node::MinMaxExpr(l) = expr_of(&parse("SELECT LEAST(a, 0) FROM t")) else { panic!("not MinMax") };
        assert!(matches!(l.op, crate::nodes::primnodes::MinMaxOp::LEAST));
    }

    #[test]
    fn typed_literal_parses_to_typecast() {
        // `NUMERIC '1.5'` -> a TypeCast of a string A_Const to pg_catalog.numeric
        // (`numeric` is a Numeric-production keyword).
        let Node::TypeCast(tc) = expr_of(&parse("SELECT NUMERIC '1.5'")) else { panic!("not a TypeCast (numeric)") };
        let names: Vec<&str> = tc.typeName.as_ref().unwrap().names.iter().map(|s| s.sval.as_str()).collect();
        assert_eq!(names, ["pg_catalog", "numeric"]);
        assert!(matches!(tc.arg.as_ref(), Some(Node::A_Const(_))));

        // `DATE '2024-01-15'` -> a TypeCast to the (unqualified) `date` type. `date`
        // is not a SQL keyword in PG, so it flows through GenericType (a bare name
        // resolved by catalog lookup), exactly as PG does.
        let Node::TypeCast(tc) = expr_of(&parse("SELECT DATE '2024-01-15'")) else { panic!("not a TypeCast (date)") };
        let names: Vec<&str> = tc.typeName.as_ref().unwrap().names.iter().map(|s| s.sval.as_str()).collect();
        assert_eq!(names, ["date"]);
    }

    /// The (cloned) target-list value of a single-target SELECT.
    fn expr_of(list: &RawStmtVec) -> Node {
        let sel = one_select(list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!("not ResTarget") };
        rt.val.clone().expect("ResTarget has a val")
    }

    #[test]
    fn func_call_parses() {
        let list = parse("SELECT f(a, 1) FROM t");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::FuncCall(fc) = rt.val.as_ref().unwrap() else { panic!("not a FuncCall") };
        let Node::String_(s) = &fc.funcname[0] else { panic!() };
        assert_eq!(s.sval, "f");
        assert_eq!(fc.args.len(), 2);
    }

    #[test]
    fn parenthesized_expr_parses() {
        // (a + b) * c  -> mul whose lhs is the add A_Expr.
        let list = parse("SELECT (a + b) * c");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::A_Expr(mul) = rt.val.as_ref().unwrap() else { panic!() };
        let Node::String_(s) = &mul.name[0] else { panic!() };
        assert_eq!(s.sval, "*");
        let Some(Node::A_Expr(add)) = &mul.lexpr else { panic!("lhs not the add A_Expr") };
        let Node::String_(asym) = &add.name[0] else { panic!() };
        assert_eq!(asym.sval, "+");
    }

    #[test]
    fn syntax_error_select_from() {
        // SELECT FROM has no FROM production yet -> grammar error -> ereport(ERROR),
        // which raises via panic_any(ErrorData) (not a bare string panic). Catch it
        // and confirm it is a structured syntax error, not an unwrap/index panic.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let res = std::panic::catch_unwind(|| parse("SELECT FROM"));
        std::panic::set_hook(prev);

        let payload = res.expect_err("SELECT FROM must raise a parse error");
        let edata = payload
            .downcast_ref::<crate::utils::elog::ErrorData>()
            .expect("panic payload should be a structured ErrorData");
        assert_eq!(edata.elevel, crate::utils::elog::ERROR);
        assert_eq!(edata.sqlerrcode, crate::utils::errcodes::ERRCODE_SYNTAX_ERROR);
        assert!(edata.message.as_deref().unwrap_or("").contains("syntax error"));
    }
}
