//! Translated from PostgreSQL src/backend/parser/parser.c
//!
//! Driver for the "raw" parser (the flex+bison phases). `raw_parser` feeds the
//! logos lexer (scan.rs) into the lalrpop grammar (gram.lalrpop) and returns the
//! list of RawStmt nodes. This module also holds the small `makeXxxConst` /
//! `makeRawStmt` node constructors that gram.y keeps as static helpers at the foot
//! of the grammar file; lalrpop semantic actions cannot hold Rust fn bodies as
//! cleanly as bison, so they live here and the grammar calls them.

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::lockoptions::{LockClauseStrength, LockWaitPolicy};
use crate::nodes::parsenodes::{
    A_Const, A_Star, ColumnRef, ColumnRefField, CreateStmt, DeleteStmt, IndexElem, IndexStmt,
    InsertStmt, LockingClause, MergeStmt, MergeWhenClause, RawStmt, ResTarget, ReturningClause,
    SelectStmt, SetOperation, SortByDir, SortByNulls, TransactionStmt, TransactionStmtKind,
    TypeName, UpdateStmt, ValUnion, VariableSetKind, VariableSetStmt, VariableShowStmt,
};
use crate::nodes::primnodes::{MergeMatchKind, OnCommitAction, OverridingKind, RangeVar};
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

/// gram.y `c_expr: PARAM opt_indirection`: a `$n` positional parameter reference.
/// The opt_indirection (subscripts/field selection on the param) grows with the
/// A_Indirection machinery; M9 reaches the bare `$n`.
pub fn make_param_ref(number: i32, location: i32) -> Node {
    Node::ParamRef(Box::new(crate::nodes::parsenodes::ParamRef { number, location }))
}

/// gram.y `PrepareStmt: PREPARE name prep_type_clause AS PreparableStmt`. The
/// argtype Typenames are each wrapped as a `Node::TypeName` (the parse-list element
/// currency); the query is the preparable statement node.
pub fn make_prepare_stmt(name: String, argtypes: Vec<TypeName>, query: Node) -> Node {
    let argtypes = argtypes.into_iter().map(|t| Node::TypeName(Box::new(t))).collect();
    Node::PrepareStmt(Box::new(crate::nodes::parsenodes::PrepareStmt {
        name: Some(name),
        argtypes,
        query: Some(query),
    }))
}

/// gram.y `ExecuteStmt: EXECUTE name execute_param_clause`.
pub fn make_execute_stmt(name: String, params: Vec<Node>) -> Node {
    Node::ExecuteStmt(Box::new(crate::nodes::parsenodes::ExecuteStmt {
        name: Some(name),
        params,
    }))
}

/// gram.y `DeallocateStmt`: DEALLOCATE [PREPARE] name | DEALLOCATE [PREPARE] ALL.
pub fn make_deallocate_stmt(name: Option<String>, location: i32) -> Node {
    let isall = name.is_none();
    Node::DeallocateStmt(Box::new(crate::nodes::parsenodes::DeallocateStmt {
        name,
        isall,
        location,
    }))
}

/// gram.y `DeclareCursorStmt`. `options` already folds cursor_options | opt_hold |
/// CURSOR_OPT_FAST_PLAN (PG always sets FAST_PLAN).
pub fn make_declare_cursor_stmt(portalname: String, options: i32, query: Node) -> Node {
    Node::DeclareCursorStmt(Box::new(crate::nodes::parsenodes::DeclareCursorStmt {
        portalname: Some(portalname),
        options,
        query: Some(query),
    }))
}

/// gram.y `ClosePortalStmt`: CLOSE cursor_name | CLOSE ALL (`None` portalname).
pub fn make_close_portal_stmt(portalname: Option<String>) -> Node {
    Node::ClosePortalStmt(Box::new(crate::nodes::parsenodes::ClosePortalStmt { portalname }))
}

/// gram.y `FetchStmt`: build the FetchStmt for a `fetch_args` form. `ismove` is
/// stamped by the FETCH vs MOVE wrapper.
pub fn make_fetch_stmt(
    direction: crate::nodes::parsenodes::FetchDirection,
    how_many: i64,
    portalname: String,
    ismove: bool,
) -> Node {
    Node::FetchStmt(Box::new(crate::nodes::parsenodes::FetchStmt {
        direction,
        howMany: how_many,
        portalname: Some(portalname),
        ismove,
    }))
}

/// gram.y `FetchStmt: MOVE fetch_args`: flip a `fetch_args`-built FetchStmt to a
/// MOVE (`ismove = true`).
pub fn set_fetch_ismove(stmt: Node) -> Node {
    let Node::FetchStmt(mut f) = stmt else {
        unreachable!("set_fetch_ismove on a non-FetchStmt");
    };
    f.ismove = true;
    Node::FetchStmt(f)
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

/// gram.y `set_clause: set_target '=' a_expr`: a ResTarget naming the assigned
/// column (`name`) with the value expression in `val` (M8, step 34).
pub fn make_set_target(col: String, val: Node) -> Node {
    Node::ResTarget(Box::new(ResTarget {
        name: Some(col),
        indirection: Vec::new(),
        val: Some(val),
        location: -1,
    }))
}

/// gram.y `returning_clause`: wrap the RETURNING target list in a ReturningClause
/// (empty list -> None, no RETURNING). M8 has no RETURNING options (OLD/NEW alias).
pub fn make_returning_clause(exprs: Vec<Node>) -> Option<Box<ReturningClause>> {
    if exprs.is_empty() {
        None
    } else {
        Some(Box::new(ReturningClause { options: Vec::new(), exprs }))
    }
}

/// gram.y `UpdateStmt`: build the raw `UpdateStmt` node (M8 plain form). WITH grows
/// at its milestone; WHERE CURRENT OF is folded into `whereClause` by the grammar.
pub fn make_update_stmt(
    relation: RangeVar,
    target_list: Vec<Node>,
    from_clause: Vec<Node>,
    where_clause: Option<Node>,
    returning: Vec<Node>,
) -> Node {
    Node::UpdateStmt(Box::new(UpdateStmt {
        relation: Some(Box::new(relation)),
        targetList: target_list,
        whereClause: where_clause,
        fromClause: from_clause,
        returningClause: make_returning_clause(returning),
        withClause: None,
    }))
}

/// gram.y `DeleteStmt`: build the raw `DeleteStmt` node (M8 plain form).
pub fn make_delete_stmt(
    relation: RangeVar,
    using_clause: Vec<Node>,
    where_clause: Option<Node>,
    returning: Vec<Node>,
) -> Node {
    Node::DeleteStmt(Box::new(DeleteStmt {
        relation: Some(Box::new(relation)),
        usingClause: using_clause,
        whereClause: where_clause,
        returningClause: make_returning_clause(returning),
        withClause: None,
    }))
}

/// gram.y `MergeStmt`: build the raw `MergeStmt` node (M8 basic form).
pub fn make_merge_stmt(
    relation: RangeVar,
    source_relation: Node,
    join_condition: Node,
    when_clauses: Vec<Node>,
    returning: Vec<Node>,
) -> Node {
    Node::MergeStmt(Box::new(MergeStmt {
        relation: Some(Box::new(relation)),
        sourceRelation: Some(source_relation),
        joinCondition: Some(join_condition),
        mergeWhenClauses: when_clauses,
        returningClause: make_returning_clause(returning),
        withClause: None,
    }))
}

/// gram.y `merge_when_clause`: build a `MergeWhenClause`. `target_list` is the SET
/// list (UPDATE) and `values` the VALUES list (INSERT); both empty for DELETE / DO
/// NOTHING. The match condition (WHEN MATCHED AND ...) grows later.
pub fn make_merge_when(
    match_kind: MergeMatchKind,
    command_type: CmdType,
    target_list: Vec<Node>,
    values: Vec<Node>,
) -> Node {
    Node::MergeWhenClause(Box::new(MergeWhenClause {
        matchKind: match_kind,
        commandType: command_type,
        r#override: OverridingKind::NOT_SET,
        condition: None,
        targetList: target_list,
        values,
    }))
}

/// gram.y `for_locking_item`: build a `LockingClause` (the locked-rel list is a
/// Vec of RangeVar nodes; empty means "all FROM rels").
pub fn make_locking_clause(
    strength: LockClauseStrength,
    locked_rels: Vec<Node>,
    wait_policy: LockWaitPolicy,
) -> LockingClause {
    LockingClause { lockedRels: locked_rels, strength, waitPolicy: wait_policy }
}

/// gram.y `select_no_parens: ... for_locking_clause`: stamp the FOR-locking clause
/// onto a SelectStmt (PG appends to `lockingClause`).
pub fn set_select_locking(stmt: Node, lock: LockingClause) -> Node {
    let Node::SelectStmt(mut sel) = stmt else {
        unreachable!("set_select_locking: not a SelectStmt");
    };
    sel.lockingClause.push(Node::LockingClause(Box::new(lock)));
    Node::SelectStmt(sel)
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

// --- M9 (step 36): transaction-control + SET/SHOW/RESET node builders ---------

/// gram.y `TransactionStmt`/`TransactionStmtLegacy` actions.
pub fn make_transaction_stmt(
    kind: TransactionStmtKind,
    options: Vec<Node>,
    savepoint_name: Option<String>,
    gid: Option<String>,
    chain: bool,
) -> Node {
    Node::TransactionStmt(Box::new(TransactionStmt {
        kind,
        options,
        savepoint_name,
        gid,
        chain,
        location: -1,
    }))
}

/// gram.y `transaction_mode_item` -> makeDefElem(name, makeStringConst(value)).
pub fn make_def_elem_str(name: &str, value: &str) -> Node {
    let de =
        crate::nodes::makefuncs::makeDefElem(name, Some(make_string_const(value.to_string())), -1);
    Node::DefElem(Box::new(de))
}

/// gram.y `transaction_mode_item` -> makeDefElem(name, makeIntConst(bool)).
pub fn make_def_elem_bool(name: &str, value: bool) -> Node {
    let de = crate::nodes::makefuncs::makeDefElem(name, Some(make_int_const(i32::from(value))), -1);
    Node::DefElem(Box::new(de))
}

/// Build a VariableSetStmt node from its parts.
fn variable_set_stmt(kind: VariableSetKind, name: Option<String>, args: Vec<Node>) -> Node {
    Node::VariableSetStmt(Box::new(VariableSetStmt {
        kind,
        name,
        args,
        jumble_args: false,
        is_local: false,
        location: -1,
    }))
}

/// gram.y `VariableSetStmt`: stamp `is_local` onto a `set_rest` result.
pub fn set_variable_local(stmt: Node, is_local: bool) -> Node {
    let Node::VariableSetStmt(mut n) = stmt else {
        unreachable!("set_rest yields a VariableSetStmt");
    };
    n.is_local = is_local;
    Node::VariableSetStmt(n)
}

/// gram.y `generic_set`: SET var {TO|=} value-list.
pub fn make_set_value(name: String, args: Vec<Node>) -> Node {
    variable_set_stmt(VariableSetKind::SET_VALUE, Some(name), args)
}

/// gram.y `generic_set`: SET var {TO|=} DEFAULT.
pub fn make_set_default(name: String) -> Node {
    variable_set_stmt(VariableSetKind::SET_DEFAULT, Some(name), Vec::new())
}

/// gram.y `set_rest_more`: SET var FROM CURRENT.
pub fn make_set_current(name: String) -> Node {
    variable_set_stmt(VariableSetKind::SET_CURRENT, Some(name), Vec::new())
}

/// gram.y `set_rest`: SET TRANSACTION / SESSION CHARACTERISTICS (VAR_SET_MULTI).
pub fn make_set_multi(name: &str, args: Vec<Node>) -> Node {
    let mut n = variable_set_stmt(VariableSetKind::SET_MULTI, Some(name.to_string()), args);
    if let Node::VariableSetStmt(s) = &mut n {
        s.jumble_args = true;
    }
    n
}

/// gram.y `set_rest_more`: SET TIME ZONE value (-> "timezone", or DEFAULT if empty).
pub fn make_set_timezone(value: Option<String>) -> Node {
    let Some(v) = value else {
        return make_set_default("timezone".to_string());
    };
    let mut n = variable_set_stmt(VariableSetKind::SET_VALUE, Some("timezone".to_string()), vec![
        make_string_const(v),
    ]);
    if let Node::VariableSetStmt(s) = &mut n {
        s.jumble_args = true;
    }
    n
}

/// gram.y `set_rest_more`: SET TRANSACTION SNAPSHOT 'id' (VAR_SET_MULTI). The
/// snapshot-import execution itself is staged (guc_funcs.rs).
pub fn make_set_transaction_snapshot(id: String) -> Node {
    variable_set_stmt(VariableSetKind::SET_MULTI, Some("TRANSACTION SNAPSHOT".to_string()), vec![
        make_string_const(id),
    ])
}

/// gram.y `reset_rest`/`generic_reset`: RESET var.
pub fn make_reset(name: String) -> Node {
    variable_set_stmt(VariableSetKind::RESET, Some(name), Vec::new())
}

/// gram.y `reset_rest`: RESET ALL.
pub fn make_reset_all() -> Node {
    variable_set_stmt(VariableSetKind::RESET_ALL, None, Vec::new())
}

/// gram.y `VariableShowStmt`.
pub fn make_variable_show(name: String) -> Node {
    Node::VariableShowStmt(Box::new(VariableShowStmt { name: Some(name) }))
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

// ===========================================================================
//  M10 (step 38): ALTER TABLE / RENAME / DROP / GRANT / COMMENT node builders.
// ===========================================================================

use crate::nodes::parsenodes::{
    AlterTableCmd, AlterTableStmt, AlterTableType, CommentStmt, Constraint, ConstrType, DropBehavior,
    DropStmt, GrantStmt, GrantTargetType, ObjectType, RenameStmt,
};

/// gram.y `opt_drop_behavior` token (CASCADE / RESTRICT). Decoupled from
/// parsenodes' `DropBehavior` so the grammar imports a small local enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehaviorTok {
    Cascade,
    Restrict,
}

impl DropBehaviorTok {
    fn to_node(self) -> DropBehavior {
        match self {
            Self::Cascade => DropBehavior::CASCADE,
            Self::Restrict => DropBehavior::RESTRICT,
        }
    }
}

/// gram.y object-type token for the M10-reachable DROP / RENAME object kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjType {
    Table,
    Index,
    View,
    Sequence,
    Type,
    Schema,
}

impl ObjType {
    fn to_object_type(self) -> ObjectType {
        match self {
            Self::Table => ObjectType::TABLE,
            Self::Index => ObjectType::INDEX,
            Self::View => ObjectType::VIEW,
            Self::Sequence => ObjectType::SEQUENCE,
            Self::Type => ObjectType::TYPE,
            Self::Schema => ObjectType::SCHEMA,
        }
    }
}

fn alter_table_cmd(
    subtype: AlterTableType,
    name: Option<String>,
    def: Option<Node>,
    behavior: DropBehavior,
    missing_ok: bool,
) -> Node {
    Node::AlterTableCmd(Box::new(AlterTableCmd {
        subtype,
        name,
        num: 0,
        newowner: None,
        def,
        behavior,
        missing_ok,
        recurse: false,
    }))
}

/// gram.y `AlterTableStmt: ALTER TABLE relation_expr alter_table_cmds`.
pub fn make_alter_table_stmt(relation: RangeVar, cmds: Vec<Node>, missing_ok: bool) -> Node {
    Node::AlterTableStmt(Box::new(AlterTableStmt {
        relation: Some(Box::new(relation)),
        cmds,
        objtype: ObjectType::TABLE,
        missing_ok,
    }))
}

/// gram.y `ADD [COLUMN] columnDef` -> AT_AddColumn.
pub fn make_at_add_column(coldef: Node) -> Node {
    alter_table_cmd(AlterTableType::AddColumn, None, Some(coldef), DropBehavior::RESTRICT, false)
}

/// gram.y `DROP [COLUMN] [IF EXISTS] name opt_drop_behavior` -> AT_DropColumn.
pub fn make_at_drop_column(name: String, missing_ok: bool, behavior: DropBehaviorTok) -> Node {
    alter_table_cmd(AlterTableType::DropColumn, Some(name), None, behavior.to_node(), missing_ok)
}

/// gram.y `ALTER [COLUMN] name SET DEFAULT expr | DROP DEFAULT` -> AT_ColumnDefault.
pub fn make_at_column_default(name: String, expr: Option<Node>) -> Node {
    alter_table_cmd(AlterTableType::ColumnDefault, Some(name), expr, DropBehavior::RESTRICT, false)
}

/// gram.y `ALTER [COLUMN] name SET|DROP NOT NULL` -> AT_SetNotNull / AT_DropNotNull.
pub fn make_at_set_not_null(name: String, set: bool) -> Node {
    let subtype = if set { AlterTableType::SetNotNull } else { AlterTableType::DropNotNull };
    alter_table_cmd(subtype, Some(name), None, DropBehavior::RESTRICT, false)
}

/// gram.y `ADD TableConstraint` -> AT_AddConstraint.
pub fn make_at_add_constraint(constraint: Node) -> Node {
    alter_table_cmd(AlterTableType::AddConstraint, None, Some(constraint), DropBehavior::RESTRICT, false)
}

/// gram.y `DROP CONSTRAINT [IF EXISTS] name opt_drop_behavior` -> AT_DropConstraint.
pub fn make_at_drop_constraint(name: String, missing_ok: bool, behavior: DropBehaviorTok) -> Node {
    alter_table_cmd(AlterTableType::DropConstraint, Some(name), None, behavior.to_node(), missing_ok)
}

/// gram.y `ConstraintElem: CHECK '(' a_expr ')'` (the minimal table constraint).
pub fn make_check_constraint(conname: Option<String>, expr: Node) -> Node {
    Node::Constraint(Box::new(Constraint {
        contype: ConstrType::CHECK,
        conname,
        deferrable: false,
        initdeferred: false,
        is_enforced: true,
        skip_validation: false,
        initially_valid: true,
        is_no_inherit: false,
        raw_expr: Some(expr),
        cooked_expr: None,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys: Vec::new(),
        without_overlaps: false,
        including: Vec::new(),
        exclusions: Vec::new(),
        options: Vec::new(),
        indexname: None,
        indexspace: None,
        reset_default_tblspc: false,
        access_method: None,
        where_clause: None,
        pktable: None,
        fk_attrs: Vec::new(),
        pk_attrs: Vec::new(),
        fk_with_period: false,
        pk_with_period: false,
        fk_matchtype: 0,
        fk_upd_action: 0,
        fk_del_action: 0,
        fk_del_set_cols: Vec::new(),
        old_conpfeqop: Vec::new(),
        old_pktable_oid: crate::postgres_ext::InvalidOid,
        location: -1,
    }))
}

/// gram.y `RenameStmt: ALTER TABLE relation RENAME TO name` (and the INDEX/VIEW
/// forms). renameType selects the catalog object kind.
pub fn make_rename_relation(objtype: ObjType, relation: RangeVar, newname: String, missing_ok: bool) -> Node {
    Node::RenameStmt(Box::new(RenameStmt {
        renameType: objtype.to_object_type(),
        relationType: objtype.to_object_type(),
        relation: Some(Box::new(relation)),
        object: None,
        subname: None,
        newname: Some(newname),
        behavior: DropBehavior::RESTRICT,
        missing_ok,
    }))
}

/// gram.y `RenameStmt: ALTER TABLE relation RENAME [COLUMN] col TO new`.
pub fn make_rename_column(
    objtype: ObjType,
    relation: RangeVar,
    col: String,
    newname: String,
    missing_ok: bool,
) -> Node {
    Node::RenameStmt(Box::new(RenameStmt {
        renameType: ObjectType::COLUMN,
        relationType: objtype.to_object_type(),
        relation: Some(Box::new(relation)),
        object: None,
        subname: Some(col),
        newname: Some(newname),
        behavior: DropBehavior::RESTRICT,
        missing_ok,
    }))
}

/// gram.y `any_name`: a (possibly schema-qualified) object name, carried as a
/// `RangeVar` node (the M10-reachable object kinds all reduce to a named relation
/// or schema-qualified object; PG carries a `List` of String, equivalent here).
pub fn make_any_name(mut parts: Vec<String>) -> Node {
    let (schema, name) = match parts.len() {
        0 => (None, None),
        1 => (None, Some(parts.remove(0))),
        _ => {
            let name = parts.pop();
            let schema = parts.pop();
            (schema, name)
        }
    };
    Node::RangeVar(Box::new(crate::nodes::makefuncs::makeRangeVar(schema, name, -1)))
}

/// gram.y `DropStmt`: the generic DROP. `objects` are the parsed names (RangeVars).
pub fn make_drop_stmt(objtype: ObjType, objects: Vec<Node>, missing_ok: bool, behavior: DropBehaviorTok) -> Node {
    Node::DropStmt(Box::new(DropStmt {
        objects,
        removeType: objtype.to_object_type(),
        behavior: behavior.to_node(),
        missing_ok,
        concurrent: false,
    }))
}

/// gram.y `GrantStmt` (M10 stub-parse): a minimal GrantStmt routed to
/// not_yet_reachable by ProcessUtility (step 39 builds the privilege machinery).
pub fn make_grant_stmt(is_grant: bool) -> Node {
    Node::GrantStmt(Box::new(GrantStmt {
        is_grant,
        targtype: GrantTargetType::OBJECT,
        objtype: ObjectType::TABLE,
        objects: Vec::new(),
        privileges: Vec::new(),
        grantees: Vec::new(),
        grant_option: false,
        grantor: None,
        behavior: DropBehavior::RESTRICT,
    }))
}

/// gram.y `CommentStmt` (M10 stub-parse): a minimal CommentStmt routed to
/// not_yet_reachable by ProcessUtility (step 39).
pub fn make_comment_stmt(comment: Option<String>) -> Node {
    Node::CommentStmt(Box::new(CommentStmt {
        objtype: ObjectType::TABLE,
        object: None,
        comment,
    }))
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

    /// Unwrap the single statement node out of a one-statement parse.
    fn one_stmt(list: &RawStmtVec) -> &Node {
        assert_eq!(list.len(), 1, "expected one statement");
        let Node::RawStmt(rs) = &list[0] else { panic!("not a RawStmt") };
        rs.stmt.as_ref().expect("RawStmt has a stmt")
    }

    #[test]
    fn update_set_where_returning_parses() {
        let list = parse("UPDATE t SET a = a + 1, b = 2 WHERE a > 0 RETURNING a, b");
        let Node::UpdateStmt(u) = one_stmt(&list) else { panic!("not an UpdateStmt") };
        assert_eq!(u.relation.as_ref().unwrap().relname.as_deref(), Some("t"));
        assert_eq!(u.targetList.len(), 2, "two SET items");
        let Node::ResTarget(rt0) = &u.targetList[0] else { panic!("SET item not a ResTarget") };
        assert_eq!(rt0.name.as_deref(), Some("a"));
        assert!(rt0.val.is_some(), "SET value present");
        assert!(u.whereClause.is_some(), "WHERE present");
        let ret = u.returningClause.as_ref().expect("RETURNING present");
        assert_eq!(ret.exprs.len(), 2, "RETURNING a, b");
    }

    #[test]
    fn delete_where_returning_star_parses() {
        let list = parse("DELETE FROM t WHERE a = 5 RETURNING *");
        let Node::DeleteStmt(d) = one_stmt(&list) else { panic!("not a DeleteStmt") };
        assert_eq!(d.relation.as_ref().unwrap().relname.as_deref(), Some("t"));
        assert!(d.whereClause.is_some(), "WHERE present");
        let ret = d.returningClause.as_ref().expect("RETURNING present");
        assert_eq!(ret.exprs.len(), 1, "RETURNING * is one star target");
    }

    #[test]
    fn select_for_update_parses_to_locking_clause() {
        let list = parse("SELECT a FROM t FOR UPDATE");
        let sel = one_select(&list);
        assert_eq!(sel.lockingClause.len(), 1, "one FOR-locking clause");
        let Node::LockingClause(lc) = &sel.lockingClause[0] else { panic!("not a LockingClause") };
        assert_eq!(lc.strength, crate::nodes::lockoptions::LockClauseStrength::FORUPDATE);
        assert_eq!(lc.waitPolicy, crate::nodes::lockoptions::LockWaitPolicy::LockWaitBlock);
        assert!(lc.lockedRels.is_empty(), "no OF list");
    }

    #[test]
    fn select_for_share_skip_locked_parses() {
        let list = parse("SELECT a FROM t FOR SHARE SKIP LOCKED");
        let sel = one_select(&list);
        let Node::LockingClause(lc) = &sel.lockingClause[0] else { panic!("not a LockingClause") };
        assert_eq!(lc.strength, crate::nodes::lockoptions::LockClauseStrength::FORSHARE);
        assert_eq!(lc.waitPolicy, crate::nodes::lockoptions::LockWaitPolicy::LockWaitSkip);
    }

    #[test]
    fn merge_basic_parses() {
        let list = parse(
            "MERGE INTO t USING s ON t.a = s.a \
             WHEN MATCHED THEN UPDATE SET b = s.b \
             WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)",
        );
        let Node::MergeStmt(m) = one_stmt(&list) else { panic!("not a MergeStmt") };
        assert_eq!(m.relation.as_ref().unwrap().relname.as_deref(), Some("t"));
        assert!(m.sourceRelation.is_some(), "USING source present");
        assert!(m.joinCondition.is_some(), "ON condition present");
        assert_eq!(m.mergeWhenClauses.len(), 2, "two WHEN clauses");
        let Node::MergeWhenClause(w0) = &m.mergeWhenClauses[0] else { panic!("not a MergeWhenClause") };
        assert_eq!(w0.commandType, CmdType::UPDATE);
        assert_eq!(w0.matchKind, MergeMatchKind::MATCHED);
        let Node::MergeWhenClause(w1) = &m.mergeWhenClauses[1] else { panic!("not a MergeWhenClause") };
        assert_eq!(w1.commandType, CmdType::INSERT);
        assert_eq!(w1.matchKind, MergeMatchKind::NOT_MATCHED_BY_TARGET);
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

    // --- M9: transaction control + SET/SHOW/RESET parsing --------------------

    fn one_tx(s: &str) -> TransactionStmt {
        let list = parse(s);
        let Node::TransactionStmt(t) = one_stmt(&list) else {
            panic!("not a TransactionStmt: {s}");
        };
        (**t).clone()
    }

    fn one_set(s: &str) -> VariableSetStmt {
        let list = parse(s);
        let Node::VariableSetStmt(v) = one_stmt(&list) else {
            panic!("not a VariableSetStmt: {s}");
        };
        (**v).clone()
    }

    #[test]
    fn transaction_stmt_kinds() {
        use TransactionStmtKind as K;
        assert_eq!(one_tx("BEGIN").kind, K::BEGIN);
        assert_eq!(one_tx("BEGIN WORK").kind, K::BEGIN);
        assert_eq!(one_tx("BEGIN TRANSACTION").kind, K::BEGIN);
        assert_eq!(one_tx("START TRANSACTION").kind, K::START);
        assert_eq!(one_tx("COMMIT").kind, K::COMMIT);
        assert_eq!(one_tx("END").kind, K::COMMIT);
        assert_eq!(one_tx("COMMIT WORK").kind, K::COMMIT);
        assert_eq!(one_tx("ROLLBACK").kind, K::ROLLBACK);
        assert_eq!(one_tx("ABORT").kind, K::ROLLBACK);

        let sp = one_tx("SAVEPOINT s");
        assert_eq!(sp.kind, K::SAVEPOINT);
        assert_eq!(sp.savepoint_name.as_deref(), Some("s"));

        assert_eq!(one_tx("RELEASE s").kind, K::RELEASE);
        assert_eq!(one_tx("RELEASE SAVEPOINT s").savepoint_name.as_deref(), Some("s"));
        assert_eq!(one_tx("ROLLBACK TO s").kind, K::ROLLBACK_TO);
        assert_eq!(one_tx("ROLLBACK TO SAVEPOINT s").savepoint_name.as_deref(), Some("s"));
    }

    #[test]
    fn transaction_chain_and_modes() {
        assert!(one_tx("COMMIT AND CHAIN").chain);
        assert!(!one_tx("COMMIT AND NO CHAIN").chain);
        assert!(!one_tx("COMMIT").chain);

        // BEGIN with a transaction_mode_list -> options DefElems.
        let b = one_tx("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY");
        assert_eq!(b.options.len(), 2, "two transaction modes");
        assert_eq!(one_tx("PREPARE TRANSACTION 'gid1'").gid.as_deref(), Some("gid1"));
    }

    #[test]
    fn variable_set_kinds() {
        use crate::nodes::parsenodes::VariableSetKind as K;
        let v = one_set("SET search_path = 'x'");
        assert_eq!(v.kind, K::SET_VALUE);
        assert_eq!(v.name.as_deref(), Some("search_path"));
        assert!(!v.is_local);

        assert_eq!(one_set("SET x TO 5").kind, K::SET_VALUE);
        assert_eq!(one_set("SET x TO DEFAULT").kind, K::SET_DEFAULT);
        assert_eq!(one_set("SET x = DEFAULT").kind, K::SET_DEFAULT);
        assert_eq!(one_set("SET x FROM CURRENT").kind, K::SET_CURRENT);
        assert!(one_set("SET LOCAL x = 1").is_local);
        assert!(!one_set("SET SESSION x = 1").is_local);
        assert_eq!(one_set("RESET x").kind, K::RESET);
        assert_eq!(one_set("RESET ALL").kind, K::RESET_ALL);

        // SET TRANSACTION -> VAR_SET_MULTI named "TRANSACTION".
        let m = one_set("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        assert_eq!(m.kind, K::SET_MULTI);
        assert_eq!(m.name.as_deref(), Some("TRANSACTION"));

        // SET TIME ZONE -> name "timezone".
        assert_eq!(one_set("SET TIME ZONE 'UTC'").name.as_deref(), Some("timezone"));
        assert_eq!(one_set("SET TIME ZONE DEFAULT").kind, K::SET_DEFAULT);
    }

    #[test]
    fn variable_show_names() {
        let names = |s: &str| {
            let list = parse(s);
            let Node::VariableShowStmt(v) = one_stmt(&list) else {
                panic!("not a VariableShowStmt: {s}");
            };
            v.name.clone().unwrap_or_default()
        };
        assert_eq!(names("SHOW search_path"), "search_path");
        assert_eq!(names("SHOW ALL"), "all");
        assert_eq!(names("SHOW TIME ZONE"), "timezone");
        assert_eq!(names("SHOW TRANSACTION ISOLATION LEVEL"), "transaction_isolation");
    }

    // --- M9 step 37: PREPARE/EXECUTE/DEALLOCATE/DECLARE/FETCH/MOVE/CLOSE + $n ---

    /// Search a node tree for any ParamRef and return its `number`.
    fn find_param(node: &Node) -> Option<i32> {
        match node {
            Node::ParamRef(p) => Some(p.number),
            Node::SelectStmt(s) => s.targetList.iter().find_map(find_param),
            Node::ResTarget(rt) => rt.val.as_ref().and_then(find_param),
            Node::A_Expr(a) => a
                .lexpr
                .as_ref()
                .and_then(find_param)
                .or_else(|| a.rexpr.as_ref().and_then(find_param)),
            _ => None,
        }
    }

    #[test]
    fn param_ref_in_select() {
        let list = parse("SELECT $1");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!("not a ResTarget") };
        let Some(Node::ParamRef(p)) = &rt.val else { panic!("target not a ParamRef") };
        assert_eq!(p.number, 1);
        assert_eq!(p.location, 7, "ParamRef carries the @L byte offset of $1");
    }

    #[test]
    fn prepare_with_argtypes_and_param() {
        let list = parse("PREPARE p (int) AS SELECT $1");
        let Node::PrepareStmt(s) = one_stmt(&list) else { panic!("not a PrepareStmt") };
        assert_eq!(s.name.as_deref(), Some("p"));
        assert_eq!(s.argtypes.len(), 1, "one declared argtype");
        assert!(matches!(&s.argtypes[0], Node::TypeName(_)), "argtype is a TypeName node");
        let q = s.query.as_ref().expect("query present");
        assert_eq!(find_param(q), Some(1), "query body contains $1");
    }

    #[test]
    fn prepare_no_argtypes() {
        let list = parse("PREPARE p AS SELECT 1");
        let Node::PrepareStmt(s) = one_stmt(&list) else { panic!("not a PrepareStmt") };
        assert_eq!(s.name.as_deref(), Some("p"));
        assert!(s.argtypes.is_empty(), "no argtypes");
    }

    #[test]
    fn prepare_multi_argtypes() {
        let list = parse("PREPARE p (int, text) AS SELECT $1, $2");
        let Node::PrepareStmt(s) = one_stmt(&list) else { panic!("not a PrepareStmt") };
        assert_eq!(s.argtypes.len(), 2);
    }

    #[test]
    fn execute_with_and_without_params() {
        let list = parse("EXECUTE p (1)");
        let Node::ExecuteStmt(s) = one_stmt(&list) else { panic!("not an ExecuteStmt") };
        assert_eq!(s.name.as_deref(), Some("p"));
        assert_eq!(s.params.len(), 1);

        let list = parse("EXECUTE p");
        let Node::ExecuteStmt(s) = one_stmt(&list) else { panic!("not an ExecuteStmt") };
        assert!(s.params.is_empty(), "no param clause");
    }

    #[test]
    fn deallocate_forms() {
        let list = parse("DEALLOCATE p");
        let Node::DeallocateStmt(s) = one_stmt(&list) else { panic!("not a DeallocateStmt") };
        assert_eq!(s.name.as_deref(), Some("p"));
        assert!(!s.isall);

        let list = parse("DEALLOCATE PREPARE p");
        let Node::DeallocateStmt(s) = one_stmt(&list) else { panic!("not a DeallocateStmt") };
        assert_eq!(s.name.as_deref(), Some("p"));
        assert!(!s.isall);

        let list = parse("DEALLOCATE ALL");
        let Node::DeallocateStmt(s) = one_stmt(&list) else { panic!("not a DeallocateStmt") };
        assert!(s.name.is_none());
        assert!(s.isall);
        assert_eq!(s.location, -1);

        let list = parse("DEALLOCATE PREPARE ALL");
        let Node::DeallocateStmt(s) = one_stmt(&list) else { panic!("not a DeallocateStmt") };
        assert!(s.isall);
    }

    #[test]
    fn declare_cursor_options_and_hold() {
        use crate::nodes::parsenodes::CursorOptions as C;
        let list = parse("DECLARE c CURSOR FOR SELECT 1");
        let Node::DeclareCursorStmt(s) = one_stmt(&list) else { panic!("not a DeclareCursorStmt") };
        assert_eq!(s.portalname.as_deref(), Some("c"));
        // PG always folds in FAST_PLAN.
        assert!(C::from_bits_truncate(s.options).contains(C::FAST_PLAN));
        assert!(s.query.is_some());

        let list = parse("DECLARE c SCROLL CURSOR WITH HOLD FOR SELECT 1");
        let Node::DeclareCursorStmt(s) = one_stmt(&list) else { panic!("not a DeclareCursorStmt") };
        let opts = C::from_bits_truncate(s.options);
        assert!(opts.contains(C::SCROLL));
        assert!(opts.contains(C::HOLD));
        assert!(opts.contains(C::FAST_PLAN));

        let list = parse("DECLARE c BINARY INSENSITIVE NO SCROLL CURSOR WITHOUT HOLD FOR SELECT 1");
        let Node::DeclareCursorStmt(s) = one_stmt(&list) else { panic!("not a DeclareCursorStmt") };
        let opts = C::from_bits_truncate(s.options);
        assert!(opts.contains(C::BINARY));
        assert!(opts.contains(C::INSENSITIVE));
        assert!(opts.contains(C::NO_SCROLL));
        assert!(!opts.contains(C::HOLD), "WITHOUT HOLD does not set HOLD");
    }

    #[test]
    fn close_portal_forms() {
        let list = parse("CLOSE c");
        let Node::ClosePortalStmt(s) = one_stmt(&list) else { panic!("not a ClosePortalStmt") };
        assert_eq!(s.portalname.as_deref(), Some("c"));

        let list = parse("CLOSE ALL");
        let Node::ClosePortalStmt(s) = one_stmt(&list) else { panic!("not a ClosePortalStmt") };
        assert!(s.portalname.is_none(), "CLOSE ALL has no portalname");
    }

    use crate::nodes::parsenodes::FETCH_ALL;

    fn one_fetch(s: &str) -> crate::nodes::parsenodes::FetchStmt {
        let list = parse(s);
        let Node::FetchStmt(f) = one_stmt(&list) else { panic!("not a FetchStmt: {s}") };
        (**f).clone()
    }

    #[test]
    fn fetch_and_move_forms() {
        use crate::nodes::parsenodes::FetchDirection as D;

        let f = one_fetch("FETCH c");
        assert_eq!(f.direction, D::FORWARD);
        assert_eq!(f.howMany, 1);
        assert_eq!(f.portalname.as_deref(), Some("c"));
        assert!(!f.ismove);

        let f = one_fetch("FETCH 1 FROM c");
        assert_eq!(f.direction, D::FORWARD);
        assert_eq!(f.howMany, 1);
        assert_eq!(f.portalname.as_deref(), Some("c"));

        let f = one_fetch("FETCH ALL IN c");
        assert_eq!(f.direction, D::FORWARD);
        assert_eq!(f.howMany, FETCH_ALL);

        let f = one_fetch("FETCH NEXT c");
        assert_eq!(f.direction, D::FORWARD);
        assert_eq!(f.howMany, 1);

        let f = one_fetch("FETCH PRIOR FROM c");
        assert_eq!(f.direction, D::BACKWARD);

        let f = one_fetch("FETCH FIRST c");
        assert_eq!(f.direction, D::ABSOLUTE);
        assert_eq!(f.howMany, 1);

        let f = one_fetch("FETCH LAST c");
        assert_eq!(f.direction, D::ABSOLUTE);
        assert_eq!(f.howMany, -1);

        let f = one_fetch("FETCH ABSOLUTE 5 c");
        assert_eq!(f.direction, D::ABSOLUTE);
        assert_eq!(f.howMany, 5);

        let f = one_fetch("FETCH RELATIVE 3 FROM c");
        assert_eq!(f.direction, D::RELATIVE);
        assert_eq!(f.howMany, 3);

        let f = one_fetch("FETCH FORWARD 2 c");
        assert_eq!(f.direction, D::FORWARD);
        assert_eq!(f.howMany, 2);

        let f = one_fetch("FETCH FORWARD ALL c");
        assert_eq!(f.howMany, FETCH_ALL);

        let f = one_fetch("FETCH BACKWARD c");
        assert_eq!(f.direction, D::BACKWARD);
        assert_eq!(f.howMany, 1);

        let f = one_fetch("FETCH BACKWARD ALL c");
        assert_eq!(f.direction, D::BACKWARD);
        assert_eq!(f.howMany, FETCH_ALL);

        // MOVE flips ismove.
        let f = one_fetch("MOVE FORWARD c");
        assert_eq!(f.direction, D::FORWARD);
        assert!(f.ismove);
    }
}
