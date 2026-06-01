//! Translation of postgres/src/include/nodes/makefuncs.h
//!                + postgres/src/backend/nodes/makefuncs.c
//!
//! Creator functions for various nodes.  The functions here are for the
//! most frequently created nodes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * Functions that build or dereference nodes are `pub unsafe fn`, matching the
//!   raw-pointer node model used throughout the port.
//!
//! * A handful of well-known, hardwired catalog type OIDs (BOOLOID, RECORDOID)
//!   are defined here as `const`s with `// TODO(pg-port)` markers; these stable
//!   numbers live in the generated catalog/pg_type_d.h in C.  Likewise
//!   InvalidAttrNumber (access/attnum.h) and RELPERSISTENCE_PERMANENT
//!   (catalog/pg_class.h) are defined locally until those headers are ported.
//!
//! * Things that require a *real* catalog or expression-walker lookup are not
//!   yet translated as their own units, so they are declared here as private
//!   stubs that `unimplemented!()` with a `// TODO(pg-port)` note, while the
//!   surrounding constructor logic is translated faithfully:
//!     - exprType/exprTypmod/exprCollation     (nodes/nodeFuncs.c)
//!     - get_rel_type_id/get_rel_name/type_is_rowtype/get_typlenbyval
//!                                              (utils/cache/lsyscache.c)
//!
//! * ereport(ERROR, (errcode(...), errmsg(...))) maps onto the port's
//!   `ereport!(ERROR, errmsg!(...))` shim; the errcode classification is
//!   evaluated for its side-effect-free value via `let _ = errcode(...);`.

use crate::prelude::*; // Datum, Oid, int*, bool, palloc, pstrdup, etc.
use crate::nodes::nodes::{Node, NodeTag};
// CoercionForm comes from primnodes::* (glob-imported below).
use crate::nodes::nodes::T_String; // NodeTag variant used as a value in makeStringConst
use crate::nodes::pg_list::*; // List, lappend, list_make1, NIL, lfirst, etc.
use crate::nodes::primnodes::*; // Var, Const, FuncExpr, OpExpr, Alias, RelabelType, ...
use crate::nodes::parsenodes::*; // A_Expr, ColumnRef, DefElem, RangeVar, GroupingSet, VacuumRelation, TypeName, ...
use crate::nodes::value::*; // Integer/Float/String/Boolean + makeInteger/makeString/...
use crate::nodes::execnodes::IndexInfo; // IndexInfo (execnodes.h)
use crate::{makeNode, IsA, castNode}; // node-construction + IsA/castNode macros
use crate::{list_make1, list_make2}; // #[macro_export] list builders live at crate root
use core::ffi::{c_char, c_int};
// NB: ereport!/errmsg!/Assert! macros, errcode(), and ERROR all arrive via
// `crate::prelude::*` above; PG_DETOAST_DATUM! lives at the crate root.

// ----------------------------------------------------------------
//   Hardwired catalog constants referenced by this file.
//
//   These OIDs/values are stable and hardcoded throughout PostgreSQL; the
//   real definitions live in generated/static catalog headers that are not
//   yet part of the port.
// ----------------------------------------------------------------

/// pg_type OID of `bool`.
// TODO(pg-port): catalog/pg_type_d.h
pub const BOOLOID: Oid = 16;

/// pg_type OID of the pseudo-type `record`.
// TODO(pg-port): catalog/pg_type_d.h
pub const RECORDOID: Oid = 2249;

/// `InvalidAttrNumber` (zero attribute number => whole-row reference).
// TODO(pg-port): access/attnum.h
pub const InvalidAttrNumber: AttrNumber = 0;

/// `RELPERSISTENCE_PERMANENT` ('p' => regular table).
// TODO(pg-port): catalog/pg_class.h
pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;

// ----------------------------------------------------------------
//   Stubs for not-yet-translated helpers from other compilation units.
// ----------------------------------------------------------------

/// `exprType(expr)` from nodes/nodeFuncs.c: get the type OID of an expression.
///
/// # Safety
/// `expr` must be a valid node pointer (or NULL).
#[inline]
unsafe fn exprType(_expr: *const Node) -> Oid {
    // TODO(pg-port): nodes/nodeFuncs.c
    unimplemented!("exprType: nodeFuncs not yet translated")
}

/// `exprTypmod(expr)` from nodes/nodeFuncs.c: get the typmod of an expression.
///
/// # Safety
/// `expr` must be a valid node pointer (or NULL).
#[inline]
unsafe fn exprTypmod(_expr: *const Node) -> int32 {
    // TODO(pg-port): nodes/nodeFuncs.c
    unimplemented!("exprTypmod: nodeFuncs not yet translated")
}

/// `exprCollation(expr)` from nodes/nodeFuncs.c: get the collation OID.
///
/// # Safety
/// `expr` must be a valid node pointer (or NULL).
#[inline]
unsafe fn exprCollation(_expr: *const Node) -> Oid {
    // TODO(pg-port): nodes/nodeFuncs.c
    unimplemented!("exprCollation: nodeFuncs not yet translated")
}

/// `get_rel_type_id(relid)` from utils/cache/lsyscache.c.
#[inline]
unsafe fn get_rel_type_id(_relid: Oid) -> Oid {
    // TODO(pg-port): utils/cache/lsyscache.c
    unimplemented!("get_rel_type_id: lsyscache not yet translated")
}

/// `get_rel_name(relid)` from utils/cache/lsyscache.c.
#[inline]
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    // TODO(pg-port): utils/cache/lsyscache.c
    unimplemented!("get_rel_name: lsyscache not yet translated")
}

/// `type_is_rowtype(typid)` from utils/cache/lsyscache.c.
#[inline]
unsafe fn type_is_rowtype(_typid: Oid) -> bool {
    // TODO(pg-port): utils/cache/lsyscache.c
    unimplemented!("type_is_rowtype: lsyscache not yet translated")
}

/// `get_typlenbyval(typid, &typlen, &typbyval)` from utils/cache/lsyscache.c.
#[inline]
unsafe fn get_typlenbyval(_typid: Oid, _typlen: *mut int16, _typbyval: *mut bool) {
    // TODO(pg-port): utils/cache/lsyscache.c
    unimplemented!("get_typlenbyval: lsyscache not yet translated")
}

/// `is_andclause(clause)` (inline in nodes/nodeFuncs.h).  Inlined here verbatim
/// because nodeFuncs is not yet translated as its own unit.
///
/// # Safety
/// `clause` must be NULL or a valid node pointer.
#[inline]
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == AND_EXPR
}

/// `ERRCODE_WRONG_OBJECT_TYPE` SQLSTATE class; used purely for classification.
// TODO(pg-port): utils/errcodes.h
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;

/*
 * makeA_Expr -
 *		makes an A_Expr node
 */
pub unsafe fn makeA_Expr(
    kind: A_Expr_Kind,
    name: *mut List,
    lexpr: *mut Node,
    rexpr: *mut Node,
    location: c_int,
) -> *mut A_Expr {
    let a: *mut A_Expr = makeNode!(A_Expr, T_A_Expr);

    (*a).kind = kind;
    (*a).name = name;
    (*a).lexpr = lexpr;
    (*a).rexpr = rexpr;
    (*a).location = location;
    a
}

/*
 * makeSimpleA_Expr -
 *		As above, given a simple (unqualified) operator name
 */
pub unsafe fn makeSimpleA_Expr(
    kind: A_Expr_Kind,
    name: *mut c_char,
    lexpr: *mut Node,
    rexpr: *mut Node,
    location: c_int,
) -> *mut A_Expr {
    let a: *mut A_Expr = makeNode!(A_Expr, T_A_Expr);

    (*a).kind = kind;
    (*a).name = list_make1!(makeString(name));
    (*a).lexpr = lexpr;
    (*a).rexpr = rexpr;
    (*a).location = location;
    a
}

/*
 * makeVar -
 *	  creates a Var node
 */
pub unsafe fn makeVar(
    varno: c_int,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: int32,
    varcollid: Oid,
    varlevelsup: Index,
) -> *mut Var {
    let var: *mut Var = makeNode!(Var, T_Var);

    (*var).varno = varno;
    (*var).varattno = varattno;
    (*var).vartype = vartype;
    (*var).vartypmod = vartypmod;
    (*var).varcollid = varcollid;
    (*var).varlevelsup = varlevelsup;

    /*
     * Only a few callers need to make Var nodes with varreturningtype
     * different from VAR_RETURNING_DEFAULT, non-null varnullingrels, or with
     * varnosyn/varattnosyn different from varno/varattno.  We don't provide
     * separate arguments for them, but just initialize them to sensible
     * default values.  This reduces code clutter and chance of error for most
     * callers.
     */
    (*var).varreturningtype = VAR_RETURNING_DEFAULT;
    (*var).varnullingrels = null_mut();
    (*var).varnosyn = varno as Index;
    (*var).varattnosyn = varattno;

    /* Likewise, we just set location to "unknown" here */
    (*var).location = -1;

    var
}

/*
 * makeVarFromTargetEntry -
 *		convenience function to create a same-level Var node from a
 *		TargetEntry
 */
pub unsafe fn makeVarFromTargetEntry(varno: c_int, tle: *mut TargetEntry) -> *mut Var {
    makeVar(
        varno,
        (*tle).resno,
        exprType((*tle).expr as *mut Node),
        exprTypmod((*tle).expr as *mut Node),
        exprCollation((*tle).expr as *mut Node),
        0,
    )
}

/*
 * makeWholeRowVar -
 *	  creates a Var node representing a whole row of the specified RTE
 *
 * A whole-row reference is a Var with varno set to the correct range
 * table entry, and varattno == 0 to signal that it references the whole
 * tuple.  (Use of zero here is unclean, since it could easily be confused
 * with error cases, but it's not worth changing now.)  The vartype indicates
 * a rowtype; either a named composite type, or a domain over a named
 * composite type (only possible if the RTE is a function returning that),
 * or RECORD.  This function encapsulates the logic for determining the
 * correct rowtype OID to use.
 *
 * If allowScalar is true, then for the case where the RTE is a single function
 * returning a non-composite result type, we produce a normal Var referencing
 * the function's result directly, instead of the single-column composite
 * value that the whole-row notation might otherwise suggest.
 */
pub unsafe fn makeWholeRowVar(
    rte: *mut RangeTblEntry,
    varno: c_int,
    varlevelsup: Index,
    allowScalar: bool,
) -> *mut Var {
    let result: *mut Var;
    let mut toid: Oid;
    let fexpr: *mut Node;

    match (*rte).rtekind {
        RTE_RELATION => {
            /* relation: the rowtype is a named composite type */
            toid = get_rel_type_id((*rte).relid);
            if !OidIsValid(toid) {
                let _ = errcode(ERRCODE_WRONG_OBJECT_TYPE);
                ereport!(
                    ERROR,
                    errmsg!(
                        "relation \"{:?}\" does not have a composite type",
                        get_rel_name((*rte).relid)
                    )
                );
            }
            result = makeVar(
                varno,
                InvalidAttrNumber,
                toid,
                -1,
                InvalidOid,
                varlevelsup,
            );
        }

        RTE_SUBQUERY => {
            /*
             * For a standard subquery, the Var should be of RECORD type.
             * However, if we're looking at a subquery that was expanded from
             * a view or SRF (only possible during planning), we must use the
             * appropriate rowtype, so that the resulting Var has the same
             * type that we would have produced from the original RTE.
             */
            if OidIsValid((*rte).relid) {
                /* Subquery was expanded from a view */
                toid = get_rel_type_id((*rte).relid);
                if !OidIsValid(toid) {
                    let _ = errcode(ERRCODE_WRONG_OBJECT_TYPE);
                    ereport!(
                        ERROR,
                        errmsg!(
                            "relation \"{:?}\" does not have a composite type",
                            get_rel_name((*rte).relid)
                        )
                    );
                }
            } else if !(*rte).functions.is_null() {
                /*
                 * Subquery was expanded from a set-returning function.  That
                 * would not have happened if there's more than one function
                 * or ordinality was requested.  We also needn't worry about
                 * the allowScalar case, since the planner doesn't use that.
                 * Otherwise this must match the RTE_FUNCTION code below.
                 */
                Assert!(!allowScalar);
                fexpr = (*(linitial((*rte).functions) as *mut RangeTblFunction)).funcexpr;
                toid = exprType(fexpr);
                if !type_is_rowtype(toid) {
                    toid = RECORDOID;
                }
            } else {
                /* Normal subquery-in-FROM */
                toid = RECORDOID;
            }
            result = makeVar(
                varno,
                InvalidAttrNumber,
                toid,
                -1,
                InvalidOid,
                varlevelsup,
            );
        }

        RTE_FUNCTION => {
            /*
             * If there's more than one function, or ordinality is requested,
             * force a RECORD result, since there's certainly more than one
             * column involved and it can't be a known named type.
             */
            if (*rte).funcordinality || list_length((*rte).functions) != 1 {
                /* always produces an anonymous RECORD result */
                result = makeVar(
                    varno,
                    InvalidAttrNumber,
                    RECORDOID,
                    -1,
                    InvalidOid,
                    varlevelsup,
                );
            } else {
                fexpr = (*(linitial((*rte).functions) as *mut RangeTblFunction)).funcexpr;
                toid = exprType(fexpr);
                if type_is_rowtype(toid) {
                    /* func returns composite; same as relation case */
                    result = makeVar(
                        varno,
                        InvalidAttrNumber,
                        toid,
                        -1,
                        InvalidOid,
                        varlevelsup,
                    );
                } else if allowScalar {
                    /* func returns scalar; just return its output as-is */
                    result = makeVar(varno, 1, toid, -1, exprCollation(fexpr), varlevelsup);
                } else {
                    /* func returns scalar, but we want a composite result */
                    result = makeVar(
                        varno,
                        InvalidAttrNumber,
                        RECORDOID,
                        -1,
                        InvalidOid,
                        varlevelsup,
                    );
                }
            }
        }

        _ => {
            /*
             * RTE is a join, tablefunc, VALUES, CTE, etc.  We represent these
             * cases as a whole-row Var of RECORD type.  (Note that in most
             * cases the Var will be expanded to a RowExpr during planning,
             * but that is not our concern here.)
             */
            result = makeVar(
                varno,
                InvalidAttrNumber,
                RECORDOID,
                -1,
                InvalidOid,
                varlevelsup,
            );
        }
    }

    result
}

/*
 * makeTargetEntry -
 *	  creates a TargetEntry node
 */
pub unsafe fn makeTargetEntry(
    expr: *mut Expr,
    resno: AttrNumber,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    let tle: *mut TargetEntry = makeNode!(TargetEntry, T_TargetEntry);

    (*tle).expr = expr;
    (*tle).resno = resno;
    (*tle).resname = resname;

    /*
     * We always set these fields to 0. If the caller wants to change them he
     * must do so explicitly.  Few callers do that, so omitting these
     * arguments reduces the chance of error.
     */
    (*tle).ressortgroupref = 0;
    (*tle).resorigtbl = InvalidOid;
    (*tle).resorigcol = 0;

    (*tle).resjunk = resjunk;

    tle
}

/*
 * flatCopyTargetEntry -
 *	  duplicate a TargetEntry, but don't copy substructure
 *
 * This is commonly used when we just want to modify the resno or substitute
 * a new expression.
 */
pub unsafe fn flatCopyTargetEntry(src_tle: *mut TargetEntry) -> *mut TargetEntry {
    let tle: *mut TargetEntry = makeNode!(TargetEntry, T_TargetEntry);

    Assert!(IsA!(src_tle, T_TargetEntry));
    core::ptr::copy_nonoverlapping(src_tle, tle, 1);
    tle
}

/*
 * makeFromExpr -
 *	  creates a FromExpr node
 */
pub unsafe fn makeFromExpr(fromlist: *mut List, quals: *mut Node) -> *mut FromExpr {
    let f: *mut FromExpr = makeNode!(FromExpr, T_FromExpr);

    (*f).fromlist = fromlist;
    (*f).quals = quals;
    f
}

/*
 * makeConst -
 *	  creates a Const node
 */
pub unsafe fn makeConst(
    consttype: Oid,
    consttypmod: int32,
    constcollid: Oid,
    constlen: c_int,
    mut constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
) -> *mut Const {
    let cnst: *mut Const = makeNode!(Const, T_Const);

    /*
     * If it's a varlena value, force it to be in non-expanded (non-toasted)
     * format; this avoids any possible dependency on external values and
     * improves consistency of representation, which is important for equal().
     */
    if !constisnull && constlen == -1 {
        constvalue = PointerGetDatum(crate::PG_DETOAST_DATUM!(constvalue) as *const c_void);
    }

    (*cnst).consttype = consttype;
    (*cnst).consttypmod = consttypmod;
    (*cnst).constcollid = constcollid;
    (*cnst).constlen = constlen;
    (*cnst).constvalue = constvalue;
    (*cnst).constisnull = constisnull;
    (*cnst).constbyval = constbyval;
    (*cnst).location = -1; /* "unknown" */

    cnst
}

/*
 * makeNullConst -
 *	  creates a Const node representing a NULL of the specified type/typmod
 *
 * This is a convenience routine that just saves a lookup of the type's
 * storage properties.
 */
pub unsafe fn makeNullConst(consttype: Oid, consttypmod: int32, constcollid: Oid) -> *mut Const {
    let mut typLen: int16 = 0;
    let mut typByVal: bool = false;

    get_typlenbyval(consttype, &mut typLen, &mut typByVal);
    makeConst(
        consttype,
        consttypmod,
        constcollid,
        typLen as c_int,
        0 as Datum,
        true,
        typByVal,
    )
}

/*
 * makeBoolConst -
 *	  creates a Const node representing a boolean value (can be NULL too)
 */
pub unsafe fn makeBoolConst(value: bool, isnull: bool) -> *mut Node {
    /* note that pg_type.h hardwires size of bool as 1 ... duplicate it */
    makeConst(
        BOOLOID,
        -1,
        InvalidOid,
        1,
        BoolGetDatum(value),
        isnull,
        true,
    ) as *mut Node
}

/*
 * makeBoolExpr -
 *	  creates a BoolExpr node
 */
pub unsafe fn makeBoolExpr(boolop: BoolExprType, args: *mut List, location: c_int) -> *mut Expr {
    let b: *mut BoolExpr = makeNode!(BoolExpr, T_BoolExpr);

    (*b).boolop = boolop;
    (*b).args = args;
    (*b).location = location;

    b as *mut Expr
}

/*
 * makeAlias -
 *	  creates an Alias node
 *
 * NOTE: the given name is copied, but the colnames list (if any) isn't.
 */
pub unsafe fn makeAlias(aliasname: *const c_char, colnames: *mut List) -> *mut Alias {
    let a: *mut Alias = makeNode!(Alias, T_Alias);

    (*a).aliasname = pstrdup(aliasname);
    (*a).colnames = colnames;

    a
}

/*
 * makeRelabelType -
 *	  creates a RelabelType node
 */
pub unsafe fn makeRelabelType(
    arg: *mut Expr,
    rtype: Oid,
    rtypmod: int32,
    rcollid: Oid,
    rformat: CoercionForm,
) -> *mut RelabelType {
    let r: *mut RelabelType = makeNode!(RelabelType, T_RelabelType);

    (*r).arg = arg;
    (*r).resulttype = rtype;
    (*r).resulttypmod = rtypmod;
    (*r).resultcollid = rcollid;
    (*r).relabelformat = rformat;
    (*r).location = -1;

    r
}

/*
 * makeRangeVar -
 *	  creates a RangeVar node (rather oversimplified case)
 */
pub unsafe fn makeRangeVar(
    schemaname: *mut c_char,
    relname: *mut c_char,
    location: c_int,
) -> *mut RangeVar {
    let r: *mut RangeVar = makeNode!(RangeVar, T_RangeVar);

    (*r).catalogname = null_mut();
    (*r).schemaname = schemaname;
    (*r).relname = relname;
    (*r).inh = true;
    (*r).relpersistence = RELPERSISTENCE_PERMANENT;
    (*r).alias = null_mut();
    (*r).location = location;

    r
}

/*
 * makeNotNullConstraint -
 *		creates a Constraint node for NOT NULL constraints
 */
pub unsafe fn makeNotNullConstraint(colname: *mut String) -> *mut Constraint {
    let notnull: *mut Constraint;

    notnull = makeNode!(Constraint, T_Constraint);
    (*notnull).contype = CONSTR_NOTNULL;
    (*notnull).conname = null_mut();
    (*notnull).is_no_inherit = false;
    (*notnull).deferrable = false;
    (*notnull).initdeferred = false;
    (*notnull).location = -1;
    (*notnull).keys = list_make1!(colname);
    (*notnull).is_enforced = true;
    (*notnull).skip_validation = false;
    (*notnull).initially_valid = true;

    notnull
}

/*
 * makeTypeName -
 *	build a TypeName node for an unqualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
pub unsafe fn makeTypeName(typnam: *mut c_char) -> *mut TypeName {
    makeTypeNameFromNameList(list_make1!(makeString(typnam)))
}

/*
 * makeTypeNameFromNameList -
 *	build a TypeName node for a String list representing a qualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
pub unsafe fn makeTypeNameFromNameList(names: *mut List) -> *mut TypeName {
    let n: *mut TypeName = makeNode!(TypeName, T_TypeName);

    (*n).names = names;
    (*n).typmods = NIL;
    (*n).typemod = -1;
    (*n).location = -1;
    n
}

/*
 * makeTypeNameFromOid -
 *	build a TypeName node to represent a type already known by OID/typmod.
 */
pub unsafe fn makeTypeNameFromOid(typeOid: Oid, typmod: int32) -> *mut TypeName {
    let n: *mut TypeName = makeNode!(TypeName, T_TypeName);

    (*n).typeOid = typeOid;
    (*n).typemod = typmod;
    (*n).location = -1;
    n
}

/*
 * makeColumnDef -
 *	build a ColumnDef node to represent a simple column definition.
 *
 * Type and collation are specified by OID.
 * Other properties are all basic to start with.
 */
pub unsafe fn makeColumnDef(
    colname: *const c_char,
    typeOid: Oid,
    typmod: int32,
    collOid: Oid,
) -> *mut ColumnDef {
    let n: *mut ColumnDef = makeNode!(ColumnDef, T_ColumnDef);

    (*n).colname = pstrdup(colname);
    (*n).typeName = makeTypeNameFromOid(typeOid, typmod);
    (*n).inhcount = 0;
    (*n).is_local = true;
    (*n).is_not_null = false;
    (*n).is_from_type = false;
    (*n).storage = 0;
    (*n).raw_default = null_mut();
    (*n).cooked_default = null_mut();
    (*n).collClause = null_mut();
    (*n).collOid = collOid;
    (*n).constraints = NIL;
    (*n).fdwoptions = NIL;
    (*n).location = -1;

    n
}

/*
 * makeFuncExpr -
 *	build an expression tree representing a function call.
 *
 * The argument expressions must have been transformed already.
 */
pub unsafe fn makeFuncExpr(
    funcid: Oid,
    rettype: Oid,
    args: *mut List,
    funccollid: Oid,
    inputcollid: Oid,
    fformat: CoercionForm,
) -> *mut FuncExpr {
    let funcexpr: *mut FuncExpr;

    funcexpr = makeNode!(FuncExpr, T_FuncExpr);
    (*funcexpr).funcid = funcid;
    (*funcexpr).funcresulttype = rettype;
    (*funcexpr).funcretset = false; /* only allowed case here */
    (*funcexpr).funcvariadic = false; /* only allowed case here */
    (*funcexpr).funcformat = fformat;
    (*funcexpr).funccollid = funccollid;
    (*funcexpr).inputcollid = inputcollid;
    (*funcexpr).args = args;
    (*funcexpr).location = -1;

    funcexpr
}

/*
 * makeStringConst -
 * 	build a A_Const node of type T_String for given string
 */
pub unsafe fn makeStringConst(str: *mut c_char, location: c_int) -> *mut Node {
    let n: *mut A_Const = makeNode!(A_Const, T_A_Const);

    // C: n->val.sval.type = T_String; n->val.sval.sval = str;
    // `val` is a union whose `sval` arm embeds a whole `String` value node, wrapped
    // in ManuallyDrop to satisfy Rust's union rules. ManuallyDrop<T> is repr(transparent),
    // so we reach the inner String value node through a transparent pointer cast.
    let sval: *mut crate::nodes::value::String = &mut (*n).val.sval as *mut _ as *mut _;
    (*sval).r#type = T_String;
    (*sval).sval = str;
    (*n).location = location;

    n as *mut Node
}

/*
 * makeDefElem -
 *	build a DefElem node
 *
 * This is sufficient for the "typical" case with an unqualified option name
 * and no special action.
 */
pub unsafe fn makeDefElem(name: *mut c_char, arg: *mut Node, location: c_int) -> *mut DefElem {
    let res: *mut DefElem = makeNode!(DefElem, T_DefElem);

    (*res).defnamespace = null_mut();
    (*res).defname = name;
    (*res).arg = arg;
    (*res).defaction = DEFELEM_UNSPEC;
    (*res).location = location;

    res
}

/*
 * makeDefElemExtended -
 *	build a DefElem node with all fields available to be specified
 */
pub unsafe fn makeDefElemExtended(
    nameSpace: *mut c_char,
    name: *mut c_char,
    arg: *mut Node,
    defaction: DefElemAction,
    location: c_int,
) -> *mut DefElem {
    let res: *mut DefElem = makeNode!(DefElem, T_DefElem);

    (*res).defnamespace = nameSpace;
    (*res).defname = name;
    (*res).arg = arg;
    (*res).defaction = defaction;
    (*res).location = location;

    res
}

/*
 * makeFuncCall -
 *
 * Initialize a FuncCall struct with the information every caller must
 * supply.  Any non-default parameters have to be inserted by the caller.
 */
pub unsafe fn makeFuncCall(
    name: *mut List,
    args: *mut List,
    funcformat: CoercionForm,
    location: c_int,
) -> *mut FuncCall {
    let n: *mut FuncCall = makeNode!(FuncCall, T_FuncCall);

    (*n).funcname = name;
    (*n).args = args;
    (*n).agg_order = NIL;
    (*n).agg_filter = null_mut();
    (*n).over = null_mut();
    (*n).agg_within_group = false;
    (*n).agg_star = false;
    (*n).agg_distinct = false;
    (*n).func_variadic = false;
    (*n).funcformat = funcformat;
    (*n).location = location;
    n
}

/*
 * make_opclause
 *	  Creates an operator clause given its operator info, left operand
 *	  and right operand (pass NULL to create single-operand clause),
 *	  and collation info.
 */
pub unsafe fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: *mut Expr,
    rightop: *mut Expr,
    opcollid: Oid,
    inputcollid: Oid,
) -> *mut Expr {
    let expr: *mut OpExpr = makeNode!(OpExpr, T_OpExpr);

    (*expr).opno = opno;
    (*expr).opfuncid = InvalidOid;
    (*expr).opresulttype = opresulttype;
    (*expr).opretset = opretset;
    (*expr).opcollid = opcollid;
    (*expr).inputcollid = inputcollid;
    if !rightop.is_null() {
        (*expr).args = list_make2!(leftop, rightop);
    } else {
        (*expr).args = list_make1!(leftop);
    }
    (*expr).location = -1;
    expr as *mut Expr
}

/*
 * make_andclause
 *
 * Creates an 'and' clause given a list of its subclauses.
 */
pub unsafe fn make_andclause(andclauses: *mut List) -> *mut Expr {
    let expr: *mut BoolExpr = makeNode!(BoolExpr, T_BoolExpr);

    (*expr).boolop = AND_EXPR;
    (*expr).args = andclauses;
    (*expr).location = -1;
    expr as *mut Expr
}

/*
 * make_orclause
 *
 * Creates an 'or' clause given a list of its subclauses.
 */
pub unsafe fn make_orclause(orclauses: *mut List) -> *mut Expr {
    let expr: *mut BoolExpr = makeNode!(BoolExpr, T_BoolExpr);

    (*expr).boolop = OR_EXPR;
    (*expr).args = orclauses;
    (*expr).location = -1;
    expr as *mut Expr
}

/*
 * make_notclause
 *
 * Create a 'not' clause given the expression to be negated.
 */
pub unsafe fn make_notclause(notclause: *mut Expr) -> *mut Expr {
    let expr: *mut BoolExpr = makeNode!(BoolExpr, T_BoolExpr);

    (*expr).boolop = NOT_EXPR;
    (*expr).args = list_make1!(notclause);
    (*expr).location = -1;
    expr as *mut Expr
}

/*
 * make_and_qual
 *
 * Variant of make_andclause for ANDing two qual conditions together.
 * Qual conditions have the property that a NULL nodetree is interpreted
 * as 'true'.
 *
 * NB: this makes no attempt to preserve AND/OR flatness; so it should not
 * be used on a qual that has already been run through prepqual.c.
 */
pub unsafe fn make_and_qual(qual1: *mut Node, qual2: *mut Node) -> *mut Node {
    if qual1.is_null() {
        return qual2;
    }
    if qual2.is_null() {
        return qual1;
    }
    make_andclause(list_make2!(qual1, qual2)) as *mut Node
}

/*
 * The planner and executor usually represent qualification expressions
 * as lists of boolean expressions with implicit AND semantics.
 *
 * These functions convert between an AND-semantics expression list and the
 * ordinary representation of a boolean expression.
 *
 * Note that an empty list is considered equivalent to TRUE.
 */
pub unsafe fn make_ands_explicit(andclauses: *mut List) -> *mut Expr {
    if andclauses == NIL {
        makeBoolConst(true, false) as *mut Expr
    } else if list_length(andclauses) == 1 {
        linitial(andclauses) as *mut Expr
    } else {
        make_andclause(andclauses)
    }
}

pub unsafe fn make_ands_implicit(clause: *mut Expr) -> *mut List {
    /*
     * NB: because the parser sets the qual field to NULL in a query that has
     * no WHERE clause, we must consider a NULL input clause as TRUE, even
     * though one might more reasonably think it FALSE.
     */
    if clause.is_null() {
        NIL /* NULL -> NIL list == TRUE */
    } else if is_andclause(clause as *const c_void) {
        (*(clause as *mut BoolExpr)).args
    } else if IsA!(clause, T_Const)
        && !(*(clause as *mut Const)).constisnull
        && DatumGetBool((*(clause as *mut Const)).constvalue)
    {
        NIL /* constant TRUE input -> NIL list */
    } else {
        list_make1!(clause)
    }
}

/*
 * makeIndexInfo
 *	  create an IndexInfo node
 */
pub unsafe fn makeIndexInfo(
    numattrs: c_int,
    numkeyattrs: c_int,
    amoid: Oid,
    expressions: *mut List,
    predicates: *mut List,
    unique: bool,
    nulls_not_distinct: bool,
    isready: bool,
    concurrent: bool,
    summarizing: bool,
    withoutoverlaps: bool,
) -> *mut IndexInfo {
    let n: *mut IndexInfo = makeNode!(IndexInfo, T_IndexInfo);

    (*n).ii_NumIndexAttrs = numattrs;
    (*n).ii_NumIndexKeyAttrs = numkeyattrs;
    Assert!((*n).ii_NumIndexKeyAttrs != 0);
    Assert!((*n).ii_NumIndexKeyAttrs <= (*n).ii_NumIndexAttrs);
    (*n).ii_Unique = unique;
    (*n).ii_NullsNotDistinct = nulls_not_distinct;
    (*n).ii_ReadyForInserts = isready;
    (*n).ii_CheckedUnchanged = false;
    (*n).ii_IndexUnchanged = false;
    (*n).ii_Concurrent = concurrent;
    (*n).ii_Summarizing = summarizing;
    (*n).ii_WithoutOverlaps = withoutoverlaps;

    /* summarizing indexes cannot contain non-key attributes */
    Assert!(!summarizing || (numkeyattrs == numattrs));

    /* expressions */
    (*n).ii_Expressions = expressions;
    (*n).ii_ExpressionsState = NIL;

    /* predicates  */
    (*n).ii_Predicate = predicates;
    (*n).ii_PredicateState = null_mut();

    /* exclusion constraints */
    (*n).ii_ExclusionOps = null_mut();
    (*n).ii_ExclusionProcs = null_mut();
    (*n).ii_ExclusionStrats = null_mut();

    /* speculative inserts */
    (*n).ii_UniqueOps = null_mut();
    (*n).ii_UniqueProcs = null_mut();
    (*n).ii_UniqueStrats = null_mut();

    /* initialize index-build state to default */
    (*n).ii_BrokenHotChain = false;
    (*n).ii_ParallelWorkers = 0;

    /* set up for possible use by index AM */
    (*n).ii_Am = amoid;
    (*n).ii_AmCache = null_mut();
    (*n).ii_Context = CurrentMemoryContext;

    n
}

/*
 * makeGroupingSet
 *
 */
pub unsafe fn makeGroupingSet(
    kind: GroupingSetKind,
    content: *mut List,
    location: c_int,
) -> *mut GroupingSet {
    let n: *mut GroupingSet = makeNode!(GroupingSet, T_GroupingSet);

    (*n).kind = kind;
    (*n).content = content;
    (*n).location = location;
    n
}

/*
 * makeVacuumRelation -
 *	  create a VacuumRelation node
 */
pub unsafe fn makeVacuumRelation(
    relation: *mut RangeVar,
    oid: Oid,
    va_cols: *mut List,
) -> *mut VacuumRelation {
    let v: *mut VacuumRelation = makeNode!(VacuumRelation, T_VacuumRelation);

    (*v).relation = relation;
    (*v).oid = oid;
    (*v).va_cols = va_cols;
    v
}

/*
 * makeJsonFormat -
 *	  creates a JsonFormat node
 */
pub unsafe fn makeJsonFormat(
    type_: JsonFormatType,
    encoding: JsonEncoding,
    location: c_int,
) -> *mut JsonFormat {
    let jf: *mut JsonFormat = makeNode!(JsonFormat, T_JsonFormat);

    (*jf).format_type = type_;
    (*jf).encoding = encoding;
    (*jf).location = location;

    jf
}

/*
 * makeJsonValueExpr -
 *	  creates a JsonValueExpr node
 */
pub unsafe fn makeJsonValueExpr(
    raw_expr: *mut Expr,
    formatted_expr: *mut Expr,
    format: *mut JsonFormat,
) -> *mut JsonValueExpr {
    let jve: *mut JsonValueExpr = makeNode!(JsonValueExpr, T_JsonValueExpr);

    (*jve).raw_expr = raw_expr;
    (*jve).formatted_expr = formatted_expr;
    (*jve).format = format;

    jve
}

/*
 * makeJsonBehavior -
 *	  creates a JsonBehavior node
 */
pub unsafe fn makeJsonBehavior(
    btype: JsonBehaviorType,
    expr: *mut Node,
    location: c_int,
) -> *mut JsonBehavior {
    let behavior: *mut JsonBehavior = makeNode!(JsonBehavior, T_JsonBehavior);

    (*behavior).btype = btype;
    (*behavior).expr = expr;
    (*behavior).location = location;

    behavior
}

/*
 * makeJsonKeyValue -
 *	  creates a JsonKeyValue node
 */
pub unsafe fn makeJsonKeyValue(key: *mut Node, value: *mut Node) -> *mut Node {
    let n: *mut JsonKeyValue = makeNode!(JsonKeyValue, T_JsonKeyValue);

    (*n).key = key as *mut Expr;
    (*n).value = castNode!(JsonValueExpr, T_JsonValueExpr, value);

    n as *mut Node
}

/*
 * makeJsonIsPredicate -
 *	  creates a JsonIsPredicate node
 */
pub unsafe fn makeJsonIsPredicate(
    expr: *mut Node,
    format: *mut JsonFormat,
    item_type: JsonValueType,
    unique_keys: bool,
    location: c_int,
) -> *mut Node {
    let n: *mut JsonIsPredicate = makeNode!(JsonIsPredicate, T_JsonIsPredicate);

    (*n).expr = expr;
    (*n).format = format;
    (*n).item_type = item_type;
    (*n).unique_keys = unique_keys;
    (*n).location = location;

    n as *mut Node
}

/*
 * makeJsonTablePathSpec -
 *		Make JsonTablePathSpec node from given path string and name (if any)
 */
pub unsafe fn makeJsonTablePathSpec(
    string: *mut c_char,
    name: *mut c_char,
    string_location: c_int,
    name_location: c_int,
) -> *mut JsonTablePathSpec {
    let pathspec: *mut JsonTablePathSpec = makeNode!(JsonTablePathSpec, T_JsonTablePathSpec);

    Assert!(!string.is_null());
    (*pathspec).string = makeStringConst(string, string_location);
    if !name.is_null() {
        (*pathspec).name = pstrdup(name);
    }

    (*pathspec).name_location = name_location;
    (*pathspec).location = string_location;

    pathspec
}

/*
 * makeJsonTablePath -
 *		Make JsonTablePath node for given path string and name
 */
pub unsafe fn makeJsonTablePath(pathvalue: *mut Const, pathname: *mut c_char) -> *mut JsonTablePath {
    let path: *mut JsonTablePath = makeNode!(JsonTablePath, T_JsonTablePath);

    Assert!(IsA!(pathvalue, T_Const));
    (*path).value = pathvalue;
    (*path).name = pathname;

    path
}
