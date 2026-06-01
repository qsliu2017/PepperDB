//! Translated from PostgreSQL 18.3 `src/include/nodes/primnodes.h`.
//!
//! Definitions for "primitive" node types, those that are used in more than one
//! of the parse/plan/execute stages of the query pipeline.  Currently, these
//! are mostly nodes for executable expressions and join trees.
//!
//! The copy/equal/out/read support functions are generated elsewhere; this file
//! holds only the node/enum definitions, verbatim from the C header.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*; // Oid, Datum, int*, bool, c_char/c_int/c_void, etc.
use crate::nodes::nodes::{Node, NodeTag, ParseLoc}; // ParseLoc, base Node
use crate::nodes::pg_list::List; // List* fields
use crate::nodes::bitmapset::Bitmapset; // Relids = *mut Bitmapset

// Cross-header types referenced by primnodes but defined in other backend
// headers (nodes.h provides CmdType/JoinType/AggSplit/OnConflictAction/Cost).
use crate::nodes::nodes::{AggSplit, CmdType, Cost, JoinType, OnConflictAction};

// ----------------------------------------------------------------
//  Forward stubs for cross-header types not yet translated.
// ----------------------------------------------------------------

/// TODO(pg-port): real def `typedef int16 AttrNumber` in access/attnum.h.
pub type AttrNumber = int16;

/// TODO(pg-port): real def `typedef enum CompareType` in access/cmptype.h.
/// Used by value in RowCompareExpr.cmptype; stubbed as the underlying int.
pub type CompareType = c_int;

// `Query` is defined in parsenodes.rs (the real node). IntoClause.viewQuery is a
// `*mut Query`; import the real type rather than restubbing it here, so a module
// that globs both primnodes and parsenodes doesn't see an ambiguous `Query`.
// (Rust resolves this cross-module `use` regardless of definition order.)
use crate::nodes::parsenodes::Query;

// ----------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OverridingKind {
    OVERRIDING_NOT_SET = 0,
    OVERRIDING_USER_VALUE,
    OVERRIDING_SYSTEM_VALUE,
}
pub use OverridingKind::*;

/* ----------------------------------------------------------------
 *						node definitions
 * ----------------------------------------------------------------
 */

/*
 * Alias -
 *	  specifies an alias for a range variable; the alias might also
 *	  specify renaming of columns within the table.
 *
 * Note: colnames is a list of String nodes.  In Alias structs
 * associated with RTEs, there may be entries corresponding to dropped
 * columns; these are normally empty strings ("").  See parsenodes.h for info.
 */
#[repr(C)]
pub struct Alias {
    pub r#type: NodeTag,
    pub aliasname: *mut c_char, /* aliased rel name (never qualified) */
    pub colnames: *mut List,    /* optional list of column aliases */
}

/* What to do at commit time for temporary relations */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnCommitAction {
    ONCOMMIT_NOOP,          /* No ON COMMIT clause (do nothing) */
    ONCOMMIT_PRESERVE_ROWS, /* ON COMMIT PRESERVE ROWS (do nothing) */
    ONCOMMIT_DELETE_ROWS,   /* ON COMMIT DELETE ROWS */
    ONCOMMIT_DROP,          /* ON COMMIT DROP */
}
pub use OnCommitAction::*;

/*
 * RangeVar - range variable, used in FROM clauses
 *
 * Also used to represent table names in utility statements; there, the alias
 * field is not used, and inh tells whether to apply the operation
 * recursively to child tables.  In some contexts it is also useful to carry
 * a TEMP table indication here.
 */
#[repr(C)]
pub struct RangeVar {
    pub r#type: NodeTag,

    /* the catalog (database) name, or NULL */
    pub catalogname: *mut c_char,

    /* the schema name, or NULL */
    pub schemaname: *mut c_char,

    /* the relation/sequence name */
    pub relname: *mut c_char,

    /* expand rel by inheritance? recursively act on children? */
    pub inh: bool,

    /* see RELPERSISTENCE_* in pg_class.h */
    pub relpersistence: c_char,

    /* table alias & optional column aliases */
    pub alias: *mut Alias,

    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TableFuncType {
    TFT_XMLTABLE,
    TFT_JSON_TABLE,
}
pub use TableFuncType::*;

/*
 * TableFunc - node for a table function, such as XMLTABLE and JSON_TABLE.
 *
 * Entries in the ns_names list are either String nodes containing
 * literal namespace names, or NULL pointers to represent DEFAULT.
 */
#[repr(C)]
pub struct TableFunc {
    pub r#type: NodeTag,
    /* XMLTABLE or JSON_TABLE */
    pub functype: TableFuncType,
    /* list of namespace URI expressions */
    pub ns_uris: *mut List, // pg_node_attr(query_jumble_ignore)
    /* list of namespace names or NULL */
    pub ns_names: *mut List, // pg_node_attr(query_jumble_ignore)
    /* input document expression */
    pub docexpr: *mut Node,
    /* row filter expression */
    pub rowexpr: *mut Node,
    /* column names (list of String) */
    pub colnames: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of column type OIDs */
    pub coltypes: *mut List, // pg_node_attr(query_jumble_ignore)
    /* integer list of column typmods */
    pub coltypmods: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of column collation OIDs */
    pub colcollations: *mut List, // pg_node_attr(query_jumble_ignore)
    /* list of column filter expressions */
    pub colexprs: *mut List,
    /* list of column default expressions */
    pub coldefexprs: *mut List, // pg_node_attr(query_jumble_ignore)
    /* JSON_TABLE: list of column value expressions */
    pub colvalexprs: *mut List, // pg_node_attr(query_jumble_ignore)
    /* JSON_TABLE: list of PASSING argument expressions */
    pub passingvalexprs: *mut List, // pg_node_attr(query_jumble_ignore)
    /* nullability flag for each output column */
    pub notnulls: *mut Bitmapset, // pg_node_attr(query_jumble_ignore)
    /* JSON_TABLE plan */
    pub plan: *mut Node, // pg_node_attr(query_jumble_ignore)
    /* counts from 0; -1 if none specified */
    pub ordinalitycol: c_int, // pg_node_attr(query_jumble_ignore)
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * IntoClause - target information for SELECT INTO, CREATE TABLE AS, and
 * CREATE MATERIALIZED VIEW
 *
 * For CREATE MATERIALIZED VIEW, viewQuery is the parsed-but-not-rewritten
 * SELECT Query for the view; otherwise it's NULL.  This is irrelevant in
 * the query jumbling as CreateTableAsStmt already includes a reference to
 * its own Query, so ignore it.  (We declare it as struct Query* to avoid a
 * forward reference.)
 */
#[repr(C)]
pub struct IntoClause {
    pub r#type: NodeTag,

    pub rel: *mut RangeVar,        /* target relation name */
    pub colNames: *mut List,       /* column names to assign, or NIL */
    pub accessMethod: *mut c_char, /* table access method */
    pub options: *mut List,        /* options from WITH clause */
    pub onCommit: OnCommitAction,  /* what do we do at COMMIT? */
    pub tableSpaceName: *mut c_char, /* table space to use, or NULL */
    /* materialized view's SELECT query */
    pub viewQuery: *mut Query, // pg_node_attr(query_jumble_ignore)
    pub skipData: bool,        /* true for WITH NO DATA */
}

/* ----------------------------------------------------------------
 *					node types for executable expressions
 * ----------------------------------------------------------------
 */

/*
 * Expr - generic superclass for executable-expression nodes
 *
 * All node types that are used in executable expression trees should derive
 * from Expr (that is, have Expr as their first field).  Since Expr only
 * contains NodeTag, this is a formality, but it is an easy form of
 * documentation.  See also the ExprState node types in execnodes.h.
 *
 * pg_node_attr(abstract): Expr has no NodeTag of its own.
 */
#[repr(C)]
pub struct Expr {
    pub r#type: NodeTag,
}

/*
 * Var - expression node representing a variable (ie, a table column)
 *
 * See the C header for the (extensive) commentary on varno/varattno and the
 * various special varno values used during planning and execution.
 */
pub const INNER_VAR: c_int = -1; /* reference to inner subplan */
pub const OUTER_VAR: c_int = -2; /* reference to outer subplan */
pub const INDEX_VAR: c_int = -3; /* reference to index column */
pub const ROWID_VAR: c_int = -4; /* row identity column during planning */

#[inline]
pub fn IS_SPECIAL_VARNO(varno: c_int) -> bool {
    (varno as c_int) < 0
}

/* Symbols for the indexes of the special RTE entries in rules */
pub const PRS2_OLD_VARNO: c_int = 1;
pub const PRS2_NEW_VARNO: c_int = 2;

/* Returning behavior for Vars in RETURNING list */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VarReturningType {
    VAR_RETURNING_DEFAULT, /* return OLD for DELETE, else return NEW */
    VAR_RETURNING_OLD,     /* return OLD for DELETE/UPDATE, else NULL */
    VAR_RETURNING_NEW,     /* return NEW for INSERT/UPDATE, else NULL */
}
pub use VarReturningType::*;

#[repr(C)]
pub struct Var {
    pub xpr: Expr,

    /*
     * index of this var's relation in the range table, or
     * INNER_VAR/OUTER_VAR/etc
     */
    pub varno: c_int,

    /*
     * attribute number of this var, or zero for all attrs ("whole-row Var")
     */
    pub varattno: AttrNumber,

    /* pg_type OID for the type of this var */
    pub vartype: Oid, // pg_node_attr(query_jumble_ignore)
    /* pg_attribute typmod value */
    pub vartypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub varcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /*
     * RT indexes of outer joins that can replace the Var's value with null.
     * We can omit varnullingrels in the query jumble, because it's fully
     * determined by varno/varlevelsup plus the Var's query location.
     */
    pub varnullingrels: *mut Bitmapset, // pg_node_attr(query_jumble_ignore)

    /*
     * for subquery variables referencing outer relations; 0 in a normal var,
     * >0 means N levels up
     */
    pub varlevelsup: Index,

    /* returning type of this var (see above) */
    pub varreturningtype: VarReturningType,

    /*
     * varnosyn/varattnosyn are ignored for equality, because Vars with
     * different syntactic identifiers are semantically the same as long as
     * their varno/varattno match.
     */
    /* syntactic relation index (0 if unknown) */
    pub varnosyn: Index, // pg_node_attr(equal_ignore, query_jumble_ignore)
    /* syntactic attribute number */
    pub varattnosyn: AttrNumber, // pg_node_attr(equal_ignore, query_jumble_ignore)

    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * Const
 *
 * Note: for varlena data types, we make a rule that a Const node's value
 * must be in non-extended form (4-byte header, no compression or external
 * references).  This ensures that the Const node is self-contained and makes
 * it more likely that equal() will see logically identical values as equal.
 *
 * Only the constant type OID is relevant for the query jumbling.
 */
// pg_node_attr(custom_copy_equal, custom_read_write)
#[repr(C)]
pub struct Const {
    pub xpr: Expr,
    /* pg_type OID of the constant's datatype */
    pub consttype: Oid,
    /* typmod value, if any */
    pub consttypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub constcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* typlen of the constant's datatype */
    pub constlen: c_int, // pg_node_attr(query_jumble_ignore)
    /* the constant's value */
    pub constvalue: Datum, // pg_node_attr(query_jumble_ignore)
    /* whether the constant is null (if true, constvalue is undefined) */
    pub constisnull: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * Whether this datatype is passed by value.  If true, then all the
     * information is stored in the Datum.  If false, then the Datum contains
     * a pointer to the information.
     */
    pub constbyval: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * token location, or -1 if unknown.  All constants are tracked as
     * locations in query jumbling, to be marked as parameters.
     */
    pub location: ParseLoc, // pg_node_attr(query_jumble_location)
}

/*
 * Param
 *
 *		paramkind specifies the kind of parameter. The possible values
 *		for this field are described in the C header (PARAM_EXTERN,
 *		PARAM_EXEC, PARAM_SUBLINK, PARAM_MULTIEXPR).
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParamKind {
    PARAM_EXTERN,
    PARAM_EXEC,
    PARAM_SUBLINK,
    PARAM_MULTIEXPR,
}
pub use ParamKind::*;

// pg_node_attr(custom_query_jumble)
#[repr(C)]
pub struct Param {
    pub xpr: Expr,
    pub paramkind: ParamKind, /* kind of parameter. See above */
    pub paramid: c_int,       /* numeric ID for parameter */
    pub paramtype: Oid,       /* pg_type OID of parameter's datatype */
    /* typmod value, if known */
    pub paramtypmod: int32,
    /* OID of collation, or InvalidOid if none */
    pub paramcollid: Oid,
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * Aggref
 *
 * See the C header for full commentary on the aggregate representation.
 *
 * Information related to collations, transition types and internal states
 * are irrelevant for the query jumbling.
 */
#[repr(C)]
pub struct Aggref {
    pub xpr: Expr,

    /* pg_proc Oid of the aggregate */
    pub aggfnoid: Oid,

    /* type Oid of result of the aggregate */
    pub aggtype: Oid, // pg_node_attr(query_jumble_ignore)

    /* OID of collation of result */
    pub aggcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /* OID of collation that function should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /*
     * type Oid of aggregate's transition value; ignored for equal since it
     * might not be set yet
     */
    pub aggtranstype: Oid, // pg_node_attr(equal_ignore, query_jumble_ignore)

    /* type Oids of direct and aggregated args */
    pub aggargtypes: *mut List, // pg_node_attr(query_jumble_ignore)

    /* direct arguments, if an ordered-set agg */
    pub aggdirectargs: *mut List,

    /* aggregated arguments and sort expressions */
    pub args: *mut List,

    /* ORDER BY (list of SortGroupClause) */
    pub aggorder: *mut List,

    /* DISTINCT (list of SortGroupClause) */
    pub aggdistinct: *mut List,

    /* FILTER expression, if any */
    pub aggfilter: *mut Expr,

    /* true if argument list was really '*' */
    pub aggstar: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * true if variadic arguments have been combined into an array last
     * argument
     */
    pub aggvariadic: bool, // pg_node_attr(query_jumble_ignore)

    /* aggregate kind (see pg_aggregate.h) */
    pub aggkind: c_char, // pg_node_attr(query_jumble_ignore)

    /* aggregate input already sorted */
    pub aggpresorted: bool, // pg_node_attr(equal_ignore, query_jumble_ignore)

    /* > 0 if agg belongs to outer query */
    pub agglevelsup: Index, // pg_node_attr(query_jumble_ignore)

    /* expected agg-splitting mode of parent Agg */
    pub aggsplit: AggSplit, // pg_node_attr(query_jumble_ignore)

    /* unique ID within the Agg node */
    pub aggno: c_int, // pg_node_attr(query_jumble_ignore)

    /* unique ID of transition state in the Agg */
    pub aggtransno: c_int, // pg_node_attr(query_jumble_ignore)

    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * GroupingFunc
 *
 * A GroupingFunc is a GROUPING(...) expression.  See the C header for full
 * commentary; the refs/cols annotations are ignored for equality and the
 * internal-state fields are irrelevant for the query jumbling.
 */
#[repr(C)]
pub struct GroupingFunc {
    pub xpr: Expr,

    /* arguments, not evaluated but kept for benefit of EXPLAIN etc. */
    pub args: *mut List, // pg_node_attr(query_jumble_ignore)

    /* ressortgrouprefs of arguments */
    pub refs: *mut List, // pg_node_attr(equal_ignore)

    /* actual column positions set by planner */
    pub cols: *mut List, // pg_node_attr(equal_ignore, query_jumble_ignore)

    /* same as Aggref.agglevelsup */
    pub agglevelsup: Index,

    /* token location */
    pub location: ParseLoc,
}

/*
 * WindowFunc
 *
 * Collation information is irrelevant for the query jumbling, as is the
 * internal state information of the node like "winstar" and "winagg".
 */
#[repr(C)]
pub struct WindowFunc {
    pub xpr: Expr,
    /* pg_proc Oid of the function */
    pub winfnoid: Oid,
    /* type Oid of result of the window function */
    pub wintype: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation of result */
    pub wincollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation that function should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* arguments to the window function */
    pub args: *mut List,
    /* FILTER expression, if any */
    pub aggfilter: *mut Expr,
    /* List of WindowFuncRunConditions to help short-circuit execution */
    pub runCondition: *mut List, // pg_node_attr(query_jumble_ignore)
    /* index of associated WindowClause */
    pub winref: Index,
    /* true if argument list was really '*' */
    pub winstar: bool, // pg_node_attr(query_jumble_ignore)
    /* is function a simple aggregate? */
    pub winagg: bool, // pg_node_attr(query_jumble_ignore)
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * WindowFuncRunCondition
 *
 * Represents intermediate OpExprs which will be used by WindowAgg to
 * short-circuit execution.
 */
#[repr(C)]
pub struct WindowFuncRunCondition {
    pub xpr: Expr,

    /* PG_OPERATOR OID of the operator */
    pub opno: Oid,
    /* OID of collation that operator should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /*
     * true of WindowFunc belongs on the left of the resulting OpExpr or false
     * if the WindowFunc is on the right.
     */
    pub wfunc_left: bool,

    /*
     * The Expr being compared to the WindowFunc to use in the OpExpr in the
     * WindowAgg's runCondition
     */
    pub arg: *mut Expr,
}

/*
 * MergeSupportFunc
 *
 * A MergeSupportFunc is a merge support function expression that can only
 * appear in the RETURNING list of a MERGE command.  It returns information
 * about the currently executing merge action.
 *
 * Currently, the only supported function is MERGE_ACTION(), which returns the
 * command executed ("INSERT", "UPDATE", or "DELETE").
 */
#[repr(C)]
pub struct MergeSupportFunc {
    pub xpr: Expr,
    /* type Oid of result */
    pub msftype: Oid,
    /* OID of collation, or InvalidOid if none */
    pub msfcollid: Oid,
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * SubscriptingRef: describes a subscripting operation over a container
 * (array, etc).  See the C header for the full semantics of the fetch/store
 * cases and slice handling.
 *
 * Any internal state data is ignored for the query jumbling.
 */
#[repr(C)]
pub struct SubscriptingRef {
    pub xpr: Expr,
    /* type of the container proper */
    pub refcontainertype: Oid, // pg_node_attr(query_jumble_ignore)
    /* the container type's pg_type.typelem */
    pub refelemtype: Oid, // pg_node_attr(query_jumble_ignore)
    /* type of the SubscriptingRef's result */
    pub refrestype: Oid, // pg_node_attr(query_jumble_ignore)
    /* typmod of the result */
    pub reftypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* collation of result, or InvalidOid if none */
    pub refcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* expressions that evaluate to upper container indexes */
    pub refupperindexpr: *mut List,

    /*
     * expressions that evaluate to lower container indexes, or NIL for single
     * container element.
     */
    pub reflowerindexpr: *mut List,
    /* the expression that evaluates to a container value */
    pub refexpr: *mut Expr,
    /* expression for the source value, or NULL if fetch */
    pub refassgnexpr: *mut Expr,
}

/*
 * CoercionContext - distinguishes the allowed set of type casts
 *
 * NB: ordering of the alternatives is significant; later (larger) values
 * allow more casts than earlier ones.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CoercionContext {
    COERCION_IMPLICIT,   /* coercion in context of expression */
    COERCION_ASSIGNMENT, /* coercion in context of assignment */
    COERCION_PLPGSQL,    /* if no assignment cast, use CoerceViaIO */
    COERCION_EXPLICIT,   /* explicit cast operation */
}
pub use CoercionContext::*;

/*
 * CoercionForm - how to display a FuncExpr or related node
 *
 * NB: equal() ignores CoercionForm fields, therefore this *must* not carry
 * any semantically significant information.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CoercionForm {
    COERCE_EXPLICIT_CALL, /* display as a function call */
    COERCE_EXPLICIT_CAST, /* display as an explicit cast */
    COERCE_IMPLICIT_CAST, /* implicit cast, so hide it */
    COERCE_SQL_SYNTAX,    /* display with SQL-mandated special syntax */
}
pub use CoercionForm::*;

/*
 * FuncExpr - expression node for a function call
 *
 * Collation information is irrelevant for the query jumbling, only the
 * arguments and the function OID matter.
 */
#[repr(C)]
pub struct FuncExpr {
    pub xpr: Expr,
    /* PG_PROC OID of the function */
    pub funcid: Oid,
    /* PG_TYPE OID of result value */
    pub funcresulttype: Oid, // pg_node_attr(query_jumble_ignore)
    /* true if function returns set */
    pub funcretset: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * true if variadic arguments have been combined into an array last
     * argument
     */
    pub funcvariadic: bool, // pg_node_attr(query_jumble_ignore)
    /* how to display this function call */
    pub funcformat: CoercionForm, // pg_node_attr(query_jumble_ignore)
    /* OID of collation of result */
    pub funccollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation that function should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* arguments to the function */
    pub args: *mut List,
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * NamedArgExpr - a named argument of a function
 *
 * This node type can only appear in the args list of a FuncCall or FuncExpr
 * node.  See the C header for details on positional/named/mixed notation.
 */
#[repr(C)]
pub struct NamedArgExpr {
    pub xpr: Expr,
    /* the argument expression */
    pub arg: *mut Expr,
    /* the name */
    pub name: *mut c_char, // pg_node_attr(query_jumble_ignore)
    /* argument's number in positional notation */
    pub argnumber: c_int,
    /* argument name location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * OpExpr - expression node for an operator invocation
 *
 * Semantically, this is essentially the same as a function call.
 *
 * Internal state information and collation data is irrelevant for the query
 * jumbling.
 */
#[repr(C)]
pub struct OpExpr {
    pub xpr: Expr,

    /* PG_OPERATOR OID of the operator */
    pub opno: Oid,

    /* PG_PROC OID of underlying function */
    pub opfuncid: Oid, // pg_node_attr(equal_ignore_if_zero, query_jumble_ignore)

    /* PG_TYPE OID of result value */
    pub opresulttype: Oid, // pg_node_attr(query_jumble_ignore)

    /* true if operator returns set */
    pub opretset: bool, // pg_node_attr(query_jumble_ignore)

    /* OID of collation of result */
    pub opcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /* OID of collation that operator should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /* arguments to the operator (1 or 2) */
    pub args: *mut List,

    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * DistinctExpr - expression node for "x IS DISTINCT FROM y"
 *
 * Except for the nodetag, this is represented identically to an OpExpr
 * referencing the "=" operator for x and y.  Has its own NodeTag
 * (T_DistinctExpr).
 */
pub type DistinctExpr = OpExpr;

/*
 * NullIfExpr - a NULLIF expression
 *
 * Like DistinctExpr, this is represented the same as an OpExpr referencing
 * the "=" operator for x and y.  Has its own NodeTag (T_NullIfExpr).
 */
pub type NullIfExpr = OpExpr;

/*
 * ScalarArrayOpExpr - expression node for "scalar op ANY/ALL (array)"
 *
 * See the C header for the full semantics, including the hashed-execution
 * (hashfuncid) and hashed NOT IN (negfuncid) cases.
 *
 * OID entries of the internal function types are irrelevant for the query
 * jumbling, but the operator OID and the arguments are.
 */
#[repr(C)]
pub struct ScalarArrayOpExpr {
    pub xpr: Expr,

    /* PG_OPERATOR OID of the operator */
    pub opno: Oid,

    /* PG_PROC OID of comparison function */
    pub opfuncid: Oid, // pg_node_attr(equal_ignore_if_zero, query_jumble_ignore)

    /* PG_PROC OID of hash func or InvalidOid */
    pub hashfuncid: Oid, // pg_node_attr(equal_ignore_if_zero, query_jumble_ignore)

    /* PG_PROC OID of negator of opfuncid function or InvalidOid.  See above */
    pub negfuncid: Oid, // pg_node_attr(equal_ignore_if_zero, query_jumble_ignore)

    /* true for ANY, false for ALL */
    pub useOr: bool,

    /* OID of collation that operator should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)

    /* the scalar and array operands */
    pub args: *mut List,

    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * BoolExpr - expression node for the basic Boolean operators AND, OR, NOT
 *
 * Notice the arguments are given as a List.  For NOT, of course the list
 * must always have exactly one element.  For AND and OR, there can be two
 * or more arguments.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BoolExprType {
    AND_EXPR,
    OR_EXPR,
    NOT_EXPR,
}
pub use BoolExprType::*;

// pg_node_attr(custom_read_write)
#[repr(C)]
pub struct BoolExpr {
    pub xpr: Expr,
    pub boolop: BoolExprType,
    pub args: *mut List,    /* arguments to this expression */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * SubLink
 *
 * A SubLink represents a subselect appearing in an expression, and in some
 * cases also the combining operator(s) just above it.  See the C header for
 * the meaning of each subLinkType form.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubLinkType {
    EXISTS_SUBLINK,
    ALL_SUBLINK,
    ANY_SUBLINK,
    ROWCOMPARE_SUBLINK,
    EXPR_SUBLINK,
    MULTIEXPR_SUBLINK,
    ARRAY_SUBLINK,
    CTE_SUBLINK, /* for SubPlans only */
}
pub use SubLinkType::*;

#[repr(C)]
pub struct SubLink {
    pub xpr: Expr,
    pub subLinkType: SubLinkType, /* see above */
    pub subLinkId: c_int,         /* ID (1..n); 0 if not MULTIEXPR */
    pub testexpr: *mut Node,      /* outer-query test for ALL/ANY/ROWCOMPARE */
    /* originally specified operator name */
    pub operName: *mut List, // pg_node_attr(query_jumble_ignore)
    /* subselect as Query* or raw parsetree */
    pub subselect: *mut Node,
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * SubPlan - executable expression node for a subplan (sub-SELECT)
 *
 * The planner replaces SubLink nodes in expression trees with SubPlan
 * nodes after it has finished planning the subquery.  See the C header for
 * the full description of the in/out parameter passing scheme.
 */
// pg_node_attr(no_query_jumble)
#[repr(C)]
pub struct SubPlan {
    pub xpr: Expr,
    /* Fields copied from original SubLink: */
    pub subLinkType: SubLinkType, /* see above */
    /* The combining operators, transformed to an executable expression: */
    pub testexpr: *mut Node, /* OpExpr or RowCompareExpr expression tree */
    pub paramIds: *mut List, /* IDs of Params embedded in the above */
    /* Identification of the Plan tree to use: */
    pub plan_id: c_int, /* Index (from 1) in PlannedStmt.subplans */
    /* Identification of the SubPlan for EXPLAIN and debugging purposes: */
    pub plan_name: *mut c_char, /* A name assigned during planning */
    /* Extra data useful for determining subplan's output type: */
    pub firstColType: Oid,      /* Type of first column of subplan result */
    pub firstColTypmod: int32,  /* Typmod of first column of subplan result */
    pub firstColCollation: Oid, /* Collation of first column of subplan
                                 * result */
    /* Information about execution strategy: */
    pub useHashTable: bool, /* true to store subselect output in a hash
                             * table (implies we are doing "IN") */
    pub unknownEqFalse: bool, /* true if it's okay to return FALSE when the
                               * spec result is UNKNOWN; this allows much
                               * simpler handling of null values */
    pub parallel_safe: bool, /* is the subplan parallel-safe? */
    /* Note: parallel_safe does not consider contents of testexpr or args */
    /* Information for passing params into and out of the subselect: */
    /* setParam and parParam are lists of integers (param IDs) */
    pub setParam: *mut List, /* initplan and MULTIEXPR subqueries have to
                              * set these Params for parent plan */
    pub parParam: *mut List, /* indices of input Params from parent plan */
    pub args: *mut List,     /* exprs to pass as parParam values */
    /* Estimated execution costs: */
    pub startup_cost: Cost,   /* one-time setup cost */
    pub per_call_cost: Cost,  /* cost for each subplan evaluation */
}

/*
 * AlternativeSubPlan - expression node for a choice among SubPlans
 *
 * This is used only transiently during planning: by the time the plan
 * reaches the executor, all AlternativeSubPlan nodes have been removed.
 */
// pg_node_attr(no_query_jumble)
#[repr(C)]
pub struct AlternativeSubPlan {
    pub xpr: Expr,
    pub subplans: *mut List, /* SubPlan(s) with equivalent results */
}

/* ----------------
 * FieldSelect
 *
 * FieldSelect represents the operation of extracting one field from a tuple
 * value.  At runtime, the input expression is expected to yield a rowtype
 * Datum.  The specified field number is extracted and returned as a Datum.
 * ----------------
 */
#[repr(C)]
pub struct FieldSelect {
    pub xpr: Expr,
    pub arg: *mut Expr,        /* input expression */
    pub fieldnum: AttrNumber,  /* attribute number of field to extract */
    /* type of the field (result type of this node) */
    pub resulttype: Oid, // pg_node_attr(query_jumble_ignore)
    /* output typmod (usually -1) */
    pub resulttypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* OID of collation of the field */
    pub resultcollid: Oid, // pg_node_attr(query_jumble_ignore)
}

/* ----------------
 * FieldStore
 *
 * FieldStore represents the operation of modifying one field in a tuple
 * value, yielding a new tuple value (the input is not touched!).  See the
 * C header for full details.
 * ----------------
 */
#[repr(C)]
pub struct FieldStore {
    pub xpr: Expr,
    pub arg: *mut Expr,    /* input tuple value */
    pub newvals: *mut List, /* new value(s) for field(s) */
    /* integer list of field attnums */
    pub fieldnums: *mut List, // pg_node_attr(query_jumble_ignore)
    /* type of result (same as type of arg) */
    pub resulttype: Oid, // pg_node_attr(query_jumble_ignore)
    /* Like RowExpr, we deliberately omit a typmod and collation here */
}

/* ----------------
 * RelabelType
 *
 * RelabelType represents a "dummy" type coercion between two binary-
 * compatible datatypes.  It is a no-op at runtime.
 * ----------------
 */
#[repr(C)]
pub struct RelabelType {
    pub xpr: Expr,
    pub arg: *mut Expr,  /* input expression */
    pub resulttype: Oid, /* output type of coercion expression */
    /* output typmod (usually -1) */
    pub resulttypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub resultcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* how to display this node */
    pub relabelformat: CoercionForm, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc,          /* token location, or -1 if unknown */
}

/* ----------------
 * CoerceViaIO
 *
 * CoerceViaIO represents a type coercion between two types whose textual
 * representations are compatible, implemented by invoking the source type's
 * typoutput function then the destination type's typinput function.
 * ----------------
 */
#[repr(C)]
pub struct CoerceViaIO {
    pub xpr: Expr,
    pub arg: *mut Expr,  /* input expression */
    pub resulttype: Oid, /* output type of coercion */
    /* output typmod is not stored, but is presumed -1 */
    /* OID of collation, or InvalidOid if none */
    pub resultcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* how to display this node */
    pub coerceformat: CoercionForm, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc,         /* token location, or -1 if unknown */
}

/* ----------------
 * ArrayCoerceExpr
 *
 * ArrayCoerceExpr represents a type coercion from one array type to another,
 * implemented by applying the per-element coercion expression "elemexpr" to
 * each element of the source array.
 * ----------------
 */
#[repr(C)]
pub struct ArrayCoerceExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,     /* input expression (yields an array) */
    pub elemexpr: *mut Expr, /* expression representing per-element work */
    pub resulttype: Oid,    /* output type of coercion (an array type) */
    /* output typmod (also element typmod) */
    pub resulttypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub resultcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* how to display this node */
    pub coerceformat: CoercionForm, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc,         /* token location, or -1 if unknown */
}

/* ----------------
 * ConvertRowtypeExpr
 *
 * ConvertRowtypeExpr represents a type coercion from one composite type
 * to another, matched up by name.  See the C header for full details.
 * ----------------
 */
#[repr(C)]
pub struct ConvertRowtypeExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,  /* input expression */
    pub resulttype: Oid, /* output type (always a composite type) */
    /* Like RowExpr, we deliberately omit a typmod and collation here */
    /* how to display this node */
    pub convertformat: CoercionForm, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc,          /* token location, or -1 if unknown */
}

/*----------
 * CollateExpr - COLLATE
 *
 * The planner replaces CollateExpr with RelabelType during expression
 * preprocessing, so execution never sees a CollateExpr.
 *----------
 */
#[repr(C)]
pub struct CollateExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,     /* input expression */
    pub collOid: Oid,       /* collation's OID */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*----------
 * CaseExpr - a CASE expression
 *
 * We support two distinct forms of CASE expression; see the C header for the
 * detailed description of the testexpr/CaseTestExpr machinery.
 *----------
 */
#[repr(C)]
pub struct CaseExpr {
    pub xpr: Expr,
    /* type of expression result */
    pub casetype: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub casecollid: Oid, // pg_node_attr(query_jumble_ignore)
    pub arg: *mut Expr,       /* implicit equality comparison argument */
    pub args: *mut List,      /* the arguments (list of WHEN clauses) */
    pub defresult: *mut Expr, /* the default result (ELSE clause) */
    pub location: ParseLoc,   /* token location, or -1 if unknown */
}

/*
 * CaseWhen - one arm of a CASE expression
 */
#[repr(C)]
pub struct CaseWhen {
    pub xpr: Expr,
    pub expr: *mut Expr,    /* condition expression */
    pub result: *mut Expr,  /* substitution result */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * Placeholder node for the test value to be processed by a CASE expression.
 * This is effectively like a Param, but can be implemented more simply
 * since we need only one replacement value at a time.  See the C header for
 * the other (abused) uses of this node type.
 */
#[repr(C)]
pub struct CaseTestExpr {
    pub xpr: Expr,
    pub typeId: Oid, /* type for substituted value */
    /* typemod for substituted value */
    pub typeMod: int32, // pg_node_attr(query_jumble_ignore)
    /* collation for the substituted value */
    pub collation: Oid, // pg_node_attr(query_jumble_ignore)
}

/*
 * ArrayExpr - an ARRAY[] expression
 *
 * Note: if multidims is false, the constituent expressions all yield the
 * scalar type identified by element_typeid.  If multidims is true, the
 * constituent expressions all yield arrays of element_typeid.
 */
#[repr(C)]
pub struct ArrayExpr {
    pub xpr: Expr,
    /* type of expression result */
    pub array_typeid: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub array_collid: Oid, // pg_node_attr(query_jumble_ignore)
    /* common type of array elements */
    pub element_typeid: Oid, // pg_node_attr(query_jumble_ignore)
    /* the array elements or sub-arrays */
    pub elements: *mut List, // pg_node_attr(query_jumble_squash)
    /* true if elements are sub-arrays */
    pub multidims: bool, // pg_node_attr(query_jumble_ignore)
    /* location of the start of the elements list */
    pub list_start: ParseLoc,
    /* location of the end of the elements list */
    pub list_end: ParseLoc,
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * RowExpr - a ROW() expression
 *
 * Note: the list of fields must have a one-for-one correspondence with
 * physical fields of the associated rowtype.  See the C header for the full
 * commentary on colnames and the deliberate omission of typmod/collation.
 */
#[repr(C)]
pub struct RowExpr {
    pub xpr: Expr,
    pub args: *mut List, /* the fields */

    /* RECORDOID or a composite type's ID */
    pub row_typeid: Oid, // pg_node_attr(query_jumble_ignore)

    /*
     * row_typeid cannot be a domain over composite, only plain composite.  To
     * create a composite domain value, apply CoerceToDomain to the RowExpr.
     *
     * Note: we deliberately do NOT store a typmod.  We must assume typmod -1
     * for a RowExpr node.  We don't need to store a collation either.
     */

    /* how to display this node */
    pub row_format: CoercionForm, // pg_node_attr(query_jumble_ignore)

    /* list of String, or NIL */
    pub colnames: *mut List, // pg_node_attr(query_jumble_ignore)

    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * RowCompareExpr - row-wise comparison, such as (a, b) <= (1, 2)
 *
 * A RowCompareExpr node is only generated for the < <= > >= cases;
 * the = and <> cases are translated to simple AND or OR combinations
 * of the pairwise comparisons.
 */
#[repr(C)]
pub struct RowCompareExpr {
    pub xpr: Expr,

    /* LT LE GE or GT, never EQ or NE */
    pub cmptype: CompareType,
    /* OID list of pairwise comparison ops */
    pub opnos: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of containing operator families */
    pub opfamilies: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of collations for comparisons */
    pub inputcollids: *mut List, // pg_node_attr(query_jumble_ignore)
    /* the left-hand input arguments */
    pub largs: *mut List,
    /* the right-hand input arguments */
    pub rargs: *mut List,
}

/*
 * CoalesceExpr - a COALESCE expression
 */
#[repr(C)]
pub struct CoalesceExpr {
    pub xpr: Expr,
    /* type of expression result */
    pub coalescetype: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub coalescecollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* the arguments */
    pub args: *mut List,
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * MinMaxExpr - a GREATEST or LEAST function
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MinMaxOp {
    IS_GREATEST,
    IS_LEAST,
}
pub use MinMaxOp::*;

#[repr(C)]
pub struct MinMaxExpr {
    pub xpr: Expr,
    /* common type of arguments and result */
    pub minmaxtype: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation of result */
    pub minmaxcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* OID of collation that function should use */
    pub inputcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* function to execute */
    pub op: MinMaxOp,
    /* the arguments */
    pub args: *mut List,
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * SQLValueFunction - parameterless functions with special grammar productions
 *
 * See the C header for the full list and rationale.  All variants return
 * non-collating datatypes, so there is no collation field.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SQLValueFunctionOp {
    SVFOP_CURRENT_DATE,
    SVFOP_CURRENT_TIME,
    SVFOP_CURRENT_TIME_N,
    SVFOP_CURRENT_TIMESTAMP,
    SVFOP_CURRENT_TIMESTAMP_N,
    SVFOP_LOCALTIME,
    SVFOP_LOCALTIME_N,
    SVFOP_LOCALTIMESTAMP,
    SVFOP_LOCALTIMESTAMP_N,
    SVFOP_CURRENT_ROLE,
    SVFOP_CURRENT_USER,
    SVFOP_USER,
    SVFOP_SESSION_USER,
    SVFOP_CURRENT_CATALOG,
    SVFOP_CURRENT_SCHEMA,
}
pub use SQLValueFunctionOp::*;

#[repr(C)]
pub struct SQLValueFunction {
    pub xpr: Expr,
    pub op: SQLValueFunctionOp, /* which function this is */

    /*
     * Result type/typmod.  Type is fully determined by "op", so no need to
     * include this Oid in the query jumbling.
     */
    pub r#type: Oid, // pg_node_attr(query_jumble_ignore)
    pub typmod: int32,
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * XmlExpr - various SQL/XML functions requiring special grammar productions
 *
 * 'name' carries the "NAME foo" argument (already XML-escaped).
 * 'named_args' and 'arg_names' represent an xml_attribute list.
 * 'args' carries all other arguments.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum XmlExprOp {
    IS_XMLCONCAT,    /* XMLCONCAT(args) */
    IS_XMLELEMENT,   /* XMLELEMENT(name, xml_attributes, args) */
    IS_XMLFOREST,    /* XMLFOREST(xml_attributes) */
    IS_XMLPARSE,     /* XMLPARSE(text, is_doc, preserve_ws) */
    IS_XMLPI,        /* XMLPI(name [, args]) */
    IS_XMLROOT,      /* XMLROOT(xml, version, standalone) */
    IS_XMLSERIALIZE, /* XMLSERIALIZE(is_document, xmlval, indent) */
    IS_DOCUMENT,     /* xmlval IS DOCUMENT */
}
pub use XmlExprOp::*;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum XmlOptionType {
    XMLOPTION_DOCUMENT,
    XMLOPTION_CONTENT,
}
pub use XmlOptionType::*;

#[repr(C)]
pub struct XmlExpr {
    pub xpr: Expr,
    /* xml function ID */
    pub op: XmlExprOp,
    /* name in xml(NAME foo ...) syntaxes */
    pub name: *mut c_char, // pg_node_attr(query_jumble_ignore)
    /* non-XML expressions for xml_attributes */
    pub named_args: *mut List,
    /* parallel list of String values */
    pub arg_names: *mut List, // pg_node_attr(query_jumble_ignore)
    /* list of expressions */
    pub args: *mut List,
    /* DOCUMENT or CONTENT */
    pub xmloption: XmlOptionType, // pg_node_attr(query_jumble_ignore)
    /* INDENT option for XMLSERIALIZE */
    pub indent: bool,
    /* target type/typmod for XMLSERIALIZE */
    pub r#type: Oid, // pg_node_attr(query_jumble_ignore)
    pub typmod: int32, // pg_node_attr(query_jumble_ignore)
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * JsonEncoding -
 *		representation of JSON ENCODING clause
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonEncoding {
    JS_ENC_DEFAULT, /* unspecified */
    JS_ENC_UTF8,
    JS_ENC_UTF16,
    JS_ENC_UTF32,
}
pub use JsonEncoding::*;

/*
 * JsonFormatType -
 *		enumeration of JSON formats used in JSON FORMAT clause
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonFormatType {
    JS_FORMAT_DEFAULT, /* unspecified */
    JS_FORMAT_JSON,    /* FORMAT JSON [ENCODING ...] */
    JS_FORMAT_JSONB,   /* implicit internal format for RETURNING jsonb */
}
pub use JsonFormatType::*;

/*
 * JsonFormat -
 *		representation of JSON FORMAT clause
 */
#[repr(C)]
pub struct JsonFormat {
    pub r#type: NodeTag,
    pub format_type: JsonFormatType, /* format type */
    pub encoding: JsonEncoding,      /* JSON encoding */
    pub location: ParseLoc,          /* token location, or -1 if unknown */
}

/*
 * JsonReturning -
 *		transformed representation of JSON RETURNING clause
 */
#[repr(C)]
pub struct JsonReturning {
    pub r#type: NodeTag,
    pub format: *mut JsonFormat, /* output JSON format */
    pub typid: Oid,              /* target type Oid */
    pub typmod: int32,           /* target type modifier */
}

/*
 * JsonValueExpr -
 *		representation of JSON value expression (expr [FORMAT JsonFormat])
 *
 * raw_expr is the user-specified value, while formatted_expr is the value
 * obtained by coercing raw_expr to the type required by either the FORMAT
 * clause or an enclosing node's RETURNING clause.
 */
#[repr(C)]
pub struct JsonValueExpr {
    pub r#type: NodeTag,
    pub raw_expr: *mut Expr,       /* user-specified expression */
    pub formatted_expr: *mut Expr, /* coerced formatted expression */
    pub format: *mut JsonFormat,   /* FORMAT clause, if specified */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonConstructorType {
    JSCTOR_JSON_OBJECT = 1,
    JSCTOR_JSON_ARRAY = 2,
    JSCTOR_JSON_OBJECTAGG = 3,
    JSCTOR_JSON_ARRAYAGG = 4,
    JSCTOR_JSON_PARSE = 5,
    JSCTOR_JSON_SCALAR = 6,
    JSCTOR_JSON_SERIALIZE = 7,
}
pub use JsonConstructorType::*;

/*
 * JsonConstructorExpr -
 *		wrapper over FuncExpr/Aggref/WindowFunc for SQL/JSON constructors
 */
#[repr(C)]
pub struct JsonConstructorExpr {
    pub xpr: Expr,
    pub r#type: JsonConstructorType, /* constructor type */
    pub args: *mut List,
    pub func: *mut Expr,             /* underlying json[b]_xxx() function call */
    pub coercion: *mut Expr,         /* coercion to RETURNING type */
    pub returning: *mut JsonReturning, /* RETURNING clause */
    pub absent_on_null: bool,        /* ABSENT ON NULL? */
    pub unique: bool,                /* WITH UNIQUE KEYS? (JSON_OBJECT[AGG] only) */
    pub location: ParseLoc,
}

/*
 * JsonValueType -
 *		representation of JSON item type in IS JSON predicate
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonValueType {
    JS_TYPE_ANY,    /* IS JSON [VALUE] */
    JS_TYPE_OBJECT, /* IS JSON OBJECT */
    JS_TYPE_ARRAY,  /* IS JSON ARRAY */
    JS_TYPE_SCALAR, /* IS JSON SCALAR */
}
pub use JsonValueType::*;

/*
 * JsonIsPredicate -
 *		representation of IS JSON predicate
 */
#[repr(C)]
pub struct JsonIsPredicate {
    pub r#type: NodeTag,
    pub expr: *mut Node,         /* subject expression */
    pub format: *mut JsonFormat, /* FORMAT clause, if specified */
    pub item_type: JsonValueType, /* JSON item type */
    pub unique_keys: bool,       /* check key uniqueness? */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/* Nodes used in SQL/JSON query functions */

/*
 * JsonWrapper -
 *		representation of WRAPPER clause for JSON_QUERY()
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonWrapper {
    JSW_UNSPEC,
    JSW_NONE,
    JSW_CONDITIONAL,
    JSW_UNCONDITIONAL,
}
pub use JsonWrapper::*;

/*
 * JsonBehaviorType -
 *		enumeration of behavior types used in SQL/JSON ON ERROR/EMPTY clauses
 *
 * 		If enum members are reordered, get_json_behavior() from ruleutils.c
 * 		must be updated accordingly.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonBehaviorType {
    JSON_BEHAVIOR_NULL = 0,
    JSON_BEHAVIOR_ERROR,
    JSON_BEHAVIOR_EMPTY,
    JSON_BEHAVIOR_TRUE,
    JSON_BEHAVIOR_FALSE,
    JSON_BEHAVIOR_UNKNOWN,
    JSON_BEHAVIOR_EMPTY_ARRAY,
    JSON_BEHAVIOR_EMPTY_OBJECT,
    JSON_BEHAVIOR_DEFAULT,
}
pub use JsonBehaviorType::*;

/*
 * JsonBehavior
 *		Specifications for ON ERROR / ON EMPTY behaviors of SQL/JSON
 *		query functions specified by a JsonExpr
 *
 * 'expr' is the expression to emit when a given behavior (EMPTY or ERROR)
 * occurs on evaluating the SQL/JSON query function.  'coerce' is set to true
 * if 'expr' isn't already of the expected target type given by
 * JsonExpr.returning.
 */
#[repr(C)]
pub struct JsonBehavior {
    pub r#type: NodeTag,

    pub btype: JsonBehaviorType,
    pub expr: *mut Node,
    pub coerce: bool,
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * JsonExprOp -
 *		enumeration of SQL/JSON query function types
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonExprOp {
    JSON_EXISTS_OP, /* JSON_EXISTS() */
    JSON_QUERY_OP,  /* JSON_QUERY() */
    JSON_VALUE_OP,  /* JSON_VALUE() */
    JSON_TABLE_OP,  /* JSON_TABLE() */
}
pub use JsonExprOp::*;

/*
 * JsonExpr -
 *		Transformed representation of JSON_VALUE(), JSON_QUERY(), and
 *		JSON_EXISTS()
 */
#[repr(C)]
pub struct JsonExpr {
    pub xpr: Expr,

    pub op: JsonExprOp,

    pub column_name: *mut c_char, /* JSON_TABLE() column name or NULL if this is
                                   * not for a JSON_TABLE() */

    /* jsonb-valued expression to query */
    pub formatted_expr: *mut Node,

    /* Format of the above expression needed by ruleutils.c */
    pub format: *mut JsonFormat,

    /* jsonpath-valued expression containing the query pattern */
    pub path_spec: *mut Node,

    /* Expected type/format of the output. */
    pub returning: *mut JsonReturning,

    /* Information about the PASSING argument expressions */
    pub passing_names: *mut List,
    pub passing_values: *mut List,

    /* User-specified or default ON EMPTY and ON ERROR behaviors */
    pub on_empty: *mut JsonBehavior,
    pub on_error: *mut JsonBehavior,

    /*
     * Information about converting the result of jsonpath functions
     * JsonPathQuery() and JsonPathValue() to the RETURNING type.
     */
    pub use_io_coercion: bool,
    pub use_json_coercion: bool,

    /* WRAPPER specification for JSON_QUERY */
    pub wrapper: JsonWrapper,

    /* KEEP or OMIT QUOTES for singleton scalars returned by JSON_QUERY() */
    pub omit_quotes: bool,

    /* JsonExpr's collation. */
    pub collation: Oid,

    /* Original JsonFuncExpr's location */
    pub location: ParseLoc,
}

/*
 * JsonTablePath
 *		A JSON path expression to be computed as part of evaluating
 *		a JSON_TABLE plan node
 */
#[repr(C)]
pub struct JsonTablePath {
    pub r#type: NodeTag,

    pub value: *mut Const,
    pub name: *mut c_char,
}

/*
 * JsonTablePlan -
 *		Abstract class to represent different types of JSON_TABLE "plans".
 *		A plan is used to generate a "row pattern" value by evaluating a JSON
 *		path expression against an input JSON document, which is then used for
 *		populating JSON_TABLE() columns.
 *
 * pg_node_attr(abstract)
 */
#[repr(C)]
pub struct JsonTablePlan {
    pub r#type: NodeTag,
}

/*
 * JSON_TABLE plan to evaluate a JSON path expression and NESTED paths, if
 * any.
 */
#[repr(C)]
pub struct JsonTablePathScan {
    pub plan: JsonTablePlan,

    /* JSON path to evaluate */
    pub path: *mut JsonTablePath,

    /*
     * ERROR/EMPTY ON ERROR behavior; only significant in the plan for the
     * top-level path.
     */
    pub errorOnError: bool,

    /* Plan(s) for nested columns, if any. */
    pub child: *mut JsonTablePlan,

    /*
     * 0-based index in TableFunc.colvalexprs of the 1st and the last column
     * covered by this plan.  Both are -1 if all columns are nested and thus
     * computed by the child plan(s).
     */
    pub colMin: c_int,
    pub colMax: c_int,
}

/*
 * JsonTableSiblingJoin -
 *		Plan to join rows of sibling NESTED COLUMNS clauses in the same parent
 *		COLUMNS clause
 */
#[repr(C)]
pub struct JsonTableSiblingJoin {
    pub plan: JsonTablePlan,

    pub lplan: *mut JsonTablePlan,
    pub rplan: *mut JsonTablePlan,
}

/* ----------------
 * NullTest
 *
 * NullTest represents the operation of testing a value for NULLness.
 * See the C header for the argisrow semantics.
 * ----------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NullTestType {
    IS_NULL,
    IS_NOT_NULL,
}
pub use NullTestType::*;

#[repr(C)]
pub struct NullTest {
    pub xpr: Expr,
    pub arg: *mut Expr,             /* input expression */
    pub nulltesttype: NullTestType, /* IS NULL, IS NOT NULL */
    /* T to perform field-by-field null checks */
    pub argisrow: bool, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * BooleanTest
 *
 * BooleanTest represents the operation of determining whether a boolean
 * is TRUE, FALSE, or UNKNOWN (ie, NULL).  All six meaningful combinations
 * are supported.  Note that a NULL input does *not* cause a NULL result.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BoolTestType {
    IS_TRUE,
    IS_NOT_TRUE,
    IS_FALSE,
    IS_NOT_FALSE,
    IS_UNKNOWN,
    IS_NOT_UNKNOWN,
}
pub use BoolTestType::*;

#[repr(C)]
pub struct BooleanTest {
    pub xpr: Expr,
    pub arg: *mut Expr,             /* input expression */
    pub booltesttype: BoolTestType, /* test type */
    pub location: ParseLoc,         /* token location, or -1 if unknown */
}

/*
 * MergeAction
 *
 * Transformed representation of a WHEN clause in a MERGE statement
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MergeMatchKind {
    MERGE_WHEN_MATCHED,
    MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
    MERGE_WHEN_NOT_MATCHED_BY_TARGET,
}
pub use MergeMatchKind::*;

pub const NUM_MERGE_MATCH_KINDS: c_int = MERGE_WHEN_NOT_MATCHED_BY_TARGET as c_int + 1;

#[repr(C)]
pub struct MergeAction {
    pub r#type: NodeTag,
    pub matchKind: MergeMatchKind, /* MATCHED/NOT MATCHED BY SOURCE/TARGET */
    pub commandType: CmdType,      /* INSERT/UPDATE/DELETE/DO NOTHING */
    /* OVERRIDING clause */
    pub r#override: OverridingKind, // pg_node_attr(query_jumble_ignore)
    pub qual: *mut Node,            /* transformed WHEN conditions */
    pub targetList: *mut List,      /* the target list (of TargetEntry) */
    /* target attribute numbers of an UPDATE */
    pub updateColnos: *mut List, // pg_node_attr(query_jumble_ignore)
}

/*
 * CoerceToDomain
 *
 * CoerceToDomain represents the operation of coercing a value to a domain
 * type.  See the C header for the runtime constraint-checking semantics.
 */
#[repr(C)]
pub struct CoerceToDomain {
    pub xpr: Expr,
    pub arg: *mut Expr,  /* input expression */
    pub resulttype: Oid, /* domain type ID (result type) */
    /* output typmod (currently always -1) */
    pub resulttypmod: int32, // pg_node_attr(query_jumble_ignore)
    /* OID of collation, or InvalidOid if none */
    pub resultcollid: Oid, // pg_node_attr(query_jumble_ignore)
    /* how to display this node */
    pub coercionformat: CoercionForm, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc,           /* token location, or -1 if unknown */
}

/*
 * Placeholder node for the value to be processed by a domain's check
 * constraint.  This is effectively like a Param, but can be implemented more
 * simply since we need only one replacement value at a time.
 *
 * Note: the typeId/typeMod/collation will be set from the domain's base type,
 * not the domain itself.
 */
#[repr(C)]
pub struct CoerceToDomainValue {
    pub xpr: Expr,
    /* type for substituted value */
    pub typeId: Oid,
    /* typemod for substituted value */
    pub typeMod: int32, // pg_node_attr(query_jumble_ignore)
    /* collation for the substituted value */
    pub collation: Oid, // pg_node_attr(query_jumble_ignore)
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * Placeholder node for a DEFAULT marker in an INSERT or UPDATE command.
 *
 * This is not an executable expression: it must be replaced by the actual
 * column default expression during rewriting.  But it is convenient to
 * treat it as an expression node during parsing and rewriting.
 */
#[repr(C)]
pub struct SetToDefault {
    pub xpr: Expr,
    /* type for substituted value */
    pub typeId: Oid,
    /* typemod for substituted value */
    pub typeMod: int32, // pg_node_attr(query_jumble_ignore)
    /* collation for the substituted value */
    pub collation: Oid, // pg_node_attr(query_jumble_ignore)
    /* token location, or -1 if unknown */
    pub location: ParseLoc,
}

/*
 * Node representing [WHERE] CURRENT OF cursor_name
 *
 * CURRENT OF is a bit like a Var, in that it carries the rangetable index
 * of the target relation being constrained.  The referenced cursor can be
 * represented either as a hardwired string or as a reference to a run-time
 * parameter of type REFCURSOR.
 */
#[repr(C)]
pub struct CurrentOfExpr {
    pub xpr: Expr,
    pub cvarno: Index,           /* RT index of target relation */
    pub cursor_name: *mut c_char, /* name of referenced cursor, or NULL */
    pub cursor_param: c_int,     /* refcursor parameter number, or 0 */
}

/*
 * NextValueExpr - get next value from sequence
 *
 * This has the same effect as calling the nextval() function, but it does not
 * check permissions on the sequence.  This is used for identity columns.
 */
#[repr(C)]
pub struct NextValueExpr {
    pub xpr: Expr,
    pub seqid: Oid,
    pub typeId: Oid,
}

/*
 * InferenceElem - an element of a unique index inference specification
 *
 * This mostly matches the structure of IndexElems, but having a dedicated
 * primnode allows for a clean separation between the use of index parameters
 * by utility commands, and this node.
 */
#[repr(C)]
pub struct InferenceElem {
    pub xpr: Expr,
    pub expr: *mut Node,    /* expression to infer from, or NULL */
    pub infercollid: Oid,   /* OID of collation, or InvalidOid */
    pub inferopclass: Oid,  /* OID of att opclass, or InvalidOid */
}

/*
 * ReturningExpr - return OLD/NEW.(expression) in RETURNING list
 *
 * See the C header: the rewriter wraps the expanded expression in a
 * ReturningExpr, equivalent to "CASE WHEN (OLD/NEW row exists) THEN (expr)
 * ELSE NULL".  These nodes never appear in a parsed Query.
 */
#[repr(C)]
pub struct ReturningExpr {
    pub xpr: Expr,
    pub retlevelsup: c_int, /* > 0 if it belongs to outer query */
    pub retold: bool,       /* true for OLD, false for NEW */
    pub retexpr: *mut Expr,  /* expression to be returned */
}

/*--------------------
 * TargetEntry -
 *	   a target entry (used in query target lists)
 *
 * Strictly speaking, a TargetEntry isn't an expression node, but we treat it
 * as one anyway.  See the C header for the extensive commentary on resno,
 * resname, ressortgroupref, resorigtbl/resorigcol, and resjunk.
 *--------------------
 */
#[repr(C)]
pub struct TargetEntry {
    pub xpr: Expr,
    /* expression to evaluate */
    pub expr: *mut Expr,
    /* attribute number (see notes above) */
    pub resno: AttrNumber,
    /* name of the column (could be NULL) */
    pub resname: *mut c_char, // pg_node_attr(query_jumble_ignore)
    /* nonzero if referenced by a sort/group clause */
    pub ressortgroupref: Index,
    /* OID of column's source table */
    pub resorigtbl: Oid, // pg_node_attr(query_jumble_ignore)
    /* column's number in source table */
    pub resorigcol: AttrNumber, // pg_node_attr(query_jumble_ignore)
    /* set to true to eliminate the attribute from final target list */
    pub resjunk: bool, // pg_node_attr(query_jumble_ignore)
}

/* ----------------------------------------------------------------
 *					node types for join trees
 *
 * The leaves of a join tree structure are RangeTblRef nodes.  Above
 * these, JoinExpr nodes can appear to denote a specific kind of join
 * or qualified join.  Also, FromExpr nodes can appear to denote an
 * ordinary cross-product join.  See the C header for the full commentary
 * on qual placement and the raw-grammar-vs-parse-analysis distinction.
 * ----------------------------------------------------------------
 */

/*
 * RangeTblRef - reference to an entry in the query's rangetable
 *
 * We could use direct pointers to the RT entries and skip having these
 * nodes, but multiple pointers to the same node in a querytree cause
 * lots of headaches, so it seems better to store an index into the RT.
 */
#[repr(C)]
pub struct RangeTblRef {
    pub r#type: NodeTag,
    pub rtindex: c_int,
}

/*----------
 * JoinExpr - for SQL JOIN expressions
 *
 * isNatural, usingClause, and quals are interdependent.  See the C header
 * for the full description of NATURAL/USING/ON, the alias semantics, and the
 * rtindex assigned during parse analysis.
 *----------
 */
#[repr(C)]
pub struct JoinExpr {
    pub r#type: NodeTag,
    pub jointype: JoinType, /* type of join */
    pub isNatural: bool,    /* Natural join? Will need to shape table */
    pub larg: *mut Node,    /* left subtree */
    pub rarg: *mut Node,    /* right subtree */
    /* USING clause, if any (list of String) */
    pub usingClause: *mut List, // pg_node_attr(query_jumble_ignore)
    /* alias attached to USING clause, if any */
    pub join_using_alias: *mut Alias, // pg_node_attr(query_jumble_ignore)
    /* qualifiers on join, if any */
    pub quals: *mut Node,
    /* user-written alias clause, if any */
    pub alias: *mut Alias, // pg_node_attr(query_jumble_ignore)
    /* RT index assigned for join, or 0 */
    pub rtindex: c_int,
}

/*----------
 * FromExpr - represents a FROM ... WHERE ... construct
 *
 * This is both more flexible than a JoinExpr (it can have any number of
 * children, including zero) and less so --- we don't need to deal with
 * aliases and so on.  The output column set is implicitly just the union
 * of the outputs of the children.
 *----------
 */
#[repr(C)]
pub struct FromExpr {
    pub r#type: NodeTag,
    pub fromlist: *mut List, /* List of join subtrees */
    pub quals: *mut Node,    /* qualifiers on join, if any */
}

/*----------
 * OnConflictExpr - represents an ON CONFLICT DO ... expression
 *
 * The optimizer requires a list of inference elements, and optionally a WHERE
 * clause to infer a unique index.  The unique index (or, occasionally,
 * indexes) inferred are used to arbitrate whether or not the alternative ON
 * CONFLICT path is taken.
 *----------
 */
#[repr(C)]
pub struct OnConflictExpr {
    pub r#type: NodeTag,
    pub action: OnConflictAction, /* DO NOTHING or UPDATE? */

    /* Arbiter */
    pub arbiterElems: *mut List, /* unique index arbiter list (of
                                  * InferenceElem's) */
    pub arbiterWhere: *mut Node, /* unique index arbiter WHERE clause */
    pub constraint: Oid,         /* pg_constraint OID for arbiter */

    /* ON CONFLICT UPDATE */
    pub onConflictSet: *mut List, /* List of ON CONFLICT SET TargetEntrys */
    pub onConflictWhere: *mut Node, /* qualifiers to restrict UPDATE to */
    pub exclRelIndex: c_int,      /* RT index of 'excluded' relation */
    pub exclRelTlist: *mut List,  /* tlist of the EXCLUDED pseudo relation */
}
