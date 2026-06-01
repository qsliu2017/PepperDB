//! Translated from PostgreSQL 18.3 `src/include/nodes/parsenodes.h`.
//!
//! Definitions for parse tree nodes.
//!
//! Many of the node types used in parsetrees include a "location" field.
//! This is a byte (not character) offset in the original source text, to be
//! used for positioning an error cursor when there is an error related to
//! the node.  Access to the original source text is needed to make use of
//! the location.  At the topmost (statement) level, we also provide a
//! statement length, likewise measured in bytes, for convenience in
//! identifying statement boundaries in multi-statement source strings.
//!
//! The copy/equal/out/read support functions are generated elsewhere; this
//! file holds only the node/enum definitions, verbatim from the C header.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*; // Oid, Datum, int*, bits32, Index, SubTransactionId, c_char/c_int/c_long/c_void, PG_INT32_MAX, etc.
use crate::nodes::nodes::{Node, NodeTag, ParseLoc, CmdType, JoinType, OnConflictAction, SetOpCmd, SetOpStrategy, LimitOption};
use crate::nodes::nodes::Cardinality; // Cardinality = f64 (RangeTblEntry.enrtuples)
use crate::nodes::pg_list::List;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::primnodes::*;
use crate::nodes::lockoptions::{LockClauseStrength, LockWaitPolicy}; // REAL, already translated

// Value-node types (nodes/value.h) referenced by ValUnion / String* fields.
use crate::nodes::value::{Integer, Float, Boolean, String, BitString};

// ----------------------------------------------------------------
//  Forward stubs for cross-header types not yet translated.
// ----------------------------------------------------------------

/// TODO(pg-port): real def `typedef Oid RelFileNumber` in common/relpath.h.
pub type RelFileNumber = Oid;

/// Grantable rights are encoded so that we can OR them together in a bitmask.
/// The present representation of AclItem limits us to 32 distinct rights,
/// even though AclMode is defined as uint64.  See utils/acl.h.
///
/// Caution: changing these codes breaks stored ACLs, hence forces initdb.
pub type AclMode = uint64; /* a bitmask of privilege bits */

pub const ACL_INSERT: AclMode = 1 << 0; /* for relations */
pub const ACL_SELECT: AclMode = 1 << 1;
pub const ACL_UPDATE: AclMode = 1 << 2;
pub const ACL_DELETE: AclMode = 1 << 3;
pub const ACL_TRUNCATE: AclMode = 1 << 4;
pub const ACL_REFERENCES: AclMode = 1 << 5;
pub const ACL_TRIGGER: AclMode = 1 << 6;
pub const ACL_EXECUTE: AclMode = 1 << 7; /* for functions */
pub const ACL_USAGE: AclMode = 1 << 8; /* for various object types */
pub const ACL_CREATE: AclMode = 1 << 9; /* for namespaces and databases */
pub const ACL_CREATE_TEMP: AclMode = 1 << 10; /* for databases */
pub const ACL_CONNECT: AclMode = 1 << 11; /* for databases */
pub const ACL_SET: AclMode = 1 << 12; /* for configuration parameters */
pub const ACL_ALTER_SYSTEM: AclMode = 1 << 13; /* for configuration parameters */
pub const ACL_MAINTAIN: AclMode = 1 << 14; /* for relations */
pub const N_ACL_RIGHTS: c_int = 15; /* 1 plus the last 1<<x */
pub const ACL_NO_RIGHTS: AclMode = 0;
/* Currently, SELECT ... FOR [KEY] UPDATE/SHARE requires UPDATE privileges */
pub const ACL_SELECT_FOR_UPDATE: AclMode = ACL_UPDATE;

// ----------------------------------------------------------------
//  Partitioning forward stubs (partitioning/partdefs.h).  Only the
//  PartitionBoundSpec is defined for real below (it lives in this header);
//  the others (PartitionSpec, PartitionRangeDatum, PartitionElem, ...) are
//  full definitions here too.  No opaque stubs are required.
// ----------------------------------------------------------------

/* Possible sources of a Query */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QuerySource {
    QSRC_ORIGINAL,         /* original parsetree (explicit query) */
    QSRC_PARSER,           /* added by parse analysis (now unused) */
    QSRC_INSTEAD_RULE,     /* added by unconditional INSTEAD rule */
    QSRC_QUAL_INSTEAD_RULE, /* added by conditional INSTEAD rule */
    QSRC_NON_INSTEAD_RULE, /* added by non-INSTEAD rule */
}
pub use QuerySource::*;

/* Sort ordering options for ORDER BY and CREATE INDEX */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SortByDir {
    SORTBY_DEFAULT,
    SORTBY_ASC,
    SORTBY_DESC,
    SORTBY_USING, /* not allowed in CREATE INDEX ... */
}
pub use SortByDir::*;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SortByNulls {
    SORTBY_NULLS_DEFAULT,
    SORTBY_NULLS_FIRST,
    SORTBY_NULLS_LAST,
}
pub use SortByNulls::*;

/* Options for [ ALL | DISTINCT ] */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetQuantifier {
    SET_QUANTIFIER_DEFAULT,
    SET_QUANTIFIER_ALL,
    SET_QUANTIFIER_DISTINCT,
}
pub use SetQuantifier::*;

/*****************************************************************************
 *	Query Tree
 *****************************************************************************/

/*
 * Query -
 *	  Parse analysis turns all statements into a Query tree
 *	  for further processing by the rewriter and planner.
 *
 *	  Utility statements (i.e. non-optimizable statements) have the
 *	  utilityStmt field set, and the rest of the Query is mostly dummy.
 *
 *	  Planning converts a Query tree into a Plan tree headed by a PlannedStmt
 *	  node --- the Query structure is not used by the executor.
 *
 *	  All the fields ignored for the query jumbling are not semantically
 *	  significant (such as alias names), as is ignored anything that can
 *	  be deduced from child nodes (else we'd just be double-hashing that
 *	  piece of information).
 */
#[repr(C)]
pub struct Query {
    pub r#type: NodeTag,

    pub commandType: CmdType, /* select|insert|update|delete|merge|utility */

    /* where did I come from? */
    pub querySource: QuerySource, // pg_node_attr(query_jumble_ignore)

    /*
     * query identifier (can be set by plugins); ignored for equal, as it
     * might not be set; also not stored.  This is the result of the query
     * jumble, hence ignored.
     *
     * We store this as a signed value as this is the form it's displayed to
     * users in places such as EXPLAIN and pg_stat_statements.  Primarily this
     * is done due to lack of an SQL type to represent the full range of
     * uint64.
     */
    pub queryId: int64, // pg_node_attr(equal_ignore, query_jumble_ignore, read_write_ignore, read_as(0))

    /* do I set the command result tag? */
    pub canSetTag: bool, // pg_node_attr(query_jumble_ignore)

    pub utilityStmt: *mut Node, /* non-null if commandType == CMD_UTILITY */

    /*
     * rtable index of target relation for INSERT/UPDATE/DELETE/MERGE; 0 for
     * SELECT.  This is ignored in the query jumble as unrelated to the
     * compilation of the query ID.
     */
    pub resultRelation: c_int, // pg_node_attr(query_jumble_ignore)

    /* has aggregates in tlist or havingQual */
    pub hasAggs: bool, // pg_node_attr(query_jumble_ignore)
    /* has window functions in tlist */
    pub hasWindowFuncs: bool, // pg_node_attr(query_jumble_ignore)
    /* has set-returning functions in tlist */
    pub hasTargetSRFs: bool, // pg_node_attr(query_jumble_ignore)
    /* has subquery SubLink */
    pub hasSubLinks: bool, // pg_node_attr(query_jumble_ignore)
    /* distinctClause is from DISTINCT ON */
    pub hasDistinctOn: bool, // pg_node_attr(query_jumble_ignore)
    /* WITH RECURSIVE was specified */
    pub hasRecursive: bool, // pg_node_attr(query_jumble_ignore)
    /* has INSERT/UPDATE/DELETE/MERGE in WITH */
    pub hasModifyingCTE: bool, // pg_node_attr(query_jumble_ignore)
    /* FOR [KEY] UPDATE/SHARE was specified */
    pub hasForUpdate: bool, // pg_node_attr(query_jumble_ignore)
    /* rewriter has applied some RLS policy */
    pub hasRowSecurity: bool, // pg_node_attr(query_jumble_ignore)
    /* parser has added an RTE_GROUP RTE */
    pub hasGroupRTE: bool, // pg_node_attr(query_jumble_ignore)
    /* is a RETURN statement */
    pub isReturn: bool, // pg_node_attr(query_jumble_ignore)

    pub cteList: *mut List, /* WITH list (of CommonTableExpr's) */

    pub rtable: *mut List, /* list of range table entries */

    /*
     * list of RTEPermissionInfo nodes for the rtable entries having
     * perminfoindex > 0
     */
    pub rteperminfos: *mut List, // pg_node_attr(query_jumble_ignore)
    pub jointree: *mut FromExpr, /* table join tree (FROM and WHERE clauses);
                                  * also USING clause for MERGE */

    pub mergeActionList: *mut List, /* list of actions for MERGE (only) */

    /*
     * rtable index of target relation for MERGE to pull data. Initially, this
     * is the same as resultRelation, but after query rewriting, if the target
     * relation is a trigger-updatable view, this is the index of the expanded
     * view subquery, whereas resultRelation is the index of the target view.
     */
    pub mergeTargetRelation: c_int, // pg_node_attr(query_jumble_ignore)

    /* join condition between source and target for MERGE */
    pub mergeJoinCondition: *mut Node,

    pub targetList: *mut List, /* target list (of TargetEntry) */

    /* OVERRIDING clause */
    pub r#override: OverridingKind, // pg_node_attr(query_jumble_ignore)

    pub onConflict: *mut OnConflictExpr, /* ON CONFLICT DO [NOTHING | UPDATE] */

    /*
     * The following three fields describe the contents of the RETURNING list
     * for INSERT/UPDATE/DELETE/MERGE. returningOldAlias and returningNewAlias
     * are the alias names for OLD and NEW, which may be user-supplied values,
     * the defaults "old" and "new", or NULL (if the default "old"/"new" is
     * already in use as the alias for some other relation).
     */
    pub returningOldAlias: *mut c_char, // pg_node_attr(query_jumble_ignore)
    pub returningNewAlias: *mut c_char, // pg_node_attr(query_jumble_ignore)
    pub returningList: *mut List, /* return-values list (of TargetEntry) */

    pub groupClause: *mut List, /* a list of SortGroupClause's */
    pub groupDistinct: bool,    /* is the group by clause distinct? */

    pub groupingSets: *mut List, /* a list of GroupingSet's if present */

    pub havingQual: *mut Node, /* qualifications applied to groups */

    pub windowClause: *mut List, /* a list of WindowClause's */

    pub distinctClause: *mut List, /* a list of SortGroupClause's */

    pub sortClause: *mut List, /* a list of SortGroupClause's */

    pub limitOffset: *mut Node,  /* # of result tuples to skip (int8 expr) */
    pub limitCount: *mut Node,   /* # of result tuples to return (int8 expr) */
    pub limitOption: LimitOption, /* limit type */

    pub rowMarks: *mut List, /* a list of RowMarkClause's */

    pub setOperations: *mut Node, /* set-operation tree if this is top level of
                                   * a UNION/INTERSECT/EXCEPT query */

    /*
     * A list of pg_constraint OIDs that the query depends on to be
     * semantically valid
     */
    pub constraintDeps: *mut List, // pg_node_attr(query_jumble_ignore)

    /* a list of WithCheckOption's (added during rewrite) */
    pub withCheckOptions: *mut List, // pg_node_attr(query_jumble_ignore)

    /*
     * The following two fields identify the portion of the source text string
     * containing this query.  They are typically only populated in top-level
     * Queries, not in sub-queries.  When not set, they might both be zero, or
     * both be -1 meaning "unknown".
     */
    /* start location, or -1 if unknown */
    pub stmt_location: ParseLoc,
    /* length in bytes; 0 means "rest of string" */
    pub stmt_len: ParseLoc, // pg_node_attr(query_jumble_ignore)
}

/****************************************************************************
 *	Supporting data structures for Parse Trees
 *
 *	Most of these node types appear in raw parsetrees output by the grammar,
 *	and get transformed to something else by the analyzer.  A few of them
 *	are used as-is in transformed querytrees.
 ****************************************************************************/

/*
 * TypeName - specifies a type in definitions
 *
 * For TypeName structures generated internally, it is often easier to
 * specify the type by OID than by name.  If "names" is NIL then the
 * actual type OID is given by typeOid, otherwise typeOid is unused.
 * Similarly, if "typmods" is NIL then the actual typmod is expected to
 * be prespecified in typemod, otherwise typemod is unused.
 *
 * If pct_type is true, then names is actually a field name and we look up
 * the type of that field.  Otherwise (the normal case), names is a type
 * name possibly qualified with schema and database name.
 */
#[repr(C)]
pub struct TypeName {
    pub r#type: NodeTag,
    pub names: *mut List,      /* qualified name (list of String nodes) */
    pub typeOid: Oid,          /* type identified by OID */
    pub setof: bool,           /* is a set? */
    pub pct_type: bool,        /* %TYPE specified? */
    pub typmods: *mut List,    /* type modifier expression(s) */
    pub typemod: int32,        /* prespecified type modifier */
    pub arrayBounds: *mut List, /* array bounds */
    pub location: ParseLoc,    /* token location, or -1 if unknown */
}

/*
 * ColumnRef - specifies a reference to a column, or possibly a whole tuple
 *
 * The "fields" list must be nonempty.  It can contain String nodes
 * (representing names) and A_Star nodes (representing occurrence of a '*').
 * Currently, A_Star must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 *
 * Note: any container subscripting or selection of fields from composite columns
 * is represented by an A_Indirection node above the ColumnRef.  However,
 * for simplicity in the normal case, initial field selection from a table
 * name is represented within ColumnRef and not by adding A_Indirection.
 */
#[repr(C)]
pub struct ColumnRef {
    pub r#type: NodeTag,
    pub fields: *mut List,  /* field names (String nodes) or A_Star */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * ParamRef - specifies a $n parameter reference
 */
#[repr(C)]
pub struct ParamRef {
    pub r#type: NodeTag,
    pub number: c_int,      /* the number of the parameter */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * A_Expr - infix, prefix, and postfix expressions
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum A_Expr_Kind {
    AEXPR_OP,           /* normal operator */
    AEXPR_OP_ANY,       /* scalar op ANY (array) */
    AEXPR_OP_ALL,       /* scalar op ALL (array) */
    AEXPR_DISTINCT,     /* IS DISTINCT FROM - name must be "=" */
    AEXPR_NOT_DISTINCT, /* IS NOT DISTINCT FROM - name must be "=" */
    AEXPR_NULLIF,       /* NULLIF - name must be "=" */
    AEXPR_IN,           /* [NOT] IN - name must be "=" or "<>" */
    AEXPR_LIKE,         /* [NOT] LIKE - name must be "~~" or "!~~" */
    AEXPR_ILIKE,        /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
    AEXPR_SIMILAR,      /* [NOT] SIMILAR - name must be "~" or "!~" */
    AEXPR_BETWEEN,      /* name must be "BETWEEN" */
    AEXPR_NOT_BETWEEN,  /* name must be "NOT BETWEEN" */
    AEXPR_BETWEEN_SYM,  /* name must be "BETWEEN SYMMETRIC" */
    AEXPR_NOT_BETWEEN_SYM, /* name must be "NOT BETWEEN SYMMETRIC" */
}
pub use A_Expr_Kind::*;

#[repr(C)]
pub struct A_Expr {
    // pg_node_attr(custom_read_write)
    pub r#type: NodeTag,
    pub kind: A_Expr_Kind, /* see above */
    pub name: *mut List,   /* possibly-qualified name of operator */
    pub lexpr: *mut Node,  /* left argument, or NULL if none */
    pub rexpr: *mut Node,  /* right argument, or NULL if none */

    /*
     * If rexpr is a list of some kind, we separately track its starting and
     * ending location; it's not the same as the starting and ending location
     * of the token itself.
     */
    pub rexpr_list_start: ParseLoc,
    pub rexpr_list_end: ParseLoc,
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * A_Const - a literal constant
 *
 * Value nodes are inline for performance.  You can treat 'val' as a node,
 * as in IsA(&val, Integer).  'val' is not valid if isnull is true.
 */
// `union ValUnion` from A_Const.  Each arm embeds a whole Value node by value;
// Rust requires non-`Copy` union fields to be wrapped in `ManuallyDrop` (which
// is `#[repr(transparent)]`, so the C ABI/layout is preserved).
#[repr(C)]
pub union ValUnion {
    pub node: core::mem::ManuallyDrop<Node>,
    pub ival: core::mem::ManuallyDrop<Integer>,
    pub fval: core::mem::ManuallyDrop<Float>,
    pub boolval: core::mem::ManuallyDrop<Boolean>,
    pub sval: core::mem::ManuallyDrop<String>,
    pub bsval: core::mem::ManuallyDrop<BitString>,
}

#[repr(C)]
pub struct A_Const {
    // pg_node_attr(custom_copy_equal, custom_read_write, custom_query_jumble)
    pub r#type: NodeTag,
    pub val: ValUnion,
    pub isnull: bool,       /* SQL NULL constant */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * TypeCast - a CAST expression
 */
#[repr(C)]
pub struct TypeCast {
    pub r#type: NodeTag,
    pub arg: *mut Node,       /* the expression being casted */
    pub typeName: *mut TypeName, /* the target type */
    pub location: ParseLoc,   /* token location, or -1 if unknown */
}

/*
 * CollateClause - a COLLATE expression
 */
#[repr(C)]
pub struct CollateClause {
    pub r#type: NodeTag,
    pub arg: *mut Node,     /* input expression */
    pub collname: *mut List, /* possibly-qualified collation name */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * RoleSpec - a role name or one of a few special values.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RoleSpecType {
    ROLESPEC_CSTRING,      /* role name is stored as a C string */
    ROLESPEC_CURRENT_ROLE, /* role spec is CURRENT_ROLE */
    ROLESPEC_CURRENT_USER, /* role spec is CURRENT_USER */
    ROLESPEC_SESSION_USER, /* role spec is SESSION_USER */
    ROLESPEC_PUBLIC,       /* role name is "public" */
}
pub use RoleSpecType::*;

#[repr(C)]
pub struct RoleSpec {
    pub r#type: NodeTag,
    pub roletype: RoleSpecType, /* Type of this rolespec */
    pub rolename: *mut c_char,  /* filled only for ROLESPEC_CSTRING */
    pub location: ParseLoc,     /* token location, or -1 if unknown */
}

/*
 * FuncCall - a function or aggregate invocation
 *
 * agg_order (if not NIL) indicates we saw 'foo(... ORDER BY ...)', or if
 * agg_within_group is true, it was 'foo(...) WITHIN GROUP (ORDER BY ...)'.
 * agg_star indicates we saw a 'foo(*)' construct, while agg_distinct
 * indicates we saw 'foo(DISTINCT ...)'.  In any of these cases, the
 * construct *must* be an aggregate call.  Otherwise, it might be either an
 * aggregate or some other kind of function.  However, if FILTER or OVER is
 * present it had better be an aggregate or window function.
 *
 * Normally, you'd initialize this via makeFuncCall() and then only change the
 * parts of the struct its defaults don't match afterwards, as needed.
 */
#[repr(C)]
pub struct FuncCall {
    pub r#type: NodeTag,
    pub funcname: *mut List,  /* qualified name of function */
    pub args: *mut List,      /* the arguments (list of exprs) */
    pub agg_order: *mut List, /* ORDER BY (list of SortBy) */
    pub agg_filter: *mut Node, /* FILTER clause, if any */
    pub over: *mut WindowDef, /* OVER clause, if any */
    pub agg_within_group: bool, /* ORDER BY appeared in WITHIN GROUP */
    pub agg_star: bool,       /* argument was really '*' */
    pub agg_distinct: bool,   /* arguments were labeled DISTINCT */
    pub func_variadic: bool,  /* last argument was labeled VARIADIC */
    pub funcformat: CoercionForm, /* how to display this node */
    pub location: ParseLoc,   /* token location, or -1 if unknown */
}

/*
 * A_Star - '*' representing all columns of a table or compound field
 *
 * This can appear within ColumnRef.fields, A_Indirection.indirection, and
 * ResTarget.indirection lists.
 */
#[repr(C)]
pub struct A_Star {
    pub r#type: NodeTag,
}

/*
 * A_Indices - array subscript or slice bounds ([idx] or [lidx:uidx])
 *
 * In slice case, either or both of lidx and uidx can be NULL (omitted).
 * In non-slice case, uidx holds the single subscript and lidx is always NULL.
 */
#[repr(C)]
pub struct A_Indices {
    pub r#type: NodeTag,
    pub is_slice: bool, /* true if slice (i.e., colon present) */
    pub lidx: *mut Node, /* slice lower bound, if any */
    pub uidx: *mut Node, /* subscript, or slice upper bound if any */
}

/*
 * A_Indirection - select a field and/or array element from an expression
 *
 * The indirection list can contain A_Indices nodes (representing
 * subscripting), String nodes (representing field selection --- the
 * string value is the name of the field to select), and A_Star nodes
 * (representing selection of all fields of a composite type).
 * For example, a complex selection operation like
 *				(foo).field1[42][7].field2
 * would be represented with a single A_Indirection node having a 4-element
 * indirection list.
 *
 * Currently, A_Star must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 */
#[repr(C)]
pub struct A_Indirection {
    pub r#type: NodeTag,
    pub arg: *mut Node,         /* the thing being selected from */
    pub indirection: *mut List, /* subscripts and/or field names and/or * */
}

/*
 * A_ArrayExpr - an ARRAY[] construct
 */
#[repr(C)]
pub struct A_ArrayExpr {
    pub r#type: NodeTag,
    pub elements: *mut List, /* array element expressions */
    pub list_start: ParseLoc, /* start of the element list */
    pub list_end: ParseLoc,  /* end of the elements list */
    pub location: ParseLoc,  /* token location, or -1 if unknown */
}

/*
 * ResTarget -
 *	  result target (used in target list of pre-transformed parse trees)
 *
 * In a SELECT target list, 'name' is the column label from an
 * 'AS ColumnLabel' clause, or NULL if there was none, and 'val' is the
 * value expression itself.  The 'indirection' field is not used.
 *
 * INSERT uses ResTarget in its target-column-names list.  Here, 'name' is
 * the name of the destination column, 'indirection' stores any subscripts
 * attached to the destination, and 'val' is not used.
 *
 * In an UPDATE target list, 'name' is the name of the destination column,
 * 'indirection' stores any subscripts attached to the destination, and
 * 'val' is the expression to assign.
 *
 * See A_Indirection for more info about what can appear in 'indirection'.
 */
#[repr(C)]
pub struct ResTarget {
    pub r#type: NodeTag,
    pub name: *mut c_char,      /* column name or NULL */
    pub indirection: *mut List, /* subscripts, field names, and '*', or NIL */
    pub val: *mut Node,         /* the value expression to compute or assign */
    pub location: ParseLoc,     /* token location, or -1 if unknown */
}

/*
 * MultiAssignRef - element of a row source expression for UPDATE
 *
 * In an UPDATE target list, when we have SET (a,b,c) = row-valued-expression,
 * we generate separate ResTarget items for each of a,b,c.  Their "val" trees
 * are MultiAssignRef nodes numbered 1..n, linking to a common copy of the
 * row-valued-expression (which parse analysis will process only once, when
 * handling the MultiAssignRef with colno=1).
 */
#[repr(C)]
pub struct MultiAssignRef {
    pub r#type: NodeTag,
    pub source: *mut Node, /* the row-valued expression */
    pub colno: c_int,      /* column number for this target (1..n) */
    pub ncolumns: c_int,   /* number of targets in the construct */
}

/*
 * SortBy - for ORDER BY clause
 */
#[repr(C)]
pub struct SortBy {
    pub r#type: NodeTag,
    pub node: *mut Node,         /* expression to sort on */
    pub sortby_dir: SortByDir,   /* ASC/DESC/USING/default */
    pub sortby_nulls: SortByNulls, /* NULLS FIRST/LAST */
    pub useOp: *mut List,        /* name of op to use, if SORTBY_USING */
    pub location: ParseLoc,      /* operator location, or -1 if none/unknown */
}

/*
 * WindowDef - raw representation of WINDOW and OVER clauses
 *
 * For entries in a WINDOW list, "name" is the window name being defined.
 * For OVER clauses, we use "name" for the "OVER window" syntax, or "refname"
 * for the "OVER (window)" syntax, which is subtly different --- the latter
 * implies overriding the window frame clause.
 */
#[repr(C)]
pub struct WindowDef {
    pub r#type: NodeTag,
    pub name: *mut c_char,    /* window's own name */
    pub refname: *mut c_char, /* referenced window name, if any */
    pub partitionClause: *mut List, /* PARTITION BY expression list */
    pub orderClause: *mut List, /* ORDER BY (list of SortBy) */
    pub frameOptions: c_int,  /* frame_clause options, see below */
    pub startOffset: *mut Node, /* expression for starting bound, if any */
    pub endOffset: *mut Node, /* expression for ending bound, if any */
    pub location: ParseLoc,   /* parse location, or -1 if none/unknown */
}

/*
 * frameOptions is an OR of these bits.  The NONDEFAULT and BETWEEN bits are
 * used so that ruleutils.c can tell which properties were specified and
 * which were defaulted; the correct behavioral bits must be set either way.
 * The START_foo and END_foo options must come in pairs of adjacent bits for
 * the convenience of gram.y, even though some of them are useless/invalid.
 */
pub const FRAMEOPTION_NONDEFAULT: c_int = 0x00001; /* any specified? */
pub const FRAMEOPTION_RANGE: c_int = 0x00002; /* RANGE behavior */
pub const FRAMEOPTION_ROWS: c_int = 0x00004; /* ROWS behavior */
pub const FRAMEOPTION_GROUPS: c_int = 0x00008; /* GROUPS behavior */
pub const FRAMEOPTION_BETWEEN: c_int = 0x00010; /* BETWEEN given? */
pub const FRAMEOPTION_START_UNBOUNDED_PRECEDING: c_int = 0x00020; /* start is U. P. */
pub const FRAMEOPTION_END_UNBOUNDED_PRECEDING: c_int = 0x00040; /* (disallowed) */
pub const FRAMEOPTION_START_UNBOUNDED_FOLLOWING: c_int = 0x00080; /* (disallowed) */
pub const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: c_int = 0x00100; /* end is U. F. */
pub const FRAMEOPTION_START_CURRENT_ROW: c_int = 0x00200; /* start is C. R. */
pub const FRAMEOPTION_END_CURRENT_ROW: c_int = 0x00400; /* end is C. R. */
pub const FRAMEOPTION_START_OFFSET_PRECEDING: c_int = 0x00800; /* start is O. P. */
pub const FRAMEOPTION_END_OFFSET_PRECEDING: c_int = 0x01000; /* end is O. P. */
pub const FRAMEOPTION_START_OFFSET_FOLLOWING: c_int = 0x02000; /* start is O. F. */
pub const FRAMEOPTION_END_OFFSET_FOLLOWING: c_int = 0x04000; /* end is O. F. */
pub const FRAMEOPTION_EXCLUDE_CURRENT_ROW: c_int = 0x08000; /* omit C.R. */
pub const FRAMEOPTION_EXCLUDE_GROUP: c_int = 0x10000; /* omit C.R. & peers */
pub const FRAMEOPTION_EXCLUDE_TIES: c_int = 0x20000; /* omit C.R.'s peers */

pub const FRAMEOPTION_START_OFFSET: c_int =
    FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING;
pub const FRAMEOPTION_END_OFFSET: c_int =
    FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING;
pub const FRAMEOPTION_EXCLUSION: c_int =
    FRAMEOPTION_EXCLUDE_CURRENT_ROW | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES;

pub const FRAMEOPTION_DEFAULTS: c_int =
    FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW;

/*
 * RangeSubselect - subquery appearing in a FROM clause
 */
#[repr(C)]
pub struct RangeSubselect {
    pub r#type: NodeTag,
    pub lateral: bool,      /* does it have LATERAL prefix? */
    pub subquery: *mut Node, /* the untransformed sub-select clause */
    pub alias: *mut Alias,  /* table alias & optional column aliases */
}

/*
 * RangeFunction - function call appearing in a FROM clause
 *
 * functions is a List because we use this to represent the construct
 * ROWS FROM(func1(...), func2(...), ...).  Each element of this list is a
 * two-element sublist, the first element being the untransformed function
 * call tree, and the second element being a possibly-empty list of ColumnDef
 * nodes representing any columndef list attached to that function within the
 * ROWS FROM() syntax.
 *
 * alias and coldeflist represent any alias and/or columndef list attached
 * at the top level.  (We disallow coldeflist appearing both here and
 * per-function, but that's checked in parse analysis, not by the grammar.)
 */
#[repr(C)]
pub struct RangeFunction {
    pub r#type: NodeTag,
    pub lateral: bool,        /* does it have LATERAL prefix? */
    pub ordinality: bool,     /* does it have WITH ORDINALITY suffix? */
    pub is_rowsfrom: bool,    /* is result of ROWS FROM() syntax? */
    pub functions: *mut List, /* per-function information, see above */
    pub alias: *mut Alias,    /* table alias & optional column aliases */
    pub coldeflist: *mut List, /* list of ColumnDef nodes to describe result
                               * of function returning RECORD */
}

/*
 * RangeTableFunc - raw form of "table functions" such as XMLTABLE
 *
 * Note: JSON_TABLE is also a "table function", but it uses JsonTable node,
 * not RangeTableFunc.
 */
#[repr(C)]
pub struct RangeTableFunc {
    pub r#type: NodeTag,
    pub lateral: bool,        /* does it have LATERAL prefix? */
    pub docexpr: *mut Node,   /* document expression */
    pub rowexpr: *mut Node,   /* row generator expression */
    pub namespaces: *mut List, /* list of namespaces as ResTarget */
    pub columns: *mut List,   /* list of RangeTableFuncCol */
    pub alias: *mut Alias,    /* table alias & optional column aliases */
    pub location: ParseLoc,   /* token location, or -1 if unknown */
}

/*
 * RangeTableFuncCol - one column in a RangeTableFunc->columns
 *
 * If for_ordinality is true (FOR ORDINALITY), then the column is an int4
 * column and the rest of the fields are ignored.
 */
#[repr(C)]
pub struct RangeTableFuncCol {
    pub r#type: NodeTag,
    pub colname: *mut c_char,    /* name of generated column */
    pub typeName: *mut TypeName, /* type of generated column */
    pub for_ordinality: bool,    /* does it have FOR ORDINALITY? */
    pub is_not_null: bool,       /* does it have NOT NULL? */
    pub colexpr: *mut Node,      /* column filter expression */
    pub coldefexpr: *mut Node,   /* column default value expression */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * RangeTableSample - TABLESAMPLE appearing in a raw FROM clause
 *
 * This node, appearing only in raw parse trees, represents
 *		<relation> TABLESAMPLE <method> (<params>) REPEATABLE (<num>)
 * Currently, the <relation> can only be a RangeVar, but we might in future
 * allow RangeSubselect and other options.  Note that the RangeTableSample
 * is wrapped around the node representing the <relation>, rather than being
 * a subfield of it.
 */
#[repr(C)]
pub struct RangeTableSample {
    pub r#type: NodeTag,
    pub relation: *mut Node, /* relation to be sampled */
    pub method: *mut List,   /* sampling method name (possibly qualified) */
    pub args: *mut List,     /* argument(s) for sampling method */
    pub repeatable: *mut Node, /* REPEATABLE expression, or NULL if none */
    pub location: ParseLoc,  /* method name location, or -1 if unknown */
}

/*
 * ColumnDef - column definition (used in various creates)
 *
 * If the column has a default value, we may have the value expression
 * in either "raw" form (an untransformed parse tree) or "cooked" form
 * (a post-parse-analysis, executable expression tree), depending on
 * how this ColumnDef node was created (by parsing, or by inheritance
 * from an existing relation).  We should never have both in the same node!
 *
 * Similarly, we may have a COLLATE specification in either raw form
 * (represented as a CollateClause with arg==NULL) or cooked form
 * (the collation's OID).
 *
 * The constraints list may contain a CONSTR_DEFAULT item in a raw
 * parsetree produced by gram.y, but transformCreateStmt will remove
 * the item and set raw_default instead.  CONSTR_DEFAULT items
 * should not appear in any subsequent processing.
 */
#[repr(C)]
pub struct ColumnDef {
    pub r#type: NodeTag,
    pub colname: *mut c_char,    /* name of column */
    pub typeName: *mut TypeName, /* type of column */
    pub compression: *mut c_char, /* compression method for column */
    pub inhcount: int16,         /* number of times column is inherited */
    pub is_local: bool,          /* column has local (non-inherited) def'n */
    pub is_not_null: bool,       /* NOT NULL constraint specified? */
    pub is_from_type: bool,      /* column definition came from table type */
    pub storage: c_char,         /* attstorage setting, or 0 for default */
    pub storage_name: *mut c_char, /* attstorage setting name or NULL for default */
    pub raw_default: *mut Node,  /* default value (untransformed parse tree) */
    pub cooked_default: *mut Node, /* default value (transformed expr tree) */
    pub identity: c_char,        /* attidentity setting */
    pub identitySequence: *mut RangeVar, /* to store identity sequence name for
                                          * ALTER TABLE ... ADD COLUMN */
    pub generated: c_char,       /* attgenerated setting */
    pub collClause: *mut CollateClause, /* untransformed COLLATE spec, if any */
    pub collOid: Oid,            /* collation OID (InvalidOid if not set) */
    pub constraints: *mut List,  /* other constraints on column */
    pub fdwoptions: *mut List,   /* per-column FDW options */
    pub location: ParseLoc,      /* parse location, or -1 if none/unknown */
}

/*
 * TableLikeClause - CREATE TABLE ( ... LIKE ... ) clause
 */
#[repr(C)]
pub struct TableLikeClause {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar,
    pub options: bits32, /* OR of TableLikeOption flags */
    pub relationOid: Oid, /* If table has been looked up, its OID */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TableLikeOption {
    CREATE_TABLE_LIKE_COMMENTS = 1 << 0,
    CREATE_TABLE_LIKE_COMPRESSION = 1 << 1,
    CREATE_TABLE_LIKE_CONSTRAINTS = 1 << 2,
    CREATE_TABLE_LIKE_DEFAULTS = 1 << 3,
    CREATE_TABLE_LIKE_GENERATED = 1 << 4,
    CREATE_TABLE_LIKE_IDENTITY = 1 << 5,
    CREATE_TABLE_LIKE_INDEXES = 1 << 6,
    CREATE_TABLE_LIKE_STATISTICS = 1 << 7,
    CREATE_TABLE_LIKE_STORAGE = 1 << 8,
    CREATE_TABLE_LIKE_ALL = PG_INT32_MAX as isize,
}
pub use TableLikeOption::*;

/*
 * IndexElem - index parameters (used in CREATE INDEX, and in ON CONFLICT)
 *
 * For a plain index attribute, 'name' is the name of the table column to
 * index, and 'expr' is NULL.  For an index expression, 'name' is NULL and
 * 'expr' is the expression tree.
 */
#[repr(C)]
pub struct IndexElem {
    pub r#type: NodeTag,
    pub name: *mut c_char,    /* name of attribute to index, or NULL */
    pub expr: *mut Node,      /* expression to index, or NULL */
    pub indexcolname: *mut c_char, /* name for index column; NULL = default */
    pub collation: *mut List, /* name of collation; NIL = default */
    pub opclass: *mut List,   /* name of desired opclass; NIL = default */
    pub opclassopts: *mut List, /* opclass-specific options, or NIL */
    pub ordering: SortByDir,  /* ASC/DESC/default */
    pub nulls_ordering: SortByNulls, /* FIRST/LAST/default */
}

/*
 * DefElem - a generic "name = value" option definition
 *
 * In some contexts the name can be qualified.  Also, certain SQL commands
 * allow a SET/ADD/DROP action to be attached to option settings, so it's
 * convenient to carry a field for that too.  (Note: currently, it is our
 * practice that the grammar allows namespace and action only in statements
 * where they are relevant; C code can just ignore those fields in other
 * statements.)
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DefElemAction {
    DEFELEM_UNSPEC, /* no action given */
    DEFELEM_SET,
    DEFELEM_ADD,
    DEFELEM_DROP,
}
pub use DefElemAction::*;

#[repr(C)]
pub struct DefElem {
    pub r#type: NodeTag,
    pub defnamespace: *mut c_char, /* NULL if unqualified name */
    pub defname: *mut c_char,
    pub arg: *mut Node, /* typically Integer, Float, String, or
                         * TypeName */
    pub defaction: DefElemAction, /* unspecified action, or SET/ADD/DROP */
    pub location: ParseLoc,       /* token location, or -1 if unknown */
}

/*
 * LockingClause - raw representation of FOR [NO KEY] UPDATE/[KEY] SHARE
 *		options
 *
 * Note: lockedRels == NIL means "all relations in query".  Otherwise it
 * is a list of RangeVar nodes.  (We use RangeVar mainly because it carries
 * a location field --- currently, parse analysis insists on unqualified
 * names in LockingClause.)
 */
#[repr(C)]
pub struct LockingClause {
    pub r#type: NodeTag,
    pub lockedRels: *mut List, /* FOR [KEY] UPDATE/SHARE relations */
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy, /* NOWAIT and SKIP LOCKED */
}

/*
 * XMLSERIALIZE (in raw parse tree only)
 */
#[repr(C)]
pub struct XmlSerialize {
    pub r#type: NodeTag,
    pub xmloption: XmlOptionType, /* DOCUMENT or CONTENT */
    pub expr: *mut Node,
    pub typeName: *mut TypeName,
    pub indent: bool,       /* [NO] INDENT */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/* Partitioning related definitions */

/*
 * PartitionElem - parse-time representation of a single partition key
 *
 * expr can be either a raw expression tree or a parse-analyzed expression.
 * We don't store these on-disk, though.
 */
#[repr(C)]
pub struct PartitionElem {
    pub r#type: NodeTag,
    pub name: *mut c_char,    /* name of column to partition on, or NULL */
    pub expr: *mut Node,      /* expression to partition on, or NULL */
    pub collation: *mut List, /* name of collation; NIL = default */
    pub opclass: *mut List,   /* name of desired opclass; NIL = default */
    pub location: ParseLoc,   /* token location, or -1 if unknown */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PartitionStrategy {
    PARTITION_STRATEGY_LIST = b'l' as isize,
    PARTITION_STRATEGY_RANGE = b'r' as isize,
    PARTITION_STRATEGY_HASH = b'h' as isize,
}
pub use PartitionStrategy::*;

/*
 * PartitionSpec - parse-time representation of a partition key specification
 *
 * This represents the key space we will be partitioning on.
 */
#[repr(C)]
pub struct PartitionSpec {
    pub r#type: NodeTag,
    pub strategy: PartitionStrategy,
    pub partParams: *mut List, /* List of PartitionElems */
    pub location: ParseLoc,    /* token location, or -1 if unknown */
}

/*
 * PartitionBoundSpec - a partition bound specification
 *
 * This represents the portion of the partition key space assigned to a
 * particular partition.  These are stored on disk in pg_class.relpartbound.
 */
#[repr(C)]
pub struct PartitionBoundSpec {
    pub r#type: NodeTag,

    pub strategy: c_char,  /* see PARTITION_STRATEGY codes above */
    pub is_default: bool,  /* is it a default partition bound? */

    /* Partitioning info for HASH strategy: */
    pub modulus: c_int,
    pub remainder: c_int,

    /* Partitioning info for LIST strategy: */
    pub listdatums: *mut List, /* List of Consts (or A_Consts in raw tree) */

    /* Partitioning info for RANGE strategy: */
    pub lowerdatums: *mut List, /* List of PartitionRangeDatums */
    pub upperdatums: *mut List, /* List of PartitionRangeDatums */

    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * PartitionRangeDatum - one of the values in a range partition bound
 *
 * This can be MINVALUE, MAXVALUE or a specific bounded value.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PartitionRangeDatumKind {
    PARTITION_RANGE_DATUM_MINVALUE = -1, /* less than any other value */
    PARTITION_RANGE_DATUM_VALUE = 0,     /* a specific (bounded) value */
    PARTITION_RANGE_DATUM_MAXVALUE = 1,  /* greater than any other value */
}
pub use PartitionRangeDatumKind::*;

#[repr(C)]
pub struct PartitionRangeDatum {
    pub r#type: NodeTag,

    pub kind: PartitionRangeDatumKind,
    pub value: *mut Node, /* Const (or A_Const in raw tree), if kind is
                           * PARTITION_RANGE_DATUM_VALUE, else NULL */

    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * PartitionCmd - info for ALTER TABLE/INDEX ATTACH/DETACH PARTITION commands
 */
#[repr(C)]
pub struct PartitionCmd {
    pub r#type: NodeTag,
    pub name: *mut RangeVar,          /* name of partition to attach/detach */
    pub bound: *mut PartitionBoundSpec, /* FOR VALUES, if attaching */
    pub concurrent: bool,
}

/****************************************************************************
 *	Nodes for a Query tree
 ****************************************************************************/

/*--------------------
 * RangeTblEntry -
 *	  A range table is a List of RangeTblEntry nodes.
 *
 *	  A range table entry may represent a plain relation, a sub-select in
 *	  FROM, or the result of a JOIN clause.  (Only explicit JOIN syntax
 *	  produces an RTE, not the implicit join resulting from multiple FROM
 *	  items.  This is because we only need the RTE to deal with SQL features
 *	  like outer joins and join-output-column aliasing.)  Other special
 *	  RTE types also exist, as indicated by RTEKind.
 *
 *	  Note that we consider RTE_RELATION to cover anything that has a pg_class
 *	  entry.  relkind distinguishes the sub-cases.
 *
 *	  alias is an Alias node representing the AS alias-clause attached to the
 *	  FROM expression, or NULL if no clause.
 *
 *	  eref is the table reference name and column reference names (either
 *	  real or aliases).  Note that system columns (OID etc) are not included
 *	  in the column list.
 *	  eref->aliasname is required to be present, and should generally be used
 *	  to identify the RTE for error messages etc.
 *
 *	  In RELATION RTEs, the colnames in both alias and eref are indexed by
 *	  physical attribute number; this means there must be colname entries for
 *	  dropped columns.  When building an RTE we insert empty strings ("") for
 *	  dropped columns.  Note however that a stored rule may have nonempty
 *	  colnames for columns dropped since the rule was created (and for that
 *	  matter the colnames might be out of date due to column renamings).
 *	  The same comments apply to FUNCTION RTEs when a function's return type
 *	  is a named composite type.
 *
 *	  In JOIN RTEs, the colnames in both alias and eref are one-to-one with
 *	  joinaliasvars entries.  A JOIN RTE will omit columns of its inputs when
 *	  those columns are known to be dropped at parse time.  Again, however,
 *	  a stored rule might contain entries for columns dropped since the rule
 *	  was created.  (This is only possible for columns not actually referenced
 *	  in the rule.)  When loading a stored rule, we replace the joinaliasvars
 *	  items for any such columns with null pointers.  (We can't simply delete
 *	  them from the joinaliasvars list, because that would affect the attnums
 *	  of Vars referencing the rest of the list.)
 *
 *	  inFromCl marks those range variables that are listed in the FROM clause.
 *	  It's false for RTEs that are added to a query behind the scenes, such
 *	  as the NEW and OLD variables for a rule, or the subqueries of a UNION.
 *	  This flag is not used during parsing (except in transformLockingClause,
 *	  q.v.); the parser now uses a separate "namespace" data structure to
 *	  control visibility.  But it is needed by ruleutils.c to determine
 *	  whether RTEs should be shown in decompiled queries.
 *
 *	  securityQuals is a list of security barrier quals (boolean expressions),
 *	  to be tested in the listed order before returning a row from the
 *	  relation.  It is always NIL in parser output.  Entries are added by the
 *	  rewriter to implement security-barrier views and/or row-level security.
 *	  Note that the planner turns each boolean expression into an implicitly
 *	  AND'ed sublist, as is its usual habit with qualification expressions.
 *--------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RTEKind {
    RTE_RELATION,       /* ordinary relation reference */
    RTE_SUBQUERY,       /* subquery in FROM */
    RTE_JOIN,           /* join */
    RTE_FUNCTION,       /* function in FROM */
    RTE_TABLEFUNC,      /* TableFunc(.., column list) */
    RTE_VALUES,         /* VALUES (<exprlist>), (<exprlist>), ... */
    RTE_CTE,            /* common table expr (WITH list element) */
    RTE_NAMEDTUPLESTORE, /* tuplestore, e.g. for AFTER triggers */
    RTE_RESULT,         /* RTE represents an empty FROM clause; such
                         * RTEs are added by the planner, they're not
                         * present during parsing or rewriting */
    RTE_GROUP,          /* the grouping step */
}
pub use RTEKind::*;

#[repr(C)]
pub struct RangeTblEntry {
    // pg_node_attr(custom_read_write)
    pub r#type: NodeTag,

    /*
     * Fields valid in all RTEs:
     *
     * put alias + eref first to make dump more legible
     */
    /* user-written alias clause, if any */
    pub alias: *mut Alias, // pg_node_attr(query_jumble_ignore)

    /*
     * Expanded reference names.  This uses a custom query jumble function so
     * that the table name is included in the computation, but not its list of
     * columns.
     */
    pub eref: *mut Alias, // pg_node_attr(custom_query_jumble)

    pub rtekind: RTEKind, /* see above */

    /*
     * Fields valid for a plain relation RTE (else zero):
     *
     * inh is true for relation references that should be expanded to include
     * inheritance children, if the rel has any.  In the parser, this will
     * only be true for RTE_RELATION entries.  The planner also uses this
     * field to mark RTE_SUBQUERY entries that contain UNION ALL queries that
     * it has flattened into pulled-up subqueries (creating a structure much
     * like the effects of inheritance).
     *
     * rellockmode is really LOCKMODE, but it's declared int to avoid having
     * to include lock-related headers here.  It must be RowExclusiveLock if
     * the RTE is an INSERT/UPDATE/DELETE/MERGE target, else RowShareLock if
     * the RTE is a SELECT FOR UPDATE/FOR SHARE target, else AccessShareLock.
     *
     * Note: in some cases, rule expansion may result in RTEs that are marked
     * with RowExclusiveLock even though they are not the target of the
     * current query; this happens if a DO ALSO rule simply scans the original
     * target table.  We leave such RTEs with their original lockmode so as to
     * avoid getting an additional, lesser lock.
     *
     * perminfoindex is 1-based index of the RTEPermissionInfo belonging to
     * this RTE in the containing struct's list of same; 0 if permissions need
     * not be checked for this RTE.
     *
     * As a special case, relid, relkind, rellockmode, and perminfoindex can
     * also be set (nonzero) in an RTE_SUBQUERY RTE.  This occurs when we
     * convert an RTE_RELATION RTE naming a view into an RTE_SUBQUERY
     * containing the view's query.  We still need to perform run-time locking
     * and permission checks on the view, even though it's not directly used
     * in the query anymore, and the most expedient way to do that is to
     * retain these fields from the old state of the RTE.
     *
     * As a special case, RTE_NAMEDTUPLESTORE can also set relid to indicate
     * that the tuple format of the tuplestore is the same as the referenced
     * relation.  This allows plans referencing AFTER trigger transition
     * tables to be invalidated if the underlying table is altered.
     */
    /* OID of the relation */
    pub relid: Oid, // pg_node_attr(query_jumble_ignore)
    /* inheritance requested? */
    pub inh: bool,
    /* relation kind (see pg_class.relkind) */
    pub relkind: c_char, // pg_node_attr(query_jumble_ignore)
    /* lock level that query requires on the rel */
    pub rellockmode: c_int, // pg_node_attr(query_jumble_ignore)
    /* index of RTEPermissionInfo entry, or 0 */
    pub perminfoindex: Index, // pg_node_attr(query_jumble_ignore)
    /* sampling info, or NULL */
    pub tablesample: *mut TableSampleClause,

    /*
     * Fields valid for a subquery RTE (else NULL):
     */
    /* the sub-query */
    pub subquery: *mut Query,
    /* is from security_barrier view? */
    pub security_barrier: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * Fields valid for a join RTE (else NULL/zero):
     *
     * joinaliasvars is a list of (usually) Vars corresponding to the columns
     * of the join result.  An alias Var referencing column K of the join
     * result can be replaced by the K'th element of joinaliasvars --- but to
     * simplify the task of reverse-listing aliases correctly, we do not do
     * that until planning time.  In detail: an element of joinaliasvars can
     * be a Var of one of the join's input relations, or such a Var with an
     * implicit coercion to the join's output column type, or a COALESCE
     * expression containing the two input column Vars (possibly coerced).
     * Elements beyond the first joinmergedcols entries are always just Vars,
     * and are never referenced from elsewhere in the query (that is, join
     * alias Vars are generated only for merged columns).  We keep these
     * entries only because they're needed in expandRTE() and similar code.
     *
     * Vars appearing within joinaliasvars are marked with varnullingrels sets
     * that describe the nulling effects of this join and lower ones.  This is
     * essential for FULL JOIN cases, because the COALESCE expression only
     * describes the semantics correctly if its inputs have been nulled by the
     * join.  For other cases, it allows expandRTE() to generate a valid
     * representation of the join's output without consulting additional
     * parser state.
     *
     * Within a Query loaded from a stored rule, it is possible for non-merged
     * joinaliasvars items to be null pointers, which are placeholders for
     * (necessarily unreferenced) columns dropped since the rule was made.
     * Also, once planning begins, joinaliasvars items can be almost anything,
     * as a result of subquery-flattening substitutions.
     *
     * joinleftcols is an integer list of physical column numbers of the left
     * join input rel that are included in the join; likewise joinrighttcols
     * for the right join input rel.  (Which rels those are can be determined
     * from the associated JoinExpr.)  If the join is USING/NATURAL, then the
     * first joinmergedcols entries in each list identify the merged columns.
     * The merged columns come first in the join output, then remaining
     * columns of the left input, then remaining columns of the right.
     *
     * Note that input columns could have been dropped after creation of a
     * stored rule, if they are not referenced in the query (in particular,
     * merged columns could not be dropped); this is not accounted for in
     * joinleftcols/joinrighttcols.
     */
    pub jointype: JoinType,
    /* number of merged (JOIN USING) columns */
    pub joinmergedcols: c_int, // pg_node_attr(query_jumble_ignore)
    /* list of alias-var expansions */
    pub joinaliasvars: *mut List, // pg_node_attr(query_jumble_ignore)
    /* left-side input column numbers */
    pub joinleftcols: *mut List, // pg_node_attr(query_jumble_ignore)
    /* right-side input column numbers */
    pub joinrightcols: *mut List, // pg_node_attr(query_jumble_ignore)

    /*
     * join_using_alias is an alias clause attached directly to JOIN/USING. It
     * is different from the alias field (below) in that it does not hide the
     * range variables of the tables being joined.
     */
    pub join_using_alias: *mut Alias, // pg_node_attr(query_jumble_ignore)

    /*
     * Fields valid for a function RTE (else NIL/zero):
     *
     * When funcordinality is true, the eref->colnames list includes an alias
     * for the ordinality column.  The ordinality column is otherwise
     * implicit, and must be accounted for "by hand" in places such as
     * expandRTE().
     */
    /* list of RangeTblFunction nodes */
    pub functions: *mut List,
    /* is this called WITH ORDINALITY? */
    pub funcordinality: bool,

    /*
     * Fields valid for a TableFunc RTE (else NULL):
     */
    pub tablefunc: *mut TableFunc,

    /*
     * Fields valid for a values RTE (else NIL):
     */
    /* list of expression lists */
    pub values_lists: *mut List,

    /*
     * Fields valid for a CTE RTE (else NULL/zero):
     */
    /* name of the WITH list item */
    pub ctename: *mut c_char,
    /* number of query levels up */
    pub ctelevelsup: Index,
    /* is this a recursive self-reference? */
    pub self_reference: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * Fields valid for CTE, VALUES, ENR, and TableFunc RTEs (else NIL):
     *
     * We need these for CTE RTEs so that the types of self-referential
     * columns are well-defined.  For VALUES RTEs, storing these explicitly
     * saves having to re-determine the info by scanning the values_lists. For
     * ENRs, we store the types explicitly here (we could get the information
     * from the catalogs if 'relid' was supplied, but we'd still need these
     * for TupleDesc-based ENRs, so we might as well always store the type
     * info here).  For TableFuncs, these fields are redundant with data in
     * the TableFunc node, but keeping them here allows some code sharing with
     * the other cases.
     *
     * For ENRs only, we have to consider the possibility of dropped columns.
     * A dropped column is included in these lists, but it will have zeroes in
     * all three lists (as well as an empty-string entry in eref).  Testing
     * for zero coltype is the standard way to detect a dropped column.
     */
    /* OID list of column type OIDs */
    pub coltypes: *mut List, // pg_node_attr(query_jumble_ignore)
    /* integer list of column typmods */
    pub coltypmods: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of column collation OIDs */
    pub colcollations: *mut List, // pg_node_attr(query_jumble_ignore)

    /*
     * Fields valid for ENR RTEs (else NULL/zero):
     */
    /* name of ephemeral named relation */
    pub enrname: *mut c_char,
    /* estimated or actual from caller */
    pub enrtuples: Cardinality, // pg_node_attr(query_jumble_ignore)

    /*
     * Fields valid for a GROUP RTE (else NIL):
     */
    /* list of grouping expressions */
    pub groupexprs: *mut List,

    /*
     * Fields valid in all RTEs:
     */
    /* was LATERAL specified? */
    pub lateral: bool, // pg_node_attr(query_jumble_ignore)
    /* present in FROM clause? */
    pub inFromCl: bool, // pg_node_attr(query_jumble_ignore)
    /* security barrier quals to apply, if any */
    pub securityQuals: *mut List, // pg_node_attr(query_jumble_ignore)
}

/*
 * RTEPermissionInfo
 * 		Per-relation information for permission checking. Added to the Query
 * 		node by the parser when adding the corresponding RTE to the query
 * 		range table and subsequently editorialized on by the rewriter if
 * 		needed after rule expansion.
 *
 * Only the relations directly mentioned in the query are checked for
 * access permissions by the core executor, so only their RTEPermissionInfos
 * are present in the Query.  However, extensions may want to check inheritance
 * children too, depending on the value of rte->inh, so it's copied in 'inh'
 * for their perusal.
 *
 * requiredPerms and checkAsUser specify run-time access permissions checks
 * to be performed at query startup.  The user must have *all* of the
 * permissions that are OR'd together in requiredPerms (never 0!).  If
 * checkAsUser is not zero, then do the permissions checks using the access
 * rights of that user, not the current effective user ID.  (This allows rules
 * to act as setuid gateways.)
 *
 * For SELECT/INSERT/UPDATE permissions, if the user doesn't have table-wide
 * permissions then it is sufficient to have the permissions on all columns
 * identified in selectedCols (for SELECT) and/or insertedCols and/or
 * updatedCols (INSERT with ON CONFLICT DO UPDATE may have all 3).
 * selectedCols, insertedCols and updatedCols are bitmapsets, which cannot have
 * negative integer members, so we subtract FirstLowInvalidHeapAttributeNumber
 * from column numbers before storing them in these fields.  A whole-row Var
 * reference is represented by setting the bit for InvalidAttrNumber.
 *
 * updatedCols is also used in some other places, for example, to determine
 * which triggers to fire and in FDWs to know which changed columns they need
 * to ship off.
 */
#[repr(C)]
pub struct RTEPermissionInfo {
    pub r#type: NodeTag,

    pub relid: Oid,                /* relation OID */
    pub inh: bool,                 /* separately check inheritance children? */
    pub requiredPerms: AclMode,    /* bitmask of required access permissions */
    pub checkAsUser: Oid,          /* if valid, check access as this role */
    pub selectedCols: *mut Bitmapset, /* columns needing SELECT permission */
    pub insertedCols: *mut Bitmapset, /* columns needing INSERT permission */
    pub updatedCols: *mut Bitmapset, /* columns needing UPDATE permission */
}

/*
 * RangeTblFunction -
 *	  RangeTblEntry subsidiary data for one function in a FUNCTION RTE.
 *
 * If the function had a column definition list (required for an
 * otherwise-unspecified RECORD result), funccolnames lists the names given
 * in the definition list, funccoltypes lists their declared column types,
 * funccoltypmods lists their typmods, funccolcollations their collations.
 * Otherwise, those fields are NIL.
 *
 * Notice we don't attempt to store info about the results of functions
 * returning named composite types, because those can change from time to
 * time.  We do however remember how many columns we thought the type had
 * (including dropped columns!), so that we can successfully ignore any
 * columns added after the query was parsed.
 *
 * The query jumbling only needs to track the function expression.
 */
#[repr(C)]
pub struct RangeTblFunction {
    pub r#type: NodeTag,

    pub funcexpr: *mut Node, /* expression tree for func call */
    /* number of columns it contributes to RTE */
    pub funccolcount: c_int, // pg_node_attr(query_jumble_ignore)
    /* These fields record the contents of a column definition list, if any: */
    /* column names (list of String) */
    pub funccolnames: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of column type OIDs */
    pub funccoltypes: *mut List, // pg_node_attr(query_jumble_ignore)
    /* integer list of column typmods */
    pub funccoltypmods: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of column collation OIDs */
    pub funccolcollations: *mut List, // pg_node_attr(query_jumble_ignore)

    /* This is set during planning for use by the executor: */
    /* PARAM_EXEC Param IDs affecting this func */
    pub funcparams: *mut Bitmapset, // pg_node_attr(query_jumble_ignore)
}

/*
 * TableSampleClause - TABLESAMPLE appearing in a transformed FROM clause
 *
 * Unlike RangeTableSample, this is a subnode of the relevant RangeTblEntry.
 */
#[repr(C)]
pub struct TableSampleClause {
    pub r#type: NodeTag,
    pub tsmhandler: Oid,    /* OID of the tablesample handler function */
    pub args: *mut List,    /* tablesample argument expression(s) */
    pub repeatable: *mut Expr, /* REPEATABLE expression, or NULL if none */
}

/*
 * WithCheckOption -
 *		representation of WITH CHECK OPTION checks to be applied to new tuples
 *		when inserting/updating an auto-updatable view, or RLS WITH CHECK
 *		policies to be applied when inserting/updating a relation with RLS.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WCOKind {
    WCO_VIEW_CHECK,           /* WCO on an auto-updatable view */
    WCO_RLS_INSERT_CHECK,     /* RLS INSERT WITH CHECK policy */
    WCO_RLS_UPDATE_CHECK,     /* RLS UPDATE WITH CHECK policy */
    WCO_RLS_CONFLICT_CHECK,   /* RLS ON CONFLICT DO UPDATE USING policy */
    WCO_RLS_MERGE_UPDATE_CHECK, /* RLS MERGE UPDATE USING policy */
    WCO_RLS_MERGE_DELETE_CHECK, /* RLS MERGE DELETE USING policy */
}
pub use WCOKind::*;

#[repr(C)]
pub struct WithCheckOption {
    pub r#type: NodeTag,
    pub kind: WCOKind,         /* kind of WCO */
    pub relname: *mut c_char,  /* name of relation that specified the WCO */
    pub polname: *mut c_char,  /* name of RLS policy being checked */
    pub qual: *mut Node,       /* constraint qual to check */
    pub cascaded: bool,        /* true for a cascaded WCO on a view */
}

/*
 * SortGroupClause -
 *		representation of ORDER BY, GROUP BY, PARTITION BY,
 *		DISTINCT, DISTINCT ON items
 *
 * You might think that ORDER BY is only interested in defining ordering,
 * and GROUP/DISTINCT are only interested in defining equality.  However,
 * one way to implement grouping is to sort and then apply a "uniq"-like
 * filter.  So it's also interesting to keep track of possible sort operators
 * for GROUP/DISTINCT, and in particular to try to sort for the grouping
 * in a way that will also yield a requested ORDER BY ordering.  So we need
 * to be able to compare ORDER BY and GROUP/DISTINCT lists, which motivates
 * the decision to give them the same representation.
 *
 * tleSortGroupRef must match ressortgroupref of exactly one entry of the
 *		query's targetlist; that is the expression to be sorted or grouped by.
 * eqop is the OID of the equality operator.
 * sortop is the OID of the ordering operator (a "<" or ">" operator),
 *		or InvalidOid if not available.
 * nulls_first means about what you'd expect.  If sortop is InvalidOid
 *		then nulls_first is meaningless and should be set to false.
 * hashable is true if eqop is hashable (note this condition also depends
 *		on the datatype of the input expression).
 *
 * In an ORDER BY item, all fields must be valid.  (The eqop isn't essential
 * here, but it's cheap to get it along with the sortop, and requiring it
 * to be valid eases comparisons to grouping items.)  Note that this isn't
 * actually enough information to determine an ordering: if the sortop is
 * collation-sensitive, a collation OID is needed too.  We don't store the
 * collation in SortGroupClause because it's not available at the time the
 * parser builds the SortGroupClause; instead, consult the exposed collation
 * of the referenced targetlist expression to find out what it is.
 *
 * In a grouping item, eqop must be valid.  If the eqop is a btree equality
 * operator, then sortop should be set to a compatible ordering operator.
 * We prefer to set eqop/sortop/nulls_first to match any ORDER BY item that
 * the query presents for the same tlist item.  If there is none, we just
 * use the default ordering op for the datatype.
 *
 * If the tlist item's type has a hash opclass but no btree opclass, then
 * we will set eqop to the hash equality operator, sortop to InvalidOid,
 * and nulls_first to false.  A grouping item of this kind can only be
 * implemented by hashing, and of course it'll never match an ORDER BY item.
 *
 * The hashable flag is provided since we generally have the requisite
 * information readily available when the SortGroupClause is constructed,
 * and it's relatively expensive to get it again later.  Note there is no
 * need for a "sortable" flag since OidIsValid(sortop) serves the purpose.
 *
 * A query might have both ORDER BY and DISTINCT (or DISTINCT ON) clauses.
 * In SELECT DISTINCT, the distinctClause list is as long or longer than the
 * sortClause list, while in SELECT DISTINCT ON it's typically shorter.
 * The two lists must match up to the end of the shorter one --- the parser
 * rearranges the distinctClause if necessary to make this true.  (This
 * restriction ensures that only one sort step is needed to both satisfy the
 * ORDER BY and set up for the Unique step.  This is semantically necessary
 * for DISTINCT ON, and presents no real drawback for DISTINCT.)
 */
#[repr(C)]
pub struct SortGroupClause {
    pub r#type: NodeTag,
    pub tleSortGroupRef: Index, /* reference into targetlist */
    pub eqop: Oid,           /* the equality operator ('=' op) */
    pub sortop: Oid,         /* the ordering operator ('<' op), or 0 */
    pub reverse_sort: bool,  /* is sortop a "greater than" operator? */
    pub nulls_first: bool,   /* do NULLs come before normal values? */
    /* can eqop be implemented by hashing? */
    pub hashable: bool, // pg_node_attr(query_jumble_ignore)
}

/*
 * GroupingSet -
 *		representation of CUBE, ROLLUP and GROUPING SETS clauses
 *
 * In a Query with grouping sets, the groupClause contains a flat list of
 * SortGroupClause nodes for each distinct expression used.  The actual
 * structure of the GROUP BY clause is given by the groupingSets tree.
 *
 * In the raw parser output, GroupingSet nodes (of all types except SIMPLE
 * which is not used) are potentially mixed in with the expressions in the
 * groupClause of the SelectStmt.  (An expression can't contain a GroupingSet,
 * but a list may mix GroupingSet and expression nodes.)  At this stage, the
 * content of each node is a list of expressions, some of which may be RowExprs
 * which represent sublists rather than actual row constructors, and nested
 * GroupingSet nodes where legal in the grammar.  The structure directly
 * reflects the query syntax.
 *
 * In parse analysis, the transformed expressions are used to build the tlist
 * and groupClause list (of SortGroupClause nodes), and the groupingSets tree
 * is eventually reduced to a fixed format:
 *
 * EMPTY nodes represent (), and obviously have no content
 *
 * SIMPLE nodes represent a list of one or more expressions to be treated as an
 * atom by the enclosing structure; the content is an integer list of
 * ressortgroupref values (see SortGroupClause)
 *
 * CUBE and ROLLUP nodes contain a list of one or more SIMPLE nodes.
 *
 * SETS nodes contain a list of EMPTY, SIMPLE, CUBE or ROLLUP nodes, but after
 * parse analysis they cannot contain more SETS nodes; enough of the syntactic
 * transforms of the spec have been applied that we no longer have arbitrarily
 * deep nesting (though we still preserve the use of cube/rollup).
 *
 * Note that if the groupingSets tree contains no SIMPLE nodes (only EMPTY
 * nodes at the leaves), then the groupClause will be empty, but this is still
 * an aggregation query (similar to using aggs or HAVING without GROUP BY).
 *
 * As an example, the following clause:
 *
 * GROUP BY GROUPING SETS ((a,b), CUBE(c,(d,e)))
 *
 * looks like this after raw parsing:
 *
 * SETS( RowExpr(a,b) , CUBE( c, RowExpr(d,e) ) )
 *
 * and parse analysis converts it to:
 *
 * SETS( SIMPLE(1,2), CUBE( SIMPLE(3), SIMPLE(4,5) ) )
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GroupingSetKind {
    GROUPING_SET_EMPTY,
    GROUPING_SET_SIMPLE,
    GROUPING_SET_ROLLUP,
    GROUPING_SET_CUBE,
    GROUPING_SET_SETS,
}
pub use GroupingSetKind::*;

#[repr(C)]
pub struct GroupingSet {
    pub r#type: NodeTag,
    pub kind: GroupingSetKind, // pg_node_attr(query_jumble_ignore)
    pub content: *mut List,
    pub location: ParseLoc,
}

/*
 * WindowClause -
 *		transformed representation of WINDOW and OVER clauses
 *
 * A parsed Query's windowClause list contains these structs.  "name" is set
 * if the clause originally came from WINDOW, and is NULL if it originally
 * was an OVER clause (but note that we collapse out duplicate OVERs).
 * partitionClause and orderClause are lists of SortGroupClause structs.
 * partitionClause is sanitized by the query planner to remove any columns or
 * expressions belonging to redundant PathKeys.
 * If we have RANGE with offset PRECEDING/FOLLOWING, the semantics of that are
 * specified by startInRangeFunc/inRangeColl/inRangeAsc/inRangeNullsFirst
 * for the start offset, or endInRangeFunc/inRange* for the end offset.
 * winref is an ID number referenced by WindowFunc nodes; it must be unique
 * among the members of a Query's windowClause list.
 * When refname isn't null, the partitionClause is always copied from there;
 * the orderClause might or might not be copied (see copiedOrder); the framing
 * options are never copied, per spec.
 *
 * The information relevant for the query jumbling is the partition clause
 * type and its bounds.
 */
#[repr(C)]
pub struct WindowClause {
    pub r#type: NodeTag,
    /* window name (NULL in an OVER clause) */
    pub name: *mut c_char, // pg_node_attr(query_jumble_ignore)
    /* referenced window name, if any */
    pub refname: *mut c_char, // pg_node_attr(query_jumble_ignore)
    pub partitionClause: *mut List, /* PARTITION BY list */
    /* ORDER BY list */
    pub orderClause: *mut List,
    pub frameOptions: c_int, /* frame_clause options, see WindowDef */
    pub startOffset: *mut Node, /* expression for starting bound, if any */
    pub endOffset: *mut Node, /* expression for ending bound, if any */
    /* in_range function for startOffset */
    pub startInRangeFunc: Oid, // pg_node_attr(query_jumble_ignore)
    /* in_range function for endOffset */
    pub endInRangeFunc: Oid, // pg_node_attr(query_jumble_ignore)
    /* collation for in_range tests */
    pub inRangeColl: Oid, // pg_node_attr(query_jumble_ignore)
    /* use ASC sort order for in_range tests? */
    pub inRangeAsc: bool, // pg_node_attr(query_jumble_ignore)
    /* nulls sort first for in_range tests? */
    pub inRangeNullsFirst: bool, // pg_node_attr(query_jumble_ignore)
    pub winref: Index, /* ID referenced by window functions */
    /* did we copy orderClause from refname? */
    pub copiedOrder: bool, // pg_node_attr(query_jumble_ignore)
}

/*
 * RowMarkClause -
 *	   parser output representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * Query.rowMarks contains a separate RowMarkClause node for each relation
 * identified as a FOR [KEY] UPDATE/SHARE target.  If one of these clauses
 * is applied to a subquery, we generate RowMarkClauses for all normal and
 * subquery rels in the subquery, but they are marked pushedDown = true to
 * distinguish them from clauses that were explicitly written at this query
 * level.  Also, Query.hasForUpdate tells whether there were explicit FOR
 * UPDATE/SHARE/KEY SHARE clauses in the current query level.
 */
#[repr(C)]
pub struct RowMarkClause {
    pub r#type: NodeTag,
    pub rti: Index, /* range table index of target relation */
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy, /* NOWAIT and SKIP LOCKED */
    pub pushedDown: bool, /* pushed down from higher query level? */
}

/*
 * WithClause -
 *	   representation of WITH clause
 *
 * Note: WithClause does not propagate into the Query representation;
 * but CommonTableExpr does.
 */
#[repr(C)]
pub struct WithClause {
    pub r#type: NodeTag,
    pub ctes: *mut List,    /* list of CommonTableExprs */
    pub recursive: bool,    /* true = WITH RECURSIVE */
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/*
 * InferClause -
 *		ON CONFLICT unique index inference clause
 *
 * Note: InferClause does not propagate into the Query representation.
 */
#[repr(C)]
pub struct InferClause {
    pub r#type: NodeTag,
    pub indexElems: *mut List, /* IndexElems to infer unique index */
    pub whereClause: *mut Node, /* qualification (partial-index predicate) */
    pub conname: *mut c_char,  /* Constraint name, or NULL if unnamed */
    pub location: ParseLoc,    /* token location, or -1 if unknown */
}

/*
 * OnConflictClause -
 *		representation of ON CONFLICT clause
 *
 * Note: OnConflictClause does not propagate into the Query representation.
 */
#[repr(C)]
pub struct OnConflictClause {
    pub r#type: NodeTag,
    pub action: OnConflictAction, /* DO NOTHING or UPDATE? */
    pub infer: *mut InferClause, /* Optional index inference clause */
    pub targetList: *mut List,   /* the target list (of ResTarget) */
    pub whereClause: *mut Node,  /* qualifications */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * CommonTableExpr -
 *	   representation of WITH list element
 */

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CTEMaterialize {
    CTEMaterializeDefault, /* no option specified */
    CTEMaterializeAlways,  /* MATERIALIZED */
    CTEMaterializeNever,   /* NOT MATERIALIZED */
}
pub use CTEMaterialize::*;

#[repr(C)]
pub struct CTESearchClause {
    pub r#type: NodeTag,
    pub search_col_list: *mut List,
    pub search_breadth_first: bool,
    pub search_seq_column: *mut c_char,
    pub location: ParseLoc,
}

#[repr(C)]
pub struct CTECycleClause {
    pub r#type: NodeTag,
    pub cycle_col_list: *mut List,
    pub cycle_mark_column: *mut c_char,
    pub cycle_mark_value: *mut Node,
    pub cycle_mark_default: *mut Node,
    pub cycle_path_column: *mut c_char,
    pub location: ParseLoc,
    /* These fields are set during parse analysis: */
    pub cycle_mark_type: Oid, /* common type of _value and _default */
    pub cycle_mark_typmod: c_int,
    pub cycle_mark_collation: Oid,
    pub cycle_mark_neop: Oid, /* <> operator for type */
}

#[repr(C)]
pub struct CommonTableExpr {
    pub r#type: NodeTag,

    /*
     * Query name (never qualified).  The string name is included in the query
     * jumbling because RTE_CTE RTEs need it.
     */
    pub ctename: *mut c_char,
    /* optional list of column names */
    pub aliascolnames: *mut List, // pg_node_attr(query_jumble_ignore)
    pub ctematerialized: CTEMaterialize, /* is this an optimization fence? */
    /* SelectStmt/InsertStmt/etc before parse analysis, Query afterwards: */
    pub ctequery: *mut Node, /* the CTE's subquery */
    pub search_clause: *mut CTESearchClause, // pg_node_attr(query_jumble_ignore)
    pub cycle_clause: *mut CTECycleClause, // pg_node_attr(query_jumble_ignore)
    pub location: ParseLoc, /* token location, or -1 if unknown */
    /* These fields are set during parse analysis: */
    /* is this CTE actually recursive? */
    pub cterecursive: bool, // pg_node_attr(query_jumble_ignore)

    /*
     * Number of RTEs referencing this CTE (excluding internal
     * self-references), irrelevant for query jumbling.
     */
    pub cterefcount: c_int, // pg_node_attr(query_jumble_ignore)
    /* list of output column names */
    pub ctecolnames: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of output column type OIDs */
    pub ctecoltypes: *mut List, // pg_node_attr(query_jumble_ignore)
    /* integer list of output column typmods */
    pub ctecoltypmods: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of column collation OIDs */
    pub ctecolcollations: *mut List, // pg_node_attr(query_jumble_ignore)
}

/* Convenience macro to get the output tlist of a CTE's query */
// #define GetCTETargetList(cte) \
// 	(AssertMacro(IsA((cte)->ctequery, Query)), \
// 	 ((Query *) (cte)->ctequery)->commandType == CMD_SELECT ? \
// 	 ((Query *) (cte)->ctequery)->targetList : \
// 	 ((Query *) (cte)->ctequery)->returningList)
/// `GetCTETargetList(cte)` in C.  Dereferences raw pointers; invoke in `unsafe`.
#[inline]
pub unsafe fn GetCTETargetList(cte: *mut CommonTableExpr) -> *mut List {
    let q = (*cte).ctequery as *mut Query;
    AssertMacro!(crate::nodes::nodes::nodeTag((*cte).ctequery) == NodeTag::T_Query);
    if (*q).commandType == CmdType::CMD_SELECT {
        (*q).targetList
    } else {
        (*q).returningList
    }
}

/*
 * MergeWhenClause -
 *		raw parser representation of a WHEN clause in a MERGE statement
 *
 * This is transformed into MergeAction by parse analysis
 */
#[repr(C)]
pub struct MergeWhenClause {
    pub r#type: NodeTag,
    pub matchKind: MergeMatchKind, /* MATCHED/NOT MATCHED BY SOURCE/TARGET */
    pub commandType: CmdType,    /* INSERT/UPDATE/DELETE/DO NOTHING */
    pub r#override: OverridingKind, /* OVERRIDING clause */
    pub condition: *mut Node,    /* WHEN conditions (raw parser) */
    pub targetList: *mut List,   /* INSERT/UPDATE targetlist */
    /* the following members are only used in INSERT actions */
    pub values: *mut List, /* VALUES to INSERT, or NULL */
}

/*
 * ReturningOptionKind -
 *		Possible kinds of option in RETURNING WITH(...) list
 *
 * Currently, this is used only for specifying OLD/NEW aliases.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReturningOptionKind {
    RETURNING_OPTION_OLD, /* specify alias for OLD in RETURNING */
    RETURNING_OPTION_NEW, /* specify alias for NEW in RETURNING */
}
pub use ReturningOptionKind::*;

/*
 * ReturningOption -
 *		An individual option in the RETURNING WITH(...) list
 */
#[repr(C)]
pub struct ReturningOption {
    pub r#type: NodeTag,
    pub option: ReturningOptionKind, /* specified option */
    pub value: *mut c_char,          /* option's value */
    pub location: ParseLoc,          /* token location, or -1 if unknown */
}

/*
 * ReturningClause -
 *		List of RETURNING expressions, together with any WITH(...) options
 */
#[repr(C)]
pub struct ReturningClause {
    pub r#type: NodeTag,
    pub options: *mut List, /* list of ReturningOption elements */
    pub exprs: *mut List,   /* list of expressions to return */
}

/*
 * TriggerTransition -
 *	   representation of transition row or table naming clause
 *
 * Only transition tables are initially supported in the syntax, and only for
 * AFTER triggers, but other permutations are accepted by the parser so we can
 * give a meaningful message from C code.
 */
#[repr(C)]
pub struct TriggerTransition {
    pub r#type: NodeTag,
    pub name: *mut c_char,
    pub isNew: bool,
    pub isTable: bool,
}

/* Nodes for SQL/JSON support */

/*
 * JsonOutput -
 *		representation of JSON output clause (RETURNING type [FORMAT format])
 */
#[repr(C)]
pub struct JsonOutput {
    pub r#type: NodeTag,
    pub typeName: *mut TypeName,   /* RETURNING type name, if specified */
    pub returning: *mut JsonReturning, /* RETURNING FORMAT clause and type Oids */
}

/*
 * JsonArgument -
 *		representation of argument from JSON PASSING clause
 */
#[repr(C)]
pub struct JsonArgument {
    pub r#type: NodeTag,
    pub val: *mut JsonValueExpr, /* argument value expression */
    pub name: *mut c_char,       /* argument name */
}

/*
 * JsonQuotes -
 *		representation of [KEEP|OMIT] QUOTES clause for JSON_QUERY()
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonQuotes {
    JS_QUOTES_UNSPEC, /* unspecified */
    JS_QUOTES_KEEP,   /* KEEP QUOTES */
    JS_QUOTES_OMIT,   /* OMIT QUOTES */
}
pub use JsonQuotes::*;

/*
 * JsonFuncExpr -
 *		untransformed representation of function expressions for
 *		SQL/JSON query functions
 */
#[repr(C)]
pub struct JsonFuncExpr {
    pub r#type: NodeTag,
    pub op: JsonExprOp,        /* expression type */
    pub column_name: *mut c_char, /* JSON_TABLE() column name or NULL if this is
                                   * not for a JSON_TABLE() */
    pub context_item: *mut JsonValueExpr, /* context item expression */
    pub pathspec: *mut Node,   /* JSON path specification expression */
    pub passing: *mut List,    /* list of PASSING clause arguments, if any */
    pub output: *mut JsonOutput, /* output clause, if specified */
    pub on_empty: *mut JsonBehavior, /* ON EMPTY behavior */
    pub on_error: *mut JsonBehavior, /* ON ERROR behavior */
    pub wrapper: JsonWrapper,  /* array wrapper behavior (JSON_QUERY only) */
    pub quotes: JsonQuotes,    /* omit or keep quotes? (JSON_QUERY only) */
    pub location: ParseLoc,    /* token location, or -1 if unknown */
}

/*
 * JsonTablePathSpec
 *		untransformed specification of JSON path expression with an optional
 *		name
 */
#[repr(C)]
pub struct JsonTablePathSpec {
    pub r#type: NodeTag,

    pub string: *mut Node,
    pub name: *mut c_char,
    pub name_location: ParseLoc,
    pub location: ParseLoc, /* location of 'string' */
}

/*
 * JsonTable -
 *		untransformed representation of JSON_TABLE
 */
#[repr(C)]
pub struct JsonTable {
    pub r#type: NodeTag,
    pub context_item: *mut JsonValueExpr, /* context item expression */
    pub pathspec: *mut JsonTablePathSpec, /* JSON path specification */
    pub passing: *mut List,    /* list of PASSING clause arguments, if any */
    pub columns: *mut List,    /* list of JsonTableColumn */
    pub on_error: *mut JsonBehavior, /* ON ERROR behavior */
    pub alias: *mut Alias,     /* table alias in FROM clause */
    pub lateral: bool,         /* does it have LATERAL prefix? */
    pub location: ParseLoc,    /* token location, or -1 if unknown */
}

/*
 * JsonTableColumnType -
 *		enumeration of JSON_TABLE column types
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonTableColumnType {
    JTC_FOR_ORDINALITY,
    JTC_REGULAR,
    JTC_EXISTS,
    JTC_FORMATTED,
    JTC_NESTED,
}
pub use JsonTableColumnType::*;

/*
 * JsonTableColumn -
 *		untransformed representation of JSON_TABLE column
 */
#[repr(C)]
pub struct JsonTableColumn {
    pub r#type: NodeTag,
    pub coltype: JsonTableColumnType, /* column type */
    pub name: *mut c_char,        /* column name */
    pub typeName: *mut TypeName,  /* column type name */
    pub pathspec: *mut JsonTablePathSpec, /* JSON path specification */
    pub format: *mut JsonFormat,  /* JSON format clause, if specified */
    pub wrapper: JsonWrapper,     /* WRAPPER behavior for formatted columns */
    pub quotes: JsonQuotes,       /* omit or keep quotes on scalar strings? */
    pub columns: *mut List,       /* nested columns */
    pub on_empty: *mut JsonBehavior, /* ON EMPTY behavior */
    pub on_error: *mut JsonBehavior, /* ON ERROR behavior */
    pub location: ParseLoc,       /* token location, or -1 if unknown */
}

/*
 * JsonKeyValue -
 *		untransformed representation of JSON object key-value pair for
 *		JSON_OBJECT() and JSON_OBJECTAGG()
 */
#[repr(C)]
pub struct JsonKeyValue {
    pub r#type: NodeTag,
    pub key: *mut Expr,          /* key expression */
    pub value: *mut JsonValueExpr, /* JSON value expression */
}

/*
 * JsonParseExpr -
 *		untransformed representation of JSON()
 */
#[repr(C)]
pub struct JsonParseExpr {
    pub r#type: NodeTag,
    pub expr: *mut JsonValueExpr, /* string expression */
    pub output: *mut JsonOutput, /* RETURNING clause, if specified */
    pub unique_keys: bool,       /* WITH UNIQUE KEYS? */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * JsonScalarExpr -
 *		untransformed representation of JSON_SCALAR()
 */
#[repr(C)]
pub struct JsonScalarExpr {
    pub r#type: NodeTag,
    pub expr: *mut Expr,         /* scalar expression */
    pub output: *mut JsonOutput, /* RETURNING clause, if specified */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * JsonSerializeExpr -
 *		untransformed representation of JSON_SERIALIZE() function
 */
#[repr(C)]
pub struct JsonSerializeExpr {
    pub r#type: NodeTag,
    pub expr: *mut JsonValueExpr, /* json value expression */
    pub output: *mut JsonOutput,  /* RETURNING clause, if specified  */
    pub location: ParseLoc,       /* token location, or -1 if unknown */
}

/*
 * JsonObjectConstructor -
 *		untransformed representation of JSON_OBJECT() constructor
 */
#[repr(C)]
pub struct JsonObjectConstructor {
    pub r#type: NodeTag,
    pub exprs: *mut List,        /* list of JsonKeyValue pairs */
    pub output: *mut JsonOutput, /* RETURNING clause, if specified  */
    pub absent_on_null: bool,    /* skip NULL values? */
    pub unique: bool,            /* check key uniqueness? */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * JsonArrayConstructor -
 *		untransformed representation of JSON_ARRAY(element,...) constructor
 */
#[repr(C)]
pub struct JsonArrayConstructor {
    pub r#type: NodeTag,
    pub exprs: *mut List,        /* list of JsonValueExpr elements */
    pub output: *mut JsonOutput, /* RETURNING clause, if specified  */
    pub absent_on_null: bool,    /* skip NULL elements? */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * JsonArrayQueryConstructor -
 *		untransformed representation of JSON_ARRAY(subquery) constructor
 */
#[repr(C)]
pub struct JsonArrayQueryConstructor {
    pub r#type: NodeTag,
    pub query: *mut Node,        /* subquery */
    pub output: *mut JsonOutput, /* RETURNING clause, if specified  */
    pub format: *mut JsonFormat, /* FORMAT clause for subquery, if specified */
    pub absent_on_null: bool,    /* skip NULL elements? */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * JsonAggConstructor -
 *		common fields of untransformed representation of
 *		JSON_ARRAYAGG() and JSON_OBJECTAGG()
 */
#[repr(C)]
pub struct JsonAggConstructor {
    pub r#type: NodeTag,
    pub output: *mut JsonOutput, /* RETURNING clause, if any */
    pub agg_filter: *mut Node,   /* FILTER clause, if any */
    pub agg_order: *mut List,    /* ORDER BY clause, if any */
    pub over: *mut WindowDef,    /* OVER clause, if any */
    pub location: ParseLoc,      /* token location, or -1 if unknown */
}

/*
 * JsonObjectAgg -
 *		untransformed representation of JSON_OBJECTAGG()
 */
#[repr(C)]
pub struct JsonObjectAgg {
    pub r#type: NodeTag,
    pub constructor: *mut JsonAggConstructor, /* common fields */
    pub arg: *mut JsonKeyValue,  /* object key-value pair */
    pub absent_on_null: bool,    /* skip NULL values? */
    pub unique: bool,            /* check key uniqueness? */
}

/*
 * JsonArrayAgg -
 *		untransformed representation of JSON_ARRAYAGG()
 */
#[repr(C)]
pub struct JsonArrayAgg {
    pub r#type: NodeTag,
    pub constructor: *mut JsonAggConstructor, /* common fields */
    pub arg: *mut JsonValueExpr, /* array element expression */
    pub absent_on_null: bool,    /* skip NULL elements? */
}

/*****************************************************************************
 *		Raw Grammar Output Statements
 *****************************************************************************/

/*
 *		RawStmt --- container for any one statement's raw parse tree
 *
 * Parse analysis converts a raw parse tree headed by a RawStmt node into
 * an analyzed statement headed by a Query node.  For optimizable statements,
 * the conversion is complex.  For utility statements, the parser usually just
 * transfers the raw parse tree (sans RawStmt) into the utilityStmt field of
 * the Query node, and all the useful work happens at execution time.
 *
 * stmt_location/stmt_len identify the portion of the source text string
 * containing this raw statement (useful for multi-statement strings).
 *
 * This is irrelevant for query jumbling, as this is not used in parsed
 * queries.
 */
#[repr(C)]
pub struct RawStmt {
    // pg_node_attr(no_query_jumble)
    pub r#type: NodeTag,
    pub stmt: *mut Node,        /* raw parse tree */
    pub stmt_location: ParseLoc, /* start location, or -1 if unknown */
    pub stmt_len: ParseLoc,     /* length in bytes; 0 means "rest of string" */
}

/*****************************************************************************
 *		Optimizable Statements
 *****************************************************************************/

/* ----------------------
 *		Insert Statement
 *
 * The source expression is represented by SelectStmt for both the
 * SELECT and VALUES cases.  If selectStmt is NULL, then the query
 * is INSERT ... DEFAULT VALUES.
 * ----------------------
 */
#[repr(C)]
pub struct InsertStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation to insert into */
    pub cols: *mut List,        /* optional: names of the target columns */
    pub selectStmt: *mut Node,  /* the source SELECT/VALUES, or NULL */
    pub onConflictClause: *mut OnConflictClause, /* ON CONFLICT clause */
    pub returningClause: *mut ReturningClause, /* RETURNING clause */
    pub withClause: *mut WithClause, /* WITH clause */
    pub r#override: OverridingKind, /* OVERRIDING clause */
}

/* ----------------------
 *		Delete Statement
 * ----------------------
 */
#[repr(C)]
pub struct DeleteStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation to delete from */
    pub usingClause: *mut List, /* optional using clause for more tables */
    pub whereClause: *mut Node, /* qualifications */
    pub returningClause: *mut ReturningClause, /* RETURNING clause */
    pub withClause: *mut WithClause, /* WITH clause */
}

/* ----------------------
 *		Update Statement
 * ----------------------
 */
#[repr(C)]
pub struct UpdateStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation to update */
    pub targetList: *mut List,  /* the target list (of ResTarget) */
    pub whereClause: *mut Node, /* qualifications */
    pub fromClause: *mut List,  /* optional from clause for more tables */
    pub returningClause: *mut ReturningClause, /* RETURNING clause */
    pub withClause: *mut WithClause, /* WITH clause */
}

/* ----------------------
 *		Merge Statement
 * ----------------------
 */
#[repr(C)]
pub struct MergeStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* target relation to merge into */
    pub sourceRelation: *mut Node, /* source relation */
    pub joinCondition: *mut Node, /* join condition between source and target */
    pub mergeWhenClauses: *mut List, /* list of MergeWhenClause(es) */
    pub returningClause: *mut ReturningClause, /* RETURNING clause */
    pub withClause: *mut WithClause, /* WITH clause */
}

/* ----------------------
 *		Select Statement
 *
 * A "simple" SELECT is represented in the output of gram.y by a single
 * SelectStmt node; so is a VALUES construct.  A query containing set
 * operators (UNION, INTERSECT, EXCEPT) is represented by a tree of SelectStmt
 * nodes, in which the leaf nodes are component SELECTs and the internal nodes
 * represent UNION, INTERSECT, or EXCEPT operators.  Using the same node
 * type for both leaf and internal nodes allows gram.y to stick ORDER BY,
 * LIMIT, etc, clause values into a SELECT statement without worrying
 * whether it is a simple or compound SELECT.
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetOperation {
    SETOP_NONE = 0,
    SETOP_UNION,
    SETOP_INTERSECT,
    SETOP_EXCEPT,
}
pub use SetOperation::*;

#[repr(C)]
pub struct SelectStmt {
    pub r#type: NodeTag,

    /*
     * These fields are used only in "leaf" SelectStmts.
     */
    pub distinctClause: *mut List, /* NULL, list of DISTINCT ON exprs, or
                                    * lcons(NIL,NIL) for all (SELECT DISTINCT) */
    pub intoClause: *mut IntoClause, /* target for SELECT INTO */
    pub targetList: *mut List,   /* the target list (of ResTarget) */
    pub fromClause: *mut List,   /* the FROM clause */
    pub whereClause: *mut Node,  /* WHERE qualification */
    pub groupClause: *mut List,  /* GROUP BY clauses */
    pub groupDistinct: bool,     /* Is this GROUP BY DISTINCT? */
    pub havingClause: *mut Node, /* HAVING conditional-expression */
    pub windowClause: *mut List, /* WINDOW window_name AS (...), ... */

    /*
     * In a "leaf" node representing a VALUES list, the above fields are all
     * null, and instead this field is set.  Note that the elements of the
     * sublists are just expressions, without ResTarget decoration. Also note
     * that a list element can be DEFAULT (represented as a SetToDefault
     * node), regardless of the context of the VALUES list. It's up to parse
     * analysis to reject that where not valid.
     */
    pub valuesLists: *mut List, /* untransformed list of expression lists */

    /*
     * These fields are used in both "leaf" SelectStmts and upper-level
     * SelectStmts.
     */
    pub sortClause: *mut List,   /* sort clause (a list of SortBy's) */
    pub limitOffset: *mut Node,  /* # of result tuples to skip */
    pub limitCount: *mut Node,   /* # of result tuples to return */
    pub limitOption: LimitOption, /* limit type */
    pub lockingClause: *mut List, /* FOR UPDATE (list of LockingClause's) */
    pub withClause: *mut WithClause, /* WITH clause */

    /*
     * These fields are used only in upper-level SelectStmts.
     */
    pub op: SetOperation,        /* type of set op */
    pub all: bool,               /* ALL specified? */
    pub larg: *mut SelectStmt,   /* left child */
    pub rarg: *mut SelectStmt,   /* right child */
                                 /* Eventually add fields for CORRESPONDING spec here */
}

/* ----------------------
 *		Set Operation node for post-analysis query trees
 *
 * After parse analysis, a SELECT with set operations is represented by a
 * top-level Query node containing the leaf SELECTs as subqueries in its
 * range table.  Its setOperations field shows the tree of set operations,
 * with leaf SelectStmt nodes replaced by RangeTblRef nodes, and internal
 * nodes replaced by SetOperationStmt nodes.  Information about the output
 * column types is added, too.  (Note that the child nodes do not necessarily
 * produce these types directly, but we've checked that their output types
 * can be coerced to the output column type.)  Also, if it's not UNION ALL,
 * information about the types' sort/group semantics is provided in the form
 * of a SortGroupClause list (same representation as, eg, DISTINCT).
 * The resolved common column collations are provided too; but note that if
 * it's not UNION ALL, it's okay for a column to not have a common collation,
 * so a member of the colCollations list could be InvalidOid even though the
 * column has a collatable type.
 * ----------------------
 */
#[repr(C)]
pub struct SetOperationStmt {
    pub r#type: NodeTag,
    pub op: SetOperation, /* type of set op */
    pub all: bool,        /* ALL specified? */
    pub larg: *mut Node,  /* left child */
    pub rarg: *mut Node,  /* right child */
                          /* Eventually add fields for CORRESPONDING spec here */

    /* Fields derived during parse analysis (irrelevant for query jumbling): */
    /* OID list of output column type OIDs */
    pub colTypes: *mut List, // pg_node_attr(query_jumble_ignore)
    /* integer list of output column typmods */
    pub colTypmods: *mut List, // pg_node_attr(query_jumble_ignore)
    /* OID list of output column collation OIDs */
    pub colCollations: *mut List, // pg_node_attr(query_jumble_ignore)
    /* a list of SortGroupClause's */
    pub groupClauses: *mut List, // pg_node_attr(query_jumble_ignore)
                                 /* groupClauses is NIL if UNION ALL, but must be set otherwise */
}

/*
 * RETURN statement (inside SQL function body)
 */
#[repr(C)]
pub struct ReturnStmt {
    pub r#type: NodeTag,
    pub returnval: *mut Node,
}

/* ----------------------
 *		PL/pgSQL Assignment Statement
 *
 * Like SelectStmt, this is transformed into a SELECT Query.
 * However, the targetlist of the result looks more like an UPDATE.
 * ----------------------
 */
#[repr(C)]
pub struct PLAssignStmt {
    pub r#type: NodeTag,

    pub name: *mut c_char,      /* initial column name */
    pub indirection: *mut List, /* subscripts and field names, if any */
    pub nnames: c_int,          /* number of names to use in ColumnRef */
    pub val: *mut SelectStmt,   /* the PL/pgSQL expression to assign */
    pub location: ParseLoc,     /* name's token location, or -1 if unknown */
}

/*****************************************************************************
 *		Other Statements (no optimizations required)
 *
 *		These are not touched by parser/analyze.c except to put them into
 *		the utilityStmt field of a Query.  This is eventually passed to
 *		ProcessUtility (by-passing rewriting and planning).  Some of the
 *		statements do need attention from parse analysis, and this is
 *		done by routines in parser/parse_utilcmd.c after ProcessUtility
 *		receives the command for execution.
 *		DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are special cases:
 *		they contain optimizable statements, which get processed normally
 *		by parser/analyze.c.
 *****************************************************************************/

/*
 * When a command can act on several kinds of objects with only one
 * parse structure required, use these constants to designate the
 * object type.  Note that commands typically don't support all the types.
 */

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ObjectType {
    OBJECT_ACCESS_METHOD,
    OBJECT_AGGREGATE,
    OBJECT_AMOP,
    OBJECT_AMPROC,
    OBJECT_ATTRIBUTE, /* type's attribute, when distinct from column */
    OBJECT_CAST,
    OBJECT_COLUMN,
    OBJECT_COLLATION,
    OBJECT_CONVERSION,
    OBJECT_DATABASE,
    OBJECT_DEFAULT,
    OBJECT_DEFACL,
    OBJECT_DOMAIN,
    OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER,
    OBJECT_EXTENSION,
    OBJECT_FDW,
    OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION,
    OBJECT_INDEX,
    OBJECT_LANGUAGE,
    OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW,
    OBJECT_OPCLASS,
    OBJECT_OPERATOR,
    OBJECT_OPFAMILY,
    OBJECT_PARAMETER_ACL,
    OBJECT_POLICY,
    OBJECT_PROCEDURE,
    OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE,
    OBJECT_PUBLICATION_REL,
    OBJECT_ROLE,
    OBJECT_ROUTINE,
    OBJECT_RULE,
    OBJECT_SCHEMA,
    OBJECT_SEQUENCE,
    OBJECT_SUBSCRIPTION,
    OBJECT_STATISTIC_EXT,
    OBJECT_TABCONSTRAINT,
    OBJECT_TABLE,
    OBJECT_TABLESPACE,
    OBJECT_TRANSFORM,
    OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY,
    OBJECT_TSPARSER,
    OBJECT_TSTEMPLATE,
    OBJECT_TYPE,
    OBJECT_USER_MAPPING,
    OBJECT_VIEW,
}
pub use ObjectType::*;

/* ----------------------
 *		Create Schema Statement
 *
 * NOTE: the schemaElts list contains raw parsetrees for component statements
 * of the schema, such as CREATE TABLE, GRANT, etc.  These are analyzed and
 * executed after the schema itself is created.
 * ----------------------
 */
#[repr(C)]
pub struct CreateSchemaStmt {
    pub r#type: NodeTag,
    pub schemaname: *mut c_char, /* the name of the schema to create */
    pub authrole: *mut RoleSpec, /* the owner of the created schema */
    pub schemaElts: *mut List,   /* schema components (list of parsenodes) */
    pub if_not_exists: bool,     /* just do nothing if schema already exists? */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DropBehavior {
    DROP_RESTRICT, /* drop fails if any dependent objects */
    DROP_CASCADE,  /* remove dependent objects too */
}
pub use DropBehavior::*;

/* ----------------------
 *	Alter Table
 * ----------------------
 */
#[repr(C)]
pub struct AlterTableStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* table to work on */
    pub cmds: *mut List,        /* list of subcommands */
    pub objtype: ObjectType,    /* type of object */
    pub missing_ok: bool,       /* skip error if table missing */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AlterTableType {
    AT_AddColumn,             /* add column */
    AT_AddColumnToView,       /* implicitly via CREATE OR REPLACE VIEW */
    AT_ColumnDefault,         /* alter column default */
    AT_CookedColumnDefault,   /* add a pre-cooked column default */
    AT_DropNotNull,           /* alter column drop not null */
    AT_SetNotNull,            /* alter column set not null */
    AT_SetExpression,         /* alter column set expression */
    AT_DropExpression,        /* alter column drop expression */
    AT_SetStatistics,         /* alter column set statistics */
    AT_SetOptions,            /* alter column set ( options ) */
    AT_ResetOptions,          /* alter column reset ( options ) */
    AT_SetStorage,            /* alter column set storage */
    AT_SetCompression,        /* alter column set compression */
    AT_DropColumn,            /* drop column */
    AT_AddIndex,              /* add index */
    AT_ReAddIndex,            /* internal to commands/tablecmds.c */
    AT_AddConstraint,         /* add constraint */
    AT_ReAddConstraint,       /* internal to commands/tablecmds.c */
    AT_ReAddDomainConstraint, /* internal to commands/tablecmds.c */
    AT_AlterConstraint,       /* alter constraint */
    AT_ValidateConstraint,    /* validate constraint */
    AT_AddIndexConstraint,    /* add constraint using existing index */
    AT_DropConstraint,        /* drop constraint */
    AT_ReAddComment,          /* internal to commands/tablecmds.c */
    AT_AlterColumnType,       /* alter column type */
    AT_AlterColumnGenericOptions, /* alter column OPTIONS (...) */
    AT_ChangeOwner,           /* change owner */
    AT_ClusterOn,             /* CLUSTER ON */
    AT_DropCluster,           /* SET WITHOUT CLUSTER */
    AT_SetLogged,             /* SET LOGGED */
    AT_SetUnLogged,           /* SET UNLOGGED */
    AT_DropOids,              /* SET WITHOUT OIDS */
    AT_SetAccessMethod,       /* SET ACCESS METHOD */
    AT_SetTableSpace,         /* SET TABLESPACE */
    AT_SetRelOptions,         /* SET (...) -- AM specific parameters */
    AT_ResetRelOptions,       /* RESET (...) -- AM specific parameters */
    AT_ReplaceRelOptions,     /* replace reloption list in its entirety */
    AT_EnableTrig,            /* ENABLE TRIGGER name */
    AT_EnableAlwaysTrig,      /* ENABLE ALWAYS TRIGGER name */
    AT_EnableReplicaTrig,     /* ENABLE REPLICA TRIGGER name */
    AT_DisableTrig,           /* DISABLE TRIGGER name */
    AT_EnableTrigAll,         /* ENABLE TRIGGER ALL */
    AT_DisableTrigAll,        /* DISABLE TRIGGER ALL */
    AT_EnableTrigUser,        /* ENABLE TRIGGER USER */
    AT_DisableTrigUser,       /* DISABLE TRIGGER USER */
    AT_EnableRule,            /* ENABLE RULE name */
    AT_EnableAlwaysRule,      /* ENABLE ALWAYS RULE name */
    AT_EnableReplicaRule,     /* ENABLE REPLICA RULE name */
    AT_DisableRule,           /* DISABLE RULE name */
    AT_AddInherit,            /* INHERIT parent */
    AT_DropInherit,           /* NO INHERIT parent */
    AT_AddOf,                 /* OF <type_name> */
    AT_DropOf,                /* NOT OF */
    AT_ReplicaIdentity,       /* REPLICA IDENTITY */
    AT_EnableRowSecurity,     /* ENABLE ROW SECURITY */
    AT_DisableRowSecurity,    /* DISABLE ROW SECURITY */
    AT_ForceRowSecurity,      /* FORCE ROW SECURITY */
    AT_NoForceRowSecurity,    /* NO FORCE ROW SECURITY */
    AT_GenericOptions,        /* OPTIONS (...) */
    AT_AttachPartition,       /* ATTACH PARTITION */
    AT_DetachPartition,       /* DETACH PARTITION */
    AT_DetachPartitionFinalize, /* DETACH PARTITION FINALIZE */
    AT_AddIdentity,           /* ADD IDENTITY */
    AT_SetIdentity,           /* SET identity column options */
    AT_DropIdentity,          /* DROP IDENTITY */
    AT_ReAddStatistics,       /* internal to commands/tablecmds.c */
}
pub use AlterTableType::*;

#[repr(C)]
pub struct AlterTableCmd /* one subcommand of an ALTER TABLE */ {
    pub r#type: NodeTag,
    pub subtype: AlterTableType, /* Type of table alteration to apply */
    pub name: *mut c_char,      /* column, constraint, or trigger to act on,
                                 * or tablespace, access method */
    pub num: int16,             /* attribute number for columns referenced by
                                 * number */
    pub newowner: *mut RoleSpec,
    pub def: *mut Node,         /* definition of new column, index,
                                 * constraint, or parent table */
    pub behavior: DropBehavior, /* RESTRICT or CASCADE for DROP cases */
    pub missing_ok: bool,       /* skip error if missing? */
    pub recurse: bool,          /* exec-time recursion */
}

/* Ad-hoc node for AT_AlterConstraint */
#[repr(C)]
pub struct ATAlterConstraint {
    pub r#type: NodeTag,
    pub conname: *mut c_char,    /* Constraint name */
    pub alterEnforceability: bool, /* changing enforceability properties? */
    pub is_enforced: bool,       /* ENFORCED? */
    pub alterDeferrability: bool, /* changing deferrability properties? */
    pub deferrable: bool,        /* DEFERRABLE? */
    pub initdeferred: bool,      /* INITIALLY DEFERRED? */
    pub alterInheritability: bool, /* changing inheritability properties */
    pub noinherit: bool,
}

/* Ad-hoc node for AT_ReplicaIdentity */
#[repr(C)]
pub struct ReplicaIdentityStmt {
    pub r#type: NodeTag,
    pub identity_type: c_char,
    pub name: *mut c_char,
}

/* ----------------------
 * Alter Collation
 * ----------------------
 */
#[repr(C)]
pub struct AlterCollationStmt {
    pub r#type: NodeTag,
    pub collname: *mut List,
}

/* ----------------------
 *	Alter Domain
 *
 * The fields are used in different ways by the different variants of
 * this command.
 * ----------------------
 */
#[repr(C)]
pub struct AlterDomainStmt {
    pub r#type: NodeTag,
    pub subtype: c_char, /*------------
                          *	T = alter column default
                          *	N = alter column drop not null
                          *	O = alter column set not null
                          *	C = add constraint
                          *	X = drop constraint
                          *------------
                          */
    pub typeName: *mut List,    /* domain to work on */
    pub name: *mut c_char,      /* column or constraint name to act on */
    pub def: *mut Node,         /* definition of default or constraint */
    pub behavior: DropBehavior, /* RESTRICT or CASCADE for DROP cases */
    pub missing_ok: bool,       /* skip error if missing? */
}

/* ----------------------
 *		Grant|Revoke Statement
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GrantTargetType {
    ACL_TARGET_OBJECT,        /* grant on specific named object(s) */
    ACL_TARGET_ALL_IN_SCHEMA, /* grant on all objects in given schema(s) */
    ACL_TARGET_DEFAULTS,      /* ALTER DEFAULT PRIVILEGES */
}
pub use GrantTargetType::*;

#[repr(C)]
pub struct GrantStmt {
    pub r#type: NodeTag,
    pub is_grant: bool,         /* true = GRANT, false = REVOKE */
    pub targtype: GrantTargetType, /* type of the grant target */
    pub objtype: ObjectType,    /* kind of object being operated on */
    pub objects: *mut List,     /* list of RangeVar nodes, ObjectWithArgs
                                 * nodes, or plain names (as String values) */
    pub privileges: *mut List,  /* list of AccessPriv nodes */
    /* privileges == NIL denotes ALL PRIVILEGES */
    pub grantees: *mut List,    /* list of RoleSpec nodes */
    pub grant_option: bool,     /* grant or revoke grant option */
    pub grantor: *mut RoleSpec,
    pub behavior: DropBehavior, /* drop behavior (for REVOKE) */
}

/*
 * ObjectWithArgs represents a function/procedure/operator name plus parameter
 * identification.
 *
 * objargs includes only the types of the input parameters of the object.
 * In some contexts, that will be all we have, and it's enough to look up
 * objects according to the traditional Postgres rules (i.e., when only input
 * arguments matter).
 *
 * objfuncargs, if not NIL, carries the full specification of the parameter
 * list, including parameter mode annotations.
 *
 * Some grammar productions can set args_unspecified = true instead of
 * providing parameter info.  In this case, lookup will succeed only if
 * the object name is unique.  Note that otherwise, NIL parameter lists
 * mean zero arguments.
 */
#[repr(C)]
pub struct ObjectWithArgs {
    pub r#type: NodeTag,
    pub objname: *mut List,    /* qualified name of function/operator */
    pub objargs: *mut List,    /* list of Typename nodes (input args only) */
    pub objfuncargs: *mut List, /* list of FunctionParameter nodes */
    pub args_unspecified: bool, /* argument list was omitted? */
}

/*
 * An access privilege, with optional list of column names
 * priv_name == NULL denotes ALL PRIVILEGES (only used with a column list)
 * cols == NIL denotes "all columns"
 * Note that simple "ALL PRIVILEGES" is represented as a NIL list, not
 * an AccessPriv with both fields null.
 */
#[repr(C)]
pub struct AccessPriv {
    pub r#type: NodeTag,
    pub priv_name: *mut c_char, /* string name of privilege */
    pub cols: *mut List,        /* list of String */
}

/* ----------------------
 *		Grant/Revoke Role Statement
 *
 * Note: because of the parsing ambiguity with the GRANT <privileges>
 * statement, granted_roles is a list of AccessPriv; the execution code
 * should complain if any column lists appear.  grantee_roles is a list
 * of role names, as String values.
 * ----------------------
 */
#[repr(C)]
pub struct GrantRoleStmt {
    pub r#type: NodeTag,
    pub granted_roles: *mut List, /* list of roles to be granted/revoked */
    pub grantee_roles: *mut List, /* list of member roles to add/delete */
    pub is_grant: bool,         /* true = GRANT, false = REVOKE */
    pub opt: *mut List,         /* options e.g. WITH GRANT OPTION */
    pub grantor: *mut RoleSpec, /* set grantor to other than current role */
    pub behavior: DropBehavior, /* drop behavior (for REVOKE) */
}

/* ----------------------
 *	Alter Default Privileges Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterDefaultPrivilegesStmt {
    pub r#type: NodeTag,
    pub options: *mut List,    /* list of DefElem */
    pub action: *mut GrantStmt, /* GRANT/REVOKE action (with objects=NIL) */
}

/* ----------------------
 *		Copy Statement
 *
 * We support "COPY relation FROM file", "COPY relation TO file", and
 * "COPY (query) TO file".  In any given CopyStmt, exactly one of "relation"
 * and "query" must be non-NULL.
 * ----------------------
 */
#[repr(C)]
pub struct CopyStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* the relation to copy */
    pub query: *mut Node,       /* the query (SELECT or DML statement with
                                 * RETURNING) to copy, as a raw parse tree */
    pub attlist: *mut List,     /* List of column names (as Strings), or NIL
                                 * for all columns */
    pub is_from: bool,          /* TO or FROM */
    pub is_program: bool,       /* is 'filename' a program to popen? */
    pub filename: *mut c_char,  /* filename, or NULL for STDIN/STDOUT */
    pub options: *mut List,     /* List of DefElem nodes */
    pub whereClause: *mut Node, /* WHERE condition (or NULL) */
}

/* ----------------------
 * SET Statement (includes RESET)
 *
 * "SET var TO DEFAULT" and "RESET var" are semantically equivalent, but we
 * preserve the distinction in VariableSetKind for CreateCommandTag().
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VariableSetKind {
    VAR_SET_VALUE,   /* SET var = value */
    VAR_SET_DEFAULT, /* SET var TO DEFAULT */
    VAR_SET_CURRENT, /* SET var FROM CURRENT */
    VAR_SET_MULTI,   /* special case for SET TRANSACTION ... */
    VAR_RESET,       /* RESET var */
    VAR_RESET_ALL,   /* RESET ALL */
}
pub use VariableSetKind::*;

#[repr(C)]
pub struct VariableSetStmt {
    // pg_node_attr(custom_query_jumble)
    pub r#type: NodeTag,
    pub kind: VariableSetKind,
    /* variable to be set */
    pub name: *mut c_char,
    /* List of A_Const nodes */
    pub args: *mut List,

    /*
     * True if arguments should be accounted for in query jumbling.  We use a
     * separate flag rather than query_jumble_ignore on "args" as several
     * grammar flavors of SET rely on a list of values that are parsed
     * directly from the grammar's keywords.
     */
    pub jumble_args: bool,
    /* SET LOCAL? */
    pub is_local: bool,
    /* token location, or -1 if unknown */
    pub location: ParseLoc, // pg_node_attr(query_jumble_location)
}

/* ----------------------
 * Show Statement
 * ----------------------
 */
#[repr(C)]
pub struct VariableShowStmt {
    pub r#type: NodeTag,
    pub name: *mut c_char,
}

/* ----------------------
 *		Create Table Statement
 *
 * NOTE: in the raw gram.y output, ColumnDef and Constraint nodes are
 * intermixed in tableElts, and constraints and nnconstraints are NIL.  After
 * parse analysis, tableElts contains just ColumnDefs, nnconstraints contains
 * Constraint nodes of CONSTR_NOTNULL type from various sources, and
 * constraints contains just CONSTR_CHECK Constraint nodes.
 * ----------------------
 */
#[repr(C)]
pub struct CreateStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation to create */
    pub tableElts: *mut List,    /* column definitions (list of ColumnDef) */
    pub inhRelations: *mut List, /* relations to inherit from (list of
                                  * RangeVar) */
    pub partbound: *mut PartitionBoundSpec, /* FOR VALUES clause */
    pub partspec: *mut PartitionSpec, /* PARTITION BY clause */
    pub ofTypename: *mut TypeName, /* OF typename */
    pub constraints: *mut List,  /* constraints (list of Constraint nodes) */
    pub nnconstraints: *mut List, /* NOT NULL constraints (ditto) */
    pub options: *mut List,      /* options from WITH clause */
    pub oncommit: OnCommitAction, /* what do we do at COMMIT? */
    pub tablespacename: *mut c_char, /* table space to use, or NULL */
    pub accessMethod: *mut c_char, /* table access method */
    pub if_not_exists: bool,     /* just do nothing if it already exists? */
}

/* ----------
 * Definitions for constraints in CreateStmt
 *
 * Note that column defaults are treated as a type of constraint,
 * even though that's a bit odd semantically.
 *
 * For constraints that use expressions (CONSTR_CHECK, CONSTR_DEFAULT)
 * we may have the expression in either "raw" form (an untransformed
 * parse tree) or "cooked" form (the nodeToString representation of
 * an executable expression tree), depending on how this Constraint
 * node was created (by parsing, or by inheritance from an existing
 * relation).  We should never have both in the same node!
 *
 * FKCONSTR_ACTION_xxx values are stored into pg_constraint.confupdtype
 * and pg_constraint.confdeltype columns; FKCONSTR_MATCH_xxx values are
 * stored into pg_constraint.confmatchtype.  Changing the code values may
 * require an initdb!
 *
 * If skip_validation is true then we skip checking that the existing rows
 * in the table satisfy the constraint, and just install the catalog entries
 * for the constraint.  A new FK constraint is marked as valid iff
 * initially_valid is true.  (Usually skip_validation and initially_valid
 * are inverses, but we can set both true if the table is known empty.)
 *
 * Constraint attributes (DEFERRABLE etc) are initially represented as
 * separate Constraint nodes for simplicity of parsing.  parse_utilcmd.c makes
 * a pass through the constraints list to insert the info into the appropriate
 * Constraint node.
 * ----------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConstrType /* types of constraints */ {
    CONSTR_NULL, /* not standard SQL, but a lot of people
                  * expect it */
    CONSTR_NOTNULL,
    CONSTR_DEFAULT,
    CONSTR_IDENTITY,
    CONSTR_GENERATED,
    CONSTR_CHECK,
    CONSTR_PRIMARY,
    CONSTR_UNIQUE,
    CONSTR_EXCLUSION,
    CONSTR_FOREIGN,
    CONSTR_ATTR_DEFERRABLE, /* attributes for previous constraint node */
    CONSTR_ATTR_NOT_DEFERRABLE,
    CONSTR_ATTR_DEFERRED,
    CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_ENFORCED,
    CONSTR_ATTR_NOT_ENFORCED,
}
pub use ConstrType::*;

/* Foreign key action codes */
pub const FKCONSTR_ACTION_NOACTION: c_char = b'a' as c_char;
pub const FKCONSTR_ACTION_RESTRICT: c_char = b'r' as c_char;
pub const FKCONSTR_ACTION_CASCADE: c_char = b'c' as c_char;
pub const FKCONSTR_ACTION_SETNULL: c_char = b'n' as c_char;
pub const FKCONSTR_ACTION_SETDEFAULT: c_char = b'd' as c_char;

/* Foreign key matchtype codes */
pub const FKCONSTR_MATCH_FULL: c_char = b'f' as c_char;
pub const FKCONSTR_MATCH_PARTIAL: c_char = b'p' as c_char;
pub const FKCONSTR_MATCH_SIMPLE: c_char = b's' as c_char;

#[repr(C)]
pub struct Constraint {
    pub r#type: NodeTag,
    pub contype: ConstrType,    /* see above */
    pub conname: *mut c_char,   /* Constraint name, or NULL if unnamed */
    pub deferrable: bool,       /* DEFERRABLE? */
    pub initdeferred: bool,     /* INITIALLY DEFERRED? */
    pub is_enforced: bool,      /* enforced constraint? */
    pub skip_validation: bool,  /* skip validation of existing rows? */
    pub initially_valid: bool,  /* mark the new constraint as valid? */
    pub is_no_inherit: bool,    /* is constraint non-inheritable? */
    pub raw_expr: *mut Node,    /* CHECK or DEFAULT expression, as
                                 * untransformed parse tree */
    pub cooked_expr: *mut c_char, /* CHECK or DEFAULT expression, as
                                   * nodeToString representation */
    pub generated_when: c_char, /* ALWAYS or BY DEFAULT */
    pub generated_kind: c_char, /* STORED or VIRTUAL */
    pub nulls_not_distinct: bool, /* null treatment for UNIQUE constraints */
    pub keys: *mut List,        /* String nodes naming referenced key
                                 * column(s); for UNIQUE/PK/NOT NULL */
    pub without_overlaps: bool, /* WITHOUT OVERLAPS specified */
    pub including: *mut List,   /* String nodes naming referenced nonkey
                                 * column(s); for UNIQUE/PK */
    pub exclusions: *mut List,  /* list of (IndexElem, operator name) pairs;
                                 * for exclusion constraints */
    pub options: *mut List,     /* options from WITH clause */
    pub indexname: *mut c_char, /* existing index to use; otherwise NULL */
    pub indexspace: *mut c_char, /* index tablespace; NULL for default */
    pub reset_default_tblspc: bool, /* reset default_tablespace prior to
                                     * creating the index */
    pub access_method: *mut c_char, /* index access method; NULL for default */
    pub where_clause: *mut Node, /* partial index predicate */

    /* Fields used for FOREIGN KEY constraints: */
    pub pktable: *mut RangeVar, /* Primary key table */
    pub fk_attrs: *mut List,    /* Attributes of foreign key */
    pub pk_attrs: *mut List,    /* Corresponding attrs in PK table */
    pub fk_with_period: bool,   /* Last attribute of FK uses PERIOD */
    pub pk_with_period: bool,   /* Last attribute of PK uses PERIOD */
    pub fk_matchtype: c_char,   /* FULL, PARTIAL, SIMPLE */
    pub fk_upd_action: c_char,  /* ON UPDATE action */
    pub fk_del_action: c_char,  /* ON DELETE action */
    pub fk_del_set_cols: *mut List, /* ON DELETE SET NULL/DEFAULT (col1, col2) */
    pub old_conpfeqop: *mut List, /* pg_constraint.conpfeqop of my former self */
    pub old_pktable_oid: Oid,   /* pg_constraint.confrelid of my former
                                 * self */

    pub location: ParseLoc, /* token location, or -1 if unknown */
}

/* ----------------------
 *		Create/Drop Table Space Statements
 * ----------------------
 */
#[repr(C)]
pub struct CreateTableSpaceStmt {
    pub r#type: NodeTag,
    pub tablespacename: *mut c_char,
    pub owner: *mut RoleSpec,
    pub location: *mut c_char,
    pub options: *mut List,
}

#[repr(C)]
pub struct DropTableSpaceStmt {
    pub r#type: NodeTag,
    pub tablespacename: *mut c_char,
    pub missing_ok: bool, /* skip error if missing? */
}

#[repr(C)]
pub struct AlterTableSpaceOptionsStmt {
    pub r#type: NodeTag,
    pub tablespacename: *mut c_char,
    pub options: *mut List,
    pub isReset: bool,
}

#[repr(C)]
pub struct AlterTableMoveAllStmt {
    pub r#type: NodeTag,
    pub orig_tablespacename: *mut c_char,
    pub objtype: ObjectType, /* Object type to move */
    pub roles: *mut List,    /* List of roles to move objects of */
    pub new_tablespacename: *mut c_char,
    pub nowait: bool,
}

/* ----------------------
 *		Create/Alter Extension Statements
 * ----------------------
 */
#[repr(C)]
pub struct CreateExtensionStmt {
    pub r#type: NodeTag,
    pub extname: *mut c_char,
    pub if_not_exists: bool, /* just do nothing if it already exists? */
    pub options: *mut List,  /* List of DefElem nodes */
}

/* Only used for ALTER EXTENSION UPDATE; later might need an action field */
#[repr(C)]
pub struct AlterExtensionStmt {
    pub r#type: NodeTag,
    pub extname: *mut c_char,
    pub options: *mut List, /* List of DefElem nodes */
}

#[repr(C)]
pub struct AlterExtensionContentsStmt {
    pub r#type: NodeTag,
    pub extname: *mut c_char, /* Extension's name */
    pub action: c_int,        /* +1 = add object, -1 = drop object */
    pub objtype: ObjectType,  /* Object's type */
    pub object: *mut Node,    /* Qualified name of the object */
}

/* ----------------------
 *		Create/Alter FOREIGN DATA WRAPPER Statements
 * ----------------------
 */
#[repr(C)]
pub struct CreateFdwStmt {
    pub r#type: NodeTag,
    pub fdwname: *mut c_char,     /* foreign-data wrapper name */
    pub func_options: *mut List, /* HANDLER/VALIDATOR options */
    pub options: *mut List,      /* generic options to FDW */
}

#[repr(C)]
pub struct AlterFdwStmt {
    pub r#type: NodeTag,
    pub fdwname: *mut c_char,     /* foreign-data wrapper name */
    pub func_options: *mut List, /* HANDLER/VALIDATOR options */
    pub options: *mut List,      /* generic options to FDW */
}

/* ----------------------
 *		Create/Alter FOREIGN SERVER Statements
 * ----------------------
 */
#[repr(C)]
pub struct CreateForeignServerStmt {
    pub r#type: NodeTag,
    pub servername: *mut c_char, /* server name */
    pub servertype: *mut c_char, /* optional server type */
    pub version: *mut c_char,    /* optional server version */
    pub fdwname: *mut c_char,    /* FDW name */
    pub if_not_exists: bool,     /* just do nothing if it already exists? */
    pub options: *mut List,      /* generic options to server */
}

#[repr(C)]
pub struct AlterForeignServerStmt {
    pub r#type: NodeTag,
    pub servername: *mut c_char, /* server name */
    pub version: *mut c_char,    /* optional server version */
    pub options: *mut List,      /* generic options to server */
    pub has_version: bool,       /* version specified */
}

/* ----------------------
 *		Create FOREIGN TABLE Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateForeignTableStmt {
    pub base: CreateStmt,
    pub servername: *mut c_char,
    pub options: *mut List,
}

/* ----------------------
 *		Create/Drop USER MAPPING Statements
 * ----------------------
 */
#[repr(C)]
pub struct CreateUserMappingStmt {
    pub r#type: NodeTag,
    pub user: *mut RoleSpec,     /* user role */
    pub servername: *mut c_char, /* server name */
    pub if_not_exists: bool,     /* just do nothing if it already exists? */
    pub options: *mut List,      /* generic options to server */
}

#[repr(C)]
pub struct AlterUserMappingStmt {
    pub r#type: NodeTag,
    pub user: *mut RoleSpec,     /* user role */
    pub servername: *mut c_char, /* server name */
    pub options: *mut List,      /* generic options to server */
}

#[repr(C)]
pub struct DropUserMappingStmt {
    pub r#type: NodeTag,
    pub user: *mut RoleSpec,     /* user role */
    pub servername: *mut c_char, /* server name */
    pub missing_ok: bool,        /* ignore missing mappings */
}

/* ----------------------
 *		Import Foreign Schema Statement
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ImportForeignSchemaType {
    FDW_IMPORT_SCHEMA_ALL,      /* all relations wanted */
    FDW_IMPORT_SCHEMA_LIMIT_TO, /* include only listed tables in import */
    FDW_IMPORT_SCHEMA_EXCEPT,   /* exclude listed tables from import */
}
pub use ImportForeignSchemaType::*;

#[repr(C)]
pub struct ImportForeignSchemaStmt {
    pub r#type: NodeTag,
    pub server_name: *mut c_char, /* FDW server name */
    pub remote_schema: *mut c_char, /* remote schema name to query */
    pub local_schema: *mut c_char, /* local schema to create objects in */
    pub list_type: ImportForeignSchemaType, /* type of table list */
    pub table_list: *mut List,    /* List of RangeVar */
    pub options: *mut List,       /* list of options to pass to FDW */
}

/*----------------------
 *		Create POLICY Statement
 *----------------------
 */
#[repr(C)]
pub struct CreatePolicyStmt {
    pub r#type: NodeTag,
    pub policy_name: *mut c_char, /* Policy's name */
    pub table: *mut RangeVar,    /* the table name the policy applies to */
    pub cmd_name: *mut c_char,   /* the command name the policy applies to */
    pub permissive: bool,        /* restrictive or permissive policy */
    pub roles: *mut List,        /* the roles associated with the policy */
    pub qual: *mut Node,         /* the policy's condition */
    pub with_check: *mut Node,   /* the policy's WITH CHECK condition. */
}

/*----------------------
 *		Alter POLICY Statement
 *----------------------
 */
#[repr(C)]
pub struct AlterPolicyStmt {
    pub r#type: NodeTag,
    pub policy_name: *mut c_char, /* Policy's name */
    pub table: *mut RangeVar,    /* the table name the policy applies to */
    pub roles: *mut List,        /* the roles associated with the policy */
    pub qual: *mut Node,         /* the policy's condition */
    pub with_check: *mut Node,   /* the policy's WITH CHECK condition. */
}

/*----------------------
 *		Create ACCESS METHOD Statement
 *----------------------
 */
#[repr(C)]
pub struct CreateAmStmt {
    pub r#type: NodeTag,
    pub amname: *mut c_char,      /* access method name */
    pub handler_name: *mut List, /* handler function name */
    pub amtype: c_char,          /* type of access method */
}

/* ----------------------
 *		Create TRIGGER Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateTrigStmt {
    pub r#type: NodeTag,
    pub replace: bool,      /* replace trigger if already exists */
    pub isconstraint: bool, /* This is a constraint trigger */
    pub trigname: *mut c_char, /* TRIGGER's name */
    pub relation: *mut RangeVar, /* relation trigger is on */
    pub funcname: *mut List, /* qual. name of function to call */
    pub args: *mut List,    /* list of String or NIL */
    pub row: bool,          /* ROW/STATEMENT */
    /* timing uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
    pub timing: int16, /* BEFORE, AFTER, or INSTEAD */
    /* events uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
    pub events: int16,        /* "OR" of INSERT/UPDATE/DELETE/TRUNCATE */
    pub columns: *mut List,   /* column names, or NIL for all columns */
    pub whenClause: *mut Node, /* qual expression, or NULL if none */
    /* explicitly named transition data */
    pub transitionRels: *mut List, /* TriggerTransition nodes, or NIL if none */
    /* The remaining fields are only used for constraint triggers */
    pub deferrable: bool,   /* [NOT] DEFERRABLE */
    pub initdeferred: bool, /* INITIALLY {DEFERRED|IMMEDIATE} */
    pub constrrel: *mut RangeVar, /* opposite relation, if RI trigger */
}

/* ----------------------
 *		Create EVENT TRIGGER Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateEventTrigStmt {
    pub r#type: NodeTag,
    pub trigname: *mut c_char,  /* TRIGGER's name */
    pub eventname: *mut c_char, /* event's identifier */
    pub whenclause: *mut List,  /* list of DefElems indicating filtering */
    pub funcname: *mut List,    /* qual. name of function to call */
}

/* ----------------------
 *		Alter EVENT TRIGGER Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterEventTrigStmt {
    pub r#type: NodeTag,
    pub trigname: *mut c_char, /* TRIGGER's name */
    pub tgenabled: c_char,     /* trigger's firing configuration WRT
                                * session_replication_role */
}

/* ----------------------
 *		Create LANGUAGE Statements
 * ----------------------
 */
#[repr(C)]
pub struct CreatePLangStmt {
    pub r#type: NodeTag,
    pub replace: bool,         /* T => replace if already exists */
    pub plname: *mut c_char,   /* PL name */
    pub plhandler: *mut List,  /* PL call handler function (qual. name) */
    pub plinline: *mut List,   /* optional inline function (qual. name) */
    pub plvalidator: *mut List, /* optional validator function (qual. name) */
    pub pltrusted: bool,       /* PL is trusted */
}

/* ----------------------
 *	Create/Alter/Drop Role Statements
 *
 * Note: these node types are also used for the backwards-compatible
 * Create/Alter/Drop User/Group statements.  In the ALTER and DROP cases
 * there's really no need to distinguish what the original spelling was,
 * but for CREATE we mark the type because the defaults vary.
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RoleStmtType {
    ROLESTMT_ROLE,
    ROLESTMT_USER,
    ROLESTMT_GROUP,
}
pub use RoleStmtType::*;

#[repr(C)]
pub struct CreateRoleStmt {
    pub r#type: NodeTag,
    pub stmt_type: RoleStmtType, /* ROLE/USER/GROUP */
    pub role: *mut c_char,      /* role name */
    pub options: *mut List,     /* List of DefElem nodes */
}

#[repr(C)]
pub struct AlterRoleStmt {
    pub r#type: NodeTag,
    pub role: *mut RoleSpec, /* role */
    pub options: *mut List,  /* List of DefElem nodes */
    pub action: c_int,       /* +1 = add members, -1 = drop members */
}

#[repr(C)]
pub struct AlterRoleSetStmt {
    pub r#type: NodeTag,
    pub role: *mut RoleSpec,    /* role */
    pub database: *mut c_char,  /* database name, or NULL */
    pub setstmt: *mut VariableSetStmt, /* SET or RESET subcommand */
}

#[repr(C)]
pub struct DropRoleStmt {
    pub r#type: NodeTag,
    pub roles: *mut List, /* List of roles to remove */
    pub missing_ok: bool, /* skip error if a role is missing? */
}

/* ----------------------
 *		{Create|Alter} SEQUENCE Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateSeqStmt {
    pub r#type: NodeTag,
    pub sequence: *mut RangeVar, /* the sequence to create */
    pub options: *mut List,
    pub ownerId: Oid, /* ID of owner, or InvalidOid for default */
    pub for_identity: bool,
    pub if_not_exists: bool, /* just do nothing if it already exists? */
}

#[repr(C)]
pub struct AlterSeqStmt {
    pub r#type: NodeTag,
    pub sequence: *mut RangeVar, /* the sequence to alter */
    pub options: *mut List,
    pub for_identity: bool,
    pub missing_ok: bool, /* skip error if a role is missing? */
}

/* ----------------------
 *		Create {Aggregate|Operator|Type} Statement
 * ----------------------
 */
#[repr(C)]
pub struct DefineStmt {
    pub r#type: NodeTag,
    pub kind: ObjectType,    /* aggregate, operator, type */
    pub oldstyle: bool,      /* hack to signal old CREATE AGG syntax */
    pub defnames: *mut List, /* qualified name (list of String) */
    pub args: *mut List,     /* a list of TypeName (if needed) */
    pub definition: *mut List, /* a list of DefElem */
    pub if_not_exists: bool, /* just do nothing if it already exists? */
    pub replace: bool,       /* replace if already exists? */
}

/* ----------------------
 *		Create Domain Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateDomainStmt {
    pub r#type: NodeTag,
    pub domainname: *mut List,  /* qualified name (list of String) */
    pub typeName: *mut TypeName, /* the base type */
    pub collClause: *mut CollateClause, /* untransformed COLLATE spec, if any */
    pub constraints: *mut List, /* constraints (list of Constraint nodes) */
}

/* ----------------------
 *		Create Operator Class Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateOpClassStmt {
    pub r#type: NodeTag,
    pub opclassname: *mut List, /* qualified name (list of String) */
    pub opfamilyname: *mut List, /* qualified name (ditto); NIL if omitted */
    pub amname: *mut c_char,    /* name of index AM opclass is for */
    pub datatype: *mut TypeName, /* datatype of indexed column */
    pub items: *mut List,       /* List of CreateOpClassItem nodes */
    pub isDefault: bool,        /* Should be marked as default for type? */
}

pub const OPCLASS_ITEM_OPERATOR: c_int = 1;
pub const OPCLASS_ITEM_FUNCTION: c_int = 2;
pub const OPCLASS_ITEM_STORAGETYPE: c_int = 3;

#[repr(C)]
pub struct CreateOpClassItem {
    pub r#type: NodeTag,
    pub itemtype: c_int,       /* see codes above */
    pub name: *mut ObjectWithArgs, /* operator or function name and args */
    pub number: c_int,         /* strategy num or support proc num */
    pub order_family: *mut List, /* only used for ordering operators */
    pub class_args: *mut List, /* amproclefttype/amprocrighttype or
                                * amoplefttype/amoprighttype */
    /* fields used for a storagetype item: */
    pub storedtype: *mut TypeName, /* datatype stored in index */
}

/* ----------------------
 *		Create Operator Family Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateOpFamilyStmt {
    pub r#type: NodeTag,
    pub opfamilyname: *mut List, /* qualified name (list of String) */
    pub amname: *mut c_char,    /* name of index AM opfamily is for */
}

/* ----------------------
 *		Alter Operator Family Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterOpFamilyStmt {
    pub r#type: NodeTag,
    pub opfamilyname: *mut List, /* qualified name (list of String) */
    pub amname: *mut c_char,    /* name of index AM opfamily is for */
    pub isDrop: bool,           /* ADD or DROP the items? */
    pub items: *mut List,       /* List of CreateOpClassItem nodes */
}

/* ----------------------
 *		Drop Table|Sequence|View|Index|Type|Domain|Conversion|Schema Statement
 * ----------------------
 */
#[repr(C)]
pub struct DropStmt {
    pub r#type: NodeTag,
    pub objects: *mut List,     /* list of names */
    pub removeType: ObjectType, /* object type */
    pub behavior: DropBehavior, /* RESTRICT or CASCADE behavior */
    pub missing_ok: bool,       /* skip error if object is missing? */
    pub concurrent: bool,       /* drop index concurrently? */
}

/* ----------------------
 *				Truncate Table Statement
 * ----------------------
 */
#[repr(C)]
pub struct TruncateStmt {
    pub r#type: NodeTag,
    pub relations: *mut List,   /* relations (RangeVars) to be truncated */
    pub restart_seqs: bool,     /* restart owned sequences? */
    pub behavior: DropBehavior, /* RESTRICT or CASCADE behavior */
}

/* ----------------------
 *				Comment On Statement
 * ----------------------
 */
#[repr(C)]
pub struct CommentStmt {
    pub r#type: NodeTag,
    pub objtype: ObjectType,  /* Object's type */
    pub object: *mut Node,    /* Qualified name of the object */
    pub comment: *mut c_char, /* Comment to insert, or NULL to remove */
}

/* ----------------------
 *				SECURITY LABEL Statement
 * ----------------------
 */
#[repr(C)]
pub struct SecLabelStmt {
    pub r#type: NodeTag,
    pub objtype: ObjectType,   /* Object's type */
    pub object: *mut Node,     /* Qualified name of the object */
    pub provider: *mut c_char, /* Label provider (or NULL) */
    pub label: *mut c_char,    /* New security label to be assigned */
}

/* ----------------------
 *		Declare Cursor Statement
 *
 * The "query" field is initially a raw parse tree, and is converted to a
 * Query node during parse analysis.  Note that rewriting and planning
 * of the query are always postponed until execution.
 * ----------------------
 */
pub const CURSOR_OPT_BINARY: c_int = 0x0001; /* BINARY */
pub const CURSOR_OPT_SCROLL: c_int = 0x0002; /* SCROLL explicitly given */
pub const CURSOR_OPT_NO_SCROLL: c_int = 0x0004; /* NO SCROLL explicitly given */
pub const CURSOR_OPT_INSENSITIVE: c_int = 0x0008; /* INSENSITIVE */
pub const CURSOR_OPT_ASENSITIVE: c_int = 0x0010; /* ASENSITIVE */
pub const CURSOR_OPT_HOLD: c_int = 0x0020; /* WITH HOLD */
/* these planner-control flags do not correspond to any SQL grammar: */
pub const CURSOR_OPT_FAST_PLAN: c_int = 0x0100; /* prefer fast-start plan */
pub const CURSOR_OPT_GENERIC_PLAN: c_int = 0x0200; /* force use of generic plan */
pub const CURSOR_OPT_CUSTOM_PLAN: c_int = 0x0400; /* force use of custom plan */
pub const CURSOR_OPT_PARALLEL_OK: c_int = 0x0800; /* parallel mode OK */

#[repr(C)]
pub struct DeclareCursorStmt {
    pub r#type: NodeTag,
    pub portalname: *mut c_char, /* name of the portal (cursor) */
    pub options: c_int,         /* bitmask of options (see above) */
    pub query: *mut Node,       /* the query (see comments above) */
}

/* ----------------------
 *		Close Portal Statement
 * ----------------------
 */
#[repr(C)]
pub struct ClosePortalStmt {
    pub r#type: NodeTag,
    pub portalname: *mut c_char, /* name of the portal (cursor) */
                                 /* NULL means CLOSE ALL */
}

/* ----------------------
 *		Fetch Statement (also Move)
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FetchDirection {
    /* for these, howMany is how many rows to fetch; FETCH_ALL means ALL */
    FETCH_FORWARD,
    FETCH_BACKWARD,
    /* for these, howMany indicates a position; only one row is fetched */
    FETCH_ABSOLUTE,
    FETCH_RELATIVE,
}
pub use FetchDirection::*;

pub const FETCH_ALL: c_long = c_long::MAX; /* LONG_MAX */

#[repr(C)]
pub struct FetchStmt {
    pub r#type: NodeTag,
    pub direction: FetchDirection, /* see above */
    pub howMany: c_long,        /* number of rows, or position argument */
    pub portalname: *mut c_char, /* name of portal (cursor) */
    pub ismove: bool,           /* true if MOVE */
}

/* ----------------------
 *		Create Index Statement
 *
 * This represents creation of an index and/or an associated constraint.
 * If isconstraint is true, we should create a pg_constraint entry along
 * with the index.  But if indexOid isn't InvalidOid, we are not creating an
 * index, just a UNIQUE/PKEY constraint using an existing index.  isconstraint
 * must always be true in this case, and the fields describing the index
 * properties are empty.
 * ----------------------
 */
#[repr(C)]
pub struct IndexStmt {
    pub r#type: NodeTag,
    pub idxname: *mut c_char,    /* name of new index, or NULL for default */
    pub relation: *mut RangeVar, /* relation to build index on */
    pub accessMethod: *mut c_char, /* name of access method (eg. btree) */
    pub tableSpace: *mut c_char, /* tablespace, or NULL for default */
    pub indexParams: *mut List,  /* columns to index: a list of IndexElem */
    pub indexIncludingParams: *mut List, /* additional columns to index: a list
                                          * of IndexElem */
    pub options: *mut List,      /* WITH clause options: a list of DefElem */
    pub whereClause: *mut Node,  /* qualification (partial-index predicate) */
    pub excludeOpNames: *mut List, /* exclusion operator names, or NIL if none */
    pub idxcomment: *mut c_char, /* comment to apply to index, or NULL */
    pub indexOid: Oid,           /* OID of an existing index, if any */
    pub oldNumber: RelFileNumber, /* relfilenumber of existing storage, if any */
    pub oldCreateSubid: SubTransactionId, /* rd_createSubid of oldNumber */
    pub oldFirstRelfilelocatorSubid: SubTransactionId, /* rd_firstRelfilelocatorSubid
                                                         * of oldNumber */
    pub unique: bool,            /* is index unique? */
    pub nulls_not_distinct: bool, /* null treatment for UNIQUE constraints */
    pub primary: bool,           /* is index a primary key? */
    pub isconstraint: bool,      /* is it for a pkey/unique constraint? */
    pub iswithoutoverlaps: bool, /* is the constraint WITHOUT OVERLAPS? */
    pub deferrable: bool,        /* is the constraint DEFERRABLE? */
    pub initdeferred: bool,      /* is the constraint INITIALLY DEFERRED? */
    pub transformed: bool,       /* true when transformIndexStmt is finished */
    pub concurrent: bool,        /* should this be a concurrent index build? */
    pub if_not_exists: bool,     /* just do nothing if index already exists? */
    pub reset_default_tblspc: bool, /* reset default_tablespace prior to
                                     * executing */
}

/* ----------------------
 *		Create Statistics Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateStatsStmt {
    pub r#type: NodeTag,
    pub defnames: *mut List,   /* qualified name (list of String) */
    pub stat_types: *mut List, /* stat types (list of String) */
    pub exprs: *mut List,      /* expressions to build statistics on */
    pub relations: *mut List,  /* rels to build stats on (list of RangeVar) */
    pub stxcomment: *mut c_char, /* comment to apply to stats, or NULL */
    pub transformed: bool,     /* true when transformStatsStmt is finished */
    pub if_not_exists: bool,   /* do nothing if stats name already exists */
}

/*
 * StatsElem - statistics parameters (used in CREATE STATISTICS)
 *
 * For a plain attribute, 'name' is the name of the referenced table column
 * and 'expr' is NULL.  For an expression, 'name' is NULL and 'expr' is the
 * expression tree.
 */
#[repr(C)]
pub struct StatsElem {
    pub r#type: NodeTag,
    pub name: *mut c_char, /* name of attribute to index, or NULL */
    pub expr: *mut Node,   /* expression to index, or NULL */
}

/* ----------------------
 *		Alter Statistics Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterStatsStmt {
    pub r#type: NodeTag,
    pub defnames: *mut List,    /* qualified name (list of String) */
    pub stxstattarget: *mut Node, /* statistics target */
    pub missing_ok: bool,       /* skip error if statistics object is missing */
}

/* ----------------------
 *		Create Function Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateFunctionStmt {
    pub r#type: NodeTag,
    pub is_procedure: bool,    /* it's really CREATE PROCEDURE */
    pub replace: bool,         /* T => replace if already exists */
    pub funcname: *mut List,   /* qualified name of function to create */
    pub parameters: *mut List, /* a list of FunctionParameter */
    pub returnType: *mut TypeName, /* the return type */
    pub options: *mut List,    /* a list of DefElem */
    pub sql_body: *mut Node,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FunctionParameterMode {
    /* the assigned enum values appear in pg_proc, don't change 'em! */
    FUNC_PARAM_IN = b'i' as isize,       /* input only */
    FUNC_PARAM_OUT = b'o' as isize,      /* output only */
    FUNC_PARAM_INOUT = b'b' as isize,    /* both */
    FUNC_PARAM_VARIADIC = b'v' as isize, /* variadic (always input) */
    FUNC_PARAM_TABLE = b't' as isize,    /* table function output column */
    /* this is not used in pg_proc: */
    FUNC_PARAM_DEFAULT = b'd' as isize, /* default; effectively same as IN */
}
pub use FunctionParameterMode::*;

#[repr(C)]
pub struct FunctionParameter {
    pub r#type: NodeTag,
    pub name: *mut c_char,      /* parameter name, or NULL if not given */
    pub argType: *mut TypeName, /* TypeName for parameter type */
    pub mode: FunctionParameterMode, /* IN/OUT/etc */
    pub defexpr: *mut Node,     /* raw default expr, or NULL if not given */
    pub location: ParseLoc,     /* token location, or -1 if unknown */
}

#[repr(C)]
pub struct AlterFunctionStmt {
    pub r#type: NodeTag,
    pub objtype: ObjectType,
    pub func: *mut ObjectWithArgs, /* name and args of function */
    pub actions: *mut List,        /* list of DefElem */
}

/* ----------------------
 *		DO Statement
 *
 * DoStmt is the raw parser output, InlineCodeBlock is the execution-time API
 * ----------------------
 */
#[repr(C)]
pub struct DoStmt {
    pub r#type: NodeTag,
    pub args: *mut List, /* List of DefElem nodes */
}

#[repr(C)]
pub struct InlineCodeBlock {
    // pg_node_attr(nodetag_only) /* this is not a member of parse trees */
    pub r#type: NodeTag,
    pub source_text: *mut c_char, /* source text of anonymous code block */
    pub langOid: Oid,             /* OID of selected language */
    pub langIsTrusted: bool,      /* trusted property of the language */
    pub atomic: bool,             /* atomic execution context */
}

/* ----------------------
 *		CALL statement
 *
 * OUT-mode arguments are removed from the transformed funcexpr.  The outargs
 * list contains copies of the expressions for all output arguments, in the
 * order of the procedure's declared arguments.  (outargs is never evaluated,
 * but is useful to the caller as a reference for what to assign to.)
 * The transformed call state is not relevant in the query jumbling, only the
 * function call is.
 * ----------------------
 */
#[repr(C)]
pub struct CallStmt {
    pub r#type: NodeTag,
    /* from the parser */
    pub funccall: *mut FuncCall, // pg_node_attr(query_jumble_ignore)
    /* transformed call, with only input args */
    pub funcexpr: *mut FuncExpr,
    /* transformed output-argument expressions */
    pub outargs: *mut List,
}

#[repr(C)]
pub struct CallContext {
    // pg_node_attr(nodetag_only) /* this is not a member of parse trees */
    pub r#type: NodeTag,
    pub atomic: bool,
}

/* ----------------------
 *		Alter Object Rename Statement
 * ----------------------
 */
#[repr(C)]
pub struct RenameStmt {
    pub r#type: NodeTag,
    pub renameType: ObjectType,  /* OBJECT_TABLE, OBJECT_COLUMN, etc */
    pub relationType: ObjectType, /* if column name, associated relation type */
    pub relation: *mut RangeVar, /* in case it's a table */
    pub object: *mut Node,       /* in case it's some other object */
    pub subname: *mut c_char,    /* name of contained object (column, rule,
                                  * trigger, etc) */
    pub newname: *mut c_char,    /* the new name */
    pub behavior: DropBehavior,  /* RESTRICT or CASCADE behavior */
    pub missing_ok: bool,        /* skip error if missing? */
}

/* ----------------------
 * ALTER object DEPENDS ON EXTENSION extname
 * ----------------------
 */
#[repr(C)]
pub struct AlterObjectDependsStmt {
    pub r#type: NodeTag,
    pub objectType: ObjectType, /* OBJECT_FUNCTION, OBJECT_TRIGGER, etc */
    pub relation: *mut RangeVar, /* in case a table is involved */
    pub object: *mut Node,      /* name of the object */
    pub extname: *mut String,   /* extension name */
    pub remove: bool,           /* set true to remove dep rather than add */
}

/* ----------------------
 *		ALTER object SET SCHEMA Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterObjectSchemaStmt {
    pub r#type: NodeTag,
    pub objectType: ObjectType, /* OBJECT_TABLE, OBJECT_TYPE, etc */
    pub relation: *mut RangeVar, /* in case it's a table */
    pub object: *mut Node,      /* in case it's some other object */
    pub newschema: *mut c_char, /* the new schema */
    pub missing_ok: bool,       /* skip error if missing? */
}

/* ----------------------
 *		Alter Object Owner Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterOwnerStmt {
    pub r#type: NodeTag,
    pub objectType: ObjectType, /* OBJECT_TABLE, OBJECT_TYPE, etc */
    pub relation: *mut RangeVar, /* in case it's a table */
    pub object: *mut Node,      /* in case it's some other object */
    pub newowner: *mut RoleSpec, /* the new owner */
}

/* ----------------------
 *		Alter Operator Set ( this-n-that )
 * ----------------------
 */
#[repr(C)]
pub struct AlterOperatorStmt {
    pub r#type: NodeTag,
    pub opername: *mut ObjectWithArgs, /* operator name and argument types */
    pub options: *mut List,            /* List of DefElem nodes */
}

/* ------------------------
 *		Alter Type Set ( this-n-that )
 * ------------------------
 */
#[repr(C)]
pub struct AlterTypeStmt {
    pub r#type: NodeTag,
    pub typeName: *mut List, /* type name (possibly qualified) */
    pub options: *mut List,  /* List of DefElem nodes */
}

/* ----------------------
 *		Create Rule Statement
 * ----------------------
 */
#[repr(C)]
pub struct RuleStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation the rule is for */
    pub rulename: *mut c_char,  /* name of the rule */
    pub whereClause: *mut Node, /* qualifications */
    pub event: CmdType,         /* SELECT, INSERT, etc */
    pub instead: bool,          /* is a 'do instead'? */
    pub actions: *mut List,     /* the action statements */
    pub replace: bool,          /* OR REPLACE */
}

/* ----------------------
 *		Notify Statement
 * ----------------------
 */
#[repr(C)]
pub struct NotifyStmt {
    pub r#type: NodeTag,
    pub conditionname: *mut c_char, /* condition name to notify */
    pub payload: *mut c_char,       /* the payload string, or NULL if none */
}

/* ----------------------
 *		Listen Statement
 * ----------------------
 */
#[repr(C)]
pub struct ListenStmt {
    pub r#type: NodeTag,
    pub conditionname: *mut c_char, /* condition name to listen on */
}

/* ----------------------
 *		Unlisten Statement
 * ----------------------
 */
#[repr(C)]
pub struct UnlistenStmt {
    pub r#type: NodeTag,
    pub conditionname: *mut c_char, /* name to unlisten on, or NULL for all */
}

/* ----------------------
 *		{Begin|Commit|Rollback} Transaction Statement
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TransactionStmtKind {
    TRANS_STMT_BEGIN,
    TRANS_STMT_START, /* semantically identical to BEGIN */
    TRANS_STMT_COMMIT,
    TRANS_STMT_ROLLBACK,
    TRANS_STMT_SAVEPOINT,
    TRANS_STMT_RELEASE,
    TRANS_STMT_ROLLBACK_TO,
    TRANS_STMT_PREPARE,
    TRANS_STMT_COMMIT_PREPARED,
    TRANS_STMT_ROLLBACK_PREPARED,
}
pub use TransactionStmtKind::*;

#[repr(C)]
pub struct TransactionStmt {
    pub r#type: NodeTag,
    pub kind: TransactionStmtKind, /* see above */
    pub options: *mut List,     /* for BEGIN/START commands */
    /* for savepoint commands */
    pub savepoint_name: *mut c_char, // pg_node_attr(query_jumble_ignore)
    /* for two-phase-commit related commands */
    pub gid: *mut c_char, // pg_node_attr(query_jumble_ignore)
    pub chain: bool,      /* AND CHAIN option */
    /* token location, or -1 if unknown */
    pub location: ParseLoc, // pg_node_attr(query_jumble_location)
}

/* ----------------------
 *		Create Type Statement, composite types
 * ----------------------
 */
#[repr(C)]
pub struct CompositeTypeStmt {
    pub r#type: NodeTag,
    pub typevar: *mut RangeVar, /* the composite type to be created */
    pub coldeflist: *mut List,  /* list of ColumnDef nodes */
}

/* ----------------------
 *		Create Type Statement, enum types
 * ----------------------
 */
#[repr(C)]
pub struct CreateEnumStmt {
    pub r#type: NodeTag,
    pub typeName: *mut List, /* qualified name (list of String) */
    pub vals: *mut List,     /* enum values (list of String) */
}

/* ----------------------
 *		Create Type Statement, range types
 * ----------------------
 */
#[repr(C)]
pub struct CreateRangeStmt {
    pub r#type: NodeTag,
    pub typeName: *mut List, /* qualified name (list of String) */
    pub params: *mut List,   /* range parameters (list of DefElem) */
}

/* ----------------------
 *		Alter Type Statement, enum types
 * ----------------------
 */
#[repr(C)]
pub struct AlterEnumStmt {
    pub r#type: NodeTag,
    pub typeName: *mut List,    /* qualified name (list of String) */
    pub oldVal: *mut c_char,    /* old enum value's name, if renaming */
    pub newVal: *mut c_char,    /* new enum value's name */
    pub newValNeighbor: *mut c_char, /* neighboring enum value, if specified */
    pub newValIsAfter: bool,    /* place new enum value after neighbor? */
    pub skipIfNewValExists: bool, /* no error if new already exists? */
}

/* ----------------------
 *		Create View Statement
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ViewCheckOption {
    NO_CHECK_OPTION,
    LOCAL_CHECK_OPTION,
    CASCADED_CHECK_OPTION,
}
pub use ViewCheckOption::*;

#[repr(C)]
pub struct ViewStmt {
    pub r#type: NodeTag,
    pub view: *mut RangeVar, /* the view to be created */
    pub aliases: *mut List,  /* target column names */
    pub query: *mut Node,    /* the SELECT query (as a raw parse tree) */
    pub replace: bool,       /* replace an existing view? */
    pub options: *mut List,  /* options from WITH clause */
    pub withCheckOption: ViewCheckOption, /* WITH CHECK OPTION */
}

/* ----------------------
 *		Load Statement
 * ----------------------
 */
#[repr(C)]
pub struct LoadStmt {
    pub r#type: NodeTag,
    pub filename: *mut c_char, /* file to load */
}

/* ----------------------
 *		Createdb Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreatedbStmt {
    pub r#type: NodeTag,
    pub dbname: *mut c_char, /* name of database to create */
    pub options: *mut List,  /* List of DefElem nodes */
}

/* ----------------------
 *	Alter Database
 * ----------------------
 */
#[repr(C)]
pub struct AlterDatabaseStmt {
    pub r#type: NodeTag,
    pub dbname: *mut c_char, /* name of database to alter */
    pub options: *mut List,  /* List of DefElem nodes */
}

#[repr(C)]
pub struct AlterDatabaseRefreshCollStmt {
    pub r#type: NodeTag,
    pub dbname: *mut c_char,
}

#[repr(C)]
pub struct AlterDatabaseSetStmt {
    pub r#type: NodeTag,
    pub dbname: *mut c_char,    /* database name */
    pub setstmt: *mut VariableSetStmt, /* SET or RESET subcommand */
}

/* ----------------------
 *		Dropdb Statement
 * ----------------------
 */
#[repr(C)]
pub struct DropdbStmt {
    pub r#type: NodeTag,
    pub dbname: *mut c_char, /* database to drop */
    pub missing_ok: bool,    /* skip error if db is missing? */
    pub options: *mut List,  /* currently only FORCE is supported */
}

/* ----------------------
 *		Alter System Statement
 * ----------------------
 */
#[repr(C)]
pub struct AlterSystemStmt {
    pub r#type: NodeTag,
    pub setstmt: *mut VariableSetStmt, /* SET subcommand */
}

/* ----------------------
 *		Cluster Statement (support pbrown's cluster index implementation)
 * ----------------------
 */
#[repr(C)]
pub struct ClusterStmt {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation being indexed, or NULL if all */
    pub indexname: *mut c_char, /* original index defined */
    pub params: *mut List,      /* list of DefElem nodes */
}

/* ----------------------
 *		Vacuum and Analyze Statements
 *
 * Even though these are nominally two statements, it's convenient to use
 * just one node type for both.
 * ----------------------
 */
#[repr(C)]
pub struct VacuumStmt {
    pub r#type: NodeTag,
    pub options: *mut List,  /* list of DefElem nodes */
    pub rels: *mut List,     /* list of VacuumRelation, or NIL for all */
    pub is_vacuumcmd: bool,  /* true for VACUUM, false for ANALYZE */
}

/*
 * Info about a single target table of VACUUM/ANALYZE.
 *
 * If the OID field is set, it always identifies the table to process.
 * Then the relation field can be NULL; if it isn't, it's used only to report
 * failure to open/lock the relation.
 */
#[repr(C)]
pub struct VacuumRelation {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* table name to process, or NULL */
    pub oid: Oid,               /* table's OID; InvalidOid if not looked up */
    pub va_cols: *mut List,     /* list of column names, or NIL for all */
}

/* ----------------------
 *		Explain Statement
 *
 * The "query" field is initially a raw parse tree, and is converted to a
 * Query node during parse analysis.  Note that rewriting and planning
 * of the query are always postponed until execution.
 * ----------------------
 */
#[repr(C)]
pub struct ExplainStmt {
    pub r#type: NodeTag,
    pub query: *mut Node,   /* the query (see comments above) */
    pub options: *mut List, /* list of DefElem nodes */
}

/* ----------------------
 *		CREATE TABLE AS Statement (a/k/a SELECT INTO)
 *
 * A query written as CREATE TABLE AS will produce this node type natively.
 * A query written as SELECT ... INTO will be transformed to this form during
 * parse analysis.
 * A query written as CREATE MATERIALIZED view will produce this node type,
 * during parse analysis, since it needs all the same data.
 *
 * The "query" field is handled similarly to EXPLAIN, though note that it
 * can be a SELECT or an EXECUTE, but not other DML statements.
 * ----------------------
 */
#[repr(C)]
pub struct CreateTableAsStmt {
    pub r#type: NodeTag,
    pub query: *mut Node,      /* the query (see comments above) */
    pub into: *mut IntoClause, /* destination table */
    pub objtype: ObjectType,   /* OBJECT_TABLE or OBJECT_MATVIEW */
    pub is_select_into: bool,  /* it was written as SELECT INTO */
    pub if_not_exists: bool,   /* just do nothing if it already exists? */
}

/* ----------------------
 *		REFRESH MATERIALIZED VIEW Statement
 * ----------------------
 */
#[repr(C)]
pub struct RefreshMatViewStmt {
    pub r#type: NodeTag,
    pub concurrent: bool, /* allow concurrent access? */
    pub skipData: bool,   /* true for WITH NO DATA */
    pub relation: *mut RangeVar, /* relation to insert into */
}

/* ----------------------
 * Checkpoint Statement
 * ----------------------
 */
#[repr(C)]
pub struct CheckPointStmt {
    pub r#type: NodeTag,
}

/* ----------------------
 * Discard Statement
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DiscardMode {
    DISCARD_ALL,
    DISCARD_PLANS,
    DISCARD_SEQUENCES,
    DISCARD_TEMP,
}
pub use DiscardMode::*;

#[repr(C)]
pub struct DiscardStmt {
    pub r#type: NodeTag,
    pub target: DiscardMode,
}

/* ----------------------
 *		LOCK Statement
 * ----------------------
 */
#[repr(C)]
pub struct LockStmt {
    pub r#type: NodeTag,
    pub relations: *mut List, /* relations to lock */
    pub mode: c_int,          /* lock mode */
    pub nowait: bool,         /* no wait mode */
}

/* ----------------------
 *		SET CONSTRAINTS Statement
 * ----------------------
 */
#[repr(C)]
pub struct ConstraintsSetStmt {
    pub r#type: NodeTag,
    pub constraints: *mut List, /* List of names as RangeVars */
    pub deferred: bool,
}

/* ----------------------
 *		REINDEX Statement
 * ----------------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReindexObjectType {
    REINDEX_OBJECT_INDEX,    /* index */
    REINDEX_OBJECT_TABLE,    /* table or materialized view */
    REINDEX_OBJECT_SCHEMA,   /* schema */
    REINDEX_OBJECT_SYSTEM,   /* system catalogs */
    REINDEX_OBJECT_DATABASE, /* database */
}
pub use ReindexObjectType::*;

#[repr(C)]
pub struct ReindexStmt {
    pub r#type: NodeTag,
    pub kind: ReindexObjectType, /* REINDEX_OBJECT_INDEX, REINDEX_OBJECT_TABLE,
                                  * etc. */
    pub relation: *mut RangeVar, /* Table or index to reindex */
    pub name: *const c_char,     /* name of database to reindex */
    pub params: *mut List,       /* list of DefElem nodes */
}

/* ----------------------
 *		CREATE CONVERSION Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateConversionStmt {
    pub r#type: NodeTag,
    pub conversion_name: *mut List, /* Name of the conversion */
    pub for_encoding_name: *mut c_char, /* source encoding name */
    pub to_encoding_name: *mut c_char, /* destination encoding name */
    pub func_name: *mut List,    /* qualified conversion function name */
    pub def: bool,               /* is this a default conversion? */
}

/* ----------------------
 *	CREATE CAST Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateCastStmt {
    pub r#type: NodeTag,
    pub sourcetype: *mut TypeName,
    pub targettype: *mut TypeName,
    pub func: *mut ObjectWithArgs,
    pub context: CoercionContext,
    pub inout: bool,
}

/* ----------------------
 *	CREATE TRANSFORM Statement
 * ----------------------
 */
#[repr(C)]
pub struct CreateTransformStmt {
    pub r#type: NodeTag,
    pub replace: bool,
    pub type_name: *mut TypeName,
    pub lang: *mut c_char,
    pub fromsql: *mut ObjectWithArgs,
    pub tosql: *mut ObjectWithArgs,
}

/* ----------------------
 *		PREPARE Statement
 * ----------------------
 */
#[repr(C)]
pub struct PrepareStmt {
    pub r#type: NodeTag,
    pub name: *mut c_char,   /* Name of plan, arbitrary */
    pub argtypes: *mut List, /* Types of parameters (List of TypeName) */
    pub query: *mut Node,    /* The query itself (as a raw parsetree) */
}

/* ----------------------
 *		EXECUTE Statement
 * ----------------------
 */
#[repr(C)]
pub struct ExecuteStmt {
    pub r#type: NodeTag,
    pub name: *mut c_char, /* The name of the plan to execute */
    pub params: *mut List, /* Values to assign to parameters */
}

/* ----------------------
 *		DEALLOCATE Statement
 * ----------------------
 */
#[repr(C)]
pub struct DeallocateStmt {
    pub r#type: NodeTag,
    /* The name of the plan to remove, NULL if DEALLOCATE ALL */
    pub name: *mut c_char, // pg_node_attr(query_jumble_ignore)

    /*
     * True if DEALLOCATE ALL.  This is redundant with "name == NULL", but we
     * make it a separate field so that exactly this condition (and not the
     * precise name) will be accounted for in query jumbling.
     */
    pub isall: bool,
    /* token location, or -1 if unknown */
    pub location: ParseLoc, // pg_node_attr(query_jumble_location)
}

/*
 *		DROP OWNED statement
 */
#[repr(C)]
pub struct DropOwnedStmt {
    pub r#type: NodeTag,
    pub roles: *mut List,
    pub behavior: DropBehavior,
}

/*
 *		REASSIGN OWNED statement
 */
#[repr(C)]
pub struct ReassignOwnedStmt {
    pub r#type: NodeTag,
    pub roles: *mut List,
    pub newrole: *mut RoleSpec,
}

/*
 * TS Dictionary stmts: DefineStmt, RenameStmt and DropStmt are default
 */
#[repr(C)]
pub struct AlterTSDictionaryStmt {
    pub r#type: NodeTag,
    pub dictname: *mut List, /* qualified name (list of String) */
    pub options: *mut List,  /* List of DefElem nodes */
}

/*
 * TS Configuration stmts: DefineStmt, RenameStmt and DropStmt are default
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AlterTSConfigType {
    ALTER_TSCONFIG_ADD_MAPPING,
    ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
    ALTER_TSCONFIG_REPLACE_DICT,
    ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
    ALTER_TSCONFIG_DROP_MAPPING,
}
pub use AlterTSConfigType::*;

#[repr(C)]
pub struct AlterTSConfigurationStmt {
    pub r#type: NodeTag,
    pub kind: AlterTSConfigType, /* ALTER_TSCONFIG_ADD_MAPPING, etc */
    pub cfgname: *mut List,     /* qualified name (list of String) */

    /*
     * dicts will be non-NIL if ADD/ALTER MAPPING was specified. If dicts is
     * NIL, but tokentype isn't, DROP MAPPING was specified.
     */
    pub tokentype: *mut List, /* list of String */
    pub dicts: *mut List,     /* list of list of String */
    pub r#override: bool,     /* if true - remove old variant */
    pub replace: bool,        /* if true - replace dictionary by another */
    pub missing_ok: bool,     /* for DROP - skip error if missing? */
}

#[repr(C)]
pub struct PublicationTable {
    pub r#type: NodeTag,
    pub relation: *mut RangeVar, /* relation to be published */
    pub whereClause: *mut Node, /* qualifications */
    pub columns: *mut List,     /* List of columns in a publication table */
}

/*
 * Publication object type
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PublicationObjSpecType {
    PUBLICATIONOBJ_TABLE,            /* A table */
    PUBLICATIONOBJ_TABLES_IN_SCHEMA, /* All tables in schema */
    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA, /* All tables in first element of
                                          * search_path */
    PUBLICATIONOBJ_CONTINUATION,     /* Continuation of previous type */
}
pub use PublicationObjSpecType::*;

#[repr(C)]
pub struct PublicationObjSpec {
    pub r#type: NodeTag,
    pub pubobjtype: PublicationObjSpecType, /* type of this publication object */
    pub name: *mut c_char,
    pub pubtable: *mut PublicationTable,
    pub location: ParseLoc, /* token location, or -1 if unknown */
}

#[repr(C)]
pub struct CreatePublicationStmt {
    pub r#type: NodeTag,
    pub pubname: *mut c_char,  /* Name of the publication */
    pub options: *mut List,    /* List of DefElem nodes */
    pub pubobjects: *mut List, /* Optional list of publication objects */
    pub for_all_tables: bool,  /* Special publication for all tables in db */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AlterPublicationAction {
    AP_AddObjects,  /* add objects to publication */
    AP_DropObjects, /* remove objects from publication */
    AP_SetObjects,  /* set list of objects */
}
pub use AlterPublicationAction::*;

#[repr(C)]
pub struct AlterPublicationStmt {
    pub r#type: NodeTag,
    pub pubname: *mut c_char, /* Name of the publication */

    /* parameters used for ALTER PUBLICATION ... WITH */
    pub options: *mut List, /* List of DefElem nodes */

    /*
     * Parameters used for ALTER PUBLICATION ... ADD/DROP/SET publication
     * objects.
     */
    pub pubobjects: *mut List, /* Optional list of publication objects */
    pub for_all_tables: bool,  /* Special publication for all tables in db */
    pub action: AlterPublicationAction, /* What action to perform with the given
                                         * objects */
}

#[repr(C)]
pub struct CreateSubscriptionStmt {
    pub r#type: NodeTag,
    pub subname: *mut c_char,    /* Name of the subscription */
    pub conninfo: *mut c_char,   /* Connection string to publisher */
    pub publication: *mut List,  /* One or more publication to subscribe to */
    pub options: *mut List,      /* List of DefElem nodes */
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AlterSubscriptionType {
    ALTER_SUBSCRIPTION_OPTIONS,
    ALTER_SUBSCRIPTION_CONNECTION,
    ALTER_SUBSCRIPTION_SET_PUBLICATION,
    ALTER_SUBSCRIPTION_ADD_PUBLICATION,
    ALTER_SUBSCRIPTION_DROP_PUBLICATION,
    ALTER_SUBSCRIPTION_REFRESH,
    ALTER_SUBSCRIPTION_ENABLED,
    ALTER_SUBSCRIPTION_SKIP,
}
pub use AlterSubscriptionType::*;

#[repr(C)]
pub struct AlterSubscriptionStmt {
    pub r#type: NodeTag,
    pub kind: AlterSubscriptionType, /* ALTER_SUBSCRIPTION_OPTIONS, etc */
    pub subname: *mut c_char,    /* Name of the subscription */
    pub conninfo: *mut c_char,   /* Connection string to publisher */
    pub publication: *mut List,  /* One or more publication to subscribe to */
    pub options: *mut List,      /* List of DefElem nodes */
}

#[repr(C)]
pub struct DropSubscriptionStmt {
    pub r#type: NodeTag,
    pub subname: *mut c_char,   /* Name of the subscription */
    pub missing_ok: bool,       /* Skip error if missing? */
    pub behavior: DropBehavior, /* RESTRICT or CASCADE behavior */
}
