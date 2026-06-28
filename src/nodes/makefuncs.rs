//! Translated from PostgreSQL src/include/nodes/makefuncs.h
//!
//! Constructors for the most frequently created node types. These are
//! non-type-centric free functions; the bodies live in the backend definition
//! module (`crate::backend::nodes::makefuncs`) as snake_case `pub fn`s, and this
//! header re-exports each under its C name so existing `use
//! crate::nodes::makefuncs::makeConst` call sites keep resolving.

pub use crate::backend::nodes::makefuncs::{
    make_a_expr as makeA_Expr, make_alias as makeAlias, make_and_qual, make_andclause,
    make_ands_explicit, make_ands_implicit, make_bool_const as makeBoolConst,
    make_bool_expr as makeBoolExpr, make_column_def as makeColumnDef, make_const as makeConst,
    make_def_elem as makeDefElem, make_def_elem_extended as makeDefElemExtended,
    make_from_expr as makeFromExpr, make_func_call as makeFuncCall,
    make_func_expr as makeFuncExpr, make_grouping_set as makeGroupingSet,
    make_index_info as makeIndexInfo, make_json_behavior as makeJsonBehavior,
    make_json_format as makeJsonFormat, make_json_is_predicate as makeJsonIsPredicate,
    make_json_key_value as makeJsonKeyValue, make_json_table_path as makeJsonTablePath,
    make_json_table_path_spec as makeJsonTablePathSpec, make_json_value_expr as makeJsonValueExpr,
    make_not_null_constraint as makeNotNullConstraint, make_notclause, make_null_const as makeNullConst,
    make_opclause, make_orclause, make_range_var as makeRangeVar,
    make_relabel_type as makeRelabelType, make_simple_a_expr as makeSimpleA_Expr,
    make_string_const as makeStringConst, make_target_entry as makeTargetEntry,
    make_type_name as makeTypeName, make_type_name_from_name_list as makeTypeNameFromNameList,
    make_type_name_from_oid as makeTypeNameFromOid, make_vacuum_relation as makeVacuumRelation,
    make_var as makeVar, make_var_from_target_entry as makeVarFromTargetEntry,
    make_whole_row_var as makeWholeRowVar, flat_copy_target_entry as flatCopyTargetEntry,
};
