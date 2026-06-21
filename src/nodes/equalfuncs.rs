//! Translation of postgres/src/backend/nodes/equalfuncs.c
//!
//! Equality functions to compare node trees.
//!
//! NOTE: it is intentional that parse location fields (in nodes that have
//! one) are not compared.  This is because we want, for example, a variable
//! "x" to be considered equal() to another reference to "x" in the query.
#![allow(non_snake_case)]
#![allow(unused_parens)]

use crate::miscadmin::check_stack_depth;
use crate::nodes::extensible::{ExtensibleNodeMethods, GetExtensibleNodeMethods, ExtensibleNode};
use crate::nodes::bitmapset::{bms_equal, Bitmapset};
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::nodes::pg_list::{lfirst, lfirst_int, lfirst_oid, lfirst_xid, List, ListCell};
use crate::nodes::primnodes::Const;
use crate::utils::adt::datum::datumIsEqual;
use crate::utils::elog::ERROR;
use crate::{elog, forboth};
use core::ffi::{c_int, c_void};
use crate::nodes::parsenodes::{ATAlterConstraint, A_ArrayExpr, A_Const, A_Expr, A_Indices, A_Indirection, A_Star, AccessPriv, AlterCollationStmt, AlterDatabaseRefreshCollStmt, AlterDatabaseSetStmt, AlterDatabaseStmt, AlterDefaultPrivilegesStmt, AlterDomainStmt, AlterEnumStmt, AlterEventTrigStmt, AlterExtensionContentsStmt, AlterExtensionStmt, AlterFdwStmt, AlterForeignServerStmt, AlterFunctionStmt, AlterObjectDependsStmt, AlterObjectSchemaStmt, AlterOpFamilyStmt, AlterOperatorStmt, AlterOwnerStmt, AlterPolicyStmt, AlterPublicationStmt, AlterRoleSetStmt, AlterRoleStmt, AlterSeqStmt, AlterStatsStmt, AlterSubscriptionStmt, AlterSystemStmt, AlterTSConfigurationStmt, AlterTSDictionaryStmt, AlterTableCmd, AlterTableMoveAllStmt, AlterTableSpaceOptionsStmt, AlterTableStmt, AlterTypeStmt, AlterUserMappingStmt, CTECycleClause, CTESearchClause, CallStmt, CheckPointStmt, ClosePortalStmt, ClusterStmt, CollateClause, ColumnDef, ColumnRef, CommentStmt, CommonTableExpr, CompositeTypeStmt, Constraint, ConstraintsSetStmt, CopyStmt, CreateAmStmt, CreateCastStmt, CreateConversionStmt, CreateDomainStmt, CreateEnumStmt, CreateEventTrigStmt, CreateExtensionStmt, CreateFdwStmt, CreateForeignServerStmt, CreateForeignTableStmt, CreateFunctionStmt, CreateOpClassItem, CreateOpClassStmt, CreateOpFamilyStmt, CreatePLangStmt, CreatePolicyStmt, CreatePublicationStmt, CreateRangeStmt, CreateRoleStmt, CreateSchemaStmt, CreateSeqStmt, CreateStatsStmt, CreateStmt, CreateSubscriptionStmt, CreateTableAsStmt, CreateTableSpaceStmt, CreateTransformStmt, CreateTrigStmt, CreateUserMappingStmt, CreatedbStmt, DeallocateStmt, DeclareCursorStmt, DefElem, DefineStmt, DeleteStmt, DiscardStmt, DoStmt, DropOwnedStmt, DropRoleStmt, DropStmt, DropSubscriptionStmt, DropTableSpaceStmt, DropUserMappingStmt, DropdbStmt, ExecuteStmt, ExplainStmt, FetchStmt, FuncCall, FunctionParameter, GrantRoleStmt, GrantStmt, GroupingSet, ImportForeignSchemaStmt, IndexElem, IndexStmt, InferClause, InsertStmt, JsonAggConstructor, JsonArgument, JsonArrayAgg, JsonArrayConstructor, JsonArrayQueryConstructor, JsonFuncExpr, JsonKeyValue, JsonObjectAgg, JsonObjectConstructor, JsonOutput, JsonParseExpr, JsonScalarExpr, JsonSerializeExpr, JsonTable, JsonTableColumn, JsonTablePathSpec, ListenStmt, LoadStmt, LockStmt, LockingClause, MergeStmt, MergeWhenClause, MultiAssignRef, NotifyStmt, ObjectWithArgs, OnConflictClause, PLAssignStmt, ParamRef, PartitionBoundSpec, PartitionCmd, PartitionElem, PartitionRangeDatum, PartitionSpec, PrepareStmt, PublicationObjSpec, PublicationTable, Query, RTEPermissionInfo, RangeFunction, RangeSubselect, RangeTableFunc, RangeTableFuncCol, RangeTableSample, RangeTblEntry, RangeTblFunction, RawStmt, ReassignOwnedStmt, RefreshMatViewStmt, ReindexStmt, RenameStmt, ReplicaIdentityStmt, ResTarget, ReturnStmt, ReturningClause, ReturningOption, RoleSpec, RowMarkClause, RuleStmt, SecLabelStmt, SelectStmt, SetOperationStmt, SortBy, SortGroupClause, StatsElem, TableLikeClause, TableSampleClause, TransactionStmt, TriggerTransition, TruncateStmt, TypeCast, TypeName, UnlistenStmt, UpdateStmt, VacuumRelation, VacuumStmt, VariableSetStmt, VariableShowStmt, ViewStmt, WindowClause, WindowDef, WithCheckOption, WithClause, XmlSerialize};
use crate::nodes::primnodes::{Aggref, Alias, AlternativeSubPlan, ArrayCoerceExpr, ArrayExpr, BoolExpr, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen, CoalesceExpr, CoerceToDomain, CoerceToDomainValue, CoerceViaIO, CollateExpr, ConvertRowtypeExpr, CurrentOfExpr, DistinctExpr, FieldSelect, FieldStore, FromExpr, FuncExpr, GroupingFunc, InferenceElem, IntoClause, JoinExpr, JsonBehavior, JsonConstructorExpr, JsonExpr, JsonFormat, JsonIsPredicate, JsonReturning, JsonTablePath, JsonTablePathScan, JsonTableSiblingJoin, JsonValueExpr, MergeAction, MergeSupportFunc, MinMaxExpr, NamedArgExpr, NextValueExpr, NullIfExpr, NullTest, OnConflictExpr, OpExpr, Param, RangeTblRef, RangeVar, RelabelType, ReturningExpr, RowCompareExpr, RowExpr, SQLValueFunction, ScalarArrayOpExpr, SetToDefault, SubLink, SubPlan, SubscriptingRef, TableFunc, TargetEntry, Var, WindowFunc, WindowFuncRunCondition, XmlExpr};
use crate::nodes::pathnodes::{AppendRelInfo, GroupByOrdering, PathKey, PlaceHolderInfo, PlaceHolderVar, RestrictInfo, SpecialJoinInfo};
use crate::nodes::value::{BitString, Boolean, Float, Integer, String};

/* equalstr: Compare string fields that might be NULL */
#[inline]
unsafe fn equalstr(a: *const i8, b: *const i8) -> bool {
    if !a.is_null() && !b.is_null() {
        libc::strcmp(a, b) == 0
    } else {
        a == b
    }
}

/*
 * Support functions for nodes with custom_copy_equal attribute
 */

unsafe fn _equalConst(a: *const Const, b: *const Const) -> bool {
    // COMPARE_SCALAR_FIELD(consttype);
    if (*a).consttype != (*b).consttype {
        return false;
    }
    // COMPARE_SCALAR_FIELD(consttypmod);
    if (*a).consttypmod != (*b).consttypmod {
        return false;
    }
    // COMPARE_SCALAR_FIELD(constcollid);
    if (*a).constcollid != (*b).constcollid {
        return false;
    }
    // COMPARE_SCALAR_FIELD(constlen);
    if (*a).constlen != (*b).constlen {
        return false;
    }
    // COMPARE_SCALAR_FIELD(constisnull);
    if (*a).constisnull != (*b).constisnull {
        return false;
    }
    // COMPARE_SCALAR_FIELD(constbyval);
    if (*a).constbyval != (*b).constbyval {
        return false;
    }
    // COMPARE_LOCATION_FIELD(location);  -- no-op

    /*
     * We treat all NULL constants of the same type as equal. Someday this
     * might need to change?  But datumIsEqual doesn't work on nulls, so...
     */
    if (*a).constisnull {
        return true;
    }
    datumIsEqual(
        (*a).constvalue,
        (*b).constvalue,
        (*a).constbyval,
        (*a).constlen,
    )
}

unsafe fn _equalExtensibleNode(a: *const ExtensibleNode, b: *const ExtensibleNode) -> bool {
    let methods: *const ExtensibleNodeMethods;

    // COMPARE_STRING_FIELD(extnodename);
    if !equalstr((*a).extnodename, (*b).extnodename) {
        return false;
    }

    /* At this point, we know extnodename is the same for both nodes. */
    methods = GetExtensibleNodeMethods((*a).extnodename, false);

    /* compare the private fields */
    if !((*methods).nodeEqual.unwrap())(a, b) {
        return false;
    }

    true
}

unsafe fn _equalA_Const(a: *const A_Const, b: *const A_Const) -> bool {
    // COMPARE_SCALAR_FIELD(isnull);
    if (*a).isnull != (*b).isnull {
        return false;
    }
    /* Hack for in-line val field.  Also val is not valid if isnull is true */
    if !(*a).isnull
        && !equal(
            &(*a).val as *const _ as *const c_void,
            &(*b).val as *const _ as *const c_void,
        )
    {
        return false;
    }
    // COMPARE_LOCATION_FIELD(location);  -- no-op

    true
}

unsafe fn _equalBitmapset(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    bms_equal(a, b)
}

/*
 * Lists are handled specially
 */
unsafe fn _equalList(a: *const List, b: *const List) -> bool {
    /*
     * Try to reject by simple scalar checks before grovelling through all the
     * list elements...
     */
    // COMPARE_SCALAR_FIELD(type);
    if (*a).r#type != (*b).r#type {
        return false;
    }
    // COMPARE_SCALAR_FIELD(length);
    if (*a).length != (*b).length {
        return false;
    }

    /*
     * We place the switch outside the loop for the sake of efficiency; this
     * may not be worth doing...
     */
    match (*a).r#type {
        NodeTag::T_List => {
            forboth!(item_a, a, item_b, b, {
                if !equal(lfirst(item_a), lfirst(item_b)) {
                    return false;
                }
            });
        }
        NodeTag::T_IntList => {
            forboth!(item_a, a, item_b, b, {
                if lfirst_int(item_a) != lfirst_int(item_b) {
                    return false;
                }
            });
        }
        NodeTag::T_OidList => {
            forboth!(item_a, a, item_b, b, {
                if lfirst_oid(item_a) != lfirst_oid(item_b) {
                    return false;
                }
            });
        }
        NodeTag::T_XidList => {
            forboth!(item_a, a, item_b, b, {
                if lfirst_xid(item_a) != lfirst_xid(item_b) {
                    return false;
                }
            });
        }
        _ => {
            elog!(ERROR, "unrecognized list node type: {}", (*a).r#type as c_int);
            return false; /* keep compiler quiet */
        }
    }

    /*
     * If we got here, we should have run out of elements of both lists
     */
    // Assert(item_a == NULL);
    // Assert(item_b == NULL);

    true
}

unsafe fn _equalAlias(a: *const Alias, b: *const Alias) -> bool {
    if !equalstr((*a).aliasname, (*b).aliasname) { return false; }
    if !equal((*a).colnames as *const c_void, (*b).colnames as *const c_void) { return false; }
    true
}

unsafe fn _equalRangeVar(a: *const RangeVar, b: *const RangeVar) -> bool {
    if !equalstr((*a).catalogname, (*b).catalogname) { return false; }
    if !equalstr((*a).schemaname, (*b).schemaname) { return false; }
    if !equalstr((*a).relname, (*b).relname) { return false; }
    if (*a).inh != (*b).inh { return false; }
    if (*a).relpersistence != (*b).relpersistence { return false; }
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    true
}

unsafe fn _equalTableFunc(a: *const TableFunc, b: *const TableFunc) -> bool {
    if (*a).functype != (*b).functype { return false; }
    if !equal((*a).ns_uris as *const c_void, (*b).ns_uris as *const c_void) { return false; }
    if !equal((*a).ns_names as *const c_void, (*b).ns_names as *const c_void) { return false; }
    if !equal((*a).docexpr as *const c_void, (*b).docexpr as *const c_void) { return false; }
    if !equal((*a).rowexpr as *const c_void, (*b).rowexpr as *const c_void) { return false; }
    if !equal((*a).colnames as *const c_void, (*b).colnames as *const c_void) { return false; }
    if !equal((*a).coltypes as *const c_void, (*b).coltypes as *const c_void) { return false; }
    if !equal((*a).coltypmods as *const c_void, (*b).coltypmods as *const c_void) { return false; }
    if !equal((*a).colcollations as *const c_void, (*b).colcollations as *const c_void) { return false; }
    if !equal((*a).colexprs as *const c_void, (*b).colexprs as *const c_void) { return false; }
    if !equal((*a).coldefexprs as *const c_void, (*b).coldefexprs as *const c_void) { return false; }
    if !equal((*a).colvalexprs as *const c_void, (*b).colvalexprs as *const c_void) { return false; }
    if !equal((*a).passingvalexprs as *const c_void, (*b).passingvalexprs as *const c_void) { return false; }
    if !bms_equal((*a).notnulls, (*b).notnulls) { return false; }
    if !equal((*a).plan as *const c_void, (*b).plan as *const c_void) { return false; }
    if (*a).ordinalitycol != (*b).ordinalitycol { return false; }
    true
}

unsafe fn _equalIntoClause(a: *const IntoClause, b: *const IntoClause) -> bool {
    if !equal((*a).rel as *const c_void, (*b).rel as *const c_void) { return false; }
    if !equal((*a).colNames as *const c_void, (*b).colNames as *const c_void) { return false; }
    if !equalstr((*a).accessMethod, (*b).accessMethod) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).onCommit != (*b).onCommit { return false; }
    if !equalstr((*a).tableSpaceName, (*b).tableSpaceName) { return false; }
    if !equal((*a).viewQuery as *const c_void, (*b).viewQuery as *const c_void) { return false; }
    if (*a).skipData != (*b).skipData { return false; }
    true
}

unsafe fn _equalVar(a: *const Var, b: *const Var) -> bool {
    if (*a).varno != (*b).varno { return false; }
    if (*a).varattno != (*b).varattno { return false; }
    if (*a).vartype != (*b).vartype { return false; }
    if (*a).vartypmod != (*b).vartypmod { return false; }
    if (*a).varcollid != (*b).varcollid { return false; }
    if !bms_equal((*a).varnullingrels, (*b).varnullingrels) { return false; }
    if (*a).varlevelsup != (*b).varlevelsup { return false; }
    if (*a).varreturningtype != (*b).varreturningtype { return false; }
    true
}

unsafe fn _equalParam(a: *const Param, b: *const Param) -> bool {
    if (*a).paramkind != (*b).paramkind { return false; }
    if (*a).paramid != (*b).paramid { return false; }
    if (*a).paramtype != (*b).paramtype { return false; }
    if (*a).paramtypmod != (*b).paramtypmod { return false; }
    if (*a).paramcollid != (*b).paramcollid { return false; }
    true
}

unsafe fn _equalAggref(a: *const Aggref, b: *const Aggref) -> bool {
    if (*a).aggfnoid != (*b).aggfnoid { return false; }
    if (*a).aggtype != (*b).aggtype { return false; }
    if (*a).aggcollid != (*b).aggcollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).aggargtypes as *const c_void, (*b).aggargtypes as *const c_void) { return false; }
    if !equal((*a).aggdirectargs as *const c_void, (*b).aggdirectargs as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).aggorder as *const c_void, (*b).aggorder as *const c_void) { return false; }
    if !equal((*a).aggdistinct as *const c_void, (*b).aggdistinct as *const c_void) { return false; }
    if !equal((*a).aggfilter as *const c_void, (*b).aggfilter as *const c_void) { return false; }
    if (*a).aggstar != (*b).aggstar { return false; }
    if (*a).aggvariadic != (*b).aggvariadic { return false; }
    if (*a).aggkind != (*b).aggkind { return false; }
    if (*a).agglevelsup != (*b).agglevelsup { return false; }
    if (*a).aggsplit != (*b).aggsplit { return false; }
    if (*a).aggno != (*b).aggno { return false; }
    if (*a).aggtransno != (*b).aggtransno { return false; }
    true
}

unsafe fn _equalGroupingFunc(a: *const GroupingFunc, b: *const GroupingFunc) -> bool {
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if (*a).agglevelsup != (*b).agglevelsup { return false; }
    true
}

unsafe fn _equalWindowFunc(a: *const WindowFunc, b: *const WindowFunc) -> bool {
    if (*a).winfnoid != (*b).winfnoid { return false; }
    if (*a).wintype != (*b).wintype { return false; }
    if (*a).wincollid != (*b).wincollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).aggfilter as *const c_void, (*b).aggfilter as *const c_void) { return false; }
    if !equal((*a).runCondition as *const c_void, (*b).runCondition as *const c_void) { return false; }
    if (*a).winref != (*b).winref { return false; }
    if (*a).winstar != (*b).winstar { return false; }
    if (*a).winagg != (*b).winagg { return false; }
    true
}

unsafe fn _equalWindowFuncRunCondition(a: *const WindowFuncRunCondition, b: *const WindowFuncRunCondition) -> bool {
    if (*a).opno != (*b).opno { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if (*a).wfunc_left != (*b).wfunc_left { return false; }
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    true
}

unsafe fn _equalMergeSupportFunc(a: *const MergeSupportFunc, b: *const MergeSupportFunc) -> bool {
    if (*a).msftype != (*b).msftype { return false; }
    if (*a).msfcollid != (*b).msfcollid { return false; }
    true
}

unsafe fn _equalSubscriptingRef(a: *const SubscriptingRef, b: *const SubscriptingRef) -> bool {
    if (*a).refcontainertype != (*b).refcontainertype { return false; }
    if (*a).refelemtype != (*b).refelemtype { return false; }
    if (*a).refrestype != (*b).refrestype { return false; }
    if (*a).reftypmod != (*b).reftypmod { return false; }
    if (*a).refcollid != (*b).refcollid { return false; }
    if !equal((*a).refupperindexpr as *const c_void, (*b).refupperindexpr as *const c_void) { return false; }
    if !equal((*a).reflowerindexpr as *const c_void, (*b).reflowerindexpr as *const c_void) { return false; }
    if !equal((*a).refexpr as *const c_void, (*b).refexpr as *const c_void) { return false; }
    if !equal((*a).refassgnexpr as *const c_void, (*b).refassgnexpr as *const c_void) { return false; }
    true
}

unsafe fn _equalFuncExpr(a: *const FuncExpr, b: *const FuncExpr) -> bool {
    if (*a).funcid != (*b).funcid { return false; }
    if (*a).funcresulttype != (*b).funcresulttype { return false; }
    if (*a).funcretset != (*b).funcretset { return false; }
    if (*a).funcvariadic != (*b).funcvariadic { return false; }
    if (*a).funccollid != (*b).funccollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalNamedArgExpr(a: *const NamedArgExpr, b: *const NamedArgExpr) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if (*a).argnumber != (*b).argnumber { return false; }
    true
}

unsafe fn _equalOpExpr(a: *const OpExpr, b: *const OpExpr) -> bool {
    if (*a).opno != (*b).opno { return false; }
    if (*a).opfuncid != (*b).opfuncid && (*a).opfuncid != 0 && (*b).opfuncid != 0 {
        return false;
    }
    if (*a).opresulttype != (*b).opresulttype { return false; }
    if (*a).opretset != (*b).opretset { return false; }
    if (*a).opcollid != (*b).opcollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalDistinctExpr(a: *const DistinctExpr, b: *const DistinctExpr) -> bool {
    if (*a).opno != (*b).opno { return false; }
    if (*a).opfuncid != (*b).opfuncid && (*a).opfuncid != 0 && (*b).opfuncid != 0 {
        return false;
    }
    if (*a).opresulttype != (*b).opresulttype { return false; }
    if (*a).opretset != (*b).opretset { return false; }
    if (*a).opcollid != (*b).opcollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalNullIfExpr(a: *const NullIfExpr, b: *const NullIfExpr) -> bool {
    if (*a).opno != (*b).opno { return false; }
    if (*a).opfuncid != (*b).opfuncid && (*a).opfuncid != 0 && (*b).opfuncid != 0 {
        return false;
    }
    if (*a).opresulttype != (*b).opresulttype { return false; }
    if (*a).opretset != (*b).opretset { return false; }
    if (*a).opcollid != (*b).opcollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalScalarArrayOpExpr(a: *const ScalarArrayOpExpr, b: *const ScalarArrayOpExpr) -> bool {
    if (*a).opno != (*b).opno { return false; }
    if (*a).opfuncid != (*b).opfuncid && (*a).opfuncid != 0 && (*b).opfuncid != 0 {
        return false;
    }
    if (*a).hashfuncid != (*b).hashfuncid && (*a).hashfuncid != 0 && (*b).hashfuncid != 0 {
        return false;
    }
    if (*a).negfuncid != (*b).negfuncid && (*a).negfuncid != 0 && (*b).negfuncid != 0 {
        return false;
    }
    if (*a).useOr != (*b).useOr { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalBoolExpr(a: *const BoolExpr, b: *const BoolExpr) -> bool {
    if (*a).boolop != (*b).boolop { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalSubLink(a: *const SubLink, b: *const SubLink) -> bool {
    if (*a).subLinkType != (*b).subLinkType { return false; }
    if (*a).subLinkId != (*b).subLinkId { return false; }
    if !equal((*a).testexpr as *const c_void, (*b).testexpr as *const c_void) { return false; }
    if !equal((*a).operName as *const c_void, (*b).operName as *const c_void) { return false; }
    if !equal((*a).subselect as *const c_void, (*b).subselect as *const c_void) { return false; }
    true
}

unsafe fn _equalSubPlan(a: *const SubPlan, b: *const SubPlan) -> bool {
    if (*a).subLinkType != (*b).subLinkType { return false; }
    if !equal((*a).testexpr as *const c_void, (*b).testexpr as *const c_void) { return false; }
    if !equal((*a).paramIds as *const c_void, (*b).paramIds as *const c_void) { return false; }
    if (*a).plan_id != (*b).plan_id { return false; }
    if !equalstr((*a).plan_name, (*b).plan_name) { return false; }
    if (*a).firstColType != (*b).firstColType { return false; }
    if (*a).firstColTypmod != (*b).firstColTypmod { return false; }
    if (*a).firstColCollation != (*b).firstColCollation { return false; }
    if (*a).useHashTable != (*b).useHashTable { return false; }
    if (*a).unknownEqFalse != (*b).unknownEqFalse { return false; }
    if (*a).parallel_safe != (*b).parallel_safe { return false; }
    if !equal((*a).setParam as *const c_void, (*b).setParam as *const c_void) { return false; }
    if !equal((*a).parParam as *const c_void, (*b).parParam as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if (*a).startup_cost != (*b).startup_cost { return false; }
    if (*a).per_call_cost != (*b).per_call_cost { return false; }
    true
}

unsafe fn _equalAlternativeSubPlan(a: *const AlternativeSubPlan, b: *const AlternativeSubPlan) -> bool {
    if !equal((*a).subplans as *const c_void, (*b).subplans as *const c_void) { return false; }
    true
}

unsafe fn _equalFieldSelect(a: *const FieldSelect, b: *const FieldSelect) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).fieldnum != (*b).fieldnum { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    if (*a).resulttypmod != (*b).resulttypmod { return false; }
    if (*a).resultcollid != (*b).resultcollid { return false; }
    true
}

unsafe fn _equalFieldStore(a: *const FieldStore, b: *const FieldStore) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equal((*a).newvals as *const c_void, (*b).newvals as *const c_void) { return false; }
    if !equal((*a).fieldnums as *const c_void, (*b).fieldnums as *const c_void) { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    true
}

unsafe fn _equalRelabelType(a: *const RelabelType, b: *const RelabelType) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    if (*a).resulttypmod != (*b).resulttypmod { return false; }
    if (*a).resultcollid != (*b).resultcollid { return false; }
    true
}

unsafe fn _equalCoerceViaIO(a: *const CoerceViaIO, b: *const CoerceViaIO) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    if (*a).resultcollid != (*b).resultcollid { return false; }
    true
}

unsafe fn _equalArrayCoerceExpr(a: *const ArrayCoerceExpr, b: *const ArrayCoerceExpr) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equal((*a).elemexpr as *const c_void, (*b).elemexpr as *const c_void) { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    if (*a).resulttypmod != (*b).resulttypmod { return false; }
    if (*a).resultcollid != (*b).resultcollid { return false; }
    true
}

unsafe fn _equalConvertRowtypeExpr(a: *const ConvertRowtypeExpr, b: *const ConvertRowtypeExpr) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    true
}

unsafe fn _equalCollateExpr(a: *const CollateExpr, b: *const CollateExpr) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).collOid != (*b).collOid { return false; }
    true
}

unsafe fn _equalCaseExpr(a: *const CaseExpr, b: *const CaseExpr) -> bool {
    if (*a).casetype != (*b).casetype { return false; }
    if (*a).casecollid != (*b).casecollid { return false; }
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).defresult as *const c_void, (*b).defresult as *const c_void) { return false; }
    true
}

unsafe fn _equalCaseWhen(a: *const CaseWhen, b: *const CaseWhen) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).result as *const c_void, (*b).result as *const c_void) { return false; }
    true
}

unsafe fn _equalCaseTestExpr(a: *const CaseTestExpr, b: *const CaseTestExpr) -> bool {
    if (*a).typeId != (*b).typeId { return false; }
    if (*a).typeMod != (*b).typeMod { return false; }
    if (*a).collation != (*b).collation { return false; }
    true
}

unsafe fn _equalArrayExpr(a: *const ArrayExpr, b: *const ArrayExpr) -> bool {
    if (*a).array_typeid != (*b).array_typeid { return false; }
    if (*a).array_collid != (*b).array_collid { return false; }
    if (*a).element_typeid != (*b).element_typeid { return false; }
    if !equal((*a).elements as *const c_void, (*b).elements as *const c_void) { return false; }
    if (*a).multidims != (*b).multidims { return false; }
    true
}

unsafe fn _equalRowExpr(a: *const RowExpr, b: *const RowExpr) -> bool {
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if (*a).row_typeid != (*b).row_typeid { return false; }
    if !equal((*a).colnames as *const c_void, (*b).colnames as *const c_void) { return false; }
    true
}

unsafe fn _equalRowCompareExpr(a: *const RowCompareExpr, b: *const RowCompareExpr) -> bool {
    if (*a).cmptype != (*b).cmptype { return false; }
    if !equal((*a).opnos as *const c_void, (*b).opnos as *const c_void) { return false; }
    if !equal((*a).opfamilies as *const c_void, (*b).opfamilies as *const c_void) { return false; }
    if !equal((*a).inputcollids as *const c_void, (*b).inputcollids as *const c_void) { return false; }
    if !equal((*a).largs as *const c_void, (*b).largs as *const c_void) { return false; }
    if !equal((*a).rargs as *const c_void, (*b).rargs as *const c_void) { return false; }
    true
}

unsafe fn _equalCoalesceExpr(a: *const CoalesceExpr, b: *const CoalesceExpr) -> bool {
    if (*a).coalescetype != (*b).coalescetype { return false; }
    if (*a).coalescecollid != (*b).coalescecollid { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalMinMaxExpr(a: *const MinMaxExpr, b: *const MinMaxExpr) -> bool {
    if (*a).minmaxtype != (*b).minmaxtype { return false; }
    if (*a).minmaxcollid != (*b).minmaxcollid { return false; }
    if (*a).inputcollid != (*b).inputcollid { return false; }
    if (*a).op != (*b).op { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalSQLValueFunction(a: *const SQLValueFunction, b: *const SQLValueFunction) -> bool {
    if (*a).op != (*b).op { return false; }
    if (*a).r#type != (*b).r#type { return false; }
    if (*a).typmod != (*b).typmod { return false; }
    true
}

unsafe fn _equalXmlExpr(a: *const XmlExpr, b: *const XmlExpr) -> bool {
    if (*a).op != (*b).op { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).named_args as *const c_void, (*b).named_args as *const c_void) { return false; }
    if !equal((*a).arg_names as *const c_void, (*b).arg_names as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if (*a).xmloption != (*b).xmloption { return false; }
    if (*a).indent != (*b).indent { return false; }
    if (*a).r#type != (*b).r#type { return false; }
    if (*a).typmod != (*b).typmod { return false; }
    true
}

unsafe fn _equalJsonFormat(a: *const JsonFormat, b: *const JsonFormat) -> bool {
    if (*a).format_type != (*b).format_type { return false; }
    if (*a).encoding != (*b).encoding { return false; }
    true
}

unsafe fn _equalJsonReturning(a: *const JsonReturning, b: *const JsonReturning) -> bool {
    if !equal((*a).format as *const c_void, (*b).format as *const c_void) { return false; }
    if (*a).typid != (*b).typid { return false; }
    if (*a).typmod != (*b).typmod { return false; }
    true
}

unsafe fn _equalJsonValueExpr(a: *const JsonValueExpr, b: *const JsonValueExpr) -> bool {
    if !equal((*a).raw_expr as *const c_void, (*b).raw_expr as *const c_void) { return false; }
    if !equal((*a).formatted_expr as *const c_void, (*b).formatted_expr as *const c_void) { return false; }
    if !equal((*a).format as *const c_void, (*b).format as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonConstructorExpr(a: *const JsonConstructorExpr, b: *const JsonConstructorExpr) -> bool {
    if (*a).r#type != (*b).r#type { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).func as *const c_void, (*b).func as *const c_void) { return false; }
    if !equal((*a).coercion as *const c_void, (*b).coercion as *const c_void) { return false; }
    if !equal((*a).returning as *const c_void, (*b).returning as *const c_void) { return false; }
    if (*a).absent_on_null != (*b).absent_on_null { return false; }
    if (*a).unique != (*b).unique { return false; }
    true
}

unsafe fn _equalJsonIsPredicate(a: *const JsonIsPredicate, b: *const JsonIsPredicate) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).format as *const c_void, (*b).format as *const c_void) { return false; }
    if (*a).item_type != (*b).item_type { return false; }
    if (*a).unique_keys != (*b).unique_keys { return false; }
    true
}

unsafe fn _equalJsonBehavior(a: *const JsonBehavior, b: *const JsonBehavior) -> bool {
    if (*a).btype != (*b).btype { return false; }
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if (*a).coerce != (*b).coerce { return false; }
    true
}

unsafe fn _equalJsonExpr(a: *const JsonExpr, b: *const JsonExpr) -> bool {
    if (*a).op != (*b).op { return false; }
    if !equalstr((*a).column_name, (*b).column_name) { return false; }
    if !equal((*a).formatted_expr as *const c_void, (*b).formatted_expr as *const c_void) { return false; }
    if !equal((*a).format as *const c_void, (*b).format as *const c_void) { return false; }
    if !equal((*a).path_spec as *const c_void, (*b).path_spec as *const c_void) { return false; }
    if !equal((*a).returning as *const c_void, (*b).returning as *const c_void) { return false; }
    if !equal((*a).passing_names as *const c_void, (*b).passing_names as *const c_void) { return false; }
    if !equal((*a).passing_values as *const c_void, (*b).passing_values as *const c_void) { return false; }
    if !equal((*a).on_empty as *const c_void, (*b).on_empty as *const c_void) { return false; }
    if !equal((*a).on_error as *const c_void, (*b).on_error as *const c_void) { return false; }
    if (*a).use_io_coercion != (*b).use_io_coercion { return false; }
    if (*a).use_json_coercion != (*b).use_json_coercion { return false; }
    if (*a).wrapper != (*b).wrapper { return false; }
    if (*a).omit_quotes != (*b).omit_quotes { return false; }
    if (*a).collation != (*b).collation { return false; }
    true
}

unsafe fn _equalJsonTablePath(a: *const JsonTablePath, b: *const JsonTablePath) -> bool {
    if !equal((*a).value as *const c_void, (*b).value as *const c_void) { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    true
}

unsafe fn _equalJsonTablePathScan(a: *const JsonTablePathScan, b: *const JsonTablePathScan) -> bool {
    if !equal((*a).path as *const c_void, (*b).path as *const c_void) { return false; }
    if (*a).errorOnError != (*b).errorOnError { return false; }
    if !equal((*a).child as *const c_void, (*b).child as *const c_void) { return false; }
    if (*a).colMin != (*b).colMin { return false; }
    if (*a).colMax != (*b).colMax { return false; }
    true
}

unsafe fn _equalJsonTableSiblingJoin(a: *const JsonTableSiblingJoin, b: *const JsonTableSiblingJoin) -> bool {
    if !equal((*a).lplan as *const c_void, (*b).lplan as *const c_void) { return false; }
    if !equal((*a).rplan as *const c_void, (*b).rplan as *const c_void) { return false; }
    true
}

unsafe fn _equalNullTest(a: *const NullTest, b: *const NullTest) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).nulltesttype != (*b).nulltesttype { return false; }
    if (*a).argisrow != (*b).argisrow { return false; }
    true
}

unsafe fn _equalBooleanTest(a: *const BooleanTest, b: *const BooleanTest) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).booltesttype != (*b).booltesttype { return false; }
    true
}

unsafe fn _equalMergeAction(a: *const MergeAction, b: *const MergeAction) -> bool {
    if (*a).matchKind != (*b).matchKind { return false; }
    if (*a).commandType != (*b).commandType { return false; }
    if (*a).r#override != (*b).r#override { return false; }
    if !equal((*a).qual as *const c_void, (*b).qual as *const c_void) { return false; }
    if !equal((*a).targetList as *const c_void, (*b).targetList as *const c_void) { return false; }
    if !equal((*a).updateColnos as *const c_void, (*b).updateColnos as *const c_void) { return false; }
    true
}

unsafe fn _equalCoerceToDomain(a: *const CoerceToDomain, b: *const CoerceToDomain) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).resulttype != (*b).resulttype { return false; }
    if (*a).resulttypmod != (*b).resulttypmod { return false; }
    if (*a).resultcollid != (*b).resultcollid { return false; }
    true
}

unsafe fn _equalCoerceToDomainValue(a: *const CoerceToDomainValue, b: *const CoerceToDomainValue) -> bool {
    if (*a).typeId != (*b).typeId { return false; }
    if (*a).typeMod != (*b).typeMod { return false; }
    if (*a).collation != (*b).collation { return false; }
    true
}

unsafe fn _equalSetToDefault(a: *const SetToDefault, b: *const SetToDefault) -> bool {
    if (*a).typeId != (*b).typeId { return false; }
    if (*a).typeMod != (*b).typeMod { return false; }
    if (*a).collation != (*b).collation { return false; }
    true
}

unsafe fn _equalCurrentOfExpr(a: *const CurrentOfExpr, b: *const CurrentOfExpr) -> bool {
    if (*a).cvarno != (*b).cvarno { return false; }
    if !equalstr((*a).cursor_name, (*b).cursor_name) { return false; }
    if (*a).cursor_param != (*b).cursor_param { return false; }
    true
}

unsafe fn _equalNextValueExpr(a: *const NextValueExpr, b: *const NextValueExpr) -> bool {
    if (*a).seqid != (*b).seqid { return false; }
    if (*a).typeId != (*b).typeId { return false; }
    true
}

unsafe fn _equalInferenceElem(a: *const InferenceElem, b: *const InferenceElem) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if (*a).infercollid != (*b).infercollid { return false; }
    if (*a).inferopclass != (*b).inferopclass { return false; }
    true
}

unsafe fn _equalReturningExpr(a: *const ReturningExpr, b: *const ReturningExpr) -> bool {
    if (*a).retlevelsup != (*b).retlevelsup { return false; }
    if (*a).retold != (*b).retold { return false; }
    if !equal((*a).retexpr as *const c_void, (*b).retexpr as *const c_void) { return false; }
    true
}

unsafe fn _equalTargetEntry(a: *const TargetEntry, b: *const TargetEntry) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if (*a).resno != (*b).resno { return false; }
    if !equalstr((*a).resname, (*b).resname) { return false; }
    if (*a).ressortgroupref != (*b).ressortgroupref { return false; }
    if (*a).resorigtbl != (*b).resorigtbl { return false; }
    if (*a).resorigcol != (*b).resorigcol { return false; }
    if (*a).resjunk != (*b).resjunk { return false; }
    true
}

unsafe fn _equalRangeTblRef(a: *const RangeTblRef, b: *const RangeTblRef) -> bool {
    if (*a).rtindex != (*b).rtindex { return false; }
    true
}

unsafe fn _equalJoinExpr(a: *const JoinExpr, b: *const JoinExpr) -> bool {
    if (*a).jointype != (*b).jointype { return false; }
    if (*a).isNatural != (*b).isNatural { return false; }
    if !equal((*a).larg as *const c_void, (*b).larg as *const c_void) { return false; }
    if !equal((*a).rarg as *const c_void, (*b).rarg as *const c_void) { return false; }
    if !equal((*a).usingClause as *const c_void, (*b).usingClause as *const c_void) { return false; }
    if !equal((*a).join_using_alias as *const c_void, (*b).join_using_alias as *const c_void) { return false; }
    if !equal((*a).quals as *const c_void, (*b).quals as *const c_void) { return false; }
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    if (*a).rtindex != (*b).rtindex { return false; }
    true
}

unsafe fn _equalFromExpr(a: *const FromExpr, b: *const FromExpr) -> bool {
    if !equal((*a).fromlist as *const c_void, (*b).fromlist as *const c_void) { return false; }
    if !equal((*a).quals as *const c_void, (*b).quals as *const c_void) { return false; }
    true
}

unsafe fn _equalOnConflictExpr(a: *const OnConflictExpr, b: *const OnConflictExpr) -> bool {
    if (*a).action != (*b).action { return false; }
    if !equal((*a).arbiterElems as *const c_void, (*b).arbiterElems as *const c_void) { return false; }
    if !equal((*a).arbiterWhere as *const c_void, (*b).arbiterWhere as *const c_void) { return false; }
    if (*a).constraint != (*b).constraint { return false; }
    if !equal((*a).onConflictSet as *const c_void, (*b).onConflictSet as *const c_void) { return false; }
    if !equal((*a).onConflictWhere as *const c_void, (*b).onConflictWhere as *const c_void) { return false; }
    if (*a).exclRelIndex != (*b).exclRelIndex { return false; }
    if !equal((*a).exclRelTlist as *const c_void, (*b).exclRelTlist as *const c_void) { return false; }
    true
}

unsafe fn _equalQuery(a: *const Query, b: *const Query) -> bool {
    if (*a).commandType != (*b).commandType { return false; }
    if (*a).querySource != (*b).querySource { return false; }
    if (*a).canSetTag != (*b).canSetTag { return false; }
    if !equal((*a).utilityStmt as *const c_void, (*b).utilityStmt as *const c_void) { return false; }
    if (*a).resultRelation != (*b).resultRelation { return false; }
    if (*a).hasAggs != (*b).hasAggs { return false; }
    if (*a).hasWindowFuncs != (*b).hasWindowFuncs { return false; }
    if (*a).hasTargetSRFs != (*b).hasTargetSRFs { return false; }
    if (*a).hasSubLinks != (*b).hasSubLinks { return false; }
    if (*a).hasDistinctOn != (*b).hasDistinctOn { return false; }
    if (*a).hasRecursive != (*b).hasRecursive { return false; }
    if (*a).hasModifyingCTE != (*b).hasModifyingCTE { return false; }
    if (*a).hasForUpdate != (*b).hasForUpdate { return false; }
    if (*a).hasRowSecurity != (*b).hasRowSecurity { return false; }
    if (*a).hasGroupRTE != (*b).hasGroupRTE { return false; }
    if (*a).isReturn != (*b).isReturn { return false; }
    if !equal((*a).cteList as *const c_void, (*b).cteList as *const c_void) { return false; }
    if !equal((*a).rtable as *const c_void, (*b).rtable as *const c_void) { return false; }
    if !equal((*a).rteperminfos as *const c_void, (*b).rteperminfos as *const c_void) { return false; }
    if !equal((*a).jointree as *const c_void, (*b).jointree as *const c_void) { return false; }
    if !equal((*a).mergeActionList as *const c_void, (*b).mergeActionList as *const c_void) { return false; }
    if (*a).mergeTargetRelation != (*b).mergeTargetRelation { return false; }
    if !equal((*a).mergeJoinCondition as *const c_void, (*b).mergeJoinCondition as *const c_void) { return false; }
    if !equal((*a).targetList as *const c_void, (*b).targetList as *const c_void) { return false; }
    if (*a).r#override != (*b).r#override { return false; }
    if !equal((*a).onConflict as *const c_void, (*b).onConflict as *const c_void) { return false; }
    if !equalstr((*a).returningOldAlias, (*b).returningOldAlias) { return false; }
    if !equalstr((*a).returningNewAlias, (*b).returningNewAlias) { return false; }
    if !equal((*a).returningList as *const c_void, (*b).returningList as *const c_void) { return false; }
    if !equal((*a).groupClause as *const c_void, (*b).groupClause as *const c_void) { return false; }
    if (*a).groupDistinct != (*b).groupDistinct { return false; }
    if !equal((*a).groupingSets as *const c_void, (*b).groupingSets as *const c_void) { return false; }
    if !equal((*a).havingQual as *const c_void, (*b).havingQual as *const c_void) { return false; }
    if !equal((*a).windowClause as *const c_void, (*b).windowClause as *const c_void) { return false; }
    if !equal((*a).distinctClause as *const c_void, (*b).distinctClause as *const c_void) { return false; }
    if !equal((*a).sortClause as *const c_void, (*b).sortClause as *const c_void) { return false; }
    if !equal((*a).limitOffset as *const c_void, (*b).limitOffset as *const c_void) { return false; }
    if !equal((*a).limitCount as *const c_void, (*b).limitCount as *const c_void) { return false; }
    if (*a).limitOption != (*b).limitOption { return false; }
    if !equal((*a).rowMarks as *const c_void, (*b).rowMarks as *const c_void) { return false; }
    if !equal((*a).setOperations as *const c_void, (*b).setOperations as *const c_void) { return false; }
    if !equal((*a).constraintDeps as *const c_void, (*b).constraintDeps as *const c_void) { return false; }
    if !equal((*a).withCheckOptions as *const c_void, (*b).withCheckOptions as *const c_void) { return false; }
    true
}

unsafe fn _equalTypeName(a: *const TypeName, b: *const TypeName) -> bool {
    if !equal((*a).names as *const c_void, (*b).names as *const c_void) { return false; }
    if (*a).typeOid != (*b).typeOid { return false; }
    if (*a).setof != (*b).setof { return false; }
    if (*a).pct_type != (*b).pct_type { return false; }
    if !equal((*a).typmods as *const c_void, (*b).typmods as *const c_void) { return false; }
    if (*a).typemod != (*b).typemod { return false; }
    if !equal((*a).arrayBounds as *const c_void, (*b).arrayBounds as *const c_void) { return false; }
    true
}

unsafe fn _equalColumnRef(a: *const ColumnRef, b: *const ColumnRef) -> bool {
    if !equal((*a).fields as *const c_void, (*b).fields as *const c_void) { return false; }
    true
}

unsafe fn _equalParamRef(a: *const ParamRef, b: *const ParamRef) -> bool {
    if (*a).number != (*b).number { return false; }
    true
}

unsafe fn _equalA_Expr(a: *const A_Expr, b: *const A_Expr) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equal((*a).name as *const c_void, (*b).name as *const c_void) { return false; }
    if !equal((*a).lexpr as *const c_void, (*b).lexpr as *const c_void) { return false; }
    if !equal((*a).rexpr as *const c_void, (*b).rexpr as *const c_void) { return false; }
    true
}

unsafe fn _equalTypeCast(a: *const TypeCast, b: *const TypeCast) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    true
}

unsafe fn _equalCollateClause(a: *const CollateClause, b: *const CollateClause) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equal((*a).collname as *const c_void, (*b).collname as *const c_void) { return false; }
    true
}

unsafe fn _equalRoleSpec(a: *const RoleSpec, b: *const RoleSpec) -> bool {
    if (*a).roletype != (*b).roletype { return false; }
    if !equalstr((*a).rolename, (*b).rolename) { return false; }
    true
}

unsafe fn _equalFuncCall(a: *const FuncCall, b: *const FuncCall) -> bool {
    if !equal((*a).funcname as *const c_void, (*b).funcname as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).agg_order as *const c_void, (*b).agg_order as *const c_void) { return false; }
    if !equal((*a).agg_filter as *const c_void, (*b).agg_filter as *const c_void) { return false; }
    if !equal((*a).over as *const c_void, (*b).over as *const c_void) { return false; }
    if (*a).agg_within_group != (*b).agg_within_group { return false; }
    if (*a).agg_star != (*b).agg_star { return false; }
    if (*a).agg_distinct != (*b).agg_distinct { return false; }
    if (*a).func_variadic != (*b).func_variadic { return false; }
    true
}

unsafe fn _equalA_Star(a: *const A_Star, b: *const A_Star) -> bool {
    true
}

unsafe fn _equalA_Indices(a: *const A_Indices, b: *const A_Indices) -> bool {
    if (*a).is_slice != (*b).is_slice { return false; }
    if !equal((*a).lidx as *const c_void, (*b).lidx as *const c_void) { return false; }
    if !equal((*a).uidx as *const c_void, (*b).uidx as *const c_void) { return false; }
    true
}

unsafe fn _equalA_Indirection(a: *const A_Indirection, b: *const A_Indirection) -> bool {
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if !equal((*a).indirection as *const c_void, (*b).indirection as *const c_void) { return false; }
    true
}

unsafe fn _equalA_ArrayExpr(a: *const A_ArrayExpr, b: *const A_ArrayExpr) -> bool {
    if !equal((*a).elements as *const c_void, (*b).elements as *const c_void) { return false; }
    true
}

unsafe fn _equalResTarget(a: *const ResTarget, b: *const ResTarget) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).indirection as *const c_void, (*b).indirection as *const c_void) { return false; }
    if !equal((*a).val as *const c_void, (*b).val as *const c_void) { return false; }
    true
}

unsafe fn _equalMultiAssignRef(a: *const MultiAssignRef, b: *const MultiAssignRef) -> bool {
    if !equal((*a).source as *const c_void, (*b).source as *const c_void) { return false; }
    if (*a).colno != (*b).colno { return false; }
    if (*a).ncolumns != (*b).ncolumns { return false; }
    true
}

unsafe fn _equalSortBy(a: *const SortBy, b: *const SortBy) -> bool {
    if !equal((*a).node as *const c_void, (*b).node as *const c_void) { return false; }
    if (*a).sortby_dir != (*b).sortby_dir { return false; }
    if (*a).sortby_nulls != (*b).sortby_nulls { return false; }
    if !equal((*a).useOp as *const c_void, (*b).useOp as *const c_void) { return false; }
    true
}

unsafe fn _equalWindowDef(a: *const WindowDef, b: *const WindowDef) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equalstr((*a).refname, (*b).refname) { return false; }
    if !equal((*a).partitionClause as *const c_void, (*b).partitionClause as *const c_void) { return false; }
    if !equal((*a).orderClause as *const c_void, (*b).orderClause as *const c_void) { return false; }
    if (*a).frameOptions != (*b).frameOptions { return false; }
    if !equal((*a).startOffset as *const c_void, (*b).startOffset as *const c_void) { return false; }
    if !equal((*a).endOffset as *const c_void, (*b).endOffset as *const c_void) { return false; }
    true
}

unsafe fn _equalRangeSubselect(a: *const RangeSubselect, b: *const RangeSubselect) -> bool {
    if (*a).lateral != (*b).lateral { return false; }
    if !equal((*a).subquery as *const c_void, (*b).subquery as *const c_void) { return false; }
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    true
}

unsafe fn _equalRangeFunction(a: *const RangeFunction, b: *const RangeFunction) -> bool {
    if (*a).lateral != (*b).lateral { return false; }
    if (*a).ordinality != (*b).ordinality { return false; }
    if (*a).is_rowsfrom != (*b).is_rowsfrom { return false; }
    if !equal((*a).functions as *const c_void, (*b).functions as *const c_void) { return false; }
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    if !equal((*a).coldeflist as *const c_void, (*b).coldeflist as *const c_void) { return false; }
    true
}

unsafe fn _equalRangeTableFunc(a: *const RangeTableFunc, b: *const RangeTableFunc) -> bool {
    if (*a).lateral != (*b).lateral { return false; }
    if !equal((*a).docexpr as *const c_void, (*b).docexpr as *const c_void) { return false; }
    if !equal((*a).rowexpr as *const c_void, (*b).rowexpr as *const c_void) { return false; }
    if !equal((*a).namespaces as *const c_void, (*b).namespaces as *const c_void) { return false; }
    if !equal((*a).columns as *const c_void, (*b).columns as *const c_void) { return false; }
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    true
}

unsafe fn _equalRangeTableFuncCol(a: *const RangeTableFuncCol, b: *const RangeTableFuncCol) -> bool {
    if !equalstr((*a).colname, (*b).colname) { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if (*a).for_ordinality != (*b).for_ordinality { return false; }
    if (*a).is_not_null != (*b).is_not_null { return false; }
    if !equal((*a).colexpr as *const c_void, (*b).colexpr as *const c_void) { return false; }
    if !equal((*a).coldefexpr as *const c_void, (*b).coldefexpr as *const c_void) { return false; }
    true
}

unsafe fn _equalRangeTableSample(a: *const RangeTableSample, b: *const RangeTableSample) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).method as *const c_void, (*b).method as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).repeatable as *const c_void, (*b).repeatable as *const c_void) { return false; }
    true
}

unsafe fn _equalColumnDef(a: *const ColumnDef, b: *const ColumnDef) -> bool {
    if !equalstr((*a).colname, (*b).colname) { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equalstr((*a).compression, (*b).compression) { return false; }
    if (*a).inhcount != (*b).inhcount { return false; }
    if (*a).is_local != (*b).is_local { return false; }
    if (*a).is_not_null != (*b).is_not_null { return false; }
    if (*a).is_from_type != (*b).is_from_type { return false; }
    if (*a).storage != (*b).storage { return false; }
    if !equalstr((*a).storage_name, (*b).storage_name) { return false; }
    if !equal((*a).raw_default as *const c_void, (*b).raw_default as *const c_void) { return false; }
    if !equal((*a).cooked_default as *const c_void, (*b).cooked_default as *const c_void) { return false; }
    if (*a).identity != (*b).identity { return false; }
    if !equal((*a).identitySequence as *const c_void, (*b).identitySequence as *const c_void) { return false; }
    if (*a).generated != (*b).generated { return false; }
    if !equal((*a).collClause as *const c_void, (*b).collClause as *const c_void) { return false; }
    if (*a).collOid != (*b).collOid { return false; }
    if !equal((*a).constraints as *const c_void, (*b).constraints as *const c_void) { return false; }
    if !equal((*a).fdwoptions as *const c_void, (*b).fdwoptions as *const c_void) { return false; }
    true
}

unsafe fn _equalTableLikeClause(a: *const TableLikeClause, b: *const TableLikeClause) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if (*a).options != (*b).options { return false; }
    if (*a).relationOid != (*b).relationOid { return false; }
    true
}

unsafe fn _equalIndexElem(a: *const IndexElem, b: *const IndexElem) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equalstr((*a).indexcolname, (*b).indexcolname) { return false; }
    if !equal((*a).collation as *const c_void, (*b).collation as *const c_void) { return false; }
    if !equal((*a).opclass as *const c_void, (*b).opclass as *const c_void) { return false; }
    if !equal((*a).opclassopts as *const c_void, (*b).opclassopts as *const c_void) { return false; }
    if (*a).ordering != (*b).ordering { return false; }
    if (*a).nulls_ordering != (*b).nulls_ordering { return false; }
    true
}

unsafe fn _equalDefElem(a: *const DefElem, b: *const DefElem) -> bool {
    if !equalstr((*a).defnamespace, (*b).defnamespace) { return false; }
    if !equalstr((*a).defname, (*b).defname) { return false; }
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).defaction != (*b).defaction { return false; }
    true
}

unsafe fn _equalLockingClause(a: *const LockingClause, b: *const LockingClause) -> bool {
    if !equal((*a).lockedRels as *const c_void, (*b).lockedRels as *const c_void) { return false; }
    if (*a).strength != (*b).strength { return false; }
    if (*a).waitPolicy != (*b).waitPolicy { return false; }
    true
}

unsafe fn _equalXmlSerialize(a: *const XmlSerialize, b: *const XmlSerialize) -> bool {
    if (*a).xmloption != (*b).xmloption { return false; }
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if (*a).indent != (*b).indent { return false; }
    true
}

unsafe fn _equalPartitionElem(a: *const PartitionElem, b: *const PartitionElem) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).collation as *const c_void, (*b).collation as *const c_void) { return false; }
    if !equal((*a).opclass as *const c_void, (*b).opclass as *const c_void) { return false; }
    true
}

unsafe fn _equalPartitionSpec(a: *const PartitionSpec, b: *const PartitionSpec) -> bool {
    if (*a).strategy != (*b).strategy { return false; }
    if !equal((*a).partParams as *const c_void, (*b).partParams as *const c_void) { return false; }
    true
}

unsafe fn _equalPartitionBoundSpec(a: *const PartitionBoundSpec, b: *const PartitionBoundSpec) -> bool {
    if (*a).strategy != (*b).strategy { return false; }
    if (*a).is_default != (*b).is_default { return false; }
    if (*a).modulus != (*b).modulus { return false; }
    if (*a).remainder != (*b).remainder { return false; }
    if !equal((*a).listdatums as *const c_void, (*b).listdatums as *const c_void) { return false; }
    if !equal((*a).lowerdatums as *const c_void, (*b).lowerdatums as *const c_void) { return false; }
    if !equal((*a).upperdatums as *const c_void, (*b).upperdatums as *const c_void) { return false; }
    true
}

unsafe fn _equalPartitionRangeDatum(a: *const PartitionRangeDatum, b: *const PartitionRangeDatum) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equal((*a).value as *const c_void, (*b).value as *const c_void) { return false; }
    true
}

unsafe fn _equalPartitionCmd(a: *const PartitionCmd, b: *const PartitionCmd) -> bool {
    if !equal((*a).name as *const c_void, (*b).name as *const c_void) { return false; }
    if !equal((*a).bound as *const c_void, (*b).bound as *const c_void) { return false; }
    if (*a).concurrent != (*b).concurrent { return false; }
    true
}

unsafe fn _equalRangeTblEntry(a: *const RangeTblEntry, b: *const RangeTblEntry) -> bool {
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    if !equal((*a).eref as *const c_void, (*b).eref as *const c_void) { return false; }
    if (*a).rtekind != (*b).rtekind { return false; }
    if (*a).relid != (*b).relid { return false; }
    if (*a).inh != (*b).inh { return false; }
    if (*a).relkind != (*b).relkind { return false; }
    if (*a).rellockmode != (*b).rellockmode { return false; }
    if (*a).perminfoindex != (*b).perminfoindex { return false; }
    if !equal((*a).tablesample as *const c_void, (*b).tablesample as *const c_void) { return false; }
    if !equal((*a).subquery as *const c_void, (*b).subquery as *const c_void) { return false; }
    if (*a).security_barrier != (*b).security_barrier { return false; }
    if (*a).jointype != (*b).jointype { return false; }
    if (*a).joinmergedcols != (*b).joinmergedcols { return false; }
    if !equal((*a).joinaliasvars as *const c_void, (*b).joinaliasvars as *const c_void) { return false; }
    if !equal((*a).joinleftcols as *const c_void, (*b).joinleftcols as *const c_void) { return false; }
    if !equal((*a).joinrightcols as *const c_void, (*b).joinrightcols as *const c_void) { return false; }
    if !equal((*a).join_using_alias as *const c_void, (*b).join_using_alias as *const c_void) { return false; }
    if !equal((*a).functions as *const c_void, (*b).functions as *const c_void) { return false; }
    if (*a).funcordinality != (*b).funcordinality { return false; }
    if !equal((*a).tablefunc as *const c_void, (*b).tablefunc as *const c_void) { return false; }
    if !equal((*a).values_lists as *const c_void, (*b).values_lists as *const c_void) { return false; }
    if !equalstr((*a).ctename, (*b).ctename) { return false; }
    if (*a).ctelevelsup != (*b).ctelevelsup { return false; }
    if (*a).self_reference != (*b).self_reference { return false; }
    if !equal((*a).coltypes as *const c_void, (*b).coltypes as *const c_void) { return false; }
    if !equal((*a).coltypmods as *const c_void, (*b).coltypmods as *const c_void) { return false; }
    if !equal((*a).colcollations as *const c_void, (*b).colcollations as *const c_void) { return false; }
    if !equalstr((*a).enrname, (*b).enrname) { return false; }
    if (*a).enrtuples != (*b).enrtuples { return false; }
    if !equal((*a).groupexprs as *const c_void, (*b).groupexprs as *const c_void) { return false; }
    if (*a).lateral != (*b).lateral { return false; }
    if (*a).inFromCl != (*b).inFromCl { return false; }
    if !equal((*a).securityQuals as *const c_void, (*b).securityQuals as *const c_void) { return false; }
    true
}

unsafe fn _equalRTEPermissionInfo(a: *const RTEPermissionInfo, b: *const RTEPermissionInfo) -> bool {
    if (*a).relid != (*b).relid { return false; }
    if (*a).inh != (*b).inh { return false; }
    if (*a).requiredPerms != (*b).requiredPerms { return false; }
    if (*a).checkAsUser != (*b).checkAsUser { return false; }
    if !bms_equal((*a).selectedCols, (*b).selectedCols) { return false; }
    if !bms_equal((*a).insertedCols, (*b).insertedCols) { return false; }
    if !bms_equal((*a).updatedCols, (*b).updatedCols) { return false; }
    true
}

unsafe fn _equalRangeTblFunction(a: *const RangeTblFunction, b: *const RangeTblFunction) -> bool {
    if !equal((*a).funcexpr as *const c_void, (*b).funcexpr as *const c_void) { return false; }
    if (*a).funccolcount != (*b).funccolcount { return false; }
    if !equal((*a).funccolnames as *const c_void, (*b).funccolnames as *const c_void) { return false; }
    if !equal((*a).funccoltypes as *const c_void, (*b).funccoltypes as *const c_void) { return false; }
    if !equal((*a).funccoltypmods as *const c_void, (*b).funccoltypmods as *const c_void) { return false; }
    if !equal((*a).funccolcollations as *const c_void, (*b).funccolcollations as *const c_void) { return false; }
    if !bms_equal((*a).funcparams, (*b).funcparams) { return false; }
    true
}

unsafe fn _equalTableSampleClause(a: *const TableSampleClause, b: *const TableSampleClause) -> bool {
    if (*a).tsmhandler != (*b).tsmhandler { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).repeatable as *const c_void, (*b).repeatable as *const c_void) { return false; }
    true
}

unsafe fn _equalWithCheckOption(a: *const WithCheckOption, b: *const WithCheckOption) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equalstr((*a).relname, (*b).relname) { return false; }
    if !equalstr((*a).polname, (*b).polname) { return false; }
    if !equal((*a).qual as *const c_void, (*b).qual as *const c_void) { return false; }
    if (*a).cascaded != (*b).cascaded { return false; }
    true
}

unsafe fn _equalSortGroupClause(a: *const SortGroupClause, b: *const SortGroupClause) -> bool {
    if (*a).tleSortGroupRef != (*b).tleSortGroupRef { return false; }
    if (*a).eqop != (*b).eqop { return false; }
    if (*a).sortop != (*b).sortop { return false; }
    if (*a).reverse_sort != (*b).reverse_sort { return false; }
    if (*a).nulls_first != (*b).nulls_first { return false; }
    if (*a).hashable != (*b).hashable { return false; }
    true
}

unsafe fn _equalGroupingSet(a: *const GroupingSet, b: *const GroupingSet) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equal((*a).content as *const c_void, (*b).content as *const c_void) { return false; }
    true
}

unsafe fn _equalWindowClause(a: *const WindowClause, b: *const WindowClause) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equalstr((*a).refname, (*b).refname) { return false; }
    if !equal((*a).partitionClause as *const c_void, (*b).partitionClause as *const c_void) { return false; }
    if !equal((*a).orderClause as *const c_void, (*b).orderClause as *const c_void) { return false; }
    if (*a).frameOptions != (*b).frameOptions { return false; }
    if !equal((*a).startOffset as *const c_void, (*b).startOffset as *const c_void) { return false; }
    if !equal((*a).endOffset as *const c_void, (*b).endOffset as *const c_void) { return false; }
    if (*a).startInRangeFunc != (*b).startInRangeFunc { return false; }
    if (*a).endInRangeFunc != (*b).endInRangeFunc { return false; }
    if (*a).inRangeColl != (*b).inRangeColl { return false; }
    if (*a).inRangeAsc != (*b).inRangeAsc { return false; }
    if (*a).inRangeNullsFirst != (*b).inRangeNullsFirst { return false; }
    if (*a).winref != (*b).winref { return false; }
    if (*a).copiedOrder != (*b).copiedOrder { return false; }
    true
}

unsafe fn _equalRowMarkClause(a: *const RowMarkClause, b: *const RowMarkClause) -> bool {
    if (*a).rti != (*b).rti { return false; }
    if (*a).strength != (*b).strength { return false; }
    if (*a).waitPolicy != (*b).waitPolicy { return false; }
    if (*a).pushedDown != (*b).pushedDown { return false; }
    true
}

unsafe fn _equalWithClause(a: *const WithClause, b: *const WithClause) -> bool {
    if !equal((*a).ctes as *const c_void, (*b).ctes as *const c_void) { return false; }
    if (*a).recursive != (*b).recursive { return false; }
    true
}

unsafe fn _equalInferClause(a: *const InferClause, b: *const InferClause) -> bool {
    if !equal((*a).indexElems as *const c_void, (*b).indexElems as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if !equalstr((*a).conname, (*b).conname) { return false; }
    true
}

unsafe fn _equalOnConflictClause(a: *const OnConflictClause, b: *const OnConflictClause) -> bool {
    if (*a).action != (*b).action { return false; }
    if !equal((*a).infer as *const c_void, (*b).infer as *const c_void) { return false; }
    if !equal((*a).targetList as *const c_void, (*b).targetList as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    true
}

unsafe fn _equalCTESearchClause(a: *const CTESearchClause, b: *const CTESearchClause) -> bool {
    if !equal((*a).search_col_list as *const c_void, (*b).search_col_list as *const c_void) { return false; }
    if (*a).search_breadth_first != (*b).search_breadth_first { return false; }
    if !equalstr((*a).search_seq_column, (*b).search_seq_column) { return false; }
    true
}

unsafe fn _equalCTECycleClause(a: *const CTECycleClause, b: *const CTECycleClause) -> bool {
    if !equal((*a).cycle_col_list as *const c_void, (*b).cycle_col_list as *const c_void) { return false; }
    if !equalstr((*a).cycle_mark_column, (*b).cycle_mark_column) { return false; }
    if !equal((*a).cycle_mark_value as *const c_void, (*b).cycle_mark_value as *const c_void) { return false; }
    if !equal((*a).cycle_mark_default as *const c_void, (*b).cycle_mark_default as *const c_void) { return false; }
    if !equalstr((*a).cycle_path_column, (*b).cycle_path_column) { return false; }
    if (*a).cycle_mark_type != (*b).cycle_mark_type { return false; }
    if (*a).cycle_mark_typmod != (*b).cycle_mark_typmod { return false; }
    if (*a).cycle_mark_collation != (*b).cycle_mark_collation { return false; }
    if (*a).cycle_mark_neop != (*b).cycle_mark_neop { return false; }
    true
}

unsafe fn _equalCommonTableExpr(a: *const CommonTableExpr, b: *const CommonTableExpr) -> bool {
    if !equalstr((*a).ctename, (*b).ctename) { return false; }
    if !equal((*a).aliascolnames as *const c_void, (*b).aliascolnames as *const c_void) { return false; }
    if (*a).ctematerialized != (*b).ctematerialized { return false; }
    if !equal((*a).ctequery as *const c_void, (*b).ctequery as *const c_void) { return false; }
    if !equal((*a).search_clause as *const c_void, (*b).search_clause as *const c_void) { return false; }
    if !equal((*a).cycle_clause as *const c_void, (*b).cycle_clause as *const c_void) { return false; }
    if (*a).cterecursive != (*b).cterecursive { return false; }
    if (*a).cterefcount != (*b).cterefcount { return false; }
    if !equal((*a).ctecolnames as *const c_void, (*b).ctecolnames as *const c_void) { return false; }
    if !equal((*a).ctecoltypes as *const c_void, (*b).ctecoltypes as *const c_void) { return false; }
    if !equal((*a).ctecoltypmods as *const c_void, (*b).ctecoltypmods as *const c_void) { return false; }
    if !equal((*a).ctecolcollations as *const c_void, (*b).ctecolcollations as *const c_void) { return false; }
    true
}

unsafe fn _equalMergeWhenClause(a: *const MergeWhenClause, b: *const MergeWhenClause) -> bool {
    if (*a).matchKind != (*b).matchKind { return false; }
    if (*a).commandType != (*b).commandType { return false; }
    if (*a).r#override != (*b).r#override { return false; }
    if !equal((*a).condition as *const c_void, (*b).condition as *const c_void) { return false; }
    if !equal((*a).targetList as *const c_void, (*b).targetList as *const c_void) { return false; }
    if !equal((*a).values as *const c_void, (*b).values as *const c_void) { return false; }
    true
}

unsafe fn _equalReturningOption(a: *const ReturningOption, b: *const ReturningOption) -> bool {
    if (*a).option != (*b).option { return false; }
    if !equalstr((*a).value, (*b).value) { return false; }
    true
}

unsafe fn _equalReturningClause(a: *const ReturningClause, b: *const ReturningClause) -> bool {
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).exprs as *const c_void, (*b).exprs as *const c_void) { return false; }
    true
}

unsafe fn _equalTriggerTransition(a: *const TriggerTransition, b: *const TriggerTransition) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if (*a).isNew != (*b).isNew { return false; }
    if (*a).isTable != (*b).isTable { return false; }
    true
}

unsafe fn _equalJsonOutput(a: *const JsonOutput, b: *const JsonOutput) -> bool {
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equal((*a).returning as *const c_void, (*b).returning as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonArgument(a: *const JsonArgument, b: *const JsonArgument) -> bool {
    if !equal((*a).val as *const c_void, (*b).val as *const c_void) { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    true
}

unsafe fn _equalJsonFuncExpr(a: *const JsonFuncExpr, b: *const JsonFuncExpr) -> bool {
    if (*a).op != (*b).op { return false; }
    if !equalstr((*a).column_name, (*b).column_name) { return false; }
    if !equal((*a).context_item as *const c_void, (*b).context_item as *const c_void) { return false; }
    if !equal((*a).pathspec as *const c_void, (*b).pathspec as *const c_void) { return false; }
    if !equal((*a).passing as *const c_void, (*b).passing as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    if !equal((*a).on_empty as *const c_void, (*b).on_empty as *const c_void) { return false; }
    if !equal((*a).on_error as *const c_void, (*b).on_error as *const c_void) { return false; }
    if (*a).wrapper != (*b).wrapper { return false; }
    if (*a).quotes != (*b).quotes { return false; }
    true
}

unsafe fn _equalJsonTablePathSpec(a: *const JsonTablePathSpec, b: *const JsonTablePathSpec) -> bool {
    if !equal((*a).string as *const c_void, (*b).string as *const c_void) { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    true
}

unsafe fn _equalJsonTable(a: *const JsonTable, b: *const JsonTable) -> bool {
    if !equal((*a).context_item as *const c_void, (*b).context_item as *const c_void) { return false; }
    if !equal((*a).pathspec as *const c_void, (*b).pathspec as *const c_void) { return false; }
    if !equal((*a).passing as *const c_void, (*b).passing as *const c_void) { return false; }
    if !equal((*a).columns as *const c_void, (*b).columns as *const c_void) { return false; }
    if !equal((*a).on_error as *const c_void, (*b).on_error as *const c_void) { return false; }
    if !equal((*a).alias as *const c_void, (*b).alias as *const c_void) { return false; }
    if (*a).lateral != (*b).lateral { return false; }
    true
}

unsafe fn _equalJsonTableColumn(a: *const JsonTableColumn, b: *const JsonTableColumn) -> bool {
    if (*a).coltype != (*b).coltype { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equal((*a).pathspec as *const c_void, (*b).pathspec as *const c_void) { return false; }
    if !equal((*a).format as *const c_void, (*b).format as *const c_void) { return false; }
    if (*a).wrapper != (*b).wrapper { return false; }
    if (*a).quotes != (*b).quotes { return false; }
    if !equal((*a).columns as *const c_void, (*b).columns as *const c_void) { return false; }
    if !equal((*a).on_empty as *const c_void, (*b).on_empty as *const c_void) { return false; }
    if !equal((*a).on_error as *const c_void, (*b).on_error as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonKeyValue(a: *const JsonKeyValue, b: *const JsonKeyValue) -> bool {
    if !equal((*a).key as *const c_void, (*b).key as *const c_void) { return false; }
    if !equal((*a).value as *const c_void, (*b).value as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonParseExpr(a: *const JsonParseExpr, b: *const JsonParseExpr) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    if (*a).unique_keys != (*b).unique_keys { return false; }
    true
}

unsafe fn _equalJsonScalarExpr(a: *const JsonScalarExpr, b: *const JsonScalarExpr) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonSerializeExpr(a: *const JsonSerializeExpr, b: *const JsonSerializeExpr) -> bool {
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonObjectConstructor(a: *const JsonObjectConstructor, b: *const JsonObjectConstructor) -> bool {
    if !equal((*a).exprs as *const c_void, (*b).exprs as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    if (*a).absent_on_null != (*b).absent_on_null { return false; }
    if (*a).unique != (*b).unique { return false; }
    true
}

unsafe fn _equalJsonArrayConstructor(a: *const JsonArrayConstructor, b: *const JsonArrayConstructor) -> bool {
    if !equal((*a).exprs as *const c_void, (*b).exprs as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    if (*a).absent_on_null != (*b).absent_on_null { return false; }
    true
}

unsafe fn _equalJsonArrayQueryConstructor(a: *const JsonArrayQueryConstructor, b: *const JsonArrayQueryConstructor) -> bool {
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    if !equal((*a).format as *const c_void, (*b).format as *const c_void) { return false; }
    if (*a).absent_on_null != (*b).absent_on_null { return false; }
    true
}

unsafe fn _equalJsonAggConstructor(a: *const JsonAggConstructor, b: *const JsonAggConstructor) -> bool {
    if !equal((*a).output as *const c_void, (*b).output as *const c_void) { return false; }
    if !equal((*a).agg_filter as *const c_void, (*b).agg_filter as *const c_void) { return false; }
    if !equal((*a).agg_order as *const c_void, (*b).agg_order as *const c_void) { return false; }
    if !equal((*a).over as *const c_void, (*b).over as *const c_void) { return false; }
    true
}

unsafe fn _equalJsonObjectAgg(a: *const JsonObjectAgg, b: *const JsonObjectAgg) -> bool {
    if !equal((*a).constructor as *const c_void, (*b).constructor as *const c_void) { return false; }
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).absent_on_null != (*b).absent_on_null { return false; }
    if (*a).unique != (*b).unique { return false; }
    true
}

unsafe fn _equalJsonArrayAgg(a: *const JsonArrayAgg, b: *const JsonArrayAgg) -> bool {
    if !equal((*a).constructor as *const c_void, (*b).constructor as *const c_void) { return false; }
    if !equal((*a).arg as *const c_void, (*b).arg as *const c_void) { return false; }
    if (*a).absent_on_null != (*b).absent_on_null { return false; }
    true
}

unsafe fn _equalRawStmt(a: *const RawStmt, b: *const RawStmt) -> bool {
    if !equal((*a).stmt as *const c_void, (*b).stmt as *const c_void) { return false; }
    true
}

unsafe fn _equalInsertStmt(a: *const InsertStmt, b: *const InsertStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).cols as *const c_void, (*b).cols as *const c_void) { return false; }
    if !equal((*a).selectStmt as *const c_void, (*b).selectStmt as *const c_void) { return false; }
    if !equal((*a).onConflictClause as *const c_void, (*b).onConflictClause as *const c_void) { return false; }
    if !equal((*a).returningClause as *const c_void, (*b).returningClause as *const c_void) { return false; }
    if !equal((*a).withClause as *const c_void, (*b).withClause as *const c_void) { return false; }
    if (*a).r#override != (*b).r#override { return false; }
    true
}

unsafe fn _equalDeleteStmt(a: *const DeleteStmt, b: *const DeleteStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).usingClause as *const c_void, (*b).usingClause as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if !equal((*a).returningClause as *const c_void, (*b).returningClause as *const c_void) { return false; }
    if !equal((*a).withClause as *const c_void, (*b).withClause as *const c_void) { return false; }
    true
}

unsafe fn _equalUpdateStmt(a: *const UpdateStmt, b: *const UpdateStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).targetList as *const c_void, (*b).targetList as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if !equal((*a).fromClause as *const c_void, (*b).fromClause as *const c_void) { return false; }
    if !equal((*a).returningClause as *const c_void, (*b).returningClause as *const c_void) { return false; }
    if !equal((*a).withClause as *const c_void, (*b).withClause as *const c_void) { return false; }
    true
}

unsafe fn _equalMergeStmt(a: *const MergeStmt, b: *const MergeStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).sourceRelation as *const c_void, (*b).sourceRelation as *const c_void) { return false; }
    if !equal((*a).joinCondition as *const c_void, (*b).joinCondition as *const c_void) { return false; }
    if !equal((*a).mergeWhenClauses as *const c_void, (*b).mergeWhenClauses as *const c_void) { return false; }
    if !equal((*a).returningClause as *const c_void, (*b).returningClause as *const c_void) { return false; }
    if !equal((*a).withClause as *const c_void, (*b).withClause as *const c_void) { return false; }
    true
}

unsafe fn _equalSelectStmt(a: *const SelectStmt, b: *const SelectStmt) -> bool {
    if !equal((*a).distinctClause as *const c_void, (*b).distinctClause as *const c_void) { return false; }
    if !equal((*a).intoClause as *const c_void, (*b).intoClause as *const c_void) { return false; }
    if !equal((*a).targetList as *const c_void, (*b).targetList as *const c_void) { return false; }
    if !equal((*a).fromClause as *const c_void, (*b).fromClause as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if !equal((*a).groupClause as *const c_void, (*b).groupClause as *const c_void) { return false; }
    if (*a).groupDistinct != (*b).groupDistinct { return false; }
    if !equal((*a).havingClause as *const c_void, (*b).havingClause as *const c_void) { return false; }
    if !equal((*a).windowClause as *const c_void, (*b).windowClause as *const c_void) { return false; }
    if !equal((*a).valuesLists as *const c_void, (*b).valuesLists as *const c_void) { return false; }
    if !equal((*a).sortClause as *const c_void, (*b).sortClause as *const c_void) { return false; }
    if !equal((*a).limitOffset as *const c_void, (*b).limitOffset as *const c_void) { return false; }
    if !equal((*a).limitCount as *const c_void, (*b).limitCount as *const c_void) { return false; }
    if (*a).limitOption != (*b).limitOption { return false; }
    if !equal((*a).lockingClause as *const c_void, (*b).lockingClause as *const c_void) { return false; }
    if !equal((*a).withClause as *const c_void, (*b).withClause as *const c_void) { return false; }
    if (*a).op != (*b).op { return false; }
    if (*a).all != (*b).all { return false; }
    if !equal((*a).larg as *const c_void, (*b).larg as *const c_void) { return false; }
    if !equal((*a).rarg as *const c_void, (*b).rarg as *const c_void) { return false; }
    true
}

unsafe fn _equalSetOperationStmt(a: *const SetOperationStmt, b: *const SetOperationStmt) -> bool {
    if (*a).op != (*b).op { return false; }
    if (*a).all != (*b).all { return false; }
    if !equal((*a).larg as *const c_void, (*b).larg as *const c_void) { return false; }
    if !equal((*a).rarg as *const c_void, (*b).rarg as *const c_void) { return false; }
    if !equal((*a).colTypes as *const c_void, (*b).colTypes as *const c_void) { return false; }
    if !equal((*a).colTypmods as *const c_void, (*b).colTypmods as *const c_void) { return false; }
    if !equal((*a).colCollations as *const c_void, (*b).colCollations as *const c_void) { return false; }
    if !equal((*a).groupClauses as *const c_void, (*b).groupClauses as *const c_void) { return false; }
    true
}

unsafe fn _equalReturnStmt(a: *const ReturnStmt, b: *const ReturnStmt) -> bool {
    if !equal((*a).returnval as *const c_void, (*b).returnval as *const c_void) { return false; }
    true
}

unsafe fn _equalPLAssignStmt(a: *const PLAssignStmt, b: *const PLAssignStmt) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).indirection as *const c_void, (*b).indirection as *const c_void) { return false; }
    if (*a).nnames != (*b).nnames { return false; }
    if !equal((*a).val as *const c_void, (*b).val as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateSchemaStmt(a: *const CreateSchemaStmt, b: *const CreateSchemaStmt) -> bool {
    if !equalstr((*a).schemaname, (*b).schemaname) { return false; }
    if !equal((*a).authrole as *const c_void, (*b).authrole as *const c_void) { return false; }
    if !equal((*a).schemaElts as *const c_void, (*b).schemaElts as *const c_void) { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    true
}

unsafe fn _equalAlterTableStmt(a: *const AlterTableStmt, b: *const AlterTableStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).cmds as *const c_void, (*b).cmds as *const c_void) { return false; }
    if (*a).objtype != (*b).objtype { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalAlterTableCmd(a: *const AlterTableCmd, b: *const AlterTableCmd) -> bool {
    if (*a).subtype != (*b).subtype { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if (*a).num != (*b).num { return false; }
    if !equal((*a).newowner as *const c_void, (*b).newowner as *const c_void) { return false; }
    if !equal((*a).def as *const c_void, (*b).def as *const c_void) { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    if (*a).recurse != (*b).recurse { return false; }
    true
}

unsafe fn _equalATAlterConstraint(a: *const ATAlterConstraint, b: *const ATAlterConstraint) -> bool {
    if !equalstr((*a).conname, (*b).conname) { return false; }
    if (*a).alterEnforceability != (*b).alterEnforceability { return false; }
    if (*a).is_enforced != (*b).is_enforced { return false; }
    if (*a).alterDeferrability != (*b).alterDeferrability { return false; }
    if (*a).deferrable != (*b).deferrable { return false; }
    if (*a).initdeferred != (*b).initdeferred { return false; }
    if (*a).alterInheritability != (*b).alterInheritability { return false; }
    if (*a).noinherit != (*b).noinherit { return false; }
    true
}

unsafe fn _equalReplicaIdentityStmt(a: *const ReplicaIdentityStmt, b: *const ReplicaIdentityStmt) -> bool {
    if (*a).identity_type != (*b).identity_type { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    true
}

unsafe fn _equalAlterCollationStmt(a: *const AlterCollationStmt, b: *const AlterCollationStmt) -> bool {
    if !equal((*a).collname as *const c_void, (*b).collname as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterDomainStmt(a: *const AlterDomainStmt, b: *const AlterDomainStmt) -> bool {
    if (*a).subtype != (*b).subtype { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).def as *const c_void, (*b).def as *const c_void) { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalGrantStmt(a: *const GrantStmt, b: *const GrantStmt) -> bool {
    if (*a).is_grant != (*b).is_grant { return false; }
    if (*a).targtype != (*b).targtype { return false; }
    if (*a).objtype != (*b).objtype { return false; }
    if !equal((*a).objects as *const c_void, (*b).objects as *const c_void) { return false; }
    if !equal((*a).privileges as *const c_void, (*b).privileges as *const c_void) { return false; }
    if !equal((*a).grantees as *const c_void, (*b).grantees as *const c_void) { return false; }
    if (*a).grant_option != (*b).grant_option { return false; }
    if !equal((*a).grantor as *const c_void, (*b).grantor as *const c_void) { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    true
}

unsafe fn _equalObjectWithArgs(a: *const ObjectWithArgs, b: *const ObjectWithArgs) -> bool {
    if !equal((*a).objname as *const c_void, (*b).objname as *const c_void) { return false; }
    if !equal((*a).objargs as *const c_void, (*b).objargs as *const c_void) { return false; }
    if !equal((*a).objfuncargs as *const c_void, (*b).objfuncargs as *const c_void) { return false; }
    if (*a).args_unspecified != (*b).args_unspecified { return false; }
    true
}

unsafe fn _equalAccessPriv(a: *const AccessPriv, b: *const AccessPriv) -> bool {
    if !equalstr((*a).priv_name, (*b).priv_name) { return false; }
    if !equal((*a).cols as *const c_void, (*b).cols as *const c_void) { return false; }
    true
}

unsafe fn _equalGrantRoleStmt(a: *const GrantRoleStmt, b: *const GrantRoleStmt) -> bool {
    if !equal((*a).granted_roles as *const c_void, (*b).granted_roles as *const c_void) { return false; }
    if !equal((*a).grantee_roles as *const c_void, (*b).grantee_roles as *const c_void) { return false; }
    if (*a).is_grant != (*b).is_grant { return false; }
    if !equal((*a).opt as *const c_void, (*b).opt as *const c_void) { return false; }
    if !equal((*a).grantor as *const c_void, (*b).grantor as *const c_void) { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    true
}

unsafe fn _equalAlterDefaultPrivilegesStmt(a: *const AlterDefaultPrivilegesStmt, b: *const AlterDefaultPrivilegesStmt) -> bool {
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).action as *const c_void, (*b).action as *const c_void) { return false; }
    true
}

unsafe fn _equalCopyStmt(a: *const CopyStmt, b: *const CopyStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    if !equal((*a).attlist as *const c_void, (*b).attlist as *const c_void) { return false; }
    if (*a).is_from != (*b).is_from { return false; }
    if (*a).is_program != (*b).is_program { return false; }
    if !equalstr((*a).filename, (*b).filename) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    true
}

unsafe fn _equalVariableSetStmt(a: *const VariableSetStmt, b: *const VariableSetStmt) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if (*a).jumble_args != (*b).jumble_args { return false; }
    if (*a).is_local != (*b).is_local { return false; }
    true
}

unsafe fn _equalVariableShowStmt(a: *const VariableShowStmt, b: *const VariableShowStmt) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    true
}

unsafe fn _equalCreateStmt(a: *const CreateStmt, b: *const CreateStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).tableElts as *const c_void, (*b).tableElts as *const c_void) { return false; }
    if !equal((*a).inhRelations as *const c_void, (*b).inhRelations as *const c_void) { return false; }
    if !equal((*a).partbound as *const c_void, (*b).partbound as *const c_void) { return false; }
    if !equal((*a).partspec as *const c_void, (*b).partspec as *const c_void) { return false; }
    if !equal((*a).ofTypename as *const c_void, (*b).ofTypename as *const c_void) { return false; }
    if !equal((*a).constraints as *const c_void, (*b).constraints as *const c_void) { return false; }
    if !equal((*a).nnconstraints as *const c_void, (*b).nnconstraints as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).oncommit != (*b).oncommit { return false; }
    if !equalstr((*a).tablespacename, (*b).tablespacename) { return false; }
    if !equalstr((*a).accessMethod, (*b).accessMethod) { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    true
}

unsafe fn _equalConstraint(a: *const Constraint, b: *const Constraint) -> bool {
    if (*a).contype != (*b).contype { return false; }
    if !equalstr((*a).conname, (*b).conname) { return false; }
    if (*a).deferrable != (*b).deferrable { return false; }
    if (*a).initdeferred != (*b).initdeferred { return false; }
    if (*a).is_enforced != (*b).is_enforced { return false; }
    if (*a).skip_validation != (*b).skip_validation { return false; }
    if (*a).initially_valid != (*b).initially_valid { return false; }
    if (*a).is_no_inherit != (*b).is_no_inherit { return false; }
    if !equal((*a).raw_expr as *const c_void, (*b).raw_expr as *const c_void) { return false; }
    if !equalstr((*a).cooked_expr, (*b).cooked_expr) { return false; }
    if (*a).generated_when != (*b).generated_when { return false; }
    if (*a).generated_kind != (*b).generated_kind { return false; }
    if (*a).nulls_not_distinct != (*b).nulls_not_distinct { return false; }
    if !equal((*a).keys as *const c_void, (*b).keys as *const c_void) { return false; }
    if (*a).without_overlaps != (*b).without_overlaps { return false; }
    if !equal((*a).including as *const c_void, (*b).including as *const c_void) { return false; }
    if !equal((*a).exclusions as *const c_void, (*b).exclusions as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equalstr((*a).indexname, (*b).indexname) { return false; }
    if !equalstr((*a).indexspace, (*b).indexspace) { return false; }
    if (*a).reset_default_tblspc != (*b).reset_default_tblspc { return false; }
    if !equalstr((*a).access_method, (*b).access_method) { return false; }
    if !equal((*a).where_clause as *const c_void, (*b).where_clause as *const c_void) { return false; }
    if !equal((*a).pktable as *const c_void, (*b).pktable as *const c_void) { return false; }
    if !equal((*a).fk_attrs as *const c_void, (*b).fk_attrs as *const c_void) { return false; }
    if !equal((*a).pk_attrs as *const c_void, (*b).pk_attrs as *const c_void) { return false; }
    if (*a).fk_with_period != (*b).fk_with_period { return false; }
    if (*a).pk_with_period != (*b).pk_with_period { return false; }
    if (*a).fk_matchtype != (*b).fk_matchtype { return false; }
    if (*a).fk_upd_action != (*b).fk_upd_action { return false; }
    if (*a).fk_del_action != (*b).fk_del_action { return false; }
    if !equal((*a).fk_del_set_cols as *const c_void, (*b).fk_del_set_cols as *const c_void) { return false; }
    if !equal((*a).old_conpfeqop as *const c_void, (*b).old_conpfeqop as *const c_void) { return false; }
    if (*a).old_pktable_oid != (*b).old_pktable_oid { return false; }
    true
}

unsafe fn _equalCreateTableSpaceStmt(a: *const CreateTableSpaceStmt, b: *const CreateTableSpaceStmt) -> bool {
    if !equalstr((*a).tablespacename, (*b).tablespacename) { return false; }
    if !equal((*a).owner as *const c_void, (*b).owner as *const c_void) { return false; }
    if !equalstr((*a).location, (*b).location) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalDropTableSpaceStmt(a: *const DropTableSpaceStmt, b: *const DropTableSpaceStmt) -> bool {
    if !equalstr((*a).tablespacename, (*b).tablespacename) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalAlterTableSpaceOptionsStmt(a: *const AlterTableSpaceOptionsStmt, b: *const AlterTableSpaceOptionsStmt) -> bool {
    if !equalstr((*a).tablespacename, (*b).tablespacename) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).isReset != (*b).isReset { return false; }
    true
}

unsafe fn _equalAlterTableMoveAllStmt(a: *const AlterTableMoveAllStmt, b: *const AlterTableMoveAllStmt) -> bool {
    if !equalstr((*a).orig_tablespacename, (*b).orig_tablespacename) { return false; }
    if (*a).objtype != (*b).objtype { return false; }
    if !equal((*a).roles as *const c_void, (*b).roles as *const c_void) { return false; }
    if !equalstr((*a).new_tablespacename, (*b).new_tablespacename) { return false; }
    if (*a).nowait != (*b).nowait { return false; }
    true
}

unsafe fn _equalCreateExtensionStmt(a: *const CreateExtensionStmt, b: *const CreateExtensionStmt) -> bool {
    if !equalstr((*a).extname, (*b).extname) { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterExtensionStmt(a: *const AlterExtensionStmt, b: *const AlterExtensionStmt) -> bool {
    if !equalstr((*a).extname, (*b).extname) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterExtensionContentsStmt(a: *const AlterExtensionContentsStmt, b: *const AlterExtensionContentsStmt) -> bool {
    if !equalstr((*a).extname, (*b).extname) { return false; }
    if (*a).action != (*b).action { return false; }
    if (*a).objtype != (*b).objtype { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateFdwStmt(a: *const CreateFdwStmt, b: *const CreateFdwStmt) -> bool {
    if !equalstr((*a).fdwname, (*b).fdwname) { return false; }
    if !equal((*a).func_options as *const c_void, (*b).func_options as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterFdwStmt(a: *const AlterFdwStmt, b: *const AlterFdwStmt) -> bool {
    if !equalstr((*a).fdwname, (*b).fdwname) { return false; }
    if !equal((*a).func_options as *const c_void, (*b).func_options as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateForeignServerStmt(a: *const CreateForeignServerStmt, b: *const CreateForeignServerStmt) -> bool {
    if !equalstr((*a).servername, (*b).servername) { return false; }
    if !equalstr((*a).servertype, (*b).servertype) { return false; }
    if !equalstr((*a).version, (*b).version) { return false; }
    if !equalstr((*a).fdwname, (*b).fdwname) { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterForeignServerStmt(a: *const AlterForeignServerStmt, b: *const AlterForeignServerStmt) -> bool {
    if !equalstr((*a).servername, (*b).servername) { return false; }
    if !equalstr((*a).version, (*b).version) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).has_version != (*b).has_version { return false; }
    true
}

unsafe fn _equalCreateForeignTableStmt(a: *const CreateForeignTableStmt, b: *const CreateForeignTableStmt) -> bool {
    if !equal((*a).base.relation as *const c_void, (*b).base.relation as *const c_void) { return false; }
    if !equal((*a).base.tableElts as *const c_void, (*b).base.tableElts as *const c_void) { return false; }
    if !equal((*a).base.inhRelations as *const c_void, (*b).base.inhRelations as *const c_void) { return false; }
    if !equal((*a).base.partbound as *const c_void, (*b).base.partbound as *const c_void) { return false; }
    if !equal((*a).base.partspec as *const c_void, (*b).base.partspec as *const c_void) { return false; }
    if !equal((*a).base.ofTypename as *const c_void, (*b).base.ofTypename as *const c_void) { return false; }
    if !equal((*a).base.constraints as *const c_void, (*b).base.constraints as *const c_void) { return false; }
    if !equal((*a).base.nnconstraints as *const c_void, (*b).base.nnconstraints as *const c_void) { return false; }
    if !equal((*a).base.options as *const c_void, (*b).base.options as *const c_void) { return false; }
    if (*a).base.oncommit != (*b).base.oncommit { return false; }
    if !equalstr((*a).base.tablespacename, (*b).base.tablespacename) { return false; }
    if !equalstr((*a).base.accessMethod, (*b).base.accessMethod) { return false; }
    if (*a).base.if_not_exists != (*b).base.if_not_exists { return false; }
    if !equalstr((*a).servername, (*b).servername) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateUserMappingStmt(a: *const CreateUserMappingStmt, b: *const CreateUserMappingStmt) -> bool {
    if !equal((*a).user as *const c_void, (*b).user as *const c_void) { return false; }
    if !equalstr((*a).servername, (*b).servername) { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterUserMappingStmt(a: *const AlterUserMappingStmt, b: *const AlterUserMappingStmt) -> bool {
    if !equal((*a).user as *const c_void, (*b).user as *const c_void) { return false; }
    if !equalstr((*a).servername, (*b).servername) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalDropUserMappingStmt(a: *const DropUserMappingStmt, b: *const DropUserMappingStmt) -> bool {
    if !equal((*a).user as *const c_void, (*b).user as *const c_void) { return false; }
    if !equalstr((*a).servername, (*b).servername) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalImportForeignSchemaStmt(a: *const ImportForeignSchemaStmt, b: *const ImportForeignSchemaStmt) -> bool {
    if !equalstr((*a).server_name, (*b).server_name) { return false; }
    if !equalstr((*a).remote_schema, (*b).remote_schema) { return false; }
    if !equalstr((*a).local_schema, (*b).local_schema) { return false; }
    if (*a).list_type != (*b).list_type { return false; }
    if !equal((*a).table_list as *const c_void, (*b).table_list as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalCreatePolicyStmt(a: *const CreatePolicyStmt, b: *const CreatePolicyStmt) -> bool {
    if !equalstr((*a).policy_name, (*b).policy_name) { return false; }
    if !equal((*a).table as *const c_void, (*b).table as *const c_void) { return false; }
    if !equalstr((*a).cmd_name, (*b).cmd_name) { return false; }
    if (*a).permissive != (*b).permissive { return false; }
    if !equal((*a).roles as *const c_void, (*b).roles as *const c_void) { return false; }
    if !equal((*a).qual as *const c_void, (*b).qual as *const c_void) { return false; }
    if !equal((*a).with_check as *const c_void, (*b).with_check as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterPolicyStmt(a: *const AlterPolicyStmt, b: *const AlterPolicyStmt) -> bool {
    if !equalstr((*a).policy_name, (*b).policy_name) { return false; }
    if !equal((*a).table as *const c_void, (*b).table as *const c_void) { return false; }
    if !equal((*a).roles as *const c_void, (*b).roles as *const c_void) { return false; }
    if !equal((*a).qual as *const c_void, (*b).qual as *const c_void) { return false; }
    if !equal((*a).with_check as *const c_void, (*b).with_check as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateAmStmt(a: *const CreateAmStmt, b: *const CreateAmStmt) -> bool {
    if !equalstr((*a).amname, (*b).amname) { return false; }
    if !equal((*a).handler_name as *const c_void, (*b).handler_name as *const c_void) { return false; }
    if (*a).amtype != (*b).amtype { return false; }
    true
}

unsafe fn _equalCreateTrigStmt(a: *const CreateTrigStmt, b: *const CreateTrigStmt) -> bool {
    if (*a).replace != (*b).replace { return false; }
    if (*a).isconstraint != (*b).isconstraint { return false; }
    if !equalstr((*a).trigname, (*b).trigname) { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).funcname as *const c_void, (*b).funcname as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if (*a).row != (*b).row { return false; }
    if (*a).timing != (*b).timing { return false; }
    if (*a).events != (*b).events { return false; }
    if !equal((*a).columns as *const c_void, (*b).columns as *const c_void) { return false; }
    if !equal((*a).whenClause as *const c_void, (*b).whenClause as *const c_void) { return false; }
    if !equal((*a).transitionRels as *const c_void, (*b).transitionRels as *const c_void) { return false; }
    if (*a).deferrable != (*b).deferrable { return false; }
    if (*a).initdeferred != (*b).initdeferred { return false; }
    if !equal((*a).constrrel as *const c_void, (*b).constrrel as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateEventTrigStmt(a: *const CreateEventTrigStmt, b: *const CreateEventTrigStmt) -> bool {
    if !equalstr((*a).trigname, (*b).trigname) { return false; }
    if !equalstr((*a).eventname, (*b).eventname) { return false; }
    if !equal((*a).whenclause as *const c_void, (*b).whenclause as *const c_void) { return false; }
    if !equal((*a).funcname as *const c_void, (*b).funcname as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterEventTrigStmt(a: *const AlterEventTrigStmt, b: *const AlterEventTrigStmt) -> bool {
    if !equalstr((*a).trigname, (*b).trigname) { return false; }
    if (*a).tgenabled != (*b).tgenabled { return false; }
    true
}

unsafe fn _equalCreatePLangStmt(a: *const CreatePLangStmt, b: *const CreatePLangStmt) -> bool {
    if (*a).replace != (*b).replace { return false; }
    if !equalstr((*a).plname, (*b).plname) { return false; }
    if !equal((*a).plhandler as *const c_void, (*b).plhandler as *const c_void) { return false; }
    if !equal((*a).plinline as *const c_void, (*b).plinline as *const c_void) { return false; }
    if !equal((*a).plvalidator as *const c_void, (*b).plvalidator as *const c_void) { return false; }
    if (*a).pltrusted != (*b).pltrusted { return false; }
    true
}

unsafe fn _equalCreateRoleStmt(a: *const CreateRoleStmt, b: *const CreateRoleStmt) -> bool {
    if (*a).stmt_type != (*b).stmt_type { return false; }
    if !equalstr((*a).role, (*b).role) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterRoleStmt(a: *const AlterRoleStmt, b: *const AlterRoleStmt) -> bool {
    if !equal((*a).role as *const c_void, (*b).role as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).action != (*b).action { return false; }
    true
}

unsafe fn _equalAlterRoleSetStmt(a: *const AlterRoleSetStmt, b: *const AlterRoleSetStmt) -> bool {
    if !equal((*a).role as *const c_void, (*b).role as *const c_void) { return false; }
    if !equalstr((*a).database, (*b).database) { return false; }
    if !equal((*a).setstmt as *const c_void, (*b).setstmt as *const c_void) { return false; }
    true
}

unsafe fn _equalDropRoleStmt(a: *const DropRoleStmt, b: *const DropRoleStmt) -> bool {
    if !equal((*a).roles as *const c_void, (*b).roles as *const c_void) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalCreateSeqStmt(a: *const CreateSeqStmt, b: *const CreateSeqStmt) -> bool {
    if !equal((*a).sequence as *const c_void, (*b).sequence as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).ownerId != (*b).ownerId { return false; }
    if (*a).for_identity != (*b).for_identity { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    true
}

unsafe fn _equalAlterSeqStmt(a: *const AlterSeqStmt, b: *const AlterSeqStmt) -> bool {
    if !equal((*a).sequence as *const c_void, (*b).sequence as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).for_identity != (*b).for_identity { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalDefineStmt(a: *const DefineStmt, b: *const DefineStmt) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if (*a).oldstyle != (*b).oldstyle { return false; }
    if !equal((*a).defnames as *const c_void, (*b).defnames as *const c_void) { return false; }
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    if !equal((*a).definition as *const c_void, (*b).definition as *const c_void) { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    if (*a).replace != (*b).replace { return false; }
    true
}

unsafe fn _equalCreateDomainStmt(a: *const CreateDomainStmt, b: *const CreateDomainStmt) -> bool {
    if !equal((*a).domainname as *const c_void, (*b).domainname as *const c_void) { return false; }
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equal((*a).collClause as *const c_void, (*b).collClause as *const c_void) { return false; }
    if !equal((*a).constraints as *const c_void, (*b).constraints as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateOpClassStmt(a: *const CreateOpClassStmt, b: *const CreateOpClassStmt) -> bool {
    if !equal((*a).opclassname as *const c_void, (*b).opclassname as *const c_void) { return false; }
    if !equal((*a).opfamilyname as *const c_void, (*b).opfamilyname as *const c_void) { return false; }
    if !equalstr((*a).amname, (*b).amname) { return false; }
    if !equal((*a).datatype as *const c_void, (*b).datatype as *const c_void) { return false; }
    if !equal((*a).items as *const c_void, (*b).items as *const c_void) { return false; }
    if (*a).isDefault != (*b).isDefault { return false; }
    true
}

unsafe fn _equalCreateOpClassItem(a: *const CreateOpClassItem, b: *const CreateOpClassItem) -> bool {
    if (*a).itemtype != (*b).itemtype { return false; }
    if !equal((*a).name as *const c_void, (*b).name as *const c_void) { return false; }
    if (*a).number != (*b).number { return false; }
    if !equal((*a).order_family as *const c_void, (*b).order_family as *const c_void) { return false; }
    if !equal((*a).class_args as *const c_void, (*b).class_args as *const c_void) { return false; }
    if !equal((*a).storedtype as *const c_void, (*b).storedtype as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateOpFamilyStmt(a: *const CreateOpFamilyStmt, b: *const CreateOpFamilyStmt) -> bool {
    if !equal((*a).opfamilyname as *const c_void, (*b).opfamilyname as *const c_void) { return false; }
    if !equalstr((*a).amname, (*b).amname) { return false; }
    true
}

unsafe fn _equalAlterOpFamilyStmt(a: *const AlterOpFamilyStmt, b: *const AlterOpFamilyStmt) -> bool {
    if !equal((*a).opfamilyname as *const c_void, (*b).opfamilyname as *const c_void) { return false; }
    if !equalstr((*a).amname, (*b).amname) { return false; }
    if (*a).isDrop != (*b).isDrop { return false; }
    if !equal((*a).items as *const c_void, (*b).items as *const c_void) { return false; }
    true
}

unsafe fn _equalDropStmt(a: *const DropStmt, b: *const DropStmt) -> bool {
    if !equal((*a).objects as *const c_void, (*b).objects as *const c_void) { return false; }
    if (*a).removeType != (*b).removeType { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    if (*a).concurrent != (*b).concurrent { return false; }
    true
}

unsafe fn _equalTruncateStmt(a: *const TruncateStmt, b: *const TruncateStmt) -> bool {
    if !equal((*a).relations as *const c_void, (*b).relations as *const c_void) { return false; }
    if (*a).restart_seqs != (*b).restart_seqs { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    true
}

unsafe fn _equalCommentStmt(a: *const CommentStmt, b: *const CommentStmt) -> bool {
    if (*a).objtype != (*b).objtype { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    if !equalstr((*a).comment, (*b).comment) { return false; }
    true
}

unsafe fn _equalSecLabelStmt(a: *const SecLabelStmt, b: *const SecLabelStmt) -> bool {
    if (*a).objtype != (*b).objtype { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    if !equalstr((*a).provider, (*b).provider) { return false; }
    if !equalstr((*a).label, (*b).label) { return false; }
    true
}

unsafe fn _equalDeclareCursorStmt(a: *const DeclareCursorStmt, b: *const DeclareCursorStmt) -> bool {
    if !equalstr((*a).portalname, (*b).portalname) { return false; }
    if (*a).options != (*b).options { return false; }
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    true
}

unsafe fn _equalClosePortalStmt(a: *const ClosePortalStmt, b: *const ClosePortalStmt) -> bool {
    if !equalstr((*a).portalname, (*b).portalname) { return false; }
    true
}

unsafe fn _equalFetchStmt(a: *const FetchStmt, b: *const FetchStmt) -> bool {
    if (*a).direction != (*b).direction { return false; }
    if (*a).howMany != (*b).howMany { return false; }
    if !equalstr((*a).portalname, (*b).portalname) { return false; }
    if (*a).ismove != (*b).ismove { return false; }
    true
}

unsafe fn _equalIndexStmt(a: *const IndexStmt, b: *const IndexStmt) -> bool {
    if !equalstr((*a).idxname, (*b).idxname) { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equalstr((*a).accessMethod, (*b).accessMethod) { return false; }
    if !equalstr((*a).tableSpace, (*b).tableSpace) { return false; }
    if !equal((*a).indexParams as *const c_void, (*b).indexParams as *const c_void) { return false; }
    if !equal((*a).indexIncludingParams as *const c_void, (*b).indexIncludingParams as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if !equal((*a).excludeOpNames as *const c_void, (*b).excludeOpNames as *const c_void) { return false; }
    if !equalstr((*a).idxcomment, (*b).idxcomment) { return false; }
    if (*a).indexOid != (*b).indexOid { return false; }
    if (*a).oldNumber != (*b).oldNumber { return false; }
    if (*a).oldCreateSubid != (*b).oldCreateSubid { return false; }
    if (*a).oldFirstRelfilelocatorSubid != (*b).oldFirstRelfilelocatorSubid { return false; }
    if (*a).unique != (*b).unique { return false; }
    if (*a).nulls_not_distinct != (*b).nulls_not_distinct { return false; }
    if (*a).primary != (*b).primary { return false; }
    if (*a).isconstraint != (*b).isconstraint { return false; }
    if (*a).iswithoutoverlaps != (*b).iswithoutoverlaps { return false; }
    if (*a).deferrable != (*b).deferrable { return false; }
    if (*a).initdeferred != (*b).initdeferred { return false; }
    if (*a).transformed != (*b).transformed { return false; }
    if (*a).concurrent != (*b).concurrent { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    if (*a).reset_default_tblspc != (*b).reset_default_tblspc { return false; }
    true
}

unsafe fn _equalCreateStatsStmt(a: *const CreateStatsStmt, b: *const CreateStatsStmt) -> bool {
    if !equal((*a).defnames as *const c_void, (*b).defnames as *const c_void) { return false; }
    if !equal((*a).stat_types as *const c_void, (*b).stat_types as *const c_void) { return false; }
    if !equal((*a).exprs as *const c_void, (*b).exprs as *const c_void) { return false; }
    if !equal((*a).relations as *const c_void, (*b).relations as *const c_void) { return false; }
    if !equalstr((*a).stxcomment, (*b).stxcomment) { return false; }
    if (*a).transformed != (*b).transformed { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    true
}

unsafe fn _equalStatsElem(a: *const StatsElem, b: *const StatsElem) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).expr as *const c_void, (*b).expr as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterStatsStmt(a: *const AlterStatsStmt, b: *const AlterStatsStmt) -> bool {
    if !equal((*a).defnames as *const c_void, (*b).defnames as *const c_void) { return false; }
    if !equal((*a).stxstattarget as *const c_void, (*b).stxstattarget as *const c_void) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalCreateFunctionStmt(a: *const CreateFunctionStmt, b: *const CreateFunctionStmt) -> bool {
    if (*a).is_procedure != (*b).is_procedure { return false; }
    if (*a).replace != (*b).replace { return false; }
    if !equal((*a).funcname as *const c_void, (*b).funcname as *const c_void) { return false; }
    if !equal((*a).parameters as *const c_void, (*b).parameters as *const c_void) { return false; }
    if !equal((*a).returnType as *const c_void, (*b).returnType as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).sql_body as *const c_void, (*b).sql_body as *const c_void) { return false; }
    true
}

unsafe fn _equalFunctionParameter(a: *const FunctionParameter, b: *const FunctionParameter) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).argType as *const c_void, (*b).argType as *const c_void) { return false; }
    if (*a).mode != (*b).mode { return false; }
    if !equal((*a).defexpr as *const c_void, (*b).defexpr as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterFunctionStmt(a: *const AlterFunctionStmt, b: *const AlterFunctionStmt) -> bool {
    if (*a).objtype != (*b).objtype { return false; }
    if !equal((*a).func as *const c_void, (*b).func as *const c_void) { return false; }
    if !equal((*a).actions as *const c_void, (*b).actions as *const c_void) { return false; }
    true
}

unsafe fn _equalDoStmt(a: *const DoStmt, b: *const DoStmt) -> bool {
    if !equal((*a).args as *const c_void, (*b).args as *const c_void) { return false; }
    true
}

unsafe fn _equalCallStmt(a: *const CallStmt, b: *const CallStmt) -> bool {
    if !equal((*a).funccall as *const c_void, (*b).funccall as *const c_void) { return false; }
    if !equal((*a).funcexpr as *const c_void, (*b).funcexpr as *const c_void) { return false; }
    if !equal((*a).outargs as *const c_void, (*b).outargs as *const c_void) { return false; }
    true
}

unsafe fn _equalRenameStmt(a: *const RenameStmt, b: *const RenameStmt) -> bool {
    if (*a).renameType != (*b).renameType { return false; }
    if (*a).relationType != (*b).relationType { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    if !equalstr((*a).subname, (*b).subname) { return false; }
    if !equalstr((*a).newname, (*b).newname) { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalAlterObjectDependsStmt(a: *const AlterObjectDependsStmt, b: *const AlterObjectDependsStmt) -> bool {
    if (*a).objectType != (*b).objectType { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    if !equal((*a).extname as *const c_void, (*b).extname as *const c_void) { return false; }
    if (*a).remove != (*b).remove { return false; }
    true
}

unsafe fn _equalAlterObjectSchemaStmt(a: *const AlterObjectSchemaStmt, b: *const AlterObjectSchemaStmt) -> bool {
    if (*a).objectType != (*b).objectType { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    if !equalstr((*a).newschema, (*b).newschema) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalAlterOwnerStmt(a: *const AlterOwnerStmt, b: *const AlterOwnerStmt) -> bool {
    if (*a).objectType != (*b).objectType { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).object as *const c_void, (*b).object as *const c_void) { return false; }
    if !equal((*a).newowner as *const c_void, (*b).newowner as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterOperatorStmt(a: *const AlterOperatorStmt, b: *const AlterOperatorStmt) -> bool {
    if !equal((*a).opername as *const c_void, (*b).opername as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterTypeStmt(a: *const AlterTypeStmt, b: *const AlterTypeStmt) -> bool {
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalRuleStmt(a: *const RuleStmt, b: *const RuleStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equalstr((*a).rulename, (*b).rulename) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if (*a).event != (*b).event { return false; }
    if (*a).instead != (*b).instead { return false; }
    if !equal((*a).actions as *const c_void, (*b).actions as *const c_void) { return false; }
    if (*a).replace != (*b).replace { return false; }
    true
}

unsafe fn _equalNotifyStmt(a: *const NotifyStmt, b: *const NotifyStmt) -> bool {
    if !equalstr((*a).conditionname, (*b).conditionname) { return false; }
    if !equalstr((*a).payload, (*b).payload) { return false; }
    true
}

unsafe fn _equalListenStmt(a: *const ListenStmt, b: *const ListenStmt) -> bool {
    if !equalstr((*a).conditionname, (*b).conditionname) { return false; }
    true
}

unsafe fn _equalUnlistenStmt(a: *const UnlistenStmt, b: *const UnlistenStmt) -> bool {
    if !equalstr((*a).conditionname, (*b).conditionname) { return false; }
    true
}

unsafe fn _equalTransactionStmt(a: *const TransactionStmt, b: *const TransactionStmt) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equalstr((*a).savepoint_name, (*b).savepoint_name) { return false; }
    if !equalstr((*a).gid, (*b).gid) { return false; }
    if (*a).chain != (*b).chain { return false; }
    true
}

unsafe fn _equalCompositeTypeStmt(a: *const CompositeTypeStmt, b: *const CompositeTypeStmt) -> bool {
    if !equal((*a).typevar as *const c_void, (*b).typevar as *const c_void) { return false; }
    if !equal((*a).coldeflist as *const c_void, (*b).coldeflist as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateEnumStmt(a: *const CreateEnumStmt, b: *const CreateEnumStmt) -> bool {
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equal((*a).vals as *const c_void, (*b).vals as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateRangeStmt(a: *const CreateRangeStmt, b: *const CreateRangeStmt) -> bool {
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equal((*a).params as *const c_void, (*b).params as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterEnumStmt(a: *const AlterEnumStmt, b: *const AlterEnumStmt) -> bool {
    if !equal((*a).typeName as *const c_void, (*b).typeName as *const c_void) { return false; }
    if !equalstr((*a).oldVal, (*b).oldVal) { return false; }
    if !equalstr((*a).newVal, (*b).newVal) { return false; }
    if !equalstr((*a).newValNeighbor, (*b).newValNeighbor) { return false; }
    if (*a).newValIsAfter != (*b).newValIsAfter { return false; }
    if (*a).skipIfNewValExists != (*b).skipIfNewValExists { return false; }
    true
}

unsafe fn _equalViewStmt(a: *const ViewStmt, b: *const ViewStmt) -> bool {
    if !equal((*a).view as *const c_void, (*b).view as *const c_void) { return false; }
    if !equal((*a).aliases as *const c_void, (*b).aliases as *const c_void) { return false; }
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    if (*a).replace != (*b).replace { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if (*a).withCheckOption != (*b).withCheckOption { return false; }
    true
}

unsafe fn _equalLoadStmt(a: *const LoadStmt, b: *const LoadStmt) -> bool {
    if !equalstr((*a).filename, (*b).filename) { return false; }
    true
}

unsafe fn _equalCreatedbStmt(a: *const CreatedbStmt, b: *const CreatedbStmt) -> bool {
    if !equalstr((*a).dbname, (*b).dbname) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterDatabaseStmt(a: *const AlterDatabaseStmt, b: *const AlterDatabaseStmt) -> bool {
    if !equalstr((*a).dbname, (*b).dbname) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterDatabaseRefreshCollStmt(a: *const AlterDatabaseRefreshCollStmt, b: *const AlterDatabaseRefreshCollStmt) -> bool {
    if !equalstr((*a).dbname, (*b).dbname) { return false; }
    true
}

unsafe fn _equalAlterDatabaseSetStmt(a: *const AlterDatabaseSetStmt, b: *const AlterDatabaseSetStmt) -> bool {
    if !equalstr((*a).dbname, (*b).dbname) { return false; }
    if !equal((*a).setstmt as *const c_void, (*b).setstmt as *const c_void) { return false; }
    true
}

unsafe fn _equalDropdbStmt(a: *const DropdbStmt, b: *const DropdbStmt) -> bool {
    if !equalstr((*a).dbname, (*b).dbname) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterSystemStmt(a: *const AlterSystemStmt, b: *const AlterSystemStmt) -> bool {
    if !equal((*a).setstmt as *const c_void, (*b).setstmt as *const c_void) { return false; }
    true
}

unsafe fn _equalClusterStmt(a: *const ClusterStmt, b: *const ClusterStmt) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equalstr((*a).indexname, (*b).indexname) { return false; }
    if !equal((*a).params as *const c_void, (*b).params as *const c_void) { return false; }
    true
}

unsafe fn _equalVacuumStmt(a: *const VacuumStmt, b: *const VacuumStmt) -> bool {
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).rels as *const c_void, (*b).rels as *const c_void) { return false; }
    if (*a).is_vacuumcmd != (*b).is_vacuumcmd { return false; }
    true
}

unsafe fn _equalVacuumRelation(a: *const VacuumRelation, b: *const VacuumRelation) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if (*a).oid != (*b).oid { return false; }
    if !equal((*a).va_cols as *const c_void, (*b).va_cols as *const c_void) { return false; }
    true
}

unsafe fn _equalExplainStmt(a: *const ExplainStmt, b: *const ExplainStmt) -> bool {
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateTableAsStmt(a: *const CreateTableAsStmt, b: *const CreateTableAsStmt) -> bool {
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    if !equal((*a).into as *const c_void, (*b).into as *const c_void) { return false; }
    if (*a).objtype != (*b).objtype { return false; }
    if (*a).is_select_into != (*b).is_select_into { return false; }
    if (*a).if_not_exists != (*b).if_not_exists { return false; }
    true
}

unsafe fn _equalRefreshMatViewStmt(a: *const RefreshMatViewStmt, b: *const RefreshMatViewStmt) -> bool {
    if (*a).concurrent != (*b).concurrent { return false; }
    if (*a).skipData != (*b).skipData { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    true
}

unsafe fn _equalCheckPointStmt(a: *const CheckPointStmt, b: *const CheckPointStmt) -> bool {
    true
}

unsafe fn _equalDiscardStmt(a: *const DiscardStmt, b: *const DiscardStmt) -> bool {
    if (*a).target != (*b).target { return false; }
    true
}

unsafe fn _equalLockStmt(a: *const LockStmt, b: *const LockStmt) -> bool {
    if !equal((*a).relations as *const c_void, (*b).relations as *const c_void) { return false; }
    if (*a).mode != (*b).mode { return false; }
    if (*a).nowait != (*b).nowait { return false; }
    true
}

unsafe fn _equalConstraintsSetStmt(a: *const ConstraintsSetStmt, b: *const ConstraintsSetStmt) -> bool {
    if !equal((*a).constraints as *const c_void, (*b).constraints as *const c_void) { return false; }
    if (*a).deferred != (*b).deferred { return false; }
    true
}

unsafe fn _equalReindexStmt(a: *const ReindexStmt, b: *const ReindexStmt) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).params as *const c_void, (*b).params as *const c_void) { return false; }
    true
}

unsafe fn _equalCreateConversionStmt(a: *const CreateConversionStmt, b: *const CreateConversionStmt) -> bool {
    if !equal((*a).conversion_name as *const c_void, (*b).conversion_name as *const c_void) { return false; }
    if !equalstr((*a).for_encoding_name, (*b).for_encoding_name) { return false; }
    if !equalstr((*a).to_encoding_name, (*b).to_encoding_name) { return false; }
    if !equal((*a).func_name as *const c_void, (*b).func_name as *const c_void) { return false; }
    if (*a).def != (*b).def { return false; }
    true
}

unsafe fn _equalCreateCastStmt(a: *const CreateCastStmt, b: *const CreateCastStmt) -> bool {
    if !equal((*a).sourcetype as *const c_void, (*b).sourcetype as *const c_void) { return false; }
    if !equal((*a).targettype as *const c_void, (*b).targettype as *const c_void) { return false; }
    if !equal((*a).func as *const c_void, (*b).func as *const c_void) { return false; }
    if (*a).context != (*b).context { return false; }
    if (*a).inout != (*b).inout { return false; }
    true
}

unsafe fn _equalCreateTransformStmt(a: *const CreateTransformStmt, b: *const CreateTransformStmt) -> bool {
    if (*a).replace != (*b).replace { return false; }
    if !equal((*a).type_name as *const c_void, (*b).type_name as *const c_void) { return false; }
    if !equalstr((*a).lang, (*b).lang) { return false; }
    if !equal((*a).fromsql as *const c_void, (*b).fromsql as *const c_void) { return false; }
    if !equal((*a).tosql as *const c_void, (*b).tosql as *const c_void) { return false; }
    true
}

unsafe fn _equalPrepareStmt(a: *const PrepareStmt, b: *const PrepareStmt) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).argtypes as *const c_void, (*b).argtypes as *const c_void) { return false; }
    if !equal((*a).query as *const c_void, (*b).query as *const c_void) { return false; }
    true
}

unsafe fn _equalExecuteStmt(a: *const ExecuteStmt, b: *const ExecuteStmt) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).params as *const c_void, (*b).params as *const c_void) { return false; }
    true
}

unsafe fn _equalDeallocateStmt(a: *const DeallocateStmt, b: *const DeallocateStmt) -> bool {
    if !equalstr((*a).name, (*b).name) { return false; }
    if (*a).isall != (*b).isall { return false; }
    true
}

unsafe fn _equalDropOwnedStmt(a: *const DropOwnedStmt, b: *const DropOwnedStmt) -> bool {
    if !equal((*a).roles as *const c_void, (*b).roles as *const c_void) { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    true
}

unsafe fn _equalReassignOwnedStmt(a: *const ReassignOwnedStmt, b: *const ReassignOwnedStmt) -> bool {
    if !equal((*a).roles as *const c_void, (*b).roles as *const c_void) { return false; }
    if !equal((*a).newrole as *const c_void, (*b).newrole as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterTSDictionaryStmt(a: *const AlterTSDictionaryStmt, b: *const AlterTSDictionaryStmt) -> bool {
    if !equal((*a).dictname as *const c_void, (*b).dictname as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterTSConfigurationStmt(a: *const AlterTSConfigurationStmt, b: *const AlterTSConfigurationStmt) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equal((*a).cfgname as *const c_void, (*b).cfgname as *const c_void) { return false; }
    if !equal((*a).tokentype as *const c_void, (*b).tokentype as *const c_void) { return false; }
    if !equal((*a).dicts as *const c_void, (*b).dicts as *const c_void) { return false; }
    if (*a).r#override != (*b).r#override { return false; }
    if (*a).replace != (*b).replace { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    true
}

unsafe fn _equalPublicationTable(a: *const PublicationTable, b: *const PublicationTable) -> bool {
    if !equal((*a).relation as *const c_void, (*b).relation as *const c_void) { return false; }
    if !equal((*a).whereClause as *const c_void, (*b).whereClause as *const c_void) { return false; }
    if !equal((*a).columns as *const c_void, (*b).columns as *const c_void) { return false; }
    true
}

unsafe fn _equalPublicationObjSpec(a: *const PublicationObjSpec, b: *const PublicationObjSpec) -> bool {
    if (*a).pubobjtype != (*b).pubobjtype { return false; }
    if !equalstr((*a).name, (*b).name) { return false; }
    if !equal((*a).pubtable as *const c_void, (*b).pubtable as *const c_void) { return false; }
    true
}

unsafe fn _equalCreatePublicationStmt(a: *const CreatePublicationStmt, b: *const CreatePublicationStmt) -> bool {
    if !equalstr((*a).pubname, (*b).pubname) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).pubobjects as *const c_void, (*b).pubobjects as *const c_void) { return false; }
    if (*a).for_all_tables != (*b).for_all_tables { return false; }
    true
}

unsafe fn _equalAlterPublicationStmt(a: *const AlterPublicationStmt, b: *const AlterPublicationStmt) -> bool {
    if !equalstr((*a).pubname, (*b).pubname) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    if !equal((*a).pubobjects as *const c_void, (*b).pubobjects as *const c_void) { return false; }
    if (*a).for_all_tables != (*b).for_all_tables { return false; }
    if (*a).action != (*b).action { return false; }
    true
}

unsafe fn _equalCreateSubscriptionStmt(a: *const CreateSubscriptionStmt, b: *const CreateSubscriptionStmt) -> bool {
    if !equalstr((*a).subname, (*b).subname) { return false; }
    if !equalstr((*a).conninfo, (*b).conninfo) { return false; }
    if !equal((*a).publication as *const c_void, (*b).publication as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalAlterSubscriptionStmt(a: *const AlterSubscriptionStmt, b: *const AlterSubscriptionStmt) -> bool {
    if (*a).kind != (*b).kind { return false; }
    if !equalstr((*a).subname, (*b).subname) { return false; }
    if !equalstr((*a).conninfo, (*b).conninfo) { return false; }
    if !equal((*a).publication as *const c_void, (*b).publication as *const c_void) { return false; }
    if !equal((*a).options as *const c_void, (*b).options as *const c_void) { return false; }
    true
}

unsafe fn _equalDropSubscriptionStmt(a: *const DropSubscriptionStmt, b: *const DropSubscriptionStmt) -> bool {
    if !equalstr((*a).subname, (*b).subname) { return false; }
    if (*a).missing_ok != (*b).missing_ok { return false; }
    if (*a).behavior != (*b).behavior { return false; }
    true
}

unsafe fn _equalPathKey(a: *const PathKey, b: *const PathKey) -> bool {
    if (*a).pk_eclass != (*b).pk_eclass { return false; }
    if (*a).pk_opfamily != (*b).pk_opfamily { return false; }
    if (*a).pk_cmptype != (*b).pk_cmptype { return false; }
    if (*a).pk_nulls_first != (*b).pk_nulls_first { return false; }
    true
}

unsafe fn _equalGroupByOrdering(a: *const GroupByOrdering, b: *const GroupByOrdering) -> bool {
    if !equal((*a).pathkeys as *const c_void, (*b).pathkeys as *const c_void) { return false; }
    if !equal((*a).clauses as *const c_void, (*b).clauses as *const c_void) { return false; }
    true
}

unsafe fn _equalRestrictInfo(a: *const RestrictInfo, b: *const RestrictInfo) -> bool {
    if !equal((*a).clause as *const c_void, (*b).clause as *const c_void) { return false; }
    if (*a).is_pushed_down != (*b).is_pushed_down { return false; }
    if (*a).has_clone != (*b).has_clone { return false; }
    if (*a).is_clone != (*b).is_clone { return false; }
    if (*a).security_level != (*b).security_level { return false; }
    if !bms_equal((*a).required_relids, (*b).required_relids) { return false; }
    if !bms_equal((*a).incompatible_relids, (*b).incompatible_relids) { return false; }
    if !bms_equal((*a).outer_relids, (*b).outer_relids) { return false; }
    if (*a).rinfo_serial != (*b).rinfo_serial { return false; }
    true
}

unsafe fn _equalPlaceHolderVar(a: *const PlaceHolderVar, b: *const PlaceHolderVar) -> bool {
    if !bms_equal((*a).phnullingrels, (*b).phnullingrels) { return false; }
    if (*a).phid != (*b).phid { return false; }
    if (*a).phlevelsup != (*b).phlevelsup { return false; }
    true
}

unsafe fn _equalSpecialJoinInfo(a: *const SpecialJoinInfo, b: *const SpecialJoinInfo) -> bool {
    if !bms_equal((*a).min_lefthand, (*b).min_lefthand) { return false; }
    if !bms_equal((*a).min_righthand, (*b).min_righthand) { return false; }
    if !bms_equal((*a).syn_lefthand, (*b).syn_lefthand) { return false; }
    if !bms_equal((*a).syn_righthand, (*b).syn_righthand) { return false; }
    if (*a).jointype != (*b).jointype { return false; }
    if (*a).ojrelid != (*b).ojrelid { return false; }
    if !bms_equal((*a).commute_above_l, (*b).commute_above_l) { return false; }
    if !bms_equal((*a).commute_above_r, (*b).commute_above_r) { return false; }
    if !bms_equal((*a).commute_below_l, (*b).commute_below_l) { return false; }
    if !bms_equal((*a).commute_below_r, (*b).commute_below_r) { return false; }
    if (*a).lhs_strict != (*b).lhs_strict { return false; }
    if (*a).semi_can_btree != (*b).semi_can_btree { return false; }
    if (*a).semi_can_hash != (*b).semi_can_hash { return false; }
    if !equal((*a).semi_operators as *const c_void, (*b).semi_operators as *const c_void) { return false; }
    if !equal((*a).semi_rhs_exprs as *const c_void, (*b).semi_rhs_exprs as *const c_void) { return false; }
    true
}

unsafe fn _equalAppendRelInfo(a: *const AppendRelInfo, b: *const AppendRelInfo) -> bool {
    if (*a).parent_relid != (*b).parent_relid { return false; }
    if (*a).child_relid != (*b).child_relid { return false; }
    if (*a).parent_reltype != (*b).parent_reltype { return false; }
    if (*a).child_reltype != (*b).child_reltype { return false; }
    if !equal((*a).translated_vars as *const c_void, (*b).translated_vars as *const c_void) { return false; }
    if (*a).num_child_cols != (*b).num_child_cols { return false; }
    if libc::memcmp((*a).parent_colnos as *const c_void, (*b).parent_colnos as *const c_void, (*a).num_child_cols as usize * core::mem::size_of::<crate::access::attnum::AttrNumber>()) != 0 { return false; }
    if (*a).parent_reloid != (*b).parent_reloid { return false; }
    true
}

unsafe fn _equalPlaceHolderInfo(a: *const PlaceHolderInfo, b: *const PlaceHolderInfo) -> bool {
    if (*a).phid != (*b).phid { return false; }
    if !equal((*a).ph_var as *const c_void, (*b).ph_var as *const c_void) { return false; }
    if !bms_equal((*a).ph_eval_at, (*b).ph_eval_at) { return false; }
    if !bms_equal((*a).ph_lateral, (*b).ph_lateral) { return false; }
    if !bms_equal((*a).ph_needed, (*b).ph_needed) { return false; }
    if (*a).ph_width != (*b).ph_width { return false; }
    true
}

unsafe fn _equalInteger(a: *const Integer, b: *const Integer) -> bool {
    if (*a).ival != (*b).ival { return false; }
    true
}

unsafe fn _equalFloat(a: *const Float, b: *const Float) -> bool {
    if !equalstr((*a).fval, (*b).fval) { return false; }
    true
}

unsafe fn _equalBoolean(a: *const Boolean, b: *const Boolean) -> bool {
    if (*a).boolval != (*b).boolval { return false; }
    true
}

unsafe fn _equalString(a: *const String, b: *const String) -> bool {
    if !equalstr((*a).sval, (*b).sval) { return false; }
    true
}

unsafe fn _equalBitString(a: *const BitString, b: *const BitString) -> bool {
    if !equalstr((*a).bsval, (*b).bsval) { return false; }
    true
}

/*
 * equal
 *	returns whether two nodes are equal
 */
pub unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    let retval: bool;

    if a == b {
        return true;
    }

    if a.is_null() || b.is_null() {
        return false;
    }

    if nodeTag(a) != nodeTag(b) {
        return false;
    }

    check_stack_depth();

    match nodeTag(a) {
        NodeTag::T_List | NodeTag::T_IntList | NodeTag::T_OidList | NodeTag::T_XidList => {
            retval = _equalList(a as *const List, b as *const List);
        }
        NodeTag::T_Alias => { retval = _equalAlias(a as *const Alias, b as *const Alias); }
        NodeTag::T_RangeVar => { retval = _equalRangeVar(a as *const RangeVar, b as *const RangeVar); }
        NodeTag::T_TableFunc => { retval = _equalTableFunc(a as *const TableFunc, b as *const TableFunc); }
        NodeTag::T_IntoClause => { retval = _equalIntoClause(a as *const IntoClause, b as *const IntoClause); }
        NodeTag::T_Var => { retval = _equalVar(a as *const Var, b as *const Var); }
        NodeTag::T_Const => { retval = _equalConst(a as *const Const, b as *const Const); }
        NodeTag::T_Param => { retval = _equalParam(a as *const Param, b as *const Param); }
        NodeTag::T_Aggref => { retval = _equalAggref(a as *const Aggref, b as *const Aggref); }
        NodeTag::T_GroupingFunc => { retval = _equalGroupingFunc(a as *const GroupingFunc, b as *const GroupingFunc); }
        NodeTag::T_WindowFunc => { retval = _equalWindowFunc(a as *const WindowFunc, b as *const WindowFunc); }
        NodeTag::T_WindowFuncRunCondition => { retval = _equalWindowFuncRunCondition(a as *const WindowFuncRunCondition, b as *const WindowFuncRunCondition); }
        NodeTag::T_MergeSupportFunc => { retval = _equalMergeSupportFunc(a as *const MergeSupportFunc, b as *const MergeSupportFunc); }
        NodeTag::T_SubscriptingRef => { retval = _equalSubscriptingRef(a as *const SubscriptingRef, b as *const SubscriptingRef); }
        NodeTag::T_FuncExpr => { retval = _equalFuncExpr(a as *const FuncExpr, b as *const FuncExpr); }
        NodeTag::T_NamedArgExpr => { retval = _equalNamedArgExpr(a as *const NamedArgExpr, b as *const NamedArgExpr); }
        NodeTag::T_OpExpr => { retval = _equalOpExpr(a as *const OpExpr, b as *const OpExpr); }
        NodeTag::T_DistinctExpr => { retval = _equalDistinctExpr(a as *const DistinctExpr, b as *const DistinctExpr); }
        NodeTag::T_NullIfExpr => { retval = _equalNullIfExpr(a as *const NullIfExpr, b as *const NullIfExpr); }
        NodeTag::T_ScalarArrayOpExpr => { retval = _equalScalarArrayOpExpr(a as *const ScalarArrayOpExpr, b as *const ScalarArrayOpExpr); }
        NodeTag::T_BoolExpr => { retval = _equalBoolExpr(a as *const BoolExpr, b as *const BoolExpr); }
        NodeTag::T_SubLink => { retval = _equalSubLink(a as *const SubLink, b as *const SubLink); }
        NodeTag::T_SubPlan => { retval = _equalSubPlan(a as *const SubPlan, b as *const SubPlan); }
        NodeTag::T_AlternativeSubPlan => { retval = _equalAlternativeSubPlan(a as *const AlternativeSubPlan, b as *const AlternativeSubPlan); }
        NodeTag::T_FieldSelect => { retval = _equalFieldSelect(a as *const FieldSelect, b as *const FieldSelect); }
        NodeTag::T_FieldStore => { retval = _equalFieldStore(a as *const FieldStore, b as *const FieldStore); }
        NodeTag::T_RelabelType => { retval = _equalRelabelType(a as *const RelabelType, b as *const RelabelType); }
        NodeTag::T_CoerceViaIO => { retval = _equalCoerceViaIO(a as *const CoerceViaIO, b as *const CoerceViaIO); }
        NodeTag::T_ArrayCoerceExpr => { retval = _equalArrayCoerceExpr(a as *const ArrayCoerceExpr, b as *const ArrayCoerceExpr); }
        NodeTag::T_ConvertRowtypeExpr => { retval = _equalConvertRowtypeExpr(a as *const ConvertRowtypeExpr, b as *const ConvertRowtypeExpr); }
        NodeTag::T_CollateExpr => { retval = _equalCollateExpr(a as *const CollateExpr, b as *const CollateExpr); }
        NodeTag::T_CaseExpr => { retval = _equalCaseExpr(a as *const CaseExpr, b as *const CaseExpr); }
        NodeTag::T_CaseWhen => { retval = _equalCaseWhen(a as *const CaseWhen, b as *const CaseWhen); }
        NodeTag::T_CaseTestExpr => { retval = _equalCaseTestExpr(a as *const CaseTestExpr, b as *const CaseTestExpr); }
        NodeTag::T_ArrayExpr => { retval = _equalArrayExpr(a as *const ArrayExpr, b as *const ArrayExpr); }
        NodeTag::T_RowExpr => { retval = _equalRowExpr(a as *const RowExpr, b as *const RowExpr); }
        NodeTag::T_RowCompareExpr => { retval = _equalRowCompareExpr(a as *const RowCompareExpr, b as *const RowCompareExpr); }
        NodeTag::T_CoalesceExpr => { retval = _equalCoalesceExpr(a as *const CoalesceExpr, b as *const CoalesceExpr); }
        NodeTag::T_MinMaxExpr => { retval = _equalMinMaxExpr(a as *const MinMaxExpr, b as *const MinMaxExpr); }
        NodeTag::T_SQLValueFunction => { retval = _equalSQLValueFunction(a as *const SQLValueFunction, b as *const SQLValueFunction); }
        NodeTag::T_XmlExpr => { retval = _equalXmlExpr(a as *const XmlExpr, b as *const XmlExpr); }
        NodeTag::T_JsonFormat => { retval = _equalJsonFormat(a as *const JsonFormat, b as *const JsonFormat); }
        NodeTag::T_JsonReturning => { retval = _equalJsonReturning(a as *const JsonReturning, b as *const JsonReturning); }
        NodeTag::T_JsonValueExpr => { retval = _equalJsonValueExpr(a as *const JsonValueExpr, b as *const JsonValueExpr); }
        NodeTag::T_JsonConstructorExpr => { retval = _equalJsonConstructorExpr(a as *const JsonConstructorExpr, b as *const JsonConstructorExpr); }
        NodeTag::T_JsonIsPredicate => { retval = _equalJsonIsPredicate(a as *const JsonIsPredicate, b as *const JsonIsPredicate); }
        NodeTag::T_JsonBehavior => { retval = _equalJsonBehavior(a as *const JsonBehavior, b as *const JsonBehavior); }
        NodeTag::T_JsonExpr => { retval = _equalJsonExpr(a as *const JsonExpr, b as *const JsonExpr); }
        NodeTag::T_JsonTablePath => { retval = _equalJsonTablePath(a as *const JsonTablePath, b as *const JsonTablePath); }
        NodeTag::T_JsonTablePathScan => { retval = _equalJsonTablePathScan(a as *const JsonTablePathScan, b as *const JsonTablePathScan); }
        NodeTag::T_JsonTableSiblingJoin => { retval = _equalJsonTableSiblingJoin(a as *const JsonTableSiblingJoin, b as *const JsonTableSiblingJoin); }
        NodeTag::T_NullTest => { retval = _equalNullTest(a as *const NullTest, b as *const NullTest); }
        NodeTag::T_BooleanTest => { retval = _equalBooleanTest(a as *const BooleanTest, b as *const BooleanTest); }
        NodeTag::T_MergeAction => { retval = _equalMergeAction(a as *const MergeAction, b as *const MergeAction); }
        NodeTag::T_CoerceToDomain => { retval = _equalCoerceToDomain(a as *const CoerceToDomain, b as *const CoerceToDomain); }
        NodeTag::T_CoerceToDomainValue => { retval = _equalCoerceToDomainValue(a as *const CoerceToDomainValue, b as *const CoerceToDomainValue); }
        NodeTag::T_SetToDefault => { retval = _equalSetToDefault(a as *const SetToDefault, b as *const SetToDefault); }
        NodeTag::T_CurrentOfExpr => { retval = _equalCurrentOfExpr(a as *const CurrentOfExpr, b as *const CurrentOfExpr); }
        NodeTag::T_NextValueExpr => { retval = _equalNextValueExpr(a as *const NextValueExpr, b as *const NextValueExpr); }
        NodeTag::T_InferenceElem => { retval = _equalInferenceElem(a as *const InferenceElem, b as *const InferenceElem); }
        NodeTag::T_ReturningExpr => { retval = _equalReturningExpr(a as *const ReturningExpr, b as *const ReturningExpr); }
        NodeTag::T_TargetEntry => { retval = _equalTargetEntry(a as *const TargetEntry, b as *const TargetEntry); }
        NodeTag::T_RangeTblRef => { retval = _equalRangeTblRef(a as *const RangeTblRef, b as *const RangeTblRef); }
        NodeTag::T_JoinExpr => { retval = _equalJoinExpr(a as *const JoinExpr, b as *const JoinExpr); }
        NodeTag::T_FromExpr => { retval = _equalFromExpr(a as *const FromExpr, b as *const FromExpr); }
        NodeTag::T_OnConflictExpr => { retval = _equalOnConflictExpr(a as *const OnConflictExpr, b as *const OnConflictExpr); }
        NodeTag::T_Query => { retval = _equalQuery(a as *const Query, b as *const Query); }
        NodeTag::T_TypeName => { retval = _equalTypeName(a as *const TypeName, b as *const TypeName); }
        NodeTag::T_ColumnRef => { retval = _equalColumnRef(a as *const ColumnRef, b as *const ColumnRef); }
        NodeTag::T_ParamRef => { retval = _equalParamRef(a as *const ParamRef, b as *const ParamRef); }
        NodeTag::T_A_Expr => { retval = _equalA_Expr(a as *const A_Expr, b as *const A_Expr); }
        NodeTag::T_A_Const => { retval = _equalA_Const(a as *const A_Const, b as *const A_Const); }
        NodeTag::T_TypeCast => { retval = _equalTypeCast(a as *const TypeCast, b as *const TypeCast); }
        NodeTag::T_CollateClause => { retval = _equalCollateClause(a as *const CollateClause, b as *const CollateClause); }
        NodeTag::T_RoleSpec => { retval = _equalRoleSpec(a as *const RoleSpec, b as *const RoleSpec); }
        NodeTag::T_FuncCall => { retval = _equalFuncCall(a as *const FuncCall, b as *const FuncCall); }
        NodeTag::T_A_Star => { retval = _equalA_Star(a as *const A_Star, b as *const A_Star); }
        NodeTag::T_A_Indices => { retval = _equalA_Indices(a as *const A_Indices, b as *const A_Indices); }
        NodeTag::T_A_Indirection => { retval = _equalA_Indirection(a as *const A_Indirection, b as *const A_Indirection); }
        NodeTag::T_A_ArrayExpr => { retval = _equalA_ArrayExpr(a as *const A_ArrayExpr, b as *const A_ArrayExpr); }
        NodeTag::T_ResTarget => { retval = _equalResTarget(a as *const ResTarget, b as *const ResTarget); }
        NodeTag::T_MultiAssignRef => { retval = _equalMultiAssignRef(a as *const MultiAssignRef, b as *const MultiAssignRef); }
        NodeTag::T_SortBy => { retval = _equalSortBy(a as *const SortBy, b as *const SortBy); }
        NodeTag::T_WindowDef => { retval = _equalWindowDef(a as *const WindowDef, b as *const WindowDef); }
        NodeTag::T_RangeSubselect => { retval = _equalRangeSubselect(a as *const RangeSubselect, b as *const RangeSubselect); }
        NodeTag::T_RangeFunction => { retval = _equalRangeFunction(a as *const RangeFunction, b as *const RangeFunction); }
        NodeTag::T_RangeTableFunc => { retval = _equalRangeTableFunc(a as *const RangeTableFunc, b as *const RangeTableFunc); }
        NodeTag::T_RangeTableFuncCol => { retval = _equalRangeTableFuncCol(a as *const RangeTableFuncCol, b as *const RangeTableFuncCol); }
        NodeTag::T_RangeTableSample => { retval = _equalRangeTableSample(a as *const RangeTableSample, b as *const RangeTableSample); }
        NodeTag::T_ColumnDef => { retval = _equalColumnDef(a as *const ColumnDef, b as *const ColumnDef); }
        NodeTag::T_TableLikeClause => { retval = _equalTableLikeClause(a as *const TableLikeClause, b as *const TableLikeClause); }
        NodeTag::T_IndexElem => { retval = _equalIndexElem(a as *const IndexElem, b as *const IndexElem); }
        NodeTag::T_DefElem => { retval = _equalDefElem(a as *const DefElem, b as *const DefElem); }
        NodeTag::T_LockingClause => { retval = _equalLockingClause(a as *const LockingClause, b as *const LockingClause); }
        NodeTag::T_XmlSerialize => { retval = _equalXmlSerialize(a as *const XmlSerialize, b as *const XmlSerialize); }
        NodeTag::T_PartitionElem => { retval = _equalPartitionElem(a as *const PartitionElem, b as *const PartitionElem); }
        NodeTag::T_PartitionSpec => { retval = _equalPartitionSpec(a as *const PartitionSpec, b as *const PartitionSpec); }
        NodeTag::T_PartitionBoundSpec => { retval = _equalPartitionBoundSpec(a as *const PartitionBoundSpec, b as *const PartitionBoundSpec); }
        NodeTag::T_PartitionRangeDatum => { retval = _equalPartitionRangeDatum(a as *const PartitionRangeDatum, b as *const PartitionRangeDatum); }
        NodeTag::T_PartitionCmd => { retval = _equalPartitionCmd(a as *const PartitionCmd, b as *const PartitionCmd); }
        NodeTag::T_RangeTblEntry => { retval = _equalRangeTblEntry(a as *const RangeTblEntry, b as *const RangeTblEntry); }
        NodeTag::T_RTEPermissionInfo => { retval = _equalRTEPermissionInfo(a as *const RTEPermissionInfo, b as *const RTEPermissionInfo); }
        NodeTag::T_RangeTblFunction => { retval = _equalRangeTblFunction(a as *const RangeTblFunction, b as *const RangeTblFunction); }
        NodeTag::T_TableSampleClause => { retval = _equalTableSampleClause(a as *const TableSampleClause, b as *const TableSampleClause); }
        NodeTag::T_WithCheckOption => { retval = _equalWithCheckOption(a as *const WithCheckOption, b as *const WithCheckOption); }
        NodeTag::T_SortGroupClause => { retval = _equalSortGroupClause(a as *const SortGroupClause, b as *const SortGroupClause); }
        NodeTag::T_GroupingSet => { retval = _equalGroupingSet(a as *const GroupingSet, b as *const GroupingSet); }
        NodeTag::T_WindowClause => { retval = _equalWindowClause(a as *const WindowClause, b as *const WindowClause); }
        NodeTag::T_RowMarkClause => { retval = _equalRowMarkClause(a as *const RowMarkClause, b as *const RowMarkClause); }
        NodeTag::T_WithClause => { retval = _equalWithClause(a as *const WithClause, b as *const WithClause); }
        NodeTag::T_InferClause => { retval = _equalInferClause(a as *const InferClause, b as *const InferClause); }
        NodeTag::T_OnConflictClause => { retval = _equalOnConflictClause(a as *const OnConflictClause, b as *const OnConflictClause); }
        NodeTag::T_CTESearchClause => { retval = _equalCTESearchClause(a as *const CTESearchClause, b as *const CTESearchClause); }
        NodeTag::T_CTECycleClause => { retval = _equalCTECycleClause(a as *const CTECycleClause, b as *const CTECycleClause); }
        NodeTag::T_CommonTableExpr => { retval = _equalCommonTableExpr(a as *const CommonTableExpr, b as *const CommonTableExpr); }
        NodeTag::T_MergeWhenClause => { retval = _equalMergeWhenClause(a as *const MergeWhenClause, b as *const MergeWhenClause); }
        NodeTag::T_ReturningOption => { retval = _equalReturningOption(a as *const ReturningOption, b as *const ReturningOption); }
        NodeTag::T_ReturningClause => { retval = _equalReturningClause(a as *const ReturningClause, b as *const ReturningClause); }
        NodeTag::T_TriggerTransition => { retval = _equalTriggerTransition(a as *const TriggerTransition, b as *const TriggerTransition); }
        NodeTag::T_JsonOutput => { retval = _equalJsonOutput(a as *const JsonOutput, b as *const JsonOutput); }
        NodeTag::T_JsonArgument => { retval = _equalJsonArgument(a as *const JsonArgument, b as *const JsonArgument); }
        NodeTag::T_JsonFuncExpr => { retval = _equalJsonFuncExpr(a as *const JsonFuncExpr, b as *const JsonFuncExpr); }
        NodeTag::T_JsonTablePathSpec => { retval = _equalJsonTablePathSpec(a as *const JsonTablePathSpec, b as *const JsonTablePathSpec); }
        NodeTag::T_JsonTable => { retval = _equalJsonTable(a as *const JsonTable, b as *const JsonTable); }
        NodeTag::T_JsonTableColumn => { retval = _equalJsonTableColumn(a as *const JsonTableColumn, b as *const JsonTableColumn); }
        NodeTag::T_JsonKeyValue => { retval = _equalJsonKeyValue(a as *const JsonKeyValue, b as *const JsonKeyValue); }
        NodeTag::T_JsonParseExpr => { retval = _equalJsonParseExpr(a as *const JsonParseExpr, b as *const JsonParseExpr); }
        NodeTag::T_JsonScalarExpr => { retval = _equalJsonScalarExpr(a as *const JsonScalarExpr, b as *const JsonScalarExpr); }
        NodeTag::T_JsonSerializeExpr => { retval = _equalJsonSerializeExpr(a as *const JsonSerializeExpr, b as *const JsonSerializeExpr); }
        NodeTag::T_JsonObjectConstructor => { retval = _equalJsonObjectConstructor(a as *const JsonObjectConstructor, b as *const JsonObjectConstructor); }
        NodeTag::T_JsonArrayConstructor => { retval = _equalJsonArrayConstructor(a as *const JsonArrayConstructor, b as *const JsonArrayConstructor); }
        NodeTag::T_JsonArrayQueryConstructor => { retval = _equalJsonArrayQueryConstructor(a as *const JsonArrayQueryConstructor, b as *const JsonArrayQueryConstructor); }
        NodeTag::T_JsonAggConstructor => { retval = _equalJsonAggConstructor(a as *const JsonAggConstructor, b as *const JsonAggConstructor); }
        NodeTag::T_JsonObjectAgg => { retval = _equalJsonObjectAgg(a as *const JsonObjectAgg, b as *const JsonObjectAgg); }
        NodeTag::T_JsonArrayAgg => { retval = _equalJsonArrayAgg(a as *const JsonArrayAgg, b as *const JsonArrayAgg); }
        NodeTag::T_RawStmt => { retval = _equalRawStmt(a as *const RawStmt, b as *const RawStmt); }
        NodeTag::T_InsertStmt => { retval = _equalInsertStmt(a as *const InsertStmt, b as *const InsertStmt); }
        NodeTag::T_DeleteStmt => { retval = _equalDeleteStmt(a as *const DeleteStmt, b as *const DeleteStmt); }
        NodeTag::T_UpdateStmt => { retval = _equalUpdateStmt(a as *const UpdateStmt, b as *const UpdateStmt); }
        NodeTag::T_MergeStmt => { retval = _equalMergeStmt(a as *const MergeStmt, b as *const MergeStmt); }
        NodeTag::T_SelectStmt => { retval = _equalSelectStmt(a as *const SelectStmt, b as *const SelectStmt); }
        NodeTag::T_SetOperationStmt => { retval = _equalSetOperationStmt(a as *const SetOperationStmt, b as *const SetOperationStmt); }
        NodeTag::T_ReturnStmt => { retval = _equalReturnStmt(a as *const ReturnStmt, b as *const ReturnStmt); }
        NodeTag::T_PLAssignStmt => { retval = _equalPLAssignStmt(a as *const PLAssignStmt, b as *const PLAssignStmt); }
        NodeTag::T_CreateSchemaStmt => { retval = _equalCreateSchemaStmt(a as *const CreateSchemaStmt, b as *const CreateSchemaStmt); }
        NodeTag::T_AlterTableStmt => { retval = _equalAlterTableStmt(a as *const AlterTableStmt, b as *const AlterTableStmt); }
        NodeTag::T_AlterTableCmd => { retval = _equalAlterTableCmd(a as *const AlterTableCmd, b as *const AlterTableCmd); }
        NodeTag::T_ATAlterConstraint => { retval = _equalATAlterConstraint(a as *const ATAlterConstraint, b as *const ATAlterConstraint); }
        NodeTag::T_ReplicaIdentityStmt => { retval = _equalReplicaIdentityStmt(a as *const ReplicaIdentityStmt, b as *const ReplicaIdentityStmt); }
        NodeTag::T_AlterCollationStmt => { retval = _equalAlterCollationStmt(a as *const AlterCollationStmt, b as *const AlterCollationStmt); }
        NodeTag::T_AlterDomainStmt => { retval = _equalAlterDomainStmt(a as *const AlterDomainStmt, b as *const AlterDomainStmt); }
        NodeTag::T_GrantStmt => { retval = _equalGrantStmt(a as *const GrantStmt, b as *const GrantStmt); }
        NodeTag::T_ObjectWithArgs => { retval = _equalObjectWithArgs(a as *const ObjectWithArgs, b as *const ObjectWithArgs); }
        NodeTag::T_AccessPriv => { retval = _equalAccessPriv(a as *const AccessPriv, b as *const AccessPriv); }
        NodeTag::T_GrantRoleStmt => { retval = _equalGrantRoleStmt(a as *const GrantRoleStmt, b as *const GrantRoleStmt); }
        NodeTag::T_AlterDefaultPrivilegesStmt => { retval = _equalAlterDefaultPrivilegesStmt(a as *const AlterDefaultPrivilegesStmt, b as *const AlterDefaultPrivilegesStmt); }
        NodeTag::T_CopyStmt => { retval = _equalCopyStmt(a as *const CopyStmt, b as *const CopyStmt); }
        NodeTag::T_VariableSetStmt => { retval = _equalVariableSetStmt(a as *const VariableSetStmt, b as *const VariableSetStmt); }
        NodeTag::T_VariableShowStmt => { retval = _equalVariableShowStmt(a as *const VariableShowStmt, b as *const VariableShowStmt); }
        NodeTag::T_CreateStmt => { retval = _equalCreateStmt(a as *const CreateStmt, b as *const CreateStmt); }
        NodeTag::T_Constraint => { retval = _equalConstraint(a as *const Constraint, b as *const Constraint); }
        NodeTag::T_CreateTableSpaceStmt => { retval = _equalCreateTableSpaceStmt(a as *const CreateTableSpaceStmt, b as *const CreateTableSpaceStmt); }
        NodeTag::T_DropTableSpaceStmt => { retval = _equalDropTableSpaceStmt(a as *const DropTableSpaceStmt, b as *const DropTableSpaceStmt); }
        NodeTag::T_AlterTableSpaceOptionsStmt => { retval = _equalAlterTableSpaceOptionsStmt(a as *const AlterTableSpaceOptionsStmt, b as *const AlterTableSpaceOptionsStmt); }
        NodeTag::T_AlterTableMoveAllStmt => { retval = _equalAlterTableMoveAllStmt(a as *const AlterTableMoveAllStmt, b as *const AlterTableMoveAllStmt); }
        NodeTag::T_CreateExtensionStmt => { retval = _equalCreateExtensionStmt(a as *const CreateExtensionStmt, b as *const CreateExtensionStmt); }
        NodeTag::T_AlterExtensionStmt => { retval = _equalAlterExtensionStmt(a as *const AlterExtensionStmt, b as *const AlterExtensionStmt); }
        NodeTag::T_AlterExtensionContentsStmt => { retval = _equalAlterExtensionContentsStmt(a as *const AlterExtensionContentsStmt, b as *const AlterExtensionContentsStmt); }
        NodeTag::T_CreateFdwStmt => { retval = _equalCreateFdwStmt(a as *const CreateFdwStmt, b as *const CreateFdwStmt); }
        NodeTag::T_AlterFdwStmt => { retval = _equalAlterFdwStmt(a as *const AlterFdwStmt, b as *const AlterFdwStmt); }
        NodeTag::T_CreateForeignServerStmt => { retval = _equalCreateForeignServerStmt(a as *const CreateForeignServerStmt, b as *const CreateForeignServerStmt); }
        NodeTag::T_AlterForeignServerStmt => { retval = _equalAlterForeignServerStmt(a as *const AlterForeignServerStmt, b as *const AlterForeignServerStmt); }
        NodeTag::T_CreateForeignTableStmt => { retval = _equalCreateForeignTableStmt(a as *const CreateForeignTableStmt, b as *const CreateForeignTableStmt); }
        NodeTag::T_CreateUserMappingStmt => { retval = _equalCreateUserMappingStmt(a as *const CreateUserMappingStmt, b as *const CreateUserMappingStmt); }
        NodeTag::T_AlterUserMappingStmt => { retval = _equalAlterUserMappingStmt(a as *const AlterUserMappingStmt, b as *const AlterUserMappingStmt); }
        NodeTag::T_DropUserMappingStmt => { retval = _equalDropUserMappingStmt(a as *const DropUserMappingStmt, b as *const DropUserMappingStmt); }
        NodeTag::T_ImportForeignSchemaStmt => { retval = _equalImportForeignSchemaStmt(a as *const ImportForeignSchemaStmt, b as *const ImportForeignSchemaStmt); }
        NodeTag::T_CreatePolicyStmt => { retval = _equalCreatePolicyStmt(a as *const CreatePolicyStmt, b as *const CreatePolicyStmt); }
        NodeTag::T_AlterPolicyStmt => { retval = _equalAlterPolicyStmt(a as *const AlterPolicyStmt, b as *const AlterPolicyStmt); }
        NodeTag::T_CreateAmStmt => { retval = _equalCreateAmStmt(a as *const CreateAmStmt, b as *const CreateAmStmt); }
        NodeTag::T_CreateTrigStmt => { retval = _equalCreateTrigStmt(a as *const CreateTrigStmt, b as *const CreateTrigStmt); }
        NodeTag::T_CreateEventTrigStmt => { retval = _equalCreateEventTrigStmt(a as *const CreateEventTrigStmt, b as *const CreateEventTrigStmt); }
        NodeTag::T_AlterEventTrigStmt => { retval = _equalAlterEventTrigStmt(a as *const AlterEventTrigStmt, b as *const AlterEventTrigStmt); }
        NodeTag::T_CreatePLangStmt => { retval = _equalCreatePLangStmt(a as *const CreatePLangStmt, b as *const CreatePLangStmt); }
        NodeTag::T_CreateRoleStmt => { retval = _equalCreateRoleStmt(a as *const CreateRoleStmt, b as *const CreateRoleStmt); }
        NodeTag::T_AlterRoleStmt => { retval = _equalAlterRoleStmt(a as *const AlterRoleStmt, b as *const AlterRoleStmt); }
        NodeTag::T_AlterRoleSetStmt => { retval = _equalAlterRoleSetStmt(a as *const AlterRoleSetStmt, b as *const AlterRoleSetStmt); }
        NodeTag::T_DropRoleStmt => { retval = _equalDropRoleStmt(a as *const DropRoleStmt, b as *const DropRoleStmt); }
        NodeTag::T_CreateSeqStmt => { retval = _equalCreateSeqStmt(a as *const CreateSeqStmt, b as *const CreateSeqStmt); }
        NodeTag::T_AlterSeqStmt => { retval = _equalAlterSeqStmt(a as *const AlterSeqStmt, b as *const AlterSeqStmt); }
        NodeTag::T_DefineStmt => { retval = _equalDefineStmt(a as *const DefineStmt, b as *const DefineStmt); }
        NodeTag::T_CreateDomainStmt => { retval = _equalCreateDomainStmt(a as *const CreateDomainStmt, b as *const CreateDomainStmt); }
        NodeTag::T_CreateOpClassStmt => { retval = _equalCreateOpClassStmt(a as *const CreateOpClassStmt, b as *const CreateOpClassStmt); }
        NodeTag::T_CreateOpClassItem => { retval = _equalCreateOpClassItem(a as *const CreateOpClassItem, b as *const CreateOpClassItem); }
        NodeTag::T_CreateOpFamilyStmt => { retval = _equalCreateOpFamilyStmt(a as *const CreateOpFamilyStmt, b as *const CreateOpFamilyStmt); }
        NodeTag::T_AlterOpFamilyStmt => { retval = _equalAlterOpFamilyStmt(a as *const AlterOpFamilyStmt, b as *const AlterOpFamilyStmt); }
        NodeTag::T_DropStmt => { retval = _equalDropStmt(a as *const DropStmt, b as *const DropStmt); }
        NodeTag::T_TruncateStmt => { retval = _equalTruncateStmt(a as *const TruncateStmt, b as *const TruncateStmt); }
        NodeTag::T_CommentStmt => { retval = _equalCommentStmt(a as *const CommentStmt, b as *const CommentStmt); }
        NodeTag::T_SecLabelStmt => { retval = _equalSecLabelStmt(a as *const SecLabelStmt, b as *const SecLabelStmt); }
        NodeTag::T_DeclareCursorStmt => { retval = _equalDeclareCursorStmt(a as *const DeclareCursorStmt, b as *const DeclareCursorStmt); }
        NodeTag::T_ClosePortalStmt => { retval = _equalClosePortalStmt(a as *const ClosePortalStmt, b as *const ClosePortalStmt); }
        NodeTag::T_FetchStmt => { retval = _equalFetchStmt(a as *const FetchStmt, b as *const FetchStmt); }
        NodeTag::T_IndexStmt => { retval = _equalIndexStmt(a as *const IndexStmt, b as *const IndexStmt); }
        NodeTag::T_CreateStatsStmt => { retval = _equalCreateStatsStmt(a as *const CreateStatsStmt, b as *const CreateStatsStmt); }
        NodeTag::T_StatsElem => { retval = _equalStatsElem(a as *const StatsElem, b as *const StatsElem); }
        NodeTag::T_AlterStatsStmt => { retval = _equalAlterStatsStmt(a as *const AlterStatsStmt, b as *const AlterStatsStmt); }
        NodeTag::T_CreateFunctionStmt => { retval = _equalCreateFunctionStmt(a as *const CreateFunctionStmt, b as *const CreateFunctionStmt); }
        NodeTag::T_FunctionParameter => { retval = _equalFunctionParameter(a as *const FunctionParameter, b as *const FunctionParameter); }
        NodeTag::T_AlterFunctionStmt => { retval = _equalAlterFunctionStmt(a as *const AlterFunctionStmt, b as *const AlterFunctionStmt); }
        NodeTag::T_DoStmt => { retval = _equalDoStmt(a as *const DoStmt, b as *const DoStmt); }
        NodeTag::T_CallStmt => { retval = _equalCallStmt(a as *const CallStmt, b as *const CallStmt); }
        NodeTag::T_RenameStmt => { retval = _equalRenameStmt(a as *const RenameStmt, b as *const RenameStmt); }
        NodeTag::T_AlterObjectDependsStmt => { retval = _equalAlterObjectDependsStmt(a as *const AlterObjectDependsStmt, b as *const AlterObjectDependsStmt); }
        NodeTag::T_AlterObjectSchemaStmt => { retval = _equalAlterObjectSchemaStmt(a as *const AlterObjectSchemaStmt, b as *const AlterObjectSchemaStmt); }
        NodeTag::T_AlterOwnerStmt => { retval = _equalAlterOwnerStmt(a as *const AlterOwnerStmt, b as *const AlterOwnerStmt); }
        NodeTag::T_AlterOperatorStmt => { retval = _equalAlterOperatorStmt(a as *const AlterOperatorStmt, b as *const AlterOperatorStmt); }
        NodeTag::T_AlterTypeStmt => { retval = _equalAlterTypeStmt(a as *const AlterTypeStmt, b as *const AlterTypeStmt); }
        NodeTag::T_RuleStmt => { retval = _equalRuleStmt(a as *const RuleStmt, b as *const RuleStmt); }
        NodeTag::T_NotifyStmt => { retval = _equalNotifyStmt(a as *const NotifyStmt, b as *const NotifyStmt); }
        NodeTag::T_ListenStmt => { retval = _equalListenStmt(a as *const ListenStmt, b as *const ListenStmt); }
        NodeTag::T_UnlistenStmt => { retval = _equalUnlistenStmt(a as *const UnlistenStmt, b as *const UnlistenStmt); }
        NodeTag::T_TransactionStmt => { retval = _equalTransactionStmt(a as *const TransactionStmt, b as *const TransactionStmt); }
        NodeTag::T_CompositeTypeStmt => { retval = _equalCompositeTypeStmt(a as *const CompositeTypeStmt, b as *const CompositeTypeStmt); }
        NodeTag::T_CreateEnumStmt => { retval = _equalCreateEnumStmt(a as *const CreateEnumStmt, b as *const CreateEnumStmt); }
        NodeTag::T_CreateRangeStmt => { retval = _equalCreateRangeStmt(a as *const CreateRangeStmt, b as *const CreateRangeStmt); }
        NodeTag::T_AlterEnumStmt => { retval = _equalAlterEnumStmt(a as *const AlterEnumStmt, b as *const AlterEnumStmt); }
        NodeTag::T_ViewStmt => { retval = _equalViewStmt(a as *const ViewStmt, b as *const ViewStmt); }
        NodeTag::T_LoadStmt => { retval = _equalLoadStmt(a as *const LoadStmt, b as *const LoadStmt); }
        NodeTag::T_CreatedbStmt => { retval = _equalCreatedbStmt(a as *const CreatedbStmt, b as *const CreatedbStmt); }
        NodeTag::T_AlterDatabaseStmt => { retval = _equalAlterDatabaseStmt(a as *const AlterDatabaseStmt, b as *const AlterDatabaseStmt); }
        NodeTag::T_AlterDatabaseRefreshCollStmt => { retval = _equalAlterDatabaseRefreshCollStmt(a as *const AlterDatabaseRefreshCollStmt, b as *const AlterDatabaseRefreshCollStmt); }
        NodeTag::T_AlterDatabaseSetStmt => { retval = _equalAlterDatabaseSetStmt(a as *const AlterDatabaseSetStmt, b as *const AlterDatabaseSetStmt); }
        NodeTag::T_DropdbStmt => { retval = _equalDropdbStmt(a as *const DropdbStmt, b as *const DropdbStmt); }
        NodeTag::T_AlterSystemStmt => { retval = _equalAlterSystemStmt(a as *const AlterSystemStmt, b as *const AlterSystemStmt); }
        NodeTag::T_ClusterStmt => { retval = _equalClusterStmt(a as *const ClusterStmt, b as *const ClusterStmt); }
        NodeTag::T_VacuumStmt => { retval = _equalVacuumStmt(a as *const VacuumStmt, b as *const VacuumStmt); }
        NodeTag::T_VacuumRelation => { retval = _equalVacuumRelation(a as *const VacuumRelation, b as *const VacuumRelation); }
        NodeTag::T_ExplainStmt => { retval = _equalExplainStmt(a as *const ExplainStmt, b as *const ExplainStmt); }
        NodeTag::T_CreateTableAsStmt => { retval = _equalCreateTableAsStmt(a as *const CreateTableAsStmt, b as *const CreateTableAsStmt); }
        NodeTag::T_RefreshMatViewStmt => { retval = _equalRefreshMatViewStmt(a as *const RefreshMatViewStmt, b as *const RefreshMatViewStmt); }
        NodeTag::T_CheckPointStmt => { retval = _equalCheckPointStmt(a as *const CheckPointStmt, b as *const CheckPointStmt); }
        NodeTag::T_DiscardStmt => { retval = _equalDiscardStmt(a as *const DiscardStmt, b as *const DiscardStmt); }
        NodeTag::T_LockStmt => { retval = _equalLockStmt(a as *const LockStmt, b as *const LockStmt); }
        NodeTag::T_ConstraintsSetStmt => { retval = _equalConstraintsSetStmt(a as *const ConstraintsSetStmt, b as *const ConstraintsSetStmt); }
        NodeTag::T_ReindexStmt => { retval = _equalReindexStmt(a as *const ReindexStmt, b as *const ReindexStmt); }
        NodeTag::T_CreateConversionStmt => { retval = _equalCreateConversionStmt(a as *const CreateConversionStmt, b as *const CreateConversionStmt); }
        NodeTag::T_CreateCastStmt => { retval = _equalCreateCastStmt(a as *const CreateCastStmt, b as *const CreateCastStmt); }
        NodeTag::T_CreateTransformStmt => { retval = _equalCreateTransformStmt(a as *const CreateTransformStmt, b as *const CreateTransformStmt); }
        NodeTag::T_PrepareStmt => { retval = _equalPrepareStmt(a as *const PrepareStmt, b as *const PrepareStmt); }
        NodeTag::T_ExecuteStmt => { retval = _equalExecuteStmt(a as *const ExecuteStmt, b as *const ExecuteStmt); }
        NodeTag::T_DeallocateStmt => { retval = _equalDeallocateStmt(a as *const DeallocateStmt, b as *const DeallocateStmt); }
        NodeTag::T_DropOwnedStmt => { retval = _equalDropOwnedStmt(a as *const DropOwnedStmt, b as *const DropOwnedStmt); }
        NodeTag::T_ReassignOwnedStmt => { retval = _equalReassignOwnedStmt(a as *const ReassignOwnedStmt, b as *const ReassignOwnedStmt); }
        NodeTag::T_AlterTSDictionaryStmt => { retval = _equalAlterTSDictionaryStmt(a as *const AlterTSDictionaryStmt, b as *const AlterTSDictionaryStmt); }
        NodeTag::T_AlterTSConfigurationStmt => { retval = _equalAlterTSConfigurationStmt(a as *const AlterTSConfigurationStmt, b as *const AlterTSConfigurationStmt); }
        NodeTag::T_PublicationTable => { retval = _equalPublicationTable(a as *const PublicationTable, b as *const PublicationTable); }
        NodeTag::T_PublicationObjSpec => { retval = _equalPublicationObjSpec(a as *const PublicationObjSpec, b as *const PublicationObjSpec); }
        NodeTag::T_CreatePublicationStmt => { retval = _equalCreatePublicationStmt(a as *const CreatePublicationStmt, b as *const CreatePublicationStmt); }
        NodeTag::T_AlterPublicationStmt => { retval = _equalAlterPublicationStmt(a as *const AlterPublicationStmt, b as *const AlterPublicationStmt); }
        NodeTag::T_CreateSubscriptionStmt => { retval = _equalCreateSubscriptionStmt(a as *const CreateSubscriptionStmt, b as *const CreateSubscriptionStmt); }
        NodeTag::T_AlterSubscriptionStmt => { retval = _equalAlterSubscriptionStmt(a as *const AlterSubscriptionStmt, b as *const AlterSubscriptionStmt); }
        NodeTag::T_DropSubscriptionStmt => { retval = _equalDropSubscriptionStmt(a as *const DropSubscriptionStmt, b as *const DropSubscriptionStmt); }
        NodeTag::T_PathKey => { retval = _equalPathKey(a as *const PathKey, b as *const PathKey); }
        NodeTag::T_GroupByOrdering => { retval = _equalGroupByOrdering(a as *const GroupByOrdering, b as *const GroupByOrdering); }
        NodeTag::T_RestrictInfo => { retval = _equalRestrictInfo(a as *const RestrictInfo, b as *const RestrictInfo); }
        NodeTag::T_PlaceHolderVar => { retval = _equalPlaceHolderVar(a as *const PlaceHolderVar, b as *const PlaceHolderVar); }
        NodeTag::T_SpecialJoinInfo => { retval = _equalSpecialJoinInfo(a as *const SpecialJoinInfo, b as *const SpecialJoinInfo); }
        NodeTag::T_AppendRelInfo => { retval = _equalAppendRelInfo(a as *const AppendRelInfo, b as *const AppendRelInfo); }
        NodeTag::T_PlaceHolderInfo => { retval = _equalPlaceHolderInfo(a as *const PlaceHolderInfo, b as *const PlaceHolderInfo); }
        NodeTag::T_Bitmapset => { retval = _equalBitmapset(a as *const Bitmapset, b as *const Bitmapset); }
        NodeTag::T_ExtensibleNode => { retval = _equalExtensibleNode(a as *const ExtensibleNode, b as *const ExtensibleNode); }
        NodeTag::T_Integer => { retval = _equalInteger(a as *const Integer, b as *const Integer); }
        NodeTag::T_Float => { retval = _equalFloat(a as *const Float, b as *const Float); }
        NodeTag::T_Boolean => { retval = _equalBoolean(a as *const Boolean, b as *const Boolean); }
        NodeTag::T_String => { retval = _equalString(a as *const String, b as *const String); }
        NodeTag::T_BitString => { retval = _equalBitString(a as *const BitString, b as *const BitString); }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(a) as c_int);
            retval = false;
        }
    }

    retval
}
