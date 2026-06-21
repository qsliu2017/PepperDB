//! nodes/copyfuncs.c - Copy functions for Postgres tree nodes.
// ManuallyDrop union fields (Value node) require Deref on raw-pointer-derived
// places; the access is sound here (single-threaded, valid nodes).
#![allow(dangerous_implicit_autorefs)]

use crate::prelude::*;

use core::ops::DerefMut;
use crate::miscadmin::check_stack_depth;
use crate::nodes::bitmapset::{bms_copy, Bitmapset};
use crate::nodes::extensible::{ExtensibleNode, ExtensibleNodeMethods, GetExtensibleNodeMethods};
use crate::nodes::nodes::{nodeTag, newNode, NodeTag, ParseLoc};
use crate::nodes::parsenodes::{A_Const, Query, RangeTblEntry, RTEPermissionInfo};
use crate::nodes::pg_list::{list_copy, list_copy_deep, List};
use crate::nodes::primnodes::{
    Alias, Const, FromExpr, FuncExpr, OpExpr, RangeTblRef, TargetEntry, Var,
};
use crate::utils::adt::datum::datumCopy;
use crate::{elog, makeNode};

use std::ffi::{c_char, c_int, c_void};

/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 *
 * In Rust these are expressed inline because the C macros depend on textual
 * substitution of 'newnode' and 'from'.
 */

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#[inline]
unsafe fn copy_string_field(from: *const c_char) -> *mut c_char {
    if !from.is_null() {
        pstrdup(from)
    } else {
        std::ptr::null_mut()
    }
}

/*
 * Support functions for nodes with custom_copy_equal attribute
 */

unsafe fn _copyConst(from: *const Const) -> *mut Const {
    let newnode: *mut Const = makeNode!(Const, T_Const);

    (*newnode).consttype = (*from).consttype;
    (*newnode).consttypmod = (*from).consttypmod;
    (*newnode).constcollid = (*from).constcollid;
    (*newnode).constlen = (*from).constlen;

    if (*from).constbyval || (*from).constisnull {
        /*
         * passed by value so just copy the datum. Also, don't try to copy
         * struct when value is null!
         */
        (*newnode).constvalue = (*from).constvalue;
    } else {
        /*
         * passed by reference.  We need a palloc'd copy.
         */
        (*newnode).constvalue = datumCopy(
            (*from).constvalue,
            (*from).constbyval,
            (*from).constlen,
        );
    }

    (*newnode).constisnull = (*from).constisnull;
    (*newnode).constbyval = (*from).constbyval;
    (*newnode).location = (*from).location;

    newnode
}

unsafe fn _copyA_Const(from: *const A_Const) -> *mut A_Const {
    let newnode: *mut A_Const = makeNode!(A_Const, T_A_Const);

    (*newnode).isnull = (*from).isnull;
    if !(*from).isnull {
        /* This part must duplicate other _copy*() functions. */
        *(&raw mut (*newnode).val as *mut NodeTag) = (*from).val.node.r#type;
        match nodeTag(&(*from).val as *const _ as *const NodeTag) {
            NodeTag::T_Integer => {
                (*(&raw mut (*newnode).val.ival as *mut crate::nodes::value::Integer)).ival = (*from).val.ival.ival;
            }
            NodeTag::T_Float => {
                (*(&raw mut (*newnode).val.fval as *mut crate::nodes::value::Float)).fval = copy_string_field((*from).val.fval.fval);
            }
            NodeTag::T_Boolean => {
                (*(&raw mut (*newnode).val.boolval as *mut crate::nodes::value::Boolean)).boolval = (*from).val.boolval.boolval;
            }
            NodeTag::T_String => {
                (*(&raw mut (*newnode).val.sval as *mut crate::nodes::value::String)).sval = copy_string_field((*from).val.sval.sval);
            }
            NodeTag::T_BitString => {
                (*(&raw mut (*newnode).val.bsval as *mut crate::nodes::value::BitString)).bsval = copy_string_field((*from).val.bsval.bsval);
            }
            _ => {
                elog!(
                    ERROR,
                    "unrecognized node type: {}",
                    nodeTag(&(*from).val as *const _ as *const NodeTag) as c_int
                );
            }
        }
    }

    (*newnode).location = (*from).location;

    newnode
}

unsafe fn _copyExtensibleNode(from: *const ExtensibleNode) -> *mut ExtensibleNode {
    let methods: *const ExtensibleNodeMethods =
        GetExtensibleNodeMethods((*from).extnodename, false);
    let newnode: *mut ExtensibleNode =
        newNode((*methods).node_size, NodeTag::T_ExtensibleNode) as *mut ExtensibleNode;
    (*newnode).extnodename = copy_string_field((*from).extnodename);

    /* copy the private fields */
    ((*methods).nodeCopy.unwrap())(newnode, from);

    newnode
}

unsafe fn _copyBitmapset(from: *const Bitmapset) -> *mut Bitmapset {
    bms_copy(from)
}

unsafe fn _copyAlias(from: *const Alias) -> *mut Alias {
    let newnode: *mut Alias = makeNode!(Alias, T_Alias);

    (*newnode).aliasname = copy_string_field((*from).aliasname);
    (*newnode).colnames = copyObjectImpl((*from).colnames as *const c_void) as _;

    newnode
}

unsafe fn _copyVar(from: *const Var) -> *mut Var {
    let newnode: *mut Var = makeNode!(Var, T_Var);

    (*newnode).varno = (*from).varno;
    (*newnode).varattno = (*from).varattno;
    (*newnode).vartype = (*from).vartype;
    (*newnode).vartypmod = (*from).vartypmod;
    (*newnode).varcollid = (*from).varcollid;
    (*newnode).varnullingrels = bms_copy((*from).varnullingrels);
    (*newnode).varlevelsup = (*from).varlevelsup;
    (*newnode).varreturningtype = (*from).varreturningtype;
    (*newnode).varnosyn = (*from).varnosyn;
    (*newnode).varattnosyn = (*from).varattnosyn;
    (*newnode).location = (*from).location;

    newnode
}

unsafe fn _copyFuncExpr(from: *const FuncExpr) -> *mut FuncExpr {
    let newnode: *mut FuncExpr = makeNode!(FuncExpr, T_FuncExpr);

    (*newnode).funcid = (*from).funcid;
    (*newnode).funcresulttype = (*from).funcresulttype;
    (*newnode).funcretset = (*from).funcretset;
    (*newnode).funcvariadic = (*from).funcvariadic;
    (*newnode).funcformat = (*from).funcformat;
    (*newnode).funccollid = (*from).funccollid;
    (*newnode).inputcollid = (*from).inputcollid;
    (*newnode).args = copyObjectImpl((*from).args as *const c_void) as _;
    (*newnode).location = (*from).location;

    newnode
}

unsafe fn _copyOpExpr(from: *const OpExpr) -> *mut OpExpr {
    let newnode: *mut OpExpr = makeNode!(OpExpr, T_OpExpr);

    (*newnode).opno = (*from).opno;
    (*newnode).opfuncid = (*from).opfuncid;
    (*newnode).opresulttype = (*from).opresulttype;
    (*newnode).opretset = (*from).opretset;
    (*newnode).opcollid = (*from).opcollid;
    (*newnode).inputcollid = (*from).inputcollid;
    (*newnode).args = copyObjectImpl((*from).args as *const c_void) as _;
    (*newnode).location = (*from).location;

    newnode
}

unsafe fn _copyAggref(from: *const crate::nodes::primnodes::Aggref) -> *mut crate::nodes::primnodes::Aggref {
    let newnode = makeNode!(crate::nodes::primnodes::Aggref, T_Aggref);

    (*newnode).aggfnoid = (*from).aggfnoid;
    (*newnode).aggtype = (*from).aggtype;
    (*newnode).aggcollid = (*from).aggcollid;
    (*newnode).inputcollid = (*from).inputcollid;
    (*newnode).aggtranstype = (*from).aggtranstype;
    (*newnode).aggargtypes = copyObjectImpl((*from).aggargtypes as *const c_void) as _;
    (*newnode).aggdirectargs = copyObjectImpl((*from).aggdirectargs as *const c_void) as _;
    (*newnode).args = copyObjectImpl((*from).args as *const c_void) as _;
    (*newnode).aggorder = copyObjectImpl((*from).aggorder as *const c_void) as _;
    (*newnode).aggdistinct = copyObjectImpl((*from).aggdistinct as *const c_void) as _;
    (*newnode).aggfilter = copyObjectImpl((*from).aggfilter as *const c_void) as _;
    (*newnode).aggstar = (*from).aggstar;
    (*newnode).aggvariadic = (*from).aggvariadic;
    (*newnode).aggkind = (*from).aggkind;
    (*newnode).aggpresorted = (*from).aggpresorted;
    (*newnode).agglevelsup = (*from).agglevelsup;
    (*newnode).aggsplit = (*from).aggsplit;
    (*newnode).aggno = (*from).aggno;
    (*newnode).aggtransno = (*from).aggtransno;
    (*newnode).location = (*from).location;

    newnode
}

unsafe fn _copyTargetEntry(from: *const TargetEntry) -> *mut TargetEntry {
    let newnode: *mut TargetEntry = makeNode!(TargetEntry, T_TargetEntry);

    (*newnode).expr = copyObjectImpl((*from).expr as *const c_void) as _;
    (*newnode).resno = (*from).resno;
    (*newnode).resname = copy_string_field((*from).resname);
    (*newnode).ressortgroupref = (*from).ressortgroupref;
    (*newnode).resorigtbl = (*from).resorigtbl;
    (*newnode).resorigcol = (*from).resorigcol;
    (*newnode).resjunk = (*from).resjunk;

    newnode
}

unsafe fn _copyRangeTblRef(from: *const RangeTblRef) -> *mut RangeTblRef {
    let newnode: *mut RangeTblRef = makeNode!(RangeTblRef, T_RangeTblRef);

    (*newnode).rtindex = (*from).rtindex;

    newnode
}

unsafe fn _copyFromExpr(from: *const FromExpr) -> *mut FromExpr {
    let newnode: *mut FromExpr = makeNode!(FromExpr, T_FromExpr);

    (*newnode).fromlist = copyObjectImpl((*from).fromlist as *const c_void) as _;
    (*newnode).quals = copyObjectImpl((*from).quals as *const c_void) as _;

    newnode
}

unsafe fn _copyQuery(from: *const Query) -> *mut Query {
    let newnode: *mut Query = makeNode!(Query, T_Query);

    (*newnode).commandType = (*from).commandType;
    (*newnode).querySource = (*from).querySource;
    (*newnode).queryId = (*from).queryId;
    (*newnode).canSetTag = (*from).canSetTag;
    (*newnode).utilityStmt = copyObjectImpl((*from).utilityStmt as *const c_void) as _;
    (*newnode).resultRelation = (*from).resultRelation;
    (*newnode).hasAggs = (*from).hasAggs;
    (*newnode).hasWindowFuncs = (*from).hasWindowFuncs;
    (*newnode).hasTargetSRFs = (*from).hasTargetSRFs;
    (*newnode).hasSubLinks = (*from).hasSubLinks;
    (*newnode).hasDistinctOn = (*from).hasDistinctOn;
    (*newnode).hasRecursive = (*from).hasRecursive;
    (*newnode).hasModifyingCTE = (*from).hasModifyingCTE;
    (*newnode).hasForUpdate = (*from).hasForUpdate;
    (*newnode).hasRowSecurity = (*from).hasRowSecurity;
    (*newnode).hasGroupRTE = (*from).hasGroupRTE;
    (*newnode).isReturn = (*from).isReturn;
    (*newnode).cteList = copyObjectImpl((*from).cteList as *const c_void) as _;
    (*newnode).rtable = copyObjectImpl((*from).rtable as *const c_void) as _;
    (*newnode).rteperminfos = copyObjectImpl((*from).rteperminfos as *const c_void) as _;
    (*newnode).jointree = copyObjectImpl((*from).jointree as *const c_void) as _;
    (*newnode).mergeActionList = copyObjectImpl((*from).mergeActionList as *const c_void) as _;
    (*newnode).mergeTargetRelation = (*from).mergeTargetRelation;
    (*newnode).mergeJoinCondition = copyObjectImpl((*from).mergeJoinCondition as *const c_void) as _;
    (*newnode).targetList = copyObjectImpl((*from).targetList as *const c_void) as _;
    (*newnode).r#override = (*from).r#override;
    (*newnode).onConflict = copyObjectImpl((*from).onConflict as *const c_void) as _;
    (*newnode).returningOldAlias = copy_string_field((*from).returningOldAlias);
    (*newnode).returningNewAlias = copy_string_field((*from).returningNewAlias);
    (*newnode).returningList = copyObjectImpl((*from).returningList as *const c_void) as _;
    (*newnode).groupClause = copyObjectImpl((*from).groupClause as *const c_void) as _;
    (*newnode).groupDistinct = (*from).groupDistinct;
    (*newnode).groupingSets = copyObjectImpl((*from).groupingSets as *const c_void) as _;
    (*newnode).havingQual = copyObjectImpl((*from).havingQual as *const c_void) as _;
    (*newnode).windowClause = copyObjectImpl((*from).windowClause as *const c_void) as _;
    (*newnode).distinctClause = copyObjectImpl((*from).distinctClause as *const c_void) as _;
    (*newnode).sortClause = copyObjectImpl((*from).sortClause as *const c_void) as _;
    (*newnode).limitOffset = copyObjectImpl((*from).limitOffset as *const c_void) as _;
    (*newnode).limitCount = copyObjectImpl((*from).limitCount as *const c_void) as _;
    (*newnode).limitOption = (*from).limitOption;
    (*newnode).rowMarks = copyObjectImpl((*from).rowMarks as *const c_void) as _;
    (*newnode).setOperations = copyObjectImpl((*from).setOperations as *const c_void) as _;
    (*newnode).constraintDeps = copyObjectImpl((*from).constraintDeps as *const c_void) as _;
    (*newnode).withCheckOptions = copyObjectImpl((*from).withCheckOptions as *const c_void) as _;
    (*newnode).stmt_location = (*from).stmt_location;
    (*newnode).stmt_len = (*from).stmt_len;

    newnode
}

unsafe fn _copyRangeTblEntry(from: *const RangeTblEntry) -> *mut RangeTblEntry {
    let newnode: *mut RangeTblEntry = makeNode!(RangeTblEntry, T_RangeTblEntry);

    (*newnode).alias = copyObjectImpl((*from).alias as *const c_void) as _;
    (*newnode).eref = copyObjectImpl((*from).eref as *const c_void) as _;
    (*newnode).rtekind = (*from).rtekind;
    (*newnode).relid = (*from).relid;
    (*newnode).inh = (*from).inh;
    (*newnode).relkind = (*from).relkind;
    (*newnode).rellockmode = (*from).rellockmode;
    (*newnode).perminfoindex = (*from).perminfoindex;
    (*newnode).tablesample = copyObjectImpl((*from).tablesample as *const c_void) as _;
    (*newnode).subquery = copyObjectImpl((*from).subquery as *const c_void) as _;
    (*newnode).security_barrier = (*from).security_barrier;
    (*newnode).jointype = (*from).jointype;
    (*newnode).joinmergedcols = (*from).joinmergedcols;
    (*newnode).joinaliasvars = copyObjectImpl((*from).joinaliasvars as *const c_void) as _;
    (*newnode).joinleftcols = copyObjectImpl((*from).joinleftcols as *const c_void) as _;
    (*newnode).joinrightcols = copyObjectImpl((*from).joinrightcols as *const c_void) as _;
    (*newnode).join_using_alias = copyObjectImpl((*from).join_using_alias as *const c_void) as _;
    (*newnode).functions = copyObjectImpl((*from).functions as *const c_void) as _;
    (*newnode).funcordinality = (*from).funcordinality;
    (*newnode).tablefunc = copyObjectImpl((*from).tablefunc as *const c_void) as _;
    (*newnode).values_lists = copyObjectImpl((*from).values_lists as *const c_void) as _;
    (*newnode).ctename = copy_string_field((*from).ctename);
    (*newnode).ctelevelsup = (*from).ctelevelsup;
    (*newnode).self_reference = (*from).self_reference;
    (*newnode).coltypes = copyObjectImpl((*from).coltypes as *const c_void) as _;
    (*newnode).coltypmods = copyObjectImpl((*from).coltypmods as *const c_void) as _;
    (*newnode).colcollations = copyObjectImpl((*from).colcollations as *const c_void) as _;
    (*newnode).enrname = copy_string_field((*from).enrname);
    (*newnode).enrtuples = (*from).enrtuples;
    (*newnode).groupexprs = copyObjectImpl((*from).groupexprs as *const c_void) as _;
    (*newnode).lateral = (*from).lateral;
    (*newnode).inFromCl = (*from).inFromCl;
    (*newnode).securityQuals = copyObjectImpl((*from).securityQuals as *const c_void) as _;

    newnode
}

unsafe fn _copyRTEPermissionInfo(from: *const RTEPermissionInfo) -> *mut RTEPermissionInfo {
    let newnode: *mut RTEPermissionInfo = makeNode!(RTEPermissionInfo, T_RTEPermissionInfo);

    (*newnode).relid = (*from).relid;
    (*newnode).inh = (*from).inh;
    (*newnode).requiredPerms = (*from).requiredPerms;
    (*newnode).checkAsUser = (*from).checkAsUser;
    (*newnode).selectedCols = bms_copy((*from).selectedCols);
    (*newnode).insertedCols = bms_copy((*from).insertedCols);
    (*newnode).updatedCols = bms_copy((*from).updatedCols);

    newnode
}

/*
 * copyObjectImpl -- implementation of copyObject(); see nodes/nodes.h
 *
 * Create a copy of a Node tree or list.  This is a "deep" copy: all
 * substructure is copied too, recursively.
 */
#[no_mangle]
pub unsafe fn copyObjectImpl(from: *const c_void) -> *mut c_void {
    let retval: *mut c_void;

    if from.is_null() {
        return std::ptr::null_mut();
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    match nodeTag(from as *const NodeTag) {
        // The generated copyfuncs.switch.c dispatches every node tag to its
        // _copy<Tag>() function.  Those functions live in the generated
        // copyfuncs.funcs.c (produced by gen_node_support.pl) and are not yet
        // ported.  Wire in the handful of custom_copy_equal nodes whose copy
        // routines are hand-written in this file; everything else is TODO.
        NodeTag::T_Const => {
            retval = _copyConst(from as *const Const) as *mut c_void;
        }
        NodeTag::T_A_Const => {
            retval = _copyA_Const(from as *const A_Const) as *mut c_void;
        }
        NodeTag::T_ExtensibleNode => {
            retval = _copyExtensibleNode(from as *const ExtensibleNode) as *mut c_void;
        }
        NodeTag::T_Bitmapset => {
            retval = _copyBitmapset(from as *const Bitmapset) as *mut c_void;
        }

        NodeTag::T_Alias => {
            retval = _copyAlias(from as *const Alias) as *mut c_void;
        }
        NodeTag::T_Var => {
            retval = _copyVar(from as *const Var) as *mut c_void;
        }
        NodeTag::T_FuncExpr => {
            retval = _copyFuncExpr(from as *const FuncExpr) as *mut c_void;
        }
        NodeTag::T_OpExpr => {
            retval = _copyOpExpr(from as *const OpExpr) as *mut c_void;
        }
        NodeTag::T_Aggref => {
            retval = _copyAggref(from as *const crate::nodes::primnodes::Aggref) as *mut c_void;
        }
        NodeTag::T_TargetEntry => {
            retval = _copyTargetEntry(from as *const TargetEntry) as *mut c_void;
        }
        NodeTag::T_RangeTblRef => {
            retval = _copyRangeTblRef(from as *const RangeTblRef) as *mut c_void;
        }
        NodeTag::T_FromExpr => {
            retval = _copyFromExpr(from as *const FromExpr) as *mut c_void;
        }
        NodeTag::T_Query => {
            retval = _copyQuery(from as *const Query) as *mut c_void;
        }
        NodeTag::T_RangeTblEntry => {
            retval = _copyRangeTblEntry(from as *const RangeTblEntry) as *mut c_void;
        }
        NodeTag::T_RTEPermissionInfo => {
            retval = _copyRTEPermissionInfo(from as *const RTEPermissionInfo) as *mut c_void;
        }

        NodeTag::T_Integer => {
            let n = makeNode!(crate::nodes::value::Integer, T_Integer);
            (*n).ival = (*(from as *const crate::nodes::value::Integer)).ival;
            retval = n as *mut c_void;
        }
        NodeTag::T_Float => {
            let n = makeNode!(crate::nodes::value::Float, T_Float);
            (*n).fval = copy_string_field((*(from as *const crate::nodes::value::Float)).fval);
            retval = n as *mut c_void;
        }
        NodeTag::T_Boolean => {
            let n = makeNode!(crate::nodes::value::Boolean, T_Boolean);
            (*n).boolval = (*(from as *const crate::nodes::value::Boolean)).boolval;
            retval = n as *mut c_void;
        }
        NodeTag::T_String => {
            let n = makeNode!(crate::nodes::value::String, T_String);
            (*n).sval = copy_string_field((*(from as *const crate::nodes::value::String)).sval);
            retval = n as *mut c_void;
        }
        NodeTag::T_BitString => {
            let n = makeNode!(crate::nodes::value::BitString, T_BitString);
            (*n).bsval = copy_string_field((*(from as *const crate::nodes::value::BitString)).bsval);
            retval = n as *mut c_void;
        }

        NodeTag::T_List => {
            retval = list_copy_deep(from as *const List) as *mut c_void;
        }

        /*
         * Lists of integers, OIDs and XIDs don't need to be deep-copied,
         * so we perform a shallow copy via list_copy()
         */
        NodeTag::T_IntList | NodeTag::T_OidList | NodeTag::T_XidList => {
            retval = list_copy(from as *const List) as *mut c_void;
        }

        _ => {
            // TODO(pg-port): generated copyfuncs.switch.c / copyfuncs.funcs.c
            // (_copy<Tag>() for all remaining node types) not yet translated.
            elog!(
                ERROR,
                "unrecognized node type: {}",
                nodeTag(from as *const NodeTag) as c_int
            );
            retval = std::ptr::null_mut(); /* keep compiler quiet */
        }
    }

    retval
}
