//! nodes/copyfuncs.c - Copy functions for Postgres tree nodes.

use crate::prelude::*;

use crate::miscadmin::check_stack_depth;
use crate::nodes::bitmapset::{bms_copy, Bitmapset};
use crate::nodes::extensible::{ExtensibleNode, ExtensibleNodeMethods, GetExtensibleNodeMethods};
use crate::nodes::nodes::{nodeTag, newNode, NodeTag, ParseLoc};
use crate::nodes::parsenodes::A_Const;
use crate::nodes::pg_list::{list_copy, list_copy_deep, List};
use crate::nodes::primnodes::Const;
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
        (*newnode).val.node.r#type = (*from).val.node.r#type;
        match nodeTag(&(*from).val as *const _ as *const NodeTag) {
            NodeTag::T_Integer => {
                (*newnode).val.ival.ival = (*from).val.ival.ival;
            }
            NodeTag::T_Float => {
                (*newnode).val.fval.fval = copy_string_field((*from).val.fval.fval);
            }
            NodeTag::T_Boolean => {
                (*newnode).val.boolval.boolval = (*from).val.boolval.boolval;
            }
            NodeTag::T_String => {
                (*newnode).val.sval.sval = copy_string_field((*from).val.sval.sval);
            }
            NodeTag::T_BitString => {
                (*newnode).val.bsval.bsval = copy_string_field((*from).val.bsval.bsval);
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
