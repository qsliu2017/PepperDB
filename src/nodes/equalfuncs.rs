//! Translation of postgres/src/backend/nodes/equalfuncs.c
//!
//! Equality functions to compare node trees.
//!
//! NOTE: it is intentional that parse location fields (in nodes that have
//! one) are not compared.  This is because we want, for example, a variable
//! "x" to be considered equal() to another reference to "x" in the query.

use crate::miscadmin::check_stack_depth;
use crate::nodes::extensible::{ExtensibleNodeMethods, GetExtensibleNodeMethods, ExtensibleNode};
use crate::nodes::bitmapset::{bms_equal, Bitmapset};
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::nodes::pg_list::{lfirst, lfirst_int, lfirst_oid, lfirst_xid, List, ListCell};
use crate::nodes::parsenodes::A_Const;
use crate::nodes::primnodes::Const;
use crate::utils::adt::datum::datumIsEqual;
use crate::utils::elog::ERROR;
use crate::{elog, forboth};
use core::ffi::{c_int, c_void};

/*
 * Macros to simplify comparison of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

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

/*
 * equal
 *	  returns whether two nodes are equal
 */
pub unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    let retval: bool;

    if a == b {
        return true;
    }

    /*
     * note that a!=b, so only one of them can be NULL
     */
    if a.is_null() || b.is_null() {
        return false;
    }

    /*
     * are they the same type of nodes?
     */
    if nodeTag(a) != nodeTag(b) {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    match nodeTag(a) {
        // TODO(pg-port): generated per-type comparators (equalfuncs.switch.c):
        // each emits `case T_Foo: retval = _equalFoo(a, b); break;`.  These are
        // produced by gen_node_support.pl and will be filled as the node-type
        // comparators are translated.  The custom_copy_equal cases below and
        // the List cases are hand-written (per equalfuncs.c proper).
        NodeTag::T_Const => {
            retval = _equalConst(a as *const Const, b as *const Const);
        }
        NodeTag::T_ExtensibleNode => {
            retval = _equalExtensibleNode(a as *const ExtensibleNode, b as *const ExtensibleNode);
        }
        NodeTag::T_A_Const => {
            retval = _equalA_Const(a as *const A_Const, b as *const A_Const);
        }
        NodeTag::T_Bitmapset => {
            retval = _equalBitmapset(a as *const Bitmapset, b as *const Bitmapset);
        }
        NodeTag::T_List | NodeTag::T_IntList | NodeTag::T_OidList | NodeTag::T_XidList => {
            retval = _equalList(a as *const List, b as *const List);
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(a) as c_int);
            retval = false; /* keep compiler quiet */
        }
    }

    retval
}
