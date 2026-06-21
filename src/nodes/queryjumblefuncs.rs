/*-------------------------------------------------------------------------
 *
 * queryjumblefuncs.c
 *	 Query normalization and fingerprinting.
 *
 * Normalization is a process whereby similar queries, typically differing only
 * in their constants (though the exact rules are somewhat more subtle than
 * that) are recognized as equivalent, and are tracked as a single entry.  This
 * is particularly useful for non-prepared queries.
 *
 * Normalization is implemented by fingerprinting queries, selectively
 * serializing those fields of each query tree's nodes that are judged to be
 * essential to the query.  This is referred to as a query jumble.  This is
 * distinct from a regular serialization in that various extraneous
 * information is ignored as irrelevant or not essential to the query, such
 * as the collations of Vars and, most notably, the values of constants.
 *
 * This jumble is acquired at the end of parse analysis of each query, and
 * a 64-bit hash of it is stored into the query's Query.queryId field.
 * The server then copies this value around, making it available in plan
 * tree(s) generated from the query.  The executor can then use this value
 * to blame query costs on the proper queryId.
 *
 * Arrays of two or more constants and PARAM_EXTERN parameters are "squashed"
 * and contribute only once to the jumble.  This has the effect that queries
 * that differ only on the length of such lists have the same queryId.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/queryjumblefuncs.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;
// wave27 queryjumble imports
use crate::{foreach, current_cell, castNode, IsA, foreach_current_index};
use crate::nodes::nodes::{Node, NodeTag, nodeTag};
use crate::nodes::pg_list::{List, ListCell, list_length, lfirst, lfirst_int, lfirst_oid, lfirst_xid};
use crate::nodes::primnodes::{Param, FuncExpr, RelabelType, CoerceViaIO, ArrayExpr, Alias};
use crate::nodes::primnodes::ParamKind::PARAM_EXTERN;
use crate::nodes::primnodes::CoercionForm::{COERCE_IMPLICIT_CAST, COERCE_EXPLICIT_CAST};
use crate::nodes::parsenodes::{RangeTblEntry, Query, A_Const, VariableSetStmt};
use crate::catalog::catalog::FirstGenbkiObjectId;


use std::ffi::{c_char, c_int};

use crate::c::{int64, Size};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// Stub types (faithful structural placeholders; real definitions live in the
// ported headers nodes/queryjumble.h and nodes/parsenodes.h).
// ---------------------------------------------------------------------------

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
#[repr(C)]
pub struct LocationLen {
    pub location: c_int,   /* start offset in query text */
    pub length: c_int,     /* length in bytes, or -1 to ignore */

    /* Does this location represent a squashed list? */
    pub squashed: bool,

    /* Is this location a PARAM_EXTERN parameter? */
    pub extern_param: bool,
}

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
#[repr(C)]
pub struct JumbleState {
    /* Jumble of current query tree */
    pub jumble: *mut std::ffi::c_uchar,

    /* Number of bytes used in jumble[] */
    pub jumble_len: Size,

    /* Array of locations of constants that should be removed */
    pub clocations: *mut LocationLen,

    /* Allocated length of clocations array */
    pub clocations_buf_size: c_int,

    /* Current number of valid entries in clocations array */
    pub clocations_count: c_int,

    /*
     * ID of the highest PARAM_EXTERN parameter we've seen in the query; used
     * to start normalization correctly.  However, if there are any squashed
     * lists in the query, we disregard query-supplied parameter numbers and
     * renumber everything.  This is to avoid possible gaps caused by
     * squashing in case any params are in squashed lists.
     */
    pub highest_extern_param_id: c_int,

    /* Whether squashable lists are present */
    pub has_squashed_lists: bool,

    /*
     * Count of the number of NULL nodes seen since last appending a value.
     * These are flushed out to the jumble buffer before subsequent appends
     * and before performing the final jumble hash.
     */
    pub pending_nulls: std::ffi::c_uint,

    /* The total number of bytes added to the jumble buffer (USE_ASSERT_CHECKING) */
    pub total_jumble_len: Size,
}

/* Values for the compute_query_id GUC */
pub const COMPUTE_QUERY_ID_OFF: c_int = 0;
pub const COMPUTE_QUERY_ID_ON: c_int = 1;
pub const COMPUTE_QUERY_ID_AUTO: c_int = 2;
pub const COMPUTE_QUERY_ID_REGRESS: c_int = 3;

/*
 * Returns whether query identifier computation has been enabled, either
 * directly in the GUC or by a module when the setting is 'auto'.
 */
#[inline]
pub unsafe fn IsQueryIdEnabled() -> bool {
    if compute_query_id == COMPUTE_QUERY_ID_OFF {
        return false;
    }
    if compute_query_id == COMPUTE_QUERY_ID_ON {
        return true;
    }
    query_id_enabled
}

const JUMBLE_SIZE: usize = 1024; /* query serialization buffer size */

/* GUC parameters */
#[no_mangle]
pub static mut compute_query_id: c_int = COMPUTE_QUERY_ID_AUTO;

/*
 * True when compute_query_id is ON or AUTO, and a module requests them.
 *
 * Note that IsQueryIdEnabled() should be used instead of checking
 * query_id_enabled or compute_query_id directly when we want to know
 * whether query identifiers are computed in the core or not.
 */
#[no_mangle]
pub static mut query_id_enabled: bool = false;

/*
 * Given a possibly multi-statement source string, confine our attention to the
 * relevant part of the string.
 */
#[no_mangle]
pub unsafe extern "C" fn CleanQuerytext(
    query: *const c_char,
    location: *mut c_int,
    len: *mut c_int,
) -> *const c_char {
    let mut query = query;
    let mut query_location: c_int = *location;
    let mut query_len: c_int = *len;

    /* First apply starting offset, unless it's -1 (unknown). */
    if query_location >= 0 {
        Assert!(query_location as usize <= strlen(query));
        query = query.add(query_location as usize);
        /* Length of 0 (or -1) means "rest of string" */
        if query_len <= 0 {
            query_len = strlen(query) as c_int;
        } else {
            Assert!(query_len as usize <= strlen(query));
        }
    } else {
        /* If query location is unknown, distrust query_len as well */
        query_location = 0;
        query_len = strlen(query) as c_int;
    }

    /*
     * Discard leading and trailing whitespace, too.  Use scanner_isspace()
     * not libc's isspace(), because we want to match the lexer's behavior.
     *
     * Note: the parser now strips leading comments and whitespace from the
     * reported stmt_location, so this first loop will only iterate in the
     * unusual case that the location didn't propagate to here.  But the
     * statement length will extend to the end-of-string or terminating
     * semicolon, so the second loop often does something useful.
     */
    while query_len > 0 && scanner_isspace(*query) {
        query = query.add(1);
        query_location += 1;
        query_len -= 1;
    }
    while query_len > 0 && scanner_isspace(*query.add((query_len - 1) as usize)) {
        query_len -= 1;
    }

    *location = query_location;
    *len = query_len;

    query
}

/*
 * JumbleQuery
 *		Recursively process the given Query producing a 64-bit hash value by
 *		hashing the relevant fields and record that value in the Query's queryId
 *		field.  Return the JumbleState object used for jumbling the query.
 */
#[no_mangle]
pub unsafe extern "C" fn JumbleQuery(query: *mut Query) -> *mut JumbleState {
    let jstate: *mut JumbleState;

    Assert!(IsQueryIdEnabled());

    jstate = InitJumble();

    (*query).queryId = DoJumble(jstate, query as *mut Node) as i64;

    /*
     * If we are unlucky enough to get a hash of zero, use 1 instead for
     * normal statements and 2 for utility queries.
     */
    if (*query).queryId == 0 {
        if !(*query).utilityStmt.is_null() {
            (*query).queryId = 2;
        } else {
            (*query).queryId = 1;
        }
    }

    jstate
}

/*
 * Enables query identifier computation.
 *
 * Third-party plugins can use this function to inform core that they require
 * a query identifier to be computed.
 */
#[no_mangle]
pub unsafe extern "C" fn EnableQueryId() {
    if compute_query_id != COMPUTE_QUERY_ID_OFF {
        query_id_enabled = true;
    }
}

/*
 * InitJumble
 *		Allocate a JumbleState object and make it ready to jumble.
 */
unsafe fn InitJumble() -> *mut JumbleState {
    let jstate: *mut JumbleState;

    jstate = palloc(size_of::<JumbleState>()) as *mut JumbleState;

    /* Set up workspace for query jumbling */
    (*jstate).jumble = palloc(JUMBLE_SIZE) as *mut std::ffi::c_uchar;
    (*jstate).jumble_len = 0;
    (*jstate).clocations_buf_size = 32;
    (*jstate).clocations = palloc(
        (*jstate).clocations_buf_size as usize * size_of::<LocationLen>(),
    ) as *mut LocationLen;
    (*jstate).clocations_count = 0;
    (*jstate).highest_extern_param_id = 0;
    (*jstate).pending_nulls = 0;
    (*jstate).has_squashed_lists = false;
    // USE_ASSERT_CHECKING
    (*jstate).total_jumble_len = 0;

    jstate
}

/*
 * DoJumble
 *		Jumble the given Node using the given JumbleState and return the resulting
 *		jumble hash.
 */
unsafe fn DoJumble(jstate: *mut JumbleState, node: *mut Node) -> int64 {
    /* Jumble the given node */
    _jumbleNode(jstate, node);

    /* Flush any pending NULLs before doing the final hash */
    if (*jstate).pending_nulls > 0 {
        FlushPendingNulls(jstate);
    }

    /* Squashed list found, reset highest_extern_param_id */
    if (*jstate).has_squashed_lists {
        (*jstate).highest_extern_param_id = 0;
    }

    /* Process the jumble buffer and produce the hash value */
    DatumGetInt64(hash_any_extended(
        (*jstate).jumble,
        (*jstate).jumble_len,
        0,
    ))
}

/*
 * AppendJumbleInternal: Internal function for appending to the jumble buffer
 *
 * Note: Callers must ensure that size > 0.
 */
#[inline(always)]
unsafe fn AppendJumbleInternal(
    jstate: *mut JumbleState,
    item: *const std::ffi::c_uchar,
    size: Size,
) {
    let mut item = item;
    let mut size = size;
    let jumble = (*jstate).jumble;
    let mut jumble_len = (*jstate).jumble_len;

    /* Ensure the caller didn't mess up */
    Assert!(size > 0);

    /*
     * Fast path for when there's enough space left in the buffer.  This is
     * worthwhile as means the memcpy can be inlined into very efficient code
     * when 'size' is a compile-time constant.
     */
    if likely(size <= JUMBLE_SIZE - (*jstate).jumble_len) {
        memcpy(
            jumble.add((*jstate).jumble_len) as *mut c_void,
            item as *const c_void,
            size,
        );
        (*jstate).jumble_len += size;

        // USE_ASSERT_CHECKING
        (*jstate).total_jumble_len += size;

        return;
    }

    /*
     * Whenever the jumble buffer is full, we hash the current contents and
     * reset the buffer to contain just that hash value, thus relying on the
     * hash to summarize everything so far.
     */
    loop {
        let part_size: Size;

        if unlikely(jumble_len >= JUMBLE_SIZE) {
            let start_hash: int64;

            start_hash =
                DatumGetInt64(hash_any_extended(jumble, JUMBLE_SIZE, 0));
            memcpy(
                jumble as *mut c_void,
                &start_hash as *const int64 as *const c_void,
                size_of::<int64>(),
            );
            jumble_len = size_of::<int64>();
        }
        part_size = Min(size, JUMBLE_SIZE - jumble_len);
        memcpy(
            jumble.add(jumble_len) as *mut c_void,
            item as *const c_void,
            part_size,
        );
        jumble_len += part_size;
        item = item.add(part_size);
        size -= part_size;

        // USE_ASSERT_CHECKING
        (*jstate).total_jumble_len += part_size;

        if size == 0 {
            break;
        }
    }

    (*jstate).jumble_len = jumble_len;
}

/*
 * AppendJumble
 *		Add 'size' bytes of the given jumble 'value' to the jumble state
 */
unsafe fn AppendJumble(
    jstate: *mut JumbleState,
    value: *const std::ffi::c_uchar,
    size: Size,
) {
    if (*jstate).pending_nulls > 0 {
        FlushPendingNulls(jstate);
    }

    AppendJumbleInternal(jstate, value, size);
}

/*
 * AppendJumbleNull
 *		For jumbling NULL pointers
 */
#[inline(always)]
unsafe fn AppendJumbleNull(jstate: *mut JumbleState) {
    (*jstate).pending_nulls += 1;
}

/*
 * AppendJumble8
 *		Add the first byte from the given 'value' pointer to the jumble state
 */
unsafe fn AppendJumble8(jstate: *mut JumbleState, value: *const std::ffi::c_uchar) {
    if (*jstate).pending_nulls > 0 {
        FlushPendingNulls(jstate);
    }

    AppendJumbleInternal(jstate, value, 1);
}

/*
 * AppendJumble16
 *		Add the first 2 bytes from the given 'value' pointer to the jumble
 *		state.
 */
unsafe fn AppendJumble16(jstate: *mut JumbleState, value: *const std::ffi::c_uchar) {
    if (*jstate).pending_nulls > 0 {
        FlushPendingNulls(jstate);
    }

    AppendJumbleInternal(jstate, value, 2);
}

/*
 * AppendJumble32
 *		Add the first 4 bytes from the given 'value' pointer to the jumble
 *		state.
 */
unsafe fn AppendJumble32(jstate: *mut JumbleState, value: *const std::ffi::c_uchar) {
    if (*jstate).pending_nulls > 0 {
        FlushPendingNulls(jstate);
    }

    AppendJumbleInternal(jstate, value, 4);
}

/*
 * AppendJumble64
 *		Add the first 8 bytes from the given 'value' pointer to the jumble
 *		state.
 */
unsafe fn AppendJumble64(jstate: *mut JumbleState, value: *const std::ffi::c_uchar) {
    if (*jstate).pending_nulls > 0 {
        FlushPendingNulls(jstate);
    }

    AppendJumbleInternal(jstate, value, 8);
}

/*
 * FlushPendingNulls
 *		Incorporate the pending_nulls value into the jumble buffer.
 *
 * Note: Callers must ensure that there's at least 1 pending NULL.
 */
#[inline(always)]
unsafe fn FlushPendingNulls(jstate: *mut JumbleState) {
    Assert!((*jstate).pending_nulls > 0);

    AppendJumbleInternal(
        jstate,
        &(*jstate).pending_nulls as *const std::ffi::c_uint as *const std::ffi::c_uchar,
        4,
    );
    (*jstate).pending_nulls = 0;
}

/*
 * Record the location of some kind of constant within a query string.
 * These are not only bare constants but also expressions that ultimately
 * constitute a constant, such as those inside casts and simple function
 * calls; if extern_param, then it corresponds to a PARAM_EXTERN Param.
 *
 * If length is -1, it indicates a single such constant element.  If
 * it's a positive integer, it indicates the length of a squashable
 * list of them.
 */
unsafe fn RecordConstLocation(
    jstate: *mut JumbleState,
    extern_param: bool,
    location: c_int,
    len: c_int,
) {
    /* -1 indicates unknown or undefined location */
    if location >= 0 {
        /* enlarge array if needed */
        if (*jstate).clocations_count >= (*jstate).clocations_buf_size {
            (*jstate).clocations_buf_size *= 2;
            (*jstate).clocations = repalloc(
                (*jstate).clocations as *mut c_void,
                (*jstate).clocations_buf_size as usize * size_of::<LocationLen>(),
            ) as *mut LocationLen;
        }
        let idx = (*jstate).clocations_count as usize;
        (*(*jstate).clocations.add(idx)).location = location;

        /*
         * Lengths are either positive integers (indicating a squashable
         * list), or -1.
         */
        Assert!(len > -1 || len == -1);
        (*(*jstate).clocations.add(idx)).length = len;
        (*(*jstate).clocations.add(idx)).squashed = len > -1;
        (*(*jstate).clocations.add(idx)).extern_param = extern_param;
        (*jstate).clocations_count += 1;
    }
}

/*
 * Subroutine for _jumbleElements: Verify a few simple cases where we can
 * deduce that the expression is a constant:
 *
 * - See through any wrapping RelabelType and CoerceViaIO layers.
 * - If it's a FuncExpr, check that the function is a builtin
 *   cast and its arguments are Const.
 * - Otherwise test if the expression is a simple Const or a
 *   PARAM_EXTERN param.
 */
unsafe fn IsSquashableConstant(element: *mut Node) -> bool {
    let mut element = element;
    // restart:
    loop {
        match nodeTag(element) {
            NodeTag::T_RelabelType => {
                /* Unwrap RelabelType */
                element = (*(element as *mut RelabelType)).arg as *mut Node;
                continue; // goto restart;
            }

            NodeTag::T_CoerceViaIO => {
                /* Unwrap CoerceViaIO */
                element = (*(element as *mut CoerceViaIO)).arg as *mut Node;
                continue; // goto restart;
            }

            NodeTag::T_Const => return true,

            NodeTag::T_Param => {
                return (*castNode!(Param, T_Param, element)).paramkind == PARAM_EXTERN;
            }

            NodeTag::T_FuncExpr => {
                let func = element as *mut FuncExpr;

                if (*func).funcformat != COERCE_IMPLICIT_CAST
                    && (*func).funcformat != COERCE_EXPLICIT_CAST
                {
                    return false;
                }

                if (*func).funcid > FirstGenbkiObjectId {
                    return false;
                }

                /*
                 * We can check function arguments recursively, being careful
                 * about recursing too deep.  At each recursion level it's
                 * enough to test the stack on the first element.  (Note that
                 * I wasn't able to hit this without bloating the stack
                 * artificially in this function: the parser errors out before
                 * stack size becomes a problem here.)
                 */
                foreach!(temp, (*func).args, {
                    let arg = lfirst(current_cell!(temp)) as *mut Node;

                    if !IsA!(arg, T_Const) {
                        if foreach_current_index!(temp) == 0 && stack_is_too_deep() {
                            return false;
                        } else if !IsSquashableConstant(arg) {
                            return false;
                        }
                    }
                });

                return true;
            }

            _ => return false,
        }
    }
}

/*
 * Subroutine for _jumbleElements: Verify whether the provided list
 * can be squashed, meaning it contains only constant expressions.
 *
 * Return value indicates if squashing is possible.
 *
 * Note that this function searches only for explicit Const nodes with
 * possibly very simple decorations on top and PARAM_EXTERN parameters,
 * and does not try to simplify expressions.
 */
unsafe fn IsSquashableConstantList(elements: *mut List) -> bool {
    /* If the list is too short, we don't try to squash it. */
    if list_length(elements) < 2 {
        return false;
    }

    foreach!(temp, elements, {
        if !IsSquashableConstant(lfirst(current_cell!(temp)) as *mut Node) {
            return false;
        }
    });

    true
}

// ---------------------------------------------------------------------------
// The JUMBLE_* macros below are expanded inline at their use sites in the
// generated code (queryjumblefuncs.funcs.c).  They are reproduced here as
// helper closures/macros where used.
//
//   JUMBLE_NODE(item)        -> _jumbleNode(jstate, expr->item as *Node)
//   JUMBLE_ELEMENTS(list, n) -> _jumbleElements(jstate, expr->list, n)
//   JUMBLE_LOCATION(loc)     -> RecordConstLocation(jstate, false, expr->loc, -1)
//   JUMBLE_FIELD(item)       -> AppendJumbleN by sizeof(expr->item)
//   JUMBLE_STRING(str)       -> AppendJumble(expr->str, strlen+1) or AppendJumbleNull
//   JUMBLE_CUSTOM(type,item) -> _jumble<type>_<item>(jstate, expr, expr->item)
// ---------------------------------------------------------------------------

// #include "queryjumblefuncs.funcs.c"
//
// The per-node-type jumble functions (_jumbleQuery, _jumbleVar, etc.) are
// generated at build time from the node definitions by gen_node_support.pl.
// They are not present in the source tree, so the generated body is stubbed.
unsafe fn _jumble_generated_funcs(_jstate: *mut JumbleState, _node: *mut Node) {
    unimplemented!() // TODO: src/backend/nodes/queryjumblefuncs.funcs.c (generated)
}

unsafe fn _jumbleNode(jstate: *mut JumbleState, node: *mut Node) {
    let expr = node;
    // USE_ASSERT_CHECKING
    let prev_jumble_len: Size = (*jstate).total_jumble_len;

    if expr.is_null() {
        AppendJumbleNull(jstate);
        return;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /*
     * We always emit the node's NodeTag, then any additional fields that are
     * considered significant, and then we recurse to any child nodes.
     */
    // JUMBLE_FIELD(type);  -- type is the NodeTag (4 bytes)
    AppendJumble32(
        jstate,
        &(*expr).r#type as *const NodeTag as *const std::ffi::c_uchar,
    );

    match nodeTag(expr) {
        // #include "queryjumblefuncs.switch.c"
        //
        // The generated switch dispatches each concrete NodeTag to its
        // _jumble<Type> function (from queryjumblefuncs.funcs.c).  That
        // generated content is not in the source tree, so it is stubbed.
        NodeTag::T_List | NodeTag::T_IntList | NodeTag::T_OidList | NodeTag::T_XidList => {
            _jumbleList(jstate, expr);
        }

        _ => {
            // Generated switch arms handle all other recognized node types.
            // Fall back to the generated dispatcher (stubbed); on the real
            // default, C only warns and stumbles along.
            _jumble_generated_funcs(jstate, expr);
            // Only a warning, since we can stumble along anyway
            // elog!(WARNING, "unrecognized node type: {}", nodeTag(expr) as c_int);
        }
    }

    /* Ensure we added something to the jumble buffer */
    Assert!((*jstate).total_jumble_len > prev_jumble_len);
}

unsafe fn _jumbleList(jstate: *mut JumbleState, node: *mut Node) {
    let expr = node as *mut List;

    match (*expr).r#type {
        NodeTag::T_List => {
            foreach!(l, expr, {
                _jumbleNode(jstate, lfirst(current_cell!(l)) as *mut Node);
            });
        }
        NodeTag::T_IntList => {
            foreach!(l, expr, {
                AppendJumble32(
                    jstate,
                    &lfirst_int(current_cell!(l)) as *const c_int as *const std::ffi::c_uchar,
                );
            });
        }
        NodeTag::T_OidList => {
            foreach!(l, expr, {
                AppendJumble32(
                    jstate,
                    &lfirst_oid(current_cell!(l)) as *const Oid as *const std::ffi::c_uchar,
                );
            });
        }
        NodeTag::T_XidList => {
            foreach!(l, expr, {
                AppendJumble32(
                    jstate,
                    &lfirst_xid(current_cell!(l)) as *const TransactionId
                        as *const std::ffi::c_uchar,
                );
            });
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized list node type: {}",
                (*expr).r#type as c_int
            );
            #[allow(unreachable_code)]
            {
                return;
            }
        }
    }
}

/*
 * We try to jumble lists of expressions as one individual item regardless
 * of how many elements are in the list. This is know as squashing, which
 * results in different queries jumbling to the same query_id, if the only
 * difference is the number of elements in the list.
 *
 * We allow constants and PARAM_EXTERN parameters to be squashed. To normalize
 * such queries, we use the start and end locations of the list of elements in
 * a list.
 */
unsafe fn _jumbleElements(jstate: *mut JumbleState, elements: *mut List, node: *mut Node) {
    let mut normalize_list = false;

    if IsSquashableConstantList(elements) {
        if IsA!(node, T_ArrayExpr) {
            let aexpr = node as *mut ArrayExpr;

            if (*aexpr).list_start > 0 && (*aexpr).list_end > 0 {
                RecordConstLocation(
                    jstate,
                    false,
                    (*aexpr).list_start + 1,
                    ((*aexpr).list_end - (*aexpr).list_start) - 1,
                );
                normalize_list = true;
                (*jstate).has_squashed_lists = true;
            }
        }
    }

    if !normalize_list {
        _jumbleNode(jstate, elements as *mut Node);
    }
}

/*
 * We store the highest param ID of extern params.  This can later be used
 * to start the numbering of the placeholder for squashed lists.
 */
unsafe fn _jumbleParam(jstate: *mut JumbleState, node: *mut Node) {
    let expr = node as *mut Param;

    // JUMBLE_FIELD(paramkind);
    AppendJumble32(
        jstate,
        &(*expr).paramkind as *const _ as *const std::ffi::c_uchar,
    );
    // JUMBLE_FIELD(paramid);
    AppendJumble32(
        jstate,
        &(*expr).paramid as *const c_int as *const std::ffi::c_uchar,
    );
    // JUMBLE_FIELD(paramtype);
    AppendJumble32(
        jstate,
        &(*expr).paramtype as *const Oid as *const std::ffi::c_uchar,
    );
    /* paramtypmode and paramcollid are ignored */

    if (*expr).paramkind == PARAM_EXTERN {
        /*
         * At this point, only external parameter locations outside of
         * squashable lists will be recorded.
         */
        RecordConstLocation(jstate, true, (*expr).location, -1);

        /*
         * Update the highest Param id seen, in order to start normalization
         * correctly.
         *
         * Note: This value is reset at the end of jumbling if there exists a
         * squashable list. See the comment in the definition of JumbleState.
         */
        if (*expr).paramid > (*jstate).highest_extern_param_id {
            (*jstate).highest_extern_param_id = (*expr).paramid;
        }
    }
}

unsafe fn _jumbleA_Const(jstate: *mut JumbleState, node: *mut Node) {
    let expr = node as *mut A_Const;

    // JUMBLE_FIELD(isnull);
    AppendJumble8(
        jstate,
        &(*expr).isnull as *const bool as *const std::ffi::c_uchar,
    );
    if !(*expr).isnull {
        // JUMBLE_FIELD(val.node.type);
        AppendJumble32(
            jstate,
            core::ptr::addr_of!((*expr).val.node) as *const NodeTag as *const std::ffi::c_uchar,
        );
        match nodeTag(&mut (*expr).val as *mut _ as *mut Node) {
            NodeTag::T_Integer => {
                // JUMBLE_FIELD(val.ival.ival);
                AppendJumble32(
                    jstate,
                    core::ptr::addr_of!((*expr).val.ival) as *const c_int as *const std::ffi::c_uchar,
                );
            }
            NodeTag::T_Float => {
                // JUMBLE_STRING(val.fval.fval);
                let s = (*(core::ptr::addr_of!((*expr).val.fval) as *const crate::nodes::value::Float)).fval;
                if !s.is_null() {
                    AppendJumble(jstate, s as *const std::ffi::c_uchar, strlen(s) + 1);
                } else {
                    AppendJumbleNull(jstate);
                }
            }
            NodeTag::T_Boolean => {
                // JUMBLE_FIELD(val.boolval.boolval);
                AppendJumble8(
                    jstate,
                    core::ptr::addr_of!((*expr).val.boolval) as *const bool as *const std::ffi::c_uchar,
                );
            }
            NodeTag::T_String => {
                // JUMBLE_STRING(val.sval.sval);
                let s = (*(core::ptr::addr_of!((*expr).val.sval) as *const crate::nodes::value::String)).sval;
                if !s.is_null() {
                    AppendJumble(jstate, s as *const std::ffi::c_uchar, strlen(s) + 1);
                } else {
                    AppendJumbleNull(jstate);
                }
            }
            NodeTag::T_BitString => {
                // JUMBLE_STRING(val.bsval.bsval);
                let s = (*(core::ptr::addr_of!((*expr).val.bsval) as *const crate::nodes::value::BitString)).bsval;
                if !s.is_null() {
                    AppendJumble(jstate, s as *const std::ffi::c_uchar, strlen(s) + 1);
                } else {
                    AppendJumbleNull(jstate);
                }
            }
            _ => {
                elog!(
                    ERROR,
                    "unrecognized node type: {}",
                    nodeTag(&mut (*expr).val as *mut _ as *mut Node) as c_int
                );
            }
        }
    }
}

unsafe fn _jumbleVariableSetStmt(jstate: *mut JumbleState, node: *mut Node) {
    let expr = node as *mut VariableSetStmt;

    // JUMBLE_FIELD(kind);
    AppendJumble32(
        jstate,
        &(*expr).kind as *const _ as *const std::ffi::c_uchar,
    );
    // JUMBLE_STRING(name);
    {
        let s = (*expr).name;
        if !s.is_null() {
            AppendJumble(jstate, s as *const std::ffi::c_uchar, strlen(s) + 1);
        } else {
            AppendJumbleNull(jstate);
        }
    }

    /*
     * Account for the list of arguments in query jumbling only if told by the
     * parser.
     */
    if (*expr).jumble_args {
        // JUMBLE_NODE(args);
        _jumbleNode(jstate, (*expr).args as *mut Node);
    }
    // JUMBLE_FIELD(is_local);
    AppendJumble8(
        jstate,
        &(*expr).is_local as *const bool as *const std::ffi::c_uchar,
    );
    // JUMBLE_LOCATION(location);
    RecordConstLocation(jstate, false, (*expr).location, -1);
}

/*
 * Custom query jumble function for RangeTblEntry.eref.
 */
unsafe fn _jumbleRangeTblEntry_eref(
    jstate: *mut JumbleState,
    _rte: *mut RangeTblEntry,
    expr: *mut Alias,
) {
    // JUMBLE_FIELD(type);
    AppendJumble32(
        jstate,
        &(*expr).r#type as *const NodeTag as *const std::ffi::c_uchar,
    );

    /*
     * This includes only the table name, the list of column names is ignored.
     */
    // JUMBLE_STRING(aliasname);
    {
        let s = (*expr).aliasname;
        if !s.is_null() {
            AppendJumble(jstate, s as *const std::ffi::c_uchar, strlen(s) + 1);
        } else {
            AppendJumbleNull(jstate);
        }
    }
}

// ---------------------------------------------------------------------------
// Local stubs for helpers not yet ported.
// ---------------------------------------------------------------------------

unsafe fn scanner_isspace(_ch: c_char) -> bool {
    crate::parser::scansup::scanner_isspace(_ch)
}

unsafe fn hash_any_extended(_k: *const std::ffi::c_uchar, _keylen: Size, _seed: u64) -> Datum {
    crate::common::hashfn::hash_any_extended(_k as _, _keylen as _, _seed)
}

unsafe fn check_stack_depth() {
    crate::utils::misc::stack_depth::check_stack_depth()
}

unsafe fn stack_is_too_deep() -> bool {
    crate::utils::misc::stack_depth::stack_is_too_deep()
}
