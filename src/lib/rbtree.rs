//! Translation of postgres/src/include/lib/rbtree.h
//!                + postgres/src/backend/lib/rbtree.c
//!
//! Generic Red-Black binary tree package. Adopted from Thomas Niemann's
//! "Sorting and Searching Algorithms: a Cookbook".
//!
//! Red-black trees keep (1) any child of a red node black, and (2) every path
//! from root to leaf traversing an equal number of black nodes, guaranteeing
//! O(lg n) lookups.
//!
//! Copyright (c) 2009-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

/*
 * RBTNode is intended to be used as the first field of a larger struct,
 * whose additional fields carry whatever payload data the caller needs
 * for a tree entry.  (The total size of that larger struct is passed to
 * rbt_create.)	RBTNode is declared here to support this usage, but
 * callers must treat it as an opaque struct.
 */
#[repr(C)]
pub struct RBTNode {
    /// node's current color, red or black
    pub color: c_char,
    /// left child, or RBTNIL if none
    pub left: *mut RBTNode,
    /// right child, or RBTNIL if none
    pub right: *mut RBTNode,
    /// parent, or NULL (not RBTNIL!) if none
    pub parent: *mut RBTNode,
}

/* Available tree iteration orderings */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RBTOrderControl {
    /// inorder: left child, node, right child
    LeftRightWalk = 0,
    /// reverse inorder: right, node, left
    RightLeftWalk = 1,
}
pub use RBTOrderControl::*;

/*
 * RBTreeIterator holds state while traversing a tree.  This is declared
 * here so that callers can stack-allocate this, but must otherwise be
 * treated as an opaque struct.
 */
#[repr(C)]
pub struct RBTreeIterator {
    pub rbt: *mut RBTree,
    pub iterate: Option<unsafe fn(iter: *mut RBTreeIterator) -> *mut RBTNode>,
    pub last_visited: *mut RBTNode,
    pub is_over: bool,
}

/* Support functions to be provided by caller */
pub type rbt_comparator = unsafe fn(a: *const RBTNode, b: *const RBTNode, arg: *mut c_void) -> c_int;
pub type rbt_combiner =
    unsafe fn(existing: *mut RBTNode, newdata: *const RBTNode, arg: *mut c_void);
pub type rbt_allocfunc = unsafe fn(arg: *mut c_void) -> *mut RBTNode;
pub type rbt_freefunc = unsafe fn(x: *mut RBTNode, arg: *mut c_void);

/*
 * Colors of nodes (values of RBTNode.color)
 */
const RBTBLACK: c_char = 0;
const RBTRED: c_char = 1;

/*
 * RBTree control structure
 */
#[repr(C)]
pub struct RBTree {
    /// root node, or RBTNIL if tree is empty
    pub root: *mut RBTNode,

    /* Remaining fields are constant after rbt_create */
    /// actual size of tree nodes
    node_size: Size,
    /* The caller-supplied manipulation functions */
    comparator: rbt_comparator,
    combiner: rbt_combiner,
    allocfunc: rbt_allocfunc,
    freefunc: Option<rbt_freefunc>,
    /// Passthrough arg passed to all manipulation functions
    arg: *mut c_void,
}

/*
 * all leafs are sentinels, use customized NIL name to prevent
 * collision with system-wide constant NIL which is actually NULL
 *
 * In C this is `static RBTNode sentinel = { RBTBLACK, RBTNIL, RBTNIL, NULL }`,
 * i.e. its left/right point at itself. We cannot form that self-reference in a
 * Rust static initializer, so left/right start NULL; this is immaterial because
 * RBTNIL is a leaf whose children are never traversed (callers always test
 * `!= RBTNIL` before descending). Only its color (always RBTBLACK) is read.
 */
static mut sentinel: RBTNode = RBTNode {
    color: RBTBLACK,
    left: core::ptr::null_mut(),
    right: core::ptr::null_mut(),
    parent: core::ptr::null_mut(),
};

/// `RBTNIL`: the shared leaf sentinel pointer (`&sentinel` in C).
#[inline]
#[allow(static_mut_refs)]
fn RBTNIL() -> *mut RBTNode {
    &raw mut sentinel
}

/*
 * rbt_create: create an empty RBTree
 *
 * Arguments are:
 *	node_size: actual size of tree nodes (> sizeof(RBTNode))
 *	The manipulation functions:
 *	comparator: compare two RBTNodes for less/equal/greater
 *	combiner: merge an existing tree entry with a new one
 *	allocfunc: allocate a new RBTNode
 *	freefunc: free an old RBTNode
 *	arg: passthrough pointer that will be passed to the manipulation functions
 *
 * The freefunc should just be pfree or equivalent; it can be NULL (None) if the
 * caller doesn't require retail space reclamation.
 *
 * The RBTree node is palloc'd in the caller's memory context.  Note that
 * all contents of the tree are actually allocated by the caller, not here.
 */
pub unsafe fn rbt_create(
    node_size: Size,
    comparator: rbt_comparator,
    combiner: rbt_combiner,
    allocfunc: rbt_allocfunc,
    freefunc: Option<rbt_freefunc>,
    arg: *mut c_void,
) -> *mut RBTree {
    let tree = palloc(core::mem::size_of::<RBTree>()) as *mut RBTree;

    Assert!(node_size > core::mem::size_of::<RBTNode>());

    (*tree).root = RBTNIL();
    (*tree).node_size = node_size;
    (*tree).comparator = comparator;
    (*tree).combiner = combiner;
    (*tree).allocfunc = allocfunc;
    (*tree).freefunc = freefunc;

    (*tree).arg = arg;

    tree
}

/* Copy the additional data fields from one RBTNode to another */
#[inline]
unsafe fn rbt_copy_data(rbt: *mut RBTree, dest: *mut RBTNode, src: *const RBTNode) {
    core::ptr::copy_nonoverlapping(
        src.add(1) as *const u8,
        dest.add(1) as *mut u8,
        (*rbt).node_size - core::mem::size_of::<RBTNode>(),
    );
}

/**********************************************************************
 *						  Search									  *
 **********************************************************************/

/*
 * rbt_find: search for a value in an RBTree
 *
 * Returns the matching tree entry, or NULL if no match is found.
 */
pub unsafe fn rbt_find(rbt: *mut RBTree, data: *const RBTNode) -> *mut RBTNode {
    let mut node = (*rbt).root;

    while node != RBTNIL() {
        let cmp = ((*rbt).comparator)(data, node, (*rbt).arg);

        if cmp == 0 {
            return node;
        } else if cmp < 0 {
            node = (*node).left;
        } else {
            node = (*node).right;
        }
    }

    core::ptr::null_mut()
}

/*
 * rbt_find_great: search for a greater value in an RBTree
 *
 * If equal_match is true, this will be a great or equal search.
 */
pub unsafe fn rbt_find_great(
    rbt: *mut RBTree,
    data: *const RBTNode,
    equal_match: bool,
) -> *mut RBTNode {
    let mut node = (*rbt).root;
    let mut greater: *mut RBTNode = core::ptr::null_mut();

    while node != RBTNIL() {
        let cmp = ((*rbt).comparator)(data, node, (*rbt).arg);

        if equal_match && cmp == 0 {
            return node;
        } else if cmp < 0 {
            greater = node;
            node = (*node).left;
        } else {
            node = (*node).right;
        }
    }

    greater
}

/*
 * rbt_find_less: search for a lesser value in an RBTree
 *
 * If equal_match is true, this will be a less or equal search.
 */
pub unsafe fn rbt_find_less(
    rbt: *mut RBTree,
    data: *const RBTNode,
    equal_match: bool,
) -> *mut RBTNode {
    let mut node = (*rbt).root;
    let mut lesser: *mut RBTNode = core::ptr::null_mut();

    while node != RBTNIL() {
        let cmp = ((*rbt).comparator)(data, node, (*rbt).arg);

        if equal_match && cmp == 0 {
            return node;
        } else if cmp > 0 {
            lesser = node;
            node = (*node).right;
        } else {
            node = (*node).left;
        }
    }

    lesser
}

/*
 * rbt_leftmost: fetch the leftmost (smallest-valued) tree node.
 * Returns NULL if tree is empty.
 */
pub unsafe fn rbt_leftmost(rbt: *mut RBTree) -> *mut RBTNode {
    let mut node = (*rbt).root;
    let mut leftmost = (*rbt).root;

    while node != RBTNIL() {
        leftmost = node;
        node = (*node).left;
    }

    if leftmost != RBTNIL() {
        return leftmost;
    }

    core::ptr::null_mut()
}

/**********************************************************************
 *							  Insertion								  *
 **********************************************************************/

/*
 * Rotate node x to left.
 *
 * x's right child takes its place in the tree, and x becomes the left
 * child of that node.
 */
unsafe fn rbt_rotate_left(rbt: *mut RBTree, x: *mut RBTNode) {
    let y = (*x).right;

    /* establish x->right link */
    (*x).right = (*y).left;
    if (*y).left != RBTNIL() {
        (*(*y).left).parent = x;
    }

    /* establish y->parent link */
    if y != RBTNIL() {
        (*y).parent = (*x).parent;
    }
    if !(*x).parent.is_null() {
        if x == (*(*x).parent).left {
            (*(*x).parent).left = y;
        } else {
            (*(*x).parent).right = y;
        }
    } else {
        (*rbt).root = y;
    }

    /* link x and y */
    (*y).left = x;
    if x != RBTNIL() {
        (*x).parent = y;
    }
}

/*
 * Rotate node x to right.
 *
 * x's left right child takes its place in the tree, and x becomes the right
 * child of that node.
 */
unsafe fn rbt_rotate_right(rbt: *mut RBTree, x: *mut RBTNode) {
    let y = (*x).left;

    /* establish x->left link */
    (*x).left = (*y).right;
    if (*y).right != RBTNIL() {
        (*(*y).right).parent = x;
    }

    /* establish y->parent link */
    if y != RBTNIL() {
        (*y).parent = (*x).parent;
    }
    if !(*x).parent.is_null() {
        if x == (*(*x).parent).right {
            (*(*x).parent).right = y;
        } else {
            (*(*x).parent).left = y;
        }
    } else {
        (*rbt).root = y;
    }

    /* link x and y */
    (*y).right = x;
    if x != RBTNIL() {
        (*x).parent = y;
    }
}

/*
 * Maintain Red-Black tree balance after inserting node x.
 *
 * The newly inserted node is always initially marked red.  That may lead to
 * a situation where a red node has a red child, which is prohibited.  We can
 * always fix the problem by a series of color changes and/or "rotations".
 */
unsafe fn rbt_insert_fixup(rbt: *mut RBTree, mut x: *mut RBTNode) {
    /*
     * x is always a red node.  Initially, it is the newly inserted node. Each
     * iteration of this loop moves it higher up in the tree.
     */
    while x != (*rbt).root && (*(*x).parent).color == RBTRED {
        /*
         * x and x->parent are both red.  Fix depends on whether x->parent is
         * a left or right child.  In either case, we define y to be the
         * "uncle" of x, that is, the other child of x's grandparent.
         */
        if (*x).parent == (*(*(*x).parent).parent).left {
            let y = (*(*(*x).parent).parent).right;

            if (*y).color == RBTRED {
                /* uncle is RBTRED */
                (*(*x).parent).color = RBTBLACK;
                (*y).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                x = (*(*x).parent).parent;
            } else {
                /* uncle is RBTBLACK */
                if x == (*(*x).parent).right {
                    /* make x a left child */
                    x = (*x).parent;
                    rbt_rotate_left(rbt, x);
                }

                /* recolor and rotate */
                (*(*x).parent).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                rbt_rotate_right(rbt, (*(*x).parent).parent);
            }
        } else {
            /* mirror image of above code */
            let y = (*(*(*x).parent).parent).left;

            if (*y).color == RBTRED {
                /* uncle is RBTRED */
                (*(*x).parent).color = RBTBLACK;
                (*y).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                x = (*(*x).parent).parent;
            } else {
                /* uncle is RBTBLACK */
                if x == (*(*x).parent).left {
                    x = (*x).parent;
                    rbt_rotate_right(rbt, x);
                }
                (*(*x).parent).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                rbt_rotate_left(rbt, (*(*x).parent).parent);
            }
        }
    }

    /*
     * The root may already have been black; if not, the black-height of every
     * node in the tree increases by one.
     */
    (*(*rbt).root).color = RBTBLACK;
}

/*
 * rbt_insert: insert a new value into the tree.
 *
 * If the value represented by "data" is not present in the tree, then we copy
 * "data" into a new tree entry and return that node, setting *isNew to true.
 *
 * If the value represented by "data" is already present, then we call the
 * combiner function to merge data into the existing node, and return the
 * existing node, setting *isNew to false.
 */
pub unsafe fn rbt_insert(
    rbt: *mut RBTree,
    data: *const RBTNode,
    isNew: *mut bool,
) -> *mut RBTNode {
    let mut current: *mut RBTNode;
    let mut parent: *mut RBTNode;
    let x: *mut RBTNode;
    let mut cmp: c_int;

    /* find where node belongs */
    current = (*rbt).root;
    parent = core::ptr::null_mut();
    cmp = 0; /* just to prevent compiler warning */

    while current != RBTNIL() {
        cmp = ((*rbt).comparator)(data, current, (*rbt).arg);
        if cmp == 0 {
            /*
             * Found node with given key.  Apply combiner.
             */
            ((*rbt).combiner)(current, data, (*rbt).arg);
            *isNew = false;
            return current;
        }
        parent = current;
        current = if cmp < 0 { (*current).left } else { (*current).right };
    }

    /*
     * Value is not present, so create a new node containing data.
     */
    *isNew = true;

    x = ((*rbt).allocfunc)((*rbt).arg);

    (*x).color = RBTRED;

    (*x).left = RBTNIL();
    (*x).right = RBTNIL();
    (*x).parent = parent;
    rbt_copy_data(rbt, x, data);

    /* insert node in tree */
    if !parent.is_null() {
        if cmp < 0 {
            (*parent).left = x;
        } else {
            (*parent).right = x;
        }
    } else {
        (*rbt).root = x;
    }

    rbt_insert_fixup(rbt, x);

    x
}

/**********************************************************************
 *							Deletion								  *
 **********************************************************************/

/*
 * Maintain Red-Black tree balance after deleting a black node.
 */
unsafe fn rbt_delete_fixup(rbt: *mut RBTree, mut x: *mut RBTNode) {
    /*
     * x is always a black node.  Initially, it is the former child of the
     * deleted node.  Each iteration of this loop moves it higher up in the
     * tree.
     */
    while x != (*rbt).root && (*x).color == RBTBLACK {
        /*
         * Left and right cases are symmetric.  Any nodes that are children of
         * x have a black-height one less than the remainder of the nodes in
         * the tree.  We rotate and recolor nodes to move the problem up the
         * tree: at some stage we'll either fix the problem, or reach the root.
         */
        if x == (*(*x).parent).left {
            let mut w = (*(*x).parent).right;

            if (*w).color == RBTRED {
                (*w).color = RBTBLACK;
                (*(*x).parent).color = RBTRED;

                rbt_rotate_left(rbt, (*x).parent);
                w = (*(*x).parent).right;
            }

            if (*(*w).left).color == RBTBLACK && (*(*w).right).color == RBTBLACK {
                (*w).color = RBTRED;

                x = (*x).parent;
            } else {
                if (*(*w).right).color == RBTBLACK {
                    (*(*w).left).color = RBTBLACK;
                    (*w).color = RBTRED;

                    rbt_rotate_right(rbt, w);
                    w = (*(*x).parent).right;
                }
                (*w).color = (*(*x).parent).color;
                (*(*x).parent).color = RBTBLACK;
                (*(*w).right).color = RBTBLACK;

                rbt_rotate_left(rbt, (*x).parent);
                x = (*rbt).root; /* Arrange for loop to terminate. */
            }
        } else {
            let mut w = (*(*x).parent).left;

            if (*w).color == RBTRED {
                (*w).color = RBTBLACK;
                (*(*x).parent).color = RBTRED;

                rbt_rotate_right(rbt, (*x).parent);
                w = (*(*x).parent).left;
            }

            if (*(*w).right).color == RBTBLACK && (*(*w).left).color == RBTBLACK {
                (*w).color = RBTRED;

                x = (*x).parent;
            } else {
                if (*(*w).left).color == RBTBLACK {
                    (*(*w).right).color = RBTBLACK;
                    (*w).color = RBTRED;

                    rbt_rotate_left(rbt, w);
                    w = (*(*x).parent).left;
                }
                (*w).color = (*(*x).parent).color;
                (*(*x).parent).color = RBTBLACK;
                (*(*w).left).color = RBTBLACK;

                rbt_rotate_right(rbt, (*x).parent);
                x = (*rbt).root; /* Arrange for loop to terminate. */
            }
        }
    }
    (*x).color = RBTBLACK;
}

/*
 * Delete node z from tree.
 */
unsafe fn rbt_delete_node(rbt: *mut RBTree, z: *mut RBTNode) {
    let x: *mut RBTNode;
    let y: *mut RBTNode;

    /* This is just paranoia: we should only get called on a valid node */
    if z.is_null() || z == RBTNIL() {
        return;
    }

    /*
     * y is the node that will actually be removed from the tree.  This will
     * be z if z has fewer than two children, or the tree successor of z
     * otherwise.
     */
    if (*z).left == RBTNIL() || (*z).right == RBTNIL() {
        /* y has a RBTNIL node as a child */
        y = z;
    } else {
        /* find tree successor */
        let mut yy = (*z).right;
        while (*yy).left != RBTNIL() {
            yy = (*yy).left;
        }
        y = yy;
    }

    /* x is y's only child */
    if (*y).left != RBTNIL() {
        x = (*y).left;
    } else {
        x = (*y).right;
    }

    /* Remove y from the tree. */
    (*x).parent = (*y).parent;
    if !(*y).parent.is_null() {
        if y == (*(*y).parent).left {
            (*(*y).parent).left = x;
        } else {
            (*(*y).parent).right = x;
        }
    } else {
        (*rbt).root = x;
    }

    /*
     * If we removed the tree successor of z rather than z itself, then move
     * the data for the removed node to the one we were supposed to remove.
     */
    if y != z {
        rbt_copy_data(rbt, z, y);
    }

    /*
     * Removing a black node might make some paths from root to leaf contain
     * fewer black nodes than others, or it might make two red nodes adjacent.
     */
    if (*y).color == RBTBLACK {
        rbt_delete_fixup(rbt, x);
    }

    /* Now we can recycle the y node */
    if let Some(freefunc) = (*rbt).freefunc {
        freefunc(y, (*rbt).arg);
    }
}

/*
 * rbt_delete: remove the given tree entry
 *
 * "node" must have previously been found via rbt_find or rbt_leftmost.
 */
pub unsafe fn rbt_delete(rbt: *mut RBTree, node: *mut RBTNode) {
    rbt_delete_node(rbt, node);
}

/**********************************************************************
 *						  Traverse									  *
 **********************************************************************/

unsafe fn rbt_left_right_iterator(iter: *mut RBTreeIterator) -> *mut RBTNode {
    if (*iter).last_visited.is_null() {
        (*iter).last_visited = (*(*iter).rbt).root;
        while (*(*iter).last_visited).left != RBTNIL() {
            (*iter).last_visited = (*(*iter).last_visited).left;
        }

        return (*iter).last_visited;
    }

    if (*(*iter).last_visited).right != RBTNIL() {
        (*iter).last_visited = (*(*iter).last_visited).right;
        while (*(*iter).last_visited).left != RBTNIL() {
            (*iter).last_visited = (*(*iter).last_visited).left;
        }

        return (*iter).last_visited;
    }

    loop {
        let came_from = (*iter).last_visited;

        (*iter).last_visited = (*(*iter).last_visited).parent;
        if (*iter).last_visited.is_null() {
            (*iter).is_over = true;
            break;
        }

        if (*(*iter).last_visited).left == came_from {
            break; /* came from left sub-tree, return current node */
        }

        /* else - came from right sub-tree, continue to move up */
    }

    (*iter).last_visited
}

unsafe fn rbt_right_left_iterator(iter: *mut RBTreeIterator) -> *mut RBTNode {
    if (*iter).last_visited.is_null() {
        (*iter).last_visited = (*(*iter).rbt).root;
        while (*(*iter).last_visited).right != RBTNIL() {
            (*iter).last_visited = (*(*iter).last_visited).right;
        }

        return (*iter).last_visited;
    }

    if (*(*iter).last_visited).left != RBTNIL() {
        (*iter).last_visited = (*(*iter).last_visited).left;
        while (*(*iter).last_visited).right != RBTNIL() {
            (*iter).last_visited = (*(*iter).last_visited).right;
        }

        return (*iter).last_visited;
    }

    loop {
        let came_from = (*iter).last_visited;

        (*iter).last_visited = (*(*iter).last_visited).parent;
        if (*iter).last_visited.is_null() {
            (*iter).is_over = true;
            break;
        }

        if (*(*iter).last_visited).right == came_from {
            break; /* came from right sub-tree, return current node */
        }

        /* else - came from left sub-tree, continue to move up */
    }

    (*iter).last_visited
}

/*
 * rbt_begin_iterate: prepare to traverse the tree in any of several orders
 *
 * After calling rbt_begin_iterate, call rbt_iterate repeatedly until it
 * returns NULL or the traversal stops being of interest.
 */
pub unsafe fn rbt_begin_iterate(rbt: *mut RBTree, ctrl: RBTOrderControl, iter: *mut RBTreeIterator) {
    /* Common initialization for all traversal orders */
    (*iter).rbt = rbt;
    (*iter).last_visited = core::ptr::null_mut();
    (*iter).is_over = (*rbt).root == RBTNIL();

    match ctrl {
        LeftRightWalk => {
            /* visit left, then self, then right */
            (*iter).iterate = Some(rbt_left_right_iterator);
        }
        RightLeftWalk => {
            /* visit right, then self, then left */
            (*iter).iterate = Some(rbt_right_left_iterator);
        }
    }
}

/*
 * rbt_iterate: return the next node in traversal order, or NULL if no more
 */
pub unsafe fn rbt_iterate(iter: *mut RBTreeIterator) -> *mut RBTNode {
    if (*iter).is_over {
        return core::ptr::null_mut();
    }

    ((*iter).iterate.unwrap())(iter)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A tree entry: RBTNode embedded as the first field (the intrusive pattern).
    #[repr(C)]
    struct IntNode {
        rbtnode: RBTNode,
        key: i32,
    }

    unsafe fn cmp(a: *const RBTNode, b: *const RBTNode, _arg: *mut c_void) -> c_int {
        let ka = (*(a as *const IntNode)).key;
        let kb = (*(b as *const IntNode)).key;
        (ka - kb) as c_int
    }
    unsafe fn combine(_existing: *mut RBTNode, _newdata: *const RBTNode, _arg: *mut c_void) {}
    unsafe fn alloc(_arg: *mut c_void) -> *mut RBTNode {
        palloc(core::mem::size_of::<IntNode>()) as *mut RBTNode
    }
    unsafe fn free(x: *mut RBTNode, _arg: *mut c_void) {
        pfree(x as *mut c_void);
    }

    #[test]
    fn insert_find_iterate_delete() {
        unsafe {
            let rbt = rbt_create(
                core::mem::size_of::<IntNode>(),
                cmp,
                combine,
                alloc,
                Some(free),
                core::ptr::null_mut(),
            );

            // Insert keys in a scrambled order.
            let keys = [50, 20, 80, 10, 30, 70, 90, 5, 15, 25, 35, 60, 75, 85, 95];
            for &k in keys.iter() {
                let probe = IntNode {
                    rbtnode: RBTNode {
                        color: 0,
                        left: core::ptr::null_mut(),
                        right: core::ptr::null_mut(),
                        parent: core::ptr::null_mut(),
                    },
                    key: k,
                };
                let mut is_new = false;
                rbt_insert(rbt, &probe as *const IntNode as *const RBTNode, &mut is_new);
                assert!(is_new);
            }

            // Find an existing and a missing key.
            let probe = IntNode {
                rbtnode: RBTNode {
                    color: 0,
                    left: core::ptr::null_mut(),
                    right: core::ptr::null_mut(),
                    parent: core::ptr::null_mut(),
                },
                key: 35,
            };
            let found = rbt_find(rbt, &probe as *const IntNode as *const RBTNode);
            assert!(!found.is_null());
            assert_eq!((*(found as *const IntNode)).key, 35);

            let mut miss = probe;
            miss.key = 999;
            assert!(rbt_find(rbt, &miss as *const IntNode as *const RBTNode).is_null());

            // In-order traversal must yield sorted keys.
            let mut iter = RBTreeIterator {
                rbt: core::ptr::null_mut(),
                iterate: None,
                last_visited: core::ptr::null_mut(),
                is_over: false,
            };
            rbt_begin_iterate(rbt, LeftRightWalk, &mut iter);
            let mut out = Vec::new();
            loop {
                let n = rbt_iterate(&mut iter);
                if n.is_null() {
                    break;
                }
                out.push((*(n as *const IntNode)).key);
            }
            let mut sorted = keys.to_vec();
            sorted.sort();
            assert_eq!(out, sorted);

            // Delete the leftmost few and confirm order/size shrink.
            for _ in 0..5 {
                let lm = rbt_leftmost(rbt);
                assert!(!lm.is_null());
                rbt_delete(rbt, lm);
            }
            rbt_begin_iterate(rbt, LeftRightWalk, &mut iter);
            let mut out2 = Vec::new();
            loop {
                let n = rbt_iterate(&mut iter);
                if n.is_null() {
                    break;
                }
                out2.push((*(n as *const IntNode)).key);
            }
            assert_eq!(out2, sorted[5..].to_vec());
        }
    }
}
