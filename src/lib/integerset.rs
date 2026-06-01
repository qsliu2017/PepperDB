//! Translation of postgres/src/include/lib/integerset.h
//!                + postgres/src/backend/lib/integerset.c
//!
//! Data structure to hold a large set of 64-bit integers efficiently
//!
//! IntegerSet provides an in-memory data structure to hold a set of
//! arbitrary 64-bit integers.  Internally, the values are stored in a
//! B-tree, with a special packed representation at the leaf level using
//! the Simple-8b algorithm, which can pack clusters of nearby values
//! very tightly.
//!
//! Memory consumption depends on the number of values stored, but also
//! on how far the values are from each other.  In the best case, with
//! long runs of consecutive integers, memory consumption can be as low as
//! 0.1 bytes per integer.  In the worst case, if integers are more than
//! 2^32 apart, it uses about 8 bytes per integer.  In typical use, the
//! consumption per integer is somewhere between those extremes, depending
//! on the range of integers stored, and how "clustered" they are.
//!
//!
//! Interface
//! ---------
//!
//!	intset_create			- Create a new, empty set
//!	intset_add_member		- Add an integer to the set
//!	intset_is_member		- Test if an integer is in the set
//!	intset_begin_iterate	- Begin iterating through all integers in set
//!	intset_iterate_next		- Return next set member, if any
//!
//! intset_create() creates the set in the current memory context.  Subsequent
//! operations that add to the data structure will continue to allocate from
//! that same context, even if it's not current anymore.
//!
//! Note that there is no function to free an integer set.  If you need to do
//! that, create a dedicated memory context to hold it, and destroy the memory
//! context instead.
//!
//!
//! Limitations
//! -----------
//!
//! - Values must be added in order.  (Random insertions would require
//!   splitting nodes, which hasn't been implemented.)
//!
//! - Values cannot be added while iteration is in progress.
//!
//! - No support for removing values.
//!
//! None of these limitations are fundamental to the data structure, so they
//! could be lifted if needed, by writing some new code.  But the current
//! users of this facility don't need them.
//!
//!
//! References
//! ----------
//!
//! Simple-8b encoding is based on:
//!
//! Vo Ngoc Anh, Alistair Moffat, Index compression using 64-bit words,
//!   Software - Practice & Experience, v.40 n.2, p.131-147, February 2010
//!   (https://doi.org/10.1002/spe.948)
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/lib/integerset.c

use crate::prelude::*;
use core::ffi::{c_int, c_void};
use core::ptr::null_mut;

/*
 * Maximum number of integers that can be encoded in a single Simple-8b
 * codeword. (Defined here before anything else, so that we can size arrays
 * using this.)
 */
const SIMPLE8B_MAX_VALUES_PER_CODEWORD: usize = 240;

/*
 * Parameters for shape of the in-memory B-tree.
 *
 * These set the size of each internal and leaf node.  They don't necessarily
 * need to be the same, because the tree is just an in-memory structure.
 * With the default 64, each node is about 1 kb.
 *
 * If you change these, you must recalculate MAX_TREE_LEVELS, too!
 */
const MAX_INTERNAL_ITEMS: usize = 64;
const MAX_LEAF_ITEMS: usize = 64;

/*
 * Maximum height of the tree.
 *
 * MAX_TREE_ITEMS is calculated from the "fan-out" of the B-tree.  The
 * theoretical maximum number of items that we can store in a set is 2^64,
 * so MAX_TREE_LEVELS should be set so that:
 *
 *   MAX_LEAF_ITEMS * MAX_INTERNAL_ITEMS ^ (MAX_TREE_LEVELS - 1) >= 2^64.
 *
 * In practice, we'll need far fewer levels, because you will run out of
 * memory long before reaching that number, but let's be conservative.
 */
const MAX_TREE_LEVELS: usize = 11;

/*
 * Node structures, for the in-memory B-tree.
 *
 * An internal node holds a number of downlink pointers to leaf nodes, or
 * to internal nodes on a lower level.  For each downlink, the key value
 * corresponding to the lower level node is stored in a sorted array.  The
 * stored key values are low keys.  In other words, if the downlink has value
 * X, then all items stored on that child are >= X.
 *
 * Each leaf node holds a number of "items", with a varying number of
 * integers packed into each item.  Each item consists of two 64-bit words:
 * The first word holds the first integer stored in the item, in plain format.
 * The second word contains between 0 and 240 more integers, packed using
 * Simple-8b encoding.  By storing the first integer in plain, unpacked,
 * format, we can use binary search to quickly find an item that holds (or
 * would hold) a particular integer.  And by storing the rest in packed form,
 * we still get pretty good memory density, if there are clusters of integers
 * with similar values.
 *
 * Each leaf node also has a pointer to the next leaf node, so that the leaf
 * nodes can be easily walked from beginning to end when iterating.
 */

/* Common structure of both leaf and internal nodes. */
#[repr(C)]
pub struct intset_node {
    pub level: uint16,     /* tree level of this node */
    pub num_items: uint16, /* number of items in this node */
}

/* Internal node */
#[repr(C)]
pub struct intset_internal_node {
    /* common header, must match intset_node */
    pub level: uint16, /* >= 1 on internal nodes */
    pub num_items: uint16,

    /*
     * 'values' is an array of key values, and 'downlinks' are pointers to
     * lower-level nodes, corresponding to the key values.
     */
    pub values: [uint64; MAX_INTERNAL_ITEMS],
    pub downlinks: [*mut intset_node; MAX_INTERNAL_ITEMS],
}

/* Leaf node */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct leaf_item {
    pub first: uint64,    /* first integer in this item */
    pub codeword: uint64, /* simple8b encoded differences from 'first' */
}

const MAX_VALUES_PER_LEAF_ITEM: usize = 1 + SIMPLE8B_MAX_VALUES_PER_CODEWORD;

#[repr(C)]
pub struct intset_leaf_node {
    /* common header, must match intset_node */
    pub level: uint16, /* 0 on leafs */
    pub num_items: uint16,

    pub next: *mut intset_leaf_node, /* right sibling, if any */

    pub items: [leaf_item; MAX_LEAF_ITEMS],
}

/*
 * We buffer insertions in a simple array, before packing and inserting them
 * into the B-tree.  MAX_BUFFERED_VALUES sets the size of the buffer.  The
 * encoder assumes that it is large enough that we can always fill a leaf
 * item with buffered new items.  In other words, MAX_BUFFERED_VALUES must be
 * larger than MAX_VALUES_PER_LEAF_ITEM.  For efficiency, make it much larger.
 */
const MAX_BUFFERED_VALUES: usize = MAX_VALUES_PER_LEAF_ITEM * 2;

/*
 * IntegerSet is the top-level object representing the set.
 *
 * The integers are stored in an in-memory B-tree structure, plus an array
 * for newly-added integers.  IntegerSet also tracks information about memory
 * usage, as well as the current position when iterating the set with
 * intset_begin_iterate / intset_iterate_next.
 */
#[repr(C)]
pub struct IntegerSet {
    /*
     * 'context' is the memory context holding this integer set and all its
     * tree nodes.
     *
     * 'mem_used' tracks the amount of memory used.  We don't do anything with
     * it in integerset.c itself, but the callers can ask for it with
     * intset_memory_usage().
     */
    pub context: MemoryContext,
    pub mem_used: uint64,

    pub num_entries: uint64,  /* total # of values in the set */
    pub highest_value: uint64, /* highest value stored in this set */

    /*
     * B-tree to hold the packed values.
     *
     * 'rightmost_nodes' hold pointers to the rightmost node on each level.
     * rightmost_parent[0] is rightmost leaf, rightmost_parent[1] is its
     * parent, and so forth, all the way up to the root. These are needed when
     * adding new values. (Currently, we require that new values are added at
     * the end.)
     */
    pub num_levels: c_int, /* height of the tree */
    pub root: *mut intset_node, /* root node */
    pub rightmost_nodes: [*mut intset_node; MAX_TREE_LEVELS],
    pub leftmost_leaf: *mut intset_leaf_node, /* leftmost leaf node */

    /*
     * Holding area for new items that haven't been inserted to the tree yet.
     */
    pub buffered_values: [uint64; MAX_BUFFERED_VALUES],
    pub num_buffered_values: c_int,

    /*
     * Iterator support.
     *
     * 'iter_values' is an array of integers ready to be returned to the
     * caller; 'iter_num_values' is the length of that array, and
     * 'iter_valueno' is the next index.  'iter_node' and 'iter_itemno' point
     * to the leaf node, and item within the leaf node, to get the next batch
     * of values from.
     *
     * Normally, 'iter_values' points to 'iter_values_buf', which holds items
     * decoded from a leaf item.  But after we have scanned the whole B-tree,
     * we iterate through all the unbuffered values, too, by pointing
     * iter_values to 'buffered_values'.
     */
    pub iter_active: bool, /* is iteration in progress? */

    pub iter_values: *const uint64,
    pub iter_num_values: c_int, /* number of elements in 'iter_values' */
    pub iter_valueno: c_int,    /* next index into 'iter_values' */

    pub iter_node: *mut intset_leaf_node, /* current leaf node */
    pub iter_itemno: c_int,               /* next item in 'iter_node' to decode */

    pub iter_values_buf: [uint64; MAX_VALUES_PER_LEAF_ITEM],
}

/*
 * GetMemoryChunkSpace() (utils/mmgr/mcxt.c) returns the actual amount of space
 * a chunk occupies in its context, including the per-chunk bookkeeping header.
 *
 * TODO(pg-port): the PepperDB allocator is a context-less bump allocator with
 * no chunk headers, and GetMemoryChunkSpace is not yet part of the translated
 * palloc/mcxt surface.  We approximate the chunk space by the requested
 * allocation size of the object, which is what the C code allocates for each
 * node and for the IntegerSet itself.  This keeps intset_memory_usage()
 * monotonic and proportional, which is all its (instrumentation-only) callers
 * rely on.
 */
#[inline]
unsafe fn GetMemoryChunkSpace(_pointer: *mut c_void, size: Size) -> Size {
    size
}

/*
 * Create a new, initially empty, integer set.
 *
 * The integer set is created in the current memory context.
 * We will do all subsequent allocations in the same context, too, regardless
 * of which memory context is current when new integers are added to the set.
 */
pub unsafe fn intset_create() -> *mut IntegerSet {
    let intset: *mut IntegerSet;

    intset = palloc(size_of::<IntegerSet>()) as *mut IntegerSet;
    (*intset).context = CurrentMemoryContext;
    (*intset).mem_used =
        GetMemoryChunkSpace(intset as *mut c_void, size_of::<IntegerSet>()) as uint64;

    (*intset).num_entries = 0;
    (*intset).highest_value = 0;

    (*intset).num_levels = 0;
    (*intset).root = null_mut();
    write_bytes_zero(
        (*intset).rightmost_nodes.as_mut_ptr() as *mut u8,
        size_of::<[*mut intset_node; MAX_TREE_LEVELS]>(),
    );
    (*intset).leftmost_leaf = null_mut();

    (*intset).num_buffered_values = 0;

    (*intset).iter_active = false;
    (*intset).iter_node = null_mut();
    (*intset).iter_itemno = 0;
    (*intset).iter_valueno = 0;
    (*intset).iter_num_values = 0;
    (*intset).iter_values = null_mut();

    intset
}

/*
 * Helper for the `memset(ptr, 0, n)` in intset_create.  Mirrors C's memset of
 * the rightmost_nodes array to all-zero (a null pointer is all-zero bits).
 */
#[inline]
unsafe fn write_bytes_zero(ptr: *mut u8, n: Size) {
    ptr.write_bytes(0, n);
}

/*
 * Allocate a new node.
 */
unsafe fn intset_new_internal_node(intset: *mut IntegerSet) -> *mut intset_internal_node {
    let n: *mut intset_internal_node;

    n = MemoryContextAlloc((*intset).context, size_of::<intset_internal_node>())
        as *mut intset_internal_node;
    (*intset).mem_used +=
        GetMemoryChunkSpace(n as *mut c_void, size_of::<intset_internal_node>())
            as uint64;

    (*n).level = 0; /* caller must set */
    (*n).num_items = 0;

    n
}

unsafe fn intset_new_leaf_node(intset: *mut IntegerSet) -> *mut intset_leaf_node {
    let n: *mut intset_leaf_node;

    n = MemoryContextAlloc((*intset).context, size_of::<intset_leaf_node>())
        as *mut intset_leaf_node;
    (*intset).mem_used +=
        GetMemoryChunkSpace(n as *mut c_void, size_of::<intset_leaf_node>()) as uint64;

    (*n).level = 0;
    (*n).num_items = 0;
    (*n).next = null_mut();

    n
}

/*
 * Return the number of entries in the integer set.
 */
pub unsafe fn intset_num_entries(intset: *mut IntegerSet) -> uint64 {
    (*intset).num_entries
}

/*
 * Return the amount of memory used by the integer set.
 */
pub unsafe fn intset_memory_usage(intset: *mut IntegerSet) -> uint64 {
    (*intset).mem_used
}

/*
 * Add a value to the set.
 *
 * Values must be added in order.
 */
pub unsafe fn intset_add_member(intset: *mut IntegerSet, x: uint64) {
    if (*intset).iter_active {
        elog!(
            ERROR,
            "cannot add new values to integer set while iteration is in progress"
        );
    }

    if x <= (*intset).highest_value && (*intset).num_entries > 0 {
        elog!(ERROR, "cannot add value to integer set out of order");
    }

    if (*intset).num_buffered_values as usize >= MAX_BUFFERED_VALUES {
        /* Time to flush our buffer */
        intset_flush_buffered_values(intset);
        Assert!(((*intset).num_buffered_values as usize) < MAX_BUFFERED_VALUES);
    }

    /* Add it to the buffer of newly-added values */
    (*intset).buffered_values[(*intset).num_buffered_values as usize] = x;
    (*intset).num_buffered_values += 1;
    (*intset).num_entries += 1;
    (*intset).highest_value = x;
}

/*
 * Take a batch of buffered values, and pack them into the B-tree.
 */
unsafe fn intset_flush_buffered_values(intset: *mut IntegerSet) {
    let values: *mut uint64 = (*intset).buffered_values.as_mut_ptr();
    let num_values: uint64 = (*intset).num_buffered_values as uint64;
    let mut num_packed: c_int = 0;
    let mut leaf: *mut intset_leaf_node;

    leaf = (*intset).rightmost_nodes[0] as *mut intset_leaf_node;

    /*
     * If the tree is completely empty, create the first leaf page, which is
     * also the root.
     */
    if leaf.is_null() {
        /*
         * This is the very first item in the set.
         *
         * Allocate root node. It's also a leaf.
         */
        leaf = intset_new_leaf_node(intset);

        (*intset).root = leaf as *mut intset_node;
        (*intset).leftmost_leaf = leaf;
        (*intset).rightmost_nodes[0] = leaf as *mut intset_node;
        (*intset).num_levels = 1;
    }

    /*
     * If there are less than MAX_VALUES_PER_LEAF_ITEM values in the buffer,
     * stop.  In most cases, we cannot encode that many values in a single
     * value, but this way, the encoder doesn't have to worry about running
     * out of input.
     */
    while num_values - (num_packed as uint64) >= MAX_VALUES_PER_LEAF_ITEM as uint64 {
        let mut item: leaf_item = leaf_item {
            first: 0,
            codeword: 0,
        };
        let mut num_encoded: c_int = 0;

        /*
         * Construct the next leaf item, packing as many buffered values as
         * possible.
         */
        item.first = *values.add(num_packed as usize);
        item.codeword = simple8b_encode(
            values.add((num_packed + 1) as usize),
            &mut num_encoded,
            item.first,
        );

        /*
         * Add the item to the node, allocating a new node if the old one is
         * full.
         */
        if (*leaf).num_items as usize >= MAX_LEAF_ITEMS {
            /* Allocate new leaf and link it to the tree */
            let old_leaf: *mut intset_leaf_node = leaf;

            leaf = intset_new_leaf_node(intset);
            (*old_leaf).next = leaf;
            (*intset).rightmost_nodes[0] = leaf as *mut intset_node;
            intset_update_upper(intset, 1, leaf as *mut intset_node, item.first);
        }
        (*leaf).items[(*leaf).num_items as usize] = item;
        (*leaf).num_items += 1;

        num_packed += 1 + num_encoded;
    }

    /*
     * Move any remaining buffered values to the beginning of the array.
     */
    if num_packed < (*intset).num_buffered_values {
        core::ptr::copy(
            (*intset).buffered_values.as_ptr().add(num_packed as usize),
            (*intset).buffered_values.as_mut_ptr(),
            ((*intset).num_buffered_values - num_packed) as usize,
        );
    }
    (*intset).num_buffered_values -= num_packed;
}

/*
 * Insert a downlink into parent node, after creating a new node.
 *
 * Recurses if the parent node is full, too.
 */
unsafe fn intset_update_upper(
    intset: *mut IntegerSet,
    level: c_int,
    child: *mut intset_node,
    child_key: uint64,
) {
    let mut parent: *mut intset_internal_node;

    Assert!(level > 0);

    /*
     * Create a new root node, if necessary.
     */
    if level >= (*intset).num_levels {
        let oldroot: *mut intset_node = (*intset).root;
        let downlink_key: uint64;

        /* MAX_TREE_LEVELS should be more than enough, this shouldn't happen */
        if (*intset).num_levels == MAX_TREE_LEVELS as c_int {
            elog!(
                ERROR,
                "could not expand integer set, maximum number of levels reached"
            );
        }
        (*intset).num_levels += 1;

        /*
         * Get the first value on the old root page, to be used as the
         * downlink.
         */
        if (*(*intset).root).level == 0 {
            downlink_key = (*(oldroot as *mut intset_leaf_node)).items[0].first;
        } else {
            downlink_key = (*(oldroot as *mut intset_internal_node)).values[0];
        }

        parent = intset_new_internal_node(intset);
        (*parent).level = level as uint16;
        (*parent).values[0] = downlink_key;
        (*parent).downlinks[0] = oldroot;
        (*parent).num_items = 1;

        (*intset).root = parent as *mut intset_node;
        (*intset).rightmost_nodes[level as usize] = parent as *mut intset_node;
    }

    /*
     * Place the downlink on the parent page.
     */
    parent = (*intset).rightmost_nodes[level as usize] as *mut intset_internal_node;

    if ((*parent).num_items as usize) < MAX_INTERNAL_ITEMS {
        (*parent).values[(*parent).num_items as usize] = child_key;
        (*parent).downlinks[(*parent).num_items as usize] = child;
        (*parent).num_items += 1;
    } else {
        /*
         * Doesn't fit.  Allocate new parent, with the downlink as the first
         * item on it, and recursively insert the downlink to the new parent
         * to the grandparent.
         */
        parent = intset_new_internal_node(intset);
        (*parent).level = level as uint16;
        (*parent).values[0] = child_key;
        (*parent).downlinks[0] = child;
        (*parent).num_items = 1;

        (*intset).rightmost_nodes[level as usize] = parent as *mut intset_node;

        intset_update_upper(intset, level + 1, parent as *mut intset_node, child_key);
    }
}

/*
 * Does the set contain the given value?
 */
pub unsafe fn intset_is_member(intset: *mut IntegerSet, x: uint64) -> bool {
    let mut node: *mut intset_node;
    let leaf: *mut intset_leaf_node;
    let mut level: c_int;
    let mut itemno: c_int;
    let item: *mut leaf_item;

    /*
     * The value might be in the buffer of newly-added values.
     */
    if (*intset).num_buffered_values > 0 && x >= (*intset).buffered_values[0] {
        itemno = intset_binsrch_uint64(
            x,
            (*intset).buffered_values.as_mut_ptr(),
            (*intset).num_buffered_values,
            false,
        );
        if itemno >= (*intset).num_buffered_values {
            return false;
        } else {
            return (*intset).buffered_values[itemno as usize] == x;
        }
    }

    /*
     * Start from the root, and walk down the B-tree to find the right leaf
     * node.
     */
    if (*intset).root.is_null() {
        return false;
    }
    node = (*intset).root;
    level = (*intset).num_levels - 1;
    while level > 0 {
        let n: *mut intset_internal_node = node as *mut intset_internal_node;

        Assert!((*node).level as c_int == level);

        itemno = intset_binsrch_uint64(x, (*n).values.as_mut_ptr(), (*n).num_items as c_int, true);
        if itemno == 0 {
            return false;
        }
        node = (*n).downlinks[(itemno - 1) as usize];

        level -= 1;
    }
    Assert!((*node).level == 0);
    leaf = node as *mut intset_leaf_node;

    /*
     * Binary search to find the right item on the leaf page
     */
    itemno = intset_binsrch_leaf(x, (*leaf).items.as_mut_ptr(), (*leaf).num_items as c_int, true);
    if itemno == 0 {
        return false;
    }
    item = &mut (*leaf).items[(itemno - 1) as usize];

    /* Is this a match to the first value on the item? */
    if (*item).first == x {
        return true;
    }
    Assert!(x > (*item).first);

    /* Is it in the packed codeword? */
    if simple8b_contains((*item).codeword, x, (*item).first) {
        return true;
    }

    false
}

/*
 * Begin in-order scan through all the values.
 *
 * While the iteration is in-progress, you cannot add new values to the set.
 */
pub unsafe fn intset_begin_iterate(intset: *mut IntegerSet) {
    /* Note that we allow an iteration to be abandoned midway */
    (*intset).iter_active = true;
    (*intset).iter_node = (*intset).leftmost_leaf;
    (*intset).iter_itemno = 0;
    (*intset).iter_valueno = 0;
    (*intset).iter_num_values = 0;
    (*intset).iter_values = (*intset).iter_values_buf.as_ptr();
}

/*
 * Returns the next integer, when iterating.
 *
 * intset_begin_iterate() must be called first.  intset_iterate_next() returns
 * the next value in the set.  Returns true, if there was another value, and
 * stores the value in *next.  Otherwise, returns false.
 */
pub unsafe fn intset_iterate_next(intset: *mut IntegerSet, next: *mut uint64) -> bool {
    Assert!((*intset).iter_active);
    loop {
        /* Return next iter_values[] entry if any */
        if (*intset).iter_valueno < (*intset).iter_num_values {
            *next = *(*intset).iter_values.add((*intset).iter_valueno as usize);
            (*intset).iter_valueno += 1;
            return true;
        }

        /* Decode next item in current leaf node, if any */
        if !(*intset).iter_node.is_null()
            && (*intset).iter_itemno < (*(*intset).iter_node).num_items as c_int
        {
            let item: *mut leaf_item;
            let num_decoded: c_int;

            item = &mut (*(*intset).iter_node).items[(*intset).iter_itemno as usize];
            (*intset).iter_itemno += 1;

            (*intset).iter_values_buf[0] = (*item).first;
            num_decoded = simple8b_decode(
                (*item).codeword,
                (*intset).iter_values_buf.as_mut_ptr().add(1),
                (*item).first,
            );
            (*intset).iter_num_values = num_decoded + 1;
            (*intset).iter_valueno = 0;
            continue;
        }

        /* No more items on this leaf, step to next node */
        if !(*intset).iter_node.is_null() {
            (*intset).iter_node = (*(*intset).iter_node).next;
            (*intset).iter_itemno = 0;
            continue;
        }

        /*
         * We have reached the end of the B-tree.  But we might still have
         * some integers in the buffer of newly-added values.
         */
        if (*intset).iter_values == (*intset).iter_values_buf.as_ptr() as *const uint64 {
            (*intset).iter_values = (*intset).buffered_values.as_ptr();
            (*intset).iter_num_values = (*intset).num_buffered_values;
            (*intset).iter_valueno = 0;
            continue;
        }

        break;
    }

    /* No more results. */
    (*intset).iter_active = false;
    *next = 0; /* prevent uninitialized-variable warnings */
    false
}

/*
 * intset_binsrch_uint64() -- search a sorted array of uint64s
 *
 * Returns the first position with key equal or less than the given key.
 * The returned position would be the "insert" location for the given key,
 * that is, the position where the new key should be inserted to.
 *
 * 'nextkey' affects the behavior on equal keys.  If true, and there is an
 * equal key in the array, this returns the position immediately after the
 * equal key.  If false, this returns the position of the equal key itself.
 */
unsafe fn intset_binsrch_uint64(item: uint64, arr: *mut uint64, arr_elems: c_int, nextkey: bool) -> c_int {
    let mut low: c_int;
    let mut high: c_int;
    let mut mid: c_int;

    low = 0;
    high = arr_elems;
    while high > low {
        mid = low + (high - low) / 2;

        if nextkey {
            if item >= *arr.add(mid as usize) {
                low = mid + 1;
            } else {
                high = mid;
            }
        } else if item > *arr.add(mid as usize) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low
}

/* same, but for an array of leaf items */
unsafe fn intset_binsrch_leaf(item: uint64, arr: *mut leaf_item, arr_elems: c_int, nextkey: bool) -> c_int {
    let mut low: c_int;
    let mut high: c_int;
    let mut mid: c_int;

    low = 0;
    high = arr_elems;
    while high > low {
        mid = low + (high - low) / 2;

        if nextkey {
            if item >= (*arr.add(mid as usize)).first {
                low = mid + 1;
            } else {
                high = mid;
            }
        } else if item > (*arr.add(mid as usize)).first {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low
}

/*
 * Simple-8b encoding.
 *
 * The simple-8b algorithm packs between 1 and 240 integers into 64-bit words,
 * called "codewords".  The number of integers packed into a single codeword
 * depends on the integers being packed; small integers are encoded using
 * fewer bits than large integers.  A single codeword can store a single
 * 60-bit integer, or two 30-bit integers, for example.
 *
 * Since we're storing a unique, sorted, set of integers, we actually encode
 * the *differences* between consecutive integers.  That way, clusters of
 * integers that are close to each other are packed efficiently, regardless
 * of their absolute values.
 *
 * In Simple-8b, each codeword consists of a 4-bit selector, which indicates
 * how many integers are encoded in the codeword, and the encoded integers are
 * packed into the remaining 60 bits.  The selector allows for 16 different
 * ways of using the remaining 60 bits, called "modes".  The number of integers
 * packed into a single codeword in each mode is listed in the simple8b_modes
 * table below.  For example, consider the following codeword:
 *
 *      20-bit integer       20-bit integer       20-bit integer
 * 1101 00000000000000010010 01111010000100100000 00000000000000010100
 * ^
 * selector
 *
 * The selector 1101 is 13 in decimal.  From the modes table below, we see
 * that it means that the codeword encodes three 20-bit integers.  In decimal,
 * those integers are 18, 500000 and 20.  Because we encode deltas rather than
 * absolute values, the actual values that they represent are 18, 500018 and
 * 500038.
 *
 * Modes 0 and 1 are a bit special; they encode a run of 240 or 120 zeroes
 * (which means 240 or 120 consecutive integers, since we're encoding the
 * deltas between integers), without using the rest of the codeword bits
 * for anything.
 *
 * Simple-8b cannot encode integers larger than 60 bits.  Values larger than
 * that are always stored in the 'first' field of a leaf item, never in the
 * packed codeword.  If there is a sequence of integers that are more than
 * 2^60 apart, the codeword will go unused on those items.  To represent that,
 * we use a magic EMPTY_CODEWORD codeword value.
 */
struct simple8b_mode {
    bits_per_int: uint8,
    num_ints: uint8,
}

static SIMPLE8B_MODES: [simple8b_mode; 17] = [
    simple8b_mode { bits_per_int: 0, num_ints: 240 }, /* mode  0: 240 zeroes */
    simple8b_mode { bits_per_int: 0, num_ints: 120 }, /* mode  1: 120 zeroes */
    simple8b_mode { bits_per_int: 1, num_ints: 60 },  /* mode  2: sixty 1-bit integers */
    simple8b_mode { bits_per_int: 2, num_ints: 30 },  /* mode  3: thirty 2-bit integers */
    simple8b_mode { bits_per_int: 3, num_ints: 20 },  /* mode  4: twenty 3-bit integers */
    simple8b_mode { bits_per_int: 4, num_ints: 15 },  /* mode  5: fifteen 4-bit integers */
    simple8b_mode { bits_per_int: 5, num_ints: 12 },  /* mode  6: twelve 5-bit integers */
    simple8b_mode { bits_per_int: 6, num_ints: 10 },  /* mode  7: ten 6-bit integers */
    simple8b_mode { bits_per_int: 7, num_ints: 8 },   /* mode  8: eight 7-bit integers (four bits
                                                       * are wasted) */
    simple8b_mode { bits_per_int: 8, num_ints: 7 },   /* mode  9: seven 8-bit integers (four bits
                                                       * are wasted) */
    simple8b_mode { bits_per_int: 10, num_ints: 6 },  /* mode 10: six 10-bit integers */
    simple8b_mode { bits_per_int: 12, num_ints: 5 },  /* mode 11: five 12-bit integers */
    simple8b_mode { bits_per_int: 15, num_ints: 4 },  /* mode 12: four 15-bit integers */
    simple8b_mode { bits_per_int: 20, num_ints: 3 },  /* mode 13: three 20-bit integers */
    simple8b_mode { bits_per_int: 30, num_ints: 2 },  /* mode 14: two 30-bit integers */
    simple8b_mode { bits_per_int: 60, num_ints: 1 },  /* mode 15: one 60-bit integer */
    simple8b_mode { bits_per_int: 0, num_ints: 0 },   /* sentinel value */
];

/*
 * EMPTY_CODEWORD is a special value, used to indicate "no values".
 * It is used if the next value is too large to be encoded with Simple-8b.
 *
 * This value looks like a mode-0 codeword, but we can distinguish it
 * because a regular mode-0 codeword would have zeroes in the unused bits.
 */
const EMPTY_CODEWORD: uint64 = UINT64CONST(0x0FFFFFFFFFFFFFFF);

/*
 * Encode a number of integers into a Simple-8b codeword.
 *
 * (What we actually encode are deltas between successive integers.
 * "base" is the value before ints[0].)
 *
 * The input array must contain at least SIMPLE8B_MAX_VALUES_PER_CODEWORD
 * elements, ensuring that we can produce a full codeword.
 *
 * Returns the encoded codeword, and sets *num_encoded to the number of
 * input integers that were encoded.  That can be zero, if the first delta
 * is too large to be encoded.
 */
unsafe fn simple8b_encode(ints: *const uint64, num_encoded: *mut c_int, base: uint64) -> uint64 {
    let mut selector: c_int;
    let mut nints: c_int;
    let mut bits: c_int;
    let mut diff: uint64;
    let mut last_val: uint64;
    let mut codeword: uint64;
    let mut i: c_int;

    Assert!(*ints.add(0) > base);

    /*
     * Select the "mode" to use for this codeword.
     *
     * In each iteration, check if the next value can be represented in the
     * current mode we're considering.  If it's too large, then step up the
     * mode to a wider one, and repeat.  If it fits, move on to the next
     * integer.  Repeat until the codeword is full, given the current mode.
     *
     * Note that we don't have any way to represent unused slots in the
     * codeword, so we require each codeword to be "full".  It is always
     * possible to produce a full codeword unless the very first delta is too
     * large to be encoded.  For example, if the first delta is small but the
     * second is too large to be encoded, we'll end up using the last "mode",
     * which has nints == 1.
     */
    selector = 0;
    nints = SIMPLE8B_MODES[0].num_ints as c_int;
    bits = SIMPLE8B_MODES[0].bits_per_int as c_int;
    diff = (*ints.add(0)).wrapping_sub(base).wrapping_sub(1);
    last_val = *ints.add(0);
    i = 0; /* number of deltas we have accepted */
    loop {
        if diff >= (UINT64CONST(1) << bits) {
            /* too large, step up to next mode */
            selector += 1;
            nints = SIMPLE8B_MODES[selector as usize].num_ints as c_int;
            bits = SIMPLE8B_MODES[selector as usize].bits_per_int as c_int;
            /* we might already have accepted enough deltas for this mode */
            if i >= nints {
                break;
            }
        } else {
            /* accept this delta; then done if codeword is full */
            i += 1;
            if i >= nints {
                break;
            }
            /* examine next delta */
            Assert!(*ints.add(i as usize) > last_val);
            diff = (*ints.add(i as usize)).wrapping_sub(last_val).wrapping_sub(1);
            last_val = *ints.add(i as usize);
        }
    }

    if nints == 0 {
        /*
         * The first delta is too large to be encoded with Simple-8b.
         *
         * If there is at least one not-too-large integer in the input, we
         * will encode it using mode 15 (or a more compact mode).  Hence, we
         * can only get here if the *first* delta is >= 2^60.
         */
        Assert!(i == 0);
        *num_encoded = 0;
        return EMPTY_CODEWORD;
    }

    /*
     * Encode the integers using the selected mode.  Note that we shift them
     * into the codeword in reverse order, so that they will come out in the
     * correct order in the decoder.
     */
    codeword = 0;
    if bits > 0 {
        i = nints - 1;
        while i > 0 {
            diff = (*ints.add(i as usize))
                .wrapping_sub(*ints.add((i - 1) as usize))
                .wrapping_sub(1);
            codeword |= diff;
            codeword <<= bits;
            i -= 1;
        }
        diff = (*ints.add(0)).wrapping_sub(base).wrapping_sub(1);
        codeword |= diff;
    }

    /* add selector to the codeword, and return */
    codeword |= (selector as uint64) << 60;

    *num_encoded = nints;
    codeword
}

/*
 * Decode a codeword into an array of integers.
 * Returns the number of integers decoded.
 */
unsafe fn simple8b_decode(codeword: uint64, decoded: *mut uint64, base: uint64) -> c_int {
    let selector: c_int = (codeword >> 60) as c_int;
    let nints: c_int = SIMPLE8B_MODES[selector as usize].num_ints as c_int;
    let bits: c_int = SIMPLE8B_MODES[selector as usize].bits_per_int as c_int;
    let mask: uint64 = (UINT64CONST(1) << bits).wrapping_sub(1);
    let mut curr_value: uint64;
    let mut codeword = codeword;

    if codeword == EMPTY_CODEWORD {
        return 0;
    }

    curr_value = base;
    let mut i: c_int = 0;
    while i < nints {
        let diff: uint64 = codeword & mask;

        curr_value = curr_value.wrapping_add(1).wrapping_add(diff);
        *decoded.add(i as usize) = curr_value;
        codeword >>= bits;

        i += 1;
    }
    nints
}

/*
 * This is very similar to simple8b_decode(), but instead of decoding all
 * the values to an array, it just checks if the given "key" is part of
 * the codeword.
 */
unsafe fn simple8b_contains(codeword: uint64, key: uint64, base: uint64) -> bool {
    let selector: c_int = (codeword >> 60) as c_int;
    let nints: c_int = SIMPLE8B_MODES[selector as usize].num_ints as c_int;
    let bits: c_int = SIMPLE8B_MODES[selector as usize].bits_per_int as c_int;
    let mut codeword = codeword;

    if codeword == EMPTY_CODEWORD {
        return false;
    }

    if bits == 0 {
        /* Special handling for 0-bit cases. */
        return key.wrapping_sub(base) <= nints as uint64;
    } else {
        let mask: uint64 = (UINT64CONST(1) << bits).wrapping_sub(1);
        let mut curr_value: uint64;

        curr_value = base;
        let mut i: c_int = 0;
        while i < nints {
            let diff: uint64 = codeword & mask;

            curr_value = curr_value.wrapping_add(1).wrapping_add(diff);

            if curr_value >= key {
                if curr_value == key {
                    return true;
                } else {
                    return false;
                }
            }

            codeword >>= bits;

            i += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_members_membership_and_iterate() {
        unsafe {
            let s = intset_create();
            // Members MUST be added in ascending order (write-optimized structure).
            let vals: [uint64; 8] = [1, 2, 100, 1000, 1 << 40, (1 << 40) + 1, 1 << 50, u64::MAX / 2];
            for &v in vals.iter() {
                intset_add_member(s, v);
            }
            for &v in vals.iter() {
                assert!(intset_is_member(s, v), "missing {}", v);
            }
            assert!(!intset_is_member(s, 3));
            assert!(!intset_is_member(s, 999));
            assert_eq!(intset_num_entries(s), vals.len() as uint64);

            intset_begin_iterate(s);
            let mut out: Vec<uint64> = Vec::new();
            let mut next: uint64 = 0;
            while intset_iterate_next(s, &mut next) {
                out.push(next);
            }
            assert_eq!(out, vals.to_vec()); // already ascending
        }
    }
}
