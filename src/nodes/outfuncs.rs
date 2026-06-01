//! src/backend/nodes/outfuncs.c
//!
//! Output functions for Postgres tree nodes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::access::attnum::AttrNumber;
use crate::c::Index;
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{List, ListCell};

// Crate-root #[macro_export] macros used unqualified below.
use crate::{current_cell, foreach, IsA};

// ---------------------------------------------------------------------------
// Stub types for nodes referenced by the custom-write functions below.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct StringInfoData;
pub type StringInfo = *mut StringInfoData;

pub type Bitmapset = crate::nodes::bitmapset::Bitmapset;

/* State flag that determines how nodeToStringInternal() should treat location fields */
static mut write_location_fields: bool = false;

// ---------------------------------------------------------------------------
// Local stubs for helper functions whose real definitions live elsewhere.
// ---------------------------------------------------------------------------

unsafe fn appendStringInfoString(_str: StringInfo, _s: *const c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}

unsafe fn appendStringInfoChar(_str: StringInfo, _ch: c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}

unsafe fn initStringInfo(_str: StringInfo) {
    unimplemented!() // TODO: lib/stringinfo.c
}

unsafe fn double_to_shortest_decimal_buf(_d: f64, _buf: *mut c_char) -> c_int {
    unimplemented!() // TODO: common/shortest_dec.c
}

unsafe fn datumGetSize(_value: Datum, _typByVal: bool, _typLen: c_int) -> Size {
    unimplemented!() // TODO: utils/adt/datum.c
}

unsafe fn bms_next_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int {
    unimplemented!() // TODO: nodes/bitmapset.c
}

unsafe fn list_length(_l: *const List) -> c_int {
    unimplemented!() // TODO: nodes/list.c
}

unsafe fn check_stack_depth() {
    unimplemented!() // TODO: tcop/postgres.c
}

unsafe fn GetExtensibleNodeMethods(
    _extnodename: *const c_char,
    _missing_ok: bool,
) -> *const ExtensibleNodeMethods {
    unimplemented!() // TODO: nodes/extensible.c
}

#[repr(C)]
pub struct ExtensibleNodeMethods {
    pub extnodename: *const c_char,
    pub node_size: Size,
    pub nodeCopy: Option<unsafe extern "C" fn(*mut Node, *const Node)>,
    pub nodeEqual: Option<unsafe extern "C" fn(*const Node, *const Node) -> bool>,
    pub nodeOut: Option<unsafe extern "C" fn(StringInfo, *const Node)>,
    pub nodeRead: Option<unsafe extern "C" fn(*mut Node)>,
}

// snprintf-style formatting is performed through appendStringInfo, declared as
// an extern variadic C function below.
extern "C" {
    fn appendStringInfo(str: StringInfo, fmt: *const c_char, ...);
}

/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Write the label for the node type */
macro_rules! WRITE_NODE_TYPE {
    ($nodelabel:expr) => {
        appendStringInfoString(str, $nodelabel)
    };
}

/* Write an integer field (anything written as ":fldname %d") */
macro_rules! WRITE_INT_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %d\0").as_ptr() as *const c_char,
            $node.$fldname as c_int,
        )
    };
}

/* Write an unsigned integer field (anything written as ":fldname %u") */
macro_rules! WRITE_UINT_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %u\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write a signed integer field (anything written with INT64_FORMAT) */
macro_rules! WRITE_INT64_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %lld\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write an unsigned integer field (anything written with UINT64_FORMAT) */
macro_rules! WRITE_UINT64_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %llu\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
macro_rules! WRITE_OID_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %u\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write a long-integer field */
macro_rules! WRITE_LONG_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %ld\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write a char field (ie, one ascii character) */
macro_rules! WRITE_CHAR_FIELD {
    ($node:expr, $fldname:ident) => {{
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outChar(str, $node.$fldname);
    }};
}

/* Write an enumerated-type field as an integer code */
macro_rules! WRITE_ENUM_FIELD {
    ($node:expr, $fldname:ident, $enumtype:ty) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %d\0").as_ptr() as *const c_char,
            $node.$fldname as c_int,
        )
    };
}

/* Write a float field (actually, they're double) */
macro_rules! WRITE_FLOAT_FIELD {
    ($node:expr, $fldname:ident) => {{
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outDouble(str, $node.$fldname);
    }};
}

/* Write a boolean field */
macro_rules! WRITE_BOOL_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %s\0").as_ptr() as *const c_char,
            booltostr($node.$fldname),
        )
    };
}

/* Write a character-string (possibly NULL) field */
macro_rules! WRITE_STRING_FIELD {
    ($node:expr, $fldname:ident) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outToken(str, $node.$fldname);
    }};
}

/* Write a parse location field (actually same as INT case) */
macro_rules! WRITE_LOCATION_FIELD {
    ($node:expr, $fldname:ident) => {
        appendStringInfo(
            str,
            concat!(" :", stringify!($fldname), " %d\0").as_ptr() as *const c_char,
            if write_location_fields {
                $node.$fldname
            } else {
                -1
            },
        )
    };
}

/* Write a Node field */
macro_rules! WRITE_NODE_FIELD {
    ($node:expr, $fldname:ident) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outNode(str, $node.$fldname as *const _ as *const ::std::ffi::c_void);
    }};
}

/* Write a bitmapset field */
macro_rules! WRITE_BITMAPSET_FIELD {
    ($node:expr, $fldname:ident) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outBitmapset(str, $node.$fldname);
    }};
}

/* Write a variable-length array (not a List) of Node pointers */
macro_rules! WRITE_NODE_ARRAY {
    ($node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeNodeArray(str, $node.$fldname as *const *const Node, $len);
    }};
}

/* Write a variable-length array of AttrNumber */
macro_rules! WRITE_ATTRNUMBER_ARRAY {
    ($node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeAttrNumberCols(str, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of Oid */
macro_rules! WRITE_OID_ARRAY {
    ($node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeOidCols(str, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of Index */
macro_rules! WRITE_INDEX_ARRAY {
    ($node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeIndexCols(str, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of int */
macro_rules! WRITE_INT_ARRAY {
    ($node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeIntCols(str, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of bool */
macro_rules! WRITE_BOOL_ARRAY {
    ($node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            str,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeBoolCols(str, $node.$fldname, $len);
    }};
}

#[inline]
fn booltostr(x: bool) -> *const c_char {
    if x {
        c"true".as_ptr()
    } else {
        c"false".as_ptr()
    }
}

/*
 * outToken
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null string pointer is given, it is encoded as '<>'.
 *	  An empty string is encoded as '""'.  To avoid ambiguity, input
 *	  strings beginning with '<' or '"' receive a leading backslash.
 */
pub unsafe fn outToken(str: StringInfo, s: *const c_char) {
    if s.is_null() {
        appendStringInfoString(str, c"<>".as_ptr());
        return;
    }
    if *s == b'\0' as c_char {
        appendStringInfoString(str, c"\"\"".as_ptr());
        return;
    }

    /*
     * Look for characters or patterns that are treated specially by read.c
     * (either in pg_strtok() or in nodeRead()), and therefore need a
     * protective backslash.
     */
    /* These characters only need to be quoted at the start of the string */
    let mut p = s;
    if *p == b'<' as c_char
        || *p == b'"' as c_char
        || isdigit(*p as u8)
        || ((*p == b'+' as c_char || *p == b'-' as c_char)
            && (isdigit(*p.add(1) as u8) || *p.add(1) == b'.' as c_char))
    {
        appendStringInfoChar(str, b'\\' as c_char);
    }
    while *p != b'\0' as c_char {
        /* These chars must be backslashed anywhere in the string */
        let c = *p;
        if c == b' ' as c_char
            || c == b'\n' as c_char
            || c == b'\t' as c_char
            || c == b'(' as c_char
            || c == b')' as c_char
            || c == b'{' as c_char
            || c == b'}' as c_char
            || c == b'\\' as c_char
        {
            appendStringInfoChar(str, b'\\' as c_char);
        }
        appendStringInfoChar(str, c);
        p = p.add(1);
    }
}

#[inline]
fn isdigit(c: u8) -> bool {
    c.is_ascii_digit()
}

/*
 * Convert one char.  Goes through outToken() so that special characters are
 * escaped.
 */
unsafe fn outChar(str: StringInfo, c: c_char) {
    let mut in_: [c_char; 2] = [0; 2];

    /* Traditionally, we've represented \0 as <>, so keep doing that */
    if c == b'\0' as c_char {
        appendStringInfoString(str, c"<>".as_ptr());
        return;
    }

    in_[0] = c;
    in_[1] = b'\0' as c_char;

    outToken(str, in_.as_ptr());
}

/*
 * Convert a double value, attempting to ensure the value is preserved exactly.
 */
unsafe fn outDouble(str: StringInfo, d: f64) {
    let mut buf: [c_char; DOUBLE_SHORTEST_DECIMAL_LEN] = [0; DOUBLE_SHORTEST_DECIMAL_LEN];

    double_to_shortest_decimal_buf(d, buf.as_mut_ptr());
    appendStringInfoString(str, buf.as_ptr());
}

const DOUBLE_SHORTEST_DECIMAL_LEN: usize = 25;

/*
 * common implementation for scalar-array-writing functions
 *
 * The data format is either "<>" for a NULL pointer or "(item item item)".
 * fmtstr must include a leading space, and the rest of it must produce
 * something that will be seen as a single simple token by pg_strtok().
 * convfunc can be empty, or the name of a conversion macro or function.
 */
macro_rules! WRITE_SCALAR_ARRAY {
    ($fnname:ident, $datatype:ty, $fmtstr:expr, $convfunc:expr) => {
        unsafe fn $fnname(str: StringInfo, arr: *const $datatype, len: c_int) {
            if !arr.is_null() {
                appendStringInfoChar(str, b'(' as c_char);
                for i in 0..len {
                    appendStringInfo(str, $fmtstr.as_ptr() as *const c_char, $convfunc(*arr.offset(i as isize)));
                }
                appendStringInfoChar(str, b')' as c_char);
            } else {
                appendStringInfoString(str, c"<>".as_ptr());
            }
        }
    };
}

WRITE_SCALAR_ARRAY!(writeAttrNumberCols, AttrNumber, " %d\0", |v: AttrNumber| v as c_int);
WRITE_SCALAR_ARRAY!(writeOidCols, Oid, " %u\0", |v: Oid| v);
WRITE_SCALAR_ARRAY!(writeIndexCols, Index, " %u\0", |v: Index| v);
WRITE_SCALAR_ARRAY!(writeIntCols, c_int, " %d\0", |v: c_int| v);
WRITE_SCALAR_ARRAY!(writeBoolCols, bool, " %s\0", |v: bool| booltostr(v));

/*
 * Print an array (not a List) of Node pointers.
 *
 * The decoration is identical to that of scalar arrays, but we can't
 * quite use appendStringInfo() in the loop.
 */
unsafe fn writeNodeArray(str: StringInfo, arr: *const *const Node, len: c_int) {
    if !arr.is_null() {
        appendStringInfoChar(str, b'(' as c_char);
        for i in 0..len {
            appendStringInfoChar(str, b' ' as c_char);
            outNode(
                str,
                *arr.offset(i as isize) as *const ::std::ffi::c_void,
            );
        }
        appendStringInfoChar(str, b')' as c_char);
    } else {
        appendStringInfoString(str, c"<>".as_ptr());
    }
}

/*
 * Print a List.
 */
unsafe fn _outList(str: StringInfo, node: *const List) {
    appendStringInfoChar(str, b'(' as c_char);

    if IsA!(node, T_IntList) {
        appendStringInfoChar(str, b'i' as c_char);
    } else if IsA!(node, T_OidList) {
        appendStringInfoChar(str, b'o' as c_char);
    } else if IsA!(node, T_XidList) {
        appendStringInfoChar(str, b'x' as c_char);
    }

    foreach!(lc, node, {
        /*
         * For the sake of backward compatibility, we emit a slightly
         * different whitespace format for lists of nodes vs. other types of
         * lists. XXX: is this necessary?
         */
        if IsA!(node, T_List) {
            outNode(str, lfirst(current_cell!(lc)));
            if !lnext(node, current_cell!(lc)).is_null() {
                appendStringInfoChar(str, b' ' as c_char);
            }
        } else if IsA!(node, T_IntList) {
            appendStringInfo(str, c" %d".as_ptr(), lfirst_int(current_cell!(lc)));
        } else if IsA!(node, T_OidList) {
            appendStringInfo(str, c" %u".as_ptr(), lfirst_oid(current_cell!(lc)));
        } else if IsA!(node, T_XidList) {
            appendStringInfo(str, c" %u".as_ptr(), lfirst_xid(current_cell!(lc)));
        } else {
            elog!(ERROR, "unrecognized list node type: {}", (*node).r#type as c_int);
        }
    });

    appendStringInfoChar(str, b')' as c_char);
}

/*
 * outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Note: the output format is "(b int int ...)", similar to an integer List.
 *
 * We export this function for use by extensions that define extensible nodes.
 * That's somewhat historical, though, because calling outNode() will work.
 */
pub unsafe fn outBitmapset(str: StringInfo, bms: *const Bitmapset) {
    appendStringInfoChar(str, b'(' as c_char);
    appendStringInfoChar(str, b'b' as c_char);
    let mut x: c_int = -1;
    loop {
        x = bms_next_member(bms, x);
        if x < 0 {
            break;
        }
        appendStringInfo(str, c" %d".as_ptr(), x);
    }
    appendStringInfoChar(str, b')' as c_char);
}

/*
 * Print the value of a Datum given its type.
 */
pub unsafe fn outDatum(str: StringInfo, value: Datum, typlen: c_int, typbyval: bool) {
    let length: Size;
    let s: *const c_char;

    length = datumGetSize(value, typbyval, typlen);

    if typbyval {
        s = (&value) as *const Datum as *const c_char;
        appendStringInfo(str, c"%u [ ".as_ptr(), length as u32);
        for i in 0..(::std::mem::size_of::<Datum>() as Size) {
            appendStringInfo(str, c"%d ".as_ptr(), *s.offset(i as isize) as c_int);
        }
        appendStringInfoChar(str, b']' as c_char);
    } else {
        s = DatumGetPointer(value) as *const c_char;
        if !PointerIsValid(s) {
            appendStringInfoString(str, c"0 [ ]".as_ptr());
        } else {
            appendStringInfo(str, c"%u [ ".as_ptr(), length as u32);
            for i in 0..length {
                appendStringInfo(str, c"%d ".as_ptr(), *s.offset(i as isize) as c_int);
            }
            appendStringInfoChar(str, b']' as c_char);
        }
    }
}

#[inline]
fn PointerIsValid<T>(p: *const T) -> bool {
    !p.is_null()
}

// #include "outfuncs.funcs.c"
//
// The bodies of the per-node _out<NodeType>() functions are mechanically
// generated by gen_node_support.pl from the node definitions.  They have not
// been ported here; each one is provided as a stub once the corresponding node
// type is translated.
// TODO: nodes/outfuncs.funcs.c (generated)

/*
 * Support functions for nodes with custom_read_write attribute or
 * special_read_write attribute
 */

unsafe fn _outConst(str: StringInfo, node: *const Const) {
    let node = &*node;
    WRITE_NODE_TYPE!(c"CONST".as_ptr());

    WRITE_OID_FIELD!(node, consttype);
    WRITE_INT_FIELD!(node, consttypmod);
    WRITE_OID_FIELD!(node, constcollid);
    WRITE_INT_FIELD!(node, constlen);
    WRITE_BOOL_FIELD!(node, constbyval);
    WRITE_BOOL_FIELD!(node, constisnull);
    WRITE_LOCATION_FIELD!(node, location);

    appendStringInfoString(str, c" :constvalue ".as_ptr());
    if node.constisnull {
        appendStringInfoString(str, c"<>".as_ptr());
    } else {
        outDatum(str, node.constvalue, node.constlen, node.constbyval);
    }
}

unsafe fn _outBoolExpr(str: StringInfo, node: *const BoolExpr) {
    let node = &*node;
    let opstr: *const c_char;

    WRITE_NODE_TYPE!(c"BOOLEXPR".as_ptr());

    /* do-it-yourself enum representation */
    match node.boolop {
        BoolExprType::AND_EXPR => opstr = c"and".as_ptr(),
        BoolExprType::OR_EXPR => opstr = c"or".as_ptr(),
        BoolExprType::NOT_EXPR => opstr = c"not".as_ptr(),
    }
    appendStringInfoString(str, c" :boolop ".as_ptr());
    outToken(str, opstr);

    WRITE_NODE_FIELD!(node, args);
    WRITE_LOCATION_FIELD!(node, location);
}

unsafe fn _outForeignKeyOptInfo(str: StringInfo, node: *const ForeignKeyOptInfo) {
    let node = &*node;

    WRITE_NODE_TYPE!(c"FOREIGNKEYOPTINFO".as_ptr());

    WRITE_UINT_FIELD!(node, con_relid);
    WRITE_UINT_FIELD!(node, ref_relid);
    WRITE_INT_FIELD!(node, nkeys);
    WRITE_ATTRNUMBER_ARRAY!(node, conkey, node.nkeys);
    WRITE_ATTRNUMBER_ARRAY!(node, confkey, node.nkeys);
    WRITE_OID_ARRAY!(node, conpfeqop, node.nkeys);
    WRITE_INT_FIELD!(node, nmatched_ec);
    WRITE_INT_FIELD!(node, nconst_ec);
    WRITE_INT_FIELD!(node, nmatched_rcols);
    WRITE_INT_FIELD!(node, nmatched_ri);
    /* for compactness, just print the number of matches per column: */
    appendStringInfoString(str, c" :eclass".as_ptr());
    for i in 0..node.nkeys {
        appendStringInfo(
            str,
            c" %d".as_ptr(),
            (!(*node.eclass.offset(i as isize)).is_null()) as c_int,
        );
    }
    appendStringInfoString(str, c" :rinfos".as_ptr());
    for i in 0..node.nkeys {
        appendStringInfo(
            str,
            c" %d".as_ptr(),
            list_length(*node.rinfos.offset(i as isize)),
        );
    }
}

unsafe fn _outEquivalenceClass(str: StringInfo, node: *const EquivalenceClass) {
    /*
     * To simplify reading, we just chase up to the topmost merged EC and
     * print that, without bothering to show the merge-ees separately.
     */
    let mut node = node;
    while !(*node).ec_merged.is_null() {
        node = (*node).ec_merged;
    }
    let node = &*node;

    WRITE_NODE_TYPE!(c"EQUIVALENCECLASS".as_ptr());

    WRITE_NODE_FIELD!(node, ec_opfamilies);
    WRITE_OID_FIELD!(node, ec_collation);
    WRITE_INT_FIELD!(node, ec_childmembers_size);
    WRITE_NODE_FIELD!(node, ec_members);
    WRITE_NODE_ARRAY!(node, ec_childmembers, node.ec_childmembers_size);
    WRITE_NODE_FIELD!(node, ec_sources);
    /* Only ec_derives_list is written; hash is not serialized. */
    WRITE_NODE_FIELD!(node, ec_derives_list);
    WRITE_BITMAPSET_FIELD!(node, ec_relids);
    WRITE_BOOL_FIELD!(node, ec_has_const);
    WRITE_BOOL_FIELD!(node, ec_has_volatile);
    WRITE_BOOL_FIELD!(node, ec_broken);
    WRITE_UINT_FIELD!(node, ec_sortref);
    WRITE_UINT_FIELD!(node, ec_min_security);
    WRITE_UINT_FIELD!(node, ec_max_security);
}

unsafe fn _outExtensibleNode(str: StringInfo, node: *const ExtensibleNode) {
    let node = &*node;
    let methods: *const ExtensibleNodeMethods;

    methods = GetExtensibleNodeMethods(node.extnodename, false);

    WRITE_NODE_TYPE!(c"EXTENSIBLENODE".as_ptr());

    WRITE_STRING_FIELD!(node, extnodename);

    /* serialize the private fields */
    ((*methods).nodeOut.unwrap())(str, node as *const ExtensibleNode as *const Node);
}

unsafe fn _outRangeTblEntry(str: StringInfo, node: *const RangeTblEntry) {
    let node = &*node;

    WRITE_NODE_TYPE!(c"RANGETBLENTRY".as_ptr());

    WRITE_NODE_FIELD!(node, alias);
    WRITE_NODE_FIELD!(node, eref);
    WRITE_ENUM_FIELD!(node, rtekind, RTEKind);

    match node.rtekind {
        RTEKind::RTE_RELATION => {
            WRITE_OID_FIELD!(node, relid);
            WRITE_BOOL_FIELD!(node, inh);
            WRITE_CHAR_FIELD!(node, relkind);
            WRITE_INT_FIELD!(node, rellockmode);
            WRITE_UINT_FIELD!(node, perminfoindex);
            WRITE_NODE_FIELD!(node, tablesample);
        }
        RTEKind::RTE_SUBQUERY => {
            WRITE_NODE_FIELD!(node, subquery);
            WRITE_BOOL_FIELD!(node, security_barrier);
            /* we re-use these RELATION fields, too: */
            WRITE_OID_FIELD!(node, relid);
            WRITE_BOOL_FIELD!(node, inh);
            WRITE_CHAR_FIELD!(node, relkind);
            WRITE_INT_FIELD!(node, rellockmode);
            WRITE_UINT_FIELD!(node, perminfoindex);
        }
        RTEKind::RTE_JOIN => {
            WRITE_ENUM_FIELD!(node, jointype, JoinType);
            WRITE_INT_FIELD!(node, joinmergedcols);
            WRITE_NODE_FIELD!(node, joinaliasvars);
            WRITE_NODE_FIELD!(node, joinleftcols);
            WRITE_NODE_FIELD!(node, joinrightcols);
            WRITE_NODE_FIELD!(node, join_using_alias);
        }
        RTEKind::RTE_FUNCTION => {
            WRITE_NODE_FIELD!(node, functions);
            WRITE_BOOL_FIELD!(node, funcordinality);
        }
        RTEKind::RTE_TABLEFUNC => {
            WRITE_NODE_FIELD!(node, tablefunc);
        }
        RTEKind::RTE_VALUES => {
            WRITE_NODE_FIELD!(node, values_lists);
            WRITE_NODE_FIELD!(node, coltypes);
            WRITE_NODE_FIELD!(node, coltypmods);
            WRITE_NODE_FIELD!(node, colcollations);
        }
        RTEKind::RTE_CTE => {
            WRITE_STRING_FIELD!(node, ctename);
            WRITE_UINT_FIELD!(node, ctelevelsup);
            WRITE_BOOL_FIELD!(node, self_reference);
            WRITE_NODE_FIELD!(node, coltypes);
            WRITE_NODE_FIELD!(node, coltypmods);
            WRITE_NODE_FIELD!(node, colcollations);
        }
        RTEKind::RTE_NAMEDTUPLESTORE => {
            WRITE_STRING_FIELD!(node, enrname);
            WRITE_FLOAT_FIELD!(node, enrtuples);
            WRITE_NODE_FIELD!(node, coltypes);
            WRITE_NODE_FIELD!(node, coltypmods);
            WRITE_NODE_FIELD!(node, colcollations);
            /* we re-use these RELATION fields, too: */
            WRITE_OID_FIELD!(node, relid);
        }
        RTEKind::RTE_RESULT => {
            /* no extra fields */
        }
        RTEKind::RTE_GROUP => {
            WRITE_NODE_FIELD!(node, groupexprs);
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized RTE kind: {}", node.rtekind as c_int);
        }
    }

    WRITE_BOOL_FIELD!(node, lateral);
    WRITE_BOOL_FIELD!(node, inFromCl);
    WRITE_NODE_FIELD!(node, securityQuals);
}

unsafe fn _outA_Expr(str: StringInfo, node: *const A_Expr) {
    let node = &*node;

    WRITE_NODE_TYPE!(c"A_EXPR".as_ptr());

    match node.kind {
        A_Expr_Kind::AEXPR_OP => {
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_OP_ANY => {
            appendStringInfoString(str, c" ANY".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_OP_ALL => {
            appendStringInfoString(str, c" ALL".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_DISTINCT => {
            appendStringInfoString(str, c" DISTINCT".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_NOT_DISTINCT => {
            appendStringInfoString(str, c" NOT_DISTINCT".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_NULLIF => {
            appendStringInfoString(str, c" NULLIF".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_IN => {
            appendStringInfoString(str, c" IN".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_LIKE => {
            appendStringInfoString(str, c" LIKE".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_ILIKE => {
            appendStringInfoString(str, c" ILIKE".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_SIMILAR => {
            appendStringInfoString(str, c" SIMILAR".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_BETWEEN => {
            appendStringInfoString(str, c" BETWEEN".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN => {
            appendStringInfoString(str, c" NOT_BETWEEN".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_BETWEEN_SYM => {
            appendStringInfoString(str, c" BETWEEN_SYM".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
            appendStringInfoString(str, c" NOT_BETWEEN_SYM".as_ptr());
            WRITE_NODE_FIELD!(node, name);
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized A_Expr_Kind: {}", node.kind as c_int);
        }
    }

    WRITE_NODE_FIELD!(node, lexpr);
    WRITE_NODE_FIELD!(node, rexpr);
    WRITE_LOCATION_FIELD!(node, rexpr_list_start);
    WRITE_LOCATION_FIELD!(node, rexpr_list_end);
    WRITE_LOCATION_FIELD!(node, location);
}

unsafe fn _outInteger(str: StringInfo, node: *const Integer) {
    let node = &*node;
    appendStringInfo(str, c"%d".as_ptr(), node.ival);
}

unsafe fn _outFloat(str: StringInfo, node: *const Float) {
    let node = &*node;
    /*
     * We assume the value is a valid numeric literal and so does not need
     * quoting.
     */
    appendStringInfoString(str, node.fval);
}

unsafe fn _outBoolean(str: StringInfo, node: *const Boolean) {
    let node = &*node;
    appendStringInfoString(
        str,
        if node.boolval {
            c"true".as_ptr()
        } else {
            c"false".as_ptr()
        },
    );
}

unsafe fn _outString(str: StringInfo, node: *const PgString) {
    let node = &*node;
    /*
     * We use outToken to provide escaping of the string's content, but we
     * don't want it to convert an empty string to '""', because we're putting
     * double quotes around the string already.
     */
    appendStringInfoChar(str, b'"' as c_char);
    if *node.sval != b'\0' as c_char {
        outToken(str, node.sval);
    }
    appendStringInfoChar(str, b'"' as c_char);
}

unsafe fn _outBitString(str: StringInfo, node: *const BitString) {
    let node = &*node;
    /*
     * The lexer will always produce a string starting with 'b' or 'x'.  There
     * might be characters following that that need escaping, but outToken
     * won't escape the 'b' or 'x'.  This is relied on by nodeTokenType.
     */
    Assert!(*node.bsval == b'b' as c_char || *node.bsval == b'x' as c_char);
    outToken(str, node.bsval);
}

unsafe fn _outA_Const(str: StringInfo, node: *const A_Const) {
    let node = &*node;

    WRITE_NODE_TYPE!(c"A_CONST".as_ptr());

    if node.isnull {
        appendStringInfoString(str, c" NULL".as_ptr());
    } else {
        appendStringInfoString(str, c" :val ".as_ptr());
        outNode(str, &node.val as *const _ as *const ::std::ffi::c_void);
    }
    WRITE_LOCATION_FIELD!(node, location);
}

/*
 * outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
pub unsafe fn outNode(str: StringInfo, obj: *const ::std::ffi::c_void) {
    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    if obj.is_null() {
        appendStringInfoString(str, c"<>".as_ptr());
    } else if IsA!(obj, T_List) || IsA!(obj, T_IntList) || IsA!(obj, T_OidList) || IsA!(obj, T_XidList) {
        _outList(str, obj as *const List);
    }
    /* nodeRead does not want to see { } around these! */
    else if IsA!(obj, T_Integer) {
        _outInteger(str, obj as *const Integer);
    } else if IsA!(obj, T_Float) {
        _outFloat(str, obj as *const Float);
    } else if IsA!(obj, T_Boolean) {
        _outBoolean(str, obj as *const Boolean);
    } else if IsA!(obj, T_String) {
        _outString(str, obj as *const PgString);
    } else if IsA!(obj, T_BitString) {
        _outBitString(str, obj as *const BitString);
    } else if IsA!(obj, T_Bitmapset) {
        outBitmapset(str, obj as *const Bitmapset);
    } else {
        appendStringInfoChar(str, b'{' as c_char);
        match nodeTag(obj) {
            // #include "outfuncs.switch.c"
            //
            // The switch dispatch on every node tag is mechanically generated
            // by gen_node_support.pl.  It is omitted here pending translation
            // of the individual node types.
            // TODO: nodes/outfuncs.switch.c (generated)
            _ => {
                /*
                 * This should be an ERROR, but it's too useful to be able to
                 * dump structures that outNode only understands part of.
                 */
                elog!(
                    WARNING,
                    "could not dump unrecognized node type: {}",
                    nodeTag(obj) as c_int
                );
            }
        }
        appendStringInfoChar(str, b'}' as c_char);
    }
}

/*
 * nodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 *
 * write_loc_fields determines whether location fields are output with their
 * actual value rather than -1.  The actual value can be useful for debugging,
 * but for most uses, the actual value is not useful, since the original query
 * string is no longer available.
 */
unsafe fn nodeToStringInternal(obj: *const ::std::ffi::c_void, write_loc_fields: bool) -> *mut c_char {
    let mut str: StringInfoData = StringInfoData;
    let save_write_location_fields: bool;

    save_write_location_fields = write_location_fields;
    write_location_fields = write_loc_fields;

    /* see stringinfo.h for an explanation of this maneuver */
    initStringInfo(&mut str);
    outNode(&mut str, obj);

    write_location_fields = save_write_location_fields;

    // str.data
    StringInfoData_data(&str)
}

unsafe fn StringInfoData_data(_str: *const StringInfoData) -> *mut c_char {
    unimplemented!() // TODO: lib/stringinfo.c (str.data field accessor)
}

/*
 * Externally visible entry points
 */
pub unsafe fn nodeToString(obj: *const ::std::ffi::c_void) -> *mut c_char {
    nodeToStringInternal(obj, false)
}

pub unsafe fn nodeToStringWithLocations(obj: *const ::std::ffi::c_void) -> *mut c_char {
    nodeToStringInternal(obj, true)
}

/*
 * bmsToString -
 *	   returns the ascii representation of the Bitmapset as a palloc'd string
 */
pub unsafe fn bmsToString(bms: *const Bitmapset) -> *mut c_char {
    let mut str: StringInfoData = StringInfoData;

    /* see stringinfo.h for an explanation of this maneuver */
    initStringInfo(&mut str);
    outBitmapset(&mut str, bms);
    StringInfoData_data(&str)
}

// ---------------------------------------------------------------------------
// Local stub node types referenced above (real definitions live in the
// parsenodes/primnodes/pathnodes modules once ported).
// ---------------------------------------------------------------------------

use crate::nodes::value::String as PgString;

unsafe fn lnext(_l: *const List, _cell: *const ListCell) -> *const ListCell {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn lfirst(_cell: *const ListCell) -> *const ::std::ffi::c_void {
    unimplemented!() // TODO: nodes/pg_list.h
}
unsafe fn lfirst_int(_cell: *const ListCell) -> c_int {
    unimplemented!() // TODO: nodes/pg_list.h
}
unsafe fn lfirst_oid(_cell: *const ListCell) -> Oid {
    unimplemented!() // TODO: nodes/pg_list.h
}
unsafe fn lfirst_xid(_cell: *const ListCell) -> Oid {
    unimplemented!() // TODO: nodes/pg_list.h
}
unsafe fn nodeTag(_obj: *const ::std::ffi::c_void) -> c_int {
    unimplemented!() // TODO: nodes/nodes.h
}

// Stub node-type structs (faithful field sets used above).

#[repr(C)]
pub struct Const {
    pub consttype: Oid,
    pub consttypmod: c_int,
    pub constcollid: Oid,
    pub constlen: c_int,
    pub constvalue: Datum,
    pub constisnull: bool,
    pub constbyval: bool,
    pub location: c_int,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BoolExprType {
    AND_EXPR = 0,
    OR_EXPR,
    NOT_EXPR,
}
pub use BoolExprType::*;

#[repr(C)]
pub struct BoolExpr {
    pub boolop: BoolExprType,
    pub args: *mut List,
    pub location: c_int,
}

#[repr(C)]
pub struct ForeignKeyOptInfo {
    pub con_relid: Index,
    pub ref_relid: Index,
    pub nkeys: c_int,
    pub conkey: *const AttrNumber,
    pub confkey: *const AttrNumber,
    pub conpfeqop: *const Oid,
    pub nmatched_ec: c_int,
    pub nconst_ec: c_int,
    pub nmatched_rcols: c_int,
    pub nmatched_ri: c_int,
    pub eclass: *const *const EquivalenceClass,
    pub rinfos: *const *const List,
}

#[repr(C)]
pub struct EquivalenceClass {
    pub ec_opfamilies: *mut List,
    pub ec_collation: Oid,
    pub ec_childmembers_size: c_int,
    pub ec_members: *mut List,
    pub ec_childmembers: *const *const List,
    pub ec_sources: *mut List,
    pub ec_derives_list: *mut List,
    pub ec_relids: *mut Bitmapset,
    pub ec_has_const: bool,
    pub ec_has_volatile: bool,
    pub ec_broken: bool,
    pub ec_sortref: Index,
    pub ec_min_security: Index,
    pub ec_max_security: Index,
    pub ec_merged: *const EquivalenceClass,
}

#[repr(C)]
pub struct ExtensibleNode {
    pub extnodename: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RTEKind {
    RTE_RELATION = 0,
    RTE_SUBQUERY,
    RTE_JOIN,
    RTE_FUNCTION,
    RTE_TABLEFUNC,
    RTE_VALUES,
    RTE_CTE,
    RTE_NAMEDTUPLESTORE,
    RTE_RESULT,
    RTE_GROUP,
}
pub use RTEKind::*;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    JOIN_INNER = 0,
}

#[repr(C)]
pub struct RangeTblEntry {
    pub alias: *mut Node,
    pub eref: *mut Node,
    pub rtekind: RTEKind,
    pub relid: Oid,
    pub inh: bool,
    pub relkind: c_char,
    pub rellockmode: c_int,
    pub perminfoindex: Index,
    pub tablesample: *mut Node,
    pub subquery: *mut Node,
    pub security_barrier: bool,
    pub jointype: JoinType,
    pub joinmergedcols: c_int,
    pub joinaliasvars: *mut List,
    pub joinleftcols: *mut List,
    pub joinrightcols: *mut List,
    pub join_using_alias: *mut Node,
    pub functions: *mut List,
    pub funcordinality: bool,
    pub tablefunc: *mut Node,
    pub values_lists: *mut List,
    pub coltypes: *mut List,
    pub coltypmods: *mut List,
    pub colcollations: *mut List,
    pub ctename: *const c_char,
    pub ctelevelsup: Index,
    pub self_reference: bool,
    pub enrname: *const c_char,
    pub enrtuples: f64,
    pub groupexprs: *mut List,
    pub lateral: bool,
    pub inFromCl: bool,
    pub securityQuals: *mut List,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum A_Expr_Kind {
    AEXPR_OP = 0,
    AEXPR_OP_ANY,
    AEXPR_OP_ALL,
    AEXPR_DISTINCT,
    AEXPR_NOT_DISTINCT,
    AEXPR_NULLIF,
    AEXPR_IN,
    AEXPR_LIKE,
    AEXPR_ILIKE,
    AEXPR_SIMILAR,
    AEXPR_BETWEEN,
    AEXPR_NOT_BETWEEN,
    AEXPR_BETWEEN_SYM,
    AEXPR_NOT_BETWEEN_SYM,
}
pub use A_Expr_Kind::*;

#[repr(C)]
pub struct A_Expr {
    pub kind: A_Expr_Kind,
    pub name: *mut List,
    pub lexpr: *mut Node,
    pub rexpr: *mut Node,
    pub rexpr_list_start: c_int,
    pub rexpr_list_end: c_int,
    pub location: c_int,
}

#[repr(C)]
pub struct Integer {
    pub ival: c_int,
}

#[repr(C)]
pub struct Float {
    pub fval: *const c_char,
}

#[repr(C)]
pub struct Boolean {
    pub boolval: bool,
}

#[repr(C)]
pub struct BitString {
    pub bsval: *const c_char,
}

#[repr(C)]
pub struct A_Const {
    pub val: Node,
    pub isnull: bool,
    pub location: c_int,
}
