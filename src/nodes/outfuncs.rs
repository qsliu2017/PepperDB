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

// Enum types referenced by generated _out functions (WRITE_ENUM_FIELD ignores
// the type argument, but match arms in custom code below use these).
use crate::nodes::nodes::NodeTag;

// ---------------------------------------------------------------------------
// Stub types for nodes referenced by the custom-write functions below.
// ---------------------------------------------------------------------------

pub use crate::lib::stringinfo::{StringInfo, StringInfoData};

pub type Bitmapset = crate::nodes::bitmapset::Bitmapset;

/* State flag that determines how nodeToStringInternal() should treat location fields */
static mut write_location_fields: bool = false;

// ---------------------------------------------------------------------------
// Local stubs for helper functions whose real definitions live elsewhere.
// ---------------------------------------------------------------------------

unsafe fn appendStringInfoString(_str: StringInfo, _s: *const c_char) {
    crate::lib::stringinfo::appendStringInfoString(_str as _, _s as _)
}

unsafe fn appendStringInfoChar(_str: StringInfo, _ch: c_char) {
    crate::lib::stringinfo::appendStringInfoChar(_str as _, _ch as _)
}

unsafe fn initStringInfo(_str: StringInfo) {
    crate::lib::stringinfo::initStringInfo(_str as _)
}

unsafe fn double_to_shortest_decimal_buf(_d: f64, _buf: *mut c_char) -> c_int {
    crate::common::shortest_dec::double_to_shortest_decimal_buf(_d, _buf as _) as _
}

unsafe fn datumGetSize(_value: Datum, _typByVal: bool, _typLen: c_int) -> Size {
    crate::utils::adt::datum::datumGetSize(_value as _, _typByVal, _typLen as _) as _
}

unsafe fn bms_next_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int {
    crate::nodes::bitmapset::bms_next_member(_a as _, _prevbit as _) as _
}

unsafe fn list_length(_l: *const List) -> c_int {
    crate::nodes::pg_list::list_length(_l as _) as _
}

unsafe fn check_stack_depth() {
    crate::miscadmin::check_stack_depth()
}

unsafe fn GetExtensibleNodeMethods(
    _extnodename: *const c_char,
    _missing_ok: bool,
) -> *const ExtensibleNodeMethods {
    crate::nodes::extensible::GetExtensibleNodeMethods(_extnodename, _missing_ok) as _
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
    fn appendStringInfo(string: StringInfo, fmt: *const c_char, ...);
}

/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

// Each Out routine has a local StringInfo named `string`; the C macros relied
// on textual substitution of that name.  Rust macro hygiene cannot capture a
// free identifier from the call site, so the StringInfo is passed in explicitly
// as the first macro argument.

/* Write the label for the node type */
macro_rules! WRITE_NODE_TYPE {
    ($string:expr, $nodelabel:expr) => {
        appendStringInfoString($string, $nodelabel)
    };
}

/* Write an integer field (anything written as ":fldname %d") */
macro_rules! WRITE_INT_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %d\0").as_ptr() as *const c_char,
            $node.$fldname as c_int,
        )
    };
}

/* Write an unsigned integer field (anything written as ":fldname %u") */
macro_rules! WRITE_UINT_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %u\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write a signed integer field (anything written with INT64_FORMAT) */
macro_rules! WRITE_INT64_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %lld\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write an unsigned integer field (anything written with UINT64_FORMAT) */
macro_rules! WRITE_UINT64_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %llu\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
macro_rules! WRITE_OID_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %u\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write a long-integer field */
macro_rules! WRITE_LONG_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %ld\0").as_ptr() as *const c_char,
            $node.$fldname,
        )
    };
}

/* Write a char field (ie, one ascii character) */
macro_rules! WRITE_CHAR_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {{
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outChar($string, $node.$fldname);
    }};
}

/* Write an enumerated-type field as an integer code */
macro_rules! WRITE_ENUM_FIELD {
    ($string:expr, $node:expr, $fldname:ident, $enumtype:ty) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %d\0").as_ptr() as *const c_char,
            $node.$fldname as c_int,
        )
    };
}

/* Write a float field (actually, they're double) */
macro_rules! WRITE_FLOAT_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {{
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outDouble($string, $node.$fldname);
    }};
}

/* Write a boolean field */
macro_rules! WRITE_BOOL_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
            concat!(" :", stringify!($fldname), " %s\0").as_ptr() as *const c_char,
            booltostr($node.$fldname),
        )
    };
}

/* Write a character-string (possibly NULL) field */
macro_rules! WRITE_STRING_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outToken($string, $node.$fldname);
    }};
}

/* Write a parse location field (actually same as INT case) */
macro_rules! WRITE_LOCATION_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {
        appendStringInfo(
            $string,
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
    ($string:expr, $node:expr, $fldname:ident) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outNode($string, $node.$fldname as *const _ as *const ::std::ffi::c_void);
    }};
}

/* Write a bitmapset field */
macro_rules! WRITE_BITMAPSET_FIELD {
    ($string:expr, $node:expr, $fldname:ident) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        outBitmapset($string, $node.$fldname);
    }};
}

/* Write a variable-length array (not a List) of Node pointers */
macro_rules! WRITE_NODE_ARRAY {
    ($string:expr, $node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeNodeArray($string, $node.$fldname as *const *const Node, $len);
    }};
}

/* Write a variable-length array of AttrNumber */
macro_rules! WRITE_ATTRNUMBER_ARRAY {
    ($string:expr, $node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeAttrNumberCols($string, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of Oid */
macro_rules! WRITE_OID_ARRAY {
    ($string:expr, $node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeOidCols($string, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of Index */
macro_rules! WRITE_INDEX_ARRAY {
    ($string:expr, $node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeIndexCols($string, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of int */
macro_rules! WRITE_INT_ARRAY {
    ($string:expr, $node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeIntCols($string, $node.$fldname, $len);
    }};
}

/* Write a variable-length array of bool */
macro_rules! WRITE_BOOL_ARRAY {
    ($string:expr, $node:expr, $fldname:ident, $len:expr) => {{
        appendStringInfoString(
            $string,
            concat!(" :", stringify!($fldname), " \0").as_ptr() as *const c_char,
        );
        writeBoolCols($string, $node.$fldname, $len);
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
pub unsafe fn outToken(string: StringInfo, s: *const c_char) {
    if s.is_null() {
        appendStringInfoString(string, c"<>".as_ptr());
        return;
    }
    if *s == b'\0' as c_char {
        appendStringInfoString(string, c"\"\"".as_ptr());
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
        appendStringInfoChar(string, b'\\' as c_char);
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
            appendStringInfoChar(string, b'\\' as c_char);
        }
        appendStringInfoChar(string, c);
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
unsafe fn outChar(string: StringInfo, c: c_char) {
    let mut in_: [c_char; 2] = [0; 2];

    /* Traditionally, we've represented \0 as <>, so keep doing that */
    if c == b'\0' as c_char {
        appendStringInfoString(string, c"<>".as_ptr());
        return;
    }

    in_[0] = c;
    in_[1] = b'\0' as c_char;

    outToken(string, in_.as_ptr());
}

/*
 * Convert a double value, attempting to ensure the value is preserved exactly.
 */
unsafe fn outDouble(string: StringInfo, d: f64) {
    let mut buf: [c_char; DOUBLE_SHORTEST_DECIMAL_LEN] = [0; DOUBLE_SHORTEST_DECIMAL_LEN];

    double_to_shortest_decimal_buf(d, buf.as_mut_ptr());
    appendStringInfoString(string, buf.as_ptr());
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
        unsafe fn $fnname(string: StringInfo, arr: *const $datatype, len: c_int) {
            if !arr.is_null() {
                appendStringInfoChar(string, b'(' as c_char);
                for i in 0..len {
                    appendStringInfo(string, $fmtstr.as_ptr() as *const c_char, $convfunc(*arr.offset(i as isize)));
                }
                appendStringInfoChar(string, b')' as c_char);
            } else {
                appendStringInfoString(string, c"<>".as_ptr());
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
unsafe fn writeNodeArray(string: StringInfo, arr: *const *const Node, len: c_int) {
    if !arr.is_null() {
        appendStringInfoChar(string, b'(' as c_char);
        for i in 0..len {
            appendStringInfoChar(string, b' ' as c_char);
            outNode(
                string,
                *arr.offset(i as isize) as *const ::std::ffi::c_void,
            );
        }
        appendStringInfoChar(string, b')' as c_char);
    } else {
        appendStringInfoString(string, c"<>".as_ptr());
    }
}

/*
 * Print a List.
 */
unsafe fn _outList(string: StringInfo, node: *const List) {
    appendStringInfoChar(string, b'(' as c_char);

    if IsA!(node, T_IntList) {
        appendStringInfoChar(string, b'i' as c_char);
    } else if IsA!(node, T_OidList) {
        appendStringInfoChar(string, b'o' as c_char);
    } else if IsA!(node, T_XidList) {
        appendStringInfoChar(string, b'x' as c_char);
    }

    foreach!(lc, node, {
        /*
         * For the sake of backward compatibility, we emit a slightly
         * different whitespace format for lists of nodes vs. other types of
         * lists. XXX: is this necessary?
         */
        if IsA!(node, T_List) {
            outNode(string, lfirst(current_cell!(lc)));
            if !lnext(node, current_cell!(lc)).is_null() {
                appendStringInfoChar(string, b' ' as c_char);
            }
        } else if IsA!(node, T_IntList) {
            appendStringInfo(string, c" %d".as_ptr(), lfirst_int(current_cell!(lc)));
        } else if IsA!(node, T_OidList) {
            appendStringInfo(string, c" %u".as_ptr(), lfirst_oid(current_cell!(lc)));
        } else if IsA!(node, T_XidList) {
            appendStringInfo(string, c" %u".as_ptr(), lfirst_xid(current_cell!(lc)));
        } else {
            elog!(ERROR, "unrecognized list node type: {}", (*node).r#type as c_int);
        }
    });

    appendStringInfoChar(string, b')' as c_char);
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
pub unsafe fn outBitmapset(string: StringInfo, bms: *const Bitmapset) {
    appendStringInfoChar(string, b'(' as c_char);
    appendStringInfoChar(string, b'b' as c_char);
    let mut x: c_int = -1;
    loop {
        x = bms_next_member(bms, x);
        if x < 0 {
            break;
        }
        appendStringInfo(string, c" %d".as_ptr(), x);
    }
    appendStringInfoChar(string, b')' as c_char);
}

/*
 * Print the value of a Datum given its type.
 */
pub unsafe fn outDatum(string: StringInfo, value: Datum, typlen: c_int, typbyval: bool) {
    let length: Size;
    let s: *const c_char;

    length = datumGetSize(value, typbyval, typlen);

    if typbyval {
        s = (&value) as *const Datum as *const c_char;
        appendStringInfo(string, c"%u [ ".as_ptr(), length as u32);
        for i in 0..(::std::mem::size_of::<Datum>() as Size) {
            appendStringInfo(string, c"%d ".as_ptr(), *s.offset(i as isize) as c_int);
        }
        appendStringInfoChar(string, b']' as c_char);
    } else {
        s = DatumGetPointer(value) as *const c_char;
        if !PointerIsValid(s) {
            appendStringInfoString(string, c"0 [ ]".as_ptr());
        } else {
            appendStringInfo(string, c"%u [ ".as_ptr(), length as u32);
            for i in 0..length {
                appendStringInfo(string, c"%d ".as_ptr(), *s.offset(i as isize) as c_int);
            }
            appendStringInfoChar(string, b']' as c_char);
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

unsafe fn _outConst(string: StringInfo, node: *const Const) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"CONST".as_ptr());

    WRITE_OID_FIELD!(string, node, consttype);
    WRITE_INT_FIELD!(string, node, consttypmod);
    WRITE_OID_FIELD!(string, node, constcollid);
    WRITE_INT_FIELD!(string, node, constlen);
    WRITE_BOOL_FIELD!(string, node, constbyval);
    WRITE_BOOL_FIELD!(string, node, constisnull);
    WRITE_LOCATION_FIELD!(string, node, location);

    appendStringInfoString(string, c" :constvalue ".as_ptr());
    if node.constisnull {
        appendStringInfoString(string, c"<>".as_ptr());
    } else {
        outDatum(string, node.constvalue, node.constlen, node.constbyval);
    }
}

unsafe fn _outBoolExpr(string: StringInfo, node: *const BoolExpr) {
    let node = &*node;
    let opstr: *const c_char;

    WRITE_NODE_TYPE!(string, c"BOOLEXPR".as_ptr());

    /* do-it-yourself enum representation */
    match node.boolop {
        BoolExprType::AND_EXPR => opstr = c"and".as_ptr(),
        BoolExprType::OR_EXPR => opstr = c"or".as_ptr(),
        BoolExprType::NOT_EXPR => opstr = c"not".as_ptr(),
    }
    appendStringInfoString(string, c" :boolop ".as_ptr());
    outToken(string, opstr);

    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outForeignKeyOptInfo(string: StringInfo, node: *const ForeignKeyOptInfo) {
    let node = &*node;

    WRITE_NODE_TYPE!(string, c"FOREIGNKEYOPTINFO".as_ptr());

    WRITE_UINT_FIELD!(string, node, con_relid);
    WRITE_UINT_FIELD!(string, node, ref_relid);
    WRITE_INT_FIELD!(string, node, nkeys);
    WRITE_ATTRNUMBER_ARRAY!(string, node, conkey, node.nkeys);
    WRITE_ATTRNUMBER_ARRAY!(string, node, confkey, node.nkeys);
    WRITE_OID_ARRAY!(string, node, conpfeqop, node.nkeys);
    WRITE_INT_FIELD!(string, node, nmatched_ec);
    WRITE_INT_FIELD!(string, node, nconst_ec);
    WRITE_INT_FIELD!(string, node, nmatched_rcols);
    WRITE_INT_FIELD!(string, node, nmatched_ri);
    /* for compactness, just print the number of matches per column: */
    appendStringInfoString(string, c" :eclass".as_ptr());
    for i in 0..node.nkeys {
        appendStringInfo(
            string,
            c" %d".as_ptr(),
            (!(*node.eclass.offset(i as isize)).is_null()) as c_int,
        );
    }
    appendStringInfoString(string, c" :rinfos".as_ptr());
    for i in 0..node.nkeys {
        appendStringInfo(
            string,
            c" %d".as_ptr(),
            list_length(*node.rinfos.offset(i as isize)),
        );
    }
}

unsafe fn _outEquivalenceClass(string: StringInfo, node: *const EquivalenceClass) {
    /*
     * To simplify reading, we just chase up to the topmost merged EC and
     * print that, without bothering to show the merge-ees separately.
     */
    let mut node = node;
    while !(*node).ec_merged.is_null() {
        node = (*node).ec_merged;
    }
    let node = &*node;

    WRITE_NODE_TYPE!(string, c"EQUIVALENCECLASS".as_ptr());

    WRITE_NODE_FIELD!(string, node, ec_opfamilies);
    WRITE_OID_FIELD!(string, node, ec_collation);
    WRITE_INT_FIELD!(string, node, ec_childmembers_size);
    WRITE_NODE_FIELD!(string, node, ec_members);
    WRITE_NODE_ARRAY!(string, node, ec_childmembers, node.ec_childmembers_size);
    WRITE_NODE_FIELD!(string, node, ec_sources);
    /* Only ec_derives_list is written; hash is not serialized. */
    WRITE_NODE_FIELD!(string, node, ec_derives_list);
    WRITE_BITMAPSET_FIELD!(string, node, ec_relids);
    WRITE_BOOL_FIELD!(string, node, ec_has_const);
    WRITE_BOOL_FIELD!(string, node, ec_has_volatile);
    WRITE_BOOL_FIELD!(string, node, ec_broken);
    WRITE_UINT_FIELD!(string, node, ec_sortref);
    WRITE_UINT_FIELD!(string, node, ec_min_security);
    WRITE_UINT_FIELD!(string, node, ec_max_security);
}

unsafe fn _outExtensibleNode(string: StringInfo, node: *const ExtensibleNode) {
    let node = &*node;
    let methods: *const ExtensibleNodeMethods;

    methods = GetExtensibleNodeMethods(node.extnodename, false);

    WRITE_NODE_TYPE!(string, c"EXTENSIBLENODE".as_ptr());

    WRITE_STRING_FIELD!(string, node, extnodename);

    /* serialize the private fields */
    ((*methods).nodeOut.unwrap())(string, node as *const ExtensibleNode as *const Node);
}

unsafe fn _outRangeTblEntry(string: StringInfo, node: *const RangeTblEntry) {
    let node = &*node;

    WRITE_NODE_TYPE!(string, c"RANGETBLENTRY".as_ptr());

    WRITE_NODE_FIELD!(string, node, alias);
    WRITE_NODE_FIELD!(string, node, eref);
    WRITE_ENUM_FIELD!(string, node, rtekind, RTEKind);

    match node.rtekind {
        RTEKind::RTE_RELATION => {
            WRITE_OID_FIELD!(string, node, relid);
            WRITE_BOOL_FIELD!(string, node, inh);
            WRITE_CHAR_FIELD!(string, node, relkind);
            WRITE_INT_FIELD!(string, node, rellockmode);
            WRITE_UINT_FIELD!(string, node, perminfoindex);
            WRITE_NODE_FIELD!(string, node, tablesample);
        }
        RTEKind::RTE_SUBQUERY => {
            WRITE_NODE_FIELD!(string, node, subquery);
            WRITE_BOOL_FIELD!(string, node, security_barrier);
            /* we re-use these RELATION fields, too: */
            WRITE_OID_FIELD!(string, node, relid);
            WRITE_BOOL_FIELD!(string, node, inh);
            WRITE_CHAR_FIELD!(string, node, relkind);
            WRITE_INT_FIELD!(string, node, rellockmode);
            WRITE_UINT_FIELD!(string, node, perminfoindex);
        }
        RTEKind::RTE_JOIN => {
            WRITE_ENUM_FIELD!(string, node, jointype, JoinType);
            WRITE_INT_FIELD!(string, node, joinmergedcols);
            WRITE_NODE_FIELD!(string, node, joinaliasvars);
            WRITE_NODE_FIELD!(string, node, joinleftcols);
            WRITE_NODE_FIELD!(string, node, joinrightcols);
            WRITE_NODE_FIELD!(string, node, join_using_alias);
        }
        RTEKind::RTE_FUNCTION => {
            WRITE_NODE_FIELD!(string, node, functions);
            WRITE_BOOL_FIELD!(string, node, funcordinality);
        }
        RTEKind::RTE_TABLEFUNC => {
            WRITE_NODE_FIELD!(string, node, tablefunc);
        }
        RTEKind::RTE_VALUES => {
            WRITE_NODE_FIELD!(string, node, values_lists);
            WRITE_NODE_FIELD!(string, node, coltypes);
            WRITE_NODE_FIELD!(string, node, coltypmods);
            WRITE_NODE_FIELD!(string, node, colcollations);
        }
        RTEKind::RTE_CTE => {
            WRITE_STRING_FIELD!(string, node, ctename);
            WRITE_UINT_FIELD!(string, node, ctelevelsup);
            WRITE_BOOL_FIELD!(string, node, self_reference);
            WRITE_NODE_FIELD!(string, node, coltypes);
            WRITE_NODE_FIELD!(string, node, coltypmods);
            WRITE_NODE_FIELD!(string, node, colcollations);
        }
        RTEKind::RTE_NAMEDTUPLESTORE => {
            WRITE_STRING_FIELD!(string, node, enrname);
            WRITE_FLOAT_FIELD!(string, node, enrtuples);
            WRITE_NODE_FIELD!(string, node, coltypes);
            WRITE_NODE_FIELD!(string, node, coltypmods);
            WRITE_NODE_FIELD!(string, node, colcollations);
            /* we re-use these RELATION fields, too: */
            WRITE_OID_FIELD!(string, node, relid);
        }
        RTEKind::RTE_RESULT => {
            /* no extra fields */
        }
        RTEKind::RTE_GROUP => {
            WRITE_NODE_FIELD!(string, node, groupexprs);
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized RTE kind: {}", node.rtekind as c_int);
        }
    }

    WRITE_BOOL_FIELD!(string, node, lateral);
    WRITE_BOOL_FIELD!(string, node, inFromCl);
    WRITE_NODE_FIELD!(string, node, securityQuals);
}

unsafe fn _outA_Expr(string: StringInfo, node: *const A_Expr) {
    let node = &*node;

    WRITE_NODE_TYPE!(string, c"A_EXPR".as_ptr());

    match node.kind {
        A_Expr_Kind::AEXPR_OP => {
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_OP_ANY => {
            appendStringInfoString(string, c" ANY".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_OP_ALL => {
            appendStringInfoString(string, c" ALL".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_DISTINCT => {
            appendStringInfoString(string, c" DISTINCT".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_NOT_DISTINCT => {
            appendStringInfoString(string, c" NOT_DISTINCT".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_NULLIF => {
            appendStringInfoString(string, c" NULLIF".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_IN => {
            appendStringInfoString(string, c" IN".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_LIKE => {
            appendStringInfoString(string, c" LIKE".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_ILIKE => {
            appendStringInfoString(string, c" ILIKE".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_SIMILAR => {
            appendStringInfoString(string, c" SIMILAR".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_BETWEEN => {
            appendStringInfoString(string, c" BETWEEN".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN => {
            appendStringInfoString(string, c" NOT_BETWEEN".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_BETWEEN_SYM => {
            appendStringInfoString(string, c" BETWEEN_SYM".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
            appendStringInfoString(string, c" NOT_BETWEEN_SYM".as_ptr());
            WRITE_NODE_FIELD!(string, node, name);
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized A_Expr_Kind: {}", node.kind as c_int);
        }
    }

    WRITE_NODE_FIELD!(string, node, lexpr);
    WRITE_NODE_FIELD!(string, node, rexpr);
    WRITE_LOCATION_FIELD!(string, node, rexpr_list_start);
    WRITE_LOCATION_FIELD!(string, node, rexpr_list_end);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outInteger(string: StringInfo, node: *const Integer) {
    let node = &*node;
    appendStringInfo(string, c"%d".as_ptr(), node.ival);
}

unsafe fn _outFloat(string: StringInfo, node: *const Float) {
    let node = &*node;
    /*
     * We assume the value is a valid numeric literal and so does not need
     * quoting.
     */
    appendStringInfoString(string, node.fval);
}

unsafe fn _outBoolean(string: StringInfo, node: *const Boolean) {
    let node = &*node;
    appendStringInfoString(
        string,
        if node.boolval {
            c"true".as_ptr()
        } else {
            c"false".as_ptr()
        },
    );
}

unsafe fn _outString(string: StringInfo, node: *const PgString) {
    let node = &*node;
    /*
     * We use outToken to provide escaping of the string's content, but we
     * don't want it to convert an empty string to '""', because we're putting
     * double quotes around the string already.
     */
    appendStringInfoChar(string, b'"' as c_char);
    if *node.sval != b'\0' as c_char {
        outToken(string, node.sval);
    }
    appendStringInfoChar(string, b'"' as c_char);
}

unsafe fn _outBitString(string: StringInfo, node: *const BitString) {
    let node = &*node;
    /*
     * The lexer will always produce a string starting with 'b' or 'x'.  There
     * might be characters following that that need escaping, but outToken
     * won't escape the 'b' or 'x'.  This is relied on by nodeTokenType.
     */
    Assert!(*node.bsval == b'b' as c_char || *node.bsval == b'x' as c_char);
    outToken(string, node.bsval);
}

unsafe fn _outA_Const(string: StringInfo, node: *const A_Const) {
    let node = &*node;

    WRITE_NODE_TYPE!(string, c"A_CONST".as_ptr());

    if node.isnull {
        appendStringInfoString(string, c" NULL".as_ptr());
    } else {
        appendStringInfoString(string, c" :val ".as_ptr());
        outNode(string, &node.val as *const _ as *const ::std::ffi::c_void);
    }
    WRITE_LOCATION_FIELD!(string, node, location);
}

// ---------------------------------------------------------------------------
// Generated per-node writer functions ported 1:1 from outfuncs.funcs.c.
// Struct types are referenced by fully-qualified path to avoid collisions with
// the local stub structs defined at the bottom of this file.
// ---------------------------------------------------------------------------

unsafe fn _outAlias(string: StringInfo, node: *const crate::nodes::primnodes::Alias) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ALIAS".as_ptr());
    WRITE_STRING_FIELD!(string, node, aliasname);
    WRITE_NODE_FIELD!(string, node, colnames);
}

unsafe fn _outRangeVar(string: StringInfo, node: *const crate::nodes::primnodes::RangeVar) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"RANGEVAR".as_ptr());
    WRITE_STRING_FIELD!(string, node, catalogname);
    WRITE_STRING_FIELD!(string, node, schemaname);
    WRITE_STRING_FIELD!(string, node, relname);
    WRITE_BOOL_FIELD!(string, node, inh);
    WRITE_CHAR_FIELD!(string, node, relpersistence);
    WRITE_NODE_FIELD!(string, node, alias);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outVar(string: StringInfo, node: *const crate::nodes::primnodes::Var) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"VAR".as_ptr());
    WRITE_INT_FIELD!(string, node, varno);
    WRITE_INT_FIELD!(string, node, varattno);
    WRITE_OID_FIELD!(string, node, vartype);
    WRITE_INT_FIELD!(string, node, vartypmod);
    WRITE_OID_FIELD!(string, node, varcollid);
    WRITE_BITMAPSET_FIELD!(string, node, varnullingrels);
    WRITE_UINT_FIELD!(string, node, varlevelsup);
    WRITE_ENUM_FIELD!(string, node, varreturningtype, VarReturningType);
    WRITE_UINT_FIELD!(string, node, varnosyn);
    WRITE_INT_FIELD!(string, node, varattnosyn);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outParam(string: StringInfo, node: *const crate::nodes::primnodes::Param) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"PARAM".as_ptr());
    WRITE_ENUM_FIELD!(string, node, paramkind, ParamKind);
    WRITE_INT_FIELD!(string, node, paramid);
    WRITE_OID_FIELD!(string, node, paramtype);
    WRITE_INT_FIELD!(string, node, paramtypmod);
    WRITE_OID_FIELD!(string, node, paramcollid);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outAggref(string: StringInfo, node: *const crate::nodes::primnodes::Aggref) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"AGGREF".as_ptr());
    WRITE_OID_FIELD!(string, node, aggfnoid);
    WRITE_OID_FIELD!(string, node, aggtype);
    WRITE_OID_FIELD!(string, node, aggcollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_OID_FIELD!(string, node, aggtranstype);
    WRITE_NODE_FIELD!(string, node, aggargtypes);
    WRITE_NODE_FIELD!(string, node, aggdirectargs);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_NODE_FIELD!(string, node, aggorder);
    WRITE_NODE_FIELD!(string, node, aggdistinct);
    WRITE_NODE_FIELD!(string, node, aggfilter);
    WRITE_BOOL_FIELD!(string, node, aggstar);
    WRITE_BOOL_FIELD!(string, node, aggvariadic);
    WRITE_CHAR_FIELD!(string, node, aggkind);
    WRITE_BOOL_FIELD!(string, node, aggpresorted);
    WRITE_UINT_FIELD!(string, node, agglevelsup);
    WRITE_ENUM_FIELD!(string, node, aggsplit, AggSplit);
    WRITE_INT_FIELD!(string, node, aggno);
    WRITE_INT_FIELD!(string, node, aggtransno);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outGroupingFunc(string: StringInfo, node: *const crate::nodes::primnodes::GroupingFunc) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"GROUPINGFUNC".as_ptr());
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_NODE_FIELD!(string, node, refs);
    WRITE_NODE_FIELD!(string, node, cols);
    WRITE_UINT_FIELD!(string, node, agglevelsup);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outWindowFunc(string: StringInfo, node: *const crate::nodes::primnodes::WindowFunc) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"WINDOWFUNC".as_ptr());
    WRITE_OID_FIELD!(string, node, winfnoid);
    WRITE_OID_FIELD!(string, node, wintype);
    WRITE_OID_FIELD!(string, node, wincollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_NODE_FIELD!(string, node, aggfilter);
    WRITE_NODE_FIELD!(string, node, runCondition);
    WRITE_UINT_FIELD!(string, node, winref);
    WRITE_BOOL_FIELD!(string, node, winstar);
    WRITE_BOOL_FIELD!(string, node, winagg);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outWindowFuncRunCondition(
    string: StringInfo,
    node: *const crate::nodes::primnodes::WindowFuncRunCondition,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"WINDOWFUNCRUNCONDITION".as_ptr());
    WRITE_OID_FIELD!(string, node, opno);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_BOOL_FIELD!(string, node, wfunc_left);
    WRITE_NODE_FIELD!(string, node, arg);
}

unsafe fn _outMergeSupportFunc(
    string: StringInfo,
    node: *const crate::nodes::primnodes::MergeSupportFunc,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"MERGESUPPORTFUNC".as_ptr());
    WRITE_OID_FIELD!(string, node, msftype);
    WRITE_OID_FIELD!(string, node, msfcollid);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outSubscriptingRef(
    string: StringInfo,
    node: *const crate::nodes::primnodes::SubscriptingRef,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SUBSCRIPTINGREF".as_ptr());
    WRITE_OID_FIELD!(string, node, refcontainertype);
    WRITE_OID_FIELD!(string, node, refelemtype);
    WRITE_OID_FIELD!(string, node, refrestype);
    WRITE_INT_FIELD!(string, node, reftypmod);
    WRITE_OID_FIELD!(string, node, refcollid);
    WRITE_NODE_FIELD!(string, node, refupperindexpr);
    WRITE_NODE_FIELD!(string, node, reflowerindexpr);
    WRITE_NODE_FIELD!(string, node, refexpr);
    WRITE_NODE_FIELD!(string, node, refassgnexpr);
}

unsafe fn _outFuncExpr(string: StringInfo, node: *const crate::nodes::primnodes::FuncExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"FUNCEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, funcid);
    WRITE_OID_FIELD!(string, node, funcresulttype);
    WRITE_BOOL_FIELD!(string, node, funcretset);
    WRITE_BOOL_FIELD!(string, node, funcvariadic);
    WRITE_ENUM_FIELD!(string, node, funcformat, CoercionForm);
    WRITE_OID_FIELD!(string, node, funccollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outNamedArgExpr(string: StringInfo, node: *const crate::nodes::primnodes::NamedArgExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"NAMEDARGEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_STRING_FIELD!(string, node, name);
    WRITE_INT_FIELD!(string, node, argnumber);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outOpExpr(string: StringInfo, node: *const crate::nodes::primnodes::OpExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"OPEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, opno);
    WRITE_OID_FIELD!(string, node, opfuncid);
    WRITE_OID_FIELD!(string, node, opresulttype);
    WRITE_BOOL_FIELD!(string, node, opretset);
    WRITE_OID_FIELD!(string, node, opcollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outDistinctExpr(string: StringInfo, node: *const crate::nodes::primnodes::DistinctExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"DISTINCTEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, opno);
    WRITE_OID_FIELD!(string, node, opfuncid);
    WRITE_OID_FIELD!(string, node, opresulttype);
    WRITE_BOOL_FIELD!(string, node, opretset);
    WRITE_OID_FIELD!(string, node, opcollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outNullIfExpr(string: StringInfo, node: *const crate::nodes::primnodes::NullIfExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"NULLIFEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, opno);
    WRITE_OID_FIELD!(string, node, opfuncid);
    WRITE_OID_FIELD!(string, node, opresulttype);
    WRITE_BOOL_FIELD!(string, node, opretset);
    WRITE_OID_FIELD!(string, node, opcollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outScalarArrayOpExpr(
    string: StringInfo,
    node: *const crate::nodes::primnodes::ScalarArrayOpExpr,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SCALARARRAYOPEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, opno);
    WRITE_OID_FIELD!(string, node, opfuncid);
    WRITE_OID_FIELD!(string, node, hashfuncid);
    WRITE_OID_FIELD!(string, node, negfuncid);
    WRITE_BOOL_FIELD!(string, node, useOr);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outSubLink(string: StringInfo, node: *const crate::nodes::primnodes::SubLink) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SUBLINK".as_ptr());
    WRITE_ENUM_FIELD!(string, node, subLinkType, SubLinkType);
    WRITE_INT_FIELD!(string, node, subLinkId);
    WRITE_NODE_FIELD!(string, node, testexpr);
    WRITE_NODE_FIELD!(string, node, operName);
    WRITE_NODE_FIELD!(string, node, subselect);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outFieldSelect(string: StringInfo, node: *const crate::nodes::primnodes::FieldSelect) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"FIELDSELECT".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_INT_FIELD!(string, node, fieldnum);
    WRITE_OID_FIELD!(string, node, resulttype);
    WRITE_INT_FIELD!(string, node, resulttypmod);
    WRITE_OID_FIELD!(string, node, resultcollid);
}

unsafe fn _outFieldStore(string: StringInfo, node: *const crate::nodes::primnodes::FieldStore) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"FIELDSTORE".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_NODE_FIELD!(string, node, newvals);
    WRITE_NODE_FIELD!(string, node, fieldnums);
    WRITE_OID_FIELD!(string, node, resulttype);
}

unsafe fn _outRelabelType(string: StringInfo, node: *const crate::nodes::primnodes::RelabelType) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"RELABELTYPE".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_OID_FIELD!(string, node, resulttype);
    WRITE_INT_FIELD!(string, node, resulttypmod);
    WRITE_OID_FIELD!(string, node, resultcollid);
    WRITE_ENUM_FIELD!(string, node, relabelformat, CoercionForm);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCoerceViaIO(string: StringInfo, node: *const crate::nodes::primnodes::CoerceViaIO) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COERCEVIAIO".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_OID_FIELD!(string, node, resulttype);
    WRITE_OID_FIELD!(string, node, resultcollid);
    WRITE_ENUM_FIELD!(string, node, coerceformat, CoercionForm);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outArrayCoerceExpr(
    string: StringInfo,
    node: *const crate::nodes::primnodes::ArrayCoerceExpr,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ARRAYCOERCEEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_NODE_FIELD!(string, node, elemexpr);
    WRITE_OID_FIELD!(string, node, resulttype);
    WRITE_INT_FIELD!(string, node, resulttypmod);
    WRITE_OID_FIELD!(string, node, resultcollid);
    WRITE_ENUM_FIELD!(string, node, coerceformat, CoercionForm);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outConvertRowtypeExpr(
    string: StringInfo,
    node: *const crate::nodes::primnodes::ConvertRowtypeExpr,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"CONVERTROWTYPEEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_OID_FIELD!(string, node, resulttype);
    WRITE_ENUM_FIELD!(string, node, convertformat, CoercionForm);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCollateExpr(string: StringInfo, node: *const crate::nodes::primnodes::CollateExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COLLATEEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_OID_FIELD!(string, node, collOid);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCaseExpr(string: StringInfo, node: *const crate::nodes::primnodes::CaseExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"CASEEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, casetype);
    WRITE_OID_FIELD!(string, node, casecollid);
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_NODE_FIELD!(string, node, defresult);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCaseWhen(string: StringInfo, node: *const crate::nodes::primnodes::CaseWhen) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"CASEWHEN".as_ptr());
    WRITE_NODE_FIELD!(string, node, expr);
    WRITE_NODE_FIELD!(string, node, result);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCaseTestExpr(string: StringInfo, node: *const crate::nodes::primnodes::CaseTestExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"CASETESTEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, typeId);
    WRITE_INT_FIELD!(string, node, typeMod);
    WRITE_OID_FIELD!(string, node, collation);
}

unsafe fn _outArrayExpr(string: StringInfo, node: *const crate::nodes::primnodes::ArrayExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ARRAYEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, array_typeid);
    WRITE_OID_FIELD!(string, node, array_collid);
    WRITE_OID_FIELD!(string, node, element_typeid);
    WRITE_NODE_FIELD!(string, node, elements);
    WRITE_BOOL_FIELD!(string, node, multidims);
    WRITE_LOCATION_FIELD!(string, node, list_start);
    WRITE_LOCATION_FIELD!(string, node, list_end);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outRowExpr(string: StringInfo, node: *const crate::nodes::primnodes::RowExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ROWEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_OID_FIELD!(string, node, row_typeid);
    WRITE_ENUM_FIELD!(string, node, row_format, CoercionForm);
    WRITE_NODE_FIELD!(string, node, colnames);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outRowCompareExpr(
    string: StringInfo,
    node: *const crate::nodes::primnodes::RowCompareExpr,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ROWCOMPAREEXPR".as_ptr());
    /* cmptype is a plain int (CompareType) in this port */
    WRITE_INT_FIELD!(string, node, cmptype);
    WRITE_NODE_FIELD!(string, node, opnos);
    WRITE_NODE_FIELD!(string, node, opfamilies);
    WRITE_NODE_FIELD!(string, node, inputcollids);
    WRITE_NODE_FIELD!(string, node, largs);
    WRITE_NODE_FIELD!(string, node, rargs);
}

unsafe fn _outCoalesceExpr(string: StringInfo, node: *const crate::nodes::primnodes::CoalesceExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COALESCEEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, coalescetype);
    WRITE_OID_FIELD!(string, node, coalescecollid);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outMinMaxExpr(string: StringInfo, node: *const crate::nodes::primnodes::MinMaxExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"MINMAXEXPR".as_ptr());
    WRITE_OID_FIELD!(string, node, minmaxtype);
    WRITE_OID_FIELD!(string, node, minmaxcollid);
    WRITE_OID_FIELD!(string, node, inputcollid);
    WRITE_ENUM_FIELD!(string, node, op, MinMaxOp);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outSQLValueFunction(
    string: StringInfo,
    node: *const crate::nodes::primnodes::SQLValueFunction,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SQLVALUEFUNCTION".as_ptr());
    WRITE_ENUM_FIELD!(string, node, op, SQLValueFunctionOp);
    /* result-type field is named `type` (r#type) in this port */
    appendStringInfo(string, " :type %u\0".as_ptr() as *const c_char, node.r#type);
    WRITE_INT_FIELD!(string, node, typmod);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outNullTest(string: StringInfo, node: *const crate::nodes::primnodes::NullTest) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"NULLTEST".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_ENUM_FIELD!(string, node, nulltesttype, NullTestType);
    WRITE_BOOL_FIELD!(string, node, argisrow);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outBooleanTest(string: StringInfo, node: *const crate::nodes::primnodes::BooleanTest) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"BOOLEANTEST".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_ENUM_FIELD!(string, node, booltesttype, BoolTestType);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outMergeAction(string: StringInfo, node: *const crate::nodes::primnodes::MergeAction) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"MERGEACTION".as_ptr());
    WRITE_ENUM_FIELD!(string, node, matchKind, MergeMatchKind);
    WRITE_ENUM_FIELD!(string, node, commandType, CmdType);
    WRITE_ENUM_FIELD!(string, node, r#override, OverridingKind);
    WRITE_NODE_FIELD!(string, node, qual);
    WRITE_NODE_FIELD!(string, node, targetList);
    WRITE_NODE_FIELD!(string, node, updateColnos);
}

unsafe fn _outCoerceToDomain(
    string: StringInfo,
    node: *const crate::nodes::primnodes::CoerceToDomain,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COERCETODOMAIN".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_OID_FIELD!(string, node, resulttype);
    WRITE_INT_FIELD!(string, node, resulttypmod);
    WRITE_OID_FIELD!(string, node, resultcollid);
    WRITE_ENUM_FIELD!(string, node, coercionformat, CoercionForm);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCoerceToDomainValue(
    string: StringInfo,
    node: *const crate::nodes::primnodes::CoerceToDomainValue,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COERCETODOMAINVALUE".as_ptr());
    WRITE_OID_FIELD!(string, node, typeId);
    WRITE_INT_FIELD!(string, node, typeMod);
    WRITE_OID_FIELD!(string, node, collation);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outSetToDefault(string: StringInfo, node: *const crate::nodes::primnodes::SetToDefault) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SETTODEFAULT".as_ptr());
    WRITE_OID_FIELD!(string, node, typeId);
    WRITE_INT_FIELD!(string, node, typeMod);
    WRITE_OID_FIELD!(string, node, collation);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outInferenceElem(
    string: StringInfo,
    node: *const crate::nodes::primnodes::InferenceElem,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"INFERENCEELEM".as_ptr());
    WRITE_NODE_FIELD!(string, node, expr);
    WRITE_OID_FIELD!(string, node, infercollid);
    WRITE_OID_FIELD!(string, node, inferopclass);
}

unsafe fn _outTargetEntry(string: StringInfo, node: *const crate::nodes::primnodes::TargetEntry) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"TARGETENTRY".as_ptr());
    WRITE_NODE_FIELD!(string, node, expr);
    WRITE_INT_FIELD!(string, node, resno);
    WRITE_STRING_FIELD!(string, node, resname);
    WRITE_UINT_FIELD!(string, node, ressortgroupref);
    WRITE_OID_FIELD!(string, node, resorigtbl);
    WRITE_INT_FIELD!(string, node, resorigcol);
    WRITE_BOOL_FIELD!(string, node, resjunk);
}

unsafe fn _outRangeTblRef(string: StringInfo, node: *const crate::nodes::primnodes::RangeTblRef) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"RANGETBLREF".as_ptr());
    WRITE_INT_FIELD!(string, node, rtindex);
}

unsafe fn _outJoinExpr(string: StringInfo, node: *const crate::nodes::primnodes::JoinExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"JOINEXPR".as_ptr());
    WRITE_ENUM_FIELD!(string, node, jointype, JoinType);
    WRITE_BOOL_FIELD!(string, node, isNatural);
    WRITE_NODE_FIELD!(string, node, larg);
    WRITE_NODE_FIELD!(string, node, rarg);
    WRITE_NODE_FIELD!(string, node, usingClause);
    WRITE_NODE_FIELD!(string, node, join_using_alias);
    WRITE_NODE_FIELD!(string, node, quals);
    WRITE_NODE_FIELD!(string, node, alias);
    WRITE_INT_FIELD!(string, node, rtindex);
}

unsafe fn _outFromExpr(string: StringInfo, node: *const crate::nodes::primnodes::FromExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"FROMEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, fromlist);
    WRITE_NODE_FIELD!(string, node, quals);
}

unsafe fn _outOnConflictExpr(
    string: StringInfo,
    node: *const crate::nodes::primnodes::OnConflictExpr,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ONCONFLICTEXPR".as_ptr());
    WRITE_ENUM_FIELD!(string, node, action, OnConflictAction);
    WRITE_NODE_FIELD!(string, node, arbiterElems);
    WRITE_NODE_FIELD!(string, node, arbiterWhere);
    WRITE_OID_FIELD!(string, node, constraint);
    WRITE_NODE_FIELD!(string, node, onConflictSet);
    WRITE_NODE_FIELD!(string, node, onConflictWhere);
    WRITE_INT_FIELD!(string, node, exclRelIndex);
    WRITE_NODE_FIELD!(string, node, exclRelTlist);
}

unsafe fn _outQuery(string: StringInfo, node: *const crate::nodes::parsenodes::Query) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"QUERY".as_ptr());
    WRITE_ENUM_FIELD!(string, node, commandType, CmdType);
    WRITE_ENUM_FIELD!(string, node, querySource, QuerySource);
    WRITE_BOOL_FIELD!(string, node, canSetTag);
    WRITE_NODE_FIELD!(string, node, utilityStmt);
    WRITE_INT_FIELD!(string, node, resultRelation);
    WRITE_BOOL_FIELD!(string, node, hasAggs);
    WRITE_BOOL_FIELD!(string, node, hasWindowFuncs);
    WRITE_BOOL_FIELD!(string, node, hasTargetSRFs);
    WRITE_BOOL_FIELD!(string, node, hasSubLinks);
    WRITE_BOOL_FIELD!(string, node, hasDistinctOn);
    WRITE_BOOL_FIELD!(string, node, hasRecursive);
    WRITE_BOOL_FIELD!(string, node, hasModifyingCTE);
    WRITE_BOOL_FIELD!(string, node, hasForUpdate);
    WRITE_BOOL_FIELD!(string, node, hasRowSecurity);
    WRITE_BOOL_FIELD!(string, node, hasGroupRTE);
    WRITE_BOOL_FIELD!(string, node, isReturn);
    WRITE_NODE_FIELD!(string, node, cteList);
    WRITE_NODE_FIELD!(string, node, rtable);
    WRITE_NODE_FIELD!(string, node, rteperminfos);
    WRITE_NODE_FIELD!(string, node, jointree);
    WRITE_NODE_FIELD!(string, node, mergeActionList);
    WRITE_INT_FIELD!(string, node, mergeTargetRelation);
    WRITE_NODE_FIELD!(string, node, mergeJoinCondition);
    WRITE_NODE_FIELD!(string, node, targetList);
    WRITE_ENUM_FIELD!(string, node, r#override, OverridingKind);
    WRITE_NODE_FIELD!(string, node, onConflict);
    WRITE_STRING_FIELD!(string, node, returningOldAlias);
    WRITE_STRING_FIELD!(string, node, returningNewAlias);
    WRITE_NODE_FIELD!(string, node, returningList);
    WRITE_NODE_FIELD!(string, node, groupClause);
    WRITE_BOOL_FIELD!(string, node, groupDistinct);
    WRITE_NODE_FIELD!(string, node, groupingSets);
    WRITE_NODE_FIELD!(string, node, havingQual);
    WRITE_NODE_FIELD!(string, node, windowClause);
    WRITE_NODE_FIELD!(string, node, distinctClause);
    WRITE_NODE_FIELD!(string, node, sortClause);
    WRITE_NODE_FIELD!(string, node, limitOffset);
    WRITE_NODE_FIELD!(string, node, limitCount);
    WRITE_ENUM_FIELD!(string, node, limitOption, LimitOption);
    WRITE_NODE_FIELD!(string, node, rowMarks);
    WRITE_NODE_FIELD!(string, node, setOperations);
    WRITE_NODE_FIELD!(string, node, constraintDeps);
    WRITE_NODE_FIELD!(string, node, withCheckOptions);
    WRITE_LOCATION_FIELD!(string, node, stmt_location);
    WRITE_LOCATION_FIELD!(string, node, stmt_len);
}

unsafe fn _outTypeName(string: StringInfo, node: *const crate::nodes::parsenodes::TypeName) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"TYPENAME".as_ptr());
    WRITE_NODE_FIELD!(string, node, names);
    WRITE_OID_FIELD!(string, node, typeOid);
    WRITE_BOOL_FIELD!(string, node, setof);
    WRITE_BOOL_FIELD!(string, node, pct_type);
    WRITE_NODE_FIELD!(string, node, typmods);
    WRITE_INT_FIELD!(string, node, typemod);
    WRITE_NODE_FIELD!(string, node, arrayBounds);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outColumnRef(string: StringInfo, node: *const crate::nodes::parsenodes::ColumnRef) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COLUMNREF".as_ptr());
    WRITE_NODE_FIELD!(string, node, fields);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outParamRef(string: StringInfo, node: *const crate::nodes::parsenodes::ParamRef) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"PARAMREF".as_ptr());
    WRITE_INT_FIELD!(string, node, number);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outTypeCast(string: StringInfo, node: *const crate::nodes::parsenodes::TypeCast) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"TYPECAST".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_NODE_FIELD!(string, node, typeName);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outCollateClause(string: StringInfo, node: *const crate::nodes::parsenodes::CollateClause) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"COLLATECLAUSE".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_NODE_FIELD!(string, node, collname);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outRoleSpec(string: StringInfo, node: *const crate::nodes::parsenodes::RoleSpec) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"ROLESPEC".as_ptr());
    WRITE_ENUM_FIELD!(string, node, roletype, RoleSpecType);
    WRITE_STRING_FIELD!(string, node, rolename);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outFuncCall(string: StringInfo, node: *const crate::nodes::parsenodes::FuncCall) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"FUNCCALL".as_ptr());
    WRITE_NODE_FIELD!(string, node, funcname);
    WRITE_NODE_FIELD!(string, node, args);
    WRITE_NODE_FIELD!(string, node, agg_order);
    WRITE_NODE_FIELD!(string, node, agg_filter);
    WRITE_NODE_FIELD!(string, node, over);
    WRITE_BOOL_FIELD!(string, node, agg_within_group);
    WRITE_BOOL_FIELD!(string, node, agg_star);
    WRITE_BOOL_FIELD!(string, node, agg_distinct);
    WRITE_BOOL_FIELD!(string, node, func_variadic);
    WRITE_ENUM_FIELD!(string, node, funcformat, CoercionForm);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outA_Star(string: StringInfo, _node: *const crate::nodes::parsenodes::A_Star) {
    WRITE_NODE_TYPE!(string, c"A_STAR".as_ptr());
}

unsafe fn _outA_Indices(string: StringInfo, node: *const crate::nodes::parsenodes::A_Indices) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"A_INDICES".as_ptr());
    WRITE_BOOL_FIELD!(string, node, is_slice);
    WRITE_NODE_FIELD!(string, node, lidx);
    WRITE_NODE_FIELD!(string, node, uidx);
}

unsafe fn _outA_Indirection(
    string: StringInfo,
    node: *const crate::nodes::parsenodes::A_Indirection,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"A_INDIRECTION".as_ptr());
    WRITE_NODE_FIELD!(string, node, arg);
    WRITE_NODE_FIELD!(string, node, indirection);
}

unsafe fn _outA_ArrayExpr(string: StringInfo, node: *const crate::nodes::parsenodes::A_ArrayExpr) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"A_ARRAYEXPR".as_ptr());
    WRITE_NODE_FIELD!(string, node, elements);
    WRITE_LOCATION_FIELD!(string, node, list_start);
    WRITE_LOCATION_FIELD!(string, node, list_end);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outResTarget(string: StringInfo, node: *const crate::nodes::parsenodes::ResTarget) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"RESTARGET".as_ptr());
    WRITE_STRING_FIELD!(string, node, name);
    WRITE_NODE_FIELD!(string, node, indirection);
    WRITE_NODE_FIELD!(string, node, val);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outMultiAssignRef(
    string: StringInfo,
    node: *const crate::nodes::parsenodes::MultiAssignRef,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"MULTIASSIGNREF".as_ptr());
    WRITE_NODE_FIELD!(string, node, source);
    WRITE_INT_FIELD!(string, node, colno);
    WRITE_INT_FIELD!(string, node, ncolumns);
}

unsafe fn _outSortBy(string: StringInfo, node: *const crate::nodes::parsenodes::SortBy) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SORTBY".as_ptr());
    WRITE_NODE_FIELD!(string, node, node);
    WRITE_ENUM_FIELD!(string, node, sortby_dir, SortByDir);
    WRITE_ENUM_FIELD!(string, node, sortby_nulls, SortByNulls);
    WRITE_NODE_FIELD!(string, node, useOp);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outWindowDef(string: StringInfo, node: *const crate::nodes::parsenodes::WindowDef) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"WINDOWDEF".as_ptr());
    WRITE_STRING_FIELD!(string, node, name);
    WRITE_STRING_FIELD!(string, node, refname);
    WRITE_NODE_FIELD!(string, node, partitionClause);
    WRITE_NODE_FIELD!(string, node, orderClause);
    WRITE_INT_FIELD!(string, node, frameOptions);
    WRITE_NODE_FIELD!(string, node, startOffset);
    WRITE_NODE_FIELD!(string, node, endOffset);
    WRITE_LOCATION_FIELD!(string, node, location);
}

unsafe fn _outRangeSubselect(
    string: StringInfo,
    node: *const crate::nodes::parsenodes::RangeSubselect,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"RANGESUBSELECT".as_ptr());
    WRITE_BOOL_FIELD!(string, node, lateral);
    WRITE_NODE_FIELD!(string, node, subquery);
    WRITE_NODE_FIELD!(string, node, alias);
}

unsafe fn _outRangeFunction(string: StringInfo, node: *const crate::nodes::parsenodes::RangeFunction) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"RANGEFUNCTION".as_ptr());
    WRITE_BOOL_FIELD!(string, node, lateral);
    WRITE_BOOL_FIELD!(string, node, ordinality);
    WRITE_BOOL_FIELD!(string, node, is_rowsfrom);
    WRITE_NODE_FIELD!(string, node, functions);
    WRITE_NODE_FIELD!(string, node, alias);
    WRITE_NODE_FIELD!(string, node, coldeflist);
}

unsafe fn _outSortGroupClause(
    string: StringInfo,
    node: *const crate::nodes::parsenodes::SortGroupClause,
) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"SORTGROUPCLAUSE".as_ptr());
    WRITE_UINT_FIELD!(string, node, tleSortGroupRef);
    WRITE_OID_FIELD!(string, node, eqop);
    WRITE_OID_FIELD!(string, node, sortop);
    WRITE_BOOL_FIELD!(string, node, reverse_sort);
    WRITE_BOOL_FIELD!(string, node, nulls_first);
    WRITE_BOOL_FIELD!(string, node, hashable);
}

unsafe fn _outGroupingSet(string: StringInfo, node: *const crate::nodes::parsenodes::GroupingSet) {
    let node = &*node;
    WRITE_NODE_TYPE!(string, c"GROUPINGSET".as_ptr());
    WRITE_ENUM_FIELD!(string, node, kind, GroupingSetKind);
    WRITE_NODE_FIELD!(string, node, content);
    WRITE_LOCATION_FIELD!(string, node, location);
}

// Helper to compare the local c_int node tag against a NodeTag enum value.
#[inline]
fn tag(t: NodeTag) -> c_int {
    t as c_int
}

/*
 * outNode -
 *	  converts a Node into ascii string and append it to 'string'
 */
pub unsafe fn outNode(string: StringInfo, obj: *const ::std::ffi::c_void) {
    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    if obj.is_null() {
        appendStringInfoString(string, c"<>".as_ptr());
    } else if IsA!(obj, T_List) || IsA!(obj, T_IntList) || IsA!(obj, T_OidList) || IsA!(obj, T_XidList) {
        _outList(string, obj as *const List);
    }
    /* nodeRead does not want to see { } around these! */
    else if IsA!(obj, T_Integer) {
        _outInteger(string, obj as *const Integer);
    } else if IsA!(obj, T_Float) {
        _outFloat(string, obj as *const Float);
    } else if IsA!(obj, T_Boolean) {
        _outBoolean(string, obj as *const Boolean);
    } else if IsA!(obj, T_String) {
        _outString(string, obj as *const PgString);
    } else if IsA!(obj, T_BitString) {
        _outBitString(string, obj as *const BitString);
    } else if IsA!(obj, T_Bitmapset) {
        outBitmapset(string, obj as *const Bitmapset);
    } else {
        appendStringInfoChar(string, b'{' as c_char);
        let t = nodeTag(obj);
        // Generated dispatch ported from outfuncs.switch.c.  Only the node
        // types whose _out function has been translated are listed; everything
        // else falls through to the WARNING arm below.
        if t == tag(NodeTag::T_Alias) {
            _outAlias(string, obj as *const _);
        } else if t == tag(NodeTag::T_RangeVar) {
            _outRangeVar(string, obj as *const _);
        } else if t == tag(NodeTag::T_Var) {
            _outVar(string, obj as *const _);
        } else if t == tag(NodeTag::T_Const) {
            _outConst(string, obj as *const Const);
        } else if t == tag(NodeTag::T_Param) {
            _outParam(string, obj as *const _);
        } else if t == tag(NodeTag::T_Aggref) {
            _outAggref(string, obj as *const _);
        } else if t == tag(NodeTag::T_GroupingFunc) {
            _outGroupingFunc(string, obj as *const _);
        } else if t == tag(NodeTag::T_WindowFunc) {
            _outWindowFunc(string, obj as *const _);
        } else if t == tag(NodeTag::T_WindowFuncRunCondition) {
            _outWindowFuncRunCondition(string, obj as *const _);
        } else if t == tag(NodeTag::T_MergeSupportFunc) {
            _outMergeSupportFunc(string, obj as *const _);
        } else if t == tag(NodeTag::T_SubscriptingRef) {
            _outSubscriptingRef(string, obj as *const _);
        } else if t == tag(NodeTag::T_FuncExpr) {
            _outFuncExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_NamedArgExpr) {
            _outNamedArgExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_OpExpr) {
            _outOpExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_DistinctExpr) {
            _outDistinctExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_NullIfExpr) {
            _outNullIfExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_ScalarArrayOpExpr) {
            _outScalarArrayOpExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_BoolExpr) {
            _outBoolExpr(string, obj as *const BoolExpr);
        } else if t == tag(NodeTag::T_SubLink) {
            _outSubLink(string, obj as *const _);
        } else if t == tag(NodeTag::T_FieldSelect) {
            _outFieldSelect(string, obj as *const _);
        } else if t == tag(NodeTag::T_FieldStore) {
            _outFieldStore(string, obj as *const _);
        } else if t == tag(NodeTag::T_RelabelType) {
            _outRelabelType(string, obj as *const _);
        } else if t == tag(NodeTag::T_CoerceViaIO) {
            _outCoerceViaIO(string, obj as *const _);
        } else if t == tag(NodeTag::T_ArrayCoerceExpr) {
            _outArrayCoerceExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_ConvertRowtypeExpr) {
            _outConvertRowtypeExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_CollateExpr) {
            _outCollateExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_CaseExpr) {
            _outCaseExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_CaseWhen) {
            _outCaseWhen(string, obj as *const _);
        } else if t == tag(NodeTag::T_CaseTestExpr) {
            _outCaseTestExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_ArrayExpr) {
            _outArrayExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_RowExpr) {
            _outRowExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_RowCompareExpr) {
            _outRowCompareExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_CoalesceExpr) {
            _outCoalesceExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_MinMaxExpr) {
            _outMinMaxExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_SQLValueFunction) {
            _outSQLValueFunction(string, obj as *const _);
        } else if t == tag(NodeTag::T_NullTest) {
            _outNullTest(string, obj as *const _);
        } else if t == tag(NodeTag::T_BooleanTest) {
            _outBooleanTest(string, obj as *const _);
        } else if t == tag(NodeTag::T_MergeAction) {
            _outMergeAction(string, obj as *const _);
        } else if t == tag(NodeTag::T_CoerceToDomain) {
            _outCoerceToDomain(string, obj as *const _);
        } else if t == tag(NodeTag::T_CoerceToDomainValue) {
            _outCoerceToDomainValue(string, obj as *const _);
        } else if t == tag(NodeTag::T_SetToDefault) {
            _outSetToDefault(string, obj as *const _);
        } else if t == tag(NodeTag::T_InferenceElem) {
            _outInferenceElem(string, obj as *const _);
        } else if t == tag(NodeTag::T_TargetEntry) {
            _outTargetEntry(string, obj as *const _);
        } else if t == tag(NodeTag::T_RangeTblRef) {
            _outRangeTblRef(string, obj as *const _);
        } else if t == tag(NodeTag::T_JoinExpr) {
            _outJoinExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_FromExpr) {
            _outFromExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_OnConflictExpr) {
            _outOnConflictExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_Query) {
            _outQuery(string, obj as *const _);
        } else if t == tag(NodeTag::T_TypeName) {
            _outTypeName(string, obj as *const _);
        } else if t == tag(NodeTag::T_ColumnRef) {
            _outColumnRef(string, obj as *const _);
        } else if t == tag(NodeTag::T_ParamRef) {
            _outParamRef(string, obj as *const _);
        } else if t == tag(NodeTag::T_A_Expr) {
            _outA_Expr(string, obj as *const A_Expr);
        } else if t == tag(NodeTag::T_A_Const) {
            _outA_Const(string, obj as *const A_Const);
        } else if t == tag(NodeTag::T_TypeCast) {
            _outTypeCast(string, obj as *const _);
        } else if t == tag(NodeTag::T_CollateClause) {
            _outCollateClause(string, obj as *const _);
        } else if t == tag(NodeTag::T_RoleSpec) {
            _outRoleSpec(string, obj as *const _);
        } else if t == tag(NodeTag::T_FuncCall) {
            _outFuncCall(string, obj as *const _);
        } else if t == tag(NodeTag::T_A_Star) {
            _outA_Star(string, obj as *const _);
        } else if t == tag(NodeTag::T_A_Indices) {
            _outA_Indices(string, obj as *const _);
        } else if t == tag(NodeTag::T_A_Indirection) {
            _outA_Indirection(string, obj as *const _);
        } else if t == tag(NodeTag::T_A_ArrayExpr) {
            _outA_ArrayExpr(string, obj as *const _);
        } else if t == tag(NodeTag::T_ResTarget) {
            _outResTarget(string, obj as *const _);
        } else if t == tag(NodeTag::T_MultiAssignRef) {
            _outMultiAssignRef(string, obj as *const _);
        } else if t == tag(NodeTag::T_SortBy) {
            _outSortBy(string, obj as *const _);
        } else if t == tag(NodeTag::T_WindowDef) {
            _outWindowDef(string, obj as *const _);
        } else if t == tag(NodeTag::T_RangeSubselect) {
            _outRangeSubselect(string, obj as *const _);
        } else if t == tag(NodeTag::T_RangeFunction) {
            _outRangeFunction(string, obj as *const _);
        } else if t == tag(NodeTag::T_RangeTblEntry) {
            _outRangeTblEntry(string, obj as *const RangeTblEntry);
        } else if t == tag(NodeTag::T_SortGroupClause) {
            _outSortGroupClause(string, obj as *const _);
        } else if t == tag(NodeTag::T_GroupingSet) {
            _outGroupingSet(string, obj as *const _);
        } else if t == tag(NodeTag::T_ForeignKeyOptInfo) {
            _outForeignKeyOptInfo(string, obj as *const ForeignKeyOptInfo);
        } else if t == tag(NodeTag::T_EquivalenceClass) {
            _outEquivalenceClass(string, obj as *const EquivalenceClass);
        } else if t == tag(NodeTag::T_ExtensibleNode) {
            _outExtensibleNode(string, obj as *const ExtensibleNode);
        } else {
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
        appendStringInfoChar(string, b'}' as c_char);
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
    let mut string: StringInfoData = core::mem::zeroed();
    let save_write_location_fields: bool;

    save_write_location_fields = write_location_fields;
    write_location_fields = write_loc_fields;

    /* see stringinfo.h for an explanation of this maneuver */
    initStringInfo(&mut string);
    outNode(&mut string, obj);

    write_location_fields = save_write_location_fields;

    // string.data
    StringInfoData_data(&string)
}

unsafe fn StringInfoData_data(_str: *const StringInfoData) -> *mut c_char {
    (*_str).data
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
    let mut string: StringInfoData = core::mem::zeroed();

    /* see stringinfo.h for an explanation of this maneuver */
    initStringInfo(&mut string);
    outBitmapset(&mut string, bms);
    StringInfoData_data(&string)
}

// ---------------------------------------------------------------------------
// Local stub node types referenced above (real definitions live in the
// parsenodes/primnodes/pathnodes modules once ported).
// ---------------------------------------------------------------------------

use crate::nodes::value::String as PgString;

unsafe fn lnext(_l: *const List, _cell: *const ListCell) -> *const ListCell {
    crate::nodes::pg_list::lnext(_l as _, _cell as _) as _
}
unsafe fn lfirst(_cell: *const ListCell) -> *const ::std::ffi::c_void {
    crate::nodes::pg_list::lfirst(_cell as _) as _
}
unsafe fn lfirst_int(_cell: *const ListCell) -> c_int {
    crate::nodes::pg_list::lfirst_int(_cell as _) as _
}
unsafe fn lfirst_oid(_cell: *const ListCell) -> Oid {
    crate::nodes::pg_list::lfirst_oid(_cell as _) as _
}
unsafe fn lfirst_xid(_cell: *const ListCell) -> Oid {
    crate::nodes::pg_list::lfirst_xid(_cell as _) as _
}
unsafe fn nodeTag(_obj: *const ::std::ffi::c_void) -> c_int {
    *(_obj as *const c_int)
}

// Stub node-type structs (faithful field sets used above).

pub use crate::nodes::primnodes::Const;

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
