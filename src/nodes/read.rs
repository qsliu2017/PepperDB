//! Translation of postgres/src/backend/nodes/read.c
//!
//! Routines to convert a string (legal ascii representation of node) back to
//! nodes.  This is the node-string READER infrastructure -- the lexer behind
//! `stringToNode`.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! #include mapping:
//!   #include "postgres.h"              -> use crate::prelude::*
//!   #include <ctype.h>                 -> local isdigit helper
//!   #include "common/string.h"         -> strtoint via libc strtol (see notes)
//!   #include "nodes/bitmapset.h"       -> crate::nodes::bitmapset
//!   #include "nodes/pg_list.h"         -> crate::nodes::pg_list
//!   #include "nodes/readfuncs.h"       -> crate::nodes::readfuncs (parseNodeString STUB)
//!   #include "nodes/value.h"           -> crate::nodes::value
//!
//! This unit additionally hosts the reader helpers that the C source keeps in
//! the *generated* readfuncs.c, but which are hand-written, self-contained
//! lexer-level routines logically belonging with this file:
//!     readDatum, _readBitmapset, readBitmapset,
//!     readAttrNumberCols, readOidCols, readIntCols, readBoolCols
//! (from postgres/src/backend/nodes/readfuncs.c).  These are FULLY REAL here.
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The C globals `pg_strtok_ptr` are modeled with a file-`static mut`.  Unlike
//!   C, we also keep a static length register is unnecessary -- pg_strtok still
//!   returns (ptr, len) -- but we DO need the read cursor as a static, exactly
//!   like the C `static const char *pg_strtok_ptr`.
//!
//! * Pointers here are raw `*const c_char` into the original input string.  The
//!   caller of `stringToNode` must keep the string alive for the duration of
//!   the read (true in C as well, since pg_strtok never copies).
//!
//! * `nodeTokenType` returns a plain `c_int`.  For value-node tokens it returns
//!   the `NodeTag` discriminant cast to int (matching the C `NodeTag` return);
//!   for structural tokens it returns the C sentinels RIGHT_PAREN/LEFT_PAREN/
//!   LEFT_BRACE/OTHER_TOKEN (1000000+N).  `nodeRead` switches on `(int) type`
//!   exactly as the C does.
//!
//! * `strtoint(s, &end, 10)` (from common/string.h) is a range-checked int32
//!   parse setting errno on overflow.  We bind libc `strtol` and replicate the
//!   range check against `INT_MIN..=INT_MAX` plus the `ERANGE` long-overflow.
//!
//! * `parseNodeString()` lives in the generated readfuncs.c (crate::nodes::
//!   readfuncs), which is NOT yet ported.  Its single call site in `nodeRead`
//!   is STUBBED with `unimplemented!()` + TODO(pg-port); the rest of nodeRead
//!   (tokenizing + dispatch) is real.

use crate::prelude::*;
use core::ffi::c_char;

use crate::nodes::bitmapset::{bms_add_member, Bitmapset};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{lappend, lappend_int, lappend_oid, lappend_xid, List, NIL};
use crate::nodes::value::{makeBitString, makeBoolean, makeFloat, makeInteger, makeString};

// C library functions used by the lexer / datum reader.
extern "C" {
    fn strtol(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;
    fn strtoul(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn atoi(nptr: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// `errno` access for the strtoint range check (mirrors C `errno == ERANGE`).
extern "C" {
    fn __error() -> *mut c_int; // macOS `errno` location
}

const ERANGE: c_int = 34;

#[inline]
unsafe fn errno_get() -> c_int {
    *__error()
}

#[inline]
unsafe fn errno_set(v: c_int) {
    *__error() = v;
}

/* Static state for pg_strtok */
static mut pg_strtok_ptr: *const c_char = null();

/*
 * State flag that determines how readfuncs.c should treat location fields.
 *
 * In C this is `#ifdef DEBUG_NODE_TESTS_ENABLED bool restore_location_fields`.
 * We keep it unconditionally as a static for readfuncs.c to consult.
 */
pub static mut restore_location_fields: bool = false;

/* C macro sentinels for non-NodeTag token types (read.c) */
const RIGHT_PAREN: c_int = 1000000 + 1;
const LEFT_PAREN: c_int = 1000000 + 2;
const LEFT_BRACE: c_int = 1000000 + 3;
const OTHER_TOKEN: c_int = 1000000 + 4;

/* Convenience: NodeTag discriminant as c_int (matches C's `(int) NodeTag`). */
#[inline]
const fn tag(t: NodeTag) -> c_int {
    t as c_int
}

/* ctype.h isdigit on an unsigned char (ASCII). */
#[inline]
fn isdigit_c(c: c_char) -> bool {
    let u = c as u8;
    u.is_ascii_digit()
}

/*
 * stringToNode -
 *	  builds a Node tree from its string representation (assumed valid)
 *
 * restore_loc_fields instructs readfuncs.c whether to restore location
 * fields rather than set them to -1.
 */
unsafe fn stringToNodeInternal(str: *const c_char, restore_loc_fields: bool) -> *mut c_void {
    let retval: *mut c_void;

    /*
     * We save and restore the pre-existing state of pg_strtok. This makes the
     * world safe for re-entrant invocation of stringToNode.
     */
    let save_strtok = pg_strtok_ptr;

    pg_strtok_ptr = str; /* point pg_strtok at the string to read */

    let save_restore_location_fields = restore_location_fields;
    restore_location_fields = restore_loc_fields;

    retval = nodeRead(null(), 0); /* do the reading */

    pg_strtok_ptr = save_strtok;

    restore_location_fields = save_restore_location_fields;

    retval
}

/*
 * Externally visible entry points
 */
pub unsafe fn stringToNode(str: *const c_char) -> *mut c_void {
    stringToNodeInternal(str, false)
}

pub unsafe fn stringToNodeWithLocations(str: *const c_char) -> *mut c_void {
    stringToNodeInternal(str, true)
}

/*****************************************************************************
 *
 * the lisp token parser
 *
 *****************************************************************************/

/*
 * pg_strtok --- retrieve next "token" from a string.
 *
 * Works kinda like strtok, except it never modifies the source string.
 * (Instead of storing nulls into the string, the length of the token
 * is returned to the caller.)
 *
 * Returns a pointer to the start of the next token, and the length of the
 * token (including any embedded backslashes!) in *length.  If there are
 * no more tokens, NULL and 0 are returned.
 *
 * NOTE: this routine doesn't remove backslashes; the caller must do so
 * if necessary (see "debackslash").
 */
pub unsafe fn pg_strtok(length: *mut c_int) -> *const c_char {
    let mut local_str: *const c_char; /* working pointer to string */
    let ret_str: *const c_char; /* start of token to return */

    local_str = pg_strtok_ptr;

    while *local_str == b' ' as c_char
        || *local_str == b'\n' as c_char
        || *local_str == b'\t' as c_char
    {
        local_str = local_str.add(1);
    }

    if *local_str == 0 {
        *length = 0;
        pg_strtok_ptr = local_str;
        return null(); /* no more tokens */
    }

    /*
     * Now pointing at start of next token.
     */
    ret_str = local_str;

    if *local_str == b'(' as c_char
        || *local_str == b')' as c_char
        || *local_str == b'{' as c_char
        || *local_str == b'}' as c_char
    {
        /* special 1-character token */
        local_str = local_str.add(1);
    } else {
        /* Normal token, possibly containing backslashes */
        while *local_str != 0
            && *local_str != b' ' as c_char
            && *local_str != b'\n' as c_char
            && *local_str != b'\t' as c_char
            && *local_str != b'(' as c_char
            && *local_str != b')' as c_char
            && *local_str != b'{' as c_char
            && *local_str != b'}' as c_char
        {
            if *local_str == b'\\' as c_char && *local_str.add(1) != 0 {
                local_str = local_str.add(2);
            } else {
                local_str = local_str.add(1);
            }
        }
    }

    let mut len = local_str.offset_from(ret_str) as c_int;

    /* Recognize special case for "empty" token */
    if len == 2 && *ret_str == b'<' as c_char && *ret_str.add(1) == b'>' as c_char {
        len = 0;
    }

    *length = len;

    pg_strtok_ptr = local_str;

    ret_str
}

/*
 * debackslash -
 *	  create a palloc'd string holding the given token.
 *	  any protective backslashes in the token are removed.
 */
pub unsafe fn debackslash(mut token: *const c_char, mut length: c_int) -> *mut c_char {
    let result = palloc((length + 1) as Size) as *mut c_char;
    let mut ptr = result;

    while length > 0 {
        if *token == b'\\' as c_char && length > 1 {
            token = token.add(1);
            length -= 1;
        }
        *ptr = *token;
        ptr = ptr.add(1);
        token = token.add(1);
        length -= 1;
    }
    *ptr = 0;
    result
}

/*
 * strtoint shim (common/string.h):
 *	  parse a base-10 int32 starting at `s`, set *endptr to first unparsed
 *	  char, and set errno=ERANGE if the value does not fit in an int32.
 *	  Returns the (possibly clamped) value; callers in this file only use it
 *	  for the syntax/range check in nodeTokenType.
 */
unsafe fn strtoint(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_int {
    let val = strtol(s, endptr, base);
    if val < c_int::MIN as c_long || val > c_int::MAX as c_long {
        errno_set(ERANGE);
        if val < 0 {
            c_int::MIN
        } else {
            c_int::MAX
        }
    } else {
        val as c_int
    }
}

/*
 * nodeTokenType -
 *	  returns the type of the node token contained in token.
 *	  It returns one of the following valid NodeTags:
 *		T_Integer, T_Float, T_Boolean, T_String, T_BitString
 *	  and some of its own:
 *		RIGHT_PAREN, LEFT_PAREN, LEFT_BRACE, OTHER_TOKEN
 *
 *	  Assumption: the ascii representation is legal
 */
unsafe fn nodeTokenType(token: *const c_char, length: c_int) -> c_int {
    let retval: c_int;

    /*
     * Check if the token is a number
     */
    let mut numptr = token;
    let mut numlen = length;
    if *numptr == b'+' as c_char || *numptr == b'-' as c_char {
        numptr = numptr.add(1);
        numlen -= 1;
    }
    if (numlen > 0 && isdigit_c(*numptr))
        || (numlen > 1 && *numptr == b'.' as c_char && isdigit_c(*numptr.add(1)))
    {
        /*
         * Yes.  Figure out whether it is integral or float; this requires both
         * a syntax check and a range check.  strtoint() can do both for us.
         */
        let mut endptr: *mut c_char = null_mut();

        errno_set(0);
        let _ = strtoint(numptr, &mut endptr, 10);
        if endptr != token.add(length as usize) as *mut c_char || errno_get() == ERANGE {
            return tag(NodeTag::T_Float);
        }
        return tag(NodeTag::T_Integer);
    }
    /*
     * these three cases do not need length checks, since pg_strtok() will
     * always treat them as single-byte tokens
     */
    else if *token == b'(' as c_char {
        retval = LEFT_PAREN;
    } else if *token == b')' as c_char {
        retval = RIGHT_PAREN;
    } else if *token == b'{' as c_char {
        retval = LEFT_BRACE;
    } else if (length == 4 && strncmp(token, c"true".as_ptr(), 4) == 0)
        || (length == 5 && strncmp(token, c"false".as_ptr(), 5) == 0)
    {
        retval = tag(NodeTag::T_Boolean);
    } else if *token == b'"' as c_char && length > 1 && *token.add((length - 1) as usize) == b'"' as c_char {
        retval = tag(NodeTag::T_String);
    } else if *token == b'b' as c_char || *token == b'x' as c_char {
        retval = tag(NodeTag::T_BitString);
    } else {
        retval = OTHER_TOKEN;
    }
    retval
}

/*
 * nodeRead -
 *	  Slightly higher-level reader.
 *
 * This routine applies some semantic knowledge on top of the purely lexical
 * tokenizer pg_strtok().  It can read
 *	* Value token nodes (integers, floats, booleans, or strings);
 *	* General nodes (via parseNodeString() from readfuncs.c);
 *	* Lists of the above;
 *	* Lists of integers, OIDs, or TransactionIds.
 *
 * External callers should always pass NULL/0 for the arguments.
 */
pub unsafe fn nodeRead(mut token: *const c_char, mut tok_len: c_int) -> *mut c_void {
    let result: *mut Node;

    if token.is_null() {
        /* need to read a token? */
        token = pg_strtok(&mut tok_len);

        if token.is_null() {
            /* end of input */
            return null_mut();
        }
    }

    let r#type = nodeTokenType(token, tok_len);

    match r#type {
        LEFT_BRACE => {
            result = crate::nodes::readfuncs::parseNodeString();
            token = pg_strtok(&mut tok_len);
            if token.is_null() || *token != b'}' as c_char {
                crate::elog!(crate::utils::elog::ERROR, "did not find '}}' at end of input node");
            }
        }
        LEFT_PAREN => {
            let mut l: *mut List = NIL;

            /*----------
             * Could be an integer list:	(i int int ...)
             * or an OID list:				(o int int ...)
             * or an XID list:				(x int int ...)
             * or a bitmapset:				(b int int ...)
             * or a list of nodes/values:	(node node ...)
             *----------
             */
            token = pg_strtok(&mut tok_len);
            if token.is_null() {
                elog!(ERROR, "unterminated List structure");
            }
            if tok_len == 1 && *token == b'i' as c_char {
                /* List of integers */
                loop {
                    let mut endptr: *mut c_char = null_mut();

                    token = pg_strtok(&mut tok_len);
                    if token.is_null() {
                        elog!(ERROR, "unterminated List structure");
                    }
                    if *token == b')' as c_char {
                        break;
                    }
                    let val = strtol(token, &mut endptr, 10) as c_int;
                    if endptr != token.add(tok_len as usize) as *mut c_char {
                        elog!(ERROR, "unrecognized integer: \"{}\"", token_str(token, tok_len));
                    }
                    l = lappend_int(l, val);
                }
                result = l as *mut Node;
            } else if tok_len == 1 && *token == b'o' as c_char {
                /* List of OIDs */
                loop {
                    let mut endptr: *mut c_char = null_mut();

                    token = pg_strtok(&mut tok_len);
                    if token.is_null() {
                        elog!(ERROR, "unterminated List structure");
                    }
                    if *token == b')' as c_char {
                        break;
                    }
                    let val = strtoul(token, &mut endptr, 10) as Oid;
                    if endptr != token.add(tok_len as usize) as *mut c_char {
                        elog!(ERROR, "unrecognized OID: \"{}\"", token_str(token, tok_len));
                    }
                    l = lappend_oid(l, val);
                }
                result = l as *mut Node;
            } else if tok_len == 1 && *token == b'x' as c_char {
                /* List of TransactionIds */
                loop {
                    let mut endptr: *mut c_char = null_mut();

                    token = pg_strtok(&mut tok_len);
                    if token.is_null() {
                        elog!(ERROR, "unterminated List structure");
                    }
                    if *token == b')' as c_char {
                        break;
                    }
                    let val = strtoul(token, &mut endptr, 10) as TransactionId;
                    if endptr != token.add(tok_len as usize) as *mut c_char {
                        elog!(ERROR, "unrecognized Xid: \"{}\"", token_str(token, tok_len));
                    }
                    l = lappend_xid(l, val);
                }
                result = l as *mut Node;
            } else if tok_len == 1 && *token == b'b' as c_char {
                /* Bitmapset -- see also _readBitmapset() */
                let mut bms: *mut Bitmapset = null_mut();

                loop {
                    let mut endptr: *mut c_char = null_mut();

                    token = pg_strtok(&mut tok_len);
                    if token.is_null() {
                        elog!(ERROR, "unterminated Bitmapset structure");
                    }
                    if tok_len == 1 && *token == b')' as c_char {
                        break;
                    }
                    let val = strtol(token, &mut endptr, 10) as c_int;
                    if endptr != token.add(tok_len as usize) as *mut c_char {
                        elog!(ERROR, "unrecognized integer: \"{}\"", token_str(token, tok_len));
                    }
                    bms = bms_add_member(bms, val);
                }
                result = bms as *mut Node;
            } else {
                /* List of other node types */
                loop {
                    /* We have already scanned next token... */
                    if *token == b')' as c_char {
                        break;
                    }
                    l = lappend(l, nodeRead(token, tok_len));
                    token = pg_strtok(&mut tok_len);
                    if token.is_null() {
                        elog!(ERROR, "unterminated List structure");
                    }
                }
                result = l as *mut Node;
            }
        }
        RIGHT_PAREN => {
            elog!(ERROR, "unexpected right parenthesis");
            unreachable!();
        }
        OTHER_TOKEN => {
            if tok_len == 0 {
                /* must be "<>" --- represents a null pointer */
                result = null_mut();
            } else {
                elog!(ERROR, "unrecognized token: \"{}\"", token_str(token, tok_len));
                unreachable!();
            }
        }
        t if t == tag(NodeTag::T_Integer) => {
            /*
             * we know that the token terminates on a char atoi will stop at
             */
            result = makeInteger(atoi(token)) as *mut Node;
        }
        t if t == tag(NodeTag::T_Float) => {
            let fval = palloc((tok_len + 1) as Size) as *mut c_char;

            memcpy(fval as *mut c_void, token as *const c_void, tok_len as usize);
            *fval.add(tok_len as usize) = 0;
            result = makeFloat(fval) as *mut Node;
        }
        t if t == tag(NodeTag::T_Boolean) => {
            result = makeBoolean(*token == b't' as c_char) as *mut Node;
        }
        t if t == tag(NodeTag::T_String) => {
            /* need to remove leading and trailing quotes, and backslashes */
            result = makeString(debackslash(token.add(1), tok_len - 2)) as *mut Node;
        }
        t if t == tag(NodeTag::T_BitString) => {
            /* need to remove backslashes, but there are no quotes */
            result = makeBitString(debackslash(token, tok_len)) as *mut Node;
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", r#type);
            unreachable!();
        }
    }

    result as *mut c_void
}

/*
 * Helper: render a (ptr, len) token as a Rust string for error messages,
 * standing in for C's `"%.*s"` formatting.
 */
unsafe fn token_str(token: *const c_char, length: c_int) -> std::string::String {
    if token.is_null() || length <= 0 {
        return std::string::String::new();
    }
    let bytes = core::slice::from_raw_parts(token as *const u8, length as usize);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

/*****************************************************************************
 *
 * reader helpers that the C source keeps in the generated readfuncs.c,
 * but which are hand-written lexer-level routines (readfuncs.c).
 *
 *****************************************************************************/

/* atoui(x) := (unsigned int) strtoul(x, NULL, 10)   [readfuncs.c] */
#[inline]
unsafe fn atoui(x: *const c_char) -> c_uint {
    strtoul(x, null_mut(), 10) as c_uint
}

/* atooid(x) := (Oid) strtoul(x, NULL, 10)   [postgres_ext.h] */
#[inline]
unsafe fn atooid(x: *const c_char) -> Oid {
    strtoul(x, null_mut(), 10) as Oid
}

/* strtobool(x) := ((*(x) == 't') ? true : false)   [readfuncs.c] */
#[inline]
unsafe fn strtobool(x: *const c_char) -> bool {
    *x == b't' as c_char
}

/*
 * _readBitmapset
 *
 * Note: this code is used in contexts where we know that a Bitmapset is
 * expected.  There is equivalent code in nodeRead() that can read a Bitmapset
 * when we come across one in other contexts.
 */
pub unsafe fn _readBitmapset() -> *mut Bitmapset {
    let mut result: *mut Bitmapset = null_mut();

    let mut length: c_int = 0;
    let mut token: *const c_char;

    token = pg_strtok(&mut length);
    if token.is_null() {
        elog!(ERROR, "incomplete Bitmapset structure");
    }
    if length != 1 || *token != b'(' as c_char {
        elog!(ERROR, "unrecognized token: \"{}\"", token_str(token, length));
    }

    token = pg_strtok(&mut length);
    if token.is_null() {
        elog!(ERROR, "incomplete Bitmapset structure");
    }
    if length != 1 || *token != b'b' as c_char {
        elog!(ERROR, "unrecognized token: \"{}\"", token_str(token, length));
    }

    loop {
        let mut endptr: *mut c_char = null_mut();

        token = pg_strtok(&mut length);
        if token.is_null() {
            elog!(ERROR, "unterminated Bitmapset structure");
        }
        if length == 1 && *token == b')' as c_char {
            break;
        }
        let val = strtol(token, &mut endptr, 10) as c_int;
        if endptr != token.add(length as usize) as *mut c_char {
            elog!(ERROR, "unrecognized integer: \"{}\"", token_str(token, length));
        }
        result = bms_add_member(result, val);
    }

    result
}

/*
 * We export this function for use by extensions that define extensible nodes.
 * That's somewhat historical, though, because calling nodeRead() will work.
 */
pub unsafe fn readBitmapset() -> *mut Bitmapset {
    _readBitmapset()
}

/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate Datum.
 * The string representation embeds length info, but not byValue, so we must be
 * told that.
 */
pub unsafe fn readDatum(typbyval: bool) -> Datum {
    let mut token_length: c_int = 0;
    let mut token: *const c_char;
    let res: Datum;
    let s: *mut c_char;

    /*
     * read the actual length of the value
     */
    token = pg_strtok(&mut token_length);
    let length = atoui(token) as Size;

    token = pg_strtok(&mut token_length); /* read the '[' */
    if token.is_null() || *token != b'[' as c_char {
        elog!(
            ERROR,
            "expected \"[\" to start datum, but got \"{}\"; length = {}",
            if token.is_null() { "[NULL]".into() } else { token_str(token, token_length) },
            length
        );
    }

    if typbyval {
        if length > core::mem::size_of::<Datum>() as Size {
            elog!(ERROR, "byval datum but length = {}", length);
        }
        let mut tmp: Datum = 0;
        let sp = (&mut tmp) as *mut Datum as *mut c_char;
        let mut i: Size = 0;
        while i < core::mem::size_of::<Datum>() as Size {
            token = pg_strtok(&mut token_length);
            *sp.add(i as usize) = atoi(token) as c_char;
            i += 1;
        }
        res = tmp;
    } else if (length as isize) <= 0 {
        res = 0 as Datum; /* (Datum) NULL */
    } else {
        s = palloc(length) as *mut c_char;
        let mut i: Size = 0;
        while i < length {
            token = pg_strtok(&mut token_length);
            *s.add(i as usize) = atoi(token) as c_char;
            i += 1;
        }
        res = PointerGetDatum(s as *const c_void);
    }

    token = pg_strtok(&mut token_length); /* read the ']' */
    if token.is_null() || *token != b']' as c_char {
        elog!(
            ERROR,
            "expected \"]\" to end datum, but got \"{}\"; length = {}",
            if token.is_null() { "[NULL]".into() } else { token_str(token, token_length) },
            length
        );
    }

    res
}

/*
 * common implementation for scalar-array-reading functions
 *
 * The data format is either "<>" for a NULL pointer (in which case numCols is
 * ignored) or "(item item item)" where the number of items must equal numCols.
 *
 * In C this is the READ_SCALAR_ARRAY macro; here it is a generic helper
 * parameterized over the element type and a per-token conversion closure.
 */
unsafe fn read_scalar_array<T: Copy>(
    numCols: c_int,
    convfunc: impl Fn(*const c_char) -> T,
) -> *mut T {
    let mut length: c_int = 0;
    let mut token: *const c_char;

    token = pg_strtok(&mut length);
    if token.is_null() {
        elog!(ERROR, "incomplete scalar array");
    }
    if length == 0 {
        return null_mut(); /* it was "<>", so return NULL pointer */
    }
    if length != 1 || *token != b'(' as c_char {
        elog!(ERROR, "unrecognized token: \"{}\"", token_str(token, length));
    }
    let vals = palloc(numCols as Size * core::mem::size_of::<T>() as Size) as *mut T;
    let mut i: c_int = 0;
    while i < numCols {
        token = pg_strtok(&mut length);
        if token.is_null() || *token == b')' as c_char {
            elog!(ERROR, "incomplete scalar array");
        }
        *vals.add(i as usize) = convfunc(token);
        i += 1;
    }
    token = pg_strtok(&mut length);
    if token.is_null() || length != 1 || *token != b')' as c_char {
        elog!(ERROR, "incomplete scalar array");
    }
    vals
}

/*
 * Note: these functions are exported in nodes.h for possible use by
 * extensions, so don't mess too much with their names or API.
 */
pub unsafe fn readAttrNumberCols(numCols: c_int) -> *mut int16 {
    read_scalar_array(numCols, |t| atoi(t) as int16)
}

pub unsafe fn readOidCols(numCols: c_int) -> *mut Oid {
    read_scalar_array(numCols, |t| atooid(t))
}

pub unsafe fn readIntCols(numCols: c_int) -> *mut c_int {
    read_scalar_array(numCols, |t| atoi(t))
}

pub unsafe fn readBoolCols(numCols: c_int) -> *mut bool {
    read_scalar_array(numCols, |t| strtobool(t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::bms_is_member;
    // The intVal!/boolVal!/strVal! accessor macros expand to castNode!(Integer/
    // Boolean/String, ...), so those value-node types must be in scope here.
    use crate::nodes::value::{Boolean, Integer, String};
    use crate::{boolVal, intVal, strVal};

    /*
     * Point pg_strtok at a NUL-terminated &CStr literal and run a closure.
     * pg_strtok_ptr is a process-global `static mut`, so serialize these tests
     * with a mutex to avoid races under cargo's parallel test threads.
     */
    static STRTOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    unsafe fn with_input<R>(input: &core::ffi::CStr, f: impl FnOnce() -> R) -> R {
        let _guard = STRTOK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        pg_strtok_ptr = input.as_ptr();
        f()
    }

    #[test]
    fn pg_strtok_tokenizes_in_order() {
        unsafe {
            with_input(c"(123 abc)", || {
                let mut len: c_int = 0;

                let t = pg_strtok(&mut len);
                assert_eq!(len, 1);
                assert_eq!(*t, b'(' as c_char);

                let t = pg_strtok(&mut len);
                assert_eq!(token_str(t, len), "123");

                let t = pg_strtok(&mut len);
                assert_eq!(token_str(t, len), "abc");

                let t = pg_strtok(&mut len);
                assert_eq!(len, 1);
                assert_eq!(*t, b')' as c_char);

                let t = pg_strtok(&mut len);
                assert!(t.is_null());
                assert_eq!(len, 0);
            });
        }
    }

    #[test]
    fn pg_strtok_empty_token_for_diamond() {
        unsafe {
            with_input(c"<> next", || {
                let mut len: c_int = 0;
                let t = pg_strtok(&mut len);
                assert!(!t.is_null());
                assert_eq!(len, 0); /* "<>" -> length 0, non-NULL ptr */

                let t = pg_strtok(&mut len);
                assert_eq!(token_str(t, len), "next");
            });
        }
    }

    #[test]
    fn node_token_type_classifies() {
        unsafe {
            assert_eq!(nodeTokenType(c"123".as_ptr(), 3), tag(NodeTag::T_Integer));
            assert_eq!(nodeTokenType(c"-45".as_ptr(), 3), tag(NodeTag::T_Integer));
            assert_eq!(nodeTokenType(c"1.5".as_ptr(), 3), tag(NodeTag::T_Float));
            assert_eq!(nodeTokenType(c".5".as_ptr(), 2), tag(NodeTag::T_Float));
            /* integer too big for int32 -> float */
            assert_eq!(
                nodeTokenType(c"99999999999999".as_ptr(), 14),
                tag(NodeTag::T_Float)
            );
            assert_eq!(nodeTokenType(c"true".as_ptr(), 4), tag(NodeTag::T_Boolean));
            assert_eq!(nodeTokenType(c"false".as_ptr(), 5), tag(NodeTag::T_Boolean));
            assert_eq!(nodeTokenType(c"\"hi\"".as_ptr(), 4), tag(NodeTag::T_String));
            assert_eq!(nodeTokenType(c"b101".as_ptr(), 4), tag(NodeTag::T_BitString));
            assert_eq!(nodeTokenType(c"(".as_ptr(), 1), LEFT_PAREN);
            assert_eq!(nodeTokenType(c")".as_ptr(), 1), RIGHT_PAREN);
            assert_eq!(nodeTokenType(c"{".as_ptr(), 1), LEFT_BRACE);
            assert_eq!(nodeTokenType(c"foo".as_ptr(), 3), OTHER_TOKEN);
        }
    }

    #[test]
    fn debackslash_unescapes() {
        unsafe {
            /* "a\\)b" (token chars: a \ ) b) -> "a)b" */
            let tok = c"a\\)b";
            let out = debackslash(tok.as_ptr(), 4);
            assert_eq!(token_str(out, 3), "a)b");
            assert_eq!(*out.add(3), 0);
        }
    }

    #[test]
    fn read_bitmapset_round_trips() {
        unsafe {
            with_input(c"(b 1 5 9)", || {
                let bms = _readBitmapset() as *const Bitmapset;
                assert!(bms_is_member(1, bms));
                assert!(bms_is_member(5, bms));
                assert!(bms_is_member(9, bms));
                assert!(!bms_is_member(2, bms));
            });
        }
    }

    #[test]
    fn node_read_value_nodes() {
        unsafe {
            with_input(c"42", || {
                let p = nodeRead(null(), 0);
                assert_eq!(intVal!(p), 42);
            });
            with_input(c"true", || {
                let p = nodeRead(null(), 0);
                assert!(boolVal!(p));
            });
            with_input(c"\"hello\"", || {
                let p = nodeRead(null(), 0) as *mut c_void;
                let s = strVal!(p);
                assert_eq!(token_str(s, 5), "hello");
            });
            with_input(c"<>", || {
                let p = nodeRead(null(), 0);
                assert!(p.is_null());
            });
        }
    }

    #[test]
    fn node_read_int_list() {
        unsafe {
            with_input(c"(i 10 20 30)", || {
                let l = nodeRead(null(), 0) as *mut List;
                assert!(!l.is_null());
                assert_eq!(crate::nodes::pg_list::list_length(l), 3);
            });
        }
    }

    #[test]
    fn read_int_cols_reads_array() {
        unsafe {
            with_input(c"(7 8 9)", || {
                let v = readIntCols(3);
                assert!(!v.is_null());
                assert_eq!(*v.add(0), 7);
                assert_eq!(*v.add(1), 8);
                assert_eq!(*v.add(2), 9);
            });
            with_input(c"<>", || {
                let v = readIntCols(0);
                assert!(v.is_null());
            });
        }
    }

    #[test]
    fn read_datum_byref_round_trips() {
        unsafe {
            /* 3 bytes: 65 66 67 -> "ABC" */
            with_input(c"3 [ 65 66 67 ]", || {
                let d = readDatum(false);
                let s = d as *const c_char;
                assert_eq!(*s.add(0), 65);
                assert_eq!(*s.add(1), 66);
                assert_eq!(*s.add(2), 67);
            });
        }
    }
}
