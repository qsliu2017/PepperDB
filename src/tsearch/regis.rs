//! tsearch/regis.rs - fast regex subset matcher used by the ISpell dictionary.
//!
//! Source: postgres/src/backend/tsearch/regis.c
//! Merged header: postgres/src/include/tsearch/dicts/regis.h
//!
//! #include mapping:
//!   - "postgres.h"               -> crate::prelude::* (Datum, c-types, palloc/pfree,
//!                                    elog!, null_mut, ...).
//!   - "tsearch/dicts/regis.h"    -> merged below (RegisNode, Regis, RSF_ONEOF/RSF_NONEOF,
//!                                    RNHDRSZ, function decls).
//!   - "tsearch/ts_locale.h"      -> crate::tsearch::ts_locale (t_iseq, t_isalpha_cstr,
//!                                    ts_copychar_cstr).
//!   - "mb/pg_wchar.h"            -> crate::mb::mbutils (pg_mblen_cstr), re-exported via
//!                                    ts_locale as well.
//!
//! REAL vs STUB: ALL functions here are REAL (RS_isRegis, RS_compile, RS_free,
//! RS_execute, plus the static helpers newRegisNode / mb_strchr).  They build on
//! the REAL ts_locale classification helpers and pg_mblen_cstr.
//!
//! Layout note: upstream RegisNode packs type:2/len:16/unused:14 into a single
//! uint32 bitfield, and stores the matched-char bytes in a FLEXIBLE_ARRAY_MEMBER
//! `data[]` right after the header.  Rust has no flexible array members, so we
//! lay the node out manually: a fixed header (allocated as RNHDRSZ + len + 1
//! bytes via palloc0, exactly like the C) followed by `len`+1 data bytes that we
//! address through a helper that returns a raw pointer past the header.  `type`
//! and `len` are kept as plain u32 fields (the :2 / :16 widths only ever hold
//! RSF_ONEOF/RSF_NONEOF and a small byte count, so full-width storage is
//! behaviourally identical).

use crate::prelude::*;

use crate::mb::mbutils::pg_mblen_cstr;
use crate::tsearch::ts_locale::{t_isalpha_cstr, t_iseq, ts_copychar_cstr};

// ----------------------------------------------------------------------------
// Merged regis.h definitions.
// ----------------------------------------------------------------------------

/// `#define RSF_ONEOF 1`
pub const RSF_ONEOF: u32 = 1;
/// `#define RSF_NONEOF 2`
pub const RSF_NONEOF: u32 = 2;

/// One node in a compiled regis: a single `[...]` class (or a literal letter,
/// treated as a one-element class), with the matched bytes stored inline after
/// the header.
///
/// Upstream:
/// ```c
/// typedef struct RegisNode {
///     uint32 type:2, len:16, unused:14;
///     struct RegisNode *next;
///     unsigned char data[FLEXIBLE_ARRAY_MEMBER];
/// } RegisNode;
/// ```
#[repr(C)]
pub struct RegisNode {
    /// RSF_ONEOF or RSF_NONEOF (upstream `type:2`).
    pub r#type: uint32,
    /// Number of data bytes currently stored (upstream `len:16`).
    pub len: uint32,
    /// `struct RegisNode *next`.
    pub next: *mut RegisNode,
    // The FLEXIBLE_ARRAY_MEMBER `data[]` lives in the palloc'd bytes immediately
    // following this header; see regis_node_data() / RNHDRSZ.
}

/// `#define RNHDRSZ (offsetof(RegisNode,data))`
///
/// The header size, i.e. the offset of the inline `data[]` array.  Upstream this
/// is `offsetof(RegisNode, data)`; here it is the full size of our header
/// struct (the data bytes are allocated immediately after).
#[inline]
pub fn RNHDRSZ() -> Size {
    core::mem::size_of::<RegisNode>()
}

/// Raw pointer to the inline `data[]` bytes that follow a node header.
#[inline]
unsafe fn regis_node_data(ptr: *mut RegisNode) -> *mut c_char {
    (ptr as *mut u8).add(RNHDRSZ()) as *mut c_char
}

/// A compiled regis pattern: a linked list of RegisNode plus match metadata.
///
/// Upstream:
/// ```c
/// typedef struct Regis {
///     RegisNode *node;
///     uint32 issuffix:1, nchar:16, unused:15;
/// } Regis;
/// ```
#[repr(C)]
pub struct Regis {
    /// Head of the node list.
    pub node: *mut RegisNode,
    /// Match against the suffix of the string rather than the prefix
    /// (upstream `issuffix:1`).
    pub issuffix: uint32,
    /// Number of nodes / characters to match (upstream `nchar:16`).
    pub nchar: uint32,
}

// State machine constants (file-local in regis.c).
const RS_IN_ONEOF: c_int = 1;
const RS_IN_ONEOF_IN: c_int = 2;
const RS_IN_NONEOF: c_int = 3;
const RS_IN_WAIT: c_int = 4;

// ----------------------------------------------------------------------------
// RS_isRegis - validate that a pattern is a simple regis.
// Keep this in sync with RS_compile!
// ----------------------------------------------------------------------------

/// Test whether a regex is of the subset supported here.
pub unsafe fn RS_isRegis(str: *const c_char) -> bool {
    let mut state = RS_IN_WAIT;
    let mut c = str;

    while *c != 0 {
        if state == RS_IN_WAIT {
            if t_isalpha_cstr(c) != 0 {
                /* okay */
            } else if t_iseq(c, b'[' as c_char) {
                state = RS_IN_ONEOF;
            } else {
                return false;
            }
        } else if state == RS_IN_ONEOF {
            if t_iseq(c, b'^' as c_char) {
                state = RS_IN_NONEOF;
            } else if t_isalpha_cstr(c) != 0 {
                state = RS_IN_ONEOF_IN;
            } else {
                return false;
            }
        } else if state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF {
            if t_isalpha_cstr(c) != 0 {
                /* okay */
            } else if t_iseq(c, b']' as c_char) {
                state = RS_IN_WAIT;
            } else {
                return false;
            }
        } else {
            elog!(ERROR, "internal error in RS_isRegis: state {}", state);
        }
        c = c.add(pg_mblen_cstr(c) as usize);
    }

    state == RS_IN_WAIT
}

// ----------------------------------------------------------------------------
// newRegisNode (static) - allocate a node with `len`+1 inline data bytes.
// ----------------------------------------------------------------------------

unsafe fn newRegisNode(prev: *mut RegisNode, len: c_int) -> *mut RegisNode {
    // palloc0(RNHDRSZ + len + 1): header + inline data, zeroed.
    let ptr = palloc0(RNHDRSZ() + len as Size + 1) as *mut RegisNode;
    if !prev.is_null() {
        (*prev).next = ptr;
    }
    ptr
}

// ----------------------------------------------------------------------------
// RS_compile - parse a "[...]" pattern into a RegisNode list.
// ----------------------------------------------------------------------------

/// Parse `str` (a simple regis pattern) into the node list of `r`.
pub unsafe fn RS_compile(r: *mut Regis, issuffix: bool, str: *const c_char) {
    // len = strlen(str)
    let mut len: c_int = 0;
    while *str.add(len as usize) != 0 {
        len += 1;
    }
    let mut state = RS_IN_WAIT;
    let mut c = str;
    let mut ptr: *mut RegisNode = null_mut();

    // memset(r, 0, sizeof(Regis));
    core::ptr::write_bytes(r, 0, 1);
    (*r).issuffix = if issuffix { 1 } else { 0 };

    while *c != 0 {
        if state == RS_IN_WAIT {
            if t_isalpha_cstr(c) != 0 {
                if !ptr.is_null() {
                    ptr = newRegisNode(ptr, len);
                } else {
                    ptr = newRegisNode(null_mut(), len);
                    (*r).node = ptr;
                }
                (*ptr).r#type = RSF_ONEOF;
                (*ptr).len = ts_copychar_cstr(regis_node_data(ptr) as *mut c_void, c as *const c_void)
                    as uint32;
            } else if t_iseq(c, b'[' as c_char) {
                if !ptr.is_null() {
                    ptr = newRegisNode(ptr, len);
                } else {
                    ptr = newRegisNode(null_mut(), len);
                    (*r).node = ptr;
                }
                (*ptr).r#type = RSF_ONEOF;
                state = RS_IN_ONEOF;
            } else {
                /* shouldn't get here */
                elog!(ERROR, "invalid regis pattern: \"{}\"", cstr_to_string(str));
            }
        } else if state == RS_IN_ONEOF {
            if t_iseq(c, b'^' as c_char) {
                (*ptr).r#type = RSF_NONEOF;
                state = RS_IN_NONEOF;
            } else if t_isalpha_cstr(c) != 0 {
                (*ptr).len = ts_copychar_cstr(regis_node_data(ptr) as *mut c_void, c as *const c_void)
                    as uint32;
                state = RS_IN_ONEOF_IN;
            } else {
                /* shouldn't get here */
                elog!(ERROR, "invalid regis pattern: \"{}\"", cstr_to_string(str));
            }
        } else if state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF {
            if t_isalpha_cstr(c) != 0 {
                let dest = regis_node_data(ptr).add((*ptr).len as usize) as *mut c_void;
                (*ptr).len += ts_copychar_cstr(dest, c as *const c_void) as uint32;
            } else if t_iseq(c, b']' as c_char) {
                state = RS_IN_WAIT;
            } else {
                /* shouldn't get here */
                elog!(ERROR, "invalid regis pattern: \"{}\"", cstr_to_string(str));
            }
        } else {
            elog!(ERROR, "internal error in RS_compile: state {}", state);
        }
        c = c.add(pg_mblen_cstr(c) as usize);
    }

    if state != RS_IN_WAIT {
        /* shouldn't get here */
        elog!(ERROR, "invalid regis pattern: \"{}\"", cstr_to_string(str));
    }

    ptr = (*r).node;
    while !ptr.is_null() {
        (*r).nchar += 1;
        ptr = (*ptr).next;
    }
}

// ----------------------------------------------------------------------------
// RS_free - free the node list.
// ----------------------------------------------------------------------------

/// Free the compiled node list of `r`.
pub unsafe fn RS_free(r: *mut Regis) {
    let mut ptr = (*r).node;
    while !ptr.is_null() {
        let tmp = (*ptr).next;
        pfree(ptr as *mut c_void);
        ptr = tmp;
    }
    (*r).node = null_mut();
}

// ----------------------------------------------------------------------------
// mb_strchr (static) - does the (multibyte) char at `c` appear in `str`?
// ----------------------------------------------------------------------------

unsafe fn mb_strchr(str: *mut c_char, c: *mut c_char) -> bool {
    let clen = pg_mblen_cstr(c);
    let mut ptr = str;
    let mut res = false;

    while *ptr != 0 && !res {
        let plen = pg_mblen_cstr(ptr);
        if plen == clen {
            let mut i = plen;
            res = true;
            while i > 0 {
                i -= 1;
                if *ptr.add(i as usize) != *c.add(i as usize) {
                    res = false;
                    break;
                }
            }
        }
        ptr = ptr.add(plen as usize);
    }

    res
}

// ----------------------------------------------------------------------------
// RS_execute - match `str` against the compiled regis `r`.
// ----------------------------------------------------------------------------

/// Returns true if `str` matches the compiled regis `r`.
pub unsafe fn RS_execute(r: *mut Regis, str: *mut c_char) -> bool {
    let mut ptr = (*r).node;
    let mut c = str;
    let mut len: c_int = 0;

    while *c != 0 {
        len += 1;
        c = c.add(pg_mblen_cstr(c) as usize);
    }

    if len < (*r).nchar as c_int {
        return false;
    }

    c = str;
    if (*r).issuffix != 0 {
        let mut remaining = len - (*r).nchar as c_int;
        while remaining > 0 {
            remaining -= 1;
            c = c.add(pg_mblen_cstr(c) as usize);
        }
    }

    while !ptr.is_null() {
        match (*ptr).r#type {
            t if t == RSF_ONEOF => {
                if !mb_strchr(regis_node_data(ptr), c) {
                    return false;
                }
            }
            t if t == RSF_NONEOF => {
                if mb_strchr(regis_node_data(ptr), c) {
                    return false;
                }
            }
            other => {
                elog!(ERROR, "unrecognized regis node type: {}", other);
            }
        }
        ptr = (*ptr).next;
        c = c.add(pg_mblen_cstr(c) as usize);
    }

    true
}

// ----------------------------------------------------------------------------
// Helper: render a C string for elog! messages (ASCII-lossy is fine for errors).
// ----------------------------------------------------------------------------
unsafe fn cstr_to_string(s: *const c_char) -> String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    String::from_utf8_lossy(bytes).into_owned()
}

// ----------------------------------------------------------------------------
// Tests for the REAL compile/execute logic.
// ----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn cstr(s: &str) -> std::ffi::CString {
        std::ffi::CString::new(s).unwrap()
    }

    #[test]
    fn is_regis_accepts_simple_class() {
        let s = cstr("[abc]def");
        unsafe {
            assert!(RS_isRegis(s.as_ptr()));
        }
    }

    #[test]
    fn is_regis_accepts_negated_and_plain() {
        let neg = cstr("[^xyz]abc");
        let plain = cstr("abcd");
        unsafe {
            assert!(RS_isRegis(neg.as_ptr()));
            assert!(RS_isRegis(plain.as_ptr()));
        }
    }

    #[test]
    fn is_regis_rejects_unbalanced() {
        let bad = cstr("[abc");
        let bad2 = cstr("ab1c"); // digit is not alpha
        unsafe {
            assert!(!RS_isRegis(bad.as_ptr()));
            assert!(!RS_isRegis(bad2.as_ptr()));
        }
    }

    #[test]
    fn compile_counts_nodes() {
        // "[abc]de" -> 3 nodes: [abc], d, e
        let pat = cstr("[abc]de");
        unsafe {
            let mut r: Regis = core::mem::zeroed();
            RS_compile(&mut r, false, pat.as_ptr());
            assert_eq!(r.nchar, 3);
            // first node is the [abc] class with 3 bytes of data.
            let n0 = r.node;
            assert!(!n0.is_null());
            assert_eq!((*n0).r#type, RSF_ONEOF);
            assert_eq!((*n0).len, 3);
            RS_free(&mut r);
            assert!(r.node.is_null());
        }
    }

    #[test]
    fn execute_prefix_match() {
        // Pattern "[abc]d" matches a 2-char prefix where char1 in {a,b,c} and char2 == 'd'.
        let pat = cstr("[abc]d");
        unsafe {
            let mut r: Regis = core::mem::zeroed();
            RS_compile(&mut r, false, pat.as_ptr());

            let mut yes = cstr("ad").into_bytes_with_nul();
            let mut no = cstr("xd").into_bytes_with_nul();
            let mut short = cstr("a").into_bytes_with_nul();

            assert!(RS_execute(&mut r, yes.as_mut_ptr() as *mut c_char));
            assert!(!RS_execute(&mut r, no.as_mut_ptr() as *mut c_char));
            // too short to satisfy nchar
            assert!(!RS_execute(&mut r, short.as_mut_ptr() as *mut c_char));

            RS_free(&mut r);
        }
    }

    #[test]
    fn execute_negated_class() {
        // "[^abc]" matches a single char NOT in {a,b,c}.
        let pat = cstr("[^abc]");
        unsafe {
            let mut r: Regis = core::mem::zeroed();
            RS_compile(&mut r, false, pat.as_ptr());
            assert_eq!((*r.node).r#type, RSF_NONEOF);

            let mut yes = cstr("z").into_bytes_with_nul();
            let mut no = cstr("a").into_bytes_with_nul();
            assert!(RS_execute(&mut r, yes.as_mut_ptr() as *mut c_char));
            assert!(!RS_execute(&mut r, no.as_mut_ptr() as *mut c_char));

            RS_free(&mut r);
        }
    }

    #[test]
    fn execute_suffix_match() {
        // issuffix: match against the tail of the string.
        // Pattern "ing" (3 plain nodes) against "testing".
        let pat = cstr("ing");
        unsafe {
            let mut r: Regis = core::mem::zeroed();
            RS_compile(&mut r, true, pat.as_ptr());
            assert_eq!(r.issuffix, 1);
            assert_eq!(r.nchar, 3);

            let mut yes = cstr("testing").into_bytes_with_nul();
            let mut no = cstr("tested").into_bytes_with_nul();
            assert!(RS_execute(&mut r, yes.as_mut_ptr() as *mut c_char));
            assert!(!RS_execute(&mut r, no.as_mut_ptr() as *mut c_char));

            RS_free(&mut r);
        }
    }
}
