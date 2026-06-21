// regc_locale.c --
//
//	This file contains locale-specific regexp routines.
//	This file is #included by regcomp.c.
//
// Copyright (c) 1998 by Scriptics Corporation.
//
// This software is copyrighted by the Regents of the University of
// California, Sun Microsystems, Inc., Scriptics Corporation, ActiveState
// Corporation and other parties.  The following terms apply to all files
// associated with the software unless explicitly disclaimed in
// individual files.
//
// The authors hereby grant permission to use, copy, modify, distribute,
// and license this software and its documentation for any purpose, provided
// that existing copyright notices are retained in all copies and that this
// notice is included verbatim in any distributions. No written agreement,
// license, or royalty fee is required for any of the authorized uses.
// Modifications to this software may be copyrighted by their authors
// and need not follow the licensing terms described here, provided that
// the new terms are clearly indicated on the first page of each file where
// they apply.
//
// src/backend/regex/regc_locale.c

use std::os::raw::c_int;

use crate::mb::pg_wchar::pg_char_and_wchar_strncmp;
use crate::regex::regc_cvec::{addchr, addrange, getcvec};
use crate::regex::regcomp::{vars, EOS};
use crate::regex::regcustom::{chr, CHR, MAX_SIMPLE_CHR};
use crate::regex::regerror::{REG_ECOLLATE, REG_ECTYPE, REG_ERANGE, REG_ESPACE, REG_ETOOBIG};
use crate::regex::regex::{REG_FAKE, REG_ULOCALE};
use crate::regex::regguts::{
    char_classes, colormap, cvec, CC_ALNUM, CC_ALPHA, CC_ASCII, CC_BLANK, CC_CNTRL, CC_DIGIT,
    CC_GRAPH, CC_LOWER, CC_PRINT, CC_PUNCT, CC_SPACE, CC_UPPER, CC_WORD, CC_XDIGIT, NUM_CCLASSES,
};

// ---------------------------------------------------------------------------
// parsing macros copied from regcomp.c, with `v' the struct vars pointer
// ---------------------------------------------------------------------------

// #define ERR(e) VERR(v, e)  /* record an error */
macro_rules! ERR {
    ($v:expr, $e:expr) => {{
        (*$v).nexttype = EOS;
        (*$v).err = if (*$v).err != 0 { (*$v).err } else { ($e) };
    }};
}

// #define ISERR() VISERR(v)
macro_rules! ISERR {
    ($v:expr) => {
        (*$v).err != 0
    };
}

// #define NOERRN() {if (ISERR()) return NULL;}
macro_rules! NOERRN {
    ($v:expr) => {
        if ISERR!($v) {
            return std::ptr::null_mut();
        }
    };
}

// #define NOTE(b) (v->re->re_info |= (b))
macro_rules! NOTE {
    ($v:expr, $b:expr) => {
        (*(*$v).re).re_info |= ($b)
    };
}

// #define INTERRUPT(re) CHECK_FOR_INTERRUPTS()
macro_rules! INTERRUPT {
    ($re:expr) => {{
        let _ = $re;
        crate::miscadmin::CHECK_FOR_INTERRUPTS();
    }};
}

// ASCII character-name table
struct cname {
    name: &'static str,
    code: chr,
}

static CNAMES: &[cname] = &[
    cname { name: "NUL", code: 0o0 },
    cname { name: "SOH", code: 0o1 },
    cname { name: "STX", code: 0o2 },
    cname { name: "ETX", code: 0o3 },
    cname { name: "EOT", code: 0o4 },
    cname { name: "ENQ", code: 0o5 },
    cname { name: "ACK", code: 0o6 },
    cname { name: "BEL", code: 0o7 },
    cname { name: "alert", code: 0o7 },
    cname { name: "BS", code: 0o10 },
    cname { name: "backspace", code: 0x08 },
    cname { name: "HT", code: 0o11 },
    cname { name: "tab", code: b'\t' as chr },
    cname { name: "LF", code: 0o12 },
    cname { name: "newline", code: b'\n' as chr },
    cname { name: "VT", code: 0o13 },
    cname { name: "vertical-tab", code: 0x0b },
    cname { name: "FF", code: 0o14 },
    cname { name: "form-feed", code: 0x0c },
    cname { name: "CR", code: 0o15 },
    cname { name: "carriage-return", code: b'\r' as chr },
    cname { name: "SO", code: 0o16 },
    cname { name: "SI", code: 0o17 },
    cname { name: "DLE", code: 0o20 },
    cname { name: "DC1", code: 0o21 },
    cname { name: "DC2", code: 0o22 },
    cname { name: "DC3", code: 0o23 },
    cname { name: "DC4", code: 0o24 },
    cname { name: "NAK", code: 0o25 },
    cname { name: "SYN", code: 0o26 },
    cname { name: "ETB", code: 0o27 },
    cname { name: "CAN", code: 0o30 },
    cname { name: "EM", code: 0o31 },
    cname { name: "SUB", code: 0o32 },
    cname { name: "ESC", code: 0o33 },
    cname { name: "IS4", code: 0o34 },
    cname { name: "FS", code: 0o34 },
    cname { name: "IS3", code: 0o35 },
    cname { name: "GS", code: 0o35 },
    cname { name: "IS2", code: 0o36 },
    cname { name: "RS", code: 0o36 },
    cname { name: "IS1", code: 0o37 },
    cname { name: "US", code: 0o37 },
    cname { name: "space", code: b' ' as chr },
    cname { name: "exclamation-mark", code: b'!' as chr },
    cname { name: "quotation-mark", code: b'"' as chr },
    cname { name: "number-sign", code: b'#' as chr },
    cname { name: "dollar-sign", code: b'$' as chr },
    cname { name: "percent-sign", code: b'%' as chr },
    cname { name: "ampersand", code: b'&' as chr },
    cname { name: "apostrophe", code: b'\'' as chr },
    cname { name: "left-parenthesis", code: b'(' as chr },
    cname { name: "right-parenthesis", code: b')' as chr },
    cname { name: "asterisk", code: b'*' as chr },
    cname { name: "plus-sign", code: b'+' as chr },
    cname { name: "comma", code: b',' as chr },
    cname { name: "hyphen", code: b'-' as chr },
    cname { name: "hyphen-minus", code: b'-' as chr },
    cname { name: "period", code: b'.' as chr },
    cname { name: "full-stop", code: b'.' as chr },
    cname { name: "slash", code: b'/' as chr },
    cname { name: "solidus", code: b'/' as chr },
    cname { name: "zero", code: b'0' as chr },
    cname { name: "one", code: b'1' as chr },
    cname { name: "two", code: b'2' as chr },
    cname { name: "three", code: b'3' as chr },
    cname { name: "four", code: b'4' as chr },
    cname { name: "five", code: b'5' as chr },
    cname { name: "six", code: b'6' as chr },
    cname { name: "seven", code: b'7' as chr },
    cname { name: "eight", code: b'8' as chr },
    cname { name: "nine", code: b'9' as chr },
    cname { name: "colon", code: b':' as chr },
    cname { name: "semicolon", code: b';' as chr },
    cname { name: "less-than-sign", code: b'<' as chr },
    cname { name: "equals-sign", code: b'=' as chr },
    cname { name: "greater-than-sign", code: b'>' as chr },
    cname { name: "question-mark", code: b'?' as chr },
    cname { name: "commercial-at", code: b'@' as chr },
    cname { name: "left-square-bracket", code: b'[' as chr },
    cname { name: "backslash", code: b'\\' as chr },
    cname { name: "reverse-solidus", code: b'\\' as chr },
    cname { name: "right-square-bracket", code: b']' as chr },
    cname { name: "circumflex", code: b'^' as chr },
    cname { name: "circumflex-accent", code: b'^' as chr },
    cname { name: "underscore", code: b'_' as chr },
    cname { name: "low-line", code: b'_' as chr },
    cname { name: "grave-accent", code: b'`' as chr },
    cname { name: "left-brace", code: b'{' as chr },
    cname { name: "left-curly-bracket", code: b'{' as chr },
    cname { name: "vertical-line", code: b'|' as chr },
    cname { name: "right-brace", code: b'}' as chr },
    cname { name: "right-curly-bracket", code: b'}' as chr },
    cname { name: "tilde", code: b'~' as chr },
    cname { name: "DEL", code: 0o177 },
];

// The following array defines the valid character class names.
// The entries must match enum char_classes in regguts.h.
static CLASS_NAMES: [Option<&'static str>; NUM_CCLASSES + 1] = [
    Some("alnum"),
    Some("alpha"),
    Some("ascii"),
    Some("blank"),
    Some("cntrl"),
    Some("digit"),
    Some("graph"),
    Some("lower"),
    Some("print"),
    Some("punct"),
    Some("space"),
    Some("upper"),
    Some("xdigit"),
    Some("word"),
    None,
];

// We do not use the hard-wired Unicode classification tables that Tcl does.
// This is because (a) we need to deal with other encodings besides Unicode,
// and (b) we want to track the behavior of the libc locale routines as
// closely as possible.  For example, it wouldn't be unreasonable for a
// locale to not consider every Unicode letter as a letter.  So we build
// character classification cvecs by asking libc, even for Unicode.

// element - map collating-element name to chr
pub unsafe fn element(
    v: *mut vars,           // context
    startp: *const chr,     // points to start of name
    endp: *const chr,       // points just past end of name
) -> chr {
    let len: usize;

    // generic:  one-chr names stand for themselves
    assert!(startp < endp);
    len = endp.offset_from(startp) as usize;
    if len == 1 {
        return *startp;
    }

    NOTE!(v, REG_ULOCALE);

    // search table
    for cn in CNAMES.iter() {
        if cn.name.len() == len
            && pg_char_and_wchar_strncmp(
                cn.name.as_ptr() as *const std::os::raw::c_char,
                startp,
                len,
            ) == 0
        {
            // NOTE BREAK OUT
            return CHR(cn.code as c_int);
        }
    }

    // couldn't find it
    ERR!(v, REG_ECOLLATE);
    0
}

// range - supply cvec for a range, including legality check
pub unsafe fn range(
    v: *mut vars, // context
    a: chr,       // range start
    b: chr,       // range end, might equal a
    cases: c_int, // case-independent?
) -> *mut cvec {
    let mut nchrs: c_int;
    let cv: *mut cvec;
    let mut c: chr;
    let mut cc: chr;

    if a != b && !(before(a, b) != 0) {
        ERR!(v, REG_ERANGE);
        return std::ptr::null_mut();
    }

    if cases == 0 {
        // easy version
        let cv = getcvec(v, 0, 1);
        NOERRN!(v);
        addrange(cv, a, b);
        return cv;
    }

    // When case-independent, it's hard to decide when cvec ranges are usable,
    // so for now at least, we won't try.  We use a range for the originally
    // specified chrs and then add on any case-equivalents that are outside
    // that range as individual chrs.
    //
    // To ensure sane behavior if someone specifies a very large range, limit
    // the allocation size to 100000 chrs (arbitrary) and check for overrun
    // inside the loop below.
    nchrs = (b - a + 1) as c_int;
    if nchrs <= 0 || nchrs > 100000 {
        nchrs = 100000;
    }

    cv = getcvec(v, nchrs, 1);
    NOERRN!(v);
    addrange(cv, a, b);

    c = a;
    while c <= b {
        cc = pg_wc_tolower(c);
        if cc != c && (before(cc, a) != 0 || before(b, cc) != 0) {
            if (*cv).nchrs >= (*cv).chrspace {
                ERR!(v, REG_ETOOBIG);
                return std::ptr::null_mut();
            }
            addchr(cv, cc);
        }
        cc = pg_wc_toupper(c);
        if cc != c && (before(cc, a) != 0 || before(b, cc) != 0) {
            if (*cv).nchrs >= (*cv).chrspace {
                ERR!(v, REG_ETOOBIG);
                return std::ptr::null_mut();
            }
            addchr(cv, cc);
        }
        INTERRUPT!((*v).re);
        c += 1;
    }

    cv
}

// before - is chr x before chr y, for purposes of range legality?
unsafe fn before(x: chr, y: chr) -> c_int {
    // predicate
    if x < y {
        return 1;
    }
    0
}

// eclass - supply cvec for an equivalence class
// Must include case counterparts on request.
pub unsafe fn eclass(
    v: *mut vars, // context
    c: chr,       // Collating element representing the equivalence class.
    cases: c_int, // all cases?
) -> *mut cvec {
    let cv: *mut cvec;

    // crude fake equivalence class for testing
    if ((*v).cflags & REG_FAKE) != 0 && c == 'x' as chr {
        cv = getcvec(v, 4, 0);
        addchr(cv, CHR('x' as c_int));
        addchr(cv, CHR('y' as c_int));
        if cases != 0 {
            addchr(cv, CHR('X' as c_int));
            addchr(cv, CHR('Y' as c_int));
        }
        return cv;
    }

    // otherwise, none
    if cases != 0 {
        return allcases(v, c);
    }
    cv = getcvec(v, 1, 0);
    assert!(!cv.is_null());
    addchr(cv, c);
    cv
}

// lookupcclass - lookup a character class identified by name
//
// On failure, sets an error code in *v; the result is then garbage.
pub unsafe fn lookupcclass(
    v: *mut vars,       // context (for returning errors)
    startp: *const chr, // where the name starts
    endp: *const chr,   // just past the end of the name
) -> char_classes {
    let len: usize;

    // Map the name to the corresponding enumerated value.
    len = endp.offset_from(startp) as usize;
    let mut i: c_int = 0;
    for namePtr in CLASS_NAMES.iter() {
        match namePtr {
            None => break,
            Some(name) => {
                if name.len() == len
                    && pg_char_and_wchar_strncmp(
                        name.as_ptr() as *const std::os::raw::c_char,
                        startp,
                        len,
                    ) == 0
                {
                    return i as char_classes;
                }
            }
        }
        i += 1;
    }

    ERR!(v, REG_ECTYPE);
    0 as char_classes
}

// cclasscvec - supply cvec for a character class
//
// Must include case counterparts if "cases" is true.
//
// The returned cvec might be either a transient cvec gotten from getcvec(),
// or a permanently cached one from pg_ctype_get_cache().  This is okay
// because callers are not supposed to explicitly free the result either way.
pub unsafe fn cclasscvec(
    v: *mut vars,                  // context
    cclasscode: char_classes,      // class to build a cvec for
    cases: c_int,                  // case-independent?
) -> *mut cvec {
    let mut cv: *mut cvec = std::ptr::null_mut();
    let mut cclasscode = cclasscode;

    // Remap lower and upper to alpha if the match is case insensitive.
    if cases != 0 && (cclasscode == CC_LOWER || cclasscode == CC_UPPER) {
        cclasscode = CC_ALPHA;
    }

    // Now compute the character class contents.  For classes that are based
    // on the behavior of a <wctype.h> or <ctype.h> function, we use
    // pg_ctype_get_cache so that we can cache the results.  Other classes
    // have definitions that are hard-wired here, and for those we just
    // construct a transient cvec on the fly.
    //
    // NB: keep this code in sync with cclass_column_index(), below.
    match cclasscode {
        x if x == CC_PRINT => {
            cv = pg_ctype_get_cache(pg_wc_isprint, cclasscode);
        }
        x if x == CC_ALNUM => {
            cv = pg_ctype_get_cache(pg_wc_isalnum, cclasscode);
        }
        x if x == CC_ALPHA => {
            cv = pg_ctype_get_cache(pg_wc_isalpha, cclasscode);
        }
        x if x == CC_WORD => {
            cv = pg_ctype_get_cache(pg_wc_isword, cclasscode);
        }
        x if x == CC_ASCII => {
            // hard-wired meaning
            cv = getcvec(v, 0, 1);
            if !cv.is_null() {
                addrange(cv, 0, 0x7f);
            }
        }
        x if x == CC_BLANK => {
            // hard-wired meaning
            cv = getcvec(v, 2, 0);
            addchr(cv, b'\t' as chr);
            addchr(cv, b' ' as chr);
        }
        x if x == CC_CNTRL => {
            // hard-wired meaning
            cv = getcvec(v, 0, 2);
            addrange(cv, 0x0, 0x1f);
            addrange(cv, 0x7f, 0x9f);
        }
        x if x == CC_DIGIT => {
            cv = pg_ctype_get_cache(pg_wc_isdigit, cclasscode);
        }
        x if x == CC_PUNCT => {
            cv = pg_ctype_get_cache(pg_wc_ispunct, cclasscode);
        }
        x if x == CC_XDIGIT => {
            // It's not clear how to define this in non-western locales, and
            // even less clear that there's any particular use in trying. So
            // just hard-wire the meaning.
            cv = getcvec(v, 0, 3);
            if !cv.is_null() {
                addrange(cv, b'0' as chr, b'9' as chr);
                addrange(cv, b'a' as chr, b'f' as chr);
                addrange(cv, b'A' as chr, b'F' as chr);
            }
        }
        x if x == CC_SPACE => {
            cv = pg_ctype_get_cache(pg_wc_isspace, cclasscode);
        }
        x if x == CC_LOWER => {
            cv = pg_ctype_get_cache(pg_wc_islower, cclasscode);
        }
        x if x == CC_UPPER => {
            cv = pg_ctype_get_cache(pg_wc_isupper, cclasscode);
        }
        x if x == CC_GRAPH => {
            cv = pg_ctype_get_cache(pg_wc_isgraph, cclasscode);
        }
        _ => {}
    }

    // If cv is NULL now, the reason must be "out of memory"
    if cv.is_null() {
        ERR!(v, REG_ESPACE);
    }
    cv
}

// cclass_column_index - get appropriate high colormap column index for chr
pub unsafe fn cclass_column_index(cm: *mut colormap, c: chr) -> c_int {
    let mut colnum: c_int = 0;

    // Shouldn't go through all these pushups for simple chrs
    assert!(c > MAX_SIMPLE_CHR);

    // Note: we should not see requests to consider cclasses that are not
    // treated as locale-specific by cclasscvec(), above.
    if (*cm).classbits[CC_PRINT as usize] != 0 && pg_wc_isprint(c) != 0 {
        colnum |= (*cm).classbits[CC_PRINT as usize];
    }
    if (*cm).classbits[CC_ALNUM as usize] != 0 && pg_wc_isalnum(c) != 0 {
        colnum |= (*cm).classbits[CC_ALNUM as usize];
    }
    if (*cm).classbits[CC_ALPHA as usize] != 0 && pg_wc_isalpha(c) != 0 {
        colnum |= (*cm).classbits[CC_ALPHA as usize];
    }
    if (*cm).classbits[CC_WORD as usize] != 0 && pg_wc_isword(c) != 0 {
        colnum |= (*cm).classbits[CC_WORD as usize];
    }
    assert!((*cm).classbits[CC_ASCII as usize] == 0);
    assert!((*cm).classbits[CC_BLANK as usize] == 0);
    assert!((*cm).classbits[CC_CNTRL as usize] == 0);
    if (*cm).classbits[CC_DIGIT as usize] != 0 && pg_wc_isdigit(c) != 0 {
        colnum |= (*cm).classbits[CC_DIGIT as usize];
    }
    if (*cm).classbits[CC_PUNCT as usize] != 0 && pg_wc_ispunct(c) != 0 {
        colnum |= (*cm).classbits[CC_PUNCT as usize];
    }
    assert!((*cm).classbits[CC_XDIGIT as usize] == 0);
    if (*cm).classbits[CC_SPACE as usize] != 0 && pg_wc_isspace(c) != 0 {
        colnum |= (*cm).classbits[CC_SPACE as usize];
    }
    if (*cm).classbits[CC_LOWER as usize] != 0 && pg_wc_islower(c) != 0 {
        colnum |= (*cm).classbits[CC_LOWER as usize];
    }
    if (*cm).classbits[CC_UPPER as usize] != 0 && pg_wc_isupper(c) != 0 {
        colnum |= (*cm).classbits[CC_UPPER as usize];
    }
    if (*cm).classbits[CC_GRAPH as usize] != 0 && pg_wc_isgraph(c) != 0 {
        colnum |= (*cm).classbits[CC_GRAPH as usize];
    }

    colnum
}

// allcases - supply cvec for all case counterparts of a chr (including itself)
//
// This is a shortcut, preferably an efficient one, for simple characters;
// messy cases are done via range().
pub unsafe fn allcases(
    v: *mut vars, // context
    c: chr,       // character to get case equivs of
) -> *mut cvec {
    let cv: *mut cvec;
    let lc: chr;
    let uc: chr;

    lc = pg_wc_tolower(c);
    uc = pg_wc_toupper(c);

    cv = getcvec(v, 2, 0);
    addchr(cv, lc);
    if lc != uc {
        addchr(cv, uc);
    }
    cv
}

// cmp - chr-substring compare
//
// Backrefs need this.  It should preferably be efficient.
// Note that it does not need to report anything except equal/unequal.
// Note also that the length is exact, and the comparison should not
// stop at embedded NULs!
pub unsafe extern "C" fn cmp(
    x: *const chr, // strings to compare
    y: *const chr,
    len: usize, // exact length of comparison
) -> c_int {
    // 0 for equal, nonzero for unequal
    libc::memcmp(
        x as *const std::ffi::c_void,
        y as *const std::ffi::c_void,
        len * std::mem::size_of::<chr>(),
    )
}

// casecmp - case-independent chr-substring compare
//
// REG_ICASE backrefs need this.  It should preferably be efficient.
// Note that it does not need to report anything except equal/unequal.
// Note also that the length is exact, and the comparison should not
// stop at embedded NULs!
pub unsafe extern "C" fn casecmp(
    x: *const chr, // strings to compare
    y: *const chr,
    len: usize, // exact length of comparison
) -> c_int {
    // 0 for equal, nonzero for unequal
    let mut x = x;
    let mut y = y;
    let mut len = len;
    while len > 0 {
        if *x != *y && pg_wc_tolower(*x) != pg_wc_tolower(*y) {
            return 1;
        }
        len -= 1;
        x = x.add(1);
        y = y.add(1);
    }
    0
}

// ---------------------------------------------------------------------------
// TODO(pg-port): dependencies provided by regc_pg_locale.c
// ---------------------------------------------------------------------------

unsafe fn pg_wc_tolower(_c: chr) -> chr {
    // TODO(pg-port): implement in regc_pg_locale.rs
    _c
}

unsafe fn pg_wc_toupper(_c: chr) -> chr {
    // TODO(pg-port): implement in regc_pg_locale.rs
    _c
}

unsafe fn pg_wc_isprint(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isalnum(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isalpha(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isword(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isdigit(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_ispunct(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isspace(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_islower(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isupper(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_wc_isgraph(_c: chr) -> c_int {
    // TODO(pg-port): implement in regc_pg_locale.rs
    0
}

unsafe fn pg_ctype_get_cache(
    _probefunc: unsafe fn(chr) -> c_int,
    _cclasscode: char_classes,
) -> *mut cvec {
    // TODO(pg-port): implement in regc_pg_locale.rs
    std::ptr::null_mut()
}
