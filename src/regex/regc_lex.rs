//! regex/regc_lex.c - lexical analyzer.
//!
//! This file is #included by regcomp.c. Copyright (c) 1998, 1999 Henry Spencer.
//! See PostgreSQL source for the full license text. Implements the lexer that
//! turns the regex source string into tokens for the parser in regcomp.c.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use core::ffi::c_int;
use core::ffi::c_void;

use crate::regex::regcomp::{
    BACKREF, CCLASS, CCLASSC, CCLASSS, COLLEL, DIGIT, ECLASS, EMPTY, END, EOS, LACON, NWBDRY,
    PLAIN, RANGE, SBEGIN, SEND, WBDRY,
};
use crate::regex::regcustom::{
    chr, iscalnum, iscalpha, iscdigit, iscspace, uchr, CHR, CHR_IS_IN_RANGE, DIGITVAL,
};
use crate::regex::regerror::{
    REG_BADBR, REG_BADOPT, REG_BADPAT, REG_BADRPT, REG_EBRACE, REG_EBRACK, REG_EESCAPE,
};
use crate::regex::regguts::{
    color, colormap, cvec, nfa, state, subre, CC_DIGIT, CC_SPACE, CC_WORD, LATYPE_AHEAD_NEG,
    LATYPE_AHEAD_POS, LATYPE_BEHIND_NEG, LATYPE_BEHIND_POS, NOTREACHED,
};
use crate::regex::regex::{
    regex_t, REG_ADVANCED, REG_ADVF, REG_BOSONLY, REG_EXPANDED, REG_EXTENDED, REG_ICASE,
    REG_NEWLINE, REG_NLANCH, REG_NLSTOP, REG_QUOTE, REG_UBACKREF, REG_UBBS, REG_UBOUNDS,
    REG_UBRACES, REG_UBSALNUM, REG_ULOCALE, REG_ULOOKAROUND, REG_UNONPOSIX, REG_UUNPORT,
    REG_UUNSPEC,
};

// ---------------------------------------------------------------------------
// struct vars (defined in regcomp.c, not a header). regc_lex.c reaches into it
// through v for the scan pointers, flags, and token state. We materialize a
// faithful layout here; the consuming code casts. This mirrors the local copy
// kept in regc_nfa.rs.
// TODO(pg-port): unify with regcomp.c's struct vars once #include relationship
// is resolved in the Rust tree.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct vars {
    pub re: *mut regex_t,
    pub now: *const chr,
    pub stop: *const chr,
    pub err: c_int,
    pub cflags: c_int,
    pub lasttype: c_int,
    pub nexttype: c_int,
    pub nextvalue: chr,
    pub lexcon: c_int,
    pub nsubexp: c_int,
    pub subs: *mut *mut subre,
    pub nsubs: usize,
    pub sub10: [*mut subre; 10],
    pub nfa: *mut nfa,
    pub cm: *mut colormap,
    pub nlcolor: color,
    pub wordchrs: *mut state,
    pub tree: *mut subre,
    pub treechain: *mut subre,
    pub treefree: *mut subre,
    pub ntree: c_int,
    pub cv: *mut c_void,
    pub cv2: *mut c_void,
    pub lacons: *mut subre,
    pub nlacons: c_int,
    pub spaceused: usize,
}

// ---------------------------------------------------------------------------
// scanning macros (know about v)
// ---------------------------------------------------------------------------

/// #define ATEOS() (v->now >= v->stop)
macro_rules! ATEOS {
    ($v:expr) => {
        (*$v).now >= (*$v).stop
    };
}

/// #define HAVE(n) (v->stop - v->now >= (n))
macro_rules! HAVE {
    ($v:expr, $n:expr) => {
        (*$v).stop.offset_from((*$v).now) >= ($n) as isize
    };
}

/// #define NEXT1(c) (!ATEOS() && *v->now == CHR(c))
macro_rules! NEXT1 {
    ($v:expr, $c:expr) => {
        !ATEOS!($v) && *(*$v).now == CHR($c as c_int)
    };
}

/// #define NEXT2(a,b) (HAVE(2) && *v->now == CHR(a) && *(v->now+1) == CHR(b))
macro_rules! NEXT2 {
    ($v:expr, $a:expr, $b:expr) => {
        HAVE!($v, 2)
            && *(*$v).now == CHR($a as c_int)
            && *(*$v).now.add(1) == CHR($b as c_int)
    };
}

/// #define NEXT3(a,b,c) (HAVE(3) && *v->now == CHR(a) && *(v->now+1) == CHR(b) && *(v->now+2) == CHR(c))
macro_rules! NEXT3 {
    ($v:expr, $a:expr, $b:expr, $c:expr) => {
        HAVE!($v, 3)
            && *(*$v).now == CHR($a as c_int)
            && *(*$v).now.add(1) == CHR($b as c_int)
            && *(*$v).now.add(2) == CHR($c as c_int)
    };
}

/// #define SET(c) (v->nexttype = (c))
macro_rules! SET {
    ($v:expr, $c:expr) => {
        (*$v).nexttype = ($c)
    };
}

/// #define SETV(c, n) (v->nexttype = (c), v->nextvalue = (n))
macro_rules! SETV {
    ($v:expr, $c:expr, $n:expr) => {{
        (*$v).nexttype = ($c);
        (*$v).nextvalue = ($n) as chr;
    }};
}

/// #define RET(c) return (SET(c), 1)
macro_rules! RET {
    ($v:expr, $c:expr) => {{
        SET!($v, $c);
        return 1;
    }};
}

/// #define RETV(c, n) return (SETV(c, n), 1)
macro_rules! RETV {
    ($v:expr, $c:expr, $n:expr) => {{
        SETV!($v, $c, $n);
        return 1;
    }};
}

/// #define FAILW(e) return (ERR(e), 0) -- ERR does SET(EOS)
macro_rules! FAILW {
    ($v:expr, $e:expr) => {{
        ERR!($v, $e);
        return 0;
    }};
}

/// #define LASTTYPE(t) (v->lasttype == (t))
macro_rules! LASTTYPE {
    ($v:expr, $t:expr) => {
        (*$v).lasttype == ($t)
    };
}

// ---------------------------------------------------------------------------
// parsing macros borrowed from regcomp.c (where regc_lex.c is #included).
// ---------------------------------------------------------------------------

/// #define VISERR(vv) ((vv)->err != 0)
macro_rules! ISERR {
    ($v:expr) => {
        (*$v).err != 0
    };
}

/// #define VERR(vv,e) ((vv)->nexttype = EOS, (vv)->err = ((vv)->err ? (vv)->err : (e)))
macro_rules! ERR {
    ($v:expr, $e:expr) => {{
        (*$v).nexttype = EOS;
        (*$v).err = if (*$v).err != 0 { (*$v).err } else { ($e) };
    }};
}

/// #define NOERR() {if (ISERR()) return;}
macro_rules! NOERR {
    ($v:expr) => {
        if ISERR!($v) {
            return;
        }
    };
}

/// #define NOTE(b) (v->re->re_info |= (b))
macro_rules! NOTE {
    ($v:expr, $b:expr) => {
        (*(*$v).re).re_info |= ($b)
    };
}

// ---------------------------------------------------------------------------
// lexical contexts
// ---------------------------------------------------------------------------

const L_ERE: c_int = 1; // mainline ERE/ARE
const L_BRE: c_int = 2; // mainline BRE
const L_Q: c_int = 3; // REG_QUOTE
const L_EBND: c_int = 4; // ERE/ARE bound
const L_BBND: c_int = 5; // BRE bound
const L_BRACK: c_int = 6; // brackets
const L_CEL: c_int = 7; // collating element
const L_ECL: c_int = 8; // equivalence class
const L_CCL: c_int = 9; // character class

/// #define INTOCON(c) (v->lexcon = (c))
macro_rules! INTOCON {
    ($v:expr, $c:expr) => {
        (*$v).lexcon = ($c)
    };
}

/// #define INCON(con) (v->lexcon == (con))
macro_rules! INCON {
    ($v:expr, $con:expr) => {
        (*$v).lexcon == ($con)
    };
}

// #define ENDOF(array) ((array) + sizeof(array)/sizeof(chr))
// In Rust this is expressed at the call site as alert.as_ptr().add(alert.len()).

/*
 * lexstart - set up lexical stuff, scan leading options
 */
pub unsafe fn lexstart(v: *mut vars) {
    prefixes(v); // may turn on new type bits etc.
    NOERR!(v);

    if (*v).cflags & REG_QUOTE != 0 {
        Assert!((*v).cflags & (REG_ADVANCED | REG_EXPANDED | REG_NEWLINE) == 0);
        INTOCON!(v, L_Q);
    } else if (*v).cflags & REG_EXTENDED != 0 {
        Assert!((*v).cflags & REG_QUOTE == 0);
        INTOCON!(v, L_ERE);
    } else {
        Assert!((*v).cflags & (REG_QUOTE | REG_ADVF) == 0);
        INTOCON!(v, L_BRE);
    }

    (*v).nexttype = EMPTY; // remember we were at the start
    next(v); // set up the first token
}

/*
 * prefixes - implement various special prefixes
 */
pub unsafe fn prefixes(v: *mut vars) {
    /* literal string doesn't get any of this stuff */
    if (*v).cflags & REG_QUOTE != 0 {
        return;
    }

    /* initial "***" gets special things */
    if HAVE!(v, 4) && NEXT3!(v, '*', '*', '*') {
        match *(*v).now.add(3) {
            // "***?" error, msg shows version
            x if x == CHR('?' as c_int) => {
                ERR!(v, REG_BADPAT);
                return; // proceed no further
            }
            // "***=" shifts to literal string
            x if x == CHR('=' as c_int) => {
                NOTE!(v, REG_UNONPOSIX);
                (*v).cflags |= REG_QUOTE;
                (*v).cflags &= !(REG_ADVANCED | REG_EXPANDED | REG_NEWLINE);
                (*v).now = (*v).now.add(4);
                return; // and there can be no more prefixes
            }
            // "***:" shifts to AREs
            x if x == CHR(':' as c_int) => {
                NOTE!(v, REG_UNONPOSIX);
                (*v).cflags |= REG_ADVANCED;
                (*v).now = (*v).now.add(4);
            }
            // otherwise *** is just an error
            _ => {
                ERR!(v, REG_BADRPT);
                return;
            }
        }
    }

    /* BREs and EREs don't get embedded options */
    if (*v).cflags & REG_ADVANCED != REG_ADVANCED {
        return;
    }

    /* embedded options (AREs only) */
    if HAVE!(v, 3) && NEXT2!(v, '(', '?') && iscalpha(*(*v).now.add(2)) {
        NOTE!(v, REG_UNONPOSIX);
        (*v).now = (*v).now.add(2);
        while !ATEOS!(v) && iscalpha(*(*v).now) {
            match *(*v).now {
                // BREs (but why???)
                x if x == CHR('b' as c_int) => {
                    (*v).cflags &= !(REG_ADVANCED | REG_QUOTE);
                }
                // case sensitive
                x if x == CHR('c' as c_int) => {
                    (*v).cflags &= !REG_ICASE;
                }
                // plain EREs
                x if x == CHR('e' as c_int) => {
                    (*v).cflags |= REG_EXTENDED;
                    (*v).cflags &= !(REG_ADVF | REG_QUOTE);
                }
                // case insensitive
                x if x == CHR('i' as c_int) => {
                    (*v).cflags |= REG_ICASE;
                }
                // 'm': Perloid synonym for n; 'n': \n affects ^ $ . [^
                x if x == CHR('m' as c_int) || x == CHR('n' as c_int) => {
                    (*v).cflags |= REG_NEWLINE;
                }
                // ~Perl, \n affects . [^
                x if x == CHR('p' as c_int) => {
                    (*v).cflags |= REG_NLSTOP;
                    (*v).cflags &= !REG_NLANCH;
                }
                // literal string
                x if x == CHR('q' as c_int) => {
                    (*v).cflags |= REG_QUOTE;
                    (*v).cflags &= !REG_ADVANCED;
                }
                // single line, \n ordinary
                x if x == CHR('s' as c_int) => {
                    (*v).cflags &= !REG_NEWLINE;
                }
                // tight syntax
                x if x == CHR('t' as c_int) => {
                    (*v).cflags &= !REG_EXPANDED;
                }
                // weird, \n affects ^ $ only
                x if x == CHR('w' as c_int) => {
                    (*v).cflags &= !REG_NLSTOP;
                    (*v).cflags |= REG_NLANCH;
                }
                // expanded syntax
                x if x == CHR('x' as c_int) => {
                    (*v).cflags |= REG_EXPANDED;
                }
                _ => {
                    ERR!(v, REG_BADOPT);
                    return;
                }
            }
            (*v).now = (*v).now.add(1);
        }
        if !NEXT1!(v, ')') {
            ERR!(v, REG_BADOPT);
            return;
        }
        (*v).now = (*v).now.add(1);
        if (*v).cflags & REG_QUOTE != 0 {
            (*v).cflags &= !(REG_EXPANDED | REG_NEWLINE);
        }
    }
}

/*
 * next - get next token
 *
 * Returns 1 normal, 0 failure.
 */
pub unsafe fn next(v: *mut vars) -> c_int {
    let c: chr;

    'next_restart: loop {
        // loop here after eating a comment

        /* errors yield an infinite sequence of failures */
        if ISERR!(v) {
            return 0; // the error has set nexttype to EOS
        }

        /* remember flavor of last token */
        (*v).lasttype = (*v).nexttype;

        /* REG_BOSONLY */
        if (*v).nexttype == EMPTY && (*v).cflags & REG_BOSONLY != 0 {
            /* at start of a REG_BOSONLY RE */
            RETV!(v, SBEGIN, 0); // same as \A
        }

        /* skip white space etc. if appropriate (not in literal or []) */
        if (*v).cflags & REG_EXPANDED != 0 {
            match (*v).lexcon {
                L_ERE | L_BRE | L_EBND | L_BBND => {
                    skip(v);
                }
                _ => {}
            }
        }

        /* handle EOS, depending on context */
        if ATEOS!(v) {
            match (*v).lexcon {
                L_ERE | L_BRE | L_Q => {
                    RET!(v, EOS);
                }
                L_EBND | L_BBND => {
                    FAILW!(v, REG_EBRACE);
                }
                L_BRACK | L_CEL | L_ECL | L_CCL => {
                    FAILW!(v, REG_EBRACK);
                }
                _ => {}
            }
            Assert!(NOTREACHED != 0);
        }

        /* okay, time to actually get a character */
        c = *(*v).now;
        (*v).now = (*v).now.add(1);

        /* deal with the easy contexts, punt EREs to code below */
        match (*v).lexcon {
            // punt BREs to separate function
            L_BRE => {
                return brenext(v, c);
            }
            // see below
            L_ERE => {}
            // literal strings are easy
            L_Q => {
                RETV!(v, PLAIN, c);
            }
            // bounds are fairly simple
            L_BBND | L_EBND => {
                match c {
                    x if x == CHR('0' as c_int)
                        || x == CHR('1' as c_int)
                        || x == CHR('2' as c_int)
                        || x == CHR('3' as c_int)
                        || x == CHR('4' as c_int)
                        || x == CHR('5' as c_int)
                        || x == CHR('6' as c_int)
                        || x == CHR('7' as c_int)
                        || x == CHR('8' as c_int)
                        || x == CHR('9' as c_int) =>
                    {
                        RETV!(v, DIGIT, DIGITVAL(c));
                    }
                    x if x == CHR(',' as c_int) => {
                        RET!(v, ',' as c_int);
                    }
                    // ERE bound ends with }
                    x if x == CHR('}' as c_int) => {
                        if INCON!(v, L_EBND) {
                            INTOCON!(v, L_ERE);
                            if (*v).cflags & REG_ADVF != 0 && NEXT1!(v, '?') {
                                (*v).now = (*v).now.add(1);
                                NOTE!(v, REG_UNONPOSIX);
                                RETV!(v, '}' as c_int, 0);
                            }
                            RETV!(v, '}' as c_int, 1);
                        } else {
                            FAILW!(v, REG_BADBR);
                        }
                    }
                    // BRE bound ends with \}
                    x if x == CHR('\\' as c_int) => {
                        if INCON!(v, L_BBND) && NEXT1!(v, '}') {
                            (*v).now = (*v).now.add(1);
                            INTOCON!(v, L_BRE);
                            RETV!(v, '}' as c_int, 1);
                        } else {
                            FAILW!(v, REG_BADBR);
                        }
                    }
                    _ => {
                        FAILW!(v, REG_BADBR);
                    }
                }
                #[allow(unreachable_code)]
                {
                    Assert!(NOTREACHED != 0);
                }
            }
            // brackets are not too hard
            L_BRACK => {
                match c {
                    x if x == CHR(']' as c_int) => {
                        if LASTTYPE!(v, '[' as c_int) {
                            RETV!(v, PLAIN, c);
                        } else {
                            INTOCON!(
                                v,
                                if (*v).cflags & REG_EXTENDED != 0 {
                                    L_ERE
                                } else {
                                    L_BRE
                                }
                            );
                            RET!(v, ']' as c_int);
                        }
                    }
                    x if x == CHR('\\' as c_int) => {
                        NOTE!(v, REG_UBBS);
                        if (*v).cflags & REG_ADVF == 0 {
                            RETV!(v, PLAIN, c);
                        }
                        NOTE!(v, REG_UNONPOSIX);
                        if ATEOS!(v) {
                            FAILW!(v, REG_EESCAPE);
                        }
                        if lexescape(v) == 0 {
                            return 0;
                        }
                        match (*v).nexttype {
                            // not all escapes okay here
                            PLAIN | CCLASSS | CCLASSC => {
                                return 1;
                            }
                            _ => {}
                        }
                        /* not one of the acceptable escapes */
                        FAILW!(v, REG_EESCAPE);
                    }
                    x if x == CHR('-' as c_int) => {
                        if LASTTYPE!(v, '[' as c_int) || NEXT1!(v, ']') {
                            RETV!(v, PLAIN, c);
                        } else {
                            RETV!(v, RANGE, c);
                        }
                    }
                    x if x == CHR('[' as c_int) => {
                        if ATEOS!(v) {
                            FAILW!(v, REG_EBRACK);
                        }
                        let d = *(*v).now;
                        (*v).now = (*v).now.add(1);
                        match d {
                            y if y == CHR('.' as c_int) => {
                                INTOCON!(v, L_CEL);
                                /* might or might not be locale-specific */
                                RET!(v, COLLEL);
                            }
                            y if y == CHR('=' as c_int) => {
                                INTOCON!(v, L_ECL);
                                NOTE!(v, REG_ULOCALE);
                                RET!(v, ECLASS);
                            }
                            y if y == CHR(':' as c_int) => {
                                INTOCON!(v, L_CCL);
                                NOTE!(v, REG_ULOCALE);
                                RET!(v, CCLASS);
                            }
                            // oops
                            _ => {
                                (*v).now = (*v).now.sub(1);
                                RETV!(v, PLAIN, c);
                            }
                        }
                        #[allow(unreachable_code)]
                        {
                            Assert!(NOTREACHED != 0);
                        }
                    }
                    _ => {
                        RETV!(v, PLAIN, c);
                    }
                }
                #[allow(unreachable_code)]
                {
                    Assert!(NOTREACHED != 0);
                }
            }
            // collating elements are easy
            L_CEL => {
                if c == CHR('.' as c_int) && NEXT1!(v, ']') {
                    (*v).now = (*v).now.add(1);
                    INTOCON!(v, L_BRACK);
                    RETV!(v, END, '.' as c_int);
                } else {
                    RETV!(v, PLAIN, c);
                }
            }
            // ditto equivalence classes
            L_ECL => {
                if c == CHR('=' as c_int) && NEXT1!(v, ']') {
                    (*v).now = (*v).now.add(1);
                    INTOCON!(v, L_BRACK);
                    RETV!(v, END, '=' as c_int);
                } else {
                    RETV!(v, PLAIN, c);
                }
            }
            // ditto character classes
            L_CCL => {
                if c == CHR(':' as c_int) && NEXT1!(v, ']') {
                    (*v).now = (*v).now.add(1);
                    INTOCON!(v, L_BRACK);
                    RETV!(v, END, ':' as c_int);
                } else {
                    RETV!(v, PLAIN, c);
                }
            }
            _ => {
                Assert!(NOTREACHED != 0);
            }
        }

        /* that got rid of everything except EREs and AREs */
        Assert!(INCON!(v, L_ERE));

        /* deal with EREs and AREs, except for backslashes */
        match c {
            x if x == CHR('|' as c_int) => {
                RET!(v, '|' as c_int);
            }
            x if x == CHR('*' as c_int) => {
                if (*v).cflags & REG_ADVF != 0 && NEXT1!(v, '?') {
                    (*v).now = (*v).now.add(1);
                    NOTE!(v, REG_UNONPOSIX);
                    RETV!(v, '*' as c_int, 0);
                }
                RETV!(v, '*' as c_int, 1);
            }
            x if x == CHR('+' as c_int) => {
                if (*v).cflags & REG_ADVF != 0 && NEXT1!(v, '?') {
                    (*v).now = (*v).now.add(1);
                    NOTE!(v, REG_UNONPOSIX);
                    RETV!(v, '+' as c_int, 0);
                }
                RETV!(v, '+' as c_int, 1);
            }
            x if x == CHR('?' as c_int) => {
                if (*v).cflags & REG_ADVF != 0 && NEXT1!(v, '?') {
                    (*v).now = (*v).now.add(1);
                    NOTE!(v, REG_UNONPOSIX);
                    RETV!(v, '?' as c_int, 0);
                }
                RETV!(v, '?' as c_int, 1);
            }
            // bounds start or plain character
            x if x == CHR('{' as c_int) => {
                if (*v).cflags & REG_EXPANDED != 0 {
                    skip(v);
                }
                if ATEOS!(v) || !iscdigit(*(*v).now) {
                    NOTE!(v, REG_UBRACES);
                    NOTE!(v, REG_UUNSPEC);
                    RETV!(v, PLAIN, c);
                } else {
                    NOTE!(v, REG_UBOUNDS);
                    INTOCON!(v, L_EBND);
                    RET!(v, '{' as c_int);
                }
                #[allow(unreachable_code)]
                {
                    Assert!(NOTREACHED != 0);
                }
            }
            // parenthesis, or advanced extension
            x if x == CHR('(' as c_int) => {
                if (*v).cflags & REG_ADVF != 0 && NEXT1!(v, '?') {
                    NOTE!(v, REG_UNONPOSIX);
                    (*v).now = (*v).now.add(1);
                    if ATEOS!(v) {
                        FAILW!(v, REG_BADRPT);
                    }
                    let d = *(*v).now;
                    (*v).now = (*v).now.add(1);
                    match d {
                        // non-capturing paren
                        y if y == CHR(':' as c_int) => {
                            RETV!(v, '(' as c_int, 0);
                        }
                        // comment
                        y if y == CHR('#' as c_int) => {
                            while !ATEOS!(v) && *(*v).now != CHR(')' as c_int) {
                                (*v).now = (*v).now.add(1);
                            }
                            if !ATEOS!(v) {
                                (*v).now = (*v).now.add(1);
                            }
                            Assert!((*v).nexttype == (*v).lasttype);
                            continue 'next_restart;
                        }
                        // positive lookahead
                        y if y == CHR('=' as c_int) => {
                            NOTE!(v, REG_ULOOKAROUND);
                            RETV!(v, LACON, LATYPE_AHEAD_POS);
                        }
                        // negative lookahead
                        y if y == CHR('!' as c_int) => {
                            NOTE!(v, REG_ULOOKAROUND);
                            RETV!(v, LACON, LATYPE_AHEAD_NEG);
                        }
                        y if y == CHR('<' as c_int) => {
                            if ATEOS!(v) {
                                FAILW!(v, REG_BADRPT);
                            }
                            let e = *(*v).now;
                            (*v).now = (*v).now.add(1);
                            match e {
                                // positive lookbehind
                                z if z == CHR('=' as c_int) => {
                                    NOTE!(v, REG_ULOOKAROUND);
                                    RETV!(v, LACON, LATYPE_BEHIND_POS);
                                }
                                // negative lookbehind
                                z if z == CHR('!' as c_int) => {
                                    NOTE!(v, REG_ULOOKAROUND);
                                    RETV!(v, LACON, LATYPE_BEHIND_NEG);
                                }
                                _ => {
                                    FAILW!(v, REG_BADRPT);
                                }
                            }
                            #[allow(unreachable_code)]
                            {
                                Assert!(NOTREACHED != 0);
                            }
                        }
                        _ => {
                            FAILW!(v, REG_BADRPT);
                        }
                    }
                    #[allow(unreachable_code)]
                    {
                        Assert!(NOTREACHED != 0);
                    }
                }
                RETV!(v, '(' as c_int, 1);
            }
            x if x == CHR(')' as c_int) => {
                if LASTTYPE!(v, '(' as c_int) {
                    NOTE!(v, REG_UUNSPEC);
                }
                RETV!(v, ')' as c_int, c);
            }
            // easy except for [[:<:]] and [[:>:]]
            x if x == CHR('[' as c_int) => {
                let mut c = c;
                if HAVE!(v, 6)
                    && *(*v).now.add(0) == CHR('[' as c_int)
                    && *(*v).now.add(1) == CHR(':' as c_int)
                    && (*(*v).now.add(2) == CHR('<' as c_int)
                        || *(*v).now.add(2) == CHR('>' as c_int))
                    && *(*v).now.add(3) == CHR(':' as c_int)
                    && *(*v).now.add(4) == CHR(']' as c_int)
                    && *(*v).now.add(5) == CHR(']' as c_int)
                {
                    c = *(*v).now.add(2);
                    (*v).now = (*v).now.add(6);
                    NOTE!(v, REG_UNONPOSIX);
                    RET!(
                        v,
                        if c == CHR('<' as c_int) {
                            '<' as c_int
                        } else {
                            '>' as c_int
                        }
                    );
                }
                INTOCON!(v, L_BRACK);
                if NEXT1!(v, '^') {
                    (*v).now = (*v).now.add(1);
                    RETV!(v, '[' as c_int, 0);
                }
                RETV!(v, '[' as c_int, 1);
            }
            x if x == CHR('.' as c_int) => {
                RET!(v, '.' as c_int);
            }
            x if x == CHR('^' as c_int) => {
                RET!(v, '^' as c_int);
            }
            x if x == CHR('$' as c_int) => {
                RET!(v, '$' as c_int);
            }
            // mostly punt backslashes to code below
            x if x == CHR('\\' as c_int) => {
                if ATEOS!(v) {
                    FAILW!(v, REG_EESCAPE);
                }
            }
            // ordinary character
            _ => {
                RETV!(v, PLAIN, c);
            }
        }

        /* ERE/ARE backslash handling; backslash already eaten */
        Assert!(!ATEOS!(v));
        if (*v).cflags & REG_ADVF == 0 {
            /* only AREs have non-trivial escapes */
            if iscalnum(*(*v).now) {
                NOTE!(v, REG_UBSALNUM);
                NOTE!(v, REG_UUNSPEC);
            }
            let pc = *(*v).now;
            (*v).now = (*v).now.add(1);
            RETV!(v, PLAIN, pc);
        }
        return lexescape(v);
    }
}

/*
 * lexescape - parse an ARE backslash escape (backslash already eaten)
 *
 * This is used for ARE backslashes both normally and inside bracket
 * expressions.  In the latter case, not all escape types are allowed,
 * but the caller must reject unwanted ones after we return.
 */
pub unsafe fn lexescape(v: *mut vars) -> c_int {
    let mut c: chr;
    static alert: [chr; 5] = [
        (b'a' as u8) as chr,
        (b'l' as u8) as chr,
        (b'e' as u8) as chr,
        (b'r' as u8) as chr,
        (b't' as u8) as chr,
    ];
    static esc: [chr; 3] = [
        (b'E' as u8) as chr,
        (b'S' as u8) as chr,
        (b'C' as u8) as chr,
    ];
    let save: *const chr;

    Assert!((*v).cflags & REG_ADVF != 0);

    Assert!(!ATEOS!(v));
    c = *(*v).now;
    (*v).now = (*v).now.add(1);

    /* if it's not alphanumeric ASCII, treat it as a plain character */
    if !('a' as chr <= c && c <= 'z' as chr)
        && !('A' as chr <= c && c <= 'Z' as chr)
        && !('0' as chr <= c && c <= '9' as chr)
    {
        RETV!(v, PLAIN, c);
    }

    NOTE!(v, REG_UNONPOSIX);
    match c {
        x if x == CHR('a' as c_int) => {
            RETV!(
                v,
                PLAIN,
                chrnamed(v, alert.as_ptr(), alert.as_ptr().add(alert.len()), CHR('\u{0007}' as c_int))
            );
        }
        x if x == CHR('A' as c_int) => {
            RETV!(v, SBEGIN, 0);
        }
        x if x == CHR('b' as c_int) => {
            RETV!(v, PLAIN, CHR('\u{0008}' as c_int));
        }
        x if x == CHR('B' as c_int) => {
            RETV!(v, PLAIN, CHR('\\' as c_int));
        }
        x if x == CHR('c' as c_int) => {
            NOTE!(v, REG_UUNPORT);
            if ATEOS!(v) {
                FAILW!(v, REG_EESCAPE);
            }
            let nc = *(*v).now;
            (*v).now = (*v).now.add(1);
            RETV!(v, PLAIN, nc & 0o37);
        }
        x if x == CHR('d' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, CCLASSS, CC_DIGIT);
        }
        x if x == CHR('D' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, CCLASSC, CC_DIGIT);
        }
        x if x == CHR('e' as c_int) => {
            NOTE!(v, REG_UUNPORT);
            RETV!(
                v,
                PLAIN,
                chrnamed(v, esc.as_ptr(), esc.as_ptr().add(esc.len()), CHR('\u{001b}' as c_int))
            );
        }
        x if x == CHR('f' as c_int) => {
            RETV!(v, PLAIN, CHR('\u{000c}' as c_int));
        }
        x if x == CHR('m' as c_int) => {
            RET!(v, '<' as c_int);
        }
        x if x == CHR('M' as c_int) => {
            RET!(v, '>' as c_int);
        }
        x if x == CHR('n' as c_int) => {
            RETV!(v, PLAIN, CHR('\n' as c_int));
        }
        x if x == CHR('r' as c_int) => {
            RETV!(v, PLAIN, CHR('\r' as c_int));
        }
        x if x == CHR('s' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, CCLASSS, CC_SPACE);
        }
        x if x == CHR('S' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, CCLASSC, CC_SPACE);
        }
        x if x == CHR('t' as c_int) => {
            RETV!(v, PLAIN, CHR('\t' as c_int));
        }
        x if x == CHR('u' as c_int) => {
            c = lexdigits(v, 16, 4, 4);
            if ISERR!(v) || !CHR_IS_IN_RANGE(c) {
                FAILW!(v, REG_EESCAPE);
            }
            RETV!(v, PLAIN, c);
        }
        x if x == CHR('U' as c_int) => {
            c = lexdigits(v, 16, 8, 8);
            if ISERR!(v) || !CHR_IS_IN_RANGE(c) {
                FAILW!(v, REG_EESCAPE);
            }
            RETV!(v, PLAIN, c);
        }
        x if x == CHR('v' as c_int) => {
            RETV!(v, PLAIN, CHR('\u{000b}' as c_int));
        }
        x if x == CHR('w' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, CCLASSS, CC_WORD);
        }
        x if x == CHR('W' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, CCLASSC, CC_WORD);
        }
        x if x == CHR('x' as c_int) => {
            NOTE!(v, REG_UUNPORT);
            c = lexdigits(v, 16, 1, 255); /* REs >255 long outside spec */
            if ISERR!(v) || !CHR_IS_IN_RANGE(c) {
                FAILW!(v, REG_EESCAPE);
            }
            RETV!(v, PLAIN, c);
        }
        x if x == CHR('y' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, WBDRY, 0);
        }
        x if x == CHR('Y' as c_int) => {
            NOTE!(v, REG_ULOCALE);
            RETV!(v, NWBDRY, 0);
        }
        x if x == CHR('Z' as c_int) => {
            RETV!(v, SEND, 0);
        }
        x if x == CHR('1' as c_int)
            || x == CHR('2' as c_int)
            || x == CHR('3' as c_int)
            || x == CHR('4' as c_int)
            || x == CHR('5' as c_int)
            || x == CHR('6' as c_int)
            || x == CHR('7' as c_int)
            || x == CHR('8' as c_int)
            || x == CHR('9' as c_int) =>
        {
            save = (*v).now;
            (*v).now = (*v).now.sub(1); // put first digit back
            c = lexdigits(v, 10, 1, 255); /* REs >255 long outside spec */
            if ISERR!(v) {
                FAILW!(v, REG_EESCAPE);
            }
            /* ugly heuristic (first test is "exactly 1 digit?") */
            if (*v).now == save || ((c as c_int) > 0 && (c as c_int) <= (*v).nsubexp) {
                NOTE!(v, REG_UBACKREF);
                RETV!(v, BACKREF, c);
            }
            /* oops, doesn't look like it's a backref after all... */
            (*v).now = save;
            /* and fall through into octal number */
            /* FALLTHROUGH */
            NOTE!(v, REG_UUNPORT);
            (*v).now = (*v).now.sub(1); // put first digit back
            c = lexdigits(v, 8, 1, 3);
            if ISERR!(v) {
                FAILW!(v, REG_EESCAPE);
            }
            if c > 0xff {
                /* out of range, so we handled one digit too much */
                (*v).now = (*v).now.sub(1);
                c >>= 3;
            }
            RETV!(v, PLAIN, c);
        }
        x if x == CHR('0' as c_int) => {
            NOTE!(v, REG_UUNPORT);
            (*v).now = (*v).now.sub(1); // put first digit back
            c = lexdigits(v, 8, 1, 3);
            if ISERR!(v) {
                FAILW!(v, REG_EESCAPE);
            }
            if c > 0xff {
                /* out of range, so we handled one digit too much */
                (*v).now = (*v).now.sub(1);
                c >>= 3;
            }
            RETV!(v, PLAIN, c);
        }
        _ => {
            /*
             * Throw an error for unrecognized ASCII alpha escape sequences,
             * which reserves them for future use if needed.
             */
            FAILW!(v, REG_EESCAPE);
        }
    }
    #[allow(unreachable_code)]
    {
        Assert!(NOTREACHED != 0);
        0
    }
}

/*
 * lexdigits - slurp up digits and return chr value
 *
 * This does not account for overflow; callers should range-check the result
 * if maxlen is large enough to make that possible.
 *
 * chr value; errors signalled via ERR
 */
pub unsafe fn lexdigits(v: *mut vars, base: c_int, minlen: c_int, maxlen: c_int) -> chr {
    let mut n: uchr; // unsigned to avoid overflow misbehavior
    let mut len: c_int;
    let mut c: chr;
    let mut d: c_int;
    let ub: uchr = base as uchr;

    n = 0;
    len = 0;
    while len < maxlen && !ATEOS!(v) {
        c = *(*v).now;
        (*v).now = (*v).now.add(1);
        match c {
            x if x == CHR('0' as c_int)
                || x == CHR('1' as c_int)
                || x == CHR('2' as c_int)
                || x == CHR('3' as c_int)
                || x == CHR('4' as c_int)
                || x == CHR('5' as c_int)
                || x == CHR('6' as c_int)
                || x == CHR('7' as c_int)
                || x == CHR('8' as c_int)
                || x == CHR('9' as c_int) =>
            {
                d = DIGITVAL(c) as c_int;
            }
            x if x == CHR('a' as c_int) || x == CHR('A' as c_int) => {
                d = 10;
            }
            x if x == CHR('b' as c_int) || x == CHR('B' as c_int) => {
                d = 11;
            }
            x if x == CHR('c' as c_int) || x == CHR('C' as c_int) => {
                d = 12;
            }
            x if x == CHR('d' as c_int) || x == CHR('D' as c_int) => {
                d = 13;
            }
            x if x == CHR('e' as c_int) || x == CHR('E' as c_int) => {
                d = 14;
            }
            x if x == CHR('f' as c_int) || x == CHR('F' as c_int) => {
                d = 15;
            }
            _ => {
                (*v).now = (*v).now.sub(1); // oops, not a digit at all
                d = -1;
            }
        }

        if d >= base {
            /* not a plausible digit */
            (*v).now = (*v).now.sub(1);
            d = -1;
        }
        if d < 0 {
            break; // NOTE BREAK OUT
        }
        n = n * ub + (d as uchr);
        len += 1;
    }
    if len < minlen {
        ERR!(v, REG_EESCAPE);
    }

    n as chr
}

/*
 * brenext - get next BRE token
 *
 * This is much like EREs except for all the stupid backslashes and the
 * context-dependency of some things.
 *
 * Returns 1 normal, 0 failure.
 */
pub unsafe fn brenext(v: *mut vars, mut c: chr) -> c_int {
    match c {
        x if x == CHR('*' as c_int) => {
            if LASTTYPE!(v, EMPTY) || LASTTYPE!(v, '(' as c_int) || LASTTYPE!(v, '^' as c_int) {
                RETV!(v, PLAIN, c);
            }
            RETV!(v, '*' as c_int, 1);
        }
        x if x == CHR('[' as c_int) => {
            if HAVE!(v, 6)
                && *(*v).now.add(0) == CHR('[' as c_int)
                && *(*v).now.add(1) == CHR(':' as c_int)
                && (*(*v).now.add(2) == CHR('<' as c_int)
                    || *(*v).now.add(2) == CHR('>' as c_int))
                && *(*v).now.add(3) == CHR(':' as c_int)
                && *(*v).now.add(4) == CHR(']' as c_int)
                && *(*v).now.add(5) == CHR(']' as c_int)
            {
                c = *(*v).now.add(2);
                (*v).now = (*v).now.add(6);
                NOTE!(v, REG_UNONPOSIX);
                RET!(
                    v,
                    if c == CHR('<' as c_int) {
                        '<' as c_int
                    } else {
                        '>' as c_int
                    }
                );
            }
            INTOCON!(v, L_BRACK);
            if NEXT1!(v, '^') {
                (*v).now = (*v).now.add(1);
                RETV!(v, '[' as c_int, 0);
            }
            RETV!(v, '[' as c_int, 1);
        }
        x if x == CHR('.' as c_int) => {
            RET!(v, '.' as c_int);
        }
        x if x == CHR('^' as c_int) => {
            if LASTTYPE!(v, EMPTY) {
                RET!(v, '^' as c_int);
            }
            if LASTTYPE!(v, '(' as c_int) {
                NOTE!(v, REG_UUNSPEC);
                RET!(v, '^' as c_int);
            }
            RETV!(v, PLAIN, c);
        }
        x if x == CHR('$' as c_int) => {
            if (*v).cflags & REG_EXPANDED != 0 {
                skip(v);
            }
            if ATEOS!(v) {
                RET!(v, '$' as c_int);
            }
            if NEXT2!(v, '\\', ')') {
                NOTE!(v, REG_UUNSPEC);
                RET!(v, '$' as c_int);
            }
            RETV!(v, PLAIN, c);
        }
        x if x == CHR('\\' as c_int) => {
            // see below
        }
        _ => {
            RETV!(v, PLAIN, c);
        }
    }

    Assert!(c == CHR('\\' as c_int));

    if ATEOS!(v) {
        FAILW!(v, REG_EESCAPE);
    }

    c = *(*v).now;
    (*v).now = (*v).now.add(1);
    match c {
        x if x == CHR('{' as c_int) => {
            INTOCON!(v, L_BBND);
            NOTE!(v, REG_UBOUNDS);
            RET!(v, '{' as c_int);
        }
        x if x == CHR('(' as c_int) => {
            RETV!(v, '(' as c_int, 1);
        }
        x if x == CHR(')' as c_int) => {
            RETV!(v, ')' as c_int, c);
        }
        x if x == CHR('<' as c_int) => {
            NOTE!(v, REG_UNONPOSIX);
            RET!(v, '<' as c_int);
        }
        x if x == CHR('>' as c_int) => {
            NOTE!(v, REG_UNONPOSIX);
            RET!(v, '>' as c_int);
        }
        x if x == CHR('1' as c_int)
            || x == CHR('2' as c_int)
            || x == CHR('3' as c_int)
            || x == CHR('4' as c_int)
            || x == CHR('5' as c_int)
            || x == CHR('6' as c_int)
            || x == CHR('7' as c_int)
            || x == CHR('8' as c_int)
            || x == CHR('9' as c_int) =>
        {
            NOTE!(v, REG_UBACKREF);
            RETV!(v, BACKREF, DIGITVAL(c));
        }
        _ => {
            if iscalnum(c) {
                NOTE!(v, REG_UBSALNUM);
                NOTE!(v, REG_UUNSPEC);
            }
            RETV!(v, PLAIN, c);
        }
    }

    #[allow(unreachable_code)]
    {
        Assert!(NOTREACHED != 0);
        0
    }
}

/*
 * skip - skip white space and comments in expanded form
 */
pub unsafe fn skip(v: *mut vars) {
    let start: *const chr = (*v).now;

    Assert!((*v).cflags & REG_EXPANDED != 0);

    loop {
        while !ATEOS!(v) && iscspace(*(*v).now) {
            (*v).now = (*v).now.add(1);
        }
        if ATEOS!(v) || *(*v).now != CHR('#' as c_int) {
            break; // NOTE BREAK OUT
        }
        Assert!(NEXT1!(v, '#'));
        while !ATEOS!(v) && *(*v).now != CHR('\n' as c_int) {
            (*v).now = (*v).now.add(1);
        }
        /* leave the newline to be picked up by the iscspace loop */
    }

    if (*v).now != start {
        NOTE!(v, REG_UNONPOSIX);
    }
}

/*
 * newline - return the chr for a newline
 *
 * This helps confine use of CHR to this source file.
 */
pub unsafe fn newline() -> chr {
    CHR('\n' as c_int)
}

/*
 * chrnamed - return the chr known by a given (chr string) name
 *
 * The code is a bit clumsy, but this routine gets only such specialized
 * use that it hardly matters.
 *
 * startp: start of name; endp: just past end of name;
 * lastresort: what to return if name lookup fails.
 */
pub unsafe fn chrnamed(
    v: *mut vars,
    startp: *const chr,
    endp: *const chr,
    lastresort: chr,
) -> chr {
    let c: chr;
    let errsave: c_int;
    let e: c_int;
    let cv: *mut cvec;

    errsave = (*v).err;
    (*v).err = 0;
    c = element(v, startp, endp);
    e = (*v).err;
    (*v).err = errsave;

    if e != 0 {
        return lastresort;
    }

    cv = range(v, c, c, 0);
    if (*cv).nchrs == 0 {
        return lastresort;
    }
    *(*cv).chrs.add(0)
}

// ---------------------------------------------------------------------------
// dependencies from regc_locale.c (#included by regcomp.c). These are stubbed
// here; the real implementations live in that translation unit.
// TODO(pg-port): import from regc_locale once it is translated.
// ---------------------------------------------------------------------------

/// element - map collating-element name to celt -- from regc_locale.c
unsafe fn element(_v: *mut vars, _startp: *const chr, _endp: *const chr) -> chr {
    // TODO(pg-port): implement in regc_locale.rs
    0
}

/// range - set up cvec for a range of characters -- from regc_locale.c
unsafe fn range(_v: *mut vars, _a: chr, _b: chr, _cases: c_int) -> *mut cvec {
    // TODO(pg-port): implement in regc_locale.rs
    std::ptr::null_mut()
}
